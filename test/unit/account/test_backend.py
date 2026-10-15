# Copyright (c) 2010-2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Tests for swift.account.backend """
from collections import defaultdict
import json
import pickle
import os
from time import sleep, time
from uuid import uuid4
import sqlite3
import itertools
import operator
from functools import reduce
from contextlib import contextmanager
import random
from unittest import mock
import base64
import shutil

from swift.account.backend import AccountBroker
from swift.common.utils import Timestamp
from swift.common.utils.timestamp import NormalTimestamp as LegacyTimestamp
from test.unit import patch_policies, with_tempdir, mock_normal_timestamp_now
from swift.common.db import DatabaseConnectionError, TombstoneReclaimer
from swift.common.request_helpers import get_reserved_name
from swift.common.storage_policy import StoragePolicy, POLICIES
from swift.common.utils import md5

from test.unit.common import test_db


def chexor_hash(*rows):
    """
    Compute the account db hash for rows, each of which is a (name, value)
    tuple, independently of the db triggers.
    """
    digests = [md5(('%s-%s' % row).encode('ascii'),
                   usedforsecurity=False).digest() for row in rows]
    return ''.join('%02x' % b for b in
                   (reduce(operator.xor, vals) for vals in zip(*digests)))


@patch_policies
class TestAccountBroker(test_db.TestDbBase):
    """Tests for AccountBroker"""

    def setUp(self):
        super(TestAccountBroker, self).setUp()

    def test_creation(self):
        # Test AccountBroker.__init__
        db_file = self.get_db_path()
        broker = AccountBroker(db_file, account='a')
        self.assertEqual(broker.db_file, db_file)
        try:
            with broker.get() as conn:
                pass
        except DatabaseConnectionError as e:
            self.assertTrue(hasattr(e, 'path'))
            self.assertEqual(e.path, db_file)
            self.assertTrue(hasattr(e, 'msg'))
            self.assertEqual(e.msg, "DB doesn't exist")
        except Exception as e:
            self.fail("Unexpected exception raised: %r" % e)
        else:
            self.fail("Expected a DatabaseConnectionError exception")
        broker.initialize(Timestamp('1').internal)
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)

    def test_initialize_fail(self):
        broker = AccountBroker(self.get_db_path())
        with self.assertRaises(ValueError) as cm:
            broker.initialize(Timestamp('1').internal)
        self.assertEqual(str(cm.exception), 'Attempting to create a new'
                         ' database with no account set')

    def test_exception(self):
        # Test AccountBroker throwing a conn away after exception
        first_conn = None
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        with broker.get() as conn:
            first_conn = conn
        try:
            with broker.get() as conn:
                self.assertEqual(first_conn, conn)
                raise Exception('OMG')
        except Exception:
            pass
        self.assertIsNone(broker.conn)

    def test_empty(self):
        # Test AccountBroker.empty
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        self.assertTrue(broker.empty())
        broker.put_container('o', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        self.assertFalse(broker.empty())
        sleep(.00001)
        broker.put_container('o', 0, Timestamp.now().internal, 0, 0,
                             POLICIES.default.idx)
        self.assertTrue(broker.empty())

    def test_is_status_deleted(self):
        # Test AccountBroker.is_status_deleted
        broker1 = AccountBroker(self.get_db_path(), account='a')
        broker1.initialize(Timestamp.now().internal)
        self.assertFalse(broker1.is_status_deleted())
        broker1.delete_db(Timestamp.now().internal)
        self.assertTrue(broker1.is_status_deleted())
        broker2 = AccountBroker(self.get_db_path(), account='a')
        broker2.initialize(Timestamp.now().internal)
        # Set delete_timestamp greater than put_timestamp
        broker2.merge_timestamps(
            time(), Timestamp.now().internal,
            Timestamp(time() + 999).internal)
        self.assertTrue(broker2.is_status_deleted())

    def test_reclaim(self):
        broker = AccountBroker(self.get_db_path(), account='test_account')
        broker.initialize(Timestamp('1').internal)
        broker.put_container('c', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        # commit pending file into db
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(Timestamp(time() - 999).internal, time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('c', 0, Timestamp.now().internal, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(Timestamp(time() - 999).internal, time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(Timestamp.now().internal, time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test reclaim after deletion. Create 3 test containers
        broker.put_container('x', 0, 0, 0, 0, POLICIES.default.idx)
        broker.put_container('y', 0, 0, 0, 0, POLICIES.default.idx)
        broker.put_container('z', 0, 0, 0, 0, POLICIES.default.idx)
        broker._commit_puts()
        broker.reclaim(Timestamp.now().internal, time())
        # Now delete the account
        broker.delete_db(Timestamp.now().internal)
        broker.reclaim(Timestamp.now().internal, time())

    def test_batched_reclaim(self):
        num_of_containers = 60
        container_specs = []
        now = time()
        top_of_the_minute = now - (now % 60)
        c = itertools.cycle([True, False])
        for m, is_deleted in zip(range(num_of_containers), c):
            offset = top_of_the_minute - (m * 60)
            container_specs.append((Timestamp(offset), is_deleted))
        random.seed(now)
        random.shuffle(container_specs)
        policy_indexes = list(p.idx for p in POLICIES)
        broker = AccountBroker(self.get_db_path(), account='test_account')
        broker.initialize(Timestamp('1').internal)
        for i, container_spec in enumerate(container_specs):
            # with container12 before container2 and shuffled ts.internal we
            # shouldn't be able to accidentally rely on any implicit ordering
            name = 'container%s' % i
            pidx = random.choice(policy_indexes)
            ts, is_deleted = container_spec
            if is_deleted:
                broker.put_container(name, 0, ts.internal, 0, 0, pidx)
            else:
                broker.put_container(name, ts.internal, 0, 0, 0, pidx)

        # commit pending file into db
        broker._commit_puts()

        def count_reclaimable(conn, reclaim_age):
            return conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1 AND delete_timestamp < ?", (reclaim_age,)
            ).fetchone()[0]

        # This is intended to divide the set of timestamps exactly in half
        # regardless of the value of now
        reclaim_age = top_of_the_minute + 1 - (num_of_containers / 2 * 60)
        with broker.get() as conn:
            self.assertEqual(count_reclaimable(conn, reclaim_age),
                             num_of_containers / 4)

        trace = []

        class TracingReclaimer(TombstoneReclaimer):
            def _reclaim(self, conn):
                trace.append(
                    (self.age_timestamp, self.marker,
                     count_reclaimable(conn, self.age_timestamp)))
                return super(TracingReclaimer, self)._reclaim(conn)

        with mock.patch(
                'swift.common.db.TombstoneReclaimer', TracingReclaimer), \
                mock.patch('swift.common.db.RECLAIM_PAGE_SIZE', 10):
            broker.reclaim(reclaim_age, reclaim_age)
        with broker.get() as conn:
            self.assertEqual(count_reclaimable(conn, reclaim_age), 0)
        self.assertEqual(3, len(trace), trace)
        self.assertEqual([age for age, marker, reclaimable in trace],
                         [reclaim_age] * 3)
        # markers are in-order
        self.assertLess(trace[0][1], trace[1][1])
        self.assertLess(trace[1][1], trace[2][1])
        # reclaimable count gradually decreases
        # generally, count1 > count2 > count3, but because of the randomness
        # we may occassionally have count1 == count2 or count2 == count3
        self.assertGreaterEqual(trace[0][2], trace[1][2])
        self.assertGreaterEqual(trace[1][2], trace[2][2])
        # technically, this might happen occasionally, but *really* rarely
        self.assertTrue(trace[0][2] > trace[1][2] or
                        trace[1][2] > trace[2][2])

    def test_delete_db_status(self):
        start = self.normal_ts()
        broker = AccountBroker(self.get_db_path(), account='a')
        with mock_normal_timestamp_now(self.normal_ts()) as exp_created_at:
            broker.initialize(start.internal)
        info = broker.get_info()
        self.assertEqual(info['put_timestamp'], start.internal)
        self.assertEqual(info['created_at'], exp_created_at.internal)
        self.assertGreaterEqual(Timestamp(info['created_at']), start)
        self.assertEqual(info['delete_timestamp'], '0')
        if self.__class__ == TestAccountBrokerBeforeMetadata:
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(info['status_changed_at'], start.internal)

        # delete it
        delete_timestamp = self.ts()
        broker.delete_db(delete_timestamp.internal)
        info = broker.get_info()
        self.assertEqual(info['put_timestamp'], start.internal)
        self.assertGreaterEqual(Timestamp(info['created_at']), start)
        self.assertEqual(info['delete_timestamp'], delete_timestamp.internal)
        self.assertEqual(info['status_changed_at'], delete_timestamp.internal)

    def test_delete_container(self):
        # Test AccountBroker.delete_container
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        broker.put_container('o', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        # commit pending file into db
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('o', 0, Timestamp.now().internal, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_put_container(self):
        # Test AccountBroker.put_container
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)

        # Create initial container
        timestamp = Timestamp.now().internal
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        # commit pending file into db
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Reput same event
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put new event
        sleep(.00001)
        timestamp = Timestamp.now().internal
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put old event
        otimestamp = Timestamp(float(Timestamp(timestamp)) - 1).internal
        broker.put_container('"{<container \'&\' name>}"', otimestamp, 0, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put old delete event
        dtimestamp = Timestamp(float(Timestamp(timestamp)) - 1).internal
        broker.put_container('"{<container \'&\' name>}"', 0, dtimestamp, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT delete_timestamp FROM container").fetchone()[0],
                dtimestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put new delete event
        sleep(.00001)
        timestamp = Timestamp.now().internal
        broker.put_container('"{<container \'&\' name>}"', 0, timestamp, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT delete_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 1)

        # Put new event
        sleep(.00001)
        timestamp = Timestamp.now().internal
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

    def test_get_info(self):
        # Test AccountBroker.get_info
        broker = AccountBroker(self.get_db_path(), account='test1')
        put_ts = self.ts()
        broker.initialize(put_ts.internal)

        info = broker.get_info()
        self.assertEqual(info['account'], 'test1')
        self.assertEqual(info['hash'], '00000000000000000000000000000000')
        self.assertEqual(info['put_timestamp'], put_ts.internal)
        self.assertEqual(info['delete_timestamp'], '0')
        if self.__class__ == TestAccountBrokerBeforeMetadata:
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(info['status_changed_at'], put_ts.internal)

        info = broker.get_info()
        self.assertEqual(info['container_count'], 0)

        broker.put_container('c1', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c2', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c1', 0, Timestamp.now().internal, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', 0, Timestamp.now().internal, 0, 0,
                             POLICIES.default.idx)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 0)

    def test_list_containers_iter(self):
        # Test AccountBroker.list_containers_iter
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        for cont1 in range(4):
            for cont2 in range(125):
                broker.put_container('%d-%04d' % (cont1, cont2),
                                     Timestamp.now().internal, 0, 0, 0,
                                     POLICIES.default.idx)
        for cont in range(125):
            broker.put_container('2-0051-%04d' % cont,
                                 Timestamp.now().internal, 0, 0, 0,
                                 POLICIES.default.idx)

        for cont in range(125):
            broker.put_container('3-%04d-0049' % cont,
                                 Timestamp.now().internal, 0, 0, 0,
                                 POLICIES.default.idx)

        listing = broker.list_containers_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0-0000')
        self.assertEqual(listing[-1][0], '0-0099')

        listing = broker.list_containers_iter(100, '', '0-0050', None, '')
        self.assertEqual(len(listing), 50)
        self.assertEqual(listing[0][0], '0-0000')
        self.assertEqual(listing[-1][0], '0-0049')

        listing = broker.list_containers_iter(100, '0-0099', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0-0100')
        self.assertEqual(listing[-1][0], '1-0074')

        listing = broker.list_containers_iter(55, '1-0074', None, None, '')
        self.assertEqual(len(listing), 55)
        self.assertEqual(listing[0][0], '1-0075')
        self.assertEqual(listing[-1][0], '2-0004')

        listing = broker.list_containers_iter(10, '', None, '0-01', '')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0100')
        self.assertEqual(listing[-1][0], '0-0109')

        listing = broker.list_containers_iter(10, '', None, '0-01', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0100')
        self.assertEqual(listing[-1][0], '0-0109')

        listing = broker.list_containers_iter(10, '', None, '0-00', '-',
                                              reverse=True)
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0099')
        self.assertEqual(listing[-1][0], '0-0090')

        listing = broker.list_containers_iter(10, '', None, '0-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0000')
        self.assertEqual(listing[-1][0], '0-0009')

        listing = broker.list_containers_iter(10, '', None, '0-', '-',
                                              reverse=True)
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0124')
        self.assertEqual(listing[-1][0], '0-0115')

        listing = broker.list_containers_iter(10, '', None, '', '-')
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['0-', '1-', '2-', '3-'])

        listing = broker.list_containers_iter(10, '', None, '', '-',
                                              reverse=True)
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['3-', '2-', '1-', '0-'])

        listing = broker.list_containers_iter(10, '2-', None, None, '-')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['3-'])

        listing = broker.list_containers_iter(10, '2-', None, None, '-',
                                              reverse=True)
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['1-', '0-'])

        listing = broker.list_containers_iter(10, '2.', None, None, '-',
                                              reverse=True)
        self.assertEqual(len(listing), 3)
        self.assertEqual([row[0] for row in listing], ['2-', '1-', '0-'])

        listing = broker.list_containers_iter(10, '', None, '2', '-')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['2-'])

        listing = broker.list_containers_iter(10, '2-0050', None, '2-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '2-0051')
        self.assertEqual(listing[1][0], '2-0051-')
        self.assertEqual(listing[2][0], '2-0052')
        self.assertEqual(listing[-1][0], '2-0059')

        listing = broker.list_containers_iter(10, '3-0045', None, '3-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3-0045-', '3-0046', '3-0046-', '3-0047',
                          '3-0047-', '3-0048', '3-0048-', '3-0049',
                          '3-0049-', '3-0050'])

        broker.put_container('3-0049-', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        listing = broker.list_containers_iter(10, '3-0048', None, None, None)
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3-0048-0049', '3-0049', '3-0049-', '3-0049-0049',
                          '3-0050', '3-0050-0049', '3-0051', '3-0051-0049',
                          '3-0052', '3-0052-0049'])

        listing = broker.list_containers_iter(10, '3-0048', None, '3-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3-0048-', '3-0049', '3-0049-', '3-0050',
                          '3-0050-', '3-0051', '3-0051-', '3-0052',
                          '3-0052-', '3-0053'])

        listing = broker.list_containers_iter(10, None, None, '3-0049-', '-')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing],
                         ['3-0049-', '3-0049-0049'])

    def test_list_objects_iter_order_and_reverse(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        put_ts = self.ts()
        broker.initialize(put_ts.internal, 0)

        broker.put_container(
            'c1', self.ts().internal, 0, 0, 0, POLICIES.default.idx)
        broker.put_container(
            'c10', self.ts().internal, 0, 0, 0, POLICIES.default.idx)
        broker.put_container(
            'C1', self.ts().internal, 0, 0, 0, POLICIES.default.idx)
        broker.put_container(
            'c2', self.ts().internal, 0, 0, 0, POLICIES.default.idx)
        broker.put_container(
            'c3', self.ts().internal, 0, 0, 0, POLICIES.default.idx)
        broker.put_container(
            'C4', self.ts().internal, 0, 0, 0, POLICIES.default.idx)

        listing = broker.list_containers_iter(100, None, None, '', '',
                                              reverse=False)
        self.assertEqual([row[0] for row in listing],
                         ['C1', 'C4', 'c1', 'c10', 'c2', 'c3'])
        listing = broker.list_containers_iter(100, None, None, '', '',
                                              reverse=True)
        self.assertEqual([row[0] for row in listing],
                         ['c3', 'c2', 'c10', 'c1', 'C4', 'C1'])
        listing = broker.list_containers_iter(2, None, None, '', '',
                                              reverse=True)
        self.assertEqual([row[0] for row in listing],
                         ['c3', 'c2'])
        listing = broker.list_containers_iter(100, 'c2', 'C4', '', '',
                                              reverse=True)
        self.assertEqual([row[0] for row in listing],
                         ['c10', 'c1'])

    def test_list_container_iter_with_reserved_name(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal, 0)

        broker.put_container(
            'foo', self.ts().internal, 0, 0, 0, POLICIES.default.idx)
        broker.put_container(
            get_reserved_name('foo'), self.ts().internal, 0, 0, 0,
            POLICIES.default.idx)

        listing = broker.list_containers_iter(100, None, None, '', '')
        self.assertEqual([row[0] for row in listing], ['foo'])

        listing = broker.list_containers_iter(100, None, None, '', '',
                                              reverse=True)
        self.assertEqual([row[0] for row in listing], ['foo'])

        listing = broker.list_containers_iter(100, None, None, '', '',
                                              allow_reserved=True)
        self.assertEqual([row[0] for row in listing],
                         [get_reserved_name('foo'), 'foo'])

        listing = broker.list_containers_iter(100, None, None, '', '',
                                              reverse=True,
                                              allow_reserved=True)
        self.assertEqual([row[0] for row in listing],
                         ['foo', get_reserved_name('foo')])

    def test_reverse_prefix_delim(self):
        expectations = [
            {
                'containers': [
                    'topdir1-subdir1,0-c1',
                    'topdir1-subdir1,1-c1',
                    'topdir1-subdir1-c1',
                ],
                'params': {
                    'prefix': 'topdir1-',
                    'delimiter': '-',
                },
                'expected': [
                    'topdir1-subdir1,0-',
                    'topdir1-subdir1,1-',
                    'topdir1-subdir1-',
                ],
            },
            {
                'containers': [
                    'topdir1-subdir1,0-c1',
                    'topdir1-subdir1,1-c1',
                    'topdir1-subdir1-c1',
                    'topdir1-subdir1.',
                    'topdir1-subdir1.-c1',
                ],
                'params': {
                    'prefix': 'topdir1-',
                    'delimiter': '-',
                },
                'expected': [
                    'topdir1-subdir1,0-',
                    'topdir1-subdir1,1-',
                    'topdir1-subdir1-',
                    'topdir1-subdir1.',
                    'topdir1-subdir1.-',
                ],
            },
            {
                'containers': [
                    'topdir1-subdir1-c1',
                    'topdir1-subdir1,0-c1',
                    'topdir1-subdir1,1-c1',
                ],
                'params': {
                    'prefix': 'topdir1-',
                    'delimiter': '-',
                    'reverse': True,
                },
                'expected': [
                    'topdir1-subdir1-',
                    'topdir1-subdir1,1-',
                    'topdir1-subdir1,0-',
                ],
            },
            {
                'containers': [
                    'topdir1-subdir1.-c1',
                    'topdir1-subdir1.',
                    'topdir1-subdir1-c1',
                    'topdir1-subdir1-',
                    'topdir1-subdir1,',
                    'topdir1-subdir1,0-c1',
                    'topdir1-subdir1,1-c1',
                ],
                'params': {
                    'prefix': 'topdir1-',
                    'delimiter': '-',
                    'reverse': True,
                },
                'expected': [
                    'topdir1-subdir1.-',
                    'topdir1-subdir1.',
                    'topdir1-subdir1-',
                    'topdir1-subdir1,1-',
                    'topdir1-subdir1,0-',
                    'topdir1-subdir1,',
                ],
            },
            {
                'containers': [
                    '1',
                    '2',
                    '3:1',
                    '3:2:1',
                    '3:2:2',
                    '3:3',
                    '4',
                ],
                'params': {
                    'prefix': '3:',
                    'delimiter': ':',
                    'reverse': True,
                },
                'expected': [
                    '3:3',
                    '3:2:',
                    '3:1',
                ],
            },
        ]
        default_listing_params = {
            'limit': 10000,
            'marker': '',
            'end_marker': None,
            'prefix': None,
            'delimiter': None,
        }
        failures = []
        for expected in expectations:
            broker = AccountBroker(self.get_db_path(), account='a')
            broker.initialize(self.ts().internal, 0)
            for name in expected['containers']:
                broker.put_container(name, self.ts().internal, 0, 0, 0,
                                     POLICIES.default.idx)
            # commit pending file into db
            broker._commit_puts()
            params = default_listing_params.copy()
            params.update(expected['params'])
            listing = list(c[0] for c in broker.list_containers_iter(**params))
            if listing != expected['expected']:
                expected['listing'] = listing
                failures.append(
                    "With containers %(containers)r, the params %(params)r "
                    "produced %(listing)r instead of %(expected)r" % expected)
        self.assertFalse(failures, "Found the following failures:\n%s" %
                         '\n'.join(failures))

    def test_double_check_trailing_delimiter(self):
        # Test AccountBroker.list_containers_iter for an
        # account that has an odd container with a trailing delimiter
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        broker.put_container('a', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-a', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-a-a', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-a-b', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a-b', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        # NB: ord(".") == ord("-") + 1
        broker.put_container('a.', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('a.b', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b-a', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b-b', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('c', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        listing = broker.list_containers_iter(15, None, None, None, None)
        self.assertEqual([row[0] for row in listing],
                         ['a', 'a-', 'a-a', 'a-a-a', 'a-a-b', 'a-b', 'a.',
                          'a.b', 'b', 'b-a', 'b-b', 'c'])
        listing = broker.list_containers_iter(15, None, None, '', '-')
        self.assertEqual([row[0] for row in listing],
                         ['a', 'a-', 'a.', 'a.b', 'b', 'b-', 'c'])
        listing = broker.list_containers_iter(15, None, None, 'a-', '-')
        self.assertEqual([row[0] for row in listing],
                         ['a-', 'a-a', 'a-a-', 'a-b'])
        listing = broker.list_containers_iter(15, None, None, 'b-', '-')
        self.assertEqual([row[0] for row in listing], ['b-a', 'b-b'])

    def test_chexor(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        put_ts = self.ts()
        delete_ts = Timestamp(0)
        timestamps = [self.ts() for _ in range(3)]
        broker.initialize(put_ts.internal)
        broker.put_container('a', timestamps[0].internal,
                             delete_ts.internal, 0, 0,
                             POLICIES.default.idx)
        broker.put_container('b', timestamps[1].internal,
                             delete_ts.internal, 0, 0,
                             POLICIES.default.idx)
        text = '%s-%s' % ('a', "%s-%s-%s-%s" % (
               timestamps[0].internal, delete_ts.internal, 0, 0))
        hasha = md5(text.encode('ascii'), usedforsecurity=False).digest()
        text = '%s-%s' % ('b', "%s-%s-%s-%s" % (
               timestamps[1].internal, delete_ts.internal, 0, 0))
        hashb = md5(text.encode('ascii'), usedforsecurity=False).digest()
        hashc = ''.join(('%02x' % (a ^ b)
                         for a, b in zip(hasha, hashb)))
        self.assertEqual(broker.get_info()['hash'], hashc)
        broker.put_container('b', timestamps[2].internal,
                             delete_ts.internal, 0, 0,
                             POLICIES.default.idx)
        text = '%s-%s' % ('b', "%s-%s-%s-%s" % (
               timestamps[2].internal, delete_ts.internal, 0, 0))
        hashb = md5(text.encode('ascii'), usedforsecurity=False).digest()
        hashc = ''.join(('%02x' % (a ^ b)
                         for a, b in zip(hasha, hashb)))
        self.assertEqual(broker.get_info()['hash'], hashc)

    def test_chexor_includes_mpu_bytes_when_non_zero(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)
        ts = self.ts().internal
        broker.put_container('a', ts, '0', 3, 30, POLICIES.default.idx)
        broker.put_container('b', ts, '0', 3, 30, POLICIES.default.idx,
                             bytes_in_parts=20, bytes_in_manifests=5)
        # only the row with mpu bytes has them in its hash value; a row
        # without them hashes as it did before the columns existed
        self.assertEqual(
            chexor_hash(('a', '%s-0-3-30' % ts),
                        ('b', '%s-0-3-30-20-5' % ts)),
            broker.get_info()['hash'])

    def test_chexor_mpu_bytes_individually_non_zero(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)
        ts = self.ts().internal
        broker.put_container('a', ts, '0', 3, 30, POLICIES.default.idx,
                             bytes_in_parts=20)
        broker.put_container('b', ts, '0', 3, 30, POLICIES.default.idx,
                             bytes_in_manifests=5)
        self.assertEqual(
            chexor_hash(('a', '%s-0-3-30-20-0' % ts),
                        ('b', '%s-0-3-30-0-5' % ts)),
            broker.get_info()['hash'])

    def test_chexor_replacing_mpu_row_cancels_previous_value(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)
        ts1, ts2, ts3 = (self.ts().internal for _ in range(3))
        broker.put_container('a', ts1, '0', 3, 30, POLICIES.default.idx,
                             bytes_in_parts=20, bytes_in_manifests=5)
        # replace with different mpu bytes, then with none
        broker.put_container('a', ts2, '0', 3, 30, POLICIES.default.idx,
                             bytes_in_parts=21, bytes_in_manifests=6)
        self.assertEqual(chexor_hash(('a', '%s-0-3-30-21-6' % ts2)),
                         broker.get_info()['hash'])
        broker.put_container('a', ts3, '0', 3, 30, POLICIES.default.idx)
        self.assertEqual(chexor_hash(('a', '%s-0-3-30' % ts3)),
                         broker.get_info()['hash'])

    def test_chexor_tombstone_with_mpu_bytes_cancels(self):
        # mpu byte counts are accumulated for a tombstoned container row just
        # like bytes_used, so they must also be part of its hash value
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)
        ts_put, ts_delete, ts_new = (self.ts().internal for _ in range(3))
        broker.put_container('a', ts_put, ts_delete, 0, 0,
                             POLICIES.default.idx,
                             bytes_in_parts=20, bytes_in_manifests=5)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(1, conn.execute(
                'SELECT deleted FROM container WHERE name = "a"'
            ).fetchone()[0])
        self.assertEqual(chexor_hash(('a', '%s-%s-0-0-20-5'
                                      % (ts_put, ts_delete))),
                         broker.get_info()['hash'])
        # replacing the tombstone cancels its contribution exactly
        broker.put_container('a', ts_new, '0', 3, 30, POLICIES.default.idx)
        self.assertEqual(
            chexor_hash(('a', '%s-%s-3-30' % (ts_new, ts_delete))),
            broker.get_info()['hash'])

    def test_chexor_merge_items_legacy_item_replaces_mpu_row(self):
        # an item from a legacy peer has no mpu bytes, so replacing an mpu row
        # with it returns the row to the legacy hash value
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)
        ts1, ts2 = self.ts().internal, self.ts().internal
        broker.put_container('c', ts1, '0', 3, 30, POLICIES.default.idx,
                             bytes_in_parts=20, bytes_in_manifests=5)
        broker._commit_puts()
        self.assertEqual(chexor_hash(('c', '%s-0-3-30-20-5' % ts1)),
                         broker.get_info()['hash'])
        broker.merge_items([{'name': 'c',
                             'put_timestamp': ts2,
                             'delete_timestamp': '0',
                             'object_count': 4,
                             'bytes_used': 40,
                             'deleted': 0,
                             'storage_policy_index': POLICIES.default.idx}])
        info = broker.get_info()
        self.assertEqual(0, info['bytes_in_parts'])
        self.assertEqual(0, info['bytes_in_manifests'])
        self.assertEqual(chexor_hash(('c', '%s-0-4-40' % ts2)), info['hash'])

    def test_merge_items(self):
        broker1 = AccountBroker(self.get_db_path(), account='a')
        broker1.initialize(Timestamp('1').internal)
        broker2 = AccountBroker(self.get_db_path(), account='a')
        broker2.initialize(Timestamp('1').internal)
        broker1.put_container('a', Timestamp(1).internal, 0, 0, 0,
                              POLICIES.default.idx)
        broker1.put_container('b', Timestamp(2).internal, 0, 0, 0,
                              POLICIES.default.idx)
        id = broker1.get_info()['id']
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(len(items), 2)
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_container('c', Timestamp(3).internal, 0, 0, 0,
                              POLICIES.default.idx)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(len(items), 3)
        self.assertEqual(['a', 'b', 'c'],
                         sorted([rec['name'] for rec in items]))

    def test_merge_items_overwrite_unicode(self):
        snowman = u'\N{SNOWMAN}'
        broker1 = AccountBroker(self.get_db_path(), account='a')
        broker1.initialize(Timestamp('1').internal, 0)
        id1 = broker1.get_info()['id']
        broker2 = AccountBroker(self.get_db_path(), account='a')
        broker2.initialize(Timestamp('1').internal, 0)
        broker1.put_container(snowman, Timestamp(2).internal, 0, 1, 100,
                              POLICIES.default.idx)
        broker1.put_container('b', Timestamp(3).internal, 0, 0, 0,
                              POLICIES.default.idx)
        broker2.merge_items(json.loads(json.dumps(broker1.get_items_since(
            broker2.get_sync(id1), 1000))), id1)
        broker1.put_container(snowman, Timestamp(4).internal, 0, 2, 200,
                              POLICIES.default.idx)
        broker2.merge_items(json.loads(json.dumps(broker1.get_items_since(
            broker2.get_sync(id1), 1000))), id1)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(['b', snowman],
                         sorted([rec['name'] for rec in items]))
        items_by_name = dict((rec['name'], rec) for rec in items)

        self.assertEqual(items_by_name[snowman]['object_count'], 2)
        self.assertEqual(items_by_name[snowman]['bytes_used'], 200)

        self.assertEqual(items_by_name['b']['object_count'], 0)
        self.assertEqual(items_by_name['b']['bytes_used'], 0)

    @with_tempdir
    def test_load_old_pending_puts(self, tempdir):
        # pending puts from pre-storage-policy account brokers won't contain
        # the storage policy index
        broker_path = os.path.join(tempdir, 'test-load-old.db')
        broker = AccountBroker(broker_path, account='real')
        broker.initialize(Timestamp(1).internal)
        with open(broker.pending_file, 'a+b') as pending:
            pending.write(b':')
            pending.write(base64.b64encode(pickle.dumps(
                # name, put_timestamp, delete_timestamp, object_count,
                # bytes_used, deleted
                ('oldcon', Timestamp(200).internal,
                 Timestamp(0).internal,
                 896, 9216695, 0))))

        broker._commit_puts()
        with broker.get() as conn:
            results = list(conn.execute('''
                SELECT name, storage_policy_index FROM container
            '''))
        self.assertEqual(len(results), 1)
        self.assertEqual(dict(results[0]),
                         {'name': 'oldcon', 'storage_policy_index': 0})

    @with_tempdir
    def test_get_info_stale_read_ok(self, tempdir):
        # test getting a stale read from the db
        broker_path = os.path.join(tempdir, 'test-load-old.db')

        def mock_commit_puts():
            raise sqlite3.OperationalError('unable to open database file')

        broker = AccountBroker(broker_path, account='real',
                               stale_reads_ok=True)
        broker.initialize(Timestamp(1).internal)
        with open(broker.pending_file, 'a+b') as pending:
            pending.write(b':')
            pending.write(base64.b64encode(pickle.dumps(
                # name, put_timestamp, delete_timestamp, object_count,
                # bytes_used, deleted
                ('oldcon', Timestamp(200).internal,
                 Timestamp(0).internal,
                 896, 9216695, 0))))

        broker._commit_puts = mock_commit_puts
        broker.get_info()

    @with_tempdir
    def test_get_info_no_stale_reads(self, tempdir):
        broker_path = os.path.join(tempdir, 'test-load-old.db')

        def mock_commit_puts():
            raise sqlite3.OperationalError('unable to open database file')

        broker = AccountBroker(broker_path, account='real',
                               stale_reads_ok=False)
        broker.initialize(Timestamp(1).internal)
        with open(broker.pending_file, 'a+b') as pending:
            pending.write(b':')
            pending.write(base64.b64encode(pickle.dumps(
                # name, put_timestamp, delete_timestamp, object_count,
                # bytes_used, deleted
                ('oldcon', Timestamp(200).internal,
                 Timestamp(0).internal,
                 896, 9216695, 0))))

        broker._commit_puts = mock_commit_puts

        with self.assertRaises(sqlite3.OperationalError) as exc_context:
            broker.get_info()
        self.assertIn('unable to open database file',
                      str(exc_context.exception))

    @patch_policies([StoragePolicy(0, 'zero', False),
                     StoragePolicy(1, 'one', True),
                     StoragePolicy(2, 'two', False),
                     StoragePolicy(3, 'three', False)])
    def test_get_policy_stats(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)
        # check empty policy_stats
        self.assertTrue(broker.empty())
        policy_stats = broker.get_policy_stats()
        self.assertEqual(policy_stats, {})

        # add some empty containers
        for policy in POLICIES:
            container_name = 'c-%s' % policy.name
            put_timestamp = self.ts()
            broker.put_container(container_name,
                                 put_timestamp.internal, 0,
                                 0, 0,
                                 policy.idx)
            policy_stats = broker.get_policy_stats()
            stats = policy_stats[policy.idx]
            if 'container_count' in stats:
                self.assertEqual(stats['container_count'], 1)
            self.assertEqual(stats['object_count'], 0)
            self.assertEqual(stats['bytes_used'], 0)

        # update the containers object & byte count
        for policy in POLICIES:
            container_name = 'c-%s' % policy.name
            put_timestamp = self.ts()
            count = policy.idx * 100  # good as any integer
            broker.put_container(container_name,
                                 put_timestamp.internal, 0,
                                 count, count,
                                 policy.idx)

            policy_stats = broker.get_policy_stats()
            stats = policy_stats[policy.idx]
            if 'container_count' in stats:
                self.assertEqual(stats['container_count'], 1)
            self.assertEqual(stats['object_count'], count)
            self.assertEqual(stats['bytes_used'], count)

        # check all the policy_stats at once
        for policy_index, stats in policy_stats.items():
            policy = POLICIES[policy_index]
            count = policy.idx * 100  # coupled with policy for test
            if 'container_count' in stats:
                self.assertEqual(stats['container_count'], 1)
            self.assertEqual(stats['object_count'], count)
            self.assertEqual(stats['bytes_used'], count)

        # now delete the containers one by one
        for policy in POLICIES:
            container_name = 'c-%s' % policy.name
            delete_timestamp = self.ts()
            broker.put_container(container_name,
                                 0, delete_timestamp.internal,
                                 0, 0,
                                 policy.idx)

            policy_stats = broker.get_policy_stats()
            stats = policy_stats[policy.idx]
            if 'container_count' in stats:
                self.assertEqual(stats['container_count'], 0)
            self.assertEqual(stats['object_count'], 0)
            self.assertEqual(stats['bytes_used'], 0)

    @patch_policies([StoragePolicy(0, 'zero', False),
                     StoragePolicy(1, 'one', True)])
    def test_policy_stats_tracking(self):
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)

        # policy 0
        broker.put_container('con1', self.ts().internal, 0, 12, 2798641, 0)
        broker.put_container('con1', self.ts().internal, 0, 13, 8156441, 0)
        # policy 1
        broker.put_container('con2', self.ts().internal, 0, 7, 5751991, 1)
        broker.put_container('con2', self.ts().internal, 0, 8, 6085379, 1)

        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 2)
        if 'container_count' in stats[0]:
            self.assertEqual(stats[0]['container_count'], 1)
        self.assertEqual(stats[0]['object_count'], 13)
        self.assertEqual(stats[0]['bytes_used'], 8156441)
        if 'container_count' in stats[1]:
            self.assertEqual(stats[1]['container_count'], 1)
        self.assertEqual(stats[1]['object_count'], 8)
        self.assertEqual(stats[1]['bytes_used'], 6085379)

        # Break encapsulation here to make sure that there's only 2 rows in
        # the stats table. It's possible that there could be 4 rows (one per
        # put_container) but that they came out in the right order so that
        # get_policy_stats() collapsed them down to the right number. To prove
        # that's not so, we have to go peek at the broker's internals.
        with broker.get() as conn:
            nrows = conn.execute(
                "SELECT COUNT(*) FROM policy_stat").fetchall()[0][0]
        self.assertEqual(nrows, 2)

    @with_tempdir
    def test_newid(self, tempdir):
        # test DatabaseBroker.newid
        db_path = os.path.join(
            tempdir, "d1234", 'accounts', 'part', 'suffix', 'hsh')
        os.makedirs(db_path)
        broker = AccountBroker(os.path.join(db_path, 'my.db'),
                               account='a')
        broker.initialize(Timestamp('1').internal, 0)
        id = broker.get_info()['id']
        broker.newid('someid')
        self.assertNotEqual(id, broker.get_info()['id'])
        # ends in the device name (from the path) unless it's an old
        # container with just a uuid4 (tested in legacy broker
        # tests e.g *BeforeMetaData)
        if len(id) > 36:
            self.assertTrue(id.endswith('d1234'))
        # But the newid'ed version will now have the decide
        self.assertTrue(broker.get_info()['id'].endswith('d1234'))

        # if we move the broker (happens after an rsync)
        new_db_path = os.path.join(
            tempdir, "d5678", 'containers', 'part', 'suffix', 'hsh')
        os.makedirs(new_db_path)
        shutil.copy(os.path.join(db_path, 'my.db'),
                    os.path.join(new_db_path, 'my.db'))

        new_broker = AccountBroker(os.path.join(new_db_path, 'my.db'),
                                   account='a')
        new_broker.newid(id)
        # ends in the device name (from the path)
        self.assertFalse(new_broker.get_info()['id'].endswith('d1234'))
        self.assertTrue(new_broker.get_info()['id'].endswith('d5678'))

    def _create_db(self, account='a'):
        db_file = self.get_db_path()
        b = AccountBroker(db_file, account=account)
        b.initialize(Timestamp('1').internal)
        return db_file

    def test_path_caches_account(self):
        db_file = self._create_db()
        b = AccountBroker(db_file)
        self.assertIsNone(b.account)
        called = {'n': 0}

        orig_get_info = AccountBroker.get_info

        def capture_get_info(self):
            called['n'] += 1
            return orig_get_info(self)

        self.assertEqual(0, called['n'])
        with mock.patch.object(AccountBroker, 'get_info', capture_get_info):
            self.assertEqual('a', b.path)
            # accessing path will call get_info!
            self.assertEqual(1, called['n'])
            # and populate the account attribute
            self.assertEqual('a', b.account)
            # it's cached
            for i in range(3):
                self.assertEqual('a', b.path)
                self.assertEqual('a', b.account)
        # we only call get_info once
        self.assertEqual(1, called['n'])

    def test_path_uses_account_kwag(self):
        db_file = self._create_db()
        b = AccountBroker(db_file, account='a')
        called = {'n': 0}

        orig_get_info = AccountBroker.get_info

        def capture_get_info(self):
            called['n'] += 1
            return orig_get_info(self)

        self.assertEqual(0, called['n'])
        with mock.patch.object(AccountBroker, 'get_info', capture_get_info):
            self.assertEqual('a', b.path)
        # we never call get_info
        self.assertEqual(0, called['n'])

    def test_path_on_uninitialized_broker(self):
        db_file = self.get_db_path()
        b = AccountBroker(db_file)
        self.assertEqual(b.db_file, db_file)
        with self.assertRaises(DatabaseConnectionError) as ctx:
            getattr(b, 'path')
        self.assertIn("DB doesn't exist", str(ctx.exception))
        self.assertIn(db_file, str(ctx.exception))
        with self.assertRaises(ValueError) as ctx:
            b.initialize(self.ts())
        self.assertEqual(
            'Attempting to create a new database with no account set',
            str(ctx.exception))
        b.account = 'a'
        self.assertEqual('a', b.path)


def prespi_AccountBroker_initialize(self, conn, put_timestamp, **kwargs):
    """
    The AccountBroker initialize() function before we added the
    policy stat table.  Used by test_policy_table_creation() to
    make sure that the AccountBroker will correctly add the table
    for cases where the DB existed before the policy support was added.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if not self.account:
        raise ValueError(
            'Attempting to create a new database with no account set')
    self.create_container_table(conn)
    self.create_account_stat_table(conn, put_timestamp)


def premetadata_create_account_stat_table(self, conn, put_timestamp):
    """
    Copied from AccountBroker before the metadata column was
    added; used for testing with TestAccountBrokerBeforeMetadata.

    Create account_stat table which is specific to the account DB.

    Update 2026: Timestamp has evolved, so use NormalTimestamp in legacy tests
    because it better represents the behaviour of Timestamp c. 2013.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    conn.executescript('''
        CREATE TABLE account_stat (
            account TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            container_count INTEGER,
            object_count INTEGER DEFAULT 0,
            bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0'
        );

        INSERT INTO account_stat (container_count) VALUES (0);
    ''')

    conn.execute('''
        UPDATE account_stat SET account = ?, created_at = ?, id = ?,
               put_timestamp = ?
        ''', (self.account, LegacyTimestamp.now().internal, str(uuid4()),
              put_timestamp))


class TestCommonAccountBroker(test_db.TestExampleBroker):

    broker_class = AccountBroker
    server_type = 'account'

    def setUp(self):
        super(TestCommonAccountBroker, self).setUp()
        self.policy = random.choice(list(POLICIES))

    def put_item(self, broker, timestamp):
        broker.put_container('test', timestamp, 0, 0, 0,
                             int(self.policy))

    def delete_item(self, broker, timestamp):
        broker.put_container('test', 0, timestamp, 0, 0,
                             int(self.policy))


class TestAccountBrokerBeforeMetadata(TestAccountBroker):
    """
    Tests for AccountBroker against databases created before
    the metadata column was added.
    """

    def setUp(self):
        super(TestAccountBroker, self).setUp()
        self._imported_create_account_stat_table = \
            AccountBroker.create_account_stat_table
        AccountBroker.create_account_stat_table = \
            premetadata_create_account_stat_table
        # the policy_stat table is seeded from account_stat, so it must be
        # created with a schema of the same vintage
        self._imported_create_policy_stat_table = \
            AccountBroker.create_policy_stat_table
        AccountBroker.create_policy_stat_table = \
            pre_mpu_stats_create_policy_stat_table
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT metadata FROM account_stat')
            except BaseException as err:
                exc = err
        self.assertIn('no such column: metadata', str(exc))

    def tearDown(self):
        AccountBroker.create_account_stat_table = \
            self._imported_create_account_stat_table
        AccountBroker.create_policy_stat_table = \
            self._imported_create_policy_stat_table
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        with broker.get() as conn:
            conn.execute('SELECT metadata FROM account_stat')
        super(TestAccountBrokerBeforeMetadata, self).tearDown()


def prespi_create_container_table(self, conn):
    """
    Copied from AccountBroker before the sstoage_policy_index column was
    added; used for testing with TestAccountBrokerBeforeSPI.

    Create container table which is specific to the account DB.

    :param conn: DB connection object
    """
    conn.executescript("""
        CREATE TABLE container (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            put_timestamp TEXT,
            delete_timestamp TEXT,
            object_count INTEGER,
            bytes_used INTEGER,
            deleted INTEGER DEFAULT 0
        );

        CREATE INDEX ix_container_deleted_name ON
            container (deleted, name);

        CREATE TRIGGER container_insert AFTER INSERT ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count + (1 - new.deleted),
                object_count = object_count + new.object_count,
                bytes_used = bytes_used + new.bytes_used,
                hash = chexor(hash, new.name,
                              new.put_timestamp || '-' ||
                                new.delete_timestamp || '-' ||
                                new.object_count || '-' || new.bytes_used);
        END;

        CREATE TRIGGER container_update BEFORE UPDATE ON container
        BEGIN
            SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
        END;


        CREATE TRIGGER container_delete AFTER DELETE ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count - (1 - old.deleted),
                object_count = object_count - old.object_count,
                bytes_used = bytes_used - old.bytes_used,
                hash = chexor(hash, old.name,
                              old.put_timestamp || '-' ||
                                old.delete_timestamp || '-' ||
                                old.object_count || '-' || old.bytes_used);
        END;
    """)


class TestAccountBrokerBeforeSPI(TestAccountBroker):
    """
    Tests for AccountBroker against databases created before
    the storage_policy_index column was added.
    """

    def setUp(self):
        super(TestAccountBrokerBeforeSPI, self).setUp()
        self._imported_create_container_table = \
            AccountBroker.create_container_table
        AccountBroker.create_container_table = \
            prespi_create_container_table
        self._imported_initialize = AccountBroker._initialize
        AccountBroker._initialize = prespi_AccountBroker_initialize
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT storage_policy_index FROM container')
            except BaseException as err:
                exc = err
        self.assertIn('no such column: storage_policy_index', str(exc))
        with broker.get() as conn:
            try:
                conn.execute('SELECT * FROM policy_stat')
            except sqlite3.OperationalError as err:
                self.assertIn('no such table: policy_stat', str(err))
            else:
                self.fail('database created with policy_stat table')

    def tearDown(self):
        AccountBroker.create_container_table = \
            self._imported_create_container_table
        AccountBroker._initialize = self._imported_initialize
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        with broker.get() as conn:
            conn.execute('SELECT storage_policy_index FROM container')
        super(TestAccountBrokerBeforeSPI, self).tearDown()

    @with_tempdir
    def test_policy_table_migration(self, tempdir):
        db_path = os.path.join(tempdir, 'account.db')

        # first init an acct DB without the policy_stat table present
        broker = AccountBroker(db_path, account='a')
        broker.initialize(Timestamp('1').internal)

        def confirm_no_table():
            with broker.get() as conn:
                try:
                    conn.execute('''
                        SELECT * FROM policy_stat
                        ''').fetchone()[0]
                except sqlite3.OperationalError as err:
                    # confirm that the table really isn't there
                    self.assertIn('no such table: policy_stat', str(err))
                else:
                    self.fail('broker did not raise sqlite3.OperationalError '
                              'trying to select from policy_stat table!')

        confirm_no_table()

        # make sure we can HEAD this thing w/o the table
        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 0)

        confirm_no_table()

        # now do a PUT to create the table
        broker.put_container('o', Timestamp.now().internal, 0, 0, 0,
                             POLICIES.default.idx)
        broker._commit_puts_stale_ok()

        # now confirm that the table was created
        with broker.get() as conn:
            conn.execute('SELECT * FROM policy_stat')

        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 1)

    @with_tempdir
    def test_policy_table_migration_in_get_policy_stats(self, tempdir):
        db_path = os.path.join(tempdir, 'account.db')

        # first init an acct DB without the policy_stat table present
        broker = AccountBroker(db_path, account='a')
        broker.initialize(Timestamp('1').internal)

        # And manually add some container records for the default policy
        with broker.get() as conn:
            conn.execute('''
                INSERT INTO container (
                    name, put_timestamp, delete_timestamp,
                    object_count, bytes_used
                ) VALUES (
                    'c', '%s', '0', 0, 0
                )''' % Timestamp.now().internal).fetchone()
            conn.commit()

        def confirm_no_table():
            with broker.get() as conn:
                try:
                    conn.execute('''
                        SELECT * FROM policy_stat
                        ''').fetchone()[0]
                except sqlite3.OperationalError as err:
                    # confirm that the table really isn't there
                    self.assertIn('no such table: policy_stat', str(err))
                else:
                    self.fail('broker did not raise sqlite3.OperationalError '
                              'trying to select from policy_stat table!')

        confirm_no_table()

        # make sure we can HEAD this thing w/o the table
        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 0)

        confirm_no_table()

        # but if we pass in do_migrations (like in the auditor), it comes in
        stats = broker.get_policy_stats(do_migrations=True)
        self.assertEqual(len(stats), 1)

        # double check that it really exists
        with broker.get() as conn:
            conn.execute('SELECT * FROM policy_stat')

    @patch_policies
    @with_tempdir
    def test_container_table_migration(self, tempdir):
        db_path = os.path.join(tempdir, 'account.db')

        # first init an acct DB without the policy_stat table present
        broker = AccountBroker(db_path, account='a')
        broker.initialize(Timestamp('1').internal)
        with broker.get() as conn:
            try:
                conn.execute('''
                    SELECT storage_policy_index FROM container
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                # confirm that the table doesn't have this column
                self.assertIn('no such column: storage_policy_index',
                              str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from storage_policy_index '
                          'from container table!')

        # manually insert an existing row to avoid migration
        timestamp = Timestamp.now().internal
        with broker.get() as conn:
            conn.execute('''
                INSERT INTO container (name, put_timestamp,
                    delete_timestamp, object_count, bytes_used,
                    deleted)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('test_name', timestamp, 0, 1, 2, 0))
            conn.commit()

        # make sure we can iter containers without the migration
        for c in broker.list_containers_iter(1, None, None, None, None):
            self.assertEqual(c, ('test_name', 1, 2, timestamp, 0, 0))

        # stats table is mysteriously empty...
        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 0)

        # now do a PUT with a different value for storage_policy_index
        # which will update the DB schema as well as update policy_stats
        # for legacy containers in the DB (those without an SPI)
        other_policy = [p for p in POLICIES if p.idx != 0][0]
        broker.put_container('test_second', Timestamp.now().internal,
                             0, 3, 4, other_policy.idx)
        broker._commit_puts_stale_ok()

        with broker.get() as conn:
            rows = conn.execute('''
                SELECT name, storage_policy_index FROM container
                ''').fetchall()
            for row in rows:
                if row[0] == 'test_name':
                    self.assertEqual(row[1], 0)
                else:
                    self.assertEqual(row[1], other_policy.idx)

        # we should have stats for both containers
        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 2)
        if 'container_count' in stats[0]:
            self.assertEqual(stats[0]['container_count'], 1)
        self.assertEqual(stats[0]['object_count'], 1)
        self.assertEqual(stats[0]['bytes_used'], 2)
        if 'container_count' in stats[1]:
            self.assertEqual(stats[1]['container_count'], 1)
        self.assertEqual(stats[1]['object_count'], 3)
        self.assertEqual(stats[1]['bytes_used'], 4)

        # now lets delete a container and make sure policy_stats is OK
        with broker.get() as conn:
            conn.execute('''
                DELETE FROM container WHERE name = ?
                ''', ('test_name',))
            conn.commit()
        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 2)
        if 'container_count' in stats[0]:
            self.assertEqual(stats[0]['container_count'], 0)
        self.assertEqual(stats[0]['object_count'], 0)
        self.assertEqual(stats[0]['bytes_used'], 0)
        if 'container_count' in stats[1]:
            self.assertEqual(stats[1]['container_count'], 1)
        self.assertEqual(stats[1]['object_count'], 3)
        self.assertEqual(stats[1]['bytes_used'], 4)

    @with_tempdir
    def test_half_upgraded_database(self, tempdir):
        db_path = os.path.join(tempdir, 'account.db')

        broker = AccountBroker(db_path, account='a')
        broker.initialize(self.ts().internal)

        self.assertTrue(broker.empty())

        # add a container (to pending file)
        broker.put_container('c', self.ts().internal, 0, 0, 0,
                             POLICIES.default.idx)

        real_get = broker.get
        called = []

        class ExpectedError(Exception):
            '''Expected error to be raised during the test'''

        @contextmanager
        def mock_get():
            with real_get() as conn:

                class MockConn(object):
                    def __init__(self, real_conn):
                        self.real_conn = real_conn

                    @property
                    def cursor(self):
                        return self.real_conn.cursor

                    @property
                    def execute(self):
                        return self.real_conn.execute

                    @property
                    def rollback(self):
                        return self.real_conn.rollback

                    def executescript(self, script):
                        called.append(script)
                        raise ExpectedError('kaboom!')

                yield MockConn(conn)

        broker.get = mock_get

        try:
            broker._commit_puts()
        except ExpectedError:
            pass
        else:
            self.fail('mock exception was not raised')

        self.assertEqual(len(called), 1)
        self.assertIn('CREATE TABLE policy_stat', called[0])

        # nothing was committed
        broker = AccountBroker(db_path, account='a')
        with broker.get() as conn:
            try:
                conn.execute('SELECT * FROM policy_stat')
            except sqlite3.OperationalError as err:
                self.assertIn('no such table: policy_stat', str(err))
            else:
                self.fail('half upgraded database!')
            container_count = conn.execute(
                'SELECT count(*) FROM container').fetchone()[0]
            self.assertEqual(container_count, 0)

        # try again to commit puts
        self.assertFalse(broker.empty())

        # full migration successful
        with broker.get() as conn:
            conn.execute('SELECT * FROM policy_stat')
            conn.execute('SELECT storage_policy_index FROM container')

    @with_tempdir
    def test_pre_storage_policy_replication(self, tempdir):
        # make and two account database "replicas"
        old_broker = AccountBroker(os.path.join(tempdir, 'old_account.db'),
                                   account='a')
        old_broker.initialize(self.ts().internal)
        new_broker = AccountBroker(os.path.join(tempdir, 'new_account.db'),
                                   account='a')
        new_broker.initialize(self.ts().internal)
        timestamp = self.ts().internal

        # manually insert an existing row to avoid migration for old database
        with old_broker.get() as conn:
            conn.execute('''
                INSERT INTO container (name, put_timestamp,
                    delete_timestamp, object_count, bytes_used,
                    deleted)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('test_name', timestamp, 0, 1, 2, 0))
            conn.commit()

        # get replication info and rows form old database
        info = old_broker.get_info()
        rows = old_broker.get_items_since(0, 10)

        # "send" replication rows to new database
        new_broker.merge_items(rows, info['id'])

        # make sure "test_name" container in new database
        self.assertEqual(new_broker.get_info()['container_count'], 1)
        for c in new_broker.list_containers_iter(1, None, None, None, None):
            self.assertEqual(c, ('test_name', 1, 2, timestamp, 0, 0))

        # full migration successful
        with new_broker.get() as conn:
            conn.execute('SELECT * FROM policy_stat')
            conn.execute('SELECT storage_policy_index FROM container')


def pre_track_containers_create_policy_stat(self, conn):
    """
    Copied from AccountBroker before the container_count column was
    added.
    Create policy_stat table which is specific to the account DB.
    Not a part of Pluggable Back-ends, internal to the baseline code.

    :param conn: DB connection object
    """
    conn.executescript("""
        CREATE TABLE policy_stat (
            storage_policy_index INTEGER PRIMARY KEY,
            object_count INTEGER DEFAULT 0,
            bytes_used INTEGER DEFAULT 0
        );
        INSERT OR IGNORE INTO policy_stat (
            storage_policy_index, object_count, bytes_used
        )
        SELECT 0, object_count, bytes_used
        FROM account_stat
        WHERE container_count > 0;
    """)


def pre_track_containers_create_container_table(self, conn):
    """
    Copied from AccountBroker before the container_count column was
    added (using old stat trigger script)
    Create container table which is specific to the account DB.

    :param conn: DB connection object
    """
    # revert to old trigger script to support one of the tests
    OLD_POLICY_STAT_TRIGGER_SCRIPT = """
        CREATE TRIGGER container_insert_ps AFTER INSERT ON container
        BEGIN
            INSERT OR IGNORE INTO policy_stat
                (storage_policy_index, object_count, bytes_used)
                VALUES (new.storage_policy_index, 0, 0);
            UPDATE policy_stat
            SET object_count = object_count + new.object_count,
                bytes_used = bytes_used + new.bytes_used
            WHERE storage_policy_index = new.storage_policy_index;
        END;
        CREATE TRIGGER container_delete_ps AFTER DELETE ON container
        BEGIN
            UPDATE policy_stat
            SET object_count = object_count - old.object_count,
                bytes_used = bytes_used - old.bytes_used
            WHERE storage_policy_index = old.storage_policy_index;
        END;

    """
    conn.executescript("""
        CREATE TABLE container (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            put_timestamp TEXT,
            delete_timestamp TEXT,
            object_count INTEGER,
            bytes_used INTEGER,
            deleted INTEGER DEFAULT 0,
            storage_policy_index INTEGER DEFAULT 0
        );

        CREATE INDEX ix_container_deleted_name ON
            container (deleted, name);

        CREATE TRIGGER container_insert AFTER INSERT ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count + (1 - new.deleted),
                object_count = object_count + new.object_count,
                bytes_used = bytes_used + new.bytes_used,
                hash = chexor(hash, new.name,
                              new.put_timestamp || '-' ||
                                new.delete_timestamp || '-' ||
                                new.object_count || '-' || new.bytes_used);
        END;

        CREATE TRIGGER container_update BEFORE UPDATE ON container
        BEGIN
            SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
        END;


        CREATE TRIGGER container_delete AFTER DELETE ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count - (1 - old.deleted),
                object_count = object_count - old.object_count,
                bytes_used = bytes_used - old.bytes_used,
                hash = chexor(hash, old.name,
                              old.put_timestamp || '-' ||
                                old.delete_timestamp || '-' ||
                                old.object_count || '-' || old.bytes_used);
        END;
    """ + OLD_POLICY_STAT_TRIGGER_SCRIPT)


class AccountBrokerPreTrackContainerCountSetup(test_db.TestDbBase):
    def assertUnmigrated(self, broker):
        with broker.get() as conn:
            try:
                conn.execute('''
                    SELECT container_count FROM policy_stat
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                # confirm that the column really isn't there
                self.assertIn('no such column: container_count', str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select container_count from policy_stat!')

    def setUp(self):
        super(AccountBrokerPreTrackContainerCountSetup, self).setUp()
        # use old version of policy_stat
        self._imported_create_policy_stat_table = \
            AccountBroker.create_policy_stat_table
        AccountBroker.create_policy_stat_table = \
            pre_track_containers_create_policy_stat
        # use old container table so we use old trigger for
        # updating policy_stat
        self._imported_create_container_table = \
            AccountBroker.create_container_table
        AccountBroker.create_container_table = \
            pre_track_containers_create_container_table

        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        self.assertUnmigrated(broker)

        self.db_path = os.path.join(self.testdir, 'sda', 'accounts',
                                    '0', '0', '0', 'test.db')
        self.broker = AccountBroker(self.get_db_path(), account='a')
        self.broker.initialize(self.ts().internal)

        # Common sanity-check that our starting, pre-migration state correctly
        # does not have the container_count column.
        self.assertUnmigrated(self.broker)

    def tearDown(self):
        self.restore_account_broker()

        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(Timestamp('1').internal)
        with broker.get() as conn:
            conn.execute('SELECT container_count FROM policy_stat')
        super(AccountBrokerPreTrackContainerCountSetup, self).tearDown()

    def restore_account_broker(self):
        AccountBroker.create_policy_stat_table = \
            self._imported_create_policy_stat_table
        AccountBroker.create_container_table = \
            self._imported_create_container_table


@patch_policies([StoragePolicy(0, 'zero', False),
                 StoragePolicy(1, 'one', True),
                 StoragePolicy(2, 'two', False),
                 StoragePolicy(3, 'three', False)])
class TestAccountBrokerBeforePerPolicyContainerTrack(
        AccountBrokerPreTrackContainerCountSetup, TestAccountBroker):
    """
    Tests for AccountBroker against databases created before
    the container_count column was added to the policy_stat table.
    """

    def test_policy_table_cont_count_do_migrations(self):
        # add a few containers
        num_containers = 8
        policies = itertools.cycle(POLICIES)
        per_policy_container_counts = defaultdict(int)

        # add a few container entries
        for i in range(num_containers):
            name = 'test-container-%02d' % i
            policy = next(policies)
            self.broker.put_container(name, self.ts().internal,
                                      0, 0, 0, int(policy))
            per_policy_container_counts[int(policy)] += 1

        total_container_count = self.broker.get_info()['container_count']
        self.assertEqual(total_container_count, num_containers)

        # put_container migrated the db: the stat columns are migrated
        # together, so container_count arrives along with the mpu columns
        with self.broker.get() as conn:
            conn.execute('SELECT container_count FROM policy_stat')

        policy_stats = self.broker.get_policy_stats()
        self.assertEqual(len(policy_stats), len(per_policy_container_counts))
        for policy_index, stats in policy_stats.items():
            self.assertEqual(stats['object_count'], 0)
            self.assertEqual(stats['bytes_used'], 0)
            self.assertEqual(stats['container_count'],
                             per_policy_container_counts[policy_index])

        # asking for the migration again changes nothing
        policy_stats = self.broker.get_policy_stats(do_migrations=True)
        self.assertEqual(len(policy_stats), len(per_policy_container_counts))
        for policy_index, stats in policy_stats.items():
            self.assertEqual(stats['object_count'], 0)
            self.assertEqual(stats['bytes_used'], 0)
            self.assertEqual(stats['container_count'],
                             per_policy_container_counts[policy_index])

    def test_policy_table_cont_count_update_get_stats(self):
        # add a few container entries
        ts_zero = Timestamp(0)
        for policy in POLICIES:
            for i in range(0, policy.idx + 1):
                container_name = 'c%s_0' % policy.idx
                self.broker.put_container('c%s_%s' % (policy.idx, i),
                                          ts_zero.internal, ts_zero.internal,
                                          0, 0, policy.idx)
        # _commit_puts_stale_ok() called by get_policy_stats()

        # calling get_policy_stats() with do_migrations will alter the table
        # and populate it based on what's in the container table now
        stats = self.broker.get_policy_stats(do_migrations=True)

        # now confirm that the column was created
        with self.broker.get() as conn:
            conn.execute('SELECT container_count FROM policy_stat')

        # confirm stats reporting back correctly
        self.assertEqual(len(stats), 4)
        for policy in POLICIES:
            self.assertEqual(stats[policy.idx]['container_count'],
                             policy.idx + 1)

        # now delete one from each policy and check the stats
        with self.broker.get() as conn:
            for policy in POLICIES:
                container_name = 'c%s_0' % policy.idx
                conn.execute('''
                        DELETE FROM container
                        WHERE name = ?
                        ''', (container_name,))
            conn.commit()
        stats = self.broker.get_policy_stats()
        self.assertEqual(len(stats), 4)
        for policy in POLICIES:
            self.assertEqual(stats[policy.idx]['container_count'],
                             policy.idx)

        # now put them back and make sure things are still cool
        for policy in POLICIES:
            container_name = 'c%s_0' % policy.idx
            self.broker.put_container(
                container_name, ts_zero.internal, ts_zero.internal, 0, 0,
                policy.idx)
        # _commit_puts_stale_ok() called by get_policy_stats()

        # confirm stats reporting back correctly
        stats = self.broker.get_policy_stats()
        self.assertEqual(len(stats), 4)
        for policy in POLICIES:
            self.assertEqual(stats[policy.idx]['container_count'],
                             policy.idx + 1)

    def test_per_policy_cont_count_backfill_preserves_mpu_bytes(self):
        # a db partially migrated by an older implementation has the mpu
        # columns, holding non-zero values, but no container_count. The
        # backfill rewrites whole policy_stat rows so must not reset them.
        with self.broker.get() as conn:
            # insert the row first: the old trigger would replace the whole
            # policy_stat row, resetting the mpu columns
            conn.execute('''
                INSERT INTO container (name, put_timestamp, delete_timestamp,
                    object_count, bytes_used, deleted, storage_policy_index)
                VALUES ('c', ?, '0', 1, 10, 0, 0)
            ''', (self.ts().internal,))
            conn.executescript('''
                ALTER TABLE policy_stat
                ADD COLUMN bytes_in_parts INTEGER DEFAULT 0;
                ALTER TABLE policy_stat
                ADD COLUMN bytes_in_manifests INTEGER DEFAULT 0;
                UPDATE policy_stat SET bytes_in_parts = 40,
                                       bytes_in_manifests = 5;
            ''')
            self.broker._migrate_container_table(conn)
        with self.broker.get() as conn:
            row = conn.execute('''
                SELECT container_count, bytes_in_parts, bytes_in_manifests
                FROM policy_stat WHERE storage_policy_index = 0
            ''').fetchone()
        self.assertEqual((1, 40, 5), tuple(row))

    def test_per_policy_cont_count_backfilled_from_existing_rows(self):
        # rows that pre-date the container_count column are counted when the
        # column is added; insert them directly so that the migration is not
        # triggered until every row is in place
        per_policy_container_counts = defaultdict(int)
        policies = itertools.cycle(POLICIES)
        with self.broker.get() as conn:
            for i in range(8):
                policy = next(policies)
                deleted = i % 4 == 0
                conn.execute('''
                    INSERT INTO container (name, put_timestamp,
                        delete_timestamp, object_count, bytes_used, deleted,
                        storage_policy_index)
                    VALUES (?, ?, ?, 0, 0, ?, ?)
                ''', ('test-container-%02d' % i, self.ts().internal, '0',
                      int(deleted), int(policy)))
                if not deleted:
                    per_policy_container_counts[int(policy)] += 1
            conn.commit()
        self.assertUnmigrated(self.broker)

        policy_stats = self.broker.get_policy_stats(do_migrations=True)
        self.assertEqual(
            per_policy_container_counts,
            {policy_index: stats['container_count']
             for policy_index, stats in policy_stats.items()
             if stats['container_count']})

    def test_per_policy_cont_count_migration_with_deleted(self):
        num_containers = 15
        policies = itertools.cycle(POLICIES)
        container_policy_map = {}

        # add a few container entries
        for i in range(num_containers):
            name = 'test-container-%02d' % i
            policy = next(policies)
            self.broker.put_container(name, self.ts().internal,
                                      0, 0, 0, int(policy))
            # keep track of stub container policies
            container_policy_map[name] = policy

        # delete about half of the containers
        for i in range(0, num_containers, 2):
            name = 'test-container-%02d' % i
            policy = container_policy_map[name]
            self.broker.put_container(name, 0, self.ts().internal,
                                      0, 0, int(policy))

        total_container_count = self.broker.get_info()['container_count']
        self.assertEqual(total_container_count, num_containers // 2)

        # trigger migration
        policy_info = self.broker.get_policy_stats(do_migrations=True)
        self.assertEqual(len(policy_info), min(num_containers, len(POLICIES)))
        policy_container_count = sum(p['container_count'] for p in
                                     policy_info.values())
        self.assertEqual(total_container_count, policy_container_count)

    def test_per_policy_cont_count_migration_with_single_policy(self):
        num_containers = 100

        with patch_policies(legacy_only=True):
            policy = POLICIES[0]
            # add a few container entries
            for i in range(num_containers):
                name = 'test-container-%02d' % i
                self.broker.put_container(name, self.ts().internal,
                                          0, 0, 0, int(policy))
            # delete about half of the containers
            for i in range(0, num_containers, 2):
                name = 'test-container-%02d' % i
                self.broker.put_container(name, 0, self.ts().internal,
                                          0, 0, int(policy))

            total_container_count = self.broker.get_info()['container_count']
            # trigger migration
            policy_info = self.broker.get_policy_stats(do_migrations=True)

        self.assertEqual(total_container_count, num_containers / 2)

        self.assertEqual(len(policy_info), 1)
        policy_container_count = sum(p['container_count'] for p in
                                     policy_info.values())
        self.assertEqual(total_container_count, policy_container_count)

    def test_per_policy_cont_count_migration_impossible(self):
        with patch_policies(legacy_only=True):
            # add a container for the legacy policy
            policy = POLICIES[0]
            self.broker.put_container('test-legacy-container',
                                      self.ts().internal, 0, 0, 0,
                                      int(policy))

            # now create an impossible situation by adding a container for a
            # policy index that doesn't exist
            non_existent_policy_index = int(policy) + 1
            self.broker.put_container('test-non-existent-policy',
                                      self.ts().internal, 0, 0, 0,
                                      non_existent_policy_index)

            total_container_count = self.broker.get_info()['container_count']

            # trigger migration
            policy_info = self.broker.get_policy_stats(do_migrations=True)

        self.assertEqual(total_container_count, 2)
        self.assertEqual(len(policy_info), 2)
        for policy_stat in policy_info.values():
            self.assertEqual(policy_stat['container_count'], 1)


class TestAccountBrokerMigration(test_db.TestDbBase):
    """
    Tests for the single pass that migrates an older db to the current schema.
    """
    server_type = 'account'

    def _make_prespi_broker(self):
        # a db from before the storage_policy_index column was added: no
        # policy_stat table, and no mpu byte counts in any table
        replacements = {
            'create_container_table': prespi_create_container_table,
            'create_account_stat_table': premetadata_create_account_stat_table,
            '_initialize': prespi_AccountBroker_initialize,
        }
        originals = {name: getattr(AccountBroker, name)
                     for name in replacements}
        for name, replacement in replacements.items():
            setattr(AccountBroker, name, replacement)
        try:
            broker = AccountBroker(self.db_path, account='a')
            broker.initialize(Timestamp('1').internal)
        finally:
            for name, original in originals.items():
                setattr(AccountBroker, name, original)
        return AccountBroker(self.db_path, account='a')

    def _get_columns(self, broker, table):
        with broker.get() as conn:
            return {row[1] for row in
                    conn.execute('PRAGMA table_info(%s)' % table)}

    def test_prespi_db_is_migrated_in_one_pass(self):
        broker = self._make_prespi_broker()
        # a container row that pre-dates every migration
        with broker.get() as conn:
            conn.execute('''
                INSERT INTO container (name, put_timestamp, delete_timestamp,
                    object_count, bytes_used, deleted)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('c1', Timestamp(1).internal, '0', 2, 30, 0))
            conn.commit()

        # any write path that needs a column it hasn't got migrates the lot
        broker.merge_items([{'name': 'c2',
                             'put_timestamp': Timestamp(2).internal,
                             'delete_timestamp': '0',
                             'object_count': 1,
                             'bytes_used': 10,
                             'deleted': 0,
                             'storage_policy_index': 0,
                             'bytes_in_parts': 6,
                             'bytes_in_manifests': 4}])

        for table in ('container', 'account_stat', 'policy_stat'):
            self.assertLessEqual({'bytes_in_parts', 'bytes_in_manifests'},
                                 self._get_columns(broker, table),
                                 table)
        self.assertIn('storage_policy_index',
                      self._get_columns(broker, 'container'))

        # policy_stat was seeded from the migrated account_stat, so it counts
        # the container row that pre-dated it as well as the merged one
        self.assertEqual({0: {'container_count': 2,
                              'object_count': 3,
                              'bytes_used': 40,
                              'bytes_in_parts': 6,
                              'bytes_in_manifests': 4}},
                         broker.get_policy_stats())
        info = broker.get_info()
        self.assertEqual(2, info['container_count'])
        self.assertEqual(6, info['bytes_in_parts'])
        self.assertEqual(4, info['bytes_in_manifests'])

        # the reinstalled triggers maintain the new columns
        broker.put_container('c3', Timestamp(3).internal, '0', 1, 10, 0,
                             bytes_in_parts=5, bytes_in_manifests=2)
        broker._commit_puts()
        self.assertEqual(11, broker.get_info()['bytes_in_parts'])
        self.assertEqual(
            11, broker.get_policy_stats()[0]['bytes_in_parts'])

    def test_migrate_create_policy_stat_table_fail(self):
        broker = self._make_prespi_broker()
        with mock.patch.object(
                broker, 'create_policy_stat_table',
                side_effect=sqlite3.OperationalError('foobar')):
            with broker.get() as conn:
                self.assertRaisesRegex(
                    sqlite3.OperationalError, '.*foobar.*',
                    broker._migrate_container_table,
                    conn=conn)

    def test_migrate_tolerates_racing_create_policy_stat_table(self):
        # another process may create the table between us finding it missing
        # and us creating it
        broker = self._make_prespi_broker()
        real_create = broker.create_policy_stat_table

        def racing_create(conn):
            real_create(conn)
            raise sqlite3.OperationalError(
                'table policy_stat already exists')

        with mock.patch.object(broker, 'create_policy_stat_table',
                               side_effect=racing_create):
            with broker.get() as conn:
                broker._migrate_container_table(conn)
        self.assertLessEqual({'container_count', 'bytes_in_parts',
                              'bytes_in_manifests'},
                             self._get_columns(broker, 'policy_stat'))

    def _get_triggers(self, broker):
        with broker.get() as conn:
            return {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'trigger'")}

    def test_migrate_is_atomic_when_trigger_swap_fails(self):
        broker = self._make_prespi_broker()
        orig_container_columns = self._get_columns(broker, 'container')
        orig_triggers = self._get_triggers(broker)
        self.assertTrue(orig_triggers)

        # inject a failure after the container ALTERs and trigger drops have
        # been executed within the script
        with mock.patch('swift.account.backend.POLICY_STAT_TRIGGER_SCRIPT',
                        'CREATE TRIGGER;'):
            with broker.get() as conn:
                with self.assertRaises(sqlite3.OperationalError):
                    broker._migrate_container_table(conn)
                self.assertFalse(conn.in_transaction)

        # the container columns were rolled back with the triggers, so the
        # db is still detectably un-migrated, and still has its old triggers
        self.assertEqual(orig_container_columns,
                         self._get_columns(broker, 'container'))
        self.assertEqual(orig_triggers, self._get_triggers(broker))

        # a later attempt completes the migration
        broker.merge_items([{'name': 'c1',
                             'put_timestamp': Timestamp(1).internal,
                             'delete_timestamp': '0',
                             'object_count': 1,
                             'bytes_used': 10,
                             'deleted': 0,
                             'storage_policy_index': 0,
                             'bytes_in_parts': 6,
                             'bytes_in_manifests': 4}])
        self.assertLessEqual({'storage_policy_index', 'bytes_in_parts',
                              'bytes_in_manifests'},
                             self._get_columns(broker, 'container'))
        self.assertEqual(6, broker.get_info()['bytes_in_parts'])
        self.assertEqual(
            4, broker.get_policy_stats()[0]['bytes_in_manifests'])

    def test_migrate_tolerates_racing_container_column_add(self):
        # another process may add the container columns between us finding
        # them missing and us beginning the transaction
        broker = self._make_prespi_broker()
        other_broker = AccountBroker(self.db_path, account='a')
        real_get_columns = broker._get_table_columns
        raced = []

        def racing_get_columns(conn, table):
            columns = real_get_columns(conn, table)
            if table == 'container' and not raced:
                raced.append(True)
                with other_broker.get() as other_conn:
                    other_broker._migrate_container_table(other_conn)
            return columns

        with mock.patch.object(broker, '_get_table_columns',
                               side_effect=racing_get_columns):
            with broker.get() as conn:
                broker._migrate_container_table(conn)
        self.assertLessEqual({'storage_policy_index', 'bytes_in_parts',
                              'bytes_in_manifests'},
                             self._get_columns(broker, 'container'))
        self.assertIn('container_insert', self._get_triggers(broker))


def pre_mpu_stats_create_container_table(self, conn):
    """
    Copied from AccountBroker before the bytes_in_parts and
    bytes_in_manifests columns were added (using the old trigger scripts).
    """
    OLD_POLICY_STAT_TRIGGER_SCRIPT = """
        CREATE TRIGGER container_insert_ps AFTER INSERT ON container
        BEGIN
            INSERT OR IGNORE INTO policy_stat
                (storage_policy_index, container_count, object_count,
                 bytes_used)
                VALUES (new.storage_policy_index, 0, 0, 0);
            UPDATE policy_stat
            SET container_count = container_count + (1 - new.deleted),
                object_count = object_count + new.object_count,
                bytes_used = bytes_used + new.bytes_used
            WHERE storage_policy_index = new.storage_policy_index;
        END;
        CREATE TRIGGER container_delete_ps AFTER DELETE ON container
        BEGIN
            UPDATE policy_stat
            SET container_count = container_count - (1 - old.deleted),
                object_count = object_count - old.object_count,
                bytes_used = bytes_used - old.bytes_used
            WHERE storage_policy_index = old.storage_policy_index;
        END;

    """
    conn.executescript("""
        CREATE TABLE container (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            put_timestamp TEXT,
            delete_timestamp TEXT,
            object_count INTEGER,
            bytes_used INTEGER,
            deleted INTEGER DEFAULT 0,
            storage_policy_index INTEGER DEFAULT 0
        );

        CREATE INDEX ix_container_deleted_name ON
            container (deleted, name);

        CREATE TRIGGER container_insert AFTER INSERT ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count + (1 - new.deleted),
                object_count = object_count + new.object_count,
                bytes_used = bytes_used + new.bytes_used,
                hash = chexor(hash, new.name,
                              new.put_timestamp || '-' ||
                                new.delete_timestamp || '-' ||
                                new.object_count || '-' || new.bytes_used);
        END;

        CREATE TRIGGER container_update BEFORE UPDATE ON container
        BEGIN
            SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
        END;

        CREATE TRIGGER container_delete AFTER DELETE ON container
        BEGIN
            UPDATE account_stat
            SET container_count = container_count - (1 - old.deleted),
                object_count = object_count - old.object_count,
                bytes_used = bytes_used - old.bytes_used,
                hash = chexor(hash, old.name,
                              old.put_timestamp || '-' ||
                                old.delete_timestamp || '-' ||
                                old.object_count || '-' || old.bytes_used);
        END;
    """ + OLD_POLICY_STAT_TRIGGER_SCRIPT)


def pre_mpu_stats_create_account_stat_table(self, conn, put_timestamp):
    """
    Copied from AccountBroker before the bytes_in_parts and
    bytes_in_manifests columns were added to the account_stat table.
    """
    conn.executescript("""
        CREATE TABLE account_stat (
            account TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            container_count INTEGER,
            object_count INTEGER DEFAULT 0,
            bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0',
            metadata TEXT DEFAULT ''
        );

        INSERT INTO account_stat (container_count) VALUES (0);
    """)
    conn.execute('''
        UPDATE account_stat SET account = ?, created_at = ?, id = ?,
               put_timestamp = ?, status_changed_at = ?
        ''', (self.account, LegacyTimestamp.now().internal,
              str(uuid4()), put_timestamp, put_timestamp))


def pre_mpu_stats_create_policy_stat_table(self, conn):
    """
    Copied from AccountBroker before the bytes_in_parts and
    bytes_in_manifests columns were added to the policy_stat table.
    """
    conn.executescript("""
        CREATE TABLE policy_stat (
            storage_policy_index INTEGER PRIMARY KEY,
            container_count INTEGER DEFAULT 0,
            object_count INTEGER DEFAULT 0,
            bytes_used INTEGER DEFAULT 0
        );
        INSERT OR IGNORE INTO policy_stat (
            storage_policy_index, container_count, object_count, bytes_used
        )
        SELECT 0, container_count, object_count, bytes_used
        FROM account_stat
        WHERE container_count > 0;
    """)


class AccountBrokerPreMpuStatsSetup(test_db.TestDbBase):
    """
    Sets up an account db created before the bytes_in_parts and
    bytes_in_manifests columns were added.
    """
    def setUp(self):
        super(AccountBrokerPreMpuStatsSetup, self).setUp()
        self._originals = {}
        for name, replacement in (
                ('create_container_table',
                 pre_mpu_stats_create_container_table),
                ('create_account_stat_table',
                 pre_mpu_stats_create_account_stat_table),
                ('create_policy_stat_table',
                 pre_mpu_stats_create_policy_stat_table)):
            self._originals[name] = getattr(AccountBroker, name)
            setattr(AccountBroker, name, replacement)
        self.broker = AccountBroker(self.get_db_path(), account='a')
        self.broker.initialize(self.ts().internal)
        self.assertUnmigrated(self.broker)

    def tearDown(self):
        self.restore_account_broker()
        # sanity check: a fresh broker has the columns
        broker = AccountBroker(self.get_db_path(), account='a')
        broker.initialize(self.ts().internal)
        self.assertMigrated(broker)
        super(AccountBrokerPreMpuStatsSetup, self).tearDown()

    def restore_account_broker(self):
        for name, original in self._originals.items():
            setattr(AccountBroker, name, original)

    def _assert_columns(self, broker, tables, present):
        with broker.get() as conn:
            for table in tables:
                columns = {row[1] for row in
                           conn.execute('PRAGMA table_info(%s)' % table)}
                for column in ('bytes_in_parts', 'bytes_in_manifests'):
                    if present:
                        self.assertIn(column, columns,
                                      '%s.%s' % (table, column))
                    else:
                        self.assertNotIn(column, columns,
                                         '%s.%s' % (table, column))

    def assertUnmigrated(self, broker):
        self._assert_columns(
            broker, ('container', 'account_stat', 'policy_stat'), False)

    def assertMigrated(self, broker):
        self._assert_columns(
            broker, ('container', 'account_stat', 'policy_stat'), True)


@patch_policies([StoragePolicy(0, 'zero', True),
                 StoragePolicy(1, 'one', False)])
class TestAccountBrokerBeforeMpuStats(AccountBrokerPreMpuStatsSetup,
                                      TestAccountBroker):
    """
    Tests for AccountBroker against databases created before the
    bytes_in_parts and bytes_in_manifests columns were added.
    """
    def _insert_container_row(self, name, object_count=10, bytes_used=100,
                              storage_policy_index=0):
        # insert directly, bypassing put_container, so that merge_items does
        # not migrate the db
        with self.broker.get() as conn:
            conn.execute('''
                INSERT INTO container (name, put_timestamp, delete_timestamp,
                    object_count, bytes_used, deleted, storage_policy_index)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (name, self.ts().internal, '0', object_count, bytes_used, 0,
                  storage_policy_index))
            conn.commit()

    def test_hash_of_pre_existing_rows_survives_migration(self):
        # a row hashed in by an unmigrated db's triggers must be cancelled
        # out by the migrated triggers when the row is replaced
        self._insert_container_row('c')
        self._insert_container_row('d')
        self.assertUnmigrated(self.broker)
        before = self.broker.get_info()['hash']
        ts = self.ts().internal
        # put_container triggers the migration and then replaces 'c'
        self.broker.put_container('c', ts, '0', 7, 70, 0)
        self.broker._commit_puts()
        self.assertMigrated(self.broker)
        with self.broker.get() as conn:
            rows = {r['name']: r for r in conn.execute(
                'SELECT * FROM container')}
        self.assertNotEqual(before, self.broker.get_info()['hash'])
        self.assertEqual(
            chexor_hash(('c', '%s-0-7-70' % ts),
                        ('d', '%s-0-10-100' % rows['d']['put_timestamp'])),
            self.broker.get_info()['hash'])

    def test_hash_matches_unmigrated_peer_for_rows_without_mpu_bytes(self):
        # an unmigrated and a migrated replica that hold the same rows,
        # none of which has mpu bytes, have equal hashes
        ts = self.ts().internal
        peer = AccountBroker(self.get_db_path() + '.peer', account='a')
        peer.initialize(self.ts().internal)
        self.assertUnmigrated(peer)
        with peer.get() as conn:
            conn.execute('''
                INSERT INTO container (name, put_timestamp, delete_timestamp,
                    object_count, bytes_used, deleted, storage_policy_index)
                VALUES ('c', ?, '0', 3, 30, 0, 0)
            ''', (ts,))
            conn.commit()
        self.broker.put_container('c', ts, '0', 3, 30, 0)
        self.broker._commit_puts()
        self.assertMigrated(self.broker)
        self.assertUnmigrated(peer)
        self.assertEqual(peer.get_info()['hash'],
                         self.broker.get_info()['hash'])

    def test_get_info_unmigrated(self):
        # reading stats must not migrate the db
        self._insert_container_row('c')
        info = self.broker.get_info()
        self.assertUnmigrated(self.broker)
        self.assertEqual(10, info['object_count'])
        self.assertEqual(100, info['bytes_used'])
        self.assertEqual(0, info['bytes_in_parts'])
        self.assertEqual(0, info['bytes_in_manifests'])

    def test_get_policy_stats_unmigrated(self):
        self._insert_container_row('c')
        stats = self.broker.get_policy_stats()
        self.assertUnmigrated(self.broker)
        self.assertEqual({0: {'container_count': 1,
                              'object_count': 10,
                              'bytes_used': 100}}, stats)

    def test_get_policy_stats_do_migrations(self):
        self._insert_container_row('c')
        stats = self.broker.get_policy_stats(do_migrations=True)
        self.assertEqual({0: {'container_count': 1,
                              'object_count': 10,
                              'bytes_used': 100,
                              'bytes_in_parts': 0,
                              'bytes_in_manifests': 0}}, stats)
        self.assertMigrated(self.broker)

        # ... and the reinstalled triggers work against the migrated schema
        self.broker.put_container('c2', self.ts().internal, 0, 1, 50, 0,
                                  bytes_in_parts=40, bytes_in_manifests=2)
        info = self.broker.get_info()
        self.assertEqual(40, info['bytes_in_parts'])
        self.assertEqual(2, info['bytes_in_manifests'])

    def test_put_container_migrates(self):
        self.broker.put_container('c', self.ts().internal, 0, 10, 100, 0,
                                  bytes_in_parts=60, bytes_in_manifests=3)
        # merge_items triggers the migration
        info = self.broker.get_info()
        self.assertMigrated(self.broker)
        self.assertEqual(10, info['object_count'])
        self.assertEqual(100, info['bytes_used'])
        self.assertEqual(60, info['bytes_in_parts'])
        self.assertEqual(3, info['bytes_in_manifests'])
        self.assertEqual({0: {'container_count': 1,
                              'object_count': 10,
                              'bytes_used': 100,
                              'bytes_in_parts': 60,
                              'bytes_in_manifests': 3}},
                         self.broker.get_policy_stats())

    def test_merge_items_from_legacy_peer(self):
        # verify that if an item dict from an legacy peer has no mpu byte
        # counts then these default to zero so that they are consistent with
        # bytes_used from the peer that IS merged
        self.broker.put_container('c', self.ts().internal, 0, 10, 100, 0,
                                  bytes_in_parts=60, bytes_in_manifests=3)
        self.broker._commit_puts()
        self.assertMigrated(self.broker)

        self.broker.merge_items([{'name': 'c',
                                  'put_timestamp': self.ts().internal,
                                  'delete_timestamp': '0',
                                  'object_count': 12,
                                  'bytes_used': 22,
                                  'deleted': 0,
                                  'storage_policy_index': 0}])
        info = self.broker.get_info()
        self.assertEqual(22, info['bytes_used'])
        # the locally known values are set to zero due to being absent in the
        # remote's update
        self.assertEqual(0, info['bytes_in_parts'])
        self.assertEqual(0, info['bytes_in_manifests'])

    def test_merge_items_with_none_does_not_poison_stats(self):
        # a None must never reach the db: n + NULL is NULL in sqlite, and the
        # account_stat accumulator would never recover
        self.broker.merge_items([{'name': 'c',
                                  'put_timestamp': self.ts().internal,
                                  'delete_timestamp': '0',
                                  'object_count': 10,
                                  'bytes_used': 100,
                                  'deleted': 0,
                                  'storage_policy_index': 0,
                                  'bytes_in_parts': None,
                                  'bytes_in_manifests': None}])
        info = self.broker.get_info()
        self.assertEqual(0, info['bytes_in_parts'])
        self.assertEqual(0, info['bytes_in_manifests'])
        # the stored row must be zero rather than NULL: this row is what gets
        # replicated on, and an older peer's triggers do not coalesce
        with self.broker.get() as conn:
            row = conn.execute('SELECT bytes_in_parts, bytes_in_manifests '
                               'FROM container WHERE name = "c"').fetchone()
        self.assertEqual((0, 0), tuple(row))

        # a subsequent real value still accumulates correctly
        self.broker.merge_items([{'name': 'c2',
                                  'put_timestamp': self.ts().internal,
                                  'delete_timestamp': '0',
                                  'object_count': 1,
                                  'bytes_used': 50,
                                  'deleted': 0,
                                  'storage_policy_index': 0,
                                  'bytes_in_parts': 50,
                                  'bytes_in_manifests': 5}])
        info = self.broker.get_info()
        self.assertEqual(50, info['bytes_in_parts'])
        self.assertEqual(5, info['bytes_in_manifests'])

    def test_commit_puts_load_legacy_pending_entries(self):
        # entries written by older versions have 6 or 7 elements
        item_list = []
        self.broker._commit_puts_load(
            item_list, ('c6', '1', '0', 10, 100, 0))
        self.broker._commit_puts_load(
            item_list, ('c7', '1', '0', 10, 100, 0, 1))
        self.broker._commit_puts_load(
            item_list, ('c9', '1', '0', 10, 100, 0, 1, 60, 3))
        self.assertEqual(['c6', 'c7', 'c9'],
                         [item['name'] for item in item_list])
        self.assertEqual([0, 1, 1],
                         [item['storage_policy_index'] for item in item_list])
        # legacy entries leave the counts unknown rather than claiming zero
        self.assertNotIn('bytes_in_parts', item_list[0])
        self.assertNotIn('bytes_in_parts', item_list[1])
        self.assertEqual(60, item_list[2]['bytes_in_parts'])
        self.assertEqual(3, item_list[2]['bytes_in_manifests'])
