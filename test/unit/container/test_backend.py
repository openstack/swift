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

""" Tests for swift.container.backend """

import os
import hashlib
import unittest
from time import sleep, time
from uuid import uuid4
import itertools
import random
from collections import defaultdict
from contextlib import contextmanager
import sqlite3
import pickle
import json

from swift.container.backend import ContainerBroker
from swift.common.utils import Timestamp
from swift.common.storage_policy import POLICIES

import mock

from test.unit import patch_policies, with_tempdir
from test.unit.common.test_db import TestExampleBroker


class TestContainerBroker(unittest.TestCase):
    """Tests for ContainerBroker"""

    def test_creation(self):
        # Test ContainerBroker.__init__
        broker = ContainerBroker(':memory:', account='a', container='c')
        self.assertEqual(broker.db_file, ':memory:')
        broker.initialize(Timestamp('1').internal, 0)
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)

    @patch_policies
    def test_storage_policy_property(self):
        ts = (Timestamp(t).internal for t in itertools.count(int(time())))
        for policy in POLICIES:
            broker = ContainerBroker(':memory:', account='a',
                                     container='policy_%s' % policy.name)
            broker.initialize(ts.next(), policy.idx)
            with broker.get() as conn:
                try:
                    conn.execute('''SELECT storage_policy_index
                                    FROM container_stat''')
                except Exception:
                    is_migrated = False
                else:
                    is_migrated = True
            if not is_migrated:
                # pre spi tests don't set policy on initialize
                broker.set_storage_policy_index(policy.idx)
            self.assertEqual(policy.idx, broker.storage_policy_index)
            # make sure it's cached
            with mock.patch.object(broker, 'get'):
                self.assertEqual(policy.idx, broker.storage_policy_index)

    def test_exception(self):
        # Test ContainerBroker throwing a conn away after
        # unhandled exception
        first_conn = None
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        with broker.get() as conn:
            first_conn = conn
        try:
            with broker.get() as conn:
                self.assertEquals(first_conn, conn)
                raise Exception('OMG')
        except Exception:
            pass
        self.assert_(broker.conn is None)

    def test_empty(self):
        # Test ContainerBroker.empty
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        self.assert_(broker.empty())
        broker.put_object('o', Timestamp(time()).internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        self.assert_(not broker.empty())
        sleep(.00001)
        broker.delete_object('o', Timestamp(time()).internal)
        self.assert_(broker.empty())

    def test_reclaim(self):
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('o', Timestamp(time()).internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(Timestamp(time() - 999).internal, time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', Timestamp(time()).internal)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(Timestamp(time() - 999).internal, time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(Timestamp(time()).internal, time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test the return values of reclaim()
        broker.put_object('w', Timestamp(time()).internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('x', Timestamp(time()).internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('y', Timestamp(time()).internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('z', Timestamp(time()).internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        # Test before deletion
        broker.reclaim(Timestamp(time()).internal, time())
        broker.delete_db(Timestamp(time()).internal)

    def test_get_info_is_deleted(self):
        start = int(time())
        ts = (Timestamp(t).internal for t in itertools.count(start))
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        # create it
        broker.initialize(ts.next(), POLICIES.default.idx)
        info, is_deleted = broker.get_info_is_deleted()
        self.assertEqual(is_deleted, broker.is_deleted())
        self.assertEqual(is_deleted, False)  # sanity
        self.assertEqual(info, broker.get_info())
        self.assertEqual(info['put_timestamp'], Timestamp(start).internal)
        self.assert_(Timestamp(info['created_at']) >= start)
        self.assertEqual(info['delete_timestamp'], '0')
        if self.__class__ in (TestContainerBrokerBeforeMetadata,
                              TestContainerBrokerBeforeXSync,
                              TestContainerBrokerBeforeSPI):
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(info['status_changed_at'],
                             Timestamp(start).internal)

        # delete it
        delete_timestamp = ts.next()
        broker.delete_db(delete_timestamp)
        info, is_deleted = broker.get_info_is_deleted()
        self.assertEqual(is_deleted, True)  # sanity
        self.assertEqual(is_deleted, broker.is_deleted())
        self.assertEqual(info, broker.get_info())
        self.assertEqual(info['put_timestamp'], Timestamp(start).internal)
        self.assert_(Timestamp(info['created_at']) >= start)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertEqual(info['status_changed_at'], delete_timestamp)

        # bring back to life
        broker.put_object('obj', ts.next(), 0, 'text/plain', 'etag',
                          storage_policy_index=broker.storage_policy_index)
        info, is_deleted = broker.get_info_is_deleted()
        self.assertEqual(is_deleted, False)  # sanity
        self.assertEqual(is_deleted, broker.is_deleted())
        self.assertEqual(info, broker.get_info())
        self.assertEqual(info['put_timestamp'], Timestamp(start).internal)
        self.assert_(Timestamp(info['created_at']) >= start)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertEqual(info['status_changed_at'], delete_timestamp)

    def test_delete_object(self):
        # Test ContainerBroker.delete_object
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('o', Timestamp(time()).internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', Timestamp(time()).internal)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_put_object(self):
        # Test ContainerBroker.put_object
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)

        # Create initial object
        timestamp = Timestamp(time()).internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Reput same event
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put new event
        sleep(.00001)
        timestamp = Timestamp(time()).internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 124,
                          'application/x-test',
                          'aa0749bacbc79ec65fe206943d8fe449')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put old event
        otimestamp = Timestamp(float(Timestamp(timestamp)) - 1).internal
        broker.put_object('"{<object \'&\' name>}"', otimestamp, 124,
                          'application/x-test',
                          'aa0749bacbc79ec65fe206943d8fe449')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put old delete event
        dtimestamp = Timestamp(float(Timestamp(timestamp)) - 1).internal
        broker.put_object('"{<object \'&\' name>}"', dtimestamp, 0, '', '',
                          deleted=1)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put new delete event
        sleep(.00001)
        timestamp = Timestamp(time()).internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 0, '', '',
                          deleted=1)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 1)

        # Put new event
        sleep(.00001)
        timestamp = Timestamp(time()).internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # We'll use this later
        sleep(.0001)
        in_between_timestamp = Timestamp(time()).internal

        # New post event
        sleep(.0001)
        previous_timestamp = timestamp
        timestamp = Timestamp(time()).internal
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0],
                previous_timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put event from after last put but before last post
        timestamp = in_between_timestamp
        broker.put_object('"{<object \'&\' name>}"', timestamp, 456,
                          'application/x-test3',
                          '6af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT size FROM object").fetchone()[0], 456)
            self.assertEquals(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test3')
            self.assertEquals(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '6af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEquals(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

    @patch_policies
    def test_put_misplaced_object_does_not_effect_container_stats(self):
        policy = random.choice(list(POLICIES))
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time())))
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        broker.initialize(ts.next(), policy.idx)
        # migration tests may not honor policy on initialize
        if isinstance(self, ContainerBrokerMigrationMixin):
            real_storage_policy_index = \
                broker.get_info()['storage_policy_index']
            policy = filter(lambda p: p.idx == real_storage_policy_index,
                            POLICIES)[0]
        broker.put_object('correct_o', ts.next(), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=policy.idx)
        info = broker.get_info()
        self.assertEqual(1, info['object_count'])
        self.assertEqual(123, info['bytes_used'])
        other_policy = random.choice([p for p in POLICIES
                                      if p is not policy])
        broker.put_object('wrong_o', ts.next(), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=other_policy.idx)
        self.assertEqual(1, info['object_count'])
        self.assertEqual(123, info['bytes_used'])

    @patch_policies
    def test_has_multiple_policies(self):
        policy = random.choice(list(POLICIES))
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time())))
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        broker.initialize(ts.next(), policy.idx)
        # migration tests may not honor policy on initialize
        if isinstance(self, ContainerBrokerMigrationMixin):
            real_storage_policy_index = \
                broker.get_info()['storage_policy_index']
            policy = filter(lambda p: p.idx == real_storage_policy_index,
                            POLICIES)[0]
        broker.put_object('correct_o', ts.next(), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=policy.idx)
        self.assertFalse(broker.has_multiple_policies())
        other_policy = [p for p in POLICIES if p is not policy][0]
        broker.put_object('wrong_o', ts.next(), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=other_policy.idx)
        self.assert_(broker.has_multiple_policies())

    @patch_policies
    def test_get_policy_info(self):
        policy = random.choice(list(POLICIES))
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time())))
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        broker.initialize(ts.next(), policy.idx)
        # migration tests may not honor policy on initialize
        if isinstance(self, ContainerBrokerMigrationMixin):
            real_storage_policy_index = \
                broker.get_info()['storage_policy_index']
            policy = filter(lambda p: p.idx == real_storage_policy_index,
                            POLICIES)[0]
        policy_stats = broker.get_policy_stats()
        expected = {policy.idx: {'bytes_used': 0, 'object_count': 0}}
        self.assertEqual(policy_stats, expected)

        # add an object
        broker.put_object('correct_o', ts.next(), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=policy.idx)
        policy_stats = broker.get_policy_stats()
        expected = {policy.idx: {'bytes_used': 123, 'object_count': 1}}
        self.assertEqual(policy_stats, expected)

        # add a misplaced object
        other_policy = random.choice([p for p in POLICIES
                                      if p is not policy])
        broker.put_object('wrong_o', ts.next(), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=other_policy.idx)
        policy_stats = broker.get_policy_stats()
        expected = {
            policy.idx: {'bytes_used': 123, 'object_count': 1},
            other_policy.idx: {'bytes_used': 123, 'object_count': 1},
        }
        self.assertEqual(policy_stats, expected)

    def test_policy_stat_tracking(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time())))
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        broker.initialize(ts.next(), POLICIES.default.idx)
        stats = defaultdict(dict)

        iters = 100
        for i in range(iters):
            policy_index = random.randint(0, iters * 0.1)
            name = 'object-%s' % random.randint(0, iters * 0.1)
            size = random.randint(0, iters)
            broker.put_object(name, ts.next(), size, 'text/plain',
                              '5af83e3196bf99f440f31f2e1a6c9afe',
                              storage_policy_index=policy_index)
            # track the size of the latest timestamp put for each object
            # in each storage policy
            stats[policy_index][name] = size
        policy_stats = broker.get_policy_stats()
        # if no objects were added for the default policy we still
        # expect an entry for the default policy in the returned info
        # because the database was initialized with that storage policy
        # - but it must be empty.
        if POLICIES.default.idx not in stats:
            default_stats = policy_stats.pop(POLICIES.default.idx)
            expected = {'object_count': 0, 'bytes_used': 0}
            self.assertEqual(default_stats, expected)
        self.assertEqual(len(policy_stats), len(stats))
        for policy_index, stat in policy_stats.items():
            self.assertEqual(stat['object_count'], len(stats[policy_index]))
            self.assertEqual(stat['bytes_used'],
                             sum(stats[policy_index].values()))

    def test_initialize_container_broker_in_default(self):
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')

        # initialize with no storage_policy_index argument
        broker.initialize(Timestamp(1).internal)

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['container'], 'test2')
        self.assertEquals(info['hash'], '00000000000000000000000000000000')
        self.assertEqual(info['put_timestamp'], Timestamp(1).internal)
        self.assertEqual(info['delete_timestamp'], '0')

        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)

        policy_stats = broker.get_policy_stats()

        # Act as policy-0
        self.assertTrue(0 in policy_stats)
        self.assertEquals(policy_stats[0]['bytes_used'], 0)
        self.assertEquals(policy_stats[0]['object_count'], 0)

        broker.put_object('o1', Timestamp(time()).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')

        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 123)

        policy_stats = broker.get_policy_stats()

        self.assertTrue(0 in policy_stats)
        self.assertEquals(policy_stats[0]['object_count'], 1)
        self.assertEquals(policy_stats[0]['bytes_used'], 123)

    def test_get_info(self):
        # Test ContainerBroker.get_info
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(Timestamp('1').internal, 0)

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['container'], 'test2')
        self.assertEquals(info['hash'], '00000000000000000000000000000000')
        self.assertEqual(info['put_timestamp'], Timestamp(1).internal)
        self.assertEqual(info['delete_timestamp'], '0')
        if self.__class__ in (TestContainerBrokerBeforeMetadata,
                              TestContainerBrokerBeforeXSync,
                              TestContainerBrokerBeforeSPI):
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(info['status_changed_at'],
                             Timestamp(1).internal)

        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)

        broker.put_object('o1', Timestamp(time()).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 123)

        sleep(.00001)
        broker.put_object('o2', Timestamp(time()).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 246)

        sleep(.00001)
        broker.put_object('o2', Timestamp(time()).internal, 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', Timestamp(time()).internal)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 1000)

        sleep(.00001)
        broker.delete_object('o2', Timestamp(time()).internal)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)

        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

    def test_set_x_syncs(self):
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(Timestamp('1').internal, 0)

        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

        broker.set_x_container_sync_points(1, 2)
        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], 1)
        self.assertEquals(info['x_container_sync_point2'], 2)

    def test_get_report_info(self):
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(Timestamp('1').internal, 0)

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['container'], 'test2')
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        broker.put_object('o1', Timestamp(time()).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 123)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', Timestamp(time()).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 246)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', Timestamp(time()).internal, 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        put_timestamp = Timestamp(time()).internal
        sleep(.001)
        delete_timestamp = Timestamp(time()).internal
        broker.reported(put_timestamp, delete_timestamp, 2, 1123)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)
        self.assertEquals(info['reported_put_timestamp'], put_timestamp)
        self.assertEquals(info['reported_delete_timestamp'], delete_timestamp)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', Timestamp(time()).internal)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 1000)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o2', Timestamp(time()).internal)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

    def test_list_objects_iter(self):
        # Test ContainerBroker.list_objects_iter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        for obj1 in xrange(4):
            for obj2 in xrange(125):
                broker.put_object('%d/%04d' % (obj1, obj2),
                                  Timestamp(time()).internal, 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e')
        for obj in xrange(125):
            broker.put_object('2/0051/%04d' % obj,
                              Timestamp(time()).internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        for obj in xrange(125):
            broker.put_object('3/%04d/0049' % obj,
                              Timestamp(time()).internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0099')

        listing = broker.list_objects_iter(100, '', '0/0050', None, '')
        self.assertEquals(len(listing), 50)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0049')

        listing = broker.list_objects_iter(100, '0/0099', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0/0100')
        self.assertEquals(listing[-1][0], '1/0074')

        listing = broker.list_objects_iter(55, '1/0074', None, None, '')
        self.assertEquals(len(listing), 55)
        self.assertEquals(listing[0][0], '1/0075')
        self.assertEquals(listing[-1][0], '2/0004')

        listing = broker.list_objects_iter(10, '', None, '0/01', '')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0100')
        self.assertEquals(listing[-1][0], '0/0109')

        listing = broker.list_objects_iter(10, '', None, '0/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0009')

        # Same as above, but using the path argument.
        listing = broker.list_objects_iter(10, '', None, None, '', '0')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0009')

        listing = broker.list_objects_iter(10, '', None, '', '/')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
                          ['0/', '1/', '2/', '3/'])

        listing = broker.list_objects_iter(10, '2', None, None, '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['2/', '3/'])

        listing = broker.list_objects_iter(10, '2/', None, None, '/')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['3/'])

        listing = broker.list_objects_iter(10, '2/0050', None, '2/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '2/0051')
        self.assertEquals(listing[1][0], '2/0051/')
        self.assertEquals(listing[2][0], '2/0052')
        self.assertEquals(listing[-1][0], '2/0059')

        listing = broker.list_objects_iter(10, '3/0045', None, '3/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                          ['3/0045/', '3/0046', '3/0046/', '3/0047',
                           '3/0047/', '3/0048', '3/0048/', '3/0049',
                           '3/0049/', '3/0050'])

        broker.put_object('3/0049/', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(10, '3/0048', None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3/0048/0049', '3/0049', '3/0049/',
             '3/0049/0049', '3/0050', '3/0050/0049', '3/0051', '3/0051/0049',
             '3/0052', '3/0052/0049'])

        listing = broker.list_objects_iter(10, '3/0048', None, '3/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3/0048/', '3/0049', '3/0049/', '3/0050',
             '3/0050/', '3/0051', '3/0051/', '3/0052', '3/0052/', '3/0053'])

        listing = broker.list_objects_iter(10, None, None, '3/0049/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['3/0049/', '3/0049/0049'])

        listing = broker.list_objects_iter(10, None, None, None, None,
                                           '3/0049')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['3/0049/0049'])

        listing = broker.list_objects_iter(2, None, None, '3/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['3/0000', '3/0000/'])

        listing = broker.list_objects_iter(2, None, None, None, None, '3')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['3/0000', '3/0001'])

    def test_list_objects_iter_non_slash(self):
        # Test ContainerBroker.list_objects_iter using a
        # delimiter that is not a slash
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        for obj1 in xrange(4):
            for obj2 in xrange(125):
                broker.put_object('%d:%04d' % (obj1, obj2),
                                  Timestamp(time()).internal, 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e')
        for obj in xrange(125):
            broker.put_object('2:0051:%04d' % obj,
                              Timestamp(time()).internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        for obj in xrange(125):
            broker.put_object('3:%04d:0049' % obj,
                              Timestamp(time()).internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0:0000')
        self.assertEquals(listing[-1][0], '0:0099')

        listing = broker.list_objects_iter(100, '', '0:0050', None, '')
        self.assertEquals(len(listing), 50)
        self.assertEquals(listing[0][0], '0:0000')
        self.assertEquals(listing[-1][0], '0:0049')

        listing = broker.list_objects_iter(100, '0:0099', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0:0100')
        self.assertEquals(listing[-1][0], '1:0074')

        listing = broker.list_objects_iter(55, '1:0074', None, None, '')
        self.assertEquals(len(listing), 55)
        self.assertEquals(listing[0][0], '1:0075')
        self.assertEquals(listing[-1][0], '2:0004')

        listing = broker.list_objects_iter(10, '', None, '0:01', '')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0:0100')
        self.assertEquals(listing[-1][0], '0:0109')

        listing = broker.list_objects_iter(10, '', None, '0:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0:0000')
        self.assertEquals(listing[-1][0], '0:0009')

        # Same as above, but using the path argument, so nothing should be
        # returned since path uses a '/' as a delimiter.
        listing = broker.list_objects_iter(10, '', None, None, '', '0')
        self.assertEquals(len(listing), 0)

        listing = broker.list_objects_iter(10, '', None, '', ':')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
                          ['0:', '1:', '2:', '3:'])

        listing = broker.list_objects_iter(10, '2', None, None, ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['2:', '3:'])

        listing = broker.list_objects_iter(10, '2:', None, None, ':')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['3:'])

        listing = broker.list_objects_iter(10, '2:0050', None, '2:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '2:0051')
        self.assertEquals(listing[1][0], '2:0051:')
        self.assertEquals(listing[2][0], '2:0052')
        self.assertEquals(listing[-1][0], '2:0059')

        listing = broker.list_objects_iter(10, '3:0045', None, '3:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                          ['3:0045:', '3:0046', '3:0046:', '3:0047',
                           '3:0047:', '3:0048', '3:0048:', '3:0049',
                           '3:0049:', '3:0050'])

        broker.put_object('3:0049:', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(10, '3:0048', None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3:0048:0049', '3:0049', '3:0049:',
             '3:0049:0049', '3:0050', '3:0050:0049', '3:0051', '3:0051:0049',
             '3:0052', '3:0052:0049'])

        listing = broker.list_objects_iter(10, '3:0048', None, '3:', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['3:0048:', '3:0049', '3:0049:', '3:0050',
             '3:0050:', '3:0051', '3:0051:', '3:0052', '3:0052:', '3:0053'])

        listing = broker.list_objects_iter(10, None, None, '3:0049:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['3:0049:', '3:0049:0049'])

        # Same as above, but using the path argument, so nothing should be
        # returned since path uses a '/' as a delimiter.
        listing = broker.list_objects_iter(10, None, None, None, None,
                                           '3:0049')
        self.assertEquals(len(listing), 0)

        listing = broker.list_objects_iter(2, None, None, '3:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['3:0000', '3:0000:'])

        listing = broker.list_objects_iter(2, None, None, None, None, '3')
        self.assertEquals(len(listing), 0)

    def test_list_objects_iter_prefix_delim(self):
        # Test ContainerBroker.list_objects_iter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)

        broker.put_object(
            '/pets/dogs/1', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/dogs/2', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/fish/a', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/fish/b', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/fish_info.txt', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/snakes', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')

        #def list_objects_iter(self, limit, marker, prefix, delimiter,
        #                      path=None, format=None):
        listing = broker.list_objects_iter(100, None, None, '/pets/f', '/')
        self.assertEquals([row[0] for row in listing],
                          ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish', '/')
        self.assertEquals([row[0] for row in listing],
                          ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish/', '/')
        self.assertEquals([row[0] for row in listing],
                          ['/pets/fish/a', '/pets/fish/b'])

    def test_double_check_trailing_delimiter(self):
        # Test ContainerBroker.list_objects_iter for a
        # container that has an odd file with a trailing delimiter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a/a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a/b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b/a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b/b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('c', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('00', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/00', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1/', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1/0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1/', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1/0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(25, None, None, None, None)
        self.assertEquals(len(listing), 22)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '0/', '0/0', '0/00', '0/1', '0/1/', '0/1/0', '00', '1', '1/',
             '1/0', 'a', 'a/', 'a/0', 'a/a', 'a/a/a', 'a/a/b', 'a/b', 'b',
             'b/a', 'b/b', 'c'])
        listing = broker.list_objects_iter(25, None, None, '', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '0/', '00', '1', '1/', 'a', 'a/', 'b', 'b/', 'c'])
        listing = broker.list_objects_iter(25, None, None, 'a/', '/')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['a/', 'a/0', 'a/a', 'a/a/', 'a/b'])
        listing = broker.list_objects_iter(25, None, None, '0/', '/')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['0/', '0/0', '0/00', '0/1', '0/1/'])
        listing = broker.list_objects_iter(25, None, None, '0/1/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['0/1/', '0/1/0'])
        listing = broker.list_objects_iter(25, None, None, 'b/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['b/a', 'b/b'])

    def test_double_check_trailing_delimiter_non_slash(self):
        # Test ContainerBroker.list_objects_iter for a
        # container that has an odd file with a trailing delimiter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a:a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a:b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b:a', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b:b', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('c', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('00', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:00', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1:', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1:0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1:', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1:0', Timestamp(time()).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(25, None, None, None, None)
        self.assertEquals(len(listing), 22)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '00', '0:', '0:0', '0:00', '0:1', '0:1:', '0:1:0', '1', '1:',
             '1:0', 'a', 'a:', 'a:0', 'a:a', 'a:a:a', 'a:a:b', 'a:b', 'b',
             'b:a', 'b:b', 'c'])
        listing = broker.list_objects_iter(25, None, None, '', ':')
        self.assertEquals(len(listing), 10)
        self.assertEquals(
            [row[0] for row in listing],
            ['0', '00', '0:', '1', '1:', 'a', 'a:', 'b', 'b:', 'c'])
        listing = broker.list_objects_iter(25, None, None, 'a:', ':')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['a:', 'a:0', 'a:a', 'a:a:', 'a:b'])
        listing = broker.list_objects_iter(25, None, None, '0:', ':')
        self.assertEquals(len(listing), 5)
        self.assertEquals(
            [row[0] for row in listing],
            ['0:', '0:0', '0:00', '0:1', '0:1:'])
        listing = broker.list_objects_iter(25, None, None, '0:1:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals(
            [row[0] for row in listing],
            ['0:1:', '0:1:0'])
        listing = broker.list_objects_iter(25, None, None, 'b:', ':')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['b:a', 'b:b'])

    def test_chexor(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('a', Timestamp(1).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', Timestamp(2).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hasha = hashlib.md5('%s-%s' % ('a', Timestamp(1).internal)).digest()
        hashb = hashlib.md5('%s-%s' % ('b', Timestamp(2).internal)).digest()
        hashc = ''.join(
            ('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)
        broker.put_object('b', Timestamp(3).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hashb = hashlib.md5('%s-%s' % ('b', Timestamp(3).internal)).digest()
        hashc = ''.join(
            ('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)

    def test_newid(self):
        # test DatabaseBroker.newid
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        id = broker.get_info()['id']
        broker.newid('someid')
        self.assertNotEquals(id, broker.get_info()['id'])

    def test_get_items_since(self):
        # test DatabaseBroker.get_items_since
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('a', Timestamp(1).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        max_row = broker.get_replication_info()['max_row']
        broker.put_object('b', Timestamp(2).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        items = broker.get_items_since(max_row, 1000)
        self.assertEquals(len(items), 1)
        self.assertEquals(items[0]['name'], 'b')

    def test_sync_merging(self):
        # exercise the DatabaseBroker sync functions a bit
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(Timestamp('1').internal, 0)
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(Timestamp('1').internal, 0)
        self.assertEquals(broker2.get_sync('12345'), -1)
        broker1.merge_syncs([{'sync_point': 3, 'remote_id': '12345'}])
        broker2.merge_syncs(broker1.get_syncs())
        self.assertEquals(broker2.get_sync('12345'), 3)

    def test_merge_items(self):
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(Timestamp('1').internal, 0)
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(Timestamp('1').internal, 0)
        broker1.put_object('a', Timestamp(1).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker1.put_object('b', Timestamp(2).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        id = broker1.get_info()['id']
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 2)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_object('c', Timestamp(3).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 3)
        self.assertEquals(['a', 'b', 'c'],
                          sorted([rec['name'] for rec in items]))

    def test_merge_items_overwrite_unicode(self):
        # test DatabaseBroker.merge_items
        snowman = u'\N{SNOWMAN}'.encode('utf-8')
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(Timestamp('1').internal, 0)
        id = broker1.get_info()['id']
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(Timestamp('1').internal, 0)
        broker1.put_object(snowman, Timestamp(2).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker1.put_object('b', Timestamp(3).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(json.loads(json.dumps(broker1.get_items_since(
            broker2.get_sync(id), 1000))), id)
        broker1.put_object(snowman, Timestamp(4).internal, 0, 'text/plain',
                           'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(json.loads(json.dumps(broker1.get_items_since(
            broker2.get_sync(id), 1000))), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['b', snowman],
                          sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == snowman:
                self.assertEquals(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], Timestamp(3).internal)

    def test_merge_items_overwrite(self):
        # test DatabaseBroker.merge_items
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(Timestamp('1').internal, 0)
        id = broker1.get_info()['id']
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(Timestamp('1').internal, 0)
        broker1.put_object('a', Timestamp(2).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker1.put_object('b', Timestamp(3).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        broker1.put_object('a', Timestamp(4).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], Timestamp(3).internal)

    def test_merge_items_post_overwrite_out_of_order(self):
        # test DatabaseBroker.merge_items
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(Timestamp('1').internal, 0)
        id = broker1.get_info()['id']
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(Timestamp('1').internal, 0)
        broker1.put_object('a', Timestamp(2).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker1.put_object('b', Timestamp(3).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        broker1.put_object('a', Timestamp(4).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], Timestamp(3).internal)
                self.assertEquals(rec['content_type'], 'text/plain')
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], Timestamp(3).internal)
        broker1.put_object('b', Timestamp(5).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], Timestamp(5).internal)
                self.assertEquals(rec['content_type'], 'text/plain')

    def test_set_storage_policy_index(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time())))
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        timestamp = ts.next()
        broker.initialize(timestamp, 0)

        info = broker.get_info()
        self.assertEqual(0, info['storage_policy_index'])  # sanity check
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        if self.__class__ in (TestContainerBrokerBeforeMetadata,
                              TestContainerBrokerBeforeXSync,
                              TestContainerBrokerBeforeSPI):
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(timestamp, info['status_changed_at'])
        expected = {0: {'object_count': 0, 'bytes_used': 0}}
        self.assertEqual(expected, broker.get_policy_stats())

        timestamp = ts.next()
        broker.set_storage_policy_index(111, timestamp)
        self.assertEqual(broker.storage_policy_index, 111)
        info = broker.get_info()
        self.assertEqual(111, info['storage_policy_index'])
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        self.assertEqual(timestamp, info['status_changed_at'])
        expected[111] = {'object_count': 0, 'bytes_used': 0}
        self.assertEqual(expected, broker.get_policy_stats())

        timestamp = ts.next()
        broker.set_storage_policy_index(222, timestamp)
        self.assertEqual(broker.storage_policy_index, 222)
        info = broker.get_info()
        self.assertEqual(222, info['storage_policy_index'])
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        self.assertEqual(timestamp, info['status_changed_at'])
        expected[222] = {'object_count': 0, 'bytes_used': 0}
        self.assertEqual(expected, broker.get_policy_stats())

        old_timestamp, timestamp = timestamp, ts.next()
        broker.set_storage_policy_index(222, timestamp)  # it's idempotent
        info = broker.get_info()
        self.assertEqual(222, info['storage_policy_index'])
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        self.assertEqual(old_timestamp, info['status_changed_at'])
        self.assertEqual(expected, broker.get_policy_stats())

    def test_set_storage_policy_index_empty(self):
        # Putting an object may trigger migrations, so test with a
        # never-had-an-object container to make sure we handle it
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(Timestamp('1').internal, 0)
        info = broker.get_info()
        self.assertEqual(0, info['storage_policy_index'])

        broker.set_storage_policy_index(2)
        info = broker.get_info()
        self.assertEqual(2, info['storage_policy_index'])

    def test_reconciler_sync(self):
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(Timestamp('1').internal, 0)
        self.assertEquals(-1, broker.get_reconciler_sync())
        broker.update_reconciler_sync(10)
        self.assertEquals(10, broker.get_reconciler_sync())

    @with_tempdir
    def test_legacy_pending_files(self, tempdir):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time())))
        db_path = os.path.join(tempdir, 'container.db')

        # first init an acct DB without the policy_stat table present
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(ts.next(), 1)

        # manually make some pending entries lacking storage_policy_index
        with open(broker.pending_file, 'a+b') as fp:
            for i in range(10):
                name, timestamp, size, content_type, etag, deleted = (
                    'o%s' % i, ts.next(), 0, 'c', 'e', 0)
                fp.write(':')
                fp.write(pickle.dumps(
                    (name, timestamp, size, content_type, etag, deleted),
                    protocol=2).encode('base64'))
                fp.flush()

        # use put_object to append some more entries with different
        # values for storage_policy_index
        for i in range(10, 30):
            name = 'o%s' % i
            if i < 20:
                size = 1
                storage_policy_index = 0
            else:
                size = 2
                storage_policy_index = 1
            broker.put_object(name, ts.next(), size, 'c', 'e', 0,
                              storage_policy_index=storage_policy_index)

        broker._commit_puts_stale_ok()

        # 10 objects with 0 bytes each in the legacy pending entries
        # 10 objects with 1 bytes each in storage policy 0
        # 10 objects with 2 bytes each in storage policy 1
        expected = {
            0: {'object_count': 20, 'bytes_used': 10},
            1: {'object_count': 10, 'bytes_used': 20},
        }
        self.assertEqual(broker.get_policy_stats(), expected)


class TestCommonContainerBroker(TestExampleBroker):

    broker_class = ContainerBroker

    def setUp(self):
        super(TestCommonContainerBroker, self).setUp()
        self.policy = random.choice(list(POLICIES))

    def put_item(self, broker, timestamp):
        broker.put_object('test', timestamp, 0, 'text/plain', 'x',
                          storage_policy_index=int(self.policy))

    def delete_item(self, broker, timestamp):
        broker.delete_object('test', timestamp,
                             storage_policy_index=int(self.policy))


class ContainerBrokerMigrationMixin(object):
    """
    Mixin for running ContainerBroker against databases created with
    older schemas.
    """
    def setUp(self):
        self._imported_create_object_table = \
            ContainerBroker.create_object_table
        ContainerBroker.create_object_table = \
            prespi_create_object_table
        self._imported_create_container_info_table = \
            ContainerBroker.create_container_info_table
        ContainerBroker.create_container_info_table = \
            premetadata_create_container_info_table
        self._imported_create_policy_stat_table = \
            ContainerBroker.create_policy_stat_table
        ContainerBroker.create_policy_stat_table = lambda *args: None

    @classmethod
    @contextmanager
    def old_broker(cls):
        cls.runTest = lambda *a, **k: None
        case = cls()
        case.setUp()
        try:
            yield ContainerBroker
        finally:
            case.tearDown()

    def tearDown(self):
        ContainerBroker.create_container_info_table = \
            self._imported_create_container_info_table
        ContainerBroker.create_object_table = \
            self._imported_create_object_table
        ContainerBroker.create_policy_stat_table = \
            self._imported_create_policy_stat_table


def premetadata_create_container_info_table(self, conn, put_timestamp,
                                            _spi=None):
    """
    Copied from ContainerBroker before the metadata column was
    added; used for testing with TestContainerBrokerBeforeMetadata.

    Create the container_stat table which is specific to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = Timestamp(0).internal
    conn.executescript('''
        CREATE TABLE container_stat (
            account TEXT,
            container TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            object_count INTEGER,
            bytes_used INTEGER,
            reported_put_timestamp TEXT DEFAULT '0',
            reported_delete_timestamp TEXT DEFAULT '0',
            reported_object_count INTEGER DEFAULT 0,
            reported_bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0'
        );

        INSERT INTO container_stat (object_count, bytes_used)
            VALUES (0, 0);
    ''')
    conn.execute('''
        UPDATE container_stat
        SET account = ?, container = ?, created_at = ?, id = ?,
            put_timestamp = ?
    ''', (self.account, self.container, Timestamp(time()).internal,
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeMetadata(ContainerBrokerMigrationMixin,
                                        TestContainerBroker):
    """
    Tests for ContainerBroker against databases created before
    the metadata column was added.
    """

    def setUp(self):
        super(TestContainerBrokerBeforeMetadata, self).setUp()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT metadata FROM container_stat')
            except BaseException as err:
                exc = err
        self.assert_('no such column: metadata' in str(exc))

    def tearDown(self):
        super(TestContainerBrokerBeforeMetadata, self).tearDown()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        with broker.get() as conn:
            conn.execute('SELECT metadata FROM container_stat')


def prexsync_create_container_info_table(self, conn, put_timestamp,
                                         _spi=None):
    """
    Copied from ContainerBroker before the
    x_container_sync_point[12] columns were added; used for testing with
    TestContainerBrokerBeforeXSync.

    Create the container_stat table which is specific to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = Timestamp(0).internal
    conn.executescript("""
        CREATE TABLE container_stat (
            account TEXT,
            container TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            object_count INTEGER,
            bytes_used INTEGER,
            reported_put_timestamp TEXT DEFAULT '0',
            reported_delete_timestamp TEXT DEFAULT '0',
            reported_object_count INTEGER DEFAULT 0,
            reported_bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0',
            metadata TEXT DEFAULT ''
        );

        INSERT INTO container_stat (object_count, bytes_used)
            VALUES (0, 0);
    """)
    conn.execute('''
        UPDATE container_stat
        SET account = ?, container = ?, created_at = ?, id = ?,
            put_timestamp = ?
    ''', (self.account, self.container, Timestamp(time()).internal,
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeXSync(ContainerBrokerMigrationMixin,
                                     TestContainerBroker):
    """
    Tests for ContainerBroker against databases created
    before the x_container_sync_point[12] columns were added.
    """

    def setUp(self):
        super(TestContainerBrokerBeforeXSync, self).setUp()
        ContainerBroker.create_container_info_table = \
            prexsync_create_container_info_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('''SELECT x_container_sync_point1
                                FROM container_stat''')
            except BaseException as err:
                exc = err
        self.assert_('no such column: x_container_sync_point1' in str(exc))

    def tearDown(self):
        super(TestContainerBrokerBeforeXSync, self).tearDown()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        with broker.get() as conn:
            conn.execute('SELECT x_container_sync_point1 FROM container_stat')


def prespi_create_object_table(self, conn, *args, **kwargs):
    conn.executescript("""
        CREATE TABLE object (
            ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            created_at TEXT,
            size INTEGER,
            content_type TEXT,
            etag TEXT,
            deleted INTEGER DEFAULT 0
        );

        CREATE INDEX ix_object_deleted_name ON object (deleted, name);

        CREATE TRIGGER object_insert AFTER INSERT ON object
        BEGIN
            UPDATE container_stat
            SET object_count = object_count + (1 - new.deleted),
                bytes_used = bytes_used + new.size,
                hash = chexor(hash, new.name, new.created_at);
        END;

        CREATE TRIGGER object_update BEFORE UPDATE ON object
        BEGIN
            SELECT RAISE(FAIL, 'UPDATE not allowed; DELETE and INSERT');
        END;

        CREATE TRIGGER object_delete AFTER DELETE ON object
        BEGIN
            UPDATE container_stat
            SET object_count = object_count - (1 - old.deleted),
                bytes_used = bytes_used - old.size,
                hash = chexor(hash, old.name, old.created_at);
        END;
    """)


def prespi_create_container_info_table(self, conn, put_timestamp,
                                       _spi=None):
    """
    Copied from ContainerBroker before the
    storage_policy_index column was added; used for testing with
    TestContainerBrokerBeforeSPI.

    Create the container_stat table which is specific to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = Timestamp(0).internal
    conn.executescript("""
        CREATE TABLE container_stat (
            account TEXT,
            container TEXT,
            created_at TEXT,
            put_timestamp TEXT DEFAULT '0',
            delete_timestamp TEXT DEFAULT '0',
            object_count INTEGER,
            bytes_used INTEGER,
            reported_put_timestamp TEXT DEFAULT '0',
            reported_delete_timestamp TEXT DEFAULT '0',
            reported_object_count INTEGER DEFAULT 0,
            reported_bytes_used INTEGER DEFAULT 0,
            hash TEXT default '00000000000000000000000000000000',
            id TEXT,
            status TEXT DEFAULT '',
            status_changed_at TEXT DEFAULT '0',
            metadata TEXT DEFAULT '',
            x_container_sync_point1 INTEGER DEFAULT -1,
            x_container_sync_point2 INTEGER DEFAULT -1
        );

        INSERT INTO container_stat (object_count, bytes_used)
            VALUES (0, 0);
    """)
    conn.execute('''
        UPDATE container_stat
        SET account = ?, container = ?, created_at = ?, id = ?,
            put_timestamp = ?
    ''', (self.account, self.container, Timestamp(time()).internal,
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeSPI(ContainerBrokerMigrationMixin,
                                   TestContainerBroker):
    """
    Tests for ContainerBroker against databases created
    before the storage_policy_index column was added.
    """

    def setUp(self):
        super(TestContainerBrokerBeforeSPI, self).setUp()
        ContainerBroker.create_container_info_table = \
            prespi_create_container_info_table

        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('''SELECT storage_policy_index
                                FROM container_stat''')
            except BaseException as err:
                exc = err
        self.assert_('no such column: storage_policy_index' in str(exc))

    def tearDown(self):
        super(TestContainerBrokerBeforeSPI, self).tearDown()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        with broker.get() as conn:
            conn.execute('SELECT storage_policy_index FROM container_stat')

    @patch_policies
    @with_tempdir
    def test_object_table_migration(self, tempdir):
        db_path = os.path.join(tempdir, 'container.db')

        # initialize an un-migrated database
        broker = ContainerBroker(db_path, account='a', container='c')
        put_timestamp = Timestamp(int(time())).internal
        broker.initialize(put_timestamp, None)
        with broker.get() as conn:
            try:
                conn.execute('''
                    SELECT storage_policy_index FROM object
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                # confirm that the table doesn't have this column
                self.assert_('no such column: storage_policy_index' in
                             str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from storage_policy_index '
                          'from object table!')

        # manually insert an existing row to avoid automatic migration
        obj_put_timestamp = Timestamp(time()).internal
        with broker.get() as conn:
            conn.execute('''
                INSERT INTO object (name, created_at, size,
                    content_type, etag, deleted)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', ('test_name', obj_put_timestamp, 123,
                  'text/plain', '8f4c680e75ca4c81dc1917ddab0a0b5c', 0))
            conn.commit()

        # make sure we can iter objects without performing migration
        for o in broker.list_objects_iter(1, None, None, None, None):
            self.assertEqual(o, ('test_name', obj_put_timestamp, 123,
                                 'text/plain',
                                 '8f4c680e75ca4c81dc1917ddab0a0b5c'))

        # get_info
        info = broker.get_info()
        expected = {
            'account': 'a',
            'container': 'c',
            'put_timestamp': put_timestamp,
            'delete_timestamp': '0',
            'status_changed_at': '0',
            'bytes_used': 123,
            'object_count': 1,
            'reported_put_timestamp': '0',
            'reported_delete_timestamp': '0',
            'reported_object_count': 0,
            'reported_bytes_used': 0,
            'x_container_sync_point1': -1,
            'x_container_sync_point2': -1,
            'storage_policy_index': 0,
        }
        for k, v in expected.items():
            self.assertEqual(info[k], v,
                             'The value for %s was %r not %r' % (
                                 k, info[k], v))
        self.assert_(Timestamp(info['created_at']) > Timestamp(put_timestamp))
        self.assertNotEqual(int(info['hash'], 16), 0)
        orig_hash = info['hash']
        # get_replication_info
        info = broker.get_replication_info()
        # translate object count for replicators
        expected['count'] = expected.pop('object_count')
        for k, v in expected.items():
            self.assertEqual(info[k], v)
        self.assert_(Timestamp(info['created_at']) > Timestamp(put_timestamp))
        self.assertEqual(info['hash'], orig_hash)
        self.assertEqual(info['max_row'], 1)
        self.assertEqual(info['metadata'], '')
        # get_policy_stats
        info = broker.get_policy_stats()
        expected = {
            0: {'bytes_used': 123, 'object_count': 1}
        }
        self.assertEqual(info, expected)
        # empty & is_deleted
        self.assertEqual(broker.empty(), False)
        self.assertEqual(broker.is_deleted(), False)

        # no migrations have occurred yet

        # container_stat table
        with broker.get() as conn:
            try:
                conn.execute('''
                    SELECT storage_policy_index FROM container_stat
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                # confirm that the table doesn't have this column
                self.assert_('no such column: storage_policy_index' in
                             str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from storage_policy_index '
                          'from container_stat table!')

        # object table
        with broker.get() as conn:
            try:
                conn.execute('''
                    SELECT storage_policy_index FROM object
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                # confirm that the table doesn't have this column
                self.assert_('no such column: storage_policy_index' in
                             str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from storage_policy_index '
                          'from object table!')

        # policy_stat table
        with broker.get() as conn:
            try:
                conn.execute('''
                    SELECT storage_policy_index FROM policy_stat
                    ''').fetchone()[0]
            except sqlite3.OperationalError as err:
                # confirm that the table does not exist yet
                self.assert_('no such table: policy_stat' in str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from storage_policy_index '
                          'from policy_stat table!')

        # now do a PUT with a different value for storage_policy_index
        # which will update the DB schema as well as update policy_stats
        # for legacy objects in the DB (those without an SPI)
        second_object_put_timestamp = Timestamp(time()).internal
        other_policy = [p for p in POLICIES if p.idx != 0][0]
        broker.put_object('test_second', second_object_put_timestamp,
                          456, 'text/plain',
                          'cbac50c175793513fa3c581551c876ab',
                          storage_policy_index=other_policy.idx)
        broker._commit_puts_stale_ok()

        # we are fully migrated and both objects have their
        # storage_policy_index
        with broker.get() as conn:
            storage_policy_index = conn.execute('''
                SELECT storage_policy_index FROM container_stat
                ''').fetchone()[0]
            self.assertEqual(storage_policy_index, 0)
            rows = conn.execute('''
                SELECT name, storage_policy_index FROM object
                ''').fetchall()
            for row in rows:
                if row[0] == 'test_name':
                    self.assertEqual(row[1], 0)
                else:
                    self.assertEqual(row[1], other_policy.idx)

        # and all stats tracking is in place
        stats = broker.get_policy_stats()
        self.assertEqual(len(stats), 2)
        self.assertEqual(stats[0]['object_count'], 1)
        self.assertEqual(stats[0]['bytes_used'], 123)
        self.assertEqual(stats[other_policy.idx]['object_count'], 1)
        self.assertEqual(stats[other_policy.idx]['bytes_used'], 456)

        # get info still reports on the legacy storage policy
        info = broker.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 123)

        # unless you change the storage policy
        broker.set_storage_policy_index(other_policy.idx)
        info = broker.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 456)
