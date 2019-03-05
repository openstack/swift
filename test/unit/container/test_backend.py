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
import base64
import errno
import os
import hashlib
import inspect
import unittest
from time import sleep, time
from uuid import uuid4
import random
from collections import defaultdict
from contextlib import contextmanager
import sqlite3
import pickle
import json

import six

from swift.common.exceptions import LockTimeout
from swift.container.backend import ContainerBroker, \
    update_new_item_from_existing, UNSHARDED, SHARDING, SHARDED, \
    COLLAPSED, SHARD_LISTING_STATES, SHARD_UPDATE_STATES
from swift.common.db import DatabaseAlreadyExists, GreenDBConnection
from swift.common.utils import Timestamp, encode_timestamps, hash_path, \
    ShardRange, make_db_file_path
from swift.common.storage_policy import POLICIES

import mock

from test import annotate_failure
from test.unit import (patch_policies, with_tempdir, make_timestamp_iter,
                       EMPTY_ETAG, FakeLogger, mock_timestamp_now)
from test.unit.common import test_db


class TestContainerBroker(unittest.TestCase):
    """Tests for ContainerBroker"""
    expected_db_tables = {'outgoing_sync', 'incoming_sync', 'object',
                          'sqlite_sequence', 'policy_stat',
                          'container_info', 'shard_range'}

    def _assert_shard_ranges(self, broker, expected, include_own=False):
        actual = broker.get_shard_ranges(include_deleted=True,
                                         include_own=include_own)
        self.assertEqual([dict(sr) for sr in expected],
                         [dict(sr) for sr in actual])

    def test_creation(self):
        # Test ContainerBroker.__init__
        broker = ContainerBroker(':memory:', account='a', container='c')
        self.assertEqual(broker._db_file, ':memory:')
        broker.initialize(Timestamp('1').internal, 0)
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)
            curs.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.assertEqual(self.expected_db_tables,
                             {row[0] for row in curs.fetchall()})
        # check the update trigger
        broker.put_object('blah', Timestamp.now().internal, 0, 'text/plain',
                          'etag', 0, 0)
        with broker.get() as conn:
            with self.assertRaises(sqlite3.DatabaseError) as cm:
                conn.execute('UPDATE object SET name="blah";')
        self.assertIn('UPDATE not allowed', str(cm.exception))
        if 'shard_range' in self.expected_db_tables:
            # check the update trigger
            broker.merge_shard_ranges(broker.get_own_shard_range())
            with broker.get() as conn:
                with self.assertRaises(sqlite3.DatabaseError) as cm:
                    conn.execute('UPDATE shard_range SET name="blah";')
            self.assertIn('UPDATE not allowed', str(cm.exception))

    @patch_policies
    def test_storage_policy_property(self):
        ts = make_timestamp_iter()
        for policy in POLICIES:
            broker = ContainerBroker(':memory:', account='a',
                                     container='policy_%s' % policy.name)
            broker.initialize(next(ts).internal, policy.idx)
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
                self.assertEqual(first_conn, conn)
                raise Exception('OMG')
        except Exception:
            pass
        self.assertTrue(broker.conn is None)

    @with_tempdir
    def test_is_deleted(self, tempdir):
        # Test ContainerBroker.is_deleted() and get_info_is_deleted()
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)

        self.assertFalse(broker.is_deleted())
        broker.delete_db(next(ts_iter).internal)
        self.assertTrue(broker.is_deleted())

        def check_object_counted(broker_to_test, broker_with_object):
            obj = {'name': 'o', 'created_at': next(ts_iter).internal,
                   'size': 0, 'content_type': 'text/plain', 'etag': EMPTY_ETAG,
                   'deleted': 0}
            broker_with_object.merge_items([dict(obj)])
            self.assertFalse(broker_to_test.is_deleted())
            info, deleted = broker_to_test.get_info_is_deleted()
            self.assertFalse(deleted)
            self.assertEqual(1, info['object_count'])
            obj.update({'created_at': next(ts_iter).internal, 'deleted': 1})
            broker_with_object.merge_items([dict(obj)])
            self.assertTrue(broker_to_test.is_deleted())
            info, deleted = broker_to_test.get_info_is_deleted()
            self.assertTrue(deleted)
            self.assertEqual(0, info['object_count'])

        def check_object_not_counted(broker):
            obj = {'name': 'o', 'created_at': next(ts_iter).internal,
                   'size': 0, 'content_type': 'text/plain', 'etag': EMPTY_ETAG,
                   'deleted': 0}
            broker.merge_items([dict(obj)])
            self.assertTrue(broker.is_deleted())
            info, deleted = broker.get_info_is_deleted()
            self.assertTrue(deleted)
            self.assertEqual(0, info['object_count'])
            obj.update({'created_at': next(ts_iter).internal, 'deleted': 1})
            broker.merge_items([dict(obj)])
            self.assertTrue(broker.is_deleted())
            info, deleted = broker.get_info_is_deleted()
            self.assertTrue(deleted)
            self.assertEqual(0, info['object_count'])

        def check_shard_ranges_not_counted():
            sr = ShardRange('.shards_a/shard_c', next(ts_iter), object_count=0)
            sr.update_meta(13, 99, meta_timestamp=next(ts_iter))
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertTrue(broker.is_deleted())
                info, deleted = broker.get_info_is_deleted()
                self.assertTrue(deleted)
                self.assertEqual(0, info['object_count'])

        def check_shard_ranges_counted():
            sr = ShardRange('.shards_a/shard_c', next(ts_iter), object_count=0)
            sr.update_meta(13, 99, meta_timestamp=next(ts_iter))
            counted_states = (ShardRange.ACTIVE, ShardRange.SHARDING,
                              ShardRange.SHRINKING)
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                expected = state not in counted_states
                self.assertEqual(expected, broker.is_deleted())
                info, deleted = broker.get_info_is_deleted()
                self.assertEqual(expected, deleted)
                self.assertEqual(0 if expected else 13, info['object_count'])

            sr.update_meta(0, 0, meta_timestamp=next(ts_iter))
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertTrue(broker.is_deleted())
                info, deleted = broker.get_info_is_deleted()
                self.assertTrue(deleted)
                self.assertEqual(0, info['object_count'])

        # unsharded
        check_object_counted(broker, broker)
        check_shard_ranges_not_counted()

        # move to sharding state
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        broker.delete_db(next(ts_iter).internal)
        self.assertTrue(broker.is_deleted())

        # check object in retiring db is considered
        check_object_counted(broker, broker.get_brokers()[0])
        self.assertTrue(broker.is_deleted())
        check_shard_ranges_not_counted()
        # misplaced object in fresh db is not considered
        check_object_not_counted(broker)

        # move to sharded state
        self.assertTrue(broker.set_sharded_state())
        check_object_not_counted(broker)
        check_shard_ranges_counted()

        # own shard range has no influence
        own_sr = broker.get_own_shard_range()
        own_sr.update_meta(3, 4, meta_timestamp=next(ts_iter))
        broker.merge_shard_ranges([own_sr])
        self.assertTrue(broker.is_deleted())

    @with_tempdir
    def test_empty(self, tempdir):
        # Test ContainerBroker.empty
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)
        self.assertTrue(broker.is_root_container())

        def check_object_counted(broker_to_test, broker_with_object):
            obj = {'name': 'o', 'created_at': next(ts_iter).internal,
                   'size': 0, 'content_type': 'text/plain', 'etag': EMPTY_ETAG,
                   'deleted': 0}
            broker_with_object.merge_items([dict(obj)])
            self.assertFalse(broker_to_test.empty())
            # and delete it
            obj.update({'created_at': next(ts_iter).internal, 'deleted': 1})
            broker_with_object.merge_items([dict(obj)])
            self.assertTrue(broker_to_test.empty())

        def check_shard_ranges_not_counted():
            sr = ShardRange('.shards_a/shard_c', next(ts_iter), object_count=0)
            sr.update_meta(13, 99, meta_timestamp=next(ts_iter))
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertTrue(broker.empty())

            # empty other shard ranges do not influence result
            sr.update_meta(0, 0, meta_timestamp=next(ts_iter))
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertTrue(broker.empty())

        self.assertTrue(broker.empty())
        check_object_counted(broker, broker)
        check_shard_ranges_not_counted()

        # own shard range is not considered for object count
        own_sr = broker.get_own_shard_range()
        self.assertEqual(0, own_sr.object_count)
        broker.merge_shard_ranges([own_sr])
        self.assertTrue(broker.empty())

        broker.put_object('o', next(ts_iter).internal, 0, 'text/plain',
                          EMPTY_ETAG)
        own_sr = broker.get_own_shard_range()
        self.assertEqual(1, own_sr.object_count)
        broker.merge_shard_ranges([own_sr])
        self.assertFalse(broker.empty())
        broker.delete_object('o', next(ts_iter).internal)
        self.assertTrue(broker.empty())

        # have own shard range but in state ACTIVE
        self.assertEqual(ShardRange.ACTIVE, own_sr.state)
        check_object_counted(broker, broker)
        check_shard_ranges_not_counted()

        def check_shard_ranges_counted():
            # other shard range is considered
            sr = ShardRange('.shards_a/shard_c', next(ts_iter), object_count=0)
            sr.update_meta(13, 99, meta_timestamp=next(ts_iter))
            counted_states = (ShardRange.ACTIVE, ShardRange.SHARDING,
                              ShardRange.SHRINKING)
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertEqual(state not in counted_states, broker.empty())

            # empty other shard ranges do not influence result
            sr.update_meta(0, 0, meta_timestamp=next(ts_iter))
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertTrue(broker.empty())

        # enable sharding
        broker.enable_sharding(next(ts_iter))
        check_object_counted(broker, broker)
        check_shard_ranges_counted()

        # move to sharding state
        self.assertTrue(broker.set_sharding_state())
        # check object in retiring db is considered
        check_object_counted(broker, broker.get_brokers()[0])
        self.assertTrue(broker.empty())
        # as well as misplaced objects in fresh db
        check_object_counted(broker, broker)
        check_shard_ranges_counted()

        # move to sharded state
        self.assertTrue(broker.set_sharded_state())
        self.assertTrue(broker.empty())
        check_object_counted(broker, broker)
        check_shard_ranges_counted()

        # own shard range still has no influence
        own_sr = broker.get_own_shard_range()
        own_sr.update_meta(3, 4, meta_timestamp=next(ts_iter))
        broker.merge_shard_ranges([own_sr])
        self.assertTrue(broker.empty())

    @with_tempdir
    def test_empty_shard_container(self, tempdir):
        # Test ContainerBroker.empty for a shard container where shard range
        # usage should not be considered
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(db_path, account='.shards_a', container='cc')
        broker.initialize(next(ts_iter).internal, 0)
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(broker.is_root_container())

        def check_object_counted(broker_to_test, broker_with_object):
            obj = {'name': 'o', 'created_at': next(ts_iter).internal,
                   'size': 0, 'content_type': 'text/plain', 'etag': EMPTY_ETAG,
                   'deleted': 0}
            broker_with_object.merge_items([dict(obj)])
            self.assertFalse(broker_to_test.empty())
            # and delete it
            obj.update({'created_at': next(ts_iter).internal, 'deleted': 1})
            broker_with_object.merge_items([dict(obj)])
            self.assertTrue(broker_to_test.empty())

        self.assertTrue(broker.empty())
        check_object_counted(broker, broker)

        # own shard range is not considered for object count
        own_sr = broker.get_own_shard_range()
        self.assertEqual(0, own_sr.object_count)
        broker.merge_shard_ranges([own_sr])
        self.assertTrue(broker.empty())

        broker.put_object('o', next(ts_iter).internal, 0, 'text/plain',
                          EMPTY_ETAG)
        own_sr = broker.get_own_shard_range()
        self.assertEqual(1, own_sr.object_count)
        broker.merge_shard_ranges([own_sr])
        self.assertFalse(broker.empty())
        broker.delete_object('o', next(ts_iter).internal)
        self.assertTrue(broker.empty())

        def check_shard_ranges_not_counted():
            sr = ShardRange('.shards_a/shard_c', next(ts_iter), object_count=0)
            sr.update_meta(13, 99, meta_timestamp=next(ts_iter))
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertTrue(broker.empty())

            # empty other shard ranges do not influence result
            sr.update_meta(0, 0, meta_timestamp=next(ts_iter))
            for state in ShardRange.STATES:
                sr.update_state(state, state_timestamp=next(ts_iter))
                broker.merge_shard_ranges([sr])
                self.assertTrue(broker.empty())

        check_shard_ranges_not_counted()

        # move to sharding state
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())

        # check object in retiring db is considered
        check_object_counted(broker, broker.get_brokers()[0])
        self.assertTrue(broker.empty())
        # as well as misplaced objects in fresh db
        check_object_counted(broker, broker)
        check_shard_ranges_not_counted()

        # move to sharded state
        self.assertTrue(broker.set_sharded_state())
        self.assertTrue(broker.empty())
        check_object_counted(broker, broker)
        check_shard_ranges_not_counted()

        # own shard range still has no influence
        own_sr = broker.get_own_shard_range()
        own_sr.update_meta(3, 4, meta_timestamp=next(ts_iter))
        broker.merge_shard_ranges([own_sr])
        self.assertTrue(broker.empty())

    def test_reclaim(self):
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('o', Timestamp.now().internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(Timestamp(time() - 999).internal, time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', Timestamp.now().internal)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(Timestamp(time() - 999).internal, time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(Timestamp.now().internal, time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test the return values of reclaim()
        broker.put_object('w', Timestamp.now().internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('x', Timestamp.now().internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('y', Timestamp.now().internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('z', Timestamp.now().internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        # Test before deletion
        broker.reclaim(Timestamp.now().internal, time())
        broker.delete_db(Timestamp.now().internal)

    @with_tempdir
    def test_reclaim_deadlock(self, tempdir):
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', '%s.db' % uuid4())
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(Timestamp(100).internal, 0)
        # there's some magic count here that causes the failure, something
        # about the size of object records and sqlite page size maybe?
        count = 23000
        for i in range(count):
            obj_name = 'o%d' % i
            ts = Timestamp(200).internal
            broker.delete_object(obj_name, ts)
        broker._commit_puts()
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object").fetchone()[0], count)
        # make a broker whose container attribute is not yet set so that
        # reclaim will need to query info to set it
        broker = ContainerBroker(db_path, timeout=1)
        # verify that reclaim doesn't get deadlocked and timeout
        broker.reclaim(300, 300)
        # check all objects were reclaimed
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object"
            ).fetchone()[0], 0)

    @with_tempdir
    def test_reclaim_shard_ranges(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', '%s.db' % uuid4())
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)
        older = next(ts_iter)
        same = next(ts_iter)
        newer = next(ts_iter)
        shard_ranges = [
            ShardRange('.shards_a/older_deleted', older.internal, '', 'a',
                       deleted=True),
            ShardRange('.shards_a/same_deleted', same.internal, 'a', 'b',
                       deleted=True),
            ShardRange('.shards_a/newer_deleted', newer.internal, 'b', 'c',
                       deleted=True),
            ShardRange('.shards_a/older', older.internal, 'c', 'd'),
            ShardRange('.shards_a/same', same.internal, 'd', 'e'),
            ShardRange('.shards_a/newer', newer.internal, 'e', 'f'),
            # own shard range is never reclaimed, even if deleted
            ShardRange('a/c', older.internal, '', '', deleted=True)]
        broker.merge_shard_ranges(
            random.sample(shard_ranges, len(shard_ranges)))

        def assert_row_count(expected):
            with broker.get() as conn:
                res = conn.execute("SELECT count(*) FROM shard_range")
            self.assertEqual(expected, res.fetchone()[0])

        broker.reclaim(older.internal, older.internal)
        assert_row_count(7)
        self._assert_shard_ranges(broker, shard_ranges, include_own=True)
        broker.reclaim(older.internal, same.internal)
        assert_row_count(6)
        self._assert_shard_ranges(broker, shard_ranges[1:], include_own=True)
        broker.reclaim(older.internal, newer.internal)
        assert_row_count(5)
        self._assert_shard_ranges(broker, shard_ranges[2:], include_own=True)
        broker.reclaim(older.internal, next(ts_iter).internal)
        assert_row_count(4)
        self._assert_shard_ranges(broker, shard_ranges[3:], include_own=True)

    def test_get_info_is_deleted(self):
        ts = make_timestamp_iter()
        start = next(ts)
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        # create it
        broker.initialize(start.internal, POLICIES.default.idx)
        info, is_deleted = broker.get_info_is_deleted()
        self.assertEqual(is_deleted, broker.is_deleted())
        self.assertEqual(is_deleted, False)  # sanity
        self.assertEqual(info, broker.get_info())
        self.assertEqual(info['put_timestamp'], start.internal)
        self.assertTrue(Timestamp(info['created_at']) >= start)
        self.assertEqual(info['delete_timestamp'], '0')
        if self.__class__ in (TestContainerBrokerBeforeMetadata,
                              TestContainerBrokerBeforeXSync,
                              TestContainerBrokerBeforeSPI,
                              TestContainerBrokerBeforeShardRanges):
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(info['status_changed_at'],
                             start.internal)

        # delete it
        delete_timestamp = next(ts)
        broker.delete_db(delete_timestamp.internal)
        info, is_deleted = broker.get_info_is_deleted()
        self.assertEqual(is_deleted, True)  # sanity
        self.assertEqual(is_deleted, broker.is_deleted())
        self.assertEqual(info, broker.get_info())
        self.assertEqual(info['put_timestamp'], start.internal)
        self.assertTrue(Timestamp(info['created_at']) >= start)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertEqual(info['status_changed_at'], delete_timestamp)

        # bring back to life
        broker.put_object('obj', next(ts).internal, 0, 'text/plain', 'etag',
                          storage_policy_index=broker.storage_policy_index)
        info, is_deleted = broker.get_info_is_deleted()
        self.assertEqual(is_deleted, False)  # sanity
        self.assertEqual(is_deleted, broker.is_deleted())
        self.assertEqual(info, broker.get_info())
        self.assertEqual(info['put_timestamp'], start.internal)
        self.assertTrue(Timestamp(info['created_at']) >= start)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertEqual(info['status_changed_at'], delete_timestamp)

    def test_delete_object(self):
        # Test ContainerBroker.delete_object
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('o', Timestamp.now().internal, 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', Timestamp.now().internal)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_put_object(self):
        # Test ContainerBroker.put_object
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)

        # Create initial object
        timestamp = Timestamp.now().internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Reput same event
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put new event
        sleep(.00001)
        timestamp = Timestamp.now().internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 124,
                          'application/x-test',
                          'aa0749bacbc79ec65fe206943d8fe449')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put old event
        otimestamp = Timestamp(float(Timestamp(timestamp)) - 1).internal
        broker.put_object('"{<object \'&\' name>}"', otimestamp, 124,
                          'application/x-test',
                          'aa0749bacbc79ec65fe206943d8fe449')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put old delete event
        dtimestamp = Timestamp(float(Timestamp(timestamp)) - 1).internal
        broker.put_object('"{<object \'&\' name>}"', dtimestamp, 0, '', '',
                          deleted=1)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 124)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                'aa0749bacbc79ec65fe206943d8fe449')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put new delete event
        sleep(.00001)
        timestamp = Timestamp.now().internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 0, '', '',
                          deleted=1)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 1)

        # Put new event
        sleep(.00001)
        timestamp = Timestamp.now().internal
        broker.put_object('"{<object \'&\' name>}"', timestamp, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # We'll use this later
        sleep(.0001)
        in_between_timestamp = Timestamp.now().internal

        # New post event
        sleep(.0001)
        previous_timestamp = timestamp
        timestamp = Timestamp.now().internal
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0],
                previous_timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 123)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '5af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

        # Put event from after last put but before last post
        timestamp = in_between_timestamp
        broker.put_object('"{<object \'&\' name>}"', timestamp, 456,
                          'application/x-test3',
                          '6af83e3196bf99f440f31f2e1a6c9afe')
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0],
                '"{<object \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], 456)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                'application/x-test3')
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0],
                '6af83e3196bf99f440f31f2e1a6c9afe')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], 0)

    def test_merge_shard_range_single_record(self):
        # Test ContainerBroker.merge_shard_range
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)

        ts_iter = make_timestamp_iter()
        # Stash these for later
        old_put_timestamp = next(ts_iter).internal
        old_delete_timestamp = next(ts_iter).internal

        # Create initial object
        timestamp = next(ts_iter).internal
        meta_timestamp = next(ts_iter).internal
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', timestamp,
                       'low', 'up', meta_timestamp=meta_timestamp))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'low')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'up')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 0)

        # Reput same event
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', timestamp,
                       'low', 'up', meta_timestamp=meta_timestamp))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'low')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'up')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 0)

        # Put new event
        timestamp = next(ts_iter).internal
        meta_timestamp = next(ts_iter).internal
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', timestamp,
                       'lower', 'upper', 1, 2, meta_timestamp=meta_timestamp))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'lower')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'upper')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 2)

        # Put old event
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', old_put_timestamp,
                       'lower', 'upper', 1, 2, meta_timestamp=meta_timestamp))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)  # Not old_put_timestamp!
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'lower')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'upper')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 2)

        # Put old delete event
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', old_delete_timestamp,
                       'lower', 'upper', meta_timestamp=meta_timestamp,
                       deleted=1))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)  # Not old_delete_timestamp!
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'lower')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'upper')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 2)

        # Put new delete event
        timestamp = next(ts_iter).internal
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', timestamp,
                       'lower', 'upper', meta_timestamp=meta_timestamp,
                       deleted=1))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 1)

        # Put new event
        timestamp = next(ts_iter).internal
        meta_timestamp = next(ts_iter).internal
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', timestamp,
                       'lowerer', 'upperer', 3, 4,
                       meta_timestamp=meta_timestamp))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'lowerer')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'upperer')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 3)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 4)

        # We'll use this later
        in_between_timestamp = next(ts_iter).internal

        # New update event, meta_timestamp increases
        meta_timestamp = next(ts_iter).internal
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', timestamp,
                       'lowerer', 'upperer', 3, 4,
                       meta_timestamp=meta_timestamp))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'lowerer')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'upperer')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 3)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 4)

        # Put event from after last put but before last post
        timestamp = in_between_timestamp
        broker.merge_shard_ranges(
            ShardRange('"a/{<shardrange \'&\' name>}"', timestamp,
                       'lowererer', 'uppererer', 5, 6,
                       meta_timestamp=meta_timestamp))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM shard_range").fetchone()[0],
                '"a/{<shardrange \'&\' name>}"')
            self.assertEqual(conn.execute(
                "SELECT timestamp FROM shard_range").fetchone()[0],
                timestamp)
            self.assertEqual(conn.execute(
                "SELECT meta_timestamp FROM shard_range").fetchone()[0],
                meta_timestamp)
            self.assertEqual(conn.execute(
                "SELECT lower FROM shard_range").fetchone()[0], 'lowererer')
            self.assertEqual(conn.execute(
                "SELECT upper FROM shard_range").fetchone()[0], 'uppererer')
            self.assertEqual(conn.execute(
                "SELECT deleted FROM shard_range").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT object_count FROM shard_range").fetchone()[0], 5)
            self.assertEqual(conn.execute(
                "SELECT bytes_used FROM shard_range").fetchone()[0], 6)

    def test_merge_shard_ranges_deleted(self):
        # Test ContainerBroker.merge_shard_ranges sets deleted attribute
        ts_iter = make_timestamp_iter()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        # put shard range
        broker.merge_shard_ranges(ShardRange('a/o', next(ts_iter).internal))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM shard_range "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM shard_range "
                "WHERE deleted = 1").fetchone()[0], 0)

        # delete shard range
        broker.merge_shard_ranges(ShardRange('a/o', next(ts_iter).internal,
                                             deleted=1))
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM shard_range "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM shard_range "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_make_tuple_for_pickle(self):
        record = {'name': 'obj',
                  'created_at': '1234567890.12345',
                  'size': 42,
                  'content_type': 'text/plain',
                  'etag': 'hash_test',
                  'deleted': '1',
                  'storage_policy_index': '2',
                  'ctype_timestamp': None,
                  'meta_timestamp': None}
        broker = ContainerBroker(':memory:', account='a', container='c')

        expect = ('obj', '1234567890.12345', 42, 'text/plain', 'hash_test',
                  '1', '2', None, None)
        result = broker.make_tuple_for_pickle(record)
        self.assertEqual(expect, result)

        record['ctype_timestamp'] = '2233445566.00000'
        expect = ('obj', '1234567890.12345', 42, 'text/plain', 'hash_test',
                  '1', '2', '2233445566.00000', None)
        result = broker.make_tuple_for_pickle(record)
        self.assertEqual(expect, result)

        record['meta_timestamp'] = '5566778899.00000'
        expect = ('obj', '1234567890.12345', 42, 'text/plain', 'hash_test',
                  '1', '2', '2233445566.00000', '5566778899.00000')
        result = broker.make_tuple_for_pickle(record)
        self.assertEqual(expect, result)

    @with_tempdir
    def test_load_old_record_from_pending_file(self, tempdir):
        # Test reading old update record from pending file
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(time(), 0)

        record = {'name': 'obj',
                  'created_at': '1234567890.12345',
                  'size': 42,
                  'content_type': 'text/plain',
                  'etag': 'hash_test',
                  'deleted': '1',
                  'storage_policy_index': '2',
                  'ctype_timestamp': None,
                  'meta_timestamp': None}

        # sanity check
        self.assertFalse(os.path.isfile(broker.pending_file))

        # simulate existing pending items written with old code,
        # i.e. without content_type and meta timestamps
        def old_make_tuple_for_pickle(_, record):
            return (record['name'], record['created_at'], record['size'],
                    record['content_type'], record['etag'], record['deleted'],
                    record['storage_policy_index'])

        _new = 'swift.container.backend.ContainerBroker.make_tuple_for_pickle'
        with mock.patch(_new, old_make_tuple_for_pickle):
            broker.put_record(dict(record))

        self.assertTrue(os.path.getsize(broker.pending_file) > 0)
        read_items = []

        def mock_merge_items(_, item_list, *args):
            # capture the items read from the pending file
            read_items.extend(item_list)

        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items):
            broker._commit_puts()

        self.assertEqual(1, len(read_items))
        self.assertEqual(record, read_items[0])
        self.assertTrue(os.path.getsize(broker.pending_file) == 0)

    @with_tempdir
    def test_save_and_load_record_from_pending_file(self, tempdir):
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(time(), 0)

        record = {'name': 'obj',
                  'created_at': '1234567890.12345',
                  'size': 42,
                  'content_type': 'text/plain',
                  'etag': 'hash_test',
                  'deleted': '1',
                  'storage_policy_index': '2',
                  'ctype_timestamp': '1234567890.44444',
                  'meta_timestamp': '1234567890.99999'}

        # sanity check
        self.assertFalse(os.path.isfile(broker.pending_file))
        broker.put_record(dict(record))
        self.assertTrue(os.path.getsize(broker.pending_file) > 0)
        read_items = []

        def mock_merge_items(_, item_list, *args):
            # capture the items read from the pending file
            read_items.extend(item_list)

        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items):
            broker._commit_puts()

        self.assertEqual(1, len(read_items))
        self.assertEqual(record, read_items[0])
        self.assertTrue(os.path.getsize(broker.pending_file) == 0)

    def _assert_db_row(self, broker, name, timestamp, size, content_type, hash,
                       deleted=0):
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT name FROM object").fetchone()[0], name)
            self.assertEqual(conn.execute(
                "SELECT created_at FROM object").fetchone()[0], timestamp)
            self.assertEqual(conn.execute(
                "SELECT size FROM object").fetchone()[0], size)
            self.assertEqual(conn.execute(
                "SELECT content_type FROM object").fetchone()[0],
                content_type)
            self.assertEqual(conn.execute(
                "SELECT etag FROM object").fetchone()[0], hash)
            self.assertEqual(conn.execute(
                "SELECT deleted FROM object").fetchone()[0], deleted)

    def _test_put_object_multiple_encoded_timestamps(self, broker):
        ts = make_timestamp_iter()
        broker.initialize(next(ts).internal, 0)
        t = [next(ts) for _ in range(9)]

        # Create initial object
        broker.put_object('obj_name', t[0].internal, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t[0].internal, 123,
                            'application/x-test',
                            '5af83e3196bf99f440f31f2e1a6c9afe')

        # hash and size change with same data timestamp are ignored
        t_encoded = encode_timestamps(t[0], t[1], t[1])
        broker.put_object('obj_name', t_encoded, 456,
                          'application/x-test-2',
                          '1234567890abcdeffedcba0987654321')
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 123,
                            'application/x-test-2',
                            '5af83e3196bf99f440f31f2e1a6c9afe')

        # content-type change with same timestamp is ignored
        t_encoded = encode_timestamps(t[0], t[1], t[2])
        broker.put_object('obj_name', t_encoded, 456,
                          'application/x-test-3',
                          '1234567890abcdeffedcba0987654321')
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 123,
                            'application/x-test-2',
                            '5af83e3196bf99f440f31f2e1a6c9afe')

        # update with differing newer timestamps
        t_encoded = encode_timestamps(t[4], t[6], t[8])
        broker.put_object('obj_name', t_encoded, 789,
                          'application/x-test-3',
                          'abcdef1234567890abcdef1234567890')
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 789,
                            'application/x-test-3',
                            'abcdef1234567890abcdef1234567890')

        # update with differing older timestamps should be ignored
        t_encoded_older = encode_timestamps(t[3], t[5], t[7])
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        broker.put_object('obj_name', t_encoded_older, 9999,
                          'application/x-test-ignored',
                          'ignored_hash')
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 789,
                            'application/x-test-3',
                            'abcdef1234567890abcdef1234567890')

    def test_put_object_multiple_encoded_timestamps_using_memory(self):
        # Test ContainerBroker.put_object with differing data, content-type
        # and metadata timestamps
        broker = ContainerBroker(':memory:', account='a', container='c')
        self._test_put_object_multiple_encoded_timestamps(broker)

    @with_tempdir
    def test_get_db_state(self, tempdir):
        acct = 'account'
        cont = 'container'
        hsh = hash_path(acct, cont)
        db_file = "%s.db" % hsh
        epoch = Timestamp.now()
        fresh_db_file = "%s_%s.db" % (hsh, epoch.normal)
        db_path = os.path.join(tempdir, db_file)
        fresh_db_path = os.path.join(tempdir, fresh_db_file)
        ts = Timestamp.now()

        # First test NOTFOUND state
        broker = ContainerBroker(db_path, account=acct, container=cont)
        self.assertEqual(broker.get_db_state(), 'not_found')

        # Test UNSHARDED state, that is when db_file exists and fresh_db_file
        # doesn't
        broker.initialize(ts.internal, 0)
        self.assertEqual(broker.get_db_state(), 'unsharded')

        # Test the SHARDING state, this is the period when both the db_file and
        # the fresh_db_file exist
        fresh_broker = ContainerBroker(fresh_db_path, account=acct,
                                       container=cont, force_db_file=True)
        fresh_broker.initialize(ts.internal, 0)
        own_shard_range = fresh_broker.get_own_shard_range()
        own_shard_range.update_state(ShardRange.SHARDING)
        own_shard_range.epoch = epoch
        shard_range = ShardRange(
            '.shards_%s/%s' % (acct, cont), Timestamp.now())
        fresh_broker.merge_shard_ranges([own_shard_range, shard_range])

        self.assertEqual(fresh_broker.get_db_state(), 'sharding')
        # old broker will also change state if we reload its db files
        broker.reload_db_files()
        self.assertEqual(broker.get_db_state(), 'sharding')

        # Test the SHARDED state, this is when only fresh_db_file exists.
        os.unlink(db_path)
        fresh_broker.reload_db_files()
        self.assertEqual(fresh_broker.get_db_state(), 'sharded')

        # Test the COLLAPSED state, this is when only fresh_db_file exists.
        shard_range.deleted = 1
        shard_range.timestamp = Timestamp.now()
        fresh_broker.merge_shard_ranges([shard_range])
        self.assertEqual(fresh_broker.get_db_state(), 'collapsed')

        # back to UNSHARDED if the desired epoch changes
        own_shard_range.update_state(ShardRange.SHRINKING,
                                     state_timestamp=Timestamp.now())
        own_shard_range.epoch = Timestamp.now()
        fresh_broker.merge_shard_ranges([own_shard_range])
        self.assertEqual(fresh_broker.get_db_state(), 'unsharded')

    @with_tempdir
    def test_db_file(self, tempdir):
        acct = 'account'
        cont = 'continer'
        hsh = hash_path(acct, cont)
        db_file = "%s.db" % hsh
        ts_epoch = Timestamp.now()
        fresh_db_file = "%s_%s.db" % (hsh, ts_epoch.normal)
        db_path = os.path.join(tempdir, db_file)
        fresh_db_path = os.path.join(tempdir, fresh_db_file)
        ts = Timestamp.now()

        # First test NOTFOUND state, this will return the db_file passed
        # in the constructor
        def check_unfound_db_files(broker, init_db_file):
            self.assertEqual(init_db_file, broker.db_file)
            self.assertEqual(broker._db_file, db_path)
            self.assertFalse(os.path.exists(db_path))
            self.assertFalse(os.path.exists(fresh_db_path))
            self.assertEqual([], broker.db_files)

        broker = ContainerBroker(db_path, account=acct, container=cont)
        check_unfound_db_files(broker, db_path)
        broker = ContainerBroker(fresh_db_path, account=acct, container=cont)
        check_unfound_db_files(broker, fresh_db_path)

        # Test UNSHARDED state, that is when db_file exists and fresh_db_file
        # doesn't, so it should return the db_path
        def check_unsharded_db_files(broker):
            self.assertEqual(broker.db_file, db_path)
            self.assertEqual(broker._db_file, db_path)
            self.assertTrue(os.path.exists(db_path))
            self.assertFalse(os.path.exists(fresh_db_path))
            self.assertEqual([db_path], broker.db_files)

        broker = ContainerBroker(db_path, account=acct, container=cont)
        broker.initialize(ts.internal, 0)
        check_unsharded_db_files(broker)
        broker = ContainerBroker(fresh_db_path, account=acct, container=cont)
        check_unsharded_db_files(broker)
        # while UNSHARDED db_path is still used despite giving fresh_db_path
        # to init, so we cannot initialize this broker
        with self.assertRaises(DatabaseAlreadyExists):
            broker.initialize(ts.internal, 0)

        # Test the SHARDING state, this is the period when both the db_file and
        # the fresh_db_file exist, in this case it should return the
        # fresh_db_path.
        def check_sharding_db_files(broker):
            self.assertEqual(broker.db_file, fresh_db_path)
            self.assertEqual(broker._db_file, db_path)
            self.assertTrue(os.path.exists(db_path))
            self.assertTrue(os.path.exists(fresh_db_path))
            self.assertEqual([db_path, fresh_db_path], broker.db_files)

        # Use force_db_file to have db_shard_path created when initializing
        broker = ContainerBroker(fresh_db_path, account=acct,
                                 container=cont, force_db_file=True)
        self.assertEqual([db_path], broker.db_files)
        broker.initialize(ts.internal, 0)
        check_sharding_db_files(broker)
        broker = ContainerBroker(db_path, account=acct, container=cont)
        check_sharding_db_files(broker)
        broker = ContainerBroker(fresh_db_path, account=acct, container=cont)
        check_sharding_db_files(broker)

        # force_db_file can be used to open db_path specifically
        forced_broker = ContainerBroker(db_path, account=acct,
                                        container=cont, force_db_file=True)
        self.assertEqual(forced_broker.db_file, db_path)
        self.assertEqual(forced_broker._db_file, db_path)

        def check_sharded_db_files(broker):
            self.assertEqual(broker.db_file, fresh_db_path)
            self.assertEqual(broker._db_file, db_path)
            self.assertFalse(os.path.exists(db_path))
            self.assertTrue(os.path.exists(fresh_db_path))
            self.assertEqual([fresh_db_path], broker.db_files)

        # Test the SHARDED state, this is when only fresh_db_file exists, so
        # obviously this should return the fresh_db_path
        os.unlink(db_path)
        broker.reload_db_files()
        check_sharded_db_files(broker)
        broker = ContainerBroker(db_path, account=acct, container=cont)
        check_sharded_db_files(broker)

    @with_tempdir
    def test_sharding_initiated_and_required(self, tempdir):
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', '%s.db' % uuid4())
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(Timestamp.now().internal, 0)
        # no shard ranges
        self.assertIs(False, broker.sharding_initiated())
        self.assertIs(False, broker.sharding_required())
        # only own shard range
        own_sr = broker.get_own_shard_range()
        for state in ShardRange.STATES:
            own_sr.update_state(state, state_timestamp=Timestamp.now())
            broker.merge_shard_ranges(own_sr)
            self.assertIs(False, broker.sharding_initiated())
            self.assertIs(False, broker.sharding_required())

        # shard ranges, still ACTIVE
        own_sr.update_state(ShardRange.ACTIVE,
                            state_timestamp=Timestamp.now())
        broker.merge_shard_ranges(own_sr)
        broker.merge_shard_ranges(ShardRange('.shards_a/cc', Timestamp.now()))
        self.assertIs(False, broker.sharding_initiated())
        self.assertIs(False, broker.sharding_required())

        # shard ranges and SHARDING, SHRINKING or SHARDED
        broker.enable_sharding(Timestamp.now())
        self.assertTrue(broker.set_sharding_state())
        self.assertIs(True, broker.sharding_initiated())
        self.assertIs(True, broker.sharding_required())

        epoch = broker.db_epoch
        own_sr.update_state(ShardRange.SHRINKING,
                            state_timestamp=Timestamp.now())
        own_sr.epoch = epoch
        broker.merge_shard_ranges(own_sr)
        self.assertIs(True, broker.sharding_initiated())
        self.assertIs(True, broker.sharding_required())

        own_sr.update_state(ShardRange.SHARDED)
        broker.merge_shard_ranges(own_sr)
        self.assertTrue(broker.set_sharded_state())
        self.assertIs(True, broker.sharding_initiated())
        self.assertIs(False, broker.sharding_required())

    @with_tempdir
    def test_put_object_multiple_encoded_timestamps_using_file(self, tempdir):
        # Test ContainerBroker.put_object with differing data, content-type
        # and metadata timestamps, using file db to ensure that the code paths
        # to write/read pending file are exercised.
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        self._test_put_object_multiple_encoded_timestamps(broker)

    def _test_put_object_multiple_explicit_timestamps(self, broker):
        ts = make_timestamp_iter()
        broker.initialize(next(ts).internal, 0)
        t = [next(ts) for _ in range(11)]

        # Create initial object
        broker.put_object('obj_name', t[0].internal, 123,
                          'application/x-test',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          ctype_timestamp=None,
                          meta_timestamp=None)
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t[0].internal, 123,
                            'application/x-test',
                            '5af83e3196bf99f440f31f2e1a6c9afe')

        # hash and size change with same data timestamp are ignored
        t_encoded = encode_timestamps(t[0], t[1], t[1])
        broker.put_object('obj_name', t[0].internal, 456,
                          'application/x-test-2',
                          '1234567890abcdeffedcba0987654321',
                          ctype_timestamp=t[1].internal,
                          meta_timestamp=t[1].internal)
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 123,
                            'application/x-test-2',
                            '5af83e3196bf99f440f31f2e1a6c9afe')

        # content-type change with same timestamp is ignored
        t_encoded = encode_timestamps(t[0], t[1], t[2])
        broker.put_object('obj_name', t[0].internal, 456,
                          'application/x-test-3',
                          '1234567890abcdeffedcba0987654321',
                          ctype_timestamp=t[1].internal,
                          meta_timestamp=t[2].internal)
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 123,
                            'application/x-test-2',
                            '5af83e3196bf99f440f31f2e1a6c9afe')

        # update with differing newer timestamps
        t_encoded = encode_timestamps(t[4], t[6], t[8])
        broker.put_object('obj_name', t[4].internal, 789,
                          'application/x-test-3',
                          'abcdef1234567890abcdef1234567890',
                          ctype_timestamp=t[6].internal,
                          meta_timestamp=t[8].internal)
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 789,
                            'application/x-test-3',
                            'abcdef1234567890abcdef1234567890')

        # update with differing older timestamps should be ignored
        broker.put_object('obj_name', t[3].internal, 9999,
                          'application/x-test-ignored',
                          'ignored_hash',
                          ctype_timestamp=t[5].internal,
                          meta_timestamp=t[7].internal)
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 789,
                            'application/x-test-3',
                            'abcdef1234567890abcdef1234567890')

        # content_type_timestamp == None defaults to data timestamp
        t_encoded = encode_timestamps(t[9], t[9], t[8])
        broker.put_object('obj_name', t[9].internal, 9999,
                          'application/x-test-new',
                          'new_hash',
                          ctype_timestamp=None,
                          meta_timestamp=t[7].internal)
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 9999,
                            'application/x-test-new',
                            'new_hash')

        # meta_timestamp == None defaults to data timestamp
        t_encoded = encode_timestamps(t[9], t[10], t[10])
        broker.put_object('obj_name', t[8].internal, 1111,
                          'application/x-test-newer',
                          'older_hash',
                          ctype_timestamp=t[10].internal,
                          meta_timestamp=None)
        self.assertEqual(1, len(broker.get_items_since(0, 100)))
        self._assert_db_row(broker, 'obj_name', t_encoded, 9999,
                            'application/x-test-newer',
                            'new_hash')

    def test_put_object_multiple_explicit_timestamps_using_memory(self):
        # Test ContainerBroker.put_object with differing data, content-type
        # and metadata timestamps passed as explicit args
        broker = ContainerBroker(':memory:', account='a', container='c')
        self._test_put_object_multiple_explicit_timestamps(broker)

    @with_tempdir
    def test_put_object_multiple_explicit_timestamps_using_file(self, tempdir):
        # Test ContainerBroker.put_object with differing data, content-type
        # and metadata timestamps passed as explicit args, using file db to
        # ensure that the code paths to write/read pending file are exercised.
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        self._test_put_object_multiple_explicit_timestamps(broker)

    def test_last_modified_time(self):
        # Test container listing reports the most recent of data or metadata
        # timestamp as last-modified time
        ts = make_timestamp_iter()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(next(ts).internal, 0)

        # simple 'single' timestamp case
        t0 = next(ts)
        broker.put_object('obj1', t0.internal, 0, 'text/plain', 'hash1')
        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0][0], 'obj1')
        self.assertEqual(listing[0][1], t0.internal)

        # content-type and metadata are updated at t1
        t1 = next(ts)
        t_encoded = encode_timestamps(t0, t1, t1)
        broker.put_object('obj1', t_encoded, 0, 'text/plain', 'hash1')
        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0][0], 'obj1')
        self.assertEqual(listing[0][1], t1.internal)

        # used later
        t2 = next(ts)

        # metadata is updated at t3
        t3 = next(ts)
        t_encoded = encode_timestamps(t0, t1, t3)
        broker.put_object('obj1', t_encoded, 0, 'text/plain', 'hash1')
        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0][0], 'obj1')
        self.assertEqual(listing[0][1], t3.internal)

        # all parts updated at t2, last-modified should remain at t3
        t_encoded = encode_timestamps(t2, t2, t2)
        broker.put_object('obj1', t_encoded, 0, 'text/plain', 'hash1')
        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0][0], 'obj1')
        self.assertEqual(listing[0][1], t3.internal)

        # all parts updated at t4, last-modified should be t4
        t4 = next(ts)
        t_encoded = encode_timestamps(t4, t4, t4)
        broker.put_object('obj1', t_encoded, 0, 'text/plain', 'hash1')
        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 1)
        self.assertEqual(listing[0][0], 'obj1')
        self.assertEqual(listing[0][1], t4.internal)

    @patch_policies
    def test_put_misplaced_object_does_not_effect_container_stats(self):
        policy = random.choice(list(POLICIES))
        ts = make_timestamp_iter()
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        broker.initialize(next(ts).internal, policy.idx)
        # migration tests may not honor policy on initialize
        if isinstance(self, ContainerBrokerMigrationMixin):
            real_storage_policy_index = \
                broker.get_info()['storage_policy_index']
            policy = [p for p in POLICIES
                      if p.idx == real_storage_policy_index][0]
        broker.put_object('correct_o', next(ts).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=policy.idx)
        info = broker.get_info()
        self.assertEqual(1, info['object_count'])
        self.assertEqual(123, info['bytes_used'])
        other_policy = random.choice([p for p in POLICIES
                                      if p is not policy])
        broker.put_object('wrong_o', next(ts).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=other_policy.idx)
        self.assertEqual(1, info['object_count'])
        self.assertEqual(123, info['bytes_used'])

    @patch_policies
    def test_has_multiple_policies(self):
        policy = random.choice(list(POLICIES))
        ts = make_timestamp_iter()
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        broker.initialize(next(ts).internal, policy.idx)
        # migration tests may not honor policy on initialize
        if isinstance(self, ContainerBrokerMigrationMixin):
            real_storage_policy_index = \
                broker.get_info()['storage_policy_index']
            policy = [p for p in POLICIES
                      if p.idx == real_storage_policy_index][0]
        broker.put_object('correct_o', next(ts).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=policy.idx)
        self.assertFalse(broker.has_multiple_policies())
        other_policy = [p for p in POLICIES if p is not policy][0]
        broker.put_object('wrong_o', next(ts).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=other_policy.idx)
        self.assertTrue(broker.has_multiple_policies())

    @patch_policies
    def test_get_policy_info(self):
        policy = random.choice(list(POLICIES))
        ts = make_timestamp_iter()
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        broker.initialize(next(ts).internal, policy.idx)
        # migration tests may not honor policy on initialize
        if isinstance(self, ContainerBrokerMigrationMixin):
            real_storage_policy_index = \
                broker.get_info()['storage_policy_index']
            policy = [p for p in POLICIES
                      if p.idx == real_storage_policy_index][0]
        policy_stats = broker.get_policy_stats()
        expected = {policy.idx: {'bytes_used': 0, 'object_count': 0}}
        self.assertEqual(policy_stats, expected)

        # add an object
        broker.put_object('correct_o', next(ts).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=policy.idx)
        policy_stats = broker.get_policy_stats()
        expected = {policy.idx: {'bytes_used': 123, 'object_count': 1}}
        self.assertEqual(policy_stats, expected)

        # add a misplaced object
        other_policy = random.choice([p for p in POLICIES
                                      if p is not policy])
        broker.put_object('wrong_o', next(ts).internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe',
                          storage_policy_index=other_policy.idx)
        policy_stats = broker.get_policy_stats()
        expected = {
            policy.idx: {'bytes_used': 123, 'object_count': 1},
            other_policy.idx: {'bytes_used': 123, 'object_count': 1},
        }
        self.assertEqual(policy_stats, expected)

    @patch_policies
    def test_policy_stat_tracking(self):
        ts = make_timestamp_iter()
        broker = ContainerBroker(':memory:',
                                 account='a', container='c')
        # Note: in subclasses of this TestCase that inherit the
        # ContainerBrokerMigrationMixin, passing POLICIES.default.idx here has
        # no effect and broker.get_policy_stats() returns a dict with a single
        # entry mapping policy index 0 to the container stats
        broker.initialize(next(ts).internal, POLICIES.default.idx)
        stats = defaultdict(dict)

        def assert_empty_default_policy_stats(policy_stats):
            # if no objects were added for the default policy we still
            # expect an entry for the default policy in the returned info
            # because the database was initialized with that storage policy
            # - but it must be empty.
            default_stats = policy_stats[POLICIES.default.idx]
            expected = {'object_count': 0, 'bytes_used': 0}
            self.assertEqual(default_stats, expected)

        policy_stats = broker.get_policy_stats()
        assert_empty_default_policy_stats(policy_stats)

        iters = 100
        for i in range(iters):
            policy_index = random.randint(0, iters * 0.1)
            name = 'object-%s' % random.randint(0, iters * 0.1)
            size = random.randint(0, iters)
            broker.put_object(name, next(ts).internal, size, 'text/plain',
                              '5af83e3196bf99f440f31f2e1a6c9afe',
                              storage_policy_index=policy_index)
            # track the size of the latest timestamp put for each object
            # in each storage policy
            stats[policy_index][name] = size
        policy_stats = broker.get_policy_stats()
        if POLICIES.default.idx not in stats:
            # unlikely, but check empty default index still in policy stats
            assert_empty_default_policy_stats(policy_stats)
            policy_stats.pop(POLICIES.default.idx)
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
        self.assertEqual(info['account'], 'test1')
        self.assertEqual(info['container'], 'test2')
        self.assertEqual(info['hash'], '00000000000000000000000000000000')
        self.assertEqual(info['put_timestamp'], Timestamp(1).internal)
        self.assertEqual(info['delete_timestamp'], '0')

        info = broker.get_info()
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)

        policy_stats = broker.get_policy_stats()

        # Act as policy-0
        self.assertTrue(0 in policy_stats)
        self.assertEqual(policy_stats[0]['bytes_used'], 0)
        self.assertEqual(policy_stats[0]['object_count'], 0)

        broker.put_object('o1', Timestamp.now().internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')

        info = broker.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 123)

        policy_stats = broker.get_policy_stats()

        self.assertTrue(0 in policy_stats)
        self.assertEqual(policy_stats[0]['object_count'], 1)
        self.assertEqual(policy_stats[0]['bytes_used'], 123)

    def test_get_info(self):
        # Test ContainerBroker.get_info
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(Timestamp('1').internal, 0)

        info = broker.get_info()
        self.assertEqual(info['account'], 'test1')
        self.assertEqual(info['container'], 'test2')
        self.assertEqual(info['hash'], '00000000000000000000000000000000')
        self.assertEqual(info['put_timestamp'], Timestamp(1).internal)
        self.assertEqual(info['delete_timestamp'], '0')
        if self.__class__ in (TestContainerBrokerBeforeMetadata,
                              TestContainerBrokerBeforeXSync,
                              TestContainerBrokerBeforeSPI,
                              TestContainerBrokerBeforeShardRanges):
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(info['status_changed_at'],
                             Timestamp(1).internal)

        info = broker.get_info()
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)

        broker.put_object('o1', Timestamp.now().internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 123)

        sleep(.00001)
        broker.put_object('o2', Timestamp.now().internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEqual(info['object_count'], 2)
        self.assertEqual(info['bytes_used'], 246)

        sleep(.00001)
        broker.put_object('o2', Timestamp.now().internal, 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEqual(info['object_count'], 2)
        self.assertEqual(info['bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', Timestamp.now().internal)
        info = broker.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 1000)

        sleep(.00001)
        broker.delete_object('o2', Timestamp.now().internal)
        info = broker.get_info()
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)

        info = broker.get_info()
        self.assertEqual(info['x_container_sync_point1'], -1)
        self.assertEqual(info['x_container_sync_point2'], -1)

    @with_tempdir
    def test_get_info_sharding_states(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'hash.db')
        broker = ContainerBroker(
            db_path, account='myaccount', container='mycontainer')
        broker.initialize(next(ts_iter).internal, 0)
        broker.put_object('o1', next(ts_iter).internal, 123, 'text/plain',
                          'fake etag')
        sr = ShardRange('.shards_a/c', next(ts_iter))
        broker.merge_shard_ranges(sr)

        def check_info(expected):
            errors = []
            for k, v in expected.items():
                if info.get(k) != v:
                    errors.append((k, v, info.get(k)))
            if errors:
                self.fail('Mismatches: %s' % ', '.join(
                    ['%s should be %s but got %s' % error
                     for error in errors]))

        # unsharded
        with mock.patch.object(
                broker, 'get_shard_usage') as mock_get_shard_usage:
            info = broker.get_info()
        mock_get_shard_usage.assert_not_called()
        check_info({'account': 'myaccount',
                    'container': 'mycontainer',
                    'object_count': 1,
                    'bytes_used': 123,
                    'db_state': 'unsharded'})

        # sharding
        epoch = next(ts_iter)
        broker.enable_sharding(epoch)
        self.assertTrue(broker.set_sharding_state())
        broker.put_object('o2', next(ts_iter).internal, 1, 'text/plain',
                          'fake etag')
        broker.put_object('o3', next(ts_iter).internal, 320, 'text/plain',
                          'fake etag')
        with mock.patch.object(
                broker, 'get_shard_usage') as mock_get_shard_usage:
            info = broker.get_info()
        mock_get_shard_usage.assert_not_called()
        check_info({'account': 'myaccount',
                    'container': 'mycontainer',
                    'object_count': 1,
                    'bytes_used': 123,
                    'db_state': 'sharding'})

        # sharded
        self.assertTrue(broker.set_sharded_state())
        shard_stats = {'object_count': 1001, 'bytes_used': 3003}
        with mock.patch.object(
                broker, 'get_shard_usage') as mock_get_shard_usage:
            mock_get_shard_usage.return_value = shard_stats
            info = broker.get_info()
        mock_get_shard_usage.assert_called_once_with()
        check_info({'account': 'myaccount',
                    'container': 'mycontainer',
                    'object_count': 1001,
                    'bytes_used': 3003,
                    'db_state': 'sharded'})

        # collapsed
        sr.set_deleted(next(ts_iter))
        broker.merge_shard_ranges(sr)
        with mock.patch.object(
                broker, 'get_shard_usage') as mock_get_shard_usage:
            info = broker.get_info()
        mock_get_shard_usage.assert_not_called()
        check_info({'account': 'myaccount',
                    'container': 'mycontainer',
                    'object_count': 2,
                    'bytes_used': 321,
                    'db_state': 'collapsed'})

    def test_set_x_syncs(self):
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(Timestamp('1').internal, 0)

        info = broker.get_info()
        self.assertEqual(info['x_container_sync_point1'], -1)
        self.assertEqual(info['x_container_sync_point2'], -1)

        broker.set_x_container_sync_points(1, 2)
        info = broker.get_info()
        self.assertEqual(info['x_container_sync_point1'], 1)
        self.assertEqual(info['x_container_sync_point2'], 2)

    def test_get_report_info(self):
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(Timestamp('1').internal, 0)

        info = broker.get_info()
        self.assertEqual(info['account'], 'test1')
        self.assertEqual(info['container'], 'test2')
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        broker.put_object('o1', Timestamp.now().internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 123)
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', Timestamp.now().internal, 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEqual(info['object_count'], 2)
        self.assertEqual(info['bytes_used'], 246)
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', Timestamp.now().internal, 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEqual(info['object_count'], 2)
        self.assertEqual(info['bytes_used'], 1123)
        self.assertEqual(info['reported_object_count'], 0)
        self.assertEqual(info['reported_bytes_used'], 0)

        put_timestamp = Timestamp.now().internal
        sleep(.001)
        delete_timestamp = Timestamp.now().internal
        broker.reported(put_timestamp, delete_timestamp, 2, 1123)
        info = broker.get_info()
        self.assertEqual(info['object_count'], 2)
        self.assertEqual(info['bytes_used'], 1123)
        self.assertEqual(info['reported_put_timestamp'], put_timestamp)
        self.assertEqual(info['reported_delete_timestamp'], delete_timestamp)
        self.assertEqual(info['reported_object_count'], 2)
        self.assertEqual(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', Timestamp.now().internal)
        info = broker.get_info()
        self.assertEqual(info['object_count'], 1)
        self.assertEqual(info['bytes_used'], 1000)
        self.assertEqual(info['reported_object_count'], 2)
        self.assertEqual(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o2', Timestamp.now().internal)
        info = broker.get_info()
        self.assertEqual(info['object_count'], 0)
        self.assertEqual(info['bytes_used'], 0)
        self.assertEqual(info['reported_object_count'], 2)
        self.assertEqual(info['reported_bytes_used'], 1123)

    @with_tempdir
    def test_get_replication_info(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'hash.db')
        broker = ContainerBroker(
            db_path, account='myaccount', container='mycontainer')
        broker.initialize(next(ts_iter).internal, 0)
        metadata = {'blah': ['val', next(ts_iter).internal]}
        broker.update_metadata(metadata)
        expected = broker.get_info()
        expected['metadata'] = json.dumps(metadata)
        expected.pop('object_count')
        expected['count'] = 0
        expected['max_row'] = -1
        expected['shard_max_row'] = -1
        actual = broker.get_replication_info()
        self.assertEqual(expected, actual)

        broker.put_object('o1', next(ts_iter).internal, 123, 'text/plain',
                          'fake etag')
        expected = broker.get_info()
        expected['metadata'] = json.dumps(metadata)
        expected.pop('object_count')
        expected['count'] = 1
        expected['max_row'] = 1
        expected['shard_max_row'] = -1
        actual = broker.get_replication_info()
        self.assertEqual(expected, actual)

        sr = ShardRange('.shards_a/c', next(ts_iter))
        broker.merge_shard_ranges(sr)
        expected['shard_max_row'] = 1
        actual = broker.get_replication_info()
        self.assertEqual(expected, actual)

    @with_tempdir
    def test_remove_objects(self, tempdir):
        objects = (('undeleted', Timestamp.now().internal, 0, 'text/plain',
                    EMPTY_ETAG, 0, 0),
                   ('other_policy', Timestamp.now().internal, 0, 'text/plain',
                    EMPTY_ETAG, 0, 1),
                   ('deleted', Timestamp.now().internal, 0, 'text/plain',
                    EMPTY_ETAG, 1, 0))
        object_names = [o[0] for o in objects]

        def get_rows(broker):
            with broker.get() as conn:
                cursor = conn.execute("SELECT * FROM object")
                return [r[1] for r in cursor]

        def do_setup():
            db_path = os.path.join(
                tempdir, 'containers', 'part', 'suffix',
                'hash', '%s.db' % uuid4())
            broker = ContainerBroker(db_path, account='a', container='c')
            broker.initialize(Timestamp.now().internal, 0)
            for obj in objects:
                # ensure row order matches put order
                broker.put_object(*obj)
                broker._commit_puts()

            self.assertEqual(3, broker.get_max_row())  # sanity check
            self.assertEqual(object_names, get_rows(broker))  # sanity check
            return broker

        broker = do_setup()
        broker.remove_objects('', '')
        self.assertFalse(get_rows(broker))

        broker = do_setup()
        broker.remove_objects('deleted', '')
        self.assertEqual([object_names[2]], get_rows(broker))

        broker = do_setup()
        broker.remove_objects('', 'deleted', max_row=2)
        self.assertEqual(object_names, get_rows(broker))

        broker = do_setup()
        broker.remove_objects('deleted', 'un')
        self.assertEqual([object_names[0], object_names[2]], get_rows(broker))

        broker = do_setup()
        broker.remove_objects('', '', max_row=-1)
        self.assertEqual(object_names, get_rows(broker))

        broker = do_setup()
        broker.remove_objects('', '', max_row=0)
        self.assertEqual(object_names, get_rows(broker))

        broker = do_setup()
        broker.remove_objects('', '', max_row=1)
        self.assertEqual(object_names[1:], get_rows(broker))

        broker = do_setup()
        broker.remove_objects('', '', max_row=2)
        self.assertEqual(object_names[2:], get_rows(broker))

        broker = do_setup()
        broker.remove_objects('', '', max_row=3)
        self.assertFalse(get_rows(broker))

        broker = do_setup()
        broker.remove_objects('', '', max_row=99)
        self.assertFalse(get_rows(broker))

    def test_get_objects(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        ts_iter = make_timestamp_iter()
        objects_0 = [{'name': 'obj_0_%d' % i,
                      'created_at': next(ts_iter).normal,
                      'content_type': 'text/plain',
                      'etag': 'etag_%d' % i,
                      'size': 1024 * i,
                      'deleted': i % 2,
                      'storage_policy_index': 0
                      } for i in range(1, 8)]
        objects_1 = [{'name': 'obj_1_%d' % i,
                      'created_at': next(ts_iter).normal,
                      'content_type': 'text/plain',
                      'etag': 'etag_%d' % i,
                      'size': 1024 * i,
                      'deleted': i % 2,
                      'storage_policy_index': 1
                      } for i in range(1, 8)]
        # merge_objects mutates items
        broker.merge_items([dict(obj) for obj in objects_0 + objects_1])

        actual = broker.get_objects()
        self.assertEqual(objects_0 + objects_1, actual)

        with mock.patch('swift.container.backend.CONTAINER_LISTING_LIMIT', 2):
            actual = broker.get_objects()
            self.assertEqual(objects_0[:2], actual)

        with mock.patch('swift.container.backend.CONTAINER_LISTING_LIMIT', 2):
            actual = broker.get_objects(limit=9)
            self.assertEqual(objects_0 + objects_1[:2], actual)

        actual = broker.get_objects(marker=objects_0[2]['name'])
        self.assertEqual(objects_0[3:] + objects_1, actual)

        actual = broker.get_objects(end_marker=objects_0[2]['name'])
        self.assertEqual(objects_0[:2], actual)

        actual = broker.get_objects(include_deleted=True)
        self.assertEqual(objects_0[::2] + objects_1[::2], actual)

        actual = broker.get_objects(include_deleted=False)
        self.assertEqual(objects_0[1::2] + objects_1[1::2], actual)

        actual = broker.get_objects(include_deleted=None)
        self.assertEqual(objects_0 + objects_1, actual)

    def test_get_objects_since_row(self):
        ts_iter = make_timestamp_iter()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        obj_names = ['obj%03d' % i for i in range(20)]
        timestamps = [next(ts_iter) for o in obj_names]
        for name, timestamp in zip(obj_names, timestamps):
            broker.put_object(name, timestamp.internal,
                              0, 'text/plain', EMPTY_ETAG)
            broker._commit_puts()  # ensure predictable row order
        timestamps = [next(ts_iter) for o in obj_names[10:]]
        for name, timestamp in zip(obj_names[10:], timestamps):
            broker.put_object(name, timestamp.internal,
                              0, 'text/plain', EMPTY_ETAG, deleted=1)
            broker._commit_puts()  # ensure predictable row order

        # sanity check
        self.assertEqual(30, broker.get_max_row())
        actual = broker.get_objects()
        self.assertEqual(obj_names, [o['name'] for o in actual])

        # all rows included
        actual = broker.get_objects(since_row=None)
        self.assertEqual(obj_names, [o['name'] for o in actual])

        actual = broker.get_objects(since_row=-1)
        self.assertEqual(obj_names, [o['name'] for o in actual])

        # selected rows
        for since_row in range(10):
            actual = broker.get_objects(since_row=since_row)
            with annotate_failure(since_row):
                self.assertEqual(obj_names[since_row:],
                                 [o['name'] for o in actual])

        for since_row in range(10, 20):
            actual = broker.get_objects(since_row=since_row)
            with annotate_failure(since_row):
                self.assertEqual(obj_names[10:],
                                 [o['name'] for o in actual])

        for since_row in range(20, len(obj_names) + 1):
            actual = broker.get_objects(since_row=since_row)
            with annotate_failure(since_row):
                self.assertEqual(obj_names[since_row - 10:],
                                 [o['name'] for o in actual])

        self.assertFalse(broker.get_objects(end_marker=obj_names[5],
                                            since_row=5))

    def test_list_objects_iter(self):
        # Test ContainerBroker.list_objects_iter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        for obj1 in range(4):
            for obj2 in range(125):
                broker.put_object('%d/%04d' % (obj1, obj2),
                                  Timestamp.now().internal, 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e')
        for obj in range(125):
            broker.put_object('2/0051/%04d' % obj,
                              Timestamp.now().internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        for obj in range(125):
            broker.put_object('3/%04d/0049' % obj,
                              Timestamp.now().internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0/0000')
        self.assertEqual(listing[-1][0], '0/0099')

        listing = broker.list_objects_iter(100, '', '0/0050', None, '')
        self.assertEqual(len(listing), 50)
        self.assertEqual(listing[0][0], '0/0000')
        self.assertEqual(listing[-1][0], '0/0049')

        listing = broker.list_objects_iter(100, '0/0099', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0/0100')
        self.assertEqual(listing[-1][0], '1/0074')

        listing = broker.list_objects_iter(55, '1/0074', None, None, '')
        self.assertEqual(len(listing), 55)
        self.assertEqual(listing[0][0], '1/0075')
        self.assertEqual(listing[-1][0], '2/0004')

        listing = broker.list_objects_iter(55, '2/0005', None, None, '',
                                           reverse=True)
        self.assertEqual(len(listing), 55)
        self.assertEqual(listing[0][0], '2/0004')
        self.assertEqual(listing[-1][0], '1/0075')

        listing = broker.list_objects_iter(10, '', None, '0/01', '')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0/0100')
        self.assertEqual(listing[-1][0], '0/0109')

        listing = broker.list_objects_iter(10, '', None, '0/', '/')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0/0000')
        self.assertEqual(listing[-1][0], '0/0009')

        listing = broker.list_objects_iter(10, '', None, '0/', '/',
                                           reverse=True)
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0/0124')
        self.assertEqual(listing[-1][0], '0/0115')

        # Same as above, but using the path argument.
        listing = broker.list_objects_iter(10, '', None, None, '', '0')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0/0000')
        self.assertEqual(listing[-1][0], '0/0009')

        listing = broker.list_objects_iter(10, '', None, None, '', '0',
                                           reverse=True)
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0/0124')
        self.assertEqual(listing[-1][0], '0/0115')

        listing = broker.list_objects_iter(10, '', None, '', '/')
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['0/', '1/', '2/', '3/'])

        listing = broker.list_objects_iter(10, '', None, '', '/', reverse=True)
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['3/', '2/', '1/', '0/'])

        listing = broker.list_objects_iter(10, '2', None, None, '/')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['2/', '3/'])

        listing = broker.list_objects_iter(10, '2/', None, None, '/')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['3/'])

        listing = broker.list_objects_iter(10, '2/', None, None, '/',
                                           reverse=True)
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['1/', '0/'])

        listing = broker.list_objects_iter(10, '20', None, None, '/',
                                           reverse=True)
        self.assertEqual(len(listing), 3)
        self.assertEqual([row[0] for row in listing], ['2/', '1/', '0/'])

        listing = broker.list_objects_iter(10, '2/0050', None, '2/', '/')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '2/0051')
        self.assertEqual(listing[1][0], '2/0051/')
        self.assertEqual(listing[2][0], '2/0052')
        self.assertEqual(listing[-1][0], '2/0059')

        listing = broker.list_objects_iter(10, '3/0045', None, '3/', '/')
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3/0045/', '3/0046', '3/0046/', '3/0047',
                          '3/0047/', '3/0048', '3/0048/', '3/0049',
                          '3/0049/', '3/0050'])

        broker.put_object('3/0049/', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(10, '3/0048', None, None, None)
        self.assertEqual(len(listing), 10)
        self.assertEqual(
            [row[0] for row in listing],
            ['3/0048/0049', '3/0049', '3/0049/',
             '3/0049/0049', '3/0050', '3/0050/0049', '3/0051', '3/0051/0049',
             '3/0052', '3/0052/0049'])

        listing = broker.list_objects_iter(10, '3/0048', None, '3/', '/')
        self.assertEqual(len(listing), 10)
        self.assertEqual(
            [row[0] for row in listing],
            ['3/0048/', '3/0049', '3/0049/', '3/0050',
             '3/0050/', '3/0051', '3/0051/', '3/0052', '3/0052/', '3/0053'])

        listing = broker.list_objects_iter(10, None, None, '3/0049/', '/')
        self.assertEqual(len(listing), 2)
        self.assertEqual(
            [row[0] for row in listing],
            ['3/0049/', '3/0049/0049'])

        listing = broker.list_objects_iter(10, None, None, None, None,
                                           '3/0049')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['3/0049/0049'])

        listing = broker.list_objects_iter(2, None, None, '3/', '/')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['3/0000', '3/0000/'])

        listing = broker.list_objects_iter(2, None, None, None, None, '3')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['3/0000', '3/0001'])

    def test_reverse_prefix_delim(self):
        expectations = [
            {
                'objects': [
                    'topdir1/subdir1.0/obj1',
                    'topdir1/subdir1.1/obj1',
                    'topdir1/subdir1/obj1',
                ],
                'params': {
                    'prefix': 'topdir1/',
                    'delimiter': '/',
                },
                'expected': [
                    'topdir1/subdir1.0/',
                    'topdir1/subdir1.1/',
                    'topdir1/subdir1/',
                ],
            },
            {
                'objects': [
                    'topdir1/subdir1.0/obj1',
                    'topdir1/subdir1.1/obj1',
                    'topdir1/subdir1/obj1',
                    'topdir1/subdir10',
                    'topdir1/subdir10/obj1',
                ],
                'params': {
                    'prefix': 'topdir1/',
                    'delimiter': '/',
                },
                'expected': [
                    'topdir1/subdir1.0/',
                    'topdir1/subdir1.1/',
                    'topdir1/subdir1/',
                    'topdir1/subdir10',
                    'topdir1/subdir10/',
                ],
            },
            {
                'objects': [
                    'topdir1/subdir1/obj1',
                    'topdir1/subdir1.0/obj1',
                    'topdir1/subdir1.1/obj1',
                ],
                'params': {
                    'prefix': 'topdir1/',
                    'delimiter': '/',
                    'reverse': True,
                },
                'expected': [
                    'topdir1/subdir1/',
                    'topdir1/subdir1.1/',
                    'topdir1/subdir1.0/',
                ],
            },
            {
                'objects': [
                    'topdir1/subdir10/obj1',
                    'topdir1/subdir10',
                    'topdir1/subdir1/obj1',
                    'topdir1/subdir1.0/obj1',
                    'topdir1/subdir1.1/obj1',
                ],
                'params': {
                    'prefix': 'topdir1/',
                    'delimiter': '/',
                    'reverse': True,
                },
                'expected': [
                    'topdir1/subdir10/',
                    'topdir1/subdir10',
                    'topdir1/subdir1/',
                    'topdir1/subdir1.1/',
                    'topdir1/subdir1.0/',
                ],
            },
            {
                'objects': [
                    '1',
                    '2',
                    '3/1',
                    '3/2.2',
                    '3/2/1',
                    '3/2/2',
                    '3/3',
                    '4',
                ],
                'params': {
                    'path': '3/',
                },
                'expected': [
                    '3/1',
                    '3/2.2',
                    '3/3',
                ],
            },
            {
                'objects': [
                    '1',
                    '2',
                    '3/1',
                    '3/2.2',
                    '3/2/1',
                    '3/2/2',
                    '3/3',
                    '4',
                ],
                'params': {
                    'path': '3/',
                    'reverse': True,
                },
                'expected': [
                    '3/3',
                    '3/2.2',
                    '3/1',
                ],
            },
        ]
        ts = make_timestamp_iter()
        default_listing_params = {
            'limit': 10000,
            'marker': '',
            'end_marker': None,
            'prefix': None,
            'delimiter': None,
        }
        obj_create_params = {
            'size': 0,
            'content_type': 'application/test',
            'etag': EMPTY_ETAG,
        }
        failures = []
        for expected in expectations:
            broker = ContainerBroker(':memory:', account='a', container='c')
            broker.initialize(next(ts).internal, 0)
            for name in expected['objects']:
                broker.put_object(name, next(ts).internal, **obj_create_params)
            params = default_listing_params.copy()
            params.update(expected['params'])
            listing = list(o[0] for o in broker.list_objects_iter(**params))
            if listing != expected['expected']:
                expected['listing'] = listing
                failures.append(
                    "With objects %(objects)r, the params %(params)r "
                    "produced %(listing)r instead of %(expected)r" % expected)
        self.assertFalse(failures, "Found the following failures:\n%s" %
                         '\n'.join(failures))

    def test_list_objects_iter_non_slash(self):
        # Test ContainerBroker.list_objects_iter using a
        # delimiter that is not a slash
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        for obj1 in range(4):
            for obj2 in range(125):
                broker.put_object('%d:%04d' % (obj1, obj2),
                                  Timestamp.now().internal, 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e')
        for obj in range(125):
            broker.put_object('2:0051:%04d' % obj,
                              Timestamp.now().internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        for obj in range(125):
            broker.put_object('3:%04d:0049' % obj,
                              Timestamp.now().internal, 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        listing = broker.list_objects_iter(100, '', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0:0000')
        self.assertEqual(listing[-1][0], '0:0099')

        listing = broker.list_objects_iter(100, '', '0:0050', None, '')
        self.assertEqual(len(listing), 50)
        self.assertEqual(listing[0][0], '0:0000')
        self.assertEqual(listing[-1][0], '0:0049')

        listing = broker.list_objects_iter(100, '0:0099', None, None, '')
        self.assertEqual(len(listing), 100)
        self.assertEqual(listing[0][0], '0:0100')
        self.assertEqual(listing[-1][0], '1:0074')

        listing = broker.list_objects_iter(55, '1:0074', None, None, '')
        self.assertEqual(len(listing), 55)
        self.assertEqual(listing[0][0], '1:0075')
        self.assertEqual(listing[-1][0], '2:0004')

        listing = broker.list_objects_iter(10, '', None, '0:01', '')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0:0100')
        self.assertEqual(listing[-1][0], '0:0109')

        listing = broker.list_objects_iter(10, '', None, '0:', ':')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0:0000')
        self.assertEqual(listing[-1][0], '0:0009')

        # Same as above, but using the path argument, so nothing should be
        # returned since path uses a '/' as a delimiter.
        listing = broker.list_objects_iter(10, '', None, None, '', '0')
        self.assertEqual(len(listing), 0)

        listing = broker.list_objects_iter(10, '', None, '', ':')
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['0:', '1:', '2:', '3:'])

        listing = broker.list_objects_iter(10, '2', None, None, ':')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['2:', '3:'])

        listing = broker.list_objects_iter(10, '2:', None, None, ':')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['3:'])

        listing = broker.list_objects_iter(10, '2:0050', None, '2:', ':')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '2:0051')
        self.assertEqual(listing[1][0], '2:0051:')
        self.assertEqual(listing[2][0], '2:0052')
        self.assertEqual(listing[-1][0], '2:0059')

        listing = broker.list_objects_iter(10, '3:0045', None, '3:', ':')
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['3:0045:', '3:0046', '3:0046:', '3:0047',
                          '3:0047:', '3:0048', '3:0048:', '3:0049',
                          '3:0049:', '3:0050'])

        broker.put_object('3:0049:', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(10, '3:0048', None, None, None)
        self.assertEqual(len(listing), 10)
        self.assertEqual(
            [row[0] for row in listing],
            ['3:0048:0049', '3:0049', '3:0049:',
             '3:0049:0049', '3:0050', '3:0050:0049', '3:0051', '3:0051:0049',
             '3:0052', '3:0052:0049'])

        listing = broker.list_objects_iter(10, '3:0048', None, '3:', ':')
        self.assertEqual(len(listing), 10)
        self.assertEqual(
            [row[0] for row in listing],
            ['3:0048:', '3:0049', '3:0049:', '3:0050',
             '3:0050:', '3:0051', '3:0051:', '3:0052', '3:0052:', '3:0053'])

        listing = broker.list_objects_iter(10, None, None, '3:0049:', ':')
        self.assertEqual(len(listing), 2)
        self.assertEqual(
            [row[0] for row in listing],
            ['3:0049:', '3:0049:0049'])

        # Same as above, but using the path argument, so nothing should be
        # returned since path uses a '/' as a delimiter.
        listing = broker.list_objects_iter(10, None, None, None, None,
                                           '3:0049')
        self.assertEqual(len(listing), 0)

        listing = broker.list_objects_iter(2, None, None, '3:', ':')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['3:0000', '3:0000:'])

        listing = broker.list_objects_iter(2, None, None, None, None, '3')
        self.assertEqual(len(listing), 0)

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

        # def list_objects_iter(self, limit, marker, prefix, delimiter,
        #                       path=None, format=None):
        listing = broker.list_objects_iter(100, None, None, '/pets/f', '/')
        self.assertEqual([row[0] for row in listing],
                         ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish', '/')
        self.assertEqual([row[0] for row in listing],
                         ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish/', '/')
        self.assertEqual([row[0] for row in listing],
                         ['/pets/fish/a', '/pets/fish/b'])
        listing = broker.list_objects_iter(100, None, None, None, '/')
        self.assertEqual([row[0] for row in listing],
                         ['/'])

    def test_list_objects_iter_order_and_reverse(self):
        # Test ContainerBroker.list_objects_iter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)

        broker.put_object(
            'o1', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            'o10', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            'O1', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            'o2', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            'o3', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            'O4', Timestamp(0).internal, 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')

        listing = broker.list_objects_iter(100, None, None, '', '',
                                           reverse=False)
        self.assertEqual([row[0] for row in listing],
                         ['O1', 'O4', 'o1', 'o10', 'o2', 'o3'])
        listing = broker.list_objects_iter(100, None, None, '', '',
                                           reverse=True)
        self.assertEqual([row[0] for row in listing],
                         ['o3', 'o2', 'o10', 'o1', 'O4', 'O1'])
        listing = broker.list_objects_iter(2, None, None, '', '',
                                           reverse=True)
        self.assertEqual([row[0] for row in listing],
                         ['o3', 'o2'])
        listing = broker.list_objects_iter(100, 'o2', 'O4', '', '',
                                           reverse=True)
        self.assertEqual([row[0] for row in listing],
                         ['o10', 'o1'])

    def test_double_check_trailing_delimiter(self):
        # Test ContainerBroker.list_objects_iter for a
        # container that has an odd file with a trailing delimiter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a/a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a/b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b/a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b/b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('c', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('00', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/00', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1/', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1/0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1/', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1/0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(25, None, None, None, None)
        self.assertEqual(len(listing), 22)
        self.assertEqual(
            [row[0] for row in listing],
            ['0', '0/', '0/0', '0/00', '0/1', '0/1/', '0/1/0', '00', '1', '1/',
             '1/0', 'a', 'a/', 'a/0', 'a/a', 'a/a/a', 'a/a/b', 'a/b', 'b',
             'b/a', 'b/b', 'c'])
        listing = broker.list_objects_iter(25, None, None, '', '/')
        self.assertEqual(len(listing), 10)
        self.assertEqual(
            [row[0] for row in listing],
            ['0', '0/', '00', '1', '1/', 'a', 'a/', 'b', 'b/', 'c'])
        listing = broker.list_objects_iter(25, None, None, 'a/', '/')
        self.assertEqual(len(listing), 5)
        self.assertEqual(
            [row[0] for row in listing],
            ['a/', 'a/0', 'a/a', 'a/a/', 'a/b'])
        listing = broker.list_objects_iter(25, None, None, '0/', '/')
        self.assertEqual(len(listing), 5)
        self.assertEqual(
            [row[0] for row in listing],
            ['0/', '0/0', '0/00', '0/1', '0/1/'])
        listing = broker.list_objects_iter(25, None, None, '0/1/', '/')
        self.assertEqual(len(listing), 2)
        self.assertEqual(
            [row[0] for row in listing],
            ['0/1/', '0/1/0'])
        listing = broker.list_objects_iter(25, None, None, 'b/', '/')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['b/a', 'b/b'])

    def test_double_check_trailing_delimiter_non_slash(self):
        # Test ContainerBroker.list_objects_iter for a
        # container that has an odd file with a trailing delimiter
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a:a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a:b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b:a', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b:b', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('c', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('00', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:00', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1:', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1:0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1:', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1:0', Timestamp.now().internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(25, None, None, None, None)
        self.assertEqual(len(listing), 22)
        self.assertEqual(
            [row[0] for row in listing],
            ['0', '00', '0:', '0:0', '0:00', '0:1', '0:1:', '0:1:0', '1', '1:',
             '1:0', 'a', 'a:', 'a:0', 'a:a', 'a:a:a', 'a:a:b', 'a:b', 'b',
             'b:a', 'b:b', 'c'])
        listing = broker.list_objects_iter(25, None, None, '', ':')
        self.assertEqual(len(listing), 10)
        self.assertEqual(
            [row[0] for row in listing],
            ['0', '00', '0:', '1', '1:', 'a', 'a:', 'b', 'b:', 'c'])
        listing = broker.list_objects_iter(25, None, None, 'a:', ':')
        self.assertEqual(len(listing), 5)
        self.assertEqual(
            [row[0] for row in listing],
            ['a:', 'a:0', 'a:a', 'a:a:', 'a:b'])
        listing = broker.list_objects_iter(25, None, None, '0:', ':')
        self.assertEqual(len(listing), 5)
        self.assertEqual(
            [row[0] for row in listing],
            ['0:', '0:0', '0:00', '0:1', '0:1:'])
        listing = broker.list_objects_iter(25, None, None, '0:1:', ':')
        self.assertEqual(len(listing), 2)
        self.assertEqual(
            [row[0] for row in listing],
            ['0:1:', '0:1:0'])
        listing = broker.list_objects_iter(25, None, None, 'b:', ':')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['b:a', 'b:b'])

    def test_chexor(self):
        def md5_str(s):
            if not isinstance(s, bytes):
                s = s.encode('utf8')
            return hashlib.md5(s).hexdigest()

        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        broker.put_object('a', Timestamp(1).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', Timestamp(2).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hasha = md5_str('%s-%s' % ('a', Timestamp(1).internal))
        hashb = md5_str('%s-%s' % ('b', Timestamp(2).internal))
        hashc = '%032x' % (int(hasha, 16) ^ int(hashb, 16))
        self.assertEqual(broker.get_info()['hash'], hashc)
        broker.put_object('b', Timestamp(3).internal, 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hashb = md5_str('%s-%s' % ('b', Timestamp(3).internal))
        hashc = '%032x' % (int(hasha, 16) ^ int(hashb, 16))
        self.assertEqual(broker.get_info()['hash'], hashc)

    def test_newid(self):
        # test DatabaseBroker.newid
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        id = broker.get_info()['id']
        broker.newid('someid')
        self.assertNotEqual(id, broker.get_info()['id'])

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
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]['name'], 'b')

    def test_sync_merging(self):
        # exercise the DatabaseBroker sync functions a bit
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(Timestamp('1').internal, 0)
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(Timestamp('1').internal, 0)
        self.assertEqual(broker2.get_sync('12345'), -1)
        broker1.merge_syncs([{'sync_point': 3, 'remote_id': '12345'}])
        broker2.merge_syncs(broker1.get_syncs())
        self.assertEqual(broker2.get_sync('12345'), 3)

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
        self.assertEqual(len(items), 2)
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_object('c', Timestamp(3).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(len(items), 3)
        self.assertEqual(['a', 'b', 'c'],
                         sorted([rec['name'] for rec in items]))

    @with_tempdir
    def test_merge_items_is_green(self, tempdir):
        ts = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')

        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts).internal, 1)

        broker.put_object('b', next(ts).internal, 0, 'text/plain',
                          EMPTY_ETAG)

        with mock.patch('swift.container.backend.tpool') as mock_tpool:
            broker.get_info()
        mock_tpool.execute.assert_called_once()

    def test_merge_items_overwrite_unicode(self):
        # test DatabaseBroker.merge_items
        snowman = u'\N{SNOWMAN}'
        if six.PY2:
            snowman = snowman.encode('utf-8')
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
        self.assertEqual(['b', snowman],
                         sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == snowman:
                self.assertEqual(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEqual(rec['created_at'], Timestamp(3).internal)

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
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEqual(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEqual(rec['created_at'], Timestamp(3).internal)

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
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEqual(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEqual(rec['created_at'], Timestamp(3).internal)
                self.assertEqual(rec['content_type'], 'text/plain')
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEqual(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEqual(rec['created_at'], Timestamp(3).internal)
        broker1.put_object('b', Timestamp(5).internal, 0,
                           'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEqual(rec['created_at'], Timestamp(4).internal)
            if rec['name'] == 'b':
                self.assertEqual(rec['created_at'], Timestamp(5).internal)
                self.assertEqual(rec['content_type'], 'text/plain')

    def test_set_storage_policy_index(self):
        ts = make_timestamp_iter()
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        timestamp = next(ts)
        broker.initialize(timestamp.internal, 0)

        info = broker.get_info()
        self.assertEqual(0, info['storage_policy_index'])  # sanity check
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        if self.__class__ in (TestContainerBrokerBeforeMetadata,
                              TestContainerBrokerBeforeXSync,
                              TestContainerBrokerBeforeSPI,
                              TestContainerBrokerBeforeShardRanges):
            self.assertEqual(info['status_changed_at'], '0')
        else:
            self.assertEqual(timestamp.internal, info['status_changed_at'])
        expected = {0: {'object_count': 0, 'bytes_used': 0}}
        self.assertEqual(expected, broker.get_policy_stats())

        timestamp = next(ts)
        broker.set_storage_policy_index(111, timestamp.internal)
        self.assertEqual(broker.storage_policy_index, 111)
        info = broker.get_info()
        self.assertEqual(111, info['storage_policy_index'])
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        self.assertEqual(timestamp.internal, info['status_changed_at'])
        expected[111] = {'object_count': 0, 'bytes_used': 0}
        self.assertEqual(expected, broker.get_policy_stats())

        timestamp = next(ts)
        broker.set_storage_policy_index(222, timestamp.internal)
        self.assertEqual(broker.storage_policy_index, 222)
        info = broker.get_info()
        self.assertEqual(222, info['storage_policy_index'])
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        self.assertEqual(timestamp.internal, info['status_changed_at'])
        expected[222] = {'object_count': 0, 'bytes_used': 0}
        self.assertEqual(expected, broker.get_policy_stats())

        old_timestamp, timestamp = timestamp, next(ts)
        # setting again is idempotent
        broker.set_storage_policy_index(222, timestamp.internal)
        info = broker.get_info()
        self.assertEqual(222, info['storage_policy_index'])
        self.assertEqual(0, info['object_count'])
        self.assertEqual(0, info['bytes_used'])
        self.assertEqual(old_timestamp.internal, info['status_changed_at'])
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
        self.assertEqual(-1, broker.get_reconciler_sync())
        broker.update_reconciler_sync(10)
        self.assertEqual(10, broker.get_reconciler_sync())

    @with_tempdir
    def test_legacy_pending_files(self, tempdir):
        ts = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')

        # first init an acct DB without the policy_stat table present
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts).internal, 1)

        # manually make some pending entries lacking storage_policy_index
        with open(broker.pending_file, 'a+b') as fp:
            for i in range(10):
                name, timestamp, size, content_type, etag, deleted = (
                    'o%s' % i, next(ts).internal, 0, 'c', 'e', 0)
                fp.write(b':')
                fp.write(base64.b64encode(pickle.dumps(
                    (name, timestamp, size, content_type, etag, deleted),
                    protocol=2)))
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
            broker.put_object(name, next(ts).internal, size, 'c', 'e', 0,
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

    @with_tempdir
    def test_get_info_no_stale_reads(self, tempdir):
        ts = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')

        def mock_commit_puts():
            raise sqlite3.OperationalError('unable to open database file')

        broker = ContainerBroker(db_path, account='a', container='c',
                                 stale_reads_ok=False)
        broker.initialize(next(ts).internal, 1)

        # manually make some pending entries
        with open(broker.pending_file, 'a+b') as fp:
            for i in range(10):
                name, timestamp, size, content_type, etag, deleted = (
                    'o%s' % i, next(ts).internal, 0, 'c', 'e', 0)
                fp.write(b':')
                fp.write(base64.b64encode(pickle.dumps(
                    (name, timestamp, size, content_type, etag, deleted),
                    protocol=2)))
                fp.flush()

        broker._commit_puts = mock_commit_puts
        with self.assertRaises(sqlite3.OperationalError) as exc_context:
            broker.get_info()
        self.assertIn('unable to open database file',
                      str(exc_context.exception))

    @with_tempdir
    def test_get_info_stale_read_ok(self, tempdir):
        ts = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')

        def mock_commit_puts():
            raise sqlite3.OperationalError('unable to open database file')

        broker = ContainerBroker(db_path, account='a', container='c',
                                 stale_reads_ok=True)
        broker.initialize(next(ts).internal, 1)

        # manually make some pending entries
        with open(broker.pending_file, 'a+b') as fp:
            for i in range(10):
                name, timestamp, size, content_type, etag, deleted = (
                    'o%s' % i, next(ts).internal, 0, 'c', 'e', 0)
                fp.write(b':')
                fp.write(base64.b64encode(pickle.dumps(
                    (name, timestamp, size, content_type, etag, deleted),
                    protocol=2)))
                fp.flush()

        broker._commit_puts = mock_commit_puts
        broker.get_info()

    @with_tempdir
    def test_create_broker(self, tempdir):
        broker = ContainerBroker.create_broker(tempdir, 0, 'a', 'c')
        hsh = hash_path('a', 'c')
        expected_path = os.path.join(
            tempdir, 'containers', '0', hsh[-3:], hsh, hsh + '.db')
        self.assertEqual(expected_path, broker.db_file)
        self.assertTrue(os.path.isfile(expected_path))

        ts = Timestamp.now()
        broker = ContainerBroker.create_broker(tempdir, 0, 'a', 'c1',
                                               put_timestamp=ts.internal)
        hsh = hash_path('a', 'c1')
        expected_path = os.path.join(
            tempdir, 'containers', '0', hsh[-3:], hsh, hsh + '.db')
        self.assertEqual(expected_path, broker.db_file)
        self.assertTrue(os.path.isfile(expected_path))
        self.assertEqual(ts.internal, broker.get_info()['put_timestamp'])
        self.assertEqual(0, broker.get_info()['storage_policy_index'])

        epoch = Timestamp.now()
        broker = ContainerBroker.create_broker(tempdir, 0, 'a', 'c3',
                                               epoch=epoch)
        hsh = hash_path('a', 'c3')
        expected_path = os.path.join(
            tempdir, 'containers', '0', hsh[-3:],
            hsh, '%s_%s.db' % (hsh, epoch.internal))
        self.assertEqual(expected_path, broker.db_file)

    @with_tempdir
    def test_pending_file_name(self, tempdir):
        # pending file should have same name for sharded or unsharded db
        expected_pending_path = os.path.join(tempdir, 'container.db.pending')

        db_path = os.path.join(tempdir, 'container.db')
        fresh_db_path = os.path.join(tempdir, 'container_epoch.db')

        def do_test(given_db_file, expected_db_file):
            broker = ContainerBroker(given_db_file, account='a', container='c')
            self.assertEqual(expected_pending_path, broker.pending_file)
            self.assertEqual(expected_db_file, broker.db_file)

        # no files exist
        do_test(db_path, db_path)
        do_test(fresh_db_path, fresh_db_path)

        # only container.db exists - unsharded
        with open(db_path, 'wb'):
            pass
        do_test(db_path, db_path)
        do_test(fresh_db_path, db_path)

        # container.db and container_shard.db exist - sharding
        with open(fresh_db_path, 'wb'):
            pass
        do_test(db_path, fresh_db_path)
        do_test(fresh_db_path, fresh_db_path)

        # only container_shard.db exists - sharded
        os.unlink(db_path)
        do_test(db_path, fresh_db_path)
        do_test(fresh_db_path, fresh_db_path)

    @with_tempdir
    def test_sharding_sysmeta(self, tempdir):
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(
            db_path, account='myaccount', container='mycontainer')
        broker.initialize(Timestamp.now().internal)

        expected = 'aaa/ccc'
        with mock_timestamp_now() as now:
            broker.set_sharding_sysmeta('Root', expected)
        actual = broker.metadata
        self.assertEqual([expected, now.internal],
                         actual.get('X-Container-Sysmeta-Shard-Root'))
        self.assertEqual(expected, broker.get_sharding_sysmeta('Root'))

        expected = {'key': 'value'}
        with mock_timestamp_now() as now:
            broker.set_sharding_sysmeta('test', expected)
        actual = broker.metadata
        self.assertEqual([expected, now.internal],
                         actual.get('X-Container-Sysmeta-Shard-test'))
        self.assertEqual(expected, broker.get_sharding_sysmeta('test'))

    @with_tempdir
    def test_path(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(
            db_path, account='myaccount', container='mycontainer')
        broker.initialize(next(ts_iter).internal, 1)
        # make sure we can cope with unitialized account and container
        broker.account = broker.container = None
        self.assertEqual('myaccount/mycontainer', broker.path)

    @with_tempdir
    def test_root_account_container_path(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(
            db_path, account='root_a', container='root_c')
        broker.initialize(next(ts_iter).internal, 1)
        # make sure we can cope with unitialized account and container
        broker.account = broker.container = None

        self.assertEqual('root_a', broker.root_account)
        self.assertEqual('root_c', broker.root_container)
        self.assertEqual('root_a/root_c', broker.root_path)
        self.assertTrue(broker.is_root_container())
        self.assertEqual('root_a', broker.account)  # sanity check
        self.assertEqual('root_c', broker.container)  # sanity check

        # we don't expect root containers to have this sysmeta set but if it is
        # the broker should still behave like a root container
        metadata = {
            'X-Container-Sysmeta-Shard-Root':
                ('root_a/root_c', next(ts_iter).internal)}
        broker = ContainerBroker(
            db_path, account='root_a', container='root_c')
        broker.update_metadata(metadata)
        broker.account = broker.container = None
        self.assertEqual('root_a', broker.root_account)
        self.assertEqual('root_c', broker.root_container)
        self.assertEqual('root_a/root_c', broker.root_path)
        self.assertTrue(broker.is_root_container())

        # if root is marked deleted, it still considers itself to be a root
        broker.delete_db(next(ts_iter).internal)
        self.assertEqual('root_a', broker.root_account)
        self.assertEqual('root_c', broker.root_container)
        self.assertEqual('root_a/root_c', broker.root_path)
        self.assertTrue(broker.is_root_container())
        # check the values are not just being cached
        broker = ContainerBroker(db_path)
        self.assertEqual('root_a', broker.root_account)
        self.assertEqual('root_c', broker.root_container)
        self.assertEqual('root_a/root_c', broker.root_path)
        self.assertTrue(broker.is_root_container())

        # check a shard container
        db_path = os.path.join(tempdir, 'shard_container.db')
        broker = ContainerBroker(
            db_path, account='.shards_root_a', container='c_shard')
        broker.initialize(next(ts_iter).internal, 1)
        # now the metadata is significant...
        metadata = {
            'X-Container-Sysmeta-Shard-Root':
                ('root_a/root_c', next(ts_iter).internal)}
        broker.update_metadata(metadata)
        broker.account = broker.container = None
        broker._root_account = broker._root_container = None

        self.assertEqual('root_a', broker.root_account)
        self.assertEqual('root_c', broker.root_container)
        self.assertEqual('root_a/root_c', broker.root_path)
        self.assertFalse(broker.is_root_container())

        # check validation
        def check_validation(root_value):
            metadata = {
                'X-Container-Sysmeta-Shard-Root':
                    (root_value, next(ts_iter).internal)}
            broker.update_metadata(metadata)
            broker.account = broker.container = None
            broker._root_account = broker._root_container = None
            with self.assertRaises(ValueError) as cm:
                broker.root_account
            self.assertIn('Expected X-Container-Sysmeta-Shard-Root',
                          str(cm.exception))
            with self.assertRaises(ValueError):
                broker.root_container

        check_validation('root_a')
        check_validation('/root_a')
        check_validation('/root_a/root_c')
        check_validation('/root_a/root_c/blah')
        check_validation('/')

    def test_resolve_shard_range_states(self):
        self.assertIsNone(ContainerBroker.resolve_shard_range_states(None))
        self.assertIsNone(ContainerBroker.resolve_shard_range_states([]))

        for state_num, state_name in ShardRange.STATES.items():
            self.assertEqual({state_num},
                             ContainerBroker.resolve_shard_range_states(
                                 [state_name]))
            self.assertEqual({state_num},
                             ContainerBroker.resolve_shard_range_states(
                                 [state_num]))

        self.assertEqual(set(ShardRange.STATES),
                         ContainerBroker.resolve_shard_range_states(
                         ShardRange.STATES_BY_NAME))

        self.assertEqual(
            set(ShardRange.STATES),
            ContainerBroker.resolve_shard_range_states(ShardRange.STATES))

        # check aliases
        self.assertEqual(
            {ShardRange.CLEAVED, ShardRange.ACTIVE, ShardRange.SHARDING,
             ShardRange.SHRINKING},
            ContainerBroker.resolve_shard_range_states(['listing']))

        self.assertEqual(
            {ShardRange.CLEAVED, ShardRange.ACTIVE, ShardRange.SHARDING,
             ShardRange.SHRINKING},
            ContainerBroker.resolve_shard_range_states(['listing', 'active']))

        self.assertEqual(
            {ShardRange.CLEAVED, ShardRange.ACTIVE, ShardRange.SHARDING,
             ShardRange.SHRINKING, ShardRange.CREATED},
            ContainerBroker.resolve_shard_range_states(['listing', 'created']))

        self.assertEqual(
            {ShardRange.CREATED, ShardRange.CLEAVED, ShardRange.ACTIVE,
             ShardRange.SHARDING},
            ContainerBroker.resolve_shard_range_states(['updating']))

        self.assertEqual(
            {ShardRange.CREATED, ShardRange.CLEAVED, ShardRange.ACTIVE,
             ShardRange.SHARDING, ShardRange.SHRINKING},
            ContainerBroker.resolve_shard_range_states(
                ['updating', 'listing']))

        def check_bad_value(value):
            with self.assertRaises(ValueError) as cm:
                ContainerBroker.resolve_shard_range_states(value)
            self.assertIn('Invalid state', str(cm.exception))

        check_bad_value(['bad_state', 'active'])
        check_bad_value([''])
        check_bad_value('active')

    @with_tempdir
    def test_get_shard_ranges(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)

        # no rows
        self.assertFalse(broker.get_shard_ranges())
        # check that a default own shard range is not generated
        self.assertFalse(broker.get_shard_ranges(include_own=True))

        # merge row for own shard range
        own_shard_range = ShardRange(broker.path, next(ts_iter), 'l', 'u',
                                     state=ShardRange.SHARDING)
        broker.merge_shard_ranges([own_shard_range])
        self.assertFalse(broker.get_shard_ranges())
        self.assertFalse(broker.get_shard_ranges(include_own=False))

        actual = broker.get_shard_ranges(include_own=True)
        self.assertEqual([dict(sr) for sr in [own_shard_range]],
                         [dict(sr) for sr in actual])

        # merge rows for other shard ranges
        shard_ranges = [
            ShardRange('.a/c0', next(ts_iter), 'a', 'c'),
            ShardRange('.a/c1', next(ts_iter), 'c', 'd'),
            ShardRange('.a/c2', next(ts_iter), 'd', 'f',
                       state=ShardRange.ACTIVE),
            ShardRange('.a/c3', next(ts_iter), 'e', 'f', deleted=1,
                       state=ShardRange.SHARDED,),
            ShardRange('.a/c4', next(ts_iter), 'f', 'h',
                       state=ShardRange.CREATED),
            ShardRange('.a/c5', next(ts_iter), 'h', 'j', deleted=1)
        ]
        broker.merge_shard_ranges(shard_ranges)
        actual = broker.get_shard_ranges()
        undeleted = shard_ranges[:3] + shard_ranges[4:5]
        self.assertEqual([dict(sr) for sr in undeleted],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(include_deleted=True)
        self.assertEqual([dict(sr) for sr in shard_ranges],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(reverse=True)
        self.assertEqual([dict(sr) for sr in reversed(undeleted)],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(marker='c', end_marker='e')
        self.assertEqual([dict(sr) for sr in shard_ranges[1:3]],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(marker='c', end_marker='e',
                                         states=ShardRange.ACTIVE)
        self.assertEqual([dict(sr) for sr in shard_ranges[2:3]],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(marker='e', end_marker='e')
        self.assertFalse([dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(includes='f')
        self.assertEqual([dict(sr) for sr in shard_ranges[2:3]],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(includes='i')
        self.assertFalse(actual)

        actual = broker.get_shard_ranges(
            states=[ShardRange.CREATED, ShardRange.ACTIVE])
        self.assertEqual(
            [dict(sr) for sr in [shard_ranges[2], shard_ranges[4]]],
            [dict(sr) for sr in actual])

        # get everything
        actual = broker.get_shard_ranges(include_own=True)
        self.assertEqual([dict(sr) for sr in undeleted + [own_shard_range]],
                         [dict(sr) for sr in actual])

        # get just own range
        actual = broker.get_shard_ranges(include_own=True, exclude_others=True)
        self.assertEqual([dict(sr) for sr in [own_shard_range]],
                         [dict(sr) for sr in actual])

        # if you ask for nothing you'll get nothing
        actual = broker.get_shard_ranges(
            include_own=False, exclude_others=True)
        self.assertFalse(actual)

    @with_tempdir
    def test_get_shard_ranges_with_sharding_overlaps(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)
        shard_ranges = [
            ShardRange('.shards_a/c0', next(ts_iter), 'a', 'd',
                       state=ShardRange.ACTIVE),
            ShardRange('.shards_a/c1_0', next(ts_iter), 'd', 'g',
                       state=ShardRange.CLEAVED),
            ShardRange('.shards_a/c1_1', next(ts_iter), 'g', 'j',
                       state=ShardRange.CLEAVED),
            ShardRange('.shards_a/c1_2', next(ts_iter), 'j', 'm',
                       state=ShardRange.CREATED),
            ShardRange('.shards_a/c1', next(ts_iter), 'd', 'm',
                       state=ShardRange.SHARDING),
            ShardRange('.shards_a/c2', next(ts_iter), 'm', '',
                       state=ShardRange.ACTIVE),
        ]
        broker.merge_shard_ranges(
            random.sample(shard_ranges, len(shard_ranges)))
        actual = broker.get_shard_ranges()
        self.assertEqual([dict(sr) for sr in shard_ranges],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(states=SHARD_LISTING_STATES)
        self.assertEqual(
            [dict(sr) for sr in shard_ranges[:3] + shard_ranges[4:]],
            [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(states=SHARD_UPDATE_STATES,
                                         includes='e')
        self.assertEqual([shard_ranges[1]], actual)
        actual = broker.get_shard_ranges(states=SHARD_UPDATE_STATES,
                                         includes='j')
        self.assertEqual([shard_ranges[2]], actual)
        actual = broker.get_shard_ranges(states=SHARD_UPDATE_STATES,
                                         includes='k')
        self.assertEqual([shard_ranges[3]], actual)

    @with_tempdir
    def test_get_shard_ranges_with_shrinking_overlaps(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)
        shard_ranges = [
            ShardRange('.shards_a/c0', next(ts_iter), 'a', 'k',
                       state=ShardRange.ACTIVE),
            ShardRange('.shards_a/c1', next(ts_iter), 'k', 'm',
                       state=ShardRange.SHRINKING),
            ShardRange('.shards_a/c2', next(ts_iter), 'k', 't',
                       state=ShardRange.ACTIVE),
            ShardRange('.shards_a/c3', next(ts_iter), 't', '',
                       state=ShardRange.ACTIVE),
        ]
        broker.merge_shard_ranges(
            random.sample(shard_ranges, len(shard_ranges)))
        actual = broker.get_shard_ranges()
        self.assertEqual([dict(sr) for sr in shard_ranges],
                         [dict(sr) for sr in actual])

        actual = broker.get_shard_ranges(states=SHARD_UPDATE_STATES,
                                         includes='l')
        self.assertEqual([shard_ranges[2]], actual)

    @with_tempdir
    def test_get_own_shard_range(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(
            db_path, account='.shards_a', container='shard_c')
        broker.initialize(next(ts_iter).internal, 0)

        # no row for own shard range - expect entire namespace default
        now = Timestamp.now()
        expected = ShardRange(broker.path, now, '', '', 0, 0, now,
                              state=ShardRange.ACTIVE)
        with mock.patch('swift.container.backend.Timestamp.now',
                        return_value=now):
            actual = broker.get_own_shard_range()
        self.assertEqual(dict(expected), dict(actual))

        actual = broker.get_own_shard_range(no_default=True)
        self.assertIsNone(actual)

        # row for own shard range and others
        ts_1 = next(ts_iter)
        own_sr = ShardRange(broker.path, ts_1, 'l', 'u')
        broker.merge_shard_ranges(
            [own_sr,
             ShardRange('.a/c1', next(ts_iter), 'b', 'c'),
             ShardRange('.a/c2', next(ts_iter), 'c', 'd')])
        expected = ShardRange(broker.path, ts_1, 'l', 'u', 0, 0, now)
        with mock.patch('swift.container.backend.Timestamp.now',
                        return_value=now):
            actual = broker.get_own_shard_range()
        self.assertEqual(dict(expected), dict(actual))

        # check stats get updated
        broker.put_object(
            'o1', next(ts_iter).internal, 100, 'text/plain', 'etag1')
        broker.put_object(
            'o2', next(ts_iter).internal, 99, 'text/plain', 'etag2')
        expected = ShardRange(
            broker.path, ts_1, 'l', 'u', 2, 199, now)
        with mock.patch('swift.container.backend.Timestamp.now',
                        return_value=now):
            actual = broker.get_own_shard_range()
        self.assertEqual(dict(expected), dict(actual))

        # still returned when deleted
        delete_ts = next(ts_iter)
        own_sr.set_deleted(timestamp=delete_ts)
        broker.merge_shard_ranges(own_sr)
        with mock.patch('swift.container.backend.Timestamp.now',
                        return_value=now):
            actual = broker.get_own_shard_range()
        expected = ShardRange(
            broker.path, delete_ts, 'l', 'u', 2, 199, now, deleted=True)
        self.assertEqual(dict(expected), dict(actual))

        # still in table after reclaim_age
        broker.reclaim(next(ts_iter).internal, next(ts_iter).internal)
        with mock.patch('swift.container.backend.Timestamp.now',
                        return_value=now):
            actual = broker.get_own_shard_range()
        self.assertEqual(dict(expected), dict(actual))

        # entire namespace
        ts_2 = next(ts_iter)
        broker.merge_shard_ranges(
            [ShardRange(broker.path, ts_2, '', '')])
        expected = ShardRange(
            broker.path, ts_2, '', '', 2, 199, now)
        with mock.patch('swift.container.backend.Timestamp.now',
                        return_value=now):
            actual = broker.get_own_shard_range()
        self.assertEqual(dict(expected), dict(actual))

    @with_tempdir
    def test_enable_sharding(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'container.db')
        broker = ContainerBroker(
            db_path, account='.shards_a', container='shard_c')
        broker.initialize(next(ts_iter).internal, 0)
        epoch = next(ts_iter)
        broker.enable_sharding(epoch)
        own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(epoch, own_sr.epoch)
        self.assertEqual(epoch, own_sr.state_timestamp)
        self.assertEqual(ShardRange.SHARDING, own_sr.state)

    @with_tempdir
    def test_get_shard_usage(self, tempdir):
        ts_iter = make_timestamp_iter()
        shard_range_by_state = dict(
            (state, ShardRange('.shards_a/c_%s' % state, next(ts_iter),
                               str(state), str(state + 1),
                               2 * state, 2 * state + 1, 2,
                               state=state))
            for state in ShardRange.STATES)

        def make_broker(a, c):
            db_path = os.path.join(tempdir, '%s.db' % uuid4())
            broker = ContainerBroker(db_path, account=a, container=c)
            broker.initialize(next(ts_iter).internal, 0)
            broker.set_sharding_sysmeta('Root', 'a/c')
            broker.merge_shard_ranges(list(shard_range_by_state.values()))
            return broker

        # make broker appear to be a root container
        broker = make_broker('a', 'c')
        self.assertTrue(broker.is_root_container())
        included_states = (ShardRange.ACTIVE, ShardRange.SHARDING,
                           ShardRange.SHRINKING)
        included = [shard_range_by_state[state] for state in included_states]
        expected = {
            'object_count': sum([sr.object_count for sr in included]),
            'bytes_used': sum([sr.bytes_used for sr in included])
        }
        self.assertEqual(expected, broker.get_shard_usage())

    @with_tempdir
    def _check_find_shard_ranges(self, c_lower, c_upper, tempdir):
        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()
        container_name = 'test_container'

        def do_test(expected_bounds, expected_last_found, shard_size, limit,
                    start_index=0, existing=None):
            # expected_bounds is a list of tuples (lower, upper, object_count)
            # build expected shard ranges
            expected_shard_ranges = [
                dict(lower=lower, upper=upper, index=index,
                     object_count=object_count)
                for index, (lower, upper, object_count)
                in enumerate(expected_bounds, start_index)]

            with mock.patch('swift.common.utils.time.time',
                            return_value=float(ts_now.normal)):
                ranges, last_found = broker.find_shard_ranges(
                    shard_size, limit=limit, existing_ranges=existing)
            self.assertEqual(expected_shard_ranges, ranges)
            self.assertEqual(expected_last_found, last_found)

        db_path = os.path.join(tempdir, 'test_container.db')
        broker = ContainerBroker(
            db_path, account='a', container=container_name)
        # shard size > object count, no objects
        broker.initialize(next(ts_iter).internal, 0)

        ts = next(ts_iter)
        if c_lower or c_upper:
            # testing a shard, so set its own shard range
            own_shard_range = ShardRange(broker.path, ts, c_lower, c_upper)
            broker.merge_shard_ranges([own_shard_range])

        self.assertEqual(([], False), broker.find_shard_ranges(10))

        for i in range(10):
            broker.put_object(
                'obj%02d' % i, next(ts_iter).internal, 0, 'text/plain', 'etag')

        expected_bounds = [(c_lower, 'obj04', 5), ('obj04', c_upper, 5)]
        do_test(expected_bounds, True, shard_size=5, limit=None)

        expected = [(c_lower, 'obj06', 7), ('obj06', c_upper, 3)]
        do_test(expected, True, shard_size=7, limit=None)
        expected = [(c_lower, 'obj08', 9), ('obj08', c_upper, 1)]
        do_test(expected, True, shard_size=9, limit=None)
        # shard size >= object count
        do_test([], False, shard_size=10, limit=None)
        do_test([], False, shard_size=11, limit=None)

        # check use of limit
        do_test([], False, shard_size=4, limit=0)
        expected = [(c_lower, 'obj03', 4)]
        do_test(expected, False, shard_size=4, limit=1)
        expected = [(c_lower, 'obj03', 4), ('obj03', 'obj07', 4)]
        do_test(expected, False, shard_size=4, limit=2)
        expected = [(c_lower, 'obj03', 4), ('obj03', 'obj07', 4),
                    ('obj07', c_upper, 2)]
        do_test(expected, True, shard_size=4, limit=3)
        do_test(expected, True, shard_size=4, limit=4)
        do_test(expected, True, shard_size=4, limit=-1)

        # increase object count to 11
        broker.put_object(
            'obj10', next(ts_iter).internal, 0, 'text/plain', 'etag')
        expected = [(c_lower, 'obj03', 4), ('obj03', 'obj07', 4),
                    ('obj07', c_upper, 3)]
        do_test(expected, True, shard_size=4, limit=None)

        expected = [(c_lower, 'obj09', 10), ('obj09', c_upper, 1)]
        do_test(expected, True, shard_size=10, limit=None)
        do_test([], False, shard_size=11, limit=None)

        # now pass in a pre-existing shard range
        existing = [ShardRange(
            '.shards_a/srange-0', Timestamp.now(), '', 'obj03',
            object_count=4, state=ShardRange.FOUND)]

        expected = [('obj03', 'obj07', 4), ('obj07', c_upper, 3)]
        do_test(expected, True, shard_size=4, limit=None, start_index=1,
                existing=existing)
        expected = [('obj03', 'obj07', 4)]
        do_test(expected, False, shard_size=4, limit=1, start_index=1,
                existing=existing)
        # using increased shard size should not distort estimation of progress
        expected = [('obj03', 'obj09', 6), ('obj09', c_upper, 1)]
        do_test(expected, True, shard_size=6, limit=None, start_index=1,
                existing=existing)

        # add another existing...
        existing.append(ShardRange(
            '.shards_a/srange-1', Timestamp.now(), '', 'obj07',
            object_count=4, state=ShardRange.FOUND))
        expected = [('obj07', c_upper, 3)]
        do_test(expected, True, shard_size=10, limit=None, start_index=2,
                existing=existing)
        # an existing shard range not in FOUND state should not distort
        # estimation of progress, but may cause final range object count to
        # default to shard_size
        existing[-1].state = ShardRange.CREATED
        existing[-1].object_count = 10
        # there's only 3 objects left to scan but progress cannot be reliably
        # calculated, so final shard range has object count of 2
        expected = [('obj07', 'obj09', 2), ('obj09', c_upper, 2)]
        do_test(expected, True, shard_size=2, limit=None, start_index=2,
                existing=existing)

        # add last shard range so there's none left to find
        existing.append(ShardRange(
            '.shards_a/srange-2', Timestamp.now(), 'obj07', c_upper,
            object_count=4, state=ShardRange.FOUND))
        do_test([], True, shard_size=4, limit=None, existing=existing)

    def test_find_shard_ranges(self):
        self._check_find_shard_ranges('', '')
        self._check_find_shard_ranges('', 'upper')
        self._check_find_shard_ranges('lower', '')
        self._check_find_shard_ranges('lower', 'upper')

    @with_tempdir
    def test_find_shard_ranges_with_misplaced_objects(self, tempdir):
        # verify that misplaced objects outside of a shard's range do not
        # influence choice of shard ranges (but do distort the object counts)
        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()
        container_name = 'test_container'

        db_path = os.path.join(tempdir, 'test_container.db')
        broker = ContainerBroker(
            db_path, account='a', container=container_name)
        # shard size > object count, no objects
        broker.initialize(next(ts_iter).internal, 0)

        ts = next(ts_iter)
        own_shard_range = ShardRange(broker.path, ts, 'l', 'u')
        broker.merge_shard_ranges([own_shard_range])

        self.assertEqual(([], False), broker.find_shard_ranges(10))

        for name in ('a-misplaced', 'm', 'n', 'p', 'q', 'r', 'z-misplaced'):
            broker.put_object(
                name, next(ts_iter).internal, 0, 'text/plain', 'etag')

        expected_bounds = (
            ('l', 'n', 2),  # contains m, n
            ('n', 'q', 2),  # contains p, q
            ('q', 'u', 3)   # contains r; object count distorted by 2 misplaced
        )
        expected_shard_ranges = [
            dict(lower=lower, upper=upper, index=index,
                 object_count=object_count)
            for index, (lower, upper, object_count)
            in enumerate(expected_bounds)]

        with mock.patch('swift.common.utils.time.time',
                        return_value=float(ts_now.normal)):
            actual_shard_ranges, last_found = broker.find_shard_ranges(2, -1)
        self.assertEqual(expected_shard_ranges, actual_shard_ranges)

        ts_iter = make_timestamp_iter()
        ts_now = Timestamp.now()
        container_name = 'test_container'

    @with_tempdir
    def test_find_shard_ranges_errors(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(tempdir, 'test_container.db')
        broker = ContainerBroker(db_path, account='a', container='c',
                                 logger=FakeLogger())
        broker.initialize(next(ts_iter).internal, 0)
        for i in range(2):
            broker.put_object(
                'obj%d' % i, next(ts_iter).internal, 0, 'text/plain', 'etag')

        klass = 'swift.container.backend.ContainerBroker'
        with mock.patch(klass + '._get_next_shard_range_upper',
                        side_effect=LockTimeout()):
            ranges, last_found = broker.find_shard_ranges(1)
        self.assertFalse(ranges)
        self.assertFalse(last_found)
        lines = broker.logger.get_lines_for_level('error')
        self.assertIn('Problem finding shard upper', lines[0])
        self.assertFalse(lines[1:])

        broker.logger.clear()
        with mock.patch(klass + '._get_next_shard_range_upper',
                        side_effect=sqlite3.OperationalError()):
            ranges, last_found = broker.find_shard_ranges(1)
        self.assertFalse(last_found)
        self.assertFalse(ranges)
        lines = broker.logger.get_lines_for_level('error')
        self.assertIn('Problem finding shard upper', lines[0])
        self.assertFalse(lines[1:])

    @with_tempdir
    def test_set_db_states(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)

        # load up the broker with some objects
        objects = [{'name': 'obj_%d' % i,
                    'created_at': next(ts_iter).normal,
                    'content_type': 'text/plain',
                    'etag': 'etag_%d' % i,
                    'size': 1024 * i,
                    'deleted': 0,
                    'storage_policy_index': 0,
                    } for i in range(1, 6)]
        # merge_items mutates items
        broker.merge_items([dict(obj) for obj in objects])
        original_info = broker.get_info()

        # Add some metadata
        meta = {
            'X-Container-Meta-Color': ['Blue', next(ts_iter).normal],
            'X-Container-Meta-Cleared': ['', next(ts_iter).normal],
            'X-Container-Sysmeta-Shape': ['Circle', next(ts_iter).normal],
        }
        broker.update_metadata(meta)

        # Add some syncs
        incoming_sync = {'remote_id': 'incoming_123', 'sync_point': 1}
        outgoing_sync = {'remote_id': 'outgoing_123', 'sync_point': 2}
        broker.merge_syncs([outgoing_sync], incoming=False)
        broker.merge_syncs([incoming_sync], incoming=True)

        # Add some ShardRanges
        shard_ranges = [ShardRange(
            name='.shards_a/shard_range_%s' % i,
            timestamp=next(ts_iter), lower='obj_%d' % i,
            upper='obj_%d' % (i + 2),
            object_count=len(objects[i:i + 2]),
            bytes_used=sum(obj['size'] for obj in objects[i:i + 2]),
            meta_timestamp=next(ts_iter)) for i in range(0, 6, 2)]
        deleted_range = ShardRange('.shards_a/shard_range_z', next(ts_iter),
                                   'z', '', state=ShardRange.SHARDED,
                                   deleted=1)
        own_sr = ShardRange(name='a/c', timestamp=next(ts_iter),
                            state=ShardRange.ACTIVE)
        broker.merge_shard_ranges([own_sr] + shard_ranges + [deleted_range])
        ts_epoch = next(ts_iter)
        new_db_path = os.path.join(tempdir, 'containers', 'part', 'suffix',
                                   'hash', 'container_%s.db' % ts_epoch.normal)

        def check_broker_properties(broker):
            # these broker properties should remain unchanged as state changes
            self.assertEqual(broker.get_max_row(), 5)
            all_metadata = broker.metadata
            original_meta = dict((k, all_metadata[k]) for k in meta)
            self.assertEqual(original_meta, meta)
            self.assertEqual(broker.get_syncs(True)[0], incoming_sync)
            self.assertEqual(broker.get_syncs(False)[0], outgoing_sync)
            self.assertEqual(shard_ranges + [own_sr, deleted_range],
                             broker.get_shard_ranges(include_own=True,
                                                     include_deleted=True))

        def check_broker_info(actual_info):
            for key in ('db_state', 'id', 'hash'):
                actual_info.pop(key, None)
                original_info.pop(key, None)
            self.assertEqual(original_info, actual_info)

        def check_unsharded_state(broker):
            # these are expected properties in unsharded state
            self.assertEqual(len(broker.get_brokers()), 1)
            self.assertEqual(broker.get_db_state(), UNSHARDED)
            self.assertTrue(os.path.exists(db_path))
            self.assertFalse(os.path.exists(new_db_path))
            self.assertEqual(objects, broker.get_objects())

        # Sanity checks
        check_broker_properties(broker)
        check_unsharded_state(broker)
        check_broker_info(broker.get_info())

        # first test that moving from UNSHARDED to SHARDED doesn't work
        self.assertFalse(broker.set_sharded_state())
        # check nothing changed
        check_broker_properties(broker)
        check_broker_info(broker.get_info())
        check_unsharded_state(broker)

        # cannot go to SHARDING without an epoch set
        self.assertFalse(broker.set_sharding_state())

        # now set sharding epoch and make sure everything moves.
        broker.enable_sharding(ts_epoch)
        self.assertTrue(broker.set_sharding_state())
        check_broker_properties(broker)
        check_broker_info(broker.get_info())

        def check_sharding_state(broker):
            self.assertEqual(len(broker.get_brokers()), 2)
            self.assertEqual(broker.get_db_state(), SHARDING)
            self.assertTrue(os.path.exists(db_path))
            self.assertTrue(os.path.exists(new_db_path))
            self.assertEqual([], broker.get_objects())
            self.assertEqual(objects, broker.get_brokers()[0].get_objects())
        check_sharding_state(broker)

        # to confirm we're definitely looking at the shard db
        broker2 = ContainerBroker(new_db_path)
        check_broker_properties(broker2)
        check_broker_info(broker2.get_info())
        self.assertEqual([], broker2.get_objects())

        # Try to set sharding state again
        self.assertFalse(broker.set_sharding_state())
        # check nothing changed
        check_broker_properties(broker)
        check_broker_info(broker.get_info())
        check_sharding_state(broker)

        # Now move to the final state - update shard ranges' state
        broker.merge_shard_ranges(
            [dict(sr, state=ShardRange.ACTIVE,
                  state_timestamp=next(ts_iter).internal)
             for sr in shard_ranges])
        # pretend all ranges have been cleaved
        self.assertTrue(broker.set_sharded_state())
        check_broker_properties(broker)
        check_broker_info(broker.get_info())

        def check_sharded_state(broker):
            self.assertEqual(broker.get_db_state(), SHARDED)
            self.assertEqual(len(broker.get_brokers()), 1)
            self.assertFalse(os.path.exists(db_path))
            self.assertTrue(os.path.exists(new_db_path))
            self.assertEqual([], broker.get_objects())
        check_sharded_state(broker)

        # Try to set sharded state again
        self.assertFalse(broker.set_sharded_state())
        # check nothing changed
        check_broker_properties(broker)
        check_broker_info(broker.get_info())
        check_sharded_state(broker)

        # delete the container - sharding sysmeta gets erased
        broker.delete_db(next(ts_iter).internal)
        # but it is not considered deleted while shards have content
        self.assertFalse(broker.is_deleted())
        check_sharded_state(broker)
        # empty the shard ranges
        empty_shard_ranges = [sr.copy(object_count=0, bytes_used=0,
                                      meta_timestamp=next(ts_iter))
                              for sr in shard_ranges]
        broker.merge_shard_ranges(empty_shard_ranges)
        # and no it is deleted
        self.assertTrue(broker.is_deleted())
        check_sharded_state(broker)

        def do_revive_shard_delete(shard_ranges):
            # delete all shard ranges
            deleted_shard_ranges = [sr.copy(timestamp=next(ts_iter), deleted=1)
                                    for sr in shard_ranges]
            broker.merge_shard_ranges(deleted_shard_ranges)
            self.assertEqual(COLLAPSED, broker.get_db_state())

            # add new shard ranges and go to sharding state - need to force
            # broker time to be after the delete time in order to write new
            # sysmeta
            broker.enable_sharding(next(ts_iter))
            shard_ranges = [sr.copy(timestamp=next(ts_iter))
                            for sr in shard_ranges]
            broker.merge_shard_ranges(shard_ranges)
            with mock.patch('swift.common.db.time.time',
                            lambda: float(next(ts_iter))):
                self.assertTrue(broker.set_sharding_state())
            self.assertEqual(SHARDING, broker.get_db_state())

            # go to sharded
            self.assertTrue(
                broker.set_sharded_state())
            self.assertEqual(SHARDED, broker.get_db_state())

            # delete again
            broker.delete_db(next(ts_iter).internal)
            self.assertTrue(broker.is_deleted())
            self.assertEqual(SHARDED, broker.get_db_state())

        do_revive_shard_delete(shard_ranges)
        do_revive_shard_delete(shard_ranges)

    @with_tempdir
    def test_set_sharding_state_errors(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c',
                                 logger=FakeLogger())
        broker.initialize(next(ts_iter).internal, 0)
        broker.enable_sharding(next(ts_iter))

        orig_execute = GreenDBConnection.execute
        trigger = 'INSERT into object'

        def mock_execute(conn, *args, **kwargs):
            if trigger in args[0]:
                raise sqlite3.OperationalError()
            return orig_execute(conn, *args, **kwargs)

        with mock.patch('swift.common.db.GreenDBConnection.execute',
                        mock_execute):
            res = broker.set_sharding_state()
        self.assertFalse(res)
        lines = broker.logger.get_lines_for_level('error')
        self.assertIn('Failed to set the ROWID', lines[0])
        self.assertFalse(lines[1:])

        broker.logger.clear()
        trigger = 'UPDATE container_stat SET created_at'
        with mock.patch('swift.common.db.GreenDBConnection.execute',
                        mock_execute):
            res = broker.set_sharding_state()
        self.assertFalse(res)
        lines = broker.logger.get_lines_for_level('error')
        self.assertIn('Failed to set matching', lines[0])
        self.assertFalse(lines[1:])

    @with_tempdir
    def test_set_sharded_state_errors(self, tempdir):
        ts_iter = make_timestamp_iter()
        retiring_db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(retiring_db_path, account='a', container='c',
                                 logger=FakeLogger())
        broker.initialize(next(ts_iter).internal, 0)
        pre_epoch = next(ts_iter)
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        # unlink fails
        with mock.patch('os.unlink', side_effect=OSError(errno.EPERM)):
            self.assertFalse(broker.set_sharded_state())
        lines = broker.logger.get_lines_for_level('error')
        self.assertIn('Failed to unlink', lines[0])
        self.assertFalse(lines[1:])
        self.assertFalse(broker.logger.get_lines_for_level('warning'))
        self.assertTrue(os.path.exists(retiring_db_path))
        self.assertTrue(os.path.exists(broker.db_file))

        # extra files
        extra_filename = make_db_file_path(broker.db_file, pre_epoch)
        self.assertNotEqual(extra_filename, broker.db_file)  # sanity check
        with open(extra_filename, 'wb'):
            pass
        broker.logger.clear()
        self.assertFalse(broker.set_sharded_state())
        lines = broker.logger.get_lines_for_level('warning')
        self.assertIn('Still have multiple db files', lines[0])
        self.assertFalse(lines[1:])
        self.assertFalse(broker.logger.get_lines_for_level('error'))
        self.assertTrue(os.path.exists(retiring_db_path))
        self.assertTrue(os.path.exists(broker.db_file))

        # retiring file missing
        broker.logger.clear()
        os.unlink(retiring_db_path)
        self.assertFalse(broker.set_sharded_state())
        lines = broker.logger.get_lines_for_level('warning')
        self.assertIn('Refusing to delete', lines[0])
        self.assertFalse(lines[1:])
        self.assertFalse(broker.logger.get_lines_for_level('error'))
        self.assertTrue(os.path.exists(broker.db_file))

    @with_tempdir
    def test_get_brokers(self, tempdir):
        ts_iter = make_timestamp_iter()
        retiring_db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(retiring_db_path, account='a', container='c',
                                 logger=FakeLogger())
        broker.initialize(next(ts_iter).internal, 0)
        brokers = broker.get_brokers()
        self.assertEqual(retiring_db_path, brokers[0].db_file)
        self.assertFalse(brokers[0].skip_commits)
        self.assertFalse(brokers[1:])

        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        brokers = broker.get_brokers()
        self.assertEqual(retiring_db_path, brokers[0].db_file)
        self.assertTrue(brokers[0].skip_commits)
        self.assertEqual(broker.db_file, brokers[1].db_file)
        self.assertFalse(brokers[1].skip_commits)
        self.assertFalse(brokers[2:])

        # same outcome when called on retiring db broker
        brokers = brokers[0].get_brokers()
        self.assertEqual(retiring_db_path, brokers[0].db_file)
        self.assertTrue(brokers[0].skip_commits)
        self.assertEqual(broker.db_file, brokers[1].db_file)
        self.assertFalse(brokers[1].skip_commits)
        self.assertFalse(brokers[2:])

        self.assertTrue(broker.set_sharded_state())
        brokers = broker.get_brokers()
        self.assertEqual(broker.db_file, brokers[0].db_file)
        self.assertFalse(brokers[0].skip_commits)
        self.assertFalse(brokers[1:])

        # unexpected extra file should be ignored
        with open(retiring_db_path, 'wb'):
            pass
        retiring_db_path = broker.db_file
        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        broker.reload_db_files()
        self.assertEqual(3, len(broker.db_files))  # sanity check
        brokers = broker.get_brokers()
        self.assertEqual(retiring_db_path, brokers[0].db_file)
        self.assertTrue(brokers[0].skip_commits)
        self.assertEqual(broker.db_file, brokers[1].db_file)
        self.assertFalse(brokers[1].skip_commits)
        self.assertFalse(brokers[2:])
        lines = broker.logger.get_lines_for_level('warning')
        self.assertIn('Unexpected db files', lines[0])
        self.assertFalse(lines[1:])

    @with_tempdir
    def test_merge_shard_ranges(self, tempdir):
        ts_iter = make_timestamp_iter()
        ts = [next(ts_iter) for _ in range(13)]
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(
            db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)

        # sanity check
        self.assertFalse(broker.get_shard_ranges(include_deleted=True))

        broker.merge_shard_ranges(None)
        self.assertFalse(broker.get_shard_ranges(include_deleted=True))

        # merge item at ts1
        # sr_<upper>_<created ts>_<meta ts>
        sr_b_1_1 = ShardRange('a/c_b', ts[1], lower='a', upper='b',
                              object_count=2)
        broker.merge_shard_ranges([sr_b_1_1])
        self._assert_shard_ranges(broker, [sr_b_1_1])

        # merge older item - ignored
        sr_b_0_0 = ShardRange('a/c_b', ts[0], lower='a', upper='b',
                              object_count=1)
        broker.merge_shard_ranges([sr_b_0_0])
        self._assert_shard_ranges(broker, [sr_b_1_1])

        # merge same timestamp - ignored
        broker.merge_shard_ranges([dict(sr_b_1_1, lower='', upper='c')])
        self._assert_shard_ranges(broker, [sr_b_1_1])
        broker.merge_shard_ranges([dict(sr_b_1_1, object_count=99)])
        self._assert_shard_ranges(broker, [sr_b_1_1])

        # merge list with older item *after* newer item
        sr_c_2_2 = ShardRange('a/c_c', ts[2], lower='b', upper='c',
                              object_count=3)
        sr_c_3_3 = ShardRange('a/c_c', ts[3], lower='b', upper='c',
                              object_count=4)
        broker.merge_shard_ranges([sr_c_3_3, sr_c_2_2])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_3_3])

        # merge newer item - updated
        sr_c_5_5 = ShardRange('a/c_c', ts[5], lower='b', upper='c',
                              object_count=5)
        broker.merge_shard_ranges([sr_c_5_5])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_5])

        # merge older metadata item - ignored
        sr_c_5_4 = ShardRange('a/c_c', ts[5], lower='b', upper='c',
                              object_count=6, meta_timestamp=ts[4])
        broker.merge_shard_ranges([sr_c_5_4])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_5])

        # merge newer metadata item - only metadata is updated
        sr_c_5_6 = ShardRange('a/c_c', ts[5], lower='b', upper='c',
                              object_count=7, meta_timestamp=ts[6])
        broker.merge_shard_ranges([dict(sr_c_5_6, lower='', upper='d')])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_6])

        # merge older created_at, newer metadata item - ignored
        sr_c_4_7 = ShardRange('a/c_c', ts[4], lower='b', upper='c',
                              object_count=8, meta_timestamp=ts[7])
        broker.merge_shard_ranges([sr_c_4_7])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_6])

        # merge list with older metadata item *after* newer metadata item
        sr_c_5_11 = ShardRange('a/c_c', ts[5], lower='b', upper='c',
                               object_count=9, meta_timestamp=ts[11])
        broker.merge_shard_ranges([sr_c_5_11, sr_c_5_6])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_11])

        # deleted item at *same timestamp* as existing - deleted ignored
        broker.merge_shard_ranges([dict(sr_b_1_1, deleted=1, object_count=0)])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_11])
        sr_b_1_1.meta_timestamp = ts[11]
        broker.merge_shard_ranges([dict(sr_b_1_1, deleted=1)])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_11])
        sr_b_1_1.state_timestamp = ts[11]
        broker.merge_shard_ranges([dict(sr_b_1_1, deleted=1)])
        self._assert_shard_ranges(broker, [sr_b_1_1, sr_c_5_11])

        # delete item at *newer timestamp* - updated
        sr_b_2_2_deleted = ShardRange('a/c_b', ts[2], lower='a', upper='b',
                                      object_count=0, deleted=1)
        broker.merge_shard_ranges([sr_b_2_2_deleted])
        self._assert_shard_ranges(broker, [sr_b_2_2_deleted, sr_c_5_11])

        # merge list with older undeleted item *after* newer deleted item
        # NB deleted timestamp trumps newer meta timestamp
        sr_c_9_12 = ShardRange('a/c_c', ts[9], lower='b', upper='c',
                               object_count=10, meta_timestamp=ts[12])
        sr_c_10_10_deleted = ShardRange('a/c_c', ts[10], lower='b', upper='c',
                                        object_count=0, deleted=1)
        broker.merge_shard_ranges([sr_c_10_10_deleted, sr_c_9_12])
        self._assert_shard_ranges(
            broker, [sr_b_2_2_deleted, sr_c_10_10_deleted])

    @with_tempdir
    def test_merge_shard_ranges_state(self, tempdir):
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(db_path, account='a', container='c')
        broker.initialize(next(ts_iter).internal, 0)
        expected_shard_ranges = []

        def do_test(orig_state, orig_timestamp, test_state, test_timestamp,
                    expected_state, expected_timestamp):
            index = len(expected_shard_ranges)
            sr = ShardRange('a/%s' % index, orig_timestamp, '%03d' % index,
                            '%03d' % (index + 1), state=orig_state)
            broker.merge_shard_ranges([sr])
            sr.state = test_state
            sr.state_timestamp = test_timestamp
            broker.merge_shard_ranges([sr])
            sr.state = expected_state
            sr.state_timestamp = expected_timestamp
            expected_shard_ranges.append(sr)
            self._assert_shard_ranges(broker, expected_shard_ranges)

        # state at older state_timestamp is not merged
        for orig_state in ShardRange.STATES:
            for test_state in ShardRange.STATES:
                ts_older = next(ts_iter)
                ts = next(ts_iter)
                do_test(orig_state, ts, test_state, ts_older, orig_state, ts)

        # more advanced state at same timestamp is merged
        for orig_state in ShardRange.STATES:
            for test_state in ShardRange.STATES:
                ts = next(ts_iter)
                do_test(orig_state, ts, test_state, ts,
                        test_state if test_state > orig_state else orig_state,
                        ts)

        # any state at newer timestamp is merged
        for orig_state in ShardRange.STATES:
            for test_state in ShardRange.STATES:
                ts = next(ts_iter)
                ts_newer = next(ts_iter)
                do_test(orig_state, ts, test_state, ts_newer, test_state,
                        ts_newer)

    def _check_object_stats_when_sharded(self, a, c, root_a, root_c, tempdir):
        # common setup and assertions for root and shard containers
        ts_iter = make_timestamp_iter()
        db_path = os.path.join(
            tempdir, 'containers', 'part', 'suffix', 'hash', 'container.db')
        broker = ContainerBroker(
            db_path, account=a, container=c)
        broker.initialize(next(ts_iter).internal, 0)
        broker.set_sharding_sysmeta('Root', '%s/%s' % (root_a, root_c))
        broker.merge_items([{'name': 'obj', 'size': 14, 'etag': 'blah',
                             'content_type': 'text/plain', 'deleted': 0,
                             'created_at': Timestamp.now().internal}])
        self.assertEqual(1, broker.get_info()['object_count'])
        self.assertEqual(14, broker.get_info()['bytes_used'])

        broker.enable_sharding(next(ts_iter))
        self.assertTrue(broker.set_sharding_state())
        sr_1 = ShardRange(
            '%s/%s1' % (root_a, root_c), Timestamp.now(), lower='', upper='m',
            object_count=99, bytes_used=999, state=ShardRange.ACTIVE)
        sr_2 = ShardRange(
            '%s/%s2' % (root_a, root_c), Timestamp.now(), lower='m', upper='',
            object_count=21, bytes_used=1000, state=ShardRange.ACTIVE)
        broker.merge_shard_ranges([sr_1, sr_2])
        self.assertEqual(1, broker.get_info()['object_count'])
        self.assertEqual(14, broker.get_info()['bytes_used'])
        return broker

    @with_tempdir
    def test_object_stats_root_container(self, tempdir):
        broker = self._check_object_stats_when_sharded(
            'a', 'c', 'a', 'c', tempdir)
        self.assertTrue(broker.is_root_container())  # sanity
        self.assertTrue(broker.set_sharded_state())
        self.assertEqual(120, broker.get_info()['object_count'])
        self.assertEqual(1999, broker.get_info()['bytes_used'])

    @with_tempdir
    def test_object_stats_shard_container(self, tempdir):
        broker = self._check_object_stats_when_sharded(
            '.shard_a', 'c-blah', 'a', 'c', tempdir)
        self.assertFalse(broker.is_root_container())  # sanity
        self.assertTrue(broker.set_sharded_state())
        self.assertEqual(0, broker.get_info()['object_count'])
        self.assertEqual(0, broker.get_info()['bytes_used'])


class TestCommonContainerBroker(test_db.TestExampleBroker):

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
    class OverrideCreateShardRangesTable(object):
        def __init__(self, func):
            self.func = func

        def __get__(self, obj, obj_type):
            if inspect.stack()[1][3] == '_initialize':
                return lambda *a, **kw: None
            return self.func.__get__(obj, obj_type)

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

        self._imported_create_shard_range_table = \
            ContainerBroker.create_shard_range_table
        if 'shard_range' not in self.expected_db_tables:
            ContainerBroker.create_shard_range_table = \
                self.OverrideCreateShardRangesTable(
                    ContainerBroker.create_shard_range_table)

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
        ContainerBroker.create_shard_range_table = \
            self._imported_create_shard_range_table
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
    ''', (self.account, self.container, Timestamp.now().internal,
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeMetadata(ContainerBrokerMigrationMixin,
                                        TestContainerBroker):
    """
    Tests for ContainerBroker against databases created before
    the metadata column was added.
    """
    expected_db_tables = {'outgoing_sync', 'incoming_sync', 'object',
                          'sqlite_sequence', 'container_stat'}

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
        self.assertTrue('no such column: metadata' in str(exc))

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
    ''', (self.account, self.container, Timestamp.now().internal,
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeXSync(ContainerBrokerMigrationMixin,
                                     TestContainerBroker):
    """
    Tests for ContainerBroker against databases created
    before the x_container_sync_point[12] columns were added.
    """
    expected_db_tables = {'outgoing_sync', 'incoming_sync', 'object',
                          'sqlite_sequence', 'container_stat'}

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
        self.assertTrue('no such column: x_container_sync_point1' in str(exc))

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
    ''', (self.account, self.container, Timestamp.now().internal,
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeSPI(ContainerBrokerMigrationMixin,
                                   TestContainerBroker):
    """
    Tests for ContainerBroker against databases created
    before the storage_policy_index column was added.
    """
    expected_db_tables = {'outgoing_sync', 'incoming_sync', 'object',
                          'sqlite_sequence', 'container_stat'}

    def setUp(self):
        super(TestContainerBrokerBeforeSPI, self).setUp()
        ContainerBroker.create_container_info_table = \
            prespi_create_container_info_table

        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        with self.assertRaises(sqlite3.DatabaseError) as raised, \
                broker.get() as conn:
            conn.execute('''SELECT storage_policy_index
                            FROM container_stat''')
        self.assertIn('no such column: storage_policy_index',
                      str(raised.exception))

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
                self.assertTrue('no such column: storage_policy_index' in
                                str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from storage_policy_index '
                          'from object table!')

        # manually insert an existing row to avoid automatic migration
        obj_put_timestamp = Timestamp.now().internal
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
        self.assertTrue(
            Timestamp(info['created_at']) > Timestamp(put_timestamp))
        self.assertNotEqual(int(info['hash'], 16), 0)
        orig_hash = info['hash']
        # get_replication_info
        info = broker.get_replication_info()
        # translate object count for replicators
        expected['count'] = expected.pop('object_count')
        for k, v in expected.items():
            self.assertEqual(info[k], v)
        self.assertTrue(
            Timestamp(info['created_at']) > Timestamp(put_timestamp))
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
                self.assertTrue('no such column: storage_policy_index' in
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
                self.assertTrue('no such column: storage_policy_index' in
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
                self.assertTrue('no such table: policy_stat' in str(err))
            else:
                self.fail('broker did not raise sqlite3.OperationalError '
                          'trying to select from storage_policy_index '
                          'from policy_stat table!')

        # now do a PUT with a different value for storage_policy_index
        # which will update the DB schema as well as update policy_stats
        # for legacy objects in the DB (those without an SPI)
        second_object_put_timestamp = Timestamp.now().internal
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


class TestContainerBrokerBeforeShardRanges(ContainerBrokerMigrationMixin,
                                           TestContainerBroker):
    """
    Tests for ContainerBroker against databases created
    before the shard_ranges table was added.
    """
    # *grumble grumble* This should include container_info/policy_stat :-/
    expected_db_tables = {'outgoing_sync', 'incoming_sync', 'object',
                          'sqlite_sequence', 'container_stat'}

    def setUp(self):
        super(TestContainerBrokerBeforeShardRanges, self).setUp()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        with self.assertRaises(sqlite3.DatabaseError) as raised, \
                broker.get() as conn:
            conn.execute('''SELECT *
                            FROM shard_range''')
        self.assertIn('no such table: shard_range', str(raised.exception))

    def tearDown(self):
        super(TestContainerBrokerBeforeShardRanges, self).tearDown()
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(Timestamp('1').internal, 0)
        with broker.get() as conn:
            conn.execute('''SELECT *
                            FROM shard_range''')


class TestUpdateNewItemFromExisting(unittest.TestCase):
    # TODO: add test scenarios that have swift_bytes in content_type
    t0 = '1234567890.00000'
    t1 = '1234567890.00001'
    t2 = '1234567890.00002'
    t3 = '1234567890.00003'
    t4 = '1234567890.00004'
    t5 = '1234567890.00005'
    t6 = '1234567890.00006'
    t7 = '1234567890.00007'
    t8 = '1234567890.00008'
    t20 = '1234567890.00020'
    t30 = '1234567890.00030'

    base_new_item = {'etag': 'New_item',
                     'size': 'nEw_item',
                     'content_type': 'neW_item',
                     'deleted': '0'}
    base_existing = {'etag': 'Existing',
                     'size': 'eXisting',
                     'content_type': 'exIsting',
                     'deleted': '0'}
    #
    # each scenario is a tuple of:
    #    (existing time, new item times, expected updated item)
    #
    #  e.g.:
    # existing -> ({'created_at': t5},
    # new_item -> {'created_at': t, 'ctype_timestamp': t, 'meta_timestamp': t},
    # expected -> {'created_at': t,
    #              'etag': <val>, 'size': <val>, 'content_type': <val>})
    #
    scenarios_when_all_existing_wins = (
        #
        # all new_item times <= all existing times -> existing values win
        #
        # existing has attrs at single time
        #
        ({'created_at': t3},
         {'created_at': t0, 'ctype_timestamp': t0, 'meta_timestamp': t0},
         {'created_at': t3,
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3},
         {'created_at': t0, 'ctype_timestamp': t0, 'meta_timestamp': t1},
         {'created_at': t3,
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3},
         {'created_at': t0, 'ctype_timestamp': t1, 'meta_timestamp': t1},
         {'created_at': t3,
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3},
         {'created_at': t0, 'ctype_timestamp': t1, 'meta_timestamp': t2},
         {'created_at': t3,
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3},
         {'created_at': t0, 'ctype_timestamp': t1, 'meta_timestamp': t3},
         {'created_at': t3,
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3},
         {'created_at': t0, 'ctype_timestamp': t3, 'meta_timestamp': t3},
         {'created_at': t3,
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3},
         {'created_at': t3, 'ctype_timestamp': t3, 'meta_timestamp': t3},
         {'created_at': t3,
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        #
        # existing has attrs at multiple times:
        # data @ t3, ctype @ t5, meta @t7 -> existing created_at = t3+2+2
        #
        ({'created_at': t3 + '+2+2'},
         {'created_at': t0, 'ctype_timestamp': t0, 'meta_timestamp': t0},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t3, 'meta_timestamp': t3},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t4, 'meta_timestamp': t4},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t4, 'meta_timestamp': t5},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t4, 'meta_timestamp': t7},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t4, 'meta_timestamp': t7},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t5, 'meta_timestamp': t5},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t5, 'meta_timestamp': t6},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t5, 'meta_timestamp': t7},
         {'created_at': t3 + '+2+2',
         'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),
    )

    scenarios_when_all_new_item_wins = (
        # no existing record
        (None,
         {'created_at': t4, 'ctype_timestamp': t4, 'meta_timestamp': t4},
         {'created_at': t4,
          'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        (None,
         {'created_at': t4, 'ctype_timestamp': t4, 'meta_timestamp': t5},
         {'created_at': t4 + '+0+1',
          'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        (None,
         {'created_at': t4, 'ctype_timestamp': t5, 'meta_timestamp': t5},
         {'created_at': t4 + '+1+0',
          'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        (None,
         {'created_at': t4, 'ctype_timestamp': t5, 'meta_timestamp': t6},
         {'created_at': t4 + '+1+1',
          'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        #
        # all new_item times > all existing times -> new item values win
        #
        # existing has attrs at single time
        #
        ({'created_at': t3},
         {'created_at': t4, 'ctype_timestamp': t4, 'meta_timestamp': t4},
         {'created_at': t4,
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3},
         {'created_at': t4, 'ctype_timestamp': t4, 'meta_timestamp': t5},
         {'created_at': t4 + '+0+1',
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3},
         {'created_at': t4, 'ctype_timestamp': t5, 'meta_timestamp': t5},
         {'created_at': t4 + '+1+0',
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3},
         {'created_at': t4, 'ctype_timestamp': t5, 'meta_timestamp': t6},
         {'created_at': t4 + '+1+1',
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        #
        # existing has attrs at multiple times:
        # data @ t3, ctype @ t5, meta @t7 -> existing created_at = t3+2+2
        #
        ({'created_at': t3 + '+2+2'},
         {'created_at': t4, 'ctype_timestamp': t6, 'meta_timestamp': t8},
         {'created_at': t4 + '+2+2',
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t6, 'ctype_timestamp': t6, 'meta_timestamp': t8},
         {'created_at': t6 + '+0+2',
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t4, 'ctype_timestamp': t8, 'meta_timestamp': t8},
         {'created_at': t4 + '+4+0',
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t6, 'ctype_timestamp': t8, 'meta_timestamp': t8},
         {'created_at': t6 + '+2+0',
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t8, 'ctype_timestamp': t8, 'meta_timestamp': t8},
         {'created_at': t8,
         'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),
    )

    scenarios_when_some_new_item_wins = (
        #
        # some but not all new_item times > existing times -> mixed updates
        #
        # existing has attrs at single time
        #
        ({'created_at': t3},
         {'created_at': t3, 'ctype_timestamp': t3, 'meta_timestamp': t4},
         {'created_at': t3 + '+0+1',
          'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3},
         {'created_at': t3, 'ctype_timestamp': t4, 'meta_timestamp': t4},
         {'created_at': t3 + '+1+0',
          'etag': 'Existing', 'size': 'eXisting', 'content_type': 'neW_item'}),

        ({'created_at': t3},
         {'created_at': t3, 'ctype_timestamp': t4, 'meta_timestamp': t5},
         {'created_at': t3 + '+1+1',
          'etag': 'Existing', 'size': 'eXisting', 'content_type': 'neW_item'}),

        #
        # existing has attrs at multiple times:
        # data @ t3, ctype @ t5, meta @t7 -> existing created_at = t3+2+2
        #
        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t3, 'meta_timestamp': t8},
         {'created_at': t3 + '+2+3',
          'etag': 'Existing', 'size': 'eXisting', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t3, 'ctype_timestamp': t6, 'meta_timestamp': t8},
         {'created_at': t3 + '+3+2',
          'etag': 'Existing', 'size': 'eXisting', 'content_type': 'neW_item'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t4, 'ctype_timestamp': t4, 'meta_timestamp': t6},
         {'created_at': t4 + '+1+2',
          'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'exIsting'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t4, 'ctype_timestamp': t6, 'meta_timestamp': t6},
         {'created_at': t4 + '+2+1',
          'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'neW_item'}),

        ({'created_at': t3 + '+2+2'},
         {'created_at': t4, 'ctype_timestamp': t4, 'meta_timestamp': t8},
         {'created_at': t4 + '+1+3',
          'etag': 'New_item', 'size': 'nEw_item', 'content_type': 'exIsting'}),

        # this scenario is to check that the deltas are in hex
        ({'created_at': t3 + '+2+2'},
         {'created_at': t2, 'ctype_timestamp': t20, 'meta_timestamp': t30},
         {'created_at': t3 + '+11+a',
          'etag': 'Existing', 'size': 'eXisting', 'content_type': 'neW_item'}),
    )

    def _test_scenario(self, scenario, newer):
        existing_time, new_item_times, expected_attrs = scenario
        # this is the existing record...
        existing = None
        if existing_time:
            existing = dict(self.base_existing)
            existing.update(existing_time)

        # this is the new item to update
        new_item = dict(self.base_new_item)
        new_item.update(new_item_times)

        # this is the expected result of the update
        expected = dict(new_item)
        expected.update(expected_attrs)
        expected['data_timestamp'] = new_item['created_at']

        try:
            self.assertIs(newer,
                          update_new_item_from_existing(new_item, existing))
            self.assertDictEqual(expected, new_item)
        except AssertionError as e:
            msg = ('Scenario: existing %s, new_item %s, expected %s.'
                   % scenario)
            msg = '%s Failed with: %s' % (msg, e.message)
            raise AssertionError(msg)

    def test_update_new_item_from_existing(self):
        for scenario in self.scenarios_when_all_existing_wins:
            self._test_scenario(scenario, False)

        for scenario in self.scenarios_when_all_new_item_wins:
            self._test_scenario(scenario, True)

        for scenario in self.scenarios_when_some_new_item_wins:
            self._test_scenario(scenario, True)
