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

"""Tests for swift.common.db"""
import contextlib
import os
import unittest
from tempfile import mkdtemp
from shutil import rmtree, copy
from uuid import uuid4
import pickle
from unittest import mock

import base64
import json
import sqlite3
import itertools
import time
import random
from unittest.mock import patch, MagicMock

from eventlet.timeout import Timeout

import swift.common.db
from swift.common.constraints import \
    MAX_META_VALUE_LENGTH, MAX_META_COUNT, MAX_META_OVERALL_SIZE
from swift.common.db import chexor, dict_factory, get_db_connection, \
    DatabaseBroker, DatabaseConnectionError, DatabaseAlreadyExists, \
    GreenDBConnection, PICKLE_PROTOCOL, zero_like, TombstoneReclaimer
from swift.common.utils import normalize_timestamp, mkdirs, Timestamp
from swift.common.exceptions import LockTimeout
from swift.common.swob import HTTPException

from test.unit import make_timestamp_iter, generate_db_path


class TestHelperFunctions(unittest.TestCase):

    def test_zero_like(self):
        expectations = {
            # value => expected
            None: True,
            True: False,
            '': True,
            'asdf': False,
            0: True,
            1: False,
            '0': True,
            '1': False,
        }
        errors = []
        for value, expected in expectations.items():
            rv = zero_like(value)
            if rv != expected:
                errors.append('zero_like(%r) => %r expected %r' % (
                    value, rv, expected))
        if errors:
            self.fail('Some unexpected return values:\n' + '\n'.join(errors))


class TestDatabaseConnectionError(unittest.TestCase):

    def test_str(self):
        err = \
            DatabaseConnectionError(':memory:', 'No valid database connection')
        self.assertIn(':memory:', str(err))
        self.assertIn('No valid database connection', str(err))
        err = DatabaseConnectionError(':memory:',
                                      'No valid database connection',
                                      timeout=1357)
        self.assertIn(':memory:', str(err))
        self.assertIn('No valid database connection', str(err))
        self.assertIn('1357', str(err))


class TestDictFactory(unittest.TestCase):

    def test_normal_case(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE test (one TEXT, two INTEGER)')
        conn.execute('INSERT INTO test (one, two) VALUES ("abc", 123)')
        conn.execute('INSERT INTO test (one, two) VALUES ("def", 456)')
        conn.commit()
        curs = conn.execute('SELECT one, two FROM test')
        self.assertEqual(dict_factory(curs, next(curs)),
                         {'one': 'abc', 'two': 123})
        self.assertEqual(dict_factory(curs, next(curs)),
                         {'one': 'def', 'two': 456})


class TestChexor(unittest.TestCase):

    def test_normal_case(self):
        self.assertEqual(
            chexor('d41d8cd98f00b204e9800998ecf8427e',
                   'new name', normalize_timestamp(1)),
            '4f2ea31ac14d4273fe32ba08062b21de')

    def test_invalid_old_hash(self):
        self.assertRaises(ValueError, chexor, 'oldhash', 'name',
                          normalize_timestamp(1))

    def test_no_name(self):
        self.assertRaises(Exception, chexor,
                          'd41d8cd98f00b204e9800998ecf8427e', None,
                          normalize_timestamp(1))

    def test_chexor(self):
        ts = (normalize_timestamp(ts) for ts in
              itertools.count(int(time.time())))

        objects = [
            ('frank', next(ts)),
            ('bob', next(ts)),
            ('tom', next(ts)),
            ('frank', next(ts)),
            ('tom', next(ts)),
            ('bob', next(ts)),
        ]
        hash_ = '0'
        random.shuffle(objects)
        for obj in objects:
            hash_ = chexor(hash_, *obj)

        other_hash = '0'
        random.shuffle(objects)
        for obj in objects:
            other_hash = chexor(other_hash, *obj)

        self.assertEqual(hash_, other_hash)


class TestGreenDBConnection(unittest.TestCase):

    def test_execute_when_locked(self):
        # This test is dependent on the code under test calling execute and
        # commit as sqlite3.Cursor.execute in a subclass.
        class InterceptCursor(sqlite3.Cursor):
            pass
        db_error = sqlite3.OperationalError('database is locked')
        InterceptCursor.execute = MagicMock(side_effect=db_error)
        with patch('sqlite3.Cursor', new=InterceptCursor):
            conn = sqlite3.connect(':memory:', check_same_thread=False,
                                   factory=GreenDBConnection, timeout=0.1)
            self.assertRaises(Timeout, conn.execute, 'select 1')
            self.assertTrue(InterceptCursor.execute.called)
            self.assertEqual(InterceptCursor.execute.call_args_list,
                             list((InterceptCursor.execute.call_args,) *
                                  InterceptCursor.execute.call_count))

    def text_commit_when_locked(self):
        # This test is dependent on the code under test calling commit and
        # commit as sqlite3.Connection.commit in a subclass.
        class InterceptConnection(sqlite3.Connection):
            pass
        db_error = sqlite3.OperationalError('database is locked')
        InterceptConnection.commit = MagicMock(side_effect=db_error)
        with patch('sqlite3.Connection', new=InterceptConnection):
            conn = sqlite3.connect(':memory:', check_same_thread=False,
                                   factory=GreenDBConnection, timeout=0.1)
            self.assertRaises(Timeout, conn.commit)
            self.assertTrue(InterceptConnection.commit.called)
            self.assertEqual(InterceptConnection.commit.call_args_list,
                             list((InterceptConnection.commit.call_args,) *
                                  InterceptConnection.commit.call_count))


class TestDbBase(unittest.TestCase):
    server_type = 'container'
    testdir = None

    def setUp(self):
        self.testdir = mkdtemp()
        self.db_path = self.get_db_path()

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=True)

    def get_db_path(self):
        return generate_db_path(self.testdir, self.server_type)


class TestGetDBConnection(TestDbBase):
    def setUp(self):
        super(TestGetDBConnection, self).setUp()
        self.db_path = self.init_db_path()

    def init_db_path(self):
        # Test ContainerBroker.empty
        db_path = self.get_db_path()
        broker = ExampleBroker(db_path, account='a')
        broker.initialize(Timestamp.now().internal, 0)
        return db_path

    def test_normal_case(self):
        conn = get_db_connection(self.db_path)
        self.assertTrue(hasattr(conn, 'execute'))

    def test_invalid_path(self):
        self.assertRaises(DatabaseConnectionError, get_db_connection,
                          'invalid database path / name')

    def test_locked_db(self):
        # This test is dependent on the code under test calling execute and
        # commit as sqlite3.Cursor.execute in a subclass.
        class InterceptCursor(sqlite3.Cursor):
            pass

        db_error = sqlite3.OperationalError('database is locked')
        mock_db_cmd = MagicMock(side_effect=db_error)
        InterceptCursor.execute = mock_db_cmd

        with patch('sqlite3.Cursor', new=InterceptCursor):
            self.assertRaises(Timeout, get_db_connection,
                              self.db_path, timeout=0.1)
            self.assertTrue(mock_db_cmd.called)
            self.assertEqual(mock_db_cmd.call_args_list,
                             list((mock_db_cmd.call_args,) *
                                  mock_db_cmd.call_count))


class ExampleBroker(DatabaseBroker):
    """
    Concrete enough implementation of a DatabaseBroker.
    """

    db_type = 'test'
    db_contains_type = 'test'
    db_reclaim_timestamp = 'created_at'

    def _initialize(self, conn, put_timestamp, **kwargs):
        if not self.account:
            raise ValueError(
                'Attempting to create a new database with no account set')
        conn.executescript('''
            CREATE TABLE test_stat (
                account TEXT,
                test_count INTEGER DEFAULT 0,
                created_at TEXT,
                put_timestamp TEXT DEFAULT '0',
                delete_timestamp TEXT DEFAULT '0',
                hash TEXT default '00000000000000000000000000000000',
                id TEXT,
                status TEXT DEFAULT '',
                status_changed_at TEXT DEFAULT '0',
                metadata TEXT DEFAULT ''
            );
            CREATE TABLE test (
                ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                created_at TEXT,
                deleted INTEGER DEFAULT 0
            );
            CREATE TRIGGER test_insert AFTER INSERT ON test
            BEGIN
                UPDATE test_stat
                SET test_count = test_count + (1 - new.deleted);
            END;
            CREATE TRIGGER test_delete AFTER DELETE ON test
            BEGIN
                UPDATE test_stat
                SET test_count = test_count - (1 - old.deleted);
            END;
        ''')
        conn.execute("""
        INSERT INTO test_stat (
            account, created_at, id, put_timestamp, status_changed_at, status)
        VALUES (?, ?, ?, ?, ?, ?);
        """, (self.account, Timestamp.now().internal, str(uuid4()),
              put_timestamp, put_timestamp, ''))

    def merge_items(self, item_list):
        with self.get() as conn:
            for rec in item_list:
                conn.execute(
                    'DELETE FROM test WHERE name = ? and created_at < ?', (
                        rec['name'], rec['created_at']))
                if not conn.execute(
                        'SELECT 1 FROM test WHERE name = ?',
                        (rec['name'],)).fetchall():
                    conn.execute('''
                    INSERT INTO test (name, created_at, deleted)
                    VALUES (?, ?, ?)''', (
                        rec['name'], rec['created_at'], rec['deleted']))
            conn.commit()

    def _commit_puts_load(self, item_list, entry):
        (name, timestamp, deleted) = entry
        item_list.append({
            'name': name,
            'created_at': timestamp,
            'deleted': deleted,
        })

    def _load_item(self, name, timestamp, deleted):
        if self.db_file == ':memory:':
            record = {
                'name': name,
                'created_at': timestamp,
                'deleted': deleted,
            }
            self.merge_items([record])
            return
        with open(self.pending_file, 'a+b') as fp:
            fp.write(b':')
            fp.write(base64.b64encode(pickle.dumps(
                (name, timestamp, deleted),
                protocol=PICKLE_PROTOCOL)))
            fp.flush()

    def put_test(self, name, timestamp):
        self._load_item(name, timestamp, 0)

    def delete_test(self, name, timestamp):
        self._load_item(name, timestamp, 1)

    def _delete_db(self, conn, timestamp):
        conn.execute("""
            UPDATE test_stat
            SET delete_timestamp = ?,
                status = 'DELETED',
                status_changed_at = ?
            WHERE delete_timestamp < ? """, (timestamp, timestamp, timestamp))

    def _is_deleted(self, conn):
        info = conn.execute('SELECT * FROM test_stat').fetchone()
        return (info['test_count'] in (None, '', 0, '0')) and \
            (Timestamp(info['delete_timestamp']) >
             Timestamp(info['put_timestamp']))


class TestExampleBroker(TestDbBase):
    """
    Tests that use the mostly Concrete enough ExampleBroker to exercise some
    of the abstract methods on DatabaseBroker.
    """

    broker_class = ExampleBroker
    policy = 0
    server_type = 'example'

    def setUp(self):
        super(TestExampleBroker, self).setUp()
        self.ts = make_timestamp_iter()

    def test_delete_db(self):
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(next(self.ts).internal)
        broker.delete_db(next(self.ts).internal)
        self.assertTrue(broker.is_deleted())

    def test_merge_timestamps_simple_delete(self):
        put_timestamp = next(self.ts).internal
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(put_timestamp)
        created_at = broker.get_info()['created_at']
        broker.merge_timestamps(created_at, put_timestamp, '0')
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], '0')
        self.assertEqual(info['status_changed_at'], put_timestamp)
        # delete
        delete_timestamp = next(self.ts).internal
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assertTrue(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertTrue(info['status_changed_at'] > Timestamp(put_timestamp))

    def put_item(self, broker, timestamp):
        broker.put_test('test', timestamp)

    def delete_item(self, broker, timestamp):
        broker.delete_test('test', timestamp)

    def test_merge_timestamps_delete_with_objects(self):
        put_timestamp = next(self.ts).internal
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        created_at = broker.get_info()['created_at']
        broker.merge_timestamps(created_at, put_timestamp, '0')
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], '0')
        self.assertEqual(info['status_changed_at'], put_timestamp)
        # add object
        self.put_item(broker, next(self.ts).internal)
        self.assertEqual(broker.get_info()[
            '%s_count' % broker.db_contains_type], 1)
        # delete
        delete_timestamp = next(self.ts).internal
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assertFalse(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        # status is unchanged
        self.assertEqual(info['status_changed_at'], put_timestamp)
        # count is causing status to hold on
        self.delete_item(broker, next(self.ts).internal)
        self.assertEqual(broker.get_info()[
            '%s_count' % broker.db_contains_type], 0)
        self.assertTrue(broker.is_deleted())

    def test_merge_timestamps_simple_recreate(self):
        put_timestamp = next(self.ts).internal
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        virgin_status_changed_at = broker.get_info()['status_changed_at']
        created_at = broker.get_info()['created_at']
        delete_timestamp = next(self.ts).internal
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assertTrue(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        orig_status_changed_at = info['status_changed_at']
        self.assertTrue(orig_status_changed_at >
                        Timestamp(virgin_status_changed_at))
        # recreate
        recreate_timestamp = next(self.ts).internal
        status_changed_at = time.time()
        with patch('swift.common.db.time.time', new=lambda: status_changed_at):
            broker.merge_timestamps(created_at, recreate_timestamp, '0')
        self.assertFalse(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], recreate_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertEqual(Timestamp(status_changed_at).normal,
                         info['status_changed_at'])

    def test_merge_timestamps_recreate_with_objects(self):
        put_timestamp = next(self.ts).internal
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        created_at = broker.get_info()['created_at']
        # delete
        delete_timestamp = next(self.ts).internal
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assertTrue(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        orig_status_changed_at = info['status_changed_at']
        self.assertTrue(Timestamp(orig_status_changed_at) >=
                        Timestamp(put_timestamp))
        # add object
        self.put_item(broker, next(self.ts).internal)
        count_key = '%s_count' % broker.db_contains_type
        self.assertEqual(broker.get_info()[count_key], 1)
        self.assertFalse(broker.is_deleted())
        # recreate
        recreate_timestamp = next(self.ts).internal
        broker.merge_timestamps(created_at, recreate_timestamp, '0')
        self.assertFalse(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], recreate_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertEqual(info['status_changed_at'], orig_status_changed_at)
        # count is not causing status to hold on
        self.delete_item(broker, next(self.ts).internal)
        self.assertFalse(broker.is_deleted())

    def test_merge_timestamps_update_put_no_status_change(self):
        put_timestamp = next(self.ts).internal
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        info = broker.get_info()
        orig_status_changed_at = info['status_changed_at']
        created_at = info['created_at']
        new_put_timestamp = next(self.ts).internal
        broker.merge_timestamps(created_at, new_put_timestamp, '0')
        info = broker.get_info()
        self.assertEqual(new_put_timestamp, info['put_timestamp'])
        self.assertEqual(orig_status_changed_at, info['status_changed_at'])

    def test_merge_timestamps_update_delete_no_status_change(self):
        put_timestamp = next(self.ts).internal
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        created_at = broker.get_info()['created_at']
        broker.merge_timestamps(created_at, put_timestamp,
                                next(self.ts).internal)
        orig_status_changed_at = broker.get_info()['status_changed_at']
        new_delete_timestamp = next(self.ts).internal
        broker.merge_timestamps(created_at, put_timestamp,
                                new_delete_timestamp)
        info = broker.get_info()
        self.assertEqual(new_delete_timestamp, info['delete_timestamp'])
        self.assertEqual(orig_status_changed_at, info['status_changed_at'])

    def test_get_max_row(self):
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(next(self.ts).internal,
                          storage_policy_index=int(self.policy))
        self.assertEqual(-1, broker.get_max_row())
        self.put_item(broker, next(self.ts).internal)
        # commit pending file into db
        broker._commit_puts()
        self.assertEqual(1, broker.get_max_row())
        self.delete_item(broker, next(self.ts).internal)
        broker._commit_puts()
        self.assertEqual(2, broker.get_max_row())
        self.put_item(broker, next(self.ts).internal)
        broker._commit_puts()
        self.assertEqual(3, broker.get_max_row())

    def test_get_info(self):
        broker = self.broker_class(self.db_path, account='test', container='c')
        created_at = time.time()
        with patch('swift.common.db.time.time', new=lambda: created_at):
            broker.initialize(Timestamp(1).internal,
                              storage_policy_index=int(self.policy))
        info = broker.get_info()
        count_key = '%s_count' % broker.db_contains_type
        expected = {
            count_key: 0,
            'created_at': Timestamp(created_at).internal,
            'put_timestamp': Timestamp(1).internal,
            'status_changed_at': Timestamp(1).internal,
            'delete_timestamp': '0',
        }
        for k, v in expected.items():
            self.assertEqual(info[k], v,
                             'mismatch for %s, %s != %s' % (
                                 k, info[k], v))

    def test_get_raw_metadata(self):
        broker = self.broker_class(self.db_path, account='test', container='c')
        broker.initialize(Timestamp(0).internal,
                          storage_policy_index=int(self.policy))
        self.assertEqual(broker.metadata, {})
        self.assertEqual(broker.get_raw_metadata(), '')
        metadata = {
            'test\u062a': ['value\u062a', Timestamp(1).internal]
        }
        broker.update_metadata(metadata)
        self.assertEqual(broker.metadata, metadata)
        self.assertEqual(broker.get_raw_metadata(),
                         json.dumps(metadata))

    def test_put_timestamp(self):
        broker = self.broker_class(self.db_path, account='a', container='c')
        orig_put_timestamp = next(self.ts).internal
        broker.initialize(orig_put_timestamp,
                          storage_policy_index=int(self.policy))
        self.assertEqual(broker.get_info()['put_timestamp'],
                         orig_put_timestamp)
        # put_timestamp equal - no change
        broker.update_put_timestamp(orig_put_timestamp)
        self.assertEqual(broker.get_info()['put_timestamp'],
                         orig_put_timestamp)
        # put_timestamp newer - gets newer
        newer_put_timestamp = next(self.ts).internal
        broker.update_put_timestamp(newer_put_timestamp)
        self.assertEqual(broker.get_info()['put_timestamp'],
                         newer_put_timestamp)
        # put_timestamp older - no change
        broker.update_put_timestamp(orig_put_timestamp)
        self.assertEqual(broker.get_info()['put_timestamp'],
                         newer_put_timestamp)

    def test_status_changed_at(self):
        broker = self.broker_class(self.db_path, account='test', container='c')
        put_timestamp = next(self.ts).internal
        created_at = time.time()
        with patch('swift.common.db.time.time', new=lambda: created_at):
            broker.initialize(put_timestamp,
                              storage_policy_index=int(self.policy))
        self.assertEqual(broker.get_info()['status_changed_at'],
                         put_timestamp)
        self.assertEqual(broker.get_info()['created_at'],
                         Timestamp(created_at).internal)
        status_changed_at = next(self.ts).internal
        broker.update_status_changed_at(status_changed_at)
        self.assertEqual(broker.get_info()['status_changed_at'],
                         status_changed_at)
        # save the old and get a new status_changed_at
        old_status_changed_at, status_changed_at = \
            status_changed_at, next(self.ts).internal
        broker.update_status_changed_at(status_changed_at)
        self.assertEqual(broker.get_info()['status_changed_at'],
                         status_changed_at)
        # status changed at won't go backwards...
        broker.update_status_changed_at(old_status_changed_at)
        self.assertEqual(broker.get_info()['status_changed_at'],
                         status_changed_at)

    def test_get_syncs(self):
        broker = self.broker_class(self.db_path, account='a', container='c')
        broker.initialize(Timestamp.now().internal,
                          storage_policy_index=int(self.policy))
        self.assertEqual([], broker.get_syncs())
        broker.merge_syncs([{'sync_point': 1, 'remote_id': 'remote1'}])
        self.assertEqual([{'sync_point': 1, 'remote_id': 'remote1'}],
                         broker.get_syncs())
        self.assertEqual([], broker.get_syncs(incoming=False))
        broker.merge_syncs([{'sync_point': 2, 'remote_id': 'remote2'}],
                           incoming=False)
        self.assertEqual([{'sync_point': 2, 'remote_id': 'remote2'}],
                         broker.get_syncs(incoming=False))

    def test_commit_pending(self):
        broker = self.broker_class(os.path.join(self.testdir, 'test.db'),
                                   account='a', container='c')
        broker.initialize(next(self.ts).internal,
                          storage_policy_index=int(self.policy))
        self.put_item(broker, next(self.ts).internal)
        qry = 'select * from %s_stat' % broker.db_type
        with broker.get() as conn:
            rows = [dict(x) for x in conn.execute(qry)]
        info = rows[0]
        count_key = '%s_count' % broker.db_contains_type
        self.assertEqual(0, info[count_key])
        # commit pending file into db
        broker._commit_puts()
        self.assertEqual(1, broker.get_info()[count_key])

    def test_maybe_get(self):
        broker = self.broker_class(os.path.join(self.testdir, 'test.db'),
                                   account='a', container='c')
        broker.initialize(next(self.ts).internal,
                          storage_policy_index=int(self.policy))
        qry = 'select account from %s_stat' % broker.db_type
        with broker.maybe_get(None) as conn:
            rows = [dict(x) for x in conn.execute(qry)]
        self.assertEqual([{'account': 'a'}], rows)
        self.assertEqual(conn, broker.conn)
        with broker.get() as other_conn:
            self.assertEqual(broker.conn, None)
            with broker.maybe_get(other_conn) as identity_conn:
                self.assertIs(other_conn, identity_conn)
                self.assertEqual(broker.conn, None)
            self.assertEqual(broker.conn, None)
        self.assertEqual(broker.conn, conn)


class TestDatabaseBroker(TestDbBase):

    def test_DB_PREALLOCATION_setting(self):
        u = uuid4().hex
        b = DatabaseBroker(u)
        swift.common.db.DB_PREALLOCATION = False
        b._preallocate()
        swift.common.db.DB_PREALLOCATION = True
        self.assertRaises(OSError, b._preallocate)

    def test_memory_db_init(self):
        broker = DatabaseBroker(self.db_path)
        self.assertEqual(broker.db_file, self.db_path)
        self.assertRaises(AttributeError, broker.initialize,
                          normalize_timestamp('0'))

    def test_disk_db_init(self):
        db_file = os.path.join(self.testdir, '1.db')
        broker = DatabaseBroker(db_file)
        self.assertEqual(broker.db_file, db_file)
        self.assertIsNone(broker.conn)

    def test_disk_preallocate(self):
        test_size = [-1]

        def fallocate_stub(fd, size):
            test_size[0] = size

        with patch('swift.common.db.fallocate', fallocate_stub):
            db_file = os.path.join(self.testdir, 'pre.db')
            # Write 1 byte and hope that the fs will allocate less than 1 MB.
            f = open(db_file, "w")
            f.write('@')
            f.close()
            b = DatabaseBroker(db_file)
            b._preallocate()
            # We only wrote 1 byte, so we should end with the 1st step or 1 MB.
            self.assertEqual(test_size[0], 1024 * 1024)

    def test_initialize(self):
        self.assertRaises(AttributeError,
                          DatabaseBroker(self.db_path).initialize,
                          normalize_timestamp('1'))
        stub_dict = {}

        def stub(*args, **kwargs):
            stub_dict.clear()
            stub_dict['args'] = args
            stub_dict.update(kwargs)
        broker = DatabaseBroker(self.db_path)
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        self.assertTrue(hasattr(stub_dict['args'][0], 'execute'))
        self.assertEqual(stub_dict['args'][1], '0000000001.00000')
        with broker.get() as conn:
            conn.execute('SELECT * FROM outgoing_sync')
            conn.execute('SELECT * FROM incoming_sync')
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        self.assertTrue(hasattr(stub_dict['args'][0], 'execute'))
        self.assertEqual(stub_dict['args'][1], '0000000001.00000')
        with broker.get() as conn:
            conn.execute('SELECT * FROM outgoing_sync')
            conn.execute('SELECT * FROM incoming_sync')
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        broker._initialize = stub
        self.assertRaises(DatabaseAlreadyExists,
                          broker.initialize, normalize_timestamp('1'))

    def test_delete_db(self):
        meta = {'foo': ['bar', normalize_timestamp('0')]}

        def init_stub(conn, put_timestamp, **kwargs):
            conn.execute('CREATE TABLE test (one TEXT)')
            conn.execute('''CREATE TABLE test_stat (
                id TEXT, put_timestamp TEXT, delete_timestamp TEXT,
                status TEXT, status_changed_at TEXT, metadata TEXT)''')
            conn.execute(
                '''INSERT INTO test_stat (
                    id, put_timestamp, delete_timestamp, status,
                    status_changed_at, metadata) VALUES (?, ?, ?, ?, ?, ?)''',
                (str(uuid4), put_timestamp, '0', '', '0', json.dumps(meta)))
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()

        def do_test(expected_metadata, delete_meta_whitelist=None):
            if not delete_meta_whitelist:
                delete_meta_whitelist = []
            broker = DatabaseBroker(self.get_db_path())
            broker.delete_meta_whitelist = delete_meta_whitelist
            broker.db_type = 'test'
            broker._initialize = init_stub
            # Initializes a good broker for us
            broker.initialize(normalize_timestamp('1'))
            info = broker.get_info()
            self.assertEqual('0', info['delete_timestamp'])
            self.assertEqual('', info['status'])
            self.assertIsNotNone(broker.conn)
            broker.delete_db(normalize_timestamp('2'))
            info = broker.get_info()
            self.assertEqual(normalize_timestamp('2'),
                             info['delete_timestamp'])
            self.assertEqual('DELETED', info['status'])

            # check meta
            m2 = broker.metadata
            self.assertEqual(m2, expected_metadata)

            broker = DatabaseBroker(os.path.join(self.testdir,
                                                 '%s.db' % uuid4()))
            broker.delete_meta_whitelist = delete_meta_whitelist
            broker.db_type = 'test'
            broker._initialize = init_stub
            broker.initialize(normalize_timestamp('1'))
            info = broker.get_info()
            self.assertEqual('0', info['delete_timestamp'])
            self.assertEqual('', info['status'])
            broker.delete_db(normalize_timestamp('2'))
            info = broker.get_info()
            self.assertEqual(normalize_timestamp('2'),
                             info['delete_timestamp'])
            self.assertEqual('DELETED', info['status'])

            # check meta
            m2 = broker.metadata
            self.assertEqual(m2, expected_metadata)

        # ensure that metadata was cleared by default
        do_test({'foo': ['', normalize_timestamp('2')]})

        # If the meta is in the brokers delete_meta_whitelist it wont get
        # cleared up
        do_test(meta, ['foo'])

        # delete_meta_whitelist things need to be in lower case, as the keys
        # are lower()'ed before checked
        meta["X-Container-Meta-Test"] = ['value', normalize_timestamp('0')]
        meta["X-Something-else"] = ['other', normalize_timestamp('0')]
        do_test({'foo': ['', normalize_timestamp('2')],
                 'X-Container-Meta-Test': ['value', normalize_timestamp('0')],
                 'X-Something-else': ['other', normalize_timestamp('0')]},
                ['x-container-meta-test', 'x-something-else'])

    def test_get(self):
        broker = DatabaseBroker(self.db_path)
        with self.assertRaises(DatabaseConnectionError) as raised, \
                broker.get() as conn:
            conn.execute('SELECT 1')
        self.assertEqual(
            str(raised.exception),
            "DB connection error (%s, 0):\nDB doesn't exist" % self.db_path)

        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        with self.assertRaises(DatabaseConnectionError) as raised, \
                broker.get() as conn:
            conn.execute('SELECT 1')
        self.assertEqual(
            str(raised.exception),
            "DB connection error (%s, 0):\nDB doesn't exist" % broker.db_file)

        def stub(*args, **kwargs):
            pass
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('CREATE TABLE test (one TEXT)')
        try:
            with broker.get() as conn:
                conn.execute('INSERT INTO test (one) VALUES ("1")')
                raise Exception('test')
                conn.commit()
        except Exception:
            pass
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        with broker.get() as conn:
            self.assertEqual(
                [r[0] for r in conn.execute('SELECT * FROM test')], [])
        with broker.get() as conn:
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        with broker.get() as conn:
            self.assertEqual(
                [r[0] for r in conn.execute('SELECT * FROM test')], ['1'])

        dbpath = os.path.join(self.testdir, 'dev', 'dbs', 'par', 'pre', 'db')
        mkdirs(dbpath)
        qpath = os.path.join(self.testdir, 'dev', 'quarantined', 'tests', 'db')
        with patch('swift.common.db.renamer', lambda a, b,
                   fsync: b):
            # Test malformed database
            copy(os.path.join(os.path.dirname(__file__),
                              'malformed_example.db'),
                 os.path.join(dbpath, '1.db'))
            broker = DatabaseBroker(os.path.join(dbpath, '1.db'))
            broker.db_type = 'test'
            with self.assertRaises(sqlite3.DatabaseError) as raised, \
                    broker.get() as conn:
                conn.execute('SELECT * FROM test')
            self.assertEqual(
                str(raised.exception),
                'Quarantined %s to %s due to malformed database' %
                (dbpath, qpath))
            # Test malformed schema database
            copy(os.path.join(os.path.dirname(__file__),
                              'malformed_schema_example.db'),
                 os.path.join(dbpath, '1.db'))
            broker = DatabaseBroker(os.path.join(dbpath, '1.db'))
            broker.db_type = 'test'
            with self.assertRaises(sqlite3.DatabaseError) as raised, \
                    broker.get() as conn:
                conn.execute('SELECT * FROM test')
            self.assertEqual(
                str(raised.exception),
                'Quarantined %s to %s due to malformed database' %
                (dbpath, qpath))
            # Test corrupted database
            copy(os.path.join(os.path.dirname(__file__),
                              'corrupted_example.db'),
                 os.path.join(dbpath, '1.db'))
            broker = DatabaseBroker(os.path.join(dbpath, '1.db'))
            broker.db_type = 'test'
            with self.assertRaises(sqlite3.DatabaseError) as raised, \
                    broker.get() as conn:
                conn.execute('SELECT * FROM test')
            self.assertEqual(
                str(raised.exception),
                'Quarantined %s to %s due to corrupted database' %
                (dbpath, qpath))

    def test_get_raw_metadata_missing_container_info(self):
        # Test missing container_info/container_stat row
        dbpath = os.path.join(self.testdir, 'dev', 'dbs', 'par', 'pre', 'db')
        mkdirs(dbpath)
        qpath = os.path.join(self.testdir, 'dev', 'quarantined', 'containers',
                             'db')
        copy(os.path.join(os.path.dirname(__file__),
                          'missing_container_info.db'),
             os.path.join(dbpath, '1.db'))

        broker = DatabaseBroker(os.path.join(dbpath, '1.db'))
        broker.db_type = 'container'

        with self.assertRaises(sqlite3.DatabaseError) as raised:
            broker.get_raw_metadata()
        self.assertEqual(
            str(raised.exception),
            'Quarantined %s to %s due to missing row in container_stat table' %
            (dbpath, qpath))

    def test_lock(self):
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'), timeout=.1)
        with self.assertRaises(DatabaseConnectionError) as raised, \
                broker.lock():
            pass
        self.assertEqual(
            str(raised.exception),
            "DB connection error (%s, 0):\nDB doesn't exist" % broker.db_file)

        def stub(*args, **kwargs):
            pass
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        with broker.lock():
            pass
        with broker.lock():
            pass

        with self.assertRaises(RuntimeError) as raised, broker.lock():
            raise RuntimeError('boom!')
        self.assertEqual(raised.exception.args[0], 'boom!')

        broker2 = DatabaseBroker(os.path.join(self.testdir, '1.db'),
                                 timeout=.1)
        broker2._initialize = stub
        with broker.lock():
            # broker2 raises the timeout
            with self.assertRaises(LockTimeout) as raised:
                with broker2.lock():
                    pass
        self.assertEqual(str(raised.exception),
                         '0.1 seconds: %s' % broker.db_file)

        # and the timeout bubbles up out of broker.lock()
        with self.assertRaises(LockTimeout) as raised:
            with broker.lock():
                with broker2.lock():
                    pass
        self.assertEqual(str(raised.exception),
                         '0.1 seconds: %s' % broker.db_file)

        try:
            with broker.lock():
                raise Exception('test')
        except Exception:
            pass
        with broker.lock():
            pass

    def test_newid(self):
        broker = DatabaseBroker(self.db_path)
        broker.db_type = 'test'
        broker.db_contains_type = 'test'
        uuid1 = str(uuid4())

        def _initialize(conn, timestamp, **kwargs):
            conn.execute('CREATE TABLE test (one TEXT)')
            conn.execute('CREATE TABLE test_stat (id TEXT)')
            conn.execute('INSERT INTO test_stat (id) VALUES (?)', (uuid1,))
            conn.commit()
        broker._initialize = _initialize
        broker.initialize(normalize_timestamp('1'))
        uuid2 = str(uuid4())
        broker.newid(uuid2)
        with broker.get() as conn:
            uuids = [r[0] for r in conn.execute('SELECT * FROM test_stat')]
            self.assertEqual(len(uuids), 1)
            self.assertNotEqual(uuids[0], uuid1)
            uuid1 = uuids[0]
            points = [(r[0], r[1]) for r in conn.execute(
                'SELECT sync_point, '
                'remote_id FROM incoming_sync WHERE remote_id = ?', (uuid2,))]
            self.assertEqual(len(points), 1)
            self.assertEqual(points[0][0], -1)
            self.assertEqual(points[0][1], uuid2)
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()
        uuid3 = str(uuid4())
        broker.newid(uuid3)
        with broker.get() as conn:
            uuids = [r[0] for r in conn.execute('SELECT * FROM test_stat')]
            self.assertEqual(len(uuids), 1)
            self.assertNotEqual(uuids[0], uuid1)
            uuid1 = uuids[0]
            points = [(r[0], r[1]) for r in conn.execute(
                'SELECT sync_point, '
                'remote_id FROM incoming_sync WHERE remote_id = ?', (uuid3,))]
            self.assertEqual(len(points), 1)
            self.assertEqual(points[0][1], uuid3)
        broker.newid(uuid2)
        with broker.get() as conn:
            uuids = [r[0] for r in conn.execute('SELECT * FROM test_stat')]
            self.assertEqual(len(uuids), 1)
            self.assertNotEqual(uuids[0], uuid1)
            points = [(r[0], r[1]) for r in conn.execute(
                'SELECT sync_point, '
                'remote_id FROM incoming_sync WHERE remote_id = ?', (uuid2,))]
            self.assertEqual(len(points), 1)
            self.assertEqual(points[0][1], uuid2)

    def test_get_items_since(self):
        broker = DatabaseBroker(self.db_path)
        broker.db_type = 'test'
        broker.db_contains_type = 'test'

        def _initialize(conn, timestamp, **kwargs):
            conn.execute('CREATE TABLE test (one TEXT)')
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.execute('INSERT INTO test (one) VALUES ("2")')
            conn.execute('INSERT INTO test (one) VALUES ("3")')
            conn.commit()
        broker._initialize = _initialize
        broker.initialize(normalize_timestamp('1'))
        self.assertEqual(broker.get_items_since(-1, 10),
                         [{'one': '1'}, {'one': '2'}, {'one': '3'}])
        self.assertEqual(broker.get_items_since(-1, 2),
                         [{'one': '1'}, {'one': '2'}])
        self.assertEqual(broker.get_items_since(1, 2),
                         [{'one': '2'}, {'one': '3'}])
        self.assertEqual(broker.get_items_since(3, 2), [])
        self.assertEqual(broker.get_items_since(999, 2), [])

    def test_get_syncs(self):
        broker = DatabaseBroker(self.db_path)
        broker.db_type = 'test'
        broker.db_contains_type = 'test'
        uuid1 = str(uuid4())

        def _initialize(conn, timestamp, **kwargs):
            conn.execute('CREATE TABLE test (one TEXT)')
            conn.execute('CREATE TABLE test_stat (id TEXT)')
            conn.execute('INSERT INTO test_stat (id) VALUES (?)', (uuid1,))
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()
            pass
        broker._initialize = _initialize
        broker.initialize(normalize_timestamp('1'))

        for incoming in (True, False):
            # Can't mock out timestamp now, because the update_at in the sync
            # tables are cuase by a trigger inside sqlite which uses it's own
            # now method. So instead track the time before and after to make
            # sure we're getting the right timestamps.
            ts0 = Timestamp.now()
            broker.merge_syncs([
                {'sync_point': 0, 'remote_id': 'remote_0'},
                {'sync_point': 1, 'remote_id': 'remote_1'}], incoming)

            time.sleep(0.005)
            broker.merge_syncs([
                {'sync_point': 2, 'remote_id': 'remote_2'}], incoming)

            ts1 = Timestamp.now()
            expected_syncs = [{'sync_point': 0, 'remote_id': 'remote_0'},
                              {'sync_point': 1, 'remote_id': 'remote_1'},
                              {'sync_point': 2, 'remote_id': 'remote_2'}]

            self.assertEqual(expected_syncs, broker.get_syncs(incoming))

            # if we want the updated_at timestamps too then:
            expected_syncs[0]['updated_at'] = mock.ANY
            expected_syncs[1]['updated_at'] = mock.ANY
            expected_syncs[2]['updated_at'] = mock.ANY
            actual_syncs = broker.get_syncs(incoming, include_timestamp=True)
            self.assertEqual(expected_syncs, actual_syncs)
            # Note that most of the time, we expect these all to be ==
            # but we've been known to see sizeable delays in the gate at times
            self.assertTrue(all([
                str(int(ts0)) <= s['updated_at'] <= str(int(ts1))
                for s in actual_syncs]))

    def test_get_sync(self):
        broker = DatabaseBroker(self.db_path)
        broker.db_type = 'test'
        broker.db_contains_type = 'test'
        uuid1 = str(uuid4())

        def _initialize(conn, timestamp, **kwargs):
            conn.execute('CREATE TABLE test (one TEXT)')
            conn.execute('CREATE TABLE test_stat (id TEXT)')
            conn.execute('INSERT INTO test_stat (id) VALUES (?)', (uuid1,))
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()
            pass
        broker._initialize = _initialize
        broker.initialize(normalize_timestamp('1'))
        uuid2 = str(uuid4())
        self.assertEqual(broker.get_sync(uuid2), -1)
        broker.newid(uuid2)
        self.assertEqual(broker.get_sync(uuid2), 1)
        uuid3 = str(uuid4())
        self.assertEqual(broker.get_sync(uuid3), -1)
        with broker.get() as conn:
            conn.execute('INSERT INTO test (one) VALUES ("2")')
            conn.commit()
        broker.newid(uuid3)
        self.assertEqual(broker.get_sync(uuid2), 1)
        self.assertEqual(broker.get_sync(uuid3), 2)
        self.assertEqual(broker.get_sync(uuid2, incoming=False), -1)
        self.assertEqual(broker.get_sync(uuid3, incoming=False), -1)
        broker.merge_syncs([{'sync_point': 1, 'remote_id': uuid2}],
                           incoming=False)
        self.assertEqual(broker.get_sync(uuid2), 1)
        self.assertEqual(broker.get_sync(uuid3), 2)
        self.assertEqual(broker.get_sync(uuid2, incoming=False), 1)
        self.assertEqual(broker.get_sync(uuid3, incoming=False), -1)
        broker.merge_syncs([{'sync_point': 2, 'remote_id': uuid3}],
                           incoming=False)
        self.assertEqual(broker.get_sync(uuid2, incoming=False), 1)
        self.assertEqual(broker.get_sync(uuid3, incoming=False), 2)

    def test_merge_syncs(self):
        broker = DatabaseBroker(self.db_path)

        def stub(*args, **kwargs):
            pass
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        uuid2 = str(uuid4())
        broker.merge_syncs([{'sync_point': 1, 'remote_id': uuid2}])
        self.assertEqual(broker.get_sync(uuid2), 1)
        uuid3 = str(uuid4())
        broker.merge_syncs([{'sync_point': 2, 'remote_id': uuid3}])
        self.assertEqual(broker.get_sync(uuid2), 1)
        self.assertEqual(broker.get_sync(uuid3), 2)
        self.assertEqual(broker.get_sync(uuid2, incoming=False), -1)
        self.assertEqual(broker.get_sync(uuid3, incoming=False), -1)
        broker.merge_syncs([{'sync_point': 3, 'remote_id': uuid2},
                            {'sync_point': 4, 'remote_id': uuid3}],
                           incoming=False)
        self.assertEqual(broker.get_sync(uuid2, incoming=False), 3)
        self.assertEqual(broker.get_sync(uuid3, incoming=False), 4)
        self.assertEqual(broker.get_sync(uuid2), 1)
        self.assertEqual(broker.get_sync(uuid3), 2)
        broker.merge_syncs([{'sync_point': 5, 'remote_id': uuid2}])
        self.assertEqual(broker.get_sync(uuid2), 5)
        # max sync point sticks
        broker.merge_syncs([{'sync_point': 5, 'remote_id': uuid2}])
        self.assertEqual(broker.get_sync(uuid2), 5)
        self.assertEqual(broker.get_sync(uuid3), 2)
        broker.merge_syncs([{'sync_point': 4, 'remote_id': uuid2}])
        self.assertEqual(broker.get_sync(uuid2), 5)
        self.assertEqual(broker.get_sync(uuid3), 2)
        broker.merge_syncs([{'sync_point': -1, 'remote_id': uuid2},
                            {'sync_point': 3, 'remote_id': uuid3}])
        self.assertEqual(broker.get_sync(uuid2), 5)
        self.assertEqual(broker.get_sync(uuid3), 3)
        self.assertEqual(broker.get_sync(uuid2, incoming=False), 3)
        self.assertEqual(broker.get_sync(uuid3, incoming=False), 4)

    def test_get_replication_info(self):
        self.get_replication_info_tester(metadata=False)

    def test_get_replication_info_with_metadata(self):
        self.get_replication_info_tester(metadata=True)

    def get_replication_info_tester(self, metadata=False):
        broker = DatabaseBroker(self.db_path, account='a')
        broker.db_type = 'test'
        broker.db_contains_type = 'test'
        broker.db_reclaim_timestamp = 'created_at'
        broker_creation = normalize_timestamp(1)
        broker_uuid = str(uuid4())
        broker_metadata = metadata and json.dumps(
            {'Test': ('Value', normalize_timestamp(1))}) or ''

        def _initialize(conn, put_timestamp, **kwargs):
            if put_timestamp is None:
                put_timestamp = normalize_timestamp(0)
            conn.executescript('''
                CREATE TABLE test (
                    ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE,
                    created_at TEXT
                );
                CREATE TRIGGER test_insert AFTER INSERT ON test
                BEGIN
                    UPDATE test_stat
                    SET test_count = test_count + 1,
                        hash = chexor(hash, new.name, new.created_at);
                END;
                CREATE TRIGGER test_update BEFORE UPDATE ON test
                BEGIN
                    SELECT RAISE(FAIL,
                                 'UPDATE not allowed; DELETE and INSERT');
                END;
                CREATE TRIGGER test_delete AFTER DELETE ON test
                BEGIN
                    UPDATE test_stat
                    SET test_count = test_count - 1,
                        hash = chexor(hash, old.name, old.created_at);
                END;
                CREATE TABLE test_stat (
                    account TEXT,
                    created_at TEXT,
                    put_timestamp TEXT DEFAULT '0',
                    delete_timestamp TEXT DEFAULT '0',
                    status_changed_at TEXT DEFAULT '0',
                    test_count INTEGER,
                    hash TEXT default '00000000000000000000000000000000',
                    id TEXT
                    %s
                );
                INSERT INTO test_stat (test_count) VALUES (0);
            ''' % (metadata and ", metadata TEXT DEFAULT ''" or ""))
            conn.execute('''
                UPDATE test_stat
                SET account = ?, created_at = ?,  id = ?, put_timestamp = ?,
                    status_changed_at = ?
            ''', (broker.account, broker_creation, broker_uuid, put_timestamp,
                  put_timestamp))
            if metadata:
                conn.execute('UPDATE test_stat SET metadata = ?',
                             (broker_metadata,))
            conn.commit()
        broker._initialize = _initialize
        put_timestamp = normalize_timestamp(2)
        broker.initialize(put_timestamp)
        info = broker.get_replication_info()
        self.assertEqual(info, {
            'account': broker.account, 'count': 0,
            'hash': '00000000000000000000000000000000',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'status_changed_at': put_timestamp,
            'max_row': -1, 'id': broker_uuid, 'metadata': broker_metadata})
        insert_timestamp = normalize_timestamp(3)
        with broker.get() as conn:
            conn.execute('''
                INSERT INTO test (name, created_at) VALUES ('test', ?)
            ''', (insert_timestamp,))
            conn.commit()
        info = broker.get_replication_info()
        self.assertEqual(info, {
            'account': broker.account, 'count': 1,
            'hash': 'bdc4c93f574b0d8c2911a27ce9dd38ba',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'status_changed_at': put_timestamp,
            'max_row': 1, 'id': broker_uuid, 'metadata': broker_metadata})
        with broker.get() as conn:
            conn.execute('DELETE FROM test')
            conn.commit()
        info = broker.get_replication_info()
        self.assertEqual(info, {
            'account': broker.account, 'count': 0,
            'hash': '00000000000000000000000000000000',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'status_changed_at': put_timestamp,
            'max_row': 1, 'id': broker_uuid, 'metadata': broker_metadata})
        return broker

    # only testing _reclaim_metadata here
    @patch.object(TombstoneReclaimer, 'reclaim')
    def test_metadata(self, mock_reclaim):
        # Initializes a good broker for us
        broker = self.get_replication_info_tester(metadata=True)
        # Add our first item
        first_timestamp = normalize_timestamp(1)
        first_value = '1'
        broker.update_metadata({'First': [first_value, first_timestamp]})
        self.assertIn('First', broker.metadata)
        self.assertEqual(broker.metadata['First'],
                         [first_value, first_timestamp])
        # Add our second item
        second_timestamp = normalize_timestamp(2)
        second_value = '2'
        broker.update_metadata({'Second': [second_value, second_timestamp]})
        self.assertIn('First', broker.metadata)
        self.assertEqual(broker.metadata['First'],
                         [first_value, first_timestamp])
        self.assertIn('Second', broker.metadata)
        self.assertEqual(broker.metadata['Second'],
                         [second_value, second_timestamp])
        # Update our first item
        first_timestamp = normalize_timestamp(3)
        first_value = '1b'
        broker.update_metadata({'First': [first_value, first_timestamp]})
        self.assertIn('First', broker.metadata)
        self.assertEqual(broker.metadata['First'],
                         [first_value, first_timestamp])
        self.assertIn('Second', broker.metadata)
        self.assertEqual(broker.metadata['Second'],
                         [second_value, second_timestamp])
        # Delete our second item (by setting to empty string)
        second_timestamp = normalize_timestamp(4)
        second_value = ''
        broker.update_metadata({'Second': [second_value, second_timestamp]})
        self.assertIn('First', broker.metadata)
        self.assertEqual(broker.metadata['First'],
                         [first_value, first_timestamp])
        self.assertIn('Second', broker.metadata)
        self.assertEqual(broker.metadata['Second'],
                         [second_value, second_timestamp])
        # Reclaim at point before second item was deleted
        broker.reclaim(normalize_timestamp(3), normalize_timestamp(3))
        self.assertIn('First', broker.metadata)
        self.assertEqual(broker.metadata['First'],
                         [first_value, first_timestamp])
        self.assertIn('Second', broker.metadata)
        self.assertEqual(broker.metadata['Second'],
                         [second_value, second_timestamp])
        # Reclaim at point second item was deleted
        broker.reclaim(normalize_timestamp(4), normalize_timestamp(4))
        self.assertIn('First', broker.metadata)
        self.assertEqual(broker.metadata['First'],
                         [first_value, first_timestamp])
        self.assertIn('Second', broker.metadata)
        self.assertEqual(broker.metadata['Second'],
                         [second_value, second_timestamp])
        # Reclaim after point second item was deleted
        broker.reclaim(normalize_timestamp(5), normalize_timestamp(5))
        self.assertIn('First', broker.metadata)
        self.assertEqual(broker.metadata['First'],
                         [first_value, first_timestamp])
        self.assertNotIn('Second', broker.metadata)
        # Delete first item (by setting to empty string)
        first_timestamp = normalize_timestamp(6)
        broker.update_metadata({'First': ['', first_timestamp]})
        self.assertIn('First', broker.metadata)
        # Check that sync_timestamp doesn't cause item to be reclaimed
        broker.reclaim(normalize_timestamp(5), normalize_timestamp(99))
        self.assertIn('First', broker.metadata)

    def test_update_metadata_missing_container_info(self):
        # Test missing container_info/container_stat row
        dbpath = os.path.join(self.testdir, 'dev', 'dbs', 'par', 'pre', 'db')
        mkdirs(dbpath)
        qpath = os.path.join(self.testdir, 'dev', 'quarantined', 'containers',
                             'db')
        copy(os.path.join(os.path.dirname(__file__),
                          'missing_container_info.db'),
             os.path.join(dbpath, '1.db'))

        broker = DatabaseBroker(os.path.join(dbpath, '1.db'))
        broker.db_type = 'container'

        with self.assertRaises(sqlite3.DatabaseError) as raised:
            broker.update_metadata({'First': ['1', normalize_timestamp(1)]})
        self.assertEqual(
            str(raised.exception),
            'Quarantined %s to %s due to missing row in container_stat table' %
            (dbpath, qpath))

    def test_reclaim_missing_container_info(self):
        # Test missing container_info/container_stat row
        dbpath = os.path.join(self.testdir, 'dev', 'dbs', 'par', 'pre', 'db')
        mkdirs(dbpath)
        qpath = os.path.join(self.testdir, 'dev', 'quarantined', 'containers',
                             'db')
        copy(os.path.join(os.path.dirname(__file__),
                          'missing_container_info.db'),
             os.path.join(dbpath, '1.db'))

        broker = DatabaseBroker(os.path.join(dbpath, '1.db'))
        broker.db_type = 'container'

        with self.assertRaises(sqlite3.DatabaseError) as raised, \
                broker.get() as conn:
            broker._reclaim_metadata(conn, 0)
        self.assertEqual(
            str(raised.exception),
            'Quarantined %s to %s due to missing row in container_stat table' %
            (dbpath, qpath))

    @patch.object(DatabaseBroker, 'validate_metadata')
    def test_validate_metadata_is_called_from_update_metadata(self, mock):
        broker = self.get_replication_info_tester(metadata=True)
        first_timestamp = normalize_timestamp(1)
        first_value = '1'
        metadata = {'First': [first_value, first_timestamp]}
        broker.update_metadata(metadata, validate_metadata=True)
        self.assertTrue(mock.called)

    @patch.object(DatabaseBroker, 'validate_metadata')
    def test_validate_metadata_is_not_called_from_update_metadata(self, mock):
        broker = self.get_replication_info_tester(metadata=True)
        first_timestamp = normalize_timestamp(1)
        first_value = '1'
        metadata = {'First': [first_value, first_timestamp]}
        broker.update_metadata(metadata)
        self.assertFalse(mock.called)

    def test_metadata_with_max_count(self):
        metadata = {}
        for c in range(MAX_META_COUNT):
            key = 'X-Account-Meta-F{0}'.format(c)
            metadata[key] = ('B', normalize_timestamp(1))
        key = 'X-Account-Meta-Foo'
        metadata[key] = ('', normalize_timestamp(1))
        self.assertIsNone(DatabaseBroker.validate_metadata(metadata))

    def test_metadata_raises_exception_on_non_utf8(self):
        def try_validate(metadata):
            with self.assertRaises(HTTPException) as raised:
                DatabaseBroker.validate_metadata(metadata)
            self.assertEqual(str(raised.exception), '400 Bad Request')
        ts = normalize_timestamp(1)
        try_validate({'X-Account-Meta-Foo': (b'\xff', ts)})
        try_validate({b'X-Container-Meta-\xff': ('bar', ts)})

    def test_metadata_raises_exception_over_max_count(self):
        metadata = {}
        for c in range(MAX_META_COUNT + 1):
            key = 'X-Account-Meta-F{0}'.format(c)
            metadata[key] = ('B', normalize_timestamp(1))
        message = ''
        try:
            DatabaseBroker.validate_metadata(metadata)
        except HTTPException as e:
            message = str(e)
        self.assertEqual(message, '400 Bad Request')

    def test_metadata_with_max_overall_size(self):
        metadata = {}
        metadata_value = 'v' * MAX_META_VALUE_LENGTH
        size = 0
        x = 0
        while size < (MAX_META_OVERALL_SIZE - 4
                      - MAX_META_VALUE_LENGTH):
            size += 4 + MAX_META_VALUE_LENGTH
            metadata['X-Account-Meta-%04d' % x] = (metadata_value,
                                                   normalize_timestamp(1))
            x += 1
        if MAX_META_OVERALL_SIZE - size > 1:
            metadata['X-Account-Meta-k'] = (
                'v' * (MAX_META_OVERALL_SIZE - size - 1),
                normalize_timestamp(1))
        self.assertIsNone(DatabaseBroker.validate_metadata(metadata))

    def test_metadata_raises_exception_over_max_overall_size(self):
        metadata = {}
        metadata_value = 'k' * MAX_META_VALUE_LENGTH
        size = 0
        x = 0
        while size < (MAX_META_OVERALL_SIZE - 4
                      - MAX_META_VALUE_LENGTH):
            size += 4 + MAX_META_VALUE_LENGTH
            metadata['X-Account-Meta-%04d' % x] = (metadata_value,
                                                   normalize_timestamp(1))
            x += 1
        if MAX_META_OVERALL_SIZE - size > 1:
            metadata['X-Account-Meta-k'] = (
                'v' * (MAX_META_OVERALL_SIZE - size - 1),
                normalize_timestamp(1))
        metadata['X-Account-Meta-k2'] = ('v', normalize_timestamp(1))
        message = ''
        try:
            DatabaseBroker.validate_metadata(metadata)
        except HTTPException as e:
            message = str(e)
        self.assertEqual(message, '400 Bad Request')

    def test_possibly_quarantine_db_errors(self):
        dbpath = os.path.join(self.testdir, 'dev', 'dbs', 'par', 'pre', 'db')
        qpath = os.path.join(self.testdir, 'dev', 'quarantined', 'tests', 'db')
        # Data is a list of Excpetions to be raised and expected values in the
        # log
        data = [
            (sqlite3.DatabaseError('database disk image is malformed'),
             'malformed'),
            (sqlite3.DatabaseError('malformed database schema'), 'malformed'),
            (sqlite3.DatabaseError('file is encrypted or is not a database'),
             'corrupted'),
            (sqlite3.OperationalError('disk I/O error'),
             'disk error while accessing')]

        for i, (ex, hint) in enumerate(data):
            mkdirs(dbpath)
            broker = DatabaseBroker(os.path.join(dbpath, '%d.db' % (i)))
            broker.db_type = 'test'
            with self.assertRaises(sqlite3.DatabaseError) as raised:
                broker.possibly_quarantine(ex)
            self.assertEqual(
                str(raised.exception),
                'Quarantined %s to %s due to %s database' %
                (dbpath, qpath, hint))

    def test_skip_commits(self):
        broker = DatabaseBroker(self.db_path)
        self.assertTrue(broker._skip_commit_puts())
        broker._initialize = MagicMock()
        broker.initialize(Timestamp.now())
        self.assertTrue(broker._skip_commit_puts())

        # not initialized
        db_file = os.path.join(self.testdir, '1.db')
        broker = DatabaseBroker(db_file)
        self.assertFalse(os.path.exists(broker.db_file))  # sanity check
        self.assertTrue(broker._skip_commit_puts())

        # no pending file
        broker._initialize = MagicMock()
        broker.initialize(Timestamp.now())
        self.assertTrue(os.path.exists(broker.db_file))  # sanity check
        self.assertFalse(os.path.exists(broker.pending_file))  # sanity check
        self.assertTrue(broker._skip_commit_puts())

        # pending file exists
        with open(broker.pending_file, 'wb'):
            pass
        self.assertTrue(os.path.exists(broker.pending_file))  # sanity check
        self.assertFalse(broker._skip_commit_puts())

        # skip_commits is True
        broker.skip_commits = True
        self.assertTrue(broker._skip_commit_puts())

        # re-init
        broker = DatabaseBroker(db_file)
        self.assertFalse(broker._skip_commit_puts())

        # constructor can override
        broker = DatabaseBroker(db_file, skip_commits=True)
        self.assertTrue(broker._skip_commit_puts())

    def test_commit_puts(self):
        db_file = os.path.join(self.testdir, '1.db')
        broker = DatabaseBroker(db_file)
        broker._initialize = MagicMock()
        broker.initialize(Timestamp.now())
        with open(broker.pending_file, 'wb'):
            pass

        # merge given list
        with patch.object(broker, 'merge_items') as mock_merge_items:
            broker._commit_puts(['test'])
        mock_merge_items.assert_called_once_with(['test'])

        # load file and merge
        with open(broker.pending_file, 'wb') as fd:
            for v in (1, 2, 99):
                fd.write(b':' + base64.b64encode(pickle.dumps(
                    v, protocol=PICKLE_PROTOCOL)))
        with patch.object(broker, 'merge_items') as mock_merge_items:
            broker._commit_puts_load = lambda l, e: l.append(e)
            broker._commit_puts()
        mock_merge_items.assert_called_once_with([1, 2, 99])
        self.assertEqual(0, os.path.getsize(broker.pending_file))

        # load file and merge with given list
        with open(broker.pending_file, 'wb') as fd:
            fd.write(b':' + base64.b64encode(pickle.dumps(
                b'bad', protocol=PICKLE_PROTOCOL)))
        with patch.object(broker, 'merge_items') as mock_merge_items:
            broker._commit_puts_load = lambda l, e: l.append(e)
            broker._commit_puts([b'not'])
        mock_merge_items.assert_called_once_with([b'not', b'bad'])
        self.assertEqual(0, os.path.getsize(broker.pending_file))

        # load a pending entry that's caused trouble in py2/py3 upgrade tests
        # can't quite figure out how it got generated, though, so hard-code it
        with open(broker.pending_file, 'wb') as fd:
            fd.write(b':gAIoVS3olIngpILrjIvrjIvpkIngpIHlmIjlmIbjnIbgp'
                     b'IPjnITimIPvhI/rjI3tiI5xAVUQMTU1OTI0MTg0Ni40NjY'
                     b'wMXECVQEwVQEwVQEwSwBVATB0Lg==')
        with patch.object(broker, 'merge_items') as mock_merge_items:
            broker._commit_puts_load = lambda l, e: l.append(e)
            broker._commit_puts([])
        expected_name = (u'\u8509\u0902\ub30b\ub30b\u9409\u0901\u5608\u5606'
                         u'\u3706\u0903\u3704\u2603\uf10f\ub30d\ud20e')
        mock_merge_items.assert_called_once_with([
            (expected_name, '1559241846.46601', '0', '0', '0', 0, '0')])
        self.assertEqual(0, os.path.getsize(broker.pending_file))

        # skip_commits True - no merge
        db_file = os.path.join(self.testdir, '2.db')
        broker = DatabaseBroker(db_file, skip_commits=True)
        broker._initialize = MagicMock()
        broker.initialize(Timestamp.now())
        with open(broker.pending_file, 'wb') as fd:
            fd.write(b':ignored')
        with patch.object(broker, 'merge_items') as mock_merge_items:
            with self.assertRaises(DatabaseConnectionError) as cm:
                broker._commit_puts([b'hmmm'])
        mock_merge_items.assert_not_called()
        self.assertIn('commits not accepted', str(cm.exception))
        with open(broker.pending_file, 'rb') as fd:
            self.assertEqual(b':ignored', fd.read())

    def test_put_record(self):
        db_file = os.path.join(self.testdir, '1.db')
        broker = DatabaseBroker(db_file)
        broker._initialize = MagicMock()
        broker.initialize(Timestamp.now())

        # pending file created and record written
        broker.make_tuple_for_pickle = lambda x: x.upper()
        with patch.object(broker, '_commit_puts') as mock_commit_puts:
            broker.put_record('pinky')
        mock_commit_puts.assert_not_called()
        with open(broker.pending_file, 'rb') as fd:
            pending = fd.read()
        items = pending.split(b':')
        self.assertEqual(['PINKY'],
                         [pickle.loads(base64.b64decode(i))
                             for i in items[1:]])

        # record appended
        with patch.object(broker, '_commit_puts') as mock_commit_puts:
            broker.put_record('perky')
        mock_commit_puts.assert_not_called()
        with open(broker.pending_file, 'rb') as fd:
            pending = fd.read()
        items = pending.split(b':')
        self.assertEqual(['PINKY', 'PERKY'],
                         [pickle.loads(base64.b64decode(i))
                             for i in items[1:]])

        # pending file above cap
        cap = swift.common.db.PENDING_CAP
        while os.path.getsize(broker.pending_file) < cap:
            with open(broker.pending_file, 'ab') as fd:
                fd.write(b'x' * 100000)
        with patch.object(broker, '_commit_puts') as mock_commit_puts:
            broker.put_record('direct')
        mock_commit_puts.assert_called_once_with(['direct'])

        # records shouldn't be put to brokers with skip_commits True because
        # they cannot be accepted if the pending file is full
        broker.skip_commits = True
        with open(broker.pending_file, 'wb'):
            # empty the pending file
            pass
        with patch.object(broker, '_commit_puts') as mock_commit_puts:
            with self.assertRaises(DatabaseConnectionError) as cm:
                broker.put_record('unwelcome')
        self.assertIn('commits not accepted', str(cm.exception))
        mock_commit_puts.assert_not_called()
        with open(broker.pending_file, 'rb') as fd:
            pending = fd.read()
        self.assertFalse(pending)


class TestTombstoneReclaimer(TestDbBase):
    def _make_object(self, broker, obj_name, ts, deleted):
        if deleted:
            broker.delete_test(obj_name, ts.internal)
        else:
            broker.put_test(obj_name, ts.internal)

    def _count_reclaimable(self, conn, reclaim_age):
        return conn.execute(
            "SELECT count(*) FROM test "
            "WHERE deleted = 1 AND created_at < ?", (reclaim_age,)
        ).fetchone()[0]

    def _get_reclaimable(self, broker, reclaim_age):
        with broker.get() as conn:
            return self._count_reclaimable(conn, reclaim_age)

    def _setup_tombstones(self, reverse_names=True):
        broker = ExampleBroker(self.db_path,
                               account='test_account',
                               container='test_container')
        broker.initialize(Timestamp('1').internal, 0)
        now = time.time()
        top_of_the_minute = now - (now % 60)

        # namespace if reverse:
        #  a-* has 70 'active' tombstones followed by 70 reclaimable
        #  b-* has 70 'active' tombstones followed by 70 reclaimable
        # else:
        #  a-* has 70 reclaimable followed by 70 'active' tombstones
        #  b-* has 70 reclaimable followed by 70 'active' tombstones
        for i in range(0, 560, 4):
            self._make_object(
                broker, 'a_%3d' % (560 - i if reverse_names else i),
                Timestamp(top_of_the_minute - (i * 60)), True)
            self._make_object(
                broker, 'a_%3d' % (559 - i if reverse_names else i + 1),
                Timestamp(top_of_the_minute - ((i + 1) * 60)), False)
            self._make_object(
                broker, 'b_%3d' % (560 - i if reverse_names else i),
                Timestamp(top_of_the_minute - ((i + 2) * 60)), True)
            self._make_object(
                broker, 'b_%3d' % (559 - i if reverse_names else i + 1),
                Timestamp(top_of_the_minute - ((i + 3) * 60)), False)
        broker._commit_puts()

        # divide the set of timestamps exactly in half for reclaim
        reclaim_age = top_of_the_minute + 1 - (560 / 2 * 60)
        self.assertEqual(140, self._get_reclaimable(broker, reclaim_age))

        tombstones = self._get_reclaimable(broker, top_of_the_minute + 1)
        self.assertEqual(280, tombstones)
        return broker, top_of_the_minute, reclaim_age

    @contextlib.contextmanager
    def _mock_broker_get(self, broker, reclaim_age):
        # intercept broker.get() calls and capture the current reclaimable
        # count before returning a conn
        orig_get = broker.get
        reclaimable = []

        @contextlib.contextmanager
        def mock_get():
            with orig_get() as conn:
                reclaimable.append(self._count_reclaimable(conn, reclaim_age))
                yield conn
        with patch.object(broker, 'get', mock_get):
            yield reclaimable

    def test_batched_reclaim_several_small_batches(self):
        broker, totm, reclaim_age = self._setup_tombstones()

        with self._mock_broker_get(broker, reclaim_age) as reclaimable:
            with patch('swift.common.db.RECLAIM_PAGE_SIZE', 50):
                reclaimer = TombstoneReclaimer(broker, reclaim_age)
                reclaimer.reclaim()

        expected_reclaimable = [140,  # 0 rows fetched
                                90,  # 50 rows fetched, 50 reclaimed
                                70,  # 100 rows fetched, 20 reclaimed
                                60,  # 150 rows fetched, 10 reclaimed
                                10,  # 200 rows fetched, 50 reclaimed
                                0,  # 250 rows fetched, 10 reclaimed
                                ]
        self.assertEqual(expected_reclaimable, reclaimable)
        self.assertEqual(0, self._get_reclaimable(broker, reclaim_age))

    def test_batched_reclaim_exactly_two_batches(self):
        broker, totm, reclaim_age = self._setup_tombstones()

        with self._mock_broker_get(broker, reclaim_age) as reclaimable:
            with patch('swift.common.db.RECLAIM_PAGE_SIZE', 140):
                reclaimer = TombstoneReclaimer(broker, reclaim_age)
                reclaimer.reclaim()

        expected_reclaimable = [140,  # 0 rows fetched
                                70,  # 140 rows fetched, 70 reclaimed
                                ]
        self.assertEqual(expected_reclaimable, reclaimable)
        self.assertEqual(0, self._get_reclaimable(broker, reclaim_age))

    def test_batched_reclaim_one_large_batch(self):
        broker, totm, reclaim_age = self._setup_tombstones()

        with self._mock_broker_get(broker, reclaim_age) as reclaimable:
            with patch('swift.common.db.RECLAIM_PAGE_SIZE', 1000):
                reclaimer = TombstoneReclaimer(broker, reclaim_age)
                reclaimer.reclaim()

        expected_reclaimable = [140]  # 0 rows fetched
        self.assertEqual(expected_reclaimable, reclaimable)
        self.assertEqual(0, self._get_reclaimable(broker, reclaim_age))

    def test_reclaim_get_tombstone_count(self):
        broker, totm, reclaim_age = self._setup_tombstones(reverse_names=False)
        with patch('swift.common.db.RECLAIM_PAGE_SIZE', 122):
            reclaimer = TombstoneReclaimer(broker, reclaim_age)
            reclaimer.reclaim()
        self.assertEqual(0, self._get_reclaimable(broker, reclaim_age))
        tombstones = self._get_reclaimable(broker, totm + 1)
        self.assertEqual(140, tombstones)
        # in this scenario the reclaim phase finds the remaining tombstone
        # count (140)
        self.assertEqual(140, reclaimer.remaining_tombstones)
        self.assertEqual(140, reclaimer.get_tombstone_count())

    def test_reclaim_get_tombstone_count_with_leftover(self):
        broker, totm, reclaim_age = self._setup_tombstones()
        with patch('swift.common.db.RECLAIM_PAGE_SIZE', 122):
            reclaimer = TombstoneReclaimer(broker, reclaim_age)
            reclaimer.reclaim()

        self.assertEqual(0, self._get_reclaimable(broker, reclaim_age))
        tombstones = self._get_reclaimable(broker, totm + 1)
        self.assertEqual(140, tombstones)
        # in this scenario the reclaim phase finds a subset (104) of all
        # tombstones (140)
        self.assertEqual(104, reclaimer.remaining_tombstones)
        # get_tombstone_count finds the rest
        actual = reclaimer.get_tombstone_count()
        self.assertEqual(140, actual)

    def test_get_tombstone_count_with_leftover(self):
        # verify that a call to get_tombstone_count() will invoke a reclaim if
        # reclaim not already invoked
        broker, totm, reclaim_age = self._setup_tombstones()
        with patch('swift.common.db.RECLAIM_PAGE_SIZE', 122):
            reclaimer = TombstoneReclaimer(broker, reclaim_age)
            actual = reclaimer.get_tombstone_count()

        self.assertEqual(0, self._get_reclaimable(broker, reclaim_age))
        self.assertEqual(140, actual)


if __name__ == '__main__':
    unittest.main()
