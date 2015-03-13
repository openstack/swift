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

import os
import unittest
from tempfile import mkdtemp
from shutil import rmtree, copy
from uuid import uuid4
import cPickle as pickle

import simplejson
import sqlite3
import itertools
import time
import random
from mock import patch, MagicMock

from eventlet.timeout import Timeout

import swift.common.db
from swift.common.constraints import \
    MAX_META_VALUE_LENGTH, MAX_META_COUNT, MAX_META_OVERALL_SIZE
from swift.common.db import chexor, dict_factory, get_db_connection, \
    DatabaseBroker, DatabaseConnectionError, DatabaseAlreadyExists, \
    GreenDBConnection, PICKLE_PROTOCOL
from swift.common.utils import normalize_timestamp, mkdirs, json, Timestamp
from swift.common.exceptions import LockTimeout
from swift.common.swob import HTTPException

from test.unit import with_tempdir


class TestDatabaseConnectionError(unittest.TestCase):

    def test_str(self):
        err = \
            DatabaseConnectionError(':memory:', 'No valid database connection')
        self.assert_(':memory:' in str(err))
        self.assert_('No valid database connection' in str(err))
        err = DatabaseConnectionError(':memory:',
                                      'No valid database connection',
                                      timeout=1357)
        self.assert_(':memory:' in str(err))
        self.assert_('No valid database connection' in str(err))
        self.assert_('1357' in str(err))


class TestDictFactory(unittest.TestCase):

    def test_normal_case(self):
        conn = sqlite3.connect(':memory:')
        conn.execute('CREATE TABLE test (one TEXT, two INTEGER)')
        conn.execute('INSERT INTO test (one, two) VALUES ("abc", 123)')
        conn.execute('INSERT INTO test (one, two) VALUES ("def", 456)')
        conn.commit()
        curs = conn.execute('SELECT one, two FROM test')
        self.assertEquals(dict_factory(curs, curs.next()),
                          {'one': 'abc', 'two': 123})
        self.assertEquals(dict_factory(curs, curs.next()),
                          {'one': 'def', 'two': 456})


class TestChexor(unittest.TestCase):

    def test_normal_case(self):
        self.assertEquals(
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
            ('frank', ts.next()),
            ('bob', ts.next()),
            ('tom', ts.next()),
            ('frank', ts.next()),
            ('tom', ts.next()),
            ('bob', ts.next()),
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


class TestGetDBConnection(unittest.TestCase):

    def test_normal_case(self):
        conn = get_db_connection(':memory:')
        self.assert_(hasattr(conn, 'execute'))

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
            self.assertRaises(Timeout, get_db_connection, ':memory:',
                              timeout=0.1)
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
            account, created_at, id, put_timestamp, status_changed_at)
        VALUES (?, ?, ?, ?, ?);
        """, (self.account, Timestamp(time.time()).internal, str(uuid4()),
              put_timestamp, put_timestamp))

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
        (name, timestamp, deleted) = pickle.loads(entry.decode('base64'))
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
            fp.write(':')
            fp.write(pickle.dumps(
                (name, timestamp, deleted),
                protocol=PICKLE_PROTOCOL).encode('base64'))
            fp.flush()

    def put_test(self, name, timestamp):
        self._load_item(name, timestamp, 0)

    def delete_test(self, name, timestamp):
        self._load_item(name, timestamp, 1)

    def _delete_db(self, conn, timestamp):
        conn.execute("""
            UPDATE test_stat
            SET delete_timestamp = ?,
                status_changed_at = ?
            WHERE delete_timestamp < ? """, (timestamp, timestamp, timestamp))

    def _is_deleted(self, conn):
        info = conn.execute('SELECT * FROM test_stat').fetchone()
        return (info['test_count'] in (None, '', 0, '0')) and \
            (Timestamp(info['delete_timestamp']) >
             Timestamp(info['put_timestamp']))


class TestExampleBroker(unittest.TestCase):
    """
    Tests that use the mostly Concrete enough ExampleBroker to exercise some
    of the abstract methods on DatabaseBroker.
    """

    broker_class = ExampleBroker
    policy = 0

    def setUp(self):
        self.ts = (Timestamp(t).internal for t in
                   itertools.count(int(time.time())))

    def test_delete_db(self):
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(next(self.ts))
        broker.delete_db(next(self.ts))
        self.assertTrue(broker.is_deleted())

    def test_merge_timestamps_simple_delete(self):
        put_timestamp = next(self.ts)
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(put_timestamp)
        created_at = broker.get_info()['created_at']
        broker.merge_timestamps(created_at, put_timestamp, '0')
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], '0')
        self.assertEqual(info['status_changed_at'], put_timestamp)
        # delete
        delete_timestamp = next(self.ts)
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assert_(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assert_(info['status_changed_at'] > Timestamp(put_timestamp))

    def put_item(self, broker, timestamp):
        broker.put_test('test', timestamp)

    def delete_item(self, broker, timestamp):
        broker.delete_test('test', timestamp)

    def test_merge_timestamps_delete_with_objects(self):
        put_timestamp = next(self.ts)
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        created_at = broker.get_info()['created_at']
        broker.merge_timestamps(created_at, put_timestamp, '0')
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], '0')
        self.assertEqual(info['status_changed_at'], put_timestamp)
        # add object
        self.put_item(broker, next(self.ts))
        self.assertEqual(broker.get_info()[
            '%s_count' % broker.db_contains_type], 1)
        # delete
        delete_timestamp = next(self.ts)
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assertFalse(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        # status is unchanged
        self.assertEqual(info['status_changed_at'], put_timestamp)
        # count is causing status to hold on
        self.delete_item(broker, next(self.ts))
        self.assertEqual(broker.get_info()[
            '%s_count' % broker.db_contains_type], 0)
        self.assert_(broker.is_deleted())

    def test_merge_timestamps_simple_recreate(self):
        put_timestamp = next(self.ts)
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        virgin_status_changed_at = broker.get_info()['status_changed_at']
        created_at = broker.get_info()['created_at']
        delete_timestamp = next(self.ts)
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assert_(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        orig_status_changed_at = info['status_changed_at']
        self.assert_(orig_status_changed_at >
                     Timestamp(virgin_status_changed_at))
        # recreate
        recreate_timestamp = next(self.ts)
        status_changed_at = time.time()
        with patch('swift.common.db.time.time', new=lambda: status_changed_at):
            broker.merge_timestamps(created_at, recreate_timestamp, '0')
        self.assertFalse(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], recreate_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assert_(info['status_changed_at'], status_changed_at)

    def test_merge_timestamps_recreate_with_objects(self):
        put_timestamp = next(self.ts)
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        created_at = broker.get_info()['created_at']
        # delete
        delete_timestamp = next(self.ts)
        broker.merge_timestamps(created_at, put_timestamp, delete_timestamp)
        self.assert_(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], put_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        orig_status_changed_at = info['status_changed_at']
        self.assert_(Timestamp(orig_status_changed_at) >=
                     Timestamp(put_timestamp))
        # add object
        self.put_item(broker, next(self.ts))
        count_key = '%s_count' % broker.db_contains_type
        self.assertEqual(broker.get_info()[count_key], 1)
        self.assertFalse(broker.is_deleted())
        # recreate
        recreate_timestamp = next(self.ts)
        broker.merge_timestamps(created_at, recreate_timestamp, '0')
        self.assertFalse(broker.is_deleted())
        info = broker.get_info()
        self.assertEqual(info['created_at'], created_at)
        self.assertEqual(info['put_timestamp'], recreate_timestamp)
        self.assertEqual(info['delete_timestamp'], delete_timestamp)
        self.assertEqual(info['status_changed_at'], orig_status_changed_at)
        # count is not causing status to hold on
        self.delete_item(broker, next(self.ts))
        self.assertFalse(broker.is_deleted())

    def test_merge_timestamps_update_put_no_status_change(self):
        put_timestamp = next(self.ts)
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        info = broker.get_info()
        orig_status_changed_at = info['status_changed_at']
        created_at = info['created_at']
        new_put_timestamp = next(self.ts)
        broker.merge_timestamps(created_at, new_put_timestamp, '0')
        info = broker.get_info()
        self.assertEqual(new_put_timestamp, info['put_timestamp'])
        self.assertEqual(orig_status_changed_at, info['status_changed_at'])

    def test_merge_timestamps_update_delete_no_status_change(self):
        put_timestamp = next(self.ts)
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(put_timestamp, storage_policy_index=int(self.policy))
        created_at = broker.get_info()['created_at']
        broker.merge_timestamps(created_at, put_timestamp, next(self.ts))
        orig_status_changed_at = broker.get_info()['status_changed_at']
        new_delete_timestamp = next(self.ts)
        broker.merge_timestamps(created_at, put_timestamp,
                                new_delete_timestamp)
        info = broker.get_info()
        self.assertEqual(new_delete_timestamp, info['delete_timestamp'])
        self.assertEqual(orig_status_changed_at, info['status_changed_at'])

    def test_get_max_row(self):
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(next(self.ts), storage_policy_index=int(self.policy))
        self.assertEquals(-1, broker.get_max_row())
        self.put_item(broker, next(self.ts))
        self.assertEquals(1, broker.get_max_row())
        self.delete_item(broker, next(self.ts))
        self.assertEquals(2, broker.get_max_row())
        self.put_item(broker, next(self.ts))
        self.assertEquals(3, broker.get_max_row())

    def test_get_info(self):
        broker = self.broker_class(':memory:', account='test', container='c')
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
        broker = self.broker_class(':memory:', account='test', container='c')
        broker.initialize(Timestamp(0).internal,
                          storage_policy_index=int(self.policy))
        self.assertEqual(broker.metadata, {})
        self.assertEqual(broker.get_raw_metadata(), '')
        key = u'test\u062a'.encode('utf-8')
        value = u'value\u062a'
        metadata = {
            key: [value, Timestamp(1).internal]
        }
        broker.update_metadata(metadata)
        self.assertEqual(broker.metadata, metadata)
        self.assertEqual(broker.get_raw_metadata(),
                         json.dumps(metadata))

    def test_put_timestamp(self):
        broker = self.broker_class(':memory:', account='a', container='c')
        orig_put_timestamp = next(self.ts)
        broker.initialize(orig_put_timestamp,
                          storage_policy_index=int(self.policy))
        self.assertEqual(broker.get_info()['put_timestamp'],
                         orig_put_timestamp)
        # put_timestamp equal - no change
        broker.update_put_timestamp(orig_put_timestamp)
        self.assertEqual(broker.get_info()['put_timestamp'],
                         orig_put_timestamp)
        # put_timestamp newer - gets newer
        newer_put_timestamp = next(self.ts)
        broker.update_put_timestamp(newer_put_timestamp)
        self.assertEqual(broker.get_info()['put_timestamp'],
                         newer_put_timestamp)
        # put_timestamp older - no change
        broker.update_put_timestamp(orig_put_timestamp)
        self.assertEqual(broker.get_info()['put_timestamp'],
                         newer_put_timestamp)

    def test_status_changed_at(self):
        broker = self.broker_class(':memory:', account='test', container='c')
        put_timestamp = next(self.ts)
        created_at = time.time()
        with patch('swift.common.db.time.time', new=lambda: created_at):
            broker.initialize(put_timestamp,
                              storage_policy_index=int(self.policy))
        self.assertEquals(broker.get_info()['status_changed_at'],
                          put_timestamp)
        self.assertEquals(broker.get_info()['created_at'],
                          Timestamp(created_at).internal)
        status_changed_at = next(self.ts)
        broker.update_status_changed_at(status_changed_at)
        self.assertEqual(broker.get_info()['status_changed_at'],
                         status_changed_at)
        # save the old and get a new status_changed_at
        old_status_changed_at, status_changed_at = \
            status_changed_at, next(self.ts)
        broker.update_status_changed_at(status_changed_at)
        self.assertEqual(broker.get_info()['status_changed_at'],
                         status_changed_at)
        # status changed at won't go backwards...
        broker.update_status_changed_at(old_status_changed_at)
        self.assertEqual(broker.get_info()['status_changed_at'],
                         status_changed_at)

    def test_get_syncs(self):
        broker = self.broker_class(':memory:', account='a', container='c')
        broker.initialize(Timestamp(time.time()).internal,
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

    @with_tempdir
    def test_commit_pending(self, tempdir):
        broker = self.broker_class(os.path.join(tempdir, 'test.db'),
                                   account='a', container='c')
        broker.initialize(next(self.ts),
                          storage_policy_index=int(self.policy))
        self.put_item(broker, next(self.ts))
        qry = 'select * from %s_stat' % broker.db_type
        with broker.get() as conn:
            rows = [dict(x) for x in conn.execute(qry)]
        info = rows[0]
        count_key = '%s_count' % broker.db_contains_type
        self.assertEqual(0, info[count_key])
        broker.get_info()
        self.assertEqual(1, broker.get_info()[count_key])


class TestDatabaseBroker(unittest.TestCase):

    def setUp(self):
        self.testdir = mkdtemp()

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_DB_PREALLOCATION_setting(self):
        u = uuid4().hex
        b = DatabaseBroker(u)
        swift.common.db.DB_PREALLOCATION = False
        b._preallocate()
        swift.common.db.DB_PREALLOCATION = True
        self.assertRaises(OSError, b._preallocate)

    def test_memory_db_init(self):
        broker = DatabaseBroker(':memory:')
        self.assertEqual(broker.db_file, ':memory:')
        self.assertRaises(AttributeError, broker.initialize,
                          normalize_timestamp('0'))

    def test_disk_db_init(self):
        db_file = os.path.join(self.testdir, '1.db')
        broker = DatabaseBroker(db_file)
        self.assertEqual(broker.db_file, db_file)
        self.assert_(broker.conn is None)

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
            self.assertEquals(test_size[0], 1024 * 1024)

    def test_initialize(self):
        self.assertRaises(AttributeError,
                          DatabaseBroker(':memory:').initialize,
                          normalize_timestamp('1'))
        stub_dict = {}

        def stub(*args, **kwargs):
            for key in stub_dict.keys():
                del stub_dict[key]
            stub_dict['args'] = args
            for key, value in kwargs.items():
                stub_dict[key] = value
        broker = DatabaseBroker(':memory:')
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        self.assert_(hasattr(stub_dict['args'][0], 'execute'))
        self.assertEquals(stub_dict['args'][1], '0000000001.00000')
        with broker.get() as conn:
            conn.execute('SELECT * FROM outgoing_sync')
            conn.execute('SELECT * FROM incoming_sync')
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        self.assert_(hasattr(stub_dict['args'][0], 'execute'))
        self.assertEquals(stub_dict['args'][1], '0000000001.00000')
        with broker.get() as conn:
            conn.execute('SELECT * FROM outgoing_sync')
            conn.execute('SELECT * FROM incoming_sync')
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        broker._initialize = stub
        self.assertRaises(DatabaseAlreadyExists,
                          broker.initialize, normalize_timestamp('1'))

    def test_delete_db(self):
        def init_stub(conn, put_timestamp, **kwargs):
            conn.execute('CREATE TABLE test (one TEXT)')
            conn.execute('CREATE TABLE test_stat (id TEXT)')
            conn.execute('INSERT INTO test_stat (id) VALUES (?)',
                         (str(uuid4),))
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()
        stub_called = [False]

        def delete_stub(*a, **kw):
            stub_called[0] = True
        broker = DatabaseBroker(':memory:')
        broker.db_type = 'test'
        broker._initialize = init_stub
        # Initializes a good broker for us
        broker.initialize(normalize_timestamp('1'))
        self.assert_(broker.conn is not None)
        broker._delete_db = delete_stub
        stub_called[0] = False
        broker.delete_db('2')
        self.assert_(stub_called[0])
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        broker.db_type = 'test'
        broker._initialize = init_stub
        broker.initialize(normalize_timestamp('1'))
        broker._delete_db = delete_stub
        stub_called[0] = False
        broker.delete_db('2')
        self.assert_(stub_called[0])
        # ensure that metadata was cleared
        m2 = broker.metadata
        self.assert_(not any(v[0] for v in m2.itervalues()))
        self.assert_(all(v[1] == normalize_timestamp('2')
                         for v in m2.itervalues()))

    def test_get(self):
        broker = DatabaseBroker(':memory:')
        got_exc = False
        try:
            with broker.get() as conn:
                conn.execute('SELECT 1')
        except Exception:
            got_exc = True
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        got_exc = False
        try:
            with broker.get() as conn:
                conn.execute('SELECT 1')
        except Exception:
            got_exc = True
        self.assert_(got_exc)

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
            self.assertEquals(
                [r[0] for r in conn.execute('SELECT * FROM test')], [])
        with broker.get() as conn:
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
        with broker.get() as conn:
            self.assertEquals(
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
            exc = None
            try:
                with broker.get() as conn:
                    conn.execute('SELECT * FROM test')
            except Exception as err:
                exc = err
            self.assertEquals(
                str(exc),
                'Quarantined %s to %s due to malformed database' %
                (dbpath, qpath))
            # Test corrupted database
            copy(os.path.join(os.path.dirname(__file__),
                              'corrupted_example.db'),
                 os.path.join(dbpath, '1.db'))
            broker = DatabaseBroker(os.path.join(dbpath, '1.db'))
            broker.db_type = 'test'
            exc = None
            try:
                with broker.get() as conn:
                    conn.execute('SELECT * FROM test')
            except Exception as err:
                exc = err
            self.assertEquals(
                str(exc),
                'Quarantined %s to %s due to corrupted database' %
                (dbpath, qpath))

    def test_lock(self):
        broker = DatabaseBroker(os.path.join(self.testdir, '1.db'), timeout=.1)
        got_exc = False
        try:
            with broker.lock():
                pass
        except Exception:
            got_exc = True
        self.assert_(got_exc)

        def stub(*args, **kwargs):
            pass
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        with broker.lock():
            pass
        with broker.lock():
            pass
        broker2 = DatabaseBroker(os.path.join(self.testdir, '1.db'),
                                 timeout=.1)
        broker2._initialize = stub
        with broker.lock():
            got_exc = False
            try:
                with broker2.lock():
                    pass
            except LockTimeout:
                got_exc = True
            self.assert_(got_exc)
        try:
            with broker.lock():
                raise Exception('test')
        except Exception:
            pass
        with broker.lock():
            pass

    def test_newid(self):
        broker = DatabaseBroker(':memory:')
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
            self.assertEquals(len(uuids), 1)
            self.assertNotEquals(uuids[0], uuid1)
            uuid1 = uuids[0]
            points = [(r[0], r[1]) for r in conn.execute(
                'SELECT sync_point, '
                'remote_id FROM incoming_sync WHERE remote_id = ?', (uuid2,))]
            self.assertEquals(len(points), 1)
            self.assertEquals(points[0][0], -1)
            self.assertEquals(points[0][1], uuid2)
            conn.execute('INSERT INTO test (one) VALUES ("1")')
            conn.commit()
        uuid3 = str(uuid4())
        broker.newid(uuid3)
        with broker.get() as conn:
            uuids = [r[0] for r in conn.execute('SELECT * FROM test_stat')]
            self.assertEquals(len(uuids), 1)
            self.assertNotEquals(uuids[0], uuid1)
            uuid1 = uuids[0]
            points = [(r[0], r[1]) for r in conn.execute(
                'SELECT sync_point, '
                'remote_id FROM incoming_sync WHERE remote_id = ?', (uuid3,))]
            self.assertEquals(len(points), 1)
            self.assertEquals(points[0][1], uuid3)
        broker.newid(uuid2)
        with broker.get() as conn:
            uuids = [r[0] for r in conn.execute('SELECT * FROM test_stat')]
            self.assertEquals(len(uuids), 1)
            self.assertNotEquals(uuids[0], uuid1)
            points = [(r[0], r[1]) for r in conn.execute(
                'SELECT sync_point, '
                'remote_id FROM incoming_sync WHERE remote_id = ?', (uuid2,))]
            self.assertEquals(len(points), 1)
            self.assertEquals(points[0][1], uuid2)

    def test_get_items_since(self):
        broker = DatabaseBroker(':memory:')
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
        self.assertEquals(broker.get_items_since(-1, 10),
                          [{'one': '1'}, {'one': '2'}, {'one': '3'}])
        self.assertEquals(broker.get_items_since(-1, 2),
                          [{'one': '1'}, {'one': '2'}])
        self.assertEquals(broker.get_items_since(1, 2),
                          [{'one': '2'}, {'one': '3'}])
        self.assertEquals(broker.get_items_since(3, 2), [])
        self.assertEquals(broker.get_items_since(999, 2), [])

    def test_get_sync(self):
        broker = DatabaseBroker(':memory:')
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
        self.assertEquals(broker.get_sync(uuid2), -1)
        broker.newid(uuid2)
        self.assertEquals(broker.get_sync(uuid2), 1)
        uuid3 = str(uuid4())
        self.assertEquals(broker.get_sync(uuid3), -1)
        with broker.get() as conn:
            conn.execute('INSERT INTO test (one) VALUES ("2")')
            conn.commit()
        broker.newid(uuid3)
        self.assertEquals(broker.get_sync(uuid2), 1)
        self.assertEquals(broker.get_sync(uuid3), 2)
        self.assertEquals(broker.get_sync(uuid2, incoming=False), -1)
        self.assertEquals(broker.get_sync(uuid3, incoming=False), -1)
        broker.merge_syncs([{'sync_point': 1, 'remote_id': uuid2}],
                           incoming=False)
        self.assertEquals(broker.get_sync(uuid2), 1)
        self.assertEquals(broker.get_sync(uuid3), 2)
        self.assertEquals(broker.get_sync(uuid2, incoming=False), 1)
        self.assertEquals(broker.get_sync(uuid3, incoming=False), -1)
        broker.merge_syncs([{'sync_point': 2, 'remote_id': uuid3}],
                           incoming=False)
        self.assertEquals(broker.get_sync(uuid2, incoming=False), 1)
        self.assertEquals(broker.get_sync(uuid3, incoming=False), 2)

    def test_merge_syncs(self):
        broker = DatabaseBroker(':memory:')

        def stub(*args, **kwargs):
            pass
        broker._initialize = stub
        broker.initialize(normalize_timestamp('1'))
        uuid2 = str(uuid4())
        broker.merge_syncs([{'sync_point': 1, 'remote_id': uuid2}])
        self.assertEquals(broker.get_sync(uuid2), 1)
        uuid3 = str(uuid4())
        broker.merge_syncs([{'sync_point': 2, 'remote_id': uuid3}])
        self.assertEquals(broker.get_sync(uuid2), 1)
        self.assertEquals(broker.get_sync(uuid3), 2)
        self.assertEquals(broker.get_sync(uuid2, incoming=False), -1)
        self.assertEquals(broker.get_sync(uuid3, incoming=False), -1)
        broker.merge_syncs([{'sync_point': 3, 'remote_id': uuid2},
                            {'sync_point': 4, 'remote_id': uuid3}],
                           incoming=False)
        self.assertEquals(broker.get_sync(uuid2, incoming=False), 3)
        self.assertEquals(broker.get_sync(uuid3, incoming=False), 4)
        self.assertEquals(broker.get_sync(uuid2), 1)
        self.assertEquals(broker.get_sync(uuid3), 2)
        broker.merge_syncs([{'sync_point': 5, 'remote_id': uuid2}])
        self.assertEquals(broker.get_sync(uuid2), 5)

    def test_get_replication_info(self):
        self.get_replication_info_tester(metadata=False)

    def test_get_replication_info_with_metadata(self):
        self.get_replication_info_tester(metadata=True)

    def get_replication_info_tester(self, metadata=False):
        broker = DatabaseBroker(':memory:', account='a')
        broker.db_type = 'test'
        broker.db_contains_type = 'test'
        broker_creation = normalize_timestamp(1)
        broker_uuid = str(uuid4())
        broker_metadata = metadata and simplejson.dumps(
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
        self.assertEquals(info, {
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
        self.assertEquals(info, {
            'account': broker.account, 'count': 1,
            'hash': 'bdc4c93f574b0d8c2911a27ce9dd38ba',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'status_changed_at': put_timestamp,
            'max_row': 1, 'id': broker_uuid, 'metadata': broker_metadata})
        with broker.get() as conn:
            conn.execute('DELETE FROM test')
            conn.commit()
        info = broker.get_replication_info()
        self.assertEquals(info, {
            'account': broker.account, 'count': 0,
            'hash': '00000000000000000000000000000000',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'status_changed_at': put_timestamp,
            'max_row': 1, 'id': broker_uuid, 'metadata': broker_metadata})
        return broker

    def test_metadata(self):
        def reclaim(broker, timestamp):
            with broker.get() as conn:
                broker._reclaim(conn, timestamp)
                conn.commit()
        # Initializes a good broker for us
        broker = self.get_replication_info_tester(metadata=True)
        # Add our first item
        first_timestamp = normalize_timestamp(1)
        first_value = '1'
        broker.update_metadata({'First': [first_value, first_timestamp]})
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        # Add our second item
        second_timestamp = normalize_timestamp(2)
        second_value = '2'
        broker.update_metadata({'Second': [second_value, second_timestamp]})
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' in broker.metadata)
        self.assertEquals(broker.metadata['Second'],
                          [second_value, second_timestamp])
        # Update our first item
        first_timestamp = normalize_timestamp(3)
        first_value = '1b'
        broker.update_metadata({'First': [first_value, first_timestamp]})
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' in broker.metadata)
        self.assertEquals(broker.metadata['Second'],
                          [second_value, second_timestamp])
        # Delete our second item (by setting to empty string)
        second_timestamp = normalize_timestamp(4)
        second_value = ''
        broker.update_metadata({'Second': [second_value, second_timestamp]})
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' in broker.metadata)
        self.assertEquals(broker.metadata['Second'],
                          [second_value, second_timestamp])
        # Reclaim at point before second item was deleted
        reclaim(broker, normalize_timestamp(3))
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' in broker.metadata)
        self.assertEquals(broker.metadata['Second'],
                          [second_value, second_timestamp])
        # Reclaim at point second item was deleted
        reclaim(broker, normalize_timestamp(4))
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' in broker.metadata)
        self.assertEquals(broker.metadata['Second'],
                          [second_value, second_timestamp])
        # Reclaim after point second item was deleted
        reclaim(broker, normalize_timestamp(5))
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' not in broker.metadata)

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
        for c in xrange(MAX_META_COUNT):
            key = 'X-Account-Meta-F{0}'.format(c)
            metadata[key] = ('B', normalize_timestamp(1))
        key = 'X-Account-Meta-Foo'.format(c)
        metadata[key] = ('', normalize_timestamp(1))
        try:
            DatabaseBroker.validate_metadata(metadata)
        except HTTPException:
            self.fail('Unexpected HTTPException')

    def test_metadata_raises_exception_over_max_count(self):
        metadata = {}
        for c in xrange(MAX_META_COUNT + 1):
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
        try:
            DatabaseBroker.validate_metadata(metadata)
        except HTTPException:
            self.fail('Unexpected HTTPException')

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


if __name__ == '__main__':
    unittest.main()
