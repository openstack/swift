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

from __future__ import with_statement
import os
import unittest
from shutil import rmtree, copy
from uuid import uuid4

import simplejson
import sqlite3
from mock import patch

import swift.common.db
from swift.common.db import chexor, dict_factory, get_db_connection, \
    DatabaseBroker, DatabaseConnectionError, DatabaseAlreadyExists
from swift.common.utils import normalize_timestamp
from swift.common.exceptions import LockTimeout


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


class TestGetDBConnection(unittest.TestCase):

    def test_normal_case(self):
        conn = get_db_connection(':memory:')
        self.assert_(hasattr(conn, 'execute'))

    def test_invalid_path(self):
        self.assertRaises(DatabaseConnectionError, get_db_connection,
                          'invalid database path / name')


class TestDatabaseBroker(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(os.path.dirname(__file__), 'db')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)

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

        def my_ismount(*a, **kw):
            return True

        with patch('os.path.ismount', my_ismount):
            broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
            broker._initialize = stub
            self.assertRaises(DatabaseAlreadyExists,
                              broker.initialize, normalize_timestamp('1'))

    def test_delete_db(self):
        def init_stub(conn, put_timestamp):
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
        with patch('swift.common.db.renamer', lambda a, b: b):
            qpath = os.path.dirname(os.path.dirname(os.path.dirname(
                os.path.dirname(self.testdir))))
            if qpath:
                qpath += '/quarantined/tests/db'
            else:
                qpath = 'quarantined/tests/db'
            # Test malformed database
            copy(os.path.join(os.path.dirname(__file__),
                              'malformed_example.db'),
                 os.path.join(self.testdir, '1.db'))
            broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
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
                (self.testdir, qpath))
            # Test corrupted database
            copy(os.path.join(os.path.dirname(__file__),
                              'corrupted_example.db'),
                 os.path.join(self.testdir, '1.db'))
            broker = DatabaseBroker(os.path.join(self.testdir, '1.db'))
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
                (self.testdir, qpath))

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

        def _initialize(conn, timestamp):
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

        def _initialize(conn, timestamp):
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

        def _initialize(conn, timestamp):
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

        def _initialize(conn, put_timestamp):
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
                    test_count INTEGER,
                    hash TEXT default '00000000000000000000000000000000',
                    id TEXT
                    %s
                );
                INSERT INTO test_stat (test_count) VALUES (0);
            ''' % (metadata and ", metadata TEXT DEFAULT ''" or ""))
            conn.execute('''
                UPDATE test_stat
                SET account = ?, created_at = ?,  id = ?, put_timestamp = ?
            ''', (broker.account, broker_creation, broker_uuid, put_timestamp))
            if metadata:
                conn.execute('UPDATE test_stat SET metadata = ?',
                             (broker_metadata,))
            conn.commit()
        broker._initialize = _initialize
        put_timestamp = normalize_timestamp(2)
        broker.initialize(put_timestamp)
        info = broker.get_replication_info()
        self.assertEquals(info, {
            'count': 0,
            'hash': '00000000000000000000000000000000',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'max_row': -1, 'id': broker_uuid,
            'metadata': broker_metadata})
        insert_timestamp = normalize_timestamp(3)
        with broker.get() as conn:
            conn.execute('''
                INSERT INTO test (name, created_at) VALUES ('test', ?)
            ''', (insert_timestamp,))
            conn.commit()
        info = broker.get_replication_info()
        self.assertEquals(info, {
            'count': 1,
            'hash': 'bdc4c93f574b0d8c2911a27ce9dd38ba',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'max_row': 1, 'id': broker_uuid,
            'metadata': broker_metadata})
        with broker.get() as conn:
            conn.execute('DELETE FROM test')
            conn.commit()
        info = broker.get_replication_info()
        self.assertEquals(info, {
            'count': 0,
            'hash': '00000000000000000000000000000000',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'max_row': 1, 'id': broker_uuid,
            'metadata': broker_metadata})
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


if __name__ == '__main__':
    unittest.main()
