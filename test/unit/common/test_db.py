# Copyright (c) 2010-2011 OpenStack, LLC.
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

""" Tests for swift.common.db """

from __future__ import with_statement
import hashlib
import os
import unittest
from shutil import rmtree, copy
from StringIO import StringIO
from time import sleep, time
from uuid import uuid4

import simplejson
import sqlite3

import swift.common.db
from swift.common.db import AccountBroker, chexor, ContainerBroker, \
    DatabaseBroker, DatabaseConnectionError, dict_factory, get_db_connection
from swift.common.utils import normalize_timestamp
from swift.common.exceptions import LockTimeout


class TestDatabaseConnectionError(unittest.TestCase):

    def test_str(self):
        err = \
            DatabaseConnectionError(':memory:', 'No valid database connection')
        self.assert_(':memory:' in str(err))
        self.assert_('No valid database connection' in str(err))
        err = DatabaseConnectionError(':memory:',
                'No valid database connection', timeout=1357)
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
        self.assertEquals(chexor('d41d8cd98f00b204e9800998ecf8427e',
            'new name', normalize_timestamp(1)),
            '4f2ea31ac14d4273fe32ba08062b21de')

    def test_invalid_old_hash(self):
        self.assertRaises(TypeError, chexor, 'oldhash', 'name',
                          normalize_timestamp(1))

    def test_no_name(self):
        self.assertRaises(Exception, chexor,
            'd41d8cd98f00b204e9800998ecf8427e', None, normalize_timestamp(1))


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
        orig_renamer = swift.common.db.renamer
        try:
            swift.common.db.renamer = lambda a, b: b
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
            except Exception, err:
                exc = err
            self.assertEquals(str(exc),
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
            except Exception, err:
                exc = err
            self.assertEquals(str(exc),
                'Quarantined %s to %s due to corrupted database' %
                (self.testdir, qpath))
        finally:
            swift.common.db.renamer = orig_renamer

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
        broker2 = DatabaseBroker(os.path.join(self.testdir, '1.db'), timeout=.1)
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
            points = [(r[0], r[1]) for r in conn.execute('SELECT sync_point, '
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
            points = [(r[0], r[1]) for r in conn.execute('SELECT sync_point, '
                'remote_id FROM incoming_sync WHERE remote_id = ?', (uuid3,))]
            self.assertEquals(len(points), 1)
            self.assertEquals(points[0][1], uuid3)
        broker.newid(uuid2)
        with broker.get() as conn:
            uuids = [r[0] for r in conn.execute('SELECT * FROM test_stat')]
            self.assertEquals(len(uuids), 1)
            self.assertNotEquals(uuids[0], uuid1)
            points = [(r[0], r[1]) for r in conn.execute('SELECT sync_point, '
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
        self.assertEquals(info, {'count': 0,
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
        self.assertEquals(info, {'count': 1,
            'hash': 'bdc4c93f574b0d8c2911a27ce9dd38ba',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'max_row': 1, 'id': broker_uuid,
            'metadata': broker_metadata})
        with broker.get() as conn:
            conn.execute('DELETE FROM test')
            conn.commit()
        info = broker.get_replication_info()
        self.assertEquals(info, {'count': 0,
            'hash': '00000000000000000000000000000000',
            'created_at': broker_creation, 'put_timestamp': put_timestamp,
            'delete_timestamp': '0', 'max_row': 1, 'id': broker_uuid,
            'metadata': broker_metadata})
        return broker

    def test_metadata(self):
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
        broker.reclaim(normalize_timestamp(3))
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' in broker.metadata)
        self.assertEquals(broker.metadata['Second'],
                          [second_value, second_timestamp])
        # Reclaim at point second item was deleted
        broker.reclaim(normalize_timestamp(4))
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' in broker.metadata)
        self.assertEquals(broker.metadata['Second'],
                          [second_value, second_timestamp])
        # Reclaim after point second item was deleted
        broker.reclaim(normalize_timestamp(5))
        self.assert_('First' in broker.metadata)
        self.assertEquals(broker.metadata['First'],
                          [first_value, first_timestamp])
        self.assert_('Second' not in broker.metadata)


class TestContainerBroker(unittest.TestCase):
    """ Tests for swift.common.db.ContainerBroker """

    def test_creation(self):
        """ Test swift.common.db.ContainerBroker.__init__ """
        broker = ContainerBroker(':memory:', account='a', container='c')
        self.assertEqual(broker.db_file, ':memory:')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)

    def test_exception(self):
        """ Test swift.common.db.ContainerBroker throwing a conn away after
            unhandled exception """
        first_conn = None
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
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
        """ Test swift.common.db.ContainerBroker.empty """
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        self.assert_(broker.empty())
        broker.put_object('o', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        self.assert_(not broker.empty())
        sleep(.00001)
        broker.delete_object('o', normalize_timestamp(time()))
        self.assert_(broker.empty())

    def test_reclaim(self):
        broker = ContainerBroker(':memory:', account='test_account',
                                 container='test_container')
        broker.initialize(normalize_timestamp('1'))
        broker.put_object('o', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', normalize_timestamp(time()))
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(normalize_timestamp(time()), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test the return values of reclaim()
        broker.put_object('w', normalize_timestamp(time()), 0, 'text/plain',
                'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('x', normalize_timestamp(time()), 0, 'text/plain',
                'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('y', normalize_timestamp(time()), 0, 'text/plain',
                'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('z', normalize_timestamp(time()), 0, 'text/plain',
                'd41d8cd98f00b204e9800998ecf8427e')
        # Test before deletion
        res = broker.reclaim(normalize_timestamp(time()), time())
        broker.delete_db(normalize_timestamp(time()))

        
    def test_delete_object(self):
        """ Test swift.common.db.ContainerBroker.delete_object """
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        broker.put_object('o', normalize_timestamp(time()), 0, 'text/plain',
                          'd41d8cd98f00b204e9800998ecf8427e')
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.delete_object('o', normalize_timestamp(time()))
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM object "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_put_object(self):
        """ Test swift.common.db.ContainerBroker.put_object """
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))

        # Create initial object
        timestamp = normalize_timestamp(time())
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
        timestamp = normalize_timestamp(time())
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
        otimestamp = normalize_timestamp(float(timestamp) - 1)
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
        dtimestamp = normalize_timestamp(float(timestamp) - 1)
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
        timestamp = normalize_timestamp(time())
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
        timestamp = normalize_timestamp(time())
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
        in_between_timestamp = normalize_timestamp(time())

        # New post event
        sleep(.0001)
        previous_timestamp = timestamp
        timestamp = normalize_timestamp(time())
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
        timestamp =  in_between_timestamp
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

    def test_get_info(self):
        """ Test swift.common.db.ContainerBroker.get_info """
        broker = ContainerBroker(':memory:', account='test1', container='test2')
        broker.initialize(normalize_timestamp('1'))

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['container'], 'test2')
        self.assertEquals(info['hash'], '00000000000000000000000000000000')

        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)

        broker.put_object('o1', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 123)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 246)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', normalize_timestamp(time()))
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 1000)

        sleep(.00001)
        broker.delete_object('o2', normalize_timestamp(time()))
        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)

        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

    def test_set_x_syncs(self):
        broker = ContainerBroker(':memory:', account='test1', container='test2')
        broker.initialize(normalize_timestamp('1'))

        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], -1)
        self.assertEquals(info['x_container_sync_point2'], -1)

        broker.set_x_container_sync_points(1, 2)
        info = broker.get_info()
        self.assertEquals(info['x_container_sync_point1'], 1)
        self.assertEquals(info['x_container_sync_point2'], 2)

    def test_get_report_info(self):
        broker = ContainerBroker(':memory:', account='test1', container='test2')
        broker.initialize(normalize_timestamp('1'))

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['container'], 'test2')
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        broker.put_object('o1', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 123)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 123, 'text/plain',
                          '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 246)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        sleep(.00001)
        broker.put_object('o2', normalize_timestamp(time()), 1000,
                          'text/plain', '5af83e3196bf99f440f31f2e1a6c9afe')
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)
        self.assertEquals(info['reported_object_count'], 0)
        self.assertEquals(info['reported_bytes_used'], 0)

        put_timestamp = normalize_timestamp(time())
        sleep(.001)
        delete_timestamp = normalize_timestamp(time())
        broker.reported(put_timestamp, delete_timestamp, 2, 1123)
        info = broker.get_info()
        self.assertEquals(info['object_count'], 2)
        self.assertEquals(info['bytes_used'], 1123)
        self.assertEquals(info['reported_put_timestamp'], put_timestamp)
        self.assertEquals(info['reported_delete_timestamp'], delete_timestamp)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o1', normalize_timestamp(time()))
        info = broker.get_info()
        self.assertEquals(info['object_count'], 1)
        self.assertEquals(info['bytes_used'], 1000)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

        sleep(.00001)
        broker.delete_object('o2', normalize_timestamp(time()))
        info = broker.get_info()
        self.assertEquals(info['object_count'], 0)
        self.assertEquals(info['bytes_used'], 0)
        self.assertEquals(info['reported_object_count'], 2)
        self.assertEquals(info['reported_bytes_used'], 1123)

    def test_list_objects_iter(self):
        """ Test swift.common.db.ContainerBroker.list_objects_iter """
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        for obj1 in xrange(4):
            for obj2 in xrange(125):
                broker.put_object('%d/%04d' % (obj1, obj2),
                                  normalize_timestamp(time()), 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e')
        for obj in xrange(125):
            broker.put_object('2/0051/%04d' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        for obj in xrange(125):
            broker.put_object('3/%04d/0049' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
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

        listing = broker.list_objects_iter(10, '', None, '', '/')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
                          ['0/', '1/', '2/', '3/'])

        listing = broker.list_objects_iter(10, '2', None, None, '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['2/', '3/'])

        listing = broker.list_objects_iter(10, '2/',None,  None, '/')
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

        broker.put_object('3/0049/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(10, '3/0048', None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
            ['3/0048/0049', '3/0049', '3/0049/',
            '3/0049/0049', '3/0050', '3/0050/0049', '3/0051', '3/0051/0049',
            '3/0052', '3/0052/0049'])

        listing = broker.list_objects_iter(10, '3/0048', None, '3/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
            ['3/0048/', '3/0049', '3/0049/', '3/0050',
            '3/0050/', '3/0051', '3/0051/', '3/0052', '3/0052/', '3/0053'])

        listing = broker.list_objects_iter(10, None, None, '3/0049/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing],
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

    def test_list_objects_iter_prefix_delim(self):
        """ Test swift.common.db.ContainerBroker.list_objects_iter """
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))

        broker.put_object('/pets/dogs/1', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('/pets/dogs/2', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('/pets/fish/a', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('/pets/fish/b', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('/pets/fish_info.txt', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('/snakes', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')

        #def list_objects_iter(self, limit, marker, prefix, delimiter, path=None,
        #                          format=None):
        listing = broker.list_objects_iter(100, None, None, '/pets/f', '/')
        self.assertEquals([row[0] for row in listing], ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish', '/')
        self.assertEquals([row[0] for row in listing], ['/pets/fish/', '/pets/fish_info.txt'])
        listing = broker.list_objects_iter(100, None, None, '/pets/fish/', '/')
        self.assertEquals([row[0] for row in listing], ['/pets/fish/a', '/pets/fish/b'])

    def test_double_check_trailing_delimiter(self):
        """ Test swift.common.db.ContainerBroker.list_objects_iter for a
            container that has an odd file with a trailing delimiter """
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        broker.put_object('a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a/a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/a/b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a/b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b/a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b/b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('c', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        listing = broker.list_objects_iter(15, None, None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
            ['a', 'a/', 'a/a', 'a/a/a', 'a/a/b', 'a/b', 'b', 'b/a', 'b/b', 'c'])
        listing = broker.list_objects_iter(15, None, None, '', '/')
        self.assertEquals(len(listing), 5)
        self.assertEquals([row[0] for row in listing],
            ['a', 'a/', 'b', 'b/', 'c'])
        listing = broker.list_objects_iter(15, None, None, 'a/', '/')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
            ['a/', 'a/a', 'a/a/', 'a/b'])
        listing = broker.list_objects_iter(15, None, None, 'b/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['b/a', 'b/b'])

    def test_chexor(self):
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        broker.put_object('a', normalize_timestamp(1), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hasha = hashlib.md5('%s-%s' % ('a', '0000000001.00000')).digest()
        hashb = hashlib.md5('%s-%s' % ('b', '0000000002.00000')).digest()
        hashc = ''.join(('%2x' % (ord(a)^ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)
        broker.put_object('b', normalize_timestamp(3), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hashb = hashlib.md5('%s-%s' % ('b', '0000000003.00000')).digest()
        hashc = ''.join(('%02x' % (ord(a)^ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)

    def test_newid(self):
        """test DatabaseBroker.newid"""
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        id = broker.get_info()['id']
        broker.newid('someid')
        self.assertNotEquals(id, broker.get_info()['id'])

    def test_get_items_since(self):
        """test DatabaseBroker.get_items_since"""
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        broker.put_object('a', normalize_timestamp(1), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        max_row = broker.get_replication_info()['max_row']
        broker.put_object('b', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        items = broker.get_items_since(max_row, 1000)
        self.assertEquals(len(items), 1)
        self.assertEquals(items[0]['name'], 'b')

    def test_sync_merging(self):
        """ exercise the DatabaseBroker sync functions a bit """
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'))
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'))
        self.assertEquals(broker2.get_sync('12345'), -1)
        broker1.merge_syncs([{'sync_point': 3, 'remote_id': '12345'}])
        broker2.merge_syncs(broker1.get_syncs())
        self.assertEquals(broker2.get_sync('12345'), 3)

    def test_merge_items(self):
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'))
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'))
        broker1.put_object('a', normalize_timestamp(1), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker1.put_object('b', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        id = broker1.get_info()['id']
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 2)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_object('c', normalize_timestamp(3), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 3)
        self.assertEquals(['a', 'b', 'c'],
                          sorted([rec['name'] for rec in items]))

    def test_merge_items_overwrite(self):
        """test DatabaseBroker.merge_items"""
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'))
        id = broker1.get_info()['id']
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'))
        broker1.put_object('a', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker1.put_object('b', normalize_timestamp(3), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        broker1.put_object('a', normalize_timestamp(4), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(3))

    def test_merge_items_post_overwrite_out_of_order(self):
        """test DatabaseBroker.merge_items"""
        broker1 = ContainerBroker(':memory:', account='a', container='c')
        broker1.initialize(normalize_timestamp('1'))
        id = broker1.get_info()['id']
        broker2 = ContainerBroker(':memory:', account='a', container='c')
        broker2.initialize(normalize_timestamp('1'))
        broker1.put_object('a', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker1.put_object('b', normalize_timestamp(3), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        broker1.put_object('a', normalize_timestamp(4), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(3))
                self.assertEquals(rec['content_type'], 'text/plain')
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(3))
        broker1.put_object('b', normalize_timestamp(5), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        for rec in items:
            if rec['name'] == 'a':
                self.assertEquals(rec['created_at'], normalize_timestamp(4))
            if rec['name'] == 'b':
                self.assertEquals(rec['created_at'], normalize_timestamp(5))
                self.assertEquals(rec['content_type'], 'text/plain')


def premetadata_create_container_stat_table(self, conn, put_timestamp=None):
    """
    Copied from swift.common.db.ContainerBroker before the metadata column was
    added; used for testing with TestContainerBrokerBeforeMetadata.

    Create the container_stat table which is specifc to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = normalize_timestamp(0)
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
            status_changed_at TEXT DEFAULT '0'
        );

        INSERT INTO container_stat (object_count, bytes_used)
            VALUES (0, 0);
    """)
    conn.execute('''
        UPDATE container_stat
        SET account = ?, container = ?, created_at = ?, id = ?,
            put_timestamp = ?
    ''', (self.account, self.container, normalize_timestamp(time()),
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeMetadata(TestContainerBroker):
    """
    Tests for swift.common.db.ContainerBroker against databases created before
    the metadata column was added.
    """

    def setUp(self):
        self._imported_create_container_stat_table = \
            ContainerBroker.create_container_stat_table
        ContainerBroker.create_container_stat_table = \
            premetadata_create_container_stat_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT metadata FROM container_stat')
            except BaseException, err:
                exc = err
        self.assert_('no such column: metadata' in str(exc))

    def tearDown(self):
        ContainerBroker.create_container_stat_table = \
            self._imported_create_container_stat_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('SELECT metadata FROM container_stat')


def prexsync_create_container_stat_table(self, conn, put_timestamp=None):
    """
    Copied from swift.common.db.ContainerBroker before the
    x_container_sync_point[12] columns were added; used for testing with
    TestContainerBrokerBeforeXSync.

    Create the container_stat table which is specifc to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = normalize_timestamp(0)
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
    ''', (self.account, self.container, normalize_timestamp(time()),
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeXSync(TestContainerBroker):
    """
    Tests for swift.common.db.ContainerBroker against databases created before
    the x_container_sync_point[12] columns were added.
    """

    def setUp(self):
        self._imported_create_container_stat_table = \
            ContainerBroker.create_container_stat_table
        ContainerBroker.create_container_stat_table = \
            prexsync_create_container_stat_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('''SELECT x_container_sync_point1
                                FROM container_stat''')
            except BaseException, err:
                exc = err
        self.assert_('no such column: x_container_sync_point1' in str(exc))

    def tearDown(self):
        ContainerBroker.create_container_stat_table = \
            self._imported_create_container_stat_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('SELECT x_container_sync_point1 FROM container_stat')


class TestAccountBroker(unittest.TestCase):
    """ Tests for swift.common.db.AccountBroker """

    def test_creation(self):
        """ Test swift.common.db.AccountBroker.__init__ """
        broker = AccountBroker(':memory:', account='a')
        self.assertEqual(broker.db_file, ':memory:')
        got_exc = False
        try:
            with broker.get() as conn:
                pass
        except Exception:
            got_exc = True
        self.assert_(got_exc)
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)

    def test_exception(self):
        """ Test swift.common.db.AccountBroker throwing a conn away after
            exception """
        first_conn = None
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
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
        """ Test swift.common.db.AccountBroker.empty """
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        self.assert_(broker.empty())
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0)
        self.assert_(not broker.empty())
        sleep(.00001)
        broker.put_container('o', 0, normalize_timestamp(time()), 0, 0)
        self.assert_(broker.empty())

    def test_reclaim(self):
        broker = AccountBroker(':memory:', account='test_account')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('c', normalize_timestamp(time()), 0, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('c', 0, normalize_timestamp(time()), 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(normalize_timestamp(time()), time())
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test reclaim after deletion. Create 3 test containers
        broker.put_container('x', 0, 0, 0, 0)
        broker.put_container('y', 0, 0, 0, 0)
        broker.put_container('z', 0, 0, 0, 0)
        res = broker.reclaim(normalize_timestamp(time()), time())
        # self.assertEquals(len(res), 2)
        # self.assert_(isinstance(res, tuple))
        # containers, account_name = res
        # self.assert_(containers is None)
        # self.assert_(account_name is None)
        # Now delete the account
        broker.delete_db(normalize_timestamp(time()))
        res = broker.reclaim(normalize_timestamp(time()), time())
        # self.assertEquals(len(res), 2)
        # self.assert_(isinstance(res, tuple))
        # containers, account_name = res
        # self.assertEquals(account_name, 'test_account')
        # self.assertEquals(len(containers), 3)
        # self.assert_('x' in containers)
        # self.assert_('y' in containers)
        # self.assert_('z' in containers)
        # self.assert_('a' not in containers)


    def test_delete_container(self):
        """ Test swift.common.db.AccountBroker.delete_container """
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('o', 0, normalize_timestamp(time()), 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEquals(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_get_container_timestamp(self):
        """ Test swift.common.db.AccountBroker.get_container_timestamp """
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))

        # Create initial container
        timestamp = normalize_timestamp(time())
        broker.put_container('container_name', timestamp, 0, 0, 0)
        # test extant map
        ts = broker.get_container_timestamp('container_name')
        self.assertEquals(ts, timestamp)
        # test missing map
        ts = broker.get_container_timestamp('something else')
        self.assertEquals(ts, None)

    def test_put_container(self):
        """ Test swift.common.db.AccountBroker.put_container """
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))

        # Create initial container
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Reput same event
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put new event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put old event
        otimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_container('"{<container \'&\' name>}"', otimestamp, 0, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put old delete event
        dtimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_container('"{<container \'&\' name>}"', 0, dtimestamp, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT delete_timestamp FROM container").fetchone()[0],
                dtimestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

        # Put new delete event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', 0, timestamp, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT delete_timestamp FROM container").fetchone()[0],
                timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 1)

        # Put new event
        sleep(.00001)
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
        with broker.get() as conn:
            self.assertEquals(conn.execute(
                "SELECT name FROM container").fetchone()[0],
                '"{<container \'&\' name>}"')
            self.assertEquals(conn.execute(
                "SELECT put_timestamp FROM container").fetchone()[0], timestamp)
            self.assertEquals(conn.execute(
                "SELECT deleted FROM container").fetchone()[0], 0)

    def test_get_info(self):
        """ Test swift.common.db.AccountBroker.get_info """
        broker = AccountBroker(':memory:', account='test1')
        broker.initialize(normalize_timestamp('1'))

        info = broker.get_info()
        self.assertEquals(info['account'], 'test1')
        self.assertEquals(info['hash'], '00000000000000000000000000000000')

        info = broker.get_info()
        self.assertEquals(info['container_count'], 0)

        broker.put_container('c1', normalize_timestamp(time()), 0, 0, 0)
        info = broker.get_info()
        self.assertEquals(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', normalize_timestamp(time()), 0, 0, 0)
        info = broker.get_info()
        self.assertEquals(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c2', normalize_timestamp(time()), 0, 0, 0)
        info = broker.get_info()
        self.assertEquals(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c1', 0, normalize_timestamp(time()), 0, 0)
        info = broker.get_info()
        self.assertEquals(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', 0, normalize_timestamp(time()), 0, 0)
        info = broker.get_info()
        self.assertEquals(info['container_count'], 0)

    def test_list_containers_iter(self):
        """ Test swift.common.db.AccountBroker.list_containers_iter """
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        for cont1 in xrange(4):
            for cont2 in xrange(125):
                broker.put_container('%d/%04d' % (cont1, cont2),
                                     normalize_timestamp(time()), 0, 0, 0)
        for cont in xrange(125):
            broker.put_container('2/0051/%04d' % cont,
                                 normalize_timestamp(time()), 0, 0, 0)

        for cont in xrange(125):
            broker.put_container('3/%04d/0049' % cont,
                                 normalize_timestamp(time()), 0, 0, 0)

        listing = broker.list_containers_iter(100, '', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0099')

        listing = broker.list_containers_iter(100, '', '0/0050', None, '')
        self.assertEquals(len(listing), 51)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0050')

        listing = broker.list_containers_iter(100, '0/0099', None, None, '')
        self.assertEquals(len(listing), 100)
        self.assertEquals(listing[0][0], '0/0100')
        self.assertEquals(listing[-1][0], '1/0074')

        listing = broker.list_containers_iter(55, '1/0074', None, None, '')
        self.assertEquals(len(listing), 55)
        self.assertEquals(listing[0][0], '1/0075')
        self.assertEquals(listing[-1][0], '2/0004')

        listing = broker.list_containers_iter(10, '', None, '0/01', '')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0100')
        self.assertEquals(listing[-1][0], '0/0109')

        listing = broker.list_containers_iter(10, '', None, '0/01', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0100')
        self.assertEquals(listing[-1][0], '0/0109')

        listing = broker.list_containers_iter(10, '', None, '0/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '0/0000')
        self.assertEquals(listing[-1][0], '0/0009')

        listing = broker.list_containers_iter(10, '', None, '', '/')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
                          ['0/', '1/', '2/', '3/'])

        listing = broker.list_containers_iter(10, '2/', None, None, '/')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['3/'])

        listing = broker.list_containers_iter(10, '', None, '2', '/')
        self.assertEquals(len(listing), 1)
        self.assertEquals([row[0] for row in listing], ['2/'])

        listing = broker.list_containers_iter(10, '2/0050', None, '2/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals(listing[0][0], '2/0051')
        self.assertEquals(listing[1][0], '2/0051/')
        self.assertEquals(listing[2][0], '2/0052')
        self.assertEquals(listing[-1][0], '2/0059')

        listing = broker.list_containers_iter(10, '3/0045', None, '3/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                           ['3/0045/', '3/0046', '3/0046/', '3/0047',
                            '3/0047/', '3/0048', '3/0048/', '3/0049',
                            '3/0049/', '3/0050'])

        broker.put_container('3/0049/', normalize_timestamp(time()), 0, 0, 0)
        listing = broker.list_containers_iter(10, '3/0048', None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                           ['3/0048/0049', '3/0049', '3/0049/', '3/0049/0049',
                            '3/0050', '3/0050/0049', '3/0051', '3/0051/0049',
                            '3/0052', '3/0052/0049'])

        listing = broker.list_containers_iter(10, '3/0048', None, '3/', '/')
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                           ['3/0048/', '3/0049', '3/0049/', '3/0050',
                            '3/0050/', '3/0051', '3/0051/', '3/0052',
                            '3/0052/', '3/0053'])

        listing = broker.list_containers_iter(10, None, None, '3/0049/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing],
                          ['3/0049/', '3/0049/0049'])

    def test_double_check_trailing_delimiter(self):
        """ Test swift.common.db.AccountBroker.list_containers_iter for an
            account that has an odd file with a trailing delimiter """
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a/', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a/a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a/a/a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a/a/b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a/b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('b/a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('b/b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('c', normalize_timestamp(time()), 0, 0, 0)
        listing = broker.list_containers_iter(15, None, None, None, None)
        self.assertEquals(len(listing), 10)
        self.assertEquals([row[0] for row in listing],
                           ['a', 'a/', 'a/a', 'a/a/a', 'a/a/b', 'a/b', 'b',
                            'b/a', 'b/b', 'c'])
        listing = broker.list_containers_iter(15, None, None, '', '/')
        self.assertEquals(len(listing), 5)
        self.assertEquals([row[0] for row in listing],
                          ['a', 'a/', 'b', 'b/', 'c'])
        listing = broker.list_containers_iter(15, None, None, 'a/', '/')
        self.assertEquals(len(listing), 4)
        self.assertEquals([row[0] for row in listing],
                          ['a/', 'a/a', 'a/a/', 'a/b'])
        listing = broker.list_containers_iter(15, None, None, 'b/', '/')
        self.assertEquals(len(listing), 2)
        self.assertEquals([row[0] for row in listing], ['b/a', 'b/b'])

    def test_chexor(self):
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('a', normalize_timestamp(1),
                             normalize_timestamp(0), 0, 0)
        broker.put_container('b', normalize_timestamp(2),
                             normalize_timestamp(0), 0, 0)
        hasha = hashlib.md5('%s-%s' %
            ('a', '0000000001.00000-0000000000.00000-0-0')
        ).digest()
        hashb = hashlib.md5('%s-%s' %
            ('b', '0000000002.00000-0000000000.00000-0-0')
        ).digest()
        hashc = \
            ''.join(('%02x' % (ord(a)^ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)
        broker.put_container('b', normalize_timestamp(3),
                             normalize_timestamp(0), 0, 0)
        hashb = hashlib.md5('%s-%s' %
            ('b', '0000000003.00000-0000000000.00000-0-0')
        ).digest()
        hashc = \
            ''.join(('%02x' % (ord(a)^ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)

    def test_merge_items(self):
        broker1 = AccountBroker(':memory:', account='a')
        broker1.initialize(normalize_timestamp('1'))
        broker2 = AccountBroker(':memory:', account='a')
        broker2.initialize(normalize_timestamp('1'))
        broker1.put_container('a', normalize_timestamp(1), 0, 0, 0)
        broker1.put_container('b', normalize_timestamp(2), 0, 0, 0)
        id = broker1.get_info()['id']
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 2)
        self.assertEquals(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_container('c', normalize_timestamp(3), 0, 0, 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEquals(len(items), 3)
        self.assertEquals(['a', 'b', 'c'],
                          sorted([rec['name'] for rec in items]))


def premetadata_create_account_stat_table(self, conn, put_timestamp):
    """
    Copied from swift.common.db.AccountBroker before the metadata column was
    added; used for testing with TestAccountBrokerBeforeMetadata.

    Create account_stat table which is specific to the account DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
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
            status_changed_at TEXT DEFAULT '0'
        );

        INSERT INTO account_stat (container_count) VALUES (0);
    """)

    conn.execute('''
        UPDATE account_stat SET account = ?, created_at = ?, id = ?,
               put_timestamp = ?
        ''', (self.account, normalize_timestamp(time()), str(uuid4()),
        put_timestamp))


class TestAccountBrokerBeforeMetadata(TestAccountBroker):
    """
    Tests for swift.common.db.AccountBroker against databases created before
    the metadata column was added.
    """

    def setUp(self):
        self._imported_create_account_stat_table = \
            AccountBroker.create_account_stat_table
        AccountBroker.create_account_stat_table = \
            premetadata_create_account_stat_table
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        exc = None
        with broker.get() as conn:
            try:
                conn.execute('SELECT metadata FROM account_stat')
            except BaseException, err:
                exc = err
        self.assert_('no such column: metadata' in str(exc))

    def tearDown(self):
        AccountBroker.create_account_stat_table = \
            self._imported_create_account_stat_table
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('SELECT metadata FROM account_stat')


if __name__ == '__main__':
    unittest.main()
