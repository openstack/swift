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

from __future__ import with_statement
import hashlib
import unittest
from time import sleep, time
from uuid import uuid4

from swift.account.backend import AccountBroker
from swift.common.utils import normalize_timestamp


class TestAccountBroker(unittest.TestCase):
    """Tests for AccountBroker"""

    def test_creation(self):
        # Test AccountBroker.__init__
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
        # Test AccountBroker throwing a conn away after exception
        first_conn = None
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            first_conn = conn
        try:
            with broker.get() as conn:
                self.assertEqual(first_conn, conn)
                raise Exception('OMG')
        except Exception:
            pass
        self.assert_(broker.conn is None)

    def test_empty(self):
        # Test AccountBroker.empty
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
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('c', 0, normalize_timestamp(time()), 0, 0)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        broker.reclaim(normalize_timestamp(time() - 999), time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)
        sleep(.00001)
        broker.reclaim(normalize_timestamp(time()), time())
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        # Test reclaim after deletion. Create 3 test containers
        broker.put_container('x', 0, 0, 0, 0)
        broker.put_container('y', 0, 0, 0, 0)
        broker.put_container('z', 0, 0, 0, 0)
        broker.reclaim(normalize_timestamp(time()), time())
        # self.assertEqual(len(res), 2)
        # self.assert_(isinstance(res, tuple))
        # containers, account_name = res
        # self.assert_(containers is None)
        # self.assert_(account_name is None)
        # Now delete the account
        broker.delete_db(normalize_timestamp(time()))
        broker.reclaim(normalize_timestamp(time()), time())
        # self.assertEqual(len(res), 2)
        # self.assert_(isinstance(res, tuple))
        # containers, account_name = res
        # self.assertEqual(account_name, 'test_account')
        # self.assertEqual(len(containers), 3)
        # self.assert_('x' in containers)
        # self.assert_('y' in containers)
        # self.assert_('z' in containers)
        # self.assert_('a' not in containers)

    def test_delete_container(self):
        # Test AccountBroker.delete_container
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('o', normalize_timestamp(time()), 0, 0, 0)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 1)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 0)
        sleep(.00001)
        broker.put_container('o', 0, normalize_timestamp(time()), 0, 0)
        with broker.get() as conn:
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 0").fetchone()[0], 0)
            self.assertEqual(conn.execute(
                "SELECT count(*) FROM container "
                "WHERE deleted = 1").fetchone()[0], 1)

    def test_put_container(self):
        # Test AccountBroker.put_container
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))

        # Create initial container
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
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
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
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
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
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
        otimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_container('"{<container \'&\' name>}"', otimestamp, 0, 0, 0)
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
        dtimestamp = normalize_timestamp(float(timestamp) - 1)
        broker.put_container('"{<container \'&\' name>}"', 0, dtimestamp, 0, 0)
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
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', 0, timestamp, 0, 0)
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
        timestamp = normalize_timestamp(time())
        broker.put_container('"{<container \'&\' name>}"', timestamp, 0, 0, 0)
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
        broker = AccountBroker(':memory:', account='test1')
        broker.initialize(normalize_timestamp('1'))

        info = broker.get_info()
        self.assertEqual(info['account'], 'test1')
        self.assertEqual(info['hash'], '00000000000000000000000000000000')

        info = broker.get_info()
        self.assertEqual(info['container_count'], 0)

        broker.put_container('c1', normalize_timestamp(time()), 0, 0, 0)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', normalize_timestamp(time()), 0, 0, 0)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c2', normalize_timestamp(time()), 0, 0, 0)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 2)

        sleep(.00001)
        broker.put_container('c1', 0, normalize_timestamp(time()), 0, 0)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 1)

        sleep(.00001)
        broker.put_container('c2', 0, normalize_timestamp(time()), 0, 0)
        info = broker.get_info()
        self.assertEqual(info['container_count'], 0)

    def test_list_containers_iter(self):
        # Test AccountBroker.list_containers_iter
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        for cont1 in xrange(4):
            for cont2 in xrange(125):
                broker.put_container('%d-%04d' % (cont1, cont2),
                                     normalize_timestamp(time()), 0, 0, 0)
        for cont in xrange(125):
            broker.put_container('2-0051-%04d' % cont,
                                 normalize_timestamp(time()), 0, 0, 0)

        for cont in xrange(125):
            broker.put_container('3-%04d-0049' % cont,
                                 normalize_timestamp(time()), 0, 0, 0)

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

        listing = broker.list_containers_iter(10, '', None, '0-', '-')
        self.assertEqual(len(listing), 10)
        self.assertEqual(listing[0][0], '0-0000')
        self.assertEqual(listing[-1][0], '0-0009')

        listing = broker.list_containers_iter(10, '', None, '', '-')
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['0-', '1-', '2-', '3-'])

        listing = broker.list_containers_iter(10, '2-', None, None, '-')
        self.assertEqual(len(listing), 1)
        self.assertEqual([row[0] for row in listing], ['3-'])

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

        broker.put_container('3-0049-', normalize_timestamp(time()), 0, 0, 0)
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

    def test_double_check_trailing_delimiter(self):
        # Test AccountBroker.list_containers_iter for an
        # account that has an odd container with a trailing delimiter
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a-', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a-a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a-a-a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a-a-b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('a-b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('b-a', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('b-b', normalize_timestamp(time()), 0, 0, 0)
        broker.put_container('c', normalize_timestamp(time()), 0, 0, 0)
        listing = broker.list_containers_iter(15, None, None, None, None)
        self.assertEqual(len(listing), 10)
        self.assertEqual([row[0] for row in listing],
                         ['a', 'a-', 'a-a', 'a-a-a', 'a-a-b', 'a-b', 'b',
                          'b-a', 'b-b', 'c'])
        listing = broker.list_containers_iter(15, None, None, '', '-')
        self.assertEqual(len(listing), 5)
        self.assertEqual([row[0] for row in listing],
                         ['a', 'a-', 'b', 'b-', 'c'])
        listing = broker.list_containers_iter(15, None, None, 'a-', '-')
        self.assertEqual(len(listing), 4)
        self.assertEqual([row[0] for row in listing],
                         ['a-', 'a-a', 'a-a-', 'a-b'])
        listing = broker.list_containers_iter(15, None, None, 'b-', '-')
        self.assertEqual(len(listing), 2)
        self.assertEqual([row[0] for row in listing], ['b-a', 'b-b'])

    def test_chexor(self):
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        broker.put_container('a', normalize_timestamp(1),
                             normalize_timestamp(0), 0, 0)
        broker.put_container('b', normalize_timestamp(2),
                             normalize_timestamp(0), 0, 0)
        hasha = hashlib.md5(
            '%s-%s' % ('a', '0000000001.00000-0000000000.00000-0-0')
        ).digest()
        hashb = hashlib.md5(
            '%s-%s' % ('b', '0000000002.00000-0000000000.00000-0-0')
        ).digest()
        hashc = \
            ''.join(('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEqual(broker.get_info()['hash'], hashc)
        broker.put_container('b', normalize_timestamp(3),
                             normalize_timestamp(0), 0, 0)
        hashb = hashlib.md5(
            '%s-%s' % ('b', '0000000003.00000-0000000000.00000-0-0')
        ).digest()
        hashc = \
            ''.join(('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEqual(broker.get_info()['hash'], hashc)

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
        self.assertEqual(len(items), 2)
        self.assertEqual(['a', 'b'], sorted([rec['name'] for rec in items]))
        broker1.put_container('c', normalize_timestamp(3), 0, 0, 0)
        broker2.merge_items(broker1.get_items_since(
            broker2.get_sync(id), 1000), id)
        items = broker2.get_items_since(-1, 1000)
        self.assertEqual(len(items), 3)
        self.assertEqual(['a', 'b', 'c'],
                         sorted([rec['name'] for rec in items]))


def premetadata_create_account_stat_table(self, conn, put_timestamp):
    """
    Copied from AccountBroker before the metadata column was
    added; used for testing with TestAccountBrokerBeforeMetadata.

    Create account_stat table which is specific to the account DB.

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
        ''', (self.account, normalize_timestamp(time()), str(uuid4()),
              put_timestamp))


class TestAccountBrokerBeforeMetadata(TestAccountBroker):
    """
    Tests for AccountBroker against databases created before
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
            except BaseException as err:
                exc = err
        self.assert_('no such column: metadata' in str(exc))

    def tearDown(self):
        AccountBroker.create_account_stat_table = \
            self._imported_create_account_stat_table
        broker = AccountBroker(':memory:', account='a')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('SELECT metadata FROM account_stat')
