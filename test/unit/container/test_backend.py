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

from __future__ import with_statement
import hashlib
import unittest
from time import sleep, time
from uuid import uuid4

from swift.container.backend import ContainerBroker
from swift.common.utils import normalize_timestamp


class TestContainerBroker(unittest.TestCase):
    """Tests for ContainerBroker"""

    def test_creation(self):
        # Test ContainerBroker.__init__
        broker = ContainerBroker(':memory:', account='a', container='c')
        self.assertEqual(broker.db_file, ':memory:')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            curs = conn.cursor()
            curs.execute('SELECT 1')
            self.assertEqual(curs.fetchall()[0][0], 1)

    def test_exception(self):
        # Test ContainerBroker throwing a conn away after
        # unhandled exception
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
        # Test ContainerBroker.empty
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
        broker.reclaim(normalize_timestamp(time()), time())
        broker.delete_db(normalize_timestamp(time()))

    def test_delete_object(self):
        # Test ContainerBroker.delete_object
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
        # Test ContainerBroker.put_object
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

    def test_get_info(self):
        # Test ContainerBroker.get_info
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
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
        broker = ContainerBroker(':memory:', account='test1',
                                 container='test2')
        broker.initialize(normalize_timestamp('1'))

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
        # Test ContainerBroker.list_objects_iter
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

        broker.put_object('3/0049/', normalize_timestamp(time()), 0,
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
        broker.initialize(normalize_timestamp('1'))
        for obj1 in xrange(4):
            for obj2 in xrange(125):
                broker.put_object('%d:%04d' % (obj1, obj2),
                                  normalize_timestamp(time()), 0, 'text/plain',
                                  'd41d8cd98f00b204e9800998ecf8427e')
        for obj in xrange(125):
            broker.put_object('2:0051:%04d' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
                              'd41d8cd98f00b204e9800998ecf8427e')

        for obj in xrange(125):
            broker.put_object('3:%04d:0049' % obj,
                              normalize_timestamp(time()), 0, 'text/plain',
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

        broker.put_object('3:0049:', normalize_timestamp(time()), 0,
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
        broker.initialize(normalize_timestamp('1'))

        broker.put_object(
            '/pets/dogs/1', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/dogs/2', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/fish/a', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/fish/b', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/pets/fish_info.txt', normalize_timestamp(0), 0,
            'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object(
            '/snakes', normalize_timestamp(0), 0,
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
        broker.put_object('a/0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0/1/0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1/', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1/0', normalize_timestamp(time()), 0,
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
        broker.initialize(normalize_timestamp('1'))
        broker.put_object('a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a:a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:a:b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b:a', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b:b', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('c', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('a:0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:00', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('0:1:0', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1:', normalize_timestamp(time()), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('1:0', normalize_timestamp(time()), 0,
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
        broker.initialize(normalize_timestamp('1'))
        broker.put_object('a', normalize_timestamp(1), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        broker.put_object('b', normalize_timestamp(2), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hasha = hashlib.md5('%s-%s' % ('a', '0000000001.00000')).digest()
        hashb = hashlib.md5('%s-%s' % ('b', '0000000002.00000')).digest()
        hashc = ''.join(
            ('%2x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)
        broker.put_object('b', normalize_timestamp(3), 0,
                          'text/plain', 'd41d8cd98f00b204e9800998ecf8427e')
        hashb = hashlib.md5('%s-%s' % ('b', '0000000003.00000')).digest()
        hashc = ''.join(
            ('%02x' % (ord(a) ^ ord(b)) for a, b in zip(hasha, hashb)))
        self.assertEquals(broker.get_info()['hash'], hashc)

    def test_newid(self):
        # test DatabaseBroker.newid
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        id = broker.get_info()['id']
        broker.newid('someid')
        self.assertNotEquals(id, broker.get_info()['id'])

    def test_get_items_since(self):
        # test DatabaseBroker.get_items_since
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
        # exercise the DatabaseBroker sync functions a bit
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
        # test DatabaseBroker.merge_items
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
        # test DatabaseBroker.merge_items
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
    Copied from ContainerBroker before the metadata column was
    added; used for testing with TestContainerBrokerBeforeMetadata.

    Create the container_stat table which is specifc to the container DB.

    :param conn: DB connection object
    :param put_timestamp: put timestamp
    """
    if put_timestamp is None:
        put_timestamp = normalize_timestamp(0)
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
    ''', (self.account, self.container, normalize_timestamp(time()),
          str(uuid4()), put_timestamp))


class TestContainerBrokerBeforeMetadata(TestContainerBroker):
    """
    Tests for ContainerBroker against databases created before
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
            except BaseException as err:
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
    Copied from ContainerBroker before the
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
    Tests for ContainerBroker against databases created
    before the x_container_sync_point[12] columns were added.
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
            except BaseException as err:
                exc = err
        self.assert_('no such column: x_container_sync_point1' in str(exc))

    def tearDown(self):
        ContainerBroker.create_container_stat_table = \
            self._imported_create_container_stat_table
        broker = ContainerBroker(':memory:', account='a', container='c')
        broker.initialize(normalize_timestamp('1'))
        with broker.get() as conn:
            conn.execute('SELECT x_container_sync_point1 FROM container_stat')
