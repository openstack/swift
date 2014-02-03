# -*- coding:utf-8 -*-
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

"""Tests for swift.common.utils"""

from collections import defaultdict
import logging
import socket
import time
import unittest
from uuid import uuid4

from eventlet import GreenPool, sleep, Queue
from eventlet.pools import Pool

from swift.common import memcached
from mock import patch, MagicMock
from test.unit import NullLoggingHandler


class MockedMemcachePool(memcached.MemcacheConnPool):
    def __init__(self, mocks):
        Pool.__init__(self, max_size=2)
        self.mocks = mocks
        # setting this for the eventlet workaround in the MemcacheConnPool
        self._parent_class_getter = super(memcached.MemcacheConnPool, self).get

    def create(self):
        return self.mocks.pop(0)


class ExplodingMockMemcached(object):
    exploded = False

    def sendall(self, string):
        self.exploded = True
        raise socket.error()

    def readline(self):
        self.exploded = True
        raise socket.error()

    def read(self, size):
        self.exploded = True
        raise socket.error()

    def close(self):
        pass


class MockMemcached(object):

    def __init__(self):
        self.inbuf = ''
        self.outbuf = ''
        self.cache = {}
        self.down = False
        self.exc_on_delete = False
        self.read_return_none = False
        self.close_called = False

    def sendall(self, string):
        if self.down:
            raise Exception('mock is down')
        self.inbuf += string
        while '\n' in self.inbuf:
            cmd, self.inbuf = self.inbuf.split('\n', 1)
            parts = cmd.split()
            if parts[0].lower() == 'set':
                self.cache[parts[1]] = parts[2], parts[3], \
                    self.inbuf[:int(parts[4])]
                self.inbuf = self.inbuf[int(parts[4]) + 2:]
                if len(parts) < 6 or parts[5] != 'noreply':
                    self.outbuf += 'STORED\r\n'
            elif parts[0].lower() == 'add':
                value = self.inbuf[:int(parts[4])]
                self.inbuf = self.inbuf[int(parts[4]) + 2:]
                if parts[1] in self.cache:
                    if len(parts) < 6 or parts[5] != 'noreply':
                        self.outbuf += 'NOT_STORED\r\n'
                else:
                    self.cache[parts[1]] = parts[2], parts[3], value
                    if len(parts) < 6 or parts[5] != 'noreply':
                        self.outbuf += 'STORED\r\n'
            elif parts[0].lower() == 'delete':
                if self.exc_on_delete:
                    raise Exception('mock is has exc_on_delete set')
                if parts[1] in self.cache:
                    del self.cache[parts[1]]
                    if 'noreply' not in parts:
                        self.outbuf += 'DELETED\r\n'
                elif 'noreply' not in parts:
                    self.outbuf += 'NOT_FOUND\r\n'
            elif parts[0].lower() == 'get':
                for key in parts[1:]:
                    if key in self.cache:
                        val = self.cache[key]
                        self.outbuf += 'VALUE %s %s %s\r\n' % (
                            key, val[0], len(val[2]))
                        self.outbuf += val[2] + '\r\n'
                self.outbuf += 'END\r\n'
            elif parts[0].lower() == 'incr':
                if parts[1] in self.cache:
                    val = list(self.cache[parts[1]])
                    val[2] = str(int(val[2]) + int(parts[2]))
                    self.cache[parts[1]] = val
                    self.outbuf += str(val[2]) + '\r\n'
                else:
                    self.outbuf += 'NOT_FOUND\r\n'
            elif parts[0].lower() == 'decr':
                if parts[1] in self.cache:
                    val = list(self.cache[parts[1]])
                    if int(val[2]) - int(parts[2]) > 0:
                        val[2] = str(int(val[2]) - int(parts[2]))
                    else:
                        val[2] = '0'
                    self.cache[parts[1]] = val
                    self.outbuf += str(val[2]) + '\r\n'
                else:
                    self.outbuf += 'NOT_FOUND\r\n'

    def readline(self):
        if self.read_return_none:
            return None
        if self.down:
            raise Exception('mock is down')
        if '\n' in self.outbuf:
            response, self.outbuf = self.outbuf.split('\n', 1)
            return response + '\n'

    def read(self, size):
        if self.down:
            raise Exception('mock is down')
        if len(self.outbuf) >= size:
            response = self.outbuf[:size]
            self.outbuf = self.outbuf[size:]
            return response

    def close(self):
        self.close_called = True
        pass


class TestMemcached(unittest.TestCase):
    """Tests for swift.common.memcached"""

    def test_get_conns(self):
        sock1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock1.bind(('127.0.0.1', 0))
        sock1.listen(1)
        sock1ipport = '%s:%s' % sock1.getsockname()
        sock2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock2.bind(('127.0.0.1', 0))
        sock2.listen(1)
        orig_port = memcached.DEFAULT_MEMCACHED_PORT
        try:
            sock2ip, memcached.DEFAULT_MEMCACHED_PORT = sock2.getsockname()
            sock2ipport = '%s:%s' % (sock2ip, memcached.DEFAULT_MEMCACHED_PORT)
            # We're deliberately using sock2ip (no port) here to test that the
            # default port is used.
            memcache_client = memcached.MemcacheRing([sock1ipport, sock2ip])
            one = two = True
            while one or two:  # Run until we match hosts one and two
                key = uuid4().hex
                for conn in memcache_client._get_conns(key):
                    peeripport = '%s:%s' % conn[2].getpeername()
                    self.assert_(peeripport in (sock1ipport, sock2ipport))
                    if peeripport == sock1ipport:
                        one = False
                    if peeripport == sock2ipport:
                        two = False
        finally:
            memcached.DEFAULT_MEMCACHED_PORT = orig_port

    def test_set_get(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEquals(mock.cache.values()[0][1], '0')
        memcache_client.set('some_key', [4, 5, 6])
        self.assertEquals(memcache_client.get('some_key'), [4, 5, 6])
        memcache_client.set('some_key', ['simple str', 'utf8 str éà'])
        # As per http://wiki.openstack.org/encoding,
        # we should expect to have unicode
        self.assertEquals(
            memcache_client.get('some_key'), ['simple str', u'utf8 str éà'])
        self.assert_(float(mock.cache.values()[0][1]) == 0)
        memcache_client.set('some_key', [1, 2, 3], timeout=10)
        self.assertEquals(mock.cache.values()[0][1], '10')
        memcache_client.set('some_key', [1, 2, 3], time=20)
        self.assertEquals(mock.cache.values()[0][1], '20')

        sixtydays = 60 * 24 * 60 * 60
        esttimeout = time.time() + sixtydays
        memcache_client.set('some_key', [1, 2, 3], timeout=sixtydays)
        self.assert_(-1 <= float(mock.cache.values()[0][1]) - esttimeout <= 1)
        memcache_client.set('some_key', [1, 2, 3], time=sixtydays)
        self.assert_(-1 <= float(mock.cache.values()[0][1]) - esttimeout <= 1)

    def test_incr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        self.assertEquals(memcache_client.incr('some_key', delta=5), 5)
        self.assertEquals(memcache_client.get('some_key'), '5')
        self.assertEquals(memcache_client.incr('some_key', delta=5), 10)
        self.assertEquals(memcache_client.get('some_key'), '10')
        self.assertEquals(memcache_client.incr('some_key', delta=1), 11)
        self.assertEquals(memcache_client.get('some_key'), '11')
        self.assertEquals(memcache_client.incr('some_key', delta=-5), 6)
        self.assertEquals(memcache_client.get('some_key'), '6')
        self.assertEquals(memcache_client.incr('some_key', delta=-15), 0)
        self.assertEquals(memcache_client.get('some_key'), '0')
        mock.read_return_none = True
        self.assertRaises(memcached.MemcacheConnectionError,
                          memcache_client.incr, 'some_key', delta=-15)
        self.assertTrue(mock.close_called)

    def test_incr_w_timeout(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.incr('some_key', delta=5, time=55)
        self.assertEquals(memcache_client.get('some_key'), '5')
        self.assertEquals(mock.cache.values()[0][1], '55')
        memcache_client.delete('some_key')
        self.assertEquals(memcache_client.get('some_key'), None)
        fiftydays = 50 * 24 * 60 * 60
        esttimeout = time.time() + fiftydays
        memcache_client.incr('some_key', delta=5, time=fiftydays)
        self.assertEquals(memcache_client.get('some_key'), '5')
        self.assert_(-1 <= float(mock.cache.values()[0][1]) - esttimeout <= 1)
        memcache_client.delete('some_key')
        self.assertEquals(memcache_client.get('some_key'), None)
        memcache_client.incr('some_key', delta=5)
        self.assertEquals(memcache_client.get('some_key'), '5')
        self.assertEquals(mock.cache.values()[0][1], '0')
        memcache_client.incr('some_key', delta=5, time=55)
        self.assertEquals(memcache_client.get('some_key'), '10')
        self.assertEquals(mock.cache.values()[0][1], '0')

    def test_decr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        self.assertEquals(memcache_client.decr('some_key', delta=5), 0)
        self.assertEquals(memcache_client.get('some_key'), '0')
        self.assertEquals(memcache_client.incr('some_key', delta=15), 15)
        self.assertEquals(memcache_client.get('some_key'), '15')
        self.assertEquals(memcache_client.decr('some_key', delta=4), 11)
        self.assertEquals(memcache_client.get('some_key'), '11')
        self.assertEquals(memcache_client.decr('some_key', delta=15), 0)
        self.assertEquals(memcache_client.get('some_key'), '0')
        mock.read_return_none = True
        self.assertRaises(memcached.MemcacheConnectionError,
                          memcache_client.decr, 'some_key', delta=15)

    def test_retry(self):
        logging.getLogger().addHandler(NullLoggingHandler())
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211', '1.2.3.5:11211'])
        mock1 = ExplodingMockMemcached()
        mock2 = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock2, mock2)])
        memcache_client._client_cache['1.2.3.5:11211'] = MockedMemcachePool(
            [(mock1, mock1)])
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEquals(mock1.exploded, True)

    def test_delete(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client.delete('some_key')
        self.assertEquals(memcache_client.get('some_key'), None)

    def test_multi(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key')
        self.assertEquals(
            memcache_client.get_multi(('some_key2', 'some_key1'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3]])
        self.assertEquals(mock.cache.values()[0][1], '0')
        self.assertEquals(mock.cache.values()[1][1], '0')
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
            timeout=10)
        self.assertEquals(mock.cache.values()[0][1], '10')
        self.assertEquals(mock.cache.values()[1][1], '10')
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
            time=20)
        self.assertEquals(mock.cache.values()[0][1], '20')
        self.assertEquals(mock.cache.values()[1][1], '20')

        fortydays = 50 * 24 * 60 * 60
        esttimeout = time.time() + fortydays
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
            timeout=fortydays)
        self.assert_(-1 <= float(mock.cache.values()[0][1]) - esttimeout <= 1)
        self.assert_(-1 <= float(mock.cache.values()[1][1]) - esttimeout <= 1)
        self.assertEquals(memcache_client.get_multi(
            ('some_key2', 'some_key1', 'not_exists'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3], None])

    def test_serialization(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 allow_pickle=True)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_pickle = False
        memcache_client._allow_unpickle = True
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_unpickle = False
        self.assertEquals(memcache_client.get('some_key'), None)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_unpickle = True
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_pickle = True
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])

    def test_connection_pooling(self):
        with patch('swift.common.memcached.socket') as mock_module:
            # patch socket, stub socket.socket, mock sock
            mock_sock = mock_module.socket.return_value

            # track clients waiting for connections
            connected = []
            connections = Queue()
            errors = []

            def wait_connect(addr):
                connected.append(addr)
                sleep(0.1)  # yield
                val = connections.get()
                if val is not None:
                    errors.append(val)

            mock_sock.connect = wait_connect

            memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                     connect_timeout=10)
            # sanity
            self.assertEquals(1, len(memcache_client._client_cache))
            for server, pool in memcache_client._client_cache.items():
                self.assertEqual(2, pool.max_size)

            # make 10 requests "at the same time"
            p = GreenPool()
            for i in range(10):
                p.spawn(memcache_client.set, 'key', 'value')
            for i in range(3):
                sleep(0.1)
                self.assertEqual(2, len(connected))

            # give out a connection
            connections.put(None)

            # at this point, only one connection should have actually been
            # created, the other is in the creation step, and the rest of the
            # clients are not attempting to connect. we let this play out a
            # bit to verify.
            for i in range(3):
                sleep(0.1)
                self.assertEqual(2, len(connected))

            # finish up, this allows the final connection to be created, so
            # that all the other clients can use the two existing connections
            # and no others will be created.
            connections.put(None)
            connections.put('nono')
            self.assertEqual(2, len(connected))
            p.waitall()
            self.assertEqual(2, len(connected))
            self.assertEqual(0, len(errors),
                             "A client was allowed a third connection")
            connections.get_nowait()
            self.assertTrue(connections.empty())

    # Ensure we exercise the backported-for-pre-eventlet-version-0.9.17 get()
    # code, even if the executing eventlet's version is already newer.
    @patch.object(memcached, 'eventlet_version', '0.9.16')
    def test_connection_pooling_pre_0_9_17(self):
        with patch('swift.common.memcached.socket') as mock_module:
            connected = []
            count = [0]

            def _slow_yielding_connector(addr):
                count[0] += 1
                if count[0] % 3 == 0:
                    raise ValueError('whoops!')
                sleep(0.1)
                connected.append(addr)

            mock_module.socket.return_value.connect.side_effect = \
                _slow_yielding_connector

            # If POOL_SIZE is not small enough relative to USER_COUNT, the
            # "free_items" business in the eventlet.pools.Pool will cause
            # spurious failures below.  I found these values to work well on a
            # VM running in VirtualBox on a late 2013 Retina MacbookPro:
            POOL_SIZE = 5
            USER_COUNT = 50

            pool = memcached.MemcacheConnPool('1.2.3.4:11211', size=POOL_SIZE,
                                              connect_timeout=10)
            self.assertEqual(POOL_SIZE, pool.max_size)

            def _user():
                got = None
                while not got:
                    try:
                        got = pool.get()
                    except:  # noqa
                        pass
                pool.put(got)

            # make a bunch of requests "at the same time"
            p = GreenPool()
            for i in range(USER_COUNT):
                p.spawn(_user)
            p.waitall()

            # If the except block after the "created = self.create()" call
            # doesn't correctly decrement self.current_size, this test will
            # fail by having some number less than POOL_SIZE connections (in my
            # testing, anyway).
            self.assertEqual(POOL_SIZE, len(connected))

            # Subsequent requests should get and use the existing
            # connections, not creating any more.
            for i in range(USER_COUNT):
                p.spawn(_user)
            p.waitall()

            self.assertEqual(POOL_SIZE, len(connected))

    def test_connection_pool_timeout(self):
        orig_conn_pool = memcached.MemcacheConnPool
        try:
            connections = defaultdict(Queue)
            pending = defaultdict(int)
            served = defaultdict(int)

            class MockConnectionPool(orig_conn_pool):
                def get(self):
                    pending[self.server] += 1
                    conn = connections[self.server].get()
                    pending[self.server] -= 1
                    return conn

                def put(self, *args, **kwargs):
                    connections[self.server].put(*args, **kwargs)
                    served[self.server] += 1

            memcached.MemcacheConnPool = MockConnectionPool

            memcache_client = memcached.MemcacheRing(['1.2.3.4:11211',
                                                      '1.2.3.5:11211'],
                                                     io_timeout=0.5,
                                                     pool_timeout=0.1)

            # Hand out a couple slow connections to 1.2.3.5, leaving 1.2.3.4
            # fast. All ten (10) clients should try to talk to .5 first, and
            # then move on to .4, and we'll assert all that below.
            mock_conn = MagicMock(), MagicMock()
            mock_conn[1].sendall = lambda x: sleep(0.2)
            connections['1.2.3.5:11211'].put(mock_conn)
            connections['1.2.3.5:11211'].put(mock_conn)

            mock_conn = MagicMock(), MagicMock()
            connections['1.2.3.4:11211'].put(mock_conn)
            connections['1.2.3.4:11211'].put(mock_conn)

            p = GreenPool()
            for i in range(10):
                p.spawn(memcache_client.set, 'key', 'value')

            # Wait for the dust to settle.
            p.waitall()

            self.assertEqual(pending['1.2.3.5:11211'], 8)
            self.assertEqual(len(memcache_client._errors['1.2.3.5:11211']), 8)
            self.assertEqual(served['1.2.3.5:11211'], 2)
            self.assertEqual(pending['1.2.3.4:11211'], 0)
            self.assertEqual(len(memcache_client._errors['1.2.3.4:11211']), 0)
            self.assertEqual(served['1.2.3.4:11211'], 8)

            # and we never got more put in that we gave out
            self.assertEqual(connections['1.2.3.5:11211'].qsize(), 2)
            self.assertEqual(connections['1.2.3.4:11211'].qsize(), 2)
        finally:
            memcached.MemcacheConnPool = orig_conn_pool

if __name__ == '__main__':
    unittest.main()
