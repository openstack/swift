# Copyright (c) 2010-2012 OpenStack, LLC.
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

""" Tests for swift.common.utils """

from __future__ import with_statement
import hashlib
import logging
import socket
import time
import unittest
from uuid import uuid4

from swift.common import memcached


class NullLoggingHandler(logging.Handler):

    def emit(self, record):
        pass


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

class MockMemcached(object):
    def __init__(self):
        self.inbuf = ''
        self.outbuf = ''
        self.cache = {}
        self.down = False
        self.exc_on_delete = False
        self.read_return_none = False

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
                self.inbuf = self.inbuf[int(parts[4])+2:]
                if len(parts) < 6 or parts[5] != 'noreply':
                    self.outbuf += 'STORED\r\n'
            elif parts[0].lower() == 'add':
                value = self.inbuf[:int(parts[4])]
                self.inbuf = self.inbuf[int(parts[4])+2:]
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
                        self.outbuf += 'VALUE %s %s %s\r\n' % (key, val[0], len(val[2]))
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
            return response+'\n'
    def read(self, size):
        if self.down:
            raise Exception('mock is down')
        if len(self.outbuf) >= size:
            response = self.outbuf[:size]
            self.outbuf = self.outbuf[size:]
            return response

class TestMemcached(unittest.TestCase):
    """ Tests for swift.common.memcached"""

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
        memcache_client._client_cache['1.2.3.4:11211'] = [(mock, mock)] * 2
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client.set('some_key', [4, 5, 6])
        self.assertEquals(memcache_client.get('some_key'), [4, 5, 6])
        self.assert_(float(mock.cache.values()[0][1]) == 0)
        esttimeout = time.time() + 10
        memcache_client.set('some_key', [1, 2, 3], timeout=10)
        self.assert_(-1 <= float(mock.cache.values()[0][1]) - esttimeout <= 1)

    def test_incr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = [(mock, mock)] * 2
        memcache_client.incr('some_key', delta=5)
        self.assertEquals(memcache_client.get('some_key'), '5')
        memcache_client.incr('some_key', delta=5)
        self.assertEquals(memcache_client.get('some_key'), '10')
        memcache_client.incr('some_key', delta=1)
        self.assertEquals(memcache_client.get('some_key'), '11')
        memcache_client.incr('some_key', delta=-5)
        self.assertEquals(memcache_client.get('some_key'), '6')
        memcache_client.incr('some_key', delta=-15)
        self.assertEquals(memcache_client.get('some_key'), '0')
        mock.read_return_none = True
        self.assertRaises(memcached.MemcacheConnectionError,
                          memcache_client.incr, 'some_key', delta=-15)

    def test_decr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = [(mock, mock)] * 2
        memcache_client.decr('some_key', delta=5)
        self.assertEquals(memcache_client.get('some_key'), '0')
        memcache_client.incr('some_key', delta=15)
        self.assertEquals(memcache_client.get('some_key'), '15')
        memcache_client.decr('some_key', delta=4)
        self.assertEquals(memcache_client.get('some_key'), '11')
        memcache_client.decr('some_key', delta=15)
        self.assertEquals(memcache_client.get('some_key'), '0')
        mock.read_return_none = True
        self.assertRaises(memcached.MemcacheConnectionError,
                          memcache_client.decr, 'some_key', delta=15)


    def test_retry(self):
        logging.getLogger().addHandler(NullLoggingHandler())
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211', '1.2.3.5:11211'])
        mock1 = ExplodingMockMemcached()
        mock2 = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = [(mock2, mock2)]
        memcache_client._client_cache['1.2.3.5:11211'] = [(mock1, mock1)]
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEquals(mock1.exploded, True)

    def test_delete(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = [(mock, mock)] * 2
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEquals(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client.delete('some_key')
        self.assertEquals(memcache_client.get('some_key'), None)

    def test_multi(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = [(mock, mock)] * 2
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key')
        self.assertEquals(
            memcache_client.get_multi(('some_key2', 'some_key1'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3]])
        esttimeout = time.time() + 10
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
            timeout=10)
        self.assert_(-1 <= float(mock.cache.values()[0][1]) - esttimeout <= 1)
        self.assert_(-1 <= float(mock.cache.values()[1][1]) - esttimeout <= 1)
        self.assertEquals(memcache_client.get_multi(('some_key2', 'some_key1',
            'not_exists'), 'multi_key'), [[4, 5, 6], [1, 2, 3], None])


if __name__ == '__main__':
    unittest.main()

