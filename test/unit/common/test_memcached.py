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
import errno
from hashlib import md5
import six
import socket
import time
import unittest
from uuid import uuid4
import os

import mock

from eventlet import GreenPool, sleep, Queue
from eventlet.pools import Pool

from swift.common import memcached
from mock import patch, MagicMock
from test.unit import debug_logger


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
        raise socket.error(errno.EPIPE, os.strerror(errno.EPIPE))

    def readline(self):
        self.exploded = True
        raise socket.error(errno.EPIPE, os.strerror(errno.EPIPE))

    def read(self, size):
        self.exploded = True
        raise socket.error(errno.EPIPE, os.strerror(errno.EPIPE))

    def close(self):
        pass


class MockMemcached(object):
    # See https://github.com/memcached/memcached/blob/master/doc/protocol.txt
    # In particular, the "Storage commands" section may be interesting.

    def __init__(self):
        self.inbuf = b''
        self.outbuf = b''
        self.cache = {}
        self.down = False
        self.exc_on_delete = False
        self.read_return_none = False
        self.read_return_empty_str = False
        self.close_called = False

    def sendall(self, string):
        if self.down:
            raise Exception('mock is down')
        self.inbuf += string
        while b'\n' in self.inbuf:
            cmd, self.inbuf = self.inbuf.split(b'\n', 1)
            parts = cmd.split()
            cmd_name = parts[0].decode('ascii').lower()
            handler = getattr(self, 'handle_%s' % cmd_name, None)
            if handler:
                handler(*parts[1:])
            else:
                raise ValueError('Unhandled command: %s' % parts[0])

    def handle_set(self, key, flags, exptime, num_bytes, noreply=b''):
        self.cache[key] = flags, exptime, self.inbuf[:int(num_bytes)]
        self.inbuf = self.inbuf[int(num_bytes) + 2:]
        if noreply != b'noreply':
            self.outbuf += b'STORED\r\n'

    def handle_add(self, key, flags, exptime, num_bytes, noreply=b''):
        value = self.inbuf[:int(num_bytes)]
        self.inbuf = self.inbuf[int(num_bytes) + 2:]
        if key in self.cache:
            if noreply != b'noreply':
                self.outbuf += b'NOT_STORED\r\n'
        else:
            self.cache[key] = flags, exptime, value
            if noreply != b'noreply':
                self.outbuf += b'STORED\r\n'

    def handle_delete(self, key, noreply=b''):
        if self.exc_on_delete:
            raise Exception('mock is has exc_on_delete set')
        if key in self.cache:
            del self.cache[key]
            if noreply != b'noreply':
                self.outbuf += b'DELETED\r\n'
        elif noreply != b'noreply':
            self.outbuf += b'NOT_FOUND\r\n'

    def handle_get(self, *keys):
        for key in keys:
            if key in self.cache:
                val = self.cache[key]
                self.outbuf += b' '.join([
                    b'VALUE',
                    key,
                    val[0],
                    str(len(val[2])).encode('ascii')
                ]) + b'\r\n'
                self.outbuf += val[2] + b'\r\n'
        self.outbuf += b'END\r\n'

    def handle_incr(self, key, value, noreply=b''):
        if key in self.cache:
            current = self.cache[key][2]
            new_val = str(int(current) + int(value)).encode('ascii')
            self.cache[key] = self.cache[key][:2] + (new_val, )
            self.outbuf += new_val + b'\r\n'
        else:
            self.outbuf += b'NOT_FOUND\r\n'

    def handle_decr(self, key, value, noreply=b''):
        if key in self.cache:
            current = self.cache[key][2]
            new_val = str(int(current) - int(value)).encode('ascii')
            if new_val[:1] == b'-':  # ie, val is negative
                new_val = b'0'
            self.cache[key] = self.cache[key][:2] + (new_val, )
            self.outbuf += new_val + b'\r\n'
        else:
            self.outbuf += b'NOT_FOUND\r\n'

    def readline(self):
        if self.read_return_empty_str:
            return b''
        if self.read_return_none:
            return None
        if self.down:
            raise Exception('mock is down')
        if b'\n' in self.outbuf:
            response, self.outbuf = self.outbuf.split(b'\n', 1)
            return response + b'\n'

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

    def setUp(self):
        self.logger = debug_logger()
        patcher = mock.patch('swift.common.memcached.logging', self.logger)
        self.addCleanup(patcher.stop)
        patcher.start()

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
                key = uuid4().hex.encode('ascii')
                for conn in memcache_client._get_conns(key):
                    peeripport = '%s:%s' % conn[2].getpeername()
                    self.assertTrue(peeripport in (sock1ipport, sock2ipport))
                    if peeripport == sock1ipport:
                        one = False
                    if peeripport == sock2ipport:
                        two = False
            self.assertEqual(len(memcache_client._errors[sock1ipport]), 0)
            self.assertEqual(len(memcache_client._errors[sock2ip]), 0)
        finally:
            memcached.DEFAULT_MEMCACHED_PORT = orig_port

    def test_get_conns_v6(self):
        if not socket.has_ipv6:
            return
        try:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            sock.bind(('::1', 0, 0, 0))
            sock.listen(1)
            sock_addr = sock.getsockname()
            server_socket = '[%s]:%s' % (sock_addr[0], sock_addr[1])
            memcache_client = memcached.MemcacheRing([server_socket])
            key = uuid4().hex.encode('ascii')
            for conn in memcache_client._get_conns(key):
                peer_sockaddr = conn[2].getpeername()
                peer_socket = '[%s]:%s' % (peer_sockaddr[0], peer_sockaddr[1])
                self.assertEqual(peer_socket, server_socket)
            self.assertEqual(len(memcache_client._errors[server_socket]), 0)
        finally:
            sock.close()

    def test_get_conns_v6_default(self):
        if not socket.has_ipv6:
            return
        try:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            sock.bind(('::1', 0))
            sock.listen(1)
            sock_addr = sock.getsockname()
            server_socket = '[%s]:%s' % (sock_addr[0], sock_addr[1])
            server_host = '[%s]' % sock_addr[0]
            memcached.DEFAULT_MEMCACHED_PORT = sock_addr[1]
            memcache_client = memcached.MemcacheRing([server_host])
            key = uuid4().hex.encode('ascii')
            for conn in memcache_client._get_conns(key):
                peer_sockaddr = conn[2].getpeername()
                peer_socket = '[%s]:%s' % (peer_sockaddr[0], peer_sockaddr[1])
                self.assertEqual(peer_socket, server_socket)
            self.assertEqual(len(memcache_client._errors[server_host]), 0)
        finally:
            sock.close()

    def test_get_conns_bad_v6(self):
        with self.assertRaises(ValueError):
            # IPv6 address with missing [] is invalid
            server_socket = '%s:%s' % ('::1', 11211)
            memcached.MemcacheRing([server_socket])

    def test_get_conns_hostname(self):
        with patch('swift.common.memcached.socket.getaddrinfo') as addrinfo:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.bind(('127.0.0.1', 0))
                sock.listen(1)
                sock_addr = sock.getsockname()
                fqdn = socket.getfqdn()
                server_socket = '%s:%s' % (fqdn, sock_addr[1])
                addrinfo.return_value = [(socket.AF_INET,
                                          socket.SOCK_STREAM, 0, '',
                                          ('127.0.0.1', sock_addr[1]))]
                memcache_client = memcached.MemcacheRing([server_socket])
                key = uuid4().hex.encode('ascii')
                for conn in memcache_client._get_conns(key):
                    peer_sockaddr = conn[2].getpeername()
                    peer_socket = '%s:%s' % (peer_sockaddr[0],
                                             peer_sockaddr[1])
                    self.assertEqual(peer_socket,
                                     '127.0.0.1:%d' % sock_addr[1])
                self.assertEqual(len(memcache_client._errors[server_socket]),
                                 0)
            finally:
                sock.close()

    def test_get_conns_hostname6(self):
        with patch('swift.common.memcached.socket.getaddrinfo') as addrinfo:
            try:
                sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                sock.bind(('::1', 0))
                sock.listen(1)
                sock_addr = sock.getsockname()
                fqdn = socket.getfqdn()
                server_socket = '%s:%s' % (fqdn, sock_addr[1])
                addrinfo.return_value = [(socket.AF_INET6,
                                          socket.SOCK_STREAM, 0, '',
                                          ('::1', sock_addr[1]))]
                memcache_client = memcached.MemcacheRing([server_socket])
                key = uuid4().hex.encode('ascii')
                for conn in memcache_client._get_conns(key):
                    peer_sockaddr = conn[2].getpeername()
                    peer_socket = '[%s]:%s' % (peer_sockaddr[0],
                                               peer_sockaddr[1])
                    self.assertEqual(peer_socket, '[::1]:%d' % sock_addr[1])
                self.assertEqual(len(memcache_client._errors[server_socket]),
                                 0)
            finally:
                sock.close()

    def test_set_get_json(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        cache_key = md5(b'some_key').hexdigest().encode('ascii')

        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        # See JSON_FLAG
        self.assertEqual(mock.cache, {cache_key: (b'2', b'0', b'[1, 2, 3]')})

        memcache_client.set('some_key', [4, 5, 6])
        self.assertEqual(memcache_client.get('some_key'), [4, 5, 6])
        self.assertEqual(mock.cache, {cache_key: (b'2', b'0', b'[4, 5, 6]')})

        memcache_client.set('some_key', ['simple str', 'utf8 str éà'])
        # As per http://wiki.openstack.org/encoding,
        # we should expect to have unicode
        self.assertEqual(
            memcache_client.get('some_key'), ['simple str', u'utf8 str éà'])
        self.assertEqual(mock.cache, {cache_key: (
            b'2', b'0', b'["simple str", "utf8 str \\u00e9\\u00e0"]')})

        memcache_client.set('some_key', [1, 2, 3], time=20)
        self.assertEqual(mock.cache, {cache_key: (b'2', b'20', b'[1, 2, 3]')})

        sixtydays = 60 * 24 * 60 * 60
        esttimeout = time.time() + sixtydays
        memcache_client.set('some_key', [1, 2, 3], time=sixtydays)
        _junk, cache_timeout, _junk = mock.cache[cache_key]
        self.assertAlmostEqual(float(cache_timeout), esttimeout, delta=1)

    def test_get_failed_connection_mid_request(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEqual(list(mock.cache.values()),
                         [(b'2', b'0', b'[1, 2, 3]')])

        # Now lets return an empty string, and make sure we aren't logging
        # the error.
        fake_stdout = six.StringIO()
        # force the logging through the DebugLogger instead of the nose
        # handler. This will use stdout, so we can assert that no stack trace
        # is logged.
        logger = debug_logger()
        with patch("sys.stdout", fake_stdout),\
                patch('swift.common.memcached.logging', logger):
            mock.read_return_empty_str = True
            self.assertIsNone(memcache_client.get('some_key'))
        log_lines = logger.get_lines_for_level('error')
        self.assertIn('Error talking to memcached', log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertNotIn("Traceback", fake_stdout.getvalue())

    def test_incr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        self.assertEqual(memcache_client.incr('some_key', delta=5), 5)
        self.assertEqual(memcache_client.get('some_key'), b'5')
        self.assertEqual(memcache_client.incr('some_key', delta=5), 10)
        self.assertEqual(memcache_client.get('some_key'), b'10')
        self.assertEqual(memcache_client.incr('some_key', delta=1), 11)
        self.assertEqual(memcache_client.get('some_key'), b'11')
        self.assertEqual(memcache_client.incr('some_key', delta=-5), 6)
        self.assertEqual(memcache_client.get('some_key'), b'6')
        self.assertEqual(memcache_client.incr('some_key', delta=-15), 0)
        self.assertEqual(memcache_client.get('some_key'), b'0')
        mock.read_return_none = True
        self.assertRaises(memcached.MemcacheConnectionError,
                          memcache_client.incr, 'some_key', delta=-15)
        self.assertTrue(mock.close_called)

    def test_incr_failed_connection_mid_request(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        self.assertEqual(memcache_client.incr('some_key', delta=5), 5)
        self.assertEqual(memcache_client.get('some_key'), b'5')
        self.assertEqual(memcache_client.incr('some_key', delta=5), 10)
        self.assertEqual(memcache_client.get('some_key'), b'10')

        # Now lets return an empty string, and make sure we aren't logging
        # the error.
        fake_stdout = six.StringIO()
        # force the logging through the DebugLogger instead of the nose
        # handler. This will use stdout, so we can assert that no stack trace
        # is logged.
        logger = debug_logger()
        with patch("sys.stdout", fake_stdout), \
                patch('swift.common.memcached.logging', logger):
            mock.read_return_empty_str = True
            self.assertRaises(memcached.MemcacheConnectionError,
                              memcache_client.incr, 'some_key', delta=1)
        log_lines = logger.get_lines_for_level('error')
        self.assertIn('Error talking to memcached', log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertNotIn('Traceback', fake_stdout.getvalue())

    def test_incr_w_timeout(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        cache_key = md5(b'some_key').hexdigest().encode('ascii')

        memcache_client.incr('some_key', delta=5, time=55)
        self.assertEqual(memcache_client.get('some_key'), b'5')
        self.assertEqual(mock.cache, {cache_key: (b'0', b'55', b'5')})

        memcache_client.delete('some_key')
        self.assertIsNone(memcache_client.get('some_key'))

        fiftydays = 50 * 24 * 60 * 60
        esttimeout = time.time() + fiftydays
        memcache_client.incr('some_key', delta=5, time=fiftydays)
        self.assertEqual(memcache_client.get('some_key'), b'5')
        _junk, cache_timeout, _junk = mock.cache[cache_key]
        self.assertAlmostEqual(float(cache_timeout), esttimeout, delta=1)

        memcache_client.delete('some_key')
        self.assertIsNone(memcache_client.get('some_key'))

        memcache_client.incr('some_key', delta=5)
        self.assertEqual(memcache_client.get('some_key'), b'5')
        self.assertEqual(mock.cache, {cache_key: (b'0', b'0', b'5')})

        memcache_client.incr('some_key', delta=5, time=55)
        self.assertEqual(memcache_client.get('some_key'), b'10')
        self.assertEqual(mock.cache, {cache_key: (b'0', b'0', b'10')})

    def test_decr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        self.assertEqual(memcache_client.decr('some_key', delta=5), 0)
        self.assertEqual(memcache_client.get('some_key'), b'0')
        self.assertEqual(memcache_client.incr('some_key', delta=15), 15)
        self.assertEqual(memcache_client.get('some_key'), b'15')
        self.assertEqual(memcache_client.decr('some_key', delta=4), 11)
        self.assertEqual(memcache_client.get('some_key'), b'11')
        self.assertEqual(memcache_client.decr('some_key', delta=15), 0)
        self.assertEqual(memcache_client.get('some_key'), b'0')
        mock.read_return_none = True
        self.assertRaises(memcached.MemcacheConnectionError,
                          memcache_client.decr, 'some_key', delta=15)

    def test_retry(self):
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211', '1.2.3.5:11211'])
        mock1 = ExplodingMockMemcached()
        mock2 = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock2, mock2)])
        memcache_client._client_cache['1.2.3.5:11211'] = MockedMemcachePool(
            [(mock1, mock1), (mock1, mock1)])
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(mock1.exploded, True)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            '[Errno 32] Broken pipe',
        ])

        self.logger.clear()
        mock1.exploded = False
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEqual(mock1.exploded, True)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            '[Errno 32] Broken pipe',
        ])
        # Check that we really did call create() twice
        self.assertEqual(memcache_client._client_cache['1.2.3.5:11211'].mocks,
                         [])

    def test_delete(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client.delete('some_key')
        self.assertIsNone(memcache_client.get('some_key'))

    def test_multi(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'])
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)

        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key')
        self.assertEqual(
            memcache_client.get_multi(('some_key2', 'some_key1'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3]])
        for key in (b'some_key1', b'some_key2'):
            key = md5(key).hexdigest().encode('ascii')
            self.assertIn(key, mock.cache)
            _junk, cache_timeout, _junk = mock.cache[key]
            self.assertEqual(cache_timeout, b'0')

        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
            time=20)
        for key in (b'some_key1', b'some_key2'):
            key = md5(key).hexdigest().encode('ascii')
            _junk, cache_timeout, _junk = mock.cache[key]
            self.assertEqual(cache_timeout, b'20')

        fortydays = 50 * 24 * 60 * 60
        esttimeout = time.time() + fortydays
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
            time=fortydays)
        for key in (b'some_key1', b'some_key2'):
            key = md5(key).hexdigest().encode('ascii')
            _junk, cache_timeout, _junk = mock.cache[key]
            self.assertAlmostEqual(float(cache_timeout), esttimeout, delta=1)
        self.assertEqual(memcache_client.get_multi(
            ('some_key2', 'some_key1', 'not_exists'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3], None])

        # Now lets simulate a lost connection and make sure we don't get
        # the index out of range stack trace when it does
        mock_stderr = six.StringIO()
        not_expected = "IndexError: list index out of range"
        with patch("sys.stderr", mock_stderr):
            mock.read_return_empty_str = True
            self.assertEqual(memcache_client.get_multi(
                ('some_key2', 'some_key1', 'not_exists'), 'multi_key'),
                None)
            self.assertFalse(not_expected in mock_stderr.getvalue())

    def test_serialization(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 allow_pickle=True)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_pickle = False
        memcache_client._allow_unpickle = True
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_unpickle = False
        self.assertIsNone(memcache_client.get('some_key'))
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_unpickle = True
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client._allow_pickle = True
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])

    def test_connection_pooling(self):
        with patch('swift.common.memcached.socket') as mock_module:
            def mock_getaddrinfo(host, port, family=socket.AF_INET,
                                 socktype=socket.SOCK_STREAM, proto=0,
                                 flags=0):
                return [(family, socktype, proto, '', (host, port))]

            mock_module.getaddrinfo = mock_getaddrinfo

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
            self.assertEqual(1, len(memcache_client._client_cache))
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

    def test_connection_pool_timeout(self):
        connections = defaultdict(Queue)
        pending = defaultdict(int)
        served = defaultdict(int)

        class MockConnectionPool(memcached.MemcacheConnPool):
            def get(self):
                pending[self.host] += 1
                conn = connections[self.host].get()
                pending[self.host] -= 1
                return conn

            def put(self, *args, **kwargs):
                connections[self.host].put(*args, **kwargs)
                served[self.host] += 1

        with mock.patch.object(memcached, 'MemcacheConnPool',
                               MockConnectionPool):
            memcache_client = memcached.MemcacheRing(['1.2.3.4:11211',
                                                      '1.2.3.5:11211'],
                                                     io_timeout=0.5,
                                                     pool_timeout=0.1)

            # Hand out a couple slow connections to 1.2.3.5, leaving 1.2.3.4
            # fast. All ten (10) clients should try to talk to .5 first, and
            # then move on to .4, and we'll assert all that below.
            mock_conn = MagicMock(), MagicMock()
            mock_conn[1].sendall = lambda x: sleep(0.2)
            connections['1.2.3.5'].put(mock_conn)
            connections['1.2.3.5'].put(mock_conn)

            mock_conn = MagicMock(), MagicMock()
            connections['1.2.3.4'].put(mock_conn)
            connections['1.2.3.4'].put(mock_conn)

            p = GreenPool()
            for i in range(10):
                p.spawn(memcache_client.set, 'key', 'value')

            # Wait for the dust to settle.
            p.waitall()

        self.assertEqual(pending['1.2.3.5'], 8)
        self.assertEqual(len(memcache_client._errors['1.2.3.5:11211']), 8)
        self.assertEqual(
            self.logger.get_lines_for_level('error'),
            ['Timeout getting a connection to memcached: 1.2.3.5:11211'] * 8)
        self.assertEqual(served['1.2.3.5'], 2)
        self.assertEqual(pending['1.2.3.4'], 0)
        self.assertEqual(len(memcache_client._errors['1.2.3.4:11211']), 0)
        self.assertEqual(served['1.2.3.4'], 8)

        # and we never got more put in that we gave out
        self.assertEqual(connections['1.2.3.5'].qsize(), 2)
        self.assertEqual(connections['1.2.3.4'].qsize(), 2)


if __name__ == '__main__':
    unittest.main()
