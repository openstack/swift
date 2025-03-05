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
import itertools
from collections import defaultdict
import errno
import io
import logging
import socket
import time
import unittest
import os

from unittest import mock
from configparser import NoSectionError, NoOptionError

from eventlet import GreenPool, sleep, Queue
from eventlet.pools import Pool
from eventlet.green import ssl

from swift.common import memcached
from swift.common.memcached import MemcacheConnectionError, md5hash, \
    MemcacheCommand, EXPTIME_MAXDELTA
from swift.common.utils import md5, human_readable
from unittest.mock import patch, MagicMock
from test.debug_logger import debug_logger


class MockedMemcachePool(memcached.MemcacheConnPool):
    def __init__(self, mocks):
        Pool.__init__(self, max_size=2)
        self.mocks = mocks
        # setting this for the eventlet workaround in the MemcacheConnPool
        self._parent_class_getter = super(memcached.MemcacheConnPool, self).get

    def create(self):
        return self.mocks.pop(0)


class ExplodingMockMemcached(object):
    should_explode = True
    exploded = False

    def sendall(self, string):
        if self.should_explode:
            self.exploded = True
            raise socket.error(errno.EPIPE, os.strerror(errno.EPIPE))

    def readline(self):
        if self.should_explode:
            self.exploded = True
            raise socket.error(errno.EPIPE, os.strerror(errno.EPIPE))
        return b'STORED\r\n'

    def read(self, size):
        if self.should_explode:
            self.exploded = True
            raise socket.error(errno.EPIPE, os.strerror(errno.EPIPE))

    def close(self):
        pass


TOO_BIG_KEY = md5(
    b'too-big', usedforsecurity=False).hexdigest().encode('ascii')


class MockMemcached(object):
    # See https://github.com/memcached/memcached/blob/master/doc/protocol.txt
    # In particular, the "Storage commands" section may be interesting.

    def __init__(self):
        self.inbuf = b''
        self.outbuf = b''
        # Structure: key -> (flags, absolute exptime, value)
        self.cache = {}
        self.down = False
        self.exc_on_delete = False
        self.read_return_none = False
        self.read_return_empty_str = False
        self.close_called = False

    def _get_absolute_exptime(self, exptime):
        exptime = int(exptime)
        if exptime == 0:
            # '0' means this cache item doesn't expire.
            return 0
        elif exptime <= EXPTIME_MAXDELTA:
            # Expiration time client passes in is delta from current unix time.
            return exptime + time.time()
        else:
            # Already a absolute time.
            return exptime

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
        self.cache[key] = (
            flags,
            self._get_absolute_exptime(exptime),
            self.inbuf[:int(num_bytes)]
        )
        self.inbuf = self.inbuf[int(num_bytes) + 2:]
        if noreply != b'noreply':
            if key == TOO_BIG_KEY:
                self.outbuf += b'SERVER_ERROR object too large for cache\r\n'
            else:
                self.outbuf += b'STORED\r\n'

    def handle_add(self, key, flags, exptime, num_bytes, noreply=b''):
        value = self.inbuf[:int(num_bytes)]
        self.inbuf = self.inbuf[int(num_bytes) + 2:]
        if key in self.cache:
            if noreply != b'noreply':
                self.outbuf += b'NOT_STORED\r\n'
        else:
            self.cache[key] = flags, self._get_absolute_exptime(exptime), value
            if noreply != b'noreply':
                self.outbuf += b'STORED\r\n'

    def _is_expired(self, key):
        _, exptime, _ = self.cache[key]
        if exptime != 0 and time.time() > exptime:
            self.cache.pop(key)
            return True
        else:
            return False

    def handle_delete(self, key, noreply=b''):
        if self.exc_on_delete:
            raise Exception('mock is has exc_on_delete set')
        if key in self.cache and not self._is_expired(key):
            del self.cache[key]
            if noreply != b'noreply':
                self.outbuf += b'DELETED\r\n'
        elif noreply != b'noreply':
            self.outbuf += b'NOT_FOUND\r\n'

    def handle_get(self, *keys):
        for key in keys:
            if key in self.cache and not self._is_expired(key):
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
        if key in self.cache and not self._is_expired(key):
            current = self.cache[key][2]
            new_val = str(int(current) + int(value)).encode('ascii')
            self.cache[key] = self.cache[key][:2] + (new_val, )
            self.outbuf += new_val + b'\r\n'
        else:
            self.outbuf += b'NOT_FOUND\r\n'

    def handle_decr(self, key, value, noreply=b''):
        if key in self.cache and not self._is_expired(key):
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


class TestMemcacheCommand(unittest.TestCase):
    def test_init(self):
        cmd = MemcacheCommand("set", "shard-updating-v2/a/c")
        self.assertEqual(cmd.method, "set")
        self.assertEqual(cmd.command, b"set")
        self.assertEqual(cmd.key, "shard-updating-v2/a/c")
        self.assertEqual(cmd.key_prefix, "shard-updating-v2/a")
        self.assertEqual(cmd.hash_key, md5hash("shard-updating-v2/a/c"))

    def test_get_key_prefix(self):
        cmd = MemcacheCommand("set", "shard-updating-v2/a/c")
        self.assertEqual(cmd.key_prefix, "shard-updating-v2/a")
        cmd = MemcacheCommand("set", "shard-listing-v2/accout/container3")
        self.assertEqual(cmd.key_prefix, "shard-listing-v2/accout")
        cmd = MemcacheCommand(
            "set", "auth_reseller_name/token/X58E34EL2SDFLEY3")
        self.assertEqual(cmd.key_prefix, "auth_reseller_name/token")
        cmd = MemcacheCommand("set", "nvratelimit/v2/wf/2345392374")
        self.assertEqual(cmd.key_prefix, "nvratelimit/v2/wf")
        cmd = MemcacheCommand("set", "some_key")
        self.assertEqual(cmd.key_prefix, "some_key")


class TestMemcached(unittest.TestCase):
    """Tests for swift.common.memcached"""

    def setUp(self):
        self.logger = debug_logger()
        self.set_cmd = MemcacheCommand('set', 'key')

    def test_logger_kwarg(self):
        server_socket = '%s:%s' % ('[::1]', 11211)
        client = memcached.MemcacheRing([server_socket])
        self.assertIs(client.logger, logging.getLogger())

        client = memcached.MemcacheRing([server_socket], logger=self.logger)
        self.assertIs(client.logger, self.logger)

    def test_tls_context_kwarg(self):
        with patch('swift.common.memcached.socket.socket'):
            server = '%s:%s' % ('[::1]', 11211)
            client = memcached.MemcacheRing([server])
            self.assertIsNone(client._client_cache[server]._tls_context)

            context = mock.Mock()
            client = memcached.MemcacheRing([server], tls_context=context)
            self.assertIs(client._client_cache[server]._tls_context, context)

            list(client._get_conns(self.set_cmd))
            context.wrap_socket.assert_called_once()

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
            memcache_client = memcached.MemcacheRing([sock1ipport, sock2ip],
                                                     logger=self.logger)
            one = two = True
            while one or two:  # Run until we match hosts one and two
                for conn in memcache_client._get_conns(self.set_cmd):
                    if 'b' not in getattr(conn[1], 'mode', ''):
                        self.assertIsInstance(conn[1], (
                            io.RawIOBase, io.BufferedIOBase))
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
            memcache_client = memcached.MemcacheRing([server_socket],
                                                     logger=self.logger)
            for conn in memcache_client._get_conns(self.set_cmd):
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
            memcache_client = memcached.MemcacheRing([server_host],
                                                     logger=self.logger)
            for conn in memcache_client._get_conns(self.set_cmd):
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
            memcached.MemcacheRing([server_socket], logger=self.logger)

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
                memcache_client = memcached.MemcacheRing([server_socket],
                                                         logger=self.logger)
                for conn in memcache_client._get_conns(self.set_cmd):
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
                memcache_client = memcached.MemcacheRing([server_socket],
                                                         logger=self.logger)
                for conn in memcache_client._get_conns(self.set_cmd):
                    peer_sockaddr = conn[2].getpeername()
                    peer_socket = '[%s]:%s' % (peer_sockaddr[0],
                                               peer_sockaddr[1])
                    self.assertEqual(peer_socket, '[::1]:%d' % sock_addr[1])
                self.assertEqual(len(memcache_client._errors[server_socket]),
                                 0)
            finally:
                sock.close()

    def test_set_get_json(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        cache_key = md5(b'some_key',
                        usedforsecurity=False).hexdigest().encode('ascii')

        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        # See JSON_FLAG
        self.assertEqual(mock.cache, {cache_key: (b'2', 0, b'[1, 2, 3]')})

        memcache_client.set('some_key', [4, 5, 6])
        self.assertEqual(memcache_client.get('some_key'), [4, 5, 6])
        self.assertEqual(mock.cache, {cache_key: (b'2', 0, b'[4, 5, 6]')})

        memcache_client.set('some_key', ['simple str', 'utf8 str éà'])
        # As per http://wiki.openstack.org/encoding,
        # we should expect to have unicode
        self.assertEqual(
            memcache_client.get('some_key'), ['simple str', u'utf8 str éà'])
        self.assertEqual(mock.cache, {cache_key: (
            b'2', 0, b'["simple str", "utf8 str \\u00e9\\u00e0"]')})

        now = time.time()
        with patch('time.time', return_value=now):
            memcache_client.set('some_key', [1, 2, 3], time=20)
        self.assertEqual(
            mock.cache, {cache_key: (b'2', now + 20, b'[1, 2, 3]')})

        sixtydays = 60 * 24 * 60 * 60
        esttimeout = time.time() + sixtydays
        memcache_client.set('some_key', [1, 2, 3], time=sixtydays)
        _junk, cache_timeout, _junk = mock.cache[cache_key]
        self.assertAlmostEqual(float(cache_timeout), esttimeout, delta=1)

    def test_set_error(self):
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211'], logger=self.logger,
            item_size_warning_threshold=1)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        now = time.time()
        with patch('time.time', return_value=now):
            memcache_client.set('too-big', [1, 2, 3])
        self.assertEqual(
            self.logger.get_lines_for_level('error'),
            ['Error talking to memcached: 1.2.3.4:11211: '
             'with key_prefix too-big, method set, time_spent 0.0, '
             'failed set: SERVER_ERROR object too large for cache'])
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warning_lines))
        self.assertIn('Item size larger than warning threshold',
                      warning_lines[0])
        self.assertTrue(mock.close_called)

    def test_set_error_raise_on_error(self):
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211'], logger=self.logger,
            item_size_warning_threshold=1)
        mock = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        now = time.time()

        with self.assertRaises(MemcacheConnectionError) as cm:
            with patch('time.time', return_value=now):
                memcache_client.set('too-big', [1, 2, 3], raise_on_error=True)
        self.assertIn("No memcached connections succeeded", str(cm.exception))
        self.assertEqual(
            self.logger.get_lines_for_level('error'),
            ['Error talking to memcached: 1.2.3.4:11211: '
             'with key_prefix too-big, method set, time_spent 0.0, '
             'failed set: SERVER_ERROR object too large for cache'])
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warning_lines))
        self.assertIn('Item size larger than warning threshold',
                      warning_lines[0])
        self.assertTrue(mock.close_called)

    def test_get_failed_connection_mid_request(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEqual(list(mock.cache.values()),
                         [(b'2', 0, b'[1, 2, 3]')])

        # Now lets return an empty string, and make sure we aren't logging
        # the error.
        fake_stdout = io.StringIO()
        # force the logging through the DebugLogger instead of the nose
        # handler. This will use stdout, so we can assert that no stack trace
        # is logged.
        with patch("sys.stdout", fake_stdout):
            mock.read_return_empty_str = True
            self.assertIsNone(memcache_client.get('some_key'))
        log_lines = self.logger.get_lines_for_level('error')
        self.assertIn('Error talking to memcached', log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertNotIn("Traceback", fake_stdout.getvalue())

    def test_incr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
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
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        self.assertEqual(memcache_client.incr('some_key', delta=5), 5)
        self.assertEqual(memcache_client.get('some_key'), b'5')
        self.assertEqual(memcache_client.incr('some_key', delta=5), 10)
        self.assertEqual(memcache_client.get('some_key'), b'10')

        # Now lets return an empty string, and make sure we aren't logging
        # the error.
        fake_stdout = io.StringIO()
        # force the logging through the DebugLogger instead of the nose
        # handler. This will use stdout, so we can assert that no stack trace
        # is logged.
        with patch("sys.stdout", fake_stdout):
            mock.read_return_empty_str = True
            self.assertRaises(memcached.MemcacheConnectionError,
                              memcache_client.incr, 'some_key', delta=1)
        log_lines = self.logger.get_lines_for_level('error')
        self.assertIn('Error talking to memcached', log_lines[0])
        self.assertFalse(log_lines[1:])
        self.assertNotIn('Traceback', fake_stdout.getvalue())

    def test_incr_w_timeout(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        cache_key = md5(b'some_key',
                        usedforsecurity=False).hexdigest().encode('ascii')

        now = time.time()
        with patch('time.time', return_value=now):
            memcache_client.incr('some_key', delta=5, time=55)
        self.assertEqual(memcache_client.get('some_key'), b'5')
        self.assertEqual(mock.cache, {cache_key: (b'0', now + 55, b'5')})

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
        self.assertEqual(mock.cache, {cache_key: (b'0', 0, b'5')})

        memcache_client.incr('some_key', delta=5, time=55)
        self.assertEqual(memcache_client.get('some_key'), b'10')
        self.assertEqual(mock.cache, {cache_key: (b'0', 0, b'10')})

    def test_incr_expiration_time(self):
        # Test increment with different expiration times
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211'], logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)

        now = time.time()
        # Test expiration time < 'EXPTIME_MAXDELTA'
        with patch('time.time', return_value=now):
            memcache_client.incr('expiring_key', delta=5, time=1)
            self.assertEqual(memcache_client.get('expiring_key'), b'5')
        with patch('time.time', return_value=now + 2):
            self.assertIsNone(memcache_client.get('expiring_key'))
        # Test expiration time is 0
        with patch('time.time', return_value=now):
            memcache_client.incr('expiring_key', delta=5, time=0)
            self.assertEqual(memcache_client.get('expiring_key'), b'5')
        with patch('time.time', return_value=now + 100):
            self.assertEqual(memcache_client.get('expiring_key'), b'5')
        memcache_client.delete('expiring_key')
        # Test expiration time > 'EXPTIME_MAXDELTA'
        with patch('time.time', return_value=now):
            memcache_client.incr(
                'expiring_key', delta=5, time=(EXPTIME_MAXDELTA + 10))
        with patch('time.time', return_value=(now + EXPTIME_MAXDELTA + 2)):
            self.assertEqual(memcache_client.get('expiring_key'), b'5')
        with patch('time.time', return_value=(now + EXPTIME_MAXDELTA + 11)):
            self.assertIsNone(memcache_client.get('expiring_key'))

    def test_set_expiration_time(self):
        # Test set with different expiration times
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211'], logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)

        now = time.time()
        # Test expiration time < 'EXPTIME_MAXDELTA'
        with patch('time.time', return_value=now):
            memcache_client.set('expiring_key', value=5, time=1)
            self.assertEqual(memcache_client.get('expiring_key'), 5)
        with patch('time.time', return_value=now + 2):
            self.assertIsNone(memcache_client.get('expiring_key'))
        # Test expiration time is 0
        with patch('time.time', return_value=now):
            memcache_client.set('expiring_key', value=5, time=0)
            self.assertEqual(memcache_client.get('expiring_key'), 5)
        with patch('time.time', return_value=now + 100):
            self.assertEqual(memcache_client.get('expiring_key'), 5)
        memcache_client.delete('expiring_key')
        # Test expiration time > 'EXPTIME_MAXDELTA'
        with patch('time.time', return_value=now):
            memcache_client.set(
                'expiring_key', value=5, time=(EXPTIME_MAXDELTA + 10))
        with patch('time.time', return_value=(now + EXPTIME_MAXDELTA + 2)):
            self.assertEqual(memcache_client.get('expiring_key'), 5)
        with patch('time.time', return_value=(now + EXPTIME_MAXDELTA + 11)):
            self.assertIsNone(memcache_client.get('expiring_key'))

    def test_decr(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
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
            ['1.2.3.4:11211', '1.2.3.5:11211'], logger=self.logger)
        mock1 = ExplodingMockMemcached()
        mock2 = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock2, mock2)])
        memcache_client._client_cache['1.2.3.5:11211'] = MockedMemcachePool(
            [(mock1, mock1), (mock1, mock1)])
        now = time.time()
        with patch('time.time', return_value=now):
            memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(mock1.exploded, True)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ])

        self.logger.clear()
        mock1.exploded = False
        now = time.time()
        with patch('time.time', return_value=now):
            self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEqual(mock1.exploded, True)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method get, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ])
        # Check that we really did call create() twice
        self.assertEqual(memcache_client._client_cache['1.2.3.5:11211'].mocks,
                         [])

    def test_error_limiting(self):
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211', '1.2.3.5:11211'], logger=self.logger)
        mock1 = ExplodingMockMemcached()
        mock2 = ExplodingMockMemcached()
        mock2.should_explode = False
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock2, mock2)] * 12)
        memcache_client._client_cache['1.2.3.5:11211'] = MockedMemcachePool(
            [(mock1, mock1)] * 12)

        now = time.time()
        with patch('time.time', return_value=now):
            for _ in range(12):
                memcache_client.set('some_key', [1, 2, 3])
        # twelfth one skips .5 because of error limiting and goes straight
        # to .4
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ] * 11 + [
            'Error limiting server 1.2.3.5:11211'
        ])
        self.logger.clear()

        mock2.should_explode = True
        now = time.time()
        with patch('time.time', return_value=now):
            for _ in range(12):
                memcache_client.set('some_key', [1, 2, 3])
        # as we keep going, eventually .4 gets error limited, too
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ] * 10 + [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
            'Error limiting server 1.2.3.4:11211',
            'Error connecting to memcached: ALL: with key_prefix some_key, '
            'method set: No more memcached servers to try',
        ])
        self.logger.clear()

        # continued requests just keep bypassing memcache
        for _ in range(12):
            memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error connecting to memcached: ALL: with key_prefix some_key, '
            'method set: No more memcached servers to try',
        ] * 12)
        self.logger.clear()

        # and get()s are all a "cache miss"
        self.assertIsNone(memcache_client.get('some_key'))
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error connecting to memcached: ALL: with key_prefix some_key, '
            'method get: No more memcached servers to try',
        ])

    def test_error_disabled(self):
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211'], logger=self.logger, error_limit_time=0)
        mock1 = ExplodingMockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock1, mock1)] * 20)

        now = time.time()
        with patch('time.time', return_value=now):
            for _ in range(20):
                memcache_client.set('some_key', [1, 2, 3])
        # twelfth one skips .5 because of error limiting and goes straight
        # to .4
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ] * 20)

    def test_error_raising(self):
        memcache_client = memcached.MemcacheRing(
            ['1.2.3.4:11211'], logger=self.logger, error_limit_time=0)
        mock1 = ExplodingMockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock1, mock1)] * 20)

        # expect exception when requested...
        now = time.time()
        with patch('time.time', return_value=now):
            with self.assertRaises(MemcacheConnectionError):
                memcache_client.set('some_key', [1, 2, 3], raise_on_error=True)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ])
        self.logger.clear()

        with patch('time.time', return_value=now):
            with self.assertRaises(MemcacheConnectionError):
                memcache_client.get('some_key', raise_on_error=True)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix some_key, method get, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ])
        self.logger.clear()

        with patch('time.time', return_value=now):
            with self.assertRaises(MemcacheConnectionError):
                memcache_client.set(
                    'shard-updating-v2/acc/container', [1, 2, 3],
                    raise_on_error=True)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix shard-updating-v2/acc, method set, '
            'time_spent 0.0, [Errno 32] Broken pipe',
        ])
        self.logger.clear()

        # ...but default is no exception
        with patch('time.time', return_value=now):
            memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ])
        self.logger.clear()

        with patch('time.time', return_value=now):
            memcache_client.get('some_key')
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix some_key, method get, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ])
        self.logger.clear()

        with patch('time.time', return_value=now):
            memcache_client.set('shard-updating-v2/acc/container', [1, 2, 3])
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.4:11211: '
            'with key_prefix shard-updating-v2/acc, method set, '
            'time_spent 0.0, [Errno 32] Broken pipe',
        ])

    def test_error_limiting_custom_config(self):
        def do_calls(time_step, num_calls, **memcache_kwargs):
            self.logger.clear()
            memcache_client = memcached.MemcacheRing(
                ['1.2.3.5:11211'], logger=self.logger,
                **memcache_kwargs)
            mock1 = ExplodingMockMemcached()
            memcache_client._client_cache['1.2.3.5:11211'] = \
                MockedMemcachePool([(mock1, mock1)] * num_calls)

            for n in range(num_calls):
                with mock.patch.object(memcached.tm, 'time',
                                       return_value=time_step * n):
                    memcache_client.set('some_key', [1, 2, 3])

        # with default error_limit_time of 60, one call per 5 secs, twelfth one
        # triggers error limit
        do_calls(5.0, 12)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ] * 10 + [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
            'Error limiting server 1.2.3.5:11211',
            'Error connecting to memcached: ALL: with key_prefix some_key, '
            'method set: No more memcached servers to try',
        ])

        # with default error_limit_time of 60, one call per 6 secs, error limit
        # is not triggered
        do_calls(6.0, 20)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ] * 20)

        # with error_limit_time of 66, one call per 6 secs, twelfth one
        # triggers error limit
        do_calls(6.0, 12, error_limit_time=66)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ] * 10 + [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
            'Error limiting server 1.2.3.5:11211',
            'Error connecting to memcached: ALL: with key_prefix some_key, '
            'method set: No more memcached servers to try'])

        # with error_limit_time of 70, one call per 6 secs, error_limit_count
        # of 11, 13th call triggers error limit
        do_calls(6.0, 13, error_limit_time=70, error_limit_count=11)
        self.assertEqual(self.logger.get_lines_for_level('error'), [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
        ] * 11 + [
            'Error talking to memcached: 1.2.3.5:11211: '
            'with key_prefix some_key, method set, time_spent 0.0, '
            '[Errno 32] Broken pipe',
            'Error limiting server 1.2.3.5:11211',
            'Error connecting to memcached: ALL: with key_prefix some_key, '
            'method set: No more memcached servers to try'])

    def test_delete(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        memcache_client.delete('some_key')
        self.assertIsNone(memcache_client.get('some_key'))

    def test_multi(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)

        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key')
        self.assertEqual(
            memcache_client.get_multi(('some_key2', 'some_key1'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3]])
        for key in (b'some_key1', b'some_key2'):
            key = md5(key, usedforsecurity=False).hexdigest().encode('ascii')
            self.assertIn(key, mock.cache)
            _junk, cache_timeout, _junk = mock.cache[key]
            self.assertEqual(cache_timeout, 0)

        now = time.time()
        with patch('time.time', return_value=now):
            memcache_client.set_multi(
                {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
                time=20)
        for key in (b'some_key1', b'some_key2'):
            key = md5(key, usedforsecurity=False).hexdigest().encode('ascii')
            _junk, cache_timeout, _junk = mock.cache[key]
            self.assertEqual(cache_timeout, now + 20)

        fortydays = 50 * 24 * 60 * 60
        esttimeout = time.time() + fortydays
        memcache_client.set_multi(
            {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key',
            time=fortydays)
        for key in (b'some_key1', b'some_key2'):
            key = md5(key, usedforsecurity=False).hexdigest().encode('ascii')
            _junk, cache_timeout, _junk = mock.cache[key]
            self.assertAlmostEqual(float(cache_timeout), esttimeout, delta=1)
        self.assertEqual(memcache_client.get_multi(
            ('some_key2', 'some_key1', 'not_exists'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3], None])

        # Now lets simulate a lost connection and make sure we don't get
        # the index out of range stack trace when it does
        mock_stderr = io.StringIO()
        not_expected = "IndexError: list index out of range"
        with patch("sys.stderr", mock_stderr):
            mock.read_return_empty_str = True
            self.assertEqual(memcache_client.get_multi(
                ('some_key2', 'some_key1', 'not_exists'), 'multi_key'),
                None)
            self.assertFalse(not_expected in mock_stderr.getvalue())

    def test_multi_delete(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211',
                                                  '1.2.3.5:11211'],
                                                 logger=self.logger)
        mock1 = MockMemcached()
        mock2 = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock1, mock1)] * 2)
        memcache_client._client_cache['1.2.3.5:11211'] = MockedMemcachePool(
            [(mock2, mock2)] * 2)

        # MemcacheRing will put 'some_key0' on server 1.2.3.5:11211 and
        # 'some_key1' and 'multi_key' on '1.2.3.4:11211'
        memcache_client.set_multi(
            {'some_key0': [1, 2, 3], 'some_key1': [4, 5, 6]}, 'multi_key')
        self.assertEqual(
            memcache_client.get_multi(('some_key1', 'some_key0'), 'multi_key'),
            [[4, 5, 6], [1, 2, 3]])
        for key in (b'some_key0', b'some_key1'):
            key = md5(key, usedforsecurity=False).hexdigest().encode('ascii')
            self.assertIn(key, mock1.cache)
            _junk, cache_timeout, _junk = mock1.cache[key]
            self.assertEqual(cache_timeout, 0)

        memcache_client.set('some_key0', [7, 8, 9])
        self.assertEqual(memcache_client.get('some_key0'), [7, 8, 9])
        key = md5(b'some_key0',
                  usedforsecurity=False).hexdigest().encode('ascii')
        self.assertIn(key, mock2.cache)

        # Delete 'some_key0' with server_key='multi_key'
        memcache_client.delete('some_key0', server_key='multi_key')
        self.assertEqual(memcache_client.get_multi(
            ('some_key0', 'some_key1'), 'multi_key'),
            [None, [4, 5, 6]])

        # 'some_key0' have to be available on 1.2.3.5:11211
        self.assertEqual(memcache_client.get('some_key0'), [7, 8, 9])
        self.assertIn(key, mock2.cache)

    def test_serialization(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)
        memcache_client.set('some_key', [1, 2, 3])
        self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
        self.assertEqual(len(mock.cache), 1)
        key = next(iter(mock.cache))
        self.assertEqual(mock.cache[key][0], b'2')  # JSON_FLAG
        # Pretend we've got some really old pickle data in there
        mock.cache[key] = (b'1',) + mock.cache[key][1:]
        self.assertIsNone(memcache_client.get('some_key'))

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
                                                     connect_timeout=10,
                                                     logger=self.logger)
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
                                                     pool_timeout=0.1,
                                                     logger=self.logger)

            # Hand out a couple slow connections to 1.2.3.5, leaving 1.2.3.4
            # fast. All ten (10) clients should try to talk to .5 first, and
            # then move on to .4, and we'll assert all that below.
            mock_conn = MagicMock(), MagicMock()
            mock_conn[0].readline = lambda: b'STORED\r\n'
            mock_conn[1].sendall = lambda x: sleep(0.2)
            connections['1.2.3.5'].put(mock_conn)
            connections['1.2.3.5'].put(mock_conn)

            mock_conn = MagicMock(), MagicMock()
            mock_conn[0].readline = lambda: b'STORED\r\n'
            connections['1.2.3.4'].put(mock_conn)
            connections['1.2.3.4'].put(mock_conn)

            p = GreenPool()
            for i in range(10):
                p.spawn(memcache_client.set, 'key', 'value')

            # Wait for the dust to settle.
            p.waitall()

        self.assertEqual(pending['1.2.3.5'], 8)
        self.assertEqual(len(memcache_client._errors['1.2.3.5:11211']), 8)
        error_logs = self.logger.get_lines_for_level('error')
        self.assertEqual(len(error_logs), 8)
        for each_log in error_logs:
            self.assertIn(
                'Timeout getting a connection to memcached: 1.2.3.5:11211: '
                'with key_prefix key',
                each_log)
        self.assertEqual(served['1.2.3.5'], 2)
        self.assertEqual(pending['1.2.3.4'], 0)
        self.assertEqual(len(memcache_client._errors['1.2.3.4:11211']), 0)
        self.assertEqual(served['1.2.3.4'], 8)

        # and we never got more put in that we gave out
        self.assertEqual(connections['1.2.3.5'].qsize(), 2)
        self.assertEqual(connections['1.2.3.4'].qsize(), 2)

    def test_connection_slow_connect(self):
        with patch('swift.common.memcached.socket') as mock_module:
            def mock_getaddrinfo(host, port, family=socket.AF_INET,
                                 socktype=socket.SOCK_STREAM, proto=0,
                                 flags=0):
                return [(family, socktype, proto, '', (host, port))]

            mock_module.getaddrinfo = mock_getaddrinfo

            # patch socket, stub socket.socket, mock sock
            mock_sock = mock_module.socket.return_value

            def wait_connect(addr):
                # slow connect gives Timeout Exception
                sleep(1)

            # patch connect method
            mock_sock.connect = wait_connect

            memcache_client = memcached.MemcacheRing(
                ['1.2.3.4:11211'], connect_timeout=0.1, logger=self.logger)

            # sanity
            self.assertEqual(1, len(memcache_client._client_cache))
            for server, pool in memcache_client._client_cache.items():
                self.assertEqual(2, pool.max_size)

            # try to get connect and no connection found
            # so it will result in StopIteration
            conn_generator = memcache_client._get_conns(self.set_cmd)
            with self.assertRaises(StopIteration):
                next(conn_generator)

            self.assertEqual(1, mock_sock.close.call_count)

    def test_item_size_warning_threshold(self):
        mock = MockMemcached()
        mocked_pool = MockedMemcachePool([(mock, mock)] * 2)

        def do_test(d, threshold, should_warn, error=False):
            self.logger.clear()
            try:
                memcache_client = memcached.MemcacheRing(
                    ['1.2.3.4:11211'], item_size_warning_threshold=threshold,
                    logger=self.logger)
                memcache_client._client_cache['1.2.3.4:11211'] = mocked_pool
                memcache_client.set('some_key', d, serialize=False)
                warning_lines = self.logger.get_lines_for_level('warning')
                if should_warn:
                    self.assertIn(
                        'Item size larger than warning threshold: '
                        '%d (%s) >= %d (%s)' % (
                            len(d), human_readable(len(d)), threshold,
                            human_readable(threshold)),
                        warning_lines[0])
                else:
                    self.assertFalse(warning_lines)
            except ValueError as err:
                if not err:
                    self.fail(err)
                else:
                    self.assertIn(
                        'Config option must be a number, greater than 0, '
                        'less than 100, not "%s".' % threshold,
                        str(err))

        data = '1' * 100
        # let's start with something easy, say warning at 80
        for data_size, warn in ((79, False), (80, True), (81, True),
                                (99, True), (100, True)):
            do_test(data[:data_size], 80, warn)

        # if we set the threshold to -1 will turn off the warning
        for data_size, warn in ((79, False), (80, False), (81, False),
                                (99, False), (100, False)):
            do_test(data[:data_size], -1, warn)

        # Changing to 0 should warn on everything
        for data_size, warn in ((0, True), (1, True), (50, True),
                                (99, True), (100, True)):
            do_test(data[:data_size], 0, warn)

        # Let's do a big number
        do_test('1' * 2048576, 1000000, True)

    def test_operations_timing_stats(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock = MockMemcached()
        memcache_client._client_cache['1.2.3.4:11211'] = MockedMemcachePool(
            [(mock, mock)] * 2)

        with patch('time.time',) as mock_time:
            mock_time.return_value = 1000.99
            memcache_client.set('some_key', [1, 2, 3])
            last_stats = self.logger.statsd_client.calls['timing_since'][-1]
            self.assertEqual('memcached.set.timing', last_stats[0][0])
            self.assertEqual(last_stats[0][1], 1000.99)
            mock_time.return_value = 2000.99
            self.assertEqual(memcache_client.get('some_key'), [1, 2, 3])
            last_stats = self.logger.statsd_client.calls['timing_since'][-1]
            self.assertEqual('memcached.get.timing', last_stats[0][0])
            self.assertEqual(last_stats[0][1], 2000.99)
            mock_time.return_value = 3000.99
            self.assertEqual(memcache_client.decr('decr_key', delta=5), 0)
            last_stats = self.logger.statsd_client.calls['timing_since'][-1]
            self.assertEqual('memcached.decr.timing', last_stats[0][0])
            self.assertEqual(last_stats[0][1], 3000.99)
            mock_time.return_value = 4000.99
            self.assertEqual(memcache_client.incr('decr_key', delta=5), 5)
            last_stats = self.logger.statsd_client.calls['timing_since'][-1]
            self.assertEqual('memcached.incr.timing', last_stats[0][0])
            self.assertEqual(last_stats[0][1], 4000.99)
            mock_time.return_value = 5000.99
            memcache_client.set_multi(
                {'some_key1': [1, 2, 3], 'some_key2': [4, 5, 6]}, 'multi_key')
            last_stats = self.logger.statsd_client.calls['timing_since'][-1]
            self.assertEqual('memcached.set_multi.timing', last_stats[0][0])
            self.assertEqual(last_stats[0][1], 5000.99)
            mock_time.return_value = 6000.99
            self.assertEqual(
                memcache_client.get_multi(
                    ('some_key2', 'some_key1'),
                    'multi_key'),
                [[4, 5, 6],
                 [1, 2, 3]])
            last_stats = self.logger.statsd_client.calls['timing_since'][-1]
            self.assertEqual('memcached.get_multi.timing', last_stats[0][0])
            self.assertEqual(last_stats[0][1], 6000.99)
            mock_time.return_value = 7000.99
            memcache_client.delete('some_key')
            last_stats = self.logger.statsd_client.calls['timing_since'][-1]
            self.assertEqual('memcached.delete.timing', last_stats[0][0])
            self.assertEqual(last_stats[0][1], 7000.99)

    def test_operations_timing_stats_with_incr_exception(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)

        def handle_add(key, flags, exptime, num_bytes, noreply=b''):
            raise Exception('add failed')

        with patch('time.time', ) as mock_time:
            with mock.patch.object(mock_memcache, 'handle_add', handle_add):
                mock_time.return_value = 4000.99
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.incr('incr_key', delta=5)
                self.assertTrue(
                    self.logger.statsd_client.calls['timing_since'])
                last_stats = \
                    self.logger.statsd_client.calls['timing_since'][-1]
                self.assertEqual('memcached.incr.errors.timing',
                                 last_stats[0][0])
                self.assertEqual(last_stats[0][1], 4000.99)
                self.assertEqual(
                    'Error talking to memcached: 1.2.3.4:11211: '
                    'with key_prefix incr_key, method incr, time_spent 0.0: ',
                    self.logger.get_lines_for_level('error')[0])

    def test_operations_timing_stats_with_set_exception(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)

        def handle_set(key, flags, exptime, num_bytes, noreply=b''):
            raise Exception('set failed')

        with patch('time.time', ) as mock_time:
            with mock.patch.object(mock_memcache, 'handle_set', handle_set):
                mock_time.return_value = 4000.99
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.set(
                        'set_key', [1, 2, 3],
                        raise_on_error=True)
                self.assertTrue(
                    self.logger.statsd_client.calls['timing_since'])
                last_stats = \
                    self.logger.statsd_client.calls['timing_since'][-1]
                self.assertEqual('memcached.set.errors.timing',
                                 last_stats[0][0])
                self.assertEqual(last_stats[0][1], 4000.99)
                self.assertEqual(
                    'Error talking to memcached: 1.2.3.4:11211: '
                    'with key_prefix set_key, method set, time_spent 0.0: ',
                    self.logger.get_lines_for_level('error')[0])

    def test_operations_timing_stats_with_get_exception(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)

        def handle_get(*keys):
            raise Exception('get failed')

        with patch('time.time', ) as mock_time:
            with mock.patch.object(mock_memcache, 'handle_get', handle_get):
                mock_time.return_value = 4000.99
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.get('get_key', raise_on_error=True)
                self.assertTrue(
                    self.logger.statsd_client.calls['timing_since'])
                last_stats = \
                    self.logger.statsd_client.calls['timing_since'][-1]
                self.assertEqual('memcached.get.errors.timing',
                                 last_stats[0][0])
                self.assertEqual(last_stats[0][1], 4000.99)
                self.assertEqual(
                    'Error talking to memcached: 1.2.3.4:11211: '
                    'with key_prefix get_key, method get, time_spent 0.0: ',
                    self.logger.get_lines_for_level('error')[0])

    def test_operations_timing_stats_with_get_error(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)

        def handle_get(*keys):
            raise MemcacheConnectionError('failed to connect')

        with patch('time.time', ) as mock_time:
            with mock.patch.object(mock_memcache, 'handle_get', handle_get):
                mock_time.return_value = 4000.99
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.get('get_key', raise_on_error=True)
                self.assertTrue(
                    self.logger.statsd_client.calls['timing_since'])
                last_stats = \
                    self.logger.statsd_client.calls['timing_since'][-1]
                self.assertEqual('memcached.get.conn_err.timing',
                                 last_stats[0][0])
                self.assertEqual(last_stats[0][1], 4000.99)
                self.assertEqual('Error talking to memcached: 1.2.3.4:11211: '
                                 'with key_prefix get_key, method get, '
                                 'time_spent 0.0, failed to connect',
                                 self.logger.get_lines_for_level('error')[0])

    def test_operations_timing_stats_with_incr_timeout(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 io_timeout=0.01,
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)

        def handle_add(key, flags, exptime, num_bytes, noreply=b''):
            sleep(0.05)

        with patch('time.time', ) as mock_time:
            with mock.patch.object(mock_memcache, 'handle_add', handle_add):
                mock_time.side_effect = itertools.count(4000.99, 1.0)
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.incr('nvratelimit/v2/wf/124593', delta=5)
                self.assertTrue(
                    self.logger.statsd_client.calls['timing_since'])
                last_stats = \
                    self.logger.statsd_client.calls['timing_since'][-1]
                self.assertEqual('memcached.incr.timeout.timing',
                                 last_stats[0][0])
                self.assertEqual(last_stats[0][1], 4002.99)
                error_logs = self.logger.get_lines_for_level('error')
                self.assertIn('Timeout talking to memcached: 1.2.3.4:11211: ',
                              error_logs[0])
                self.assertIn(
                    'with key_prefix nvratelimit/v2/wf, ', error_logs[0])
                self.assertIn('method incr, ', error_logs[0])
                self.assertIn(
                    'config_timeout 0.01, time_spent 1.0', error_logs[0])

    def test_operations_timing_stats_with_set_timeout(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 io_timeout=0.01,
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)

        def handle_set(key, flags, exptime, num_bytes, noreply=b''):
            sleep(0.05)

        with patch('time.time', ) as mock_time:
            with mock.patch.object(mock_memcache, 'handle_set', handle_set):
                mock_time.side_effect = itertools.count(4000.99, 1.0)
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.set(
                        'shard-updating-v2/acc/container', [1, 2, 3],
                        raise_on_error=True)
                self.assertTrue(
                    self.logger.statsd_client.calls['timing_since'])
                last_stats = \
                    self.logger.statsd_client.calls['timing_since'][-1]
                self.assertEqual('memcached.set.timeout.timing',
                                 last_stats[0][0])
                self.assertEqual(last_stats[0][1], 4002.99)
                error_logs = self.logger.get_lines_for_level('error')
                self.assertIn('Timeout talking to memcached: 1.2.3.4:11211: ',
                              error_logs[0])
                self.assertIn(
                    'with key_prefix shard-updating-v2/acc, ', error_logs[0])
                self.assertIn('method set, ', error_logs[0])
                self.assertIn(
                    'config_timeout 0.01, time_spent 1.0', error_logs[0])

    def test_operations_timing_stats_with_get_timeout(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 io_timeout=0.01,
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)

        def handle_get(*keys):
            sleep(0.05)

        with patch('time.time', ) as mock_time:
            with mock.patch.object(mock_memcache, 'handle_get', handle_get):
                mock_time.side_effect = itertools.count(4000.99, 1.0)
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.get(
                        'shard-updating-v2/acc/container', raise_on_error=True)
                self.assertTrue(
                    self.logger.statsd_client.calls['timing_since'])
                last_stats = \
                    self.logger.statsd_client.calls['timing_since'][-1]
                self.assertEqual('memcached.get.timeout.timing',
                                 last_stats[0][0])
                self.assertEqual(last_stats[0][1], 4002.99)
                error_logs = self.logger.get_lines_for_level('error')
                self.assertIn('Timeout talking to memcached: 1.2.3.4:11211: ',
                              error_logs[0])
                self.assertIn(
                    'with key_prefix shard-updating-v2/acc, ', error_logs[0])
                self.assertIn('method get, ', error_logs[0])
                self.assertIn(
                    'config_timeout 0.01, time_spent 1.0', error_logs[0])

    def test_incr_add_expires(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 io_timeout=0.01,
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)
        incr_calls = []
        orig_incr = mock_memcache.handle_incr
        orig_add = mock_memcache.handle_add

        def handle_incr(key, value, noreply=b''):
            if incr_calls:
                mock_memcache.cache.clear()
            incr_calls.append(key)
            orig_incr(key, value, noreply)

        def handle_add(key, flags, exptime, num_bytes, noreply=b''):
            mock_memcache.cache[key] = 'already set!'
            orig_add(key, flags, exptime, num_bytes, noreply)
            mock_memcache.cache.clear()

        with patch('time.time', ) as mock_time:
            mock_time.side_effect = itertools.count(4000.99, 1.0)
            with mock.patch.object(mock_memcache, 'handle_incr', handle_incr):
                with mock.patch.object(mock_memcache, 'handle_add',
                                       handle_add):
                    with self.assertRaises(MemcacheConnectionError):
                        memcache_client.incr(
                            'shard-updating-v2/acc/container', time=1.23)
        self.assertTrue(self.logger.statsd_client.calls['timing_since'])
        last_stats = self.logger.statsd_client.calls['timing_since'][-1]
        self.assertEqual('memcached.incr.conn_err.timing',
                         last_stats[0][0])
        self.assertEqual(last_stats[0][1], 4002.99)
        error_logs = self.logger.get_lines_for_level('error')
        self.assertIn('Error talking to memcached: 1.2.3.4:11211: ',
                      error_logs[0])
        self.assertIn('with key_prefix shard-updating-v2/acc, method incr, '
                      'time_spent 1.0, expired ttl=1.23',
                      error_logs[0])
        self.assertIn('1.2.3.4:11211', memcache_client._errors)
        self.assertFalse(memcache_client._errors['1.2.3.4:11211'])

    def test_incr_unexpected_response(self):
        memcache_client = memcached.MemcacheRing(['1.2.3.4:11211'],
                                                 io_timeout=0.01,
                                                 logger=self.logger)
        mock_memcache = MockMemcached()
        memcache_client._client_cache[
            '1.2.3.4:11211'] = MockedMemcachePool(
            [(mock_memcache, mock_memcache)] * 2)
        resp = b'UNEXPECTED RESPONSE\r\n'

        def handle_incr(key, value, noreply=b''):
            mock_memcache.outbuf += resp

        with patch('time.time') as mock_time:
            mock_time.side_effect = itertools.count(4000.99, 1.0)
            with mock.patch.object(mock_memcache, 'handle_incr', handle_incr):
                with self.assertRaises(MemcacheConnectionError):
                    memcache_client.incr(
                        'shard-updating-v2/acc/container', time=1.23)
        self.assertTrue(self.logger.statsd_client.calls['timing_since'])
        last_stats = self.logger.statsd_client.calls['timing_since'][-1]
        self.assertEqual('memcached.incr.errors.timing',
                         last_stats[0][0])
        self.assertEqual(last_stats[0][1], 4002.99)
        error_logs = self.logger.get_lines_for_level('error')
        self.assertIn('Error talking to memcached: 1.2.3.4:11211: ',
                      error_logs[0])
        self.assertIn("with key_prefix shard-updating-v2/acc, method incr, "
                      "time_spent 1.0" % resp.split(), error_logs[0])
        self.assertIn('1.2.3.4:11211', memcache_client._errors)
        self.assertEqual([4005.99], memcache_client._errors['1.2.3.4:11211'])


class ExcConfigParser(object):

    def read(self, path):
        raise Exception('read called with %r' % path)


class EmptyConfigParser(object):

    def read(self, path):
        return False


def get_config_parser(memcache_servers='1.2.3.4:5',
                      memcache_max_connections='4',
                      section='memcache',
                      item_size_warning_threshold='75'):
    _srvs = memcache_servers
    _maxc = memcache_max_connections
    _section = section
    _warn_threshold = item_size_warning_threshold

    class SetConfigParser(object):

        def items(self, section_name):
            if section_name != section:
                raise NoSectionError(section_name)
            return {
                'memcache_servers': memcache_servers,
                'memcache_max_connections': memcache_max_connections
            }

        def read(self, path):
            return True

        def get(self, section, option):
            if _section == section:
                if option == 'memcache_servers':
                    if _srvs == 'error':
                        raise NoOptionError(option, section)
                    return _srvs
                elif option in ('memcache_max_connections',
                                'max_connections'):
                    if _maxc == 'error':
                        raise NoOptionError(option, section)
                    return _maxc
                elif option == 'item_size_warning_threshold':
                    if _warn_threshold == 'error':
                        raise NoOptionError(option, section)
                    return _warn_threshold
                else:
                    raise NoOptionError(option, section)
            else:
                raise NoSectionError(option)

    return SetConfigParser


def start_response(*args):
    pass


class TestLoadMemcache(unittest.TestCase):
    def setUp(self):
        self.logger = debug_logger()

    def test_conf_default_read(self):
        with mock.patch.object(memcached, 'ConfigParser', ExcConfigParser):
            for d in ({},
                      {'memcache_servers': '6.7.8.9:10'},
                      {'memcache_max_connections': '30'},
                      {'item_size_warning_threshold': 75},
                      {'memcache_servers': '6.7.8.9:10',
                       'item_size_warning_threshold': '75'},
                      {'item_size_warning_threshold': '75',
                       'memcache_max_connections': '30'},
                      ):
                with self.assertRaises(Exception) as catcher:
                    memcached.load_memcache(d, self.logger)
                self.assertEqual(
                    str(catcher.exception),
                    "read called with '/etc/swift/memcache.conf'")

    def test_conf_set_no_read(self):
        with mock.patch.object(memcached, 'ConfigParser', ExcConfigParser):
            exc = None
            try:
                memcached.load_memcache({
                    'memcache_servers': '1.2.3.4:5',
                    'memcache_max_connections': '30',
                    'item_size_warning_threshold': '80'

                }, self.logger)
            except Exception as err:
                exc = err
        self.assertIsNone(exc)

    def test_conf_default(self):
        with mock.patch.object(memcached, 'ConfigParser', EmptyConfigParser):
            memcache = memcached.load_memcache({}, self.logger)
        self.assertEqual(memcache.memcache_servers, ['127.0.0.1:11211'])
        self.assertEqual(
            memcache._client_cache['127.0.0.1:11211'].max_size, 2)
        self.assertEqual(memcache.item_size_warning_threshold, -1)

    def test_conf_inline(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            memcache = memcached.load_memcache({
                'memcache_servers': '6.7.8.9:10',
                'memcache_max_connections': '5',
                'item_size_warning_threshold': '75'
            }, self.logger)
        self.assertEqual(memcache.memcache_servers, ['6.7.8.9:10'])
        self.assertEqual(
            memcache._client_cache['6.7.8.9:10'].max_size, 5)
        self.assertEqual(memcache.item_size_warning_threshold, 75)

    def test_conf_inline_ratelimiting(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            memcache = memcached.load_memcache({
                'error_suppression_limit': '5',
                'error_suppression_interval': '2.5',
            }, self.logger)
        self.assertEqual(memcache._error_limit_count, 5)
        self.assertEqual(memcache._error_limit_time, 2.5)
        self.assertEqual(memcache._error_limit_duration, 2.5)

    def test_conf_inline_tls(self):
        fake_context = mock.Mock()
        with mock.patch.object(ssl, 'create_default_context',
                               return_value=fake_context):
            with mock.patch.object(memcached, 'ConfigParser',
                                   get_config_parser()):
                memcached.load_memcache({
                    'tls_enabled': 'true',
                    'tls_cafile': 'cafile',
                    'tls_certfile': 'certfile',
                    'tls_keyfile': 'keyfile',
                }, self.logger)
            ssl.create_default_context.assert_called_with(cafile='cafile')
            fake_context.load_cert_chain.assert_called_with('certfile',
                                                            'keyfile')

    def test_conf_extra_no_section(self):
        with mock.patch.object(memcached, 'ConfigParser',
                               get_config_parser(section='foobar')):
            memcache = memcached.load_memcache({}, self.logger)
        self.assertEqual(memcache.memcache_servers, ['127.0.0.1:11211'])
        self.assertEqual(
            memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_extra_no_option(self):
        replacement_parser = get_config_parser(
            memcache_servers='error',
            memcache_max_connections='error')
        with mock.patch.object(memcached, 'ConfigParser', replacement_parser):
            memcache = memcached.load_memcache({}, self.logger)
        self.assertEqual(memcache.memcache_servers, ['127.0.0.1:11211'])
        self.assertEqual(
            memcache._client_cache['127.0.0.1:11211'].max_size, 2)

    def test_conf_inline_other_max_conn(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            memcache = memcached.load_memcache({
                'memcache_servers': '6.7.8.9:10',
                'max_connections': '5'
            }, self.logger)
        self.assertEqual(memcache.memcache_servers, ['6.7.8.9:10'])
        self.assertEqual(
            memcache._client_cache['6.7.8.9:10'].max_size, 5)

    def test_conf_inline_bad_max_conn(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            memcache = memcached.load_memcache({
                'memcache_servers': '6.7.8.9:10',
                'max_connections': 'bad42',
            }, self.logger)
        self.assertEqual(memcache.memcache_servers, ['6.7.8.9:10'])
        self.assertEqual(
            memcache._client_cache['6.7.8.9:10'].max_size, 4)

    def test_conf_inline_bad_item_warning_threshold(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            with self.assertRaises(ValueError) as err:
                memcached.load_memcache({
                    'memcache_servers': '6.7.8.9:10',
                    'item_size_warning_threshold': 'bad42',
                }, self.logger)
        self.assertIn('invalid literal for int() with base 10:',
                      str(err.exception))

    def test_conf_from_extra_conf(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            memcache = memcached.load_memcache({}, self.logger)
        self.assertEqual(memcache.memcache_servers, ['1.2.3.4:5'])
        self.assertEqual(
            memcache._client_cache['1.2.3.4:5'].max_size, 4)

    def test_conf_from_extra_conf_bad_max_conn(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser(
                memcache_max_connections='bad42')):
            memcache = memcached.load_memcache({}, self.logger)
        self.assertEqual(memcache.memcache_servers, ['1.2.3.4:5'])
        self.assertEqual(
            memcache._client_cache['1.2.3.4:5'].max_size, 2)

    def test_conf_from_inline_and_maxc_from_extra_conf(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            memcache = memcached.load_memcache({
                'memcache_servers': '6.7.8.9:10'}, self.logger)
        self.assertEqual(memcache.memcache_servers, ['6.7.8.9:10'])
        self.assertEqual(
            memcache._client_cache['6.7.8.9:10'].max_size, 4)

    def test_conf_from_inline_and_sers_from_extra_conf(self):
        with mock.patch.object(memcached, 'ConfigParser', get_config_parser()):
            memcache = memcached.load_memcache({
                'memcache_servers': '6.7.8.9:10',
                'memcache_max_connections': '42',
            }, self.logger)
        self.assertEqual(memcache.memcache_servers, ['6.7.8.9:10'])
        self.assertEqual(
            memcache._client_cache['6.7.8.9:10'].max_size, 42)


if __name__ == '__main__':
    unittest.main()
