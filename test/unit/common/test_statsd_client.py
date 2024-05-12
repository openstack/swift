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
import errno
import random
import re
import socket
import sys
import threading
import time
import unittest
import warnings

import mock
import six
from six.moves.queue import Queue, Empty
from mock import patch


from swift.common import utils
from swift.common import statsd_client
from swift.common.statsd_client import StatsdClient

from test.debug_logger import debug_logger
from swift.common.swob import Response


class MockUdpSocket(object):
    def __init__(self, sendto_errno=None):
        self.sent = []
        self.sendto_errno = sendto_errno

    def sendto(self, data, target):
        if self.sendto_errno:
            raise socket.error(self.sendto_errno,
                               'test errno %s' % self.sendto_errno)
        self.sent.append((data, target))
        return len(data)

    def close(self):
        pass


class BaseTestStasdClient(unittest.TestCase):
    def setUp(self):
        self.getaddrinfo_calls = []

        def fake_getaddrinfo(host, port, *args):
            self.getaddrinfo_calls.append((host, port))
            # this is what a real getaddrinfo('localhost', port,
            # socket.AF_INET) returned once
            return [(socket.AF_INET,      # address family
                     socket.SOCK_STREAM,  # socket type
                     socket.IPPROTO_TCP,  # socket protocol
                     '',                  # canonical name,
                     ('127.0.0.1', port)),  # socket address
                    (socket.AF_INET,
                     socket.SOCK_DGRAM,
                     socket.IPPROTO_UDP,
                     '',
                     ('127.0.0.1', port))]

        self.real_getaddrinfo = statsd_client.socket.getaddrinfo
        self.getaddrinfo_patcher = mock.patch.object(
            statsd_client.socket, 'getaddrinfo', fake_getaddrinfo)
        self.mock_getaddrinfo = self.getaddrinfo_patcher.start()
        self.addCleanup(self.getaddrinfo_patcher.stop)


class TestStatsdClient(BaseTestStasdClient):
    def test_init_host(self):
        client = StatsdClient('myhost', 1234)
        self.assertEqual([('myhost', 1234)], self.getaddrinfo_calls)
        client1 = statsd_client.get_statsd_client(
            conf={'log_statsd_host': 'myhost1',
                  'log_statsd_port': 1235})
        with mock.patch.object(client, '_open_socket') as mock_open:
            self.assertIs(client.increment('tunafish'),
                          mock_open.return_value.sendto.return_value)
        self.assertEqual(mock_open.mock_calls, [
            mock.call(),
            mock.call().sendto(b'tunafish:1|c', ('myhost', 1234)),
            mock.call().close(),
        ])
        with mock.patch.object(client1, '_open_socket') as mock_open1:
            self.assertIs(client1.increment('tunafish'),
                          mock_open1.return_value.sendto.return_value)
        self.assertEqual(mock_open1.mock_calls, [
            mock.call(),
            mock.call().sendto(b'tunafish:1|c', ('myhost1', 1235)),
            mock.call().close(),
        ])

    def test_init_host_is_none(self):
        client = StatsdClient(None, None)
        client1 = statsd_client.get_statsd_client(conf=None,
                                                  logger=None)
        self.assertIsNone(client._host)
        self.assertIsNone(client1._host)
        self.assertFalse(self.getaddrinfo_calls)
        with mock.patch.object(client, '_open_socket') as mock_open:
            self.assertIsNone(client.increment('tunafish'))
        self.assertFalse(mock_open.mock_calls)
        with mock.patch.object(client1, '_open_socket') as mock_open1:
            self.assertIsNone(client1.increment('tunafish'))
        self.assertFalse(mock_open1.mock_calls)
        self.assertFalse(self.getaddrinfo_calls)


class TestStatsdLogging(BaseTestStasdClient):
    def test_get_logger_statsd_client_not_specified(self):
        logger = utils.get_logger({}, 'some-name', log_route='some-route')
        # white-box construction validation:
        # there may be a client instance, but it won't send anything;
        # won't even open a socket
        self.assertIsInstance(logger.logger.statsd_client,
                              statsd_client.StatsdClient)
        self.assertIsNone(logger.logger.statsd_client._host)
        with mock.patch.object(logger.logger.statsd_client,
                               '_open_socket') as mock_open:
            logger.increment('tunafish')
        self.assertFalse(mock_open.mock_calls)

    def test_get_logger_statsd_client_defaults(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com'},
                                  'some-name', log_route='some-route')
        # white-box construction validation
        self.assertIsInstance(logger.logger.statsd_client,
                              statsd_client.StatsdClient)
        self.assertEqual(logger.logger.statsd_client._host, 'some.host.com')
        self.assertEqual(logger.logger.statsd_client._port, 8125)
        self.assertEqual(logger.logger.statsd_client._prefix, 'some-name.')
        self.assertEqual(logger.logger.statsd_client._default_sample_rate, 1)

        logger2 = utils.get_logger(
            {'log_statsd_host': 'some.host.com'},
            'other-name', log_route='some-route',
            statsd_tail_prefix='some-name.more-specific')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'some-name.more-specific.')
        self.assertEqual(logger2.logger.statsd_client._prefix,
                         'some-name.more-specific.')

        # note: set_statsd_prefix is deprecated
        logger2 = utils.get_logger({'log_statsd_host': 'some.host.com'},
                                   'other-name', log_route='some-route')
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            logger.set_statsd_prefix('some-name.more-specific')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'some-name.more-specific.')
        self.assertEqual(logger2.logger.statsd_client._prefix,
                         'some-name.more-specific.')
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            logger.set_statsd_prefix('')
        self.assertEqual(logger.logger.statsd_client._prefix, '')
        self.assertEqual(logger2.logger.statsd_client._prefix, '')

    def test_get_logger_statsd_client_non_defaults(self):
        conf = {
            'log_statsd_host': 'another.host.com',
            'log_statsd_port': '9876',
            'log_statsd_default_sample_rate': '0.75',
            'log_statsd_sample_rate_factor': '0.81',
            'log_statsd_metric_prefix': 'tomato.sauce',
        }
        logger = utils.get_logger(conf, 'some-name', log_route='some-route')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'tomato.sauce.some-name.')

        logger = utils.get_logger(conf, 'other-name', log_route='some-route',
                                  statsd_tail_prefix='some-name.more-specific')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'tomato.sauce.some-name.more-specific.')

        # note: set_statsd_prefix is deprecated
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            logger.set_statsd_prefix('some-name.more-specific')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'tomato.sauce.some-name.more-specific.')
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            logger.set_statsd_prefix('')
        self.assertEqual(logger.logger.statsd_client._prefix, 'tomato.sauce.')
        self.assertEqual(logger.logger.statsd_client._host, 'another.host.com')
        self.assertEqual(logger.logger.statsd_client._port, 9876)
        self.assertEqual(logger.logger.statsd_client._default_sample_rate,
                         0.75)
        self.assertEqual(logger.logger.statsd_client._sample_rate_factor,
                         0.81)

    def test_statsd_set_prefix_deprecation(self):
        conf = {'log_statsd_host': 'another.host.com'}

        with warnings.catch_warnings(record=True) as cm:
            if six.PY2:
                getattr(utils, '__warningregistry__', {}).clear()
            warnings.resetwarnings()
            warnings.simplefilter('always', DeprecationWarning)
            logger = utils.get_logger(
                conf, 'some-name', log_route='some-route')
            logger.logger.statsd_client.set_prefix('some-name.more-specific')
        msgs = [str(warning.message)
                for warning in cm
                if str(warning.message).startswith('set_prefix')]
        self.assertEqual(
            ['set_prefix() is deprecated; use the ``tail_prefix`` argument of '
             'the constructor when instantiating the class instead.'],
            msgs)

        with warnings.catch_warnings(record=True) as cm:
            warnings.resetwarnings()
            warnings.simplefilter('always', DeprecationWarning)
            logger = utils.get_logger(
                conf, 'some-name', log_route='some-route')
            logger.set_statsd_prefix('some-name.more-specific')
        msgs = [str(warning.message)
                for warning in cm
                if str(warning.message).startswith('set_statsd_prefix')]
        self.assertEqual(
            ['set_statsd_prefix() is deprecated; use the '
             '``statsd_tail_prefix`` argument to ``get_logger`` instead.'],
            msgs)

    def test_ipv4_or_ipv6_hostname_defaults_to_ipv4(self):
        def stub_getaddrinfo_both_ipv4_and_ipv6(host, port, family, *rest):
            if family == socket.AF_INET:
                return [(socket.AF_INET, 'blah', 'blah', 'blah',
                        ('127.0.0.1', int(port)))]
            elif family == socket.AF_INET6:
                # Implemented so an incorrectly ordered implementation (IPv6
                # then IPv4) would realistically fail.
                return [(socket.AF_INET6, 'blah', 'blah', 'blah',
                        ('::1', int(port), 0, 0))]

        with mock.patch.object(statsd_client.socket, 'getaddrinfo',
                               new=stub_getaddrinfo_both_ipv4_and_ipv6):
            logger = utils.get_logger({
                'log_statsd_host': 'localhost',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        client = logger.logger.statsd_client

        self.assertEqual(client._sock_family, socket.AF_INET)
        self.assertEqual(client._target, ('localhost', 9876))

        got_sock = client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET)

    def test_ipv4_instantiation_and_socket_creation(self):
        logger = utils.get_logger({
            'log_statsd_host': '127.0.0.1',
            'log_statsd_port': '9876',
        }, 'some-name', log_route='some-route')
        statsd_client = logger.logger.statsd_client

        self.assertEqual(statsd_client._sock_family, socket.AF_INET)
        self.assertEqual(statsd_client._target, ('127.0.0.1', 9876))

        got_sock = statsd_client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET)

    def test_ipv6_instantiation_and_socket_creation(self):
        # We have to check the given hostname or IP for IPv4/IPv6 on logger
        # instantiation so we don't call getaddrinfo() too often and don't have
        # to call bind() on our socket to detect IPv4/IPv6 on every send.
        #
        # This test patches over the existing mock. If we just stop the
        # existing mock, then unittest.exit() blows up, but stacking
        # real-fake-fake works okay.
        calls = []

        def fake_getaddrinfo(host, port, family, *args):
            calls.append(family)
            if len(calls) == 1:
                raise socket.gaierror
            # this is what a real getaddrinfo('::1', port,
            # socket.AF_INET6) returned once
            return [(socket.AF_INET6,
                     socket.SOCK_STREAM,
                     socket.IPPROTO_TCP,
                     '', ('::1', port, 0, 0)),
                    (socket.AF_INET6,
                     socket.SOCK_DGRAM,
                     socket.IPPROTO_UDP,
                     '',
                     ('::1', port, 0, 0))]

        with mock.patch.object(statsd_client.socket,
                               'getaddrinfo', fake_getaddrinfo):
            logger = utils.get_logger({
                'log_statsd_host': '::1',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        client = logger.logger.statsd_client
        self.assertEqual([socket.AF_INET, socket.AF_INET6], calls)
        self.assertEqual(client._sock_family, socket.AF_INET6)
        self.assertEqual(client._target, ('::1', 9876, 0, 0))

        got_sock = client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET6)

    def test_bad_hostname_instantiation(self):
        stub_err = statsd_client.socket.gaierror('whoops')
        with mock.patch.object(statsd_client.socket, 'getaddrinfo',
                               side_effect=stub_err):
            logger = utils.get_logger({
                'log_statsd_host': 'i-am-not-a-hostname-or-ip',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        client = logger.logger.statsd_client

        self.assertEqual(client._sock_family, socket.AF_INET)
        self.assertEqual(client._target,
                         ('i-am-not-a-hostname-or-ip', 9876))

        got_sock = client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET)
        # Maybe the DNS server gets fixed in a bit and it starts working... or
        # maybe the DNS record hadn't propagated yet.  In any case, failed
        # statsd sends will warn in the logs until the DNS failure or invalid
        # IP address in the configuration is fixed.

    def test_sending_ipv6(self):
        def fake_getaddrinfo(host, port, *args):
            # this is what a real getaddrinfo('::1', port,
            # socket.AF_INET6) returned once
            return [(socket.AF_INET6,
                     socket.SOCK_STREAM,
                     socket.IPPROTO_TCP,
                     '', ('::1', port, 0, 0)),
                    (socket.AF_INET6,
                     socket.SOCK_DGRAM,
                     socket.IPPROTO_UDP,
                     '',
                     ('::1', port, 0, 0))]

        with mock.patch.object(statsd_client.socket, 'getaddrinfo',
                               fake_getaddrinfo):
            logger = utils.get_logger({
                'log_statsd_host': '::1',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        client = logger.logger.statsd_client

        fl = debug_logger()
        client.logger = fl
        mock_socket = MockUdpSocket()

        client._open_socket = lambda *_: mock_socket
        logger.increment('tunafish')
        self.assertEqual(fl.get_lines_for_level('warning'), [])
        self.assertEqual(mock_socket.sent,
                         [(b'some-name.tunafish:1|c', ('::1', 9876, 0, 0))])

    def test_no_exception_when_cant_send_udp_packet(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com'})
        client = logger.logger.statsd_client
        fl = debug_logger()
        client.logger = fl
        mock_socket = MockUdpSocket(sendto_errno=errno.EPERM)
        client._open_socket = lambda *_: mock_socket
        logger.increment('tunafish')
        expected = ["Error sending UDP message to ('some.host.com', 8125): "
                    "[Errno 1] test errno 1"]
        self.assertEqual(fl.get_lines_for_level('warning'), expected)

    def test_sample_rates(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com'})

        mock_socket = MockUdpSocket()
        # encapsulation? what's that?
        client = logger.logger.statsd_client
        self.assertTrue(client.random is random.random)

        client._open_socket = lambda *_: mock_socket
        client.random = lambda: 0.50001

        self.assertIsNone(logger.increment('tribbles', sample_rate=0.5))
        self.assertEqual(len(mock_socket.sent), 0)

        client.random = lambda: 0.49999
        rv = logger.increment('tribbles', sample_rate=0.5)
        self.assertIsInstance(rv, int)
        self.assertEqual(len(mock_socket.sent), 1)

        payload = mock_socket.sent[0][0]
        self.assertTrue(payload.endswith(b"|@0.5"))

    def test_sample_rates_with_sample_rate_factor(self):
        logger = utils.get_logger({
            'log_statsd_host': 'some.host.com',
            'log_statsd_default_sample_rate': '0.82',
            'log_statsd_sample_rate_factor': '0.91',
        })
        effective_sample_rate = 0.82 * 0.91

        mock_socket = MockUdpSocket()
        # encapsulation? what's that?
        client = logger.logger.statsd_client
        self.assertTrue(client.random is random.random)

        client._open_socket = lambda *_: mock_socket
        client.random = lambda: effective_sample_rate + 0.001

        logger.increment('tribbles')
        self.assertEqual(len(mock_socket.sent), 0)

        client.random = lambda: effective_sample_rate - 0.001
        logger.increment('tribbles')
        self.assertEqual(len(mock_socket.sent), 1)

        payload = mock_socket.sent[0][0]
        suffix = "|@%s" % effective_sample_rate
        if six.PY3:
            suffix = suffix.encode('utf-8')
        self.assertTrue(payload.endswith(suffix), payload)

        effective_sample_rate = 0.587 * 0.91
        client.random = lambda: effective_sample_rate - 0.001
        logger.increment('tribbles', sample_rate=0.587)
        self.assertEqual(len(mock_socket.sent), 2)

        payload = mock_socket.sent[1][0]
        suffix = "|@%s" % effective_sample_rate
        if six.PY3:
            suffix = suffix.encode('utf-8')
        self.assertTrue(payload.endswith(suffix), payload)

    def test_timing_stats(self):
        class MockController(object):
            def __init__(self, status):
                self.status = status
                self.logger = self
                self.args = ()
                self.called = 'UNKNOWN'

            def timing_since(self, *args):
                self.called = 'timing'
                self.args = args

        @utils.timing_stats()
        def METHOD(controller):
            return Response(status=controller.status)

        mock_controller = MockController(200)
        METHOD(mock_controller)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(400)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(404)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(412)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(416)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(500)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.errors.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(507)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.errors.timing')
        self.assertTrue(mock_controller.args[1] > 0)

    def test_memcached_timing_stats(self):
        class MockMemcached(object):
            def __init__(self):
                self.logger = self
                self.args = ()
                self.called = 'UNKNOWN'

            def timing_since(self, *args):
                self.called = 'timing'
                self.args = args

        @utils.memcached_timing_stats()
        def set(cache):
            pass

        @utils.memcached_timing_stats()
        def get(cache):
            pass

        mock_cache = MockMemcached()
        with patch('time.time',) as mock_time:
            mock_time.return_value = 1000.99
            set(mock_cache)
            self.assertEqual(mock_cache.called, 'timing')
            self.assertEqual(len(mock_cache.args), 2)
            self.assertEqual(mock_cache.args[0], 'memcached.set.timing')
            self.assertEqual(mock_cache.args[1], 1000.99)
            mock_time.return_value = 2000.99
            get(mock_cache)
            self.assertEqual(mock_cache.called, 'timing')
            self.assertEqual(len(mock_cache.args), 2)
            self.assertEqual(mock_cache.args[0], 'memcached.get.timing')
            self.assertEqual(mock_cache.args[1], 2000.99)


class TestStatsdLoggingDelegation(unittest.TestCase):

    def setUp(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        self.port = self.sock.getsockname()[1]
        self.queue = Queue()
        self.reader_thread = threading.Thread(target=self.statsd_reader)
        self.reader_thread.daemon = True
        self.reader_thread.start()

    def tearDown(self):
        # The "no-op when disabled" test doesn't set up a real logger, so
        # create one here so we can tell the reader thread to stop.
        if not getattr(self, 'logger', None):
            self.logger = utils.get_logger({
                'log_statsd_host': 'localhost',
                'log_statsd_port': str(self.port),
            }, 'some-name')
        self.logger.increment('STOP')
        self.reader_thread.join(timeout=4)
        self.sock.close()
        del self.logger

    def statsd_reader(self):
        while True:
            try:
                payload = self.sock.recv(4096)
                if payload and b'STOP' in payload:
                    return 42
                self.queue.put(payload)
            except Exception as e:
                sys.stderr.write('statsd_reader thread: %r' % (e,))
                break

    def _send_and_get(self, sender_fn, *args, **kwargs):
        """
        Because the client library may not actually send a packet with
        sample_rate < 1, we keep trying until we get one through.
        """
        got = None
        while not got:
            sender_fn(*args, **kwargs)
            try:
                got = self.queue.get(timeout=0.5)
            except Empty:
                pass
        return got

    def assertStat(self, expected, sender_fn, *args, **kwargs):
        got = self._send_and_get(sender_fn, *args, **kwargs)
        if six.PY3:
            got = got.decode('utf-8')
        return self.assertEqual(expected, got)

    def assertStatMatches(self, expected_regexp, sender_fn, *args, **kwargs):
        got = self._send_and_get(sender_fn, *args, **kwargs)
        if six.PY3:
            got = got.decode('utf-8')
        return self.assertTrue(re.search(expected_regexp, got),
                               [got, expected_regexp])

    def test_methods_are_no_ops_when_not_enabled(self):
        logger = utils.get_logger({
            # No "log_statsd_host" means "disabled"
            'log_statsd_port': str(self.port),
        }, 'some-name')
        # Delegate methods are no-ops
        self.assertIsNone(logger.update_stats('foo', 88))
        self.assertIsNone(logger.update_stats('foo', 88, 0.57))
        self.assertIsNone(logger.update_stats('foo', 88,
                                              sample_rate=0.61))
        self.assertIsNone(logger.increment('foo'))
        self.assertIsNone(logger.increment('foo', 0.57))
        self.assertIsNone(logger.increment('foo', sample_rate=0.61))
        self.assertIsNone(logger.decrement('foo'))
        self.assertIsNone(logger.decrement('foo', 0.57))
        self.assertIsNone(logger.decrement('foo', sample_rate=0.61))
        self.assertIsNone(logger.timing('foo', 88.048))
        self.assertIsNone(logger.timing('foo', 88.57, 0.34))
        self.assertIsNone(logger.timing('foo', 88.998, sample_rate=0.82))
        self.assertIsNone(logger.timing_since('foo', 8938))
        self.assertIsNone(logger.timing_since('foo', 8948, 0.57))
        self.assertIsNone(logger.timing_since('foo', 849398,
                                              sample_rate=0.61))
        # Now, the queue should be empty (no UDP packets sent)
        self.assertRaises(Empty, self.queue.get_nowait)

    def test_delegate_methods_with_no_default_sample_rate(self):
        self.logger = utils.get_logger({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
        }, 'some-name')
        self.assertStat('some-name.some.counter:1|c', self.logger.increment,
                        'some.counter')
        self.assertStat('some-name.some.counter:-1|c', self.logger.decrement,
                        'some.counter')
        self.assertStat('some-name.some.operation:4900.0|ms',
                        self.logger.timing, 'some.operation', 4.9 * 1000)
        self.assertStatMatches(r'some-name\.another\.operation:\d+\.\d+\|ms',
                               self.logger.timing_since, 'another.operation',
                               time.time())
        self.assertStat('some-name.another.counter:42|c',
                        self.logger.update_stats, 'another.counter', 42)

        # Each call can override the sample_rate (also, bonus prefix test)
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.logger.set_statsd_prefix('pfx')
        self.assertStat('pfx.some.counter:1|c|@0.972', self.logger.increment,
                        'some.counter', sample_rate=0.972)
        self.assertStat('pfx.some.counter:-1|c|@0.972', self.logger.decrement,
                        'some.counter', sample_rate=0.972)
        self.assertStat('pfx.some.operation:4900.0|ms|@0.972',
                        self.logger.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.972)
        self.assertStat(
            'pfx.some.hi-res.operation:3141.5927|ms|@0.367879441171',
            self.logger.timing, 'some.hi-res.operation',
            3.141592653589793 * 1000, sample_rate=0.367879441171)
        self.assertStatMatches(r'pfx\.another\.op:\d+\.\d+\|ms|@0.972',
                               self.logger.timing_since, 'another.op',
                               time.time(), sample_rate=0.972)
        self.assertStat('pfx.another.counter:3|c|@0.972',
                        self.logger.update_stats, 'another.counter', 3,
                        sample_rate=0.972)

        # Can override sample_rate with non-keyword arg
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.logger.set_statsd_prefix('')
        self.assertStat('some.counter:1|c|@0.939', self.logger.increment,
                        'some.counter', 0.939)
        self.assertStat('some.counter:-1|c|@0.939', self.logger.decrement,
                        'some.counter', 0.939)
        self.assertStat('some.operation:4900.0|ms|@0.939',
                        self.logger.timing, 'some.operation',
                        4.9 * 1000, 0.939)
        self.assertStatMatches(r'another\.op:\d+\.\d+\|ms|@0.939',
                               self.logger.timing_since, 'another.op',
                               time.time(), 0.939)
        self.assertStat('another.counter:3|c|@0.939',
                        self.logger.update_stats, 'another.counter', 3, 0.939)

    def test_delegate_methods_with_default_sample_rate(self):
        self.logger = utils.get_logger({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_default_sample_rate': '0.93',
        }, 'pfx')
        self.assertStat('pfx.some.counter:1|c|@0.93', self.logger.increment,
                        'some.counter')
        self.assertStat('pfx.some.counter:-1|c|@0.93', self.logger.decrement,
                        'some.counter')
        self.assertStat('pfx.some.operation:4760.0|ms|@0.93',
                        self.logger.timing, 'some.operation', 4.76 * 1000)
        self.assertStatMatches(r'pfx\.another\.op:\d+\.\d+\|ms|@0.93',
                               self.logger.timing_since, 'another.op',
                               time.time())
        self.assertStat('pfx.another.counter:3|c|@0.93',
                        self.logger.update_stats, 'another.counter', 3)

        # Each call can override the sample_rate
        self.assertStat('pfx.some.counter:1|c|@0.9912', self.logger.increment,
                        'some.counter', sample_rate=0.9912)
        self.assertStat('pfx.some.counter:-1|c|@0.9912', self.logger.decrement,
                        'some.counter', sample_rate=0.9912)
        self.assertStat('pfx.some.operation:4900.0|ms|@0.9912',
                        self.logger.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.9912)
        self.assertStatMatches(r'pfx\.another\.op:\d+\.\d+\|ms|@0.9912',
                               self.logger.timing_since, 'another.op',
                               time.time(), sample_rate=0.9912)
        self.assertStat('pfx.another.counter:3|c|@0.9912',
                        self.logger.update_stats, 'another.counter', 3,
                        sample_rate=0.9912)

        # Can override sample_rate with non-keyword arg
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.logger.set_statsd_prefix('')
        self.assertStat('some.counter:1|c|@0.987654', self.logger.increment,
                        'some.counter', 0.987654)
        self.assertStat('some.counter:-1|c|@0.987654', self.logger.decrement,
                        'some.counter', 0.987654)
        self.assertStat('some.operation:4900.0|ms|@0.987654',
                        self.logger.timing, 'some.operation',
                        4.9 * 1000, 0.987654)
        self.assertStatMatches(r'another\.op:\d+\.\d+\|ms|@0.987654',
                               self.logger.timing_since, 'another.op',
                               time.time(), 0.987654)
        self.assertStat('another.counter:3|c|@0.987654',
                        self.logger.update_stats, 'another.counter',
                        3, 0.987654)

    def test_delegate_methods_with_metric_prefix(self):
        self.logger = utils.get_logger({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'alpha.beta',
        }, 'pfx')
        self.assertStat('alpha.beta.pfx.some.counter:1|c',
                        self.logger.increment, 'some.counter')
        self.assertStat('alpha.beta.pfx.some.counter:-1|c',
                        self.logger.decrement, 'some.counter')
        self.assertStat('alpha.beta.pfx.some.operation:4760.0|ms',
                        self.logger.timing, 'some.operation', 4.76 * 1000)
        self.assertStatMatches(
            r'alpha\.beta\.pfx\.another\.op:\d+\.\d+\|ms',
            self.logger.timing_since, 'another.op', time.time())
        self.assertStat('alpha.beta.pfx.another.counter:3|c',
                        self.logger.update_stats, 'another.counter', 3)

        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.logger.set_statsd_prefix('')
        self.assertStat('alpha.beta.some.counter:1|c|@0.9912',
                        self.logger.increment, 'some.counter',
                        sample_rate=0.9912)
        self.assertStat('alpha.beta.some.counter:-1|c|@0.9912',
                        self.logger.decrement, 'some.counter', 0.9912)
        self.assertStat('alpha.beta.some.operation:4900.0|ms|@0.9912',
                        self.logger.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.9912)
        self.assertStatMatches(
            r'alpha\.beta\.another\.op:\d+\.\d+\|ms|@0.9912',
            self.logger.timing_since, 'another.op',
            time.time(), sample_rate=0.9912)
        self.assertStat('alpha.beta.another.counter:3|c|@0.9912',
                        self.logger.update_stats, 'another.counter', 3,
                        sample_rate=0.9912)


class TestModuleFunctions(unittest.TestCase):
    def setUp(self):
        self.logger = debug_logger()

    def test_get_statsd_client_defaults(self):
        # no options configured
        client = statsd_client.get_statsd_client({})
        self.assertIsInstance(client, StatsdClient)
        self.assertIsNone(client._host)
        self.assertEqual(8125, client._port)
        self.assertEqual('', client._base_prefix)
        self.assertEqual('', client._prefix)
        self.assertEqual(1.0, client._default_sample_rate)
        self.assertEqual(1.0, client._sample_rate_factor)
        self.assertIsNone(client.logger)

    def test_get_statsd_client_options(self):
        # legacy options...
        conf = {
            'log_statsd_host': 'example.com',
            'log_statsd_port': '6789',
            'log_statsd_metric_prefix': 'banana',
            'log_statsd_default_sample_rate': '3.3',
            'log_statsd_sample_rate_factor': '4.4',
            'log_junk': 'ignored',
        }
        client = statsd_client.get_statsd_client(
            conf, tail_prefix='milkshake', logger=self.logger)
        self.assertIsInstance(client, StatsdClient)
        self.assertEqual('example.com', client._host)
        self.assertEqual(6789, client._port)
        self.assertEqual('banana', client._base_prefix)
        self.assertEqual('banana.milkshake.', client._prefix)
        self.assertEqual(3.3, client._default_sample_rate)
        self.assertEqual(4.4, client._sample_rate_factor)
        self.assertEqual(self.logger, client.logger)
        warn_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual([], warn_lines)
