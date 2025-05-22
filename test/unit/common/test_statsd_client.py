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

from unittest import mock
from queue import Queue, Empty


from swift.common import statsd_client
from swift.common.statsd_client import StatsdClient, get_statsd_client

from test.debug_logger import debug_logger


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


class BaseTestStatsdClient(unittest.TestCase):
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
        self.logger = debug_logger()


class TestStatsdClient(BaseTestStatsdClient):
    """
    Tests here construct a StatsdClient directly.
    """
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

    def test_statsd_set_prefix_deprecation(self):
        with warnings.catch_warnings(record=True) as cm:
            warnings.resetwarnings()
            warnings.simplefilter('always', DeprecationWarning)
            client = StatsdClient(None, None)
            client.set_prefix('some-name.more-specific')
        msgs = [str(warning.message)
                for warning in cm
                if str(warning.message).startswith('set_prefix')]
        self.assertEqual(
            ['set_prefix() is deprecated; use the ``tail_prefix`` argument of '
             'the constructor when instantiating the class instead.'],
            msgs)
        self.assertEqual('some-name.more-specific.', client._prefix)


class TestGetStatsdClientConfParsing(BaseTestStatsdClient):
    """
    Tests here use get_statsd_client to make a StatsdClient.
    """
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
        with mock.patch.object(client, '_open_socket') as mock_open:
            client.increment('tunafish')
        self.assertFalse(mock_open.mock_calls)

    def test_get_statsd_client_options(self):
        # legacy options...
        conf = {
            'log_statsd_host': 'example.com',
            'log_statsd_port': '6789',
            'log_statsd_metric_prefix': 'banana',
            'log_statsd_default_sample_rate': '3.3',
            'log_statsd_sample_rate_factor': '4.4',
            'log_junk': 'ignored',
            'statsd_label_mode': 'dogstatsd',  # ignored
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

    def test_emit_legacy(self):
        conf = {
            'log_statsd_host': 'myhost',
            'log_statsd_port': '1234',
        }
        client = statsd_client.get_statsd_client(conf)
        with mock.patch.object(client, '_open_socket') as mock_open:
            client.increment('tunafish')
        self.assertEqual(mock_open.mock_calls, [
            mock.call(),
            mock.call().sendto(b'tunafish:1|c', ('myhost', 1234)),
            mock.call().close(),
        ])

        conf = {
            'log_statsd_host': 'myhost',
            'log_statsd_port': '1234',
            'statsd_emit_legacy': 'False',
        }
        client = statsd_client.get_statsd_client(conf)
        with mock.patch.object(client, '_open_socket') as mock_open:
            client.increment('tunafish')
        self.assertEqual(mock_open.mock_calls, [])

    def test_legacy_client_does_not_support_labels_kwarg(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': '123451',
            'statsd_label_mode': 'dogstatsd',
        }
        client = statsd_client.get_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        with mock.patch.object(client, '_send_line') as mocked:
            # legacy client accepts sample_rate kwarg as positional argument
            # for backwards compat as demonstrated in other tests
            client.random = lambda: 0.4
            client.increment('metric', 0.5)
            # but will never accept labels kwarg
            with self.assertRaises(TypeError):
                client.increment('metric', labels=labels)
        self.assertEqual(
            [mock.call('metric:1|c|@0.5')],
            mocked.call_args_list)


class TestGetLabeledStatsdClientConfParsing(BaseTestStatsdClient):
    """
    Tests here use get_labeled_statsd_client to make a LabeledStatsdClient.
    """
    def test_conf_defaults(self):
        # no options configured
        client = statsd_client.get_labeled_statsd_client({})
        self.assertIsInstance(client, statsd_client.LabeledStatsdClient)
        self.assertIsNone(client._host)
        self.assertEqual(8125, client._port)
        self.assertEqual(1.0, client._default_sample_rate)
        self.assertEqual(1.0, client._sample_rate_factor)
        self.assertIsNone(client.logger)
        with mock.patch.object(client, '_open_socket') as mock_open:
            # because legacy statsd.increment last pos arg was sample_rate
            # we're always explicit with labels kwarg
            client.increment('tunafish', labels={})
        self.assertFalse(mock_open.mock_calls)

    def test_conf_non_defaults(self):
        # legacy options...
        conf = {
            'log_statsd_host': 'example.com',
            'log_statsd_port': '6789',
            'log_statsd_default_sample_rate': '3.3',
            'log_statsd_sample_rate_factor': '4.4',
            'log_junk': 'ignored',
            'statsd_emit_legacy': 'False',  # ignored
        }
        client = statsd_client.get_labeled_statsd_client(
            conf, logger=self.logger)
        self.assertIsInstance(client, statsd_client.LabeledStatsdClient)
        self.assertEqual('example.com', client._host)
        self.assertEqual(6789, client._port)
        self.assertEqual(3.3, client._default_sample_rate)
        self.assertEqual(4.4, client._sample_rate_factor)
        self.assertEqual(self.logger, client.logger)
        warn_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual([], warn_lines)

    def test_invalid_label_mode(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': '1234',
            'statsd_label_mode': 'invalid',
        }
        with self.assertRaises(ValueError) as cm:
            statsd_client.get_labeled_statsd_client(conf, self.logger)
        self.assertIn("unknown statsd_label_mode 'invalid'", str(cm.exception))

    def test_valid_label_mode(self):
        conf = {'statsd_label_mode': 'dogstatsd'}
        logger = debug_logger(log_route='my-log-route')
        client = statsd_client.get_labeled_statsd_client(conf, logger)
        self.assertEqual(statsd_client.dogstatsd, client.label_formatter)
        log_lines = logger.get_lines_for_level('debug')
        self.assertEqual(1, len(log_lines))
        self.assertEqual(
            'Labeled statsd mode: dogstatsd (my-log-route)', log_lines[0])

    def test_weird_invalid_attrname_label_mode(self):
        conf = {'statsd_label_mode': '__class__'}
        with self.assertRaises(ValueError) as cm:
            statsd_client.get_labeled_statsd_client(conf, self.logger)
        self.assertIn("unknown statsd_label_mode '__class__'",
                      str(cm.exception))

    def test_disabled_by_default(self):
        conf = {}
        logger = debug_logger(log_route='my-log-route')
        client = statsd_client.get_labeled_statsd_client(conf, logger)
        self.assertIsNone(client.label_formatter)
        log_lines = logger.get_lines_for_level('debug')
        self.assertEqual(1, len(log_lines))
        self.assertEqual(
            'Labeled statsd mode: disabled (my-log-route)', log_lines[0])

    def test_label_must_be_kwarg(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': '123451',
            'statsd_label_mode': 'dogstatsd',
        }
        client = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        with mock.patch.object(client, '_send_line') as mocked:
            # labels can not be a positional arg
            with self.assertRaises(TypeError):
                client.increment('metric', labels)
            client.random = lambda: 0.4
            # order of kwargs does not matter
            client.increment('metric', sample_rate=0.5, labels=labels)
        self.assertEqual(
            [mock.call('metric:1|c|@0.5|#action:some,result:ok')],
            mocked.call_args_list)

    def test_label_values_to_str(self):
        # verify that simple non-str types can be passed as label values
        conf = {
            'log_statsd_host': 'myhost1',
            'log_statsd_port': 1235,
            'statsd_label_mode': 'librato',
        }
        client = statsd_client.get_labeled_statsd_client(conf)
        labels = {'bool': True, 'number': 42.1, 'null': None}
        with mock.patch.object(client, '_send_line') as mocked:
            client.update_stats('metric', '10', labels=labels)
        self.assertEqual(
            [mock.call('metric#bool=True,null=None,number=42.1:10|c')],
            mocked.call_args_list)

    def test_user_label(self):
        conf = {
            'log_statsd_host': 'myhost1',
            'log_statsd_port': 1235,
            'statsd_label_mode': 'librato',
            'statsd_user_label_foo': 'foo.bar.com',
        }
        client = statsd_client.get_labeled_statsd_client(conf)
        self.assertEqual({'user_foo': 'foo.bar.com'}, client.default_labels)
        with mock.patch.object(client, '_send_line') as mocked:
            client.update_stats('metric', '10', labels={'app': 'value'})
        self.assertEqual(
            [mock.call('metric#app=value,user_foo=foo.bar.com:10|c')],
            mocked.call_args_list)

    def test_user_label_overridden_by_call_label(self):
        conf = {
            'log_statsd_host': 'myhost1',
            'log_statsd_port': 1235,
            'statsd_label_mode': 'librato',
            'statsd_user_label_foo': 'foo',
        }
        client = statsd_client.get_labeled_statsd_client(conf)
        self.assertEqual({'user_foo': 'foo'}, client.default_labels)
        with mock.patch.object(client, '_send_line') as mocked:
            client.update_stats('metric', '10', labels={'user_foo': 'bar'})
        self.assertEqual(
            [mock.call('metric#user_foo=bar:10|c')],
            mocked.call_args_list)

    def test_user_label_sorting(self):
        conf = {
            'log_statsd_host': 'myhost1',
            'log_statsd_port': 1235,
            'statsd_label_mode': 'librato',
            'statsd_user_label_foo': 'middle',
        }
        labels = {'z': 'last', 'a': 'first'}
        client = statsd_client.get_labeled_statsd_client(conf)
        with mock.patch.object(client, '_send_line') as mocked:
            client.update_stats('metric', '10', labels=labels)
        self.assertEqual(
            [mock.call('metric#a=first,user_foo=middle,z=last:10|c')],
            mocked.call_args_list)

    def test_user_label_invalid_chars(self):
        invalid = ',|=[]:.'
        for c in invalid:
            user_label = 'statsd_user_label_foo%sbar' % c
            conf = {
                'log_statsd_host': 'myhost1',
                'log_statsd_port': 1235,
                'statsd_label_mode': 'librato',
                user_label: 'buz',
            }
            with self.assertRaises(ValueError) as ctx:
                statsd_client.get_labeled_statsd_client(conf)
            self.assertEqual("invalid character in statsd "
                             "user label configuration "
                             "'%s': '%s'" % (user_label, c),
                             str(ctx.exception))

    def test_user_label_value_invalid_chars(self):
        invalid = ',|=[]:'
        for c in invalid:
            label_value = 'bar%sbaz' % c
            conf = {
                'log_statsd_host': 'myhost1',
                'log_statsd_port': 1235,
                'statsd_label_mode': 'librato',
                'statsd_user_label_foo': label_value
            }
            with self.assertRaises(ValueError) as ctx:
                statsd_client.get_labeled_statsd_client(conf)
            self.assertEqual("invalid character in configuration "
                             "'statsd_user_label_foo' value "
                             "'%s': '%s'" % (label_value, c),
                             str(ctx.exception))


class CommonBaseTestsMixIn(object):

    # N.B. we use a MixIn here to help maintain/transfer the understanding that
    # the tests defined in this "MixIn" are run in multiple concrete TestCase
    # subclasses.  We can't inherit from TestCase ourselves because unittest
    # does not know how to skip abstract common base TestCases - although we
    # may explore alternatives in the future.
    def make_test_client(self, conf, tail_prefix='', **kwargs):
        """
        Concrete TestCase classes should implement this method and have the
        following attributes:
             * tail_prefix
             * expected_prefix_bytes
        """
        raise NotImplementedError()

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
            client = self.make_test_client({
                'log_statsd_host': 'localhost',
                'log_statsd_port': '9876',
            }, self.tail_prefix, logger=self.logger)

        self.assertEqual(client._sock_family, socket.AF_INET)
        self.assertEqual(client._target, ('localhost', 9876))

        got_sock = client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET)

    def test_ipv4_instantiation_and_socket_creation(self):
        client = self.make_test_client({
            'log_statsd_host': '127.0.0.1',
            'log_statsd_port': '9876',
        }, 'some-name', logger=self.logger)

        self.assertEqual(client._sock_family, socket.AF_INET)
        self.assertEqual(client._target, ('127.0.0.1', 9876))

        got_sock = client._open_socket()
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
            client = self.make_test_client({
                'log_statsd_host': '::1',
                'log_statsd_port': '9876',
            }, 'some-name', logger=self.logger)
        self.assertEqual([socket.AF_INET, socket.AF_INET6], calls)
        self.assertEqual(client._sock_family, socket.AF_INET6)
        self.assertEqual(client._target, ('::1', 9876, 0, 0))

        got_sock = client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET6)

    def test_bad_hostname_instantiation(self):
        stub_err = statsd_client.socket.gaierror('whoops')
        with mock.patch.object(statsd_client.socket, 'getaddrinfo',
                               side_effect=stub_err):
            client = self.make_test_client({
                'log_statsd_host': 'i-am-not-a-hostname-or-ip',
                'log_statsd_port': '9876',
            }, 'some-name', logger=self.logger)

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
            client = self.make_test_client({
                'log_statsd_host': '::1',
                'log_statsd_port': '9876',
            }, 'some-name', logger=self.logger)

        fl = debug_logger()
        client.logger = fl
        mock_socket = MockUdpSocket()

        client._open_socket = lambda *_: mock_socket
        client.increment('tunafish')
        self.assertEqual(fl.get_lines_for_level('warning'), [])
        self.assertEqual(mock_socket.sent,
                         [(self.expected_prefix_bytes + b'tunafish:1|c',
                           ('::1', 9876, 0, 0))])

    def test_no_exception_when_cant_send_udp_packet(self):
        client = self.make_test_client({'log_statsd_host': 'some.host.com'})
        fl = debug_logger()
        client.logger = fl
        mock_socket = MockUdpSocket(sendto_errno=errno.EPERM)
        client._open_socket = lambda *_: mock_socket
        client.increment('tunafish')
        expected = ["Error sending UDP message to ('some.host.com', 8125): "
                    "[Errno 1] test errno 1"]
        self.assertEqual(fl.get_lines_for_level('warning'), expected)

    def test_sample_rates(self):
        client = self.make_test_client({'log_statsd_host': 'some.host.com'})

        mock_socket = MockUdpSocket()
        self.assertTrue(client.random is random.random)

        client._open_socket = lambda *_: mock_socket
        client.random = lambda: 0.50001

        self.assertIsNone(client.increment('tribbles', sample_rate=0.5))
        self.assertFalse(mock_socket.sent)

        client.random = lambda: 0.49999
        rv = client.increment('tribbles', sample_rate=0.5)
        self.assertIsInstance(rv, int)
        self.assertEqual([(b"tribbles:1|c|@0.5", ('some.host.com', 8125))],
                         mock_socket.sent)

    def test_sample_rates_with_sample_rate_factor(self):
        client = self.make_test_client({
            'log_statsd_host': 'some.host.com',
            'log_statsd_default_sample_rate': '0.82',
            'log_statsd_sample_rate_factor': '0.91',
        })
        effective_sample_rate = 0.82 * 0.91

        mock_socket = MockUdpSocket()
        self.assertIs(client.random, random.random)

        client._open_socket = lambda *_: mock_socket
        client.random = lambda: effective_sample_rate + 0.001

        client.increment('tribbles')
        self.assertFalse(mock_socket.sent)

        client.random = lambda: effective_sample_rate - 0.001
        client.increment('tribbles')
        expected = ("tribbles:1|c|@%s" % effective_sample_rate).encode('utf-8')
        self.assertEqual([(expected, ('some.host.com', 8125))],
                         mock_socket.sent)

        # caller specifies non-default sample rate
        mock_socket = MockUdpSocket()
        effective_sample_rate = 0.587 * 0.91
        client.random = lambda: effective_sample_rate + 0.001
        client.increment('tribbles', sample_rate=0.587)
        self.assertFalse(mock_socket.sent)

        client.random = lambda: effective_sample_rate - 0.001
        client.increment('tribbles', sample_rate=0.587)
        expected = ("tribbles:1|c|@%s" % effective_sample_rate).encode('utf-8')
        self.assertEqual([(expected, ('some.host.com', 8125))],
                         mock_socket.sent)


class TestGetStatsdClient(BaseTestStatsdClient, CommonBaseTestsMixIn):
    """
    Tests here use get_statsd_client to make a LabeledStatsdClient.
    """
    tail_prefix = 'some-name'
    expected_prefix_bytes = ('%s.' % tail_prefix).encode()

    def make_test_client(self, conf, tail_prefix='', **kwargs):
        return statsd_client.get_statsd_client(conf, tail_prefix, **kwargs)


class TestGetLabeledStatsdClient(BaseTestStatsdClient, CommonBaseTestsMixIn):
    """
    Tests here use get_labeled_statsd_client to make a LabeledStatsdClient.
    """
    tail_prefix = None
    expected_prefix_bytes = b''

    def make_test_client(self, conf, _tail_prefix='', **kwargs):
        conf.setdefault('statsd_label_mode', 'dogstatsd')
        return statsd_client.get_labeled_statsd_client(conf, **kwargs)


class BaseTestStatsdClientOutput(unittest.TestCase):

    def setUp(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        self.port = self.sock.getsockname()[1]
        self.queue = Queue()
        self.reader_thread = threading.Thread(target=self.statsd_reader)
        self.reader_thread.daemon = True
        self.reader_thread.start()
        self.client = None

    def tearDown(self):
        # The "no-op when disabled" test doesn't set up a real client, so
        # create one here so we can tell the reader thread to stop.
        if not self.client:
            self.client = get_statsd_client({
                'log_statsd_host': 'localhost',
                'log_statsd_port': str(self.port),
            })
        self.client.increment('STOP')
        self.reader_thread.join(timeout=4)
        self.sock.close()

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
                got = self.queue.get(timeout=0.3)
            except Empty:
                pass
        return got.decode('utf-8')

    def assertStat(self, expected, sender_fn, *args, **kwargs):
        got = self._send_and_get(sender_fn, *args, **kwargs)
        return self.assertEqual(expected, got)

    def assertStatMatches(self, expected_regexp, sender_fn, *args, **kwargs):
        got = self._send_and_get(sender_fn, *args, **kwargs)
        return self.assertTrue(re.search(expected_regexp, got),
                               [got, expected_regexp])


class TestGetStatsdClientOutput(BaseTestStatsdClientOutput):
    """
    Tests here use get_statsd_client to make a StatsdClient.
    """
    def test_methods_are_no_ops_when_not_enabled(self):
        # *Don't* use self.client -- we want tearDown to create it
        client = get_statsd_client({
            # No "log_statsd_host" means "disabled"
            'log_statsd_port': str(self.port),
        }, 'some-name')
        self.assertIsNone(client.update_stats('foo', 88))
        self.assertIsNone(client.update_stats('foo', 88, 0.57))
        self.assertIsNone(client.update_stats('foo', 88,
                                              sample_rate=0.61))
        self.assertIsNone(client.increment('foo'))
        self.assertIsNone(client.increment('foo', 0.57))
        self.assertIsNone(client.increment('foo', sample_rate=0.61))
        self.assertIsNone(client.decrement('foo'))
        self.assertIsNone(client.decrement('foo', 0.57))
        self.assertIsNone(client.decrement('foo', sample_rate=0.61))
        self.assertIsNone(client.timing('foo', 88.048))
        self.assertIsNone(client.timing('foo', 88.57, 0.34))
        self.assertIsNone(client.timing('foo', 88.998, sample_rate=0.82))
        self.assertIsNone(client.timing_since('foo', 8938))
        self.assertIsNone(client.timing_since('foo', 8948, 0.57))
        self.assertIsNone(client.timing_since('foo', 849398,
                                              sample_rate=0.61))
        # Now, the queue should be empty (no UDP packets sent)
        self.assertRaises(Empty, self.queue.get_nowait)

    def test_methods_with_no_default_sample_rate(self):
        self.client = get_statsd_client({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'statsd_label_mode': 'disabled',  # ignored
        }, 'some-name')
        self.assertStat('some-name.some.counter:1|c', self.client.increment,
                        'some.counter')
        self.assertStat('some-name.some.counter:-1|c', self.client.decrement,
                        'some.counter')
        self.assertStat('some-name.some.operation:4900.0|ms',
                        self.client.timing, 'some.operation', 4.9 * 1000)
        self.assertStatMatches(r'some-name\.another\.operation:\d+\.\d+\|ms',
                               self.client.timing_since, 'another.operation',
                               time.time())
        self.assertStat('some-name.another.counter:42|c',
                        self.client.update_stats, 'another.counter', 42)

        # Each call can override the sample_rate (also, bonus prefix test)
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.client.set_prefix('pfx')
        self.assertStat('pfx.some.counter:1|c|@0.972', self.client.increment,
                        'some.counter', sample_rate=0.972)
        self.assertStat('pfx.some.counter:-1|c|@0.972', self.client.decrement,
                        'some.counter', sample_rate=0.972)
        self.assertStat('pfx.some.operation:4900.0|ms|@0.972',
                        self.client.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.972)
        self.assertStat(
            'pfx.some.hi-res.operation:3141.5927|ms|@0.367879441171',
            self.client.timing, 'some.hi-res.operation',
            3.141592653589793 * 1000, sample_rate=0.367879441171)
        self.assertStatMatches(r'pfx\.another\.op:\d+\.\d+\|ms|@0.972',
                               self.client.timing_since, 'another.op',
                               time.time(), sample_rate=0.972)
        self.assertStat('pfx.another.counter:3|c|@0.972',
                        self.client.update_stats, 'another.counter', 3,
                        sample_rate=0.972)

        # Can override sample_rate with non-keyword arg
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.client.set_prefix('')
        self.assertStat('some.counter:1|c|@0.939', self.client.increment,
                        'some.counter', 0.939)
        self.assertStat('some.counter:-1|c|@0.939', self.client.decrement,
                        'some.counter', 0.939)
        self.assertStat('some.operation:4900.0|ms|@0.939',
                        self.client.timing, 'some.operation',
                        4.9 * 1000, 0.939)
        self.assertStatMatches(r'another\.op:\d+\.\d+\|ms|@0.939',
                               self.client.timing_since, 'another.op',
                               time.time(), 0.939)
        self.assertStat('another.counter:3|c|@0.939',
                        self.client.update_stats, 'another.counter', 3, 0.939)

    def test_methods_with_default_sample_rate(self):
        self.client = get_statsd_client({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_default_sample_rate': '0.93',
        }, 'pfx')
        self.assertStat('pfx.some.counter:1|c|@0.93', self.client.increment,
                        'some.counter')
        self.assertStat('pfx.some.counter:-1|c|@0.93', self.client.decrement,
                        'some.counter')
        self.assertStat('pfx.some.operation:4760.0|ms|@0.93',
                        self.client.timing, 'some.operation', 4.76 * 1000)
        self.assertStatMatches(r'pfx\.another\.op:\d+\.\d+\|ms|@0.93',
                               self.client.timing_since, 'another.op',
                               time.time())
        self.assertStat('pfx.another.counter:3|c|@0.93',
                        self.client.update_stats, 'another.counter', 3)

        # Each call can override the sample_rate
        self.assertStat('pfx.some.counter:1|c|@0.9912', self.client.increment,
                        'some.counter', sample_rate=0.9912)
        self.assertStat('pfx.some.counter:-1|c|@0.9912', self.client.decrement,
                        'some.counter', sample_rate=0.9912)
        self.assertStat('pfx.some.operation:4900.0|ms|@0.9912',
                        self.client.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.9912)
        self.assertStatMatches(r'pfx\.another\.op:\d+\.\d+\|ms|@0.9912',
                               self.client.timing_since, 'another.op',
                               time.time(), sample_rate=0.9912)
        self.assertStat('pfx.another.counter:3|c|@0.9912',
                        self.client.update_stats, 'another.counter', 3,
                        sample_rate=0.9912)

        # Can override sample_rate with non-keyword arg
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.client.set_prefix('')
        self.assertStat('some.counter:1|c|@0.987654', self.client.increment,
                        'some.counter', 0.987654)
        self.assertStat('some.counter:-1|c|@0.987654', self.client.decrement,
                        'some.counter', 0.987654)
        self.assertStat('some.operation:4900.0|ms|@0.987654',
                        self.client.timing, 'some.operation',
                        4.9 * 1000, 0.987654)
        self.assertStatMatches(r'another\.op:\d+\.\d+\|ms|@0.987654',
                               self.client.timing_since, 'another.op',
                               time.time(), 0.987654)
        self.assertStat('another.counter:3|c|@0.987654',
                        self.client.update_stats, 'another.counter',
                        3, 0.987654)

    def test_methods_with_metric_prefix(self):
        self.client = get_statsd_client({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'alpha.beta',
        }, 'pfx')
        self.assertStat('alpha.beta.pfx.some.counter:1|c',
                        self.client.increment, 'some.counter')
        self.assertStat('alpha.beta.pfx.some.counter:-1|c',
                        self.client.decrement, 'some.counter')
        self.assertStat('alpha.beta.pfx.some.operation:4760.0|ms',
                        self.client.timing, 'some.operation', 4.76 * 1000)
        self.assertStatMatches(
            r'alpha\.beta\.pfx\.another\.op:\d+\.\d+\|ms',
            self.client.timing_since, 'another.op', time.time())
        self.assertStat('alpha.beta.pfx.another.counter:3|c',
                        self.client.update_stats, 'another.counter', 3)

        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'set_statsd_prefix\(\) is deprecated')
            self.client.set_prefix('')
        self.assertStat('alpha.beta.some.counter:1|c|@0.9912',
                        self.client.increment, 'some.counter',
                        sample_rate=0.9912)
        self.assertStat('alpha.beta.some.counter:-1|c|@0.9912',
                        self.client.decrement, 'some.counter', 0.9912)
        self.assertStat('alpha.beta.some.operation:4900.0|ms|@0.9912',
                        self.client.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.9912)
        self.assertStatMatches(
            r'alpha\.beta\.another\.op:\d+\.\d+\|ms|@0.9912',
            self.client.timing_since, 'another.op',
            time.time(), sample_rate=0.9912)
        self.assertStat('alpha.beta.another.counter:3|c|@0.9912',
                        self.client.update_stats, 'another.counter', 3,
                        sample_rate=0.9912)

    def test_statsd_methods_legacy_disabled(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'my_prefix',
            'statsd_emit_legacy': 'false',
        }
        statsd = statsd_client.get_statsd_client(conf, tail_prefix='pfx')
        with mock.patch.object(statsd, '_open_socket') as mock_open:
            statsd.increment('some.counter')
            statsd.decrement('some.counter')
            statsd.timing('some.timing', 6.28 * 1000)
            statsd.update_stats('some.stat', 3)
        self.assertFalse(mock_open.mock_calls)


class TestGetLabeledStatsdClientOutput(BaseTestStatsdClientOutput):
    """
    Tests here use get_labeled_statsd_client to make a LabeledStatsdClient.
    """
    def test_statsd_methods_disabled(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'my_prefix',
            'statsd_label_mode': 'disabled',
        }
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        with mock.patch.object(labeled_statsd,
                               '_open_socket') as mock_open:
            # Any labeled-metrics callers should not emit any metrics
            labeled_statsd.increment('the_counter', labels=labels)
            labeled_statsd.decrement('the_counter', labels=labels)
            labeled_statsd.timing('the_timing', 6.28 * 1000, labels=labels)
            labeled_statsd.update_stats('the_stat', 3, labels=labels)
        self.assertFalse(mock_open.mock_calls)

    def test_statsd_methods_dogstatsd(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'statsd_label_mode': 'dogstatsd',
            'statsd_emit_legacy': 'false',  # ignored
        }
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the_counter:1|c|#action:some,result:ok',
            labeled_statsd.increment, 'the_counter', labels=labels)
        self.assertStat(
            'the_counter:-1|c|#action:some,result:ok',
            labeled_statsd.decrement, 'the_counter', labels=labels)
        self.assertStat(
            'the_timing:6280.0|ms'
            '|#action:some,result:ok',
            labeled_statsd.timing, 'the_timing', 6.28 * 1000, labels=labels)
        self.assertStat(
            'the_stat:3|c|#action:some,result:ok',
            labeled_statsd.update_stats, 'the_stat', 3, labels=labels)

    def test_statsd_methods_dogstatsd_sample_rate(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'statsd_label_mode': 'dogstatsd',
            'log_statsd_default_sample_rate': '0.9',
            'log_statsd_sample_rate_factor': '0.5'}
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the_counter:1|c|@0.45|#action:some,result:ok',
            labeled_statsd.increment, 'the_counter', labels=labels)

    def test_statsd_methods_graphite(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'my_prefix',
            'statsd_label_mode': 'graphite',
        }
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the_counter;action=some;result=ok:1|c',
            labeled_statsd.increment, 'the_counter', labels=labels)
        self.assertStat(
            'the_counter;action=some;result=ok:-1|c',
            labeled_statsd.decrement, 'the_counter', labels=labels)
        self.assertStat(
            'the_timing;action=some;result=ok'
            ':6280.0|ms',
            labeled_statsd.timing, 'the_timing', 6.28 * 1000, labels=labels)
        self.assertStat(
            'the_stat;action=some;result=ok:3|c',
            labeled_statsd.update_stats, 'the_stat', 3, labels=labels)

    def test_statsd_methods_graphite_sample_rate(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'statsd_label_mode': 'graphite',
            'log_statsd_default_sample_rate': '0.9',
            'log_statsd_sample_rate_factor': '0.5'}
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the_counter;action=some;result=ok:1|c|@0.45',
            labeled_statsd.increment, 'the_counter', labels=labels)

    def test_statsd_methods_influxdb(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'my_prefix',
            'statsd_label_mode': 'influxdb',
        }
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the_counter,action=some,result=ok:1|c',
            labeled_statsd.increment, 'the_counter', labels=labels)
        self.assertStat(
            'the_counter,action=some,result=ok:-1|c',
            labeled_statsd.decrement, 'the_counter', labels=labels)
        self.assertStat(
            'the_counter,action=some,result=ok:-1|c',
            labeled_statsd.decrement, 'the_counter', labels=labels)
        self.assertStat(
            'the_timing,action=some,result=ok'
            ':6280.0|ms',
            labeled_statsd.timing, 'the_timing', 6.28 * 1000, labels=labels)
        self.assertStat(
            'the_stat,action=some,result=ok:3|c',
            labeled_statsd.update_stats, 'the_stat', 3, labels=labels)

    def test_statsd_methods_influxdb_sample_rate(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'statsd_label_mode': 'influxdb',
            'log_statsd_default_sample_rate': '0.9',
            'log_statsd_sample_rate_factor': '0.5'}
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the.counter,action=some,result=ok:1|c|@0.45',
            labeled_statsd.increment, 'the.counter', labels=labels)

    def test_statsd_methods_librato(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'my_prefix',
            'statsd_label_mode': 'librato',
        }
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the_counter#action=some,result=ok:1|c',
            labeled_statsd.increment, 'the_counter', labels=labels)
        self.assertStat(
            'the_counter#action=some,result=ok:-1|c',
            labeled_statsd.decrement, 'the_counter', labels=labels)
        self.assertStat(
            'the_timing#action=some,result=ok'
            ':6280.0|ms',
            labeled_statsd.timing, 'the_timing', 6.28 * 1000, labels=labels)
        self.assertStat(
            'the_stat#action=some,result=ok:3|c',
            labeled_statsd.update_stats, 'the_stat', 3, labels=labels)

    def test_statsd_methods_librato_sample_rate(self):
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'statsd_label_mode': 'librato',
            'log_statsd_default_sample_rate': '0.9',
            'log_statsd_sample_rate_factor': '0.5'}
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        labels = {'action': 'some', 'result': 'ok'}
        self.assertStat(
            'the_counter#action=some,result=ok:1|c|@0.45',
            labeled_statsd.increment, 'the_counter', labels=labels)

    def _do_test_statsd_methods_no_labels(self, label_mode):
        # no default_sample_rate option
        conf = {
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'statsd_label_mode': label_mode,
        }
        labeled_statsd = statsd_client.get_labeled_statsd_client(conf)
        self.assertStat('the.counter:1|c',
                        labeled_statsd.increment, 'the.counter', labels={})
        self.assertStat('the.counter:-1|c',
                        labeled_statsd.decrement, 'the.counter', labels={})
        # but individual call sites could set sample_rate
        self.assertStat('the.counter:1|c|@0.9912',
                        labeled_statsd.increment, 'the.counter', labels={},
                        sample_rate=0.9912)
        self.assertStat(
            'the.timing:6280.0|ms',
            labeled_statsd.timing, 'the.timing', 6.28 * 1000, labels={})
        self.assertStat('the.stat:3|c',
                        labeled_statsd.update_stats, 'the.stat', 3, labels={})

        self.assertStat('the.counter:1|c',
                        labeled_statsd.increment, 'the.counter')
        self.assertStat('the.counter:-1|c',
                        labeled_statsd.decrement, 'the.counter')
        self.assertStat('the.timing:6280.0|ms',
                        labeled_statsd.timing, 'the.timing', 6.28 * 1000)
        self.assertStat('the.stat:3|c',
                        labeled_statsd.update_stats, 'the.stat', 3)
        self.assertStat('the.stat:500.0|ms',
                        labeled_statsd.transfer_rate, 'the.stat', 3.3, 6600)

    def test_statsd_methods_dogstatsd_no_labels(self):
        self._do_test_statsd_methods_no_labels('dogstatsd')

    def test_statsd_methods_graphite_no_labels(self):
        self._do_test_statsd_methods_no_labels('graphite')

    def test_statsd_methods_influxdb_no_labels(self):
        self._do_test_statsd_methods_no_labels('influxdb')

    def test_statsd_methods_librato_no_labels(self):
        self._do_test_statsd_methods_no_labels('librato')
