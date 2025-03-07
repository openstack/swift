# Copyright (c) 2010-2011 OpenStack Foundation
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
import logging

from unittest import mock
import time
import unittest
from io import BytesIO
from logging.handlers import SysLogHandler

from urllib.parse import unquote

from swift.common.utils import get_swift_logger, split_path
from swift.common.statsd_client import StatsdClient
from swift.common.middleware import proxy_logging
from swift.common.registry import register_sensitive_header, \
    register_sensitive_param, get_sensitive_headers
from swift.common.swob import Request, Response, HTTPServiceUnavailable
from swift.common import constraints, registry
from swift.common.storage_policy import StoragePolicy
from test.debug_logger import debug_logger, FakeStatsdClient
from test.unit import patch_policies
from test.unit.common.middleware.helpers import FakeAppThatExcepts, FakeSwift


class FakeApp(object):

    def __init__(self, body=None, response_str='200 OK', policy_idx='0',
                 chunked=False, environ_updates=None):
        if body is None:
            body = [b'FAKE APP']
        elif isinstance(body, bytes):
            body = [body]

        self.body = body
        self.response_str = response_str
        self.policy_idx = policy_idx
        self.chunked = chunked
        self.environ_updates = environ_updates or {}

    def __call__(self, env, start_response):
        try:
            # /v1/a/c or /v1/a/c/o
            split_path(env['PATH_INFO'], 3, 4, True)
            is_container_or_object_req = True
        except ValueError:
            is_container_or_object_req = False

        headers = [('Content-Type', 'text/plain')]
        if self.chunked:
            headers.append(('Transfer-Encoding', 'chunked'))
        elif not hasattr(self.body, 'close'):
            content_length = sum(map(len, self.body))
            headers.append(('Content-Length', str(content_length)))

        if is_container_or_object_req and self.policy_idx is not None:
            headers.append(('X-Backend-Storage-Policy-Index',
                           str(self.policy_idx)))
        start_response(self.response_str, headers)
        while env['wsgi.input'].read(5):
            pass
        # N.B. mw can set this anytime before the resp is finished
        env.update(self.environ_updates)
        return self.body


class FakeAppNoContentLengthNoTransferEncoding(object):

    def __init__(self, body=None):
        if body is None:
            body = [b'FAKE APP']

        self.body = body

    def __call__(self, env, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain')])
        while env['wsgi.input'].read(5):
            pass
        return self.body


class FileLikeExceptor(object):

    def __init__(self):
        pass

    def read(self, len):
        raise IOError('of some sort')

    def readline(self, len=1024):
        raise IOError('of some sort')


class FakeAppReadline(object):

    def __call__(self, env, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain'),
                                  ('Content-Length', '8')])
        env['wsgi.input'].readline()
        return [b"FAKE APP"]


def start_response(*args):
    pass


@patch_policies([StoragePolicy(0, 'zero', False)])
class TestProxyLogging(unittest.TestCase):
    def setUp(self):
        self.logger = debug_logger()
        # really, this would come by way of base_prefix/tail_prefix in
        # get_logger, ultimately tracing back to our hard-coded
        # statsd_tail_prefix
        self.logger.logger.statsd_client._prefix = 'proxy-server.'

    def _log_parts(self, app, should_be_empty=False):
        info_calls = app.access_logger.log_dict['info']
        if should_be_empty:
            self.assertEqual([], info_calls)
        else:
            self.assertEqual(1, len(info_calls))
            return info_calls[0][0][0].split(' ')

    def assertTiming(self, exp_metric, app, exp_timing=None):
        timing_calls = app.access_logger.statsd_client.calls['timing']
        found = False
        for timing_call in timing_calls:
            self.assertEqual({}, timing_call[1])
            self.assertEqual(2, len(timing_call[0]))
            if timing_call[0][0] == exp_metric:
                found = True
                if exp_timing is not None:
                    self.assertAlmostEqual(exp_timing, timing_call[0][1],
                                           places=4)
        if not found:
            self.fail('assertTiming: %s not found in %r' % (
                exp_metric, timing_calls))

    def assertNotTiming(self, not_exp_metric, app):
        timing_calls = app.access_logger.statsd_client.calls['timing']
        for timing_call in timing_calls:
            self.assertNotEqual(not_exp_metric, timing_call[0][0])

    def assertUpdateStats(self, exp_metrics_and_values, app):
        update_stats_calls = sorted(
            app.access_logger.statsd_client.calls['update_stats'])
        got_metrics_values_and_kwargs = [(usc[0][0], usc[0][1], usc[1])
                                         for usc in update_stats_calls]
        exp_metrics_values_and_kwargs = [(emv[0], emv[1], {})
                                         for emv in exp_metrics_and_values]
        self.assertEqual(got_metrics_values_and_kwargs,
                         exp_metrics_values_and_kwargs)
        self.assertIs(self.logger, app.access_logger)
        for metric, value in exp_metrics_and_values:
            self.assertIn(
                (('proxy-server.%s:%s|c' % (metric, value)).encode(),
                 ('host', 8125)),
                app.access_logger.statsd_client.sendto_calls)

    def test_init_statsd_options_log_prefix(self):
        conf = {
            'log_headers': 'no',
            'log_statsd_valid_http_methods': 'GET',
            'log_facility': 'LOG_LOCAL7',
            'log_name': 'bob',
            'log_level': 'DEBUG',
            'log_udp_host': 'example.com',
            'log_udp_port': '3456',
            'log_statsd_host': 'example.com',
            'log_statsd_port': '1234',
            'log_statsd_default_sample_rate': 10,
            'log_statsd_sample_rate_factor': .04,
            'log_statsd_metric_prefix': 'foo',
        }
        with mock.patch('swift.common.statsd_client.StatsdClient',
                        FakeStatsdClient):
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), conf)

        self.assertFalse(app.log_hdrs)
        self.assertEqual(['GET'], app.valid_methods)

        log_adapter = app.access_logger
        self.assertEqual('proxy-access', log_adapter.name)
        self.assertEqual('bob', app.access_logger.server)
        self.assertEqual(logging.DEBUG, log_adapter.logger.level)
        self.assertEqual(('example.com', 3456),
                         log_adapter.logger.handlers[0].address)
        self.assertEqual(SysLogHandler.LOG_LOCAL7,
                         log_adapter.logger.handlers[0].facility)

        statsd_client = app.access_logger.logger.statsd_client
        self.assertIsInstance(statsd_client, FakeStatsdClient)
        with mock.patch.object(statsd_client, 'random', return_value=0):
            statsd_client.increment('baz')
        self.assertEqual(
            [(b'foo.proxy-server.baz:1|c|@0.4', ('example.com', 1234))],
            statsd_client.sendto_calls)

    def test_init_statsd_options_access_log_prefix(self):
        # verify that access_log_ prefix has precedence over log_
        conf = {
            'access_log_route': 'my-proxy-access',
            'access_log_headers': 'yes',
            'access_log_statsd_valid_http_methods': 'GET, HEAD',
            'access_log_facility': 'LOG_LOCAL6',
            'access_log_name': 'alice',
            'access_log_level': 'WARN',
            'access_log_udp_host': 'access.com',
            'access_log_udp_port': '6789',
            'log_headers': 'no',
            'log_statsd_valid_http_methods': 'GET',
            'log_facility': 'LOG_LOCAL7',
            'log_name': 'bob',
            'log_level': 'DEBUG',
            'log_udp_host': 'example.com',
            'log_udp_port': '3456',
            'access_log_statsd_host': 'access.com',
            'access_log_statsd_port': '5678',
            'access_log_statsd_default_sample_rate': 20,
            'access_log_statsd_sample_rate_factor': .03,
            'access_log_statsd_metric_prefix': 'access_foo',
            'log_statsd_host': 'example.com',
            'log_statsd_port': '1234',
            'log_statsd_default_sample_rate': 10,
            'log_statsd_sample_rate_factor': .04,
            'log_statsd_metric_prefix': 'foo',
        }
        with mock.patch('swift.common.statsd_client.StatsdClient',
                        FakeStatsdClient):
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), conf)

        self.assertTrue(app.log_hdrs)
        self.assertEqual(['GET', 'HEAD'], app.valid_methods)

        log_adapter = app.access_logger
        self.assertEqual('my-proxy-access', log_adapter.name)
        self.assertEqual('alice', app.access_logger.server)
        self.assertEqual(logging.WARN, log_adapter.logger.level)
        self.assertEqual(('access.com', 6789),
                         log_adapter.logger.handlers[0].address)
        self.assertEqual(SysLogHandler.LOG_LOCAL6,
                         log_adapter.logger.handlers[0].facility)

        statsd_client = app.access_logger.logger.statsd_client
        self.assertIsInstance(statsd_client, FakeStatsdClient)
        with mock.patch.object(statsd_client, 'random', return_value=0):
            statsd_client.increment('baz')
        self.assertEqual(
            [(b'access_foo.proxy-server.baz:1|c|@0.6', ('access.com', 5678))],
            statsd_client.sendto_calls)

    def test_logger_statsd_prefix(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(), {'log_statsd_host': 'example.com'})
        self.assertIsNotNone(app.access_logger.logger.statsd_client)
        self.assertIsInstance(app.access_logger.logger.statsd_client,
                              StatsdClient)
        self.assertEqual('proxy-server.',
                         app.access_logger.logger.statsd_client._prefix)

        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(), {'log_statsd_metric_prefix': 'foo',  # set base prefix
                        'access_log_name': 'bar',  # not used as tail prefix
                        'log_name': 'baz',  # not used as tail prefix
                        'log_statsd_host': 'example.com'})
        self.assertIsNotNone(app.access_logger.logger.statsd_client)
        self.assertIsInstance(app.access_logger.logger.statsd_client,
                              StatsdClient)
        self.assertEqual('foo.proxy-server.',
                         app.access_logger.logger.statsd_client._prefix)

    def test_log_request_statsd_invalid_stats_types(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(), {}, logger=self.logger)
        for url in ['/', '/foo', '/foo/bar', '/v1', '/v1.0']:
            self.logger.clear()
            req = Request.blank(url, environ={'REQUEST_METHOD': 'GET'})
            resp = app(req.environ, start_response)
            # get body
            b''.join(resp)
            self.assertEqual(
                [(('UNKNOWN.GET.200.first-byte.timing', mock.ANY), {}),
                 (('UNKNOWN.GET.200.timing', mock.ANY), {})],
                app.access_logger.statsd_client.calls['timing'])
            self.assertEqual(
                [(('UNKNOWN.GET.200.xfer', mock.ANY), {})],
                app.access_logger.statsd_client.calls['update_stats'])

    def test_log_request_stat_type_bad(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(), {}, logger=self.logger)
        for bad_path in [
                '',
                '/',
                '/bad',
                '/baddy/mc_badderson',
                '/v1',
                '/v1/',
                '/v1.0',
                '/v1.0/',
                '/v1.0//',
                '/v1.0//c',
                '/v1.0/a//',
                '/v1.0/a//o',
        ]:
            req = Request.blank(bad_path, environ={'REQUEST_METHOD': 'GET'})
            now = 10000.0
            app.log_request(req, 123, 7, 13, now, now + 2.71828182846)
            self.assertEqual(
                [(('UNKNOWN.GET.123.timing', 2718.2818284600216), {})],
                app.access_logger.statsd_client.calls['timing'])
            self.assertEqual(
                [(('UNKNOWN.GET.123.xfer', 20), {})],
                app.access_logger.statsd_client.calls['update_stats'])
            app.access_logger.clear()

    def test_log_request_stat_type_good(self):
        """
        log_request() should send timing and byte-count counters for GET
        requests.  Also, __call__()'s iter_response() function should
        statsd-log time to first byte (calling the passed-in start_response
        function), but only for GET requests.
        """
        stub_times = []

        def stub_time():
            return stub_times.pop(0)

        path_types = {
            '/v1/a': 'account',
            '/v1/a/': 'account',
            '/v1/a/c': 'container',
            '/v1/a/c/': 'container',
            '/v1/a/c/o': 'object',
            '/v1/a/c/o/': 'object',
            '/v1/a/c/o/p': 'object',
            '/v1/a/c/o/p/': 'object',
            '/v1/a/c/o/p/p2': 'object',
            '/v1.0/a': 'account',
            '/v1.0/a/': 'account',
            '/v1.0/a/c': 'container',
            '/v1.0/a/c/': 'container',
            '/v1.0/a/c/o': 'object',
            '/v1.0/a/c/o/': 'object',
            '/v1.0/a/c/o/p': 'object',
            '/v1.0/a/c/o/p/': 'object',
            '/v1.0/a/c/o/p/p2': 'object',
        }
        with mock.patch("time.time", stub_time):
            for path, exp_type in path_types.items():
                # GET
                self.logger.clear()
                app = proxy_logging.ProxyLoggingMiddleware(
                    FakeApp(body=b'7654321', response_str='321 Fubar'),
                    {},
                    logger=self.logger)
                req = Request.blank(path, environ={
                    'REQUEST_METHOD': 'GET',
                    'wsgi.input': BytesIO(b'4321')})
                stub_times = [18.0, 18.5, 20.71828182846]
                iter_response = app(req.environ, lambda *_: None)

                self.assertEqual(b'7654321', b''.join(iter_response))
                self.assertTiming('%s.GET.321.timing' % exp_type, app,
                                  exp_timing=2.71828182846 * 1000)
                self.assertTiming('%s.GET.321.first-byte.timing'
                                  % exp_type, app, exp_timing=0.5 * 1000)
                if exp_type == 'object':
                    # Object operations also return stats by policy
                    # In this case, the value needs to match the timing for GET
                    self.assertTiming('%s.policy.0.GET.321.timing' % exp_type,
                                      app, exp_timing=2.71828182846 * 1000)
                    self.assertTiming(
                        '%s.policy.0.GET.321.first-byte.timing'
                        % exp_type, app, exp_timing=0.5 * 1000)
                    self.assertUpdateStats([('%s.GET.321.xfer' % exp_type,
                                             4 + 7),
                                            ('object.policy.0.GET.321.xfer',
                                             4 + 7)],
                                           app)
                else:
                    self.assertUpdateStats([('%s.GET.321.xfer' % exp_type,
                                            4 + 7)],
                                           app)

                # GET Repeat the test above, but with a non-existent policy
                # Do this only for object types
                if exp_type == 'object':
                    self.logger.clear()
                    app = proxy_logging.ProxyLoggingMiddleware(
                        FakeApp(body=b'7654321', response_str='321 Fubar',
                                policy_idx='-1'), {}, logger=self.logger)
                    req = Request.blank(path, environ={
                        'REQUEST_METHOD': 'GET',
                        'wsgi.input': BytesIO(b'4321')})
                    stub_times = [18.0, 18.5, 20.71828182846]
                    iter_response = app(req.environ, lambda *_: None)

                    self.assertEqual(b'7654321', b''.join(iter_response))
                    self.assertTiming('%s.GET.321.timing' % exp_type, app,
                                      exp_timing=2.71828182846 * 1000)
                    # No results returned for the non-existent policy
                    self.assertUpdateStats([('%s.GET.321.xfer' % exp_type,
                                            4 + 7)],
                                           app)

                # GET with swift.proxy_access_log_made already set
                app = proxy_logging.ProxyLoggingMiddleware(
                    FakeApp(body=b'7654321', response_str='321 Fubar'), {})
                app.access_logger = debug_logger()
                req = Request.blank(path, environ={
                    'REQUEST_METHOD': 'GET',
                    'swift.proxy_access_log_made': True,
                    'wsgi.input': BytesIO(b'4321')})
                stub_times = [18.0, 20.71828182846]
                iter_response = app(req.environ, lambda *_: None)
                self.assertEqual(b'7654321', b''.join(iter_response))
                self.assertEqual(
                    [], app.access_logger.statsd_client.calls['timing'])
                self.assertEqual(
                    [], app.access_logger.statsd_client.calls['timing_since'])
                self.assertEqual(
                    [], app.access_logger.statsd_client.calls['update_stats'])

                # PUT (no first-byte timing!)
                self.logger.clear()
                app = proxy_logging.ProxyLoggingMiddleware(
                    FakeApp(body=b'87654321', response_str='314 PiTown'), {},
                    logger=self.logger)
                req = Request.blank(path, environ={
                    'REQUEST_METHOD': 'PUT',
                    'wsgi.input': BytesIO(b'654321')})
                # (it's not a GET, so time() doesn't have a 2nd call)
                stub_times = [58.2, 58.2 + 7.3321]
                iter_response = app(req.environ, lambda *_: None)
                self.assertEqual(b'87654321', b''.join(iter_response))
                self.assertTiming('%s.PUT.314.timing' % exp_type, app,
                                  exp_timing=7.3321 * 1000)
                self.assertNotTiming(
                    '%s.GET.314.first-byte.timing' % exp_type, app)
                self.assertNotTiming(
                    '%s.PUT.314.first-byte.timing' % exp_type, app)
                if exp_type == 'object':
                    # Object operations also return stats by policy In this
                    # case, the value needs to match the timing for PUT.
                    self.assertTiming('%s.policy.0.PUT.314.timing' %
                                      exp_type, app,
                                      exp_timing=7.3321 * 1000)
                    self.assertUpdateStats(
                        [('object.PUT.314.xfer', 6 + 8),
                         ('object.policy.0.PUT.314.xfer', 6 + 8)], app)
                else:
                    self.assertUpdateStats(
                        [('%s.PUT.314.xfer' % exp_type, 6 + 8)], app)

                # PUT Repeat the test above, but with a non-existent policy
                # Do this only for object types
                if exp_type == 'object':
                    self.logger.clear()
                    app = proxy_logging.ProxyLoggingMiddleware(
                        FakeApp(body=b'87654321', response_str='314 PiTown',
                                policy_idx='-1'), {}, logger=self.logger)
                    req = Request.blank(path, environ={
                        'REQUEST_METHOD': 'PUT',
                        'wsgi.input': BytesIO(b'654321')})
                    # (it's not a GET, so time() doesn't have a 2nd call)
                    stub_times = [58.2, 58.2 + 7.3321]
                    iter_response = app(req.environ, lambda *_: None)
                    self.assertEqual(b'87654321', b''.join(iter_response))
                    self.assertTiming('%s.PUT.314.timing' % exp_type, app,
                                      exp_timing=7.3321 * 1000)
                    self.assertNotTiming(
                        '%s.GET.314.first-byte.timing' % exp_type, app)
                    self.assertNotTiming(
                        '%s.PUT.314.first-byte.timing' % exp_type, app)
                    # No results returned for the non-existent policy
                    self.assertUpdateStats(
                        [('object.PUT.314.xfer', 6 + 8)], app)

    def test_log_request_stat_method_filtering_default(self):
        method_map = {
            'foo': 'BAD_METHOD',
            '': 'BAD_METHOD',
            'PUTT': 'BAD_METHOD',
            'SPECIAL': 'BAD_METHOD',
            'GET': 'GET',
            'PUT': 'PUT',
            'COPY': 'COPY',
            'HEAD': 'HEAD',
            'POST': 'POST',
            'DELETE': 'DELETE',
            'OPTIONS': 'OPTIONS',
        }
        for method, exp_method in method_map.items():
            self.logger.clear()
            app = proxy_logging.ProxyLoggingMiddleware(
                FakeApp(), {}, logger=self.logger)
            req = Request.blank('/v1/a/', environ={'REQUEST_METHOD': method})
            now = 10000.0
            app.log_request(req, 299, 11, 3, now, now + 1.17)
            self.assertTiming('account.%s.299.timing' % exp_method, app,
                              exp_timing=1.17 * 1000)
            self.assertUpdateStats([('account.%s.299.xfer' % exp_method,
                                   11 + 3)], app)

    def test_log_request_stat_method_filtering_custom(self):
        method_map = {
            'foo': 'BAD_METHOD',
            '': 'BAD_METHOD',
            'PUTT': 'BAD_METHOD',
            'SPECIAL': 'SPECIAL',  # will be configured
            'GET': 'GET',
            'PUT': 'PUT',
            'COPY': 'BAD_METHOD',  # prove no one's special
        }
        # this conf var supports optional leading access_
        for conf_key in ['access_log_statsd_valid_http_methods',
                         'log_statsd_valid_http_methods']:
            for method, exp_method in method_map.items():
                self.logger.clear()
                app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
                    conf_key: 'SPECIAL,  GET,PUT ',  # crazy spaces ok
                }, logger=self.logger)
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': method})
                now = 10000.0
                app.log_request(req, 911, 4, 43, now, now + 1.01)
                self.assertTiming('container.%s.911.timing' % exp_method, app,
                                  exp_timing=1.01 * 1000)
                self.assertUpdateStats([('container.%s.911.xfer' % exp_method,
                                       4 + 43)], app)

    def test_basic_req(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'FAKE APP')
        self.assertEqual(log_parts[11], str(len(resp_body)))

    def test_object_error(self):
        swift = FakeSwift()
        self.logger = debug_logger()
        app = proxy_logging.ProxyLoggingMiddleware(swift, {},
                                                   logger=self.logger)
        swift.register('GET', '/v1/a/c/o', HTTPServiceUnavailable, {}, None)
        req = Request.blank('/v1/a/c/o')
        start = time.time()
        ttfb = start + 0.2
        end = ttfb + 0.5
        with mock.patch("swift.common.middleware.proxy_logging.time.time",
                        side_effect=[start, ttfb, end]):
            resp = req.get_response(app)
            self.assertEqual(503, resp.status_int)
            # we have to consume the resp body to trigger logging
            self.assertIn(b'Service Unavailable', resp.body)
            log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/v1/a/c/o')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '503')
        # we can also expect error metrics
        self.assertTiming('object.GET.503.timing', app,
                          exp_timing=700.0)
        self.assertTiming('object.GET.503.first-byte.timing', app,
                          exp_timing=200.0)

    def test_basic_error(self):
        swift = FakeSwift()
        self.logger = debug_logger()
        app = proxy_logging.ProxyLoggingMiddleware(swift, {},
                                                   logger=self.logger)
        swift.register('GET', '/path', HTTPServiceUnavailable, {}, None)
        req = Request.blank('/path')
        start = time.time()
        ttfb = start + 0.2
        end = ttfb + 0.5
        with mock.patch("swift.common.middleware.proxy_logging.time.time",
                        side_effect=[start, ttfb, end]):
            resp = req.get_response(app)
            self.assertEqual(503, resp.status_int)
            # we have to consume the resp body to trigger logging
            self.assertIn(b'Service Unavailable', resp.body)
            log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/path')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '503')
        # we can also expect error metrics
        self.assertTiming('UNKNOWN.GET.503.timing', app,
                          exp_timing=700.0)
        self.assertTiming('UNKNOWN.GET.503.first-byte.timing', app,
                          exp_timing=200.0)

    def test_middleware_exception(self):
        self.logger = debug_logger()
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppThatExcepts(), {}, logger=self.logger)
        req = Request.blank('/path')
        start = time.time()
        ttfb = start + 0.2
        with mock.patch("swift.common.middleware.proxy_logging.time.time",
                        side_effect=[start, ttfb]), \
                self.assertRaises(Exception):
            req.get_response(app)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/path')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '500')
        # we can also expect error metrics
        self.assertTiming('UNKNOWN.GET.500.timing', app,
                          exp_timing=200.0)

    def test_middleware_error(self):
        class ErrorFakeApp(object):

            def __call__(self, env, start_response):
                env['swift.source'] = 'FA'
                resp = HTTPServiceUnavailable()
                return resp(env, start_response)

        self.logger = debug_logger()
        app = proxy_logging.ProxyLoggingMiddleware(ErrorFakeApp(), {},
                                                   logger=self.logger)
        req = Request.blank('/path')
        start = time.time()
        ttfb = start + 0.2
        end = ttfb + 0.5
        with mock.patch("swift.common.middleware.proxy_logging.time.time",
                        side_effect=[start, ttfb, end]):
            resp = req.get_response(app)
            self.assertEqual(503, resp.status_int)
            # we have to consume the resp body to trigger logging
            self.assertIn(b'Service Unavailable', resp.body)
            log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/path')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '503')
        # we can also expect error metrics
        self.assertTiming('FA.GET.503.timing', app,
                          exp_timing=700.0)
        self.assertTiming('FA.GET.503.first-byte.timing', app,
                          exp_timing=200.0)

    def test_basic_req_second_time(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={
            'swift.proxy_access_log_made': True,
            'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        self._log_parts(app, should_be_empty=True)
        self.assertEqual(resp_body, b'FAKE APP')

    def test_log_msg_template(self):
        # Access logs configuration should override the default one
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'log_anonymization_salt': 'secret_salt',
            'log_msg_template': (
                'template which can be edited in config: '
                '{protocol} {path} {method} '
                '{path.anonymized} {container.anonymized} '
                '{request_time} {start_time.datetime} {end_time} {ttfb} '
                '{domain}')})
        app.access_logger = debug_logger()
        req = Request.blank('/', headers={'Host': 'example.com'})
        with mock.patch('time.time',
                        mock.MagicMock(
                            side_effect=[10000000.0, 10000000.5, 10000001.0])):
            resp = app(req.environ, start_response)
            # exhaust generator
            resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[0], 'template')
        self.assertEqual(log_parts[7], 'HTTP/1.0')
        self.assertEqual(log_parts[8], '/')
        self.assertEqual(log_parts[9], 'GET')
        self.assertEqual(log_parts[10],
                         '{SMD5}c65475e457fea0951fbb9ec9596b2177')
        self.assertEqual(log_parts[11], '-')
        self.assertEqual(log_parts[13], '26/Apr/1970/17/46/40')
        self.assertEqual(log_parts[14], '10000001.000000000')
        self.assertEqual(log_parts[15], '0.5')
        self.assertEqual(log_parts[16], 'example.com')
        self.assertEqual(resp_body, b'FAKE APP')

    def test_log_msg_template_s3api(self):
        # Access logs configuration should override the default one
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'log_msg_template': (
                '{protocol} {path} {method} '
                '{account} {container} {object}')
        }, logger=self.logger)
        req = Request.blank('/bucket/path/to/key', environ={
            'REQUEST_METHOD': 'GET',
            # This would actually get set in the app, but w/e
            'swift.backend_path': '/v1/AUTH_test/bucket/path/to/key'})
        with mock.patch("time.time", side_effect=[
                18.0, 18.5, 20.71828182846]):
            resp = app(req.environ, start_response)
            # exhaust generator
            resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts, [
            'HTTP/1.0',
            '/bucket/path/to/key',
            'GET',
            'AUTH_test',
            'bucket',
            'path/to/key',
        ])
        self.assertEqual(resp_body, b'FAKE APP')
        self.assertTiming('object.policy.0.GET.200.timing',
                          app, exp_timing=2.71828182846 * 1000)
        self.assertUpdateStats([('object.GET.200.xfer', 8),
                                ('object.policy.0.GET.200.xfer', 8)],
                               app)

    def test_invalid_log_config(self):
        with self.assertRaises(ValueError):
            proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
                'log_anonymization_salt': 'secret_salt',
                'log_msg_template': '{invalid_field}'})

        with self.assertRaises(ValueError):
            proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
                'log_anonymization_method': 'invalid_hash_method',
                'log_anonymization_salt': 'secret_salt',
                'log_msg_template': '{protocol}'})

    def test_multi_segment_resp(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(
            [b'some', b'chunks', b'of data']), {}, logger=self.logger)
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'swift.source': 'SOS'})
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'somechunksof data')
        self.assertEqual(log_parts[11], str(len(resp_body)))
        self.assertUpdateStats([('SOS.GET.200.xfer', len(resp_body))],
                               app)

    def test_log_headers(self):
        for conf_key in ['access_log_headers', 'log_headers']:
            self.logger.clear()
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                                                       {conf_key: 'yes'},
                                                       logger=self.logger)
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
            resp = app(req.environ, start_response)
            # exhaust generator
            [x for x in resp]
            log_parts = self._log_parts(app)
            headers = unquote(log_parts[14]).split('\n')
            self.assertIn('Host: localhost:80', headers)

    def test_access_log_headers_only(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(), {'log_headers': 'yes',
                        'access_log_headers_only': 'FIRST, seCond'})
        app.access_logger = debug_logger()
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'First': '1',
                                     'Second': '2',
                                     'Third': '3'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        headers = unquote(log_parts[14]).split('\n')
        self.assertIn('First: 1', headers)
        self.assertIn('Second: 2', headers)
        self.assertNotIn('Third: 3', headers)
        self.assertNotIn('Host: localhost:80', headers)

    def test_upload_size(self):
        # Using default policy
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                                                   {'log_headers': 'yes'},
                                                   logger=self.logger)
        req = Request.blank(
            '/v1/a/c/o/foo',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': BytesIO(b'some stuff')})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[11], str(len('FAKE APP')))
        self.assertEqual(log_parts[10], str(len('some stuff')))
        self.assertUpdateStats([('object.PUT.200.xfer',
                                 len('some stuff') + len('FAKE APP')),
                                ('object.policy.0.PUT.200.xfer',
                                 len('some stuff') + len('FAKE APP'))],
                               app)

        # Using a non-existent policy
        self.logger.clear()
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(policy_idx='-1'),
                                                   {'log_headers': 'yes'},
                                                   logger=self.logger)
        req = Request.blank(
            '/v1/a/c/o/foo',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': BytesIO(b'some stuff')})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[11], str(len('FAKE APP')))
        self.assertEqual(log_parts[10], str(len('some stuff')))
        self.assertUpdateStats([('object.PUT.200.xfer',
                                 len('some stuff') + len('FAKE APP'))],
                               app)

    def test_upload_size_no_policy(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(policy_idx=None),
                                                   {'log_headers': 'yes'},
                                                   logger=self.logger)
        req = Request.blank(
            '/v1/a/c/o/foo',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': BytesIO(b'some stuff')})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[11], str(len('FAKE APP')))
        self.assertEqual(log_parts[10], str(len('some stuff')))
        self.assertUpdateStats([('object.PUT.200.xfer',
                                 len('some stuff') + len('FAKE APP'))],
                               app)

    def test_upload_line(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeAppReadline(),
                                                   {'log_headers': 'yes'},
                                                   logger=self.logger)
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'POST',
                     'wsgi.input': BytesIO(b'some stuff\nsome other stuff\n')})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[11], str(len('FAKE APP')))
        self.assertEqual(log_parts[10], str(len('some stuff\n')))
        self.assertUpdateStats([('container.POST.200.xfer',
                               len('some stuff\n') + len('FAKE APP'))],
                               app)

    def test_log_query_string(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'QUERY_STRING': 'x=3'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(unquote(log_parts[4]), '/?x=3')

    def test_client_logging(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'REMOTE_ADDR': '1.2.3.4'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[0], '1.2.3.4')  # client ip
        self.assertEqual(log_parts[1], '1.2.3.4')  # remote addr

    def test_iterator_closing(self):

        class CloseableBody(object):
            def __init__(self):
                self.msg = b"CloseableBody"
                self.closed = False

            def close(self):
                self.closed = True

            def __iter__(self):
                return self

            def __next__(self):
                if not self.msg:
                    raise StopIteration
                result, self.msg = self.msg, b''
                return result

        body = CloseableBody()
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(body), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'REMOTE_ADDR': '1.2.3.4'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        self.assertTrue(body.closed)

    def test_chunked_response(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(chunked=True), {})
        req = Request.blank('/')
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]

    def test_proxy_client_logging(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={
            'REQUEST_METHOD': 'GET',
            'REMOTE_ADDR': '1.2.3.4',
            'HTTP_X_FORWARDED_FOR': '4.5.6.7,8.9.10.11'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[0], '4.5.6.7')  # client ip
        self.assertEqual(log_parts[1], '1.2.3.4')  # remote addr

        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={
            'REQUEST_METHOD': 'GET',
            'REMOTE_ADDR': '1.2.3.4',
            'HTTP_X_CLUSTER_CLIENT_IP': '4.5.6.7'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[0], '4.5.6.7')  # client ip
        self.assertEqual(log_parts[1], '1.2.3.4')  # remote addr

    def test_facility(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(),
            {'log_headers': 'yes',
             'access_log_facility': 'LOG_LOCAL7'})
        handler = get_swift_logger.handler4logger[app.access_logger.logger]
        self.assertEqual(SysLogHandler.LOG_LOCAL7, handler.facility)

    def test_filter(self):
        factory = proxy_logging.filter_factory({})
        self.assertTrue(callable(factory))
        self.assertTrue(callable(factory(FakeApp())))

    def test_sensitive_headers_registered(self):
        with mock.patch.object(registry, '_sensitive_headers', set()):
            self.assertNotIn('x-auth-token', get_sensitive_headers())
            self.assertNotIn('x-storage-token', get_sensitive_headers())
            proxy_logging.filter_factory({})(FakeApp())
            self.assertIn('x-auth-token', get_sensitive_headers())
            self.assertIn('x-storage-token', get_sensitive_headers())

    def test_unread_body(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(['some', 'stuff']), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        # read first chunk
        next(resp)
        resp.close()  # raise a GeneratorExit in middleware app_iter loop
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[6], '499')
        self.assertEqual(log_parts[11], '4')  # write length

    def test_exploding_body(self):

        def exploding_body():
            yield 'some'
            yield 'stuff'
            raise Exception('kaboom!')

        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(exploding_body()), {
                'log_msg_template': '{method} {path} '
                '{status_int} {wire_status_int}',
            })
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = req.get_response(app)
        with self.assertRaises(Exception) as ctx:
            resp.body
        self.assertEqual('kaboom!', str(ctx.exception))
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts, ['GET', '/', '500', '200'])

    def test_disconnect_on_readline(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeAppReadline(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'wsgi.input': FileLikeExceptor()})
        try:
            resp = app(req.environ, start_response)
            # read body
            b''.join(resp)
        except IOError:
            pass
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[6], '499')
        self.assertEqual(log_parts[10], '-')  # read length

    def test_disconnect_on_read(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(['some', 'stuff']), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'wsgi.input': FileLikeExceptor()})
        try:
            resp = app(req.environ, start_response)
            # read body
            b''.join(resp)
        except IOError:
            pass
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[6], '499')
        self.assertEqual(log_parts[10], '-')  # read length

    def test_environ_has_proxy_logging_status(self):
        conf = {'log_msg_template':
                '{method} {path} {status_int} {wire_status_int}'}

        def do_test(environ_updates):
            fake_app = FakeApp(body=[b'Slow Down'],
                               response_str='503 Slow Down',
                               environ_updates=environ_updates)
            app = proxy_logging.ProxyLoggingMiddleware(fake_app, conf)
            app.access_logger = debug_logger()
            req = Request.blank('/v1/a/c')
            captured_start_resp = mock.MagicMock()
            try:
                resp = app(req.environ, captured_start_resp)
                b''.join(resp)  # read body
            except IOError:
                pass
            captured_start_resp.assert_called_once_with(
                '503 Slow Down', mock.ANY, None)
            return self._log_parts(app)

        # control case, logged status == wire status
        environ_updates = {}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '503', '503'])

        # logged status is forced to other value
        environ_updates = {'swift.proxy_logging_status': 429}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '429', '503'])

        environ_updates = {'swift.proxy_logging_status': '429'}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '429', '503'])

        environ_updates = {'swift.proxy_logging_status': None}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '-', '503'])

        # middleware should use an int like the docs tell them too, but we
        # won't like ... "blow up" or anything
        environ_updates = {'swift.proxy_logging_status': ''}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '-', '503'])

        environ_updates = {'swift.proxy_logging_status': True}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', 'True', '503'])

        environ_updates = {'swift.proxy_logging_status': False}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '-', '503'])

        environ_updates = {'swift.proxy_logging_status': 'parsing ok'}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', 'parsing%20ok', '503'])

    def test_body_iter_updates_environ_proxy_logging_status(self):
        conf = {'log_msg_template':
                '{method} {path} {status_int} {wire_status_int}'}

        def do_test(req, body_iter, updated_status):
            fake_app = FakeApp(body=body_iter,
                               response_str='205 Weird')
            app = proxy_logging.ProxyLoggingMiddleware(fake_app, conf)
            app.access_logger = debug_logger()
            captured_start_resp = mock.MagicMock()
            try:
                resp = app(req.environ, captured_start_resp)
                b''.join(resp)  # read body
            except IOError:
                pass
            captured_start_resp.assert_called_once_with(
                '205 Weird', mock.ANY, None)
            self.assertEqual(self._log_parts(app),
                             ['GET', '/v1/a/c', updated_status, '205'])

        # sanity
        req = Request.blank('/v1/a/c')
        do_test(req, [b'normal', b'chunks'], '205')

        def update_in_middle_chunk_gen():
            yield b'foo'
            yield b'bar'
            req.environ['swift.proxy_logging_status'] = 209
            yield b'baz'

        req = Request.blank('/v1/a/c')
        do_test(req, update_in_middle_chunk_gen(), '209')

        def update_in_finally_chunk_gen():
            try:
                for i in range(3):
                    yield ('foo%s' % i).encode()
            finally:
                req.environ['swift.proxy_logging_status'] = 210

        req = Request.blank('/v1/a/c')
        do_test(req, update_in_finally_chunk_gen(), '210')

    def test_environ_has_proxy_logging_status_unread_body(self):
        conf = {'log_msg_template':
                '{method} {path} {status_int} {wire_status_int}'}

        def do_test(environ_updates):
            fake_app = FakeApp(body=[b'Slow Down'],
                               response_str='503 Slow Down',
                               environ_updates=environ_updates)
            app = proxy_logging.ProxyLoggingMiddleware(fake_app, conf)
            app.access_logger = debug_logger()
            req = Request.blank('/v1/a/c')
            captured_start_resp = mock.MagicMock()
            resp = app(req.environ, captured_start_resp)
            # read first chunk
            next(resp)
            resp.close()  # raise a GeneratorExit in middleware app_iter loop
            captured_start_resp.assert_called_once_with(
                '503 Slow Down', mock.ANY, None)
            return self._log_parts(app)

        # control case, logged status is 499
        environ_updates = {}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '499', '503'])

        # logged status is forced to 499 despite swift.proxy_logging_status
        environ_updates = {'swift.proxy_logging_status': '429'}
        self.assertEqual(do_test(environ_updates),
                         ['GET', '/v1/a/c', '499', '503'])

    def test_environ_has_proxy_logging_status_and_app_explodes(self):
        # verify exception overrides proxy_logging_status
        conf = {'log_msg_template':
                '{method} {path} {status_int} {wire_status_int}'}

        class ExplodingFakeApp(object):

            def __call__(self, env, start_response):
                # this is going to be so great!
                env['swift.proxy_logging_status'] = '456'
                start_response('568 Bespoke', [('X-Special', 'fun')])
                raise Exception('oops!')

        fake_app = ExplodingFakeApp()
        app = proxy_logging.ProxyLoggingMiddleware(fake_app, conf)
        app.access_logger = debug_logger()
        req = Request.blank('/v1/a/c')
        captured_start_resp = mock.MagicMock()
        with self.assertRaises(Exception) as cm:
            app(req.environ, captured_start_resp)
        captured_start_resp.assert_not_called()
        self.assertEqual('oops!', str(cm.exception))
        self.assertEqual(self._log_parts(app),
                         ['GET', '/v1/a/c', '500', '500'])

    def test_environ_has_proxy_logging_status_and_body_explodes(self):
        # verify exception overrides proxy_logging_status
        conf = {'log_msg_template':
                '{method} {path} {status_int} {wire_status_int}'}

        def exploding_body():
            yield 'some'
            yield 'stuff'
            raise Exception('oops!')

        class ExplodingFakeApp(object):

            def __call__(self, env, start_response):
                # this is going to be so great!
                env['swift.proxy_logging_status'] = '456'
                start_response('568 Bespoke', [('X-Special', 'fun')])
                return exploding_body()

        fake_app = ExplodingFakeApp()
        app = proxy_logging.ProxyLoggingMiddleware(fake_app, conf)
        app.access_logger = debug_logger()
        req = Request.blank('/v1/a/c')
        captured_start_resp = mock.MagicMock()
        app_iter = app(req.environ, captured_start_resp)
        with self.assertRaises(Exception) as cm:
            b''.join(app_iter)
        captured_start_resp.assert_called_once_with(
            '568 Bespoke', [('X-Special', 'fun')], None)
        self.assertEqual('oops!', str(cm.exception))
        self.assertEqual(self._log_parts(app),
                         ['GET', '/v1/a/c', '500', '568'])

    def test_app_exception(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppThatExcepts(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        try:
            app(req.environ, start_response)
        except Exception:
            pass
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[6], '500')
        self.assertEqual(log_parts[10], '-')  # read length

    def test_no_content_length_no_transfer_encoding_with_list_body(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppNoContentLengthNoTransferEncoding(
                # test the "while not chunk: chunk = next(iterator)"
                body=[b'', b'', b'line1\n', b'line2\n'],
            ), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'line1\nline2\n')
        self.assertEqual(log_parts[11], str(len(resp_body)))

    def test_no_content_length_no_transfer_encoding_with_empty_strings(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppNoContentLengthNoTransferEncoding(
                # test the "while not chunk: chunk = next(iterator)"
                body=[b'', b'', b''],
            ), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'')
        self.assertEqual(log_parts[11], '-')

    def test_no_content_length_no_transfer_encoding_with_generator(self):

        class BodyGen(object):
            def __init__(self, data):
                self.data = data

            def __iter__(self):
                yield self.data

        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppNoContentLengthNoTransferEncoding(
                body=BodyGen(b'abc'),
            ), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'abc')
        self.assertEqual(log_parts[11], '3')

    def test_req_path_info_popping(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/v1/something', environ={'REQUEST_METHOD': 'GET'})
        req.path_info_pop()
        self.assertEqual(req.environ['PATH_INFO'], '/something')
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/v1/something')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'FAKE APP')
        self.assertEqual(log_parts[11], str(len(resp_body)))

    def test_ipv6(self):
        ipv6addr = '2001:db8:85a3:8d3:1319:8a2e:370:7348'
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.remote_addr = ipv6addr
        resp = app(req.environ, start_response)
        resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[0], ipv6addr)
        self.assertEqual(log_parts[1], ipv6addr)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'FAKE APP')
        self.assertEqual(log_parts[11], str(len(resp_body)))

    def test_log_info_none(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[17], '-')

        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.environ['swift.log_info'] = []
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[17], '-')

    def test_log_info_single(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.environ['swift.log_info'] = ['one']
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[17], 'one')

    def test_log_info_multiple(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.environ['swift.log_info'] = ['one', 'and two']
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[17], 'one%2Cand%20two')

    def test_log_auth_token(self):
        auth_token = 'b05bf940-0464-4c0e-8c70-87717d2d73e8'
        with mock.patch.object(registry, '_sensitive_headers', set()):
            # Default - reveal_sensitive_prefix is 16
            # No x-auth-token header
            app = proxy_logging.filter_factory({})(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], '-')
            # Has x-auth-token header
            app = proxy_logging.filter_factory({})(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'HTTP_X_AUTH_TOKEN': auth_token})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], 'b05bf940-0464-4c...', log_parts)

            # Truncate to first 8 characters
            app = proxy_logging.filter_factory(
                {'reveal_sensitive_prefix': '8'})(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], '-')
            app = proxy_logging.filter_factory(
                {'reveal_sensitive_prefix': '8'})(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'HTTP_X_AUTH_TOKEN': auth_token})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], 'b05bf940...')

            # Token length and reveal_sensitive_prefix are same (no truncate)
            app = proxy_logging.filter_factory(
                {'reveal_sensitive_prefix': str(len(auth_token))})(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'HTTP_X_AUTH_TOKEN': auth_token})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], auth_token)

            # No effective limit on auth token
            app = proxy_logging.filter_factory(
                {'reveal_sensitive_prefix': constraints.MAX_HEADER_SIZE}
            )(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'HTTP_X_AUTH_TOKEN': auth_token})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], auth_token)

            # Don't log x-auth-token
            app = proxy_logging.filter_factory(
                {'reveal_sensitive_prefix': '0'})(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], '-')
            app = proxy_logging.filter_factory(
                {'reveal_sensitive_prefix': '0'})(FakeApp())
            app.access_logger = debug_logger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                              'HTTP_X_AUTH_TOKEN': auth_token})
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
            log_parts = self._log_parts(app)
            self.assertEqual(log_parts[9], '...')

            # Avoids pyflakes error, "local variable 'resp_body' is assigned to
            # but never used
            self.assertTrue(resp_body is not None)

    def test_ensure_fields(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('time.time',
                        mock.MagicMock(
                            side_effect=[10000000.0, 10000000.5, 10000001.0])):
            resp = app(req.environ, start_response)
            resp_body = b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(len(log_parts), 21)
        self.assertEqual(log_parts[0], '-')
        self.assertEqual(log_parts[1], '-')
        self.assertEqual(log_parts[2], '26/Apr/1970/17/46/41')
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(log_parts[7], '-')
        self.assertEqual(log_parts[8], '-')
        self.assertEqual(log_parts[9], '-')
        self.assertEqual(log_parts[10], '-')
        self.assertEqual(resp_body, b'FAKE APP')
        self.assertEqual(log_parts[11], str(len(resp_body)))
        self.assertEqual(log_parts[12], '-')
        self.assertEqual(log_parts[13], '-')
        self.assertEqual(log_parts[14], '-')
        self.assertEqual(log_parts[15], '1.0000')
        self.assertEqual(log_parts[16], '-')
        self.assertEqual(log_parts[17], '-')
        self.assertEqual(log_parts[18], '10000000.000000000')
        self.assertEqual(log_parts[19], '10000001.000000000')
        self.assertEqual(log_parts[20], '-')

    def test_dual_logging_middlewares(self):
        # Since no internal request is being made, outer most proxy logging
        # middleware, log1, should have performed the logging.
        app = FakeApp()
        flg0 = debug_logger()
        env = {}
        log0 = proxy_logging.ProxyLoggingMiddleware(app, env, logger=flg0)
        flg1 = debug_logger()
        log1 = proxy_logging.ProxyLoggingMiddleware(log0, env, logger=flg1)

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = log1(req.environ, start_response)
        resp_body = b''.join(resp)
        self._log_parts(log0, should_be_empty=True)
        log_parts = self._log_parts(log1)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'FAKE APP')
        self.assertEqual(log_parts[11], str(len(resp_body)))

    def test_dual_logging_middlewares_w_inner(self):

        class FakeMiddleware(object):
            """
            Fake middleware to make a separate internal request, but construct
            the response with different data.
            """
            def __init__(self, app, conf):
                self.app = app
                self.conf = conf

            def GET(self, req):
                # Make the internal request
                ireq = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
                resp = self.app(ireq.environ, start_response)
                resp_body = b''.join(resp)
                if resp_body != b'FAKE APP':
                    return Response(request=req,
                                    body=b"FAKE APP WAS NOT RETURNED",
                                    content_type="text/plain")
                # But our response is different
                return Response(request=req, body=b"FAKE MIDDLEWARE",
                                content_type="text/plain")

            def __call__(self, env, start_response):
                req = Request(env)
                return self.GET(req)(env, start_response)

        # Since an internal request is being made, inner most proxy logging
        # middleware, log0, should have performed the logging.
        app = FakeApp()
        flg0 = debug_logger()
        env = {}
        log0 = proxy_logging.ProxyLoggingMiddleware(app, env, logger=flg0)
        fake = FakeMiddleware(log0, env)
        flg1 = debug_logger()
        log1 = proxy_logging.ProxyLoggingMiddleware(fake, env, logger=flg1)

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = log1(req.environ, start_response)
        resp_body = b''.join(resp)

        # Inner most logger should have logged the app's response
        log_parts = self._log_parts(log0)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(log_parts[11], str(len('FAKE APP')))

        # Outer most logger should have logged the other middleware's response
        log_parts = self._log_parts(log1)
        self.assertEqual(log_parts[3], 'GET')
        self.assertEqual(log_parts[4], '/')
        self.assertEqual(log_parts[5], 'HTTP/1.0')
        self.assertEqual(log_parts[6], '200')
        self.assertEqual(resp_body, b'FAKE MIDDLEWARE')
        self.assertEqual(log_parts[11], str(len(resp_body)))

    def test_policy_index(self):
        # Policy index can be specified by X-Backend-Storage-Policy-Index
        # in the request header for object API
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(policy_idx='1'), {})
        app.access_logger = debug_logger()
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'})
        resp = app(req.environ, start_response)
        b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[20], '1')

        # Policy index can be specified by X-Backend-Storage-Policy-Index
        # in the response header for container API
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()
        req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'GET'})

        def fake_call(app, env, start_response):
            start_response(app.response_str,
                           [('Content-Type', 'text/plain'),
                            ('Content-Length', str(sum(map(len, app.body)))),
                            ('X-Backend-Storage-Policy-Index', '1')])
            while env['wsgi.input'].read(5):
                pass
            return app.body

        with mock.patch.object(FakeApp, '__call__', fake_call):
            resp = app(req.environ, start_response)
            b''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEqual(log_parts[20], '1')

    def test_obscure_req(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = debug_logger()

        params = [('param_one',
                   'some_long_string_that_might_need_to_be_obscured'),
                  ('param_two',
                   "super_secure_param_that_needs_to_be_obscured")]
        headers = {'X-Auth-Token': 'this_is_my_auth_token',
                   'X-Other-Header': 'another_header_that_we_may_obscure'}

        req = Request.blank('a/c/o', environ={'REQUEST_METHOD': 'GET'},
                            headers=headers)
        req.params = params

        # if nothing is sensitive, nothing will be obscured
        with mock.patch.object(registry, '_sensitive_params', set()):
            with mock.patch.object(registry, '_sensitive_headers', set()):
                app.obscure_req(req)
        # show that nothing changed
        for header, expected_value in headers.items():
            self.assertEqual(req.headers[header], expected_value)

        for param, expected_value in params:
            self.assertEqual(req.params[param], expected_value)

        # If an obscured param or header doesn't exist in a req, that's fine
        with mock.patch.object(registry, '_sensitive_params', set()):
            with mock.patch.object(registry, '_sensitive_headers', set()):
                register_sensitive_header('X-Not-Exist')
                register_sensitive_param('non-existent-param')
                app.obscure_req(req)

        # show that nothing changed
        for header, expected_value in headers.items():
            self.assertEqual(req.headers[header], expected_value)

        for param, expected_value in params:
            self.assertEqual(req.params[param], expected_value)

        def obscured_test(params, headers, params_to_add, headers_to_add,
                          expected_params, expected_headers):
            with mock.patch.object(registry, '_sensitive_params', set()):
                with mock.patch.object(registry, '_sensitive_headers', set()):
                    app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
                    app.access_logger = debug_logger()
                    req = Request.blank('a/c/o',
                                        environ={'REQUEST_METHOD': 'GET'},
                                        headers=dict(headers))
                    req.params = params
                    for param in params_to_add:
                        register_sensitive_param(param)

                    for header in headers_to_add:
                        register_sensitive_header(header)

                    app.obscure_req(req)
                    for header, expected_value in expected_headers.items():
                        self.assertEqual(req.headers[header], expected_value)

                    for param, expected_value in expected_params:
                        self.assertEqual(req.params[param], expected_value)

        # first just 1 param
        expected_params = list(params)
        expected_params[0] = ('param_one', 'some_long_string...')
        obscured_test(params, headers, ['param_one'], [], expected_params,
                      headers)
        # case sensitive
        expected_params = list(params)
        obscured_test(params, headers, ['Param_one'], [], expected_params,
                      headers)
        # Other param
        expected_params = list(params)
        expected_params[1] = ('param_two', 'super_secure_par...')
        obscured_test(params, headers, ['param_two'], [], expected_params,
                      headers)
        # both
        expected_params[0] = ('param_one', 'some_long_string...')
        obscured_test(params, headers, ['param_two', 'param_one'], [],
                      expected_params, headers)

        # Now the headers
        # first just 1 header
        expected_headers = headers.copy()
        expected_headers["X-Auth-Token"] = 'this_is_my_auth_...'
        obscured_test(params, headers, [], ['X-Auth-Token'], params,
                      expected_headers)
        # case insensitive
        obscured_test(params, headers, [], ['x-auth-token'], params,
                      expected_headers)
        # Other headers
        expected_headers = headers.copy()
        expected_headers["X-Other-Header"] = 'another_header_t...'
        obscured_test(params, headers, [], ['X-Other-Header'], params,
                      expected_headers)
        # both
        expected_headers["X-Auth-Token"] = 'this_is_my_auth_...'
        obscured_test(params, headers, [], ['X-Auth-Token', 'X-Other-Header'],
                      params, expected_headers)

        # all together
        obscured_test(params, headers, ['param_two', 'param_one'],
                      ['X-Auth-Token', 'X-Other-Header'],
                      expected_params, expected_headers)
