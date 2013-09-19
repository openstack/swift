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

import unittest
import time
from urllib import unquote
import cStringIO as StringIO
from logging.handlers import SysLogHandler

from test.unit import FakeLogger
from swift.common.utils import get_logger
from swift.common.middleware import proxy_logging
from swift.common.swob import Request


class FakeApp(object):

    def __init__(self, body=['FAKE APP'], response_str='200 OK'):
        self.body = body
        self.response_str = response_str

    def __call__(self, env, start_response):
        start_response(self.response_str,
                       [('Content-Type', 'text/plain'),
                        ('Content-Length', str(sum(map(len, self.body))))])
        while env['wsgi.input'].read(5):
            pass
        return self.body


class FakeAppNoContentLengthNoTransferEncoding(object):

    def __init__(self, body=['FAKE APP']):
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
        return ["FAKE APP"]


def start_response(*args):
    pass


class TestProxyLogging(unittest.TestCase):

    def _log_parts(self, app, should_be_empty=False):
        info_calls = app.access_logger.log_dict['info']
        if should_be_empty:
            self.assertEquals([], info_calls)
        else:
            self.assertEquals(1, len(info_calls))
            return info_calls[0][0][0].split(' ')

    def assertTiming(self, exp_metric, app, exp_timing=None):
        timing_calls = app.access_logger.log_dict['timing']
        found = False
        for timing_call in timing_calls:
            self.assertEquals({}, timing_call[1])
            self.assertEquals(2, len(timing_call[0]))
            if timing_call[0][0] == exp_metric:
                found = True
                if exp_timing is not None:
                    self.assertAlmostEqual(exp_timing, timing_call[0][1],
                                           places=4)
        if not found:
            self.assertTrue(False, 'assertTiming: %s not found in %r' % (
                exp_metric, timing_calls))

    def assertTimingSince(self, exp_metric, app, exp_start=None):
        timing_calls = app.access_logger.log_dict['timing_since']
        found = False
        for timing_call in timing_calls:
            self.assertEquals({}, timing_call[1])
            self.assertEquals(2, len(timing_call[0]))
            if timing_call[0][0] == exp_metric:
                found = True
                if exp_start is not None:
                    self.assertAlmostEqual(exp_start, timing_call[0][1],
                                           places=4)
        if not found:
            self.assertTrue(False, 'assertTimingSince: %s not found in %r' % (
                exp_metric, timing_calls))

    def assertNotTiming(self, not_exp_metric, app):
        timing_calls = app.access_logger.log_dict['timing']
        for timing_call in timing_calls:
            self.assertNotEqual(not_exp_metric, timing_call[0][0])

    def assertUpdateStats(self, exp_metric, exp_bytes, app):
        update_stats_calls = app.access_logger.log_dict['update_stats']
        self.assertEquals(1, len(update_stats_calls))
        self.assertEquals({}, update_stats_calls[0][1])
        self.assertEquals((exp_metric, exp_bytes), update_stats_calls[0][0])

    def test_log_request_statsd_invalid_stats_types(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        for url in ['/', '/foo', '/foo/bar', '/v1']:
            req = Request.blank(url, environ={'REQUEST_METHOD': 'GET'})
            resp = app(req.environ, start_response)
            # get body
            ''.join(resp)
            self.assertEqual([], app.access_logger.log_dict['timing'])
            self.assertEqual([], app.access_logger.log_dict['update_stats'])

    def test_log_request_stat_type_bad(self):
        for bad_path in ['', '/', '/bad', '/baddy/mc_badderson', '/v1',
                         '/v1/']:
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
            app.access_logger = FakeLogger()
            req = Request.blank(bad_path, environ={'REQUEST_METHOD': 'GET'})
            app.log_request(req, 123, 7, 13, 2.71828182846)
            self.assertEqual([], app.access_logger.log_dict['timing'])
            self.assertEqual([], app.access_logger.log_dict['update_stats'])

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
        }
        for path, exp_type in path_types.iteritems():
            orig_time = time.time
            try:
                time.time = stub_time
                # GET
                app = proxy_logging.ProxyLoggingMiddleware(
                    FakeApp(body='7654321', response_str='321 Fubar'), {})
                app.access_logger = FakeLogger()
                req = Request.blank(path, environ={
                    'REQUEST_METHOD': 'GET',
                    'wsgi.input': StringIO.StringIO('4321')})
                stub_times = [18.0, 20.71828182846]
                iter_response = app(req.environ, lambda *_: None)
                self.assertEqual('7654321', ''.join(iter_response))
                self.assertTiming('%s.GET.321.timing' % exp_type, app,
                                  exp_timing=2.71828182846 * 1000)
                self.assertTimingSince(
                    '%s.GET.321.first-byte.timing' % exp_type, app,
                    exp_start=18.0)
                self.assertUpdateStats('%s.GET.321.xfer' % exp_type,
                                       4 + 7, app)

                # GET with swift.proxy_access_log_made already set
                app = proxy_logging.ProxyLoggingMiddleware(
                    FakeApp(body='7654321', response_str='321 Fubar'), {})
                app.access_logger = FakeLogger()
                req = Request.blank(path, environ={
                    'REQUEST_METHOD': 'GET',
                    'swift.proxy_access_log_made': True,
                    'wsgi.input': StringIO.StringIO('4321')})
                stub_times = [18.0, 20.71828182846]
                iter_response = app(req.environ, lambda *_: None)
                self.assertEqual('7654321', ''.join(iter_response))
                self.assertEqual([], app.access_logger.log_dict['timing'])
                self.assertEqual([],
                                 app.access_logger.log_dict['timing_since'])
                self.assertEqual([],
                                 app.access_logger.log_dict['update_stats'])

                # PUT (no first-byte timing!)
                app = proxy_logging.ProxyLoggingMiddleware(
                    FakeApp(body='87654321', response_str='314 PiTown'), {})
                app.access_logger = FakeLogger()
                req = Request.blank(path, environ={
                    'REQUEST_METHOD': 'PUT',
                    'wsgi.input': StringIO.StringIO('654321')})
                # (it's not a GET, so time() doesn't have a 2nd call)
                stub_times = [58.2, 58.2 + 7.3321]
                iter_response = app(req.environ, lambda *_: None)
                self.assertEqual('87654321', ''.join(iter_response))
                self.assertTiming('%s.PUT.314.timing' % exp_type, app,
                                  exp_timing=7.3321 * 1000)
                self.assertNotTiming(
                    '%s.GET.314.first-byte.timing' % exp_type, app)
                self.assertNotTiming(
                    '%s.PUT.314.first-byte.timing' % exp_type, app)
                self.assertUpdateStats(
                    '%s.PUT.314.xfer' % exp_type, 6 + 8, app)
            finally:
                time.time = orig_time

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
        for method, exp_method in method_map.iteritems():
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
            app.access_logger = FakeLogger()
            req = Request.blank('/v1/a/', environ={'REQUEST_METHOD': method})
            app.log_request(req, 299, 11, 3, 1.17)
            self.assertTiming('account.%s.299.timing' % exp_method, app,
                              exp_timing=1.17 * 1000)
            self.assertUpdateStats('account.%s.299.xfer' % exp_method,
                                   11 + 3, app)

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
            for method, exp_method in method_map.iteritems():
                app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
                    conf_key: 'SPECIAL,  GET,PUT ',  # crazy spaces ok
                })
                app.access_logger = FakeLogger()
                req = Request.blank('/v1/a/c',
                                    environ={'REQUEST_METHOD': method})
                app.log_request(req, 911, 4, 43, 1.01)
                self.assertTiming('container.%s.911.timing' % exp_method, app,
                                  exp_timing=1.01 * 1000)
                self.assertUpdateStats('container.%s.911.xfer' % exp_method,
                                       4 + 43, app)

    def test_basic_req(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'FAKE APP')
        self.assertEquals(log_parts[11], str(len(resp_body)))

    def test_basic_req_second_time(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={
            'swift.proxy_access_log_made': True,
            'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        self._log_parts(app, should_be_empty=True)
        self.assertEquals(resp_body, 'FAKE APP')

    def test_multi_segment_resp(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(
            ['some', 'chunks', 'of data']), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'swift.source': 'SOS'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'somechunksof data')
        self.assertEquals(log_parts[11], str(len(resp_body)))
        self.assertUpdateStats('SOS.GET.200.xfer', len(resp_body), app)

    def test_log_headers(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                                                   {'log_headers': 'yes'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        headers = unquote(log_parts[14]).split('\n')
        self.assert_('Host: localhost:80' in headers)

    def test_upload_size(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                                                   {'log_headers': 'yes'})
        app.access_logger = FakeLogger()
        req = Request.blank(
            '/v1/a/c/o/foo',
            environ={'REQUEST_METHOD': 'PUT',
                     'wsgi.input': StringIO.StringIO('some stuff')})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[11], str(len('FAKE APP')))
        self.assertEquals(log_parts[10], str(len('some stuff')))
        self.assertUpdateStats('object.PUT.200.xfer',
                               len('some stuff') + len('FAKE APP'),
                               app)

    def test_upload_line(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeAppReadline(),
                                                   {'log_headers': 'yes'})
        app.access_logger = FakeLogger()
        req = Request.blank(
            '/v1/a/c',
            environ={'REQUEST_METHOD': 'POST',
                     'wsgi.input': StringIO.StringIO(
                         'some stuff\nsome other stuff\n')})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[11], str(len('FAKE APP')))
        self.assertEquals(log_parts[10], str(len('some stuff\n')))
        self.assertUpdateStats('container.POST.200.xfer',
                               len('some stuff\n') + len('FAKE APP'),
                               app)

    def test_log_query_string(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'QUERY_STRING': 'x=3'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(unquote(log_parts[4]), '/?x=3')

    def test_client_logging(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'REMOTE_ADDR': '1.2.3.4'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[0], '1.2.3.4')  # client ip
        self.assertEquals(log_parts[1], '1.2.3.4')  # remote addr

    def test_proxy_client_logging(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={
            'REQUEST_METHOD': 'GET',
            'REMOTE_ADDR': '1.2.3.4',
            'HTTP_X_FORWARDED_FOR': '4.5.6.7,8.9.10.11'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[0], '4.5.6.7')  # client ip
        self.assertEquals(log_parts[1], '1.2.3.4')  # remote addr

        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={
            'REQUEST_METHOD': 'GET',
            'REMOTE_ADDR': '1.2.3.4',
            'HTTP_X_CLUSTER_CLIENT_IP': '4.5.6.7'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[0], '4.5.6.7')  # client ip
        self.assertEquals(log_parts[1], '1.2.3.4')  # remote addr

    def test_facility(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(),
            {'log_headers': 'yes',
             'access_log_facility': 'LOG_LOCAL7'})
        handler = get_logger.handler4logger[app.access_logger.logger]
        self.assertEquals(SysLogHandler.LOG_LOCAL7, handler.facility)

    def test_filter(self):
        factory = proxy_logging.filter_factory({})
        self.assert_(callable(factory))
        self.assert_(callable(factory(FakeApp())))

    def test_unread_body(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(['some', 'stuff']), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        # read first chunk
        next(resp)
        resp.close()  # raise a GeneratorExit in middleware app_iter loop
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[6], '499')
        self.assertEquals(log_parts[11], '4')  # write length

    def test_disconnect_on_readline(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeAppReadline(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'wsgi.input': FileLikeExceptor()})
        try:
            resp = app(req.environ, start_response)
            # read body
            ''.join(resp)
        except IOError:
            pass
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[6], '499')
        self.assertEquals(log_parts[10], '-')  # read length

    def test_disconnect_on_read(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(['some', 'stuff']), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'wsgi.input': FileLikeExceptor()})
        try:
            resp = app(req.environ, start_response)
            # read body
            ''.join(resp)
        except IOError:
            pass
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[6], '499')
        self.assertEquals(log_parts[10], '-')  # read length

    def test_no_content_length_no_transfer_encoding_with_list_body(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppNoContentLengthNoTransferEncoding(
                # test the "while not chunk: chunk = iterator.next()"
                body=['', '', 'line1\n', 'line2\n'],
            ), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'line1\nline2\n')
        self.assertEquals(log_parts[11], str(len(resp_body)))

    def test_no_content_length_no_transfer_encoding_with_empty_strings(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppNoContentLengthNoTransferEncoding(
                # test the "while not chunk: chunk = iterator.next()"
                body=['', '', ''],
            ), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, '')
        self.assertEquals(log_parts[11], '-')

    def test_req_path_info_popping(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/v1/something', environ={'REQUEST_METHOD': 'GET'})
        req.path_info_pop()
        self.assertEquals(req.environ['PATH_INFO'], '/something')
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/v1/something')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'FAKE APP')
        self.assertEquals(log_parts[11], str(len(resp_body)))

    def test_ipv6(self):
        ipv6addr = '2001:db8:85a3:8d3:1319:8a2e:370:7348'
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.remote_addr = ipv6addr
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[0], ipv6addr)
        self.assertEquals(log_parts[1], ipv6addr)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'FAKE APP')
        self.assertEquals(log_parts[11], str(len(resp_body)))

    def test_log_info_none(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[17], '-')

        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.environ['swift.log_info'] = []
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[17], '-')

    def test_log_info_single(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.environ['swift.log_info'] = ['one']
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[17], 'one')

    def test_log_info_multiple(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        req.environ['swift.log_info'] = ['one', 'and two']
        list(app(req.environ, start_response))
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[17], 'one%2Cand%20two')

    def test_log_auth_token(self):
        auth_token = 'b05bf940-0464-4c0e-8c70-87717d2d73e8'

        # Default - no reveal_sensitive_prefix in config
        # No x-auth-token header
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[9], '-')
        # Has x-auth-token header
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'HTTP_X_AUTH_TOKEN': auth_token})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[9], auth_token)

        # Truncate to first 8 characters
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'reveal_sensitive_prefix': '8'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[9], '-')
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'reveal_sensitive_prefix': '8'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'HTTP_X_AUTH_TOKEN': auth_token})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[9], 'b05bf940...')

        # Token length and reveal_sensitive_prefix are same (no truncate)
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'reveal_sensitive_prefix': str(len(auth_token))})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'HTTP_X_AUTH_TOKEN': auth_token})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[9], auth_token)

        # Don't log x-auth-token
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'reveal_sensitive_prefix': '0'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[9], '-')
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'reveal_sensitive_prefix': '0'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'HTTP_X_AUTH_TOKEN': auth_token})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[9], '...')

        # Avoids pyflakes error, "local variable 'resp_body' is assigned to
        # but never used
        self.assertTrue(resp_body is not None)

if __name__ == '__main__':
    unittest.main()
