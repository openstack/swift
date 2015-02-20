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
from urllib import unquote
import cStringIO as StringIO
from logging.handlers import SysLogHandler
import mock

from test.unit import FakeLogger
from swift.common.utils import get_logger
from swift.common.middleware import proxy_logging
from swift.common.swob import Request, Response
from swift.common import constraints


class FakeApp(object):

    def __init__(self, body=None, response_str='200 OK'):
        if body is None:
            body = ['FAKE APP']

        self.body = body
        self.response_str = response_str

    def __call__(self, env, start_response):
        start_response(self.response_str,
                       [('Content-Type', 'text/plain'),
                        ('Content-Length', str(sum(map(len, self.body))))])
        while env['wsgi.input'].read(5):
            pass
        return self.body


class FakeAppThatExcepts(object):

    def __call__(self, env, start_response):
        raise Exception("We take exception to that!")


class FakeAppNoContentLengthNoTransferEncoding(object):

    def __init__(self, body=None):
        if body is None:
            body = ['FAKE APP']

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
            now = 10000.0
            app.log_request(req, 123, 7, 13, now, now + 2.71828182846)
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
        with mock.patch("time.time", stub_time):
            for path, exp_type in path_types.iteritems():
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
            now = 10000.0
            app.log_request(req, 299, 11, 3, now, now + 1.17)
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
                now = 10000.0
                app.log_request(req, 911, 4, 43, now, now + 1.01)
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
        for conf_key in ['access_log_headers', 'log_headers']:
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                                                       {conf_key: 'yes'})
            app.access_logger = FakeLogger()
            req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
            resp = app(req.environ, start_response)
            # exhaust generator
            [x for x in resp]
            log_parts = self._log_parts(app)
            headers = unquote(log_parts[14]).split('\n')
            self.assert_('Host: localhost:80' in headers)

    def test_access_log_headers_only(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeApp(), {'log_headers': 'yes',
                        'access_log_headers_only': 'FIRST, seCond'})
        app.access_logger = FakeLogger()
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
        self.assert_('First: 1' in headers)
        self.assert_('Second: 2' in headers)
        self.assert_('Third: 3' not in headers)
        self.assert_('Host: localhost:80' not in headers)

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

    def test_iterator_closing(self):

        class CloseableBody(object):
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

            def __iter__(self):
                return iter(["CloseableBody"])

        body = CloseableBody()
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(body), {})
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                                          'REMOTE_ADDR': '1.2.3.4'})
        resp = app(req.environ, start_response)
        # exhaust generator
        [x for x in resp]
        self.assertTrue(body.closed)

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

    def test_app_exception(self):
        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppThatExcepts(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        try:
            app(req.environ, start_response)
        except Exception:
            pass
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[6], '500')
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

    def test_no_content_length_no_transfer_encoding_with_generator(self):

        class BodyGen(object):
            def __init__(self, data):
                self.data = data

            def __iter__(self):
                yield self.data

        app = proxy_logging.ProxyLoggingMiddleware(
            FakeAppNoContentLengthNoTransferEncoding(
                body=BodyGen('abc'),
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
        self.assertEquals(resp_body, 'abc')
        self.assertEquals(log_parts[11], '3')

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

        # Default - reveal_sensitive_prefix is 16
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
        self.assertEquals(log_parts[9], 'b05bf940-0464-4c...')

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

        # No effective limit on auth token
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {
            'reveal_sensitive_prefix': constraints.MAX_HEADER_SIZE})
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

    def test_ensure_fields(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        with mock.patch('time.time',
                        mock.MagicMock(
                            side_effect=[10000000.0, 10000001.0])):
            resp = app(req.environ, start_response)
            resp_body = ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(len(log_parts), 21)
        self.assertEquals(log_parts[0], '-')
        self.assertEquals(log_parts[1], '-')
        self.assertEquals(log_parts[2], '26/Apr/1970/17/46/41')
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(log_parts[7], '-')
        self.assertEquals(log_parts[8], '-')
        self.assertEquals(log_parts[9], '-')
        self.assertEquals(log_parts[10], '-')
        self.assertEquals(resp_body, 'FAKE APP')
        self.assertEquals(log_parts[11], str(len(resp_body)))
        self.assertEquals(log_parts[12], '-')
        self.assertEquals(log_parts[13], '-')
        self.assertEquals(log_parts[14], '-')
        self.assertEquals(log_parts[15], '1.0000')
        self.assertEquals(log_parts[16], '-')
        self.assertEquals(log_parts[17], '-')
        self.assertEquals(log_parts[18], '10000000.000000000')
        self.assertEquals(log_parts[19], '10000001.000000000')
        self.assertEquals(log_parts[20], '-')

    def test_dual_logging_middlewares(self):
        # Since no internal request is being made, outer most proxy logging
        # middleware, log1, should have performed the logging.
        app = FakeApp()
        flg0 = FakeLogger()
        env = {}
        log0 = proxy_logging.ProxyLoggingMiddleware(app, env, logger=flg0)
        flg1 = FakeLogger()
        log1 = proxy_logging.ProxyLoggingMiddleware(log0, env, logger=flg1)

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = log1(req.environ, start_response)
        resp_body = ''.join(resp)
        self._log_parts(log0, should_be_empty=True)
        log_parts = self._log_parts(log1)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'FAKE APP')
        self.assertEquals(log_parts[11], str(len(resp_body)))

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
                resp_body = ''.join(resp)
                if resp_body != 'FAKE APP':
                    return Response(request=req,
                                    body="FAKE APP WAS NOT RETURNED",
                                    content_type="text/plain")
                # But our response is different
                return Response(request=req, body="FAKE MIDDLEWARE",
                                content_type="text/plain")

            def __call__(self, env, start_response):
                req = Request(env)
                return self.GET(req)(env, start_response)

        # Since an internal request is being made, inner most proxy logging
        # middleware, log0, should have performed the logging.
        app = FakeApp()
        flg0 = FakeLogger()
        env = {}
        log0 = proxy_logging.ProxyLoggingMiddleware(app, env, logger=flg0)
        fake = FakeMiddleware(log0, env)
        flg1 = FakeLogger()
        log1 = proxy_logging.ProxyLoggingMiddleware(fake, env, logger=flg1)

        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = log1(req.environ, start_response)
        resp_body = ''.join(resp)

        # Inner most logger should have logged the app's response
        log_parts = self._log_parts(log0)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(log_parts[11], str(len('FAKE APP')))

        # Outer most logger should have logged the other middleware's response
        log_parts = self._log_parts(log1)
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'FAKE MIDDLEWARE')
        self.assertEquals(log_parts[11], str(len(resp_body)))

    def test_policy_index(self):
        # Policy index can be specified by X-Backend-Storage-Policy-Index
        # in the request header for object API
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/v1/a/c/o', environ={'REQUEST_METHOD': 'PUT'},
                            headers={'X-Backend-Storage-Policy-Index': '1'})
        resp = app(req.environ, start_response)
        ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[20], '1')

        # Policy index can be specified by X-Backend-Storage-Policy-Index
        # in the response header for container API
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
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
            ''.join(resp)
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[20], '1')


if __name__ == '__main__':
    unittest.main()
