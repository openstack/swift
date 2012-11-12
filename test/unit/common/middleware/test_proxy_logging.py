# Copyright (c) 2010-2011 OpenStack, LLC.
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
from urllib import quote, unquote
import cStringIO as StringIO
from logging.handlers import SysLogHandler

from test.unit import FakeLogger
from swift.common.utils import get_logger
from swift.common.middleware import proxy_logging
from swift.common.swob import Request


class FakeApp(object):
    def __init__(self, body=['FAKE APP']):
        self.body = body

    def __call__(self, env, start_response):
        start_response('200 OK', [('Content-Type', 'text/plain'),
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
        line = env['wsgi.input'].readline()
        return ["FAKE APP"]


def start_response(*args):
    pass


class TestProxyLogging(unittest.TestCase):

    def _log_parts(self, app):
        info_calls = app.access_logger.log_dict['info']
        self.assertEquals(1, len(info_calls))
        return info_calls[0][0][0].split(' ')

    def assertTiming(self, exp_metric, app, exp_timing=None):
        timing_calls = app.access_logger.log_dict['timing']
        self.assertEquals(1, len(timing_calls))
        self.assertEquals({}, timing_calls[0][1])
        self.assertEquals(2, len(timing_calls[0][0]))
        self.assertEquals(exp_metric, timing_calls[0][0][0])
        if exp_timing is not None:
            self.assertEquals(exp_timing, timing_calls[0][0][1])

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
            resp_body = ''.join(resp)
            self.assertEquals(0, len(app.access_logger.log_dict['timing']))
            self.assertEquals(0,
                              len(app.access_logger.log_dict['update_stats']))

    def test_log_request_stat_type_bad(self):
        for bad_path in ['', '/', '/bad', '/baddy/mc_badderson', '/v1',
                         '/v1/']:
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
            app.access_logger = FakeLogger()
            req = Request.blank(bad_path, environ={'REQUEST_METHOD': 'GET'})
            app.log_request(req.environ, 123, 7, 13, 2.71828182846, False)
            self.assertEqual([], app.access_logger.log_dict['timing'])
            self.assertEqual([], app.access_logger.log_dict['update_stats'])

    def test_log_request_stat_type_good(self):
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
            app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
            app.access_logger = FakeLogger()
            req = Request.blank(path, environ={'REQUEST_METHOD': 'GET'})
            app.log_request(req.environ, 321, 7, 13, 2.71828182846, False)
            self.assertTiming('%s.GET.321.timing' % exp_type, app,
                              exp_timing=2.71828182846 * 1000)
            self.assertUpdateStats('%s.GET.321.xfer' % exp_type, 7 + 13, app)

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
            app.log_request(req.environ, 299, 11, 3, 1.17, False)
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
                req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': method})
                app.log_request(req.environ, 911, 4, 43, 1.01, False)
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
        exhaust_generator = [x for x in resp]
        log_parts = self._log_parts(app)
        headers = unquote(log_parts[14]).split('\n')
        self.assert_('Host: localhost:80' in headers)

    def test_upload_size(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                    {'log_headers': 'yes'})
        app.access_logger = FakeLogger()
        req = Request.blank('/v1/a/c/o/foo', environ={'REQUEST_METHOD': 'PUT',
            'wsgi.input': StringIO.StringIO('some stuff')})
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
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
        req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'POST',
            'wsgi.input': StringIO.StringIO(
                            'some stuff\nsome other stuff\n')})
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
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
        exhaust_generator = [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(unquote(log_parts[4]), '/?x=3')

    def test_client_logging(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                'REMOTE_ADDR': '1.2.3.4'})
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
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
        exhaust_generator = [x for x in resp]
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
        exhaust_generator = [x for x in resp]
        log_parts = self._log_parts(app)
        self.assertEquals(log_parts[0], '4.5.6.7')  # client ip
        self.assertEquals(log_parts[1], '1.2.3.4')  # remote addr

    def test_facility(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
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
        read_first_chunk = next(resp)
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
            body = ''.join(resp)
        except Exception:
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
            body = ''.join(resp)
        except Exception:
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


if __name__ == '__main__':
    unittest.main()
