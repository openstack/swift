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

from webob import Request

from swift.common.middleware import proxy_logging


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


class FakeLogger(object):
    def __init__(self, *args, **kwargs):
        self.msg = ''

    def info(self, string):
        self.msg = string


def start_response(*args):
    pass


class TestProxyLogging(unittest.TestCase):

    def test_basic_req(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = app.access_logger.msg.split()
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
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'somechunksof data')
        self.assertEquals(log_parts[11], str(len(resp_body)))

    def test_log_headers(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                    {'log_headers': 'yes'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
        log_parts = app.access_logger.msg.split()
        headers = unquote(log_parts[14]).split('\n')
        self.assert_('Host: localhost:80' in headers)

    def test_upload_size(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                    {'log_headers': 'yes'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
            'wsgi.input': StringIO.StringIO('some stuff')})
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[10], str(len('some stuff')))

    def test_upload_line(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeAppReadline(),
                    {'log_headers': 'yes'})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
            'wsgi.input': StringIO.StringIO(
                            'some stuff\nsome other stuff\n')})
        resp = app(req.environ, start_response)
        exhaust_generator = ''.join(resp)
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[10], str(len('some stuff\n')))

    def test_log_query_string(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                'QUERY_STRING': 'x=3'})
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
        log_parts = app.access_logger.msg.split()
        self.assertEquals(unquote(log_parts[4]), '/?x=3')

    def test_client_logging(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                'REMOTE_ADDR': '1.2.3.4'})
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[0], '1.2.3.4')  # client ip
        self.assertEquals(log_parts[1], '1.2.3.4')  # remote addr

    def test_proxy_client_logging(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                'REMOTE_ADDR': '1.2.3.4',
                'HTTP_X_FORWARDED_FOR': '4.5.6.7,8.9.10.11'
                })
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[0], '4.5.6.7')  # client ip
        self.assertEquals(log_parts[1], '1.2.3.4')  # remote addr

        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET',
                'REMOTE_ADDR': '1.2.3.4',
                'HTTP_X_CLUSTER_CLIENT_IP': '4.5.6.7'
                })
        resp = app(req.environ, start_response)
        exhaust_generator = [x for x in resp]
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[0], '4.5.6.7')  # client ip
        self.assertEquals(log_parts[1], '1.2.3.4')  # remote addr

    def test_facility(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(),
                    {'log_headers': 'yes', 'access_log_facility': 'whatever'})

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
        log_parts = app.access_logger.msg.split()
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
        log_parts = app.access_logger.msg.split()
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
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[6], '499')
        self.assertEquals(log_parts[10], '-')  # read length

    def test_no_content_length_no_transfer_encoding(self):
        app = proxy_logging.ProxyLoggingMiddleware(
                FakeAppNoContentLengthNoTransferEncoding(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = app(req.environ, start_response)
        body = ''.join(resp)

    def test_req_path_info_popping(self):
        app = proxy_logging.ProxyLoggingMiddleware(FakeApp(), {})
        app.access_logger = FakeLogger()
        req = Request.blank('/v1/something', environ={'REQUEST_METHOD': 'GET'})
        req.path_info_pop()
        self.assertEquals(req.environ['PATH_INFO'], '/something')
        resp = app(req.environ, start_response)
        resp_body = ''.join(resp)
        log_parts = app.access_logger.msg.split()
        self.assertEquals(log_parts[3], 'GET')
        self.assertEquals(log_parts[4], '/v1/something')
        self.assertEquals(log_parts[5], 'HTTP/1.0')
        self.assertEquals(log_parts[6], '200')
        self.assertEquals(resp_body, 'FAKE APP')
        self.assertEquals(log_parts[11], str(len(resp_body)))


if __name__ == '__main__':
    unittest.main()
