# Copyright (c) 2011 OpenStack Foundation
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

import base64
import hmac
import hashlib
import unittest
from time import time

from unittest import mock
from io import BytesIO, StringIO

from swift.common.swob import Request, Response, wsgi_quote
from swift.common.middleware import tempauth, formpost
from swift.common.middleware.tempurl import DEFAULT_ALLOWED_DIGESTS
from swift.common.utils import split_path
from swift.common import registry, digest as digest_utils
from swift.proxy.controllers.base import get_cache_key
from test.debug_logger import debug_logger


def hmac_msg(path, redirect, max_file_size, max_file_count, expires):
    msg = '%s\n%s\n%s\n%s\n%s' % (
        path, redirect, max_file_size, max_file_count, expires)
    return msg.encode('utf-8')


class FakeApp(object):

    def __init__(self, status_headers_body_iter=None,
                 check_no_query_string=True):
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter([('404 Not Found', {
                'x-test-header-one-a': 'value1',
                'x-test-header-two-a': 'value2',
                'x-test-header-two-b': 'value3'}, b'')])
        self.requests = []
        self.check_no_query_string = check_no_query_string

    def __call__(self, env, start_response):
        # use wsgi_quote to spot check that it really *is* a WSGI string
        wsgi_quote(env['PATH_INFO'])
        try:
            if self.check_no_query_string and env.get('QUERY_STRING'):
                raise Exception('Query string %s should have been discarded!' %
                                env['QUERY_STRING'])
            body = b''
            while True:
                chunk = env['wsgi.input'].read()
                if not chunk:
                    break
                body += chunk
            env['wsgi.input'] = BytesIO(body)
            self.requests.append(Request.blank('', environ=env))
            if env.get('swift.authorize_override') and \
                    env.get('REMOTE_USER') != '.wsgi.pre_authed':
                raise Exception(
                    'Invalid REMOTE_USER %r with swift.authorize_override' % (
                        env.get('REMOTE_USER'),))
            if 'swift.authorize' in env:
                resp = env['swift.authorize'](self.requests[-1])
                if resp:
                    return resp(env, start_response)
            status, headers, body = next(self.status_headers_body_iter)
            return Response(status=status, headers=headers,
                            body=body)(env, start_response)
        except EOFError:
            start_response('499 Client Disconnect',
                           [('Content-Type', 'text/plain')])
            return [b'Client Disconnect\n']


class TestCappedFileLikeObject(unittest.TestCase):

    def test_whole(self):
        self.assertEqual(
            formpost._CappedFileLikeObject(BytesIO(b'abc'), 10).read(),
            b'abc')

    def test_exceeded(self):
        exc = None
        try:
            formpost._CappedFileLikeObject(BytesIO(b'abc'), 2).read()
        except EOFError as err:
            exc = err
        self.assertEqual(str(exc), 'max_file_size exceeded')

    def test_whole_readline(self):
        fp = formpost._CappedFileLikeObject(BytesIO(b'abc\ndef'), 10)
        self.assertEqual(fp.readline(), b'abc\n')
        self.assertEqual(fp.readline(), b'def')
        self.assertEqual(fp.readline(), b'')

    def test_exceeded_readline(self):
        fp = formpost._CappedFileLikeObject(BytesIO(b'abc\ndef'), 5)
        self.assertEqual(fp.readline(), b'abc\n')
        exc = None
        try:
            self.assertEqual(fp.readline(), b'def')
        except EOFError as err:
            exc = err
        self.assertEqual(str(exc), 'max_file_size exceeded')

    def test_read_sized(self):
        fp = formpost._CappedFileLikeObject(BytesIO(b'abcdefg'), 10)
        self.assertEqual(fp.read(2), b'ab')
        self.assertEqual(fp.read(2), b'cd')
        self.assertEqual(fp.read(2), b'ef')
        self.assertEqual(fp.read(2), b'g')
        self.assertEqual(fp.read(2), b'')


class TestFormPost(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        self.logger = self.formpost.logger = debug_logger()

    def _make_request(self, path, tempurl_keys=(), **kwargs):
        req = Request.blank(path, **kwargs)

        # Fake out the caching layer so that get_account_info() finds its
        # data. Include something that isn't tempurl keys to prove we skip it.
        meta = {'user-job-title': 'Personal Trainer',
                'user-real-name': 'Jim Shortz'}
        for idx, key in enumerate(tempurl_keys):
            meta_name = 'temp-url-key' + (("-%d" % (idx + 1) if idx else ""))
            if key:
                meta[meta_name] = key

        _junk, account, _junk, _junk = split_path(path, 2, 4)
        req.environ.setdefault('swift.infocache', {})
        req.environ['swift.infocache'][get_cache_key(account)] = \
            self._fake_cache_env(account, tempurl_keys)
        return req

    def _fake_cache_env(self, account, tempurl_keys=()):
        # Fake out the caching layer so that get_account_info() finds its
        # data. Include something that isn't tempurl keys to prove we skip it.
        meta = {'user-job-title': 'Personal Trainer',
                'user-real-name': 'Jim Shortz'}
        for idx, key in enumerate(tempurl_keys):
            meta_name = 'temp-url-key' + ("-%d" % (idx + 1) if idx else "")
            if key:
                meta[meta_name] = key

        return {'status': 204,
                'container_count': '0',
                'total_object_count': '0',
                'bytes': '0',
                'meta': meta}

    def _make_sig_env_body(self, path, redirect, max_file_size, max_file_count,
                           expires, key, user_agent=True, algorithm='sha512',
                           prefix=True):
        alg_name = algorithm
        mac = hmac.new(
            key,
            hmac_msg(path, redirect, max_file_size, max_file_count, expires),
            algorithm)
        if prefix:
            sig = alg_name + ':' + base64.b64encode(
                mac.digest()).decode('ascii')
        else:
            sig = mac.hexdigest()
        body = [
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file1"; '
            'filename="testfile1.txt"',
            'Content-Type: text/plain',
            '',
            'Test File\nOne\n',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file2"; '
            'filename="testfile2.txt"',
            'Content-Type: text/plain',
            'Content-Encoding: gzip',
            '',
            'Test\nFile\nTwo\n',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file3"; filename=""',
            'Content-Type: application/octet-stream',
            '',
            '',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR--',
            '',
        ]
        body = [line.encode('utf-8') for line in body]
        wsgi_errors = StringIO()
        env = {
            'CONTENT_TYPE': 'multipart/form-data; '
            'boundary=----WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'HTTP_ACCEPT_ENCODING': 'gzip, deflate',
            'HTTP_ACCEPT_LANGUAGE': 'en-us',
            'HTTP_ACCEPT': 'text/html,application/xhtml+xml,application/xml;'
            'q=0.9,*/*;q=0.8',
            'HTTP_CONNECTION': 'keep-alive',
            'HTTP_HOST': 'ubuntu:8080',
            'HTTP_ORIGIN': 'file://',
            'HTTP_USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X '
            '10_7_2) AppleWebKit/534.52.7 (KHTML, like Gecko) '
            'Version/5.1.2 Safari/534.52.7',
            'PATH_INFO': path,
            'REMOTE_ADDR': '172.16.83.1',
            'REQUEST_METHOD': 'POST',
            'SCRIPT_NAME': '',
            'SERVER_NAME': '172.16.83.128',
            'SERVER_PORT': '8080',
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'swift.infocache': {},
            'wsgi.errors': wsgi_errors,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        if user_agent is False:
            del env['HTTP_USER_AGENT']

        return sig, env, body

    def test_passthrough(self):
        for method in ('HEAD', 'GET', 'PUT', 'POST', 'DELETE'):
            resp = self._make_request(
                '/v1/a/c/o',
                environ={'REQUEST_METHOD': method}).get_response(self.formpost)
            self.assertEqual(resp.status_int, 401)
            self.assertNotIn(b'FormPost', resp.body)

    def test_auth_scheme(self):
        # FormPost rejects
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() - 10), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        authenticate_v = None
        for h, v in headers:
            if h.lower() == 'www-authenticate':
                authenticate_v = v
        self.assertTrue(b'FormPost: Form Expired' in body)
        self.assertEqual('Swift realm="unknown"', authenticate_v)

    def test_safari(self):
        key = b'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(
            key,
            hmac_msg(path, redirect, max_file_size, max_file_count, expires),
            hashlib.sha512).hexdigest()
        wsgi_input = '\r\n'.join([
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file1"; '
            'filename="testfile1.txt"',
            'Content-Type: text/plain',
            '',
            'Test File\nOne\n',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file2"; '
            'filename="testfile2.txt"',
            'Content-Type: text/plain',
            '',
            'Test\nFile\nTwo\n',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file3"; filename=""',
            'Content-Type: application/octet-stream',
            '',
            '',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR--',
            '',
        ])
        wsgi_input = BytesIO(wsgi_input.encode('utf-8'))
        wsgi_errors = StringIO()
        env = {
            'CONTENT_TYPE': 'multipart/form-data; '
            'boundary=----WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'HTTP_ACCEPT_ENCODING': 'gzip, deflate',
            'HTTP_ACCEPT_LANGUAGE': 'en-us',
            'HTTP_ACCEPT': 'text/html,application/xhtml+xml,application/xml;'
            'q=0.9,*/*;q=0.8',
            'HTTP_CONNECTION': 'keep-alive',
            'HTTP_HOST': 'ubuntu:8080',
            'HTTP_ORIGIN': 'file://',
            'HTTP_USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X '
            '10_7_2) AppleWebKit/534.52.7 (KHTML, like Gecko) '
            'Version/5.1.2 Safari/534.52.7',
            'PATH_INFO': path,
            'REMOTE_ADDR': '172.16.83.1',
            'REQUEST_METHOD': 'POST',
            'SCRIPT_NAME': '',
            'SERVER_NAME': '172.16.83.128',
            'SERVER_PORT': '8080',
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'swift.infocache': {
                get_cache_key('AUTH_test'): self._fake_cache_env(
                    'AUTH_test', [key]),
                get_cache_key('AUTH_test', 'container'): {
                    'meta': {}}},
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location, 'http://brim.net?status=201&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(b'http://brim.net?status=201&message=' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_firefox(self):
        key = b'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(
            key,
            hmac_msg(path, redirect, max_file_size, max_file_count, expires),
            hashlib.sha512).hexdigest()
        wsgi_input = '\r\n'.join([
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="file1"; '
            'filename="testfile1.txt"',
            'Content-Type: text/plain',
            '',
            'Test File\nOne\n',
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="file2"; '
            'filename="testfile2.txt"',
            'Content-Type: text/plain',
            '',
            'Test\nFile\nTwo\n',
            '-----------------------------168072824752491622650073',
            'Content-Disposition: form-data; name="file3"; filename=""',
            'Content-Type: application/octet-stream',
            '',
            '',
            '-----------------------------168072824752491622650073--',
            ''
        ])
        wsgi_input = BytesIO(wsgi_input.encode('utf-8'))
        wsgi_errors = StringIO()
        env = {
            'CONTENT_TYPE': 'multipart/form-data; '
            'boundary=---------------------------168072824752491622650073',
            'HTTP_ACCEPT_CHARSET': 'ISO-8859-1,utf-8;q=0.7,*;q=0.7',
            'HTTP_ACCEPT_ENCODING': 'gzip, deflate',
            'HTTP_ACCEPT_LANGUAGE': 'en-us,en;q=0.5',
            'HTTP_ACCEPT': 'text/html,application/xhtml+xml,application/xml;'
            'q=0.9,*/*;q=0.8',
            'HTTP_CONNECTION': 'keep-alive',
            'HTTP_HOST': 'ubuntu:8080',
            'HTTP_USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; '
            'rv:8.0.1) Gecko/20100101 Firefox/8.0.1',
            'PATH_INFO': '/v1/AUTH_test/container',
            'REMOTE_ADDR': '172.16.83.1',
            'REQUEST_METHOD': 'POST',
            'SCRIPT_NAME': '',
            'SERVER_NAME': '172.16.83.128',
            'SERVER_PORT': '8080',
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'swift.infocache': {
                get_cache_key('AUTH_test'): self._fake_cache_env(
                    'AUTH_test', [key]),
                get_cache_key('AUTH_test', 'container'): {
                    'meta': {}}},
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location, 'http://brim.net?status=201&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(b'http://brim.net?status=201&message=' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_chrome(self):
        key = b'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(
            key,
            hmac_msg(path, redirect, max_file_size, max_file_count, expires),
            hashlib.sha512).hexdigest()
        wsgi_input = '\r\n'.join([
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="file1"; '
            'filename="testfile1.txt"',
            'Content-Type: text/plain',
            '',
            'Test File\nOne\n',
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="file2"; '
            'filename="testfile2.txt"',
            'Content-Type: text/plain',
            '',
            'Test\nFile\nTwo\n',
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'Content-Disposition: form-data; name="file3"; filename=""',
            'Content-Type: application/octet-stream',
            '',
            '',
            '------WebKitFormBoundaryq3CFxUjfsDMu8XsA--',
            ''
        ])
        wsgi_input = BytesIO(wsgi_input.encode('utf-8'))
        wsgi_errors = StringIO()
        env = {
            'CONTENT_TYPE': 'multipart/form-data; '
            'boundary=----WebKitFormBoundaryq3CFxUjfsDMu8XsA',
            'HTTP_ACCEPT_CHARSET': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
            'HTTP_ACCEPT_ENCODING': 'gzip,deflate,sdch',
            'HTTP_ACCEPT_LANGUAGE': 'en-US,en;q=0.8',
            'HTTP_ACCEPT': 'text/html,application/xhtml+xml,application/xml;'
            'q=0.9,*/*;q=0.8',
            'HTTP_CACHE_CONTROL': 'max-age=0',
            'HTTP_CONNECTION': 'keep-alive',
            'HTTP_HOST': 'ubuntu:8080',
            'HTTP_ORIGIN': 'null',
            'HTTP_USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X '
            '10_7_2) AppleWebKit/535.7 (KHTML, like Gecko) '
            'Chrome/16.0.912.63 Safari/535.7',
            'PATH_INFO': '/v1/AUTH_test/container',
            'REMOTE_ADDR': '172.16.83.1',
            'REQUEST_METHOD': 'POST',
            'SCRIPT_NAME': '',
            'SERVER_NAME': '172.16.83.128',
            'SERVER_PORT': '8080',
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'swift.infocache': {
                get_cache_key('AUTH_test'): self._fake_cache_env(
                    'AUTH_test', [key]),
                get_cache_key('AUTH_test', 'container'): {
                    'meta': {}}},
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location, 'http://brim.net?status=201&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(b'http://brim.net?status=201&message=' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_explorer(self):
        key = b'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(
            key,
            hmac_msg(path, redirect, max_file_size, max_file_count, expires),
            hashlib.sha512).hexdigest()
        wsgi_input = '\r\n'.join([
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="file1"; '
            'filename="C:\\testfile1.txt"',
            'Content-Type: text/plain',
            '',
            'Test File\nOne\n',
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="file2"; '
            'filename="C:\\testfile2.txt"',
            'Content-Type: text/plain',
            '',
            'Test\nFile\nTwo\n',
            '-----------------------------7db20d93017c',
            'Content-Disposition: form-data; name="file3"; filename=""',
            'Content-Type: application/octet-stream',
            '',
            '',
            '-----------------------------7db20d93017c--',
            ''
        ])
        wsgi_input = BytesIO(wsgi_input.encode('utf-8'))
        wsgi_errors = StringIO()
        env = {
            'CONTENT_TYPE': 'multipart/form-data; '
            'boundary=---------------------------7db20d93017c',
            'HTTP_ACCEPT_ENCODING': 'gzip, deflate',
            'HTTP_ACCEPT_LANGUAGE': 'en-US',
            'HTTP_ACCEPT': 'text/html, application/xhtml+xml, */*',
            'HTTP_CACHE_CONTROL': 'no-cache',
            'HTTP_CONNECTION': 'Keep-Alive',
            'HTTP_HOST': '172.16.83.128:8080',
            'HTTP_USER_AGENT': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT '
            '6.1; WOW64; Trident/5.0)',
            'PATH_INFO': '/v1/AUTH_test/container',
            'REMOTE_ADDR': '172.16.83.129',
            'REQUEST_METHOD': 'POST',
            'SCRIPT_NAME': '',
            'SERVER_NAME': '172.16.83.128',
            'SERVER_PORT': '8080',
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'swift.infocache': {
                get_cache_key('AUTH_test'): self._fake_cache_env(
                    'AUTH_test', [key]),
                get_cache_key('AUTH_test', 'container'): {
                    'meta': {}}},
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location, 'http://brim.net?status=201&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(b'http://brim.net?status=201&message=' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_curl_with_unicode(self):
        key = b'abc'
        path = u'/v1/AUTH_test/container/let_it_\N{SNOWFLAKE}/'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(
            key,
            hmac_msg(path, redirect, max_file_size, max_file_count, expires),
            hashlib.sha512).hexdigest()
        wsgi_input = '\r\n'.join([
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="file1"; '
            'filename="\xe2\x98\x83.txt"',
            'Content-Type: text/plain',
            '',
            'Test File\nOne\n',
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="file2"; '
            'filename="testfile2.txt"',
            'Content-Type: text/plain',
            '',
            'Test\nFile\nTwo\n',
            '--------------------------dea19ac8502ca805',
            'Content-Disposition: form-data; name="file3"; filename=""',
            'Content-Type: application/octet-stream',
            '',
            '',
            '--------------------------dea19ac8502ca805--',
            ''
        ]).encode('latin1')
        wsgi_input = BytesIO(wsgi_input)
        wsgi_errors = StringIO()
        env = {
            'CONTENT_LENGTH': str(len(wsgi_input.getvalue())),
            'CONTENT_TYPE': 'multipart/form-data; '
            'boundary=------------------------dea19ac8502ca805',
            'HTTP_ACCEPT': '*/*',
            'HTTP_HOST': 'ubuntu:8080',
            'HTTP_USER_AGENT': 'curl/7.58.0',
            'PATH_INFO': '/v1/AUTH_test/container/let_it_\xE2\x9D\x84/',
            'REMOTE_ADDR': '172.16.83.1',
            'REQUEST_METHOD': 'POST',
            'SCRIPT_NAME': '',
            'SERVER_NAME': '172.16.83.128',
            'SERVER_PORT': '8080',
            'SERVER_PROTOCOL': 'HTTP/1.0',
            'swift.infocache': {
                get_cache_key('AUTH_test'): self._fake_cache_env(
                    'AUTH_test', [key]),
                get_cache_key('AUTH_test', 'container'): {
                    'meta': {}}},
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other', body)
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location, 'http://brim.net?status=201&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(b'http://brim.net?status=201&message=' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(
            self.app.requests[0].path,
            '/v1/AUTH_test/container/let_it_%E2%9D%84/%E2%98%83.txt')
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(
            self.app.requests[1].path,
            '/v1/AUTH_test/container/let_it_%E2%9D%84/testfile2.txt')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_messed_up_start(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://brim.net', 5, 10,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'XX' + b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)

        def log_assert_int_status(env, response_status_int):
            self.assertIsInstance(response_status_int, int)

        self.formpost._log_request = log_assert_int_status
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '400 Bad Request')
        self.assertIsNone(exc_info)
        self.assertIn(b'FormPost: invalid starting boundary', body)
        self.assertEqual(len(self.app.requests), 0)

    def test_max_file_size_exceeded(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://brim.net', 5, 10,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '400 Bad Request')
        self.assertIsNone(exc_info)
        self.assertIn(b'FormPost: max_file_size exceeded', body)
        self.assertEqual(len(self.app.requests), 0)

    def test_max_file_count_exceeded(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://brim.net', 1024, 1,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(
            location,
            'http://brim.net?status=400&message=max%20file%20count%20exceeded')
        self.assertIsNone(exc_info)
        self.assertTrue(
            b'http://brim.net?status=400&message=max%20file%20count%20exceeded'
            in body)
        self.assertEqual(len(self.app.requests), 1)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')

    def test_subrequest_does_not_pass_query(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['QUERY_STRING'] = 'this=should&not=get&passed'
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(
            iter([('201 Created', {}, b''),
                  ('201 Created', {}, b'')]),
            check_no_query_string=True)
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        # Make sure we 201 Created, which means we made the final subrequest
        # (and FakeApp verifies that no QUERY_STRING got passed).
        self.assertEqual(status, '201 Created')
        self.assertIsNone(exc_info)
        self.assertTrue(b'201 Created' in body)
        self.assertEqual(len(self.app.requests), 2)

    def test_subrequest_fails_redirect_404(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://brim.net', 1024, 10,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('404 Not Found', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location, 'http://brim.net?status=404&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(b'http://brim.net?status=404&message=' in body)
        self.assertEqual(len(self.app.requests), 1)

    def test_subrequest_fails_no_redirect_503(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('503 Server Error', {}, b'some bad news')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '503 Server Error')
        self.assertTrue(b'bad news' in body)
        self.assertEqual(len(self.app.requests), 1)

    def test_truncated_attr_value(self):
        key = b'abc'
        redirect = 'a' * formpost.MAX_VALUE_LENGTH
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', redirect, max_file_size, max_file_count,
            expires, key)
        # Tack on an extra char to redirect, but shouldn't matter since it
        # should get truncated off on read.
        redirect += 'b'
        wsgi_input = '\r\n'.join([
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file1"; '
            'filename="testfile1.txt"',
            'Content-Type: text/plain',
            '',
            'Test File\nOne\n',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file2"; '
            'filename="testfile2.txt"',
            'Content-Type: text/plain',
            '',
            'Test\nFile\nTwo\n',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file3"; filename=""',
            'Content-Type: application/octet-stream',
            '',
            '',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR--',
            '',
        ])
        env['wsgi.input'] = BytesIO(wsgi_input.encode('utf-8'))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(
            location,
            ('a' * formpost.MAX_VALUE_LENGTH) + '?status=201&message=')
        self.assertIsNone(exc_info)
        self.assertIn(
            (b'a' * formpost.MAX_VALUE_LENGTH) + b'?status=201&message=', body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_no_file_to_process(self):
        key = b'abc'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', redirect, max_file_size, max_file_count,
            expires, key)
        wsgi_input = '\r\n'.join([
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="redirect"',
            '',
            redirect,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_size"',
            '',
            str(max_file_size),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="max_file_count"',
            '',
            str(max_file_count),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="expires"',
            '',
            str(expires),
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="signature"',
            '',
            sig,
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR--',
            '',
        ])
        env['wsgi.input'] = BytesIO(wsgi_input.encode('utf-8'))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(
            location,
            'http://brim.net?status=400&message=no%20files%20to%20process')
        self.assertIsNone(exc_info)
        self.assertTrue(
            b'http://brim.net?status=400&message=no%20files%20to%20process'
            in body)
        self.assertEqual(len(self.app.requests), 0)

    def test_formpost_without_useragent(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://redirect', 1024, 10,
            int(time() + 86400), key, user_agent=False)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)

        def start_response(s, h, e=None):
            pass
        body = b''.join(self.formpost(env, start_response))
        self.assertIn('User-Agent', self.app.requests[0].headers)
        self.assertEqual(self.app.requests[0].headers['User-Agent'],
                         'FormPost')

    def test_formpost_with_origin(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://redirect', 1024, 10,
            int(time() + 86400), key, user_agent=False)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        env['HTTP_ORIGIN'] = 'http://localhost:5000'
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created',
                                  {'Access-Control-Allow-Origin':
                                   'http://localhost:5000'}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)

        headers = {}

        def start_response(s, h, e=None):
            for k, v in h:
                headers[k] = v
            pass

        body = b''.join(self.formpost(env, start_response))
        self.assertEqual(headers['Access-Control-Allow-Origin'],
                         'http://localhost:5000')

    def test_formpost_with_multiple_keys(self):
        key = b'ernie'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://redirect', 1024, 10,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        # Stick it in X-Account-Meta-Temp-URL-Key-2 and make sure we get it
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)

        status = [None]
        headers = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
        body = b''.join(self.formpost(env, start_response))
        self.assertEqual('303 See Other', status[0])
        self.assertEqual(
            'http://redirect?status=201&message=',
            dict(headers[0]).get('Location'))

    def test_formpost_with_multiple_container_keys(self):
        first_key = b'ernie'
        second_key = b'bert'
        keys = [first_key, second_key]

        meta = {}
        for idx, key in enumerate(keys):
            meta_name = 'temp-url-key' + ("-%d" % (idx + 1) if idx else "")
            if key:
                meta[meta_name] = key

        for key in keys:
            sig, env, body = self._make_sig_env_body(
                '/v1/AUTH_test/container', 'http://redirect', 1024, 10,
                int(time() + 86400), key)
            env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
            env['swift.infocache'][get_cache_key('AUTH_test')] = (
                self._fake_cache_env('AUTH_test'))
            # Stick it in X-Container-Meta-Temp-URL-Key-2 and ensure we get it
            env['swift.infocache'][get_cache_key(
                'AUTH_test', 'container')] = {'meta': meta}
            self.app = FakeApp(iter([('201 Created', {}, b''),
                                     ('201 Created', {}, b'')]))
            self.auth = tempauth.filter_factory({})(self.app)
            self.formpost = formpost.filter_factory({})(self.auth)

            status = [None]
            headers = [None]

            def start_response(s, h, e=None):
                status[0] = s
                headers[0] = h
            body = b''.join(self.formpost(env, start_response))
            self.assertEqual('303 See Other', status[0])
            self.assertEqual(
                'http://redirect?status=201&message=',
                dict(headers[0]).get('Location'))

    def test_redirect(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://redirect', 1024, 10,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location, 'http://redirect?status=201&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(location.encode('utf-8') in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_redirect_with_query(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', 'http://redirect?one=two', 1024, 10,
            int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEqual(location,
                         'http://redirect?one=two&status=201&message=')
        self.assertIsNone(exc_info)
        self.assertTrue(location.encode('utf-8') in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_no_redirect(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '201 Created')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'201 Created' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')

    def test_prefixed_and_not_prefixed_sigs_good(self):
        def do_test(digest, prefixed):
            key = b'abc'
            sig, env, body = self._make_sig_env_body(
                '/v1/AUTH_test/container', '', 1024, 10,
                int(time() + 86400), key, algorithm=digest, prefix=prefixed)
            env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
            env['swift.infocache'][get_cache_key('AUTH_test')] = (
                self._fake_cache_env('AUTH_test', [key]))
            env['swift.infocache'][get_cache_key(
                'AUTH_test', 'container')] = {'meta': {}}
            self.auth.app = app = FakeApp(iter([('201 Created', {}, b''),
                                                ('201 Created', {}, b'')]))
            status = [None]
            headers = [None]
            exc_info = [None]

            def start_response(s, h, e=None):
                status[0] = s
                headers[0] = h
                exc_info[0] = e

            body = b''.join(self.formpost(env, start_response))
            status = status[0]
            headers = headers[0]
            exc_info = exc_info[0]
            self.assertEqual(status, '201 Created')
            location = None
            for h, v in headers:
                if h.lower() == 'location':
                    location = v
            self.assertIsNone(location)
            self.assertIsNone(exc_info)
            self.assertTrue(b'201 Created' in body)
            self.assertEqual(len(app.requests), 2)
            self.assertEqual(app.requests[0].body, b'Test File\nOne\n')
            self.assertEqual(app.requests[1].body, b'Test\nFile\nTwo\n')

        for digest in ('sha1', 'sha256', 'sha512'):
            do_test(digest, True)
            do_test(digest, False)

        # NB: one increment per *upload*, not client request
        self.assertEqual(self.logger.statsd_client.get_increment_counts(), {
            'formpost.digests.sha1': 4,
            'formpost.digests.sha256': 4,
            'formpost.digests.sha512': 4,
        })

    def test_prefixed_and_not_prefixed_sigs_unsupported(self):
        def do_test(digest, prefixed):
            key = b'abc'
            sig, env, body = self._make_sig_env_body(
                '/v1/AUTH_test/container', '', 1024, 10,
                int(time() + 86400), key, algorithm=digest, prefix=prefixed)
            env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
            env['swift.infocache'][get_cache_key('AUTH_test')] = (
                self._fake_cache_env('AUTH_test', [key]))
            env['swift.infocache'][get_cache_key(
                'AUTH_test', 'container')] = {'meta': {}}
            self.app = FakeApp(iter([('201 Created', {}, b''),
                                     ('201 Created', {}, b'')]))
            self.auth = tempauth.filter_factory({})(self.app)
            self.formpost = formpost.filter_factory({})(self.auth)
            status = [None]

            def start_response(s, h, e=None):
                status[0] = s

            body = b''.join(self.formpost(env, start_response))
            status = status[0]
            self.assertEqual(status, '401 Unauthorized')

        for digest in ('md5', 'sha224'):
            do_test(digest, True)
            do_test(digest, False)

    def test_no_redirect_expired(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() - 10), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: Form Expired' in body)

    def test_no_redirect_invalid_sig(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        # Change key to invalidate sig
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key + b' is bogus now']))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: Invalid Signature' in body)

    def test_no_redirect_with_error(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'XX' + b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '400 Bad Request')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: invalid starting boundary' in body)

    def test_redirect_allowed_deprecated_and_unsupported_digests(self):
        logger = debug_logger()

        def do_test(digest):
            logger.clear()
            key = b'abc'
            sig, env, body = self._make_sig_env_body(
                '/v1/AUTH_test/container', 'http://redirect', 1024, 10,
                int(time() + 86400), key, algorithm=digest)
            env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
            env['swift.infocache'][get_cache_key('AUTH_test')] = (
                self._fake_cache_env('AUTH_test', [key]))
            env['swift.infocache'][get_cache_key(
                'AUTH_test', 'container')] = {'meta': {}}
            self.app = FakeApp(iter([('201 Created', {}, b''),
                                     ('201 Created', {}, b'')]))
            self.auth = tempauth.filter_factory({})(self.app)
            with mock.patch('swift.common.middleware.formpost.get_logger',
                            return_value=logger):
                self.formpost = formpost.filter_factory(
                    {
                        'allowed_digests': DEFAULT_ALLOWED_DIGESTS})(self.auth)
            status = [None]
            headers = [None]
            exc_info = [None]

            def start_response(s, h, e=None):
                status[0] = s
                headers[0] = h
                exc_info[0] = e

            body = b''.join(self.formpost(env, start_response))
            return body, status[0], headers[0], exc_info[0]

        for algorithm in ('sha1', 'sha256', 'sha512'):
            body, status, headers, exc_info = do_test(algorithm)
            self.assertEqual(status, '303 See Other')
            location = None
            for h, v in headers:
                if h.lower() == 'location':
                    location = v
            self.assertEqual(location, 'http://redirect?status=201&message=')
            self.assertIsNone(exc_info)
            self.assertTrue(location.encode('utf-8') in body)
            self.assertEqual(len(self.app.requests), 2)
            self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
            self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')
            if algorithm in digest_utils.DEPRECATED_DIGESTS:
                self.assertIn(
                    'The following digest algorithms are configured but '
                    'deprecated: %s. Support will be removed in a '
                    'future release.' % algorithm,
                    logger.get_lines_for_level('warning'))

        # unsupported
        _body, status, _headers, _exc_info = do_test("md5")
        self.assertEqual(status, '401 Unauthorized')

    def test_no_v1(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v2/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: Invalid Signature' in body)

    def test_empty_v1(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '//AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: Invalid Signature' in body)

    def test_empty_account(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1//container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: Invalid Signature' in body)

    def test_wrong_account(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_tst/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([
            ('200 Ok', {'x-account-meta-temp-url-key': 'def'}, b''),
            ('201 Created', {}, b''),
            ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: Invalid Signature' in body)

    def test_no_container(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: Invalid Signature' in body)

    def test_completely_non_int_expires(self):
        key = b'abc'
        expires = int(time() + 86400)
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, expires, key)
        for i, v in enumerate(body):
            if v.decode('utf-8') == str(expires):
                body[i] = b'badvalue'
                break
        env['wsgi.input'] = BytesIO(b'\r\n'.join(body))
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '400 Bad Request')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertIsNone(location)
        self.assertIsNone(exc_info)
        self.assertTrue(b'FormPost: expired not an integer' in body)

    def test_x_delete_at(self):
        delete_at = int(time() + 100)
        x_delete_body_part = [
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="x_delete_at"',
            '',
            str(delete_at),
        ]
        x_delete_body_part = [line.encode('utf-8')
                              for line in x_delete_body_part]
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        wsgi_input = b'\r\n'.join(x_delete_body_part + body)
        env['wsgi.input'] = BytesIO(wsgi_input)
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '201 Created')
        self.assertTrue(b'201 Created' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertIn("X-Delete-At", self.app.requests[0].headers)
        self.assertIn("X-Delete-At", self.app.requests[1].headers)
        self.assertEqual(delete_at,
                         self.app.requests[0].headers["X-Delete-At"])
        self.assertEqual(delete_at,
                         self.app.requests[1].headers["X-Delete-At"])

    def test_x_delete_at_not_int(self):
        delete_at = "2014-07-16"
        x_delete_body_part = [
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="x_delete_at"',
            '',
            str(delete_at),
        ]
        x_delete_body_part = [line.encode('utf-8')
                              for line in x_delete_body_part]
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        wsgi_input = b'\r\n'.join(x_delete_body_part + body)
        env['wsgi.input'] = BytesIO(wsgi_input)
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '400 Bad Request')
        self.assertTrue(b'FormPost: x_delete_at not an integer' in body)

    def test_x_delete_after(self):
        delete_after = 100
        x_delete_body_part = [
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="x_delete_after"',
            '',
            str(delete_after),
        ]
        x_delete_body_part = [line.encode('utf-8')
                              for line in x_delete_body_part]
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        wsgi_input = b'\r\n'.join(x_delete_body_part + body)
        env['wsgi.input'] = BytesIO(wsgi_input)
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '201 Created')
        self.assertTrue(b'201 Created' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertIn("X-Delete-After", self.app.requests[0].headers)
        self.assertIn("X-Delete-After", self.app.requests[1].headers)
        self.assertEqual(delete_after,
                         self.app.requests[0].headers["X-Delete-After"])
        self.assertEqual(delete_after,
                         self.app.requests[1].headers["X-Delete-After"])

    def test_x_delete_after_not_int(self):
        delete_after = "2 days"
        x_delete_body_part = [
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="x_delete_after"',
            '',
            str(delete_after),
        ]
        x_delete_body_part = [line.encode('utf-8')
                              for line in x_delete_body_part]
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        wsgi_input = b'\r\n'.join(x_delete_body_part + body)
        env['wsgi.input'] = BytesIO(wsgi_input)
        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '400 Bad Request')
        self.assertTrue(b'FormPost: x_delete_after not an integer' in body)

    def test_global_content_type_encoding(self):
        body_part = [
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="content-encoding"',
            '',
            'gzip',
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="content-type"',
            '',
            'text/html',
        ]
        body_part = [line.encode('utf-8') for line in body_part]

        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        wsgi_input = b'\r\n'.join(body_part + body)
        env['wsgi.input'] = BytesIO(wsgi_input)

        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '201 Created')
        self.assertTrue(b'201 Created' in body)
        self.assertEqual(len(self.app.requests), 2)
        self.assertIn("Content-Type", self.app.requests[0].headers)
        self.assertIn("Content-Type", self.app.requests[1].headers)
        self.assertIn("Content-Encoding", self.app.requests[0].headers)
        self.assertIn("Content-Encoding", self.app.requests[1].headers)
        self.assertEqual("text/html",
                         self.app.requests[0].headers["Content-Type"])
        self.assertEqual("text/html",
                         self.app.requests[1].headers["Content-Type"])
        self.assertEqual("gzip",
                         self.app.requests[0].headers["Content-Encoding"])
        self.assertEqual("gzip",
                         self.app.requests[1].headers["Content-Encoding"])

    def test_single_content_type_encoding(self):
        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        wsgi_input = b'\r\n'.join(body)
        env['wsgi.input'] = BytesIO(wsgi_input)

        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b''),
                                 ('201 Created', {}, b'')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEqual(status, '201 Created')
        self.assertTrue(b'201 Created' in body)

        self.assertEqual(len(self.app.requests), 2)
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')
        self.assertIn("Content-Type", self.app.requests[0].headers)
        self.assertIn("Content-Type", self.app.requests[1].headers)
        self.assertEqual("text/plain",
                         self.app.requests[0].headers["Content-Type"])
        self.assertEqual("text/plain",
                         self.app.requests[1].headers["Content-Type"])

        self.assertFalse("Content-Encoding" in self.app.requests[0].headers)
        self.assertIn("Content-Encoding", self.app.requests[1].headers)
        self.assertEqual("gzip",
                         self.app.requests[1].headers["Content-Encoding"])

    def test_multiple_content_type_encoding(self):
        body_part = [
            '------WebKitFormBoundaryNcxTqxSlX7t4TDkR',
            'Content-Disposition: form-data; name="file4"; '
            'filename="testfile4.txt"',
            'Content-Type: application/json',
            '',
            '{"four": 4}\n',
        ]
        body_part = [line.encode('utf-8') for line in body_part]

        key = b'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        # splice in another file with a different content type
        before_closing_boundary = len(body) - 2
        body[before_closing_boundary:before_closing_boundary] = body_part
        wsgi_input = b'\r\n'.join(body)
        env['wsgi.input'] = BytesIO(wsgi_input)

        env['swift.infocache'][get_cache_key('AUTH_test')] = (
            self._fake_cache_env('AUTH_test', [key]))
        env['swift.infocache'][get_cache_key(
            'AUTH_test', 'container')] = {'meta': {}}
        self.app = FakeApp(iter([('201 Created', {}, b'')] * 3))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h

        body = b''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        self.assertEqual(status, '201 Created')
        self.assertTrue(b'201 Created' in body)

        self.assertEqual(len(self.app.requests), 3)
        self.assertEqual(self.app.requests[0].body, b'Test File\nOne\n')
        self.assertEqual(self.app.requests[1].body, b'Test\nFile\nTwo\n')
        self.assertEqual(self.app.requests[2].body, b'{"four": 4}\n')

        self.assertIn("Content-Type", self.app.requests[0].headers)
        self.assertIn("Content-Type", self.app.requests[1].headers)
        self.assertIn("Content-Type", self.app.requests[2].headers)
        self.assertEqual("text/plain",
                         self.app.requests[0].headers["Content-Type"])
        self.assertEqual("text/plain",
                         self.app.requests[1].headers["Content-Type"])
        self.assertEqual("application/json",
                         self.app.requests[2].headers["Content-Type"])

        self.assertFalse("Content-Encoding" in self.app.requests[0].headers)
        self.assertIn("Content-Encoding", self.app.requests[1].headers)
        self.assertEqual("gzip",
                         self.app.requests[1].headers["Content-Encoding"])
        self.assertFalse("Content-Encoding" in self.app.requests[2].headers)


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        registry._swift_info = {}
        registry._swift_admin_info = {}

    def test_registered_defaults(self):
        formpost.filter_factory({})
        swift_info = registry.get_swift_info()
        self.assertIn('formpost', swift_info)
        info = swift_info['formpost']
        self.assertIn('allowed_digests', info)
        self.assertIn('deprecated_digests', info)
        self.assertEqual(info['allowed_digests'], ['sha1', 'sha256', 'sha512'])
        self.assertEqual(info['deprecated_digests'], ['sha1'])

    def test_non_default_methods(self):
        logger = debug_logger()
        with mock.patch('swift.common.middleware.formpost.get_logger',
                        return_value=logger):
            formpost.filter_factory({
                'allowed_digests': 'sha1 sha512 md5 not-a-valid-digest',
            })
        swift_info = registry.get_swift_info()
        self.assertIn('formpost', swift_info)
        info = swift_info['formpost']
        self.assertIn('allowed_digests', info)
        self.assertIn('deprecated_digests', info)
        self.assertEqual(info['allowed_digests'], ['sha1', 'sha512'])
        self.assertEqual(info['deprecated_digests'], ['sha1'])
        warning_lines = logger.get_lines_for_level('warning')
        self.assertIn(
            'The following digest algorithms are configured '
            'but not supported:',
            warning_lines[0])
        self.assertIn('not-a-valid-digest', warning_lines[0])
        self.assertIn('md5', warning_lines[0])

    def test_no_deprecated_digests(self):
        formpost.filter_factory({'allowed_digests': 'sha256 sha512'})
        swift_info = registry.get_swift_info()
        self.assertIn('formpost', swift_info)
        info = swift_info['formpost']
        self.assertIn('allowed_digests', info)
        self.assertNotIn('deprecated_digests', info)
        self.assertEqual(info['allowed_digests'], ['sha256', 'sha512'])

    def test_bad_config(self):
        with self.assertRaises(ValueError):
            formpost.filter_factory({
                'allowed_digests': 'md4',
            })


if __name__ == '__main__':
    unittest.main()
