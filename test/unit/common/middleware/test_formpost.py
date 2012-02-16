# Copyright (c) 2011 OpenStack, LLC.
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

import hmac
import unittest
from hashlib import sha1
from contextlib import contextmanager
from StringIO import StringIO
from time import time

from webob import Request, Response

from swift.common.middleware import tempauth, formpost


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, timeout=0):
        self.store[key] = value
        return True

    def incr(self, key, timeout=0):
        self.store[key] = self.store.setdefault(key, 0) + 1
        return self.store[key]

    @contextmanager
    def soft_lock(self, key, timeout=0, retries=5):
        yield True

    def delete(self, key):
        try:
            del self.store[key]
        except Exception:
            pass
        return True


class FakeApp(object):

    def __init__(self, status_headers_body_iter=None):
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter([('404 Not Found', {
                'x-test-header-one-a': 'value1',
                'x-test-header-two-a': 'value2',
                'x-test-header-two-b': 'value3'}, '')])
        self.requests = []

    def __call__(self, env, start_response):
        body = ''
        while True:
            chunk = env['wsgi.input'].read()
            if not chunk:
                break
            body += chunk
        env['wsgi.input'] = StringIO(body)
        self.requests.append(Request.blank('', environ=env))
        if env.get('swift.authorize_override') and \
                env.get('REMOTE_USER') != '.wsgi.formpost':
            raise Exception('Invalid REMOTE_USER %r with '
                'swift.authorize_override' % (env.get('REMOTE_USER'),))
        if 'swift.authorize' in env:
            resp = env['swift.authorize'](self.requests[-1])
            if resp:
                return resp(env, start_response)
        status, headers, body = self.status_headers_body_iter.next()
        return Response(status=status, headers=headers,
                        body=body)(env, start_response)


class TestParseAttrs(unittest.TestCase):

    def test_basic_content_type(self):
        name, attrs = formpost._parse_attrs('text/plain')
        self.assertEquals(name, 'text/plain')
        self.assertEquals(attrs, {})

    def test_content_type_with_charset(self):
        name, attrs = formpost._parse_attrs('text/plain; charset=UTF8')
        self.assertEquals(name, 'text/plain')
        self.assertEquals(attrs, {'charset': 'UTF8'})

    def test_content_disposition(self):
        name, attrs = formpost._parse_attrs(
            'form-data; name="somefile"; filename="test.html"')
        self.assertEquals(name, 'form-data')
        self.assertEquals(attrs, {'name': 'somefile', 'filename': 'test.html'})


class TestIterRequests(unittest.TestCase):

    def test_bad_start(self):
        it = formpost._iter_requests(StringIO('blah'), 'unique')
        exc = None
        try:
            it.next()
        except formpost.FormInvalid, err:
            exc = err
        self.assertEquals(str(exc), 'invalid starting boundary')

    def test_empty(self):
        it = formpost._iter_requests(StringIO('--unique'), 'unique')
        fp = it.next()
        self.assertEquals(fp.read(), '')
        exc = None
        try:
            it.next()
        except StopIteration, err:
            exc = err
        self.assertTrue(exc is not None)

    def test_basic(self):
        it = formpost._iter_requests(
            StringIO('--unique\r\nabcdefg\r\n--unique--'), 'unique')
        fp = it.next()
        self.assertEquals(fp.read(), 'abcdefg')
        exc = None
        try:
            it.next()
        except StopIteration, err:
            exc = err
        self.assertTrue(exc is not None)

    def test_basic2(self):
        it = formpost._iter_requests(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = it.next()
        self.assertEquals(fp.read(), 'abcdefg')
        fp = it.next()
        self.assertEquals(fp.read(), 'hijkl')
        exc = None
        try:
            it.next()
        except StopIteration, err:
            exc = err
        self.assertTrue(exc is not None)

    def test_tiny_reads(self):
        it = formpost._iter_requests(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = it.next()
        self.assertEquals(fp.read(2), 'ab')
        self.assertEquals(fp.read(2), 'cd')
        self.assertEquals(fp.read(2), 'ef')
        self.assertEquals(fp.read(2), 'g')
        self.assertEquals(fp.read(2), '')
        fp = it.next()
        self.assertEquals(fp.read(), 'hijkl')
        exc = None
        try:
            it.next()
        except StopIteration, err:
            exc = err
        self.assertTrue(exc is not None)

    def test_big_reads(self):
        it = formpost._iter_requests(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = it.next()
        self.assertEquals(fp.read(65536), 'abcdefg')
        self.assertEquals(fp.read(), '')
        fp = it.next()
        self.assertEquals(fp.read(), 'hijkl')
        exc = None
        try:
            it.next()
        except StopIteration, err:
            exc = err
        self.assertTrue(exc is not None)

    def test_broken_mid_stream(self):
        # We go ahead and accept whatever is sent instead of rejecting the
        # whole request, in case the partial form is still useful.
        it = formpost._iter_requests(
            StringIO('--unique\r\nabc'), 'unique')
        fp = it.next()
        self.assertEquals(fp.read(), 'abc')
        exc = None
        try:
            it.next()
        except StopIteration, err:
            exc = err
        self.assertTrue(exc is not None)

    def test_readline(self):
        it = formpost._iter_requests(StringIO('--unique\r\nab\r\ncd\ref\ng\r\n'
            '--unique\r\nhi\r\n\r\njkl\r\n\r\n--unique--'), 'unique')
        fp = it.next()
        self.assertEquals(fp.readline(), 'ab\r\n')
        self.assertEquals(fp.readline(), 'cd\ref\ng')
        self.assertEquals(fp.readline(), '')
        fp = it.next()
        self.assertEquals(fp.readline(), 'hi\r\n')
        self.assertEquals(fp.readline(), '\r\n')
        self.assertEquals(fp.readline(), 'jkl\r\n')
        exc = None
        try:
            it.next()
        except StopIteration, err:
            exc = err
        self.assertTrue(exc is not None)

    def test_readline_with_tiny_chunks(self):
        orig_read_chunk_size = formpost.READ_CHUNK_SIZE
        try:
            formpost.READ_CHUNK_SIZE = 2
            it = formpost._iter_requests(StringIO('--unique\r\nab\r\ncd\ref\ng'
                '\r\n--unique\r\nhi\r\n\r\njkl\r\n\r\n--unique--'), 'unique')
            fp = it.next()
            self.assertEquals(fp.readline(), 'ab\r\n')
            self.assertEquals(fp.readline(), 'cd\ref\ng')
            self.assertEquals(fp.readline(), '')
            fp = it.next()
            self.assertEquals(fp.readline(), 'hi\r\n')
            self.assertEquals(fp.readline(), '\r\n')
            self.assertEquals(fp.readline(), 'jkl\r\n')
            exc = None
            try:
                it.next()
            except StopIteration, err:
                exc = err
            self.assertTrue(exc is not None)
        finally:
            formpost.READ_CHUNK_SIZE = orig_read_chunk_size


class TestCappedFileLikeObject(unittest.TestCase):

    def test_whole(self):
        self.assertEquals(
            formpost._CappedFileLikeObject(StringIO('abc'), 10).read(), 'abc')

    def test_exceeded(self):
        exc = None
        try:
            formpost._CappedFileLikeObject(StringIO('abc'), 2).read()
        except EOFError, err:
            exc = err
        self.assertEquals(str(exc), 'max_file_size exceeded')

    def test_whole_readline(self):
        fp = formpost._CappedFileLikeObject(StringIO('abc\ndef'), 10)
        self.assertEquals(fp.readline(), 'abc\n')
        self.assertEquals(fp.readline(), 'def')
        self.assertEquals(fp.readline(), '')

    def test_exceeded_readline(self):
        fp = formpost._CappedFileLikeObject(StringIO('abc\ndef'), 5)
        self.assertEquals(fp.readline(), 'abc\n')
        exc = None
        try:
            self.assertEquals(fp.readline(), 'def')
        except EOFError, err:
            exc = err
        self.assertEquals(str(err), 'max_file_size exceeded')

    def test_read_sized(self):
        fp = formpost._CappedFileLikeObject(StringIO('abcdefg'), 10)
        self.assertEquals(fp.read(2), 'ab')
        self.assertEquals(fp.read(2), 'cd')
        self.assertEquals(fp.read(2), 'ef')
        self.assertEquals(fp.read(2), 'g')
        self.assertEquals(fp.read(2), '')


class TestFormPost(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)

    def _make_request(self, path, **kwargs):
        req = Request.blank(path, **kwargs)
        req.environ['swift.cache'] = FakeMemcache()
        return req

    def _make_sig_env_body(self, path, redirect, max_file_size, max_file_count,
                           expires, key):
        sig = hmac.new(key, '%s\n%s\n%s\n%s\n%s' % (path, redirect,
            max_file_size, max_file_count, expires), sha1).hexdigest()
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
            'wsgi.errors': wsgi_errors,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        return sig, env, body

    def test_passthrough(self):
        for method in ('HEAD', 'GET', 'PUT', 'POST', 'DELETE'):
            resp = self._make_request('/v1/a/c/o',
                environ={'REQUEST_METHOD': method}).get_response(self.formpost)
            self.assertEquals(resp.status_int, 401)
            self.assertTrue('FormPost' not in resp.body)

    def test_safari(self):
        key = 'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(key, '%s\n%s\n%s\n%s\n%s' % (path, redirect,
            max_file_size, max_file_count, expires), sha1).hexdigest()
        memcache = FakeMemcache()
        memcache.set('temp-url-key/AUTH_test', key)
        wsgi_input = StringIO('\r\n'.join([
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
        ]))
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
            'swift.cache': memcache,
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, 'http://brim.net?status=201&message=')
        self.assertEquals(exc_info, None)
        self.assertTrue('http://brim.net?status=201&message=' in body)
        self.assertEquals(len(self.app.requests), 2)
        self.assertEquals(self.app.requests[0].body, 'Test File\nOne\n')
        self.assertEquals(self.app.requests[1].body, 'Test\nFile\nTwo\n')

    def test_firefox(self):
        key = 'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(key, '%s\n%s\n%s\n%s\n%s' % (path, redirect,
            max_file_size, max_file_count, expires), sha1).hexdigest()
        memcache = FakeMemcache()
        memcache.set('temp-url-key/AUTH_test', key)
        wsgi_input = StringIO('\r\n'.join([
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
        ]))
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
            'swift.cache': memcache,
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, 'http://brim.net?status=201&message=')
        self.assertEquals(exc_info, None)
        self.assertTrue('http://brim.net?status=201&message=' in body)
        self.assertEquals(len(self.app.requests), 2)
        self.assertEquals(self.app.requests[0].body, 'Test File\nOne\n')
        self.assertEquals(self.app.requests[1].body, 'Test\nFile\nTwo\n')

    def test_chrome(self):
        key = 'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(key, '%s\n%s\n%s\n%s\n%s' % (path, redirect,
            max_file_size, max_file_count, expires), sha1).hexdigest()
        memcache = FakeMemcache()
        memcache.set('temp-url-key/AUTH_test', key)
        wsgi_input = StringIO('\r\n'.join([
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
        ]))
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
            'swift.cache': memcache,
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, 'http://brim.net?status=201&message=')
        self.assertEquals(exc_info, None)
        self.assertTrue('http://brim.net?status=201&message=' in body)
        self.assertEquals(len(self.app.requests), 2)
        self.assertEquals(self.app.requests[0].body, 'Test File\nOne\n')
        self.assertEquals(self.app.requests[1].body, 'Test\nFile\nTwo\n')

    def test_explorer(self):
        key = 'abc'
        path = '/v1/AUTH_test/container'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig = hmac.new(key, '%s\n%s\n%s\n%s\n%s' % (path, redirect,
            max_file_size, max_file_count, expires), sha1).hexdigest()
        memcache = FakeMemcache()
        memcache.set('temp-url-key/AUTH_test', key)
        wsgi_input = StringIO('\r\n'.join([
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
        ]))
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
            'swift.cache': memcache,
            'wsgi.errors': wsgi_errors,
            'wsgi.input': wsgi_input,
            'wsgi.multiprocess': False,
            'wsgi.multithread': True,
            'wsgi.run_once': False,
            'wsgi.url_scheme': 'http',
            'wsgi.version': (1, 0),
        }
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, 'http://brim.net?status=201&message=')
        self.assertEquals(exc_info, None)
        self.assertTrue('http://brim.net?status=201&message=' in body)
        self.assertEquals(len(self.app.requests), 2)
        self.assertEquals(self.app.requests[0].body, 'Test File\nOne\n')
        self.assertEquals(self.app.requests[1].body, 'Test\nFile\nTwo\n')

    def test_messed_up_start(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body('/v1/AUTH_test/container',
            'http://brim.net', 5, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('XX' + '\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '400 Bad Request')
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: invalid starting boundary' in body)
        self.assertEquals(len(self.app.requests), 0)

    def test_max_file_size_exceeded(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body('/v1/AUTH_test/container',
            'http://brim.net', 5, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '400 Bad Request')
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: max_file_size exceeded' in body)
        self.assertEquals(len(self.app.requests), 0)

    def test_max_file_count_exceeded(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body('/v1/AUTH_test/container',
            'http://brim.net', 1024, 1, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location,
            'http://brim.net?status=400&message=max%20file%20count%20exceeded')
        self.assertEquals(exc_info, None)
        self.assertTrue(
            'http://brim.net?status=400&message=max%20file%20count%20exceeded'
            in body)
        self.assertEquals(len(self.app.requests), 1)
        self.assertEquals(self.app.requests[0].body, 'Test File\nOne\n')

    def test_subrequest_fails(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body('/v1/AUTH_test/container',
            'http://brim.net', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('404 Not Found', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, 'http://brim.net?status=404&message=')
        self.assertEquals(exc_info, None)
        self.assertTrue('http://brim.net?status=404&message=' in body)
        self.assertEquals(len(self.app.requests), 1)

    def test_truncated_attr_value(self):
        key = 'abc'
        redirect = 'a' * formpost.MAX_VALUE_LENGTH
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig, env, body = self._make_sig_env_body('/v1/AUTH_test/container',
            redirect, max_file_size, max_file_count, expires, key)
        # Tack on an extra char to redirect, but shouldn't matter since it
        # should get truncated off on read.
        redirect += 'b'
        env['wsgi.input'] = StringIO('\r\n'.join([
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
        ]))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location,
            ('a' * formpost.MAX_VALUE_LENGTH) + '?status=201&message=')
        self.assertEquals(exc_info, None)
        self.assertTrue(
            ('a' * formpost.MAX_VALUE_LENGTH) + '?status=201&message=' in body)
        self.assertEquals(len(self.app.requests), 2)
        self.assertEquals(self.app.requests[0].body, 'Test File\nOne\n')
        self.assertEquals(self.app.requests[1].body, 'Test\nFile\nTwo\n')

    def test_no_file_to_process(self):
        key = 'abc'
        redirect = 'http://brim.net'
        max_file_size = 1024
        max_file_count = 10
        expires = int(time() + 86400)
        sig, env, body = self._make_sig_env_body('/v1/AUTH_test/container',
            redirect, max_file_size, max_file_count, expires, key)
        env['wsgi.input'] = StringIO('\r\n'.join([
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
        ]))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '303 See Other')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location,
            'http://brim.net?status=400&message=no%20files%20to%20process')
        self.assertEquals(exc_info, None)
        self.assertTrue(
            'http://brim.net?status=400&message=no%20files%20to%20process'
            in body)
        self.assertEquals(len(self.app.requests), 0)

    def test_no_redirect(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '201 Created')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('201 Created' in body)
        self.assertEquals(len(self.app.requests), 2)
        self.assertEquals(self.app.requests[0].body, 'Test File\nOne\n')
        self.assertEquals(self.app.requests[1].body, 'Test\nFile\nTwo\n')

    def test_no_redirect_expired(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() - 10), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: Form Expired' in body)

    def test_no_redirect_invalid_sig(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        # Change key to invalidate sig
        key = 'def'
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: Invalid Signature' in body)

    def test_no_redirect_with_error(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('XX' + '\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '400 Bad Request')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: invalid starting boundary' in body)

    def test_no_v1(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v2/AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: Invalid Signature' in body)

    def test_empty_v1(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '//AUTH_test/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: Invalid Signature' in body)

    def test_empty_account(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1//container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: Invalid Signature' in body)

    def test_wrong_account(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_tst/container', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([
            ('200 Ok', {'x-account-meta-temp-url-key': 'def'}, ''),
            ('201 Created', {}, ''),
            ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: Invalid Signature' in body)

    def test_no_container(self):
        key = 'abc'
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test', '', 1024, 10, int(time() + 86400), key)
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '401 Unauthorized')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: Invalid Signature' in body)

    def test_completely_non_int_expires(self):
        key = 'abc'
        expires = int(time() + 86400)
        sig, env, body = self._make_sig_env_body(
            '/v1/AUTH_test/container', '', 1024, 10, expires, key)
        for i, v in enumerate(body):
            if v == str(expires):
                body[i] = 'badvalue'
                break
        env['wsgi.input'] = StringIO('\r\n'.join(body))
        env['swift.cache'] = FakeMemcache()
        env['swift.cache'].set('temp-url-key/AUTH_test', key)
        self.app = FakeApp(iter([('201 Created', {}, ''),
                                 ('201 Created', {}, '')]))
        self.auth = tempauth.filter_factory({})(self.app)
        self.formpost = formpost.filter_factory({})(self.auth)
        status = [None]
        headers = [None]
        exc_info = [None]

        def start_response(s, h, e=None):
            status[0] = s
            headers[0] = h
            exc_info[0] = e

        body = ''.join(self.formpost(env, start_response))
        status = status[0]
        headers = headers[0]
        exc_info = exc_info[0]
        self.assertEquals(status, '400 Bad Request')
        location = None
        for h, v in headers:
            if h.lower() == 'location':
                location = v
        self.assertEquals(location, None)
        self.assertEquals(exc_info, None)
        self.assertTrue('FormPost: expired not an integer' in body)


if __name__ == '__main__':
    unittest.main()
