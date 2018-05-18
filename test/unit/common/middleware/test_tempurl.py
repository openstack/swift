# Copyright (c) 2011-2014 Greg Holt
# Copyright (c) 2012-2013 Peter Portante
# Copyright (c) 2012 Iryoung Jeong
# Copyright (c) 2012 Michael Barton
# Copyright (c) 2013 Alex Gaynor
# Copyright (c) 2013 Chuck Thier
# Copyright (c) 2013 David Goetz
# Copyright (c) 2015 Donagh McCabe
# Copyright (c) 2013 Greg Lange
# Copyright (c) 2013 John Dickinson
# Copyright (c) 2013 Kun Huang
# Copyright (c) 2013 Richard Hawkins
# Copyright (c) 2013 Samuel Merritt
# Copyright (c) 2013 Shri Javadekar
# Copyright (c) 2013 Tong Li
# Copyright (c) 2013 ZhiQiang Fan
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
import itertools
import mock
import unittest
import hashlib
from time import time, strftime, gmtime

from swift.common.middleware import tempauth, tempurl
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import Request, Response
from swift.common import utils


class FakeApp(object):

    def __init__(self, status_headers_body_iter=None):
        self.calls = 0
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter(
                itertools.repeat((
                    '404 Not Found', {
                        'x-test-header-one-a': 'value1',
                        'x-test-header-two-a': 'value2',
                        'x-test-header-two-b': 'value3'},
                    '')))
        self.request = None

    def __call__(self, env, start_response):
        self.calls += 1
        self.request = Request.blank('', environ=env)
        if 'swift.authorize' in env:
            resp = env['swift.authorize'](self.request)
            if resp:
                return resp(env, start_response)
        status, headers, body = next(self.status_headers_body_iter)
        return Response(status=status, headers=headers,
                        body=body)(env, start_response)


class TestTempURL(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.auth = tempauth.filter_factory({'reseller_prefix': ''})(self.app)
        self.tempurl = tempurl.filter_factory({})(self.auth)

    def _make_request(self, path, environ=None, keys=(), container_keys=None,
                      **kwargs):
        if environ is None:
            environ = {}

        _junk, account, _junk, _junk = utils.split_path(path, 2, 4, True)
        self._fake_cache_environ(environ, account, keys,
                                 container_keys=container_keys)
        req = Request.blank(path, environ=environ, **kwargs)
        return req

    def _fake_cache_environ(self, environ, account, keys, container_keys=None):
        """
        Fake out the caching layer for get_account_info(). Injects account data
        into environ such that keys are the tempurl keys, if set.
        """
        meta = {'swash': 'buckle'}
        for idx, key in enumerate(keys):
            meta_name = 'Temp-URL-key' + (("-%d" % (idx + 1) if idx else ""))
            if key:
                meta[meta_name] = key

        ic = environ.setdefault('swift.infocache', {})
        ic['account/' + account] = {
            'status': 204,
            'container_count': '0',
            'total_object_count': '0',
            'bytes': '0',
            'meta': meta}

        meta = {}
        for i, key in enumerate(container_keys or []):
            meta_name = 'Temp-URL-key' + (("-%d" % (i + 1) if i else ""))
            meta[meta_name] = key

        container_cache_key = 'container/' + account + '/c'
        ic.setdefault(container_cache_key, {'meta': meta})

    def test_passthrough(self):
        resp = self._make_request('/v1/a/c/o').get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertNotIn('Temp URL invalid', resp.body)

    def test_allow_options(self):
        self.app.status_headers_body_iter = iter([('200 Ok', {}, '')])
        resp = self._make_request(
            '/v1/a/c/o?temp_url_sig=abcde&temp_url_expires=12345',
            environ={'REQUEST_METHOD': 'OPTIONS'}).get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)

    def assert_valid_sig(self, expires, path, keys, sig, environ=None,
                         prefix=None):
        if not environ:
            environ = {}
        environ['QUERY_STRING'] = 'temp_url_sig=%s&temp_url_expires=%s' % (
            sig.replace('+', '%2B'), expires)
        if prefix is not None:
            environ['QUERY_STRING'] += '&temp_url_prefix=%s' % prefix
        req = self._make_request(path, keys=keys, environ=environ)
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'],
                         'attachment; filename="o"; ' + "filename*=UTF-8''o")
        self.assertEqual(resp.headers['expires'],
                         strftime('%a, %d %b %Y %H:%M:%S GMT',
                                  gmtime(expires)))
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        self.assert_valid_sig(expires, path, [key], sig)

        sig = hmac.new(key, hmac_body, hashlib.sha256).hexdigest()
        self.assert_valid_sig(expires, path, [key], sig)

        sig = base64.b64encode(hmac.new(
            key, hmac_body, hashlib.sha256).digest())
        self.assert_valid_sig(expires, path, [key], 'sha256:' + sig)

        sig = base64.b64encode(hmac.new(
            key, hmac_body, hashlib.sha512).digest())
        self.assert_valid_sig(expires, path, [key], 'sha512:' + sig)

    def test_get_valid_key2(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key1 = 'abc123'
        key2 = 'def456'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig1 = hmac.new(key1, hmac_body, hashlib.sha1).hexdigest()
        sig2 = hmac.new(key2, hmac_body, hashlib.sha1).hexdigest()
        for sig in (sig1, sig2):
            self.assert_valid_sig(expires, path, [key1, key2], sig)

    def test_get_valid_container_keys(self):
        ic = {}
        environ = {'swift.infocache': ic}
        # Add two static container keys
        container_keys = ['me', 'other']
        meta = {}
        for idx, key in enumerate(container_keys):
            meta_name = 'Temp-URL-key' + (("-%d" % (idx + 1) if idx else ""))
            if key:
                meta[meta_name] = key
        ic['container/a/c'] = {'meta': meta}

        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key1 = 'me'
        key2 = 'other'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig1 = hmac.new(key1, hmac_body, hashlib.sha1).hexdigest()
        sig2 = hmac.new(key2, hmac_body, hashlib.sha1).hexdigest()
        account_keys = []
        for sig in (sig1, sig2):
            self.assert_valid_sig(expires, path, account_keys, sig, environ)

    @mock.patch('swift.common.middleware.tempurl.time', return_value=0)
    def test_get_valid_with_filename(self, mock_time):
        method = 'GET'
        expires = (((24 + 1) * 60 + 1) * 60) + 1
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'filename=bob%%20%%22killer%%22.txt' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'],
                         'attachment; filename="bob %22killer%22.txt"; ' +
                         "filename*=UTF-8''bob%20%22killer%22.txt")
        self.assertIn('expires', resp.headers)
        self.assertEqual('Fri, 02 Jan 1970 01:01:01 GMT',
                         resp.headers['expires'])
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_valid_with_filename(self):
        method = 'HEAD'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'REQUEST_METHOD': 'HEAD',
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'filename=bob_killer.txt' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'],
                         'attachment; filename="bob_killer.txt"; ' +
                         "filename*=UTF-8''bob_killer.txt")

    def test_head_and_get_headers_match(self):
        method = 'HEAD'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'REQUEST_METHOD': 'HEAD',
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s'
            % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)

        get_method = 'GET'
        get_hmac_body = '%s\n%s\n%s' % (get_method, expires, path)
        get_sig = hmac.new(key, get_hmac_body, hashlib.sha1).hexdigest()
        get_req = self._make_request(path, keys=[key], environ={
            'REQUEST_METHOD': 'GET',
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s'
            % (get_sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        get_resp = get_req.get_response(self.tempurl)
        self.assertEqual(resp.headers, get_resp.headers)

    @mock.patch('swift.common.middleware.tempurl.time', return_value=0)
    def test_get_valid_with_filename_and_inline(self, mock_time):
        method = 'GET'
        expires = 1
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'filename=bob%%20%%22killer%%22.txt&inline=' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'],
                         'inline; filename="bob %22killer%22.txt"; ' +
                         "filename*=UTF-8''bob%20%22killer%22.txt")
        self.assertIn('expires', resp.headers)
        self.assertEqual('Thu, 01 Jan 1970 00:00:01 GMT',
                         resp.headers['expires'])
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid_with_inline(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'inline=' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'], 'inline')
        self.assertIn('expires', resp.headers)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid_with_prefix(self):
        method = 'GET'
        expires = int(time() + 86400)
        prefix = 'p1/p2/'
        sig_path = 'prefix:/v1/a/c/' + prefix
        query_path = '/v1/a/c/' + prefix + 'o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, sig_path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        self.assert_valid_sig(expires, query_path, [key], sig, prefix=prefix)

        query_path = query_path[:-1] + 'p3/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, sig_path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        self.assert_valid_sig(expires, query_path, [key], sig, prefix=prefix)

    def test_get_valid_with_prefix_empty(self):
        method = 'GET'
        expires = int(time() + 86400)
        sig_path = 'prefix:/v1/a/c/'
        query_path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, sig_path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        self.assert_valid_sig(expires, query_path, [key], sig, prefix='')

    def test_obj_odd_chars(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/a\r\nb'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'],
                         'attachment; filename="a%0D%0Ab"; ' +
                         "filename*=UTF-8''a%0D%0Ab")
        self.assertIn('expires', resp.headers)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_obj_odd_chars_in_content_disposition_metadata(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        headers = [('Content-Disposition', 'attachment; filename="fu\nbar"')]
        self.tempurl.app = FakeApp(iter([('200 Ok', headers, '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'],
                         'attachment; filename="fu%0Abar"')
        self.assertIn('expires', resp.headers)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_obj_trailing_slash(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o/'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['content-disposition'],
                         'attachment; filename="o"; ' +
                         "filename*=UTF-8''o")
        self.assertIn('expires', resp.headers)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_filename_trailing_slash(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'filename=/i/want/this/just/as/it/is/' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(
            resp.headers['content-disposition'],
            'attachment; filename="/i/want/this/just/as/it/is/"; ' +
            "filename*=UTF-8''/i/want/this/just/as/it/is/")
        self.assertIn('expires', resp.headers)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid_but_404(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('content-disposition', resp.headers)
        self.assertNotIn('expires', resp.headers)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_put_not_allowed_by_get(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'PUT',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_put_valid(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'PUT',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_not_allowed_by_put(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_missing_sig(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_expires=%s' % expires})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_missing_expires(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s' % sig})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_bad_path(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_no_key(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_head_allowed_by_get(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_allowed_by_put(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_allowed_by_post(self):
        method = 'POST'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_otherwise_not_allowed(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        # Deliberately fudge expires to show HEADs aren't just automatically
        # allowed.
        expires += 1
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_post_when_forbidden_by_config(self):
        self.tempurl.conf['methods'].remove('POST')
        method = 'POST'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'POST',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_delete_when_forbidden_by_config(self):
        self.tempurl.conf['methods'].remove('DELETE')
        method = 'DELETE'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'DELETE',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_delete_allowed(self):
        method = 'DELETE'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'DELETE',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)

    def test_unknown_not_allowed(self):
        method = 'UNKNOWN'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'UNKNOWN',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_authorize_limits_scope(self):
        req_other_object = Request.blank("/v1/a/c/o2")
        req_other_container = Request.blank("/v1/a/c2/o2")
        req_other_account = Request.blank("/v1/a2/c2/o2")

        key_kwargs = {
            'keys': ['account-key', 'shared-key'],
            'container_keys': ['container-key', 'shared-key'],
        }

        # A request with the account key limits the pre-authed scope to the
        # account level.
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'

        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new('account-key', hmac_body, hashlib.sha1).hexdigest()
        qs = '?temp_url_sig=%s&temp_url_expires=%s' % (sig, expires)

        # make request will setup the environ cache for us
        req = self._make_request(path + qs, **key_kwargs)
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)  # sanity check

        authorize = req.environ['swift.authorize']
        # Requests for other objects happen if, for example, you're
        # downloading a large object or creating a large-object manifest.
        oo_resp = authorize(req_other_object)
        self.assertIsNone(oo_resp)
        oc_resp = authorize(req_other_container)
        self.assertIsNone(oc_resp)
        oa_resp = authorize(req_other_account)
        self.assertEqual(oa_resp.status_int, 401)

        # A request with the container key limits the pre-authed scope to
        # the container level; a different container in the same account is
        # out of scope and thus forbidden.
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new('container-key', hmac_body, hashlib.sha1).hexdigest()
        qs = '?temp_url_sig=%s&temp_url_expires=%s' % (sig, expires)

        req = self._make_request(path + qs, **key_kwargs)
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)  # sanity check

        authorize = req.environ['swift.authorize']
        oo_resp = authorize(req_other_object)
        self.assertIsNone(oo_resp)
        oc_resp = authorize(req_other_container)
        self.assertEqual(oc_resp.status_int, 401)
        oa_resp = authorize(req_other_account)
        self.assertEqual(oa_resp.status_int, 401)

        # If account and container share a key (users set these, so this can
        # happen by accident, stupidity, *or* malice!), limit the scope to
        # account level. This prevents someone from shrinking the scope of
        # account-level tempurls by reusing one of the account's keys on a
        # container.
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new('shared-key', hmac_body, hashlib.sha1).hexdigest()
        qs = '?temp_url_sig=%s&temp_url_expires=%s' % (sig, expires)

        req = self._make_request(path + qs, **key_kwargs)
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)  # sanity check

        authorize = req.environ['swift.authorize']
        oo_resp = authorize(req_other_object)
        self.assertIsNone(oo_resp)
        oc_resp = authorize(req_other_container)
        self.assertIsNone(oc_resp)
        oa_resp = authorize(req_other_account)
        self.assertEqual(oa_resp.status_int, 401)

    def test_changed_path_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path + '2', keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_changed_sig_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        if sig[-1] != '0':
            sig = sig[:-1] + '0'
        else:
            sig = sig[:-1] + '1'
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_changed_expires_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires + 1)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_ip_range_value_error(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        ip = '127.0.0.1'
        not_an_ip = 'abcd'
        hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={
                'QUERY_STRING':
                'temp_url_sig=%s&temp_url_expires=%s&temp_url_ip_range=%s'
                % (sig, expires, not_an_ip),
                'REMOTE_ADDR': '127.0.0.1'
            },
        )
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_bad_ip_range_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        ip = '127.0.0.1'
        bad_ip = '127.0.0.2'
        hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={
                'QUERY_STRING':
                'temp_url_sig=%s&temp_url_expires=%s&temp_url_ip_range=%s'
                % (sig, expires, ip),
                'REMOTE_ADDR': bad_ip
            },
        )
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_different_key_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key + '2'],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_no_prefix_match_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        sig_path = 'prefix:/v1/a/c/p1/p2/'
        query_path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, sig_path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            query_path, keys=[key],
            environ={'QUERY_STRING':
                     'temp_url_sig=%s&temp_url_expires=%s&temp_url_prefix=%s' %
                     (sig, expires, 'p1/p2/')})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_object_url_with_prefix_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING':
                     'temp_url_sig=%s&temp_url_expires=%s&temp_url_prefix=o' %
                     (sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_disallowed_header_object_manifest(self):
        self.tempurl = tempurl.filter_factory({})(self.auth)
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        for method in ('PUT', 'POST'):
            for hdr, value in [('X-Object-Manifest', 'private/secret'),
                               ('X-Symlink-Target', 'cont/symlink')]:
                hmac_body = '%s\n%s\n%s' % (method, expires, path)
                sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
                req = self._make_request(
                    path, method=method, keys=[key],
                    headers={hdr: value},
                    environ={'QUERY_STRING':
                             'temp_url_sig=%s&temp_url_expires=%s'
                             % (sig, expires)})
                resp = req.get_response(self.tempurl)
                self.assertEqual(resp.status_int, 400)
                self.assertTrue('header' in resp.body)
                self.assertTrue('not allowed' in resp.body)
                self.assertTrue(hdr in resp.body)

    def test_removed_incoming_header(self):
        self.tempurl = tempurl.filter_factory({
            'incoming_remove_headers': 'x-remove-this'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-remove-this': 'value'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('x-remove-this', self.app.request.headers)

    def test_removed_incoming_headers_match(self):
        self.tempurl = tempurl.filter_factory({
            'incoming_remove_headers': 'x-remove-this-*',
            'incoming_allow_headers': 'x-remove-this-except-this'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-remove-this-one': 'value1',
                     'x-remove-this-except-this': 'value2'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('x-remove-this-one', self.app.request.headers)
        self.assertEqual(
            self.app.request.headers['x-remove-this-except-this'], 'value2')

    def test_allow_trumps_incoming_header_conflict(self):
        self.tempurl = tempurl.filter_factory({
            'incoming_remove_headers': 'x-conflict-header',
            'incoming_allow_headers': 'x-conflict-header'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-conflict-header': 'value'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('x-conflict-header' in self.app.request.headers)

    def test_allow_trumps_incoming_header_startswith_conflict(self):
        self.tempurl = tempurl.filter_factory({
            'incoming_remove_headers': 'x-conflict-header-*',
            'incoming_allow_headers': 'x-conflict-header-*'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-conflict-header-test': 'value'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertTrue('x-conflict-header-test' in self.app.request.headers)

    def test_removed_outgoing_header(self):
        self.tempurl = tempurl.filter_factory({
            'outgoing_remove_headers': 'x-test-header-one-a'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('x-test-header-one-a', resp.headers)
        self.assertEqual(resp.headers['x-test-header-two-a'], 'value2')

    def test_removed_outgoing_headers_match(self):
        self.tempurl = tempurl.filter_factory({
            'outgoing_remove_headers': 'x-test-header-two-*',
            'outgoing_allow_headers': 'x-test-header-two-b'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 404)
        self.assertEqual(resp.headers['x-test-header-one-a'], 'value1')
        self.assertNotIn('x-test-header-two-a', resp.headers)
        self.assertEqual(resp.headers['x-test-header-two-b'], 'value3')

    def test_allow_trumps_outgoing_header_conflict(self):
        self.tempurl = tempurl.filter_factory({
            'outgoing_remove_headers': 'x-conflict-header',
            'outgoing_allow_headers': 'x-conflict-header'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', {
            'X-Conflict-Header': 'value'}, '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('x-conflict-header' in resp.headers)
        self.assertEqual(resp.headers['x-conflict-header'], 'value')

    def test_allow_trumps_outgoing_header_startswith_conflict(self):
        self.tempurl = tempurl.filter_factory({
            'outgoing_remove_headers': 'x-conflict-header-*',
            'outgoing_allow_headers': 'x-conflict-header-*'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', {
            'X-Conflict-Header-Test': 'value'}, '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue('x-conflict-header-test' in resp.headers)
        self.assertEqual(resp.headers['x-conflict-header-test'], 'value')

    def test_get_path_parts(self):
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'HEAD', 'PATH_INFO': '/v1/a/c/o'}),
            ('a', 'c', 'o'))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c/o'}),
            ('a', 'c', 'o'))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'PUT', 'PATH_INFO': '/v1/a/c/o'}),
            ('a', 'c', 'o'))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'POST', 'PATH_INFO': '/v1/a/c/o'}),
            ('a', 'c', 'o'))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'DELETE', 'PATH_INFO': '/v1/a/c/o'}),
            ('a', 'c', 'o'))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'UNKNOWN', 'PATH_INFO': '/v1/a/c/o'}),
            (None, None, None))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c/'}),
            (None, None, None))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c//////'}),
            (None, None, None))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c///o///'}),
            ('a', 'c', '//o///'))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c'}),
            (None, None, None))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a//o'}),
            (None, None, None))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1//c/o'}),
            (None, None, None))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '//a/c/o'}),
            (None, None, None))
        self.assertEqual(self.tempurl._get_path_parts({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v2/a/c/o'}),
            (None, None, None))

    def test_get_temp_url_info(self):
        s = 'f5d5051bddf5df7e27c628818738334f'
        e_ts = int(time() + 86400)
        e_8601 = strftime(tempurl.EXPIRES_ISO8601_FORMAT, gmtime(e_ts))
        for e in (e_ts, e_8601):
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                        s, e)}),
                (s, e_ts, None, None, None, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING':
                     'temp_url_sig=%s&temp_url_expires=%s&temp_url_prefix=%s'
                     % (s, e, 'prefix')}),
                (s, e_ts, 'prefix', None, None, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
                     'filename=bobisyouruncle' % (s, e)}),
                (s, e_ts, None, 'bobisyouruncle', None, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info({}),
                (None, None, None, None, None, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_expires=%s' % e}),
                (None, e_ts, None, None, None, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s' % s}),
                (s, None, None, None, None, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=bad' % (
                        s)}),
                (s, 0, None, None, None, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
                     'inline=' % (s, e)}),
                (s, e_ts, None, None, True, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
                     'filename=bobisyouruncle&inline=' % (s, e)}),
                (s, e_ts, None, 'bobisyouruncle', True, None))
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
                     'filename=bobisyouruncle&inline='
                     '&temp_url_ip_range=127.0.0.1' % (s, e)}),
                (s, e_ts, None, 'bobisyouruncle', True, '127.0.0.1'))

        e_ts = int(time() - 1)
        e_8601 = strftime(tempurl.EXPIRES_ISO8601_FORMAT, gmtime(e_ts))
        for e in (e_ts, e_8601):
            self.assertEqual(
                self.tempurl._get_temp_url_info(
                    {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                        s, e)}),
                (s, 0, None, None, None, None))
        # Offsets not supported (yet?).
        e_8601 = strftime('%Y-%m-%dT%H:%M:%S+0000', gmtime(e_ts))
        self.assertEqual(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                    s, e_8601)}),
            (s, 0, None, None, None, None))

    def test_get_hmacs(self):
        self.assertEqual(
            self.tempurl._get_hmacs(
                {'REQUEST_METHOD': 'GET'}, 1, '/v1/a/c/o',
                [('abc', 'account')], 'sha1'),
            [('026d7f7cc25256450423c7ad03fc9f5ffc1dab6d', 'account')])
        self.assertEqual(
            self.tempurl._get_hmacs(
                {'REQUEST_METHOD': 'HEAD'}, 1, '/v1/a/c/o',
                [('abc', 'account')], 'sha512', request_method='GET'),
            [('240866478d94bbe683ab1d25fba52c7d0df21a60951'
              '4fe6a493dc30f951d2748abc51da0cbc633cd1e0acf'
              '6fadd3af3aedff00ee3d3434dc6a4c423e74adfc4a', 'account')])
        self.assertEqual(
            self.tempurl._get_hmacs(
                {'REQUEST_METHOD': 'HEAD'}, 1, '/v1/a/c/o',
                [('abc', 'account')], 'sha512', request_method='GET',
                ip_range='127.0.0.1'
            ),
            [('b713f99a66911cdf41dbcdff16db3efbd1ca89340a20'
              '86cc2ed88f0d3a74c7159e7687a312b12345d3721b7b'
              '94e36c2753d7cc01e9a91cc318c5081d788f2cfe', 'account')])

    def test_invalid(self):

        def _start_response(status, headers, exc_info=None):
            self.assertTrue(status, '401 Unauthorized')

        self.assertTrue('Temp URL invalid' in ''.join(
            self.tempurl._invalid({'REQUEST_METHOD': 'GET'},
                                  _start_response)))
        self.assertEqual('', ''.join(
            self.tempurl._invalid({'REQUEST_METHOD': 'HEAD'},
                                  _start_response)))

    def test_auth_scheme_value(self):
        # Passthrough
        environ = {}
        resp = self._make_request('/v1/a/c/o', environ=environ).get_response(
            self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertNotIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)
        self.assertNotIn('swift.auth_scheme', environ)

        # Rejected by TempURL
        environ = {'REQUEST_METHOD': 'PUT',
                   'QUERY_STRING':
                   'temp_url_sig=dummy&temp_url_expires=1234'}
        req = self._make_request('/v1/a/c/o', keys=['abc'],
                                 environ=environ)
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    def test_clean_incoming_headers(self):
        irh = []
        iah = []
        env = {'HTTP_TEST_HEADER': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertIn('HTTP_TEST_HEADER', env)

        irh = ['test-header']
        iah = []
        env = {'HTTP_TEST_HEADER': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertNotIn('HTTP_TEST_HEADER', env)

        irh = ['test-header-*']
        iah = []
        env = {'HTTP_TEST_HEADER_ONE': 'value',
               'HTTP_TEST_HEADER_TWO': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertNotIn('HTTP_TEST_HEADER_ONE', env)
        self.assertNotIn('HTTP_TEST_HEADER_TWO', env)

        irh = ['test-header-*']
        iah = ['test-header-two']
        env = {'HTTP_TEST_HEADER_ONE': 'value',
               'HTTP_TEST_HEADER_TWO': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertNotIn('HTTP_TEST_HEADER_ONE', env)
        self.assertIn('HTTP_TEST_HEADER_TWO', env)

        irh = ['test-header-*', 'test-other-header']
        iah = ['test-header-two', 'test-header-yes-*']
        env = {'HTTP_TEST_HEADER_ONE': 'value',
               'HTTP_TEST_HEADER_TWO': 'value',
               'HTTP_TEST_OTHER_HEADER': 'value',
               'HTTP_TEST_HEADER_YES': 'value',
               'HTTP_TEST_HEADER_YES_THIS': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertNotIn('HTTP_TEST_HEADER_ONE', env)
        self.assertIn('HTTP_TEST_HEADER_TWO', env)
        self.assertNotIn('HTTP_TEST_OTHER_HEADER', env)
        self.assertNotIn('HTTP_TEST_HEADER_YES', env)
        self.assertIn('HTTP_TEST_HEADER_YES_THIS', env)

    def test_clean_outgoing_headers(self):
        orh = []
        oah = []
        hdrs = {'test-header': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.items()))
        self.assertIn('test-header', hdrs)

        orh = ['test-header']
        oah = []
        hdrs = {'test-header': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.items()))
        self.assertNotIn('test-header', hdrs)

        orh = ['test-header-*']
        oah = []
        hdrs = {'test-header-one': 'value',
                'test-header-two': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.items()))
        self.assertNotIn('test-header-one', hdrs)
        self.assertNotIn('test-header-two', hdrs)

        orh = ['test-header-*']
        oah = ['test-header-two']
        hdrs = {'test-header-one': 'value',
                'test-header-two': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.items()))
        self.assertNotIn('test-header-one', hdrs)
        self.assertIn('test-header-two', hdrs)

        orh = ['test-header-*', 'test-other-header']
        oah = ['test-header-two', 'test-header-yes-*']
        hdrs = {'test-header-one': 'value',
                'test-header-two': 'value',
                'test-other-header': 'value',
                'test-header-yes': 'value',
                'test-header-yes-this': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.items()))
        self.assertNotIn('test-header-one', hdrs)
        self.assertIn('test-header-two', hdrs)
        self.assertNotIn('test-other-header', hdrs)
        self.assertNotIn('test-header-yes', hdrs)
        self.assertIn('test-header-yes-this', hdrs)

    def test_unicode_metadata_value(self):
        meta = {"temp-url-key": "test", "temp-url-key-2": u"test2"}
        results = tempurl.get_tempurl_keys_from_metadata(meta)
        for str_value in results:
            self.assertIsInstance(str_value, str)

    @mock.patch('swift.common.middleware.tempurl.time', return_value=0)
    def test_get_valid_with_ip_range(self, mock_time):
        method = 'GET'
        expires = (((24 + 1) * 60 + 1) * 60) + 1
        path = '/v1/a/c/o'
        key = 'abc'
        ip_range = '127.0.0.0/29'
        hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip_range, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'temp_url_ip_range=%s' % (sig, expires, ip_range),
            'REMOTE_ADDR': '127.0.0.1'},
        )
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertIn('expires', resp.headers)
        self.assertEqual('Fri, 02 Jan 1970 01:01:01 GMT',
                         resp.headers['expires'])
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    @mock.patch('swift.common.middleware.tempurl.time', return_value=0)
    def test_get_valid_with_ip_from_remote_addr(self, mock_time):
        method = 'GET'
        expires = (((24 + 1) * 60 + 1) * 60) + 1
        path = '/v1/a/c/o'
        key = 'abc'
        ip = '127.0.0.1'
        hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'temp_url_ip_range=%s' % (sig, expires, ip),
            'REMOTE_ADDR': ip},
        )
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertIn('expires', resp.headers)
        self.assertEqual('Fri, 02 Jan 1970 01:01:01 GMT',
                         resp.headers['expires'])
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid_with_fake_ip_from_x_forwarded_for(self):
        method = 'GET'
        expires = (((24 + 1) * 60 + 1) * 60) + 1
        path = '/v1/a/c/o'
        key = 'abc'
        ip = '127.0.0.1'
        remote_addr = '127.0.0.2'
        hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'temp_url_ip_range=%s' % (sig, expires, ip),
            'REMOTE_ADDR': remote_addr},
            headers={'x-forwarded-for': ip})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)

    @mock.patch('swift.common.middleware.tempurl.time', return_value=0)
    def test_get_valid_with_single_ipv6(self, mock_time):
        method = 'GET'
        expires = (((24 + 1) * 60 + 1) * 60) + 1
        path = '/v1/a/c/o'
        key = 'abc'
        ip = '2001:db8::'
        hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'temp_url_ip_range=%s' % (sig, expires, ip),
            'REMOTE_ADDR': '2001:db8::'},
        )
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertIn('expires', resp.headers)
        self.assertEqual('Fri, 02 Jan 1970 01:01:01 GMT',
                         resp.headers['expires'])
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    @mock.patch('swift.common.middleware.tempurl.time', return_value=0)
    def test_get_valid_with_ipv6_range(self, mock_time):
        method = 'GET'
        expires = (((24 + 1) * 60 + 1) * 60) + 1
        path = '/v1/a/c/o'
        key = 'abc'
        ip_range = '2001:db8::/127'
        hmac_body = 'ip=%s\n%s\n%s\n%s' % (ip_range, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'temp_url_ip_range=%s' % (sig, expires, ip_range),
            'REMOTE_ADDR': '2001:db8::'},
        )
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 200)
        self.assertIn('expires', resp.headers)
        self.assertEqual('Fri, 02 Jan 1970 01:01:01 GMT',
                         resp.headers['expires'])
        self.assertEqual(req.environ['swift.authorize_override'], True)
        self.assertEqual(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid_with_no_client_address(self):
        method = 'GET'
        expires = (((24 + 1) * 60 + 1) * 60) + 1
        path = '/v1/a/c/o'
        key = 'abc'
        ip = '127.0.0.1'
        hmac_body = '%s\n%s\n%s\n%s' % (ip, method, expires, path)
        sig = hmac.new(key, hmac_body, hashlib.sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'temp_url_ip_range=%s' % (sig, expires, ip)},
        )
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEqual(resp.status_int, 401)
        self.assertIn('Temp URL invalid', resp.body)
        self.assertIn('Www-Authenticate', resp.headers)


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):
        tempurl.filter_factory({})
        swift_info = utils.get_swift_info()
        self.assertIn('tempurl', swift_info)
        info = swift_info['tempurl']
        self.assertEqual(set(info['methods']),
                         set(('GET', 'HEAD', 'PUT', 'POST', 'DELETE')))
        self.assertEqual(set(info['incoming_remove_headers']),
                         set(('x-timestamp',)))
        self.assertEqual(set(info['incoming_allow_headers']), set())
        self.assertEqual(set(info['outgoing_remove_headers']),
                         set(('x-object-meta-*',)))
        self.assertEqual(set(info['outgoing_allow_headers']),
                         set(('x-object-meta-public-*',)))
        self.assertEqual(info['allowed_digests'], ['sha1', 'sha256', 'sha512'])

    def test_non_default_methods(self):
        tempurl.filter_factory({
            'methods': 'GET HEAD PUT DELETE BREW',
            'incoming_remove_headers': '',
            'incoming_allow_headers': 'x-timestamp x-versions-location',
            'outgoing_remove_headers': 'x-*',
            'outgoing_allow_headers': 'x-object-meta-* content-type',
            'allowed_digests': 'sha512 md5 not-a-valid-digest',
        })
        swift_info = utils.get_swift_info()
        self.assertIn('tempurl', swift_info)
        info = swift_info['tempurl']
        self.assertEqual(set(info['methods']),
                         set(('GET', 'HEAD', 'PUT', 'DELETE', 'BREW')))
        self.assertEqual(set(info['incoming_remove_headers']), set())
        self.assertEqual(set(info['incoming_allow_headers']),
                         set(('x-timestamp', 'x-versions-location')))
        self.assertEqual(set(info['outgoing_remove_headers']), set(('x-*', )))
        self.assertEqual(set(info['outgoing_allow_headers']),
                         set(('x-object-meta-*', 'content-type')))
        self.assertEqual(info['allowed_digests'], ['sha512'])

    def test_bad_config(self):
        with self.assertRaises(ValueError):
            tempurl.filter_factory({
                'allowed_digests': 'md4',
            })


if __name__ == '__main__':
    unittest.main()
