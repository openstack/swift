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

import hmac
import unittest
from hashlib import sha1
from time import time

from swift.common.middleware import tempauth, tempurl
from swift.common.swob import Request, Response, HeaderKeyDict
from swift.common import utils


class FakeApp(object):

    def __init__(self, status_headers_body_iter=None):
        self.calls = 0
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter([('404 Not Found', {
                'x-test-header-one-a': 'value1',
                'x-test-header-two-a': 'value2',
                'x-test-header-two-b': 'value3'}, '')])
        self.request = None

    def __call__(self, env, start_response):
        self.calls += 1
        self.request = Request.blank('', environ=env)
        if 'swift.authorize' in env:
            resp = env['swift.authorize'](self.request)
            if resp:
                return resp(env, start_response)
        status, headers, body = self.status_headers_body_iter.next()
        return Response(status=status, headers=headers,
                        body=body)(env, start_response)


class TestTempURL(unittest.TestCase):

    def setUp(self):
        self.app = FakeApp()
        self.auth = tempauth.filter_factory({'reseller_prefix': ''})(self.app)
        self.tempurl = tempurl.filter_factory({})(self.auth)

    def _make_request(self, path, environ=None, keys=(), **kwargs):
        if environ is None:
            environ = {}

        _junk, account, _junk, _junk = utils.split_path(path, 2, 4)
        self._fake_cache_environ(environ, account, keys)
        req = Request.blank(path, environ=environ, **kwargs)
        return req

    def _fake_cache_environ(self, environ, account, keys):
        """
        Fake out the caching layer for get_account_info(). Injects account data
        into environ such that keys are the tempurl keys, if set.
        """
        meta = {'swash': 'buckle'}
        for idx, key in enumerate(keys):
            meta_name = 'Temp-URL-key' + (("-%d" % (idx + 1) if idx else ""))
            if key:
                meta[meta_name] = key

        environ['swift.account/' + account] = {
            'status': 204,
            'container_count': '0',
            'total_object_count': '0',
            'bytes': '0',
            'meta': meta}

        container_cache_key = 'swift.container/' + account + '/c'
        environ.setdefault(container_cache_key, {'meta': {}})

    def test_passthrough(self):
        resp = self._make_request('/v1/a/c/o').get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' not in resp.body)

    def test_allow_options(self):
        self.app.status_headers_body_iter = iter([('200 Ok', {}, '')])
        resp = self._make_request(
            '/v1/a/c/o?temp_url_sig=abcde&temp_url_expires=12345',
            environ={'REQUEST_METHOD': 'OPTIONS'}).get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)

    def assert_valid_sig(self, expires, path, keys, sig, environ=None):
        if not environ:
            environ = {}
        environ['QUERY_STRING'] = 'temp_url_sig=%s&temp_url_expires=%s' % (
            sig, expires)
        req = self._make_request(path, keys=keys, environ=environ)
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-disposition'],
                          'attachment; filename="o"; ' + "filename*=UTF-8''o")
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        self.assert_valid_sig(expires, path, [key], sig)

    def test_get_valid_key2(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key1 = 'abc123'
        key2 = 'def456'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig1 = hmac.new(key1, hmac_body, sha1).hexdigest()
        sig2 = hmac.new(key2, hmac_body, sha1).hexdigest()
        for sig in (sig1, sig2):
            self.assert_valid_sig(expires, path, [key1, key2], sig)

    def test_get_valid_container_keys(self):
        environ = {}
        # Add two static container keys
        container_keys = ['me', 'other']
        meta = {}
        for idx, key in enumerate(container_keys):
            meta_name = 'Temp-URL-key' + (("-%d" % (idx + 1) if idx else ""))
            if key:
                meta[meta_name] = key
        environ['swift.container/a/c'] = {'meta': meta}

        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key1 = 'me'
        key2 = 'other'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig1 = hmac.new(key1, hmac_body, sha1).hexdigest()
        sig2 = hmac.new(key2, hmac_body, sha1).hexdigest()
        account_keys = []
        for sig in (sig1, sig2):
            self.assert_valid_sig(expires, path, account_keys, sig, environ)

    def test_get_valid_with_filename(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'filename=bob%%20%%22killer%%22.txt' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-disposition'],
                          'attachment; filename="bob %22killer%22.txt"; ' +
                          "filename*=UTF-8''bob%20%22killer%22.txt")
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_valid(self):
        method = 'HEAD'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'REQUEST_METHOD': 'HEAD',
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s'
            % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)

    def test_get_valid_with_filename_and_inline(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'filename=bob%%20%%22killer%%22.txt&inline=' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-disposition'], 'inline')
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid_with_inline(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'inline=' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-disposition'], 'inline')
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_obj_odd_chars(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/a\r\nb'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-disposition'],
                          'attachment; filename="a%0D%0Ab"; ' +
                          "filename*=UTF-8''a%0D%0Ab")
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_obj_odd_chars_in_content_disposition_metadata(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        headers = [('Content-Disposition', 'attachment; filename="fu\nbar"')]
        self.tempurl.app = FakeApp(iter([('200 Ok', headers, '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-disposition'],
                          'attachment; filename="fu%0Abar"')
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_obj_trailing_slash(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o/'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['content-disposition'],
                          'attachment; filename="o"; ' +
                          "filename*=UTF-8''o")
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_filename_trailing_slash(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(path, keys=[key], environ={
            'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
            'filename=/i/want/this/just/as/it/is/' % (sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', (), '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(
            resp.headers['content-disposition'],
            'attachment; filename="/i/want/this/just/as/it/is/"; ' +
            "filename*=UTF-8''/i/want/this/just/as/it/is/")
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_valid_but_404(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertFalse('content-disposition' in resp.headers)
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_put_not_allowed_by_get(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'PUT',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_put_valid(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'PUT',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_get_not_allowed_by_put(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_missing_sig(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_expires=%s' % expires})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_missing_expires(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s' % sig})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_bad_path(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_no_key(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_head_allowed_by_get(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_allowed_by_put(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_allowed_by_post(self):
        method = 'POST'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(req.environ['swift.authorize_override'], True)
        self.assertEquals(req.environ['REMOTE_USER'], '.wsgi.tempurl')

    def test_head_otherwise_not_allowed(self):
        method = 'PUT'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        # Deliberately fudge expires to show HEADs aren't just automatically
        # allowed.
        expires += 1
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'HEAD',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_post_when_forbidden_by_config(self):
        self.tempurl.methods.remove('POST')
        method = 'POST'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'POST',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_delete_when_forbidden_by_config(self):
        self.tempurl.methods.remove('DELETE')
        method = 'DELETE'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'DELETE',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_delete_allowed(self):
        method = 'DELETE'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'DELETE',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)

    def test_unknown_not_allowed(self):
        method = 'UNKNOWN'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'REQUEST_METHOD': 'UNKNOWN',
                     'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                         sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_changed_path_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path + '2', keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_changed_sig_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        if sig[-1] != '0':
            sig = sig[:-1] + '0'
        else:
            sig = sig[:-1] + '1'
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_changed_expires_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires + 1)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_different_key_invalid(self):
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key + '2'],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_removed_incoming_header(self):
        self.tempurl = tempurl.filter_factory({
            'incoming_remove_headers': 'x-remove-this'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-remove-this': 'value'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('x-remove-this' not in self.app.request.headers)

    def test_removed_incoming_headers_match(self):
        self.tempurl = tempurl.filter_factory({
            'incoming_remove_headers': 'x-remove-this-*',
            'incoming_allow_headers': 'x-remove-this-except-this'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-remove-this-one': 'value1',
                     'x-remove-this-except-this': 'value2'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('x-remove-this-one' not in self.app.request.headers)
        self.assertEquals(
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
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-conflict-header': 'value'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
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
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={'x-conflict-header-test': 'value'},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('x-conflict-header-test' in self.app.request.headers)

    def test_removed_outgoing_header(self):
        self.tempurl = tempurl.filter_factory({
            'outgoing_remove_headers': 'x-test-header-one-a'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('x-test-header-one-a' not in resp.headers)
        self.assertEquals(resp.headers['x-test-header-two-a'], 'value2')

    def test_removed_outgoing_headers_match(self):
        self.tempurl = tempurl.filter_factory({
            'outgoing_remove_headers': 'x-test-header-two-*',
            'outgoing_allow_headers': 'x-test-header-two-b'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(resp.headers['x-test-header-one-a'], 'value1')
        self.assertTrue('x-test-header-two-a' not in resp.headers)
        self.assertEquals(resp.headers['x-test-header-two-b'], 'value3')

    def test_allow_trumps_outgoing_header_conflict(self):
        self.tempurl = tempurl.filter_factory({
            'outgoing_remove_headers': 'x-conflict-header',
            'outgoing_allow_headers': 'x-conflict-header'})(self.auth)
        method = 'GET'
        expires = int(time() + 86400)
        path = '/v1/a/c/o'
        key = 'abc'
        hmac_body = '%s\n%s\n%s' % (method, expires, path)
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', {
            'X-Conflict-Header': 'value'}, '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
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
        sig = hmac.new(key, hmac_body, sha1).hexdigest()
        req = self._make_request(
            path, keys=[key],
            headers={},
            environ={'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                sig, expires)})
        self.tempurl.app = FakeApp(iter([('200 Ok', {
            'X-Conflict-Header-Test': 'value'}, '123')]))
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 200)
        self.assertTrue('x-conflict-header-test' in resp.headers)
        self.assertEqual(resp.headers['x-conflict-header-test'], 'value')

    def test_get_account(self):
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'HEAD', 'PATH_INFO': '/v1/a/c/o'}), 'a')
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c/o'}), 'a')
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'PUT', 'PATH_INFO': '/v1/a/c/o'}), 'a')
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'POST', 'PATH_INFO': '/v1/a/c/o'}), 'a')
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'DELETE', 'PATH_INFO': '/v1/a/c/o'}), 'a')
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'UNKNOWN', 'PATH_INFO': '/v1/a/c/o'}), None)
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c/'}), None)
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c//////'}), None)
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c///o///'}), 'a')
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c'}), None)
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a//o'}), None)
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1//c/o'}), None)
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '//a/c/o'}), None)
        self.assertEquals(self.tempurl._get_account({
            'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v2/a/c/o'}), None)

    def test_get_temp_url_info(self):
        s = 'f5d5051bddf5df7e27c628818738334f'
        e = int(time() + 86400)
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                    s, e)}),
            (s, e, None, None))
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
                 'filename=bobisyouruncle' % (s, e)}),
            (s, e, 'bobisyouruncle', None))
        self.assertEquals(
            self.tempurl._get_temp_url_info({}),
            (None, None, None, None))
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_expires=%s' % e}),
            (None, e, None, None))
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s' % s}),
            (s, None, None, None))
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=bad' % (
                    s)}),
            (s, 0, None, None))
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
                 'inline=' % (s, e)}),
            (s, e, None, True))
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s&'
                 'filename=bobisyouruncle&inline=' % (s, e)}),
            (s, e, 'bobisyouruncle', True))
        e = int(time() - 1)
        self.assertEquals(
            self.tempurl._get_temp_url_info(
                {'QUERY_STRING': 'temp_url_sig=%s&temp_url_expires=%s' % (
                    s, e)}),
            (s, 0, None, None))

    def test_get_hmacs(self):
        self.assertEquals(
            self.tempurl._get_hmacs(
                {'REQUEST_METHOD': 'GET', 'PATH_INFO': '/v1/a/c/o'},
                1, ['abc']),
            ['026d7f7cc25256450423c7ad03fc9f5ffc1dab6d'])
        self.assertEquals(
            self.tempurl._get_hmacs(
                {'REQUEST_METHOD': 'HEAD', 'PATH_INFO': '/v1/a/c/o'},
                1, ['abc'], request_method='GET'),
            ['026d7f7cc25256450423c7ad03fc9f5ffc1dab6d'])

    def test_invalid(self):

        def _start_response(status, headers, exc_info=None):
            self.assertTrue(status, '401 Unauthorized')

        self.assertTrue('Temp URL invalid' in ''.join(
            self.tempurl._invalid({'REQUEST_METHOD': 'GET'},
                                  _start_response)))
        self.assertEquals('', ''.join(
            self.tempurl._invalid({'REQUEST_METHOD': 'HEAD'},
                                  _start_response)))

    def test_auth_scheme_value(self):
        # Passthrough
        environ = {}
        resp = self._make_request('/v1/a/c/o', environ=environ).get_response(
            self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' not in resp.body)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertTrue('swift.auth_scheme' not in environ)

        # Rejected by TempURL
        req = self._make_request('/v1/a/c/o', keys=['abc'],
                                 environ={'REQUEST_METHOD': 'PUT',
                                 'QUERY_STRING':
                                 'temp_url_sig=dummy&temp_url_expires=1234'})
        resp = req.get_response(self.tempurl)
        self.assertEquals(resp.status_int, 401)
        self.assertTrue('Temp URL invalid' in resp.body)
        self.assert_('Www-Authenticate' in resp.headers)

    def test_clean_incoming_headers(self):
        irh = ''
        iah = ''
        env = {'HTTP_TEST_HEADER': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertTrue('HTTP_TEST_HEADER' in env)

        irh = 'test-header'
        iah = ''
        env = {'HTTP_TEST_HEADER': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertTrue('HTTP_TEST_HEADER' not in env)

        irh = 'test-header-*'
        iah = ''
        env = {'HTTP_TEST_HEADER_ONE': 'value',
               'HTTP_TEST_HEADER_TWO': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertTrue('HTTP_TEST_HEADER_ONE' not in env)
        self.assertTrue('HTTP_TEST_HEADER_TWO' not in env)

        irh = 'test-header-*'
        iah = 'test-header-two'
        env = {'HTTP_TEST_HEADER_ONE': 'value',
               'HTTP_TEST_HEADER_TWO': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertTrue('HTTP_TEST_HEADER_ONE' not in env)
        self.assertTrue('HTTP_TEST_HEADER_TWO' in env)

        irh = 'test-header-* test-other-header'
        iah = 'test-header-two test-header-yes-*'
        env = {'HTTP_TEST_HEADER_ONE': 'value',
               'HTTP_TEST_HEADER_TWO': 'value',
               'HTTP_TEST_OTHER_HEADER': 'value',
               'HTTP_TEST_HEADER_YES': 'value',
               'HTTP_TEST_HEADER_YES_THIS': 'value'}
        tempurl.TempURL(
            None, {'incoming_remove_headers': irh,
                   'incoming_allow_headers': iah}
        )._clean_incoming_headers(env)
        self.assertTrue('HTTP_TEST_HEADER_ONE' not in env)
        self.assertTrue('HTTP_TEST_HEADER_TWO' in env)
        self.assertTrue('HTTP_TEST_OTHER_HEADER' not in env)
        self.assertTrue('HTTP_TEST_HEADER_YES' not in env)
        self.assertTrue('HTTP_TEST_HEADER_YES_THIS' in env)

    def test_clean_outgoing_headers(self):
        orh = ''
        oah = ''
        hdrs = {'test-header': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.iteritems()))
        self.assertTrue('test-header' in hdrs)

        orh = 'test-header'
        oah = ''
        hdrs = {'test-header': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.iteritems()))
        self.assertTrue('test-header' not in hdrs)

        orh = 'test-header-*'
        oah = ''
        hdrs = {'test-header-one': 'value',
                'test-header-two': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.iteritems()))
        self.assertTrue('test-header-one' not in hdrs)
        self.assertTrue('test-header-two' not in hdrs)

        orh = 'test-header-*'
        oah = 'test-header-two'
        hdrs = {'test-header-one': 'value',
                'test-header-two': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.iteritems()))
        self.assertTrue('test-header-one' not in hdrs)
        self.assertTrue('test-header-two' in hdrs)

        orh = 'test-header-* test-other-header'
        oah = 'test-header-two test-header-yes-*'
        hdrs = {'test-header-one': 'value',
                'test-header-two': 'value',
                'test-other-header': 'value',
                'test-header-yes': 'value',
                'test-header-yes-this': 'value'}
        hdrs = HeaderKeyDict(tempurl.TempURL(
            None,
            {'outgoing_remove_headers': orh, 'outgoing_allow_headers': oah}
        )._clean_outgoing_headers(hdrs.iteritems()))
        self.assertTrue('test-header-one' not in hdrs)
        self.assertTrue('test-header-two' in hdrs)
        self.assertTrue('test-other-header' not in hdrs)
        self.assertTrue('test-header-yes' not in hdrs)
        self.assertTrue('test-header-yes-this' in hdrs)

    def test_unicode_metadata_value(self):
        meta = {"temp-url-key": "test", "temp-url-key-2": u"test2"}
        results = tempurl.get_tempurl_keys_from_metadata(meta)
        for str_value in results:
            self.assertTrue(isinstance(str_value, str))


class TestSwiftInfo(unittest.TestCase):
    def setUp(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_registered_defaults(self):
        tempurl.filter_factory({})
        swift_info = utils.get_swift_info()
        self.assertTrue('tempurl' in swift_info)
        self.assertEqual(set(swift_info['tempurl']['methods']),
                         set(('GET', 'HEAD', 'PUT', 'POST', 'DELETE')))

    def test_non_default_methods(self):
        tempurl.filter_factory({'methods': 'GET HEAD PUT DELETE BREW'})
        swift_info = utils.get_swift_info()
        self.assertTrue('tempurl' in swift_info)
        self.assertEqual(set(swift_info['tempurl']['methods']),
                         set(('GET', 'HEAD', 'PUT', 'DELETE', 'BREW')))


if __name__ == '__main__':
    unittest.main()
