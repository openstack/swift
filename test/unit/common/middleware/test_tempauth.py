# -*- coding: utf-8 -*-
# Copyright (c) 2011-2015 OpenStack Foundation
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

import json
import unittest
from contextlib import contextmanager
from base64 import b64encode as _b64encode
from time import time

import six
from six.moves.urllib.parse import quote, urlparse
from swift.common.middleware import tempauth as auth
from swift.common.middleware.acl import format_acl
from swift.common.swob import Request, Response
from swift.common.utils import split_path

NO_CONTENT_RESP = (('204 No Content', {}, ''),)   # mock server response


def b64encode(str_or_bytes):
    if not isinstance(str_or_bytes, bytes):
        str_or_bytes = str_or_bytes.encode('utf8')
    return _b64encode(str_or_bytes).decode('ascii')


class FakeMemcache(object):

    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, time=0):
        if isinstance(value, (tuple, list)):
            decoded = []
            for elem in value:
                if isinstance(elem, bytes):
                    decoded.append(elem.decode('utf8'))
                else:
                    decoded.append(elem)
            value = tuple(decoded)
        self.store[key] = value
        return True

    def incr(self, key, time=0):
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

    def __init__(self, status_headers_body_iter=None, acl=None, sync_key=None):
        self.calls = 0
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter([('404 Not Found', {}, '')])
        self.acl = acl
        self.sync_key = sync_key

    def __call__(self, env, start_response):
        self.calls += 1
        self.request = Request(env)
        if self.acl:
            self.request.acl = self.acl
        if self.sync_key:
            self.request.environ['swift_sync_key'] = self.sync_key
        if 'swift.authorize' in env:
            resp = env['swift.authorize'](self.request)
            if resp:
                return resp(env, start_response)
        status, headers, body = next(self.status_headers_body_iter)
        return Response(status=status, headers=headers,
                        body=body)(env, start_response)


class FakeConn(object):

    def __init__(self, status_headers_body_iter=None):
        self.calls = 0
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter([('404 Not Found', {}, '')])

    def request(self, method, path, headers):
        self.calls += 1
        self.request_path = path
        self.status, self.headers, self.body = \
            next(self.status_headers_body_iter)
        self.status, self.reason = self.status.split(' ', 1)
        self.status = int(self.status)

    def getresponse(self):
        return self

    def read(self):
        body = self.body
        self.body = ''
        return body


class TestAuth(unittest.TestCase):

    def setUp(self):
        self.test_auth = auth.filter_factory({})(FakeApp())

    def _make_request(self, path, **kwargs):
        req = Request.blank(path, **kwargs)
        req.environ['swift.cache'] = FakeMemcache()
        return req

    def test_reseller_prefix_init(self):
        app = FakeApp()
        ath = auth.filter_factory({})(app)
        self.assertEqual(ath.reseller_prefix, 'AUTH_')
        self.assertEqual(ath.reseller_prefixes, ['AUTH_'])
        ath = auth.filter_factory({'reseller_prefix': 'TEST'})(app)
        self.assertEqual(ath.reseller_prefix, 'TEST_')
        self.assertEqual(ath.reseller_prefixes, ['TEST_'])
        ath = auth.filter_factory({'reseller_prefix': 'TEST_'})(app)
        self.assertEqual(ath.reseller_prefix, 'TEST_')
        self.assertEqual(ath.reseller_prefixes, ['TEST_'])
        ath = auth.filter_factory({'reseller_prefix': ''})(app)
        self.assertEqual(ath.reseller_prefix, '')
        self.assertEqual(ath.reseller_prefixes, [''])
        ath = auth.filter_factory({'reseller_prefix': '    '})(app)
        self.assertEqual(ath.reseller_prefix, '')
        self.assertEqual(ath.reseller_prefixes, [''])
        ath = auth.filter_factory({'reseller_prefix': '  ''  '})(app)
        self.assertEqual(ath.reseller_prefix, '')
        self.assertEqual(ath.reseller_prefixes, [''])
        ath = auth.filter_factory({'reseller_prefix': " '', TEST"})(app)
        self.assertEqual(ath.reseller_prefix, '')
        self.assertTrue('' in ath.reseller_prefixes)
        self.assertTrue('TEST_' in ath.reseller_prefixes)

    def test_auth_prefix_init(self):
        app = FakeApp()
        ath = auth.filter_factory({})(app)
        self.assertEqual(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'auth_prefix': ''})(app)
        self.assertEqual(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'auth_prefix': '/'})(app)
        self.assertEqual(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'auth_prefix': '/test/'})(app)
        self.assertEqual(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'auth_prefix': '/test'})(app)
        self.assertEqual(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'auth_prefix': 'test/'})(app)
        self.assertEqual(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'auth_prefix': 'test'})(app)
        self.assertEqual(ath.auth_prefix, '/test/')

    def test_top_level_deny(self):
        req = self._make_request('/')
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(req.environ['swift.authorize'],
                         self.test_auth.denied_response)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="unknown"')

    def test_anon(self):
        req = self._make_request('/v1/AUTH_account')
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(req.environ['swift.authorize'],
                         self.test_auth.authorize)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_account"')

    def test_anon_badpath(self):
        req = self._make_request('/v1')
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="unknown"')

    def test_override_asked_for_but_not_allowed(self):
        self.test_auth = \
            auth.filter_factory({'allow_overrides': 'false'})(FakeApp())
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_account"')
        self.assertEqual(req.environ['swift.authorize'],
                         self.test_auth.authorize)

    def test_override_asked_for_and_allowed(self):
        self.test_auth = \
            auth.filter_factory({'allow_overrides': 'true'})(FakeApp())
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('swift.authorize', req.environ)

    def test_override_default_allowed(self):
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 404)
        self.assertNotIn('swift.authorize', req.environ)

    def test_auth_deny_non_reseller_prefix(self):
        req = self._make_request('/v1/BLAH_account',
                                 headers={'X-Auth-Token': 'BLAH_t'})
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="BLAH_account"')
        self.assertEqual(req.environ['swift.authorize'],
                         self.test_auth.denied_response)

    def test_auth_deny_non_reseller_prefix_no_override(self):
        fake_authorize = lambda x: Response(status='500 Fake')
        req = self._make_request('/v1/BLAH_account',
                                 headers={'X-Auth-Token': 'BLAH_t'},
                                 environ={'swift.authorize': fake_authorize}
                                 )
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 500)
        self.assertEqual(req.environ['swift.authorize'], fake_authorize)

    def test_auth_no_reseller_prefix_deny(self):
        # Ensures that when we have no reseller prefix, we don't deny a request
        # outright but set up a denial swift.authorize and pass the request on
        # down the chain.
        local_app = FakeApp()
        local_auth = auth.filter_factory({'reseller_prefix': ''})(local_app)
        req = self._make_request('/v1/account',
                                 headers={'X-Auth-Token': 't'})
        resp = req.get_response(local_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="account"')
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.denied_response)

    def test_auth_reseller_prefix_with_s3_deny(self):
        # Ensures that when we have a reseller prefix and using a middleware
        # relying on Http-Authorization (for example swift3), we don't deny a
        # request outright but set up a denial swift.authorize and pass the
        # request on down the chain.
        local_app = FakeApp()
        local_auth = auth.filter_factory({'reseller_prefix': 'PRE'})(local_app)
        req = self._make_request('/v1/account',
                                 headers={'X-Auth-Token': 't',
                                          'Authorization': 'AWS user:pw'})
        resp = req.get_response(local_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.denied_response)

    def test_auth_with_swift3_authorization_good(self):
        local_app = FakeApp()
        local_auth = auth.filter_factory(
            {'user_s3_s3': 'secret .admin'})(local_app)
        req = self._make_request('/v1/s3:s3', environ={
            'swift3.auth_details': {
                'access_key': 's3:s3',
                'signature': b64encode('sig'),
                'string_to_sign': 't',
                'check_signature': lambda secret: True}})
        resp = req.get_response(local_auth)

        self.assertEqual(resp.status_int, 404)
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['PATH_INFO'], '/v1/AUTH_s3')
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.authorize)

    def test_auth_with_s3api_authorization_good(self):
        local_app = FakeApp()
        local_auth = auth.filter_factory(
            {'user_s3_s3': 'secret .admin'})(local_app)
        req = self._make_request('/v1/s3:s3', environ={
            's3api.auth_details': {
                'access_key': 's3:s3',
                'signature': b64encode('sig'),
                'string_to_sign': 't',
                'check_signature': lambda secret: True}})
        resp = req.get_response(local_auth)

        self.assertEqual(resp.status_int, 404)
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['PATH_INFO'], '/v1/AUTH_s3')
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.authorize)

    def test_auth_with_swift3_authorization_invalid(self):
        local_app = FakeApp()
        local_auth = auth.filter_factory(
            {'user_s3_s3': 'secret .admin'})(local_app)
        req = self._make_request('/v1/s3:s3', environ={
            'swift3.auth_details': {
                'access_key': 's3:s3',
                'signature': b64encode('sig'),
                'string_to_sign': 't',
                'check_signature': lambda secret: False}})
        resp = req.get_response(local_auth)

        self.assertEqual(resp.status_int, 401)
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['PATH_INFO'], '/v1/s3:s3')
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.denied_response)

    def test_auth_with_s3api_authorization_invalid(self):
        local_app = FakeApp()
        local_auth = auth.filter_factory(
            {'user_s3_s3': 'secret .admin'})(local_app)
        req = self._make_request('/v1/s3:s3', environ={
            's3api.auth_details': {
                'access_key': 's3:s3',
                'signature': b64encode('sig'),
                'string_to_sign': 't',
                'check_signature': lambda secret: False}})
        resp = req.get_response(local_auth)

        self.assertEqual(resp.status_int, 401)
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['PATH_INFO'], '/v1/s3:s3')
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.denied_response)

    def test_auth_with_old_swift3_details(self):
        local_app = FakeApp()
        local_auth = auth.filter_factory(
            {'user_s3_s3': 'secret .admin'})(local_app)
        req = self._make_request('/v1/s3:s3', environ={
            'swift3.auth_details': {
                'access_key': 's3:s3',
                'signature': b64encode('sig'),
                'string_to_sign': 't'}})
        resp = req.get_response(local_auth)

        self.assertEqual(resp.status_int, 401)
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['PATH_INFO'], '/v1/s3:s3')
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.denied_response)

    def test_auth_with_old_s3api_details(self):
        local_app = FakeApp()
        local_auth = auth.filter_factory(
            {'user_s3_s3': 'secret .admin'})(local_app)
        req = self._make_request('/v1/s3:s3', environ={
            's3api.auth_details': {
                'access_key': 's3:s3',
                'signature': b64encode('sig'),
                'string_to_sign': 't'}})
        resp = req.get_response(local_auth)

        self.assertEqual(resp.status_int, 401)
        self.assertEqual(local_app.calls, 1)
        self.assertEqual(req.environ['PATH_INFO'], '/v1/s3:s3')
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.denied_response)

    def test_auth_no_reseller_prefix_no_token(self):
        # Check that normally we set up a call back to our authorize.
        local_auth = auth.filter_factory({'reseller_prefix': ''})(FakeApp())
        req = self._make_request('/v1/account')
        resp = req.get_response(local_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="account"')
        self.assertEqual(req.environ['swift.authorize'],
                         local_auth.authorize)
        # Now make sure we don't override an existing swift.authorize when we
        # have no reseller prefix.
        local_auth = \
            auth.filter_factory({'reseller_prefix': ''})(FakeApp())
        local_authorize = lambda req: Response('test')
        req = self._make_request('/v1/account', environ={'swift.authorize':
                                 local_authorize})
        resp = req.get_response(local_auth)
        self.assertEqual(req.environ['swift.authorize'], local_authorize)
        self.assertEqual(resp.status_int, 200)

    def test_auth_fail(self):
        resp = self._make_request(
            '/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')

    def test_authorize_bad_path(self):
        req = self._make_request('/badpath')
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="unknown"')
        req = self._make_request('/badpath')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

    def test_authorize_account_access(self):
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        self.assertIsNone(self.test_auth.authorize(req))
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

    def test_authorize_acl_group_access(self):
        self.test_auth = auth.filter_factory({})(
            FakeApp(iter(NO_CONTENT_RESP * 3)))
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act'
        self.assertIsNone(self.test_auth.authorize(req))
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr'
        self.assertIsNone(self.test_auth.authorize(req))
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act2'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr2'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

    def test_deny_cross_reseller(self):
        # Tests that cross-reseller is denied, even if ACLs/group names match
        req = self._make_request('/v1/OTHER_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        req.acl = 'act'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

    def test_authorize_acl_referer_after_user_groups(self):
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr'
        req.acl = '.r:*,act:usr'
        self.assertIsNone(self.test_auth.authorize(req))

    def test_authorize_acl_referrer_access(self):
        self.test_auth = auth.filter_factory({})(
            FakeApp(iter(NO_CONTENT_RESP * 6)))
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*,.rlistings'
        self.assertIsNone(self.test_auth.authorize(req))
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*'  # No listings allowed
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:.example.com,.rlistings'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com,.rlistings'
        self.assertIsNone(self.test_auth.authorize(req))
        req = self._make_request('/v1/AUTH_cfa/c')
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')
        req = self._make_request('/v1/AUTH_cfa/c')
        req.acl = '.r:*,.rlistings'
        self.assertIsNone(self.test_auth.authorize(req))
        req = self._make_request('/v1/AUTH_cfa/c')
        req.acl = '.r:*'  # No listings allowed
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')
        req = self._make_request('/v1/AUTH_cfa/c')
        req.acl = '.r:.example.com,.rlistings'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')
        req = self._make_request('/v1/AUTH_cfa/c')
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com,.rlistings'
        self.assertIsNone(self.test_auth.authorize(req))

    def test_detect_reseller_request(self):
        req = self._make_request('/v1/AUTH_admin',
                                 headers={'X-Auth-Token': 'AUTH_t'})
        cache_key = 'AUTH_/token/AUTH_t'
        cache_entry = (time() + 3600, '.reseller_admin')
        req.environ['swift.cache'].set(cache_key, cache_entry)
        req.get_response(self.test_auth)
        self.assertTrue(req.environ.get('reseller_request', False))

    def test_account_put_permissions(self):
        self.test_auth = auth.filter_factory({})(
            FakeApp(iter(NO_CONTENT_RESP * 4)))
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

        # Even PUTs to your own account as account admin should fail
        req = self._make_request('/v1/AUTH_old',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertIsNone(resp)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

    def test_account_delete_permissions(self):
        self.test_auth = auth.filter_factory({})(
            FakeApp(iter(NO_CONTENT_RESP * 4)))
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

        # Even DELETEs to your own account as account admin should fail
        req = self._make_request('/v1/AUTH_old',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertIsNone(resp)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        self.assertEqual(resp.status_int, 403)

    def test_get_token_success(self):
        # Example of how to simulate the auth transaction
        test_auth = auth.filter_factory({'user_ac_user': 'testing'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'ac:user', 'X-Auth-Key': 'testing'})
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue(resp.headers['x-storage-url'].endswith('/v1/AUTH_ac'))
        self.assertTrue(resp.headers['x-auth-token'].startswith('AUTH_'))
        self.assertEqual(resp.headers['x-auth-token'],
                         resp.headers['x-storage-token'])
        self.assertAlmostEqual(int(resp.headers['x-auth-token-expires']),
                               auth.DEFAULT_TOKEN_LIFE - 0.5, delta=0.5)
        self.assertGreater(len(resp.headers['x-auth-token']), 10)

    def test_get_token_success_other_auth_prefix(self):
        test_auth = auth.filter_factory({'user_ac_user': 'testing',
                                         'auth_prefix': '/other/'})(FakeApp())
        req = self._make_request(
            '/other/v1.0',
            headers={'X-Auth-User': 'ac:user', 'X-Auth-Key': 'testing'})
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertTrue(resp.headers['x-storage-url'].endswith('/v1/AUTH_ac'))
        self.assertTrue(resp.headers['x-auth-token'].startswith('AUTH_'))
        self.assertTrue(len(resp.headers['x-auth-token']) > 10)

    def test_use_token_success(self):
        # Example of how to simulate an authorized request
        test_auth = auth.filter_factory({'user_acct_user': 'testing'})(
            FakeApp(iter(NO_CONTENT_RESP * 1)))
        req = self._make_request('/v1/AUTH_acct',
                                 headers={'X-Auth-Token': 'AUTH_t'})
        cache_key = 'AUTH_/token/AUTH_t'
        cache_entry = (time() + 3600, 'AUTH_acct')
        req.environ['swift.cache'].set(cache_key, cache_entry)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 204)

    def test_get_token_fail(self):
        resp = self._make_request('/auth/v1.0').get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="unknown"')
        resp = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="act"')

    def test_get_token_fail_invalid_x_auth_user_format(self):
        resp = self._make_request(
            '/auth/v1/act/auth',
            headers={'X-Auth-User': 'usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="act"')

    def test_get_token_fail_non_matching_account_in_request(self):
        resp = self._make_request(
            '/auth/v1/act/auth',
            headers={'X-Auth-User': 'act2:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="act"')

    def test_get_token_fail_bad_path(self):
        resp = self._make_request(
            '/auth/v1/act/auth/invalid',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEqual(resp.status_int, 400)

    def test_get_token_fail_missing_key(self):
        resp = self._make_request(
            '/auth/v1/act/auth',
            headers={'X-Auth-User': 'act:usr'}).get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="act"')

    def test_object_name_containing_slash(self):
        test_auth = auth.filter_factory({'user_acct_user': 'testing'})(
            FakeApp(iter(NO_CONTENT_RESP * 1)))
        req = self._make_request('/v1/AUTH_acct/cont/obj/name/with/slash',
                                 headers={'X-Auth-Token': 'AUTH_t'})
        cache_key = 'AUTH_/token/AUTH_t'
        cache_entry = (time() + 3600, 'AUTH_acct')
        req.environ['swift.cache'].set(cache_key, cache_entry)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 204)

    def test_storage_url_default(self):
        self.test_auth = \
            auth.filter_factory({'user_test_tester': 'testing'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'testing'})
        del req.environ['HTTP_HOST']
        req.environ['SERVER_NAME'] = 'bob'
        req.environ['SERVER_PORT'] = '1234'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['x-storage-url'],
                         'http://bob:1234/v1/AUTH_test')

    def test_storage_url_based_on_host(self):
        self.test_auth = \
            auth.filter_factory({'user_test_tester': 'testing'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'testing'})
        req.environ['HTTP_HOST'] = 'somehost:5678'
        req.environ['SERVER_NAME'] = 'bob'
        req.environ['SERVER_PORT'] = '1234'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['x-storage-url'],
                         'http://somehost:5678/v1/AUTH_test')

    def test_storage_url_overridden_scheme(self):
        self.test_auth = \
            auth.filter_factory({'user_test_tester': 'testing',
                                 'storage_url_scheme': 'fake'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'testing'})
        req.environ['HTTP_HOST'] = 'somehost:5678'
        req.environ['SERVER_NAME'] = 'bob'
        req.environ['SERVER_PORT'] = '1234'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['x-storage-url'],
                         'fake://somehost:5678/v1/AUTH_test')

    def test_use_old_token_from_memcached(self):
        self.test_auth = \
            auth.filter_factory({'user_test_tester': 'testing',
                                 'storage_url_scheme': 'fake'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'testing'})
        req.environ['HTTP_HOST'] = 'somehost:5678'
        req.environ['SERVER_NAME'] = 'bob'
        req.environ['SERVER_PORT'] = '1234'
        req.environ['swift.cache'].set('AUTH_/user/test:tester', 'uuid_token')
        expires = time() + 180
        req.environ['swift.cache'].set('AUTH_/token/uuid_token',
                                       (expires, 'test,test:tester'))
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['x-auth-token'], 'uuid_token')
        self.assertEqual(resp.headers['x-auth-token'],
                         resp.headers['x-storage-token'])
        self.assertAlmostEqual(int(resp.headers['x-auth-token-expires']),
                               179.5, delta=0.5)

    def test_old_token_overdate(self):
        self.test_auth = \
            auth.filter_factory({'user_test_tester': 'testing',
                                 'storage_url_scheme': 'fake'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'testing'})
        req.environ['HTTP_HOST'] = 'somehost:5678'
        req.environ['SERVER_NAME'] = 'bob'
        req.environ['SERVER_PORT'] = '1234'
        req.environ['swift.cache'].set('AUTH_/user/test:tester', 'uuid_token')
        req.environ['swift.cache'].set('AUTH_/token/uuid_token',
                                       (0, 'test,test:tester'))
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertNotEqual(resp.headers['x-auth-token'], 'uuid_token')
        self.assertEqual(resp.headers['x-auth-token'][:7], 'AUTH_tk')
        self.assertAlmostEqual(int(resp.headers['x-auth-token-expires']),
                               auth.DEFAULT_TOKEN_LIFE - 0.5, delta=0.5)

    def test_old_token_with_old_data(self):
        self.test_auth = \
            auth.filter_factory({'user_test_tester': 'testing',
                                 'storage_url_scheme': 'fake'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'test:tester', 'X-Auth-Key': 'testing'})
        req.environ['HTTP_HOST'] = 'somehost:5678'
        req.environ['SERVER_NAME'] = 'bob'
        req.environ['SERVER_PORT'] = '1234'
        req.environ['swift.cache'].set('AUTH_/user/test:tester', 'uuid_token')
        req.environ['swift.cache'].set('AUTH_/token/uuid_token',
                                       (time() + 99, 'test,test:tester,.role'))
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertNotEqual(resp.headers['x-auth-token'], 'uuid_token')
        self.assertEqual(resp.headers['x-auth-token'][:7], 'AUTH_tk')
        self.assertAlmostEqual(int(resp.headers['x-auth-token-expires']),
                               auth.DEFAULT_TOKEN_LIFE - 0.5, delta=0.5)

    def test_reseller_admin_is_owner(self):
        orig_authorize = self.test_auth.authorize
        owner_values = []

        def mitm_authorize(req):
            rv = orig_authorize(req)
            owner_values.append(req.environ.get('swift_owner', False))
            return rv

        self.test_auth.authorize = mitm_authorize

        req = self._make_request('/v1/AUTH_cfa',
                                 headers={'X-Auth-Token': 'AUTH_t'})
        req.remote_user = '.reseller_admin'
        self.test_auth.authorize(req)
        self.assertEqual(owner_values, [True])

    def test_admin_is_owner(self):
        orig_authorize = self.test_auth.authorize
        owner_values = []

        def mitm_authorize(req):
            rv = orig_authorize(req)
            owner_values.append(req.environ.get('swift_owner', False))
            return rv

        self.test_auth.authorize = mitm_authorize

        req = self._make_request(
            '/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'})
        req.remote_user = 'AUTH_cfa'
        self.test_auth.authorize(req)
        self.assertEqual(owner_values, [True])

    def test_regular_is_not_owner(self):
        orig_authorize = self.test_auth.authorize
        owner_values = []

        def mitm_authorize(req):
            rv = orig_authorize(req)
            owner_values.append(req.environ.get('swift_owner', False))
            return rv

        self.test_auth.authorize = mitm_authorize

        req = self._make_request(
            '/v1/AUTH_cfa/c',
            headers={'X-Auth-Token': 'AUTH_t'})
        req.remote_user = 'act:usr'
        self.test_auth.authorize(req)
        self.assertEqual(owner_values, [False])

    def test_sync_request_success(self):
        self.test_auth.app = FakeApp(iter(NO_CONTENT_RESP * 1),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 204)

    def test_sync_request_fail_key(self):
        self.test_auth.app = FakeApp(sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'wrongsecret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')

        self.test_auth.app = FakeApp(sync_key='othersecret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')

        self.test_auth.app = FakeApp(sync_key=None)
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')

    def test_sync_request_fail_no_timestamp(self):
        self.test_auth.app = FakeApp(sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="AUTH_cfa"')

    def test_sync_request_success_lb_sync_host(self):
        self.test_auth.app = FakeApp(iter(NO_CONTENT_RESP * 1),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456',
                     'x-forwarded-for': '127.0.0.1'})
        req.remote_addr = '127.0.0.2'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 204)

        self.test_auth.app = FakeApp(iter(NO_CONTENT_RESP * 1),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456',
                     'x-cluster-client-ip': '127.0.0.1'})
        req.remote_addr = '127.0.0.2'
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 204)

    def test_options_call(self):
        req = self._make_request('/v1/AUTH_cfa/c/o',
                                 environ={'REQUEST_METHOD': 'OPTIONS'})
        resp = self.test_auth.authorize(req)
        self.assertIsNone(resp)

    def test_get_user_group(self):
        # More tests in TestGetUserGroups class
        app = FakeApp()
        ath = auth.filter_factory({})(app)

        ath.users = {'test:tester': {'groups': ['.admin']}}
        groups = ath._get_user_groups('test', 'test:tester', 'AUTH_test')
        self.assertEqual(groups, 'test,test:tester,AUTH_test')

        ath.users = {'test:tester': {'groups': []}}
        groups = ath._get_user_groups('test', 'test:tester', 'AUTH_test')
        self.assertEqual(groups, 'test,test:tester')

    def test_auth_scheme(self):
        req = self._make_request('/v1/BLAH_account',
                                 headers={'X-Auth-Token': 'BLAH_t'})
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)
        self.assertEqual(resp.headers.get('Www-Authenticate'),
                         'Swift realm="BLAH_account"')

    def test_successful_token_unicode_user(self):
        app = FakeApp(iter(NO_CONTENT_RESP * 2))
        conf = {u'user_t\u00e9st_t\u00e9ster': u'p\u00e1ss .admin'}
        if six.PY2:
            conf = {k.encode('utf8'): v.encode('utf8')
                    for k, v in conf.items()}
        ath = auth.filter_factory(conf)(app)
        quoted_acct = quote(u'/v1/AUTH_t\u00e9st'.encode('utf8'))
        memcache = FakeMemcache()

        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': u't\u00e9st:t\u00e9ster',
                     'X-Auth-Key': u'p\u00e1ss'})
        req.environ['swift.cache'] = memcache
        resp = req.get_response(ath)
        self.assertEqual(resp.status_int, 200)
        auth_token = resp.headers['X-Auth-Token']
        self.assertEqual(quoted_acct,
                         urlparse(resp.headers['X-Storage-Url']).path)

        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': u't\u00e9st:t\u00e9ster',
                     'X-Auth-Key': u'p\u00e1ss'})
        req.environ['swift.cache'] = memcache
        resp = req.get_response(ath)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(auth_token, resp.headers['X-Auth-Token'])
        self.assertEqual(quoted_acct,
                         urlparse(resp.headers['X-Storage-Url']).path)

        # storage urls should be url-encoded...
        req = self._make_request(
            quoted_acct, headers={'X-Auth-Token': auth_token})
        req.environ['swift.cache'] = memcache
        resp = req.get_response(ath)
        self.assertEqual(204, resp.status_int)

        # ...but it also works if you send the account raw
        req = self._make_request(
            u'/v1/AUTH_t\u00e9st'.encode('utf8'),
            headers={'X-Auth-Token': auth_token})
        req.environ['swift.cache'] = memcache
        resp = req.get_response(ath)
        self.assertEqual(204, resp.status_int)

    def test_request_method_not_allowed(self):
        test_auth = auth.filter_factory({'user_ac_user': 'testing'})(FakeApp())
        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'ac:user', 'X-Auth-Key': 'testing'},
            environ={'REQUEST_METHOD': 'PUT'})
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 405)

        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'ac:user', 'X-Auth-Key': 'testing'},
            environ={'REQUEST_METHOD': 'HEAD'})
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 405)

        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'ac:user', 'X-Auth-Key': 'testing'},
            environ={'REQUEST_METHOD': 'POST'})
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 405)

        req = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'ac:user', 'X-Auth-Key': 'testing'},
            environ={'REQUEST_METHOD': 'DELETE'})
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 405)


class TestAuthWithMultiplePrefixes(TestAuth):
    """
    Repeats all tests in TestAuth except adds multiple
    reseller_prefix items
    """

    def setUp(self):
        self.test_auth = auth.filter_factory(
            {'reseller_prefix': 'AUTH_, SOMEOTHER_, YETANOTHER_'})(FakeApp())


class TestGetUserGroups(unittest.TestCase):

    def test_custom_url_config(self):
        app = FakeApp()
        ath = auth.filter_factory({
            'user_test_tester':
            'testing .admin http://saio:8080/v1/AUTH_monkey'})(app)
        groups = ath._get_user_groups('test', 'test:tester', 'AUTH_monkey')
        self.assertEqual(groups, 'test,test:tester,AUTH_test,AUTH_monkey')

    def test_no_prefix_reseller(self):
        app = FakeApp()
        ath = auth.filter_factory({'reseller_prefix': ''})(app)

        ath.users = {'test:tester': {'groups': ['.admin']}}
        groups = ath._get_user_groups('test', 'test:tester', 'test')
        self.assertEqual(groups, 'test,test:tester')

        ath.users = {'test:tester': {'groups': []}}
        groups = ath._get_user_groups('test', 'test:tester', 'test')
        self.assertEqual(groups, 'test,test:tester')

    def test_single_reseller(self):
        app = FakeApp()
        ath = auth.filter_factory({})(app)

        ath.users = {'test:tester': {'groups': ['.admin']}}
        groups = ath._get_user_groups('test', 'test:tester', 'AUTH_test')
        self.assertEqual(groups, 'test,test:tester,AUTH_test')

        ath.users = {'test:tester': {'groups': []}}
        groups = ath._get_user_groups('test', 'test:tester', 'AUTH_test')
        self.assertEqual(groups, 'test,test:tester')

    def test_multiple_reseller(self):
        app = FakeApp()
        ath = auth.filter_factory(
            {'reseller_prefix': 'AUTH_, SOMEOTHER_, YETANOTHER_'})(app)
        self.assertEqual(ath.reseller_prefixes, ['AUTH_', 'SOMEOTHER_',
                                                 'YETANOTHER_'])

        ath.users = {'test:tester': {'groups': ['.admin']}}
        groups = ath._get_user_groups('test', 'test:tester', 'AUTH_test')
        self.assertEqual(groups,
                         'test,test:tester,AUTH_test,'
                         'SOMEOTHER_test,YETANOTHER_test')

        ath.users = {'test:tester': {'groups': []}}
        groups = ath._get_user_groups('test', 'test:tester', 'AUTH_test')
        self.assertEqual(groups, 'test,test:tester')


class TestDefinitiveAuth(unittest.TestCase):
    def setUp(self):
        self.test_auth = auth.filter_factory(
            {'reseller_prefix': 'AUTH_, SOMEOTHER_'})(FakeApp())

    def test_noreseller_prefix(self):
        ath = auth.filter_factory({'reseller_prefix': ''})(FakeApp())
        result = ath._is_definitive_auth(path='/v1/test')
        self.assertEqual(result, False)
        result = ath._is_definitive_auth(path='/v1/AUTH_test')
        self.assertEqual(result, False)
        result = ath._is_definitive_auth(path='/v1/BLAH_test')
        self.assertEqual(result, False)

    def test_blank_prefix(self):
        ath = auth.filter_factory({'reseller_prefix':
                                   " '', SOMEOTHER"})(FakeApp())
        result = ath._is_definitive_auth(path='/v1/test')
        self.assertEqual(result, False)
        result = ath._is_definitive_auth(path='/v1/SOMEOTHER_test')
        self.assertEqual(result, True)
        result = ath._is_definitive_auth(path='/v1/SOMEOTHERtest')
        self.assertEqual(result, False)

    def test_default_prefix(self):
        ath = auth.filter_factory({})(FakeApp())
        result = ath._is_definitive_auth(path='/v1/AUTH_test')
        self.assertEqual(result, True)
        result = ath._is_definitive_auth(path='/v1/BLAH_test')
        self.assertEqual(result, False)
        ath = auth.filter_factory({'reseller_prefix': 'AUTH'})(FakeApp())
        result = ath._is_definitive_auth(path='/v1/AUTH_test')
        self.assertEqual(result, True)
        result = ath._is_definitive_auth(path='/v1/BLAH_test')
        self.assertEqual(result, False)

    def test_multiple_prefixes(self):
        ath = auth.filter_factory({'reseller_prefix':
                                   'AUTH, SOMEOTHER'})(FakeApp())
        result = ath._is_definitive_auth(path='/v1/AUTH_test')
        self.assertEqual(result, True)
        result = ath._is_definitive_auth(path='/v1/SOMEOTHER_test')
        self.assertEqual(result, True)
        result = ath._is_definitive_auth(path='/v1/BLAH_test')
        self.assertEqual(result, False)


class TestParseUserCreation(unittest.TestCase):
    def test_parse_user_creation(self):
        auth_filter = auth.filter_factory({
            'reseller_prefix': 'ABC',
            'user_test_tester3': 'testing',
            'user_has_url': 'urlly .admin http://a.b/v1/DEF_has',
            'user_admin_admin': 'admin .admin .reseller_admin',
        })(FakeApp())
        self.assertEqual(auth_filter.users, {
            'admin:admin': {
                'url': '$HOST/v1/ABC_admin',
                'groups': ['.admin', '.reseller_admin'],
                'key': 'admin'
            }, 'test:tester3': {
                'url': '$HOST/v1/ABC_test',
                'groups': [],
                'key': 'testing'
            }, 'has:url': {
                'url': 'http://a.b/v1/DEF_has',
                'groups': ['.admin'],
                'key': 'urlly'
            },
        })

    def test_base64_encoding(self):
        auth_filter = auth.filter_factory({
            'reseller_prefix': 'ABC',
            'user64_%s_%s' % (
                b64encode('test').rstrip('='),
                b64encode('tester3').rstrip('=')):
            'testing .reseller_admin',
            'user64_%s_%s' % (
                b64encode('user_foo').rstrip('='),
                b64encode('ab').rstrip('=')):
            'urlly .admin http://a.b/v1/DEF_has',
        })(FakeApp())
        self.assertEqual(auth_filter.users, {
            'test:tester3': {
                'url': '$HOST/v1/ABC_test',
                'groups': ['.reseller_admin'],
                'key': 'testing'
            }, 'user_foo:ab': {
                'url': 'http://a.b/v1/DEF_has',
                'groups': ['.admin'],
                'key': 'urlly'
            },
        })

    def test_key_with_no_value(self):
        self.assertRaises(ValueError, auth.filter_factory({
            'user_test_tester3': 'testing',
            'user_bob_bobby': '',
            'user_admin_admin': 'admin .admin .reseller_admin',
        }), FakeApp())


class TestAccountAcls(unittest.TestCase):
    """
    These tests use a single reseller prefix (AUTH_) and the
    target paths are /v1/AUTH_<blah>
    """

    def setUp(self):
        self.reseller_prefix = {}
        self.accpre = 'AUTH'

    def _make_request(self, path, **kwargs):
        # Our TestAccountAcls default request will have a valid auth token
        version, acct, _ = split_path(path, 1, 3, True)
        headers = kwargs.pop('headers', {'X-Auth-Token': 'AUTH_t'})
        user_groups = kwargs.pop('user_groups', 'AUTH_firstacct')

        # The account being accessed will have account ACLs
        acl = {'admin': ['AUTH_admin'], 'read-write': ['AUTH_rw'],
               'read-only': ['AUTH_ro']}
        header_data = {'core-access-control':
                       format_acl(version=2, acl_dict=acl)}
        acls = kwargs.pop('acls', header_data)

        req = Request.blank(path, headers=headers, **kwargs)

        # Authorize the token by populating the request's cache
        req.environ['swift.cache'] = FakeMemcache()
        cache_key = 'AUTH_/token/AUTH_t'
        cache_entry = (time() + 3600, user_groups)
        req.environ['swift.cache'].set(cache_key, cache_entry)

        # Pretend get_account_info returned ACLs in sysmeta, and we cached that
        cache_key = 'account/%s' % acct
        cache_entry = {'sysmeta': acls}
        req.environ['swift.cache'].set(cache_key, cache_entry)

        return req

    def _conf(self, moreconf):
        conf = self.reseller_prefix
        conf.update(moreconf)
        return conf

    def test_account_acl_success(self):
        test_auth = auth.filter_factory(
            self._conf({'user_admin_user': 'testing'}))(
                FakeApp(iter(NO_CONTENT_RESP * 1)))

        # admin (not a swift admin) wants to read from otheracct
        req = self._make_request('/v1/%s_otheract' % self.accpre,
                                 user_groups="AUTH_admin")

        # The request returned by _make_request should be allowed
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 204)

    def test_account_acl_failures(self):
        test_auth = auth.filter_factory(
            self._conf({'user_admin_user': 'testing'}))(
                FakeApp())

        # If I'm not authed as anyone on the ACLs, I shouldn't get in
        req = self._make_request('/v1/%s_otheract' % self.accpre,
                                 user_groups="AUTH_bob")
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 403)

        # If the target account has no ACLs, a non-owner shouldn't get in
        req = self._make_request('/v1/%s_otheract' % self.accpre,
                                 user_groups="AUTH_admin",
                                 acls={})
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 403)

    def test_admin_privileges(self):
        test_auth = auth.filter_factory(
            self._conf({'user_admin_user': 'testing'}))(
                FakeApp(iter(NO_CONTENT_RESP * 18)))

        for target in (
                '/v1/%s_otheracct' % self.accpre,
                '/v1/%s_otheracct/container' % self.accpre,
                '/v1/%s_otheracct/container/obj' % self.accpre):
            for method in ('GET', 'HEAD', 'OPTIONS', 'PUT', 'POST', 'DELETE'):
                # Admin ACL user can do anything
                req = self._make_request(target, user_groups="AUTH_admin",
                                         environ={'REQUEST_METHOD': method})
                resp = req.get_response(test_auth)
                self.assertEqual(resp.status_int, 204)

                # swift_owner should be set to True
                if method != 'OPTIONS':
                    self.assertTrue(req.environ.get('swift_owner'))

    def test_readwrite_privileges(self):
        test_auth = auth.filter_factory(
            self._conf({'user_rw_user': 'testing'}))(
                FakeApp(iter(NO_CONTENT_RESP * 15)))

        for target in ('/v1/%s_otheracct' % self.accpre,):
            for method in ('GET', 'HEAD', 'OPTIONS'):
                # Read-Write user can read account data
                req = self._make_request(target, user_groups="AUTH_rw",
                                         environ={'REQUEST_METHOD': method})
                resp = req.get_response(test_auth)
                self.assertEqual(resp.status_int, 204)

                # swift_owner should NOT be set to True
                self.assertFalse(req.environ.get('swift_owner'))

            # RW user should NOT be able to PUT, POST, or DELETE to the account
            for method in ('PUT', 'POST', 'DELETE'):
                req = self._make_request(target, user_groups="AUTH_rw",
                                         environ={'REQUEST_METHOD': method})
                resp = req.get_response(test_auth)
                self.assertEqual(resp.status_int, 403)

        # RW user should be able to GET, PUT, POST, or DELETE to containers
        # and objects
        for target in ('/v1/%s_otheracct/c' % self.accpre,
                       '/v1/%s_otheracct/c/o' % self.accpre):
            for method in ('GET', 'HEAD', 'OPTIONS', 'PUT', 'POST', 'DELETE'):
                req = self._make_request(target, user_groups="AUTH_rw",
                                         environ={'REQUEST_METHOD': method})
                resp = req.get_response(test_auth)
                self.assertEqual(resp.status_int, 204)

    def test_readonly_privileges(self):
        test_auth = auth.filter_factory(
            self._conf({'user_ro_user': 'testing'}))(
                FakeApp(iter(NO_CONTENT_RESP * 9)))

        # ReadOnly user should NOT be able to PUT, POST, or DELETE to account,
        # container, or object
        for target in ('/v1/%s_otheracct' % self.accpre,
                       '/v1/%s_otheracct/cont' % self.accpre,
                       '/v1/%s_otheracct/cont/obj' % self.accpre):
            for method in ('GET', 'HEAD', 'OPTIONS'):
                req = self._make_request(target, user_groups="AUTH_ro",
                                         environ={'REQUEST_METHOD': method})
                resp = req.get_response(test_auth)
                self.assertEqual(resp.status_int, 204)
                # swift_owner should NOT be set to True for the ReadOnly ACL
                self.assertFalse(req.environ.get('swift_owner'))
            for method in ('PUT', 'POST', 'DELETE'):
                req = self._make_request(target, user_groups="AUTH_ro",
                                         environ={'REQUEST_METHOD': method})
                resp = req.get_response(test_auth)
                self.assertEqual(resp.status_int, 403)
                # swift_owner should NOT be set to True for the ReadOnly ACL
                self.assertFalse(req.environ.get('swift_owner'))

    def test_user_gets_best_acl(self):
        test_auth = auth.filter_factory(
            self._conf({'user_acct_username': 'testing'}))(
                FakeApp(iter(NO_CONTENT_RESP * 18)))

        mygroups = "AUTH_acct,AUTH_ro,AUTH_something,AUTH_admin"
        for target in ('/v1/%s_otheracct' % self.accpre,
                       '/v1/%s_otheracct/container' % self.accpre,
                       '/v1/%s_otheracct/container/obj' % self.accpre):
            for method in ('GET', 'HEAD', 'OPTIONS', 'PUT', 'POST', 'DELETE'):
                # Admin ACL user can do anything
                req = self._make_request(target, user_groups=mygroups,
                                         environ={'REQUEST_METHOD': method})
                resp = req.get_response(test_auth)
                self.assertEqual(
                    resp.status_int, 204, "%s (%s) - expected 204, got %d" %
                    (target, method, resp.status_int))

                # swift_owner should be set to True
                if method != 'OPTIONS':
                    self.assertTrue(req.environ.get('swift_owner'))

    def test_acl_syntax_verification(self):
        test_auth = auth.filter_factory(
            self._conf({'user_admin_user': 'testing .admin'}))(
                FakeApp(iter(NO_CONTENT_RESP * 5)))
        user_groups = test_auth._get_user_groups('admin', 'admin:user',
                                                 'AUTH_admin')
        good_headers = {'X-Auth-Token': 'AUTH_t'}
        good_acl = json.dumps({"read-only": [u"á", "b"]})
        bad_list_types = '{"read-only": ["a", 99]}'
        bad_acl = 'syntactically invalid acl -- this does not parse as JSON'
        wrong_acl = '{"other-auth-system":["valid","json","but","wrong"]}'
        bad_value_acl = '{"read-write":["fine"],"admin":"should be a list"}'
        not_dict_acl = '["read-only"]'
        not_dict_acl2 = 1
        empty_acls = ['{}', '', '{ }']
        target = '/v1/%s_firstacct' % self.accpre

        # no acls -- no problem!
        req = self._make_request(target, headers=good_headers,
                                 user_groups=user_groups)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 204)

        # syntactically valid acls should go through
        update = {'x-account-access-control': good_acl}
        req = self._make_request(target, user_groups=user_groups,
                                 headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 204,
                         'Expected 204, got %s, response body: %s'
                         % (resp.status_int, resp.body))

        # syntactically valid empty acls should go through
        for acl in empty_acls:
            update = {'x-account-access-control': acl}
            req = self._make_request(target, user_groups=user_groups,
                                     headers=dict(good_headers, **update))
            resp = req.get_response(test_auth)
            self.assertEqual(resp.status_int, 204)

        errmsg = b'X-Account-Access-Control invalid: %s'
        # syntactically invalid acls get a 400
        update = {'x-account-access-control': bad_acl}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual(errmsg % b"Syntax error", resp.body[:46])

        # syntactically valid acls with bad keys also get a 400
        update = {'x-account-access-control': wrong_acl}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 400)
        self.assertTrue(resp.body.startswith(
            errmsg % b'Key "other-auth-system" not recognized'), resp.body)

        # and do something sane with crazy data
        update = {'x-account-access-control': u'{"\u1234": []}'.encode('utf8')}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 400)
        self.assertTrue(resp.body.startswith(
            errmsg % b'Key "\\u1234" not recognized'), resp.body)

        # acls with good keys but bad values also get a 400
        update = {'x-account-access-control': bad_value_acl}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 400)
        self.assertTrue(resp.body.startswith(
            errmsg % b'Value for key "admin" must be a list'), resp.body)

        # acls with non-string-types in list also get a 400
        update = {'x-account-access-control': bad_list_types}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 400)
        self.assertTrue(resp.body.startswith(
            errmsg % b'Elements of "read-only" list must be strings'),
            resp.body)

        # acls with wrong json structure also get a 400
        update = {'x-account-access-control': not_dict_acl}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual(errmsg % b"Syntax error", resp.body[:46])

        # acls with wrong json structure also get a 400
        update = {'x-account-access-control': not_dict_acl2}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual(errmsg % b"Syntax error", resp.body[:46])

    def test_acls_propagate_to_sysmeta(self):
        test_auth = auth.filter_factory({'user_admin_user': 'testing'})(
            FakeApp(iter(NO_CONTENT_RESP * 3)))

        sysmeta_hdr = 'x-account-sysmeta-core-access-control'
        target = '/v1/AUTH_firstacct'
        good_headers = {'X-Auth-Token': 'AUTH_t'}
        good_acl = '{"read-only":["a","b"]}'

        # no acls -- no problem!
        req = self._make_request(target, headers=good_headers)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 204)
        self.assertIsNone(req.headers.get(sysmeta_hdr))

        # syntactically valid acls should go through
        update = {'x-account-access-control': good_acl}
        req = self._make_request(target, headers=dict(good_headers, **update))
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 204)
        self.assertEqual(good_acl, req.headers.get(sysmeta_hdr))

    def test_bad_acls_get_denied(self):
        test_auth = auth.filter_factory({'user_admin_user': 'testing'})(
            FakeApp(iter(NO_CONTENT_RESP * 3)))

        target = '/v1/AUTH_firstacct'
        good_headers = {'X-Auth-Token': 'AUTH_t'}
        bad_acls = (
            'syntax error',
            '{"bad_key":"should_fail"}',
            '{"admin":"not a list, should fail"}',
            '{"admin":["valid"],"read-write":"not a list, should fail"}',
        )

        for bad_acl in bad_acls:
            hdrs = dict(good_headers, **{'x-account-access-control': bad_acl})
            req = self._make_request(target, headers=hdrs)
            resp = req.get_response(test_auth)
            self.assertEqual(resp.status_int, 400)


class TestAuthMultiplePrefixes(TestAccountAcls):
    """
    These tests repeat the same tests as TestAccountACLs,
    but use multiple reseller prefix items (AUTH_ and SOMEOTHER_).
    The target paths are /v1/SOMEOTHER_<blah>
    """

    def setUp(self):
        self.reseller_prefix = {'reseller_prefix': 'AUTH_, SOMEOTHER_'}
        self.accpre = 'SOMEOTHER'


class PrefixAccount(unittest.TestCase):

    def test_default(self):
        conf = {}
        test_auth = auth.filter_factory(conf)(FakeApp())
        self.assertEqual(test_auth._get_account_prefix(
                         'AUTH_1234'), 'AUTH_')
        self.assertIsNone(test_auth._get_account_prefix('JUNK_1234'))

    def test_same_as_default(self):
        conf = {'reseller_prefix': 'AUTH'}
        test_auth = auth.filter_factory(conf)(FakeApp())
        self.assertEqual(test_auth._get_account_prefix(
                         'AUTH_1234'), 'AUTH_')
        self.assertIsNone(test_auth._get_account_prefix('JUNK_1234'))

    def test_blank_reseller(self):
        conf = {'reseller_prefix': ''}
        test_auth = auth.filter_factory(conf)(FakeApp())
        self.assertEqual(test_auth._get_account_prefix(
                         '1234'), '')
        self.assertEqual(test_auth._get_account_prefix(
                         'JUNK_1234'), '')  # yes, it should return ''

    def test_multiple_resellers(self):
        conf = {'reseller_prefix': 'AUTH, PRE2'}
        test_auth = auth.filter_factory(conf)(FakeApp())
        self.assertEqual(test_auth._get_account_prefix(
                         'AUTH_1234'), 'AUTH_')
        self.assertIsNone(test_auth._get_account_prefix('JUNK_1234'))


class ServiceTokenFunctionality(unittest.TestCase):

    def _make_authed_request(self, conf, remote_user, path, method='GET'):
        """Make a request with tempauth as auth

        Acts as though the user had presented a token
        granting groups as described in remote_user.
        If remote_user contains the .service group, it emulates presenting
        X-Service-Token containing a .service group.

        :param conf: configuration for tempauth
        :param remote_user: the groups the user belongs to. Examples:
            acct:joe,acct                         user joe, no .admin
            acct:joe,acct,AUTH_joeacct            user joe, jas .admin group
            acct:joe,acct,AUTH_joeacct,.service   adds .service group
        :param path: the path of the request
        :param method: the method (defaults to GET)

        :returns: response object
        """
        self.req = Request.blank(path)
        self.req.method = method
        self.req.remote_user = remote_user
        fake_app = FakeApp(iter([('200 OK', {}, '')]))
        test_auth = auth.filter_factory(conf)(fake_app)
        resp = self.req.get_response(test_auth)
        return resp

    def test_authed_for_path_single(self):
        resp = self._make_authed_request({}, 'acct:joe,acct,AUTH_acct',
                                             '/v1/AUTH_acct')
        self.assertEqual(resp.status_int, 200)
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH'}, 'acct:joe,acct,AUTH_acct',
                                         '/v1/AUTH_acct/c', method='PUT')
        self.assertEqual(resp.status_int, 200)
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH'},
            'admin:mary,admin,AUTH_admin,.reseller_admin',
            '/v1/AUTH_acct', method='GET')
        self.assertEqual(resp.status_int, 200)
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH'},
            'admin:mary,admin,AUTH_admin,.reseller_admin',
            '/v1/AUTH_acct', method='DELETE')
        self.assertEqual(resp.status_int, 200)

    def test_denied_for_path_single(self):
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH'},
            'fredacc:fred,fredacct,AUTH_fredacc',
            '/v1/AUTH_acct')
        self.assertEqual(resp.status_int, 403)
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH'},
            'acct:joe,acct',
            '/v1/AUTH_acct',
            method='PUT')
        self.assertEqual(resp.status_int, 403)
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH'},
            'acct:joe,acct,AUTH_acct',
            '/v1/AUTH_acct',
            method='DELETE')
        self.assertEqual(resp.status_int, 403)

    def test_authed_for_primary_path_multiple(self):
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2'},
            'acct:joe,acct,AUTH_acct,PRE2_acct',
            '/v1/PRE2_acct')
        self.assertEqual(resp.status_int, 200)

    def test_denied_for_second_path_with_only_operator_role(self):
        # User only presents a token in X-Auth-Token (or in X-Service-Token)
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'acct:joe,acct,AUTH_acct,PRE2_acct',
            '/v1/PRE2_acct')
        self.assertEqual(resp.status_int, 403)

        # User puts token in both X-Auth-Token and X-Service-Token
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'acct:joe,acct,AUTH_acct,PRE2_acct,AUTH_acct,PRE2_acct',
            '/v1/PRE2_acct')
        self.assertEqual(resp.status_int, 403)

    def test_authed_for_second_path_with_operator_role_and_service(self):
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'acct:joe,acct,AUTH_acct,PRE2_acct,'
            'admin:mary,admin,AUTH_admin,PRE2_admin,.service',
            '/v1/PRE2_acct')
        self.assertEqual(resp.status_int, 200)

    def test_denied_for_second_path_with_only_service(self):
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'admin:mary,admin,AUTH_admin,PRE2_admin,.service',
            '/v1/PRE2_acct')
        self.assertEqual(resp.status_int, 403)

    def test_denied_for_second_path_for_service_user(self):
        # User presents token with 'service' role in X-Auth-Token
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'admin:mary,admin,AUTH_admin,PRE2_admin,.service',
            '/v1/PRE2_acct')
        self.assertEqual(resp.status_int, 403)

        # User presents token with 'service' role in X-Auth-Token
        # and also in X-Service-Token
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'admin:mary,admin,AUTH_admin,PRE2_admin,.service,'
            'admin:mary,admin,AUTH_admin,PRE2_admin,.service',
            '/v1/PRE2_acct')
        self.assertEqual(resp.status_int, 403)

    def test_delete_denied_for_second_path(self):
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'acct:joe,acct,AUTH_acct,PRE2_acct,'
            'admin:mary,admin,AUTH_admin,PRE2_admin,.service',
            '/v1/PRE2_acct',
            method='DELETE')
        self.assertEqual(resp.status_int, 403)

    def test_delete_of_second_path_by_reseller_admin(self):
        resp = self._make_authed_request(
            {'reseller_prefix': 'AUTH, PRE2',
             'PRE2_require_group': '.service'},
            'acct:joe,acct,AUTH_acct,PRE2_acct,'
            'admin:mary,admin,AUTH_admin,PRE2_admin,.reseller_admin',
            '/v1/PRE2_acct',
            method='DELETE')
        self.assertEqual(resp.status_int, 200)


class TestTokenHandling(unittest.TestCase):

    def _make_request(self, conf, path, headers, method='GET'):
        """Make a request with tempauth as auth

        It sets up AUTH_t and AUTH_s as tokens in memcache, where "joe"
        has .admin role on /v1/AUTH_acct and user "glance" has .service
        role on /v1/AUTH_admin.

        :param conf: configuration for tempauth
        :param path: the path of the request
        :param headers: allows you to pass X-Auth-Token, etc.
        :param method: the method (defaults to GET)

        :returns: response object
        """
        fake_app = FakeApp(iter([('200 OK', {}, '')]))
        self.test_auth = auth.filter_factory(conf)(fake_app)
        self.req = Request.blank(path, headers=headers)
        self.req.method = method
        self.req.environ['swift.cache'] = FakeMemcache()
        self._setup_user_and_token('AUTH_t', 'acct', 'acct:joe',
                                   '.admin')
        self._setup_user_and_token('AUTH_s', 'admin', 'admin:glance',
                                   '.service')
        resp = self.req.get_response(self.test_auth)
        return resp

    def _setup_user_and_token(self, token_name, account, account_user,
                              groups):
        """Setup named token in memcache

        :param token_name: name of token
        :param account: example: acct
        :param account_user: example: acct_joe
        :param groups: example: .admin
        """
        self.test_auth.users[account_user] = dict(groups=[groups])
        account_id = 'AUTH_%s' % account
        cache_key = 'AUTH_/token/%s' % token_name
        cache_entry = (time() + 3600,
                       self.test_auth._get_user_groups(account,
                                                       account_user,
                                                       account_id))
        self.req.environ['swift.cache'].set(cache_key, cache_entry)

    def test_tokens_set_remote_user(self):
        conf = {}  # Default conf
        resp = self._make_request(conf, '/v1/AUTH_acct',
                                  {'x-auth-token': 'AUTH_t'})
        self.assertEqual(self.req.environ['REMOTE_USER'],
                         'acct,acct:joe,AUTH_acct')
        self.assertEqual(resp.status_int, 200)
        # Add x-service-token
        resp = self._make_request(conf, '/v1/AUTH_acct',
                                  {'x-auth-token': 'AUTH_t',
                                   'x-service-token': 'AUTH_s'})
        self.assertEqual(self.req.environ['REMOTE_USER'],
                         'acct,acct:joe,AUTH_acct,admin,admin:glance,.service')
        self.assertEqual(resp.status_int, 200)
        # Put x-auth-token value into x-service-token
        resp = self._make_request(conf, '/v1/AUTH_acct',
                                  {'x-auth-token': 'AUTH_t',
                                   'x-service-token': 'AUTH_t'})
        self.assertEqual(self.req.environ['REMOTE_USER'],
                         'acct,acct:joe,AUTH_acct,acct,acct:joe,AUTH_acct')
        self.assertEqual(resp.status_int, 200)

    def test_service_token_given_and_needed(self):
        conf = {'reseller_prefix': 'AUTH, PRE2',
                'PRE2_require_group': '.service'}
        resp = self._make_request(conf, '/v1/PRE2_acct',
                                  {'x-auth-token': 'AUTH_t',
                                   'x-service-token': 'AUTH_s'})
        self.assertEqual(resp.status_int, 200)

    def test_service_token_omitted(self):
        conf = {'reseller_prefix': 'AUTH, PRE2',
                'PRE2_require_group': '.service'}
        resp = self._make_request(conf, '/v1/PRE2_acct',
                                  {'x-auth-token': 'AUTH_t'})
        self.assertEqual(resp.status_int, 403)

    def test_invalid_tokens(self):
        conf = {'reseller_prefix': 'AUTH, PRE2',
                'PRE2_require_group': '.service'}
        resp = self._make_request(conf, '/v1/PRE2_acct',
                                  {'x-auth-token': 'AUTH_junk'})
        self.assertEqual(resp.status_int, 401)
        resp = self._make_request(conf, '/v1/PRE2_acct',
                                  {'x-auth-token': 'AUTH_t',
                                   'x-service-token': 'AUTH_junk'})
        self.assertEqual(resp.status_int, 403)
        resp = self._make_request(conf, '/v1/PRE2_acct',
                                  {'x-auth-token': 'AUTH_junk',
                                   'x-service-token': 'AUTH_s'})
        self.assertEqual(resp.status_int, 401)


class TestUtilityMethods(unittest.TestCase):
    def test_account_acls_bad_path_raises_exception(self):
        auth_inst = auth.filter_factory({})(FakeApp())
        req = Request({'PATH_INFO': '/'})
        self.assertRaises(ValueError, auth_inst.account_acls, req)

if __name__ == '__main__':
    unittest.main()
