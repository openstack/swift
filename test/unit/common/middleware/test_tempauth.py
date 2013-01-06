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

import unittest
from contextlib import contextmanager
from base64 import b64encode

from swift.common.middleware import tempauth as auth
from swift.common.swob import Request, Response


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

    def __init__(self, status_headers_body_iter=None, acl=None, sync_key=None):
        self.calls = 0
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter([('404 Not Found', {}, '')])
        self.acl = acl
        self.sync_key = sync_key

    def __call__(self, env, start_response):
        self.calls += 1
        self.request = Request.blank('', environ=env)
        if self.acl:
            self.request.acl = self.acl
        if self.sync_key:
            self.request.environ['swift_sync_key'] = self.sync_key
        if 'swift.authorize' in env:
            resp = env['swift.authorize'](self.request)
            if resp:
                return resp(env, start_response)
        status, headers, body = self.status_headers_body_iter.next()
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
            self.status_headers_body_iter.next()
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
        self.assertEquals(ath.reseller_prefix, 'AUTH_')
        ath = auth.filter_factory({'reseller_prefix': 'TEST'})(app)
        self.assertEquals(ath.reseller_prefix, 'TEST_')
        ath = auth.filter_factory({'reseller_prefix': 'TEST_'})(app)
        self.assertEquals(ath.reseller_prefix, 'TEST_')

    def test_auth_prefix_init(self):
        app = FakeApp()
        ath = auth.filter_factory({})(app)
        self.assertEquals(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'auth_prefix': ''})(app)
        self.assertEquals(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'auth_prefix': '/'})(app)
        self.assertEquals(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'auth_prefix': '/test/'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'auth_prefix': '/test'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'auth_prefix': 'test/'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'auth_prefix': 'test'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')

    def test_top_level_deny(self):
        req = self._make_request('/')
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(req.environ['swift.authorize'],
                          self.test_auth.denied_response)

    def test_anon(self):
        req = self._make_request('/v1/AUTH_account')
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(req.environ['swift.authorize'],
                          self.test_auth.authorize)

    def test_override_asked_for_but_not_allowed(self):
        self.test_auth = \
            auth.filter_factory({'allow_overrides': 'false'})(FakeApp())
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(req.environ['swift.authorize'],
                          self.test_auth.authorize)

    def test_override_asked_for_and_allowed(self):
        self.test_auth = \
            auth.filter_factory({'allow_overrides': 'true'})(FakeApp())
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('swift.authorize' not in req.environ)

    def test_override_default_allowed(self):
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertTrue('swift.authorize' not in req.environ)

    def test_auth_deny_non_reseller_prefix(self):
        req = self._make_request('/v1/BLAH_account',
                                 headers={'X-Auth-Token': 'BLAH_t'})
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(req.environ['swift.authorize'],
                          self.test_auth.denied_response)

    def test_auth_deny_non_reseller_prefix_no_override(self):
        fake_authorize = lambda x: Response(status='500 Fake')
        req = self._make_request('/v1/BLAH_account',
                                 headers={'X-Auth-Token': 'BLAH_t'},
                                 environ={'swift.authorize': fake_authorize}
                                 )
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(req.environ['swift.authorize'], fake_authorize)

    def test_auth_no_reseller_prefix_deny(self):
        # Ensures that when we have no reseller prefix, we don't deny a request
        # outright but set up a denial swift.authorize and pass the request on
        # down the chain.
        local_app = FakeApp()
        local_auth = auth.filter_factory({'reseller_prefix': ''})(local_app)
        req = self._make_request('/v1/account',
                                 headers={'X-Auth-Token': 't'})
        resp = req.get_response(local_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(local_app.calls, 1)
        self.assertEquals(req.environ['swift.authorize'],
                          local_auth.denied_response)

    def test_auth_no_reseller_prefix_no_token(self):
        # Check that normally we set up a call back to our authorize.
        local_auth = \
            auth.filter_factory({'reseller_prefix': ''})(FakeApp(iter([])))
        req = self._make_request('/v1/account')
        resp = req.get_response(local_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(req.environ['swift.authorize'],
                          local_auth.authorize)
        # Now make sure we don't override an existing swift.authorize when we
        # have no reseller prefix.
        local_auth = \
            auth.filter_factory({'reseller_prefix': ''})(FakeApp())
        local_authorize = lambda req: Response('test')
        req = self._make_request('/v1/account', environ={'swift.authorize':
                                 local_authorize})
        resp = req.get_response(local_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(req.environ['swift.authorize'], local_authorize)

    def test_auth_fail(self):
        resp = self._make_request(
            '/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_authorize_bad_path(self):
        req = self._make_request('/badpath')
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 401)
        req = self._make_request('/badpath')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_authorize_account_access(self):
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_authorize_acl_group_access(self):
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act2'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr2'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_deny_cross_reseller(self):
        # Tests that cross-reseller is denied, even if ACLs/group names match
        req = self._make_request('/v1/OTHER_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        req.acl = 'act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_authorize_acl_referrer_access(self):
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*,.rlistings'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*'  # No listings allowed
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:.example.com,.rlistings'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.remote_user = 'act:usr,act'
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com,.rlistings'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = self._make_request('/v1/AUTH_cfa/c')
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 401)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.acl = '.r:*,.rlistings'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.acl = '.r:*'  # No listings allowed
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 401)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.acl = '.r:.example.com,.rlistings'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 401)
        req = self._make_request('/v1/AUTH_cfa/c')
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com,.rlistings'
        self.assertEquals(self.test_auth.authorize(req), None)

    def test_account_put_permissions(self):
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        # Even PUTs to your own account as account admin should fail
        req = self._make_request('/v1/AUTH_old',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_account_delete_permissions(self):
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        # Even DELETEs to your own account as account admin should fail
        req = self._make_request('/v1/AUTH_old',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = self._make_request('/v1/AUTH_new',
                                 environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_get_token_fail(self):
        resp = self._make_request('/auth/v1.0').get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        resp = self._make_request(
            '/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_get_token_fail_invalid_x_auth_user_format(self):
        resp = self._make_request(
            '/auth/v1/act/auth',
            headers={'X-Auth-User': 'usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_get_token_fail_non_matching_account_in_request(self):
        resp = self._make_request(
            '/auth/v1/act/auth',
            headers={'X-Auth-User': 'act2:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_get_token_fail_bad_path(self):
        resp = self._make_request(
            '/auth/v1/act/auth/invalid',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_get_token_fail_missing_key(self):
        resp = self._make_request(
            '/auth/v1/act/auth',
            headers={'X-Auth-User': 'act:usr'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

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
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['x-storage-url'],
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
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['x-storage-url'],
                          'http://somehost:5678/v1/AUTH_test')

    def test_storage_url_overriden_scheme(self):
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
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers['x-storage-url'],
                          'fake://somehost:5678/v1/AUTH_test')

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
        self.assertEquals(owner_values, [True])

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
        self.assertEquals(owner_values, [True])

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
        self.assertEquals(owner_values, [False])

    def test_sync_request_success(self):
        self.test_auth.app = FakeApp(iter([('204 No Content', {}, '')]),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)

    def test_sync_request_fail_key(self):
        self.test_auth.app = FakeApp(iter([('204 No Content', {}, '')]),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'wrongsecret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

        self.test_auth.app = FakeApp(iter([('204 No Content', {}, '')]),
                                     sync_key='othersecret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

        self.test_auth.app = FakeApp(iter([('204 No Content', {}, '')]),
                                     sync_key=None)
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_sync_request_fail_no_timestamp(self):
        self.test_auth.app = FakeApp(iter([('204 No Content', {}, '')]),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret'})
        req.remote_addr = '127.0.0.1'
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_sync_request_success_lb_sync_host(self):
        self.test_auth.app = FakeApp(iter([('204 No Content', {}, '')]),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456',
                     'x-forwarded-for': '127.0.0.1'})
        req.remote_addr = '127.0.0.2'
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)

        self.test_auth.app = FakeApp(iter([('204 No Content', {}, '')]),
                                     sync_key='secret')
        req = self._make_request(
            '/v1/AUTH_cfa/c/o',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'x-container-sync-key': 'secret',
                     'x-timestamp': '123.456',
                     'x-cluster-client-ip': '127.0.0.1'})
        req.remote_addr = '127.0.0.2'
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)

    def test_options_call(self):
        req = self._make_request('/v1/AUTH_cfa/c/o',
                                 environ={'REQUEST_METHOD': 'OPTIONS'})
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)


class TestParseUserCreation(unittest.TestCase):
    def test_parse_user_creation(self):
        auth_filter = auth.filter_factory({
            'reseller_prefix': 'ABC',
            'user_test_tester3': 'testing',
            'user_has_url': 'urlly .admin http://a.b/v1/DEF_has',
            'user_admin_admin': 'admin .admin .reseller_admin',
        })(FakeApp())
        self.assertEquals(auth_filter.users, {
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
        self.assertEquals(auth_filter.users, {
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


if __name__ == '__main__':
    unittest.main()
