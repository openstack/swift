# Copyright (c) 2010 OpenStack, LLC.
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

try:
    import simplejson as json
except ImportError:
    import json
import unittest
from contextlib import contextmanager
from time import time

from webob import Request, Response

from swift.common.middleware import swauth as auth


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
        self.calls = 0
        self.status_headers_body_iter = status_headers_body_iter
        if not self.status_headers_body_iter:
            self.status_headers_body_iter = iter([('404 Not Found', {}, '')])

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
        self.test_auth = \
            auth.filter_factory({'super_admin_key': 'supertest'})(FakeApp())

    def test_super_admin_key_required(self):
        app = FakeApp()
        exc = None
        try:
            auth.filter_factory({})(app)
        except ValueError, err:
            exc = err
        self.assertEquals(str(exc),
                          'No super_admin_key set in conf file! Exiting.')
        auth.filter_factory({'super_admin_key': 'supertest'})(app)

    def test_reseller_prefix_init(self):
        app = FakeApp()
        ath = auth.filter_factory({'super_admin_key': 'supertest'})(app)
        self.assertEquals(ath.reseller_prefix, 'AUTH_')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
                                   'reseller_prefix': 'TEST'})(app)
        self.assertEquals(ath.reseller_prefix, 'TEST_')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
                                   'reseller_prefix': 'TEST_'})(app)
        self.assertEquals(ath.reseller_prefix, 'TEST_')

    def test_auth_prefix_init(self):
        app = FakeApp()
        ath = auth.filter_factory({'super_admin_key': 'supertest'})(app)
        self.assertEquals(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
                                   'auth_prefix': ''})(app)
        self.assertEquals(ath.auth_prefix, '/auth/')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
                                   'auth_prefix': '/test/'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
                                   'auth_prefix': '/test'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
                                   'auth_prefix': 'test/'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
                                   'auth_prefix': 'test'})(app)
        self.assertEquals(ath.auth_prefix, '/test/')

    def test_default_swift_cluster_init(self):
        app = FakeApp()
        self.assertRaises(Exception, auth.filter_factory({
            'super_admin_key': 'supertest',
            'default_swift_cluster': 'local#badscheme://host/path'}), app)
        ath = auth.filter_factory({'super_admin_key': 'supertest'})(app)
        self.assertEquals(ath.default_swift_cluster,
                          'local#http://127.0.0.1:8080/v1')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster': 'local#http://host/path'})(app)
        self.assertEquals(ath.default_swift_cluster,
                          'local#http://host/path')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster': 'local#https://host/path/'})(app)
        self.assertEquals(ath.dsc_url, 'https://host/path')
        self.assertEquals(ath.dsc_url2, 'https://host/path')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster':
                'local#https://host/path/#http://host2/path2/'})(app)
        self.assertEquals(ath.dsc_url, 'https://host/path')
        self.assertEquals(ath.dsc_url2, 'http://host2/path2')

    def test_top_level_ignore(self):
        resp = Request.blank('/').get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)

    def test_anon(self):
        resp = Request.blank('/v1/AUTH_account').get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(resp.environ['swift.authorize'],
                          self.test_auth.authorize)

    def test_auth_deny_non_reseller_prefix(self):
        resp = Request.blank('/v1/BLAH_account',
            headers={'X-Auth-Token': 'BLAH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(resp.environ['swift.authorize'],
                          self.test_auth.denied_response)

    def test_auth_deny_non_reseller_prefix_no_override(self):
        fake_authorize = lambda x: Response(status='500 Fake')
        resp = Request.blank('/v1/BLAH_account',
            headers={'X-Auth-Token': 'BLAH_t'},
            environ={'swift.authorize': fake_authorize}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(resp.environ['swift.authorize'], fake_authorize)

    def test_auth_no_reseller_prefix_deny(self):
        # Ensures that when we have no reseller prefix, we don't deny a request
        # outright but set up a denial swift.authorize and pass the request on
        # down the chain.
        local_app = FakeApp()
        local_auth = auth.filter_factory({'super_admin_key': 'supertest',
                                          'reseller_prefix': ''})(local_app)
        resp = Request.blank('/v1/account',
            headers={'X-Auth-Token': 't'}).get_response(local_auth)
        self.assertEquals(resp.status_int, 401)
        # one for checking auth, two for request passed along
        self.assertEquals(local_app.calls, 2)
        self.assertEquals(resp.environ['swift.authorize'],
                          local_auth.denied_response)

    def test_auth_no_reseller_prefix_allow(self):
        # Ensures that when we have no reseller prefix, we can still allow
        # access if our auth server accepts requests
        local_app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() + 60})),
            ('204 No Content', {}, '')]))
        local_auth = auth.filter_factory({'super_admin_key': 'supertest',
                                          'reseller_prefix': ''})(local_app)
        resp = Request.blank('/v1/act',
            headers={'X-Auth-Token': 't'}).get_response(local_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(local_app.calls, 2)
        self.assertEquals(resp.environ['swift.authorize'],
                          local_auth.authorize)

    def test_auth_no_reseller_prefix_no_token(self):
        # Check that normally we set up a call back to our authorize.
        local_auth = \
            auth.filter_factory({'super_admin_key': 'supertest',
                                 'reseller_prefix': ''})(FakeApp(iter([])))
        resp = Request.blank('/v1/account').get_response(local_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(resp.environ['swift.authorize'],
                          local_auth.authorize)
        # Now make sure we don't override an existing swift.authorize when we
        # have no reseller prefix.
        local_auth = \
            auth.filter_factory({'super_admin_key': 'supertest',
                                 'reseller_prefix': ''})(FakeApp())
        local_authorize = lambda req: Response('test')
        resp = Request.blank('/v1/account', environ={'swift.authorize':
            local_authorize}).get_response(local_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.environ['swift.authorize'], local_authorize)

    def test_auth_fail(self):
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_auth_success(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() + 60})),
            ('204 No Content', {}, '')]))
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_auth_memcache(self):
        # First run our test without memcache, showing we need to return the
        # token contents twice.
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() + 60})),
            ('204 No Content', {}, ''),
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() + 60})),
            ('204 No Content', {}, '')]))
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 4)
        # Now run our test with memcache, showing we no longer need to return
        # the token contents twice.
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() + 60})),
            ('204 No Content', {}, ''),
            # Don't need a second token object returned if memcache is used
            ('204 No Content', {}, '')]))
        fake_memcache = FakeMemcache()
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'},
            environ={'swift.cache': fake_memcache}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'},
            environ={'swift.cache': fake_memcache}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_auth_just_expired(self):
        self.test_auth.app = FakeApp(iter([
            # Request for token (which will have expired)
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() - 1})),
            # Request to delete token
            ('204 No Content', {}, '')]))
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_middleware_storage_token(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() + 60})),
            ('204 No Content', {}, '')]))
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Storage-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_authorize_bad_path(self):
        req = Request.blank('/badpath')
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 401)
        req = Request.blank('/badpath')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_authorize_account_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_authorize_acl_group_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act2'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr2'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_deny_cross_reseller(self):
        # Tests that cross-reseller is denied, even if ACLs/group names match
        req = Request.blank('/v1/OTHER_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        req.acl = 'act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_authorize_acl_referrer_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:.example.com'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 401)
        req = Request.blank('/v1/AUTH_cfa')
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.acl = '.r:.example.com'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 401)
        req = Request.blank('/v1/AUTH_cfa')
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)

    def test_account_put_permissions(self):
        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        # Even PUTs to your own account as account admin should fail
        req = Request.blank('/v1/AUTH_old', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_account_delete_permissions(self):
        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        # Even DELETEs to your own account as account admin should fail
        req = Request.blank('/v1/AUTH_old',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = Request.blank('/v1/AUTH_new',
                            environ={'REQUEST_METHOD': 'DELETE'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_get_token_fail(self):
        resp = Request.blank('/auth/v1.0').get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_get_token_fail_invalid_key(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]}))]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'invalid'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_token_fail_invalid_x_auth_user_format(self):
        resp = Request.blank('/auth/v1/act/auth',
            headers={'X-Auth-User': 'usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_get_token_fail_non_matching_account_in_request(self):
        resp = Request.blank('/auth/v1/act/auth',
            headers={'X-Auth-User': 'act2:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_get_token_fail_bad_path(self):
        resp = Request.blank('/auth/v1/act/auth/invalid',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_get_token_fail_missing_key(self):
        resp = Request.blank('/auth/v1/act/auth',
            headers={'X-Auth-User': 'act:usr'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_get_token_fail_get_user_details(self):
        self.test_auth.app = FakeApp(iter([
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_token_fail_get_account(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_get_token_fail_put_new_token(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_get_token_fail_post_to_user(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 4)

    def test_get_token_fail_get_services(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('204 No Content', {}, ''),
            # GET of services object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_get_token_fail_get_existing_token(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tktest'},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of token
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_get_token_success_v1_0(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('204 No Content', {}, ''),
            # GET of services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assert_(resp.headers.get('x-auth-token',
            '').startswith('AUTH_tk'), resp.headers.get('x-auth-token'))
        self.assertEquals(resp.headers.get('x-auth-token'),
                          resp.headers.get('x-storage-token'))
        self.assertEquals(resp.headers.get('x-storage-url'),
                          'http://127.0.0.1:8080/v1/AUTH_cfa')
        self.assertEquals(json.loads(resp.body),
            {"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_get_token_success_v1_act_auth(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('204 No Content', {}, ''),
            # GET of services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v1/act/auth',
            headers={'X-Storage-User': 'usr',
                     'X-Storage-Pass': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assert_(resp.headers.get('x-auth-token',
            '').startswith('AUTH_tk'), resp.headers.get('x-auth-token'))
        self.assertEquals(resp.headers.get('x-auth-token'),
                          resp.headers.get('x-storage-token'))
        self.assertEquals(resp.headers.get('x-storage-url'),
                          'http://127.0.0.1:8080/v1/AUTH_cfa')
        self.assertEquals(json.loads(resp.body),
            {"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_get_token_success_storage_instead_of_auth(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('204 No Content', {}, ''),
            # GET of services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Storage-User': 'act:usr',
                     'X-Storage-Pass': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assert_(resp.headers.get('x-auth-token',
            '').startswith('AUTH_tk'), resp.headers.get('x-auth-token'))
        self.assertEquals(resp.headers.get('x-auth-token'),
                          resp.headers.get('x-storage-token'))
        self.assertEquals(resp.headers.get('x-storage-url'),
                          'http://127.0.0.1:8080/v1/AUTH_cfa')
        self.assertEquals(json.loads(resp.body),
            {"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_get_token_success_v1_act_auth_auth_instead_of_storage(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('204 No Content', {}, ''),
            # GET of services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v1/act/auth',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assert_(resp.headers.get('x-auth-token',
            '').startswith('AUTH_tk'), resp.headers.get('x-auth-token'))
        self.assertEquals(resp.headers.get('x-auth-token'),
                          resp.headers.get('x-storage-token'))
        self.assertEquals(resp.headers.get('x-storage-url'),
                          'http://127.0.0.1:8080/v1/AUTH_cfa')
        self.assertEquals(json.loads(resp.body),
            {"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_get_token_success_existing_token(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tktest'},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of token
            ('200 Ok', {}, json.dumps({"account": "act", "user": "usr",
             "account_id": "AUTH_cfa", "groups": [{'name': "act:usr"},
             {'name': "key"}, {'name': ".admin"}],
             "expires": 9999999999.9999999})),
            # GET of services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.headers.get('x-auth-token'), 'AUTH_tktest')
        self.assertEquals(resp.headers.get('x-auth-token'),
                          resp.headers.get('x-storage-token'))
        self.assertEquals(resp.headers.get('x-storage-url'),
                          'http://127.0.0.1:8080/v1/AUTH_cfa')
        self.assertEquals(json.loads(resp.body),
            {"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_get_token_success_existing_token_expired(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tktest'},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of token
            ('200 Ok', {}, json.dumps({"account": "act", "user": "usr",
             "account_id": "AUTH_cfa", "groups": [{'name': "act:usr"},
             {'name': "key"}, {'name': ".admin"}],
             "expires": 0.0})),
            # DELETE of expired token
            ('204 No Content', {}, ''),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('204 No Content', {}, ''),
            # GET of services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertNotEquals(resp.headers.get('x-auth-token'), 'AUTH_tktest')
        self.assertEquals(resp.headers.get('x-auth-token'),
                          resp.headers.get('x-storage-token'))
        self.assertEquals(resp.headers.get('x-storage-url'),
                          'http://127.0.0.1:8080/v1/AUTH_cfa')
        self.assertEquals(json.loads(resp.body),
            {"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})
        self.assertEquals(self.test_auth.app.calls, 7)

    def test_get_token_success_existing_token_expired_fail_deleting_old(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tktest'},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]})),
            # GET of token
            ('200 Ok', {}, json.dumps({"account": "act", "user": "usr",
             "account_id": "AUTH_cfa", "groups": [{'name': "act:usr"},
             {'name': "key"}, {'name': ".admin"}],
             "expires": 0.0})),
            # DELETE of expired token
            ('503 Service Unavailable', {}, ''),
            # GET of account
            ('204 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of new token
            ('201 Created', {}, ''),
            # POST of token to user object
            ('204 No Content', {}, ''),
            # GET of services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v1.0',
            headers={'X-Auth-User': 'act:usr',
                     'X-Auth-Key': 'key'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertNotEquals(resp.headers.get('x-auth-token'), 'AUTH_tktest')
        self.assertEquals(resp.headers.get('x-auth-token'),
                          resp.headers.get('x-storage-token'))
        self.assertEquals(resp.headers.get('x-storage-url'),
                          'http://127.0.0.1:8080/v1/AUTH_cfa')
        self.assertEquals(json.loads(resp.body),
            {"storage": {"default": "local",
             "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})
        self.assertEquals(self.test_auth.app.calls, 7)

    def test_prep_success(self):
        list_to_iter = [
            # PUT of .auth account
            ('201 Created', {}, ''),
            # PUT of .account_id container
            ('201 Created', {}, '')]
        # PUT of .token* containers
        for x in xrange(16):
            list_to_iter.append(('201 Created', {}, ''))
        self.test_auth.app = FakeApp(iter(list_to_iter))
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 18)

    def test_prep_bad_method(self):
        resp = Request.blank('/auth/v2/.prep',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'HEAD'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_prep_bad_creds(self):
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': 'super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'upertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)

    def test_prep_fail_account_create(self):
        self.test_auth.app = FakeApp(iter([
            # PUT of .auth account
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_prep_fail_token_container_create(self):
        self.test_auth.app = FakeApp(iter([
            # PUT of .auth account
            ('201 Created', {}, ''),
            # PUT of .token container
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_prep_fail_account_id_container_create(self):
        self.test_auth.app = FakeApp(iter([
            # PUT of .auth account
            ('201 Created', {}, ''),
            # PUT of .token container
            ('201 Created', {}, ''),
            # PUT of .account_id container
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/.prep',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_get_reseller_success(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .auth account (list containers)
            ('200 Ok', {}, json.dumps([
                {"name": ".token", "count": 0, "bytes": 0},
                {"name": ".account_id", "count": 0, "bytes": 0},
                {"name": "act", "count": 0, "bytes": 0}])),
            # GET of .auth account (list containers continuation)
            ('200 Ok', {}, '[]')]))
        resp = Request.blank('/auth/v2',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body),
                          {"accounts": [{"name": "act"}]})
        self.assertEquals(self.test_auth.app.calls, 2)

        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"},
             {"name": ".reseller_admin"}], "auth": "plaintext:key"})),
            # GET of .auth account (list containers)
            ('200 Ok', {}, json.dumps([
                {"name": ".token", "count": 0, "bytes": 0},
                {"name": ".account_id", "count": 0, "bytes": 0},
                {"name": "act", "count": 0, "bytes": 0}])),
            # GET of .auth account (list containers continuation)
            ('200 Ok', {}, '[]')]))
        resp = Request.blank('/auth/v2',
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body),
                          {"accounts": [{"name": "act"}]})
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_get_reseller_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2',
            headers={'X-Auth-Admin-User': 'super:admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but not reseller admin)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2',
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2',
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_reseller_fail_listing(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .auth account (list containers)
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of .auth account (list containers)
            ('200 Ok', {}, json.dumps([
                {"name": ".token", "count": 0, "bytes": 0},
                {"name": ".account_id", "count": 0, "bytes": 0},
                {"name": "act", "count": 0, "bytes": 0}])),
            # GET of .auth account (list containers continuation)
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_get_account_success(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # GET of account container (list objects)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"},
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"},
                {"name": "tester3", "hash": "etag", "bytes": 86,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:28.135530"}])),
            # GET of account container (list objects continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body),
            {'account_id': 'AUTH_cfa',
             'services': {'storage':
                            {'default': 'local',
                             'local': 'http://127.0.0.1:8080/v1/AUTH_cfa'}},
             'users': [{'name': 'tester'}, {'name': 'tester3'}]})
        self.assertEquals(self.test_auth.app.calls, 3)

        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"})),
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # GET of account container (list objects)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"},
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"},
                {"name": "tester3", "hash": "etag", "bytes": 86,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:28.135530"}])),
            # GET of account container (list objects continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body),
            {'account_id': 'AUTH_cfa',
             'services': {'storage':
                            {'default': 'local',
                             'local': 'http://127.0.0.1:8080/v1/AUTH_cfa'}},
             'users': [{'name': 'tester'}, {'name': 'tester3'}]})
        self.assertEquals(self.test_auth.app.calls, 4)

    def test_get_account_fail_bad_account_name(self):
        resp = Request.blank('/auth/v2/.token',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)
        resp = Request.blank('/auth/v2/.anything',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_get_account_fail_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': 'super:admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but wrong account)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act2:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': 'act2:adm',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_account_fail_get_services(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_account_fail_listing(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # GET of account container (list objects)
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # GET of account container (list objects)
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 2)

        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # GET of account container (list objects)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"},
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"},
                {"name": "tester3", "hash": "etag", "bytes": 86,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:28.135530"}])),
            # GET of account container (list objects continuation)
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_set_services_new_service(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # PUT of new .services object
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'new_service': {'new_endpoint': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body),
            {'storage': {'default': 'local',
                         'local': 'http://127.0.0.1:8080/v1/AUTH_cfa'},
             'new_service': {'new_endpoint': 'new_value'}})
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_set_services_new_endpoint(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # PUT of new .services object
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'storage': {'new_endpoint': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body),
            {'storage': {'default': 'local',
                         'local': 'http://127.0.0.1:8080/v1/AUTH_cfa',
                         'new_endpoint': 'new_value'}})
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_set_services_update_endpoint(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # PUT of new .services object
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'storage': {'local': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(json.loads(resp.body),
            {'storage': {'default': 'local',
                         'local': 'new_value'}})
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_set_services_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': 'super:admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'storage': {'local': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but not reseller admin)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'},
            body=json.dumps({'storage': {'local': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'},
            body=json.dumps({'storage': {'local': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_set_services_fail_bad_account_name(self):
        resp = Request.blank('/auth/v2/.act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'storage': {'local': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_set_services_fail_bad_json(self):
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body='garbage'
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=''
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_set_services_fail_get_services(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('503 Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'new_service': {'new_endpoint': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'new_service': {'new_endpoint': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_set_services_fail_put_services(self):
        self.test_auth.app = FakeApp(iter([
            # GET of .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # PUT of new .services object
            ('503 Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/.services',
            environ={'REQUEST_METHOD': 'POST'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            body=json.dumps({'new_service': {'new_endpoint': 'new_value'}})
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_put_account_success(self):
        conn = FakeConn(iter([
            # PUT of storage account itself
            ('201 Created', {}, '')]))
        self.test_auth.get_conn = lambda: conn
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('404 Not Found', {}, ''),
            # PUT of account container
            ('204 No Content', {}, ''),
            # PUT of .account_id mapping object
            ('204 No Content', {}, ''),
            # PUT of .services object
            ('204 No Content', {}, ''),
            # POST to account container updating X-Container-Meta-Account-Id
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(self.test_auth.app.calls, 5)
        self.assertEquals(conn.calls, 1)

    def test_put_account_success_preexist_but_not_completed(self):
        conn = FakeConn(iter([
            # PUT of storage account itself
            ('201 Created', {}, '')]))
        self.test_auth.get_conn = lambda: conn
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            # We're going to show it as existing this time, but with no
            # X-Container-Meta-Account-Id, indicating a failed previous attempt
            ('200 Ok', {}, ''),
            # PUT of .account_id mapping object
            ('204 No Content', {}, ''),
            # PUT of .services object
            ('204 No Content', {}, ''),
            # POST to account container updating X-Container-Meta-Account-Id
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(self.test_auth.app.calls, 4)
        self.assertEquals(conn.calls, 1)

    def test_put_account_success_preexist_and_completed(self):
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            # We're going to show it as existing this time, and with an
            # X-Container-Meta-Account-Id, indicating it already exists
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 202)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_put_account_success_with_given_suffix(self):
        conn = FakeConn(iter([
            # PUT of storage account itself
            ('201 Created', {}, '')]))
        self.test_auth.get_conn = lambda: conn
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('404 Not Found', {}, ''),
            # PUT of account container
            ('204 No Content', {}, ''),
            # PUT of .account_id mapping object
            ('204 No Content', {}, ''),
            # PUT of .services object
            ('204 No Content', {}, ''),
            # POST to account container updating X-Container-Meta-Account-Id
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Account-Suffix': 'test-suffix'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(conn.request_path, '/v1/AUTH_test-suffix')
        self.assertEquals(self.test_auth.app.calls, 5)
        self.assertEquals(conn.calls, 1)

    def test_put_account_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': 'super:admin',
                     'X-Auth-Admin-Key': 'supertest'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but not reseller admin)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_put_account_fail_invalid_account_name(self):
        resp = Request.blank('/auth/v2/.act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_put_account_fail_on_initial_account_head(self):
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_put_account_fail_on_account_marker_put(self):
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('404 Not Found', {}, ''),
            # PUT of account container
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_put_account_fail_on_storage_account_put(self):
        conn = FakeConn(iter([
            # PUT of storage account itself
            ('503 Service Unavailable', {}, '')]))
        self.test_auth.get_conn = lambda: conn
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('404 Not Found', {}, ''),
            # PUT of account container
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(conn.calls, 1)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_put_account_fail_on_account_id_mapping(self):
        conn = FakeConn(iter([
            # PUT of storage account itself
            ('201 Created', {}, '')]))
        self.test_auth.get_conn = lambda: conn
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('404 Not Found', {}, ''),
            # PUT of account container
            ('204 No Content', {}, ''),
            # PUT of .account_id mapping object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(conn.calls, 1)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_put_account_fail_on_services_object(self):
        conn = FakeConn(iter([
            # PUT of storage account itself
            ('201 Created', {}, '')]))
        self.test_auth.get_conn = lambda: conn
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('404 Not Found', {}, ''),
            # PUT of account container
            ('204 No Content', {}, ''),
            # PUT of .account_id mapping object
            ('204 No Content', {}, ''),
            # PUT of .services object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(conn.calls, 1)
        self.assertEquals(self.test_auth.app.calls, 4)

    def test_put_account_fail_on_post_mapping(self):
        conn = FakeConn(iter([
            # PUT of storage account itself
            ('201 Created', {}, '')]))
        self.test_auth.get_conn = lambda: conn
        self.test_auth.app = FakeApp(iter([
            # Initial HEAD of account container to check for pre-existence
            ('404 Not Found', {}, ''),
            # PUT of account container
            ('204 No Content', {}, ''),
            # PUT of .account_id mapping object
            ('204 No Content', {}, ''),
            # PUT of .services object
            ('204 No Content', {}, ''),
            # POST to account container updating X-Container-Meta-Account-Id
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'PUT', 'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(conn.calls, 1)
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_delete_account_success(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # DELETE the .services object
            ('204 No Content', {}, ''),
            # DELETE the .account_id mapping object
            ('204 No Content', {}, ''),
            # DELETE the account container
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 6)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_success_missing_services(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('404 Not Found', {}, ''),
            # DELETE the .account_id mapping object
            ('204 No Content', {}, ''),
            # DELETE the account container
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_delete_account_success_missing_storage_account(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('404 Not Found', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # DELETE the .services object
            ('204 No Content', {}, ''),
            # DELETE the .account_id mapping object
            ('204 No Content', {}, ''),
            # DELETE the account container
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 6)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_success_missing_account_id_mapping(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # DELETE the .services object
            ('204 No Content', {}, ''),
            # DELETE the .account_id mapping object
            ('404 Not Found', {}, ''),
            # DELETE the account container
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 6)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_success_missing_account_container_at_end(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # DELETE the .services object
            ('204 No Content', {}, ''),
            # DELETE the .account_id mapping object
            ('204 No Content', {}, ''),
            # DELETE the account container
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 6)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': 'super:admin',
                     'X-Auth-Admin-Key': 'supertest'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but not reseller admin)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_account_fail_invalid_account_name(self):
        resp = Request.blank('/auth/v2/.act',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_delete_account_fail_not_found(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_account_fail_not_found_concurrency(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_delete_account_fail_list_account(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_account_fail_list_account_concurrency(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_delete_account_fail_has_users(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"},
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"}]))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 409)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_account_fail_has_users2(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"}]))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 409)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_delete_account_fail_get_services(self):
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_delete_account_fail_delete_storage_account(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('409 Conflict', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 409)
        self.assertEquals(self.test_auth.app.calls, 3)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_fail_delete_storage_account2(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, ''),
            # DELETE of storage account itself
            ('409 Conflict', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa",
                "other": "http://127.0.0.1:8080/v1/AUTH_cfa2"}}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)
        self.assertEquals(conn.calls, 2)

    def test_delete_account_fail_delete_storage_account3(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('503 Service Unavailable', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_fail_delete_storage_account4(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, ''),
            # DELETE of storage account itself
            ('503 Service Unavailable', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa",
                "other": "http://127.0.0.1:8080/v1/AUTH_cfa2"}}))]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)
        self.assertEquals(conn.calls, 2)

    def test_delete_account_fail_delete_services(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # DELETE the .services object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 4)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_fail_delete_account_id_mapping(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # DELETE the .services object
            ('204 No Content', {}, ''),
            # DELETE the .account_id mapping object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 5)
        self.assertEquals(conn.calls, 1)

    def test_delete_account_fail_delete_account_container(self):
        conn = FakeConn(iter([
            # DELETE of storage account itself
            ('204 No Content', {}, '')]))
        self.test_auth.get_conn = lambda x: conn
        self.test_auth.app = FakeApp(iter([
            # Account's container listing, checking for users
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"}])),
            # Account's container listing, checking for users (continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]'),
            # GET the .services object
            ('200 Ok', {}, json.dumps({"storage": {"default": "local",
                "local": "http://127.0.0.1:8080/v1/AUTH_cfa"}})),
            # DELETE the .services object
            ('204 No Content', {}, ''),
            # DELETE the .account_id mapping object
            ('204 No Content', {}, ''),
            # DELETE the account container
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act',
            environ={'REQUEST_METHOD': 'DELETE',
                     'swift.cache': FakeMemcache()},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 6)
        self.assertEquals(conn.calls, 1)

    def test_get_user_success(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('200 Ok', {}, json.dumps(
                {"groups": [{"name": "act:usr"}, {"name": "act"},
                            {"name": ".admin"}],
                 "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, json.dumps(
            {"groups": [{"name": "act:usr"}, {"name": "act"},
                        {"name": ".admin"}],
             "auth": "plaintext:key"}))
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_user_groups_success(self):
        self.test_auth.app = FakeApp(iter([
            # GET of account container (list objects)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"},
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"},
                {"name": "tester3", "hash": "etag", "bytes": 86,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:28.135530"}])),
            # GET of user object
            ('200 Ok', {}, json.dumps(
                {"groups": [{"name": "act:tester"}, {"name": "act"},
                            {"name": ".admin"}],
                 "auth": "plaintext:key"})),
            # GET of user object
            ('200 Ok', {}, json.dumps(
                {"groups": [{"name": "act:tester3"}, {"name": "act"}],
                 "auth": "plaintext:key3"})),
            # GET of account container (list objects continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]')]))
        resp = Request.blank('/auth/v2/act/.groups',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, json.dumps(
            {"groups": [{"name": ".admin"}, {"name": "act"},
                        {"name": "act:tester"}, {"name": "act:tester3"}]}))
        self.assertEquals(self.test_auth.app.calls, 4)

    def test_get_user_groups_success2(self):
        self.test_auth.app = FakeApp(iter([
            # GET of account container (list objects)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"},
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"}])),
            # GET of user object
            ('200 Ok', {}, json.dumps(
                {"groups": [{"name": "act:tester"}, {"name": "act"},
                            {"name": ".admin"}],
                 "auth": "plaintext:key"})),
            # GET of account container (list objects continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": "tester3", "hash": "etag", "bytes": 86,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:28.135530"}])),
            # GET of user object
            ('200 Ok', {}, json.dumps(
                {"groups": [{"name": "act:tester3"}, {"name": "act"}],
                 "auth": "plaintext:key3"})),
            # GET of account container (list objects continuation)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, '[]')]))
        resp = Request.blank('/auth/v2/act/.groups',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, json.dumps(
            {"groups": [{"name": ".admin"}, {"name": "act"},
                        {"name": "act:tester"}, {"name": "act:tester3"}]}))
        self.assertEquals(self.test_auth.app.calls, 5)

    def test_get_user_fail_invalid_account(self):
        resp = Request.blank('/auth/v2/.invalid/usr',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_get_user_fail_invalid_user(self):
        resp = Request.blank('/auth/v2/act/.invalid',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_get_user_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            headers={'X-Auth-Admin-User': 'super:admin',
                     'X-Auth-Admin-Key': 'supertest'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'},
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_user_account_admin_success(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but not reseller admin)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"})),
            # GET of requested user object
            ('200 Ok', {}, json.dumps(
                {"groups": [{"name": "act:usr"}, {"name": "act"},
                            {"name": ".admin"}],
                 "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 200)
        self.assertEquals(resp.body, json.dumps(
            {"groups": [{"name": "act:usr"}, {"name": "act"},
                        {"name": ".admin"}],
             "auth": "plaintext:key"}))
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_get_user_groups_not_found(self):
        self.test_auth.app = FakeApp(iter([
            # GET of account container (list objects)
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/.groups',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_user_groups_fail_listing(self):
        self.test_auth.app = FakeApp(iter([
            # GET of account container (list objects)
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/.groups',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_user_groups_fail_get_user(self):
        self.test_auth.app = FakeApp(iter([
            # GET of account container (list objects)
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'},
             json.dumps([
                {"name": ".services", "hash": "etag", "bytes": 112,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.618110"},
                {"name": "tester", "hash": "etag", "bytes": 104,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:27.736680"},
                {"name": "tester3", "hash": "etag", "bytes": 86,
                 "content_type": "application/octet-stream",
                 "last_modified": "2010-12-03T17:16:28.135530"}])),
            # GET of user object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/.groups',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_get_user_not_found(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_user_fail(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_put_user_fail_invalid_account(self):
        resp = Request.blank('/auth/v2/.invalid/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_put_user_fail_invalid_user(self):
        resp = Request.blank('/auth/v2/act/.usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_put_user_fail_no_user_key(self):
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_put_user_reseller_admin_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object (reseller admin)
            # This shouldn't actually get called, checked below
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:rdm"},
             {"name": "test"}, {"name": ".admin"},
             {"name": ".reseller_admin"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': 'act:rdm',
                     'X-Auth-Admin-Key': 'key',
                     'X-Auth-User-Key': 'key',
                     'X-Auth-User-Reseller-Admin': 'true'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 0)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but not reseller admin)
            # This shouldn't actually get called, checked below
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key',
                     'X-Auth-User-Key': 'key',
                     'X-Auth-User-Reseller-Admin': 'true'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 0)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            # This shouldn't actually get called, checked below
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key',
                     'X-Auth-User-Key': 'key',
                     'X-Auth-User-Reseller-Admin': 'true'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 0)

    def test_put_user_account_admin_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but wrong account)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act2:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': 'act2:adm',
                     'X-Auth-Admin-Key': 'key',
                     'X-Auth-User-Key': 'key',
                     'X-Auth-User-Admin': 'true'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key',
                     'X-Auth-User-Key': 'key',
                     'X-Auth-User-Admin': 'true'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_put_user_regular_fail_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but wrong account)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act2:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': 'act2:adm',
                     'X-Auth-Admin-Key': 'key',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_put_user_regular_success(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of user object
            ('201 Created', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(self.test_auth.app.calls, 2)
        self.assertEquals(json.loads(self.test_auth.app.request.body),
            {"groups": [{"name": "act:usr"}, {"name": "act"}],
             "auth": "plaintext:key"})

    def test_put_user_account_admin_success(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of user object
            ('201 Created', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key',
                     'X-Auth-User-Admin': 'true'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(self.test_auth.app.calls, 2)
        self.assertEquals(json.loads(self.test_auth.app.request.body),
            {"groups": [{"name": "act:usr"}, {"name": "act"},
                        {"name": ".admin"}],
             "auth": "plaintext:key"})

    def test_put_user_reseller_admin_success(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of user object
            ('201 Created', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key',
                     'X-Auth-User-Reseller-Admin': 'true'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 201)
        self.assertEquals(self.test_auth.app.calls, 2)
        self.assertEquals(json.loads(self.test_auth.app.request.body),
            {"groups": [{"name": "act:usr"}, {"name": "act"},
                        {"name": ".admin"}, {"name": ".reseller_admin"}],
             "auth": "plaintext:key"})

    def test_put_user_fail_not_found(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {'X-Container-Meta-Account-Id': 'AUTH_cfa'}, ''),
            # PUT of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_put_user_fail(self):
        self.test_auth.app = FakeApp(iter([
            # PUT of user object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest',
                     'X-Auth-User-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_user_bad_creds(self):
        self.test_auth.app = FakeApp(iter([
            # GET of user object (account admin, but wrong account)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act2:adm"},
             {"name": "test"}, {"name": ".admin"}],
             "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': 'act2:adm',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

        self.test_auth.app = FakeApp(iter([
            # GET of user object (regular user)
            ('200 Ok', {}, json.dumps({"groups": [{"name": "act:usr"},
             {"name": "test"}], "auth": "plaintext:key"}))]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 403)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_user_invalid_account(self):
        resp = Request.blank('/auth/v2/.invalid/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_delete_user_invalid_user(self):
        resp = Request.blank('/auth/v2/act/.invalid',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_delete_user_not_found(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_user_fail_head_user(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_delete_user_fail_delete_token(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tk'}, ''),
            # DELETE of token
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_delete_user_fail_delete_user(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tk'}, ''),
            # DELETE of token
            ('204 No Content', {}, ''),
            # DELETE of user object
            ('503 Service Unavailable', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_delete_user_success(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tk'}, ''),
            # DELETE of token
            ('204 No Content', {}, ''),
            # DELETE of user object
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_delete_user_success_missing_user_at_end(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tk'}, ''),
            # DELETE of token
            ('204 No Content', {}, ''),
            # DELETE of user object
            ('404 Not Found', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_delete_user_success_missing_token(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('200 Ok', {'X-Object-Meta-Auth-Token': 'AUTH_tk'}, ''),
            # DELETE of token
            ('404 Not Found', {}, ''),
            # DELETE of user object
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 3)

    def test_delete_user_success_no_token(self):
        self.test_auth.app = FakeApp(iter([
            # HEAD of user object
            ('200 Ok', {}, ''),
            # DELETE of user object
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/act/usr',
            environ={'REQUEST_METHOD': 'DELETE'},
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}
            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_validate_token_bad_prefix(self):
        resp = Request.blank('/auth/v2/.token/BAD_token'
                            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_validate_token_tmi(self):
        resp = Request.blank('/auth/v2/.token/AUTH_token/tmi'
                            ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 400)

    def test_validate_token_bad_memcache(self):
        fake_memcache = FakeMemcache()
        fake_memcache.set('AUTH_/auth/AUTH_token', 'bogus')
        resp = Request.blank('/auth/v2/.token/AUTH_token',
            environ={'swift.cache':
            fake_memcache}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 500)

    def test_validate_token_from_memcache(self):
        fake_memcache = FakeMemcache()
        fake_memcache.set('AUTH_/auth/AUTH_token', (time() + 1, 'act:usr,act'))
        resp = Request.blank('/auth/v2/.token/AUTH_token',
            environ={'swift.cache':
            fake_memcache}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('x-auth-groups'), 'act:usr,act')
        self.assert_(float(resp.headers['x-auth-ttl']) < 1,
                     resp.headers['x-auth-ttl'])

    def test_validate_token_from_memcache_expired(self):
        fake_memcache = FakeMemcache()
        fake_memcache.set('AUTH_/auth/AUTH_token', (time() - 1, 'act:usr,act'))
        resp = Request.blank('/auth/v2/.token/AUTH_token',
            environ={'swift.cache':
            fake_memcache}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assert_('x-auth-groups' not in resp.headers)
        self.assert_('x-auth-ttl' not in resp.headers)

    def test_validate_token_from_object(self):
        self.test_auth.app = FakeApp(iter([
            # GET of token object
            ('200 Ok', {}, json.dumps({'groups': [{'name': 'act:usr'},
             {'name': 'act'}], 'expires': time() + 1}))]))
        resp = Request.blank('/auth/v2/.token/AUTH_token'
                             ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 1)
        self.assertEquals(resp.headers.get('x-auth-groups'), 'act:usr,act')
        self.assert_(float(resp.headers['x-auth-ttl']) < 1,
                     resp.headers['x-auth-ttl'])

    def test_validate_token_from_object_expired(self):
        self.test_auth.app = FakeApp(iter([
            # GET of token object
            ('200 Ok', {}, json.dumps({'groups': 'act:usr,act',
             'expires': time() - 1})),
            # DELETE of expired token object
            ('204 No Content', {}, '')]))
        resp = Request.blank('/auth/v2/.token/AUTH_token'
                             ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)
        self.assertEquals(self.test_auth.app.calls, 2)

    def test_validate_token_from_object_with_admin(self):
        self.test_auth.app = FakeApp(iter([
            # GET of token object
            ('200 Ok', {}, json.dumps({'account_id': 'AUTH_cfa', 'groups':
             [{'name': 'act:usr'}, {'name': 'act'}, {'name': '.admin'}],
             'expires': time() + 1}))]))
        resp = Request.blank('/auth/v2/.token/AUTH_token'
                             ).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(self.test_auth.app.calls, 1)
        self.assertEquals(resp.headers.get('x-auth-groups'),
                          'act:usr,act,AUTH_cfa')
        self.assert_(float(resp.headers['x-auth-ttl']) < 1,
                     resp.headers['x-auth-ttl'])

    def test_get_conn_default(self):
        conn = self.test_auth.get_conn()
        self.assertEquals(conn.__class__, auth.HTTPConnection)
        self.assertEquals(conn.host, '127.0.0.1')
        self.assertEquals(conn.port, 8080)

    def test_get_conn_default_https(self):
        local_auth = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster': 'local#https://1.2.3.4/v1'})(FakeApp())
        conn = local_auth.get_conn()
        self.assertEquals(conn.__class__, auth.HTTPSConnection)
        self.assertEquals(conn.host, '1.2.3.4')
        self.assertEquals(conn.port, 443)

    def test_get_conn_overridden(self):
        local_auth = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster': 'local#https://1.2.3.4/v1'})(FakeApp())
        conn = \
            local_auth.get_conn(urlparsed=auth.urlparse('http://5.6.7.8/v1'))
        self.assertEquals(conn.__class__, auth.HTTPConnection)
        self.assertEquals(conn.host, '5.6.7.8')
        self.assertEquals(conn.port, 80)

    def test_get_conn_overridden_https(self):
        local_auth = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster': 'local#http://1.2.3.4/v1'})(FakeApp())
        conn = \
            local_auth.get_conn(urlparsed=auth.urlparse('https://5.6.7.8/v1'))
        self.assertEquals(conn.__class__, auth.HTTPSConnection)
        self.assertEquals(conn.host, '5.6.7.8')
        self.assertEquals(conn.port, 443)

    def test_get_itoken_fail_no_memcache(self):
        exc = None
        try:
            self.test_auth.get_itoken({})
        except Exception, err:
            exc = err
        self.assertEquals(str(exc),
                          'No memcache set up; required for Swauth middleware')

    def test_get_itoken_success(self):
        fmc = FakeMemcache()
        itk = self.test_auth.get_itoken({'swift.cache': fmc})
        self.assert_(itk.startswith('AUTH_itk'), itk)
        expires, groups = fmc.get('AUTH_/auth/%s' % itk)
        self.assert_(expires > time(), expires)
        self.assertEquals(groups, '.auth,.reseller_admin,AUTH_.auth')

    def test_get_admin_detail_fail_no_colon(self):
        self.test_auth.app = FakeApp(iter([]))
        self.assertEquals(self.test_auth.get_admin_detail(Request.blank('/')),
                          None)
        self.assertEquals(self.test_auth.get_admin_detail(Request.blank('/',
            headers={'X-Auth-Admin-User': 'usr'})), None)
        self.assertRaises(StopIteration, self.test_auth.get_admin_detail,
            Request.blank('/', headers={'X-Auth-Admin-User': 'act:usr'}))

    def test_get_admin_detail_fail_user_not_found(self):
        self.test_auth.app = FakeApp(iter([('404 Not Found', {}, '')]))
        self.assertEquals(self.test_auth.get_admin_detail(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:usr'})), None)
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_admin_detail_fail_get_user_error(self):
        self.test_auth.app = FakeApp(iter([
            ('503 Service Unavailable', {}, '')]))
        exc = None
        try:
            self.test_auth.get_admin_detail(Request.blank('/',
                headers={'X-Auth-Admin-User': 'act:usr'}))
        except Exception, err:
            exc = err
        self.assertEquals(str(exc), 'Could not get admin user object: '
            '/v1/AUTH_.auth/act/usr 503 Service Unavailable')
        self.assertEquals(self.test_auth.app.calls, 1)

    def test_get_admin_detail_success(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({"auth": "plaintext:key",
                         "groups": [{'name': "act:usr"}, {'name': "act"},
                                    {'name': ".admin"}]}))]))
        detail = self.test_auth.get_admin_detail(Request.blank('/',
                    headers={'X-Auth-Admin-User': 'act:usr'}))
        self.assertEquals(self.test_auth.app.calls, 1)
        self.assertEquals(detail, {'account': 'act',
            'auth': 'plaintext:key',
            'groups': [{'name': 'act:usr'}, {'name': 'act'},
                       {'name': '.admin'}]})

    def test_credentials_match_success(self):
        self.assert_(self.test_auth.credentials_match(
            {'auth': 'plaintext:key'}, 'key'))

    def test_credentials_match_fail_no_details(self):
        self.assert_(not self.test_auth.credentials_match(None, 'notkey'))

    def test_credentials_match_fail_plaintext(self):
        self.assert_(not self.test_auth.credentials_match(
            {'auth': 'plaintext:key'}, 'notkey'))

    def test_is_super_admin_success(self):
        self.assert_(self.test_auth.is_super_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'})))

    def test_is_super_admin_fail_bad_key(self):
        self.assert_(not self.test_auth.is_super_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'bad'})))
        self.assert_(not self.test_auth.is_super_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': '.super_admin'})))
        self.assert_(not self.test_auth.is_super_admin(Request.blank('/')))

    def test_is_super_admin_fail_bad_user(self):
        self.assert_(not self.test_auth.is_super_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'bad',
                     'X-Auth-Admin-Key': 'supertest'})))
        self.assert_(not self.test_auth.is_super_admin(Request.blank('/',
            headers={'X-Auth-Admin-Key': 'supertest'})))
        self.assert_(not self.test_auth.is_super_admin(Request.blank('/')))

    def test_is_reseller_admin_success_is_super_admin(self):
        self.assert_(self.test_auth.is_reseller_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'})))

    def test_is_reseller_admin_success_called_get_admin_detail(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:rdm'}, {'name': 'act'},
                                    {'name': '.admin'},
                                    {'name': '.reseller_admin'}]}))]))
        self.assert_(self.test_auth.is_reseller_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:rdm',
                     'X-Auth-Admin-Key': 'key'})))

    def test_is_reseller_admin_fail_only_account_admin(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:adm'}, {'name': 'act'},
                                    {'name': '.admin'}]}))]))
        self.assert_(not self.test_auth.is_reseller_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'})))

    def test_is_reseller_admin_fail_regular_user(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'}]}))]))
        self.assert_(not self.test_auth.is_reseller_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'})))

    def test_is_reseller_admin_fail_bad_key(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:rdm'}, {'name': 'act'},
                                    {'name': '.admin'},
                                    {'name': '.reseller_admin'}]}))]))
        self.assert_(not self.test_auth.is_reseller_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:rdm',
                     'X-Auth-Admin-Key': 'bad'})))

    def test_is_account_admin_success_is_super_admin(self):
        self.assert_(self.test_auth.is_account_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': '.super_admin',
                     'X-Auth-Admin-Key': 'supertest'}), 'act'))

    def test_is_account_admin_success_is_reseller_admin(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:rdm'}, {'name': 'act'},
                                    {'name': '.admin'},
                                    {'name': '.reseller_admin'}]}))]))
        self.assert_(self.test_auth.is_account_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:rdm',
                     'X-Auth-Admin-Key': 'key'}), 'act'))

    def test_is_account_admin_success(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:adm'}, {'name': 'act'},
                                    {'name': '.admin'}]}))]))
        self.assert_(self.test_auth.is_account_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:adm',
                     'X-Auth-Admin-Key': 'key'}), 'act'))

    def test_is_account_admin_fail_account_admin_different_account(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act2:adm'}, {'name': 'act2'},
                                    {'name': '.admin'}]}))]))
        self.assert_(not self.test_auth.is_account_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act2:adm',
                     'X-Auth-Admin-Key': 'key'}), 'act'))

    def test_is_account_admin_fail_regular_user(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'}]}))]))
        self.assert_(not self.test_auth.is_account_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:usr',
                     'X-Auth-Admin-Key': 'key'}), 'act'))

    def test_is_account_admin_fail_bad_key(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'auth': 'plaintext:key',
                         'groups': [{'name': 'act:rdm'}, {'name': 'act'},
                                    {'name': '.admin'},
                                    {'name': '.reseller_admin'}]}))]))
        self.assert_(not self.test_auth.is_account_admin(Request.blank('/',
            headers={'X-Auth-Admin-User': 'act:rdm',
                     'X-Auth-Admin-Key': 'bad'}), 'act'))

    def test_reseller_admin_but_account_is_internal_use_only(self):
        req = Request.blank('/v1/AUTH_.auth',
                            environ={'REQUEST_METHOD': 'GET'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)

    def test_reseller_admin_but_account_is_exactly_reseller_prefix(self):
        req = Request.blank('/v1/AUTH_', environ={'REQUEST_METHOD': 'GET'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp.status_int, 403)


if __name__ == '__main__':
    unittest.main()
