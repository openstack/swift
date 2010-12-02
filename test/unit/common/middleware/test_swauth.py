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
        except:
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
        req = Request.blank('', environ=env)
        if 'swift.authorize' in env:
            resp = env['swift.authorize'](req)
            if resp:
                return resp(env, start_response)
        status, headers, body = self.status_headers_body_iter.next()
        return Response(status=status, headers=headers,
                        body=body)(env, start_response)


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
            'default_swift_cluster': 'local:badscheme://host/path'}), app)
        ath = auth.filter_factory({'super_admin_key': 'supertest'})(app)
        self.assertEquals(ath.default_swift_cluster,
                          'local:http://127.0.0.1:8080/v1')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster': 'local:http://host/path'})(app)
        self.assertEquals(ath.default_swift_cluster,
                          'local:http://host/path')
        ath = auth.filter_factory({'super_admin_key': 'supertest',
            'default_swift_cluster': 'local:http://host/path/'})(app)
        self.assertEquals(ath.default_swift_cluster,
                          'local:http://host/path')

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

    def test_auth_just_expired(self):
        self.test_auth.app = FakeApp(iter([
            ('200 Ok', {},
             json.dumps({'account': 'act', 'user': 'act:usr',
                         'account_id': 'AUTH_cfa',
                         'groups': [{'name': 'act:usr'}, {'name': 'act'},
                                    {'name': '.admin'}],
                         'expires': time() - 1}))]))
        resp = Request.blank('/v1/AUTH_cfa',
            headers={'X-Auth-Token': 'AUTH_t'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

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

    def test_authorize_bad_path(self):
        req = Request.blank('/badpath')
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('401'), resp)
        req = Request.blank('/badpath')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

    def test_authorize_account_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

    def test_authorize_acl_group_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
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
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = 'act:usr2'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

    def test_deny_cross_reseller(self):
        # Tests that cross-reseller is denied, even if ACLs/group names match
        req = Request.blank('/v1/OTHER_cfa')
        req.remote_user = 'act:usr,act,AUTH_cfa'
        req.acl = 'act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

    def test_authorize_acl_referrer_access(self):
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.acl = '.r:.example.com'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)
        req = Request.blank('/v1/AUTH_cfa')
        req.remote_user = 'act:usr,act'
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('401'), resp)
        req = Request.blank('/v1/AUTH_cfa')
        req.acl = '.r:*'
        self.assertEquals(self.test_auth.authorize(req), None)
        req = Request.blank('/v1/AUTH_cfa')
        req.acl = '.r:.example.com'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('401'), resp)
        req = Request.blank('/v1/AUTH_cfa')
        req.referer = 'http://www.example.com/index.html'
        req.acl = '.r:.example.com'
        self.assertEquals(self.test_auth.authorize(req), None)

    def test_account_put_permissions(self):
        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_other'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

        # Even PUTs to your own account as account admin should fail
        req = Request.blank('/v1/AUTH_old', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,AUTH_old'
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.reseller_admin'
        resp = self.test_auth.authorize(req)
        self.assertEquals(resp, None)

        # .super_admin is not something the middleware should ever see or care
        # about
        req = Request.blank('/v1/AUTH_new', environ={'REQUEST_METHOD': 'PUT'})
        req.remote_user = 'act:usr,act,.super_admin'
        resp = self.test_auth.authorize(req)
        resp = str(self.test_auth.authorize(req))
        self.assert_(resp.startswith('403'), resp)

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
                     'X-Auth-Key': 'invalid'}).get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

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


if __name__ == '__main__':
    unittest.main()
