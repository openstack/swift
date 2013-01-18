# Copyright (c) 2012 OpenStack, LLC.
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

from swift.common.middleware import keystoneauth
from swift.common.swob import Request, Response, HTTPForbidden
from swift.common.http import HTTP_FORBIDDEN


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


class SwiftAuth(unittest.TestCase):
    def setUp(self):
        self.test_auth = keystoneauth.filter_factory({})(FakeApp())

    def _make_request(self, path=None, headers=None, **kwargs):
        if not path:
            path = '/v1/%s/c/o' % self.test_auth._get_account_for_tenant('foo')
        return Request.blank(path, headers=headers, **kwargs)

    def _get_identity_headers(self, status='Confirmed', tenant_id='1',
                              tenant_name='acct', user='usr', role=''):
        return dict(X_IDENTITY_STATUS=status,
                    X_TENANT_ID=tenant_id,
                    X_TENANT_NAME=tenant_name,
                    X_ROLES=role,
                    X_USER_NAME=user)

    def _get_successful_middleware(self):
        response_iter = iter([('200 OK', {}, '')])
        return keystoneauth.filter_factory({})(FakeApp(response_iter))

    def test_confirmed_identity_is_authorized(self):
        role = self.test_auth.reseller_admin_role
        headers = self._get_identity_headers(role=role)
        req = self._make_request('/v1/AUTH_acct/c', headers)
        resp = req.get_response(self._get_successful_middleware())
        self.assertEqual(resp.status_int, 200)

    def test_confirmed_identity_is_not_authorized(self):
        headers = self._get_identity_headers()
        req = self._make_request('/v1/AUTH_acct/c', headers)
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 403)

    def test_anonymous_is_authorized_for_permitted_referrer(self):
        req = self._make_request(headers={'X_IDENTITY_STATUS': 'Invalid'})
        req.acl = '.r:*'
        resp = req.get_response(self._get_successful_middleware())
        self.assertEqual(resp.status_int, 200)

    def test_anonymous_is_not_authorized_for_unknown_reseller_prefix(self):
        req = self._make_request(path='/v1/BLAH_foo/c/o',
                                 headers={'X_IDENTITY_STATUS': 'Invalid'})
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)

    def test_blank_reseller_prefix(self):
        conf = {'reseller_prefix': ''}
        test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        account = tenant_id = 'foo'
        self.assertTrue(test_auth._reseller_check(account, tenant_id))

    def test_override_asked_for_but_not_allowed(self):
        conf = {'allow_overrides': 'false'}
        self.test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 401)

    def test_override_asked_for_and_allowed(self):
        conf = {'allow_overrides': 'true'}
        self.test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)

    def test_override_default_allowed(self):
        req = self._make_request('/v1/AUTH_account',
                                 environ={'swift.authorize_override': True})
        resp = req.get_response(self.test_auth)
        self.assertEquals(resp.status_int, 404)

    def test_anonymous_options_allowed(self):
        req = self._make_request('/v1/AUTH_account',
                                 environ={'REQUEST_METHOD': 'OPTIONS'})
        resp = req.get_response(self._get_successful_middleware())
        self.assertEqual(resp.status_int, 200)

    def test_identified_options_allowed(self):
        headers = self._get_identity_headers()
        headers['REQUEST_METHOD'] = 'OPTIONS'
        req = self._make_request('/v1/AUTH_account',
                                 headers=self._get_identity_headers(),
                                 environ={'REQUEST_METHOD': 'OPTIONS'})
        resp = req.get_response(self._get_successful_middleware())
        self.assertEqual(resp.status_int, 200)


class TestAuthorize(unittest.TestCase):
    def setUp(self):
        self.test_auth = keystoneauth.filter_factory({})(FakeApp())

    def _make_request(self, path, **kwargs):
        return Request.blank(path, **kwargs)

    def _get_account(self, identity=None):
        if not identity:
            identity = self._get_identity()
        return self.test_auth._get_account_for_tenant(identity['tenant'][0])

    def _get_identity(self, tenant_id='tenant_id',
                      tenant_name='tenant_name', user='user', roles=None):
        if not roles:
            roles = []
        return dict(tenant=(tenant_id, tenant_name), user=user, roles=roles)

    def _check_authenticate(self, account=None, identity=None, headers=None,
                            exception=None, acl=None, env=None, path=None):
        if not identity:
            identity = self._get_identity()
        if not account:
            account = self._get_account(identity)
        if not path:
            path = '/v1/%s/c' % account
        default_env = {'keystone.identity': identity,
                       'REMOTE_USER': identity['tenant']}
        if env:
            default_env.update(env)
        req = self._make_request(path, headers=headers, environ=default_env)
        req.acl = acl
        result = self.test_auth.authorize(req)
        if exception:
            self.assertEquals(result.status_int, exception)
        else:
            self.assertTrue(result is None)
        return req

    def test_authorize_fails_for_unauthorized_user(self):
        self._check_authenticate(exception=HTTP_FORBIDDEN)

    def test_authorize_fails_for_invalid_reseller_prefix(self):
        self._check_authenticate(account='BLAN_a',
                                 exception=HTTP_FORBIDDEN)

    def test_authorize_succeeds_for_reseller_admin(self):
        roles = [self.test_auth.reseller_admin_role]
        identity = self._get_identity(roles=roles)
        req = self._check_authenticate(identity=identity)
        self.assertTrue(req.environ.get('swift_owner'))

    def test_authorize_succeeds_as_owner_for_operator_role(self):
        roles = self.test_auth.operator_roles.split(',')[0]
        identity = self._get_identity(roles=roles)
        req = self._check_authenticate(identity=identity)
        self.assertTrue(req.environ.get('swift_owner'))

    def _check_authorize_for_tenant_owner_match(self, exception=None):
        identity = self._get_identity()
        identity['user'] = identity['tenant'][1]
        req = self._check_authenticate(identity=identity, exception=exception)
        expected = bool(exception is None)
        self.assertEqual(bool(req.environ.get('swift_owner')), expected)

    def test_authorize_succeeds_as_owner_for_tenant_owner_match(self):
        self.test_auth.is_admin = True
        self._check_authorize_for_tenant_owner_match()

    def test_authorize_fails_as_owner_for_tenant_owner_match(self):
        self.test_auth.is_admin = False
        self._check_authorize_for_tenant_owner_match(
            exception=HTTP_FORBIDDEN)

    def test_authorize_succeeds_for_container_sync(self):
        env = {'swift_sync_key': 'foo', 'REMOTE_ADDR': '127.0.0.1'}
        headers = {'x-container-sync-key': 'foo', 'x-timestamp': '1'}
        self._check_authenticate(env=env, headers=headers)

    def test_authorize_fails_for_invalid_referrer(self):
        env = {'HTTP_REFERER': 'http://invalid.com/index.html'}
        self._check_authenticate(acl='.r:example.com', env=env,
                                 exception=HTTP_FORBIDDEN)

    def test_authorize_fails_for_referrer_without_rlistings(self):
        env = {'HTTP_REFERER': 'http://example.com/index.html'}
        self._check_authenticate(acl='.r:example.com', env=env,
                                 exception=HTTP_FORBIDDEN)

    def test_authorize_succeeds_for_referrer_with_rlistings(self):
        env = {'HTTP_REFERER': 'http://example.com/index.html'}
        self._check_authenticate(acl='.r:example.com,.rlistings', env=env)

    def test_authorize_succeeds_for_referrer_with_obj(self):
        path = '/v1/%s/c/o' % self._get_account()
        env = {'HTTP_REFERER': 'http://example.com/index.html'}
        self._check_authenticate(acl='.r:example.com', env=env, path=path)

    def test_authorize_succeeds_for_user_role_in_roles(self):
        acl = 'allowme'
        identity = self._get_identity(roles=[acl])
        self._check_authenticate(identity=identity, acl=acl)

    def test_authorize_succeeds_for_tenant_name_user_in_roles(self):
        identity = self._get_identity()
        acl = '%s:%s' % (identity['tenant'][1], identity['user'])
        self._check_authenticate(identity=identity, acl=acl)

    def test_authorize_succeeds_for_tenant_id_user_in_roles(self):
        identity = self._get_identity()
        acl = '%s:%s' % (identity['tenant'][0], identity['user'])
        self._check_authenticate(identity=identity, acl=acl)

    def test_authorize_succeeds_for_wildcard_tenant_user_in_roles(self):
        identity = self._get_identity()
        acl = '*:%s' % (identity['user'])
        self._check_authenticate(identity=identity, acl=acl)

    def test_cross_tenant_authorization_success(self):
        self.assertTrue(self.test_auth._authorize_cross_tenant('userA',
            'tenantID', 'tenantNAME', ['tenantID:userA']))
        self.assertTrue(self.test_auth._authorize_cross_tenant('userA',
            'tenantID', 'tenantNAME', ['tenantNAME:userA']))
        self.assertTrue(self.test_auth._authorize_cross_tenant('userA',
            'tenantID', 'tenantNAME', ['*:userA']))

    def test_cross_tenant_authorization_failure(self):
        self.assertFalse(self.test_auth._authorize_cross_tenant('userA',
            'tenantID', 'tenantNAME', ['tenantXYZ:userA']))


if __name__ == '__main__':
    unittest.main()
