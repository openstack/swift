# Copyright (c) 2012 OpenStack Foundation
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
from swift.common.swob import Request, Response
from swift.common.http import HTTP_FORBIDDEN
from swift.proxy.controllers.base import _get_cache_key
from test.unit import FakeLogger

UNKNOWN_ID = keystoneauth.UNKNOWN_ID


def _fake_token_info(version='2'):
    if version == '2':
        return {'access': 'fake_value'}
    if version == '3':
        return {'token': 'fake_value'}


class FakeApp(object):
    def __init__(self, status_headers_body_iter=None):
        self.calls = 0
        self.call_contexts = []
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
        context = {'method': self.request.method,
                   'headers': self.request.headers}
        self.call_contexts.append(context)
        status, headers, body = self.status_headers_body_iter.next()
        return Response(status=status, headers=headers,
                        body=body)(env, start_response)


class SwiftAuth(unittest.TestCase):
    def setUp(self):
        self.test_auth = keystoneauth.filter_factory({})(FakeApp())
        self.test_auth.logger = FakeLogger()

    def _make_request(self, path=None, headers=None, **kwargs):
        if not path:
            path = '/v1/%s/c/o' % self.test_auth._get_account_for_tenant('foo')
        return Request.blank(path, headers=headers, **kwargs)

    def _get_identity_headers(self, status='Confirmed', tenant_id='1',
                              tenant_name='acct', project_domain_name='domA',
                              project_domain_id='99',
                              user_name='usr', user_id='42',
                              user_domain_name='domA', user_domain_id='99',
                              role='admin'):
        return dict(X_IDENTITY_STATUS=status,
                    X_TENANT_ID=tenant_id,
                    X_TENANT_NAME=tenant_name,
                    X_PROJECT_ID=tenant_id,
                    X_PROJECT_NAME=tenant_name,
                    X_PROJECT_DOMAIN_ID=project_domain_id,
                    X_PROJECT_DOMAIN_NAME=project_domain_name,
                    X_ROLES=role,
                    X_USER_NAME=user_name,
                    X_USER_ID=user_id,
                    X_USER_DOMAIN_NAME=user_domain_name,
                    X_USER_DOMAIN_ID=user_domain_id)

    def _get_successful_middleware(self):
        response_iter = iter([('200 OK', {}, '')])
        return keystoneauth.filter_factory({})(FakeApp(response_iter))

    def test_invalid_request_authorized(self):
        role = self.test_auth.reseller_admin_role
        headers = self._get_identity_headers(role=role)
        req = self._make_request('/', headers=headers)
        resp = req.get_response(self._get_successful_middleware())
        self.assertEqual(resp.status_int, 404)

    def test_invalid_request_non_authorized(self):
        req = self._make_request('/')
        resp = req.get_response(self._get_successful_middleware())
        self.assertEqual(resp.status_int, 404)

    def test_confirmed_identity_is_authorized(self):
        role = self.test_auth.reseller_admin_role
        headers = self._get_identity_headers(role=role)
        req = self._make_request('/v1/AUTH_acct/c', headers)
        resp = req.get_response(self._get_successful_middleware())
        self.assertEqual(resp.status_int, 200)

    def test_detect_reseller_request(self):
        role = self.test_auth.reseller_admin_role
        headers = self._get_identity_headers(role=role)
        req = self._make_request('/v1/AUTH_acct/c', headers)
        req.get_response(self._get_successful_middleware())
        self.assertTrue(req.environ.get('reseller_request'))

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

    def test_anonymous_with_validtoken_authorized_for_permitted_referrer(self):
        req = self._make_request(headers={'X_IDENTITY_STATUS': 'Confirmed'})
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

    def test_reseller_prefix_added_underscore(self):
        conf = {'reseller_prefix': 'AUTH'}
        test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        self.assertEqual(test_auth.reseller_prefix, "AUTH_")

    def test_reseller_prefix_not_added_double_underscores(self):
        conf = {'reseller_prefix': 'AUTH_'}
        test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        self.assertEqual(test_auth.reseller_prefix, "AUTH_")

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

    def test_auth_scheme(self):
        req = self._make_request(path='/v1/BLAH_foo/c/o',
                                 headers={'X_IDENTITY_STATUS': 'Invalid'})
        resp = req.get_response(self.test_auth)
        self.assertEqual(resp.status_int, 401)
        self.assertTrue('Www-Authenticate' in resp.headers)

    def test_project_domain_id_sysmeta_set(self):
        proj_id = '12345678'
        proj_domain_id = '13'
        headers = self._get_identity_headers(tenant_id=proj_id,
                                             project_domain_id=proj_domain_id)
        account = self.test_auth._get_account_for_tenant(proj_id)
        path = '/v1/' + account
        # fake cached account info
        _, info_key = _get_cache_key(account, None)
        env = {info_key: {'status': 0, 'sysmeta': {}},
               'keystone.token_info': _fake_token_info(version='3')}
        req = Request.blank(path, environ=env, headers=headers)
        req.method = 'POST'
        headers_out = {'X-Account-Sysmeta-Project-Domain-Id': proj_domain_id}
        fake_app = FakeApp(iter([('200 OK', headers_out, '')]))
        test_auth = keystoneauth.filter_factory({})(fake_app)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(len(fake_app.call_contexts), 1)
        headers_sent = fake_app.call_contexts[0]['headers']
        self.assertTrue('X-Account-Sysmeta-Project-Domain-Id' in headers_sent,
                        headers_sent)
        self.assertEqual(headers_sent['X-Account-Sysmeta-Project-Domain-Id'],
                         proj_domain_id)
        self.assertTrue('X-Account-Project-Domain-Id' in resp.headers)
        self.assertEqual(resp.headers['X-Account-Project-Domain-Id'],
                         proj_domain_id)

    def test_project_domain_id_sysmeta_set_to_unknown(self):
        proj_id = '12345678'
        # token scoped to a different project
        headers = self._get_identity_headers(tenant_id='87654321',
                                             project_domain_id='default',
                                             role='reselleradmin')
        account = self.test_auth._get_account_for_tenant(proj_id)
        path = '/v1/' + account
        # fake cached account info
        _, info_key = _get_cache_key(account, None)
        env = {info_key: {'status': 0, 'sysmeta': {}},
               'keystone.token_info': _fake_token_info(version='3')}
        req = Request.blank(path, environ=env, headers=headers)
        req.method = 'POST'
        fake_app = FakeApp(iter([('200 OK', {}, '')]))
        test_auth = keystoneauth.filter_factory({})(fake_app)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(len(fake_app.call_contexts), 1)
        headers_sent = fake_app.call_contexts[0]['headers']
        self.assertTrue('X-Account-Sysmeta-Project-Domain-Id' in headers_sent,
                        headers_sent)
        self.assertEqual(headers_sent['X-Account-Sysmeta-Project-Domain-Id'],
                         UNKNOWN_ID)

    def test_project_domain_id_sysmeta_not_set(self):
        proj_id = '12345678'
        headers = self._get_identity_headers(tenant_id=proj_id, role='admin')
        account = self.test_auth._get_account_for_tenant(proj_id)
        path = '/v1/' + account
        _, info_key = _get_cache_key(account, None)
        # v2 token
        env = {info_key: {'status': 0, 'sysmeta': {}},
               'keystone.token_info': _fake_token_info(version='2')}
        req = Request.blank(path, environ=env, headers=headers)
        req.method = 'POST'
        fake_app = FakeApp(iter([('200 OK', {}, '')]))
        test_auth = keystoneauth.filter_factory({})(fake_app)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(len(fake_app.call_contexts), 1)
        headers_sent = fake_app.call_contexts[0]['headers']
        self.assertFalse('X-Account-Sysmeta-Project-Domain-Id' in headers_sent,
                         headers_sent)

    def test_project_domain_id_sysmeta_set_unknown_with_v2(self):
        proj_id = '12345678'
        # token scoped to a different project
        headers = self._get_identity_headers(tenant_id='87654321',
                                             role='reselleradmin')
        account = self.test_auth._get_account_for_tenant(proj_id)
        path = '/v1/' + account
        _, info_key = _get_cache_key(account, None)
        # v2 token
        env = {info_key: {'status': 0, 'sysmeta': {}},
               'keystone.token_info': _fake_token_info(version='2')}
        req = Request.blank(path, environ=env, headers=headers)
        req.method = 'POST'
        fake_app = FakeApp(iter([('200 OK', {}, '')]))
        test_auth = keystoneauth.filter_factory({})(fake_app)
        resp = req.get_response(test_auth)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(len(fake_app.call_contexts), 1)
        headers_sent = fake_app.call_contexts[0]['headers']
        self.assertTrue('X-Account-Sysmeta-Project-Domain-Id' in headers_sent,
                        headers_sent)
        self.assertEqual(headers_sent['X-Account-Sysmeta-Project-Domain-Id'],
                         UNKNOWN_ID)


class BaseTestAuthorize(unittest.TestCase):
    def setUp(self):
        self.test_auth = keystoneauth.filter_factory({})(FakeApp())
        self.test_auth.logger = FakeLogger()

    def _make_request(self, path, **kwargs):
        return Request.blank(path, **kwargs)

    def _get_account(self, identity=None):
        if not identity:
            identity = self._get_identity()
        return self.test_auth._get_account_for_tenant(
            identity['HTTP_X_TENANT_ID'])

    def _get_identity(self, tenant_id='tenant_id', tenant_name='tenant_name',
                      user_id='user_id', user_name='user_name', roles=None,
                      project_domain_name='domA', project_domain_id='foo',
                      user_domain_name='domA', user_domain_id='foo'):
        if roles is None:
            roles = []
        if isinstance(roles, list):
            roles = ','.join(roles)
        return {'HTTP_X_USER_ID': user_id,
                'HTTP_X_USER_NAME': user_name,
                'HTTP_X_USER_DOMAIN_NAME': user_domain_name,
                'HTTP_X_USER_DOMAIN_ID': user_domain_id,
                'HTTP_X_TENANT_ID': tenant_id,
                'HTTP_X_TENANT_NAME': tenant_name,
                'HTTP_X_PROJECT_DOMAIN_ID': project_domain_id,
                'HTTP_X_PROJECT_DOMAIN_NAME': project_domain_name,
                'HTTP_X_ROLES': roles,
                'HTTP_X_IDENTITY_STATUS': 'Confirmed'}

    def _get_env_id(self, tenant_id='tenant_id', tenant_name='tenant_name',
                    user_id='user_id', user_name='user_name', roles=[],
                    project_domain_name='domA', project_domain_id='99',
                    user_domain_name='domA', user_domain_id='99',
                    auth_version='3'):
        env = self._get_identity(tenant_id, tenant_name, user_id, user_name,
                                 roles, project_domain_name,
                                 project_domain_id, user_domain_name,
                                 user_domain_id)
        token_info = _fake_token_info(version=auth_version)
        env.update({'keystone.token_info': token_info})
        return self.test_auth._integral_keystone_identity(env)


class TestAuthorize(BaseTestAuthorize):
    def _check_authenticate(self, account=None, identity=None, headers=None,
                            exception=None, acl=None, env=None, path=None):
        if not identity:
            identity = self._get_identity()
        if not account:
            account = self._get_account(identity)
        if not path:
            path = '/v1/%s/c' % account
        # fake cached account info
        _, info_key = _get_cache_key(account, None)
        default_env = {'REMOTE_USER': identity['HTTP_X_TENANT_ID'],
                       info_key: {'status': 200, 'sysmeta': {}}}
        default_env.update(identity)
        if env:
            default_env.update(env)
        req = self._make_request(path, headers=headers, environ=default_env)
        req.acl = acl

        env_identity = self.test_auth._integral_keystone_identity(req.environ)
        result = self.test_auth.authorize(env_identity, req)

        # if we have requested an exception but nothing came back then
        if exception and not result:
            self.fail("error %s was not returned" % (str(exception)))
        elif exception:
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

    def test_authorize_succeeds_for_insensitive_reseller_admin(self):
        roles = [self.test_auth.reseller_admin_role.upper()]
        identity = self._get_identity(roles=roles)
        req = self._check_authenticate(identity=identity)
        self.assertTrue(req.environ.get('swift_owner'))

    def test_authorize_succeeds_as_owner_for_operator_role(self):
        roles = self.test_auth.operator_roles.split(',')
        identity = self._get_identity(roles=roles)
        req = self._check_authenticate(identity=identity)
        self.assertTrue(req.environ.get('swift_owner'))

    def test_authorize_succeeds_as_owner_for_insensitive_operator_role(self):
        roles = [r.upper() for r in self.test_auth.operator_roles.split(',')]
        identity = self._get_identity(roles=roles)
        req = self._check_authenticate(identity=identity)
        self.assertTrue(req.environ.get('swift_owner'))

    def _check_authorize_for_tenant_owner_match(self, exception=None):
        identity = self._get_identity(user_name='same_name',
                                      tenant_name='same_name')
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
        user_name = identity['HTTP_X_USER_NAME']
        user_id = identity['HTTP_X_USER_ID']
        tenant_id = identity['HTTP_X_TENANT_ID']
        for user in [user_id, user_name, '*']:
            acl = '%s:%s' % (tenant_id, user)
            self._check_authenticate(identity=identity, acl=acl)

    def test_authorize_succeeds_for_tenant_id_user_in_roles(self):
        identity = self._get_identity()
        user_name = identity['HTTP_X_USER_NAME']
        user_id = identity['HTTP_X_USER_ID']
        tenant_name = identity['HTTP_X_TENANT_NAME']
        for user in [user_id, user_name, '*']:
            acl = '%s:%s' % (tenant_name, user)
            self._check_authenticate(identity=identity, acl=acl)

    def test_authorize_succeeds_for_wildcard_tenant_user_in_roles(self):
        identity = self._get_identity()
        user_name = identity['HTTP_X_USER_NAME']
        user_id = identity['HTTP_X_USER_ID']
        for user in [user_id, user_name, '*']:
            acl = '*:%s' % user
            self._check_authenticate(identity=identity, acl=acl)

    def test_cross_tenant_authorization_success(self):
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantID:userA']),
            'tenantID:userA')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantNAME:userA']),
            'tenantNAME:userA')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME', ['*:userA']),
            '*:userA')

        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantID:userID']),
            'tenantID:userID')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantNAME:userID']),
            'tenantNAME:userID')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME', ['*:userID']),
            '*:userID')

        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME', ['tenantID:*']),
            'tenantID:*')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME', ['tenantNAME:*']),
            'tenantNAME:*')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME', ['*:*']),
            '*:*')

    def test_cross_tenant_authorization_failure(self):
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantXYZ:userA']),
            None)

    def test_cross_tenant_authorization_allow_names(self):
        # tests that the allow_names arg does the right thing
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantNAME:userA'], allow_names=True),
            'tenantNAME:userA')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantNAME:userID'], allow_names=True),
            'tenantNAME:userID')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantID:userA'], allow_names=True),
            'tenantID:userA')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantID:userID'], allow_names=True),
            'tenantID:userID')
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantNAME:userA'], allow_names=False),
            None)
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantID:userA'], allow_names=False),
            None)
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantNAME:userID'], allow_names=False),
            None)
        self.assertEqual(
            self.test_auth._authorize_cross_tenant(
                'userID', 'userA', 'tenantID', 'tenantNAME',
                ['tenantID:userID'], allow_names=False),
            'tenantID:userID')

    def test_delete_own_account_not_allowed(self):
        roles = self.test_auth.operator_roles.split(',')
        identity = self._get_identity(roles=roles)
        account = self._get_account(identity)
        self._check_authenticate(account=account,
                                 identity=identity,
                                 exception=HTTP_FORBIDDEN,
                                 path='/v1/' + account,
                                 env={'REQUEST_METHOD': 'DELETE'})

    def test_delete_own_account_when_reseller_allowed(self):
        roles = [self.test_auth.reseller_admin_role]
        identity = self._get_identity(roles=roles)
        account = self._get_account(identity)
        req = self._check_authenticate(account=account,
                                       identity=identity,
                                       path='/v1/' + account,
                                       env={'REQUEST_METHOD': 'DELETE'})
        self.assertEqual(bool(req.environ.get('swift_owner')), True)

    def test_identity_set_up_at_call(self):
        def fake_start_response(*args, **kwargs):
            pass
        the_env = self._get_identity(
            tenant_id='test', roles=['reselleradmin'])
        self.test_auth(the_env, fake_start_response)

        subreq = Request.blank(
            '/v1/%s/c/o' % self.test_auth._get_account_for_tenant('test'))
        subreq.environ.update(
            self._get_identity(tenant_id='test', roles=['got_erased']))

        authorize_resp = the_env['swift.authorize'](subreq)
        self.assertEqual(authorize_resp, None)

    def test_names_disallowed_in_acls_outside_default_domain(self):
        id = self._get_identity(user_domain_id='non-default',
                                project_domain_id='non-default')
        env = {'keystone.token_info': _fake_token_info(version='3')}
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env,
                                 exception=HTTP_FORBIDDEN)
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env,
                                 exception=HTTP_FORBIDDEN)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env,
                                 exception=HTTP_FORBIDDEN)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env)

    def test_names_allowed_in_acls_inside_default_domain(self):
        id = self._get_identity(user_domain_id='default',
                                project_domain_id='default')
        env = {'keystone.token_info': _fake_token_info(version='3')}
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env)
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env)

    def test_names_allowed_in_acls_inside_default_domain_with_config(self):
        conf = {'allow_names_in_acls': 'yes'}
        self.test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        self.test_auth.logger = FakeLogger()
        id = self._get_identity(user_domain_id='default',
                                project_domain_id='default')
        env = {'keystone.token_info': _fake_token_info(version='3')}
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env)
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env)

    def test_names_disallowed_in_acls_inside_default_domain(self):
        conf = {'allow_names_in_acls': 'false'}
        self.test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        self.test_auth.logger = FakeLogger()
        id = self._get_identity(user_domain_id='default',
                                project_domain_id='default')
        env = {'keystone.token_info': _fake_token_info(version='3')}
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env,
                                 exception=HTTP_FORBIDDEN)
        acl = '%s:%s' % (id['HTTP_X_TENANT_NAME'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env,
                                 exception=HTTP_FORBIDDEN)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_NAME'])
        self._check_authenticate(acl=acl, identity=id, env=env,
                                 exception=HTTP_FORBIDDEN)
        acl = '%s:%s' % (id['HTTP_X_TENANT_ID'], id['HTTP_X_USER_ID'])
        self._check_authenticate(acl=acl, identity=id, env=env)

    def test_integral_keystone_identity(self):
        user = ('U_ID', 'U_NAME')
        roles = ('ROLE1', 'ROLE2')
        project = ('P_ID', 'P_NAME')
        user_domain = ('UD_ID', 'UD_NAME')
        project_domain = ('PD_ID', 'PD_NAME')

        # no valid identity info in headers
        req = Request.blank('/v/a/c/o')
        data = self.test_auth._integral_keystone_identity(req.environ)
        self.assertEqual(None, data)

        # valid identity info in headers, but status unconfirmed
        req.headers.update({'X-Identity-Status': 'Blah',
                            'X-Roles': '%s,%s' % roles,
                            'X-User-Id': user[0],
                            'X-User-Name': user[1],
                            'X-Tenant-Id': project[0],
                            'X-Tenant-Name': project[1],
                            'X-User-Domain-Id': user_domain[0],
                            'X-User-Domain-Name': user_domain[1],
                            'X-Project-Domain-Id': project_domain[0],
                            'X-Project-Domain-Name': project_domain[1]})
        data = self.test_auth._integral_keystone_identity(req.environ)
        self.assertEqual(None, data)

        # valid identity info in headers, no token info in environ
        req.headers.update({'X-Identity-Status': 'Confirmed'})
        expected = {'user': user,
                    'tenant': project,
                    'roles': list(roles),
                    'user_domain': (None, None),
                    'project_domain': (None, None),
                    'auth_version': 0}
        data = self.test_auth._integral_keystone_identity(req.environ)
        self.assertEquals(expected, data)

        # v2 token info in environ
        req.environ['keystone.token_info'] = _fake_token_info(version='2')
        expected = {'user': user,
                    'tenant': project,
                    'roles': list(roles),
                    'user_domain': (None, None),
                    'project_domain': (None, None),
                    'auth_version': 2}
        data = self.test_auth._integral_keystone_identity(req.environ)
        self.assertEquals(expected, data)

        # v3 token info in environ
        req.environ['keystone.token_info'] = _fake_token_info(version='3')
        expected = {'user': user,
                    'tenant': project,
                    'roles': list(roles),
                    'user_domain': user_domain,
                    'project_domain': project_domain,
                    'auth_version': 3}
        data = self.test_auth._integral_keystone_identity(req.environ)
        self.assertEquals(expected, data)

    def test_get_project_domain_id(self):
        sysmeta = {}
        info = {'sysmeta': sysmeta}
        _, info_key = _get_cache_key('AUTH_1234', None)
        env = {'PATH_INFO': '/v1/AUTH_1234',
               info_key: info}

        # account does not exist
        info['status'] = 404
        self.assertEqual(self.test_auth._get_project_domain_id(env),
                         (False, None))
        info['status'] = 0
        self.assertEqual(self.test_auth._get_project_domain_id(env),
                         (False, None))

        # account exists, no project domain id in sysmeta
        info['status'] = 200
        self.assertEqual(self.test_auth._get_project_domain_id(env),
                         (True, None))

        # account exists with project domain id in sysmeta
        sysmeta['project-domain-id'] = 'default'
        self.assertEqual(self.test_auth._get_project_domain_id(env),
                         (True, 'default'))


class TestIsNameAllowedInACL(BaseTestAuthorize):
    def setUp(self):
        super(TestIsNameAllowedInACL, self).setUp()
        self.default_id = 'default'

    def _assert_names_allowed(self, expected, user_domain_id=None,
                              req_project_domain_id=None,
                              sysmeta_project_domain_id=None,
                              scoped='account'):
        project_name = 'foo'
        account_id = '12345678'
        account = self.test_auth._get_account_for_tenant(account_id)
        parts = ('v1', account, None, None)
        path = '/%s/%s' % parts[0:2]

        sysmeta = {}
        if sysmeta_project_domain_id:
            sysmeta = {'project-domain-id': sysmeta_project_domain_id}

        # pretend account exists
        info = {'status': 200, 'sysmeta': sysmeta}
        _, info_key = _get_cache_key(account, None)
        req = Request.blank(path, environ={info_key: info})

        if scoped == 'account':
            project_name = 'account_name'
            project_id = account_id
        elif scoped == 'other':
            project_name = 'other_name'
            project_id = '87654321'
        else:
            # unscoped token
            project_name, project_id, req_project_domain_id = None, None, None

        if user_domain_id:
            id = self._get_env_id(tenant_name=project_name,
                                  tenant_id=project_id,
                                  user_domain_id=user_domain_id,
                                  project_domain_id=req_project_domain_id)
        else:
            # must be v2 token info
            id = self._get_env_id(tenant_name=project_name,
                                  tenant_id=project_id,
                                  auth_version='2')

        actual = self.test_auth._is_name_allowed_in_acl(req, parts, id)
        self.assertEqual(actual, expected, '%s, %s, %s, %s'
                         % (user_domain_id, req_project_domain_id,
                            sysmeta_project_domain_id, scoped))

    def test_is_name_allowed_in_acl_with_token_scoped_to_tenant(self):
        # no user or project domain ids in request token so must be v2,
        # user and project should be assumed to be in default domain
        self._assert_names_allowed(True, user_domain_id=None,
                                   req_project_domain_id=None,
                                   sysmeta_project_domain_id=None)
        self._assert_names_allowed(True, user_domain_id=None,
                                   req_project_domain_id=None,
                                   sysmeta_project_domain_id=self.default_id)
        self._assert_names_allowed(True, user_domain_id=None,
                                   req_project_domain_id=None,
                                   sysmeta_project_domain_id=UNKNOWN_ID)
        self._assert_names_allowed(True, user_domain_id=None,
                                   req_project_domain_id=None,
                                   sysmeta_project_domain_id='foo')

        # user in default domain, project domain in token info takes precedence
        self._assert_names_allowed(True, user_domain_id=self.default_id,
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=None)
        self._assert_names_allowed(True, user_domain_id=self.default_id,
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=UNKNOWN_ID)
        self._assert_names_allowed(True, user_domain_id=self.default_id,
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id='bar')
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id='foo',
                                   sysmeta_project_domain_id=None)
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id='foo',
                                   sysmeta_project_domain_id=self.default_id)
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id='foo',
                                   sysmeta_project_domain_id='foo')

        # user in non-default domain so names should never be allowed
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=None)
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=self.default_id)
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=UNKNOWN_ID)
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id='foo')

    def test_is_name_allowed_in_acl_with_unscoped_token(self):
        # user in default domain
        self._assert_names_allowed(True, user_domain_id=self.default_id,
                                   sysmeta_project_domain_id=None,
                                   scoped=False)
        self._assert_names_allowed(True, user_domain_id=self.default_id,
                                   sysmeta_project_domain_id=self.default_id,
                                   scoped=False)
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   sysmeta_project_domain_id=UNKNOWN_ID,
                                   scoped=False)
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   sysmeta_project_domain_id='foo',
                                   scoped=False)

        # user in non-default domain so names should never be allowed
        self._assert_names_allowed(False, user_domain_id='foo',
                                   sysmeta_project_domain_id=None,
                                   scoped=False)
        self._assert_names_allowed(False, user_domain_id='foo',
                                   sysmeta_project_domain_id=self.default_id,
                                   scoped=False)
        self._assert_names_allowed(False, user_domain_id='foo',
                                   sysmeta_project_domain_id=UNKNOWN_ID,
                                   scoped=False)
        self._assert_names_allowed(False, user_domain_id='foo',
                                   sysmeta_project_domain_id='foo',
                                   scoped=False)

    def test_is_name_allowed_in_acl_with_token_scoped_to_other_tenant(self):
        # user and scoped tenant in default domain
        self._assert_names_allowed(True, user_domain_id=self.default_id,
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=None,
                                   scoped='other')
        self._assert_names_allowed(True, user_domain_id=self.default_id,
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=self.default_id,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=UNKNOWN_ID,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id='foo',
                                   scoped='other')

        # user in default domain, but scoped tenant in non-default domain
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id='foo',
                                   sysmeta_project_domain_id=None,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id='foo',
                                   sysmeta_project_domain_id=self.default_id,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id='foo',
                                   sysmeta_project_domain_id=UNKNOWN_ID,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id=self.default_id,
                                   req_project_domain_id='foo',
                                   sysmeta_project_domain_id='foo',
                                   scoped='other')

        # user in non-default domain, scoped tenant in default domain
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=None,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=self.default_id,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id=UNKNOWN_ID,
                                   scoped='other')
        self._assert_names_allowed(False, user_domain_id='foo',
                                   req_project_domain_id=self.default_id,
                                   sysmeta_project_domain_id='foo',
                                   scoped='other')


class TestIsNameAllowedInACLWithConfiguredDomain(TestIsNameAllowedInACL):
    def setUp(self):
        super(TestIsNameAllowedInACLWithConfiguredDomain, self).setUp()
        conf = {'default_domain_id': 'mydefault'}
        self.test_auth = keystoneauth.filter_factory(conf)(FakeApp())
        self.test_auth.logger = FakeLogger()
        self.default_id = 'mydefault'


class TestSetProjectDomain(BaseTestAuthorize):
    def _assert_set_project_domain(self, expected, account, req_project_id,
                                   req_project_domain_id,
                                   sysmeta_project_domain_id,
                                   warning=False):
        hdr = 'X-Account-Sysmeta-Project-Domain-Id'

        # set up fake account info in req env
        status = 0 if sysmeta_project_domain_id is None else 200
        sysmeta = {}
        if sysmeta_project_domain_id:
            sysmeta['project-domain-id'] = sysmeta_project_domain_id
        info = {'status': status, 'sysmeta': sysmeta}
        _, info_key = _get_cache_key(account, None)
        env = {info_key: info}

        # create fake env identity
        env_id = self._get_env_id(tenant_id=req_project_id,
                                  project_domain_id=req_project_domain_id)

        # reset fake logger
        self.test_auth.logger = FakeLogger()
        num_warnings = 0

        # check account requests
        path = '/v1/%s' % account
        for method in ['PUT', 'POST']:
            req = Request.blank(path, environ=env)
            req.method = method
            path_parts = req.split_path(1, 4, True)
            self.test_auth._set_project_domain_id(req, path_parts, env_id)
            if warning:
                num_warnings += 1
                warnings = self.test_auth.logger.get_lines_for_level('warning')
                self.assertEqual(len(warnings), num_warnings)
                self.assertTrue(warnings[-1].startswith('Inconsistent proj'))
            if expected is not None:
                self.assertTrue(hdr in req.headers)
                self.assertEqual(req.headers[hdr], expected)
            else:
                self.assertFalse(hdr in req.headers, req.headers)

        for method in ['GET', 'HEAD', 'DELETE', 'OPTIONS']:
            req = Request.blank(path, environ=env)
            req.method = method
            self.test_auth._set_project_domain_id(req, path_parts, env_id)
            self.assertFalse(hdr in req.headers)

        # check container requests
        path = '/v1/%s/c' % account
        for method in ['PUT']:
            req = Request.blank(path, environ=env)
            req.method = method
            path_parts = req.split_path(1, 4, True)
            self.test_auth._set_project_domain_id(req, path_parts, env_id)
            if warning:
                num_warnings += 1
                warnings = self.test_auth.logger.get_lines_for_level('warning')
                self.assertEqual(len(warnings), num_warnings)
                self.assertTrue(warnings[-1].startswith('Inconsistent proj'))
            if expected is not None:
                self.assertTrue(hdr in req.headers)
                self.assertEqual(req.headers[hdr], expected)
            else:
                self.assertFalse(hdr in req.headers)

        for method in ['POST', 'GET', 'HEAD', 'DELETE', 'OPTIONS']:
            req = Request.blank(path, environ=env)
            req.method = method
            self.test_auth._set_project_domain_id(req, path_parts, env_id)
            self.assertFalse(hdr in req.headers)

        # never set for object requests
        path = '/v1/%s/c/o' % account
        for method in ['PUT', 'COPY', 'POST', 'GET', 'HEAD', 'DELETE',
                       'OPTIONS']:
            req = Request.blank(path, environ=env)
            req.method = method
            path_parts = req.split_path(1, 4, True)
            self.test_auth._set_project_domain_id(req, path_parts, env_id)
            self.assertFalse(hdr in req.headers)

    def test_set_project_domain_id_new_account(self):
        # scoped token with project domain info
        self._assert_set_project_domain('test_id',
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id='test_id',
                                        sysmeta_project_domain_id=None)

        # scoped v2 token without project domain id
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id=None)

        # unscoped v2 token without project domain id
        self._assert_set_project_domain(UNKNOWN_ID,
                                        account='AUTH_1234',
                                        req_project_id=None,
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id=None)

        # token scoped on another project
        self._assert_set_project_domain(UNKNOWN_ID,
                                        account='AUTH_1234',
                                        req_project_id='4321',
                                        req_project_domain_id='default',
                                        sysmeta_project_domain_id=None)

    def test_set_project_domain_id_existing_v2_account(self):
        # project domain id provided in scoped request token,
        # update empty value
        self._assert_set_project_domain('default',
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id='default',
                                        sysmeta_project_domain_id='')

        # inconsistent project domain id provided in scoped request token,
        # leave known value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id='unexpected_id',
                                        sysmeta_project_domain_id='',
                                        warning=True)

        # project domain id not provided, scoped request token,
        # no change to empty value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id='')

        # unscoped request token, no change to empty value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id=None,
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id='')

        # token scoped on another project,
        # update empty value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='4321',
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id='')

    def test_set_project_domain_id_existing_account_unknown_domain(self):

        # project domain id provided in scoped request token,
        # set known value
        self._assert_set_project_domain('test_id',
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id='test_id',
                                        sysmeta_project_domain_id=UNKNOWN_ID)

        # project domain id not provided, scoped request token,
        # set empty value
        self._assert_set_project_domain('',
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id=UNKNOWN_ID)

        # project domain id not provided, unscoped request token,
        # leave unknown value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id=None,
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id=UNKNOWN_ID)

        # token scoped on another project, leave unknown value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='4321',
                                        req_project_domain_id='default',
                                        sysmeta_project_domain_id=UNKNOWN_ID)

    def test_set_project_domain_id_existing_known_domain(self):
        # project domain id provided in scoped request token,
        # leave known value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id='test_id',
                                        sysmeta_project_domain_id='test_id')

        # inconsistent project domain id provided in scoped request token,
        # leave known value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id='unexpected_id',
                                        sysmeta_project_domain_id='test_id',
                                        warning=True)

        # project domain id not provided, scoped request token,
        # leave known value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='1234',
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id='test_id')

        # project domain id not provided, unscoped request token,
        # leave known value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id=None,
                                        req_project_domain_id=None,
                                        sysmeta_project_domain_id='test_id')

        # project domain id not provided, token scoped on another project,
        # leave known value
        self._assert_set_project_domain(None,
                                        account='AUTH_1234',
                                        req_project_id='4321',
                                        req_project_domain_id='default',
                                        sysmeta_project_domain_id='test_id')


if __name__ == '__main__':
    unittest.main()
