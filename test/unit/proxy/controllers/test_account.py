# Copyright (c) 2010-2012 OpenStack Foundation
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

import mock
import unittest

from swift.common.swob import Request, Response
from swift.common.middleware.acl import format_acl
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_account_info
from swift.common import constraints
from test.unit import fake_http_connect, FakeRing, FakeMemcache
from swift.common.storage_policy import StoragePolicy
from swift.common.request_helpers import get_sys_meta_prefix
import swift.proxy.controllers.base

from test.unit import patch_policies


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestAccountController(unittest.TestCase):
    def setUp(self):
        self.app = proxy_server.Application(
            None, FakeMemcache(),
            account_ring=FakeRing(), container_ring=FakeRing())

    def test_account_info_in_response_env(self):
        controller = proxy_server.AccountController(self.app, 'AUTH_bob')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, body='')):
            req = Request.blank('/v1/AUTH_bob', {'PATH_INFO': '/v1/AUTH_bob'})
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        self.assertTrue('swift.account/AUTH_bob' in resp.environ)
        self.assertEqual(headers_to_account_info(resp.headers),
                         resp.environ['swift.account/AUTH_bob'])

    def test_swift_owner(self):
        owner_headers = {
            'x-account-meta-temp-url-key': 'value',
            'x-account-meta-temp-url-key-2': 'value'}
        controller = proxy_server.AccountController(self.app, 'a')

        req = Request.blank('/v1/a')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, headers=owner_headers)):
            resp = controller.HEAD(req)
        self.assertEquals(2, resp.status_int // 100)
        for key in owner_headers:
            self.assertTrue(key not in resp.headers)

        req = Request.blank('/v1/a', environ={'swift_owner': True})
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, headers=owner_headers)):
            resp = controller.HEAD(req)
        self.assertEquals(2, resp.status_int // 100)
        for key in owner_headers:
            self.assertTrue(key in resp.headers)

    def test_get_deleted_account(self):
        resp_headers = {
            'x-account-status': 'deleted',
        }
        controller = proxy_server.AccountController(self.app, 'a')

        req = Request.blank('/v1/a')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(404, headers=resp_headers)):
            resp = controller.HEAD(req)
        self.assertEquals(410, resp.status_int)

    def test_long_acct_names(self):
        long_acct_name = '%sLongAccountName' % (
            'Very' * (constraints.MAX_ACCOUNT_NAME_LENGTH // 4))
        controller = proxy_server.AccountController(self.app, long_acct_name)

        req = Request.blank('/v1/%s' % long_acct_name)
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200)):
            resp = controller.HEAD(req)
        self.assertEquals(400, resp.status_int)

        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200)):
            resp = controller.GET(req)
        self.assertEquals(400, resp.status_int)

        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200)):
            resp = controller.POST(req)
        self.assertEquals(400, resp.status_int)

    def _make_callback_func(self, context):
        def callback(ipaddr, port, device, partition, method, path,
                     headers=None, query_string=None, ssl=False):
            context['method'] = method
            context['path'] = path
            context['headers'] = headers or {}
        return callback

    def test_sys_meta_headers_PUT(self):
        # check that headers in sys meta namespace make it through
        # the proxy controller
        sys_meta_key = '%stest' % get_sys_meta_prefix('account')
        sys_meta_key = sys_meta_key.title()
        user_meta_key = 'X-Account-Meta-Test'
        # allow PUTs to account...
        self.app.allow_account_management = True
        controller = proxy_server.AccountController(self.app, 'a')
        context = {}
        callback = self._make_callback_func(context)
        hdrs_in = {sys_meta_key: 'foo',
                   user_meta_key: 'bar',
                   'x-timestamp': '1.0'}
        req = Request.blank('/v1/a', headers=hdrs_in)
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, give_connect=callback)):
            controller.PUT(req)
        self.assertEqual(context['method'], 'PUT')
        self.assertTrue(sys_meta_key in context['headers'])
        self.assertEqual(context['headers'][sys_meta_key], 'foo')
        self.assertTrue(user_meta_key in context['headers'])
        self.assertEqual(context['headers'][user_meta_key], 'bar')
        self.assertNotEqual(context['headers']['x-timestamp'], '1.0')

    def test_sys_meta_headers_POST(self):
        # check that headers in sys meta namespace make it through
        # the proxy controller
        sys_meta_key = '%stest' % get_sys_meta_prefix('account')
        sys_meta_key = sys_meta_key.title()
        user_meta_key = 'X-Account-Meta-Test'
        controller = proxy_server.AccountController(self.app, 'a')
        context = {}
        callback = self._make_callback_func(context)
        hdrs_in = {sys_meta_key: 'foo',
                   user_meta_key: 'bar',
                   'x-timestamp': '1.0'}
        req = Request.blank('/v1/a', headers=hdrs_in)
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, give_connect=callback)):
            controller.POST(req)
        self.assertEqual(context['method'], 'POST')
        self.assertTrue(sys_meta_key in context['headers'])
        self.assertEqual(context['headers'][sys_meta_key], 'foo')
        self.assertTrue(user_meta_key in context['headers'])
        self.assertEqual(context['headers'][user_meta_key], 'bar')
        self.assertNotEqual(context['headers']['x-timestamp'], '1.0')

    def _make_user_and_sys_acl_headers_data(self):
        acl = {
            'admin': ['AUTH_alice', 'AUTH_bob'],
            'read-write': ['AUTH_carol'],
            'read-only': [],
        }
        user_prefix = 'x-account-'  # external, user-facing
        user_headers = {(user_prefix + 'access-control'): format_acl(
            version=2, acl_dict=acl)}
        sys_prefix = get_sys_meta_prefix('account')   # internal, system-facing
        sys_headers = {(sys_prefix + 'core-access-control'): format_acl(
            version=2, acl_dict=acl)}
        return user_headers, sys_headers

    def test_account_acl_headers_translated_for_GET_HEAD(self):
        # Verify that a GET/HEAD which receives X-Account-Sysmeta-Acl-* headers
        # from the account server will remap those headers to X-Account-Acl-*

        hdrs_ext, hdrs_int = self._make_user_and_sys_acl_headers_data()
        controller = proxy_server.AccountController(self.app, 'acct')

        for verb in ('GET', 'HEAD'):
            req = Request.blank('/v1/acct', environ={'swift_owner': True})
            controller.GETorHEAD_base = lambda *_: Response(
                headers=hdrs_int, environ={
                    'PATH_INFO': '/acct',
                    'REQUEST_METHOD': verb,
                })
            method = getattr(controller, verb)
            resp = method(req)
            for header, value in hdrs_ext.items():
                if value:
                    self.assertEqual(resp.headers.get(header), value)
                else:
                    # blank ACLs should result in no header
                    self.assert_(header not in resp.headers)

    def test_add_acls_impossible_cases(self):
        # For test coverage: verify that defensive coding does defend, in cases
        # that shouldn't arise naturally

        # add_acls should do nothing if REQUEST_METHOD isn't HEAD/GET/PUT/POST
        resp = Response()
        controller = proxy_server.AccountController(self.app, 'a')
        resp.environ['PATH_INFO'] = '/a'
        resp.environ['REQUEST_METHOD'] = 'OPTIONS'
        controller.add_acls_from_sys_metadata(resp)
        self.assertEqual(1, len(resp.headers))  # we always get Content-Type
        self.assertEqual(2, len(resp.environ))

    def test_memcache_key_impossible_cases(self):
        # For test coverage: verify that defensive coding does defend, in cases
        # that shouldn't arise naturally
        self.assertRaises(
            ValueError,
            lambda: swift.proxy.controllers.base.get_container_memcache_key(
                '/a', None))

    def test_stripping_swift_admin_headers(self):
        # Verify that a GET/HEAD which receives privileged headers from the
        # account server will strip those headers for non-swift_owners

        hdrs_ext, hdrs_int = self._make_user_and_sys_acl_headers_data()
        headers = {
            'x-account-meta-harmless': 'hi mom',
            'x-account-meta-temp-url-key': 's3kr1t',
        }
        controller = proxy_server.AccountController(self.app, 'acct')

        for verb in ('GET', 'HEAD'):
            for env in ({'swift_owner': True}, {'swift_owner': False}):
                req = Request.blank('/v1/acct', environ=env)
                controller.GETorHEAD_base = lambda *_: Response(
                    headers=headers, environ={
                        'PATH_INFO': '/acct',
                        'REQUEST_METHOD': verb,
                    })
                method = getattr(controller, verb)
                resp = method(req)
                self.assertEqual(resp.headers.get('x-account-meta-harmless'),
                                 'hi mom')
                privileged_header_present = (
                    'x-account-meta-temp-url-key' in resp.headers)
                self.assertEqual(privileged_header_present, env['swift_owner'])


if __name__ == '__main__':
    unittest.main()
