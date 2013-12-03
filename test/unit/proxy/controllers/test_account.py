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

from swift.common.swob import Request
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_account_info
from swift.common.constraints import MAX_ACCOUNT_NAME_LENGTH as MAX_ANAME_LEN
from test.unit import fake_http_connect, FakeRing, FakeMemcache
from swift.common.request_helpers import get_sys_meta_prefix


class TestAccountController(unittest.TestCase):
    def setUp(self):
        self.app = proxy_server.Application(None, FakeMemcache(),
                                            account_ring=FakeRing(),
                                            container_ring=FakeRing(),
                                            object_ring=FakeRing())

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
        long_acct_name = '%sLongAccountName' % ('Very' * (MAX_ANAME_LEN // 4))
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


if __name__ == '__main__':
    unittest.main()
