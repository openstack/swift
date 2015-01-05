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

from eventlet import Timeout

from swift.common.swob import Request
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_container_info
from test.unit import fake_http_connect, FakeRing, FakeMemcache
from swift.common.storage_policy import StoragePolicy
from swift.common.request_helpers import get_sys_meta_prefix

from test.unit import patch_policies, mocked_http_conn, debug_logger
from test.unit.common.ring.test_ring import TestRingBase
from test.unit.proxy.test_server import node_error_count


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestContainerController(TestRingBase):
    def setUp(self):
        TestRingBase.setUp(self)
        self.logger = debug_logger()
        self.container_ring = FakeRing(max_more_nodes=9)
        self.app = proxy_server.Application(None, FakeMemcache(),
                                            logger=self.logger,
                                            account_ring=FakeRing(),
                                            container_ring=self.container_ring)

        self.account_info = {
            'status': 200,
            'container_count': '10',
            'total_object_count': '100',
            'bytes': '1000',
            'meta': {},
            'sysmeta': {},
        }

        class FakeAccountInfoContainerController(
                proxy_server.ContainerController):

            def account_info(controller, *args, **kwargs):
                patch_path = 'swift.proxy.controllers.base.get_info'
                with mock.patch(patch_path) as mock_get_info:
                    mock_get_info.return_value = dict(self.account_info)
                    return super(FakeAccountInfoContainerController,
                                 controller).account_info(
                                     *args, **kwargs)
        _orig_get_controller = self.app.get_controller

        def wrapped_get_controller(*args, **kwargs):
            with mock.patch('swift.proxy.server.ContainerController',
                            new=FakeAccountInfoContainerController):
                return _orig_get_controller(*args, **kwargs)
        self.app.get_controller = wrapped_get_controller

    def test_container_info_in_response_env(self):
        controller = proxy_server.ContainerController(self.app, 'a', 'c')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, body='')):
            req = Request.blank('/v1/a/c', {'PATH_INFO': '/v1/a/c'})
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        self.assertTrue("swift.container/a/c" in resp.environ)
        self.assertEqual(headers_to_container_info(resp.headers),
                         resp.environ['swift.container/a/c'])

    def test_swift_owner(self):
        owner_headers = {
            'x-container-read': 'value', 'x-container-write': 'value',
            'x-container-sync-key': 'value', 'x-container-sync-to': 'value'}
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        req = Request.blank('/v1/a/c')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, headers=owner_headers)):
            resp = controller.HEAD(req)
        self.assertEquals(2, resp.status_int // 100)
        for key in owner_headers:
            self.assertTrue(key not in resp.headers)

        req = Request.blank('/v1/a/c', environ={'swift_owner': True})
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, headers=owner_headers)):
            resp = controller.HEAD(req)
        self.assertEquals(2, resp.status_int // 100)
        for key in owner_headers:
            self.assertTrue(key in resp.headers)

    def _make_callback_func(self, context):
        def callback(ipaddr, port, device, partition, method, path,
                     headers=None, query_string=None, ssl=False):
            context['method'] = method
            context['path'] = path
            context['headers'] = headers or {}
        return callback

    def test_sys_meta_headers_PUT(self):
        # check that headers in sys meta namespace make it through
        # the container controller
        sys_meta_key = '%stest' % get_sys_meta_prefix('container')
        sys_meta_key = sys_meta_key.title()
        user_meta_key = 'X-Container-Meta-Test'
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        context = {}
        callback = self._make_callback_func(context)
        hdrs_in = {sys_meta_key: 'foo',
                   user_meta_key: 'bar',
                   'x-timestamp': '1.0'}
        req = Request.blank('/v1/a/c', headers=hdrs_in)
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
        # the container controller
        sys_meta_key = '%stest' % get_sys_meta_prefix('container')
        sys_meta_key = sys_meta_key.title()
        user_meta_key = 'X-Container-Meta-Test'
        controller = proxy_server.ContainerController(self.app, 'a', 'c')
        context = {}
        callback = self._make_callback_func(context)
        hdrs_in = {sys_meta_key: 'foo',
                   user_meta_key: 'bar',
                   'x-timestamp': '1.0'}
        req = Request.blank('/v1/a/c', headers=hdrs_in)
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, give_connect=callback)):
            controller.POST(req)
        self.assertEqual(context['method'], 'POST')
        self.assertTrue(sys_meta_key in context['headers'])
        self.assertEqual(context['headers'][sys_meta_key], 'foo')
        self.assertTrue(user_meta_key in context['headers'])
        self.assertEqual(context['headers'][user_meta_key], 'bar')
        self.assertNotEqual(context['headers']['x-timestamp'], '1.0')

    def test_node_errors(self):
        self.app.sort_nodes = lambda n: n

        for method in ('PUT', 'DELETE', 'POST'):
            def test_status_map(statuses, expected):
                self.app._error_limiting = {}
                req = Request.blank('/v1/a/c', method=method)
                with mocked_http_conn(*statuses) as fake_conn:
                    print 'a' * 50
                    resp = req.get_response(self.app)
                self.assertEqual(resp.status_int, expected)
                for req in fake_conn.requests:
                    self.assertEqual(req['method'], method)
                    self.assert_(req['path'].endswith('/a/c'))

            base_status = [201] * 3
            # test happy path
            test_status_map(list(base_status), 201)
            for i in range(3):
                self.assertEqual(node_error_count(
                    self.app, self.container_ring.devs[i]), 0)
            # single node errors and test isolation
            for i in range(3):
                status_list = list(base_status)
                status_list[i] = 503
                status_list.append(201)
                test_status_map(status_list, 201)
                for j in range(3):
                    expected = 1 if j == i else 0
                    self.assertEqual(node_error_count(
                        self.app, self.container_ring.devs[j]), expected)
            # timeout
            test_status_map((201, Timeout(), 201, 201), 201)
            self.assertEqual(node_error_count(
                self.app, self.container_ring.devs[1]), 1)

            # exception
            test_status_map((Exception('kaboom!'), 201, 201, 201), 201)
            self.assertEqual(node_error_count(
                self.app, self.container_ring.devs[0]), 1)

            # insufficient storage
            test_status_map((201, 201, 507, 201), 201)
            self.assertEqual(node_error_count(
                self.app, self.container_ring.devs[2]),
                self.app.error_suppression_limit + 1)


if __name__ == '__main__':
    unittest.main()
