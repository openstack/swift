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
import json

import mock
import socket
import unittest

from eventlet import Timeout
from six.moves import urllib

from swift.common.constraints import CONTAINER_LISTING_LIMIT
from swift.common.swob import Request
from swift.common.utils import ShardRange
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_container_info, Controller, \
    get_container_info
from test.unit import fake_http_connect, FakeRing, FakeMemcache, \
    make_timestamp_iter
from swift.common.storage_policy import StoragePolicy
from swift.common.request_helpers import get_sys_meta_prefix

from test.unit import patch_policies, mocked_http_conn, debug_logger
from test.unit.common.ring.test_ring import TestRingBase
from test.unit.proxy.test_server import node_error_count


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestContainerController(TestRingBase):

    CONTAINER_REPLICAS = 3

    def setUp(self):
        TestRingBase.setUp(self)
        self.logger = debug_logger()
        self.container_ring = FakeRing(replicas=self.CONTAINER_REPLICAS,
                                       max_more_nodes=9)
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
                patch_path = 'swift.proxy.controllers.base.get_account_info'
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
        self.ts_iter = make_timestamp_iter()

    def _make_callback_func(self, context):
        def callback(ipaddr, port, device, partition, method, path,
                     headers=None, query_string=None, ssl=False):
            context['method'] = method
            context['path'] = path
            context['headers'] = headers or {}
        return callback

    def _assert_responses(self, method, test_cases):
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        for responses, expected in test_cases:
            with mock.patch(
                    'swift.proxy.controllers.base.http_connect',
                    fake_http_connect(*responses)):
                req = Request.blank('/v1/a/c')
                resp = getattr(controller, method)(req)

            self.assertEqual(expected,
                             resp.status_int,
                             'Expected %s but got %s. Failed case: %s' %
                             (expected, resp.status_int, str(responses)))

    def test_container_info_got_cached(self):
        controller = proxy_server.ContainerController(self.app, 'a', 'c')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, body='')):
            req = Request.blank('/v1/a/c', {'PATH_INFO': '/v1/a/c'})
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        # Make sure it's in both swift.infocache and memcache
        self.assertIn("container/a/c", resp.environ['swift.infocache'])
        self.assertEqual(
            headers_to_container_info(resp.headers),
            resp.environ['swift.infocache']['container/a/c'])
        from_memcache = self.app.memcache.get('container/a/c')
        self.assertTrue(from_memcache)

    @mock.patch('swift.proxy.controllers.container.clear_info_cache')
    @mock.patch.object(Controller, 'make_requests')
    def test_container_cache_cleared_after_PUT(
            self, mock_make_requests, mock_clear_info_cache):
        parent_mock = mock.Mock()
        parent_mock.attach_mock(mock_make_requests, 'make_requests')
        parent_mock.attach_mock(mock_clear_info_cache, 'clear_info_cache')
        controller = proxy_server.ContainerController(self.app, 'a', 'c')
        callback = self._make_callback_func({})
        req = Request.blank('/v1/a/c')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, give_connect=callback)):
            controller.PUT(req)

        # Ensure cache is cleared after the PUT request
        self.assertEqual(parent_mock.mock_calls[0][0], 'make_requests')
        self.assertEqual(parent_mock.mock_calls[1][0], 'clear_info_cache')

    def test_swift_owner(self):
        owner_headers = {
            'x-container-read': 'value', 'x-container-write': 'value',
            'x-container-sync-key': 'value', 'x-container-sync-to': 'value'}
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        req = Request.blank('/v1/a/c')
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, headers=owner_headers)):
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        for key in owner_headers:
            self.assertNotIn(key, resp.headers)

        req = Request.blank('/v1/a/c', environ={'swift_owner': True})
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200, headers=owner_headers)):
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        for key in owner_headers:
            self.assertIn(key, resp.headers)

    def test_reseller_admin(self):
        reseller_internal_headers = {
            get_sys_meta_prefix('container') + 'sharding': 'True'}
        reseller_external_headers = {'x-container-sharding': 'on'}
        controller = proxy_server.ContainerController(self.app, 'a', 'c')

        # Normal users, even swift owners, can't set it
        req = Request.blank('/v1/a/c', method='PUT',
                            headers=reseller_external_headers,
                            environ={'swift_owner': True})
        with mocked_http_conn(*[201] * self.CONTAINER_REPLICAS) as mock_conn:
            resp = req.get_response(self.app)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_internal_headers:
            for captured in mock_conn.requests:
                self.assertNotIn(key.title(), captured['headers'])

        req = Request.blank('/v1/a/c', method='POST',
                            headers=reseller_external_headers,
                            environ={'swift_owner': True})
        with mocked_http_conn(*[204] * self.CONTAINER_REPLICAS) as mock_conn:
            resp = req.get_response(self.app)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_internal_headers:
            for captured in mock_conn.requests:
                self.assertNotIn(key.title(), captured['headers'])

        req = Request.blank('/v1/a/c', environ={'swift_owner': True})
        # Heck, they don't even get to know
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200,
                                          headers=reseller_internal_headers)):
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_external_headers:
            self.assertNotIn(key, resp.headers)

        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200,
                                          headers=reseller_internal_headers)):
            resp = controller.GET(req)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_external_headers:
            self.assertNotIn(key, resp.headers)

        # But reseller admins can set it
        req = Request.blank('/v1/a/c', method='PUT',
                            headers=reseller_external_headers,
                            environ={'reseller_request': True})
        with mocked_http_conn(*[201] * self.CONTAINER_REPLICAS) as mock_conn:
            resp = req.get_response(self.app)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_internal_headers:
            for captured in mock_conn.requests:
                self.assertIn(key.title(), captured['headers'])

        req = Request.blank('/v1/a/c', method='POST',
                            headers=reseller_external_headers,
                            environ={'reseller_request': True})
        with mocked_http_conn(*[204] * self.CONTAINER_REPLICAS) as mock_conn:
            resp = req.get_response(self.app)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_internal_headers:
            for captured in mock_conn.requests:
                self.assertIn(key.title(), captured['headers'])

        # And see that they have
        req = Request.blank('/v1/a/c', environ={'reseller_request': True})
        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200,
                                          headers=reseller_internal_headers)):
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_external_headers:
            self.assertIn(key, resp.headers)
            self.assertEqual(resp.headers[key], 'True')

        with mock.patch('swift.proxy.controllers.base.http_connect',
                        fake_http_connect(200, 200,
                                          headers=reseller_internal_headers)):
            resp = controller.GET(req)
        self.assertEqual(2, resp.status_int // 100)
        for key in reseller_external_headers:
            self.assertEqual(resp.headers[key], 'True')

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
        self.assertIn(sys_meta_key, context['headers'])
        self.assertEqual(context['headers'][sys_meta_key], 'foo')
        self.assertIn(user_meta_key, context['headers'])
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
        self.assertIn(sys_meta_key, context['headers'])
        self.assertEqual(context['headers'][sys_meta_key], 'foo')
        self.assertIn(user_meta_key, context['headers'])
        self.assertEqual(context['headers'][user_meta_key], 'bar')
        self.assertNotEqual(context['headers']['x-timestamp'], '1.0')

    def test_node_errors(self):
        self.app.sort_nodes = lambda n, *args, **kwargs: n

        for method in ('PUT', 'DELETE', 'POST'):
            def test_status_map(statuses, expected):
                self.app._error_limiting = {}
                req = Request.blank('/v1/a/c', method=method)
                with mocked_http_conn(*statuses) as fake_conn:
                    resp = req.get_response(self.app)
                self.assertEqual(resp.status_int, expected)
                for req in fake_conn.requests:
                    self.assertEqual(req['method'], method)
                    self.assertTrue(req['path'].endswith('/a/c'))

            base_status = [201] * self.CONTAINER_REPLICAS
            # test happy path
            test_status_map(list(base_status), 201)
            for i in range(self.CONTAINER_REPLICAS):
                self.assertEqual(node_error_count(
                    self.app, self.container_ring.devs[i]), 0)
            # single node errors and test isolation
            for i in range(self.CONTAINER_REPLICAS):
                test_status_map(base_status[:i] + [503] + base_status[i:], 201)
                for j in range(self.CONTAINER_REPLICAS):
                    expected = 1 if j == i else 0
                    self.assertEqual(node_error_count(
                        self.app, self.container_ring.devs[j]), expected)
            # timeout
            test_status_map(base_status[:1] + [Timeout()] + base_status[1:],
                            201)
            self.assertEqual(node_error_count(
                self.app, self.container_ring.devs[1]), 1)

            # exception
            test_status_map([Exception('kaboom!')] + base_status, 201)
            self.assertEqual(node_error_count(
                self.app, self.container_ring.devs[0]), 1)

            # insufficient storage
            test_status_map(base_status[:2] + [507] + base_status[2:], 201)
            self.assertEqual(node_error_count(
                self.app, self.container_ring.devs[2]),
                self.app.error_suppression_limit + 1)

    def test_response_codes_for_GET(self):
        nodes = self.app.container_ring.replicas
        handoffs = self.app.request_node_count(nodes) - nodes
        GET_TEST_CASES = [
            ([socket.error()] * (nodes + handoffs), 503),
            ([500] * (nodes + handoffs), 503),
            ([200], 200),
            ([404, 200], 200),
            ([404] * nodes + [200], 200),
            ([Timeout()] * nodes + [404] * handoffs, 404),
            ([Timeout()] * (nodes + handoffs), 503),
            ([Timeout()] * (nodes + handoffs - 1) + [404], 404),
            ([503, 200], 200),
            ([507, 200], 200),
        ]
        failures = []
        for case, expected in GET_TEST_CASES:
            try:
                with mocked_http_conn(*case):
                    req = Request.blank('/v1/a/c')
                    resp = req.get_response(self.app)
                    try:
                        self.assertEqual(resp.status_int, expected)
                    except AssertionError:
                        msg = '%r => %s (expected %s)' % (
                            case, resp.status_int, expected)
                        failures.append(msg)
            except AssertionError as e:
                # left over status failure
                msg = '%r => %s' % (case, e)
                failures.append(msg)
        if failures:
            self.fail('Some requests did not have expected response:\n' +
                      '\n'.join(failures))

        # One more test, simulating all nodes being error-limited
        with mocked_http_conn(), mock.patch.object(self.app, 'iter_nodes',
                                                   return_value=[]):
            req = Request.blank('/v1/a/c')
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 503)

    def test_response_code_for_PUT(self):
        PUT_TEST_CASES = [
            ((201, 201, 201), 201),
            ((201, 201, 404), 201),
            ((201, 201, 503), 201),
            ((201, 404, 404), 404),
            ((201, 404, 503), 503),
            ((201, 503, 503), 503),
            ((404, 404, 404), 404),
            ((404, 404, 503), 404),
            ((404, 503, 503), 503),
            ((503, 503, 503), 503)
        ]
        self._assert_responses('PUT', PUT_TEST_CASES)

    def test_response_code_for_DELETE(self):
        DELETE_TEST_CASES = [
            ((204, 204, 204), 204),
            ((204, 204, 404), 204),
            ((204, 204, 503), 204),
            ((204, 404, 404), 404),
            ((204, 404, 503), 503),
            ((204, 503, 503), 503),
            ((404, 404, 404), 404),
            ((404, 404, 503), 404),
            ((404, 503, 503), 503),
            ((503, 503, 503), 503)
        ]
        self._assert_responses('DELETE', DELETE_TEST_CASES)

    def test_response_code_for_POST(self):
        POST_TEST_CASES = [
            ((204, 204, 204), 204),
            ((204, 204, 404), 204),
            ((204, 204, 503), 204),
            ((204, 404, 404), 404),
            ((204, 404, 503), 503),
            ((204, 503, 503), 503),
            ((404, 404, 404), 404),
            ((404, 404, 503), 404),
            ((404, 503, 503), 503),
            ((503, 503, 503), 503)
        ]
        self._assert_responses('POST', POST_TEST_CASES)

    def _make_shard_objects(self, shard_range):
        lower = ord(shard_range.lower[0]) if shard_range.lower else ord('@')
        upper = ord(shard_range.upper[0]) if shard_range.upper else ord('z')

        objects = [{'name': chr(i), 'bytes': i, 'hash': 'hash%s' % chr(i),
                    'content_type': 'text/plain', 'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
                   for i in range(lower + 1, upper + 1)]
        return objects

    def _check_GET_shard_listing(self, responses, expected_objects,
                                 expected_requests, query_string=''):
        # responses is a list of tuples (status, json body, headers)
        # expected objects is a list of dicts
        # expected_requests is a list of tuples (path, params dict)
        container_path = '/v1/a/c' + query_string
        codes = (resp[0] for resp in responses)
        bodies = iter([json.dumps(resp[1]) for resp in responses])
        headers = [resp[2] for resp in responses]
        request = Request.blank(container_path)
        with mocked_http_conn(
                *codes, body_iter=bodies, headers=headers) as fake_conn:
            resp = request.get_response(self.app)
        for backend_req in fake_conn.requests:
            # TODO: add assertion wrt swift source, user agent
            self.assertEqual(request.headers['X-Trans-Id'],
                             backend_req['headers']['X-Trans-Id'])
        self.assertEqual(200, resp.status_int)
        actual_objects = json.loads(resp.body)
        self.assertEqual(len(expected_objects), len(actual_objects))
        self.assertEqual(expected_objects, actual_objects)
        self.assertEqual(len(expected_requests), len(fake_conn.requests))
        for i, ((path, params), req) in enumerate(
                zip(expected_requests, fake_conn.requests)):
            try:
                # strip off /sdx/0/ from path
                self.assertEqual(path, req['path'][7:])
                self.assertEqual(
                    dict(params, format='json'),
                    dict(urllib.parse.parse_qsl(req['qs'], True)))
            except AssertionError as e:
                self.fail('Request check failed at index %d: %s' % (i, e))
        return resp

    def test_GET_sharded_container(self):
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        shard_ranges = [ShardRange.create('a', 'c', lower, upper)
                        for lower, upper in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        sr_headers = [
            {'X-Backend-Sharding-State': '1',
             'X-Container-Object-Count': len(sr_objs[i]),
             'X-Container-Bytes-Used':
                 sum([obj['bytes'] for obj in sr_objs[i]]),
             'X-Container-Meta-Flavour': 'flavour%d' % i,
             'X-Backend-Storage-Policy-Index': 0}
            for i in range(3)]

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)

        # GET all objects
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects
        root_headers = {'X-Backend-Sharding-State': '3',
                        'X-Container-Object-Count': num_all_objects,
                        'X-Container-Bytes-Used': size_all_objects,
                        'X-Container-Meta-Flavour': 'peach',
                        'X-Backend-Storage-Policy-Index': 0}
        # include some failed responses
        responses = [(404, '', {}), (200, {}, root_headers)]
        responses += [(404, '', {}), (200, sr_dicts, root_headers)]
        responses += [(200, sr_objs[0], sr_headers[0])]
        responses += [(200, sr_objs[1], sr_headers[1])]
        responses += [(200, sr_objs[2], sr_headers[2])]
        expected_requests = [
            ('a/c', {}),  # 404
            ('a/c', {}),  # 200
            ('a/c', dict(items='shard', state='active')),  # 404
            ('a/c', dict(items='shard', state='active')),  # 200
            (shard_ranges[0].name,
             dict(marker='', end_marker='ham\x00', scope='root',
                  limit=str(limit))),  # 200
            (shard_ranges[1].name,
             dict(marker='ham', end_marker='pie\x00', scope='root',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (shard_ranges[2].name,
             dict(marker='pie', end_marker='', scope='root',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]
        resp = self._check_GET_shard_listing(
            responses, expected_objects, expected_requests)

        def check_response(resp):
            self.assertEqual(len(all_objects),
                             int(resp.headers['X-Container-Object-Count']))
            self.assertEqual('3', resp.headers['X-Backend-Sharding-State'])
            # check that info cache is correct for root container
            info = get_container_info(resp.request.environ, self.app)
            self.assertEqual(headers_to_container_info(root_headers), info)
        check_response(resp)

        # GET with limit param
        limit = len(sr_objs[0]) + len(sr_objs[1]) + 1
        expected_objects = all_objects[:limit]
        responses = [(404, '', {}), (200, {}, root_headers)]
        responses += [(404, '', {}), (200, sr_dicts, root_headers)]
        responses += [(200, sr_objs[0], sr_headers[0])]
        responses += [(200, sr_objs[1], sr_headers[1])]
        responses += [(200, sr_objs[2][:1], sr_headers[2])]
        expected_requests = [
            ('a/c', dict(limit=str(limit))),  # 404
            ('a/c', dict(limit=str(limit))),  # 200
            ('a/c',
             dict(items='shard', limit=str(limit), state='active')),  # 404
            ('a/c',
             dict(items='shard', limit=str(limit), state='active')),  # 200
            (shard_ranges[0].name,  # 200
             dict(marker='', end_marker='ham\x00', scope='root',
                  limit=str(limit))),
            (shard_ranges[1].name,  # 200
             dict(marker='ham', end_marker='pie\x00', scope='root',
                  limit=str(limit - len(sr_objs[0])))),
            (shard_ranges[2].name,  # 200
             dict(marker='pie', end_marker='', scope='root',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))
        ]
        self._check_GET_shard_listing(
            responses, expected_objects, expected_requests,
            query_string='?limit=%s' % limit)
        check_response(resp)

        # GET with marker
        marker = sr_objs[1][2]['name']
        first_included = len(sr_objs[0]) + 2
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects[first_included:]
        responses = [(404, '', {}), (200, {}, root_headers)]
        responses += [(404, '', {}), (200, sr_dicts[1:], root_headers)]
        responses += [(200, sr_objs[1][2:], sr_headers[1])]
        responses += [(200, sr_objs[2], sr_headers[2])]
        expected_requests = [
            ('a/c', dict(marker=marker)),  # 404
            ('a/c', dict(marker=marker)),  # 200
            ('a/c', dict(items='shard', marker=marker, state='active')),  # 404
            ('a/c', dict(items='shard', marker=marker, state='active')),  # 200
            (shard_ranges[1].name,  # 200
             dict(marker=marker, end_marker='pie\x00', scope='root',
                  limit=str(limit))),
            (shard_ranges[2].name,  # 200
             dict(marker='pie', end_marker='', scope='root',
                  limit=str(limit - len(sr_objs[1][2:])))),
        ]
        self._check_GET_shard_listing(
            responses, expected_objects, expected_requests,
            query_string='?marker=%s' % marker)
        check_response(resp)

        # GET with end marker
        end_marker = sr_objs[1][6]['name']
        first_excluded = len(sr_objs[0]) + 6
        expected_objects = all_objects[:first_excluded]
        responses = [(404, '', {}), (200, {}, root_headers)]
        responses += [(404, '', {}), (200, sr_dicts[:2], root_headers)]
        responses += [(200, sr_objs[0], sr_headers[0])]
        responses += [(200, sr_objs[1][:6], sr_headers[1])]
        expected_requests = [
            ('a/c', dict(end_marker=end_marker)),  # 404
            ('a/c', dict(end_marker=end_marker)),  # 200
            ('a/c',
             dict(items='shard', end_marker=end_marker,
                  state='active')),  # 404
            ('a/c',
             dict(items='shard', end_marker=end_marker,
                  state='active')),  # 200
            (shard_ranges[0].name,  # 200
             dict(marker='', end_marker='ham\x00', scope='root',
                  limit=str(limit))),
            (shard_ranges[1].name,  # 200
             dict(marker='ham', end_marker=end_marker, scope='root',
                  limit=str(limit - len(sr_objs[0])))),
        ]
        self._check_GET_shard_listing(
            responses, expected_objects, expected_requests,
            query_string='?end_marker=%s' % end_marker)
        check_response(resp)

        # marker and end_marker and limit
        limit = 2
        expected_objects = all_objects[first_included:first_excluded]
        responses = [(404, '', {}), (200, {}, root_headers)]
        responses += [(404, '', {}), (200, sr_dicts[1:2], root_headers)]
        responses += [(200, sr_objs[1][2:6], sr_headers[1])]
        expected_requests = [
            ('a/c',
             dict(marker=marker, end_marker=end_marker,
                  limit=str(limit))),  # 404
            ('a/c',
             dict(marker=marker, end_marker=end_marker,
                  limit=str(limit))),  # 200
            ('a/c',
             dict(items='shard', limit=str(limit), state='active',
                  marker=marker, end_marker=end_marker)),  # 404
            ('a/c',
             dict(items='shard', limit=str(limit), state='active',
                  marker=marker, end_marker=end_marker)),  # 200
            (shard_ranges[1].name,  # 200
             dict(marker=marker, end_marker=end_marker, scope='root',
                  limit=str(limit))),
        ]
        self._check_GET_shard_listing(
            responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s'
            % (marker, end_marker, limit))
        check_response(resp)

        # reverse
        expected_objects.reverse()
        responses = [(404, '', {}), (200, {}, root_headers)]
        responses += [(404, '', {}), (200, sr_dicts[1:2], root_headers)]
        responses += [(200, list(reversed(sr_objs[1][2:6])), sr_headers[1])]
        expected_requests = [
            ('a/c', dict(marker=marker, reverse='true',
                         end_marker=end_marker, limit=str(limit))),  # 404
            ('a/c', dict(marker=marker, reverse='true',
                         end_marker=end_marker, limit=str(limit))),  # 200
            ('a/c', dict(items='shard', limit=str(limit), state='active',
                         marker=marker, end_marker=end_marker,
                         reverse='true')),  # 404
            ('a/c', dict(items='shard', limit=str(limit), state='active',
                         marker=marker, end_marker=end_marker,
                         reverse='true')),  # 200
            (shard_ranges[1].name,  # 200
             dict(marker=marker, end_marker=end_marker, scope='root',
                  limit=str(limit), reverse='true')),
        ]
        self._check_GET_shard_listing(
            responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s&reverse=true'
            % (marker, end_marker, limit))
        check_response(resp)

    def test_GET_sharded_container_bad_params(self):
        # TODO: check that bad params like limit too large get handled like a
        # normal listing
        pass


@patch_policies(
    [StoragePolicy(0, 'zero', True, object_ring=FakeRing(replicas=4))])
class TestContainerController4Replicas(TestContainerController):

    CONTAINER_REPLICAS = 4

    def test_response_code_for_PUT(self):
        PUT_TEST_CASES = [
            ((201, 201, 201, 201), 201),
            ((201, 201, 201, 404), 201),
            ((201, 201, 201, 503), 201),
            ((201, 201, 404, 404), 201),
            ((201, 201, 404, 503), 201),
            ((201, 201, 503, 503), 201),
            ((201, 404, 404, 404), 404),
            ((201, 404, 404, 503), 404),
            ((201, 404, 503, 503), 503),
            ((201, 503, 503, 503), 503),
            ((404, 404, 404, 404), 404),
            ((404, 404, 404, 503), 404),
            ((404, 404, 503, 503), 404),
            ((404, 503, 503, 503), 503),
            ((503, 503, 503, 503), 503)
        ]
        self._assert_responses('PUT', PUT_TEST_CASES)

    def test_response_code_for_DELETE(self):
        DELETE_TEST_CASES = [
            ((204, 204, 204, 204), 204),
            ((204, 204, 204, 404), 204),
            ((204, 204, 204, 503), 204),
            ((204, 204, 404, 404), 204),
            ((204, 204, 404, 503), 204),
            ((204, 204, 503, 503), 204),
            ((204, 404, 404, 404), 404),
            ((204, 404, 404, 503), 404),
            ((204, 404, 503, 503), 503),
            ((204, 503, 503, 503), 503),
            ((404, 404, 404, 404), 404),
            ((404, 404, 404, 503), 404),
            ((404, 404, 503, 503), 404),
            ((404, 503, 503, 503), 503),
            ((503, 503, 503, 503), 503)
        ]
        self._assert_responses('DELETE', DELETE_TEST_CASES)

    def test_response_code_for_POST(self):
        POST_TEST_CASES = [
            ((204, 204, 204, 204), 204),
            ((204, 204, 204, 404), 204),
            ((204, 204, 204, 503), 204),
            ((204, 204, 404, 404), 204),
            ((204, 204, 404, 503), 204),
            ((204, 204, 503, 503), 204),
            ((204, 404, 404, 404), 404),
            ((204, 404, 404, 503), 404),
            ((204, 404, 503, 503), 503),
            ((204, 503, 503, 503), 503),
            ((404, 404, 404, 404), 404),
            ((404, 404, 404, 503), 404),
            ((404, 404, 503, 503), 404),
            ((404, 503, 503, 503), 503),
            ((503, 503, 503, 503), 503)
        ]
        self._assert_responses('POST', POST_TEST_CASES)


if __name__ == '__main__':
    unittest.main()
