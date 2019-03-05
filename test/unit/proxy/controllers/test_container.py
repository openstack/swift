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
from swift.common.utils import ShardRange, Timestamp
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_container_info, Controller, \
    get_container_info
from test import annotate_failure
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

    def _check_GET_shard_listing(self, mock_responses, expected_objects,
                                 expected_requests, query_string='',
                                 reverse=False):
        # mock_responses is a list of tuples (status, json body, headers)
        # expected objects is a list of dicts
        # expected_requests is a list of tuples (path, hdrs dict, params dict)

        # sanity check that expected objects is name ordered with no repeats
        def name(obj):
            return obj.get('name', obj.get('subdir'))

        for (prev, next_) in zip(expected_objects, expected_objects[1:]):
            if reverse:
                self.assertGreater(name(prev), name(next_))
            else:
                self.assertLess(name(prev), name(next_))
        container_path = '/v1/a/c' + query_string
        codes = (resp[0] for resp in mock_responses)
        bodies = iter([json.dumps(resp[1]).encode('ascii')
                       for resp in mock_responses])
        exp_headers = [resp[2] for resp in mock_responses]
        request = Request.blank(container_path)
        with mocked_http_conn(
                *codes, body_iter=bodies, headers=exp_headers) as fake_conn:
            resp = request.get_response(self.app)
        for backend_req in fake_conn.requests:
            self.assertEqual(request.headers['X-Trans-Id'],
                             backend_req['headers']['X-Trans-Id'])
            self.assertTrue(backend_req['headers']['User-Agent'].startswith(
                'proxy-server'))
        self.assertEqual(200, resp.status_int)
        actual_objects = json.loads(resp.body)
        self.assertEqual(len(expected_objects), len(actual_objects))
        self.assertEqual(expected_objects, actual_objects)
        self.assertEqual(len(expected_requests), len(fake_conn.requests))
        for i, ((exp_path, exp_headers, exp_params), req) in enumerate(
                zip(expected_requests, fake_conn.requests)):
            with annotate_failure('Request check at index %d.' % i):
                # strip off /sdx/0/ from path
                self.assertEqual(exp_path, req['path'][7:])
                self.assertEqual(
                    dict(exp_params, format='json'),
                    dict(urllib.parse.parse_qsl(req['qs'], True)))
                for k, v in exp_headers.items():
                    self.assertIn(k, req['headers'])
                    self.assertEqual(v, req['headers'][k])
                self.assertNotIn('X-Backend-Override-Delete', req['headers'])
        return resp

    def check_response(self, resp, root_resp_hdrs, expected_objects=None):
        info_hdrs = dict(root_resp_hdrs)
        if expected_objects is None:
            # default is to expect whatever the root container sent
            expected_obj_count = root_resp_hdrs['X-Container-Object-Count']
            expected_bytes_used = root_resp_hdrs['X-Container-Bytes-Used']
        else:
            expected_bytes_used = sum([o['bytes'] for o in expected_objects])
            expected_obj_count = len(expected_objects)
            info_hdrs['X-Container-Bytes-Used'] = expected_bytes_used
            info_hdrs['X-Container-Object-Count'] = expected_obj_count
        self.assertEqual(expected_bytes_used,
                         int(resp.headers['X-Container-Bytes-Used']))
        self.assertEqual(expected_obj_count,
                         int(resp.headers['X-Container-Object-Count']))
        self.assertEqual('sharded', resp.headers['X-Backend-Sharding-State'])
        for k, v in root_resp_hdrs.items():
            if k.lower().startswith('x-container-meta'):
                self.assertEqual(v, resp.headers[k])
        # check that info cache is correct for root container
        info = get_container_info(resp.request.environ, self.app)
        self.assertEqual(headers_to_container_info(info_hdrs), info)

    def test_GET_sharded_container(self):
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        shard_ranges = [
            ShardRange('.shards_a/c_%s' % upper, Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
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
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          # pretend root object stats are not yet updated
                          'X-Container-Object-Count': num_all_objects - 1,
                          'X-Container-Bytes-Used': size_all_objects - 1,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        # GET all objects
        # include some failed responses
        mock_responses = [
            # status, body, headers
            (404, '', {}),
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs,
                            expected_objects=expected_objects)

        # GET all objects - sharding, final shard range points back to root
        root_range = ShardRange('a/c', Timestamp.now(), 'pie', '')
        mock_responses = [
            # status, body, headers
            (200, sr_dicts[:2] + [dict(root_range)], root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], root_resp_hdrs)
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (root_range.name, {'X-Backend-Record-Type': 'object'},
             dict(marker='p', end_marker='',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs,
                            expected_objects=expected_objects)

        # GET all objects in reverse
        mock_responses = [
            # status, body, headers
            (200, list(reversed(sr_dicts)), root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[2])), shard_resp_hdrs[2]),
            (200, list(reversed(sr_objs[1])), shard_resp_hdrs[1]),
            (200, list(reversed(sr_objs[0])), shard_resp_hdrs[0]),
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', reverse='true')),
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='pie', reverse='true',
                  limit=str(limit), states='listing')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='q', end_marker='ham', states='listing',
                  reverse='true', limit=str(limit - len(sr_objs[2])))),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='i', end_marker='', states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[2] + sr_objs[1])))),  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(expected_objects)),
            expected_requests, query_string='?reverse=true', reverse=True)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs,
                            expected_objects=expected_objects)

        # GET with limit param
        limit = len(sr_objs[0]) + len(sr_objs[1]) + 1
        expected_objects = all_objects[:limit]
        mock_responses = [
            (404, '', {}),
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2][:1], shard_resp_hdrs[2])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(limit=str(limit), states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(limit=str(limit), states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},   # 200
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?limit=%s' % limit)
        self.check_response(resp, root_resp_hdrs)

        # GET with marker
        marker = sr_objs[1][2]['name']
        first_included = len(sr_objs[0]) + 2
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects[first_included:]
        mock_responses = [
            (404, '', {}),
            (200, sr_dicts[1:], root_shard_resp_hdrs),
            (404, '', {}),
            (200, sr_objs[1][2:], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=marker, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=marker, states='listing')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},  # 404
             dict(marker=marker, end_marker='pie\x00', states='listing',
                  limit=str(limit))),
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker=marker, end_marker='pie\x00', states='listing',
                  limit=str(limit))),
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[1][2:])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s' % marker)
        self.check_response(resp, root_resp_hdrs)

        # GET with end marker
        end_marker = sr_objs[1][6]['name']
        first_excluded = len(sr_objs[0]) + 6
        expected_objects = all_objects[:first_excluded]
        mock_responses = [
            (404, '', {}),
            (200, sr_dicts[:2], root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (404, '', {}),
            (200, sr_objs[1][:6], shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(end_marker=end_marker, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(end_marker=end_marker, states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},  # 404
             dict(marker='h', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='h', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?end_marker=%s' % end_marker)
        self.check_response(resp, root_resp_hdrs)

        # marker and end_marker and limit
        limit = 2
        expected_objects = all_objects[first_included:first_excluded]
        mock_responses = [
            (200, sr_dicts[1:2], root_shard_resp_hdrs),
            (200, sr_objs[1][2:6], shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', limit=str(limit),
                  marker=marker, end_marker=end_marker)),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker=marker, end_marker=end_marker, states='listing',
                  limit=str(limit))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s'
            % (marker, end_marker, limit))
        self.check_response(resp, root_resp_hdrs)

        # reverse with marker, end_marker
        expected_objects.reverse()
        mock_responses = [
            (200, sr_dicts[1:2], root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[1][2:6])), shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=end_marker, reverse='true', end_marker=marker,
                  limit=str(limit), states='listing',)),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker=end_marker, end_marker=marker, states='listing',
                  limit=str(limit), reverse='true')),
        ]
        self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s&reverse=true'
            % (end_marker, marker, limit), reverse=True)
        self.check_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_with_delimiter(self):
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        shard_ranges = [
            ShardRange('.shards_a/c_%s' % upper, Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        shard_resp_hdrs = {'X-Backend-Sharding-State': 'unsharded',
                           'X-Container-Object-Count': 2,
                           'X-Container-Bytes-Used': 4,
                           'X-Backend-Storage-Policy-Index': 0}

        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          # pretend root object stats are not yet updated
                          'X-Container-Object-Count': 6,
                          'X-Container-Bytes-Used': 12,
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        sr_0_obj = {'name': 'apple',
                    'bytes': 1,
                    'hash': 'hash',
                    'content_type': 'text/plain',
                    'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
        sr_2_obj = {'name': 'pumpkin',
                    'bytes': 1,
                    'hash': 'hash',
                    'content_type': 'text/plain',
                    'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
        subdir = {'subdir': 'ha/'}
        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, [sr_0_obj, subdir], shard_resp_hdrs),
            (200, [], shard_resp_hdrs),
            (200, [sr_2_obj], shard_resp_hdrs)
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', delimiter='/')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing', delimiter='/')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='ha/', end_marker='pie\x00', states='listing',
                  limit=str(limit - 2), delimiter='/')),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='ha/', end_marker='', states='listing',
                  limit=str(limit - 2), delimiter='/'))  # 200
        ]

        expected_objects = [sr_0_obj, subdir, sr_2_obj]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?delimiter=/')
        self.check_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_overlapping_shards(self):
        # verify ordered listing even if unexpected overlapping shard ranges
        shard_bounds = (('', 'ham', ShardRange.CLEAVED),
                        ('', 'pie', ShardRange.ACTIVE),
                        ('lemon', '', ShardRange.ACTIVE))
        shard_ranges = [
            ShardRange('.shards_a/c_' + upper, Timestamp.now(), lower, upper,
                       state=state)
            for lower, upper, state in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
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
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          # pretend root object stats are not yet updated
                          'X-Container-Object-Count': num_all_objects - 1,
                          'X-Container-Bytes-Used': size_all_objects - 1,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        # forwards listing

        # expect subset of second shard range
        objs_1 = [o for o in sr_objs[1] if o['name'] > sr_objs[0][-1]['name']]
        # expect subset of third shard range
        objs_2 = [o for o in sr_objs[2] if o['name'] > sr_objs[1][-1]['name']]
        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, objs_1, shard_resp_hdrs[1]),
            (200, objs_2, shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + objs_1))))  # 200
        ]

        expected_objects = sr_objs[0] + objs_1 + objs_2
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs,
                            expected_objects=expected_objects)

        # reverse listing

        # expect subset of third shard range
        objs_0 = [o for o in sr_objs[0] if o['name'] < sr_objs[1][0]['name']]
        # expect subset of second shard range
        objs_1 = [o for o in sr_objs[1] if o['name'] < sr_objs[2][0]['name']]
        mock_responses = [
            # status, body, headers
            (200, list(reversed(sr_dicts)), root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[2])), shard_resp_hdrs[2]),
            (200, list(reversed(objs_1)), shard_resp_hdrs[1]),
            (200, list(reversed(objs_0)), shard_resp_hdrs[0]),
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', reverse='true')),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='lemon', states='listing',
                  limit=str(limit),
                  reverse='true')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='m', end_marker='', reverse='true', states='listing',
                  limit=str(limit - len(sr_objs[2])))),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='A', end_marker='', reverse='true', states='listing',
                  limit=str(limit - len(sr_objs[2] + objs_1))))  # 200
        ]

        expected_objects = list(reversed(objs_0 + objs_1 + sr_objs[2]))
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?reverse=true', reverse=True)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs,
                            expected_objects=expected_objects)

    def test_GET_sharded_container_gap_in_shards(self):
        # verify ordered listing even if unexpected gap between shard ranges
        shard_bounds = (('', 'ham'), ('onion', 'pie'), ('rhubarb', ''))
        shard_ranges = [
            ShardRange('.shards_a/c_' + upper, Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
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
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, all_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_empty_shard(self):
        # verify ordered listing when a shard is empty
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        shard_ranges = [
            ShardRange('.shards_a/c_%s' % upper, Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
             'X-Container-Object-Count': len(sr_objs[i]),
             'X-Container-Bytes-Used':
                 sum([obj['bytes'] for obj in sr_objs[i]]),
             'X-Container-Meta-Flavour': 'flavour%d' % i,
             'X-Backend-Storage-Policy-Index': 0}
            for i in range(3)]
        empty_shard_resp_hdrs = {
            'X-Backend-Sharding-State': 'unsharded',
            'X-Container-Object-Count': 0,
            'X-Container-Bytes-Used': 0,
            'X-Container-Meta-Flavour': 'flavour',
            'X-Backend-Storage-Policy-Index': 0}

        # empty first shard range
        all_objects = sr_objs[1] + sr_objs[2]
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Container-Object-Count': len(all_objects),
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, [], empty_shard_resp_hdrs),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker does not advance until an object is in the listing
        limit = CONTAINER_LISTING_LIMIT
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='pie\x00', states='listing',
                  limit=str(limit))),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, sr_objs[1] + sr_objs[2], expected_requests)
        self.check_response(resp, root_resp_hdrs)

        # empty last shard range, reverse
        all_objects = sr_objs[0] + sr_objs[1]
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Container-Object-Count': len(all_objects),
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        mock_responses = [
            # status, body, headers
            (200, list(reversed(sr_dicts)), root_shard_resp_hdrs),
            (200, [], empty_shard_resp_hdrs),
            (200, list(reversed(sr_objs[1])), shard_resp_hdrs[1]),
            (200, list(reversed(sr_objs[0])), shard_resp_hdrs[0]),
        ]
        limit = CONTAINER_LISTING_LIMIT
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', reverse='true')),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='pie', states='listing',
                  limit=str(limit), reverse='true')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham', states='listing',
                  limit=str(limit), reverse='true')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker=sr_objs[1][0]['name'], end_marker='',
                  states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(sr_objs[0] + sr_objs[1])),
            expected_requests, query_string='?reverse=true', reverse=True)
        self.check_response(resp, root_resp_hdrs)

        # empty second shard range
        all_objects = sr_objs[0] + sr_objs[2]
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Container-Object-Count': len(all_objects),
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, [], empty_shard_resp_hdrs),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        limit = CONTAINER_LISTING_LIMIT
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, sr_objs[0] + sr_objs[2], expected_requests)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs)

        # marker in empty second range
        mock_responses = [
            # status, body, headers
            (200, sr_dicts[1:], root_shard_resp_hdrs),
            (200, [], empty_shard_resp_hdrs),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker unchanged when getting from third range
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', marker='koolaid')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='pie\x00', states='listing',
                  limit=str(limit))),  # 200
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='', states='listing',
             limit=str(limit)))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, sr_objs[2], expected_requests,
            query_string='?marker=koolaid')
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs)

        # marker in empty second range, reverse
        mock_responses = [
            # status, body, headers
            (200, list(reversed(sr_dicts[:2])), root_shard_resp_hdrs),
            (200, [], empty_shard_resp_hdrs),
            (200, list(reversed(sr_objs[0])), shard_resp_hdrs[2])
        ]
        # NB marker unchanged when getting from first range
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', marker='koolaid', reverse='true')),  # 200
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='ham', reverse='true',
                  states='listing', limit=str(limit))),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='', reverse='true',
                  states='listing', limit=str(limit)))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(sr_objs[0])), expected_requests,
            query_string='?marker=koolaid&reverse=true', reverse=True)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs)

    def _check_GET_sharded_container_shard_error(self, error):
        # verify ordered listing when a shard is empty
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('lemon', ''))
        shard_ranges = [
            ShardRange('.shards_a/c_%s' % upper, Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        # empty second shard range
        sr_objs[1] = []
        shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
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
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0])] + \
            [(error, [], {})] * 2 * self.CONTAINER_REPLICAS + \
            [(200, sr_objs[2], shard_resp_hdrs[2])]

        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit)))] \
            + [(shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
                dict(marker='h', end_marker='pie\x00', states='listing',
                     limit=str(limit - len(sr_objs[0]))))
               ] * 2 * self.CONTAINER_REPLICAS \
            + [(shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
                dict(marker='h', end_marker='', states='listing',
                     limit=str(limit - len(sr_objs[0] + sr_objs[1]))))]

        resp = self._check_GET_shard_listing(
            mock_responses, all_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_shard_errors(self):
        self._check_GET_sharded_container_shard_error(404)
        self._check_GET_sharded_container_shard_error(500)

    def test_GET_sharded_container_sharding_shard(self):
        # one shard is in process of sharding
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        shard_ranges = [
            ShardRange('.shards_a/c_' + upper, Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
             'X-Container-Object-Count': len(sr_objs[i]),
             'X-Container-Bytes-Used':
                 sum([obj['bytes'] for obj in sr_objs[i]]),
             'X-Container-Meta-Flavour': 'flavour%d' % i,
             'X-Backend-Storage-Policy-Index': 0}
            for i in range(3)]
        shard_1_shard_resp_hdrs = dict(shard_resp_hdrs[1])
        shard_1_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        # second shard is sharding and has cleaved two out of three sub shards
        shard_resp_hdrs[1]['X-Backend-Sharding-State'] = 'sharding'
        sub_shard_bounds = (('ham', 'juice'), ('juice', 'lemon'))
        sub_shard_ranges = [
            ShardRange('a/c_sub_' + upper, Timestamp.now(), lower, upper)
            for lower, upper in sub_shard_bounds]
        sub_sr_dicts = [dict(sr) for sr in sub_shard_ranges]
        sub_sr_objs = [self._make_shard_objects(sr) for sr in sub_shard_ranges]
        sub_shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
             'X-Container-Object-Count': len(sub_sr_objs[i]),
             'X-Container-Bytes-Used':
                 sum([obj['bytes'] for obj in sub_sr_objs[i]]),
             'X-Container-Meta-Flavour': 'flavour%d' % i,
             'X-Backend-Storage-Policy-Index': 0}
            for i in range(2)]

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sub_sr_dicts + [sr_dicts[1]], shard_1_shard_resp_hdrs),
            (200, sub_sr_objs[0], sub_shard_resp_hdrs[0]),
            (200, sub_sr_objs[1], sub_shard_resp_hdrs[1]),
            (200, sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):],
             shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # get root shard ranges
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            # get first shard objects
            (shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            # get second shard sub-shard ranges
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get first sub-shard objects
            (sub_shard_ranges[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='juice\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get second sub-shard objects
            (sub_shard_ranges[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='j', end_marker='lemon\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0])))),
            # get remainder of first shard objects
            (shard_ranges[1].name, {'X-Backend-Record-Type': 'object'},
             dict(marker='l', end_marker='pie\x00',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0] +
                                        sub_sr_objs[1])))),  # 200
            # get third shard objects
            (shard_ranges[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]
        expected_objects = (
            sr_objs[0] + sub_sr_objs[0] + sub_sr_objs[1] +
            sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):] + sr_objs[2])
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_response(resp, root_resp_hdrs)


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
