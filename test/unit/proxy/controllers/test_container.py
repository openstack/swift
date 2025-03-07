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

from unittest import mock
import socket
import unittest

from eventlet import Timeout
import urllib.parse
from itertools import zip_longest

from swift.common.constraints import CONTAINER_LISTING_LIMIT
from swift.common.swob import Request, bytes_to_wsgi, str_to_wsgi, wsgi_quote
from swift.common.utils import ShardRange, Timestamp, Namespace, \
    NamespaceBoundList
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_container_info, \
    Controller, get_container_info, get_cache_key
from test import annotate_failure
from test.unit import fake_http_connect, FakeRing, FakeMemcache, \
    make_timestamp_iter
from swift.common.storage_policy import StoragePolicy
from swift.common.request_helpers import get_sys_meta_prefix

from test.debug_logger import debug_logger
from test.unit import patch_policies, mocked_http_conn
from test.unit.common.ring.test_ring import TestRingBase
from test.unit.proxy.test_server import node_error_count


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class BaseTestContainerController(TestRingBase):
    CONTAINER_REPLICAS = 3

    def setUp(self):
        TestRingBase.setUp(self)
        self.logger = debug_logger()
        self.container_ring = FakeRing(replicas=self.CONTAINER_REPLICAS,
                                       max_more_nodes=9)
        self.app = proxy_server.Application(None,
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
                cache = FakeMemcache()
                cache.set(get_cache_key('a'), {'status': 204})
                req = Request.blank('/v1/a/c', environ={'swift.cache': cache})
                resp = getattr(controller, method)(req)

            self.assertEqual(expected,
                             resp.status_int,
                             'Expected %s but got %s. Failed case: %s' %
                             (expected, resp.status_int, str(responses)))


class TestContainerController(BaseTestContainerController):
    def test_container_info_got_cached(self):
        memcache = FakeMemcache()
        controller = proxy_server.ContainerController(self.app, 'a', 'c')
        with mocked_http_conn(200, 200) as mock_conn:
            req = Request.blank('/v1/a/c', {'swift.cache': memcache})
            resp = controller.HEAD(req)
        self.assertEqual(2, resp.status_int // 100)
        self.assertEqual(['/a', '/a/c'],
                         # requests are like /sdX/0/..
                         [r['path'][6:] for r in mock_conn.requests])
        # Make sure it's in both swift.infocache and memcache
        header_info = headers_to_container_info(resp.headers)
        info_cache = resp.environ['swift.infocache']
        self.assertIn("container/a/c", resp.environ['swift.infocache'])
        self.assertEqual(header_info, info_cache['container/a/c'])
        self.assertEqual(header_info, memcache.get('container/a/c'))

        # The failure doesn't lead to cache eviction
        errors = [500] * self.CONTAINER_REPLICAS * 2
        with mocked_http_conn(*errors) as mock_conn:
            req = Request.blank('/v1/a/c', {'swift.infocache': info_cache,
                                            'swift.cache': memcache})
            resp = controller.HEAD(req)
        self.assertEqual(5, resp.status_int // 100)
        self.assertEqual(['/a/c'] * self.CONTAINER_REPLICAS * 2,
                         # requests are like /sdX/0/..
                         [r['path'][6:] for r in mock_conn.requests])
        self.assertIs(info_cache, resp.environ['swift.infocache'])
        self.assertIn("container/a/c", resp.environ['swift.infocache'])
        # NB: this is the *old* header_info, from the good req
        self.assertEqual(header_info, info_cache['container/a/c'])
        self.assertEqual(header_info, memcache.get('container/a/c'))

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
                self.app.error_limiter.stats.clear()
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
                self.app.error_limiter.suppression_limit + 1)

    def test_response_codes_for_GET(self):
        nodes = self.app.container_ring.replicas
        handoffs = self.app.request_node_count(nodes) - nodes
        GET_TEST_CASES = [
            ([socket.error()] * (nodes + handoffs), 503),
            ([500] * (nodes + handoffs), 503),
            ([200], 200),
            ([404, 200], 200),
            ([404] * nodes + [200], 200),
            ([Timeout()] * nodes + [404] * handoffs, 503),
            ([Timeout()] * (nodes + handoffs), 503),
            ([Timeout()] * (nodes + handoffs - 1) + [404], 503),
            ([Timeout()] * (nodes - 1) + [404] * (handoffs + 1), 503),
            ([Timeout()] * (nodes - 2) + [404] * (handoffs + 2), 404),
            ([500] * (nodes - 1) + [404] * (handoffs + 1), 503),
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
        class FakeIter(object):
            num_primary_nodes = 3

            def __iter__(self):
                return iter([])

        with mocked_http_conn(), mock.patch(
                'swift.proxy.controllers.container.NodeIter',
                return_value=FakeIter()):
            req = Request.blank('/v1/a/c')
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 503)

    def test_handoff_has_deleted_database(self):
        nodes = self.app.container_ring.replicas
        handoffs = self.app.request_node_count(nodes) - nodes
        status = [Timeout()] * nodes + [404] * handoffs
        timestamps = tuple([None] * nodes + ['1'] + [None] * (handoffs - 1))
        with mocked_http_conn(*status, timestamps=timestamps):
            req = Request.blank('/v1/a/c')
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 404)

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

    def test_GET_bad_requests(self):
        # verify that the proxy controller enforces checks on request params
        req = Request.blank(
            '/v1/a/c?limit=%d' % (CONTAINER_LISTING_LIMIT + 1))
        self.assertEqual(412, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?delimiter=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?marker=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?end_marker=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?prefix=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?format=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?path=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?includes=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)
        req = Request.blank('/v1/a/c?states=%ff')
        self.assertEqual(400, req.get_response(self.app).status_int)


class TestGetShardedContainer(BaseTestContainerController):
    RESP_SHARD_FORMAT_HEADERS = {'X-Backend-Record-Shard-Format': 'namespace'}

    def setUp(self):
        super(TestGetShardedContainer, self).setUp()

    def _make_root_resp_hdrs(self, object_count, bytes_used, extra_hdrs=None,
                             extra_shard_hdrs=None):
        # basic headers that backend will return...
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': object_count,
                          'X-Container-Bytes-Used': bytes_used,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        if extra_hdrs:
            root_resp_hdrs.update(extra_hdrs)

        # headers returned when namespaces are returned...
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs.update(
            {'X-Backend-Record-Type': 'shard'})
        root_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)
        if extra_shard_hdrs:
            root_shard_resp_hdrs.update(extra_shard_hdrs)
        return root_resp_hdrs, root_shard_resp_hdrs

    def _make_shard_resp_hdrs(self, sr_objs, extra_hdrs=None):
        # headers returned from unsharded shard backends...
        hdrs = []
        for i, _ in enumerate(sr_objs):
            shard_hdrs = {'X-Backend-Sharding-State': 'unsharded',
                          'X-Container-Object-Count': len(sr_objs[i]),
                          'X-Container-Bytes-Used':
                              sum([obj['bytes'] for obj in sr_objs[i]]),
                          'X-Container-Meta-Flavour': 'flavour%d' % i,
                          'X-Backend-Storage-Policy-Index': 0,
                          'X-Backend-Record-Type': 'object'}
            if extra_hdrs:
                shard_hdrs.update(extra_hdrs)
            hdrs.append(shard_hdrs)
        return hdrs

    def _make_shard_objects(self, shard_range):
        lower = ord(shard_range.lower[0] if shard_range.lower else '@')
        upper = ord(shard_range.upper[0] if shard_range.upper
                    else '\U0001ffff')

        objects = [{'name': chr(i), 'bytes': i,
                    'hash': 'hash%s' % chr(i),
                    'content_type': 'text/plain', 'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
                   for i in range(lower + 1, upper + 1)][:1024]
        return objects

    def _check_GET_shard_listing(self, mock_responses, expected_objects,
                                 expected_requests, query_string='',
                                 reverse=False, expected_status=200,
                                 memcache=False, req_headers=None):
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
        if req_headers:
            request.headers.update(req_headers)
        if memcache:
            # memcache exists, which causes backend to ignore constraints and
            # reverse params for shard range GETs
            request.environ['swift.cache'] = FakeMemcache()

        with mocked_http_conn(
                *codes, body_iter=bodies, headers=exp_headers) as fake_conn:
            resp = request.get_response(self.app)
        for backend_req in fake_conn.requests:
            self.assertEqual(request.headers['X-Trans-Id'],
                             backend_req['headers']['X-Trans-Id'])
            self.assertTrue(backend_req['headers']['User-Agent'].startswith(
                'proxy-server'))
        self.assertEqual(expected_status, resp.status_int)
        if expected_status == 200:
            actual_objects = json.loads(resp.body)
            self.assertEqual(len(expected_objects), len(actual_objects))
            self.assertEqual(expected_objects, actual_objects)
            self.assertEqual(len(expected_requests), len(fake_conn.requests))
        for i, ((exp_path, exp_headers, exp_params), req) in enumerate(
                zip(expected_requests, fake_conn.requests)):
            with annotate_failure('Request check at index %d.' % i):
                # strip off /sdx/0/ from path
                self.assertEqual(exp_path, req['path'][7:])
                got_params = dict(urllib.parse.parse_qsl(
                    req['qs'], True, encoding='latin1'))
                self.assertEqual(dict(exp_params, format='json'), got_params)
                for k, v in exp_headers.items():
                    self.assertIn(k, req['headers'])
                    self.assertEqual(v, req['headers'][k], k)
                self.assertNotIn('X-Backend-Override-Delete', req['headers'])
                if memcache:
                    self.assertEqual('sharded', req['headers'].get(
                        'X-Backend-Override-Shard-Name-Filter'))
                else:
                    self.assertNotIn('X-Backend-Override-Shard-Name-Filter',
                                     req['headers'])
        return resp

    def check_listing_response(self, resp, root_resp_hdrs,
                               expected_objects=None,
                               exp_sharding_state='sharded'):
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
        self.assertEqual(exp_sharding_state,
                         resp.headers['X-Backend-Sharding-State'])
        self.assertNotIn('X-Backend-Record-Type', resp.headers)
        self.assertNotIn('X-Backend-Record-Shard-Format', resp.headers)
        for k, v in root_resp_hdrs.items():
            if k.lower().startswith('x-container-meta'):
                self.assertEqual(v, resp.headers[k])
        # check that info cache is correct for root container
        info = get_container_info(resp.request.environ, self.app)
        self.assertEqual(headers_to_container_info(info_hdrs), info)

    def create_server_namespace_dict(self, name, lower, upper):
        # return a dict representation of an instance of the type the backend
        # server returns for shard format = 'namespace'
        return dict(Namespace(name, lower, upper))

    def create_server_response_data(self, bounds, states=None,
                                    name_prefix='.shards_a/c_'):
        if not isinstance(bounds[0], (list, tuple)):
            bounds = [(l, u) for l, u in zip(bounds[:-1], bounds[1:])]
        # some tests use bounds with '/' char, so replace this before using the
        # upper bound to synthesize a valid container name
        namespaces = [Namespace(name_prefix + upper.replace('/', '-'),
                                lower, upper)
                      for lower, upper in bounds]
        ns_dicts = [dict(ns) for ns in namespaces]
        ns_objs = [self._make_shard_objects(ns) for ns in namespaces]
        return namespaces, ns_dicts, ns_objs

    def test_GET_sharded_container_no_memcache(self):
        # Don't worry, ShardRange._encode takes care of unicode/bytes issues
        shard_bounds = ('', 'ham', 'pie', u'\N{SNOWMAN}', u'\U0001F334', '')
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects
        # pretend root object stats are not yet updated
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects - 1, size_all_objects - 1)

        # GET all objects
        # include some failed responses
        mock_responses = [
            # status, body, headers
            (404, '', {}),
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2]),
            (200, sr_objs[3], shard_resp_hdrs[3]),
            (200, sr_objs[4], shard_resp_hdrs[4]),
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xd1\xb0', end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[4].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xe2\xa8\x83', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1] + sr_objs[2]
                                        + sr_objs[3])))),  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            req_headers={'X-Backend-Record-Type': 'auto'})
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            req_headers={'X-Backend-Record-Type': 'banana'})
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        # GET all objects - sharding, final shard range points back to root
        root_range = ShardRange('a/c', Timestamp.now(), 'pie', '')
        mock_responses = [
            # status, body, headers
            (200, ns_dicts[:2] + [dict(root_range)], root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2] + sr_objs[3] + sr_objs[4], root_resp_hdrs)
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing')),  # 200
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (root_range.name,
             {'X-Backend-Record-Type': 'object',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        # GET all objects in reverse and *blank* limit
        mock_responses = [
            # status, body, headers
            # NB: the backend returns reversed shard range list
            (200, list(reversed(ns_dicts)), root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[4])), shard_resp_hdrs[4]),
            (200, list(reversed(sr_objs[3])), shard_resp_hdrs[3]),
            (200, list(reversed(sr_objs[2])), shard_resp_hdrs[2]),
            (200, list(reversed(sr_objs[1])), shard_resp_hdrs[1]),
            (200, list(reversed(sr_objs[0])), shard_resp_hdrs[0]),
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', reverse='true', limit='')),
            (wsgi_quote(str_to_wsgi(namespaces[4].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='\xf0\x9f\x8c\xb4', states='listing',
                  reverse='true', limit=str(limit))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xf0\x9f\x8c\xb5', end_marker='\xe2\x98\x83',
                  states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[4])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xe2\x98\x84', end_marker='pie', states='listing',
                  reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='q', end_marker='ham', states='listing',
                  reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3]
                                        + sr_objs[2])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='i', end_marker='', states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3] + sr_objs[2]
                                        + sr_objs[1])))),  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(expected_objects)),
            expected_requests, query_string='?reverse=true&limit=',
            reverse=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        # GET with limit param
        limit = len(sr_objs[0]) + len(sr_objs[1]) + 1
        expected_objects = all_objects[:limit]
        mock_responses = [
            (404, '', {}),
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2][:1], shard_resp_hdrs[2])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(limit=str(limit), states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(limit=str(limit), states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},   # 200
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?limit=%s' % limit)
        self.check_listing_response(resp, root_resp_hdrs)

        # GET with marker
        marker = bytes_to_wsgi(sr_objs[3][2]['name'].encode('utf8'))
        first_included = (len(sr_objs[0]) + len(sr_objs[1])
                          + len(sr_objs[2]) + 2)
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects[first_included:]
        mock_responses = [
            (404, '', {}),
            (200, ns_dicts[3:], root_shard_resp_hdrs),
            (404, '', {}),
            (200, sr_objs[3][2:], shard_resp_hdrs[3]),
            (200, sr_objs[4], shard_resp_hdrs[4]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=marker, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=marker, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=marker, end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing', limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=marker, end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing', limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[4].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='\xe2\xa8\x83', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[3][2:])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s' % marker)
        self.check_listing_response(resp, root_resp_hdrs)

        # GET with end marker
        end_marker = bytes_to_wsgi(sr_objs[3][6]['name'].encode('utf8'))
        first_excluded = (len(sr_objs[0]) + len(sr_objs[1])
                          + len(sr_objs[2]) + 6)
        expected_objects = all_objects[:first_excluded]
        mock_responses = [
            (404, '', {}),
            (200, ns_dicts[:4], root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (404, '', {}),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2]),
            (404, '', {}),
            (200, sr_objs[3][:6], shard_resp_hdrs[3]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(end_marker=end_marker, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(end_marker=end_marker, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 404
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 404
             dict(marker='\xd1\xb0', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='\xd1\xb0', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?end_marker=%s' % end_marker)
        self.check_listing_response(resp, root_resp_hdrs)

        # GET with prefix
        prefix = 'hat'
        # they're all 1-character names; the important thing
        # is which shards we query
        expected_objects = []
        mock_responses = [
            (404, '', {}),
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, [], shard_resp_hdrs[1]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(prefix=prefix, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(prefix=prefix, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 404
             dict(prefix=prefix, marker='', end_marker='pie\x00',
                  states='listing', limit=str(limit))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?prefix=%s' % prefix)
        self.check_listing_response(resp, root_resp_hdrs)

        # marker and end_marker and limit
        limit = 2
        expected_objects = all_objects[first_included:first_excluded]
        mock_responses = [
            (200, ns_dicts[3:4], root_shard_resp_hdrs),
            (200, sr_objs[3][2:6], shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', limit=str(limit),
                  marker=marker, end_marker=end_marker)),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=marker, end_marker=end_marker, states='listing',
                  limit=str(limit))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s'
            % (marker, end_marker, limit))
        self.check_listing_response(resp, root_resp_hdrs)

        # reverse with marker, end_marker, and limit
        expected_objects.reverse()
        mock_responses = [
            (200, ns_dicts[3:4], root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[3][2:6])), shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=end_marker, reverse='true', end_marker=marker,
                  limit=str(limit), states='listing',)),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=end_marker, end_marker=marker, states='listing',
                  limit=str(limit), reverse='true')),
        ]
        self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s&reverse=true'
            % (end_marker, marker, limit), reverse=True)
        self.check_listing_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_with_memcache(self):
        # verify alternative code path in ContainerController when memcache is
        # available...
        shard_bounds = ('', 'ham', 'pie', u'\N{SNOWMAN}', u'\U0001F334', '')
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects
        # pretend root object stats are not yet updated
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects - 1, size_all_objects - 1,
            extra_shard_hdrs={'x-backend-override-shard-name-filter': 'true'})

        # GET all objects
        # include some failed responses
        mock_responses = [
            # status, body, headers
            (404, '', {}),
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2]),
            (200, sr_objs[3], shard_resp_hdrs[3]),
            (200, sr_objs[4], shard_resp_hdrs[4]),
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xd1\xb0', end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[4].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xe2\xa8\x83', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1] + sr_objs[2]
                                        + sr_objs[3])))),  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests, memcache=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests, memcache=True,
            req_headers={'X-Backend-Record-Type': 'auto'})
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests, memcache=True,
            req_headers={'X-Backend-Record-Type': 'banana'})
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        # GET all objects - sharding, final shard range points back to root
        root_range = ShardRange('a/c', Timestamp.now(), 'pie', '')
        mock_responses = [
            # status, body, headers
            (200, ns_dicts[:2] + [dict(root_range)], root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2] + sr_objs[3] + sr_objs[4], root_resp_hdrs)
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(states='listing')),  # 200
            (namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing')),  # 200
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (root_range.name,
             {'X-Backend-Record-Type': 'object',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests, memcache=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

        # GET all objects in reverse and *blank* limit
        mock_responses = [
            # status, body, headers
            (200, list(ns_dicts), root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[4])), shard_resp_hdrs[4]),
            (200, list(reversed(sr_objs[3])), shard_resp_hdrs[3]),
            (200, list(reversed(sr_objs[2])), shard_resp_hdrs[2]),
            (200, list(reversed(sr_objs[1])), shard_resp_hdrs[1]),
            (200, list(reversed(sr_objs[0])), shard_resp_hdrs[0]),
        ]
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(states='listing', reverse='true', limit='')),
            (wsgi_quote(str_to_wsgi(namespaces[4].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='\xf0\x9f\x8c\xb4', states='listing',
                  reverse='true', limit=str(limit))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xf0\x9f\x8c\xb5', end_marker='\xe2\x98\x83',
                  states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[4])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='\xe2\x98\x84', end_marker='pie', states='listing',
                  reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='q', end_marker='ham', states='listing',
                  reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3]
                                        + sr_objs[2])))),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='i', end_marker='', states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3] + sr_objs[2]
                                        + sr_objs[1])))),  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(expected_objects)),
            expected_requests, query_string='?reverse=true&limit=',
            reverse=True, memcache=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

        # GET with limit param
        limit = len(sr_objs[0]) + len(sr_objs[1]) + 1
        expected_objects = all_objects[:limit]
        mock_responses = [
            (404, '', {}),
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2][:1], shard_resp_hdrs[2])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(limit=str(limit), states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(limit=str(limit), states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},   # 200
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?limit=%s' % limit, memcache=True)
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

        # GET with marker
        marker = bytes_to_wsgi(sr_objs[3][2]['name'].encode('utf8'))
        first_included = (len(sr_objs[0]) + len(sr_objs[1])
                          + len(sr_objs[2]) + 2)
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects[first_included:]
        mock_responses = [
            (404, '', {}),
            # NB: proxy sent X-Backend-Override-Shard-Name-Filter so root
            # returns complete shard listing despite marker
            (200, ns_dicts, root_shard_resp_hdrs),
            (404, '', {}),
            (200, sr_objs[3][2:], shard_resp_hdrs[3]),
            (200, sr_objs[4], shard_resp_hdrs[4]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(marker=marker, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(marker=marker, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=marker, end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing', limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=marker, end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing', limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[4].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='\xe2\xa8\x83', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[3][2:])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s' % marker, memcache=True)
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

        # GET with end marker
        end_marker = bytes_to_wsgi(sr_objs[3][6]['name'].encode('utf8'))
        first_excluded = (len(sr_objs[0]) + len(sr_objs[1])
                          + len(sr_objs[2]) + 6)
        expected_objects = all_objects[:first_excluded]
        mock_responses = [
            (404, '', {}),
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (404, '', {}),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2]),
            (404, '', {}),
            (200, sr_objs[3][:6], shard_resp_hdrs[3]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(end_marker=end_marker, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(end_marker=end_marker, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 404
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(namespaces[2].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 404
             dict(marker='\xd1\xb0', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker='\xd1\xb0', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?end_marker=%s' % end_marker, memcache=True)
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

        # GET with prefix
        prefix = 'hat'
        # they're all 1-character names; the important thing
        # is which shards we query
        expected_objects = []
        mock_responses = [
            (404, '', {}),
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, [], shard_resp_hdrs[1]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(prefix=prefix, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(prefix=prefix, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[1].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 404
             dict(prefix=prefix, marker='', end_marker='pie\x00',
                  states='listing', limit=str(limit))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?prefix=%s' % prefix, memcache=True)
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

        # marker and end_marker and limit
        limit = 2
        expected_objects = all_objects[first_included:first_excluded]
        mock_responses = [
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[3][2:6], shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(states='listing', limit=str(limit),
                  marker=marker, end_marker=end_marker)),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=marker, end_marker=end_marker, states='listing',
                  limit=str(limit))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s'
            % (marker, end_marker, limit), memcache=True)
        self.check_listing_response(resp, root_resp_hdrs)

        # reverse with marker, end_marker, and limit
        expected_objects.reverse()
        mock_responses = [
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[3][2:6])), shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(marker=end_marker, reverse='true', end_marker=marker,
                  limit=str(limit), states='listing',)),  # 200
            (wsgi_quote(str_to_wsgi(namespaces[3].name)),
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},  # 200
             dict(marker=end_marker, end_marker=marker, states='listing',
                  limit=str(limit), reverse='true')),
        ]
        self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s&reverse=true'
            % (end_marker, marker, limit), reverse=True, memcache=True)
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

    def _do_test_GET_sharded_container_with_deleted_shards(self, shard_specs):
        # verify that if a shard fails to return its listing component then the
        # client response is 503
        shard_bounds = (('a', 'b'), ('b', 'c'), ('c', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)

        # pretend root object stats are not yet updated
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(6, 12)
        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
        ]
        for i, spec in enumerate(shard_specs):
            if spec == 200:
                mock_responses.append((200, sr_objs[i], shard_resp_hdrs[i]))
            else:
                mock_responses.extend(
                    [(spec, '', {})] * 2 * self.CONTAINER_REPLICAS)

        codes = (resp[0] for resp in mock_responses)
        bodies = iter([json.dumps(resp[1]).encode('ascii')
                       for resp in mock_responses])
        exp_headers = [resp[2] for resp in mock_responses]
        request = Request.blank('/v1/a/c')
        with mocked_http_conn(
                *codes, body_iter=bodies, headers=exp_headers) as fake_conn:
            resp = request.get_response(self.app)
        self.assertEqual(len(mock_responses), len(fake_conn.requests))
        return request, resp

    def test_GET_sharded_container_with_deleted_shard(self):
        req, resp = self._do_test_GET_sharded_container_with_deleted_shards(
            [404])
        warning_lines = self.app.logger.get_lines_for_level('warning')
        start = 'Failed to get container auto listing from /v1/.shards_a/c_b?'
        msg, _, status_txn = warning_lines[0].partition(': ')
        self.assertEqual(start, msg[:len(start)])
        actual_qs = msg[len(start):]
        actual_params = dict(
            urllib.parse.parse_qsl(actual_qs, keep_blank_values=True))
        self.assertEqual({'format': 'json',
                          'limit': '10000',
                          'marker': '',
                          'end_marker': 'b\x00',
                          'states': 'listing'},
                         actual_params)
        self.assertEqual('404', status_txn[:3])
        self.assertFalse(warning_lines[1:])
        self.assertEqual(resp.status_int, 503)
        errors = self.logger.get_lines_for_level('error')
        self.assertEqual(
            ['Aborting listing from shards due to bad response: %s'
             % ([404])], errors)

    def test_GET_sharded_container_with_mix_ok_and_deleted_shard(self):
        req, resp = self._do_test_GET_sharded_container_with_deleted_shards(
            [200, 200, 404])
        warning_lines = self.app.logger.get_lines_for_level('warning')
        start = 'Failed to get container auto listing from /v1/.shards_a/c_?'
        msg, _, status_txn = warning_lines[0].partition(': ')
        self.assertEqual(start, msg[:len(start)])
        actual_qs = msg[len(start):]
        actual_params = dict(
            urllib.parse.parse_qsl(actual_qs, keep_blank_values=True))
        self.assertEqual({'format': 'json',
                          'limit': '9998',
                          'marker': 'c',
                          'end_marker': '',
                          'states': 'listing'},
                         actual_params)
        self.assertEqual('404', status_txn[:3])
        self.assertFalse(warning_lines[1:])
        self.assertEqual(resp.status_int, 503)
        errors = self.logger.get_lines_for_level('error')
        self.assertEqual(
            ['Aborting listing from shards due to bad response: %s'
             % ([200, 200, 404],)], errors)

    def test_GET_sharded_container_mix_ok_and_unavailable_shards(self):
        req, resp = self._do_test_GET_sharded_container_with_deleted_shards(
            [200, 200, 503])
        warning_lines = self.app.logger.get_lines_for_level('warning')
        start = 'Failed to get container auto listing from /v1/.shards_a/c_?'
        msg, _, status_txn = warning_lines[0].partition(': ')
        self.assertEqual(start, msg[:len(start)])
        actual_qs = msg[len(start):]
        actual_params = dict(
            urllib.parse.parse_qsl(actual_qs, keep_blank_values=True))
        self.assertEqual({'format': 'json',
                          'limit': '9998',
                          'marker': 'c',
                          'end_marker': '',
                          'states': 'listing'},
                         actual_params)
        self.assertEqual('503', status_txn[:3])
        self.assertFalse(warning_lines[1:])
        self.assertEqual(resp.status_int, 503)
        errors = self.logger.get_lines_for_level('error')
        self.assertEqual(
            ['Aborting listing from shards due to bad response: %s'
             % ([200, 200, 503],)], errors[-1:])

    def test_GET_sharded_container_marker_beyond_end_marker_memcache(self):
        # verify that if request params result in the filtered namespaces list
        # being empty the response body still has an empty object list
        shard_bounds = (('a', 'b'), ('b', 'c'), ('c', ''))
        namespaces, ns_dicts, _ = self.create_server_response_data(
            shard_bounds)
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            6, 12, extra_shard_hdrs={
                'x-backend-override-shard-name-filter': 'true'})

        # NB: root returns complete shard listing
        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', marker='bb', end_marker='aa')),  # 200
        ]
        expected_objects = []
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=bb&end_marker=aa', memcache=True)
        self.check_listing_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_with_delimiter_no_memcache(self):
        shard_bounds = (('', 'ha/ppy'), ('ha/ppy', 'ha/ptic'),
                        ('ha/ptic', 'ham'), ('ham', 'pie'), ('pie', ''))
        namespaces, ns_dicts, _ = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = {'X-Backend-Sharding-State': 'unsharded',
                           'X-Container-Object-Count': 2,
                           'X-Container-Bytes-Used': 4,
                           'X-Backend-Storage-Policy-Index': 0}

        limit = CONTAINER_LISTING_LIMIT
        # pretend root object stats are not yet updated
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(6, 12)

        sr_0_obj = {'name': 'apple',
                    'bytes': 1,
                    'hash': 'hash',
                    'content_type': 'text/plain',
                    'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
        sr_5_obj = {'name': 'pumpkin',
                    'bytes': 1,
                    'hash': 'hash',
                    'content_type': 'text/plain',
                    'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
        subdir = {'subdir': 'ha/'}
        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, [sr_0_obj, subdir], shard_resp_hdrs),
            (200, [], shard_resp_hdrs),
            (200, [], shard_resp_hdrs),
            (200, [sr_5_obj], shard_resp_hdrs)
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', delimiter='/')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ha/ppy\x00', limit=str(limit),
                  states='listing', delimiter='/')),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='ha/', end_marker='ham\x00', states='listing',
                  limit=str(limit - 2), delimiter='/')),  # 200
            (namespaces[3].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='ha/', end_marker='pie\x00', states='listing',
                  limit=str(limit - 2), delimiter='/')),  # 200
            (namespaces[4].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='ha/', end_marker='', states='listing',
                  limit=str(limit - 2), delimiter='/')),  # 200
        ]

        expected_objects = [sr_0_obj, subdir, sr_5_obj]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?delimiter=/')
        self.check_listing_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_with_delimiter_reverse_no_memcache(self):
        shard_bounds = ('', 'ha.d', 'ha/ppy', 'ha/ptic', 'ham', 'pie', '')
        namespaces, ns_dicts, _ = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = {'X-Backend-Sharding-State': 'unsharded',
                           'X-Container-Object-Count': 2,
                           'X-Container-Bytes-Used': 4,
                           'X-Backend-Storage-Policy-Index': 0}

        limit = CONTAINER_LISTING_LIMIT
        # pretend root object stats are not yet updated
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(6, 12)

        sr_0_obj = {'name': 'apple',
                    'bytes': 1,
                    'hash': 'hash',
                    'content_type': 'text/plain',
                    'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
        sr_1_obj = {'name': 'ha.ggle',
                    'bytes': 1,
                    'hash': 'hash',
                    'content_type': 'text/plain',
                    'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
        sr_5_obj = {'name': 'pumpkin',
                    'bytes': 1,
                    'hash': 'hash',
                    'content_type': 'text/plain',
                    'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
        subdir = {'subdir': 'ha/'}
        mock_responses = [
            # status, body, headers
            (200, list(reversed(ns_dicts)), root_shard_resp_hdrs),
            (200, [sr_5_obj], shard_resp_hdrs),
            (200, [], shard_resp_hdrs),
            (200, [subdir], shard_resp_hdrs),
            (200, [sr_1_obj], shard_resp_hdrs),
            (200, [sr_0_obj], shard_resp_hdrs),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', delimiter='/', reverse='on')),  # 200
            (namespaces[5].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='pie', states='listing',
                  limit=str(limit), delimiter='/', reverse='on')),  # 200
            (namespaces[4].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='pumpkin', end_marker='ham', states='listing',
                  limit=str(limit - 1), delimiter='/', reverse='on')),  # 200
            (namespaces[3].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='pumpkin', end_marker='ha/ptic', states='listing',
                  limit=str(limit - 1), delimiter='/', reverse='on')),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='ha/', end_marker='ha.d', limit=str(limit - 2),
                  states='listing', delimiter='/', reverse='on')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='ha.ggle', end_marker='', limit=str(limit - 3),
                  states='listing', delimiter='/', reverse='on')),  # 200
        ]

        expected_objects = [sr_5_obj, subdir, sr_1_obj, sr_0_obj]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?delimiter=/&reverse=on', reverse=True)
        self.check_listing_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_shard_redirects_to_root_no_memcache(self):
        # check that if the root redirects listing to a shard, but the shard
        # returns the root shard (e.g. it was the final shard to shrink into
        # the root) objects are requested from the root, rather than a loop.

        # single shard spanning entire namespace
        shard_bounds = ('', '')
        namespaces, _, sr_objs = self.create_server_response_data(
            shard_bounds)
        all_objects = sr_objs[0]
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT

        # when shrinking the final shard will return the root shard range into
        # which it is shrinking
        shard_resp_hdrs = {
            'X-Backend-Sharding-State': 'sharding',
            'X-Container-Object-Count': 0,
            'X-Container-Bytes-Used': 0,
            'X-Backend-Storage-Policy-Index': 0,
            'X-Backend-Record-Type': 'shard'
        }

        # root still thinks it has a shard
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects, size_all_objects)

        root_sr_dict = self.create_server_namespace_dict('a/c', '', '')
        mock_responses = [
            # status, body, headers
            (200, [dict(namespaces[0])], root_shard_resp_hdrs),  # from root
            (200, [root_sr_dict], shard_resp_hdrs),  # from shard
            (200, all_objects, root_resp_hdrs),  # from root
        ]
        expected_requests = [
            # path, headers, params
            # first request to root should specify auto record type
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),
            # request to shard should specify auto record type
            (wsgi_quote(str_to_wsgi(namespaces[0].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='', limit=str(limit),
                  states='listing')),  # 200
            # second request to root should specify object record type
            ('a/c', {'X-Backend-Record-Type': 'object'},
             dict(marker='', end_marker='', limit=str(limit))),  # 200
        ]

        expected_objects = all_objects
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)
        self.assertEqual(
            [('a', 'c'), ('.shards_a', 'c_')],
            resp.request.environ.get('swift.shard_listing_history'))
        lines = [line for line in self.app.logger.get_lines_for_level('debug')
                 if line.startswith('Found 1024 objects in shard')]
        self.assertEqual(2, len(lines), lines)
        self.assertIn("(state=sharded), total = 1024", lines[0])  # shard->root
        self.assertIn("(state=sharding), total = 1024", lines[1])  # shard

    def test_GET_sharded_container_shard_redirects_between_shards(self):
        # check that if one shard redirects listing to another shard that
        # somehow redirects listing back to the first shard, then we will break
        # out of the loop (this isn't an expected scenario, but could perhaps
        # happen if multiple conflicting shard-shrinking decisions are made)
        shard_bounds = ('', 'a', 'b', '')
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        self.assertEqual([
            '.shards_a/c_a',
            '.shards_a/c_b',
            '.shards_a/c_',
        ], [sr.name for sr in namespaces])
        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)

        # pretend root object stats are not yet updated
        _, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects, size_all_objects)

        shard_resp_hdrs = {'X-Backend-Sharding-State': 'unsharded',
                           'X-Container-Object-Count': 2,
                           'X-Container-Bytes-Used': 4,
                           'X-Backend-Storage-Policy-Index': 0,
                           'X-Backend-Record-Storage-Policy-Index': 0,
                           }
        shrinking_resp_hdrs = {
            'X-Backend-Sharding-State': 'sharded',
            'X-Backend-Record-Type': 'shard',
            'X-Backend-Storage-Policy-Index': 0
        }
        shrinking_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)
        limit = CONTAINER_LISTING_LIMIT

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),  # from root
            (200, sr_objs[0], shard_resp_hdrs),  # objects from 1st shard
            (200, [ns_dicts[2]], shrinking_resp_hdrs),  # 2nd points to 3rd
            (200, [ns_dicts[1]], shrinking_resp_hdrs),  # 3rd points to 2nd
            (200, sr_objs[1], shard_resp_hdrs),  # objects from 2nd
            (200, sr_objs[2], shard_resp_hdrs),  # objects from 3rd
        ]
        expected_requests = [
            # each list item is tuple (path, headers, params)
            # request to root
            # context GET(a/c)
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),
            # request to 1st shard as per shard list from root;
            # context GET(a/c);
            # end_marker dictated by 1st shard range upper bound
            ('.shards_a/c_a', {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='a\x00', states='listing',
                  limit=str(limit))),  # 200
            # request to 2nd shard as per shard list from root;
            # context GET(a/c);
            # end_marker dictated by 2nd shard range upper bound
            ('.shards_a/c_b', {'X-Backend-Record-Type': 'auto'},
             dict(marker='a', end_marker='b\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # request to 3rd shard as per shard list from *2nd shard*;
            # new context GET(a/c)->GET(.shards_a/c_b);
            # end_marker still dictated by 2nd shard range upper bound
            ('.shards_a/c_', {'X-Backend-Record-Type': 'auto'},
             dict(marker='a', end_marker='b\x00', states='listing',
                  limit=str(
                      limit - len(sr_objs[0])))),
            # request to 2nd shard as per shard list from *3rd shard*; this one
            # should specify record type object;
            # new context GET(a/c)->GET(.shards_a/c_b)->GET(.shards_a/c_);
            # end_marker still dictated by 2nd shard range upper bound
            ('.shards_a/c_b', {'X-Backend-Record-Type': 'object'},
             dict(marker='a', end_marker='b\x00',
                  limit=str(
                      limit - len(sr_objs[0])))),
            # request to 3rd shard *as per shard list from root*; this one
            # should specify record type object;
            # context GET(a/c);
            # end_marker dictated by 3rd shard range upper bound
            ('.shards_a/c_', {'X-Backend-Record-Type': 'object'},
             dict(marker='b', end_marker='',
                  limit=str(
                      limit - len(sr_objs[0]) - len(sr_objs[1])))),  # 200
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, all_objects, expected_requests)
        self.check_listing_response(resp, root_shard_resp_hdrs,
                                    expected_objects=all_objects)
        self.assertEqual(
            [('a', 'c'), ('.shards_a', 'c_b'), ('.shards_a', 'c_')],
            resp.request.environ.get('swift.shard_listing_history'))

    def test_GET_sharded_container_overlapping_shards_no_memcache(self):
        # verify ordered listing even if unexpected overlapping shard ranges
        shard_bounds = (('', 'ham'), ('', 'pie'), ('lemon', ''))
        shard_states = (ShardRange.CLEAVED, ShardRange.ACTIVE,
                        ShardRange.ACTIVE)
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds, states=shard_states)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        # pretend root object stats are not yet updated
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects - 1, size_all_objects - 1)

        # forwards listing

        # expect subset of second shard range
        objs_1 = [o for o in sr_objs[1] if o['name'] > sr_objs[0][-1]['name']]
        # expect subset of third shard range
        objs_2 = [o for o in sr_objs[2] if o['name'] > sr_objs[1][-1]['name']]
        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, objs_1, shard_resp_hdrs[1]),
            (200, objs_2, shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + objs_1))))  # 200
        ]

        expected_objects = sr_objs[0] + objs_1 + objs_2
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

        # reverse listing

        # expect subset of third shard range
        objs_0 = [o for o in sr_objs[0] if o['name'] < sr_objs[1][0]['name']]
        # expect subset of second shard range
        objs_1 = [o for o in sr_objs[1] if o['name'] < sr_objs[2][0]['name']]
        mock_responses = [
            # status, body, headers
            (200, list(reversed(ns_dicts)), root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[2])), shard_resp_hdrs[2]),
            (200, list(reversed(objs_1)), shard_resp_hdrs[1]),
            (200, list(reversed(objs_0)), shard_resp_hdrs[0]),
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', reverse='true')),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='lemon', states='listing',
                  limit=str(limit),
                  reverse='true')),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='m', end_marker='', reverse='true', states='listing',
                  limit=str(limit - len(sr_objs[2])))),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='A', end_marker='', reverse='true', states='listing',
                  limit=str(limit - len(sr_objs[2] + objs_1))))  # 200
        ]

        expected_objects = list(reversed(objs_0 + objs_1 + sr_objs[2]))
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?reverse=true', reverse=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    expected_objects=expected_objects)

    def test_GET_sharded_container_gap_in_shards_no_memcache(self):
        # verify ordered listing even if unexpected gap between shard ranges
        shard_bounds = (('', 'ham'), ('onion', 'pie'), ('rhubarb', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        # pretend root object stats are not yet updated
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects, size_all_objects)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, all_objects, expected_requests, memcache=False)
        # root object count will be overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertNotIn('swift.cache', resp.request.environ)

    def test_GET_sharding_container_gap_in_shards_with_memcache(self):
        # verify ordered listing even if unexpected gap between shard ranges;
        # root is sharding so shard ranges are not cached
        shard_bounds = (('', 'ham'), ('onion', 'pie'), ('rhubarb', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects, size_all_objects,
            extra_hdrs={'X-Backend-Sharding-State': 'sharding'})

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        # NB end_markers are upper of the current available shard range
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, all_objects, expected_requests, memcache=True)
        # root object count will be overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs,
                                    exp_sharding_state='sharding')
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertFalse(cached_keys)

    def test_GET_sharded_container_gap_in_shards_with_memcache(self):
        # verify ordered listing even if unexpected gap between shard ranges
        shard_bounds = (('', 'ham'), ('onion', 'pie'), ('rhubarb', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects, size_all_objects,
            extra_shard_hdrs={'x-backend-override-shard-name-filter': 'true'})

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sr_objs[1], shard_resp_hdrs[1]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        # NB compaction of shard range data to cached bounds loses the gaps, so
        # end_markers are lower of the next available shard range
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='onion\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='rhubarb\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, all_objects, expected_requests, memcache=True)
        # root object count will be overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)
        # NB compact bounds in cache do not reveal the gap in shard ranges
        self.assertEqual(
            [['', '.shards_a/c_ham'],
             ['onion', '.shards_a/c_pie'],
             ['rhubarb', '.shards_a/c_']],
            resp.request.environ['swift.cache'].store['shard-listing-v2/a/c'])

    def test_GET_sharded_container_empty_shard_no_memcache(self):
        # verify ordered listing when a shard is empty
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)
        empty_shard_resp_hdrs = {
            'X-Backend-Sharding-State': 'unsharded',
            'X-Container-Object-Count': 0,
            'X-Container-Bytes-Used': 0,
            'X-Container-Meta-Flavour': 'flavour',
            'X-Backend-Storage-Policy-Index': 0}

        # empty first shard range
        all_objects = sr_objs[1] + sr_objs[2]
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects, size_all_objects)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
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
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='pie\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, sr_objs[1] + sr_objs[2], expected_requests)
        self.check_listing_response(resp, root_resp_hdrs)

        # empty last shard range, reverse
        all_objects = sr_objs[0] + sr_objs[1]
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': len(all_objects),
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        root_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        mock_responses = [
            # status, body, headers
            (200, list(reversed(ns_dicts)), root_shard_resp_hdrs),
            (200, [], empty_shard_resp_hdrs),
            (200, list(reversed(sr_objs[1])), shard_resp_hdrs[1]),
            (200, list(reversed(sr_objs[0])), shard_resp_hdrs[0]),
        ]
        limit = CONTAINER_LISTING_LIMIT
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', reverse='true')),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='pie', states='listing',
                  limit=str(limit), reverse='true')),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham', states='listing',
                  limit=str(limit), reverse='true')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker=sr_objs[1][0]['name'], end_marker='',
                  states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[1]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(sr_objs[0] + sr_objs[1])),
            expected_requests, query_string='?reverse=true', reverse=True)
        self.check_listing_response(resp, root_resp_hdrs)

        # empty second shard range
        all_objects = sr_objs[0] + sr_objs[2]
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': len(all_objects),
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        root_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
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
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0]))))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, sr_objs[0] + sr_objs[2], expected_requests)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)

        # marker in empty second range
        mock_responses = [
            # status, body, headers
            (200, ns_dicts[1:], root_shard_resp_hdrs),
            (200, [], empty_shard_resp_hdrs),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker unchanged when getting from third range
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', marker='koolaid')),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='pie\x00', states='listing',
                  limit=str(limit))),  # 200
            (namespaces[2].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='', states='listing',
             limit=str(limit)))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, sr_objs[2], expected_requests,
            query_string='?marker=koolaid')
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)

        # marker in empty second range, reverse
        mock_responses = [
            # status, body, headers
            (200, list(reversed(ns_dicts[:2])), root_shard_resp_hdrs),
            (200, [], empty_shard_resp_hdrs),
            (200, list(reversed(sr_objs[0])), shard_resp_hdrs[2])
        ]
        # NB marker unchanged when getting from first range
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', marker='koolaid', reverse='true')),  # 200
            (namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='ham', reverse='true',
                  states='listing', limit=str(limit))),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='koolaid', end_marker='', reverse='true',
                  states='listing', limit=str(limit)))  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(sr_objs[0])), expected_requests,
            query_string='?marker=koolaid&reverse=true', reverse=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)

    def _check_GET_sharded_container_shard_error(self, error):
        # verify ordered listing when a shard is empty
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('lemon', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        # empty second shard range
        sr_objs[1] = []
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
            num_all_objects, size_all_objects)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0])] + \
            [(error, [], {})] * 2 * self.CONTAINER_REPLICAS

        # NB marker always advances to last object name
        expected_requests = [
            # path, headers, params
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            (namespaces[0].name, {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit)))] \
            + [(namespaces[1].name, {'X-Backend-Record-Type': 'auto'},
                dict(marker='h', end_marker='pie\x00', states='listing',
                     limit=str(limit - len(sr_objs[0]))))
               ] * 2 * self.CONTAINER_REPLICAS

        self._check_GET_shard_listing(
            mock_responses, all_objects, expected_requests,
            expected_status=503)

    def test_GET_sharded_container_shard_errors_no_memcache(self):
        self._check_GET_sharded_container_shard_error(404)
        self._check_GET_sharded_container_shard_error(500)

    def test_GET_sharded_container_sharding_shard_no_memcache(self):
        # one shard is in process of sharding
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        # headers returned with obj listing from shard containers...
        shard_obj_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)
        # modify second shard's obj listing resp - this one is sharding...
        shard_obj_resp_hdrs[1]['X-Backend-Sharding-State'] = 'sharding'
        # ...and will return shards in 'response' to auto record-type...
        shard_1_shard_resp_hdrs = dict(shard_obj_resp_hdrs[1])
        shard_1_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        shard_1_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        # second shard is sharding and has cleaved two out of three sub shards
        sub_shard_bounds = (('ham', 'juice'), ('juice', 'lemon'))
        sub_namespaces, sub_ns_dicts, sub_sr_objs = \
            self.create_server_response_data(sub_shard_bounds,
                                             name_prefix='a/c_sub_')
        filler_sr_dict = self.create_server_namespace_dict(
            namespaces[1].name, lower=sub_ns_dicts[-1]['upper'],
            upper=namespaces[1].upper)
        sub_sr_objs = [self._make_shard_objects(sr) for sr in sub_namespaces]
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
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        # headers returned with root response to auto record-type listing...
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        root_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_obj_resp_hdrs[0]),
            (200, sub_ns_dicts + [filler_sr_dict], shard_1_shard_resp_hdrs),
            (200, sub_sr_objs[0], sub_shard_resp_hdrs[0]),
            (200, sub_sr_objs[1], sub_shard_resp_hdrs[1]),
            (200, sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):],
             shard_obj_resp_hdrs[1]),
            (200, sr_objs[2], shard_obj_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # get root shard ranges
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            # get first shard objects
            (namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            # get second shard sub-shard ranges
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get first sub-shard objects
            (sub_namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='juice\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get second sub-shard objects
            (sub_namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='j', end_marker='lemon\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0])))),
            # get remainder of first shard objects
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'object',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='l', end_marker='pie\x00',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0] +
                                        sub_sr_objs[1])))),  # 200
            # get third shard objects
            (namespaces[2].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]
        expected_objects = (
            sr_objs[0] + sub_sr_objs[0] + sub_sr_objs[1] +
            sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):] + sr_objs[2])
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)

    def test_GET_sharded_container_sharding_shard_with_memcache(self):
        # one shard is in process of sharding
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        # headers returned with obj listing from shard containers...
        shard_obj_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)
        # modify second shard's obj listing resp - this one is sharding...
        shard_obj_resp_hdrs[1]['X-Backend-Sharding-State'] = 'sharding'
        # ...and will return shards in 'response' to auto record-type...
        shard_1_shard_resp_hdrs = dict(shard_obj_resp_hdrs[1])
        shard_1_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        shard_1_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        # second shard is sharding and has cleaved two out of three sub shards
        sub_shard_bounds = (('ham', 'juice'), ('juice', 'lemon'))
        sub_namespaces, sub_ns_dicts, sub_sr_objs = \
            self.create_server_response_data(sub_shard_bounds,
                                             name_prefix='a/c_sub_')
        filler_sr_dict = self.create_server_namespace_dict(
            namespaces[1].name, lower=sub_ns_dicts[-1]['upper'],
            upper=namespaces[1].upper)
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
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        # headers returned with root response to auto record-type listing...
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        root_shard_resp_hdrs['X-Backend-Override-Shard-Name-Filter'] = 'true'
        root_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_obj_resp_hdrs[0]),
            (200, sub_ns_dicts + [filler_sr_dict], shard_1_shard_resp_hdrs),
            (200, sub_sr_objs[0], sub_shard_resp_hdrs[0]),
            (200, sub_sr_objs[1], sub_shard_resp_hdrs[1]),
            (200, sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):],
             shard_obj_resp_hdrs[1]),
            (200, sr_objs[2], shard_obj_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # get root shard ranges
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(states='listing')),  # 200
            # get first shard objects
            (namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            # get second shard sub-shard ranges
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get first sub-shard objects
            (sub_namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='juice\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get second sub-shard objects
            (sub_namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='j', end_marker='lemon\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0])))),
            # get remainder of first shard objects (filler shard range)
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'object',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='l', end_marker='pie\x00',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0] +
                                        sub_sr_objs[1])))),  # 200
            # get third shard objects
            (namespaces[2].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]
        expected_objects = (
            sr_objs[0] + sub_sr_objs[0] + sub_sr_objs[1] +
            sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):] + sr_objs[2])
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests, memcache=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        # sub-shards are not cached because the shard is still 'sharding'...
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual({'shard-listing-v2/a/c'}, cached_keys)

    def test_GET_sharded_container_sharded_shard_with_memcache(self):
        # one shard is sharded but still in shard listing returned by root
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_resp_hdrs = self._make_shard_resp_hdrs(sr_objs)
        shard_1_shard_resp_hdrs = dict(shard_resp_hdrs[1])
        shard_1_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        shard_1_shard_resp_hdrs['X-Backend-Sharding-State'] = 'sharded'
        shard_1_shard_resp_hdrs[
            'X-Backend-Override-Shard-Name-Filter'] = 'true'
        shard_1_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        # second shard is sharded and has cleaved three sub shards
        sub_shard_bounds = (('ham', 'juice'), ('juice', 'lemon'),
                            ('lemon', 'pie'))
        sub_namespaces, sub_ns_dicts, sub_sr_objs = \
            self.create_server_response_data(sub_shard_bounds,
                                             name_prefix='a/c_sub_')
        sub_shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
             'X-Container-Object-Count': len(sub_sr_objs[i]),
             'X-Container-Bytes-Used':
                 sum([obj['bytes'] for obj in sub_sr_objs[i]]),
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
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        root_shard_resp_hdrs['X-Backend-Override-Shard-Name-Filter'] = 'true'
        root_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_resp_hdrs[0]),
            (200, sub_ns_dicts, shard_1_shard_resp_hdrs),
            (200, sub_sr_objs[0], sub_shard_resp_hdrs[0]),
            (200, sub_sr_objs[1], sub_shard_resp_hdrs[1]),
            (200, sub_sr_objs[2], sub_shard_resp_hdrs[2]),
            (200, sr_objs[2], shard_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # get root shard ranges
            ('a/c', {'X-Backend-Record-Type': 'auto',
                     'X-Backend-Override-Shard-Name-Filter': 'sharded'},
             dict(states='listing')),  # 200
            # get first shard objects
            (namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            # get second shard sub-shard ranges
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get first sub-shard objects
            (sub_namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='juice\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get second sub-shard objects
            (sub_namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='j', end_marker='lemon\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0])))),
            # get third sub-shard objects
            (sub_namespaces[2].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='l', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0] +
                                        sub_sr_objs[1])))),
            # get third shard objects
            (namespaces[2].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Override-Shard-Name-Filter': 'sharded',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]
        expected_objects = (
            sr_objs[0] + sub_sr_objs[0] + sub_sr_objs[1] + sub_sr_objs[2] +
            sr_objs[2])
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests, memcache=True)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)
        self.assertIn('swift.cache', resp.request.environ)
        cached_keys = set(k for k in resp.request.environ['swift.cache'].store
                          if k.startswith('shard-listing'))
        self.assertEqual(
            {'shard-listing-v2/a/c', 'shard-listing-v2/.shards_a/c_pie'},
            cached_keys)

    @patch_policies([
        StoragePolicy(0, 'zero', True, object_ring=FakeRing()),
        StoragePolicy(1, 'one', False, object_ring=FakeRing())
    ])
    def test_GET_sharded_container_sharding_shard_mixed_policies(self):
        # scenario: one shard is in process of sharding, shards have different
        # policy than root, expect listing to always request root policy index
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        shard_obj_resp_hdrs = self._make_shard_resp_hdrs(
            sr_objs, extra_hdrs={
                'X-Backend-Storage-Policy-Index': 1,
                'X-Backend-Record-Storage-Policy-Index': 0})
        # second shard is sharding and has cleaved two out of three sub shards
        shard_obj_resp_hdrs[1]['X-Backend-Sharding-State'] = 'sharding'
        shard_1_shard_resp_hdrs = dict(shard_obj_resp_hdrs[1])
        shard_1_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        shard_1_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        sub_shard_bounds = (('ham', 'juice'), ('juice', 'lemon'))
        sub_namespaces, sub_ns_dicts, sub_sr_objs = \
            self.create_server_response_data(sub_shard_bounds,
                                             name_prefix='a/c_sub_')
        filler_sr_dict = self.create_server_namespace_dict(
            namespaces[1].name, lower=sub_ns_dicts[-1]['upper'],
            upper=namespaces[1].upper)
        sub_shard_resp_hdrs = self._make_shard_resp_hdrs(
            sub_sr_objs, extra_hdrs={
                'X-Backend-Storage-Policy-Index': 1,
                'X-Backend-Record-Storage-Policy-Index': 0})

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Container-Meta-Flavour': 'peach',
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'
        root_shard_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        mock_responses = [
            # status, body, headers
            (200, ns_dicts, root_shard_resp_hdrs),
            (200, sr_objs[0], shard_obj_resp_hdrs[0]),
            (200, sub_ns_dicts + [filler_sr_dict], shard_1_shard_resp_hdrs),
            (200, sub_sr_objs[0], sub_shard_resp_hdrs[0]),
            (200, sub_sr_objs[1], sub_shard_resp_hdrs[1]),
            (200, sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):],
             shard_obj_resp_hdrs[1]),
            (200, sr_objs[2], shard_obj_resp_hdrs[2])
        ]
        # NB marker always advances to last object name
        expected_requests = [
            # get root shard ranges
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),  # 200
            # get first shard objects
            (namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),  # 200
            # get second shard sub-shard ranges
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get first sub-shard objects
            (sub_namespaces[0].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='h', end_marker='juice\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            # get second sub-shard objects
            (sub_namespaces[1].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='j', end_marker='lemon\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0])))),
            # get remainder of second shard objects
            (namespaces[1].name,
             {'X-Backend-Record-Type': 'object',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='l', end_marker='pie\x00',
                  limit=str(limit - len(sr_objs[0] + sub_sr_objs[0] +
                                        sub_sr_objs[1])))),  # 200
            # get third shard objects
            (namespaces[2].name,
             {'X-Backend-Record-Type': 'auto',
              'X-Backend-Storage-Policy-Index': '0'},
             dict(marker='p', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]))))  # 200
        ]
        expected_objects = (
            sr_objs[0] + sub_sr_objs[0] + sub_sr_objs[1] +
            sr_objs[1][len(sub_sr_objs[0] + sub_sr_objs[1]):] + sr_objs[2])
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests)
        # root object count will overridden by actual length of listing
        self.check_listing_response(resp, root_resp_hdrs)

    @patch_policies([
        StoragePolicy(0, 'zero', True, object_ring=FakeRing()),
        StoragePolicy(1, 'one', False, object_ring=FakeRing())
    ])
    def test_GET_sharded_container_mixed_policies_error(self):
        # scenario: shards have different policy than root, listing requests
        # root policy index but shards not upgraded and respond with their own
        # policy index
        def do_test(shard_policy):
            # only need first shard for this test...
            shard_bounds = ('', 'pie')
            namespaces, _, sr_objs = self.create_server_response_data(
                shard_bounds)
            sr = namespaces[0]
            sr_objs = sr_objs[0]
            shard_resp_hdrs = {
                'X-Backend-Sharding-State': 'unsharded',
                'X-Container-Object-Count': len(sr_objs),
                'X-Container-Bytes-Used':
                    sum([obj['bytes'] for obj in sr_objs]),
            }

            if shard_policy is not None:
                shard_resp_hdrs['X-Backend-Storage-Policy-Index'] = \
                    shard_policy

            size_all_objects = sum([obj['bytes'] for obj in sr_objs])
            num_all_objects = len(sr_objs)
            limit = CONTAINER_LISTING_LIMIT
            root_resp_hdrs, root_shard_resp_hdrs = self._make_root_resp_hdrs(
                num_all_objects, size_all_objects,
                extra_hdrs={'X-Backend-Storage-Policy-Index': 1})

            mock_responses = [
                # status, body, headers
                (200, [dict(sr)], root_shard_resp_hdrs),
                (200, sr_objs, shard_resp_hdrs),
            ]
            # NB marker always advances to last object name
            expected_requests = [
                # get root shard ranges
                ('a/c', {'X-Backend-Record-Type': 'auto'},
                 dict(states='listing')),  # 200
                # get first shard objects
                (sr.name,
                 {'X-Backend-Record-Type': 'auto',
                  'X-Backend-Storage-Policy-Index': '1'},
                 dict(marker='', end_marker='pie\x00', states='listing',
                      limit=str(limit))),  # 200
                # error to client; no request for second shard objects
            ]
            self._check_GET_shard_listing(
                mock_responses, [], expected_requests,
                expected_status=503)

        do_test(0)
        do_test(None)

    def test_GET_record_type_shard(self):
        # explicit request for namespaces
        memcache = FakeMemcache()
        shard_bounds = ('', 'pie')
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        _, root_shard_resp_hdrs = self._make_root_resp_hdrs(2, 4)

        body = json.dumps(ns_dicts).encode('ascii')
        req = Request.blank('/v1/a/c', {'swift.cache': memcache})
        req.headers['X-Backend-Record-Type'] = 'shard'
        req.headers['X-Backend-Record-Shard-Format'] = 'namespace'
        with mocked_http_conn(200, body_iter=[body],
                              headers=root_shard_resp_hdrs) as fake_conn:
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(1, len(fake_conn.requests))
        exp_backend_hdrs = {
            'X-Backend-Record-Type': 'shard',
            'X-Backend-Record-Shard-Format': 'namespace',
            'Host': mock.ANY, 'X-Trans-Id': mock.ANY, 'X-Timestamp': mock.ANY,
            'Connection': 'close', 'User-Agent': mock.ANY,
            'Referer': mock.ANY}
        self.assertEqual(exp_backend_hdrs, fake_conn.requests[0]['headers'])
        self.assertNotIn('state=', fake_conn.requests[0]['qs'])
        # NB: no namespaces cached
        self.assertEqual([mock.call.set('container/a/c', mock.ANY, time=60)],
                         memcache.calls)
        self.assertEqual(ns_dicts, json.loads(resp.body))

    def test_GET_record_type_shard_with_listing_state(self):
        # explicit request for namespaces specifying list state
        memcache = FakeMemcache()
        shard_bounds = ('', 'pie')
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        _, root_shard_resp_hdrs = self._make_root_resp_hdrs(2, 4)

        body = json.dumps(ns_dicts).encode('ascii')
        req = Request.blank('/v1/a/c?state=listing', {'swift.cache': memcache})
        req.headers['X-Backend-Record-Type'] = 'shard'
        req.headers['X-Backend-Record-Shard-Format'] = 'namespace'
        with mocked_http_conn(200, body_iter=[body],
                              headers=root_shard_resp_hdrs) as fake_conn:
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(1, len(fake_conn.requests))
        exp_backend_hdrs = {
            'X-Backend-Record-Type': 'shard',
            'X-Backend-Record-Shard-Format': 'namespace',
            'Host': mock.ANY, 'X-Trans-Id': mock.ANY, 'X-Timestamp': mock.ANY,
            'Connection': 'close', 'User-Agent': mock.ANY,
            'Referer': mock.ANY}
        self.assertEqual(exp_backend_hdrs, fake_conn.requests[0]['headers'])
        self.assertIn('state=listing', fake_conn.requests[0]['qs'])
        # NB: no namespaces cached
        self.assertEqual([mock.call.set('container/a/c', mock.ANY, time=60)],
                         memcache.calls)
        self.assertEqual(ns_dicts, json.loads(resp.body))

    def test_GET_record_type_object(self):
        # explicit request for objects
        memcache = FakeMemcache()
        shard_bounds = ('', 'pie')
        namespaces, ns_dicts, sr_objs = self.create_server_response_data(
            shard_bounds)
        all_objs = sum(sr_objs, [])
        root_resp_hdrs, _ = self._make_root_resp_hdrs(
            len(all_objs), 4, extra_hdrs={'X-Backend-Record-Type': 'object'})
        body = json.dumps(sr_objs[0]).encode('ascii')
        req = Request.blank('/v1/a/c', {'swift.cache': memcache})
        req.headers['X-Backend-Record-Type'] = 'object'
        with mocked_http_conn(200, body_iter=[body],
                              headers=root_resp_hdrs) as fake_conn:
            resp = req.get_response(self.app)

        self.assertEqual(resp.status_int, 200)
        self.assertEqual(1, len(fake_conn.requests))
        exp_backend_hdrs = {
            'X-Backend-Record-Type': 'object',
            'Host': mock.ANY, 'X-Trans-Id': mock.ANY, 'X-Timestamp': mock.ANY,
            'Connection': 'close', 'User-Agent': mock.ANY,
            'Referer': 'GET http://localhost/v1/a/c?format=json'}
        self.assertEqual(exp_backend_hdrs, fake_conn.requests[0]['headers'])
        self.assertEqual([mock.call.set('container/a/c', mock.ANY, time=60)],
                         memcache.calls)
        self.assertEqual(all_objs, json.loads(resp.body))
        self.assertEqual('object', resp.headers.get('X-Backend-Record-Type'))


@patch_policies([StoragePolicy(0, 'zero', True, object_ring=FakeRing())])
class TestGetShardedContainerLegacy(TestGetShardedContainer):
    """
    Test all existing test cases to query Namespaces from container server but
    get ShardRanges returned. This is to test backward compatibility that new
    proxy-servers ask for new Namespace format from the older version container
    servers who don't support Namespace format yet.
    """
    # old container servers did not return this header
    RESP_SHARD_FORMAT_HEADERS = {}

    def create_server_namespace_dict(self, name, lower, upper):
        # return a dict representation of an instance of the type the backend
        # server returns for shard format = 'namespace'
        return dict(ShardRange(name, Timestamp.now(), lower, upper,
                               state=ShardRange.ACTIVE))

    def create_server_response_data(self, bounds, states=None,
                                    name_prefix='.shards_a/c_'):
        if not isinstance(bounds[0], (list, tuple)):
            bounds = [(l, u) for l, u in zip(bounds[:-1], bounds[1:])]
        if not states:
            states = []
        shard_ranges = [
            ShardRange(name_prefix + bound[1].replace('/', '-'),
                       Timestamp.now(), bound[0], bound[1], state=state)
            for bound, state in zip_longest(
                bounds, states, fillvalue=ShardRange.FOUND)]
        sr_dicts = [dict(sr, last_modified=sr.timestamp.isoformat)
                    for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        return shard_ranges, sr_dicts, sr_objs


class BaseTestContainerControllerGetPath(BaseTestContainerController):
    def setUp(self):
        super(BaseTestContainerControllerGetPath, self).setUp()
        self.memcache = FakeMemcache()
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        self.ns_dicts = [{'name': '.shards_a/c_%s' % upper,
                          'lower': lower,
                          'upper': upper}
                         for lower, upper in shard_bounds]
        self.root_resp_hdrs = {
            'Accept-Ranges': 'bytes',
            'Content-Type': 'application/json',
            'Last-Modified': 'Thu, 01 Jan 1970 00:00:03 GMT',
            'X-Backend-Timestamp': '2',
            'X-Backend-Put-Timestamp': '3',
            'X-Backend-Delete-Timestamp': '0',
            'X-Backend-Status-Changed-At': '0',
            'X-Timestamp': '2',
            'X-Put-Timestamp': '3',
            'X-Container-Object-Count': '6',
            'X-Container-Bytes-Used': '12',
            'X-Backend-Storage-Policy-Index': '0'}

    def _call_app(self, req):
        return req.get_response(self.app)

    def _build_request(self, headers, params, infocache=None):
        # helper to make a GET request with caches set in environ
        query_string = '?' + '&'.join('%s=%s' % (k, v)
                                      for k, v in params.items())
        container_path = '/v1/a/c' + query_string
        request = Request.blank(container_path, headers=headers)
        request.environ['swift.cache'] = self.memcache
        request.environ['swift.infocache'] = infocache if infocache else {}
        return request

    def _check_response(self, resp, exp_listing, extra_hdrs):
        # helper to check a shard listing response
        actual_shards = json.loads(resp.body)
        self.assertEqual(exp_listing, actual_shards)
        exp_hdrs = dict(self.root_resp_hdrs)
        # x-put-timestamp is sent from backend but removed in proxy base
        # controller GETorHEAD_base so not expected in response from proxy
        exp_hdrs.pop('X-Put-Timestamp')
        self.assertIn('X-Timestamp', resp.headers)
        actual_timestamp = resp.headers.pop('X-Timestamp')
        exp_timestamp = exp_hdrs.pop('X-Timestamp')
        self.assertEqual(Timestamp(exp_timestamp),
                         Timestamp(actual_timestamp))
        exp_hdrs.update(extra_hdrs)
        exp_hdrs.update(
            {'X-Storage-Policy': 'zero',  # added in container controller
             'Content-Length':
                 str(len(json.dumps(exp_listing).encode('ascii'))),
             }
        )
        # we expect this header to be removed by proxy
        exp_hdrs.pop('X-Backend-Override-Shard-Name-Filter', None)
        for ignored in ('x-account-container-count', 'x-object-meta-test',
                        'x-delete-at', 'etag', 'x-works'):
            # FakeConn adds these
            resp.headers.pop(ignored, None)
        self.assertEqual(exp_hdrs, resp.headers)

    def _capture_backend_request(self, req, resp_status, resp_body,
                                 resp_extra_hdrs, num_resp=1):
        self.assertGreater(num_resp, 0)  # sanity check
        resp_hdrs = dict(self.root_resp_hdrs)
        resp_hdrs.update(resp_extra_hdrs)
        resp_status = [resp_status] * num_resp
        with mocked_http_conn(
                *resp_status, body_iter=[resp_body] * num_resp,
                headers=[resp_hdrs] * num_resp) as fake_conn:
            resp = self._call_app(req)
        self.assertEqual(resp_status[0], resp.status_int)
        self.assertEqual(num_resp, len(fake_conn.requests))
        return fake_conn.requests[0], resp

    def _check_backend_req(self, req, backend_req, extra_params=None,
                           extra_hdrs=None):
        self.assertEqual('a/c', backend_req['path'][7:])

        expected_params = {'states': 'listing', 'format': 'json'}
        if extra_params:
            expected_params.update(extra_params)
        backend_params = dict(urllib.parse.parse_qsl(
            backend_req['qs'], True, encoding='latin1'))
        self.assertEqual(expected_params, backend_params)

        backend_hdrs = backend_req['headers']
        self.assertIsNotNone(backend_hdrs.pop('Referer', None))
        self.assertIsNotNone(backend_hdrs.pop('X-Timestamp', None))
        self.assertTrue(backend_hdrs.pop('User-Agent', '').startswith(
            'proxy-server'))
        expected_headers = {
            'Connection': 'close',
            'Host': 'localhost:80',
            'X-Trans-Id': req.headers['X-Trans-Id']}
        if extra_hdrs:
            expected_headers.update(extra_hdrs)
        self.assertEqual(expected_headers, backend_hdrs)
        for k, v in expected_headers.items():
            self.assertIn(k, backend_hdrs)
            self.assertEqual(v, backend_hdrs.get(k))


class TestGetPathNamespaceCaching(BaseTestContainerControllerGetPath):
    # These tests are verifying the content and caching of the backend
    # namespace responses so we're not interested in gathering objects from the
    # shards. We therefore mock _get_from_shards so that the response actually
    # contains a fake listing and also capture the namespace listing passed to
    # _get_from_shards. This avoids faking all the object listing responses
    # from shards, and facilitates making assertions about the namespaces
    # passed to _get_from_shards.
    RESP_SHARD_FORMAT_HEADERS = {'X-Backend-Record-Shard-Format': 'namespace'}
    bogus_listing = [{'name': 'x'}, {'name': 'y'}]
    bogus_listing_body = json.dumps(bogus_listing).encode('ascii')

    def setUp(self):
        super(TestGetPathNamespaceCaching, self).setUp()
        self.namespaces = [Namespace(**ns) for ns in self.ns_dicts]
        self.ns_bound_list = NamespaceBoundList.parse(self.namespaces)
        self._setup_namespace_stubs()
        self.get_from_shards_lists = []

    def _fake_get_from_shards(self, req, resp, namespaces):
        self.get_from_shards_lists.append(namespaces)
        resp.body = self.bogus_listing_body
        return resp

    def _call_app(self, req):
        # override base class method to mock get_from_shards
        self.get_from_shards_lists = []
        with mock.patch(
                'swift.proxy.controllers.container.'
                'ContainerController._get_from_shards',
                side_effect=self._fake_get_from_shards):
            return req.get_response(self.app)

    def _setup_namespace_stubs(self):
        self._stub_namespaces = self.ns_dicts
        self._stub_namespaces_dump = json.dumps(
            self._stub_namespaces).encode('ascii')

    def _do_test_GET_namespace_caching(self, record_type, exp_recheck_listing,
                                       extra_backend_req_hdrs=None):
        # this test gets shard ranges into cache and then reads from cache
        exp_backend_req_hdrs = {
            'X-Backend-Record-Type': 'auto',
            'X-Backend-Record-Shard-Format': 'namespace',
            'X-Backend-Include-Deleted': 'false',
            'X-Backend-Override-Shard-Name-Filter': 'sharded'}
        if extra_backend_req_hdrs:
            exp_backend_req_hdrs.update(extra_backend_req_hdrs)
        sharding_state = 'sharded'
        exp_noncache_resp_hdrs = {
            'X-Backend-Recheck-Container-Existence': '60',
            'X-Backend-Sharding-State': 'sharded'}
        exp_cache_resp_hdrs = {
            'X-Backend-Cached-Results': 'true',
            'X-Backend-Sharding-State': sharding_state}
        self.memcache.delete_all()
        # container is sharded but proxy does not have that state cached;
        # expect a backend request and expect namespaces to be cached
        self.memcache.clear_calls()
        self.logger.clear()
        req = self._build_request({'X-Backend-Record-Type': record_type},
                                  {'states': 'listing'}, {})
        backend_resp_hdrs = {'X-Backend-Record-Type': 'shard',
                             'X-Backend-Sharding-State': 'sharded',
                             'X-Backend-Override-Shard-Name-Filter': 'true'}
        backend_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_namespaces_dump, backend_resp_hdrs)

        self._check_backend_req(
            req, backend_req, extra_hdrs=exp_backend_req_hdrs)
        self._check_response(resp, self.bogus_listing, exp_noncache_resp_hdrs)

        cache_key = 'shard-listing-v2/a/c'
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set(cache_key, self.ns_bound_list.bounds,
                           time=exp_recheck_listing, raise_on_error=True),
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual('sharded',
                         self.memcache.calls[2][1][1]['sharding_state'])
        self.assertIn('swift.infocache', req.environ)
        self.assertIn(cache_key, req.environ['swift.infocache'])
        self.assertEqual(self.ns_bound_list,
                         req.environ['swift.infocache'][cache_key])
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.cache.miss',
             'container.shard_listing.cache.bypass.200'])

        # container is sharded and proxy has that state cached, but
        # no namespaces cached; expect a cache miss and write-back
        self.memcache.delete(cache_key)
        self.memcache.clear_calls()
        self.logger.clear()
        req = self._build_request({'X-Backend-Record-Type': record_type},
                                  {'states': 'listing'}, {})
        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_namespaces_dump, backend_resp_hdrs)
        self._check_backend_req(
            req, backend_req, extra_hdrs=exp_backend_req_hdrs)
        self._check_response(resp, self.bogus_listing, exp_noncache_resp_hdrs)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.get(cache_key, raise_on_error=True),
             mock.call.set(cache_key, self.ns_bound_list.bounds,
                           time=exp_recheck_listing, raise_on_error=True),
             # Since there was a backend request, we go ahead and cache
             # container info, too
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertIn('swift.infocache', req.environ)
        self.assertIn(cache_key, req.environ['swift.infocache'])
        self.assertEqual(self.ns_bound_list,
                         req.environ['swift.infocache'][cache_key])
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.cache.hit',
             'container.shard_listing.cache.miss.200'])

        # container is sharded and proxy does have that state cached and
        # also has namespaces cached; expect a read from cache
        self.memcache.clear_calls()
        self.logger.clear()
        req = self._build_request({'X-Backend-Record-Type': record_type},
                                  {'states': 'listing'}, {})
        resp = self._call_app(req)
        self._check_response(resp, self.bogus_listing, exp_cache_resp_hdrs)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.get(cache_key, raise_on_error=True)],
            self.memcache.calls)
        self.assertIn('swift.infocache', req.environ)
        self.assertIn(cache_key, req.environ['swift.infocache'])
        self.assertEqual(self.ns_bound_list,
                         req.environ['swift.infocache'][cache_key])
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.cache.hit',
             'container.shard_listing.cache.hit'])

        # if there's a chance to skip cache, maybe we go to disk again...
        self.memcache.clear_calls()
        self.logger.clear()
        self.app.container_listing_shard_ranges_skip_cache = 0.10
        req = self._build_request({'X-Backend-Record-Type': record_type},
                                  {'states': 'listing'}, {})
        with mock.patch('random.random', return_value=0.05):
            backend_req, resp = self._capture_backend_request(
                req, 200, self._stub_namespaces_dump, backend_resp_hdrs)
        self._check_backend_req(
            req, backend_req, extra_hdrs=exp_backend_req_hdrs)
        self._check_response(resp, self.bogus_listing, exp_noncache_resp_hdrs)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set(cache_key, self.ns_bound_list.bounds,
                           time=exp_recheck_listing, raise_on_error=True),
             # Since there was a backend request, we go ahead and cache
             # container info, too
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertIn('swift.infocache', req.environ)
        self.assertIn(cache_key, req.environ['swift.infocache'])
        self.assertEqual(self.ns_bound_list,
                         req.environ['swift.infocache'][cache_key])
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.cache.hit',
             'container.shard_listing.cache.skip.200'])

        # ... or maybe we serve from cache
        self.memcache.clear_calls()
        self.logger.clear()
        req = self._build_request({'X-Backend-Record-Type': record_type},
                                  {'states': 'listing'}, {})
        with mock.patch('random.random', return_value=0.11):
            resp = self._call_app(req)
            self._check_response(resp, self.bogus_listing, exp_cache_resp_hdrs)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.get(cache_key, raise_on_error=True)],
            self.memcache.calls)
        self.assertIn('swift.infocache', req.environ)
        self.assertIn(cache_key, req.environ['swift.infocache'])
        self.assertEqual(self.ns_bound_list,
                         req.environ['swift.infocache'][cache_key])
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.cache.hit',
             'container.shard_listing.cache.hit'])

        # test request to hit infocache.
        self.memcache.clear_calls()
        self.logger.clear()
        req = self._build_request(
            {'X-Backend-Record-Type': record_type},
            {'states': 'listing'},
            infocache=req.environ['swift.infocache'])
        with mock.patch('random.random', return_value=0.11):
            resp = self._call_app(req)
        self._check_response(resp, self.bogus_listing, exp_cache_resp_hdrs)
        self.assertEqual([], self.memcache.calls)
        self.assertIn('swift.infocache', req.environ)
        self.assertIn(cache_key, req.environ['swift.infocache'])
        self.assertEqual(self.ns_bound_list,
                         req.environ['swift.infocache'][cache_key])
        self.assertEqual(
            [x[0][0] for x in
             self.logger.logger.statsd_client.calls['increment']],
            ['container.info.infocache.hit',
             'container.shard_listing.infocache.hit'])

        # put this back the way we found it for later subtests
        self.app.container_listing_shard_ranges_skip_cache = 0.0

        # delete the container; check that namespaces are evicted from cache
        self.memcache.clear_calls()
        infocache = {}
        req = Request.blank('/v1/a/c', method='DELETE')
        req.environ['swift.cache'] = self.memcache
        req.environ['swift.infocache'] = infocache
        self._capture_backend_request(req, 204, b'', {},
                                      num_resp=self.CONTAINER_REPLICAS)
        self.assertEqual(
            [mock.call.delete('container/a/c'),
             mock.call.delete(cache_key)],
            self.memcache.calls)

    def test_GET_namespace_caching(self):
        # no record type defaults to 'auto' in backend requests;
        # expect shard ranges cache time to be default value of 600
        self._do_test_GET_namespace_caching('', 600)
        # expect shard ranges cache time to be configured value of 120
        self.app.recheck_listing_shard_ranges = 120
        self._do_test_GET_namespace_caching('', 120)
        # explicitly requesting record type 'auto'
        self._do_test_GET_namespace_caching('auto', 120)
        # nonsense record type defaults to 'auto'
        self._do_test_GET_namespace_caching('banana', 120)

    def test_get_from_shards_add_root_spi(self):
        shard_resp = mock.MagicMock(status_int=204,
                                    headers={'x-backend-record-type': 'shard'})

        def mock_get_container_listing(self_, req, *args, **kargs):
            captured_hdrs.update(req.headers)
            return None, shard_resp

        # header in response -> header added to request
        captured_hdrs = {}
        req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp_hdrs = dict(self.root_resp_hdrs)
        resp_hdrs['x-backend-record-type'] = 'shard'
        resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        resp = mock.MagicMock(status_int=200,
                              headers=resp_hdrs,
                              request=req)
        resp.headers['X-Backend-Storage-Policy-Index'] = '0'
        with mock.patch('swift.proxy.controllers.container.'
                        'ContainerController._get_container_listing',
                        mock_get_container_listing):
            controller_cls, d = self.app.get_controller(req)
            controller = controller_cls(self.app, **d)
            controller._get_from_shards(req, resp, list(self.namespaces))

        self.assertIn('X-Backend-Storage-Policy-Index', captured_hdrs)
        self.assertEqual(
            captured_hdrs['X-Backend-Storage-Policy-Index'], '0')

        captured_hdrs = {}
        req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'GET'})
        resp = mock.MagicMock(status_int=200,
                              headers=resp_hdrs,
                              request=req)
        resp.headers['X-Backend-Storage-Policy-Index'] = '1'
        with mock.patch('swift.proxy.controllers.container.'
                        'ContainerController._get_container_listing',
                        mock_get_container_listing):
            controller_cls, d = self.app.get_controller(req)
            controller = controller_cls(self.app, **d)
            controller._get_from_shards(req, resp, list(self.namespaces))

        self.assertIn('X-Backend-Storage-Policy-Index', captured_hdrs)
        self.assertEqual(
            captured_hdrs['X-Backend-Storage-Policy-Index'], '1')

        # header not added to request if not root request
        captured_hdrs = {}
        req = Request.blank('/v1/a/c',
                            environ={
                                'REQUEST_METHOD': 'GET',
                                'swift.shard_listing_history': [('a', 'c')]}
                            )
        resp = mock.MagicMock(status_int=200,
                              headers=self.root_resp_hdrs,
                              request=req)
        resp.headers['X-Backend-Storage-Policy-Index'] = '0'
        with mock.patch('swift.proxy.controllers.container.'
                        'ContainerController._get_container_listing',
                        mock_get_container_listing):
            controller_cls, d = self.app.get_controller(req)
            controller = controller_cls(self.app, **d)
            controller._get_from_shards(req, resp, list(self.namespaces))

        self.assertNotIn('X-Backend-Storage-Policy-Index', captured_hdrs)

        # existing X-Backend-Storage-Policy-Index in request is respected
        captured_hdrs = {}
        req = Request.blank('/v1/a/c', environ={'REQUEST_METHOD': 'GET'})
        req.headers['X-Backend-Storage-Policy-Index'] = '0'
        resp = mock.MagicMock(status_int=200,
                              headers=resp_hdrs,
                              request=req)
        resp.headers['X-Backend-Storage-Policy-Index'] = '1'
        with mock.patch('swift.proxy.controllers.container.'
                        'ContainerController._get_container_listing',
                        mock_get_container_listing):
            controller_cls, d = self.app.get_controller(req)
            controller = controller_cls(self.app, **d)
            controller._get_from_shards(req, resp, list(self.namespaces))

        self.assertIn('X-Backend-Storage-Policy-Index', captured_hdrs)
        self.assertEqual(
            captured_hdrs['X-Backend-Storage-Policy-Index'], '0')

    def test_GET_namespaces_404_response(self):
        # pre-warm cache with container info but not shard ranges so that the
        # backend request tries to get a cacheable listing, but backend 404's
        info = headers_to_container_info(self.root_resp_hdrs)
        info['status'] = 200
        info['sharding_state'] = 'sharded'
        self.memcache.set('container/a/c', info)
        self.memcache.clear_calls()
        req = self._build_request({'X-Backend-Record-Type': ''},
                                  {'states': 'listing'}, {})
        backend_req, resp = self._capture_backend_request(
            req, 404, b'', {}, num_resp=2 * self.CONTAINER_REPLICAS)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        self.assertNotIn('X-Backend-Cached-Results', resp.headers)
        # Note: container metadata is updated in cache but shard ranges are not
        # deleted from cache
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.get('shard-listing-v2/a/c', raise_on_error=True),
             mock.call.set('container/a/c', mock.ANY, time=6.0)],
            self.memcache.calls)
        self.assertEqual(404, self.memcache.calls[2][1][1]['status'])
        self.assertEqual(b'', resp.body)
        self.assertEqual(404, resp.status_int)
        self.assertEqual({'container.info.cache.hit': 1,
                          'container.shard_listing.cache.miss.404': 1},
                         self.logger.statsd_client.get_increment_counts())

    def test_GET_namespaces_read_from_cache_error(self):
        info = headers_to_container_info(self.root_resp_hdrs)
        info['status'] = 200
        info['sharding_state'] = 'sharded'
        self.memcache.set('container/a/c', info)
        self.memcache.clear_calls()
        self.memcache.error_on_get = [False, True]

        req = self._build_request({'X-Backend-Record-Type': ''},
                                  {'states': 'listing'}, {})
        backend_req, resp = self._capture_backend_request(
            req, 404, b'', {}, num_resp=2 * self.CONTAINER_REPLICAS)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        self.assertNotIn('X-Backend-Cached-Results', resp.headers)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.get('shard-listing-v2/a/c', raise_on_error=True),
             mock.call.set('container/a/c', mock.ANY, time=6.0)],
            self.memcache.calls)
        self.assertEqual(404, self.memcache.calls[2][1][1]['status'])
        self.assertEqual(b'', resp.body)
        self.assertEqual(404, resp.status_int)
        self.assertEqual({'container.info.cache.hit': 1,
                          'container.shard_listing.cache.error.404': 1},
                         self.logger.statsd_client.get_increment_counts())

    def test_GET_namespaces_read_from_cache_empty_list(self):
        info = headers_to_container_info(self.root_resp_hdrs)
        info['status'] = 200
        info['sharding_state'] = 'sharded'
        self.memcache.set('container/a/c', info)
        # note: an empty list in cache is unexpected and is treated as a miss
        self.memcache.set('shard-listing-v2/a/c', [])
        self.memcache.clear_calls()

        req = self._build_request({'X-Backend-Record-Type': ''},
                                  {'states': 'listing'}, {})
        backend_req, resp = self._capture_backend_request(
            req, 404, b'', {}, num_resp=2 * self.CONTAINER_REPLICAS)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        self.assertNotIn('X-Backend-Cached-Results', resp.headers)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.get('shard-listing-v2/a/c', raise_on_error=True),
             mock.call.set('container/a/c', mock.ANY, time=6.0)],
            self.memcache.calls)
        self.assertEqual(404, self.memcache.calls[2][1][1]['status'])
        self.assertEqual(b'', resp.body)
        self.assertEqual(404, resp.status_int)
        self.assertEqual({'container.info.cache.hit': 1,
                          'container.shard_listing.cache.miss.404': 1},
                         self.logger.statsd_client.get_increment_counts())

    def _do_test_GET_namespaces_read_from_cache(self, params, record_type):
        # pre-warm cache with container metadata and shard ranges and verify
        # that shard range listing are read from cache when appropriate
        self.memcache.delete_all()
        self.logger.clear()
        info = headers_to_container_info(self.root_resp_hdrs)
        info['status'] = 200
        info['sharding_state'] = 'sharded'
        self.memcache.set('container/a/c', info)
        self.memcache.set('shard-listing-v2/a/c', self.ns_bound_list.bounds)
        self.memcache.clear_calls()

        req_hdrs = {'X-Backend-Record-Type': record_type}
        req = self._build_request(req_hdrs, params, {})
        resp = self._call_app(req)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.get('shard-listing-v2/a/c', raise_on_error=True)],
            self.memcache.calls)
        self.assertEqual({'container.info.cache.hit': 1,
                          'container.shard_listing.cache.hit': 1},
                         self.logger.statsd_client.get_increment_counts())
        return resp

    def test_GET_namespaces_read_from_cache(self):
        exp_resp_hdrs = {'X-Backend-Cached-Results': 'true',
                         'X-Backend-Override-Shard-Name-Filter': 'true',
                         'X-Backend-Sharding-State': 'sharded'}

        resp = self._do_test_GET_namespaces_read_from_cache(
            {'states': 'listing'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces], self.get_from_shards_lists)

        # no record type defaults to auto
        resp = self._do_test_GET_namespaces_read_from_cache(
            {'states': 'listing'}, '')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_read_from_cache(
            {'states': 'listing', 'reverse': 'true'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        exp_shards = list(self.namespaces)
        exp_shards.reverse()
        self.assertEqual([exp_shards], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_read_from_cache(
            {'states': 'listing', 'marker': 'jam'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces[1:]], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_read_from_cache(
            {'states': 'listing', 'marker': 'jam', 'end_marker': 'kale'},
            'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces[1:2]], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_read_from_cache(
            {'states': 'listing', 'includes': 'egg'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces[:1]], self.get_from_shards_lists)

    def _do_test_GET_namespaces_write_to_cache(self, params, record_type):
        # verify that namespace listing is written to cache when appropriate
        self.logger.clear()
        self.memcache.delete_all()
        self.memcache.clear_calls()
        # set request up for cacheable listing
        req_hdrs = {'X-Backend-Record-Type': record_type}
        req = self._build_request(req_hdrs, params, {})
        # response indicates cacheable listing
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Shard-Name-Filter': 'true',
                     'X-Backend-Sharding-State': 'sharded'}
        resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)
        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_namespaces_dump, resp_hdrs)
        self._check_backend_req(
            req, backend_req,
            extra_params=params,
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        expected_hdrs = {'X-Backend-Recheck-Container-Existence': '60'}
        expected_hdrs.update(resp_hdrs)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set('shard-listing-v2/a/c', self.ns_bound_list.bounds,
                           time=600, raise_on_error=True),
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        info_lines = self.logger.get_lines_for_level('info')
        self.assertIn(
            'Caching listing namespaces for shard-listing-v2/a/c '
            '(3 namespaces)', info_lines)
        # shards were cached
        self.assertEqual('sharded',
                         self.memcache.calls[2][1][1]['sharding_state'])
        self.assertEqual({'container.info.cache.miss': 1,
                          'container.shard_listing.cache.bypass.200': 1},
                         self.logger.statsd_client.get_increment_counts())
        return resp

    def test_GET_namespaces_write_to_cache(self):
        exp_resp_hdrs = {'X-Backend-Recheck-Container-Existence': '60',
                         'X-Backend-Override-Shard-Name-Filter': 'true',
                         'X-Backend-Sharding-State': 'sharded'}

        resp = self._do_test_GET_namespaces_write_to_cache(
            {'states': 'listing'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces], self.get_from_shards_lists)

        # no record type defaults to auto
        resp = self._do_test_GET_namespaces_write_to_cache(
            {'states': 'listing'}, '')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_write_to_cache(
            {'states': 'listing', 'reverse': 'true'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        exp_shards = list(self.namespaces)
        exp_shards.reverse()
        self.assertEqual([exp_shards], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_write_to_cache(
            {'states': 'listing', 'marker': 'jam'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces[1:]], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_write_to_cache(
            {'states': 'listing', 'marker': 'jam', 'end_marker': 'kale'},
            'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces[1:2]], self.get_from_shards_lists)

        resp = self._do_test_GET_namespaces_write_to_cache(
            {'states': 'listing', 'includes': 'egg'}, 'auto')
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces[:1]], self.get_from_shards_lists)

    def test_GET_namespaces_write_to_cache_with_x_newest(self):
        # when x-newest is sent, verify that there is no cache lookup to check
        # sharding state but then backend requests are made requesting complete
        # namespace list which can be cached
        req_hdrs = {'X-Backend-Record-Type': 'auto',
                    'X-Newest': 'true'}
        params = {'states': 'listing'}
        req = self._build_request(req_hdrs, params, {})
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Shard-Name-Filter': 'true',
                     'X-Backend-Sharding-State': 'sharded'}
        resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_namespaces_dump, resp_hdrs,
            num_resp=2 * self.CONTAINER_REPLICAS)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Newest': 'true',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        exp_resp_hdrs = {'X-Backend-Recheck-Container-Existence': '60',
                         'X-Backend-Override-Shard-Name-Filter': 'true',
                         'X-Backend-Sharding-State': 'sharded'}
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([self.namespaces], self.get_from_shards_lists)
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set('shard-listing-v2/a/c', self.ns_bound_list.bounds,
                           time=600, raise_on_error=True),
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual('sharded',
                         self.memcache.calls[2][1][1]['sharding_state'])
        self.assertEqual({'container.info.cache.miss': 1,
                          'container.shard_listing.cache.force_skip.200': 1},
                         self.logger.statsd_client.get_increment_counts())

    def _do_test_GET_namespaces_no_cache_write(self, resp_hdrs):
        # verify that there is a cache lookup to check container info but then
        # a backend request is made requesting complete shard list, but do not
        # expect shard ranges to be cached; check that marker, end_marker etc
        # are passed to backend
        self.logger.clear()
        self.memcache.clear_calls()
        req = self._build_request(
            {'X-Backend-Record-Type': ''},  # no record type defaults to auto
            {'states': 'listing', 'marker': 'egg', 'end_marker': 'jam',
             'reverse': 'true'}, {})
        resp_namespaces = self._stub_namespaces[:2]
        resp_namespaces.reverse()
        backend_req, resp = self._capture_backend_request(
            req, 200, json.dumps(resp_namespaces).encode('ascii'),
            resp_hdrs)
        self._check_backend_req(
            req, backend_req,
            extra_params={'marker': 'egg', 'end_marker': 'jam',
                          'reverse': 'true'},
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        exp_resp_hdrs = {'X-Backend-Recheck-Container-Existence': '60',
                         'X-Backend-Override-Shard-Name-Filter': 'true'}
        if 'X-Backend-Sharding-State' in resp_hdrs:
            exp_resp_hdrs['X-Backend-Sharding-State'] = \
                resp_hdrs['X-Backend-Sharding-State']
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        expected_shards = self.namespaces[:2]
        expected_shards.reverse()
        self.assertEqual([expected_shards], self.get_from_shards_lists)
        # container metadata is looked up in memcache for sharding state
        # container metadata is set in memcache
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual(resp.headers.get('X-Backend-Sharding-State'),
                         self.memcache.calls[1][1][1]['sharding_state'])
        self.memcache.delete_all()

    def test_GET_namespaces_no_cache_write_with_cached_container_info(self):
        # pre-warm cache with container info, but verify that shard range cache
        # lookup is only attempted when the cached sharding state and status
        # are suitable, and full set of headers can be constructed from cache;
        # Note: backend response has state unsharded so no shard ranges cached

        def do_test(info):
            self.memcache.set('container/a/c', info)
            # expect the same outcomes as if there was no cached container info
            resp_headers = {'X-Backend-Record-Type': 'shard',
                            'X-Backend-Override-Shard-Name-Filter': 'true',
                            'X-Backend-Sharding-State': 'unsharded'}
            resp_headers.update(self.RESP_SHARD_FORMAT_HEADERS)
            self._do_test_GET_namespaces_no_cache_write(resp_headers)

        # setup a default 'good' info
        info = headers_to_container_info(self.root_resp_hdrs)
        info['status'] = 200
        info['sharding_state'] = 'sharded'
        do_test(dict(info, status=404))
        do_test(dict(info, sharding_state='unsharded'))
        do_test(dict(info, sharding_state='sharding'))
        do_test(dict(info, sharding_state='collapsed'))
        do_test(dict(info, sharding_state='unexpected'))

        stale_info = dict(info)
        stale_info.pop('created_at')
        do_test(stale_info)

        stale_info = dict(info)
        stale_info.pop('put_timestamp')
        do_test(stale_info)

        stale_info = dict(info)
        stale_info.pop('delete_timestamp')
        do_test(stale_info)

        stale_info = dict(info)
        stale_info.pop('status_changed_at')
        do_test(stale_info)

    def test_GET_namespaces_no_cache_write_for_non_sharded_states(self):
        # verify that namespaces are not written to cache when container
        # state returned by backend is not 'sharded'; we don't expect
        # 'X-Backend-Override-Shard-Name-Filter': 'true' to be returned unless
        # the sharding state is 'sharded' but include it in this test to check
        # that the state is checked by proxy controller
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Shard-Name-Filter': 'true',
                     'X-Backend-Sharding-State': 'unsharded'}
        resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)
        resp_hdrs['X-Backend-Sharding-State'] = 'sharding'
        self._do_test_GET_namespaces_no_cache_write(resp_hdrs)
        resp_hdrs['X-Backend-Sharding-State'] = 'collapsed'
        self._do_test_GET_namespaces_no_cache_write(resp_hdrs)
        resp_hdrs['X-Backend-Sharding-State'] = 'unexpected'
        self._do_test_GET_namespaces_no_cache_write(resp_hdrs)

    def test_GET_namespaces_no_cache_write_for_incomplete_listing(self):
        # verify that namespaces are not written to cache when container
        # response does not acknowledge x-backend-override-shard-name-filter
        # e.g. container server not upgraded
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Sharding-State': 'sharded'}
        resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)
        self._do_test_GET_namespaces_no_cache_write(resp_hdrs)
        resp_hdrs['X-Backend-Override-Shard-Name-Filter'] = 'false'
        self._do_test_GET_namespaces_no_cache_write(resp_hdrs)
        resp_hdrs['X-Backend-Override-Shard-Name-Filter'] = 'rogue'
        self._do_test_GET_namespaces_no_cache_write(resp_hdrs)

    def _do_test_GET_namespaces_no_cache_write_not_namespaces(self, resp_hdrs):
        # verify that there's no cache write for namespaces when backend
        # response doesn't return namespaces
        self.logger.clear()
        self.memcache.clear_calls()
        req = self._build_request(
            {'X-Backend-Record-Type': ''},  # no record type defaults to auto
            {'states': 'listing'}, {})

        backend_req, resp = self._capture_backend_request(
            req, 200, self.bogus_listing_body, resp_hdrs)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        exp_resp_hdrs = {'X-Backend-Recheck-Container-Existence': '60',
                         'X-Backend-Override-Shard-Name-Filter': 'true'}
        for k in ('X-Backend-Record-Shard-Format', 'X-Backend-Sharding-State'):
            if k in resp_hdrs:
                exp_resp_hdrs[k] = resp_hdrs[k]
        self._check_response(resp, self.bogus_listing, exp_resp_hdrs)
        self.assertEqual([], self.get_from_shards_lists)
        # container metadata is looked up in memcache for sharding state
        # container metadata is set in memcache
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual(resp.headers.get('X-Backend-Sharding-State'),
                         self.memcache.calls[1][1][1]['sharding_state'])
        self.memcache.delete_all()

    def test_GET_namespaces_no_cache_write_for_object_listing(self):
        # verify that namespaces are not written to cache when container
        # response does not return shard ranges
        self._do_test_GET_namespaces_no_cache_write_not_namespaces(
            {'X-Backend-Record-Type': 'object',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_namespaces_no_cache_write_not_namespaces(
            {'X-Backend-Record-Type': 'other',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_namespaces_no_cache_write_not_namespaces(
            {'X-Backend-Record-Type': 'true',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_namespaces_no_cache_write_not_namespaces(
            {'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})

    def _do_test_GET_namespaces_bad_response_body(self, resp_body):
        # verify that resp body is not cached if shard range parsing fails;
        # check the original unparseable response body is returned
        self.bogus_listing_body = json.dumps(resp_body).encode('ascii')
        self.memcache.clear_calls()
        req = self._build_request(
            {'X-Backend-Record-Type': ''},
            {'states': 'listing'}, {})
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Shard-Name-Filter': 'true',
                     'X-Backend-Sharding-State': 'sharded'}
        resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)

        backend_req, resp = self._capture_backend_request(
            req, 200, self.bogus_listing_body, resp_hdrs)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'auto',
                        'X-Backend-Record-Shard-Format': 'namespace',
                        'X-Backend-Include-Deleted': 'false',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        exp_resp_hdrs = {'X-Backend-Recheck-Container-Existence': '60',
                         'X-Backend-Override-Shard-Name-Filter': 'true'}
        if 'X-Backend-Sharding-State' in resp_hdrs:
            exp_resp_hdrs['X-Backend-Sharding-State'] = \
                resp_hdrs['X-Backend-Sharding-State']
        self._check_response(resp, resp_body, exp_resp_hdrs)
        # container metadata is looked up in memcache for sharding state
        # container metadata is set in memcache
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual(resp.headers.get('X-Backend-Sharding-State'),
                         self.memcache.calls[1][1][1]['sharding_state'])
        self.assertEqual({'container.info.cache.miss': 1,
                          'container.shard_listing.cache.bypass.200': 1},
                         self.logger.statsd_client.get_increment_counts())
        self.memcache.delete_all()

    def test_GET_namespaces_bad_response_body(self):
        self._do_test_GET_namespaces_bad_response_body(
            {'bad': 'data', 'not': ' a list'})
        error_lines = self.logger.get_lines_for_level('error')
        start = 'Problem with container shard listing response from /v1/a/c?'
        msg, _, _ = error_lines[0].partition(':')
        self.assertEqual(start, msg[:len(start)])
        actual_qs = msg[len(start):]
        actual_params = dict(
            urllib.parse.parse_qsl(actual_qs, keep_blank_values=True))
        self.assertEqual({'format': 'json', 'states': 'listing'},
                         actual_params)
        self.assertFalse(error_lines[1:])

        self.logger.clear()
        self._do_test_GET_namespaces_bad_response_body(
            [{'not': 'a namespace'}])
        error_lines = self.logger.get_lines_for_level('error')
        start = 'Failed to get namespaces from /v1/a/c?'
        msg, _, _ = error_lines[0].partition(':')
        self.assertEqual(start, msg[:len(start)])
        actual_qs = msg[len(start):]
        actual_params = dict(
            urllib.parse.parse_qsl(actual_qs, keep_blank_values=True))
        self.assertEqual({'format': 'json', 'states': 'listing'},
                         actual_params)
        self.assertFalse(error_lines[1:])

        self.logger.clear()
        self._do_test_GET_namespaces_bad_response_body('not a list')
        error_lines = self.logger.get_lines_for_level('error')
        start = 'Problem with container shard listing response from /v1/a/c?'
        msg, _, _ = error_lines[0].partition(':')
        self.assertEqual(start, msg[:len(start)])
        actual_qs = msg[len(start):]
        actual_params = dict(
            urllib.parse.parse_qsl(actual_qs, keep_blank_values=True))
        self.assertEqual({'format': 'json', 'states': 'listing'},
                         actual_params)
        self.assertFalse(error_lines[1:])

    def _do_test_GET_namespaces_cache_unused(self, sharding_state, req_params,
                                             req_hdrs=None):
        # verify cases when a GET request does not lookup in cache or attempt
        # to cache namespaces fetched from backend
        self.memcache.delete_all()
        self.memcache.clear_calls()
        req_params.update(dict(marker='egg', end_marker='jam'))
        hdrs = {'X-Backend-Record-Type': ''}
        if req_hdrs:
            hdrs.update(req_hdrs)

        req = self._build_request(hdrs, req_params, {})
        resp_shards = self._stub_namespaces[:2]

        resp_headers = {'X-Backend-Record-Type': 'shard',
                        'X-Backend-Sharding-State': sharding_state}
        resp_headers.update(self.RESP_SHARD_FORMAT_HEADERS)
        backend_req, resp = self._capture_backend_request(
            req, 200, json.dumps(resp_shards).encode('ascii'),
            resp_headers)

        exp_backend_req_hdrs = dict(hdrs)
        exp_backend_req_hdrs.update({
            'X-Backend-Record-Type': 'auto',
            'X-Backend-Record-Shard-Format': 'namespace',
            'X-Backend-Include-Deleted': 'false',
        })
        self._check_backend_req(
            req, backend_req, extra_hdrs=exp_backend_req_hdrs,
            extra_params=req_params)
        self._check_response(resp, self.bogus_listing, {
            'X-Backend-Recheck-Container-Existence': '60',
            'X-Backend-Sharding-State': sharding_state})
        self.assertEqual([self.namespaces[:2]], self.get_from_shards_lists)

    def _do_test_GET_namespaces_cache_unused_listing(self, sharding_state):
        # container metadata from backend response is set in memcache
        self._do_test_GET_namespaces_cache_unused(sharding_state,
                                                  {'states': 'listing'})
        self.assertEqual(
            [mock.call.get('container/a/c'),
             mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual(sharding_state,
                         self.memcache.calls[1][1][1]['sharding_state'])

    def test_GET_namespaces_cache_unused_recheck_listing_shard_ranges(self):
        # verify that a GET does not lookup or store namespaces in cache when
        # cache expiry time is set to  zero
        self.app.recheck_listing_shard_ranges = 0
        self._do_test_GET_namespaces_cache_unused_listing('unsharded')
        self._do_test_GET_namespaces_cache_unused_listing('sharding')
        self._do_test_GET_namespaces_cache_unused_listing('sharded')
        self._do_test_GET_namespaces_cache_unused_listing('collapsed')
        self._do_test_GET_namespaces_cache_unused_listing('unexpected')

    def test_GET_namespaces_no_memcache_available(self):
        req_hdrs = {'X-Backend-Record-Type': ''}
        params = {'states': 'listing'}
        req = self._build_request(req_hdrs, params, {})
        req.environ['swift.cache'] = None

        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Sharding-State': 'sharded'}
        resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)
        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_namespaces_dump, resp_hdrs)

        exp_backend_req_hdrs = dict(req_hdrs)
        exp_backend_req_hdrs.update(
            {'X-Backend-Record-Type': 'auto',
             'X-Backend-Record-Shard-Format': 'namespace',
             'X-Backend-Include-Deleted': 'false',
             })
        self._check_backend_req(
            req, backend_req, extra_params=params,
            extra_hdrs=exp_backend_req_hdrs)
        self._check_response(resp, self.bogus_listing, {
            'X-Backend-Recheck-Container-Existence': '60',
            'X-Backend-Sharding-State': 'sharded'})
        self.assertEqual([self.namespaces], self.get_from_shards_lists)
        self.assertEqual([], self.memcache.calls)  # sanity check

    def test_cache_clearing(self):
        # verify that both metadata and shard ranges are purged from memcache
        # on PUT, POST and DELETE
        def do_test(method, resp_status, num_resp):
            self.assertGreater(num_resp, 0)  # sanity check
            memcache = FakeMemcache()
            cont_key = get_cache_key('a', 'c')
            shard_key = get_cache_key('a', 'c', shard='listing')
            memcache.set(cont_key, 'container info', 60)
            memcache.set(shard_key, 'shard ranges', 600)
            req = Request.blank('/v1/a/c', method=method)
            req.environ['swift.cache'] = memcache
            self.assertIn(cont_key, req.environ['swift.cache'].store)
            self.assertIn(shard_key, req.environ['swift.cache'].store)
            resp_status = [resp_status] * num_resp
            with mocked_http_conn(
                    *resp_status, body_iter=[b''] * num_resp,
                    headers=[{}] * num_resp):
                resp = self._call_app(req)
            self.assertEqual(resp_status[0], resp.status_int)
            self.assertNotIn(cont_key, req.environ['swift.cache'].store)
            self.assertNotIn(shard_key, req.environ['swift.cache'].store)
        do_test('DELETE', 204, self.CONTAINER_REPLICAS)
        do_test('POST', 204, self.CONTAINER_REPLICAS)
        do_test('PUT', 202, self.CONTAINER_REPLICAS)


class TestGetPathNamespaceCachingLegacy(TestGetPathNamespaceCaching):
    # old container servers did not return this header
    RESP_SHARD_FORMAT_HEADERS = {}

    def setUp(self):
        super(TestGetPathNamespaceCachingLegacy, self).setUp()

    def _setup_namespace_stubs(self):
        # old container servers always returned full format ShardRange dicts
        self._stub_namespaces = [
            dict(ShardRange(timestamp=Timestamp.now(), **ns))
            for ns in self.ns_dicts]
        self._stub_namespaces_dump = json.dumps(self._stub_namespaces).encode(
            'ascii')


class TestGetExplicitRecordType(BaseTestContainerControllerGetPath):
    RESP_SHARD_FORMAT_HEADERS = {'X-Backend-Record-Shard-Format': 'full'}

    def setUp(self):
        super(TestGetExplicitRecordType, self).setUp()
        self._setup_shard_range_stubs()

    def _setup_shard_range_stubs(self):
        self._stub_shards = [dict(ShardRange(timestamp=Timestamp.now(), **ns))
                             for ns in self.ns_dicts]
        self._stub_shards_dump = json.dumps(self.ns_dicts).encode('ascii')

    def _do_test_GET_shard_ranges_no_cache(self, sharding_state, req_params,
                                           req_hdrs=None):
        # verify that an explicit shard GET request does not lookup in cache or
        # attempt to cache shard ranges fetched from backend
        self.memcache.delete_all()
        self.memcache.clear_calls()
        req_params.update(dict(marker='egg', end_marker='jam'))
        hdrs = {'X-Backend-Record-Type': 'shard'}
        if req_hdrs:
            hdrs.update(req_hdrs)

        req = self._build_request(hdrs, req_params, {})
        resp_shards = self._stub_shards[:2]

        resp_headers = {'X-Backend-Record-Type': 'shard',
                        'X-Backend-Sharding-State': sharding_state}
        resp_headers.update(self.RESP_SHARD_FORMAT_HEADERS)
        backend_req, resp = self._capture_backend_request(
            req, 200, json.dumps(resp_shards).encode('ascii'),
            resp_headers)

        exp_backend_req_hdrs = dict(hdrs)
        exp_backend_req_hdrs.update({
            'X-Backend-Record-Type': 'shard',
        })
        self._check_backend_req(
            req, backend_req, extra_hdrs=exp_backend_req_hdrs,
            extra_params=req_params)
        expected_shards = self._stub_shards[:2]
        exp_resp_hdrs = dict(resp_headers)
        exp_resp_hdrs.update(self.RESP_SHARD_FORMAT_HEADERS)
        exp_resp_hdrs['X-Backend-Recheck-Container-Existence'] = '60'
        self._check_response(resp, expected_shards, exp_resp_hdrs)

    def _do_test_GET_shard_ranges_no_cache_updating(self, sharding_state):
        # container metadata from backend response is set in memcache
        self._do_test_GET_shard_ranges_no_cache(sharding_state,
                                                {'states': 'updating'})
        self.assertEqual(
            [mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual(sharding_state,
                         self.memcache.calls[0][1][1]['sharding_state'])

    def test_GET_shard_ranges_no_cache_when_requesting_updating_shards(self):
        # verify that a GET for shard record type in updating states does not
        # lookup or store in cache
        self._do_test_GET_shard_ranges_no_cache_updating('unsharded')
        self._do_test_GET_shard_ranges_no_cache_updating('sharding')
        self._do_test_GET_shard_ranges_no_cache_updating('sharded')
        self._do_test_GET_shard_ranges_no_cache_updating('collapsed')
        self._do_test_GET_shard_ranges_no_cache_updating('unexpected')

    def _do_test_GET_shard_ranges_no_cache_listing(self, sharding_state):
        # container metadata from backend response is set in memcache
        self._do_test_GET_shard_ranges_no_cache(sharding_state,
                                                {'states': 'listing'})
        self.assertEqual(
            [mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual(sharding_state,
                         self.memcache.calls[0][1][1]['sharding_state'])

    def test_GET_shard_ranges_no_cache_when_requesting_listing_shards(self):
        # verify that a GET for shard record type in listing states does not
        # lookup or store in cache
        self._do_test_GET_shard_ranges_no_cache_listing('unsharded')
        self._do_test_GET_shard_ranges_no_cache_listing('sharding')
        self._do_test_GET_shard_ranges_no_cache_listing('sharded')
        self._do_test_GET_shard_ranges_no_cache_listing('collapsed')
        self._do_test_GET_shard_ranges_no_cache_listing('unexpected')

    def test_GET_shard_ranges_no_cache_when_include_deleted_shards(self):
        # verify that a GET for shards in listing states does not lookup or
        # store in cache if x-backend-include-deleted is true
        self._do_test_GET_shard_ranges_no_cache(
            'unsharded', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shard_ranges_no_cache(
            'sharding', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shard_ranges_no_cache(
            'sharded', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shard_ranges_no_cache(
            'collapsed', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shard_ranges_no_cache(
            'unexpected', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})

    def test_GET_record_type_object_makes_no_cache_lookup(self):
        # verify that an GET request explicitly asking for record-type 'object'
        # does not lookup container metadata in cache
        req_hdrs = {'X-Backend-Record-Type': 'object'}
        # we would not expect states=listing to be used with an object request
        # but include it here to verify that it is ignored
        req = self._build_request(req_hdrs, {'states': 'listing'}, {})
        resp_body = json.dumps(['object listing']).encode('ascii')
        backend_req, resp = self._capture_backend_request(
            req, 200, resp_body,
            {'X-Backend-Record-Type': 'object',
             'X-Backend-Sharding-State': 'sharded'})
        self._check_backend_req(
            req, backend_req,
            extra_hdrs=req_hdrs)
        self._check_response(resp, ['object listing'], {
            'X-Backend-Recheck-Container-Existence': '60',
            'X-Backend-Record-Type': 'object',
            'X-Backend-Sharding-State': 'sharded'})
        # container metadata from backend response is set in memcache
        self.assertEqual(
            [mock.call.set('container/a/c', mock.ANY, time=60)],
            self.memcache.calls)
        self.assertEqual('sharded',
                         self.memcache.calls[0][1][1]['sharding_state'])


class TestGetExplicitRecordTypeLegacy(TestGetExplicitRecordType):
    # old container servers did not return this header
    RESP_SHARD_FORMAT_HEADERS = {}


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
