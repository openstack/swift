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
import six
from six.moves import urllib

from swift.common.constraints import CONTAINER_LISTING_LIMIT
from swift.common.swob import Request, bytes_to_wsgi, str_to_wsgi, wsgi_quote
from swift.common.utils import ShardRange, Timestamp
from swift.proxy import server as proxy_server
from swift.proxy.controllers.base import headers_to_container_info, \
    Controller, get_container_info, get_cache_key
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

        with mocked_http_conn(), mock.patch.object(self.app, 'iter_nodes',
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

    def _make_shard_objects(self, shard_range):
        if six.PY2:
            lower = ord(shard_range.lower.decode('utf8')[0]
                        if shard_range.lower else '@')
            upper = ord(shard_range.upper.decode('utf8')[0]
                        if shard_range.upper else u'\U0001ffff')
        else:
            lower = ord(shard_range.lower[0] if shard_range.lower else '@')
            upper = ord(shard_range.upper[0] if shard_range.upper
                        else '\U0001ffff')

        objects = [{'name': six.unichr(i), 'bytes': i,
                    'hash': 'hash%s' % six.unichr(i),
                    'content_type': 'text/plain', 'deleted': 0,
                    'last_modified': next(self.ts_iter).isoformat}
                   for i in range(lower + 1, upper + 1)][:1024]
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
                if six.PY2:
                    got_params = dict(urllib.parse.parse_qsl(req['qs'], True))
                else:
                    got_params = dict(urllib.parse.parse_qsl(
                        req['qs'], True, encoding='latin1'))
                self.assertEqual(dict(exp_params, format='json'), got_params)
                for k, v in exp_headers.items():
                    self.assertIn(k, req['headers'])
                    self.assertEqual(v, req['headers'][k], k)
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
        # Don't worry, ShardRange._encode takes care of unicode/bytes issues
        shard_bounds = ('', 'ham', 'pie', u'\N{SNOWMAN}', u'\U0001F334', '')
        shard_ranges = [
            ShardRange('.shards_a/c_%s' % upper, Timestamp.now(), lower, upper)
            for lower, upper in zip(shard_bounds[:-1], shard_bounds[1:])]
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        shard_resp_hdrs = [
            {'X-Backend-Sharding-State': 'unsharded',
             'X-Container-Object-Count': len(sr_objs[i]),
             'X-Container-Bytes-Used':
                 sum([obj['bytes'] for obj in sr_objs[i]]),
             'X-Container-Meta-Flavour': 'flavour%d' % i,
             'X-Backend-Storage-Policy-Index': 0}
            for i, _ in enumerate(shard_ranges)]

        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Backend-Timestamp': '99',
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
            (wsgi_quote(str_to_wsgi(shard_ranges[0].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='ham\x00', limit=str(limit),
                  states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[1].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[2].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='\xd1\xb0', end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[4].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='\xe2\xa8\x83', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1] + sr_objs[2]
                                        + sr_objs[3])))),  # 200
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
            (200, sr_objs[2] + sr_objs[3] + sr_objs[4], root_resp_hdrs)
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

        # GET all objects in reverse and *blank* limit
        mock_responses = [
            # status, body, headers
            (200, list(reversed(sr_dicts)), root_shard_resp_hdrs),
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
            (wsgi_quote(str_to_wsgi(shard_ranges[4].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='', end_marker='\xf0\x9f\x8c\xb4', states='listing',
                  reverse='true', limit=str(limit))),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='\xf0\x9f\x8c\xb5', end_marker='\xe2\x98\x83',
                  states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[4])))),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[2].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='\xe2\x98\x84', end_marker='pie', states='listing',
                  reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3])))),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[1].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='q', end_marker='ham', states='listing',
                  reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3]
                                        + sr_objs[2])))),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[0].name)),
             {'X-Backend-Record-Type': 'auto'},
             dict(marker='i', end_marker='', states='listing', reverse='true',
                  limit=str(limit - len(sr_objs[4] + sr_objs[3] + sr_objs[2]
                                        + sr_objs[1])))),  # 200
        ]

        resp = self._check_GET_shard_listing(
            mock_responses, list(reversed(expected_objects)),
            expected_requests, query_string='?reverse=true&limit=',
            reverse=True)
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
            (wsgi_quote(str_to_wsgi(shard_ranges[0].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (wsgi_quote(str_to_wsgi(shard_ranges[1].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(shard_ranges[2].name)),
             {'X-Backend-Record-Type': 'auto'},   # 200
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?limit=%s' % limit)
        self.check_response(resp, root_resp_hdrs)

        # GET with marker
        marker = bytes_to_wsgi(sr_objs[3][2]['name'].encode('utf8'))
        first_included = (len(sr_objs[0]) + len(sr_objs[1])
                          + len(sr_objs[2]) + 2)
        limit = CONTAINER_LISTING_LIMIT
        expected_objects = all_objects[first_included:]
        mock_responses = [
            (404, '', {}),
            (200, sr_dicts[3:], root_shard_resp_hdrs),
            (404, '', {}),
            (200, sr_objs[3][2:], shard_resp_hdrs[3]),
            (200, sr_objs[4], shard_resp_hdrs[4]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=marker, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=marker, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker=marker, end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing', limit=str(limit))),
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker=marker, end_marker='\xf0\x9f\x8c\xb4\x00',
                  states='listing', limit=str(limit))),
            (wsgi_quote(str_to_wsgi(shard_ranges[4].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='\xe2\xa8\x83', end_marker='', states='listing',
                  limit=str(limit - len(sr_objs[3][2:])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s' % marker)
        self.check_response(resp, root_resp_hdrs)

        # GET with end marker
        end_marker = bytes_to_wsgi(sr_objs[3][6]['name'].encode('utf8'))
        first_excluded = (len(sr_objs[0]) + len(sr_objs[1])
                          + len(sr_objs[2]) + 6)
        expected_objects = all_objects[:first_excluded]
        mock_responses = [
            (404, '', {}),
            (200, sr_dicts[:4], root_shard_resp_hdrs),
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
            (wsgi_quote(str_to_wsgi(shard_ranges[0].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='', end_marker='ham\x00', states='listing',
                  limit=str(limit))),
            (wsgi_quote(str_to_wsgi(shard_ranges[1].name)),
             {'X-Backend-Record-Type': 'auto'},  # 404
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(shard_ranges[1].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='h', end_marker='pie\x00', states='listing',
                  limit=str(limit - len(sr_objs[0])))),
            (wsgi_quote(str_to_wsgi(shard_ranges[2].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='p', end_marker='\xe2\x98\x83\x00', states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1])))),
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},  # 404
             dict(marker='\xd1\xb0', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker='\xd1\xb0', end_marker=end_marker, states='listing',
                  limit=str(limit - len(sr_objs[0] + sr_objs[1]
                                        + sr_objs[2])))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?end_marker=%s' % end_marker)
        self.check_response(resp, root_resp_hdrs)

        # GET with prefix
        prefix = 'hat'
        # they're all 1-character names; the important thing
        # is which shards we query
        expected_objects = []
        mock_responses = [
            (404, '', {}),
            (200, sr_dicts, root_shard_resp_hdrs),
            (200, [], shard_resp_hdrs[1]),
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(prefix=prefix, states='listing')),  # 404
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(prefix=prefix, states='listing')),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[1].name)),
             {'X-Backend-Record-Type': 'auto'},  # 404
             dict(prefix=prefix, marker='', end_marker='pie\x00',
                  states='listing', limit=str(limit))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?prefix=%s' % prefix)
        self.check_response(resp, root_resp_hdrs)

        # marker and end_marker and limit
        limit = 2
        expected_objects = all_objects[first_included:first_excluded]
        mock_responses = [
            (200, sr_dicts[3:4], root_shard_resp_hdrs),
            (200, sr_objs[3][2:6], shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing', limit=str(limit),
                  marker=marker, end_marker=end_marker)),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
             dict(marker=marker, end_marker=end_marker, states='listing',
                  limit=str(limit))),
        ]
        resp = self._check_GET_shard_listing(
            mock_responses, expected_objects, expected_requests,
            query_string='?marker=%s&end_marker=%s&limit=%s'
            % (marker, end_marker, limit))
        self.check_response(resp, root_resp_hdrs)

        # reverse with marker, end_marker, and limit
        expected_objects.reverse()
        mock_responses = [
            (200, sr_dicts[3:4], root_shard_resp_hdrs),
            (200, list(reversed(sr_objs[3][2:6])), shard_resp_hdrs[1])
        ]
        expected_requests = [
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(marker=end_marker, reverse='true', end_marker=marker,
                  limit=str(limit), states='listing',)),  # 200
            (wsgi_quote(str_to_wsgi(shard_ranges[3].name)),
             {'X-Backend-Record-Type': 'auto'},  # 200
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
                          'X-Backend-Timestamp': '99',
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

    def test_GET_sharded_container_shard_redirects_to_root(self):
        # check that if the root redirects listing to a shard, but the shard
        # returns the root shard (e.g. it was the final shard to shrink into
        # the root) objects are requested from the root, rather than a loop.

        # single shard spanning entire namespace
        shard_sr = ShardRange('.shards_a/c_xyz', Timestamp.now(), '', '')
        all_objects = self._make_shard_objects(shard_sr)
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
        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Backend-Storage-Policy-Index': 0}
        root_shard_resp_hdrs = dict(root_resp_hdrs)
        root_shard_resp_hdrs['X-Backend-Record-Type'] = 'shard'

        root_sr = ShardRange('a/c', Timestamp.now(), '', '')
        mock_responses = [
            # status, body, headers
            (200, [dict(shard_sr)], root_shard_resp_hdrs),  # from root
            (200, [dict(root_sr)], shard_resp_hdrs),  # from shard
            (200, all_objects, root_resp_hdrs),  # from root
        ]
        expected_requests = [
            # path, headers, params
            # first request to root should specify auto record type
            ('a/c', {'X-Backend-Record-Type': 'auto'},
             dict(states='listing')),
            # request to shard should specify auto record type
            (wsgi_quote(str_to_wsgi(shard_sr.name)),
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
        self.check_response(resp, root_resp_hdrs,
                            expected_objects=expected_objects)
        self.assertEqual(
            [('a', 'c'), ('.shards_a', 'c_xyz')],
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
        shard_ranges = [
            ShardRange('.shards_a/c_%s' % upper, Timestamp.now(), lower, upper)
            for lower, upper in zip(shard_bounds[:-1], shard_bounds[1:])]
        self.assertEqual([
            '.shards_a/c_a',
            '.shards_a/c_b',
            '.shards_a/c_',
        ], [sr.name for sr in shard_ranges])
        sr_dicts = [dict(sr) for sr in shard_ranges]
        sr_objs = [self._make_shard_objects(sr) for sr in shard_ranges]
        all_objects = []
        for objects in sr_objs:
            all_objects.extend(objects)
        size_all_objects = sum([obj['bytes'] for obj in all_objects])
        num_all_objects = len(all_objects)

        root_resp_hdrs = {'X-Backend-Sharding-State': 'sharded',
                          'X-Backend-Timestamp': '99',
                          'X-Container-Object-Count': num_all_objects,
                          'X-Container-Bytes-Used': size_all_objects,
                          'X-Backend-Storage-Policy-Index': 0,
                          'X-Backend-Record-Type': 'shard',
                          }
        shard_resp_hdrs = {'X-Backend-Sharding-State': 'unsharded',
                           'X-Container-Object-Count': 2,
                           'X-Container-Bytes-Used': 4,
                           'X-Backend-Storage-Policy-Index': 0}
        shrinking_resp_hdrs = {
            'X-Backend-Sharding-State': 'sharded',
            'X-Backend-Record-Type': 'shard',
        }
        limit = CONTAINER_LISTING_LIMIT

        mock_responses = [
            # status, body, headers
            (200, sr_dicts, root_resp_hdrs),  # from root
            (200, sr_objs[0], shard_resp_hdrs),  # objects from 1st shard
            (200, [sr_dicts[2]], shrinking_resp_hdrs),  # 2nd points to 3rd
            (200, [sr_dicts[1]], shrinking_resp_hdrs),  # 3rd points to 2nd
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
        self.check_response(resp, root_resp_hdrs,
                            expected_objects=all_objects)
        self.assertEqual(
            [('a', 'c'), ('.shards_a', 'c_b'), ('.shards_a', 'c_')],
            resp.request.environ.get('swift.shard_listing_history'))

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
                          'X-Backend-Timestamp': '99',
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
                          'X-Backend-Timestamp': '99',
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
                          'X-Backend-Timestamp': '99',
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
                          'X-Backend-Timestamp': '99',
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
                          'X-Backend-Timestamp': '99',
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
                          'X-Backend-Timestamp': '99',
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
                          'X-Backend-Timestamp': '99',
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

    def _build_request(self, headers, params, infocache=None):
        # helper to make a GET request with caches set in environ
        query_string = '?' + ';'.join('%s=%s' % (k, v)
                                      for k, v in params.items())
        container_path = '/v1/a/c' + query_string
        request = Request.blank(container_path, headers=headers)
        request.environ['swift.cache'] = self.memcache
        request.environ['swift.infocache'] = infocache if infocache else {}
        return request

    def _check_response(self, resp, exp_shards, extra_hdrs):
        # helper to check a shard listing response
        actual_shards = json.loads(resp.body)
        self.assertEqual(exp_shards, actual_shards)
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
                 str(len(json.dumps(exp_shards).encode('ascii'))),
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
            resp = req.get_response(self.app)
        self.assertEqual(resp_status[0], resp.status_int)
        self.assertEqual(num_resp, len(fake_conn.requests))
        return fake_conn.requests[0], resp

    def _check_backend_req(self, req, backend_req, extra_params=None,
                           extra_hdrs=None):
        self.assertEqual('a/c', backend_req['path'][7:])

        expected_params = {'states': 'listing', 'format': 'json'}
        if extra_params:
            expected_params.update(extra_params)
        if six.PY2:
            backend_params = dict(urllib.parse.parse_qsl(
                backend_req['qs'], True))
        else:
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

    def _setup_shard_range_stubs(self):
        self.memcache = FakeMemcache()
        shard_bounds = (('', 'ham'), ('ham', 'pie'), ('pie', ''))
        shard_ranges = [
            ShardRange('.shards_a/c_%s' % upper, Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds]
        self.sr_dicts = [dict(sr) for sr in shard_ranges]
        self._stub_shards_dump = json.dumps(self.sr_dicts).encode('ascii')
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

    def _do_test_caching(self, record_type, exp_recheck_listing):
        # this test gets shard ranges into cache and then reads from cache
        sharding_state = 'sharded'
        self.memcache.delete_all()
        self.memcache.clear_calls()
        # container is sharded but proxy does not have that state cached;
        # expect a backend request and expect shard ranges to be cached
        self.memcache.clear_calls()
        req = self._build_request({'X-Backend-Record-Type': record_type},
                                  {'states': 'listing'}, {})
        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_shards_dump,
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Sharding-State': sharding_state,
             'X-Backend-Override-Shard-Name-Filter': 'true'})
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': record_type,
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        self._check_response(resp, self.sr_dicts, {
            'X-Backend-Recheck-Container-Existence': '60',
            'X-Backend-Record-Type': 'shard',
            'X-Backend-Sharding-State': sharding_state})
        self.assertEqual(
            [('get', 'container/a/c', None, None),
             ('set', 'shard-listing/a/c', self.sr_dicts,
              exp_recheck_listing),
             ('set', 'container/a/c', mock.ANY, 60)],
            self.memcache.calls)
        self.assertEqual(self.sr_dicts, self.memcache.calls[1][2])
        self.assertEqual(sharding_state,
                         self.memcache.calls[2][2]['sharding_state'])
        self.assertIn('swift.infocache', req.environ)
        self.assertIn('shard-listing/a/c', req.environ['swift.infocache'])
        self.assertEqual(tuple(self.sr_dicts),
                         req.environ['swift.infocache']['shard-listing/a/c'])

        # container is sharded and proxy does have that state cached and
        # also has shard ranges cached; expect a read from cache
        self.memcache.clear_calls()
        req = self._build_request({'X-Backend-Record-Type': record_type},
                                  {'states': 'listing'}, {})
        resp = req.get_response(self.app)
        self._check_response(resp, self.sr_dicts, {
            'X-Backend-Cached-Results': 'true',
            'X-Backend-Record-Type': 'shard',
            'X-Backend-Sharding-State': sharding_state})
        self.assertEqual(
            [('get', 'container/a/c', None, None),
             ('get', 'shard-listing/a/c', None, None)],
            self.memcache.calls)
        self.assertIn('swift.infocache', req.environ)
        self.assertIn('shard-listing/a/c', req.environ['swift.infocache'])
        self.assertEqual(tuple(self.sr_dicts),
                         req.environ['swift.infocache']['shard-listing/a/c'])

        # delete the container; check that shard ranges are evicted from cache
        self.memcache.clear_calls()
        infocache = {}
        req = Request.blank('/v1/a/c', method='DELETE')
        req.environ['swift.cache'] = self.memcache
        req.environ['swift.infocache'] = infocache
        self._capture_backend_request(req, 204, b'', {},
                                      num_resp=self.CONTAINER_REPLICAS)
        self.assertEqual(
            [('delete', 'container/a/c', None, None),
             ('delete', 'shard-listing/a/c', None, None)],
            self.memcache.calls)

    def test_GET_shard_ranges(self):
        self._setup_shard_range_stubs()
        # expect shard ranges cache time to be default value of 600
        self._do_test_caching('shard', 600)
        # expect shard ranges cache time to be configured value of 120
        self.app.recheck_listing_shard_ranges = 120
        self._do_test_caching('shard', 120)

        def mock_get_from_shards(self, req, resp):
            # for the purposes of these tests we override _get_from_shards so
            # that the response contains the shard listing even though the
            # record_type is 'auto'; these tests are verifying the content and
            # caching of the backend shard range response so we're not
            # interested in gathering object from the shards
            return resp

        with mock.patch('swift.proxy.controllers.container.'
                        'ContainerController._get_from_shards',
                        mock_get_from_shards):
            self.app.recheck_listing_shard_ranges = 600
            self._do_test_caching('auto', 600)

    def test_GET_shard_ranges_404_response(self):
        # pre-warm cache with container info but not shard ranges so that the
        # backend request tries to get a cacheable listing, but backend 404's
        self._setup_shard_range_stubs()
        self.memcache.delete_all()
        info = headers_to_container_info(self.root_resp_hdrs)
        info['status'] = 200
        info['sharding_state'] = 'sharded'
        self.memcache.set('container/a/c', info)
        self.memcache.clear_calls()
        req = self._build_request({'X-Backend-Record-Type': 'shard'},
                                  {'states': 'listing'}, {})
        backend_req, resp = self._capture_backend_request(
            req, 404, b'', {}, num_resp=2 * self.CONTAINER_REPLICAS)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'shard',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        self.assertNotIn('X-Backend-Cached-Results', resp.headers)
        # Note: container metadata is updated in cache but shard ranges are not
        # deleted from cache
        self.assertEqual(
            [('get', 'container/a/c', None, None),
             ('get', 'shard-listing/a/c', None, None),
             ('set', 'container/a/c', mock.ANY, 6.0)],
            self.memcache.calls)
        self.assertEqual(404, self.memcache.calls[2][2]['status'])
        self.assertEqual(b'', resp.body)
        self.assertEqual(404, resp.status_int)

    def _do_test_GET_shard_ranges_read_from_cache(self, params, record_type):
        # pre-warm cache with container metadata and shard ranges and verify
        # that shard range listing are read from cache when appropriate
        self.memcache.delete_all()
        info = headers_to_container_info(self.root_resp_hdrs)
        info['status'] = 200
        info['sharding_state'] = 'sharded'
        self.memcache.set('container/a/c', info)
        self.memcache.set('shard-listing/a/c', self.sr_dicts)
        self.memcache.clear_calls()

        req_hdrs = {'X-Backend-Record-Type': record_type}
        req = self._build_request(req_hdrs, params, {})
        resp = req.get_response(self.app)
        self.assertEqual(
            [('get', 'container/a/c', None, None),
             ('get', 'shard-listing/a/c', None, None)],
            self.memcache.calls)
        return resp

    def test_GET_shard_ranges_read_from_cache(self):
        self._setup_shard_range_stubs()
        exp_hdrs = {'X-Backend-Cached-Results': 'true',
                    'X-Backend-Record-Type': 'shard',
                    'X-Backend-Override-Shard-Name-Filter': 'true',
                    'X-Backend-Sharding-State': 'sharded'}

        resp = self._do_test_GET_shard_ranges_read_from_cache(
            {'states': 'listing'}, 'shard')
        self._check_response(resp, self.sr_dicts, exp_hdrs)

        resp = self._do_test_GET_shard_ranges_read_from_cache(
            {'states': 'listing', 'reverse': 'true'}, 'shard')
        exp_shards = list(self.sr_dicts)
        exp_shards.reverse()
        self._check_response(resp, exp_shards, exp_hdrs)

        resp = self._do_test_GET_shard_ranges_read_from_cache(
            {'states': 'listing', 'marker': 'jam'}, 'shard')
        self._check_response(resp, self.sr_dicts[1:], exp_hdrs)

        resp = self._do_test_GET_shard_ranges_read_from_cache(
            {'states': 'listing', 'marker': 'jam', 'end_marker': 'kale'},
            'shard')
        self._check_response(resp, self.sr_dicts[1:2], exp_hdrs)

        resp = self._do_test_GET_shard_ranges_read_from_cache(
            {'states': 'listing', 'includes': 'egg'}, 'shard')
        self._check_response(resp, self.sr_dicts[:1], exp_hdrs)

        # override _get_from_shards so that the response contains the shard
        # listing that we want to verify even though the record_type is 'auto'
        def mock_get_from_shards(self, req, resp):
            return resp

        with mock.patch('swift.proxy.controllers.container.'
                        'ContainerController._get_from_shards',
                        mock_get_from_shards):
            resp = self._do_test_GET_shard_ranges_read_from_cache(
                {'states': 'listing', 'reverse': 'true'}, 'auto')
            exp_shards = list(self.sr_dicts)
            exp_shards.reverse()
            self._check_response(resp, exp_shards, exp_hdrs)

            resp = self._do_test_GET_shard_ranges_read_from_cache(
                {'states': 'listing', 'marker': 'jam'}, 'auto')
            self._check_response(resp, self.sr_dicts[1:], exp_hdrs)

            resp = self._do_test_GET_shard_ranges_read_from_cache(
                {'states': 'listing', 'marker': 'jam', 'end_marker': 'kale'},
                'auto')
            self._check_response(resp, self.sr_dicts[1:2], exp_hdrs)

            resp = self._do_test_GET_shard_ranges_read_from_cache(
                {'states': 'listing', 'includes': 'egg'}, 'auto')
            self._check_response(resp, self.sr_dicts[:1], exp_hdrs)

    def _do_test_GET_shard_ranges_write_to_cache(self, params, record_type):
        # verify that shard range listing are written to cache when appropriate
        self.memcache.delete_all()
        self.memcache.clear_calls()
        # set request up for cacheable listing
        req_hdrs = {'X-Backend-Record-Type': record_type}
        req = self._build_request(req_hdrs, params, {})
        # response indicates cacheable listing
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Shard-Name-Filter': 'true',
                     'X-Backend-Sharding-State': 'sharded'}
        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_shards_dump, resp_hdrs)
        self._check_backend_req(
            req, backend_req,
            extra_params=params,
            extra_hdrs={'X-Backend-Record-Type': record_type,
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        expected_hdrs = {'X-Backend-Recheck-Container-Existence': '60'}
        expected_hdrs.update(resp_hdrs)
        self.assertEqual(
            [('get', 'container/a/c', None, None),
             ('set', 'shard-listing/a/c', self.sr_dicts, 600),
             ('set', 'container/a/c', mock.ANY, 60)],
            self.memcache.calls)
        # shards were cached
        self.assertEqual(self.sr_dicts, self.memcache.calls[1][2])
        self.assertEqual('sharded',
                         self.memcache.calls[2][2]['sharding_state'])
        return resp

    def test_GET_shard_ranges_write_to_cache(self):
        self._setup_shard_range_stubs()
        exp_hdrs = {'X-Backend-Recheck-Container-Existence': '60',
                    'X-Backend-Record-Type': 'shard',
                    'X-Backend-Override-Shard-Name-Filter': 'true',
                    'X-Backend-Sharding-State': 'sharded'}

        resp = self._do_test_GET_shard_ranges_write_to_cache(
            {'states': 'listing'}, 'shard')
        self._check_response(resp, self.sr_dicts, exp_hdrs)

        resp = self._do_test_GET_shard_ranges_write_to_cache(
            {'states': 'listing', 'reverse': 'true'}, 'shard')
        exp_shards = list(self.sr_dicts)
        exp_shards.reverse()
        self._check_response(resp, exp_shards, exp_hdrs)

        resp = self._do_test_GET_shard_ranges_write_to_cache(
            {'states': 'listing', 'marker': 'jam'}, 'shard')
        self._check_response(resp, self.sr_dicts[1:], exp_hdrs)

        resp = self._do_test_GET_shard_ranges_write_to_cache(
            {'states': 'listing', 'marker': 'jam', 'end_marker': 'kale'},
            'shard')
        self._check_response(resp, self.sr_dicts[1:2], exp_hdrs)

        resp = self._do_test_GET_shard_ranges_write_to_cache(
            {'states': 'listing', 'includes': 'egg'}, 'shard')
        self._check_response(resp, self.sr_dicts[:1], exp_hdrs)

        # override _get_from_shards so that the response contains the shard
        # listing that we want to verify even though the record_type is 'auto'
        def mock_get_from_shards(self, req, resp):
            return resp

        with mock.patch('swift.proxy.controllers.container.'
                        'ContainerController._get_from_shards',
                        mock_get_from_shards):
            resp = self._do_test_GET_shard_ranges_write_to_cache(
                {'states': 'listing', 'reverse': 'true'}, 'auto')
            exp_shards = list(self.sr_dicts)
            exp_shards.reverse()
            self._check_response(resp, exp_shards, exp_hdrs)

            resp = self._do_test_GET_shard_ranges_write_to_cache(
                {'states': 'listing', 'marker': 'jam'}, 'auto')
            self._check_response(resp, self.sr_dicts[1:], exp_hdrs)

            resp = self._do_test_GET_shard_ranges_write_to_cache(
                {'states': 'listing', 'marker': 'jam', 'end_marker': 'kale'},
                'auto')
            self._check_response(resp, self.sr_dicts[1:2], exp_hdrs)

            resp = self._do_test_GET_shard_ranges_write_to_cache(
                {'states': 'listing', 'includes': 'egg'}, 'auto')
            self._check_response(resp, self.sr_dicts[:1], exp_hdrs)

    def test_GET_shard_ranges_write_to_cache_with_x_newest(self):
        # when x-newest is sent, verify that there is no cache lookup to check
        # sharding state but then backend requests are made requesting complete
        # shard list which can be cached
        self._setup_shard_range_stubs()
        self.memcache.delete_all()
        self.memcache.clear_calls()
        req_hdrs = {'X-Backend-Record-Type': 'shard',
                    'X-Newest': 'true'}
        params = {'states': 'listing'}
        req = self._build_request(req_hdrs, params, {})
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Shard-Name-Filter': 'true',
                     'X-Backend-Sharding-State': 'sharded'}
        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_shards_dump, resp_hdrs,
            num_resp=2 * self.CONTAINER_REPLICAS)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'shard',
                        'X-Newest': 'true',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        expected_hdrs = {'X-Backend-Recheck-Container-Existence': '60'}
        expected_hdrs.update(resp_hdrs)
        self._check_response(resp, self.sr_dicts, expected_hdrs)
        self.assertEqual(
            [('set', 'shard-listing/a/c', self.sr_dicts, 600),
             ('set', 'container/a/c', mock.ANY, 60)],
            self.memcache.calls)
        self.assertEqual(self.sr_dicts, self.memcache.calls[0][2])
        self.assertEqual('sharded',
                         self.memcache.calls[1][2]['sharding_state'])

    def _do_test_GET_shard_ranges_no_cache_write(self, resp_hdrs):
        # verify that there is a cache lookup to check container info but then
        # a backend request is made requesting complete shard list, but do not
        # expect shard ranges to be cached; check that marker, end_marker etc
        # are passed to backend
        self.memcache.clear_calls()
        req = self._build_request(
            {'X-Backend-Record-Type': 'shard'},
            {'states': 'listing', 'marker': 'egg', 'end_marker': 'jam',
             'reverse': 'true'}, {})
        resp_shards = self.sr_dicts[:2]
        resp_shards.reverse()
        backend_req, resp = self._capture_backend_request(
            req, 200, json.dumps(resp_shards).encode('ascii'),
            resp_hdrs)
        self._check_backend_req(
            req, backend_req,
            extra_params={'marker': 'egg', 'end_marker': 'jam',
                          'reverse': 'true'},
            extra_hdrs={'X-Backend-Record-Type': 'shard',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        expected_shards = self.sr_dicts[:2]
        expected_shards.reverse()
        expected_hdrs = {'X-Backend-Recheck-Container-Existence': '60'}
        expected_hdrs.update(resp_hdrs)
        self._check_response(resp, expected_shards, expected_hdrs)
        # container metadata is looked up in memcache for sharding state
        # container metadata is set in memcache
        self.assertEqual(
            [('get', 'container/a/c', None, None),
             ('set', 'container/a/c', mock.ANY, 60)],
            self.memcache.calls)
        self.assertEqual(resp.headers.get('X-Backend-Sharding-State'),
                         self.memcache.calls[1][2]['sharding_state'])
        self.memcache.delete_all()

    def test_GET_shard_ranges_no_cache_write_with_cached_container_info(self):
        # pre-warm cache with container info, but verify that shard range cache
        # lookup is only attempted when the cached sharding state and status
        # are suitable, and full set of headers can be constructed from cache;
        # Note: backend response has state unsharded so no shard ranges cached
        self._setup_shard_range_stubs()

        def do_test(info):
            self._setup_shard_range_stubs()
            self.memcache.set('container/a/c', info)
            # expect the same outcomes as if there was no cached container info
            self._do_test_GET_shard_ranges_no_cache_write(
                {'X-Backend-Record-Type': 'shard',
                 'X-Backend-Override-Shard-Name-Filter': 'true',
                 'X-Backend-Sharding-State': 'unsharded'})

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

    def test_GET_shard_ranges_no_cache_write_for_non_sharded_states(self):
        # verify that shard ranges are not written to cache when container
        # state returned by backend is not 'sharded'; we don't expect
        # 'X-Backend-Override-Shard-Name-Filter': 'true' to be returned unless
        # the sharding state is 'sharded' but include it in this test to check
        # that the state is checked by proxy controller
        self._setup_shard_range_stubs()
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'unsharded'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharding'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'collapsed'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'unexpected'})

    def test_GET_shard_ranges_no_cache_write_for_incomplete_listing(self):
        # verify that shard ranges are not written to cache when container
        # response does not acknowledge x-backend-override-shard-name-filter
        # e.g. container server not upgraded
        self._setup_shard_range_stubs()
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Override-Shard-Name-Filter': 'false',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Override-Shard-Name-Filter': 'rogue',
             'X-Backend-Sharding-State': 'sharded'})

    def test_GET_shard_ranges_no_cache_write_for_object_listing(self):
        # verify that shard ranges are not written to cache when container
        # response does not return shard ranges
        self._setup_shard_range_stubs()
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'object',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'other',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Record-Type': 'true',
             'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})
        self._do_test_GET_shard_ranges_no_cache_write(
            {'X-Backend-Override-Shard-Name-Filter': 'true',
             'X-Backend-Sharding-State': 'sharded'})

    def _do_test_GET_shard_ranges_bad_response_body(self, resp_body):
        # verify that resp body is not cached if shard range parsing fails;
        # check the original unparseable response body is returned
        self._setup_shard_range_stubs()
        self.memcache.clear_calls()
        req = self._build_request(
            {'X-Backend-Record-Type': 'shard'},
            {'states': 'listing'}, {})
        resp_hdrs = {'X-Backend-Record-Type': 'shard',
                     'X-Backend-Override-Shard-Name-Filter': 'true',
                     'X-Backend-Sharding-State': 'sharded'}
        backend_req, resp = self._capture_backend_request(
            req, 200, json.dumps(resp_body).encode('ascii'),
            resp_hdrs)
        self._check_backend_req(
            req, backend_req,
            extra_hdrs={'X-Backend-Record-Type': 'shard',
                        'X-Backend-Override-Shard-Name-Filter': 'sharded'})
        expected_hdrs = {'X-Backend-Recheck-Container-Existence': '60'}
        expected_hdrs.update(resp_hdrs)
        self._check_response(resp, resp_body, expected_hdrs)
        # container metadata is looked up in memcache for sharding state
        # container metadata is set in memcache
        self.assertEqual(
            [('get', 'container/a/c', None, None),
             ('set', 'container/a/c', mock.ANY, 60)],
            self.memcache.calls)
        self.assertEqual(resp.headers.get('X-Backend-Sharding-State'),
                         self.memcache.calls[1][2]['sharding_state'])
        self.memcache.delete_all()

    def test_GET_shard_ranges_bad_response_body(self):
        self._do_test_GET_shard_ranges_bad_response_body(
            {'bad': 'data', 'not': ' a list'})
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines), error_lines)
        self.assertIn('Problem with listing response', error_lines[0])

        self.logger.clear()
        self._do_test_GET_shard_ranges_bad_response_body(
            [{'not': ' a shard range'}])
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines), error_lines)
        self.assertIn('Failed to get shard ranges', error_lines[0])

        self.logger.clear()
        self._do_test_GET_shard_ranges_bad_response_body(
            'not json')
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines), error_lines)
        self.assertIn('Problem with listing response', error_lines[0])

    def _do_test_GET_shards_no_cache(self, sharding_state, req_params,
                                     req_hdrs=None):
        # verify that a shard GET request does not lookup in cache or attempt
        # to cache shard ranges fetched from backend
        self.memcache.delete_all()
        self.memcache.clear_calls()
        req_params.update(dict(marker='egg', end_marker='jam'))
        hdrs = {'X-Backend-Record-Type': 'shard'}
        if req_hdrs:
            hdrs.update(req_hdrs)
        req = self._build_request(hdrs, req_params, {})
        resp_shards = self.sr_dicts[:2]
        backend_req, resp = self._capture_backend_request(
            req, 200, json.dumps(resp_shards).encode('ascii'),
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Sharding-State': sharding_state})
        self._check_backend_req(
            req, backend_req, extra_hdrs=hdrs, extra_params=req_params)
        expected_shards = self.sr_dicts[:2]
        self._check_response(resp, expected_shards, {
            'X-Backend-Recheck-Container-Existence': '60',
            'X-Backend-Record-Type': 'shard',
            'X-Backend-Sharding-State': sharding_state})
        # container metadata from backend response is set in memcache
        self.assertEqual(
            [('set', 'container/a/c', mock.ANY, 60)],
            self.memcache.calls)
        self.assertEqual(sharding_state,
                         self.memcache.calls[0][2]['sharding_state'])

    def test_GET_shard_ranges_no_cache_recheck_listing_shard_ranges(self):
        # verify that a GET for shards does not lookup or store in cache when
        # cache expiry time is set to  zero
        self._setup_shard_range_stubs()
        self.app.recheck_listing_shard_ranges = 0
        self._do_test_GET_shards_no_cache('unsharded', {'states': 'listing'})
        self._do_test_GET_shards_no_cache('sharding', {'states': 'listing'})
        self._do_test_GET_shards_no_cache('sharded', {'states': 'listing'})
        self._do_test_GET_shards_no_cache('collapsed', {'states': 'listing'})
        self._do_test_GET_shards_no_cache('unexpected', {'states': 'listing'})

    def test_GET_shard_ranges_no_cache_when_requesting_updating_shards(self):
        # verify that a GET for shards in updating states does not lookup or
        # store in cache
        self._setup_shard_range_stubs()
        self._do_test_GET_shards_no_cache('unsharded', {'states': 'updating'})
        self._do_test_GET_shards_no_cache('sharding', {'states': 'updating'})
        self._do_test_GET_shards_no_cache('sharded', {'states': 'updating'})
        self._do_test_GET_shards_no_cache('collapsed', {'states': 'updating'})
        self._do_test_GET_shards_no_cache('unexpected', {'states': 'updating'})

    def test_GET_shard_ranges_no_cache_when_include_deleted_shards(self):
        # verify that a GET for shards in listing states does not lookup or
        # store in cache if x-backend-include-deleted is true
        self._setup_shard_range_stubs()
        self._do_test_GET_shards_no_cache(
            'unsharded', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shards_no_cache(
            'sharding', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shards_no_cache(
            'sharded', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shards_no_cache(
            'collapsed', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})
        self._do_test_GET_shards_no_cache(
            'unexpected', {'states': 'listing'},
            {'X-Backend-Include-Deleted': 'true'})

    def test_GET_objects_makes_no_cache_lookup(self):
        # verify that an object GET request does not lookup container metadata
        # in cache
        self._setup_shard_range_stubs()
        self.memcache.delete_all()
        self.memcache.clear_calls()
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
            [('set', 'container/a/c', mock.ANY, 60)],
            self.memcache.calls)
        self.assertEqual('sharded',
                         self.memcache.calls[0][2]['sharding_state'])

    def test_GET_shard_ranges_no_memcache_available(self):
        self._setup_shard_range_stubs()
        self.memcache.clear_calls()
        hdrs = {'X-Backend-Record-Type': 'shard'}
        params = {'states': 'listing'}
        req = self._build_request(hdrs, params, {})
        req.environ['swift.cache'] = None
        backend_req, resp = self._capture_backend_request(
            req, 200, self._stub_shards_dump,
            {'X-Backend-Record-Type': 'shard',
             'X-Backend-Sharding-State': 'sharded'})
        self._check_backend_req(
            req, backend_req, extra_params=params, extra_hdrs=hdrs)
        expected_shards = self.sr_dicts
        self._check_response(resp, expected_shards, {
            'X-Backend-Recheck-Container-Existence': '60',
            'X-Backend-Record-Type': 'shard',
            'X-Backend-Sharding-State': 'sharded'})
        self.assertEqual([], self.memcache.calls)  # sanity check

    def test_cache_clearing(self):
        # verify that both metadata and shard ranges are purged form memcache
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
                resp = req.get_response(self.app)
            self.assertEqual(resp_status[0], resp.status_int)
            self.assertNotIn(cont_key, req.environ['swift.cache'].store)
            self.assertNotIn(shard_key, req.environ['swift.cache'].store)
        do_test('DELETE', 204, self.CONTAINER_REPLICAS)
        do_test('POST', 204, self.CONTAINER_REPLICAS)
        do_test('PUT', 202, self.CONTAINER_REPLICAS)

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
