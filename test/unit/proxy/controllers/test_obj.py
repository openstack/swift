#!/usr/bin/env python
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

import itertools
import random
import time
import unittest
from contextlib import contextmanager

import mock
from eventlet import Timeout

import swift
from swift.common import utils, swob
from swift.proxy import server as proxy_server
from swift.common.storage_policy import StoragePolicy, POLICIES, \
    REPL_POLICY

from test.unit import FakeRing, FakeMemcache, fake_http_connect, \
    debug_logger, patch_policies
from test.unit.proxy.test_server import node_error_count


@contextmanager
def set_http_connect(*args, **kwargs):
    old_connect = swift.proxy.controllers.base.http_connect
    new_connect = fake_http_connect(*args, **kwargs)
    try:
        swift.proxy.controllers.base.http_connect = new_connect
        swift.proxy.controllers.obj.http_connect = new_connect
        swift.proxy.controllers.account.http_connect = new_connect
        swift.proxy.controllers.container.http_connect = new_connect
        yield new_connect
        left_over_status = list(new_connect.code_iter)
        if left_over_status:
            raise AssertionError('left over status %r' % left_over_status)
    finally:
        swift.proxy.controllers.base.http_connect = old_connect
        swift.proxy.controllers.obj.http_connect = old_connect
        swift.proxy.controllers.account.http_connect = old_connect
        swift.proxy.controllers.container.http_connect = old_connect


class PatchedObjControllerApp(proxy_server.Application):
    """
    This patch is just a hook over handle_request to ensure that when
    get_controller is called the ObjectController class is patched to
    return a (possibly stubbed) ObjectController class.
    """

    object_controller = proxy_server.ObjectController

    def handle_request(self, req):
        with mock.patch('swift.proxy.server.ObjectController',
                        new=self.object_controller):
            return super(PatchedObjControllerApp, self).handle_request(req)


@patch_policies([
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 0, 'name': 'zero', 'is_default': True,
                      'object_ring': FakeRing(max_more_nodes=9)})
])
class TestObjControllerWriteAffinity(unittest.TestCase):
    def setUp(self):
        self.app = proxy_server.Application(
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing(), logger=debug_logger())
        self.app.request_node_count = lambda ring: 10000000
        self.app.sort_nodes = lambda l: l  # stop shuffling the primary nodes

    def test_iter_nodes_local_first_noops_when_no_affinity(self):
        controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
        self.app.write_affinity_is_local_fn = None
        object_ring = self.app.get_object_ring(None)
        all_nodes = object_ring.get_part_nodes(1)
        all_nodes.extend(object_ring.get_more_nodes(1))

        local_first_nodes = list(controller.iter_nodes_local_first(
            object_ring, 1))

        self.maxDiff = None

        self.assertEqual(all_nodes, local_first_nodes)

    def test_iter_nodes_local_first_moves_locals_first(self):
        controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
        self.app.write_affinity_is_local_fn = (
            lambda node: node['region'] == 1)
        self.app.write_affinity_node_count = lambda ring: 4

        object_ring = self.app.get_object_ring(None)
        all_nodes = object_ring.get_part_nodes(1)
        all_nodes.extend(object_ring.get_more_nodes(1))

        local_first_nodes = list(controller.iter_nodes_local_first(
            object_ring, 1))

        # the local nodes move up in the ordering
        self.assertEqual([1, 1, 1, 1],
                         [node['region'] for node in local_first_nodes[:4]])
        # we don't skip any nodes
        self.assertEqual(len(all_nodes), len(local_first_nodes))
        self.assertEqual(sorted(all_nodes), sorted(local_first_nodes))

    def test_connect_put_node_timeout(self):
        controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
        self.app.conn_timeout = 0.05
        with set_http_connect(slow_connect=True):
            nodes = [dict(ip='', port='', device='')]
            res = controller._connect_put_node(nodes, '', '', {}, ('', ''),
                                               False)
        self.assertTrue(res is None)


@patch_policies([
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 0, 'name': 'zero', 'is_default': True}),
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 1, 'name': 'one'}),
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 2, 'name': 'two'})
])
class TestObjController(unittest.TestCase):
    container_info = {
        'partition': 1,
        'nodes': [
            {'ip': '127.0.0.1', 'port': '1', 'device': 'sda'},
            {'ip': '127.0.0.1', 'port': '2', 'device': 'sda'},
            {'ip': '127.0.0.1', 'port': '3', 'device': 'sda'},
        ],
        'write_acl': None,
        'read_acl': None,
        'storage_policy': None,
        'sync_key': None,
        'versions': None,
    }

    def setUp(self):
        # setup fake rings with handoffs
        self.obj_ring = FakeRing(max_more_nodes=3)
        for policy in POLICIES:
            policy.object_ring = self.obj_ring

        logger = debug_logger('proxy-server')
        logger.thread_locals = ('txn1', '127.0.0.2')
        self.app = PatchedObjControllerApp(
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing(), logger=logger)

        class FakeContainerInfoObjController(proxy_server.ObjectController):

            def container_info(controller, *args, **kwargs):
                patch_path = 'swift.proxy.controllers.base.get_info'
                with mock.patch(patch_path) as mock_get_info:
                    mock_get_info.return_value = dict(self.container_info)
                    return super(FakeContainerInfoObjController,
                                 controller).container_info(*args, **kwargs)

        # this is taking advantage of the fact that self.app is a
        # PachedObjControllerApp, so handle_response will route into an
        # instance of our FakeContainerInfoObjController just by
        # overriding the class attribute for object_controller
        self.app.object_controller = FakeContainerInfoObjController

    def test_determine_chunk_destinations(self):
        class FakePutter(object):
            def __init__(self, index):
                self.node_index = index

        controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')

        # create a dummy list of putters, check no handoffs
        putters = []
        for index in range(0, 4):
            putters.append(FakePutter(index))
        got = controller._determine_chunk_destinations(putters)
        expected = {}
        for i, p in enumerate(putters):
            expected[p] = i
        self.assertEquals(got, expected)

        # now lets make a handoff at the end
        putters[3].node_index = 5
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)
        putters[3].node_index = 3

        # now lets make a handoff at the start
        putters[0].node_index = 5
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)
        putters[0].node_index = 0

        # now lets make a handoff in the middle
        putters[2].node_index = 5
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)
        putters[2].node_index = 0

        # now lets make all of them handoffs
        for index in range(0, 4):
            putters[index].node_index = (index + 1) * 10
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)

    def test_PUT_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT')
        req.headers['content-length'] = '0'
        with set_http_connect(201, 201, 201):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_if_none_match(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT')
        req.headers['if-none-match'] = '*'
        req.headers['content-length'] = '0'
        with set_http_connect(201, 201, 201):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_if_none_match_denied(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT')
        req.headers['if-none-match'] = '*'
        req.headers['content-length'] = '0'
        with set_http_connect(201, 412, 201):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 412)

    def test_PUT_if_none_match_not_star(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT')
        req.headers['if-none-match'] = 'somethingelse'
        req.headers['content-length'] = '0'
        with set_http_connect():
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 400)

    def test_PUT_connect_exceptions(self):
        object_ring = self.app.get_object_ring(None)
        self.app.sort_nodes = lambda n: n  # disable shuffle

        def test_status_map(statuses, expected):
            self.app._error_limiting = {}
            req = swob.Request.blank('/v1/a/c/o.jpg', method='PUT',
                                     body='test body')
            with set_http_connect(*statuses):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, expected)

        base_status = [201] * 3
        # test happy path
        test_status_map(list(base_status), 201)
        for i in range(3):
            self.assertEqual(node_error_count(
                self.app, object_ring.devs[i]), 0)
        # single node errors and test isolation
        for i in range(3):
            status_list = list(base_status)
            status_list[i] = 503
            test_status_map(status_list, 201)
            for j in range(3):
                self.assertEqual(node_error_count(
                    self.app, object_ring.devs[j]), 1 if j == i else 0)
        # connect errors
        test_status_map((201, Timeout(), 201, 201), 201)
        self.assertEqual(node_error_count(
            self.app, object_ring.devs[1]), 1)
        test_status_map((Exception('kaboom!'), 201, 201, 201), 201)
        self.assertEqual(node_error_count(
            self.app, object_ring.devs[0]), 1)
        # expect errors
        test_status_map((201, 201, (503, None), 201), 201)
        self.assertEqual(node_error_count(
            self.app, object_ring.devs[2]), 1)
        test_status_map(((507, None), 201, 201, 201), 201)
        self.assertEqual(
            node_error_count(self.app, object_ring.devs[0]),
            self.app.error_suppression_limit + 1)
        # response errors
        test_status_map(((100, Timeout()), 201, 201), 201)
        self.assertEqual(
            node_error_count(self.app, object_ring.devs[0]), 1)
        test_status_map((201, 201, (100, Exception())), 201)
        self.assertEqual(
            node_error_count(self.app, object_ring.devs[2]), 1)
        test_status_map((201, (100, 507), 201), 201)
        self.assertEqual(
            node_error_count(self.app, object_ring.devs[1]),
            self.app.error_suppression_limit + 1)

    def test_GET_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        with set_http_connect(200):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_GET_error(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        with set_http_connect(503, 200):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_GET_handoff(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        codes = [503] * self.obj_ring.replicas + [200]
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_GET_not_found(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        codes = [404] * (self.obj_ring.replicas +
                         self.obj_ring.max_more_nodes)
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        with set_http_connect(204, 204, 204):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_missing_one(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        with set_http_connect(404, 204, 204):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_half_not_found_statuses(self):
        self.obj_ring.set_replicas(4)

        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        with set_http_connect(404, 204, 404, 204):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_half_not_found_headers_and_body(self):
        # Transformed responses have bogus bodies and headers, so make sure we
        # send the client headers and body from a real node's response.
        self.obj_ring.set_replicas(4)

        status_codes = (404, 404, 204, 204)
        bodies = ('not found', 'not found', '', '')
        headers = [{}, {}, {'Pick-Me': 'yes'}, {'Pick-Me': 'yes'}]

        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        with set_http_connect(*status_codes, body_iter=bodies,
                              headers=headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)
        self.assertEquals(resp.headers.get('Pick-Me'), 'yes')
        self.assertEquals(resp.body, '')

    def test_DELETE_not_found(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        with set_http_connect(404, 404, 204):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_handoff(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        codes = [204] * self.obj_ring.replicas
        with set_http_connect(507, *codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_POST_as_COPY_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='POST')
        head_resp = [200] * self.obj_ring.replicas + \
            [404] * self.obj_ring.max_more_nodes
        put_resp = [201] * self.obj_ring.replicas
        codes = head_resp + put_resp
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 202)

    def test_POST_delete_at(self):
        t = str(int(time.time() + 100))
        req = swob.Request.blank('/v1/a/c/o', method='POST',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        post_headers = []

        def capture_headers(ip, port, device, part, method, path, headers,
                            **kwargs):
            if method == 'POST':
                post_headers.append(headers)
        x_newest_responses = [200] * self.obj_ring.replicas + \
            [404] * self.obj_ring.max_more_nodes
        post_resp = [200] * self.obj_ring.replicas
        codes = x_newest_responses + post_resp
        with set_http_connect(*codes, give_connect=capture_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)
        for given_headers in post_headers:
            self.assertEquals(given_headers.get('X-Delete-At'), t)
            self.assertTrue('X-Delete-At-Host' in given_headers)
            self.assertTrue('X-Delete-At-Device' in given_headers)
            self.assertTrue('X-Delete-At-Partition' in given_headers)
            self.assertTrue('X-Delete-At-Container' in given_headers)

    def test_POST_non_int_delete_after(self):
        t = str(int(time.time() + 100)) + '.1'
        req = swob.Request.blank('/v1/a/c/o', method='POST',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': t})
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('Non-integer X-Delete-After', resp.body)

    def test_POST_negative_delete_after(self):
        req = swob.Request.blank('/v1/a/c/o', method='POST',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': '-60'})
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('X-Delete-After in past', resp.body)

    def test_POST_delete_at_non_integer(self):
        t = str(int(time.time() + 100)) + '.1'
        req = swob.Request.blank('/v1/a/c/o', method='POST',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('Non-integer X-Delete-At', resp.body)

    def test_POST_delete_at_in_past(self):
        t = str(int(time.time() - 100))
        req = swob.Request.blank('/v1/a/c/o', method='POST',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('X-Delete-At in past', resp.body)

    def test_PUT_converts_delete_after_to_delete_at(self):
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': '60'})
        put_headers = []

        def capture_headers(ip, port, device, part, method, path, headers,
                            **kwargs):
            if method == 'PUT':
                put_headers.append(headers)
        codes = [201] * self.obj_ring.replicas
        t = time.time()
        with set_http_connect(*codes, give_connect=capture_headers):
            with mock.patch('time.time', lambda: t):
                resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)
        expected_delete_at = str(int(t) + 60)
        for given_headers in put_headers:
            self.assertEquals(given_headers.get('X-Delete-At'),
                              expected_delete_at)
            self.assertTrue('X-Delete-At-Host' in given_headers)
            self.assertTrue('X-Delete-At-Device' in given_headers)
            self.assertTrue('X-Delete-At-Partition' in given_headers)
            self.assertTrue('X-Delete-At-Container' in given_headers)

    def test_PUT_non_int_delete_after(self):
        t = str(int(time.time() + 100)) + '.1'
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': t})
        with set_http_connect():
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('Non-integer X-Delete-After', resp.body)

    def test_PUT_negative_delete_after(self):
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': '-60'})
        with set_http_connect():
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('X-Delete-After in past', resp.body)

    def test_PUT_delete_at(self):
        t = str(int(time.time() + 100))
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        put_headers = []

        def capture_headers(ip, port, device, part, method, path, headers,
                            **kwargs):
            if method == 'PUT':
                put_headers.append(headers)
        codes = [201] * self.obj_ring.replicas
        with set_http_connect(*codes, give_connect=capture_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)
        for given_headers in put_headers:
            self.assertEquals(given_headers.get('X-Delete-At'), t)
            self.assertTrue('X-Delete-At-Host' in given_headers)
            self.assertTrue('X-Delete-At-Device' in given_headers)
            self.assertTrue('X-Delete-At-Partition' in given_headers)
            self.assertTrue('X-Delete-At-Container' in given_headers)

    def test_PUT_delete_at_non_integer(self):
        t = str(int(time.time() - 100)) + '.1'
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        with set_http_connect():
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('Non-integer X-Delete-At', resp.body)

    def test_PUT_delete_at_in_past(self):
        t = str(int(time.time() - 100))
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        with set_http_connect():
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('X-Delete-At in past', resp.body)

    def test_container_sync_put_x_timestamp_not_found(self):
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            self.container_info['storage_policy'] = policy_index
            put_timestamp = utils.Timestamp(time.time()).normal
            req = swob.Request.blank(
                '/v1/a/c/o', method='PUT', headers={
                    'Content-Length': 0,
                    'X-Timestamp': put_timestamp})
            codes = [201] * self.obj_ring.replicas
            with set_http_connect(*codes):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 201)

    def test_container_sync_put_x_timestamp_match(self):
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            self.container_info['storage_policy'] = policy_index
            put_timestamp = utils.Timestamp(time.time()).normal
            req = swob.Request.blank(
                '/v1/a/c/o', method='PUT', headers={
                    'Content-Length': 0,
                    'X-Timestamp': put_timestamp})
            ts_iter = itertools.repeat(put_timestamp)
            codes = [409] * self.obj_ring.replicas
            with set_http_connect(*codes, timestamps=ts_iter):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 202)

    def test_container_sync_put_x_timestamp_older(self):
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            self.container_info['storage_policy'] = policy_index
            req = swob.Request.blank(
                '/v1/a/c/o', method='PUT', headers={
                    'Content-Length': 0,
                    'X-Timestamp': ts.next().internal})
            ts_iter = itertools.repeat(ts.next().internal)
            codes = [409] * self.obj_ring.replicas
            with set_http_connect(*codes, timestamps=ts_iter):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 202)

    def test_container_sync_put_x_timestamp_newer(self):
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            orig_timestamp = ts.next().internal
            req = swob.Request.blank(
                '/v1/a/c/o', method='PUT', headers={
                    'Content-Length': 0,
                    'X-Timestamp': ts.next().internal})
            ts_iter = itertools.repeat(orig_timestamp)
            codes = [201] * self.obj_ring.replicas
            with set_http_connect(*codes, timestamps=ts_iter):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 201)

    def test_container_sync_delete(self):
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            req = swob.Request.blank(
                '/v1/a/c/o', method='DELETE', headers={
                    'X-Timestamp': ts.next().internal})
            codes = [409] * self.obj_ring.replicas
            ts_iter = itertools.repeat(ts.next().internal)
            with set_http_connect(*codes, timestamps=ts_iter):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 409)

    def test_put_x_timestamp_conflict(self):
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        req = swob.Request.blank(
            '/v1/a/c/o', method='PUT', headers={
                'Content-Length': 0,
                'X-Timestamp': ts.next().internal})
        ts_iter = iter([ts.next().internal, None, None])
        codes = [409] + [201] * (self.obj_ring.replicas - 1)
        with set_http_connect(*codes, timestamps=ts_iter):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 202)

    def test_container_sync_put_x_timestamp_race(self):
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            put_timestamp = ts.next().internal
            req = swob.Request.blank(
                '/v1/a/c/o', method='PUT', headers={
                    'Content-Length': 0,
                    'X-Timestamp': put_timestamp})

            # object nodes they respond 409 because another in-flight request
            # finished and now the on disk timestamp is equal to the request.
            put_ts = [put_timestamp] * self.obj_ring.replicas
            codes = [409] * self.obj_ring.replicas

            ts_iter = iter(put_ts)
            with set_http_connect(*codes, timestamps=ts_iter):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 202)

    def test_container_sync_put_x_timestamp_unsynced_race(self):
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            put_timestamp = ts.next().internal
            req = swob.Request.blank(
                '/v1/a/c/o', method='PUT', headers={
                    'Content-Length': 0,
                    'X-Timestamp': put_timestamp})

            # only one in-flight request finished
            put_ts = [None] * (self.obj_ring.replicas - 1)
            put_resp = [201] * (self.obj_ring.replicas - 1)
            put_ts += [put_timestamp]
            put_resp += [409]

            ts_iter = iter(put_ts)
            codes = put_resp
            with set_http_connect(*codes, timestamps=ts_iter):
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 202)

    def test_COPY_simple(self):
        req = swift.common.swob.Request.blank(
            '/v1/a/c/o', method='COPY',
            headers={'Content-Length': 0,
                     'Destination': 'c/o-copy'})
        head_resp = [200] * self.obj_ring.replicas + \
            [404] * self.obj_ring.max_more_nodes
        put_resp = [201] * self.obj_ring.replicas
        codes = head_resp + put_resp
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_HEAD_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='HEAD')
        with set_http_connect(200):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_HEAD_x_newest(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='HEAD',
                                              headers={'X-Newest': 'true'})
        with set_http_connect(200, 200, 200):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_HEAD_x_newest_different_timestamps(self):
        req = swob.Request.blank('/v1/a/c/o', method='HEAD',
                                 headers={'X-Newest': 'true'})
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        timestamps = [next(ts) for i in range(3)]
        newest_timestamp = timestamps[-1]
        random.shuffle(timestamps)
        backend_response_headers = [{
            'X-Backend-Timestamp': t.internal,
            'X-Timestamp': t.normal
        } for t in timestamps]
        with set_http_connect(200, 200, 200,
                              headers=backend_response_headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['x-timestamp'], newest_timestamp.normal)

    def test_HEAD_x_newest_with_two_vector_timestamps(self):
        req = swob.Request.blank('/v1/a/c/o', method='HEAD',
                                 headers={'X-Newest': 'true'})
        ts = (utils.Timestamp(time.time(), offset=offset)
              for offset in itertools.count())
        timestamps = [next(ts) for i in range(3)]
        newest_timestamp = timestamps[-1]
        random.shuffle(timestamps)
        backend_response_headers = [{
            'X-Backend-Timestamp': t.internal,
            'X-Timestamp': t.normal
        } for t in timestamps]
        with set_http_connect(200, 200, 200,
                              headers=backend_response_headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(resp.headers['x-backend-timestamp'],
                         newest_timestamp.internal)

    def test_HEAD_x_newest_with_some_missing(self):
        req = swob.Request.blank('/v1/a/c/o', method='HEAD',
                                 headers={'X-Newest': 'true'})
        ts = (utils.Timestamp(t) for t in itertools.count(int(time.time())))
        request_count = self.app.request_node_count(self.obj_ring.replicas)
        backend_response_headers = [{
            'x-timestamp': next(ts).normal,
        } for i in range(request_count)]
        responses = [404] * (request_count - 1)
        responses.append(200)
        request_log = []

        def capture_requests(ip, port, device, part, method, path,
                             headers=None, **kwargs):
            req = {
                'ip': ip,
                'port': port,
                'device': device,
                'part': part,
                'method': method,
                'path': path,
                'headers': headers,
            }
            request_log.append(req)
        with set_http_connect(*responses,
                              headers=backend_response_headers,
                              give_connect=capture_requests):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        for req in request_log:
            self.assertEqual(req['method'], 'HEAD')
            self.assertEqual(req['path'], '/a/c/o')

    def test_PUT_log_info(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT')
        req.headers['x-copy-from'] = 'some/where'
        req.headers['Content-Length'] = 0
        # override FakeConn default resp headers to keep log_info clean
        resp_headers = {'x-delete-at': None}
        head_resp = [200] * self.obj_ring.replicas + \
            [404] * self.obj_ring.max_more_nodes
        put_resp = [201] * self.obj_ring.replicas
        codes = head_resp + put_resp
        with set_http_connect(*codes, headers=resp_headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 201)
        self.assertEquals(
            req.environ.get('swift.log_info'), ['x-copy-from:some/where'])
        # and then check that we don't do that for originating POSTs
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        req.method = 'POST'
        req.headers['x-copy-from'] = 'else/where'
        with set_http_connect(*codes, headers=resp_headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 202)
        self.assertEquals(req.environ.get('swift.log_info'), None)


@patch_policies([
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 0, 'name': 'zero', 'is_default': True}),
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 1, 'name': 'one'}),
    StoragePolicy.from_conf(
        REPL_POLICY, {'idx': 2, 'name': 'two'})
])
class TestObjControllerLegacyCache(TestObjController):
    """
    This test pretends like memcache returned a stored value that should
    resemble whatever "old" format.  It catches KeyErrors you'd get if your
    code was expecting some new format during a rolling upgrade.
    """

    container_info = {
        'read_acl': None,
        'write_acl': None,
        'sync_key': None,
        'versions': None,
    }


if __name__ == '__main__':
    unittest.main()
