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

import email.parser
import itertools
import random
import time
import unittest
from collections import defaultdict
from contextlib import contextmanager
import json
from hashlib import md5

import mock
from eventlet import Timeout

import swift
from swift.common import utils, swob
from swift.proxy import server as proxy_server
from swift.proxy.controllers import obj
from swift.proxy.controllers.base import get_info as _real_get_info
from swift.common.storage_policy import POLICIES, ECDriverError

from test.unit import FakeRing, FakeMemcache, fake_http_connect, \
    debug_logger, patch_policies, SlowBody
from test.unit.proxy.test_server import node_error_count


def unchunk_body(chunked_body):
    body = ''
    remaining = chunked_body
    while remaining:
        hex_length, remaining = remaining.split('\r\n', 1)
        length = int(hex_length, 16)
        body += remaining[:length]
        remaining = remaining[length + 2:]
    return body


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
    This patch is just a hook over the proxy server's __call__ to ensure
    that calls to get_info will return the stubbed value for
    container_info if it's a container info call.
    """

    container_info = {}
    per_container_info = {}

    def __call__(self, *args, **kwargs):

        def _fake_get_info(app, env, account, container=None, **kwargs):
            if container:
                if container in self.per_container_info:
                    return self.per_container_info[container]
                return self.container_info
            else:
                return _real_get_info(app, env, account, container, **kwargs)

        mock_path = 'swift.proxy.controllers.base.get_info'
        with mock.patch(mock_path, new=_fake_get_info):
            return super(
                PatchedObjControllerApp, self).__call__(*args, **kwargs)


class BaseObjectControllerMixin(object):
    container_info = {
        'write_acl': None,
        'read_acl': None,
        'storage_policy': None,
        'sync_key': None,
        'versions': None,
    }

    # this needs to be set on the test case
    controller_cls = None

    def setUp(self):
        # setup fake rings with handoffs
        for policy in POLICIES:
            policy.object_ring.max_more_nodes = policy.object_ring.replicas

        self.logger = debug_logger('proxy-server')
        self.logger.thread_locals = ('txn1', '127.0.0.2')
        self.app = PatchedObjControllerApp(
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing(), logger=self.logger)
        # you can over-ride the container_info just by setting it on the app
        self.app.container_info = dict(self.container_info)
        # default policy and ring references
        self.policy = POLICIES.default
        self.obj_ring = self.policy.object_ring
        self._ts_iter = (utils.Timestamp(t) for t in
                         itertools.count(int(time.time())))

    def ts(self):
        return self._ts_iter.next()

    def replicas(self, policy=None):
        policy = policy or POLICIES.default
        return policy.object_ring.replicas

    def quorum(self, policy=None):
        policy = policy or POLICIES.default
        return policy.quorum

    def test_iter_nodes_local_first_noops_when_no_affinity(self):
        # this test needs a stable node order - most don't
        self.app.sort_nodes = lambda l: l
        controller = self.controller_cls(
            self.app, 'a', 'c', 'o')
        self.app.write_affinity_is_local_fn = None
        object_ring = self.app.get_object_ring(None)
        all_nodes = object_ring.get_part_nodes(1)
        all_nodes.extend(object_ring.get_more_nodes(1))

        local_first_nodes = list(controller.iter_nodes_local_first(
            object_ring, 1))

        self.maxDiff = None

        self.assertEqual(all_nodes, local_first_nodes)

    def test_iter_nodes_local_first_moves_locals_first(self):
        controller = self.controller_cls(
            self.app, 'a', 'c', 'o')
        self.app.write_affinity_is_local_fn = (
            lambda node: node['region'] == 1)
        # we'll write to one more than replica count local nodes
        self.app.write_affinity_node_count = lambda r: r + 1

        object_ring = self.app.get_object_ring(None)
        # make our fake ring have plenty of nodes, and not get limited
        # artificially by the proxy max request node count
        object_ring.max_more_nodes = 100000
        self.app.request_node_count = lambda r: 100000

        all_nodes = object_ring.get_part_nodes(1)
        all_nodes.extend(object_ring.get_more_nodes(1))

        # i guess fake_ring wants the get_more_nodes iter to more safely be
        # converted to a list with a smallish sort of limit which *can* be
        # lower than max_more_nodes
        fake_rings_real_max_more_nodes_value = object_ring.replicas ** 2
        self.assertEqual(len(all_nodes), fake_rings_real_max_more_nodes_value)

        # make sure we have enough local nodes (sanity)
        all_local_nodes = [n for n in all_nodes if
                           self.app.write_affinity_is_local_fn(n)]
        self.assertTrue(len(all_local_nodes) >= self.replicas() + 1)

        # finally, create the local_first_nodes iter and flatten it out
        local_first_nodes = list(controller.iter_nodes_local_first(
            object_ring, 1))

        # the local nodes move up in the ordering
        self.assertEqual([1] * (self.replicas() + 1), [
            node['region'] for node in local_first_nodes[
                :self.replicas() + 1]])
        # we don't skip any nodes
        self.assertEqual(len(all_nodes), len(local_first_nodes))
        self.assertEqual(sorted(all_nodes), sorted(local_first_nodes))

    def test_iter_nodes_local_first_best_effort(self):
        controller = self.controller_cls(
            self.app, 'a', 'c', 'o')
        self.app.write_affinity_is_local_fn = (
            lambda node: node['region'] == 1)

        object_ring = self.app.get_object_ring(None)
        all_nodes = object_ring.get_part_nodes(1)
        all_nodes.extend(object_ring.get_more_nodes(1))

        local_first_nodes = list(controller.iter_nodes_local_first(
            object_ring, 1))

        # we won't have quite enough local nodes...
        self.assertEqual(len(all_nodes), self.replicas() +
                         POLICIES.default.object_ring.max_more_nodes)
        all_local_nodes = [n for n in all_nodes if
                           self.app.write_affinity_is_local_fn(n)]
        self.assertEqual(len(all_local_nodes), self.replicas())
        # but the local nodes we do have are at the front of the local iter
        first_n_local_first_nodes = local_first_nodes[:len(all_local_nodes)]
        self.assertEqual(sorted(all_local_nodes),
                         sorted(first_n_local_first_nodes))
        # but we *still* don't *skip* any nodes
        self.assertEqual(len(all_nodes), len(local_first_nodes))
        self.assertEqual(sorted(all_nodes), sorted(local_first_nodes))

    def test_connect_put_node_timeout(self):
        controller = self.controller_cls(
            self.app, 'a', 'c', 'o')
        self.app.conn_timeout = 0.05
        with set_http_connect(slow_connect=True):
            nodes = [dict(ip='', port='', device='')]
            res = controller._connect_put_node(nodes, '', '', {}, ('', ''))
        self.assertTrue(res is None)

    def test_DELETE_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        codes = [204] * self.replicas()
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_missing_one(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        codes = [404] + [204] * (self.replicas() - 1)
        random.shuffle(codes)
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_not_found(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        codes = [404] * (self.replicas() - 1) + [204]
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 404)

    def test_DELETE_mostly_found(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        mostly_204s = [204] * self.quorum()
        codes = mostly_204s + [404] * (self.replicas() - len(mostly_204s))
        self.assertEqual(len(codes), self.replicas())
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_DELETE_mostly_not_found(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        mostly_404s = [404] * self.quorum()
        codes = mostly_404s + [204] * (self.replicas() - len(mostly_404s))
        self.assertEqual(len(codes), self.replicas())
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 404)

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

    def test_DELETE_handoff(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='DELETE')
        codes = [204] * self.replicas()
        with set_http_connect(507, *codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 204)

    def test_POST_non_int_delete_after(self):
        t = str(int(time.time() + 100)) + '.1'
        req = swob.Request.blank('/v1/a/c/o', method='POST',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': t})
        resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('Non-integer X-Delete-After', resp.body)

    def test_PUT_non_int_delete_after(self):
        t = str(int(time.time() + 100)) + '.1'
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': t})
        with set_http_connect():
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

    def test_PUT_negative_delete_after(self):
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-After': '-60'})
        with set_http_connect():
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

    def test_PUT_delete_at_non_integer(self):
        t = str(int(time.time() - 100)) + '.1'
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        with set_http_connect():
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

    def test_PUT_delete_at_in_past(self):
        t = str(int(time.time() - 100))
        req = swob.Request.blank('/v1/a/c/o', method='PUT', body='',
                                 headers={'Content-Type': 'foo/bar',
                                          'X-Delete-At': t})
        with set_http_connect():
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 400)
        self.assertEqual('X-Delete-At in past', resp.body)

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

    def test_PUT_requires_length(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT')
        resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 411)

# end of BaseObjectControllerMixin


@patch_policies()
class TestReplicatedObjController(BaseObjectControllerMixin,
                                  unittest.TestCase):

    controller_cls = obj.ReplicatedObjectController

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

    def test_container_sync_put_x_timestamp_not_found(self):
        test_indexes = [None] + [int(p) for p in POLICIES]
        for policy_index in test_indexes:
            self.app.container_info['storage_policy'] = policy_index
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
            self.app.container_info['storage_policy'] = policy_index
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
            self.app.container_info['storage_policy'] = policy_index
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


@patch_policies(legacy_only=True)
class TestObjControllerLegacyCache(TestReplicatedObjController):
    """
    This test pretends like memcache returned a stored value that should
    resemble whatever "old" format.  It catches KeyErrors you'd get if your
    code was expecting some new format during a rolling upgrade.
    """

    # in this case policy_index is missing
    container_info = {
        'read_acl': None,
        'write_acl': None,
        'sync_key': None,
        'versions': None,
    }

    def test_invalid_storage_policy_cache(self):
        self.app.container_info['storage_policy'] = 1
        for method in ('GET', 'HEAD', 'POST', 'PUT', 'COPY'):
            req = swob.Request.blank('/v1/a/c/o', method=method)
            with set_http_connect():
                resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 503)


@patch_policies(with_ec_default=True)
class TestECObjController(BaseObjectControllerMixin, unittest.TestCase):
    container_info = {
        'read_acl': None,
        'write_acl': None,
        'sync_key': None,
        'versions': None,
        'storage_policy': '0',
    }

    controller_cls = obj.ECObjectController

    def test_determine_chunk_destinations(self):
        class FakePutter(object):
            def __init__(self, index):
                self.node_index = index

        controller = self.controller_cls(
            self.app, 'a', 'c', 'o')

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
        putters[3].node_index = None
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)
        putters[3].node_index = 3

        # now lets make a handoff at the start
        putters[0].node_index = None
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)
        putters[0].node_index = 0

        # now lets make a handoff in the middle
        putters[2].node_index = None
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)
        putters[2].node_index = 0

        # now lets make all of them handoffs
        for index in range(0, 4):
            putters[index].node_index = None
        got = controller._determine_chunk_destinations(putters)
        self.assertEquals(got, expected)

    def test_GET_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        get_resp = [200] * self.policy.ec_ndata
        with set_http_connect(*get_resp):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_GET_simple_x_newest(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o',
                                              headers={'X-Newest': 'true'})
        codes = [200] * self.replicas()
        codes += [404] * self.obj_ring.max_more_nodes
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_GET_error(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        get_resp = [503] + [200] * self.policy.ec_ndata
        with set_http_connect(*get_resp):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)

    def test_GET_with_body(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        # turn a real body into fragments
        segment_size = self.policy.ec_segment_size
        real_body = ('asdf' * segment_size)[:-10]
        # split it up into chunks
        chunks = [real_body[x:x + segment_size]
                  for x in range(0, len(real_body), segment_size)]
        fragment_payloads = []
        for chunk in chunks:
            fragments = self.policy.pyeclib_driver.encode(chunk)
            if not fragments:
                break
            fragment_payloads.append(fragments)
        # sanity
        sanity_body = ''
        for fragment_payload in fragment_payloads:
            sanity_body += self.policy.pyeclib_driver.decode(
                fragment_payload)
        self.assertEqual(len(real_body), len(sanity_body))
        self.assertEqual(real_body, sanity_body)

        node_fragments = zip(*fragment_payloads)
        self.assertEqual(len(node_fragments), self.replicas())  # sanity
        responses = [(200, ''.join(node_fragments[i]), {})
                     for i in range(POLICIES.default.ec_ndata)]
        status_codes, body_iter, headers = zip(*responses)
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 200)
        self.assertEqual(len(real_body), len(resp.body))
        self.assertEqual(real_body, resp.body)

    def test_PUT_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [201] * self.replicas()
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_with_explicit_commit_status(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [(100, 100, 201)] * self.replicas()
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_error(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [503] * self.replicas()
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 503)

    def test_PUT_mostly_success(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [201] * self.quorum()
        codes += [503] * (self.replicas() - len(codes))
        random.shuffle(codes)
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_error_commit(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [(100, 503, Exception('not used'))] * self.replicas()
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 503)

    def test_PUT_mostly_success_commit(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [201] * self.quorum()
        codes += [(100, 503, Exception('not used'))] * (
            self.replicas() - len(codes))
        random.shuffle(codes)
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_mostly_error_commit(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [(100, 503, Exception('not used'))] * self.quorum()
        codes += [201] * (self.replicas() - len(codes))
        random.shuffle(codes)
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 503)

    def test_PUT_commit_timeout(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [201] * (self.replicas() - 1)
        codes.append((100, Timeout(), Exception('not used')))
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_commit_exception(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        codes = [201] * (self.replicas() - 1)
        codes.append((100, Exception('kaboom!'), Exception('not used')))
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_with_body(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT')
        segment_size = self.policy.ec_segment_size
        test_body = ('asdf' * segment_size)[:-10]
        etag = md5(test_body).hexdigest()
        size = len(test_body)
        req.body = test_body
        codes = [201] * self.replicas()
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }

        put_requests = defaultdict(lambda: {'boundary': None, 'chunks': []})

        def capture_body(conn_id, chunk):
            put_requests[conn_id]['chunks'].append(chunk)

        def capture_headers(ip, port, device, part, method, path, headers,
                            **kwargs):
            conn_id = kwargs['connection_id']
            put_requests[conn_id]['boundary'] = headers[
                'X-Backend-Obj-Multipart-Mime-Boundary']

        with set_http_connect(*codes, expect_headers=expect_headers,
                              give_send=capture_body,
                              give_connect=capture_headers):
            resp = req.get_response(self.app)

        self.assertEquals(resp.status_int, 201)
        frag_archives = []
        for connection_id, info in put_requests.items():
            body = unchunk_body(''.join(info['chunks']))
            self.assertTrue(info['boundary'] is not None,
                            "didn't get boundary for conn %r" % (
                                connection_id,))

            # email.parser.FeedParser doesn't know how to take a multipart
            # message and boundary together and parse it; it only knows how
            # to take a string, parse the headers, and figure out the
            # boundary on its own.
            parser = email.parser.FeedParser()
            parser.feed(
                "Content-Type: multipart/nobodycares; boundary=%s\r\n\r\n" %
                info['boundary'])
            parser.feed(body)
            message = parser.close()

            self.assertTrue(message.is_multipart())  # sanity check
            mime_parts = message.get_payload()
            self.assertEqual(len(mime_parts), 3)
            obj_part, footer_part, commit_part = mime_parts

            # attach the body to frag_archives list
            self.assertEqual(obj_part['X-Document'], 'object body')
            frag_archives.append(obj_part.get_payload())

            # validate some footer metadata
            self.assertEqual(footer_part['X-Document'], 'object metadata')
            footer_metadata = json.loads(footer_part.get_payload())
            self.assertTrue(footer_metadata)
            expected = {
                'X-Object-Sysmeta-EC-Content-Length': str(size),
                'X-Backend-Container-Update-Override-Size': str(size),
                'X-Object-Sysmeta-EC-Etag': etag,
                'X-Backend-Container-Update-Override-Etag': etag,
                'X-Object-Sysmeta-EC-Segment-Size': str(segment_size),
            }
            for header, value in expected.items():
                self.assertEqual(footer_metadata[header], value)

            # sanity on commit message
            self.assertEqual(commit_part['X-Document'], 'put commit')

        self.assertEqual(len(frag_archives), self.replicas())
        fragment_size = self.policy.fragment_size
        node_payloads = []
        for fa in frag_archives:
            payload = [fa[x:x + fragment_size]
                       for x in range(0, len(fa), fragment_size)]
            node_payloads.append(payload)
        fragment_payloads = zip(*node_payloads)

        expected_body = ''
        for fragment_payload in fragment_payloads:
            self.assertEqual(len(fragment_payload), self.replicas())
            if True:
                fragment_payload = list(fragment_payload)
            expected_body += self.policy.pyeclib_driver.decode(
                fragment_payload)

        self.assertEqual(len(test_body), len(expected_body))
        self.assertEqual(test_body, expected_body)

    def test_PUT_old_obj_server(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o', method='PUT',
                                              body='')
        responses = [
            # one server will response 100-continue but not include the
            # needful expect headers and the connection will be dropped
            ((100, Exception('not used')), {}),
        ] + [
            # and pleanty of successful responses too
            (201, {
                'X-Obj-Metadata-Footer': 'yes',
                'X-Obj-Multiphase-Commit': 'yes',
            }),
        ] * self.replicas()
        random.shuffle(responses)
        if responses[-1][0] != 201:
            # whoops, stupid random
            responses = responses[1:] + [responses[0]]
        codes, expect_headers = zip(*responses)
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEquals(resp.status_int, 201)

    def test_COPY_cross_policy_type_from_replicated(self):
        self.app.per_container_info = {
            'c1': self.app.container_info.copy(),
            'c2': self.app.container_info.copy(),
        }
        # make c2 use replicated storage policy 1
        self.app.per_container_info['c2']['storage_policy'] = '1'

        # a put request with copy from source c2
        req = swift.common.swob.Request.blank('/v1/a/c1/o', method='PUT',
                                              body='', headers={
                                                  'X-Copy-From': 'c2/o'})

        # c2 get
        codes = [200] * self.replicas(POLICIES[1])
        codes += [404] * POLICIES[1].object_ring.max_more_nodes
        # c1 put
        codes += [201] * self.replicas()
        expect_headers = {
            'X-Obj-Metadata-Footer': 'yes',
            'X-Obj-Multiphase-Commit': 'yes'
        }
        with set_http_connect(*codes, expect_headers=expect_headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 201)

    def test_COPY_cross_policy_type_to_replicated(self):
        self.app.per_container_info = {
            'c1': self.app.container_info.copy(),
            'c2': self.app.container_info.copy(),
        }
        # make c1 use replicated storage policy 1
        self.app.per_container_info['c1']['storage_policy'] = '1'

        # a put request with copy from source c2
        req = swift.common.swob.Request.blank('/v1/a/c1/o', method='PUT',
                                              body='', headers={
                                                  'X-Copy-From': 'c2/o'})

        # c2 get
        codes = [200] * self.replicas()
        codes += [404] * self.obj_ring.max_more_nodes
        headers = {
            'X-Object-Sysmeta-Ec-Content-Length': 0,
        }
        # c1 put
        codes += [201] * self.replicas(POLICIES[1])
        with set_http_connect(*codes, headers=headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 201)

    def test_COPY_cross_policy_type_unknown(self):
        self.app.per_container_info = {
            'c1': self.app.container_info.copy(),
            'c2': self.app.container_info.copy(),
        }
        # make c1 use some made up storage policy index
        self.app.per_container_info['c1']['storage_policy'] = '13'

        # a COPY request of c2 with destination in c1
        req = swift.common.swob.Request.blank('/v1/a/c2/o', method='COPY',
                                              body='', headers={
                                                  'Destination': 'c1/o'})
        with set_http_connect():
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 503)

    def _make_ec_archive_bodies(self, test_body, policy=None):
        policy = policy or self.policy
        segment_size = policy.ec_segment_size
        # split up the body into buffers
        chunks = [test_body[x:x + segment_size]
                  for x in range(0, len(test_body), segment_size)]
        # encode the buffers into fragment payloads
        fragment_payloads = []
        for chunk in chunks:
            fragments = self.policy.pyeclib_driver.encode(chunk)
            if not fragments:
                break
            fragment_payloads.append(fragments)

        # join up the fragment payloads per node
        ec_archive_bodies = [''.join(fragments)
                             for fragments in zip(*fragment_payloads)]
        return ec_archive_bodies

    def test_GET_mismatched_fragment_archives(self):
        segment_size = self.policy.ec_segment_size
        test_data1 = ('test' * segment_size)[:-333]
        # N.B. the object data *length* here is different
        test_data2 = ('blah1' * segment_size)[:-333]

        etag1 = md5(test_data1).hexdigest()
        etag2 = md5(test_data2).hexdigest()

        ec_archive_bodies1 = self._make_ec_archive_bodies(test_data1)
        ec_archive_bodies2 = self._make_ec_archive_bodies(test_data2)

        headers1 = {'X-Object-Sysmeta-Ec-Etag': etag1}
        # here we're going to *lie* and say the etag here matches
        headers2 = {'X-Object-Sysmeta-Ec-Etag': etag1}

        responses1 = [(200, body, headers1)
                      for body in ec_archive_bodies1]
        responses2 = [(200, body, headers2)
                      for body in ec_archive_bodies2]

        req = swob.Request.blank('/v1/a/c/o')

        # sanity check responses1
        responses = responses1[:self.policy.ec_ndata]
        status_codes, body_iter, headers = zip(*responses)
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(md5(resp.body).hexdigest(), etag1)

        # sanity check responses2
        responses = responses2[:self.policy.ec_ndata]
        status_codes, body_iter, headers = zip(*responses)
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        self.assertEqual(md5(resp.body).hexdigest(), etag2)

        # now mix the responses a bit
        mix_index = random.randint(0, self.policy.ec_ndata - 1)
        mixed_responses = responses1[:self.policy.ec_ndata]
        mixed_responses[mix_index] = responses2[mix_index]

        status_codes, body_iter, headers = zip(*mixed_responses)
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 200)
        try:
            resp.body
        except ECDriverError:
            pass
        else:
            self.fail('invalid ec fragment response body did not blow up!')
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines))
        msg = error_lines[0]
        self.assertTrue('Error decoding fragments' in msg)
        self.assertTrue('/a/c/o' in msg)
        log_msg_args, log_msg_kwargs = self.logger.log_dict['error'][0]
        self.assertEqual(log_msg_kwargs['exc_info'][0], ECDriverError)

    def test_GET_read_timeout(self):
        segment_size = self.policy.ec_segment_size
        test_data = ('test' * segment_size)[:-333]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = self._make_ec_archive_bodies(test_data)
        headers = {'X-Object-Sysmeta-Ec-Etag': etag}
        self.app.recoverable_node_timeout = 0.01
        responses = [(200, SlowBody(body, 0.1), headers)
                     for body in ec_archive_bodies]

        req = swob.Request.blank('/v1/a/c/o')

        status_codes, body_iter, headers = zip(*responses + [
            (404, '', {}) for i in range(
                self.policy.object_ring.max_more_nodes)])
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers):
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 200)
            # do this inside the fake http context manager, it'll try to
            # resume but won't be able to give us all the right bytes
            self.assertNotEqual(md5(resp.body).hexdigest(), etag)
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(self.replicas(), len(error_lines))
        nparity = self.policy.ec_nparity
        for line in error_lines[:nparity]:
            self.assertTrue('retrying' in line)
        for line in error_lines[nparity:]:
            self.assertTrue('ChunkReadTimeout (0.01s)' in line)

    def test_GET_read_timeout_resume(self):
        segment_size = self.policy.ec_segment_size
        test_data = ('test' * segment_size)[:-333]
        etag = md5(test_data).hexdigest()
        ec_archive_bodies = self._make_ec_archive_bodies(test_data)
        headers = {'X-Object-Sysmeta-Ec-Etag': etag}
        self.app.recoverable_node_timeout = 0.05
        # first one is slow
        responses = [(200, SlowBody(ec_archive_bodies[0], 0.1), headers)]
        # ... the rest are fine
        responses += [(200, body, headers)
                      for body in ec_archive_bodies[1:]]

        req = swob.Request.blank('/v1/a/c/o')

        status_codes, body_iter, headers = zip(
            *responses[:self.policy.ec_ndata + 1])
        with set_http_connect(*status_codes, body_iter=body_iter,
                              headers=headers):
            resp = req.get_response(self.app)
            self.assertEqual(resp.status_int, 200)
            self.assertTrue(md5(resp.body).hexdigest(), etag)
        error_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(error_lines))
        self.assertTrue('retrying' in error_lines[0])


if __name__ == '__main__':
    unittest.main()
