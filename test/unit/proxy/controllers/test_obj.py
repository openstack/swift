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
import time
import unittest
from contextlib import contextmanager

import mock

import swift
from swift.common import utils, swob
from swift.proxy import server as proxy_server
from swift.common.storage_policy import StoragePolicy, POLICIES

from test.unit import FakeRing, FakeMemcache, fake_http_connect, \
    debug_logger, patch_policies


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

    object_controller = proxy_server.ObjectController

    def handle_request(self, req):
        with mock.patch('swift.proxy.server.ObjectController',
                        new=self.object_controller):
            return super(PatchedObjControllerApp, self).handle_request(req)


@patch_policies([StoragePolicy(0, 'zero', True,
                               object_ring=FakeRing(max_more_nodes=9))])
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
            res = controller._connect_put_node(nodes, '', '', {}, ('', ''))
        self.assertTrue(res is None)


@patch_policies([
    StoragePolicy(0, 'zero', True),
    StoragePolicy(1, 'one'),
    StoragePolicy(2, 'two'),
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

        self.app.object_controller = FakeContainerInfoObjController

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
            ts_iter = itertools.repeat(put_timestamp)
            head_resp = [404] * self.obj_ring.replicas + \
                [404] * self.obj_ring.max_more_nodes
            put_resp = [201] * self.obj_ring.replicas
            codes = head_resp + put_resp
            with set_http_connect(*codes, timestamps=ts_iter):
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
            head_resp = [200] * self.obj_ring.replicas + \
                [404] * self.obj_ring.max_more_nodes
            codes = head_resp
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
            head_resp = [200] * self.obj_ring.replicas + \
                [404] * self.obj_ring.max_more_nodes
            codes = head_resp
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
            head_resp = [200] * self.obj_ring.replicas + \
                [404] * self.obj_ring.max_more_nodes
            put_resp = [201] * self.obj_ring.replicas
            codes = head_resp + put_resp
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
        head_resp = [404] * self.obj_ring.replicas + \
            [404] * self.obj_ring.max_more_nodes
        put_resp = [409] + [201] * self.obj_ring.replicas
        codes = head_resp + put_resp
        with set_http_connect(*codes):
            resp = req.get_response(self.app)
        self.assertEqual(resp.status_int, 201)

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
    StoragePolicy(0, 'zero', True),
    StoragePolicy(1, 'one'),
    StoragePolicy(2, 'two'),
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
