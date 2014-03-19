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

import unittest
from contextlib import contextmanager

import mock

import swift
from swift.proxy import server as proxy_server
from swift.common.swob import HTTPException
from test.unit import FakeRing, FakeMemcache, fake_http_connect, debug_logger


@contextmanager
def set_http_connect(*args, **kwargs):
    old_connect = swift.proxy.controllers.base.http_connect
    new_connect = fake_http_connect(*args, **kwargs)
    swift.proxy.controllers.base.http_connect = new_connect
    swift.proxy.controllers.obj.http_connect = new_connect
    swift.proxy.controllers.account.http_connect = new_connect
    swift.proxy.controllers.container.http_connect = new_connect
    yield new_connect
    swift.proxy.controllers.base.http_connect = old_connect
    swift.proxy.controllers.obj.http_connect = old_connect
    swift.proxy.controllers.account.http_connect = old_connect
    swift.proxy.controllers.container.http_connect = old_connect


class TestObjControllerWriteAffinity(unittest.TestCase):
    def setUp(self):
        self.app = proxy_server.Application(
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing(), object_ring=FakeRing(max_more_nodes=9))
        self.app.request_node_count = lambda replicas: 10000000
        self.app.sort_nodes = lambda l: l  # stop shuffling the primary nodes

    def test_iter_nodes_local_first_noops_when_no_affinity(self):
        controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
        self.app.write_affinity_is_local_fn = None

        all_nodes = self.app.object_ring.get_part_nodes(1)
        all_nodes.extend(self.app.object_ring.get_more_nodes(1))

        local_first_nodes = list(controller.iter_nodes_local_first(
            self.app.object_ring, 1))

        self.maxDiff = None

        self.assertEqual(all_nodes, local_first_nodes)

    def test_iter_nodes_local_first_moves_locals_first(self):
        controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
        self.app.write_affinity_is_local_fn = (
            lambda node: node['region'] == 1)
        self.app.write_affinity_node_count = lambda ring: 4

        all_nodes = self.app.object_ring.get_part_nodes(1)
        all_nodes.extend(self.app.object_ring.get_more_nodes(1))

        local_first_nodes = list(controller.iter_nodes_local_first(
            self.app.object_ring, 1))

        # the local nodes move up in the ordering
        self.assertEqual([1, 1, 1, 1],
                         [node['region'] for node in local_first_nodes[:4]])
        # we don't skip any nodes
        self.assertEqual(sorted(all_nodes), sorted(local_first_nodes))

    def test_connect_put_node_timeout(self):
        controller = proxy_server.ObjectController(self.app, 'a', 'c', 'o')
        self.app.conn_timeout = 0.1
        with set_http_connect(200, slow_connect=True):
            nodes = [dict(ip='', port='', device='')]
            res = controller._connect_put_node(nodes, '', '', {}, ('', ''))
        self.assertTrue(res is None)


class TestObjController(unittest.TestCase):
    def setUp(self):
        logger = debug_logger('proxy-server')
        logger.thread_locals = ('txn1', '127.0.0.2')
        self.app = proxy_server.Application(
            None, FakeMemcache(), account_ring=FakeRing(),
            container_ring=FakeRing(), object_ring=FakeRing(),
            logger=logger)
        self.controller = proxy_server.ObjectController(self.app,
                                                        'a', 'c', 'o')
        self.controller.container_info = mock.MagicMock(return_value={
            'partition': 1,
            'nodes': [
                {'ip': '127.0.0.1', 'port': '1', 'device': 'sda'},
                {'ip': '127.0.0.1', 'port': '2', 'device': 'sda'},
                {'ip': '127.0.0.1', 'port': '3', 'device': 'sda'},
            ],
            'write_acl': None,
            'read_acl': None,
            'sync_key': None,
            'versions': None})

    def test_PUT_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        req.headers['content-length'] = '0'
        with set_http_connect(201, 201, 201):
            resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_if_none_match(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        req.headers['if-none-match'] = '*'
        req.headers['content-length'] = '0'
        with set_http_connect(201, 201, 201):
            resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 201)

    def test_PUT_if_none_match_denied(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        req.headers['if-none-match'] = '*'
        req.headers['content-length'] = '0'
        with set_http_connect(201, (412, 412), 201):
            resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 412)

    def test_PUT_if_none_match_not_star(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        req.headers['if-none-match'] = 'somethingelse'
        req.headers['content-length'] = '0'
        with set_http_connect(201, 201, 201):
            resp = self.controller.PUT(req)
        self.assertEquals(resp.status_int, 400)

    def test_GET_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        with set_http_connect(200):
            resp = self.controller.GET(req)
        self.assertEquals(resp.status_int, 200)

    def test_DELETE_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        with set_http_connect(204, 204, 204):
            resp = self.controller.DELETE(req)
        self.assertEquals(resp.status_int, 204)

    def test_POST_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        with set_http_connect(200, 200, 200, 201, 201, 201):
            resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 202)

    def test_COPY_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        with set_http_connect(200, 200, 200, 201, 201, 201):
            resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 202)

    def test_HEAD_simple(self):
        req = swift.common.swob.Request.blank('/v1/a/c/o')
        with set_http_connect(200, 200, 200, 201, 201, 201):
            resp = self.controller.POST(req)
        self.assertEquals(resp.status_int, 202)

    def test_PUT_log_info(self):
        # mock out enough to get to the area of the code we want to test
        with mock.patch('swift.proxy.controllers.obj.check_object_creation',
                        mock.MagicMock(return_value=None)):
            req = swift.common.swob.Request.blank('/v1/a/c/o')
            req.headers['x-copy-from'] = 'somewhere'
            try:
                self.controller.PUT(req)
            except HTTPException:
                pass
            self.assertEquals(
                req.environ.get('swift.log_info'), ['x-copy-from:somewhere'])
            # and then check that we don't do that for originating POSTs
            req = swift.common.swob.Request.blank('/v1/a/c/o')
            req.method = 'POST'
            req.headers['x-copy-from'] = 'elsewhere'
            try:
                self.controller.PUT(req)
            except HTTPException:
                pass
            self.assertEquals(req.environ.get('swift.log_info'), None)


if __name__ == '__main__':
    unittest.main()
