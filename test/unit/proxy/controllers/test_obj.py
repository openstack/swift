#!/usr/bin/env python
# Copyright (c) 2010-2012 OpenStack, LLC.
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

import swift
from swift.proxy import server as proxy_server
from test.unit import FakeRing, FakeMemcache, fake_http_connect


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
        self.app.request_node_count = lambda ring: 10000000
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

if __name__ == '__main__':
    unittest.main()
