#!/usr/bin/python -u
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

import random

from six.moves import http_client as httplib

from swift.common.storage_policy import POLICIES
from swift.common.ring import Ring
from swift.common.manager import Manager

from test.probe.common import resetswift


def putrequest(conn, method, path, headers):

    conn.putrequest(method, path, skip_host=(headers and 'Host' in headers))
    if headers:
        for header, value in headers.items():
            conn.putheader(header, str(value))
    conn.endheaders()


class TestWSGIServerProcessHandling(unittest.TestCase):

    def setUp(self):
        resetswift()

    def _check_reload(self, server_name, ip, port):
        manager = Manager([server_name])
        manager.start()

        starting_pids = set(pid for server in manager.servers
                            for (_, pid) in server.iter_pid_files())

        body = 'test' * 10
        conn = httplib.HTTPConnection('%s:%s' % (ip, port))

        # sanity request
        putrequest(conn, 'PUT', 'blah',
                   headers={'Content-Length': len(body)})
        conn.send(body)
        resp = conn.getresponse()
        self.assertEqual(resp.status // 100, 4)
        resp.read()

        manager.reload()

        post_reload_pids = set(pid for server in manager.servers
                               for (_, pid) in server.iter_pid_files())

        # none of the pids we started with are being tracked after reload
        msg = 'expected all pids from %r to have died, but found %r' % (
            starting_pids, post_reload_pids)
        self.assertFalse(starting_pids & post_reload_pids, msg)

        # ... and yet we can keep using the same connection!
        putrequest(conn, 'PUT', 'blah',
                   headers={'Content-Length': len(body)})
        conn.send(body)
        resp = conn.getresponse()
        self.assertEqual(resp.status // 100, 4)
        resp.read()

        # close our connection
        conn.close()

        # sanity
        post_close_pids = set(pid for server in manager.servers
                              for (_, pid) in server.iter_pid_files())
        self.assertEqual(post_reload_pids, post_close_pids)

    def test_proxy_reload(self):
        self._check_reload('proxy-server', 'localhost', 8080)

    def test_object_reload(self):
        policy = random.choice(list(POLICIES))
        policy.load_ring('/etc/swift')
        node = random.choice(policy.object_ring.get_part_nodes(1))
        self._check_reload('object', node['ip'], node['port'])

    def test_account_container_reload(self):
        for server in ('account', 'container'):
            ring = Ring('/etc/swift', ring_name=server)
            node = random.choice(ring.get_part_nodes(1))
            self._check_reload(server, node['ip'], node['port'])


if __name__ == '__main__':
    unittest.main()
