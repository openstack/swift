#!/usr/bin/python -u
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

from subprocess import Popen
from unittest import main, TestCase
from uuid import uuid4

from swiftclient import client

from swift.common import direct_client
from test.probe.common import kill_nonprimary_server, kill_server, \
    kill_servers, reset_environment, start_server


class TestObjectAsyncUpdate(TestCase):

    def setUp(self):
        (self.pids, self.port2server, self.account_ring, self.container_ring,
         self.object_ring, self.url, self.token,
         self.account) = reset_environment()

    def tearDown(self):
        kill_servers(self.port2server, self.pids)

    def test_main(self):
        # Create container
        # Kill container servers excepting two of the primaries
        # Create container/obj
        # Restart other primary server
        # Assert it does not know about container/obj
        # Run the object-updaters
        # Assert the other primary server now knows about container/obj
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)
        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        cnode = cnodes[0]
        kill_nonprimary_server(cnodes, self.port2server, self.pids)
        kill_server(cnode['port'], self.port2server, self.pids)
        obj = 'object-%s' % uuid4()
        client.put_object(self.url, self.token, container, obj, '')
        start_server(cnode['port'], self.port2server, self.pids)
        self.assert_(not direct_client.direct_get_container(
            cnode, cpart, self.account, container)[1])
        processes = []
        for node in xrange(1, 5):
            processes.append(Popen(['swift-object-updater',
                                    '/etc/swift/object-server/%d.conf' % node,
                                    'once']))
        for process in processes:
            process.wait()
        objs = [o['name'] for o in direct_client.direct_get_container(
            cnode, cpart, self.account, container)[1]]
        self.assert_(obj in objs)


if __name__ == '__main__':
    main()
