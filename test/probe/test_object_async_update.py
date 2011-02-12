#!/usr/bin/python -u
# Copyright (c) 2010-2011 OpenStack, LLC.
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
from os import kill
from signal import SIGTERM
from subprocess import Popen
from time import sleep
from uuid import uuid4

from swift.common import client, direct_client

from test.probe.common import kill_pids, reset_environment


class TestObjectAsyncUpdate(unittest.TestCase):

    def setUp(self):
        self.pids, self.port2server, self.account_ring, self.container_ring, \
            self.object_ring, self.url, self.token, self.account = \
                reset_environment()

    def tearDown(self):
        kill_pids(self.pids)

    def test_main(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)
        apart, anodes = self.account_ring.get_nodes(self.account)
        anode = anodes[0]
        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        cnode = cnodes[0]
        kill(self.pids[self.port2server[cnode['port']]], SIGTERM)
        obj = 'object-%s' % uuid4()
        client.put_object(self.url, self.token, container, obj, '')
        self.pids[self.port2server[cnode['port']]] = \
            Popen(['swift-container-server',
                   '/etc/swift/container-server/%d.conf' %
                    ((cnode['port'] - 6001) / 10)]).pid
        sleep(2)
        self.assert_(not direct_client.direct_get_container(cnode, cpart,
                            self.account, container)[1])
        ps = []
        for n in xrange(1, 5):
            ps.append(Popen(['swift-object-updater',
                             '/etc/swift/object-server/%d.conf' % n, 'once']))
        for p in ps:
            p.wait()
        objs = [o['name'] for o in direct_client.direct_get_container(cnode,
                                    cpart, self.account, container)[1]]
        self.assert_(obj in objs)


if __name__ == '__main__':
    unittest.main()
