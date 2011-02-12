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

from swift.common import client

from test.probe.common import get_to_final_state, kill_pids, reset_environment


class TestRunningWithEachTypeDown(unittest.TestCase):

    def setUp(self):
        self.pids, self.port2server, self.account_ring, self.container_ring, \
            self.object_ring, self.url, self.token, self.account = \
                reset_environment()

    def tearDown(self):
        kill_pids(self.pids)

    def test_main(self):
        # TODO: This test "randomly" pass or doesn't pass; need to find out why
        return
        apart, anodes = self.account_ring.get_nodes(self.account)
        kill(self.pids[self.port2server[anodes[0]['port']]], SIGTERM)
        cpart, cnodes = \
            self.container_ring.get_nodes(self.account, 'container1')
        kill(self.pids[self.port2server[cnodes[0]['port']]], SIGTERM)
        opart, onodes = \
            self.object_ring.get_nodes(self.account, 'container1', 'object1')
        kill(self.pids[self.port2server[onodes[0]['port']]], SIGTERM)

        try:
            client.put_container(self.url, self.token, 'container1')
        except client.ClientException, err:
            # This might 503 if one of the up container nodes tries to update
            # the down account node. It'll still be saved on one node, but we
            # can't assure the user.
            pass
        client.put_object(self.url, self.token, 'container1', 'object1', '1234')
        get_to_final_state()
        headers, containers = client.head_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '1')
        self.assertEquals(headers['x-account-object-count'], '1')
        self.assertEquals(headers['x-account-bytes-used'], '4')
        found1 = False
        for container in containers:
            if container['name'] == 'container1':
                found1 = True
                self.assertEquals(container['count'], 1)
                self.assertEquals(container['bytes'], 4)
        self.assert_(found1)
        found1 = False
        for obj in client.get_container(self.url, self.token, 'container1')[1]:
            if obj['name'] == 'object1':
                found1 = True
                self.assertEquals(obj['bytes'], 4)
        self.assert_(found1)

        self.pids[self.port2server[anodes[0]['port']]] = \
            Popen(['swift-account-server',
                   '/etc/swift/account-server/%d.conf' %
                    ((anodes[0]['port'] - 6002) / 10)]).pid
        self.pids[self.port2server[cnodes[0]['port']]] = \
            Popen(['swift-container-server',
                   '/etc/swift/container-server/%d.conf' %
                    ((cnodes[0]['port'] - 6001) / 10)]).pid
        self.pids[self.port2server[onodes[0]['port']]] = \
            Popen(['swift-object-server',
                   '/etc/swift/object-server/%d.conf' %
                    ((onodes[0]['port'] - 6000) / 10)]).pid
        sleep(2)
        headers, containers = client.head_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '1')
        self.assertEquals(headers['x-account-object-count'], '1')
        self.assertEquals(headers['x-account-bytes-used'], '4')
        found1 = False
        for container in containers:
            if container['name'] == 'container1':
                found1 = True
        # The account node was previously down.
        self.assert_(not found1)
        found1 = False
        for obj in client.get_container(self.url, self.token, 'container1')[1]:
            if obj['name'] == 'object1':
                found1 = True
                self.assertEquals(obj['bytes'], 4)
        # The first container node 404s, but the proxy will try the next node
        # and succeed.
        self.assert_(found1)

        get_to_final_state()
        headers, containers = client.head_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '1')
        self.assertEquals(headers['x-account-object-count'], '1')
        self.assertEquals(headers['x-account-bytes-used'], '4')
        found1 = False
        for container in containers:
            if container['name'] == 'container1':
                found1 = True
                self.assertEquals(container['count'], 1)
                self.assertEquals(container['bytes'], 4)
        self.assert_(found1)
        found1 = False
        for obj in client.get_container(self.url, self.token, 'container1')[1]:
            if obj['name'] == 'object1':
                found1 = True
                self.assertEquals(obj['bytes'], 4)
        self.assert_(found1)


if __name__ == '__main__':
    unittest.main()
