#!/usr/bin/python -u
# Copyright (c) 2010 OpenStack, LLC.
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
from common import get_to_final_state, kill_pids, reset_environment


class TestAccountFailures(unittest.TestCase):

    def setUp(self):
        self.pids, self.port2server, self.account_ring, self.container_ring, \
            self.object_ring, self.url, self.token, self.account = \
                reset_environment()

    def tearDown(self):
        kill_pids(self.pids)

    def test_main(self):
        container1 = 'container1'
        client.put_container(self.url, self.token, container1)
        container2 = 'container2'
        client.put_container(self.url, self.token, container2)
        self.assert_(client.head_account(self.url, self.token), (2, 0, 0))
        containers = client.get_account(self.url, self.token)
        found1 = False
        found2 = False
        for c in containers:
            if c['name'] == container1:
                found1 = True
                self.assertEquals(c['count'], 0)
                self.assertEquals(c['bytes'], 0)
            elif c['name'] == container2:
                found2 = True
                self.assertEquals(c['count'], 0)
                self.assertEquals(c['bytes'], 0)
        self.assert_(found1)
        self.assert_(found2)

        client.put_object(self.url, self.token, container2, 'object1', '1234')
        self.assert_(client.head_account(self.url, self.token), (2, 0, 0))
        containers = client.get_account(self.url, self.token)
        found1 = False
        found2 = False
        for c in containers:
            if c['name'] == container1:
                found1 = True
                self.assertEquals(c['count'], 0)
                self.assertEquals(c['bytes'], 0)
            elif c['name'] == container2:
                found2 = True
                self.assertEquals(c['count'], 0)
                self.assertEquals(c['bytes'], 0)
        self.assert_(found1)
        self.assert_(found2)

        get_to_final_state()
        containers = client.get_account(self.url, self.token)
        self.assert_(client.head_account(self.url, self.token), (2, 1, 4))
        found1 = False
        found2 = False
        for c in containers:
            if c['name'] == container1:
                found1 = True
                self.assertEquals(c['count'], 0)
                self.assertEquals(c['bytes'], 0)
            elif c['name'] == container2:
                found2 = True
                self.assertEquals(c['count'], 1)
                self.assertEquals(c['bytes'], 4)
        self.assert_(found1)
        self.assert_(found2)

        apart, anodes = self.account_ring.get_nodes(self.account)
        kill(self.pids[self.port2server[anodes[0]['port']]], SIGTERM)

        client.delete_container(self.url, self.token, container1)
        client.put_object(self.url, self.token, container2, 'object2', '12345')
        self.assert_(client.head_account(self.url, self.token), (2, 1, 4))
        containers = client.get_account(self.url, self.token)
        found1 = False
        found2 = False
        for c in containers:
            if c['name'] == container1:
                found1 = True
            elif c['name'] == container2:
                found2 = True
                self.assertEquals(c['count'], 1)
                self.assertEquals(c['bytes'], 4)
        self.assert_(not found1)
        self.assert_(found2)

        ps = []
        for n in xrange(1, 5):
            ps.append(Popen(['swift-container-updater',
                             '/etc/swift/container-server/%d.conf' % n,
                             'once']))
        for p in ps:
            p.wait()
        self.assert_(client.head_account(self.url, self.token), (2, 2, 9))
        containers = client.get_account(self.url, self.token)
        found1 = False
        found2 = False
        for c in containers:
            if c['name'] == container1:
                found1 = True
            elif c['name'] == container2:
                found2 = True
                self.assertEquals(c['count'], 2)
                self.assertEquals(c['bytes'], 9)
        self.assert_(not found1)
        self.assert_(found2)

        self.pids[self.port2server[anodes[0]['port']]] = \
            Popen(['swift-account-server',
                   '/etc/swift/account-server/%d.conf' %
                    ((anodes[0]['port'] - 6002) / 10)]).pid
        sleep(2)
        # This is the earlier object count and bytes because the first node
        # doesn't have the newest udpates yet.
        self.assert_(client.head_account(self.url, self.token), (2, 1, 4))
        containers = client.get_account(self.url, self.token)
        found1 = False
        found2 = False
        for c in containers:
            if c['name'] == container1:
                found1 = True
            elif c['name'] == container2:
                found2 = True
                # This is the earlier count and bytes because the first node
                # doesn't have the newest udpates yet.
                self.assertEquals(c['count'], 1)
                self.assertEquals(c['bytes'], 4)
        # This okay because the first node hasn't got the update that
        # container1 was deleted yet.
        self.assert_(found1)
        self.assert_(found2)

        get_to_final_state()
        containers = client.get_account(self.url, self.token)
        self.assert_(client.head_account(self.url, self.token), (2, 2, 9))
        found1 = False
        found2 = False
        for c in containers:
            if c['name'] == container1:
                found1 = True
            elif c['name'] == container2:
                found2 = True
                self.assertEquals(c['count'], 2)
                self.assertEquals(c['bytes'], 9)
        self.assert_(not found1)
        self.assert_(found2)


if __name__ == '__main__':
    unittest.main()
