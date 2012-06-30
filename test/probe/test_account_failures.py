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

from swiftclient import client

from swift.common import direct_client
from test.probe.common import get_to_final_state, kill_nonprimary_server, \
    kill_server, kill_servers, reset_environment, start_server


class TestAccountFailures(TestCase):

    def setUp(self):
        (self.pids, self.port2server, self.account_ring, self.container_ring,
         self.object_ring, self.url, self.token,
         self.account) = reset_environment()

    def tearDown(self):
        kill_servers(self.port2server, self.pids)

    def test_main(self):
        # Create container1 and container2
        # Assert account level sees them
        # Create container2/object1
        # Assert account level doesn't see it yet
        # Get to final state
        # Assert account level now sees the container2/object1
        # Kill account servers excepting two of the primaries
        # Delete container1
        # Assert account level knows container1 is gone but doesn't know about
        #   container2/object2 yet
        # Put container2/object2
        # Run container updaters
        # Assert account level now knows about container2/object2
        # Restart other primary account server
        # Assert that server doesn't know about container1's deletion or the
        #   new container2/object2 yet
        # Get to final state
        # Assert that server is now up to date

        container1 = 'container1'
        client.put_container(self.url, self.token, container1)
        container2 = 'container2'
        client.put_container(self.url, self.token, container2)
        headers, containers = client.get_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '2')
        self.assertEquals(headers['x-account-object-count'], '0')
        self.assertEquals(headers['x-account-bytes-used'], '0')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
                self.assertEquals(container['count'], 0)
                self.assertEquals(container['bytes'], 0)
            elif container['name'] == container2:
                found2 = True
                self.assertEquals(container['count'], 0)
                self.assertEquals(container['bytes'], 0)
        self.assert_(found1)
        self.assert_(found2)

        client.put_object(self.url, self.token, container2, 'object1', '1234')
        headers, containers = client.get_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '2')
        self.assertEquals(headers['x-account-object-count'], '0')
        self.assertEquals(headers['x-account-bytes-used'], '0')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
                self.assertEquals(container['count'], 0)
                self.assertEquals(container['bytes'], 0)
            elif container['name'] == container2:
                found2 = True
                self.assertEquals(container['count'], 0)
                self.assertEquals(container['bytes'], 0)
        self.assert_(found1)
        self.assert_(found2)

        get_to_final_state()
        headers, containers = client.get_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '2')
        self.assertEquals(headers['x-account-object-count'], '1')
        self.assertEquals(headers['x-account-bytes-used'], '4')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
                self.assertEquals(container['count'], 0)
                self.assertEquals(container['bytes'], 0)
            elif container['name'] == container2:
                found2 = True
                self.assertEquals(container['count'], 1)
                self.assertEquals(container['bytes'], 4)
        self.assert_(found1)
        self.assert_(found2)

        apart, anodes = self.account_ring.get_nodes(self.account)
        kill_nonprimary_server(anodes, self.port2server, self.pids)
        kill_server(anodes[0]['port'], self.port2server, self.pids)

        client.delete_container(self.url, self.token, container1)
        client.put_object(self.url, self.token, container2, 'object2', '12345')
        headers, containers = client.get_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '1')
        self.assertEquals(headers['x-account-object-count'], '1')
        self.assertEquals(headers['x-account-bytes-used'], '4')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEquals(container['count'], 1)
                self.assertEquals(container['bytes'], 4)
        self.assert_(not found1)
        self.assert_(found2)

        processes = []
        for node in xrange(1, 5):
            processes.append(Popen([
                'swift-container-updater',
                '/etc/swift/container-server/%d.conf' % node,
                'once']))
        for process in processes:
            process.wait()
        headers, containers = client.get_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '1')
        self.assertEquals(headers['x-account-object-count'], '2')
        self.assertEquals(headers['x-account-bytes-used'], '9')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEquals(container['count'], 2)
                self.assertEquals(container['bytes'], 9)
        self.assert_(not found1)
        self.assert_(found2)

        start_server(anodes[0]['port'], self.port2server, self.pids)

        headers, containers = \
            direct_client.direct_get_account(anodes[0], apart, self.account)
        self.assertEquals(headers['x-account-container-count'], '2')
        self.assertEquals(headers['x-account-object-count'], '1')
        self.assertEquals(headers['x-account-bytes-used'], '4')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEquals(container['count'], 1)
                self.assertEquals(container['bytes'], 4)
        self.assert_(found1)
        self.assert_(found2)

        get_to_final_state()
        headers, containers = \
            direct_client.direct_get_account(anodes[0], apart, self.account)
        self.assertEquals(headers['x-account-container-count'], '1')
        self.assertEquals(headers['x-account-object-count'], '2')
        self.assertEquals(headers['x-account-bytes-used'], '9')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEquals(container['count'], 2)
                self.assertEquals(container['bytes'], 9)
        self.assert_(not found1)
        self.assert_(found2)


if __name__ == '__main__':
    main()
