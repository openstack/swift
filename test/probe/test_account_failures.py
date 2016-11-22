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

from unittest import main

from swiftclient import client

from swift.common import direct_client
from swift.common.manager import Manager
from test.probe.common import kill_nonprimary_server, \
    kill_server, ReplProbeTest, start_server


class TestAccountFailures(ReplProbeTest):

    def test_main(self):
        # Create container1 and container2
        container1 = 'container1'
        client.put_container(self.url, self.token, container1)
        container2 = 'container2'
        client.put_container(self.url, self.token, container2)

        # Assert account level sees them
        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(headers['x-account-container-count'], '2')
        self.assertEqual(headers['x-account-object-count'], '0')
        self.assertEqual(headers['x-account-bytes-used'], '0')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
                self.assertEqual(container['count'], 0)
                self.assertEqual(container['bytes'], 0)
            elif container['name'] == container2:
                found2 = True
                self.assertEqual(container['count'], 0)
                self.assertEqual(container['bytes'], 0)
        self.assertTrue(found1)
        self.assertTrue(found2)

        # Create container2/object1
        client.put_object(self.url, self.token, container2, 'object1', '1234')

        # Assert account level doesn't see it yet
        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(headers['x-account-container-count'], '2')
        self.assertEqual(headers['x-account-object-count'], '0')
        self.assertEqual(headers['x-account-bytes-used'], '0')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
                self.assertEqual(container['count'], 0)
                self.assertEqual(container['bytes'], 0)
            elif container['name'] == container2:
                found2 = True
                self.assertEqual(container['count'], 0)
                self.assertEqual(container['bytes'], 0)
        self.assertTrue(found1)
        self.assertTrue(found2)

        # Get to final state
        self.get_to_final_state()

        # Assert account level now sees the container2/object1
        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(headers['x-account-container-count'], '2')
        self.assertEqual(headers['x-account-object-count'], '1')
        self.assertEqual(headers['x-account-bytes-used'], '4')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
                self.assertEqual(container['count'], 0)
                self.assertEqual(container['bytes'], 0)
            elif container['name'] == container2:
                found2 = True
                self.assertEqual(container['count'], 1)
                self.assertEqual(container['bytes'], 4)
        self.assertTrue(found1)
        self.assertTrue(found2)

        apart, anodes = self.account_ring.get_nodes(self.account)
        kill_nonprimary_server(anodes, self.ipport2server)
        kill_server((anodes[0]['ip'], anodes[0]['port']), self.ipport2server)
        # Kill account servers excepting two of the primaries

        # Delete container1
        client.delete_container(self.url, self.token, container1)

        # Put container2/object2
        client.put_object(self.url, self.token, container2, 'object2', '12345')

        # Assert account level knows container1 is gone but doesn't know about
        #   container2/object2 yet
        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(headers['x-account-container-count'], '1')
        self.assertEqual(headers['x-account-object-count'], '1')
        self.assertEqual(headers['x-account-bytes-used'], '4')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEqual(container['count'], 1)
                self.assertEqual(container['bytes'], 4)
        self.assertFalse(found1)
        self.assertTrue(found2)

        # Run container updaters
        Manager(['container-updater']).once()

        # Assert account level now knows about container2/object2
        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(headers['x-account-container-count'], '1')
        self.assertEqual(headers['x-account-object-count'], '2')
        self.assertEqual(headers['x-account-bytes-used'], '9')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEqual(container['count'], 2)
                self.assertEqual(container['bytes'], 9)
        self.assertFalse(found1)
        self.assertTrue(found2)

        # Restart other primary account server
        start_server((anodes[0]['ip'], anodes[0]['port']), self.ipport2server)

        # Assert that server doesn't know about container1's deletion or the
        #   new container2/object2 yet
        headers, containers = \
            direct_client.direct_get_account(anodes[0], apart, self.account)
        self.assertEqual(headers['x-account-container-count'], '2')
        self.assertEqual(headers['x-account-object-count'], '1')
        self.assertEqual(headers['x-account-bytes-used'], '4')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEqual(container['count'], 1)
                self.assertEqual(container['bytes'], 4)
        self.assertTrue(found1)
        self.assertTrue(found2)

        # Get to final state
        self.get_to_final_state()

        # Assert that server is now up to date
        headers, containers = \
            direct_client.direct_get_account(anodes[0], apart, self.account)
        self.assertEqual(headers['x-account-container-count'], '1')
        self.assertEqual(headers['x-account-object-count'], '2')
        self.assertEqual(headers['x-account-bytes-used'], '9')
        found1 = False
        found2 = False
        for container in containers:
            if container['name'] == container1:
                found1 = True
            elif container['name'] == container2:
                found2 = True
                self.assertEqual(container['count'], 2)
                self.assertEqual(container['bytes'], 9)
                self.assertEqual(container['bytes'], 9)
        self.assertFalse(found1)
        self.assertTrue(found2)


if __name__ == '__main__':
    main()
