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

from swift.common import client

from test.probe.common import get_to_final_state, kill_pids, reset_environment


class TestContainerFailures(unittest.TestCase):

    def setUp(self):
        self.pids, self.port2server, self.account_ring, self.container_ring, \
            self.object_ring, self.url, self.token, self.account = \
                reset_environment()

    def tearDown(self):
        kill_pids(self.pids)

    def test_first_node_fail(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])

        object1 = 'object1'
        client.put_object(self.url, self.token, container, object1, 'test')
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        kill(self.pids[self.port2server[cnodes[0]['port']]], SIGTERM)

        client.delete_object(self.url, self.token, container, object1)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        self.pids[self.port2server[cnodes[0]['port']]] = \
            Popen(['swift-container-server',
                   '/etc/swift/container-server/%d.conf' %
                    ((cnodes[0]['port'] - 6001) / 10)]).pid
        sleep(2)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        # This okay because the first node hasn't got the update that the
        # object was deleted yet.
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        # This fails because all three nodes have to indicate deletion before
        # we tell the user it worked. Since the first node 409s (it hasn't got
        # the update that the object was deleted yet), the whole must 503
        # (until every is synced up, then the delete would work).
        exc = None
        try:
            client.delete_container(self.url, self.token, container)
        except client.ClientException, err:
            exc = err
        self.assert_(exc)
        self.assert_(exc.http_status, 503)
        # Unfortunately, the following might pass or fail, depending on the
        # position of the account server associated with the first container
        # server we had killed. If the associated happens to be the first
        # account server, this'll pass, otherwise the first account server will
        # serve the listing and not have the container.
        # self.assert_(container in [c['name'] for c in
        #              client.get_account(self.url, self.token)[1]])

        object2 = 'object2'
        # This will work because at least one (in this case, just one) account
        # server has to indicate the container exists for the put to continue.
        client.put_object(self.url, self.token, container, object2, 'test')
        # First node still doesn't know object1 was deleted yet; this is okay.
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        # And, of course, our new object2 exists.
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        get_to_final_state()
        # Our container delete never "finalized" because we started using it
        # before the delete settled.
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        # And, so our object2 should still exist and object1's delete should
        # have finalized.
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

    def test_second_node_fail(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])

        object1 = 'object1'
        client.put_object(self.url, self.token, container, object1, 'test')
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        kill(self.pids[self.port2server[cnodes[1]['port']]], SIGTERM)

        client.delete_object(self.url, self.token, container, object1)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        self.pids[self.port2server[cnodes[1]['port']]] = \
            Popen(['swift-container-server',
                   '/etc/swift/container-server/%d.conf' %
                    ((cnodes[1]['port'] - 6001) / 10)]).pid
        sleep(2)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        # This fails because all three nodes have to indicate deletion before
        # we tell the user it worked. Since the first node 409s (it hasn't got
        # the update that the object was deleted yet), the whole must 503
        # (until every is synced up, then the delete would work).
        exc = None
        try:
            client.delete_container(self.url, self.token, container)
        except client.ClientException, err:
            exc = err
        self.assert_(exc)
        self.assert_(exc.http_status, 503)
        # Unfortunately, the following might pass or fail, depending on the
        # position of the account server associated with the first container
        # server we had killed. If the associated happens to be the first
        # account server, this'll pass, otherwise the first account server will
        # serve the listing and not have the container.
        # self.assert_(container in [c['name'] for c in
        #     client.get_account(self.url, self.token)[1]])

        object2 = 'object2'
        # This will work because at least one (in this case, just one) account
        # server has to indicate the container exists for the put to continue.
        client.put_object(self.url, self.token, container, object2, 'test')
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        # And, of course, our new object2 exists.
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        get_to_final_state()
        # Our container delete never "finalized" because we started using it
        # before the delete settled.
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        # And, so our object2 should still exist and object1's delete should
        # have finalized.
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

    def test_first_two_nodes_fail(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])

        object1 = 'object1'
        client.put_object(self.url, self.token, container, object1, 'test')
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        for x in xrange(2):
            kill(self.pids[self.port2server[cnodes[x]['port']]], SIGTERM)

        client.delete_object(self.url, self.token, container, object1)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        for x in xrange(2):
            self.pids[self.port2server[cnodes[x]['port']]] = \
                Popen(['swift-container-server',
                       '/etc/swift/container-server/%d.conf' %
                        ((cnodes[x]['port'] - 6001) / 10)]).pid
        sleep(2)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        # This okay because the first node hasn't got the update that the
        # object was deleted yet.
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        # This fails because all three nodes have to indicate deletion before
        # we tell the user it worked. Since the first node 409s (it hasn't got
        # the update that the object was deleted yet), the whole must 503
        # (until every is synced up, then the delete would work).
        exc = None
        try:
            client.delete_container(self.url, self.token, container)
        except client.ClientException, err:
            exc = err
        self.assert_(exc)
        self.assert_(exc.http_status, 503)
        # Unfortunately, the following might pass or fail, depending on the
        # position of the account server associated with the first container
        # server we had killed. If the associated happens to be the first
        # account server, this'll pass, otherwise the first account server will
        # serve the listing and not have the container.
        # self.assert_(container in [c['name'] for c in
        #              client.get_account(self.url, self.token)[1]])

        object2 = 'object2'
        # This will work because at least one (in this case, just one) account
        # server has to indicate the container exists for the put to continue.
        client.put_object(self.url, self.token, container, object2, 'test')
        # First node still doesn't know object1 was deleted yet; this is okay.
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        # And, of course, our new object2 exists.
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        get_to_final_state()
        # Our container delete never "finalized" because we started using it
        # before the delete settled.
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        # And, so our object2 should still exist and object1's delete should
        # have finalized.
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

    def test_last_two_nodes_fail(self):
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])

        object1 = 'object1'
        client.put_object(self.url, self.token, container, object1, 'test')
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        for x in (1, 2):
            kill(self.pids[self.port2server[cnodes[x]['port']]], SIGTERM)

        client.delete_object(self.url, self.token, container, object1)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        for x in (1, 2):
            self.pids[self.port2server[cnodes[x]['port']]] = \
                Popen(['swift-container-server',
                       '/etc/swift/container-server/%d.conf' %
                        ((cnodes[x]['port'] - 6001) / 10)]).pid
        sleep(2)
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        # This fails because all three nodes have to indicate deletion before
        # we tell the user it worked. Since the first node 409s (it hasn't got
        # the update that the object was deleted yet), the whole must 503
        # (until every is synced up, then the delete would work).
        exc = None
        try:
            client.delete_container(self.url, self.token, container)
        except client.ClientException, err:
            exc = err
        self.assert_(exc)
        self.assert_(exc.http_status, 503)
        # Unfortunately, the following might pass or fail, depending on the
        # position of the account server associated with the first container
        # server we had killed. If the associated happens to be the first
        # account server, this'll pass, otherwise the first account server will
        # serve the listing and not have the container.
        # self.assert_(container in [c['name'] for c in
        #     client.get_account(self.url, self.token)[1]])

        object2 = 'object2'
        # This will work because at least one (in this case, just one) account
        # server has to indicate the container exists for the put to continue.
        client.put_object(self.url, self.token, container, object2, 'test')
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        # And, of course, our new object2 exists.
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])

        get_to_final_state()
        # Our container delete never "finalized" because we started using it
        # before the delete settled.
        self.assert_(container in [c['name'] for c in
                     client.get_account(self.url, self.token)[1]])
        # And, so our object2 should still exist and object1's delete should
        # have finalized.
        self.assert_(object1 not in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])
        self.assert_(object2 in [o['name'] for o in
                     client.get_container(self.url, self.token, container)[1]])


if __name__ == '__main__':
    unittest.main()
