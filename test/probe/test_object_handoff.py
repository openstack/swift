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
from uuid import uuid4

from swiftclient import client

from swift.common import direct_client
from swift.common.exceptions import ClientException
from swift.common.manager import Manager
from test.probe.common import kill_server, ReplProbeTest, start_server


class TestObjectHandoff(ReplProbeTest):

    def test_main(self):
        # Create container
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container,
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        # Kill one container/obj primary server
        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        cnode = cnodes[0]
        obj = 'object-%s' % uuid4()
        opart, onodes = self.object_ring.get_nodes(
            self.account, container, obj)
        onode = onodes[0]
        kill_server(onode['port'], self.port2server, self.pids)

        # Create container/obj (goes to two primary servers and one handoff)
        client.put_object(self.url, self.token, container, obj, 'VERIFY')
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != 'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))

        # Kill other two container/obj primary servers
        #   to ensure GET handoff works
        for node in onodes[1:]:
            kill_server(node['port'], self.port2server, self.pids)

        # Indirectly through proxy assert we can get container/obj
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != 'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))

        # Restart those other two container/obj primary servers
        for node in onodes[1:]:
            start_server(node['port'], self.port2server, self.pids)

        # We've indirectly verified the handoff node has the container/object,
        #   but let's directly verify it.
        another_onode = self.object_ring.get_more_nodes(opart).next()
        odata = direct_client.direct_get_object(
            another_onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]
        if odata != 'VERIFY':
            raise Exception('Direct object GET did not return VERIFY, instead '
                            'it returned: %s' % repr(odata))

        # Assert container listing (via proxy and directly) has container/obj
        objs = [o['name'] for o in
                client.get_container(self.url, self.token, container)[1]]
        if obj not in objs:
            raise Exception('Container listing did not know about object')
        for cnode in cnodes:
            objs = [o['name'] for o in
                    direct_client.direct_get_container(
                        cnode, cpart, self.account, container)[1]]
            if obj not in objs:
                raise Exception(
                    'Container server %s:%s did not know about object' %
                    (cnode['ip'], cnode['port']))

        # Bring the first container/obj primary server back up
        start_server(onode['port'], self.port2server, self.pids)

        # Assert that it doesn't have container/obj yet
        try:
            direct_client.direct_get_object(
                onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
        except ClientException as err:
            self.assertEquals(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")

        # Run object replication, ensuring we run the handoff node last so it
        #   will remove its extra handoff partition
        for node in onodes:
            try:
                port_num = node['replication_port']
            except KeyError:
                port_num = node['port']
            node_id = (port_num - 6000) / 10
            Manager(['object-replicator']).once(number=node_id)
        try:
            another_port_num = another_onode['replication_port']
        except KeyError:
            another_port_num = another_onode['port']
        another_num = (another_port_num - 6000) / 10
        Manager(['object-replicator']).once(number=another_num)

        # Assert the first container/obj primary server now has container/obj
        odata = direct_client.direct_get_object(
            onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]
        if odata != 'VERIFY':
            raise Exception('Direct object GET did not return VERIFY, instead '
                            'it returned: %s' % repr(odata))

        # Assert the handoff server no longer has container/obj
        try:
            direct_client.direct_get_object(
                another_onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
        except ClientException as err:
            self.assertEquals(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")

        # Kill the first container/obj primary server again (we have two
        #   primaries and the handoff up now)
        kill_server(onode['port'], self.port2server, self.pids)

        # Delete container/obj
        try:
            client.delete_object(self.url, self.token, container, obj)
        except client.ClientException as err:
            if self.object_ring.replica_count > 2:
                raise
            # Object DELETE returning 503 for (404, 204)
            # remove this with fix for
            # https://bugs.launchpad.net/swift/+bug/1318375
            self.assertEqual(503, err.http_status)

        # Assert we can't head container/obj
        try:
            client.head_object(self.url, self.token, container, obj)
        except client.ClientException as err:
            self.assertEquals(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")

        # Assert container/obj is not in the container listing, both indirectly
        #   and directly
        objs = [o['name'] for o in
                client.get_container(self.url, self.token, container)[1]]
        if obj in objs:
            raise Exception('Container listing still knew about object')
        for cnode in cnodes:
            objs = [o['name'] for o in
                    direct_client.direct_get_container(
                        cnode, cpart, self.account, container)[1]]
            if obj in objs:
                raise Exception(
                    'Container server %s:%s still knew about object' %
                    (cnode['ip'], cnode['port']))

        # Restart the first container/obj primary server again
        start_server(onode['port'], self.port2server, self.pids)

        # Assert it still has container/obj
        direct_client.direct_get_object(
            onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})

        # Run object replication, ensuring we run the handoff node last so it
        #   will remove its extra handoff partition
        for node in onodes:
            try:
                port_num = node['replication_port']
            except KeyError:
                port_num = node['port']
            node_id = (port_num - 6000) / 10
            Manager(['object-replicator']).once(number=node_id)
        another_node_id = (another_port_num - 6000) / 10
        Manager(['object-replicator']).once(number=another_node_id)

        # Assert primary node no longer has container/obj
        try:
            direct_client.direct_get_object(
                another_onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
        except ClientException as err:
            self.assertEquals(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")


if __name__ == '__main__':
    main()
