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
import random
from collections import defaultdict
import os
import socket
import errno

from swiftclient import client

from swift.common import direct_client
from swift.common.exceptions import ClientException
from swift.common.manager import Manager
from swift.common.utils import md5
from test.probe.common import (
    Body, get_server_number, kill_server, start_server,
    ReplProbeTest, ECProbeTest)


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
        kill_server((onode['ip'], onode['port']), self.ipport2server)

        # Create container/obj (goes to two primary servers and one handoff)
        client.put_object(self.url, self.token, container, obj, b'VERIFY')
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != b'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))

        # Stash the on disk data from a primary for future comparison with the
        # handoff - this may not equal 'VERIFY' if for example the proxy has
        # crypto enabled
        direct_get_data = direct_client.direct_get_object(
            onodes[1], opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]

        # Kill other two container/obj primary servers
        #   to ensure GET handoff works
        for node in onodes[1:]:
            kill_server((node['ip'], node['port']), self.ipport2server)

        # Indirectly through proxy assert we can get container/obj
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != b'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))

        # Restart those other two container/obj primary servers
        for node in onodes[1:]:
            start_server((node['ip'], node['port']), self.ipport2server)

        # We've indirectly verified the handoff node has the container/object,
        #   but let's directly verify it.
        another_onode = next(self.object_ring.get_more_nodes(opart))
        odata = direct_client.direct_get_object(
            another_onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]
        self.assertEqual(direct_get_data, odata)

        # drop a tempfile in the handoff's datadir, like it might have
        # had if there was an rsync failure while it was previously a
        # primary
        handoff_device_path = self.device_dir(another_onode)
        data_filename = None
        for root, dirs, files in os.walk(handoff_device_path):
            for filename in files:
                if filename.endswith('.data'):
                    data_filename = filename
                    temp_filename = '.%s.6MbL6r' % data_filename
                    temp_filepath = os.path.join(root, temp_filename)
        if not data_filename:
            self.fail('Did not find any data files on %r' %
                      handoff_device_path)
        open(temp_filepath, 'w')

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
        start_server((onode['ip'], onode['port']), self.ipport2server)

        # Assert that it doesn't have container/obj yet
        try:
            direct_client.direct_get_object(
                onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
        except ClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")

        # Run object replication, ensuring we run the handoff node last so it
        #   will remove its extra handoff partition
        for node in onodes:
            _, node_id = get_server_number(
                (node['ip'], node.get('replication_port', node['port'])),
                self.ipport2server)
            Manager(['object-replicator']).once(number=node_id)
        another_port_num = another_onode.get(
            'replication_port', another_onode['port'])
        _, another_num = get_server_number(
            (another_onode['ip'], another_port_num), self.ipport2server)
        Manager(['object-replicator']).once(number=another_num)

        # Assert the first container/obj primary server now has container/obj
        odata = direct_client.direct_get_object(
            onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]
        self.assertEqual(direct_get_data, odata)

        # and that it does *not* have a temporary rsync dropping!
        found_data_filename = False
        primary_device_path = self.device_dir(onode)
        for root, dirs, files in os.walk(primary_device_path):
            for filename in files:
                if filename.endswith('.6MbL6r'):
                    self.fail('Found unexpected file %s' %
                              os.path.join(root, filename))
                if filename == data_filename:
                    found_data_filename = True
        self.assertTrue(found_data_filename,
                        'Did not find data file %r on %r' % (
                            data_filename, primary_device_path))

        # Assert the handoff server no longer has container/obj
        try:
            direct_client.direct_get_object(
                another_onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
        except ClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")

        # Kill the first container/obj primary server again (we have two
        #   primaries and the handoff up now)
        kill_server((onode['ip'], onode['port']), self.ipport2server)

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
            self.assertEqual(err.http_status, 404)
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
        start_server((onode['ip'], onode['port']), self.ipport2server)

        # Assert it still has container/obj
        direct_client.direct_get_object(
            onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})

        # Run object replication, ensuring we run the handoff node last so it
        #   will remove its extra handoff partition
        for node in onodes:
            _, node_id = get_server_number(
                (node['ip'], node.get('replication_port', node['port'])),
                self.ipport2server)
            Manager(['object-replicator']).once(number=node_id)
        _, another_node_id = get_server_number(
            (another_onode['ip'], another_port_num), self.ipport2server)
        Manager(['object-replicator']).once(number=another_node_id)

        # Assert primary node no longer has container/obj
        try:
            direct_client.direct_get_object(
                another_onode, opart, self.account, container, obj, headers={
                    'X-Backend-Storage-Policy-Index': self.policy.idx})
        except ClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")

    def test_stale_reads(self):
        # Create container
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container,
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        # Kill one primary obj server
        obj = 'object-%s' % uuid4()
        opart, onodes = self.object_ring.get_nodes(
            self.account, container, obj)
        onode = onodes[0]
        kill_server((onode['ip'], onode['port']), self.ipport2server)

        # Create container/obj (goes to two primaries and one handoff)
        client.put_object(self.url, self.token, container, obj, b'VERIFY')
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != b'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))

        # Stash the on disk data from a primary for future comparison with the
        # handoff - this may not equal 'VERIFY' if for example the proxy has
        # crypto enabled
        direct_get_data = direct_client.direct_get_object(
            onodes[1], opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]

        # Restart the first container/obj primary server again
        start_server((onode['ip'], onode['port']), self.ipport2server)

        # send a delete request to primaries
        client.delete_object(self.url, self.token, container, obj)

        # there should be .ts files in all primaries now
        for node in onodes:
            try:
                direct_client.direct_get_object(
                    node, opart, self.account, container, obj, headers={
                        'X-Backend-Storage-Policy-Index': self.policy.idx})
            except ClientException as err:
                self.assertEqual(err.http_status, 404)
            else:
                self.fail("Expected ClientException but didn't get it")

        # verify that handoff still has the data, DELETEs should have gone
        # only to primaries
        another_onode = next(self.object_ring.get_more_nodes(opart))
        handoff_data = direct_client.direct_get_object(
            another_onode, opart, self.account, container, obj, headers={
                'X-Backend-Storage-Policy-Index': self.policy.idx})[-1]
        self.assertEqual(handoff_data, direct_get_data)

        # Indirectly (i.e., through proxy) try to GET object, it should return
        # a 404, before bug #1560574, the proxy would return the stale object
        # from the handoff
        try:
            client.get_object(self.url, self.token, container, obj)
        except client.ClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")

    def test_missing_primaries(self):
        # Create container
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container,
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        # Create container/obj (goes to all three primaries)
        obj = 'object-%s' % uuid4()
        client.put_object(self.url, self.token, container, obj, b'VERIFY')
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != b'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))

        # Kill all primaries obj server
        obj = 'object-%s' % uuid4()
        opart, onodes = self.object_ring.get_nodes(
            self.account, container, obj)
        for onode in onodes:
            kill_server((onode['ip'], onode['port']), self.ipport2server)

        # Indirectly (i.e., through proxy) try to GET object, it should return
        # a 503, since all primaries will Timeout and handoffs return a 404.
        try:
            client.get_object(self.url, self.token, container, obj)
        except client.ClientException as err:
            self.assertEqual(err.http_status, 503)
        else:
            self.fail("Expected ClientException but didn't get it")

        # Restart the first container/obj primary server again
        onode = onodes[0]
        start_server((onode['ip'], onode['port']), self.ipport2server)

        # Send a delete that will reach first primary and handoff.
        # Sure, the DELETE will return a 404 since the handoff doesn't
        # have a .data file, but object server will still write a
        # Tombstone in the handoff node!
        try:
            client.delete_object(self.url, self.token, container, obj)
        except client.ClientException as err:
            self.assertEqual(err.http_status, 404)

        # kill the first container/obj primary server again
        kill_server((onode['ip'], onode['port']), self.ipport2server)

        # a new GET should return a 404, since all primaries will Timeout
        # and the handoff will return a 404 but this time with a tombstone
        try:
            client.get_object(self.url, self.token, container, obj)
        except client.ClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")


class TestECObjectHandoff(ECProbeTest):

    def get_object(self, container_name, object_name):
        headers, body = client.get_object(self.url, self.token,
                                          container_name,
                                          object_name,
                                          resp_chunk_size=64 * 2 ** 10)
        resp_checksum = md5(usedforsecurity=False)
        for chunk in body:
            resp_checksum.update(chunk)
        return resp_checksum.hexdigest()

    def test_ec_handoff_overwrite(self):
        container_name = 'container-%s' % uuid4()
        object_name = 'object-%s' % uuid4()

        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, container_name,
                             headers=headers)

        # PUT object
        old_contents = Body()
        client.put_object(self.url, self.token, container_name,
                          object_name, contents=old_contents)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, container_name, object_name)

        # shutdown one of the primary data nodes
        failed_primary = random.choice(onodes)
        failed_primary_device_path = self.device_dir(failed_primary)
        # first read its ec etag value for future reference - this may not
        # equal old_contents.etag if for example the proxy has crypto enabled
        req_headers = {'X-Backend-Storage-Policy-Index': int(self.policy)}
        headers = direct_client.direct_head_object(
            failed_primary, opart, self.account, container_name,
            object_name, headers=req_headers)
        old_backend_etag = headers['X-Object-Sysmeta-EC-Etag']

        self.kill_drive(failed_primary_device_path)

        # overwrite our object with some new data
        new_contents = Body()
        client.put_object(self.url, self.token, container_name,
                          object_name, contents=new_contents)
        self.assertNotEqual(new_contents.etag, old_contents.etag)

        # restore failed primary device
        self.revive_drive(failed_primary_device_path)

        # sanity - failed node has old contents
        req_headers = {'X-Backend-Storage-Policy-Index': int(self.policy)}
        headers = direct_client.direct_head_object(
            failed_primary, opart, self.account, container_name,
            object_name, headers=req_headers)
        self.assertEqual(headers['X-Object-Sysmeta-EC-Etag'],
                         old_backend_etag)

        # we have 1 primary with wrong old etag, and we should have 5 with
        # new etag plus a handoff with the new etag, so killing 2 other
        # primaries forces proxy to try to GET from all primaries plus handoff.
        other_nodes = [n for n in onodes if n != failed_primary]
        random.shuffle(other_nodes)
        # grab the value of the new content's ec etag for future reference
        headers = direct_client.direct_head_object(
            other_nodes[0], opart, self.account, container_name,
            object_name, headers=req_headers)
        new_backend_etag = headers['X-Object-Sysmeta-EC-Etag']
        for node in other_nodes[:2]:
            self.kill_drive(self.device_dir(node))

        # sanity, after taking out two primaries we should be down to
        # only four primaries, one of which has the old etag - but we
        # also have a handoff with the new etag out there
        found_frags = defaultdict(int)
        req_headers = {'X-Backend-Storage-Policy-Index': int(self.policy)}
        for node in onodes + list(self.object_ring.get_more_nodes(opart)):
            try:
                headers = direct_client.direct_head_object(
                    node, opart, self.account, container_name,
                    object_name, headers=req_headers)
            except Exception:
                continue
            found_frags[headers['X-Object-Sysmeta-EC-Etag']] += 1
        self.assertEqual(found_frags, {
            new_backend_etag: 4,  # this should be enough to rebuild!
            old_backend_etag: 1,
        })

        # clear node error limiting
        Manager(['proxy']).restart()

        resp_etag = self.get_object(container_name, object_name)
        self.assertEqual(resp_etag, new_contents.etag)

    def _check_nodes(self, opart, onodes, container_name, object_name):
        found_frags = defaultdict(int)
        req_headers = {'X-Backend-Storage-Policy-Index': int(self.policy)}
        for node in onodes + list(self.object_ring.get_more_nodes(opart)):
            try:
                headers = direct_client.direct_head_object(
                    node, opart, self.account, container_name,
                    object_name, headers=req_headers)
            except socket.error as e:
                if e.errno != errno.ECONNREFUSED:
                    raise
            except direct_client.DirectClientException as e:
                if e.http_status != 404:
                    raise
            else:
                found_frags[headers['X-Object-Sysmeta-Ec-Frag-Index']] += 1
        return found_frags

    def test_ec_handoff_duplicate_available(self):
        container_name = 'container-%s' % uuid4()
        object_name = 'object-%s' % uuid4()

        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, container_name,
                             headers=headers)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, container_name, object_name)

        # find both primary servers that have both of their devices in
        # the primary node list
        group_nodes_by_config = defaultdict(list)
        for n in onodes:
            group_nodes_by_config[self.config_number(n)].append(n)
        double_disk_primary = []
        for config_number, node_list in group_nodes_by_config.items():
            if len(node_list) > 1:
                double_disk_primary.append((config_number, node_list))

        # sanity, in a 4+2 with 8 disks two servers will be doubled
        self.assertEqual(len(double_disk_primary), 2)

        # shutdown the first double primary
        primary0_config_number, primary0_node_list = double_disk_primary[0]
        Manager(['object-server']).stop(number=primary0_config_number)

        # PUT object
        contents = Body()
        client.put_object(self.url, self.token, container_name,
                          object_name, contents=contents)

        # sanity fetch two frags on handoffs
        handoff_frags = []
        for node in self.object_ring.get_more_nodes(opart):
            headers, data = direct_client.direct_get_object(
                node, opart, self.account, container_name, object_name,
                headers={'X-Backend-Storage-Policy-Index': int(self.policy)}
            )
            handoff_frags.append((node, headers, data))

        # bring the first double primary back, and fail the other one
        Manager(['object-server']).start(number=primary0_config_number)
        primary1_config_number, primary1_node_list = double_disk_primary[1]
        Manager(['object-server']).stop(number=primary1_config_number)

        # we can still GET the object
        resp_etag = self.get_object(container_name, object_name)
        self.assertEqual(resp_etag, contents.etag)

        # now start to "revert" the first handoff frag
        node = primary0_node_list[0]
        handoff_node, headers, data = handoff_frags[0]
        # N.B. object server api returns quoted ETag
        headers['ETag'] = headers['Etag'].strip('"')
        headers['X-Backend-Storage-Policy-Index'] = int(self.policy)
        direct_client.direct_put_object(
            node, opart,
            self.account, container_name, object_name,
            contents=data, headers=headers)

        # sanity - check available frags
        frag2count = self._check_nodes(opart, onodes,
                                       container_name, object_name)
        # ... five frags total
        self.assertEqual(sum(frag2count.values()), 5)
        # ... only 4 unique indexes
        self.assertEqual(len(frag2count), 4)

        # we can still GET the object
        resp_etag = self.get_object(container_name, object_name)
        self.assertEqual(resp_etag, contents.etag)

        # ... but we need both handoffs or we get a error
        for handoff_node, hdrs, data in handoff_frags:
            Manager(['object-server']).stop(
                number=self.config_number(handoff_node))
            with self.assertRaises(Exception) as cm:
                self.get_object(container_name, object_name)
            self.assertIn(cm.exception.http_status, (404, 503))
            Manager(['object-server']).start(
                number=self.config_number(handoff_node))

        # fix everything
        Manager(['object-server']).start(number=primary1_config_number)
        Manager(["object-reconstructor"]).once()

        # sanity - check available frags
        frag2count = self._check_nodes(opart, onodes,
                                       container_name, object_name)
        # ... six frags total
        self.assertEqual(sum(frag2count.values()), 6)
        # ... all six unique
        self.assertEqual(len(frag2count), 6)

    def test_ec_primary_timeout(self):
        container_name = 'container-%s' % uuid4()
        object_name = 'object-%s' % uuid4()

        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, container_name,
                             headers=headers)

        # PUT object, should go to primary nodes
        old_contents = Body()
        client.put_object(self.url, self.token, container_name,
                          object_name, contents=old_contents)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, container_name, object_name)

        # shutdown three of the primary data nodes
        for i in range(3):
            failed_primary = onodes[i]
            failed_primary_device_path = self.device_dir(failed_primary)
            self.kill_drive(failed_primary_device_path)

        # Indirectly (i.e., through proxy) try to GET object, it should return
        # a 503, since all primaries will Timeout and handoffs return a 404.
        try:
            client.get_object(self.url, self.token, container_name,
                              object_name)
        except client.ClientException as err:
            self.assertEqual(err.http_status, 503)
        else:
            self.fail("Expected ClientException but didn't get it")

        # Send a delete to write down tombstones in the handoff nodes
        client.delete_object(self.url, self.token, container_name, object_name)

        # Now a new GET should return 404 because the handoff nodes
        # return a 404 with a Tombstone.
        try:
            client.get_object(self.url, self.token, container_name,
                              object_name)
        except client.ClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")


if __name__ == '__main__':
    main()
