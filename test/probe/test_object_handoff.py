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
from hashlib import md5
from collections import defaultdict
import os

from swiftclient import client

from swift.common import direct_client
from swift.common.exceptions import ClientException
from swift.common.manager import Manager
from test.probe.common import (kill_server, start_server, ReplProbeTest,
                               ECProbeTest, Body)


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
        client.put_object(self.url, self.token, container, obj, 'VERIFY')
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != 'VERIFY':
            raise Exception('Object GET did not return VERIFY, instead it '
                            'returned: %s' % repr(odata))

        # Kill other two container/obj primary servers
        #   to ensure GET handoff works
        for node in onodes[1:]:
            kill_server((node['ip'], node['port']), self.ipport2server)

        # Indirectly through proxy assert we can get container/obj
        odata = client.get_object(self.url, self.token, container, obj)[-1]
        if odata != 'VERIFY':
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
        if odata != 'VERIFY':
            raise Exception('Direct object GET did not return VERIFY, instead '
                            'it returned: %s' % repr(odata))

        # drop a tempfile in the handoff's datadir, like it might have
        # had if there was an rsync failure while it was previously a
        # primary
        handoff_device_path = self.device_dir('object', another_onode)
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

        # and that it does *not* have a temporary rsync dropping!
        found_data_filename = False
        primary_device_path = self.device_dir('object', onode)
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
            self.assertEqual(err.http_status, 404)
        else:
            self.fail("Expected ClientException but didn't get it")


class TestECObjectHandoffOverwrite(ECProbeTest):

    def get_object(self, container_name, object_name):
        headers, body = client.get_object(self.url, self.token,
                                          container_name,
                                          object_name,
                                          resp_chunk_size=64 * 2 ** 10)
        resp_checksum = md5()
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
        failed_primary_device_path = self.device_dir('object', failed_primary)
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
                         old_contents.etag)

        # we have 1 primary with wrong old etag, and we should have 5 with
        # new etag plus a handoff with the new etag, so killing 2 other
        # primaries forces proxy to try to GET from all primaries plus handoff.
        other_nodes = [n for n in onodes if n != failed_primary]
        random.shuffle(other_nodes)
        for node in other_nodes[:2]:
            self.kill_drive(self.device_dir('object', node))

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
            new_contents.etag: 4,  # this should be enough to rebuild!
            old_contents.etag: 1,
        })

        # clear node error limiting
        Manager(['proxy']).restart()

        resp_etag = self.get_object(container_name, object_name)
        self.assertEqual(resp_etag, new_contents.etag)

if __name__ == '__main__':
    main()
