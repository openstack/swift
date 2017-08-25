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

from hashlib import md5
import itertools
import unittest
import uuid
import random
import shutil
from collections import defaultdict

from test.probe.common import ECProbeTest, Body

from swift.common import direct_client
from swift.common.storage_policy import EC_POLICY
from swift.common.manager import Manager
from swift.obj import reconstructor

from swiftclient import client


class TestReconstructorRevert(ECProbeTest):

    def setUp(self):
        super(TestReconstructorRevert, self).setUp()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()

        # sanity
        self.assertEqual(self.policy.policy_type, EC_POLICY)
        self.reconstructor = Manager(["object-reconstructor"])

    def proxy_get(self):
        # GET object
        headers, body = client.get_object(self.url, self.token,
                                          self.container_name,
                                          self.object_name,
                                          resp_chunk_size=64 * 2 ** 10)
        resp_checksum = md5()
        for chunk in body:
            resp_checksum.update(chunk)
        return resp_checksum.hexdigest()

    def direct_get(self, node, part):
        req_headers = {'X-Backend-Storage-Policy-Index': int(self.policy)}
        headers, data = direct_client.direct_get_object(
            node, part, self.account, self.container_name,
            self.object_name, headers=req_headers,
            resp_chunk_size=64 * 2 ** 20)
        hasher = md5()
        for chunk in data:
            hasher.update(chunk)
        return hasher.hexdigest()

    def test_revert_object(self):
        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        hnodes = self.object_ring.get_more_nodes(opart)

        # kill 2 a parity count number of primary nodes so we can
        # force data onto handoffs, we do that by renaming dev dirs
        # to induce 507
        p_dev1 = self.device_dir('object', onodes[0])
        p_dev2 = self.device_dir('object', onodes[1])
        self.kill_drive(p_dev1)
        self.kill_drive(p_dev2)

        # PUT object
        contents = Body()
        headers = {'x-object-meta-foo': 'meta-foo'}
        headers_post = {'x-object-meta-bar': 'meta-bar'}
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, contents=contents,
                          headers=headers)
        client.post_object(self.url, self.token, self.container_name,
                           self.object_name, headers=headers_post)
        # (Some versions of?) swiftclient will mutate the headers dict on post
        headers_post.pop('X-Auth-Token', None)

        # these primaries can't serve the data any more, we expect 507
        # here and not 404 because we're using mount_check to kill nodes
        for onode in (onodes[0], onodes[1]):
            try:
                self.direct_get(onode, opart)
            except direct_client.DirectClientException as err:
                self.assertEqual(err.http_status, 507)
            else:
                self.fail('Node data on %r was not fully destroyed!' %
                          (onode,))

        # now take out another primary
        p_dev3 = self.device_dir('object', onodes[2])
        self.kill_drive(p_dev3)

        # this node can't servce the data any more
        try:
            self.direct_get(onodes[2], opart)
        except direct_client.DirectClientException as err:
            self.assertEqual(err.http_status, 507)
        else:
            self.fail('Node data on %r was not fully destroyed!' %
                      (onode,))

        # make sure we can still GET the object and its correct
        # we're now pulling from handoffs and reconstructing
        etag = self.proxy_get()
        self.assertEqual(etag, contents.etag)

        # rename the dev dirs so they don't 507 anymore
        self.revive_drive(p_dev1)
        self.revive_drive(p_dev2)
        self.revive_drive(p_dev3)

        # fire up reconstructor on handoff nodes only
        for hnode in hnodes:
            hnode_id = (hnode['port'] - 6000) / 10
            self.reconstructor.once(number=hnode_id)

        # first three primaries have data again
        for onode in (onodes[0], onodes[2]):
            self.direct_get(onode, opart)

        # check meta
        meta = client.head_object(self.url, self.token,
                                  self.container_name,
                                  self.object_name)
        for key in headers_post:
            self.assertIn(key, meta)
            self.assertEqual(meta[key], headers_post[key])

        # handoffs are empty
        for hnode in hnodes:
            try:
                self.direct_get(hnode, opart)
            except direct_client.DirectClientException as err:
                self.assertEqual(err.http_status, 404)
            else:
                self.fail('Node data on %r was not fully destroyed!' %
                          (hnode,))

    def test_delete_propagate(self):
        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        hnodes = list(itertools.islice(
            self.object_ring.get_more_nodes(opart), 2))

        # PUT object
        contents = Body()
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, contents=contents)

        # now lets shut down a couple of primaries
        failed_nodes = random.sample(onodes, 2)
        for node in failed_nodes:
            self.kill_drive(self.device_dir('object', node))

        # Write tombstones over the nodes that are still online
        client.delete_object(self.url, self.token,
                             self.container_name,
                             self.object_name)

        # spot check the primary nodes that are still online
        delete_timestamp = None
        for node in onodes:
            if node in failed_nodes:
                continue
            try:
                self.direct_get(node, opart)
            except direct_client.DirectClientException as err:
                self.assertEqual(err.http_status, 404)
                delete_timestamp = err.http_headers['X-Backend-Timestamp']
            else:
                self.fail('Node data on %r was not fully destroyed!' %
                          (node,))

        # run the reconstructor on the handoff node multiple times until
        # tombstone is pushed out - each handoff node syncs to a few
        # primaries each time
        iterations = 0
        while iterations < 52:
            self.reconstructor.once(number=self.config_number(hnodes[0]))
            iterations += 1
            # see if the tombstone is reverted
            try:
                self.direct_get(hnodes[0], opart)
            except direct_client.DirectClientException as err:
                self.assertEqual(err.http_status, 404)
                if 'X-Backend-Timestamp' not in err.http_headers:
                    # this means the tombstone is *gone* so it's reverted
                    break
        else:
            self.fail('Still found tombstone on %r after %s iterations' % (
                hnodes[0], iterations))

        # tombstone is still on the *second* handoff
        try:
            self.direct_get(hnodes[1], opart)
        except direct_client.DirectClientException as err:
            self.assertEqual(err.http_status, 404)
            self.assertEqual(err.http_headers['X-Backend-Timestamp'],
                             delete_timestamp)
        else:
            self.fail('Found obj data on %r' % hnodes[1])

        # repair the primaries
        self.revive_drive(self.device_dir('object', failed_nodes[0]))
        self.revive_drive(self.device_dir('object', failed_nodes[1]))

        # run reconstructor on second handoff
        self.reconstructor.once(number=self.config_number(hnodes[1]))

        # verify tombstone is reverted on the first pass
        try:
            self.direct_get(hnodes[1], opart)
        except direct_client.DirectClientException as err:
            self.assertEqual(err.http_status, 404)
            self.assertNotIn('X-Backend-Timestamp', err.http_headers)
        else:
            self.fail('Found obj data on %r' % hnodes[1])

        # sanity make sure proxy get can't find it
        try:
            self.proxy_get()
        except Exception as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail('Node data on %r was not fully destroyed!' %
                      (onodes[0]))

    def test_reconstruct_from_reverted_fragment_archive(self):
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)

        # find a primary server that only has one of it's devices in the
        # primary node list
        group_nodes_by_config = defaultdict(list)
        for n in onodes:
            group_nodes_by_config[self.config_number(n)].append(n)
        for config_number, node_list in group_nodes_by_config.items():
            if len(node_list) == 1:
                break
        else:
            self.fail('ring balancing did not use all available nodes')
        primary_node = node_list[0]

        # ... and 507 it's device
        primary_device = self.device_dir('object', primary_node)
        self.kill_drive(primary_device)

        # PUT object
        contents = Body()
        etag = client.put_object(self.url, self.token, self.container_name,
                                 self.object_name, contents=contents)
        self.assertEqual(contents.etag, etag)

        # fix the primary device and sanity GET
        self.revive_drive(primary_device)
        self.assertEqual(etag, self.proxy_get())

        # find a handoff holding the fragment
        for hnode in self.object_ring.get_more_nodes(opart):
            try:
                reverted_fragment_etag = self.direct_get(hnode, opart)
            except direct_client.DirectClientException as err:
                if err.http_status != 404:
                    raise
            else:
                break
        else:
            self.fail('Unable to find handoff fragment!')

        # we'll force the handoff device to revert instead of potentially
        # racing with rebuild by deleting any other fragments that may be on
        # the same server
        handoff_fragment_etag = None
        for node in onodes:
            if self.is_local_to(node, hnode):
                # we'll keep track of the etag of this fragment we're removing
                # in case we need it later (queue forshadowing music)...
                try:
                    handoff_fragment_etag = self.direct_get(node, opart)
                except direct_client.DirectClientException as err:
                    if err.http_status != 404:
                        raise
                    # this just means our handoff device was on the same
                    # machine as the primary!
                    continue
                # use the primary nodes device - not the hnode device
                part_dir = self.storage_dir('object', node, part=opart)
                shutil.rmtree(part_dir, True)

        # revert from handoff device with reconstructor
        self.reconstructor.once(number=self.config_number(hnode))

        # verify fragment reverted to primary server
        self.assertEqual(reverted_fragment_etag,
                         self.direct_get(primary_node, opart))

        # now we'll remove some data on one of the primary node's partners
        partner = random.choice(reconstructor._get_partners(
            primary_node['index'], onodes))

        try:
            rebuilt_fragment_etag = self.direct_get(partner, opart)
        except direct_client.DirectClientException as err:
            if err.http_status != 404:
                raise
            # partner already had it's fragment removed
            if (handoff_fragment_etag is not None and
                    self.is_local_to(hnode, partner)):
                # oh, well that makes sense then...
                rebuilt_fragment_etag = handoff_fragment_etag
            else:
                # I wonder what happened?
                self.fail('Partner inexplicably missing fragment!')
        part_dir = self.storage_dir('object', partner, part=opart)
        shutil.rmtree(part_dir, True)

        # sanity, it's gone
        try:
            self.direct_get(partner, opart)
        except direct_client.DirectClientException as err:
            if err.http_status != 404:
                raise
        else:
            self.fail('successful GET of removed partner fragment archive!?')

        # and force the primary node to do a rebuild
        self.reconstructor.once(number=self.config_number(primary_node))

        # and validate the partners rebuilt_fragment_etag
        try:
            self.assertEqual(rebuilt_fragment_etag,
                             self.direct_get(partner, opart))
        except direct_client.DirectClientException as err:
            if err.http_status != 404:
                raise
            else:
                self.fail('Did not find rebuilt fragment on partner node')


if __name__ == "__main__":
    unittest.main()
