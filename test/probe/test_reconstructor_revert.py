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
import unittest
import uuid
import os

from test.probe.common import ECProbeTest

from swift.common import direct_client
from swift.common.storage_policy import EC_POLICY
from swift.common.manager import Manager
from swift.common.utils import renamer

from swiftclient import client


class Body(object):

    def __init__(self, total=3.5 * 2 ** 20):
        self.total = total
        self.hasher = md5()
        self.size = 0
        self.chunk = 'test' * 16 * 2 ** 10

    @property
    def etag(self):
        return self.hasher.hexdigest()

    def __iter__(self):
        return self

    def next(self):
        if self.size > self.total:
            raise StopIteration()
        self.size += len(self.chunk)
        self.hasher.update(self.chunk)
        return self.chunk

    def __next__(self):
        return self.next()


class TestReconstructorRevert(ECProbeTest):

    def setUp(self):
        super(TestReconstructorRevert, self).setUp()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()

        # sanity
        self.assertEqual(self.policy.policy_type, EC_POLICY)
        self.reconstructor = Manager(["object-reconstructor"])

    def kill_drive(self, device):
        if os.path.ismount(device):
            os.system('sudo umount %s' % device)
        else:
            renamer(device, device + "X")

    def revive_drive(self, device):
        disabled_name = device + "X"
        if os.path.isdir(disabled_name):
            renamer(device + "X", device)
        else:
            os.system('sudo mount %s' % device)

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
        del headers_post['X-Auth-Token']  # WTF, where did this come from?

        # these primaries can't servce the data any more, we expect 507
        # here and not 404 because we're using mount_check to kill nodes
        for onode in (onodes[0], onodes[1]):
            try:
                self.direct_get(onode, opart)
            except direct_client.DirectClientException as err:
                self.assertEqual(err.http_status, 507)
            else:
                self.fail('Node data on %r was not fully destoryed!' %
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
            self.fail('Node data on %r was not fully destoryed!' %
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

        # first threee primaries have data again
        for onode in (onodes[0], onodes[2]):
            self.direct_get(onode, opart)

        # check meta
        meta = client.head_object(self.url, self.token,
                                  self.container_name,
                                  self.object_name)
        for key in headers_post:
            self.assertTrue(key in meta)
            self.assertEqual(meta[key], headers_post[key])

        # handoffs are empty
        for hnode in hnodes:
            try:
                self.direct_get(hnode, opart)
            except direct_client.DirectClientException as err:
                self.assertEqual(err.http_status, 404)
            else:
                self.fail('Node data on %r was not fully destoryed!' %
                          (hnode,))

    def test_delete_propogate(self):
        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        hnodes = self.object_ring.get_more_nodes(opart)
        p_dev2 = self.device_dir('object', onodes[1])

        # PUT object
        contents = Body()
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, contents=contents)

        # now lets shut one down
        self.kill_drive(p_dev2)

        # delete on the ones that are left
        client.delete_object(self.url, self.token,
                             self.container_name,
                             self.object_name)

        # spot check a node
        try:
            self.direct_get(onodes[0], opart)
        except direct_client.DirectClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail('Node data on %r was not fully destoryed!' %
                      (onodes[0],))

        # enable the first node again
        self.revive_drive(p_dev2)

        # propogate the delete...
        # fire up reconstructor on handoff nodes only
        for hnode in hnodes:
            hnode_id = (hnode['port'] - 6000) / 10
            self.reconstructor.once(number=hnode_id, override_devices=['sdb8'])

        # check the first node to make sure its gone
        try:
            self.direct_get(onodes[1], opart)
        except direct_client.DirectClientException as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail('Node data on %r was not fully destoryed!' %
                      (onodes[0]))

        # make sure proxy get can't find it
        try:
            self.proxy_get()
        except Exception as err:
            self.assertEqual(err.http_status, 404)
        else:
            self.fail('Node data on %r was not fully destoryed!' %
                      (onodes[0]))


if __name__ == "__main__":
    unittest.main()
