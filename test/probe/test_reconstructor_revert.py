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

import itertools
import unittest
import random
import shutil
from collections import defaultdict

from swift.obj.reconstructor import ObjectReconstructor
from test.probe.common import ECProbeTest, Body

from swift.common import direct_client
from swift.obj import reconstructor

from swiftclient import client


class TestReconstructorRevert(ECProbeTest):

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
        p_dev1 = self.device_dir(onodes[0])
        p_dev2 = self.device_dir(onodes[1])
        self.kill_drive(p_dev1)
        self.kill_drive(p_dev2)

        # PUT object
        contents = Body()
        headers = {'x-object-meta-foo': 'meta-foo',
                   u'x-object-meta-non-ascii-value1': u'meta-f\xf6\xf6'}
        headers_post = {'x-object-meta-bar': 'meta-bar',
                        u'x-object-meta-non-ascii-value2': u'meta-b\xe4r'}
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
            self.assert_direct_get_fails(onode, opart, 507)

        # now take out another primary
        p_dev3 = self.device_dir(onodes[2])
        self.kill_drive(p_dev3)

        # this node can't serve the data any more
        self.assert_direct_get_fails(onodes[2], opart, 507)

        # make sure we can still GET the object and its correct
        # we're now pulling from handoffs and reconstructing
        _headers, etag = self.proxy_get()
        self.assertEqual(etag, contents.etag)

        # rename the dev dirs so they don't 507 anymore
        self.revive_drive(p_dev1)
        self.revive_drive(p_dev2)
        self.revive_drive(p_dev3)

        # fire up reconstructor on handoff nodes only
        for hnode in hnodes:
            hnode_id = self.config_number(hnode)
            self.reconstructor.once(number=hnode_id)

        # first three primaries have data again
        for onode in (onodes[0], onodes[2]):
            self.assert_direct_get_succeeds(onode, opart)

        # check meta
        meta = client.head_object(self.url, self.token,
                                  self.container_name,
                                  self.object_name)
        for key in headers_post:
            self.assertIn(key, meta)
            self.assertEqual(meta[key], headers_post[key])

        # handoffs are empty
        for hnode in hnodes:
            self.assert_direct_get_fails(hnode, opart, 404)

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
            self.kill_drive(self.device_dir(node))

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
        self.revive_drive(self.device_dir(failed_nodes[0]))
        self.revive_drive(self.device_dir(failed_nodes[1]))

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
        primary_device = self.device_dir(primary_node)
        self.kill_drive(primary_device)

        # PUT object
        contents = Body()
        etag = client.put_object(self.url, self.token, self.container_name,
                                 self.object_name, contents=contents)
        self.assertEqual(contents.etag, etag)

        # fix the primary device and sanity GET
        self.revive_drive(primary_device)
        _headers, actual_etag = self.proxy_get()
        self.assertEqual(etag, actual_etag)

        # find a handoff holding the fragment
        for hnode in self.object_ring.get_more_nodes(opart):
            try:
                _hdrs, reverted_fragment_etag = self.direct_get(hnode, opart)
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
                    _hdrs, handoff_fragment_etag = self.direct_get(node, opart)
                except direct_client.DirectClientException as err:
                    if err.http_status != 404:
                        raise
                    # this just means our handoff device was on the same
                    # machine as the primary!
                    continue
                # use the primary nodes device - not the hnode device
                part_dir = self.storage_dir(node, part=opart)
                shutil.rmtree(part_dir, True)

        # revert from handoff device with reconstructor
        self.reconstructor.once(number=self.config_number(hnode))

        # verify fragment reverted to primary server
        self.assertEqual(reverted_fragment_etag,
                         self.direct_get(primary_node, opart)[1])

        # now we'll remove some data on one of the primary node's partners
        partner = random.choice(reconstructor._get_partners(
            primary_node['index'], onodes))

        try:
            _hdrs, rebuilt_fragment_etag = self.direct_get(partner, opart)
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
        part_dir = self.storage_dir(partner, part=opart)
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
                             self.direct_get(partner, opart)[1])
        except direct_client.DirectClientException as err:
            if err.http_status != 404:
                raise
            else:
                self.fail('Did not find rebuilt fragment on partner node')

    def test_handoff_non_durable(self):
        # verify that reconstructor reverts non-durable frags from handoff to
        # primary (and also durable frag of same object on same handoff) and
        # cleans up non-durable data files on handoffs after revert
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

        # get our node lists
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        pdevs = [self.device_dir(onode) for onode in onodes]
        hnodes = list(itertools.islice(
            self.object_ring.get_more_nodes(opart), 2))

        # kill a primary nodes so we can force data onto a handoff
        self.kill_drive(pdevs[0])

        # PUT object at t1
        contents = Body(total=3.5 * 2 ** 20)
        headers = {'x-object-meta-foo': 'meta-foo'}
        headers_post = {'content-type': 'meta/bar'}
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, contents=contents,
                          headers=headers)
        client.post_object(self.url, self.token, self.container_name,
                           self.object_name, headers=headers_post)
        # (Some versions of?) swiftclient will mutate the headers dict on post
        headers_post.pop('X-Auth-Token', None)

        # this primary can't serve the data; we expect 507 here and not 404
        # because we're using mount_check to kill nodes
        self.assert_direct_get_fails(onodes[0], opart, 507)
        # these primaries and first handoff do have the data
        for onode in (onodes[1:]):
            self.assert_direct_get_succeeds(onode, opart)
        _hdrs, older_frag_etag = self.assert_direct_get_succeeds(hnodes[0],
                                                                 opart)
        self.assert_direct_get_fails(hnodes[1], opart, 404)

        # make sure we can GET the object; there's 5 primaries and 1 handoff
        headers, older_obj_etag = self.proxy_get()
        self.assertEqual(contents.etag, older_obj_etag)
        self.assertEqual('meta/bar', headers.get('content-type'))

        # PUT object at t2; make all frags non-durable so that the previous
        # durable frags at t1 remain on object server; use InternalClient so
        # that x-backend-no-commit is passed through
        internal_client = self.make_internal_client()
        contents2 = Body(total=2.5 * 2 ** 20)  # different content
        self.assertNotEqual(contents2.etag, older_obj_etag)  # sanity check
        headers = {'x-backend-no-commit': 'True',
                   'content-type': 'meta/bar-new'}
        internal_client.upload_object(contents2, self.account,
                                      self.container_name.decode('utf8'),
                                      self.object_name.decode('utf8'),
                                      headers)
        # GET should still return the older durable object
        headers, obj_etag = self.proxy_get()
        self.assertEqual(older_obj_etag, obj_etag)
        self.assertEqual('meta/bar', headers.get('content-type'))
        # on handoff we have older durable and newer non-durable
        _hdrs, frag_etag = self.assert_direct_get_succeeds(hnodes[0], opart)
        self.assertEqual(older_frag_etag, frag_etag)
        _hdrs, newer_frag_etag = self.assert_direct_get_succeeds(
            hnodes[0], opart, require_durable=False)
        self.assertNotEqual(older_frag_etag, newer_frag_etag)

        # now make all the newer frags durable only on the 5 primaries
        self.assertEqual(5, self.make_durable(onodes[1:], opart))
        # now GET will return the newer object
        headers, newer_obj_etag = self.proxy_get()
        self.assertEqual(contents2.etag, newer_obj_etag)
        self.assertNotEqual(older_obj_etag, newer_obj_etag)
        self.assertEqual('meta/bar-new', headers.get('content-type'))

        # fix the 507'ing primary
        self.revive_drive(pdevs[0])

        # fire up reconstructor on handoff node only; commit_window is
        # set to zero to ensure the nondurable handoff frag is purged
        hnode_id = self.config_number(hnodes[0])
        self.run_custom_daemon(
            ObjectReconstructor, 'object-reconstructor', hnode_id,
            {'commit_window': '0'})

        # primary now has only the newer non-durable frag
        self.assert_direct_get_fails(onodes[0], opart, 404)
        _hdrs, frag_etag = self.assert_direct_get_succeeds(
            onodes[0], opart, require_durable=False)
        self.assertEqual(newer_frag_etag, frag_etag)

        # handoff has only the older durable
        _hdrs, frag_etag = self.assert_direct_get_succeeds(hnodes[0], opart)
        self.assertEqual(older_frag_etag, frag_etag)
        headers, frag_etag = self.assert_direct_get_succeeds(
            hnodes[0], opart, require_durable=False)
        self.assertEqual(older_frag_etag, frag_etag)
        self.assertEqual('meta/bar', headers.get('content-type'))

        # fire up reconstructor on handoff node only, again
        self.reconstructor.once(number=hnode_id)

        # primary now has the newer non-durable frag and the older durable frag
        headers, frag_etag = self.assert_direct_get_succeeds(onodes[0], opart)
        self.assertEqual(older_frag_etag, frag_etag)
        self.assertEqual('meta/bar', headers.get('content-type'))
        headers, frag_etag = self.assert_direct_get_succeeds(
            onodes[0], opart, require_durable=False)
        self.assertEqual(newer_frag_etag, frag_etag)
        self.assertEqual('meta/bar-new', headers.get('content-type'))

        # handoff has nothing
        self.assert_direct_get_fails(hnodes[0], opart, 404,
                                     require_durable=False)

        # kill all but first two primaries
        for pdev in pdevs[2:]:
            self.kill_drive(pdev)
        # fire up reconstructor on the remaining primary[1]; without the
        # other primaries, primary[1] cannot rebuild the frag but it can let
        # primary[0] know that its non-durable frag can be made durable
        self.reconstructor.once(number=self.config_number(onodes[1]))

        # first primary now has a *durable* *newer* frag - it *was* useful to
        # sync the non-durable!
        headers, frag_etag = self.assert_direct_get_succeeds(onodes[0], opart)
        self.assertEqual(newer_frag_etag, frag_etag)
        self.assertEqual('meta/bar-new', headers.get('content-type'))

        # revive primaries (in case we want to debug)
        for pdev in pdevs[2:]:
            self.revive_drive(pdev)


if __name__ == "__main__":
    unittest.main()
