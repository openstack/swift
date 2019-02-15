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

import errno
import json
from contextlib import contextmanager
from hashlib import md5
import unittest
import uuid
import shutil
import random
import os
import time

from swift.common.direct_client import DirectClientException
from test.probe.common import ECProbeTest

from swift.common import direct_client
from swift.common.storage_policy import EC_POLICY
from swift.common.manager import Manager

from swiftclient import client, ClientException


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
        return next(self)


class TestReconstructorRebuild(ECProbeTest):

    def _make_name(self, prefix):
        return '%s%s' % (prefix, uuid.uuid4())

    def setUp(self):
        super(TestReconstructorRebuild, self).setUp()
        self.container_name = self._make_name('container-')
        self.object_name = self._make_name('object-')
        # sanity
        self.assertEqual(self.policy.policy_type, EC_POLICY)
        self.reconstructor = Manager(["object-reconstructor"])

        # create EC container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

        # PUT object and POST some metadata
        self.proxy_put()
        self.headers_post = {
            self._make_name('x-object-meta-').decode('utf8'):
                self._make_name('meta-bar-').decode('utf8')}
        client.post_object(self.url, self.token, self.container_name,
                           self.object_name, headers=dict(self.headers_post))

        self.opart, self.onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)

        # stash frag etags and metadata for later comparison
        self.frag_headers, self.frag_etags = self._assert_all_nodes_have_frag()
        for node_index, hdrs in self.frag_headers.items():
            # sanity check
            self.assertIn(
                'X-Backend-Durable-Timestamp', hdrs,
                'Missing durable timestamp in %r' % self.frag_headers)

    def proxy_put(self, extra_headers=None):
        contents = Body()
        headers = {
            self._make_name('x-object-meta-').decode('utf8'):
                self._make_name('meta-foo-').decode('utf8'),
        }
        if extra_headers:
            headers.update(extra_headers)
        self.etag = client.put_object(self.url, self.token,
                                      self.container_name,
                                      self.object_name,
                                      contents=contents, headers=headers)

    def proxy_get(self):
        # GET object
        headers, body = client.get_object(self.url, self.token,
                                          self.container_name,
                                          self.object_name,
                                          resp_chunk_size=64 * 2 ** 10)
        resp_checksum = md5()
        for chunk in body:
            resp_checksum.update(chunk)
        return headers, resp_checksum.hexdigest()

    def direct_get(self, node, part, require_durable=True, extra_headers=None):
        req_headers = {'X-Backend-Storage-Policy-Index': int(self.policy)}
        if extra_headers:
            req_headers.update(extra_headers)
        if not require_durable:
            req_headers.update(
                {'X-Backend-Fragment-Preferences': json.dumps([])})
        # node dict has unicode values so utf8 decode our path parts too in
        # case they have non-ascii characters
        headers, data = direct_client.direct_get_object(
            node, part, self.account.decode('utf8'),
            self.container_name.decode('utf8'),
            self.object_name.decode('utf8'), headers=req_headers,
            resp_chunk_size=64 * 2 ** 20)
        hasher = md5()
        for chunk in data:
            hasher.update(chunk)
        return headers, hasher.hexdigest()

    def _break_nodes(self, failed, non_durable):
        # delete partitions on the failed nodes and remove durable marker from
        # non-durable nodes
        for i, node in enumerate(self.onodes):
            part_dir = self.storage_dir('object', node, part=self.opart)
            if i in failed:
                shutil.rmtree(part_dir, True)
                try:
                    self.direct_get(node, self.opart)
                except direct_client.DirectClientException as err:
                    self.assertEqual(err.http_status, 404)
            elif i in non_durable:
                for dirs, subdirs, files in os.walk(part_dir):
                    for fname in files:
                        if fname.endswith('.data'):
                            non_durable_fname = fname.replace('#d', '')
                            os.rename(os.path.join(dirs, fname),
                                      os.path.join(dirs, non_durable_fname))
                            break
                headers, etag = self.direct_get(node, self.opart,
                                                require_durable=False)
                self.assertNotIn('X-Backend-Durable-Timestamp', headers)
            try:
                os.remove(os.path.join(part_dir, 'hashes.pkl'))
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise

    def _format_node(self, node):
        return '%s#%s' % (node['device'], node['index'])

    def _assert_all_nodes_have_frag(self, extra_headers=None):
        # check all frags are in place
        failures = []
        frag_etags = {}
        frag_headers = {}
        for node in self.onodes:
            try:
                headers, etag = self.direct_get(node, self.opart,
                                                extra_headers=extra_headers)
                frag_etags[node['index']] = etag
                del headers['Date']  # Date header will vary so remove it
                frag_headers[node['index']] = headers
            except direct_client.DirectClientException as err:
                failures.append((node, err))
        if failures:
            self.fail('\n'.join(['    Node %r raised %r' %
                                 (self._format_node(node), exc)
                                 for (node, exc) in failures]))
        return frag_headers, frag_etags

    @contextmanager
    def _annotate_failure_with_scenario(self, failed, non_durable):
        try:
            yield
        except (AssertionError, ClientException) as err:
            self.fail(
                'Scenario with failed nodes: %r, non-durable nodes: %r\n'
                ' failed with:\n%s' %
                ([self._format_node(self.onodes[n]) for n in failed],
                 [self._format_node(self.onodes[n]) for n in non_durable], err)
            )

    def _test_rebuild_scenario(self, failed, non_durable,
                               reconstructor_cycles):
        # helper method to test a scenario with some nodes missing their
        # fragment and some nodes having non-durable fragments
        with self._annotate_failure_with_scenario(failed, non_durable):
            self._break_nodes(failed, non_durable)

        # make sure we can still GET the object and it is correct; the
        # proxy is doing decode on remaining fragments to get the obj
        with self._annotate_failure_with_scenario(failed, non_durable):
            headers, etag = self.proxy_get()
            self.assertEqual(self.etag, etag)
            for key in self.headers_post:
                self.assertIn(key, headers)
                self.assertEqual(self.headers_post[key], headers[key])

        # fire up reconstructor
        for i in range(reconstructor_cycles):
            self.reconstructor.once()

        # check GET via proxy returns expected data and metadata
        with self._annotate_failure_with_scenario(failed, non_durable):
            headers, etag = self.proxy_get()
            self.assertEqual(self.etag, etag)
            for key in self.headers_post:
                self.assertIn(key, headers)
                self.assertEqual(self.headers_post[key], headers[key])
        # check all frags are intact, durable and have expected metadata
        with self._annotate_failure_with_scenario(failed, non_durable):
            frag_headers, frag_etags = self._assert_all_nodes_have_frag()
            self.assertEqual(self.frag_etags, frag_etags)
            # self._frag_headers include X-Backend-Durable-Timestamp so this
            # assertion confirms that the rebuilt frags are all durable
            self.assertEqual(self.frag_headers, frag_headers)

    def test_rebuild_missing_frags(self):
        # build up a list of node lists to kill data from,
        # first try a single node
        # then adjacent nodes and then nodes >1 node apart
        single_node = (random.randint(0, 5),)
        adj_nodes = (0, 5)
        far_nodes = (0, 4)

        for failed_nodes in [single_node, adj_nodes, far_nodes]:
            self._test_rebuild_scenario(failed_nodes, [], 1)

    def test_rebuild_non_durable_frags(self):
        # build up a list of node lists to make non-durable,
        # first try a single node
        # then adjacent nodes and then nodes >1 node apart
        single_node = (random.randint(0, 5),)
        adj_nodes = (0, 5)
        far_nodes = (0, 4)

        for non_durable_nodes in [single_node, adj_nodes, far_nodes]:
            self._test_rebuild_scenario([], non_durable_nodes, 1)

    def test_rebuild_with_missing_frags_and_non_durable_frags(self):
        # pick some nodes with parts deleted, some with non-durable fragments
        scenarios = [
            # failed, non-durable
            ((0, 2), (4,)),
            ((0, 4), (2,)),
        ]
        for failed, non_durable in scenarios:
            self._test_rebuild_scenario(failed, non_durable, 3)
        scenarios = [
            # failed, non-durable
            ((0, 1), (2,)),
            ((0, 2), (1,)),
        ]
        for failed, non_durable in scenarios:
            # why 2 repeats? consider missing fragment on nodes 0, 1  and
            # missing durable on node 2: first reconstructor cycle on node 3
            # will make node 2 durable, first cycle on node 5 will rebuild on
            # node 0; second cycle on node 0 or 2 will rebuild on node 1. Note
            # that it is possible, that reconstructor processes on each node
            # run in order such that all rebuild complete in once cycle, but
            # that is not guaranteed, we allow 2 cycles to be sure.
            self._test_rebuild_scenario(failed, non_durable, 2)
        scenarios = [
            # failed, non-durable
            ((0, 2), (1, 3, 5)),
            ((0,), (1, 2, 4, 5)),
        ]
        for failed, non_durable in scenarios:
            # why 3 repeats? consider missing fragment on node 0 and single
            # durable on node 3: first reconstructor cycle on node 3 will make
            # nodes 2 and 4 durable, second cycle on nodes 2 and 4 will make
            # node 1 and 5 durable, third cycle on nodes 1 or 5 will
            # reconstruct the missing fragment on node 0.
            self._test_rebuild_scenario(failed, non_durable, 3)

    def test_rebuild_partner_down(self):
        # we have to pick a lower index because we have few handoffs
        nodes = self.onodes[:2]
        random.shuffle(nodes)  # left or right is fine
        primary_node, partner_node = nodes

        # capture fragment etag from partner
        failed_partner_meta, failed_partner_etag = self.direct_get(
            partner_node, self.opart)

        # and 507 the failed partner device
        device_path = self.device_dir('object', partner_node)
        self.kill_drive(device_path)

        # reconstruct from the primary, while one of it's partners is 507'd
        self.reconstructor.once(number=self.config_number(primary_node))

        # a handoff will pickup the rebuild
        hnodes = list(self.object_ring.get_more_nodes(self.opart))
        for node in hnodes:
            try:
                found_meta, found_etag = self.direct_get(
                    node, self.opart)
            except DirectClientException as e:
                if e.http_status != 404:
                    raise
            else:
                break
        else:
            self.fail('Unable to fetch rebuilt frag from handoffs %r '
                      'given primary nodes %r with %s unmounted '
                      'trying to rebuild from %s' % (
                          [h['device'] for h in hnodes],
                          [n['device'] for n in self.onodes],
                          partner_node['device'],
                          primary_node['device'],
                      ))
        self.assertEqual(failed_partner_etag, found_etag)
        del failed_partner_meta['Date']
        del found_meta['Date']
        self.assertEqual(failed_partner_meta, found_meta)

        # just to be nice
        self.revive_drive(device_path)

    def test_sync_expired_object(self):
        # verify that missing frag can be rebuilt for an expired object
        delete_after = 2
        self.proxy_put(extra_headers={'x-delete-after': delete_after})
        self.proxy_get()  # sanity check
        orig_frag_headers, orig_frag_etags = self._assert_all_nodes_have_frag(
            extra_headers={'X-Backend-Replication': 'True'})

        # wait for object to expire
        timeout = time.time() + delete_after + 1
        while time.time() < timeout:
            try:
                self.proxy_get()
            except ClientException as e:
                if e.http_status == 404:
                    break
                else:
                    raise
        else:
            self.fail('Timed out waiting for %s/%s to expire after %ss' % (
                self.container_name, self.object_name, delete_after))

        # sanity check - X-Backend-Replication let's us get expired frag...
        fail_node = random.choice(self.onodes)
        self.direct_get(fail_node, self.opart,
                        extra_headers={'X-Backend-Replication': 'True'})
        # ...until we remove the frag from fail_node
        self._break_nodes([self.onodes.index(fail_node)], [])
        # ...now it's really gone
        with self.assertRaises(DirectClientException) as cm:
            self.direct_get(fail_node, self.opart,
                            extra_headers={'X-Backend-Replication': 'True'})
        self.assertEqual(404, cm.exception.http_status)
        self.assertNotIn('X-Backend-Timestamp', cm.exception.http_headers)

        # run the reconstructor
        self.reconstructor.once()

        # the missing frag is now in place but expired
        with self.assertRaises(DirectClientException) as cm:
            self.direct_get(fail_node, self.opart)
        self.assertEqual(404, cm.exception.http_status)
        self.assertIn('X-Backend-Timestamp', cm.exception.http_headers)

        # check all frags are intact, durable and have expected metadata
        frag_headers, frag_etags = self._assert_all_nodes_have_frag(
            extra_headers={'X-Backend-Replication': 'True'})
        self.assertEqual(orig_frag_etags, frag_etags)
        self.maxDiff = None
        self.assertEqual(orig_frag_headers, frag_headers)

    def test_sync_unexpired_object_metadata(self):
        # verify that metadata can be sync'd to a frag that has missed a POST
        # and consequently that frag appears to be expired, when in fact the
        # POST removed the x-delete-at header
        client.put_container(self.url, self.token, self.container_name,
                             headers={'x-storage-policy': self.policy.name})
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        delete_at = int(time.time() + 3)
        contents = 'body-%s' % uuid.uuid4()
        headers = {'x-delete-at': delete_at}
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, headers=headers, contents=contents)
        # fail a primary
        post_fail_node = random.choice(onodes)
        post_fail_path = self.device_dir('object', post_fail_node)
        self.kill_drive(post_fail_path)
        # post over w/o x-delete-at
        client.post_object(self.url, self.token, self.container_name,
                           self.object_name, {'content-type': 'something-new'})
        # revive failed primary
        self.revive_drive(post_fail_path)
        # wait for the delete_at to pass, and check that it thinks the object
        # is expired
        timeout = time.time() + 5
        while time.time() < timeout:
            try:
                direct_client.direct_head_object(
                    post_fail_node, opart, self.account, self.container_name,
                    self.object_name, headers={
                        'X-Backend-Storage-Policy-Index': int(self.policy)})
            except direct_client.ClientException as err:
                if err.http_status != 404:
                    raise
                break
            else:
                time.sleep(0.1)
        else:
            self.fail('Failed to get a 404 from node with expired object')
        self.assertEqual(err.http_status, 404)
        self.assertIn('X-Backend-Timestamp', err.http_headers)

        # but from the proxy we've got the whole story
        headers, body = client.get_object(self.url, self.token,
                                          self.container_name,
                                          self.object_name)
        self.assertNotIn('X-Delete-At', headers)
        self.reconstructor.once()

        # ... and all the nodes have the final unexpired state
        for node in onodes:
            headers = direct_client.direct_head_object(
                node, opart, self.account, self.container_name,
                self.object_name, headers={
                    'X-Backend-Storage-Policy-Index': int(self.policy)})
            self.assertNotIn('X-Delete-At', headers)


class TestReconstructorRebuildUTF8(TestReconstructorRebuild):

    def _make_name(self, prefix):
        return '%s\xc3\xa8-%s' % (prefix, uuid.uuid4())


if __name__ == "__main__":
    unittest.main()
