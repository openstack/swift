#!/usr/bin/python -u
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
import time
import uuid
import random
import unittest

from swift.common.manager import Manager
from swift.common.internal_client import InternalClient
from swift.common import utils, direct_client
from swift.common.storage_policy import POLICIES
from swift.common.http import HTTP_NOT_FOUND
from test.probe.brain import BrainSplitter
from test.probe.common import (ReplProbeTest, ENABLED_POLICIES,
                               POLICIES_BY_TYPE, REPL_POLICY)

from swiftclient import client, ClientException

TIMEOUT = 60


class TestContainerMergePolicyIndex(ReplProbeTest):

    @unittest.skipIf(len(ENABLED_POLICIES) < 2, "Need more than one policy")
    def setUp(self):
        super(TestContainerMergePolicyIndex, self).setUp()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   self.object_name, 'container')

    def _get_object_patiently(self, policy_index):
        # use proxy to access object (bad container info might be cached...)
        timeout = time.time() + TIMEOUT
        while time.time() < timeout:
            try:
                return client.get_object(self.url, self.token,
                                         self.container_name,
                                         self.object_name)
            except ClientException as err:
                if err.http_status != HTTP_NOT_FOUND:
                    raise
                time.sleep(1)
        else:
            self.fail('could not HEAD /%s/%s/%s/ from policy %s '
                      'after %s seconds.' % (
                          self.account, self.container_name, self.object_name,
                          int(policy_index), TIMEOUT))

    def test_merge_storage_policy_index(self):
        # generic split brain
        self.brain.stop_primary_half()
        self.brain.put_container()
        self.brain.start_primary_half()
        self.brain.stop_handoff_half()
        self.brain.put_container()
        self.brain.put_object(headers={'x-object-meta-test': 'custom-meta'},
                              contents='VERIFY')
        self.brain.start_handoff_half()
        # make sure we have some manner of split brain
        container_part, container_nodes = self.container_ring.get_nodes(
            self.account, self.container_name)
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        found_policy_indexes = \
            set(metadata['X-Backend-Storage-Policy-Index'] for
                node, metadata in head_responses)
        self.assertGreater(
            len(found_policy_indexes), 1,
            'primary nodes did not disagree about policy index %r' %
            head_responses)
        # find our object
        orig_policy_index = None
        for policy_index in found_policy_indexes:
            object_ring = POLICIES.get_object_ring(policy_index, '/etc/swift')
            part, nodes = object_ring.get_nodes(
                self.account, self.container_name, self.object_name)
            for node in nodes:
                try:
                    direct_client.direct_head_object(
                        node, part, self.account, self.container_name,
                        self.object_name,
                        headers={'X-Backend-Storage-Policy-Index':
                                 policy_index})
                except direct_client.ClientException as err:
                    continue
                orig_policy_index = policy_index
                break
            if orig_policy_index is not None:
                break
        else:
            self.fail('Unable to find /%s/%s/%s in %r' % (
                self.account, self.container_name, self.object_name,
                found_policy_indexes))
        self.get_to_final_state()
        Manager(['container-reconciler']).once()
        # validate containers
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        found_policy_indexes = \
            set(metadata['X-Backend-Storage-Policy-Index'] for
                node, metadata in head_responses)
        self.assertEqual(len(found_policy_indexes), 1,
                         'primary nodes disagree about policy index %r' %
                         head_responses)

        expected_policy_index = found_policy_indexes.pop()
        self.assertNotEqual(orig_policy_index, expected_policy_index)
        # validate object placement
        orig_policy_ring = POLICIES.get_object_ring(orig_policy_index,
                                                    '/etc/swift')
        for node in orig_policy_ring.devs:
            try:
                direct_client.direct_head_object(
                    node, part, self.account, self.container_name,
                    self.object_name, headers={
                        'X-Backend-Storage-Policy-Index': orig_policy_index})
            except direct_client.ClientException as err:
                if err.http_status == HTTP_NOT_FOUND:
                    continue
                raise
            else:
                self.fail('Found /%s/%s/%s in %s' % (
                    self.account, self.container_name, self.object_name,
                    orig_policy_index))
        # verify that the object data read by external client is correct
        headers, data = self._get_object_patiently(expected_policy_index)
        self.assertEqual('VERIFY', data)
        self.assertEqual('custom-meta', headers['x-object-meta-test'])

    def test_reconcile_delete(self):
        # generic split brain
        self.brain.stop_primary_half()
        self.brain.put_container()
        self.brain.put_object()
        self.brain.start_primary_half()
        self.brain.stop_handoff_half()
        self.brain.put_container()
        self.brain.delete_object()
        self.brain.start_handoff_half()
        # make sure we have some manner of split brain
        container_part, container_nodes = self.container_ring.get_nodes(
            self.account, self.container_name)
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        found_policy_indexes = \
            set(metadata['X-Backend-Storage-Policy-Index'] for
                node, metadata in head_responses)
        self.assertGreater(
            len(found_policy_indexes), 1,
            'primary nodes did not disagree about policy index %r' %
            head_responses)
        # find our object
        orig_policy_index = ts_policy_index = None
        for policy_index in found_policy_indexes:
            object_ring = POLICIES.get_object_ring(policy_index, '/etc/swift')
            part, nodes = object_ring.get_nodes(
                self.account, self.container_name, self.object_name)
            for node in nodes:
                try:
                    direct_client.direct_head_object(
                        node, part, self.account, self.container_name,
                        self.object_name,
                        headers={'X-Backend-Storage-Policy-Index':
                                 policy_index})
                except direct_client.ClientException as err:
                    if 'x-backend-timestamp' in err.http_headers:
                        ts_policy_index = policy_index
                        break
                else:
                    orig_policy_index = policy_index
                    break
        if not orig_policy_index:
            self.fail('Unable to find /%s/%s/%s in %r' % (
                self.account, self.container_name, self.object_name,
                found_policy_indexes))
        if not ts_policy_index:
            self.fail('Unable to find tombstone /%s/%s/%s in %r' % (
                self.account, self.container_name, self.object_name,
                found_policy_indexes))
        self.get_to_final_state()
        Manager(['container-reconciler']).once()
        # validate containers
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        new_found_policy_indexes = \
            set(metadata['X-Backend-Storage-Policy-Index'] for node,
                metadata in head_responses)
        self.assertEqual(len(new_found_policy_indexes), 1,
                         'primary nodes disagree about policy index %r' %
                         dict((node['port'],
                               metadata['X-Backend-Storage-Policy-Index'])
                              for node, metadata in head_responses))
        expected_policy_index = new_found_policy_indexes.pop()
        self.assertEqual(orig_policy_index, expected_policy_index)
        # validate object fully deleted
        for policy_index in found_policy_indexes:
            object_ring = POLICIES.get_object_ring(policy_index, '/etc/swift')
            part, nodes = object_ring.get_nodes(
                self.account, self.container_name, self.object_name)
            for node in nodes:
                try:
                    direct_client.direct_head_object(
                        node, part, self.account, self.container_name,
                        self.object_name,
                        headers={'X-Backend-Storage-Policy-Index':
                                 policy_index})
                except direct_client.ClientException as err:
                    if err.http_status == HTTP_NOT_FOUND:
                        continue
                else:
                    self.fail('Found /%s/%s/%s in %s on %s' % (
                        self.account, self.container_name, self.object_name,
                        orig_policy_index, node))

    def test_reconcile_manifest(self):
        if 'slo' not in self.cluster_info:
            raise unittest.SkipTest(
                "SLO not enabled in proxy; can't test manifest reconciliation")
        # this test is not only testing a split brain scenario on
        # multiple policies with mis-placed objects - it even writes out
        # a static large object directly to the storage nodes while the
        # objects are unavailably mis-placed from *behind* the proxy and
        # doesn't know how to do that for EC_POLICY (clayg: why did you
        # guys let me write a test that does this!?) - so we force
        # wrong_policy (where the manifest gets written) to be one of
        # any of your configured REPL_POLICY (we know you have one
        # because this is a ReplProbeTest)
        wrong_policy = random.choice(POLICIES_BY_TYPE[REPL_POLICY])
        policy = random.choice([p for p in ENABLED_POLICIES
                                if p is not wrong_policy])
        manifest_data = []

        def write_part(i):
            body = 'VERIFY%0.2d' % i + '\x00' * 1048576
            part_name = 'manifest_part_%0.2d' % i
            manifest_entry = {
                "path": "/%s/%s" % (self.container_name, part_name),
                "etag": md5(body).hexdigest(),
                "size_bytes": len(body),
            }
            client.put_object(self.url, self.token, self.container_name,
                              part_name, contents=body)
            manifest_data.append(manifest_entry)

        # get an old container stashed
        self.brain.stop_primary_half()
        self.brain.put_container(int(policy))
        self.brain.start_primary_half()
        # write some parts
        for i in range(10):
            write_part(i)

        self.brain.stop_handoff_half()
        self.brain.put_container(int(wrong_policy))
        # write some more parts
        for i in range(10, 20):
            write_part(i)

        # write manifest
        with self.assertRaises(ClientException) as catcher:
            client.put_object(self.url, self.token, self.container_name,
                              self.object_name,
                              contents=utils.json.dumps(manifest_data),
                              query_string='multipart-manifest=put')

        # so as it works out, you can't really upload a multi-part
        # manifest for objects that are currently misplaced - you have to
        # wait until they're all available - which is about the same as
        # some other failure that causes data to be unavailable to the
        # proxy at the time of upload
        self.assertEqual(catcher.exception.http_status, 400)

        # but what the heck, we'll sneak one in just to see what happens...
        direct_manifest_name = self.object_name + '-direct-test'
        object_ring = POLICIES.get_object_ring(wrong_policy.idx, '/etc/swift')
        part, nodes = object_ring.get_nodes(
            self.account, self.container_name, direct_manifest_name)
        container_part = self.container_ring.get_part(self.account,
                                                      self.container_name)

        def translate_direct(data):
            return {
                'hash': data['etag'],
                'bytes': data['size_bytes'],
                'name': data['path'],
            }
        direct_manifest_data = map(translate_direct, manifest_data)
        headers = {
            'x-container-host': ','.join('%s:%s' % (n['ip'], n['port']) for n
                                         in self.container_ring.devs),
            'x-container-device': ','.join(n['device'] for n in
                                           self.container_ring.devs),
            'x-container-partition': container_part,
            'X-Backend-Storage-Policy-Index': wrong_policy.idx,
            'X-Static-Large-Object': 'True',
        }
        for node in nodes:
            direct_client.direct_put_object(
                node, part, self.account, self.container_name,
                direct_manifest_name,
                contents=utils.json.dumps(direct_manifest_data),
                headers=headers)
            break  # one should do it...

        self.brain.start_handoff_half()
        self.get_to_final_state()
        Manager(['container-reconciler']).once()
        # clear proxy cache
        client.post_container(self.url, self.token, self.container_name, {})

        # let's see how that direct upload worked out...
        metadata, body = client.get_object(
            self.url, self.token, self.container_name, direct_manifest_name,
            query_string='multipart-manifest=get')
        self.assertEqual(metadata['x-static-large-object'].lower(), 'true')
        for i, entry in enumerate(utils.json.loads(body)):
            for key in ('hash', 'bytes', 'name'):
                self.assertEqual(entry[key], direct_manifest_data[i][key])
        metadata, body = client.get_object(
            self.url, self.token, self.container_name, direct_manifest_name)
        self.assertEqual(metadata['x-static-large-object'].lower(), 'true')
        self.assertEqual(int(metadata['content-length']),
                         sum(part['size_bytes'] for part in manifest_data))
        self.assertEqual(body, ''.join('VERIFY%0.2d' % i + '\x00' * 1048576
                                       for i in range(20)))

        # and regular upload should work now too
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name,
                          contents=utils.json.dumps(manifest_data),
                          query_string='multipart-manifest=put')
        metadata = client.head_object(self.url, self.token,
                                      self.container_name,
                                      self.object_name)
        self.assertEqual(int(metadata['content-length']),
                         sum(part['size_bytes'] for part in manifest_data))

    def test_reconcile_symlink(self):
        if 'symlink' not in self.cluster_info:
            raise unittest.SkipTest(
                "Symlink not enabled in proxy; can't test "
                "symlink reconciliation")
        wrong_policy = random.choice(ENABLED_POLICIES)
        policy = random.choice([p for p in ENABLED_POLICIES
                                if p is not wrong_policy])
        # get an old container stashed
        self.brain.stop_primary_half()
        self.brain.put_container(int(policy))
        self.brain.start_primary_half()
        # write some target data
        client.put_object(self.url, self.token, self.container_name, 'target',
                          contents='this is the target data')

        # write the symlink
        self.brain.stop_handoff_half()
        self.brain.put_container(int(wrong_policy))
        client.put_object(
            self.url, self.token, self.container_name, 'symlink',
            headers={
                'X-Symlink-Target': '%s/target' % self.container_name,
                'Content-Type': 'application/symlink',
            })

        # at this point we have a broken symlink (the container_info has the
        # proxy looking for the target in the wrong policy)
        with self.assertRaises(ClientException) as ctx:
            client.get_object(self.url, self.token, self.container_name,
                              'symlink')
        self.assertEqual(ctx.exception.http_status, 404)

        # of course the symlink itself is fine
        metadata, body = client.get_object(self.url, self.token,
                                           self.container_name, 'symlink',
                                           query_string='symlink=get')
        self.assertEqual(metadata['x-symlink-target'],
                         '%s/target' % self.container_name)
        self.assertEqual(metadata['content-type'], 'application/symlink')
        self.assertEqual(body, '')
        # ... although in the wrong policy
        object_ring = POLICIES.get_object_ring(int(wrong_policy), '/etc/swift')
        part, nodes = object_ring.get_nodes(
            self.account, self.container_name, 'symlink')
        for node in nodes:
            metadata = direct_client.direct_head_object(
                node, part, self.account, self.container_name, 'symlink',
                headers={'X-Backend-Storage-Policy-Index': int(wrong_policy)})
            self.assertEqual(metadata['X-Object-Sysmeta-Symlink-Target'],
                             '%s/target' % self.container_name)

        # let the reconciler run
        self.brain.start_handoff_half()
        self.get_to_final_state()
        Manager(['container-reconciler']).once()
        # clear proxy cache
        client.post_container(self.url, self.token, self.container_name, {})

        # now the symlink works
        metadata, body = client.get_object(self.url, self.token,
                                           self.container_name, 'symlink')
        self.assertEqual(body, 'this is the target data')
        # and it's in the correct policy
        object_ring = POLICIES.get_object_ring(int(policy), '/etc/swift')
        part, nodes = object_ring.get_nodes(
            self.account, self.container_name, 'symlink')
        for node in nodes:
            metadata = direct_client.direct_head_object(
                node, part, self.account, self.container_name, 'symlink',
                headers={'X-Backend-Storage-Policy-Index': int(policy)})
            self.assertEqual(metadata['X-Object-Sysmeta-Symlink-Target'],
                             '%s/target' % self.container_name)

    def test_reconciler_move_object_twice(self):
        # select some policies
        old_policy = random.choice(ENABLED_POLICIES)
        new_policy = random.choice([p for p in ENABLED_POLICIES
                                    if p != old_policy])

        # setup a split brain
        self.brain.stop_handoff_half()
        # get old_policy on two primaries
        self.brain.put_container(policy_index=int(old_policy))
        self.brain.start_handoff_half()
        self.brain.stop_primary_half()
        # force a recreate on handoffs
        self.brain.put_container(policy_index=int(old_policy))
        self.brain.delete_container()
        self.brain.put_container(policy_index=int(new_policy))
        self.brain.put_object()  # populate memcache with new_policy
        self.brain.start_primary_half()

        # at this point two primaries have old policy
        container_part, container_nodes = self.container_ring.get_nodes(
            self.account, self.container_name)
        head_responses = [
            (node, direct_client.direct_head_container(
                node, container_part, self.account, self.container_name))
            for node in container_nodes]
        old_container_nodes = [
            node for node, metadata in head_responses
            if int(old_policy) ==
            int(metadata['X-Backend-Storage-Policy-Index'])]
        self.assertEqual(2, len(old_container_nodes))

        # hopefully memcache still has the new policy cached
        self.brain.put_object(headers={'x-object-meta-test': 'custom-meta'},
                              contents='VERIFY')
        # double-check object correctly written to new policy
        conf_files = []
        for server in Manager(['container-reconciler']).servers:
            conf_files.extend(server.conf_files())
        conf_file = conf_files[0]
        int_client = InternalClient(conf_file, 'probe-test', 3)
        int_client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            headers={'X-Backend-Storage-Policy-Index': int(new_policy)})
        int_client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            acceptable_statuses=(4,),
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})

        # shutdown the containers that know about the new policy
        self.brain.stop_handoff_half()

        # and get rows enqueued from old nodes
        for server_type in ('container-replicator', 'container-updater'):
            server = Manager([server_type])
            for node in old_container_nodes:
                server.once(number=self.config_number(node))

        # verify entry in the queue for the "misplaced" new_policy
        for container in int_client.iter_containers('.misplaced_objects'):
            for obj in int_client.iter_objects('.misplaced_objects',
                                               container['name']):
                expected = '%d:/%s/%s/%s' % (new_policy, self.account,
                                             self.container_name,
                                             self.object_name)
                self.assertEqual(obj['name'], expected)

        Manager(['container-reconciler']).once()

        # verify object in old_policy
        int_client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})

        # verify object is *not* in new_policy
        int_client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            acceptable_statuses=(4,),
            headers={'X-Backend-Storage-Policy-Index': int(new_policy)})

        self.get_to_final_state()

        # verify entry in the queue
        for container in int_client.iter_containers('.misplaced_objects'):
            for obj in int_client.iter_objects('.misplaced_objects',
                                               container['name']):
                expected = '%d:/%s/%s/%s' % (old_policy, self.account,
                                             self.container_name,
                                             self.object_name)
                self.assertEqual(obj['name'], expected)

        Manager(['container-reconciler']).once()

        # and now it flops back
        int_client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            headers={'X-Backend-Storage-Policy-Index': int(new_policy)})
        int_client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            acceptable_statuses=(4,),
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})

        # make sure the queue is settled
        self.get_to_final_state()
        for container in int_client.iter_containers('.misplaced_objects'):
            for obj in int_client.iter_objects('.misplaced_objects',
                                               container['name']):
                self.fail('Found unexpected object %r in the queue' % obj)

        # verify that the object data read by external client is correct
        headers, data = self._get_object_patiently(int(new_policy))
        self.assertEqual('VERIFY', data)
        self.assertEqual('custom-meta', headers['x-object-meta-test'])


if __name__ == "__main__":
    unittest.main()
