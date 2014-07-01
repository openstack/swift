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
import sys
import itertools
import time
import unittest
import uuid
from optparse import OptionParser
from urlparse import urlparse
import random

from nose import SkipTest

from swift.common.manager import Manager
from swift.common.internal_client import InternalClient
from swift.common import utils, direct_client, ring
from swift.common.storage_policy import POLICIES, POLICY_INDEX
from swift.common.http import HTTP_NOT_FOUND
from test.probe.common import reset_environment, get_to_final_state

from swiftclient import client, get_auth, ClientException

TIMEOUT = 60


def meta_command(name, bases, attrs):
    """
    Look for attrs with a truthy attribute __command__ and add them to an
    attribute __commands__ on the type that maps names to decorated methods.
    The decorated methods' doc strings also get mapped in __docs__.

    Also adds a method run(command_name, *args, **kwargs) that will
    execute the method mapped to the name in __commands__.
    """
    commands = {}
    docs = {}
    for attr, value in attrs.items():
        if getattr(value, '__command__', False):
            commands[attr] = value
            # methods have always have a __doc__ attribute, sometimes empty
            docs[attr] = (getattr(value, '__doc__', None) or
                          'perform the %s command' % attr).strip()
    attrs['__commands__'] = commands
    attrs['__docs__'] = docs

    def run(self, command, *args, **kwargs):
        return self.__commands__[command](self, *args, **kwargs)
    attrs.setdefault('run', run)
    return type(name, bases, attrs)


def command(f):
    f.__command__ = True
    return f


class BrainSplitter(object):

    __metaclass__ = meta_command

    def __init__(self, url, token, container_name='test', object_name='test'):
        self.url = url
        self.token = token
        self.account = utils.split_path(urlparse(url).path, 2, 2)[1]
        self.container_name = container_name
        self.object_name = object_name
        self.servers = Manager(['container-server'])
        policies = list(POLICIES)
        random.shuffle(policies)
        self.policies = itertools.cycle(policies)

        container_part, container_nodes = ring.Ring(
            '/etc/swift/container.ring.gz').get_nodes(
                self.account, self.container_name)
        container_node_ids = [n['id'] for n in container_nodes]
        if all(n_id in container_node_ids for n_id in (0, 1)):
            self.primary_numbers = (1, 2)
            self.handoff_numbers = (3, 4)
        else:
            self.primary_numbers = (3, 4)
            self.handoff_numbers = (1, 2)

    @command
    def start_primary_half(self):
        """
        start container servers 1 & 2
        """
        tuple(self.servers.start(number=n) for n in self.primary_numbers)

    @command
    def stop_primary_half(self):
        """
        stop container servers 1 & 2
        """
        tuple(self.servers.stop(number=n) for n in self.primary_numbers)

    @command
    def start_handoff_half(self):
        """
        start container servers 3 & 4
        """
        tuple(self.servers.start(number=n) for n in self.handoff_numbers)

    @command
    def stop_handoff_half(self):
        """
        stop container servers 3 & 4
        """
        tuple(self.servers.stop(number=n) for n in self.handoff_numbers)

    @command
    def put_container(self, policy_index=None):
        """
        put container with next storage policy
        """
        policy = self.policies.next()
        if policy_index is not None:
            policy = POLICIES.get_by_index(int(policy_index))
            if not policy:
                raise ValueError('Unknown policy with index %s' % policy)
        headers = {'X-Storage-Policy': policy.name}
        client.put_container(self.url, self.token, self.container_name,
                             headers=headers)

    @command
    def delete_container(self):
        """
        delete container
        """
        client.delete_container(self.url, self.token, self.container_name)

    @command
    def put_object(self, headers=None):
        """
        issue put for zero byte test object
        """
        client.put_object(self.url, self.token, self.container_name,
                          self.object_name, headers=headers)

    @command
    def delete_object(self):
        """
        issue delete for test object
        """
        try:
            client.delete_object(self.url, self.token, self.container_name,
                                 self.object_name)
        except ClientException as err:
            if err.http_status != HTTP_NOT_FOUND:
                raise

parser = OptionParser('%prog split-brain [options] '
                      '<command>[:<args>[,<args>...]] [<command>...]')
parser.usage += '\n\nCommands:\n\t' + \
    '\n\t'.join("%s - %s" % (name, doc) for name, doc in
                BrainSplitter.__docs__.items())
parser.add_option('-c', '--container', default='container-%s' % uuid.uuid4(),
                  help='set container name')
parser.add_option('-o', '--object', default='object-%s' % uuid.uuid4(),
                  help='set object name')


class TestContainerMergePolicyIndex(unittest.TestCase):

    def setUp(self):
        if len(POLICIES) < 2:
            raise SkipTest()
        (self.pids, self.port2server, self.account_ring, self.container_ring,
         self.object_ring, self.policy, self.url, self.token,
         self.account, self.configs) = reset_environment()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   self.object_name)

    def test_merge_storage_policy_index(self):
        # generic split brain
        self.brain.stop_primary_half()
        self.brain.put_container()
        self.brain.start_primary_half()
        self.brain.stop_handoff_half()
        self.brain.put_container()
        self.brain.put_object()
        self.brain.start_handoff_half()
        # make sure we have some manner of split brain
        container_part, container_nodes = self.container_ring.get_nodes(
            self.account, self.container_name)
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        found_policy_indexes = set(metadata[POLICY_INDEX] for
                                   node, metadata in head_responses)
        self.assert_(len(found_policy_indexes) > 1,
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
                        self.object_name, headers={POLICY_INDEX: policy_index})
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
        get_to_final_state()
        Manager(['container-reconciler']).once()
        # validate containers
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        found_policy_indexes = set(metadata[POLICY_INDEX] for
                                   node, metadata in head_responses)
        self.assert_(len(found_policy_indexes) == 1,
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
                        POLICY_INDEX: orig_policy_index})
            except direct_client.ClientException as err:
                if err.http_status == HTTP_NOT_FOUND:
                    continue
                raise
            else:
                self.fail('Found /%s/%s/%s in %s' % (
                    self.account, self.container_name, self.object_name,
                    orig_policy_index))
        # use proxy to access object (bad container info might be cached...)
        timeout = time.time() + TIMEOUT
        while time.time() < timeout:
            try:
                metadata = client.head_object(self.url, self.token,
                                              self.container_name,
                                              self.object_name)
            except ClientException as err:
                if err.http_status != HTTP_NOT_FOUND:
                    raise
                time.sleep(1)
            else:
                break
        else:
            self.fail('could not HEAD /%s/%s/%s/ from policy %s '
                      'after %s seconds.' % (
                          self.account, self.container_name, self.object_name,
                          expected_policy_index, TIMEOUT))

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
        found_policy_indexes = set(metadata[POLICY_INDEX] for
                                   node, metadata in head_responses)
        self.assert_(len(found_policy_indexes) > 1,
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
                        self.object_name, headers={POLICY_INDEX: policy_index})
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
        get_to_final_state()
        Manager(['container-reconciler']).once()
        # validate containers
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        new_found_policy_indexes = set(metadata[POLICY_INDEX] for node,
                                       metadata in head_responses)
        self.assert_(len(new_found_policy_indexes) == 1,
                     'primary nodes disagree about policy index %r' %
                     dict((node['port'], metadata[POLICY_INDEX])
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
                        self.object_name, headers={POLICY_INDEX: policy_index})
                except direct_client.ClientException as err:
                    if err.http_status == HTTP_NOT_FOUND:
                        continue
                else:
                    self.fail('Found /%s/%s/%s in %s on %s' % (
                        self.account, self.container_name, self.object_name,
                        orig_policy_index, node))

    def test_reconcile_manifest(self):
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
        policy = random.choice(list(POLICIES))
        self.brain.put_container(policy.idx)
        self.brain.start_primary_half()
        # write some parts
        for i in range(10):
            write_part(i)

        self.brain.stop_handoff_half()
        wrong_policy = random.choice([p for p in POLICIES if p is not policy])
        self.brain.put_container(wrong_policy.idx)
        # write some more parts
        for i in range(10, 20):
            write_part(i)

        # write manifest
        try:
            client.put_object(self.url, self.token, self.container_name,
                              self.object_name,
                              contents=utils.json.dumps(manifest_data),
                              query_string='multipart-manifest=put')
        except ClientException as err:
            # so as it works out, you can't really upload a multi-part
            # manifest for objects that are currently misplaced - you have to
            # wait until they're all available - which is about the same as
            # some other failure that causes data to be unavailable to the
            # proxy at the time of upload
            self.assertEqual(err.http_status, 400)

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
            POLICY_INDEX: wrong_policy.idx,
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
        get_to_final_state()
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
                self.assertEquals(entry[key], direct_manifest_data[i][key])
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

    def test_reconciler_move_object_twice(self):
        # select some policies
        old_policy = random.choice(list(POLICIES))
        new_policy = random.choice([p for p in POLICIES if p != old_policy])

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
        head_responses = []
        for node in container_nodes:
            metadata = direct_client.direct_head_container(
                node, container_part, self.account, self.container_name)
            head_responses.append((node, metadata))
        old_container_node_ids = [
            node['id'] for node, metadata in head_responses
            if int(old_policy) ==
            int(metadata['X-Backend-Storage-Policy-Index'])]
        self.assertEqual(2, len(old_container_node_ids))

        # hopefully memcache still has the new policy cached
        self.brain.put_object()
        # double-check object correctly written to new policy
        conf_files = []
        for server in Manager(['container-reconciler']).servers:
            conf_files.extend(server.conf_files())
        conf_file = conf_files[0]
        client = InternalClient(conf_file, 'probe-test', 3)
        client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            headers={'X-Backend-Storage-Policy-Index': int(new_policy)})
        client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            acceptable_statuses=(4,),
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})

        # shutdown the containers that know about the new policy
        self.brain.stop_handoff_half()

        # and get rows enqueued from old nodes
        for server_type in ('container-replicator', 'container-updater'):
            server = Manager([server_type])
            tuple(server.once(number=n + 1) for n in old_container_node_ids)

        # verify entry in the queue for the "misplaced" new_policy
        for container in client.iter_containers('.misplaced_objects'):
            for obj in client.iter_objects('.misplaced_objects',
                                           container['name']):
                expected = '%d:/%s/%s/%s' % (new_policy, self.account,
                                             self.container_name,
                                             self.object_name)
                self.assertEqual(obj['name'], expected)

        Manager(['container-reconciler']).once()

        # verify object in old_policy
        client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})

        # verify object is *not* in new_policy
        client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            acceptable_statuses=(4,),
            headers={'X-Backend-Storage-Policy-Index': int(new_policy)})

        get_to_final_state()

        # verify entry in the queue
        client = InternalClient(conf_file, 'probe-test', 3)
        for container in client.iter_containers('.misplaced_objects'):
            for obj in client.iter_objects('.misplaced_objects',
                                           container['name']):
                expected = '%d:/%s/%s/%s' % (old_policy, self.account,
                                             self.container_name,
                                             self.object_name)
                self.assertEqual(obj['name'], expected)

        Manager(['container-reconciler']).once()

        # and now it flops back
        client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            headers={'X-Backend-Storage-Policy-Index': int(new_policy)})
        client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            acceptable_statuses=(4,),
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})

        # make sure the queue is settled
        get_to_final_state()
        for container in client.iter_containers('.misplaced_objects'):
            for obj in client.iter_objects('.misplaced_objects',
                                           container['name']):
                self.fail('Found unexpected object %r in the queue' % obj)


def main():
    options, commands = parser.parse_args()
    commands.remove('split-brain')
    if not commands:
        parser.print_help()
        return 'ERROR: must specify at least one command'
    for cmd_args in commands:
        cmd = cmd_args.split(':', 1)[0]
        if cmd not in BrainSplitter.__commands__:
            parser.print_help()
            return 'ERROR: unknown command %s' % cmd
    url, token = get_auth('http://127.0.0.1:8080/auth/v1.0',
                          'test:tester', 'testing')
    brain = BrainSplitter(url, token, options.container, options.object)
    for cmd_args in commands:
        parts = cmd_args.split(':', 1)
        command = parts[0]
        if len(parts) > 1:
            args = utils.list_from_csv(parts[1])
        else:
            args = ()
        try:
            brain.run(command, *args)
        except ClientException as e:
            print '**WARNING**: %s raised %s' % (command, e)
    print 'STATUS'.join(['*' * 25] * 2)
    brain.servers.status()
    sys.exit()


if __name__ == "__main__":
    if any('split-brain' in arg for arg in sys.argv):
        sys.exit(main())
    unittest.main()
