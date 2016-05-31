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

import uuid
import random
from nose import SkipTest
import unittest

from six.moves.urllib.parse import urlparse
from swiftclient import client, ClientException

from swift.common.http import HTTP_NOT_FOUND
from swift.common.manager import Manager
from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, ENABLED_POLICIES


def get_current_realm_cluster(url):
    parts = urlparse(url)
    url = parts.scheme + '://' + parts.netloc + '/info'
    http_conn = client.http_connection(url)
    try:
        info = client.get_capabilities(http_conn)
    except client.ClientException:
        raise SkipTest('Unable to retrieve cluster info')
    try:
        realms = info['container_sync']['realms']
    except KeyError:
        raise SkipTest('Unable to find container sync realms')
    for realm, realm_info in realms.items():
        for cluster, options in realm_info['clusters'].items():
            if options.get('current', False):
                return realm, cluster
    raise SkipTest('Unable find current realm cluster')


class TestContainerSync(ReplProbeTest):

    def setUp(self):
        super(TestContainerSync, self).setUp()
        self.realm, self.cluster = get_current_realm_cluster(self.url)

    def _setup_synced_containers(self, skey='secret', dkey='secret'):
        # setup dest container
        dest_container = 'dest-container-%s' % uuid.uuid4()
        dest_headers = {}
        dest_policy = None
        if len(ENABLED_POLICIES) > 1:
            dest_policy = random.choice(ENABLED_POLICIES)
            dest_headers['X-Storage-Policy'] = dest_policy.name
        if dkey is not None:
            dest_headers['X-Container-Sync-Key'] = dkey
        client.put_container(self.url, self.token, dest_container,
                             headers=dest_headers)

        # setup source container
        source_container = 'source-container-%s' % uuid.uuid4()
        source_headers = {}
        sync_to = '//%s/%s/%s/%s' % (self.realm, self.cluster, self.account,
                                     dest_container)
        source_headers['X-Container-Sync-To'] = sync_to
        if skey is not None:
            source_headers['X-Container-Sync-Key'] = skey
        if dest_policy:
            source_policy = random.choice([p for p in ENABLED_POLICIES
                                           if p is not dest_policy])
            source_headers['X-Storage-Policy'] = source_policy.name
        client.put_container(self.url, self.token, source_container,
                             headers=source_headers)

        return source_container, dest_container

    def _test_sync(self, object_post_as_copy):
        source_container, dest_container = self._setup_synced_containers()

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        put_headers = {'X-Object-Meta-Test': 'put_value'}
        client.put_object(self.url, self.token, source_container, object_name,
                          'test-body', headers=put_headers)

        # cycle container-sync
        Manager(['container-sync']).once()

        resp_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'test-body')
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('put_value', resp_headers['x-object-meta-test'])

        # update metadata with a POST, using an internal client so we can
        # vary the object_post_as_copy setting - first use post-as-copy
        post_headers = {'Content-Type': 'image/jpeg',
                        'X-Object-Meta-Test': 'post_value'}
        int_client = self.make_internal_client(
            object_post_as_copy=object_post_as_copy)
        int_client.set_object_metadata(self.account, source_container,
                                       object_name, post_headers)
        # sanity checks...
        resp_headers = client.head_object(
            self.url, self.token, source_container, object_name)
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('post_value', resp_headers['x-object-meta-test'])
        self.assertEqual('image/jpeg', resp_headers['content-type'])

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify that metadata changes were sync'd
        resp_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'test-body')
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('post_value', resp_headers['x-object-meta-test'])
        self.assertEqual('image/jpeg', resp_headers['content-type'])

        # delete the object
        client.delete_object(
            self.url, self.token, source_container, object_name)
        with self.assertRaises(ClientException) as cm:
            client.get_object(
                self.url, self.token, source_container, object_name)
        self.assertEqual(404, cm.exception.http_status)  # sanity check

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify delete has been sync'd
        with self.assertRaises(ClientException) as cm:
            client.get_object(
                self.url, self.token, dest_container, object_name)
        self.assertEqual(404, cm.exception.http_status)  # sanity check

    def test_sync_with_post_as_copy(self):
        self._test_sync(True)

    def test_sync_with_fast_post(self):
        self._test_sync(False)

    def test_sync_lazy_skey(self):
        # Create synced containers, but with no key at source
        source_container, dest_container =\
            self._setup_synced_containers(None, 'secret')

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        client.put_object(self.url, self.token, source_container, object_name,
                          'test-body')

        # cycle container-sync, nothing should happen
        Manager(['container-sync']).once()
        with self.assertRaises(ClientException) as err:
            _junk, body = client.get_object(self.url, self.token,
                                            dest_container, object_name)
        self.assertEqual(err.exception.http_status, HTTP_NOT_FOUND)

        # amend source key
        source_headers = {'X-Container-Sync-Key': 'secret'}
        client.put_container(self.url, self.token, source_container,
                             headers=source_headers)
        # cycle container-sync, should replicate
        Manager(['container-sync']).once()
        _junk, body = client.get_object(self.url, self.token,
                                        dest_container, object_name)
        self.assertEqual(body, 'test-body')

    def test_sync_lazy_dkey(self):
        # Create synced containers, but with no key at dest
        source_container, dest_container =\
            self._setup_synced_containers('secret', None)

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        client.put_object(self.url, self.token, source_container, object_name,
                          'test-body')

        # cycle container-sync, nothing should happen
        Manager(['container-sync']).once()
        with self.assertRaises(ClientException) as err:
            _junk, body = client.get_object(self.url, self.token,
                                            dest_container, object_name)
        self.assertEqual(err.exception.http_status, HTTP_NOT_FOUND)

        # amend dest key
        dest_headers = {'X-Container-Sync-Key': 'secret'}
        client.put_container(self.url, self.token, dest_container,
                             headers=dest_headers)
        # cycle container-sync, should replicate
        Manager(['container-sync']).once()
        _junk, body = client.get_object(self.url, self.token,
                                        dest_container, object_name)
        self.assertEqual(body, 'test-body')

    def test_sync_with_stale_container_rows(self):
        source_container, dest_container = self._setup_synced_containers()
        brain = BrainSplitter(self.url, self.token, source_container,
                              None, 'container')

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        client.put_object(self.url, self.token, source_container, object_name,
                          'test-body')

        # check source container listing
        _, listing = client.get_container(
            self.url, self.token, source_container)
        for expected_obj_dict in listing:
            if expected_obj_dict['name'] == object_name:
                break
        else:
            self.fail('Failed to find source object %r in container listing %r'
                      % (object_name, listing))

        # stop all container servers
        brain.stop_primary_half()
        brain.stop_handoff_half()

        # upload new object content to source - container updates will fail
        client.put_object(self.url, self.token, source_container, object_name,
                          'new-test-body')
        source_headers = client.head_object(
            self.url, self.token, source_container, object_name)

        # start all container servers
        brain.start_primary_half()
        brain.start_handoff_half()

        # sanity check: source container listing should not have changed
        _, listing = client.get_container(
            self.url, self.token, source_container)
        for actual_obj_dict in listing:
            if actual_obj_dict['name'] == object_name:
                self.assertDictEqual(expected_obj_dict, actual_obj_dict)
                break
        else:
            self.fail('Failed to find source object %r in container listing %r'
                      % (object_name, listing))

        # cycle container-sync - object should be correctly sync'd despite
        # stale info in container row
        Manager(['container-sync']).once()

        # verify sync'd object has same content and headers
        dest_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'new-test-body')
        mismatched_headers = []
        for k in ('etag', 'content-length', 'content-type', 'x-timestamp',
                  'last-modified'):
            if source_headers[k] == dest_headers[k]:
                continue
            mismatched_headers.append((k, source_headers[k], dest_headers[k]))
        if mismatched_headers:
            msg = '\n'.join([('Mismatched header %r, expected %r but got %r'
                              % item) for item in mismatched_headers])
            self.fail(msg)

    def test_sync_newer_remote(self):
        source_container, dest_container = self._setup_synced_containers()

        # upload to source
        object_name = 'object-%s' % uuid.uuid4()
        client.put_object(self.url, self.token, source_container, object_name,
                          'old-source-body')

        # upload to dest with same name
        client.put_object(self.url, self.token, dest_container, object_name,
                          'new-test-body')

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify that the remote object did not change
        resp_headers, body = client.get_object(self.url, self.token,
                                               dest_container, object_name)
        self.assertEqual(body, 'new-test-body')


if __name__ == "__main__":
    unittest.main()
