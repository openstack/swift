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
import json
import uuid
import random
import unittest

from six.moves.urllib.parse import urlparse
from swiftclient import client, ClientException

from swift.common.http import HTTP_NOT_FOUND
from swift.common.manager import Manager
from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, ENABLED_POLICIES


def get_info(url):
    parts = urlparse(url)
    url = parts.scheme + '://' + parts.netloc + '/info'
    http_conn = client.http_connection(url)
    try:
        return client.get_capabilities(http_conn)
    except client.ClientException:
        raise unittest.SkipTest('Unable to retrieve cluster info')


def get_current_realm_cluster(info):
    try:
        realms = info['container_sync']['realms']
    except KeyError:
        raise unittest.SkipTest('Unable to find container sync realms')
    for realm, realm_info in realms.items():
        for cluster, options in realm_info['clusters'].items():
            if options.get('current', False):
                return realm, cluster
    raise unittest.SkipTest('Unable find current realm cluster')


class BaseTestContainerSync(ReplProbeTest):

    def setUp(self):
        super(BaseTestContainerSync, self).setUp()
        self.info = get_info(self.url)
        self.realm, self.cluster = get_current_realm_cluster(self.info)

    def _setup_synced_containers(
            self, source_overrides=None, dest_overrides=None):
        # these defaults are used to create both source and dest containers
        # unless overridden by source_overrides and/or dest_overrides
        default_params = {'url': self.url,
                          'token': self.token,
                          'account': self.account,
                          'sync_key': 'secret'}

        # setup dest container
        dest = dict(default_params)
        dest['name'] = 'dest-container-%s' % uuid.uuid4()
        dest.update(dest_overrides or {})
        dest_headers = {}
        dest_policy = None
        if len(ENABLED_POLICIES) > 1:
            dest_policy = random.choice(ENABLED_POLICIES)
            dest_headers['X-Storage-Policy'] = dest_policy.name
        if dest['sync_key'] is not None:
            dest_headers['X-Container-Sync-Key'] = dest['sync_key']
        client.put_container(dest['url'], dest['token'], dest['name'],
                             headers=dest_headers)

        # setup source container
        source = dict(default_params)
        source['name'] = 'source-container-%s' % uuid.uuid4()
        source.update(source_overrides or {})
        source_headers = {}
        sync_to = '//%s/%s/%s/%s' % (self.realm, self.cluster, dest['account'],
                                     dest['name'])
        source_headers['X-Container-Sync-To'] = sync_to
        if source['sync_key'] is not None:
            source_headers['X-Container-Sync-Key'] = source['sync_key']
        if dest_policy:
            source_policy = random.choice([p for p in ENABLED_POLICIES
                                           if p is not dest_policy])
            source_headers['X-Storage-Policy'] = source_policy.name
        client.put_container(source['url'], source['token'], source['name'],
                             headers=source_headers)

        return source['name'], dest['name']


class TestContainerSync(BaseTestContainerSync):

    def test_sync(self):
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

        # update metadata with a POST
        post_headers = {'Content-Type': 'image/jpeg',
                        'X-Object-Meta-Test': 'post_value'}
        int_client = self.make_internal_client()
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

    def test_sync_slo_manifest(self):
        # Verify that SLO manifests are sync'd even if their segments can not
        # be found in the destination account at time of sync'ing.
        # Create source and dest containers for manifest in separate accounts.
        dest_account = self.account_2
        source_container, dest_container = self._setup_synced_containers(
            dest_overrides=dest_account
        )

        # Create source and dest containers for segments in separate accounts.
        # These containers must have same name for the destination SLO manifest
        # to be able to resolve segments. Initially the destination has no sync
        # key so segments will not sync.
        segs_container = 'segments-%s' % uuid.uuid4()
        dest_segs_info = dict(dest_account)
        dest_segs_info.update({'name': segs_container, 'sync_key': None})
        self._setup_synced_containers(
            source_overrides={'name': segs_container, 'sync_key': 'segs_key'},
            dest_overrides=dest_segs_info)

        # upload a segment to source
        segment_name = 'segment-%s' % uuid.uuid4()
        segment_data = 'segment body'  # it's ok for first segment to be small
        segment_etag = client.put_object(
            self.url, self.token, segs_container, segment_name,
            segment_data)

        manifest = [{'etag': segment_etag,
                     'size_bytes': len(segment_data),
                     'path': '/%s/%s' % (segs_container, segment_name)}]
        manifest_name = 'manifest-%s' % uuid.uuid4()
        put_headers = {'X-Object-Meta-Test': 'put_value'}
        client.put_object(
            self.url, self.token, source_container, manifest_name,
            json.dumps(manifest), headers=put_headers,
            query_string='multipart-manifest=put')

        resp_headers, manifest_body = client.get_object(
            self.url, self.token, source_container, manifest_name,
            query_string='multipart-manifest=get')
        int_manifest = json.loads(manifest_body)

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify manifest was sync'd
        resp_headers, dest_listing = client.get_container(
            dest_account['url'], dest_account['token'], dest_container)
        self.assertFalse(dest_listing[1:])
        self.assertEqual(manifest_name, dest_listing[0]['name'])

        # verify manifest body
        resp_headers, body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            manifest_name, query_string='multipart-manifest=get')
        self.assertEqual(int_manifest, json.loads(body))
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('put_value', resp_headers['x-object-meta-test'])

        # attempt to GET the SLO will fail because the segment wasn't sync'd
        with self.assertRaises(ClientException) as cm:
            client.get_object(dest_account['url'], dest_account['token'],
                              dest_container, manifest_name)
        self.assertEqual(409, cm.exception.http_status)

        # now set sync key on destination segments container
        client.put_container(
            dest_account['url'], dest_account['token'], segs_container,
            headers={'X-Container-Sync-Key': 'segs_key'})

        # cycle container-sync
        Manager(['container-sync']).once()

        # sanity check - verify manifest body
        resp_headers, body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            manifest_name, query_string='multipart-manifest=get')
        self.assertEqual(int_manifest, json.loads(body))
        self.assertIn('x-object-meta-test', resp_headers)
        self.assertEqual('put_value', resp_headers['x-object-meta-test'])

        # verify GET of SLO manifest now succeeds
        resp_headers, body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            manifest_name)
        self.assertEqual(segment_data, body)

    def test_sync_lazy_skey(self):
        # Create synced containers, but with no key at source
        source_container, dest_container =\
            self._setup_synced_containers(source_overrides={'sync_key': None})

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
            self._setup_synced_containers(dest_overrides={'sync_key': None})

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


class TestContainerSyncAndSymlink(BaseTestContainerSync):

    def setUp(self):
        super(TestContainerSyncAndSymlink, self).setUp()
        symlinks_enabled = self.info.get('symlink') or False
        if not symlinks_enabled:
            raise unittest.SkipTest("Symlinks not enabled")

    def test_sync_symlink(self):
        # Verify that symlinks are sync'd as symlinks.
        dest_account = self.account_2
        source_container, dest_container = self._setup_synced_containers(
            dest_overrides=dest_account
        )

        # Create source and dest containers for target objects in separate
        # accounts.
        # These containers must have same name for the destination symlink
        # to use the same target object. Initially the destination has no sync
        # key so target will not sync.
        tgt_container = 'targets-%s' % uuid.uuid4()
        dest_tgt_info = dict(dest_account)
        dest_tgt_info.update({'name': tgt_container, 'sync_key': None})
        self._setup_synced_containers(
            source_overrides={'name': tgt_container, 'sync_key': 'tgt_key'},
            dest_overrides=dest_tgt_info)

        # upload a target to source
        target_name = 'target-%s' % uuid.uuid4()
        target_body = 'target body'
        client.put_object(
            self.url, self.token, tgt_container, target_name,
            target_body)

        # Note that this tests when the target object is in the same account
        target_path = '%s/%s' % (tgt_container, target_name)
        symlink_name = 'symlink-%s' % uuid.uuid4()
        put_headers = {'X-Symlink-Target': target_path}

        # upload the symlink
        client.put_object(
            self.url, self.token, source_container, symlink_name,
            '', headers=put_headers)

        # verify object is a symlink
        resp_headers, symlink_body = client.get_object(
            self.url, self.token, source_container, symlink_name,
            query_string='symlink=get')
        self.assertEqual('', symlink_body)
        self.assertIn('x-symlink-target', resp_headers)

        # verify symlink behavior
        resp_headers, actual_target_body = client.get_object(
            self.url, self.token, source_container, symlink_name)
        self.assertEqual(target_body, actual_target_body)

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify symlink was sync'd
        resp_headers, dest_listing = client.get_container(
            dest_account['url'], dest_account['token'], dest_container)
        self.assertFalse(dest_listing[1:])
        self.assertEqual(symlink_name, dest_listing[0]['name'])

        # verify symlink remained only a symlink
        resp_headers, symlink_body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            symlink_name, query_string='symlink=get')
        self.assertEqual('', symlink_body)
        self.assertIn('x-symlink-target', resp_headers)

        # attempt to GET the target object via symlink will fail because
        # the target wasn't sync'd
        with self.assertRaises(ClientException) as cm:
            client.get_object(dest_account['url'], dest_account['token'],
                              dest_container, symlink_name)
        self.assertEqual(404, cm.exception.http_status)

        # now set sync key on destination target container
        client.put_container(
            dest_account['url'], dest_account['token'], tgt_container,
            headers={'X-Container-Sync-Key': 'tgt_key'})

        # cycle container-sync
        Manager(['container-sync']).once()

        # sanity:
        resp_headers, body = client.get_object(
            dest_account['url'], dest_account['token'],
            tgt_container, target_name)

        # sanity check - verify symlink remained only a symlink
        resp_headers, symlink_body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            symlink_name, query_string='symlink=get')
        self.assertEqual('', symlink_body)
        self.assertIn('x-symlink-target', resp_headers)

        # verify GET of target object via symlink now succeeds
        resp_headers, actual_target_body = client.get_object(
            dest_account['url'], dest_account['token'], dest_container,
            symlink_name)
        self.assertEqual(target_body, actual_target_body)

    def test_sync_cross_acc_symlink(self):
        # Verify that cross-account symlinks are sync'd as cross-account
        # symlinks.
        source_container, dest_container = self._setup_synced_containers()

        # Sync'd symlinks will have the same target path "/a/c/o".
        # So if we want to execute probe test with syncing targets,
        # two swift clusters will be required.
        # Therefore, for probe test in single cluster, target object is not
        # sync'd in this test.
        tgt_account = self.account_2
        tgt_container = 'targets-%s' % uuid.uuid4()

        tgt_container_headers = {'X-Container-Read': 'test:tester'}
        if len(ENABLED_POLICIES) > 1:
            tgt_policy = random.choice(ENABLED_POLICIES)
            tgt_container_headers['X-Storage-Policy'] = tgt_policy.name
        client.put_container(tgt_account['url'], tgt_account['token'],
                             tgt_container, headers=tgt_container_headers)

        # upload a target to source
        target_name = 'target-%s' % uuid.uuid4()
        target_body = 'target body'
        client.put_object(tgt_account['url'], tgt_account['token'],
                          tgt_container, target_name, target_body)

        # Note that this tests when the target object is in a different account
        target_path = '%s/%s' % (tgt_container, target_name)
        symlink_name = 'symlink-%s' % uuid.uuid4()
        put_headers = {
            'X-Symlink-Target': target_path,
            'X-Symlink-Target-Account': tgt_account['account']}

        # upload the symlink
        client.put_object(
            self.url, self.token, source_container, symlink_name,
            '', headers=put_headers)

        # verify object is a cross-account symlink
        resp_headers, symlink_body = client.get_object(
            self.url, self.token, source_container, symlink_name,
            query_string='symlink=get')
        self.assertEqual('', symlink_body)
        self.assertIn('x-symlink-target', resp_headers)
        self.assertIn('x-symlink-target-account', resp_headers)

        # verify symlink behavior
        resp_headers, actual_target_body = client.get_object(
            self.url, self.token, source_container, symlink_name)
        self.assertEqual(target_body, actual_target_body)

        # cycle container-sync
        Manager(['container-sync']).once()

        # verify symlink was sync'd
        resp_headers, dest_listing = client.get_container(
            self.url, self.token, dest_container)
        self.assertFalse(dest_listing[1:])
        self.assertEqual(symlink_name, dest_listing[0]['name'])

        # verify symlink remained only a symlink
        resp_headers, symlink_body = client.get_object(
            self.url, self.token, dest_container,
            symlink_name, query_string='symlink=get')
        self.assertEqual('', symlink_body)
        self.assertIn('x-symlink-target', resp_headers)
        self.assertIn('x-symlink-target-account', resp_headers)

        # verify GET of target object via symlink now succeeds
        resp_headers, actual_target_body = client.get_object(
            self.url, self.token, dest_container, symlink_name)
        self.assertEqual(target_body, actual_target_body)


if __name__ == "__main__":
    unittest.main()
