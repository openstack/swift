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
from io import StringIO
import unittest

import os
import uuid

from swift.common.direct_client import direct_get_suffix_hashes
from swift.common.exceptions import DiskFileDeleted
from swift.common.internal_client import UnexpectedResponse
from swift.container.backend import ContainerBroker
from swift.common import utils
from swiftclient import client
from swift.common.ring import Ring
from swift.common.utils import Timestamp, get_logger, hash_path
from swift.obj.diskfile import DiskFileManager
from swift.common.storage_policy import POLICIES

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest


class Test(ReplProbeTest):
    def setUp(self):
        """
        Reset all environment and start all servers.
        """
        super(Test, self).setUp()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   self.object_name, 'object',
                                   policy=self.policy)
        self.container_brain = BrainSplitter(self.url, self.token,
                                             self.container_name)
        self.int_client = self.make_internal_client()

    def _get_object_info(self, account, container, obj, number):
        obj_conf = self.configs['object-server']
        config_path = obj_conf[number]
        options = utils.readconf(config_path, 'app:object-server')
        swift_dir = options.get('swift_dir', '/etc/swift')
        ring = POLICIES.get_object_ring(int(self.policy), swift_dir)
        part, nodes = ring.get_nodes(account, container, obj)
        for node in nodes:
            # assumes one to one mapping
            if node['port'] == int(options.get('bind_port')):
                device = node['device']
                break
        else:
            return None
        mgr = DiskFileManager(options, get_logger(options))
        disk_file = mgr.get_diskfile(device, part, account, container, obj,
                                     self.policy)
        info = disk_file.read_metadata()
        return info

    def _assert_consistent_object_metadata(self):
        obj_info = []
        for i in range(1, 5):
            info_i = self._get_object_info(self.account, self.container_name,
                                           self.object_name, i)
            if info_i:
                obj_info.append(info_i)
        self.assertGreater(len(obj_info), 1)
        for other in obj_info[1:]:
            self.assertDictEqual(obj_info[0], other)

    def _assert_consistent_deleted_object(self):
        for i in range(1, 5):
            try:
                info = self._get_object_info(self.account, self.container_name,
                                             self.object_name, i)
                if info is not None:
                    self.fail('Expected no disk file info but found %s' % info)
            except DiskFileDeleted:
                pass

    def _get_db_info(self, account, container, number):
        server_type = 'container'
        obj_conf = self.configs['%s-server' % server_type]
        config_path = obj_conf[number]
        options = utils.readconf(config_path, 'app:container-server')
        root = options.get('devices')

        swift_dir = options.get('swift_dir', '/etc/swift')
        ring = Ring(swift_dir, ring_name=server_type)
        part, nodes = ring.get_nodes(account, container)
        for node in nodes:
            # assumes one to one mapping
            if node['port'] == int(options.get('bind_port')):
                device = node['device']
                break
        else:
            return None

        path_hash = utils.hash_path(account, container)
        _dir = utils.storage_directory('%ss' % server_type, part, path_hash)
        db_dir = os.path.join(root, device, _dir)
        db_file = os.path.join(db_dir, '%s.db' % path_hash)
        db = ContainerBroker(db_file)
        return db.get_info()

    def _assert_consistent_container_dbs(self):
        db_info = []
        for i in range(1, 5):
            info_i = self._get_db_info(self.account, self.container_name, i)
            if info_i:
                db_info.append(info_i)
        self.assertGreater(len(db_info), 1)
        for other in db_info[1:]:
            self.assertEqual(db_info[0]['hash'], other['hash'],
                             'Container db hash mismatch: %s != %s'
                             % (db_info[0]['hash'], other['hash']))

    def _assert_object_metadata_matches_listing(self, listing, metadata):
        self.assertEqual(listing['bytes'], int(metadata['content-length']))
        self.assertEqual(listing['hash'], metadata['etag'])
        self.assertEqual(listing['content_type'], metadata['content-type'])
        modified = Timestamp(metadata['x-timestamp']).isoformat
        self.assertEqual(listing['last_modified'], modified)

    def _put_object(self, headers=None, body=u'stuff'):
        headers = headers or {}
        self.int_client.upload_object(StringIO(body), self.account,
                                      self.container_name,
                                      self.object_name, headers)

    def _post_object(self, headers):
        self.int_client.set_object_metadata(self.account, self.container_name,
                                            self.object_name, headers)

    def _delete_object(self):
        self.int_client.delete_object(self.account, self.container_name,
                                      self.object_name)

    def _get_object(self, headers=None, expect_statuses=(2,)):
        return self.int_client.get_object(self.account,
                                          self.container_name,
                                          self.object_name,
                                          headers,
                                          acceptable_statuses=expect_statuses)

    def _get_object_metadata(self):
        return self.int_client.get_object_metadata(self.account,
                                                   self.container_name,
                                                   self.object_name)

    def _assert_consistent_suffix_hashes(self):
        opart, onodes = self.object_ring.get_nodes(
            self.account, self.container_name, self.object_name)
        name_hash = hash_path(
            self.account, self.container_name, self.object_name)
        results = []
        for node in onodes:
            results.append(
                (node,
                 direct_get_suffix_hashes(node, opart, [name_hash[-3:]])))
        for (node, hashes) in results[1:]:
            self.assertEqual(results[0][1], hashes,
                             'Inconsistent suffix hashes found: %s' % results)

    def test_object_delete_is_replicated(self):
        self.brain.put_container()
        # put object
        self._put_object()

        # put newer object with sysmeta to first server subset
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object()
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # delete object on second server subset
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._delete_object()
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check object deletion has been replicated on first server set
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._get_object(expect_statuses=(4,))
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # check object deletion persists on second server set
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._get_object(expect_statuses=(4,))

        # put newer object to second server set
        self._put_object()
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check new object  has been replicated on first server set
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._get_object()
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # check new object persists on second server set
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._get_object()

    def test_object_after_replication_with_subsequent_post(self):
        self.brain.put_container()

        # put object
        self._put_object(headers={'Content-Type': 'foo'}, body=u'older')

        # put newer object to first server subset
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'bar'}, body=u'newer')
        metadata = self._get_object_metadata()
        etag = metadata['etag']
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # post some user meta to all servers
        self._post_object({'x-object-meta-bar': 'meta-bar'})

        # run replicator
        self.get_to_final_state()

        # check that newer data has been replicated to second server subset
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        self.assertEqual(etag, metadata['etag'])
        self.assertEqual('bar', metadata['content-type'])
        self.assertEqual('meta-bar', metadata['x-object-meta-bar'])
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()
        self._assert_consistent_suffix_hashes()

    def test_sysmeta_after_replication_with_subsequent_put(self):
        sysmeta = {'x-object-sysmeta-foo': 'older'}
        sysmeta2 = {'x-object-sysmeta-foo': 'newer'}
        usermeta = {'x-object-meta-bar': 'meta-bar'}
        self.brain.put_container()

        # put object with sysmeta to first server subset
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers=sysmeta)
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # put object with updated sysmeta to second server subset
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._put_object(headers=sysmeta2)
        metadata = self._get_object_metadata()
        for key in sysmeta2:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], sysmeta2[key])
        self._post_object(usermeta)
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], usermeta[key])
        for key in sysmeta2:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], sysmeta2[key])

        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check sysmeta has been replicated to first server subset
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], usermeta[key])
        for key in sysmeta2.keys():
            self.assertIn(key, metadata, key)
            self.assertEqual(metadata[key], sysmeta2[key])
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # check user sysmeta ok on second server subset
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], usermeta[key])
        for key in sysmeta2.keys():
            self.assertIn(key, metadata, key)
            self.assertEqual(metadata[key], sysmeta2[key])
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()
        self._assert_consistent_suffix_hashes()

    def test_sysmeta_after_replication_with_subsequent_post(self):
        sysmeta = {'x-object-sysmeta-foo': 'sysmeta-foo'}
        usermeta = {'x-object-meta-bar': 'meta-bar'}
        transient_sysmeta = {
            'x-object-transient-sysmeta-bar': 'transient-sysmeta-bar'}
        self.brain.put_container()
        # put object
        self._put_object()
        # put newer object with sysmeta to first server subset
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers=sysmeta)
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # post some user meta to second server subset
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        user_and_transient_sysmeta = dict(usermeta)
        user_and_transient_sysmeta.update(transient_sysmeta)
        self._post_object(user_and_transient_sysmeta)
        metadata = self._get_object_metadata()
        for key in user_and_transient_sysmeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], user_and_transient_sysmeta[key])
        for key in sysmeta:
            self.assertNotIn(key, metadata)
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check user metadata has been replicated to first server subset
        # and sysmeta is unchanged
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        metadata = self._get_object_metadata()
        expected = dict(sysmeta)
        expected.update(usermeta)
        expected.update(transient_sysmeta)
        for key in expected.keys():
            self.assertIn(key, metadata, key)
            self.assertEqual(metadata[key], expected[key])
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # check user metadata and sysmeta both on second server subset
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        for key in expected.keys():
            self.assertIn(key, metadata, key)
            self.assertEqual(metadata[key], expected[key])
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()
        self._assert_consistent_suffix_hashes()

    def test_sysmeta_after_replication_with_prior_post(self):
        sysmeta = {'x-object-sysmeta-foo': 'sysmeta-foo'}
        usermeta = {'x-object-meta-bar': 'meta-bar'}
        transient_sysmeta = {
            'x-object-transient-sysmeta-bar': 'transient-sysmeta-bar'}
        self.brain.put_container()
        # put object
        self._put_object()

        # put user meta to first server subset
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        user_and_transient_sysmeta = dict(usermeta)
        user_and_transient_sysmeta.update(transient_sysmeta)
        self._post_object(user_and_transient_sysmeta)
        metadata = self._get_object_metadata()
        for key in user_and_transient_sysmeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], user_and_transient_sysmeta[key])
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # put newer object with sysmeta to second server subset
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers=sysmeta)
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # run replicator
        self.get_to_final_state()

        # check stale user metadata is not replicated to first server subset
        # and sysmeta is unchanged
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        for key in user_and_transient_sysmeta:
            self.assertNotIn(key, metadata)
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # check stale user metadata is removed from second server subset
        # and sysmeta is replicated
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertIn(key, metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        for key in user_and_transient_sysmeta:
            self.assertNotIn(key, metadata)
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()
        self._assert_consistent_suffix_hashes()

    def test_post_ctype_replicated_when_previous_incomplete_puts(self):
        # primary half                     handoff half
        # ------------                     ------------
        # t0.data: ctype = foo
        #                                  t1.data: ctype = bar
        # t2.meta: ctype = baz
        #
        #              ...run replicator and expect...
        #
        #               t1.data:
        #               t2.meta: ctype = baz
        self.brain.put_container()

        # incomplete write to primary half
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._put_object(headers={'Content-Type': 'foo'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # handoff write
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'bar'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # content-type update to primary half
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._post_object(headers={'Content-Type': 'baz'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        self.get_to_final_state()

        # check object metadata
        metadata = client.head_object(self.url, self.token,
                                      self.container_name,
                                      self.object_name)

        # check container listing metadata
        container_metadata, objs = client.get_container(self.url, self.token,
                                                        self.container_name)

        for obj in objs:
            if obj['name'] == self.object_name:
                break
        expected = 'baz'
        self.assertEqual(obj['content_type'], expected)
        self._assert_object_metadata_matches_listing(obj, metadata)
        self._assert_consistent_container_dbs()
        self._assert_consistent_object_metadata()
        self._assert_consistent_suffix_hashes()

    def test_put_ctype_replicated_when_subsequent_post(self):
        # primary half                     handoff half
        # ------------                     ------------
        # t0.data: ctype = foo
        #                                  t1.data: ctype = bar
        # t2.meta:
        #
        #              ...run replicator and expect...
        #
        #               t1.data: ctype = bar
        #               t2.meta:
        self.brain.put_container()

        # incomplete write
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._put_object(headers={'Content-Type': 'foo'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # handoff write
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'bar'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # metadata update with newest data unavailable
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._post_object(headers={'X-Object-Meta-Color': 'Blue'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        self.get_to_final_state()

        # check object metadata
        metadata = client.head_object(self.url, self.token,
                                      self.container_name,
                                      self.object_name)

        # check container listing metadata
        container_metadata, objs = client.get_container(self.url, self.token,
                                                        self.container_name)

        for obj in objs:
            if obj['name'] == self.object_name:
                break
        else:
            self.fail('obj not found in container listing')
        expected = 'bar'
        self.assertEqual(obj['content_type'], expected)
        self.assertEqual(metadata['x-object-meta-color'], 'Blue')
        self._assert_object_metadata_matches_listing(obj, metadata)
        self._assert_consistent_container_dbs()
        self._assert_consistent_object_metadata()
        self._assert_consistent_suffix_hashes()

    def test_post_ctype_replicated_when_subsequent_post_without_ctype(self):
        # primary half                     handoff half
        # ------------                     ------------
        # t0.data: ctype = foo
        #                                  t1.data: ctype = bar
        # t2.meta: ctype = bif
        #                                  t3.data: ctype = baz, color = 'Red'
        #               t4.meta: color = Blue
        #
        #              ...run replicator and expect...
        #
        #               t1.data:
        #               t4-delta.meta: ctype = baz, color = Blue
        self.brain.put_container()

        # incomplete write
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._put_object(headers={'Content-Type': 'foo',
                                  'X-Object-Sysmeta-Test': 'older'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # handoff write
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'bar',
                                  'X-Object-Sysmeta-Test': 'newer'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # incomplete post with content type
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._post_object(headers={'Content-Type': 'bif'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # incomplete post to handoff with content type
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._post_object(headers={'Content-Type': 'baz',
                                   'X-Object-Meta-Color': 'Red'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # complete post with no content type
        self._post_object(headers={'X-Object-Meta-Color': 'Blue',
                                   'X-Object-Sysmeta-Test': 'ignored'})

        # 'baz' wins over 'bar' but 'Blue' wins over 'Red'
        self.get_to_final_state()

        # check object metadata
        metadata = self._get_object_metadata()

        # check container listing metadata
        container_metadata, objs = client.get_container(self.url, self.token,
                                                        self.container_name)

        for obj in objs:
            if obj['name'] == self.object_name:
                break
        expected = 'baz'
        self.assertEqual(obj['content_type'], expected)
        self.assertEqual(metadata['x-object-meta-color'], 'Blue')
        self.assertEqual(metadata['x-object-sysmeta-test'], 'newer')
        self._assert_object_metadata_matches_listing(obj, metadata)
        self._assert_consistent_container_dbs()
        self._assert_consistent_object_metadata()
        self._assert_consistent_suffix_hashes()

    def test_put_ctype_replicated_when_subsequent_posts_without_ctype(self):
        # primary half                     handoff half
        # ------------                     ------------
        #               t0.data: ctype = foo
        #                                  t1.data: ctype = bar
        # t2.meta:
        #                                  t3.meta
        #
        #              ...run replicator and expect...
        #
        #               t1.data: ctype = bar
        #               t3.meta
        self.brain.put_container()

        self._put_object(headers={'Content-Type': 'foo',
                                  'X-Object-Sysmeta-Test': 'older'})

        # incomplete write to handoff half
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'bar',
                                  'X-Object-Sysmeta-Test': 'newer'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # incomplete post with no content type to primary half
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._post_object(headers={'X-Object-Meta-Color': 'Red',
                                   'X-Object-Sysmeta-Test': 'ignored'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # incomplete post with no content type to handoff half
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._post_object(headers={'X-Object-Meta-Color': 'Blue'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        self.get_to_final_state()

        # check object metadata
        metadata = self._get_object_metadata()

        # check container listing metadata
        container_metadata, objs = client.get_container(self.url, self.token,
                                                        self.container_name)

        for obj in objs:
            if obj['name'] == self.object_name:
                break
        expected = 'bar'
        self.assertEqual(obj['content_type'], expected)
        self._assert_object_metadata_matches_listing(obj, metadata)
        self.assertEqual(metadata['x-object-meta-color'], 'Blue')
        self.assertEqual(metadata['x-object-sysmeta-test'], 'newer')
        self._assert_object_metadata_matches_listing(obj, metadata)
        self._assert_consistent_container_dbs()
        self._assert_consistent_object_metadata()
        self._assert_consistent_suffix_hashes()

    def test_posted_metadata_only_persists_after_prior_put(self):
        # newer metadata posted to subset of nodes should persist after an
        # earlier put on other nodes, but older content-type on that subset
        # should not persist
        self.brain.put_container()
        # incomplete put to handoff
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'oldest',
                                  'X-Object-Sysmeta-Test': 'oldest',
                                  'X-Object-Meta-Test': 'oldest'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()
        # incomplete put to primary
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._put_object(headers={'Content-Type': 'oldest',
                                  'X-Object-Sysmeta-Test': 'oldest',
                                  'X-Object-Meta-Test': 'oldest'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # incomplete post with content-type to handoff
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._post_object(headers={'Content-Type': 'newer',
                                   'X-Object-Meta-Test': 'newer'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # incomplete put to primary
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._put_object(headers={'Content-Type': 'newest',
                                  'X-Object-Sysmeta-Test': 'newest',
                                  'X-Object-Meta-Test': 'newer'})
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # incomplete post with no content-type to handoff which still has
        # out of date content-type
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._post_object(headers={'X-Object-Meta-Test': 'newest'})
        metadata = self._get_object_metadata()
        self.assertEqual(metadata['x-object-meta-test'], 'newest')
        self.assertEqual(metadata['content-type'], 'newer')
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        self.get_to_final_state()

        # check object metadata
        metadata = self._get_object_metadata()
        self.assertEqual(metadata['x-object-meta-test'], 'newest')
        self.assertEqual(metadata['x-object-sysmeta-test'], 'newest')
        self.assertEqual(metadata['content-type'], 'newest')

        # check container listing metadata
        container_metadata, objs = client.get_container(self.url, self.token,
                                                        self.container_name)

        for obj in objs:
            if obj['name'] == self.object_name:
                break
        self.assertEqual(obj['content_type'], 'newest')
        self._assert_object_metadata_matches_listing(obj, metadata)
        self._assert_object_metadata_matches_listing(obj, metadata)
        self._assert_consistent_container_dbs()
        self._assert_consistent_object_metadata()
        self._assert_consistent_suffix_hashes()

    def test_post_trumped_by_prior_delete(self):
        # new metadata and content-type posted to subset of nodes should not
        # cause object to persist after replication of an earlier delete on
        # other nodes.
        self.brain.put_container()
        # incomplete put
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'oldest',
                                  'X-Object-Sysmeta-Test': 'oldest',
                                  'X-Object-Meta-Test': 'oldest'})
        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # incomplete put then delete
        self.brain.stop_handoff_half()
        self.container_brain.stop_handoff_half()
        self._put_object(headers={'Content-Type': 'oldest',
                                  'X-Object-Sysmeta-Test': 'oldest',
                                  'X-Object-Meta-Test': 'oldest'})
        self._delete_object()
        self.brain.start_handoff_half()
        self.container_brain.start_handoff_half()

        # handoff post
        self.brain.stop_primary_half()
        self.container_brain.stop_primary_half()
        self._post_object(headers={'Content-Type': 'newest',
                                   'X-Object-Sysmeta-Test': 'ignored',
                                   'X-Object-Meta-Test': 'newest'})

        # check object metadata
        metadata = self._get_object_metadata()
        self.assertEqual(metadata['x-object-sysmeta-test'], 'oldest')
        self.assertEqual(metadata['x-object-meta-test'], 'newest')
        self.assertEqual(metadata['content-type'], 'newest')

        self.brain.start_primary_half()
        self.container_brain.start_primary_half()

        # delete trumps later post
        self.get_to_final_state()

        # check object is now deleted
        self.assertRaises(UnexpectedResponse, self._get_object_metadata)
        container_metadata, objs = client.get_container(self.url, self.token,
                                                        self.container_name)
        self.assertEqual(0, len(objs))
        self._assert_consistent_container_dbs()
        self._assert_consistent_deleted_object()
        self._assert_consistent_suffix_hashes()


if __name__ == "__main__":
    unittest.main()
