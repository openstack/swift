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
from tempfile import mkdtemp
from textwrap import dedent
import unittest

import os
import shutil
import uuid
from swift.common.exceptions import DiskFileDeleted

from swift.container.backend import ContainerBroker
from swift.common import internal_client, utils
from swift.common.ring import Ring
from swift.common.utils import Timestamp, get_logger
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
        self.tempdir = mkdtemp()
        conf_path = os.path.join(self.tempdir, 'internal_client.conf')
        conf_body = """
        [DEFAULT]
        swift_dir = /etc/swift

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy
        object_post_as_copy = false

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors
        """
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        self.int_client = internal_client.InternalClient(conf_path, 'test', 1)

    def tearDown(self):
        super(Test, self).tearDown()
        shutil.rmtree(self.tempdir)

    def _get_object_info(self, account, container, obj, number,
                         policy=None):
        obj_conf = self.configs['object-server']
        config_path = obj_conf[number]
        options = utils.readconf(config_path, 'app:object-server')
        swift_dir = options.get('swift_dir', '/etc/swift')
        ring = POLICIES.get_object_ring(policy, swift_dir)
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
                                     policy)
        info = disk_file.read_metadata()
        return info

    def _assert_consistent_object_metadata(self):
        obj_info = []
        for i in range(1, 5):
            info_i = self._get_object_info(self.account, self.container_name,
                                           self.object_name, i)
            if info_i:
                obj_info.append(info_i)
        self.assertTrue(len(obj_info) > 1)
        for other in obj_info[1:]:
            self.assertEqual(obj_info[0], other,
                             'Object metadata mismatch: %s != %s'
                             % (obj_info[0], other))

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
        self.assertTrue(len(db_info) > 1)
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

    def test_object_delete_is_replicated(self):
        self.brain.put_container(policy_index=int(self.policy))
        # put object
        self._put_object()

        # put newer object with sysmeta to first server subset
        self.brain.stop_primary_half()
        self._put_object()
        self.brain.start_primary_half()

        # delete object on second server subset
        self.brain.stop_handoff_half()
        self._delete_object()
        self.brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check object deletion has been replicated on first server set
        self.brain.stop_primary_half()
        self._get_object(expect_statuses=(4,))
        self.brain.start_primary_half()

        # check object deletion persists on second server set
        self.brain.stop_handoff_half()
        self._get_object(expect_statuses=(4,))

        # put newer object to second server set
        self._put_object()
        self.brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check new object  has been replicated on first server set
        self.brain.stop_primary_half()
        self._get_object()
        self.brain.start_primary_half()

        # check new object persists on second server set
        self.brain.stop_handoff_half()
        self._get_object()

    def test_object_after_replication_with_subsequent_post(self):
        self.brain.put_container(policy_index=0)

        # put object
        self._put_object(headers={'Content-Type': 'foo'}, body=u'older')

        # put newer object to first server subset
        self.brain.stop_primary_half()
        self._put_object(headers={'Content-Type': 'bar'}, body=u'newer')
        metadata = self._get_object_metadata()
        etag = metadata['etag']
        self.brain.start_primary_half()

        # post some user meta to all servers
        self._post_object({'x-object-meta-bar': 'meta-bar'})

        # run replicator
        self.get_to_final_state()

        # check that newer data has been replicated to second server subset
        self.brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        self.assertEqual(etag, metadata['etag'])
        self.assertEqual('bar', metadata['content-type'])
        self.assertEqual('meta-bar', metadata['x-object-meta-bar'])
        self.brain.start_handoff_half()

        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()

    def test_sysmeta_after_replication_with_subsequent_put(self):
        sysmeta = {'x-object-sysmeta-foo': 'older'}
        sysmeta2 = {'x-object-sysmeta-foo': 'newer'}
        usermeta = {'x-object-meta-bar': 'meta-bar'}
        self.brain.put_container(policy_index=0)

        # put object with sysmeta to first server subset
        self.brain.stop_primary_half()
        self._put_object(headers=sysmeta)
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        self.brain.start_primary_half()

        # put object with updated sysmeta to second server subset
        self.brain.stop_handoff_half()
        self._put_object(headers=sysmeta2)
        metadata = self._get_object_metadata()
        for key in sysmeta2:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], sysmeta2[key])
        self._post_object(usermeta)
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], usermeta[key])
        for key in sysmeta2:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], sysmeta2[key])

        self.brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check sysmeta has been replicated to first server subset
        self.brain.stop_primary_half()
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], usermeta[key])
        for key in sysmeta2.keys():
            self.assertTrue(key in metadata, key)
            self.assertEqual(metadata[key], sysmeta2[key])
        self.brain.start_primary_half()

        # check user sysmeta ok on second server subset
        self.brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], usermeta[key])
        for key in sysmeta2.keys():
            self.assertTrue(key in metadata, key)
            self.assertEqual(metadata[key], sysmeta2[key])

        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()

    def test_sysmeta_after_replication_with_subsequent_post(self):
        sysmeta = {'x-object-sysmeta-foo': 'sysmeta-foo'}
        usermeta = {'x-object-meta-bar': 'meta-bar'}
        self.brain.put_container(policy_index=int(self.policy))
        # put object
        self._put_object()
        # put newer object with sysmeta to first server subset
        self.brain.stop_primary_half()
        self._put_object(headers=sysmeta)
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        self.brain.start_primary_half()

        # post some user meta to second server subset
        self.brain.stop_handoff_half()
        self._post_object(usermeta)
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], usermeta[key])
        for key in sysmeta:
            self.assertFalse(key in metadata)
        self.brain.start_handoff_half()

        # run replicator
        self.get_to_final_state()

        # check user metadata has been replicated to first server subset
        # and sysmeta is unchanged
        self.brain.stop_primary_half()
        metadata = self._get_object_metadata()
        expected = dict(sysmeta)
        expected.update(usermeta)
        for key in expected.keys():
            self.assertTrue(key in metadata, key)
            self.assertEqual(metadata[key], expected[key])
        self.brain.start_primary_half()

        # check user metadata and sysmeta both on second server subset
        self.brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        for key in expected.keys():
            self.assertTrue(key in metadata, key)
            self.assertEqual(metadata[key], expected[key])
        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()

    def test_sysmeta_after_replication_with_prior_post(self):
        sysmeta = {'x-object-sysmeta-foo': 'sysmeta-foo'}
        usermeta = {'x-object-meta-bar': 'meta-bar'}
        self.brain.put_container(policy_index=int(self.policy))
        # put object
        self._put_object()

        # put user meta to first server subset
        self.brain.stop_handoff_half()
        self._post_object(headers=usermeta)
        metadata = self._get_object_metadata()
        for key in usermeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], usermeta[key])
        self.brain.start_handoff_half()

        # put newer object with sysmeta to second server subset
        self.brain.stop_primary_half()
        self._put_object(headers=sysmeta)
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        self.brain.start_primary_half()

        # run replicator
        self.get_to_final_state()

        # check stale user metadata is not replicated to first server subset
        # and sysmeta is unchanged
        self.brain.stop_primary_half()
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        for key in usermeta:
            self.assertFalse(key in metadata)
        self.brain.start_primary_half()

        # check stale user metadata is removed from second server subset
        # and sysmeta is replicated
        self.brain.stop_handoff_half()
        metadata = self._get_object_metadata()
        for key in sysmeta:
            self.assertTrue(key in metadata)
            self.assertEqual(metadata[key], sysmeta[key])
        for key in usermeta:
            self.assertFalse(key in metadata)
        self._assert_consistent_object_metadata()
        self._assert_consistent_container_dbs()


if __name__ == "__main__":
    unittest.main()
