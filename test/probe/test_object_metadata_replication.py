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
import functools
import unittest

import os
import shutil
import uuid

from swift.common import internal_client, utils

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest


def _sync_methods(object_server_config_paths):
    """
    Get the set of all configured sync_methods for the object-replicator
    sections in the list of config paths.
    """
    sync_methods = set()
    for config_path in object_server_config_paths:
        options = utils.readconf(config_path, 'object-replicator')
        sync_methods.add(options.get('sync_method', 'rsync'))
    return sync_methods


def expected_failure_with_ssync(m):
    """
    Wrapper for probetests that don't pass if you use ssync
    """
    @functools.wraps(m)
    def wrapper(self, *args, **kwargs):
        obj_conf = self.configs['object-server']
        config_paths = [v for k, v in obj_conf.items()
                        if k in self.brain.handoff_numbers]
        using_ssync = 'ssync' in _sync_methods(config_paths)
        failed = False
        try:
            return m(self, *args, **kwargs)
        except AssertionError:
            failed = True
            if not using_ssync:
                raise
        finally:
            if using_ssync and not failed:
                self.fail('This test is expected to fail with ssync')
    return wrapper


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

    def _put_object(self, headers=None):
        headers = headers or {}
        self.int_client.upload_object(StringIO(u'stuff'), self.account,
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

    @expected_failure_with_ssync
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


if __name__ == "__main__":
    unittest.main()
