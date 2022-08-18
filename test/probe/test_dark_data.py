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

import collections
import unittest

import os
import uuid
import shutil

from datetime import datetime
from configparser import ConfigParser

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest
from swift.common import manager
from swift.common.storage_policy import get_policy_string
from swift.common.manager import Manager, Server
from swift.common.utils import readconf


CONF_SECTION = 'object-auditor:watcher:swift#dark_data'


class TestDarkDataDeletion(ReplProbeTest):
    # NB: could be 'quarantine' in another test
    action = 'delete'

    def setUp(self):
        """
        Reset all environment and start all servers.
        """
        super(TestDarkDataDeletion, self).setUp()

        self.conf_dest = \
            os.path.join('/tmp/',
                         datetime.now().strftime('swift-%Y-%m-%d_%H-%M-%S-%f'))
        os.mkdir(self.conf_dest)

        object_server_dir = os.path.join(self.conf_dest, 'object-server')
        os.mkdir(object_server_dir)

        for conf_file in Server('object-auditor').conf_files():
            config = readconf(conf_file)
            if 'object-auditor' not in config:
                continue  # *somebody* should be set up to run the auditor
            config['object-auditor'].update(
                {'watchers': 'swift#dark_data'})
            # Note that this setdefault business may mean the watcher doesn't
            # pick up DEFAULT values, but that (probably?) won't matter.
            # We set grace_age to 0 so that tests don't have to deal with time.
            config.setdefault(CONF_SECTION, {}).update(
                {'action': self.action,
                 'grace_age': "0"})

            parser = ConfigParser()
            for section in ('object-auditor', CONF_SECTION):
                parser.add_section(section)
                for option, value in config[section].items():
                    parser.set(section, option, value)

            file_name = os.path.basename(conf_file)
            if file_name.endswith('.d'):
                # Work around conf.d setups (like you might see with VSAIO)
                file_name = file_name[:-2]
            with open(os.path.join(object_server_dir, file_name), 'w') as fp:
                parser.write(fp)

        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   self.object_name, 'object',
                                   policy=self.policy)

    def tearDown(self):
        shutil.rmtree(self.conf_dest)

    def gather_object_files_by_ext(self):
        result = collections.defaultdict(set)
        for node in self.brain.nodes:
            for path, _, files in os.walk(os.path.join(
                    self.device_dir(node),
                    get_policy_string('objects', self.policy))):
                for file in files:
                    if file in ('.lock', 'hashes.pkl', 'hashes.invalid',
                                '.lock-replication'):
                        continue
                    _, ext = os.path.splitext(file)
                    result[ext].add(os.path.join(path, file))
        return result

    def test_dark_data(self):
        self.brain.put_container()
        self.brain.put_object()
        self.brain.stop_handoff_half()
        self.brain.delete_object()
        Manager(['object-updater']).once()
        Manager(['container-replicator']).once()

        # Sanity check:
        # * all containers are empty
        # * primaries that are still up have two .ts files
        # * primary that's down has one .data file
        for index, (headers, items) in self.direct_get_container(
                container=self.container_name).items():
            self.assertEqual(headers['X-Container-Object-Count'], '0')
            self.assertEqual(items, [])

        files = self.gather_object_files_by_ext()
        self.assertLengthEqual(files, 2)
        self.assertLengthEqual(files['.ts'], 2)
        self.assertLengthEqual(files['.data'], 1)

        # Simulate a reclaim_age passing,
        # so the tombstones all got cleaned up
        for file_path in files['.ts']:
            os.unlink(file_path)

        # Old node gets reintroduced to the cluster
        self.brain.start_handoff_half()
        # ...so replication thinks its got some work to do
        Manager(['object-replicator']).once()

        # Now we're back to *three* .data files
        files = self.gather_object_files_by_ext()
        self.assertLengthEqual(files, 1)
        self.assertLengthEqual(files['.data'], 3)

        # But that's OK, audit watchers to the rescue!
        old_swift_dir = manager.SWIFT_DIR
        manager.SWIFT_DIR = self.conf_dest
        try:
            Manager(['object-auditor']).once()
        finally:
            manager.SWIFT_DIR = old_swift_dir

        # Verify that the policy was applied.
        self.check_on_disk_files(files['.data'])

    def check_on_disk_files(self, files):
        for file_path in files:
            # File's not there
            self.assertFalse(os.path.exists(file_path))
            # And it's not quaratined, either!
            self.assertPathDoesNotExist(os.path.join(
                file_path[:file_path.index('objects')], 'quarantined'))

    def assertPathExists(self, path):
        msg = "Expected path %r to exist, but it doesn't" % path
        self.assertTrue(os.path.exists(path), msg)

    def assertPathDoesNotExist(self, path):
        msg = "Expected path %r to not exist, but it does" % path
        self.assertFalse(os.path.exists(path), msg)


class TestDarkDataQuarantining(TestDarkDataDeletion):
    action = 'quarantine'

    def check_on_disk_files(self, files):
        for file_path in files:
            # File's not there
            self.assertPathDoesNotExist(file_path)
            # Got quarantined
            parts = file_path.split(os.path.sep)
            policy_dir = get_policy_string('objects', self.policy)
            quarantine_dir = parts[:parts.index(policy_dir)] + ['quarantined']
            quarantine_path = os.path.sep.join(
                quarantine_dir + [policy_dir] + parts[-2:])
            self.assertPathExists(quarantine_path)


if __name__ == "__main__":
    unittest.main()
