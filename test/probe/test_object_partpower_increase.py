#!/usr/bin/env python
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

import os

from errno import EEXIST
from shutil import copyfile
from tempfile import mkstemp
from time import time
from unittest import main
from uuid import uuid4

from swiftclient import client

from swift.cli.relinker import relink, cleanup
from swift.common.manager import Manager
from swift.common.ring import RingBuilder
from swift.common.utils import replace_partition_in_path
from swift.obj.diskfile import get_data_dir
from test.probe.common import ECProbeTest, ProbeTest, ReplProbeTest


class TestPartPowerIncrease(ProbeTest):
    def setUp(self):
        super(TestPartPowerIncrease, self).setUp()
        _, self.ring_file_backup = mkstemp()
        _, self.builder_file_backup = mkstemp()
        self.ring_file = self.object_ring.serialized_path
        self.builder_file = self.ring_file.replace('ring.gz', 'builder')
        copyfile(self.ring_file, self.ring_file_backup)
        copyfile(self.builder_file, self.builder_file_backup)
        # In case the test user is not allowed to write rings
        self.assertTrue(os.access('/etc/swift', os.W_OK))
        self.assertTrue(os.access('/etc/swift/backups', os.W_OK))
        self.assertTrue(os.access('/etc/swift/object.builder', os.W_OK))
        self.assertTrue(os.access('/etc/swift/object.ring.gz', os.W_OK))
        # Ensure the test object will be erasure coded
        self.data = ' ' * getattr(self.policy, 'ec_segment_size', 1)

        self.devices = [
            self.device_dir('object', {'ip': ip, 'port': port, 'device': ''})
            for ip, port in set((dev['ip'], dev['port'])
                                for dev in self.object_ring.devs)]

    def tearDown(self):
        # Keep a backup copy of the modified .builder file
        backup_dir = os.path.join(
            os.path.dirname(self.builder_file), 'backups')
        try:
            os.mkdir(backup_dir)
        except OSError as err:
            if err.errno != EEXIST:
                raise
        backup_name = (os.path.join(
            backup_dir,
            '%d.probe.' % time() + os.path.basename(self.builder_file)))
        copyfile(self.builder_file, backup_name)

        # Restore original ring
        os.system('sudo mv %s %s' % (
            self.ring_file_backup, self.ring_file))
        os.system('sudo mv %s %s' % (
            self.builder_file_backup, self.builder_file))

    def _find_objs_ondisk(self, container, obj):
        locations = []
        opart, onodes = self.object_ring.get_nodes(
            self.account, container, obj)
        for node in onodes:
            start_dir = os.path.join(
                self.device_dir('object', node),
                get_data_dir(self.policy),
                str(opart))
            for root, dirs, files in os.walk(start_dir):
                for filename in files:
                    if filename.endswith('.data'):
                        locations.append(os.path.join(root, filename))
        return locations

    def _test_main(self, cancel=False):
        container = 'container-%s' % uuid4()
        obj = 'object-%s' % uuid4()
        obj2 = 'object-%s' % uuid4()

        # Create container
        headers = {'X-Storage-Policy': self.policy.name}
        client.put_container(self.url, self.token, container, headers=headers)

        # Create a new object
        client.put_object(self.url, self.token, container, obj, self.data)
        client.head_object(self.url, self.token, container, obj)

        # Prepare partition power increase
        builder = RingBuilder.load(self.builder_file)
        builder.prepare_increase_partition_power()
        builder.save(self.builder_file)
        ring_data = builder.get_ring()
        ring_data.save(self.ring_file)

        # Ensure the proxy uses the changed ring
        Manager(['proxy']).restart()

        # Ensure object is still accessible
        client.head_object(self.url, self.token, container, obj)

        # Relink existing objects
        for device in self.devices:
            self.assertEqual(0, relink(skip_mount_check=True, devices=device))

        # Create second object after relinking and ensure it is accessible
        client.put_object(self.url, self.token, container, obj2, self.data)
        client.head_object(self.url, self.token, container, obj2)

        # Remember the original object locations
        org_locations = self._find_objs_ondisk(container, obj)
        org_locations += self._find_objs_ondisk(container, obj2)

        # Remember the new object locations
        new_locations = []
        for loc in org_locations:
            new_locations.append(replace_partition_in_path(
                str(loc), self.object_ring.part_power + 1))

        # Overwrite existing object - to ensure that older timestamp files
        # will be cleaned up properly later
        client.put_object(self.url, self.token, container, obj, self.data)

        # Ensure objects are still accessible
        client.head_object(self.url, self.token, container, obj)
        client.head_object(self.url, self.token, container, obj2)

        # Increase partition power
        builder = RingBuilder.load(self.builder_file)
        if not cancel:
            builder.increase_partition_power()
        else:
            builder.cancel_increase_partition_power()
        builder.save(self.builder_file)
        ring_data = builder.get_ring()
        ring_data.save(self.ring_file)

        # Ensure the proxy uses the changed ring
        Manager(['proxy']).restart()

        # Ensure objects are still accessible
        client.head_object(self.url, self.token, container, obj)
        client.head_object(self.url, self.token, container, obj2)

        # Overwrite existing object - to ensure that older timestamp files
        # will be cleaned up properly later
        client.put_object(self.url, self.token, container, obj, self.data)

        # Cleanup old objects in the wrong location
        for device in self.devices:
            self.assertEqual(0, cleanup(skip_mount_check=True, devices=device))

        # Ensure objects are still accessible
        client.head_object(self.url, self.token, container, obj)
        client.head_object(self.url, self.token, container, obj2)

        # Ensure data in old or relinked object locations is removed
        if not cancel:
            for fn in org_locations:
                self.assertFalse(os.path.exists(fn))
        else:
            for fn in new_locations:
                self.assertFalse(os.path.exists(fn))


class TestReplPartPowerIncrease(TestPartPowerIncrease, ReplProbeTest):
    def test_main(self):
        self._test_main()

    def test_canceled(self):
        self._test_main(cancel=True)


class TestECPartPowerIncrease(TestPartPowerIncrease, ECProbeTest):
    def test_main(self):
        self._test_main()

    def test_canceled(self):
        self._test_main(cancel=True)


if __name__ == '__main__':
    main()
