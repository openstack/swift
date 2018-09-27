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

import binascii
import os
import shutil
import struct
import tempfile
import unittest

from swift.cli import relinker
from swift.common import exceptions, ring, utils
from swift.common import storage_policy
from swift.common.storage_policy import (
    StoragePolicy, StoragePolicyCollection, POLICIES, ECStoragePolicy)

from swift.obj.diskfile import write_metadata

from test.unit import FakeLogger, skip_if_no_xattrs, DEFAULT_TEST_EC_TYPE, \
    patch_policies


class TestRelinker(unittest.TestCase):
    def setUp(self):
        skip_if_no_xattrs()
        self.logger = FakeLogger()
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        shutil.rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)

        self.rb = ring.RingBuilder(8, 6.0, 1)

        for i in range(6):
            ip = "127.0.0.%s" % i
            self.rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 1,
                             'ip': ip, 'port': 10000, 'device': 'sda1'})
        self.rb.rebalance(seed=1)

        self.existing_device = 'sda1'
        os.mkdir(os.path.join(self.devices, self.existing_device))
        self.objects = os.path.join(self.devices, self.existing_device,
                                    'objects')
        os.mkdir(self.objects)
        self._hash = utils.hash_path('a/c/o')
        digest = binascii.unhexlify(self._hash)
        part = struct.unpack_from('>I', digest)[0] >> 24
        self.next_part = struct.unpack_from('>I', digest)[0] >> 23
        self.objdir = os.path.join(
            self.objects, str(part), self._hash[-3:], self._hash)
        os.makedirs(self.objdir)
        self.object_fname = "1278553064.00000.data"
        self.objname = os.path.join(self.objdir, self.object_fname)
        with open(self.objname, "wb") as dummy:
            dummy.write(b"Hello World!")
            write_metadata(dummy, {'name': '/a/c/o', 'Content-Length': '12'})

        test_policies = [StoragePolicy(0, 'platin', True)]
        storage_policy._POLICIES = StoragePolicyCollection(test_policies)

        self.expected_dir = os.path.join(
            self.objects, str(self.next_part), self._hash[-3:], self._hash)
        self.expected_file = os.path.join(self.expected_dir, self.object_fname)

    def _save_ring(self):
        rd = self.rb.get_ring()
        for policy in POLICIES:
            rd.save(os.path.join(
                self.testdir, '%s.ring.gz' % policy.ring_name))
            # Enforce ring reloading in relinker
            policy.object_ring = None

    def tearDown(self):
        shutil.rmtree(self.testdir, ignore_errors=1)
        storage_policy.reload_storage_policies()

    def test_relink(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        relinker.relink(self.testdir, self.devices, True)

        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))

        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)

    def _common_test_cleanup(self, relink=True):
        # Create a ring that has prev_part_power set
        self.rb.prepare_increase_partition_power()
        self.rb.increase_partition_power()
        self._save_ring()

        os.makedirs(self.expected_dir)

        if relink:
            # Create a hardlink to the original object name. This is expected
            # after a normal relinker run
            os.link(os.path.join(self.objdir, self.object_fname),
                    self.expected_file)

    def test_cleanup(self):
        self._common_test_cleanup()
        self.assertEqual(0, relinker.cleanup(self.testdir, self.devices, True))

        # Old objectname should be removed, new should still exist
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        self.assertFalse(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))

    def test_cleanup_not_yet_relinked(self):
        self._common_test_cleanup(relink=False)
        self.assertEqual(1, relinker.cleanup(self.testdir, self.devices, True))

        self.assertTrue(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))

    def test_cleanup_deleted(self):
        self._common_test_cleanup()

        # Pretend the object got deleted inbetween and there is a tombstone
        fname_ts = self.expected_file[:-4] + "ts"
        os.rename(self.expected_file, fname_ts)

        self.assertEqual(0, relinker.cleanup(self.testdir, self.devices, True))

    def test_cleanup_doesnotexist(self):
        self._common_test_cleanup()

        # Pretend the file in the new place got deleted inbetween
        os.remove(self.expected_file)

        self.assertEqual(
            1, relinker.cleanup(self.testdir, self.devices, True, self.logger))
        self.assertEqual(self.logger.get_lines_for_level('warning'),
                         ['Error cleaning up %s: %s' % (self.objname,
                          repr(exceptions.DiskFileNotExist()))])

    @patch_policies(
        [ECStoragePolicy(
         0, name='platin', is_default=True, ec_type=DEFAULT_TEST_EC_TYPE,
         ec_ndata=4, ec_nparity=2)])
    def test_cleanup_non_durable_fragment(self):
        self._common_test_cleanup()

        # Switch the policy type so that actually all fragments are non-durable
        # and raise a DiskFileNotExist in EC in this test. However, if the
        # counterpart exists in the new location, this is ok - it will be fixed
        # by the reconstructor later on
        self.assertEqual(
            0, relinker.cleanup(self.testdir, self.devices, True,
                                self.logger))
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])

    def test_cleanup_quarantined(self):
        self._common_test_cleanup()
        # Pretend the object in the new place got corrupted
        with open(self.expected_file, "wb") as obj:
            obj.write(b'trash')

        self.assertEqual(
            1, relinker.cleanup(self.testdir, self.devices, True, self.logger))

        self.assertIn('failed audit and was quarantined',
                      self.logger.get_lines_for_level('warning')[0])
