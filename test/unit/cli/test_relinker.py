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
import errno
import fcntl
import json
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


PART_POWER = 8


class TestRelinker(unittest.TestCase):
    def setUp(self):
        skip_if_no_xattrs()
        self.logger = FakeLogger()
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        shutil.rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)

        self.rb = ring.RingBuilder(PART_POWER, 6.0, 1)

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
        self.part = struct.unpack_from('>I', digest)[0] >> 24
        self.next_part = struct.unpack_from('>I', digest)[0] >> 23
        self.objdir = os.path.join(
            self.objects, str(self.part), self._hash[-3:], self._hash)
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

    def test_relink_device_filter(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        relinker.relink(self.testdir, self.devices, True,
                        device=self.existing_device)

        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))

        stat_old = os.stat(os.path.join(self.objdir, self.object_fname))
        stat_new = os.stat(self.expected_file)
        self.assertEqual(stat_old.st_ino, stat_new.st_ino)

    def test_relink_device_filter_invalid(self):
        self.rb.prepare_increase_partition_power()
        self._save_ring()
        relinker.relink(self.testdir, self.devices, True, device='none')

        self.assertFalse(os.path.isdir(self.expected_dir))
        self.assertFalse(os.path.isfile(self.expected_file))

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

    def test_cleanup_device_filter(self):
        self._common_test_cleanup()
        self.assertEqual(0, relinker.cleanup(self.testdir, self.devices, True,
                                             device=self.existing_device))

        # Old objectname should be removed, new should still exist
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        self.assertFalse(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))

    def test_cleanup_device_filter_invalid(self):
        self._common_test_cleanup()
        self.assertEqual(0, relinker.cleanup(self.testdir, self.devices, True,
                                             device='none'))

        # Old objectname should still exist, new should still exist
        self.assertTrue(os.path.isdir(self.expected_dir))
        self.assertTrue(os.path.isfile(self.expected_file))
        self.assertTrue(os.path.isfile(
            os.path.join(self.objdir, self.object_fname)))

    def test_relink_cleanup(self):
        state_file = os.path.join(self.devices, self.existing_device,
                                  'relink.objects.json')

        self.rb.prepare_increase_partition_power()
        self._save_ring()
        relinker.relink(self.testdir, self.devices, True)
        with open(state_file, 'rt') as f:
            orig_inode = os.stat(state_file).st_ino
            self.assertEqual(json.load(f), {
                "part_power": PART_POWER,
                "next_part_power": PART_POWER + 1,
                "state": {str(self.part): True}})

        self.rb.increase_partition_power()
        self.rb._ring = None  # Force builder to reload ring
        self._save_ring()
        with open(state_file, 'rt') as f:
            # Keep the state file open during cleanup so the inode can't be
            # released/re-used when it gets unlinked
            self.assertEqual(orig_inode, os.stat(state_file).st_ino)
            relinker.cleanup(self.testdir, self.devices, True)
            self.assertNotEqual(orig_inode, os.stat(state_file).st_ino)
        with open(state_file, 'rt') as f:
            # NB: part_power/next_part_power tuple changed, so state was reset
            # (though we track prev_part_power for an efficient clean up)
            self.assertEqual(json.load(f), {
                "prev_part_power": PART_POWER,
                "part_power": PART_POWER + 1,
                "next_part_power": PART_POWER + 1,
                "state": {str(self.part): True,
                          str(self.next_part): True}})

    def test_devices_filter_filtering(self):
        # With no filtering, returns all devices
        devices = relinker.devices_filter(None, "", [self.existing_device])
        self.assertEqual(set([self.existing_device]), devices)

        # With a matching filter, returns what is matching
        devices = relinker.devices_filter(self.existing_device, "",
                                          [self.existing_device, 'sda2'])
        self.assertEqual(set([self.existing_device]), devices)

        # With a non matching filter, returns nothing
        devices = relinker.devices_filter('none', "", [self.existing_device])
        self.assertEqual(set(), devices)

    def test_hook_pre_post_device_locking(self):
        locks = [None]
        device_path = os.path.join(self.devices, self.existing_device)
        datadir = 'object'
        lock_file = os.path.join(device_path, '.relink.%s.lock' % datadir)

        # The first run gets the lock
        states = {"state": {}}
        relinker.hook_pre_device(locks, states, datadir, device_path)
        self.assertNotEqual([None], locks)

        # A following run would block
        with self.assertRaises(IOError) as raised:
            with open(lock_file, 'a') as f:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        self.assertEqual(errno.EAGAIN, raised.exception.errno)

        # Another must not get the lock, so it must return an empty list
        relinker.hook_post_device(locks, "")
        self.assertEqual([None], locks)

        with open(lock_file, 'a') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def test_state_file(self):
        device_path = os.path.join(self.devices, self.existing_device)
        datadir = 'objects'
        datadir_path = os.path.join(device_path, datadir)
        state_file = os.path.join(device_path, 'relink.%s.json' % datadir)

        def call_partition_filter(part_power, next_part_power, parts):
            # Partition 312 will be ignored because it must have been created
            # by the relinker
            return relinker.partitions_filter(states,
                                              part_power, next_part_power,
                                              datadir_path, parts)

        # Start relinking
        states = {"part_power": PART_POWER, "next_part_power": PART_POWER + 1,
                  "state": {}}

        # Load the states: As it starts, it must be empty
        locks = [None]
        relinker.hook_pre_device(locks, states, datadir, device_path)
        self.assertEqual({}, states["state"])
        os.close(locks[0])  # Release the lock

        # Partition 312 is ignored because it must have been created with the
        # next_part_power, so it does not need to be relinked
        # 96 and 227 are reverse ordered
        # auditor_status_ALL.json is ignored because it's not a partition
        self.assertEqual(['227', '96'],
                         call_partition_filter(PART_POWER, PART_POWER + 1,
                                               ['96', '227', '312',
                                                'auditor_status.json']))
        self.assertEqual(states["state"], {'96': False, '227': False})

        # Ack partition 96
        relinker.hook_post_partition(states, relinker.STEP_RELINK,
                                     os.path.join(datadir_path, '96'))
        self.assertEqual(states["state"], {'96': True, '227': False})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "part_power": PART_POWER,
                "next_part_power": PART_POWER + 1,
                "state": {'96': True, '227': False}})

        # Restart relinking after only part 96 was done
        self.assertEqual(['227'],
                         call_partition_filter(PART_POWER, PART_POWER + 1,
                                               ['96', '227', '312']))
        self.assertEqual(states["state"], {'96': True, '227': False})

        # Ack partition 227
        relinker.hook_post_partition(states, relinker.STEP_RELINK,
                                     os.path.join(datadir_path, '227'))
        self.assertEqual(states["state"], {'96': True, '227': True})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "part_power": PART_POWER,
                "next_part_power": PART_POWER + 1,
                "state": {'96': True, '227': True}})

        # If the process restarts, it reload the state
        locks = [None]
        states = {
            "part_power": PART_POWER,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }
        relinker.hook_pre_device(locks, states, datadir, device_path)
        self.assertEqual(states, {
            "part_power": PART_POWER,
            "next_part_power": PART_POWER + 1,
            "state": {'96': True, '227': True}})
        os.close(locks[0])  # Release the lock

        # Start cleanup -- note that part_power and next_part_power now match!
        states = {
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }
        # ...which means our state file was ignored
        relinker.hook_pre_device(locks, states, datadir, device_path)
        self.assertEqual(states, {
            "prev_part_power": PART_POWER,
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 1,
            "state": {}})
        os.close(locks[0])  # Release the lock

        self.assertEqual(['227', '96'],
                         call_partition_filter(PART_POWER + 1, PART_POWER + 1,
                                               ['96', '227', '312']))
        # Ack partition 227
        relinker.hook_post_partition(states, relinker.STEP_CLEANUP,
                                     os.path.join(datadir_path, '227'))
        self.assertEqual(states["state"],
                         {'96': False, '227': True})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "prev_part_power": PART_POWER,
                "part_power": PART_POWER + 1,
                "next_part_power": PART_POWER + 1,
                "state": {'96': False, '227': True}})

        # Restart cleanup after only part 227 was done
        self.assertEqual(['96'],
                         call_partition_filter(PART_POWER + 1, PART_POWER + 1,
                                               ['96', '227', '312']))
        self.assertEqual(states["state"],
                         {'96': False, '227': True})

        # Ack partition 96
        relinker.hook_post_partition(states, relinker.STEP_CLEANUP,
                                     os.path.join(datadir_path, '96'))
        self.assertEqual(states["state"],
                         {'96': True, '227': True})
        with open(state_file, 'rt') as f:
            self.assertEqual(json.load(f), {
                "prev_part_power": PART_POWER,
                "part_power": PART_POWER + 1,
                "next_part_power": PART_POWER + 1,
                "state": {'96': True, '227': True}})

        # At the end, the state is still accurate
        locks = [None]
        states = {
            "prev_part_power": PART_POWER,
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 1,
            "state": {},
        }
        relinker.hook_pre_device(locks, states, datadir, device_path)
        self.assertEqual(states["state"],
                         {'96': True, '227': True})
        os.close(locks[0])  # Release the lock

        # If the part_power/next_part_power tuple differs, restart from scratch
        locks = [None]
        states = {
            "part_power": PART_POWER + 1,
            "next_part_power": PART_POWER + 2,
            "state": {},
        }
        relinker.hook_pre_device(locks, states, datadir, device_path)
        self.assertEqual(states["state"], {})
        self.assertFalse(os.path.exists(state_file))
        os.close(locks[0])  # Release the lock

        # If the file gets corrupted, restart from scratch
        with open(state_file, 'wt') as f:
            f.write('NOT JSON')
        locks = [None]
        states = {"part_power": PART_POWER, "next_part_power": PART_POWER + 1,
                  "state": {}}
        relinker.hook_pre_device(locks, states, datadir, device_path)
        self.assertEqual(states["state"], {})
        self.assertFalse(os.path.exists(state_file))
        os.close(locks[0])  # Release the lock

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


if __name__ == '__main__':
    unittest.main()
