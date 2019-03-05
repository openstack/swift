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

import array
from contextlib import contextmanager
import json
import mock
import os
from posix import stat_result, statvfs_result
from shutil import rmtree
import tempfile
import unittest
from unittest import TestCase

from swift import __version__ as swiftver
from swift.common import ring, utils
from swift.common.swob import Request
from swift.common.middleware import recon
from swift.common.storage_policy import StoragePolicy
from test.unit import patch_policies


def fake_check_mount(a, b):
    raise OSError('Input/Output Error')


def fail_os_listdir():
    raise OSError('No such file or directory')


def fail_io_open(file_path, open_mode):
    raise IOError('No such file or directory')


class FakeApp(object):
    def __call__(self, env, start_response):
        return b"FAKE APP"


def start_response(*args):
    pass


class FakeFromCache(object):

    def __init__(self, out=None):
        self.fakeout = out
        self.fakeout_calls = []

    def fake_from_recon_cache(self, *args, **kwargs):
        self.fakeout_calls.append((args, kwargs))
        return self.fakeout


class OpenAndReadTester(object):

    def __init__(self, output_iter):
        self.index = 0
        self.out_len = len(output_iter) - 1
        self.data = output_iter
        self.output_iter = iter(output_iter)
        self.read_calls = []
        self.open_calls = []

    def __iter__(self):
        return self

    def __next__(self):
        if self.index == self.out_len:
            raise StopIteration
        else:
            line = self.data[self.index]
            self.index += 1
        return line

    next = __next__

    def read(self, *args, **kwargs):
        self.read_calls.append((args, kwargs))
        try:
            return next(self.output_iter)
        except StopIteration:
            return ''

    @contextmanager
    def open(self, *args, **kwargs):
        self.open_calls.append((args, kwargs))
        yield self


class MockOS(object):

    def __init__(self, ls_out=None, isdir_out=None, ismount_out=False,
                 statvfs_out=None):
        self.ls_output = ls_out
        self.isdir_output = isdir_out
        self.ismount_output = ismount_out
        self.statvfs_output = statvfs_out
        self.listdir_calls = []
        self.isdir_calls = []
        self.ismount_calls = []
        self.statvfs_calls = []

    def fake_listdir(self, *args, **kwargs):
        self.listdir_calls.append((args, kwargs))
        return self.ls_output

    def fake_isdir(self, *args, **kwargs):
        self.isdir_calls.append((args, kwargs))
        return self.isdir_output

    def fake_ismount(self, *args, **kwargs):
        self.ismount_calls.append((args, kwargs))
        if isinstance(self.ismount_output, Exception):
            raise self.ismount_output
        else:
            return self.ismount_output

    def fake_statvfs(self, *args, **kwargs):
        self.statvfs_calls.append((args, kwargs))
        return statvfs_result(self.statvfs_output)


class FakeRecon(object):

    def __init__(self):
        self.fake_replication_rtype = None
        self.fake_updater_rtype = None
        self.fake_auditor_rtype = None
        self.fake_expirer_rtype = None

    def fake_mem(self):
        return {'memtest': "1"}

    def fake_load(self):
        return {'loadtest': "1"}

    def fake_async(self):
        return {'asynctest': "1"}

    def fake_get_device_info(self):
        return {"/srv/1/node": ["sdb1"]}

    def fake_replication(self, recon_type):
        self.fake_replication_rtype = recon_type
        return {'replicationtest': "1"}

    def fake_updater(self, recon_type):
        self.fake_updater_rtype = recon_type
        return {'updatertest': "1"}

    def fake_auditor(self, recon_type):
        self.fake_auditor_rtype = recon_type
        return {'auditortest': "1"}

    def fake_expirer(self, recon_type):
        self.fake_expirer_rtype = recon_type
        return {'expirertest': "1"}

    def fake_mounted(self):
        return {'mountedtest': "1"}

    def fake_unmounted(self):
        return {'unmountedtest': "1"}

    def fake_unmounted_empty(self):
        return []

    def fake_diskusage(self):
        return {'diskusagetest': "1"}

    def fake_ringmd5(self):
        return {'ringmd5test': "1"}

    def fake_swiftconfmd5(self):
        return {'/etc/swift/swift.conf': "abcdef"}

    def fake_quarantined(self):
        return {'quarantinedtest': "1"}

    def fake_sockstat(self):
        return {'sockstattest': "1"}

    def fake_driveaudit(self):
        return {'driveaudittest': "1"}

    def fake_time(self):
        return {'timetest': "1"}

    def nocontent(self):
        return None

    def raise_IOError(self, *args, **kwargs):
        raise IOError

    def raise_ValueError(self, *args, **kwargs):
        raise ValueError

    def raise_Exception(self, *args, **kwargs):
        raise Exception


@patch_policies(legacy_only=True)
class TestReconSuccess(TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='swift_recon_md5_test')
        utils.mkdirs(self.tempdir)
        self.app = self._get_app()
        self.mockos = MockOS()
        self.fakecache = FakeFromCache()
        self.real_listdir = os.listdir
        self.real_isdir = os.path.isdir
        self.real_ismount = utils.ismount
        self.real_statvfs = os.statvfs
        os.listdir = self.mockos.fake_listdir
        os.path.isdir = self.mockos.fake_isdir
        utils.ismount = self.mockos.fake_ismount
        os.statvfs = self.mockos.fake_statvfs
        self.real_from_cache = self.app._from_recon_cache
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache
        self.frecon = FakeRecon()

        # replace hash md5 implementation of the md5_hash_for_file function
        mock_hash_for_file = mock.patch(
            'swift.common.middleware.recon.md5_hash_for_file',
            lambda f, **kwargs: 'hash-' + os.path.basename(f))
        self.addCleanup(mock_hash_for_file.stop)
        mock_hash_for_file.start()

        self.ring_part_shift = 5
        self.ring_devs = [{'id': 0, 'zone': 0, 'weight': 1.0,
                           'ip': '10.1.1.1', 'port': 6200,
                           'device': 'sda1'},
                          {'id': 1, 'zone': 0, 'weight': 1.0,
                           'ip': '10.1.1.1', 'port': 6200,
                           'device': 'sdb1'},
                          None,
                          {'id': 3, 'zone': 2, 'weight': 1.0,
                           'ip': '10.1.2.1', 'port': 6200,
                           'device': 'sdc1'},
                          {'id': 4, 'zone': 2, 'weight': 1.0,
                           'ip': '10.1.2.2', 'port': 6200,
                           'device': 'sdd1'}]
        self._create_rings()

    def tearDown(self):
        os.listdir = self.real_listdir
        os.path.isdir = self.real_isdir
        utils.ismount = self.real_ismount
        os.statvfs = self.real_statvfs
        del self.mockos
        self.app._from_recon_cache = self.real_from_cache
        del self.fakecache
        rmtree(self.tempdir)

    def _get_app(self):
        app = recon.ReconMiddleware(FakeApp(), {'swift_dir': self.tempdir})
        return app

    def _create_ring(self, ringpath, replica_map, devs, part_shift):
        ring.RingData(replica_map, devs, part_shift).save(ringpath,
                                                          mtime=None)

    def _create_rings(self):
        # make the rings unique so they have different md5 sums
        rings = {
            'account.ring.gz': [
                array.array('H', [3, 1, 3, 1]),
                array.array('H', [0, 3, 1, 4]),
                array.array('H', [1, 4, 0, 3])],
            'container.ring.gz': [
                array.array('H', [4, 3, 0, 1]),
                array.array('H', [0, 1, 3, 4]),
                array.array('H', [3, 4, 0, 1])],
            'object.ring.gz': [
                array.array('H', [0, 1, 0, 1]),
                array.array('H', [0, 1, 0, 1]),
                array.array('H', [3, 4, 3, 4])],
            'object-1.ring.gz': [
                array.array('H', [1, 0, 1, 0]),
                array.array('H', [1, 0, 1, 0]),
                array.array('H', [4, 3, 4, 3])],
            'object-2.ring.gz': [
                array.array('H', [1, 1, 1, 0]),
                array.array('H', [1, 0, 1, 3]),
                array.array('H', [4, 2, 4, 3])]
        }

        for ringfn, replica_map in rings.items():
            ringpath = os.path.join(self.tempdir, ringfn)
            self._create_ring(ringpath, replica_map, self.ring_devs,
                              self.ring_part_shift)

    @patch_policies([
        StoragePolicy(0, 'stagecoach'),
        StoragePolicy(1, 'pinto', is_deprecated=True),
        StoragePolicy(2, 'toyota', is_default=True),
    ])
    def test_get_ring_md5(self):
        # We should only see configured and present rings, so to handle the
        # "normal" case just patch the policies to match the existing rings.
        expt_out = {'%s/account.ring.gz' % self.tempdir:
                    'hash-account.ring.gz',
                    '%s/container.ring.gz' % self.tempdir:
                    'hash-container.ring.gz',
                    '%s/object.ring.gz' % self.tempdir:
                    'hash-object.ring.gz',
                    '%s/object-1.ring.gz' % self.tempdir:
                    'hash-object-1.ring.gz',
                    '%s/object-2.ring.gz' % self.tempdir:
                    'hash-object-2.ring.gz'}

        # We need to instantiate app after overriding the configured policies.
        app = self._get_app()
        # object-{1,2}.ring.gz should both appear as they are present on disk
        # and were configured as policies.
        self.assertEqual(sorted(app.get_ring_md5().items()),
                         sorted(expt_out.items()))

    def test_get_ring_md5_ioerror_produces_none_hash(self):
        # Ring files that are present but produce an IOError on read should
        # still produce a ringmd5 entry with a None for the hash. Note that
        # this is different than if an expected ring file simply doesn't exist,
        # in which case it is excluded altogether from the ringmd5 response.
        expt_out = {'%s/account.ring.gz' % self.tempdir: None,
                    '%s/container.ring.gz' % self.tempdir: None,
                    '%s/object.ring.gz' % self.tempdir: None}
        with mock.patch('swift.common.middleware.recon.md5_hash_for_file',
                        side_effect=IOError):
            ringmd5 = self.app.get_ring_md5()
        self.assertEqual(sorted(ringmd5.items()),
                         sorted(expt_out.items()))

    def test_get_ring_md5_failed_ring_hash_recovers_without_restart(self):
        # Ring files that are present but produce an IOError on read will
        # show a None hash, but if they can be read later their hash
        # should become available in the ringmd5 response.
        expt_out = {'%s/account.ring.gz' % self.tempdir: None,
                    '%s/container.ring.gz' % self.tempdir: None,
                    '%s/object.ring.gz' % self.tempdir: None}
        with mock.patch('swift.common.middleware.recon.md5_hash_for_file',
                        side_effect=IOError):
            ringmd5 = self.app.get_ring_md5()
        self.assertEqual(sorted(ringmd5.items()),
                         sorted(expt_out.items()))

        # If we fix a ring and it can be read again, its hash should then
        # appear using the same app instance
        def fake_hash_for_file(fn):
            if 'object' not in fn:
                raise IOError
            return 'hash-' + os.path.basename(fn)

        expt_out = {'%s/account.ring.gz' % self.tempdir: None,
                    '%s/container.ring.gz' % self.tempdir: None,
                    '%s/object.ring.gz' % self.tempdir:
                    'hash-object.ring.gz'}

        with mock.patch('swift.common.middleware.recon.md5_hash_for_file',
                        fake_hash_for_file):
            ringmd5 = self.app.get_ring_md5()
        self.assertEqual(sorted(ringmd5.items()),
                         sorted(expt_out.items()))

    @patch_policies([
        StoragePolicy(0, 'stagecoach'),
        StoragePolicy(2, 'bike', is_default=True),
        StoragePolicy(3502, 'train')
    ])
    def test_get_ring_md5_missing_ring_recovers_without_restart(self):
        # If a configured ring is missing when the app is instantiated, but is
        # later moved into place, we shouldn't need to restart object-server
        # for it to appear in recon.
        expt_out = {'%s/account.ring.gz' % self.tempdir:
                    'hash-account.ring.gz',
                    '%s/container.ring.gz' % self.tempdir:
                    'hash-container.ring.gz',
                    '%s/object.ring.gz' % self.tempdir:
                    'hash-object.ring.gz',
                    '%s/object-2.ring.gz' % self.tempdir:
                    'hash-object-2.ring.gz'}

        # We need to instantiate app after overriding the configured policies.
        app = self._get_app()
        # object-1.ring.gz should not appear as it's present but unconfigured.
        # object-3502.ring.gz should not appear as it's configured but not
        # (yet) present.
        self.assertEqual(sorted(app.get_ring_md5().items()),
                         sorted(expt_out.items()))

        # Simulate the configured policy's missing ringfile being moved into
        # place during runtime
        ringfn = 'object-3502.ring.gz'
        ringpath = os.path.join(self.tempdir, ringfn)
        ringmap = [array.array('H', [1, 2, 1, 4]),
                   array.array('H', [4, 0, 1, 3]),
                   array.array('H', [1, 1, 0, 3])]
        self._create_ring(os.path.join(self.tempdir, ringfn),
                          ringmap, self.ring_devs, self.ring_part_shift)
        expt_out[ringpath] = 'hash-' + ringfn

        # We should now see it in the ringmd5 response, without a restart
        # (using the same app instance)
        self.assertEqual(sorted(app.get_ring_md5().items()),
                         sorted(expt_out.items()))

    @patch_policies([
        StoragePolicy(0, 'stagecoach', is_default=True),
        StoragePolicy(2, 'bike'),
        StoragePolicy(2305, 'taxi')
    ])
    def test_get_ring_md5_excludes_configured_missing_obj_rings(self):
        # Object rings that are configured but missing aren't meant to appear
        # in the ringmd5 response.
        expt_out = {'%s/account.ring.gz' % self.tempdir:
                    'hash-account.ring.gz',
                    '%s/container.ring.gz' % self.tempdir:
                    'hash-container.ring.gz',
                    '%s/object.ring.gz' % self.tempdir:
                    'hash-object.ring.gz',
                    '%s/object-2.ring.gz' % self.tempdir:
                    'hash-object-2.ring.gz'}

        # We need to instantiate app after overriding the configured policies.
        app = self._get_app()
        # object-1.ring.gz should not appear as it's present but unconfigured.
        # object-2305.ring.gz should not appear as it's configured but not
        # present.
        self.assertEqual(sorted(app.get_ring_md5().items()),
                         sorted(expt_out.items()))

    @patch_policies([
        StoragePolicy(0, 'zero', is_default=True),
    ])
    def test_get_ring_md5_excludes_unconfigured_present_obj_rings(self):
        # Object rings that are present but not configured in swift.conf
        # aren't meant to appear in the ringmd5 response.
        expt_out = {'%s/account.ring.gz' % self.tempdir:
                    'hash-account.ring.gz',
                    '%s/container.ring.gz' % self.tempdir:
                    'hash-container.ring.gz',
                    '%s/object.ring.gz' % self.tempdir:
                    'hash-object.ring.gz'}

        # We need to instantiate app after overriding the configured policies.
        app = self._get_app()
        # object-{1,2}.ring.gz should not appear as they are present on disk
        # but were not configured as policies.
        self.assertEqual(sorted(app.get_ring_md5().items()),
                         sorted(expt_out.items()))

    def test_from_recon_cache(self):
        oart = OpenAndReadTester(['{"notneeded": 5, "testkey1": "canhazio"}'])
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                        'test.cache', openr=oart.open)
        self.assertEqual(oart.read_calls, [((), {})])
        self.assertEqual(oart.open_calls, [(('test.cache', 'r'), {})])
        self.assertEqual(rv, {'notpresentkey': None, 'testkey1': 'canhazio'})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_from_recon_cache_ioerror(self):
        oart = self.frecon.raise_IOError
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                        'test.cache', openr=oart)
        self.assertEqual(rv, {'notpresentkey': None, 'testkey1': None})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_from_recon_cache_valueerror(self):
        oart = self.frecon.raise_ValueError
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                        'test.cache', openr=oart)
        self.assertEqual(rv, {'notpresentkey': None, 'testkey1': None})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_from_recon_cache_exception(self):
        oart = self.frecon.raise_Exception
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                        'test.cache', openr=oart)
        self.assertEqual(rv, {'notpresentkey': None, 'testkey1': None})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_get_mounted(self):
        mounts_content = [
            'rootfs / rootfs rw 0 0',
            'none /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0',
            'none /proc proc rw,nosuid,nodev,noexec,relatime 0 0',
            'none /dev devtmpfs rw,relatime,size=248404k,nr_inodes=62101,'
            'mode=755 0 0',
            'none /dev/pts devpts rw,nosuid,noexec,relatime,gid=5,mode=620,'
            'ptmxmode=000 0 0',
            '/dev/disk/by-uuid/e5b143bd-9f31-49a7-b018-5e037dc59252 / ext4'
            ' rw,relatime,errors=remount-ro,barrier=1,data=ordered 0 0',
            'none /sys/fs/fuse/connections fusectl rw,relatime 0 0',
            'none /sys/kernel/debug debugfs rw,relatime 0 0',
            'none /sys/kernel/security securityfs rw,relatime 0 0',
            'none /dev/shm tmpfs rw,nosuid,nodev,relatime 0 0',
            'none /var/run tmpfs rw,nosuid,relatime,mode=755 0 0',
            'none /var/lock tmpfs rw,nosuid,nodev,noexec,relatime 0 0',
            'none /lib/init/rw tmpfs rw,nosuid,relatime,mode=755 0 0',
            '/dev/loop0 /mnt/sdb1 xfs rw,noatime,nodiratime,attr2,nobarrier,'
            'logbufs=8,noquota 0 0',
            'rpc_pipefs /var/lib/nfs/rpc_pipefs rpc_pipefs rw,relatime 0 0',
            'nfsd /proc/fs/nfsd nfsd rw,relatime 0 0',
            'none /proc/fs/vmblock/mountPoint vmblock rw,relatime 0 0',
            '']
        mounted_resp = [
            {'device': 'rootfs', 'path': '/'},
            {'device': 'none', 'path': '/sys'},
            {'device': 'none', 'path': '/proc'},
            {'device': 'none', 'path': '/dev'},
            {'device': 'none', 'path': '/dev/pts'},
            {'device': '/dev/disk/by-uuid/'
             'e5b143bd-9f31-49a7-b018-5e037dc59252', 'path': '/'},
            {'device': 'none', 'path': '/sys/fs/fuse/connections'},
            {'device': 'none', 'path': '/sys/kernel/debug'},
            {'device': 'none', 'path': '/sys/kernel/security'},
            {'device': 'none', 'path': '/dev/shm'},
            {'device': 'none', 'path': '/var/run'},
            {'device': 'none', 'path': '/var/lock'},
            {'device': 'none', 'path': '/lib/init/rw'},
            {'device': '/dev/loop0', 'path': '/mnt/sdb1'},
            {'device': 'rpc_pipefs', 'path': '/var/lib/nfs/rpc_pipefs'},
            {'device': 'nfsd', 'path': '/proc/fs/nfsd'},
            {'device': 'none', 'path': '/proc/fs/vmblock/mountPoint'}]
        oart = OpenAndReadTester(mounts_content)
        rv = self.app.get_mounted(openr=oart.open)
        self.assertEqual(oart.open_calls, [(('/proc/mounts', 'r'), {})])
        self.assertEqual(rv, mounted_resp)

    def test_get_load(self):
        oart = OpenAndReadTester(['0.03 0.03 0.00 1/220 16306'])
        rv = self.app.get_load(openr=oart.open)
        self.assertEqual(oart.read_calls, [((), {})])
        self.assertEqual(oart.open_calls, [(('/proc/loadavg', 'r'), {})])
        self.assertEqual(rv, {'5m': 0.029999999999999999, '15m': 0.0,
                              'processes': 16306, 'tasks': '1/220',
                              '1m': 0.029999999999999999})

    def test_get_mem(self):
        meminfo_content = ['MemTotal:         505840 kB',
                           'MemFree:           26588 kB',
                           'Buffers:           44948 kB',
                           'Cached:           146376 kB',
                           'SwapCached:        14736 kB',
                           'Active:           194900 kB',
                           'Inactive:         193412 kB',
                           'Active(anon):      94208 kB',
                           'Inactive(anon):   102848 kB',
                           'Active(file):     100692 kB',
                           'Inactive(file):    90564 kB',
                           'Unevictable:           0 kB',
                           'Mlocked:               0 kB',
                           'SwapTotal:        407544 kB',
                           'SwapFree:         313436 kB',
                           'Dirty:               104 kB',
                           'Writeback:             0 kB',
                           'AnonPages:        185268 kB',
                           'Mapped:             9592 kB',
                           'Shmem:                68 kB',
                           'Slab:              61716 kB',
                           'SReclaimable:      46620 kB',
                           'SUnreclaim:        15096 kB',
                           'KernelStack:        1760 kB',
                           'PageTables:         8832 kB',
                           'NFS_Unstable:          0 kB',
                           'Bounce:                0 kB',
                           'WritebackTmp:          0 kB',
                           'CommitLimit:      660464 kB',
                           'Committed_AS:     565608 kB',
                           'VmallocTotal:   34359738367 kB',
                           'VmallocUsed:      266724 kB',
                           'VmallocChunk:   34359467156 kB',
                           'HardwareCorrupted:     0 kB',
                           'HugePages_Total:       0',
                           'HugePages_Free:        0',
                           'HugePages_Rsvd:        0',
                           'HugePages_Surp:        0',
                           'Hugepagesize:       2048 kB',
                           'DirectMap4k:       10240 kB',
                           'DirectMap2M:      514048 kB',
                           '']
        meminfo_resp = {'WritebackTmp': '0 kB',
                        'SwapTotal': '407544 kB',
                        'Active(anon)': '94208 kB',
                        'SwapFree': '313436 kB',
                        'DirectMap4k': '10240 kB',
                        'KernelStack': '1760 kB',
                        'MemFree': '26588 kB',
                        'HugePages_Rsvd': '0',
                        'Committed_AS': '565608 kB',
                        'Active(file)': '100692 kB',
                        'NFS_Unstable': '0 kB',
                        'VmallocChunk': '34359467156 kB',
                        'Writeback': '0 kB',
                        'Inactive(file)': '90564 kB',
                        'MemTotal': '505840 kB',
                        'VmallocUsed': '266724 kB',
                        'HugePages_Free': '0',
                        'AnonPages': '185268 kB',
                        'Active': '194900 kB',
                        'Inactive(anon)': '102848 kB',
                        'CommitLimit': '660464 kB',
                        'Hugepagesize': '2048 kB',
                        'Cached': '146376 kB',
                        'SwapCached': '14736 kB',
                        'VmallocTotal': '34359738367 kB',
                        'Shmem': '68 kB',
                        'Mapped': '9592 kB',
                        'SUnreclaim': '15096 kB',
                        'Unevictable': '0 kB',
                        'SReclaimable': '46620 kB',
                        'Mlocked': '0 kB',
                        'DirectMap2M': '514048 kB',
                        'HugePages_Surp': '0',
                        'Bounce': '0 kB',
                        'Inactive': '193412 kB',
                        'PageTables': '8832 kB',
                        'HardwareCorrupted': '0 kB',
                        'HugePages_Total': '0',
                        'Slab': '61716 kB',
                        'Buffers': '44948 kB',
                        'Dirty': '104 kB'}
        oart = OpenAndReadTester(meminfo_content)
        rv = self.app.get_mem(openr=oart.open)
        self.assertEqual(oart.open_calls, [(('/proc/meminfo', 'r'), {})])
        self.assertEqual(rv, meminfo_resp)

    def test_get_async_info(self):
        from_cache_response = {'async_pending': 5}
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_async_info()
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['async_pending'],
                             '/var/cache/swift/object.recon'), {})])
        self.assertEqual(rv, {'async_pending': 5})

    def test_get_replication_info_account(self):
        from_cache_response = {
            "replication_stats": {
                "attempted": 1, "diff": 0,
                "diff_capped": 0, "empty": 0,
                "failure": 0, "hashmatch": 0,
                "failure_nodes": {
                    "192.168.0.1": 0,
                    "192.168.0.2": 0},
                "no_change": 2, "remote_merge": 0,
                "remove": 0, "rsync": 0,
                "start": 1333044050.855202,
                "success": 2, "ts_repl": 0},
            "replication_time": 0.2615511417388916,
            "replication_last": 1357969645.25}
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_replication_info('account')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['replication_time', 'replication_stats',
                             'replication_last'],
                             '/var/cache/swift/account.recon'), {})])
        self.assertEqual(rv, {
            "replication_stats": {
                "attempted": 1, "diff": 0,
                "diff_capped": 0, "empty": 0,
                "failure": 0, "hashmatch": 0,
                "failure_nodes": {
                    "192.168.0.1": 0,
                    "192.168.0.2": 0},
                "no_change": 2, "remote_merge": 0,
                "remove": 0, "rsync": 0,
                "start": 1333044050.855202,
                "success": 2, "ts_repl": 0},
            "replication_time": 0.2615511417388916,
            "replication_last": 1357969645.25})

    def test_get_replication_info_container(self):
        from_cache_response = {
            "replication_time": 200.0,
            "replication_stats": {
                "attempted": 179, "diff": 0,
                "diff_capped": 0, "empty": 0,
                "failure": 0, "hashmatch": 0,
                "failure_nodes": {
                    "192.168.0.1": 0,
                    "192.168.0.2": 0},
                "no_change": 358, "remote_merge": 0,
                "remove": 0, "rsync": 0,
                "start": 5.5, "success": 358,
                "ts_repl": 0},
            "replication_last": 1357969645.25}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_replication_info('container')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['replication_time', 'replication_stats',
                             'replication_last'],
                             '/var/cache/swift/container.recon'), {})])
        self.assertEqual(rv, {
            "replication_time": 200.0,
            "replication_stats": {
                "attempted": 179, "diff": 0,
                "diff_capped": 0, "empty": 0,
                "failure": 0, "hashmatch": 0,
                "failure_nodes": {
                    "192.168.0.1": 0,
                    "192.168.0.2": 0},
                "no_change": 358, "remote_merge": 0,
                "remove": 0, "rsync": 0,
                "start": 5.5, "success": 358,
                "ts_repl": 0},
            "replication_last": 1357969645.25})

    def test_get_replication_object(self):
        from_cache_response = {
            "replication_time": 0.2615511417388916,
            "replication_stats": {
                "attempted": 179,
                "failure": 0, "hashmatch": 0,
                "failure_nodes": {
                    "192.168.0.1": 0,
                    "192.168.0.2": 0},
                "remove": 0, "rsync": 0,
                "start": 1333044050.855202, "success": 358},
            "replication_last": 1357969645.25,
            "object_replication_time": 0.2615511417388916,
            "object_replication_last": 1357969645.25}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_replication_info('object')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['replication_time', 'replication_stats',
                             'replication_last', 'object_replication_time',
                             'object_replication_last'],
                             '/var/cache/swift/object.recon'), {})])
        self.assertEqual(rv, {
            "replication_time": 0.2615511417388916,
            "replication_stats": {
                "attempted": 179,
                "failure": 0, "hashmatch": 0,
                "failure_nodes": {
                    "192.168.0.1": 0,
                    "192.168.0.2": 0},
                "remove": 0, "rsync": 0,
                "start": 1333044050.855202, "success": 358},
            "replication_last": 1357969645.25,
            "object_replication_time": 0.2615511417388916,
            "object_replication_last": 1357969645.25})

    def test_get_replication_info_unrecognized(self):
        rv = self.app.get_replication_info('unrecognized_recon_type')
        self.assertIsNone(rv)

    def test_get_updater_info_container(self):
        from_cache_response = {"container_updater_sweep": 18.476239919662476}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_updater_info('container')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['container_updater_sweep'],
                            '/var/cache/swift/container.recon'), {})])
        self.assertEqual(rv, {"container_updater_sweep": 18.476239919662476})

    def test_get_updater_info_object(self):
        from_cache_response = {"object_updater_sweep": 0.79848217964172363}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_updater_info('object')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['object_updater_sweep'],
                            '/var/cache/swift/object.recon'), {})])
        self.assertEqual(rv, {"object_updater_sweep": 0.79848217964172363})

    def test_get_updater_info_unrecognized(self):
        rv = self.app.get_updater_info('unrecognized_recon_type')
        self.assertIsNone(rv)

    def test_get_expirer_info_object(self):
        from_cache_response = {'object_expiration_pass': 0.79848217964172363,
                               'expired_last_pass': 99}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_expirer_info('object')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['object_expiration_pass', 'expired_last_pass'],
                            '/var/cache/swift/object.recon'), {})])
        self.assertEqual(rv, from_cache_response)

    def test_get_auditor_info_account(self):
        from_cache_response = {"account_auditor_pass_completed": 0.24,
                               "account_audits_failed": 0,
                               "account_audits_passed": 6,
                               "account_audits_since": "1333145374.1373529"}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_auditor_info('account')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['account_audits_passed',
                             'account_auditor_pass_completed',
                             'account_audits_since',
                             'account_audits_failed'],
                             '/var/cache/swift/account.recon'), {})])
        self.assertEqual(rv, {"account_auditor_pass_completed": 0.24,
                              "account_audits_failed": 0,
                              "account_audits_passed": 6,
                              "account_audits_since": "1333145374.1373529"})

    def test_get_auditor_info_container(self):
        from_cache_response = {"container_auditor_pass_completed": 0.24,
                               "container_audits_failed": 0,
                               "container_audits_passed": 6,
                               "container_audits_since": "1333145374.1373529"}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_auditor_info('container')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['container_audits_passed',
                             'container_auditor_pass_completed',
                             'container_audits_since',
                             'container_audits_failed'],
                             '/var/cache/swift/container.recon'), {})])
        self.assertEqual(rv, {"container_auditor_pass_completed": 0.24,
                              "container_audits_failed": 0,
                              "container_audits_passed": 6,
                              "container_audits_since": "1333145374.1373529"})

    def test_get_auditor_info_object(self):
        from_cache_response = {
            "object_auditor_stats_ALL": {
                "audit_time": 115.14418768882751,
                "bytes_processed": 234660,
                "completed": 115.4512460231781,
                "errors": 0,
                "files_processed": 2310,
                "quarantined": 0},
            "object_auditor_stats_ZBF": {
                "audit_time": 45.877294063568115,
                "bytes_processed": 0,
                "completed": 46.181446075439453,
                "errors": 0,
                "files_processed": 2310,
                "quarantined": 0}}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_auditor_info('object')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['object_auditor_stats_ALL',
                             'object_auditor_stats_ZBF'],
                             '/var/cache/swift/object.recon'), {})])
        self.assertEqual(rv, {
            "object_auditor_stats_ALL": {
                "audit_time": 115.14418768882751,
                "bytes_processed": 234660,
                "completed": 115.4512460231781,
                "errors": 0,
                "files_processed": 2310,
                "quarantined": 0},
            "object_auditor_stats_ZBF": {
                "audit_time": 45.877294063568115,
                "bytes_processed": 0,
                "completed": 46.181446075439453,
                "errors": 0,
                "files_processed": 2310,
                "quarantined": 0}})

    def test_get_auditor_info_object_parallel_once(self):
        from_cache_response = {
            "object_auditor_stats_ALL": {
                'disk1': {
                    "audit_time": 115.14418768882751,
                    "bytes_processed": 234660,
                    "completed": 115.4512460231781,
                    "errors": 0,
                    "files_processed": 2310,
                    "quarantined": 0},
                'disk2': {
                    "audit_time": 115,
                    "bytes_processed": 234660,
                    "completed": 115,
                    "errors": 0,
                    "files_processed": 2310,
                    "quarantined": 0}},
            "object_auditor_stats_ZBF": {'disk1disk2': {
                "audit_time": 45.877294063568115,
                "bytes_processed": 0,
                "completed": 46.181446075439453,
                "errors": 0,
                "files_processed": 2310,
                "quarantined": 0}}}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_auditor_info('object')
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['object_auditor_stats_ALL',
                             'object_auditor_stats_ZBF'],
                             '/var/cache/swift/object.recon'), {})])
        self.assertEqual(rv, {
            "object_auditor_stats_ALL": {
                'disk1': {
                    "audit_time": 115.14418768882751,
                    "bytes_processed": 234660,
                    "completed": 115.4512460231781,
                    "errors": 0,
                    "files_processed": 2310,
                    "quarantined": 0},
                'disk2': {
                    "audit_time": 115,
                    "bytes_processed": 234660,
                    "completed": 115,
                    "errors": 0,
                    "files_processed": 2310,
                    "quarantined": 0}},
            "object_auditor_stats_ZBF": {'disk1disk2': {
                "audit_time": 45.877294063568115,
                "bytes_processed": 0,
                "completed": 46.181446075439453,
                "errors": 0,
                "files_processed": 2310,
                "quarantined": 0}}})

    def test_get_auditor_info_unrecognized(self):
        rv = self.app.get_auditor_info('unrecognized_recon_type')
        self.assertIsNone(rv)

    def test_get_unmounted(self):
        unmounted_resp = [{'device': 'fakeone', 'mounted': False},
                          {'device': 'faketwo', 'mounted': False}]
        self.mockos.ls_output = ['fakeone', 'faketwo']
        self.mockos.isdir_output = True
        self.mockos.ismount_output = False
        rv = self.app.get_unmounted()
        self.assertEqual(self.mockos.listdir_calls, [(('/srv/node',), {})])
        self.assertEqual(self.mockos.isdir_calls,
                         [(('/srv/node/fakeone',), {}),
                          (('/srv/node/faketwo',), {})])
        self.assertEqual(rv, unmounted_resp)

    def test_get_unmounted_excludes_files(self):
        unmounted_resp = []
        self.mockos.ls_output = ['somerando.log']
        self.mockos.isdir_output = False
        self.mockos.ismount_output = False
        rv = self.app.get_unmounted()
        self.assertEqual(self.mockos.listdir_calls, [(('/srv/node',), {})])
        self.assertEqual(self.mockos.isdir_calls,
                         [(('/srv/node/somerando.log',), {})])
        self.assertEqual(rv, unmounted_resp)

    def test_get_unmounted_all_mounted(self):
        unmounted_resp = []
        self.mockos.ls_output = ['fakeone', 'faketwo']
        self.mockos.isdir_output = True
        self.mockos.ismount_output = True
        rv = self.app.get_unmounted()
        self.assertEqual(self.mockos.listdir_calls, [(('/srv/node',), {})])
        self.assertEqual(self.mockos.isdir_calls,
                         [(('/srv/node/fakeone',), {}),
                          (('/srv/node/faketwo',), {})])
        self.assertEqual(rv, unmounted_resp)

    def test_get_unmounted_checkmount_fail(self):
        unmounted_resp = [{'device': 'fakeone', 'mounted': 'brokendrive'}]
        self.mockos.ls_output = ['fakeone']
        self.mockos.isdir_output = True
        self.mockos.ismount_output = OSError('brokendrive')
        rv = self.app.get_unmounted()
        self.assertEqual(self.mockos.listdir_calls, [(('/srv/node',), {})])
        self.assertEqual(self.mockos.isdir_calls,
                         [(('/srv/node/fakeone',), {})])
        self.assertEqual(self.mockos.ismount_calls,
                         [(('/srv/node/fakeone',), {})])
        self.assertEqual(rv, unmounted_resp)

    def test_get_unmounted_no_mounts(self):

        def fake_checkmount_true(*args):
            return True

        unmounted_resp = []
        self.mockos.ls_output = []
        self.mockos.isdir_output = False
        self.mockos.ismount_output = False
        rv = self.app.get_unmounted()
        self.assertEqual(self.mockos.listdir_calls, [(('/srv/node',), {})])
        self.assertEqual(self.mockos.isdir_calls, [])
        self.assertEqual(rv, unmounted_resp)

    def test_get_diskusage(self):
        # posix.statvfs_result(f_bsize=4096, f_frsize=4096, f_blocks=1963185,
        #                      f_bfree=1113075, f_bavail=1013351,
        #                      f_files=498736,
        #                      f_ffree=397839, f_favail=397839, f_flag=0,
        #                      f_namemax=255)
        statvfs_content = (4096, 4096, 1963185, 1113075, 1013351, 498736,
                           397839, 397839, 0, 255)
        du_resp = [{'device': 'canhazdrive1', 'avail': 4150685696,
                    'mounted': True, 'used': 3890520064, 'size': 8041205760}]
        self.mockos.ls_output = ['canhazdrive1']
        self.mockos.isdir_output = True
        self.mockos.statvfs_output = statvfs_content
        self.mockos.ismount_output = True
        rv = self.app.get_diskusage()
        self.assertEqual(self.mockos.listdir_calls, [(('/srv/node',), {})])
        self.assertEqual(self.mockos.isdir_calls,
                         [(('/srv/node/canhazdrive1',), {})])
        self.assertEqual(self.mockos.statvfs_calls,
                         [(('/srv/node/canhazdrive1',), {})])
        self.assertEqual(rv, du_resp)

    def test_get_diskusage_excludes_files(self):
        du_resp = []
        self.mockos.ls_output = ['somerando.log']
        self.mockos.isdir_output = False
        rv = self.app.get_diskusage()
        self.assertEqual(self.mockos.isdir_calls,
                         [(('/srv/node/somerando.log',), {})])
        self.assertEqual(self.mockos.statvfs_calls, [])
        self.assertEqual(rv, du_resp)

    def test_get_diskusage_checkmount_fail(self):
        du_resp = [{'device': 'canhazdrive1', 'avail': '',
                    'mounted': 'brokendrive', 'used': '', 'size': ''}]
        self.mockos.ls_output = ['canhazdrive1']
        self.mockos.isdir_output = True
        self.mockos.ismount_output = OSError('brokendrive')
        rv = self.app.get_diskusage()
        self.assertEqual(self.mockos.listdir_calls, [(('/srv/node',), {})])
        self.assertEqual(self.mockos.isdir_calls,
                         [(('/srv/node/canhazdrive1',), {})])
        self.assertEqual(self.mockos.ismount_calls,
                         [(('/srv/node/canhazdrive1',), {})])
        self.assertEqual(rv, du_resp)

    @mock.patch("swift.common.middleware.recon.check_mount", fake_check_mount)
    def test_get_diskusage_oserror(self):
        du_resp = [{'device': 'canhazdrive1', 'avail': '',
                    'mounted': 'Input/Output Error', 'used': '', 'size': ''}]
        self.mockos.ls_output = ['canhazdrive1']
        self.mockos.isdir_output = True
        rv = self.app.get_diskusage()
        self.assertEqual(rv, du_resp)

    def test_get_quarantine_count(self):
        dirs = [['sda'], ['accounts', 'containers', 'objects', 'objects-1']]
        self.mockos.ismount_output = True

        def fake_lstat(*args, **kwargs):
            # posix.lstat_result(st_mode=1, st_ino=2, st_dev=3, st_nlink=4,
            #                    st_uid=5, st_gid=6, st_size=7, st_atime=8,
            #                    st_mtime=9, st_ctime=10)
            return stat_result((1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

        def fake_exists(*args, **kwargs):
            return True

        def fake_listdir(*args, **kwargs):
            return dirs.pop(0)

        with mock.patch("os.lstat", fake_lstat):
            with mock.patch("os.path.exists", fake_exists):
                with mock.patch("os.listdir", fake_listdir):
                    rv = self.app.get_quarantine_count()
        self.assertEqual(rv, {'objects': 4, 'accounts': 2, 'policies':
                              {'1': {'objects': 2}, '0': {'objects': 2}},
                              'containers': 2})

    def test_get_socket_info(self):
        sockstat_content = ['sockets: used 271',
                            'TCP: inuse 30 orphan 0 tw 0 alloc 31 mem 0',
                            'UDP: inuse 16 mem 4', 'UDPLITE: inuse 0',
                            'RAW: inuse 0', 'FRAG: inuse 0 memory 0',
                            '']
        oart = OpenAndReadTester(sockstat_content)
        self.app.get_socket_info(openr=oart.open)
        self.assertEqual(oart.open_calls, [
            (('/proc/net/sockstat', 'r'), {}),
            (('/proc/net/sockstat6', 'r'), {})])

    def test_get_driveaudit_info(self):
        from_cache_response = {'drive_audit_errors': 7}
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_driveaudit_error()
        self.assertEqual(self.fakecache.fakeout_calls,
                         [((['drive_audit_errors'],
                            '/var/cache/swift/drive.recon'), {})])
        self.assertEqual(rv, {'drive_audit_errors': 7})

    def test_get_time(self):
        def fake_time():
            return 1430000000.0

        with mock.patch("time.time", fake_time):
            now = fake_time()
            rv = self.app.get_time()
            self.assertEqual(rv, now)


class TestReconMiddleware(unittest.TestCase):

    def fake_list(self, path):
        return ['a', 'b']

    def setUp(self):
        self.frecon = FakeRecon()
        self.real_listdir = os.listdir
        os.listdir = self.fake_list
        self.app = recon.ReconMiddleware(FakeApp(), {'object_recon': "true"})
        self.real_app_get_device_info = self.app.get_device_info
        self.real_app_get_swift_conf_md5 = self.app.get_swift_conf_md5
        os.listdir = self.real_listdir
        # self.app.object_recon = True
        self.app.get_mem = self.frecon.fake_mem
        self.app.get_load = self.frecon.fake_load
        self.app.get_async_info = self.frecon.fake_async
        self.app.get_device_info = self.frecon.fake_get_device_info
        self.app.get_replication_info = self.frecon.fake_replication
        self.app.get_auditor_info = self.frecon.fake_auditor
        self.app.get_updater_info = self.frecon.fake_updater
        self.app.get_expirer_info = self.frecon.fake_expirer
        self.app.get_mounted = self.frecon.fake_mounted
        self.app.get_unmounted = self.frecon.fake_unmounted
        self.app.get_diskusage = self.frecon.fake_diskusage
        self.app.get_ring_md5 = self.frecon.fake_ringmd5
        self.app.get_swift_conf_md5 = self.frecon.fake_swiftconfmd5
        self.app.get_quarantine_count = self.frecon.fake_quarantined
        self.app.get_socket_info = self.frecon.fake_sockstat
        self.app.get_driveaudit_error = self.frecon.fake_driveaudit
        self.app.get_time = self.frecon.fake_time

    def test_recon_get_mem(self):
        get_mem_resp = [b'{"memtest": "1"}']
        req = Request.blank('/recon/mem', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_mem_resp)

    def test_recon_get_version(self):
        req = Request.blank('/recon/version',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [json.dumps({
            'version': swiftver}).encode('ascii')])

    def test_recon_get_load(self):
        get_load_resp = [b'{"loadtest": "1"}']
        req = Request.blank('/recon/load', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_load_resp)

    def test_recon_get_async(self):
        get_async_resp = [b'{"asynctest": "1"}']
        req = Request.blank('/recon/async', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_async_resp)

    def test_get_device_info(self):
        get_device_resp = [b'{"/srv/1/node": ["sdb1"]}']
        req = Request.blank('/recon/devices',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_device_resp)

    def test_recon_get_replication_notype(self):
        get_replication_resp = [b'{"replicationtest": "1"}']
        req = Request.blank('/recon/replication',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_replication_resp)
        self.assertEqual(self.frecon.fake_replication_rtype, 'object')
        self.frecon.fake_replication_rtype = None

    def test_recon_get_replication_all(self):
        get_replication_resp = [b'{"replicationtest": "1"}']
        # test account
        req = Request.blank('/recon/replication/account',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_replication_resp)
        self.assertEqual(self.frecon.fake_replication_rtype, 'account')
        self.frecon.fake_replication_rtype = None
        # test container
        req = Request.blank('/recon/replication/container',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_replication_resp)
        self.assertEqual(self.frecon.fake_replication_rtype, 'container')
        self.frecon.fake_replication_rtype = None
        # test object
        req = Request.blank('/recon/replication/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_replication_resp)
        self.assertEqual(self.frecon.fake_replication_rtype, 'object')
        self.frecon.fake_replication_rtype = None

    def test_recon_get_auditor_invalid(self):
        get_auditor_resp = [b'Invalid path: /recon/auditor/invalid']
        req = Request.blank('/recon/auditor/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_auditor_resp)

    def test_recon_get_auditor_notype(self):
        get_auditor_resp = [b'Invalid path: /recon/auditor']
        req = Request.blank('/recon/auditor',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_auditor_resp)

    def test_recon_get_auditor_all(self):
        get_auditor_resp = [b'{"auditortest": "1"}']
        req = Request.blank('/recon/auditor/account',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_auditor_resp)
        self.assertEqual(self.frecon.fake_auditor_rtype, 'account')
        self.frecon.fake_auditor_rtype = None
        req = Request.blank('/recon/auditor/container',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_auditor_resp)
        self.assertEqual(self.frecon.fake_auditor_rtype, 'container')
        self.frecon.fake_auditor_rtype = None
        req = Request.blank('/recon/auditor/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_auditor_resp)
        self.assertEqual(self.frecon.fake_auditor_rtype, 'object')
        self.frecon.fake_auditor_rtype = None

    def test_recon_get_updater_invalid(self):
        get_updater_resp = [b'Invalid path: /recon/updater/invalid']
        req = Request.blank('/recon/updater/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_updater_resp)

    def test_recon_get_updater_notype(self):
        get_updater_resp = [b'Invalid path: /recon/updater']
        req = Request.blank('/recon/updater',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_updater_resp)

    def test_recon_get_updater(self):
        get_updater_resp = [b'{"updatertest": "1"}']
        req = Request.blank('/recon/updater/container',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(self.frecon.fake_updater_rtype, 'container')
        self.frecon.fake_updater_rtype = None
        self.assertEqual(resp, get_updater_resp)
        req = Request.blank('/recon/updater/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_updater_resp)
        self.assertEqual(self.frecon.fake_updater_rtype, 'object')
        self.frecon.fake_updater_rtype = None

    def test_recon_get_expirer_invalid(self):
        get_updater_resp = [b'Invalid path: /recon/expirer/invalid']
        req = Request.blank('/recon/expirer/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_updater_resp)

    def test_recon_get_expirer_notype(self):
        get_updater_resp = [b'Invalid path: /recon/expirer']
        req = Request.blank('/recon/expirer',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_updater_resp)

    def test_recon_get_expirer_object(self):
        get_expirer_resp = [b'{"expirertest": "1"}']
        req = Request.blank('/recon/expirer/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_expirer_resp)
        self.assertEqual(self.frecon.fake_expirer_rtype, 'object')
        self.frecon.fake_updater_rtype = None

    def test_recon_get_mounted(self):
        get_mounted_resp = [b'{"mountedtest": "1"}']
        req = Request.blank('/recon/mounted',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_mounted_resp)

    def test_recon_get_unmounted(self):
        get_unmounted_resp = [b'{"unmountedtest": "1"}']
        self.app.get_unmounted = self.frecon.fake_unmounted
        req = Request.blank('/recon/unmounted',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_unmounted_resp)

    def test_recon_get_unmounted_empty(self):
        get_unmounted_resp = b'[]'
        self.app.get_unmounted = self.frecon.fake_unmounted_empty
        req = Request.blank('/recon/unmounted',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = b''.join(self.app(req.environ, start_response))
        self.assertEqual(resp, get_unmounted_resp)

    def test_recon_get_diskusage(self):
        get_diskusage_resp = [b'{"diskusagetest": "1"}']
        req = Request.blank('/recon/diskusage',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_diskusage_resp)

    def test_recon_get_ringmd5(self):
        get_ringmd5_resp = [b'{"ringmd5test": "1"}']
        req = Request.blank('/recon/ringmd5',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_ringmd5_resp)

    def test_recon_get_swiftconfmd5(self):
        get_swiftconfmd5_resp = [b'{"/etc/swift/swift.conf": "abcdef"}']
        req = Request.blank('/recon/swiftconfmd5',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_swiftconfmd5_resp)

    def test_recon_get_quarantined(self):
        get_quarantined_resp = [b'{"quarantinedtest": "1"}']
        req = Request.blank('/recon/quarantined',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_quarantined_resp)

    def test_recon_get_sockstat(self):
        get_sockstat_resp = [b'{"sockstattest": "1"}']
        req = Request.blank('/recon/sockstat',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_sockstat_resp)

    def test_recon_invalid_path(self):
        req = Request.blank('/recon/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'Invalid path: /recon/invalid'])

    def test_no_content(self):
        self.app.get_load = self.frecon.nocontent
        req = Request.blank('/recon/load', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, [b'Internal server error.'])

    def test_recon_pass(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, b'FAKE APP')

    def test_recon_get_driveaudit(self):
        get_driveaudit_resp = [b'{"driveaudittest": "1"}']
        req = Request.blank('/recon/driveaudit',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_driveaudit_resp)

    def test_recon_get_time(self):
        get_time_resp = [b'{"timetest": "1"}']
        req = Request.blank('/recon/time',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEqual(resp, get_time_resp)

    def test_get_device_info_function(self):
        """Test get_device_info function call success"""
        resp = self.app.get_device_info()
        self.assertEqual(['sdb1'], resp['/srv/1/node'])

    def test_get_device_info_fail(self):
        """Test get_device_info failure by failing os.listdir"""
        os.listdir = fail_os_listdir
        resp = self.real_app_get_device_info()
        os.listdir = self.real_listdir
        device_path = list(resp)[0]
        self.assertIsNone(resp[device_path])

    def test_get_swift_conf_md5(self):
        """Test get_swift_conf_md5 success"""
        resp = self.app.get_swift_conf_md5()
        self.assertEqual('abcdef', resp['/etc/swift/swift.conf'])

    def test_get_swift_conf_md5_fail(self):
        """Test get_swift_conf_md5 failure by failing file open"""
        with mock.patch('swift.common.middleware.recon.md5_hash_for_file',
                        side_effect=IOError):
            resp = self.real_app_get_swift_conf_md5()
        self.assertIsNone(resp['/etc/swift/swift.conf'])


if __name__ == '__main__':
    unittest.main()
