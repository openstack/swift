# Copyright (c) 2010-2012 OpenStack, LLC.
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

import unittest
from unittest import TestCase
from contextlib import contextmanager
from posix import stat_result, statvfs_result
import os

import swift.common.constraints
from swift.common.swob import Request
from swift.common.middleware import recon


class FakeApp(object):
    def __call__(self, env, start_response):
        return "FAKE APP"

def start_response(*args):
    pass

class FakeFromCache(object):

    def __init__(self, out=None):
        self.fakeout = out
        self.fakeout_calls  = []

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

    def next(self):
        if self.index == self.out_len:
            raise StopIteration
        else:
            line = self.data[self.index]
            self.index += 1
        return line

    def read(self, *args, **kwargs):
        self.read_calls.append((args, kwargs))
        try:
            return self.output_iter.next()
        except StopIteration:
            return ''

    @contextmanager
    def open(self, *args, **kwargs):
        self.open_calls.append((args, kwargs))
        yield self

class MockOS(object):

    def __init__(self, ls_out=None, pe_out=None, statvfs_out=None,
                 lstat_out=(1, 1, 5, 4, 5, 5, 55, 55, 55, 55)):
        self.ls_output = ls_out
        self.path_exists_output = pe_out
        self.statvfs_output = statvfs_out
        self.lstat_output_tuple = lstat_out
        self.listdir_calls = []
        self.statvfs_calls = []
        self.path_exists_calls = []
        self.lstat_calls = []

    def fake_listdir(self, *args, **kwargs):
        self.listdir_calls.append((args, kwargs))
        return self.ls_output

    def fake_path_exists(self, *args, **kwargs):
        self.path_exists_calls.append((args, kwargs))
        return self.path_exists_output

    def fake_statvfs(self, *args, **kwargs):
        self.statvfs_calls.append((args, kwargs))
        return statvfs_result(self.statvfs_output)

    def fake_lstat(self, *args, **kwargs):
        self.lstat_calls.append((args, kwargs))
        return stat_result(self.lstat_output_tuple)


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

    def fake_no_unmounted(self):
        return []

    def fake_diskusage(self):
        return {'diskusagetest': "1"}

    def fake_ringmd5(self):
        return {'ringmd5test': "1"}

    def fake_quarantined(self):
        return {'quarantinedtest': "1"}

    def fake_sockstat(self):
        return {'sockstattest': "1"}

    def nocontent(self):
        return None

    def raise_IOError(self, *args, **kwargs):
        raise IOError

    def raise_ValueError(self, *args, **kwargs):
        raise ValueError

    def raise_Exception(self, *args, **kwargs):
        raise Exception

class TestReconSuccess(TestCase):

    def setUp(self):
        self.app = recon.ReconMiddleware(FakeApp(), {})
        self.mockos = MockOS()
        self.fakecache = FakeFromCache()
        self.real_listdir = os.listdir
        self.real_path_exists = os.path.exists
        self.real_lstat = os.lstat
        self.real_statvfs = os.statvfs
        os.listdir = self.mockos.fake_listdir
        os.path.exists = self.mockos.fake_path_exists
        os.lstat = self.mockos.fake_lstat
        os.statvfs = self.mockos.fake_statvfs
        self.real_from_cache = self.app._from_recon_cache
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache
        self.frecon = FakeRecon()

    def tearDown(self):
        os.listdir = self.real_listdir
        os.path.exists = self.real_path_exists
        os.lstat = self.real_lstat
        os.statvfs = self.real_statvfs
        del self.mockos
        self.app._from_recon_cache = self.real_from_cache
        del self.fakecache

    def test_from_recon_cache(self):
        oart = OpenAndReadTester(['{"notneeded": 5, "testkey1": "canhazio"}'])
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                         'test.cache', openr=oart.open)
        self.assertEquals(oart.read_calls, [((), {})])
        self.assertEquals(oart.open_calls, [(('test.cache', 'r'), {})])
        self.assertEquals(rv, {'notpresentkey': None, 'testkey1': 'canhazio'})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_from_recon_cache_ioerror(self):
        oart = self.frecon.raise_IOError
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                         'test.cache', openr=oart)
        self.assertEquals(rv, {'notpresentkey': None, 'testkey1': None})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_from_recon_cache_valueerror(self):
        oart = self.frecon.raise_ValueError
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                         'test.cache', openr=oart)
        self.assertEquals(rv, {'notpresentkey': None, 'testkey1': None})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_from_recon_cache_exception(self):
        oart = self.frecon.raise_Exception
        self.app._from_recon_cache = self.real_from_cache
        rv = self.app._from_recon_cache(['testkey1', 'notpresentkey'],
                                         'test.cache', openr=oart)
        self.assertEquals(rv, {'notpresentkey': None, 'testkey1': None})
        self.app._from_recon_cache = self.fakecache.fake_from_recon_cache

    def test_get_mounted(self):
        mounts_content = ['rootfs / rootfs rw 0 0',
                          'none /sys sysfs rw,nosuid,nodev,noexec,relatime 0 0',
                          'none /proc proc rw,nosuid,nodev,noexec,relatime 0 0',
                          'none /dev devtmpfs rw,relatime,size=248404k,nr_inodes=62101,mode=755 0 0',
                          'none /dev/pts devpts rw,nosuid,noexec,relatime,gid=5,mode=620,ptmxmode=000 0 0',
                          '/dev/disk/by-uuid/e5b143bd-9f31-49a7-b018-5e037dc59252 / ext4 rw,relatime,errors=remount-ro,barrier=1,data=ordered 0 0',
                          'none /sys/fs/fuse/connections fusectl rw,relatime 0 0',
                          'none /sys/kernel/debug debugfs rw,relatime 0 0',
                          'none /sys/kernel/security securityfs rw,relatime 0 0',
                          'none /dev/shm tmpfs rw,nosuid,nodev,relatime 0 0',
                          'none /var/run tmpfs rw,nosuid,relatime,mode=755 0 0',
                          'none /var/lock tmpfs rw,nosuid,nodev,noexec,relatime 0 0',
                          'none /lib/init/rw tmpfs rw,nosuid,relatime,mode=755 0 0',
                          '/dev/loop0 /mnt/sdb1 xfs rw,noatime,nodiratime,attr2,nobarrier,logbufs=8,noquota 0 0',
                          'rpc_pipefs /var/lib/nfs/rpc_pipefs rpc_pipefs rw,relatime 0 0',
                          'nfsd /proc/fs/nfsd nfsd rw,relatime 0 0',
                          'none /proc/fs/vmblock/mountPoint vmblock rw,relatime 0 0',
                          '']
        mounted_resp = [{'device': 'rootfs', 'path': '/'},
                        {'device': 'none', 'path': '/sys'},
                        {'device': 'none', 'path': '/proc'},
                        {'device': 'none', 'path': '/dev'},
                        {'device': 'none', 'path': '/dev/pts'},
                        {'device': '/dev/disk/by-uuid/e5b143bd-9f31-49a7-b018-5e037dc59252', 'path': '/'},
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
        self.assertEquals(oart.open_calls, [(('/proc/mounts', 'r'), {})])
        self.assertEquals(rv, mounted_resp)

    def test_get_load(self):
        oart = OpenAndReadTester(['0.03 0.03 0.00 1/220 16306'])
        rv = self.app.get_load(openr=oart.open)
        self.assertEquals(oart.read_calls, [((), {})])
        self.assertEquals(oart.open_calls, [(('/proc/loadavg', 'r'), {})])
        self.assertEquals(rv, {'5m': 0.029999999999999999, '15m': 0.0,
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
        self.assertEquals(oart.open_calls, [(('/proc/meminfo', 'r'), {})])
        self.assertEquals(rv, meminfo_resp)

    def test_get_async_info(self):
        from_cache_response = {'async_pending': 5}
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_async_info()
        self.assertEquals(rv, {'async_pending': 5})

    def test_get_replication_info_account(self):
        from_cache_response = {"replication_stats": {
                                    "attempted": 1, "diff": 0,
                                    "diff_capped": 0, "empty": 0,
                                    "failure": 0, "hashmatch": 0,
                                    "no_change": 2, "remote_merge": 0,
                                    "remove": 0, "rsync": 0,
                                    "start": 1333044050.855202,
                                    "success": 2, "ts_repl": 0 },
                               "replication_time": 0.2615511417388916,
                               "replication_last": 1357969645.25}
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_replication_info('account')
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['replication_time', 'replication_stats',
                                'replication_last'],
                                '/var/cache/swift/account.recon'), {})])
        self.assertEquals(rv, {"replication_stats": {
                                    "attempted": 1, "diff": 0,
                                    "diff_capped": 0, "empty": 0,
                                    "failure": 0, "hashmatch": 0,
                                    "no_change": 2, "remote_merge": 0,
                                    "remove": 0, "rsync": 0,
                                    "start": 1333044050.855202,
                                    "success": 2, "ts_repl": 0 },
                                "replication_time": 0.2615511417388916,
                                "replication_last": 1357969645.25})

    def test_get_replication_info_container(self):
        from_cache_response = {"replication_time": 200.0,
                               "replication_stats": {
                                    "attempted": 179, "diff": 0,
                                    "diff_capped": 0, "empty": 0,
                                    "failure": 0, "hashmatch": 0,
                                    "no_change": 358, "remote_merge": 0,
                                    "remove": 0, "rsync": 0,
                                    "start": 5.5, "success": 358,
                                    "ts_repl": 0},
                               "replication_last": 1357969645.25}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_replication_info('container')
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['replication_time', 'replication_stats',
                                'replication_last'],
                                '/var/cache/swift/container.recon'), {})])
        self.assertEquals(rv, {"replication_time": 200.0,
                               "replication_stats": {
                                    "attempted": 179, "diff": 0,
                                    "diff_capped": 0, "empty": 0,
                                    "failure": 0, "hashmatch": 0,
                                    "no_change": 358, "remote_merge": 0,
                                    "remove": 0, "rsync": 0,
                                    "start": 5.5, "success": 358,
                                    "ts_repl": 0},
                               "replication_last": 1357969645.25})

    def test_get_replication_object(self):
        from_cache_response = {"object_replication_time": 200.0,
                               "object_replication_last": 1357962809.15}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_replication_info('object')
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['object_replication_time',
                                'object_replication_last'],
                                '/var/cache/swift/object.recon'), {})])
        self.assertEquals(rv, {'object_replication_time': 200.0,
                               'object_replication_last': 1357962809.15})

    def test_get_updater_info_container(self):
        from_cache_response = {"container_updater_sweep": 18.476239919662476}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_updater_info('container')
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['container_updater_sweep'],
                            '/var/cache/swift/container.recon'), {})])
        self.assertEquals(rv, {"container_updater_sweep": 18.476239919662476})

    def test_get_updater_info_object(self):
        from_cache_response = {"object_updater_sweep": 0.79848217964172363}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_updater_info('object')
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['object_updater_sweep'],
                            '/var/cache/swift/object.recon'), {})])
        self.assertEquals(rv, {"object_updater_sweep": 0.79848217964172363})

    def test_get_auditor_info_account(self):
        from_cache_response = {"account_auditor_pass_completed": 0.24,
                               "account_audits_failed": 0,
                               "account_audits_passed": 6,
                               "account_audits_since": "1333145374.1373529"}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_auditor_info('account')
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['account_audits_passed',
                                'account_auditor_pass_completed',
                                'account_audits_since',
                                'account_audits_failed'],
                                '/var/cache/swift/account.recon'), {})])
        self.assertEquals(rv, {"account_auditor_pass_completed": 0.24,
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
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['container_audits_passed',
                                'container_auditor_pass_completed',
                                'container_audits_since',
                                'container_audits_failed'],
                                '/var/cache/swift/container.recon'), {})])
        self.assertEquals(rv, {"container_auditor_pass_completed": 0.24,
                               "container_audits_failed": 0,
                               "container_audits_passed": 6,
                               "container_audits_since": "1333145374.1373529"})

    def test_get_auditor_info_object(self):
        from_cache_response = {"object_auditor_stats_ALL": {
                                    "audit_time": 115.14418768882751,
                                    "bytes_processed": 234660,
                                    "completed": 115.4512460231781,
                                    "errors": 0,
                                    "files_processed": 2310,
                                    "quarantined": 0 },
                                "object_auditor_stats_ZBF": {
                                    "audit_time": 45.877294063568115,
                                    "bytes_processed": 0,
                                    "completed": 46.181446075439453,
                                    "errors": 0,
                                    "files_processed": 2310,
                                    "quarantined": 0 }}
        self.fakecache.fakeout_calls = []
        self.fakecache.fakeout = from_cache_response
        rv = self.app.get_auditor_info('object')
        self.assertEquals(self.fakecache.fakeout_calls,
                            [((['object_auditor_stats_ALL',
                                'object_auditor_stats_ZBF'],
                            '/var/cache/swift/object.recon'), {})])
        self.assertEquals(rv, {"object_auditor_stats_ALL": {
                                    "audit_time": 115.14418768882751,
                                    "bytes_processed": 234660,
                                    "completed": 115.4512460231781,
                                    "errors": 0,
                                    "files_processed": 2310,
                                    "quarantined": 0 },
                                "object_auditor_stats_ZBF": {
                                    "audit_time": 45.877294063568115,
                                    "bytes_processed": 0,
                                    "completed": 46.181446075439453,
                                    "errors": 0,
                                    "files_processed": 2310,
                                    "quarantined": 0 }})

    def test_get_unmounted(self):

        def fake_checkmount_true(*args):
            return True

        unmounted_resp = [{'device': 'fakeone', 'mounted': False},
                          {'device': 'faketwo', 'mounted': False}]
        self.mockos.ls_output=['fakeone', 'faketwo']
        self.mockos.path_exists_output=False
        real_checkmount = swift.common.constraints.check_mount
        swift.common.constraints.check_mount = fake_checkmount_true
        rv = self.app.get_unmounted()
        swift.common.constraints.check_mount = real_checkmount
        self.assertEquals(self.mockos.listdir_calls, [(('/srv/node/',), {})])
        self.assertEquals(rv, unmounted_resp)

    def test_no_get_unmounted(self):

        def fake_checkmount_true(*args):
            return True

        unmounted_resp = []
        self.mockos.ls_output=[]
        self.mockos.path_exists_output=False
        real_checkmount = swift.common.constraints.check_mount
        swift.common.constraints.check_mount = fake_checkmount_true
        rv = self.app.get_unmounted()
        swift.common.constraints.check_mount = real_checkmount
        self.assertEquals(self.mockos.listdir_calls, [(('/srv/node/',), {})])
        self.assertEquals(rv, unmounted_resp)

    def test_get_diskusage(self):
        #posix.statvfs_result(f_bsize=4096, f_frsize=4096, f_blocks=1963185,
        #                     f_bfree=1113075, f_bavail=1013351, f_files=498736,
        #                     f_ffree=397839, f_favail=397839, f_flag=0,
        #                     f_namemax=255)
        statvfs_content=(4096, 4096, 1963185, 1113075, 1013351, 498736, 397839,
                         397839, 0, 255)
        du_resp = [{'device': 'canhazdrive1', 'avail': 4150685696,
                    'mounted': True, 'used': 3890520064, 'size': 8041205760}]
        self.mockos.ls_output=['canhazdrive1']
        self.mockos.statvfs_output=statvfs_content
        self.mockos.path_exists_output=True
        rv = self.app.get_diskusage()
        self.assertEquals(self.mockos.statvfs_calls,
                            [(('/srv/node/canhazdrive1',), {})])
        self.assertEquals(rv, du_resp)

    def test_get_diskusage_checkmount_fail(self):
        du_resp = [{'device': 'canhazdrive1', 'avail': '',
                    'mounted': False, 'used': '', 'size': ''}]
        self.mockos.ls_output=['canhazdrive1']
        self.mockos.path_exists_output=False
        rv = self.app.get_diskusage()
        self.assertEquals(self.mockos.listdir_calls,[(('/srv/node/',), {})])
        self.assertEquals(self.mockos.path_exists_calls,
                            [(('/srv/node/canhazdrive1',), {})])
        self.assertEquals(rv, du_resp)

    def test_get_quarantine_count(self):
        #posix.lstat_result(st_mode=1, st_ino=2, st_dev=3, st_nlink=4,
        #                   st_uid=5, st_gid=6, st_size=7, st_atime=8,
        #                   st_mtime=9, st_ctime=10)
        lstat_content = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        self.mockos.ls_output=['sda']
        self.mockos.path_exists_output=True
        self.mockos.lstat_output=lstat_content
        rv = self.app.get_quarantine_count()
        self.assertEquals(rv, {'objects': 2, 'accounts': 2, 'containers': 2})

    def test_get_socket_info(self):
        sockstat_content = ['sockets: used 271',
                            'TCP: inuse 30 orphan 0 tw 0 alloc 31 mem 0',
                            'UDP: inuse 16 mem 4', 'UDPLITE: inuse 0',
                            'RAW: inuse 0', 'FRAG: inuse 0 memory 0',
                            '']
        sockstat6_content = ['TCP6: inuse 1',
                             'UDP6: inuse 3',
                             'UDPLITE6: inuse 0',
                             'RAW6: inuse 0',
                             'FRAG6: inuse 0 memory 0',
                             '']
        oart = OpenAndReadTester(sockstat_content)
        rv = self.app.get_socket_info(openr=oart.open)
        self.assertEquals(oart.open_calls, [(('/proc/net/sockstat', 'r'), {}),
                                            (('/proc/net/sockstat6', 'r'), {})])

class TestReconMiddleware(unittest.TestCase):

    def setUp(self):
        self.frecon = FakeRecon()
        self.app = recon.ReconMiddleware(FakeApp(), {'object_recon': "true"})
        #self.app.object_recon = True
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
        self.app.get_quarantine_count = self.frecon.fake_quarantined
        self.app.get_socket_info = self.frecon.fake_sockstat

    def test_recon_get_mem(self):
        get_mem_resp = ['{"memtest": "1"}']
        req = Request.blank('/recon/mem', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_mem_resp)

    def test_recon_get_load(self):
        get_load_resp = ['{"loadtest": "1"}']
        req = Request.blank('/recon/load', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_load_resp)

    def test_recon_get_async(self):
        get_async_resp = ['{"asynctest": "1"}']
        req = Request.blank('/recon/async', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_async_resp)

    def test_get_device_info(self):
        get_device_resp = ['{"/srv/1/node": ["sdb1"]}']
        req = Request.blank('/recon/devices', 
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_device_resp)

    def test_recon_get_replication_notype(self):
        get_replication_resp = ['{"replicationtest": "1"}']
        req = Request.blank('/recon/replication',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_replication_resp)
        self.assertEquals(self.frecon.fake_replication_rtype, 'object')
        self.frecon.fake_replication_rtype = None

    def test_recon_get_replication_all(self):
        get_replication_resp = ['{"replicationtest": "1"}']
        #test account
        req = Request.blank('/recon/replication/account',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_replication_resp)
        self.assertEquals(self.frecon.fake_replication_rtype, 'account')
        self.frecon.fake_replication_rtype = None
        #test container
        req = Request.blank('/recon/replication/container',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_replication_resp)
        self.assertEquals(self.frecon.fake_replication_rtype, 'container')
        self.frecon.fake_replication_rtype = None
        #test object
        req = Request.blank('/recon/replication/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_replication_resp)
        self.assertEquals(self.frecon.fake_replication_rtype, 'object')
        self.frecon.fake_replication_rtype = None

    def test_recon_get_auditor_invalid(self):
        get_auditor_resp = ['Invalid path: /recon/auditor/invalid']
        req = Request.blank('/recon/auditor/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_auditor_resp)

    def test_recon_get_auditor_notype(self):
        get_auditor_resp = ['Invalid path: /recon/auditor']
        req = Request.blank('/recon/auditor',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_auditor_resp)

    def test_recon_get_auditor_all(self):
        get_auditor_resp = ['{"auditortest": "1"}']
        req = Request.blank('/recon/auditor/account',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_auditor_resp)
        self.assertEquals(self.frecon.fake_auditor_rtype, 'account')
        self.frecon.fake_auditor_rtype = None
        req = Request.blank('/recon/auditor/container',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_auditor_resp)
        self.assertEquals(self.frecon.fake_auditor_rtype, 'container')
        self.frecon.fake_auditor_rtype = None
        req = Request.blank('/recon/auditor/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_auditor_resp)
        self.assertEquals(self.frecon.fake_auditor_rtype, 'object')
        self.frecon.fake_auditor_rtype = None

    def test_recon_get_updater_invalid(self):
        get_updater_resp = ['Invalid path: /recon/updater/invalid']
        req = Request.blank('/recon/updater/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_updater_resp)

    def test_recon_get_updater_notype(self):
        get_updater_resp = ['Invalid path: /recon/updater']
        req = Request.blank('/recon/updater',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_updater_resp)

    def test_recon_get_updater(self):
        get_updater_resp = ['{"updatertest": "1"}']
        req = Request.blank('/recon/updater/container',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(self.frecon.fake_updater_rtype, 'container')
        self.frecon.fake_updater_rtype = None
        self.assertEquals(resp, get_updater_resp)
        req = Request.blank('/recon/updater/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_updater_resp)
        self.assertEquals(self.frecon.fake_updater_rtype, 'object')
        self.frecon.fake_updater_rtype = None

    def test_recon_get_expirer_invalid(self):
        get_updater_resp = ['Invalid path: /recon/expirer/invalid']
        req = Request.blank('/recon/expirer/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_updater_resp)

    def test_recon_get_expirer_notype(self):
        get_updater_resp = ['Invalid path: /recon/expirer']
        req = Request.blank('/recon/expirer',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_updater_resp)

    def test_recon_get_expirer_object(self):
        get_expirer_resp = ['{"expirertest": "1"}']
        req = Request.blank('/recon/expirer/object',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_expirer_resp)
        self.assertEquals(self.frecon.fake_expirer_rtype, 'object')
        self.frecon.fake_updater_rtype = None

    def test_recon_get_mounted(self):
        get_mounted_resp = ['{"mountedtest": "1"}']
        req = Request.blank('/recon/mounted',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_mounted_resp)

    def test_recon_get_unmounted(self):
        get_unmounted_resp = ['{"unmountedtest": "1"}']
        self.app.get_unmounted = self.frecon.fake_unmounted
        req = Request.blank('/recon/unmounted',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_unmounted_resp)
    
    def test_recon_no_get_unmounted(self):
        get_unmounted_resp = '[]'
        self.app.get_unmounted = self.frecon.fake_no_unmounted
        req = Request.blank('/recon/unmounted',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = ''.join(self.app(req.environ, start_response))
        self.assertEquals(resp, get_unmounted_resp)

    def test_recon_get_diskusage(self):
        get_diskusage_resp = ['{"diskusagetest": "1"}']
        req = Request.blank('/recon/diskusage',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_diskusage_resp)

    def test_recon_get_ringmd5(self):
        get_ringmd5_resp = ['{"ringmd5test": "1"}']
        req = Request.blank('/recon/ringmd5',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_ringmd5_resp)

    def test_recon_get_quarantined(self):
        get_quarantined_resp = ['{"quarantinedtest": "1"}']
        req = Request.blank('/recon/quarantined',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_quarantined_resp)

    def test_recon_get_sockstat(self):
        get_sockstat_resp = ['{"sockstattest": "1"}']
        req = Request.blank('/recon/sockstat',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_sockstat_resp)

    def test_recon_invalid_path(self):
        req = Request.blank('/recon/invalid',
                            environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, ['Invalid path: /recon/invalid'])

    def test_no_content(self):
        self.app.get_load = self.frecon.nocontent
        req = Request.blank('/recon/load', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, ['Internal server error.'])

    def test_recon_pass(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

if __name__ == '__main__':
    unittest.main()
