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
from webob import Request
from swift.common.middleware import recon
from unittest import TestCase
from contextlib import contextmanager
from posix import stat_result, statvfs_result
import os
import swift.common.constraints


class FakeApp(object):
    def __call__(self, env, start_response):
        return "FAKE APP"

def start_response(*args):
    pass

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



class TestReconSuccess(TestCase):

    def setUp(self):
        self.app = recon.ReconMiddleware(FakeApp(), {})
        self.mockos = MockOS()
        self.real_listdir = os.listdir
        self.real_path_exists = os.path.exists
        self.real_lstat = os.lstat
        self.real_statvfs = os.statvfs
        os.listdir = self.mockos.fake_listdir
        os.path.exists = self.mockos.fake_path_exists
        os.lstat = self.mockos.fake_lstat
        os.statvfs = self.mockos.fake_statvfs

    def tearDown(self):
        os.listdir = self.real_listdir
        os.path.exists = self.real_path_exists
        os.lstat = self.real_lstat
        os.statvfs = self.real_statvfs
        del self.mockos

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
        obj_recon_content = """{"object_replication_time": 200.0, "async_pending": 5}"""
        oart = OpenAndReadTester([obj_recon_content])
        rv = self.app.get_async_info(openr=oart.open)
        self.assertEquals(oart.read_calls, [((), {})])
        self.assertEquals(oart.open_calls, [(('/var/cache/swift/object.recon', 'r'), {})])
        self.assertEquals(rv, {'async_pending': 5})

    def test_get_async_info_empty_file(self):
        obj_recon_content = """{"object_replication_time": 200.0}"""
        oart = OpenAndReadTester([obj_recon_content])
        rv = self.app.get_async_info(openr=oart.open)
        self.assertEquals(oart.read_calls, [((), {})])
        self.assertEquals(oart.open_calls, [(('/var/cache/swift/object.recon', 'r'), {})])
        self.assertEquals(rv, {'async_pending': -1})

    def test_get_replication_info(self):
        obj_recon_content = """{"object_replication_time": 200.0, "async_pending": 5}"""
        oart = OpenAndReadTester([obj_recon_content])
        rv = self.app.get_replication_info(openr=oart.open)
        self.assertEquals(oart.read_calls, [((), {})])
        self.assertEquals(oart.open_calls, [(('/var/cache/swift/object.recon', 'r'), {})])
        self.assertEquals(rv, {'object_replication_time': 200.0})

    def test_get_replication_info_empty_file(self):
        obj_recon_content = """{"async_pending": 5}"""
        oart = OpenAndReadTester([obj_recon_content])
        rv = self.app.get_replication_info(openr=oart.open)
        self.assertEquals(oart.read_calls, [((), {})])
        self.assertEquals(oart.open_calls, [(('/var/cache/swift/object.recon', 'r'), {})])
        self.assertEquals(rv, {'object_replication_time': -1})

    def test_get_device_info(self):
        rv = self.app.get_device_info()
        self.assertEquals(rv, '/srv/node/')

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
        self.assertEquals(self.mockos.statvfs_calls,[(('/srv/node/canhazdrive1',), {})])
        self.assertEquals(rv, du_resp)

    def test_get_diskusage_checkmount_fail(self):
        du_resp = [{'device': 'canhazdrive1', 'avail': '',
                    'mounted': False, 'used': '', 'size': ''}]
        self.mockos.ls_output=['canhazdrive1']
        self.mockos.path_exists_output=False
        rv = self.app.get_diskusage()
        self.assertEquals(self.mockos.listdir_calls,[(('/srv/node/',), {})])
        self.assertEquals(self.mockos.path_exists_calls,[(('/srv/node/canhazdrive1',), {})])
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
        #todo verify parsed result of sockstat6
        #self.assertEquals(rv, {'time_wait': 0, 'tcp_in_use': 30, 'orphan': 0, 'tcp_mem_allocated_bytes': 0})

class FakeRecon(object):

    def fake_mem(self):
        return {'memtest': "1"}

    def fake_load(self):
        return {'loadtest': "1"}

    def fake_async(self):
        return {'asynctest': "1"}

    def fake_replication(self):
        return {'replicationtest': "1"}

    def fake_mounted(self):
        return {'mountedtest': "1"}

    def fake_unmounted(self):
        return {'unmountedtest': "1"}

    def fake_diskusage(self):
        return {'diskusagetest': "1"}

    def fake_ringmd5(self):
        return {'ringmd5test': "1"}

    def fake_quarantined(self):
        return {'quarantinedtest': "1"}

    def fake_sockstat(self):
        return {'sockstattest': "1"}

    def raise_IOError(self):
        raise IOError

    def raise_ValueError(self):
        raise ValueError

class TestHealthCheck(unittest.TestCase):

    def setUp(self):
        self.frecon = FakeRecon()
        self.app = recon.ReconMiddleware(FakeApp(), {})
        self.app.get_mem = self.frecon.fake_mem
        self.app.get_load = self.frecon.fake_load
        self.app.get_async_info = self.frecon.fake_async
        self.app.get_replication_info = self.frecon.fake_replication
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

    def test_recon_get_async_ioerror(self):
        orig = self.app.get_async_info
        self.app.get_async_info = self.frecon.raise_IOError
        req = Request.blank('/recon/async', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.app.get_async_info = orig
        self.assertEquals(resp, ['Internal server error.'])

    def test_recon_get_replication(self):
        get_replication_resp = ['{"replicationtest": "1"}']
        req = Request.blank('/recon/replication', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_replication_resp)

    def test_recon_get_replication_ioerror(self):
        orig = self.app.get_replication_info
        self.app.get_replication_info = self.frecon.raise_IOError
        req = Request.blank('/recon/replication', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.app.get_async_info = orig
        self.assertEquals(resp, ['Internal server error.'])

    def test_recon_get_mounted(self):
        get_mounted_resp = ['{"mountedtest": "1"}']
        req = Request.blank('/recon/mounted', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_mounted_resp)

    def test_recon_get_unmounted(self):
        get_unmounted_resp = ['{"unmountedtest": "1"}']
        req = Request.blank('/recon/unmounted', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_unmounted_resp)

    def test_recon_get_diskusage(self):
        get_diskusage_resp = ['{"diskusagetest": "1"}']
        req = Request.blank('/recon/diskusage', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_diskusage_resp)

    def test_recon_get_ringmd5(self):
        get_ringmd5_resp = ['{"ringmd5test": "1"}']
        req = Request.blank('/recon/ringmd5', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_ringmd5_resp)

    def test_recon_get_quarantined(self):
        get_quarantined_resp = ['{"quarantinedtest": "1"}']
        req = Request.blank('/recon/quarantined', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_quarantined_resp)

    def test_recon_get_sockstat(self):
        get_sockstat_resp = ['{"sockstattest": "1"}']
        req = Request.blank('/recon/sockstat', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, get_sockstat_resp)

    def test_recon_invalid_path(self):
        req = Request.blank('/recon/invalid', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, ['Invalid path: /recon/invalid'])

    def test_recon_failed_json_dumps(self):
        orig = self.app.get_replication_info
        self.app.get_replication_info = self.frecon.raise_ValueError
        req = Request.blank('/recon/replication', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.app.get_async_info = orig
        self.assertEquals(resp, ['Internal server error.'])

    def test_recon_pass(self):
        req = Request.blank('/', environ={'REQUEST_METHOD': 'GET'})
        resp = self.app(req.environ, start_response)
        self.assertEquals(resp, 'FAKE APP')

if __name__ == '__main__':
    unittest.main()
