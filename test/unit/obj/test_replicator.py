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

import unittest
import os
import mock
from gzip import GzipFile
from shutil import rmtree
import cPickle as pickle
import time
import tempfile
from contextlib import contextmanager, closing

from eventlet.green import subprocess
from eventlet import Timeout, tpool

from test.unit import FakeLogger, patch_policies
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, normalize_timestamp
from swift.common import ring
from swift.obj import diskfile, replicator as object_replicator
from swift.common.storage_policy import StoragePolicy, POLICIES


def _ips():
    return ['127.0.0.0']
object_replicator.whataremyips = _ips


def mock_http_connect(status):

    class FakeConn(object):

        def __init__(self, status, *args, **kwargs):
            self.status = status
            self.reason = 'Fake'
            self.host = args[0]
            self.port = args[1]
            self.method = args[4]
            self.path = args[5]
            self.with_exc = False
            self.headers = kwargs.get('headers', {})

        def getresponse(self):
            if self.with_exc:
                raise Exception('test')
            return self

        def getheader(self, header):
            return self.headers[header]

        def read(self, amt=None):
            return pickle.dumps({})

        def close(self):
            return
    return lambda *args, **kwargs: FakeConn(status, *args, **kwargs)

process_errors = []


class MockProcess(object):
    ret_code = None
    ret_log = None
    check_args = None

    class Stream(object):

        def read(self):
            return MockProcess.ret_log.next()

    def __init__(self, *args, **kwargs):
        targs = MockProcess.check_args.next()
        for targ in targs:
            if targ not in args[0]:
                process_errors.append("Invalid: %s not in %s" % (targ,
                                                                 args))
        self.stdout = self.Stream()

    def wait(self):
        return self.ret_code.next()


@contextmanager
def _mock_process(ret):
    orig_process = subprocess.Popen
    MockProcess.ret_code = (i[0] for i in ret)
    MockProcess.ret_log = (i[1] for i in ret)
    MockProcess.check_args = (i[2] for i in ret)
    object_replicator.subprocess.Popen = MockProcess
    yield
    object_replicator.subprocess.Popen = orig_process


def _create_test_rings(path):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2, 3, 4, 5, 6],
        [1, 2, 3, 0, 5, 6, 4],
        [2, 3, 0, 1, 6, 4, 5],
    ]
    intended_devs = [
        {'id': 0, 'device': 'sda', 'zone': 0, 'ip': '127.0.0.0', 'port': 6000},
        {'id': 1, 'device': 'sda', 'zone': 1, 'ip': '127.0.0.1', 'port': 6000},
        {'id': 2, 'device': 'sda', 'zone': 2, 'ip': '127.0.0.2', 'port': 6000},
        {'id': 3, 'device': 'sda', 'zone': 4, 'ip': '127.0.0.3', 'port': 6000},
        {'id': 4, 'device': 'sda', 'zone': 5, 'ip': '127.0.0.4', 'port': 6000},
        {'id': 5, 'device': 'sda', 'zone': 6,
         'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6000},
        {'id': 6, 'device': 'sda', 'zone': 7,
         'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'port': 6000},
    ]
    intended_part_shift = 30
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift),
            f)

    testgz = os.path.join(path, 'object-1.ring.gz')
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id,
                          intended_devs, intended_part_shift),
            f)
    return


@patch_policies([StoragePolicy(0, 'zero', False),
                StoragePolicy(1, 'one', True)])
class TestObjectReplicator(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        # Setup a test ring (stolen from common/test_ring.py)
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        os.mkdir(os.path.join(self.devices, 'sda'))
        self.objects = os.path.join(self.devices, 'sda',
                                    diskfile.get_data_dir(0))
        self.objects_1 = os.path.join(self.devices, 'sda',
                                      diskfile.get_data_dir(1))
        os.mkdir(self.objects)
        os.mkdir(self.objects_1)
        self.parts = {}
        self.parts_1 = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(os.path.join(self.objects, part))
            self.parts_1[part] = os.path.join(self.objects_1, part)
            os.mkdir(os.path.join(self.objects_1, part))
        _create_test_rings(self.testdir)
        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')
        self.replicator = object_replicator.ObjectReplicator(self.conf)
        self.replicator.logger = FakeLogger()
        self.df_mgr = diskfile.DiskFileManager(self.conf,
                                               self.replicator.logger)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_run_once(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf)
        was_connector = object_replicator.http_connect
        object_replicator.http_connect = mock_http_connect(200)
        cur_part = '0'
        df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o',
                                      policy_idx=0)
        mkdirs(df._datadir)
        f = open(os.path.join(df._datadir,
                              normalize_timestamp(time.time()) + '.data'),
                 'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, cur_part, data_dir)
        process_arg_checker = []
        ring = replicator.get_object_ring(0)
        nodes = [node for node in
                 ring.get_part_nodes(int(cur_part))
                 if node['ip'] not in _ips()]
        for node in nodes:
            rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], cur_part)
            process_arg_checker.append(
                (0, '', ['rsync', whole_path_from, rsync_mod]))
        with _mock_process(process_arg_checker):
            replicator.run_once()
        self.assertFalse(process_errors)
        object_replicator.http_connect = was_connector

    # policy 1
    def test_run_once_1(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf)
        was_connector = object_replicator.http_connect
        object_replicator.http_connect = mock_http_connect(200)
        cur_part = '0'
        df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o',
                                      policy_idx=1)
        mkdirs(df._datadir)
        f = open(os.path.join(df._datadir,
                              normalize_timestamp(time.time()) + '.data'),
                 'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects_1, cur_part, data_dir)
        process_arg_checker = []
        ring = replicator.get_object_ring(1)
        nodes = [node for node in
                 ring.get_part_nodes(int(cur_part))
                 if node['ip'] not in _ips()]
        for node in nodes:
            rsync_mod = '%s::object/sda/objects-1/%s' % (node['ip'], cur_part)
            process_arg_checker.append(
                (0, '', ['rsync', whole_path_from, rsync_mod]))
        with _mock_process(process_arg_checker):
            replicator.run_once()
        self.assertFalse(process_errors)
        object_replicator.http_connect = was_connector

    def test_check_ring(self):
        for pol in POLICIES:
            obj_ring = self.replicator.get_object_ring(pol.idx)
            self.assertTrue(self.replicator.check_ring(obj_ring))
            orig_check = self.replicator.next_check
            self.replicator.next_check = orig_check - 30
            self.assertTrue(self.replicator.check_ring(obj_ring))
            self.replicator.next_check = orig_check
            orig_ring_time = obj_ring._mtime
            obj_ring._mtime = orig_ring_time - 30
            self.assertTrue(self.replicator.check_ring(obj_ring))
            self.replicator.next_check = orig_check - 30
            self.assertFalse(self.replicator.check_ring(obj_ring))

    def test_collect_jobs_mkdirs_error(self):

        def blowup_mkdirs(path):
            raise OSError('Ow!')

        mkdirs_orig = object_replicator.mkdirs
        try:
            rmtree(self.objects, ignore_errors=1)
            object_replicator.mkdirs = blowup_mkdirs
            self.replicator.collect_jobs()
            self.assertTrue('exception' in self.replicator.logger.log_dict)
            self.assertEquals(
                len(self.replicator.logger.log_dict['exception']), 1)
            exc_args, exc_kwargs, exc_str = \
                self.replicator.logger.log_dict['exception'][0]
            self.assertEquals(len(exc_args), 1)
            self.assertTrue(exc_args[0].startswith('ERROR creating '))
            self.assertEquals(exc_kwargs, {})
            self.assertEquals(exc_str, 'Ow!')
        finally:
            object_replicator.mkdirs = mkdirs_orig

    def test_collect_jobs(self):
        jobs = self.replicator.collect_jobs()
        jobs_to_delete = [j for j in jobs if j['delete']]
        jobs_by_pol_part = {}
        for job in jobs:
            jobs_by_pol_part[str(job['policy_idx']) + job['partition']] = job
        self.assertEquals(len(jobs_to_delete), 2)
        self.assertTrue('1', jobs_to_delete[0]['partition'])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['00']['nodes']], [1, 2])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['01']['nodes']],
            [1, 2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['02']['nodes']], [2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['03']['nodes']], [3, 1])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['10']['nodes']], [1, 2])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['11']['nodes']],
            [1, 2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['12']['nodes']], [2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['13']['nodes']], [3, 1])
        for part in ['00', '01', '02', '03', ]:
            for node in jobs_by_pol_part[part]['nodes']:
                self.assertEquals(node['device'], 'sda')
            self.assertEquals(jobs_by_pol_part[part]['path'],
                              os.path.join(self.objects, part[1:]))
        for part in ['10', '11', '12', '13', ]:
            for node in jobs_by_pol_part[part]['nodes']:
                self.assertEquals(node['device'], 'sda')
            self.assertEquals(jobs_by_pol_part[part]['path'],
                              os.path.join(self.objects_1, part[1:]))

    def test_collect_jobs_handoffs_first(self):
        self.replicator.handoffs_first = True
        jobs = self.replicator.collect_jobs()
        self.assertTrue(jobs[0]['delete'])
        self.assertEquals('1', jobs[0]['partition'])

    def test_collect_jobs_removes_zbf(self):
        """
        After running xfs_repair, a partition directory could become a
        zero-byte file.  If this happens, collect_jobs() should clean it up and
        *not* create a job which will hit an exception as it tries to listdir()
        a file.
        """
        # Surprise! Partition dir 1 is actually a zero-byte-file
        part_1_path = os.path.join(self.objects, '1')
        rmtree(part_1_path)
        with open(part_1_path, 'w'):
            pass
        self.assertTrue(os.path.isfile(part_1_path))  # sanity check
        part_1_path_1 = os.path.join(self.objects_1, '1')
        rmtree(part_1_path_1)
        with open(part_1_path_1, 'w'):
            pass
        self.assertTrue(os.path.isfile(part_1_path_1))  # sanity check
        jobs = self.replicator.collect_jobs()
        jobs_to_delete = [j for j in jobs if j['delete']]
        jobs_by_pol_part = {}
        for job in jobs:
            jobs_by_pol_part[str(job['policy_idx']) + job['partition']] = job
        self.assertEquals(len(jobs_to_delete), 0)
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['00']['nodes']], [1, 2])
        self.assertFalse('1' in jobs_by_pol_part)
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['02']['nodes']], [2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['03']['nodes']], [3, 1])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['10']['nodes']], [1, 2])
        self.assertFalse('1' in jobs_by_pol_part)
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['12']['nodes']], [2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_pol_part['13']['nodes']], [3, 1])
        for part in ['00', '02', '03']:
            for node in jobs_by_pol_part[part]['nodes']:
                self.assertEquals(node['device'], 'sda')
            self.assertEquals(jobs_by_pol_part[part]['path'],
                              os.path.join(self.objects, part[1:]))
        self.assertFalse(os.path.exists(part_1_path))
        expected = sorted(self.replicator.logger.log_dict['warning'])
        self.assertEquals(
            (('Removing partition directory which was a file: %s',
             part_1_path), {}), expected[1])
        # policy 1
        for part in ['10', '12', '13']:
            for node in jobs_by_pol_part[part]['nodes']:
                self.assertEquals(node['device'], 'sda')
            self.assertEquals(jobs_by_pol_part[part]['path'],
                              os.path.join(self.objects_1, part[1:]))
        self.assertFalse(os.path.exists(part_1_path_1))
        self.assertEquals(
            (('Removing partition directory which was a file: %s',
             part_1_path_1), {}), expected[0])

    def test_delete_partition(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o')
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.get_object_ring(0)
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for node in nodes:
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_1(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o',
                                          policy_idx=1)
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects_1, '1', data_dir)
            part_path = os.path.join(self.objects_1, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.get_object_ring(1)
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for node in nodes:
                rsync_mod = '%s::object/sda/objects-1/%s' % (node['ip'], 1)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_with_failures(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o')
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.get_object_ring(0)
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for i, node in enumerate(nodes):
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                if i == 0:
                    # force one of the rsync calls to fail
                    ret_code = 1
                else:
                    ret_code = 0
                process_arg_checker.append(
                    (ret_code, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            # The path should still exist
            self.assertTrue(os.access(part_path, os.F_OK))

    def test_delete_partition_with_handoff_delete(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            self.replicator.handoff_delete = 2
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o')
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.get_object_ring(0)
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for i, node in enumerate(nodes):
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                if i == 0:
                    # force one of the rsync calls to fail
                    ret_code = 1
                else:
                    ret_code = 0
                process_arg_checker.append(
                    (ret_code, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            self.assertFalse(os.access(part_path, os.F_OK))

    def test_delete_partition_with_handoff_delete_failures(self):
        with mock.patch('swift.obj.replicator.http_connect',
                        mock_http_connect(200)):
            self.replicator.handoff_delete = 2
            df = self.df_mgr.get_diskfile('sda', '1', 'a', 'c', 'o')
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, '1', data_dir)
            part_path = os.path.join(self.objects, '1')
            self.assertTrue(os.access(part_path, os.F_OK))
            ring = self.replicator.get_object_ring(0)
            nodes = [node for node in
                     ring.get_part_nodes(1)
                     if node['ip'] not in _ips()]
            process_arg_checker = []
            for i, node in enumerate(nodes):
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'], 1)
                if i in (0, 1):
                    # force two of the rsync calls to fail
                    ret_code = 1
                else:
                    ret_code = 0
                process_arg_checker.append(
                    (ret_code, '', ['rsync', whole_path_from, rsync_mod]))
            with _mock_process(process_arg_checker):
                self.replicator.replicate()
            # The file should still exist
            self.assertTrue(os.access(part_path, os.F_OK))

    def test_delete_partition_override_params(self):
        df = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o')
        mkdirs(df._datadir)
        part_path = os.path.join(self.objects, '1')
        self.assertTrue(os.access(part_path, os.F_OK))
        self.replicator.replicate(override_devices=['sdb'])
        self.assertTrue(os.access(part_path, os.F_OK))
        self.replicator.replicate(override_partitions=['9'])
        self.assertTrue(os.access(part_path, os.F_OK))
        self.replicator.replicate(override_devices=['sda'],
                                  override_partitions=['1'])
        self.assertFalse(os.access(part_path, os.F_OK))

    def test_run_once_recover_from_failure(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf)
        was_connector = object_replicator.http_connect
        try:
            object_replicator.http_connect = mock_http_connect(200)
            # Write some files into '1' and run replicate- they should be moved
            # to the other partitoins and then node should get deleted.
            cur_part = '1'
            df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o')
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, cur_part, data_dir)
            ring = replicator.get_object_ring(0)
            process_arg_checker = []
            nodes = [node for node in
                     ring.get_part_nodes(int(cur_part))
                     if node['ip'] not in _ips()]
            for node in nodes:
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'],
                                                           cur_part)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            self.assertTrue(os.access(os.path.join(self.objects,
                                                   '1', data_dir, ohash),
                                      os.F_OK))
            with _mock_process(process_arg_checker):
                replicator.run_once()
            self.assertFalse(process_errors)
            for i, result in [('0', True), ('1', False),
                              ('2', True), ('3', True)]:
                self.assertEquals(os.access(
                    os.path.join(self.objects,
                                 i, diskfile.HASH_FILE),
                    os.F_OK), result)
        finally:
            object_replicator.http_connect = was_connector

    def test_run_once_recover_from_timeout(self):
        conf = dict(swift_dir=self.testdir, devices=self.devices,
                    mount_check='false', timeout='300', stats_interval='1')
        replicator = object_replicator.ObjectReplicator(conf)
        was_connector = object_replicator.http_connect
        was_get_hashes = object_replicator.get_hashes
        was_execute = tpool.execute
        self.get_hash_count = 0
        try:

            def fake_get_hashes(*args, **kwargs):
                self.get_hash_count += 1
                if self.get_hash_count == 3:
                    # raise timeout on last call to get hashes
                    raise Timeout()
                return 2, {'abc': 'def'}

            def fake_exc(tester, *args, **kwargs):
                if 'Error syncing partition' in args[0]:
                    tester.i_failed = True

            self.i_failed = False
            object_replicator.http_connect = mock_http_connect(200)
            object_replicator.get_hashes = fake_get_hashes
            replicator.logger.exception = \
                lambda *args, **kwargs: fake_exc(self, *args, **kwargs)
            # Write some files into '1' and run replicate- they should be moved
            # to the other partitions and then node should get deleted.
            cur_part = '1'
            df = self.df_mgr.get_diskfile('sda', cur_part, 'a', 'c', 'o')
            mkdirs(df._datadir)
            f = open(os.path.join(df._datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, cur_part, data_dir)
            process_arg_checker = []
            ring = replicator.get_object_ring(0)
            nodes = [node for node in
                     ring.get_part_nodes(int(cur_part))
                     if node['ip'] not in _ips()]
            for node in nodes:
                rsync_mod = '%s::object/sda/objects/%s' % (node['ip'],
                                                           cur_part)
                process_arg_checker.append(
                    (0, '', ['rsync', whole_path_from, rsync_mod]))
            self.assertTrue(os.access(os.path.join(self.objects,
                                                   '1', data_dir, ohash),
                                      os.F_OK))
            with _mock_process(process_arg_checker):
                replicator.run_once()
            self.assertFalse(process_errors)
            self.assertFalse(self.i_failed)
        finally:
            object_replicator.http_connect = was_connector
            object_replicator.get_hashes = was_get_hashes
            tpool.execute = was_execute

    def test_run(self):
        with _mock_process([(0, '')] * 100):
            with mock.patch('swift.obj.replicator.http_connect',
                            mock_http_connect(200)):
                self.replicator.replicate()

    def test_run_withlog(self):
        with _mock_process([(0, "stuff in log")] * 100):
            with mock.patch('swift.obj.replicator.http_connect',
                            mock_http_connect(200)):
                self.replicator.replicate()

    def test_sync_just_calls_sync_method(self):
        self.replicator.sync_method = mock.MagicMock()
        self.replicator.sync('node', 'job', 'suffixes')
        self.replicator.sync_method.assert_called_once_with(
            'node', 'job', 'suffixes')

    @mock.patch('swift.obj.replicator.tpool_reraise', autospec=True)
    @mock.patch('swift.obj.replicator.http_connect', autospec=True)
    def test_update(self, mock_http, mock_tpool_reraise):

        def set_default(self):
            self.replicator.suffix_count = 0
            self.replicator.suffix_sync = 0
            self.replicator.suffix_hash = 0
            self.replicator.replication_count = 0
            self.replicator.partition_times = []

        self.headers = {'Content-Length': '0',
                        'user-agent': 'object-replicator %s' % os.getpid()}
        self.replicator.logger = mock_logger = mock.MagicMock()
        mock_tpool_reraise.return_value = (0, {})

        all_jobs = self.replicator.collect_jobs()
        jobs = [job for job in all_jobs if not job['delete']]

        mock_http.return_value = answer = mock.MagicMock()
        answer.getresponse.return_value = resp = mock.MagicMock()
        # Check uncorrect http_connect with status 507 and
        # count of attempts and call args
        resp.status = 507
        error = '%(ip)s/%(device)s responded as unmounted'
        expect = 'Error syncing partition'
        for job in jobs:
            set_default(self)
            ring = self.replicator.get_object_ring(job['policy_idx'])
            self.headers['X-Backend-Storage-Policy-Index'] = job['policy_idx']
            self.replicator.update(job)
            self.assertTrue(error in mock_logger.error.call_args[0][0])
            self.assertTrue(expect in mock_logger.exception.call_args[0][0])
            self.assertEquals(len(self.replicator.partition_times), 1)
            self.assertEquals(mock_http.call_count, len(ring._devs) - 1)
            reqs = []
            for node in job['nodes']:
                reqs.append(mock.call(node['ip'], node['port'], node['device'],
                                      job['partition'], 'REPLICATE', '',
                                      headers=self.headers))
            if job['partition'] == '0':
                self.assertEquals(self.replicator.suffix_hash, 0)
            mock_http.assert_has_calls(reqs, any_order=True)
            mock_http.reset_mock()
            mock_logger.reset_mock()

        # Check uncorrect http_connect with status 400 != HTTP_OK
        resp.status = 400
        error = 'Invalid response %(resp)s from %(ip)s'
        for job in jobs:
            set_default(self)
            self.replicator.update(job)
            self.assertTrue(error in mock_logger.error.call_args[0][0])
            self.assertEquals(len(self.replicator.partition_times), 1)
            mock_logger.reset_mock()

        # Check successful http_connection and exception with
        # uncorrect pickle.loads(resp.read())
        resp.status = 200
        expect = 'Error syncing with node:'
        for job in jobs:
            set_default(self)
            self.replicator.update(job)
            self.assertTrue(expect in mock_logger.exception.call_args[0][0])
            self.assertEquals(len(self.replicator.partition_times), 1)
            mock_logger.reset_mock()

        # Check successful http_connection and correct
        # pickle.loads(resp.read()) for non local node
        resp.status = 200
        local_job = None
        resp.read.return_value = pickle.dumps({})
        for job in jobs:
            set_default(self)
            # limit local job to policy 0 for simplicty
            if job['partition'] == '0' and job['policy_idx'] == 0:
                local_job = job.copy()
                continue
            self.replicator.update(job)
            self.assertEquals(mock_logger.exception.call_count, 0)
            self.assertEquals(mock_logger.error.call_count, 0)
            self.assertEquals(len(self.replicator.partition_times), 1)
            self.assertEquals(self.replicator.suffix_hash, 0)
            self.assertEquals(self.replicator.suffix_sync, 0)
            self.assertEquals(self.replicator.suffix_count, 0)
            mock_logger.reset_mock()

        # Check successful http_connect and sync for local node
        mock_tpool_reraise.return_value = (1, {'a83': 'ba47fd314242ec8c'
                                                      '7efb91f5d57336e4'})
        resp.read.return_value = pickle.dumps({'a83': 'c130a2c17ed45102a'
                                                      'ada0f4eee69494ff'})
        set_default(self)
        self.replicator.sync = fake_func = mock.MagicMock()
        self.replicator.update(local_job)
        reqs = []
        for node in local_job['nodes']:
            reqs.append(mock.call(node, local_job, ['a83']))
        fake_func.assert_has_calls(reqs, any_order=True)
        self.assertEquals(fake_func.call_count, 2)
        self.assertEquals(self.replicator.replication_count, 1)
        self.assertEquals(self.replicator.suffix_sync, 2)
        self.assertEquals(self.replicator.suffix_hash, 1)
        self.assertEquals(self.replicator.suffix_count, 1)
        mock_http.reset_mock()
        mock_logger.reset_mock()

        # test for replication params on policy 0 only
        repl_job = local_job.copy()
        for node in repl_job['nodes']:
            node['replication_ip'] = '127.0.0.11'
            node['replication_port'] = '6011'
        set_default(self)
        # with only one set of headers make sure we speicy index 0 here
        # as otherwise it may be different from earlier tests
        self.headers['X-Backend-Storage-Policy-Index'] = 0
        self.replicator.update(repl_job)
        reqs = []
        for node in repl_job['nodes']:
            reqs.append(mock.call(node['replication_ip'],
                                  node['replication_port'], node['device'],
                                  repl_job['partition'], 'REPLICATE',
                                  '', headers=self.headers))
            reqs.append(mock.call(node['replication_ip'],
                                  node['replication_port'], node['device'],
                                  repl_job['partition'], 'REPLICATE',
                                  '/a83', headers=self.headers))
        mock_http.assert_has_calls(reqs, any_order=True)


if __name__ == '__main__':
    unittest.main()
