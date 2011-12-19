# Copyright (c) 2010-2011 OpenStack, LLC.
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

from __future__ import with_statement

import unittest
import os
from gzip import GzipFile
from shutil import rmtree
import cPickle as pickle
import logging
import fcntl
import time
import tempfile
from contextlib import contextmanager
from eventlet.green import subprocess
from eventlet import Timeout, tpool
from test.unit import FakeLogger
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, normalize_timestamp
from swift.common import ring
from swift.obj import replicator as object_replicator
from swift.obj.server import DiskFile


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


def _create_test_ring(path):
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
        {'id': 5, 'device': 'sda', 'zone': 6, 'ip': '127.0.0.5', 'port': 6000},
        {'id': 6, 'device': 'sda', 'zone': 7, 'ip': '127.0.0.6', 'port': 6000},
        ]
    intended_part_shift = 30
    intended_reload_time = 15
    pickle.dump(ring.RingData(intended_replica2part2dev_id,
        intended_devs, intended_part_shift),
        GzipFile(testgz, 'wb'))
    return ring.Ring(testgz, reload_time=intended_reload_time)


class TestObjectReplicator(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        # Setup a test ring (stolen from common/test_ring.py)
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        os.mkdir(os.path.join(self.devices, 'sda'))
        self.objects = os.path.join(self.devices, 'sda', 'objects')
        os.mkdir(self.objects)
        self.parts = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(os.path.join(self.objects, part))
        self.ring = _create_test_ring(self.testdir)
        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')
        self.replicator = object_replicator.ObjectReplicator(
            self.conf)

    def tearDown(self):
        process_errors = []
        rmtree(self.testdir, ignore_errors=1)

    def test_run_once(self):
        replicator = object_replicator.ObjectReplicator(
            dict(swift_dir=self.testdir, devices=self.devices,
                mount_check='false', timeout='300', stats_interval='1'))
        was_connector = object_replicator.http_connect
        object_replicator.http_connect = mock_http_connect(200)
        cur_part = '0'
        df = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o',
                      FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time.time()) + '.data'),
                 'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, cur_part, data_dir)
        process_arg_checker = []
        nodes = [node for node in
                 self.ring.get_part_nodes(int(cur_part)) \
                     if node['ip'] not in _ips()]
        for node in nodes:
            rsync_mod = '[%s]::object/sda/objects/%s' % (node['ip'], cur_part)
            process_arg_checker.append(
                (0, '', ['rsync', whole_path_from, rsync_mod]))
        with _mock_process(process_arg_checker):
            replicator.run_once()
        self.assertFalse(process_errors)

        object_replicator.http_connect = was_connector

    def test_get_hashes(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        with open(os.path.join(df.datadir, normalize_timestamp(
                    time.time()) + '.ts'), 'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = object_replicator.get_hashes(part)
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)
        hashed, hashes = object_replicator.get_hashes(part, do_listdir=True)
        self.assertEquals(hashed, 0)
        self.assert_('a83' in hashes)
        hashed, hashes = object_replicator.get_hashes(part,
                                                      recalculate=['a83'])
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)

    def test_hash_suffix_hash_dir_is_file_quarantine(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(os.path.dirname(df.datadir))
        open(df.datadir, 'wb').close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        orig_quarantine_renamer = object_replicator.quarantine_renamer
        called = [False]

        def wrapped(*args, **kwargs):
            called[0] = True
            return orig_quarantine_renamer(*args, **kwargs)

        try:
            object_replicator.quarantine_renamer = wrapped
            object_replicator.hash_suffix(whole_path_from, 101)
        finally:
            object_replicator.quarantine_renamer = orig_quarantine_renamer
        self.assertTrue(called[0])

    def test_hash_suffix_one_file(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                     normalize_timestamp(time.time() - 100) + '.ts'),
                 'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        object_replicator.hash_suffix(whole_path_from, 101)
        self.assertEquals(len(os.listdir(self.parts['0'])), 1)

        object_replicator.hash_suffix(whole_path_from, 99)
        self.assertEquals(len(os.listdir(self.parts['0'])), 0)

    def test_hash_suffix_multi_file_one(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        for tdiff in [1, 50, 100, 500]:
            for suff in ['.meta', '.data', '.ts']:
                f = open(os.path.join(df.datadir,
                        normalize_timestamp(int(time.time()) - tdiff) + suff),
                         'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        object_replicator.hash_suffix(whole_path_from, 99)
        # only the tombstone should be left
        self.assertEquals(len(os.listdir(whole_hsh_path)), 1)

    def test_hash_suffix_multi_file_two(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        for tdiff in [1, 50, 100, 500]:
            suffs = ['.meta', '.data']
            if tdiff > 50:
                suffs.append('.ts')
            for suff in suffs:
                f = open(os.path.join(df.datadir,
                        normalize_timestamp(int(time.time()) - tdiff) + suff),
                         'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        object_replicator.hash_suffix(whole_path_from, 99)
        # only the meta and data should be left
        self.assertEquals(len(os.listdir(whole_hsh_path)), 2)

    def test_invalidate_hash(self):

        def assertFileData(file_path, data):
            with open(file_path, 'r') as fp:
                fdata = fp.read()
                self.assertEquals(pickle.loads(fdata), pickle.loads(data))

        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hashes_file = os.path.join(self.objects, '0',
                                   object_replicator.HASH_FILE)
        # test that non existant file except caught
        self.assertEquals(object_replicator.invalidate_hash(whole_path_from),
                          None)
        # test that hashes get cleared
        check_pickle_data = pickle.dumps({data_dir: None},
                                         object_replicator.PICKLE_PROTOCOL)
        for data_hash in [{data_dir: None}, {data_dir: 'abcdefg'}]:
            with open(hashes_file, 'wb') as fp:
                pickle.dump(data_hash, fp, object_replicator.PICKLE_PROTOCOL)
            object_replicator.invalidate_hash(whole_path_from)
            assertFileData(hashes_file, check_pickle_data)

    def test_check_ring(self):
        self.assertTrue(self.replicator.check_ring())
        orig_check = self.replicator.next_check
        self.replicator.next_check = orig_check - 30
        self.assertTrue(self.replicator.check_ring())
        self.replicator.next_check = orig_check
        orig_ring_time = self.replicator.object_ring._mtime
        self.replicator.object_ring._mtime = orig_ring_time - 30
        self.assertTrue(self.replicator.check_ring())
        self.replicator.next_check = orig_check - 30
        self.assertFalse(self.replicator.check_ring())

    def test_collect_jobs(self):
        jobs = self.replicator.collect_jobs()
        jobs_to_delete = [j for j in jobs if j['delete']]
        jobs_to_keep = [j for j in jobs if not j['delete']]
        jobs_by_part = {}
        for job in jobs:
            jobs_by_part[job['partition']] = job
        self.assertEquals(len(jobs_to_delete), 1)
        self.assertTrue('1', jobs_to_delete[0]['partition'])
        self.assertEquals(
            [node['id'] for node in jobs_by_part['0']['nodes']], [1, 2])
        self.assertEquals(
            [node['id'] for node in jobs_by_part['1']['nodes']], [1, 2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_part['2']['nodes']], [2, 3])
        self.assertEquals(
            [node['id'] for node in jobs_by_part['3']['nodes']], [3, 1])
        for part in ['0', '1', '2', '3']:
            for node in jobs_by_part[part]['nodes']:
                self.assertEquals(node['device'], 'sda')
            self.assertEquals(jobs_by_part[part]['path'],
                              os.path.join(self.objects, part))

    def test_delete_partition(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        part_path = os.path.join(self.objects, '1')
        self.assertTrue(os.access(part_path, os.F_OK))
        self.replicator.replicate()
        self.assertFalse(os.access(part_path, os.F_OK))

    def test_run_once_recover_from_failure(self):
        replicator = object_replicator.ObjectReplicator(
            dict(swift_dir=self.testdir, devices=self.devices,
                mount_check='false', timeout='300', stats_interval='1'))
        was_connector = object_replicator.http_connect
        try:
            object_replicator.http_connect = mock_http_connect(200)
            # Write some files into '1' and run replicate- they should be moved
            # to the other partitoins and then node should get deleted.
            cur_part = '1'
            df = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o',
                          FakeLogger())
            mkdirs(df.datadir)
            f = open(os.path.join(df.datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, cur_part, data_dir)
            process_arg_checker = []
            nodes = [node for node in
                     self.ring.get_part_nodes(int(cur_part)) \
                         if node['ip'] not in _ips()]
            for node in nodes:
                rsync_mod = '[%s]::object/sda/objects/%s' % (node['ip'],
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
                                     i, object_replicator.HASH_FILE),
                        os.F_OK), result)
        finally:
            object_replicator.http_connect = was_connector

    def test_run_once_recover_from_timeout(self):
        replicator = object_replicator.ObjectReplicator(
            dict(swift_dir=self.testdir, devices=self.devices,
                mount_check='false', timeout='300', stats_interval='1'))
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
            # to the other partitoins and then node should get deleted.
            cur_part = '1'
            df = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o',
                          FakeLogger())
            mkdirs(df.datadir)
            f = open(os.path.join(df.datadir,
                                  normalize_timestamp(time.time()) + '.data'),
                     'wb')
            f.write('1234567890')
            f.close()
            ohash = hash_path('a', 'c', 'o')
            data_dir = ohash[-3:]
            whole_path_from = os.path.join(self.objects, cur_part, data_dir)
            process_arg_checker = []
            nodes = [node for node in
                     self.ring.get_part_nodes(int(cur_part)) \
                         if node['ip'] not in _ips()]
            for node in nodes:
                rsync_mod = '[%s]::object/sda/objects/%s' % (node['ip'],
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
            self.replicator.replicate()

    def test_run_withlog(self):
        with _mock_process([(0, "stuff in log")] * 100):
            self.replicator.replicate()

if __name__ == '__main__':
    unittest.main()
