# Copyright (c) 2010-2013 OpenStack, LLC.
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
import time
import tempfile
from test.unit import FakeLogger, mock as unit_mock
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, normalize_timestamp
from swift.common import ring
from swift.obj import base as object_base
from swift.obj.server import DiskFile


def _create_test_ring(path):
    testgz = os.path.join(path, 'object.ring.gz')
    intended_replica2part2dev_id = [
        [0, 1, 2, 3, 4, 5, 6],
        [1, 2, 3, 0, 5, 6, 4],
        [2, 3, 0, 1, 6, 4, 5]]
    intended_devs = [
        {'id': 0, 'device': 'sda', 'zone': 0, 'ip': '127.0.0.0', 'port': 6000},
        {'id': 1, 'device': 'sda', 'zone': 1, 'ip': '127.0.0.1', 'port': 6000},
        {'id': 2, 'device': 'sda', 'zone': 2, 'ip': '127.0.0.2', 'port': 6000},
        {'id': 3, 'device': 'sda', 'zone': 4, 'ip': '127.0.0.3', 'port': 6000},
        {'id': 4, 'device': 'sda', 'zone': 5, 'ip': '127.0.0.4', 'port': 6000},
        {'id': 5, 'device': 'sda', 'zone': 6,
         'ip': 'fe80::202:b3ff:fe1e:8329', 'port': 6000},
        {'id': 6, 'device': 'sda', 'zone': 7,
         'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334', 'port': 6000}]
    intended_part_shift = 30
    intended_reload_time = 15
    pickle.dump(
        ring.RingData(intended_replica2part2dev_id, intended_devs,
                      intended_part_shift),
        GzipFile(testgz, 'wb'))
    return ring.Ring(path, ring_name='object',
                     reload_time=intended_reload_time)


class TestObjectBase(unittest.TestCase):

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

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_hash_suffix_hash_dir_is_file_quarantine(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(os.path.dirname(df.datadir))
        open(df.datadir, 'wb').close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        orig_quarantine_renamer = object_base.quarantine_renamer
        called = [False]

        def wrapped(*args, **kwargs):
            called[0] = True
            return orig_quarantine_renamer(*args, **kwargs)

        try:
            object_base.quarantine_renamer = wrapped
            object_base.hash_suffix(whole_path_from, 101)
        finally:
            object_base.quarantine_renamer = orig_quarantine_renamer
        self.assertTrue(called[0])

    def test_hash_suffix_one_file(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        f = open(
            os.path.join(df.datadir,
                         normalize_timestamp(time.time() - 100) + '.ts'),
            'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        object_base.hash_suffix(whole_path_from, 101)
        self.assertEquals(len(os.listdir(self.parts['0'])), 1)

        object_base.hash_suffix(whole_path_from, 99)
        self.assertEquals(len(os.listdir(self.parts['0'])), 0)

    def test_hash_suffix_multi_file_one(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        for tdiff in [1, 50, 100, 500]:
            for suff in ['.meta', '.data', '.ts']:
                f = open(
                    os.path.join(
                        df.datadir,
                        normalize_timestamp(int(time.time()) - tdiff) + suff),
                    'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        object_base.hash_suffix(whole_path_from, 99)
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
                f = open(
                    os.path.join(
                        df.datadir,
                        normalize_timestamp(int(time.time()) - tdiff) + suff),
                    'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        object_base.hash_suffix(whole_path_from, 99)
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
                                   object_base.HASH_FILE)
        # test that non existent file except caught
        self.assertEquals(object_base.invalidate_hash(whole_path_from),
                          None)
        # test that hashes get cleared
        check_pickle_data = pickle.dumps({data_dir: None},
                                         object_base.PICKLE_PROTOCOL)
        for data_hash in [{data_dir: None}, {data_dir: 'abcdefg'}]:
            with open(hashes_file, 'wb') as fp:
                pickle.dump(data_hash, fp, object_base.PICKLE_PROTOCOL)
            object_base.invalidate_hash(whole_path_from)
            assertFileData(hashes_file, check_pickle_data)

    def test_get_hashes(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        with open(
                os.path.join(df.datadir,
                             normalize_timestamp(time.time()) + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = object_base.get_hashes(part)
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)
        hashed, hashes = object_base.get_hashes(part, do_listdir=True)
        self.assertEquals(hashed, 0)
        self.assert_('a83' in hashes)
        hashed, hashes = object_base.get_hashes(part, recalculate=['a83'])
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)

    def test_get_hashes_bad_dir(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        with open(os.path.join(self.objects, '0', 'bad'), 'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = object_base.get_hashes(part)
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)
        self.assert_('bad' not in hashes)

    def test_get_hashes_unmodified(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        with open(
                os.path.join(df.datadir,
                             normalize_timestamp(time.time()) + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = object_base.get_hashes(part)
        i = [0]

        def getmtime(filename):
            i[0] += 1
            return 1
        with unit_mock({'os.path.getmtime': getmtime}):
            hashed, hashes = object_base.get_hashes(
                part, recalculate=['a83'])
        self.assertEquals(i[0], 2)

    def test_get_hashes_unmodified_and_zero_bytes(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        part = os.path.join(self.objects, '0')
        open(os.path.join(part, object_base.HASH_FILE), 'w')
        # Now the hash file is zero bytes.
        i = [0]

        def getmtime(filename):
            i[0] += 1
            return 1
        with unit_mock({'os.path.getmtime': getmtime}):
            hashed, hashes = object_base.get_hashes(
                part, recalculate=[])
        # getmtime will actually not get called.  Initially, the pickle.load
        # will raise an exception first and later, force_rewrite will
        # short-circuit the if clause to determine whether to write out a fresh
        # hashes_file.
        self.assertEquals(i[0], 0)
        self.assertTrue('a83' in hashes)

    def test_get_hashes_modified(self):
        df = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o', FakeLogger())
        mkdirs(df.datadir)
        with open(
                os.path.join(df.datadir,
                             normalize_timestamp(time.time()) + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = object_base.get_hashes(part)
        i = [0]

        def getmtime(filename):
            if i[0] < 3:
                i[0] += 1
            return i[0]
        with unit_mock({'os.path.getmtime': getmtime}):
            hashed, hashes = object_base.get_hashes(
                part, recalculate=['a83'])
        self.assertEquals(i[0], 3)


if __name__ == '__main__':
    unittest.main()
