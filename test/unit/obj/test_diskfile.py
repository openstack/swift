#-*- coding:utf-8 -*-
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

"""Tests for swift.obj.diskfile"""

import cPickle as pickle
import os
import errno
import mock
import unittest
import email
import tempfile
import uuid
import xattr
from shutil import rmtree
from time import time
from tempfile import mkdtemp
from hashlib import md5
from contextlib import closing, nested
from gzip import GzipFile

from eventlet import tpool
from test.unit import (FakeLogger, mock as unit_mock, temptree,
                       patch_policies, debug_logger)

from swift.obj import diskfile
from swift.common import utils
from swift.common.utils import hash_path, mkdirs, Timestamp
from swift.common import ring
from swift.common.exceptions import DiskFileNotExist, DiskFileQuarantined, \
    DiskFileDeviceUnavailable, DiskFileDeleted, DiskFileNotOpen, \
    DiskFileError, ReplicationLockTimeout, PathNotDir, DiskFileCollision, \
    DiskFileExpired, SwiftException, DiskFileNoSpace
from swift.common.storage_policy import POLICIES, get_policy_string
from functools import partial


get_data_dir = partial(get_policy_string, diskfile.DATADIR_BASE)
get_tmp_dir = partial(get_policy_string, diskfile.TMP_BASE)


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
    with closing(GzipFile(testgz, 'wb')) as f:
        pickle.dump(
            ring.RingData(intended_replica2part2dev_id, intended_devs,
                          intended_part_shift),
            f)
    return ring.Ring(path, ring_name='object',
                     reload_time=intended_reload_time)


@patch_policies
class TestDiskFileModuleMethods(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''
        # Setup a test ring (stolen from common/test_ring.py)
        self.testdir = tempfile.mkdtemp()
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        self.existing_device = 'sda'
        os.mkdir(os.path.join(self.devices, self.existing_device))
        self.objects = os.path.join(self.devices, self.existing_device,
                                    'objects')
        os.mkdir(self.objects)
        self.parts = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(os.path.join(self.objects, part))
        self.ring = _create_test_ring(self.testdir)
        self.conf = dict(
            swift_dir=self.testdir, devices=self.devices, mount_check='false',
            timeout='300', stats_interval='1')
        self.df_mgr = diskfile.DiskFileManager(self.conf, FakeLogger())

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def _create_diskfile(self, policy_idx=0):
        return self.df_mgr.get_diskfile(self.existing_device,
                                        '0', 'a', 'c', 'o',
                                        policy_idx)

    def test_extract_policy_index(self):
        # good path names
        pn = 'objects/0/606/1984527ed7ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy_index(pn), 0)
        pn = 'objects-1/0/606/198452b6ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy_index(pn), 1)
        good_path = '/srv/node/sda1/objects-1/1/abc/def/1234.data'
        self.assertEquals(1, diskfile.extract_policy_index(good_path))
        good_path = '/srv/node/sda1/objects/1/abc/def/1234.data'
        self.assertEquals(0, diskfile.extract_policy_index(good_path))

        # short paths still ok
        path = '/srv/node/sda1/objects/1/1234.data'
        self.assertEqual(diskfile.extract_policy_index(path), 0)
        path = '/srv/node/sda1/objects-1/1/1234.data'
        self.assertEqual(diskfile.extract_policy_index(path), 1)

        # leading slash, just in case
        pn = '/objects/0/606/1984527ed7ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy_index(pn), 0)
        pn = '/objects-1/0/606/198452b6ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy_index(pn), 1)

        # bad policy index
        pn = 'objects-2/0/606/198427efcff042c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy_index(pn), 0)
        bad_path = '/srv/node/sda1/objects-t/1/abc/def/1234.data'
        self.assertRaises(ValueError,
                          diskfile.extract_policy_index, bad_path)

        # malformed path (no objects dir or nothing at all)
        pn = 'XXXX/0/606/1984527ed42b6ef6247c78606/1401379842.14643.data'
        self.assertEqual(diskfile.extract_policy_index(pn), 0)
        self.assertEqual(diskfile.extract_policy_index(''), 0)

        # no datadir base in path
        bad_path = '/srv/node/sda1/foo-1/1/abc/def/1234.data'
        self.assertEqual(diskfile.extract_policy_index(bad_path), 0)
        bad_path = '/srv/node/sda1/obj1/1/abc/def/1234.data'
        self.assertEqual(diskfile.extract_policy_index(bad_path), 0)

    def test_quarantine_renamer(self):
        for policy in POLICIES:
            # we use this for convenience, not really about a diskfile layout
            df = self._create_diskfile(policy_idx=policy.idx)
            mkdirs(df._datadir)
            exp_dir = os.path.join(self.devices, 'quarantined',
                                   get_data_dir(policy.idx),
                                   os.path.basename(df._datadir))
            qbit = os.path.join(df._datadir, 'qbit')
            with open(qbit, 'w') as f:
                f.write('abc')
            to_dir = diskfile.quarantine_renamer(self.devices, qbit)
            self.assertEqual(to_dir, exp_dir)
            self.assertRaises(OSError, diskfile.quarantine_renamer,
                              self.devices, qbit)

    def test_hash_suffix_enoent(self):
        self.assertRaises(PathNotDir, diskfile.hash_suffix,
                          os.path.join(self.testdir, "doesnotexist"), 101)

    def test_hash_suffix_oserror(self):
        mocked_os_listdir = mock.Mock(
            side_effect=OSError(errno.EACCES, os.strerror(errno.EACCES)))
        with mock.patch("os.listdir", mocked_os_listdir):
            self.assertRaises(OSError, diskfile.hash_suffix,
                              os.path.join(self.testdir, "doesnotexist"), 101)

    def test_get_data_dir(self):
        self.assertEquals(diskfile.get_data_dir(0), diskfile.DATADIR_BASE)
        self.assertEquals(diskfile.get_data_dir(1),
                          diskfile.DATADIR_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_data_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_data_dir, 99)

    def test_get_async_dir(self):
        self.assertEquals(diskfile.get_async_dir(0),
                          diskfile.ASYNCDIR_BASE)
        self.assertEquals(diskfile.get_async_dir(1),
                          diskfile.ASYNCDIR_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_async_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_async_dir, 99)

    def test_get_tmp_dir(self):
        self.assertEquals(diskfile.get_tmp_dir(0),
                          diskfile.TMP_BASE)
        self.assertEquals(diskfile.get_tmp_dir(1),
                          diskfile.TMP_BASE + "-1")
        self.assertRaises(ValueError, diskfile.get_tmp_dir, 'junk')

        self.assertRaises(ValueError, diskfile.get_tmp_dir, 99)

    def test_pickle_async_update_tmp_dir(self):
        for policy in POLICIES:
            if int(policy) == 0:
                tmp_part = 'tmp'
            else:
                tmp_part = 'tmp-%d' % policy
            tmp_path = os.path.join(
                self.devices, self.existing_device, tmp_part)
            self.assertFalse(os.path.isdir(tmp_path))
            pickle_args = (self.existing_device, 'a', 'c', 'o',
                           'data', 0.0, int(policy))
            # async updates don't create their tmpdir on their own
            self.assertRaises(OSError, self.df_mgr.pickle_async_update,
                              *pickle_args)
            os.makedirs(tmp_path)
            # now create a async update
            self.df_mgr.pickle_async_update(*pickle_args)
            # check tempdir
            self.assertTrue(os.path.isdir(tmp_path))

    def test_hash_suffix_hash_dir_is_file_quarantine(self):
        df = self._create_diskfile()
        mkdirs(os.path.dirname(df._datadir))
        open(df._datadir, 'wb').close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        orig_quarantine_renamer = diskfile.quarantine_renamer
        called = [False]

        def wrapped(*args, **kwargs):
            called[0] = True
            return orig_quarantine_renamer(*args, **kwargs)

        try:
            diskfile.quarantine_renamer = wrapped
            diskfile.hash_suffix(whole_path_from, 101)
        finally:
            diskfile.quarantine_renamer = orig_quarantine_renamer
        self.assertTrue(called[0])

    def test_hash_suffix_one_file(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        f = open(
            os.path.join(df._datadir,
                         Timestamp(time() - 100).internal + '.ts'),
            'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        diskfile.hash_suffix(whole_path_from, 101)
        self.assertEquals(len(os.listdir(self.parts['0'])), 1)

        diskfile.hash_suffix(whole_path_from, 99)
        self.assertEquals(len(os.listdir(self.parts['0'])), 0)

    def test_hash_suffix_oserror_on_hcl(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        f = open(
            os.path.join(df._datadir,
                         Timestamp(time() - 100).internal + '.ts'),
            'wb')
        f.write('1234567890')
        f.close()
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        state = [0]
        orig_os_listdir = os.listdir

        def mock_os_listdir(*args, **kwargs):
            # We want the first call to os.listdir() to succeed, which is the
            # one directly from hash_suffix() itself, but then we want to fail
            # the next call to os.listdir() which is from
            # hash_cleanup_listdir()
            if state[0] == 1:
                raise OSError(errno.EACCES, os.strerror(errno.EACCES))
            state[0] = 1
            return orig_os_listdir(*args, **kwargs)

        with mock.patch('os.listdir', mock_os_listdir):
            self.assertRaises(OSError, diskfile.hash_suffix, whole_path_from,
                              101)

    def test_hash_suffix_multi_file_one(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        for tdiff in [1, 50, 100, 500]:
            for suff in ['.meta', '.data', '.ts']:
                f = open(
                    os.path.join(
                        df._datadir,
                        Timestamp(int(time()) - tdiff).internal + suff),
                    'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        diskfile.hash_suffix(whole_path_from, 99)
        # only the tombstone should be left
        self.assertEquals(len(os.listdir(whole_hsh_path)), 1)

    def test_hash_suffix_multi_file_two(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        for tdiff in [1, 50, 100, 500]:
            suffs = ['.meta', '.data']
            if tdiff > 50:
                suffs.append('.ts')
            for suff in suffs:
                f = open(
                    os.path.join(
                        df._datadir,
                        Timestamp(int(time()) - tdiff).internal + suff),
                    'wb')
                f.write('1234567890')
                f.close()

        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hsh_path = os.listdir(whole_path_from)[0]
        whole_hsh_path = os.path.join(whole_path_from, hsh_path)

        diskfile.hash_suffix(whole_path_from, 99)
        # only the meta and data should be left
        self.assertEquals(len(os.listdir(whole_hsh_path)), 2)

    def test_hash_suffix_hsh_path_disappearance(self):
        orig_rmdir = os.rmdir

        def _rmdir(path):
            # Done twice to recreate what happens when it doesn't exist.
            orig_rmdir(path)
            orig_rmdir(path)

        df = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o')
        mkdirs(df._datadir)
        ohash = hash_path('a', 'c', 'o')
        suffix = ohash[-3:]
        suffix_path = os.path.join(self.objects, '0', suffix)
        with mock.patch('os.rmdir', _rmdir):
            # If hash_suffix doesn't handle the exception _rmdir will raise,
            # this test will fail.
            diskfile.hash_suffix(suffix_path, 123)

    def test_invalidate_hash(self):

        def assertFileData(file_path, data):
            with open(file_path, 'r') as fp:
                fdata = fp.read()
                self.assertEquals(pickle.loads(fdata), pickle.loads(data))

        df = self._create_diskfile()
        mkdirs(df._datadir)
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hashes_file = os.path.join(self.objects, '0',
                                   diskfile.HASH_FILE)
        # test that non existent file except caught
        self.assertEquals(diskfile.invalidate_hash(whole_path_from),
                          None)
        # test that hashes get cleared
        check_pickle_data = pickle.dumps({data_dir: None},
                                         diskfile.PICKLE_PROTOCOL)
        for data_hash in [{data_dir: None}, {data_dir: 'abcdefg'}]:
            with open(hashes_file, 'wb') as fp:
                pickle.dump(data_hash, fp, diskfile.PICKLE_PROTOCOL)
            diskfile.invalidate_hash(whole_path_from)
            assertFileData(hashes_file, check_pickle_data)

    def test_invalidate_hash_bad_pickle(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        ohash = hash_path('a', 'c', 'o')
        data_dir = ohash[-3:]
        whole_path_from = os.path.join(self.objects, '0', data_dir)
        hashes_file = os.path.join(self.objects, '0',
                                   diskfile.HASH_FILE)
        for data_hash in [{data_dir: None}, {data_dir: 'abcdefg'}]:
            with open(hashes_file, 'wb') as fp:
                fp.write('bad hash data')
            try:
                diskfile.invalidate_hash(whole_path_from)
            except Exception as err:
                self.fail("Unexpected exception raised: %s" % err)
            else:
                pass

    def test_get_hashes(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        with open(
                os.path.join(df._datadir,
                             Timestamp(time()).internal + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)
        hashed, hashes = diskfile.get_hashes(part, do_listdir=True)
        self.assertEquals(hashed, 0)
        self.assert_('a83' in hashes)
        hashed, hashes = diskfile.get_hashes(part, recalculate=['a83'])
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)

    def test_get_hashes_bad_dir(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        with open(os.path.join(self.objects, '0', 'bad'), 'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        self.assertEquals(hashed, 1)
        self.assert_('a83' in hashes)
        self.assert_('bad' not in hashes)

    def test_get_hashes_unmodified(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        with open(
                os.path.join(df._datadir,
                             Timestamp(time()).internal + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        i = [0]

        def _getmtime(filename):
            i[0] += 1
            return 1
        with unit_mock({'swift.obj.diskfile.getmtime': _getmtime}):
            hashed, hashes = diskfile.get_hashes(
                part, recalculate=['a83'])
        self.assertEquals(i[0], 2)

    def test_get_hashes_unmodified_norecalc(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        with open(
                os.path.join(df._datadir,
                             Timestamp(time()).internal + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes_0 = diskfile.get_hashes(part)
        self.assertEqual(hashed, 1)
        self.assertTrue('a83' in hashes_0)
        hashed, hashes_1 = diskfile.get_hashes(part)
        self.assertEqual(hashed, 0)
        self.assertTrue('a83' in hashes_0)
        self.assertEqual(hashes_1, hashes_0)

    def test_get_hashes_hash_suffix_error(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        with open(
                os.path.join(df._datadir,
                             Timestamp(time()).internal + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        mocked_hash_suffix = mock.MagicMock(
            side_effect=OSError(errno.EACCES, os.strerror(errno.EACCES)))
        with mock.patch('swift.obj.diskfile.hash_suffix', mocked_hash_suffix):
            hashed, hashes = diskfile.get_hashes(part)
            self.assertEqual(hashed, 0)
            self.assertEqual(hashes, {'a83': None})

    def test_get_hashes_unmodified_and_zero_bytes(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        part = os.path.join(self.objects, '0')
        open(os.path.join(part, diskfile.HASH_FILE), 'w')
        # Now the hash file is zero bytes.
        i = [0]

        def _getmtime(filename):
            i[0] += 1
            return 1
        with unit_mock({'swift.obj.diskfile.getmtime': _getmtime}):
            hashed, hashes = diskfile.get_hashes(
                part, recalculate=[])
        # getmtime will actually not get called.  Initially, the pickle.load
        # will raise an exception first and later, force_rewrite will
        # short-circuit the if clause to determine whether to write out a
        # fresh hashes_file.
        self.assertEquals(i[0], 0)
        self.assertTrue('a83' in hashes)

    def test_get_hashes_modified(self):
        df = self._create_diskfile()
        mkdirs(df._datadir)
        with open(
                os.path.join(df._datadir,
                             Timestamp(time()).internal + '.ts'),
                'wb') as f:
            f.write('1234567890')
        part = os.path.join(self.objects, '0')
        hashed, hashes = diskfile.get_hashes(part)
        i = [0]

        def _getmtime(filename):
            if i[0] < 3:
                i[0] += 1
            return i[0]
        with unit_mock({'swift.obj.diskfile.getmtime': _getmtime}):
            hashed, hashes = diskfile.get_hashes(
                part, recalculate=['a83'])
        self.assertEquals(i[0], 3)

    def check_hash_cleanup_listdir(self, input_files, output_files):
        orig_unlink = os.unlink
        file_list = list(input_files)

        def mock_listdir(path):
            return list(file_list)

        def mock_unlink(path):
            # timestamp 1 is a special tag to pretend a file disappeared while
            # working.
            if '/0000000001.00000.' in path:
                # Using actual os.unlink to reproduce exactly what OSError it
                # raises.
                orig_unlink(uuid.uuid4().hex)
            file_list.remove(os.path.basename(path))

        with unit_mock({'os.listdir': mock_listdir, 'os.unlink': mock_unlink}):
            self.assertEquals(diskfile.hash_cleanup_listdir('/whatever'),
                              output_files)

    def test_hash_cleanup_listdir_purge_data_newer_ts(self):
        # purge .data if there's a newer .ts
        file1 = Timestamp(time()).internal + '.data'
        file2 = Timestamp(time() + 1).internal + '.ts'
        file_list = [file1, file2]
        self.check_hash_cleanup_listdir(file_list, [file2])

    def test_hash_cleanup_listdir_purge_ts_newer_data(self):
        # purge .ts if there's a newer .data
        file1 = Timestamp(time()).internal + '.ts'
        file2 = Timestamp(time() + 1).internal + '.data'
        file_list = [file1, file2]
        self.check_hash_cleanup_listdir(file_list, [file2])

    def test_hash_cleanup_listdir_keep_meta_data_purge_ts(self):
        # keep .meta and .data if meta newer than data and purge .ts
        file1 = Timestamp(time()).internal + '.ts'
        file2 = Timestamp(time() + 1).internal + '.data'
        file3 = Timestamp(time() + 2).internal + '.meta'
        file_list = [file1, file2, file3]
        self.check_hash_cleanup_listdir(file_list, [file3, file2])

    def test_hash_cleanup_listdir_keep_one_ts(self):
        # keep only latest of multiple .ts files
        file1 = Timestamp(time()).internal + '.ts'
        file2 = Timestamp(time() + 1).internal + '.ts'
        file3 = Timestamp(time() + 2).internal + '.ts'
        file_list = [file1, file2, file3]
        self.check_hash_cleanup_listdir(file_list, [file3])

    def test_hash_cleanup_listdir_keep_one_data(self):
        # keep only latest of multiple .data files
        file1 = Timestamp(time()).internal + '.data'
        file2 = Timestamp(time() + 1).internal + '.data'
        file3 = Timestamp(time() + 2).internal + '.data'
        file_list = [file1, file2, file3]
        self.check_hash_cleanup_listdir(file_list, [file3])

    def test_hash_cleanup_listdir_keep_one_meta(self):
        # keep only latest of multiple .meta files
        file1 = Timestamp(time()).internal + '.data'
        file2 = Timestamp(time() + 1).internal + '.meta'
        file3 = Timestamp(time() + 2).internal + '.meta'
        file_list = [file1, file2, file3]
        self.check_hash_cleanup_listdir(file_list, [file3, file1])

    def test_hash_cleanup_listdir_ignore_orphaned_ts(self):
        # A more recent orphaned .meta file will prevent old .ts files
        # from being cleaned up otherwise
        file1 = Timestamp(time()).internal + '.ts'
        file2 = Timestamp(time() + 1).internal + '.ts'
        file3 = Timestamp(time() + 2).internal + '.meta'
        file_list = [file1, file2, file3]
        self.check_hash_cleanup_listdir(file_list, [file3, file2])

    def test_hash_cleanup_listdir_purge_old_data_only(self):
        # Oldest .data will be purge, .meta and .ts won't be touched
        file1 = Timestamp(time()).internal + '.data'
        file2 = Timestamp(time() + 1).internal + '.ts'
        file3 = Timestamp(time() + 2).internal + '.meta'
        file_list = [file1, file2, file3]
        self.check_hash_cleanup_listdir(file_list, [file3, file2])

    def test_hash_cleanup_listdir_purge_old_ts(self):
        # A single old .ts file will be removed
        file1 = Timestamp(time() - (diskfile.ONE_WEEK + 1)).internal + '.ts'
        file_list = [file1]
        self.check_hash_cleanup_listdir(file_list, [])

    def test_hash_cleanup_listdir_meta_keeps_old_ts(self):
        # An orphaned .meta will not clean up a very old .ts
        file1 = Timestamp(time() - (diskfile.ONE_WEEK + 1)).internal + '.ts'
        file2 = Timestamp(time() + 2).internal + '.meta'
        file_list = [file1, file2]
        self.check_hash_cleanup_listdir(file_list, [file2, file1])

    def test_hash_cleanup_listdir_keep_single_old_data(self):
        # A single old .data file will not be removed
        file1 = Timestamp(time() - (diskfile.ONE_WEEK + 1)).internal + '.data'
        file_list = [file1]
        self.check_hash_cleanup_listdir(file_list, [file1])

    def test_hash_cleanup_listdir_keep_single_old_meta(self):
        # A single old .meta file will not be removed
        file1 = Timestamp(time() - (diskfile.ONE_WEEK + 1)).internal + '.meta'
        file_list = [file1]
        self.check_hash_cleanup_listdir(file_list, [file1])

    def test_hash_cleanup_listdir_disappeared_path(self):
        # Next line listing a non-existent dir used to propagate the OSError;
        # now should mute that.
        self.assertEqual(diskfile.hash_cleanup_listdir(uuid.uuid4().hex), [])

    def test_hash_cleanup_listdir_disappeared_before_unlink_1(self):
        # Timestamp 1 makes other test routines pretend the file disappeared
        # while working.
        file1 = '0000000001.00000.ts'
        file_list = [file1]
        self.check_hash_cleanup_listdir(file_list, [])

    def test_hash_cleanup_listdir_disappeared_before_unlink_2(self):
        # Timestamp 1 makes other test routines pretend the file disappeared
        # while working.
        file1 = '0000000001.00000.data'
        file2 = '0000000002.00000.ts'
        file_list = [file1, file2]
        self.check_hash_cleanup_listdir(file_list, [file2])


@patch_policies
class TestObjectAuditLocationGenerator(unittest.TestCase):
    def _make_file(self, path):
        try:
            os.makedirs(os.path.dirname(path))
        except OSError as err:
            if err.errno != errno.EEXIST:
                raise

        with open(path, 'w'):
            pass

    def test_audit_location_class(self):
        al = diskfile.AuditLocation('abc', '123', '_-_')
        self.assertEqual(str(al), 'abc')

    def test_finding_of_hashdirs(self):
        with temptree([]) as tmpdir:
            # the good
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                                     "5c1fdc1ffb12e5eaf84edc30d8b67aca"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                                     "fdfd184d39080020bc8b487f8a7beaca"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "1519", "df2",
                                     "b0fe7af831cc7b1af5bf486b1c841df2"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects", "9720", "ca5",
                                     "4a943bc72c2e647c4675923d58cf4ca5"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects", "3071", "8eb",
                                     "fcd938702024c25fef6c32fef05298eb"))
            os.makedirs(os.path.join(tmpdir, "sdp", "objects-1", "9970", "ca5",
                                     "4a943bc72c2e647c4675923d58cf4ca5"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-2", "9971", "8eb",
                                     "fcd938702024c25fef6c32fef05298eb"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-99", "9972",
                                     "8eb",
                                     "fcd938702024c25fef6c32fef05298eb"))
            # the bad
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-", "1135",
                                     "6c3",
                                     "fcd938702024c25fef6c32fef05298eb"))
            os.makedirs(os.path.join(tmpdir, "sdq", "objects-fud", "foo"))

            self._make_file(os.path.join(tmpdir, "sdp", "objects", "1519",
                                         "fed"))
            self._make_file(os.path.join(tmpdir, "sdq", "objects", "9876"))

            # the empty
            os.makedirs(os.path.join(tmpdir, "sdr"))
            os.makedirs(os.path.join(tmpdir, "sds", "objects"))
            os.makedirs(os.path.join(tmpdir, "sdt", "objects", "9601"))
            os.makedirs(os.path.join(tmpdir, "sdu", "objects", "6499", "f80"))

            # the irrelevant
            os.makedirs(os.path.join(tmpdir, "sdv", "accounts", "77", "421",
                                     "4b8c86149a6d532f4af018578fd9f421"))
            os.makedirs(os.path.join(tmpdir, "sdw", "containers", "28", "51e",
                                     "4f9eee668b66c6f0250bfa3c7ab9e51e"))

            logger = debug_logger()
            locations = [(loc.path, loc.device, loc.partition)
                         for loc in diskfile.object_audit_location_generator(
                             devices=tmpdir, mount_check=False,
                             logger=logger)]
            locations.sort()

            # expect some warnings about those bad dirs
            warnings = logger.get_lines_for_level('warning')
            self.assertEqual(set(warnings), set([
                'Directory objects- does not map to a valid policy',
                'Directory objects-2 does not map to a valid policy',
                'Directory objects-99 does not map to a valid policy',
                'Directory objects-fud does not map to a valid policy']))

            expected =  \
                [(os.path.join(tmpdir, "sdp", "objects-1", "9970", "ca5",
                               "4a943bc72c2e647c4675923d58cf4ca5"),
                  "sdp", "9970"),
                 (os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                               "5c1fdc1ffb12e5eaf84edc30d8b67aca"),
                  "sdp", "1519"),
                 (os.path.join(tmpdir, "sdp", "objects", "1519", "aca",
                               "fdfd184d39080020bc8b487f8a7beaca"),
                  "sdp", "1519"),
                 (os.path.join(tmpdir, "sdp", "objects", "1519", "df2",
                               "b0fe7af831cc7b1af5bf486b1c841df2"),
                  "sdp", "1519"),
                 (os.path.join(tmpdir, "sdp", "objects", "9720", "ca5",
                               "4a943bc72c2e647c4675923d58cf4ca5"),
                  "sdp", "9720"),
                 (os.path.join(tmpdir, "sdq", "objects-", "1135", "6c3",
                               "fcd938702024c25fef6c32fef05298eb"),
                  "sdq", "1135"),
                 (os.path.join(tmpdir, "sdq", "objects-2", "9971", "8eb",
                               "fcd938702024c25fef6c32fef05298eb"),
                  "sdq", "9971"),
                 (os.path.join(tmpdir, "sdq", "objects-99", "9972", "8eb",
                               "fcd938702024c25fef6c32fef05298eb"),
                  "sdq", "9972"),
                 (os.path.join(tmpdir, "sdq", "objects", "3071", "8eb",
                               "fcd938702024c25fef6c32fef05298eb"),
                  "sdq", "3071"),
                 ]
            self.assertEqual(locations, expected)

            #now without a logger
            locations = [(loc.path, loc.device, loc.partition)
                         for loc in diskfile.object_audit_location_generator(
                             devices=tmpdir, mount_check=False)]
            locations.sort()
            self.assertEqual(locations, expected)

    def test_skipping_unmounted_devices(self):
        def mock_ismount(path):
            return path.endswith('sdp')

        with mock.patch('swift.obj.diskfile.ismount', mock_ismount):
            with temptree([]) as tmpdir:
                os.makedirs(os.path.join(tmpdir, "sdp", "objects",
                                         "2607", "df3",
                                         "ec2871fe724411f91787462f97d30df3"))
                os.makedirs(os.path.join(tmpdir, "sdq", "objects",
                                         "9785", "a10",
                                         "4993d582f41be9771505a8d4cb237a10"))

                locations = [
                    (loc.path, loc.device, loc.partition)
                    for loc in diskfile.object_audit_location_generator(
                        devices=tmpdir, mount_check=True)]
                locations.sort()

                self.assertEqual(
                    locations,
                    [(os.path.join(tmpdir, "sdp", "objects",
                                   "2607", "df3",
                                   "ec2871fe724411f91787462f97d30df3"),
                      "sdp", "2607")])

                # Do it again, this time with a logger.
                ml = mock.MagicMock()
                locations = [
                    (loc.path, loc.device, loc.partition)
                    for loc in diskfile.object_audit_location_generator(
                        devices=tmpdir, mount_check=True, logger=ml)]
                ml.debug.assert_called_once_with(
                    'Skipping %s as it is not mounted',
                    'sdq')

    def test_only_catch_expected_errors(self):
        # Crazy exceptions should still escape object_audit_location_generator
        # so that errors get logged and a human can see what's going wrong;
        # only normal FS corruption should be skipped over silently.

        def list_locations(dirname):
            return [(loc.path, loc.device, loc.partition)
                    for loc in diskfile.object_audit_location_generator(
                        devices=dirname, mount_check=False)]

        real_listdir = os.listdir

        def splode_if_endswith(suffix):
            def sploder(path):
                if path.endswith(suffix):
                    raise OSError(errno.EACCES, "don't try to ad-lib")
                else:
                    return real_listdir(path)
            return sploder

        with temptree([]) as tmpdir:
            os.makedirs(os.path.join(tmpdir, "sdf", "objects",
                                     "2607", "b54",
                                     "fe450ec990a88cc4b252b181bab04b54"))
            with mock.patch('os.listdir', splode_if_endswith("sdf/objects")):
                self.assertRaises(OSError, list_locations, tmpdir)
            with mock.patch('os.listdir', splode_if_endswith("2607")):
                self.assertRaises(OSError, list_locations, tmpdir)
            with mock.patch('os.listdir', splode_if_endswith("b54")):
                self.assertRaises(OSError, list_locations, tmpdir)


class TestDiskFileManager(unittest.TestCase):

    def setUp(self):
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(
            self.tmpdir, 'tmp_test_obj_server_DiskFile')
        self.existing_device1 = 'sda1'
        self.existing_device2 = 'sda2'
        mkdirs(os.path.join(self.testdir, self.existing_device1, 'tmp'))
        mkdirs(os.path.join(self.testdir, self.existing_device2, 'tmp'))
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.conf = dict(devices=self.testdir, mount_check='false',
                         keep_cache_size=2 * 1024)
        self.df_mgr = diskfile.DiskFileManager(self.conf, FakeLogger())

    def tearDown(self):
        rmtree(self.tmpdir, ignore_errors=1)

    def test_construct_dev_path(self):
        res_path = self.df_mgr.construct_dev_path('abc')
        self.assertEqual(os.path.join(self.df_mgr.devices, 'abc'), res_path)

    def test_pickle_async_update(self):
        self.df_mgr.logger.increment = mock.MagicMock()
        ts = Timestamp(10000.0).internal
        with mock.patch('swift.obj.diskfile.write_pickle') as wp:
            self.df_mgr.pickle_async_update(self.existing_device1,
                                            'a', 'c', 'o',
                                            dict(a=1, b=2), ts, 0)
            dp = self.df_mgr.construct_dev_path(self.existing_device1)
            ohash = diskfile.hash_path('a', 'c', 'o')
            wp.assert_called_with({'a': 1, 'b': 2},
                                  os.path.join(dp, diskfile.get_async_dir(0),
                                               ohash[-3:], ohash + '-' + ts),
                                  os.path.join(dp, 'tmp'))
        self.df_mgr.logger.increment.assert_called_with('async_pendings')

    def test_object_audit_location_generator(self):
        locations = list(self.df_mgr.object_audit_location_generator())
        self.assertEqual(locations, [])

    def test_get_hashes_bad_dev(self):
        self.df_mgr.mount_check = True
        with mock.patch('swift.obj.diskfile.check_mount',
                        mock.MagicMock(side_effect=[False])):
            self.assertRaises(DiskFileDeviceUnavailable,
                              self.df_mgr.get_hashes, 'sdb1', '0', '123',
                              'objects')

    def test_get_hashes_w_nothing(self):
        hashes = self.df_mgr.get_hashes(self.existing_device1, '0', '123', '0')
        self.assertEqual(hashes, {})
        # get_hashes creates the partition path, so call again for code
        # path coverage, ensuring the result is unchanged
        hashes = self.df_mgr.get_hashes(self.existing_device1, '0', '123', '0')
        self.assertEqual(hashes, {})

    def test_replication_lock_on(self):
        # Double check settings
        self.df_mgr.replication_one_per_device = True
        self.df_mgr.replication_lock_timeout = 0.1
        dev_path = os.path.join(self.testdir, self.existing_device1)
        with self.df_mgr.replication_lock(dev_path):
            lock_exc = None
            exc = None
            try:
                with self.df_mgr.replication_lock(dev_path):
                    raise Exception(
                        '%r was not replication locked!' % dev_path)
            except ReplicationLockTimeout as err:
                lock_exc = err
            except Exception as err:
                exc = err
            self.assertTrue(lock_exc is not None)
            self.assertTrue(exc is None)

    def test_replication_lock_off(self):
        # Double check settings
        self.df_mgr.replication_one_per_device = False
        self.df_mgr.replication_lock_timeout = 0.1
        dev_path = os.path.join(self.testdir, self.existing_device1)
        with self.df_mgr.replication_lock(dev_path):
            lock_exc = None
            exc = None
            try:
                with self.df_mgr.replication_lock(dev_path):
                    raise Exception(
                        '%r was not replication locked!' % dev_path)
            except ReplicationLockTimeout as err:
                lock_exc = err
            except Exception as err:
                exc = err
            self.assertTrue(lock_exc is None)
            self.assertTrue(exc is not None)

    def test_replication_lock_another_device_fine(self):
        # Double check settings
        self.df_mgr.replication_one_per_device = True
        self.df_mgr.replication_lock_timeout = 0.1
        dev_path = os.path.join(self.testdir, self.existing_device1)
        dev_path2 = os.path.join(self.testdir, self.existing_device2)
        with self.df_mgr.replication_lock(dev_path):
            lock_exc = None
            try:
                with self.df_mgr.replication_lock(dev_path2):
                    pass
            except ReplicationLockTimeout as err:
                lock_exc = err
            self.assertTrue(lock_exc is None)


@patch_policies
class TestDiskFile(unittest.TestCase):
    """Test swift.obj.diskfile.DiskFile"""

    def setUp(self):
        """Set up for testing swift.obj.diskfile"""
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(
            self.tmpdir, 'tmp_test_obj_server_DiskFile')
        self.existing_device = 'sda1'
        for policy in POLICIES:
            mkdirs(os.path.join(self.testdir, self.existing_device,
                                get_tmp_dir(policy.idx)))
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)
        self.conf = dict(devices=self.testdir, mount_check='false',
                         keep_cache_size=2 * 1024, mb_per_sync=1)
        self.df_mgr = diskfile.DiskFileManager(self.conf, FakeLogger())

    def tearDown(self):
        """Tear down for testing swift.obj.diskfile"""
        rmtree(self.tmpdir, ignore_errors=1)
        tpool.execute = self._orig_tpool_exc

    def _create_ondisk_file(self, df, data, timestamp, metadata=None,
                            ext='.data'):
        mkdirs(df._datadir)
        if timestamp is None:
            timestamp = time()
        timestamp = Timestamp(timestamp).internal
        if not metadata:
            metadata = {}
        if 'X-Timestamp' not in metadata:
            metadata['X-Timestamp'] = Timestamp(timestamp).internal
        if 'ETag' not in metadata:
            etag = md5()
            etag.update(data)
            metadata['ETag'] = etag.hexdigest()
        if 'name' not in metadata:
            metadata['name'] = '/a/c/o'
        if 'Content-Length' not in metadata:
            metadata['Content-Length'] = str(len(data))
        data_file = os.path.join(df._datadir, timestamp + ext)
        with open(data_file, 'wb') as f:
            f.write(data)
            xattr.setxattr(f.fileno(), diskfile.METADATA_KEY,
                           pickle.dumps(metadata, diskfile.PICKLE_PROTOCOL))

    def _simple_get_diskfile(self, partition='0', account='a', container='c',
                             obj='o', policy_idx=0):
        return self.df_mgr.get_diskfile(self.existing_device,
                                        partition, account, container, obj,
                                        policy_idx)

    def _create_test_file(self, data, timestamp=None, metadata=None,
                          account='a', container='c', obj='o'):
        if metadata is None:
            metadata = {}
        metadata.setdefault('name', '/%s/%s/%s' % (account, container, obj))
        df = self._simple_get_diskfile(account=account, container=container,
                                       obj=obj)
        self._create_ondisk_file(df, data, timestamp, metadata)
        df = self._simple_get_diskfile(account=account, container=container,
                                       obj=obj)
        df.open()
        return df

    def test_open_not_exist(self):
        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileNotExist, df.open)

    def test_open_expired(self):
        self.assertRaises(DiskFileExpired,
                          self._create_test_file,
                          '1234567890', metadata={'X-Delete-At': '0'})

    def test_open_not_expired(self):
        try:
            self._create_test_file(
                '1234567890', metadata={'X-Delete-At': str(2 * int(time()))})
        except SwiftException as err:
            self.fail("Unexpected swift exception raised: %r" % err)

    def test_get_metadata(self):
        df = self._create_test_file('1234567890', timestamp=42)
        md = df.get_metadata()
        self.assertEqual(md['X-Timestamp'], Timestamp(42).internal)

    def test_read_metadata(self):
        self._create_test_file('1234567890', timestamp=42)
        df = self._simple_get_diskfile()
        md = df.read_metadata()
        self.assertEqual(md['X-Timestamp'], Timestamp(42).internal)

    def test_get_metadata_not_opened(self):
        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileNotOpen, df.get_metadata)

    def test_not_opened(self):
        df = self._simple_get_diskfile()
        try:
            with df:
                pass
        except DiskFileNotOpen:
            pass
        else:
            self.fail("Expected DiskFileNotOpen exception")

    def test_disk_file_default_disallowed_metadata(self):
        # build an object with some meta (ts 41)
        orig_metadata = {'X-Object-Meta-Key1': 'Value1',
                         'Content-Type': 'text/garbage'}
        df = self._get_open_disk_file(ts=41, extra_metadata=orig_metadata)
        with df.open():
            self.assertEquals('1024', df._metadata['Content-Length'])
        # write some new metadata (fast POST, don't send orig meta, ts 42)
        df = self._simple_get_diskfile()
        df.write_metadata({'X-Timestamp': Timestamp(42).internal,
                           'X-Object-Meta-Key2': 'Value2'})
        df = self._simple_get_diskfile()
        with df.open():
            # non-fast-post updateable keys are preserved
            self.assertEquals('text/garbage', df._metadata['Content-Type'])
            # original fast-post updateable keys are removed
            self.assert_('X-Object-Meta-Key1' not in df._metadata)
            # new fast-post updateable keys are added
            self.assertEquals('Value2', df._metadata['X-Object-Meta-Key2'])

    def test_disk_file_preserves_sysmeta(self):
        # build an object with some meta (ts 41)
        orig_metadata = {'X-Object-Sysmeta-Key1': 'Value1',
                         'Content-Type': 'text/garbage'}
        df = self._get_open_disk_file(ts=41, extra_metadata=orig_metadata)
        with df.open():
            self.assertEquals('1024', df._metadata['Content-Length'])
        # write some new metadata (fast POST, don't send orig meta, ts 42)
        df = self._simple_get_diskfile()
        df.write_metadata({'X-Timestamp': Timestamp(42).internal,
                           'X-Object-Sysmeta-Key1': 'Value2',
                           'X-Object-Meta-Key3': 'Value3'})
        df = self._simple_get_diskfile()
        with df.open():
            # non-fast-post updateable keys are preserved
            self.assertEquals('text/garbage', df._metadata['Content-Type'])
            # original sysmeta keys are preserved
            self.assertEquals('Value1', df._metadata['X-Object-Sysmeta-Key1'])

    def test_disk_file_reader_iter(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(''.join(reader), '1234567890')
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_reader_iter_w_quarantine(self):
        df = self._create_test_file('1234567890')

        def raise_dfq(m):
            raise DiskFileQuarantined(m)

        reader = df.reader(_quarantine_hook=raise_dfq)
        reader._obj_size += 1
        self.assertRaises(DiskFileQuarantined, ''.join, reader)

    def test_disk_file_app_iter_corners(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEquals(''.join(reader.app_iter_range(0, None)),
                          '1234567890')
        self.assertEquals(quarantine_msgs, [])
        df = self._simple_get_diskfile()
        with df.open():
            reader = df.reader()
            self.assertEqual(''.join(reader.app_iter_range(5, None)), '67890')

    def test_disk_file_app_iter_range_w_none(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        self.assertEqual(''.join(reader.app_iter_range(None, None)),
                         '1234567890')
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_partial_closes(self):
        df = self._create_test_file('1234567890')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_range(0, 5)
        self.assertEqual(''.join(it), '12345')
        self.assertEqual(quarantine_msgs, [])
        self.assertTrue(reader._fp is None)

    def test_disk_file_app_iter_ranges(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(0, 10), (10, 20), (20, 30)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('0123456789' in value)
        self.assertTrue('1123456789' in value)
        self.assertTrue('2123456789' in value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_ranges_w_quarantine(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        reader._obj_size += 1
        it = reader.app_iter_ranges([(0, 30)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('0123456789' in value)
        self.assertTrue('1123456789' in value)
        self.assertTrue('2123456789' in value)
        self.assertEqual(quarantine_msgs,
                         ["Bytes read: 30, does not match metadata: 31"])

    def test_disk_file_app_iter_ranges_w_no_etag_quarantine(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(0, 10)],
                                    'plain/text',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('0123456789' in value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_app_iter_ranges_edges(self):
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(3, 10), (0, 2)], 'application/whatever',
                                    '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assertTrue('3456789' in value)
        self.assertTrue('01' in value)
        self.assertEqual(quarantine_msgs, [])

    def test_disk_file_large_app_iter_ranges(self):
        # This test case is to make sure that the disk file app_iter_ranges
        # method all the paths being tested.
        long_str = '01234567890' * 65536
        target_strs = ['3456789', long_str[0:65590]]
        df = self._create_test_file(long_str)
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([(3, 10), (0, 65590)], 'plain/text',
                                    '5e816ff8b8b8e9a5d355497e5d9e0301', 655360)

        # The produced string actually missing the MIME headers
        # need to add these headers to make it as real MIME message.
        # The body of the message is produced by method app_iter_ranges
        # off of DiskFile object.
        header = ''.join(['Content-Type: multipart/byteranges;',
                          'boundary=',
                          '5e816ff8b8b8e9a5d355497e5d9e0301\r\n'])

        value = header + ''.join(it)
        self.assertEquals(quarantine_msgs, [])

        parts = map(lambda p: p.get_payload(decode=True),
                    email.message_from_string(value).walk())[1:3]
        self.assertEqual(parts, target_strs)

    def test_disk_file_app_iter_ranges_empty(self):
        # This test case tests when empty value passed into app_iter_ranges
        # When ranges passed into the method is either empty array or None,
        # this method will yield empty string
        df = self._create_test_file('012345678911234567892123456789')
        quarantine_msgs = []
        reader = df.reader(_quarantine_hook=quarantine_msgs.append)
        it = reader.app_iter_ranges([], 'application/whatever',
                                    '\r\n--someheader\r\n', 100)
        self.assertEqual(''.join(it), '')

        df = self._simple_get_diskfile()
        with df.open():
            reader = df.reader()
            it = reader.app_iter_ranges(None, 'app/something',
                                        '\r\n--someheader\r\n', 150)
            self.assertEqual(''.join(it), '')
            self.assertEqual(quarantine_msgs, [])

    def test_disk_file_mkstemp_creates_dir(self):
        for policy in POLICIES:
            tmpdir = os.path.join(self.testdir, self.existing_device,
                                  get_tmp_dir(policy.idx))
            os.rmdir(tmpdir)
            df = self._simple_get_diskfile(policy_idx=policy.idx)
            with df.create():
                self.assert_(os.path.exists(tmpdir))

    def _get_open_disk_file(self, invalid_type=None, obj_name='o', fsize=1024,
                            csize=8, mark_deleted=False, prealloc=False,
                            ts=None, mount_check=False, extra_metadata=None):
        '''returns a DiskFile'''
        df = self._simple_get_diskfile(obj=obj_name)
        data = '0' * fsize
        etag = md5()
        if ts:
            timestamp = ts
        else:
            timestamp = Timestamp(time()).internal
        if prealloc:
            prealloc_size = fsize
        else:
            prealloc_size = None
        with df.create(size=prealloc_size) as writer:
            upload_size = writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(upload_size),
            }
            metadata.update(extra_metadata or {})
            writer.put(metadata)
            if invalid_type == 'ETag':
                etag = md5()
                etag.update('1' + '0' * (fsize - 1))
                etag = etag.hexdigest()
                metadata['ETag'] = etag
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Content-Length':
                metadata['Content-Length'] = fsize - 1
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Bad-Content-Length':
                metadata['Content-Length'] = 'zero'
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Missing-Content-Length':
                del metadata['Content-Length']
                diskfile.write_metadata(writer._fd, metadata)
            elif invalid_type == 'Bad-X-Delete-At':
                metadata['X-Delete-At'] = 'bad integer'
                diskfile.write_metadata(writer._fd, metadata)

        if mark_deleted:
            df.delete(timestamp)

        data_files = [os.path.join(df._datadir, fname)
                      for fname in sorted(os.listdir(df._datadir),
                                          reverse=True)
                      if fname.endswith('.data')]
        if invalid_type == 'Corrupt-Xattrs':
            # We have to go below read_metadata/write_metadata to get proper
            # corruption.
            meta_xattr = xattr.getxattr(data_files[0], "user.swift.metadata")
            wrong_byte = 'X' if meta_xattr[0] != 'X' else 'Y'
            xattr.setxattr(data_files[0], "user.swift.metadata",
                           wrong_byte + meta_xattr[1:])
        elif invalid_type == 'Truncated-Xattrs':
            meta_xattr = xattr.getxattr(data_files[0], "user.swift.metadata")
            xattr.setxattr(data_files[0], "user.swift.metadata",
                           meta_xattr[:-1])
        elif invalid_type == 'Missing-Name':
            md = diskfile.read_metadata(data_files[0])
            del md['name']
            diskfile.write_metadata(data_files[0], md)
        elif invalid_type == 'Bad-Name':
            md = diskfile.read_metadata(data_files[0])
            md['name'] = md['name'] + 'garbage'
            diskfile.write_metadata(data_files[0], md)

        self.conf['disk_chunk_size'] = csize
        self.conf['mount_check'] = mount_check
        self.df_mgr = diskfile.DiskFileManager(self.conf, FakeLogger())
        df = self._simple_get_diskfile(obj=obj_name)
        df.open()
        if invalid_type == 'Zero-Byte':
            fp = open(df._data_file, 'w')
            fp.close()
        df.unit_test_len = fsize
        return df

    def test_keep_cache(self):
        df = self._get_open_disk_file(fsize=65)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as foo:
            for _ in df.reader():
                pass
            self.assertTrue(foo.called)

        df = self._get_open_disk_file(fsize=65)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as bar:
            for _ in df.reader(keep_cache=False):
                pass
            self.assertTrue(bar.called)

        df = self._get_open_disk_file(fsize=65)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as boo:
            for _ in df.reader(keep_cache=True):
                pass
            self.assertFalse(boo.called)

        df = self._get_open_disk_file(fsize=5 * 1024, csize=256)
        with mock.patch("swift.obj.diskfile.drop_buffer_cache") as goo:
            for _ in df.reader(keep_cache=True):
                pass
            self.assertTrue(goo.called)

    def test_quarantine_valids(self):

        def verify(*args, **kwargs):
            try:
                df = self._get_open_disk_file(**kwargs)
                reader = df.reader()
                for chunk in reader:
                    pass
            except DiskFileQuarantined:
                self.fail(
                    "Unexpected quarantining occurred: args=%r, kwargs=%r" % (
                        args, kwargs))
            else:
                pass

        verify(obj_name='1')

        verify(obj_name='2', csize=1)

        verify(obj_name='3', csize=100000)

    def run_quarantine_invalids(self, invalid_type):

        def verify(*args, **kwargs):
            open_exc = invalid_type in ('Content-Length', 'Bad-Content-Length',
                                        'Corrupt-Xattrs', 'Truncated-Xattrs',
                                        'Missing-Name', 'Bad-X-Delete-At')
            open_collision = invalid_type == 'Bad-Name'
            reader = None
            quarantine_msgs = []
            try:
                df = self._get_open_disk_file(**kwargs)
                reader = df.reader(_quarantine_hook=quarantine_msgs.append)
            except DiskFileQuarantined as err:
                if not open_exc:
                    self.fail(
                        "Unexpected DiskFileQuarantine raised: %r" % err)
                return
            except DiskFileCollision as err:
                if not open_collision:
                    self.fail(
                        "Unexpected DiskFileCollision raised: %r" % err)
                return
            else:
                if open_exc:
                    self.fail("Expected DiskFileQuarantine exception")
            try:
                for chunk in reader:
                    pass
            except DiskFileQuarantined as err:
                self.fail("Unexpected DiskFileQuarantine raised: :%r" % err)
            else:
                if not open_exc:
                    self.assertEqual(1, len(quarantine_msgs))

        verify(invalid_type=invalid_type, obj_name='1')

        verify(invalid_type=invalid_type, obj_name='2', csize=1)

        verify(invalid_type=invalid_type, obj_name='3', csize=100000)

        verify(invalid_type=invalid_type, obj_name='4')

        def verify_air(params, start=0, adjustment=0):
            """verify (a)pp (i)ter (r)ange"""
            open_exc = invalid_type in ('Content-Length', 'Bad-Content-Length',
                                        'Corrupt-Xattrs', 'Truncated-Xattrs',
                                        'Missing-Name', 'Bad-X-Delete-At')
            open_collision = invalid_type == 'Bad-Name'
            reader = None
            try:
                df = self._get_open_disk_file(**params)
                reader = df.reader()
            except DiskFileQuarantined as err:
                if not open_exc:
                    self.fail(
                        "Unexpected DiskFileQuarantine raised: %r" % err)
                return
            except DiskFileCollision as err:
                if not open_collision:
                    self.fail(
                        "Unexpected DiskFileCollision raised: %r" % err)
                return
            else:
                if open_exc:
                    self.fail("Expected DiskFileQuarantine exception")
            try:
                for chunk in reader.app_iter_range(
                        start,
                        df.unit_test_len + adjustment):
                    pass
            except DiskFileQuarantined as err:
                self.fail("Unexpected DiskFileQuarantine raised: :%r" % err)

        verify_air(dict(invalid_type=invalid_type, obj_name='5'))

        verify_air(dict(invalid_type=invalid_type, obj_name='6'), 0, 100)

        verify_air(dict(invalid_type=invalid_type, obj_name='7'), 1)

        verify_air(dict(invalid_type=invalid_type, obj_name='8'), 0, -1)

        verify_air(dict(invalid_type=invalid_type, obj_name='8'), 1, 1)

    def test_quarantine_corrupt_xattrs(self):
        self.run_quarantine_invalids('Corrupt-Xattrs')

    def test_quarantine_truncated_xattrs(self):
        self.run_quarantine_invalids('Truncated-Xattrs')

    def test_quarantine_invalid_etag(self):
        self.run_quarantine_invalids('ETag')

    def test_quarantine_invalid_missing_name(self):
        self.run_quarantine_invalids('Missing-Name')

    def test_quarantine_invalid_bad_name(self):
        self.run_quarantine_invalids('Bad-Name')

    def test_quarantine_invalid_bad_x_delete_at(self):
        self.run_quarantine_invalids('Bad-X-Delete-At')

    def test_quarantine_invalid_content_length(self):
        self.run_quarantine_invalids('Content-Length')

    def test_quarantine_invalid_content_length_bad(self):
        self.run_quarantine_invalids('Bad-Content-Length')

    def test_quarantine_invalid_zero_byte(self):
        self.run_quarantine_invalids('Zero-Byte')

    def test_quarantine_deleted_files(self):
        try:
            self._get_open_disk_file(invalid_type='Content-Length')
        except DiskFileQuarantined:
            pass
        else:
            self.fail("Expected DiskFileQuarantined exception")
        try:
            self._get_open_disk_file(invalid_type='Content-Length',
                                     mark_deleted=True)
        except DiskFileQuarantined as err:
            self.fail("Unexpected DiskFileQuarantined exception"
                      " encountered: %r" % err)
        except DiskFileNotExist:
            pass
        else:
            self.fail("Expected DiskFileNotExist exception")
        try:
            self._get_open_disk_file(invalid_type='Content-Length',
                                     mark_deleted=True)
        except DiskFileNotExist:
            pass
        else:
            self.fail("Expected DiskFileNotExist exception")

    def test_quarantine_missing_content_length(self):
        self.assertRaises(
            DiskFileQuarantined,
            self._get_open_disk_file,
            invalid_type='Missing-Content-Length')

    def test_quarantine_bad_content_length(self):
        self.assertRaises(
            DiskFileQuarantined,
            self._get_open_disk_file,
            invalid_type='Bad-Content-Length')

    def test_quarantine_fstat_oserror(self):
        invocations = [0]
        orig_os_fstat = os.fstat

        def bad_fstat(fd):
            invocations[0] += 1
            if invocations[0] == 4:
                # FIXME - yes, this an icky way to get code coverage ... worth
                # it?
                raise OSError()
            return orig_os_fstat(fd)

        with mock.patch('os.fstat', bad_fstat):
            self.assertRaises(
                DiskFileQuarantined,
                self._get_open_disk_file)

    def test_quarantine_hashdir_not_a_directory(self):
        df = self._create_test_file('1234567890', account="abc",
                                    container='123', obj='xyz')
        hashdir = df._datadir
        rmtree(hashdir)
        with open(hashdir, 'w'):
            pass

        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz')
        self.assertRaises(DiskFileQuarantined, df.open)

        # make sure the right thing got quarantined; the suffix dir should not
        # have moved, as that could have many objects in it
        self.assertFalse(os.path.exists(hashdir))
        self.assertTrue(os.path.exists(os.path.dirname(hashdir)))

    def test_create_prealloc(self):
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz')
        with mock.patch("swift.obj.diskfile.fallocate") as fa:
            with df.create(size=200) as writer:
                used_fd = writer._fd
        fa.assert_called_with(used_fd, 200)

    def test_create_prealloc_oserror(self):
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz')
        with mock.patch("swift.obj.diskfile.fallocate",
                        mock.MagicMock(side_effect=OSError(
                            errno.EACCES, os.strerror(errno.EACCES)))):
            try:
                with df.create(size=200):
                    pass
            except DiskFileNoSpace:
                pass
            else:
                self.fail("Expected exception DiskFileNoSpace")

    def test_create_close_oserror(self):
        df = self.df_mgr.get_diskfile(self.existing_device, '0', 'abc', '123',
                                      'xyz')
        with mock.patch("swift.obj.diskfile.os.close",
                        mock.MagicMock(side_effect=OSError(
                            errno.EACCES, os.strerror(errno.EACCES)))):
            try:
                with df.create(size=200):
                    pass
            except Exception as err:
                self.fail("Unexpected exception raised: %r" % err)
            else:
                pass

    def test_write_metadata(self):
        df = self._create_test_file('1234567890')
        timestamp = Timestamp(time()).internal
        metadata = {'X-Timestamp': timestamp, 'X-Object-Meta-test': 'data'}
        df.write_metadata(metadata)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 2)
        exp_name = '%s.meta' % timestamp
        self.assertTrue(exp_name in set(dl))

    def test_delete(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % Timestamp(ts).internal
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))

    def test_open_deleted(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileDeleted, df.open)

    def test_open_deleted_with_corrupt_tombstone(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        # it's pickle-format, so removing the last byte is sufficient to
        # corrupt it
        ts_fullpath = os.path.join(df._datadir, exp_name)
        self.assertTrue(os.path.exists(ts_fullpath))  # sanity check
        meta_xattr = xattr.getxattr(ts_fullpath, "user.swift.metadata")
        xattr.setxattr(ts_fullpath, "user.swift.metadata", meta_xattr[:-1])

        df = self._simple_get_diskfile()
        self.assertRaises(DiskFileNotExist, df.open)
        self.assertFalse(os.path.exists(ts_fullpath))

    def test_from_audit_location(self):
        hashdir = self._create_test_file(
            'blah blah',
            account='three', container='blind', obj='mice')._datadir
        df = self.df_mgr.get_diskfile_from_audit_location(
            diskfile.AuditLocation(hashdir, self.existing_device, '0'))
        df.open()
        self.assertEqual(df._name, '/three/blind/mice')

    def test_from_audit_location_with_mismatched_hash(self):
        hashdir = self._create_test_file(
            'blah blah',
            account='this', container='is', obj='right')._datadir

        datafile = os.path.join(hashdir, os.listdir(hashdir)[0])
        meta = diskfile.read_metadata(datafile)
        meta['name'] = '/this/is/wrong'
        diskfile.write_metadata(datafile, meta)

        df = self.df_mgr.get_diskfile_from_audit_location(
            diskfile.AuditLocation(hashdir, self.existing_device, '0'))
        self.assertRaises(DiskFileQuarantined, df.open)

    def test_close_error(self):

        def mock_handle_close_quarantine():
            raise Exception("Bad")

        df = self._get_open_disk_file(fsize=1024 * 1024 * 2, csize=1024)
        reader = df.reader()
        reader._handle_close_quarantine = mock_handle_close_quarantine
        for chunk in reader:
            pass
        # close is called at the end of the iterator
        self.assertEquals(reader._fp, None)
        self.assertEquals(len(df._logger.log_dict['error']), 1)

    def test_mount_checking(self):

        def _mock_cm(*args, **kwargs):
            return False

        with mock.patch("swift.common.constraints.check_mount", _mock_cm):
            self.assertRaises(
                DiskFileDeviceUnavailable,
                self._get_open_disk_file,
                mount_check=True)

    def test_ondisk_search_loop_ts_meta_data(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, '', ext='.ts', timestamp=10)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=9)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=8)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=7)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=6)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=5)
        df = self._simple_get_diskfile()
        try:
            df.open()
        except DiskFileDeleted as d:
            self.assertEquals(d.timestamp, Timestamp(10).internal)
        else:
            self.fail("Expected DiskFileDeleted exception")

    def test_ondisk_search_loop_meta_ts_data(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, '', ext='.meta', timestamp=10)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=9)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=6)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=5)
        df = self._simple_get_diskfile()
        try:
            df.open()
        except DiskFileDeleted as d:
            self.assertEquals(d.timestamp, Timestamp(8).internal)
        else:
            self.fail("Expected DiskFileDeleted exception")

    def test_ondisk_search_loop_meta_data_ts(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, '', ext='.meta', timestamp=10)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=9)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=8)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=7)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=6)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEquals(df._metadata['X-Timestamp'],
                              Timestamp(10).internal)
            self.assertTrue('deleted' not in df._metadata)

    def test_ondisk_search_loop_data_meta_ts(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=10)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=9)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=6)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEquals(df._metadata['X-Timestamp'],
                              Timestamp(10).internal)
            self.assertTrue('deleted' not in df._metadata)

    def test_ondisk_search_loop_wayward_files_ignored(self):
        df = self._simple_get_diskfile()
        self._create_ondisk_file(df, 'X', ext='.bar', timestamp=11)
        self._create_ondisk_file(df, 'B', ext='.data', timestamp=10)
        self._create_ondisk_file(df, 'A', ext='.data', timestamp=9)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
        self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=6)
        self._create_ondisk_file(df, '', ext='.meta', timestamp=5)
        df = self._simple_get_diskfile()
        with df.open():
            self.assertTrue('X-Timestamp' in df._metadata)
            self.assertEquals(df._metadata['X-Timestamp'],
                              Timestamp(10).internal)
            self.assertTrue('deleted' not in df._metadata)

    def test_ondisk_search_loop_listdir_error(self):
        df = self._simple_get_diskfile()

        def mock_listdir_exp(*args, **kwargs):
            raise OSError(errno.EACCES, os.strerror(errno.EACCES))

        with mock.patch("os.listdir", mock_listdir_exp):
            self._create_ondisk_file(df, 'X', ext='.bar', timestamp=11)
            self._create_ondisk_file(df, 'B', ext='.data', timestamp=10)
            self._create_ondisk_file(df, 'A', ext='.data', timestamp=9)
            self._create_ondisk_file(df, '', ext='.ts', timestamp=8)
            self._create_ondisk_file(df, '', ext='.ts', timestamp=7)
            self._create_ondisk_file(df, '', ext='.meta', timestamp=6)
            self._create_ondisk_file(df, '', ext='.meta', timestamp=5)
            df = self._simple_get_diskfile()
            self.assertRaises(DiskFileError, df.open)

    def test_exception_in_handle_close_quarantine(self):
        df = self._get_open_disk_file()

        def blow_up():
            raise Exception('a very special error')

        reader = df.reader()
        reader._handle_close_quarantine = blow_up
        for _ in reader:
            pass
        reader.close()
        log_lines = df._logger.get_lines_for_level('error')
        self.assert_('a very special error' in log_lines[-1])

    def test_get_diskfile_from_hash_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileDeviceUnavailable,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)

    def test_get_diskfile_from_hash_not_dir(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata'),
                mock.patch('swift.obj.diskfile.quarantine_renamer')) as \
                (dfclass, hclistdir, readmeta, quarantine_renamer):
            osexc = OSError()
            osexc.errno = errno.ENOTDIR
            hclistdir.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)
            quarantine_renamer.assert_called_once_with(
                '/srv/dev/',
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900')

    def test_get_diskfile_from_hash_no_dir(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            osexc = OSError()
            osexc.errno = errno.ENOENT
            hclistdir.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)

    def test_get_diskfile_from_hash_other_oserror(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            osexc = OSError()
            hclistdir.side_effect = osexc
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                OSError,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)

    def test_get_diskfile_from_hash_no_actual_files(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = []
            readmeta.return_value = {'name': '/a/c/o'}
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)

    def test_get_diskfile_from_hash_read_metadata_problem(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.side_effect = EOFError()
            self.assertRaises(
                DiskFileNotExist,
                self.df_mgr.get_diskfile_from_hash,
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)

    def test_get_diskfile_from_hash_no_meta_name(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {}
            try:
                self.df_mgr.get_diskfile_from_hash(
                    'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)
            except DiskFileNotExist as err:
                exc = err
            self.assertEqual(str(exc), '')

    def test_get_diskfile_from_hash_bad_meta_name(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {'name': 'bad'}
            try:
                self.df_mgr.get_diskfile_from_hash(
                    'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)
            except DiskFileNotExist as err:
                exc = err
            self.assertEqual(str(exc), '')

    def test_get_diskfile_from_hash(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value='/srv/dev/')
        with nested(
                mock.patch('swift.obj.diskfile.DiskFile'),
                mock.patch('swift.obj.diskfile.hash_cleanup_listdir'),
                mock.patch('swift.obj.diskfile.read_metadata')) as \
                (dfclass, hclistdir, readmeta):
            hclistdir.return_value = ['1381679759.90941.data']
            readmeta.return_value = {'name': '/a/c/o'}
            self.df_mgr.get_diskfile_from_hash(
                'dev', '9', '9a7175077c01a23ade5956b8a2bba900', 0)
            dfclass.assert_called_once_with(
                self.df_mgr, '/srv/dev/', self.df_mgr.threadpools['dev'], '9',
                'a', 'c', 'o', policy_idx=0)
            hclistdir.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900',
                604800)
            readmeta.assert_called_once_with(
                '/srv/dev/objects/9/900/9a7175077c01a23ade5956b8a2bba900/'
                '1381679759.90941.data')

    def test_listdir_enoent(self):
        oserror = OSError()
        oserror.errno = errno.ENOENT
        self.df_mgr.logger.error = mock.MagicMock()
        with mock.patch('os.listdir', side_effect=oserror):
            self.assertEqual(self.df_mgr._listdir('path'), [])
            self.assertEqual(self.df_mgr.logger.error.mock_calls, [])

    def test_listdir_other_oserror(self):
        oserror = OSError()
        self.df_mgr.logger.error = mock.MagicMock()
        with mock.patch('os.listdir', side_effect=oserror):
            self.assertEqual(self.df_mgr._listdir('path'), [])
            self.df_mgr.logger.error.assert_called_once_with(
                'ERROR: Skipping %r due to error with listdir attempt: %s',
                'path', oserror)

    def test_listdir(self):
        self.df_mgr.logger.error = mock.MagicMock()
        with mock.patch('os.listdir', return_value=['abc', 'def']):
            self.assertEqual(self.df_mgr._listdir('path'), ['abc', 'def'])
            self.assertEqual(self.df_mgr.logger.error.mock_calls, [])

    def test_yield_suffixes_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        exc = None
        try:
            list(self.df_mgr.yield_suffixes('dev', '9', 0))
        except DiskFileDeviceUnavailable as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_yield_suffixes(self):
        self.df_mgr._listdir = mock.MagicMock(return_value=[
            'abc', 'def', 'ghi', 'abcd', '012'])
        self.assertEqual(
            list(self.df_mgr.yield_suffixes('dev', '9', 0)),
            [(self.testdir + '/dev/objects/9/abc', 'abc'),
             (self.testdir + '/dev/objects/9/def', 'def'),
             (self.testdir + '/dev/objects/9/012', '012')])

    def test_yield_hashes_dev_path_fail(self):
        self.df_mgr.get_dev_path = mock.MagicMock(return_value=None)
        exc = None
        try:
            list(self.df_mgr.yield_hashes('dev', '9', 0))
        except DiskFileDeviceUnavailable as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_yield_hashes_empty(self):
        def _listdir(path):
            return []

        with mock.patch('os.listdir', _listdir):
            self.assertEqual(list(self.df_mgr.yield_hashes('dev', '9', 0)), [])

    def test_yield_hashes_empty_suffixes(self):
        def _listdir(path):
            return []

        with mock.patch('os.listdir', _listdir):
            self.assertEqual(
                list(self.df_mgr.yield_hashes('dev', '9', 0,
                                              suffixes=['456'])), [])

    def test_yield_hashes(self):
        fresh_ts = Timestamp(time() - 10).internal
        fresher_ts = Timestamp(time() - 1).internal

        def _listdir(path):
            if path.endswith('/dev/objects/9'):
                return ['abc', '456', 'def']
            elif path.endswith('/dev/objects/9/abc'):
                return ['9373a92d072897b136b3fc06595b4abc']
            elif path.endswith(
                    '/dev/objects/9/abc/9373a92d072897b136b3fc06595b4abc'):
                return [fresh_ts + '.ts']
            elif path.endswith('/dev/objects/9/456'):
                return ['9373a92d072897b136b3fc06595b0456',
                        '9373a92d072897b136b3fc06595b7456']
            elif path.endswith(
                    '/dev/objects/9/456/9373a92d072897b136b3fc06595b0456'):
                return ['1383180000.12345.data']
            elif path.endswith(
                    '/dev/objects/9/456/9373a92d072897b136b3fc06595b7456'):
                return [fresh_ts + '.ts',
                        fresher_ts + '.data']
            elif path.endswith('/dev/objects/9/def'):
                return []
            else:
                raise Exception('Unexpected listdir of %r' % path)

        with nested(
                mock.patch('os.listdir', _listdir),
                mock.patch('os.unlink')):
            self.assertEqual(
                list(self.df_mgr.yield_hashes('dev', '9', 0)),
                [(self.testdir +
                  '/dev/objects/9/abc/9373a92d072897b136b3fc06595b4abc',
                  '9373a92d072897b136b3fc06595b4abc', fresh_ts),
                 (self.testdir +
                  '/dev/objects/9/456/9373a92d072897b136b3fc06595b0456',
                  '9373a92d072897b136b3fc06595b0456', '1383180000.12345'),
                 (self.testdir +
                  '/dev/objects/9/456/9373a92d072897b136b3fc06595b7456',
                  '9373a92d072897b136b3fc06595b7456', fresher_ts)])

    def test_yield_hashes_suffixes(self):
        fresh_ts = Timestamp(time() - 10).internal
        fresher_ts = Timestamp(time() - 1).internal

        def _listdir(path):
            if path.endswith('/dev/objects/9'):
                return ['abc', '456', 'def']
            elif path.endswith('/dev/objects/9/abc'):
                return ['9373a92d072897b136b3fc06595b4abc']
            elif path.endswith(
                    '/dev/objects/9/abc/9373a92d072897b136b3fc06595b4abc'):
                return [fresh_ts + '.ts']
            elif path.endswith('/dev/objects/9/456'):
                return ['9373a92d072897b136b3fc06595b0456',
                        '9373a92d072897b136b3fc06595b7456']
            elif path.endswith(
                    '/dev/objects/9/456/9373a92d072897b136b3fc06595b0456'):
                return ['1383180000.12345.data']
            elif path.endswith(
                    '/dev/objects/9/456/9373a92d072897b136b3fc06595b7456'):
                return [fresh_ts + '.ts',
                        fresher_ts + '.data']
            elif path.endswith('/dev/objects/9/def'):
                return []
            else:
                raise Exception('Unexpected listdir of %r' % path)

        with nested(
                mock.patch('os.listdir', _listdir),
                mock.patch('os.unlink')):
            self.assertEqual(
                list(self.df_mgr.yield_hashes(
                    'dev', '9', 0, suffixes=['456'])),
                [(self.testdir +
                  '/dev/objects/9/456/9373a92d072897b136b3fc06595b0456',
                  '9373a92d072897b136b3fc06595b0456', '1383180000.12345'),
                 (self.testdir +
                  '/dev/objects/9/456/9373a92d072897b136b3fc06595b7456',
                  '9373a92d072897b136b3fc06595b7456', fresher_ts)])

    def test_diskfile_names(self):
        df = self._simple_get_diskfile()
        self.assertEqual(df.account, 'a')
        self.assertEqual(df.container, 'c')
        self.assertEqual(df.obj, 'o')

    def test_diskfile_content_length_not_open(self):
        df = self._simple_get_diskfile()
        exc = None
        try:
            df.content_length
        except DiskFileNotOpen as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_content_length_deleted(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        df = self._simple_get_diskfile()
        exc = None
        try:
            with df.open():
                df.content_length
        except DiskFileDeleted as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_content_length(self):
        self._get_open_disk_file()
        df = self._simple_get_diskfile()
        with df.open():
            self.assertEqual(df.content_length, 1024)

    def test_diskfile_timestamp_not_open(self):
        df = self._simple_get_diskfile()
        exc = None
        try:
            df.timestamp
        except DiskFileNotOpen as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_timestamp_deleted(self):
        df = self._get_open_disk_file()
        ts = time()
        df.delete(ts)
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 1)
        self.assertTrue(exp_name in set(dl))
        df = self._simple_get_diskfile()
        exc = None
        try:
            with df.open():
                df.timestamp
        except DiskFileDeleted as err:
            exc = err
        self.assertEqual(str(exc), '')

    def test_diskfile_timestamp(self):
        self._get_open_disk_file(ts='1383181759.12345')
        df = self._simple_get_diskfile()
        with df.open():
            self.assertEqual(df.timestamp, '1383181759.12345')

    def test_error_in_hash_cleanup_listdir(self):

        def mock_hcl(*args, **kwargs):
            raise OSError()

        df = self._get_open_disk_file()
        ts = time()
        with mock.patch("swift.obj.diskfile.hash_cleanup_listdir",
                        mock_hcl):
            try:
                df.delete(ts)
            except OSError:
                self.fail("OSError raised when it should have been swallowed")
        exp_name = '%s.ts' % str(Timestamp(ts).internal)
        dl = os.listdir(df._datadir)
        self.assertEquals(len(dl), 2)
        self.assertTrue(exp_name in set(dl))


if __name__ == '__main__':
    unittest.main()
