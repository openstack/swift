#-*- coding:utf-8 -*-
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

""" Tests for swift.obj.diskfile """

import cPickle as pickle
import os
import mock
import unittest
import email
from shutil import rmtree
from time import time
from tempfile import mkdtemp
from hashlib import md5

from eventlet import tpool
from test.unit import FakeLogger
from test.unit import _setxattr as setxattr
from swift.obj import diskfile
from swift.common.utils import mkdirs, normalize_timestamp
from swift.common.exceptions import DiskFileNotExist, DiskFileDeviceUnavailable


class TestDiskFile(unittest.TestCase):
    """Test swift.obj.diskfile.DiskFile"""

    def setUp(self):
        """ Set up for testing swift.obj.diskfile"""
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_obj_server_DiskFile')
        mkdirs(os.path.join(self.testdir, 'sda1', 'tmp'))
        self._orig_tpool_exc = tpool.execute
        tpool.execute = lambda f, *args, **kwargs: f(*args, **kwargs)

    def tearDown(self):
        """ Tear down for testing swift.obj.diskfile"""
        rmtree(os.path.dirname(self.testdir))
        tpool.execute = self._orig_tpool_exc

    def _create_test_file(self, data, keep_data_fp=True):
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        f.write(data)
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))
        f.close()
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=keep_data_fp)
        return df

    def test_disk_file_app_iter_corners(self):
        df = self._create_test_file('1234567890')
        self.assertEquals(''.join(df.app_iter_range(0, None)), '1234567890')

        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=True)
        self.assertEqual(''.join(df.app_iter_range(5, None)), '67890')

    def test_disk_file_app_iter_ranges(self):
        df = self._create_test_file('012345678911234567892123456789')
        it = df.app_iter_ranges([(0, 10), (10, 20), (20, 30)], 'plain/text',
                                '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assert_('0123456789' in value)
        self.assert_('1123456789' in value)
        self.assert_('2123456789' in value)

    def test_disk_file_app_iter_ranges_edges(self):
        df = self._create_test_file('012345678911234567892123456789')
        it = df.app_iter_ranges([(3, 10), (0, 2)], 'application/whatever',
                                '\r\n--someheader\r\n', 30)
        value = ''.join(it)
        self.assert_('3456789' in value)
        self.assert_('01' in value)

    def test_disk_file_large_app_iter_ranges(self):
        """
        This test case is to make sure that the disk file app_iter_ranges
        method all the paths being tested.
        """
        long_str = '01234567890' * 65536
        target_strs = ['3456789', long_str[0:65590]]
        df = self._create_test_file(long_str)

        it = df.app_iter_ranges([(3, 10), (0, 65590)], 'plain/text',
                                '5e816ff8b8b8e9a5d355497e5d9e0301', 655360)

        """
        the produced string actually missing the MIME headers
        need to add these headers to make it as real MIME message.
        The body of the message is produced by method app_iter_ranges
        off of DiskFile object.
        """
        header = ''.join(['Content-Type: multipart/byteranges;',
                          'boundary=',
                          '5e816ff8b8b8e9a5d355497e5d9e0301\r\n'])

        value = header + ''.join(it)

        parts = map(lambda p: p.get_payload(decode=True),
                    email.message_from_string(value).walk())[1:3]
        self.assertEqual(parts, target_strs)

    def test_disk_file_app_iter_ranges_empty(self):
        """
        This test case tests when empty value passed into app_iter_ranges
        When ranges passed into the method is either empty array or None,
        this method will yield empty string
        """
        df = self._create_test_file('012345678911234567892123456789')
        it = df.app_iter_ranges([], 'application/whatever',
                                '\r\n--someheader\r\n', 100)
        self.assertEqual(''.join(it), '')

        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=True)
        it = df.app_iter_ranges(None, 'app/something',
                                '\r\n--someheader\r\n', 150)
        self.assertEqual(''.join(it), '')

    def test_disk_file_mkstemp_creates_dir(self):
        tmpdir = os.path.join(self.testdir, 'sda1', 'tmp')
        os.rmdir(tmpdir)
        with diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                                    'o', FakeLogger()).writer():
            self.assert_(os.path.exists(tmpdir))

    def test_iter_hook(self):
        hook_call_count = [0]

        def hook():
            hook_call_count[0] += 1

        df = self._get_disk_file(fsize=65, csize=8, iter_hook=hook)
        for _ in df:
            pass

        self.assertEquals(hook_call_count[0], 9)

    def test_quarantine(self):
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        df.quarantine()
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined',
                                'objects', os.path.basename(os.path.dirname(
                                                            df.data_file)))
        self.assert_(os.path.isdir(quar_dir))

    def test_quarantine_same_file(self):
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger())
        new_dir = df.quarantine()
        quar_dir = os.path.join(self.testdir, 'sda1', 'quarantined',
                                'objects', os.path.basename(os.path.dirname(
                                                            df.data_file)))
        self.assert_(os.path.isdir(quar_dir))
        self.assertEquals(quar_dir, new_dir)
        # have to remake the datadir and file
        mkdirs(df.datadir)
        f = open(os.path.join(df.datadir,
                              normalize_timestamp(time()) + '.data'), 'wb')
        setxattr(f.fileno(), diskfile.METADATA_KEY,
                 pickle.dumps({}, diskfile.PICKLE_PROTOCOL))

        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c', 'o',
                                    FakeLogger(), keep_data_fp=True)
        double_uuid_path = df.quarantine()
        self.assert_(os.path.isdir(double_uuid_path))
        self.assert_('-' in os.path.basename(double_uuid_path))

    def _get_disk_file(self, invalid_type=None, obj_name='o',
                       fsize=1024, csize=8, mark_deleted=False, ts=None,
                       iter_hook=None, mount_check=False):
        '''returns a DiskFile'''
        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                                    obj_name, FakeLogger())
        data = '0' * fsize
        etag = md5()
        if ts:
            timestamp = ts
        else:
            timestamp = str(normalize_timestamp(time()))
        with df.writer() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer.fd).st_size),
            }
            writer.put(metadata)
            if invalid_type == 'ETag':
                etag = md5()
                etag.update('1' + '0' * (fsize - 1))
                etag = etag.hexdigest()
                metadata['ETag'] = etag
                diskfile.write_metadata(writer.fd, metadata)
            if invalid_type == 'Content-Length':
                metadata['Content-Length'] = fsize - 1
                diskfile.write_metadata(writer.fd, metadata)

        if mark_deleted:
            metadata = {
                'X-Timestamp': timestamp,
                'deleted': True
            }
            df.put_metadata(metadata, tombstone=True)

        df = diskfile.DiskFile(self.testdir, 'sda1', '0', 'a', 'c',
                               obj_name, FakeLogger(),
                               keep_data_fp=True, disk_chunk_size=csize,
                               iter_hook=iter_hook, mount_check=mount_check)
        if invalid_type == 'Zero-Byte':
            os.remove(df.data_file)
            fp = open(df.data_file, 'w')
            fp.close()
        df.unit_test_len = fsize
        return df

    def test_quarantine_valids(self):
        df = self._get_disk_file(obj_name='1')
        for chunk in df:
            pass
        self.assertFalse(df.quarantined_dir)

        df = self._get_disk_file(obj_name='2', csize=1)
        for chunk in df:
            pass
        self.assertFalse(df.quarantined_dir)

        df = self._get_disk_file(obj_name='3', csize=100000)
        for chunk in df:
            pass
        self.assertFalse(df.quarantined_dir)

    def run_quarantine_invalids(self, invalid_type):
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='1')
        for chunk in df:
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type,
                                 obj_name='2', csize=1)
        for chunk in df:
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type,
                                 obj_name='3', csize=100000)
        for chunk in df:
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='4')
        self.assertFalse(df.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='5')
        for chunk in df.app_iter_range(0, df.unit_test_len):
            pass
        self.assertTrue(df.quarantined_dir)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='6')
        for chunk in df.app_iter_range(0, df.unit_test_len + 100):
            pass
        self.assertTrue(df.quarantined_dir)
        expected_quar = False
        # for the following, Content-Length/Zero-Byte errors will always result
        # in a quarantine, even if the whole file isn't check-summed
        if invalid_type in ('Zero-Byte', 'Content-Length'):
            expected_quar = True
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='7')
        for chunk in df.app_iter_range(1, df.unit_test_len):
            pass
        self.assertEquals(bool(df.quarantined_dir), expected_quar)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='8')
        for chunk in df.app_iter_range(0, df.unit_test_len - 1):
            pass
        self.assertEquals(bool(df.quarantined_dir), expected_quar)
        df = self._get_disk_file(invalid_type=invalid_type, obj_name='8')
        for chunk in df.app_iter_range(1, df.unit_test_len + 1):
            pass
        self.assertEquals(bool(df.quarantined_dir), expected_quar)

    def test_quarantine_invalids(self):
        self.run_quarantine_invalids('ETag')
        self.run_quarantine_invalids('Content-Length')
        self.run_quarantine_invalids('Zero-Byte')

    def test_quarantine_deleted_files(self):
        df = self._get_disk_file(invalid_type='Content-Length')
        df.close()
        self.assertTrue(df.quarantined_dir)
        df = self._get_disk_file(invalid_type='Content-Length',
                                 mark_deleted=True)
        df.close()
        self.assertFalse(df.quarantined_dir)
        df = self._get_disk_file(invalid_type='Content-Length',
                                 mark_deleted=True)
        self.assertRaises(DiskFileNotExist, df.get_data_file_size)

    def test_put_metadata(self):
        df = self._get_disk_file()
        ts = time()
        metadata = {'X-Timestamp': ts, 'X-Object-Meta-test': 'data'}
        df.put_metadata(metadata)
        exp_name = '%s.meta' % str(normalize_timestamp(ts))
        dl = os.listdir(df.datadir)
        self.assertEquals(len(dl), 2)
        self.assertTrue(exp_name in set(dl))

    def test_put_metadata_ts(self):
        df = self._get_disk_file()
        ts = time()
        metadata = {'X-Timestamp': ts, 'X-Object-Meta-test': 'data'}
        df.put_metadata(metadata, tombstone=True)
        exp_name = '%s.ts' % str(normalize_timestamp(ts))
        dl = os.listdir(df.datadir)
        self.assertEquals(len(dl), 2)
        self.assertTrue(exp_name in set(dl))

    def test_unlinkold(self):
        df1 = self._get_disk_file()
        future_time = str(normalize_timestamp(time() + 100))
        self._get_disk_file(ts=future_time)
        self.assertEquals(len(os.listdir(df1.datadir)), 2)
        df1.unlinkold(future_time)
        self.assertEquals(len(os.listdir(df1.datadir)), 1)
        self.assertEquals(os.listdir(df1.datadir)[0], "%s.data" % future_time)

    def test_close_error(self):

        def err():
            raise Exception("bad")

        df = self._get_disk_file(fsize=1024 * 1024 * 2)
        df._handle_close_quarantine = err
        for chunk in df:
            pass
        # close is called at the end of the iterator
        self.assertEquals(df.fp, None)
        self.assertEquals(len(df.logger.log_dict['error']), 1)

    def test_quarantine_twice(self):
        df = self._get_disk_file(invalid_type='Content-Length')
        self.assert_(os.path.isfile(df.data_file))
        quar_dir = df.quarantine()
        self.assertFalse(os.path.isfile(df.data_file))
        self.assert_(os.path.isdir(quar_dir))
        self.assertEquals(df.quarantine(), None)

    def test_mount_checking(self):
        def _mock_ismount(*args, **kwargs):
            return False
        with mock.patch("os.path.ismount", _mock_ismount):
            self.assertRaises(DiskFileDeviceUnavailable, self._get_disk_file,
                              mount_check=True)
