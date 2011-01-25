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

# TODO: Tests
from test import unit as _setup_mocks
import unittest
import tempfile
import os
import time
from shutil import rmtree
from hashlib import md5
from tempfile import mkdtemp
from swift.obj import auditor
from swift.obj import server as object_server
from swift.obj.server import DiskFile, write_metadata
from swift.common.utils import hash_path, mkdirs, normalize_timestamp, renamer
from swift.obj.replicator import invalidate_hash
from swift.common.exceptions import AuditException


class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = \
            os.path.join(mkdtemp(), 'tmp_test_object_auditor')
        self.devices = os.path.join(self.testdir, 'node')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        os.mkdir(self.devices)
        os.mkdir(os.path.join(self.devices, 'sda'))
        self.objects = os.path.join(self.devices, 'sda', 'objects')

        os.mkdir(os.path.join(self.devices, 'sdb'))
        self.objects_2 = os.path.join(self.devices, 'sdb', 'objects')

        os.mkdir(self.objects)
        self.parts = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            os.mkdir(os.path.join(self.objects, part))

        self.conf = dict(
            devices=self.devices,
            mount_check='false')

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def test_object_audit_extra_data(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        cur_part = '0'
        disk_file = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o')
        data = '0' * 1024
        etag = md5()
        with disk_file.mkstemp() as (fd, tmppath):
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            timestamp = str(normalize_timestamp(time.time()))
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            disk_file.put(fd, tmppath, metadata)
            pre_quarantines = self.auditor.quarantines

            self.auditor.object_audit(
                os.path.join(disk_file.datadir, timestamp + '.data'),
                'sda', cur_part)
            self.assertEquals(self.auditor.quarantines, pre_quarantines)

            os.write(fd, 'extra_data')
            self.auditor.object_audit(
                os.path.join(disk_file.datadir, timestamp + '.data'),
                'sda', cur_part)
            self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_audit_diff_data(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        cur_part = '0'
        disk_file = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o')
        data = '0' * 1024
        etag = md5()
        timestamp = str(normalize_timestamp(time.time()))
        with disk_file.mkstemp() as (fd, tmppath):
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            disk_file.put(fd, tmppath, metadata)
            pre_quarantines = self.auditor.quarantines

            self.auditor.object_audit(
                os.path.join(disk_file.datadir, timestamp + '.data'),
                'sda', cur_part)
            self.assertEquals(self.auditor.quarantines, pre_quarantines)
            etag = md5()
            etag.update('1' + '0' * 1023)
            etag = etag.hexdigest()
            metadata['ETag'] = etag
            write_metadata(fd, metadata)

            self.auditor.object_audit(
                os.path.join(disk_file.datadir, timestamp + '.data'),
                'sda', cur_part)
            self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_audit_no_meta(self):
        cur_part = '0'
        disk_file = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o')
        timestamp = str(normalize_timestamp(time.time()))
        path = os.path.join(disk_file.datadir, timestamp + '.data')
        mkdirs(disk_file.datadir)
        fp = open(path, 'w')
        fp.write('0' * 1024)
        fp.close()
        invalidate_hash(os.path.dirname(disk_file.datadir))
        self.auditor = auditor.ObjectAuditor(self.conf)
        pre_quarantines = self.auditor.quarantines
        self.auditor.object_audit(
            os.path.join(disk_file.datadir, timestamp + '.data'),
            'sda', cur_part)
        self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_audit_bad_args(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        pre_errors = self.auditor.errors
        self.auditor.object_audit(5, 'sda', '0')
        self.assertEquals(self.auditor.errors, pre_errors + 1)
        pre_errors = self.auditor.errors
        self.auditor.object_audit('badpath', 'sda', '0')
        self.assertEquals(self.auditor.errors, pre_errors)  # just returns

    def test_object_run_once_pass(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.auditor.log_time = 0
        cur_part = '0'
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = self.auditor.quarantines
        disk_file = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o')
        data = '0' * 1024
        etag = md5()
        with disk_file.mkstemp() as (fd, tmppath):
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            disk_file.put(fd, tmppath, metadata)
            disk_file.close()
        self.auditor.run_once()
        self.assertEquals(self.auditor.quarantines, pre_quarantines)

    def test_object_run_once_no_sda(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        cur_part = '0'
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = self.auditor.quarantines
        disk_file = DiskFile(self.devices, 'sdb', cur_part, 'a', 'c', 'o')
        data = '0' * 1024
        etag = md5()
        with disk_file.mkstemp() as (fd, tmppath):
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            disk_file.put(fd, tmppath, metadata)
            disk_file.close()
            os.write(fd, 'extra_data')
        self.auditor.run_once()
        self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_run_once_multi_devices(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        cur_part = '0'
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = self.auditor.quarantines
        disk_file = DiskFile(self.devices, 'sda', cur_part, 'a', 'c', 'o')
        data = '0' * 10
        etag = md5()
        with disk_file.mkstemp() as (fd, tmppath):
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            disk_file.put(fd, tmppath, metadata)
            disk_file.close()
        self.auditor.run_once()
        disk_file = DiskFile(self.devices, 'sdb', cur_part, 'a', 'c', 'ob')
        data = '1' * 10
        etag = md5()
        with disk_file.mkstemp() as (fd, tmppath):
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            disk_file.put(fd, tmppath, metadata)
            disk_file.close()
            os.write(fd, 'extra_data')
        self.auditor.run_once()
        self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)


if __name__ == '__main__':
    unittest.main()
