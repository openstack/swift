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

from test import unit
import unittest
import tempfile
import os
import time
from shutil import rmtree
from hashlib import md5
from tempfile import mkdtemp
from test.unit import FakeLogger
from swift.obj import auditor
from swift.obj import server as object_server
from swift.obj.server import DiskFile, write_metadata, DATADIR
from swift.common.utils import hash_path, mkdirs, normalize_timestamp, \
    renamer, storage_directory
from swift.obj.replicator import invalidate_hash
from swift.common.exceptions import AuditException


class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_object_auditor')
        self.devices = os.path.join(self.testdir, 'node')
        self.logger = FakeLogger()
        rmtree(self.testdir, ignore_errors=1)
        mkdirs(os.path.join(self.devices, 'sda'))
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
        self.disk_file = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                                  self.logger)

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)
        unit.xattr_data = {}

    def test_object_audit_extra_data(self):
        self.auditor = auditor.AuditorWorker(self.conf, self.logger)
        data = '0' * 1024
        etag = md5()
        with self.disk_file.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            timestamp = str(normalize_timestamp(time.time()))
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            self.disk_file.put(fd, metadata)
            pre_quarantines = self.auditor.quarantines

            self.auditor.object_audit(
                os.path.join(self.disk_file.datadir, timestamp + '.data'),
                'sda', '0')
            self.assertEquals(self.auditor.quarantines, pre_quarantines)

            os.write(fd, 'extra_data')
            self.auditor.object_audit(
                os.path.join(self.disk_file.datadir, timestamp + '.data'),
                'sda', '0')
            self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_audit_diff_data(self):
        self.auditor = auditor.AuditorWorker(self.conf, self.logger)
        data = '0' * 1024
        etag = md5()
        timestamp = str(normalize_timestamp(time.time()))
        with self.disk_file.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            self.disk_file.put(fd, metadata)
            pre_quarantines = self.auditor.quarantines
            # remake so it will have metadata
            self.disk_file = DiskFile(self.devices, 'sda', '0', 'a', 'c', 'o',
                                      self.logger)

            self.auditor.object_audit(
                os.path.join(self.disk_file.datadir, timestamp + '.data'),
                'sda', '0')
            self.assertEquals(self.auditor.quarantines, pre_quarantines)
            etag = md5()
            etag.update('1' + '0' * 1023)
            etag = etag.hexdigest()
            metadata['ETag'] = etag
            write_metadata(fd, metadata)

            self.auditor.object_audit(
                os.path.join(self.disk_file.datadir, timestamp + '.data'),
                'sda', '0')
            self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_audit_no_meta(self):
        timestamp = str(normalize_timestamp(time.time()))
        path = os.path.join(self.disk_file.datadir, timestamp + '.data')
        mkdirs(self.disk_file.datadir)
        fp = open(path, 'w')
        fp.write('0' * 1024)
        fp.close()
        invalidate_hash(os.path.dirname(self.disk_file.datadir))
        self.auditor = auditor.AuditorWorker(self.conf, self.logger)
        pre_quarantines = self.auditor.quarantines
        self.auditor.object_audit(
            os.path.join(self.disk_file.datadir, timestamp + '.data'),
            'sda', '0')
        self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_audit_bad_args(self):
        self.auditor = auditor.AuditorWorker(self.conf, self.logger)
        pre_errors = self.auditor.errors
        self.auditor.object_audit(5, 'sda', '0')
        self.assertEquals(self.auditor.errors, pre_errors + 1)
        pre_errors = self.auditor.errors
        self.auditor.object_audit('badpath', 'sda', '0')
        self.assertEquals(self.auditor.errors, pre_errors)  # just returns

    def test_object_run_once_pass(self):
        self.auditor = auditor.AuditorWorker(self.conf, self.logger)
        self.auditor.log_time = 0
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = self.auditor.quarantines
        data = '0' * 1024
        etag = md5()
        with self.disk_file.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            self.disk_file.put(fd, metadata)
            self.disk_file.close()
        self.auditor.audit_all_objects()
        self.assertEquals(self.auditor.quarantines, pre_quarantines)

    def test_object_run_once_no_sda(self):
        self.auditor = auditor.AuditorWorker(self.conf, self.logger)
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = self.auditor.quarantines
        data = '0' * 1024
        etag = md5()
        with self.disk_file.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            self.disk_file.put(fd, metadata)
            self.disk_file.close()
            os.write(fd, 'extra_data')
        self.auditor.audit_all_objects()
        self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_run_once_multi_devices(self):
        self.auditor = auditor.AuditorWorker(self.conf, self.logger)
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = self.auditor.quarantines
        data = '0' * 10
        etag = md5()
        with self.disk_file.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            self.disk_file.put(fd, metadata)
            self.disk_file.close()
        self.auditor.audit_all_objects()
        self.disk_file = DiskFile(self.devices, 'sdb', '0', 'a', 'c',
                                  'ob', self.logger)
        data = '1' * 10
        etag = md5()
        with self.disk_file.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(fd).st_size),
            }
            self.disk_file.put(fd, metadata)
            self.disk_file.close()
            os.write(fd, 'extra_data')
        self.auditor.audit_all_objects()
        self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)

    def test_object_run_fast_track_non_zero(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.auditor.log_time = 0
        data = '0' * 1024
        etag = md5()
        with self.disk_file.mkstemp() as fd:
            os.write(fd, data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': str(normalize_timestamp(time.time())),
                'Content-Length': str(os.fstat(fd).st_size),
            }
            self.disk_file.put(fd, metadata)
            etag = md5()
            etag.update('1' + '0' * 1023)
            etag = etag.hexdigest()
            metadata['ETag'] = etag
            write_metadata(fd, metadata)

        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        self.auditor.run_once(zero_byte_fps=50)
        self.assertFalse(os.path.isdir(quarantine_path))
        self.auditor.run_once()
        self.assertTrue(os.path.isdir(quarantine_path))

    def setup_bad_zero_byte(self, with_ts=False):
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.auditor.log_time = 0
        ts_file_path = ''
        if with_ts:

            name_hash = hash_path('a', 'c', 'o')
            dir_path = os.path.join(self.devices, 'sda',
                               storage_directory(DATADIR, '0', name_hash))
            ts_file_path = os.path.join(dir_path, '99999.ts')
            if not os.path.exists(dir_path):
                mkdirs(dir_path)
            fp = open(ts_file_path, 'w')
            fp.close()

        etag = md5()
        with self.disk_file.mkstemp() as fd:
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': str(normalize_timestamp(time.time())),
                'Content-Length': 10,
            }
            self.disk_file.put(fd, metadata)
            etag = md5()
            etag = etag.hexdigest()
            metadata['ETag'] = etag
            write_metadata(fd, metadata)
        if self.disk_file.data_file:
            return self.disk_file.data_file
        return ts_file_path

    def test_object_run_fast_track_all(self):
        self.setup_bad_zero_byte()
        self.auditor.run_once()
        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        self.assertTrue(os.path.isdir(quarantine_path))

    def test_object_run_fast_track_zero(self):
        self.setup_bad_zero_byte()
        self.auditor.run_once(zero_byte_fps=50)
        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        self.assertTrue(os.path.isdir(quarantine_path))

    def test_with_tombstone(self):
        ts_file_path = self.setup_bad_zero_byte(with_ts=True)
        self.auditor.run_once()
        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        self.assertTrue(ts_file_path.endswith('ts'))
        self.assertTrue(os.path.exists(ts_file_path))

    def test_sleeper(self):
        auditor.SLEEP_BETWEEN_AUDITS = 0.10
        my_auditor = auditor.ObjectAuditor(self.conf)
        start = time.time()
        my_auditor._sleep()
        delta_t = time.time() - start
        self.assert_(delta_t > 0.08)
        self.assert_(delta_t < 0.12)

    def test_object_run_fast_track_zero_check_closed(self):
        rat = [False]

        class FakeFile(DiskFile):

            def close(self, verify_file=True):
                rat[0] = True
                DiskFile.close(self, verify_file=verify_file)
        self.setup_bad_zero_byte()
        was_df = object_server.DiskFile
        try:
            object_server.DiskFile = FakeFile
            self.auditor.run_once(zero_byte_fps=50)
            quarantine_path = os.path.join(self.devices,
                                           'sda', 'quarantined', 'objects')
            self.assertTrue(os.path.isdir(quarantine_path))
            self.assertTrue(rat[0])
        finally:
            object_server.DiskFile = was_df

    def test_run_forever(self):

        class StopForever(Exception):
            pass

        class ObjectAuditorMock(object):
            check_args = ()
            check_kwargs = {}
            fork_called = 0
            fork_res = 0

            def mock_run(self, *args, **kwargs):
                self.check_args = args
                self.check_kwargs = kwargs

            def mock_sleep(self):
                raise StopForever('stop')

            def mock_fork(self):
                self.fork_called += 1
                return self.fork_res

        my_auditor = auditor.ObjectAuditor(dict(devices=self.devices,
                                                mount_check='false',
                                                zero_byte_files_per_second=89))
        mocker = ObjectAuditorMock()
        my_auditor.run_once = mocker.mock_run
        my_auditor._sleep = mocker.mock_sleep
        was_fork = os.fork
        try:
            os.fork = mocker.mock_fork
            self.assertRaises(StopForever,
                              my_auditor.run_forever, zero_byte_fps=50)
            self.assertEquals(mocker.check_kwargs['zero_byte_fps'], 50)
            self.assertEquals(mocker.fork_called, 0)

            self.assertRaises(StopForever, my_auditor.run_forever)
            self.assertEquals(mocker.fork_called, 1)
            self.assertEquals(mocker.check_args, ())

            mocker.fork_res = 1
            self.assertRaises(StopForever, my_auditor.run_forever)
            self.assertEquals(mocker.fork_called, 2)
            self.assertEquals(mocker.check_kwargs['zero_byte_fps'], 89)

        finally:
            os.fork = was_fork

if __name__ == '__main__':
    unittest.main()
