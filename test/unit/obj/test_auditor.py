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

from test import unit
import unittest
import mock
import os
import time
import string
from shutil import rmtree
from hashlib import md5
from tempfile import mkdtemp
from test.unit import FakeLogger, patch_policies
from swift.obj import auditor
from swift.obj.diskfile import DiskFile, write_metadata, invalidate_hash, \
    get_data_dir, DiskFileManager, AuditLocation
from swift.common.utils import hash_path, mkdirs, normalize_timestamp, \
    storage_directory
from swift.common.storage_policy import StoragePolicy, POLICIES


_mocked_policies = [StoragePolicy(0, 'zero', False),
                    StoragePolicy(1, 'one', True)]


@patch_policies(_mocked_policies)
class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_object_auditor')
        self.devices = os.path.join(self.testdir, 'node')
        self.rcache = os.path.join(self.testdir, 'object.recon')
        self.logger = FakeLogger()
        rmtree(self.testdir, ignore_errors=1)
        mkdirs(os.path.join(self.devices, 'sda'))
        os.mkdir(os.path.join(self.devices, 'sdb'))

        # policy 0
        self.objects = os.path.join(self.devices, 'sda',
                                    get_data_dir(POLICIES[0]))
        self.objects_2 = os.path.join(self.devices, 'sdb',
                                      get_data_dir(POLICIES[0]))
        os.mkdir(self.objects)
        # policy 1
        self.objects_p1 = os.path.join(self.devices, 'sda',
                                       get_data_dir(POLICIES[1]))
        self.objects_2_p1 = os.path.join(self.devices, 'sdb',
                                         get_data_dir(POLICIES[1]))
        os.mkdir(self.objects_p1)

        self.parts = self.parts_p1 = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            self.parts_p1[part] = os.path.join(self.objects_p1, part)
            os.mkdir(os.path.join(self.objects, part))
            os.mkdir(os.path.join(self.objects_p1, part))

        self.conf = dict(
            devices=self.devices,
            mount_check='false',
            object_size_stats='10,100,1024,10240')
        self.df_mgr = DiskFileManager(self.conf, self.logger)

        # diskfiles for policy 0, 1
        self.disk_file = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o',
                                                  policy=POLICIES[0])
        self.disk_file_p1 = self.df_mgr.get_diskfile('sda', '0', 'a', 'c',
                                                     'o', policy=POLICIES[1])

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)
        unit.xattr_data = {}

    def test_worker_conf_parms(self):
        def check_common_defaults():
            self.assertEquals(auditor_worker.max_bytes_per_second, 10000000)
            self.assertEquals(auditor_worker.log_time, 3600)

        # test default values
        conf = dict(
            devices=self.devices,
            mount_check='false',
            object_size_stats='10,100,1024,10240')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        check_common_defaults()
        self.assertEquals(auditor_worker.diskfile_mgr.disk_chunk_size, 65536)
        self.assertEquals(auditor_worker.max_files_per_second, 20)
        self.assertEquals(auditor_worker.zero_byte_only_at_fps, 0)

        # test specified audit value overrides
        conf.update({'disk_chunk_size': 4096})
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices,
                                               zero_byte_only_at_fps=50)
        check_common_defaults()
        self.assertEquals(auditor_worker.diskfile_mgr.disk_chunk_size, 4096)
        self.assertEquals(auditor_worker.max_files_per_second, 50)
        self.assertEquals(auditor_worker.zero_byte_only_at_fps, 50)

    def test_object_audit_extra_data(self):
        def run_tests(disk_file):
            auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                                   self.rcache, self.devices)
            data = '0' * 1024
            etag = md5()
            with disk_file.create() as writer:
                writer.write(data)
                etag.update(data)
                etag = etag.hexdigest()
                timestamp = str(normalize_timestamp(time.time()))
                metadata = {
                    'ETag': etag,
                    'X-Timestamp': timestamp,
                    'Content-Length': str(os.fstat(writer._fd).st_size),
                }
                writer.put(metadata)
                pre_quarantines = auditor_worker.quarantines

                auditor_worker.object_audit(
                    AuditLocation(disk_file._datadir, 'sda', '0',
                                  policy=POLICIES.legacy))
                self.assertEquals(auditor_worker.quarantines, pre_quarantines)

                os.write(writer._fd, 'extra_data')

                auditor_worker.object_audit(
                    AuditLocation(disk_file._datadir, 'sda', '0',
                                  policy=POLICIES.legacy))
                self.assertEquals(auditor_worker.quarantines,
                                  pre_quarantines + 1)
        run_tests(self.disk_file)
        run_tests(self.disk_file_p1)

    def test_object_audit_diff_data(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        data = '0' * 1024
        etag = md5()
        timestamp = str(normalize_timestamp(time.time()))
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
            pre_quarantines = auditor_worker.quarantines

        # remake so it will have metadata
        self.disk_file = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o',
                                                  policy=POLICIES.legacy)

        auditor_worker.object_audit(
            AuditLocation(self.disk_file._datadir, 'sda', '0',
                          policy=POLICIES.legacy))
        self.assertEquals(auditor_worker.quarantines, pre_quarantines)
        etag = md5()
        etag.update('1' + '0' * 1023)
        etag = etag.hexdigest()
        metadata['ETag'] = etag

        with self.disk_file.create() as writer:
            writer.write(data)
            writer.put(metadata)

        auditor_worker.object_audit(
            AuditLocation(self.disk_file._datadir, 'sda', '0',
                          policy=POLICIES.legacy))
        self.assertEquals(auditor_worker.quarantines, pre_quarantines + 1)

    def test_object_audit_no_meta(self):
        timestamp = str(normalize_timestamp(time.time()))
        path = os.path.join(self.disk_file._datadir, timestamp + '.data')
        mkdirs(self.disk_file._datadir)
        fp = open(path, 'w')
        fp.write('0' * 1024)
        fp.close()
        invalidate_hash(os.path.dirname(self.disk_file._datadir))
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        pre_quarantines = auditor_worker.quarantines
        auditor_worker.object_audit(
            AuditLocation(self.disk_file._datadir, 'sda', '0',
                          policy=POLICIES.legacy))
        self.assertEquals(auditor_worker.quarantines, pre_quarantines + 1)

    def test_object_audit_will_not_swallow_errors_in_tests(self):
        timestamp = str(normalize_timestamp(time.time()))
        path = os.path.join(self.disk_file._datadir, timestamp + '.data')
        mkdirs(self.disk_file._datadir)
        with open(path, 'w') as f:
            write_metadata(f, {'name': '/a/c/o'})
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)

        def blowup(*args):
            raise NameError('tpyo')
        with mock.patch.object(DiskFileManager,
                               'get_diskfile_from_audit_location', blowup):
            self.assertRaises(NameError, auditor_worker.object_audit,
                              AuditLocation(os.path.dirname(path), 'sda', '0',
                                            policy=POLICIES.legacy))

    def test_failsafe_object_audit_will_swallow_errors_in_tests(self):
        timestamp = str(normalize_timestamp(time.time()))
        path = os.path.join(self.disk_file._datadir, timestamp + '.data')
        mkdirs(self.disk_file._datadir)
        with open(path, 'w') as f:
            write_metadata(f, {'name': '/a/c/o'})
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)

        def blowup(*args):
            raise NameError('tpyo')
        with mock.patch('swift.obj.diskfile.DiskFileManager.diskfile_cls',
                        blowup):
            auditor_worker.failsafe_object_audit(
                AuditLocation(os.path.dirname(path), 'sda', '0',
                              policy=POLICIES.legacy))
        self.assertEquals(auditor_worker.errors, 1)

    def test_generic_exception_handling(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        # pretend that we logged (and reset counters) just now
        auditor_worker.last_logged = time.time()
        timestamp = str(normalize_timestamp(time.time()))
        pre_errors = auditor_worker.errors
        data = '0' * 1024
        etag = md5()
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
        with mock.patch('swift.obj.diskfile.DiskFileManager.diskfile_cls',
                        lambda *_: 1 / 0):
            auditor_worker.audit_all_objects()
        self.assertEquals(auditor_worker.errors, pre_errors + 1)

    def test_object_run_once_pass(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        auditor_worker.log_time = 0
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = auditor_worker.quarantines
        data = '0' * 1024

        def write_file(df):
            etag = md5()
            with df.create() as writer:
                writer.write(data)
                etag.update(data)
                etag = etag.hexdigest()
                metadata = {
                    'ETag': etag,
                    'X-Timestamp': timestamp,
                    'Content-Length': str(os.fstat(writer._fd).st_size),
                }
                writer.put(metadata)

        # policy 0
        write_file(self.disk_file)
        # policy 1
        write_file(self.disk_file_p1)

        auditor_worker.audit_all_objects()
        self.assertEquals(auditor_worker.quarantines, pre_quarantines)
        # 1 object per policy falls into 1024 bucket
        self.assertEquals(auditor_worker.stats_buckets[1024], 2)
        self.assertEquals(auditor_worker.stats_buckets[10240], 0)

        # pick up some additional code coverage, large file
        data = '0' * 1024 * 1024
        etag = md5()
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
        auditor_worker.audit_all_objects(device_dirs=['sda', 'sdb'])
        self.assertEquals(auditor_worker.quarantines, pre_quarantines)
        # still have the 1024 byte object left in policy-1 (plus the
        # stats from the original 2)
        self.assertEquals(auditor_worker.stats_buckets[1024], 3)
        self.assertEquals(auditor_worker.stats_buckets[10240], 0)
        # and then policy-0 disk_file was re-written as a larger object
        self.assertEquals(auditor_worker.stats_buckets['OVER'], 1)

        # pick up even more additional code coverage, misc paths
        auditor_worker.log_time = -1
        auditor_worker.stats_sizes = []
        auditor_worker.audit_all_objects(device_dirs=['sda', 'sdb'])
        self.assertEquals(auditor_worker.quarantines, pre_quarantines)
        self.assertEquals(auditor_worker.stats_buckets[1024], 3)
        self.assertEquals(auditor_worker.stats_buckets[10240], 0)
        self.assertEquals(auditor_worker.stats_buckets['OVER'], 1)

    def test_object_run_logging(self):
        logger = FakeLogger()
        auditor_worker = auditor.AuditorWorker(self.conf, logger,
                                               self.rcache, self.devices)
        auditor_worker.audit_all_objects(device_dirs=['sda'])
        log_lines = logger.get_lines_for_level('info')
        self.assertTrue(len(log_lines) > 0)
        self.assertTrue(log_lines[0].index('ALL - parallel, sda'))

        logger = FakeLogger()
        auditor_worker = auditor.AuditorWorker(self.conf, logger,
                                               self.rcache, self.devices,
                                               zero_byte_only_at_fps=50)
        auditor_worker.audit_all_objects(device_dirs=['sda'])
        log_lines = logger.get_lines_for_level('info')
        self.assertTrue(len(log_lines) > 0)
        self.assertTrue(log_lines[0].index('ZBF - sda'))

    def test_object_run_once_no_sda(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = auditor_worker.quarantines
        # pretend that we logged (and reset counters) just now
        auditor_worker.last_logged = time.time()
        data = '0' * 1024
        etag = md5()
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
            os.write(writer._fd, 'extra_data')
        auditor_worker.audit_all_objects()
        self.assertEquals(auditor_worker.quarantines, pre_quarantines + 1)

    def test_object_run_once_multi_devices(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        # pretend that we logged (and reset counters) just now
        auditor_worker.last_logged = time.time()
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = auditor_worker.quarantines
        data = '0' * 10
        etag = md5()
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
        auditor_worker.audit_all_objects()
        self.disk_file = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'ob',
                                                  policy=POLICIES.legacy)
        data = '1' * 10
        etag = md5()
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
            os.write(writer._fd, 'extra_data')
        auditor_worker.audit_all_objects()
        self.assertEquals(auditor_worker.quarantines, pre_quarantines + 1)

    def test_object_run_fast_track_non_zero(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.auditor.log_time = 0
        data = '0' * 1024
        etag = md5()
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': str(normalize_timestamp(time.time())),
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
            etag = md5()
            etag.update('1' + '0' * 1023)
            etag = etag.hexdigest()
            metadata['ETag'] = etag
            write_metadata(writer._fd, metadata)

        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        kwargs = {'mode': 'once'}
        kwargs['zero_byte_fps'] = 50
        self.auditor.run_audit(**kwargs)
        self.assertFalse(os.path.isdir(quarantine_path))
        del(kwargs['zero_byte_fps'])
        self.auditor.run_audit(**kwargs)
        self.assertTrue(os.path.isdir(quarantine_path))

    def setup_bad_zero_byte(self, with_ts=False):
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.auditor.log_time = 0
        ts_file_path = ''
        if with_ts:
            name_hash = hash_path('a', 'c', 'o')
            dir_path = os.path.join(
                self.devices, 'sda',
                storage_directory(get_data_dir(POLICIES[0]), '0', name_hash))
            ts_file_path = os.path.join(dir_path, '99999.ts')
            if not os.path.exists(dir_path):
                mkdirs(dir_path)
            fp = open(ts_file_path, 'w')
            write_metadata(fp, {'X-Timestamp': '99999', 'name': '/a/c/o'})
            fp.close()

        etag = md5()
        with self.disk_file.create() as writer:
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': str(normalize_timestamp(time.time())),
                'Content-Length': 10,
            }
            writer.put(metadata)
            etag = md5()
            etag = etag.hexdigest()
            metadata['ETag'] = etag
            write_metadata(writer._fd, metadata)
        return ts_file_path

    def test_object_run_fast_track_all(self):
        self.setup_bad_zero_byte()
        kwargs = {'mode': 'once'}
        self.auditor.run_audit(**kwargs)
        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        self.assertTrue(os.path.isdir(quarantine_path))

    def test_object_run_fast_track_zero(self):
        self.setup_bad_zero_byte()
        kwargs = {'mode': 'once'}
        kwargs['zero_byte_fps'] = 50
        self.auditor.run_audit(**kwargs)
        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        self.assertTrue(os.path.isdir(quarantine_path))

    def test_object_run_fast_track_zero_check_closed(self):
        rat = [False]

        class FakeFile(DiskFile):

            def _quarantine(self, data_file, msg):
                rat[0] = True
                DiskFile._quarantine(self, data_file, msg)

        self.setup_bad_zero_byte()
        with mock.patch('swift.obj.diskfile.DiskFileManager.diskfile_cls',
                        FakeFile):
            kwargs = {'mode': 'once'}
            kwargs['zero_byte_fps'] = 50
            self.auditor.run_audit(**kwargs)
            quarantine_path = os.path.join(self.devices,
                                           'sda', 'quarantined', 'objects')
            self.assertTrue(os.path.isdir(quarantine_path))
            self.assertTrue(rat[0])

    @mock.patch.object(auditor.ObjectAuditor, 'run_audit')
    @mock.patch('os.fork', return_value=0)
    def test_with_inaccessible_object_location(self, mock_os_fork,
                                               mock_run_audit):
        # Need to ensure that any failures in run_audit do
        # not prevent sys.exit() from running.  Otherwise we get
        # zombie processes.
        e = OSError('permission denied')
        mock_run_audit.side_effect = e
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.assertRaises(SystemExit, self.auditor.fork_child, self)

    def test_with_tombstone(self):
        ts_file_path = self.setup_bad_zero_byte(with_ts=True)
        self.assertTrue(ts_file_path.endswith('ts'))
        kwargs = {'mode': 'once'}
        self.auditor.run_audit(**kwargs)
        self.assertTrue(os.path.exists(ts_file_path))

    def test_sleeper(self):
        with mock.patch(
                'time.sleep', mock.MagicMock()) as mock_sleep:
            auditor.SLEEP_BETWEEN_AUDITS = 0.10
            my_auditor = auditor.ObjectAuditor(self.conf)
            my_auditor._sleep()
            mock_sleep.assert_called_with(auditor.SLEEP_BETWEEN_AUDITS)

    def test_run_parallel_audit(self):

        class StopForever(Exception):
            pass

        class Bogus(Exception):
            pass

        class ObjectAuditorMock(object):
            check_args = ()
            check_kwargs = {}
            check_device_dir = None
            fork_called = 0
            master = 0
            wait_called = 0

            def mock_run(self, *args, **kwargs):
                self.check_args = args
                self.check_kwargs = kwargs
                if 'zero_byte_fps' in kwargs:
                    self.check_device_dir = kwargs.get('device_dirs')

            def mock_sleep_stop(self):
                raise StopForever('stop')

            def mock_sleep_continue(self):
                return

            def mock_audit_loop_error(self, parent, zbo_fps,
                                      override_devices=None, **kwargs):
                raise Bogus('exception')

            def mock_fork(self):
                self.fork_called += 1
                if self.master:
                    return self.fork_called
                else:
                    return 0

            def mock_wait(self):
                self.wait_called += 1
                return (self.wait_called, 0)

        for i in string.ascii_letters[2:26]:
            mkdirs(os.path.join(self.devices, 'sd%s' % i))

        my_auditor = auditor.ObjectAuditor(dict(devices=self.devices,
                                                mount_check='false',
                                                zero_byte_files_per_second=89,
                                                concurrency=1))

        mocker = ObjectAuditorMock()
        my_auditor.logger.exception = mock.MagicMock()
        real_audit_loop = my_auditor.audit_loop
        my_auditor.audit_loop = mocker.mock_audit_loop_error
        my_auditor.run_audit = mocker.mock_run
        was_fork = os.fork
        was_wait = os.wait
        os.fork = mocker.mock_fork
        os.wait = mocker.mock_wait
        try:
            my_auditor._sleep = mocker.mock_sleep_stop
            my_auditor.run_once(zero_byte_fps=50)
            my_auditor.logger.exception.assert_called_once_with(
                'ERROR auditing: exception')
            my_auditor.logger.exception.reset_mock()
            self.assertRaises(StopForever, my_auditor.run_forever)
            my_auditor.logger.exception.assert_called_once_with(
                'ERROR auditing: exception')
            my_auditor.audit_loop = real_audit_loop

            self.assertRaises(StopForever,
                              my_auditor.run_forever, zero_byte_fps=50)
            self.assertEquals(mocker.check_kwargs['zero_byte_fps'], 50)
            self.assertEquals(mocker.fork_called, 0)

            self.assertRaises(SystemExit, my_auditor.run_once)
            self.assertEquals(mocker.fork_called, 1)
            self.assertEquals(mocker.check_kwargs['zero_byte_fps'], 89)
            self.assertEquals(mocker.check_device_dir, [])
            self.assertEquals(mocker.check_args, ())

            device_list = ['sd%s' % i for i in string.ascii_letters[2:10]]
            device_string = ','.join(device_list)
            device_string_bogus = device_string + ',bogus'

            mocker.fork_called = 0
            self.assertRaises(SystemExit, my_auditor.run_once,
                              devices=device_string_bogus)
            self.assertEquals(mocker.fork_called, 1)
            self.assertEquals(mocker.check_kwargs['zero_byte_fps'], 89)
            self.assertEquals(sorted(mocker.check_device_dir), device_list)

            mocker.master = 1

            mocker.fork_called = 0
            self.assertRaises(StopForever, my_auditor.run_forever)
            # Fork is called 2 times since the zbf process is forked just
            # once before self._sleep() is called and StopForever is raised
            # Also wait is called just once before StopForever is raised
            self.assertEquals(mocker.fork_called, 2)
            self.assertEquals(mocker.wait_called, 1)

            my_auditor._sleep = mocker.mock_sleep_continue

            my_auditor.concurrency = 2
            mocker.fork_called = 0
            mocker.wait_called = 0
            my_auditor.run_once()
            # Fork is called no. of devices + (no. of devices)/2 + 1 times
            # since zbf process is forked (no.of devices)/2 + 1 times
            no_devices = len(os.listdir(self.devices))
            self.assertEquals(mocker.fork_called, no_devices + no_devices / 2
                              + 1)
            self.assertEquals(mocker.wait_called, no_devices + no_devices / 2
                              + 1)

        finally:
            os.fork = was_fork
            os.wait = was_wait

if __name__ == '__main__':
    unittest.main()
