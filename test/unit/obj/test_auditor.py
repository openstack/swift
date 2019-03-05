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
import json

import unittest
import mock
import os
import sys
import signal
import time
import string
import xattr
from shutil import rmtree
from hashlib import md5
from tempfile import mkdtemp
import textwrap
from os.path import dirname, basename
from test.unit import (debug_logger, patch_policies, make_timestamp_iter,
                       DEFAULT_TEST_EC_TYPE, skip_if_no_xattrs)
from swift.obj import auditor, replicator
from swift.obj.diskfile import (
    DiskFile, write_metadata, invalidate_hash, get_data_dir,
    DiskFileManager, ECDiskFileManager, AuditLocation, clear_auditor_status,
    get_auditor_status, HASH_FILE, HASH_INVALIDATIONS_FILE)
from swift.common.utils import (
    mkdirs, normalize_timestamp, Timestamp, readconf)
from swift.common.storage_policy import (
    ECStoragePolicy, StoragePolicy, POLICIES, EC_POLICY)
from test.unit.obj.common import write_diskfile

_mocked_policies = [
    StoragePolicy(0, 'zero', False),
    StoragePolicy(1, 'one', True),
    ECStoragePolicy(2, 'two', ec_type=DEFAULT_TEST_EC_TYPE,
                    ec_ndata=2, ec_nparity=1, ec_segment_size=4096),
]


def works_only_once(callable_thing, exception):
    called = [False]

    def only_once(*a, **kw):
        if called[0]:
            raise exception
        else:
            called[0] = True
            return callable_thing(*a, **kw)

    return only_once


@patch_policies(_mocked_policies)
class TestAuditor(unittest.TestCase):

    def setUp(self):
        skip_if_no_xattrs()
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_object_auditor')
        self.devices = os.path.join(self.testdir, 'node')
        self.rcache = os.path.join(self.testdir, 'object.recon')
        self.logger = debug_logger()
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
        # policy 2
        self.objects_p2 = os.path.join(self.devices, 'sda',
                                       get_data_dir(POLICIES[2]))
        self.objects_2_p2 = os.path.join(self.devices, 'sdb',
                                         get_data_dir(POLICIES[2]))
        os.mkdir(self.objects_p2)

        self.parts = {}
        self.parts_p1 = {}
        self.parts_p2 = {}
        for part in ['0', '1', '2', '3']:
            self.parts[part] = os.path.join(self.objects, part)
            self.parts_p1[part] = os.path.join(self.objects_p1, part)
            self.parts_p2[part] = os.path.join(self.objects_p2, part)
            os.mkdir(os.path.join(self.objects, part))
            os.mkdir(os.path.join(self.objects_p1, part))
            os.mkdir(os.path.join(self.objects_p2, part))

        self.conf = dict(
            devices=self.devices,
            mount_check='false',
            object_size_stats='10,100,1024,10240')
        self.df_mgr = DiskFileManager(self.conf, self.logger)
        self.ec_df_mgr = ECDiskFileManager(self.conf, self.logger)

        # diskfiles for policy 0, 1, 2
        self.disk_file = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o',
                                                  policy=POLICIES[0])
        self.disk_file_p1 = self.df_mgr.get_diskfile('sda', '0', 'a', 'c',
                                                     'o', policy=POLICIES[1])
        self.disk_file_ec = self.ec_df_mgr.get_diskfile(
            'sda', '0', 'a', 'c', 'o', policy=POLICIES[2], frag_index=1)

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    def test_worker_conf_parms(self):
        def check_common_defaults():
            self.assertEqual(auditor_worker.max_bytes_per_second, 10000000)
            self.assertEqual(auditor_worker.log_time, 3600)

        # test default values
        conf = dict(
            devices=self.devices,
            mount_check='false',
            object_size_stats='10,100,1024,10240')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        check_common_defaults()
        for policy in POLICIES:
            mgr = auditor_worker.diskfile_router[policy]
            self.assertEqual(mgr.disk_chunk_size, 65536)
        self.assertEqual(auditor_worker.max_files_per_second, 20)
        self.assertEqual(auditor_worker.zero_byte_only_at_fps, 0)

        # test specified audit value overrides
        conf.update({'disk_chunk_size': 4096})
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices,
                                               zero_byte_only_at_fps=50)
        check_common_defaults()
        for policy in POLICIES:
            mgr = auditor_worker.diskfile_router[policy]
            self.assertEqual(mgr.disk_chunk_size, 4096)
        self.assertEqual(auditor_worker.max_files_per_second, 50)
        self.assertEqual(auditor_worker.zero_byte_only_at_fps, 50)

    def test_object_audit_extra_data(self):
        def run_tests(disk_file):
            auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                                   self.rcache, self.devices)
            data = b'0' * 1024
            if disk_file.policy.policy_type == EC_POLICY:
                data = disk_file.policy.pyeclib_driver.encode(data)[0]
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
                writer.commit(Timestamp(timestamp))
                pre_quarantines = auditor_worker.quarantines

                auditor_worker.object_audit(
                    AuditLocation(disk_file._datadir, 'sda', '0',
                                  policy=disk_file.policy))
                self.assertEqual(auditor_worker.quarantines, pre_quarantines)

                os.write(writer._fd, b'extra_data')

                auditor_worker.object_audit(
                    AuditLocation(disk_file._datadir, 'sda', '0',
                                  policy=disk_file.policy))
                self.assertEqual(auditor_worker.quarantines,
                                 pre_quarantines + 1)
        run_tests(self.disk_file)
        run_tests(self.disk_file_p1)
        run_tests(self.disk_file_ec)

    def test_object_audit_adds_metadata_checksums(self):
        disk_file = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o-md',
                                             policy=POLICIES.legacy)

        # simulate a PUT
        now = time.time()
        data = b'boots and cats and ' * 1024
        hasher = md5()
        with disk_file.create() as writer:
            writer.write(data)
            hasher.update(data)
            etag = hasher.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': str(normalize_timestamp(now)),
                'Content-Length': len(data),
                'Content-Type': 'the old type',
            }
            writer.put(metadata)
            writer.commit(Timestamp(now))

        # simulate a subsequent POST
        post_metadata = metadata.copy()
        post_metadata['Content-Type'] = 'the new type'
        post_metadata['X-Object-Meta-Biff'] = 'buff'
        post_metadata['X-Timestamp'] = str(normalize_timestamp(now + 1))
        disk_file.write_metadata(post_metadata)

        file_paths = [os.path.join(disk_file._datadir, fname)
                      for fname in os.listdir(disk_file._datadir)
                      if fname not in ('.', '..')]
        file_paths.sort()

        # sanity check: make sure we have a .data and a .meta file
        self.assertEqual(len(file_paths), 2)
        self.assertTrue(file_paths[0].endswith(".data"))
        self.assertTrue(file_paths[1].endswith(".meta"))

        # Go remove the xattr "user.swift.metadata_checksum" as if this
        # object were written before Swift supported metadata checksums.
        for file_path in file_paths:
            xattr.removexattr(file_path, "user.swift.metadata_checksum")

        # Run the auditor...
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        auditor_worker.object_audit(
            AuditLocation(disk_file._datadir, 'sda', '0',
                          policy=disk_file.policy))
        self.assertEqual(auditor_worker.quarantines, 0)  # sanity

        # ...and the checksums are back
        for file_path in file_paths:
            metadata = xattr.getxattr(file_path, "user.swift.metadata")
            i = 1
            while True:
                try:
                    metadata += xattr.getxattr(
                        file_path, "user.swift.metadata%d" % i)
                    i += 1
                except (IOError, OSError):
                    break

            checksum = xattr.getxattr(
                file_path, "user.swift.metadata_checksum")

            self.assertEqual(checksum,
                             md5(metadata).hexdigest().encode('ascii'))

    def test_object_audit_diff_data(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        data = b'0' * 1024
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
            writer.commit(Timestamp(timestamp))
            pre_quarantines = auditor_worker.quarantines

        # remake so it will have metadata
        self.disk_file = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'o',
                                                  policy=POLICIES.legacy)

        auditor_worker.object_audit(
            AuditLocation(self.disk_file._datadir, 'sda', '0',
                          policy=POLICIES.legacy))
        self.assertEqual(auditor_worker.quarantines, pre_quarantines)
        etag = md5(b'1' + b'0' * 1023).hexdigest()
        metadata['ETag'] = etag

        with self.disk_file.create() as writer:
            writer.write(data)
            writer.put(metadata)
            writer.commit(Timestamp(timestamp))

        auditor_worker.object_audit(
            AuditLocation(self.disk_file._datadir, 'sda', '0',
                          policy=POLICIES.legacy))
        self.assertEqual(auditor_worker.quarantines, pre_quarantines + 1)

    def test_object_audit_checks_EC_fragments(self):
        disk_file = self.disk_file_ec

        def do_test(data):
            # create diskfile and set ETag and content-length to match the data
            etag = md5(data).hexdigest()
            timestamp = str(normalize_timestamp(time.time()))
            with disk_file.create() as writer:
                writer.write(data)
                metadata = {
                    'ETag': etag,
                    'X-Timestamp': timestamp,
                    'Content-Length': len(data),
                }
                writer.put(metadata)
                writer.commit(Timestamp(timestamp))

            self.logger.clear()
            auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                                   self.rcache, self.devices)
            self.assertEqual(0, auditor_worker.quarantines)  # sanity check
            auditor_worker.object_audit(
                AuditLocation(disk_file._datadir, 'sda', '0',
                              policy=disk_file.policy))
            return auditor_worker

        # two good frags in an EC archive
        frag_0 = disk_file.policy.pyeclib_driver.encode(
            b'x' * disk_file.policy.ec_segment_size)[0]
        frag_1 = disk_file.policy.pyeclib_driver.encode(
            b'y' * disk_file.policy.ec_segment_size)[0]
        data = frag_0 + frag_1
        auditor_worker = do_test(data)
        self.assertEqual(0, auditor_worker.quarantines)
        self.assertFalse(auditor_worker.logger.get_lines_for_level('error'))

        # corrupt second frag headers
        corrupt_frag_1 = b'blah' * 16 + frag_1[64:]
        data = frag_0 + corrupt_frag_1
        auditor_worker = do_test(data)
        self.assertEqual(1, auditor_worker.quarantines)
        log_lines = auditor_worker.logger.get_lines_for_level('error')
        self.assertIn('failed audit and was quarantined: '
                      'Invalid EC metadata at offset 0x%x' %
                      len(frag_0),
                      log_lines[0])

        # dangling extra corrupt frag data
        data = frag_0 + frag_1 + b'wtf' * 100
        auditor_worker = do_test(data)
        self.assertEqual(1, auditor_worker.quarantines)
        log_lines = auditor_worker.logger.get_lines_for_level('error')
        self.assertIn('failed audit and was quarantined: '
                      'Invalid EC metadata at offset 0x%x' %
                      len(frag_0 + frag_1),
                      log_lines[0])

        # simulate bug https://bugs.launchpad.net/bugs/1631144 by writing start
        # of an ssync subrequest into the diskfile
        data = (
            b'PUT /a/c/o\r\n' +
            b'Content-Length: 999\r\n' +
            b'Content-Type: image/jpeg\r\n' +
            b'X-Object-Sysmeta-Ec-Content-Length: 1024\r\n' +
            b'X-Object-Sysmeta-Ec-Etag: 1234bff7eb767cc6d19627c6b6f9edef\r\n' +
            b'X-Object-Sysmeta-Ec-Frag-Index: 1\r\n' +
            b'X-Object-Sysmeta-Ec-Scheme: ' +
            DEFAULT_TEST_EC_TYPE.encode('ascii') + b'\r\n' +
            b'X-Object-Sysmeta-Ec-Segment-Size: 1048576\r\n' +
            b'X-Timestamp: 1471512345.17333\r\n\r\n'
        )
        data += frag_0[:disk_file.policy.fragment_size - len(data)]
        auditor_worker = do_test(data)
        self.assertEqual(1, auditor_worker.quarantines)
        log_lines = auditor_worker.logger.get_lines_for_level('error')
        self.assertIn('failed audit and was quarantined: '
                      'Invalid EC metadata at offset 0x0',
                      log_lines[0])

    def test_object_audit_no_meta(self):
        timestamp = str(normalize_timestamp(time.time()))
        path = os.path.join(self.disk_file._datadir, timestamp + '.data')
        mkdirs(self.disk_file._datadir)
        fp = open(path, 'wb')
        fp.write(b'0' * 1024)
        fp.close()
        invalidate_hash(os.path.dirname(self.disk_file._datadir))
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        pre_quarantines = auditor_worker.quarantines
        auditor_worker.object_audit(
            AuditLocation(self.disk_file._datadir, 'sda', '0',
                          policy=POLICIES.legacy))
        self.assertEqual(auditor_worker.quarantines, pre_quarantines + 1)

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
        self.assertEqual(auditor_worker.errors, 1)

    def test_audit_location_gets_quarantined(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)

        location = AuditLocation(self.disk_file._datadir, 'sda', '0',
                                 policy=self.disk_file.policy)

        # instead of a datadir, we'll make a file!
        mkdirs(os.path.dirname(self.disk_file._datadir))
        open(self.disk_file._datadir, 'w')

        # after we turn the crank ...
        auditor_worker.object_audit(location)

        # ... it should get quarantined
        self.assertFalse(os.path.exists(self.disk_file._datadir))
        self.assertEqual(1, auditor_worker.quarantines)

    def test_rsync_tempfile_timeout_auto_option(self):
        # if we don't have access to the replicator config section we'll use
        # our default
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        self.assertEqual(auditor_worker.rsync_tempfile_timeout, 86400)
        # if the rsync_tempfile_timeout option is set explicitly we use that
        self.conf['rsync_tempfile_timeout'] = '1800'
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        self.assertEqual(auditor_worker.rsync_tempfile_timeout, 1800)
        # if we have a real config we can be a little smarter
        config_path = os.path.join(self.testdir, 'objserver.conf')
        stub_config = """
        [object-auditor]
        rsync_tempfile_timeout = auto
        """
        with open(config_path, 'w') as f:
            f.write(textwrap.dedent(stub_config))
        # the Daemon loader will hand the object-auditor config to the
        # auditor who will build the workers from it
        conf = readconf(config_path, 'object-auditor')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        # if there is no object-replicator section we still have to fall back
        # to default because we can't parse the config for that section!
        self.assertEqual(auditor_worker.rsync_tempfile_timeout, 86400)
        stub_config = """
        [object-replicator]
        [object-auditor]
        rsync_tempfile_timeout = auto
        """
        with open(config_path, 'w') as f:
            f.write(textwrap.dedent(stub_config))
        conf = readconf(config_path, 'object-auditor')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        # if the object-replicator section will parse but does not override
        # the default rsync_timeout we assume the default rsync_timeout value
        # and add 15mins
        self.assertEqual(auditor_worker.rsync_tempfile_timeout,
                         replicator.DEFAULT_RSYNC_TIMEOUT + 900)
        stub_config = """
        [DEFAULT]
        reclaim_age = 1209600
        [object-replicator]
        rsync_timeout = 3600
        [object-auditor]
        rsync_tempfile_timeout = auto
        """
        with open(config_path, 'w') as f:
            f.write(textwrap.dedent(stub_config))
        conf = readconf(config_path, 'object-auditor')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        # if there is an object-replicator section with a rsync_timeout
        # configured we'll use that value (3600) + 900
        self.assertEqual(auditor_worker.rsync_tempfile_timeout, 3600 + 900)

    def test_inprogress_rsync_tempfiles_get_cleaned_up(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)

        location = AuditLocation(self.disk_file._datadir, 'sda', '0',
                                 policy=self.disk_file.policy)

        data = b'VERIFY'
        etag = md5()
        timestamp = str(normalize_timestamp(time.time()))
        with self.disk_file.create() as writer:
            writer.write(data)
            etag.update(data)
            metadata = {
                'ETag': etag.hexdigest(),
                'X-Timestamp': timestamp,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
            writer.commit(Timestamp(timestamp))

        datafilename = None
        datadir_files = os.listdir(self.disk_file._datadir)
        for filename in datadir_files:
            if filename.endswith('.data'):
                datafilename = filename
                break
        else:
            self.fail('Did not find .data file in %r: %r' %
                      (self.disk_file._datadir, datadir_files))
        rsynctempfile_path = os.path.join(self.disk_file._datadir,
                                          '.%s.9ILVBL' % datafilename)
        open(rsynctempfile_path, 'w')
        # sanity check we have an extra file
        rsync_files = os.listdir(self.disk_file._datadir)
        self.assertEqual(len(datadir_files) + 1, len(rsync_files))

        # and after we turn the crank ...
        auditor_worker.object_audit(location)

        # ... we've still got the rsync file
        self.assertEqual(rsync_files, os.listdir(self.disk_file._datadir))

        # and we'll keep it - depending on the rsync_tempfile_timeout
        self.assertEqual(auditor_worker.rsync_tempfile_timeout, 86400)
        self.conf['rsync_tempfile_timeout'] = '3600'
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        self.assertEqual(auditor_worker.rsync_tempfile_timeout, 3600)
        now = time.time() + 1900
        with mock.patch('swift.obj.auditor.time.time',
                        return_value=now):
            auditor_worker.object_audit(location)
        self.assertEqual(rsync_files, os.listdir(self.disk_file._datadir))

        # but *tomorrow* when we run
        tomorrow = time.time() + 86400
        with mock.patch('swift.obj.auditor.time.time',
                        return_value=tomorrow):
            auditor_worker.object_audit(location)

        # ... we'll totally clean that stuff up!
        self.assertEqual(datadir_files, os.listdir(self.disk_file._datadir))

        # but if we have some random crazy file in there
        random_crazy_file_path = os.path.join(self.disk_file._datadir,
                                              '.random.crazy.file')
        open(random_crazy_file_path, 'w')

        tomorrow = time.time() + 86400
        with mock.patch('swift.obj.auditor.time.time',
                        return_value=tomorrow):
            auditor_worker.object_audit(location)

        # that's someone elses problem
        self.assertIn(os.path.basename(random_crazy_file_path),
                      os.listdir(self.disk_file._datadir))

    def test_generic_exception_handling(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        # pretend that we logged (and reset counters) just now
        auditor_worker.last_logged = time.time()
        timestamp = str(normalize_timestamp(time.time()))
        pre_errors = auditor_worker.errors
        data = b'0' * 1024
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
            writer.commit(Timestamp(timestamp))
        with mock.patch('swift.obj.diskfile.DiskFileManager.diskfile_cls',
                        lambda *_: 1 / 0):
            auditor_worker.audit_all_objects()
        self.assertEqual(auditor_worker.errors, pre_errors + 1)

    def test_object_run_once_pass(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        auditor_worker.log_time = 0
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = auditor_worker.quarantines
        data = b'0' * 1024

        def write_file(df):
            with df.create() as writer:
                writer.write(data)
                metadata = {
                    'ETag': md5(data).hexdigest(),
                    'X-Timestamp': timestamp,
                    'Content-Length': str(os.fstat(writer._fd).st_size),
                }
                writer.put(metadata)
                writer.commit(Timestamp(timestamp))

        # policy 0
        write_file(self.disk_file)
        # policy 1
        write_file(self.disk_file_p1)
        # policy 2
        write_file(self.disk_file_ec)

        auditor_worker.audit_all_objects()
        self.assertEqual(auditor_worker.quarantines, pre_quarantines)
        # 1 object per policy falls into 1024 bucket
        self.assertEqual(auditor_worker.stats_buckets[1024], 3)
        self.assertEqual(auditor_worker.stats_buckets[10240], 0)

        # pick up some additional code coverage, large file
        data = b'0' * 1024 * 1024
        for df in (self.disk_file, self.disk_file_ec):
            with df.create() as writer:
                writer.write(data)
                metadata = {
                    'ETag': md5(data).hexdigest(),
                    'X-Timestamp': timestamp,
                    'Content-Length': str(os.fstat(writer._fd).st_size),
                }
                writer.put(metadata)
                writer.commit(Timestamp(timestamp))
        auditor_worker.audit_all_objects(device_dirs=['sda', 'sdb'])
        self.assertEqual(auditor_worker.quarantines, pre_quarantines)
        # still have the 1024 byte object left in policy-1 (plus the
        # stats from the original 3)
        self.assertEqual(auditor_worker.stats_buckets[1024], 4)
        self.assertEqual(auditor_worker.stats_buckets[10240], 0)
        # and then policy-0 disk_file was re-written as a larger object
        self.assertEqual(auditor_worker.stats_buckets['OVER'], 2)

        # pick up even more additional code coverage, misc paths
        auditor_worker.log_time = -1
        auditor_worker.stats_sizes = []
        auditor_worker.audit_all_objects(device_dirs=['sda', 'sdb'])
        self.assertEqual(auditor_worker.quarantines, pre_quarantines)
        self.assertEqual(auditor_worker.stats_buckets[1024], 4)
        self.assertEqual(auditor_worker.stats_buckets[10240], 0)
        self.assertEqual(auditor_worker.stats_buckets['OVER'], 2)

    def test_object_run_logging(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        auditor_worker.audit_all_objects(device_dirs=['sda'])
        log_lines = self.logger.get_lines_for_level('info')
        self.assertGreater(len(log_lines), 0)
        self.assertTrue(log_lines[0].index('ALL - parallel, sda'))

        self.logger.clear()
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices,
                                               zero_byte_only_at_fps=50)
        auditor_worker.audit_all_objects(device_dirs=['sda'])
        log_lines = self.logger.get_lines_for_level('info')
        self.assertGreater(len(log_lines), 0)
        self.assertTrue(log_lines[0].index('ZBF - sda'))

    def test_object_run_recon_cache(self):
        ts = Timestamp(time.time())
        data = b'test_data'

        with self.disk_file.create() as writer:
            writer.write(data)
            metadata = {
                'ETag': md5(data).hexdigest(),
                'X-Timestamp': ts.normal,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
            writer.commit(ts)

        # all devices
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        auditor_worker.audit_all_objects()
        with open(self.rcache) as fd:
            actual_rcache = json.load(fd)
        expected = {'object_auditor_stats_ALL':
                    {'passes': 1, 'errors': 0, 'audit_time': mock.ANY,
                     'start_time': mock.ANY, 'quarantined': 0,
                     'bytes_processed': 9}}
        with open(self.rcache) as fd:
            actual_rcache = json.load(fd)
        self.assertEqual(expected, actual_rcache)

        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices,
                                               zero_byte_only_at_fps=50)
        auditor_worker.audit_all_objects()
        self.assertEqual(expected, actual_rcache)
        with open(self.rcache) as fd:
            actual_rcache = json.load(fd)
        expected.update({
            'object_auditor_stats_ZBF':
            {'passes': 1, 'errors': 0, 'audit_time': mock.ANY,
             'start_time': mock.ANY, 'quarantined': 0,
             'bytes_processed': 0}})
        self.assertEqual(expected, actual_rcache)

        # specific devices
        os.unlink(self.rcache)
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        auditor_worker.audit_all_objects(device_dirs=['sda'])
        with open(self.rcache) as fd:
            actual_rcache = json.load(fd)
        expected = {'object_auditor_stats_ALL':
                    {'sda': {'passes': 1, 'errors': 0, 'audit_time': mock.ANY,
                             'start_time': mock.ANY, 'quarantined': 0,
                             'bytes_processed': 9}}}
        with open(self.rcache) as fd:
            actual_rcache = json.load(fd)
        self.assertEqual(expected, actual_rcache)

        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices,
                                               zero_byte_only_at_fps=50)
        auditor_worker.audit_all_objects(device_dirs=['sda'])
        self.assertEqual(expected, actual_rcache)
        with open(self.rcache) as fd:
            actual_rcache = json.load(fd)
        expected.update({
            'object_auditor_stats_ZBF':
            {'sda': {'passes': 1, 'errors': 0, 'audit_time': mock.ANY,
                     'start_time': mock.ANY, 'quarantined': 0,
                     'bytes_processed': 0}}})
        self.assertEqual(expected, actual_rcache)

    def test_object_run_once_no_sda(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = auditor_worker.quarantines
        # pretend that we logged (and reset counters) just now
        auditor_worker.last_logged = time.time()
        data = b'0' * 1024
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
            os.write(writer._fd, b'extra_data')
            writer.commit(Timestamp(timestamp))
        auditor_worker.audit_all_objects()
        self.assertEqual(auditor_worker.quarantines, pre_quarantines + 1)

    def test_object_run_once_multi_devices(self):
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        # pretend that we logged (and reset counters) just now
        auditor_worker.last_logged = time.time()
        timestamp = str(normalize_timestamp(time.time()))
        pre_quarantines = auditor_worker.quarantines
        data = b'0' * 10
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
            writer.commit(Timestamp(timestamp))
        auditor_worker.audit_all_objects()
        self.disk_file = self.df_mgr.get_diskfile('sda', '0', 'a', 'c', 'ob',
                                                  policy=POLICIES.legacy)
        data = b'1' * 10
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
            writer.commit(Timestamp(timestamp))
            os.write(writer._fd, b'extra_data')
        auditor_worker.audit_all_objects()
        self.assertEqual(auditor_worker.quarantines, pre_quarantines + 1)

    def test_object_run_fast_track_non_zero(self):
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.auditor.log_time = 0
        data = b'0' * 1024
        etag = md5()
        with self.disk_file.create() as writer:
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
            writer.commit(Timestamp(timestamp))
            etag = md5()
            etag.update(b'1' + b'0' * 1023)
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
        clear_auditor_status(self.devices, 'objects')
        self.auditor.run_audit(**kwargs)
        self.assertTrue(os.path.isdir(quarantine_path))

    def setup_bad_zero_byte(self, timestamp=None):
        if timestamp is None:
            timestamp = Timestamp.now()
        self.auditor = auditor.ObjectAuditor(self.conf)
        self.auditor.log_time = 0
        etag = md5()
        with self.disk_file.create() as writer:
            etag = etag.hexdigest()
            metadata = {
                'ETag': etag,
                'X-Timestamp': timestamp.internal,
                'Content-Length': 10,
            }
            writer.put(metadata)
            writer.commit(Timestamp(timestamp))
            etag = md5()
            etag = etag.hexdigest()
            metadata['ETag'] = etag
            write_metadata(writer._fd, metadata)

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

        called_args = [0]

        def mock_get_auditor_status(path, logger, audit_type):
            called_args[0] = audit_type
            return get_auditor_status(path, logger, audit_type)

        with mock.patch('swift.obj.diskfile.get_auditor_status',
                        mock_get_auditor_status):
                self.auditor.run_audit(**kwargs)
        quarantine_path = os.path.join(self.devices,
                                       'sda', 'quarantined', 'objects')
        self.assertTrue(os.path.isdir(quarantine_path))
        self.assertEqual('ZBF', called_args[0])

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

    def test_with_only_tombstone(self):
        # sanity check that auditor doesn't touch solitary tombstones
        ts_iter = make_timestamp_iter()
        self.setup_bad_zero_byte(timestamp=next(ts_iter))
        self.disk_file.delete(next(ts_iter))
        files = os.listdir(self.disk_file._datadir)
        self.assertEqual(1, len(files))
        self.assertTrue(files[0].endswith('ts'))
        kwargs = {'mode': 'once'}
        self.auditor.run_audit(**kwargs)
        files_after = os.listdir(self.disk_file._datadir)
        self.assertEqual(files, files_after)

    def test_with_tombstone_and_data(self):
        # rsync replication could leave a tombstone and data file in object
        # dir - verify they are both removed during audit
        ts_iter = make_timestamp_iter()
        ts_tomb = next(ts_iter)
        ts_data = next(ts_iter)
        self.setup_bad_zero_byte(timestamp=ts_data)
        tomb_file_path = os.path.join(self.disk_file._datadir,
                                      '%s.ts' % ts_tomb.internal)
        with open(tomb_file_path, 'wb') as fd:
            write_metadata(fd, {'X-Timestamp': ts_tomb.internal})
        files = os.listdir(self.disk_file._datadir)
        self.assertEqual(2, len(files))
        self.assertTrue(os.path.basename(tomb_file_path) in files, files)
        kwargs = {'mode': 'once'}
        self.auditor.run_audit(**kwargs)
        self.assertFalse(os.path.exists(self.disk_file._datadir))

    def _audit_tombstone(self, conf, ts_tomb, zero_byte_fps=0):
        self.auditor = auditor.ObjectAuditor(conf)
        self.auditor.log_time = 0
        # create tombstone and hashes.pkl file, ensuring the tombstone is not
        # reclaimed by mocking time to be the tombstone time
        with mock.patch('time.time', return_value=float(ts_tomb)):
            # this delete will create an invalid hashes entry
            self.disk_file.delete(ts_tomb)
            # this get_hashes call will truncate the invalid hashes entry
            self.disk_file.manager.get_hashes(
                'sda', '0', [], self.disk_file.policy)
        suffix = basename(dirname(self.disk_file._datadir))
        part_dir = dirname(dirname(self.disk_file._datadir))
        # sanity checks...
        self.assertEqual(['%s.ts' % ts_tomb.internal],
                         os.listdir(self.disk_file._datadir))
        self.assertTrue(os.path.exists(os.path.join(part_dir, HASH_FILE)))
        hash_invalid = os.path.join(part_dir, HASH_INVALIDATIONS_FILE)
        self.assertTrue(os.path.exists(hash_invalid))
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(b'', fp.read().strip(b'\n'))
        # Run auditor
        self.auditor.run_audit(mode='once', zero_byte_fps=zero_byte_fps)
        # sanity check - auditor should not remove tombstone file
        self.assertEqual(['%s.ts' % ts_tomb.internal],
                         os.listdir(self.disk_file._datadir))
        return part_dir, suffix

    def test_non_reclaimable_tombstone(self):
        # audit with a recent tombstone
        ts_tomb = Timestamp(time.time() - 55)
        part_dir, suffix = self._audit_tombstone(self.conf, ts_tomb)
        self.assertTrue(os.path.exists(os.path.join(part_dir, HASH_FILE)))
        hash_invalid = os.path.join(part_dir, HASH_INVALIDATIONS_FILE)
        self.assertTrue(os.path.exists(hash_invalid))
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(b'', fp.read().strip(b'\n'))

    def test_reclaimable_tombstone(self):
        # audit with a reclaimable tombstone
        ts_tomb = Timestamp(time.time() - 604800)
        part_dir, suffix = self._audit_tombstone(self.conf, ts_tomb)
        self.assertTrue(os.path.exists(os.path.join(part_dir, HASH_FILE)))
        hash_invalid = os.path.join(part_dir, HASH_INVALIDATIONS_FILE)
        self.assertTrue(os.path.exists(hash_invalid))
        with open(hash_invalid, 'rb') as fp:
            hash_val = fp.read()
        self.assertEqual(suffix.encode('ascii'), hash_val.strip(b'\n'))

    def test_non_reclaimable_tombstone_with_custom_reclaim_age(self):
        # audit with a tombstone newer than custom reclaim age
        ts_tomb = Timestamp(time.time() - 604800)
        conf = dict(self.conf)
        conf['reclaim_age'] = 2 * 604800
        part_dir, suffix = self._audit_tombstone(conf, ts_tomb)
        self.assertTrue(os.path.exists(os.path.join(part_dir, HASH_FILE)))
        hash_invalid = os.path.join(part_dir, HASH_INVALIDATIONS_FILE)
        self.assertTrue(os.path.exists(hash_invalid))
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(b'', fp.read().strip(b'\n'))

    def test_reclaimable_tombstone_with_custom_reclaim_age(self):
        # audit with a tombstone older than custom reclaim age
        ts_tomb = Timestamp(time.time() - 55)
        conf = dict(self.conf)
        conf['reclaim_age'] = 10
        part_dir, suffix = self._audit_tombstone(conf, ts_tomb)
        self.assertTrue(os.path.exists(os.path.join(part_dir, HASH_FILE)))
        hash_invalid = os.path.join(part_dir, HASH_INVALIDATIONS_FILE)
        self.assertTrue(os.path.exists(hash_invalid))
        with open(hash_invalid, 'rb') as fp:
            hash_val = fp.read()
        self.assertEqual(suffix.encode('ascii'), hash_val.strip(b'\n'))

    def test_reclaimable_tombstone_with_zero_byte_fps(self):
        # audit with a tombstone older than reclaim age by a zero_byte_fps
        # worker does not invalidate the hash
        ts_tomb = Timestamp(time.time() - 604800)
        part_dir, suffix = self._audit_tombstone(
            self.conf, ts_tomb, zero_byte_fps=50)
        self.assertTrue(os.path.exists(os.path.join(part_dir, HASH_FILE)))
        hash_invalid = os.path.join(part_dir, HASH_INVALIDATIONS_FILE)
        self.assertTrue(os.path.exists(hash_invalid))
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(b'', fp.read().strip(b'\n'))

    def _test_expired_object_is_ignored(self, zero_byte_fps):
        # verify that an expired object does not get mistaken for a tombstone
        audit = auditor.ObjectAuditor(self.conf, logger=self.logger)
        audit.log_time = 0
        now = time.time()
        write_diskfile(self.disk_file, Timestamp(now - 20),
                       extra_metadata={'X-Delete-At': now - 10})
        files = os.listdir(self.disk_file._datadir)
        self.assertTrue([f for f in files if f.endswith('.data')])  # sanity
        # diskfile write appends to invalid hashes file
        part_dir = dirname(dirname(self.disk_file._datadir))
        hash_invalid = os.path.join(part_dir, HASH_INVALIDATIONS_FILE)
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(
                basename(dirname(self.disk_file._datadir)).encode('ascii'),
                fp.read().strip(b'\n'))  # sanity check

        # run the auditor...
        with mock.patch.object(auditor, 'dump_recon_cache'):
            audit.run_audit(mode='once', zero_byte_fps=zero_byte_fps)

        # the auditor doesn't touch anything on the invalidation file
        # (i.e. not truncate and add no entry)
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(
                basename(dirname(self.disk_file._datadir)).encode('ascii'),
                fp.read().strip(b'\n'))  # sanity check

        # this get_hashes call will truncate the invalid hashes entry
        self.disk_file.manager.get_hashes(
            'sda', '0', [], self.disk_file.policy)
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(b'', fp.read().strip(b'\n'))  # sanity check

        # run the auditor, again...
        with mock.patch.object(auditor, 'dump_recon_cache'):
            audit.run_audit(mode='once', zero_byte_fps=zero_byte_fps)

        # verify nothing changed
        self.assertTrue(os.path.exists(self.disk_file._datadir))
        self.assertEqual(files, os.listdir(self.disk_file._datadir))
        self.assertFalse(audit.logger.get_lines_for_level('error'))
        self.assertFalse(audit.logger.get_lines_for_level('warning'))
        # and there was no hash invalidation
        with open(hash_invalid, 'rb') as fp:
            self.assertEqual(b'', fp.read().strip(b'\n'))

    def test_expired_object_is_ignored(self):
        self._test_expired_object_is_ignored(0)

    def test_expired_object_is_ignored_with_zero_byte_fps(self):
        self._test_expired_object_is_ignored(50)

    def test_auditor_reclaim_age(self):
        # if we don't have access to the replicator config section we'll use
        # diskfile's default
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        router = auditor_worker.diskfile_router
        for policy in POLICIES:
            self.assertEqual(router[policy].reclaim_age, 86400 * 7)

        # if the reclaim_age option is set explicitly we use that
        self.conf['reclaim_age'] = '1800'
        auditor_worker = auditor.AuditorWorker(self.conf, self.logger,
                                               self.rcache, self.devices)
        router = auditor_worker.diskfile_router
        for policy in POLICIES:
            self.assertEqual(router[policy].reclaim_age, 1800)

        # if we have a real config we can be a little smarter
        config_path = os.path.join(self.testdir, 'objserver.conf')

        # if there is no object-replicator section we still have to fall back
        # to default because we can't parse the config for that section!
        stub_config = """
        [object-auditor]
        """
        with open(config_path, 'w') as f:
            f.write(textwrap.dedent(stub_config))
        conf = readconf(config_path, 'object-auditor')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        router = auditor_worker.diskfile_router
        for policy in POLICIES:
            self.assertEqual(router[policy].reclaim_age, 86400 * 7)

        # verify reclaim_age is of auditor config value
        stub_config = """
                [object-replicator]
                [object-auditor]
                reclaim_age = 60
                """
        with open(config_path, 'w') as f:
            f.write(textwrap.dedent(stub_config))
        conf = readconf(config_path, 'object-auditor')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        router = auditor_worker.diskfile_router
        for policy in POLICIES:
            self.assertEqual(router[policy].reclaim_age, 60)

        # verify reclaim_age falls back to replicator config value
        # if there is no auditor config value
        config_path = os.path.join(self.testdir, 'objserver.conf')
        stub_config = """
                [object-replicator]
                reclaim_age = 60
                [object-auditor]
                """
        with open(config_path, 'w') as f:
            f.write(textwrap.dedent(stub_config))
        conf = readconf(config_path, 'object-auditor')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        router = auditor_worker.diskfile_router
        for policy in POLICIES:
            self.assertEqual(router[policy].reclaim_age, 60)

        # we'll prefer our own DEFAULT section to the replicator though
        self.assertEqual(auditor_worker.rsync_tempfile_timeout,
                         replicator.DEFAULT_RSYNC_TIMEOUT + 900)
        stub_config = """
        [DEFAULT]
        reclaim_age = 1209600
        [object-replicator]
        reclaim_age = 1800
        [object-auditor]
        """
        with open(config_path, 'w') as f:
            f.write(textwrap.dedent(stub_config))
        conf = readconf(config_path, 'object-auditor')
        auditor_worker = auditor.AuditorWorker(conf, self.logger,
                                               self.rcache, self.devices)
        router = auditor_worker.diskfile_router
        for policy in POLICIES:
            self.assertEqual(router[policy].reclaim_age, 1209600)

    def test_sleeper(self):
        with mock.patch(
                'time.sleep', mock.MagicMock()) as mock_sleep:
            my_auditor = auditor.ObjectAuditor(self.conf)
            my_auditor._sleep()
            mock_sleep.assert_called_with(30)

            my_conf = dict(interval=2)
            my_conf.update(self.conf)
            my_auditor = auditor.ObjectAuditor(my_conf)
            my_auditor._sleep()
            mock_sleep.assert_called_with(2)

            my_auditor = auditor.ObjectAuditor(self.conf)
            my_auditor.interval = 2
            my_auditor._sleep()
            mock_sleep.assert_called_with(2)

    def test_run_parallel_audit(self):

        class StopForever(Exception):
            pass

        class Bogus(Exception):
            pass

        loop_error = Bogus('exception')

        class LetMeOut(BaseException):
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
                raise loop_error

            def mock_fork(self):
                self.fork_called += 1
                if self.master:
                    return self.fork_called
                else:
                    return 0

            def mock_wait(self):
                self.wait_called += 1
                return (self.wait_called, 0)

            def mock_signal(self, sig, action):
                pass

            def mock_exit(self):
                pass

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
        was_signal = signal.signal
        was_exit = sys.exit
        os.fork = mocker.mock_fork
        os.wait = mocker.mock_wait
        signal.signal = mocker.mock_signal
        sys.exit = mocker.mock_exit
        try:
            my_auditor._sleep = mocker.mock_sleep_stop
            my_auditor.run_once(zero_byte_fps=50)
            my_auditor.logger.exception.assert_called_once_with(
                'ERROR auditing: %s', loop_error)
            my_auditor.logger.exception.reset_mock()
            self.assertRaises(StopForever, my_auditor.run_forever)
            my_auditor.logger.exception.assert_called_once_with(
                'ERROR auditing: %s', loop_error)
            my_auditor.audit_loop = real_audit_loop

            # sleep between ZBF scanner forks
            self.assertRaises(StopForever, my_auditor.fork_child, True, True)

            mocker.fork_called = 0
            signal.signal = was_signal
            sys.exit = was_exit
            self.assertRaises(StopForever,
                              my_auditor.run_forever, zero_byte_fps=50)
            self.assertEqual(mocker.check_kwargs['zero_byte_fps'], 50)
            self.assertEqual(mocker.fork_called, 0)

            self.assertRaises(SystemExit, my_auditor.run_once)
            self.assertEqual(mocker.fork_called, 1)
            self.assertEqual(mocker.check_kwargs['zero_byte_fps'], 89)
            self.assertEqual(mocker.check_device_dir, [])
            self.assertEqual(mocker.check_args, ())

            device_list = ['sd%s' % i for i in string.ascii_letters[2:10]]
            device_string = ','.join(device_list)
            device_string_bogus = device_string + ',bogus'

            mocker.fork_called = 0
            self.assertRaises(SystemExit, my_auditor.run_once,
                              devices=device_string_bogus)
            self.assertEqual(mocker.fork_called, 1)
            self.assertEqual(mocker.check_kwargs['zero_byte_fps'], 89)
            self.assertEqual(sorted(mocker.check_device_dir), device_list)

            mocker.master = 1

            mocker.fork_called = 0
            self.assertRaises(StopForever, my_auditor.run_forever)
            # Fork or Wait are called greate than or equal to 2 times in the
            # main process. 2 times if zbf run once and 3 times if zbf run
            # again
            self.assertGreaterEqual(mocker.fork_called, 2)
            self.assertGreaterEqual(mocker.wait_called, 2)

            my_auditor._sleep = mocker.mock_sleep_continue
            my_auditor.audit_loop = works_only_once(my_auditor.audit_loop,
                                                    LetMeOut())

            my_auditor.concurrency = 2
            mocker.fork_called = 0
            mocker.wait_called = 0
            self.assertRaises(LetMeOut, my_auditor.run_forever)
            # Fork or Wait are called greater than or equal to
            # no. of devices + (no. of devices)/2 + 1 times in main process
            no_devices = len(os.listdir(self.devices))
            self.assertGreaterEqual(mocker.fork_called, no_devices +
                                    no_devices / 2 + 1)
            self.assertGreaterEqual(mocker.wait_called, no_devices +
                                    no_devices / 2 + 1)

        finally:
            os.fork = was_fork
            os.wait = was_wait

    def test_run_audit_once(self):
        my_auditor = auditor.ObjectAuditor(dict(devices=self.devices,
                                                mount_check='false',
                                                zero_byte_files_per_second=89,
                                                concurrency=1))

        forked_pids = []
        next_zbf_pid = [2]
        next_normal_pid = [1001]
        outstanding_pids = [[]]

        def fake_fork_child(**kwargs):
            if len(forked_pids) > 10:
                # something's gone horribly wrong
                raise BaseException("forking too much")

            # ZBF pids are all smaller than the normal-audit pids; this way
            # we can return them first.
            #
            # Also, ZBF pids are even and normal-audit pids are odd; this is
            # so humans seeing this test fail can better tell what's happening.
            if kwargs.get('zero_byte_fps'):
                pid = next_zbf_pid[0]
                next_zbf_pid[0] += 2
            else:
                pid = next_normal_pid[0]
                next_normal_pid[0] += 2
            outstanding_pids[0].append(pid)
            forked_pids.append(pid)
            return pid

        def fake_os_wait():
            # Smallest pid first; that's ZBF if we have one, else normal
            outstanding_pids[0].sort()
            pid = outstanding_pids[0].pop(0)
            return (pid, 0)   # (pid, status)

        with mock.patch("swift.obj.auditor.os.wait", fake_os_wait), \
                mock.patch.object(my_auditor, 'fork_child', fake_fork_child), \
                mock.patch.object(my_auditor, '_sleep', lambda *a: None):
            my_auditor.run_once()

        self.assertEqual(sorted(forked_pids), [2, 1001])

    def test_run_audit_once_zbfps(self):
        my_auditor = auditor.ObjectAuditor(dict(devices=self.devices,
                                                mount_check='false',
                                                zero_byte_files_per_second=89,
                                                concurrency=1,
                                                recon_cache_path=self.testdir))

        with mock.patch.object(my_auditor, '_sleep', lambda *a: None):
            my_auditor.run_once(zero_byte_fps=50)

        with open(self.rcache) as fd:
            # there's no objects to audit so expect no stats; this assertion
            # may change if https://bugs.launchpad.net/swift/+bug/1704858 is
            # fixed
            self.assertEqual({}, json.load(fd))

        # check recon cache stays clean after a second run
        with mock.patch.object(my_auditor, '_sleep', lambda *a: None):
            my_auditor.run_once(zero_byte_fps=50)

        with open(self.rcache) as fd:
            self.assertEqual({}, json.load(fd))

        ts = Timestamp(time.time())
        with self.disk_file.create() as writer:
            metadata = {
                'ETag': md5(b'').hexdigest(),
                'X-Timestamp': ts.normal,
                'Content-Length': str(os.fstat(writer._fd).st_size),
            }
            writer.put(metadata)
            writer.commit(ts)

        # check recon cache stays clean after a second run
        with mock.patch.object(my_auditor, '_sleep', lambda *a: None):
            my_auditor.run_once(zero_byte_fps=50)
        with open(self.rcache) as fd:
            self.assertEqual({
                'object_auditor_stats_ZBF': {
                    'audit_time': 0,
                    'bytes_processed': 0,
                    'errors': 0,
                    'passes': 1,
                    'quarantined': 0,
                    'start_time': mock.ANY}},
                json.load(fd))

    def test_run_parallel_audit_once(self):
        my_auditor = auditor.ObjectAuditor(
            dict(devices=self.devices, mount_check='false',
                 zero_byte_files_per_second=89, concurrency=2))

        # ZBF pids are smaller than the normal-audit pids; this way we can
        # return them first from our mocked os.wait().
        #
        # Also, ZBF pids are even and normal-audit pids are odd; this is so
        # humans seeing this test fail can better tell what's happening.
        forked_pids = []
        next_zbf_pid = [2]
        next_normal_pid = [1001]
        outstanding_pids = [[]]

        def fake_fork_child(**kwargs):
            if len(forked_pids) > 10:
                # something's gone horribly wrong; try not to hang the test
                # run because of it
                raise BaseException("forking too much")

            if kwargs.get('zero_byte_fps'):
                pid = next_zbf_pid[0]
                next_zbf_pid[0] += 2
            else:
                pid = next_normal_pid[0]
                next_normal_pid[0] += 2
            outstanding_pids[0].append(pid)
            forked_pids.append(pid)
            return pid

        def fake_os_wait():
            if not outstanding_pids[0]:
                raise BaseException("nobody waiting")

            # ZBF auditor finishes first
            outstanding_pids[0].sort()
            pid = outstanding_pids[0].pop(0)
            return (pid, 0)   # (pid, status)

        # make sure we've got enough devs that the ZBF auditor can finish
        # before all the normal auditors have been started
        mkdirs(os.path.join(self.devices, 'sdc'))
        mkdirs(os.path.join(self.devices, 'sdd'))

        with mock.patch("swift.obj.auditor.os.wait", fake_os_wait), \
                mock.patch.object(my_auditor, 'fork_child', fake_fork_child), \
                mock.patch.object(my_auditor, '_sleep', lambda *a: None):
            my_auditor.run_once()

        self.assertEqual(sorted(forked_pids), [2, 1001, 1003, 1005, 1007])

    def test_run_parallel_audit_once_failed_fork(self):
        my_auditor = auditor.ObjectAuditor(
            dict(devices=self.devices, mount_check='false',
                 concurrency=2))

        start_pid = [1001]
        outstanding_pids = []
        failed_once = [False]

        def failing_fork(**kwargs):
            # this fork fails only on the 2nd call
            # it's enough to cause the growth of orphaned child processes
            if len(outstanding_pids) > 0 and not failed_once[0]:
                failed_once[0] = True
                raise OSError
            start_pid[0] += 2
            pid = start_pid[0]
            outstanding_pids.append(pid)
            return pid

        def fake_wait():
            return outstanding_pids.pop(0), 0

        with mock.patch("swift.obj.auditor.os.wait", fake_wait), \
                mock.patch.object(my_auditor, 'fork_child', failing_fork), \
                mock.patch.object(my_auditor, '_sleep', lambda *a: None):
            for i in range(3):
                my_auditor.run_once()

        self.assertEqual(len(outstanding_pids), 0,
                         "orphaned children left {0}, expected 0."
                         .format(outstanding_pids))


if __name__ == '__main__':
    unittest.main()
