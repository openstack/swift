# Copyright (c) 2010 OpenStack, LLC.
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

import unittest
import tempfile
import os
import time
from shutil import rmtree
from hashlib import md5
from swift.obj import auditor
from swift.obj.server import DiskFile
from swift.common.utils import hash_path, mkdirs, normalize_timestamp
from swift.common.exceptions import AuditException

class TestAuditor(unittest.TestCase):

    def setUp(self):
        # Setup a test ring (stolen from common/test_ring.py)
        self.path_to_test_xfs = os.environ.get('PATH_TO_TEST_XFS')
        if not self.path_to_test_xfs or \
                not os.path.exists(self.path_to_test_xfs):
            print >>sys.stderr, 'WARNING: PATH_TO_TEST_XFS not set or not ' \
                'pointing to a valid directory.\n' \
                'Please set PATH_TO_TEST_XFS to a directory on an XFS file ' \
                'system for testing.'
            self.testdir = '/tmp/SWIFTUNITTEST'
        else:
            self.testdir = os.path.join(self.path_to_test_xfs,
                            'tmp_test_object_auditor')

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

        self.conf = dict(
            devices=self.devices,
            mount_check='false',
            timeout='300', stats_interval='1')

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_object_audit(self):
        self.auditor = auditor.ObjectAuditor(
            self.conf)
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

            # etag = md5()
            # etag.update(data)
            # etag = etag.hexdigest()
            # metadata['ETag'] = etag
            # disk_file.put(fd, tmppath, metadata)

            # self.auditor.object_audit(
            #     os.path.join(disk_file.datadir, timestamp + '.data'),
            #     'sda', cur_part)
            # self.assertEquals(self.auditor.quarantines, pre_quarantines)

            os.write(fd, 'bad_data')
            disk_file.close()
            self.auditor.object_audit(
                os.path.join(disk_file.datadir, timestamp + '.data'),
                'sda', cur_part)
            self.assertEquals(self.auditor.quarantines, pre_quarantines + 1)


if __name__ == '__main__':
    unittest.main()
