# Copyright (c) 2010-2022 OpenStack Foundation
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

import tempfile
import shutil
import os
from unittest import mock

from unittest import TestCase
from swift.cli.recon_cron import get_async_count
from swift.obj.diskfile import ASYNCDIR_BASE


class TestReconCron(TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir)

    def test_get_async_count(self):
        device_dir = os.path.join(self.temp_dir, 'device')
        device_index = os.path.join(device_dir, '1')
        async_dir = os.path.join(device_index, ASYNCDIR_BASE)
        entry1 = os.path.join(async_dir, 'entry1')
        entry2 = os.path.join(async_dir, 'entry2')
        os.makedirs(entry1)
        os.makedirs(entry2)

        pending_file1 = os.path.join(entry1, 'pending_file1')
        pending_file2 = os.path.join(entry1, 'pending_file2')
        pending_file3 = os.path.join(entry2, 'pending_file3')
        open(pending_file1, 'w').close()
        open(pending_file2, 'w').close()
        open(pending_file3, 'w').close()

        count = get_async_count(device_dir)
        self.assertEqual(count, 3)

    def test_get_async_count_deleted(self):
        device_dir = os.path.join(self.temp_dir, 'device')
        device_index = os.path.join(device_dir, '1')
        async_dir = os.path.join(device_index, ASYNCDIR_BASE)
        entry1 = os.path.join(async_dir, 'entry1')
        entry2 = os.path.join(async_dir, 'entry2')
        os.makedirs(entry1)
        os.makedirs(entry2)

        pending_file1 = os.path.join(entry1, 'pending_file1')
        pending_file2 = os.path.join(entry1, 'pending_file2')
        pending_file3 = os.path.join(entry2, 'pending_file3')
        open(pending_file1, 'w').close()
        open(pending_file2, 'w').close()
        open(pending_file3, 'w').close()

        orig_isdir = os.path.isdir

        def racy_isdir(d):
            result = orig_isdir(d)
            if d == entry1:
                # clean it up before caller has a chance to descend
                shutil.rmtree(entry1)
            return result

        with mock.patch('os.path.isdir', racy_isdir):
            count = get_async_count(device_dir)
        self.assertEqual(count, 1)
