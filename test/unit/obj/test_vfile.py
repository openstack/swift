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

"""Tests for swift.obj.vfile"""

import unittest

from swift.common.storage_policy import StoragePolicy
from test.unit import patch_policies, FakeRing
import os.path
from swift.common import utils
from shutil import rmtree
from swift.obj import vfile
from swift.obj.vfile_utils import VFileUtilException
import tempfile
import mock


class MockRpc(mock.MagicMock):
    def list_partitions(self, socket_path, part_power):
        # TODO: need review, I'm not sure if the list_partition would return
        # only part numbers (or path)
        return ["1", "2", "3"]


class TestVFile(unittest.TestCase):

    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.object_dir = os.path.join(self.testdir, 'objects')
        utils.mkdirs(self.object_dir)
        self.mock_rpc = MockRpc()
        self.ring = FakeRing(part_power=18)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    @patch_policies([StoragePolicy(0, 'zero', False,
                                   object_ring=FakeRing(part_power=18)),
                     StoragePolicy(1, 'one', True,
                                   object_ring=FakeRing(part_power=20))])
    def test_vfile_listdir_partitions(self):

        tests = [
            {"path": "/srv/node/sda/objects",
             "expected": ["/srv/node/sda/losf/rpc.socket", 18]},
            {"path": "/sdb/objects",
             "expected": ["/sdb/losf/rpc.socket", 18]},
            {"path": "/sdc/objects-1",
             "expected": ["/sdc/losf-1/rpc.socket", 20]},
        ]
        with mock.patch(
                "swift.obj.vfile.rpc.list_partitions") as m_list_partitions:
            for test in tests:
                vfile.listdir(test["path"])
                m_list_partitions.assert_called_once_with(*test["expected"])
                m_list_partitions.reset_mock()

    @patch_policies([StoragePolicy(0, 'zero', False,
                                   object_ring=FakeRing(part_power=18)),
                     StoragePolicy(1, 'one', True,
                                   object_ring=FakeRing(part_power=20))])
    def test_vfile_listdir_partition(self):

        tests = [
            {"path": "/srv/node/sda/objects/123",
             "expected": ["/srv/node/sda/losf/rpc.socket", 123, 18]},
            {"path": "/sdb/objects/124",
             "expected": ["/sdb/losf/rpc.socket", 124, 18]},
            {"path": "/sdc/objects-1/789",
             "expected": ["/sdc/losf-1/rpc.socket", 789, 20]},
        ]
        with mock.patch(
                "swift.obj.vfile.rpc.list_partition") as m_list_partition:
            for test in tests:
                vfile.listdir(test["path"])
                m_list_partition.assert_called_once_with(*test["expected"])
                m_list_partition.reset_mock()

    @patch_policies([StoragePolicy(0, 'zero', False,
                                   object_ring=FakeRing(part_power=18)),
                     StoragePolicy(1, 'one', True,
                                   object_ring=FakeRing(part_power=20))])
    def test_vfile_listdir_suffix(self):

        tests = [
            {"path": "/srv/node/sda/objects/123/abc",
             "expected": ["/srv/node/sda/losf/rpc.socket", 123, "abc", 18]},
            {"path": "/sdb/objects/124/bcd",
             "expected": ["/sdb/losf/rpc.socket", 124, "bcd", 18]},
            {"path": "/sdc/objects-1/789/def",
             "expected": ["/sdc/losf-1/rpc.socket", 789, "def", 20]},
        ]
        with mock.patch(
                "swift.obj.vfile.rpc.list_suffix") as m_list_suffix:
            for test in tests:
                vfile.listdir(test["path"])
                m_list_suffix.assert_called_once_with(*test["expected"])
                m_list_suffix.reset_mock()

    def test_vfile_listdir_with_invalid_path(self):
        self.assertRaises(VFileUtilException, vfile.listdir, "/test/no-obj/")


if __name__ == '__main__':
    unittest.main()
