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

"""Tests for swift.obj.vfile_utils"""

import unittest
from itertools import chain

import mock

from swift.common.storage_policy import StoragePolicy
from swift.obj import vfile_utils
from test.unit import patch_policies


class TestVfileUtils(unittest.TestCase):

    def test_get_socket_path_from_volume_path(self):
        volume_path = "/srv/node/sdb1/losf/volumes/v0000001"
        expected = "/srv/node/sdb1/losf/rpc.socket"
        socket_path = vfile_utils.get_socket_path_from_volume_path(volume_path)
        self.assertEqual(socket_path, expected)

        volume_path = "/sdc1/losf-2/volumes/v0000234"
        expected = "/sdc1/losf-2/rpc.socket"
        socket_path = vfile_utils.get_socket_path_from_volume_path(volume_path)
        self.assertEqual(expected, socket_path)

        volume_path = "/losf-99/volumes/v0000001"
        expected = "/losf-99/rpc.socket"
        socket_path = vfile_utils.get_socket_path_from_volume_path(volume_path)
        self.assertEqual(expected, socket_path)

        volume_path = "/volumes/v0000001"
        with self.assertRaises(ValueError):
            vfile_utils.get_socket_path_from_volume_path(volume_path)

        volume_path = "/srv/node/sdb1/losf-notAnInt/volumes/v0000001"
        with self.assertRaises(ValueError):
            vfile_utils.get_socket_path_from_volume_path(volume_path)

        volume_path = "/srv/node/sdb1/losf/not-volumes/v0000001"
        with self.assertRaises(ValueError):
            vfile_utils.get_socket_path_from_volume_path(volume_path)

        volume_path = "/srv/node/sdb1/losf/volumes/not-a-volume"
        with self.assertRaises(ValueError):
            vfile_utils.get_socket_path_from_volume_path(volume_path)

    def test_get_mountpoint_from_volume_path(self):
        volume_path = "/srv/node/sdb1/losf/volumes/v0000001"
        expected = "/srv/node/sdb1"
        mountpoint = vfile_utils.get_mountpoint_from_volume_path(volume_path)
        self.assertEqual(expected, mountpoint)

        volume_path = "/sdb1/losf-2/volumes/v0000234"
        expected = "/sdb1"
        mountpoint = vfile_utils.get_mountpoint_from_volume_path(volume_path)
        self.assertEqual(expected, mountpoint)

        volume_path = "/losf-99/volumes/v0000001"
        expected = "/"
        socket_path = vfile_utils.get_mountpoint_from_volume_path(volume_path)
        self.assertEqual(expected, socket_path)

        volume_path = "/volumes/v0000001"
        with self.assertRaises(ValueError):
            vfile_utils.get_mountpoint_from_volume_path(volume_path)

        volume_path = "/srv/node/sdb1/losf-notAnInt/volumes/v0000001"
        with self.assertRaises(ValueError):
            vfile_utils.get_mountpoint_from_volume_path(volume_path)

        volume_path = "/srv/node/sdb1/losf/not-volumes/v0000001"
        with self.assertRaises(ValueError):
            vfile_utils.get_mountpoint_from_volume_path(volume_path)

        volume_path = "/srv/node/sdb1/losf/volumes/not-a-volume"
        with self.assertRaises(ValueError):
            vfile_utils.get_mountpoint_from_volume_path(volume_path)

    def test_get_volume_index(self):
        v_index = vfile_utils.get_volume_index("v0000001")
        self.assertEqual(v_index, 1)
        v_index = vfile_utils.get_volume_index("/sda/losf/volumes/v0000001")
        self.assertEqual(v_index, 1)
        v_index = vfile_utils.get_volume_index("v9876543")
        self.assertEqual(v_index, 9876543)
        with self.assertRaises(ValueError):
            vfile_utils.get_volume_index("v1")
        with self.assertRaises(ValueError):
            vfile_utils.get_volume_index("v12345678")
        with self.assertRaises(ValueError):
            vfile_utils.get_volume_index("x0000001")
        with self.assertRaises(ValueError):
            vfile_utils.get_volume_index("12345678")
        with self.assertRaises(ValueError):
            vfile_utils.get_volume_index("vz000001")

    def test_next_aligned_offset(self):
        test_data = [
            {"args": [0, 4096], "expected": 0},
            {"args": [4096, 4096], "expected": 4096},
            {"args": [4095, 4096], "expected": 4096},
            {"args": [4097, 4096], "expected": 8192},
            {"args": [4095, 8192], "expected": 8192},
        ]
        for test in test_data:
            aligned_offset = vfile_utils.next_aligned_offset(*test["args"])
            self.assertEqual(aligned_offset, test["expected"])

    def test_change_user(self):
        with mock.patch("swift.obj.vfile_utils.pwd.getpwnam") as m_getpwnam, \
                mock.patch("swift.obj.vfile_utils.os.setuid") as m_setuid:
            pw = mock.MagicMock()
            pw.pw_uid = 123
            m_getpwnam.return_value = pw

            vfile_utils.change_user("dummy")
            m_getpwnam.assert_called_once_with("dummy")
            m_setuid.assert_called_once_with(123)


@patch_policies([StoragePolicy(0, 'zero', False),
                 StoragePolicy(1, 'one', True)])
class TestSwiftPathInfo(unittest.TestCase):

    def test_swift_path_info(self):
        test_data = [
            {"path": "/sda/objects/1234/def/d41d8cd98f00b204e9800998ecf8427e/"
                     "1522913866.16520#12#d.data",
             "type": "file",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "partition": "1234",
             "suffix": "def",
             "ohash": "d41d8cd98f00b204e9800998ecf8427e",
             "filename": "1522913866.16520#12#d.data"},
            {"path": "/sda/objects/1234/def/d41d8cd98f00b204e9800998ecf8427e",
             "type": "ohash",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "partition": "1234",
             "suffix": "def",
             "ohash": "d41d8cd98f00b204e9800998ecf8427e",
             "filename": None},
            {"path": "/sda/objects/1234/def",
             "type": "suffix",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "partition": "1234",
             "suffix": "def",
             "ohash": None,
             "filename": None},
            {"path": "/sda/objects/1234",
             "type": "partition",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "partition": "1234",
             "suffix": None,
             "ohash": None,
             "filename": None},
            {"path": "/sda/objects",
             "type": "partitions",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "partition": None,
             "suffix": None,
             "ohash": None,
             "filename": None},
        ]

        test_data_others = [
            # extra slashes
            {"path": "//sda/objects/1234/def/d41d8cd98f00b204e9800998ecf8427e/"
                     "1522913866.16520#12#d.data/",
             "type": "file",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "partition": "1234",
             "suffix": "def",
             "ohash": "d41d8cd98f00b204e9800998ecf8427e",
             "filename": "1522913866.16520#12#d.data"},
            # Longer mountpoint
            {"path": "/srv/node1/sda1/objects",
             "type": "partitions",
             "socket_path": "/srv/node1/sda1/losf/rpc.socket",
             "volume_dir": "/srv/node1/sda1/losf/volumes",
             "policy_idx": 0,
             "partition": None,
             "suffix": None,
             "ohash": None,
             "filename": None},
            # Policy 1
            {"path": "/srv/node1/sda1/objects-1",
             "type": "partitions",
             "socket_path": "/srv/node1/sda1/losf-1/rpc.socket",
             "volume_dir": "/srv/node1/sda1/losf-1/volumes",
             "policy_idx": 1,
             "partition": None,
             "suffix": None,
             "ohash": None,
             "filename": None},
        ]

        for test in chain(test_data, test_data_others):
            si = vfile_utils.SwiftPathInfo.from_path(test["path"])
            self.assertEqual(si.type, test["type"])
            self.assertEqual(si.socket_path, test["socket_path"])
            self.assertEqual(si.volume_dir, test["volume_dir"])
            self.assertEqual(si.policy_idx, test["policy_idx"])
            self.assertEqual(si.partition, test["partition"])
            self.assertEqual(si.suffix, test["suffix"])
            self.assertEqual(si.ohash, test["ohash"])
            self.assertEqual(si.filename, test["filename"])

    def test_swift_path_info_error(self):
        with self.assertRaises(vfile_utils.VOSError):
            vfile_utils.SwiftPathInfo.from_path("/invalid")
        with self.assertRaises(vfile_utils.VOSError):
            vfile_utils.SwiftPathInfo.from_path("/srv/node/sda2")
        with self.assertRaises(vfile_utils.VOSError):
            invalid_path = "/sda/objects/1234/def" \
                           "/d41d8cd98f00b204e9800998ecf8427e/" \
                           "1522913866.16520#12#d.data/extra_dir"
            vfile_utils.SwiftPathInfo.from_path(invalid_path)


@patch_policies([StoragePolicy(0, 'zero', False),
                 StoragePolicy(1, 'one', True)])
class TestSwiftQuarantinedPathInfo(unittest.TestCase):

    def test_swift_path_info(self):
        test_data = [
            {"path": "/sda/quarantined/objects/"
                     "d41d8cd98f00b204e9800998ecf8427e/"
                     "1522913866.16520#12#d.data",
             "type": "file",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "ohash": "d41d8cd98f00b204e9800998ecf8427e",
             "filename": "1522913866.16520#12#d.data"},
            {"path": "/sda/quarantined/objects/"
                     "d41d8cd98f00b204e9800998ecf8427e/",
             "type": "ohash",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "ohash": "d41d8cd98f00b204e9800998ecf8427e",
             "filename": None},
            {"path": "/sda/quarantined/objects",
             "type": "ohashes",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "partition": None,
             "suffix": None,
             "ohash": None,
             "filename": None},
        ]

        test_data_others = [
            # extra slashes
            {"path": "//sda/quarantined/objects/"
                     "d41d8cd98f00b204e9800998ecf8427e//"
                     "1522913866.16520#12#d.data/",
             "type": "file",
             "socket_path": "/sda/losf/rpc.socket",
             "volume_dir": "/sda/losf/volumes",
             "policy_idx": 0,
             "ohash": "d41d8cd98f00b204e9800998ecf8427e",
             "filename": "1522913866.16520#12#d.data"},
            # Longer mountpoint
            {"path": "/srv/node1/sda1/quarantined/objects",
             "type": "ohashes",
             "socket_path": "/srv/node1/sda1/losf/rpc.socket",
             "volume_dir": "/srv/node1/sda1/losf/volumes",
             "policy_idx": 0,
             "ohash": None,
             "filename": None},
            # Policy 1
            {"path": "/srv/node1/sda1/quarantined/objects-1",
             "type": "ohashes",
             "socket_path": "/srv/node1/sda1/losf-1/rpc.socket",
             "volume_dir": "/srv/node1/sda1/losf-1/volumes",
             "policy_idx": 1,
             "ohash": None,
             "filename": None},
        ]

        for test in chain(test_data, test_data_others):
            si = vfile_utils.SwiftQuarantinedPathInfo.from_path(test["path"])
            self.assertEqual(si.type, test["type"])
            self.assertEqual(si.socket_path, test["socket_path"])
            self.assertEqual(si.volume_dir, test["volume_dir"])
            self.assertEqual(si.policy_idx, test["policy_idx"])
            self.assertEqual(si.ohash, test["ohash"])
            self.assertEqual(si.filename, test["filename"])

    def test_swift_path_info_error(self):
        with self.assertRaises(vfile_utils.VOSError):
            vfile_utils.SwiftPathInfo.from_path("/invalid")
        with self.assertRaises(vfile_utils.VOSError):
            vfile_utils.SwiftPathInfo.from_path("/srv/node/sda2")
        with self.assertRaises(vfile_utils.VOSError):
            invalid_path = "/sdb/objects/1234/def" \
                           "/d41d8cd98f00b204e9800998ecf8427e/" \
                           "1522913866.16520#12#d.data/extra_dir"
            vfile_utils.SwiftPathInfo.from_path(invalid_path)
