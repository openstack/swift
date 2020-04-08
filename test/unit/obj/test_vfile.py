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
import errno
import fcntl
import shutil
import unittest
from random import randint

import six

from swift.common.storage_policy import StoragePolicy
from swift.obj.header import ObjectHeader, STATE_OBJ_FILE, \
    MAX_OBJECT_HEADER_LEN
from swift.obj.meta_pb2 import Metadata
from swift.obj.vfile import VFileWriter
from swift.obj.vfile_utils import VOSError, next_aligned_offset
from swift.obj.rpc_http import RpcError, StatusCode
from test.unit import patch_policies, FakeRing
import os.path
from swift.common import utils
from shutil import rmtree
from swift.obj import vfile, header, fmgr_pb2
import tempfile
import mock


class TestVFileModuleMethods(unittest.TestCase):

    def setUp(self):
        self.testdir = tempfile.mkdtemp()
        self.object_dir = os.path.join(self.testdir, 'objects')
        utils.mkdirs(self.object_dir)
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
        self.assertRaises(VOSError, vfile.listdir, "/test/no-obj/")


@patch_policies([StoragePolicy(0, 'zero', False),
                 StoragePolicy(1, 'one', True)])
class TestVFileWriter(unittest.TestCase):
    @mock.patch("swift.obj.vfile.open_or_create_volume")
    @mock.patch("swift.obj.rpc_http.get_next_offset")
    @mock.patch("swift.obj.vfile._may_grow_volume")
    @mock.patch("swift.obj.vfile.os.lseek")
    @mock.patch("swift.obj.vfile.VFileWriter.__new__")
    def test_create(self, m_cls, m_lseek, m_grow_vol, m_get_next_offset,
                    m_open_or_create_vol):
        vfile_conf = {
            'volume_alloc_chunk_size': 16 * 1024,
            'volume_low_free_space': 8 * 1024,
            'metadata_reserve': 500,
            'max_volume_count': 1000,
            'max_volume_size': 10 * 1024 * 1024 * 1024,
        }

        test_sets = [
            # small file with policy 0
            {
                "datadir": "/sda/objects/123/4d8/"
                           "acbd18db4cc2f85cedef654fccc4a4d8",
                "obj_size": 421,

                "volume_file": 101,  # file descriptor
                "lock_file": 102,   # file descriptor
                "volume_path": "/sda/losf/volumes/v0000001",
                "volume_next_offset": 8192,

                "expected_socket": "/sda/losf/rpc.socket",
                "expected_ohash": "acbd18db4cc2f85cedef654fccc4a4d8",
                "expected_partition": "123",
                "expected_extension": None,
                "expected_vol_dir": "/sda/losf/volumes",
                "expected_vol_index": 1,
            },
            # large file with policy 1
            {
                "datadir": "/sdb/objects-1/456/289/"
                           "48e2e79fec9bc01d9a00e0a8fa68b289",
                "obj_size": 2 * 1024 * 1024 * 1024,

                "volume_file": 201,
                "lock_file": 202,
                "volume_path": "/sdb/losf-1/volumes/v0012345",
                "volume_next_offset": 8 * 1024 * 1024 * 1024,

                "expected_socket": "/sdb/losf-1/rpc.socket",
                "expected_ohash": "48e2e79fec9bc01d9a00e0a8fa68b289",
                "expected_partition": "456",
                "expected_extension": None,
                "expected_vol_dir": "/sdb/losf-1/volumes",
                "expected_vol_index": 12345,
            },
            # file of unknown size, policy 1
            {
                "datadir": "/sdb/objects-1/789/45d/"
                           "b2f5ff47436671b6e533d8dc3614845d",
                "obj_size": None,

                "volume_file": 301,
                "lock_file": 302,
                "volume_path": "/sdb/losf/volumes/v9999999",
                "volume_next_offset": 8 * 1024 * 1024 * 1024,

                "expected_socket": "/sdb/losf-1/rpc.socket",
                "expected_ohash": "b2f5ff47436671b6e533d8dc3614845d",
                "expected_partition": "789",
                "expected_extension": None,
                "expected_vol_dir": "/sdb/losf-1/volumes",
                "expected_vol_index": 9999999,
            },
            # empty file is ok (zero length)
            {
                "datadir": "/sdb/objects-1/789/45d/"
                           "b2f5ff47436671b6e533d8dc3614845d",
                "obj_size": 0,

                "volume_file": 301,
                "lock_file": 302,
                "volume_path": "/sdb/losf/volumes/v9999999",
                "volume_next_offset": 8 * 1024 * 1024 * 1024,

                "expected_socket": "/sdb/losf-1/rpc.socket",
                "expected_ohash": "b2f5ff47436671b6e533d8dc3614845d",
                "expected_partition": "789",
                "expected_extension": None,
                "expected_vol_dir": "/sdb/losf-1/volumes",
                "expected_vol_index": 9999999,
            },
        ]

        for t in test_sets:
            # When creating a new "vfile", expect the writer to seek to the
            # offset where we can start writing the file content.
            # The layout of a single vfile within a volume is :
            # |header | metadata(*) | file content | (more metadata, optional)|
            #
            # The first reserved metadata field size is defined in the
            # configuration by the "metadata_reserve" parameter.
            #
            # So the absolute location is:
            # next_offset for the volume + len(header) + len(metadata_reserve)
            expected_absolute_offset = (t["volume_next_offset"] +
                                        len(ObjectHeader()) +
                                        vfile_conf["metadata_reserve"])
            expected_relative_offset = (len(ObjectHeader()) +
                                        vfile_conf["metadata_reserve"])

            m_open_or_create_vol.return_value = (t["volume_file"],
                                                 t["lock_file"],
                                                 t["volume_path"])

            m_get_next_offset.return_value = t["volume_next_offset"]

            VFileWriter.create(t["datadir"], t["obj_size"], vfile_conf, None)
            ordered_args = m_open_or_create_vol.call_args[0]
            named_args = m_open_or_create_vol.call_args[1]
            self.assertEqual(ordered_args[0], t["expected_socket"])
            self.assertEqual(ordered_args[1], t["expected_partition"])
            self.assertEqual(ordered_args[2], t["expected_extension"])
            self.assertEqual(ordered_args[3], t["expected_vol_dir"])
            self.assertEqual(named_args["size"], t["obj_size"])

            m_get_next_offset.assert_called_once_with(t["expected_socket"],
                                                      t["expected_vol_index"])

            ordered_args = m_grow_vol.call_args[0]
            self.assertEqual(ordered_args[0], t["volume_file"])
            self.assertEqual(ordered_args[1], t["volume_next_offset"])
            self.assertEqual(ordered_args[2], t["obj_size"])

            m_lseek.assert_called_with(t["volume_file"],
                                       expected_absolute_offset, os.SEEK_SET)

            args = m_cls.call_args[0]
            self.assertEqual(args[1], t["datadir"])
            self.assertEqual(args[2], t["volume_file"])
            self.assertEqual(args[3], t["lock_file"])
            self.assertEqual(args[4], t["expected_vol_dir"])
            self.assertEqual(args[5], t["expected_vol_index"])
            header = args[6]
            self.assertEqual(header.ohash, t["expected_ohash"])
            self.assertEqual(header.data_offset, expected_relative_offset)
            # we have not written anything yet so data_size should be zero
            self.assertEqual(header.data_size, 0)
            # state should be STATE_OBJ_FILE (not quarantined)
            self.assertEqual(header.state, STATE_OBJ_FILE)

            for test_m in [m_lseek, m_grow_vol, m_get_next_offset,
                           m_open_or_create_vol]:
                test_m.reset_mock()

            with self.assertRaises(VOSError):
                VFileWriter.create("/foo", 123, vfile_conf, None)

            with self.assertRaises(VOSError):
                VFileWriter.create("/mnt/objects/"
                                   "b2f5ff47436671b6e533d8dc3614845d", 123,
                                   vfile_conf, None)

            with self.assertRaises(VOSError):
                VFileWriter.create("/mnt/objects/123/"
                                   "b2f5ff47436671b6e533d8dc3614845d", 123,
                                   vfile_conf, None)

            # negative size
            with self.assertRaises(VOSError):
                VFileWriter.create("/mnt/objects/123/abc/"
                                   "b2f5ff47436671b6e533d8dc3614845d", -1,
                                   vfile_conf, None)

    @mock.patch("swift.obj.vfile.open_or_create_volume")
    @mock.patch("swift.obj.rpc_http.get_next_offset")
    @mock.patch("swift.obj.vfile.os.close")
    def test_create_rpc_error(self, m_os_close, m_get_next_offset,
                              m_open_or_create_vol):
        vfile_conf = {
            'volume_alloc_chunk_size': 16 * 1024,
            'volume_low_free_space': 8 * 1024,
            'metadata_reserve': 500,
            'max_volume_count': 1000,
            'max_volume_size': 10 * 1024 * 1024 * 1024,
        }

        datadir = "/sda/objects/123/4d8/acbd18db4cc2f85cedef654fccc4a4d8"
        obj_size = 0
        volume_file = 101
        lock_file = 102
        volume_path = "/sda/losf/volumes/v0000001"

        m_open_or_create_vol.return_value = (volume_file, lock_file,
                                             volume_path)
        m_get_next_offset.side_effect = RpcError(StatusCode.Unavailable,
                                                 "Unavailable")

        try:
            VFileWriter.create(datadir, obj_size, vfile_conf, None)
        except RpcError:
            close_args = m_os_close.call_args_list
            expected_args = [mock.call(volume_file), mock.call(lock_file)]
            self.assertEqual(close_args, expected_args)

    def _get_vfile_writer(self, offset=0, metadata_reserve=500,
                          ohash="d41d8cd98f00b204e9800998ecf8427e"):
        """
        returns a fake VFileWriter, backed by a TemporaryFile
        :param offset: absolute offset in volume where the file starts
        :param metadata_reserve: space to reserve for metadata between header
        and file content. (if insufficient, the remaining serialized metadata
        will be written after the file content)
        :param ohash: object hash
        :return: a VFileWriter instance
        """
        volume_file = tempfile.TemporaryFile()
        lock_file = 102  # dummy fd
        volume_dir = "/sda/losf/volumes"
        volume_index = randint(1, 9999999)
        datadir = "/sda/objects/123/{}/{}".format(ohash[-3:], ohash)
        header = ObjectHeader(version=vfile.OBJECT_HEADER_VERSION)
        header.ohash = ohash
        header.policy_idx = 0
        header.data_offset = len(header) + metadata_reserve
        header.data_size = 0
        header.state = STATE_OBJ_FILE
        logger = None

        volume_fd = volume_file.fileno()
        # seek to where file data would start as expected by caller
        os.lseek(volume_fd, offset + header.data_offset, os.SEEK_SET)

        return (vfile.VFileWriter(datadir, volume_fd, lock_file, volume_dir,
                                  volume_index, header, offset, logger),
                volume_file)

    @mock.patch("swift.obj.vfile.fdatasync")
    @mock.patch("swift.obj.vfile.rpc.register_object")
    def test_commit(self, m_register, m_fdatasync):
        default_metadata = {"Content-Length": "92",
                            "name": "/AUTH_test/foo/truc",
                            "Content-Type": "application/octet-stream",
                            "ETag": "89408008f2585c957c031716600d5a80",
                            "X-Timestamp": "1560866451.10093",
                            "X-Object-Meta-Mtime": "1523272228.000000"}
        test_sets = [
            # empty file, volume offset 0
            {
                "offset": 0,
                "metadata_reserve": 500,
                "ohash": "d41d8cd98f00b204e9800998ecf8427e",
                "filename": "1560866451.10093.data",
                "metadata": default_metadata,
                "data_bytes": 0,  # file content length
            },
            # non-zero random file size with random offset
            {
                "offset": 4096 * randint(0, 2621440),  # up to 10GB
                "metadata_reserve": 500,
                "ohash": "d41d8cd98f00b204e9800998ecf8427e",
                "filename": "1560866451.10093.data",
                "metadata": default_metadata,
                "data_bytes": randint(1, 1024 * 1024)  # up to 1MB
            },
            {
                "offset": 4096 * randint(0, 2621440),  # up to 10GB
                "metadata_reserve": 10,
                "ohash": "d41d8cd98f00b204e9800998ecf8427e",
                "filename": "1560866451.10093.data",
                "metadata": default_metadata,
                "data_bytes": randint(1, 1024 * 1024)  # up to 1MB
            },
        ]

        for t in test_sets:
            vfile_writer, vol_file = self._get_vfile_writer(
                offset=t["offset"],
                metadata_reserve=t["metadata_reserve"],
            )
            if t["data_bytes"]:
                os.write(vfile_writer.fd, b"x" * t["data_bytes"])
            vfile_writer.commit(t["filename"], t["metadata"])

            # check header
            vol_file.seek(t["offset"])
            serialized_header = vol_file.read(MAX_OBJECT_HEADER_LEN)
            header = ObjectHeader.unpack(serialized_header)
            self.assertEqual(header.version, vfile.OBJECT_HEADER_VERSION)
            self.assertEqual(header.ohash, "d41d8cd98f00b204e9800998ecf8427e")
            self.assertEqual(header.filename, t["filename"])
            self.assertEqual(header.data_size, t["data_bytes"])
            self.assertEqual(header.state, STATE_OBJ_FILE)

            # check swift metadata
            vol_file.seek(t["offset"] + header.metadata_offset)
            # if metadata couldn't fit in the reserved space, we should find
            # the rest after the file content.
            if header.metadata_size <= t["metadata_reserve"]:
                serialized_metadata = vol_file.read(header.metadata_size)
            else:
                reserved_bytes = header.data_offset - header.metadata_offset
                serialized_metadata = vol_file.read(reserved_bytes)
                vol_file.seek(
                    t["offset"] + header.data_offset + header.data_size)
                serialized_metadata += vol_file.read(header.metadata_size -
                                                     t["metadata_reserve"])
            ondisk_meta = Metadata()
            ondisk_meta.ParseFromString(serialized_metadata)
            # Metadata() is a protobuf message instance, with a single field, a
            # repeated Attr. an Attr has a key and a value
            self.assertEqual(len(ondisk_meta.attrs), len(t["metadata"]))
            for attr in ondisk_meta.attrs:
                if six.PY2:
                    self.assertEqual(attr.value, t["metadata"][attr.key])
                else:
                    self.assertEqual(
                        attr.value.decode("utf8", "surrogateescape"),
                        t["metadata"][attr.key.decode("utf8",
                                                      "surrogateescape")])

            # check data has been flushed to disk
            m_fdatasync.assert_called_once_with(vol_file.fileno())

            # check call to register the file to the index server
            m_register.assert_called_once()
            socket_path, full_name, vol_idx, offset, end_offset = \
                m_register.call_args_list[0][0]

            expected_full_name = "{}{}".format(t["ohash"], t["filename"])
            self.assertEqual(socket_path, vfile_writer.socket_path)
            self.assertEqual(full_name, expected_full_name)
            self.assertEqual(vol_idx, vfile_writer.volume_index)
            # end offset should be the next 4k aligned offset
            volume_length = os.lseek(vol_file.fileno(), 0, os.SEEK_END)
            expected_end_offset = next_aligned_offset(volume_length, 4096)
            self.assertEqual(end_offset, expected_end_offset)

            m_fdatasync.reset_mock()
            m_register.reset_mock()

    def test_commit_bad_file(self):
        vfile_writer, _ = self._get_vfile_writer()
        vfile_writer.fd = -1
        self.assertRaises(vfile.VIOError, vfile_writer.commit, "foo", {})

    def test_commit_no_name(self):
        vfile_writer, _ = self._get_vfile_writer()
        self.assertRaises(vfile.VIOError, vfile_writer.commit, "", {})

    @mock.patch("swift.obj.rpc_http.register_object")
    def test_commit_register_fail(self, m_register_object):
        """
        Check that the header object is erased if commit() fails to register
        the object on the index server.
        """
        m_register_object.side_effect = RpcError("failed to register object",
                                                 StatusCode.Unavailable)
        offset = 4096
        metadata_reserve = 500
        vfile_writer, vol_file = self._get_vfile_writer(
            offset=offset, metadata_reserve=metadata_reserve)
        content = b"dummy data"
        os.write(vfile_writer.fd, content)

        filename = "dummy-filename"
        metadata = {"dummy": "metadata"}

        self.assertRaises(RpcError, vfile_writer.commit, filename, metadata)

        # check the header was erased
        vol_file.seek(offset)
        serialized_header = vol_file.read(MAX_OBJECT_HEADER_LEN)
        self.assertEqual(serialized_header, b"\x00" * MAX_OBJECT_HEADER_LEN)

        # check we did not write past the header by checking the file data
        data_offset = (offset +
                       len(ObjectHeader(
                           version=header.OBJECT_HEADER_VERSION)) +
                       metadata_reserve)
        vol_file.seek(data_offset)
        data = vol_file.read(len(content))
        self.assertEqual(data, content)

    @mock.patch("swift.obj.vfile.open", new_callable=mock.mock_open)
    @mock.patch("swift.obj.vfile.fcntl.flock")
    @mock.patch("swift.obj.vfile.get_next_volume_index")
    @mock.patch("swift.obj.vfile.os.open")
    def test__create_new_lock_file(self, m_os_open, m_get_next_v_idx,
                                   m_flock, m_open):
        volume_dir = "/sda/losf/volumes"
        logger = None
        m_get_next_v_idx.return_value = 1

        expected_vol_creation_lock = os.path.join(volume_dir,
                                                  "volume_creation.lock")

        m_creation_lock = mock.mock_open().return_value
        m_open.side_effect = [m_creation_lock]

        m_volume_lock = mock.Mock()
        m_os_open.return_value = m_volume_lock

        index, next_lock_path, lock_file = vfile._create_new_lock_file(
            volume_dir, logger)
        m_open.assert_called_once_with(expected_vol_creation_lock, "w")
        m_os_open.assert_called_once_with(
            "/sda/losf/volumes/v0000001.writelock",
            os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            0o600)

        # Check locking order
        calls = m_flock.call_args_list
        self.assertEqual(calls[0], mock.call(m_creation_lock, fcntl.LOCK_EX))
        self.assertEqual(calls[1], mock.call(m_volume_lock,
                                             fcntl.LOCK_EX | fcntl.LOCK_NB))

        self.assertEqual(index, 1)
        self.assertEqual(next_lock_path,
                         "/sda/losf/volumes/v0000001.writelock")
        self.assertEqual(lock_file, m_volume_lock)

    def test_get_next_volume_index(self):
        tests = [
            {
                "dir_entries": [],
                "expected_next_index": 1,
            },
            {
                "dir_entries": ["invalid-file"],
                "expected_next_index": 1,
            },
            {
                "dir_entries": [
                    "v0000001",
                    "v0000001.writelock",
                    "v0000002",
                    "v0000002.writelock",
                ],
                "expected_next_index": 3,
            },
            # the volume is gone. shouldn't reuse the index anyway
            {
                "dir_entries": [
                    "v0000001",
                    "v0000001.writelock",
                    "v0000002.writelock",
                ],
                "expected_next_index": 3,
            },
            {
                "dir_entries": [
                    "v0000002",
                    "v0000002.writelock",
                    "v0000003",
                    "v0000003.writelock",
                    "v0000005",
                    "v0000005.writelock",
                ],
                "expected_next_index": 1,
            },
            {
                "dir_entries": ["invalid.txt"],
                "expected_next_index": 1,
            },
            {
                "dir_entries": [
                    "v0000001",
                    "v0000001.writelock",
                    "invalid-name",
                    "v0000003",
                    "v0000003.writelock",
                ],
                "expected_next_index": 2,
            },
            # the lock file is gone. shouldn't reuse the index anyway
            {
                "dir_entries": [
                    "v0000001",
                ],
                "expected_next_index": 2,
            },
        ]

        for t in tests:
            tempdir = tempfile.mkdtemp()
            try:
                for filename in t["dir_entries"]:
                    filepath = os.path.join(tempdir, filename)
                    open(filepath, "w").close()
                result = vfile.get_next_volume_index(tempdir)
                self.assertEqual(result, t["expected_next_index"])
            finally:
                shutil.rmtree(tempdir)

    @mock.patch("swift.obj.vfile.open_writable_volume")
    @mock.patch("swift.obj.vfile.create_writable_volume")
    @mock.patch("swift.obj.vfile.os.makedirs")
    def test_open_or_create_volume_available(self, m_makedirs, m_create_vol,
                                             m_open_vol):
        socket_path = "/sda/losf/rpc.socket"
        partition = "123"
        extension = ".data"
        volume_dir = "/sda/losf/volumes"
        conf = {"dummy": "conf"}
        logger = None

        # open_writable_volume finds an available volume
        m_open_vol.return_value = 321, 322, "/path/to/vol"

        vol_file, lock_file, vol_path = vfile.open_or_create_volume(
            socket_path, partition, extension, volume_dir, conf, logger)

        m_open_vol.assert_called_once_with(socket_path, partition, extension,
                                           volume_dir, conf, logger)
        m_create_vol.assert_not_called()

        self.assertEqual((vol_file, lock_file, vol_path),
                         (321, 322, "/path/to/vol"))

    @mock.patch("swift.obj.vfile.open_writable_volume")
    @mock.patch("swift.obj.vfile.create_writable_volume")
    @mock.patch("swift.obj.vfile.os.makedirs")
    def test_open_or_create_volume_create(self, m_makedirs, m_create_vol,
                                          m_open_vol):
        socket_path = "/sda/losf/rpc.socket"
        partition = "123"
        extension = None
        volume_dir = "/sda/losf/volumes"
        conf = {}
        logger = None

        # open_writable_volume does not return a volume, but
        # create_writabl_volume does.
        m_open_vol.return_value = None, None, None
        m_create_vol.return_value = 543, 544, "/path/to/vol"

        vol_file, lock_file, vol_path = vfile.open_or_create_volume(
            socket_path, partition, extension, volume_dir, conf, logger)

        m_open_vol.assert_called_once_with(socket_path, partition, extension,
                                           volume_dir, conf, logger)
        self.assertEqual((vol_file, lock_file, vol_path),
                         (543, 544, "/path/to/vol"))

    @mock.patch("swift.obj.vfile.open_writable_volume")
    @mock.patch("swift.obj.vfile.create_writable_volume")
    @mock.patch("swift.obj.vfile.os.makedirs")
    def test_open_or_create_volume_fail(self, m_makedirs, m_create_vol,
                                        m_open_vol):
        socket_path = "/sda/losf/rpc.socket"
        partition = "123"
        extension = None
        volume_dir = "/sda/losf/volumes"
        conf = {}
        logger = None

        # Cannot find an existing volume, then creating a new one fails
        # because we have exceeded the volume count.
        m_open_vol.return_value = None, None, None
        m_create_vol.side_effect = OSError(errno.EDQUOT, "max vol count")

        try:
            vfile.open_or_create_volume(socket_path, partition, extension,
                                        volume_dir, conf, logger)
        except vfile.VOSError as exc:
            self.assertEqual(str(exc), "[Errno 28] Failed to open or create"
                                       " a volume for writing: max vol count")
        m_open_vol.assert_called_once()
        m_create_vol.assert_called_once()

        # Similar failure but with an exception that has no strerror
        m_open_vol.reset_mock()
        m_create_vol.reset_mock()
        m_create_vol.side_effect = Exception("Dummy exception")
        try:
            vfile.open_or_create_volume(socket_path, partition, extension,
                                        volume_dir, conf, logger)
        except vfile.VOSError as exc:
            self.assertEqual(str(exc), "[Errno 28] Failed to open or create"
                                       " a volume for writing: Unknown error")
        m_open_vol.assert_called_once()
        m_create_vol.assert_called_once()

    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    def test_create_writable_volume_max_count_exceeded(self, m_list_volumes):
        socket_path = "/sda/losf/rpc.socket"
        partition = "123"
        extension = None
        volume_dir = "/sda/losf/volumes"
        conf = {"max_volume_count": 1}
        logger = None

        m_list_volumes.return_value = ["v1"]
        try:
            vfile.create_writable_volume(socket_path, partition, extension,
                                         volume_dir, conf, logger)
        except vfile.VOSError as exc:
            self.assertEqual(str(exc),
                             "[Errno 122] Maximum count of volumes reached for"
                             " partition: 123 type: 0")

    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    @mock.patch("swift.obj.vfile.os.makedirs")
    def test_create_writable_volume_makedirs_exceptions(self, m_os_makedirs,
                                                        m_list_volumes):
        socket_path = "/sda/losf/rpc.socket"
        partition = "123"
        extension = None
        volume_dir = "/sda/losf/volumes"
        conf = {"max_volume_count": 1}
        logger = None

        m_list_volumes.return_value = ["v1"]
        m_os_makedirs.side_effect = OSError(errno.ENOSPC, "No space")

        self.assertRaises(OSError, vfile.create_writable_volume, socket_path,
                          partition, extension, volume_dir, conf, logger)

        m_os_makedirs.side_effect = vfile.VFileException("test error")
        self.assertRaises(VOSError, vfile.create_writable_volume, socket_path,
                          partition, extension, volume_dir, conf, logger)

    @mock.patch("swift.obj.vfile.get_next_volume_index")
    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    @mock.patch("swift.obj.vfile.os.makedirs")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    @mock.patch("swift.obj.vfile._allocate_volume_space")
    @mock.patch("swift.obj.vfile.fsync")
    @mock.patch("swift.obj.vfile.fsync_dir")
    @mock.patch("swift.obj.vfile.rpc.register_volume")
    def test_create_writable_volume(self, m_register_volume,
                                    m_fsync_dir, m_fsync,
                                    m_allocate_volume_space,
                                    m_flock, m_os_makedirs,
                                    m_list_volumes,
                                    m_get_next_volume_index):
        socket_path = "/sda/losf/rpc.socket"
        partition = "123"
        extension = None
        conf = {"max_volume_count": 1, "volume_alloc_chunk_size": 50}
        logger = None

        m_list_volumes.return_value = []
        next_vol_idx = 1
        m_get_next_volume_index.return_value = next_vol_idx

        tempdir = tempfile.mkdtemp()
        volume_dir = tempdir

        try:
            vfile.create_writable_volume(socket_path, partition, extension,
                                         volume_dir, conf, logger)

            # are the expected files here?
            expected = ["v0000001", "v0000001.writelock",
                        "volume_creation.lock"]
            files = os.listdir(tempdir)
            files.sort()
            self.assertEqual(files, expected)

            # have the locks been taken?
            # TODO: how to properly assert both locks? (one for the global
            # volume creation lock, another for the volume itself)
            self.assertEqual(m_flock.call_count, 2)

            # check the volume header is correct
            with open(os.path.join(tempdir, "v0000001"), 'rb') as vol:
                vol_header = header.read_volume_header(vol)
                self.assertEqual(vol_header.volume_idx, next_vol_idx)
                self.assertEqual(vol_header.partition, int(partition))
                self.assertEqual(vol_header.first_obj_offset, 4096)
                self.assertEqual(vol_header.type,
                                 fmgr_pb2.VOLUME_DEFAULT)  # extension was None
                self.assertEqual(vol_header.state, fmgr_pb2.STATE_RW)

            # check volume registration to the index server
            m_register_volume.assert_called_once_with(socket_path,
                                                      str(partition),
                                                      fmgr_pb2.VOLUME_DEFAULT,
                                                      next_vol_idx, 4096,
                                                      fmgr_pb2.STATE_RW)
        finally:
            shutil.rmtree(tempdir)
            m_flock.reset_mock()
            m_register_volume.reset_mock()

        partition = "456"
        extension = ".ts"
        conf = {"max_volume_count": 101, "volume_alloc_chunk_size": 50}
        logger = None
        m_list_volumes.return_value = ["x"] * 100
        next_vol_idx = 101
        m_get_next_volume_index.return_value = next_vol_idx

        tempdir = tempfile.mkdtemp()
        volume_dir = tempdir
        try:
            vfile.create_writable_volume(socket_path, partition, extension,
                                         volume_dir, conf, logger)

            # are the expected files here?
            expected = ["v0000101", "v0000101.writelock",
                        "volume_creation.lock"]
            files = os.listdir(tempdir)
            files.sort()
            self.assertEqual(files, expected)

            # have the locks been taken?
            # TODO: how to properly assert both locks? (one for the global
            # volume creation lock, another for the volume itself)
            self.assertEqual(m_flock.call_count, 2)

            # check the volume header is correct
            with open(os.path.join(tempdir, "v0000101"), 'rb') as vol:
                vol_header = header.read_volume_header(vol)
                self.assertEqual(vol_header.volume_idx, next_vol_idx)
                self.assertEqual(vol_header.partition, int(partition))
                self.assertEqual(vol_header.first_obj_offset, 4096)
                self.assertEqual(vol_header.type, fmgr_pb2.VOLUME_TOMBSTONE)
                self.assertEqual(vol_header.state, fmgr_pb2.STATE_RW)

            # check volume registration to the index server
            m_register_volume.assert_called_once_with(
                socket_path, str(partition), fmgr_pb2.VOLUME_TOMBSTONE,
                next_vol_idx, 4096, fmgr_pb2.STATE_RW)
        finally:
            shutil.rmtree(tempdir)
            m_flock.reset_mock()
            m_register_volume.reset_mock()

    @mock.patch("swift.obj.vfile.get_next_volume_index")
    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    @mock.patch("swift.obj.vfile.os.makedirs")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    @mock.patch("swift.obj.vfile._allocate_volume_space")
    @mock.patch("swift.obj.vfile.fsync")
    @mock.patch("swift.obj.vfile.fsync_dir")
    @mock.patch("swift.obj.vfile.rpc.register_volume")
    @mock.patch("swift.obj.vfile.os.close")
    def test_create_writable_volume_flock_error(self, m_os_close,
                                                m_register_volume,
                                                m_fsync_dir, m_fsync,
                                                m_allocate_volume_space,
                                                m_flock, m_os_makedirs,
                                                m_list_volumes,
                                                m_get_next_volume_index):
        socket_path = "/sda/losf/rpc.socket"
        partition = 123
        extension = None
        conf = {"max_volume_count": 1, "volume_alloc_chunk_size": 50}
        logger = None

        m_list_volumes.return_value = []
        next_vol_idx = 1
        m_get_next_volume_index.return_value = next_vol_idx

        tempdir = tempfile.mkdtemp()
        volume_dir = tempdir

        m_flock.side_effect = [True, IOError(errno.EACCES, "cannot lock")]

        with self.assertRaises(IOError):
            vfile.create_writable_volume(socket_path, partition, extension,
                                         volume_dir, conf, logger)

        try:
            # how to assert close() arguments ?
            self.assertEqual(m_os_close.call_count, 1)
            # check that the volume and its lock file have been removed
            self.assertEqual(os.listdir(tempdir), ['volume_creation.lock'])
            m_register_volume.assert_not_called()
        finally:
            shutil.rmtree(tempdir)

    @mock.patch("swift.obj.vfile.get_next_volume_index")
    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    @mock.patch("swift.obj.vfile.os.makedirs")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    @mock.patch("swift.obj.vfile._allocate_volume_space")
    @mock.patch("swift.obj.vfile.fsync")
    @mock.patch("swift.obj.vfile.fsync_dir")
    @mock.patch("swift.obj.vfile.rpc.register_volume")
    @mock.patch("swift.obj.vfile.os.close")
    def test_create_writable_volume_rpc_error(self, m_os_close,
                                              m_register_volume,
                                              m_fsync_dir, m_fsync,
                                              m_allocate_volume_space,
                                              m_flock, m_os_makedirs,
                                              m_list_volumes,
                                              m_get_next_volume_index):
        socket_path = "/sda/losf/rpc.socket"
        partition = 123
        extension = None
        conf = {"max_volume_count": 1, "volume_alloc_chunk_size": 50}
        logger = None

        m_list_volumes.return_value = []
        next_vol_idx = 1
        m_get_next_volume_index.return_value = next_vol_idx

        tempdir = tempfile.mkdtemp()
        volume_dir = tempdir

        m_register_volume.side_effect = RpcError(StatusCode.InvalidArgument,
                                                 "volume exists")

        try:
            vfile.create_writable_volume(socket_path, partition, extension,
                                         volume_dir, conf, logger)
        except RpcError:
            pass

        try:
            # how to assert close() arguments ?
            self.assertEqual(m_os_close.call_count, 2)
            # check that the volume and its lock file have been removed
            self.assertEqual(os.listdir(tempdir), ['volume_creation.lock'])
        finally:
            shutil.rmtree(tempdir)

    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    def test_open_writable_volume_no_volume(self, m_list_volumes):
        socket_path = "/path/to/rpc.socket"
        volume_dir = "/path/to/volumes"
        partition = 123,
        extension = None
        conf = {}
        logger = None

        rpc_reply = fmgr_pb2.ListVolumesReply()
        volume = fmgr_pb2.Volume(volume_index=1,
                                 volume_type=fmgr_pb2.VOLUME_DEFAULT,
                                 volume_state=fmgr_pb2.STATE_COMPACTION_TARGET,
                                 partition=123)
        rpc_reply.volumes.append(volume)

        m_list_volumes.return_value = rpc_reply.volumes

        m_list_volumes.return_value = []

        vol_file, lock_file, volume_file_path = vfile.open_writable_volume(
            socket_path, partition, extension, volume_dir, conf, logger)
        self.assertEqual((vol_file, lock_file, volume_file_path),
                         (None, None, None))

    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    def test_open_writable_volume_no_rw_volume(self, m_list_volumes):
        socket_path = "/path/to/rpc.socket"
        volume_dir = "/path/to/volumes"
        partition = 123,
        extension = None
        conf = {}
        logger = None

        m_list_volumes.return_value = []

        vol_file, lock_file, volume_file_path = vfile.open_writable_volume(
            socket_path, partition, extension, volume_dir, conf, logger)
        self.assertEqual((vol_file, lock_file, volume_file_path),
                         (None, None, None))

    @mock.patch("swift.obj.vfile.open_volume")
    @mock.patch("swift.obj.vfile.rpc.list_volumes")
    def test_open_writable_volume(self, m_list_volumes, m_open_volume):
        socket_path = "/path/to/rpc.socket"
        volume_dir = "/path/to/volumes"
        conf = {"max_volume_size": 100 * 1024 * 1024}
        logger = None

        test_sets = [
            {
                # partition and extension have no impact on the test as we
                # mock the RPC call that returns a list of volumes based on
                # partition, and extension.
                "partition": 123,
                "extension": None,
                "volumes": [
                    {
                        "index": 1,
                        "partition": 123,
                        "type": fmgr_pb2.VOLUME_DEFAULT,
                        "state": fmgr_pb2.STATE_RW,
                    },
                ],
                "expected_vol_path": "/path/to/volumes/v0000001"

            },
            {
                "partition": 123,
                "extension": None,
                "volumes": [
                    {
                        "index": 1,
                        "partition": 123,
                        "type": fmgr_pb2.VOLUME_DEFAULT,
                        "state": fmgr_pb2.STATE_COMPACTION_SRC,
                    },
                    {
                        "index": 2,
                        "partition": 123,
                        "type": fmgr_pb2.VOLUME_DEFAULT,
                        "state": fmgr_pb2.STATE_RW,
                    }
                ],
                "expected_vol_path": "/path/to/volumes/v0000002"

            },
            {
                "partition": 123,
                "extension": None,
                "volumes": [
                    {
                        "index": 1,
                        "partition": 123,
                        "type": fmgr_pb2.VOLUME_DEFAULT,
                        "state": fmgr_pb2.STATE_COMPACTION_SRC,
                    },
                    {
                        "index": 2,
                        "partition": 123,
                        "type": fmgr_pb2.VOLUME_DEFAULT,
                        "state": fmgr_pb2.STATE_COMPACTION_TARGET,
                    },
                    {
                        "index": 999,
                        "partition": 123,
                        "type": fmgr_pb2.VOLUME_DEFAULT,
                        "state": fmgr_pb2.STATE_RW,
                    },
                    {
                        "index": 1234,
                        "partition": 123,
                        "type": fmgr_pb2.VOLUME_DEFAULT,
                        "state": fmgr_pb2.STATE_COMPACTION_SRC,
                    },
                ],
                "expected_vol_path": "/path/to/volumes/v0000999"

            },
        ]

        for t in test_sets:
            # build the RPC reply
            rpc_reply = fmgr_pb2.ListVolumesReply()
            for vol in t["volumes"]:
                volume = fmgr_pb2.Volume(volume_index=vol["index"],
                                         volume_type=vol["type"],
                                         volume_state=vol["state"],
                                         partition=vol["partition"])
                rpc_reply.volumes.append(volume)

            m_list_volumes.return_value = rpc_reply.volumes

            # files that would be returned by open_volume
            m_vol_file = mock.Mock()
            m_lock_file = mock.Mock()
            m_open_volume.return_value = (m_vol_file, m_lock_file)

            vol_file, lock_file, volume_file_path = vfile.open_writable_volume(
                socket_path, t["partition"], t["extension"], volume_dir, conf,
                logger)
            m_open_volume.assert_called_once_with(t["expected_vol_path"])
            self.assertEqual((vol_file, lock_file, volume_file_path),
                             (m_vol_file, m_lock_file, t["expected_vol_path"]))
            m_open_volume.reset_mock()

    @mock.patch("swift.obj.vfile.os.open")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    def test_open_volume(self, m_flock, m_os_open):
        volume_path = "/path/to/volumes/v0000001"
        expected_lock_path = "/path/to/volumes/v0000001.writelock"

        m_lock_file = mock.Mock()
        m_volume_file = mock.Mock()
        m_os_open.side_effect = [m_lock_file, m_volume_file]
        vol_fd, lock_fd = vfile.open_volume(volume_path)
        # assert the volume lock file has been opened and locked
        args = m_os_open.call_args_list[0]
        self.assertEqual(args, mock.call(expected_lock_path, os.O_WRONLY))
        m_flock.assert_called_once_with(m_lock_file, fcntl.LOCK_EX |
                                        fcntl.LOCK_NB)
        # expect the second call to open to be for the volume itself
        args = m_os_open.call_args_list[1]
        self.assertEqual(args, mock.call(volume_path, os.O_WRONLY))

        self.assertEqual((vol_fd, lock_fd), (m_volume_file, m_lock_file))

    @mock.patch("swift.obj.vfile.os.open")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    @mock.patch("swift.obj.vfile.os.close")
    def test_open_volume_missing_lock_file(self, m_os_close, m_flock,
                                           m_os_open):
        """If the lock file is missing, it should be created"""
        def fake_os_open(path, flags, mode=None):
            if path == "/path/to/volumes/v0000001":
                return 123
            if flags != (os.O_CREAT | os.O_EXCL | os.O_WRONLY):
                raise OSError(errno.ENOENT,
                              "No such file or directory: {}".format(path))
            else:
                return 456

        volume_path = "/path/to/volumes/v0000001"
        expected_lock_path = "/path/to/volumes/v0000001.writelock"

        m_os_open.side_effect = fake_os_open
        vfile.open_volume(volume_path)
        second_open_args = m_os_open.call_args_list[1]
        self.assertEqual(
            second_open_args,
            mock.call(expected_lock_path,
                      os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        )

    @mock.patch("swift.obj.vfile.os.open")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    @mock.patch("swift.obj.vfile.os.close")
    def test_open_volume_cannot_open_lock_file(self, m_os_close, m_flock,
                                               m_os_open):
        """The lock file cannot be opened"""
        def fake_os_open(path, flags, mode=None):
            if path == "/path/to/volumes/v0000001":
                return 123
            raise OSError(errno.EPERM, "Permission denied")

        volume_path = "/path/to/volumes/v0000001"

        m_os_open.side_effect = fake_os_open
        self.assertRaises(OSError, vfile.open_volume, volume_path)

    @mock.patch("swift.obj.vfile.os.open")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    def test_open_volume_missing_volume(self, m_flock, m_os_open):
        volume_path = "/path/to/volumes/v0000001"
        expected_lock_path = "/path/to/volumes/v0000001.writelock"

        m_lock_file = mock.Mock()
        m_os_open.side_effect = [m_lock_file,
                                 OSError(2, "No such file or directory")]
        self.assertRaises(OSError, vfile.open_volume, volume_path)
        args_lock = m_os_open.call_args_list[0]
        self.assertEqual(args_lock, mock.call(expected_lock_path, os.O_WRONLY))
        m_flock.assert_called_once_with(m_lock_file, fcntl.LOCK_EX |
                                        fcntl.LOCK_NB)
        args_volume = m_os_open.call_args_list[1]
        self.assertEqual(args_volume, mock.call(volume_path, os.O_WRONLY))

    @mock.patch("swift.obj.vfile.os.open")
    @mock.patch("swift.obj.vfile.os.close")
    @mock.patch("swift.obj.vfile.fcntl.flock")
    def test_open_volume_cannot_lock(self, m_flock, m_os_close, m_os_open):
        volume_path = "/path/to/volumes/v0000001"
        expected_lock_path = "/path/to/volumes/v0000001.writelock"

        # Test we get (None, None) and not an exception if we get an IOError
        # with EACCES when attempting to get a lock
        m_lock_file = mock.Mock()
        m_os_open.return_value = m_lock_file
        m_flock.side_effect = IOError(errno.EACCES, "cannot lock")
        vol_fd, lock_fd = vfile.open_volume(volume_path)
        self.assertEqual((vol_fd, lock_fd), (None, None))
        m_os_open.assert_called_once_with(expected_lock_path, os.O_WRONLY)
        m_flock.assert_called_once_with(m_lock_file, fcntl.LOCK_EX |
                                        fcntl.LOCK_NB)
        m_os_close.assert_called_once_with(m_lock_file)

        # Same test with EAGAIN
        m_os_open.reset_mock()
        m_flock.reset_mock()
        m_os_close.reset_mock()
        m_flock.side_effect = IOError(errno.EAGAIN, "cannot lock")
        vol_fd, lock_fd = vfile.open_volume(volume_path)
        self.assertEqual((vol_fd, lock_fd), (None, None))
        m_os_open.assert_called_once_with(expected_lock_path, os.O_WRONLY)
        m_flock.assert_called_once_with(m_lock_file, fcntl.LOCK_EX |
                                        fcntl.LOCK_NB)
        m_os_close.assert_called_once_with(m_lock_file)

        # Same test with EBADF, this should raise
        m_os_open.reset_mock()
        m_flock.reset_mock()
        m_os_close.reset_mock()
        m_flock.side_effect = IOError(errno.EBADF, "cannot lock")
        self.assertRaises(IOError, vfile.open_volume, volume_path)
        m_os_open.assert_called_once_with(expected_lock_path, os.O_WRONLY)
        m_flock.assert_called_once_with(m_lock_file, fcntl.LOCK_EX |
                                        fcntl.LOCK_NB)
        m_os_close.assert_called_once_with(m_lock_file)

    def test_get_lock_file_name(self):
        name = vfile.get_lock_file_name(1)
        self.assertEqual(name, "v0000001.writelock")

        name = vfile.get_lock_file_name(9999999)
        self.assertEqual(name, "v9999999.writelock")

        self.assertRaises(vfile.VFileException, vfile.get_lock_file_name, 0)
        self.assertRaises(vfile.VFileException, vfile.get_lock_file_name,
                          10000000)

    def test_get_volume_name(self):
        name = vfile.get_volume_name(1)
        self.assertEqual(name, "v0000001")

        name = vfile.get_volume_name(9999999)
        self.assertEqual(name, "v9999999")

        self.assertRaises(vfile.VFileException, vfile.get_volume_name, 0)
        self.assertRaises(vfile.VFileException, vfile.get_volume_name,
                          10000000)


if __name__ == '__main__':
    unittest.main()
