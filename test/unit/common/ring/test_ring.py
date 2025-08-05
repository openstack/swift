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

import array
import collections
from gzip import GzipFile
import hashlib
import json
import os
import unittest
import stat
import struct
from tempfile import mkdtemp
from shutil import rmtree
from time import sleep, time
import sys
import copy
from unittest import mock
import zlib

from swift.common.exceptions import DevIdBytesTooSmall
from swift.common import ring, utils
from swift.common.ring import io, utils as ring_utils


class TestRingBase(unittest.TestCase):
    longMessage = True

    def setUp(self):
        self._orig_hash_suffix = utils.HASH_PATH_SUFFIX
        self._orig_hash_prefix = utils.HASH_PATH_PREFIX
        utils.HASH_PATH_SUFFIX = b'endcap'
        utils.HASH_PATH_PREFIX = b''

    def tearDown(self):
        utils.HASH_PATH_SUFFIX = self._orig_hash_suffix
        utils.HASH_PATH_PREFIX = self._orig_hash_prefix


class TestRingData(unittest.TestCase):

    def setUp(self):
        self.testdir = mkdtemp()

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def assert_ring_data_equal(self, rd_expected, rd_got, metadata_only=False):
        self.assertEqual(rd_expected.devs, rd_got.devs)
        self.assertEqual(rd_expected._part_shift, rd_got._part_shift)
        self.assertEqual(rd_expected.next_part_power, rd_got.next_part_power)
        self.assertEqual(rd_expected.version, rd_got.version)
        self.assertEqual(rd_expected.dev_id_bytes, rd_got.dev_id_bytes)
        self.assertEqual(rd_expected.replica_count, rd_got.replica_count)

        if metadata_only:
            self.assertEqual([], rd_got._replica2part2dev_id)
        else:
            self.assertEqual(rd_expected._replica2part2dev_id,
                             rd_got._replica2part2dev_id)

    def test_attrs(self):
        r2p2d = [[0, 1, 0, 1], [0, 1, 0, 1]]
        d = [{'id': 0, 'zone': 0, 'region': 0, 'ip': '10.1.1.0', 'port': 7000,
              'replication_ip': '10.1.1.0', 'replication_port': 7000},
             {'id': 1, 'zone': 1, 'region': 1, 'ip': '10.1.1.1', 'port': 7000,
              'replication_ip': '10.1.1.1', 'replication_port': 7000}]
        s = 30
        rd = ring.RingData(r2p2d, d, s)
        self.assertEqual(rd._replica2part2dev_id, r2p2d)
        self.assertEqual(rd.devs, d)
        self.assertEqual(rd._part_shift, s)

    def test_roundtrip_serialization(self):
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])],
            [
                {'id': 0, 'region': 1, 'zone': 0},
                {'id': 1, 'region': 1, 'zone': 1},
            ],
            30)
        rd.save(ring_fname)

        meta_only = ring.RingData.load(ring_fname, metadata_only=True)
        self.assert_ring_data_equal(rd, meta_only, metadata_only=True)

        rd2 = ring.RingData.load(ring_fname)
        self.assert_ring_data_equal(rd, rd2)

    def test_load_closes_file(self):
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])],
            [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        rd.save(ring_fname)

        with mock.patch('swift.common.ring.io.open',
                        return_value=open(ring_fname, 'rb')) as mock_open:
            self.assertFalse(mock_open.return_value.closed)  # sanity
            ring.RingData.load(ring_fname)
            self.assertTrue(mock_open.return_value.closed)

    def test_byteswapped_serialization(self):
        # Manually byte swap a ring and write it out, claiming it was written
        # on a different endian machine. Then read it back in and see if it's
        # the same as the non-byte swapped original.

        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        data = [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])]
        swapped_data = copy.deepcopy(data)
        for x in swapped_data:
            x.byteswap()

        with mock.patch.object(sys, 'byteorder',
                               'big' if sys.byteorder == 'little'
                               else 'little'):
            rds = ring.RingData(swapped_data,
                                [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}],
                                30)
            # note that this can only be an issue for v1 rings;
            # v2 rings always write network order
            rds.save(ring_fname, format_version=1)

        rd1 = ring.RingData(data, [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}],
                            30)
        rd2 = ring.RingData.load(ring_fname)
        self.assert_ring_data_equal(rd1, rd2)

    def test_deterministic_serialization(self):
        """
        Two identical rings should produce identical .gz files on disk.
        """
        os.mkdir(os.path.join(self.testdir, '1'))
        os.mkdir(os.path.join(self.testdir, '2'))
        # These have to have the same filename (not full path,
        # obviously) since the filename gets encoded in the gzip data.
        ring_fname1 = os.path.join(self.testdir, '1', 'the.ring.gz')
        ring_fname2 = os.path.join(self.testdir, '2', 'the.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])],
            [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        rd.save(ring_fname1)
        rd.save(ring_fname2)
        with open(ring_fname1, 'rb') as ring1:
            with open(ring_fname2, 'rb') as ring2:
                self.assertEqual(ring1.read(), ring2.read())

    def test_permissions(self):
        ring_fname = os.path.join(self.testdir, 'stat.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])],
            [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        rd.save(ring_fname)
        ring_mode = stat.S_IMODE(os.stat(ring_fname).st_mode)
        expected_mode = (stat.S_IRUSR | stat.S_IWUSR |
                         stat.S_IRGRP | stat.S_IROTH)
        self.assertEqual(
            ring_mode, expected_mode,
            'Ring has mode 0%o, expected 0%o' % (ring_mode, expected_mode))

    def test_replica_count(self):
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'zone': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'ip': '10.1.1.1', 'port': 7000}],
            30)
        self.assertEqual(rd.replica_count, 2)

        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0]],
            [{'id': 0, 'zone': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'ip': '10.1.1.1', 'port': 7000}],
            30)
        self.assertEqual(rd.replica_count, 1.75)

    def test_deserialize_v1(self):
        # First save it as a ring v2 and then try and load it using
        # deserialize_v1
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'region': 1, 'zone': 0, 'ip': '10.1.1.0',
              'port': 7000},
             {'id': 1, 'region': 1, 'zone': 1, 'ip': '10.1.1.1',
              'port': 7000}],
            30)
        rd.save(ring_fname, format_version=2)

        with self.assertRaises(ValueError) as err:
            ring.RingData.deserialize_v1(io.RingReader(open(ring_fname, 'rb')))
        self.assertIn("unexpected magic:", str(err.exception))

        # Now let's save it as v1 then load it up metadata_only
        rd.save(ring_fname, format_version=1)
        loaded_rd = ring.RingData.deserialize_v1(
            io.RingReader(open(ring_fname, 'rb')),
            metadata_only=True)
        self.assertTrue(loaded_rd['byteorder'])
        expected_devs = [
            {'id': 0, 'ip': '10.1.1.0', 'port': 7000, 'region': 1, 'zone': 0,
             'replication_ip': '10.1.1.0', 'replication_port': 7000},
            {'id': 1, 'ip': '10.1.1.1', 'port': 7000, 'region': 1, 'zone': 1,
             'replication_ip': '10.1.1.1', 'replication_port': 7000}]
        self.assertEqual(loaded_rd['devs'], expected_devs)
        self.assertEqual(loaded_rd['part_shift'], 30)
        self.assertEqual(loaded_rd['replica_count'], 2)

        # but the replica2part2dev table is empty
        self.assertFalse(loaded_rd['replica2part2dev_id'])

        # But if we load it up with metadata_only = false
        loaded_rd = ring.RingData.deserialize_v1(
            io.RingReader(open(ring_fname, 'rb')))
        self.assertTrue(loaded_rd['byteorder'])
        self.assertEqual(loaded_rd['devs'], expected_devs)
        self.assertEqual(loaded_rd['part_shift'], 30)
        self.assertEqual(loaded_rd['replica_count'], 2)
        self.assertTrue(loaded_rd['replica2part2dev_id'])

    def test_deserialize_v2(self):
        # First save it as a ring v1 and then try and load it using
        # deserialize_v2
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'region': 1, 'zone': 0, 'ip': '10.1.1.0',
              'port': 7000},
             {'id': 1, 'region': 1, 'zone': 1, 'ip': '10.1.1.1',
              'port': 7000}],
            30)
        rd.save(ring_fname, format_version=1)
        with self.assertRaises(ValueError) as err:
            ring.RingData.deserialize_v2(io.RingReader(open(ring_fname, 'rb')))
        self.assertEqual("No index loaded", str(err.exception))

        # Now let's save it as v2 then load it up metadata_only
        rd.save(ring_fname, format_version=2)
        loaded_rd = ring.RingData.deserialize_v2(
            io.RingReader(open(ring_fname, 'rb')),
            metadata_only=True,
            include_devices=False)
        self.assertEqual(loaded_rd['part_shift'], 30)
        self.assertEqual(loaded_rd['replica_count'], 2)
        # minimum size we use is 2 byte dev ids
        self.assertEqual(loaded_rd['dev_id_bytes'], 2)

        # but the replica2part2dev table and devs are both empty
        self.assertFalse(loaded_rd['devs'])
        self.assertFalse(loaded_rd['replica2part2dev_id'])

        # Next we load it up with metadata and devs only
        loaded_rd = ring.RingData.deserialize_v2(
            io.RingReader(open(ring_fname, 'rb')),
            metadata_only=True)
        expected_devs = [
            {'id': 0, 'ip': '10.1.1.0', 'port': 7000, 'region': 1, 'zone': 0,
             'replication_ip': '10.1.1.0', 'replication_port': 7000},
            {'id': 1, 'ip': '10.1.1.1', 'port': 7000, 'region': 1, 'zone': 1,
             'replication_ip': '10.1.1.1', 'replication_port': 7000}]
        self.assertEqual(loaded_rd['devs'], expected_devs)
        self.assertEqual(loaded_rd['part_shift'], 30)
        self.assertEqual(loaded_rd['replica_count'], 2)
        self.assertEqual(loaded_rd['dev_id_bytes'], 2)
        self.assertFalse(loaded_rd['replica2part2dev_id'])

        # But if we load it up with metadata_only = false
        loaded_rd = ring.RingData.deserialize_v2(
            io.RingReader(open(ring_fname, 'rb')))
        self.assertEqual(loaded_rd['devs'], expected_devs)
        self.assertEqual(loaded_rd['part_shift'], 30)
        self.assertEqual(loaded_rd['replica_count'], 2)
        self.assertEqual(loaded_rd['dev_id_bytes'], 2)
        self.assertTrue(loaded_rd['replica2part2dev_id'])

        # Can also load up assignments but not devs; idk why you'd want that
        loaded_rd = ring.RingData.deserialize_v2(
            io.RingReader(open(ring_fname, 'rb')),
            metadata_only=False,
            include_devices=False)
        self.assertFalse(loaded_rd['devs'])
        self.assertEqual(loaded_rd['part_shift'], 30)
        self.assertEqual(loaded_rd['replica_count'], 2)
        self.assertEqual(loaded_rd['dev_id_bytes'], 2)
        self.assertTrue(loaded_rd['replica2part2dev_id'])

    def test_load(self):
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'region': 1, 'zone': 0, 'ip': '10.1.1.0',
              'port': 7000},
             {'id': 1, 'region': 1, 'zone': 1, 'ip': '10.1.1.1',
              'port': 7000}],
            30)
        ring_fname_1 = os.path.join(self.testdir, 'foo-1.ring.gz')
        ring_fname_2 = os.path.join(self.testdir, 'foo-2.ring.gz')
        ring_fname_bad_version = os.path.join(self.testdir, 'foo-bar.ring.gz')
        rd.save(ring_fname_1, format_version=1)
        rd.save(ring_fname_2, format_version=2)
        with io.RingWriter.open(ring_fname_bad_version) as writer:
            writer.write_magic(5)
            with writer.section('foo'):
                writer.write_blob(b'\xde\xad\xbe\xef' * 10240)

        # Loading the bad ring will fail because it's an unknown version
        with self.assertRaises(Exception) as ex:
            ring.RingData.load(ring_fname_bad_version)
        self.assertEqual(
            f'Unsupported ring version: 5 for {ring_fname_bad_version!r}',
            str(ex.exception))

        orig_load_index = io.RingReader.load_index

        def mock_load_index(cls):
            cls.version = 5
            orig_load_index(cls)

        with mock.patch('swift.common.ring.io.RingReader.load_index',
                        mock_load_index):
            with self.assertRaises(Exception) as ex:
                ring.RingData.load(ring_fname_1)
        self.assertEqual(
            f'Unknown ring format version 5 for {ring_fname_1!r}',
            str(ex.exception))

        expected_r2p2d = [
            array.array('H', [0, 1, 0, 1]),
            array.array('H', [0, 1, 0, 1])]
        expected_rd_dict = {
            'devs': [
                {'id': 0, 'region': 1, 'zone': 0,
                 'ip': '10.1.1.0', 'port': 7000,
                 'replication_ip': '10.1.1.0', 'replication_port': 7000},
                {'id': 1, 'zone': 1, 'region': 1,
                 'ip': '10.1.1.1', 'port': 7000,
                 'replication_ip': '10.1.1.1', 'replication_port': 7000}],
            'replica2part2dev_id': expected_r2p2d,
            'part_shift': 30,
            'next_part_power': None,
            'dev_id_bytes': 2,
            'version': None}

        # version 2
        loaded_rd = ring.RingData.load(ring_fname_2)
        self.assertEqual(loaded_rd.to_dict(), expected_rd_dict)

        # version 1
        loaded_rd = ring.RingData.load(ring_fname_1)
        self.assertEqual(loaded_rd.to_dict(), expected_rd_dict)

    def test_load_metadata_only(self):
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'region': 1, 'zone': 0, 'ip': '10.1.1.0',
              'port': 7000},
             {'id': 1, 'region': 1, 'zone': 1, 'ip': '10.1.1.1',
              'port': 7000}],
            30)
        ring_fname_1 = os.path.join(self.testdir, 'foo-1.ring.gz')
        ring_fname_2 = os.path.join(self.testdir, 'foo-2.ring.gz')
        ring_fname_bad_version = os.path.join(self.testdir, 'foo-bar.ring.gz')
        rd.save(ring_fname_1, format_version=1)
        rd.save(ring_fname_2, format_version=2)
        with io.RingWriter.open(ring_fname_bad_version) as writer:
            writer.write_magic(5)
            with writer.section('foo'):
                writer.write_blob(b'\xde\xad\xbe\xef' * 10240)

        # Loading the bad ring will fail because it's an unknown version
        with self.assertRaises(Exception) as ex:
            ring.RingData.load(ring_fname_bad_version)
        self.assertEqual(
            f'Unsupported ring version: 5 for {ring_fname_bad_version!r}',
            str(ex.exception))

        orig_load_index = io.RingReader.load_index

        def mock_load_index(cls):
            cls.version = 5
            orig_load_index(cls)

        with mock.patch('swift.common.ring.io.RingReader.load_index',
                        mock_load_index):
            with self.assertRaises(Exception) as ex:
                ring.RingData.load(ring_fname_1)
        self.assertEqual(
            f'Unknown ring format version 5 for {ring_fname_1!r}',
            str(ex.exception))

        expected_rd_dict = {
            'devs': [
                {'id': 0, 'region': 1, 'zone': 0,
                 'ip': '10.1.1.0', 'port': 7000,
                 'replication_ip': '10.1.1.0', 'replication_port': 7000},
                {'id': 1, 'zone': 1, 'region': 1,
                 'ip': '10.1.1.1', 'port': 7000,
                 'replication_ip': '10.1.1.1', 'replication_port': 7000}],
            'replica2part2dev_id': [],
            'part_shift': 30,
            'next_part_power': None,
            'dev_id_bytes': 2,
            'version': None}

        # version 2
        loaded_rd = ring.RingData.load(ring_fname_2, metadata_only=True)
        self.assertEqual(loaded_rd.to_dict(), expected_rd_dict)

        # version 1
        loaded_rd = ring.RingData.load(ring_fname_1, metadata_only=True)
        self.assertEqual(loaded_rd.to_dict(), expected_rd_dict)

    def test_save(self):
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'zone': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'ip': '10.1.1.1', 'port': 7000}],
            30)

        # First test the supported versions
        for version in (1, 2):
            rd.save(ring_fname, format_version=version)

        # Now try an unknown version
        with self.assertRaises(ValueError) as err:
            for version in (3, None, "some version"):
                rd.save(ring_fname, format_version=version)
        self.assertEqual("format_version must be one of (1, 2)",
                         str(err.exception))
        # re-serialisation is already handled in test_load.

    def test_save_bad_dev_id_bytes(self):
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [array.array('I', [0, 1, 0, 1]), array.array('I', [0, 1, 0, 1])],
            [{'id': 0, 'zone': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'ip': '10.1.1.1', 'port': 7000}],
            30)

        # v2 ring can handle wide devices fine
        rd.save(ring_fname, format_version=2)
        # but not v1! Only 2-byte dev ids there!
        with self.assertRaises(DevIdBytesTooSmall):
            rd.save(ring_fname, format_version=1)


class TestRing(TestRingBase):
    FORMAT_VERSION = 1

    def setUp(self):
        super(TestRing, self).setUp()
        self.testdir = mkdtemp()
        self.testgz = os.path.join(self.testdir, 'whatever.ring.gz')
        self.intended_replica2part2dev_id = [
            array.array('H', [0, 1, 0, 1]),
            array.array('H', [0, 1, 0, 1]),
            array.array('H', [3, 4, 3, 4])]
        self.intended_devs = [{'id': 0, 'region': 0, 'zone': 0, 'weight': 1.0,
                               'ip': '10.1.1.1', 'port': 6200,
                               'replication_ip': '10.1.0.1',
                               'replication_port': 6066},
                              {'id': 1, 'region': 0, 'zone': 0, 'weight': 1.0,
                               'ip': '10.1.1.1', 'port': 6200,
                               'replication_ip': '10.1.0.2',
                               'replication_port': 6066},
                              None,
                              {'id': 3, 'region': 0, 'zone': 2, 'weight': 1.0,
                               'ip': '10.1.2.1', 'port': 6200,
                               'replication_ip': '10.2.0.1',
                               'replication_port': 6066},
                              {'id': 4, 'region': 0, 'zone': 2, 'weight': 1.0,
                               'ip': '10.1.2.2', 'port': 6200,
                               'replication_ip': '10.2.0.1',
                               'replication_port': 6066}]
        self.intended_part_shift = 30
        self.intended_reload_time = 15
        rd = ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift)
        rd.save(self.testgz, format_version=self.FORMAT_VERSION)
        self.ring = ring.Ring(
            self.testdir,
            reload_time=self.intended_reload_time, ring_name='whatever')

    def tearDown(self):
        super(TestRing, self).tearDown()
        rmtree(self.testdir, ignore_errors=1)

    def test_creation(self):
        self.assertEqual(self.ring._replica2part2dev_id,
                         self.intended_replica2part2dev_id)
        self.assertEqual(self.ring._part_shift, self.intended_part_shift)
        self.assertEqual(self.ring.devs, self.intended_devs)
        self.assertEqual(self.ring.reload_time, self.intended_reload_time)
        self.assertEqual(self.ring.serialized_path, self.testgz)
        self.assertIsNone(self.ring.version)

        with open(self.testgz, 'rb') as fp:
            expected_size = 0
            for chunk in iter(lambda: fp.read(2 ** 16), b''):
                expected_size += len(chunk)
        self.assertEqual(self.ring.size, expected_size)

        # test invalid endcap
        with mock.patch.object(utils, 'HASH_PATH_SUFFIX', b''), \
                mock.patch.object(utils, 'HASH_PATH_PREFIX', b''), \
                mock.patch.object(utils, 'SWIFT_CONF_FILE', ''):
            self.assertRaises(IOError, ring.Ring, self.testdir, 'whatever')

    def test_replica_count(self):
        self.assertEqual(self.ring.replica_count, 3)
        self.ring._replica2part2dev_id.append([0])
        self.assertEqual(self.ring.replica_count, 3.25)

    def test_has_changed(self):
        self.assertFalse(self.ring.has_changed())
        os.utime(self.testgz, (time() + 60, time() + 60))
        self.assertTrue(self.ring.has_changed())

    def test_reload(self):
        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        self.assertEqual(len(self.ring.devs), 5)
        self.intended_devs.append(
            {'id': 3, 'region': 0, 'zone': 3, 'weight': 1.0,
             'ip': '10.1.1.1', 'port': 9876})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift,
        ).save(self.testgz, format_version=self.FORMAT_VERSION)
        sleep(0.1)
        self.ring.get_nodes('a')
        self.assertEqual(len(self.ring.devs), 6)
        self.assertNotEqual(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        self.assertEqual(len(self.ring.devs), 6)
        self.intended_devs.append(
            {'id': 5, 'region': 0, 'zone': 4, 'weight': 1.0,
             'ip': '10.5.5.5', 'port': 9876})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift,
        ).save(self.testgz, format_version=self.FORMAT_VERSION)
        sleep(0.1)
        self.ring.get_part_nodes(0)
        self.assertEqual(len(self.ring.devs), 7)
        self.assertNotEqual(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        part, nodes = self.ring.get_nodes('a')
        self.assertEqual(len(self.ring.devs), 7)
        self.intended_devs.append(
            {'id': 6, 'region': 0, 'zone': 5, 'weight': 1.0,
             'ip': '10.6.6.6', 'port': 6200})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift,
        ).save(self.testgz, format_version=self.FORMAT_VERSION)
        sleep(0.1)
        next(self.ring.get_more_nodes(part))
        self.assertEqual(len(self.ring.devs), 8)
        self.assertNotEqual(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        self.assertEqual(len(self.ring.devs), 8)
        self.intended_devs.append(
            {'id': 5, 'region': 0, 'zone': 4, 'weight': 1.0,
             'ip': '10.5.5.5', 'port': 6200})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift,
        ).save(self.testgz, format_version=self.FORMAT_VERSION)
        sleep(0.1)
        self.assertEqual(len(self.ring.devs), 9)
        self.assertNotEqual(self.ring._mtime, orig_mtime)

    def test_reload_without_replication(self):
        replication_less_devs = [{'id': 0, 'region': 0, 'zone': 0,
                                  'weight': 1.0, 'ip': '10.1.1.1',
                                  'port': 6200},
                                 {'id': 1, 'region': 0, 'zone': 0,
                                  'weight': 1.0, 'ip': '10.1.1.1',
                                  'port': 6200},
                                 None,
                                 {'id': 3, 'region': 0, 'zone': 2,
                                  'weight': 1.0, 'ip': '10.1.2.1',
                                  'port': 6200},
                                 {'id': 4, 'region': 0, 'zone': 2,
                                  'weight': 1.0, 'ip': '10.1.2.2',
                                  'port': 6200}]
        intended_devs = [{'id': 0, 'region': 0, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6200,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6200},
                         {'id': 1, 'region': 0, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6200,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6200},
                         None,
                         {'id': 3, 'region': 0, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.1', 'port': 6200,
                          'replication_ip': '10.1.2.1',
                          'replication_port': 6200},
                         {'id': 4, 'region': 0, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.2', 'port': 6200,
                          'replication_ip': '10.1.2.2',
                          'replication_port': 6200}]
        testgz = os.path.join(self.testdir, 'without_replication.ring.gz')
        ring.RingData(
            self.intended_replica2part2dev_id,
            replication_less_devs, self.intended_part_shift,
        ).save(testgz, format_version=self.FORMAT_VERSION)
        self.ring = ring.Ring(
            self.testdir,
            reload_time=self.intended_reload_time,
            ring_name='without_replication')
        self.assertEqual(self.ring.devs, intended_devs)

    def test_get_part(self):
        part1 = self.ring.get_part('a')
        nodes1 = self.ring.get_part_nodes(part1)
        part2, nodes2 = self.ring.get_nodes('a')
        self.assertEqual(part1, part2)
        self.assertEqual(nodes1, nodes2)

    def test_get_part_nodes(self):
        part, nodes = self.ring.get_nodes('a')
        self.assertEqual(nodes, self.ring.get_part_nodes(part))

    def test_get_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results the ring produces, they know it.
        self.assertRaises(TypeError, self.ring.get_nodes)
        part, nodes = self.ring.get_nodes('a')
        self.assertEqual(part, 0)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a1')
        self.assertEqual(part, 0)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a4')
        self.assertEqual(part, 1)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[1],
                                    self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('aa')
        self.assertEqual(part, 1)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[1],
                                    self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('a', 'c1')
        self.assertEqual(part, 0)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c0')
        self.assertEqual(part, 3)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[1],
                                    self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('a', 'c3')
        self.assertEqual(part, 2)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c2')
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o1')
        self.assertEqual(part, 1)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[1],
                                    self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o5')
        self.assertEqual(part, 0)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o0')
        self.assertEqual(part, 0)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEqual(part, 2)
        self.assertEqual(nodes, [dict(node, index=i) for i, node in
                         enumerate([self.intended_devs[0],
                                    self.intended_devs[3]])])

    def add_dev_to_ring(self, new_dev):
        self.ring.devs.append(new_dev)
        self.ring._rebuild_tier_data()

    def test_get_more_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someone changes the results the ring produces, they know it.
        exp_part = 6
        exp_devs = [102, 39, 93]
        exp_zones = set([8, 9, 4])

        exp_handoffs = [
            69, 10, 22, 35, 56, 83, 100, 42, 92, 25, 50, 74, 61, 4,
            13, 67, 8, 20, 106, 47, 89, 27, 59, 76, 97, 37, 85, 64,
            0, 15, 32, 52, 79, 71, 11, 23, 99, 44, 90, 68, 6, 18,
            96, 36, 84, 103, 41, 95, 33, 54, 81, 24, 48, 72, 60, 3,
            12, 63, 2, 17, 28, 58, 75, 66, 7, 19, 104, 40, 94, 107,
            45, 87, 101, 43, 91, 29, 57, 77, 62, 5, 14, 105, 46, 88,
            98, 38, 86, 70, 9, 21, 65, 1, 16, 34, 55, 82, 31, 53,
            78, 30, 51, 80, 26, 49, 73]

        exp_first_handoffs = [
            28, 34, 101, 99, 35, 62, 69, 65, 71, 67, 60, 34,
            34, 101, 96, 98, 101, 27, 25, 106, 61, 63, 60,
            104, 106, 65, 106, 31, 25, 25, 32, 62, 70, 35, 31,
            99, 35, 33, 33, 64, 64, 32, 98, 69, 60, 102, 68,
            33, 34, 60, 26, 60, 98, 32, 29, 60, 107, 96, 31,
            65, 32, 26, 103, 62, 96, 62, 25, 103, 34, 30, 107,
            104, 25, 97, 32, 65, 102, 24, 67, 97, 70, 63, 35,
            105, 33, 104, 69, 29, 63, 30, 24, 102, 60, 30, 26,
            105, 103, 104, 35, 24, 30, 64, 99, 27, 71, 107,
            30, 25, 34, 33, 32, 62, 100, 103, 32, 33, 34, 99,
            70, 32, 68, 69, 33, 27, 71, 101, 102, 99, 30, 31,
            98, 71, 34, 33, 31, 100, 61, 107, 106, 66, 97,
            106, 96, 101, 34, 33, 33, 28, 106, 30, 64, 96,
            104, 105, 67, 32, 99, 102, 102, 30, 97, 105, 34,
            99, 31, 61, 64, 29, 64, 61, 30, 101, 106, 60, 35,
            34, 64, 61, 65, 101, 65, 62, 69, 60, 102, 107, 30,
            28, 28, 34, 28, 65, 99, 105, 33, 62, 99, 71, 29,
            66, 61, 101, 104, 104, 33, 96, 26, 62, 24, 64, 25,
            99, 97, 35, 103, 32, 67, 70, 102, 26, 99, 102,
            105, 65, 97, 31, 60, 60, 103, 98, 97, 98, 35, 66,
            24, 98, 71, 0, 24, 67, 67, 30, 62, 69, 105, 71,
            64, 101, 65, 32, 102, 35, 31, 34, 29, 105]

        rb = ring.RingBuilder(8, 3, 1)
        next_dev_id = 0
        for zone in range(1, 10):
            for server in range(1, 5):
                for device in range(1, 4):
                    rb.add_dev({'id': next_dev_id,
                                'ip': '1.2.%d.%d' % (zone, server),
                                'port': 1234 + device,
                                'zone': zone, 'region': 0,
                                'weight': 1.0,
                                'device': "d%s" % device})
                    next_dev_id += 1
        rb.rebalance(seed=43)
        rb.get_ring().save(self.testgz, format_version=self.FORMAT_VERSION)
        r = ring.Ring(self.testdir, ring_name='whatever')

        # every part has the same number of handoffs
        part_handoff_counts = set()
        for part in range(r.partition_count):
            part_handoff_counts.add(len(list(r.get_more_nodes(part))))
        self.assertEqual(part_handoff_counts, {105})
        # which less the primaries - is every device in the ring
        self.assertEqual(len(list(rb._iter_devs())) - rb.replicas, 105)

        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEqual(part, exp_part)
        self.assertEqual([d['id'] for d in devs], exp_devs)
        self.assertEqual(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        self.assertEqual(len(devs), len(exp_handoffs))
        dev_ids = [d['id'] for d in devs]
        self.assertEqual(dev_ids, exp_handoffs)
        # We mark handoffs so code consuming extra nodes can reason about how
        # far they've gone
        for i, d in enumerate(devs):
            self.assertEqual(d['handoff_index'], i)

        # The first 6 replicas plus the 3 primary nodes should cover all 9
        # zones in this test
        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEqual(seen_zones, set(range(1, 10)))

        # The first handoff nodes for each partition in the ring
        devs = []
        for part in range(r.partition_count):
            devs.append(next(r.get_more_nodes(part))['id'])
        self.assertEqual(devs, exp_first_handoffs)

        # Add a new device we can handoff to.
        zone = 5
        server = 0
        rb.add_dev({'id': next_dev_id,
                    'ip': '1.2.%d.%d' % (zone, server),
                    'port': 1234, 'zone': zone, 'region': 0, 'weight': 1.0,
                    'device': 'xd0'})
        next_dev_id += 1
        rb.pretend_min_part_hours_passed()
        num_parts_changed, _balance, _removed_dev = rb.rebalance(seed=43)
        rb.get_ring().save(self.testgz, format_version=self.FORMAT_VERSION)
        r = ring.Ring(self.testdir, ring_name='whatever')

        # so now we expect the device list to be longer by one device
        part_handoff_counts = set()
        for part in range(r.partition_count):
            part_handoff_counts.add(len(list(r.get_more_nodes(part))))
        self.assertEqual(part_handoff_counts, {106})
        self.assertEqual(len(list(rb._iter_devs())) - rb.replicas, 106)
        # I don't think there's any special reason this dev goes at this index
        exp_handoffs.insert(33, rb.devs[-1]['id'])

        # We would change expectations here, but in this part only the added
        # device changed at all.
        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEqual(part, exp_part)
        self.assertEqual([d['id'] for d in devs], exp_devs)
        self.assertEqual(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        dev_ids = [d['id'] for d in devs]
        self.assertEqual(len(dev_ids), len(exp_handoffs))
        for index, dev in enumerate(dev_ids):
            self.assertEqual(
                dev, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))

        # The handoffs still cover all the non-primary zones first
        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEqual(seen_zones, set(range(1, 10)))

        # Change expectations for the rest of the parts
        devs = []
        for part in range(r.partition_count):
            devs.append(next(r.get_more_nodes(part))['id'])
        changed_first_handoff = 0
        for part in range(r.partition_count):
            if devs[part] != exp_first_handoffs[part]:
                changed_first_handoff += 1
                exp_first_handoffs[part] = devs[part]
        self.assertEqual(devs, exp_first_handoffs)
        self.assertEqual(changed_first_handoff, num_parts_changed)

        # Remove a device - no need to fluff min_part_hours.
        rb.remove_dev(0)
        num_parts_changed, _balance, _removed_dev = rb.rebalance(seed=87)
        rb.get_ring().save(self.testgz, format_version=self.FORMAT_VERSION)
        r = ring.Ring(self.testdir, ring_name='whatever')

        # so now we expect the device list to be shorter by one device
        part_handoff_counts = set()
        for part in range(r.partition_count):
            part_handoff_counts.add(len(list(r.get_more_nodes(part))))
        self.assertEqual(part_handoff_counts, {105})
        self.assertEqual(len(list(rb._iter_devs())) - rb.replicas, 105)

        # Change expectations for our part
        exp_handoffs.remove(0)
        first_matches = 0
        total_changed = 0
        devs = list(d['id'] for d in r.get_more_nodes(exp_part))
        for i, part in enumerate(devs):
            if exp_handoffs[i] != devs[i]:
                total_changed += 1
                exp_handoffs[i] = devs[i]
            if not total_changed:
                first_matches += 1
        self.assertEqual(devs, exp_handoffs)
        # the first 32 handoffs were the same across the rebalance
        self.assertEqual(first_matches, 32)
        # but as you dig deeper some of the differences show up
        self.assertEqual(total_changed, 27)

        # Change expectations for the rest of the parts
        devs = []
        for part in range(r.partition_count):
            devs.append(next(r.get_more_nodes(part))['id'])
        changed_first_handoff = 0
        for part in range(r.partition_count):
            if devs[part] != exp_first_handoffs[part]:
                changed_first_handoff += 1
                exp_first_handoffs[part] = devs[part]
        self.assertEqual(devs, exp_first_handoffs)
        self.assertEqual(changed_first_handoff, num_parts_changed)

        # Test
        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEqual(part, exp_part)
        self.assertEqual([d['id'] for d in devs], exp_devs)
        self.assertEqual(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        dev_ids = [d['id'] for d in devs]
        self.assertEqual(len(dev_ids), len(exp_handoffs))
        for index, dev in enumerate(dev_ids):
            self.assertEqual(
                dev, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))

        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEqual(seen_zones, set(range(1, 10)))

        devs = []
        for part in range(r.partition_count):
            devs.append(next(r.get_more_nodes(part))['id'])
        for part in range(r.partition_count):
            self.assertEqual(
                devs[part], exp_first_handoffs[part],
                'handoff for partitition %d is now device id %d' % (
                    part, devs[part]))

        # Add a partial replica
        rb.set_replicas(3.5)
        num_parts_changed, _balance, _removed_dev = rb.rebalance(seed=164)
        rb.get_ring().save(self.testgz, format_version=self.FORMAT_VERSION)
        r = ring.Ring(self.testdir, ring_name='whatever')

        # Change expectations

        # We have another replica now
        exp_devs.append(13)
        exp_zones.add(2)
        # and therefore one less handoff
        exp_handoffs = exp_handoffs[:-1]
        # Caused some major changes in the sequence of handoffs for our test
        # partition, but at least the first stayed the same.
        devs = list(d['id'] for d in r.get_more_nodes(exp_part))
        first_matches = 0
        total_changed = 0
        for i, part in enumerate(devs):
            if exp_handoffs[i] != devs[i]:
                total_changed += 1
                exp_handoffs[i] = devs[i]
            if not total_changed:
                first_matches += 1
        # most seeds seem to throw out first handoff stabilization with
        # replica_count change
        self.assertEqual(first_matches, 0)
        # and lots of other handoff changes...
        self.assertEqual(total_changed, 95)

        self.assertEqual(devs, exp_handoffs)

        # Change expectations for the rest of the parts
        devs = []
        for part in range(r.partition_count):
            devs.append(next(r.get_more_nodes(part))['id'])
        changed_first_handoff = 0
        for part in range(r.partition_count):
            if devs[part] != exp_first_handoffs[part]:
                changed_first_handoff += 1
                exp_first_handoffs[part] = devs[part]
        self.assertEqual(devs, exp_first_handoffs)
        self.assertLessEqual(changed_first_handoff, num_parts_changed)

        # Test
        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEqual(part, exp_part)
        self.assertEqual([d['id'] for d in devs], exp_devs)
        self.assertEqual(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        dev_ids = [d['id'] for d in devs]
        self.assertEqual(len(dev_ids), len(exp_handoffs))

        for index, dev in enumerate(dev_ids):
            self.assertEqual(
                dev, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))

        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEqual(seen_zones, set(range(1, 10)))

        devs = []
        for part in range(r.partition_count):
            devs.append(next(r.get_more_nodes(part))['id'])
        for part in range(r.partition_count):
            self.assertEqual(
                devs[part], exp_first_handoffs[part],
                'handoff for partitition %d is now device id %d' % (
                    part, devs[part]))

        # One last test of a partial replica partition
        exp_part2 = 136
        exp_devs2 = [35, 56, 83]
        exp_zones2 = set([3, 5, 7])
        exp_handoffs2 = [
            61, 4, 13, 86, 103, 41, 63, 2, 17, 95, 70, 67, 8, 20,
            106, 100, 11, 23, 87, 47, 51, 42, 30, 24, 48, 72, 27,
            59, 76, 97, 38, 90, 108, 79, 55, 68, 6, 18, 105, 71,
            62, 5, 14, 107, 89, 7, 45, 69, 10, 22, 12, 99, 44, 46,
            88, 74, 39, 15, 102, 93, 85, 34, 98, 29, 57, 77, 84, 9,
            21, 58, 78, 32, 52, 66, 19, 28, 75, 65, 1, 16, 33, 37,
            49, 82, 31, 53, 54, 81, 96, 92, 3, 25, 50, 60, 36, 101,
            43, 104, 40, 94, 64, 80, 26, 73, 91]

        part2, devs2 = r.get_nodes('a', 'c', 'o2')
        primary_zones2 = set([d['zone'] for d in devs2])
        self.assertEqual(part2, exp_part2)
        self.assertEqual([d['id'] for d in devs2], exp_devs2)
        self.assertEqual(primary_zones2, exp_zones2)
        devs2 = list(r.get_more_nodes(part2))
        dev_ids2 = [d['id'] for d in devs2]

        self.assertEqual(len(dev_ids2), len(exp_handoffs2))
        for index, dev in enumerate(dev_ids2):
            self.assertEqual(
                dev, exp_handoffs2[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids2[index:], exp_handoffs2[index:]))

        seen_zones = set(primary_zones2)
        seen_zones.update([d['zone'] for d in devs2[:6]])
        self.assertEqual(seen_zones, set(range(1, 10)))

        # Test distribution across regions
        rb.set_replicas(3)
        for region in range(1, 5):
            rb.add_dev({'id': next_dev_id,
                        'ip': '1.%d.1.%d' % (region, server), 'port': 1234,
                        # 108.0 is the weight of all devices created prior to
                        # this test in region 0; this way all regions have
                        # equal combined weight
                        'zone': 1, 'region': region, 'weight': 108.0,
                        'device': 'sdx'})
            next_dev_id += 1
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=1)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=1)
        rb.get_ring().save(self.testgz, format_version=self.FORMAT_VERSION)
        r = ring.Ring(self.testdir, ring_name='whatever')

        # There's 5 regions now, so the primary nodes + first 2 handoffs
        # should span all 5 regions
        part, devs = r.get_nodes('a1', 'c1', 'o1')
        primary_regions = set([d['region'] for d in devs])
        primary_zones = set([(d['region'], d['zone']) for d in devs])
        more_devs = list(r.get_more_nodes(part))

        seen_regions = set(primary_regions)
        seen_regions.update([d['region'] for d in more_devs[:2]])
        self.assertEqual(seen_regions, set(range(0, 5)))

        # There are 13 zones now, so the first 13 nodes should all have
        # distinct zones (that's r0z0, r0z1, ..., r0z8, r1z1, r2z1, r3z1, and
        # r4z1).
        seen_zones = set(primary_zones)
        seen_zones.update([(d['region'], d['zone']) for d in more_devs[:10]])
        self.assertEqual(13, len(seen_zones))

        # Here's a brittle canary-in-the-coalmine test to make sure the region
        # handoff computation didn't change accidentally
        exp_handoffs = [111, 112, 83, 45, 21, 95, 51, 26, 3, 102, 72, 80, 59,
                        61, 14, 89, 105, 31, 1, 39, 90, 16, 86, 75, 49, 42, 35,
                        71, 99, 20, 97, 27, 54, 67, 8, 11, 37, 108, 73, 78, 23,
                        53, 79, 82, 57, 106, 85, 22, 25, 13, 47, 76, 18, 84,
                        81, 12, 32, 17, 103, 41, 19, 50, 52, 4, 94, 64, 48, 63,
                        43, 66, 104, 6, 62, 87, 69, 68, 46, 98, 77, 2, 107, 93,
                        9, 28, 55, 33, 5, 92, 74, 96, 7, 40, 30, 100, 36, 15,
                        88, 58, 24, 56, 34, 101, 60, 10, 38, 29, 70, 44, 91]

        dev_ids = [d['id'] for d in more_devs]

        self.assertEqual(len(dev_ids), len(exp_handoffs))
        for index, dev_id in enumerate(dev_ids):
            self.assertEqual(
                dev_id, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))

    def test_get_more_nodes_with_zero_weight_region(self):
        rb = ring.RingBuilder(8, 3, 1)
        devs = [
            ring_utils.parse_add_value(v) for v in [
                'r1z1-127.0.0.1:6200/d1',
                'r1z1-127.0.0.1:6201/d2',
                'r1z1-127.0.0.1:6202/d3',
                'r1z1-127.0.0.1:6203/d4',
                'r1z2-127.0.0.2:6200/d1',
                'r1z2-127.0.0.2:6201/d2',
                'r1z2-127.0.0.2:6202/d3',
                'r1z2-127.0.0.2:6203/d4',
                'r2z1-127.0.1.1:6200/d1',
                'r2z1-127.0.1.1:6201/d2',
                'r2z1-127.0.1.1:6202/d3',
                'r2z1-127.0.1.1:6203/d4',
                'r2z2-127.0.1.2:6200/d1',
                'r2z2-127.0.1.2:6201/d2',
                'r2z2-127.0.1.2:6202/d3',
                'r2z2-127.0.1.2:6203/d4',
            ]
        ]
        for dev in devs:
            if dev['region'] == 2:
                dev['weight'] = 0.0
            else:
                dev['weight'] = 1.0
            rb.add_dev(dev)
        rb.rebalance()
        rb.get_ring().save(self.testgz, format_version=self.FORMAT_VERSION)
        r = ring.Ring(self.testdir, ring_name='whatever')
        self.assertEqual(r.version, rb.version)

        class CountingRingTable(object):

            def __init__(self, table):
                self.table = table
                self.count = 0

            def __iter__(self):
                self._iter = iter(self.table)
                return self

            def __next__(self):
                self.count += 1
                return next(self._iter)

            def __getitem__(self, key):
                return self.table[key]

        histogram = collections.defaultdict(int)
        for part in range(r.partition_count):
            counting_table = CountingRingTable(r._replica2part2dev_id)
            with mock.patch.object(r, '_replica2part2dev_id', counting_table):
                node_iter = r.get_more_nodes(part)
                next(node_iter)
            histogram[counting_table.count] += 1
        # Don't let our summing muddy our histogram
        histogram = dict(histogram)

        # sanity
        self.assertEqual(1, r._num_regions)
        self.assertEqual(2, r._num_zones)
        self.assertEqual(256, r.partition_count)

        # We always do one loop (including the StopIteration) while getting
        # primaries, so every part should hit next() at least 5 times
        self.assertEqual(sum(histogram.get(x, 0) for x in range(5)), 0,
                         histogram)

        # Most of the parts should find a handoff device in the next partition,
        # but because some of the primary devices may *also* be used for that
        # partition, that means 5, 6, or 7 calls to next().
        self.assertGreater(sum(histogram.get(x, 0) for x in range(8)), 160,
                           histogram)

        # Want 90% confidence that it'll happen within two partitions
        self.assertGreater(sum(histogram.get(x, 0) for x in range(12)), 230,
                           histogram)

        # Tail should fall off fairly quickly
        self.assertLess(sum(histogram.get(x, 0) for x in range(20, 100)), 5,
                        histogram)

        # Hard limit at 50 (we've seen as bad as 41, 45)
        self.assertEqual(sum(histogram.get(x, 0) for x in range(50, 100)), 0,
                         histogram)


class TestRingV2(TestRing):
    FORMAT_VERSION = 2

    def test_4_byte_dev_ids(self):
        ring_file = os.path.join(self.testdir, 'test.ring.gz')
        index = {}
        with GzipFile(ring_file, 'wb') as fp:
            fp.write(b'R1NG\x00\x02')
            fp.flush(zlib.Z_FULL_FLUSH)

            comp_start = os.fstat(fp.fileno()).st_size
            uncomp_start = fp.tell()
            meta = json.dumps({
                "dev_id_bytes": 4,
                "part_shift": 29,
                "replica_count": 1.5,
            }).encode('ascii')
            to_write = struct.pack('!Q', len(meta)) + meta
            fp.write(to_write)
            fp.flush(zlib.Z_FULL_FLUSH)
            index['swift/ring/metadata'] = [
                comp_start,
                uncomp_start,
                os.fstat(fp.fileno()).st_size,
                fp.tell(),
                'sha256',
                hashlib.sha256(to_write).hexdigest()]

            comp_start = os.fstat(fp.fileno()).st_size
            uncomp_start = fp.tell()
            devs = json.dumps([
                {"id": 0, "region": 1, "zone": 1, "ip": "127.0.0.1",
                 "port": 6200, "device": "sda", "weight": 1},
                None,
                {"id": 2, "region": 1, "zone": 1, "ip": "127.0.0.1",
                 "port": 6201, "device": "sdb", "weight": 1},
                {"id": 3, "region": 1, "zone": 1, "ip": "127.0.0.1",
                 "port": 6202, "device": "sdc", "weight": 1},
            ]).encode('ascii')
            to_write = struct.pack('!Q', len(devs)) + devs
            fp.write(to_write)
            fp.flush(zlib.Z_FULL_FLUSH)
            index['swift/ring/devices'] = [
                comp_start,
                uncomp_start,
                os.fstat(fp.fileno()).st_size,
                fp.tell(),
                'sha256',
                hashlib.sha256(to_write).hexdigest()]

            comp_start = os.fstat(fp.fileno()).st_size
            uncomp_start = fp.tell()
            to_write = struct.pack('!Q', 48) + 4 * (
                b'\x00\x00\x00\x03'
                b'\x00\x00\x00\x02'
                b'\x00\x00\x00\x00')
            fp.write(to_write)
            fp.flush(zlib.Z_FULL_FLUSH)
            index['swift/ring/assignments'] = [
                comp_start,
                uncomp_start,
                os.fstat(fp.fileno()).st_size,
                fp.tell(),
                'sha256',
                hashlib.sha256(to_write).hexdigest()]

            comp_start = os.fstat(fp.fileno()).st_size
            uncomp_start = fp.tell()
            blob = json.dumps(index).encode('ascii')
            fp.write(struct.pack('!Q', len(blob)) + blob)
            fp.flush(zlib.Z_FULL_FLUSH)

            fp.compress = zlib.compressobj(
                0, zlib.DEFLATED, -zlib.MAX_WBITS, zlib.DEF_MEM_LEVEL, 0)
            fp.write(struct.pack('!Q', uncomp_start))
            fp.flush(zlib.Z_FULL_FLUSH)
            fp.write(struct.pack('!Q', comp_start))
            fp.flush(zlib.Z_FULL_FLUSH)

        r = ring.Ring(ring_file)
        self.assertEqual(
            [[d['id'] for d in r.get_part_nodes(p)] for p in range(8)],
            [[3, 0], [2, 3], [0, 2], [3, 0], [2], [0], [3], [2]])


class ExtendedRingData(ring.RingData):
    extra = b'some super-specific data'

    def to_dict(self):
        ring_data = super().to_dict()
        ring_data.setdefault('extra', self.extra)
        return ring_data

    def serialize_v2(self, writer):
        super().serialize_v2(writer)
        with writer.section('my-custom-section') as s:
            s.write_blob(self.extra)

    @classmethod
    def deserialize_v2(cls, reader, *args, **kwargs):
        ring_data = super().deserialize_v2(reader, *args, **kwargs)
        # If you're adding custom data to your rings, you probably want an
        # upgrade story that includes that data not being present
        if 'my-custom-section' in reader.index:
            with reader.open_section('my-custom-section') as s:
                ring_data['extra'] = s.read()
        return ring_data

    @classmethod
    def from_dict(cls, ring_data):
        obj = super().from_dict(ring_data)
        obj.extra = ring_data.get('extra')
        return obj


class TestRingExtensibility(unittest.TestCase):
    def test(self):
        r2p2d = [[0, 1, 0, 1], [0, 1, 0, 1]]
        d = [{'id': 0, 'zone': 0, 'region': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'region': 1, 'ip': '10.1.1.1', 'port': 7000}]
        s = 30
        rd = ExtendedRingData(r2p2d, d, s)
        self.assertEqual(rd._replica2part2dev_id, r2p2d)
        self.assertEqual(rd.devs, d)
        self.assertEqual(rd._part_shift, s)
        self.assertEqual(rd.extra, b'some super-specific data')

        # Can update it and round-trip to disk and back
        rd.extra = b'some other value'
        testdir = mkdtemp()
        try:
            ring_fname = os.path.join(testdir, 'foo.ring.gz')
            rd.save(ring_fname, format_version=2)
            bytes_written = os.path.getsize(ring_fname)
            rd2 = ExtendedRingData.load(ring_fname)
            # Vanilla Swift can also read the custom ring
            vanilla_ringdata = ring.RingData.load(ring_fname)
        finally:
            rmtree(testdir, ignore_errors=1)

        self.assertEqual(rd2._replica2part2dev_id, r2p2d)
        self.assertEqual(rd2.devs, d)
        self.assertEqual(rd2._part_shift, s)
        self.assertEqual(rd2.extra, b'some other value')
        self.assertEqual(rd2.size, bytes_written)

        self.assertEqual(vanilla_ringdata._replica2part2dev_id, r2p2d)
        self.assertEqual(vanilla_ringdata.devs, d)
        self.assertEqual(vanilla_ringdata._part_shift, s)
        self.assertFalse(hasattr(vanilla_ringdata, 'extra'))
        self.assertEqual(vanilla_ringdata.size, bytes_written)

    def test_missing_custom_data(self):
        r2p2d = [[0, 1, 0, 1], [0, 1, 0, 1]]
        d = [{'id': 0, 'zone': 0, 'region': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'region': 1, 'ip': '10.1.1.1', 'port': 7000}]
        s = 30
        rd = ring.RingData(r2p2d, d, s)
        self.assertEqual(rd._replica2part2dev_id, r2p2d)
        self.assertEqual(rd.devs, d)
        self.assertEqual(rd._part_shift, s)
        self.assertFalse(hasattr(rd, 'extra'))

        # Can load a vanilla ring and get some default behavior based on the
        # overridden from_dict
        testdir = mkdtemp()
        try:
            ring_fname = os.path.join(testdir, 'foo.ring.gz')
            rd.save(ring_fname, format_version=2)
            bytes_written = os.path.getsize(ring_fname)
            rd2 = ExtendedRingData.load(ring_fname)
        finally:
            rmtree(testdir, ignore_errors=1)

        self.assertEqual(rd2._replica2part2dev_id, r2p2d)
        self.assertEqual(rd2.devs, d)
        self.assertEqual(rd2._part_shift, s)
        self.assertIsNone(rd2.extra)
        self.assertEqual(rd2.size, bytes_written)


if __name__ == '__main__':
    unittest.main()
