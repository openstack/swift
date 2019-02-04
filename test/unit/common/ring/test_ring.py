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
import six.moves.cPickle as pickle
import os
import unittest
import stat
from contextlib import closing
from gzip import GzipFile
from tempfile import mkdtemp
from shutil import rmtree
from time import sleep, time
import sys
import copy
import mock

from six.moves import range

from swift.common import ring, utils
from swift.common.ring import utils as ring_utils


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
        self.testdir = os.path.join(os.path.dirname(__file__), 'ring_data')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def assert_ring_data_equal(self, rd_expected, rd_got):
        self.assertEqual(rd_expected._replica2part2dev_id,
                         rd_got._replica2part2dev_id)
        self.assertEqual(rd_expected.devs, rd_got.devs)
        self.assertEqual(rd_expected._part_shift, rd_got._part_shift)

    def test_attrs(self):
        r2p2d = [[0, 1, 0, 1], [0, 1, 0, 1]]
        d = [{'id': 0, 'zone': 0, 'region': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'region': 1, 'ip': '10.1.1.1', 'port': 7000}]
        s = 30
        rd = ring.RingData(r2p2d, d, s)
        self.assertEqual(rd._replica2part2dev_id, r2p2d)
        self.assertEqual(rd.devs, d)
        self.assertEqual(rd._part_shift, s)

    def test_can_load_pickled_ring_data(self):
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'zone': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'ip': '10.1.1.1', 'port': 7000}],
            30)
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        for p in range(pickle.HIGHEST_PROTOCOL):
            with closing(GzipFile(ring_fname, 'wb')) as f:
                pickle.dump(rd, f, protocol=p)
            meta_only = ring.RingData.load(ring_fname, metadata_only=True)
            self.assertEqual([
                {'id': 0, 'zone': 0, 'region': 1, 'ip': '10.1.1.0',
                 'port': 7000},
                {'id': 1, 'zone': 1, 'region': 1, 'ip': '10.1.1.1',
                 'port': 7000},
            ], meta_only.devs)
            # Pickled rings can't load only metadata, so you get it all
            self.assert_ring_data_equal(rd, meta_only)
            ring_data = ring.RingData.load(ring_fname)
            self.assert_ring_data_equal(rd, ring_data)

    def test_roundtrip_serialization(self):
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])],
            [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        rd.save(ring_fname)
        meta_only = ring.RingData.load(ring_fname, metadata_only=True)
        self.assertEqual([
            {'id': 0, 'zone': 0, 'region': 1},
            {'id': 1, 'zone': 1, 'region': 1},
        ], meta_only.devs)
        self.assertEqual([], meta_only._replica2part2dev_id)
        rd2 = ring.RingData.load(ring_fname)
        self.assert_ring_data_equal(rd, rd2)

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
            rds.save(ring_fname)

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


class TestRing(TestRingBase):

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
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift).save(self.testgz)
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
        # test invalid endcap
        with mock.patch.object(utils, 'HASH_PATH_SUFFIX', b''), \
                mock.patch.object(utils, 'HASH_PATH_PREFIX', b''), \
                mock.patch.object(utils, 'SWIFT_CONF_FILE', ''):
            self.assertRaises(SystemExit, ring.Ring, self.testdir, 'whatever')

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
            self.intended_devs, self.intended_part_shift).save(self.testgz)
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
            self.intended_devs, self.intended_part_shift).save(self.testgz)
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
            self.intended_devs, self.intended_part_shift).save(self.testgz)
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
            self.intended_devs, self.intended_part_shift).save(self.testgz)
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
            replication_less_devs, self.intended_part_shift).save(testgz)
        self.ring = ring.Ring(
            self.testdir,
            reload_time=self.intended_reload_time,
            ring_name='without_replication')
        self.assertEqual(self.ring.devs, intended_devs)

    def test_reload_old_style_pickled_ring(self):
        devs = [{'id': 0, 'zone': 0,
                 'weight': 1.0, 'ip': '10.1.1.1',
                 'port': 6200},
                {'id': 1, 'zone': 0,
                 'weight': 1.0, 'ip': '10.1.1.1',
                 'port': 6200},
                None,
                {'id': 3, 'zone': 2,
                 'weight': 1.0, 'ip': '10.1.2.1',
                 'port': 6200},
                {'id': 4, 'zone': 2,
                 'weight': 1.0, 'ip': '10.1.2.2',
                 'port': 6200}]
        intended_devs = [{'id': 0, 'region': 1, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6200,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6200},
                         {'id': 1, 'region': 1, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6200,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6200},
                         None,
                         {'id': 3, 'region': 1, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.1', 'port': 6200,
                          'replication_ip': '10.1.2.1',
                          'replication_port': 6200},
                         {'id': 4, 'region': 1, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.2', 'port': 6200,
                          'replication_ip': '10.1.2.2',
                          'replication_port': 6200}]

        # simulate an old-style pickled ring
        testgz = os.path.join(self.testdir,
                              'without_replication_or_region.ring.gz')
        ring_data = ring.RingData(self.intended_replica2part2dev_id,
                                  devs,
                                  self.intended_part_shift)
        # an old-style pickled ring won't have region data
        for dev in ring_data.devs:
            if dev:
                del dev["region"]
        gz_file = GzipFile(testgz, 'wb')
        pickle.dump(ring_data, gz_file, protocol=2)
        gz_file.close()

        self.ring = ring.Ring(
            self.testdir,
            reload_time=self.intended_reload_time,
            ring_name='without_replication_or_region')
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

    @unittest.skipIf(sys.version_info >= (3,),
                     "Seed-specific tests don't work well on py3")
    def test_get_more_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someone changes the results the ring produces, they know it.
        exp_part = 6
        exp_devs = [71, 77, 30]
        exp_zones = set([6, 3, 7])

        exp_handoffs = [99, 43, 94, 13, 1, 49, 60, 72, 27, 68, 78, 26, 21, 9,
                        51, 105, 47, 89, 65, 82, 34, 98, 38, 85, 16, 4, 59,
                        102, 40, 90, 20, 8, 54, 66, 80, 25, 14, 2, 50, 12, 0,
                        48, 70, 76, 32, 107, 45, 87, 101, 44, 93, 100, 42, 95,
                        106, 46, 88, 97, 37, 86, 96, 36, 84, 17, 5, 57, 63,
                        81, 33, 67, 79, 24, 15, 3, 58, 69, 75, 31, 61, 74, 29,
                        23, 10, 52, 22, 11, 53, 64, 83, 35, 62, 73, 28, 18, 6,
                        56, 104, 39, 91, 103, 41, 92, 19, 7, 55]

        exp_first_handoffs = [23, 64, 105, 102, 67, 17, 99, 65, 69, 97, 15,
                              17, 24, 98, 66, 65, 69, 18, 104, 105, 16, 107,
                              100, 15, 14, 19, 102, 105, 63, 104, 99, 12, 107,
                              99, 16, 105, 71, 15, 15, 63, 63, 99, 21, 68, 20,
                              64, 96, 21, 98, 19, 68, 99, 15, 69, 62, 100, 96,
                              102, 17, 62, 13, 61, 102, 105, 22, 16, 21, 18,
                              21, 100, 20, 16, 21, 106, 66, 106, 16, 99, 16,
                              22, 62, 60, 99, 69, 18, 23, 104, 98, 106, 61,
                              21, 23, 23, 16, 67, 71, 101, 16, 64, 66, 70, 15,
                              102, 63, 19, 98, 18, 106, 101, 100, 62, 63, 98,
                              18, 13, 97, 23, 22, 100, 13, 14, 67, 96, 14,
                              105, 97, 71, 64, 96, 22, 65, 66, 98, 19, 105,
                              98, 97, 21, 15, 69, 100, 98, 106, 65, 66, 97,
                              62, 22, 68, 63, 61, 67, 67, 20, 105, 106, 105,
                              18, 71, 100, 17, 62, 60, 13, 103, 99, 101, 96,
                              97, 16, 60, 21, 14, 20, 12, 60, 69, 104, 65, 65,
                              17, 16, 67, 13, 64, 15, 16, 68, 96, 21, 104, 66,
                              96, 105, 58, 105, 103, 21, 96, 60, 16, 96, 21,
                              71, 16, 99, 101, 63, 62, 103, 18, 102, 60, 17,
                              19, 106, 97, 14, 99, 68, 102, 13, 70, 103, 21,
                              22, 19, 61, 103, 23, 104, 65, 62, 68, 16, 65,
                              15, 102, 102, 71, 99, 63, 67, 19, 23, 15, 69,
                              107, 14, 13, 64, 13, 105, 15, 98, 69]

        rb = ring.RingBuilder(8, 3, 1)
        next_dev_id = 0
        for zone in range(1, 10):
            for server in range(1, 5):
                for device in range(1, 4):
                    rb.add_dev({'id': next_dev_id,
                                'ip': '1.2.%d.%d' % (zone, server),
                                'port': 1234 + device,
                                'zone': zone, 'region': 0,
                                'weight': 1.0})
                    next_dev_id += 1
        rb.rebalance(seed=2)
        rb.get_ring().save(self.testgz)
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
                    'port': 1234, 'zone': zone, 'region': 0, 'weight': 1.0})
        next_dev_id += 1
        rb.pretend_min_part_hours_passed()
        num_parts_changed, _balance, _removed_dev = rb.rebalance(seed=2)
        rb.get_ring().save(self.testgz)
        r = ring.Ring(self.testdir, ring_name='whatever')

        # so now we expect the device list to be longer by one device
        part_handoff_counts = set()
        for part in range(r.partition_count):
            part_handoff_counts.add(len(list(r.get_more_nodes(part))))
        self.assertEqual(part_handoff_counts, {106})
        self.assertEqual(len(list(rb._iter_devs())) - rb.replicas, 106)
        # I don't think there's any special reason this dev goes at this index
        exp_handoffs.insert(27, rb.devs[-1]['id'])

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
        num_parts_changed, _balance, _removed_dev = rb.rebalance(seed=1)
        rb.get_ring().save(self.testgz)
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
        # the first 21 handoffs were the same across the rebalance
        self.assertEqual(first_matches, 21)
        # but as you dig deeper some of the differences show up
        self.assertEqual(total_changed, 41)

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
        rb.get_ring().save(self.testgz)
        r = ring.Ring(self.testdir, ring_name='whatever')

        # Change expectations

        # We have another replica now
        exp_devs.append(90)
        exp_zones.add(8)
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
        self.assertEqual(first_matches, 2)
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
        exp_devs2 = [70, 76, 32]
        exp_zones2 = set([3, 6, 7])
        exp_handoffs2 = [89, 97, 37, 53, 20, 1, 86, 64, 102, 40, 90, 60, 72,
                         27, 99, 68, 78, 26, 105, 45, 42, 95, 22, 13, 49, 55,
                         11, 8, 83, 16, 4, 59, 33, 108, 61, 74, 29, 88, 66,
                         80, 25, 100, 39, 67, 79, 24, 65, 96, 36, 84, 54, 21,
                         63, 81, 56, 71, 77, 30, 48, 23, 10, 52, 82, 34, 17,
                         107, 87, 104, 5, 35, 2, 50, 43, 62, 73, 28, 18, 14,
                         98, 38, 85, 15, 57, 9, 51, 12, 6, 91, 3, 103, 41, 92,
                         47, 75, 44, 69, 101, 93, 106, 46, 94, 31, 19, 7, 58]

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
                        'zone': 1, 'region': region, 'weight': 108.0})
            next_dev_id += 1
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=1)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=1)
        rb.get_ring().save(self.testgz)
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
        exp_handoffs = [111, 112, 35, 58, 62, 74, 20, 105, 41, 90, 53, 6, 3,
                        67, 55, 76, 108, 32, 12, 80, 38, 85, 94, 42, 27, 99,
                        50, 47, 70, 87, 26, 9, 15, 97, 102, 81, 23, 65, 33,
                        77, 34, 4, 75, 8, 5, 30, 13, 73, 36, 92, 54, 51, 72,
                        78, 66, 1, 48, 14, 93, 95, 88, 86, 84, 106, 60, 101,
                        57, 43, 89, 59, 79, 46, 61, 52, 44, 45, 37, 68, 25,
                        100, 49, 24, 16, 71, 96, 21, 107, 98, 64, 39, 18, 29,
                        103, 91, 22, 63, 69, 28, 56, 11, 82, 10, 17, 19, 7,
                        40, 83, 104, 31]
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
        rb.get_ring().save(self.testgz)
        r = ring.Ring(self.testdir, ring_name='whatever')

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

            # complete the api
            next = __next__

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


if __name__ == '__main__':
    unittest.main()
