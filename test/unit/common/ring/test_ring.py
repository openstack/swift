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
import cPickle as pickle
import os
import sys
import unittest
import stat
from contextlib import closing
from gzip import GzipFile
from tempfile import mkdtemp
from shutil import rmtree
from time import sleep, time

from swift.common import ring, utils


class TestRingBase(unittest.TestCase):

    def setUp(self):
        self._orig_hash_suffix = utils.HASH_PATH_SUFFIX
        self._orig_hash_prefix = utils.HASH_PATH_PREFIX
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = ''

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
        self.assertEquals(rd_expected._replica2part2dev_id,
                          rd_got._replica2part2dev_id)
        self.assertEquals(rd_expected.devs, rd_got.devs)
        self.assertEquals(rd_expected._part_shift, rd_got._part_shift)

    def test_attrs(self):
        r2p2d = [[0, 1, 0, 1], [0, 1, 0, 1]]
        d = [{'id': 0, 'zone': 0, 'region': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'region': 1, 'ip': '10.1.1.1', 'port': 7000}]
        s = 30
        rd = ring.RingData(r2p2d, d, s)
        self.assertEquals(rd._replica2part2dev_id, r2p2d)
        self.assertEquals(rd.devs, d)
        self.assertEquals(rd._part_shift, s)

    def test_can_load_pickled_ring_data(self):
        rd = ring.RingData(
            [[0, 1, 0, 1], [0, 1, 0, 1]],
            [{'id': 0, 'zone': 0, 'ip': '10.1.1.0', 'port': 7000},
             {'id': 1, 'zone': 1, 'ip': '10.1.1.1', 'port': 7000}],
            30)
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        for p in xrange(pickle.HIGHEST_PROTOCOL):
            with closing(GzipFile(ring_fname, 'wb')) as f:
                pickle.dump(rd, f, protocol=p)
            ring_data = ring.RingData.load(ring_fname)
            self.assert_ring_data_equal(rd, ring_data)

    def test_roundtrip_serialization(self):
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])],
            [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        rd.save(ring_fname)
        rd2 = ring.RingData.load(ring_fname)
        self.assert_ring_data_equal(rd, rd2)

    def test_deterministic_serialization(self):
        """
        Two identical rings should produce identical .gz files on disk.

        Only true on Python 2.7 or greater.
        """
        if sys.version_info[0] == 2 and sys.version_info[1] < 7:
            return
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
        with open(ring_fname1) as ring1:
            with open(ring_fname2) as ring2:
                self.assertEqual(ring1.read(), ring2.read())

    def test_permissions(self):
        ring_fname = os.path.join(self.testdir, 'stat.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H', [0, 1, 0, 1])],
            [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        rd.save(ring_fname)
        self.assertEqual(oct(stat.S_IMODE(os.stat(ring_fname).st_mode)),
                         '0644')


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
                               'ip': '10.1.1.1', 'port': 6000,
                               'replication_ip': '10.1.0.1',
                               'replication_port': 6066},
                              {'id': 1, 'region': 0, 'zone': 0, 'weight': 1.0,
                               'ip': '10.1.1.1', 'port': 6000,
                               'replication_ip': '10.1.0.2',
                               'replication_port': 6066},
                              None,
                              {'id': 3, 'region': 0, 'zone': 2, 'weight': 1.0,
                               'ip': '10.1.2.1', 'port': 6000,
                               'replication_ip': '10.2.0.1',
                               'replication_port': 6066},
                              {'id': 4, 'region': 0, 'zone': 2, 'weight': 1.0,
                               'ip': '10.1.2.2', 'port': 6000,
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
        self.assertEquals(self.ring._replica2part2dev_id,
                          self.intended_replica2part2dev_id)
        self.assertEquals(self.ring._part_shift, self.intended_part_shift)
        self.assertEquals(self.ring.devs, self.intended_devs)
        self.assertEquals(self.ring.reload_time, self.intended_reload_time)
        self.assertEquals(self.ring.serialized_path, self.testgz)
        # test invalid endcap
        _orig_hash_path_suffix = utils.HASH_PATH_SUFFIX
        _orig_hash_path_prefix = utils.HASH_PATH_PREFIX
        _orig_swift_conf_file = utils.SWIFT_CONF_FILE
        try:
            utils.HASH_PATH_SUFFIX = ''
            utils.HASH_PATH_PREFIX = ''
            utils.SWIFT_CONF_FILE = ''
            self.assertRaises(SystemExit, ring.Ring, self.testdir, 'whatever')
        finally:
            utils.HASH_PATH_SUFFIX = _orig_hash_path_suffix
            utils.HASH_PATH_PREFIX = _orig_hash_path_prefix
            utils.SWIFT_CONF_FILE = _orig_swift_conf_file

    def test_has_changed(self):
        self.assertEquals(self.ring.has_changed(), False)
        os.utime(self.testgz, (time() + 60, time() + 60))
        self.assertEquals(self.ring.has_changed(), True)

    def test_reload(self):
        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        self.assertEquals(len(self.ring.devs), 5)
        self.intended_devs.append(
            {'id': 3, 'region': 0, 'zone': 3, 'weight': 1.0,
             'ip': '10.1.1.1', 'port': 9876})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift).save(self.testgz)
        sleep(0.1)
        self.ring.get_nodes('a')
        self.assertEquals(len(self.ring.devs), 6)
        self.assertNotEquals(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        self.assertEquals(len(self.ring.devs), 6)
        self.intended_devs.append(
            {'id': 5, 'region': 0, 'zone': 4, 'weight': 1.0,
             'ip': '10.5.5.5', 'port': 9876})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift).save(self.testgz)
        sleep(0.1)
        self.ring.get_part_nodes(0)
        self.assertEquals(len(self.ring.devs), 7)
        self.assertNotEquals(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        part, nodes = self.ring.get_nodes('a')
        self.assertEquals(len(self.ring.devs), 7)
        self.intended_devs.append(
            {'id': 6, 'region': 0, 'zone': 5, 'weight': 1.0,
             'ip': '10.6.6.6', 'port': 6000})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift).save(self.testgz)
        sleep(0.1)
        self.ring.get_more_nodes(part).next()
        self.assertEquals(len(self.ring.devs), 8)
        self.assertNotEquals(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testdir, reload_time=0.001,
                              ring_name='whatever')
        orig_mtime = self.ring._mtime
        self.assertEquals(len(self.ring.devs), 8)
        self.intended_devs.append(
            {'id': 5, 'region': 0, 'zone': 4, 'weight': 1.0,
             'ip': '10.5.5.5', 'port': 6000})
        ring.RingData(
            self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift).save(self.testgz)
        sleep(0.1)
        self.assertEquals(len(self.ring.devs), 9)
        self.assertNotEquals(self.ring._mtime, orig_mtime)

    def test_reload_without_replication(self):
        replication_less_devs = [{'id': 0, 'region': 0, 'zone': 0,
                                  'weight': 1.0, 'ip': '10.1.1.1',
                                  'port': 6000},
                                 {'id': 1, 'region': 0, 'zone': 0,
                                  'weight': 1.0, 'ip': '10.1.1.1',
                                  'port': 6000},
                                 None,
                                 {'id': 3, 'region': 0, 'zone': 2,
                                  'weight': 1.0, 'ip': '10.1.2.1',
                                  'port': 6000},
                                 {'id': 4, 'region': 0, 'zone': 2,
                                  'weight': 1.0, 'ip': '10.1.2.2',
                                  'port': 6000}]
        intended_devs = [{'id': 0, 'region': 0, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6000,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6000},
                         {'id': 1, 'region': 0, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6000,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6000},
                         None,
                         {'id': 3, 'region': 0, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.1', 'port': 6000,
                          'replication_ip': '10.1.2.1',
                          'replication_port': 6000},
                         {'id': 4, 'region': 0, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.2', 'port': 6000,
                          'replication_ip': '10.1.2.2',
                          'replication_port': 6000}]
        testgz = os.path.join(self.testdir, 'without_replication.ring.gz')
        ring.RingData(
            self.intended_replica2part2dev_id,
            replication_less_devs, self.intended_part_shift).save(testgz)
        self.ring = ring.Ring(
            self.testdir,
            reload_time=self.intended_reload_time,
            ring_name='without_replication')
        self.assertEquals(self.ring.devs, intended_devs)

    def test_reload_old_style_pickled_ring(self):
        devs = [{'id': 0, 'zone': 0,
                'weight': 1.0, 'ip': '10.1.1.1',
                'port': 6000},
                {'id': 1, 'zone': 0,
                 'weight': 1.0, 'ip': '10.1.1.1',
                 'port': 6000},
                None,
                {'id': 3, 'zone': 2,
                 'weight': 1.0, 'ip': '10.1.2.1',
                 'port': 6000},
                {'id': 4, 'zone': 2,
                 'weight': 1.0, 'ip': '10.1.2.2',
                 'port': 6000}]
        intended_devs = [{'id': 0, 'region': 1, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6000,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6000},
                         {'id': 1, 'region': 1, 'zone': 0, 'weight': 1.0,
                          'ip': '10.1.1.1', 'port': 6000,
                          'replication_ip': '10.1.1.1',
                          'replication_port': 6000},
                         None,
                         {'id': 3, 'region': 1, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.1', 'port': 6000,
                          'replication_ip': '10.1.2.1',
                          'replication_port': 6000},
                         {'id': 4, 'region': 1, 'zone': 2, 'weight': 1.0,
                          'ip': '10.1.2.2', 'port': 6000,
                          'replication_ip': '10.1.2.2',
                          'replication_port': 6000}]

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
        self.assertEquals(self.ring.devs, intended_devs)

    def test_get_part(self):
        part1 = self.ring.get_part('a')
        nodes1 = self.ring.get_part_nodes(part1)
        part2, nodes2 = self.ring.get_nodes('a')
        self.assertEquals(part1, part2)
        self.assertEquals(nodes1, nodes2)

    def test_get_part_nodes(self):
        part, nodes = self.ring.get_nodes('a')
        self.assertEquals(nodes, self.ring.get_part_nodes(part))

    def test_get_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results the ring produces, they know it.
        self.assertRaises(TypeError, self.ring.get_nodes)
        part, nodes = self.ring.get_nodes('a')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a1')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a4')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[1],
                          self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('aa')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[1],
                          self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('a', 'c1')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c0')
        self.assertEquals(part, 3)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[1],
                          self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('a', 'c3')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c2')
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o1')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[1],
                          self.intended_devs[4]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o5')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o0')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [dict(node, index=i) for i, node in
                          enumerate([self.intended_devs[0],
                          self.intended_devs[3]])])

    def add_dev_to_ring(self, new_dev):
        self.ring.devs.append(new_dev)
        self.ring._rebuild_tier_data()

    def test_get_more_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someone changes the results the ring produces, they know it.
        exp_part = 6
        exp_devs = [48, 93, 96]
        exp_zones = set([5, 8, 9])

        exp_handoffs = [11, 47, 25, 76, 69, 23, 99, 59, 106, 64, 43, 34, 88, 3,
                        30, 83, 16, 27, 103, 39, 60, 0, 8, 72, 56, 19, 91, 13,
                        84, 38, 66, 52, 78, 107, 50, 57, 31, 32, 77, 24, 42,
                        100, 71, 26, 9, 20, 35, 5, 14, 94, 28, 41, 18, 102,
                        101, 61, 95, 21, 81, 1, 105, 58, 74, 90, 86, 46, 4, 68,
                        40, 80, 54, 75, 45, 79, 44, 49, 62, 29, 7, 15, 70, 87,
                        65, 12, 82, 17, 104, 97, 55, 22, 6, 89, 2, 67, 37, 63,
                        53, 92, 33, 85, 73, 51, 98, 36, 10]
        exp_first_handoffs = [1, 37, 48, 68, 84, 75, 11, 101, 14, 73, 100, 75,
                              29, 19, 18, 101, 15, 99, 95, 24, 46, 82, 73, 62,
                              24, 89, 9, 22, 107, 74, 54, 63, 40, 106, 99, 83,
                              64, 73, 73, 106, 106, 80, 6, 25, 20, 33, 6, 79,
                              59, 42, 62, 24, 14, 107, 28, 0, 85, 5, 4, 12, 58,
                              11, 92, 18, 36, 56, 86, 1, 21, 33, 80, 97, 4, 81,
                              79, 76, 89, 50, 75, 27, 7, 96, 47, 55, 81, 104,
                              12, 5, 18, 106, 27, 93, 39, 92, 42, 30, 20, 88,
                              58, 105, 65, 29, 17, 52, 11, 106, 7, 24, 21, 91,
                              62, 52, 50, 31, 77, 102, 19, 11, 8, 58, 53, 20,
                              26, 8, 18, 82, 48, 68, 82, 89, 101, 50, 3, 52,
                              46, 11, 2, 30, 79, 66, 4, 61, 3, 56, 45, 102, 73,
                              84, 36, 19, 34, 84, 49, 40, 103, 66, 31, 33, 93,
                              33, 4, 52, 26, 58, 30, 47, 100, 57, 40, 79, 33,
                              107, 24, 20, 44, 4, 7, 59, 83, 101, 1, 56, 20,
                              61, 33, 16, 5, 74, 98, 4, 80, 15, 104, 52, 73,
                              18, 67, 75, 98, 73, 79, 68, 75, 27, 91, 36, 100,
                              52, 95, 37, 46, 70, 14, 47, 3, 70, 23, 40, 105,
                              62, 86, 48, 22, 54, 4, 72, 81, 13, 0, 18, 98,
                              101, 36, 29, 24, 39, 79, 97, 105, 28, 107, 47,
                              52, 101, 20, 22, 29, 65, 27, 7, 33, 64, 101, 60,
                              19, 55]
        rb = ring.RingBuilder(8, 3, 1)
        next_dev_id = 0
        for zone in xrange(1, 10):
            for server in xrange(1, 5):
                for device in xrange(1, 4):
                    rb.add_dev({'id': next_dev_id,
                                'ip': '1.2.%d.%d' % (zone, server),
                                'port': 1234, 'zone': zone, 'region': 0,
                                'weight': 1.0})
                    next_dev_id += 1
        rb.rebalance(seed=1)
        rb.get_ring().save(self.testgz)
        r = ring.Ring(self.testdir, ring_name='whatever')
        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEquals(part, exp_part)
        self.assertEquals([d['id'] for d in devs], exp_devs)
        self.assertEquals(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        self.assertEquals([d['id'] for d in devs], exp_handoffs)

        # The first 6 replicas plus the 3 primary nodes should cover all 9
        # zones in this test
        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEquals(seen_zones, set(range(1, 10)))

        # The first handoff nodes for each partition in the ring
        devs = []
        for part in xrange(r.partition_count):
            devs.append(r.get_more_nodes(part).next()['id'])
        self.assertEquals(devs, exp_first_handoffs)

        # Add a new device we can handoff to.
        zone = 5
        server = 0
        rb.add_dev({'id': next_dev_id,
                    'ip': '1.2.%d.%d' % (zone, server),
                    'port': 1234, 'zone': zone, 'region': 0, 'weight': 1.0})
        next_dev_id += 1
        rb.rebalance(seed=1)
        rb.get_ring().save(self.testgz)
        r = ring.Ring(self.testdir, ring_name='whatever')
        # We would change expectations here, but in this test no handoffs
        # changed at all.
        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEquals(part, exp_part)
        self.assertEquals([d['id'] for d in devs], exp_devs)
        self.assertEquals(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        dev_ids = [d['id'] for d in devs]
        self.assertEquals(len(dev_ids), len(exp_handoffs))
        for index, dev in enumerate(dev_ids):
            self.assertEquals(
                dev, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))

        # The handoffs still cover all the non-primary zones first
        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEquals(seen_zones, set(range(1, 10)))

        devs = []
        for part in xrange(r.partition_count):
            devs.append(r.get_more_nodes(part).next()['id'])
        for part in xrange(r.partition_count):
            self.assertEquals(
                devs[part], exp_first_handoffs[part],
                'handoff for partitition %d is now device id %d' % (
                    part, devs[part]))

        # Remove a device.
        rb.remove_dev(0)
        rb.rebalance(seed=1)
        rb.get_ring().save(self.testgz)
        r = ring.Ring(self.testdir, ring_name='whatever')
        # Change expectations
        # The long string of handoff nodes for the partition were the same for
        # the first 20, which is pretty good.
        exp_handoffs[20:] = [60, 108, 8, 72, 56, 19, 91, 13, 84, 38, 66, 52,
                             1, 78, 107, 50, 57, 31, 32, 77, 24, 42, 100, 71,
                             26, 9, 20, 35, 5, 14, 94, 28, 41, 18, 102, 101,
                             61, 95, 21, 81, 105, 58, 74, 90, 86, 46, 4, 68,
                             40, 80, 54, 75, 45, 79, 44, 49, 62, 29, 7, 15, 70,
                             87, 65, 12, 82, 17, 104, 97, 55, 22, 6, 89, 2, 67,
                             37, 63, 53, 92, 33, 85, 73, 51, 98, 36, 10]
        # Just a few of the first handoffs changed
        exp_first_handoffs[3] = 68
        exp_first_handoffs[55] = 104
        exp_first_handoffs[116] = 6
        exp_first_handoffs[181] = 15
        exp_first_handoffs[228] = 38
        # Test
        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEquals(part, exp_part)
        self.assertEquals([d['id'] for d in devs], exp_devs)
        self.assertEquals(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        dev_ids = [d['id'] for d in devs]
        self.assertEquals(len(dev_ids), len(exp_handoffs))
        for index, dev in enumerate(dev_ids):
            self.assertEquals(
                dev, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))

        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEquals(seen_zones, set(range(1, 10)))

        devs = []
        for part in xrange(r.partition_count):
            devs.append(r.get_more_nodes(part).next()['id'])
        for part in xrange(r.partition_count):
            self.assertEquals(
                devs[part], exp_first_handoffs[part],
                'handoff for partitition %d is now device id %d' % (
                    part, devs[part]))

        # Add a partial replica
        rb.set_replicas(3.5)
        rb.rebalance(seed=1)
        rb.get_ring().save(self.testgz)
        r = ring.Ring(self.testdir, ring_name='whatever')
        # Change expectations
        # We have another replica now
        exp_devs.append(47)
        exp_zones.add(4)
        # Caused some major changes in the sequence of handoffs for our test
        # partition, but at least the first stayed the same.
        exp_handoffs[1:] = [81, 25, 69, 23, 99, 59, 76, 3, 106, 64, 43, 13, 34,
                            88, 30, 16, 27, 103, 39, 74, 60, 108, 8, 56, 19,
                            91, 52, 84, 38, 66, 1, 78, 45, 107, 50, 57, 83, 31,
                            46, 32, 77, 24, 42, 63, 100, 72, 71, 7, 26, 9, 20,
                            35, 5, 87, 14, 94, 62, 28, 41, 90, 18, 82, 102, 22,
                            101, 61, 85, 95, 21, 98, 67, 105, 58, 86, 4, 79,
                            68, 40, 80, 54, 75, 44, 49, 6, 29, 15, 70, 65, 12,
                            17, 104, 97, 55, 89, 2, 37, 53, 92, 33, 73, 51, 36,
                            10]

        # Lots of first handoffs changed, but 30 of 256 is still just 11.72%.
        exp_first_handoffs[1] = 6
        exp_first_handoffs[4] = 104
        exp_first_handoffs[11] = 106
        exp_first_handoffs[17] = 13
        exp_first_handoffs[21] = 77
        exp_first_handoffs[22] = 95
        exp_first_handoffs[27] = 46
        exp_first_handoffs[29] = 65
        exp_first_handoffs[30] = 3
        exp_first_handoffs[31] = 20
        exp_first_handoffs[51] = 50
        exp_first_handoffs[53] = 8
        exp_first_handoffs[54] = 2
        exp_first_handoffs[72] = 107
        exp_first_handoffs[79] = 72
        exp_first_handoffs[85] = 71
        exp_first_handoffs[88] = 66
        exp_first_handoffs[92] = 29
        exp_first_handoffs[93] = 46
        exp_first_handoffs[96] = 38
        exp_first_handoffs[101] = 57
        exp_first_handoffs[103] = 87
        exp_first_handoffs[104] = 28
        exp_first_handoffs[107] = 1
        exp_first_handoffs[109] = 69
        exp_first_handoffs[110] = 50
        exp_first_handoffs[111] = 76
        exp_first_handoffs[115] = 47
        exp_first_handoffs[117] = 48
        exp_first_handoffs[119] = 7
        # Test
        part, devs = r.get_nodes('a', 'c', 'o')
        primary_zones = set([d['zone'] for d in devs])
        self.assertEquals(part, exp_part)
        self.assertEquals([d['id'] for d in devs], exp_devs)
        self.assertEquals(primary_zones, exp_zones)
        devs = list(r.get_more_nodes(part))
        dev_ids = [d['id'] for d in devs]
        self.assertEquals(len(dev_ids), len(exp_handoffs))

        for index, dev in enumerate(dev_ids):
            self.assertEquals(
                dev, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))

        seen_zones = set(primary_zones)
        seen_zones.update([d['zone'] for d in devs[:6]])
        self.assertEquals(seen_zones, set(range(1, 10)))

        devs = []
        for part in xrange(r.partition_count):
            devs.append(r.get_more_nodes(part).next()['id'])
        for part in xrange(r.partition_count):
            self.assertEquals(
                devs[part], exp_first_handoffs[part],
                'handoff for partitition %d is now device id %d' % (
                    part, devs[part]))

        # One last test of a partial replica partition
        exp_part2 = 136
        exp_devs2 = [52, 76, 97]
        exp_zones2 = set([9, 5, 7])
        exp_handoffs2 = [2, 67, 37, 92, 33, 23, 107, 63, 44, 103, 108, 85,
                         73, 10, 89, 80, 4, 17, 49, 32, 12, 41, 58, 20, 25,
                         61, 94, 47, 69, 56, 101, 28, 83, 8, 96, 53, 51, 42,
                         98, 35, 36, 84, 43, 104, 31, 65, 1, 40, 9, 74, 95,
                         45, 5, 71, 86, 78, 30, 93, 48, 91, 15, 88, 39, 18,
                         57, 72, 70, 27, 54, 16, 24, 21, 14, 11, 77, 62, 50,
                         6, 105, 26, 55, 29, 60, 34, 13, 87, 59, 38, 99, 75,
                         106, 3, 82, 66, 79, 7, 46, 64, 81, 22, 68, 19, 102,
                         90, 100]

        part2, devs2 = r.get_nodes('a', 'c', 'o2')
        primary_zones2 = set([d['zone'] for d in devs2])
        self.assertEquals(part2, exp_part2)
        self.assertEquals([d['id'] for d in devs2], exp_devs2)
        self.assertEquals(primary_zones2, exp_zones2)
        devs2 = list(r.get_more_nodes(part2))
        dev_ids2 = [d['id'] for d in devs2]

        self.assertEquals(len(dev_ids2), len(exp_handoffs2))
        for index, dev in enumerate(dev_ids2):
            self.assertEquals(
                dev, exp_handoffs2[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids2[index:], exp_handoffs2[index:]))

        seen_zones = set(primary_zones2)
        seen_zones.update([d['zone'] for d in devs2[:6]])
        self.assertEquals(seen_zones, set(range(1, 10)))

        # Test distribution across regions
        rb.set_replicas(3)
        for region in xrange(1, 5):
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
        self.assertEquals(seen_regions, set(range(0, 5)))

        # There are 13 zones now, so the first 13 nodes should all have
        # distinct zones (that's r0z0, r0z1, ..., r0z8, r1z1, r2z1, r3z1, and
        # r4z1).
        seen_zones = set(primary_zones)
        seen_zones.update([(d['region'], d['zone']) for d in more_devs[:10]])
        self.assertEquals(13, len(seen_zones))

        # Here's a brittle canary-in-the-coalmine test to make sure the region
        # handoff computation didn't change accidentally
        exp_handoffs = [111, 112, 74, 54, 93, 31, 2, 43, 100, 22, 71, 92, 35,
                        9, 50, 41, 76, 80, 84, 88, 17, 96, 6, 102, 37, 29,
                        105, 5, 47, 20, 13, 108, 66, 81, 53, 65, 25, 58, 32,
                        94, 101, 1, 10, 44, 73, 75, 21, 97, 28, 106, 30, 16,
                        39, 77, 42, 72, 34, 99, 14, 61, 90, 4, 40, 3, 45, 62,
                        7, 15, 87, 12, 83, 89, 33, 98, 49, 107, 56, 86, 48,
                        57, 24, 11, 23, 26, 46, 64, 69, 38, 36, 79, 63, 104,
                        51, 70, 82, 67, 68, 8, 95, 91, 55, 59, 85]
        dev_ids = [d['id'] for d in more_devs]

        self.assertEquals(len(dev_ids), len(exp_handoffs))
        for index, dev_id in enumerate(dev_ids):
            self.assertEquals(
                dev_id, exp_handoffs[index],
                'handoff differs at position %d\n%s\n%s' % (
                    index, dev_ids[index:], exp_handoffs[index:]))


if __name__ == '__main__':
    unittest.main()
