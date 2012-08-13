# Copyright (c) 2010-2012 OpenStack, LLC.
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
import unittest
from gzip import GzipFile
from shutil import rmtree
from time import sleep, time

from swift.common import ring, utils


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
        d = [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}]
        s = 30
        rd = ring.RingData(r2p2d, d, s)
        self.assertEquals(rd._replica2part2dev_id, r2p2d)
        self.assertEquals(rd.devs, d)
        self.assertEquals(rd._part_shift, s)

    def test_can_load_pickled_ring_data(self):
        rd = ring.RingData([[0, 1, 0, 1], [0, 1, 0, 1]],
                [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        for p in xrange(pickle.HIGHEST_PROTOCOL):
            pickle.dump(rd, GzipFile(ring_fname, 'wb'), protocol=p)
            ring_data = ring.RingData.load(ring_fname)
            self.assert_ring_data_equal(rd, ring_data)

    def test_roundtrip_serialization(self):
        ring_fname = os.path.join(self.testdir, 'foo.ring.gz')
        rd = ring.RingData(
            [array.array('H', [0, 1, 0, 1]), array.array('H',[0, 1, 0, 1])],
            [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        rd.save(ring_fname)
        rd2 = ring.RingData.load(ring_fname)
        self.assert_ring_data_equal(rd, rd2)


class TestRing(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir = os.path.join(os.path.dirname(__file__), 'ring')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        self.testgz = os.path.join(self.testdir, 'whatever.ring.gz')
        self.intended_replica2part2dev_id = [
            array.array('H', [0, 1, 0, 1]),
            array.array('H', [0, 1, 0, 1]),
            array.array('H', [3, 4, 3, 4])]
        self.intended_devs = [{'id': 0, 'zone': 0, 'weight': 1.0,
                               'ip': '10.1.1.1', 'port': 6000},
                              {'id': 1, 'zone': 0, 'weight': 1.0,
                               'ip': '10.1.1.1', 'port': 6000},
                              None,
                              {'id': 3, 'zone': 2, 'weight': 1.0,
                               'ip': '10.1.2.1', 'port': 6000},
                              {'id': 4, 'zone': 2, 'weight': 1.0,
                               'ip': '10.1.2.2', 'port': 6000}]
        self.intended_part_shift = 30
        self.intended_reload_time = 15
        ring.RingData(self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift).save(self.testgz)
        self.ring = ring.Ring(self.testdir,
            reload_time=self.intended_reload_time, ring_name='whatever')

    def tearDown(self):
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
        try:
            utils.HASH_PATH_SUFFIX = ''
            self.assertRaises(SystemExit, ring.Ring, self.testdir, 'whatever')
        finally:
            utils.HASH_PATH_SUFFIX = _orig_hash_path_suffix

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
        self.intended_devs.append({'id': 3, 'zone': 3, 'weight': 1.0})
        ring.RingData(self.intended_replica2part2dev_id,
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
        self.intended_devs.append({'id': 5, 'zone': 4, 'weight': 1.0})
        ring.RingData(self.intended_replica2part2dev_id,
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
        self.intended_devs.append({'id': 6, 'zone': 5, 'weight': 1.0})
        ring.RingData(self.intended_replica2part2dev_id,
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
        self.intended_devs.append({'id': 5, 'zone': 4, 'weight': 1.0})
        ring.RingData(self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift).save(self.testgz)
        sleep(0.1)
        self.assertEquals(len(self.ring.devs), 9)
        self.assertNotEquals(self.ring._mtime, orig_mtime)

    def test_get_part_nodes(self):
        part, nodes = self.ring.get_nodes('a')
        self.assertEquals(nodes, self.ring.get_part_nodes(part))

    def test_get_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results the ring produces, they know it.
        self.assertRaises(TypeError, self.ring.get_nodes)
        part, nodes = self.ring.get_nodes('a')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

        part, nodes = self.ring.get_nodes('a1')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

        part, nodes = self.ring.get_nodes('a4')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [self.intended_devs[1],
                                  self.intended_devs[4]])

        part, nodes = self.ring.get_nodes('aa')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [self.intended_devs[1],
                                  self.intended_devs[4]])

        part, nodes = self.ring.get_nodes('a', 'c1')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

        part, nodes = self.ring.get_nodes('a', 'c0')
        self.assertEquals(part, 3)
        self.assertEquals(nodes, [self.intended_devs[1],
                                  self.intended_devs[4]])

        part, nodes = self.ring.get_nodes('a', 'c3')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

        part, nodes = self.ring.get_nodes('a', 'c2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

        part, nodes = self.ring.get_nodes('a', 'c', 'o1')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [self.intended_devs[1],
                                  self.intended_devs[4]])

        part, nodes = self.ring.get_nodes('a', 'c', 'o5')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

        part, nodes = self.ring.get_nodes('a', 'c', 'o0')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])

    def add_dev_to_ring(self, new_dev):
        self.ring.devs.append(new_dev)
        self.ring._rebuild_tier_data()

    def test_get_more_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someone changes the results the ring produces, they know it.
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [self.intended_devs[4],
                                  self.intended_devs[1]])

        new_dev = {'id': 5, 'zone': 0, 'weight': 1.0,
                   'ip': '10.1.1.1', 'port': 6000}
        self.add_dev_to_ring(new_dev)
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [self.intended_devs[4],
                                  new_dev,
                                  self.intended_devs[1]])

        self.ring.devs[5]['zone'] = 3
        self.ring._rebuild_tier_data()
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [new_dev,
                                  self.intended_devs[4],
                                  self.intended_devs[1]])

        self.ring.devs.append(None)
        new_dev2 = {'id': 6, 'zone': 6, 'weight': 1.0,
                    'ip': '10.1.6.1', 'port': 6000}
        self.add_dev_to_ring(new_dev2)
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [self.intended_devs[0],
                                  self.intended_devs[3]])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [new_dev,
                                  new_dev2,
                                  self.intended_devs[4],
                                  self.intended_devs[1]])

        new_dev3 = {'id': 7, 'zone': 7, 'weight': 1.0,
                    'ip': '10.1.7.1', 'port': 6000}
        self.add_dev_to_ring(new_dev3)
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [new_dev, new_dev2, new_dev3,
                                  self.intended_devs[4],
                                  self.intended_devs[1]])
        new_dev3['weight'] = 0
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [new_dev, new_dev2,
                                  self.intended_devs[4],
                                  self.intended_devs[1]])
        self.ring.devs[7]['weight'] = 1.0

        new_dev4 = {'id': 8, 'zone': 8, 'weight': 0.0,
                    'ip': '10.1.8.1', 'port': 6000}
        self.add_dev_to_ring(new_dev4)
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [new_dev, new_dev2,
                                  self.intended_devs[4],
                                  self.intended_devs[1]])


if __name__ == '__main__':
    unittest.main()
