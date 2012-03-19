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

import cPickle as pickle
import os
import unittest
from gzip import GzipFile
from shutil import rmtree
from time import sleep, time

from swift.common import ring, utils


class TestRingData(unittest.TestCase):

    def test_attrs(self):
        r2p2d = [[0, 1, 0, 1], [0, 1, 0, 1]]
        d = [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}]
        s = 30
        rd = ring.RingData(r2p2d, d, s)
        self.assertEquals(rd._replica2part2dev_id, r2p2d)
        self.assertEquals(rd.devs, d)
        self.assertEquals(rd._part_shift, s)

    def test_pickleable(self):
        rd = ring.RingData([[0, 1, 0, 1], [0, 1, 0, 1]],
                [{'id': 0, 'zone': 0}, {'id': 1, 'zone': 1}], 30)
        for p in xrange(pickle.HIGHEST_PROTOCOL):
            pickle.loads(pickle.dumps(rd, protocol=p))


class TestRing(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        self.testdir = os.path.join(os.path.dirname(__file__), 'ring')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        self.testgz = os.path.join(self.testdir, 'ring.gz')
        self.intended_replica2part2dev_id = [[0, 2, 0, 2], [2, 0, 2, 0]]
        self.intended_devs = [{'id': 0, 'zone': 0, 'weight': 1.0}, None,
                              {'id': 2, 'zone': 2, 'weight': 1.0}]
        self.intended_part_shift = 30
        self.intended_reload_time = 15
        pickle.dump(ring.RingData(self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift),
            GzipFile(self.testgz, 'wb'))
        self.ring = \
            ring.Ring(self.testgz, reload_time=self.intended_reload_time)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_creation(self):
        self.assertEquals(self.ring._replica2part2dev_id,
                          self.intended_replica2part2dev_id)
        self.assertEquals(self.ring._part_shift, self.intended_part_shift)
        self.assertEquals(self.ring.devs, self.intended_devs)
        self.assertEquals(self.ring.reload_time, self.intended_reload_time)
        self.assertEquals(self.ring.pickle_gz_path, self.testgz)
        # test invalid endcap
        _orig_hash_path_suffix = utils.HASH_PATH_SUFFIX
        try:
            utils.HASH_PATH_SUFFIX = ''
            self.assertRaises(SystemExit, ring.Ring, self.testgz)
        finally:
            utils.HASH_PATH_SUFFIX = _orig_hash_path_suffix

    def test_has_changed(self):
        self.assertEquals(self.ring.has_changed(), False)
        os.utime(self.testgz, (time() + 60, time() + 60))
        self.assertEquals(self.ring.has_changed(), True)

    def test_reload(self):
        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testgz, reload_time=0.001)
        orig_mtime = self.ring._mtime
        self.assertEquals(len(self.ring.devs), 3)
        self.intended_devs.append({'id': 3, 'zone': 3, 'weight': 1.0})
        pickle.dump(ring.RingData(self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift),
            GzipFile(self.testgz, 'wb'))
        sleep(0.1)
        self.ring.get_nodes('a')
        self.assertEquals(len(self.ring.devs), 4)
        self.assertNotEquals(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = ring.Ring(self.testgz, reload_time=0.001)
        orig_mtime = self.ring._mtime
        self.assertEquals(len(self.ring.devs), 4)
        self.intended_devs.append({'id': 4, 'zone': 4, 'weight': 1.0})
        pickle.dump(ring.RingData(self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift),
            GzipFile(self.testgz, 'wb'))
        sleep(0.1)
        self.ring.get_part_nodes(0)
        self.assertEquals(len(self.ring.devs), 5)
        self.assertNotEquals(self.ring._mtime, orig_mtime)

        os.utime(self.testgz, (time() - 300, time() - 300))
        self.ring = \
            ring.Ring(self.testgz, reload_time=0.001)
        orig_mtime = self.ring._mtime
        part, nodes = self.ring.get_nodes('a')
        self.assertEquals(len(self.ring.devs), 5)
        self.intended_devs.append({'id': 5, 'zone': 5, 'weight': 1.0})
        pickle.dump(ring.RingData(self.intended_replica2part2dev_id,
            self.intended_devs, self.intended_part_shift),
            GzipFile(self.testgz, 'wb'))
        sleep(0.1)
        self.ring.get_more_nodes(part).next()
        self.assertEquals(len(self.ring.devs), 6)
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
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a1')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a4')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [{'id': 2, 'zone': 2, 'weight': 1.0},
                                  {'id': 0, 'zone': 0, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('aa')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [{'id': 2, 'zone': 2, 'weight': 1.0},
                                  {'id': 0, 'zone': 0, 'weight': 1.0}])

        part, nodes = self.ring.get_nodes('a', 'c1')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a', 'c0')
        self.assertEquals(part, 3)
        self.assertEquals(nodes, [{'id': 2, 'zone': 2, 'weight': 1.0},
                                  {'id': 0, 'zone': 0, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a', 'c3')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a', 'c2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])

        part, nodes = self.ring.get_nodes('a', 'c', 'o1')
        self.assertEquals(part, 1)
        self.assertEquals(nodes, [{'id': 2, 'zone': 2, 'weight': 1.0},
                                  {'id': 0, 'zone': 0, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a', 'c', 'o5')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a', 'c', 'o0')
        self.assertEquals(part, 0)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])

    def test_get_more_nodes(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someone changes the results the ring produces, they know it.
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [])

        self.ring.devs.append({'id': 3, 'zone': 0, 'weight': 1.0})
        self.ring.zone2devs[0].append(self.ring.devs[3])
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [])

        self.ring.zone2devs[self.ring.devs[3]['zone']].remove(self.ring.devs[3])
        self.ring.devs[3]['zone'] = 3
        self.ring.zone2devs[3] = [self.ring.devs[3]]
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [{'id': 3, 'zone': 3, 'weight': 1.0}])

        self.ring.devs.append(None)
        self.ring.devs.append({'id': 5, 'zone': 5, 'weight': 1.0})
        self.ring.zone2devs[5] = [self.ring.devs[5]]
        part, nodes = self.ring.get_nodes('a', 'c', 'o2')
        self.assertEquals(part, 2)
        self.assertEquals(nodes, [{'id': 0, 'zone': 0, 'weight': 1.0},
                                  {'id': 2, 'zone': 2, 'weight': 1.0}])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [{'id': 3, 'zone': 3, 'weight': 1.0},
                                  {'id': 5, 'zone': 5, 'weight': 1.0}])

        self.ring.devs.append({'id': 6, 'zone': 5, 'weight': 1.0})
        self.ring.zone2devs[5].append(self.ring.devs[6])
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [{'id': 3, 'zone': 3, 'weight': 1.0},
                                  {'id': 5, 'zone': 5, 'weight': 1.0}])
        self.ring.devs[5]['weight'] = 0
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [{'id': 3, 'zone': 3, 'weight': 1.0},
                                  {'id': 6, 'zone': 5, 'weight': 1.0}])
        self.ring.devs[3]['weight'] = 0
        self.ring.devs.append({'id': 7, 'zone': 6, 'weight': 0.0})
        self.ring.zone2devs[6] = [self.ring.devs[7]]
        nodes = list(self.ring.get_more_nodes(part))
        self.assertEquals(nodes, [{'id': 6, 'zone': 5, 'weight': 1.0}])


if __name__ == '__main__':
    unittest.main()
