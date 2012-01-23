# Copyright (c) 2010-2011 OpenStack, LLC.
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

import os
import unittest
from shutil import rmtree

from swift.common import exceptions
from swift.common import ring
from swift.common.ring import RingBuilder, RingData

class TestRingBuilder(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(os.path.dirname(__file__),
                                    'ring_builder')
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def test_init(self):
        rb = ring.RingBuilder(8, 3, 1)
        self.assertEquals(rb.part_power, 8)
        self.assertEquals(rb.replicas, 3)
        self.assertEquals(rb.min_part_hours, 1)
        self.assertEquals(rb.parts, 2**8)
        self.assertEquals(rb.devs, [])
        self.assertEquals(rb.devs_changed, False)
        self.assertEquals(rb.version, 0)

    def test_get_ring(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10004, 'device': 'sda1'})
        rb.remove_dev(1)
        rb.rebalance()
        r = rb.get_ring()
        self.assert_(isinstance(r, ring.RingData))
        r2 = rb.get_ring()
        self.assert_(r is r2)
        rb.rebalance()
        r3 = rb.get_ring()
        self.assert_(r3 is not r2)
        r4 = rb.get_ring()
        self.assert_(r3 is r4)

    def test_add_dev(self):
        rb = ring.RingBuilder(8, 3, 1)
        dev = \
            {'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1', 'port': 10000}
        rb.add_dev(dev)
        self.assertRaises(exceptions.DuplicateDeviceError, rb.add_dev, dev)

    def test_set_dev_weight(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 0.5, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 0, 'weight': 0.5, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1'})
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 128, 1: 128, 2: 256, 3: 256})
        rb.set_dev_weight(0, 0.75)
        rb.set_dev_weight(1, 0.25)
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 192, 1: 64, 2: 256, 3: 256})

    def test_remove_dev(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'zone': 3, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1'})
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 192, 1: 192, 2: 192, 3: 192})
        rb.remove_dev(1)
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 256, 2: 256, 3: 256})

    def test_shuffled_gather(self):
        if self._shuffled_gather_helper() and \
            self._shuffled_gather_helper():
                raise AssertionError('It is highly likely the ring is no '
                    'longer shuffling the set of partitions to reassign on a '
                    'rebalance.')

    def _shuffled_gather_helper(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1'})
        rb.rebalance()
        rb.add_dev({'id': 3, 'zone': 3, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        parts = rb._gather_reassign_parts()
        max_run = 0
        run = 0
        last_part = 0
        for part in parts:
            if part > last_part:
                run += 1
            else:
                if run > max_run:
                    max_run = run
                run = 0
            last_part = part
        if run > max_run:
            max_run = run
        return max_run > len(parts) / 2

    def test_rerebalance(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1'})
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 256, 1: 256, 2: 256})
        rb.add_dev({'id': 3, 'zone': 3, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 192, 1: 192, 2: 192, 3: 192})
        rb.set_dev_weight(3, 100)
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts[3], 256)

    def test_rerebalance_mirrored(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1', 'mirror_copies': 2})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1', 'mirror_copies': 2})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1', 'mirror_copies': 2})
        rb.rebalance()
        self.assertEqual(rb.max_dev_repeat_count, 2)
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 256, 1: 256, 2: 256})
        rb.add_dev({'id': 3, 'zone': 3, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1', 'mirror_copies': 1})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        for i in xrange(3):
            self.assertTrue(counts[i] > counts[3])
            self.assertTrue(counts[i] in range(219, 223))
        self.assertTrue(counts[3] in range(105, 110))
        rb.remove_dev(3)
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        rb.add_dev({'id': 3, 'zone': 3, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1', 'mirror_copies': 2})
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        for i in counts:
            self.assertTrue(counts[i] in range(190, 195))

    def test_add_rebalance_add_rebalance_delete_rebalance(self):
        """ Test for https://bugs.launchpad.net/swift/+bug/845952 """
        # min_part of 0 to allow for rapid rebalancing
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1'})

        rb.rebalance()

        rb.add_dev({'id': 3, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10004, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10005, 'device': 'sda1'})

        rb.rebalance()

        rb.remove_dev(1)

        rb.rebalance()

    def test_validate(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 2, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'zone': 3, 'weight': 2, 'ip': '127.0.0.1',
                    'port': 10003, 'device': 'sda1'})
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEquals(counts, {0: 128, 1: 128, 2: 256, 3: 256})

        dev_usage, worst = rb.validate()
        self.assert_(dev_usage is None)
        self.assert_(worst is None)

        dev_usage, worst = rb.validate(stats=True)
        self.assertEquals(list(dev_usage), [128, 128, 256, 256])
        self.assertEquals(int(worst), 0)

        rb.set_dev_weight(2, 0)
        rb.rebalance()
        self.assertEquals(rb.validate(stats=True)[1], 999.99)

        # Test not all partitions doubly accounted for
        rb.devs[1]['parts'] -= 1
        self.assertRaises(exceptions.RingValidationError, rb.validate)
        rb.devs[1]['parts'] += 1

        # Test duplicate device for partition
        orig_dev_id = rb._replica2part2dev[0][0]
        rb._replica2part2dev[0][0] = rb._replica2part2dev[1][0]
        self.assertRaises(exceptions.RingValidationError, rb.validate)
        rb._replica2part2dev[0][0] = orig_dev_id

        # Test duplicate zone for partition
        rb.add_dev({'id': 5, 'zone': 0, 'weight': 2, 'ip': '127.0.0.1',
                    'port': 10005, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        rb.validate()
        orig_replica = orig_partition = orig_device = None
        for part2dev in rb._replica2part2dev:
            for p in xrange(2**8):
                if part2dev[p] == 5:
                    for r in xrange(len(rb._replica2part2dev)):
                        if rb._replica2part2dev[r][p] != 5:
                            orig_replica = r
                            orig_partition = p
                            orig_device = rb._replica2part2dev[r][p]
                            rb._replica2part2dev[r][p] = 0
                            break
                if orig_replica is not None:
                    break
            if orig_replica is not None:
                break
        self.assertRaises(exceptions.RingValidationError, rb.validate)
        rb._replica2part2dev[orig_replica][orig_partition] = orig_device

        # Tests that validate can handle 'holes' in .devs
        rb.remove_dev(2)
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        rb.validate(stats=True)

        # Validate that zero weight devices with no partitions don't count on
        # the 'worst' value.
        self.assertNotEquals(rb.validate(stats=True)[1], 999.99)
        rb.add_dev({'id': 4, 'zone': 0, 'weight': 0, 'ip': '127.0.0.1',
                    'port': 10004, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        self.assertNotEquals(rb.validate(stats=True)[1], 999.99)

    def test_max_mirror_dev_repeat_count(self):
        def add_devs(rb, mirror_copies):
            for i, mc in enumerate(mirror_copies):
                rb.add_dev({'id': i, 'zone': i, 'weight': 1, 'ip': '127.0.0.1',
                            'port': 10000 + i, 'device': 'sda1',
                            'mirror_copies': mc})

        rb = ring.RingBuilder(8, 2, 0)
        add_devs(rb, [2] * 2)
        self.assertEqual(rb.max_dev_repeat_count, 1)

        for i in range(3, 0, -1):
            rb = ring.RingBuilder(8, 3, 0)
            add_devs(rb, [2] * (3 - i) + [1] * i)
            self.assertEqual(rb.max_dev_repeat_count, 1)

        rb = ring.RingBuilder(8, 3, 0)
        add_devs(rb, [3] * 2 + [1])
        self.assertEqual(rb.max_dev_repeat_count, 1)

        rb = ring.RingBuilder(8, 3, 0)
        add_devs(rb, [2] * 3)
        self.assertEqual(rb.max_dev_repeat_count, 2)

        rb = ring.RingBuilder(8, 3, 0)
        add_devs(rb, [3] * 3)
        self.assertEqual(rb.max_dev_repeat_count, 2)

        for i in range(3, 1, -1):
            rb = ring.RingBuilder(8, 4, 0)
            add_devs(rb, [2] * (4 - i) + [1] * i)
            self.assertEqual(rb.max_dev_repeat_count, 1)

        rb = ring.RingBuilder(8, 4, 0)
        add_devs(rb, [2] * 3 + [1])
        self.assertEqual(rb.max_dev_repeat_count, 2)

        rb = ring.RingBuilder(8, 4, 0)
        add_devs(rb, [2] * 4)
        self.assertEqual(rb.max_dev_repeat_count, 2)

        rb = ring.RingBuilder(8, 4, 0)
        add_devs(rb, [3] * 4)
        self.assertEqual(rb.max_dev_repeat_count, 3)

if __name__ == '__main__':
    unittest.main()
