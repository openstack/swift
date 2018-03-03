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

import copy
import errno
import mock
import operator
import os
import unittest
import six.moves.cPickle as pickle
from array import array
from collections import Counter, defaultdict
from math import ceil
from tempfile import mkdtemp
from shutil import rmtree
import sys
import random
import uuid
import itertools

from six.moves import range

from swift.common import exceptions
from swift.common import ring
from swift.common.ring import utils
from swift.common.ring.builder import MAX_BALANCE


def _partition_counts(builder, key='id'):
    """
    Returns a dictionary mapping the given device key to (number of
    partitions assigned to that key).
    """
    return Counter(builder.devs[dev_id][key]
                   for part2dev_id in builder._replica2part2dev
                   for dev_id in part2dev_id)


class TestRingBuilder(unittest.TestCase):

    def setUp(self):
        self.testdir = mkdtemp()

    def tearDown(self):
        rmtree(self.testdir, ignore_errors=1)

    def _get_population_by_region(self, builder):
        """
        Returns a dictionary mapping region to number of partitions in that
        region.
        """
        return _partition_counts(builder, key='region')

    def test_init(self):
        rb = ring.RingBuilder(8, 3, 1)
        self.assertEqual(rb.part_power, 8)
        self.assertEqual(rb.replicas, 3)
        self.assertEqual(rb.min_part_hours, 1)
        self.assertEqual(rb.parts, 2 ** 8)
        self.assertEqual(rb.devs, [])
        self.assertFalse(rb.devs_changed)
        self.assertEqual(rb.version, 0)
        self.assertIsNotNone(rb._last_part_moves)

    def test_overlarge_part_powers(self):
        expected_msg = 'part_power must be at most 32 (was 33)'
        with self.assertRaises(ValueError) as ctx:
            ring.RingBuilder(33, 3, 1)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_insufficient_replicas(self):
        expected_msg = 'replicas must be at least 1 (was 0.999000)'
        with self.assertRaises(ValueError) as ctx:
            ring.RingBuilder(8, 0.999, 1)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_negative_min_part_hours(self):
        expected_msg = 'min_part_hours must be non-negative (was -1)'
        with self.assertRaises(ValueError) as ctx:
            ring.RingBuilder(8, 3, -1)
        self.assertEqual(str(ctx.exception), expected_msg)

    def test_deepcopy(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})

        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sdb1'})

        # more devices in zone #1
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sdc1'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sdd1'})
        rb.rebalance()
        rb_copy = copy.deepcopy(rb)

        self.assertEqual(rb.to_dict(), rb_copy.to_dict())
        self.assertIsNot(rb.devs, rb_copy.devs)
        self.assertIsNot(rb._replica2part2dev, rb_copy._replica2part2dev)
        self.assertIsNot(rb._last_part_moves, rb_copy._last_part_moves)
        self.assertIsNot(rb._remove_devs, rb_copy._remove_devs)
        self.assertIsNot(rb._dispersion_graph, rb_copy._dispersion_graph)

    def test_get_ring(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})
        rb.remove_dev(1)
        rb.rebalance()
        r = rb.get_ring()
        self.assertIsInstance(r, ring.RingData)
        r2 = rb.get_ring()
        self.assertIs(r, r2)
        rb.rebalance()
        r3 = rb.get_ring()
        self.assertIsNot(r3, r2)
        r4 = rb.get_ring()
        self.assertIs(r3, r4)

    def test_rebalance_with_seed(self):
        devs = [(0, 10000), (1, 10001), (2, 10002), (1, 10003)]
        ring_builders = []
        for n in range(3):
            rb = ring.RingBuilder(8, 3, 1)
            idx = 0
            for zone, port in devs:
                for d in ('sda1', 'sdb1'):
                    rb.add_dev({'id': idx, 'region': 0, 'zone': zone,
                                'ip': '127.0.0.1', 'port': port,
                                'device': d, 'weight': 1})
                    idx += 1
            ring_builders.append(rb)

        rb0 = ring_builders[0]
        rb1 = ring_builders[1]
        rb2 = ring_builders[2]

        r0 = rb0.get_ring()
        self.assertIs(rb0.get_ring(), r0)

        rb0.rebalance()  # NO SEED
        rb1.rebalance(seed=10)
        rb2.rebalance(seed=10)

        r1 = rb1.get_ring()
        r2 = rb2.get_ring()

        self.assertIsNot(rb0.get_ring(), r0)
        self.assertNotEqual(r0.to_dict(), r1.to_dict())
        self.assertEqual(r1.to_dict(), r2.to_dict())

        # check that random state is reset
        pre_state = random.getstate()
        rb2.rebalance(seed=10)
        self.assertEqual(pre_state, random.getstate(),
                         "Random state was not reset")

        pre_state = random.getstate()
        with mock.patch.object(rb2, "_build_replica_plan",
                               side_effect=Exception()):
            self.assertRaises(Exception, rb2.rebalance, seed=10)
        self.assertEqual(pre_state, random.getstate(),
                         "Random state was not reset")

    def test_rebalance_part_on_deleted_other_part_on_drained(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10005, 'device': 'sda1'})

        rb.rebalance(seed=1)
        # We want a partition where 1 replica is on a removed device, 1
        # replica is on a 0-weight device, and 1 on a normal device. To
        # guarantee we have one, we see where partition 123 is, then
        # manipulate its devices accordingly.
        zero_weight_dev_id = rb._replica2part2dev[1][123]
        delete_dev_id = rb._replica2part2dev[2][123]

        rb.set_dev_weight(zero_weight_dev_id, 0.0)
        rb.remove_dev(delete_dev_id)
        rb.rebalance()

    def test_set_replicas(self):
        rb = ring.RingBuilder(8, 3.2, 1)
        rb.devs_changed = False
        rb.set_replicas(3.25)
        self.assertTrue(rb.devs_changed)

        rb.devs_changed = False
        rb.set_replicas(3.2500001)
        self.assertFalse(rb.devs_changed)

    def test_add_dev(self):
        rb = ring.RingBuilder(8, 3, 1)
        dev = {'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
               'ip': '127.0.0.1', 'port': 10000}
        dev_id = rb.add_dev(dev)
        self.assertRaises(exceptions.DuplicateDeviceError, rb.add_dev, dev)
        self.assertEqual(dev_id, 0)
        rb = ring.RingBuilder(8, 3, 1)
        # test add new dev with no id
        dev_id = rb.add_dev({'zone': 0, 'region': 1, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 6200})
        self.assertEqual(rb.devs[0]['id'], 0)
        self.assertEqual(dev_id, 0)
        # test add another dev with no id
        dev_id = rb.add_dev({'zone': 3, 'region': 2, 'weight': 1,
                             'ip': '127.0.0.1', 'port': 6200})
        self.assertEqual(rb.devs[1]['id'], 1)
        self.assertEqual(dev_id, 1)
        # some keys are required
        self.assertRaises(ValueError, rb.add_dev, {})
        stub_dev = {'weight': 1, 'ip': '127.0.0.1', 'port': 7000}
        for key in (stub_dev.keys()):
            dev = stub_dev.copy()
            dev.pop(key)
            self.assertRaises(ValueError, rb.add_dev, dev)

    def test_set_dev_weight(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEqual(counts, {0: 128, 1: 128, 2: 256, 3: 256})
        rb.set_dev_weight(0, 0.75)
        rb.set_dev_weight(1, 0.25)
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEqual(counts, {0: 192, 1: 64, 2: 256, 3: 256})

    def test_remove_dev(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 3, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEqual(counts, {0: 192, 1: 192, 2: 192, 3: 192})
        rb.remove_dev(1)
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEqual(counts, {0: 256, 2: 256, 3: 256})

    def test_round_off_error(self):
        # 3 nodes with 11 disks each is particularly problematic. Probably has
        # to do with the binary repr. of 1/33? Those ones look suspicious...
        #
        #   >>> bin(int(struct.pack('!f', 1.0/(33)).encode('hex'), 16))
        #   '0b111100111110000011111000010000'
        rb = ring.RingBuilder(8, 3, 1)
        for dev_id, (region, zone) in enumerate(
                11 * [(0, 0), (1, 10), (1, 11)]):
            rb.add_dev({'id': dev_id, 'region': region, 'zone': zone,
                        'weight': 1, 'ip': '127.0.0.1',
                        'port': 10000 + region * 100 + zone,
                        'device': 'sda%d' % dev_id})
        rb.rebalance()
        self.assertEqual(_partition_counts(rb, 'zone'),
                         {0: 256, 10: 256, 11: 256})
        wanted_by_zone = defaultdict(lambda: defaultdict(int))
        for dev in rb._iter_devs():
            wanted_by_zone[dev['zone']][dev['parts_wanted']] += 1
        # We're nicely balanced, but parts_wanted is slightly lumpy
        # because reasons.
        self.assertEqual(wanted_by_zone, {
            0: {0: 10, 1: 1},
            10: {0: 11},
            11: {0: 10, -1: 1}})

    def test_remove_a_lot(self):
        rb = ring.RingBuilder(3, 3, 1)
        rb.add_dev({'id': 0, 'device': 'd0', 'ip': '10.0.0.1',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 1})
        rb.add_dev({'id': 1, 'device': 'd1', 'ip': '10.0.0.2',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 2})
        rb.add_dev({'id': 2, 'device': 'd2', 'ip': '10.0.0.3',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 3})
        rb.add_dev({'id': 3, 'device': 'd3', 'ip': '10.0.0.1',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 1})
        rb.add_dev({'id': 4, 'device': 'd4', 'ip': '10.0.0.2',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 2})
        rb.add_dev({'id': 5, 'device': 'd5', 'ip': '10.0.0.3',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 3})
        rb.rebalance()
        rb.validate()

        # this has to put more than 1/3 of the partitions in the
        # cluster on removed devices in order to ensure that at least
        # one partition has multiple replicas that need to move.
        #
        # (for an N-replica ring, it's more than 1/N of the
        # partitions, of course)
        rb.remove_dev(3)
        rb.remove_dev(4)
        rb.remove_dev(5)

        rb.rebalance()
        rb.validate()

    def test_remove_zero_weighted(self):
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'device': 'd0', 'ip': '10.0.0.1',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 1})
        rb.add_dev({'id': 1, 'device': 'd1', 'ip': '10.0.0.2',
                    'port': 6202, 'weight': 0.0, 'region': 0, 'zone': 2})
        rb.add_dev({'id': 2, 'device': 'd2', 'ip': '10.0.0.3',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 3})
        rb.add_dev({'id': 3, 'device': 'd3', 'ip': '10.0.0.1',
                    'port': 6202, 'weight': 1000.0, 'region': 0, 'zone': 1})
        rb.rebalance()

        rb.remove_dev(1)
        parts, balance, removed = rb.rebalance()
        self.assertEqual(removed, 1)

    def test_shuffled_gather(self):
        if self._shuffled_gather_helper() and \
                self._shuffled_gather_helper():
                raise AssertionError('It is highly likely the ring is no '
                                     'longer shuffling the set of partitions '
                                     'to reassign on a rebalance.')

    def _shuffled_gather_helper(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.rebalance()
        rb.add_dev({'id': 3, 'region': 0, 'zone': 3, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        replica_plan = rb._build_replica_plan()
        rb._set_parts_wanted(replica_plan)
        for dev in rb._iter_devs():
            dev['tiers'] = utils.tiers_for_dev(dev)
        assign_parts = defaultdict(list)
        rb._gather_parts_for_balance(assign_parts, replica_plan, False)
        max_run = 0
        run = 0
        last_part = 0
        for part, _ in assign_parts.items():
            if part > last_part:
                run += 1
            else:
                if run > max_run:
                    max_run = run
                run = 0
            last_part = part
        if run > max_run:
            max_run = run
        return max_run > len(assign_parts) / 2

    def test_initial_balance(self):
        # 2 boxes, 2 drives each in zone 1
        # 1 box, 2 drives in zone 2
        #
        # This is balanceable, but there used to be some nondeterminism in
        # rebalance() that would sometimes give you an imbalanced ring.
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'region': 1, 'zone': 1, 'weight': 4000.0,
                    'ip': '10.1.1.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'region': 1, 'zone': 1, 'weight': 4000.0,
                    'ip': '10.1.1.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'region': 1, 'zone': 1, 'weight': 4000.0,
                    'ip': '10.1.1.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'region': 1, 'zone': 1, 'weight': 4000.0,
                    'ip': '10.1.1.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'region': 1, 'zone': 2, 'weight': 4000.0,
                    'ip': '10.1.1.3', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'region': 1, 'zone': 2, 'weight': 4000.0,
                    'ip': '10.1.1.3', 'port': 10000, 'device': 'sdb'})

        _, balance, _ = rb.rebalance(seed=2)

        # maybe not *perfect*, but should be close
        self.assertLessEqual(balance, 1)

    def test_multitier_partial(self):
        # Multitier test, nothing full
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 2, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 3, 'zone': 3, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            counts = defaultdict(lambda: defaultdict(int))
            for replica in range(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['region'][dev['region']] += 1
                counts['zone'][dev['zone']] += 1

            if any(c > 1 for c in counts['region'].values()):
                raise AssertionError(
                    "Partition %d not evenly region-distributed (got %r)" %
                    (part, counts['region']))
            if any(c > 1 for c in counts['zone'].values()):
                raise AssertionError(
                    "Partition %d not evenly zone-distributed (got %r)" %
                    (part, counts['zone']))

        # Multitier test, zones full, nodes not full
        rb = ring.RingBuilder(8, 6, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})

        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdd'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sde'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdf'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sdg'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sdh'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sdi'})

        rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            counts = defaultdict(lambda: defaultdict(int))
            for replica in range(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1
            if counts['zone'] != {0: 2, 1: 2, 2: 2}:
                raise AssertionError(
                    "Partition %d not evenly distributed (got %r)" %
                    (part, counts['zone']))
            for dev_id, replica_count in counts['dev_id'].items():
                if replica_count > 1:
                    raise AssertionError(
                        "Partition %d is on device %d more than once (%r)" %
                        (part, dev_id, counts['dev_id']))

    def test_multitier_full(self):
        # Multitier test, #replicas == #devs
        rb = ring.RingBuilder(8, 6, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdd'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sde'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdf'})

        rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            counts = defaultdict(lambda: defaultdict(int))
            for replica in range(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1
            if counts['zone'] != {0: 2, 1: 2, 2: 2}:
                raise AssertionError(
                    "Partition %d not evenly distributed (got %r)" %
                    (part, counts['zone']))
            for dev_id, replica_count in counts['dev_id'].items():
                if replica_count != 1:
                    raise AssertionError(
                        "Partition %d is on device %d %d times, not 1 (%r)" %
                        (part, dev_id, replica_count, counts['dev_id']))

    def test_multitier_overfull(self):
        # Multitier test, #replicas > #zones (to prove even distribution)
        rb = ring.RingBuilder(8, 8, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdg'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdd'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdh'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sde'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdf'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdi'})

        rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            counts = defaultdict(lambda: defaultdict(int))
            for replica in range(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1

            self.assertEqual(8, sum(counts['zone'].values()))
            for zone, replica_count in counts['zone'].items():
                if replica_count not in (2, 3):
                    raise AssertionError(
                        "Partition %d not evenly distributed (got %r)" %
                        (part, counts['zone']))
            for dev_id, replica_count in counts['dev_id'].items():
                if replica_count not in (1, 2):
                    raise AssertionError(
                        "Partition %d is on device %d %d times, "
                        "not 1 or 2 (%r)" %
                        (part, dev_id, replica_count, counts['dev_id']))

    def test_multitier_expansion_more_devices(self):
        rb = ring.RingBuilder(8, 6, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 2, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})

        rb.rebalance()
        rb.validate()

        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdf'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 10, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde'})
        rb.add_dev({'id': 11, 'region': 0, 'zone': 2, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdf'})

        for _ in range(5):
            rb.pretend_min_part_hours_passed()
            rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            counts = dict(zone=defaultdict(int),
                          dev_id=defaultdict(int))
            for replica in range(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1

            self.assertEqual({0: 2, 1: 2, 2: 2}, dict(counts['zone']))
            # each part is assigned once to six unique devices
            self.assertEqual(list(counts['dev_id'].values()), [1] * 6)
            self.assertEqual(len(set(counts['dev_id'].keys())), 6)

    def test_multitier_part_moves_with_0_min_part_hours(self):
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde1'})
        rb.rebalance()
        rb.validate()

        # min_part_hours is 0, so we're clear to move 2 replicas to
        # new devs
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc1'})
        rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            devs = set()
            for replica in range(rb.replicas):
                devs.add(rb._replica2part2dev[replica][part])

            if len(devs) != 3:
                raise AssertionError(
                    "Partition %d not on 3 devs (got %r)" % (part, devs))

    def test_multitier_part_moves_with_positive_min_part_hours(self):
        rb = ring.RingBuilder(8, 3, 99)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde1'})
        rb.rebalance()
        rb.validate()

        # min_part_hours is >0, so we'll only be able to move 1
        # replica to a new home
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            devs = set()
            for replica in range(rb.replicas):
                devs.add(rb._replica2part2dev[replica][part])
            if not any(rb.devs[dev_id]['zone'] == 1 for dev_id in devs):
                raise AssertionError(
                    "Partition %d did not move (got %r)" % (part, devs))

    def test_multitier_dont_move_too_many_replicas(self):
        rb = ring.RingBuilder(8, 3, 1)
        # there'll be at least one replica in z0 and z1
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.rebalance()
        rb.validate()

        # only 1 replica should move
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 3, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 4, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdf1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        rb.validate()

        for part in range(rb.parts):
            zones = set()
            for replica in range(rb.replicas):
                zones.add(rb.devs[rb._replica2part2dev[replica][part]]['zone'])

            if len(zones) != 3:
                raise AssertionError(
                    "Partition %d not in 3 zones (got %r)" % (part, zones))
            if 0 not in zones or 1 not in zones:
                raise AssertionError(
                    "Partition %d not in zones 0 and 1 (got %r)" %
                    (part, zones))

    def test_min_part_hours_zero_will_move_one_replica(self):
        rb = ring.RingBuilder(8, 3, 0)
        # there'll be at least one replica in z0 and z1
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.rebalance(seed=1)
        rb.validate()

        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 3, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 4, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdf1'})
        rb.rebalance(seed=3)
        rb.validate()

        self.assertEqual(0, rb.dispersion)
        # Only one replica could move, so some zones are quite unbalanced
        self.assertAlmostEqual(rb.get_balance(), 66.66, delta=0.5)

        # There was only zone 0 and 1 before adding more devices. Only one
        # replica should have been moved, therefore we expect 256 parts in zone
        # 0 and 1, and a total of 256 in zone 2,3, and 4
        expected = defaultdict(int, {0: 256, 1: 256, 2: 86, 3: 85, 4: 85})
        self.assertEqual(expected, _partition_counts(rb, key='zone'))

        zone_histogram = defaultdict(int)
        for part in range(rb.parts):
            zones = [
                rb.devs[rb._replica2part2dev[replica][part]]['zone']
                for replica in range(rb.replicas)]
            zone_histogram[tuple(sorted(zones))] += 1

        # We expect that every partition moved exactly one replica
        expected = {
            (0, 1, 2): 86,
            (0, 1, 3): 85,
            (0, 1, 4): 85,
        }
        self.assertEqual(zone_histogram, expected)

        # After rebalancing one more times, we expect that everything is in a
        # good state
        rb.rebalance(seed=3)

        self.assertEqual(0, rb.dispersion)
        # a balance of w/i a 1% isn't too bad for 3 replicas on 7
        # devices when part power is only 8
        self.assertAlmostEqual(rb.get_balance(), 0, delta=0.5)

        # every zone has either 153 or 154 parts
        for zone, count in _partition_counts(
                rb, key='zone').items():
            self.assertAlmostEqual(153.5, count, delta=1)

        parts_with_moved_count = defaultdict(int)
        for part in range(rb.parts):
            zones = set()
            for replica in range(rb.replicas):
                zones.add(rb.devs[rb._replica2part2dev[replica][part]]['zone'])
            moved_replicas = len(zones - {0, 1})
            parts_with_moved_count[moved_replicas] += 1

        # as usual, the real numbers depend on the seed, but we want to
        # validate a few things here:
        #
        # 1) every part had to move one replica to hit dispersion (so no
        # one can have a moved count 0)
        #
        # 2) it's quite reasonable that some small percent of parts will
        # have a replica in {0, 1, X} (meaning only one replica of the
        # part moved)
        #
        # 3) when min_part_hours is 0, more than one replica of a part
        # can move in a rebalance, and since that movement would get to
        # better dispersion faster we expect to observe most parts in
        # {[0,1], X, X} (meaning *two* replicas of the part moved)
        #
        # 4) there's plenty of weight in z0 & z1 to hold a whole
        # replicanth, so there is no reason for any part to have to move
        # all three replicas out of those zones (meaning no one can have
        # a moved count 3)
        #
        expected = {
            1: 52,
            2: 204,
        }
        self.assertEqual(parts_with_moved_count, expected)

    def test_ever_rebalanced(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        self.assertFalse(rb.ever_rebalanced)
        builder_file = os.path.join(self.testdir, 'test.buider')
        rb.save(builder_file)
        rb = ring.RingBuilder.load(builder_file)
        self.assertFalse(rb.ever_rebalanced)
        rb.rebalance()
        self.assertTrue(rb.ever_rebalanced)
        rb.save(builder_file)
        rb = ring.RingBuilder.load(builder_file)
        self.assertTrue(rb.ever_rebalanced)

    def test_rerebalance(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        self.assertFalse(rb.ever_rebalanced)
        rb.rebalance()
        self.assertTrue(rb.ever_rebalanced)
        counts = _partition_counts(rb)
        self.assertEqual(counts, {0: 256, 1: 256, 2: 256})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 3, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        self.assertTrue(rb.ever_rebalanced)
        counts = _partition_counts(rb)
        self.assertEqual(counts, {0: 192, 1: 192, 2: 192, 3: 192})
        rb.set_dev_weight(3, 100)
        rb.rebalance()
        counts = _partition_counts(rb)
        self.assertEqual(counts[3], 256)

    def test_add_rebalance_add_rebalance_delete_rebalance(self):
        # Test for https://bugs.launchpad.net/swift/+bug/845952
        # min_part of 0 to allow for rapid rebalancing
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})

        rb.rebalance()
        rb.validate()

        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10005, 'device': 'sda1'})

        rb.rebalance()
        rb.validate()

        rb.remove_dev(1)

        # well now we have only one device in z0
        rb.set_overload(0.5)

        rb.rebalance()
        rb.validate()

    def test_remove_last_partition_from_zero_weight(self):
        rb = ring.RingBuilder(4, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 1, 'weight': 1.0,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})

        rb.add_dev({'id': 1, 'region': 0, 'zone': 2, 'weight': 1.0,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'weight': 1.0,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 3, 'weight': 1.0,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 3, 'weight': 1.0,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 3, 'weight': 1.0,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sdc'})

        rb.add_dev({'id': 3, 'region': 0, 'zone': 3, 'weight': 0.4,
                    'ip': '127.0.0.3', 'port': 10001, 'device': 'zero'})

        zero_weight_dev = 3

        rb.rebalance(seed=1)

        # We want at least one partition with replicas only in zone 2 and 3
        # due to device weights. It would *like* to spread out into zone 1,
        # but can't, due to device weight.
        #
        # Also, we want such a partition to have a replica on device 3,
        # which we will then reduce to zero weight. This should cause the
        # removal of the replica from device 3.
        #
        # Getting this to happen by chance is hard, so let's just set up a
        # builder so that it's in the state we want. This is a synthetic
        # example; while the bug has happened on a real cluster, that
        # builder file had a part_power of 16, so its contents are much too
        # big to include here.
        rb._replica2part2dev = [
            #                            these are the relevant ones
            #                                   |  |  |
            #                                   v  v  v
            array('H', [2, 5, 6, 2, 5, 6, 2, 5, 6, 2, 5, 6, 2, 5, 6, 2]),
            array('H', [1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4, 1, 4]),
            array('H', [0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 3, 5, 6, 2, 5, 6])]

        # fix up bookkeeping
        new_dev_parts = defaultdict(int)
        for part2dev_id in rb._replica2part2dev:
            for dev_id in part2dev_id:
                new_dev_parts[dev_id] += 1
        for dev in rb._iter_devs():
            dev['parts'] = new_dev_parts[dev['id']]

        rb.set_dev_weight(zero_weight_dev, 0.0)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=1)

        node_counts = defaultdict(int)
        for part2dev_id in rb._replica2part2dev:
            for dev_id in part2dev_id:
                node_counts[dev_id] += 1

        self.assertEqual(node_counts[zero_weight_dev], 0)

        # it's as balanced as it gets, so nothing moves anymore
        rb.pretend_min_part_hours_passed()
        parts_moved, _balance, _removed = rb.rebalance(seed=1)

        new_node_counts = defaultdict(int)
        for part2dev_id in rb._replica2part2dev:
            for dev_id in part2dev_id:
                new_node_counts[dev_id] += 1

        del node_counts[zero_weight_dev]
        self.assertEqual(node_counts, new_node_counts)

        self.assertEqual(parts_moved, 0)

    def test_part_swapping_problem(self):
        rb = ring.RingBuilder(4, 3, 1)
        # 127.0.0.1 (2 devs)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        # 127.0.0.2 (3 devs)
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdc'})

        expected = {
            '127.0.0.1': 1.2,
            '127.0.0.2': 1.7999999999999998,
        }
        for wr in (rb._build_weighted_replicas_by_tier(),
                   rb._build_wanted_replicas_by_tier(),
                   rb._build_target_replicas_by_tier()):
            self.assertEqual(expected, {t[-1]: r for (t, r) in
                                        wr.items() if len(t) == 3})
        self.assertEqual(rb.get_required_overload(), 0)
        rb.rebalance(seed=3)
        # so 127.0.0.1 ended up with...
        tier = (0, 0, '127.0.0.1')
        # ... 6 parts with 1 replicas
        self.assertEqual(rb._dispersion_graph[tier][1], 12)
        # ... 4 parts with 2 replicas
        self.assertEqual(rb._dispersion_graph[tier][2], 4)
        # but since we only have two tiers, this is *totally* dispersed
        self.assertEqual(0, rb.dispersion)

        # small rings are hard to balance...
        expected = {0: 10, 1: 10, 2: 10, 3: 9, 4: 9}
        self.assertEqual(expected, {d['id']: d['parts']
                                    for d in rb._iter_devs()})
        # everyone wants 9.6 parts
        expected = {
            0: 4.166666666666671,
            1: 4.166666666666671,
            2: 4.166666666666671,
            3: -6.25,
            4: -6.25,
        }
        self.assertEqual(expected, rb._build_balance_per_dev())

        # original sorted _replica2part2dev
        """
        rb._replica2part2dev = [
            array('H', [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1]),
            array('H', [1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 2, 2, 2, 3, 3, 3]),
            array('H', [2, 2, 2, 2, 3, 3, 4, 4, 4, 4, 3, 4, 4, 4, 4, 4])]
        """

        # now imagine if we came along this _replica2part2dev through no
        # fault of our own; if instead of the 12 parts with only one
        # replica on 127.0.0.1 being split evenly (6 and 6) on device's
        # 0 and 1 - device 1 inexplicitly had 3 extra parts
        rb._replica2part2dev = [
            #                    these are the relevant one's here
            #                                |  |  |
            #                                v  v  v
            array('H', [0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
            array('H', [1, 1, 1, 1, 2, 2, 2, 3, 3, 3, 2, 2, 2, 3, 3, 3]),
            array('H', [2, 2, 2, 2, 3, 3, 4, 4, 4, 4, 3, 4, 4, 4, 4, 4])]

        # fix up bookkeeping
        new_dev_parts = defaultdict(int)
        for part2dev_id in rb._replica2part2dev:
            for dev_id in part2dev_id:
                new_dev_parts[dev_id] += 1
        for dev in rb._iter_devs():
            dev['parts'] = new_dev_parts[dev['id']]
        # reset the _last_part_gather_start otherwise
        # there is a chance it'll unluckly wrap and try and
        # move one of the device 1's from replica 2
        # causing the intermitant failure in bug 1724356
        rb._last_part_gather_start = 0

        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        expected = {
            0: 4.166666666666671,
            1: 4.166666666666671,
            2: 4.166666666666671,
            3: -6.25,
            4: -6.25,
        }
        self.assertEqual(expected, rb._build_balance_per_dev())

        self.assertEqual(rb.get_balance(), 6.25)

    def test_wrong_tier_with_no_where_to_go(self):
        rb = ring.RingBuilder(4, 3, 1)

        # 127.0.0.1 (even devices)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 900,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 900,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 900,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        # 127.0.0.2 (odd devices)
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdd'})

        expected = {
            '127.0.0.1': 1.75,
            '127.0.0.2': 1.25,
        }
        for wr in (rb._build_weighted_replicas_by_tier(),
                   rb._build_wanted_replicas_by_tier(),
                   rb._build_target_replicas_by_tier()):
            self.assertEqual(expected, {t[-1]: r for (t, r) in
                                        wr.items() if len(t) == 3})
        self.assertEqual(rb.get_required_overload(), 0)
        rb.rebalance(seed=3)
        # so 127.0.0.1 ended up with...
        tier = (0, 0, '127.0.0.1')
        # ... 4 parts with 1 replicas
        self.assertEqual(rb._dispersion_graph[tier][1], 4)
        # ... 12 parts with 2 replicas
        self.assertEqual(rb._dispersion_graph[tier][2], 12)
        # ... and of course 0 parts with 3 replicas
        self.assertEqual(rb._dispersion_graph[tier][3], 0)
        # but since we only have two tiers, this is *totally* dispersed
        self.assertEqual(0, rb.dispersion)

        # small rings are hard to balance, but it's possible when
        # part-replicas (3 * 2 ** 4) can go evenly into device weights
        # (4800) like we've done here
        expected = {
            0: 1,
            2: 9,
            4: 9,
            6: 9,
            1: 5,
            3: 5,
            5: 5,
            7: 5,
        }
        self.assertEqual(expected, {d['id']: d['parts']
                                    for d in rb._iter_devs()})
        expected = {
            0: 0.0,
            1: 0.0,
            2: 0.0,
            3: 0.0,
            4: 0.0,
            5: 0.0,
            6: 0.0,
            7: 0.0,
        }
        self.assertEqual(expected, rb._build_balance_per_dev())

        # all devices have exactly the # of parts they want
        expected = {
            0: 0,
            2: 0,
            4: 0,
            6: 0,
            1: 0,
            3: 0,
            5: 0,
            7: 0,
        }
        self.assertEqual(expected, {d['id']: d['parts_wanted']
                                    for d in rb._iter_devs()})

        # original sorted _replica2part2dev
        """
        rb._replica2part2dev = [
            array('H', [0, 2, 2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 4, 4, 4, 4, ]),
            array('H', [4, 4, 4, 6, 6, 6, 6, 6, 6, 6, 6, 6, 1, 1, 1, 1, ]),
            array('H', [1, 3, 3, 3, 3, 3, 5, 5, 5, 5, 5, 7, 7, 7, 7, 7, ])]
        """
        # now imagine if we came along this _replica2part2dev through no
        # fault of our own; and device 0 had extra parts, but both
        # copies of the other replicas were already in the other tier!
        rb._replica2part2dev = [
            #                          these are the relevant one's here
            #                                                     |  |
            #                                                     v  v
            array('H', [2, 2, 2, 2, 2, 2, 2, 2, 2, 4, 4, 4, 4, 4, 0, 0]),
            array('H', [4, 4, 4, 4, 6, 6, 6, 6, 6, 6, 6, 6, 6, 1, 1, 1]),
            array('H', [1, 1, 3, 3, 3, 3, 5, 5, 5, 5, 5, 7, 7, 7, 7, 7])]

        # fix up bookkeeping
        new_dev_parts = defaultdict(int)
        for part2dev_id in rb._replica2part2dev:
            for dev_id in part2dev_id:
                new_dev_parts[dev_id] += 1
        for dev in rb._iter_devs():
            dev['parts'] = new_dev_parts[dev['id']]
        replica_plan = rb._build_replica_plan()
        rb._set_parts_wanted(replica_plan)

        expected = {
            0: -1,  # this device wants to shed
            2: 0,
            4: 0,
            6: 0,
            1: 0,
            3: 1,  # there's devices with room on the other server
            5: 0,
            7: 0,
        }
        self.assertEqual(expected, {d['id']: d['parts_wanted']
                                    for d in rb._iter_devs()})
        self.assertEqual(rb.get_balance(), 100)

        rb.pretend_min_part_hours_passed()
        # There's something like a 11% chance that we won't be able to get to
        # a balance of 0 (and a 6% chance that we won't change anything at all)
        # Pick a seed to make this pass.
        rb.rebalance(seed=123)
        self.assertEqual(rb.get_balance(), 0)

    def test_multiple_duplicate_device_assignment(self):
        rb = ring.RingBuilder(4, 4, 1)
        devs = [
            'r1z1-127.0.0.1:6200/d1',
            'r1z1-127.0.0.1:6201/d2',
            'r1z1-127.0.0.1:6202/d3',
            'r1z1-127.0.0.1:33443/d4',
            'r1z1-127.0.0.2:6200/d5',
            'r1z1-127.0.0.2:6201/d6',
            'r1z1-127.0.0.2:6202/d7',
            'r1z1-127.0.0.2:6202/d8',
        ]
        for add_value in devs:
            dev = utils.parse_add_value(add_value)
            dev['weight'] = 1.0
            rb.add_dev(dev)
        rb.rebalance()
        rb._replica2part2dev = [
            #         these are the relevant one's here
            #           |  |  |                 |  |
            #           v  v  v                 v  v
            array('H', [0, 1, 2, 3, 3, 0, 0, 0, 4, 6, 4, 4, 4, 4, 4, 4]),
            array('H', [0, 1, 3, 1, 1, 1, 1, 1, 5, 7, 5, 5, 5, 5, 5, 5]),
            array('H', [0, 1, 2, 2, 2, 2, 2, 2, 4, 6, 6, 6, 6, 6, 6, 6]),
            array('H', [0, 3, 2, 3, 3, 3, 3, 3, 5, 7, 7, 7, 7, 7, 7, 7])
            #                    ^
            #                    |
            #      this sort of thing worked already
        ]
        # fix up bookkeeping
        new_dev_parts = defaultdict(int)
        for part2dev_id in rb._replica2part2dev:
            for dev_id in part2dev_id:
                new_dev_parts[dev_id] += 1
        for dev in rb._iter_devs():
            dev['parts'] = new_dev_parts[dev['id']]
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        rb.validate()

    def test_region_fullness_with_balanceable_ring(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})

        rb.add_dev({'id': 2, 'region': 1, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})

        rb.add_dev({'id': 4, 'region': 2, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10005, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'region': 2, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10006, 'device': 'sda1'})

        rb.add_dev({'id': 6, 'region': 3, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10007, 'device': 'sda1'})
        rb.add_dev({'id': 7, 'region': 3, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10008, 'device': 'sda1'})
        rb.rebalance(seed=2)

        population_by_region = self._get_population_by_region(rb)
        self.assertEqual(population_by_region,
                         {0: 192, 1: 192, 2: 192, 3: 192})

    def test_region_fullness_with_unbalanceable_ring(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})

        rb.add_dev({'id': 2, 'region': 1, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 1, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})
        rb.rebalance(seed=2)

        population_by_region = self._get_population_by_region(rb)
        self.assertEqual(population_by_region, {0: 512, 1: 256})

    def test_adding_region_slowly_with_unbalanceable_ring(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc1'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd1'})

        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdc1'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 1, 'weight': 0.5,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdd1'})
        rb.rebalance(seed=2)

        rb.add_dev({'id': 2, 'region': 1, 'zone': 0, 'weight': 0.25,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 1, 'zone': 1, 'weight': 0.25,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        changed_parts, _balance, _removed = rb.rebalance(seed=2)

        # there's not enough room in r1 for every partition to have a replica
        # in it, so only 86 assignments occur in r1 (that's ~1/5 of the total,
        # since r1 has 1/5 of the weight).
        population_by_region = self._get_population_by_region(rb)
        self.assertEqual(population_by_region, {0: 682, 1: 86})

        # really 86 parts *should* move (to the new region) but to avoid
        # accidentally picking up too many and causing some parts to randomly
        # flop around devices in the original region - our gather algorithm
        # is conservative when picking up only from devices that are for sure
        # holding more parts than they want (math.ceil() of the replica_plan)
        # which guarantees any parts picked up will have new homes in a better
        # tier or failure_domain.
        self.assertEqual(86, changed_parts)

        # and since there's not enough room, subsequent rebalances will not
        # cause additional assignments to r1
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=2)
        rb.validate()
        population_by_region = self._get_population_by_region(rb)
        self.assertEqual(population_by_region, {0: 682, 1: 86})

        # after you add more weight, more partition assignments move
        rb.set_dev_weight(2, 0.5)
        rb.set_dev_weight(3, 0.5)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=2)
        rb.validate()
        population_by_region = self._get_population_by_region(rb)
        self.assertEqual(population_by_region, {0: 614, 1: 154})

        rb.set_dev_weight(2, 1.0)
        rb.set_dev_weight(3, 1.0)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=2)
        rb.validate()
        population_by_region = self._get_population_by_region(rb)
        self.assertEqual(population_by_region, {0: 512, 1: 256})

    def test_avoid_tier_change_new_region(self):
        rb = ring.RingBuilder(8, 3, 1)
        for i in range(5):
            rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 100,
                        'ip': '127.0.0.1', 'port': i, 'device': 'sda1'})
        rb.rebalance(seed=2)

        # Add a new device in new region to a balanced ring
        rb.add_dev({'id': 5, 'region': 1, 'zone': 0, 'weight': 0,
                    'ip': '127.0.0.5', 'port': 10000, 'device': 'sda1'})

        # Increase the weight of region 1 slowly
        moved_partitions = []
        errors = []
        for weight in range(0, 101, 10):
            rb.set_dev_weight(5, weight)
            rb.pretend_min_part_hours_passed()
            changed_parts, _balance, _removed = rb.rebalance(seed=2)
            rb.validate()
            moved_partitions.append(changed_parts)
            # Ensure that the second region has enough partitions
            # Otherwise there will be replicas at risk
            min_parts_for_r1 = ceil(weight / (500.0 + weight) * 768)
            parts_for_r1 = self._get_population_by_region(rb).get(1, 0)
            try:
                self.assertEqual(min_parts_for_r1, parts_for_r1)
            except AssertionError:
                errors.append('weight %s got %s parts but expected %s' % (
                    weight, parts_for_r1, min_parts_for_r1))

        self.assertFalse(errors)

        # Number of partitions moved on each rebalance
        # 10/510 * 768 ~ 15.06 -> move at least 15 partitions in first step
        ref = [0, 16, 14, 14, 13, 13, 13, 12, 11, 12, 10]
        self.assertEqual(ref, moved_partitions)

    def test_set_replicas_increase(self):
        rb = ring.RingBuilder(8, 2, 0)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.rebalance()
        rb.validate()

        rb.replicas = 2.1
        rb.rebalance()
        rb.validate()

        self.assertEqual([len(p2d) for p2d in rb._replica2part2dev],
                         [256, 256, 25])

        rb.replicas = 2.2
        rb.rebalance()
        rb.validate()
        self.assertEqual([len(p2d) for p2d in rb._replica2part2dev],
                         [256, 256, 51])

    def test_set_replicas_decrease(self):
        rb = ring.RingBuilder(4, 5, 0)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.rebalance()
        rb.validate()

        rb.replicas = 4.9
        rb.rebalance()
        rb.validate()

        self.assertEqual([len(p2d) for p2d in rb._replica2part2dev],
                         [16, 16, 16, 16, 14])

        # cross a couple of integer thresholds (4 and 3)
        rb.replicas = 2.5
        rb.rebalance()
        rb.validate()

        self.assertEqual([len(p2d) for p2d in rb._replica2part2dev],
                         [16, 16, 8])

    def test_fractional_replicas_rebalance(self):
        rb = ring.RingBuilder(8, 2.5, 0)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.rebalance()  # passes by not crashing
        rb.validate()   # also passes by not crashing
        self.assertEqual([len(p2d) for p2d in rb._replica2part2dev],
                         [256, 256, 128])

    def test_create_add_dev_add_replica_rebalance(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.set_replicas(4)
        rb.rebalance()  # this would crash since parts_wanted was not set
        rb.validate()

    def test_reduce_replicas_after_remove_device(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.rebalance()
        rb.remove_dev(0)
        self.assertRaises(exceptions.RingValidationError, rb.rebalance)
        rb.set_replicas(2)
        rb.rebalance()
        rb.validate()

    def test_rebalance_post_upgrade(self):
        rb = ring.RingBuilder(8, 3, 1)
        # 5 devices: 5 is the smallest number that does not divide 3 * 2^8,
        # which forces some rounding to happen.
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde'})
        rb.rebalance()
        rb.validate()

        # Older versions of the ring builder code would round down when
        # computing parts_wanted, while the new code rounds up. Make sure we
        # can handle a ring built by the old method.
        #
        # This code mimics the old _set_parts_wanted.
        weight_of_one_part = rb.weight_of_one_part()
        for dev in rb._iter_devs():
            if not dev['weight']:
                dev['parts_wanted'] = -rb.parts * rb.replicas
            else:
                dev['parts_wanted'] = (
                    int(weight_of_one_part * dev['weight']) -
                    dev['parts'])

        rb.pretend_min_part_hours_passed()
        rb.rebalance()  # this crashes unless rebalance resets parts_wanted
        rb.validate()

    def test_add_replicas_then_rebalance_respects_weight(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdf'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdg'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdh'})

        rb.add_dev({'id': 8, 'region': 0, 'zone': 2, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdi'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 2, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdj'})
        rb.add_dev({'id': 10, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdk'})
        rb.add_dev({'id': 11, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdl'})

        rb.rebalance(seed=1)

        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEqual(counts, {0: 96, 1: 96,
                                  2: 32, 3: 32,
                                  4: 96, 5: 96,
                                  6: 32, 7: 32,
                                  8: 96, 9: 96,
                                  10: 32, 11: 32})

        rb.replicas *= 2
        rb.rebalance(seed=1)

        r = rb.get_ring()
        counts = {}
        for part2dev_id in r._replica2part2dev_id:
            for dev_id in part2dev_id:
                counts[dev_id] = counts.get(dev_id, 0) + 1
        self.assertEqual(counts, {0: 192, 1: 192,
                                  2: 64, 3: 64,
                                  4: 192, 5: 192,
                                  6: 64, 7: 64,
                                  8: 192, 9: 192,
                                  10: 64, 11: 64})

    def test_overload(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sde'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdf'})

        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdg'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdh'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdi'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sdc'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sdj'})
        rb.add_dev({'id': 10, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sdk'})
        rb.add_dev({'id': 11, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sdl'})

        rb.rebalance(seed=12345)
        rb.validate()

        # sanity check: balance respects weights, so default
        part_counts = _partition_counts(rb, key='zone')
        self.assertEqual(part_counts[0], 192)
        self.assertEqual(part_counts[1], 192)
        self.assertEqual(part_counts[2], 384)

        # Devices 0 and 1 take 10% more than their fair shares by weight since
        # overload is 10% (0.1).
        rb.set_overload(0.1)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=12345)

        part_counts = _partition_counts(rb, key='zone')
        self.assertEqual({0: 212, 1: 211, 2: 345}, part_counts)

        # Now, devices 0 and 1 take 50% more than their fair shares by
        # weight.
        rb.set_overload(0.5)
        for _ in range(3):
            rb.pretend_min_part_hours_passed()
            rb.rebalance(seed=12345)

        part_counts = _partition_counts(rb, key='zone')
        self.assertEqual({0: 256, 1: 256, 2: 256}, part_counts)

        # Devices 0 and 1 may take up to 75% over their fair share, but the
        # placement algorithm only wants to spread things out evenly between
        # all drives, so the devices stay at 50% more.
        rb.set_overload(0.75)
        for _ in range(3):
            rb.pretend_min_part_hours_passed()
            rb.rebalance(seed=12345)

        part_counts = _partition_counts(rb, key='zone')
        self.assertEqual(part_counts[0], 256)
        self.assertEqual(part_counts[1], 256)
        self.assertEqual(part_counts[2], 256)

    def test_unoverload(self):
        # Start off needing overload to balance, then add capacity until we
        # don't need overload any more and see that things still balance.
        # Overload doesn't prevent optimal balancing.
        rb = ring.RingBuilder(8, 3, 1)
        rb.set_overload(0.125)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})

        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 2,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 0, 'weight': 2,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 10, 'region': 0, 'zone': 0, 'weight': 2,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 11, 'region': 0, 'zone': 0, 'weight': 2,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sdc'})
        rb.rebalance(seed=12345)

        # sanity check: our overload is big enough to balance things
        part_counts = _partition_counts(rb, key='ip')
        self.assertEqual(part_counts['127.0.0.1'], 216)
        self.assertEqual(part_counts['127.0.0.2'], 216)
        self.assertEqual(part_counts['127.0.0.3'], 336)

        # Add some weight: balance improves
        for dev in rb.devs:
            if dev['ip'] in ('127.0.0.1', '127.0.0.2'):
                rb.set_dev_weight(dev['id'], 1.22)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=12345)

        part_counts = _partition_counts(rb, key='ip')

        self.assertEqual({
            '127.0.0.1': 237,
            '127.0.0.2': 237,
            '127.0.0.3': 294,
        }, part_counts)

        # Even out the weights: balance becomes perfect
        for dev in rb.devs:
            if dev['ip'] in ('127.0.0.1', '127.0.0.2'):
                rb.set_dev_weight(dev['id'], 2)

        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=12345)

        part_counts = _partition_counts(rb, key='ip')
        self.assertEqual(part_counts['127.0.0.1'], 256)
        self.assertEqual(part_counts['127.0.0.2'], 256)
        self.assertEqual(part_counts['127.0.0.3'], 256)

        # Add a new server: balance stays optimal
        rb.add_dev({'id': 12, 'region': 0, 'zone': 0,
                    'weight': 2,
                    'ip': '127.0.0.4', 'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 13, 'region': 0, 'zone': 0,
                    'weight': 2,
                    'ip': '127.0.0.4', 'port': 10000, 'device': 'sde'})
        rb.add_dev({'id': 14, 'region': 0, 'zone': 0,
                    'weight': 2,
                    'ip': '127.0.0.4', 'port': 10000, 'device': 'sdf'})
        rb.add_dev({'id': 15, 'region': 0, 'zone': 0,
                    'weight': 2,
                    'ip': '127.0.0.4', 'port': 10000, 'device': 'sdf'})

        # we're moving more than 1/3 of the replicas but fewer than 2/3, so
        # we have to do this twice
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=12345)
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=12345)

        expected = {
            '127.0.0.1': 192,
            '127.0.0.2': 192,
            '127.0.0.3': 192,
            '127.0.0.4': 192,
        }

        part_counts = _partition_counts(rb, key='ip')
        self.assertEqual(part_counts, expected)

    def test_overload_keeps_balanceable_things_balanced_initially(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 8,
                    'ip': '10.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 8,
                    'ip': '10.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.3', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.3', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.4', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.4', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 8, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.5', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.5', 'port': 10000, 'device': 'sdb'})

        rb.set_overload(99999)
        rb.rebalance(seed=12345)

        part_counts = _partition_counts(rb)
        self.assertEqual(part_counts, {
            0: 128,
            1: 128,
            2: 64,
            3: 64,
            4: 64,
            5: 64,
            6: 64,
            7: 64,
            8: 64,
            9: 64,
        })

    def test_overload_keeps_balanceable_things_balanced_on_rebalance(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 8,
                    'ip': '10.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 8,
                    'ip': '10.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.3', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.3', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.4', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.4', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 8, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.5', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '10.0.0.5', 'port': 10000, 'device': 'sdb'})

        rb.set_overload(99999)

        rb.rebalance(seed=123)
        part_counts = _partition_counts(rb)
        self.assertEqual(part_counts, {
            0: 128,
            1: 128,
            2: 64,
            3: 64,
            4: 64,
            5: 64,
            6: 64,
            7: 64,
            8: 64,
            9: 64,
        })

        # swap weights between 10.0.0.1 and 10.0.0.2
        rb.set_dev_weight(0, 4)
        rb.set_dev_weight(1, 4)
        rb.set_dev_weight(2, 8)
        rb.set_dev_weight(1, 8)

        rb.rebalance(seed=456)
        part_counts = _partition_counts(rb)
        self.assertEqual(part_counts, {
            0: 128,
            1: 128,
            2: 64,
            3: 64,
            4: 64,
            5: 64,
            6: 64,
            7: 64,
            8: 64,
            9: 64,
        })

    def test_server_per_port(self):
        # 3 servers, 3 disks each, with each disk on its own port
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.1', 'port': 10000, 'device': 'sdx'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.1', 'port': 10001, 'device': 'sdy'})

        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.2', 'port': 10000, 'device': 'sdx'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.2', 'port': 10001, 'device': 'sdy'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.3', 'port': 10000, 'device': 'sdx'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.3', 'port': 10001, 'device': 'sdy'})

        rb.rebalance(seed=1)

        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.1', 'port': 10002, 'device': 'sdz'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.2', 'port': 10002, 'device': 'sdz'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '10.0.0.3', 'port': 10002, 'device': 'sdz'})

        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=1)

        poorly_dispersed = []
        for part in range(rb.parts):
            on_nodes = set()
            for replica in range(rb.replicas):
                dev_id = rb._replica2part2dev[replica][part]
                on_nodes.add(rb.devs[dev_id]['ip'])
            if len(on_nodes) < rb.replicas:
                poorly_dispersed.append(part)
        self.assertEqual(poorly_dispersed, [])

    def test_load(self):
        rb = ring.RingBuilder(8, 3, 1)
        devs = [{'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                 'ip': '127.0.0.0', 'port': 10000, 'device': 'sda1',
                 'meta': 'meta0'},
                {'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                 'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1',
                 'meta': 'meta1'},
                {'id': 2, 'region': 0, 'zone': 2, 'weight': 2,
                 'ip': '127.0.0.2', 'port': 10002, 'device': 'sdc1',
                 'meta': 'meta2'},
                {'id': 3, 'region': 0, 'zone': 3, 'weight': 2,
                 'ip': '127.0.0.3', 'port': 10003, 'device': 'sdd1'}]
        for d in devs:
            rb.add_dev(d)
        rb.rebalance()

        real_pickle = pickle.load
        fake_open = mock.mock_open()

        io_error_not_found = IOError()
        io_error_not_found.errno = errno.ENOENT

        io_error_no_perm = IOError()
        io_error_no_perm.errno = errno.EPERM

        io_error_generic = IOError()
        io_error_generic.errno = errno.EOPNOTSUPP
        try:
            # test a legit builder
            fake_pickle = mock.Mock(return_value=rb)
            pickle.load = fake_pickle
            builder = ring.RingBuilder.load('fake.builder', open=fake_open)
            self.assertEqual(fake_pickle.call_count, 1)
            fake_open.assert_has_calls([mock.call('fake.builder', 'rb')])
            self.assertEqual(builder, rb)
            fake_pickle.reset_mock()

            # test old style builder
            fake_pickle.return_value = rb.to_dict()
            pickle.load = fake_pickle
            builder = ring.RingBuilder.load('fake.builder', open=fake_open)
            fake_open.assert_has_calls([mock.call('fake.builder', 'rb')])
            self.assertEqual(builder.devs, rb.devs)
            fake_pickle.reset_mock()

            # test old devs but no meta
            no_meta_builder = rb
            for dev in no_meta_builder.devs:
                del(dev['meta'])
            fake_pickle.return_value = no_meta_builder
            pickle.load = fake_pickle
            builder = ring.RingBuilder.load('fake.builder', open=fake_open)
            fake_open.assert_has_calls([mock.call('fake.builder', 'rb')])
            self.assertEqual(builder.devs, rb.devs)

            # test an empty builder
            fake_pickle.side_effect = EOFError
            pickle.load = fake_pickle
            self.assertRaises(exceptions.UnPicklingError,
                              ring.RingBuilder.load, 'fake.builder',
                              open=fake_open)

            # test a corrupted builder
            fake_pickle.side_effect = pickle.UnpicklingError
            pickle.load = fake_pickle
            self.assertRaises(exceptions.UnPicklingError,
                              ring.RingBuilder.load, 'fake.builder',
                              open=fake_open)

            # test some error
            fake_pickle.side_effect = AttributeError
            pickle.load = fake_pickle
            self.assertRaises(exceptions.UnPicklingError,
                              ring.RingBuilder.load, 'fake.builder',
                              open=fake_open)
        finally:
            pickle.load = real_pickle

        # test non existent builder file
        fake_open.side_effect = io_error_not_found
        self.assertRaises(exceptions.FileNotFoundError,
                          ring.RingBuilder.load, 'fake.builder',
                          open=fake_open)

        # test non accessible builder file
        fake_open.side_effect = io_error_no_perm
        self.assertRaises(exceptions.PermissionError,
                          ring.RingBuilder.load, 'fake.builder',
                          open=fake_open)

        # test an error other then ENOENT and ENOPERM
        fake_open.side_effect = io_error_generic
        self.assertRaises(IOError,
                          ring.RingBuilder.load, 'fake.builder',
                          open=fake_open)

    def test_save_load(self):
        rb = ring.RingBuilder(8, 3, 1)
        devs = [{'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                 'ip': '127.0.0.0', 'port': 10000,
                 'replication_ip': '127.0.0.0', 'replication_port': 10000,
                 'device': 'sda1', 'meta': 'meta0'},
                {'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                 'ip': '127.0.0.1', 'port': 10001,
                 'replication_ip': '127.0.0.1', 'replication_port': 10001,
                 'device': 'sdb1', 'meta': 'meta1'},
                {'id': 2, 'region': 0, 'zone': 2, 'weight': 2,
                 'ip': '127.0.0.2', 'port': 10002,
                 'replication_ip': '127.0.0.2', 'replication_port': 10002,
                 'device': 'sdc1', 'meta': 'meta2'},
                {'id': 3, 'region': 0, 'zone': 3, 'weight': 2,
                 'ip': '127.0.0.3', 'port': 10003,
                 'replication_ip': '127.0.0.3', 'replication_port': 10003,
                 'device': 'sdd1', 'meta': ''}]
        rb.set_overload(3.14159)
        for d in devs:
            rb.add_dev(d)
        rb.rebalance()
        builder_file = os.path.join(self.testdir, 'test_save.builder')
        rb.save(builder_file)
        loaded_rb = ring.RingBuilder.load(builder_file)
        self.maxDiff = None
        self.assertEqual(loaded_rb.to_dict(), rb.to_dict())
        self.assertEqual(loaded_rb.overload, 3.14159)

    @mock.patch('six.moves.builtins.open', autospec=True)
    @mock.patch('swift.common.ring.builder.pickle.dump', autospec=True)
    def test_save(self, mock_pickle_dump, mock_open):
        mock_open.return_value = mock_fh = mock.MagicMock()
        rb = ring.RingBuilder(8, 3, 1)
        devs = [{'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                 'ip': '127.0.0.0', 'port': 10000, 'device': 'sda1',
                 'meta': 'meta0'},
                {'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                 'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1',
                 'meta': 'meta1'},
                {'id': 2, 'region': 0, 'zone': 2, 'weight': 2,
                 'ip': '127.0.0.2', 'port': 10002, 'device': 'sdc1',
                 'meta': 'meta2'},
                {'id': 3, 'region': 0, 'zone': 3, 'weight': 2,
                 'ip': '127.0.0.3', 'port': 10003, 'device': 'sdd1'}]
        for d in devs:
            rb.add_dev(d)
        rb.rebalance()
        rb.save('some.builder')
        mock_open.assert_called_once_with('some.builder', 'wb')
        mock_pickle_dump.assert_called_once_with(rb.to_dict(),
                                                 mock_fh.__enter__(),
                                                 protocol=2)

    def test_id(self):
        rb = ring.RingBuilder(8, 3, 1)
        # check id is assigned after save
        builder_file = os.path.join(self.testdir, 'test_save.builder')
        rb.save(builder_file)
        assigned_id = rb.id
        # check id doesn't change when builder is saved again
        rb.save(builder_file)
        self.assertEqual(assigned_id, rb.id)
        # check same id after loading
        loaded_rb = ring.RingBuilder.load(builder_file)
        self.assertEqual(assigned_id, loaded_rb.id)
        # check id doesn't change when loaded builder is saved
        rb.save(builder_file)
        self.assertEqual(assigned_id, rb.id)
        # check same id after loading again
        loaded_rb = ring.RingBuilder.load(builder_file)
        self.assertEqual(assigned_id, loaded_rb.id)
        # check id remains once assigned, even when save fails
        with self.assertRaises(IOError):
            rb.save(os.path.join(
                self.testdir, 'non_existent_dir', 'test_save.file'))
        self.assertEqual(assigned_id, rb.id)

        # sanity check that different builders get different id's
        other_rb = ring.RingBuilder(8, 3, 1)
        other_builder_file = os.path.join(self.testdir, 'test_save_2.builder')
        other_rb.save(other_builder_file)
        self.assertNotEqual(assigned_id, other_rb.id)

    def test_id_copy_from(self):
        # copy_from preserves the same id
        orig_rb = ring.RingBuilder(8, 3, 1)
        copy_rb = ring.RingBuilder(8, 3, 1)
        copy_rb.copy_from(orig_rb)
        for rb in(orig_rb, copy_rb):
            with self.assertRaises(AttributeError) as cm:
                rb.id
            self.assertIn('id attribute has not been initialised',
                          cm.exception.args[0])

        builder_file = os.path.join(self.testdir, 'test_save.builder')
        orig_rb.save(builder_file)
        copy_rb = ring.RingBuilder(8, 3, 1)
        copy_rb.copy_from(orig_rb)
        self.assertEqual(orig_rb.id, copy_rb.id)

    def test_id_legacy_builder_file(self):
        builder_file = os.path.join(self.testdir, 'legacy.builder')

        def do_test():
            # load legacy file
            loaded_rb = ring.RingBuilder.load(builder_file)
            with self.assertRaises(AttributeError) as cm:
                loaded_rb.id
            self.assertIn('id attribute has not been initialised',
                          cm.exception.args[0])

            # check saving assigns an id, and that it is persisted
            loaded_rb.save(builder_file)
            assigned_id = loaded_rb.id
            self.assertIsNotNone(assigned_id)
            loaded_rb = ring.RingBuilder.load(builder_file)
            self.assertEqual(assigned_id, loaded_rb.id)

        # older builders had no id so the pickled builder dict had no id key
        rb = ring.RingBuilder(8, 3, 1)
        orig_to_dict = rb.to_dict

        def mock_to_dict():
            result = orig_to_dict()
            result.pop('id')
            return result

        with mock.patch.object(rb, 'to_dict', mock_to_dict):
            rb.save(builder_file)
        do_test()

        # even older builders pickled the class instance, which would have had
        # no _id attribute
        rb = ring.RingBuilder(8, 3, 1)
        del rb.logger  # logger type cannot be pickled
        del rb._id
        builder_file = os.path.join(self.testdir, 'legacy.builder')
        with open(builder_file, 'wb') as f:
            pickle.dump(rb, f, protocol=2)
        do_test()

    def test_id_not_initialised_errors(self):
        rb = ring.RingBuilder(8, 3, 1)
        # id is not set until builder has been saved
        with self.assertRaises(AttributeError) as cm:
            rb.id
        self.assertIn('id attribute has not been initialised',
                      cm.exception.args[0])
        # save must succeed for id to be assigned
        with self.assertRaises(IOError):
            rb.save(os.path.join(
                self.testdir, 'non-existent-dir', 'foo.builder'))
        with self.assertRaises(AttributeError) as cm:
            rb.id
        self.assertIn('id attribute has not been initialised',
                      cm.exception.args[0])

    def test_search_devs(self):
        rb = ring.RingBuilder(8, 3, 1)
        devs = [{'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                 'ip': '127.0.0.0', 'port': 10000, 'device': 'sda1',
                 'meta': 'meta0'},
                {'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                 'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1',
                 'meta': 'meta1'},
                {'id': 2, 'region': 1, 'zone': 2, 'weight': 2,
                 'ip': '127.0.0.2', 'port': 10002, 'device': 'sdc1',
                 'meta': 'meta2'},
                {'id': 3, 'region': 1, 'zone': 3, 'weight': 2,
                 'ip': '127.0.0.3', 'port': 10003, 'device': 'sdd1',
                 'meta': 'meta3'},
                {'id': 4, 'region': 2, 'zone': 4, 'weight': 1,
                 'ip': '127.0.0.4', 'port': 10004, 'device': 'sde1',
                 'meta': 'meta4', 'replication_ip': '127.0.0.10',
                 'replication_port': 20000},
                {'id': 5, 'region': 2, 'zone': 5, 'weight': 2,
                 'ip': '127.0.0.5', 'port': 10005, 'device': 'sdf1',
                 'meta': 'meta5', 'replication_ip': '127.0.0.11',
                 'replication_port': 20001},
                {'id': 6, 'region': 2, 'zone': 6, 'weight': 2,
                 'ip': '127.0.0.6', 'port': 10006, 'device': 'sdg1',
                 'meta': 'meta6', 'replication_ip': '127.0.0.12',
                 'replication_port': 20002}]
        for d in devs:
            rb.add_dev(d)
        rb.rebalance()
        res = rb.search_devs({'region': 0})
        self.assertEqual(res, [devs[0], devs[1]])
        res = rb.search_devs({'region': 1})
        self.assertEqual(res, [devs[2], devs[3]])
        res = rb.search_devs({'region': 1, 'zone': 2})
        self.assertEqual(res, [devs[2]])
        res = rb.search_devs({'id': 1})
        self.assertEqual(res, [devs[1]])
        res = rb.search_devs({'zone': 1})
        self.assertEqual(res, [devs[1]])
        res = rb.search_devs({'ip': '127.0.0.1'})
        self.assertEqual(res, [devs[1]])
        res = rb.search_devs({'ip': '127.0.0.1', 'port': 10001})
        self.assertEqual(res, [devs[1]])
        res = rb.search_devs({'port': 10001})
        self.assertEqual(res, [devs[1]])
        res = rb.search_devs({'replication_ip': '127.0.0.10'})
        self.assertEqual(res, [devs[4]])
        res = rb.search_devs({'replication_ip': '127.0.0.10',
                              'replication_port': 20000})
        self.assertEqual(res, [devs[4]])
        res = rb.search_devs({'replication_port': 20000})
        self.assertEqual(res, [devs[4]])
        res = rb.search_devs({'device': 'sdb1'})
        self.assertEqual(res, [devs[1]])
        res = rb.search_devs({'meta': 'meta1'})
        self.assertEqual(res, [devs[1]])

    def test_validate(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 10, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 11, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 12, 'region': 0, 'zone': 2, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 3, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 13, 'region': 0, 'zone': 3, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 14, 'region': 0, 'zone': 3, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 15, 'region': 0, 'zone': 3, 'weight': 2,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})

        # Degenerate case: devices added but not rebalanced yet
        self.assertRaises(exceptions.RingValidationError, rb.validate)

        rb.rebalance()
        counts = _partition_counts(rb, key='zone')
        self.assertEqual(counts, {0: 128, 1: 128, 2: 256, 3: 256})

        dev_usage, worst = rb.validate()
        self.assertIsNone(dev_usage)
        self.assertIsNone(worst)

        dev_usage, worst = rb.validate(stats=True)
        self.assertEqual(list(dev_usage), [32, 32, 64, 64,
                                           32, 32, 32,  # added zone0
                                           32, 32, 32,  # added zone1
                                           64, 64, 64,  # added zone2
                                           64, 64, 64,  # added zone3
                                           ])
        self.assertEqual(int(worst), 0)

        # min part hours should pin all the parts assigned to this zero
        # weight device onto it such that the balance will look horrible
        rb.set_dev_weight(2, 0)
        rb.rebalance()
        self.assertEqual(rb.validate(stats=True)[1], MAX_BALANCE)

        # Test not all partitions doubly accounted for
        rb.devs[1]['parts'] -= 1
        self.assertRaises(exceptions.RingValidationError, rb.validate)
        rb.devs[1]['parts'] += 1

        # Test non-numeric port
        rb.devs[1]['port'] = '10001'
        self.assertRaises(exceptions.RingValidationError, rb.validate)
        rb.devs[1]['port'] = 10001

        # Test partition on nonexistent device
        rb.pretend_min_part_hours_passed()
        orig_dev_id = rb._replica2part2dev[0][0]
        rb._replica2part2dev[0][0] = len(rb.devs)
        self.assertRaises(exceptions.RingValidationError, rb.validate)
        rb._replica2part2dev[0][0] = orig_dev_id

        # Tests that validate can handle 'holes' in .devs
        rb.remove_dev(2)
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        rb.validate(stats=True)

        # Test partition assigned to a hole
        if rb.devs[2]:
            rb.remove_dev(2)
        rb.pretend_min_part_hours_passed()
        orig_dev_id = rb._replica2part2dev[0][0]
        rb._replica2part2dev[0][0] = 2
        self.assertRaises(exceptions.RingValidationError, rb.validate)
        rb._replica2part2dev[0][0] = orig_dev_id

        # Validate that zero weight devices with no partitions don't count on
        # the 'worst' value.
        self.assertNotEqual(rb.validate(stats=True)[1], MAX_BALANCE)
        rb.add_dev({'id': 16, 'region': 0, 'zone': 0, 'weight': 0,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        self.assertNotEqual(rb.validate(stats=True)[1], MAX_BALANCE)

    def test_validate_partial_replica(self):
        rb = ring.RingBuilder(8, 2.5, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdc'})
        rb.rebalance()
        rb.validate()  # sanity
        self.assertEqual(len(rb._replica2part2dev[0]), 256)
        self.assertEqual(len(rb._replica2part2dev[1]), 256)
        self.assertEqual(len(rb._replica2part2dev[2]), 128)

        # now swap partial replica part maps
        rb._replica2part2dev[1], rb._replica2part2dev[2] = \
            rb._replica2part2dev[2], rb._replica2part2dev[1]
        self.assertRaises(exceptions.RingValidationError, rb.validate)

    def test_validate_duplicate_part_assignment(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdc'})
        rb.rebalance()
        rb.validate()  # sanity
        # now double up a device assignment
        rb._replica2part2dev[1][200] = rb._replica2part2dev[2][200]

        with self.assertRaises(exceptions.RingValidationError) as e:
            rb.validate()

        expected = 'The partition 200 has been assigned to duplicate devices'
        self.assertIn(expected, str(e.exception))

    def test_get_part_devices(self):
        rb = ring.RingBuilder(8, 3, 1)
        self.assertEqual(rb.get_part_devices(0), [])

        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.rebalance()

        part_devs = sorted(rb.get_part_devices(0),
                           key=operator.itemgetter('id'))
        self.assertEqual(part_devs, [rb.devs[0], rb.devs[1], rb.devs[2]])

    def test_get_part_devices_partial_replicas(self):
        rb = ring.RingBuilder(8, 2.5, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.rebalance(seed=4)

        # note: partition 255 will only have 2 replicas
        part_devs = sorted(rb.get_part_devices(255),
                           key=operator.itemgetter('id'))
        self.assertEqual(part_devs, [rb.devs[1], rb.devs[2]])

    def test_dispersion_with_zero_weight_devices(self):
        rb = ring.RingBuilder(8, 3.0, 0)
        # add two devices to a single server in a single zone
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        # and a zero weight device
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 0,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        rb.rebalance()
        self.assertEqual(rb.dispersion, 0.0)
        self.assertEqual(rb._dispersion_graph, {
            (0,): [0, 0, 0, 256],
            (0, 0): [0, 0, 0, 256],
            (0, 0, '127.0.0.1'): [0, 0, 0, 256],
            (0, 0, '127.0.0.1', 0): [0, 256, 0, 0],
            (0, 0, '127.0.0.1', 1): [0, 256, 0, 0],
            (0, 0, '127.0.0.1', 2): [0, 256, 0, 0],
        })

    def test_dispersion_with_zero_weight_devices_with_parts(self):
        rb = ring.RingBuilder(8, 3.0, 1)
        # add four devices to a single server in a single zone
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        rb.rebalance(seed=1)
        self.assertEqual(rb.dispersion, 0.0)
        self.assertEqual(rb._dispersion_graph, {
            (0,): [0, 0, 0, 256],
            (0, 0): [0, 0, 0, 256],
            (0, 0, '127.0.0.1'): [0, 0, 0, 256],
            (0, 0, '127.0.0.1', 0): [64, 192, 0, 0],
            (0, 0, '127.0.0.1', 1): [64, 192, 0, 0],
            (0, 0, '127.0.0.1', 2): [64, 192, 0, 0],
            (0, 0, '127.0.0.1', 3): [64, 192, 0, 0],
        })
        # now mark a device 2 for decom
        rb.set_dev_weight(2, 0.0)
        # we'll rebalance but can't move any parts
        rb.rebalance(seed=1)
        # zero weight tier has one copy of 1/4 part-replica
        self.assertEqual(rb.dispersion, 25.0)
        self.assertEqual(rb._dispersion_graph, {
            (0,): [0, 0, 0, 256],
            (0, 0): [0, 0, 0, 256],
            (0, 0, '127.0.0.1'): [0, 0, 0, 256],
            (0, 0, '127.0.0.1', 0): [64, 192, 0, 0],
            (0, 0, '127.0.0.1', 1): [64, 192, 0, 0],
            (0, 0, '127.0.0.1', 2): [64, 192, 0, 0],
            (0, 0, '127.0.0.1', 3): [64, 192, 0, 0],
        })
        # unlock the stuck parts
        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=3)
        self.assertEqual(rb.dispersion, 0.0)
        self.assertEqual(rb._dispersion_graph, {
            (0,): [0, 0, 0, 256],
            (0, 0): [0, 0, 0, 256],
            (0, 0, '127.0.0.1'): [0, 0, 0, 256],
            (0, 0, '127.0.0.1', 0): [0, 256, 0, 0],
            (0, 0, '127.0.0.1', 1): [0, 256, 0, 0],
            (0, 0, '127.0.0.1', 3): [0, 256, 0, 0],
        })

    @unittest.skipIf(sys.version_info >= (3,),
                     "Seed-specific tests don't work well on py3")
    def test_undispersable_zone_converge_on_balance(self):
        rb = ring.RingBuilder(8, 6, 0)
        dev_id = 0
        # 3 regions, 2 zone for each region, 1 server with only *one* device in
        # each zone (this is an absolutely pathological case)
        for r in range(3):
            for z in range(2):
                ip = '127.%s.%s.1' % (r, z)
                dev_id += 1
                rb.add_dev({'id': dev_id, 'region': r, 'zone': z,
                            'weight': 1000, 'ip': ip, 'port': 10000,
                            'device': 'd%s' % dev_id})
        rb.rebalance(seed=7)

        # sanity, all balanced and 0 dispersion
        self.assertEqual(rb.get_balance(), 0)
        self.assertEqual(rb.dispersion, 0)

        # add one device to the server in z1 for each region, N.B. when we
        # *balance* this topology we will have very bad dispersion (too much
        # weight in z1 compared to z2!)
        for r in range(3):
            z = 0
            ip = '127.%s.%s.1' % (r, z)
            dev_id += 1
            rb.add_dev({'id': dev_id, 'region': r, 'zone': z,
                        'weight': 1000, 'ip': ip, 'port': 10000,
                        'device': 'd%s' % dev_id})

        changed_part, _, _ = rb.rebalance(seed=7)

        # sanity, all part but only one replica moved to new devices
        self.assertEqual(changed_part, 2 ** 8)
        # so the first time, rings are still unbalanced becase we'll only move
        # one replica of each part.
        self.assertEqual(rb.get_balance(), 50.1953125)
        self.assertEqual(rb.dispersion, 16.6015625)

        # N.B. since we mostly end up grabbing parts by "weight forced" some
        # seeds given some specific ring state will randomly pick bad
        # part-replicas that end up going back down onto the same devices
        changed_part, _, _ = rb.rebalance(seed=7)
        self.assertEqual(changed_part, 14)
        # ... this isn't a really "desirable" behavior, but even with bad luck,
        # things do get better
        self.assertEqual(rb.get_balance(), 47.265625)
        self.assertEqual(rb.dispersion, 16.6015625)

        # but if you stick with it, eventually the next rebalance, will get to
        # move "the right" part-replicas, resulting in near optimal balance
        changed_part, _, _ = rb.rebalance(seed=7)
        self.assertEqual(changed_part, 240)
        self.assertEqual(rb.get_balance(), 0.390625)
        self.assertEqual(rb.dispersion, 16.6015625)

    @unittest.skipIf(sys.version_info >= (3,),
                     "Seed-specific tests don't work well on py3")
    def test_undispersable_server_converge_on_balance(self):
        rb = ring.RingBuilder(8, 6, 0)
        dev_id = 0
        # 3 zones, 2 server for each zone, 2 device for each server
        for z in range(3):
            for i in range(2):
                ip = '127.0.%s.%s' % (z, i + 1)
                for d in range(2):
                    dev_id += 1
                    rb.add_dev({'id': dev_id, 'region': 1, 'zone': z,
                                'weight': 1000, 'ip': ip, 'port': 10000,
                                'device': 'd%s' % dev_id})
        rb.rebalance(seed=7)

        # sanity, all balanced and 0 dispersion
        self.assertEqual(rb.get_balance(), 0)
        self.assertEqual(rb.dispersion, 0)

        # add one device for first server for each zone
        for z in range(3):
            ip = '127.0.%s.1' % z
            dev_id += 1
            rb.add_dev({'id': dev_id, 'region': 1, 'zone': z,
                        'weight': 1000, 'ip': ip, 'port': 10000,
                        'device': 'd%s' % dev_id})

        changed_part, _, _ = rb.rebalance(seed=7)

        # sanity, all part but only one replica moved to new devices
        self.assertEqual(changed_part, 2 ** 8)

        # but the first time, those are still unbalance becase ring builder
        # can move only one replica for each part
        self.assertEqual(rb.get_balance(), 16.9921875)
        self.assertEqual(rb.dispersion, 9.9609375)

        rb.rebalance(seed=7)

        # converge into around 0~1
        self.assertGreaterEqual(rb.get_balance(), 0)
        self.assertLess(rb.get_balance(), 1)
        # dispersion doesn't get any worse
        self.assertEqual(rb.dispersion, 9.9609375)

    def test_effective_overload(self):
        rb = ring.RingBuilder(8, 3, 1)
        # z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb'})
        # z1
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        # z2
        rb.add_dev({'id': 6, 'region': 0, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        # this ring requires overload
        required = rb.get_required_overload()
        self.assertGreater(required, 0.1)

        # and we'll use a little bit
        rb.set_overload(0.1)

        rb.rebalance(seed=7)
        rb.validate()

        # but with-out enough overload we're not dispersed
        self.assertGreater(rb.dispersion, 0)

        # add the other dev to z2
        rb.add_dev({'id': 8, 'region': 0, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdc'})
        # but also fail another device in the same!
        rb.remove_dev(6)

        # we still require overload
        required = rb.get_required_overload()
        self.assertGreater(required, 0.1)

        rb.pretend_min_part_hours_passed()
        rb.rebalance(seed=7)
        rb.validate()

        # ... and without enough we're full dispersed
        self.assertGreater(rb.dispersion, 0)

        # ok, let's fix z2's weight for real
        rb.add_dev({'id': 6, 'region': 0, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})

        # ... technically, we no longer require overload
        self.assertEqual(rb.get_required_overload(), 0.0)

        # so let's rebalance w/o resetting min_part_hours
        rb.rebalance(seed=7)
        rb.validate()

        # ... and that got it in one pass boo-yah!
        self.assertEqual(rb.dispersion, 0)

    def zone_weights_over_device_count(self):
        rb = ring.RingBuilder(8, 3, 1)
        # z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda'})
        # z1
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        # z2
        rb.add_dev({'id': 2, 'region': 0, 'zone': 2, 'weight': 200,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})

        rb.rebalance(seed=7)
        rb.validate()
        self.assertEqual(rb.dispersion, 0)
        self.assertAlmostEqual(rb.get_balance(), (1.0 / 3.0) * 100)

    def test_more_devices_than_replicas_validation_when_removed_dev(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'weight': 1.0, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'weight': 1.0, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'weight': 1.0, 'device': 'sdc'})
        rb.rebalance()
        rb.remove_dev(2)
        with self.assertRaises(ValueError) as e:
            rb.set_dev_weight(2, 1)
        msg = "Can not set weight of dev_id 2 because it is marked " \
            "for removal"
        self.assertIn(msg, str(e.exception))
        with self.assertRaises(exceptions.RingValidationError) as e:
            rb.rebalance()
        msg = 'Replica count of 3 requires more than 2 devices'
        self.assertIn(msg, str(e.exception))

    def _add_dev_delete_first_n(self, add_dev_count, n):
        rb = ring.RingBuilder(8, 3, 1)
        dev_names = ['sda', 'sdb', 'sdc', 'sdd', 'sde', 'sdf']
        for i in range(add_dev_count):
            if i < len(dev_names):
                dev_name = dev_names[i]
            else:
                dev_name = 'sda'
            rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                        'port': 6200, 'weight': 1.0, 'device': dev_name})
        rb.rebalance()
        if (n > 0):
            rb.pretend_min_part_hours_passed()
            # remove first n
            for i in range(n):
                rb.remove_dev(i)
            rb.pretend_min_part_hours_passed()
            rb.rebalance()
        return rb

    def test_reuse_of_dev_holes_without_id(self):
        # try with contiguous holes at beginning
        add_dev_count = 6
        rb = self._add_dev_delete_first_n(add_dev_count, add_dev_count - 3)
        new_dev_id = rb.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                                 'port': 6200, 'weight': 1.0,
                                 'device': 'sda'})
        self.assertLess(new_dev_id, add_dev_count)

        # try with non-contiguous holes
        # [0, 1, None, 3, 4, None]
        rb2 = ring.RingBuilder(8, 3, 1)
        for i in range(6):
            rb2.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                         'port': 6200, 'weight': 1.0, 'device': 'sda'})
        rb2.rebalance()
        rb2.pretend_min_part_hours_passed()
        rb2.remove_dev(2)
        rb2.remove_dev(5)
        rb2.pretend_min_part_hours_passed()
        rb2.rebalance()
        first = rb2.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                             'port': 6200, 'weight': 1.0, 'device': 'sda'})
        second = rb2.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                              'port': 6200, 'weight': 1.0, 'device': 'sda'})
        # add a new one (without reusing a hole)
        third = rb2.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                             'port': 6200, 'weight': 1.0, 'device': 'sda'})
        self.assertEqual(first, 2)
        self.assertEqual(second, 5)
        self.assertEqual(third, 6)

    def test_reuse_of_dev_holes_with_id(self):
        add_dev_count = 6
        rb = self._add_dev_delete_first_n(add_dev_count, add_dev_count - 3)
        # add specifying id
        exp_new_dev_id = 2
        # [dev, dev, None, dev, dev, None]
        try:
            new_dev_id = rb.add_dev({'id': exp_new_dev_id, 'region': 0,
                                     'zone': 0, 'ip': '127.0.0.1',
                                     'port': 6200, 'weight': 1.0,
                                     'device': 'sda'})
            self.assertEqual(new_dev_id, exp_new_dev_id)
        except exceptions.DuplicateDeviceError:
            self.fail("device hole not reused")

    def test_prepare_increase_partition_power(self):
        ring_file = os.path.join(self.testdir, 'test_partpower.ring.gz')

        rb = ring.RingBuilder(8, 3.0, 1)
        self.assertEqual(rb.part_power, 8)

        # add more devices than replicas to the ring
        for i in range(10):
            dev = "sdx%s" % i
            rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 1,
                        'ip': '127.0.0.1', 'port': 10000, 'device': dev})
        rb.rebalance(seed=1)

        self.assertFalse(rb.cancel_increase_partition_power())
        self.assertEqual(rb.part_power, 8)
        self.assertIsNone(rb.next_part_power)

        self.assertFalse(rb.finish_increase_partition_power())
        self.assertEqual(rb.part_power, 8)
        self.assertIsNone(rb.next_part_power)

        self.assertTrue(rb.prepare_increase_partition_power())
        self.assertEqual(rb.part_power, 8)
        self.assertEqual(rb.next_part_power, 9)

        # Save .ring.gz, and load ring from it to ensure prev/next is set
        rd = rb.get_ring()
        rd.save(ring_file)

        r = ring.Ring(ring_file)
        expected_part_shift = 32 - 8
        self.assertEqual(expected_part_shift, r._part_shift)
        self.assertEqual(9, r.next_part_power)

    def test_increase_partition_power(self):
        rb = ring.RingBuilder(8, 3.0, 1)
        self.assertEqual(rb.part_power, 8)

        # add more devices than replicas to the ring
        for i in range(10):
            dev = "sdx%s" % i
            rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 1,
                        'ip': '127.0.0.1', 'port': 10000, 'device': dev})
        rb.rebalance(seed=1)

        # Let's save the ring, and get the nodes for an object
        ring_file = os.path.join(self.testdir, 'test_partpower.ring.gz')
        rd = rb.get_ring()
        rd.save(ring_file)
        r = ring.Ring(ring_file)
        old_part, old_nodes = r.get_nodes("acc", "cont", "obj")
        old_version = rb.version

        self.assertTrue(rb.prepare_increase_partition_power())
        self.assertTrue(rb.increase_partition_power())
        rb.validate()
        changed_parts, _balance, removed_devs = rb.rebalance()

        self.assertEqual(changed_parts, 0)
        self.assertEqual(removed_devs, 0)

        # Make sure cancellation is not possible
        # after increasing the partition power
        self.assertFalse(rb.cancel_increase_partition_power())

        old_ring = r
        rd = rb.get_ring()
        rd.save(ring_file)
        r = ring.Ring(ring_file)
        new_part, new_nodes = r.get_nodes("acc", "cont", "obj")

        # sanity checks
        self.assertEqual(9, rb.part_power)
        self.assertEqual(9, rb.next_part_power)
        self.assertEqual(rb.version, old_version + 3)

        # make sure there is always the same device assigned to every pair of
        # partitions
        for replica in rb._replica2part2dev:
            for part in range(0, len(replica), 2):
                dev = replica[part]
                next_dev = replica[part + 1]
                self.assertEqual(dev, next_dev)

        # same for last_part moves
        for part in range(0, rb.parts, 2):
            this_last_moved = rb._last_part_moves[part]
            next_last_moved = rb._last_part_moves[part + 1]
            self.assertEqual(this_last_moved, next_last_moved)

        for i in range(100):
            suffix = uuid.uuid4()
            account = 'account_%s' % suffix
            container = 'container_%s' % suffix
            obj = 'obj_%s' % suffix
            old_part, old_nodes = old_ring.get_nodes(account, container, obj)
            new_part, new_nodes = r.get_nodes(account, container, obj)
            # Due to the increased partition power, the partition each object
            # is assigned to has changed. If the old partition was X, it will
            # now be either located in 2*X or 2*X+1
            self.assertIn(new_part, [old_part * 2, old_part * 2 + 1])

            # Importantly, we expect the objects to be placed on the same
            # nodes after increasing the partition power
            self.assertEqual(old_nodes, new_nodes)

    def test_finalize_increase_partition_power(self):
        ring_file = os.path.join(self.testdir, 'test_partpower.ring.gz')

        rb = ring.RingBuilder(8, 3.0, 1)
        self.assertEqual(rb.part_power, 8)

        # add more devices than replicas to the ring
        for i in range(10):
            dev = "sdx%s" % i
            rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 1,
                        'ip': '127.0.0.1', 'port': 10000, 'device': dev})
        rb.rebalance(seed=1)

        self.assertTrue(rb.prepare_increase_partition_power())

        # Make sure this doesn't do any harm before actually increasing the
        # partition power
        self.assertFalse(rb.finish_increase_partition_power())
        self.assertEqual(rb.next_part_power, 9)

        self.assertTrue(rb.increase_partition_power())

        self.assertFalse(rb.prepare_increase_partition_power())
        self.assertEqual(rb.part_power, 9)
        self.assertEqual(rb.next_part_power, 9)

        self.assertTrue(rb.finish_increase_partition_power())

        self.assertEqual(rb.part_power, 9)
        self.assertIsNone(rb.next_part_power)

        # Save .ring.gz, and load ring from it to ensure prev/next is set
        rd = rb.get_ring()
        rd.save(ring_file)

        r = ring.Ring(ring_file)
        expected_part_shift = 32 - 9
        self.assertEqual(expected_part_shift, r._part_shift)
        self.assertIsNone(r.next_part_power)

    def test_prepare_increase_partition_power_failed(self):
        rb = ring.RingBuilder(8, 3.0, 1)
        self.assertEqual(rb.part_power, 8)

        self.assertTrue(rb.prepare_increase_partition_power())
        self.assertEqual(rb.next_part_power, 9)

        # next_part_power is still set, do not increase again
        self.assertFalse(rb.prepare_increase_partition_power())
        self.assertEqual(rb.next_part_power, 9)

    def test_increase_partition_power_failed(self):
        rb = ring.RingBuilder(8, 3.0, 1)
        self.assertEqual(rb.part_power, 8)

        # add more devices than replicas to the ring
        for i in range(10):
            dev = "sdx%s" % i
            rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 1,
                        'ip': '127.0.0.1', 'port': 10000, 'device': dev})
        rb.rebalance(seed=1)

        # next_part_power not set, can't increase the part power
        self.assertFalse(rb.increase_partition_power())
        self.assertEqual(rb.part_power, 8)

        self.assertTrue(rb.prepare_increase_partition_power())

        self.assertTrue(rb.increase_partition_power())
        self.assertEqual(rb.part_power, 9)

        # part_power already increased
        self.assertFalse(rb.increase_partition_power())
        self.assertEqual(rb.part_power, 9)

    def test_cancel_increase_partition_power(self):
        rb = ring.RingBuilder(8, 3.0, 1)
        self.assertEqual(rb.part_power, 8)

        # add more devices than replicas to the ring
        for i in range(10):
            dev = "sdx%s" % i
            rb.add_dev({'id': i, 'region': 0, 'zone': 0, 'weight': 1,
                        'ip': '127.0.0.1', 'port': 10000, 'device': dev})
        rb.rebalance(seed=1)

        old_version = rb.version
        self.assertTrue(rb.prepare_increase_partition_power())

        # sanity checks
        self.assertEqual(8, rb.part_power)
        self.assertEqual(9, rb.next_part_power)
        self.assertEqual(rb.version, old_version + 1)

        self.assertTrue(rb.cancel_increase_partition_power())
        rb.validate()

        self.assertEqual(8, rb.part_power)
        self.assertEqual(8, rb.next_part_power)
        self.assertEqual(rb.version, old_version + 2)


class TestGetRequiredOverload(unittest.TestCase):

    maxDiff = None

    def test_none_needed(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 1,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        # 4 equal-weight devs and 3 replicas: this can be balanced without
        # resorting to overload at all
        self.assertAlmostEqual(rb.get_required_overload(), 0)

        expected = {
            (0, 0, '127.0.0.1', 0): 0.75,
            (0, 0, '127.0.0.1', 1): 0.75,
            (0, 0, '127.0.0.1', 2): 0.75,
            (0, 0, '127.0.0.1', 3): 0.75,
        }

        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected, {
            tier: weighted
            for (tier, weighted) in weighted_replicas.items()
            if len(tier) == 4})
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 4})

        # since no overload is needed, target_replicas is the same
        rb.set_overload(0.10)
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 4})

        # ... no matter how high you go!
        rb.set_overload(100.0)
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 4})

        # 3 equal-weight devs and 3 replicas: this can also be balanced
        rb.remove_dev(3)
        self.assertAlmostEqual(rb.get_required_overload(), 0)

        expected = {
            (0, 0, '127.0.0.1', 0): 1.0,
            (0, 0, '127.0.0.1', 1): 1.0,
            (0, 0, '127.0.0.1', 2): 1.0,
        }

        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 4})
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 4})

        # ... still no overload
        rb.set_overload(100.0)
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 4})

    def test_equal_replica_and_devices_count_ignore_weights(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 7.47,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 5.91,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 6.44,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        expected = {
            0: 1.0,
            1: 1.0,
            2: 1.0,
        }
        # simplicity itself
        self.assertEqual(expected, {
            t[-1]: r for (t, r) in
            rb._build_weighted_replicas_by_tier().items()
            if len(t) == 4})
        self.assertEqual(expected, {
            t[-1]: r for (t, r) in
            rb._build_wanted_replicas_by_tier().items()
            if len(t) == 4})
        self.assertEqual(expected, {
            t[-1]: r for (t, r) in
            rb._build_target_replicas_by_tier().items()
            if len(t) == 4})
        # ... no overload required!
        self.assertEqual(0, rb.get_required_overload())

        rb.rebalance()
        expected = {
            0: 256,
            1: 256,
            2: 256,
        }
        self.assertEqual(expected, {d['id']: d['parts'] for d in
                                    rb._iter_devs()})

    def test_small_zone(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 4,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 4,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 4,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'weight': 4,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 3,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        expected = {
            (0, 0): 1.0434782608695652,
            (0, 1): 1.0434782608695652,
            (0, 2): 0.9130434782608695,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})
        expected = {
            (0, 0): 1.0,
            (0, 1): 1.0,
            (0, 2): 1.0,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 2})

        # the device tier is interesting because one of the devices in zone
        # two has a different weight
        expected = {
            0: 0.5217391304347826,
            1: 0.5217391304347826,
            2: 0.5217391304347826,
            3: 0.5217391304347826,
            4: 0.5217391304347826,
            5: 0.3913043478260869,
        }
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 4})

        # ... but, each pair of devices still needs to hold a whole
        # replicanth; which we'll try distribute fairly among devices in
        # zone 2, so that they can share the burden and ultimately the
        # required overload will be as small as possible.
        expected = {
            0: 0.5,
            1: 0.5,
            2: 0.5,
            3: 0.5,
            4: 0.5714285714285715,
            5: 0.42857142857142855,
        }
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 4})

        # full dispersion requires zone two's devices to eat more than
        # they're weighted for
        self.assertAlmostEqual(rb.get_required_overload(), 0.095238,
                               delta=1e-5)

        # so... if we give it enough overload it we should get full dispersion
        rb.set_overload(0.1)
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 4})

    def test_multiple_small_zones(self):
        rb = ring.RingBuilder(8, 3, 1)

        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 150,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 150,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 10, 'region': 0, 'zone': 1, 'weight': 150,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 3, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 3, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        expected = {
            (0, 0): 2.1052631578947367,
            (0, 1): 0.47368421052631576,
            (0, 2): 0.21052631578947367,
            (0, 3): 0.21052631578947367,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})

        # without any overload, we get weight
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: r
                          for (tier, r) in target_replicas.items()
                          if len(tier) == 2})

        expected = {
            (0, 0): 1.0,
            (0, 1): 1.0,
            (0, 2): 0.49999999999999994,
            (0, 3): 0.49999999999999994,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {t: r
                          for (t, r) in wanted_replicas.items()
                          if len(t) == 2})

        self.assertEqual(1.3750000000000002, rb.get_required_overload())

        # with enough overload we get the full dispersion
        rb.set_overload(1.5)
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: r
                          for (tier, r) in target_replicas.items()
                          if len(tier) == 2})

        # with not enough overload, we get somewhere in the middle
        rb.set_overload(1.0)
        expected = {
            (0, 0): 1.3014354066985647,
            (0, 1): 0.8564593301435406,
            (0, 2): 0.4210526315789473,
            (0, 3): 0.4210526315789473,
        }
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: r
                          for (tier, r) in target_replicas.items()
                          if len(tier) == 2})

    def test_big_zone(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 60,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 60,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'weight': 60,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 60,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 3, 'weight': 60,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 3, 'weight': 60,
                    'ip': '127.0.0.3', 'port': 10000, 'device': 'sdb'})

        expected = {
            (0, 0): 1.0714285714285714,
            (0, 1): 0.6428571428571429,
            (0, 2): 0.6428571428571429,
            (0, 3): 0.6428571428571429,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})
        expected = {
            (0, 0): 1.0,
            (0, 1): 0.6666666666666667,
            (0, 2): 0.6666666666666667,
            (0, 3): 0.6666666666666667,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 2})

        # when all the devices and servers in a zone are evenly weighted
        # it will accurately proxy their required overload, all the
        # zones besides 0 require the same overload
        t = random.choice([t for t in weighted_replicas
                           if len(t) == 2
                           and t[1] != 0])
        expected_overload = ((wanted_replicas[t] - weighted_replicas[t])
                             / weighted_replicas[t])
        self.assertAlmostEqual(rb.get_required_overload(),
                               expected_overload)

        # but if you only give it out half of that
        rb.set_overload(expected_overload / 2.0)
        # ... you can expect it's not going to full disperse
        expected = {
            (0, 0): 1.0357142857142856,
            (0, 1): 0.6547619047619049,
            (0, 2): 0.6547619047619049,
            (0, 3): 0.6547619047619049,
        }
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 2})

    def test_enormous_zone(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'weight': 60,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'weight': 60,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 2, 'weight': 60,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 2, 'weight': 60,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 8, 'region': 0, 'zone': 3, 'weight': 60,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 3, 'weight': 60,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        expected = {
            (0, 0): 2.542372881355932,
            (0, 1): 0.15254237288135591,
            (0, 2): 0.15254237288135591,
            (0, 3): 0.15254237288135591,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})

        expected = {
            (0, 0): 1.0,
            (0, 1): 0.6666666666666667,
            (0, 2): 0.6666666666666667,
            (0, 3): 0.6666666666666667,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 2})

        # ouch, those "tiny" devices need to hold 3x more than their
        # weighted for!
        self.assertAlmostEqual(rb.get_required_overload(), 3.370370,
                               delta=1e-5)

        # let's get a little crazy, and let devices eat up to 1x more than
        # their capacity is weighted for - see how far that gets us...
        rb.set_overload(1)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            (0, 0): 2.084745762711864,
            (0, 1): 0.30508474576271183,
            (0, 2): 0.30508474576271183,
            (0, 3): 0.30508474576271183,
        }
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 2})

    def test_two_big_two_small(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'weight': 45,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'weight': 45,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 6, 'region': 0, 'zone': 3, 'weight': 35,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 3, 'weight': 35,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})

        expected = {
            (0, 0): 1.0714285714285714,
            (0, 1): 1.0714285714285714,
            (0, 2): 0.48214285714285715,
            (0, 3): 0.375,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})

        expected = {
            (0, 0): 1.0,
            (0, 1): 1.0,
            (0, 2): 0.5625,
            (0, 3): 0.43749999999999994,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 2})

        # I'm not sure it's significant or coincidental that the devices
        # in zone 2 & 3 who end up splitting the 3rd replica turn out to
        # need to eat ~1/6th extra replicanths
        self.assertAlmostEqual(rb.get_required_overload(), 1.0 / 6.0)

        # ... *so* 10% isn't *quite* enough
        rb.set_overload(0.1)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            (0, 0): 1.0285714285714285,
            (0, 1): 1.0285714285714285,
            (0, 2): 0.5303571428571429,
            (0, 3): 0.4125,
        }
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 2})

        # ... but 20% will do the trick!
        rb.set_overload(0.2)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            (0, 0): 1.0,
            (0, 1): 1.0,
            (0, 2): 0.5625,
            (0, 3): 0.43749999999999994,
        }
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 2})

    def test_multiple_replicas_each(self):
        rb = ring.RingBuilder(8, 7, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 80,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 80,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 80,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'weight': 80,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'weight': 80,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sde'})

        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'weight': 70,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'weight': 70,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'weight': 70,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 8, 'region': 0, 'zone': 1, 'weight': 70,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdd'})

        expected = {
            (0, 0): 4.117647058823529,
            (0, 1): 2.8823529411764706,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})

        expected = {
            (0, 0): 4.0,
            (0, 1): 3.0,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 2})

        # I guess 2.88 => 3.0 is about a 4% increase
        self.assertAlmostEqual(rb.get_required_overload(),
                               0.040816326530612256)

        # ... 10% is plenty enough here
        rb.set_overload(0.1)
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 2})

    def test_small_extra_server_in_zone_with_multiple_replicas(self):
        rb = ring.RingBuilder(8, 5, 1)

        # z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 1000})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sdb', 'weight': 1000})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sdc', 'weight': 1000})

        # z1
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 1000})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sdb', 'weight': 1000})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sdc', 'weight': 1000})

        # z1 - extra small server
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'ip': '127.0.0.3',
                    'port': 6200, 'device': 'sda', 'weight': 50})

        expected = {
            (0, 0): 2.479338842975207,
            (0, 1): 2.5206611570247937,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected, {t: r for (t, r) in
                                    weighted_replicas.items()
                                    if len(t) == 2})

        # dispersion is fine with this at the zone tier
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected, {t: r for (t, r) in
                                    wanted_replicas.items()
                                    if len(t) == 2})

        # ... but not ok with that tiny server
        expected = {
            '127.0.0.1': 2.479338842975207,
            '127.0.0.2': 1.5206611570247937,
            '127.0.0.3': 1.0,
        }
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    wanted_replicas.items()
                                    if len(t) == 3})

        self.assertAlmostEqual(23.2, rb.get_required_overload())

    def test_multiple_replicas_in_zone_with_single_device(self):
        rb = ring.RingBuilder(8, 5, 0)
        # z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        # z1
        rb.add_dev({'id': 1, 'region': 0, 'zone': 1, 'ip': '127.0.1.1',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'ip': '127.0.1.1',
                    'port': 6200, 'device': 'sdb', 'weight': 100})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'ip': '127.0.1.2',
                    'port': 6200, 'device': 'sdc', 'weight': 100})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'ip': '127.0.1.2',
                    'port': 6200, 'device': 'sdd', 'weight': 100})

        # first things first, make sure we do this right
        rb.rebalance()

        # each device get's a sing replica of every part
        expected = {
            0: 256,
            1: 256,
            2: 256,
            3: 256,
            4: 256,
        }
        self.assertEqual(expected, {d['id']: d['parts']
                                    for d in rb._iter_devs()})

        # but let's make sure we're thinking about it right too
        expected = {
            0: 1.0,
            1: 1.0,
            2: 1.0,
            3: 1.0,
            4: 1.0,
        }

        # by weight everyone is equal
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    weighted_replicas.items()
                                    if len(t) == 4})

        # wanted might have liked to have fewer replicas in z1, but the
        # single device in z0 limits us one replica per device
        with rb.debug():
            wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    wanted_replicas.items()
                                    if len(t) == 4})

        # even with some overload - still one replica per device
        rb.set_overload(1.0)
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    target_replicas.items()
                                    if len(t) == 4})

        # when overload can not change the outcome none is required
        self.assertEqual(0.0, rb.get_required_overload())
        # even though dispersion is terrible (in z1 particularly)
        self.assertEqual(20.0, rb.dispersion)

    def test_one_big_guy_does_not_spoil_his_buddy(self):
        rb = ring.RingBuilder(8, 3, 0)

        # z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        # z1
        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'ip': '127.0.1.1',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'ip': '127.0.1.2',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        # z2
        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'ip': '127.0.2.1',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'ip': '127.0.2.2',
                    'port': 6200, 'device': 'sda', 'weight': 10000})

        # obviously d5 gets one whole replica; the other two replicas
        # are split evenly among the five other devices
        # (i.e. ~0.4 replicanths for each 100 units of weight)
        expected = {
            0: 0.39999999999999997,
            1: 0.39999999999999997,
            2: 0.39999999999999997,
            3: 0.39999999999999997,
            4: 0.39999999999999997,
            5: 1.0,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    weighted_replicas.items()
                                    if len(t) == 4})

        # with no overload we get the "balanced" placement
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    target_replicas.items()
                                    if len(t) == 4})

        # but in reality, these devices having such disparate weights
        # leads to a *terrible* balance even w/o overload!
        rb.rebalance(seed=9)
        self.assertEqual(rb.get_balance(), 1308.2031249999998)

        # even though part assignment is pretty reasonable
        expected = {
            0: 103,
            1: 102,
            2: 103,
            3: 102,
            4: 102,
            5: 256,
        }
        self.assertEqual(expected, {
            d['id']: d['parts'] for d in rb._iter_devs()})

        # so whats happening is the small devices are holding *way* more
        # *real* parts than their *relative* portion of the weight would
        # like them too!
        expected = {
            0: 1308.2031249999998,
            1: 1294.5312499999998,
            2: 1308.2031249999998,
            3: 1294.5312499999998,
            4: 1294.5312499999998,
            5: -65.0,

        }
        self.assertEqual(expected, rb._build_balance_per_dev())

        # increasing overload moves towards one replica in each tier
        rb.set_overload(0.20)
        expected = {
            0: 0.48,
            1: 0.48,
            2: 0.48,
            3: 0.48,
            4: 0.30857142857142855,
            5: 0.7714285714285714,
        }
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    target_replicas.items()
                                    if len(t) == 4})

        # ... and as always increasing overload makes balance *worse*
        rb.rebalance(seed=17)
        self.assertEqual(rb.get_balance(), 1581.6406249999998)

        # but despite the overall trend toward imbalance, in the tier with the
        # huge device, we want to see the small device (d4) try to shed parts
        # as effectively as it can to the huge device in the same tier (d5)
        # this is a useful behavior anytime when for whatever reason a device
        # w/i a tier wants parts from another device already in the same tier
        # another example is `test_one_small_guy_does_not_spoil_his_buddy`
        expected = {
            0: 123,
            1: 123,
            2: 123,
            3: 123,
            4: 79,
            5: 197,
        }
        self.assertEqual(expected, {
            d['id']: d['parts'] for d in rb._iter_devs()})

        # *see*, at least *someones* balance is getting better!
        expected = {
            0: 1581.6406249999998,
            1: 1581.6406249999998,
            2: 1581.6406249999998,
            3: 1581.6406249999998,
            4: 980.078125,
            5: -73.06640625,
        }
        self.assertEqual(expected, rb._build_balance_per_dev())

    def test_one_small_guy_does_not_spoil_his_buddy(self):
        rb = ring.RingBuilder(8, 3, 0)

        # z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 10000})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 10000})
        # z1
        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'ip': '127.0.1.1',
                    'port': 6200, 'device': 'sda', 'weight': 10000})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'ip': '127.0.1.2',
                    'port': 6200, 'device': 'sda', 'weight': 10000})
        # z2
        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'ip': '127.0.2.1',
                    'port': 6200, 'device': 'sda', 'weight': 10000})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'ip': '127.0.2.2',
                    'port': 6200, 'device': 'sda', 'weight': 100})

        # it's almost like 3.0 / 5 ~= 0.6, but that one little guy get's
        # his fair share
        expected = {
            0: 0.5988023952095808,
            1: 0.5988023952095808,
            2: 0.5988023952095808,
            3: 0.5988023952095808,
            4: 0.5988023952095808,
            5: 0.005988023952095809,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    weighted_replicas.items()
                                    if len(t) == 4})

        # with no overload we get a nice balanced placement
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    target_replicas.items()
                                    if len(t) == 4})
        rb.rebalance(seed=9)

        # part placement looks goods
        expected = {
            0: 154,
            1: 153,
            2: 153,
            3: 153,
            4: 153,
            5: 2,
        }
        self.assertEqual(expected, {
            d['id']: d['parts'] for d in rb._iter_devs()})

        # ... balance is a little lumpy on the small guy since he wants
        # one and a half parts :\
        expected = {
            0: 0.4609375000000142,
            1: -0.1914062499999858,
            2: -0.1914062499999858,
            3: -0.1914062499999858,
            4: -0.1914062499999858,
            5: 30.46875,
        }
        self.assertEqual(expected, rb._build_balance_per_dev())

        self.assertEqual(rb.get_balance(), 30.46875)

        # increasing overload moves towards one replica in each tier
        rb.set_overload(0.3)
        expected = {
            0: 0.553443113772455,
            1: 0.553443113772455,
            2: 0.553443113772455,
            3: 0.553443113772455,
            4: 0.778443113772455,
            5: 0.007784431137724551,
        }
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    target_replicas.items()
                                    if len(t) == 4})
        # ... and as always increasing overload makes balance *worse*
        rb.rebalance(seed=12)
        self.assertEqual(rb.get_balance(), 30.46875)

        # the little guy it really struggling to take his share tho
        expected = {
            0: 142,
            1: 141,
            2: 142,
            3: 141,
            4: 200,
            5: 2,
        }
        self.assertEqual(expected, {
            d['id']: d['parts'] for d in rb._iter_devs()})
        # ... and you can see it in the balance!
        expected = {
            0: -7.367187499999986,
            1: -8.019531249999986,
            2: -7.367187499999986,
            3: -8.019531249999986,
            4: 30.46875,
            5: 30.46875,
        }
        self.assertEqual(expected, rb._build_balance_per_dev())

        rb.set_overload(0.5)
        expected = {
            0: 0.5232035928143712,
            1: 0.5232035928143712,
            2: 0.5232035928143712,
            3: 0.5232035928143712,
            4: 0.8982035928143712,
            5: 0.008982035928143714,
        }
        target_replicas = rb._build_target_replicas_by_tier()
        self.assertEqual(expected, {t[-1]: r for (t, r) in
                                    target_replicas.items()
                                    if len(t) == 4})

        # because the device is so small, balance get's bad quick
        rb.rebalance(seed=17)
        self.assertEqual(rb.get_balance(), 95.703125)

        # but despite the overall trend toward imbalance, the little guy
        # isn't really taking on many new parts!
        expected = {
            0: 134,
            1: 134,
            2: 134,
            3: 133,
            4: 230,
            5: 3,
        }
        self.assertEqual(expected, {
            d['id']: d['parts'] for d in rb._iter_devs()})

        # *see*, at everyone's balance is getting worse *together*!
        expected = {
            0: -12.585937499999986,
            1: -12.585937499999986,
            2: -12.585937499999986,
            3: -13.238281249999986,
            4: 50.0390625,
            5: 95.703125,
        }
        self.assertEqual(expected, rb._build_balance_per_dev())

    def test_two_servers_with_more_than_one_replica(self):
        rb = ring.RingBuilder(8, 3, 0)
        # z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 60})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 60})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'ip': '127.0.0.3',
                    'port': 6200, 'device': 'sda', 'weight': 60})
        # z1
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'ip': '127.0.1.1',
                    'port': 6200, 'device': 'sda', 'weight': 80})
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'ip': '127.0.1.2',
                    'port': 6200, 'device': 'sda', 'weight': 128})
        # z2
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'ip': '127.0.2.1',
                    'port': 6200, 'device': 'sda', 'weight': 80})
        rb.add_dev({'id': 6, 'region': 0, 'zone': 2, 'ip': '127.0.2.2',
                    'port': 6200, 'device': 'sda', 'weight': 240})

        rb.set_overload(0.1)
        rb.rebalance()
        self.assertEqual(12.161458333333343, rb.get_balance())

        replica_plan = rb._build_target_replicas_by_tier()
        for dev in rb._iter_devs():
            tier = (dev['region'], dev['zone'], dev['ip'], dev['id'])
            expected_parts = replica_plan[tier] * rb.parts
            self.assertAlmostEqual(dev['parts'], expected_parts,
                                   delta=1)

    def test_multi_zone_with_failed_device(self):
        rb = ring.RingBuilder(8, 3, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})

        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})

        rb.add_dev({'id': 4, 'region': 0, 'zone': 2, 'ip': '127.0.0.3',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 2, 'ip': '127.0.0.3',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})

        # sanity, balanced and dispersed
        expected = {
            (0, 0): 1.0,
            (0, 1): 1.0,
            (0, 2): 1.0,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 2})

        self.assertEqual(rb.get_required_overload(), 0.0)

        # fail a device in zone 2
        rb.remove_dev(4)

        expected = {
            0: 0.6,
            1: 0.6,
            2: 0.6,
            3: 0.6,
            5: 0.6,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 4})

        expected = {
            0: 0.5,
            1: 0.5,
            2: 0.5,
            3: 0.5,
            5: 1.0,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 4})

        # does this make sense?  every zone was holding 1/3rd of the
        # replicas, so each device was 1/6th, remove a device and
        # suddenly it's holding *both* sixths which is 2/3rds?
        self.assertAlmostEqual(rb.get_required_overload(), 2.0 / 3.0)

        # 10% isn't nearly enough
        rb.set_overload(0.1)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            0: 0.585,
            1: 0.585,
            2: 0.585,
            3: 0.585,
            5: 0.6599999999999999,
        }
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 4})

        # 50% isn't even enough
        rb.set_overload(0.5)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            0: 0.525,
            1: 0.525,
            2: 0.525,
            3: 0.525,
            5: 0.8999999999999999,
        }
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 4})

        # even 65% isn't enough (but it's getting closer)
        rb.set_overload(0.65)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            0: 0.5025000000000001,
            1: 0.5025000000000001,
            2: 0.5025000000000001,
            3: 0.5025000000000001,
            5: 0.99,
        }
        self.assertEqual(expected,
                         {tier[3]: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 4})

    def test_balanced_zones_unbalanced_servers(self):
        rb = ring.RingBuilder(8, 3, 1)
        # zone 0 server 127.0.0.1
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 3000})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sdb', 'weight': 3000})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 3000})
        # zone 1 server 127.0.0.2
        rb.add_dev({'id': 4, 'region': 0, 'zone': 1, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 4000})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 1, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sdb', 'weight': 4000})
        # zone 1 (again) server 127.0.0.3
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'ip': '127.0.0.3',
                    'port': 6200, 'device': 'sda', 'weight': 1000})

        weighted_replicas = rb._build_weighted_replicas_by_tier()

        # zones are evenly weighted
        expected = {
            (0, 0): 1.5,
            (0, 1): 1.5,
        }
        self.assertEqual(expected,
                         {tier: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 2})

        # ... but servers are not
        expected = {
            '127.0.0.1': 1.5,
            '127.0.0.2': 1.3333333333333333,
            '127.0.0.3': 0.16666666666666666,
        }
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 3})

        # make sure wanted will even it out
        expected = {
            '127.0.0.1': 1.5,
            '127.0.0.2': 1.0,
            '127.0.0.3': 0.4999999999999999,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 3})

        # so it wants 1/6th and eats 1/2 - that's 2/6ths more than it
        # wants which is a 200% increase
        self.assertAlmostEqual(rb.get_required_overload(), 2.0)

        # the overload doesn't effect the tiers that are already dispersed
        rb.set_overload(1)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            '127.0.0.1': 1.5,
            # notice with half the overload 1/6th replicanth swapped servers
            '127.0.0.2': 1.1666666666666665,
            '127.0.0.3': 0.3333333333333333,
        }
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 3})

    def test_adding_second_zone(self):
        rb = ring.RingBuilder(3, 3, 1)
        # zone 0 server 127.0.0.1
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})
        # zone 0 server 127.0.0.2
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})
        # zone 0 server 127.0.0.3
        rb.add_dev({'id': 4, 'region': 0, 'zone': 0, 'ip': '127.0.0.3',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 5, 'region': 0, 'zone': 0, 'ip': '127.0.0.3',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})

        # sanity, balanced and dispersed
        expected = {
            '127.0.0.1': 1.0,
            '127.0.0.2': 1.0,
            '127.0.0.3': 1.0,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 3})
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 3})

        self.assertEqual(rb.get_required_overload(), 0)

        # start adding a second zone

        # zone 1 server 127.0.1.1
        rb.add_dev({'id': 6, 'region': 0, 'zone': 1, 'ip': '127.0.1.1',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        rb.add_dev({'id': 7, 'region': 0, 'zone': 1, 'ip': '127.0.1.1',
                    'port': 6200, 'device': 'sdb', 'weight': 100})
        # zone 1 server 127.0.1.2
        rb.add_dev({'id': 8, 'region': 0, 'zone': 1, 'ip': '127.0.1.2',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        rb.add_dev({'id': 9, 'region': 0, 'zone': 1, 'ip': '127.0.1.2',
                    'port': 6200, 'device': 'sdb', 'weight': 100})
        # zone 1 server 127.0.1.3
        rb.add_dev({'id': 10, 'region': 0, 'zone': 1, 'ip': '127.0.1.3',
                    'port': 6200, 'device': 'sda', 'weight': 100})
        rb.add_dev({'id': 11, 'region': 0, 'zone': 1, 'ip': '127.0.1.3',
                    'port': 6200, 'device': 'sdb', 'weight': 100})

        # this messes things up pretty royally
        expected = {
            '127.0.0.1': 0.9523809523809523,
            '127.0.0.2': 0.9523809523809523,
            '127.0.0.3': 0.9523809523809523,
            '127.0.1.1': 0.047619047619047616,
            '127.0.1.2': 0.047619047619047616,
            '127.0.1.3': 0.047619047619047616,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in weighted_replicas.items()
                          if len(tier) == 3})
        expected = {
            '127.0.0.1': 0.6666666666666667,
            '127.0.0.2': 0.6666666666666667,
            '127.0.0.3': 0.6666666666666667,
            '127.0.1.1': 0.3333333333333333,
            '127.0.1.2': 0.3333333333333333,
            '127.0.1.3': 0.3333333333333333,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in wanted_replicas.items()
                          if len(tier) == 3})

        # so dispersion would require these devices hold 6x more than
        # prescribed by weight, defeating any attempt at gradually
        # anything
        self.assertAlmostEqual(rb.get_required_overload(), 6.0)

        # so let's suppose we only allow for 10% overload
        rb.set_overload(0.10)
        target_replicas = rb._build_target_replicas_by_tier()

        expected = {
            # we expect servers in zone 0 to be between 0.952 and 0.666
            '127.0.0.1': 0.9476190476190476,
            '127.0.0.2': 0.9476190476190476,
            '127.0.0.3': 0.9476190476190476,
            # we expect servers in zone 1 to be between 0.0476 and 0.333
            # and in fact its ~10% increase (very little compared to 6x!)
            '127.0.1.1': 0.052380952380952375,
            '127.0.1.2': 0.052380952380952375,
            '127.0.1.3': 0.052380952380952375,
        }
        self.assertEqual(expected,
                         {tier[2]: weighted
                          for (tier, weighted) in target_replicas.items()
                          if len(tier) == 3})

    def test_gradual_replica_count(self):
        rb = ring.RingBuilder(3, 2.5, 1)
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})
        rb.add_dev({'id': 2, 'region': 0, 'zone': 0, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sda', 'weight': 2000})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 0, 'ip': '127.0.0.2',
                    'port': 6200, 'device': 'sdb', 'weight': 2000})

        expected = {
            0: 0.625,
            1: 0.625,
            2: 0.625,
            3: 0.625,
        }

        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected, {
            tier[3]: weighted
            for (tier, weighted) in weighted_replicas.items()
            if len(tier) == 4})
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected, {
            tier[3]: wanted
            for (tier, wanted) in wanted_replicas.items()
            if len(tier) == 4})

        self.assertEqual(rb.get_required_overload(), 0)

        # server 127.0.0.2 will have only one device
        rb.remove_dev(2)

        # server 127.0.0.1 has twice the capacity of 127.0.0.2
        expected = {
            '127.0.0.1': 1.6666666666666667,
            '127.0.0.2': 0.8333333333333334,
        }
        weighted_replicas = rb._build_weighted_replicas_by_tier()
        self.assertEqual(expected, {
            tier[2]: weighted
            for (tier, weighted) in weighted_replicas.items()
            if len(tier) == 3})

        # dispersion requirements extend only to whole replicas
        expected = {
            '127.0.0.1': 1.4999999999999998,
            '127.0.0.2': 1.0,
        }
        wanted_replicas = rb._build_wanted_replicas_by_tier()
        self.assertEqual(expected, {
            tier[2]: wanted
            for (tier, wanted) in wanted_replicas.items()
            if len(tier) == 3})

        # 5/6ths to a whole replicanth is a 20% increase
        self.assertAlmostEqual(rb.get_required_overload(), 0.2)

        # so let's suppose we only allow for 10% overload
        rb.set_overload(0.1)
        target_replicas = rb._build_target_replicas_by_tier()
        expected = {
            '127.0.0.1': 1.5833333333333333,
            '127.0.0.2': 0.9166666666666667,
        }
        self.assertEqual(expected, {
            tier[2]: wanted
            for (tier, wanted) in target_replicas.items()
            if len(tier) == 3})

    def test_perfect_four_zone_four_replica_bad_placement(self):
        rb = ring.RingBuilder(4, 4, 1)

        # this weight is sorta nuts, but it's really just to help the
        # weight_of_one_part hit a magic number where floats mess up
        # like they would on ring with a part power of 19 and 100's of
        # 1000's of units of weight.
        weight = 21739130434795e-11

        # r0z0
        rb.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': weight,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': weight,
                    'ip': '127.0.0.2', 'port': 10000, 'device': 'sdb'})
        # r0z1
        rb.add_dev({'id': 2, 'region': 0, 'zone': 1, 'weight': weight,
                    'ip': '127.0.1.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 3, 'region': 0, 'zone': 1, 'weight': weight,
                    'ip': '127.0.1.2', 'port': 10000, 'device': 'sdb'})
        # r1z0
        rb.add_dev({'id': 4, 'region': 1, 'zone': 0, 'weight': weight,
                    'ip': '127.1.0.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 5, 'region': 1, 'zone': 0, 'weight': weight,
                    'ip': '127.1.0.2', 'port': 10000, 'device': 'sdb'})
        # r1z1
        rb.add_dev({'id': 6, 'region': 1, 'zone': 1, 'weight': weight,
                    'ip': '127.1.1.1', 'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 7, 'region': 1, 'zone': 1, 'weight': weight,
                    'ip': '127.1.1.2', 'port': 10000, 'device': 'sdb'})

        # the replica plan is sound
        expectations = {
            # tier_len => expected replicas
            1: {
                (0,): 2.0,
                (1,): 2.0,
            },
            2: {
                (0, 0): 1.0,
                (0, 1): 1.0,
                (1, 0): 1.0,
                (1, 1): 1.0,
            }
        }
        wr = rb._build_replica_plan()
        for tier_len, expected in expectations.items():
            self.assertEqual(expected, {t: r['max'] for (t, r) in
                                        wr.items() if len(t) == tier_len})

        # even thought a naive ceil of weights is surprisingly wrong
        expectations = {
            # tier_len => expected replicas
            1: {
                (0,): 3.0,
                (1,): 3.0,
            },
            2: {
                (0, 0): 2.0,
                (0, 1): 2.0,
                (1, 0): 2.0,
                (1, 1): 2.0,
            }
        }
        wr = rb._build_weighted_replicas_by_tier()
        for tier_len, expected in expectations.items():
            self.assertEqual(expected, {t: ceil(r) for (t, r) in
                                        wr.items() if len(t) == tier_len})


class TestRingBuilderDispersion(unittest.TestCase):

    def setUp(self):
        self.devs = ('d%s' % i for i in itertools.count())

    def assertAlmostPartCount(self, counts, expected, delta=3):
        msgs = []
        failed = False
        for k, p in sorted(expected.items()):
            try:
                self.assertAlmostEqual(counts[k], p, delta=delta)
            except KeyError:
                self.fail('%r is missing the key %r' % (counts, k))
            except AssertionError:
                failed = True
                state = '!='
            else:
                state = 'ok'
            msgs.append('parts in %s was %s expected %s (%s)' % (
                k, counts[k], p, state))
        if failed:
            self.fail('some part counts not close enough '
                      'to expected:\n' + '\n'.join(msgs))

    def test_rebalance_dispersion(self):
        rb = ring.RingBuilder(8, 6, 0)

        for i in range(6):
            rb.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.0.1',
                        'port': 6000, 'weight': 1.0,
                        'device': next(self.devs)})
        rb.rebalance()
        self.assertEqual(0, rb.dispersion)

        for z in range(2):
            for i in range(6):
                rb.add_dev({'region': 0, 'zone': z + 1, 'ip': '127.0.1.1',
                            'port': 6000, 'weight': 1.0,
                            'device': next(self.devs)})

        self.assertAlmostPartCount(_partition_counts(rb, 'zone'),
                                   {0: 1536, 1: 0, 2: 0})
        rb.rebalance()
        self.assertEqual(rb.dispersion, 50.0)
        expected = {0: 1280, 1: 128, 2: 128}
        self.assertAlmostPartCount(_partition_counts(rb, 'zone'),
                                   expected)
        report = dict(utils.dispersion_report(
            rb, r'r\d+z\d+$', verbose=True)['graph'])
        counts = {int(k.split('z')[1]): d['placed_parts']
                  for k, d in report.items()}
        self.assertAlmostPartCount(counts, expected)
        rb.rebalance()
        self.assertEqual(rb.dispersion, 33.333333333333336)
        expected = {0: 1024, 1: 256, 2: 256}
        self.assertAlmostPartCount(_partition_counts(rb, 'zone'),
                                   expected)
        report = dict(utils.dispersion_report(
            rb, r'r\d+z\d+$', verbose=True)['graph'])
        counts = {int(k.split('z')[1]): d['placed_parts']
                  for k, d in report.items()}
        self.assertAlmostPartCount(counts, expected)
        rb.rebalance()
        self.assertEqual(rb.dispersion, 16.666666666666668)
        expected = {0: 768, 1: 384, 2: 384}
        self.assertAlmostPartCount(_partition_counts(rb, 'zone'),
                                   expected)
        report = dict(utils.dispersion_report(
            rb, r'r\d+z\d+$', verbose=True)['graph'])
        counts = {int(k.split('z')[1]): d['placed_parts']
                  for k, d in report.items()}
        self.assertAlmostPartCount(counts, expected)
        rb.rebalance()
        self.assertEqual(0, rb.dispersion)
        expected = {0: 512, 1: 512, 2: 512}
        self.assertAlmostPartCount(_partition_counts(rb, 'zone'), expected)
        report = dict(utils.dispersion_report(
            rb, r'r\d+z\d+$', verbose=True)['graph'])
        counts = {int(k.split('z')[1]): d['placed_parts']
                  for k, d in report.items()}
        self.assertAlmostPartCount(counts, expected)

    def test_weight_dispersion(self):
        rb = ring.RingBuilder(8, 3, 0)

        for i in range(2):
            for d in range(3):
                rb.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.%s.1' % i,
                            'port': 6000, 'weight': 1.0,
                            'device': next(self.devs)})
        for d in range(3):
            rb.add_dev({'region': 0, 'zone': 0, 'ip': '127.0.2.1',
                        'port': 6000, 'weight': 10.0,
                        'device': next(self.devs)})

        rb.rebalance()
        # each tier should only have 1 replicanth, but the big server has 2
        # replicas of every part and 3 replicas another 1/2 - so our total
        # dispersion is greater than one replicanth, it's 1.5
        self.assertEqual(50.0, rb.dispersion)
        expected = {
            '127.0.0.1': 64,
            '127.0.1.1': 64,
            '127.0.2.1': 640,
        }
        self.assertAlmostPartCount(_partition_counts(rb, 'ip'),
                                   expected)
        report = dict(utils.dispersion_report(
            rb, r'r\d+z\d+-[^/]*$', verbose=True)['graph'])
        counts = {k.split('-')[1]: d['placed_parts']
                  for k, d in report.items()}
        self.assertAlmostPartCount(counts, expected)

    def test_multiple_tier_dispersion(self):
        rb = ring.RingBuilder(10, 8, 0)
        r_z_to_ip_count = {
            (0, 0): 2,
            (1, 1): 1,
            (1, 2): 2,
        }
        ip_index = 0
        for (r, z), ip_count in sorted(r_z_to_ip_count.items()):
            for i in range(ip_count):
                ip_index += 1
                for d in range(3):
                    rb.add_dev({'region': r, 'zone': z,
                                'ip': '127.%s.%s.%s' % (r, z, ip_index),
                                'port': 6000, 'weight': 1.0,
                                'device': next(self.devs)})

        for i in range(3):
            # it might take a few rebalances for all the right part replicas to
            # balance from r1z2 into r1z1
            rb.rebalance()
        self.assertAlmostEqual(15.52734375, rb.dispersion, delta=5.0)
        self.assertAlmostEqual(0.0, rb.get_balance(), delta=0.5)
        expected = {
            '127.0.0.1': 1638,
            '127.0.0.2': 1638,
            '127.1.1.3': 1638,
            '127.1.2.4': 1638,
            '127.1.2.5': 1638,
        }
        delta = 10
        self.assertAlmostPartCount(_partition_counts(rb, 'ip'), expected,
                                   delta=delta)
        report = dict(utils.dispersion_report(
            rb, r'r\d+z\d+-[^/]*$', verbose=True)['graph'])
        counts = {k.split('-')[1]: d['placed_parts']
                  for k, d in report.items()}
        self.assertAlmostPartCount(counts, expected, delta=delta)


if __name__ == '__main__':
    unittest.main()
