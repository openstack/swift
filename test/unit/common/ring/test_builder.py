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

import os
import unittest
import cPickle as pickle
from collections import defaultdict
from shutil import rmtree
from mock import Mock, call as mock_call

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
        self.assertEquals(rb.parts, 2 ** 8)
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
        rb = ring.RingBuilder(8, 3, 1)
        #test add new dev with no id
        rb.add_dev({'zone': 0, 'weight': 1, 'ip': '127.0.0.1', 'port': 6000})
        self.assertEquals(rb.devs[0]['id'], 0)
        #test add another dev with no id
        rb.add_dev({'zone': 3, 'weight': 1, 'ip': '127.0.0.1', 'port': 6000})
        self.assertEquals(rb.devs[1]['id'], 1)

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

    def test_remove_a_lot(self):
        rb = ring.RingBuilder(3, 3, 1)
        rb.add_dev({'id': 0, 'device': 'd0', 'ip': '10.0.0.1',
                    'port': 6002, 'weight': 1000.0, 'zone': 1})
        rb.add_dev({'id': 1, 'device': 'd1', 'ip': '10.0.0.2',
                    'port': 6002, 'weight': 1000.0, 'zone': 2})
        rb.add_dev({'id': 2, 'device': 'd2', 'ip': '10.0.0.3',
                    'port': 6002, 'weight': 1000.0, 'zone': 3})
        rb.add_dev({'id': 3, 'device': 'd3', 'ip': '10.0.0.1',
                    'port': 6002, 'weight': 1000.0, 'zone': 1})
        rb.add_dev({'id': 4, 'device': 'd4', 'ip': '10.0.0.2',
                    'port': 6002, 'weight': 1000.0, 'zone': 2})
        rb.add_dev({'id': 5, 'device': 'd5', 'ip': '10.0.0.3',
                    'port': 6002, 'weight': 1000.0, 'zone': 3})
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

    def test_shuffled_gather(self):
        if self._shuffled_gather_helper() and \
                self._shuffled_gather_helper():
                raise AssertionError('It is highly likely the ring is no '
                                     'longer shuffling the set of partitions '
                                     'to reassign on a rebalance.')

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
        for part, _ in parts:
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

    def test_multitier_partial(self):
        # Multitier test, zones full, nodes not full
        rb = ring.RingBuilder(8, 6, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdc'})

        rb.add_dev({'id': 3, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sdd'})
        rb.add_dev({'id': 4, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sde'})
        rb.add_dev({'id': 5, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sdf'})

        rb.add_dev({'id': 6, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sdg'})
        rb.add_dev({'id': 7, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sdh'})
        rb.add_dev({'id': 8, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10002, 'device': 'sdi'})

        rb.rebalance()
        rb.validate()

        for part in xrange(rb.parts):
            counts = defaultdict(lambda: defaultdict(lambda: 0))
            for replica in xrange(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1
            if counts['zone'] != {0: 2, 1: 2, 2: 2}:
                raise AssertionError(
                    "Partition %d not evenly distributed (got %r)" %
                    (part, counts['zone']))
            for dev_id, replica_count in counts['dev_id'].iteritems():
                if replica_count > 1:
                    raise AssertionError(
                        "Partition %d is on device %d more than once (%r)" %
                        (part, dev_id, counts['dev_id']))

    def test_multitier_full(self):
        # Multitier test, #replicas == #devs
        rb = ring.RingBuilder(8, 6, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sdd'})

        rb.add_dev({'id': 4, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sde'})
        rb.add_dev({'id': 5, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sdf'})

        rb.rebalance()
        rb.validate()

        for part in xrange(rb.parts):
            counts = defaultdict(lambda: defaultdict(lambda: 0))
            for replica in xrange(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1
            if counts['zone'] != {0: 2, 1: 2, 2: 2}:
                raise AssertionError(
                    "Partition %d not evenly distributed (got %r)" %
                    (part, counts['zone']))
            for dev_id, replica_count in counts['dev_id'].iteritems():
                if replica_count != 1:
                    raise AssertionError(
                        "Partition %d is on device %d %d times, not 1 (%r)" %
                        (part, dev_id, replica_count, counts['dev_id']))

    def test_multitier_overfull(self):
        # Multitier test, #replicas > #devs + 2 (to prove even distribution)
        rb = ring.RingBuilder(8, 8, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdb'})

        rb.add_dev({'id': 2, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdc'})
        rb.add_dev({'id': 3, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sdd'})

        rb.add_dev({'id': 4, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sde'})
        rb.add_dev({'id': 5, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10001, 'device': 'sdf'})

        rb.rebalance()
        rb.validate()

        for part in xrange(rb.parts):
            counts = defaultdict(lambda: defaultdict(lambda: 0))
            for replica in xrange(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1

            self.assertEquals(8, sum(counts['zone'].values()))
            for zone, replica_count in counts['zone'].iteritems():
                if replica_count not in (2, 3):
                    raise AssertionError(
                        "Partition %d not evenly distributed (got %r)" %
                        (part, counts['zone']))
            for dev_id, replica_count in counts['dev_id'].iteritems():
                if replica_count not in (1, 2):
                    raise AssertionError(
                        "Partition %d is on device %d %d times, "
                        "not 1 or 2 (%r)" %
                        (part, dev_id, replica_count, counts['dev_id']))

    def test_multitier_expansion_more_devices(self):
        rb = ring.RingBuilder(8, 6, 1)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdb'})
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdc'})

        rb.rebalance()
        rb.validate()

        rb.add_dev({'id': 3, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdd'})
        rb.add_dev({'id': 4, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sde'})
        rb.add_dev({'id': 5, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdf'})

        for _ in xrange(5):
            rb.pretend_min_part_hours_passed()
            rb.rebalance()
        rb.validate()

        for part in xrange(rb.parts):
            counts = dict(zone=defaultdict(lambda: 0),
                          dev_id=defaultdict(lambda: 0))
            for replica in xrange(rb.replicas):
                dev = rb.devs[rb._replica2part2dev[replica][part]]
                counts['zone'][dev['zone']] += 1
                counts['dev_id'][dev['id']] += 1

            self.assertEquals({0: 2, 1: 2, 2: 2}, dict(counts['zone']))
            self.assertEquals({0: 1, 1: 1, 2: 1, 3: 1, 4: 1, 5: 1},
                              dict(counts['dev_id']))

    def test_multitier_part_moves_with_0_min_part_hours(self):
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.rebalance()
        rb.validate()

        # min_part_hours is 0, so we're clear to move 2 replicas to
        # new devs
        rb.add_dev({'id': 1, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 2, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdc1'})
        rb.rebalance()
        rb.validate()

        for part in xrange(rb.parts):
            devs = set()
            for replica in xrange(rb.replicas):
                devs.add(rb._replica2part2dev[replica][part])

            if len(devs) != 3:
                raise AssertionError(
                    "Partition %d not on 3 devs (got %r)" % (part, devs))

    def test_multitier_part_moves_with_positive_min_part_hours(self):
        rb = ring.RingBuilder(8, 3, 99)
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.rebalance()
        rb.validate()

        # min_part_hours is >0, so we'll only be able to move 1
        # replica to a new home
        rb.add_dev({'id': 1, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 2, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdc1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        rb.validate()

        for part in xrange(rb.parts):
            devs = set()
            for replica in xrange(rb.replicas):
                devs.add(rb._replica2part2dev[replica][part])

            if len(devs) != 2:
                raise AssertionError(
                    "Partition %d not on 2 devs (got %r)" % (part, devs))

    def test_multitier_dont_move_too_many_replicas(self):
        rb = ring.RingBuilder(8, 3, 0)
        # there'll be at least one replica in z0 and z1
        rb.add_dev({'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdb1'})
        rb.rebalance()
        rb.validate()

        # only 1 replica should move
        rb.add_dev({'id': 2, 'zone': 2, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdd1'})
        rb.add_dev({'id': 3, 'zone': 3, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sde1'})
        rb.add_dev({'id': 4, 'zone': 4, 'weight': 1, 'ip': '127.0.0.1',
                    'port': 10000, 'device': 'sdf1'})
        rb.rebalance()
        rb.validate()

        for part in xrange(rb.parts):
            zones = set()
            for replica in xrange(rb.replicas):
                zones.add(rb.devs[rb._replica2part2dev[replica][part]]['zone'])

            if len(zones) != 3:
                raise AssertionError(
                    "Partition %d not in 3 zones (got %r)" % (part, zones))
            if 0 not in zones or 1 not in zones:
                raise AssertionError(
                    "Partition %d not in zones 0 and 1 (got %r)" %
                    (part, zones))

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

    def test_load(self):
        rb = ring.RingBuilder(8, 3, 1)
        devs = [{'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.0',
                 'port': 10000, 'device': 'sda1', 'meta': 'meta0'},
                {'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                 'port': 10001, 'device': 'sdb1', 'meta': 'meta1'},
                {'id': 2, 'zone': 2, 'weight': 2, 'ip': '127.0.0.2',
                 'port': 10002, 'device': 'sdc1', 'meta': 'meta2'},
                {'id': 3, 'zone': 3, 'weight': 2, 'ip': '127.0.0.3',
                 'port': 10003, 'device': 'sdd1'}]
        for d in devs:
            rb.add_dev(d)
        rb.rebalance()

        real_pickle = pickle.load
        try:
            #test a legit builder
            fake_pickle = Mock(return_value=rb)
            fake_open = Mock(return_value=None)
            pickle.load = fake_pickle
            builder = RingBuilder.load('fake.builder', open=fake_open)
            self.assertEquals(fake_pickle.call_count, 1)
            fake_open.assert_has_calls([mock_call('fake.builder', 'rb')])
            self.assertEquals(builder, rb)
            fake_pickle.reset_mock()
            fake_open.reset_mock()

            #test old style builder
            fake_pickle.return_value = rb.to_dict()
            pickle.load = fake_pickle
            builder = RingBuilder.load('fake.builder', open=fake_open)
            fake_open.assert_has_calls([mock_call('fake.builder', 'rb')])
            self.assertEquals(builder.devs, rb.devs)
            fake_pickle.reset_mock()
            fake_open.reset_mock()

            #test old devs but no meta
            no_meta_builder = rb
            for dev in no_meta_builder.devs:
                del(dev['meta'])
            fake_pickle.return_value = no_meta_builder
            pickle.load = fake_pickle
            builder = RingBuilder.load('fake.builder', open=fake_open)
            fake_open.assert_has_calls([mock_call('fake.builder', 'rb')])
            self.assertEquals(builder.devs, rb.devs)
            fake_pickle.reset_mock()
        finally:
            pickle.load = real_pickle

    def test_search_devs(self):
        rb = ring.RingBuilder(8, 3, 1)
        devs = [{'id': 0, 'zone': 0, 'weight': 1, 'ip': '127.0.0.0',
                 'port': 10000, 'device': 'sda1', 'meta': 'meta0'},
                {'id': 1, 'zone': 1, 'weight': 1, 'ip': '127.0.0.1',
                 'port': 10001, 'device': 'sdb1', 'meta': 'meta1'},
                {'id': 2, 'zone': 2, 'weight': 2, 'ip': '127.0.0.2',
                 'port': 10002, 'device': 'sdc1', 'meta': 'meta2'},
                {'id': 3, 'zone': 3, 'weight': 2, 'ip': '127.0.0.3',
                 'port': 10003, 'device': 'sdd1', 'meta': 'meta3'}]
        for d in devs:
            rb.add_dev(d)
        rb.rebalance()
        res = rb.search_devs('d1')
        self.assertEquals(res, [devs[1]])
        res = rb.search_devs('z1')
        self.assertEquals(res, [devs[1]])
        res = rb.search_devs('-127.0.0.1')
        self.assertEquals(res, [devs[1]])
        res = rb.search_devs('-[127.0.0.1]:10001')
        self.assertEquals(res, [devs[1]])
        res = rb.search_devs(':10001')
        self.assertEquals(res, [devs[1]])
        res = rb.search_devs('/sdb1')
        self.assertEquals(res, [devs[1]])
        res = rb.search_devs('_meta1')
        self.assertRaises(ValueError, rb.search_devs, 'OMGPONIES')

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
        self.assertNotEquals(rb.validate(stats=True)[1], 999.99)
        rb.add_dev({'id': 4, 'zone': 0, 'weight': 0, 'ip': '127.0.0.1',
                    'port': 10004, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()
        self.assertNotEquals(rb.validate(stats=True)[1], 999.99)


if __name__ == '__main__':
    unittest.main()
