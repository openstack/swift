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

import bisect
import itertools
import math
import cPickle as pickle


from array import array
from collections import defaultdict
from random import randint, shuffle
from time import time

from swift.common import exceptions
from swift.common.ring import RingData
from swift.common.ring.utils import tiers_for_dev, build_tier_tree


class RingBuilder(object):
    """
    Used to build swift.common.ring.RingData instances to be written to disk
    and used with swift.common.ring.Ring instances. See bin/swift-ring-builder
    for example usage.

    The instance variable devs_changed indicates if the device information has
    changed since the last balancing. This can be used by tools to know whether
    a rebalance request is an isolated request or due to added, changed, or
    removed devices.

    :param part_power: number of partitions = 2**part_power
    :param replicas: number of replicas for each partition
    :param min_part_hours: minimum number of hours between partition changes
    """

    def __init__(self, part_power, replicas, min_part_hours):
        self.part_power = part_power
        self.replicas = replicas
        self.min_part_hours = min_part_hours
        self.parts = 2 ** self.part_power
        self.devs = []
        self.devs_changed = False
        self.version = 0

        # _replica2part2dev maps from replica number to partition number to
        # device id. So, for a three replica, 2**23 ring, it's an array of
        # three 2**23 arrays of device ids (unsigned shorts). This can work a
        # bit faster than the 2**23 array of triplet arrays of device ids in
        # many circumstances. Making one big 2**23 * 3 array didn't seem to
        # have any speed change; though you're welcome to try it again (it was
        # a while ago, code-wise, when I last tried it).
        self._replica2part2dev = None

        # _last_part_moves is a 2**23 array of unsigned bytes representing the
        # number of hours since a given partition was last moved. This is used
        # to guarantee we don't move a partition twice within a given number of
        # hours (24 is my usual test). Removing a device or setting its weight
        # to 0 overrides this behavior as it's assumed those actions are done
        # because of device failure.
        # _last_part_moves_epoch indicates the time the offsets in
        # _last_part_moves is based on.
        self._last_part_moves_epoch = None
        self._last_part_moves = None

        self._last_part_gather_start = 0
        self._remove_devs = []
        self._ring = None

    def weight_of_one_part(self):
        """
        Returns the weight of each partition as calculated from the
        total weight of all the devices.
        """
        try:
            return self.parts * self.replicas / \
                sum(d['weight'] for d in self._iter_devs())
        except ZeroDivisionError:
            raise exceptions.EmptyRingError('There are no devices in this '
                                            'ring, or all devices have been '
                                            'deleted')

    def copy_from(self, builder):
        """
        Reinitializes this RingBuilder instance from data obtained from the
        builder dict given. Code example::

            b = RingBuilder(1, 1, 1)  # Dummy values
            b.copy_from(builder)

        This is to restore a RingBuilder that has had its b.to_dict()
        previously saved.
        """
        if hasattr(builder, 'devs'):
            self.part_power = builder.part_power
            self.replicas = builder.replicas
            self.min_part_hours = builder.min_part_hours
            self.parts = builder.parts
            self.devs = builder.devs
            self.devs_changed = builder.devs_changed
            self.version = builder.version
            self._replica2part2dev = builder._replica2part2dev
            self._last_part_moves_epoch = builder._last_part_moves_epoch
            self._last_part_moves = builder._last_part_moves
            self._last_part_gather_start = builder._last_part_gather_start
            self._remove_devs = builder._remove_devs
        else:
            self.part_power = builder['part_power']
            self.replicas = builder['replicas']
            self.min_part_hours = builder['min_part_hours']
            self.parts = builder['parts']
            self.devs = builder['devs']
            self.devs_changed = builder['devs_changed']
            self.version = builder['version']
            self._replica2part2dev = builder['_replica2part2dev']
            self._last_part_moves_epoch = builder['_last_part_moves_epoch']
            self._last_part_moves = builder['_last_part_moves']
            self._last_part_gather_start = builder['_last_part_gather_start']
            self._remove_devs = builder['_remove_devs']
        self._ring = None

    def to_dict(self):
        """
        Returns a dict that can be used later with copy_from to
        restore a RingBuilder. swift-ring-builder uses this to
        pickle.dump the dict to a file and later load that dict into
        copy_from.
        """
        return {'part_power': self.part_power,
                'replicas': self.replicas,
                'min_part_hours': self.min_part_hours,
                'parts': self.parts,
                'devs': self.devs,
                'devs_changed': self.devs_changed,
                'version': self.version,
                '_replica2part2dev': self._replica2part2dev,
                '_last_part_moves_epoch': self._last_part_moves_epoch,
                '_last_part_moves': self._last_part_moves,
                '_last_part_gather_start': self._last_part_gather_start,
                '_remove_devs': self._remove_devs}

    def change_min_part_hours(self, min_part_hours):
        """
        Changes the value used to decide if a given partition can be moved
        again. This restriction is to give the overall system enough time to
        settle a partition to its new location before moving it to yet another
        location. While no data would be lost if a partition is moved several
        times quickly, it could make that data unreachable for a short period
        of time.

        This should be set to at least the average full partition replication
        time. Starting it at 24 hours and then lowering it to what the
        replicator reports as the longest partition cycle is best.

        :param min_part_hours: new value for min_part_hours
        """
        self.min_part_hours = min_part_hours

    def get_ring(self):
        """
        Get the ring, or more specifically, the swift.common.ring.RingData.
        This ring data is the minimum required for use of the ring. The ring
        builder itself keeps additional data such as when partitions were last
        moved.
        """
        # We cache the self._ring value so multiple requests for it don't build
        # it multiple times. Be sure to set self._ring = None whenever the ring
        # will need to be rebuilt.
        if not self._ring:
            # Make devs list (with holes for deleted devices) and not including
            # builder-specific extra attributes.
            devs = [None] * len(self.devs)
            for dev in self._iter_devs():
                devs[dev['id']] = dict((k, v) for k, v in dev.items()
                                       if k not in ('parts', 'parts_wanted'))
            # Copy over the replica+partition->device assignments, the device
            # information, and the part_shift value (the number of bits to
            # shift an unsigned int >I right to obtain the partition for the
            # int).
            if not self._replica2part2dev:
                self._ring = RingData([], devs, 32 - self.part_power)
            else:
                self._ring = \
                    RingData([array('H', p2d) for p2d in
                              self._replica2part2dev],
                             devs, 32 - self.part_power)
        return self._ring

    def add_dev(self, dev):
        """
        Add a device to the ring. This device dict should have a minimum of the
        following keys:

        ======  ===============================================================
        id      unique integer identifier amongst devices. Defaults to the next
                id if the 'id' key is not provided in the dict
        weight  a float of the relative weight of this device as compared to
                others; this indicates how many partitions the builder will try
                to assign to this device
        zone    integer indicating which zone the device is in; a given
                partition will not be assigned to multiple devices within the
                same zone
        ip      the ip address of the device
        port    the tcp port of the device
        device  the device's name on disk (sdb1, for example)
        meta    general use 'extra' field; for example: the online date, the
                hardware description
        ======  ===============================================================

        .. note::
            This will not rebalance the ring immediately as you may want to
            make multiple changes for a single rebalance.

        :param dev: device dict
        """
        if 'id' not in dev:
            dev['id'] = 0
            if self.devs:
                dev['id'] = max(d['id'] for d in self.devs if d) + 1
        if dev['id'] < len(self.devs) and self.devs[dev['id']] is not None:
            raise exceptions.DuplicateDeviceError(
                'Duplicate device id: %d' % dev['id'])
        # Add holes to self.devs to ensure self.devs[dev['id']] will be the dev
        while dev['id'] >= len(self.devs):
            self.devs.append(None)
        dev['weight'] = float(dev['weight'])
        dev['parts'] = 0
        self.devs[dev['id']] = dev
        self._set_parts_wanted()
        self.devs_changed = True
        self.version += 1

    def set_dev_weight(self, dev_id, weight):
        """
        Set the weight of a device. This should be called rather than just
        altering the weight key in the device dict directly, as the builder
        will need to rebuild some internal state to reflect the change.

        .. note::
            This will not rebalance the ring immediately as you may want to
            make multiple changes for a single rebalance.

        :param dev_id: device id
        :param weight: new weight for device
        """
        self.devs[dev_id]['weight'] = weight
        self._set_parts_wanted()
        self.devs_changed = True
        self.version += 1

    def remove_dev(self, dev_id):
        """
        Remove a device from the ring.

        .. note::
            This will not rebalance the ring immediately as you may want to
            make multiple changes for a single rebalance.

        :param dev_id: device id
        """
        dev = self.devs[dev_id]
        dev['weight'] = 0
        self._remove_devs.append(dev)
        self._set_parts_wanted()
        self.devs_changed = True
        self.version += 1

    def rebalance(self):
        """
        Rebalance the ring.

        This is the main work function of the builder, as it will assign and
        reassign partitions to devices in the ring based on weights, distinct
        zones, recent reassignments, etc.

        The process doesn't always perfectly assign partitions (that'd take a
        lot more analysis and therefore a lot more time -- I had code that did
        that before). Because of this, it keeps rebalancing until the device
        skew (number of partitions a device wants compared to what it has) gets
        below 1% or doesn't change by more than 1% (only happens with ring that
        can't be balanced no matter what -- like with 3 zones of differing
        weights with replicas set to 3).

        :returns: (number_of_partitions_altered, resulting_balance)
        """
        self._ring = None
        if self._last_part_moves_epoch is None:
            self._initial_balance()
            self.devs_changed = False
            return self.parts, self.get_balance()
        retval = 0
        self._update_last_part_moves()
        last_balance = 0
        while True:
            reassign_parts = self._gather_reassign_parts()
            self._reassign_parts(reassign_parts)
            retval += len(reassign_parts)
            while self._remove_devs:
                self.devs[self._remove_devs.pop()['id']] = None
            balance = self.get_balance()
            if balance < 1 or abs(last_balance - balance) < 1 or \
                    retval == self.parts:
                break
            last_balance = balance
        self.devs_changed = False
        self.version += 1
        return retval, balance

    def validate(self, stats=False):
        """
        Validate the ring.

        This is a safety function to try to catch any bugs in the building
        process. It ensures partitions have been assigned to real devices,
        aren't doubly assigned, etc. It can also optionally check the even
        distribution of partitions across devices.

        :param stats: if True, check distribution of partitions across devices
        :returns: if stats is True, a tuple of (device_usage, worst_stat), else
                  (None, None). device_usage[dev_id] will equal the number of
                  partitions assigned to that device. worst_stat will equal the
                  number of partitions the worst device is skewed from the
                  number it should have.
        :raises RingValidationError: problem was found with the ring.
        """

        # "len" showed up in profiling, so it's just computed once.
        dev_len = len(self.devs)
        if sum(d['parts'] for d in self._iter_devs()) != \
                self.parts * self.replicas:
            raise exceptions.RingValidationError(
                'All partitions are not double accounted for: %d != %d' %
                (sum(d['parts'] for d in self._iter_devs()),
                 self.parts * self.replicas))
        if stats:
            # dev_usage[dev_id] will equal the number of partitions assigned to
            # that device.
            dev_usage = array('I', (0 for _junk in xrange(dev_len)))
            for part2dev in self._replica2part2dev:
                for dev_id in part2dev:
                    dev_usage[dev_id] += 1

        for part in xrange(self.parts):
            for replica in xrange(self.replicas):
                dev_id = self._replica2part2dev[replica][part]
                if dev_id >= dev_len or not self.devs[dev_id]:
                    raise exceptions.RingValidationError(
                        "Partition %d, replica %d was not allocated "
                        "to a device." %
                        (part, replica))

        for dev in self._iter_devs():
            if not isinstance(dev['port'], int):
                raise exceptions.RingValidationError(
                    "Device %d has port %r, which is not an integer." %
                    (dev['id'], dev['port']))

        if stats:
            weight_of_one_part = self.weight_of_one_part()
            worst = 0
            for dev in self._iter_devs():
                if not dev['weight']:
                    if dev_usage[dev['id']]:
                        # If a device has no weight, but has partitions, then
                        # its overage is considered "infinity" and therefore
                        # always the worst possible. We show 999.99 for
                        # convenience.
                        worst = 999.99
                        break
                    continue
                skew = abs(100.0 * dev_usage[dev['id']] /
                           (dev['weight'] * weight_of_one_part) - 100.0)
                if skew > worst:
                    worst = skew
            return dev_usage, worst
        return None, None

    def get_balance(self):
        """
        Get the balance of the ring. The balance value is the highest
        percentage off the desired amount of partitions a given device wants.
        For instance, if the "worst" device wants (based on its relative weight
        and its zone's relative weight) 123 partitions and it has 124
        partitions, the balance value would be 0.83 (1 extra / 123 wanted * 100
        for percentage).

        :returns: balance of the ring
        """
        balance = 0
        weight_of_one_part = self.weight_of_one_part()
        for dev in self._iter_devs():
            if not dev['weight']:
                if dev['parts']:
                    # If a device has no weight, but has partitions, then its
                    # overage is considered "infinity" and therefore always the
                    # worst possible. We show 999.99 for convenience.
                    balance = 999.99
                    break
                continue
            dev_balance = abs(100.0 * dev['parts'] /
                              (dev['weight'] * weight_of_one_part) - 100.0)
            if dev_balance > balance:
                balance = dev_balance
        return balance

    def pretend_min_part_hours_passed(self):
        """
        Override min_part_hours by marking all partitions as having been moved
        255 hours ago. This can be used to force a full rebalance on the next
        call to rebalance.
        """
        for part in xrange(self.parts):
            self._last_part_moves[part] = 0xff

    def get_part_devices(self, part):
        """
        Get the devices that are responsible for the partition.

        :param part: partition to get devices for
        :returns: list of device dicts
        """
        return [self.devs[r[part]] for r in self._replica2part2dev]

    def _iter_devs(self):
        """
        Returns an iterator all the non-None devices in the ring. Note that
        this means list(b._iter_devs())[some_id] may not equal b.devs[some_id];
        you will have to check the 'id' key of each device to obtain its
        dev_id.
        """
        for dev in self.devs:
            if dev is not None:
                yield dev

    def _set_parts_wanted(self):
        """
        Sets the parts_wanted key for each of the devices to the number of
        partitions the device wants based on its relative weight. This key is
        used to sort the devices according to "most wanted" during rebalancing
        to best distribute partitions. A negative parts_wanted indicates the
        device is "overweight" and wishes to give partitions away if possible.
        """
        weight_of_one_part = self.weight_of_one_part()

        for dev in self._iter_devs():
            if not dev['weight']:
                # With no weight, that means we wish to "drain" the device. So
                # we set the parts_wanted to a really large negative number to
                # indicate its strong desire to give up everything it has.
                dev['parts_wanted'] = -self.parts * self.replicas
            else:
                dev['parts_wanted'] = \
                    int(weight_of_one_part * dev['weight']) - dev['parts']

    def _initial_balance(self):
        """
        Initial partition assignment is the same as rebalancing an
        existing ring, but with some initial setup beforehand.
        """
        self._replica2part2dev = \
            [array('H', (0 for _junk in xrange(self.parts)))
             for _junk in xrange(self.replicas)]

        replicas = range(self.replicas)
        self._last_part_moves = array('B', (0 for _junk in xrange(self.parts)))
        self._last_part_moves_epoch = int(time())
        self._reassign_parts((p, replicas) for p in xrange(self.parts))

    def _update_last_part_moves(self):
        """
        Updates how many hours ago each partition was moved based on the
        current time. The builder won't move a partition that has been moved
        more recently than min_part_hours.
        """
        elapsed_hours = int(time() - self._last_part_moves_epoch) / 3600
        for part in xrange(self.parts):
            # The "min(self._last_part_moves[part] + elapsed_hours, 0xff)"
            # which was here showed up in profiling, so it got inlined.
            last_plus_elapsed = self._last_part_moves[part] + elapsed_hours
            if last_plus_elapsed < 0xff:
                self._last_part_moves[part] = last_plus_elapsed
            else:
                self._last_part_moves[part] = 0xff
        self._last_part_moves_epoch = int(time())

    def _gather_reassign_parts(self):
        """
        Returns a list of (partition, replicas) pairs to be reassigned by
        gathering from removed devices, insufficiently-far-apart replicas, and
        overweight drives.
        """
        # inline memoization of tiers_for_dev() results (profiling reveals it
        # as a hot-spot).
        tfd = {}

        # First we gather partitions from removed devices. Since removed
        # devices usually indicate device failures, we have no choice but to
        # reassign these partitions. However, we mark them as moved so later
        # choices will skip other replicas of the same partition if possible.
        removed_dev_parts = defaultdict(list)
        if self._remove_devs:
            dev_ids = [d['id'] for d in self._remove_devs if d['parts']]
            if dev_ids:
                for replica in xrange(self.replicas):
                    part2dev = self._replica2part2dev[replica]
                    for part in xrange(self.parts):
                        if part2dev[part] in dev_ids:
                            self._last_part_moves[part] = 0
                            removed_dev_parts[part].append(replica)

        # Now we gather partitions that are "at risk" because they aren't
        # currently sufficient spread out across the cluster.
        spread_out_parts = defaultdict(list)
        max_allowed_replicas = self._build_max_replicas_by_tier()
        for part in xrange(self.parts):
            # Only move one replica at a time if possible.
            if part in removed_dev_parts:
                continue

            # First, add up the count of replicas at each tier for each
            # partition.
            # replicas_at_tier was a "lambda: 0" defaultdict, but profiling
            # revealed the lambda invocation as a significant cost.
            replicas_at_tier = {}
            for replica in xrange(self.replicas):
                dev = self.devs[self._replica2part2dev[replica][part]]
                if dev['id'] not in tfd:
                    tfd[dev['id']] = tiers_for_dev(dev)
                for tier in tfd[dev['id']]:
                    if tier not in replicas_at_tier:
                        replicas_at_tier[tier] = 1
                    else:
                        replicas_at_tier[tier] += 1

            # Now, look for partitions not yet spread out enough and not
            # recently moved.
            for replica in xrange(self.replicas):
                dev = self.devs[self._replica2part2dev[replica][part]]
                removed_replica = False
                if dev['id'] not in tfd:
                    tfd[dev['id']] = tiers_for_dev(dev)
                for tier in tfd[dev['id']]:
                    rep_at_tier = 0
                    if tier in replicas_at_tier:
                        rep_at_tier = replicas_at_tier[tier]
                    if (rep_at_tier > max_allowed_replicas[tier] and
                            self._last_part_moves[part] >=
                            self.min_part_hours):
                        self._last_part_moves[part] = 0
                        spread_out_parts[part].append(replica)
                        dev['parts_wanted'] += 1
                        dev['parts'] -= 1
                        removed_replica = True
                        break
                if removed_replica:
                    if dev['id'] not in tfd:
                        tfd[dev['id']] = tiers_for_dev(dev)
                    for tier in tfd[dev['id']]:
                        replicas_at_tier[tier] -= 1

        # Last, we gather partitions from devices that are "overweight" because
        # they have more partitions than their parts_wanted.
        reassign_parts = defaultdict(list)

        # We randomly pick a new starting point in the "circular" ring of
        # partitions to try to get a better rebalance when called multiple
        # times.
        start = self._last_part_gather_start / 4 + randint(0, self.parts / 2)
        self._last_part_gather_start = start
        for replica in xrange(self.replicas):
            part2dev = self._replica2part2dev[replica]
            for part in itertools.chain(xrange(start, self.parts),
                                        xrange(0, start)):
                if self._last_part_moves[part] < self.min_part_hours:
                    continue
                if part in removed_dev_parts or part in spread_out_parts:
                    continue
                dev = self.devs[part2dev[part]]
                if dev['parts_wanted'] < 0:
                    self._last_part_moves[part] = 0
                    dev['parts_wanted'] += 1
                    dev['parts'] -= 1
                    reassign_parts[part].append(replica)

        reassign_parts.update(spread_out_parts)
        reassign_parts.update(removed_dev_parts)

        reassign_parts_list = list(reassign_parts.iteritems())
        # We shuffle the partitions to reassign so we get a more even
        # distribution later. There has been discussion of trying to distribute
        # partitions more "regularly" because that would actually reduce risk
        # but 1) it is really difficult to do this with uneven clusters and 2)
        # it would concentrate load during failure recovery scenarios
        # (increasing risk). The "right" answer has yet to be debated to
        # conclusion, but working code wins for now.
        shuffle(reassign_parts_list)
        return reassign_parts_list

    def _reassign_parts(self, reassign_parts):
        """
        For an existing ring data set, partitions are reassigned similarly to
        the initial assignment. The devices are ordered by how many partitions
        they still want and kept in that order throughout the process. The
        gathered partitions are iterated through, assigning them to devices
        according to the "most wanted" while keeping the replicas as "far
        apart" as possible. Two different zones are considered the
        farthest-apart things, followed by different ip/port pairs within a
        zone; the least-far-apart things are different devices with the same
        ip/port pair in the same zone.

        If you want more replicas than devices, you won't get all your
        replicas.

        :param reassign_parts: An iterable of (part, replicas_to_replace)
                               pairs. replicas_to_replace is an iterable of the
                               replica (an int) to replace for that partition.
                               replicas_to_replace may be shared for multiple
                               partitions, so be sure you do not modify it.
        """
        for dev in self._iter_devs():
            dev['sort_key'] = self._sort_key_for(dev)
        available_devs = \
            sorted((d for d in self._iter_devs() if d['weight']),
                   key=lambda x: x['sort_key'])

        tier2children = build_tier_tree(available_devs)

        tier2devs = defaultdict(list)
        tier2sort_key = defaultdict(list)
        tiers_by_depth = defaultdict(set)
        for dev in available_devs:
            for tier in tiers_for_dev(dev):
                tier2devs[tier].append(dev)  # <-- starts out sorted!
                tier2sort_key[tier].append(dev['sort_key'])
                tiers_by_depth[len(tier)].add(tier)

        for part, replace_replicas in reassign_parts:
            # Gather up what other tiers (zones, ip_ports, and devices) the
            # replicas not-to-be-moved are in for this part.
            other_replicas = defaultdict(lambda: 0)
            for replica in xrange(self.replicas):
                if replica not in replace_replicas:
                    dev = self.devs[self._replica2part2dev[replica][part]]
                    for tier in tiers_for_dev(dev):
                        other_replicas[tier] += 1

            def find_home_for_replica(tier=(), depth=1):
                # Order the tiers by how many replicas of this
                # partition they already have. Then, of the ones
                # with the smallest number of replicas, pick the
                # tier with the hungriest drive and then continue
                # searching in that subtree.
                #
                # There are other strategies we could use here,
                # such as hungriest-tier (i.e. biggest
                # sum-of-parts-wanted) or picking one at random.
                # However, hungriest-drive is what was used here
                # before, and it worked pretty well in practice.
                #
                # Note that this allocator will balance things as
                # evenly as possible at each level of the device
                # layout. If your layout is extremely unbalanced,
                # this may produce poor results.
                candidate_tiers = tier2children[tier]
                min_count = min(other_replicas[t] for t in candidate_tiers)
                candidate_tiers = [t for t in candidate_tiers
                                   if other_replicas[t] == min_count]
                candidate_tiers.sort(
                    key=lambda t: tier2sort_key[t][-1])

                if depth == max(tiers_by_depth.keys()):
                    return tier2devs[candidate_tiers[-1]][-1]

                return find_home_for_replica(tier=candidate_tiers[-1],
                                             depth=depth + 1)

            for replica in replace_replicas:
                dev = find_home_for_replica()
                dev['parts_wanted'] -= 1
                dev['parts'] += 1
                old_sort_key = dev['sort_key']
                new_sort_key = dev['sort_key'] = self._sort_key_for(dev)
                for tier in tiers_for_dev(dev):
                    other_replicas[tier] += 1

                    index = bisect.bisect_left(tier2sort_key[tier],
                                               old_sort_key)
                    tier2devs[tier].pop(index)
                    tier2sort_key[tier].pop(index)

                    new_index = bisect.bisect_left(tier2sort_key[tier],
                                                   new_sort_key)
                    tier2devs[tier].insert(new_index, dev)
                    tier2sort_key[tier].insert(new_index, new_sort_key)

                self._replica2part2dev[replica][part] = dev['id']

        # Just to save memory and keep from accidental reuse.
        for dev in self._iter_devs():
            del dev['sort_key']

    def _sort_key_for(self, dev):
        # The maximum value of self.parts is 2^32, which is 9 hex
        # digits wide (0x100000000). Using a width of 16 here gives us
        # plenty of breathing room; you'd need more than 2^28 replicas
        # to overflow it.
        # Since the sort key is a string and therefore an ascii sort applies,
        # the maximum_parts_wanted + parts_wanted is used so negative
        # parts_wanted end up sorted above positive parts_wanted.
        return '%016x.%04x.%04x' % (
            (self.parts * self.replicas) + dev['parts_wanted'],
            randint(0, 0xffff),
            dev['id'])

    def _build_max_replicas_by_tier(self):
        """
        Returns a dict of (tier: replica_count) for all tiers in the ring.

        There will always be a () entry as the root of the structure, whose
        replica_count will equal the ring's replica_count.

        Then there will be (dev_id,) entries for each device, indicating the
        maximum number of replicas the device might have for any given
        partition. Anything greater than 1 indicates a partition at serious
        risk, as the data on that partition will not be stored distinctly at
        the ring's replica_count.

        Next there will be (dev_id, ip_port) entries for each device,
        indicating the maximum number of replicas the device shares with other
        devices on the same ip_port for any given partition. Anything greater
        than 1 indicates a partition at elevated risk, as if that ip_port were
        to fail multiple replicas of that partition would be unreachable.

        Last there will be (dev_id, ip_port, zone) entries for each device,
        indicating the maximum number of replicas the device shares with other
        devices within the same zone for any given partition. Anything greater
        than 1 indicates a partition at slightly elevated risk, as if that zone
        were to fail multiple replicas of that partition would be unreachable.

        Example return dict for the common SAIO setup::

            {(): 3,
             (1,): 1.0,
             (1, '127.0.0.1:6010'): 1.0,
             (1, '127.0.0.1:6010', 0): 1.0,
             (2,): 1.0,
             (2, '127.0.0.1:6020'): 1.0,
             (2, '127.0.0.1:6020', 1): 1.0,
             (3,): 1.0,
             (3, '127.0.0.1:6030'): 1.0,
             (3, '127.0.0.1:6030', 2): 1.0,
             (4,): 1.0,
             (4, '127.0.0.1:6040'): 1.0,
             (4, '127.0.0.1:6040', 3): 1.0}
        """
        # Used by walk_tree to know what entries to create for each recursive
        # call.
        tier2children = build_tier_tree(self._iter_devs())

        def walk_tree(tier, replica_count):
            mr = {tier: replica_count}
            if tier in tier2children:
                subtiers = tier2children[tier]
                for subtier in subtiers:
                    submax = math.ceil(float(replica_count) / len(subtiers))
                    mr.update(walk_tree(subtier, submax))
            return mr
        return walk_tree((), self.replicas)

    @classmethod
    def load(cls, builder_file, open=open):
        """
        Obtain RingBuilder instance of the provided builder file

        :param builder_file: path to builder file to load
        :return: RingBuilder instance
        """
        builder = pickle.load(open(builder_file, 'rb'))
        if not hasattr(builder, 'devs'):
            builder_dict = builder
            builder = RingBuilder(1, 1, 1)
            builder.copy_from(builder_dict)
        for dev in builder.devs:
            #really old rings didn't have meta keys
            if dev and 'meta' not in dev:
                dev['meta'] = ''
        return builder

    def search_devs(self, search_value):
        """
    The <search-value> can be of the form::

        d<device_id>z<zone>-<ip>:<port>/<device_name>_<meta>

    Any part is optional, but you must include at least one part.

    Examples::

        d74              Matches the device id 74
        z1               Matches devices in zone 1
        z1-1.2.3.4       Matches devices in zone 1 with the ip 1.2.3.4
        1.2.3.4          Matches devices in any zone with the ip 1.2.3.4
        z1:5678          Matches devices in zone 1 using port 5678
        :5678            Matches devices that use port 5678
        /sdb1            Matches devices with the device name sdb1
        _shiny           Matches devices with shiny in the meta data
        _"snet: 5.6.7.8" Matches devices with snet: 5.6.7.8 in the meta data
        [::1]            Matches devices in any zone with the ip ::1
        z1-[::1]:5678    Matches devices in zone 1 with ip ::1 and port 5678

    Most specific example::

        d74z1-1.2.3.4:5678/sdb1_"snet: 5.6.7.8"

    Nerd explanation:

        All items require their single character prefix except the ip, in which
        case the - is optional unless the device id or zone is also included.
        """
        orig_search_value = search_value
        match = []
        if search_value.startswith('d'):
            i = 1
            while i < len(search_value) and search_value[i].isdigit():
                i += 1
            match.append(('id', int(search_value[1:i])))
            search_value = search_value[i:]
        if search_value.startswith('z'):
            i = 1
            while i < len(search_value) and search_value[i].isdigit():
                i += 1
            match.append(('zone', int(search_value[1:i])))
            search_value = search_value[i:]
        if search_value.startswith('-'):
            search_value = search_value[1:]
        if len(search_value) and search_value[0].isdigit():
            i = 1
            while i < len(search_value) and search_value[i] in '0123456789.':
                i += 1
            match.append(('ip', search_value[:i]))
            search_value = search_value[i:]
        elif len(search_value) and search_value[0] == '[':
            i = 1
            while i < len(search_value) and search_value[i] != ']':
                i += 1
            i += 1
            match.append(('ip', search_value[:i].lstrip('[').rstrip(']')))
            search_value = search_value[i:]
        if search_value.startswith(':'):
            i = 1
            while i < len(search_value) and search_value[i].isdigit():
                i += 1
            match.append(('port', int(search_value[1:i])))
            search_value = search_value[i:]
        if search_value.startswith('/'):
            i = 1
            while i < len(search_value) and search_value[i] != '_':
                i += 1
            match.append(('device', search_value[1:i]))
            search_value = search_value[i:]
        if search_value.startswith('_'):
            match.append(('meta', search_value[1:]))
            search_value = ''
        if search_value:
            raise ValueError('Invalid <search-value>: %s' %
                             repr(orig_search_value))
        matched_devs = []
        for dev in self.devs:
            if not dev:
                continue
            matched = True
            for key, value in match:
                if key == 'meta':
                    if value not in dev.get(key):
                        matched = False
                elif dev.get(key) != value:
                    matched = False
            if matched:
                matched_devs.append(dev)
        return matched_devs
