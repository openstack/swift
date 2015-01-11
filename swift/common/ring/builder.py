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

import bisect
import copy
import errno
import itertools
import logging
import math
import random
import cPickle as pickle

from array import array
from collections import defaultdict
from time import time

from swift.common import exceptions
from swift.common.ring import RingData
from swift.common.ring.utils import tiers_for_dev, build_tier_tree, \
    validate_and_normalize_address

MAX_BALANCE = 999.99


try:
    # python 2.7+
    from logging import NullHandler
except ImportError:
    # python 2.6
    class NullHandler(logging.Handler):
        def emit(self, *a, **kw):
            pass


class RingBuilder(object):
    """
    Used to build swift.common.ring.RingData instances to be written to disk
    and used with swift.common.ring.Ring instances. See bin/swift-ring-builder
    for example usage.

    The instance variable devs_changed indicates if the device information has
    changed since the last balancing. This can be used by tools to know whether
    a rebalance request is an isolated request or due to added, changed, or
    removed devices.

    :param part_power: number of partitions = 2**part_power.
    :param replicas: number of replicas for each partition
    :param min_part_hours: minimum number of hours between partition changes
    """

    def __init__(self, part_power, replicas, min_part_hours):
        if part_power > 32:
            raise ValueError("part_power must be at most 32 (was %d)"
                             % (part_power,))
        if replicas < 1:
            raise ValueError("replicas must be at least 1 (was %.6f)"
                             % (replicas,))
        if min_part_hours < 0:
            raise ValueError("min_part_hours must be non-negative (was %d)"
                             % (min_part_hours,))

        self.part_power = part_power
        self.replicas = replicas
        self.min_part_hours = min_part_hours
        self.parts = 2 ** self.part_power
        self.devs = []
        self.devs_changed = False
        self.version = 0
        self.overload = 0.0

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

        self._dispersion_graph = {}
        self.dispersion = 0.0
        self._remove_devs = []
        self._ring = None

        self.logger = logging.getLogger("swift.ring.builder")
        if not self.logger.handlers:
            # silence "no handler for X" error messages
            self.logger.addHandler(NullHandler())

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
            self.overload = builder.overload
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
            self.overload = builder.get('overload', 0.0)
            self.version = builder['version']
            self._replica2part2dev = builder['_replica2part2dev']
            self._last_part_moves_epoch = builder['_last_part_moves_epoch']
            self._last_part_moves = builder['_last_part_moves']
            self._last_part_gather_start = builder['_last_part_gather_start']
            self._dispersion_graph = builder.get('_dispersion_graph', {})
            self.dispersion = builder.get('dispersion')
            self._remove_devs = builder['_remove_devs']
        self._ring = None

        # Old builders may not have a region defined for their devices, in
        # which case we default it to 1.
        for dev in self._iter_devs():
            dev.setdefault("region", 1)

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
                'overload': self.overload,
                '_replica2part2dev': self._replica2part2dev,
                '_last_part_moves_epoch': self._last_part_moves_epoch,
                '_last_part_moves': self._last_part_moves,
                '_last_part_gather_start': self._last_part_gather_start,
                '_dispersion_graph': self._dispersion_graph,
                'dispersion': self.dispersion,
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

    def set_replicas(self, new_replica_count):
        """
        Changes the number of replicas in this ring.

        If the new replica count is sufficiently different that
        self._replica2part2dev will change size, sets
        self.devs_changed. This is so tools like
        bin/swift-ring-builder can know to write out the new ring
        rather than bailing out due to lack of balance change.
        """
        old_slots_used = int(self.parts * self.replicas)
        new_slots_used = int(self.parts * new_replica_count)
        if old_slots_used != new_slots_used:
            self.devs_changed = True

        self.replicas = new_replica_count

    def set_overload(self, overload):
        self.overload = overload

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
        region  integer indicating which region the device is in
        zone    integer indicating which zone the device is in; a given
                partition will not be assigned to multiple devices within the
                same (region, zone) pair if there is any alternative
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

        :returns: id of device (not used in the tree anymore, but unknown
                  users may depend on it)
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
        return dev['id']

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

    def rebalance(self, seed=None):
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
        can't be balanced no matter what).

        :returns: (number_of_partitions_altered, resulting_balance)
        """
        old_replica2part2dev = copy.deepcopy(self._replica2part2dev)

        if seed is not None:
            random.seed(seed)

        self._ring = None
        if self._last_part_moves_epoch is None:
            self.logger.debug("New builder; performing initial balance")
            self._initial_balance()
            self.devs_changed = False
            self._build_dispersion_graph()
            return self.parts, self.get_balance()
        changed_parts = 0
        self._update_last_part_moves()
        last_balance = 0
        new_parts, removed_part_count = self._adjust_replica2part2dev_size()
        self.logger.debug(
            "%d new parts and %d removed parts from replica-count change",
            len(new_parts), removed_part_count)
        changed_parts += removed_part_count
        self._set_parts_wanted()
        self._reassign_parts(new_parts)
        changed_parts += len(new_parts)
        while True:
            reassign_parts = self._gather_reassign_parts()
            changed_parts += len(reassign_parts)
            self.logger.debug("Gathered %d parts", changed_parts)
            self._reassign_parts(reassign_parts)
            self.logger.debug("Assigned %d parts", changed_parts)
            while self._remove_devs:
                remove_dev_id = self._remove_devs.pop()['id']
                self.logger.debug("Removing dev %d", remove_dev_id)
                self.devs[remove_dev_id] = None
            balance = self.get_balance()
            if balance < 1 or abs(last_balance - balance) < 1 or \
                    changed_parts == self.parts:
                break
            last_balance = balance
        self.devs_changed = False
        self.version += 1

        changed_parts = self._build_dispersion_graph(old_replica2part2dev)
        return changed_parts, balance

    def _build_dispersion_graph(self, old_replica2part2dev=None):
        """
        Build a dict of all tiers in the cluster to a list of the number of
        parts with a replica count at each index.  The values of the dict will
        be lists of length the maximum whole replica + 1 so that the
        graph[tier][3] is the number of parts with in the tier with 3 replicas
        and graph [tier][0] is the number of parts not assigned in this tier.

        i.e.
        {
            <tier>: [
                <number_of_parts_with_0_replicas>,
                <number_of_parts_with_1_replicas>,
                ...
                <number_of_parts_with_n_replicas>,
                ],
            ...
        }

        :param old_replica2part2dev: if called from rebalance, the
            old_replica2part2dev can be used to count moved moved parts.

        :returns: number of parts with different assignments than
            old_replica2part2dev if provided
        """

        # Since we're going to loop over every replica of every part we'll
        # also count up changed_parts if old_replica2part2dev is passed in
        old_replica2part2dev = old_replica2part2dev or []
        # Compare the partition allocation before and after the rebalance
        # Only changed device ids are taken into account; devices might be
        # "touched" during the rebalance, but actually not really moved
        changed_parts = 0

        int_replicas = int(math.ceil(self.replicas))
        max_allowed_replicas = self._build_max_replicas_by_tier()
        parts_at_risk = 0

        tfd = {}

        dispersion_graph = {}
        # go over all the devices holding each replica part by part
        for part_id, dev_ids in enumerate(
                itertools.izip(*self._replica2part2dev)):
            # count the number of replicas of this part for each tier of each
            # device, some devices may have overlapping tiers!
            replicas_at_tier = defaultdict(int)
            for rep_id, dev in enumerate(iter(
                    self.devs[dev_id] for dev_id in dev_ids)):
                if dev['id'] not in tfd:
                    tfd[dev['id']] = tiers_for_dev(dev)
                for tier in tfd[dev['id']]:
                    replicas_at_tier[tier] += 1
                # IndexErrors will be raised if the replicas are increased or
                # decreased, and that actually means the partition has changed
                try:
                    old_device = old_replica2part2dev[rep_id][part_id]
                except IndexError:
                    changed_parts += 1
                    continue

                if old_device != dev['id']:
                    changed_parts += 1
            part_at_risk = False
            # update running totals for each tiers' number of parts with a
            # given replica count
            for tier, replicas in replicas_at_tier.items():
                if tier not in dispersion_graph:
                    dispersion_graph[tier] = [self.parts] + [0] * int_replicas
                dispersion_graph[tier][0] -= 1
                dispersion_graph[tier][replicas] += 1
                if replicas > max_allowed_replicas[tier]:
                    part_at_risk = True
            # this part may be at risk in multiple tiers, but we only count it
            # as at_risk once
            if part_at_risk:
                parts_at_risk += 1
        self._dispersion_graph = dispersion_graph
        self.dispersion = 100.0 * parts_at_risk / self.parts
        return changed_parts

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

        parts_on_devs = sum(d['parts'] for d in self._iter_devs())

        if not self._replica2part2dev:
            raise exceptions.RingValidationError(
                '_replica2part2dev empty; did you forget to rebalance?')

        parts_in_map = sum(len(p2d) for p2d in self._replica2part2dev)
        if parts_on_devs != parts_in_map:
            raise exceptions.RingValidationError(
                'All partitions are not double accounted for: %d != %d' %
                (parts_on_devs, parts_in_map))
        if stats:
            # dev_usage[dev_id] will equal the number of partitions assigned to
            # that device.
            dev_usage = array('I', (0 for _junk in xrange(dev_len)))
            for part2dev in self._replica2part2dev:
                for dev_id in part2dev:
                    dev_usage[dev_id] += 1

        for part, replica in self._each_part_replica():
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
                        # always the worst possible. We show MAX_BALANCE for
                        # convenience.
                        worst = MAX_BALANCE
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
        percentage off the desired amount of partitions a given device
        wants. For instance, if the "worst" device wants (based on its
        weight relative to the sum of all the devices' weights) 123
        partitions and it has 124 partitions, the balance value would
        be 0.83 (1 extra / 123 wanted * 100 for percentage).

        :returns: balance of the ring
        """
        balance = 0
        weight_of_one_part = self.weight_of_one_part()
        for dev in self._iter_devs():
            if not dev['weight']:
                if dev['parts']:
                    # If a device has no weight, but has partitions, then its
                    # overage is considered "infinity" and therefore always the
                    # worst possible. We show MAX_BALANCE for convenience.
                    balance = MAX_BALANCE
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
        Get the devices that are responsible for the partition,
        filtering out duplicates.

        :param part: partition to get devices for
        :returns: list of device dicts
        """
        devices = []
        for dev in self._devs_for_part(part):
            if dev not in devices:
                devices.append(dev)
        return devices

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
                dev['parts_wanted'] = (
                    # Round up here so that every partition ultimately ends up
                    # with a placement.
                    #
                    # Imagine 5 partitions to be placed on 4 devices. If we
                    # didn't use math.ceil() here, each device would have a
                    # parts_wanted of 1, so 4 partitions would be placed but
                    # the last would not, probably resulting in a crash. This
                    # way, some devices end up with leftover parts_wanted, but
                    # at least every partition ends up somewhere.
                    int(math.ceil(weight_of_one_part * dev['weight']
                                  - dev['parts'])))

    def _adjust_replica2part2dev_size(self):
        """
        Make sure that the lengths of the arrays in _replica2part2dev
        are correct for the current value of self.replicas.

        Example:
        self.part_power = 8
        self.replicas = 2.25

        self._replica2part2dev will contain 3 arrays: the first 2 of
        length 256 (2**8), and the last of length 64 (0.25 * 2**8).

        Returns a 2-tuple: the first element is a list of (partition,
        replicas) tuples indicating which replicas need to be
        (re)assigned to devices, and the second element is a count of
        how many replicas were removed.
        """
        removed_replicas = 0

        fractional_replicas, whole_replicas = math.modf(self.replicas)
        whole_replicas = int(whole_replicas)

        desired_lengths = [self.parts] * whole_replicas
        if fractional_replicas:
            desired_lengths.append(int(self.parts * fractional_replicas))

        to_assign = defaultdict(list)

        if self._replica2part2dev is not None:
            # If we crossed an integer threshold (say, 4.1 --> 4),
            # we'll have a partial extra replica clinging on here. Clean
            # up any such extra stuff.
            for part2dev in self._replica2part2dev[len(desired_lengths):]:
                for dev_id in part2dev:
                    dev_losing_part = self.devs[dev_id]
                    dev_losing_part['parts'] -= 1
                    removed_replicas += 1
            self._replica2part2dev = \
                self._replica2part2dev[:len(desired_lengths)]
        else:
            self._replica2part2dev = []

        for replica, desired_length in enumerate(desired_lengths):
            if replica < len(self._replica2part2dev):
                part2dev = self._replica2part2dev[replica]
                if len(part2dev) < desired_length:
                    # Not long enough: needs to be extended and the
                    # newly-added pieces assigned to devices.
                    for part in xrange(len(part2dev), desired_length):
                        to_assign[part].append(replica)
                        part2dev.append(0)
                elif len(part2dev) > desired_length:
                    # Too long: truncate this mapping.
                    for part in xrange(desired_length, len(part2dev)):
                        dev_losing_part = self.devs[part2dev[part]]
                        dev_losing_part['parts'] -= 1
                        removed_replicas += 1
                    self._replica2part2dev[replica] = part2dev[:desired_length]
            else:
                # Mapping not present at all: make one up and assign
                # all of it.
                for part in xrange(desired_length):
                    to_assign[part].append(replica)
                self._replica2part2dev.append(
                    array('H', (0 for _junk in xrange(desired_length))))

        return (to_assign.items(), removed_replicas)

    def _initial_balance(self):
        """
        Initial partition assignment is the same as rebalancing an
        existing ring, but with some initial setup beforehand.
        """
        self._last_part_moves = array('B', (0 for _junk in xrange(self.parts)))
        self._last_part_moves_epoch = int(time())
        self._set_parts_wanted()

        self._reassign_parts(self._adjust_replica2part2dev_size()[0])

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

    def _get_available_parts(self):
        """
        Returns a dict of (tier: available parts in other tiers) for all tiers
        in the ring.

        Devices that have too much partitions (negative parts_wanted) are
        ignored, otherwise the sum of all parts_wanted is 0 +/- rounding
        errors.
        """
        wanted_parts_for_tier = {}
        for dev in self._iter_devs():
            pw = (max(0, dev['parts_wanted']) +
                  max(int(math.ceil(
                      (dev['parts_wanted'] + dev['parts']) * self.overload)),
                      0))
            for tier in tiers_for_dev(dev):
                wanted_parts_for_tier.setdefault(tier, 0)
                wanted_parts_for_tier[tier] += pw
        return wanted_parts_for_tier

    def _gather_reassign_parts(self):
        """
        Returns a list of (partition, replicas) pairs to be reassigned by
        gathering from removed devices, insufficiently-far-apart replicas, and
        overweight drives.
        """
        # inline memoization of tiers_for_dev() results (profiling reveals it
        # as a hot-spot).
        tfd = {}

        tiers_by_len = defaultdict(set)
        for dev in self._iter_devs():
            tiers = tiers_for_dev(dev)
            tfd[dev['id']] = tiers
            for tier in tiers:
                tiers_by_len[len(tier)].add(tier)

        tiers_by_len = dict((length, list(tiers))
                            for length, tiers in tiers_by_len.items())

        sibling_tiers = {}
        for length, tiers in tiers_by_len.items():
            for i, tier in enumerate(tiers):
                sibling_tiers[tier] = [t for t in (tiers[:i] + tiers[(i + 1):])
                                       if t[:-1] == tier[:-1]]

        # First we gather partitions from removed devices. Since removed
        # devices usually indicate device failures, we have no choice but to
        # reassign these partitions. However, we mark them as moved so later
        # choices will skip other replicas of the same partition if possible.
        removed_dev_parts = defaultdict(list)
        if self._remove_devs:
            dev_ids = [d['id'] for d in self._remove_devs if d['parts']]
            if dev_ids:
                for part, replica in self._each_part_replica():
                    dev_id = self._replica2part2dev[replica][part]
                    if dev_id in dev_ids:
                        self._last_part_moves[part] = 0
                        removed_dev_parts[part].append(replica)
                        self.logger.debug(
                            "Gathered %d/%d from dev %d [dev removed]",
                            part, replica, dev_id)

        # Now we gather partitions that are "at risk" because they aren't
        # currently sufficient spread out across the cluster.
        spread_out_parts = defaultdict(list)
        max_allowed_replicas = self._build_max_replicas_by_tier()
        wanted_parts_for_tier = self._get_available_parts()
        moved_parts = 0
        for part in xrange(self.parts):
            # Only move one replica at a time if possible.
            if part in removed_dev_parts:
                continue

            # First, add up the count of replicas at each tier for each
            # partition.
            # replicas_at_tier was a "lambda: 0" defaultdict, but profiling
            # revealed the lambda invocation as a significant cost.
            replicas_at_tier = {}
            for dev in self._devs_for_part(part):
                for tier in tfd[dev['id']]:
                    if tier not in replicas_at_tier:
                        replicas_at_tier[tier] = 1
                    else:
                        replicas_at_tier[tier] += 1

            # Now, look for partitions not yet spread out enough and not
            # recently moved.
            for replica in self._replicas_for_part(part):
                dev = self.devs[self._replica2part2dev[replica][part]]
                removed_replica = False
                for tier in tfd[dev['id']]:
                    rep_at_tier = replicas_at_tier.get(tier, 0)

                    # If this tier's not overcrowded, there's nothing to
                    # gather, so we can avoid some calculation here as an
                    # optimization.
                    if rep_at_tier <= max_allowed_replicas[tier]:
                        continue

                    available_parts_for_tier = sum(
                        wanted_parts_for_tier[t]
                        for t in sibling_tiers[tier]
                        # If a sibling tier is "full" with respect to
                        # partition dispersion, but not "full" with respect
                        # to parts_wanted, we don't count it as a possible
                        # destination.
                        #
                        # Otherwise, we gather a partition from tier X
                        # (because its replicas are not spread out), and
                        # then we may place it right back in tier X or in
                        # another tier that already has replicas (because
                        # that tier has parts_wanted). Then, on the next
                        # rebalance, it'll happen again, and then again...
                        #
                        # Worse yet, this "dancing replica" immobilizes
                        # other replicas of the partition that want to move
                        # because they're on devices with negative
                        # parts_wanted. This can lead to a replica that
                        # sticks to a zero-weight device no matter how often
                        # the ring is rebalanced.
                        if (max_allowed_replicas[t] >
                            replicas_at_tier.get(t, 0))
                    ) - moved_parts

                    # Only allow a part to be gathered if there are wanted
                    # parts on other tiers.
                    if (self._last_part_moves[part] >= self.min_part_hours
                            and available_parts_for_tier > 0):
                        self._last_part_moves[part] = 0
                        spread_out_parts[part].append(replica)
                        dev['parts_wanted'] += 1
                        dev['parts'] -= 1
                        removed_replica = True
                        moved_parts += 1
                        self.logger.debug(
                            "Gathered %d/%d from dev %d [dispersion]",
                            part, replica, dev['id'])
                        break
                if removed_replica:
                    for tier in tfd[dev['id']]:
                        replicas_at_tier[tier] -= 1

        # Last, we gather partitions from devices that are "overweight" because
        # they have more partitions than their parts_wanted.
        reassign_parts = defaultdict(list)

        # We randomly pick a new starting point in the "circular" ring of
        # partitions to try to get a better rebalance when called multiple
        # times.

        start = self._last_part_gather_start / 4
        start += random.randint(0, self.parts / 2)  # GRAH PEP8!!!

        self._last_part_gather_start = start
        for replica, part2dev in enumerate(self._replica2part2dev):
            # If we've got a partial replica, start may be out of
            # range. Scale it down so that we get a similar movement
            # pattern (but scaled down) on sequential runs.
            this_start = int(float(start) * len(part2dev) / self.parts)

            for part in itertools.chain(xrange(this_start, len(part2dev)),
                                        xrange(0, this_start)):
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
                    self.logger.debug(
                        "Gathered %d/%d from dev %d [weight]",
                        part, replica, dev['id'])

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
        random.shuffle(reassign_parts_list)
        return reassign_parts_list

    def _reassign_parts(self, reassign_parts):
        """
        For an existing ring data set, partitions are reassigned similarly to
        the initial assignment. The devices are ordered by how many partitions
        they still want and kept in that order throughout the process. The
        gathered partitions are iterated through, assigning them to devices
        according to the "most wanted" while keeping the replicas as "far
        apart" as possible. Two different regions are considered the
        farthest-apart things, followed by zones, then different ip/port pairs
        within a zone; the least-far-apart things are different devices with
        the same ip/port pair in the same zone.

        If you want more replicas than devices, you won't get all your
        replicas.

        :param reassign_parts: An iterable of (part, replicas_to_replace)
                               pairs. replicas_to_replace is an iterable of the
                               replica (an int) to replace for that partition.
                               replicas_to_replace may be shared for multiple
                               partitions, so be sure you do not modify it.
        """
        fudge_available_in_tier = defaultdict(int)
        parts_available_in_tier = defaultdict(int)
        for dev in self._iter_devs():
            dev['sort_key'] = self._sort_key_for(dev)
            tiers = tiers_for_dev(dev)
            dev['tiers'] = tiers
            # Note: this represents how many partitions may be assigned to a
            # given tier (region/zone/server/disk). It does not take into
            # account how many partitions a given tier wants to shed.
            #
            # If we did not do this, we could have a zone where, at some
            # point during assignment, number-of-parts-to-gain equals
            # number-of-parts-to-shed. At that point, no further placement
            # into that zone would occur since its parts_available_in_tier
            # would be 0. This would happen any time a zone had any device
            # with partitions to shed, which is any time a device is being
            # removed, which is a pretty frequent operation.
            wanted = max(dev['parts_wanted'], 0)
            fudge = max(int(math.ceil(
                (dev['parts_wanted'] + dev['parts']) * self.overload)),
                0)
            for tier in tiers:
                fudge_available_in_tier[tier] += (wanted + fudge)
                parts_available_in_tier[tier] += wanted

        available_devs = \
            sorted((d for d in self._iter_devs() if d['weight']),
                   key=lambda x: x['sort_key'])

        tier2devs = defaultdict(list)
        tier2sort_key = defaultdict(tuple)
        tier2dev_sort_key = defaultdict(list)
        max_tier_depth = 0
        for dev in available_devs:
            for tier in dev['tiers']:
                tier2devs[tier].append(dev)  # <-- starts out sorted!
                tier2dev_sort_key[tier].append(dev['sort_key'])
                tier2sort_key[tier] = dev['sort_key']
                if len(tier) > max_tier_depth:
                    max_tier_depth = len(tier)

        tier2children_sets = build_tier_tree(available_devs)
        tier2children = defaultdict(list)
        tier2children_sort_key = {}
        tiers_list = [()]
        depth = 1
        while depth <= max_tier_depth:
            new_tiers_list = []
            for tier in tiers_list:
                child_tiers = list(tier2children_sets[tier])
                child_tiers.sort(key=tier2sort_key.__getitem__)
                tier2children[tier] = child_tiers
                tier2children_sort_key[tier] = map(
                    tier2sort_key.__getitem__, child_tiers)
                new_tiers_list.extend(child_tiers)
            tiers_list = new_tiers_list
            depth += 1

        for part, replace_replicas in reassign_parts:
            # Gather up what other tiers (regions, zones, ip/ports, and
            # devices) the replicas not-to-be-moved are in for this part.
            other_replicas = defaultdict(int)
            occupied_tiers_by_tier_len = defaultdict(set)
            for replica in self._replicas_for_part(part):
                if replica not in replace_replicas:
                    dev = self.devs[self._replica2part2dev[replica][part]]
                    for tier in dev['tiers']:
                        other_replicas[tier] += 1
                        occupied_tiers_by_tier_len[len(tier)].add(tier)

            for replica in replace_replicas:
                # Find a new home for this replica
                tier = ()
                depth = 1
                while depth <= max_tier_depth:
                    roomiest_tier = fudgiest_tier = None
                    # Order the tiers by how many replicas of this
                    # partition they already have. Then, of the ones
                    # with the smallest number of replicas and that have
                    # room to accept more partitions, pick the tier with
                    # the hungriest drive and then continue searching in
                    # that subtree.
                    #
                    # There are other strategies we could use here,
                    # such as hungriest-tier (i.e. biggest
                    # sum-of-parts-wanted) or picking one at random.
                    # However, hungriest-drive is what was used here
                    # before, and it worked pretty well in practice.
                    #
                    # Note that this allocator prioritizes even device
                    # filling over dispersion, so if your layout is
                    # extremely unbalanced, you may not get the replica
                    # dispersion that you expect, and your durability
                    # may be lessened.
                    #
                    # This used to be a cute, recursive function, but it's been
                    # unrolled for performance.

                    # We sort the tiers here so that, when we look for a tier
                    # with the lowest number of replicas, the first one we
                    # find is the one with the hungriest drive (i.e. drive
                    # with the largest sort_key value). This lets us
                    # short-circuit the search while still ensuring we get the
                    # right tier.
                    candidates_with_replicas = \
                        occupied_tiers_by_tier_len[len(tier) + 1]

                    # Among the tiers with room for more partitions,
                    # find one with the smallest possible number of
                    # replicas already in it, breaking ties by which one
                    # has the hungriest drive.
                    candidates_with_room = [
                        t for t in tier2children[tier]
                        if parts_available_in_tier[t] > 0]
                    candidates_with_fudge = set([
                        t for t in tier2children[tier]
                        if fudge_available_in_tier[t] > 0])
                    candidates_with_fudge.update(candidates_with_room)

                    if candidates_with_room:
                        if len(candidates_with_room) > \
                           len(candidates_with_replicas):
                            # There exists at least one tier with room for
                            # another partition and 0 other replicas already
                            # in it, so we can use a faster search. The else
                            # branch's search would work here, but it's
                            # significantly slower.
                            roomiest_tier = max(
                                (t for t in candidates_with_room
                                 if other_replicas[t] == 0),
                                key=tier2sort_key.__getitem__)
                        else:
                            roomiest_tier = max(
                                candidates_with_room,
                                key=lambda t: (-other_replicas[t],
                                               tier2sort_key[t]))
                    else:
                        roomiest_tier = None

                    fudgiest_tier = max(candidates_with_fudge,
                                        key=lambda t: (-other_replicas[t],
                                                       tier2sort_key[t]))

                    if (roomiest_tier is None or
                        (other_replicas[roomiest_tier] >
                         other_replicas[fudgiest_tier])):
                        tier = fudgiest_tier
                    else:
                        tier = roomiest_tier
                    depth += 1

                dev = tier2devs[tier][-1]
                dev['parts_wanted'] -= 1
                dev['parts'] += 1
                old_sort_key = dev['sort_key']
                new_sort_key = dev['sort_key'] = self._sort_key_for(dev)
                for tier in dev['tiers']:
                    parts_available_in_tier[tier] -= 1
                    fudge_available_in_tier[tier] -= 1
                    other_replicas[tier] += 1
                    occupied_tiers_by_tier_len[len(tier)].add(tier)

                    index = bisect.bisect_left(tier2dev_sort_key[tier],
                                               old_sort_key)
                    tier2devs[tier].pop(index)
                    tier2dev_sort_key[tier].pop(index)

                    new_index = bisect.bisect_left(tier2dev_sort_key[tier],
                                                   new_sort_key)
                    tier2devs[tier].insert(new_index, dev)
                    tier2dev_sort_key[tier].insert(new_index, new_sort_key)

                    new_last_sort_key = tier2dev_sort_key[tier][-1]
                    tier2sort_key[tier] = new_last_sort_key

                    # Now jiggle tier2children values to keep them sorted
                    parent_tier = tier[0:-1]
                    index = bisect.bisect_left(
                        tier2children_sort_key[parent_tier],
                        old_sort_key)
                    popped = tier2children[parent_tier].pop(index)
                    tier2children_sort_key[parent_tier].pop(index)

                    new_index = bisect.bisect_left(
                        tier2children_sort_key[parent_tier],
                        new_last_sort_key)
                    tier2children[parent_tier].insert(new_index, popped)
                    tier2children_sort_key[parent_tier].insert(
                        new_index, new_last_sort_key)

                self._replica2part2dev[replica][part] = dev['id']
                self.logger.debug(
                    "Placed %d/%d onto dev %d", part, replica, dev['id'])

        # Just to save memory and keep from accidental reuse.
        for dev in self._iter_devs():
            del dev['sort_key']
            del dev['tiers']

    @staticmethod
    def _sort_key_for(dev):
        return (dev['parts_wanted'], random.randint(0, 0xFFFF), dev['id'])

    def _build_max_replicas_by_tier(self):
        """
        Returns a defaultdict of (tier: replica_count) for all tiers in the
        ring excluding zero weight devices.

        There will always be a () entry as the root of the structure, whose
        replica_count will equal the ring's replica_count.

        Then there will be (region,) entries for each region, indicating the
        maximum number of replicas the region might have for any given
        partition.

        Next there will be (region, zone) entries for each zone, indicating
        the maximum number of replicas in a given region and zone.  Anything
        greater than 1 indicates a partition at slightly elevated risk, as if
        that zone were to fail multiple replicas of that partition would be
        unreachable.

        Next there will be (region, zone, ip_port) entries for each node,
        indicating the maximum number of replicas stored on a node in a given
        region and zone.  Anything greater than 1 indicates a partition at
        elevated risk, as if that ip_port were to fail multiple replicas of
        that partition would be unreachable.

        Last there will be (region, zone, ip_port, device) entries for each
        device, indicating the maximum number of replicas the device shares
        with other devices on the same node for any given partition.
        Anything greater than 1 indicates a partition at serious risk, as the
        data on that partition will not be stored distinctly at the ring's
        replica_count.

        Example return dict for the common SAIO setup::

            {(): 3.0,
            (1,): 3.0,
            (1, 1): 1.0,
            (1, 1, '127.0.0.1:6010'): 1.0,
            (1, 1, '127.0.0.1:6010', 0): 1.0,
            (1, 2): 1.0,
            (1, 2, '127.0.0.1:6020'): 1.0,
            (1, 2, '127.0.0.1:6020', 1): 1.0,
            (1, 3): 1.0,
            (1, 3, '127.0.0.1:6030'): 1.0,
            (1, 3, '127.0.0.1:6030', 2): 1.0,
            (1, 4): 1.0,
            (1, 4, '127.0.0.1:6040'): 1.0,
            (1, 4, '127.0.0.1:6040', 3): 1.0}

        """
        # Used by walk_tree to know what entries to create for each recursive
        # call.
        tier2children = build_tier_tree(d for d in self._iter_devs() if
                                        d['weight'])

        def walk_tree(tier, replica_count):
            mr = {tier: replica_count}
            if tier in tier2children:
                subtiers = tier2children[tier]
                for subtier in subtiers:
                    submax = math.ceil(float(replica_count) / len(subtiers))
                    mr.update(walk_tree(subtier, submax))
            return mr
        mr = defaultdict(float)
        mr.update(walk_tree((), self.replicas))
        return mr

    def _devs_for_part(self, part):
        """
        Returns a list of devices for a specified partition.

        Deliberately includes duplicates.
        """
        if self._replica2part2dev is None:
            return []
        return [self.devs[part2dev[part]]
                for part2dev in self._replica2part2dev
                if part < len(part2dev)]

    def _replicas_for_part(self, part):
        """
        Returns a list of replicas for a specified partition.

        These can be used as indices into self._replica2part2dev
        without worrying about IndexErrors.
        """
        return [replica for replica, part2dev
                in enumerate(self._replica2part2dev)
                if part < len(part2dev)]

    def _each_part_replica(self):
        """
        Generator yielding every (partition, replica) pair in the ring.
        """
        for replica, part2dev in enumerate(self._replica2part2dev):
            for part in xrange(len(part2dev)):
                yield (part, replica)

    @classmethod
    def load(cls, builder_file, open=open):
        """
        Obtain RingBuilder instance of the provided builder file

        :param builder_file: path to builder file to load
        :return: RingBuilder instance
        """
        try:
            fp = open(builder_file, 'rb')
        except IOError as e:
            if e.errno == errno.ENOENT:
                raise exceptions.FileNotFoundError(
                    'Ring Builder file does not exist: %s' % builder_file)
            elif e.errno in [errno.EPERM, errno.EACCES]:
                raise exceptions.PermissionError(
                    'Ring Builder file cannot be accessed: %s' % builder_file)
            else:
                raise
        else:
            with fp:
                try:
                    builder = pickle.load(fp)
                except Exception:
                    # raise error during unpickling as UnPicklingError
                    raise exceptions.UnPicklingError(
                        'Ring Builder file is invalid: %s' % builder_file)

        if not hasattr(builder, 'devs'):
            builder_dict = builder
            builder = RingBuilder(1, 1, 1)
            builder.copy_from(builder_dict)
        for dev in builder.devs:
            # really old rings didn't have meta keys
            if dev and 'meta' not in dev:
                dev['meta'] = ''
            # NOTE(akscram): An old ring builder file don't contain
            #                replication parameters.
            if dev:
                if 'ip' in dev:
                    dev.setdefault('replication_ip', dev['ip'])
                if 'port' in dev:
                    dev.setdefault('replication_port', dev['port'])
        return builder

    def save(self, builder_file):
        """Serialize this RingBuilder instance to disk.

        :param builder_file: path to builder file to save
        """
        with open(builder_file, 'wb') as f:
            pickle.dump(self.to_dict(), f, protocol=2)

    def search_devs(self, search_values):
        """Search devices by parameters.

        :param search_values: a dictionary with search values to filter
                              devices, supported parameters are id,
                              region, zone, ip, port, replication_ip,
                              replication_port, device, weight, meta

        :returns: list of device dicts
        """
        matched_devs = []
        for dev in self.devs:
            if not dev:
                continue
            matched = True
            for key in ('id', 'region', 'zone', 'ip', 'port', 'replication_ip',
                        'replication_port', 'device', 'weight', 'meta'):
                if key in search_values:
                    value = search_values.get(key)
                    if value is not None:
                        if key == 'meta':
                            if value not in dev.get(key):
                                matched = False
                        elif key == 'ip' or key == 'replication_ip':
                            cdev = ''
                            try:
                                cdev = validate_and_normalize_address(
                                    dev.get(key, ''))
                            except ValueError:
                                pass
                            if cdev != value:
                                matched = False
                        elif dev.get(key) != value:
                            matched = False
            if matched:
                matched_devs.append(dev)
        return matched_devs
