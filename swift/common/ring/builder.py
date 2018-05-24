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

import contextlib
import copy
import errno
import itertools
import logging
import math
import random
import uuid

import six.moves.cPickle as pickle
from copy import deepcopy
from contextlib import contextmanager

from array import array
from collections import defaultdict
import six
from six.moves import range
from time import time

from swift.common import exceptions
from swift.common.ring import RingData
from swift.common.ring.utils import tiers_for_dev, build_tier_tree, \
    validate_and_normalize_address, validate_replicas_by_tier, pretty_dev

# we can't store None's in the replica2part2dev array, so we high-jack
# the max value for magic to represent the part is not currently
# assigned to any device.
NONE_DEV = 2 ** 16 - 1
MAX_BALANCE = 999.99
MAX_BALANCE_GATHER_COUNT = 3


class RingValidationWarning(Warning):
    pass


@contextlib.contextmanager
def _set_random_seed(seed):
    # If random seed is set when entering this context then reset original
    # random state when exiting the context. This avoids a test calling this
    # method with a fixed seed value causing all subsequent tests to use a
    # repeatable random sequence.
    random_state = None
    if seed is not None:
        random_state = random.getstate()
        random.seed(seed)
    try:
        yield
    finally:
        if random_state:
            # resetting state rather than calling seed() eases unit testing
            random.setstate(random_state)


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
        self.next_part_power = None
        self.replicas = replicas
        self.min_part_hours = min_part_hours
        self.parts = 2 ** self.part_power
        self.devs = []
        self.devs_changed = False
        self.version = 0
        self.overload = 0.0
        self._id = None

        # _replica2part2dev maps from replica number to partition number to
        # device id. So, for a three replica, 2**23 ring, it's an array of
        # three 2**23 arrays of device ids (unsigned shorts). This can work a
        # bit faster than the 2**23 array of triplet arrays of device ids in
        # many circumstances. Making one big 2**23 * 3 array didn't seem to
        # have any speed change; though you're welcome to try it again (it was
        # a while ago, code-wise, when I last tried it).
        self._replica2part2dev = None

        # _last_part_moves is an array of unsigned bytes representing
        # the number of hours since a given partition was last moved.
        # This is used to guarantee we don't move a partition twice
        # within a given number of hours (24 is my usual test). Removing
        # a device overrides this behavior as it's assumed that's only
        # done because of device failure.
        self._last_part_moves = array('B', itertools.repeat(0, self.parts))
        # _part_moved_bitmap record parts have been moved
        self._part_moved_bitmap = None
        # _last_part_moves_epoch indicates the time the offsets in
        # _last_part_moves is based on.
        self._last_part_moves_epoch = 0

        self._last_part_gather_start = 0

        self._dispersion_graph = {}
        self.dispersion = 0.0
        self._remove_devs = []
        self._ring = None

        self.logger = logging.getLogger("swift.ring.builder")
        if not self.logger.handlers:
            self.logger.disabled = True
            # silence "no handler for X" error messages
            self.logger.addHandler(logging.NullHandler())

    @property
    def id(self):
        if self._id is None:
            # We don't automatically assign an id here because we want a caller
            # to explicitly know when a builder needs an id to be assigned. In
            # that case the caller must save the builder in order that a newly
            # assigned id is persisted.
            raise AttributeError(
                'id attribute has not been initialised by calling save()')
        return self._id

    @property
    def part_shift(self):
        return 32 - self.part_power

    @property
    def ever_rebalanced(self):
        return self._replica2part2dev is not None

    def _set_part_moved(self, part):
        self._last_part_moves[part] = 0
        byte, bit = divmod(part, 8)
        self._part_moved_bitmap[byte] |= (128 >> bit)

    def _has_part_moved(self, part):
        byte, bit = divmod(part, 8)
        return bool(self._part_moved_bitmap[byte] & (128 >> bit))

    def _can_part_move(self, part):
        # if min_part_hours is zero then checking _last_part_moves will not
        # indicate if the part has already moved during the current rebalance,
        # but _has_part_moved will.
        return (self._last_part_moves[part] >= self.min_part_hours and
                not self._has_part_moved(part))

    @contextmanager
    def debug(self):
        """
        Temporarily enables debug logging, useful in tests, e.g.

            with rb.debug():
                rb.rebalance()
        """
        self.logger.disabled = False
        try:
            yield
        finally:
            self.logger.disabled = True

    @property
    def min_part_seconds_left(self):
        """Get the total seconds until a rebalance can be performed"""
        elapsed_seconds = int(time() - self._last_part_moves_epoch)
        return max((self.min_part_hours * 3600) - elapsed_seconds, 0)

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

    @classmethod
    def from_dict(cls, builder_data):
        b = cls(1, 1, 1)  # Dummy values
        b.copy_from(builder_data)
        return b

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
            self.next_part_power = builder.next_part_power
            self.replicas = builder.replicas
            self.min_part_hours = builder.min_part_hours
            self.parts = builder.parts
            self.devs = builder.devs
            self.devs_changed = builder.devs_changed
            self.overload = builder.overload
            self.version = builder.version
            self._replica2part2dev = builder._replica2part2dev
            self._last_part_moves_epoch = builder._last_part_moves_epoch
            if builder._last_part_moves is None:
                self._last_part_moves = array(
                    'B', itertools.repeat(0, self.parts))
            else:
                self._last_part_moves = builder._last_part_moves
            self._last_part_gather_start = builder._last_part_gather_start
            self._remove_devs = builder._remove_devs
            self._id = getattr(builder, '_id', None)
        else:
            self.part_power = builder['part_power']
            self.next_part_power = builder.get('next_part_power')
            self.replicas = builder['replicas']
            self.min_part_hours = builder['min_part_hours']
            self.parts = builder['parts']
            self.devs = builder['devs']
            self.devs_changed = builder['devs_changed']
            self.overload = builder.get('overload', 0.0)
            self.version = builder['version']
            self._replica2part2dev = builder['_replica2part2dev']
            self._last_part_moves_epoch = builder['_last_part_moves_epoch']
            if builder['_last_part_moves'] is None:
                self._last_part_moves = array(
                    'B', itertools.repeat(0, self.parts))
            else:
                self._last_part_moves = builder['_last_part_moves']
            self._last_part_gather_start = builder['_last_part_gather_start']
            self._dispersion_graph = builder.get('_dispersion_graph', {})
            self.dispersion = builder.get('dispersion')
            self._remove_devs = builder['_remove_devs']
            self._id = builder.get('id')
        self._ring = None

        # Old builders may not have a region defined for their devices, in
        # which case we default it to 1.
        for dev in self._iter_devs():
            dev.setdefault("region", 1)

        if not self._last_part_moves_epoch:
            self._last_part_moves_epoch = 0

    def __deepcopy__(self, memo):
        return type(self).from_dict(deepcopy(self.to_dict(), memo))

    def to_dict(self):
        """
        Returns a dict that can be used later with copy_from to
        restore a RingBuilder. swift-ring-builder uses this to
        pickle.dump the dict to a file and later load that dict into
        copy_from.
        """
        return {'part_power': self.part_power,
                'next_part_power': self.next_part_power,
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
                '_remove_devs': self._remove_devs,
                'id': self._id}

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
                self._ring = RingData([], devs, self.part_shift)
            else:
                self._ring = \
                    RingData([array('H', p2d) for p2d in
                              self._replica2part2dev],
                             devs, self.part_shift,
                             self.next_part_power)
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
                try:
                    dev['id'] = self.devs.index(None)
                except ValueError:
                    dev['id'] = len(self.devs)
        if dev['id'] < len(self.devs) and self.devs[dev['id']] is not None:
            raise exceptions.DuplicateDeviceError(
                'Duplicate device id: %d' % dev['id'])
        # Add holes to self.devs to ensure self.devs[dev['id']] will be the dev
        while dev['id'] >= len(self.devs):
            self.devs.append(None)
        required_keys = ('ip', 'port', 'weight')
        if any(required not in dev for required in required_keys):
            raise ValueError(
                '%r is missing at least one the required key %r' % (
                    dev, required_keys))
        dev['weight'] = float(dev['weight'])
        dev['parts'] = 0
        dev.setdefault('meta', '')
        self.devs[dev['id']] = dev
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
        if any(dev_id == d['id'] for d in self._remove_devs):
            raise ValueError("Can not set weight of dev_id %s because it "
                             "is marked for removal" % (dev_id,))
        self.devs[dev_id]['weight'] = weight
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
        below 1% or doesn't change by more than 1% (only happens with a ring
        that can't be balanced no matter what).

        :param seed: a value for the random seed (optional)
        :returns: (number_of_partitions_altered, resulting_balance,
                   number_of_removed_devices)
        """
        # count up the devs, and cache some stuff
        num_devices = 0
        for dev in self._iter_devs():
            dev['tiers'] = tiers_for_dev(dev)
            if dev['weight'] > 0:
                num_devices += 1
        if num_devices < self.replicas:
            raise exceptions.RingValidationError(
                "Replica count of %(replicas)s requires more "
                "than %(num_devices)s devices" % {
                    'replicas': self.replicas,
                    'num_devices': num_devices,
                })

        self._ring = None

        old_replica2part2dev = copy.deepcopy(self._replica2part2dev)

        if not self.ever_rebalanced:
            self.logger.debug("New builder; performing initial balance")

        self._update_last_part_moves()

        with _set_random_seed(seed):
            replica_plan = self._build_replica_plan()
            self._set_parts_wanted(replica_plan)

            assign_parts = defaultdict(list)
            # gather parts from replica count adjustment
            self._adjust_replica2part2dev_size(assign_parts)
            # gather parts from failed devices
            removed_devs = self._gather_parts_from_failed_devices(assign_parts)
            # gather parts for dispersion (N.B. this only picks up parts that
            # *must* disperse according to the replica plan)
            self._gather_parts_for_dispersion(assign_parts, replica_plan)

            # we'll gather a few times, or until we archive the plan
            for gather_count in range(MAX_BALANCE_GATHER_COUNT):
                self._gather_parts_for_balance(assign_parts, replica_plan,
                                               # firsrt attempt go for disperse
                                               gather_count == 0)
                if not assign_parts:
                    # most likely min part hours
                    finish_status = 'Unable to finish'
                    break
                assign_parts_list = list(assign_parts.items())
                # shuffle the parts to be reassigned, we have no preference on
                # the order in which the replica plan is fulfilled.
                random.shuffle(assign_parts_list)
                # reset assign_parts map for next iteration
                assign_parts = defaultdict(list)

                num_part_replicas = sum(len(r) for p, r in assign_parts_list)
                self.logger.debug("Gathered %d parts", num_part_replicas)
                self._reassign_parts(assign_parts_list, replica_plan)
                self.logger.debug("Assigned %d parts", num_part_replicas)

                if not sum(d['parts_wanted'] < 0 for d in
                           self._iter_devs()):
                    finish_status = 'Finished'
                    break
            else:
                finish_status = 'Unable to finish'
            self.logger.debug(
                '%(status)s rebalance plan after %(count)s attempts',
                {'status': finish_status, 'count': gather_count + 1})

        self.devs_changed = False
        changed_parts = self._build_dispersion_graph(old_replica2part2dev)

        # clean up the cache
        for dev in self._iter_devs():
            dev.pop('tiers', None)

        return changed_parts, self.get_balance(), removed_devs

    def _build_dispersion_graph(self, old_replica2part2dev=None):
        """
        Build a dict of all tiers in the cluster to a list of the number of
        parts with a replica count at each index.  The values of the dict will
        be lists of length the maximum whole replica + 1 so that the
        graph[tier][3] is the number of parts within the tier with 3 replicas
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
            old_replica2part2dev can be used to count moved parts.

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

        dispersion_graph = {}
        # go over all the devices holding each replica part by part
        for part_id, dev_ids in enumerate(
                six.moves.zip(*self._replica2part2dev)):
            # count the number of replicas of this part for each tier of each
            # device, some devices may have overlapping tiers!
            replicas_at_tier = defaultdict(int)
            for rep_id, dev in enumerate(iter(
                    self.devs[dev_id] for dev_id in dev_ids)):
                for tier in (dev.get('tiers') or tiers_for_dev(dev)):
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
            # update running totals for each tiers' number of parts with a
            # given replica count
            part_risk_depth = defaultdict(int)
            part_risk_depth[0] = 0
            for tier, replicas in replicas_at_tier.items():
                if tier not in dispersion_graph:
                    dispersion_graph[tier] = [self.parts] + [0] * int_replicas
                dispersion_graph[tier][0] -= 1
                dispersion_graph[tier][replicas] += 1
                if replicas > max_allowed_replicas[tier]:
                    part_risk_depth[len(tier)] += (
                        replicas - max_allowed_replicas[tier])
            # count each part-replica once at tier where dispersion is worst
            parts_at_risk += max(part_risk_depth.values())
        self._dispersion_graph = dispersion_graph
        self.dispersion = 100.0 * parts_at_risk / (self.parts * self.replicas)
        self.version += 1
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
            dev_usage = array('I', (0 for _junk in range(dev_len)))
            for part2dev in self._replica2part2dev:
                for dev_id in part2dev:
                    dev_usage[dev_id] += 1

        for dev in self._iter_devs():
            if not isinstance(dev['port'], int):
                raise exceptions.RingValidationError(
                    "Device %d has port %r, which is not an integer." %
                    (dev['id'], dev['port']))

        int_replicas = int(math.ceil(self.replicas))
        rep2part_len = list(map(len, self._replica2part2dev))
        # check the assignments of each part's replicas
        for part in range(self.parts):
            devs_for_part = []
            for replica, part_len in enumerate(rep2part_len):
                if part_len <= part:
                    # last replica may be short on parts because of floating
                    # replica count
                    if replica + 1 < int_replicas:
                        raise exceptions.RingValidationError(
                            "The partition assignments of replica %r were "
                            "shorter than expected (%s < %s) - this should "
                            "only happen for the last replica" % (
                                replica,
                                len(self._replica2part2dev[replica]),
                                self.parts,
                            ))
                    break
                dev_id = self._replica2part2dev[replica][part]
                if dev_id >= dev_len or not self.devs[dev_id]:
                    raise exceptions.RingValidationError(
                        "Partition %d, replica %d was not allocated "
                        "to a device." %
                        (part, replica))
                devs_for_part.append(dev_id)
            if len(devs_for_part) != len(set(devs_for_part)):
                raise exceptions.RingValidationError(
                    "The partition %s has been assigned to "
                    "duplicate devices %r" % (
                        part, devs_for_part))

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

    def _build_balance_per_dev(self):
        """
        Build a map of <device_id> => <balance> where <balance> is a float
        representing the percentage difference from the desired amount of
        partitions a given device wants and the amount it has.

        N.B. this method only considers a device's weight and the parts
        assigned, not the parts wanted according to the replica plan.
        """
        weight_of_one_part = self.weight_of_one_part()
        balance_per_dev = {}
        for dev in self._iter_devs():
            if not dev['weight']:
                if dev['parts']:
                    # If a device has no weight, but has partitions, then its
                    # overage is considered "infinity" and therefore always the
                    # worst possible. We show MAX_BALANCE for convenience.
                    balance = MAX_BALANCE
                else:
                    balance = 0
            else:
                balance = 100.0 * dev['parts'] / (
                    dev['weight'] * weight_of_one_part) - 100.0
            balance_per_dev[dev['id']] = balance
        return balance_per_dev

    def get_balance(self):
        """
        Get the balance of the ring. The balance value is the highest
        percentage of the desired amount of partitions a given device
        wants. For instance, if the "worst" device wants (based on its
        weight relative to the sum of all the devices' weights) 123
        partitions and it has 124 partitions, the balance value would
        be 0.83 (1 extra / 123 wanted * 100 for percentage).

        :returns: balance of the ring
        """
        balance_per_dev = self._build_balance_per_dev()
        return max(abs(b) for b in balance_per_dev.values())

    def get_required_overload(self, weighted=None, wanted=None):
        """
        Returns the minimum overload value required to make the ring maximally
        dispersed.

        The required overload is the largest percentage change of any single
        device from its weighted replicanth to its wanted replicanth (note:
        under weighted devices have a negative percentage change) to archive
        dispersion - that is to say a single device that must be overloaded by
        5% is worse than 5 devices in a single tier overloaded by 1%.
        """
        weighted = weighted or self._build_weighted_replicas_by_tier()
        wanted = wanted or self._build_wanted_replicas_by_tier()
        max_overload = 0.0
        for dev in self._iter_devs():
            tier = (dev['region'], dev['zone'], dev['ip'], dev['id'])
            if not dev['weight']:
                if tier not in wanted or not wanted[tier]:
                    continue
                raise exceptions.RingValidationError(
                    'Device %s has zero weight and '
                    'should not want any replicas' % (tier,))
            required = (wanted[tier] - weighted[tier]) / weighted[tier]
            self.logger.debug('%(tier)s wants %(wanted)s and is weighted for '
                              '%(weight)s so therefore requires %(required)s '
                              'overload', {'tier': pretty_dev(dev),
                                           'wanted': wanted[tier],
                                           'weight': weighted[tier],
                                           'required': required})
            if required > max_overload:
                max_overload = required
        return max_overload

    def pretend_min_part_hours_passed(self):
        """
        Override min_part_hours by marking all partitions as having been moved
        255 hours ago and last move epoch to 'the beginning of time'. This can
        be used to force a full rebalance on the next call to rebalance.
        """
        self._last_part_moves_epoch = 0
        if not self._last_part_moves:
            return
        for part in range(self.parts):
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

    def _build_tier2children(self):
        """
        Wrap helper build_tier_tree so exclude zero-weight devices.
        """
        return build_tier_tree(d for d in self._iter_devs() if d['weight'])

    def _set_parts_wanted(self, replica_plan):
        """
        Sets the parts_wanted key for each of the devices to the number of
        partitions the device wants based on its relative weight. This key is
        used to sort the devices according to "most wanted" during rebalancing
        to best distribute partitions. A negative parts_wanted indicates the
        device is "overweight" and wishes to give partitions away if possible.

        :param replica_plan: a dict of dicts, as returned from
                             _build_replica_plan, that maps
                             each tier to it's target replicanths.
        """
        tier2children = self._build_tier2children()

        parts_by_tier = defaultdict(int)

        def place_parts(tier, parts):
            parts_by_tier[tier] = parts
            sub_tiers = sorted(tier2children[tier])
            if not sub_tiers:
                return
            to_place = defaultdict(int)
            for t in sub_tiers:
                to_place[t] = min(parts, int(math.floor(
                    replica_plan[t]['target'] * self.parts)))
                parts -= to_place[t]

            # if there's some parts left over, just throw 'em about
            sub_tier_gen = itertools.cycle(sorted(
                sub_tiers, key=lambda t: replica_plan[t]['target']))
            while parts > 0:
                t = next(sub_tier_gen)
                to_place[t] += 1
                parts -= 1

            for t, p in to_place.items():
                place_parts(t, p)

        total_parts = int(self.replicas * self.parts)
        place_parts((), total_parts)

        # belts & suspenders/paranoia -  at every level, the sum of
        # parts_by_tier should be total_parts for the ring
        tiers = ['cluster', 'regions', 'zones', 'servers', 'devices']
        for i, tier_name in enumerate(tiers):
            parts_at_tier = sum(parts_by_tier[t] for t in parts_by_tier
                                if len(t) == i)
            if parts_at_tier != total_parts:
                raise exceptions.RingValidationError(
                    '%s != %s at tier %s' % (
                        parts_at_tier, total_parts, tier_name))

        for dev in self._iter_devs():
            if not dev['weight']:
                # With no weight, that means we wish to "drain" the device. So
                # we set the parts_wanted to a really large negative number to
                # indicate its strong desire to give up everything it has.
                dev['parts_wanted'] = -self.parts * self.replicas
            else:
                tier = (dev['region'], dev['zone'], dev['ip'], dev['id'])
                dev['parts_wanted'] = parts_by_tier[tier] - dev['parts']

    def _update_last_part_moves(self):
        """
        Updates how many hours ago each partition was moved based on the
        current time. The builder won't move a partition that has been moved
        more recently than min_part_hours.
        """
        self._part_moved_bitmap = bytearray(max(2 ** (self.part_power - 3), 1))
        elapsed_hours = int(time() - self._last_part_moves_epoch) // 3600
        if elapsed_hours <= 0:
            return
        for part in range(self.parts):
            # The "min(self._last_part_moves[part] + elapsed_hours, 0xff)"
            # which was here showed up in profiling, so it got inlined.
            last_plus_elapsed = self._last_part_moves[part] + elapsed_hours
            if last_plus_elapsed < 0xff:
                self._last_part_moves[part] = last_plus_elapsed
            else:
                self._last_part_moves[part] = 0xff
        self._last_part_moves_epoch = int(time())

    def _gather_parts_from_failed_devices(self, assign_parts):
        """
        Update the map of partition => [replicas] to be reassigned from
        removed devices.
        """
        # First we gather partitions from removed devices. Since removed
        # devices usually indicate device failures, we have no choice but to
        # reassign these partitions. However, we mark them as moved so later
        # choices will skip other replicas of the same partition if possible.

        if self._remove_devs:
            dev_ids = [d['id'] for d in self._remove_devs if d['parts']]
            if dev_ids:
                for part, replica in self._each_part_replica():
                    dev_id = self._replica2part2dev[replica][part]
                    if dev_id in dev_ids:
                        self._replica2part2dev[replica][part] = NONE_DEV
                        self._set_part_moved(part)
                        assign_parts[part].append(replica)
                        self.logger.debug(
                            "Gathered %d/%d from dev %d [dev removed]",
                            part, replica, dev_id)
        removed_devs = 0
        while self._remove_devs:
            remove_dev_id = self._remove_devs.pop()['id']
            self.logger.debug("Removing dev %d", remove_dev_id)
            self.devs[remove_dev_id] = None
            removed_devs += 1
        return removed_devs

    def _adjust_replica2part2dev_size(self, to_assign):
        """
        Make sure that the lengths of the arrays in _replica2part2dev
        are correct for the current value of self.replicas.

        Example:
        self.part_power = 8
        self.replicas = 2.25

        self._replica2part2dev will contain 3 arrays: the first 2 of
        length 256 (2**8), and the last of length 64 (0.25 * 2**8).

        Update the mapping of partition => [replicas] that need assignment.
        """
        fractional_replicas, whole_replicas = math.modf(self.replicas)
        whole_replicas = int(whole_replicas)
        removed_parts = 0
        new_parts = 0

        desired_lengths = [self.parts] * whole_replicas
        if fractional_replicas:
            desired_lengths.append(int(self.parts * fractional_replicas))

        if self._replica2part2dev is not None:
            # If we crossed an integer threshold (say, 4.1 --> 4),
            # we'll have a partial extra replica clinging on here. Clean
            # up any such extra stuff.
            for part2dev in self._replica2part2dev[len(desired_lengths):]:
                for dev_id in part2dev:
                    dev_losing_part = self.devs[dev_id]
                    dev_losing_part['parts'] -= 1
                    removed_parts -= 1
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
                    for part in range(len(part2dev), desired_length):
                        to_assign[part].append(replica)
                        part2dev.append(NONE_DEV)
                        new_parts += 1
                elif len(part2dev) > desired_length:
                    # Too long: truncate this mapping.
                    for part in range(desired_length, len(part2dev)):
                        dev_losing_part = self.devs[part2dev[part]]
                        dev_losing_part['parts'] -= 1
                        removed_parts -= 1
                    self._replica2part2dev[replica] = part2dev[:desired_length]
            else:
                # Mapping not present at all: make one up and assign
                # all of it.
                for part in range(desired_length):
                    to_assign[part].append(replica)
                    new_parts += 1
                self._replica2part2dev.append(
                    array('H', itertools.repeat(NONE_DEV, desired_length)))

        self.logger.debug(
            "%d new parts and %d removed parts from replica-count change",
            new_parts, removed_parts)

    def _gather_parts_for_dispersion(self, assign_parts, replica_plan):
        """
        Update the map of partition => [replicas] to be reassigned from
        insufficiently-far-apart replicas.
        """
        # Now we gather partitions that are "at risk" because they aren't
        # currently sufficient spread out across the cluster.
        for part in range(self.parts):
            if (not self._can_part_move(part)):
                continue
            # First, add up the count of replicas at each tier for each
            # partition.
            replicas_at_tier = defaultdict(int)
            for dev in self._devs_for_part(part):
                for tier in dev['tiers']:
                    replicas_at_tier[tier] += 1

            # Now, look for partitions not yet spread out enough.
            undispersed_dev_replicas = []
            for replica in self._replicas_for_part(part):
                dev_id = self._replica2part2dev[replica][part]
                if dev_id == NONE_DEV:
                    continue
                dev = self.devs[dev_id]
                if all(replicas_at_tier[tier] <=
                       replica_plan[tier]['max']
                       for tier in dev['tiers']):
                    continue
                undispersed_dev_replicas.append((dev, replica))

            if not undispersed_dev_replicas:
                continue

            undispersed_dev_replicas.sort(
                key=lambda dr: dr[0]['parts_wanted'])
            for dev, replica in undispersed_dev_replicas:
                # the min part hour check is ignored if and only if a device
                # has more than one replica of a part assigned to it - which
                # would have only been possible on rings built with an older
                # version of the code
                if (not self._can_part_move(part) and
                        not replicas_at_tier[dev['tiers'][-1]] > 1):
                    continue
                dev['parts_wanted'] += 1
                dev['parts'] -= 1
                assign_parts[part].append(replica)
                self.logger.debug(
                    "Gathered %d/%d from dev %s [dispersion]",
                    part, replica, pretty_dev(dev))
                self._replica2part2dev[replica][part] = NONE_DEV
                for tier in dev['tiers']:
                    replicas_at_tier[tier] -= 1
                self._set_part_moved(part)

    def _gather_parts_for_balance_can_disperse(self, assign_parts, start,
                                               replica_plan):
        """
        Update the map of partition => [replicas] to be reassigned from
        overweight drives where the replicas can be better dispersed to
        another failure domain.

        :param assign_parts: the map of partition => [replica] to update
        :param start: offset into self.parts to begin search
        :param replica_plan: replicanth targets for tiers
        """
        tier2children = self._build_tier2children()
        parts_wanted_in_tier = defaultdict(int)
        for dev in self._iter_devs():
            wanted = max(dev['parts_wanted'], 0)
            for tier in dev['tiers']:
                parts_wanted_in_tier[tier] += wanted
        # Last, we gather partitions from devices that are "overweight" because
        # they have more partitions than their parts_wanted.
        for offset in range(self.parts):
            part = (start + offset) % self.parts
            if (not self._can_part_move(part)):
                continue
            # For each part we'll look at the devices holding those parts and
            # see if any are overweight, keeping track of replicas_at_tier as
            # we go
            overweight_dev_replica = []
            replicas_at_tier = defaultdict(int)
            for replica in self._replicas_for_part(part):
                dev_id = self._replica2part2dev[replica][part]
                if dev_id == NONE_DEV:
                    continue
                dev = self.devs[dev_id]
                for tier in dev['tiers']:
                    replicas_at_tier[tier] += 1
                if dev['parts_wanted'] < 0:
                    overweight_dev_replica.append((dev, replica))

            if not overweight_dev_replica:
                continue

            overweight_dev_replica.sort(
                key=lambda dr: dr[0]['parts_wanted'])
            for dev, replica in overweight_dev_replica:
                if any(replica_plan[tier]['min'] <=
                       replicas_at_tier[tier] <
                       replica_plan[tier]['max']
                       for tier in dev['tiers']):
                    # we're stuck by replica plan
                    continue
                for t in reversed(dev['tiers']):
                    if replicas_at_tier[t] - 1 < replica_plan[t]['min']:
                        # we're stuck at tier t
                        break
                if sum(parts_wanted_in_tier[c]
                       for c in tier2children[t]
                       if c not in dev['tiers']) <= 0:
                    # we're stuck by weight
                    continue
                # this is the most overweight_device holding a replica
                # of this part that can shed it according to the plan
                dev['parts_wanted'] += 1
                dev['parts'] -= 1
                assign_parts[part].append(replica)
                self.logger.debug(
                    "Gathered %d/%d from dev %s [weight disperse]",
                    part, replica, pretty_dev(dev))
                self._replica2part2dev[replica][part] = NONE_DEV
                for tier in dev['tiers']:
                    replicas_at_tier[tier] -= 1
                    parts_wanted_in_tier[tier] -= 1
                self._set_part_moved(part)
                break

    def _gather_parts_for_balance(self, assign_parts, replica_plan,
                                  disperse_first):
        """
        Gather parts that look like they should move for balance reasons.

        A simple gathers of parts that looks dispersible normally works out,
        we'll switch strategies if things don't seem to move.
        :param disperse_first: boolean, avoid replicas on overweight devices
                               that need to be there for dispersion
        """
        # pick a random starting point on the other side of the ring
        quarter_turn = (self.parts // 4)
        random_half = random.randint(0, self.parts // 2)
        start = (self._last_part_gather_start + quarter_turn +
                 random_half) % self.parts
        self.logger.debug('Gather start is %(start)s '
                          '(Last start was %(last_start)s)',
                          {'start': start,
                           'last_start': self._last_part_gather_start})
        self._last_part_gather_start = start

        if disperse_first:
            self._gather_parts_for_balance_can_disperse(
                assign_parts, start, replica_plan)
        self._gather_parts_for_balance_forced(assign_parts, start)

    def _gather_parts_for_balance_forced(self, assign_parts, start, **kwargs):
        """
        Update the map of partition => [replicas] to be reassigned from
        overweight drives without restriction, parts gathered from this method
        may be placed back onto devices that are no better (or worse) than the
        device from which they are gathered.

        This method allows devices to flop around enough to unlock replicas
        that would have otherwise potentially been locked because of
        dispersion - it should be used as a last resort.

        :param assign_parts: the map of partition => [replica] to update
        :param start: offset into self.parts to begin search
        """
        for offset in range(self.parts):
            part = (start + offset) % self.parts
            if (not self._can_part_move(part)):
                continue
            overweight_dev_replica = []
            for replica in self._replicas_for_part(part):
                dev_id = self._replica2part2dev[replica][part]
                if dev_id == NONE_DEV:
                    continue
                dev = self.devs[dev_id]
                if dev['parts_wanted'] < 0:
                    overweight_dev_replica.append((dev, replica))

            if not overweight_dev_replica:
                continue

            overweight_dev_replica.sort(
                key=lambda dr: dr[0]['parts_wanted'])

            dev, replica = overweight_dev_replica[0]
            # this is the most overweight_device holding a replica of this
            # part we don't know where it's going to end up - but we'll
            # pick it up and hope for the best.
            dev['parts_wanted'] += 1
            dev['parts'] -= 1
            assign_parts[part].append(replica)
            self.logger.debug(
                "Gathered %d/%d from dev %s [weight forced]",
                part, replica, pretty_dev(dev))
            self._replica2part2dev[replica][part] = NONE_DEV
            self._set_part_moved(part)

    def _reassign_parts(self, reassign_parts, replica_plan):
        """
        For an existing ring data set, partitions are reassigned similar to
        the initial assignment.

        The devices are ordered by how many partitions they still want and
        kept in that order throughout the process.

        The gathered partitions are iterated through, assigning them to
        devices according to the "most wanted" while keeping the replicas as
        "far apart" as possible.

        Two different regions are considered the farthest-apart things,
        followed by zones, then different ip within a zone; the
        least-far-apart things are different devices with the same ip in the
        same zone.

        :param reassign_parts: An iterable of (part, replicas_to_replace)
                               pairs. replicas_to_replace is an iterable of the
                               replica (an int) to replace for that partition.
                               replicas_to_replace may be shared for multiple
                               partitions, so be sure you do not modify it.
        """
        parts_available_in_tier = defaultdict(int)
        for dev in self._iter_devs():
            dev['sort_key'] = self._sort_key_for(dev)
            # Note: this represents how many partitions may be assigned to a
            # given tier (region/zone/server/disk). It does not take into
            # account how many partitions a given tier wants to shed.
            #
            # If we did not do this, we could have a zone where, at some
            # point during an assignment, number-of-parts-to-gain equals
            # number-of-parts-to-shed. At that point, no further placement
            # into that zone would occur since its parts_available_in_tier
            # would be 0. This would happen any time a zone had any device
            # with partitions to shed, which is any time a device is being
            # removed, which is a pretty frequent operation.
            wanted = max(dev['parts_wanted'], 0)
            for tier in dev['tiers']:
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
            # always update part_moves for min_part_hours
            self._last_part_moves[part] = 0
            # count up where these replicas be
            replicas_at_tier = defaultdict(int)
            for dev in self._devs_for_part(part):
                for tier in dev['tiers']:
                    replicas_at_tier[tier] += 1

            for replica in replace_replicas:
                # Find a new home for this replica
                tier = ()
                # This used to be a cute, recursive function, but it's been
                # unrolled for performance.
                depth = 1
                while depth <= max_tier_depth:
                    # Choose the roomiest tier among those that don't
                    # already have their max replicas assigned according
                    # to the replica_plan.
                    candidates = [t for t in tier2children[tier] if
                                  replicas_at_tier[t] <
                                  replica_plan[t]['max']]

                    if not candidates:
                        raise Exception('no home for %s/%s %s' % (
                            part, replica, {t: (
                                replicas_at_tier[t],
                                replica_plan[t]['max'],
                            ) for t in tier2children[tier]}))
                    tier = max(candidates, key=lambda t:
                               parts_available_in_tier[t])

                    depth += 1

                dev = tier2devs[tier][-1]
                dev['parts_wanted'] -= 1
                dev['parts'] += 1
                for tier in dev['tiers']:
                    parts_available_in_tier[tier] -= 1
                    replicas_at_tier[tier] += 1

                self._replica2part2dev[replica][part] = dev['id']
                self.logger.debug(
                    "Placed %d/%d onto dev %s", part, replica, pretty_dev(dev))

        # Just to save memory and keep from accidental reuse.
        for dev in self._iter_devs():
            del dev['sort_key']

    @staticmethod
    def _sort_key_for(dev):
        return (dev['parts_wanted'], random.randint(0, 0xFFFF), dev['id'])

    def _build_max_replicas_by_tier(self, bound=math.ceil):
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
        tier2children = self._build_tier2children()

        def walk_tree(tier, replica_count):
            if len(tier) == 4:
                # special case for device, it's not recursive
                replica_count = min(1, replica_count)
            mr = {tier: replica_count}
            if tier in tier2children:
                subtiers = tier2children[tier]
                for subtier in subtiers:
                    submax = bound(float(replica_count) / len(subtiers))
                    mr.update(walk_tree(subtier, submax))
            return mr
        mr = defaultdict(float)
        mr.update(walk_tree((), self.replicas))
        return mr

    def _build_weighted_replicas_by_tier(self):
        """
        Returns a dict mapping <tier> => replicanths for all tiers in
        the ring based on their weights.
        """
        weight_of_one_part = self.weight_of_one_part()

        # assign each device some replicanths by weight (can't be > 1)
        weighted_replicas_for_dev = {}
        devices_with_room = []
        for dev in self._iter_devs():
            if not dev['weight']:
                continue
            weighted_replicas = (
                dev['weight'] * weight_of_one_part / self.parts)
            if weighted_replicas < 1:
                devices_with_room.append(dev['id'])
            else:
                weighted_replicas = 1
            weighted_replicas_for_dev[dev['id']] = weighted_replicas

        while True:
            remaining = self.replicas - sum(weighted_replicas_for_dev.values())
            if remaining < 1e-10:
                break
            devices_with_room = [d for d in devices_with_room if
                                 weighted_replicas_for_dev[d] < 1]
            rel_weight = remaining / sum(
                weighted_replicas_for_dev[d] for d in devices_with_room)
            for d in devices_with_room:
                weighted_replicas_for_dev[d] = min(
                    1, weighted_replicas_for_dev[d] * (rel_weight + 1))

        weighted_replicas_by_tier = defaultdict(float)
        for dev in self._iter_devs():
            if not dev['weight']:
                continue
            assigned_replicanths = weighted_replicas_for_dev[dev['id']]
            dev_tier = (dev['region'], dev['zone'], dev['ip'], dev['id'])
            for i in range(len(dev_tier) + 1):
                tier = dev_tier[:i]
                weighted_replicas_by_tier[tier] += assigned_replicanths

        # belts & suspenders/paranoia -  at every level, the sum of
        # weighted_replicas should be very close to the total number of
        # replicas for the ring
        validate_replicas_by_tier(self.replicas, weighted_replicas_by_tier)

        return weighted_replicas_by_tier

    def _build_wanted_replicas_by_tier(self):
        """
        Returns a defaultdict of (tier: replicanths) for all tiers in the ring
        based on unique-as-possible (full dispersion) with respect to their
        weights and device counts.

        N.B.  _build_max_replicas_by_tier calculates the upper bound on the
        replicanths each tier may hold irrespective of the weights of the
        tier; this method will calculate the minimum replicanth <=
        max_replicas[tier] that will still solve dispersion.  However, it is
        not guaranteed to return a fully dispersed solution if failure domains
        are over-weighted for their device count.
        """
        weighted_replicas = self._build_weighted_replicas_by_tier()
        dispersed_replicas = {
            t: {
                'min': math.floor(r),
                'max': math.ceil(r),
            } for (t, r) in
            self._build_max_replicas_by_tier(bound=float).items()
        }

        # watch out for device limited tiers
        num_devices = defaultdict(int)
        for d in self._iter_devs():
            if d['weight'] <= 0:
                continue
            for t in (d.get('tiers') or tiers_for_dev(d)):
                num_devices[t] += 1
            num_devices[()] += 1

        tier2children = self._build_tier2children()

        wanted_replicas = defaultdict(float)

        def place_replicas(tier, replicanths):
            if replicanths > num_devices[tier]:
                raise exceptions.RingValidationError(
                    'More replicanths (%s) than devices (%s) '
                    'in tier (%s)' % (replicanths, num_devices[tier], tier))
            wanted_replicas[tier] = replicanths
            sub_tiers = sorted(tier2children[tier])
            if not sub_tiers:
                return

            to_place = defaultdict(float)
            remaining = replicanths
            tiers_to_spread = sub_tiers
            device_limited = False

            while True:
                rel_weight = remaining / sum(weighted_replicas[t]
                                             for t in tiers_to_spread)
                for t in tiers_to_spread:
                    replicas = to_place[t] + (
                        weighted_replicas[t] * rel_weight)
                    if replicas < dispersed_replicas[t]['min']:
                        replicas = dispersed_replicas[t]['min']
                    elif (replicas > dispersed_replicas[t]['max'] and
                          not device_limited):
                        replicas = dispersed_replicas[t]['max']
                    if replicas > num_devices[t]:
                        replicas = num_devices[t]
                    to_place[t] = replicas

                remaining = replicanths - sum(to_place.values())

                if remaining < -1e-10:
                    tiers_to_spread = [
                        t for t in sub_tiers
                        if to_place[t] > dispersed_replicas[t]['min']
                    ]
                elif remaining > 1e-10:
                    tiers_to_spread = [
                        t for t in sub_tiers
                        if (num_devices[t] > to_place[t] <
                            dispersed_replicas[t]['max'])
                    ]
                    if not tiers_to_spread:
                        device_limited = True
                        tiers_to_spread = [
                            t for t in sub_tiers
                            if to_place[t] < num_devices[t]
                        ]
                else:
                    # remaining is "empty"
                    break

            for t in sub_tiers:
                self.logger.debug('Planning %s on %s',
                                  to_place[t], t)
                place_replicas(t, to_place[t])

        # place all replicas in the cluster tier
        place_replicas((), self.replicas)

        # belts & suspenders/paranoia -  at every level, the sum of
        # wanted_replicas should be very close to the total number of
        # replicas for the ring
        validate_replicas_by_tier(self.replicas, wanted_replicas)

        return wanted_replicas

    def _build_target_replicas_by_tier(self):
        """
        Build a map of <tier> => <target_replicas> accounting for device
        weights, unique-as-possible dispersion and overload.

        <tier> - a tuple, describing each tier in the ring topology
        <target_replicas> - a float, the target replicanths at the tier
        """
        weighted_replicas = self._build_weighted_replicas_by_tier()
        wanted_replicas = self._build_wanted_replicas_by_tier()
        max_overload = self.get_required_overload(weighted=weighted_replicas,
                                                  wanted=wanted_replicas)
        if max_overload <= 0.0:
            return wanted_replicas
        else:
            overload = min(self.overload, max_overload)
        self.logger.debug("Using effective overload of %f", overload)
        target_replicas = defaultdict(float)
        for tier, weighted in weighted_replicas.items():
            m = (wanted_replicas[tier] - weighted) / max_overload
            target_replicas[tier] = m * overload + weighted

        # belts & suspenders/paranoia -  at every level, the sum of
        # target_replicas should be very close to the total number
        # of replicas for the ring
        validate_replicas_by_tier(self.replicas, target_replicas)

        return target_replicas

    def _build_replica_plan(self):
        """
        Wraps return value of _build_target_replicas_by_tier to include
        pre-calculated min and max values for each tier.

        :returns: a dict, mapping <tier> => <replica_plan>, where
                  <replica_plan> is itself a dict

        <replica_plan> include at least the following keys:

            min - the minimum number of replicas at the tier
            target - the target replicanths at the tier
            max - the maximum number of replicas at the tier
        """
        # replica part-y planner!
        target_replicas = self._build_target_replicas_by_tier()
        replica_plan = defaultdict(
            lambda: {'min': 0, 'target': 0, 'max': 0})
        replica_plan.update({
            t: {
                'min': math.floor(r + 1e-10),
                'target': r,
                'max': math.ceil(r - 1e-10),
            } for (t, r) in
            target_replicas.items()
        })
        return replica_plan

    def _devs_for_part(self, part):
        """
        Returns a list of devices for a specified partition.

        Deliberately includes duplicates.
        """
        if self._replica2part2dev is None:
            return []
        devs = []
        for part2dev in self._replica2part2dev:
            if part >= len(part2dev):
                continue
            dev_id = part2dev[part]
            if dev_id == NONE_DEV:
                continue
            devs.append(self.devs[dev_id])
        return devs

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
            for part in range(len(part2dev)):
                yield (part, replica)

    @classmethod
    def load(cls, builder_file, open=open, **kwargs):
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
            builder = cls(1, 1, 1, **kwargs)
            builder.copy_from(builder_dict)

        if not hasattr(builder, '_id'):
            builder._id = None

        for dev in builder.devs:
            # really old rings didn't have meta keys
            if dev and 'meta' not in dev:
                dev['meta'] = ''
            # NOTE(akscram): An old ring builder file don't contain
            #                replication parameters.
            if dev:
                dev.setdefault('replication_ip', dev['ip'])
                dev.setdefault('replication_port', dev['port'])
        return builder

    def save(self, builder_file):
        """Serialize this RingBuilder instance to disk.

        :param builder_file: path to builder file to save
        """
        # We want to be sure the builder id's are persistent, so this is the
        # only place where the id is assigned. Newly created instances of this
        # class, or instances loaded from legacy builder files that have no
        # persisted id, must be saved in order for an id to be assigned.
        id_persisted = True
        if self._id is None:
            id_persisted = False
            self._id = uuid.uuid4().hex
        try:
            with open(builder_file, 'wb') as f:
                pickle.dump(self.to_dict(), f, protocol=2)
        except Exception:
            if not id_persisted:
                self._id = None
            raise

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

    def prepare_increase_partition_power(self):
        """
        Prepares a ring for partition power increase.

        This makes it possible to compute the future location of any object
        based on the next partition power.

        In this phase object servers should create hard links when finalizing a
        write to the new location as well. A relinker will be run after
        restarting object-servers, creating hard links to all existing objects
        in their future location.

        :returns: False if next_part_power was not set, otherwise True.
        """
        if self.next_part_power:
            return False
        self.next_part_power = self.part_power + 1
        self.version += 1
        return True

    def increase_partition_power(self):
        """
        Increases ring partition power by one.

        Devices will be assigned to partitions like this:

        OLD: 0, 3, 7, 5, 2, 1, ...
        NEW: 0, 0, 3, 3, 7, 7, 5, 5, 2, 2, 1, 1, ...

        :returns: False if next_part_power was not set or is equal to current
                  part_power, None if something went wrong, otherwise True.
        """

        if not self.next_part_power:
            return False

        if self.next_part_power != (self.part_power + 1):
            return False

        new_replica2part2dev = []
        for replica in self._replica2part2dev:
            new_replica = array('H')
            for device in replica:
                new_replica.append(device)
                new_replica.append(device)  # append device a second time
            new_replica2part2dev.append(new_replica)
        self._replica2part2dev = new_replica2part2dev

        for device in self._iter_devs():
            device['parts'] *= 2

        # We need to update the time when a partition has been moved the last
        # time. Since this is an array of all partitions, we need to double it
        # too
        new_last_part_moves = []
        for partition in self._last_part_moves:
            new_last_part_moves.append(partition)
            new_last_part_moves.append(partition)
        self._last_part_moves = new_last_part_moves

        self.part_power = self.next_part_power
        self.parts *= 2
        self.version += 1
        return True

    def cancel_increase_partition_power(self):
        """
        Cancels a ring partition power increasement.

        This sets the next_part_power to the current part_power. Object
        replicators will still skip replication, and a cleanup is still
        required. Finally, a finish_increase_partition_power needs to be run.

        :returns: False if next_part_power was not set or is equal to current
                  part_power, otherwise True.
        """

        if not self.next_part_power:
            return False

        if self.next_part_power != (self.part_power + 1):
            return False

        self.next_part_power = self.part_power
        self.version += 1
        return True

    def finish_increase_partition_power(self):
        """Finish the partition power increase.

        The hard links from the old object locations should be removed by now.
        """
        if self.next_part_power and self.next_part_power == self.part_power:
            self.next_part_power = None
            self.version += 1
            return True
        return False
