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

from array import array
from random import randint, shuffle
from time import time

from swift.common import exceptions
from swift.common.ring import RingData


class RingBuilder(object):
    """
    Used to build swift.common.RingData instances to be written to disk and
    used with swift.common.ring.Ring instances. See bin/ring-builder.py for
    example usage.

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
        # hours (24 is my usual test). Removing a device or setting it's weight
        # to 0 overrides this behavior as it's assumed those actions are done
        # because of device failure.
        # _last_part_moves_epoch indicates the time the offsets in
        # _last_part_moves is based on.
        self._last_part_moves_epoch = None
        self._last_part_moves = None

        self._last_part_gather_start = 0
        self._remove_devs = []
        self._ring = None

    def weighted_parts(self):
        try:
            return self.parts * self.replicas / \
                     sum(d['weight'] for d in self.devs if d is not None)
        except ZeroDivisionError:
            raise exceptions.EmptyRingError('There are no devices in this '
                                            'ring, or all devices have been '
                                            'deleted')

    def copy_from(self, builder):
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
        if not self._ring:
            devs = [None] * len(self.devs)
            for dev in self.devs:
                if dev is None:
                    continue
                devs[dev['id']] = dict((k, v) for k, v in dev.items()
                                       if k not in ('parts', 'parts_wanted'))
            if not self._replica2part2dev:
                self._ring = RingData([], devs, 32 - self.part_power)
            else:
                self._ring = \
                RingData([array('H', p2d) for p2d in self._replica2part2dev],
                         devs, 32 - self.part_power)
        return self._ring

    def add_dev(self, dev):
        """
        Add a device to the ring. This device dict should have a minimum of the
        following keys:

        ======  ===============================================================
        id      unique integer identifier amongst devices
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
        if dev['id'] < len(self.devs) and self.devs[dev['id']] is not None:
            raise exceptions.DuplicateDeviceError(
                    'Duplicate device id: %d' % dev['id'])
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
        process. It ensures partitions have been assigned to distinct zones,
        aren't doubly assigned, etc. It can also optionally check the even
        distribution of partitions across devices.

        :param stats: if True, check distribution of partitions across devices
        :returns: if stats is True, a tuple of (device usage, worst stat), else
                  (None, None)
        :raises RingValidationError: problem was found with the ring.
        """
        if sum(d['parts'] for d in self.devs if d is not None) != \
                self.parts * self.replicas:
            raise exceptions.RingValidationError(
                'All partitions are not double accounted for: %d != %d' %
                (sum(d['parts'] for d in self.devs if d is not None),
                 self.parts * self.replicas))
        if stats:
            dev_usage = array('I', (0 for _junk in xrange(len(self.devs))))
        for part in xrange(self.parts):
            zones = {}
            for replica in xrange(self.replicas):
                dev_id = self._replica2part2dev[replica][part]
                if stats:
                    dev_usage[dev_id] += 1
                zone = self.devs[dev_id]['zone']
                if zone in zones:
                    raise exceptions.RingValidationError(
                        'Partition %d not in %d distinct zones. ' \
                        'Zones were: %s' %
                        (part, self.replicas,
                         [self.devs[self._replica2part2dev[r][part]]['zone']
                          for r in xrange(self.replicas)]))
                zones[zone] = True
        if stats:
            weighted_parts = self.weighted_parts()
            worst = 0
            for dev in self.devs:
                if dev is None:
                    continue
                if not dev['weight']:
                    if dev_usage[dev['id']]:
                        worst = 999.99
                        break
                    continue
                skew = abs(100.0 * dev_usage[dev['id']] /
                           (dev['weight'] * weighted_parts) - 100.0)
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
        weighted_parts = self.weighted_parts()
        for dev in self.devs:
            if dev is None:
                continue
            if not dev['weight']:
                if dev['parts']:
                    balance = 999.99
                    break
                continue
            dev_balance = abs(100.0 * dev['parts'] /
                              (dev['weight'] * weighted_parts) - 100.0)
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

    def _set_parts_wanted(self):
        """
        Sets the parts_wanted key for each of the devices to the number of
        partitions the device wants based on its relative weight. This key is
        used to sort the devices according to "most wanted" during rebalancing
        to best distribute partitions.
        """
        weighted_parts = self.weighted_parts()

        for dev in self.devs:
            if dev is not None:
                if not dev['weight']:
                    dev['parts_wanted'] = self.parts * -2
                else:
                    dev['parts_wanted'] = \
                        int(weighted_parts * dev['weight']) - dev['parts']

    def _initial_balance(self):
        """
        Initial partition assignment is treated separately from rebalancing an
        existing ring. Initial assignment is performed by ordering all the
        devices by how many partitions they still want (and kept in order
        during the process). The partitions are then iterated through,
        assigning them to the next "most wanted" device, with distinct zone
        restrictions.
        """
        for dev in self.devs:
            if dev is not None:
                dev['sort_key'] = \
                    '%08x.%04x' % (dev['parts_wanted'], randint(0, 0xffff))
        available_devs = sorted((d for d in self.devs if d is not None),
                                key=lambda x: x['sort_key'])
        self._replica2part2dev = \
            [array('H') for _junk in xrange(self.replicas)]
        for _junk in xrange(self.parts):
            other_zones = array('H')
            for replica in xrange(self.replicas):
                index = len(available_devs) - 1
                while available_devs[index]['zone'] in other_zones:
                    index -= 1
                dev = available_devs.pop(index)
                self._replica2part2dev[replica].append(dev['id'])
                dev['parts_wanted'] -= 1
                dev['parts'] += 1
                dev['sort_key'] = \
                    '%08x.%04x' % (dev['parts_wanted'], randint(0, 0xffff))
                index = 0
                end = len(available_devs)
                while index < end:
                    mid = (index + end) // 2
                    if dev['sort_key'] < available_devs[mid]['sort_key']:
                        end = mid
                    else:
                        index = mid + 1
                available_devs.insert(index, dev)
                other_zones.append(dev['zone'])
        self._last_part_moves = array('B', (0 for _junk in xrange(self.parts)))
        self._last_part_moves_epoch = int(time())
        for dev in self.devs:
            if dev is not None:
                del dev['sort_key']

    def _update_last_part_moves(self):
        """
        Updates how many hours ago each partition was moved based on the
        current time. The builder won't move a partition that has been moved
        more recently than min_part_hours.
        """
        elapsed_hours = int(time() - self._last_part_moves_epoch) / 3600
        for part in xrange(self.parts):
            self._last_part_moves[part] = \
                min(self._last_part_moves[part] + elapsed_hours, 0xff)
        self._last_part_moves_epoch = int(time())

    def _gather_reassign_parts(self):
        """
        Returns an array('I') of partitions to be reassigned by gathering them
        from removed devices and overweight devices.
        """
        removed_dev_parts = set()
        reassign_parts = array('I')
        if self._remove_devs:
            dev_ids = [d['id'] for d in self._remove_devs if d['parts']]
            if dev_ids:
                for replica in xrange(self.replicas):
                    part2dev = self._replica2part2dev[replica]
                    for part in xrange(self.parts):
                        if part2dev[part] in dev_ids:
                            part2dev[part] = 0xffff
                            self._last_part_moves[part] = 0
                            removed_dev_parts.add(part)

        start = self._last_part_gather_start / 4 + randint(0, self.parts / 2)
        self._last_part_gather_start = start
        for replica in xrange(self.replicas):
            part2dev = self._replica2part2dev[replica]
            for half in (xrange(start, self.parts), xrange(0, start)):
                for part in half:
                    if self._last_part_moves[part] < self.min_part_hours:
                        continue
                    if part in removed_dev_parts:
                        continue
                    dev = self.devs[part2dev[part]]
                    if dev['parts_wanted'] < 0:
                        part2dev[part] = 0xffff
                        self._last_part_moves[part] = 0
                        dev['parts_wanted'] += 1
                        dev['parts'] -= 1
                        reassign_parts.append(part)

        reassign_parts.extend(removed_dev_parts)
        shuffle(reassign_parts)
        return reassign_parts

    def _reassign_parts(self, reassign_parts):
        """
        For an existing ring data set, partitions are reassigned similarly to
        the initial assignment. The devices are ordered by how many partitions
        they still want and kept in that order throughout the process. The
        gathered partitions are iterated through, assigning them to devices
        according to the "most wanted" and distinct zone restrictions.
        """
        for dev in self.devs:
            if dev is not None:
                dev['sort_key'] = '%08x.%04x' % (self.parts +
                                    dev['parts_wanted'], randint(0, 0xffff))
        available_devs = \
            sorted((d for d in self.devs if d is not None and d['weight']),
                   key=lambda x: x['sort_key'])
        for part in reassign_parts:
            other_zones = array('H')
            replace = []
            for replica in xrange(self.replicas):
                if self._replica2part2dev[replica][part] == 0xffff:
                    replace.append(replica)
                else:
                    other_zones.append(self.devs[
                        self._replica2part2dev[replica][part]]['zone'])

            for replica in replace:
                index = len(available_devs) - 1
                while available_devs[index]['zone'] in other_zones:
                    index -= 1
                dev = available_devs.pop(index)
                other_zones.append(dev['zone'])
                self._replica2part2dev[replica][part] = dev['id']
                dev['parts_wanted'] -= 1
                dev['parts'] += 1
                dev['sort_key'] = \
                    '%08x.%04x' % (self.parts + dev['parts_wanted'],
                                   randint(0, 0xffff))
                index = 0
                end = len(available_devs)
                while index < end:
                    mid = (index + end) // 2
                    if dev['sort_key'] < available_devs[mid]['sort_key']:
                        end = mid
                    else:
                        index = mid + 1
                available_devs.insert(index, dev)
        for dev in self.devs:
            if dev is not None:
                del dev['sort_key']
