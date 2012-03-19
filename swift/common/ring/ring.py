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
from gzip import GzipFile
from os.path import getmtime
from struct import unpack_from
from time import time
from swift.common.utils import hash_path, validate_configuration


class RingData(object):
    """Partitioned consistent hashing ring data (used for serialization)."""

    def __init__(self, replica2part2dev_id, devs, part_shift):
        self.devs = devs
        self._replica2part2dev_id = replica2part2dev_id
        self._part_shift = part_shift

    def to_dict(self):
        return {'devs': self.devs,
                'replica2part2dev_id': self._replica2part2dev_id,
                'part_shift': self._part_shift}


class Ring(object):
    """
    Partitioned consistent hashing ring.

    :param pickle_gz_path: path to ring file
    :param reload_time: time interval in seconds to check for a ring change
    """

    def __init__(self, pickle_gz_path, reload_time=15):
        # can't use the ring unless HASH_PATH_SUFFIX is set
        validate_configuration()
        self.pickle_gz_path = pickle_gz_path
        self.reload_time = reload_time
        self._reload(force=True)

    def _reload(self, force=False):
        self._rtime = time() + self.reload_time
        if force or self.has_changed():
            ring_data = pickle.load(GzipFile(self.pickle_gz_path, 'rb'))
            if not hasattr(ring_data, 'devs'):
                ring_data = RingData(ring_data['replica2part2dev_id'],
                    ring_data['devs'], ring_data['part_shift'])
            self._mtime = getmtime(self.pickle_gz_path)
            self.devs = ring_data.devs
            self.zone2devs = {}
            for dev in self.devs:
                if not dev:
                    continue
                if dev['zone'] in self.zone2devs:
                    self.zone2devs[dev['zone']].append(dev)
                else:
                    self.zone2devs[dev['zone']] = [dev]
            self._replica2part2dev_id = ring_data._replica2part2dev_id
            self._part_shift = ring_data._part_shift

    @property
    def replica_count(self):
        """Number of replicas used in the ring."""
        return len(self._replica2part2dev_id)

    @property
    def partition_count(self):
        """Number of partitions in the ring."""
        return len(self._replica2part2dev_id[0])

    def has_changed(self):
        """
        Check to see if the ring on disk is different than the current one in
        memory.

        :returns: True if the ring on disk has changed, False otherwise
        """
        return getmtime(self.pickle_gz_path) != self._mtime

    def get_part_nodes(self, part):
        """
        Get the nodes that are responsible for the partition.

        :param part: partition to get nodes for
        :returns: list of node dicts

        See :func:`get_nodes` for a description of the node dicts.
        """
        if time() > self._rtime:
            self._reload()
        return [self.devs[r[part]] for r in self._replica2part2dev_id]

    def get_nodes(self, account, container=None, obj=None):
        """
        Get the partition and nodes for an account/container/object.

        :param account: account name
        :param container: container name
        :param obj: object name
        :returns: a tuple of (partition, list of node dicts)

        Each node dict will have at least the following keys:

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
        """
        key = hash_path(account, container, obj, raw_digest=True)
        if time() > self._rtime:
            self._reload()
        part = unpack_from('>I', key)[0] >> self._part_shift
        return part, [self.devs[r[part]] for r in self._replica2part2dev_id]

    def get_more_nodes(self, part):
        """
        Generator to get extra nodes for a partition for hinted handoff.

        :param part: partition to get handoff nodes for
        :returns: generator of node dicts

        See :func:`get_nodes` for a description of the node dicts.
        """
        if time() > self._rtime:
            self._reload()
        zones = sorted(self.zone2devs.keys())
        for part2dev_id in self._replica2part2dev_id:
            zones.remove(self.devs[part2dev_id[part]]['zone'])
        while zones:
            zone = zones.pop(part % len(zones))
            weighted_node = None
            for i in xrange(len(self.zone2devs[zone])):
                node = self.zone2devs[zone][(part + i) %
                                            len(self.zone2devs[zone])]
                if node.get('weight'):
                    weighted_node = node
                    break
            if weighted_node:
                yield weighted_node
