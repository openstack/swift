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

import array

import json
from collections import defaultdict
from os.path import getmtime
import struct
from time import time
import os
from itertools import chain, count
import sys

from swift.common.exceptions import RingLoadError, DevIdBytesTooSmall
from swift.common.utils import hash_path, validate_configuration, md5
from swift.common.ring.io import RingReader, RingWriter
from swift.common.ring.utils import tiers_for_dev


DEFAULT_RELOAD_TIME = 15
RING_CODECS = {
    1: {
        "serialize": lambda ring_data, writer: ring_data.serialize_v1(writer),
        "deserialize": lambda cls, reader, metadata_only, _include_devices:
            cls.deserialize_v1(reader, metadata_only=metadata_only),
    },
    2: {
        "serialize": lambda ring_data, writer: ring_data.serialize_v2(writer),
        "deserialize": lambda cls, reader, metadata_only, include_devices:
            cls.deserialize_v2(reader, metadata_only=metadata_only,
                               include_devices=include_devices),
    },
}
DEFAULT_RING_FORMAT_VERSION = 1


def calc_replica_count(replica2part2dev_id):
    if not replica2part2dev_id:
        return 0
    base = len(replica2part2dev_id) - 1
    extra = 1.0 * len(replica2part2dev_id[-1]) / len(replica2part2dev_id[0])
    return base + extra


def normalize_devices(devs):
    # NOTE(akscram): Replication parameters like replication_ip
    #                and replication_port are required for
    #                replication process. An old replication
    #                ring doesn't contain this parameters into
    #                device.
    for dev in devs:
        if dev is None:
            continue
        if 'ip' in dev:
            dev.setdefault('replication_ip', dev['ip'])
        if 'port' in dev:
            dev.setdefault('replication_port', dev['port'])


class RingData(object):
    """Partitioned consistent hashing ring data (used for serialization)."""

    def __init__(self, replica2part2dev_id, devs, part_shift,
                 next_part_power=None, version=None):
        normalize_devices(devs)
        self.devs = devs
        for i, part2dev_id in enumerate(replica2part2dev_id):
            if not isinstance(part2dev_id, array.array):
                replica2part2dev_id[i] = array.array('H', part2dev_id)
        self._replica2part2dev_id = replica2part2dev_id
        self._part_shift = part_shift
        self.next_part_power = next_part_power
        self.version = version
        self.format_version = None
        self.size = self.raw_size = None
        # Next two are used when replica2part2dev is empty
        self._dev_id_bytes = 2
        self._replica_count = 0
        self._num_devs = sum(1 if dev is not None else 0 for dev in self.devs)

    @property
    def replica_count(self):
        """Number of replicas (full or partial) used in the ring."""
        if self._replica2part2dev_id:
            return calc_replica_count(self._replica2part2dev_id)
        else:
            return self._replica_count

    @property
    def part_power(self):
        return 32 - self._part_shift

    @property
    def dev_id_bytes(self):
        if self._replica2part2dev_id:
            # There's an assumption that these will all have the same itemsize,
            # but just in case...
            return max(part2dev_id.itemsize
                       for part2dev_id in self._replica2part2dev_id)
        else:
            return self._dev_id_bytes

    @classmethod
    def deserialize_v1(cls, reader, metadata_only=False):
        """
        Deserialize a v1 ring file into a dictionary with `devs`, `part_shift`,
        and `replica2part2dev_id` keys.

        If the optional kwarg `metadata_only` is True, then the
        `replica2part2dev_id` is not loaded and that key in the returned
        dictionary just has the value `[]`.

        :param RingReader reader: An opened RingReader at the start of the file
        :param bool metadata_only: If True, only load `devs` and `part_shift`
        :returns: A dict containing `devs`, `part_shift`, and
                  `replica2part2dev_id`
        """
        magic = reader.read(6)
        if magic != b'R1NG\x00\x01':
            raise ValueError('unexpected magic: %r' % magic)

        ring_dict = json.loads(reader.read_blob('!I'))
        ring_dict['replica2part2dev_id'] = []

        if metadata_only:
            return ring_dict

        byteswap = (ring_dict.get('byteorder', sys.byteorder) != sys.byteorder)

        partition_count = 1 << (32 - ring_dict['part_shift'])
        for x in range(ring_dict['replica_count']):
            part2dev = array.array('H', reader.read(2 * partition_count))
            if byteswap:
                part2dev.byteswap()
            ring_dict['replica2part2dev_id'].append(part2dev)

        return ring_dict

    @classmethod
    def deserialize_v2(cls, reader, metadata_only=False, include_devices=True):
        """
        Deserialize a v2 ring file into a dictionary with ``devs``,
        ``part_shift``, and ``replica2part2dev_id`` keys.

        If the optional kwarg ``metadata_only`` is True, then the
        ``replica2part2dev_id`` is not loaded and that key in the returned
        dictionary just has the value ``[]``.

        If the optional kwarg ``include_devices`` is False, then the ``devs``
        list is not loaded and that key in the returned dictionary just has
        the value ``[]``.

        :param RingReader reader: An opened RingReader which has already
                                  loaded up the index at the end of the file.
        :param bool metadata_only: If True, skip loading
                                   ``replica2part2dev_id``
        :param bool include_devices: If False, skip loading ``devs``
        :returns: A dict containing ``devs``, ``part_shift``,
                  ``dev_id_bytes``, and ``replica2part2dev_id``
        """

        ring_dict = json.loads(reader.read_section('swift/ring/metadata'))
        ring_dict['replica2part2dev_id'] = []
        ring_dict['devs'] = []

        if include_devices:
            ring_dict['devs'] = json.loads(
                reader.read_section('swift/ring/devices'))

        if metadata_only:
            return ring_dict

        partition_count = 1 << (32 - ring_dict['part_shift'])

        with reader.open_section('swift/ring/assignments') as section:
            ring_dict['replica2part2dev_id'] = section.read_ring_table(
                ring_dict['dev_id_bytes'], partition_count)

        return ring_dict

    @classmethod
    def load(cls, filename, metadata_only=False, include_devices=True):
        """
        Load ring data from a file.

        :param filename: Path to a file serialized by the save() method.
        :param bool metadata_only: If True, only load `devs` and `part_shift`.
        :returns: A RingData instance containing the loaded data.
        """
        with RingReader.open(filename) as reader:
            if reader.version not in RING_CODECS:
                raise Exception('Unknown ring format version %d for %r' % (
                                reader.version, filename))
            ring_data = RING_CODECS[reader.version]['deserialize'](
                cls, reader, metadata_only, include_devices)

        ring_data = cls.from_dict(ring_data)
        ring_data.format_version = reader.version
        for attr in ('size', 'raw_size'):
            setattr(ring_data, attr, getattr(reader, attr))
        return ring_data

    @classmethod
    def from_dict(cls, ring_data):
        ring = cls(ring_data['replica2part2dev_id'],
                   ring_data['devs'], ring_data['part_shift'],
                   ring_data.get('next_part_power'),
                   ring_data.get('version'))
        # For loading with metadata_only=True
        if 'replica_count' in ring_data:
            ring._replica_count = ring_data['replica_count']
        # dev_id_bytes only written down in v2 and above
        ring._dev_id_bytes = ring_data.get('dev_id_bytes', 2)
        return ring

    def serialize_v1(self, writer):
        if self.dev_id_bytes != 2:
            raise DevIdBytesTooSmall('Ring v1 only supports 2-byte dev IDs')
        # Write out new-style serialization magic and version:
        writer.write_magic(version=1)
        ring = self.to_dict()

        # Only include next_part_power if it is set in the
        # builder, otherwise just ignore it
        _text = {'devs': ring['devs'], 'part_shift': ring['part_shift'],
                 'replica_count': len(ring['replica2part2dev_id']),
                 'byteorder': sys.byteorder}

        if ring['version'] is not None:
            _text['version'] = ring['version']

        next_part_power = ring.get('next_part_power')
        if next_part_power is not None:
            _text['next_part_power'] = next_part_power

        json_text = json.dumps(_text, sort_keys=True,
                               ensure_ascii=True).encode('ascii')
        json_len = len(json_text)
        writer.write(struct.pack('!I', json_len))
        writer.write(json_text)
        for part2dev_id in ring['replica2part2dev_id']:
            part2dev_id.tofile(writer)

    def serialize_v2(self, writer):
        writer.write_magic(version=2)
        ring = self.to_dict()

        # Only include next_part_power if it is set in the
        # builder, otherwise just ignore it
        _text = {
            'part_shift': ring['part_shift'],
            'dev_id_bytes': ring['dev_id_bytes'],
            'replica_count': calc_replica_count(ring['replica2part2dev_id']),
            'version': ring['version']}

        next_part_power = ring.get('next_part_power')
        if next_part_power is not None:
            _text['next_part_power'] = next_part_power

        with writer.section('swift/ring/metadata'):
            writer.write_json(_text)

        with writer.section('swift/ring/devices'):
            writer.write_json(ring['devs'])

        with writer.section('swift/ring/assignments'):
            writer.write_ring_table(ring['replica2part2dev_id'])

    def save(self, filename, mtime=1300507380.0,
             format_version=DEFAULT_RING_FORMAT_VERSION):
        """
        Serialize this RingData instance to disk.

        :param filename: File into which this instance should be serialized.
        :param mtime: time used to override mtime for gzip, default or None
                      if the caller wants to include time
        :param format_version: one of 0, 1, or 2. Older versions are retained
                               for the sake of clusters on older versions
        """
        if format_version not in RING_CODECS:
            raise ValueError("format_version must be one of %r" % (tuple(
                RING_CODECS.keys()),))
        # Override the timestamp so that the same ring data creates
        # the same bytes on disk. This makes a checksum comparison a
        # good way to see if two rings are identical.
        with RingWriter.open(filename, mtime) as writer:
            RING_CODECS[format_version]['serialize'](self, writer)

    def to_dict(self):
        return {'devs': self.devs,
                'replica2part2dev_id': self._replica2part2dev_id,
                'part_shift': self._part_shift,
                'next_part_power': self.next_part_power,
                'dev_id_bytes': self.dev_id_bytes,
                'version': self.version}


class Ring(object):
    """
    Partitioned consistent hashing ring.

    :param serialized_path: path to serialized RingData instance
    :param reload_time: time interval in seconds to check for a ring change
    :param ring_name: ring name string (basically specified from policy)
    :param validation_hook: hook point to validate ring configuration ontime

    :raises RingLoadError: if the loaded ring data violates its constraint
    """

    def __init__(self, serialized_path, reload_time=None, ring_name=None,
                 validation_hook=lambda ring_data: None):
        # can't use the ring unless HASH_PATH_SUFFIX is set
        validate_configuration()
        if ring_name:
            self.serialized_path = os.path.join(serialized_path,
                                                ring_name + '.ring.gz')
        else:
            self.serialized_path = os.path.join(serialized_path)
        self.reload_time = (DEFAULT_RELOAD_TIME if reload_time is None
                            else reload_time)
        self._validation_hook = validation_hook
        self._reload(force=True)

    def _reload(self, force=False):
        self._rtime = time() + self.reload_time
        if force or self.has_changed():
            ring_data = RingData.load(self.serialized_path)

            try:
                self._validation_hook(ring_data)
            except RingLoadError:
                if force:
                    raise
                else:
                    # In runtime reload at working server, it's ok to use old
                    # ring data if the new ring data is invalid.
                    return

            self._mtime = getmtime(self.serialized_path)
            self._devs = ring_data.devs
            self._dev_id_bytes = ring_data._dev_id_bytes
            self._replica2part2dev_id = ring_data._replica2part2dev_id
            self._part_shift = ring_data._part_shift
            self._rebuild_tier_data()
            self._update_bookkeeping()
            self._next_part_power = ring_data.next_part_power
            self._version = ring_data.version
            self._size = ring_data.size
            self._raw_size = ring_data.raw_size

    def _update_bookkeeping(self):
        # Do this now, when we know the data has changed, rather than
        # doing it on every call to get_more_nodes().
        #
        # Since this is to speed up the finding of handoffs, we only
        # consider devices with at least one partition assigned. This
        # way, a region, zone, or server with no partitions assigned
        # does not count toward our totals, thereby keeping the early
        # bailouts in get_more_nodes() working.
        dev_ids_with_parts = set()
        for part2dev_id in self._replica2part2dev_id:
            for dev_id in part2dev_id:
                dev_ids_with_parts.add(dev_id)
        regions = set()
        zones = set()
        ips = set()
        self._num_devs = 0
        self._num_assigned_devs = 0
        self._num_weighted_devs = 0
        for dev in self._devs:
            if dev is None:
                continue
            self._num_devs += 1
            if dev.get('weight', 0) > 0:
                self._num_weighted_devs += 1
            if dev['id'] in dev_ids_with_parts:
                regions.add(dev['region'])
                zones.add((dev['region'], dev['zone']))
                ips.add((dev['region'], dev['zone'], dev['ip']))
                self._num_assigned_devs += 1
        self._num_regions = len(regions)
        self._num_zones = len(zones)
        self._num_ips = len(ips)

    @property
    def dev_id_bytes(self):
        if self._replica2part2dev_id:
            # There's an assumption that these will all have the same itemsize,
            # but just in case...
            return max(part2dev_id.itemsize
                       for part2dev_id in self._replica2part2dev_id)
        else:
            return self._dev_id_bytes

    @property
    def next_part_power(self):
        if time() > self._rtime:
            self._reload()
        return self._next_part_power

    @property
    def part_power(self):
        return 32 - self._part_shift

    @property
    def version(self):
        return self._version

    @property
    def size(self):
        return self._size

    @property
    def raw_size(self):
        return self._raw_size

    def _rebuild_tier_data(self):
        self.tier2devs = defaultdict(list)
        for dev in self._devs:
            if not dev:
                continue
            for tier in tiers_for_dev(dev):
                self.tier2devs[tier].append(dev)

        tiers_by_length = defaultdict(list)
        for tier in self.tier2devs:
            tiers_by_length[len(tier)].append(tier)
        self.tiers_by_length = sorted(tiers_by_length.values(),
                                      key=lambda x: len(x[0]))
        for tiers in self.tiers_by_length:
            tiers.sort()

    @property
    def replica_count(self):
        """Number of replicas (full or partial) used in the ring."""
        return calc_replica_count(self._replica2part2dev_id)

    @property
    def partition_count(self):
        """Number of partitions in the ring."""
        return len(self._replica2part2dev_id[0])

    @property
    def device_count(self):
        """Number of devices in the ring."""
        return self._num_devs

    @property
    def weighted_device_count(self):
        """Number of devices with weight in the ring."""
        return self._num_weighted_devs

    @property
    def assigned_device_count(self):
        """Number of devices with assignments in the ring."""
        return self._num_assigned_devs

    @property
    def devs(self):
        """devices in the ring"""
        if time() > self._rtime:
            self._reload()
        return self._devs

    def has_changed(self):
        """
        Check to see if the ring on disk is different than the current one in
        memory.

        :returns: True if the ring on disk has changed, False otherwise
        """
        return getmtime(self.serialized_path) != self._mtime

    def _get_part_nodes(self, part):
        part_nodes = []
        seen_ids = set()
        for r2p2d in self._replica2part2dev_id:
            if part < len(r2p2d):
                dev_id = r2p2d[part]
                if dev_id not in seen_ids:
                    part_nodes.append(self.devs[dev_id])
                    seen_ids.add(dev_id)
        return [dict(node, index=i) for i, node in enumerate(part_nodes)]

    def get_part(self, account, container=None, obj=None):
        """
        Get the partition for an account/container/object.

        :param account: account name
        :param container: container name
        :param obj: object name
        :returns: the partition number
        """
        key = hash_path(account, container, obj, raw_digest=True)
        if time() > self._rtime:
            self._reload()
        part = struct.unpack_from('>I', key)[0] >> self._part_shift
        return part

    def get_part_nodes(self, part):
        """
        Get the nodes that are responsible for the partition. If one
        node is responsible for more than one replica of the same
        partition, it will only appear in the output once.

        :param part: partition to get nodes for
        :returns: list of node dicts

        See :func:`get_nodes` for a description of the node dicts.
        """

        if time() > self._rtime:
            self._reload()
        return self._get_part_nodes(part)

    def get_nodes(self, account, container=None, obj=None):
        """
        Get the partition and nodes for an account/container/object.
        If a node is responsible for more than one replica, it will
        only appear in the output once.

        :param account: account name
        :param container: container name
        :param obj: object name
        :returns: a tuple of (partition, list of node dicts)

        Each node dict will have at least the following keys:

        ======  ===============================================================
        id      unique integer identifier amongst devices
        index   offset into the primary node list for the partition
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
        part = self.get_part(account, container, obj)
        return part, self._get_part_nodes(part)

    def get_more_nodes(self, part):
        """
        Generator to get extra nodes for a partition for hinted handoff.

        The handoff nodes will try to be in zones other than the
        primary zones, will take into account the device weights, and
        will usually keep the same sequences of handoffs even with
        ring changes.

        :param part: partition to get handoff nodes for
        :returns: generator of node dicts

        See :func:`get_nodes` for a description of the node dicts.
        """
        if time() > self._rtime:
            self._reload()
        primary_nodes = self._get_part_nodes(part)
        used = set(d['id'] for d in primary_nodes)
        index = count()
        same_regions = set(d['region'] for d in primary_nodes)
        same_zones = set((d['region'], d['zone']) for d in primary_nodes)
        same_ips = set(
            (d['region'], d['zone'], d['ip']) for d in primary_nodes)

        parts = len(self._replica2part2dev_id[0])
        part_hash = md5(str(part).encode('ascii'),
                        usedforsecurity=False).digest()
        start = struct.unpack_from('>I', part_hash)[0] >> self._part_shift
        inc = int(parts / 65536) or 1
        # Multiple loops for execution speed; the checks and bookkeeping get
        # simpler as you go along
        hit_all_regions = len(same_regions) == self._num_regions
        for handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc)):
            if hit_all_regions:
                # At this point, there are no regions left untouched, so we
                # can stop looking.
                break
            for part2dev_id in self._replica2part2dev_id:
                if handoff_part < len(part2dev_id):
                    dev_id = part2dev_id[handoff_part]
                    dev = self._devs[dev_id]
                    region = dev['region']
                    if dev_id not in used and region not in same_regions:
                        yield dict(dev, handoff_index=next(index))
                        used.add(dev_id)
                        same_regions.add(region)
                        zone = dev['zone']
                        ip = (region, zone, dev['ip'])
                        same_zones.add((region, zone))
                        same_ips.add(ip)
                        if len(same_regions) == self._num_regions:
                            hit_all_regions = True
                            break

        hit_all_zones = len(same_zones) == self._num_zones
        for handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc)):
            if hit_all_zones:
                # Much like we stopped looking for fresh regions before, we
                # can now stop looking for fresh zones; there are no more.
                break
            for part2dev_id in self._replica2part2dev_id:
                if handoff_part < len(part2dev_id):
                    dev_id = part2dev_id[handoff_part]
                    dev = self._devs[dev_id]
                    zone = (dev['region'], dev['zone'])
                    if dev_id not in used and zone not in same_zones:
                        yield dict(dev, handoff_index=next(index))
                        used.add(dev_id)
                        same_zones.add(zone)
                        ip = zone + (dev['ip'],)
                        same_ips.add(ip)
                        if len(same_zones) == self._num_zones:
                            hit_all_zones = True
                            break

        hit_all_ips = len(same_ips) == self._num_ips
        for handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc)):
            if hit_all_ips:
                # We've exhausted the pool of unused backends, so stop
                # looking.
                break
            for part2dev_id in self._replica2part2dev_id:
                if handoff_part < len(part2dev_id):
                    dev_id = part2dev_id[handoff_part]
                    dev = self._devs[dev_id]
                    ip = (dev['region'], dev['zone'], dev['ip'])
                    if dev_id not in used and ip not in same_ips:
                        yield dict(dev, handoff_index=next(index))
                        used.add(dev_id)
                        same_ips.add(ip)
                        if len(same_ips) == self._num_ips:
                            hit_all_ips = True
                            break

        hit_all_devs = len(used) == self._num_assigned_devs
        for handoff_part in chain(range(start, parts, inc),
                                  range(inc - ((parts - start) % inc),
                                        start, inc)):
            if hit_all_devs:
                # We've used every device we have, so let's stop looking for
                # unused devices now.
                break
            for part2dev_id in self._replica2part2dev_id:
                if handoff_part < len(part2dev_id):
                    dev_id = part2dev_id[handoff_part]
                    if dev_id not in used:
                        dev = self._devs[dev_id]
                        yield dict(dev, handoff_index=next(index))
                        used.add(dev_id)
                        if len(used) == self._num_assigned_devs:
                            hit_all_devs = True
                            break
