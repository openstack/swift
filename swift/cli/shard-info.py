# Copyright (c) 2017 OpenStack Foundation
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
from collections import defaultdict

from swift.common import utils
from swift.common.db_replicator import roundrobin_datadirs
from swift.common.ring import ring
from swift.common.utils import Timestamp
from swift.container.backend import ContainerBroker, DATADIR

TAB = '    '


def broker_key(broker):
    broker.get_info()
    return broker.path


def container_type(broker):
    return 'ROOT' if broker.is_root_container() else 'SHARD'


def collect_brokers(conf_path, names2nodes):
    conf = utils.readconf(conf_path, 'container-replicator')
    root = conf.get('devices', '/srv/node')
    swift_dir = conf.get('swift_dir', '/etc/swift')
    c_ring = ring.Ring(swift_dir, ring_name='container')
    dirs = []
    brokers = defaultdict(dict)
    for node in c_ring.devs:
        if node is None:
            continue
        datadir = os.path.join(root, node['device'], DATADIR)
        if os.path.isdir(datadir):
            dirs.append((datadir, node['id'], lambda *args: True))
    for part, object_file, node_id in roundrobin_datadirs(dirs):
        broker = ContainerBroker(object_file)
        for node in c_ring.get_part_nodes(int(part)):
            if node['id'] == node_id:
                node_index = str(node['index'])
                break
        else:
            node_index = 'handoff'
        names2nodes[broker_key(broker)][(node_id, node_index)] = broker
    return brokers


def print_broker_info(node, broker, indent_level=0):
    indent = indent_level * TAB
    info = broker.get_info()
    raw_info = broker._get_info()
    deleted_at = float(info['delete_timestamp'])
    if deleted_at:
        deleted_at = Timestamp(info['delete_timestamp']).isoformat
    else:
        deleted_at = ' - '
    print('%s(%s) %s, objs: %s, bytes: %s, actual_objs: %s, put: %s, '
          'deleted: %s' %
          (indent, node[1][0], broker.get_db_state(),
           info['object_count'], info['bytes_used'], raw_info['object_count'],
           Timestamp(info['put_timestamp']).isoformat, deleted_at))


def print_db(node, broker, expect_type='ROOT', indent_level=0):
    indent = indent_level * TAB
    print('%s(%s) %s node id: %s, node index: %s' %
          (indent, node[1][0], broker.db_file, node[0], node[1]))
    actual_type = container_type(broker)
    if actual_type != expect_type:
        print('%s        ERROR expected %s but found %s' %
              (indent, expect_type, actual_type))


def print_own_shard_range(node, sr, indent_level):
    indent = indent_level * TAB
    range = '%r - %r' % (sr.lower, sr.upper)
    print('%s(%s) %23s, objs: %3s, bytes: %3s, timestamp: %s (%s), '
          'modified: %s (%s), %7s: %s (%s), deleted: %s epoch: %s' %
          (indent, node[1][0], range, sr.object_count, sr.bytes_used,
           sr.timestamp.isoformat, sr.timestamp.internal,
           sr.meta_timestamp.isoformat, sr.meta_timestamp.internal,
           sr.state_text, sr.state_timestamp.isoformat,
           sr.state_timestamp.internal, sr.deleted,
           sr.epoch.internal if sr.epoch else None))


def print_own_shard_range_info(node, shard_ranges, indent_level=0):
    shard_ranges.sort(key=lambda x: x.deleted)
    for sr in shard_ranges:
        print_own_shard_range(node, sr, indent_level)


def print_shard_range(node, sr, indent_level):
    indent = indent_level * TAB
    range = '%r - %r' % (sr.lower, sr.upper)
    print('%s(%s) %23s, objs: %3s, bytes: %3s, timestamp: %s (%s), '
          'modified: %s (%s), %7s: %s (%s), deleted: %s %s' %
          (indent, node[1][0], range, sr.object_count, sr.bytes_used,
           sr.timestamp.isoformat, sr.timestamp.internal,
           sr.meta_timestamp.isoformat, sr.meta_timestamp.internal,
           sr.state_text, sr.state_timestamp.isoformat,
           sr.state_timestamp.internal, sr.deleted, sr.name))


def print_shard_range_info(node, shard_ranges, indent_level=0):
    shard_ranges.sort(key=lambda x: x.deleted)
    for sr in shard_ranges:
        print_shard_range(node, sr, indent_level)


def print_sharding_info(node, broker, indent_level=0):
    indent = indent_level * TAB
    print('%s(%s) %s' % (indent, node[1][0], broker.get_sharding_sysmeta()))


def print_container(name, name2nodes2brokers, expect_type='ROOT',
                    indent_level=0, used_names=None):
    used_names = used_names or set()
    indent = indent_level * TAB
    node2broker = name2nodes2brokers[name]
    ordered_by_index = sorted(node2broker.keys(), key=lambda x: x[1])
    brokers = [(node, node2broker[node]) for node in ordered_by_index]

    print('%sName: %s' % (indent, name))
    if name in used_names:
        print('%s  (Details already listed)\n' % indent)
        return

    used_names.add(name)
    print(indent + 'DB files:')
    for node, broker in brokers:
        print_db(node, broker, expect_type, indent_level=indent_level + 1)

    print(indent + 'Info:')
    for node, broker in brokers:
        print_broker_info(node, broker, indent_level=indent_level + 1)

    print(indent + 'Sharding info:')
    for node, broker in brokers:
        print_sharding_info(node, broker, indent_level=indent_level + 1)
    print(indent + 'Own shard range:')
    for node, broker in brokers:
        shard_ranges = broker.get_shard_ranges(
            include_deleted=True, include_own=True, exclude_others=True)
        print_own_shard_range_info(node, shard_ranges,
                                   indent_level=indent_level + 1)
    print(indent + 'Shard ranges:')
    shard_names = set()
    for node, broker in brokers:
        shard_ranges = broker.get_shard_ranges(include_deleted=True)
        for sr_name in shard_ranges:
            shard_names.add(sr_name.name)
        print_shard_range_info(node, shard_ranges,
                               indent_level=indent_level + 1)
    print(indent + 'Shards:')
    for sr_name in shard_names:
        print_container(sr_name, name2nodes2brokers, expect_type='SHARD',
                        indent_level=indent_level + 1, used_names=used_names)
    print('\n')


def run(conf_paths):
    # container_name -> (node id, node index) -> broker
    name2nodes2brokers = defaultdict(dict)
    for conf_path in conf_paths:
        collect_brokers(conf_path, name2nodes2brokers)

    print('First column on each line is (node index)\n')
    for name, node2broker in name2nodes2brokers.items():
        expect_root = False
        for node, broker in node2broker.items():
            expect_root = broker.is_root_container() or expect_root
        if expect_root:
            print_container(name, name2nodes2brokers)


if __name__ == '__main__':
    conf_dir = '/etc/swift/container-server'
    conf_paths = [os.path.join(conf_dir, p) for p in os.listdir(conf_dir)
                  if p.endswith(('conf', 'conf.d'))]
    run(conf_paths)
