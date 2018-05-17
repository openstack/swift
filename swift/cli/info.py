# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import print_function
import itertools
import os
import sqlite3
from hashlib import md5

from six.moves import urllib

from swift.common.utils import hash_path, storage_directory, \
    Timestamp, is_valid_ipv6
from swift.common.ring import Ring
from swift.common.request_helpers import is_sys_meta, is_user_meta, \
    strip_sys_meta_prefix, strip_user_meta_prefix, \
    is_object_transient_sysmeta, strip_object_transient_sysmeta_prefix
from swift.account.backend import AccountBroker, DATADIR as ABDATADIR
from swift.container.backend import ContainerBroker, DATADIR as CBDATADIR
from swift.obj.diskfile import get_data_dir, read_metadata, DATADIR_BASE, \
    extract_policy
from swift.common.storage_policy import POLICIES


class InfoSystemExit(Exception):
    """
    Indicates to the caller that a sys.exit(1) should be performed.
    """
    pass


def parse_get_node_args(options, args):
    """
    Parse the get_nodes commandline args

    :returns: a tuple, (ring_path, args)
    """
    ring_path = None

    if options.policy_name:
        if POLICIES.get_by_name(options.policy_name) is None:
            raise InfoSystemExit('No policy named %r' % options.policy_name)
    elif args and args[0].endswith('.ring.gz'):
        if os.path.exists(args[0]):
            ring_path = args.pop(0)
        else:
            raise InfoSystemExit('Ring file does not exist')

    if len(args) == 1:
        args = args[0].strip('/').split('/', 2)

    if not ring_path and not options.policy_name:
        raise InfoSystemExit('Need to specify policy_name or <ring.gz>')

    if not (args or options.partition):
        raise InfoSystemExit('No target specified')

    if len(args) > 3:
        raise InfoSystemExit('Invalid arguments')

    return ring_path, args


def curl_head_command(ip, port, device, part, target, policy_index):
    """
    Provide a string that is a well formatted curl command to HEAD an object
    on a storage node.

    :param ip: the ip of the node
    :param port: the port of the node
    :param device: the device of the node
    :param target: the path of the target resource
    :param policy_index: the policy_index of the target resource (can be None)

    :returns: a string, a well formatted curl command
    """
    if is_valid_ipv6(ip):
        formatted_ip = '[%s]' % ip
    else:
        formatted_ip = ip

    cmd = 'curl -g -I -XHEAD "http://%s:%s/%s/%s/%s"' % (
        formatted_ip, port, device, part, urllib.parse.quote(target))
    if policy_index is not None:
        cmd += ' -H "%s: %s"' % ('X-Backend-Storage-Policy-Index',
                                 policy_index)
    return cmd


def print_ring_locations(ring, datadir, account, container=None, obj=None,
                         tpart=None, all_nodes=False, policy_index=None):
    """
    print out ring locations of specified type

    :param ring: ring instance
    :param datadir: name of directory where things are stored. Usually one of
                    "accounts", "containers", "objects", or "objects-N".
    :param account: account name
    :param container: container name
    :param obj: object name
    :param tpart: target partition in ring
    :param all_nodes: include all handoff nodes. If false, only the N primary
                      nodes and first N handoffs will be printed.
    :param policy_index: include policy_index in curl headers
    """
    if not ring:
        raise ValueError("No ring specified")
    if not datadir:
        raise ValueError("No datadir specified")
    if tpart is None and not account:
        raise ValueError("No partition or account/container/object specified")
    if not account and (container or obj):
        raise ValueError("Container/object specified without account")
    if obj and not container:
        raise ValueError('Object specified without container')

    if obj:
        target = '%s/%s/%s' % (account, container, obj)
    elif container:
        target = '%s/%s' % (account, container)
    else:
        target = '%s' % (account)

    if tpart:
        part = int(tpart)
    else:
        part = ring.get_part(account, container, obj)

    primary_nodes = ring.get_part_nodes(part)
    handoff_nodes = ring.get_more_nodes(part)
    if not all_nodes:
        handoff_nodes = itertools.islice(handoff_nodes, len(primary_nodes))
    handoff_nodes = list(handoff_nodes)

    if account and not tpart:
        path_hash = hash_path(account, container, obj)
    else:
        path_hash = None
    print('Partition\t%s' % part)
    print('Hash     \t%s\n' % path_hash)

    for node in primary_nodes:
        print('Server:Port Device\t%s:%s %s' % (node['ip'], node['port'],
                                                node['device']))
    for node in handoff_nodes:
        print('Server:Port Device\t%s:%s %s\t [Handoff]' % (
            node['ip'], node['port'], node['device']))

    print("\n")

    for node in primary_nodes:
        cmd = curl_head_command(node['ip'], node['port'], node['device'],
                                part, target, policy_index)
        print(cmd)
    for node in handoff_nodes:
        cmd = curl_head_command(node['ip'], node['port'], node['device'],
                                part, target, policy_index)
        cmd += ' # [Handoff]'
        print(cmd)

    print("\n\nUse your own device location of servers:")
    print("such as \"export DEVICE=/srv/node\"")
    if path_hash:
        for node in primary_nodes:
            print('ssh %s "ls -lah ${DEVICE:-/srv/node*}/%s/%s"' %
                  (node['ip'], node['device'],
                   storage_directory(datadir, part, path_hash)))
        for node in handoff_nodes:
            print('ssh %s "ls -lah ${DEVICE:-/srv/node*}/%s/%s" # [Handoff]' %
                  (node['ip'], node['device'],
                   storage_directory(datadir, part, path_hash)))
    else:
        for node in primary_nodes:
            print('ssh %s "ls -lah ${DEVICE:-/srv/node*}/%s/%s/%d"' %
                  (node['ip'], node['device'], datadir, part))
        for node in handoff_nodes:
            print('ssh %s "ls -lah ${DEVICE:-/srv/node*}/%s/%s/%d"'
                  ' # [Handoff]' %
                  (node['ip'], node['device'], datadir, part))

    print('\nnote: `/srv/node*` is used as default value of `devices`, the '
          'real value is set in the config file on each storage node.')


def print_db_info_metadata(db_type, info, metadata, drop_prefixes=False):
    """
    print out data base info/metadata based on its type

    :param db_type: database type, account or container
    :param info: dict of data base info
    :param metadata: dict of data base metadata
    :param drop_prefixes: if True, strip "X-Account-Meta-",
                          "X-Container-Meta-", "X-Account-Sysmeta-", and
                          "X-Container-Sysmeta-" when displaying
                          User Metadata and System Metadata dicts
    """
    if info is None:
        raise ValueError('DB info is None')

    if db_type not in ['container', 'account']:
        raise ValueError('Wrong DB type')

    try:
        account = info['account']
        container = None

        if db_type == 'container':
            container = info['container']
            path = '/%s/%s' % (account, container)
        else:
            path = '/%s' % account

        print('Path: %s' % path)
        print('  Account: %s' % account)

        if db_type == 'container':
            print('  Container: %s' % container)

        path_hash = hash_path(account, container)
        if db_type == 'container':
            print('  Container Hash: %s' % path_hash)
        else:
            print('  Account Hash: %s' % path_hash)

        print('Metadata:')
        print('  Created at: %s (%s)' %
              (Timestamp(info['created_at']).isoformat,
               info['created_at']))
        print('  Put Timestamp: %s (%s)' %
              (Timestamp(info['put_timestamp']).isoformat,
               info['put_timestamp']))
        print('  Delete Timestamp: %s (%s)' %
              (Timestamp(info['delete_timestamp']).isoformat,
               info['delete_timestamp']))
        print('  Status Timestamp: %s (%s)' %
              (Timestamp(info['status_changed_at']).isoformat,
               info['status_changed_at']))
        if db_type == 'account':
            print('  Container Count: %s' % info['container_count'])
        print('  Object Count: %s' % info['object_count'])
        print('  Bytes Used: %s' % info['bytes_used'])
        if db_type == 'container':
            try:
                policy_name = POLICIES[info['storage_policy_index']].name
            except KeyError:
                policy_name = 'Unknown'
            print('  Storage Policy: %s (%s)' % (
                policy_name, info['storage_policy_index']))
            print('  Reported Put Timestamp: %s (%s)' %
                  (Timestamp(info['reported_put_timestamp']).isoformat,
                   info['reported_put_timestamp']))
            print('  Reported Delete Timestamp: %s (%s)' %
                  (Timestamp(info['reported_delete_timestamp']).isoformat,
                   info['reported_delete_timestamp']))
            print('  Reported Object Count: %s' %
                  info['reported_object_count'])
            print('  Reported Bytes Used: %s' % info['reported_bytes_used'])
        print('  Chexor: %s' % info['hash'])
        print('  UUID: %s' % info['id'])
    except KeyError as e:
        raise ValueError('Info is incomplete: %s' % e)

    meta_prefix = 'x_' + db_type + '_'
    for key, value in info.items():
        if key.lower().startswith(meta_prefix):
            title = key.replace('_', '-').title()
            print('  %s: %s' % (title, value))
    user_metadata = {}
    sys_metadata = {}
    for key, (value, timestamp) in metadata.items():
        if is_user_meta(db_type, key):
            if drop_prefixes:
                key = strip_user_meta_prefix(db_type, key)
            user_metadata[key] = value
        elif is_sys_meta(db_type, key):
            if drop_prefixes:
                key = strip_sys_meta_prefix(db_type, key)
            sys_metadata[key] = value
        else:
            title = key.replace('_', '-').title()
            print('  %s: %s' % (title, value))
    if sys_metadata:
        print('  System Metadata: %s' % sys_metadata)
    else:
        print('No system metadata found in db file')

    if user_metadata:
        print('  User Metadata: %s' % user_metadata)
    else:
        print('No user metadata found in db file')


def print_obj_metadata(metadata, drop_prefixes=False):
    """
    Print out basic info and metadata from object, as returned from
    :func:`swift.obj.diskfile.read_metadata`.

    Metadata should include the keys: name, Content-Type, and
    X-Timestamp.

    Additional metadata is displayed unmodified.

    :param metadata: dict of object metadata
    :param drop_prefixes: if True, strip "X-Object-Meta-", "X-Object-Sysmeta-",
                          and "X-Object-Transient-Sysmeta-" when displaying
                          User Metadata, System Metadata, and Transient
                          System Metadata entries

    :raises ValueError:
    """
    user_metadata = {}
    sys_metadata = {}
    transient_sys_metadata = {}
    other_metadata = {}

    if not metadata:
        raise ValueError('Metadata is None')
    path = metadata.pop('name', '')
    content_type = metadata.pop('Content-Type', '')
    ts = Timestamp(metadata.pop('X-Timestamp', 0))
    account = container = obj = obj_hash = None
    if path:
        try:
            account, container, obj = path.split('/', 3)[1:]
        except ValueError:
            raise ValueError('Path is invalid for object %r' % path)
        else:
            obj_hash = hash_path(account, container, obj)
        print('Path: %s' % path)
        print('  Account: %s' % account)
        print('  Container: %s' % container)
        print('  Object: %s' % obj)
        print('  Object hash: %s' % obj_hash)
    else:
        print('Path: Not found in metadata')
    if content_type:
        print('Content-Type: %s' % content_type)
    else:
        print('Content-Type: Not found in metadata')
    if ts:
        print('Timestamp: %s (%s)' % (ts.isoformat, ts.internal))
    else:
        print('Timestamp: Not found in metadata')

    for key, value in metadata.items():
        if is_user_meta('Object', key):
            if drop_prefixes:
                key = strip_user_meta_prefix('Object', key)
            user_metadata[key] = value
        elif is_sys_meta('Object', key):
            if drop_prefixes:
                key = strip_sys_meta_prefix('Object', key)
            sys_metadata[key] = value
        elif is_object_transient_sysmeta(key):
            if drop_prefixes:
                key = strip_object_transient_sysmeta_prefix(key)
            transient_sys_metadata[key] = value
        else:
            other_metadata[key] = value

    def print_metadata(title, items):
        print(title)
        if items:
            for key, value in sorted(items.items()):
                print('  %s: %s' % (key, value))
        else:
            print('  No metadata found')

    print_metadata('System Metadata:', sys_metadata)
    print_metadata('Transient System Metadata:', transient_sys_metadata)
    print_metadata('User Metadata:', user_metadata)
    print_metadata('Other Metadata:', other_metadata)


def print_info(db_type, db_file, swift_dir='/etc/swift', stale_reads_ok=False,
               drop_prefixes=False):
    if db_type not in ('account', 'container'):
        print("Unrecognized DB type: internal error")
        raise InfoSystemExit()
    if not os.path.exists(db_file) or not db_file.endswith('.db'):
        print("DB file doesn't exist")
        raise InfoSystemExit()
    if not db_file.startswith(('/', './')):
        db_file = './' + db_file  # don't break if the bare db file is given
    if db_type == 'account':
        broker = AccountBroker(db_file, stale_reads_ok=stale_reads_ok)
        datadir = ABDATADIR
    else:
        broker = ContainerBroker(db_file, stale_reads_ok=stale_reads_ok)
        datadir = CBDATADIR
    try:
        info = broker.get_info()
    except sqlite3.OperationalError as err:
        if 'no such table' in str(err):
            print("Does not appear to be a DB of type \"%s\": %s"
                  % (db_type, db_file))
            raise InfoSystemExit()
        raise
    account = info['account']
    container = info['container'] if db_type == 'container' else None
    print_db_info_metadata(db_type, info, broker.metadata, drop_prefixes)
    try:
        ring = Ring(swift_dir, ring_name=db_type)
    except Exception:
        ring = None
    else:
        print_ring_locations(ring, datadir, account, container)


def print_obj(datafile, check_etag=True, swift_dir='/etc/swift',
              policy_name='', drop_prefixes=False):
    """
    Display information about an object read from the datafile.
    Optionally verify the datafile content matches the ETag metadata.

    :param datafile: path on disk to object file
    :param check_etag: boolean, will read datafile content and verify
                       computed checksum matches value stored in
                       metadata.
    :param swift_dir: the path on disk to rings
    :param policy_name: optionally the name to use when finding the ring
    :param drop_prefixes: if True, strip "X-Object-Meta-", "X-Object-Sysmeta-",
                          and "X-Object-Transient-Sysmeta-" when displaying
                          User Metadata, System Metadata, and Transient
                          System Metadata entries
    """
    if not os.path.exists(datafile):
        print("Data file doesn't exist")
        raise InfoSystemExit()
    if not datafile.startswith(('/', './')):
        datafile = './' + datafile

    policy_index = None
    ring = None
    datadir = DATADIR_BASE

    # try to extract policy index from datafile disk path
    fullpath = os.path.abspath(datafile)
    policy_index = int(extract_policy(fullpath) or POLICIES.legacy)

    try:
        if policy_index:
            datadir += '-' + str(policy_index)
            ring = Ring(swift_dir, ring_name='object-' + str(policy_index))
        elif policy_index == 0:
            ring = Ring(swift_dir, ring_name='object')
    except IOError:
        # no such ring
        pass

    if policy_name:
        policy = POLICIES.get_by_name(policy_name)
        if policy:
            policy_index_for_name = policy.idx
            if (policy_index is not None and
               policy_index_for_name is not None and
               policy_index != policy_index_for_name):
                print('Warning: Ring does not match policy!')
                print('Double check your policy name!')
            if not ring and policy_index_for_name:
                ring = POLICIES.get_object_ring(policy_index_for_name,
                                                swift_dir)
                datadir = get_data_dir(policy_index_for_name)

    with open(datafile, 'rb') as fp:
        try:
            metadata = read_metadata(fp)
        except EOFError:
            print("Invalid metadata")
            raise InfoSystemExit()

        etag = metadata.pop('ETag', '')
        length = metadata.pop('Content-Length', '')
        path = metadata.get('name', '')
        print_obj_metadata(metadata, drop_prefixes)

        # Optional integrity check; it's useful, but slow.
        file_len = None
        if check_etag:
            h = md5()
            file_len = 0
            while True:
                data = fp.read(64 * 1024)
                if not data:
                    break
                h.update(data)
                file_len += len(data)
            h = h.hexdigest()
            if etag:
                if h == etag:
                    print('ETag: %s (valid)' % etag)
                else:
                    print("ETag: %s doesn't match file hash of %s!" %
                          (etag, h))
            else:
                print('ETag: Not found in metadata')
        else:
            print('ETag: %s (not checked)' % etag)
            file_len = os.fstat(fp.fileno()).st_size

        if length:
            if file_len == int(length):
                print('Content-Length: %s (valid)' % length)
            else:
                print("Content-Length: %s doesn't match file length of %s"
                      % (length, file_len))
        else:
            print('Content-Length: Not found in metadata')

        account, container, obj = path.split('/', 3)[1:]
        if ring:
            print_ring_locations(ring, datadir, account, container, obj,
                                 policy_index=policy_index)


def print_item_locations(ring, ring_name=None, account=None, container=None,
                         obj=None, **kwargs):
    """
    Display placement information for an item based on ring lookup.

    If a ring is provided it always takes precedence, but warnings will be
    emitted if it doesn't match other optional arguments like the policy_name
    or ring_name.

    If no ring is provided the ring_name and/or policy_name will be used to
    lookup the ring.

    :param ring: a ring instance
    :param ring_name: server type, or storage policy ring name if object ring
    :param account: account name
    :param container: container name
    :param obj: object name
    :param partition: part number for non path lookups
    :param policy_name: name of storage policy to use to lookup the ring
    :param all_nodes: include all handoff nodes. If false, only the N primary
                      nodes and first N handoffs will be printed.
    """

    policy_name = kwargs.get('policy_name', None)
    part = kwargs.get('partition', None)
    all_nodes = kwargs.get('all', False)
    swift_dir = kwargs.get('swift_dir', '/etc/swift')

    if ring and policy_name:
        policy = POLICIES.get_by_name(policy_name)
        if policy:
            if ring_name != policy.ring_name:
                print('Warning: mismatch between ring and policy name!')
        else:
            print('Warning: Policy %s is not valid' % policy_name)

    policy_index = None
    if ring is None and (obj or part):
        if not policy_name:
            print('Need a ring or policy')
            raise InfoSystemExit()
        policy = POLICIES.get_by_name(policy_name)
        if not policy:
            print('No policy named %r' % policy_name)
            raise InfoSystemExit()
        policy_index = int(policy)
        ring = POLICIES.get_object_ring(policy_index, swift_dir)
        ring_name = (POLICIES.get_by_name(policy_name)).ring_name

    if account is None and (container is not None or obj is not None):
        print('No account specified')
        raise InfoSystemExit()

    if container is None and obj is not None:
        print('No container specified')
        raise InfoSystemExit()

    if account is None and part is None:
        print('No target specified')
        raise InfoSystemExit()

    loc = '<type>'
    if part and ring_name:
        if '-' in ring_name and ring_name.startswith('object'):
            loc = 'objects-' + ring_name.split('-', 1)[1]
        else:
            loc = ring_name + 's'
    if account and container and obj:
        loc = 'objects'
        if '-' in ring_name and ring_name.startswith('object'):
            policy_index = int(ring_name.rsplit('-', 1)[1])
            loc = 'objects-%d' % policy_index
    if account and container and not obj:
        loc = 'containers'
        if not any([ring, ring_name]):
            ring = Ring(swift_dir, ring_name='container')
        else:
            if ring_name != 'container':
                print('Warning: account/container specified ' +
                      'but ring not named "container"')
    if account and not container and not obj:
        loc = 'accounts'
        if not any([ring, ring_name]):
            ring = Ring(swift_dir, ring_name='account')
        else:
            if ring_name != 'account':
                print('Warning: account specified ' +
                      'but ring not named "account"')

    print('\nAccount  \t%s' % account)
    print('Container\t%s' % container)
    print('Object   \t%s\n\n' % obj)
    print_ring_locations(ring, loc, account, container, obj, part, all_nodes,
                         policy_index=policy_index)
