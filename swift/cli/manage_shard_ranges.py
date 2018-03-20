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
import argparse
import json
import sys
import time

from six.moves import input

from swift.common.utils import Timestamp, get_logger, ShardRange
from swift.container.backend import ContainerBroker, UNSHARDED
from swift.container.sharder import make_shard_ranges, sharding_enabled


def _load_and_validate_shard_data(args):
    try:
        with open(args.input, 'rb') as fd:
            try:
                data = json.load(fd)
                if not isinstance(data, list):
                    raise ValueError('Shard data must be a list of dicts')
                for k in ('lower', 'upper', 'index', 'object_count'):
                    for shard in data:
                        shard[k]
                return data
            except (TypeError, ValueError, KeyError) as err:
                print('Failed to load valid shard range data: %r' % err,
                      file=sys.stderr)
                exit(2)
    except IOError as err:
        print('Failed to open file %s: %s' % (args.input, err),
              file=sys.stderr)
        exit(2)


def find_ranges(broker, args):
    start = time.time()
    ranges = broker.find_shard_ranges(args.rows_per_shard)[0]
    delta_t = time.time() - start

    print(json.dumps([dict(r) for r in ranges], sort_keys=True, indent=2))
    print('Found %d ranges in %gs' % (len(ranges), delta_t), file=sys.stderr)
    return 0


def show_shard_ranges(broker, args):
    shard_ranges = broker.get_shard_ranges(
        include_deleted=getattr(args, 'include_deleted', False))
    shard_data = [dict(sr, state=sr.state_text)
                  for sr in shard_ranges]

    if not shard_data:
        print("No shard data found.", file=sys.stderr)
    else:
        print("Existing shard ranges:", file=sys.stderr)
        print(json.dumps(shard_data, sort_keys=True, indent=2))
    return 0


def db_info(broker, args):
    print('Sharding enabled = %s' % sharding_enabled(broker))
    print('db_state = %s' % broker.get_db_state_text())
    print('Metadata:')
    for k, (v, t) in broker.metadata.items():
        print('  %s = %s' % (k, v))


def delete_shard_ranges(broker, args):
    shard_ranges = broker.get_shard_ranges()
    if not shard_ranges:
        print("No shard ranges found to delete.")
        return 0

    while True and not args.force:
        print('This will delete existing %d shard ranges.' % len(shard_ranges))
        if broker.get_db_state() != UNSHARDED:
            print('WARNING: Be very cautious about deleting existing shard '
                  'ranges because:')
            print('  the db is in state %s' % broker.get_db_state_text())
            print('  %d existing shard ranges have started sharding' %
                  [sr.state != ShardRange.FOUND
                   for sr in shard_ranges].count(True))
        choice = input('Do you want to show the existing ranges [s], '
                       'delete the existing ranges [yes] '
                       'or quit without deleting [q]? ')
        if choice == 's':
            show_shard_ranges(broker, args)
            continue
        elif choice == 'q':
            return 1
        elif choice == 'yes':
            break
        else:
            print('Please make a valid choice.')
            print()

    now = Timestamp.now()
    for sr in shard_ranges:
        sr.deleted = 1
        sr.timestamp = now
    broker.merge_shard_ranges(shard_ranges)
    print('Deleted %s existing shard ranges.' % len(shard_ranges))
    return 0


def _replace_shard_ranges(broker, args, shard_data):
    shard_ranges = make_shard_ranges(
        broker, shard_data, args.shards_account_prefix)
    if args.verbose > 0:
        print('New shard ranges to be injected:')
        print(json.dumps([dict(sr) for sr in shard_ranges],
                         sort_keys=True, indent=2))

    delete_shard_ranges(broker, args)

    broker.update_sharding_info({'Scan-Done': 'True'})
    broker.merge_shard_ranges(shard_ranges)
    print('Injected %d shard ranges.' % len(shard_ranges))
    print('Run container-replicator to replicate them to other nodes.')
    print('Use the enable sub-command to enable sharding.')
    return 0


def replace_shard_ranges(broker, args):
    shard_data = _load_and_validate_shard_data(args)
    _replace_shard_ranges(broker, args, shard_data)


def find_replace_shard_ranges(broker, args):
    shard_data = broker.find_shard_ranges(args.rows_per_shard)[0]
    _replace_shard_ranges(broker, args, shard_data)


def enable_sharding(broker, args):
    broker.update_metadata({'X-Container-Sysmeta-Sharding':
                            ('True', Timestamp.now().normal)})
    print('Sharding enabled.')
    epoch = Timestamp.now()
    if not broker.set_sharding_state(epoch=epoch):
        print ('WARNING: failed to move broker to sharding state.')
        return 2
    print('Enabled sharding with epoch %s.' % epoch.internal)
    print('Run container-sharder to shard the container.')
    return 0


def _add_find_args(parser):
    parser.add_argument('rows_per_shard', nargs='?', type=int, default=500000)


def _add_replace_args(parser):
    parser.add_argument(
        '--shards_account_prefix', metavar='shards_account_prefix', type=str,
        required=False, help='Prefix for shards account', default='.shards_')
    parser.add_argument(
        '--force', '-f', action='store_true', default=False,
        help='Delete existing shard ranges; no questions asked.')


def main(args=None):
    parser = argparse.ArgumentParser(description='Manage shard ranges')
    parser.add_argument('container_db')
    parser.add_argument('--verbose', '-v', action='count',
                        help='Increase output verbosity')
    subparsers = parser.add_subparsers(
        help='Sub-command help', title='Sub-commands')

    # find
    find_parser = subparsers.add_parser(
        'find', help='Find and display shard ranges')
    _add_find_args(find_parser)
    find_parser.set_defaults(func=find_ranges)

    # delete
    delete_parser = subparsers.add_parser(
        'delete', help='Delete all existing shard ranges from db')
    delete_parser.add_argument(
        '--force', '-f', action='store_true', default=False,
        help='Delete existing shard ranges; no questions asked.')
    delete_parser.set_defaults(func=delete_shard_ranges)

    # show
    show_parser = subparsers.add_parser(
        'show', help='Print shard range data')
    show_parser.add_argument(
        '--include_deleted', '-d', action='store_true', default=False,
        help='Include deleted shard ranges in output.')
    show_parser.set_defaults(func=show_shard_ranges)

    # info
    info_parser = subparsers.add_parser(
        'info', help='Print container db info')
    info_parser.set_defaults(func=db_info)

    # replace
    replace_parser = subparsers.add_parser(
        'replace',
        help='Replace existing shard ranges. User will be prompted before '
             'deleting any existing shard ranges.')
    replace_parser.add_argument('input', metavar='input_file',
                                type=str, help='Name of file')
    _add_replace_args(replace_parser)
    replace_parser.set_defaults(func=replace_shard_ranges)

    # find_and_replace
    find_replace_parser = subparsers.add_parser(
        'find_and_replace',
        help='Find new shard ranges and replace existing shard ranges. '
             'User will be prompted before deleting any existing shard ranges.'
    )
    _add_find_args(find_replace_parser)
    _add_replace_args(find_replace_parser)
    find_replace_parser.set_defaults(func=find_replace_shard_ranges)

    # enable
    info_parser = subparsers.add_parser(
        'enable', help='Enable sharding and move db to sharding state.')
    info_parser.set_defaults(func=enable_sharding)

    args = parser.parse_args(args)
    logger = get_logger({}, name='ContainerBroker', log_to_console=True)
    broker = ContainerBroker(args.container_db, logger=logger)
    broker.get_info()
    print('Loaded db broker for %s.' % broker.path, file=sys.stderr)
    return args.func(broker, args)


if __name__ == '__main__':
    exit(main())
