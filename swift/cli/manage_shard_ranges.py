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
from swift.container.sharder import make_shard_ranges, sharding_enabled, \
    CleavingContext


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


def _check_shard_ranges(own_shard_range, shard_ranges):
    reasons = []

    def reason(x, y):
        if x != y:
            reasons.append('%s != %s' % (x, y))

    if not shard_ranges:
        reasons.append('No shard ranges.')
    else:
        reason(own_shard_range.lower, shard_ranges[0].lower)
        reason(own_shard_range.upper, shard_ranges[-1].upper)
        for x, y in zip(shard_ranges, shard_ranges[1:]):
            reason(x.upper, y.lower)

    if reasons:
        print('WARNING: invalid shard ranges: %s.' % reasons)
        print('Aborting.')
        exit(2)


def _check_own_shard_range(broker, args):
    # TODO: this check is weak - if the shards prefix changes then we may not
    # identify a shard container. The goal is to not inadvertently create an
    # entire namespace default shard range for a shard container.
    is_shard = broker.account.startswith(args.shards_account_prefix)
    own_shard_range = broker.get_own_shard_range(no_default=is_shard)
    if not own_shard_range:
        print('WARNING: shard container missing own shard range.')
        print('Aborting.')
        exit(2)
    return own_shard_range


def _find_ranges(broker, args, status_file=None):
    start = last_report = time.time()
    limit = 5 if status_file else -1
    shard_data, last_found = broker.find_shard_ranges(
        args.rows_per_shard, limit=limit)
    if shard_data:
        while not last_found:
            if last_report + 10 < time.time():
                print('Found %d ranges in %gs; looking for more...' % (
                    len(shard_data), time.time() - start), file=status_file)
                last_report = time.time()
            # prefix doesn't matter since we aren't persisting it
            found_ranges = make_shard_ranges(broker, shard_data, '.shards_')
            more_shard_data, last_found = broker.find_shard_ranges(
                args.rows_per_shard, existing_ranges=found_ranges, limit=5)
            shard_data.extend(more_shard_data)
    return shard_data, time.time() - start


def find_ranges(broker, args):
    shard_data, delta_t = _find_ranges(broker, args, sys.stderr)
    print(json.dumps(shard_data, sort_keys=True, indent=2))
    print('Found %d ranges in %gs (total object count %s)' %
          (len(shard_data), delta_t,
           sum(r['object_count'] for r in shard_data)),
          file=sys.stderr)
    return 0


def show_shard_ranges(broker, args):
    shard_ranges = broker.get_shard_ranges(
        include_deleted=getattr(args, 'include_deleted', False))
    shard_data = [dict(sr, state=sr.state_text)
                  for sr in shard_ranges]

    if not shard_data:
        print("No shard data found.", file=sys.stderr)
    elif getattr(args, 'brief', False):
        print("Existing shard ranges:", file=sys.stderr)
        print(json.dumps([(sd['lower'], sd['upper']) for sd in shard_data],
                         sort_keys=True, indent=2))
    else:
        print("Existing shard ranges:", file=sys.stderr)
        print(json.dumps(shard_data, sort_keys=True, indent=2))
    return 0


def db_info(broker, args):
    print('Sharding enabled = %s' % sharding_enabled(broker))
    own_sr = broker.get_own_shard_range(no_default=True)
    print('Own shard range: %s' %
          (json.dumps(dict(own_sr, state=own_sr.state_text),
                      sort_keys=True, indent=2)
           if own_sr else None))
    db_state = broker.get_db_state()
    print('db_state = %s' % db_state)
    if db_state == 'sharding':
        print('Retiring db id: %s' % broker.get_brokers()[0].get_info()['id'])
        print('Cleaving context: %s' %
              json.dumps(dict(CleavingContext.load(broker)),
                         sort_keys=True, indent=2))
    print('Metadata:')
    for k, (v, t) in broker.metadata.items():
        print('  %s = %s' % (k, v))


def delete_shard_ranges(broker, args):
    shard_ranges = broker.get_shard_ranges()
    if not shard_ranges:
        print("No shard ranges found to delete.")
        return 0

    while not args.force:
        print('This will delete existing %d shard ranges.' % len(shard_ranges))
        if broker.get_db_state() != UNSHARDED:
            print('WARNING: Be very cautious about deleting existing shard '
                  'ranges. Deleting all ranges in this db does not guarantee '
                  'deletion of all ranges on all replicas of the db.')
            print('  - this db is in state %s' % broker.get_db_state())
            print('  - %d existing shard ranges have started sharding' %
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


def _replace_shard_ranges(broker, args, shard_data, timeout=None):
    own_shard_range = _check_own_shard_range(broker, args)
    shard_ranges = make_shard_ranges(
        broker, shard_data, args.shards_account_prefix)
    _check_shard_ranges(own_shard_range, shard_ranges)

    if args.verbose > 0:
        print('New shard ranges to be injected:')
        print(json.dumps([dict(sr) for sr in shard_ranges],
                         sort_keys=True, indent=2))

    # Crank up the timeout in an effort to *make sure* this succeeds
    with broker.updated_timeout(max(timeout, args.replace_timeout)):
        delete_shard_ranges(broker, args)
        broker.merge_shard_ranges(shard_ranges)

    print('Injected %d shard ranges.' % len(shard_ranges))
    print('Run container-replicator to replicate them to other nodes.')
    if args.enable:
        return enable_sharding(broker, args)
    else:
        print('Use the enable sub-command to enable sharding.')
        return 0


def replace_shard_ranges(broker, args):
    shard_data = _load_and_validate_shard_data(args)
    return _replace_shard_ranges(broker, args, shard_data)


def find_replace_shard_ranges(broker, args):
    shard_data, delta_t = _find_ranges(broker, args, sys.stdout)
    # Since we're trying to one-shot this, and the previous step probably
    # took a while, make the timeout for writing *at least* that long
    return _replace_shard_ranges(broker, args, shard_data, timeout=delta_t)


def _enable_sharding(broker, own_shard_range, args):
    if own_shard_range.update_state(ShardRange.SHARDING):
        own_shard_range.epoch = Timestamp.now()
        own_shard_range.state_timestamp = own_shard_range.epoch

    with broker.updated_timeout(args.enable_timeout):
        broker.merge_shard_ranges([own_shard_range])
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                ('True', Timestamp.now().normal)})
    return own_shard_range


def enable_sharding(broker, args):
    own_shard_range = _check_own_shard_range(broker, args)
    _check_shard_ranges(own_shard_range, broker.get_shard_ranges())

    if own_shard_range.state == ShardRange.ACTIVE:
        own_shard_range = _enable_sharding(broker, own_shard_range, args)
        print('Container moved to state %r with epoch %s.' %
              (own_shard_range.state_text, own_shard_range.epoch.internal))
    elif own_shard_range.state == ShardRange.SHARDING:
        if own_shard_range.epoch:
            print('Container already in state %r with epoch %s.' %
                  (own_shard_range.state_text, own_shard_range.epoch.internal))
            print('No action required.')
        else:
            print('Container already in state %r but missing epoch.' %
                  own_shard_range.state_text)
            own_shard_range = _enable_sharding(broker, own_shard_range, args)
            print('Container in state %r given epoch %s.' %
                  (own_shard_range.state_text, own_shard_range.epoch.internal))
    else:
        print('WARNING: container in state %s (should be active or sharding).'
              % own_shard_range.state_text)
        print('Aborting.')
        return 2

    print('Run container-sharder on all nodes to shard the container.')
    return 0


def _add_find_args(parser):
    parser.add_argument('rows_per_shard', nargs='?', type=int, default=500000)


def _add_replace_args(parser):
    parser.add_argument(
        '--shards_account_prefix', metavar='shards_account_prefix', type=str,
        required=False, help='Prefix for shards account', default='.shards_')
    parser.add_argument(
        '--replace-timeout', type=int, default=600,
        help='Minimum DB timeout to use when replacing shard ranges.')
    parser.add_argument(
        '--force', '-f', action='store_true', default=False,
        help='Delete existing shard ranges; no questions asked.')
    parser.add_argument(
        '--enable', action='store_true', default=False,
        help='Enable sharding after adding shard ranges.')


def _add_enable_args(parser):
    parser.add_argument(
        '--enable-timeout', type=int, default=300,
        help='DB timeout to use when enabling sharding.')


def _make_parser():
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
    show_parser.add_argument(
        '--brief', '-b', action='store_true', default=False,
        help='Show only shard range bounds in output.')
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
    _add_enable_args(find_replace_parser)
    find_replace_parser.set_defaults(func=find_replace_shard_ranges)

    # enable
    enable_parser = subparsers.add_parser(
        'enable', help='Enable sharding and move db to sharding state.')
    _add_enable_args(enable_parser)
    enable_parser.set_defaults(func=enable_sharding)
    _add_replace_args(enable_parser)
    return parser


def main(args=None):
    parser = _make_parser()
    args = parser.parse_args(args)
    logger = get_logger({}, name='ContainerBroker', log_to_console=True)
    broker = ContainerBroker(args.container_db, logger=logger,
                             skip_commits=True)
    broker.get_info()
    print('Loaded db broker for %s.' % broker.path, file=sys.stderr)
    return args.func(broker, args)


if __name__ == '__main__':
    exit(main())
