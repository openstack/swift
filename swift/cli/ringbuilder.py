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

import logging

from collections import defaultdict
from errno import EEXIST
from itertools import islice
from operator import itemgetter
from os import mkdir
from os.path import basename, abspath, dirname, exists, join as pathjoin
import sys
from sys import argv as sys_argv, exit, stderr, stdout
from textwrap import wrap
from time import time
import traceback
from datetime import timedelta
import optparse
import math

from swift.common import exceptions
from swift.common.ring import RingBuilder, Ring, RingData
from swift.common.ring.builder import MAX_BALANCE
from swift.common.ring.composite_builder import CompositeRingBuilder
from swift.common.ring.utils import validate_args, \
    validate_and_normalize_ip, build_dev_from_opts, \
    parse_builder_ring_filename_args, parse_search_value, \
    parse_search_values_from_opts, parse_change_values_from_opts, \
    dispersion_report, parse_add_value
from swift.common.utils import lock_parent_directory, is_valid_ipv6

MAJOR_VERSION = 1
MINOR_VERSION = 3
EXIT_SUCCESS = 0
EXIT_WARNING = 1
EXIT_ERROR = 2

global argv, backup_dir, builder, builder_file, ring_file
argv = backup_dir = builder = builder_file = ring_file = None


def format_device(dev):
    """
    Format a device for display.
    """
    copy_dev = dev.copy()
    for key in ('ip', 'replication_ip'):
        if ':' in copy_dev[key]:
            copy_dev[key] = '[' + copy_dev[key] + ']'
    return ('d%(id)sr%(region)sz%(zone)s-%(ip)s:%(port)sR'
            '%(replication_ip)s:%(replication_port)s/%(device)s_'
            '"%(meta)s"' % copy_dev)


def _parse_search_values(argvish):

    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    try:
        search_values = {}
        if len(args) > 0:
            if new_cmd_format or len(args) != 1:
                print(Commands.search.__doc__.strip())
                exit(EXIT_ERROR)
            search_values = parse_search_value(args[0])
        else:
            search_values = parse_search_values_from_opts(opts)
        return search_values
    except ValueError as e:
        print(e)
        exit(EXIT_ERROR)


def _find_parts(devs):
    devs = [d['id'] for d in devs]
    if not devs or not builder._replica2part2dev:
        return None

    partition_count = {}
    for replica in builder._replica2part2dev:
        for partition, device in enumerate(replica):
            if device in devs:
                if partition not in partition_count:
                    partition_count[partition] = 0
                partition_count[partition] += 1

    # Sort by number of found replicas to keep the output format
    sorted_partition_count = sorted(
        partition_count.items(), key=itemgetter(1), reverse=True)

    return sorted_partition_count


def _parse_list_parts_values(argvish):

    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    try:
        devs = []
        if len(args) > 0:
            if new_cmd_format:
                print(Commands.list_parts.__doc__.strip())
                exit(EXIT_ERROR)

            for arg in args:
                devs.extend(
                    builder.search_devs(parse_search_value(arg)) or [])
        else:
            devs.extend(builder.search_devs(
                parse_search_values_from_opts(opts)) or [])

        return devs
    except ValueError as e:
        print(e)
        exit(EXIT_ERROR)


def _parse_add_values(argvish):
    """
    Parse devices to add as specified on the command line.

    Will exit on error and spew warnings.

    :returns: array of device dicts
    """
    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    parsed_devs = []
    if len(args) > 0:
        if new_cmd_format or len(args) % 2 != 0:
            print(Commands.add.__doc__.strip())
            exit(EXIT_ERROR)

        devs_and_weights = zip(islice(args, 0, len(args), 2),
                               islice(args, 1, len(args), 2))

        for devstr, weightstr in devs_and_weights:
            dev_dict = parse_add_value(devstr)

            if dev_dict['region'] is None:
                stderr.write('WARNING: No region specified for %s. '
                             'Defaulting to region 1.\n' % devstr)
                dev_dict['region'] = 1

            if dev_dict['replication_ip'] is None:
                dev_dict['replication_ip'] = dev_dict['ip']

            if dev_dict['replication_port'] is None:
                dev_dict['replication_port'] = dev_dict['port']

            weight = float(weightstr)
            if weight < 0:
                raise ValueError('Invalid weight value: %s' % devstr)
            dev_dict['weight'] = weight

            parsed_devs.append(dev_dict)
    else:
        parsed_devs.append(build_dev_from_opts(opts))

    return parsed_devs


def check_devs(devs, input_question, opts, abort_msg):

    if not devs:
        print('Search value matched 0 devices.\n'
              'The on-disk ring builder is unchanged.')
        exit(EXIT_ERROR)

    if len(devs) > 1:
        print('Matched more than one device:')
        for dev in devs:
            print('    %s' % format_device(dev))
        try:
            abort = not opts.yes and input(input_question) != 'y'
        except (EOFError, KeyboardInterrupt):
            abort = True
        if abort:
            print(abort_msg)
            exit(EXIT_ERROR)


def _set_weight_values(devs, weight, opts):

    input_question = 'Are you sure you want to update the weight for these ' \
                     '%s devices? (y/N) ' % len(devs)
    abort_msg = 'Aborting device modifications'
    check_devs(devs, input_question, opts, abort_msg)

    for dev in devs:
        builder.set_dev_weight(dev['id'], weight)
        print('%s weight set to %s' % (format_device(dev),
                                       dev['weight']))


def _set_region_values(devs, region, opts):

    input_question = 'Are you sure you want to update the region for these ' \
                     '%s devices? (y/N) ' % len(devs)
    abort_msg = 'Aborting device modifications'
    check_devs(devs, input_question, opts, abort_msg)

    for dev in devs:
        builder.set_dev_region(dev['id'], region)
        print('%s region set to %s' % (format_device(dev),
                                       dev['region']))


def _set_zone_values(devs, zone, opts):

    input_question = 'Are you sure you want to update the zone for these ' \
                     '%s devices? (y/N) ' % len(devs)
    abort_msg = 'Aborting device modifications'
    check_devs(devs, input_question, opts, abort_msg)

    for dev in devs:
        builder.set_dev_zone(dev['id'], zone)
        print('%s zone set to %s' % (format_device(dev),
                                     dev['zone']))


def _parse_set_weight_values(argvish):

    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    try:
        if not new_cmd_format:
            if len(args) % 2 != 0:
                print(Commands.set_weight.__doc__.strip())
                exit(EXIT_ERROR)

            devs_and_weights = zip(islice(argvish, 0, len(argvish), 2),
                                   islice(argvish, 1, len(argvish), 2))
            for devstr, weightstr in devs_and_weights:
                devs = (builder.search_devs(
                    parse_search_value(devstr)) or [])
                weight = float(weightstr)
                _set_weight_values(devs, weight, opts)
        else:
            if len(args) != 1:
                print(Commands.set_weight.__doc__.strip())
                exit(EXIT_ERROR)

            devs = (builder.search_devs(
                parse_search_values_from_opts(opts)) or [])
            weight = float(args[0])
            _set_weight_values(devs, weight, opts)
    except ValueError as e:
        print(e)
        exit(EXIT_ERROR)


def _set_info_values(devs, change, opts):

    input_question = 'Are you sure you want to update the info for these ' \
                     '%s devices? (y/N) ' % len(devs)
    abort_msg = 'Aborting device modifications'
    check_devs(devs, input_question, opts, abort_msg)

    for dev in devs:
        orig_dev_string = format_device(dev)
        test_dev = dict(dev)
        for key in change:
            test_dev[key] = change[key]
        for check_dev in builder.devs:
            if not check_dev or check_dev['id'] == test_dev['id']:
                continue
            if check_dev['ip'] == test_dev['ip'] and \
                    check_dev['port'] == test_dev['port'] and \
                    check_dev['device'] == test_dev['device']:
                print('Device %d already uses %s:%d/%s.' %
                      (check_dev['id'], check_dev['ip'],
                       check_dev['port'], check_dev['device']))
                exit(EXIT_ERROR)
        for key in change:
            dev[key] = change[key]
        print('Device %s is now %s' % (orig_dev_string,
                                       format_device(dev)))


def calculate_change_value(change_value, change, v_name, v_name_port):
    ip = ''
    if change_value and change_value[0].isdigit():
        i = 1
        while (i < len(change_value) and
               change_value[i] in '0123456789.'):
            i += 1
        ip = change_value[:i]
        change_value = change_value[i:]
    elif change_value and change_value.startswith('['):
        i = 1
        while i < len(change_value) and change_value[i] != ']':
            i += 1
        i += 1
        ip = change_value[:i].lstrip('[').rstrip(']')
        change_value = change_value[i:]
    if ip:
        change[v_name] = validate_and_normalize_ip(ip)
    if change_value.startswith(':'):
        i = 1
        while i < len(change_value) and change_value[i].isdigit():
            i += 1
        change[v_name_port] = int(change_value[1:i])
        change_value = change_value[i:]
    return change_value


def _parse_set_region_values(argvish):

    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    try:
        devs = []
        if not new_cmd_format:
            if len(args) % 2 != 0:
                print(Commands.set_region.__doc__.strip())
                exit(EXIT_ERROR)

            devs_and_regions = zip(islice(argvish, 0, len(argvish), 2),
                                   islice(argvish, 1, len(argvish), 2))
            for devstr, regionstr in devs_and_regions:
                devs.extend(builder.search_devs(
                    parse_search_value(devstr)) or [])
                region = int(regionstr)
                _set_region_values(devs, region, opts)
        else:
            if len(args) != 1:
                print(Commands.set_region.__doc__.strip())
                exit(EXIT_ERROR)

            devs.extend(builder.search_devs(
                parse_search_values_from_opts(opts)) or [])
            region = int(args[0])
            _set_region_values(devs, region, opts)
    except ValueError as e:
        print(e)
        exit(EXIT_ERROR)


def _parse_set_zone_values(argvish):

    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    try:
        devs = []
        if not new_cmd_format:
            if len(args) % 2 != 0:
                print(Commands.set_zone.__doc__.strip())
                exit(EXIT_ERROR)

            devs_and_zones = zip(islice(argvish, 0, len(argvish), 2),
                                 islice(argvish, 1, len(argvish), 2))
            for devstr, zonestr in devs_and_zones:
                devs.extend(builder.search_devs(
                    parse_search_value(devstr)) or [])
                zone = int(zonestr)
                _set_zone_values(devs, zone, opts)
        else:
            if len(args) != 1:
                print(Commands.set_zone.__doc__.strip())
                exit(EXIT_ERROR)

            devs.extend(builder.search_devs(
                parse_search_values_from_opts(opts)) or [])
            zone = int(args[0])
            _set_zone_values(devs, zone, opts)
    except ValueError as e:
        print(e)
        exit(EXIT_ERROR)


def _parse_set_info_values(argvish):

    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    if not new_cmd_format:
        if len(args) % 2 != 0:
            print(Commands.search.__doc__.strip())
            exit(EXIT_ERROR)

        searches_and_changes = zip(islice(argvish, 0, len(argvish), 2),
                                   islice(argvish, 1, len(argvish), 2))

        for search_value, change_value in searches_and_changes:
            devs = builder.search_devs(parse_search_value(search_value))
            change = {}

            change_value = calculate_change_value(change_value, change,
                                                  'ip', 'port')

            if change_value.startswith('R'):
                change_value = change_value[1:]
                change_value = calculate_change_value(change_value, change,
                                                      'replication_ip',
                                                      'replication_port')
            if change_value.startswith('/'):
                i = 1
                while i < len(change_value) and change_value[i] != '_':
                    i += 1
                change['device'] = change_value[1:i]
                change_value = change_value[i:]
            if change_value.startswith('_'):
                change['meta'] = change_value[1:]
                change_value = ''
            if change_value or not change:
                raise ValueError('Invalid set info change value: %s' %
                                 repr(argvish[1]))
            _set_info_values(devs, change, opts)
    else:
        devs = builder.search_devs(parse_search_values_from_opts(opts))
        change = parse_change_values_from_opts(opts)
        _set_info_values(devs, change, opts)


def _parse_remove_values(argvish):

    new_cmd_format, opts, args = validate_args(argvish)

    # We'll either parse the all-in-one-string format or the
    # --options format,
    # but not both. If both are specified, raise an error.
    try:
        devs = []
        if len(args) > 0:
            if new_cmd_format:
                print(Commands.remove.__doc__.strip())
                exit(EXIT_ERROR)

            for arg in args:
                devs.extend(builder.search_devs(
                    parse_search_value(arg)) or [])
        else:
            devs.extend(builder.search_devs(
                parse_search_values_from_opts(opts)))

        return (devs, opts)
    except ValueError as e:
        print(e)
        exit(EXIT_ERROR)


def _make_display_device_table(builder):
    ip_width = 10
    port_width = 4
    rep_ip_width = 14
    rep_port_width = 4
    ip_ipv6 = rep_ipv6 = False
    weight_width = 6
    for dev in builder._iter_devs():
        if is_valid_ipv6(dev['ip']):
            ip_ipv6 = True
        if is_valid_ipv6(dev['replication_ip']):
            rep_ipv6 = True
        ip_width = max(len(dev['ip']), ip_width)
        rep_ip_width = max(len(dev['replication_ip']), rep_ip_width)
        port_width = max(len(str(dev['port'])), port_width)
        rep_port_width = max(len(str(dev['replication_port'])),
                             rep_port_width)
        weight_width = max(len('%6.02f' % dev['weight']),
                           weight_width)
    if ip_ipv6:
        ip_width += 2
    if rep_ipv6:
        rep_ip_width += 2
    header_line = ('Devices:%5s %6s %4s %' + str(ip_width)
                   + 's:%-' + str(port_width) + 's %' +
                   str(rep_ip_width) + 's:%-' + str(rep_port_width) +
                   's %5s %' + str(weight_width) + 's %10s %7s %5s %s') % (
                       'id', 'region', 'zone', 'ip address',
                       'port', 'replication ip', 'port', 'name',
                       'weight', 'partitions', 'balance', 'flags',
                       'meta')

    def print_dev_f(dev, balance_per_dev=0.00, flags=''):
        def get_formated_ip(key):
            value = dev[key]
            if ':' in value:
                value = '[%s]' % value
            return value
        dev_ip = get_formated_ip('ip')
        dev_replication_ip = get_formated_ip('replication_ip')
        format_string = ''.join(['%13d %6d %4d ',
                                 '%', str(ip_width), 's:%-',
                                 str(port_width), 'd ', '%',
                                 str(rep_ip_width), 's', ':%-',
                                 str(rep_port_width), 'd %5s %',
                                 str(weight_width), '.02f'
                                 ' %10s %7.02f %5s %s'])
        args = (dev['id'], dev['region'], dev['zone'], dev_ip, dev['port'],
                dev_replication_ip, dev['replication_port'], dev['device'],
                dev['weight'], dev['parts'], balance_per_dev, flags,
                dev['meta'])
        print(format_string % args)

    return header_line, print_dev_f


class Commands(object):
    @staticmethod
    def unknown():
        print('Unknown command: %s' % argv[2])
        exit(EXIT_ERROR)

    @staticmethod
    def create():
        """
swift-ring-builder <builder_file> create <part_power> <replicas>
                                         <min_part_hours>
    Creates <builder_file> with 2^<part_power> partitions and <replicas>.
    <min_part_hours> is number of hours to restrict moving a partition more
    than once.
        """
        if len(argv) < 6:
            print(Commands.create.__doc__.strip())
            exit(EXIT_ERROR)
        try:
            builder = RingBuilder(int(argv[3]), float(argv[4]), int(argv[5]))
        except ValueError as e:
            print(e)
            exit(EXIT_ERROR)
        backup_dir = pathjoin(dirname(builder_file), 'backups')
        try:
            mkdir(backup_dir)
        except OSError as err:
            if err.errno != EEXIST:
                raise
        builder.save(pathjoin(backup_dir,
                              '%d.' % time() + basename(builder_file)))
        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def default():
        """
swift-ring-builder <builder_file>
    Shows information about the ring and the devices within. Output
    includes a table that describes the report parameters (id, region,
    port, flags, etc).
    flags: possible values are 'DEL' and ''
        DEL - indicates that the device is marked for removal from
              ring and will be removed in next rebalance.
        """
        try:
            builder_id = builder.id
        except AttributeError:
            builder_id = "(not assigned)"
        print('%s, build version %d, id %s' %
              (builder_file, builder.version, builder_id))
        balance = 0
        ring_empty_error = None
        regions = len(set(d['region'] for d in builder.devs
                          if d is not None))
        zones = len(set((d['region'], d['zone']) for d in builder.devs
                        if d is not None))
        dev_count = len([dev for dev in builder.devs
                         if dev is not None])
        try:
            balance = builder.get_balance()
        except exceptions.EmptyRingError as e:
            ring_empty_error = str(e)
        dispersion_trailer = '' if builder.dispersion is None else (
            ', %.02f dispersion' % (builder.dispersion))
        print('%d partitions, %.6f replicas, %d regions, %d zones, '
              '%d devices, %.02f balance%s' % (
                  builder.parts, builder.replicas, regions, zones, dev_count,
                  balance, dispersion_trailer))
        print('The minimum number of hours before a partition can be '
              'reassigned is %s (%s remaining)' % (
                  builder.min_part_hours,
                  timedelta(seconds=builder.min_part_seconds_left)))
        print('The overload factor is %0.2f%% (%.6f)' % (
            builder.overload * 100, builder.overload))

        ring_dict = None
        builder_dict = builder.get_ring().to_dict()

        # compare ring file against builder file
        if not exists(ring_file):
            print('Ring file %s not found, '
                  'probably it hasn\'t been written yet' % ring_file)
        else:
            try:
                ring_dict = RingData.load(ring_file).to_dict()
            except Exception as exc:
                print('Ring file %s is invalid: %r' % (ring_file, exc))
            else:
                if builder_dict == ring_dict:
                    print('Ring file %s is up-to-date' % ring_file)
                else:
                    print('Ring file %s is obsolete' % ring_file)

        if ring_empty_error:
            balance_per_dev = defaultdict(int)
        else:
            balance_per_dev = builder._build_balance_per_dev()
        header_line, print_dev_f = _make_display_device_table(builder)
        print(header_line)
        for dev in sorted(
            builder._iter_devs(),
            key=lambda x: (x['region'], x['zone'], x['ip'], x['device'])
        ):
            flags = 'DEL' if dev in builder._remove_devs else ''
            print_dev_f(dev, balance_per_dev[dev['id']], flags)

        # Print some helpful info if partition power increase in progress
        if (builder.next_part_power and
                builder.next_part_power == (builder.part_power + 1)):
            print('\nPreparing increase of partition power (%d -> %d)' % (
                  builder.part_power, builder.next_part_power))
            print('Run "swift-object-relinker relink" on all nodes before '
                  'moving on to increase_partition_power.')
        if (builder.next_part_power and
                builder.part_power == builder.next_part_power):
            print('\nIncreased partition power (%d -> %d)' % (
                  builder.part_power, builder.next_part_power))
            if builder_dict != ring_dict:
                print('First run "swift-ring-builder <builderfile> write_ring"'
                      ' now and copy the updated .ring.gz file to all nodes.')
            print('Run "swift-object-relinker cleanup" on all nodes before '
                  'moving on to finish_increase_partition_power.')

        if ring_empty_error:
            print(ring_empty_error)
        exit(EXIT_SUCCESS)

    @staticmethod
    def search():
        """
swift-ring-builder <builder_file> search <search-value>

or

swift-ring-builder <builder_file> search
    --region <region> --zone <zone> --ip <ip or hostname> --port <port>
    --replication-ip <r_ip or r_hostname> --replication-port <r_port>
    --device <device_name> --meta <meta> --weight <weight>

    Where <r_ip>, <r_hostname> and <r_port> are replication ip, hostname
    and port.
    Any of the options are optional in both cases.

    Shows information about matching devices.
        """
        if len(argv) < 4:
            print(Commands.search.__doc__.strip())
            print()
            print(parse_search_value.__doc__.strip())
            exit(EXIT_ERROR)

        devs = builder.search_devs(_parse_search_values(argv[3:]))

        if not devs:
            print('No matching devices found')
            exit(EXIT_ERROR)
        print('Devices:    id  region  zone      ip address  port  '
              'replication ip  replication port      name weight partitions '
              'balance meta')
        weighted_parts = builder.parts * builder.replicas / \
            sum(d['weight'] for d in builder.devs if d is not None)
        for dev in devs:
            if not dev['weight']:
                if dev['parts']:
                    balance = MAX_BALANCE
                else:
                    balance = 0
            else:
                balance = 100.0 * dev['parts'] / \
                    (dev['weight'] * weighted_parts) - 100.0
            print('         %5d %7d %5d %15s %5d %15s %17d %9s %6.02f %10s '
                  '%7.02f %s' %
                  (dev['id'], dev['region'], dev['zone'], dev['ip'],
                   dev['port'], dev['replication_ip'], dev['replication_port'],
                   dev['device'], dev['weight'], dev['parts'], balance,
                   dev['meta']))
        exit(EXIT_SUCCESS)

    @staticmethod
    def list_parts():
        """
swift-ring-builder <builder_file> list_parts <search-value> [<search-value>] ..

or

swift-ring-builder <builder_file> list_parts
    --region <region> --zone <zone> --ip <ip or hostname> --port <port>
    --replication-ip <r_ip or r_hostname> --replication-port <r_port>
    --device <device_name> --meta <meta> --weight <weight>

    Where <r_ip>, <r_hostname> and <r_port> are replication ip, hostname
    and port.
    Any of the options are optional in both cases.

    Returns a 2 column list of all the partitions that are assigned to any of
    the devices matching the search values given. The first column is the
    assigned partition number and the second column is the number of device
    matches for that partition. The list is ordered from most number of matches
    to least. If there are a lot of devices to match against, this command
    could take a while to run.
        """
        if len(argv) < 4:
            print(Commands.list_parts.__doc__.strip())
            print()
            print(parse_search_value.__doc__.strip())
            exit(EXIT_ERROR)

        if not builder._replica2part2dev:
            print('Specified builder file \"%s\" is not rebalanced yet. '
                  'Please rebalance first.' % builder_file)
            exit(EXIT_ERROR)

        devs = _parse_list_parts_values(argv[3:])
        if not devs:
            print('No matching devices found')
            exit(EXIT_ERROR)

        sorted_partition_count = _find_parts(devs)

        if not sorted_partition_count:
            print('No matching devices found')
            exit(EXIT_ERROR)

        print('Partition   Matches')
        for partition, count in sorted_partition_count:
            print('%9d   %7d' % (partition, count))
        exit(EXIT_SUCCESS)

    @staticmethod
    def add():
        """
swift-ring-builder <builder_file> add
    [r<region>]z<zone>-<ip>:<port>[R<r_ip>:<r_port>]/<device_name>_<meta>
     <weight>
    [[r<region>]z<zone>-<ip>:<port>[R<r_ip>:<r_port>]/<device_name>_<meta>
     <weight>] ...

    Where <r_ip> and <r_port> are replication ip and port.

or

swift-ring-builder <builder_file> add
    --region <region> --zone <zone> --ip <ip or hostname> --port <port>
    [--replication-ip <r_ip or r_hostname>] [--replication-port <r_port>]
    --device <device_name> --weight <weight>
    [--meta <meta>]

    Adds devices to the ring with the given information. No partitions will be
    assigned to the new device until after running 'rebalance'. This is so you
    can make multiple device changes and rebalance them all just once.
        """
        if len(argv) < 5:
            print(Commands.add.__doc__.strip())
            exit(EXIT_ERROR)

        if builder.next_part_power:
            print('Partition power increase in progress. You need ')
            print('to finish the increase first before adding devices.')
            exit(EXIT_ERROR)

        try:
            for new_dev in _parse_add_values(argv[3:]):
                for dev in builder.devs:
                    if dev is None:
                        continue
                    if dev['ip'] == new_dev['ip'] and \
                            dev['port'] == new_dev['port'] and \
                            dev['device'] == new_dev['device']:
                        print('Device %d already uses %s:%d/%s.' %
                              (dev['id'], dev['ip'],
                               dev['port'], dev['device']))
                        print("The on-disk ring builder is unchanged.\n")
                        exit(EXIT_ERROR)
                dev_id = builder.add_dev(new_dev)
                print('Device %s with %s weight got id %s' %
                      (format_device(new_dev), new_dev['weight'], dev_id))
        except ValueError as err:
            print(err)
            print('The on-disk ring builder is unchanged.')
            exit(EXIT_ERROR)

        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def set_weight():
        """
swift-ring-builder <builder_file> set_weight <search-value> <new_weight>
    [<search-value> <new_weight>] ...
    [--yes]

or

swift-ring-builder <builder_file> set_weight
    --region <region> --zone <zone> --ip <ip or hostname> --port <port>
    --replication-ip <r_ip or r_hostname> --replication-port <r_port>
    --device <device_name> --meta <meta> --weight <weight> <new_weight>
    [--yes]

    Where <r_ip>, <r_hostname> and <r_port> are replication ip, hostname
    and port. <weight> and <new_weight> are the search weight and new
    weight values respectively.
    Any of the options are optional in both cases.

    Resets the devices' weights. No partitions will be reassigned to or from
    the device until after running 'rebalance'. This is so you can make
    multiple device changes and rebalance them all just once.

    Option --yes assume a yes response to all questions.
        """
        # if len(argv) < 5 or len(argv) % 2 != 1:
        if len(argv) < 5:
            print(Commands.set_weight.__doc__.strip())
            print()
            print(parse_search_value.__doc__.strip())
            exit(EXIT_ERROR)

        _parse_set_weight_values(argv[3:])

        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def set_region():
        """
swift-ring-builder <builder_file> set_region <search-value> <region>
    [<search-value> <region] ...

or

swift-ring-builder <builder_file> set_region
    --region <region> --zone <zone> --ip <ip or hostname> --port <port>
    --replication-ip <r_ip or r_hostname> --replication-port <r_port>
    --device <device_name> --meta <meta> <new region> [--yes]

    Where <r_ip>, <r_hostname> and <r_port> are replication ip, hostname
    and port.
    Any of the options are optional in both cases.

    Resets the devices' regions. No partitions will be reassigned to or from
    the device until after running 'rebalance'. This is so you can make
    multiple device changes and rebalance them all just once.

    Option --yes assume a yes response to all questions.
        """
        if len(argv) < 5:
            print(Commands.set_region.__doc__.strip())
            print()
            print(parse_search_value.__doc__.strip())
            exit(EXIT_ERROR)

        _parse_set_region_values(argv[3:])

        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def set_zone():
        """
swift-ring-builder <builder_file> set_zone <search-value> <zone>
    [<search-value> <zone] ...

or

swift-ring-builder <builder_file> set_zone
    --region <region> --zone <zone> --ip <ip or hostname> --port <port>
    --replication-ip <r_ip or r_hostname> --replication-port <r_port>
    --device <device_name> --meta <meta> <new zone> [--yes]

    Where <r_ip>, <r_hostname> and <r_port> are replication ip, hostname
    and port.
    Any of the options are optional in both cases.

    Resets the devices' zones. No partitions will be reassigned to or from
    the device until after running 'rebalance'. This is so you can make
    multiple device changes and rebalance them all just once.

    Option --yes assume a yes response to all questions.
        """
        # if len(argv) < 5 or len(argv) % 2 != 1:
        if len(argv) < 5:
            print(Commands.set_zone.__doc__.strip())
            print()
            print(parse_search_value.__doc__.strip())
            exit(EXIT_ERROR)

        _parse_set_zone_values(argv[3:])

        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def set_info():
        """
swift-ring-builder <builder_file> set_info
    <search-value> <ip>:<port>[R<r_ip>:<r_port>]/<device_name>_<meta>
    [<search-value> <ip>:<port>[R<r_ip>:<r_port>]/<device_name>_<meta>] ...
    [--yes]

or

swift-ring-builder <builder_file> set_info
    --ip <ip or hostname> --port <port>
    --replication-ip <r_ip or r_hostname> --replication-port <r_port>
    --device <device_name> --meta <meta>
    --change-ip <ip or hostname> --change-port <port>
    --change-replication-ip <r_ip or r_hostname>
    --change-replication-port <r_port>
    --change-device <device_name>
    --change-meta <meta>
    [--yes]

    Where <r_ip>, <r_hostname> and <r_port> are replication ip, hostname
    and port.
    Any of the options are optional in both cases.

    For each search-value, resets the matched device's information.
    This information isn't used to assign partitions, so you can use
    'write_ring' afterward to rewrite the current ring with the newer
    device information. Any of the parts are optional in the final
    <ip>:<port>/<device_name>_<meta> parameter; just give what you
    want to change. For instance set_info d74 _"snet: 5.6.7.8" would
    just update the meta data for device id 74.

    Option --yes assume a yes response to all questions.
        """
        if len(argv) < 5:
            print(Commands.set_info.__doc__.strip())
            print()
            print(parse_search_value.__doc__.strip())
            exit(EXIT_ERROR)

        try:
            _parse_set_info_values(argv[3:])
        except ValueError as err:
            print(err)
            exit(EXIT_ERROR)

        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def remove():
        """
swift-ring-builder <builder_file> remove <search-value> [search-value ...]
    [--yes]

or

swift-ring-builder <builder_file> remove
    --region <region> --zone <zone> --ip <ip or hostname> --port <port>
    --replication-ip <r_ip or r_hostname> --replication-port <r_port>
    --device <device_name> --meta <meta> --weight <weight>
    [--yes]

    Where <r_ip>, <r_hostname> and <r_port> are replication ip, hostname
    and port.
    Any of the options are optional in both cases.

    Removes the device(s) from the ring. This should normally just be used for
    a device that has failed. For a device you wish to decommission, it's best
    to set its weight to 0, wait for it to drain all its data, then use this
    remove command. This will not take effect until after running 'rebalance'.
    This is so you can make multiple device changes and rebalance them all just
    once.

    Option --yes assume a yes response to all questions.
        """
        if len(argv) < 4:
            print(Commands.remove.__doc__.strip())
            print()
            print(parse_search_value.__doc__.strip())
            exit(EXIT_ERROR)

        if builder.next_part_power:
            print('Partition power increase in progress. You need ')
            print('to finish the increase first before removing devices.')
            exit(EXIT_ERROR)

        devs, opts = _parse_remove_values(argv[3:])

        input_question = 'Are you sure you want to remove these ' \
                         '%s devices? (y/N) ' % len(devs)
        abort_msg = 'Aborting device removals'
        check_devs(devs, input_question, opts, abort_msg)

        for dev in devs:
            try:
                builder.remove_dev(dev['id'])
            except exceptions.RingBuilderError as e:
                print('-' * 79)
                print(
                    'An error occurred while removing device with id %d\n'
                    'This usually means that you attempted to remove\n'
                    'the last device in a ring. If this is the case,\n'
                    'consider creating a new ring instead.\n'
                    'The on-disk ring builder is unchanged.\n'
                    'Original exception message: %s' %
                    (dev['id'], e))
                print('-' * 79)
                exit(EXIT_ERROR)

            print('%s marked for removal and will '
                  'be removed next rebalance.' % format_device(dev))
        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def rebalance():
        """
swift-ring-builder <builder_file> rebalance [options]
    Attempts to rebalance the ring by reassigning partitions that haven't been
    recently reassigned.
        """
        usage = Commands.rebalance.__doc__.strip()
        parser = optparse.OptionParser(usage)
        parser.add_option('-f', '--force', action='store_true',
                          help='Force a rebalanced ring to save even '
                          'if < 1% of parts changed')
        parser.add_option('-s', '--seed', help="seed to use for rebalance")
        parser.add_option('-d', '--debug', action='store_true',
                          help="print debug information")
        options, args = parser.parse_args(argv)

        def get_seed(index):
            if options.seed:
                return options.seed
            try:
                return args[index]
            except IndexError:
                pass

        if options.debug:
            logger = logging.getLogger("swift.ring.builder")
            logger.disabled = False
            logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler(stdout)
            formatter = logging.Formatter("%(levelname)s: %(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        if builder.next_part_power:
            print('Partition power increase in progress.')
            print('You need to finish the increase first before rebalancing.')
            exit(EXIT_ERROR)

        devs_changed = builder.devs_changed
        min_part_seconds_left = builder.min_part_seconds_left
        try:
            last_balance = builder.get_balance()
            last_dispersion = builder.dispersion
            parts, balance, removed_devs = builder.rebalance(seed=get_seed(3))
            dispersion = builder.dispersion
        except exceptions.RingBuilderError as e:
            print('-' * 79)
            print("An error has occurred during ring validation. Common\n"
                  "causes of failure are rings that are empty or do not\n"
                  "have enough devices to accommodate the replica count.\n"
                  "Original exception message:\n %s" %
                  (e,))
            print('-' * 79)
            exit(EXIT_ERROR)
        if not (parts or options.force or removed_devs):
            print('No partitions could be reassigned.')
            if min_part_seconds_left > 0:
                print('The time between rebalances must be at least '
                      'min_part_hours: %s hours (%s remaining)' % (
                          builder.min_part_hours,
                          timedelta(seconds=builder.min_part_seconds_left)))
            else:
                print('There is no need to do so at this time')
            exit(EXIT_WARNING)
        # If we set device's weight to zero, currently balance will be set
        # special value(MAX_BALANCE) until zero weighted device return all
        # its partitions. So we cannot check balance has changed.
        # Thus we need to check balance or last_balance is special value.
        be_cowardly = True
        if options.force:
            # User said save it, so we save it.
            be_cowardly = False
        elif devs_changed:
            # We must save if a device changed; this could be something like
            # a changed IP address.
            be_cowardly = False
        else:
            # If balance or dispersion changed (presumably improved), then
            # we should save to get the improvement.
            balance_changed = (
                abs(last_balance - balance) >= 1 or
                (last_balance == MAX_BALANCE and balance == MAX_BALANCE))
            dispersion_changed = last_dispersion is None or (
                abs(last_dispersion - dispersion) >= 1)
            if balance_changed or dispersion_changed:
                be_cowardly = False

        if be_cowardly:
            print('Cowardly refusing to save rebalance as it did not change '
                  'at least 1%.')
            exit(EXIT_WARNING)
        try:
            builder.validate()
        except exceptions.RingValidationError as e:
            print('-' * 79)
            print("An error has occurred during ring validation. Common\n"
                  "causes of failure are rings that are empty or do not\n"
                  "have enough devices to accommodate the replica count.\n"
                  "Original exception message:\n %s" %
                  (e,))
            print('-' * 79)
            exit(EXIT_ERROR)
        print('Reassigned %d (%.02f%%) partitions. '
              'Balance is now %.02f.  '
              'Dispersion is now %.02f' % (
                  parts, 100.0 * parts / builder.parts,
                  balance,
                  builder.dispersion))
        status = EXIT_SUCCESS
        if builder.dispersion > 0:
            print('-' * 79)
            print(
                'NOTE: Dispersion of %.06f indicates some parts are not\n'
                '      optimally dispersed.\n\n'
                '      You may want to adjust some device weights, increase\n'
                '      the overload or review the dispersion report.' %
                builder.dispersion)
            status = EXIT_WARNING
            print('-' * 79)
        elif balance > 5 and balance / 100.0 > builder.overload:
            print('-' * 79)
            print('NOTE: Balance of %.02f indicates you should push this ' %
                  balance)
            print('      ring, wait at least %d hours, and rebalance/repush.'
                  % builder.min_part_hours)
            print('-' * 79)
            status = EXIT_WARNING
        ts = time()
        builder.get_ring().save(
            pathjoin(backup_dir, '%d.' % ts + basename(ring_file)))
        builder.save(pathjoin(backup_dir, '%d.' % ts + basename(builder_file)))
        builder.get_ring().save(ring_file)
        builder.save(builder_file)
        exit(status)

    @staticmethod
    def dispersion():
        r"""
swift-ring-builder <builder_file> dispersion <search_filter> [options]

    Output report on dispersion.

    --recalculate option will rebuild cached dispersion info and save builder
    --verbose option will display dispersion graph broken down by tier

    You can filter which tiers are evaluated to drill down using a regex
    in the optional search_filter argument.  i.e.

        swift-ring-builder <builder_file> dispersion "r\d+z\d+$" -v

    ... would only display rows for the zone tiers

        swift-ring-builder <builder_file> dispersion ".*\-[^/]*$" -v

    ... would only display rows for the server tiers

    The reports columns are:

    Tier  : the name of the tier
    parts : the total number of partitions with assignment in the tier
    %     : the percentage of parts in the tier with replicas over assigned
    max   : maximum replicas a part should have assigned at the tier
    0 - N : the number of parts with that many replicas assigned

    e.g.
        Tier:  parts      %   max   0    1    2   3
        r1z1    1022  79.45     1   2  210  784  28

        r1z1 has 1022 total parts assigned, 79% of them have more than the
        recommend max replica count of 1 assigned.  Only 2 parts in the ring
        are *not* assigned in this tier (0 replica count), 210 parts have
        the recommend replica count of 1, 784 have 2 replicas, and 28 sadly
        have all three replicas in this tier.
        """
        status = EXIT_SUCCESS
        if not builder._replica2part2dev:
            print('Specified builder file \"%s\" is not rebalanced yet. '
                  'Please rebalance first.' % builder_file)
            exit(EXIT_ERROR)
        usage = Commands.dispersion.__doc__.strip()
        parser = optparse.OptionParser(usage)
        parser.add_option('--recalculate', action='store_true',
                          help='Rebuild cached dispersion info and save')
        parser.add_option('-v', '--verbose', action='store_true',
                          help='Display dispersion report for tiers')
        options, args = parser.parse_args(argv)
        if args[3:]:
            search_filter = args[3]
        else:
            search_filter = None
        orig_version = builder.version
        report = dispersion_report(builder, search_filter=search_filter,
                                   verbose=options.verbose,
                                   recalculate=options.recalculate)
        if builder.version != orig_version:
            # we've already done the work, better go ahead and save it!
            builder.save(builder_file)
        print('Dispersion is %.06f, Balance is %.06f, Overload is %0.2f%%' % (
            builder.dispersion, builder.get_balance(), builder.overload * 100))
        print('Required overload is %.6f%%' % (
            builder.get_required_overload() * 100))
        if report['worst_tier']:
            status = EXIT_WARNING
            print('Worst tier is %.06f (%s)' % (report['max_dispersion'],
                                                report['worst_tier']))
        if report['graph']:
            replica_range = list(range(int(math.ceil(builder.replicas + 1))))
            part_count_width = '%%%ds' % max(len(str(builder.parts)), 5)
            replica_counts_tmpl = ' '.join(part_count_width for i in
                                           replica_range)
            tiers = (tier for tier, _junk in report['graph'])
            tier_width = max(max(map(len, tiers)), 30)
            header_line = ('%-' + str(tier_width) +
                           's ' + part_count_width +
                           ' %6s %6s ' + replica_counts_tmpl) % tuple(
                               ['Tier', 'Parts', '%', 'Max'] + replica_range)
            underline = '-' * len(header_line)
            print(underline)
            print(header_line)
            print(underline)
            for tier_name, dispersion in report['graph']:
                replica_counts_repr = replica_counts_tmpl % tuple(
                    dispersion['replicas'])
                template = ''.join([
                    '%-', str(tier_width), 's ',
                    part_count_width,
                    ' %6.02f %6d %s',
                ])
                args = (
                    tier_name,
                    dispersion['placed_parts'],
                    dispersion['dispersion'],
                    dispersion['max_replicas'],
                    replica_counts_repr,
                )
                print(template % args)
        exit(status)

    @staticmethod
    def validate():
        """
swift-ring-builder <builder_file> validate
    Just runs the validation routines on the ring.
        """
        builder.validate()
        exit(EXIT_SUCCESS)

    @staticmethod
    def write_ring():
        """
swift-ring-builder <builder_file> write_ring
    Just rewrites the distributable ring file. This is done automatically after
    a successful rebalance, so really this is only useful after one or more
    'set_info' calls when no rebalance is needed but you want to send out the
    new device information.
        """
        if not builder.devs:
            print('Unable to write empty ring.')
            exit(EXIT_ERROR)

        ring_data = builder.get_ring()
        if not ring_data._replica2part2dev_id:
            if ring_data.devs:
                print('Warning: Writing a ring with no partition '
                      'assignments but with devices; did you forget to run '
                      '"rebalance"?')
        ring_data.save(
            pathjoin(backup_dir, '%d.' % time() + basename(ring_file)))
        ring_data.save(ring_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def write_builder():
        """
swift-ring-builder <ring_file> write_builder [min_part_hours]
    Recreate a builder from a ring file (lossy) if you lost your builder
    backups.  (Protip: don't lose your builder backups).
    [min_part_hours] is one of those numbers lost to the builder,
    you can change it with set_min_part_hours.
        """
        if exists(builder_file):
            print('Cowardly refusing to overwrite existing '
                  'Ring Builder file: %s' % builder_file)
            exit(EXIT_ERROR)
        if len(argv) > 3:
            min_part_hours = int(argv[3])
        else:
            stderr.write("WARNING: default min_part_hours may not match "
                         "the value in the lost builder.\n")
            min_part_hours = 24
        ring = Ring(ring_file)
        for dev in ring.devs:
            if dev is None:
                continue
            dev.update({
                'parts': 0,
                'parts_wanted': 0,
            })
        builder_dict = {
            'part_power': 32 - ring._part_shift,
            'replicas': float(ring.replica_count),
            'min_part_hours': min_part_hours,
            'parts': ring.partition_count,
            'devs': ring.devs,
            'devs_changed': False,
            'version': ring.version or 0,
            '_replica2part2dev': ring._replica2part2dev_id,
            '_last_part_moves_epoch': None,
            '_last_part_moves': None,
            '_last_part_gather_start': 0,
            '_remove_devs': [],
        }
        builder = RingBuilder.from_dict(builder_dict)
        for parts in builder._replica2part2dev:
            for dev_id in parts:
                builder.devs[dev_id]['parts'] += 1
        builder.save(builder_file)

    @staticmethod
    def pretend_min_part_hours_passed():
        """
swift-ring-builder <builder_file> pretend_min_part_hours_passed
    Resets the clock on the last time a rebalance happened, thus
    circumventing the min_part_hours check.

    *****************************
    USE THIS WITH EXTREME CAUTION
    *****************************

    If you run this command and deploy rebalanced rings before a replication
    pass completes, you may introduce unavailability in your cluster. This
    has an end-user impact.
        """
        builder.pretend_min_part_hours_passed()
        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def set_min_part_hours():
        """
swift-ring-builder <builder_file> set_min_part_hours <hours>
    Changes the <min_part_hours> to the given <hours>. This should be set to
    however long a full replication/update cycle takes. We're working on a way
    to determine this more easily than scanning logs.
        """
        if len(argv) < 4:
            print(Commands.set_min_part_hours.__doc__.strip())
            exit(EXIT_ERROR)
        builder.change_min_part_hours(int(argv[3]))
        print('The minimum number of hours before a partition can be '
              'reassigned is now set to %s' % argv[3])
        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def set_replicas():
        """
swift-ring-builder <builder_file> set_replicas <replicas>
    Changes the replica count to the given <replicas>. <replicas> may
    be a floating-point value, in which case some partitions will have
    floor(<replicas>) replicas and some will have ceiling(<replicas>)
    in the correct proportions.

    A rebalance is needed to make the change take effect.
    """
        if len(argv) < 4:
            print(Commands.set_replicas.__doc__.strip())
            exit(EXIT_ERROR)

        new_replicas = argv[3]
        try:
            new_replicas = float(new_replicas)
        except ValueError:
            print(Commands.set_replicas.__doc__.strip())
            print("\"%s\" is not a valid number." % new_replicas)
            exit(EXIT_ERROR)

        if new_replicas < 1:
            print("Replica count must be at least 1.")
            exit(EXIT_ERROR)

        builder.set_replicas(new_replicas)
        print('The replica count is now %.6f.' % builder.replicas)
        print('The change will take effect after the next rebalance.')
        builder.save(builder_file)
        exit(EXIT_SUCCESS)

    @staticmethod
    def set_overload():
        """
swift-ring-builder <builder_file> set_overload <overload>[%]
    Changes the overload factor to the given <overload>.

    A rebalance is needed to make the change take effect.
    """
        if len(argv) < 4:
            print(Commands.set_overload.__doc__.strip())
            exit(EXIT_ERROR)

        new_overload = argv[3]
        if new_overload.endswith('%'):
            percent = True
            new_overload = new_overload.rstrip('%')
        else:
            percent = False
        try:
            new_overload = float(new_overload)
        except ValueError:
            print(Commands.set_overload.__doc__.strip())
            print("%r is not a valid number." % new_overload)
            exit(EXIT_ERROR)

        if percent:
            new_overload *= 0.01
        if new_overload < 0:
            print("Overload must be non-negative.")
            exit(EXIT_ERROR)

        if new_overload > 1 and not percent:
            print("!?! Warning overload is greater than 100% !?!")
            status = EXIT_WARNING
        else:
            status = EXIT_SUCCESS

        builder.set_overload(new_overload)
        print('The overload factor is now %0.2f%% (%.6f)' % (
            builder.overload * 100, builder.overload))
        print('The change will take effect after the next rebalance.')
        builder.save(builder_file)
        exit(status)

    @staticmethod
    def prepare_increase_partition_power():
        """
swift-ring-builder <builder_file> prepare_increase_partition_power
    Prepare the ring to increase the partition power by one.

    A write_ring command is needed to make the change take effect.

    Once the updated rings have been deployed to all servers you need to run
    the swift-object-relinker tool to relink existing data.

    *****************************
    USE THIS WITH EXTREME CAUTION
    *****************************

    If you increase the partition power and deploy changed rings, you may
    introduce unavailability in your cluster. This has an end-user impact. Make
    sure you execute required operations to increase the partition power
    accurately.

    """
        if len(argv) < 3:
            print(Commands.prepare_increase_partition_power.__doc__.strip())
            exit(EXIT_ERROR)

        if "object" not in basename(builder_file):
            print(
                'Partition power increase is only supported for object rings.')
            exit(EXIT_ERROR)

        if not builder.prepare_increase_partition_power():
            print('Ring is already prepared for partition power increase.')
            exit(EXIT_ERROR)

        builder.save(builder_file)

        print('The next partition power is now %d.' % builder.next_part_power)
        print('The change will take effect after the next write_ring.')
        print('Ensure your proxy-servers, object-replicators and ')
        print('reconstructors are using the changed rings and relink ')
        print('(using swift-object-relinker) your existing data')
        print('before the partition power increase')
        exit(EXIT_SUCCESS)

    @staticmethod
    def increase_partition_power():
        """
swift-ring-builder <builder_file> increase_partition_power
    Increases the partition power by one. Needs to be run after
    prepare_increase_partition_power has been run and all existing data has
    been relinked using the swift-object-relinker tool.

    A write_ring command is needed to make the change take effect.

    Once the updated rings have been deployed to all servers you need to run
    the swift-object-relinker tool to cleanup old data.

    *****************************
    USE THIS WITH EXTREME CAUTION
    *****************************

    If you increase the partition power and deploy changed rings, you may
    introduce unavailability in your cluster. This has an end-user impact. Make
    sure you execute required operations to increase the partition power
    accurately.

    """
        if len(argv) < 3:
            print(Commands.increase_partition_power.__doc__.strip())
            exit(EXIT_ERROR)

        if builder.increase_partition_power():
            print('The partition power is now %d.' % builder.part_power)
            print('The change will take effect after the next write_ring.')

            builder._update_last_part_moves()
            builder.save(builder_file)

            exit(EXIT_SUCCESS)
        else:
            print('Ring partition power cannot be increased. Either the ring')
            print('was not prepared yet, or this operation has already run.')
            exit(EXIT_ERROR)

    @staticmethod
    def cancel_increase_partition_power():
        """
swift-ring-builder <builder_file> cancel_increase_partition_power
    Cancel the increase of the partition power.

    A write_ring command is needed to make the change take effect.

    Once the updated rings have been deployed to all servers you need to run
    the swift-object-relinker tool to cleanup unneeded links.

    *****************************
    USE THIS WITH EXTREME CAUTION
    *****************************

    If you increase the partition power and deploy changed rings, you may
    introduce unavailability in your cluster. This has an end-user impact. Make
    sure you execute required operations to increase the partition power
    accurately.

    """
        if len(argv) < 3:
            print(Commands.cancel_increase_partition_power.__doc__.strip())
            exit(EXIT_ERROR)

        if not builder.cancel_increase_partition_power():
            print('Ring partition power increase cannot be canceled.')
            exit(EXIT_ERROR)

        builder.save(builder_file)

        print('The next partition power is now %d.' % builder.next_part_power)
        print('The change will take effect after the next write_ring.')
        print('Ensure your object-servers are using the changed rings and')
        print('cleanup (using swift-object-relinker) the hard links')
        exit(EXIT_SUCCESS)

    @staticmethod
    def finish_increase_partition_power():
        """
swift-ring-builder <builder_file> finish_increase_partition_power
    Finally removes the next_part_power flag. Has to be run after the
    swift-object-relinker tool has been used to cleanup old existing data.

    A write_ring command is needed to make the change take effect.

    *****************************
    USE THIS WITH EXTREME CAUTION
    *****************************

    If you increase the partition power and deploy changed rings, you may
    introduce unavailability in your cluster. This has an end-user impact. Make
    sure you execute required operations to increase the partition power
    accurately.

    """
        if len(argv) < 3:
            print(Commands.finish_increase_partition_power.__doc__.strip())
            exit(EXIT_ERROR)

        if not builder.finish_increase_partition_power():
            print('Ring partition power increase cannot be finished.')
            exit(EXIT_ERROR)

        print('The change will take effect after the next write_ring.')
        builder.save(builder_file)

        exit(EXIT_SUCCESS)


def main(arguments=None):
    global argv, backup_dir, builder, builder_file, ring_file
    if arguments is not None:
        argv = arguments
    else:
        argv = sys_argv

    if len(argv) < 2:
        print("swift-ring-builder %(MAJOR_VERSION)s.%(MINOR_VERSION)s\n" %
              globals())
        print(Commands.default.__doc__.strip())
        print()
        cmds = [c for c in dir(Commands)
                if getattr(Commands, c).__doc__ and not c.startswith('_') and
                c != 'default']
        cmds.sort()
        for cmd in cmds:
            print(getattr(Commands, cmd).__doc__.strip())
            print()
        print(parse_search_value.__doc__.strip())
        print()
        for line in wrap(' '.join(cmds), 79, initial_indent='Quick list: ',
                         subsequent_indent='            '):
            print(line)
        print('Exit codes: 0 = operation successful\n'
              '            1 = operation completed with warnings\n'
              '            2 = error')
        exit(EXIT_SUCCESS)

    builder_file, ring_file = parse_builder_ring_filename_args(argv)
    if builder_file != argv[1]:
        print('Note: using %s instead of %s as builder file' % (
              builder_file, argv[1]))

    try:
        builder = RingBuilder.load(builder_file)
    except exceptions.UnPicklingError as e:
        msg = str(e)
        try:
            CompositeRingBuilder.load(builder_file)
            msg += ' (it appears to be a composite ring builder file?)'
        except Exception:  # noqa
            pass
        print(msg)
        exit(EXIT_ERROR)
    except (exceptions.FileNotFoundError, exceptions.PermissionError) as e:
        if len(argv) < 3 or argv[2] not in ('create', 'write_builder'):
            print(e)
            exit(EXIT_ERROR)
    except Exception as e:
        print('Problem occurred while reading builder file: %s. %s' %
              (builder_file, e))
        exit(EXIT_ERROR)

    backup_dir = pathjoin(dirname(builder_file), 'backups')
    try:
        mkdir(backup_dir)
    except OSError as err:
        if err.errno != EEXIST:
            raise

    if len(argv) == 2:
        command = "default"
    else:
        command = argv[2]
    if argv[0].endswith('-safe'):
        try:
            with lock_parent_directory(abspath(builder_file), 15):
                getattr(Commands, command, Commands.unknown)()
        except exceptions.LockTimeout:
            print("Ring/builder dir currently locked.")
            exit(2)
    else:
        getattr(Commands, command, Commands.unknown)()


def error_handling_main():
    # We exit code 1 on WARNING statuses, 2 on ERROR. This means we need
    # to handle any uncaught exceptions by printing the usual backtrace,
    # but then exiting 2 (not 1 as is usual for a python
    # exception).

    # We *don't* want to do this in main(), however, because we don't want to
    # pollute the test environment or cause a bunch of test churn to mock out
    # sys.excepthook

    def exit_with_status_two(tp, val, tb):
        traceback.print_exception(tp, val, tb)
        exit(2)

    sys.excepthook = exit_with_status_two
    main()


if __name__ == '__main__':
    error_handling_main()
