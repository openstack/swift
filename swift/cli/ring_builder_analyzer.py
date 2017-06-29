# Copyright (c) 2015 Samuel Merritt <sam@swiftstack.com>
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

"""
This is a tool for analyzing how well the ring builder performs its job
in a particular scenario. It is intended to help developers quantify any
improvements or regressions in the ring builder; it is probably not useful
to others.

The ring builder analyzer takes a scenario file containing some initial
parameters for a ring builder plus a certain number of rounds. In each
round, some modifications are made to the builder, e.g. add a device, remove
a device, change a device's weight. Then, the builder is repeatedly
rebalanced until it settles down. Data about that round is printed, and the
next round begins.

Scenarios are specified in JSON. Example scenario for a gradual device
addition::

    {
        "part_power": 12,
        "replicas": 3,
        "overload": 0.1,
        "random_seed": 203488,

        "rounds": [
            [
                ["add", "r1z2-10.20.30.40:6200/sda", 8000],
                ["add", "r1z2-10.20.30.40:6200/sdb", 8000],
                ["add", "r1z2-10.20.30.40:6200/sdc", 8000],
                ["add", "r1z2-10.20.30.40:6200/sdd", 8000],

                ["add", "r1z2-10.20.30.41:6200/sda", 8000],
                ["add", "r1z2-10.20.30.41:6200/sdb", 8000],
                ["add", "r1z2-10.20.30.41:6200/sdc", 8000],
                ["add", "r1z2-10.20.30.41:6200/sdd", 8000],

                ["add", "r1z2-10.20.30.43:6200/sda", 8000],
                ["add", "r1z2-10.20.30.43:6200/sdb", 8000],
                ["add", "r1z2-10.20.30.43:6200/sdc", 8000],
                ["add", "r1z2-10.20.30.43:6200/sdd", 8000],

                ["add", "r1z2-10.20.30.44:6200/sda", 8000],
                ["add", "r1z2-10.20.30.44:6200/sdb", 8000],
                ["add", "r1z2-10.20.30.44:6200/sdc", 8000]
            ], [
                ["add", "r1z2-10.20.30.44:6200/sdd", 1000]
            ], [
                ["set_weight", 15, 2000]
            ], [
                ["remove", 3],
                ["set_weight", 15, 3000]
            ], [
                ["set_weight", 15, 4000]
            ], [
                ["set_weight", 15, 5000]
            ], [
                ["set_weight", 15, 6000]
            ], [
                ["set_weight", 15, 7000]
            ], [
                ["set_weight", 15, 8000]
            ]]
    }

"""

import argparse
import json
import sys

from swift.common.ring import builder
from swift.common.ring.utils import parse_add_value


ARG_PARSER = argparse.ArgumentParser(
    description='Put the ring builder through its paces')
ARG_PARSER.add_argument(
    '--check', '-c', action='store_true',
    help="Just check the scenario, don't execute it.")
ARG_PARSER.add_argument(
    'scenario_path',
    help="Path to the scenario file")


class ParseCommandError(ValueError):

    def __init__(self, name, round_index, command_index, msg):
        msg = "Invalid %s (round %s, command %s): %s" % (
            name, round_index, command_index, msg)
        super(ParseCommandError, self).__init__(msg)


def _parse_weight(round_index, command_index, weight_str):
    try:
        weight = float(weight_str)
    except ValueError as err:
        raise ParseCommandError('weight', round_index, command_index, err)
    if weight < 0:
        raise ParseCommandError('weight', round_index, command_index,
                                'cannot be negative')
    return weight


def _parse_add_command(round_index, command_index, command):
    if len(command) != 3:
        raise ParseCommandError(
            'add command', round_index, command_index,
            'expected array of length 3, but got %r' % command)

    dev_str = command[1]
    weight_str = command[2]

    try:
        dev = parse_add_value(dev_str)
    except ValueError as err:
        raise ParseCommandError('device specifier', round_index,
                                command_index, err)

    dev['weight'] = _parse_weight(round_index, command_index, weight_str)

    if dev['region'] is None:
        dev['region'] = 1

    default_key_map = {
        'replication_ip': 'ip',
        'replication_port': 'port',
    }
    for empty_key, default_key in default_key_map.items():
        if dev[empty_key] is None:
            dev[empty_key] = dev[default_key]

    return ['add', dev]


def _parse_remove_command(round_index, command_index, command):
    if len(command) != 2:
        raise ParseCommandError('remove commnd', round_index, command_index,
                                "expected array of length 2, but got %r" %
                                (command,))

    dev_str = command[1]

    try:
        dev_id = int(dev_str)
    except ValueError as err:
        raise ParseCommandError('device ID in remove',
                                round_index, command_index, err)

    return ['remove', dev_id]


def _parse_set_weight_command(round_index, command_index, command):
    if len(command) != 3:
        raise ParseCommandError('remove command', round_index, command_index,
                                "expected array of length 3, but got %r" %
                                (command,))

    dev_str = command[1]
    weight_str = command[2]

    try:
        dev_id = int(dev_str)
    except ValueError as err:
        raise ParseCommandError('device ID in set_weight',
                                round_index, command_index, err)

    weight = _parse_weight(round_index, command_index, weight_str)
    return ['set_weight', dev_id, weight]


def _parse_save_command(round_index, command_index, command):
    if len(command) != 2:
        raise ParseCommandError(
            command, round_index, command_index,
            "expected array of length 2 but got %r" % (command,))
    return ['save', command[1]]


def parse_scenario(scenario_data):
    """
    Takes a serialized scenario and turns it into a data structure suitable
    for feeding to run_scenario().

    :returns: scenario
    :raises ValueError: on invalid scenario
    """

    parsed_scenario = {}

    try:
        raw_scenario = json.loads(scenario_data)
    except ValueError as err:
        raise ValueError("Invalid JSON in scenario file: %s" % err)

    if not isinstance(raw_scenario, dict):
        raise ValueError("Scenario must be a JSON object, not array or string")

    if 'part_power' not in raw_scenario:
        raise ValueError("part_power missing")
    try:
        parsed_scenario['part_power'] = int(raw_scenario['part_power'])
    except ValueError as err:
        raise ValueError("part_power not an integer: %s" % err)
    if not 1 <= parsed_scenario['part_power'] <= 32:
        raise ValueError("part_power must be between 1 and 32, but was %d"
                         % raw_scenario['part_power'])

    if 'replicas' not in raw_scenario:
        raise ValueError("replicas missing")
    try:
        parsed_scenario['replicas'] = float(raw_scenario['replicas'])
    except ValueError as err:
        raise ValueError("replicas not a float: %s" % err)
    if parsed_scenario['replicas'] < 1:
        raise ValueError("replicas must be at least 1, but is %f"
                         % parsed_scenario['replicas'])

    if 'overload' not in raw_scenario:
        raise ValueError("overload missing")
    try:
        parsed_scenario['overload'] = float(raw_scenario['overload'])
    except ValueError as err:
        raise ValueError("overload not a float: %s" % err)
    if parsed_scenario['overload'] < 0:
        raise ValueError("overload must be non-negative, but is %f"
                         % parsed_scenario['overload'])

    if 'random_seed' not in raw_scenario:
        raise ValueError("random_seed missing")
    try:
        parsed_scenario['random_seed'] = int(raw_scenario['random_seed'])
    except ValueError as err:
        raise ValueError("replicas not an integer: %s" % err)

    if 'rounds' not in raw_scenario:
        raise ValueError("rounds missing")
    if not isinstance(raw_scenario['rounds'], list):
        raise ValueError("rounds must be an array")

    parser_for_command = {
        'add': _parse_add_command,
        'remove': _parse_remove_command,
        'set_weight': _parse_set_weight_command,
        'save': _parse_save_command,
    }

    parsed_scenario['rounds'] = []
    for round_index, raw_round in enumerate(raw_scenario['rounds']):
        if not isinstance(raw_round, list):
            raise ValueError("round %d not an array" % round_index)

        parsed_round = []
        for command_index, command in enumerate(raw_round):
            if command[0] not in parser_for_command:
                raise ValueError(
                    "Unknown command (round %d, command %d): "
                    "'%s' should be one of %s" %
                    (round_index, command_index, command[0],
                     parser_for_command.keys()))
            parsed_round.append(
                parser_for_command[command[0]](
                    round_index, command_index, command))
        parsed_scenario['rounds'].append(parsed_round)
    return parsed_scenario


def run_scenario(scenario):
    """
    Takes a parsed scenario (like from parse_scenario()) and runs it.
    """
    seed = scenario['random_seed']

    rb = builder.RingBuilder(scenario['part_power'], scenario['replicas'], 1)
    rb.set_overload(scenario['overload'])

    command_map = {
        'add': rb.add_dev,
        'remove': rb.remove_dev,
        'set_weight': rb.set_dev_weight,
        'save': rb.save,
    }

    for round_index, commands in enumerate(scenario['rounds']):
        print("Round %d" % (round_index + 1))

        for command in commands:
            key = command.pop(0)
            try:
                command_f = command_map[key]
            except KeyError:
                raise ValueError("unknown command %r" % key)
            command_f(*command)

        rebalance_number = 1
        parts_moved, old_balance, removed_devs = rb.rebalance(seed=seed)
        rb.pretend_min_part_hours_passed()
        print("\tRebalance 1: moved %d parts, balance is %.6f, %d removed "
              "devs" % (parts_moved, old_balance, removed_devs))

        while True:
            rebalance_number += 1
            parts_moved, new_balance, removed_devs = rb.rebalance(seed=seed)
            rb.pretend_min_part_hours_passed()
            print("\tRebalance %d: moved %d parts, balance is %.6f, "
                  "%d removed devs" % (rebalance_number, parts_moved,
                                       new_balance, removed_devs))
            if parts_moved == 0 and removed_devs == 0:
                break
            if abs(new_balance - old_balance) < 1 and not (
                    old_balance == builder.MAX_BALANCE and
                    new_balance == builder.MAX_BALANCE):
                break
            old_balance = new_balance


def main(argv=None):
    args = ARG_PARSER.parse_args(argv)

    try:
        with open(args.scenario_path) as sfh:
            scenario_data = sfh.read()
    except OSError as err:
        sys.stderr.write("Error opening scenario %s: %s\n" %
                         (args.scenario_path, err))
        return 1

    try:
        scenario = parse_scenario(scenario_data)
    except ValueError as err:
        sys.stderr.write("Invalid scenario %s: %s\n" %
                         (args.scenario_path, err))
        return 1

    if not args.check:
        run_scenario(scenario)
    return 0
