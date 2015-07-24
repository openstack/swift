#! /usr/bin/env python
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

import json
import mock
import unittest
from StringIO import StringIO

from swift.cli.ring_builder_analyzer import parse_scenario, run_scenario


class TestRunScenario(unittest.TestCase):
    def test_it_runs(self):
        scenario = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0,
            'rounds': [[['add', 'r1z2-3.4.5.6:7/sda8', 100],
                        ['add', 'z2-3.4.5.6:7/sda9', 200]],
                       [['set_weight', 0, 150]],
                       [['remove', 1]]]}
        parsed = parse_scenario(json.dumps(scenario))

        fake_stdout = StringIO()
        with mock.patch('sys.stdout', fake_stdout):
            run_scenario(parsed)

        # Just test that it produced some output as it ran; the fact that
        # this doesn't crash and produces output that resembles something
        # useful is good enough.
        self.assertTrue('Rebalance' in fake_stdout.getvalue())


class TestParseScenario(unittest.TestCase):
    def test_good(self):
        scenario = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0,
            'rounds': [[['add', 'r1z2-3.4.5.6:7/sda8', 100],
                        ['add', 'z2-3.4.5.6:7/sda9', 200]],
                       [['set_weight', 0, 150]],
                       [['remove', 1]]]}
        parsed = parse_scenario(json.dumps(scenario))

        self.assertEqual(parsed['replicas'], 3)
        self.assertEqual(parsed['part_power'], 8)
        self.assertEqual(parsed['random_seed'], 123)
        self.assertEqual(parsed['overload'], 0)
        self.assertEqual(parsed['rounds'], [
            [['add', {'device': 'sda8',
                      'ip': '3.4.5.6',
                      'meta': '',
                      'port': 7,
                      'region': 1,
                      'replication_ip': None,
                      'replication_port': None,
                      'weight': 100.0,
                      'zone': 2}],
             ['add', {'device': u'sda9',
                      'ip': u'3.4.5.6',
                      'meta': '',
                      'port': 7,
                      'region': 1,
                      'replication_ip': None,
                      'replication_port': None,
                      'weight': 200.0,
                      'zone': 2}]],
            [['set_weight', 0, 150.0]],
            [['remove', 1]]])

    # The rest of this test class is just a catalog of the myriad ways that
    # the input can be malformed.
    def test_invalid_json(self):
        self.assertRaises(ValueError, parse_scenario, "{")

    def test_json_not_object(self):
        self.assertRaises(ValueError, parse_scenario, "[]")
        self.assertRaises(ValueError, parse_scenario, "\"stuff\"")

    def test_bad_replicas(self):
        working_scenario = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0,
            'rounds': [[['add', 'r1z2-3.4.5.6:7/sda8', 100]]]}

        busted = dict(working_scenario)
        del busted['replicas']
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, replicas='blahblah')
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, replicas=-1)
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

    def test_bad_part_power(self):
        working_scenario = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0,
            'rounds': [[['add', 'r1z2-3.4.5.6:7/sda8', 100]]]}

        busted = dict(working_scenario)
        del busted['part_power']
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, part_power='blahblah')
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, part_power=0)
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, part_power=33)
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

    def test_bad_random_seed(self):
        working_scenario = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0,
            'rounds': [[['add', 'r1z2-3.4.5.6:7/sda8', 100]]]}

        busted = dict(working_scenario)
        del busted['random_seed']
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, random_seed='blahblah')
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

    def test_bad_overload(self):
        working_scenario = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0,
            'rounds': [[['add', 'r1z2-3.4.5.6:7/sda8', 100]]]}

        busted = dict(working_scenario)
        del busted['overload']
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, overload='blahblah')
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(working_scenario, overload=-0.01)
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

    def test_bad_rounds(self):
        base = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0}

        self.assertRaises(ValueError, parse_scenario, json.dumps(base))

        busted = dict(base, rounds={})
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(base, rounds=[{}])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        busted = dict(base, rounds=[[['bork']]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

    def test_bad_add(self):
        base = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0}

        # no dev
        busted = dict(base, rounds=[[['add']]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # no weight
        busted = dict(base, rounds=[[['add', 'r1z2-1.2.3.4:6000/d7']]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # too many fields
        busted = dict(base, rounds=[[['add', 'r1z2-1.2.3.4:6000/d7', 1, 2]]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # can't parse
        busted = dict(base, rounds=[[['add', 'not a good value', 100]]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # negative weight
        busted = dict(base, rounds=[[['add', 'r1z2-1.2.3.4:6000/d7', -1]]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

    def test_bad_remove(self):
        base = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0}

        # no dev
        busted = dict(base, rounds=[[['remove']]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # bad dev id
        busted = dict(base, rounds=[[['remove', 'not an int']]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # too many fields
        busted = dict(base, rounds=[[['remove', 1, 2]]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

    def test_bad_set_weight(self):
        base = {
            'replicas': 3, 'part_power': 8, 'random_seed': 123, 'overload': 0}

        # no dev
        busted = dict(base, rounds=[[['set_weight']]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # no weight
        busted = dict(base, rounds=[[['set_weight', 0]]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # bad dev id
        busted = dict(base, rounds=[[['set_weight', 'not an int', 90]]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # negative weight
        busted = dict(base, rounds=[[['set_weight', 1, -1]]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))

        # bogus weight
        busted = dict(base, rounds=[[['set_weight', 1, 'bogus']]])
        self.assertRaises(ValueError, parse_scenario, json.dumps(busted))
