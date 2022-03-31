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

import sys
import unittest
from collections import defaultdict

from swift.common import exceptions
from swift.common import ring
from swift.common.ring.utils import (tiers_for_dev, build_tier_tree,
                                     validate_and_normalize_ip,
                                     validate_and_normalize_address,
                                     is_valid_hostname,
                                     is_local_device, parse_search_value,
                                     parse_search_values_from_opts,
                                     parse_change_values_from_opts,
                                     validate_args, parse_args,
                                     parse_builder_ring_filename_args,
                                     build_dev_from_opts, dispersion_report,
                                     parse_address, get_tier_name, pretty_dev,
                                     validate_replicas_by_tier)


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.test_dev = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                         'port': '6200', 'id': 0}

        def get_test_devs():
            dev0 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6200', 'id': 0}
            dev1 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6200', 'id': 1}
            dev2 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6200', 'id': 2}
            dev3 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6200', 'id': 3}
            dev4 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6200', 'id': 4}
            dev5 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6200', 'id': 5}
            dev6 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6200', 'id': 6}
            dev7 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6200', 'id': 7}
            dev8 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6200', 'id': 8}
            dev9 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                    'port': '6200', 'id': 9}
            dev10 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                     'port': '6200', 'id': 10}
            dev11 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                     'port': '6200', 'id': 11}
            return [dev0, dev1, dev2, dev3, dev4, dev5,
                    dev6, dev7, dev8, dev9, dev10, dev11]

        self.test_devs = get_test_devs()

    def test_tiers_for_dev(self):
        self.assertEqual(
            tiers_for_dev(self.test_dev),
            ((1,),
             (1, 1),
             (1, 1, '192.168.1.1'),
             (1, 1, '192.168.1.1', 0)))

    def test_build_tier_tree(self):
        ret = build_tier_tree(self.test_devs)
        self.assertEqual(len(ret), 8)
        self.assertEqual(ret[()], set([(1,)]))
        self.assertEqual(ret[(1,)], set([(1, 1), (1, 2)]))
        self.assertEqual(ret[(1, 1)],
                         set([(1, 1, '192.168.1.2'),
                              (1, 1, '192.168.1.1')]))
        self.assertEqual(ret[(1, 2)],
                         set([(1, 2, '192.168.2.2'),
                              (1, 2, '192.168.2.1')]))
        self.assertEqual(ret[(1, 1, '192.168.1.1')],
                         set([(1, 1, '192.168.1.1', 0),
                              (1, 1, '192.168.1.1', 1),
                              (1, 1, '192.168.1.1', 2)]))
        self.assertEqual(ret[(1, 1, '192.168.1.2')],
                         set([(1, 1, '192.168.1.2', 3),
                              (1, 1, '192.168.1.2', 4),
                              (1, 1, '192.168.1.2', 5)]))
        self.assertEqual(ret[(1, 2, '192.168.2.1')],
                         set([(1, 2, '192.168.2.1', 6),
                              (1, 2, '192.168.2.1', 7),
                              (1, 2, '192.168.2.1', 8)]))
        self.assertEqual(ret[(1, 2, '192.168.2.2')],
                         set([(1, 2, '192.168.2.2', 9),
                              (1, 2, '192.168.2.2', 10),
                              (1, 2, '192.168.2.2', 11)]))

    def test_is_valid_hostname(self):
        self.assertTrue(is_valid_hostname("local"))
        self.assertTrue(is_valid_hostname("test.test.com"))
        hostname = "test." * 51
        self.assertTrue(is_valid_hostname(hostname))
        hostname = hostname.rstrip('.')
        self.assertTrue(is_valid_hostname(hostname))
        hostname = hostname + "00"
        self.assertFalse(is_valid_hostname(hostname))
        self.assertFalse(is_valid_hostname("$blah#"))

    def test_is_local_device(self):
        # localhost shows up in whataremyips() output as "::1" for IPv6
        my_ips = ["127.0.0.1", "::1"]
        my_port = 6200
        self.assertTrue(is_local_device(my_ips, my_port,
                                        "127.0.0.1", my_port))
        self.assertTrue(is_local_device(my_ips, my_port,
                                        "::1", my_port))
        self.assertTrue(is_local_device(
            my_ips, my_port,
            "0000:0000:0000:0000:0000:0000:0000:0001", my_port))
        self.assertTrue(is_local_device(my_ips, my_port,
                                        "localhost", my_port))
        self.assertFalse(is_local_device(my_ips, my_port,
                                         "localhost", my_port + 1))
        self.assertFalse(is_local_device(my_ips, my_port,
                                         "127.0.0.2", my_port))
        # for those that don't have a local port
        self.assertTrue(is_local_device(my_ips, None,
                                        my_ips[0], None))

        # When servers_per_port is active, the "my_port" passed in is None
        # which means "don't include port in the determination of locality
        # because it's not reliable in this deployment scenario"
        self.assertTrue(is_local_device(my_ips, None,
                                        "127.0.0.1", 6666))
        self.assertTrue(is_local_device(my_ips, None,
                                        "::1", 6666))
        self.assertTrue(is_local_device(
            my_ips, None,
            "0000:0000:0000:0000:0000:0000:0000:0001", 6666))
        self.assertTrue(is_local_device(my_ips, None,
                                        "localhost", 6666))
        self.assertFalse(is_local_device(my_ips, None,
                                         "127.0.0.2", my_port))

    def test_validate_and_normalize_ip(self):
        ipv4 = "10.0.0.1"
        self.assertEqual(ipv4, validate_and_normalize_ip(ipv4))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertEqual(ipv6, validate_and_normalize_ip(ipv6.upper()))
        hostname = "test.test.com"
        self.assertRaises(ValueError,
                          validate_and_normalize_ip, hostname)
        hostname = "$blah#"
        self.assertRaises(ValueError,
                          validate_and_normalize_ip, hostname)

    def test_validate_and_normalize_address(self):
        ipv4 = "10.0.0.1"
        self.assertEqual(ipv4, validate_and_normalize_address(ipv4))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertEqual(ipv6, validate_and_normalize_address(ipv6.upper()))
        hostname = "test.test.com"
        self.assertEqual(hostname,
                         validate_and_normalize_address(hostname.upper()))
        hostname = "$blah#"
        self.assertRaises(ValueError,
                          validate_and_normalize_address, hostname)

    def test_validate_replicas_by_tier_close(self):
        one_ip_six_devices = \
            defaultdict(float,
                        {(): 4.0,
                         (0,): 4.0,
                         (0, 0): 4.0,
                         (0, 0, '127.0.0.1'): 4.0,
                         (0, 0, '127.0.0.1', 0): 0.6666666670,
                         (0, 0, '127.0.0.1', 1): 0.6666666668,
                         (0, 0, '127.0.0.1', 2): 0.6666666667,
                         (0, 0, '127.0.0.1', 3): 0.6666666666,
                         (0, 0, '127.0.0.1', 4): 0.6666666665,
                         (0, 0, '127.0.0.1', 5): 0.6666666664,
                         })
        try:
            validate_replicas_by_tier(4, one_ip_six_devices)
        except Exception as e:
            self.fail('one_ip_six_devices is invalid for %s' % e)

    def test_validate_replicas_by_tier_exact(self):
        three_regions_three_devices = \
            defaultdict(float,
                        {(): 3.0,
                         (0,): 1.0,
                         (0, 0): 1.0,
                         (0, 0, '127.0.0.1'): 1.0,
                         (0, 0, '127.0.0.1', 0): 1.0,
                         (1,): 1.0,
                         (1, 1): 1.0,
                         (1, 1, '127.0.0.1'): 1.0,
                         (1, 1, '127.0.0.1', 1): 1.0,
                         (2,): 1.0,
                         (2, 2): 1.0,
                         (2, 2, '127.0.0.1'): 1.0,
                         (2, 2, '127.0.0.1', 2): 1.0,
                         })
        try:
            validate_replicas_by_tier(3, three_regions_three_devices)
        except Exception as e:
            self.fail('three_regions_three_devices is invalid for %s' % e)

    def test_validate_replicas_by_tier_errors(self):
        pseudo_replicas = \
            defaultdict(float,
                        {(): 3.0,
                         (0,): 1.0,
                         (0, 0): 1.0,
                         (0, 0, '127.0.0.1'): 1.0,
                         (0, 0, '127.0.0.1', 0): 1.0,
                         (1,): 1.0,
                         (1, 1): 1.0,
                         (1, 1, '127.0.0.1'): 1.0,
                         (1, 1, '127.0.0.1', 1): 1.0,
                         (2,): 1.0,
                         (2, 2): 1.0,
                         (2, 2, '127.0.0.1'): 1.0,
                         (2, 2, '127.0.0.1', 2): 1.0,
                         })

        def do_test(bad_tier_key, bad_tier_name):
            # invalidate a copy of pseudo_replicas at given key and check for
            # an exception to be raised
            test_replicas = dict(pseudo_replicas)
            test_replicas[bad_tier_key] += 0.1  # <- this is not fair!
            with self.assertRaises(exceptions.RingValidationError) as ctx:
                validate_replicas_by_tier(3, test_replicas)
            self.assertEqual(
                '3.1 != 3 at tier %s' % bad_tier_name, str(ctx.exception))

        do_test((), 'cluster')
        do_test((1,), 'regions')
        do_test((0, 0), 'zones')
        do_test((2, 2, '127.0.0.1'), 'servers')
        do_test((1, 1, '127.0.0.1', 1), 'devices')

    def test_parse_search_value(self):
        res = parse_search_value('r0')
        self.assertEqual(res, {'region': 0})
        res = parse_search_value('r1')
        self.assertEqual(res, {'region': 1})
        res = parse_search_value('r1z2')
        self.assertEqual(res, {'region': 1, 'zone': 2})
        res = parse_search_value('d1')
        self.assertEqual(res, {'id': 1})
        res = parse_search_value('z1')
        self.assertEqual(res, {'zone': 1})
        res = parse_search_value('-127.0.0.1')
        self.assertEqual(res, {'ip': '127.0.0.1'})
        res = parse_search_value('127.0.0.1')
        self.assertEqual(res, {'ip': '127.0.0.1'})
        res = parse_search_value('-[127.0.0.1]:10001')
        self.assertEqual(res, {'ip': '127.0.0.1', 'port': 10001})
        res = parse_search_value(':10001')
        self.assertEqual(res, {'port': 10001})
        res = parse_search_value('R127.0.0.10')
        self.assertEqual(res, {'replication_ip': '127.0.0.10'})
        res = parse_search_value('R[127.0.0.10]:20000')
        self.assertEqual(res, {'replication_ip': '127.0.0.10',
                               'replication_port': 20000})
        res = parse_search_value('R:20000')
        self.assertEqual(res, {'replication_port': 20000})
        res = parse_search_value('/sdb1')
        self.assertEqual(res, {'device': 'sdb1'})
        res = parse_search_value('_meta1')
        self.assertEqual(res, {'meta': 'meta1'})
        self.assertRaises(ValueError, parse_search_value, 'OMGPONIES')

    def test_parse_search_values_from_opts(self):
        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6201",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'id': 1,
            'region': 2,
            'zone': 3,
            'ip': "test.test.com",
            'port': 6200,
            'replication_ip': "r.test.com",
            'replication_port': 7000,
            'device': "sda3",
            'meta': "some meta data",
            'weight': 3.14159265359,
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_search_values_from_opts(opts)
        self.assertEqual(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "127.0.0.1",
             "--port", "6200",
             "--replication-ip", "127.0.0.10",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "127.0.0.2",
             "--change-port", "6201",
             "--change-replication-ip", "127.0.0.20",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'id': 1,
            'region': 2,
            'zone': 3,
            'ip': "127.0.0.1",
            'port': 6200,
            'replication_ip': "127.0.0.10",
            'replication_port': 7000,
            'device': "sda3",
            'meta': "some meta data",
            'weight': 3.14159265359,
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_search_values_from_opts(opts)
        self.assertEqual(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "[127.0.0.1]",
             "--port", "6200",
             "--replication-ip", "[127.0.0.10]",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "[127.0.0.2]",
             "--change-port", "6201",
             "--change-replication-ip", "[127.0.0.20]",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_search_values_from_opts(opts)
        self.assertEqual(search_values, expected)

    def test_parse_change_values_from_opts(self):
        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6201",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'ip': "change.test.test.com",
            'port': 6201,
            'replication_ip': "change.r.test.com",
            'replication_port': 7001,
            'device': "sdb3",
            'meta': "some meta data for change",
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_change_values_from_opts(opts)
        self.assertEqual(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "127.0.0.1",
             "--port", "6200",
             "--replication-ip", "127.0.0.10",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "127.0.0.2",
             "--change-port", "6201",
             "--change-replication-ip", "127.0.0.20",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'ip': "127.0.0.2",
            'port': 6201,
            'replication_ip': "127.0.0.20",
            'replication_port': 7001,
            'device': "sdb3",
            'meta': "some meta data for change",
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_change_values_from_opts(opts)
        self.assertEqual(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "[127.0.0.1]",
             "--port", "6200",
             "--replication-ip", "[127.0.0.10]",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "[127.0.0.2]",
             "--change-port", "6201",
             "--change-replication-ip", "[127.0.0.20]",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_change_values_from_opts(opts)
        self.assertEqual(search_values, expected)

    def test_validate_args(self):
        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6201",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertTrue(new_cmd_format)
        self.assertEqual(opts.id, 1)
        self.assertEqual(opts.region, 2)
        self.assertEqual(opts.zone, 3)
        self.assertEqual(opts.ip, "test.test.com")
        self.assertEqual(opts.port, 6200)
        self.assertEqual(opts.replication_ip, "r.test.com")
        self.assertEqual(opts.replication_port, 7000)
        self.assertEqual(opts.device, "sda3")
        self.assertEqual(opts.meta, "some meta data")
        self.assertEqual(opts.weight, 3.14159265359)
        self.assertEqual(opts.change_ip, "change.test.test.com")
        self.assertEqual(opts.change_port, 6201)
        self.assertEqual(opts.change_replication_ip, "change.r.test.com")
        self.assertEqual(opts.change_replication_port, 7001)
        self.assertEqual(opts.change_device, "sdb3")
        self.assertEqual(opts.change_meta, "some meta data for change")

    def test_validate_args_new_cmd_format(self):
        argv = \
            ["--id", "0", "--region", "0", "--zone", "0",
             "--ip", "",
             "--port", "0",
             "--replication-ip", "",
             "--replication-port", "0",
             "--device", "",
             "--meta", "",
             "--weight", "0",
             "--change-ip", "",
             "--change-port", "0",
             "--change-replication-ip", "",
             "--change-replication-port", "0",
             "--change-device", "",
             "--change-meta", ""]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertTrue(new_cmd_format)

        argv = \
            ["--id", None, "--region", None, "--zone", None,
             "--ip", "",
             "--port", "0",
             "--replication-ip", "",
             "--replication-port", "0",
             "--device", "",
             "--meta", "",
             "--weight", None,
             "--change-ip", "change.test.test.com",
             "--change-port", "6201",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertFalse(new_cmd_format)

        argv = \
            ["--id", "0"]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertTrue(new_cmd_format)
        argv = \
            ["--region", "0"]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertTrue(new_cmd_format)
        argv = \
            ["--zone", "0"]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertTrue(new_cmd_format)
        argv = \
            ["--weight", "0"]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertTrue(new_cmd_format)

    def test_parse_args(self):
        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6201",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]

        opts, args = parse_args(argv)
        self.assertEqual(opts.id, 1)
        self.assertEqual(opts.region, 2)
        self.assertEqual(opts.zone, 3)
        self.assertEqual(opts.ip, "test.test.com")
        self.assertEqual(opts.port, 6200)
        self.assertEqual(opts.replication_ip, "r.test.com")
        self.assertEqual(opts.replication_port, 7000)
        self.assertEqual(opts.device, "sda3")
        self.assertEqual(opts.meta, "some meta data")
        self.assertEqual(opts.weight, 3.14159265359)
        self.assertEqual(opts.change_ip, "change.test.test.com")
        self.assertEqual(opts.change_port, 6201)
        self.assertEqual(opts.change_replication_ip, "change.r.test.com")
        self.assertEqual(opts.change_replication_port, 7001)
        self.assertEqual(opts.change_device, "sdb3")
        self.assertEqual(opts.change_meta, "some meta data for change")
        self.assertEqual(len(args), 0)

    def test_parse_builder_ring_filename_args(self):
        args = 'swift-ring-builder object.builder write_ring'
        self.assertEqual((
            'object.builder', 'object.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))
        args = 'swift-ring-builder container.ring.gz write_builder'
        self.assertEqual((
            'container.builder', 'container.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))
        # builder name arg should always fall through
        args = 'swift-ring-builder test create'
        self.assertEqual((
            'test', 'test.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))
        args = 'swift-ring-builder my.file.name create'
        self.assertEqual((
            'my.file.name', 'my.file.name.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))

    def test_build_dev_from_opts(self):
        argv = \
            ["--region", "0", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359"]
        expected = {
            'region': 0,
            'zone': 3,
            'ip': "test.test.com",
            'port': 6200,
            'replication_ip': "r.test.com",
            'replication_port': 7000,
            'device': "sda3",
            'meta': "some meta data",
            'weight': 3.14159265359,
        }
        opts, args = parse_args(argv)
        device = build_dev_from_opts(opts)
        self.assertEqual(device, expected)

        argv = \
            ["--region", "2", "--zone", "3",
             "--ip", "[test.test.com]",
             "--port", "6200",
             "--replication-ip", "[r.test.com]",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359"]
        opts, args = parse_args(argv)
        self.assertRaises(ValueError, build_dev_from_opts, opts)

        argv = \
            ["--region", "2", "--zone", "3",
             "--ip", "[test.test.com]",
             "--port", "6200",
             "--replication-ip", "[r.test.com]",
             "--replication-port", "7000",
             "--meta", "some meta data",
             "--weight", "3.14159265359"]
        opts, args = parse_args(argv)
        self.assertRaises(ValueError, build_dev_from_opts, opts)

    def test_replication_defaults(self):
        args = '-r 1 -z 1 -i 127.0.0.1 -p 6010 -d d1 -w 100'.split()
        opts, _ = parse_args(args)
        device = build_dev_from_opts(opts)
        expected = {
            'device': 'd1',
            'ip': '127.0.0.1',
            'meta': '',
            'port': 6010,
            'region': 1,
            'replication_ip': '127.0.0.1',
            'replication_port': 6010,
            'weight': 100.0,
            'zone': 1,
        }
        self.assertEqual(device, expected)

        args = '-r 1 -z 1 -i test.com -p 6010 -d d1 -w 100'.split()
        opts, _ = parse_args(args)
        device = build_dev_from_opts(opts)
        expected = {
            'device': 'd1',
            'ip': 'test.com',
            'meta': '',
            'port': 6010,
            'region': 1,
            'replication_ip': 'test.com',
            'replication_port': 6010,
            'weight': 100.0,
            'zone': 1,
        }
        self.assertEqual(device, expected)

    @unittest.skipIf(sys.version_info < (3,),
                     "Seed-specific tests don't work well between python "
                     "versions. This test is now PY3 only")
    def test_dispersion_report(self):
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 4, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdc1'})
        rb.add_dev({'id': 5, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.0', 'port': 10000, 'device': 'sdd1'})

        rb.add_dev({'id': 1, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 6, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1'})
        rb.add_dev({'id': 7, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdc1'})
        rb.add_dev({'id': 8, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdd1'})

        rb.add_dev({'id': 2, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 9, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sdb1'})
        rb.add_dev({'id': 10, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sdc1'})
        rb.add_dev({'id': 11, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.2', 'port': 10002, 'device': 'sdd1'})

        # this ring is pretty volatile and the assertions are pretty brittle
        # so we use a specific seed
        rb.rebalance(seed=100)
        rb.validate()

        self.assertEqual(rb.dispersion, 16.796875)
        report = dispersion_report(rb)
        self.assertEqual(report['worst_tier'], 'r1z1-127.0.0.1')
        self.assertEqual(report['max_dispersion'], 20.967741935483872)

        def build_tier_report(max_replicas, placed_parts, dispersion,
                              replicas):
            return {
                'max_replicas': max_replicas,
                'placed_parts': placed_parts,
                'dispersion': dispersion,
                'replicas': replicas,
            }

        # every partition has at least two replicas in this zone, unfortunately
        # sometimes they're both on the same server.
        expected = [
            ['r1z1', build_tier_report(
                2, 621, 17.55233494363929, [0, 0, 147, 109])],
            ['r1z1-127.0.0.1', build_tier_report(
                1, 310, 20.967741935483872, [11, 180, 65, 0])],
            ['r1z1-127.0.0.2', build_tier_report(
                1, 311, 20.578778135048232, [9, 183, 64, 0])],
        ]
        report = dispersion_report(rb, 'r1z1[^/]*$', verbose=True)
        graph = report['graph']
        for i, (expected_key, expected_report) in enumerate(expected):
            key, report = graph[i]
            self.assertEqual(
                (key, report),
                (expected_key, expected_report)
            )

        # overcompensate in r1z0
        rb.add_dev({'id': 12, 'region': 1, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.3', 'port': 10003, 'device': 'sda1'})
        rb.add_dev({'id': 13, 'region': 1, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.3', 'port': 10003, 'device': 'sdb1'})
        rb.add_dev({'id': 14, 'region': 1, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.3', 'port': 10003, 'device': 'sdc1'})
        rb.add_dev({'id': 15, 'region': 1, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.3', 'port': 10003, 'device': 'sdd1'})

        # when the biggest tier has the smallest devices things get ugly
        # can't move all the part-replicas in one rebalance
        rb.rebalance(seed=100)
        report = dispersion_report(rb, verbose=True)
        self.assertEqual(rb.dispersion, 2.8645833333333335)
        self.assertEqual(report['worst_tier'], 'r1z1-127.0.0.1')
        self.assertEqual(report['max_dispersion'], 6.593406593406593)
        # do a sencond rebalance
        rb.rebalance(seed=100)
        report = dispersion_report(rb, verbose=True)
        self.assertEqual(rb.dispersion, 16.666666666666668)
        self.assertEqual(report['worst_tier'], 'r1z0-127.0.0.3')
        self.assertEqual(report['max_dispersion'], 33.333333333333336)

        # ... but overload can square it
        rb.set_overload(rb.get_required_overload())
        rb.rebalance()
        self.assertEqual(rb.dispersion, 0.0)

    def test_parse_address_old_format(self):
        # Test old format
        argv = "127.0.0.1:6200R127.0.0.1:6200/sda1_some meta data"
        ip, port, rest = parse_address(argv)
        self.assertEqual(ip, '127.0.0.1')
        self.assertEqual(port, 6200)
        self.assertEqual(rest, 'R127.0.0.1:6200/sda1_some meta data')

    def test_normalized_device_tier_names(self):
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({
            'region': 1,
            'zone': 1,
            'ip': '127.0.0.1',
            'port': 6011,
            'device': 'd1',
            'weight': 0.0,
        })
        dev = rb.devs[0]
        expected = 'r1z1-127.0.0.1/d1'
        self.assertEqual(expected, get_tier_name(tiers_for_dev(dev)[-1], rb))
        self.assertEqual(expected, pretty_dev(dev))


if __name__ == '__main__':
    unittest.main()
