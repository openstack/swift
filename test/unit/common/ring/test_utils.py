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

import unittest

from swift.common import ring
from swift.common.ring.utils import (tiers_for_dev, build_tier_tree,
                                     validate_and_normalize_ip,
                                     validate_and_normalize_address,
                                     is_valid_ip, is_valid_ipv4,
                                     is_valid_ipv6, is_valid_hostname,
                                     is_local_device, parse_search_value,
                                     parse_search_values_from_opts,
                                     parse_change_values_from_opts,
                                     validate_args, parse_args,
                                     parse_builder_ring_filename_args,
                                     build_dev_from_opts, dispersion_report)


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.test_dev = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                         'port': '6000', 'id': 0}

        def get_test_devs():
            dev0 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6000', 'id': 0}
            dev1 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6000', 'id': 1}
            dev2 = {'region': 1, 'zone': 1, 'ip': '192.168.1.1',
                    'port': '6000', 'id': 2}
            dev3 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6000', 'id': 3}
            dev4 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6000', 'id': 4}
            dev5 = {'region': 1, 'zone': 1, 'ip': '192.168.1.2',
                    'port': '6000', 'id': 5}
            dev6 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6000', 'id': 6}
            dev7 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6000', 'id': 7}
            dev8 = {'region': 1, 'zone': 2, 'ip': '192.168.2.1',
                    'port': '6000', 'id': 8}
            dev9 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                    'port': '6000', 'id': 9}
            dev10 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                     'port': '6000', 'id': 10}
            dev11 = {'region': 1, 'zone': 2, 'ip': '192.168.2.2',
                     'port': '6000', 'id': 11}
            return [dev0, dev1, dev2, dev3, dev4, dev5,
                    dev6, dev7, dev8, dev9, dev10, dev11]

        self.test_devs = get_test_devs()

    def test_tiers_for_dev(self):
        self.assertEqual(
            tiers_for_dev(self.test_dev),
            ((1,),
             (1, 1),
             (1, 1, '192.168.1.1:6000'),
             (1, 1, '192.168.1.1:6000', 0)))

    def test_build_tier_tree(self):
        ret = build_tier_tree(self.test_devs)
        self.assertEqual(len(ret), 8)
        self.assertEqual(ret[()], set([(1,)]))
        self.assertEqual(ret[(1,)], set([(1, 1), (1, 2)]))
        self.assertEqual(ret[(1, 1)],
                         set([(1, 1, '192.168.1.2:6000'),
                              (1, 1, '192.168.1.1:6000')]))
        self.assertEqual(ret[(1, 2)],
                         set([(1, 2, '192.168.2.2:6000'),
                              (1, 2, '192.168.2.1:6000')]))
        self.assertEqual(ret[(1, 1, '192.168.1.1:6000')],
                         set([(1, 1, '192.168.1.1:6000', 0),
                              (1, 1, '192.168.1.1:6000', 1),
                              (1, 1, '192.168.1.1:6000', 2)]))
        self.assertEqual(ret[(1, 1, '192.168.1.2:6000')],
                         set([(1, 1, '192.168.1.2:6000', 3),
                              (1, 1, '192.168.1.2:6000', 4),
                              (1, 1, '192.168.1.2:6000', 5)]))
        self.assertEqual(ret[(1, 2, '192.168.2.1:6000')],
                         set([(1, 2, '192.168.2.1:6000', 6),
                              (1, 2, '192.168.2.1:6000', 7),
                              (1, 2, '192.168.2.1:6000', 8)]))
        self.assertEqual(ret[(1, 2, '192.168.2.2:6000')],
                         set([(1, 2, '192.168.2.2:6000', 9),
                              (1, 2, '192.168.2.2:6000', 10),
                              (1, 2, '192.168.2.2:6000', 11)]))

    def test_is_valid_ip(self):
        self.assertTrue(is_valid_ip("127.0.0.1"))
        self.assertTrue(is_valid_ip("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80::"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "::1"
        self.assertTrue(is_valid_ip(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(is_valid_ip(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(is_valid_ip(not_ipv6))

    def test_is_valid_ipv4(self):
        self.assertTrue(is_valid_ipv4("127.0.0.1"))
        self.assertTrue(is_valid_ipv4("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80::"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "::1"
        self.assertFalse(is_valid_ipv4(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(is_valid_ipv4(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(is_valid_ipv4(not_ipv6))

    def test_is_valid_ipv6(self):
        self.assertFalse(is_valid_ipv6("127.0.0.1"))
        self.assertFalse(is_valid_ipv6("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80::"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "::1"
        self.assertTrue(is_valid_ipv6(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(is_valid_ipv6(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(is_valid_ipv6(not_ipv6))

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
        my_ips = ["127.0.0.1",
                  "0000:0000:0000:0000:0000:0000:0000:0001"]
        my_port = 6000
        self.assertTrue(is_local_device(my_ips, my_port,
                                        "localhost",
                                        my_port))
        self.assertFalse(is_local_device(my_ips, my_port,
                                         "localhost",
                                         my_port + 1))
        self.assertFalse(is_local_device(my_ips, my_port,
                                         "127.0.0.2",
                                         my_port))
        # for those that don't have a local port
        self.assertTrue(is_local_device(my_ips, None,
                                        my_ips[0], None))

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
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6001",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'id': 1,
            'region': 2,
            'zone': 3,
            'ip': "test.test.com",
            'port': 6000,
            'replication_ip': "r.test.com",
            'replication_port': 7000,
            'device': "sda3",
            'meta': "some meta data",
            'weight': 3.14159265359,
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_search_values_from_opts(opts)
        self.assertEquals(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.10",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "127.0.0.2",
             "--change-port", "6001",
             "--change-replication-ip", "127.0.0.20",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'id': 1,
            'region': 2,
            'zone': 3,
            'ip': "127.0.0.1",
            'port': 6000,
            'replication_ip': "127.0.0.10",
            'replication_port': 7000,
            'device': "sda3",
            'meta': "some meta data",
            'weight': 3.14159265359,
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_search_values_from_opts(opts)
        self.assertEquals(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "[127.0.0.1]",
             "--port", "6000",
             "--replication-ip", "[127.0.0.10]",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "[127.0.0.2]",
             "--change-port", "6001",
             "--change-replication-ip", "[127.0.0.20]",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_search_values_from_opts(opts)
        self.assertEquals(search_values, expected)

    def test_parse_change_values_from_opts(self):
        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6001",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'ip': "change.test.test.com",
            'port': 6001,
            'replication_ip': "change.r.test.com",
            'replication_port': 7001,
            'device': "sdb3",
            'meta': "some meta data for change",
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_change_values_from_opts(opts)
        self.assertEquals(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.10",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "127.0.0.2",
             "--change-port", "6001",
             "--change-replication-ip", "127.0.0.20",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        expected = {
            'ip': "127.0.0.2",
            'port': 6001,
            'replication_ip': "127.0.0.20",
            'replication_port': 7001,
            'device': "sdb3",
            'meta': "some meta data for change",
        }
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_change_values_from_opts(opts)
        self.assertEquals(search_values, expected)

        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "[127.0.0.1]",
             "--port", "6000",
             "--replication-ip", "[127.0.0.10]",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "[127.0.0.2]",
             "--change-port", "6001",
             "--change-replication-ip", "[127.0.0.20]",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        new_cmd_format, opts, args = validate_args(argv)
        search_values = parse_change_values_from_opts(opts)
        self.assertEquals(search_values, expected)

    def test_validate_args(self):
        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6001",
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
        self.assertEqual(opts.port, 6000)
        self.assertEqual(opts.replication_ip, "r.test.com")
        self.assertEqual(opts.replication_port, 7000)
        self.assertEqual(opts.device, "sda3")
        self.assertEqual(opts.meta, "some meta data")
        self.assertEqual(opts.weight, 3.14159265359)
        self.assertEqual(opts.change_ip, "change.test.test.com")
        self.assertEqual(opts.change_port, 6001)
        self.assertEqual(opts.change_replication_ip, "change.r.test.com")
        self.assertEqual(opts.change_replication_port, 7001)
        self.assertEqual(opts.change_device, "sdb3")
        self.assertEqual(opts.change_meta, "some meta data for change")

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
        self.assertFalse(new_cmd_format)

        argv = \
            ["--id", "0", "--region", "0", "--zone", "0",
             "--ip", "",
             "--port", "0",
             "--replication-ip", "",
             "--replication-port", "0",
             "--device", "",
             "--meta", "",
             "--weight", "0",
             "--change-ip", "change.test.test.com",
             "--change-port", "6001",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]
        new_cmd_format, opts, args = validate_args(argv)
        self.assertFalse(new_cmd_format)

    def test_parse_args(self):
        argv = \
            ["--id", "1", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359",
             "--change-ip", "change.test.test.com",
             "--change-port", "6001",
             "--change-replication-ip", "change.r.test.com",
             "--change-replication-port", "7001",
             "--change-device", "sdb3",
             "--change-meta", "some meta data for change"]

        opts, args = parse_args(argv)
        self.assertEqual(opts.id, 1)
        self.assertEqual(opts.region, 2)
        self.assertEqual(opts.zone, 3)
        self.assertEqual(opts.ip, "test.test.com")
        self.assertEqual(opts.port, 6000)
        self.assertEqual(opts.replication_ip, "r.test.com")
        self.assertEqual(opts.replication_port, 7000)
        self.assertEqual(opts.device, "sda3")
        self.assertEqual(opts.meta, "some meta data")
        self.assertEqual(opts.weight, 3.14159265359)
        self.assertEqual(opts.change_ip, "change.test.test.com")
        self.assertEqual(opts.change_port, 6001)
        self.assertEqual(opts.change_replication_ip, "change.r.test.com")
        self.assertEqual(opts.change_replication_port, 7001)
        self.assertEqual(opts.change_device, "sdb3")
        self.assertEqual(opts.change_meta, "some meta data for change")
        self.assertEqual(len(args), 0)

    def test_parse_builder_ring_filename_args(self):
        args = 'swift-ring-builder object.builder write_ring'
        self.assertEquals((
            'object.builder', 'object.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))
        args = 'swift-ring-builder container.ring.gz write_builder'
        self.assertEquals((
            'container.builder', 'container.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))
        # builder name arg should always fall through
        args = 'swift-ring-builder test create'
        self.assertEquals((
            'test', 'test.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))
        args = 'swift-ring-builder my.file.name create'
        self.assertEquals((
            'my.file.name', 'my.file.name.ring.gz'
        ), parse_builder_ring_filename_args(args.split()))

    def test_build_dev_from_opts(self):
        argv = \
            ["--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3",
             "--meta", "some meta data",
             "--weight", "3.14159265359"]
        expected = {
            'region': 2,
            'zone': 3,
            'ip': "test.test.com",
            'port': 6000,
            'replication_ip': "r.test.com",
            'replication_port': 7000,
            'device': "sda3",
            'meta': "some meta data",
            'weight': 3.14159265359,
        }
        opts, args = parse_args(argv)
        device = build_dev_from_opts(opts)
        self.assertEquals(device, expected)

        argv = \
            ["--region", "2", "--zone", "3",
             "--ip", "[test.test.com]",
             "--port", "6000",
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
             "--port", "6000",
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
        self.assertEquals(device, expected)

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
        self.assertEquals(device, expected)

    def test_dispersion_report(self):
        rb = ring.RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 1, 'zone': 1, 'weight': 200,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.rebalance(seed=10)

        self.assertEqual(rb.dispersion, 39.84375)
        report = dispersion_report(rb)
        self.assertEqual(report['worst_tier'], 'r1z1')
        self.assertEqual(report['max_dispersion'], 39.84375)

        # Each node should store 256 partitions to avoid multiple replicas
        # 2/5 of total weight * 768 ~= 307 -> 51 partitions on each node in
        # zone 1 are stored at least twice on the nodes
        expected = [
            ['r1z1', 2, '0', '154', '102'],
            ['r1z1-127.0.0.1:10001', 1, '205', '51', '0'],
            ['r1z1-127.0.0.1:10001/sda1', 1, '205', '51', '0'],
            ['r1z1-127.0.0.1:10002', 1, '205', '51', '0'],
            ['r1z1-127.0.0.1:10002/sda1', 1, '205', '51', '0']]

        def build_tier_report(max_replicas, placed_parts, dispersion,
                              replicas):
            return {
                'max_replicas': max_replicas,
                'placed_parts': placed_parts,
                'dispersion': dispersion,
                'replicas': replicas,
            }
        expected = [
            ['r1z1', build_tier_report(
                2, 256, 39.84375, [0, 0, 154, 102])],
            ['r1z1-127.0.0.1:10001', build_tier_report(
                1, 256, 19.921875, [0, 205, 51, 0])],
            ['r1z1-127.0.0.1:10001/sda1', build_tier_report(
                1, 256, 19.921875, [0, 205, 51, 0])],
            ['r1z1-127.0.0.1:10002', build_tier_report(
                1, 256, 19.921875, [0, 205, 51, 0])],
            ['r1z1-127.0.0.1:10002/sda1', build_tier_report(
                1, 256, 19.921875, [0, 205, 51, 0])],
        ]
        report = dispersion_report(rb, 'r1z1.*', verbose=True)
        graph = report['graph']
        for i in range(len(expected)):
            self.assertEqual(expected[i][0], graph[i][0])
            self.assertEqual(expected[i][1], graph[i][1])

        # overcompensate in r1z0
        rb.add_dev({'id': 3, 'region': 1, 'zone': 0, 'weight': 500,
                    'ip': '127.0.0.1', 'port': 10003, 'device': 'sda1'})
        rb.rebalance(seed=10)

        report = dispersion_report(rb)
        self.assertEqual(rb.dispersion, 40.234375)
        self.assertEqual(report['worst_tier'], 'r1z0-127.0.0.1:10003')
        self.assertEqual(report['max_dispersion'], 30.078125)


if __name__ == '__main__':
    unittest.main()
