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

from mock import patch
import socket
import unittest

# Continue importing from utils, as 3rd parties may depend on those imports
from swift.common import utils
from swift.common.utils import ipaddrs as utils_ipaddrs


class TestIsValidIP(unittest.TestCase):
    def test_is_valid_ip(self):
        self.assertTrue(utils.is_valid_ip("127.0.0.1"))
        self.assertTrue(utils.is_valid_ip("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertTrue(utils.is_valid_ip(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertTrue(utils.is_valid_ip(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertTrue(utils.is_valid_ip(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertTrue(utils.is_valid_ip(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertTrue(utils.is_valid_ip(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertTrue(utils.is_valid_ip(ipv6))
        ipv6 = "fe80::"
        self.assertTrue(utils.is_valid_ip(ipv6))
        ipv6 = "::1"
        self.assertTrue(utils.is_valid_ip(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(utils.is_valid_ip(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(utils.is_valid_ip(not_ipv6))

    def test_is_valid_ipv4(self):
        self.assertTrue(utils.is_valid_ipv4("127.0.0.1"))
        self.assertTrue(utils.is_valid_ipv4("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        ipv6 = "fe80::"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        ipv6 = "::1"
        self.assertFalse(utils.is_valid_ipv4(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(utils.is_valid_ipv4(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(utils.is_valid_ipv4(not_ipv6))

    def test_is_valid_ipv6(self):
        self.assertFalse(utils.is_valid_ipv6("127.0.0.1"))
        self.assertFalse(utils.is_valid_ipv6("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        ipv6 = "fe80::"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        ipv6 = "::1"
        self.assertTrue(utils.is_valid_ipv6(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(utils.is_valid_ipv6(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(utils.is_valid_ipv6(not_ipv6))


class TestExpandIPv6(unittest.TestCase):
    def test_expand_ipv6(self):
        expanded_ipv6 = "fe80::204:61ff:fe9d:f156"
        upper_ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertEqual(expanded_ipv6, utils.expand_ipv6(upper_ipv6))
        omit_ipv6 = "fe80:0000:0000::0204:61ff:fe9d:f156"
        self.assertEqual(expanded_ipv6, utils.expand_ipv6(omit_ipv6))
        less_num_ipv6 = "fe80:0:00:000:0204:61ff:fe9d:f156"
        self.assertEqual(expanded_ipv6, utils.expand_ipv6(less_num_ipv6))


class TestWhatAreMyIPs(unittest.TestCase):
    def test_whataremyips(self):
        myips = utils.whataremyips()
        self.assertTrue(len(myips) > 1)
        self.assertIn('127.0.0.1', myips)

    def test_whataremyips_bind_to_all(self):
        for any_addr in ('0.0.0.0', '0000:0000:0000:0000:0000:0000:0000:0000',
                         '::0', '::0000', '::',
                         # Wacky parse-error input produces all IPs
                         'I am a bear'):
            myips = utils.whataremyips(any_addr)
            self.assertTrue(len(myips) > 1)
            self.assertIn('127.0.0.1', myips)

    def test_whataremyips_bind_ip_specific(self):
        self.assertEqual(['1.2.3.4'], utils.whataremyips('1.2.3.4'))

    def test_whataremyips_netifaces_error(self):
        class FakeNetifaces(object):
            @staticmethod
            def interfaces():
                return ['eth0']

            @staticmethod
            def ifaddresses(interface):
                raise ValueError

        with patch.object(utils_ipaddrs, 'netifaces', FakeNetifaces):
            self.assertEqual(utils.whataremyips(), [])

    def test_whataremyips_netifaces_ipv6(self):
        test_ipv6_address = '2001:6b0:dead:beef:2::32'
        test_interface = 'eth0'

        class FakeNetifaces(object):
            AF_INET = int(socket.AF_INET)
            AF_INET6 = int(socket.AF_INET6)

            @staticmethod
            def interfaces():
                return ['eth0']

            @staticmethod
            def ifaddresses(interface):
                return {int(socket.AF_INET6): [
                    {'netmask': 'ffff:ffff:ffff:ffff::',
                     'addr': '%s%%%s' % (test_ipv6_address, test_interface)}]}

        with patch.object(utils_ipaddrs, 'netifaces', FakeNetifaces):
            myips = utils.whataremyips()
            self.assertEqual(len(myips), 1)
            self.assertEqual(myips[0], test_ipv6_address)
