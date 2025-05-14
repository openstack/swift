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

import ctypes
from unittest.mock import patch
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

    def test_whataremyips_getifaddrs(self):
        def mock_getifaddrs(ptr):
            addrs = [
                utils_ipaddrs.ifaddrs(None, b'lo', 0, ctypes.pointer(
                    utils_ipaddrs.sockaddr_in4(
                        sin_family=socket.AF_INET,
                        sin_addr=(127, 0, 0, 1)))),
                utils_ipaddrs.ifaddrs(None, b'lo', 0, ctypes.cast(
                    ctypes.pointer(utils_ipaddrs.sockaddr_in6(
                        sin6_family=socket.AF_INET6,
                        sin6_addr=(
                            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1))),
                    ctypes.POINTER(utils_ipaddrs.sockaddr_in4))),
                utils_ipaddrs.ifaddrs(None, b'eth0', 0, ctypes.pointer(
                    utils_ipaddrs.sockaddr_in4(
                        sin_family=socket.AF_INET,
                        sin_addr=(192, 168, 50, 63)))),
                utils_ipaddrs.ifaddrs(None, b'eth0', 0, ctypes.cast(
                    ctypes.pointer(utils_ipaddrs.sockaddr_in6(
                        sin6_family=socket.AF_INET6,
                        sin6_addr=(
                            254, 128, 0, 0, 0, 0, 0, 0,
                            106, 191, 199, 168, 109, 243, 41, 35))),
                    ctypes.POINTER(utils_ipaddrs.sockaddr_in4))),
                # MAC address will be ignored
                utils_ipaddrs.ifaddrs(None, b'eth0', 0, ctypes.cast(
                    ctypes.pointer(utils_ipaddrs.sockaddr_in6(
                        sin6_family=getattr(socket, 'AF_PACKET', 17),
                        sin6_port=0,
                        sin6_flowinfo=2,
                        sin6_addr=(
                            1, 0, 0, 6, 172, 116, 177, 85,
                            64, 146, 0, 0, 0, 0, 0, 0))),
                    ctypes.POINTER(utils_ipaddrs.sockaddr_in4))),
                # Seen in the wild: no addresses at all
                utils_ipaddrs.ifaddrs(None, b'cscotun0', 69841),
            ]
            for cur, nxt in zip(addrs, addrs[1:]):
                cur.ifa_next = ctypes.pointer(nxt)
            ptr._obj.contents = addrs[0]

        with patch.object(utils_ipaddrs, 'getifaddrs', mock_getifaddrs), \
                patch('swift.common.utils.ipaddrs.freeifaddrs') as mock_free:
            self.assertEqual(utils.whataremyips(), [
                '127.0.0.1',
                '::1',
                '192.168.50.63',
                'fe80::6abf:c7a8:6df3:2923',
            ])
            self.assertEqual(len(mock_free.mock_calls), 1)

    def test_whataremyips_netifaces_error(self):
        class FakeNetifaces(object):
            @staticmethod
            def interfaces():
                return ['eth0']

            @staticmethod
            def ifaddresses(interface):
                raise ValueError

        with patch.object(utils_ipaddrs, 'getifaddrs', None), \
                patch.object(utils_ipaddrs, 'netifaces', FakeNetifaces):
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

        with patch.object(utils_ipaddrs, 'getifaddrs', None), \
                patch.object(utils_ipaddrs, 'netifaces', FakeNetifaces):
            myips = utils.whataremyips()
            self.assertEqual(len(myips), 1)
            self.assertEqual(myips[0], test_ipv6_address)
