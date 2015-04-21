# Copyright (c) 2014 Christian Schwede <christian.schwede@enovance.com>
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
import mock
import os
import StringIO
import tempfile
import unittest
import uuid

from swift.cli import ringbuilder
from swift.common import exceptions
from swift.common.ring import RingBuilder


class RunSwiftRingBuilderMixin(object):

    def run_srb(self, *argv):
        mock_stdout = StringIO.StringIO()
        mock_stderr = StringIO.StringIO()

        srb_args = ["", self.tempfile] + [str(s) for s in argv]

        try:
            with mock.patch("sys.stdout", mock_stdout):
                with mock.patch("sys.stderr", mock_stderr):
                    ringbuilder.main(srb_args)
        except SystemExit as err:
            if err.code not in (0, 1):  # (success, warning)
                raise
        return (mock_stdout.getvalue(), mock_stderr.getvalue())


class TestCommands(unittest.TestCase, RunSwiftRingBuilderMixin):

    def __init__(self, *args, **kwargs):
        super(TestCommands, self).__init__(*args, **kwargs)

        # List of search values for various actions
        # These should all match the first device in the sample ring
        # (see below) but not the second device
        self.search_values = ["d0", "/sda1", "r0", "z0", "z0-127.0.0.1",
                              "127.0.0.1", "z0:6000", ":6000", "R127.0.0.1",
                              "127.0.0.1R127.0.0.1", "R:6000",
                              "_some meta data"]
        tmpf = tempfile.NamedTemporaryFile()
        self.tempfile = self.tmpfile = tmpf.name

    def tearDown(self):
        try:
            os.remove(self.tmpfile)
        except OSError:
            pass

    def create_sample_ring(self):
        """ Create a sample ring with two devices

        At least two devices are needed to test removing
        a device, since removing the last device of a ring
        is not allowed """

        # Ensure there is no existing test builder file because
        # create_sample_ring() might be used more than once in a single test
        try:
            os.remove(self.tmpfile)
        except OSError:
            pass

        ring = RingBuilder(6, 3, 1)
        ring.add_dev({'weight': 100.0,
                      'region': 0,
                      'zone': 0,
                      'ip': '127.0.0.1',
                      'port': 6000,
                      'device': 'sda1',
                      'meta': 'some meta data',
                      })
        ring.add_dev({'weight': 100.0,
                      'region': 1,
                      'zone': 1,
                      'ip': '127.0.0.2',
                      'port': 6001,
                      'device': 'sda2'
                      })
        ring.save(self.tmpfile)

    def test_parse_search_values_old_format(self):
        # Test old format
        argv = ["d0r0z0-127.0.0.1:6000R127.0.0.1:6000/sda1_some meta data"]
        search_values = ringbuilder._parse_search_values(argv)
        self.assertEqual(search_values['id'], 0)
        self.assertEqual(search_values['region'], 0)
        self.assertEqual(search_values['zone'], 0)
        self.assertEqual(search_values['ip'], '127.0.0.1')
        self.assertEqual(search_values['port'], 6000)
        self.assertEqual(search_values['replication_ip'], '127.0.0.1')
        self.assertEqual(search_values['replication_port'], 6000)
        self.assertEqual(search_values['device'], 'sda1')
        self.assertEqual(search_values['meta'], 'some meta data')

    def test_parse_search_values_new_format(self):
        # Test new format
        argv = ["--id", "0", "--region", "0", "--zone", "0",
                "--ip", "127.0.0.1",
                "--port", "6000",
                "--replication-ip", "127.0.0.1",
                "--replication-port", "6000",
                "--device", "sda1", "--meta", "some meta data",
                "--weight", "100"]
        search_values = ringbuilder._parse_search_values(argv)
        self.assertEqual(search_values['id'], 0)
        self.assertEqual(search_values['region'], 0)
        self.assertEqual(search_values['zone'], 0)
        self.assertEqual(search_values['ip'], '127.0.0.1')
        self.assertEqual(search_values['port'], 6000)
        self.assertEqual(search_values['replication_ip'], '127.0.0.1')
        self.assertEqual(search_values['replication_port'], 6000)
        self.assertEqual(search_values['device'], 'sda1')
        self.assertEqual(search_values['meta'], 'some meta data')
        self.assertEqual(search_values['weight'], 100)

    def test_parse_search_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["--region", "2", "test"]
        err = None
        try:
            ringbuilder._parse_search_values(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_find_parts(self):
        rb = RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 1, 'region': 1, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 2, 'region': 1, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.rebalance()

        rb.add_dev({'id': 3, 'region': 2, 'zone': 1, 'weight': 10,
                    'ip': '127.0.0.1', 'port': 10004, 'device': 'sda1'})
        rb.pretend_min_part_hours_passed()
        rb.rebalance()

        ringbuilder.builder = rb
        sorted_partition_count = ringbuilder._find_parts(
            rb.search_devs({'ip': '127.0.0.1'}))

        # Expect 256 partitions in the output
        self.assertEqual(256, len(sorted_partition_count))

        # Each partitions should have 3 replicas
        for partition, count in sorted_partition_count:
            self.assertEqual(
                3, count, "Partition %d has only %d replicas" %
                (partition, count))

    def test_parse_list_parts_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["--region", "2", "test"]
        err = None
        try:
            ringbuilder._parse_list_parts_values(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_parse_address_old_format(self):
        # Test old format
        argv = "127.0.0.1:6000R127.0.0.1:6000/sda1_some meta data"
        ip, port, rest = ringbuilder._parse_address(argv)
        self.assertEqual(ip, '127.0.0.1')
        self.assertEqual(port, 6000)
        self.assertEqual(rest, 'R127.0.0.1:6000/sda1_some meta data')

    def test_parse_add_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["--region", "2", "test"]
        err = None
        try:
            ringbuilder._parse_add_values(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_weight_values_no_devices(self):
        # Test no devices
        err = None
        try:
            ringbuilder._set_weight_values([], 100)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_parse_set_weight_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["r1", "100", "r2"]
        err = None
        try:
            ringbuilder._parse_set_weight_values(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

        argv = ["--region", "2"]
        err = None
        try:
            ringbuilder._parse_set_weight_values(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_info_values_no_devices(self):
        # Test no devices
        err = None
        try:
            ringbuilder._set_info_values([], 100)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_parse_set_info_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["r1", "127.0.0.1", "r2"]
        err = None
        try:
            ringbuilder._parse_set_info_values(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_parse_remove_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["--region", "2", "test"]
        err = None
        try:
            ringbuilder._parse_remove_values(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_create_ring(self):
        argv = ["", self.tmpfile, "create", "6", "3.14159265359", "1"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.part_power, 6)
        self.assertEqual(ring.replicas, 3.14159265359)
        self.assertEqual(ring.min_part_hours, 1)

    def test_add_device_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "add",
                "r2z3-127.0.0.1:6000/sda3_some meta data", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 3.14159265359)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6000)
        self.assertEqual(dev['meta'], 'some meta data')

    def test_add_device_ipv6_old_format(self):
        self.create_sample_ring()
        # Test ipv6(old format)
        argv = \
            ["", self.tmpfile, "add",
             "r2z3-2001:0000:1234:0000:0000:C1C0:ABCD:0876:6000"
             "R2::10:7000/sda3_some meta data",
             "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '2001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 3.14159265359)
        self.assertEqual(dev['replication_ip'], '2::10')
        self.assertEqual(dev['replication_port'], 7000)
        self.assertEqual(dev['meta'], 'some meta data')
        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_add_device_ipv4_new_format(self):
        self.create_sample_ring()
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "127.0.0.2",
             "--port", "6000",
             "--replication-ip", "127.0.0.2",
             "--replication-port", "6000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 3.14159265359)
        self.assertEqual(dev['replication_ip'], '127.0.0.2')
        self.assertEqual(dev['replication_port'], 6000)
        self.assertEqual(dev['meta'], 'some meta data')
        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_add_device_ipv6_new_format(self):
        self.create_sample_ring()
        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[3001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[3::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '3001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 3.14159265359)
        self.assertEqual(dev['replication_ip'], '3::10')
        self.assertEqual(dev['replication_port'], 7000)
        self.assertEqual(dev['meta'], 'some meta data')
        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_add_device_domain_new_format(self):
        self.create_sample_ring()
        # Test domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], 'test.test.com')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 3.14159265359)
        self.assertEqual(dev['replication_ip'], 'r.test.com')
        self.assertEqual(dev['replication_port'], 7000)
        self.assertEqual(dev['meta'], 'some meta data')
        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_add_device_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "add"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_add_device_already_exists(self):
        # Test Add a device that already exists
        argv = ["", self.tmpfile, "add",
                "r0z0-127.0.0.1:6000/sda1_some meta data", "100"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_remove_device(self):
        for search_value in self.search_values:
            self.create_sample_ring()
            argv = ["", self.tmpfile, "remove", search_value]
            self.assertRaises(SystemExit, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)

            # Check that weight was set to 0
            dev = [d for d in ring.devs if d['id'] == 0][0]
            self.assertEqual(dev['weight'], 0)

            # Check that device is in list of devices to be removed
            dev = [d for d in ring._remove_devs if d['id'] == 0][0]
            self.assertEqual(dev['region'], 0)
            self.assertEqual(dev['zone'], 0)
            self.assertEqual(dev['ip'], '127.0.0.1')
            self.assertEqual(dev['port'], 6000)
            self.assertEqual(dev['device'], 'sda1')
            self.assertEqual(dev['weight'], 0)
            self.assertEqual(dev['replication_ip'], '127.0.0.1')
            self.assertEqual(dev['replication_port'], 6000)
            self.assertEqual(dev['meta'], 'some meta data')

            # Check that second device in ring is not affected
            dev = [d for d in ring.devs if d['id'] == 1][0]
            self.assertEqual(dev['weight'], 100)
            self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

            # Final check, rebalance and check ring is ok
            ring.rebalance()
            self.assertTrue(ring.validate())

    def test_remove_device_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "remove",
                "d0r0z0-127.0.0.1:6000R127.0.0.1:6000/sda1_some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was set to 0
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        dev = [d for d in ring._remove_devs if d['id'] == 0][0]
        self.assertEqual(dev['region'], 0)
        self.assertEqual(dev['zone'], 0)
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6000)
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_remove_device_ipv6_old_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "remove",
                "d2r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6000"
                "R[2::10]:7000/sda3_some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 0])

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Check that weight was set to 0
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        dev = [d for d in ring._remove_devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '2001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], '2::10')
        self.assertEqual(dev['replication_port'], 7000)
        self.assertEqual(dev['meta'], 'some meta data')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_remove_device_ipv4_new_format(self):
        self.create_sample_ring()
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "remove",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6000",
             "--device", "sda1", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was set to 0
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        dev = [d for d in ring._remove_devs if d['id'] == 0][0]
        self.assertEqual(dev['region'], 0)
        self.assertEqual(dev['zone'], 0)
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6000)
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_remove_device_ipv6_new_format(self):
        self.create_sample_ring()
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[3001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "8000",
             "--replication-ip", "[3::10]",
             "--replication-port", "9000",
             "--device", "sda30", "--meta", "other meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "remove",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "[3001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "8000",
             "--replication-ip", "[3::10]",
             "--replication-port", "9000",
             "--device", "sda30", "--meta", "other meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 0])

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Check that weight was set to 0
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        dev = [d for d in ring._remove_devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '3001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 8000)
        self.assertEqual(dev['device'], 'sda30')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], '3::10')
        self.assertEqual(dev['replication_port'], 9000)
        self.assertEqual(dev['meta'], 'other meta data')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_remove_device_domain_new_format(self):
        self.create_sample_ring()
        # add domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "remove",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 0])

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Check that weight was set to 0
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        dev = [d for d in ring._remove_devs if d['id'] == 2][0]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], 'test.test.com')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], 'r.test.com')
        self.assertEqual(dev['replication_port'], 7000)
        self.assertEqual(dev['meta'], 'some meta data')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_remove_device_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "remove"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_remove_device_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "remove",
                "--ip", "unknown"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_weight(self):
        for search_value in self.search_values:
            self.create_sample_ring()

            argv = ["", self.tmpfile, "set_weight",
                    search_value, "3.14159265359"]
            self.assertRaises(SystemExit, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)

            # Check that weight was changed
            dev = [d for d in ring.devs if d['id'] == 0][0]
            self.assertEqual(dev['weight'], 3.14159265359)

            # Check that second device in ring is not affected
            dev = [d for d in ring.devs if d['id'] == 1][0]
            self.assertEqual(dev['weight'], 100)

            # Final check, rebalance and check ring is ok
            ring.rebalance()
            self.assertTrue(ring.validate())

    def test_set_weight_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "set_weight",
                "d0r0z0-127.0.0.1:6000R127.0.0.1:6000/sda1_some meta data",
                "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was changed
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_weight_ipv6_old_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "100"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "set_weight",
                "d2r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6000"
                "R[2::10]:7000/sda3_some meta data", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 100)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)

        # Check that weight was changed
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_weight_ipv4_new_format(self):
        self.create_sample_ring()
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "set_weight",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6000",
             "--device", "sda1", "--meta", "some meta data", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was changed
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_weight_ipv6_new_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "100"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "set_weight",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 100)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)

        # Check that weight was changed
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_weight_domain_new_format(self):
        self.create_sample_ring()
        # add domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "100"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "set_weight",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['weight'], 100)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['weight'], 100)

        # Check that weight was changed
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_weight_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_weight"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_weight_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "set_weight",
                "--ip", "unknown"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_info(self):
        for search_value in self.search_values:

            self.create_sample_ring()
            argv = ["", self.tmpfile, "set_info", search_value,
                    "127.0.1.1:8000/sda1_other meta data"]
            self.assertRaises(SystemExit, ringbuilder.main, argv)

            # Check that device was created with given data
            ring = RingBuilder.load(self.tmpfile)
            dev = [d for d in ring.devs if d['id'] == 0][0]
            self.assertEqual(dev['ip'], '127.0.1.1')
            self.assertEqual(dev['port'], 8000)
            self.assertEqual(dev['device'], 'sda1')
            self.assertEqual(dev['meta'], 'other meta data')

            # Check that second device in ring is not affected
            dev = [d for d in ring.devs if d['id'] == 1][0]
            self.assertEqual(dev['ip'], '127.0.0.2')
            self.assertEqual(dev['port'], 6001)
            self.assertEqual(dev['device'], 'sda2')
            self.assertEqual(dev['meta'], '')

            # Final check, rebalance and check ring is ok
            ring.rebalance()
            self.assertTrue(ring.validate())

    def test_set_info_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "set_info",
                "d0r0z0-127.0.0.1:6000R127.0.0.1:6000/sda1_some meta data",
                "127.0.1.1:8000R127.0.1.1:8000/sda10_other meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['ip'], '127.0.1.1')
        self.assertEqual(dev['port'], 8000)
        self.assertEqual(dev['replication_ip'], '127.0.1.1')
        self.assertEqual(dev['replication_port'], 8000)
        self.assertEqual(dev['device'], 'sda10')
        self.assertEqual(dev['meta'], 'other meta data')

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6001)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_info_ipv6_old_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "set_info",
                "d2r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6000"
                "R[2::10]:7000/sda3_some meta data",
                "[3001:0000:1234:0000:0000:C1C0:ABCD:0876]:8000"
                "R[3::10]:8000/sda30_other meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6000)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6001)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['ip'], '3001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 8000)
        self.assertEqual(dev['replication_ip'], '3::10')
        self.assertEqual(dev['replication_port'], 8000)
        self.assertEqual(dev['device'], 'sda30')
        self.assertEqual(dev['meta'], 'other meta data')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_info_ipv4_new_format(self):
        self.create_sample_ring()
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "set_info",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6000",
             "--device", "sda1", "--meta", "some meta data",
             "--change-ip", "127.0.2.1",
             "--change-port", "9000",
             "--change-replication-ip", "127.0.2.1",
             "--change-replication-port", "9000",
             "--change-device", "sda100", "--change-meta", "other meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['ip'], '127.0.2.1')
        self.assertEqual(dev['port'], 9000)
        self.assertEqual(dev['replication_ip'], '127.0.2.1')
        self.assertEqual(dev['replication_port'], 9000)
        self.assertEqual(dev['device'], 'sda100')
        self.assertEqual(dev['meta'], 'other meta data')

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6001)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_info_ipv6_new_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "set_info",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--change-ip", "[4001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--change-port", "9000",
             "--change-replication-ip", "[4::10]",
             "--change-replication-port", "9000",
             "--change-device", "sda300", "--change-meta", "other meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6000)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6001)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['ip'], '4001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 9000)
        self.assertEqual(dev['replication_ip'], '4::10')
        self.assertEqual(dev['replication_port'], 9000)
        self.assertEqual(dev['device'], 'sda300')
        self.assertEqual(dev['meta'], 'other meta data')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_info_domain_new_format(self):
        self.create_sample_ring()
        # add domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "set_info",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--change-ip", "test.test2.com",
             "--change-port", "9000",
             "--change-replication-ip", "r.test2.com",
             "--change-replication-port", "9000",
             "--change-device", "sda300", "--change-meta", "other meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 0][0]
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6000)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6000)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = [d for d in ring.devs if d['id'] == 1][0]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6001)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = [d for d in ring.devs if d['id'] == 2][0]
        self.assertEqual(dev['ip'], 'test.test2.com')
        self.assertEqual(dev['port'], 9000)
        self.assertEqual(dev['replication_ip'], 'r.test2.com')
        self.assertEqual(dev['replication_port'], 9000)
        self.assertEqual(dev['device'], 'sda300')
        self.assertEqual(dev['meta'], 'other meta data')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_info_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_info"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_info_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "set_info",
                "--ip", "unknown"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_info_already_exists(self):
        self.create_sample_ring()
        # Test Set a device that already exists
        argv = \
            ["", self.tmpfile, "set_info",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6000",
             "--device", "sda1", "--meta", "some meta data",
             "--change-ip", "127.0.0.2",
             "--change-port", "6001",
             "--change-replication-ip", "127.0.0.2",
             "--change-replication-port", "6001",
             "--change-device", "sda2", "--change-meta", ""]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_min_part_hours(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_min_part_hours", "24"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.min_part_hours, 24)

    def test_set_min_part_hours_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_min_part_hours"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_replicas(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_replicas", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.replicas, 3.14159265359)

    def test_set_overload(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "0.19878"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.19878)

    def test_set_overload_negative(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "-0.19878"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.0)

    def test_set_overload_non_numeric(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "swedish fish"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.0)

    def test_set_overload_percent(self):
        self.create_sample_ring()
        argv = "set_overload 10%".split()
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.1)
        self.assertTrue('10.00%' in out)
        self.assertTrue('0.100000' in out)

    def test_set_overload_percent_strange_input(self):
        self.create_sample_ring()
        argv = "set_overload 26%%%%".split()
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.26)
        self.assertTrue('26.00%' in out)
        self.assertTrue('0.260000' in out)

    def test_server_overload_crazy_high(self):
        self.create_sample_ring()
        argv = "set_overload 10".split()
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 10.0)
        self.assertTrue('Warning overload is greater than 100%' in out)
        self.assertTrue('1000.00%' in out)
        self.assertTrue('10.000000' in out)
        # but it's cool if you do it on purpose
        argv[-1] = '1000%'
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 10.0)
        self.assertTrue('Warning overload is greater than 100%' not in out)
        self.assertTrue('1000.00%' in out)
        self.assertTrue('10.000000' in out)

    def test_set_replicas_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_replicas"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_set_replicas_invalid_value(self):
        # Test not a valid number
        argv = ["", self.tmpfile, "set_replicas", "test"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

        # Test new replicas is 0
        argv = ["", self.tmpfile, "set_replicas", "0"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_validate(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)
        argv = ["", self.tmpfile, "validate"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_validate_empty_file(self):
        open(self.tmpfile, 'a').close
        argv = ["", self.tmpfile, "validate"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_validate_corrupted_file(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        self.assertTrue(ring.validate())  # ring is valid until now
        ring.save(self.tmpfile)
        argv = ["", self.tmpfile, "validate"]

        # corrupt the file
        with open(self.tmpfile, 'wb') as f:
            f.write(os.urandom(1024))
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_validate_non_existent_file(self):
        rand_file = '%s/%s' % ('/tmp', str(uuid.uuid4()))
        argv = ["", rand_file, "validate"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_validate_non_accessible_file(self):
        with mock.patch.object(
                RingBuilder, 'load',
                mock.Mock(side_effect=exceptions.PermissionError)):
            argv = ["", self.tmpfile, "validate"]
            err = None
            try:
                ringbuilder.main(argv)
            except SystemExit as e:
                err = e
            self.assertEquals(err.code, 2)

    def test_validate_generic_error(self):
        with mock.patch.object(
                RingBuilder, 'load', mock.Mock(
                    side_effect=IOError('Generic error occurred'))):
            argv = ["", self.tmpfile, "validate"]
            err = None
            try:
                ringbuilder.main(argv)
            except SystemExit as e:
                err = e
            self.assertEquals(err.code, 2)

    def test_search_device_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "search",
                "d0r0z0-127.0.0.1:6000R127.0.0.1:6000/sda1_some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_search_device_ipv6_old_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "search",
                "d2r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6000"
                "R[2::10]:7000/sda3_some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_search_device_ipv4_new_format(self):
        self.create_sample_ring()
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "search",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6000",
             "--device", "sda1", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_search_device_ipv6_new_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "search",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_search_device_domain_new_format(self):
        self.create_sample_ring()
        # add domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "search",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_search_device_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "search"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_search_device_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "search",
                "--ip", "unknown"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_list_parts_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "list_parts",
                "d0r0z0-127.0.0.1:6000R127.0.0.1:6000/sda1_some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_list_parts_ipv6_old_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "list_parts",
                "d2r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6000"
                "R[2::10]:7000/sda3_some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_list_parts_ipv4_new_format(self):
        self.create_sample_ring()
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "list_parts",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6000",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6000",
             "--device", "sda1", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_list_parts_ipv6_new_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "list_parts",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6000",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_list_parts_domain_new_format(self):
        self.create_sample_ring()
        # add domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "list_parts",
             "--id", "2", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6000",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_list_parts_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "list_parts"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_list_parts_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "list_parts",
                "--ip", "unknown"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_unknown(self):
        argv = ["", self.tmpfile, "unknown"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_default(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_rebalance(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "rebalance", "3"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertTrue(ring.validate())

    def test_rebalance_no_device_change(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)
        # Test No change to the device
        argv = ["", self.tmpfile, "rebalance", "3"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 1)

    def test_rebalance_no_devices(self):
        # Test no devices
        argv = ["", self.tmpfile, "create", "6", "3.14159265359", "1"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "rebalance"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_write_ring(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "rebalance"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        argv = ["", self.tmpfile, "write_ring"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

    def test_write_builder(self):
        # Test builder file already exists
        self.create_sample_ring()
        argv = ["", self.tmpfile, "rebalance"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "write_builder"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 2)

    def test_warn_at_risk(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.devs[0]['weight'] = 10
        ring.save(self.tmpfile)
        argv = ["", self.tmpfile, "rebalance"]
        err = None
        try:
            ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEquals(err.code, 1)

    def test_invalid_device_name(self):
        self.create_sample_ring()
        for device_name in ["", " ", " sda1", "sda1 ", " meta "]:
            err = 0

            argv = ["",
                    self.tmpfile,
                    "add",
                    "r1z1-127.0.0.1:6000/%s" % device_name,
                    "1"]
            try:
                ringbuilder.main(argv)
            except SystemExit as exc:
                err = exc
            self.assertEquals(err.code, 2)

            argv = ["",
                    self.tmpfile,
                    "add",
                    "--region", "1",
                    "--zone", "1",
                    "--ip", "127.0.0.1",
                    "--port", "6000",
                    "--device", device_name,
                    "--weight", "100"]
            try:
                ringbuilder.main(argv)
            except SystemExit as exc:
                err = exc
            self.assertEquals(err.code, 2)


class TestRebalanceCommand(unittest.TestCase, RunSwiftRingBuilderMixin):

    def __init__(self, *args, **kwargs):
        super(TestRebalanceCommand, self).__init__(*args, **kwargs)
        tmpf = tempfile.NamedTemporaryFile()
        self.tempfile = self.tmpfile = tmpf.name

    def tearDown(self):
        try:
            os.remove(self.tempfile)
        except OSError:
            pass

    def run_srb(self, *argv):
        mock_stdout = StringIO.StringIO()
        mock_stderr = StringIO.StringIO()

        srb_args = ["", self.tempfile] + [str(s) for s in argv]

        try:
            with mock.patch("sys.stdout", mock_stdout):
                with mock.patch("sys.stderr", mock_stderr):
                    ringbuilder.main(srb_args)
        except SystemExit as err:
            if err.code not in (0, 1):  # (success, warning)
                raise
        return (mock_stdout.getvalue(), mock_stderr.getvalue())

    def test_debug(self):
        # NB: getLogger(name) always returns the same object
        rb_logger = logging.getLogger("swift.ring.builder")
        try:
            self.assertNotEqual(rb_logger.getEffectiveLevel(), logging.DEBUG)

            self.run_srb("create", 8, 3, 1)
            self.run_srb("add",
                         "r1z1-10.1.1.1:2345/sda", 100.0,
                         "r1z1-10.1.1.1:2345/sdb", 100.0,
                         "r1z1-10.1.1.1:2345/sdc", 100.0,
                         "r1z1-10.1.1.1:2345/sdd", 100.0)
            self.run_srb("rebalance", "--debug")
            self.assertEqual(rb_logger.getEffectiveLevel(), logging.DEBUG)

            rb_logger.setLevel(logging.INFO)
            self.run_srb("rebalance", "--debug", "123")
            self.assertEqual(rb_logger.getEffectiveLevel(), logging.DEBUG)

            rb_logger.setLevel(logging.INFO)
            self.run_srb("rebalance", "123", "--debug")
            self.assertEqual(rb_logger.getEffectiveLevel(), logging.DEBUG)

        finally:
            rb_logger.setLevel(logging.INFO)  # silence other test cases

    def test_rebalance_warning_appears(self):
        self.run_srb("create", 8, 3, 24)
        # all in one machine: totally balanceable
        self.run_srb("add",
                     "r1z1-10.1.1.1:2345/sda", 100.0,
                     "r1z1-10.1.1.1:2345/sdb", 100.0,
                     "r1z1-10.1.1.1:2345/sdc", 100.0,
                     "r1z1-10.1.1.1:2345/sdd", 100.0)
        out, err = self.run_srb("rebalance")
        self.assertTrue("rebalance/repush" not in out)

        # 2 machines of equal size: balanceable, but not in one pass due to
        # min_part_hours > 0
        self.run_srb("add",
                     "r1z1-10.1.1.2:2345/sda", 100.0,
                     "r1z1-10.1.1.2:2345/sdb", 100.0,
                     "r1z1-10.1.1.2:2345/sdc", 100.0,
                     "r1z1-10.1.1.2:2345/sdd", 100.0)
        self.run_srb("pretend_min_part_hours_passed")
        out, err = self.run_srb("rebalance")
        self.assertTrue("rebalance/repush" in out)

        # after two passes, it's all balanced out
        self.run_srb("pretend_min_part_hours_passed")
        out, err = self.run_srb("rebalance")
        self.assertTrue("rebalance/repush" not in out)

    def test_rebalance_warning_with_overload(self):
        self.run_srb("create", 8, 3, 24)
        self.run_srb("set_overload", 0.12)
        # The ring's balance is at least 5, so normally we'd get a warning,
        # but it's suppressed due to the overload factor.
        self.run_srb("add",
                     "r1z1-10.1.1.1:2345/sda", 100.0,
                     "r1z1-10.1.1.1:2345/sdb", 100.0,
                     "r1z1-10.1.1.1:2345/sdc", 120.0)
        out, err = self.run_srb("rebalance")
        self.assertTrue("rebalance/repush" not in out)

        # Now we add in a really big device, but not enough partitions move
        # to fill it in one pass, so we see the rebalance warning.
        self.run_srb("add", "r1z1-10.1.1.1:2345/sdd", 99999.0)
        self.run_srb("pretend_min_part_hours_passed")
        out, err = self.run_srb("rebalance")
        self.assertTrue("rebalance/repush" in out)

    def test_cached_dispersion_value(self):
        self.run_srb("create", 8, 3, 24)
        self.run_srb("add",
                     "r1z1-10.1.1.1:2345/sda", 100.0,
                     "r1z1-10.1.1.1:2345/sdb", 100.0,
                     "r1z1-10.1.1.1:2345/sdc", 100.0,
                     "r1z1-10.1.1.1:2345/sdd", 100.0)
        self.run_srb('rebalance')
        out, err = self.run_srb()  # list devices
        self.assertTrue('dispersion' in out)
        # remove cached dispersion value
        builder = RingBuilder.load(self.tempfile)
        builder.dispersion = None
        builder.save(self.tempfile)
        # now dispersion output is suppressed
        out, err = self.run_srb()  # list devices
        self.assertFalse('dispersion' in out)
        # but will show up after rebalance
        self.run_srb('rebalance', '-f')
        out, err = self.run_srb()  # list devices
        self.assertTrue('dispersion' in out)


if __name__ == '__main__':
    unittest.main()
