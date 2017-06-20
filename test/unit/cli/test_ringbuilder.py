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

import errno
import itertools
import logging
import mock
import os
import re
import six
import tempfile
import unittest
import uuid
import shlex
import shutil
import time

from swift.cli import ringbuilder
from swift.cli.ringbuilder import EXIT_SUCCESS, EXIT_WARNING, EXIT_ERROR
from swift.common import exceptions
from swift.common.ring import RingBuilder
from swift.common.ring.composite_builder import CompositeRingBuilder

from test.unit import Timeout, write_stub_builder

try:
    from itertools import zip_longest
except ImportError:
    from itertools import izip_longest as zip_longest


class RunSwiftRingBuilderMixin(object):

    def run_srb(self, *argv, **kwargs):
        if len(argv) == 1 and isinstance(argv[0], six.string_types):
            # convert a single string to a list
            argv = shlex.split(argv[0])
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()

        if 'exp_results' in kwargs:
            exp_results = kwargs['exp_results']
        else:
            exp_results = None

        srb_args = ["", self.tempfile] + [str(s) for s in argv]

        try:
            with mock.patch("sys.stdout", mock_stdout):
                with mock.patch("sys.stderr", mock_stderr):
                    ringbuilder.main(srb_args)
        except SystemExit as err:
            valid_exit_codes = None
            if exp_results is not None and 'valid_exit_codes' in exp_results:
                valid_exit_codes = exp_results['valid_exit_codes']
            else:
                valid_exit_codes = (0, 1)  # (success, warning)

            if err.code not in valid_exit_codes:
                msg = 'Unexpected exit status %s\n' % err.code
                msg += 'STDOUT:\n%s\nSTDERR:\n%s\n' % (
                    mock_stdout.getvalue(), mock_stderr.getvalue())
                self.fail(msg)
        return (mock_stdout.getvalue(), mock_stderr.getvalue())


class TestCommands(unittest.TestCase, RunSwiftRingBuilderMixin):

    def __init__(self, *args, **kwargs):
        super(TestCommands, self).__init__(*args, **kwargs)

        # List of search values for various actions
        # These should all match the first device in the sample ring
        # (see below) but not the second device
        self.search_values = ["d0", "/sda1", "r0", "z0", "z0-127.0.0.1",
                              "127.0.0.1", "z0:6200", ":6200", "R127.0.0.1",
                              "127.0.0.1R127.0.0.1", "R:6200",
                              "_some meta data"]

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        tmpf = tempfile.NamedTemporaryFile(dir=self.tmpdir)
        self.tempfile = self.tmpfile = tmpf.name

    def tearDown(self):
        try:
            shutil.rmtree(self.tmpdir, True)
        except OSError:
            pass

    def assertOutputStub(self, output, ext='stub',
                         builder_id='(not assigned)'):
        """
        assert that the given output string is equal to a in-tree stub file,
        if a test needs to check multiple outputs it can use custom ext's
        """
        filepath = os.path.abspath(
            os.path.join(os.path.dirname(__file__), self.id().split('.')[-1]))
        print(filepath)
        filepath = '%s.%s' % (filepath, ext)
        try:
            with open(filepath, 'r') as f:
                stub = f.read()
        except (IOError, OSError) as e:
            if e.errno == errno.ENOENT:
                self.fail('%r does not exist' % filepath)
            else:
                self.fail('%r could not be read (%s)' % (filepath, e))
        output = output.replace(self.tempfile, '__RINGFILE__')
        stub = stub.replace('__BUILDER_ID__', builder_id)
        for i, (value, expected) in enumerate(
                zip_longest(output.splitlines(), stub.splitlines())):
            # N.B. differences in trailing whitespace are ignored!
            value = (value or '').rstrip()
            expected = (expected or '').rstrip()
            try:
                self.assertEqual(value, expected)
            except AssertionError:
                msg = 'Line #%s value is not like expected:\n%r\n%r' % (
                    i, value, expected)
                msg += '\n\nFull output was:\n'
                for i, line in enumerate(output.splitlines()):
                    msg += '%3d: %s\n' % (i, line)
                msg += '\n\nCompared to stub:\n'
                for i, line in enumerate(stub.splitlines()):
                    msg += '%3d: %s\n' % (i, line)
                self.fail(msg)

    def create_sample_ring(self, part_power=6, replicas=3, overload=None,
                           empty=False):
        """
        Create a sample ring with four devices

        At least four devices are needed to test removing
        a device, since having less devices than replicas
        is not allowed.
        """

        # Ensure there is no existing test builder file because
        # create_sample_ring() might be used more than once in a single test
        try:
            os.remove(self.tmpfile)
        except OSError:
            pass

        ring = RingBuilder(part_power, replicas, 1)
        if overload is not None:
            ring.set_overload(overload)
        if not empty:
            ring.add_dev({'weight': 100.0,
                          'region': 0,
                          'zone': 0,
                          'ip': '127.0.0.1',
                          'port': 6200,
                          'device': 'sda1',
                          'meta': 'some meta data',
                          })
            ring.add_dev({'weight': 100.0,
                          'region': 1,
                          'zone': 1,
                          'ip': '127.0.0.2',
                          'port': 6201,
                          'device': 'sda2'
                          })
            ring.add_dev({'weight': 100.0,
                          'region': 2,
                          'zone': 2,
                          'ip': '127.0.0.3',
                          'port': 6202,
                          'device': 'sdc3'
                          })
            ring.add_dev({'weight': 100.0,
                          'region': 3,
                          'zone': 3,
                          'ip': '127.0.0.4',
                          'port': 6203,
                          'device': 'sdd4'
                          })
        ring.save(self.tmpfile)
        return ring

    def assertSystemExit(self, return_code, func, *argv):
        with self.assertRaises(SystemExit) as cm:
            func(*argv)
        self.assertEqual(return_code, cm.exception.code)

    def test_parse_search_values_old_format(self):
        # Test old format
        argv = ["d0r0z0-127.0.0.1:6200R127.0.0.1:6200/sda1_some meta data"]
        search_values = ringbuilder._parse_search_values(argv)
        self.assertEqual(search_values['id'], 0)
        self.assertEqual(search_values['region'], 0)
        self.assertEqual(search_values['zone'], 0)
        self.assertEqual(search_values['ip'], '127.0.0.1')
        self.assertEqual(search_values['port'], 6200)
        self.assertEqual(search_values['replication_ip'], '127.0.0.1')
        self.assertEqual(search_values['replication_port'], 6200)
        self.assertEqual(search_values['device'], 'sda1')
        self.assertEqual(search_values['meta'], 'some meta data')

    def test_parse_search_values_new_format(self):
        # Test new format
        argv = ["--id", "0", "--region", "0", "--zone", "0",
                "--ip", "127.0.0.1",
                "--port", "6200",
                "--replication-ip", "127.0.0.1",
                "--replication-port", "6200",
                "--device", "sda1", "--meta", "some meta data",
                "--weight", "100"]
        search_values = ringbuilder._parse_search_values(argv)
        self.assertEqual(search_values['id'], 0)
        self.assertEqual(search_values['region'], 0)
        self.assertEqual(search_values['zone'], 0)
        self.assertEqual(search_values['ip'], '127.0.0.1')
        self.assertEqual(search_values['port'], 6200)
        self.assertEqual(search_values['replication_ip'], '127.0.0.1')
        self.assertEqual(search_values['replication_port'], 6200)
        self.assertEqual(search_values['device'], 'sda1')
        self.assertEqual(search_values['meta'], 'some meta data')
        self.assertEqual(search_values['weight'], 100)

    def test_parse_search_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["--region", "2", "test"]
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._parse_search_values, argv)

    def test_find_parts(self):
        rb = RingBuilder(8, 3, 0)
        rb.add_dev({'id': 0, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sda1'})
        rb.add_dev({'id': 3, 'region': 1, 'zone': 0, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10000, 'device': 'sdb1'})
        rb.add_dev({'id': 1, 'region': 1, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sda1'})
        rb.add_dev({'id': 4, 'region': 1, 'zone': 1, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1'})
        rb.add_dev({'id': 2, 'region': 1, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sda1'})
        rb.add_dev({'id': 5, 'region': 1, 'zone': 2, 'weight': 100,
                    'ip': '127.0.0.1', 'port': 10002, 'device': 'sdb1'})
        rb.rebalance()

        rb.add_dev({'id': 6, 'region': 2, 'zone': 1, 'weight': 10,
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
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._parse_list_parts_values, argv)

    def test_parse_add_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["--region", "2", "test"]
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._parse_add_values, argv)

    def test_set_weight_values_no_devices(self):
        # Test no devices
        # _set_weight_values doesn't take argv-like arguments
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._set_weight_values, [], 100, {})

    def test_parse_set_weight_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["r1", "100", "r2"]
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._parse_set_weight_values, argv)

        argv = ["--region", "2"]
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._parse_set_weight_values, argv)

    def test_set_info_values_no_devices(self):
        # Test no devices
        # _set_info_values doesn't take argv-like arguments
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._set_info_values, [], 100, {})

    def test_parse_set_info_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["r1", "127.0.0.1", "r2"]
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._parse_set_info_values, argv)

    def test_parse_remove_values_number_of_arguments(self):
        # Test Number of arguments abnormal
        argv = ["--region", "2", "test"]
        self.assertSystemExit(
            EXIT_ERROR, ringbuilder._parse_remove_values, argv)

    def test_create_ring(self):
        argv = ["", self.tmpfile, "create", "6", "3.14159265359", "1"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.part_power, 6)
        self.assertEqual(ring.replicas, 3.14159265359)
        self.assertEqual(ring.min_part_hours, 1)

    def test_create_ring_number_of_arguments(self):
        # Test missing arguments
        argv = ["", self.tmpfile, "create"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_add_device_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "add",
                "r2z3-127.0.0.1:6200/sda3_some meta data", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[-1]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 3.14159265359)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6200)
        self.assertEqual(dev['meta'], 'some meta data')

    def test_add_duplicate_devices(self):
        self.create_sample_ring()
        # Test adding duplicate devices
        argv = ["", self.tmpfile, "add",
                "r1z1-127.0.0.1:6200/sda9", "3.14159265359",
                "r1z1-127.0.0.1:6200/sda9", "2"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_add_device_ipv6_old_format(self):
        self.create_sample_ring()
        # Test ipv6(old format)
        argv = \
            ["", self.tmpfile, "add",
             "r2z3-2001:0000:1234:0000:0000:C1C0:ABCD:0876:6200"
             "R2::10:7000/sda3_some meta data",
             "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[-1]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '2001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 6200)
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
             "--port", "6200",
             "--replication-ip", "127.0.0.2",
             "--replication-port", "6200",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[-1]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 3.14159265359)
        self.assertEqual(dev['replication_ip'], '127.0.0.2')
        self.assertEqual(dev['replication_port'], 6200)
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
             "--port", "6200",
             "--replication-ip", "[3::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[-1]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '3001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 6200)
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
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[-1]
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], 'test.test.com')
        self.assertEqual(dev['port'], 6200)
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
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_add_device_already_exists(self):
        # Test Add a device that already exists
        argv = ["", self.tmpfile, "add",
                "r0z0-127.0.0.1:6200/sda1_some meta data", "100"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_add_device_old_missing_region(self):
        self.create_sample_ring()
        # Test add device without specifying a region
        argv = ["", self.tmpfile, "add",
                "z3-127.0.0.1:6200/sde3_some meta data", "3.14159265359"]
        exp_results = {'valid_exit_codes': [2]}
        self.run_srb(*argv, exp_results=exp_results)
        # Check that ring was created with sane value for region
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[-1]
        self.assertGreater(dev['region'], 0)

    def test_add_device_part_power_increase(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.next_part_power = 1
        ring.save(self.tmpfile)

        argv = ["", self.tmpfile, "add",
                "r0z0-127.0.1.1:6200/sda1_some meta data", "100"]
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

    def test_remove_device(self):
        for search_value in self.search_values:
            self.create_sample_ring()
            argv = ["", self.tmpfile, "remove", search_value]
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)

            # Check that weight was set to 0
            dev = ring.devs[0]
            self.assertEqual(dev['weight'], 0)

            # Check that device is in list of devices to be removed
            self.assertEqual(dev['region'], 0)
            self.assertEqual(dev['zone'], 0)
            self.assertEqual(dev['ip'], '127.0.0.1')
            self.assertEqual(dev['port'], 6200)
            self.assertEqual(dev['device'], 'sda1')
            self.assertEqual(dev['weight'], 0)
            self.assertEqual(dev['replication_ip'], '127.0.0.1')
            self.assertEqual(dev['replication_port'], 6200)
            self.assertEqual(dev['meta'], 'some meta data')

            # Check that second device in ring is not affected
            dev = ring.devs[1]
            self.assertEqual(dev['weight'], 100)
            self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

            # Final check, rebalance and check ring is ok
            ring.rebalance()
            self.assertTrue(ring.validate())

    def test_remove_device_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "remove",
                "d0r0z0-127.0.0.1:6200R127.0.0.1:6200/sda1_some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was set to 0
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        self.assertEqual(dev['region'], 0)
        self.assertEqual(dev['zone'], 0)
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6200)
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = ring.devs[1]
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
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "remove",
                "d4r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6200"
                "R[2::10]:7000/sda3_some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 0])

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Check that weight was set to 0
        dev = ring.devs[-1]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], '2001:0:1234::c1c0:abcd:876')
        self.assertEqual(dev['port'], 6200)
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
             "--port", "6200",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6200",
             "--device", "sda1", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was set to 0
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        self.assertEqual(dev['region'], 0)
        self.assertEqual(dev['zone'], 0)
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6200)
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = ring.devs[1]
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
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "remove",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "[3001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "8000",
             "--replication-ip", "[3::10]",
             "--replication-port", "9000",
             "--device", "sda30", "--meta", "other meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 0])

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Check that weight was set to 0
        dev = ring.devs[-1]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
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
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "remove",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 0])

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['weight'], 100)
        self.assertFalse([d for d in ring._remove_devs if d['id'] == 1])

        # Check that weight was set to 0
        dev = ring.devs[-1]
        self.assertEqual(dev['weight'], 0)

        # Check that device is in list of devices to be removed
        self.assertEqual(dev['region'], 2)
        self.assertEqual(dev['zone'], 3)
        self.assertEqual(dev['ip'], 'test.test.com')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['device'], 'sda3')
        self.assertEqual(dev['weight'], 0)
        self.assertEqual(dev['replication_ip'], 'r.test.com')
        self.assertEqual(dev['replication_port'], 7000)
        self.assertEqual(dev['meta'], 'some meta data')

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_remove_device_number_of_arguments(self):
        self.create_sample_ring()
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "remove"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_remove_device_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "remove",
                "--ip", "unknown"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_remove_device_part_power_increase(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.next_part_power = 1
        ring.save(self.tmpfile)

        argv = ["", self.tmpfile, "remove", "d0"]
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

    def test_set_weight(self):
        for search_value in self.search_values:
            self.create_sample_ring()

            argv = ["", self.tmpfile, "set_weight",
                    search_value, "3.14159265359"]
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)

            # Check that weight was changed
            dev = ring.devs[0]
            self.assertEqual(dev['weight'], 3.14159265359)

            # Check that second device in ring is not affected
            dev = ring.devs[1]
            self.assertEqual(dev['weight'], 100)

            # Final check, rebalance and check ring is ok
            ring.rebalance()
            self.assertTrue(ring.validate())

    def test_set_weight_old_format_two_devices(self):
        # Would block without the 'yes' argument
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_weight",
                "d2", "3.14", "d1", "6.28", "--yes"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        ring = RingBuilder.load(self.tmpfile)
        # Check that weight was changed
        self.assertEqual(ring.devs[2]['weight'], 3.14)
        self.assertEqual(ring.devs[1]['weight'], 6.28)
        # Check that other devices in ring are not affected
        self.assertEqual(ring.devs[0]['weight'], 100)
        self.assertEqual(ring.devs[3]['weight'], 100)

    def test_set_weight_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "set_weight",
                "d0r0z0-127.0.0.1:6200R127.0.0.1:6200/sda1_some meta data",
                "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was changed
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Check that second device in ring is not affected
        dev = ring.devs[1]
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
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "100"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "set_weight",
                "d4r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6200"
                "R[2::10]:7000/sda3_some meta data", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 100)

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['weight'], 100)

        # Check that weight was changed
        dev = ring.devs[-1]
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
             "--port", "6200",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6200",
             "--device", "sda1", "--meta", "some meta data", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that weight was changed
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Check that second device in ring is not affected
        dev = ring.devs[1]
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
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "100"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "set_weight",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 100)

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['weight'], 100)

        # Check that weight was changed
        dev = ring.devs[-1]
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
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "100"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "set_weight",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['weight'], 100)

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['weight'], 100)

        # Check that weight was changed
        dev = ring.devs[-1]
        self.assertEqual(dev['weight'], 3.14159265359)

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_set_weight_number_of_arguments(self):
        self.create_sample_ring()
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_weight"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_weight_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "set_weight",
                "--ip", "unknown"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_info(self):
        for search_value in self.search_values:

            self.create_sample_ring()
            argv = ["", self.tmpfile, "set_info", search_value,
                    "127.0.1.1:8000/sda1_other meta data"]
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

            # Check that device was created with given data
            ring = RingBuilder.load(self.tmpfile)
            dev = ring.devs[0]
            self.assertEqual(dev['ip'], '127.0.1.1')
            self.assertEqual(dev['port'], 8000)
            self.assertEqual(dev['device'], 'sda1')
            self.assertEqual(dev['meta'], 'other meta data')

            # Check that second device in ring is not affected
            dev = ring.devs[1]
            self.assertEqual(dev['ip'], '127.0.0.2')
            self.assertEqual(dev['port'], 6201)
            self.assertEqual(dev['device'], 'sda2')
            self.assertEqual(dev['meta'], '')

            # Final check, rebalance and check ring is ok
            ring.rebalance()
            self.assertTrue(ring.validate())

    def test_set_info_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "set_info",
                "d0r0z0-127.0.0.1:6200R127.0.0.1:6200/sda1_some meta data",
                "127.0.1.1:8000R127.0.1.1:8000/sda10_other meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[0]
        self.assertEqual(dev['ip'], '127.0.1.1')
        self.assertEqual(dev['port'], 8000)
        self.assertEqual(dev['replication_ip'], '127.0.1.1')
        self.assertEqual(dev['replication_port'], 8000)
        self.assertEqual(dev['device'], 'sda10')
        self.assertEqual(dev['meta'], 'other meta data')

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6201)
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
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "set_info",
                "d4r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6200"
                "R[2::10]:7000/sda3_some meta data",
                "[3001:0000:1234:0000:0000:C1C0:ABCD:0876]:8000"
                "R[3::10]:8000/sda30_other meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6200)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6201)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Check that device was created with given data
        dev = ring.devs[-1]
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
             "--port", "6200",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6200",
             "--device", "sda1", "--meta", "some meta data",
             "--change-ip", "127.0.2.1",
             "--change-port", "9000",
             "--change-replication-ip", "127.0.2.1",
             "--change-replication-port", "9000",
             "--change-device", "sda100", "--change-meta", "other meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[0]
        self.assertEqual(dev['ip'], '127.0.2.1')
        self.assertEqual(dev['port'], 9000)
        self.assertEqual(dev['replication_ip'], '127.0.2.1')
        self.assertEqual(dev['replication_port'], 9000)
        self.assertEqual(dev['device'], 'sda100')
        self.assertEqual(dev['meta'], 'other meta data')

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6201)
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
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "set_info",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--change-ip", "[4001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--change-port", "9000",
             "--change-replication-ip", "[4::10]",
             "--change-replication-port", "9000",
             "--change-device", "sda300", "--change-meta", "other meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6200)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6201)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Check that device was created with given data
        ring = RingBuilder.load(self.tmpfile)
        dev = ring.devs[-1]
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
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Test domain name
        argv = \
            ["", self.tmpfile, "set_info",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--change-ip", "test.test2.com",
             "--change-port", "9000",
             "--change-replication-ip", "r.test2.com",
             "--change-replication-port", "9000",
             "--change-device", "sda300", "--change-meta", "other meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)

        # Check that second device in ring is not affected
        dev = ring.devs[0]
        self.assertEqual(dev['ip'], '127.0.0.1')
        self.assertEqual(dev['port'], 6200)
        self.assertEqual(dev['replication_ip'], '127.0.0.1')
        self.assertEqual(dev['replication_port'], 6200)
        self.assertEqual(dev['device'], 'sda1')
        self.assertEqual(dev['meta'], 'some meta data')

        # Check that second device in ring is not affected
        dev = ring.devs[1]
        self.assertEqual(dev['ip'], '127.0.0.2')
        self.assertEqual(dev['port'], 6201)
        self.assertEqual(dev['device'], 'sda2')
        self.assertEqual(dev['meta'], '')

        # Check that device was created with given data
        dev = ring.devs[-1]
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
        self.create_sample_ring()
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_info"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_info_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "set_info",
                "--ip", "unknown"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_info_already_exists(self):
        self.create_sample_ring()
        # Test Set a device that already exists
        argv = \
            ["", self.tmpfile, "set_info",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6200",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6200",
             "--device", "sda1", "--meta", "some meta data",
             "--change-ip", "127.0.0.2",
             "--change-port", "6201",
             "--change-replication-ip", "127.0.0.2",
             "--change-replication-port", "6201",
             "--change-device", "sda2", "--change-meta", ""]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_min_part_hours(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_min_part_hours", "24"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.min_part_hours, 24)

    def test_set_min_part_hours_number_of_arguments(self):
        self.create_sample_ring()
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_min_part_hours"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_replicas(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_replicas", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.replicas, 3.14159265359)

    def test_set_overload(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "0.19878"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.19878)

    def test_set_overload_negative(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "-0.19878"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.0)

    def test_set_overload_non_numeric(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "swedish fish"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.0)

    def test_set_overload_percent(self):
        self.create_sample_ring()
        argv = "set_overload 10%".split()
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.1)
        self.assertIn('10.00%', out)
        self.assertIn('0.100000', out)

    def test_set_overload_percent_strange_input(self):
        self.create_sample_ring()
        argv = "set_overload 26%%%%".split()
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.26)
        self.assertIn('26.00%', out)
        self.assertIn('0.260000', out)

    def test_server_overload_crazy_high(self):
        self.create_sample_ring()
        argv = "set_overload 10".split()
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 10.0)
        self.assertIn('Warning overload is greater than 100%', out)
        self.assertIn('1000.00%', out)
        self.assertIn('10.000000', out)
        # but it's cool if you do it on purpose
        argv[-1] = '1000%'
        out, err = self.run_srb(*argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 10.0)
        self.assertNotIn('Warning overload is greater than 100%', out)
        self.assertIn('1000.00%', out)
        self.assertIn('10.000000', out)

    def test_set_overload_number_of_arguments(self):
        self.create_sample_ring()
        # Test missing arguments
        argv = ["", self.tmpfile, "set_overload"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_replicas_number_of_arguments(self):
        self.create_sample_ring()
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "set_replicas"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_set_replicas_invalid_value(self):
        self.create_sample_ring()
        # Test not a valid number
        argv = ["", self.tmpfile, "set_replicas", "test"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

        # Test new replicas is 0
        argv = ["", self.tmpfile, "set_replicas", "0"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_validate(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)
        argv = ["", self.tmpfile, "validate"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_validate_composite_builder_file(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        b2, b2_file = write_stub_builder(self.tmpdir, 2)
        cb = CompositeRingBuilder([b1_file, b2_file])
        cb.compose()
        cb_file = os.path.join(self.tmpdir, 'composite.builder')
        cb.save(cb_file)
        argv = ["", cb_file, "validate"]
        with mock.patch("sys.stdout", six.StringIO()) as mock_stdout:
            self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        lines = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("Ring Builder file is invalid", lines[0])
        self.assertIn("appears to be a composite ring builder file", lines[0])
        self.assertFalse(lines[1:])

    def test_validate_empty_file(self):
        open(self.tmpfile, 'a').close
        argv = ["", self.tmpfile, "validate"]
        with mock.patch("sys.stdout", six.StringIO()) as mock_stdout:
            self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        lines = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("Ring Builder file is invalid", lines[0])
        self.assertNotIn("appears to be a composite ring builder file",
                         lines[0])
        self.assertFalse(lines[1:])

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
        with mock.patch("sys.stdout", six.StringIO()) as mock_stdout:
            self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        lines = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("Ring Builder file is invalid", lines[0])
        self.assertNotIn("appears to be a composite ring builder file",
                         lines[0])
        self.assertFalse(lines[1:])

    def test_validate_non_existent_file(self):
        rand_file = '%s/%s' % (tempfile.gettempdir(), str(uuid.uuid4()))
        argv = ["", rand_file, "validate"]
        with mock.patch("sys.stdout", six.StringIO()) as mock_stdout:
            self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        lines = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("Ring Builder file does not exist", lines[0])
        self.assertNotIn("appears to be a composite ring builder file",
                         lines[0])
        self.assertFalse(lines[1:])

    def test_validate_non_accessible_file(self):
        with mock.patch.object(
                RingBuilder, 'load',
                mock.Mock(side_effect=exceptions.PermissionError("boom"))):
            argv = ["", self.tmpfile, "validate"]
            with mock.patch("sys.stdout", six.StringIO()) as mock_stdout:
                self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        lines = mock_stdout.getvalue().strip().split('\n')
        self.assertIn("boom", lines[0])
        self.assertFalse(lines[1:])

    def test_validate_generic_error(self):
        with mock.patch.object(
                RingBuilder, 'load', mock.Mock(
                    side_effect=IOError('Generic error occurred'))):
            argv = ["", self.tmpfile, "validate"]
            self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_search_device_ipv4_old_format(self):
        self.create_sample_ring()
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "search",
                "d0r0z0-127.0.0.1:6200R127.0.0.1:6200/sda1_some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_search_device_ipv6_old_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # write ring file
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "search",
                "d4r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6200"
                "R[2::10]:7000/sda3_some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_search_device_ipv4_new_format(self):
        self.create_sample_ring()
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "search",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6200",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6200",
             "--device", "sda1", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_search_device_ipv6_new_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # write ring file
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "search",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_search_device_domain_new_format(self):
        self.create_sample_ring()
        # add domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        # write ring file
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)

        # Test domain name
        argv = \
            ["", self.tmpfile, "search",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_search_device_number_of_arguments(self):
        self.create_sample_ring()
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "search"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_search_device_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "search",
                "--ip", "unknown"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_list_parts_ipv4_old_format(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)
        # Test ipv4(old format)
        argv = ["", self.tmpfile, "list_parts",
                "d0r0z0-127.0.0.1:6200R127.0.0.1:6200/sda1_some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_list_parts_ipv6_old_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # write ring file
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)

        # Test ipv6(old format)
        argv = ["", self.tmpfile, "list_parts",
                "d4r2z3-[2001:0000:1234:0000:0000:C1C0:ABCD:0876]:6200"
                "R[2::10]:7000/sda3_some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_list_parts_ipv4_new_format(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)
        # Test ipv4(new format)
        argv = \
            ["", self.tmpfile, "list_parts",
             "--id", "0", "--region", "0", "--zone", "0",
             "--ip", "127.0.0.1",
             "--port", "6200",
             "--replication-ip", "127.0.0.1",
             "--replication-port", "6200",
             "--device", "sda1", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_list_parts_ipv6_new_format(self):
        self.create_sample_ring()
        # add IPV6
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # write ring file
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)

        # Test ipv6(new format)
        argv = \
            ["", self.tmpfile, "list_parts",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "[2001:0000:1234:0000:0000:C1C0:ABCD:0876]",
             "--port", "6200",
             "--replication-ip", "[2::10]",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_list_parts_domain_new_format(self):
        self.create_sample_ring()
        # add domain name
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data",
             "--weight", "3.14159265359"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # write ring file
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)

        # Test domain name
        argv = \
            ["", self.tmpfile, "list_parts",
             "--id", "4", "--region", "2", "--zone", "3",
             "--ip", "test.test.com",
             "--port", "6200",
             "--replication-ip", "r.test.com",
             "--replication-port", "7000",
             "--device", "sda3", "--meta", "some meta data"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_list_parts_number_of_arguments(self):
        self.create_sample_ring()
        # Test Number of arguments abnormal
        argv = ["", self.tmpfile, "list_parts"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_list_parts_no_matching(self):
        self.create_sample_ring()
        # Test No matching devices
        argv = ["", self.tmpfile, "list_parts",
                "--ip", "unknown"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_unknown(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "unknown"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_default(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_default_output(self):
        with mock.patch('uuid.uuid4', return_value=mock.Mock(hex=None)):
            self.create_sample_ring()
        out, err = self.run_srb('')
        self.assertOutputStub(out)

    def test_default_output_id_assigned(self):
        ring = self.create_sample_ring()
        out, err = self.run_srb('')
        self.assertOutputStub(out, builder_id=ring.id)

    def test_ipv6_output(self):
        ring = RingBuilder(8, 3, 1)
        ring.add_dev({'weight': 100.0,
                      'region': 0,
                      'zone': 0,
                      'ip': '2001:db8:85a3::8a2e:370:7334',
                      'port': 6200,
                      'device': 'sda1',
                      'meta': 'some meta data',
                      })
        ring.add_dev({'weight': 100.0,
                      'region': 1,
                      'zone': 1,
                      'ip': '127.0.0.1',
                      'port': 66201,
                      'device': 'sda2',
                      })
        ring.add_dev({'weight': 100.0,
                      'region': 2,
                      'zone': 2,
                      'ip': '2001:db8:85a3::8a2e:370:7336',
                      'port': 6202,
                      'device': 'sdc3',
                      'replication_ip': '127.0.10.127',
                      'replication_port': 7070,
                      })
        ring.add_dev({'weight': 100.0,
                      'region': 3,
                      'zone': 3,
                      'ip': '2001:db8:85a3::8a2e:370:7337',
                      'port': 6203,
                      'device': 'sdd4',
                      'replication_ip': '7001:db8:85a3::8a2e:370:7337',
                      'replication_port': 11664,
                      })
        ring.save(self.tmpfile)
        out, err = self.run_srb('')
        self.assertOutputStub(out, builder_id=ring.id)

    def test_default_show_removed(self):
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()

        ring = self.create_sample_ring()

        # Note: it also sets device's weight to zero.
        argv = ["", self.tmpfile, "remove", "--id", "1"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # Setting another device's weight to zero to be sure we distinguish
        # real removed device and device with zero weight.
        argv = ["", self.tmpfile, "set_weight", "0", "--id", "3"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        argv = ["", self.tmpfile]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        expected = "%s, build version 6, id %s\n" \
            "64 partitions, 3.000000 replicas, 4 regions, 4 zones, " \
            "4 devices, 100.00 balance, 0.00 dispersion\n" \
            "The minimum number of hours before a partition can be " \
            "reassigned is 1 (0:00:00 remaining)\n" \
            "The overload factor is 0.00%% (0.000000)\n" \
            "Ring file %s.ring.gz not found, probably " \
            "it hasn't been written yet\n" \
            "Devices:   id region zone ip address:port " \
            "replication ip:port  name weight " \
            "partitions balance flags meta\n" \
            "            0      0    0  127.0.0.1:6200 " \
            "     127.0.0.1:6200  sda1 100.00" \
            "          0 -100.00       some meta data\n" \
            "            1      1    1  127.0.0.2:6201 " \
            "     127.0.0.2:6201  sda2   0.00" \
            "          0    0.00   DEL \n" \
            "            2      2    2  127.0.0.3:6202 " \
            "     127.0.0.3:6202  sdc3 100.00" \
            "          0 -100.00       \n" \
            "            3      3    3  127.0.0.4:6203 " \
            "     127.0.0.4:6203  sdd4   0.00" \
            "          0    0.00       \n" %\
                   (self.tmpfile, ring.id, self.tmpfile)
        self.assertEqual(expected, mock_stdout.getvalue())

    def test_default_sorted_output(self):
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()

        # Create a sample ring and remove/add some devices.
        now = time.time()
        ring = self.create_sample_ring()
        argv = ["", self.tmpfile, "add",
                "--region", "1", "--zone", "2",
                "--ip", "127.0.0.5", "--port", "6004",
                "--replication-ip", "127.0.0.5",
                "--replication-port", "6004",
                "--device", "sda5", "--weight", "100.0"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "remove", "--id", "0"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "remove", "--id", "3"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "rebalance"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)
        argv = \
            ["", self.tmpfile, "add",
             "--region", "2", "--zone", "1",
             "--ip", "127.0.0.6", "--port", "6005",
             "--replication-ip", "127.0.0.6",
             "--replication-port", "6005",
             "--device", "sdb6", "--weight", "100.0"]
        self.assertRaises(SystemExit, ringbuilder.main, argv)

        # Check the order of the devices listed the output.
        argv = ["", self.tmpfile]
        with mock.patch("sys.stdout", mock_stdout), mock.patch(
                "sys.stderr", mock_stderr), mock.patch(
                    'swift.common.ring.builder.time', return_value=now):
            self.assertRaises(SystemExit, ringbuilder.main, argv)
        self.assertOutputStub(mock_stdout.getvalue(), builder_id=ring.id)

    def test_default_ringfile_check(self):
        self.create_sample_ring()

        # ring file not created
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()
        argv = ["", self.tmpfile]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring_not_found_re = re.compile("Ring file .*\.ring\.gz not found")
        self.assertTrue(ring_not_found_re.findall(mock_stdout.getvalue()))

        # write ring file
        argv = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        # ring file is up-to-date
        mock_stdout = six.StringIO()
        argv = ["", self.tmpfile]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring_up_to_date_re = re.compile("Ring file .*\.ring\.gz is up-to-date")
        self.assertTrue(ring_up_to_date_re.findall(mock_stdout.getvalue()))

        # change builder (set weight)
        argv = ["", self.tmpfile, "set_weight", "0", "--id", "3"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        # ring file is obsolete after set_weight
        mock_stdout = six.StringIO()
        argv = ["", self.tmpfile]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring_obsolete_re = re.compile("Ring file .*\.ring\.gz is obsolete")
        self.assertTrue(ring_obsolete_re.findall(mock_stdout.getvalue()))

        # write ring file
        argv = ["", self.tmpfile, "write_ring"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        # ring file up-to-date again
        mock_stdout = six.StringIO()
        argv = ["", self.tmpfile]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        self.assertTrue(ring_up_to_date_re.findall(mock_stdout.getvalue()))

        # Break ring file e.g. just make it empty
        open('%s.ring.gz' % self.tmpfile, 'w').close()
        # ring file is invalid
        mock_stdout = six.StringIO()
        argv = ["", self.tmpfile]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring_invalid_re = re.compile("Ring file .*\.ring\.gz is invalid")
        self.assertTrue(ring_invalid_re.findall(mock_stdout.getvalue()))

    def test_default_no_device_ring_without_exception(self):
        self.create_sample_ring()

        # remove devices from ring file
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()
        for device in ["d0", "d1", "d2", "d3"]:
            argv = ["", self.tmpfile, "remove", device]
            with mock.patch("sys.stdout", mock_stdout):
                with mock.patch("sys.stderr", mock_stderr):
                    self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        # default ring file without exception
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()
        argv = ["", self.tmpfile, "default"]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        deleted_dev_list = (
            "            0      0    0  127.0.0.1:6200      127.0.0.1:6200  "
            "sda1   0.00          0    0.00   DEL some meta data\n"
            "            1      1    1  127.0.0.2:6201      127.0.0.2:6201  "
            "sda2   0.00          0    0.00   DEL \n"
            "            2      2    2  127.0.0.3:6202      127.0.0.3:6202  "
            "sdc3   0.00          0    0.00   DEL \n"
            "            3      3    3  127.0.0.4:6203      127.0.0.4:6203  "
            "sdd4   0.00          0    0.00   DEL \n")

        output = mock_stdout.getvalue()
        self.assertIn("64 partitions", output)
        self.assertIn("all devices have been deleted", output)
        self.assertIn("all devices have been deleted", output)
        self.assertIn(deleted_dev_list, output)

    def test_empty_ring(self):
        self.create_sample_ring(empty=True)

        # default ring file without exception
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()
        argv = ["", self.tmpfile, "default"]
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        output = mock_stdout.getvalue()
        self.assertIn("64 partitions", output)
        self.assertIn("There are no devices in this ring", output)

    def test_pretend_min_part_hours_passed(self):
        self.run_srb("create", 8, 3, 1)
        argv_pretend = ["", self.tmpfile, "pretend_min_part_hours_passed"]
        # pretend_min_part_hours_passed should success, even not rebalanced
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv_pretend)
        self.run_srb("add",
                     "r1z1-10.1.1.1:2345/sda", 100.0,
                     "r1z1-10.1.1.1:2345/sdb", 100.0,
                     "r1z1-10.1.1.1:2345/sdc", 100.0)
        argv_rebalance = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv_rebalance)
        self.run_srb("add", "r1z1-10.1.1.1:2345/sdd", 100.0)
        # rebalance fail without pretend_min_part_hours_passed
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv_rebalance)
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv_pretend)
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv_rebalance)

    def test_rebalance(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "rebalance", "3"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertTrue(ring.validate())

    def test_rebalance_no_device_change(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)
        # Test No change to the device
        argv = ["", self.tmpfile, "rebalance", "3"]
        with mock.patch('swift.common.ring.RingBuilder.save') as mock_save:
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)
        self.assertEqual(len(mock_save.calls), 0)

    def test_rebalance_saves_dispersion_improvement(self):
        # We set up a situation where dispersion improves but balance
        # doesn't. We construct a ring with one zone, then add a second zone
        # concurrently with a new device in the first zone. That first
        # device won't acquire any partitions, so the ring's balance won't
        # change. However, dispersion will improve.

        ring = RingBuilder(6, 6, 1)
        devs = ('d%s' % i for i in itertools.count())
        for i in range(6):
            ring.add_dev({
                'region': 1, 'zone': 1,
                'ip': '10.0.0.1', 'port': 20001, 'weight': 1000,
                'device': next(devs)})
        ring.rebalance()

        # The last guy in zone 1
        ring.add_dev({
            'region': 1, 'zone': 1,
            'ip': '10.0.0.1', 'port': 20001, 'weight': 1000,
            'device': next(devs)})

        # Add zone 2 (same total weight as zone 1)
        for i in range(7):
            ring.add_dev({
                'region': 1, 'zone': 2,
                'ip': '10.0.0.2', 'port': 20001, 'weight': 1000,
                'device': next(devs)})
        ring.pretend_min_part_hours_passed()
        ring.save(self.tmpfile)
        del ring

        # Rebalance once: this gets 1/6th replica into zone 2; the ring is
        # saved because devices changed.
        argv = ["", self.tmpfile, "rebalance", "5759339"]
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)
        rb = RingBuilder.load(self.tmpfile)
        self.assertEqual(rb.dispersion, 33.333333333333336)
        self.assertEqual(rb.get_balance(), 100)
        self.run_srb('pretend_min_part_hours_passed')

        # Rebalance again: this gets 2/6th replica into zone 2, but no devices
        # changed and the balance stays the same. The only improvement is
        # dispersion.

        captured = {}

        def capture_save(rb, path):
            captured['dispersion'] = rb.dispersion
            captured['balance'] = rb.get_balance()
        # The warning is benign; it's just telling the user to keep on
        # rebalancing. The important assertion is that the builder was
        # saved.
        with mock.patch('swift.common.ring.RingBuilder.save', capture_save):
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)
        self.assertEqual(captured, {
            'dispersion': 16.666666666666668,
            'balance': 100,
        })

    def test_rebalance_no_devices(self):
        # Test no devices
        argv = ["", self.tmpfile, "create", "6", "3.14159265359", "1"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_rebalance_remove_zero_weighted_device(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.set_dev_weight(3, 0.0)
        ring.rebalance()
        ring.pretend_min_part_hours_passed()
        ring.remove_dev(3)
        ring.save(self.tmpfile)

        # Test rebalance after remove 0 weighted device
        argv = ["", self.tmpfile, "rebalance", "3"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertTrue(ring.validate())
        self.assertIsNone(ring.devs[3])

    def test_rebalance_resets_time_remaining(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)

        time_path = 'swift.common.ring.builder.time'
        argv = ["", self.tmpfile, "rebalance", "3"]
        time = 0

        # first rebalance, should have 1 hour left before next rebalance
        time += 3600
        with mock.patch(time_path, return_value=time):
            self.assertEqual(ring.min_part_seconds_left, 0)
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)
            self.assertEqual(ring.min_part_seconds_left, 3600)

        # min part hours passed, change ring and save for rebalance
        ring.set_dev_weight(0, ring.devs[0]['weight'] * 2)
        ring.save(self.tmpfile)

        # second rebalance, should have 1 hour left
        time += 3600
        with mock.patch(time_path, return_value=time):
            self.assertEqual(ring.min_part_seconds_left, 0)
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)
            self.assertEqual(ring.min_part_seconds_left, 3600)

    def test_time_remaining(self):
        self.create_sample_ring()
        now = time.time()
        with mock.patch('swift.common.ring.builder.time', return_value=now):
            self.run_srb('rebalance')
            out, err = self.run_srb('rebalance')
        self.assertIn('No partitions could be reassigned', out)
        self.assertIn('must be at least min_part_hours', out)
        self.assertIn('1 hours (1:00:00 remaining)', out)
        the_future = now + 3600
        with mock.patch('swift.common.ring.builder.time',
                        return_value=the_future):
            out, err = self.run_srb('rebalance')
        self.assertIn('No partitions could be reassigned', out)
        self.assertIn('There is no need to do so at this time', out)
        # or you can pretend_min_part_hours_passed
        self.run_srb('pretend_min_part_hours_passed')
        out, err = self.run_srb('rebalance')
        self.assertIn('No partitions could be reassigned', out)
        self.assertIn('There is no need to do so at this time', out)

    def test_rebalance_failure_does_not_reset_last_moves_epoch(self):
        ring = RingBuilder(8, 3, 1)
        ring.add_dev({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                      'ip': '127.0.0.1', 'port': 6010, 'device': 'sda1'})
        ring.add_dev({'id': 1, 'region': 0, 'zone': 0, 'weight': 1,
                      'ip': '127.0.0.1', 'port': 6020, 'device': 'sdb1'})
        ring.add_dev({'id': 2, 'region': 0, 'zone': 0, 'weight': 1,
                      'ip': '127.0.0.1', 'port': 6030, 'device': 'sdc1'})

        time_path = 'swift.common.ring.builder.time'
        argv = ["", self.tmpfile, "rebalance", "3"]

        with mock.patch(time_path, return_value=0):
            ring.rebalance()
        ring.save(self.tmpfile)

        # min part hours not passed
        with mock.patch(time_path, return_value=(3600 * 0.6)):
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)
            self.assertEqual(ring.min_part_seconds_left, 3600 * 0.4)

        ring.save(self.tmpfile)

        # min part hours passed, no partitions need to be moved
        with mock.patch(time_path, return_value=(3600 * 1.5)):
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)
            ring = RingBuilder.load(self.tmpfile)
            self.assertEqual(ring.min_part_seconds_left, 0)

    def test_rebalance_with_seed(self):
        self.create_sample_ring()
        # Test rebalance using explicit seed parameter
        argv = ["", self.tmpfile, "rebalance", "--seed", "2"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_rebalance_removed_devices(self):
        self.create_sample_ring()
        argvs = [
            ["", self.tmpfile, "rebalance", "3"],
            ["", self.tmpfile, "remove", "d0"],
            ["", self.tmpfile, "rebalance", "3"]]
        for argv in argvs:
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_rebalance_min_part_hours_not_passed(self):
        self.create_sample_ring()
        argvs = [
            ["", self.tmpfile, "rebalance", "3"],
            ["", self.tmpfile, "set_weight", "d0", "1000"]]
        for argv in argvs:
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        ring = RingBuilder.load(self.tmpfile)
        last_replica2part2dev = ring._replica2part2dev

        mock_stdout = six.StringIO()
        argv = ["", self.tmpfile, "rebalance", "3"]
        with mock.patch("sys.stdout", mock_stdout):
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)
        expected = "No partitions could be reassigned.\n" + \
                   "The time between rebalances must be " + \
                   "at least min_part_hours: 1 hours"
        self.assertTrue(expected in mock_stdout.getvalue())

        # Messages can be faked, so let's assure that the partition assignment
        # did not change at all, despite the warning
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(last_replica2part2dev, ring._replica2part2dev)

    def test_rebalance_part_power_increase(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.next_part_power = 1
        ring.save(self.tmpfile)

        argv = ["", self.tmpfile, "rebalance", "3"]
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

    def test_write_ring(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        argv = ["", self.tmpfile, "write_ring"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_write_empty_ring(self):
        ring = RingBuilder(6, 3, 1)
        ring.save(self.tmpfile)
        exp_results = {'valid_exit_codes': [2]}
        out, err = self.run_srb("write_ring", exp_results=exp_results)
        self.assertEqual('Unable to write empty ring.\n', out)

    def test_write_builder(self):
        # Test builder file already exists
        self.create_sample_ring()
        argv = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "write_builder"]
        exp_results = {'valid_exit_codes': [2]}
        self.run_srb(*argv, exp_results=exp_results)

    def test_write_builder_fractional_replicas(self):
        # Test builder file already exists
        self.create_sample_ring(replicas=1.2)
        argv = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        ring_file = os.path.join(os.path.dirname(self.tmpfile),
                                 os.path.basename(self.tmpfile) + ".ring.gz")
        os.remove(self.tmpfile)  # loses file...

        argv = ["", ring_file, "write_builder", "24"]
        self.assertIsNone(ringbuilder.main(argv))

        # Note that we've picked up an extension
        builder = RingBuilder.load(self.tmpfile + '.builder')
        # Note that this is different from the original! But it more-closely
        # reflects the reality that we have an extra replica for 12 of 64 parts
        self.assertEqual(builder.replicas, 1.1875)

    def test_write_builder_after_device_removal(self):
        # Test regenerating builder file after having removed a device
        # and lost the builder file
        self.create_sample_ring()

        argv = ["", self.tmpfile, "add", "r1z1-127.0.0.1:6200/sdb", "1.0"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "add", "r1z1-127.0.0.1:6200/sdc", "1.0"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

        argv = ["", self.tmpfile, "remove", "--id", "0"]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)
        argv = ["", self.tmpfile, "rebalance"]
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

        backup_file = os.path.join(os.path.dirname(self.tmpfile),
                                   os.path.basename(self.tmpfile) + ".ring.gz")
        os.remove(self.tmpfile)  # loses file...

        argv = ["", backup_file, "write_builder", "24"]
        self.assertIsNone(ringbuilder.main(argv))

        rb = RingBuilder.load(self.tmpfile + '.builder')
        self.assertIsNotNone(rb._last_part_moves)
        rb._last_part_moves = None
        rb.save(self.tmpfile)

        argv = ["", self.tmpfile + '.builder', "rebalance"]
        self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

    def test_warn_at_risk(self):
        # check that warning is generated when rebalance does not achieve
        # satisfactory balance
        self.create_sample_ring()
        orig_rebalance = RingBuilder.rebalance
        fake_balance = 6

        def fake_rebalance(builder_instance, *args, **kwargs):
            parts, balance, removed_devs = orig_rebalance(builder_instance)
            return parts, fake_balance, removed_devs

        argv = ["", self.tmpfile, "rebalance"]
        with mock.patch("swift.common.ring.builder.RingBuilder.rebalance",
                        fake_rebalance):
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

        # even when some overload is allowed
        self.create_sample_ring(overload=0.05)
        argv = ["", self.tmpfile, "rebalance"]
        with mock.patch("swift.common.ring.builder.RingBuilder.rebalance",
                        fake_rebalance):
            self.assertSystemExit(EXIT_WARNING, ringbuilder.main, argv)

    def test_no_warn_when_balanced(self):
        # check that no warning is generated when satisfactory balance is
        # achieved...
        self.create_sample_ring()
        orig_rebalance = RingBuilder.rebalance
        fake_balance = 5

        def fake_rebalance(builder_instance, *args, **kwargs):
            parts, balance, removed_devs = orig_rebalance(builder_instance)
            return parts, fake_balance, removed_devs

        argv = ["", self.tmpfile, "rebalance"]
        with mock.patch("swift.common.ring.builder.RingBuilder.rebalance",
                        fake_rebalance):
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        # ...or balance is within permitted overload
        self.create_sample_ring(overload=0.06)
        fake_balance = 6
        argv = ["", self.tmpfile, "rebalance"]
        with mock.patch("swift.common.ring.builder.RingBuilder.rebalance",
                        fake_rebalance):
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_invalid_device_name(self):
        self.create_sample_ring()
        for device_name in ["", " ", " sda1", "sda1 ", " meta "]:

            argv = ["",
                    self.tmpfile,
                    "add",
                    "r1z1-127.0.0.1:6200/%s" % device_name,
                    "1"]
            self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

            argv = ["",
                    self.tmpfile,
                    "add",
                    "--region", "1",
                    "--zone", "1",
                    "--ip", "127.0.0.1",
                    "--port", "6200",
                    "--device", device_name,
                    "--weight", "100"]
            self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)

    def test_dispersion_command(self):
        self.create_sample_ring()
        self.run_srb('rebalance')
        out, err = self.run_srb('dispersion -v')
        self.assertIn('dispersion', out.lower())
        self.assertFalse(err)

    def test_dispersion_command_recalculate(self):
        rb = RingBuilder(8, 3, 0)
        for i in range(3):
            i += 1
            rb.add_dev({'region': 1, 'zone': i, 'weight': 1.0,
                        'ip': '127.0.0.%d' % i, 'port': 6000, 'device': 'sda'})
        # extra device in z1
        rb.add_dev({'region': 1, 'zone': 1, 'weight': 1.0,
                    'ip': '127.0.0.1', 'port': 6000, 'device': 'sdb'})
        rb.rebalance()
        self.assertEqual(rb.dispersion, 16.666666666666668)
        # simulate an out-of-date dispersion calculation
        rb.dispersion = 50
        rb.save(self.tempfile)
        old_version = rb.version
        out, err = self.run_srb('dispersion')
        self.assertIn('Dispersion is 50.000000', out)
        out, err = self.run_srb('dispersion --recalculate')
        self.assertIn('Dispersion is 16.666667', out)
        rb = RingBuilder.load(self.tempfile)
        self.assertEqual(rb.version, old_version + 1)

    def test_use_ringfile_as_builderfile(self):
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()

        argv = ["", self.tmpfile, "rebalance", "3"],
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

        argv = ["", "%s.ring.gz" % self.tmpfile]

        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                self.assertSystemExit(EXIT_ERROR, ringbuilder.main, argv)
        expected = "Note: using %s.builder instead of %s.ring.gz " \
            "as builder file\n" \
            "Ring Builder file does not exist: %s.builder\n" % (
                self.tmpfile, self.tmpfile, self.tmpfile)
        self.assertEqual(expected, mock_stdout.getvalue())

    def test_main_no_arguments(self):
        # Test calling main with no arguments
        argv = []
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_main_single_argument(self):
        # Test calling main with single argument
        argv = [""]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_main_with_safe(self):
        # Test calling main with '-safe' argument
        self.create_sample_ring()
        argv = ["-safe", self.tmpfile]
        self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_remove_all_devices(self):
        # Would block without the 'yes' argument
        self.create_sample_ring()
        argv = ["", self.tmpfile, "remove", "--weight", "100", "--yes"]
        with Timeout(5):
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_set_info_all_devices(self):
        # Would block without the 'yes' argument
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_info", "--weight", "100",
                "--change-meta", "something", "--yes"]
        with Timeout(5):
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)

    def test_set_weight_all_devices(self):
        # Would block without the 'yes' argument
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_weight",
                "--weight", "100", "200", "--yes"]
        with Timeout(5):
            self.assertSystemExit(EXIT_SUCCESS, ringbuilder.main, argv)


class TestRebalanceCommand(unittest.TestCase, RunSwiftRingBuilderMixin):

    def __init__(self, *args, **kwargs):
        super(TestRebalanceCommand, self).__init__(*args, **kwargs)

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        tmpf = tempfile.NamedTemporaryFile(dir=self.tmpdir)
        self.tempfile = self.tmpfile = tmpf.name

    def tearDown(self):
        try:
            shutil.rmtree(self.tmpdir, True)
        except OSError:
            pass

    def run_srb(self, *argv):
        mock_stdout = six.StringIO()
        mock_stderr = six.StringIO()

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
        self.assertNotIn("rebalance/repush", out)

        # 2 machines of equal size: balanceable, but not in one pass due to
        # min_part_hours > 0
        self.run_srb("add",
                     "r1z1-10.1.1.2:2345/sda", 100.0,
                     "r1z1-10.1.1.2:2345/sdb", 100.0,
                     "r1z1-10.1.1.2:2345/sdc", 100.0,
                     "r1z1-10.1.1.2:2345/sdd", 100.0)
        self.run_srb("pretend_min_part_hours_passed")
        out, err = self.run_srb("rebalance")
        self.assertIn("rebalance/repush", out)

        # after two passes, it's all balanced out
        self.run_srb("pretend_min_part_hours_passed")
        out, err = self.run_srb("rebalance")
        self.assertNotIn("rebalance/repush", out)

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
        self.assertNotIn("rebalance/repush", out)

        # Now we add in a really big device, but not enough partitions move
        # to fill it in one pass, so we see the rebalance warning.
        self.run_srb("add", "r1z1-10.1.1.1:2345/sdd", 99999.0)
        self.run_srb("pretend_min_part_hours_passed")
        out, err = self.run_srb("rebalance")
        self.assertIn("rebalance/repush", out)

    def test_cached_dispersion_value(self):
        self.run_srb("create", 8, 3, 24)
        self.run_srb("add",
                     "r1z1-10.1.1.1:2345/sda", 100.0,
                     "r1z1-10.1.1.1:2345/sdb", 100.0,
                     "r1z1-10.1.1.1:2345/sdc", 100.0,
                     "r1z1-10.1.1.1:2345/sdd", 100.0)
        self.run_srb('rebalance')
        out, err = self.run_srb()  # list devices
        self.assertIn('dispersion', out)
        # remove cached dispersion value
        builder = RingBuilder.load(self.tempfile)
        builder.dispersion = None
        builder.save(self.tempfile)
        # now dispersion output is suppressed
        out, err = self.run_srb()  # list devices
        self.assertNotIn('dispersion', out)
        # but will show up after rebalance
        self.run_srb('rebalance', '-f')
        out, err = self.run_srb()  # list devices
        self.assertIn('dispersion', out)


if __name__ == '__main__':
    unittest.main()
