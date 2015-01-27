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

import mock
import os
import StringIO
import tempfile
import unittest
import uuid

import swift.cli.ringbuilder
from swift.common import exceptions
from swift.common.ring import RingBuilder


class TestCommands(unittest.TestCase):

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
        self.tmpfile = tmpf.name

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

    def test_create_ring(self):
        argv = ["", self.tmpfile, "create", "6", "3.14159265359", "1"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.part_power, 6)
        self.assertEqual(ring.replicas, 3.14159265359)
        self.assertEqual(ring.min_part_hours, 1)

    def test_list_parts(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "list_parts", "r1"]

        err = None
        try:
            swift.cli.ringbuilder.main(argv)
        except SystemExit as e:
            err = e
        self.assertEqual(err.code, 2)

    def test_add_device(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "add",
                "r2z3-127.0.0.1:6000/sda3_some meta data", "3.14159265359"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)

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

        # Final check, rebalance and check ring is ok
        ring.rebalance()
        self.assertTrue(ring.validate())

    def test_remove_device(self):
        for search_value in self.search_values:
            self.create_sample_ring()
            argv = ["", self.tmpfile, "remove", search_value]
            self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
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

    def test_set_weight(self):
        for search_value in self.search_values:
            self.create_sample_ring()

            argv = ["", self.tmpfile, "set_weight",
                    search_value, "3.14159265359"]
            self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
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

    def test_set_info(self):
        for search_value in self.search_values:

            self.create_sample_ring()
            argv = ["", self.tmpfile, "set_info", search_value,
                    "127.0.1.1:8000/sda1_other meta data"]
            self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)

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

    def test_set_min_part_hours(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_min_part_hours", "24"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.min_part_hours, 24)

    def test_set_replicas(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_replicas", "3.14159265359"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.replicas, 3.14159265359)

    def test_set_overload(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "0.19878"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.19878)

    def test_set_overload_negative(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "-0.19878"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.0)

    def test_set_overload_non_numeric(self):
        self.create_sample_ring()
        argv = ["", self.tmpfile, "set_overload", "swedish fish"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)
        ring = RingBuilder.load(self.tmpfile)
        self.assertEqual(ring.overload, 0.0)

    def test_validate(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.rebalance()
        ring.save(self.tmpfile)
        argv = ["", self.tmpfile, "validate"]
        self.assertRaises(SystemExit, swift.cli.ringbuilder.main, argv)

    def test_validate_empty_file(self):
        open(self.tmpfile, 'a').close
        argv = ["", self.tmpfile, "validate"]
        try:
            swift.cli.ringbuilder.main(argv)
        except SystemExit as e:
            self.assertEquals(e.code, 2)

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
        try:
            swift.cli.ringbuilder.main(argv)
        except SystemExit as e:
            self.assertEquals(e.code, 2)

    def test_validate_non_existent_file(self):
        rand_file = '%s/%s' % ('/tmp', str(uuid.uuid4()))
        argv = ["", rand_file, "validate"]
        try:
            swift.cli.ringbuilder.main(argv)
        except SystemExit as e:
            self.assertEquals(e.code, 2)

    def test_validate_non_accessible_file(self):
        with mock.patch.object(
                RingBuilder, 'load',
                mock.Mock(side_effect=exceptions.PermissionError)):
            argv = ["", self.tmpfile, "validate"]
            try:
                swift.cli.ringbuilder.main(argv)
            except SystemExit as e:
                self.assertEquals(e.code, 2)

    def test_validate_generic_error(self):
        with mock.patch.object(
                RingBuilder, 'load', mock.Mock(
                    side_effect=IOError('Generic error occurred'))):
            argv = ["", self.tmpfile, "validate"]
            try:
                swift.cli.ringbuilder.main(argv)
            except SystemExit as e:
                self.assertEquals(e.code, 2)

    def test_warn_at_risk(self):
        self.create_sample_ring()
        ring = RingBuilder.load(self.tmpfile)
        ring.devs[0]['weight'] = 10
        ring.save(self.tmpfile)
        argv = ["", self.tmpfile, "rebalance"]
        try:
            swift.cli.ringbuilder.main(argv)
        except SystemExit as e:
            self.assertEquals(e.code, 1)


class TestRebalanceCommand(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(TestRebalanceCommand, self).__init__(*args, **kwargs)
        tmpf = tempfile.NamedTemporaryFile()
        self.tempfile = tmpf.name

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
                    swift.cli.ringbuilder.main(srb_args)
        except SystemExit as err:
            if err.code not in (0, 1):  # (success, warning)
                raise
        return (mock_stdout.getvalue(), mock_stderr.getvalue())

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


if __name__ == '__main__':
    unittest.main()
