# Copyright (c) 2010-2017 OpenStack Foundation
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
import os
import random
import tempfile
import unittest
import shutil
import copy

from swift.common.ring import RingBuilder, Ring
from swift.common.ring.composite_builder import (
    compose_rings, CompositeRingBuilder)


def make_device_iter():
    x = 0
    base_port = 6000
    while True:
        yield {'region': 0,  # Note that region may be replaced on the tests
               'zone': 0,
               'ip': '10.0.0.%s' % x,
               'replication_ip': '10.0.0.%s' % x,
               'port': base_port + x,
               'replication_port': base_port + x,
               'device': 'sda',
               'weight': 100.0, }
        x += 1


class BaseTestCompositeBuilder(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.device_iter = make_device_iter()
        self.output_ring = os.path.join(self.tmpdir, 'composite.ring.gz')

    def pop_region_device(self, region):
        dev = next(self.device_iter)
        dev.update({'region': region})
        return dev

    def tearDown(self):
        try:
            shutil.rmtree(self.tmpdir, True)
        except OSError:
            pass

    def save_builder_with_no_id(self, builder, fname):
        orig_to_dict = builder.to_dict

        def fake_to_dict():
            res = orig_to_dict()
            res.pop('id')
            return res

        with mock.patch.object(builder, 'to_dict', fake_to_dict):
            builder.save(fname)

    def save_builders(self, builders, missing_ids=None, prefix='builder'):
        missing_ids = missing_ids or []
        builder_files = []
        for i, builder in enumerate(builders):
            fname = os.path.join(self.tmpdir, '%s_%s.builder' % (prefix, i))
            if i in missing_ids:
                self.save_builder_with_no_id(builder, fname)
            else:
                builder.save(fname)
            builder_files.append(fname)
        return builder_files

    def create_sample_ringbuilders(self, num_builders=2):
        """
        Create sample rings with four devices

        :returns: a list of ring builder instances
        """

        builders = []
        for region in range(num_builders):
            fname = os.path.join(self.tmpdir, 'builder_%s.builder' % region)
            builder = RingBuilder(6, 3, 0)
            for _ in range(5):
                dev = self.pop_region_device(region)
                builder.add_dev(dev)
            # remove last dev to simulate a ring with some history
            builder.remove_dev(dev['id'])
            # add a dev that won't be assigned any parts
            new_dev = self.pop_region_device(region)
            new_dev['weight'] = 0
            builder.add_dev(new_dev)
            builder.rebalance()
            builder.save(fname)
            self.assertTrue(os.path.exists(fname))
            builders.append(builder)

        return builders

    def add_dev_and_rebalance(self, builder, weight=None):
        dev = next(builder._iter_devs())
        new_dev = self.pop_region_device(dev['region'])
        if weight is not None:
            new_dev['weight'] = weight
        builder.add_dev(new_dev)
        builder.rebalance()

    def assertDevices(self, composite_ring, builders):
        """
        :param composite_ring: a Ring instance
        :param builders: a list of RingBuilder instances for assertion
        """
        # assert all component devices are in composite device table
        builder_devs = []
        for builder in builders:
            builder_devs.extend([
                (dev['ip'], dev['port'], dev['device'])
                for dev in builder._iter_devs()])

        got_devices = [
            (dev['ip'], dev['port'], dev['device'])
            for dev in composite_ring.devs if dev]
        self.assertEqual(sorted(builder_devs), sorted(got_devices),
                         "composite_ring mismatched with part of the rings")

        # assert composite device ids correctly index into the dev list
        dev_ids = []
        for i, dev in enumerate(composite_ring.devs):
            if dev:
                self.assertEqual(i, dev['id'])
                dev_ids.append(dev['id'])
        self.assertEqual(len(builder_devs), len(dev_ids))

        def uniqueness(dev):
            return (dev['ip'], dev['port'], dev['device'])

        # assert part assignment is ordered by ring order
        part_count = composite_ring.partition_count
        for part in range(part_count):
            primaries = [uniqueness(primary) for primary in
                         composite_ring.get_part_nodes(part)]
            offset = 0
            for builder in builders:
                sub_primaries = [uniqueness(primary) for primary in
                                 builder.get_part_devices(part)]
                self.assertEqual(
                    primaries[offset:offset + builder.replicas],
                    sub_primaries,
                    "composite ring is not ordered by ring order, %s, %s"
                    % (primaries, sub_primaries))
                offset += builder.replicas

    def check_composite_ring(self, ring_file, builders):
        got_ring = Ring(ring_file)
        self.assertEqual(got_ring.partition_count, builders[0].parts)
        self.assertEqual(got_ring.replica_count,
                         sum(b.replicas for b in builders))
        self.assertEqual(got_ring._part_shift, builders[0].part_shift)
        self.assertDevices(got_ring, builders)

    def check_composite_meta(self, cb_file, builder_files, version=1):
        with open(cb_file) as fd:
            actual = json.load(fd)
        builders = [RingBuilder.load(fname) for fname in builder_files]
        expected_metadata = {
            'saved_path': os.path.abspath(cb_file),
            'serialization_version': 1,
            'version': version,
            'components': [
                {'id': builder.id,
                 'version': builder.version,
                 'replicas': builder.replicas,
                 }
                for builder in builders
            ],
            'component_builder_files':
                dict((builder.id, os.path.abspath(builder_files[i]))
                     for i, builder in enumerate(builders))
        }
        self.assertEqual(expected_metadata, actual)


class TestCompositeBuilder(BaseTestCompositeBuilder):
    def test_compose_rings(self):
        def do_test(builder_count):
            builders = self.create_sample_ringbuilders(builder_count)
            rd = compose_rings(builders)
            rd.save(self.output_ring)
            self.check_composite_ring(self.output_ring, builders)

        do_test(2)
        do_test(3)
        do_test(4)

    def test_composite_same_region_in_the_different_rings_error(self):
        builder_1 = self.create_sample_ringbuilders(1)
        builder_2 = self.create_sample_ringbuilders(1)
        builders = builder_1 + builder_2
        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn('Same region found in different rings',
                      cm.exception.message)

    def test_composite_only_one_ring_in_the_args_error(self):
        builders = self.create_sample_ringbuilders(1)
        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn(
            'Two or more component builders are required.',
            cm.exception.message)

    def test_composite_same_device_in_the_different_rings_error(self):
        builders = self.create_sample_ringbuilders(2)
        same_device = copy.deepcopy(builders[0].devs[0])

        # create one more ring which duplicates a device in the first ring
        builder = RingBuilder(6, 3, 1)
        _, fname = tempfile.mkstemp(dir=self.tmpdir)
        # add info to feed to add_dev
        same_device.update({'region': 2, 'weight': 100})
        builder.add_dev(same_device)

        # add rest of the devices, which are unique
        for _ in range(3):
            dev = self.pop_region_device(2)
            builder.add_dev(dev)
        builder.rebalance()
        builder.save(fname)
        # sanity
        self.assertTrue(os.path.exists(fname))

        builders.append(builder)

        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn(
            'Duplicate ip/port/device combination %(ip)s/%(port)s/%(device)s '
            'found in builders at indexes 0 and 2' %
            same_device, cm.exception.message)

    def test_different_part_power_error(self):
        # create a ring builder
        # (default, part power is 6 with create_sample_ringbuilders)
        builders = self.create_sample_ringbuilders(1)

        # prepare another ring which has different part power
        incorrect_builder = RingBuilder(4, 3, 1)
        _, fname = tempfile.mkstemp(dir=self.tmpdir)
        for _ in range(4):
            dev = self.pop_region_device(1)
            incorrect_builder.add_dev(dev)
        incorrect_builder.rebalance()
        incorrect_builder.save(fname)
        # sanity
        self.assertTrue(os.path.exists(fname))

        # sanity
        correct_builder = builders[0]
        self.assertNotEqual(correct_builder.part_shift,
                            incorrect_builder.part_shift)
        self.assertNotEqual(correct_builder.part_power,
                            incorrect_builder.part_power)

        builders.append(incorrect_builder)
        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn("All builders must have same value for 'part_power'",
                      cm.exception.message)

    def test_compose_rings_float_replica_count_builder_error(self):
        builders = self.create_sample_ringbuilders(1)

        # prepare another ring which has float replica count
        incorrect_builder = RingBuilder(6, 1.5, 1)
        _, fname = tempfile.mkstemp(dir=self.tmpdir)
        for _ in range(4):
            dev = self.pop_region_device(1)
            incorrect_builder.add_dev(dev)
        incorrect_builder.rebalance()
        incorrect_builder.save(fname)
        # sanity
        self.assertTrue(os.path.exists(fname))
        self.assertEqual(1.5, incorrect_builder.replicas)
        # the first replica has 2 ** 6 partitions
        self.assertEqual(
            2 ** 6, len(incorrect_builder._replica2part2dev[0]))
        # but the second replica has the half of the first partitions
        self.assertEqual(
            2 ** 5, len(incorrect_builder._replica2part2dev[1]))
        builders.append(incorrect_builder)

        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn("Problem with builders", cm.exception.message)
        self.assertIn("Non integer replica count", cm.exception.message)

    def test_compose_rings_rebalance_needed(self):
        builders = self.create_sample_ringbuilders(2)

        # add a new device to builider 1 but no rebalance
        dev = self.pop_region_device(1)
        builders[1].add_dev(dev)
        self.assertTrue(builders[1].devs_changed)  # sanity check
        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn("Problem with builders", cm.exception.message)
        self.assertIn("Builder needs rebalance", cm.exception.message)
        # after rebalance, that works (sanity)
        builders[1].rebalance()
        compose_rings(builders)

    def test_different_replica_count_works(self):
        # create a ring builder
        # (default, part power is 6 with create_sample_ringbuilders)
        builders = self.create_sample_ringbuilders(1)

        # prepare another ring which has different replica count
        builder = RingBuilder(6, 1, 1)
        _, fname = tempfile.mkstemp(dir=self.tmpdir)
        for _ in range(4):
            dev = self.pop_region_device(1)
            builder.add_dev(dev)
        builder.rebalance()
        builder.save(fname)
        # sanity
        self.assertTrue(os.path.exists(fname))
        builders.append(builder)

        rd = compose_rings(builders)
        rd.save(self.output_ring)
        got_ring = Ring(self.output_ring)
        self.assertEqual(got_ring.partition_count, 2 ** 6)
        self.assertEqual(got_ring.replica_count, 4)  # 3 + 1
        self.assertEqual(got_ring._part_shift, 26)
        self.assertDevices(got_ring, builders)

    def test_ring_swap(self):
        # sanity
        builders = sorted(self.create_sample_ringbuilders(2))
        rd = compose_rings(builders)
        rd.save(self.output_ring)
        got_ring = Ring(self.output_ring)
        self.assertEqual(got_ring.partition_count, 2 ** 6)
        self.assertEqual(got_ring.replica_count, 6)
        self.assertEqual(got_ring._part_shift, 26)
        self.assertDevices(got_ring, builders)

        # even if swapped, it works
        reverse_builders = sorted(builders, reverse=True)
        self.assertNotEqual(reverse_builders, builders)
        rd = compose_rings(reverse_builders)
        rd.save(self.output_ring)
        got_ring = Ring(self.output_ring)
        self.assertEqual(got_ring.partition_count, 2 ** 6)
        self.assertEqual(got_ring.replica_count, 6)
        self.assertEqual(got_ring._part_shift, 26)
        self.assertDevices(got_ring, reverse_builders)

        # but if the composite rings are different order, the composite ring
        # *will* be different. Note that the CompositeRingBuilder class will
        # check builder order against the existing ring and fail if the order
        # is different (actually checking the metadata). See also
        # test_compose_different_builder_order
        with self.assertRaises(AssertionError) as cm:
            self.assertDevices(got_ring, builders)

        self.assertIn("composite ring is not ordered by ring order",
                      cm.exception.message)


class TestCompositeRingBuilder(BaseTestCompositeBuilder):
    def test_compose_with_builder_files(self):
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json')
        builders = self.create_sample_ringbuilders(2)
        cb, _ = self._make_composite_builder(builders)
        cb.save(cb_file)

        for i, b in enumerate(builders):
            self.add_dev_and_rebalance(b)
        self.save_builders(builders)
        cb = CompositeRingBuilder.load(cb_file)
        cb.compose().save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)

    def _make_composite_builder(self, builders):
        # helper to compose a ring, save it and sanity check it
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        cb.compose().save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)
        return cb, builder_files

    def test_compose_ok(self):
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json')
        builders = self.create_sample_ringbuilders(2)
        # make first version of composite ring
        cb, builder_files = self._make_composite_builder(builders)
        # check composite builder persists ok
        cb.save(cb_file)
        self.assertTrue(os.path.exists(cb_file))
        self.check_composite_meta(cb_file, builder_files)
        # and reloads ok
        cb = CompositeRingBuilder.load(cb_file)
        self.assertEqual(1, cb.version)
        # composes after with no component builder changes will fail...
        with self.assertRaises(ValueError) as cm:
            cb.compose()
        self.assertIn('None of the component builders has been modified',
                      cm.exception.message)
        self.assertEqual(1, cb.version)
        # ...unless we force it
        cb.compose(force=True).save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)
        self.assertEqual(2, cb.version)
        # check composite builder persists ok again
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json2')
        cb.save(cb_file)
        self.assertTrue(os.path.exists(cb_file))
        self.check_composite_meta(cb_file, builder_files, version=2)

    def test_compose_modified_component_builders(self):
        # check it's ok to compose again with same but modified builders
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json')
        builders = self.create_sample_ringbuilders(2)
        cb, builder_files = self._make_composite_builder(builders)
        ring = Ring(self.output_ring)
        orig_devs = [dev for dev in ring.devs if dev]
        self.assertEqual(10, len(orig_devs))  # sanity check
        self.add_dev_and_rebalance(builders[1])
        builder_files = self.save_builders(builders)
        cb.compose().save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)
        ring = Ring(self.output_ring)
        modified_devs = [dev for dev in ring.devs if dev]
        self.assertEqual(len(orig_devs) + 1, len(modified_devs))
        # check composite builder persists ok
        cb.save(cb_file)
        self.assertTrue(os.path.exists(cb_file))
        self.check_composite_meta(cb_file, builder_files, version=2)
        # and reloads ok
        cb = CompositeRingBuilder.load(cb_file)
        # and composes ok after reload
        cb.compose(force=True).save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)
        # check composite builder persists ok again
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json2')
        cb.save(cb_file)
        self.assertTrue(os.path.exists(cb_file))
        self.check_composite_meta(cb_file, builder_files, version=3)

    def test_compose_override_component_builders(self):
        # check passing different builder files to the compose() method
        # overrides loaded builder files
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json')
        builders = self.create_sample_ringbuilders(2)
        cb, builder_files = self._make_composite_builder(builders)
        # modify builders and save in different files
        self.add_dev_and_rebalance(builders[1])
        with self.assertRaises(ValueError):
            cb.compose(builder_files)  # sanity check - originals are unchanged
        other_files = self.save_builders(builders, prefix='other')
        cb.compose(other_files).save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)
        # check composite builder persists ok
        cb.save(cb_file)
        self.assertTrue(os.path.exists(cb_file))
        self.check_composite_meta(cb_file, other_files, version=2)
        # and reloads ok
        cb = CompositeRingBuilder.load(cb_file)
        # and composes ok after reload
        cb.compose(force=True).save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)
        # check composite builder persists ok again
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json2')
        cb.save(cb_file)
        self.assertTrue(os.path.exists(cb_file))
        self.check_composite_meta(cb_file, other_files, version=3)

    def test_abs_paths_persisted(self):
        cwd = os.getcwd()
        try:
            os.chdir(self.tmpdir)
            builders = self.create_sample_ringbuilders(2)
            builder_files = self.save_builders(builders)
            rel_builder_files = [os.path.basename(bf) for bf in builder_files]
            cb = CompositeRingBuilder(rel_builder_files)
            cb.compose().save(self.output_ring)
            self.check_composite_ring(self.output_ring, builders)
            cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json')
            rel_cb_file = os.path.basename(cb_file)
            cb.save(rel_cb_file)
            self.check_composite_meta(rel_cb_file, rel_builder_files)
        finally:
            os.chdir(cwd)

    def test_compose_insufficient_builders(self):
        def do_test(builder_files):
            cb = CompositeRingBuilder(builder_files)
            with self.assertRaises(ValueError) as cm:
                cb.compose()
            self.assertIn('Two or more component builders are required',
                          cm.exception.message)

            cb = CompositeRingBuilder()
            with self.assertRaises(ValueError) as cm:
                cb.compose(builder_files)
            self.assertIn('Two or more component builders are required',
                          cm.exception.message)

        builders = self.create_sample_ringbuilders(3)
        builder_files = self.save_builders(builders)
        do_test([])
        do_test(builder_files[:1])

    def test_compose_missing_builder_id(self):
        def check_missing_id(cb, builders):
            # not ok to compose with builder_files that have no id assigned
            orig_version = cb.version
            no_id = random.randint(0, len(builders) - 1)
            # rewrite the builder files so that one has missing id
            self.save_builders(builders, missing_ids=[no_id])
            with self.assertRaises(ValueError) as cm:
                cb.compose()
            error_lines = cm.exception.message.split('\n')
            self.assertIn("Problem with builder at index %s" % no_id,
                          error_lines[0])
            self.assertIn("id attribute has not been initialised",
                          error_lines[0])
            self.assertFalse(error_lines[1:])
            self.assertEqual(orig_version, cb.version)
        # check with compose not previously called, cb has no existing metadata
        builders = self.create_sample_ringbuilders(3)
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        check_missing_id(cb, builders)
        # now save good copies of builders and compose so this cb has
        # existing component metadata
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        cb.compose()  # cb now has component metadata
        check_missing_id(cb, builders)

    def test_compose_duplicate_builder_ids(self):
        builders = self.create_sample_ringbuilders(3)
        builders[2]._id = builders[0]._id
        cb = CompositeRingBuilder(self.save_builders(builders))
        with self.assertRaises(ValueError) as cm:
            cb.compose()
        error_lines = cm.exception.message.split('\n')
        self.assertIn("Builder id %r used at indexes 0, 2" % builders[0].id,
                      error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(0, cb.version)

    def test_compose_ring_unchanged_builders(self):
        def do_test(cb, builder_files):
            with self.assertRaises(ValueError) as cm:
                cb.compose(builder_files)
            error_lines = cm.exception.message.split('\n')
            self.assertIn("None of the component builders has been modified",
                          error_lines[0])
            self.assertFalse(error_lines[1:])
            self.assertEqual(1, cb.version)

        builders = self.create_sample_ringbuilders(2)
        cb, builder_files = self._make_composite_builder(builders)
        # not ok to compose again with same *unchanged* builders
        do_test(cb, builder_files)
        # even if we rewrite the files
        builder_files = self.save_builders(builders)
        do_test(cb, builder_files)
        # even if we rename the files
        builder_files = self.save_builders(builders, prefix='other')
        do_test(cb, builder_files)

    def test_compose_older_builder(self):
        # make first version of composite ring
        builders = self.create_sample_ringbuilders(2)
        cb, builder_files = self._make_composite_builder(builders)
        old_builders = [copy.deepcopy(b) for b in builders]
        for i, b in enumerate(builders):
            self.add_dev_and_rebalance(b)
            self.assertLess(old_builders[i].version, b.version)
        self.save_builders(builders)
        cb.compose()  # newer version
        self.assertEqual(2, cb.version)  # sanity check
        # not ok to use old versions of same builders
        self.save_builders([old_builders[0], builders[1]])
        with self.assertRaises(ValueError) as cm:
            cb.compose()
        error_lines = cm.exception.message.split('\n')
        self.assertIn("Invalid builder change at index 0", error_lines[0])
        self.assertIn("Older builder version", error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(2, cb.version)
        # not even if one component ring has changed
        self.add_dev_and_rebalance(builders[1])
        self.save_builders([old_builders[0], builders[1]])
        with self.assertRaises(ValueError) as cm:
            cb.compose()
        error_lines = cm.exception.message.split('\n')
        self.assertIn("Invalid builder change at index 0", error_lines[0])
        self.assertIn("Older builder version", error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(2, cb.version)

    def test_compose_different_number_builders(self):
        # not ok to use a different number of component rings
        builders = self.create_sample_ringbuilders(3)
        cb, builder_files = self._make_composite_builder(builders[:2])

        def do_test(bad_builders):
            with self.assertRaises(ValueError) as cm:
                cb.compose(self.save_builders(bad_builders))
            error_lines = cm.exception.message.split('\n')
            self.assertFalse(error_lines[1:])
            self.assertEqual(1, cb.version)
            return error_lines

        error_lines = do_test(builders[:1])  # too few
        self.assertIn("Missing builder at index 1", error_lines[0])
        error_lines = do_test(builders)  # too many
        self.assertIn("Unexpected extra builder at index 2", error_lines[0])

    def test_compose_different_builders(self):
        # not ok to change component rings
        builders = self.create_sample_ringbuilders(3)
        cb, builder_files = self._make_composite_builder(builders[:2])
        # ensure builder[0] is newer version so that's not the problem
        self.add_dev_and_rebalance(builders[0])
        with self.assertRaises(ValueError) as cm:
            cb.compose(self.save_builders([builders[0], builders[2]]))
        error_lines = cm.exception.message.split('\n')
        self.assertIn("Invalid builder change at index 1", error_lines[0])
        self.assertIn("Attribute mismatch for id", error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(1, cb.version)

    def test_compose_different_builder_order(self):
        # not ok to change order of component rings
        builders = self.create_sample_ringbuilders(4)
        cb, builder_files = self._make_composite_builder(builders)
        builder_files.reverse()
        with self.assertRaises(ValueError) as cm:
            cb.compose(builder_files)
        error_lines = cm.exception.message.split('\n')
        for i, line in enumerate(error_lines):
            self.assertIn("Invalid builder change at index %s" % i, line)
            self.assertIn("Attribute mismatch for id", line)
        self.assertEqual(1, cb.version)

    def test_compose_replica_count_changed(self):
        # not ok to change the number of replicas in a ring
        builders = self.create_sample_ringbuilders(3)
        cb, builder_files = self._make_composite_builder(builders)
        builders[0].set_replicas(4)
        self.save_builders(builders)
        with self.assertRaises(ValueError) as cm:
            cb.compose()
        error_lines = cm.exception.message.split('\n')
        for i, line in enumerate(error_lines):
            self.assertIn("Invalid builder change at index 0", line)
            self.assertIn("Attribute mismatch for replicas", line)
        self.assertEqual(1, cb.version)

    def test_load_errors(self):
        bad_file = os.path.join(self.tmpdir, 'bad_file.json')
        with self.assertRaises(IOError):
            CompositeRingBuilder.load(bad_file)

        def check_bad_content(content):
            with open(bad_file, 'wb') as fp:
                fp.write(content)
            try:
                with self.assertRaises(ValueError) as cm:
                    CompositeRingBuilder.load(bad_file)
                self.assertIn(
                    "File does not contain valid composite ring data",
                    cm.exception.message)
            except AssertionError as err:
                raise AssertionError('With content %r: %s' % (content, err))

        for content in ('', 'not json', json.dumps({}), json.dumps([])):
            check_bad_content(content)

        good_content = {
            'components': [
                {'version': 1, 'id': 'uuid_x', 'replicas': 12},
                {'version': 2, 'id': 'uuid_y', 'replicas': 12}
            ],
            'builder_files': {'uuid_x': '/path/to/file_x',
                              'uuid_y': '/path/to/file_y'},
            'version': 99}
        for missing in good_content:
            bad_content = dict(good_content)
            bad_content.pop(missing)
            check_bad_content(json.dumps(bad_content))

    def test_save_errors(self):
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json')

        def do_test(cb):
            with self.assertRaises(ValueError) as cm:
                cb.save(cb_file)
            self.assertIn("No composed ring to save", cm.exception.message)

        do_test(CompositeRingBuilder())
        do_test(CompositeRingBuilder([]))
        do_test(CompositeRingBuilder(['file1', 'file2']))


if __name__ == '__main__':
    unittest.main()
