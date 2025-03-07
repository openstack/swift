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
from contextlib import contextmanager

from unittest import mock
import os
import random
import tempfile
import unittest
import shutil
import copy
import time

from collections import defaultdict, Counter

from swift.common.exceptions import RingBuilderError
from swift.common.ring import RingBuilder, Ring
from swift.common.ring.composite_builder import (
    compose_rings, CompositeRingBuilder, CooperativeRingBuilder)


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

    def create_sample_ringbuilders(self, num_builders=2, rebalance=True):
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
            if rebalance:
                builder.rebalance()
            builder.save(fname)
            self.assertTrue(os.path.exists(fname))
            builders.append(builder)

        return builders

    def add_dev(self, builder, weight=None, region=None):
        if region is None:
            dev = next(builder._iter_devs())
            region = dev['region']
        new_dev = self.pop_region_device(region)
        if weight is not None:
            new_dev['weight'] = weight
        builder.add_dev(new_dev)

    def add_dev_and_rebalance(self, builder, weight=None):
        self.add_dev(builder, weight)
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

    def _make_composite_builder(self, builders):
        # helper to compose a ring, save it and sanity check it
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        cb.compose().save(self.output_ring)
        self.check_composite_ring(self.output_ring, builders)
        return cb, builder_files


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
                      cm.exception.args[0])

    def test_composite_only_one_ring_in_the_args_error(self):
        builders = self.create_sample_ringbuilders(1)
        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn(
            'Two or more component builders are required.',
            cm.exception.args[0])

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
            same_device, cm.exception.args[0])

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
                      cm.exception.args[0])

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
        self.assertIn("Problem with builders", cm.exception.args[0])
        self.assertIn("Non integer replica count", cm.exception.args[0])

    def test_compose_rings_rebalance_needed(self):
        builders = self.create_sample_ringbuilders(2)

        # add a new device to builder 1 but no rebalance
        dev = self.pop_region_device(1)
        builders[1].add_dev(dev)
        self.assertTrue(builders[1].devs_changed)  # sanity check
        with self.assertRaises(ValueError) as cm:
            compose_rings(builders)
        self.assertIn("Problem with builders", cm.exception.args[0])
        self.assertIn("Builder needs rebalance", cm.exception.args[0])
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
        builders = self.create_sample_ringbuilders(2)
        rd = compose_rings(builders)
        rd.save(self.output_ring)
        got_ring = Ring(self.output_ring)
        self.assertEqual(got_ring.partition_count, 2 ** 6)
        self.assertEqual(got_ring.replica_count, 6)
        self.assertEqual(got_ring._part_shift, 26)
        self.assertDevices(got_ring, builders)

        # even if swapped, it works
        reverse_builders = builders[::-1]
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
                      cm.exception.args[0])


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
        # compose detects if no component builder changes, if we ask it to...
        with self.assertRaises(ValueError) as cm:
            cb.compose(require_modified=True)
        self.assertIn('None of the component builders has been modified',
                      cm.exception.args[0])
        self.assertEqual(1, cb.version)
        # ...but by default will compose again despite no changes to components
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
            # sanity check - originals are unchanged
            cb.compose(builder_files, require_modified=True)
        other_files = self.save_builders(builders, prefix='other')
        cb.compose(other_files, require_modified=True).save(self.output_ring)
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
                    cm.exception.args[0])
            except AssertionError as err:
                raise AssertionError('With content %r: %s' % (content, err))

        for content in ('', 'not json', json.dumps({}), json.dumps([])):
            check_bad_content(content.encode('ascii'))

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
            check_bad_content(json.dumps(bad_content).encode('ascii'))

    def test_save_errors(self):
        cb_file = os.path.join(self.tmpdir, 'test-composite-ring.json')

        def do_test(cb):
            with self.assertRaises(ValueError) as cm:
                cb.save(cb_file)
            self.assertIn("No composed ring to save", cm.exception.args[0])

        do_test(CompositeRingBuilder())
        do_test(CompositeRingBuilder([]))
        do_test(CompositeRingBuilder(['file1', 'file2']))

    def test_rebalance(self):
        @contextmanager
        def mock_rebalance():
            # captures component builder rebalance call results, yields a dict
            # that maps builder -> results
            calls = defaultdict(list)
            orig_func = RingBuilder.rebalance

            def func(builder, **kwargs):
                result = orig_func(builder, **kwargs)
                calls[builder].append(result)
                return result

            with mock.patch('swift.common.ring.RingBuilder.rebalance', func):
                yield calls

        def check_results():
            self.assertEqual(2, len(rebalance_calls))  # 2 builders called
            for calls in rebalance_calls.values():
                self.assertFalse(calls[1:])  # 1 call to each builder

            self.assertEqual(sorted(expected_ids),
                             sorted([b.id for b in rebalance_calls]))
            self.assertEqual(sorted(expected_versions),
                             sorted([b.version for b in rebalance_calls]))
            for b in rebalance_calls:
                self.assertEqual(set(rebalance_calls.keys()),
                                 set(b.parent_builder._builders))

            # check the rebalanced builders were saved
            written_builders = [RingBuilder.load(f) for f in builder_files]
            self.assertEqual(expected_ids,
                             [b.id for b in written_builders])
            self.assertEqual(expected_versions,
                             [b.version for b in written_builders])

            # check returned results, should be in component order
            self.assertEqual(2, len(results))
            self.assertEqual(builder_files,
                             [r['builder_file'] for r in results])
            self.assertEqual(expected_versions,
                             [r['builder'].version for r in results])
            self.assertEqual(expected_ids, [r['builder'].id for r in results])
            self.assertEqual(
                [rebalance_calls[r['builder']][0] for r in results],
                [r['result'] for r in results])

        # N.B. the sample builders have zero min_part_hours
        builders = self.create_sample_ringbuilders(2)
        expected_versions = [b.version + 1 for b in builders]
        expected_ids = [b.id for b in builders]

        # test rebalance loads component builders
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        with mock_rebalance() as rebalance_calls:
            results = cb.rebalance()
        check_results()

        # test loading builder files via load_components
        # revert builder files to original builder state
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder()
        cb.load_components(builder_files)
        with mock_rebalance() as rebalance_calls:
            results = cb.rebalance()
        check_results()

    def test_rebalance_errors(self):
        cb = CompositeRingBuilder()
        with self.assertRaises(ValueError) as cm:
            cb.rebalance()
        self.assertIn('Two or more component builders are required',
                      cm.exception.args[0])

        builders = self.create_sample_ringbuilders(2)
        cb, builder_files = self._make_composite_builder(builders)
        with mock.patch('swift.common.ring.RingBuilder.rebalance',
                        side_effect=RingBuilderError('test')):
            with mock.patch('swift.common.ring.composite_builder.shuffle',
                            lambda x: x):
                with self.assertRaises(RingBuilderError) as cm:
                    cb.rebalance()
        self.assertIn('An error occurred while rebalancing component %s' %
                      builder_files[0], str(cm.exception))
        self.assertIsNone(cb._builders)

        with mock.patch('swift.common.ring.RingBuilder.validate',
                        side_effect=RingBuilderError('test')):
            with mock.patch('swift.common.ring.composite_builder.shuffle',
                            lambda x: x):
                with self.assertRaises(RingBuilderError) as cm:
                    cb.rebalance()
        self.assertIn('An error occurred while rebalancing component %s' %
                      builder_files[0], str(cm.exception))
        self.assertIsNone(cb._builders)

    def test_rebalance_with_unrebalanced_builders(self):
        # create 2 non-rebalanced rings
        builders = self.create_sample_ringbuilders(rebalance=False)
        # save builders
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        # sanity, it is impossible to compose un-rebalanced component rings
        with self.assertRaises(ValueError) as cm:
            cb.compose()
        self.assertIn("Builder needs rebalance", cm.exception.args[0])
        # but ok to compose after rebalance
        cb.rebalance()
        rd = cb.compose()
        rd.save(self.output_ring)
        rebalanced_builders = [RingBuilder.load(f) for f in builder_files]
        self.check_composite_ring(self.output_ring, rebalanced_builders)


class TestLoadComponents(BaseTestCompositeBuilder):
    # Tests for the loading of component builders.
    def _call_method_under_test(self, cb, *args, **kwargs):
        # Component builder loading is triggered by the load_components method
        # and the compose method. This method provides a hook for subclasses to
        # configure a different method to repeat the component loading tests.
        cb.load_components(*args, **kwargs)

    def test_load_components(self):
        builders = self.create_sample_ringbuilders(2)
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        # check lazy loading
        self.assertEqual(builder_files, cb._builder_files)
        self.assertFalse(cb._builders)  # none loaded yet

        # check loading configured files
        self._call_method_under_test(cb)
        self.assertEqual(builder_files, cb._builder_files)
        for i, builder in enumerate(cb._builders):
            self.assertEqual(builders[i].id, builder.id)
            self.assertEqual(builders[i].devs, builder.devs)

        # modify builders and save in different files
        self.add_dev_and_rebalance(builders[0])
        other_files = self.save_builders(builders, prefix='other')
        # reload from other files
        self._call_method_under_test(cb, other_files)
        self.assertEqual(other_files, cb._builder_files)
        for i, builder in enumerate(cb._builders):
            self.assertEqual(builders[i].id, builder.id)
            self.assertEqual(builders[i].devs, builder.devs)

        # modify builders again and save in same files
        self.add_dev_and_rebalance(builders[1])
        self.save_builders(builders, prefix='other')
        # reload from same files
        self._call_method_under_test(cb)
        self.assertEqual(other_files, cb._builder_files)
        for i, builder in enumerate(cb._builders):
            self.assertEqual(builders[i].id, builder.id)
            self.assertEqual(builders[i].devs, builder.devs)

    def test_load_components_insufficient_builders(self):
        def do_test(builder_files, force):
            cb = CompositeRingBuilder(builder_files)
            with self.assertRaises(ValueError) as cm:
                self._call_method_under_test(cb, builder_files,
                                             force=force)
            self.assertIn('Two or more component builders are required',
                          cm.exception.args[0])

            cb = CompositeRingBuilder()
            with self.assertRaises(ValueError) as cm:
                self._call_method_under_test(cb, builder_files,
                                             force=force)
            self.assertIn('Two or more component builders are required',
                          cm.exception.args[0])

        builders = self.create_sample_ringbuilders(3)
        builder_files = self.save_builders(builders)
        do_test([], force=False)
        do_test([], force=True)  # this error is never ignored
        do_test(builder_files[:1], force=False)
        do_test(builder_files[:1], force=True)  # this error is never ignored

    def test_load_components_missing_builder_id(self):
        def check_missing_id(cb, builders):
            # not ok to load builder_files that have no id assigned
            orig_version = cb.version
            no_id = random.randint(0, len(builders) - 1)
            # rewrite the builder files so that one has missing id
            builder_files = self.save_builders(builders, missing_ids=[no_id])

            def do_check(force):
                with self.assertRaises(ValueError) as cm:
                    self._call_method_under_test(cb, builder_files,
                                                 force=force)
                error_lines = cm.exception.args[0].split('\n')
                self.assertIn("Problem with builder at index %s" % no_id,
                              error_lines[0])
                self.assertIn("id attribute has not been initialised",
                              error_lines[0])
                self.assertFalse(error_lines[1:])
                self.assertEqual(orig_version, cb.version)

            do_check(False)
            do_check(True)  # we never ignore this error

        # check with compose not previously called, cb has no existing metadata
        builders = self.create_sample_ringbuilders(3)
        cb = CompositeRingBuilder()
        check_missing_id(cb, builders)
        # now save good copies of builders and compose so this cb has
        # existing component metadata
        builder_files = self.save_builders(builders)
        cb = CompositeRingBuilder(builder_files)
        cb.compose()  # cb now has component metadata
        check_missing_id(cb, builders)

    def test_load_components_duplicate_builder_ids(self):
        builders = self.create_sample_ringbuilders(3)
        builders[2]._id = builders[0]._id
        cb = CompositeRingBuilder(self.save_builders(builders))

        def do_check(force):
            with self.assertRaises(ValueError) as cm:
                self._call_method_under_test(cb, force=force)
            error_lines = cm.exception.args[0].split('\n')
            self.assertIn("Builder id %r used at indexes 0, 2" %
                          builders[0].id, error_lines[0])
            self.assertFalse(error_lines[1:])
            self.assertEqual(0, cb.version)

        do_check(False)
        do_check(True)

    def test_load_components_unchanged_builders(self):
        def do_test(cb, builder_files, **kwargs):
            orig_version = cb.version
            with self.assertRaises(ValueError) as cm:
                self._call_method_under_test(cb, builder_files, **kwargs)
            error_lines = cm.exception.args[0].split('\n')
            self.assertIn("None of the component builders has been modified",
                          error_lines[0])
            self.assertFalse(error_lines[1:])
            self.assertEqual(orig_version, cb.version)

        builders = self.create_sample_ringbuilders(2)
        cb, builder_files = self._make_composite_builder(builders)
        # ok to load same *unchanged* builders
        self._call_method_under_test(cb, builder_files)
        # unless require_modified is set
        do_test(cb, builder_files, require_modified=True)
        # even if we rewrite the files
        builder_files = self.save_builders(builders)
        do_test(cb, builder_files, require_modified=True)
        # even if we rename the files
        builder_files = self.save_builders(builders, prefix='other')
        do_test(cb, builder_files, require_modified=True)
        # force trumps require_modified
        self._call_method_under_test(cb, builder_files, force=True,
                                     require_modified=True)

    def test_load_components_older_builder(self):
        # make first version of composite ring
        builders = self.create_sample_ringbuilders(2)
        cb, builder_files = self._make_composite_builder(builders)
        old_builders = [copy.deepcopy(b) for b in builders]
        # update components and reload
        for i, b in enumerate(builders):
            self.add_dev_and_rebalance(b)
            self.assertLess(old_builders[i].version, b.version)
        self.save_builders(builders)
        self._call_method_under_test(cb)
        orig_version = cb.version
        cb.compose()  # compose with newer builder versions
        self.assertEqual(orig_version + 1, cb.version)  # sanity check
        # not ok to use old versions of same builders
        self.save_builders([old_builders[0], builders[1]])
        with self.assertRaises(ValueError) as cm:
            self._call_method_under_test(cb)
        error_lines = cm.exception.args[0].split('\n')
        self.assertIn("Invalid builder change at index 0", error_lines[0])
        self.assertIn("Older builder version", error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(orig_version + 1, cb.version)
        # not even if one component ring has changed
        self.add_dev_and_rebalance(builders[1])
        self.save_builders([old_builders[0], builders[1]])
        with self.assertRaises(ValueError) as cm:
            self._call_method_under_test(cb)
        error_lines = cm.exception.args[0].split('\n')
        self.assertIn("Invalid builder change at index 0", error_lines[0])
        self.assertIn("Older builder version", error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(orig_version + 1, cb.version)
        self.assertIsNone(cb._builders)
        # unless we ignore errors
        self._call_method_under_test(cb, force=True)
        self.assertEqual(old_builders[0].version, cb._builders[0].version)

    def test_load_components_different_number_builders(self):
        # not ok to use a different number of component rings
        builders = self.create_sample_ringbuilders(4)

        def do_test(bad_builders):
            cb, builder_files = self._make_composite_builder(builders[:3])
            # expect an error
            with self.assertRaises(ValueError) as cm:
                self._call_method_under_test(
                    cb, self.save_builders(bad_builders))
            error_lines = cm.exception.args[0].split('\n')
            self.assertFalse(error_lines[1:])
            self.assertEqual(1, cb.version)
            # unless we ignore errors
            self._call_method_under_test(cb, self.save_builders(bad_builders),
                                         force=True)
            self.assertEqual(len(bad_builders), len(cb._builders))
            return error_lines

        error_lines = do_test(builders[:2])  # too few
        self.assertIn("Missing builder at index 2", error_lines[0])
        error_lines = do_test(builders)  # too many
        self.assertIn("Unexpected extra builder at index 3", error_lines[0])

    def test_load_components_different_builders(self):
        # not ok to change component rings
        builders = self.create_sample_ringbuilders(3)
        cb, builder_files = self._make_composite_builder(builders[:2])
        # ensure builder[0] is newer version so that's not the problem
        self.add_dev_and_rebalance(builders[0])
        different_files = self.save_builders([builders[0], builders[2]])
        with self.assertRaises(ValueError) as cm:
            self._call_method_under_test(cb, different_files)
        error_lines = cm.exception.args[0].split('\n')
        self.assertIn("Invalid builder change at index 1", error_lines[0])
        self.assertIn("Attribute mismatch for id", error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(1, cb.version)
        # ok if we force
        self._call_method_under_test(cb, different_files, force=True)
        self.assertEqual(different_files, cb._builder_files)

    def test_load_component_different_builder_order(self):
        # not ok to change order of component rings
        builders = self.create_sample_ringbuilders(4)
        cb, builder_files = self._make_composite_builder(builders)
        builder_files.reverse()
        with self.assertRaises(ValueError) as cm:
            self._call_method_under_test(cb, builder_files)
        error_lines = cm.exception.args[0].split('\n')
        for i, line in enumerate(error_lines):
            self.assertIn("Invalid builder change at index %s" % i, line)
            self.assertIn("Attribute mismatch for id", line)
        self.assertEqual(1, cb.version)
        # ok if we force
        self._call_method_under_test(cb, builder_files, force=True)
        self.assertEqual(builder_files, cb._builder_files)

    def test_load_components_replica_count_changed(self):
        # not ok to change the number of replicas in a ring
        builders = self.create_sample_ringbuilders(3)
        cb, builder_files = self._make_composite_builder(builders)
        builders[0].set_replicas(4)
        self.save_builders(builders)
        with self.assertRaises(ValueError) as cm:
            self._call_method_under_test(cb)
        error_lines = cm.exception.args[0].split('\n')
        for i, line in enumerate(error_lines):
            self.assertIn("Invalid builder change at index 0", line)
            self.assertIn("Attribute mismatch for replicas", line)
        self.assertEqual(1, cb.version)
        # ok if we force
        self._call_method_under_test(cb, force=True)


class TestComposeLoadComponents(TestLoadComponents):
    def _call_method_under_test(self, cb, *args, **kwargs):
        cb.compose(*args, **kwargs)

    def test_load_components_replica_count_changed(self):
        # For compose method this test differs from superclass when the force
        # flag is used, because although the force flag causes load_components
        # to skip checks, the actual ring composition fails.
        # not ok to change the number of replicas in a ring
        builders = self.create_sample_ringbuilders(3)
        cb, builder_files = self._make_composite_builder(builders)
        builders[0].set_replicas(4)
        self.save_builders(builders)
        with self.assertRaises(ValueError) as cm:
            self._call_method_under_test(cb)
        error_lines = cm.exception.args[0].split('\n')
        for i, line in enumerate(error_lines):
            self.assertIn("Invalid builder change at index 0", line)
            self.assertIn("Attribute mismatch for replicas", line)
        self.assertEqual(1, cb.version)
        # if we force, then load_components succeeds but the compose pre
        # validate will fail because the builder needs rebalancing
        with self.assertRaises(ValueError) as cm:
            self._call_method_under_test(cb, force=True)
        error_lines = cm.exception.args[0].split('\n')
        self.assertIn("Problem with builders", error_lines[0])
        self.assertIn("Builder needs rebalance", error_lines[1])
        self.assertFalse(error_lines[2:])
        self.assertEqual(1, cb.version)


class TestCooperativeRingBuilder(BaseTestCompositeBuilder):
    def _make_coop_builder(self, region, composite_builder, rebalance=False,
                           min_part_hours=1):
        rb = CooperativeRingBuilder(8, 3, min_part_hours, composite_builder)
        if composite_builder._builders is None:
            composite_builder._builders = [rb]
        for i in range(3):
            self.add_dev(rb, region=region)
        if rebalance:
            rb.rebalance()
            self.assertEqual(self._partition_counts(rb),
                             [256, 256, 256])  # sanity check
        return rb

    def _partition_counts(self, builder):
        """
        Returns an array mapping device id's to (number of
        partitions assigned to that device).
        """
        c = Counter(builder.devs[dev_id]['id']
                    for part2dev_id in builder._replica2part2dev
                    for dev_id in part2dev_id)
        return [c[d['id']] for d in builder.devs]

    def get_moved_parts(self, after, before):
        def uniqueness(dev):
            return dev['ip'], dev['port'], dev['device']
        moved_parts = set()
        for p in range(before.parts):
            if ({uniqueness(dev) for dev in before._devs_for_part(p)} !=
                    {uniqueness(dev) for dev in after._devs_for_part(p)}):
                moved_parts.add(p)
        return moved_parts

    def num_parts_can_move(self, builder):
        # note that can_part_move() gives consideration to the
        # _part_moved_bitmap which is only reset when a rebalance starts
        return len(
            [p for p in range(builder.parts)
             if super(CooperativeRingBuilder, builder)._can_part_move(p)])

    @mock.patch('swift.common.ring.builder.time')
    def _check_rebalance_respects_cobuilder_part_moves(
            self, min_part_hours, mock_time):
        mock_time.return_value = now = int(time.time())
        builder_files = []
        cb = CompositeRingBuilder()
        for i in (1, 2, 3):
            b = self._make_coop_builder(i, cb, min_part_hours=min_part_hours)
            fname = os.path.join(self.tmpdir, 'builder_%s.builder' % i)
            b.save(fname)
            builder_files.append(fname)
        builder_files, builders = cb.load_components(builder_files)

        # all cobuilders can perform initial rebalance
        cb.rebalance()
        exp = [256, 256, 256]
        self.assertEqual(exp, self._partition_counts(builders[0]))
        self.assertEqual(exp, self._partition_counts(builders[1]))
        self.assertEqual(exp, self._partition_counts(builders[2]))
        exp = min_part_hours * 3600
        self.assertEqual(exp, builders[0].min_part_seconds_left)
        self.assertEqual(exp, builders[1].min_part_seconds_left)
        self.assertEqual(exp, builders[2].min_part_seconds_left)

        # jump forwards min_part_hours
        now += min_part_hours * 3600
        mock_time.return_value = now
        old_builders = []
        for builder in builders:
            old_builder = CooperativeRingBuilder(8, 3, min_part_hours, None)
            old_builder.copy_from(copy.deepcopy(builder.to_dict()))
            old_builders.append(old_builder)

        for builder in builders:
            self.add_dev(builder)
        # sanity checks: all builders are ready for rebalance
        self.assertEqual(0, builders[0].min_part_seconds_left)
        self.assertEqual(0, builders[1].min_part_seconds_left)
        self.assertEqual(0, builders[2].min_part_seconds_left)
        # ... but last_part_moves not yet updated to current epoch
        if min_part_hours > 0:
            self.assertEqual(0, self.num_parts_can_move(builders[0]))
            self.assertEqual(0, self.num_parts_can_move(builders[1]))
            self.assertEqual(0, self.num_parts_can_move(builders[2]))

        with mock.patch('swift.common.ring.composite_builder.shuffle',
                        lambda x: x):
            cb.rebalance()

        rb1_parts_moved = self.get_moved_parts(builders[0], old_builders[0])
        self.assertEqual(192, len(rb1_parts_moved))
        self.assertEqual(self._partition_counts(builders[0]),
                         [192, 192, 192, 192])

        rb2_parts_moved = self.get_moved_parts(builders[1], old_builders[1])
        self.assertEqual(64, len(rb2_parts_moved))
        counts = self._partition_counts(builders[1])
        self.assertEqual(counts[3], 64)
        self.assertEqual([234, 235, 235], sorted(counts[:3]))
        self.assertFalse(rb2_parts_moved.intersection(rb1_parts_moved))

        # rb3 can't rebalance - all parts moved while rebalancing rb1 and rb2
        self.assertEqual(
            0, len(self.get_moved_parts(builders[2], old_builders[2])))

        # jump forwards min_part_hours, all builders can move all parts again,
        # so now rb2 should be able to further rebalance
        now += min_part_hours * 3600
        mock_time.return_value = now
        old_builders = []
        for builder in builders:
            old_builder = CooperativeRingBuilder(8, 3, min_part_hours, None)
            old_builder.copy_from(copy.deepcopy(builder.to_dict()))
            old_builders.append(old_builder)
        with mock.patch('swift.common.ring.composite_builder.shuffle',
                        lambda x: x):
            cb.rebalance()

        rb2_parts_moved = self.get_moved_parts(builders[1], old_builders[1])
        self.assertGreater(len(rb2_parts_moved), 64)
        self.assertGreater(self._partition_counts(builders[1])[3], 64)
        self.assertLess(self.num_parts_can_move(builders[2]), 256)
        self.assertEqual(256, self.num_parts_can_move(builders[0]))
        # and rb3 should also have been able to move some parts
        rb3_parts_moved = self.get_moved_parts(builders[2], old_builders[2])
        self.assertGreater(len(rb3_parts_moved), 0)
        self.assertFalse(rb3_parts_moved.intersection(rb2_parts_moved))

        # but cobuilders will not prevent a new rb rebalancing for first time
        rb4 = self._make_coop_builder(4, cb, rebalance=False,
                                      min_part_hours=min_part_hours)
        builders.append(rb4)
        builder_files = []
        for i, builder in enumerate(builders):
            fname = os.path.join(self.tmpdir, 'builder_%s.builder' % i)
            builder.save(fname)
            builder_files.append(fname)
        cb = CompositeRingBuilder()
        builder_files, builders = cb.load_components(builder_files)
        cb.rebalance()
        self.assertEqual(256, len(self.get_moved_parts(builders[3], rb4)))

    def test_rebalance_respects_cobuilder_part_moves(self):
        self._check_rebalance_respects_cobuilder_part_moves(1)
        self._check_rebalance_respects_cobuilder_part_moves(0)

    @mock.patch('swift.common.ring.builder.time')
    def _check_rebalance_cobuilder_states(
            self, min_part_hours, mock_time):

        @contextmanager
        def mock_rebalance():
            # wrap rebalance() in order to capture builder states before and
            # after each component rebalance
            orig_rebalance = RingBuilder.rebalance
            # a dict mapping builder -> (list of captured builder states)
            captured_builder_states = defaultdict(list)

            def update_states():
                for b in cb._builders:
                    rb = CooperativeRingBuilder(8, 3, min_part_hours, None)
                    rb.copy_from(copy.deepcopy(b.to_dict()))
                    rb._part_moved_bitmap = bytearray(b._part_moved_bitmap)
                    captured_builder_states[b].append(rb)

            def wrap_rebalance(builder_instance):
                update_states()
                results = orig_rebalance(builder_instance)
                update_states()
                return results

            with mock.patch('swift.common.ring.RingBuilder.rebalance',
                            wrap_rebalance):
                yield captured_builder_states

        mock_time.return_value = now = int(time.time())
        builder_files = []
        cb = CompositeRingBuilder()
        for i in (1, 2, 3):
            b = self._make_coop_builder(i, cb, min_part_hours=min_part_hours)
            fname = os.path.join(self.tmpdir, 'builder_%s.builder' % i)
            b.save(fname)
            builder_files.append(fname)
        builder_files, builders = cb.load_components(builder_files)

        # all cobuilders can perform initial rebalance
        cb.rebalance()
        # jump forwards min_part_hours
        now += min_part_hours * 3600
        mock_time.return_value = now
        for builder in builders:
            self.add_dev(builder)

        with mock.patch('swift.common.ring.composite_builder.shuffle',
                        lambda x: x):
            with mock_rebalance() as captured_states:
                cb.rebalance()

        # sanity - state captured before and after each component rebalance
        self.assertEqual(len(builders), len(captured_states))
        for states in captured_states.values():
            self.assertEqual(2 * len(builders), len(states))
        # for each component we have a list of it's builder states
        rb1s = captured_states[builders[0]]
        rb2s = captured_states[builders[1]]
        rb3s = captured_states[builders[2]]

        # rebalancing will update epoch for all builders' last_part_moves
        self.assertEqual(now, rb1s[0]._last_part_moves_epoch)
        self.assertEqual(now, rb2s[0]._last_part_moves_epoch)
        self.assertEqual(now, rb3s[0]._last_part_moves_epoch)
        # so, in state before any component rebalance, all can now move parts
        # N.B. num_parts_can_move gathers super class's (i.e. RingBuilder)
        # _can_part_move so that it doesn't refer to cobuilders state.
        self.assertEqual(256, self.num_parts_can_move(rb1s[0]))
        self.assertEqual(256, self.num_parts_can_move(rb2s[0]))
        self.assertEqual(256, self.num_parts_can_move(rb3s[0]))

        # after first component has been rebalanced it has moved parts
        self.assertEqual(64, self.num_parts_can_move(rb1s[1]))
        self.assertEqual(256, self.num_parts_can_move(rb2s[2]))
        self.assertEqual(256, self.num_parts_can_move(rb3s[2]))

        rb1_parts_moved = self.get_moved_parts(rb1s[1], rb1s[0])
        self.assertEqual(192, len(rb1_parts_moved))
        self.assertEqual(self._partition_counts(rb1s[1]),
                         [192, 192, 192, 192])

        # rebalancing rb2 - rb2 in isolation could potentially move all parts
        # so would move 192 parts to new device, but it is constrained by rb1
        # only having 64 parts that can move
        rb2_parts_moved = self.get_moved_parts(rb2s[3], rb2s[2])
        self.assertEqual(64, len(rb2_parts_moved))
        counts = self._partition_counts(rb2s[3])
        self.assertEqual(counts[3], 64)
        self.assertEqual([234, 235, 235], sorted(counts[:3]))
        self.assertFalse(rb2_parts_moved.intersection(rb1_parts_moved))
        self.assertEqual(192, self.num_parts_can_move(rb2s[3]))
        self.assertEqual(64, self.num_parts_can_move(rb1s[3]))

        # rb3 can't rebalance - all parts moved while rebalancing rb1 and rb2
        self.assertEqual(0, len(self.get_moved_parts(rb3s[5], rb3s[0])))

    def test_rebalance_cobuilder_states(self):
        self._check_rebalance_cobuilder_states(1)
        self._check_rebalance_cobuilder_states(0)

    def _check_rebalance_cobuilders_calls(self, min_part_hours):
        # verify that co-builder methods are called during one builder's
        # rebalance
        @contextmanager
        def mock_update_last_part_moves():
            # intercept calls to RingBuilder._update_last_part_moves (yes, the
            # superclass method) and populate a dict mapping builder instance
            # to a list of that builder's parent builder when method was called
            calls = []
            orig_func = RingBuilder._update_last_part_moves

            def fake_update(builder):
                calls.append(builder)
                return orig_func(builder)

            with mock.patch(
                    'swift.common.ring.RingBuilder._update_last_part_moves',
                    fake_update):
                yield calls

        @contextmanager
        def mock_can_part_move():
            # intercept calls to RingBuilder._can_part_move (yes, the
            # superclass method) and populate a dict mapping builder instance
            # to a list of that builder's parent builder when method was called
            calls = defaultdict(list)
            orig_func = RingBuilder._can_part_move

            def fake_can_part_move(builder, part):
                calls[builder].append(part)
                return orig_func(builder, part)
            with mock.patch('swift.common.ring.RingBuilder._can_part_move',
                            fake_can_part_move):
                yield calls

        cb = CompositeRingBuilder()
        rb1 = self._make_coop_builder(1, cb, min_part_hours=min_part_hours)
        rb2 = self._make_coop_builder(2, cb, min_part_hours=min_part_hours)
        cb._builders = [rb1, rb2]
        # composite rebalance updates last_part_moves before any component
        # rebalance - after that expect no more updates
        with mock_update_last_part_moves() as update_calls:
            cb.update_last_part_moves()
        self.assertEqual({rb1, rb2}, set(update_calls))

        with mock_update_last_part_moves() as update_calls:
            with mock_can_part_move() as can_part_move_calls:
                rb2.rebalance()
        self.assertFalse(update_calls)
        # rb1 has never been rebalanced so no calls propagate from its
        # can_part_move method to its superclass _can_part_move method
        self.assertEqual({rb2}, set(can_part_move_calls))

        with mock_update_last_part_moves() as update_calls:
            with mock_can_part_move() as can_part_move_calls:
                rb1.rebalance()
        self.assertFalse(update_calls)
        # rb1 is being rebalanced so gets checked, and rb2 also gets checked
        self.assertEqual({rb1, rb2}, set(can_part_move_calls))
        self.assertEqual(768, len(can_part_move_calls[rb1]))
        self.assertEqual(768, len(can_part_move_calls[rb2]))

    def test_rebalance_cobuilders_calls(self):
        self._check_rebalance_cobuilders_calls(1)
        self._check_rebalance_cobuilders_calls(0)

    def test_save_then_load(self):
        cb = CompositeRingBuilder()
        coop_rb = self._make_coop_builder(1, cb, rebalance=True)
        builder_file = os.path.join(self.tmpdir, 'test.builder')
        coop_rb.save(builder_file)
        cb = CompositeRingBuilder()
        loaded_coop_rb = CooperativeRingBuilder.load(builder_file,
                                                     parent_builder=cb)
        self.assertIs(cb, loaded_coop_rb.parent_builder)
        self.assertEqual(coop_rb.to_dict(), loaded_coop_rb.to_dict())

        # check can be loaded as superclass
        loaded_rb = RingBuilder.load(builder_file)
        self.assertEqual(coop_rb.to_dict(), loaded_rb.to_dict())

        # check can load a saved superclass
        rb = RingBuilder(6, 3, 0)
        for _ in range(3):
            self.add_dev(rb, region=1)
        rb.save(builder_file)
        cb = CompositeRingBuilder()
        loaded_coop_rb = CooperativeRingBuilder.load(builder_file,
                                                     parent_builder=cb)
        self.assertIs(cb, loaded_coop_rb.parent_builder)
        self.assertEqual(rb.to_dict(), loaded_coop_rb.to_dict())


if __name__ == '__main__':
    unittest.main()
