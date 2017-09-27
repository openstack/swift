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
import os
import shutil
import time
from contextlib import contextmanager
from tempfile import mkdtemp

import mock

from swift.container.backend import ContainerBroker, DB_STATE_UNSHARDED, \
    RECORD_TYPE_SHARD_NODE, DB_STATE_SHARDING, DB_STATE_SHARDED
from swift.container.sharder import ContainerSharder, RangeAnalyser, \
    update_sharding_info
from swift.common.utils import ShardRange, Timestamp, hash_path

import unittest

from test.unit import FakeLogger, debug_logger, FakeRing, make_timestamp_iter


class TestRangeAnalyser(unittest.TestCase):
    def setUp(self):
        self.ranges = self._default_ranges()

    def _default_ranges(self):
        ts = Timestamp(time.time()).internal

        ranges = [
            ShardRange('-d', ts, '', 'd'),
            ShardRange('d-g', ts, 'd', 'g'),
            ShardRange('g-j', ts, 'g', 'j'),
            ShardRange('j-l', ts, 'j', 'l'),
            ShardRange('l-n', ts, 'l', 'n'),
            ShardRange('n-p', ts, 'n', 'p'),
            ShardRange('p-s', ts, 'p', 's'),
            ShardRange('s-v', ts, 's', 'v'),
            ShardRange('v-', ts, 'v', '')]

        return ranges

    def test_simple_shard(self):
        ts = Timestamp(time.time()).internal

        # This simulate a shard sharding by having an older 'n-p' and
        # newer split 'n-o' and 'o-p'
        overlap_without_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('o-p', ts, 'o', 'p')]

        # expected 'n-p'
        expected_other_ranges = {self.ranges[5]}
        expected_best_path = set(self.ranges[:5] + self.ranges[6:]
                                 + overlap_without_gaps)

        self.ranges.extend(overlap_without_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        path, other, complete = next(paths)

        # it's a complete path (no gaps)
        self.assertTrue(complete)
        self.assertSetEqual(expected_other_ranges, other)
        self.assertSetEqual(expected_best_path, set(path))

    def test_2_paths_diverge_and_then_join(self):
        ts = Timestamp(time.time()).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_without_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('o-q', ts, 'o', 'q'),
            ShardRange('q-s', ts, 'q', 's')]

        # expected n-p, p-s
        expected_other_ranges = {p for p in self.ranges[5:7]}
        expected_best_path = set(self.ranges[:5] + self.ranges[7:]
                                 + overlap_without_gaps)

        self.ranges.extend(overlap_without_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        path, other, complete = next(paths)

        # it's a complete path (no gaps)
        self.assertTrue(complete)
        self.assertSetEqual(expected_other_ranges, other)
        self.assertSetEqual(expected_best_path, set(path))

    def test_2_paths_diverge_older_ends_in_gap(self):
        ts = Timestamp(time.time()).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_without_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('o-q', ts, 'o', 'q'),
            ShardRange('q-s', ts, 'q', 's')]

        # expected n-p
        expected_other_ranges = {self.ranges[5]}
        expected_best_path = set(self.ranges[:5] + self.ranges[7:]
                                 + overlap_without_gaps)
        # drop p-s
        self.ranges.pop(6)

        self.ranges.extend(overlap_without_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        path, other, complete = next(paths)

        # it's a complete path (no gaps)
        self.assertTrue(complete)
        self.assertSetEqual(expected_other_ranges, other)
        self.assertSetEqual(expected_best_path, set(path))

    def test_2_paths_diverge_newer_ends_in_gap(self):
        ts = Timestamp(time.time()).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_with_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('o-q', ts, 'o', 'q')]

        # The newest will be incomplete, the second (older) path will
        # be complete.
        expected_completes = [False, True]
        # expected n-p
        expected_other_ranges = [{p for p in self.ranges[5:]},
                                 set(overlap_with_gaps)]
        expected_best_path = [set(self.ranges[:5] + overlap_with_gaps),
                              set(self.ranges)]

        self.ranges.extend(overlap_with_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        for i, (path, other, complete) in enumerate(paths):

            self.assertEqual(expected_completes[i], complete)
            self.assertSetEqual(expected_other_ranges[i], other)
            self.assertSetEqual(expected_best_path[i], set(path))

    def test_2_paths_diverge_different_ends(self):
        ts = Timestamp(time.time()).internal

        # To the end with different paths
        overlap_without_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('o-q', ts, 'o', 'q'),
            ShardRange('q-t', ts, 'q', 't'),
            ShardRange('t-w', ts, 't', 'w'),
            ShardRange('w-', ts, 'w', '')]

        # expected n-p
        expected_other_ranges = [{p for p in self.ranges[5:]},
                                 set(overlap_without_gaps)]
        expected_best_path = [set(self.ranges[:5] + overlap_without_gaps),
                              set(self.ranges)]

        self.ranges.extend(overlap_without_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        for i, (path, other, complete) in enumerate(paths):

            self.assertTrue(complete)
            self.assertSetEqual(expected_other_ranges[i], other)
            self.assertSetEqual(expected_best_path[i], set(path))

    def test_2_paths_diverge_different_ends_gap_in_newer(self):
        ts = Timestamp(time.time()).internal

        # To the end with different paths
        overlap_without_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('o-q', ts, 'o', 'q'),
            ShardRange('t-w', ts, 't', 'w'),
            ShardRange('w-', ts, 'w', '')]

        expected_completes = [False, True]
        expected_other_ranges = [{p for p in self.ranges[5:]},
                                 set(overlap_without_gaps)]
        expected_best_path = [set(self.ranges[:5] + overlap_without_gaps),
                              set(self.ranges)]

        self.ranges.extend(overlap_without_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        for i, (path, other, complete) in enumerate(paths):

            self.assertEqual(expected_completes[i], complete)
            self.assertSetEqual(expected_other_ranges[i], other)
            self.assertSetEqual(expected_best_path[i], set(path))

    def test_tiebreak_newest_difference_wins(self):
        ts = Timestamp(time.time()).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRange before the other scanner has a go and so takes off where
        # it left off).
        overlap_without_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('o-q', ts, 'o', 'q'),
            ShardRange('q-s', ts, 'q', 's')]

        expected_other_ranges = [{p for p in self.ranges[5:7]},
                                 set(overlap_without_gaps)]
        expected_best_path = [set(self.ranges[:5] + self.ranges[7:]
                                  + overlap_without_gaps),
                              set(self.ranges)]

        # make a shard range in both paths newer then any of the difference to
        # force a tie break situation
        self.ranges[2].timestamp = Timestamp(time.time()).internal
        self.ranges.extend(overlap_without_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        for i, (path, other, complete) in enumerate(paths):

            self.assertTrue(complete)
            self.assertSetEqual(expected_other_ranges[i], other)
            self.assertSetEqual(expected_best_path[i], set(path))

    def test_tiebreak_newest_difference_wins_1_with_gap(self):
        ts = Timestamp(time.time()).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_with_gaps = [
            ShardRange('n-o', ts, 'n', 'o'),
            ShardRange('q-s', ts, 'q', 's')]

        expected_completes = [False, True]
        expected_other_ranges = [{p for p in self.ranges[5:7]},
                                 set(overlap_with_gaps)]
        expected_best_path = [set(self.ranges[:5] + self.ranges[7:]
                                  + overlap_with_gaps),
                              set(self.ranges)]

        # make a shard range in both paths newer then any of the difference to
        # force a tie break situation
        self.ranges[2].timestamp = Timestamp(time.time()).internal
        self.ranges.extend(overlap_with_gaps)
        self.ranges.sort()

        # find overlaps, choose newest and check for gaps. We should get
        # the newest path (with the split) as our first.
        ra = RangeAnalyser()
        paths = ra.analyse(self.ranges)
        for i, (path, other, complete) in enumerate(paths):

            self.assertEqual(expected_completes[i], complete)
            self.assertSetEqual(expected_other_ranges[i], other)
            self.assertSetEqual(expected_best_path[i], set(path))


class TestSharder(unittest.TestCase):
    def setUp(self):
        self.tempdir = mkdtemp()
        self.ts_iter = make_timestamp_iter()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def test_init(self):
        def do_test(conf, expected):
            with mock.patch(
                    'swift.container.sharder.internal_client.InternalClient') \
                    as mock_ic:
                with mock.patch('swift.common.db_replicator.ring.Ring')\
                        as mock_ring:
                    sharder = ContainerSharder(conf)
            mock_ring.assert_called_once_with(
                '/etc/swift', ring_name='container')
            self.assertEqual(
                'container-sharder', sharder.logger.logger.name)
            for k, v in expected.items():
                self.assertTrue(hasattr(sharder, k), 'Missing attr %s' % k)
                self.assertEqual(v, getattr(sharder, k),
                                 'Incorrect value: expected %s=%s but got %s' %
                                 (k, v, getattr(sharder, k)))
            return mock_ic

        expected = {
            'mount_check': True, 'bind_ip': '0.0.0.0', 'port': 6201,
            'per_diff': 1000, 'max_diffs': 100, 'interval': 30,
            'node_timeout': 10, 'conn_timeout': 5,
            'rsync_compress': False,
            'rsync_module': '{replication_ip}::container',
            'reclaim_age': 86400 * 7,
            'shard_shrink_point': 0.25,
            'shrink_merge_point': 0.75,
            'shard_container_size': 10000000,
            'split_size': 5000000,
            'shard_batch_size': 2,
            'scanner_batch_size': 10,
            'rcache': '/var/cache/swift/container-sharder.recon'}
        mock_ic = do_test({}, expected)
        mock_ic.assert_called_once_with(
            '/etc/swift/internal-client.conf', 'Swift Container Sharder', 3,
            allow_modify_pipeline=False)

        conf = {
            'mount_check': False, 'bind_ip': '10.11.12.13', 'bind_port': 62010,
            'per_diff': 2000, 'max_diffs': 200, 'interval': 60,
            'node_timeout': 20, 'conn_timeout': 1,
            'rsync_compress': True,
            'rsync_module': '{replication_ip}::container_sda/',
            'reclaim_age': 86400 * 14,
            'shard_shrink_point': 35,
            'shard_shrink_merge_point': 85,
            'shard_container_size': 20000000,
            'shard_batch_size': 4,
            'shard_scanner_batch_size': 8,
            'request_tries': 2,
            'internal_client_conf_path': '/etc/swift/my-sharder-ic.conf',
            'rcache': '/var/cache/swift/container-sharder.recon'}
        expected = {
            'mount_check': False, 'bind_ip': '10.11.12.13', 'port': 62010,
            'per_diff': 2000, 'max_diffs': 200, 'interval': 60,
            'node_timeout': 20, 'conn_timeout': 1,
            'rsync_compress': True,
            'rsync_module': '{replication_ip}::container_sda',
            'reclaim_age': 86400 * 14,
            'shard_shrink_point': 0.35,
            'shrink_merge_point': 0.85,
            'shard_container_size': 20000000,
            'split_size': 10000000,
            'shard_batch_size': 4,
            'scanner_batch_size': 8,
            'rcache': '/var/cache/swift/container-sharder.recon'}
        mock_ic = do_test(conf, expected)
        mock_ic.assert_called_once_with(
            '/etc/swift/my-sharder-ic.conf', 'Swift Container Sharder', 2,
            allow_modify_pipeline=False)

        with self.assertRaises(ValueError) as cm:
            do_test({'shard_shrink_point': 101}, {})
        self.assertIn(
            'greater than 0, less than 100, not "101"', cm.exception.message)
        self.assertIn('shard_shrink_point', cm.exception.message)

        with self.assertRaises(ValueError) as cm:
            do_test({'shard_shrink_merge_point': 101}, {})
        self.assertIn(
            'greater than 0, less than 100, not "101"', cm.exception.message)
        self.assertIn('shard_shrink_merge_point', cm.exception.message)

    @contextmanager
    def setup_mocks(self):
        mod = 'swift.container.sharder'
        with mock.patch(mod + '.time.time') as mock_time:
            with mock.patch(mod + '.time.sleep') as mock_sleep:
                with mock.patch(mod + '.internal_client.InternalClient'):
                    with mock.patch('swift.common.db_replicator.ring.Ring'):
                        conf = {'recon_cache_path': self.tempdir}
                        sharder = ContainerSharder(conf, logger=FakeLogger())
                        with mock.patch.object(sharder, '_one_shard_cycle') \
                                as mock_pass:
                            sharder.logger.clear()
                            mocks = {
                                'time': mock_time,
                                'sleep': mock_sleep,
                                'pass': mock_pass}
                            yield sharder, mocks

    def test_run_forever(self):
        with self.setup_mocks() as (sharder, mocks):
            mocks['time'].side_effect = [
                1,  # zero stats
                2,  # set initial reported
                3,  # set begin
                20,  # calculate elapsed
                21,  # zero stats
                22,  # report_stats -> reset reported
                32,  # set begin
                3602,  # calculate elapsed
                3603,  # zero stats
                3604,  # report_stats -> reset reported
                3605,  # set begin
                4000,  # calculate elapsed
                4001,  # zero stats
                4002,  # report_stats -> reset reported
                Exception("Test over")  # set begin - exit test run
            ]

            fake_stats = [{'containers_failed': 1},
                          {'containers_sharded': 2},
                          {'container_shard_ranges': 23}]

            def set_stats(*args, **kwargs):
                sharder.stats.update(fake_stats.pop(0))
            mocks['pass'].side_effect = set_stats

            with self.assertRaises(Exception) as cm:
                sharder.run_forever()

            self.assertEqual('Test over', cm.exception.message)
            # expect initial random sleep then one sleep between first and
            # second pass
            self.assertEqual(2, mocks['sleep'].call_count)
            self.assertLessEqual(mocks['sleep'].call_args_list[0][0][0], 30)
            self.assertEqual(13, mocks['sleep'].call_args_list[1][0][0])

            lines = sharder.logger.get_lines_for_level('info')
            self.assertIn('Container sharder cycle completed: 17.00s',
                          lines[0])
            self.assertIn('1 containers failed', lines[1])
            self.assertIn('Container sharder cycle completed: 3570.00s',
                          lines[2])
            self.assertIn('2 sharded', lines[3])
            # checks stats were reset
            self.assertNotIn('1 containers failed', lines[3])
            self.assertIn('Container sharder cycle completed: 395.00s',
                          lines[4])
            self.assertIn('23 shard ranges found', lines[5])
            # checks stats were reset
            self.assertNotIn('2 sharded', lines[5])
            # TODO check recon cache

    @contextmanager
    def _mock_sharder(self):

        with mock.patch(
                'swift.container.sharder.internal_client.InternalClient'):
            with mock.patch(
                    'swift.common.db_replicator.ring.Ring',
                    lambda *args, **kwargs: FakeRing()):
                sharder = ContainerSharder(
                    {'devices': self.tempdir},
                    logger=debug_logger())
                sharder._local_device_ids = {0, 1, 2}
                sharder._replicate_object = mock.MagicMock()
                yield sharder

    def test_cleave(self):
        logger = debug_logger()
        db_file = os.path.join(self.tempdir, 'test_db')
        broker = ContainerBroker(
            db_file, account='a', container='c', logger=logger)
        broker.initialize()

        def ts_internal():
            return next(self.ts_iter).internal

        # TODO: test with some deleted, different ctype and meta timestamps
        objects = [
            ('a', ts_internal(), 10, 'text/plain', 'etag_a', 0),
            ('here', ts_internal(), 10, 'text/plain', 'etag_here', 0),
            ('m', ts_internal(), 1, 'text/plain', 'etag_m', 0),
            ('n', ts_internal(), 2, 'text/plain', 'etag_n', 0),
            ('there', ts_internal(), 3, 'text/plain', 'etag_there', 0),
            ('x', ts_internal(), 10, 'text/plain', 'etag_x', 1),  # deleted
            ('where', ts_internal(), 100, 'text/plain', 'etag_where', 0),
            ('z', ts_internal(), 1000, 'text/plain', 'etag_z', 0)
        ]
        for obj in objects:
            broker.put_object(*obj)

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', ''))
        initial_shard_ranges = [
            ShardRange('%s-%s' % (lower, upper), Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for shard_range in initial_shard_ranges:
            db_hash = hash_path('.sharded_a', shard_range.name)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        # run cleave - no shard ranges, nothing happens
        node = {'id': 2, 'index': 1}
        with self._mock_sharder() as sharder:
            sharder._cleave(broker, node, 'a', 'c')
        self.assertEqual(DB_STATE_UNSHARDED, broker.get_db_state())
        sharder._replicate_object.assert_not_called()

        broker.merge_items(
            [dict(shard_range, deleted=0, storage_policy_index=0,
                  record_type=RECORD_TYPE_SHARD_NODE)
             for shard_range in initial_shard_ranges[:3]])

        # run cleave again now we have shard ranges
        with self._mock_sharder() as sharder:
            sharder._cleave(broker, node, 'a', 'c')

        self.assertEqual(DB_STATE_SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[:2]]
        )

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(3, len(updated_shard_ranges))

        # third shard range should be unchanged - not yet cleaved
        self.assertEqual(dict(initial_shard_ranges[2]),
                         dict(updated_shard_ranges[2]))

        # first 2 shard ranges should have updated object count, bytes used and
        # meta_timestamp
        initial_shard_ranges[0].bytes_used = 20
        initial_shard_ranges[0].object_count = 2
        initial_shard_ranges[1].bytes_used = 6
        initial_shard_ranges[1].object_count = 3

        def check_shard_range(expected, actual):
            expected_dict = dict(expected)
            actual_dict = dict(actual)
            self.assertGreater(actual_dict.pop('meta_timestamp'),
                               expected_dict.pop('meta_timestamp'))
            self.assertEqual(expected_dict, actual_dict)

        check_shard_range(initial_shard_ranges[0], updated_shard_ranges[0])
        check_shard_range(initial_shard_ranges[1], updated_shard_ranges[1])

        def check_shard_objects(expected_objs, shard_db):
            shard_broker = ContainerBroker(shard_db)
            shard_objs = shard_broker.list_objects_iter(10, '', '', '', '')
            self.assertEqual(expected_objs, shard_objs)

        check_shard_objects(objects[:2], expected_shard_dbs[0])
        check_shard_objects(objects[2:5], expected_shard_dbs[1])

        metadata = broker.metadata
        self.assertIn('X-Container-Sysmeta-Shard-Last-1', metadata)
        self.assertEqual(['there', mock.ANY],
                         metadata['X-Container-Sysmeta-Shard-Last-1'])

        # run cleave - should process the third range
        with self._mock_sharder() as sharder:
            sharder._cleave(broker, node, 'a', 'c')

        self.assertEqual(DB_STATE_SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[2], 0)
        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(3, len(updated_shard_ranges))
        check_shard_range(initial_shard_ranges[0], updated_shard_ranges[0])
        check_shard_range(initial_shard_ranges[1], updated_shard_ranges[1])
        # third shard range should now have update object count, bytes used
        initial_shard_ranges[2].bytes_used = 100
        initial_shard_ranges[2].object_count = 1
        check_shard_range(initial_shard_ranges[2], updated_shard_ranges[2])
        # TODO: should we expect the deleted row to be cleaved?
        check_shard_objects(objects[6:7], expected_shard_dbs[2])
        # check_shard_objects(objects[5:], expected_shard_dbs[2])

        metadata = broker.metadata
        self.assertIn('X-Container-Sysmeta-Shard-Last-1', metadata)
        self.assertEqual(['where', mock.ANY],
                         metadata['X-Container-Sysmeta-Shard-Last-1'])

        # run cleave - should be a no-op, all existing ranges have been cleaved
        with self._mock_sharder() as sharder:
            sharder._cleave(broker, node, 'a', 'c')

        self.assertEqual(DB_STATE_SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_not_called()

        # add final shard range
        broker.merge_items(
            [dict(initial_shard_ranges[3], deleted=0, storage_policy_index=0,
                  record_type=RECORD_TYPE_SHARD_NODE)])
        update_sharding_info(broker, {'Scan-Done': True})

        with self._mock_sharder() as sharder:
            sharder._cleave(broker, node, 'a', 'c')

        self.assertEqual(DB_STATE_SHARDED, broker.get_db_state())
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[3], 0)
        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))
        check_shard_range(initial_shard_ranges[0], updated_shard_ranges[0])
        check_shard_range(initial_shard_ranges[1], updated_shard_ranges[1])
        check_shard_range(initial_shard_ranges[2], updated_shard_ranges[2])
        # fourth shard range should now have update object count, bytes used
        initial_shard_ranges[3].bytes_used = 1000
        initial_shard_ranges[3].object_count = 1
        check_shard_objects(objects[7:], expected_shard_dbs[3])

        # run cleave - should be a no-op
        with self._mock_sharder() as sharder:
            sharder._cleave(broker, node, 'a', 'c')

        self.assertEqual(DB_STATE_SHARDED, broker.get_db_state())
        sharder._replicate_object.assert_not_called()
