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

import os
import shutil
from contextlib import contextmanager
from tempfile import mkdtemp

import mock
import unittest


from swift.container.backend import ContainerBroker, UNSHARDED, SHARDING
from swift.container.sharder import ContainerSharder, RangeAnalyser, \
    sharding_enabled
from swift.common.utils import ShardRange, Timestamp, hash_path, \
    encode_timestamps
from test import annotate_failure

from test.unit import FakeLogger, debug_logger, FakeRing, make_timestamp_iter, \
    unlink_files


class TestRangeAnalyser(unittest.TestCase):
    def setUp(self):
        self.ts_iter = make_timestamp_iter()
        self.ranges = self._default_ranges()

    def _default_ranges(self):
        ts = next(self.ts_iter).internal

        ranges = [
            ShardRange('.sharded_a/-d', ts, '', 'd'),
            ShardRange('.sharded_a/d-g', ts, 'd', 'g'),
            ShardRange('.sharded_a/g-j', ts, 'g', 'j'),
            ShardRange('.sharded_a/j-l', ts, 'j', 'l'),
            ShardRange('.sharded_a/l-n', ts, 'l', 'n'),
            ShardRange('.sharded_a/n-p', ts, 'n', 'p'),
            ShardRange('.sharded_a/p-s', ts, 'p', 's'),
            ShardRange('.sharded_a/s-v', ts, 's', 'v'),
            ShardRange('.sharded_a/v-', ts, 'v', '')]

        return ranges

    def test_simple_shard(self):
        ts = next(self.ts_iter).internal

        # This simulate a shard sharding by having an older 'n-p' and
        # newer split 'n-o' and 'o-p'
        overlap_without_gaps = [
            ShardRange('.sharded_a/n-o', ts, 'n', 'o'),
            ShardRange('.sharded_a/o-p', ts, 'o', 'p')]

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
        ts = next(self.ts_iter).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_without_gaps = [
            ShardRange('a/n-o', ts, 'n', 'o'),
            ShardRange('a/o-q', ts, 'o', 'q'),
            ShardRange('a/q-s', ts, 'q', 's')]

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
        ts = next(self.ts_iter).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_without_gaps = [
            ShardRange('a/n-o', ts, 'n', 'o'),
            ShardRange('a/o-q', ts, 'o', 'q'),
            ShardRange('a/q-s', ts, 'q', 's')]

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
        ts = next(self.ts_iter).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_with_gaps = [
            ShardRange('a/n-o', ts, 'n', 'o'),
            ShardRange('a/o-q', ts, 'o', 'q')]

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
        ts = next(self.ts_iter).internal

        # To the end with different paths
        overlap_without_gaps = [
            ShardRange('a/n-o', ts, 'n', 'o'),
            ShardRange('a/o-q', ts, 'o', 'q'),
            ShardRange('a/q-t', ts, 'q', 't'),
            ShardRange('a/t-w', ts, 't', 'w'),
            ShardRange('a/w-', ts, 'w', '')]

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
        ts = next(self.ts_iter).internal

        # To the end with different paths
        overlap_without_gaps = [
            ShardRange('a/n-o', ts, 'n', 'o'),
            ShardRange('a/o-q', ts, 'o', 'q'),
            ShardRange('a/t-w', ts, 't', 'w'),
            ShardRange('a/w-', ts, 'w', '')]

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
        ts = next(self.ts_iter).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRange before the other scanner has a go and so takes off where
        # it left off).
        overlap_without_gaps = [
            ShardRange('a/n-o', ts, 'n', 'o'),
            ShardRange('a/o-q', ts, 'o', 'q'),
            ShardRange('a/q-s', ts, 'q', 's')]

        expected_other_ranges = [{p for p in self.ranges[5:7]},
                                 set(overlap_without_gaps)]
        expected_best_path = [set(self.ranges[:5] + self.ranges[7:]
                                  + overlap_without_gaps),
                              set(self.ranges)]

        # make a shard range in both paths newer then any of the difference to
        # force a tie break situation
        self.ranges[2].timestamp = next(self.ts_iter).internal
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
        ts = next(self.ts_iter).internal

        # second scanner that joins back up ( ie runs and dumps into
        # ShardRanges before the other scanner has a go and so takes off where
        # it left off).
        overlap_with_gaps = [
            ShardRange('a/n-o', ts, 'n', 'o'),
            ShardRange('a/q-s', ts, 'q', 's')]

        expected_completes = [False, True]
        expected_other_ranges = [{p for p in self.ranges[5:7]},
                                 set(overlap_with_gaps)]
        expected_best_path = [set(self.ranges[:5] + self.ranges[7:]
                                  + overlap_with_gaps),
                              set(self.ranges)]

        # make a shard range in both paths newer then any of the difference to
        # force a tie break situation
        self.ranges[2].timestamp = next(self.ts_iter).internal
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
        self.tempdir_base = mkdtemp()
        # add swift path structure needed for some tests
        self.tempdir = os.path.join(
            self.tempdir_base, 'part', 'suffix', 'hash')
        os.makedirs(self.tempdir)
        self.ts_iter = make_timestamp_iter()

    def _make_broker(self, account='a', container='c', epoch=None):
        if epoch:
            filename = 'test_%s.db' % epoch
        else:
            filename = 'test.db'
        db_file = os.path.join(self.tempdir, filename)
        broker = ContainerBroker(
            db_file, account=account, container=container,
            logger=debug_logger())
        broker.initialize()
        return broker

    def ts_internal(self):
        return next(self.ts_iter).internal

    def ts_encoded(self):
        # make a unique timestamp string with multiple timestamps encoded;
        # use different deltas between component timestamps
        timestamps = [next(self.ts_iter) for i in range(4)]
        return encode_timestamps(
            timestamps[0], timestamps[1], timestamps[3])

    def tearDown(self):
        shutil.rmtree(self.tempdir_base, ignore_errors=True)

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
            'rcache': '/var/cache/swift/container-sharder.recon',
            'shards_account_prefix': '.shards_',
        }
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
            'rcache': '/var/cache/swift/container-sharder.recon',
            'auto_create_account_prefix': '...',
        }
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
            'rcache': '/var/cache/swift/container-sharder.recon',
            'shards_account_prefix': '...shards_',
        }
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
    def _mock_sharder(self, conf=None):
        conf = conf or {}
        conf['devices'] = self.tempdir
        with mock.patch(
                'swift.container.sharder.internal_client.InternalClient'):
            with mock.patch(
                    'swift.common.db_replicator.ring.Ring',
                    lambda *args, **kwargs: FakeRing()):
                sharder = ContainerSharder(conf, logger=debug_logger())
                sharder._local_device_ids = {0, 1, 2}
                sharder._replicate_object = mock.MagicMock()
                sharder.shard_cleanups = dict()  # TODO: try to eliminate this
                yield sharder

    def _check_objects(self, expected_objs, shard_db):
        shard_broker = ContainerBroker(shard_db)
        # use list_objects_iter with no-op transform_func to get back actual
        # un-transformed rows with encoded timestamps
        shard_objs = [list(obj) for obj in shard_broker.list_objects_iter(
            10, '', '', '', '', include_deleted=True,
            transform_func=lambda record, policy_index: record)]
        expected_objs = [list(obj) for obj in expected_objs]
        self.assertEqual(expected_objs, shard_objs)

    def _check_shard_range(self, expected, actual):
        expected_dict = dict(expected)
        actual_dict = dict(actual)
        self.assertGreater(actual_dict.pop('meta_timestamp'),
                           expected_dict.pop('meta_timestamp'))
        self.assertGreater(actual_dict.pop('state_timestamp'),
                           expected_dict.pop('state_timestamp'))
        self.assertEqual(expected_dict, actual_dict)

    def test_cleave_root(self):
        broker = self._make_broker()
        objects = [
            # shard 0
            ('a', self.ts_encoded(), 10, 'text/plain', 'etag_a', 0),
            ('here', self.ts_encoded(), 10, 'text/plain', 'etag_here', 0),
            # shard 1
            ('m', self.ts_encoded(), 1, 'text/plain', 'etag_m', 0),
            ('n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0),
            ('there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0),
            # shard 2
            ('where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0),
            # shard 3
            ('x', self.ts_encoded(), 0, '', '', 1),  # deleted
            ('y', self.ts_encoded(), 1000, 'text/plain', 'etag_y', 0),
            # shard 4
            ('yyyy', self.ts_encoded(), 14, 'text/plain', 'etag_yyyy', 0),
        ]
        for obj in objects:
            broker.put_object(*obj)
        initial_root_info = broker.get_info()
        cleaving_ref = 'None-' + initial_root_info['hash']

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', 'yonder'),
                        ('yonder', ''))
        shard_ranges = [
            ShardRange('.sharded_a/%s-%s' % (lower, upper),
                       Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        # used to accumulate stats from sharded dbs
        total_shard_stats = {'object_count': 0, 'bytes_used': 0}
        # run cleave - no shard ranges, nothing happens
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        self.assertEqual(UNSHARDED, broker.get_db_state())
        sharder._replicate_object.assert_not_called()
        for db in expected_shard_dbs:
            with annotate_failure(db):
                self.assertFalse(os.path.exists(db))

        # run cleave - all shard ranges in found state, nothing happens
        broker.merge_shard_ranges(shard_ranges[:4])
        broker.set_sharding_state(epoch=Timestamp.now())

        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_not_called()
        for db in expected_shard_dbs:
            with annotate_failure(db):
                self.assertFalse(os.path.exists(db))
        for shard_range in broker.get_shard_ranges():
            with annotate_failure(shard_range):
                self.assertEqual(ShardRange.FOUND, shard_range.state)

        # move first shard range to created state, first shard range is cleaved
        shard_ranges[0].update_state(ShardRange.CREATED)
        broker.merge_shard_ranges(shard_ranges[:1])
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[0], 0)
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        self.assertEqual(
            ShardRange.FOUND, shard_broker.get_own_shard_range().state)
        shard_info = shard_broker.get_info()
        total_shard_stats['object_count'] += shard_info['object_count']
        total_shard_stats['bytes_used'] += shard_info['bytes_used']

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))
        # update expected state and metadata, check cleaved shard range
        shard_ranges[0].bytes_used = 20
        shard_ranges[0].object_count = 2
        shard_ranges[0].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        self._check_objects(objects[:2], expected_shard_dbs[0])
        # other shard ranges should be unchanged
        for i in range(1, len(shard_ranges)):
            with annotate_failure(i):
                self.assertFalse(os.path.exists(expected_shard_dbs[i]))
        for i in range(1, len(updated_shard_ranges)):
            with annotate_failure(i):
                self.assertEqual(dict(shard_ranges[i]),
                                 dict(updated_shard_ranges[i]))
        metadata = broker.metadata
        cursor_key = 'X-Container-Sysmeta-Shard-Cursor-%s' % cleaving_ref
        self.assertIn(cursor_key, metadata)
        self.assertEqual(
            {'cursor': 'here'}, json.loads(metadata[cursor_key][0]))
        unlink_files(expected_shard_dbs)

        # move more shard ranges to created state
        for i in range(1, 4):
            shard_ranges[i].update_state(ShardRange.CREATED)
        broker.merge_shard_ranges(shard_ranges[1:4])
        with self._mock_sharder() as sharder:
            self.assertFalse(sharder._cleave(broker))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[1:3]]
        )
        for db in expected_shard_dbs[1:3]:
            shard_broker = ContainerBroker(db)
            self.assertEqual(
                ShardRange.FOUND, shard_broker.get_own_shard_range().state)
            shard_info = shard_broker.get_info()
            total_shard_stats['object_count'] += shard_info['object_count']
            total_shard_stats['bytes_used'] += shard_info['bytes_used']

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))

        # only 2 are cleaved per batch
        # update expected state and metadata, check cleaved shard ranges
        shard_ranges[1].bytes_used = 6
        shard_ranges[1].object_count = 3
        shard_ranges[1].state = ShardRange.ACTIVE
        shard_ranges[2].bytes_used = 100
        shard_ranges[2].object_count = 1
        shard_ranges[2].state = ShardRange.ACTIVE
        for i in range(0, 3):
            with annotate_failure(i):
                self._check_shard_range(
                    shard_ranges[i], updated_shard_ranges[i])
        self._check_objects(objects[2:5], expected_shard_dbs[1])
        self._check_objects(objects[5:6], expected_shard_dbs[2])
        # other shard ranges should be unchanged
        self.assertFalse(os.path.exists(expected_shard_dbs[0]))
        for i, db in enumerate(expected_shard_dbs[3:], 3):
            with annotate_failure(i):
                self.assertFalse(os.path.exists(db))
        for i, updated_shard_range in enumerate(updated_shard_ranges[3:], 3):
            with annotate_failure(i):
                self.assertEqual(dict(shard_ranges[i]),
                                 dict(updated_shard_range))
        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual(
            {'cursor': 'where'}, json.loads(metadata[cursor_key][0]))
        unlink_files(expected_shard_dbs)

        # run cleave again - should process the fourth range
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[3], 0)
        shard_broker = ContainerBroker(expected_shard_dbs[3])
        self.assertEqual(
            ShardRange.FOUND, shard_broker.get_own_shard_range().state)
        shard_info = shard_broker.get_info()
        total_shard_stats['object_count'] += shard_info['object_count']
        total_shard_stats['bytes_used'] += shard_info['bytes_used']

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))

        shard_ranges[3].bytes_used = 1000
        shard_ranges[3].object_count = 1
        shard_ranges[3].state = ShardRange.ACTIVE
        for i in range(0, 4):
            with annotate_failure(i):
                self._check_shard_range(
                    shard_ranges[i], updated_shard_ranges[i])
        # NB includes the deleted object
        self._check_objects(objects[6:8], expected_shard_dbs[3])
        # other shard ranges should be unchanged
        for i, db in enumerate(expected_shard_dbs[:3]):
            with annotate_failure(i):
                self.assertFalse(os.path.exists(db))
        self.assertFalse(os.path.exists(expected_shard_dbs[4]))
        for i, updated_shard_range in enumerate(updated_shard_ranges[4:], 4):
            with annotate_failure(i):
                self.assertEqual(dict(shard_ranges[i]),
                                 dict(updated_shard_range))

        self.assertFalse(os.path.exists(expected_shard_dbs[4]))
        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual(
            {'cursor': 'yonder'}, json.loads(metadata[cursor_key][0]))
        unlink_files(expected_shard_dbs)

        # run cleave - should be a no-op, all existing ranges have been cleaved
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_not_called()

        # add final shard range - move this to ACTIVE state and update stats to
        # simulate another replica having cleaved it and replicated it state
        shard_ranges[4].update_state(ShardRange.ACTIVE)
        shard_ranges[4].update_meta(2, 15)
        broker.merge_shard_ranges(shard_ranges[4:])
        broker.update_sharding_info({'Scan-Done': 'True'})

        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[4], 0)
        shard_broker = ContainerBroker(expected_shard_dbs[4])
        # NB the state of the *shard broker's own shard range* should be
        # FOUND regardless of the shard range being ACTIVE
        self.assertEqual(
            ShardRange.FOUND, shard_broker.get_own_shard_range().state)
        shard_info = shard_broker.get_info()
        total_shard_stats['object_count'] += shard_info['object_count']
        total_shard_stats['bytes_used'] += shard_info['bytes_used']

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(5, len(updated_shard_ranges))
        # NB stats of the ACTIVE shard range should not be reset by cleaving
        for i in range(0, 4):
            with annotate_failure(i):
                self._check_shard_range(
                    shard_ranges[i], updated_shard_ranges[i])
        self.assertEqual(dict(shard_ranges[4]), dict(updated_shard_ranges[4]))

        # object copied to shard
        self._check_objects(objects[8:], expected_shard_dbs[4])
        # other shard ranges should be unchanged
        for i, db in enumerate(expected_shard_dbs[:4]):
            with annotate_failure(i):
                self.assertFalse(os.path.exists(db))

        self.assertEqual(initial_root_info['object_count'],
                         total_shard_stats['object_count'])
        self.assertEqual(initial_root_info['bytes_used'],
                         total_shard_stats['bytes_used'])

        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual(
            {'cursor': '', 'done': True}, json.loads(metadata[cursor_key][0]))

        broker.set_sharded_state()
        # run cleave - should be a no-op
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        sharder._replicate_object.assert_not_called()

    def test_cleave_root_ranges_change(self):
        # verify that objects are not missed if shard ranges change between
        # cleaving batches
        broker = self._make_broker()
        objects = [
            ('a', self.ts_encoded(), 10, 'text/plain', 'etag_a', 0),
            ('b', self.ts_encoded(), 10, 'text/plain', 'etag_b', 0),
            ('c', self.ts_encoded(), 1, 'text/plain', 'etag_c', 0),
            ('d', self.ts_encoded(), 2, 'text/plain', 'etag_d', 0),
            ('e', self.ts_encoded(), 3, 'text/plain', 'etag_e', 0),
            ('f', self.ts_encoded(), 100, 'text/plain', 'etag_f', 0),
            ('x', self.ts_encoded(), 0, '', '', 1),  # deleted
            ('z', self.ts_encoded(), 1000, 'text/plain', 'etag_z', 0)
        ]
        for obj in objects:
            broker.put_object(*obj)
        cleaving_ref = 'None-' + broker.get_info()['hash']

        shard_bounds = (('', 'd'), ('d', 'x'), ('x', ''))
        shard_ranges = [
            ShardRange('.sharded_a/%s-%s' % (lower, upper),
                       Timestamp.now(), lower, upper,
                       state=ShardRange.CREATED)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        broker.merge_shard_ranges(shard_ranges[:3])
        broker.set_sharding_state(epoch=Timestamp.now())

        # run cleave - first batch is cleaved
        with self._mock_sharder() as sharder:
            self.assertFalse(sharder._cleave(broker))
        cursor_key = 'X-Container-Sysmeta-Shard-Cursor-%s' % cleaving_ref
        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual({'cursor': shard_ranges[1].upper},
                         json.loads(metadata[cursor_key][0]))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[:2]]
        )

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(3, len(updated_shard_ranges))

        # first 2 shard ranges should have updated object count, bytes used and
        # meta_timestamp
        shard_ranges[0].bytes_used = 23
        shard_ranges[0].object_count = 4
        shard_ranges[0].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        shard_ranges[1].bytes_used = 103
        shard_ranges[1].object_count = 2
        shard_ranges[1].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[1], updated_shard_ranges[1])
        self._check_objects(objects[:4], expected_shard_dbs[0])
        self._check_objects(objects[4:7], expected_shard_dbs[1])
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

        # third shard range should be unchanged - not yet cleaved
        self.assertEqual(dict(shard_ranges[2]),
                         dict(updated_shard_ranges[2]))

        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual({'cursor': shard_ranges[1].upper},
                         json.loads(metadata[cursor_key][0]))

        # now change the shard ranges so that third consumes second
        shard_ranges[1].deleted = 1
        shard_ranges[1].timestamp = Timestamp.now()
        shard_ranges[2].lower = 'd'
        shard_ranges[2].timestamp = Timestamp.now()

        broker.merge_shard_ranges(shard_ranges[1:3])

        # run cleave - should process the extended third (final) range
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[2], 0)
        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(2, len(updated_shard_ranges))
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        # third shard range should now have updated object count, bytes used,
        # including objects previously in the second shard range
        shard_ranges[2].bytes_used = 1103
        shard_ranges[2].object_count = 3
        shard_ranges[2].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[2], updated_shard_ranges[1])
        self._check_objects(objects[4:8], expected_shard_dbs[2])

        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual({'cursor': str(shard_ranges[2].upper), 'done': True},
                         json.loads(metadata[cursor_key][0]))

    def test_cleave_shard(self):
        broker = self._make_broker(account='.sharded_a', container='.shard_c')
        ts_int = next(self.ts_iter).internal
        shard_metadata = {
            'X-Container-Sysmeta-Shard-Root': ('a/c', ts_int),
            'X-Container-Sysmeta-Shard-Lower': ('here', ts_int),
            'X-Container-Sysmeta-Shard-Upper': ('where', ts_int),
            'X-Container-Sysmeta-Shard-Timestamp': (ts_int, ts_int)}
        broker.update_metadata(shard_metadata)
        self.assertFalse(broker.is_root_container())  # sanity check

        objects = [
            ('m', self.ts_encoded(), 1, 'text/plain', 'etag_m', 0),
            ('n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0),
            ('there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0),
            ('where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0),
        ]
        for obj in objects:
            broker.put_object(*obj)
        cleaving_ref = 'None-' + broker.get_info()['hash']

        shard_bounds = (('here', 'there'),
                        ('there', 'where'))
        shard_ranges = [
            ShardRange('.sharded_a/%s-%s' % (lower, upper),
                       Timestamp.now(), lower, upper,
                       state=ShardRange.CREATED)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        broker.merge_shard_ranges(shard_ranges)
        broker.set_sharding_state(epoch=Timestamp.now())

        # run cleave - first range is cleaved
        sharder_conf = {'shard_batch_size': 1}
        with self._mock_sharder(sharder_conf) as sharder:
            self.assertFalse(sharder._cleave(broker))
        cursor_key = 'X-Container-Sysmeta-Shard-Cursor-%s' % cleaving_ref
        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual({'cursor': shard_ranges[0].upper},
                         json.loads(metadata[cursor_key][0]))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[0], 0)])
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        self.assertEqual(
            ShardRange.FOUND, shard_broker.get_own_shard_range().state)

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(2, len(updated_shard_ranges))

        # first shard range should have updated object count, bytes used and
        # meta_timestamp
        shard_ranges[0].bytes_used = 6
        shard_ranges[0].object_count = 3
        shard_ranges[0].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        self._check_objects(objects[:3], expected_shard_dbs[0])
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        unlink_files(expected_shard_dbs)

        # run cleave - second (final) range is cleaved; move this range to
        # ACTIVE state and update stats to simulate another replica having
        # cleaved it and replicated it state
        shard_ranges[1].update_state(ShardRange.ACTIVE)
        shard_ranges[1].update_meta(2, 15)
        broker.merge_shard_ranges(shard_ranges[1:2])
        with self._mock_sharder(sharder_conf) as sharder:
            self.assertTrue(sharder._cleave(broker))
        metadata = broker.metadata
        self.assertIn(cursor_key, metadata)
        self.assertEqual({'cursor': 'where', 'done': True},
                         json.loads(metadata[cursor_key][0]))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[1], 0)])
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        # NB the state of the *shard broker's own shard range* should be
        # FOUND regardless of the shard range being ACTIVE
        self.assertEqual(
            ShardRange.FOUND, shard_broker.get_own_shard_range().state)

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(2, len(updated_shard_ranges))

        # second shard range should have updated object count, bytes used and
        # meta_timestamp
        self.assertEqual(dict(shard_ranges[1]), dict(updated_shard_ranges[1]))
        self._check_objects(objects[3:], expected_shard_dbs[1])
        self.assertFalse(os.path.exists(expected_shard_dbs[0]))

    def test_misplaced_objects_root_container(self):
        broker = self._make_broker()
        cleaving_ref = 'None-' + broker.get_info()['hash']
        own_sr = broker.get_own_shard_range()
        node = {'id': 2, 'index': 1}

        objects = [
            # misplaced objects in second and third shard ranges
            ['n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0],
            ['there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0],
            ['where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0],
            # deleted
            ['x', self.ts_encoded(), 0, '', '', 1],
        ]

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', 'yonder'),
                        ('yonder', ''))
        initial_shard_ranges = [
            ShardRange('.sharded_a/%s-%s' % (lower, upper),
                       Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for shard_range in initial_shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(initial_shard_ranges)

        # unsharded
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_not_called()
        self.assertEqual(0, sharder.stats['containers_misplaced'])
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_items_found'))

        # sharding - no misplaced objects
        broker.set_sharding_state(epoch=Timestamp.now())
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_not_called()
        self.assertEqual(0, sharder.stats['containers_misplaced'])
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_items_found'))

        # pretend we cleaved up to end of second shard range
        cursor_key = 'Cursor-%s' % cleaving_ref
        broker.update_sharding_info(
            {cursor_key: json.dumps({'cursor': 'there'})})
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_not_called()
        self.assertEqual(0, sharder.stats['containers_misplaced'])
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_items_found'))

        # sharding - misplaced objects
        for obj in objects:
            broker.put_object(*obj)
        # pretend we have not cleaved any ranges
        broker.update_sharding_info({cursor_key: json.dumps({})})
        broker.update_sharding_info({cursor_key: None})
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_not_called()
        self.assertEqual(0, sharder.stats['containers_misplaced'])
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_items_found'))
        self.assertFalse(os.path.exists(expected_shard_dbs[0]))
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))
        self.assertFalse(os.path.exists(expected_shard_dbs[3]))
        self.assertFalse(os.path.exists(expected_shard_dbs[4]))

        # pretend we cleaved up to end of second shard range
        broker.update_sharding_info(
            {cursor_key: json.dumps({'cursor': 'there'})})
        broker.update_sharding_info(
            {cursor_key: json.dumps({'cursor': 'there'})})
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[1], 0)
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])
        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_shard_dbs[1])
        # ... and removed from the source db
        self._check_objects(objects[2:], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_shard_dbs[0]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))
        self.assertFalse(os.path.exists(expected_shard_dbs[3]))
        self.assertFalse(os.path.exists(expected_shard_dbs[4]))

        # pretend we cleaved up to end of fourth shard range
        broker.update_sharding_info(
            {cursor_key: json.dumps({'cursor': 'yonder'})})
        # and some new misplaced updates arrived in the first shard range
        new_objects = [
            ['b', self.ts_encoded(), 10, 'text/plain', 'etag_b', 0],
            ['c', self.ts_encoded(), 20, 'text/plain', 'etag_c', 0],
        ]
        for obj in new_objects:
            broker.put_object(*obj)

        with mock.patch('swift.container.sharder.CONTAINER_LISTING_LIMIT', 2):
            # check that *all* misplaced objects are moved despite exceeding
            # the listing limit
            with self._mock_sharder() as sharder:
                sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[2:4]],
            any_order=True
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])

        # check misplaced objects were moved
        self._check_objects(new_objects, expected_shard_dbs[0])
        self._check_objects(objects[:2], expected_shard_dbs[1])
        self._check_objects(objects[2:3], expected_shard_dbs[2])
        self._check_objects(objects[3:], expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)
        self.assertFalse(os.path.exists(expected_shard_dbs[4]))

        # pretend we cleaved all ranges - sharded state
        broker.update_sharding_info(
            {cursor_key: json.dumps({'cursor': '', 'done': True})})
        self.assertTrue(broker.set_sharded_state())
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_not_called()
        self.assertEqual(0, sharder.stats['containers_misplaced'])
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_items_found'))

        # and then more misplaced updates arrive
        newer_objects = [
            ['a', self.ts_encoded(), 51, 'text/plain', 'etag_a', 0],
            ['z', self.ts_encoded(), 52, 'text/plain', 'etag_z', 0],
        ]
        for obj in newer_objects:
            broker.put_object(*obj)
        broker.get_info()  # force updates to be committed
        # sanity check the puts landed in sharded broker
        self._check_objects(newer_objects, broker.db_file)

        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0)
             for db in (expected_shard_dbs[0], expected_shard_dbs[-1])],
            any_order=True
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])

        # check new misplaced objects were moved
        self._check_objects(newer_objects[:1] + new_objects,
                            expected_shard_dbs[0])
        self._check_objects(newer_objects[1:], expected_shard_dbs[4])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)
        # ... and other shard dbs were unchanged
        self._check_objects(objects[:2], expected_shard_dbs[1])
        self._check_objects(objects[2:3], expected_shard_dbs[2])
        self._check_objects(objects[3:], expected_shard_dbs[3])

    def _check_misplaced_objects_shard_container_unsharded(self):
        broker = self._make_broker(account='.sharded_a', container='.shard_c')
        ts_shard = next(self.ts_iter)
        own_sr = ShardRange('.sharded_a/shard_c', ts_shard, 'here', 'there')
        broker.update_sharding_info({'Lower': own_sr.lower,
                                     'Upper': own_sr.upper,
                                     'Timestamp': own_sr.timestamp.internal,
                                     'Root': 'a/c'})
        self.assertEqual(own_sr, broker.get_own_shard_range())  # sanity check
        self.assertEqual(UNSHARDED, broker.get_db_state())
        node = {'id': 2, 'index': 1}

        objects = [
            # some of these are misplaced objects
            ['b', self.ts_encoded(), 2, 'text/plain', 'etag_b', 0],
            ['here', self.ts_encoded(), 2, 'text/plain', 'etag_here', 0],
            ['n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0],
            ['there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0],
            ['x', self.ts_encoded(), 0, '', '', 1],  # deleted
            ['y', self.ts_encoded(), 10, 'text/plain', 'etag_y', 0],
        ]

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', ''))
        root_shard_ranges = [
            ShardRange('.sharded_a/%s-%s' % (lower, upper),
                       Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for sr in root_shard_ranges:
            db_hash = hash_path(sr.account, sr.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        # no objects
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_not_called()
        self.assertEqual(0, sharder.stats['containers_misplaced'])
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_items_found'))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # now put objects
        for obj in objects:
            broker.put_object(*obj)
        self._check_objects(objects, broker.db_file)  # sanity check

        # NB final shard range not available
        with self._mock_sharder() as sharder:
            sharder._get_shard_ranges = lambda *a, **k: root_shard_ranges[:-1]
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_called_with(
            0, expected_shard_dbs[0], 0),

        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])
        # some misplaced objects could not be moved...
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn(
            'Failed to find destination for at least 2 misplaced objects',
            warning_lines)
        sharder.logger.clear()

        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_shard_dbs[0])
        # ... and removed from the source db
        self._check_objects(objects[2:], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))
        self.assertFalse(os.path.exists(expected_shard_dbs[3]))

        # repeat with final shard range available
        with self._mock_sharder() as sharder:
            sharder._get_shard_ranges = lambda *a, **k: root_shard_ranges
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_called_with(
            0, expected_shard_dbs[-1], 0),

        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_shard_dbs[0])
        self._check_objects(objects[4:], expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects(objects[2:4], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

        # repeat - no work remaining
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_not_called()
        self.assertEqual(0, sharder.stats['containers_misplaced'])
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_items_found'))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # and then more misplaced updates arrive
        new_objects = [
            ['a', self.ts_encoded(), 51, 'text/plain', 'etag_a', 0],
            ['z', self.ts_encoded(), 52, 'text/plain', 'etag_z', 0],
        ]
        for obj in new_objects:
            broker.put_object(*obj)
        # sanity check the puts landed in sharded broker
        self._check_objects(new_objects[:1] + objects[2:4] + new_objects[1:],
                            broker.db_file)

        with self._mock_sharder() as sharder:
            sharder._get_shard_ranges = lambda *a, **k: root_shard_ranges
            sharder._misplaced_objects(broker, node, own_sr)
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0)
             for db in (expected_shard_dbs[0], expected_shard_dbs[3])],
            any_order=True
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # check new misplaced objects were moved
        self._check_objects(new_objects[:1] + objects[:2],
                            expected_shard_dbs[0])
        self._check_objects(objects[4:] + new_objects[1:],
                            expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects(objects[2:4], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

    def test_misplaced_objects_shard_container_unsharded(self):
        self._check_misplaced_objects_shard_container_unsharded()

    def test_misplaced_objects_shard_container_unsharded_limit_two(self):
        with mock.patch('swift.container.sharder.CONTAINER_LISTING_LIMIT', 2):
            self._check_misplaced_objects_shard_container_unsharded()

    def test_misplaced_objects_shard_container_unsharded_limit_one(self):
        with mock.patch('swift.container.sharder.CONTAINER_LISTING_LIMIT', 1):
            self._check_misplaced_objects_shard_container_unsharded()

    def test_misplaced_objects_shard_container_sharding(self):
        broker = self._make_broker(account='.sharded_a', container='.shard_c')
        ts_shard = next(self.ts_iter)
        # note that own_sr spans two root shard ranges
        own_sr = ShardRange('.sharded_a/shard_c', ts_shard, 'here', 'where')
        broker.update_sharding_info({'Lower': own_sr.lower,
                                     'Upper': own_sr.upper,
                                     'Timestamp': own_sr.timestamp.internal,
                                     'Root': 'a/c'})
        self.assertEqual(own_sr, broker.get_own_shard_range())  # sanity check
        self.assertEqual(UNSHARDED, broker.get_db_state())
        node = {'id': 2, 'index': 1}
        cleaving_ref = 'None-' + broker.get_info()['hash']

        objects = [
            # some of these are misplaced objects
            ['b', self.ts_encoded(), 2, 'text/plain', 'etag_b', 0],
            ['here', self.ts_encoded(), 2, 'text/plain', 'etag_here', 0],
            ['n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0],
            ['there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0],
            ['v', self.ts_encoded(), 10, 'text/plain', 'etag_v', 0],
            ['y', self.ts_encoded(), 10, 'text/plain', 'etag_y', 0],
        ]

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', ''))
        root_shard_ranges = [
            ShardRange('.sharded_a/%s-%s' %
                       (lower, upper), Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for sr in root_shard_ranges:
            db_hash = hash_path(sr.account, sr.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        # pretend broker is sharding but not yet cleaved a shard
        broker.set_sharding_state(epoch=Timestamp.now())
        broker.merge_shard_ranges([dict(sr) for sr in root_shard_ranges[1:3]])
        # then some updates arrive
        for obj in objects:
            broker.put_object(*obj)
        broker.get_info()
        self._check_objects(objects, broker.db_file)  # sanity check

        # first destination is not available
        with self._mock_sharder() as sharder:
            sharder._get_shard_ranges = lambda *a, **k: root_shard_ranges[1:]
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[-1], 0)],
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn(
            'Failed to find destination for at least 2 misplaced objects',
            warning_lines)
        sharder.logger.clear()

        # check some misplaced objects were moved
        self._check_objects(objects[5:], expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects(objects[:5], broker.db_file)
        self.assertFalse(os.path.exists(expected_shard_dbs[0]))
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

        # normality resumes and all destinations are available
        with self._mock_sharder() as sharder:
            sharder._get_shard_ranges = lambda *a, **k: root_shard_ranges
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[0], 0)],
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_shard_dbs[0])
        self._check_objects(objects[5:], expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects(objects[2:5], broker.db_file)
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

        # pretend first shard has been cleaved
        cursor_key = 'Cursor-%s' % cleaving_ref
        broker.update_sharding_info(
            {cursor_key: json.dumps({'cursor': 'there'})})
        # and then more misplaced updates arrive
        new_objects = [
            ['a', self.ts_encoded(), 51, 'text/plain', 'etag_a', 0],
            # this one is in the now cleaved shard range...
            ['k', self.ts_encoded(), 52, 'text/plain', 'etag_k', 0],
            ['z', self.ts_encoded(), 53, 'text/plain', 'etag_z', 0],
        ]
        for obj in new_objects:
            broker.put_object(*obj)
        broker.get_info()  # force updates to be committed
        # sanity check the puts landed in sharded broker
        self._check_objects(sorted(new_objects + objects[2:5]), broker.db_file)
        with self._mock_sharder() as sharder:
            sharder._get_shard_ranges = lambda *a, **k: root_shard_ranges
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1],
                                            expected_shard_dbs[-1])],
            any_order=True
        )

        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # check *all* the misplaced objects were moved
        self._check_objects(new_objects[:1] + objects[:2],
                            expected_shard_dbs[0])
        self._check_objects(new_objects[1:2] + objects[2:4],
                            expected_shard_dbs[1])
        self._check_objects(objects[5:] + new_objects[2:],
                            expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects(objects[4:5], broker.db_file)
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

    def test_misplaced_objects_deleted_and_updated(self):
        # setup
        broker = self._make_broker()
        cleaving_ref = 'None-' + broker.get_info()['hash']
        own_sr = broker.get_own_shard_range()
        node = {'id': 2, 'index': 1}

        shard_bounds = (('', 'here'), ('here', ''))
        root_shard_ranges = [
            ShardRange('.sharded_a/%s-%s' % (lower, upper),
                       Timestamp.now(), lower, upper)
            for lower, upper in shard_bounds
        ]
        expected_shard_dbs = []
        for sr in root_shard_ranges:
            db_hash = hash_path(sr.account, sr.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(root_shard_ranges)
        broker.set_sharding_state(epoch=Timestamp.now())

        ts_older_internal = self.ts_encoded()  # used later
        # put deleted objects into source
        objects = [
            ['b', self.ts_encoded(), 0, '', '', 1],
            ['x', self.ts_encoded(), 0, '', '', 1]
        ]
        for obj in objects:
            broker.put_object(*obj)
        broker.get_info()
        self._check_objects(objects, broker.db_file)  # sanity check
        # pretend we cleaved all ranges - sharded state
        cursor_key = 'Cursor-%s' % cleaving_ref
        broker.update_sharding_info(
            {cursor_key: json.dumps({'cursor': '', 'done': True})})
        self.assertTrue(broker.set_sharded_state())

        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1])],
            any_order=True
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])

        # check new misplaced objects were moved
        self._check_objects(objects[:1], expected_shard_dbs[0])
        self._check_objects(objects[1:], expected_shard_dbs[1])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)

        # update source db with older undeleted versions of same objects
        old_objects = [
            ['b', ts_older_internal, 2, 'text/plain', 'etag_b', 0],
            ['x', ts_older_internal, 4, 'text/plain', 'etag_x', 0]
        ]
        for obj in old_objects:
            broker.put_object(*obj)
        broker.get_info()
        self._check_objects(old_objects, broker.db_file)  # sanity check
        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1])],
            any_order=True
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])

        # check older misplaced objects were not merged to shard brokers
        self._check_objects(objects[:1], expected_shard_dbs[0])
        self._check_objects(objects[1:], expected_shard_dbs[1])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)

        # the destination shard dbs for misplaced objects may already exist so
        # check they are updated correctly when overwriting objects
        # update source db with newer deleted versions of same objects
        new_objects = [
            ['b', self.ts_encoded(), 0, '', '', 1],
            ['x', self.ts_encoded(), 0, '', '', 1]
        ]
        for obj in new_objects:
            broker.put_object(*obj)
        broker.get_info()
        self._check_objects(new_objects, broker.db_file)  # sanity check
        shard_broker = ContainerBroker(
            expected_shard_dbs[0], account=root_shard_ranges[0].account,
            container=root_shard_ranges[0].container)
        # update one shard container with even newer version of object
        timestamps = [next(self.ts_iter) for i in range(7)]
        ts_newer = encode_timestamps(
            timestamps[1], timestamps[3], timestamps[5])
        newer_object = ('b', ts_newer, 10, 'text/plain', 'etag_b', 0)
        shard_broker.put_object(*newer_object)

        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1])],
            any_order=True
        )
        self.assertEqual(1, sharder.stats['containers_misplaced'])
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_items_found'])

        # check only the newer misplaced object was moved
        self._check_objects([newer_object], expected_shard_dbs[0])
        self._check_objects(new_objects[1:], expected_shard_dbs[1])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)

        # update source with a version of 'b' that has newer data
        # but older content-type and metadata relative to shard object
        ts_update = encode_timestamps(
            timestamps[2], timestamps[3], timestamps[4])
        update_object = ('b', ts_update, 20, 'text/ignored', 'etag_newer', 0)
        broker.put_object(*update_object)

        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)

        ts_expected = encode_timestamps(
            timestamps[2], timestamps[3], timestamps[5])
        expected = ('b', ts_expected, 20, 'text/plain', 'etag_newer', 0)
        self._check_objects([expected], expected_shard_dbs[0])
        self._check_objects([], broker.db_file)

        # update source with a version of 'b' that has older data
        # and content-type but newer metadata relative to shard object
        ts_update = encode_timestamps(
            timestamps[1], timestamps[3], timestamps[6])
        update_object = ('b', ts_update, 999, 'text/ignored', 'etag_b', 0)
        broker.put_object(*update_object)

        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)

        ts_expected = encode_timestamps(
            timestamps[2], timestamps[3], timestamps[6])
        expected = ('b', ts_expected, 20, 'text/plain', 'etag_newer', 0)
        self._check_objects([expected], expected_shard_dbs[0])
        self._check_objects([], broker.db_file)

        # update source with a version of 'b' that has older data
        # but newer content-type and metadata
        ts_update = encode_timestamps(
            timestamps[2], timestamps[6], timestamps[6])
        update_object = ('b', ts_update, 999, 'text/newer', 'etag_b', 0)
        broker.put_object(*update_object)

        with self._mock_sharder() as sharder:
            sharder._misplaced_objects(broker, node, own_sr)

        ts_expected = encode_timestamps(
            timestamps[2], timestamps[6], timestamps[6])
        expected = ('b', ts_expected, 20, 'text/newer', 'etag_newer', 0)
        self._check_objects([expected], expected_shard_dbs[0])
        self._check_objects([], broker.db_file)

    def test_sharding_enabled(self):
        logger = debug_logger()
        db_file = os.path.join(self.tempdir, 'test.db')
        broker = ContainerBroker(
            db_file, account='a', container='c', logger=logger)
        broker.initialize()
        self.assertFalse(sharding_enabled(broker))
        broker.update_metadata(
            {'X-Container-Sysmeta-Sharding':
             ('yes', Timestamp.now().internal)})
        self.assertTrue(sharding_enabled(broker))
        # deleting broker clears sharding sysmeta
        broker.delete_db(Timestamp.now().internal)
        self.assertFalse(sharding_enabled(broker))
        # but if broker has a shard range then sharding is enabled
        broker.update_shard_range(
            ShardRange('acc/a_shard', Timestamp.now(), 'l', 'u'))
        self.assertTrue(sharding_enabled(broker))
