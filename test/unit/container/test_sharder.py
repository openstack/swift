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
import random
import argparse

import eventlet
import os
import shutil
from contextlib import contextmanager
from tempfile import mkdtemp
from uuid import uuid4

from unittest import mock
import unittest

from collections import defaultdict

import time

from copy import deepcopy

from swift.common import internal_client
from swift.container import replicator
from swift.container.backend import ContainerBroker, UNSHARDED, SHARDING, \
    SHARDED, DATADIR
from swift.container.sharder import ContainerSharder, sharding_enabled, \
    CleavingContext, DEFAULT_SHARDER_CONF, finalize_shrinking, \
    find_shrinking_candidates, process_compactible_shard_sequences, \
    find_compactible_shard_sequences, is_shrinking_candidate, \
    is_sharding_candidate, find_paths, rank_paths, ContainerSharderConf, \
    find_paths_with_gaps, combine_shard_ranges, find_overlapping_ranges, \
    update_own_shard_range_stats
from swift.common.utils import ShardRange, Timestamp, hash_path, \
    encode_timestamps, parse_db_filename, quorum_size, Everything, md5, \
    ShardName, Namespace
from test import annotate_failure

from test.debug_logger import debug_logger
from test.unit import FakeRing, make_timestamp_iter, unlink_files, \
    mocked_http_conn, mock_timestamp_now, mock_timestamp_now_with_iter, \
    attach_fake_replication_rpc


class BaseTestSharder(unittest.TestCase):
    def setUp(self):
        self.tempdir = mkdtemp()
        self.ts_iter = make_timestamp_iter()
        self.logger = debug_logger('sharder-test')

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _assert_shard_ranges_equal(self, expected, actual):
        self.assertEqual([dict(sr) for sr in expected],
                         [dict(sr) for sr in actual])

    def _make_broker(self, account='a', container='c', epoch=None,
                     device='sda', part=0, hash_=None, put_timestamp=None):
        hash_ = hash_ or md5(
            container.encode('utf-8'), usedforsecurity=False).hexdigest()
        datadir = os.path.join(
            self.tempdir, device, 'containers', str(part), hash_[-3:], hash_)
        if epoch:
            filename = '%s_%s.db' % (hash_, epoch)
        else:
            filename = hash_ + '.db'
        db_file = os.path.join(datadir, filename)
        broker = ContainerBroker(
            db_file, account=account, container=container,
            logger=self.logger)
        broker.initialize(put_timestamp=put_timestamp)
        return broker

    def _make_old_style_sharding_broker(self, account='a', container='c',
                                        shard_bounds=(('', 'middle'),
                                                      ('middle', ''))):
        broker = self._make_broker(account=account, container=container)
        broker.set_sharding_sysmeta('Root', 'a/c')
        old_db_id = broker.get_info()['id']
        broker.enable_sharding(next(self.ts_iter))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CLEAVED)
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        broker = ContainerBroker(broker.db_file, account='a', container='c')
        self.assertNotEqual(old_db_id, broker.get_info()['id'])  # sanity check
        return broker

    def _make_sharding_broker(self, account='a', container='c',
                              shard_bounds=(('', 'middle'), ('middle', ''))):
        broker = self._make_broker(account=account, container=container)
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        old_db_id = broker.get_info()['id']
        broker.enable_sharding(next(self.ts_iter))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CLEAVED)
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        broker = ContainerBroker(broker.db_file, account='a', container='c')
        self.assertNotEqual(old_db_id, broker.get_info()['id'])  # sanity check
        return broker

    def _make_shrinking_broker(self, account='.shards_a', container='shard_c',
                               lower='here', upper='there', objects=None):
        # caller should merge any acceptor range(s) into returned broker
        broker = self._make_broker(account=account, container=container)
        for obj in objects or []:
            broker.put_object(*obj)
        own_shard_range = ShardRange(
            broker.path, next(self.ts_iter), lower, upper,
            state=ShardRange.SHRINKING, epoch=next(self.ts_iter))
        broker.merge_shard_ranges([own_shard_range])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(broker.is_root_container())  # sanity check
        self.assertTrue(broker.set_sharding_state())
        return broker

    def _make_shard_ranges(self, bounds, state=None, object_count=0,
                           timestamp=Timestamp.now(), **kwargs):
        if not isinstance(state, (tuple, list)):
            state = [state] * len(bounds)
        state_iter = iter(state)
        return [ShardRange('.shards_a/c_%s_%s' % (upper, index), timestamp,
                           lower, upper, state=next(state_iter),
                           object_count=object_count, **kwargs)
                for index, (lower, upper) in enumerate(bounds)]

    def ts_encoded(self):
        # make a unique timestamp string with multiple timestamps encoded;
        # use different deltas between component timestamps
        timestamps = [next(self.ts_iter) for i in range(4)]
        return encode_timestamps(
            timestamps[0], timestamps[1], timestamps[3])


class TestSharder(BaseTestSharder):
    def _do_test_init(self, conf, expected, use_logger=True):
        logger = self.logger if use_logger else None
        if logger:
            logger.clear()
        with mock.patch(
                'swift.container.sharder.internal_client.InternalClient') \
                as mock_ic:
            with mock.patch('swift.common.db_replicator.ring.Ring') \
                    as mock_ring:
                mock_ring.return_value = mock.MagicMock()
                mock_ring.return_value.replica_count = 3
                sharder = ContainerSharder(conf, logger=logger)
        mock_ring.assert_called_once_with(
            '/etc/swift', ring_name='container')
        for k, v in expected.items():
            self.assertTrue(hasattr(sharder, k), 'Missing attr %s' % k)
            self.assertEqual(v, getattr(sharder, k),
                             'Incorrect value: expected %s=%s but got %s' %
                             (k, v, getattr(sharder, k)))
        return sharder, mock_ic

    def test_init(self):
        # default values
        expected = {
            'mount_check': True, 'bind_ip': '0.0.0.0', 'port': 6201,
            'per_diff': 1000, 'max_diffs': 100, 'interval': 30,
            'databases_per_second': 50,
            'cleave_row_batch_size': 10000,
            'node_timeout': 10, 'conn_timeout': 5,
            'rsync_compress': False,
            'rsync_module': '{replication_ip}::container',
            'reclaim_age': 86400 * 7,
            'shard_container_threshold': 1000000,
            'rows_per_shard': 500000,
            'shrink_threshold': 100000,
            'expansion_limit': 750000,
            'cleave_batch_size': 2,
            'shard_scanner_batch_size': 10,
            'rcache': '/var/cache/swift/container.recon',
            'shards_account_prefix': '.shards_',
            'auto_shard': False,
            'recon_candidates_limit': 5,
            'recon_sharded_timeout': 43200,
            'shard_replication_quorum': 2,
            'existing_shard_replication_quorum': 2,
            'max_shrinking': 1,
            'max_expanding': -1,
            'stats_interval': 3600,
        }
        sharder, mock_ic = self._do_test_init({}, expected, use_logger=False)
        self.assertEqual(
            'container-sharder', sharder.logger.logger.name)
        mock_ic.assert_called_once_with(
            '/etc/swift/internal-client.conf', 'Swift Container Sharder', 3,
            use_replication_network=True,
            global_conf={'log_name': 'container-sharder-ic'})
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])

        # non-default shard_container_threshold influences other defaults
        conf = {'shard_container_threshold': 20000000}
        expected.update({
            'shard_container_threshold': 20000000,
            'shrink_threshold': 2000000,
            'expansion_limit': 15000000,
            'rows_per_shard': 10000000
        })
        sharder, mock_ic = self._do_test_init(conf, expected)
        mock_ic.assert_called_once_with(
            '/etc/swift/internal-client.conf', 'Swift Container Sharder', 3,
            use_replication_network=True,
            global_conf={'log_name': 'container-sharder-ic'})
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])

        # non-default values
        conf = {
            'mount_check': False, 'bind_ip': '10.11.12.13', 'bind_port': 62010,
            'per_diff': 2000, 'max_diffs': 200, 'interval': 60,
            'databases_per_second': 5,
            'cleave_row_batch_size': 3000,
            'node_timeout': 20, 'conn_timeout': 1,
            'rsync_compress': True,
            'rsync_module': '{replication_ip}::container_sda/',
            'reclaim_age': 86400 * 14,
            'shrink_threshold': 2000000,
            'expansion_limit': 17000000,
            'shard_container_threshold': 20000000,
            'cleave_batch_size': 4,
            'shard_scanner_batch_size': 8,
            'request_tries': 2,
            'internal_client_conf_path': '/etc/swift/my-sharder-ic.conf',
            'recon_cache_path': '/var/cache/swift-alt',
            'auto_shard': 'yes',
            'recon_candidates_limit': 10,
            'recon_sharded_timeout': 7200,
            'shard_replication_quorum': 1,
            'existing_shard_replication_quorum': 0,
            'max_shrinking': 5,
            'max_expanding': 4,
            'rows_per_shard': 13000000,
            'stats_interval': 300,
        }
        expected = {
            'mount_check': False, 'bind_ip': '10.11.12.13', 'port': 62010,
            'per_diff': 2000, 'max_diffs': 200, 'interval': 60,
            'databases_per_second': 5,
            'cleave_row_batch_size': 3000,
            'node_timeout': 20, 'conn_timeout': 1,
            'rsync_compress': True,
            'rsync_module': '{replication_ip}::container_sda',
            'reclaim_age': 86400 * 14,
            'shard_container_threshold': 20000000,
            'rows_per_shard': 13000000,
            'shrink_threshold': 2000000,
            'expansion_limit': 17000000,
            'cleave_batch_size': 4,
            'shard_scanner_batch_size': 8,
            'rcache': '/var/cache/swift-alt/container.recon',
            'shards_account_prefix': '.shards_',
            'auto_shard': True,
            'recon_candidates_limit': 10,
            'recon_sharded_timeout': 7200,
            'shard_replication_quorum': 1,
            'existing_shard_replication_quorum': 0,
            'max_shrinking': 5,
            'max_expanding': 4,
            'stats_interval': 300,
        }
        sharder, mock_ic = self._do_test_init(conf, expected)
        mock_ic.assert_called_once_with(
            '/etc/swift/my-sharder-ic.conf', 'Swift Container Sharder', 2,
            use_replication_network=True,
            global_conf={'log_name': 'container-sharder-ic'})
        self.assertEqual(self.logger.get_lines_for_level('warning'), [])

        expected.update({'shard_replication_quorum': 3,
                         'existing_shard_replication_quorum': 3})
        conf.update({'shard_replication_quorum': 4,
                     'existing_shard_replication_quorum': 4})
        self._do_test_init(conf, expected)
        self.assertEqual(self.logger.get_lines_for_level('warning'), [
            'shard_replication_quorum of 4 exceeds replica count 3, '
            'reducing to 3',
            'existing_shard_replication_quorum of 4 exceeds replica count 3, '
            'reducing to 3',
        ])

        with self.assertRaises(ValueError) as cm:
            self._do_test_init({'shard_shrink_point': 101}, {})
        self.assertIn(
            'greater than 0, less than 100, not "101"', str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            self._do_test_init({'shard_shrink_merge_point': 101}, {})
        self.assertIn(
            'greater than 0, less than 100, not "101"', str(cm.exception))

    def test_init_deprecated_options(self):
        # percent values applied if absolute values not given
        conf = {
            'shard_shrink_point': 7,  # trumps shrink_threshold
            'shard_shrink_merge_point': 95,  # trumps expansion_limit
            'shard_container_threshold': 20000000,
        }
        expected = {
            'mount_check': True, 'bind_ip': '0.0.0.0', 'port': 6201,
            'per_diff': 1000, 'max_diffs': 100, 'interval': 30,
            'databases_per_second': 50,
            'cleave_row_batch_size': 10000,
            'node_timeout': 10, 'conn_timeout': 5,
            'rsync_compress': False,
            'rsync_module': '{replication_ip}::container',
            'reclaim_age': 86400 * 7,
            'shard_container_threshold': 20000000,
            'rows_per_shard': 10000000,
            'shrink_threshold': 1400000,
            'expansion_limit': 19000000,
            'cleave_batch_size': 2,
            'shard_scanner_batch_size': 10,
            'rcache': '/var/cache/swift/container.recon',
            'shards_account_prefix': '.shards_',
            'auto_shard': False,
            'recon_candidates_limit': 5,
            'shard_replication_quorum': 2,
            'existing_shard_replication_quorum': 2,
            'max_shrinking': 1,
            'max_expanding': -1
        }
        self._do_test_init(conf, expected)
        # absolute values override percent values
        conf = {
            'shard_shrink_point': 7,
            'shrink_threshold': 1300000,  # trumps shard_shrink_point
            'shard_shrink_merge_point': 95,
            'expansion_limit': 17000000,  # trumps shard_shrink_merge_point
            'shard_container_threshold': 20000000,
        }
        expected = {
            'mount_check': True, 'bind_ip': '0.0.0.0', 'port': 6201,
            'per_diff': 1000, 'max_diffs': 100, 'interval': 30,
            'databases_per_second': 50,
            'cleave_row_batch_size': 10000,
            'node_timeout': 10, 'conn_timeout': 5,
            'rsync_compress': False,
            'rsync_module': '{replication_ip}::container',
            'reclaim_age': 86400 * 7,
            'shard_container_threshold': 20000000,
            'rows_per_shard': 10000000,
            'shrink_threshold': 1300000,
            'expansion_limit': 17000000,
            'cleave_batch_size': 2,
            'shard_scanner_batch_size': 10,
            'rcache': '/var/cache/swift/container.recon',
            'shards_account_prefix': '.shards_',
            'auto_shard': False,
            'recon_candidates_limit': 5,
            'shard_replication_quorum': 2,
            'existing_shard_replication_quorum': 2,
            'max_shrinking': 1,
            'max_expanding': -1
        }
        self._do_test_init(conf, expected)

    def test_init_internal_client_conf_loading_error(self):
        with mock.patch('swift.common.db_replicator.ring.Ring') \
                as mock_ring:
            mock_ring.return_value = mock.MagicMock()
            mock_ring.return_value.replica_count = 3
            with self.assertRaises(SystemExit) as cm:
                ContainerSharder(
                    {'internal_client_conf_path':
                     os.path.join(self.tempdir, 'nonexistent')})
        self.assertIn('Unable to load internal client', str(cm.exception))

        with mock.patch('swift.common.db_replicator.ring.Ring') \
                as mock_ring:
            mock_ring.return_value = mock.MagicMock()
            mock_ring.return_value.replica_count = 3
            with mock.patch(
                    'swift.container.sharder.internal_client.InternalClient',
                    side_effect=Exception('kaboom')):
                with self.assertRaises(Exception) as cm:
                    ContainerSharder({})
        self.assertIn('kaboom', str(cm.exception))

    def test_init_internal_client_log_name(self):
        def _do_test_init_ic_log_name(conf, exp_internal_client_log_name):
            with mock.patch(
                    'swift.container.sharder.internal_client.InternalClient') \
                    as mock_ic:
                with mock.patch('swift.common.db_replicator.ring.Ring') \
                        as mock_ring:
                    mock_ring.return_value = mock.MagicMock()
                    mock_ring.return_value.replica_count = 3
                    ContainerSharder(conf)
            mock_ic.assert_called_once_with(
                '/etc/swift/internal-client.conf',
                'Swift Container Sharder', 3,
                global_conf={'log_name': exp_internal_client_log_name},
                use_replication_network=True)

        _do_test_init_ic_log_name({}, 'container-sharder-ic')
        _do_test_init_ic_log_name({'log_name': 'container-sharder-6021'},
                                  'container-sharder-6021-ic')

    def test_log_broker(self):
        broker = self._make_broker(container='c@d')

        def do_test(level):
            with self._mock_sharder() as sharder:
                func = getattr(sharder, level)
                func(broker, 'bonjour %s %s', 'mes', 'amis')
                func(broker, 'hello my %s', 'friend%04ds')
                func(broker, 'greetings friend%04ds')

            self.assertEqual(
                ['bonjour mes amis, path: a/c%40d, db: ' + broker.db_file,
                 'hello my friend%04ds, path: a/c%40d, db: ' + broker.db_file,
                 'greetings friend%04ds, path: a/c%40d, db: ' + broker.db_file
                 ], sharder.logger.get_lines_for_level(level))

            for log_level, lines in sharder.logger.all_log_lines().items():
                if log_level == level:
                    continue
                else:
                    self.assertFalse(lines)

        do_test('debug')
        do_test('info')
        do_test('warning')
        do_test('error')

    def test_log_broker_exception(self):
        broker = self._make_broker()

        with self._mock_sharder() as sharder:
            try:
                raise ValueError('test')
            except ValueError as err:
                sharder.exception(broker, 'exception: %s', err)

        self.assertEqual(
            ['exception: test, path: a/c, db: %s: ' % broker.db_file],
            sharder.logger.get_lines_for_level('error'))

        for log_level, lines in sharder.logger.all_log_lines().items():
            if log_level == 'error':
                continue
            else:
                self.assertFalse(lines)

    def test_log_broker_levels(self):
        # verify that the broker is not queried if the log level is not enabled
        broker = self._make_broker()
        # erase cached properties...
        broker.account = broker.container = None

        with self._mock_sharder() as sharder:
            with mock.patch.object(sharder.logger, 'isEnabledFor',
                                   return_value=False):
                sharder.debug(broker, 'test')
                sharder.info(broker, 'test')
                sharder.warning(broker, 'test')
                sharder.error(broker, 'test')

        # cached properties have not been set...
        self.assertIsNone(broker.account)
        self.assertIsNone(broker.container)
        self.assertFalse(sharder.logger.all_log_lines())

    def test_log_broker_exception_while_logging(self):
        broker = self._make_broker()

        def do_test(level):
            with self._mock_sharder() as sharder:
                func = getattr(sharder, level)
            with mock.patch.object(broker, '_populate_instance_cache',
                                   side_effect=Exception()):
                func(broker, 'bonjour %s %s', 'mes', 'amis')
            broker._db_files = None
            with mock.patch.object(broker, 'reload_db_files',
                                   side_effect=Exception()):
                func(broker, 'bonjour %s %s', 'mes', 'amis')

            self.assertEqual(
                ['bonjour mes amis, path: , db: %s' % broker.db_file,
                 'bonjour mes amis, path: a/c, db: '],
                sharder.logger.get_lines_for_level(level))

            for log_level, lines in sharder.logger.all_log_lines().items():
                if log_level == level:
                    continue
                else:
                    self.assertFalse(lines)

        do_test('debug')
        do_test('info')
        do_test('warning')
        do_test('error')

    def test_periodic_warning(self):
        now = [time.time()]

        def mock_time():
            return now[0]

        with mock.patch('swift.container.sharder.time.time', mock_time):
            with self._mock_sharder() as sharder:
                sharder.periodic_warnings_interval = 5
                broker1 = self._make_broker(container='c1')
                broker2 = self._make_broker(container='c2')
                for i in range(5):
                    sharder.periodic_warning(broker1, 'periodic warning 1')
                    sharder.periodic_warning(broker1, 'periodic warning 1a')
                    sharder.periodic_warning(broker2, 'periodic warning 2')
                    now[0] += 1
                sharder.warning(broker1, 'normal warning')
                sharder.periodic_warning(broker1, 'periodic warning 1')
                sharder.periodic_warning(broker1, 'periodic warning 1a')
                sharder.periodic_warning(broker2, 'periodic warning 2')
                sharder.warning(broker1, 'normal warning')
                for i in range(10):
                    sharder.periodic_warning(broker1, 'periodic warning 1')
                    sharder.periodic_warning(broker1, 'periodic warning 1a')
                    sharder.periodic_warning(broker2, 'periodic warning 2')
                    now[0] += 1
        lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(11, len(lines))
        self.assertEqual(
            ['periodic warning 1, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 1a, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 2, path: a/c2, db: %s' % broker2.db_file,
             'normal warning, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 1, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 1a, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 2, path: a/c2, db: %s' % broker2.db_file,
             'normal warning, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 1, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 1a, path: a/c1, db: %s' % broker1.db_file,
             'periodic warning 2, path: a/c2, db: %s' % broker2.db_file],
            lines)

    def _assert_stats(self, expected, sharder, category):
        # assertEqual doesn't work with a stats defaultdict so copy to a dict
        # before comparing
        stats = sharder.stats['sharding'][category]
        actual = {}
        for k, v in expected.items():
            actual[k] = stats[k]
        self.assertEqual(expected, actual)
        return stats

    def _assert_recon_stats(self, expected, sharder, category):
        with open(sharder.rcache, 'rb') as fd:
            recon = json.load(fd)
        stats = recon['sharding_stats']['sharding'].get(category)
        self.assertEqual(expected, stats)

    def test_increment_stats(self):
        with self._mock_sharder() as sharder:
            sharder._increment_stat('visited', 'success')
            sharder._increment_stat('visited', 'success')
            sharder._increment_stat('visited', 'failure')
            sharder._increment_stat('visited', 'completed')
            sharder._increment_stat('cleaved', 'success')
        expected = {'success': 2,
                    'failure': 1,
                    'completed': 1}
        self._assert_stats(expected, sharder, 'visited')
        self._assert_stats({'success': 1}, sharder, 'cleaved')

    def test_increment_stats_with_statsd(self):
        with self._mock_sharder() as sharder:
            sharder._increment_stat('visited', 'success', statsd=True)
            sharder._increment_stat('visited', 'success', statsd=True)
            sharder._increment_stat('visited', 'failure', statsd=True)
            sharder._increment_stat('visited', 'failure', statsd=False)
            sharder._increment_stat('visited', 'completed')
        expected = {'success': 2,
                    'failure': 2,
                    'completed': 1}
        self._assert_stats(expected, sharder, 'visited')
        counts = sharder.logger.statsd_client.get_stats_counts()
        self.assertEqual(2, counts.get('visited_success'))
        self.assertEqual(1, counts.get('visited_failure'))
        self.assertIsNone(counts.get('visited_completed'))

    def test_update_stat(self):
        with self._mock_sharder() as sharder:
            sharder._update_stat('scanned', 'found', step=4)
        self._assert_stats({'found': 4}, sharder, 'scanned')
        with self._mock_sharder() as sharder:
            sharder._update_stat('scanned', 'found', step=4)
            sharder._update_stat('misplaced', 'placed', step=456, statsd=True)
        self._assert_stats({'found': 4}, sharder, 'scanned')
        self._assert_stats({'placed': 456}, sharder, 'misplaced')
        self.assertEqual({'misplaced_placed': 456},
                         sharder.logger.statsd_client.get_stats_counts())

    def test_run_forever(self):
        conf = {'recon_cache_path': self.tempdir,
                'devices': self.tempdir}
        with self._mock_sharder(conf) as sharder:
            sharder._check_node = lambda node: os.path.join(
                sharder.conf['devices'], node['device'])
            sharder.logger.clear()
            brokers = []
            for container in ('c1', 'c2'):
                broker = self._make_broker(
                    container=container, hash_=container + 'hash',
                    device=sharder.ring.devs[0]['device'], part=0)
                broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                        ('true', next(self.ts_iter).internal)})
                brokers.append(broker)

            fake_stats = {
                'scanned': {'attempted': 1, 'success': 1, 'failure': 0,
                            'found': 2, 'min_time': 99, 'max_time': 123},
                'created': {'attempted': 1, 'success': 1, 'failure': 1},
                'cleaved': {'attempted': 1, 'success': 1, 'failure': 0,
                            'min_time': 0.01, 'max_time': 1.3},
                'misplaced': {'attempted': 1, 'success': 1, 'failure': 0,
                              'found': 1, 'placed': 1, 'unplaced': 0},
                'audit_root': {'attempted': 5, 'success': 4, 'failure': 1,
                               'num_overlap': 0, "has_overlap": 0},
                'audit_shard': {'attempted': 2, 'success': 2, 'failure': 0},
            }
            # NB these are time increments not absolute times...
            fake_periods = [1, 2, 3, 3600, 4, 15, 15, 0]
            fake_periods_iter = iter(fake_periods)
            recon_data = []
            fake_process_broker_calls = []

            def mock_dump_recon_cache(data, *args):
                recon_data.append(deepcopy(data))

            with mock.patch('swift.container.sharder.time.time') as fake_time:
                def fake_process_broker(broker, *args, **kwargs):
                    # increment time and inject some fake stats
                    fake_process_broker_calls.append((broker, args, kwargs))
                    try:
                        fake_time.return_value += next(fake_periods_iter)
                    except StopIteration:
                        # bail out
                        fake_time.side_effect = Exception('Test over')
                    sharder.stats['sharding'].update(fake_stats)

                with mock.patch(
                        'swift.container.sharder.time.sleep') as mock_sleep:
                    with mock.patch(
                            'swift.container.sharder.is_sharding_candidate',
                            return_value=True):
                        with mock.patch(
                                'swift.container.sharder.dump_recon_cache',
                                mock_dump_recon_cache):
                            fake_time.return_value = next(fake_periods_iter)
                            sharder._is_sharding_candidate = lambda x: True
                            sharder._process_broker = fake_process_broker
                            with self.assertRaises(Exception) as cm:
                                sharder.run_forever()

            self.assertEqual('Test over', str(cm.exception))
            # four cycles are started, two brokers visited per cycle, but
            # fourth never completes
            self.assertEqual(8, len(fake_process_broker_calls))
            # expect initial random sleep then one sleep between first and
            # second pass
            self.assertEqual(2, mock_sleep.call_count)
            self.assertLessEqual(mock_sleep.call_args_list[0][0][0], 30)
            self.assertLessEqual(mock_sleep.call_args_list[1][0][0],
                                 30 - fake_periods[0])

            lines = sharder.logger.get_lines_for_level('info')
            categories = ('visited', 'scanned', 'created', 'cleaved',
                          'misplaced', 'audit_root', 'audit_shard')

            def check_categories(start_time):
                for category in categories:
                    line = lines.pop(0)
                    self.assertIn('Since %s' % time.ctime(start_time), line)
                    self.assertIn(category, line)
                    for k, v in fake_stats.get(category, {}).items():
                        self.assertIn('%s:%s' % (k, v), line)

            def check_logs(cycle_time, start_time,
                           expect_periodic_stats=False):
                self.assertIn('Container sharder cycle starting', lines.pop(0))
                check_categories(start_time)
                if expect_periodic_stats:
                    check_categories(start_time)
                self.assertIn('Container sharder cycle completed: %.02fs' %
                              cycle_time, lines.pop(0))

            check_logs(sum(fake_periods[1:3]), fake_periods[0])
            check_logs(sum(fake_periods[3:5]), sum(fake_periods[:3]),
                       expect_periodic_stats=True)
            check_logs(sum(fake_periods[5:7]), sum(fake_periods[:5]))
            # final cycle start but then exception pops to terminate test
            self.assertIn('Container sharder cycle starting', lines.pop(0))
            self.assertFalse(lines)
            lines = sharder.logger.get_lines_for_level('error')
            self.assertIn(
                'Unhandled exception while dumping progress', lines[0])
            self.assertIn('path: a/c', lines[0])  # match one of the brokers
            self.assertIn('Test over', lines[0])

            def check_recon(data, time, last, expected_stats):
                self.assertEqual(time, data['sharding_time'])
                self.assertEqual(last, data['sharding_last'])
                self.assertEqual(
                    expected_stats, dict(data['sharding_stats']['sharding']))

            def stats_for_candidate(broker):
                return {'object_count': 0,
                        'account': broker.account,
                        'meta_timestamp': mock.ANY,
                        'container': broker.container,
                        'file_size': os.stat(broker.db_file).st_size,
                        'path': broker.db_file,
                        'root': broker.path,
                        'node_index': 0}

            self.assertEqual(4, len(recon_data))
            # stats report at end of first cycle
            fake_stats.update({'visited': {'attempted': 2, 'skipped': 0,
                                           'success': 2, 'failure': 0,
                                           'completed': 0}})
            fake_stats.update({
                'sharding_candidates': {
                    'found': 2,
                    'top': [stats_for_candidate(call[0])
                            for call in fake_process_broker_calls[:2]]
                }
            })
            fake_stats.update({
                'shrinking_candidates': {
                    'found': 0,
                    'top': []
                }
            })
            check_recon(recon_data[0], sum(fake_periods[1:3]),
                        sum(fake_periods[:3]), fake_stats)
            # periodic stats report after first broker has been visited during
            # second cycle - one candidate identified so far this cycle
            fake_stats.update({'visited': {'attempted': 1, 'skipped': 0,
                                           'success': 1, 'failure': 0,
                                           'completed': 0}})
            fake_stats.update({
                'sharding_candidates': {
                    'found': 1,
                    'top': [stats_for_candidate(call[0])
                            for call in fake_process_broker_calls[2:3]]
                }
            })
            check_recon(recon_data[1], fake_periods[3],
                        sum(fake_periods[:4]), fake_stats)
            # stats report at end of second cycle - both candidates reported
            fake_stats.update({'visited': {'attempted': 2, 'skipped': 0,
                                           'success': 2, 'failure': 0,
                                           'completed': 0}})
            fake_stats.update({
                'sharding_candidates': {
                    'found': 2,
                    'top': [stats_for_candidate(call[0])
                            for call in fake_process_broker_calls[2:4]]
                }
            })
            check_recon(recon_data[2], sum(fake_periods[3:5]),
                        sum(fake_periods[:5]), fake_stats)
            # stats report at end of third cycle
            fake_stats.update({'visited': {'attempted': 2, 'skipped': 0,
                                           'success': 2, 'failure': 0,
                                           'completed': 0}})
            fake_stats.update({
                'sharding_candidates': {
                    'found': 2,
                    'top': [stats_for_candidate(call[0])
                            for call in fake_process_broker_calls[4:6]]
                }
            })
            check_recon(recon_data[3], sum(fake_periods[5:7]),
                        sum(fake_periods[:7]), fake_stats)

    def test_one_shard_cycle(self):
        conf = {'recon_cache_path': self.tempdir,
                'devices': self.tempdir,
                'shard_container_threshold': 9}

        def fake_ismount(path):
            # unmounted_dev is defined from .get_more_nodes() below
            unmounted_path = os.path.join(conf['devices'],
                                          unmounted_dev['device'])
            if path == unmounted_path:
                return False
            else:
                return True

        with self._mock_sharder(conf) as sharder, \
                mock.patch('swift.common.utils.ismount', fake_ismount), \
                mock.patch('swift.container.sharder.is_local_device',
                           return_value=True):
            sharder.reported = time.time()
            brokers = []
            device_ids = set(d['id'] for d in sharder.ring.devs)

            sharder.ring.max_more_nodes = 1
            unmounted_dev = next(sharder.ring.get_more_nodes(1))
            unmounted_dev['device'] = 'xxxx'
            sharder.ring.add_node(unmounted_dev)
            for device_id in device_ids:
                brokers.append(self._make_broker(
                    container='c%s' % device_id, hash_='c%shash' % device_id,
                    device=sharder.ring.devs[device_id]['device'], part=0))
            # enable a/c2 and a/c3 for sharding
            for broker in brokers[1:]:
                broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                        ('true', next(self.ts_iter).internal)})
            # make a/c2 a candidate for sharding
            for i in range(10):
                brokers[1].put_object('o%s' % i, next(self.ts_iter).internal,
                                      0, 'text/plain', 'etag', 0)

            # check only sharding enabled containers are processed
            with mock.patch('eventlet.sleep'), mock.patch.object(
                    sharder, '_process_broker'
            ) as mock_process_broker:
                sharder._local_device_ids = {'stale_node_id': {}}
                sharder._one_shard_cycle(Everything(), Everything())

            lines = sharder.logger.get_lines_for_level('warning')
            expected = 'Skipping %s as it is not mounted' % \
                unmounted_dev['device']
            self.assertIn(expected, lines[0])
            self.assertEqual(device_ids, set(sharder._local_device_ids.keys()))
            self.assertEqual(2, mock_process_broker.call_count)
            processed_paths = [call[0][0].path
                               for call in mock_process_broker.call_args_list]
            self.assertEqual({'a/c1', 'a/c2'}, set(processed_paths))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            expected_stats = {'attempted': 2, 'success': 2, 'failure': 0,
                              'skipped': 1, 'completed': 0}
            self._assert_recon_stats(expected_stats, sharder, 'visited')
            expected_candidate_stats = {
                'found': 1,
                'top': [{'object_count': 10, 'account': 'a', 'container': 'c1',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[1].db_file).st_size,
                         'path': brokers[1].db_file, 'root': 'a/c1',
                         'node_index': 1}]}
            self._assert_recon_stats(
                expected_candidate_stats, sharder, 'sharding_candidates')
            self._assert_recon_stats(None, sharder, 'sharding_progress')

            # enable and progress container a/c1 by giving it shard ranges
            now = next(self.ts_iter)
            brokers[0].merge_shard_ranges(
                [ShardRange('a/c0', now, '', '', state=ShardRange.SHARDING),
                 ShardRange('.s_a/1', now, '', 'b', state=ShardRange.ACTIVE),
                 ShardRange('.s_a/2', now, 'b', 'c', state=ShardRange.CLEAVED),
                 ShardRange('.s_a/3', now, 'c', 'd', state=ShardRange.CREATED),
                 ShardRange('.s_a/4', now, 'd', 'e', state=ShardRange.CREATED),
                 ShardRange('.s_a/5', now, 'e', '', state=ShardRange.FOUND)])
            brokers[1].merge_shard_ranges(
                [ShardRange('a/c1', now, '', '', state=ShardRange.SHARDING),
                 ShardRange('.s_a/6', now, '', 'b', state=ShardRange.ACTIVE),
                 ShardRange('.s_a/7', now, 'b', 'c', state=ShardRange.ACTIVE),
                 ShardRange('.s_a/8', now, 'c', 'd', state=ShardRange.CLEAVED),
                 ShardRange('.s_a/9', now, 'd', 'e', state=ShardRange.CREATED),
                 ShardRange('.s_a/0', now, 'e', '', state=ShardRange.CREATED)])
            for i in range(11):
                brokers[2].put_object('o%s' % i, next(self.ts_iter).internal,
                                      0, 'text/plain', 'etag', 0)

            def mock_processing(broker, node, part):
                if broker.path == 'a/c1':
                    raise Exception('kapow!')
                elif broker.path not in ('a/c0', 'a/c2'):
                    raise BaseException("I don't know how to handle a broker "
                                        "for %s" % broker.path)

            # check exceptions are handled
            sharder.logger.clear()
            with mock.patch('eventlet.sleep'), mock.patch.object(
                    sharder, '_process_broker', side_effect=mock_processing
            ) as mock_process_broker:
                sharder._local_device_ids = {'stale_node_id': {}}
                sharder._one_shard_cycle(Everything(), Everything())

            lines = sharder.logger.get_lines_for_level('warning')
            expected = 'Skipping %s as it is not mounted' % \
                unmounted_dev['device']
            self.assertIn(expected, lines[0])
            self.assertEqual(device_ids, set(sharder._local_device_ids.keys()))
            self.assertEqual(3, mock_process_broker.call_count)
            processed_paths = [call[0][0].path
                               for call in mock_process_broker.call_args_list]
            self.assertEqual({'a/c0', 'a/c1', 'a/c2'}, set(processed_paths))
            lines = sharder.logger.get_lines_for_level('error')
            self.assertIn('Unhandled exception while processing', lines[0])
            self.assertIn('path: a/c', lines[0])  # match one of the brokers
            self.assertFalse(lines[1:])
            sharder.logger.clear()
            expected_stats = {'attempted': 3, 'success': 2, 'failure': 1,
                              'skipped': 0, 'completed': 0}
            self._assert_recon_stats(expected_stats, sharder, 'visited')
            expected_candidate_stats = {
                'found': 1,
                'top': [{'object_count': 11, 'account': 'a', 'container': 'c2',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[1].db_file).st_size,
                         'path': brokers[2].db_file, 'root': 'a/c2',
                         'node_index': 2}]}
            self._assert_recon_stats(
                expected_candidate_stats, sharder, 'sharding_candidates')
            expected_in_progress_stats = {
                'all': [{'object_count': 0, 'account': 'a', 'container': 'c0',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[0].db_file).st_size,
                         'path': brokers[0].db_file, 'root': 'a/c0',
                         'node_index': 0,
                         'found': 1, 'created': 2, 'cleaved': 1, 'active': 1,
                         'state': 'sharding', 'db_state': 'unsharded',
                         'error': None},
                        {'object_count': 10, 'account': 'a', 'container': 'c1',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[1].db_file).st_size,
                         'path': brokers[1].db_file, 'root': 'a/c1',
                         'node_index': 1,
                         'found': 0, 'created': 2, 'cleaved': 1, 'active': 2,
                         'state': 'sharding', 'db_state': 'unsharded',
                         'error': 'kapow!'}]}
            self._assert_stats(
                expected_in_progress_stats, sharder, 'sharding_in_progress')

            # check that candidates and in progress stats don't stick in recon
            own_shard_range = brokers[0].get_own_shard_range()
            own_shard_range.state = ShardRange.ACTIVE
            brokers[0].merge_shard_ranges([own_shard_range])
            for i in range(10):
                brokers[1].delete_object(
                    'o%s' % i, next(self.ts_iter).internal)
            with mock.patch('eventlet.sleep'), mock.patch.object(
                    sharder, '_process_broker'
            ) as mock_process_broker:
                sharder._local_device_ids = {999: {}}
                sharder._one_shard_cycle(Everything(), Everything())

            self.assertEqual(device_ids, set(sharder._local_device_ids.keys()))
            self.assertEqual(3, mock_process_broker.call_count)
            processed_paths = [call[0][0].path
                               for call in mock_process_broker.call_args_list]
            self.assertEqual({'a/c0', 'a/c1', 'a/c2'}, set(processed_paths))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            expected_stats = {'attempted': 3, 'success': 3, 'failure': 0,
                              'skipped': 0, 'completed': 0}
            self._assert_recon_stats(expected_stats, sharder, 'visited')
            self._assert_recon_stats(
                expected_candidate_stats, sharder, 'sharding_candidates')
            self._assert_recon_stats(None, sharder, 'sharding_progress')

            # let's progress broker 1 (broker[0])
            brokers[0].enable_sharding(next(self.ts_iter))
            brokers[0].set_sharding_state()
            shard_ranges = brokers[0].get_shard_ranges()
            for sr in shard_ranges[:-1]:
                sr.update_state(ShardRange.CLEAVED)
            brokers[0].merge_shard_ranges(shard_ranges)

            with mock.patch('eventlet.sleep'), mock.patch.object(
                    sharder, '_process_broker'
            ) as mock_process_broker:
                sharder._local_device_ids = {999: {}}
                sharder._one_shard_cycle(Everything(), Everything())

            expected_in_progress_stats = {
                'all': [{'object_count': 0, 'account': 'a', 'container': 'c0',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[0].db_file).st_size,
                         'path': brokers[0].db_file, 'root': 'a/c0',
                         'node_index': 0,
                         'found': 1, 'created': 0, 'cleaved': 3, 'active': 1,
                         'state': 'sharding', 'db_state': 'sharding',
                         'error': None},
                        {'object_count': 0, 'account': 'a', 'container': 'c1',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[1].db_file).st_size,
                         'path': brokers[1].db_file, 'root': 'a/c1',
                         'node_index': 1,
                         'found': 0, 'created': 2, 'cleaved': 1, 'active': 2,
                         'state': 'sharding', 'db_state': 'unsharded',
                         'error': None}]}
            self._assert_stats(
                expected_in_progress_stats, sharder, 'sharding_in_progress')

            # Now complete sharding broker 1.
            shard_ranges[-1].update_state(ShardRange.CLEAVED)
            own_sr = brokers[0].get_own_shard_range()
            own_sr.update_state(ShardRange.SHARDED)
            brokers[0].merge_shard_ranges(shard_ranges + [own_sr])
            # make and complete a cleave context, this is used for the
            # recon_sharded_timeout timer.
            cxt = CleavingContext.load(brokers[0])
            cxt.misplaced_done = cxt.cleaving_done = True
            ts_now = next(self.ts_iter)
            with mock_timestamp_now(ts_now):
                cxt.store(brokers[0])
            self.assertTrue(brokers[0].set_sharded_state())

            with mock.patch('eventlet.sleep'), \
                    mock.patch.object(sharder, '_process_broker') \
                    as mock_process_broker, mock_timestamp_now(ts_now):
                sharder._local_device_ids = {999: {}}
                sharder._one_shard_cycle(Everything(), Everything())

            expected_in_progress_stats = {
                'all': [{'object_count': 0, 'account': 'a', 'container': 'c0',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[0].db_file).st_size,
                         'path': brokers[0].db_file, 'root': 'a/c0',
                         'node_index': 0,
                         'found': 0, 'created': 0, 'cleaved': 4, 'active': 1,
                         'state': 'sharded', 'db_state': 'sharded',
                         'error': None},
                        {'object_count': 0, 'account': 'a', 'container': 'c1',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[1].db_file).st_size,
                         'path': brokers[1].db_file, 'root': 'a/c1',
                         'node_index': 1,
                         'found': 0, 'created': 2, 'cleaved': 1, 'active': 2,
                         'state': 'sharding', 'db_state': 'unsharded',
                         'error': None}]}
            self._assert_stats(
                expected_in_progress_stats, sharder, 'sharding_in_progress')

            # one more cycle at recon_sharded_timeout seconds into the
            # future to check that the completed broker is still reported
            ts_now = Timestamp(ts_now.timestamp +
                               sharder.recon_sharded_timeout)
            with mock.patch('eventlet.sleep'), \
                    mock.patch.object(sharder, '_process_broker') \
                    as mock_process_broker, mock_timestamp_now(ts_now):
                sharder._local_device_ids = {999: {}}
                sharder._one_shard_cycle(Everything(), Everything())
            self._assert_stats(
                expected_in_progress_stats, sharder, 'sharding_in_progress')

            # when we move recon_sharded_timeout + 1 seconds into the future,
            # broker 1 will be removed from the progress report
            ts_now = Timestamp(ts_now.timestamp +
                               sharder.recon_sharded_timeout + 1)
            with mock.patch('eventlet.sleep'), \
                    mock.patch.object(sharder, '_process_broker') \
                    as mock_process_broker, mock_timestamp_now(ts_now):
                sharder._local_device_ids = {999: {}}
                sharder._one_shard_cycle(Everything(), Everything())

            expected_in_progress_stats = {
                'all': [{'object_count': 0, 'account': 'a', 'container': 'c1',
                         'meta_timestamp': mock.ANY,
                         'file_size': os.stat(brokers[1].db_file).st_size,
                         'path': brokers[1].db_file, 'root': 'a/c1',
                         'node_index': 1,
                         'found': 0, 'created': 2, 'cleaved': 1, 'active': 2,
                         'state': 'sharding', 'db_state': 'unsharded',
                         'error': None}]}
            self._assert_stats(
                expected_in_progress_stats, sharder, 'sharding_in_progress')

    def test_one_shard_cycle_no_containers(self):
        conf = {'recon_cache_path': self.tempdir,
                'devices': self.tempdir,
                'mount_check': False}

        with self._mock_sharder(conf) as sharder:
            for dev in sharder.ring.devs:
                os.mkdir(os.path.join(self.tempdir, dev['device']))
            with mock.patch('swift.container.sharder.is_local_device',
                            return_value=True):
                sharder._one_shard_cycle(Everything(), Everything())
        self.assertEqual([], sharder.logger.get_lines_for_level('warning'))
        self.assertIn('Found no containers directories',
                      sharder.logger.get_lines_for_level('info'))
        with self._mock_sharder(conf) as sharder:
            os.mkdir(os.path.join(self.tempdir, dev['device'], 'containers'))
            with mock.patch('swift.container.sharder.is_local_device',
                            return_value=True):
                sharder._one_shard_cycle(Everything(), Everything())
        self.assertEqual([], sharder.logger.get_lines_for_level('warning'))
        self.assertNotIn('Found no containers directories',
                         sharder.logger.get_lines_for_level('info'))

    def test_ratelimited_roundrobin(self):
        n_databases = 100

        def stub_iter(dirs):
            for i in range(n_databases):
                yield i, '/srv/node/sda/path/to/container.db', {}

        now = time.time()
        clock = {
            'sleeps': [],
            'now': now,
        }

        def fake_sleep(t):
            clock['sleeps'].append(t)
            clock['now'] += t

        def fake_time():
            return clock['now']

        with self._mock_sharder({'databases_per_second': 1}) as sharder, \
                mock.patch('swift.common.db_replicator.roundrobin_datadirs',
                           stub_iter), \
                mock.patch('time.time', fake_time), \
                mock.patch('eventlet.sleep', fake_sleep):
            list(sharder.roundrobin_datadirs(None))
        # 100 db at 1/s should take ~100s
        run_time = sum(clock['sleeps'])
        self.assertTrue(97 <= run_time < 100, 'took %s' % run_time)

        n_databases = 1000
        now = time.time()
        clock = {
            'sleeps': [],
            'now': now,
        }

        with self._mock_sharder({'databases_per_second': 50}) as sharder, \
                mock.patch('swift.common.db_replicator.roundrobin_datadirs',
                           stub_iter), \
                mock.patch('time.time', fake_time), \
                mock.patch('eventlet.sleep', fake_sleep):
            list(sharder.roundrobin_datadirs(None))
        # 1000 db at 50/s
        run_time = sum(clock['sleeps'])
        self.assertTrue(18 <= run_time < 20, 'took %s' % run_time)

    @contextmanager
    def _mock_sharder(self, conf=None, replicas=3):
        self.logger.clear()
        conf = conf or {}
        conf['devices'] = self.tempdir
        fake_ring = FakeRing(replicas=replicas, separate_replication=True)
        with mock.patch(
                'swift.container.sharder.internal_client.InternalClient'):
            with mock.patch(
                    'swift.common.db_replicator.ring.Ring',
                    return_value=fake_ring):
                sharder = ContainerSharder(conf, logger=self.logger)
                sharder._local_device_ids = {dev['id']: dev
                                             for dev in fake_ring.devs}
                sharder._replicate_object = mock.MagicMock(
                    return_value=(True, [True] * sharder.ring.replica_count))
                yield sharder

    def _get_raw_object_records(self, broker):
        # use list_objects_iter with no-op transform_func to get back actual
        # un-transformed rows with encoded timestamps
        return [list(obj) for obj in broker.list_objects_iter(
            10, '', '', '', '', include_deleted=None, all_policies=True,
            transform_func=lambda record: record)]

    def _check_objects(self, expected_objs, shard_dbs):
        shard_dbs = shard_dbs if isinstance(shard_dbs, list) else [shard_dbs]
        shard_objs = []
        for shard_db in shard_dbs:
            shard_broker = ContainerBroker(shard_db)
            shard_objs.extend(self._get_raw_object_records(shard_broker))
        expected_objs = [list(obj) for obj in expected_objs]
        self.assertEqual(expected_objs, shard_objs)

    def _check_shard_range(self, expected, actual):
        expected_dict = dict(expected)
        actual_dict = dict(actual)
        self.assertGreater(actual_dict.pop('meta_timestamp'),
                           expected_dict.pop('meta_timestamp'))
        self.assertEqual(expected_dict, actual_dict)

    def test_check_node(self):
        node = {
            'replication_ip': '127.0.0.1',
            'replication_port': 5000,
            'device': 'd100',
        }
        with self._mock_sharder() as sharder:
            sharder.mount_check = True
            sharder.ips = ['127.0.0.1']
            sharder.port = 5000

            # normal behavior
            with mock.patch(
                    'swift.common.utils.ismount',
                    lambda *args: True):
                r = sharder._check_node(node)
            expected = os.path.join(sharder.conf['devices'], node['device'])
            self.assertEqual(r, expected)

            # test with an unmounted drive
            with mock.patch(
                    'swift.common.utils.ismount',
                    lambda *args: False):
                r = sharder._check_node(node)
            self.assertEqual(r, False)
            lines = sharder.logger.get_lines_for_level('warning')
            expected = 'Skipping %s as it is not mounted' % node['device']
            self.assertIn(expected, lines[0])

    def test_fetch_shard_ranges_unexpected_response(self):
        broker = self._make_broker()
        exc = internal_client.UnexpectedResponse(
            'Unexpected response: 404', None)
        with self._mock_sharder() as sharder:
            sharder.int_client.make_request.side_effect = exc
            self.assertIsNone(sharder._fetch_shard_ranges(broker))
        lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn('Unexpected response: 404', lines[0])
        self.assertFalse(lines[1:])

    def test_fetch_shard_ranges_bad_record_type(self):
        def do_test(mock_resp_headers):
            with self._mock_sharder() as sharder:
                mock_make_request = mock.MagicMock(
                    return_value=mock.MagicMock(headers=mock_resp_headers))
                sharder.int_client.make_request = mock_make_request
                self.assertIsNone(sharder._fetch_shard_ranges(broker))
            lines = sharder.logger.get_lines_for_level('error')
            self.assertIn('unexpected record type', lines[0])
            self.assertFalse(lines[1:])

        broker = self._make_broker()
        do_test({})
        do_test({'x-backend-record-type': 'object'})
        do_test({'x-backend-record-type': 'disco'})

    def test_fetch_shard_ranges_bad_data(self):
        def do_test(mock_resp_body):
            mock_resp_headers = {'x-backend-record-type': 'shard'}
            with self._mock_sharder() as sharder:
                mock_make_request = mock.MagicMock(
                    return_value=mock.MagicMock(headers=mock_resp_headers,
                                                body=mock_resp_body))
                sharder.int_client.make_request = mock_make_request
                self.assertIsNone(sharder._fetch_shard_ranges(broker))
            lines = sharder.logger.get_lines_for_level('error')
            self.assertIn('invalid data', lines[0])
            self.assertFalse(lines[1:])

        broker = self._make_broker()
        do_test({})
        do_test('')
        do_test(json.dumps({}))
        do_test(json.dumps([{'account': 'a', 'container': 'c'}]))
        do_test(json.dumps([dict(Namespace('a/c', 'l', 'u'))]))
        sr_dict = dict(ShardRange('a/c', next(self.ts_iter), 'l', 'u'))
        sr_dict.pop('object_count')
        do_test(json.dumps([sr_dict]))

    def test_fetch_shard_ranges_ok(self):
        def do_test(mock_resp_body, params):
            mock_resp_headers = {'x-backend-record-type': 'shard'}
            with self._mock_sharder() as sharder:
                mock_make_request = mock.MagicMock(
                    return_value=mock.MagicMock(headers=mock_resp_headers,
                                                body=mock_resp_body))
                sharder.int_client.make_request = mock_make_request
                mock_make_path = mock.MagicMock(return_value='/v1/a/c')
                sharder.int_client.make_path = mock_make_path
                actual = sharder._fetch_shard_ranges(broker, params=params)
            sharder.int_client.make_path.assert_called_once_with('a', 'c')
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            return actual, mock_make_request

        expected_headers = {'X-Backend-Record-Type': 'shard',
                            'X-Backend-Include-Deleted': 'False',
                            'X-Backend-Override-Deleted': 'true',
                            'X-Backend-Record-Shard-Format': 'full'}
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges((('', 'm'), ('m', '')))

        params = {'format': 'json'}
        actual, mock_call = do_test(json.dumps([dict(shard_ranges[0])]),
                                    params={})
        mock_call.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)
        self._assert_shard_ranges_equal([shard_ranges[0]], actual)

        params = {'format': 'json', 'includes': 'thing'}
        actual, mock_call = do_test(
            json.dumps([dict(sr) for sr in shard_ranges]), params=params)
        self._assert_shard_ranges_equal(shard_ranges, actual)
        mock_call.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)

        params = {'format': 'json',
                  'end_marker': 'there', 'marker': 'here'}
        actual, mock_call = do_test(json.dumps([]), params=params)
        self._assert_shard_ranges_equal([], actual)
        mock_call.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)

    def test_yield_objects(self):
        broker = self._make_broker()
        objects = [
            ('o%02d' % i, self.ts_encoded(), 10, 'text/plain', 'etag_a',
             i % 2, 0) for i in range(30)]
        for obj in objects:
            broker.put_object(*obj)

        src_range = ShardRange('dont/care', Timestamp.now())
        with self._mock_sharder(conf={}) as sharder:
            batches = [b for b, _ in
                       sharder.yield_objects(broker, src_range)]
        self.assertEqual([15, 15], [len(b) for b in batches])
        self.assertEqual([[0] * 15, [1] * 15],
                         [[o['deleted'] for o in b] for b in batches])

        # custom batch size
        with self._mock_sharder(conf={}) as sharder:
            batches = [b for b, _ in
                       sharder.yield_objects(broker, src_range, batch_size=10)]
        self.assertEqual([10, 5, 10, 5], [len(b) for b in batches])
        self.assertEqual([[0] * 10, [0] * 5, [1] * 10, [1] * 5],
                         [[o['deleted'] for o in b] for b in batches])

        # restricted source range
        src_range = ShardRange('dont/care', Timestamp.now(),
                               lower='o10', upper='o20')
        with self._mock_sharder(conf={}) as sharder:
            batches = [b for b, _ in
                       sharder.yield_objects(broker, src_range)]
        self.assertEqual([5, 5], [len(b) for b in batches])
        self.assertEqual([[0] * 5, [1] * 5],
                         [[o['deleted'] for o in b] for b in batches])

        # null source range
        src_range = ShardRange('dont/care', Timestamp.now(),
                               lower=ShardRange.MAX)
        with self._mock_sharder(conf={}) as sharder:
            batches = [b for b, _ in
                       sharder.yield_objects(broker, src_range)]
        self.assertEqual([], batches)
        src_range = ShardRange('dont/care', Timestamp.now(),
                               upper=ShardRange.MIN)
        with self._mock_sharder(conf={}) as sharder:
            batches = [b for b, _ in
                       sharder.yield_objects(broker, src_range)]
        self.assertEqual([], batches)

    def test_yield_objects_to_shard_range_no_objects(self):
        # verify that dest_shard_ranges func is not called if the source
        # broker has no objects
        broker = self._make_broker()
        dest_shard_ranges = mock.MagicMock()
        src_range = ShardRange('dont/care', Timestamp.now())
        with self._mock_sharder(conf={}) as sharder:
            batches = [b for b, _ in
                       sharder.yield_objects_to_shard_range(
                           broker, src_range, dest_shard_ranges)]
        self.assertEqual([], batches)
        dest_shard_ranges.assert_not_called()

    def test_yield_objects_to_shard_range(self):
        broker = self._make_broker()
        objects = [
            ('o%02d' % i, self.ts_encoded(), 10, 'text/plain', 'etag_a',
             i % 2, 0) for i in range(30)]
        for obj in objects:
            broker.put_object(*obj)
        orig_info = broker.get_info()
        # yield_objects annotates the info dict...
        orig_info['max_row'] = 30
        dest_ranges = [
            ShardRange('shard/0', Timestamp.now(), upper='o09'),
            ShardRange('shard/1', Timestamp.now(), lower='o09', upper='o19'),
            ShardRange('shard/2', Timestamp.now(), lower='o19'),
        ]

        # complete overlap of src and dest, multiple batches per dest shard
        # range per deleted/not deleted
        src_range = ShardRange('dont/care', Timestamp.now())
        dest_shard_ranges = mock.MagicMock(return_value=dest_ranges)
        with self._mock_sharder(conf={'cleave_row_batch_size': 4}) as sharder:
            yielded = [y for y in
                       sharder.yield_objects_to_shard_range(
                           broker, src_range, dest_shard_ranges)]
        self.assertEqual([dest_ranges[0], dest_ranges[0],
                          dest_ranges[0], dest_ranges[0],
                          dest_ranges[1], dest_ranges[1],
                          dest_ranges[1], dest_ranges[1],
                          dest_ranges[2], dest_ranges[2],
                          dest_ranges[2], dest_ranges[2]],
                         [dest for _, dest, _ in yielded])
        self.assertEqual([[o[0] for o in objects[0:8:2]],
                          [o[0] for o in objects[8:10:2]],
                          [o[0] for o in objects[1:8:2]],
                          [o[0] for o in objects[9:10:2]],
                          [o[0] for o in objects[10:18:2]],
                          [o[0] for o in objects[18:20:2]],
                          [o[0] for o in objects[11:18:2]],
                          [o[0] for o in objects[19:20:2]],
                          [o[0] for o in objects[20:28:2]],
                          [o[0] for o in objects[28:30:2]],
                          [o[0] for o in objects[21:28:2]],
                          [o[0] for o in objects[29:30:2]]],
                         [[o['name'] for o in objs] for objs, _, _ in yielded])
        self.assertEqual([orig_info] * 12, [info for _, _, info in yielded])

        # src narrower than dest
        src_range = ShardRange('dont/care', Timestamp.now(),
                               lower='o15', upper='o25')
        dest_shard_ranges = mock.MagicMock(return_value=dest_ranges)
        with self._mock_sharder(conf={}) as sharder:
            yielded = [y for y in
                       sharder.yield_objects_to_shard_range(
                           broker, src_range, dest_shard_ranges)]
        self.assertEqual([dest_ranges[1], dest_ranges[1],
                          dest_ranges[2], dest_ranges[2]],
                         [dest for _, dest, _ in yielded])
        self.assertEqual([[o[0] for o in objects[16:20:2]],
                          [o[0] for o in objects[17:20:2]],
                          [o[0] for o in objects[20:26:2]],
                          [o[0] for o in objects[21:26:2]]],
                         [[o['name'] for o in objs] for objs, _, _ in yielded])
        self.assertEqual([orig_info] * 4, [info for _, _, info in yielded])

        # src much narrower than dest
        src_range = ShardRange('dont/care', Timestamp.now(),
                               lower='o15', upper='o18')
        dest_shard_ranges = mock.MagicMock(return_value=dest_ranges)
        with self._mock_sharder(conf={}) as sharder:
            yielded = [y for y in
                       sharder.yield_objects_to_shard_range(
                           broker, src_range, dest_shard_ranges)]
        self.assertEqual([dest_ranges[1], dest_ranges[1]],
                         [dest for _, dest, _ in yielded])
        self.assertEqual([[o[0] for o in objects[16:19:2]],
                          [o[0] for o in objects[17:19:2]]],
                         [[o['name'] for o in objs] for objs, _, _ in yielded])
        self.assertEqual([orig_info] * 2, [info for _, _, info in yielded])

        # dest narrower than src
        src_range = ShardRange('dont/care', Timestamp.now(),
                               lower='o05', upper='o25')
        dest_shard_ranges = mock.MagicMock(return_value=dest_ranges[1:])
        with self._mock_sharder(conf={}) as sharder:
            yielded = [y for y in
                       sharder.yield_objects_to_shard_range(
                           broker, src_range, dest_shard_ranges)]
        self.assertEqual([None, None,
                          dest_ranges[1], dest_ranges[1],
                          dest_ranges[2], dest_ranges[2]],
                         [dest for _, dest, _ in yielded])
        self.assertEqual([[o[0] for o in objects[6:10:2]],
                          [o[0] for o in objects[7:10:2]],
                          [o[0] for o in objects[10:20:2]],
                          [o[0] for o in objects[11:20:2]],
                          [o[0] for o in objects[20:26:2]],
                          [o[0] for o in objects[21:26:2]]],
                         [[o['name'] for o in objs] for objs, _, _ in yielded])
        self.assertEqual([orig_info] * 6, [info for _, _, info in yielded])

        # dest much narrower than src
        src_range = ShardRange('dont/care', Timestamp.now(),
                               lower='o05', upper='o25')
        dest_shard_ranges = mock.MagicMock(return_value=dest_ranges[1:2])
        with self._mock_sharder(conf={}) as sharder:
            yielded = [y for y in
                       sharder.yield_objects_to_shard_range(
                           broker, src_range, dest_shard_ranges)]
        self.assertEqual([None, None,
                          dest_ranges[1], dest_ranges[1],
                          None, None],
                         [dest for _, dest, _ in yielded])
        self.assertEqual([[o[0] for o in objects[6:10:2]],
                          [o[0] for o in objects[7:10:2]],
                          [o[0] for o in objects[10:20:2]],
                          [o[0] for o in objects[11:20:2]],
                          [o[0] for o in objects[20:26:2]],
                          [o[0] for o in objects[21:26:2]]],
                         [[o['name'] for o in objs] for objs, _, _ in yielded])
        self.assertEqual([orig_info] * 6, [info for _, _, info in yielded])

        # no dest, source is entire namespace, multiple batches
        src_range = ShardRange('dont/care', Timestamp.now())
        dest_shard_ranges = mock.MagicMock(return_value=[])
        with self._mock_sharder(conf={'cleave_row_batch_size': 10}) as sharder:
            yielded = [y for y in
                       sharder.yield_objects_to_shard_range(
                           broker, src_range, dest_shard_ranges)]
        self.assertEqual([None] * 4,
                         [dest for _, dest, _ in yielded])
        self.assertEqual([[o[0] for o in objects[:20:2]],
                          [o[0] for o in objects[20::2]],
                          [o[0] for o in objects[1:20:2]],
                          [o[0] for o in objects[21::2]]],
                         [[o['name'] for o in objs] for objs, _, _ in yielded])
        self.assertEqual([orig_info] * 4, [info for _, _, info in yielded])

    def _check_cleave_root(self, conf=None):
        broker = self._make_broker()
        objects = [
            # shard 0
            ('a', self.ts_encoded(), 10, 'text/plain', 'etag_a', 0, 0),
            ('here', self.ts_encoded(), 10, 'text/plain', 'etag_here', 0, 0),
            # shard 1
            ('m', self.ts_encoded(), 1, 'text/plain', 'etag_m', 0, 0),
            ('n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0, 0),
            ('there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0, 0),
            # shard 2
            ('where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0,
             0),
            # shard 3
            ('x', self.ts_encoded(), 0, '', '', 1, 0),  # deleted
            ('y', self.ts_encoded(), 1000, 'text/plain', 'etag_y', 0, 0),
            # shard 4
            ('yyyy', self.ts_encoded(), 14, 'text/plain', 'etag_yyyy', 0, 0),
        ]
        for obj in objects:
            broker.put_object(*obj)
        initial_root_info = broker.get_info()
        broker.enable_sharding(Timestamp.now())

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', 'yonder'),
                        ('yonder', ''))
        shard_ranges = self._make_shard_ranges(shard_bounds)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        # used to accumulate stats from sharded dbs
        total_shard_stats = {'object_count': 0, 'bytes_used': 0}
        # run cleave - no shard ranges, nothing happens
        with self._mock_sharder(conf=conf) as sharder:
            self.assertFalse(sharder._cleave(broker))

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual('', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(0, context.ranges_done)
        self.assertEqual(0, context.ranges_todo)

        self.assertEqual(UNSHARDED, broker.get_db_state())
        sharder._replicate_object.assert_not_called()
        for db in expected_shard_dbs:
            with annotate_failure(db):
                self.assertFalse(os.path.exists(db))

        # run cleave - all shard ranges in found state, nothing happens
        broker.merge_shard_ranges(shard_ranges[:4])
        self.assertTrue(broker.set_sharding_state())

        with self._mock_sharder(conf=conf) as sharder:
            self.assertFalse(sharder._cleave(broker))

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual('', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(0, context.ranges_done)
        self.assertEqual(4, context.ranges_todo)

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
        with self._mock_sharder(conf=conf) as sharder:
            self.assertFalse(sharder._cleave(broker))

        expected = {'attempted': 1, 'success': 1, 'failure': 0,
                    'min_time': mock.ANY, 'max_time': mock.ANY,
                    'db_created': 1, 'db_exists': 0}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_exists'))
        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[0], 0)
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        shard_own_sr = shard_broker.get_own_shard_range()
        self.assertEqual(ShardRange.CLEAVED, shard_own_sr.state)
        shard_info = shard_broker.get_info()
        total_shard_stats['object_count'] += shard_info['object_count']
        total_shard_stats['bytes_used'] += shard_info['bytes_used']

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))
        # update expected state and metadata, check cleaved shard range
        shard_ranges[0].bytes_used = 20
        shard_ranges[0].object_count = 2
        shard_ranges[0].state = ShardRange.CLEAVED
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

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual('here', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(1, context.ranges_done)
        self.assertEqual(3, context.ranges_todo)

        unlink_files(expected_shard_dbs)

        # move more shard ranges to created state
        for i in range(1, 4):
            shard_ranges[i].update_state(ShardRange.CREATED)
        broker.merge_shard_ranges(shard_ranges[1:4])

        # replication of next shard range is not sufficiently successful
        with self._mock_sharder(conf=conf) as sharder:
            quorum = quorum_size(sharder.ring.replica_count)
            successes = [True] * (quorum - 1)
            fails = [False] * (sharder.ring.replica_count - len(successes))
            responses = successes + fails
            random.shuffle(responses)
            sharder._replicate_object = mock.MagicMock(
                side_effect=((False, responses),))
            self.assertFalse(sharder._cleave(broker))
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[1], 0)

        expected = {'attempted': 1, 'success': 0, 'failure': 1,
                    'min_time': mock.ANY, 'max_time': mock.ANY,
                    'db_created': 1, 'db_exists': 0}
        self._assert_stats(expected, sharder, 'cleaved')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_exists'))

        # cleaving state is unchanged
        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))
        for i in range(1, len(updated_shard_ranges)):
            with annotate_failure(i):
                self.assertEqual(dict(shard_ranges[i]),
                                 dict(updated_shard_ranges[i]))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual('here', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(1, context.ranges_done)
        self.assertEqual(3, context.ranges_todo)

        # try again, this time replication is sufficiently successful
        with self._mock_sharder(conf=conf) as sharder:
            successes = [True] * quorum
            fails = [False] * (sharder.ring.replica_count - len(successes))
            responses1 = successes + fails
            responses2 = fails + successes
            sharder._replicate_object = mock.MagicMock(
                side_effect=((False, responses1), (False, responses2)))
            self.assertFalse(sharder._cleave(broker))

        expected = {'attempted': 2, 'success': 2, 'failure': 0,
                    'min_time': mock.ANY, 'max_time': mock.ANY,
                    'db_created': 1, 'db_exists': 1}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_created'))
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_exists'))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[1:3]]
        )
        for db in expected_shard_dbs[1:3]:
            shard_broker = ContainerBroker(db)
            shard_own_sr = shard_broker.get_own_shard_range()
            self.assertEqual(ShardRange.CLEAVED, shard_own_sr.state)
            shard_info = shard_broker.get_info()
            total_shard_stats['object_count'] += shard_info['object_count']
            total_shard_stats['bytes_used'] += shard_info['bytes_used']

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))

        # only 2 are cleaved per batch
        # update expected state and metadata, check cleaved shard ranges
        shard_ranges[1].bytes_used = 6
        shard_ranges[1].object_count = 3
        shard_ranges[1].state = ShardRange.CLEAVED
        shard_ranges[2].bytes_used = 100
        shard_ranges[2].object_count = 1
        shard_ranges[2].state = ShardRange.CLEAVED
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
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual('where', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(3, context.ranges_done)
        self.assertEqual(1, context.ranges_todo)

        unlink_files(expected_shard_dbs)

        # run cleave again - should process the fourth range
        with self._mock_sharder(conf=conf) as sharder:
            self.assertFalse(sharder._cleave(broker))

        expected = {'attempted': 1, 'success': 1, 'failure': 0,
                    'min_time': mock.ANY, 'max_time': mock.ANY,
                    'db_created': 1, 'db_exists': 0}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_exists'))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[3], 0)
        shard_broker = ContainerBroker(expected_shard_dbs[3])
        shard_own_sr = shard_broker.get_own_shard_range()
        self.assertEqual(ShardRange.CLEAVED, shard_own_sr.state)
        shard_info = shard_broker.get_info()
        total_shard_stats['object_count'] += shard_info['object_count']
        total_shard_stats['bytes_used'] += shard_info['bytes_used']

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(4, len(updated_shard_ranges))

        shard_ranges[3].bytes_used = 1000
        shard_ranges[3].object_count = 1
        shard_ranges[3].state = ShardRange.CLEAVED
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
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual('yonder', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(4, context.ranges_done)
        self.assertEqual(0, context.ranges_todo)

        unlink_files(expected_shard_dbs)

        # run cleave - should be a no-op, all existing ranges have been cleaved
        with self._mock_sharder(conf=conf) as sharder:
            self.assertFalse(sharder._cleave(broker))

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_not_called()

        # add final shard range - move this to ACTIVE state and update stats to
        # simulate another replica having cleaved it and replicated its state
        shard_ranges[4].update_state(ShardRange.ACTIVE)
        shard_ranges[4].update_meta(2, 15)
        broker.merge_shard_ranges(shard_ranges[4:])

        with self._mock_sharder(conf=conf) as sharder:
            self.assertTrue(sharder._cleave(broker))

        expected = {'attempted': 1, 'success': 1, 'failure': 0,
                    'min_time': mock.ANY, 'max_time': mock.ANY,
                    'db_created': 1, 'db_exists': 0}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'cleaved_db_exists'))

        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[4], 0)
        shard_broker = ContainerBroker(expected_shard_dbs[4])
        shard_own_sr = shard_broker.get_own_shard_range()
        self.assertEqual(ShardRange.ACTIVE, shard_own_sr.state)
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

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertTrue(context.done())
        self.assertEqual('', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(5, context.ranges_done)
        self.assertEqual(0, context.ranges_todo)

        with self._mock_sharder(conf=conf) as sharder:
            self.assertTrue(sharder._cleave(broker))
        sharder._replicate_object.assert_not_called()

        self.assertTrue(broker.set_sharded_state())
        # run cleave - should be a no-op
        with self._mock_sharder(conf=conf) as sharder:
            self.assertTrue(sharder._cleave(broker))

        sharder._replicate_object.assert_not_called()

    def test_cleave_root(self):
        self._check_cleave_root()

    def test_cleave_root_listing_limit_one(self):
        # force yield_objects to update its marker and call to the broker's
        # get_objects() for each shard range, to check the marker moves on
        self._check_cleave_root(conf={'cleave_row_batch_size': 1})

    def test_cleave_root_ranges_change(self):
        # verify that objects are not missed if shard ranges change between
        # cleaving batches
        broker = self._make_broker()
        # this root db has very few object rows...
        objects = [
            ('a', self.ts_encoded(), 10, 'text/plain', 'etag_a', 0, 0),
            ('b', self.ts_encoded(), 10, 'text/plain', 'etag_b', 0, 0),
            ('c', self.ts_encoded(), 1, 'text/plain', 'etag_c', 0, 0),
            ('d', self.ts_encoded(), 2, 'text/plain', 'etag_d', 0, 0),
            ('e', self.ts_encoded(), 3, 'text/plain', 'etag_e', 0, 0),
            ('f', self.ts_encoded(), 100, 'text/plain', 'etag_f', 0, 0),
            ('x', self.ts_encoded(), 0, '', '', 1, 0),  # deleted
            ('z', self.ts_encoded(), 1000, 'text/plain', 'etag_z', 0, 0)
        ]
        for obj in objects:
            broker.put_object(*obj)
        broker.enable_sharding(Timestamp.now())

        shard_bounds = (('', 'd'), ('d', 'x'), ('x', ''))
        # shard ranges start life with object count that is typically much
        # larger than this DB's object population...
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED, object_count=500000)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        broker.merge_shard_ranges(shard_ranges[:3])
        self.assertTrue(broker.set_sharding_state())

        # run cleave - first batch is cleaved
        with self._mock_sharder() as sharder:
            self.assertFalse(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual(shard_ranges[1].upper_str, context.cursor)
        self.assertEqual(8, context.cleave_to_row)
        self.assertEqual(8, context.max_row)

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[:2]]
        )

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(3, len(updated_shard_ranges))

        # now they have reached CLEAVED state, the first 2 shard ranges should
        # have updated object count, bytes used and meta_timestamp
        shard_ranges[0].bytes_used = 23
        shard_ranges[0].object_count = 4
        shard_ranges[0].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        shard_ranges[1].bytes_used = 103
        shard_ranges[1].object_count = 2
        shard_ranges[1].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[1], updated_shard_ranges[1])
        self._check_objects(objects[:4], expected_shard_dbs[0])
        self._check_objects(objects[4:7], expected_shard_dbs[1])
        # the actual object counts were set in the new shard brokers' own_sr's
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        self.assertEqual(4, shard_broker.get_own_shard_range().object_count)
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        self.assertEqual(2, shard_broker.get_own_shard_range().object_count)
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

        # third shard range should be unchanged - not yet cleaved
        self.assertEqual(dict(shard_ranges[2]),
                         dict(updated_shard_ranges[2]))

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual(shard_ranges[1].upper_str, context.cursor)
        self.assertEqual(8, context.cleave_to_row)
        self.assertEqual(8, context.max_row)

        # now change the shard ranges so that third consumes second
        shard_ranges[1].set_deleted()
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
        shard_ranges[2].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[2], updated_shard_ranges[1])
        self._check_objects(objects[4:8], expected_shard_dbs[2])

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertTrue(context.done())
        self.assertEqual(shard_ranges[2].upper_str, context.cursor)
        self.assertEqual(8, context.cleave_to_row)
        self.assertEqual(8, context.max_row)

    def test_cleave_root_empty_db_with_ranges(self):
        broker = self._make_broker()
        broker.enable_sharding(Timestamp.now())

        shard_bounds = (('', 'd'), ('d', 'x'), ('x', ''))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)

        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())

        sharder_conf = {'cleave_batch_size': 1}
        with self._mock_sharder(sharder_conf) as sharder:
            self.assertTrue(sharder._cleave(broker))

        info_lines = sharder.logger.get_lines_for_level('info')
        expected_zero_obj = [line for line in info_lines
                             if " - zero objects found" in line]
        self.assertEqual(len(expected_zero_obj), len(shard_bounds))

        cleaving_context = CleavingContext.load(broker)
        # even though there is a cleave_batch_size of 1, we don't count empty
        # ranges when cleaving seeing as they aren't replicated
        self.assertEqual(cleaving_context.ranges_done, 3)
        self.assertEqual(cleaving_context.ranges_todo, 0)
        self.assertTrue(cleaving_context.cleaving_done)

        self.assertEqual([ShardRange.CLEAVED] * 3,
                         [sr.state for sr in broker.get_shard_ranges()])

    def test_cleave_root_empty_db_with_pre_existing_shard_db_handoff(self):
        broker = self._make_broker()
        broker.enable_sharding(Timestamp.now())

        shard_bounds = (('', 'd'), ('d', 'x'), ('x', ''))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)

        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())

        sharder_conf = {'cleave_batch_size': 1}
        with self._mock_sharder(sharder_conf) as sharder:
            # pre-create a shard broker on a handoff location. This will force
            # the sharder to not skip it but instead force to replicate it and
            # use up a cleave_batch_size count.
            sharder._get_shard_broker(shard_ranges[0], broker.root_path,
                                      0)
            self.assertFalse(sharder._cleave(broker))

        info_lines = sharder.logger.get_lines_for_level('info')
        expected_zero_obj = [line for line in info_lines
                             if " - zero objects found" in line]
        self.assertEqual(len(expected_zero_obj), 1)

        cleaving_context = CleavingContext.load(broker)
        # even though there is a cleave_batch_size of 1, we don't count empty
        # ranges when cleaving seeing as they aren't replicated
        self.assertEqual(cleaving_context.ranges_done, 1)
        self.assertEqual(cleaving_context.ranges_todo, 2)
        self.assertFalse(cleaving_context.cleaving_done)

        self.assertEqual(
            [ShardRange.CLEAVED, ShardRange.CREATED, ShardRange.CREATED],
            [sr.state for sr in broker.get_shard_ranges()])

    def test_cleave_shard_range_no_own_shard_range(self):
        # create an unsharded broker that has shard ranges but no
        # own_shard_range, verify that it does not cleave...
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        shard_ranges = self._make_shard_ranges(
            (('', 'middle'), ('middle', '')),
            state=ShardRange.CLEAVED)
        broker.merge_shard_ranges(shard_ranges)
        obj = {'name': 'obj', 'created_at': next(self.ts_iter).internal,
               'size': 14, 'content_type': 'text/plain', 'etag': 'an etag',
               'deleted': 0}
        broker.get_brokers()[0].merge_items([obj])
        with self._mock_sharder() as sharder:
            self.assertFalse(sharder._cleave(broker))
        self.assertEqual(UNSHARDED, broker.get_db_state())
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertEqual(warning_lines[0],
                         'Failed to get own_shard_range, path: a/c, db: %s'
                         % broker.db_file)
        sharder._replicate_object.assert_not_called()
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        # only the root broker on disk
        suffix_dir = os.path.dirname(broker.db_dir)
        self.assertEqual([os.path.basename(broker.db_dir)],
                         os.listdir(suffix_dir))
        partition_dir = os.path.dirname(suffix_dir)
        self.assertEqual([broker.db_dir[-3:]], os.listdir(partition_dir))
        containers_dir = os.path.dirname(partition_dir)
        self.assertEqual(['0'], os.listdir(containers_dir))

    def test_cleave_shard(self):
        broker = self._make_broker(account='.shards_a', container='shard_c')
        own_shard_range = ShardRange(
            broker.path, Timestamp.now(), 'here', 'where',
            state=ShardRange.SHARDING, epoch=Timestamp.now())
        broker.merge_shard_ranges([own_shard_range])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(broker.is_root_container())  # sanity check

        objects = [
            ('m', self.ts_encoded(), 1, 'text/plain', 'etag_m', 0, 0),
            ('n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0, 0),
            ('there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0, 0),
            ('where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0,
             0),
        ]
        misplaced_objects = [
            ('a', self.ts_encoded(), 1, 'text/plain', 'etag_a', 0, 0),
            ('z', self.ts_encoded(), 100, 'text/plain', 'etag_z', 1, 0),
        ]
        for obj in objects + misplaced_objects:
            broker.put_object(*obj)

        shard_bounds = (('here', 'there'),
                        ('there', 'where'))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        misplaced_bounds = (('', 'here'),
                            ('where', ''))
        misplaced_ranges = self._make_shard_ranges(
            misplaced_bounds, state=ShardRange.ACTIVE)
        misplaced_dbs = []
        for shard_range in misplaced_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            misplaced_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())

        # run cleave - first range is cleaved but move of misplaced objects is
        # not successful
        sharder_conf = {'cleave_batch_size': 1}
        with self._mock_sharder(sharder_conf) as sharder:
            with mock.patch.object(
                    sharder, '_make_shard_range_fetcher',
                    return_value=lambda: iter(misplaced_ranges)):
                # cause misplaced objects replication to not succeed
                quorum = quorum_size(sharder.ring.replica_count)
                successes = [True] * (quorum - 1)
                fails = [False] * (sharder.ring.replica_count - len(successes))
                responses = successes + fails
                random.shuffle(responses)
                bad_result = (False, responses)
                ok_result = (True, [True] * sharder.ring.replica_count)
                sharder._replicate_object = mock.MagicMock(
                    # result for misplaced, misplaced, cleave
                    side_effect=(bad_result, ok_result, ok_result))
                self.assertFalse(sharder._cleave(broker))

        context = CleavingContext.load(broker)
        self.assertFalse(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertEqual(shard_ranges[0].upper_str, context.cursor)
        self.assertEqual(6, context.cleave_to_row)
        self.assertEqual(6, context.max_row)

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, misplaced_dbs[0], 0),
             mock.call(0, misplaced_dbs[1], 0),
             mock.call(0, expected_shard_dbs[0], 0)])
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        # NB cleaving a shard, state goes to CLEAVED not ACTIVE
        shard_own_sr = shard_broker.get_own_shard_range()
        self.assertEqual(ShardRange.CLEAVED, shard_own_sr.state)

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(2, len(updated_shard_ranges))

        # first shard range should have updated object count, bytes used and
        # meta_timestamp
        shard_ranges[0].bytes_used = 6
        shard_ranges[0].object_count = 3
        shard_ranges[0].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        self._check_objects(objects[:3], expected_shard_dbs[0])
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self._check_objects(misplaced_objects[:1], misplaced_dbs[0])
        self._check_objects(misplaced_objects[1:], misplaced_dbs[1])
        unlink_files(expected_shard_dbs)
        unlink_files(misplaced_dbs)

        # run cleave - second (final) range is cleaved; move this range to
        # CLEAVED state and update stats to simulate another replica having
        # cleaved it and replicated its state
        shard_ranges[1].update_state(ShardRange.CLEAVED)
        shard_ranges[1].update_meta(2, 15)
        broker.merge_shard_ranges(shard_ranges[1:2])
        with self._mock_sharder(sharder_conf) as sharder:
            with mock.patch.object(
                    sharder, '_make_shard_range_fetcher',
                    return_value=lambda: iter(misplaced_ranges)):
                self.assertTrue(sharder._cleave(broker))

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertEqual(shard_ranges[1].upper_str, context.cursor)
        self.assertEqual(6, context.cleave_to_row)
        self.assertEqual(6, context.max_row)

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, misplaced_dbs[0], 0),
             mock.call(0, expected_shard_dbs[1], 0)])
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        shard_own_sr = shard_broker.get_own_shard_range()
        self.assertEqual(ShardRange.CLEAVED, shard_own_sr.state)

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(2, len(updated_shard_ranges))

        # second shard range should have updated object count, bytes used and
        # meta_timestamp
        self.assertEqual(dict(shard_ranges[1]), dict(updated_shard_ranges[1]))
        self._check_objects(objects[3:], expected_shard_dbs[1])
        self.assertFalse(os.path.exists(expected_shard_dbs[0]))
        self._check_objects(misplaced_objects[:1], misplaced_dbs[0])
        self.assertFalse(os.path.exists(misplaced_dbs[1]))

    def test_cleave_shard_shrinking(self):
        unique = [0]

        def do_test(acceptor_state, acceptor_bounds, expect_delete,
                    exp_progress_bounds=None):
            # 'unique' ensures fresh dbs on each test iteration
            unique[0] += 1

            objects = [
                ('i', self.ts_encoded(), 3, 'text/plain', 'etag_t', 0, 0),
                ('m', self.ts_encoded(), 33, 'text/plain', 'etag_m', 0, 0),
                ('w', self.ts_encoded(), 100, 'text/plain', 'etag_w', 0, 0),
            ]
            broker = self._make_shrinking_broker(
                container='donor_%s' % unique[0], lower='h', upper='w',
                objects=objects)
            acceptor_epoch = next(self.ts_iter)
            acceptors = [
                ShardRange('.shards_a/acceptor_%s_%s' % (unique[0], bounds[1]),
                           Timestamp.now(), bounds[0], bounds[1],
                           '1000', '11111',
                           state=acceptor_state, epoch=acceptor_epoch)
                for bounds in acceptor_bounds]
            # by default expect cleaving to progress through all acceptors
            if exp_progress_bounds is None:
                exp_progress_acceptors = acceptors
            else:
                exp_progress_acceptors = [
                    ShardRange(
                        '.shards_a/acceptor_%s_%s' % (unique[0], bounds[1]),
                        Timestamp.now(), bounds[0], bounds[1], '1000', '11111',
                        state=acceptor_state, epoch=acceptor_epoch)
                    for bounds in exp_progress_bounds]
            expected_acceptor_dbs = []
            for acceptor in exp_progress_acceptors:
                db_hash = hash_path(acceptor.account,
                                    acceptor.container)
                # NB expected cleaved db name includes acceptor epoch
                db_name = '%s_%s.db' % (db_hash, acceptor_epoch.internal)
                expected_acceptor_dbs.append(
                    os.path.join(self.tempdir, 'sda', 'containers', '0',
                                 db_hash[-3:], db_hash, db_name))

            broker.merge_shard_ranges(acceptors)

            # run cleave
            with mock_timestamp_now_with_iter(self.ts_iter):
                with self._mock_sharder() as sharder:
                    sharder.cleave_batch_size = 3
                    self.assertEqual(expect_delete, sharder._cleave(broker))

            # check the cleave context and source broker
            context = CleavingContext.load(broker)
            self.assertTrue(context.misplaced_done)
            self.assertEqual(expect_delete, context.cleaving_done)
            own_sr = broker.get_own_shard_range()
            if exp_progress_acceptors:
                expected_cursor = exp_progress_acceptors[-1].upper_str
            else:
                expected_cursor = own_sr.lower_str
            self.assertEqual(expected_cursor, context.cursor)
            self.assertEqual(3, context.cleave_to_row)
            self.assertEqual(3, context.max_row)
            self.assertEqual(SHARDING, broker.get_db_state())
            if expect_delete and len(acceptor_bounds) == 1:
                self.assertTrue(own_sr.deleted)
                self.assertEqual(ShardRange.SHRUNK, own_sr.state)
            else:
                self.assertFalse(own_sr.deleted)
                self.assertEqual(ShardRange.SHRINKING, own_sr.state)

            # check the acceptor db's
            sharder._replicate_object.assert_has_calls(
                [mock.call(0, acceptor_db, 0)
                 for acceptor_db in expected_acceptor_dbs])
            for acceptor_db in expected_acceptor_dbs:
                self.assertTrue(os.path.exists(acceptor_db))
                # NB when *shrinking* a shard container then expect the
                # acceptor broker's own shard range state to remain in the
                # original state of the acceptor shard range rather than being
                # set to CLEAVED as it would when *sharding*.
                acceptor_broker = ContainerBroker(acceptor_db)
                self.assertEqual(acceptor_state,
                                 acceptor_broker.get_own_shard_range().state)
                acceptor_ranges = acceptor_broker.get_shard_ranges(
                    include_deleted=True)
                if expect_delete and len(acceptor_bounds) == 1:
                    # special case when deleted shrinking shard range is
                    # forwarded to single enclosing acceptor
                    self.assertEqual([own_sr], acceptor_ranges)
                    self.assertTrue(acceptor_ranges[0].deleted)
                    self.assertEqual(ShardRange.SHRUNK,
                                     acceptor_ranges[0].state)
                else:
                    self.assertEqual([], acceptor_ranges)

            expected_objects = [
                obj for obj in objects
                if any(acceptor.lower < obj[0] <= acceptor.upper
                       for acceptor in exp_progress_acceptors)
            ]
            self._check_objects(expected_objects, expected_acceptor_dbs)

            # check that *shrinking* shard's copies of acceptor ranges are not
            # updated as they would be if *sharding*
            updated_shard_ranges = broker.get_shard_ranges()
            self.assertEqual([dict(sr) for sr in acceptors],
                             [dict(sr) for sr in updated_shard_ranges])

            # check that *shrinking* shard's copies of acceptor ranges are not
            # updated when completing sharding as they would be if *sharding*
            with mock_timestamp_now_with_iter(self.ts_iter):
                sharder._complete_sharding(broker)

            updated_shard_ranges = broker.get_shard_ranges()
            self.assertEqual([dict(sr) for sr in acceptors],
                             [dict(sr) for sr in updated_shard_ranges])
            own_sr = broker.get_own_shard_range()
            self.assertEqual(expect_delete, own_sr.deleted)
            if expect_delete:
                self.assertEqual(ShardRange.SHRUNK, own_sr.state)
            else:
                self.assertEqual(ShardRange.SHRINKING, own_sr.state)

        # note: shrinking shard bounds are (h, w)
        # shrinking to a single acceptor with enclosing namespace
        expect_delete = True
        do_test(ShardRange.CREATED, (('h', ''),), expect_delete)
        do_test(ShardRange.CLEAVED, (('h', ''),), expect_delete)
        do_test(ShardRange.ACTIVE, (('h', ''),), expect_delete)

        # shrinking to multiple acceptors that enclose namespace
        do_test(ShardRange.CREATED, (('d', 'k'), ('k', '')), expect_delete)
        do_test(ShardRange.CLEAVED, (('d', 'k'), ('k', '')), expect_delete)
        do_test(ShardRange.ACTIVE, (('d', 'k'), ('k', '')), expect_delete)
        do_test(ShardRange.CLEAVED, (('d', 'k'), ('k', 't'), ('t', '')),
                expect_delete)
        do_test(ShardRange.CREATED, (('d', 'k'), ('k', 't'), ('t', '')),
                expect_delete)
        do_test(ShardRange.ACTIVE, (('d', 'k'), ('k', 't'), ('t', '')),
                expect_delete)

        # shrinking to incomplete acceptors, gap at end of namespace
        expect_delete = False
        do_test(ShardRange.CREATED, (('d', 'k'),), expect_delete)
        do_test(ShardRange.CLEAVED, (('d', 'k'), ('k', 't')), expect_delete)
        # shrinking to incomplete acceptors, gap at start and end of namespace
        do_test(ShardRange.CREATED, (('k', 't'),), expect_delete,
                exp_progress_bounds=())
        # shrinking to incomplete acceptors, gap at start of namespace
        do_test(ShardRange.CLEAVED, (('k', 't'), ('t', '')), expect_delete,
                exp_progress_bounds=())
        # shrinking to incomplete acceptors, gap in middle - some progress
        do_test(ShardRange.CLEAVED, (('d', 'k'), ('t', '')), expect_delete,
                exp_progress_bounds=(('d', 'k'),))

    def test_cleave_repeated(self):
        # verify that if new objects are merged into retiring db after cleaving
        # started then cleaving will repeat but only new objects are cleaved
        # in the repeated cleaving pass
        broker = self._make_broker()
        objects = [
            ('obj%03d' % i, next(self.ts_iter), 1, 'text/plain', 'etag', 0, 0)
            for i in range(10)
        ]
        new_objects = [
            (name, next(self.ts_iter), 1, 'text/plain', 'etag', 0, 0)
            for name in ('alpha', 'zeta')
        ]
        for obj in objects:
            broker.put_object(*obj)
        broker._commit_puts()
        broker.enable_sharding(Timestamp.now())
        shard_bounds = (('', 'obj004'), ('obj004', ''))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(shard_ranges)
        old_broker = broker.get_brokers()[0]
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}

        calls = []
        key = ('name', 'created_at', 'size', 'content_type', 'etag', 'deleted')

        def mock_replicate_object(part, db, node_id):
            # merge new objects between cleave of first and second shard ranges
            if not calls:
                old_broker.merge_items(
                    [dict(zip(key, obj)) for obj in new_objects])
            calls.append((part, db, node_id))
            return True, [True, True, True]

        with self._mock_sharder() as sharder:
            sharder._audit_container = mock.MagicMock()
            sharder._replicate_object = mock_replicate_object
            sharder._process_broker(broker, node, 99)

        # sanity check - the new objects merged into the old db
        self.assertFalse(broker.get_objects())
        self.assertEqual(12, len(old_broker.get_objects()))

        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        self.assertEqual([(0, expected_shard_dbs[0], 0),
                          (0, expected_shard_dbs[1], 0)], calls)

        # check shard ranges were updated to CLEAVED
        updated_shard_ranges = broker.get_shard_ranges()
        # 'alpha' was not in table when first shard was cleaved
        shard_ranges[0].bytes_used = 5
        shard_ranges[0].object_count = 5
        shard_ranges[0].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        self._check_objects(objects[:5], expected_shard_dbs[0])
        # 'zeta' was in table when second shard was cleaved
        shard_ranges[1].bytes_used = 6
        shard_ranges[1].object_count = 6
        shard_ranges[1].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[1], updated_shard_ranges[1])
        self._check_objects(objects[5:] + new_objects[1:],
                            expected_shard_dbs[1])

        context = CleavingContext.load(broker)
        self.assertFalse(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.done())
        self.assertEqual('', context.cursor)
        self.assertEqual(10, context.cleave_to_row)
        self.assertEqual(12, context.max_row)  # note that max row increased
        self.assertTrue(self.logger.statsd_client.calls['timing_since'])
        self.assertEqual(
            'sharder.sharding.move_misplaced',
            self.logger.statsd_client.calls['timing_since'][-3][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-3][0][1], 0)
        self.assertEqual(
            'sharder.sharding.set_state',
            self.logger.statsd_client.calls['timing_since'][-2][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-2][0][1], 0)
        self.assertEqual(
            'sharder.sharding.cleave',
            self.logger.statsd_client.calls['timing_since'][-1][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-1][0][1], 0)
        lines = sharder.logger.get_lines_for_level('info')
        self.assertEqual(
            ["Kick off container cleaving, own shard range in state "
             "'sharding', path: a/c, db: %s" % broker.db_file,
             "Starting to cleave (2 todo), path: a/c, db: %s"
             % broker.db_file], lines[:2])
        self.assertIn('Completed cleaving, DB remaining in sharding state, '
                      'path: a/c, db: %s'
                      % broker.db_file, lines[1:])
        lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn('Repeat cleaving required', lines[0])
        self.assertFalse(lines[1:])
        unlink_files(expected_shard_dbs)

        # repeat the cleaving - the newer objects get cleaved
        with self._mock_sharder() as sharder:
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)

        # this time the sharding completed
        self.assertEqual(SHARDED, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDED,
                         broker.get_own_shard_range().state)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[0], 0),
             mock.call(0, expected_shard_dbs[1], 0)])

        # shard ranges are now ACTIVE - stats not updated by cleaving
        updated_shard_ranges = broker.get_shard_ranges()
        shard_ranges[0].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        self._check_objects(new_objects[:1], expected_shard_dbs[0])
        # both new objects are included in repeat cleaving but no older objects
        shard_ranges[1].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[1], updated_shard_ranges[1])
        self._check_objects(new_objects[1:], expected_shard_dbs[1])
        lines = sharder.logger.get_lines_for_level('info')
        self.assertEqual(
            'Starting to cleave (2 todo), path: a/c, db: %s'
            % broker.db_file, lines[0])
        self.assertIn(
            'Completed cleaving, DB set to sharded state, path: a/c, db: %s'
            % broker.db_file, lines[1:])
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertTrue(self.logger.statsd_client.calls['timing_since'])
        self.assertEqual(
            'sharder.sharding.move_misplaced',
            self.logger.statsd_client.calls['timing_since'][-4][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-4][0][1], 0)
        self.assertEqual(
            'sharder.sharding.cleave',
            self.logger.statsd_client.calls['timing_since'][-3][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-3][0][1], 0)
        self.assertEqual(
            'sharder.sharding.completed',
            self.logger.statsd_client.calls['timing_since'][-2][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-2][0][1], 0)
        self.assertEqual(
            'sharder.sharding.send_sr',
            self.logger.statsd_client.calls['timing_since'][-1][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-1][0][1], 0)

    def test_cleave_timing_metrics(self):
        broker = self._make_broker()
        objects = [{'name': 'obj_%03d' % i,
                    'created_at': Timestamp.now().normal,
                    'content_type': 'text/plain',
                    'etag': 'etag_%d' % i,
                    'size': 1024 * i,
                    'deleted': i % 2,
                    'storage_policy_index': 0,
                    } for i in range(1, 8)]
        broker.merge_items([dict(obj) for obj in objects])
        broker.enable_sharding(Timestamp.now())
        shard_ranges = self._make_shard_ranges(
            (('', 'obj_004'), ('obj_004', '')), state=ShardRange.CREATED)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}

        with self._mock_sharder() as sharder:
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)

        lines = sharder.logger.get_lines_for_level('info')
        self.assertEqual(
            'Starting to cleave (2 todo), path: a/c, db: %s'
            % broker.db_file, lines[0])
        self.assertIn(
            'Completed cleaving, DB set to sharded state, path: a/c, db: %s'
            % broker.db_file, lines[1:])

        self.assertTrue(self.logger.statsd_client.calls['timing_since'])
        self.assertEqual(
            'sharder.sharding.move_misplaced',
            self.logger.statsd_client.calls['timing_since'][-4][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-4][0][1], 0)
        self.assertEqual(
            'sharder.sharding.cleave',
            self.logger.statsd_client.calls['timing_since'][-3][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-3][0][1], 0)
        self.assertEqual(
            'sharder.sharding.completed',
            self.logger.statsd_client.calls['timing_since'][-2][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-2][0][1], 0)
        self.assertEqual(
            'sharder.sharding.send_sr',
            self.logger.statsd_client.calls['timing_since'][-1][0][0])
        self.assertGreater(
            self.logger.statsd_client.calls['timing_since'][-1][0][1], 0)

        # check shard ranges were updated to ACTIVE
        self.assertEqual([ShardRange.ACTIVE] * 2,
                         [sr.state for sr in broker.get_shard_ranges()])
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        actual_objects = shard_broker.get_objects()
        self.assertEqual(objects[:4], actual_objects)
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        actual_objects = shard_broker.get_objects()
        self.assertEqual(objects[4:], actual_objects)

    def test_cleave_multiple_storage_policies(self):
        # verify that objects in all storage policies are cleaved
        broker = self._make_broker()
        # add objects in multiple policies
        objects = [{'name': 'obj_%03d' % i,
                    'created_at': Timestamp.now().normal,
                    'content_type': 'text/plain',
                    'etag': 'etag_%d' % i,
                    'size': 1024 * i,
                    'deleted': i % 2,
                    'storage_policy_index': i % 2,
                    } for i in range(1, 8)]
        # merge_items mutates items
        broker.merge_items([dict(obj) for obj in objects])
        broker.enable_sharding(Timestamp.now())
        shard_ranges = self._make_shard_ranges(
            (('', 'obj_004'), ('obj_004', '')), state=ShardRange.CREATED)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}

        with self._mock_sharder() as sharder:
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)

        # check shard ranges were updated to ACTIVE
        self.assertEqual([ShardRange.ACTIVE] * 2,
                         [sr.state for sr in broker.get_shard_ranges()])
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        actual_objects = shard_broker.get_objects()
        self.assertEqual(objects[:4], actual_objects)

        shard_broker = ContainerBroker(expected_shard_dbs[1])
        actual_objects = shard_broker.get_objects()
        self.assertEqual(objects[4:], actual_objects)

    def test_cleave_insufficient_replication(self):
        # verify that if replication of a cleaved shard range fails then rows
        # are not merged again to the existing shard db
        broker = self._make_broker()
        retiring_db_id = broker.get_info()['id']
        objects = [
            {'name': 'obj%03d' % i, 'created_at': next(self.ts_iter),
             'size': 1, 'content_type': 'text/plain', 'etag': 'etag',
             'deleted': 0, 'storage_policy_index': 0}
            for i in range(10)
        ]
        broker.merge_items([dict(obj) for obj in objects])
        broker._commit_puts()
        broker.enable_sharding(Timestamp.now())
        shard_bounds = (('', 'obj004'), ('obj004', ''))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        new_object = {'name': 'alpha', 'created_at': next(self.ts_iter),
                      'size': 0, 'content_type': 'text/plain', 'etag': 'etag',
                      'deleted': 0, 'storage_policy_index': 0}
        broker.merge_items([dict(new_object)])

        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}
        orig_merge_items = ContainerBroker.merge_items

        def mock_merge_items(broker, items):
            merge_items_calls.append((broker.path,
                                      # merge mutates item so make a copy
                                      [dict(item) for item in items]))
            orig_merge_items(broker, items)

        # first shard range cleaved but fails to replicate
        merge_items_calls = []
        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items):
            with self._mock_sharder() as sharder:
                sharder._replicate_object = mock.MagicMock(
                    return_value=(False, [False, False, True]))
                sharder._audit_container = mock.MagicMock()
                sharder._process_broker(broker, node, 99)

        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())
        # first shard range cleaved to shard broker
        self.assertEqual([(shard_ranges[0].name, objects[:5])],
                         merge_items_calls)
        # replication of first shard range fails - no more shards attempted
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[0], 0)
        # shard broker has sync points
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        self.assertEqual(
            [{'remote_id': retiring_db_id, 'sync_point': len(objects)}],
            shard_broker.get_syncs())
        self.assertEqual(objects[:5], shard_broker.get_objects())

        # first shard range replicates ok, no new merges required, second is
        # cleaved but fails to replicate
        merge_items_calls = []
        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items), self._mock_sharder() as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(False, [False, True, True]),
                             (False, [False, False, True])])
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)

        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)

        broker_shard_ranges = broker.get_shard_ranges()
        shard_ranges[0].object_count = 5
        shard_ranges[0].bytes_used = sum(obj['size'] for obj in objects[:5])
        shard_ranges[0].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[0], broker_shard_ranges[0])
        # second shard range still in created state
        self._assert_shard_ranges_equal([shard_ranges[1]],
                                        [broker_shard_ranges[1]])
        # only second shard range rows were merged to shard db
        self.assertEqual([(shard_ranges[1].name, objects[5:])],
                         merge_items_calls)
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[0], 0),
             mock.call(0, expected_shard_dbs[1], 0)])
        # shard broker has sync points
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        self.assertEqual(
            [{'remote_id': retiring_db_id, 'sync_point': len(objects)}],
            shard_broker.get_syncs())
        self.assertEqual(objects[5:], shard_broker.get_objects())
        lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(lines))
        self.assertIn(
            'Failed to sufficiently replicate cleaved shard %s'
            % shard_ranges[1].name, lines[0])
        self.assertIn('1 successes, 2 required', lines[0])
        self.assertIn('shard db: %s' % expected_shard_dbs[1], lines[0])
        self.assertIn('db: %s' % broker.db_file, lines[0])

        # repeat - second shard range cleaves fully because its previously
        # cleaved shard db no longer exists
        self.logger.clear()
        unlink_files(expected_shard_dbs)
        merge_items_calls = []
        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items):
            with self._mock_sharder() as sharder:
                sharder._replicate_object = mock.MagicMock(
                    side_effect=[(True, [True, True, True]),  # misplaced obj
                                 (False, [False, True, True])])
                sharder._audit_container = mock.MagicMock()
                sharder._process_broker(broker, node, 99)

        self.assertEqual(SHARDED, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDED,
                         broker.get_own_shard_range().state)

        broker_shard_ranges = broker.get_shard_ranges()
        shard_ranges[1].object_count = 5
        shard_ranges[1].bytes_used = sum(obj['size'] for obj in objects[5:])
        shard_ranges[1].state = ShardRange.ACTIVE
        self._check_shard_range(shard_ranges[1], broker_shard_ranges[1])
        # second shard range rows were merged to shard db again
        self.assertEqual([(shard_ranges[0].name, [new_object]),
                          (shard_ranges[1].name, objects[5:])],
                         merge_items_calls)
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[0], 0),
             mock.call(0, expected_shard_dbs[1], 0)])
        # first shard broker was created by misplaced object - no sync point
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        self.assertFalse(shard_broker.get_syncs())
        self.assertEqual([new_object], shard_broker.get_objects())
        # second shard broker has sync points
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        self.assertEqual(
            [{'remote_id': retiring_db_id, 'sync_point': len(objects)}],
            shard_broker.get_syncs())
        self.assertEqual(objects[5:], shard_broker.get_objects())
        self.assertFalse(self.logger.get_lines_for_level('warning'))

    def test_shard_replication_quorum_failures(self):
        broker = self._make_broker()
        objects = [
            {'name': 'obj%03d' % i, 'created_at': next(self.ts_iter),
             'size': 1, 'content_type': 'text/plain', 'etag': 'etag',
             'deleted': 0, 'storage_policy_index': 0}
            for i in range(10)
        ]
        broker.merge_items([dict(obj) for obj in objects])
        broker._commit_puts()
        shard_bounds = (('', 'obj002'), ('obj002', 'obj004'),
                        ('obj004', 'obj006'), ('obj006', ''))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.enable_sharding(Timestamp.now())
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}
        with self._mock_sharder({'shard_replication_quorum': 3}) as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(False, [False, True, True]),
                             (False, [False, False, True])])
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)
        # replication of first shard range fails - no more shards attempted
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[0], 0)
        self.assertEqual([ShardRange.CREATED] * 4,
                         [sr.state for sr in broker.get_shard_ranges()])

        # and again with a chilled out quorom, so cleaving moves onto second
        # shard range which fails to reach even chilled quorum
        with self._mock_sharder({'shard_replication_quorum': 1}) as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(False, [False, False, True]),
                             (False, [False, False, False])])
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        self.assertEqual(sharder._replicate_object.call_args_list, [
            mock.call(0, expected_shard_dbs[0], 0),
            mock.call(0, expected_shard_dbs[1], 0),
        ])
        self.assertEqual(
            [ShardRange.CLEAVED, ShardRange.CREATED, ShardRange.CREATED,
             ShardRange.CREATED],
            [sr.state for sr in broker.get_shard_ranges()])

        # now pretend another node successfully cleaved the second shard range,
        # but this node still fails to replicate so still cannot move on
        shard_ranges[1].update_state(ShardRange.CLEAVED)
        broker.merge_shard_ranges(shard_ranges[1])
        with self._mock_sharder({'shard_replication_quorum': 1}) as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(False, [False, False, False])])
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[1], 0)
        self.assertEqual(
            [ShardRange.CLEAVED, ShardRange.CLEAVED, ShardRange.CREATED,
             ShardRange.CREATED],
            [sr.state for sr in broker.get_shard_ranges()])

        # until a super-chilled quorum is used - but even then there must have
        # been an attempt to replicate
        with self._mock_sharder(
                {'shard_replication_quorum': 1,
                 'existing_shard_replication_quorum': 0}) as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(False, [])])  # maybe shard db was deleted
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[1], 0)
        self.assertEqual(
            [ShardRange.CLEAVED, ShardRange.CLEAVED, ShardRange.CREATED,
             ShardRange.CREATED],
            [sr.state for sr in broker.get_shard_ranges()])

        # next pass - the second shard replication is attempted and fails, but
        # that's ok because another node has cleaved it and
        # existing_shard_replication_quorum is zero
        with self._mock_sharder(
                {'shard_replication_quorum': 1,
                 'existing_shard_replication_quorum': 0}) as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(False, [False, False, False]),
                             (False, [False, True, False])])
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        self.assertEqual(sharder._replicate_object.call_args_list, [
            mock.call(0, expected_shard_dbs[1], 0),
            mock.call(0, expected_shard_dbs[2], 0),
        ])
        self.assertEqual([ShardRange.CLEAVED] * 3 + [ShardRange.CREATED],
                         [sr.state for sr in broker.get_shard_ranges()])
        self.assertEqual(1, sharder.shard_replication_quorum)
        self.assertEqual(0, sharder.existing_shard_replication_quorum)

        # crazy replication quorums will be capped to replica_count
        with self._mock_sharder(
                {'shard_replication_quorum': 99,
                 'existing_shard_replication_quorum': 99}) as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(False, [False, True, True])])
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[3], 0)
        self.assertEqual([ShardRange.CLEAVED] * 3 + [ShardRange.CREATED],
                         [sr.state for sr in broker.get_shard_ranges()])
        self.assertEqual(3, sharder.shard_replication_quorum)
        self.assertEqual(3, sharder.existing_shard_replication_quorum)

        # ...and progress is still made if replication fully succeeds
        with self._mock_sharder(
                {'shard_replication_quorum': 99,
                 'existing_shard_replication_quorum': 99}) as sharder:
            sharder._replicate_object = mock.MagicMock(
                side_effect=[(True, [True, True, True])])
            sharder._audit_container = mock.MagicMock()
            sharder._process_broker(broker, node, 99)
        self.assertEqual(SHARDED, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDED,
                         broker.get_own_shard_range().state)
        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[3], 0)
        self.assertEqual([ShardRange.ACTIVE] * 4,
                         [sr.state for sr in broker.get_shard_ranges()])
        warnings = sharder.logger.get_lines_for_level('warning')
        self.assertIn(
            'shard_replication_quorum of 99 exceeds replica count',
            warnings[0])
        self.assertIn(
            'existing_shard_replication_quorum of 99 exceeds replica count',
            warnings[1])
        self.assertEqual(3, sharder.shard_replication_quorum)
        self.assertEqual(3, sharder.existing_shard_replication_quorum)

    def test_cleave_to_existing_shard_db(self):
        # verify that when cleaving to an already existing shard db
        def replicate(node, from_broker, part):
            # short circuit replication
            rpc = replicator.ContainerReplicatorRpc(
                self.tempdir, DATADIR, ContainerBroker, mount_check=False)

            fake_repl_connection = attach_fake_replication_rpc(rpc)
            with mock.patch('swift.common.db_replicator.ReplConnection',
                            fake_repl_connection):
                with mock.patch('swift.common.db_replicator.ring.Ring',
                                lambda *args, **kwargs: FakeRing()):
                    daemon = replicator.ContainerReplicator({})
                    info = from_broker.get_replication_info()
                    success = daemon._repl_to_node(
                        node, from_broker, part, info)
                    self.assertTrue(success)

        orig_merge_items = ContainerBroker.merge_items

        def mock_merge_items(broker, items):
            # capture merge_items calls
            merge_items_calls.append((broker.path,
                                      # merge mutates item so make a copy
                                      [dict(item) for item in items]))
            orig_merge_items(broker, items)

        objects = [
            {'name': 'obj%03d' % i, 'created_at': next(self.ts_iter),
             'size': 1, 'content_type': 'text/plain', 'etag': 'etag',
             'deleted': 0, 'storage_policy_index': 0}
            for i in range(10)
        ]
        # local db gets 4 objects
        local_broker = self._make_broker()
        local_broker.merge_items([dict(obj) for obj in objects[2:6]])
        local_broker._commit_puts()
        local_retiring_db_id = local_broker.get_info()['id']

        # remote db gets 5 objects
        remote_broker = self._make_broker(device='sdb')
        remote_broker.merge_items([dict(obj) for obj in objects[2:7]])
        remote_broker._commit_puts()
        remote_retiring_db_id = remote_broker.get_info()['id']

        local_node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda',
                      'id': '2', 'index': 0, 'replication_ip': '1.2.3.4',
                      'replication_port': 6040}
        remote_node = {'ip': '1.2.3.5', 'port': 6040, 'device': 'sdb',
                       'id': '3', 'index': 1, 'replication_ip': '1.2.3.5',
                       'replication_port': 6040}

        # remote db replicates to local, bringing local db's total to 5 objects
        self.assertNotEqual(local_broker.get_objects(),
                            remote_broker.get_objects())
        replicate(local_node, remote_broker, 0)
        self.assertEqual(local_broker.get_objects(),
                         remote_broker.get_objects())

        # local db gets 2 new objects, bringing its total to 7
        local_broker.merge_items([dict(obj) for obj in objects[1:2]])
        local_broker.merge_items([dict(obj) for obj in objects[7:8]])

        # local db gets shard ranges
        own_shard_range = local_broker.get_own_shard_range()
        now = Timestamp.now()
        own_shard_range.update_state(ShardRange.SHARDING, state_timestamp=now)
        own_shard_range.epoch = now
        shard_ranges = self._make_shard_ranges(
            (('', 'obj004'), ('obj004', '')), state=ShardRange.CREATED)
        local_broker.merge_shard_ranges([own_shard_range] + shard_ranges)
        self.assertTrue(local_broker.set_sharding_state())

        # local db shards
        merge_items_calls = []
        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items):
            with self._mock_sharder() as sharder:
                sharder._replicate_object = mock.MagicMock(
                    return_value=(True, [True, True, True]))
                sharder._audit_container = mock.MagicMock()
                sharder._process_broker(local_broker, local_node, 0)

        # all objects merged from local to shard ranges
        self.assertEqual([(shard_ranges[0].name, objects[1:5]),
                          (shard_ranges[1].name, objects[5:8])],
                         merge_items_calls)

        # shard brokers have sync points
        expected_shard_dbs = []
        for shard_range in shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        self.assertEqual(
            [{'remote_id': local_retiring_db_id, 'sync_point': 7},
             {'remote_id': remote_retiring_db_id, 'sync_point': 5}],
            shard_broker.get_syncs())
        self.assertEqual(objects[1:5], shard_broker.get_objects())
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        self.assertEqual(
            [{'remote_id': local_retiring_db_id, 'sync_point': 7},
             {'remote_id': remote_retiring_db_id, 'sync_point': 5}],
            shard_broker.get_syncs())
        self.assertEqual(objects[5:8], shard_broker.get_objects())

        # local db replicates to remote, so remote now has shard ranges
        # note: no objects replicated because local is sharded
        self.assertFalse(remote_broker.get_shard_ranges())
        replicate(remote_node, local_broker, 0)
        self._assert_shard_ranges_equal(local_broker.get_shard_ranges(),
                                        remote_broker.get_shard_ranges())

        # remote db gets 3 new objects, bringing its total to 8
        remote_broker.merge_items([dict(obj) for obj in objects[:1]])
        remote_broker.merge_items([dict(obj) for obj in objects[8:]])

        merge_items_calls = []
        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items):
            with self._mock_sharder() as sharder:
                sharder._replicate_object = mock.MagicMock(
                    return_value=(True, [True, True, True]))
                sharder._audit_container = mock.MagicMock()
                sharder._process_broker(remote_broker, remote_node, 0)

        # shard brokers have sync points for the remote db so only new objects
        # are merged from remote broker to shard brokers
        self.assertEqual([(shard_ranges[0].name, objects[:1]),
                          (shard_ranges[1].name, objects[8:])],
                         merge_items_calls)
        # sync points are updated
        shard_broker = ContainerBroker(expected_shard_dbs[0])
        self.assertEqual(
            [{'remote_id': local_retiring_db_id, 'sync_point': 7},
             {'remote_id': remote_retiring_db_id, 'sync_point': 8}],
            shard_broker.get_syncs())
        self.assertEqual(objects[:5], shard_broker.get_objects())
        shard_broker = ContainerBroker(expected_shard_dbs[1])
        self.assertEqual(
            [{'remote_id': local_retiring_db_id, 'sync_point': 7},
             {'remote_id': remote_retiring_db_id, 'sync_point': 8}],
            shard_broker.get_syncs())
        self.assertEqual(objects[5:], shard_broker.get_objects())

    def test_cleave_skips_shrinking_and_stops_at_found(self):
        broker = self._make_broker()
        broker.enable_sharding(Timestamp.now())
        shard_bounds = (('', 'b'),
                        ('b', 'c'),
                        ('b', 'd'),
                        ('d', 'f'),
                        ('f', ''))
        # make sure there is an object in every shard range so cleaving will
        # occur in batches of 2
        objects = [
            ('a', self.ts_encoded(), 10, 'text/plain', 'etag_a', 0, 0),
            ('b', self.ts_encoded(), 10, 'text/plain', 'etag_b', 0, 0),
            ('c', self.ts_encoded(), 1, 'text/plain', 'etag_c', 0, 0),
            ('d', self.ts_encoded(), 2, 'text/plain', 'etag_d', 0, 0),
            ('e', self.ts_encoded(), 3, 'text/plain', 'etag_e', 0, 0),
            ('f', self.ts_encoded(), 100, 'text/plain', 'etag_f', 0, 0),
            ('x', self.ts_encoded(), 0, '', '', 1, 0),  # deleted
            ('z', self.ts_encoded(), 1000, 'text/plain', 'etag_z', 0, 0)
        ]
        for obj in objects:
            broker.put_object(*obj)
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=[ShardRange.CREATED,
                                 ShardRange.SHRINKING,
                                 ShardRange.CREATED,
                                 ShardRange.CREATED,
                                 ShardRange.FOUND])
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())

        # run cleave - first batch is cleaved, shrinking range doesn't count
        # towards batch size of 2 nor towards ranges_done
        with self._mock_sharder() as sharder:
            self.assertFalse(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertEqual(shard_ranges[2].upper_str, context.cursor)
        self.assertEqual(2, context.ranges_done)
        self.assertEqual(2, context.ranges_todo)

        # run cleave - stops at shard range in FOUND state
        with self._mock_sharder() as sharder:
            self.assertFalse(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
        self.assertEqual(shard_ranges[3].upper_str, context.cursor)
        self.assertEqual(3, context.ranges_done)
        self.assertEqual(1, context.ranges_todo)

        # run cleave - final shard range in CREATED state, cleaving proceeds
        shard_ranges[4].update_state(ShardRange.CREATED,
                                     state_timestamp=Timestamp.now())
        broker.merge_shard_ranges(shard_ranges[4:])
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertEqual(shard_ranges[4].upper_str, context.cursor)
        self.assertEqual(4, context.ranges_done)
        self.assertEqual(0, context.ranges_todo)

    def test_cleave_shrinking_to_active_root_range(self):
        broker = self._make_shrinking_broker(account='.shards_a',
                                             container='shard_c')
        deleted_range = ShardRange(
            '.shards/other', next(self.ts_iter), 'here', 'there', deleted=True,
            state=ShardRange.SHRUNK, epoch=next(self.ts_iter))
        # root is the acceptor...
        root = ShardRange(
            'a/c', next(self.ts_iter), '', '',
            state=ShardRange.ACTIVE, epoch=next(self.ts_iter))
        broker.merge_shard_ranges([deleted_range, root])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(broker.is_root_container())  # sanity check

        # expect cleave to the root
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertEqual(root.upper_str, context.cursor)
        self.assertEqual(1, context.ranges_done)
        self.assertEqual(0, context.ranges_todo)

    def test_cleave_shrinking_to_active_acceptor_with_sharded_root_range(self):
        broker = self._make_broker(account='.shards_a', container='shard_c')
        broker.put_object(
            'here_a', next(self.ts_iter), 10, 'text/plain', 'etag_a', 0, 0)
        own_shard_range = ShardRange(
            broker.path, next(self.ts_iter), 'here', 'there',
            state=ShardRange.SHARDING, epoch=next(self.ts_iter))
        # the intended acceptor...
        acceptor = ShardRange(
            '.shards_a/shard_d', next(self.ts_iter), 'here', '',
            state=ShardRange.ACTIVE, epoch=next(self.ts_iter))
        # root range also gets pulled from root during audit...
        root = ShardRange(
            'a/c', next(self.ts_iter), '', '',
            state=ShardRange.SHARDED, epoch=next(self.ts_iter))
        broker.merge_shard_ranges([own_shard_range, acceptor, root])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(broker.is_root_container())  # sanity check
        self.assertTrue(broker.set_sharding_state())

        # sharded root range should always sort after an active acceptor so
        # expect cleave to acceptor first then cleaving completes
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertEqual(acceptor.upper_str, context.cursor)
        self.assertEqual(1, context.ranges_done)  # cleaved the acceptor
        self.assertEqual(1, context.ranges_todo)  # never reached sharded root

    def test_cleave_shrinking_to_active_root_range_with_active_acceptor(self):
        # if shrinking shard has both active root and active other acceptor,
        # verify that shard only cleaves to one of them;
        # root will sort before acceptor if acceptor.upper==MAX
        objects = (
            ('here_a', next(self.ts_iter), 10, 'text/plain', 'etag_a', 0, 0),)
        broker = self._make_shrinking_broker(objects=objects)
        # active acceptor with upper bound == MAX
        acceptor = ShardRange(
            '.shards/other', next(self.ts_iter), 'here', '', deleted=False,
            state=ShardRange.ACTIVE, epoch=next(self.ts_iter))
        # root is also active
        root = ShardRange(
            'a/c', next(self.ts_iter), '', '',
            state=ShardRange.ACTIVE, epoch=next(self.ts_iter))
        broker.merge_shard_ranges([acceptor, root])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(broker.is_root_container())  # sanity check

        # expect cleave to the root
        acceptor.upper = ''
        acceptor.timestamp = next(self.ts_iter)
        broker.merge_shard_ranges([acceptor])
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertEqual(root.upper_str, context.cursor)
        self.assertEqual(1, context.ranges_done)
        self.assertEqual(1, context.ranges_todo)
        info = [
            line for line in self.logger.get_lines_for_level('info')
            if line.startswith('Replicating new shard container a/c')
        ]
        self.assertEqual(1, len(info))

    def test_cleave_shrinking_to_active_acceptor_with_active_root_range(self):
        # if shrinking shard has both active root and active other acceptor,
        # verify that shard only cleaves to one of them;
        # root will sort after acceptor if acceptor.upper<MAX
        objects = (
            ('here_a', next(self.ts_iter), 10, 'text/plain', 'etag_a', 0, 0),)
        broker = self._make_shrinking_broker(objects=objects)
        # active acceptor with upper bound < MAX
        acceptor = ShardRange(
            '.shards/other', next(self.ts_iter), 'here', 'where',
            deleted=False, state=ShardRange.ACTIVE, epoch=next(self.ts_iter))
        # root is also active
        root = ShardRange(
            'a/c', next(self.ts_iter), '', '',
            state=ShardRange.ACTIVE, epoch=next(self.ts_iter))
        broker.merge_shard_ranges([acceptor, root])

        # expect cleave to the acceptor
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))
        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertEqual(acceptor.upper_str, context.cursor)
        self.assertEqual(1, context.ranges_done)
        self.assertEqual(1, context.ranges_todo)
        info = [
            line for line in self.logger.get_lines_for_level('info')
            if line.startswith('Replicating new shard container .shards/other')
        ]
        self.assertEqual(1, len(info))

    def _check_not_complete_sharding(self, broker):
        with self._mock_sharder() as sharder:
            self.assertFalse(sharder._complete_sharding(broker))
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn('Repeat cleaving required', warning_lines[0])
        self.assertFalse(warning_lines[1:])
        sharder.logger.clear()
        context = CleavingContext.load(broker)
        self.assertFalse(context.cleaving_done)
        self.assertFalse(context.misplaced_done)
        self.assertEqual('', context.cursor)
        self.assertEqual(ShardRange.SHARDING,
                         broker.get_own_shard_range().state)
        for shard_range in broker.get_shard_ranges():
            self.assertEqual(ShardRange.CLEAVED, shard_range.state)
        self.assertEqual(SHARDING, broker.get_db_state())

    def _check_complete_sharding(self, account, container, shard_bounds):
        broker = self._make_sharding_broker(
            account=account, container=container, shard_bounds=shard_bounds)
        obj = {'name': 'obj', 'created_at': next(self.ts_iter).internal,
               'size': 14, 'content_type': 'text/plain', 'etag': 'an etag',
               'deleted': 0}
        broker.get_brokers()[0].merge_items([obj])
        self.assertEqual(2, len(broker.db_files))  # sanity check

        # no cleave context progress
        self._check_not_complete_sharding(broker)

        # cleaving_done is False
        context = CleavingContext.load(broker)
        self.assertEqual(1, context.max_row)
        context.cleave_to_row = 1  # pretend all rows have been cleaved
        context.cleaving_done = False
        context.misplaced_done = True
        context.store(broker)
        self._check_not_complete_sharding(broker)

        # misplaced_done is False
        context.misplaced_done = False
        context.cleaving_done = True
        context.store(broker)
        self._check_not_complete_sharding(broker)

        # modified db max row
        old_broker = broker.get_brokers()[0]
        obj = {'name': 'obj', 'created_at': next(self.ts_iter).internal,
               'size': 14, 'content_type': 'text/plain', 'etag': 'an etag',
               'deleted': 1}
        old_broker.merge_items([obj])
        self.assertGreater(old_broker.get_max_row(), context.max_row)
        context.misplaced_done = True
        context.cleaving_done = True
        context.store(broker)
        self._check_not_complete_sharding(broker)

        # db id changes
        broker.get_brokers()[0].newid('fake_remote_id')
        context.cleave_to_row = 2  # pretend all rows have been cleaved, again
        context.store(broker)
        self._check_not_complete_sharding(broker)

        # context ok
        context = CleavingContext.load(broker)
        context.cleave_to_row = context.max_row
        context.misplaced_done = True
        context.cleaving_done = True
        context.store(broker)
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._complete_sharding(broker))
        self.assertEqual(SHARDED, broker.get_db_state())
        self.assertEqual(ShardRange.SHARDED,
                         broker.get_own_shard_range().state)
        for shard_range in broker.get_shard_ranges():
            self.assertEqual(ShardRange.ACTIVE, shard_range.state)
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertFalse(warning_lines)
        sharder.logger.clear()
        return broker

    def test_complete_sharding_root(self):
        broker = self._check_complete_sharding(
            'a', 'c', (('', 'mid'), ('mid', '')))
        self.assertEqual(0, broker.get_own_shard_range().deleted)

    def test_complete_sharding_shard(self):
        broker = self._check_complete_sharding(
            '.shards_', 'shard_c', (('l', 'mid'), ('mid', 'u')))
        self.assertEqual(1, broker.get_own_shard_range().deleted)

    def test_complete_sharding_missing_own_shard_range(self):
        broker = self._make_sharding_broker()
        obj = {'name': 'obj', 'created_at': next(self.ts_iter).internal,
               'size': 14, 'content_type': 'text/plain', 'etag': 'an etag',
               'deleted': 0}
        broker.get_brokers()[0].merge_items([obj])
        self.assertEqual(2, len(broker.db_files))  # sanity check

        # Make cleaving context_done
        context = CleavingContext.load(broker)
        self.assertEqual(1, context.max_row)
        context.cleave_to_row = 1  # pretend all rows have been cleaved
        context.cleaving_done = True
        context.misplaced_done = True
        context.store(broker)

        with self._mock_sharder() as sharder, mock.patch(
                'swift.container.backend.ContainerBroker.get_own_shard_range',
                return_value=None):
            self.assertFalse(sharder._complete_sharding(broker))
        self.assertEqual(SHARDING, broker.get_db_state())
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertEqual(warning_lines[0],
                         'Failed to get own_shard_range, path: a/c, db: %s'
                         % broker.db_file)

    def test_sharded_record_sharding_progress_missing_contexts(self):
        broker = self._check_complete_sharding(
            'a', 'c', (('', 'mid'), ('mid', '')))

        with self._mock_sharder() as sharder:
            with mock.patch.object(sharder, '_append_stat') as mocked:
                sharder._record_sharding_progress(broker, {}, None)
        mocked.assert_called_once_with('sharding_in_progress', 'all', mock.ANY)

        # clear the contexts then run _record_sharding_progress
        for context, _ in CleavingContext.load_all(broker):
            context.delete(broker)

        with self._mock_sharder() as sharder:
            with mock.patch.object(sharder, '_append_stat') as mocked:
                sharder._record_sharding_progress(broker, {}, None)
        mocked.assert_not_called()

    def test_incomplete_sharding_progress_warning_log(self):
        # test to verify sharder will print warning logs if sharding has been
        # taking too long.
        broker = self._make_sharding_broker(
            'a', 'c', (('', 'mid'), ('mid', '')))
        obj = {'name': 'obj', 'created_at': next(self.ts_iter).internal,
               'size': 14, 'content_type': 'text/plain', 'etag': 'an etag',
               'deleted': 0}
        broker.get_brokers()[0].merge_items([obj])
        self.assertEqual(2, len(broker.db_files))
        # sharding is not complete due to no cleave context progress.
        self._check_not_complete_sharding(broker)

        own_shard_range = broker.get_own_shard_range()
        # advance time but still within 'container_sharding_timeout'.
        future_time = 10000 + float(own_shard_range.epoch)
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._record_sharding_progress(broker, {}, None)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        future_time = 172800 + float(own_shard_range.epoch)
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._record_sharding_progress(broker, {}, None)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))

        # advance time beyond 'container_sharding_timeout'.
        future_time = 172800 + float(own_shard_range.epoch) + 1
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._record_sharding_progress(broker, {}, None)
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn(
            'Cleaving has not completed in %.2f seconds since %s. DB state: '
            'sharding' % (future_time - float(own_shard_range.epoch),
                          own_shard_range.epoch.isoformat),
            warning_lines[0])

    def test_incomplete_shrinking_progress_warning_log(self):
        # test to verify sharder will print warning logs if shrinking has been
        # taking too long.
        broker = self._make_shrinking_broker()
        obj = {'name': 'obj', 'created_at': next(self.ts_iter).internal,
               'size': 14, 'content_type': 'text/plain', 'etag': 'an etag',
               'deleted': 0}
        broker.get_brokers()[0].merge_items([obj])
        # active acceptor with upper bound < MAX
        acceptor = ShardRange(
            '.shards/other', next(self.ts_iter), 'here', 'where',
            deleted=False, state=ShardRange.ACTIVE, epoch=next(self.ts_iter))
        broker.merge_shard_ranges([acceptor])
        context = CleavingContext.load(broker)
        self.assertFalse(context.cleaving_done)

        own_shard_range = broker.get_own_shard_range()
        # advance time but still within 'container_sharding_timeout'.
        future_time = 10000 + float(own_shard_range.epoch)
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._record_sharding_progress(broker, {}, None)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))
        future_time = 172800 + float(own_shard_range.epoch)
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._record_sharding_progress(broker, {}, None)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))

        # advance time beyond 'container_sharding_timeout'.
        future_time = 172800 + float(own_shard_range.epoch) + 1
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._record_sharding_progress(broker, {}, None)
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn(
            'Cleaving has not completed in %.2f seconds since %s.' %
            (future_time - float(own_shard_range.epoch),
             own_shard_range.epoch.isoformat),
            warning_lines[0])

    def test_identify_sharding_old_style_candidate(self):
        brokers = [self._make_broker(container='c%03d' % i) for i in range(6)]
        for broker in brokers:
            broker.set_sharding_sysmeta('Root', 'a/c')
        node = {'index': 2}
        # containers are all empty
        with self._mock_sharder() as sharder:
            for broker in brokers:
                sharder._identify_sharding_candidate(broker, node)
        expected_stats = {}
        self._assert_stats(expected_stats, sharder, 'sharding_candidates')

        objects = [
            ['obj%3d' % i, next(self.ts_iter).internal, i, 'text/plain',
             'etag%s' % i, 0] for i in range(160)]

        # one container has 100 objects, which is below the sharding threshold
        for obj in objects[:100]:
            brokers[0].put_object(*obj)
        conf = {'recon_cache_path': self.tempdir}
        with self._mock_sharder(conf=conf) as sharder:
            for broker in brokers:
                sharder._identify_sharding_candidate(broker, node)
        self.assertFalse(sharder.sharding_candidates)
        expected_recon = {
            'found': 0,
            'top': []}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

        # reduce the sharding threshold and the container is reported
        conf = {'shard_container_threshold': 100,
                'recon_cache_path': self.tempdir}
        with self._mock_sharder(conf=conf) as sharder:
            with mock_timestamp_now() as now:
                for broker in brokers:
                    sharder._identify_sharding_candidate(broker, node)
        stats_0 = {'path': brokers[0].db_file,
                   'node_index': 2,
                   'account': 'a',
                   'container': 'c000',
                   'root': 'a/c',
                   'object_count': 100,
                   'meta_timestamp': now.internal,
                   'file_size': os.stat(brokers[0].db_file).st_size}
        self.assertEqual([stats_0], sharder.sharding_candidates)
        expected_recon = {
            'found': 1,
            'top': [stats_0]}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

    def test_identify_sharding_candidate(self):
        brokers = [self._make_broker(container='c%03d' % i) for i in range(6)]
        for broker in brokers:
            broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        node = {'index': 2}
        # containers are all empty
        with self._mock_sharder() as sharder:
            for broker in brokers:
                sharder._identify_sharding_candidate(broker, node)
        expected_stats = {}
        self._assert_stats(expected_stats, sharder, 'sharding_candidates')

        objects = [
            ['obj%3d' % i, next(self.ts_iter).internal, i, 'text/plain',
             'etag%s' % i, 0] for i in range(160)]

        # one container has 100 objects, which is below the sharding threshold
        for obj in objects[:100]:
            brokers[0].put_object(*obj)
        conf = {'recon_cache_path': self.tempdir}
        with self._mock_sharder(conf=conf) as sharder:
            for broker in brokers:
                sharder._identify_sharding_candidate(broker, node)
        self.assertFalse(sharder.sharding_candidates)
        expected_recon = {
            'found': 0,
            'top': []}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

        # reduce the sharding threshold and the container is reported
        conf = {'shard_container_threshold': 100,
                'recon_cache_path': self.tempdir}
        with self._mock_sharder(conf=conf) as sharder:
            with mock_timestamp_now() as now:
                for broker in brokers:
                    sharder._identify_sharding_candidate(broker, node)
        stats_0 = {'path': brokers[0].db_file,
                   'node_index': 2,
                   'account': 'a',
                   'container': 'c000',
                   'root': 'a/c',
                   'object_count': 100,
                   'meta_timestamp': now.internal,
                   'file_size': os.stat(brokers[0].db_file).st_size}
        self.assertEqual([stats_0], sharder.sharding_candidates)
        expected_recon = {
            'found': 1,
            'top': [stats_0]}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

        # repeat with handoff node and db_file error
        with self._mock_sharder(conf=conf) as sharder:
            with mock_timestamp_now(now):
                with mock.patch('os.stat', side_effect=OSError('test error')):
                    for broker in brokers:
                        sharder._identify_sharding_candidate(broker, {})
        stats_0_b = {'path': brokers[0].db_file,
                     'node_index': None,
                     'account': 'a',
                     'container': 'c000',
                     'root': 'a/c',
                     'object_count': 100,
                     'meta_timestamp': now.internal,
                     'file_size': None}
        self.assertEqual([stats_0_b], sharder.sharding_candidates)
        self._assert_stats(expected_stats, sharder, 'sharding_candidates')
        expected_recon = {
            'found': 1,
            'top': [stats_0_b]}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

        # load up another container, but not to threshold for sharding, and
        # verify it is never a candidate for sharding
        for obj in objects[:50]:
            brokers[2].put_object(*obj)
        own_sr = brokers[2].get_own_shard_range()
        for state in ShardRange.STATES:
            own_sr.update_state(state, state_timestamp=Timestamp.now())
            brokers[2].merge_shard_ranges([own_sr])
            with self._mock_sharder(conf=conf) as sharder:
                with mock_timestamp_now(now):
                    for broker in brokers:
                        sharder._identify_sharding_candidate(broker, node)
            with annotate_failure(state):
                self.assertEqual([stats_0], sharder.sharding_candidates)

        # reduce the threshold and the second container is included
        conf = {'shard_container_threshold': 50,
                'recon_cache_path': self.tempdir}
        own_sr.update_state(ShardRange.ACTIVE, state_timestamp=Timestamp.now())
        brokers[2].merge_shard_ranges([own_sr])
        with self._mock_sharder(conf=conf) as sharder:
            with mock_timestamp_now(now):
                for broker in brokers:
                    sharder._identify_sharding_candidate(broker, node)
        stats_2 = {'path': brokers[2].db_file,
                   'node_index': 2,
                   'account': 'a',
                   'container': 'c002',
                   'root': 'a/c',
                   'object_count': 50,
                   'meta_timestamp': now.internal,
                   'file_size': os.stat(brokers[2].db_file).st_size}
        self.assertEqual([stats_0, stats_2], sharder.sharding_candidates)
        expected_recon = {
            'found': 2,
            'top': [stats_0, stats_2]}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

        # a broker not in active state is not included
        own_sr = brokers[0].get_own_shard_range()
        for state in ShardRange.STATES:
            if state == ShardRange.ACTIVE:
                continue
            own_sr.update_state(state, state_timestamp=Timestamp.now())
            brokers[0].merge_shard_ranges([own_sr])
            with self._mock_sharder(conf=conf) as sharder:
                with mock_timestamp_now(now):
                    for broker in brokers:
                        sharder._identify_sharding_candidate(broker, node)
            with annotate_failure(state):
                self.assertEqual([stats_2], sharder.sharding_candidates)

        own_sr.update_state(ShardRange.ACTIVE, state_timestamp=Timestamp.now())
        brokers[0].merge_shard_ranges([own_sr])

        # load up a third container with 150 objects
        for obj in objects[:150]:
            brokers[5].put_object(*obj)
        with self._mock_sharder(conf=conf) as sharder:
            with mock_timestamp_now(now):
                for broker in brokers:
                    sharder._identify_sharding_candidate(broker, node)
        stats_5 = {'path': brokers[5].db_file,
                   'node_index': 2,
                   'account': 'a',
                   'container': 'c005',
                   'root': 'a/c',
                   'object_count': 150,
                   'meta_timestamp': now.internal,
                   'file_size': os.stat(brokers[5].db_file).st_size}
        self.assertEqual([stats_0, stats_2, stats_5],
                         sharder.sharding_candidates)
        # note recon top list is sorted by size
        expected_recon = {
            'found': 3,
            'top': [stats_5, stats_0, stats_2]}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

        # restrict the number of reported candidates
        conf = {'shard_container_threshold': 50,
                'recon_cache_path': self.tempdir,
                'recon_candidates_limit': 2}
        with self._mock_sharder(conf=conf) as sharder:
            with mock_timestamp_now(now):
                for broker in brokers:
                    sharder._identify_sharding_candidate(broker, node)
        self.assertEqual([stats_0, stats_2, stats_5],
                         sharder.sharding_candidates)
        expected_recon = {
            'found': 3,
            'top': [stats_5, stats_0]}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

        # unrestrict the number of reported candidates
        conf = {'shard_container_threshold': 50,
                'recon_cache_path': self.tempdir,
                'recon_candidates_limit': -1}
        for i, broker in enumerate([brokers[1]] + brokers[3:5]):
            for obj in objects[:(151 + i)]:
                broker.put_object(*obj)
        with self._mock_sharder(conf=conf) as sharder:
            with mock_timestamp_now(now):
                for broker in brokers:
                    sharder._identify_sharding_candidate(broker, node)

        stats_4 = {'path': brokers[4].db_file,
                   'node_index': 2,
                   'account': 'a',
                   'container': 'c004',
                   'root': 'a/c',
                   'object_count': 153,
                   'meta_timestamp': now.internal,
                   'file_size': os.stat(brokers[4].db_file).st_size}
        stats_3 = {'path': brokers[3].db_file,
                   'node_index': 2,
                   'account': 'a',
                   'container': 'c003',
                   'root': 'a/c',
                   'object_count': 152,
                   'meta_timestamp': now.internal,
                   'file_size': os.stat(brokers[3].db_file).st_size}
        stats_1 = {'path': brokers[1].db_file,
                   'node_index': 2,
                   'account': 'a',
                   'container': 'c001',
                   'root': 'a/c',
                   'object_count': 151,
                   'meta_timestamp': now.internal,
                   'file_size': os.stat(brokers[1].db_file).st_size}

        self.assertEqual(
            [stats_0, stats_1, stats_2, stats_3, stats_4, stats_5],
            sharder.sharding_candidates)
        self._assert_stats(expected_stats, sharder, 'sharding_candidates')
        expected_recon = {
            'found': 6,
            'top': [stats_4, stats_3, stats_1, stats_5, stats_0, stats_2]}
        sharder._report_stats()
        self._assert_recon_stats(
            expected_recon, sharder, 'sharding_candidates')

    def test_misplaced_objects_root_container(self):
        broker = self._make_broker()
        broker.enable_sharding(next(self.ts_iter))

        objects = [
            # misplaced objects in second and third shard ranges
            ['n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0, 0],
            ['there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0, 1],
            ['where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0,
             0],
            # deleted
            ['x', self.ts_encoded(), 0, '', '', 1, 1],
        ]

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', 'yonder'),
                        ('yonder', ''))
        initial_shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE)
        expected_shard_dbs = []
        for shard_range in initial_shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(initial_shard_ranges)

        # unsharded
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_not_called()
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 0, 'placed': 0, 'unplaced': 0,
                          'db_created': 0, 'db_exists': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_found'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_exists'))

        # sharding - no misplaced objects
        self.assertTrue(broker.set_sharding_state())
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_not_called()
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_found'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_exists'))

        # pretend we cleaved up to end of second shard range
        context = CleavingContext.load(broker)
        context.cursor = 'there'
        context.store(broker)
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_not_called()
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_found'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_exists'))

        # sharding - misplaced objects
        for obj in objects:
            broker.put_object(*obj)
        # pretend we have not cleaved any ranges
        context.cursor = ''
        context.store(broker)
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_not_called()
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_found'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_exists'))
        self.assertFalse(os.path.exists(expected_shard_dbs[0]))
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))
        self.assertFalse(os.path.exists(expected_shard_dbs[3]))
        self.assertFalse(os.path.exists(expected_shard_dbs[4]))

        # pretend we cleaved up to end of second shard range
        context.cursor = 'there'
        context.store(broker)
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)

        sharder._replicate_object.assert_called_once_with(
            0, expected_shard_dbs[1], 0)
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 2, 'unplaced': 0,
                          'db_created': 1, 'db_exists': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        self.assertEqual(
            2, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_placed'])
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_db_created'])
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_exists'))

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
        context.cursor = 'yonder'
        context.store(broker)
        # and some new misplaced updates arrived in the first shard range
        new_objects = [
            ['b', self.ts_encoded(), 10, 'text/plain', 'etag_b', 0, 0],
            ['c', self.ts_encoded(), 20, 'text/plain', 'etag_c', 0, 0],
        ]
        for obj in new_objects:
            broker.put_object(*obj)

        # check that *all* misplaced objects are moved despite exceeding
        # the listing limit
        with self._mock_sharder(conf={'cleave_row_batch_size': 2}) as sharder:
            sharder._move_misplaced_objects(broker)
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 4, 'unplaced': 0,
                          'db_created': 3, 'db_exists': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[2:4]],
            any_order=True
        )
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        self.assertEqual(
            4, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_placed'])
        self.assertEqual(
            3, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_db_created'])
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_exists'))

        # check misplaced objects were moved
        self._check_objects(new_objects, expected_shard_dbs[0])
        self._check_objects(objects[:2], expected_shard_dbs[1])
        self._check_objects(objects[2:3], expected_shard_dbs[2])
        self._check_objects(objects[3:], expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)
        self.assertFalse(os.path.exists(expected_shard_dbs[4]))

        # pretend we cleaved all ranges - sharded state
        self.assertTrue(broker.set_sharded_state())
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_not_called()
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 0, 'placed': 0, 'unplaced': 0,
                          'db_created': 0, 'db_exists': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_found'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_created'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_db_exists'))

        # and then more misplaced updates arrive
        newer_objects = [
            ['a-deleted', self.ts_encoded(), 51, 'text/plain', 'etag_a', 1, 0],
            ['z', self.ts_encoded(), 52, 'text/plain', 'etag_z', 0, 0],
            ['z-deleted', self.ts_encoded(), 52, 'text/plain', 'etag_z', 1, 0],
        ]
        for obj in newer_objects:
            broker.put_object(*obj)
        broker.get_info()  # force updates to be committed
        # sanity check the puts landed in sharded broker
        self._check_objects(newer_objects, broker.db_file)

        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0)
             for db in (expected_shard_dbs[0], expected_shard_dbs[-1])],
            any_order=True
        )
        # shard broker for first shard range was already created but not
        # removed due to mocked _replicate_object so expect one created and one
        # existed db stat...
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 3, 'unplaced': 0,
                          'db_created': 1, 'db_exists': 1}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        self.assertEqual(
            3, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_placed'])
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_db_created'])
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_db_exists'])

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

    def _setup_misplaced_objects(self):
        # make a broker with shard ranges, move it to sharded state and then
        # put some misplaced objects in it
        broker = self._make_broker()
        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', 'yonder'),
                        ('yonder', ''))
        initial_shard_ranges = [
            ShardRange('.shards_a/%s-%s' % (lower, upper),
                       Timestamp.now(), lower, upper, state=ShardRange.ACTIVE)
            for lower, upper in shard_bounds
        ]
        expected_dbs = []
        for shard_range in initial_shard_ranges:
            db_hash = hash_path(shard_range.account, shard_range.container)
            expected_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(initial_shard_ranges)
        objects = [
            # misplaced objects in second, third and fourth shard ranges
            ['n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0, 0],
            ['there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0, 0],
            ['where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0,
             0],
            # deleted
            ['x', self.ts_encoded(), 0, '', '', 1, 0],
        ]
        broker.enable_sharding(Timestamp.now())
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())
        for obj in objects:
            broker.put_object(*obj)
        self.assertEqual(SHARDED, broker.get_db_state())
        return broker, objects, expected_dbs

    def test_misplaced_objects_newer_objects(self):
        # verify that objects merged to the db after misplaced objects have
        # been identified are not removed from the db
        broker, objects, expected_dbs = self._setup_misplaced_objects()
        newer_objects = [
            ['j', self.ts_encoded(), 51, 'text/plain', 'etag_j', 0, 0],
            ['k', self.ts_encoded(), 52, 'text/plain', 'etag_k', 1, 0],
        ]

        calls = []
        pre_removal_objects = []

        def mock_replicate_object(part, db, node_id):
            calls.append((part, db, node_id))
            if db == expected_dbs[1]:
                # put some new objects in the shard range that is being
                # replicated before misplaced objects are removed from that
                # range in the source db
                for obj in newer_objects:
                    broker.put_object(*obj)
                    # grab a snapshot of the db contents - a side effect is
                    # that the newer objects are now committed to the db
                    pre_removal_objects.extend(
                        broker.get_objects())
            return True, [True, True, True]

        with self._mock_sharder(replicas=3) as sharder:
            sharder._replicate_object = mock_replicate_object
            sharder._move_misplaced_objects(broker)

        # sanity check - the newer objects were in the db before the misplaced
        # object were removed
        for obj in newer_objects:
            self.assertIn(obj[0], [o['name'] for o in pre_removal_objects])
        for obj in objects[:2]:
            self.assertIn(obj[0], [o['name'] for o in pre_removal_objects])

        self.assertEqual(
            set([(0, db, 0) for db in (expected_dbs[1:4])]), set(calls))

        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... but newer objects were not removed from the source db
        self._check_objects(newer_objects, broker.db_file)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 4, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')

        # they will be moved on next cycle
        unlink_files(expected_dbs)
        with self._mock_sharder(replicas=3) as sharder:
            sharder._move_misplaced_objects(broker)

        self._check_objects(newer_objects, expected_dbs[1])
        self._check_objects([], broker.db_file)
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 2, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')

    def test_misplaced_objects_db_id_changed(self):
        broker, objects, expected_dbs = self._setup_misplaced_objects()

        pre_info = broker.get_info()
        calls = []
        expected_retained_objects = []
        expected_retained_objects_dbs = []

        def mock_replicate_object(part, db, node_id):
            calls.append((part, db, node_id))
            if len(calls) == 2:
                broker.newid('fake_remote_id')
                # grab snapshot of the objects in the broker when it changed id
                expected_retained_objects.extend(
                    self._get_raw_object_records(broker))
            if len(calls) >= 2:
                expected_retained_objects_dbs.append(db)
            return True, [True, True, True]

        with self._mock_sharder(replicas=3) as sharder:
            sharder._replicate_object = mock_replicate_object
            sharder._move_misplaced_objects(broker)

        # sanity checks
        self.assertNotEqual(pre_info['id'], broker.get_info()['id'])
        self.assertTrue(expected_retained_objects)

        self.assertEqual(
            set([(0, db, 0) for db in (expected_dbs[1:4])]), set(calls))

        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... but objects were not removed after the source db id changed
        self._check_objects(expected_retained_objects, broker.db_file)
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'found': 1, 'placed': 4, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')

        lines = sharder.logger.get_lines_for_level('warning')
        shard_ranges = broker.get_shard_ranges()
        self.assertIn('Refused to remove misplaced objects for dest %s'
                      % shard_ranges[2].name, lines[0])
        self.assertIn('Refused to remove misplaced objects for dest %s'
                      % shard_ranges[3].name, lines[1])
        self.assertFalse(lines[2:])

        # they will be moved again on next cycle
        unlink_files(expected_dbs)
        sharder.logger.clear()
        with self._mock_sharder(replicas=3) as sharder:
            sharder._move_misplaced_objects(broker)

        self.assertEqual(2, len(set(expected_retained_objects_dbs)))
        for db in expected_retained_objects_dbs:
            if db == expected_dbs[1]:
                self._check_objects(objects[:2], expected_dbs[1])
            if db == expected_dbs[2]:
                self._check_objects(objects[2:3], expected_dbs[2])
            if db == expected_dbs[3]:
                self._check_objects(objects[3:], expected_dbs[3])
        self._check_objects([], broker.db_file)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': len(expected_retained_objects),
                          'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')

    def test_misplaced_objects_sufficient_replication(self):
        broker, objects, expected_dbs = self._setup_misplaced_objects()

        with self._mock_sharder(replicas=3) as sharder:
            sharder._replicate_object.return_value = (True, [True, True, True])
            sharder._move_misplaced_objects(broker)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_dbs[2:4])],
            any_order=True)
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 4, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_dbs[0]))
        self.assertFalse(os.path.exists(expected_dbs[4]))

    def test_misplaced_objects_insufficient_replication_3_replicas(self):
        broker, objects, expected_dbs = self._setup_misplaced_objects()

        returns = {expected_dbs[1]: (True, [True, True, True]),  # ok
                   expected_dbs[2]: (False, [True, False, False]),  # < quorum
                   expected_dbs[3]: (False, [False, True, True])}  # ok
        calls = []

        def mock_replicate_object(part, db, node_id):
            calls.append((part, db, node_id))
            return returns[db]

        with self._mock_sharder(replicas=3) as sharder:
            sharder._replicate_object = mock_replicate_object
            sharder._move_misplaced_objects(broker)

        self.assertEqual(
            set([(0, db, 0) for db in (expected_dbs[1:4])]), set(calls))
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'placed': 4, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        # check misplaced objects were moved to shard dbs
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... but only removed from the source db if sufficiently replicated
        self._check_objects(objects[2:3], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_dbs[0]))
        self.assertFalse(os.path.exists(expected_dbs[4]))
        lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(lines))
        self.assertIn(
            'Failed to sufficiently replicate misplaced objects shard %s'
            % broker.get_shard_ranges()[2].name, lines[0])
        self.assertIn('1 successes, 2 required', lines[0])
        self.assertIn('shard db: %s' % expected_dbs[2], lines[0])
        self.assertIn('db: %s' % broker.db_file, lines[0])

    def test_misplaced_objects_insufficient_replication_2_replicas(self):
        broker, objects, expected_dbs = self._setup_misplaced_objects()

        returns = {expected_dbs[1]: (True, [True, True]),  # ok
                   expected_dbs[2]: (False, [True, False]),  # ok
                   expected_dbs[3]: (False, [False, False])}  # < quorum>
        calls = []

        def mock_replicate_object(part, db, node_id):
            calls.append((part, db, node_id))
            return returns[db]

        with self._mock_sharder(replicas=2) as sharder:
            sharder._replicate_object = mock_replicate_object
            sharder._move_misplaced_objects(broker)

        self.assertEqual(
            set([(0, db, 0) for db in (expected_dbs[1:4])]), set(calls))
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'placed': 4, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        # check misplaced objects were moved to shard dbs
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... but only removed from the source db if sufficiently replicated
        self._check_objects(objects[3:], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_dbs[0]))
        self.assertFalse(os.path.exists(expected_dbs[4]))
        lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(lines))
        self.assertIn(
            'Failed to sufficiently replicate misplaced objects shard %s'
            % broker.get_shard_ranges()[3].name, lines[0])
        self.assertIn('0 successes, 1 required', lines[0])
        self.assertIn('shard db: %s' % expected_dbs[3], lines[0])
        self.assertIn('db: %s' % broker.db_file, lines[0])

    def test_misplaced_objects_insufficient_replication_4_replicas(self):
        broker, objects, expected_dbs = self._setup_misplaced_objects()

        returns = {expected_dbs[1]: (False, [True, False, False, False]),
                   expected_dbs[2]: (True, [True, False, False, True]),
                   expected_dbs[3]: (False, [False, False, False, False])}
        calls = []

        def mock_replicate_object(part, db, node_id):
            calls.append((part, db, node_id))
            return returns[db]

        with self._mock_sharder(replicas=4) as sharder:
            sharder._replicate_object = mock_replicate_object
            sharder._move_misplaced_objects(broker)

        self.assertEqual(
            set([(0, db, 0) for db in (expected_dbs[1:4])]), set(calls))
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'placed': 4, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        # check misplaced objects were moved to shard dbs
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... but only removed from the source db if sufficiently replicated
        self._check_objects(objects[:2] + objects[3:], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_dbs[0]))
        self.assertFalse(os.path.exists(expected_dbs[4]))

    def _check_misplaced_objects_shard_container_unsharded(self, conf=None):
        broker = self._make_broker(account='.shards_a', container='.shard_c')
        ts_shard = next(self.ts_iter)
        own_sr = ShardRange(broker.path, ts_shard, 'here', 'where')
        broker.merge_shard_ranges([own_sr])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertEqual(own_sr, broker.get_own_shard_range())  # sanity check
        self.assertEqual(UNSHARDED, broker.get_db_state())

        objects = [
            # some of these are misplaced objects
            ['b', self.ts_encoded(), 2, 'text/plain', 'etag_b', 0, 0],
            ['here', self.ts_encoded(), 2, 'text/plain', 'etag_here', 0, 0],
            ['n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0, 0],
            ['there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0, 0],
            ['x', self.ts_encoded(), 0, '', '', 1, 0],  # deleted
            ['y', self.ts_encoded(), 10, 'text/plain', 'etag_y', 0, 0],
        ]

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', ''))
        root_shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE)
        expected_shard_dbs = []
        for sr in root_shard_ranges:
            db_hash = hash_path(sr.account, sr.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        # no objects
        with self._mock_sharder(conf=conf) as sharder:
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges)
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_not_called()

        sharder._replicate_object.assert_not_called()
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 0, 'placed': 0, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_success'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_failure'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_found'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # now put objects
        for obj in objects:
            broker.put_object(*obj)
        self._check_objects(objects, broker.db_file)  # sanity check

        # NB final shard range not available
        with self._mock_sharder(conf=conf) as sharder:
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges[:-1])
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_has_calls(
            [mock.call(broker, newest=True, params={'states': 'updating',
                                                    'marker': '',
                                                    'end_marker': 'here\x00'}),
             mock.call(broker, newest=True, params={'states': 'updating',
                                                    'marker': 'where',
                                                    'end_marker': ''})])
        sharder._replicate_object.assert_called_with(
            0, expected_shard_dbs[0], 0),

        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'found': 1, 'placed': 2, 'unplaced': 2}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_success'))
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_failure'))
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        self.assertEqual(
            2, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertEqual(
            2, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
        # some misplaced objects could not be moved...
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn(
            'Failed to find destination for at least 2 misplaced objects',
            warning_lines[0])
        self.assertFalse(warning_lines[1:])
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
        with self._mock_sharder(conf=conf) as sharder:
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges)
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_has_calls(
            [mock.call(broker, newest=True, params={'states': 'updating',
                                                    'marker': 'where',
                                                    'end_marker': ''})])

        sharder._replicate_object.assert_called_with(
            0, expected_shard_dbs[-1], 0),

        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 2, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_success'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_failure'))
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        self.assertEqual(
            2, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
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
        with self._mock_sharder(conf=conf) as sharder:
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges)
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_not_called()
        sharder._replicate_object.assert_not_called()
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 0, 'placed': 0, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_success'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_failure'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_found'))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))

        # and then more misplaced updates arrive
        new_objects = [
            ['a', self.ts_encoded(), 51, 'text/plain', 'etag_a', 0, 0],
            ['z', self.ts_encoded(), 52, 'text/plain', 'etag_z', 0, 0],
        ]
        for obj in new_objects:
            broker.put_object(*obj)
        # sanity check the puts landed in sharded broker
        self._check_objects(new_objects[:1] + objects[2:4] + new_objects[1:],
                            broker.db_file)

        with self._mock_sharder(conf=conf) as sharder:
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges)
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_has_calls(
            [mock.call(broker, newest=True,
                       params={'states': 'updating',
                               'marker': '', 'end_marker': 'here\x00'}),
             mock.call(broker, newest=True, params={'states': 'updating',
                                                    'marker': 'where',
                                                    'end_marker': ''})])
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0)
             for db in (expected_shard_dbs[0], expected_shard_dbs[3])],
            any_order=True
        )
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 2, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_success'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_failure'))
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        self.assertEqual(
            2, sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_placed'))
        self.assertFalse(
            sharder.logger.statsd_client.get_stats_counts().get(
                'misplaced_unplaced'))
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
        self._check_misplaced_objects_shard_container_unsharded(
            conf={'cleave_row_batch_size': 2})

    def test_misplaced_objects_shard_container_unsharded_limit_one(self):
        self._check_misplaced_objects_shard_container_unsharded(
            conf={'cleave_row_batch_size': 1})

    def test_misplaced_objects_shard_container_sharding(self):
        broker = self._make_broker(account='.shards_a', container='shard_c')
        ts_shard = next(self.ts_iter)
        # note that own_sr spans two root shard ranges
        own_sr = ShardRange(broker.path, ts_shard, 'here', 'where')
        own_sr.update_state(ShardRange.SHARDING)
        own_sr.epoch = next(self.ts_iter)
        broker.merge_shard_ranges([own_sr])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertEqual(own_sr, broker.get_own_shard_range())  # sanity check
        self.assertEqual(UNSHARDED, broker.get_db_state())

        objects = [
            # some of these are misplaced objects
            ['b', self.ts_encoded(), 2, 'text/plain', 'etag_b', 0, 0],
            ['here', self.ts_encoded(), 2, 'text/plain', 'etag_here', 0, 0],
            ['n', self.ts_encoded(), 2, 'text/plain', 'etag_n', 0, 0],
            ['there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0, 0],
            ['v', self.ts_encoded(), 10, 'text/plain', 'etag_v', 0, 0],
            ['y', self.ts_encoded(), 10, 'text/plain', 'etag_y', 0, 0],
        ]

        shard_bounds = (('', 'here'), ('here', 'there'),
                        ('there', 'where'), ('where', ''))
        root_shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE)
        expected_shard_dbs = []
        for sr in root_shard_ranges:
            db_hash = hash_path(sr.account, sr.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))

        # pretend broker is sharding but not yet cleaved a shard
        self.assertTrue(broker.set_sharding_state())
        broker.merge_shard_ranges([dict(sr) for sr in root_shard_ranges[1:3]])
        # then some updates arrive
        for obj in objects:
            broker.put_object(*obj)
        broker.get_info()
        self._check_objects(objects, broker.db_file)  # sanity check

        # first destination is not available
        with self._mock_sharder() as sharder:
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges[1:])
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_has_calls(
            [mock.call(broker, newest=True,
                       params={'states': 'updating',
                               'marker': '', 'end_marker': 'here\x00'}),
             mock.call(broker, newest=True,
                       params={'states': 'updating',
                               'marker': 'where', 'end_marker': ''})])
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[-1], 0)],
        )
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'found': 1, 'placed': 1, 'unplaced': 2}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn(
            'Failed to find destination for at least 2 misplaced objects',
            warning_lines[0])
        self.assertFalse(warning_lines[1:])
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
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges)
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_has_calls(
            [mock.call(broker, newest=True, params={'states': 'updating',
                                                    'marker': '',
                                                    'end_marker': 'here\x00'})]
        )

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_dbs[0], 0)],
        )
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 2, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

        # check misplaced objects were moved
        self._check_objects(objects[:2], expected_shard_dbs[0])
        self._check_objects(objects[5:], expected_shard_dbs[3])
        # ... and removed from the source db
        self._check_objects(objects[2:5], broker.db_file)
        self.assertFalse(os.path.exists(expected_shard_dbs[1]))
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

        # pretend first shard has been cleaved
        context = CleavingContext.load(broker)
        context.cursor = 'there'
        context.store(broker)
        # and then more misplaced updates arrive
        new_objects = [
            ['a', self.ts_encoded(), 51, 'text/plain', 'etag_a', 0, 0],
            # this one is in the now cleaved shard range...
            ['k', self.ts_encoded(), 52, 'text/plain', 'etag_k', 0, 0],
            ['z', self.ts_encoded(), 53, 'text/plain', 'etag_z', 0, 0],
        ]
        for obj in new_objects:
            broker.put_object(*obj)
        broker.get_info()  # force updates to be committed
        # sanity check the puts landed in sharded broker
        self._check_objects(sorted(new_objects + objects[2:5]), broker.db_file)
        with self._mock_sharder() as sharder:
            sharder._fetch_shard_ranges = mock.MagicMock(
                return_value=root_shard_ranges)
            sharder._move_misplaced_objects(broker)

        sharder._fetch_shard_ranges.assert_has_calls(
            [mock.call(broker, newest=True,
                       params={'states': 'updating', 'marker': '',
                               'end_marker': 'there\x00'}),
             mock.call(broker, newest=True,
                       params={'states': 'updating', 'marker': 'where',
                               'end_marker': ''})])

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1],
                                            expected_shard_dbs[-1])],
            any_order=True
        )

        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 5, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])
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
        broker.enable_sharding(next(self.ts_iter))

        shard_bounds = (('', 'here'), ('here', ''))
        root_shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE)
        expected_shard_dbs = []
        for sr in root_shard_ranges:
            db_hash = hash_path(sr.account, sr.container)
            expected_shard_dbs.append(
                os.path.join(self.tempdir, 'sda', 'containers', '0',
                             db_hash[-3:], db_hash, db_hash + '.db'))
        broker.merge_shard_ranges(root_shard_ranges)
        self.assertTrue(broker.set_sharding_state())

        ts_older_internal = self.ts_encoded()  # used later
        # put deleted objects into source
        objects = [
            ['b', self.ts_encoded(), 0, '', '', 1, 0],
            ['x', self.ts_encoded(), 0, '', '', 1, 0]
        ]
        for obj in objects:
            broker.put_object(*obj)
        broker.get_info()
        self._check_objects(objects, broker.db_file)  # sanity check
        # pretend we cleaved all ranges - sharded state
        self.assertTrue(broker.set_sharded_state())

        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1])],
            any_order=True
        )
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 2, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])

        # check new misplaced objects were moved
        self._check_objects(objects[:1], expected_shard_dbs[0])
        self._check_objects(objects[1:], expected_shard_dbs[1])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)

        # update source db with older undeleted versions of same objects
        old_objects = [
            ['b', ts_older_internal, 2, 'text/plain', 'etag_b', 0, 0],
            ['x', ts_older_internal, 4, 'text/plain', 'etag_x', 0, 0]
        ]
        for obj in old_objects:
            broker.put_object(*obj)
        broker.get_info()
        self._check_objects(old_objects, broker.db_file)  # sanity check
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1])],
            any_order=True
        )
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])

        # check older misplaced objects were not merged to shard brokers
        self._check_objects(objects[:1], expected_shard_dbs[0])
        self._check_objects(objects[1:], expected_shard_dbs[1])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)

        # the destination shard dbs for misplaced objects may already exist so
        # check they are updated correctly when overwriting objects
        # update source db with newer deleted versions of same objects
        new_objects = [
            ['b', self.ts_encoded(), 0, '', '', 1, 0],
            ['x', self.ts_encoded(), 0, '', '', 1, 0]
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
        newer_object = ('b', ts_newer, 10, 'text/plain', 'etag_b', 0, 0)
        shard_broker.put_object(*newer_object)

        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)

        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in (expected_shard_dbs[0],
                                            expected_shard_dbs[1])],
            any_order=True
        )
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.statsd_client.get_stats_counts()[
                'misplaced_found'])

        # check only the newer misplaced object was moved
        self._check_objects([newer_object], expected_shard_dbs[0])
        self._check_objects(new_objects[1:], expected_shard_dbs[1])
        # ... and removed from the source db
        self._check_objects([], broker.db_file)

        # update source with a version of 'b' that has newer data
        # but older content-type and metadata relative to shard object
        ts_update = encode_timestamps(
            timestamps[2], timestamps[3], timestamps[4])
        update_object = ('b', ts_update, 20, 'text/ignored', 'etag_newer', 0,
                         0)
        broker.put_object(*update_object)

        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)

        ts_expected = encode_timestamps(
            timestamps[2], timestamps[3], timestamps[5])
        expected = ('b', ts_expected, 20, 'text/plain', 'etag_newer', 0, 0)
        self._check_objects([expected], expected_shard_dbs[0])
        self._check_objects([], broker.db_file)

        # update source with a version of 'b' that has older data
        # and content-type but newer metadata relative to shard object
        ts_update = encode_timestamps(
            timestamps[1], timestamps[3], timestamps[6])
        update_object = ('b', ts_update, 999, 'text/ignored', 'etag_b', 0, 0)
        broker.put_object(*update_object)

        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)

        ts_expected = encode_timestamps(
            timestamps[2], timestamps[3], timestamps[6])
        expected = ('b', ts_expected, 20, 'text/plain', 'etag_newer', 0, 0)
        self._check_objects([expected], expected_shard_dbs[0])
        self._check_objects([], broker.db_file)

        # update source with a version of 'b' that has older data
        # but newer content-type and metadata
        ts_update = encode_timestamps(
            timestamps[2], timestamps[6], timestamps[6])
        update_object = ('b', ts_update, 999, 'text/newer', 'etag_b', 0, 0)
        broker.put_object(*update_object)

        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)

        ts_expected = encode_timestamps(
            timestamps[2], timestamps[6], timestamps[6])
        expected = ('b', ts_expected, 20, 'text/newer', 'etag_newer', 0, 0)
        self._check_objects([expected], expected_shard_dbs[0])
        self._check_objects([], broker.db_file)

    def _setup_old_style_find_ranges(self, account, cont, lower, upper):
        broker = self._make_broker(account=account, container=cont)
        own_sr = ShardRange('%s/%s' % (account, cont), Timestamp.now(),
                            lower, upper)
        broker.merge_shard_ranges([own_sr])
        broker.set_sharding_sysmeta('Root', 'a/c')
        objects = [
            # some of these are misplaced objects
            ['obj%3d' % i, self.ts_encoded(), i, 'text/plain', 'etag%s' % i, 0]
            for i in range(100)]
        for obj in objects:
            broker.put_object(*obj)
        return broker, objects

    def _check_old_style_find_shard_ranges_none_found(self, broker, objects):
        with self._mock_sharder() as sharder:
            num_found = sharder._find_shard_ranges(broker)
        self.assertGreater(sharder.rows_per_shard, len(objects))
        self.assertEqual(0, num_found)
        self.assertFalse(broker.get_shard_ranges())
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'found': 0, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

        with self._mock_sharder(
                conf={'shard_container_threshold': 200}) as sharder:
            num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(sharder.rows_per_shard, len(objects))
        self.assertEqual(0, num_found)
        self.assertFalse(broker.get_shard_ranges())
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'found': 0, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

    def test_old_style_find_shard_ranges_none_found_root(self):
        broker, objects = self._setup_old_style_find_ranges('a', 'c', '', '')
        self._check_old_style_find_shard_ranges_none_found(broker, objects)

    def test_old_style_find_shard_ranges_none_found_shard(self):
        broker, objects = self._setup_old_style_find_ranges(
            '.shards_a', 'c', 'lower', 'upper')
        self._check_old_style_find_shard_ranges_none_found(broker, objects)

    def _check_old_style_find_shard_ranges_finds_two(
            self, account, cont, lower, upper):
        def check_ranges():
            self.assertEqual(2, len(broker.get_shard_ranges()))
            expected_ranges = [
                ShardRange(
                    ShardRange.make_path('.shards_a', 'c', cont, now, 0),
                    now, lower, objects[98][0], 99),
                ShardRange(
                    ShardRange.make_path('.shards_a', 'c', cont, now, 1),
                    now, objects[98][0], upper, 1),
            ]
            self._assert_shard_ranges_equal(expected_ranges,
                                            broker.get_shard_ranges())

        # first invocation finds both ranges
        broker, objects = self._setup_old_style_find_ranges(
            account, cont, lower, upper)
        with self._mock_sharder(conf={'shard_container_threshold': 199,
                                      'minimum_shard_size': 1,
                                      'shrink_threshold': 0}
                                ) as sharder:
            with mock_timestamp_now() as now:
                num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(99, sharder.rows_per_shard)
        self.assertEqual(2, num_found)
        check_ranges()
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 2, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

        # second invocation finds none
        with self._mock_sharder(conf={'shard_container_threshold': 199,
                                      'minimum_shard_size': 1,
                                      'shrink_threshold': 0}
                                ) as sharder:
            num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(0, num_found)
        self.assertEqual(2, len(broker.get_shard_ranges()))
        check_ranges()
        expected_stats = {'attempted': 0, 'success': 0, 'failure': 0,
                          'found': 0, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

    def test_old_style_find_shard_ranges_finds_two_root(self):
        self._check_old_style_find_shard_ranges_finds_two('a', 'c', '', '')

    def test_old_style_find_shard_ranges_finds_two_shard(self):
        self._check_old_style_find_shard_ranges_finds_two(
            '.shards_a', 'c_', 'l', 'u')

    def _setup_find_ranges(self, account, cont, lower, upper):
        broker = self._make_broker(account=account, container=cont)
        own_sr = ShardRange('%s/%s' % (account, cont), Timestamp.now(),
                            lower, upper)
        broker.merge_shard_ranges([own_sr])
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        objects = [
            # some of these are misplaced objects
            ['obj%3d' % i, self.ts_encoded(), i, 'text/plain', 'etag%s' % i, 0]
            for i in range(100)]
        for obj in objects:
            broker.put_object(*obj)
        return broker, objects

    def _check_find_shard_ranges_none_found(self, broker, objects):
        with self._mock_sharder() as sharder:
            num_found = sharder._find_shard_ranges(broker)
        self.assertGreater(sharder.rows_per_shard, len(objects))
        self.assertEqual(0, num_found)
        self.assertFalse(broker.get_shard_ranges())
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'found': 0, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

        with self._mock_sharder(
                conf={'shard_container_threshold': 200}) as sharder:
            num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(sharder.rows_per_shard, len(objects))
        self.assertEqual(0, num_found)
        self.assertFalse(broker.get_shard_ranges())
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'found': 0, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

    def test_find_shard_ranges_none_found_root(self):
        broker, objects = self._setup_find_ranges('a', 'c', '', '')
        self._check_find_shard_ranges_none_found(broker, objects)

    def test_find_shard_ranges_none_found_shard(self):
        broker, objects = self._setup_find_ranges(
            '.shards_a', 'c', 'lower', 'upper')
        self._check_find_shard_ranges_none_found(broker, objects)

    def _check_find_shard_ranges_finds_two(self, account, cont, lower, upper):
        def check_ranges():
            self.assertEqual(2, len(broker.get_shard_ranges()))
            expected_ranges = [
                ShardRange(
                    ShardRange.make_path('.shards_a', 'c', cont, now, 0),
                    now, lower, objects[98][0], 99),
                ShardRange(
                    ShardRange.make_path('.shards_a', 'c', cont, now, 1),
                    now, objects[98][0], upper, 1),
            ]
            self._assert_shard_ranges_equal(expected_ranges,
                                            broker.get_shard_ranges())

        # first invocation finds both ranges, sizes 99 and 1
        broker, objects = self._setup_find_ranges(
            account, cont, lower, upper)
        with self._mock_sharder(conf={'shard_container_threshold': 199,
                                      'minimum_shard_size': 1,
                                      'shrink_threshold': 0},
                                ) as sharder:
            with mock_timestamp_now() as now:
                num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(99, sharder.rows_per_shard)
        self.assertEqual(2, num_found)
        check_ranges()
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 2, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

        # second invocation finds none
        with self._mock_sharder(conf={'shard_container_threshold': 199}
                                ) as sharder:
            num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(0, num_found)
        self.assertEqual(2, len(broker.get_shard_ranges()))
        check_ranges()
        expected_stats = {'attempted': 0, 'success': 0, 'failure': 0,
                          'found': 0, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

    def test_find_shard_ranges_finds_two_root(self):
        self._check_find_shard_ranges_finds_two('a', 'c', '', '')

    def test_find_shard_ranges_finds_two_shard(self):
        self._check_find_shard_ranges_finds_two('.shards_a', 'c_', 'l', 'u')

    def _check_find_shard_ranges_finds_three(self, account, cont, lower,
                                             upper):
        broker, objects = self._setup_find_ranges(
            account, cont, lower, upper)
        now = Timestamp.now()
        expected_ranges = [
            ShardRange(
                ShardRange.make_path('.shards_a', 'c', cont, now, 0),
                now, lower, objects[44][0], 45),
            ShardRange(
                ShardRange.make_path('.shards_a', 'c', cont, now, 1),
                now, objects[44][0], objects[89][0], 45),
            ShardRange(
                ShardRange.make_path('.shards_a', 'c', cont, now, 2),
                now, objects[89][0], upper, 10),
        ]
        # first invocation finds 2 ranges
        # (third shard range will be > minimum_shard_size)
        with self._mock_sharder(
                conf={'shard_container_threshold': 90,
                      'shard_scanner_batch_size': 2,
                      'minimum_shard_size': 10}) as sharder:
            with mock_timestamp_now(now):
                num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(45, sharder.rows_per_shard)
        self.assertEqual(2, num_found)
        self.assertEqual(2, len(broker.get_shard_ranges()))
        self._assert_shard_ranges_equal(expected_ranges[:2],
                                        broker.get_shard_ranges())
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 2, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

        # second invocation finds third shard range
        with self._mock_sharder(conf={'shard_container_threshold': 90,
                                      'shard_scanner_batch_size': 2,
                                      'minimum_shard_size': 10}
                                ) as sharder:
            with mock_timestamp_now(now):
                num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(1, num_found)
        self.assertEqual(3, len(broker.get_shard_ranges()))
        self._assert_shard_ranges_equal(expected_ranges,
                                        broker.get_shard_ranges())
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

        # third invocation finds none
        with self._mock_sharder(conf={'shard_container_threshold': 199,
                                      'shard_scanner_batch_size': 2,
                                      'shrink_threshold': 0,
                                      'minimum_shard_size': 10}
                                ) as sharder:
            sharder._send_shard_ranges = mock.MagicMock(return_value=True)
            num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(0, num_found)
        self.assertEqual(3, len(broker.get_shard_ranges()))
        self._assert_shard_ranges_equal(expected_ranges,
                                        broker.get_shard_ranges())
        expected_stats = {'attempted': 0, 'success': 0, 'failure': 0,
                          'found': 0, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

    def test_find_shard_ranges_finds_three_root(self):
        self._check_find_shard_ranges_finds_three('a', 'c', '', '')

    def test_find_shard_ranges_finds_three_shard(self):
        self._check_find_shard_ranges_finds_three('.shards_a', 'c_', 'l', 'u')

    def test_find_shard_ranges_with_minimum_size(self):
        cont = 'c_'
        lower = 'l'
        upper = 'u'
        broker, objects = self._setup_find_ranges(
            '.shards_a', 'c_', lower, upper)
        now = Timestamp.now()
        expected_ranges = [
            ShardRange(
                ShardRange.make_path('.shards_a', 'c', cont, now, 0),
                now, lower, objects[44][0], 45),
            ShardRange(
                ShardRange.make_path('.shards_a', 'c', cont, now, 1),
                now, objects[44][0], upper, 55),
        ]
        # first invocation finds 2 ranges - second has been extended to avoid
        # final shard range < minimum_size
        with self._mock_sharder(
                conf={'shard_container_threshold': 90,
                      'shard_scanner_batch_size': 2,
                      'minimum_shard_size': 11}) as sharder:
            with mock_timestamp_now(now):
                num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(45, sharder.rows_per_shard)
        self.assertEqual(11, sharder.minimum_shard_size)
        self.assertEqual(2, num_found)
        self.assertEqual(2, len(broker.get_shard_ranges()))
        self._assert_shard_ranges_equal(expected_ranges[:2],
                                        broker.get_shard_ranges())
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 2, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

    def test_sharding_enabled(self):
        broker = self._make_broker()
        self.assertFalse(sharding_enabled(broker))
        # Setting sharding to a true value and sharding will be enabled
        broker.update_metadata(
            {'X-Container-Sysmeta-Sharding':
             ('yes', Timestamp.now().internal)})
        self.assertTrue(sharding_enabled(broker))

        # deleting broker doesn't clear the Sysmeta-Sharding sysmeta
        broker.delete_db(Timestamp.now().internal)
        self.assertTrue(sharding_enabled(broker))

        # re-init the DB for the deleted tests
        broker.set_storage_policy_index(0, Timestamp.now().internal)
        broker.update_metadata(
            {'X-Container-Sysmeta-Sharding':
             ('yes', Timestamp.now().internal)})
        self.assertTrue(sharding_enabled(broker))

        # if the Sysmeta-Sharding is falsy value then sharding isn't enabled
        for value in ('', 'no', 'false', 'some_fish'):
            broker.update_metadata(
                {'X-Container-Sysmeta-Sharding':
                    (value, Timestamp.now().internal)})
            self.assertFalse(sharding_enabled(broker))
        # deleting broker doesn't clear the Sysmeta-Sharding sysmeta
        broker.delete_db(Timestamp.now().internal)
        self.assertEqual(broker.metadata['X-Container-Sysmeta-Sharding'][0],
                         'some_fish')
        # so it still isn't enabled (some_fish isn't a true value).
        self.assertFalse(sharding_enabled(broker))

        # but if broker has a shard range then sharding is enabled
        broker.merge_shard_ranges(
            ShardRange('acc/a_shard', Timestamp.now(), 'l', 'u'))
        self.assertTrue(sharding_enabled(broker))

    def test_send_shard_ranges(self):
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges((('', 'h'), ('h', '')))

        def do_test(replicas, *resp_codes):
            sent_data = defaultdict(bytes)

            def on_send(fake_conn, data):
                sent_data[fake_conn] += data

            with self._mock_sharder(replicas=replicas) as sharder:
                with mocked_http_conn(*resp_codes, give_send=on_send) as conn:
                    with mock_timestamp_now() as now:
                        res = sharder._send_shard_ranges(
                            broker, 'a', 'c', shard_ranges)

            self.assertEqual(sharder.ring.replica_count, len(conn.requests))
            expected_body = json.dumps([dict(sr) for sr in shard_ranges])
            expected_body = expected_body.encode('ascii')
            expected_headers = {'Content-Type': 'application/json',
                                'Content-Length': str(len(expected_body)),
                                'X-Timestamp': now.internal,
                                'X-Backend-Record-Type': 'shard',
                                'User-Agent': mock.ANY}
            for data in sent_data.values():
                self.assertEqual(expected_body, data)
            hosts = set()
            for req in conn.requests:
                path_parts = req['path'].split('/')[1:]
                hosts.add('%s:%s/%s' % (req['ip'], req['port'], path_parts[0]))
                # FakeRing only has one partition
                self.assertEqual('0', path_parts[1])
                self.assertEqual('PUT', req['method'])
                self.assertEqual(['a', 'c'], path_parts[-2:])
                req_headers = req['headers']
                for k, v in expected_headers.items():
                    self.assertEqual(v, req_headers[k])
                self.assertTrue(
                    req_headers['User-Agent'].startswith('container-sharder'))
            self.assertEqual(sharder.ring.replica_count, len(hosts))
            return res, sharder, hosts

        replicas = 3
        res, sharder, _ = do_test(replicas, 202, 202, 202)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, 202, 202, 404)
        self.assertTrue(res)
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, 202, 202, Exception)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder, _ = do_test(replicas, 202, 404, 404)
        self.assertFalse(res)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, hosts = do_test(replicas, 500, 500, 500)
        self.assertFalse(res)
        self.assertEqual(set(
            'Failed to put shard ranges to %s a/c: 500, path: a/c, db: %s' %
            (host, broker.db_file) for host in hosts),
            set(sharder.logger.get_lines_for_level('warning')))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, Exception, Exception, 202)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder, _ = do_test(replicas, Exception, eventlet.Timeout(), 202)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])

        replicas = 2
        res, sharder, _ = do_test(replicas, 202, 202)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, 202, 404)
        self.assertTrue(res)
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, 202, Exception)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder, _ = do_test(replicas, 404, 404)
        self.assertFalse(res)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, hosts = do_test(replicas, Exception, Exception)
        self.assertFalse(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual(set(
            'Failed to put shard ranges to %s a/c: FakeStatus Error, '
            'path: a/c, db: %s: ' % (host, broker.db_file) for host in hosts),
            set(sharder.logger.get_lines_for_level('error')))
        res, sharder, _ = do_test(replicas, eventlet.Timeout(), Exception)
        self.assertFalse(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])

        replicas = 4
        res, sharder, _ = do_test(replicas, 202, 202, 202, 202)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertTrue(res)
        res, sharder, _ = do_test(replicas, 202, 202, 404, 404)
        self.assertTrue(res)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, 202, 202, Exception, Exception)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder, _ = do_test(replicas, 202, 404, 404, 404)
        self.assertFalse(res)
        self.assertEqual([True, True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, 500, 500, 500, 202)
        self.assertFalse(res)
        self.assertEqual([True, True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder, _ = do_test(replicas, Exception, Exception, 202, 404)
        self.assertFalse(res)
        self.assertEqual([True], [
            all(msg in line for msg in ('Failed to put shard ranges', '404'))
            for line in sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder, _ = do_test(
            replicas, eventlet.Timeout(), eventlet.Timeout(), 202, 404)
        self.assertFalse(res)
        self.assertEqual([True], [
            all(msg in line for msg in ('Failed to put shard ranges', '404'))
            for line in sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        self.assertEqual([True, True], [
            'path: a/c, db: %s' % broker.db_file in line for line in
            sharder.logger.get_lines_for_level('error')])

    def test_process_broker_not_sharding_no_others(self):
        # verify that sharding process will not start when own shard range is
        # missing or in wrong state or there are no other shard ranges
        broker = self._make_broker()
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}
        # sanity check
        self.assertIsNone(broker.get_own_shard_range(no_default=True))
        self.assertEqual(UNSHARDED, broker.get_db_state())

        # no own shard range
        with self._mock_sharder() as sharder:
            sharder._process_broker(broker, node, 99)
        self.assertIsNone(broker.get_own_shard_range(no_default=True))
        self.assertEqual(UNSHARDED, broker.get_db_state())
        self.assertFalse(broker.logger.get_lines_for_level('warning'))
        self.assertFalse(broker.logger.get_lines_for_level('error'))
        broker.logger.clear()

        # now add own shard range
        for state in sorted(ShardRange.STATES):
            own_sr = broker.get_own_shard_range()  # returns the default
            own_sr.update_state(state)
            broker.merge_shard_ranges([own_sr])
            with mock.patch.object(
                    broker, 'set_sharding_state') as mock_set_sharding_state:
                with self._mock_sharder() as sharder:
                    with mock_timestamp_now():
                        with mock.patch.object(sharder, '_audit_container'):
                            sharder._process_broker(broker, node, 99)
                            own_shard_range = broker.get_own_shard_range(
                                no_default=True)
            mock_set_sharding_state.assert_not_called()
            self.assertEqual(dict(own_sr), dict(own_shard_range))
            self.assertEqual(UNSHARDED, broker.get_db_state())
            self.assertFalse(broker.logger.get_lines_for_level('warning'))
            self.assertFalse(broker.logger.get_lines_for_level('error'))
            broker.logger.clear()

    def _check_process_broker_sharding_others(self, start_state, deleted):
        # verify that when existing own_shard_range has given state and there
        # are other shard ranges then the sharding process will complete
        broker = self._make_broker(hash_='hash%s%s' % (start_state, deleted))
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}
        own_sr = broker.get_own_shard_range()
        self.assertTrue(own_sr.update_state(start_state))
        epoch = next(self.ts_iter)
        own_sr.epoch = epoch
        shard_ranges = self._make_shard_ranges((('', 'm'), ('m', '')))
        broker.merge_shard_ranges([own_sr] + shard_ranges)
        if deleted:
            broker.delete_db(next(self.ts_iter).internal)

        with self._mock_sharder() as sharder:
            # pretend shard containers are created ok so sharding proceeds
            with mock.patch.object(
                    sharder, '_send_shard_ranges', return_value=True):
                with mock_timestamp_now_with_iter(self.ts_iter):
                    sharder._audit_container = mock.MagicMock()
                    sharder._process_broker(broker, node, 99)

        final_own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(SHARDED, broker.get_db_state())
        self.assertEqual(epoch.normal, parse_db_filename(broker.db_file)[1])
        lines = broker.logger.get_lines_for_level('info')
        self.assertIn('Completed creating 2 shard range containers, '
                      'path: a/c, db: %s' % broker.db_file, lines)
        self.assertFalse(broker.logger.get_lines_for_level('warning'))
        self.assertFalse(broker.logger.get_lines_for_level('error'))
        self.assertEqual(deleted, broker.is_deleted())
        return own_sr, final_own_sr

    def test_process_broker_sharding_completes_with_own_and_other_ranges(self):
        own_sr, final_own_sr = self._check_process_broker_sharding_others(
            ShardRange.SHARDING, False)
        exp_own_sr = dict(own_sr, state=ShardRange.SHARDED,
                          meta_timestamp=mock.ANY)
        self.assertEqual(exp_own_sr, dict(final_own_sr))

        # verify that deleted DBs will be sharded
        own_sr, final_own_sr = self._check_process_broker_sharding_others(
            ShardRange.SHARDING, True)
        exp_own_sr = dict(own_sr, state=ShardRange.SHARDED,
                          meta_timestamp=mock.ANY)
        self.assertEqual(exp_own_sr, dict(final_own_sr))

        own_sr, final_own_sr = self._check_process_broker_sharding_others(
            ShardRange.SHRINKING, False)
        exp_own_sr = dict(own_sr, state=ShardRange.SHRUNK,
                          meta_timestamp=mock.ANY)
        self.assertEqual(exp_own_sr, dict(final_own_sr))

        # verify that deleted DBs will be shrunk
        own_sr, final_own_sr = self._check_process_broker_sharding_others(
            ShardRange.SHRINKING, True)
        exp_own_sr = dict(own_sr, state=ShardRange.SHRUNK,
                          meta_timestamp=mock.ANY)
        self.assertEqual(exp_own_sr, dict(final_own_sr))

    def test_process_broker_not_sharding_others(self):
        # verify that sharding process will not start when own shard range is
        # missing or in wrong state even when other shard ranges are in the db
        broker = self._make_broker()
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}
        # sanity check
        self.assertIsNone(broker.get_own_shard_range(no_default=True))
        self.assertEqual(UNSHARDED, broker.get_db_state())

        # add shard ranges - but not own
        shard_ranges = self._make_shard_ranges((('', 'h'), ('h', '')))
        broker.merge_shard_ranges(shard_ranges)

        with self._mock_sharder() as sharder:
            sharder._process_broker(broker, node, 99)
        self.assertIsNone(broker.get_own_shard_range(no_default=True))
        self.assertEqual(UNSHARDED, broker.get_db_state())
        self.assertFalse(broker.logger.get_lines_for_level('warning'))
        self.assertFalse(broker.logger.get_lines_for_level('error'))
        broker.logger.clear()

        # now add own shard range
        for state in sorted(ShardRange.STATES):
            if state in (ShardRange.SHARDING,
                         ShardRange.SHRINKING,
                         ShardRange.SHARDED,
                         ShardRange.SHRUNK):
                epoch = None
            else:
                epoch = Timestamp.now()

            own_sr = broker.get_own_shard_range()  # returns the default
            own_sr.update_state(state)
            own_sr.epoch = epoch
            broker.merge_shard_ranges([own_sr])
            with self._mock_sharder() as sharder:
                with mock_timestamp_now():
                    sharder._process_broker(broker, node, 99)
                    own_shard_range = broker.get_own_shard_range(
                        no_default=True)
            self.assertEqual(dict(own_sr), dict(own_shard_range))
            self.assertEqual(UNSHARDED, broker.get_db_state())
            if epoch:
                self.assertFalse(broker.logger.get_lines_for_level('warning'))
            else:
                self.assertIn('missing epoch',
                              broker.logger.get_lines_for_level('warning')[0])
            self.assertFalse(broker.logger.get_lines_for_level('error'))
            broker.logger.clear()

    def _check_process_broker_sharding_stalls_others(self, state):
        # verify states in which own_shard_range will cause sharding
        # process to start when other shard ranges are in the db, but stop
        # when shard containers have not been created
        broker = self._make_broker(hash_='hash%s' % state)
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}
        # add shard ranges - but not own
        shard_ranges = self._make_shard_ranges((('', 'h'), ('h', '')))
        broker.merge_shard_ranges(shard_ranges)
        # sanity check
        self.assertIsNone(broker.get_own_shard_range(no_default=True))
        self.assertEqual(UNSHARDED, broker.get_db_state())

        # now set own shard range to given state and persist it
        own_sr = broker.get_own_shard_range()  # returns the default
        self.assertTrue(own_sr.update_state(state))
        epoch = Timestamp.now()
        own_sr.epoch = epoch
        broker.merge_shard_ranges([own_sr])
        with self._mock_sharder() as sharder:
            with mock_timestamp_now():
                # we're not testing rest of the process here so prevent any
                # attempt to progress shard range states
                sharder._create_shard_containers = lambda *args: 0
                sharder._process_broker(broker, node, 99)
                own_shard_range = broker.get_own_shard_range(no_default=True)

        self.assertEqual(dict(own_sr), dict(own_shard_range))
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(epoch.normal, parse_db_filename(broker.db_file)[1])
        self.assertFalse(broker.logger.get_lines_for_level('warning'))
        self.assertFalse(broker.logger.get_lines_for_level('error'))

    def test_process_broker_sharding_stalls_with_own_and_other_ranges(self):
        self._check_process_broker_sharding_stalls_others(ShardRange.SHARDING)
        self._check_process_broker_sharding_stalls_others(ShardRange.SHRINKING)
        self._check_process_broker_sharding_stalls_others(ShardRange.SHARDED)

    def test_process_broker_leader_auto_shard(self):
        # verify conditions for acting as auto-shard leader
        broker = self._make_broker(put_timestamp=next(self.ts_iter).internal)
        objects = [
            ['obj%3d' % i, self.ts_encoded(), i, 'text/plain',
             'etag%s' % i, 0] for i in range(10)]
        for obj in objects:
            broker.put_object(*obj)
        self.assertEqual(10, broker.get_info()['object_count'])
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}

        def do_process(conf):
            with self._mock_sharder(conf) as sharder:
                with mock_timestamp_now():
                    # we're not testing rest of the process here so prevent any
                    # attempt to progress shard range states
                    sharder._create_shard_containers = lambda *args: 0
                    sharder._process_broker(broker, node, 99)

        # auto shard disabled
        conf = {'shard_container_threshold': 10,
                'rows_per_shard': 5,
                'shrink_threshold': 1,
                'auto_shard': False}
        do_process(conf)
        self.assertEqual(UNSHARDED, broker.get_db_state())
        own_sr = broker.get_own_shard_range(no_default=True)
        self.assertIsNone(own_sr)

        # auto shard enabled, not node 0
        conf['auto_shard'] = True
        node['index'] = 1
        do_process(conf)
        self.assertEqual(UNSHARDED, broker.get_db_state())
        own_sr = broker.get_own_shard_range(no_default=True)
        self.assertIsNone(own_sr)

        # auto shard enabled, node 0 -> start sharding
        node['index'] = 0
        do_process(conf)
        self.assertEqual(SHARDING, broker.get_db_state())
        own_sr = broker.get_own_shard_range(no_default=True)
        self.assertIsNotNone(own_sr)
        self.assertEqual(ShardRange.SHARDING, own_sr.state)
        self.assertEqual(own_sr.epoch.normal,
                         parse_db_filename(broker.db_file)[1])
        self.assertEqual(2, len(broker.get_shard_ranges()))

    def test_process_broker_leader_auto_shard_deleted_db(self):
        # verify no auto-shard leader if broker is deleted
        conf = {'shard_container_threshold': 10,
                'rows_per_shard': 5,
                'shrink_threshold': 1,
                'auto_shard': True}
        broker = self._make_broker(put_timestamp=next(self.ts_iter).internal)
        broker.delete_db(next(self.ts_iter).internal)
        self.assertTrue(broker.is_deleted())  # sanity check
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}

        with self._mock_sharder(conf) as sharder:
            with mock_timestamp_now():
                with mock.patch.object(
                        sharder, '_find_and_enable_sharding_candidates'
                ) as mock_find_and_enable:
                    sharder._process_broker(broker, node, 99)

        self.assertEqual(UNSHARDED, broker.get_db_state())
        own_sr = broker.get_own_shard_range(no_default=True)
        self.assertIsNone(own_sr)
        # this is the only concrete assertion that verifies the leader actions
        # are not taken; no shard ranges would actually be found for an empty
        # deleted db so there's no other way to differentiate from an undeleted
        # db being processed...
        mock_find_and_enable.assert_not_called()

    def check_shard_ranges_sent(self, broker, expected_sent):
        bodies = []
        servers = []
        referers = []

        def capture_send(conn, data):
            bodies.append(data)

        def capture_connect(host, port, _method, _path, headers, *a, **kw):
            servers.append((host, port))
            referers.append(headers.get('Referer'))

        self.assertFalse(broker.get_own_shard_range().reported)  # sanity
        with self._mock_sharder() as sharder:
            with mocked_http_conn(204, 204, 204,
                                  give_send=capture_send,
                                  give_connect=capture_connect) as mock_conn:
                sharder._update_root_container(broker)

        for req in mock_conn.requests:
            self.assertEqual('PUT', req['method'])
        self.assertEqual([expected_sent] * 3,
                         [json.loads(b) for b in bodies])
        self.assertEqual(servers, [
            # NB: replication interfaces
            ('10.0.1.0', 1100),
            ('10.0.1.1', 1101),
            ('10.0.1.2', 1102),
        ])
        self.assertEqual([broker.path] * 3, referers)
        self.assertTrue(broker.get_own_shard_range().reported)

    def test_update_root_container_own_range(self):
        broker = self._make_broker()
        obj_names = []

        # nothing to send
        with self._mock_sharder() as sharder:
            with mocked_http_conn() as mock_conn:
                sharder._update_root_container(broker)
        self.assertFalse(mock_conn.requests)

        def check_only_own_shard_range_sent(state):
            own_shard_range = broker.get_own_shard_range()
            self.assertTrue(own_shard_range.update_state(
                state, state_timestamp=next(self.ts_iter)))
            broker.merge_shard_ranges([own_shard_range])
            # add an object, expect to see it reflected in the own shard range
            # that is sent
            obj_names.append(uuid4())
            broker.put_object(str(obj_names[-1]),
                              next(self.ts_iter).internal, 1, '', '')
            with mock_timestamp_now() as now:
                # check if the state if in SHARD_UPDATE_STAT_STATES
                if state in [ShardRange.CLEAVED, ShardRange.ACTIVE,
                             ShardRange.SHARDING, ShardRange.SHARDED,
                             ShardRange.SHRINKING, ShardRange.SHRUNK]:
                    exp_obj_count = len(obj_names)
                    expected_sent = [
                        dict(own_shard_range,
                             meta_timestamp=now.internal,
                             object_count=len(obj_names),
                             bytes_used=len(obj_names))]
                else:
                    exp_obj_count = own_shard_range.object_count
                    expected_sent = [
                        dict(own_shard_range)]
                self.check_shard_ranges_sent(broker, expected_sent)
                self.assertEqual(
                    exp_obj_count, broker.get_own_shard_range().object_count)

        # initialise tombstones
        with mock_timestamp_now(next(self.ts_iter)):
            own_shard_range = broker.get_own_shard_range()
            own_shard_range.update_tombstones(0)
            broker.merge_shard_ranges([own_shard_range])

        for state in ShardRange.STATES:
            with annotate_failure(state):
                check_only_own_shard_range_sent(state)

        init_obj_count = len(obj_names)

        def check_tombstones_sent(state):
            own_shard_range = broker.get_own_shard_range()
            self.assertTrue(own_shard_range.update_state(
                state, state_timestamp=next(self.ts_iter)))
            broker.merge_shard_ranges([own_shard_range])
            # delete an object, expect to see it reflected in the own shard
            # range that is sent
            broker.delete_object(str(obj_names.pop(-1)),
                                 next(self.ts_iter).internal)
            with mock_timestamp_now() as now:
                # check if the state if in SHARD_UPDATE_STAT_STATES
                if state in [ShardRange.CLEAVED, ShardRange.ACTIVE,
                             ShardRange.SHARDING, ShardRange.SHARDED,
                             ShardRange.SHRINKING, ShardRange.SHRUNK]:
                    expected_sent = [
                        dict(own_shard_range,
                             meta_timestamp=now.internal,
                             object_count=len(obj_names),
                             bytes_used=len(obj_names),
                             tombstones=init_obj_count - len(obj_names))]
                else:
                    expected_sent = [
                        dict(own_shard_range)]
                self.check_shard_ranges_sent(broker, expected_sent)

        for i, state in enumerate(ShardRange.STATES):
            with annotate_failure(state):
                check_tombstones_sent(state)

    def test_update_root_container_already_reported(self):
        broker = self._make_broker()

        def check_already_reported_not_sent(state):
            own_shard_range = broker.get_own_shard_range()

            own_shard_range.reported = True
            self.assertTrue(own_shard_range.update_state(
                state, state_timestamp=next(self.ts_iter)))
            # Check that updating state clears the flag
            self.assertFalse(own_shard_range.reported)

            # If we claim to have already updated...
            own_shard_range.reported = True
            broker.merge_shard_ranges([own_shard_range])

            # ... then there's nothing to send
            with self._mock_sharder() as sharder:
                with mocked_http_conn() as mock_conn:
                    sharder._update_root_container(broker)
            self.assertFalse(mock_conn.requests)

        # initialise tombstones
        with mock_timestamp_now(next(self.ts_iter)):
            own_shard_range = broker.get_own_shard_range()
            own_shard_range.update_tombstones(0)
            broker.merge_shard_ranges([own_shard_range])

        for state in ShardRange.STATES:
            with annotate_failure(state):
                check_already_reported_not_sent(state)

    def test_update_root_container_all_ranges(self):
        broker = self._make_broker()
        other_shard_ranges = self._make_shard_ranges((('', 'h'), ('h', '')))
        self.assertTrue(other_shard_ranges[0].set_deleted())
        broker.merge_shard_ranges(other_shard_ranges)
        obj_names = []

        # own range missing - send nothing
        with self._mock_sharder() as sharder:
            with mocked_http_conn() as mock_conn:
                sharder._update_root_container(broker)
        self.assertFalse(mock_conn.requests)

        def check_all_shard_ranges_sent(state):
            own_shard_range = broker.get_own_shard_range()
            self.assertTrue(own_shard_range.update_state(
                state, state_timestamp=next(self.ts_iter)))
            broker.merge_shard_ranges([own_shard_range])
            # add an object, expect to see it reflected in the own shard range
            # that is sent
            obj_names.append(uuid4())
            broker.put_object(str(obj_names[-1]),
                              next(self.ts_iter).internal, 1, '', '')
            with mock_timestamp_now() as now:
                shard_ranges = broker.get_shard_ranges(include_deleted=True)
                exp_own_shard_range = own_shard_range.copy()
                # check if the state if in SHARD_UPDATE_STAT_STATES
                if state in [ShardRange.CLEAVED, ShardRange.ACTIVE,
                             ShardRange.SHARDING, ShardRange.SHARDED,
                             ShardRange.SHRINKING, ShardRange.SHRUNK]:
                    exp_own_shard_range.object_count = len(obj_names)
                    exp_own_shard_range.bytes_used = len(obj_names)
                    exp_own_shard_range.meta_timestamp = now.internal
                    exp_own_shard_range.tombstones = 0
                expected_sent = sorted(
                    [exp_own_shard_range] + shard_ranges,
                    key=lambda sr: (sr.upper, sr.state, sr.lower))
                self.check_shard_ranges_sent(
                    broker, [dict(sr) for sr in expected_sent])

        for state in ShardRange.STATES.keys():
            with annotate_failure(state):
                check_all_shard_ranges_sent(state)

    def test_audit_root_container_reset_epoch(self):
        epoch = next(self.ts_iter)
        broker = self._make_broker(epoch=epoch.normal)
        shard_bounds = (('', 'j'), ('j', 'k'), ('k', 's'),
                        ('s', 'y'), ('y', ''))
        shard_ranges = self._make_shard_ranges(shard_bounds,
                                               ShardRange.ACTIVE,
                                               timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shard_ranges)
        own_shard_range = broker.get_own_shard_range()
        own_shard_range.update_state(ShardRange.SHARDED, next(self.ts_iter))
        own_shard_range.epoch = epoch
        broker.merge_shard_ranges(own_shard_range)
        with self._mock_sharder() as sharder:
            with mock.patch.object(
                    sharder, '_audit_shard_container') as mocked:
                sharder._audit_container(broker)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self._assert_stats({'attempted': 1, 'success': 1, 'failure': 0},
                           sharder, 'audit_root')
        mocked.assert_not_called()

        # test for a reset epoch
        own_shard_range = broker.get_own_shard_range()
        own_shard_range.epoch = None
        own_shard_range.state_timestamp = next(self.ts_iter)
        broker.merge_shard_ranges(own_shard_range)
        with self._mock_sharder() as sharder:
            with mock.patch.object(
                    sharder, '_audit_shard_container') as mocked:
                sharder._audit_container(broker)
        lines = sharder.logger.get_lines_for_level('warning')

        self.assertIn("own_shard_range reset to None should be %s"
                      % broker.db_epoch, lines[0])

    def test_audit_root_container(self):
        broker = self._make_broker()

        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'has_overlap': 0, 'num_overlap': 0}
        with self._mock_sharder() as sharder:
            with mock.patch.object(
                    sharder, '_audit_shard_container') as mocked:
                sharder._audit_container(broker)
        self._assert_stats(expected_stats, sharder, 'audit_root')
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        mocked.assert_not_called()

        def assert_overlap_warning(line, state_text):
            self.assertIn('Audit failed for root', line)
            self.assertIn(broker.db_file, line)
            self.assertIn(broker.path, line)
            self.assertIn(
                'overlapping ranges in state %r: k-t s-y, y-z y-z'
                % state_text, line)
            # check for no duplicates in reversed order
            self.assertNotIn('s-z k-t', line)

        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'has_overlap': 1, 'num_overlap': 2}
        shard_bounds = (('a', 'j'), ('k', 't'), ('s', 'y'),
                        ('y', 'z'), ('y', 'z'))
        for state, state_text in ShardRange.STATES.items():
            if state in (ShardRange.SHRINKING,
                         ShardRange.SHARDED,
                         ShardRange.SHRUNK):
                continue  # tested separately below
            shard_ranges = self._make_shard_ranges(
                shard_bounds, state, timestamp=next(self.ts_iter))
            broker.merge_shard_ranges(shard_ranges)
            with self._mock_sharder() as sharder:
                with mock.patch.object(
                        sharder, '_audit_shard_container') as mocked:
                    sharder._audit_container(broker)
            lines = sharder.logger.get_lines_for_level('warning')
            assert_overlap_warning(lines[0], state_text)
            self.assertFalse(lines[1:])
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            self._assert_stats(expected_stats, sharder, 'audit_root')
            mocked.assert_not_called()

        shard_ranges = self._make_shard_ranges(shard_bounds,
                                               ShardRange.SHRINKING,
                                               timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shard_ranges)
        with self._mock_sharder() as sharder:
            with mock.patch.object(
                    sharder, '_audit_shard_container') as mocked:
                sharder._audit_container(broker)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self._assert_stats({'attempted': 1, 'success': 1, 'failure': 0,
                            'has_overlap': 0, 'num_overlap': 0},
                           sharder, 'audit_root')
        mocked.assert_not_called()

        for state in (ShardRange.SHRUNK, ShardRange.SHARDED):
            shard_ranges = self._make_shard_ranges(
                shard_bounds, state, timestamp=next(self.ts_iter))
            for sr in shard_ranges:
                sr.set_deleted(Timestamp.now())
            broker.merge_shard_ranges(shard_ranges)
            with self._mock_sharder() as sharder:
                with mock.patch.object(
                        sharder, '_audit_shard_container') as mocked:
                    sharder._audit_container(broker)
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            self._assert_stats({'attempted': 1, 'success': 1, 'failure': 0,
                                'has_overlap': 0, 'num_overlap': 0},
                               sharder, 'audit_root')
            mocked.assert_not_called()

        # Put the shards back to a "useful" state
        shard_ranges = self._make_shard_ranges(shard_bounds,
                                               ShardRange.ACTIVE,
                                               timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shard_ranges)

        def assert_missing_warning(line):
            self.assertIn('Audit failed for root', line)
            self.assertIn('missing range(s): -a j-k z-', line)
            self.assertIn('path: %s, db: %s' % (broker.path, broker.db_file),
                          line)

        def check_missing():
            own_shard_range = broker.get_own_shard_range()
            states = (ShardRange.SHARDING, ShardRange.SHARDED)
            for state in states:
                own_shard_range.update_state(
                    state, state_timestamp=next(self.ts_iter))
                broker.merge_shard_ranges([own_shard_range])
                with self._mock_sharder() as sharder:
                    with mock.patch.object(
                            sharder, '_audit_shard_container') as mocked:
                        sharder._audit_container(broker)
                lines = sharder.logger.get_lines_for_level('warning')
                assert_missing_warning(lines[0])
                assert_overlap_warning(lines[0], 'active')
                self.assertFalse(lines[1:])
                self.assertFalse(sharder.logger.get_lines_for_level('error'))
                self._assert_stats(expected_stats, sharder, 'audit_root')
                mocked.assert_not_called()

        check_missing()

        # fill the gaps with shrinking shards and check that these are still
        # reported as 'missing'
        missing_shard_bounds = (('', 'a'), ('j', 'k'), ('z', ''))
        shrinking_shard_ranges = self._make_shard_ranges(
            missing_shard_bounds, ShardRange.SHRINKING,
            timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shrinking_shard_ranges)
        check_missing()

    def test_audit_root_container_with_parent_child_overlapping(self):
        # Test '_audit_root_container' when overlapping shard ranges are
        # parent and children, expect no warnings. The case of non parent-child
        # overlapping is tested in 'test_audit_root_container'.
        now_ts = next(self.ts_iter)
        past_ts = Timestamp(float(now_ts) - 604801)
        root_sr = ShardRange('a/c', past_ts, state=ShardRange.SHARDED)
        parent_range = ShardRange(ShardRange.make_path(
            '.shards_a', 'c', root_sr.container,
            past_ts, 0),
            past_ts, 'a', 'f', object_count=1,
            state=ShardRange.CLEAVED)
        child_ranges = [
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_range.container, past_ts, 0),
                past_ts, lower='a', upper='c', object_count=1,
                state=ShardRange.CLEAVED),
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_range.container, past_ts, 1),
                past_ts, lower='c', upper='f', object_count=1,
                state=ShardRange.CLEAVED)]
        self.assertTrue(find_overlapping_ranges([parent_range] + child_ranges))
        broker = self._make_broker()

        # The case of transient overlapping within reclaim_age.
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'has_overlap': 0, 'num_overlap': 0}
        broker.merge_shard_ranges([parent_range] + child_ranges)
        with mock.patch('swift.container.sharder.time.time',
                        return_value=float(now_ts) - 10):
            with self._mock_sharder() as sharder:
                with mock.patch.object(
                        sharder, '_audit_shard_container') as mocked:
                    sharder._audit_container(broker)
        self._assert_stats(expected_stats, sharder, 'audit_root')
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        mocked.assert_not_called()

        # The case of overlapping past reclaim_age.
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1,
                          'has_overlap': 1, 'num_overlap': 2}
        with mock.patch('swift.container.sharder.time.time',
                        return_value=float(now_ts)):
            with self._mock_sharder() as sharder:
                with mock.patch.object(
                        sharder, '_audit_shard_container') as mocked:
                    sharder._audit_container(broker)
        lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn('Audit failed for root', lines[0])
        self.assertFalse(lines[1:])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self._assert_stats(expected_stats, sharder, 'audit_root')
        mocked.assert_not_called()

    def test_audit_deleted_root_container(self):
        broker = self._make_broker()
        shard_bounds = (
            ('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'))
        shard_ranges = self._make_shard_ranges(shard_bounds, ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.is_root_container())
        with self._mock_sharder() as sharder:
            sharder._audit_container(broker)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))

        # delete it
        delete_ts = next(self.ts_iter)
        broker.delete_db(delete_ts.internal)
        with self._mock_sharder() as sharder:
            sharder._audit_container(broker)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))

        # advance time
        future_time = 6048000 + float(delete_ts)
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._audit_container(broker)
        self.assertEqual(
            ['Reclaimable db stuck waiting for shrinking, path: %s, db: %s'
             % (broker.path, broker.db_file)],
            self.logger.get_lines_for_level('warning'))

        # delete all shard ranges
        for sr in shard_ranges:
            sr.update_state(ShardRange.SHRUNK, Timestamp.now())
            sr.deleted = True
            sr.timestamp = Timestamp.now()
        broker.merge_shard_ranges(shard_ranges)

        # no more warning
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=future_time), self._mock_sharder() as sharder:
            sharder._audit_container(broker)
        self.assertEqual([], self.logger.get_lines_for_level('warning'))

    def call_audit_container(self, broker, shard_ranges, exc=None):
        with self._mock_sharder() as sharder:
            with mock.patch.object(sharder, '_audit_root_container') \
                    as mocked, mock.patch.object(
                        sharder, 'int_client') as mock_swift:
                mock_response = mock.MagicMock()
                mock_response.headers = {
                    'x-backend-record-type': 'shard',
                    'X-Backend-Record-Shard-Format': 'full'}
                shard_ranges.sort(key=ShardRange.sort_key)
                mock_response.body = json.dumps(
                    [dict(sr) for sr in shard_ranges])
                mock_swift.make_request.return_value = mock_response
                mock_swift.make_request.side_effect = exc
                mock_swift.make_path = (lambda a, c:
                                        '/v1/%s/%s' % (a, c))
                sharder.reclaim_age = 0
                sharder._audit_container(broker)
        mocked.assert_not_called()
        return sharder, mock_swift

    def assert_no_audit_messages(self, sharder, mock_swift,
                                 marker='k', end_marker='t'):
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        expected_headers = {'X-Backend-Record-Type': 'shard',
                            'X-Newest': 'true',
                            'X-Backend-Include-Deleted': 'True',
                            'X-Backend-Override-Deleted': 'true',
                            'X-Backend-Record-Shard-Format': 'full'}
        params = {'format': 'json', 'marker': marker, 'end_marker': end_marker,
                  'states': 'auditing'}
        mock_swift.make_request.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)

    def _do_test_audit_shard_container(self, *args):
        # include overlaps to verify correct match for updating own shard range
        broker = self._make_broker(account='.shards_a', container='shard_c')
        broker.set_sharding_sysmeta(*args)
        shard_bounds = (
            ('a', 'j'), ('k', 't'), ('k', 'u'), ('l', 'v'), ('s', 'z'))
        shard_states = (
            ShardRange.ACTIVE, ShardRange.ACTIVE, ShardRange.ACTIVE,
            ShardRange.FOUND, ShardRange.CREATED
        )
        shard_ranges = self._make_shard_ranges(shard_bounds, shard_states,
                                               timestamp=next(self.ts_iter))
        shard_ranges[1].name = broker.path
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1}

        # bad account name
        broker.account = 'bad_account'
        sharder, mock_swift = self.call_audit_container(broker, shard_ranges)
        lines = sharder.logger.get_lines_for_level('warning')
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertIn('Audit failed for shard', lines[0])
        self.assertIn('missing own shard range', lines[0])
        self.assertIn('path: %s, db: %s' % (broker.path, broker.db_file),
                      lines[0])
        self.assertIn('Audit warnings for shard', lines[1])
        self.assertIn('account not in shards namespace', lines[1])
        self.assertIn('path: %s, db: %s' % (broker.path, broker.db_file),
                      lines[1])
        self.assertNotIn('root has no matching shard range', lines[1])
        self.assertNotIn('unable to get shard ranges from root', lines[1])
        self.assertFalse(lines[2:])
        self.assertFalse(broker.is_deleted())

        # missing own shard range
        broker.get_info()
        sharder, mock_swift = self.call_audit_container(broker, shard_ranges)
        lines = sharder.logger.get_lines_for_level('warning')
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertIn('Audit failed for shard', lines[0])
        self.assertIn('missing own shard range', lines[0])
        self.assertIn('path: %s, db: %s' % (broker.path, broker.db_file),
                      lines[0])
        self.assertNotIn('unable to get shard ranges from root', lines[0])
        self.assertFalse(lines[1:])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertFalse(broker.is_deleted())

        # own shard range bounds don't match what's in root (e.g. this shard is
        # expanding to be an acceptor)
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        with mock_timestamp_now(next(self.ts_iter)):
            own_shard_range = broker.get_own_shard_range()  # get the default
        own_shard_range.lower = 'j'
        own_shard_range.upper = 'k'
        own_shard_range.name = broker.path
        broker.merge_shard_ranges([own_shard_range])
        # bump timestamp of root shard range to be newer than own
        root_ts = next(self.ts_iter)
        self.assertTrue(shard_ranges[1].update_state(ShardRange.ACTIVE,
                                                     state_timestamp=root_ts))
        shard_ranges[1].timestamp = root_ts
        with mock_timestamp_now():
            sharder, mock_swift = self.call_audit_container(
                broker, shard_ranges)
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertEqual(['Updating own shard range from root, path: '
                          '.shards_a/shard_c, db: %s' % broker.db_file],
                         sharder.logger.get_lines_for_level('debug'))
        expected = shard_ranges[1].copy()
        self.assertEqual(
            ['Updated own shard range from %s to %s, path: .shards_a/shard_c, '
             'db: %s' % (own_shard_range, expected, broker.db_file)],
            sharder.logger.get_lines_for_level('info'))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertFalse(broker.is_deleted())
        expected_headers = {'X-Backend-Record-Type': 'shard',
                            'X-Newest': 'true',
                            'X-Backend-Include-Deleted': 'True',
                            'X-Backend-Override-Deleted': 'true',
                            'X-Backend-Record-Shard-Format': 'full'}
        params = {'format': 'json', 'marker': 'j', 'end_marker': 'k',
                  'states': 'auditing'}
        mock_swift.make_request.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)
        # own shard range bounds are updated from root version
        own_shard_range = broker.get_own_shard_range()
        self.assertEqual(ShardRange.ACTIVE, own_shard_range.state)
        self.assertEqual(root_ts, own_shard_range.state_timestamp)
        self.assertEqual('k', own_shard_range.lower)
        self.assertEqual('t', own_shard_range.upper)
        # check other shard ranges from root are not merged (not shrinking)
        self.assertEqual([own_shard_range],
                         broker.get_shard_ranges(include_own=True))

        # move root version of own shard range to shrinking state
        root_ts = next(self.ts_iter)
        self.assertTrue(shard_ranges[1].update_state(ShardRange.SHRINKING,
                                                     state_timestamp=root_ts))
        # bump own shard range state timestamp so it is newer than root
        own_ts = next(self.ts_iter)
        own_shard_range = broker.get_own_shard_range()
        own_shard_range.update_state(ShardRange.ACTIVE, state_timestamp=own_ts)
        broker.merge_shard_ranges([own_shard_range])

        sharder, mock_swift = self.call_audit_container(broker, shard_ranges)
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertEqual(['Updating own shard range from root, path: '
                          '.shards_a/shard_c, db: %s' % broker.db_file],
                         sharder.logger.get_lines_for_level('debug'))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertFalse(broker.is_deleted())
        expected_headers = {'X-Backend-Record-Type': 'shard',
                            'X-Newest': 'true',
                            'X-Backend-Include-Deleted': 'True',
                            'X-Backend-Override-Deleted': 'true',
                            'X-Backend-Record-Shard-Format': 'full'}
        params = {'format': 'json', 'marker': 'k', 'end_marker': 't',
                  'states': 'auditing'}
        mock_swift.make_request.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)
        # check own shard range bounds
        own_shard_range = broker.get_own_shard_range()
        # own shard range state has not changed (root is older)
        self.assertEqual(ShardRange.ACTIVE, own_shard_range.state)
        self.assertEqual(own_ts, own_shard_range.state_timestamp)
        self.assertEqual('k', own_shard_range.lower)
        self.assertEqual('t', own_shard_range.upper)

        # reset own shard range bounds, failed response from root
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        own_shard_range = broker.get_own_shard_range()  # get the default
        own_shard_range.lower = 'j'
        own_shard_range.upper = 'k'
        own_shard_range.timestamp = next(self.ts_iter)
        broker.merge_shard_ranges([own_shard_range])
        sharder, mock_swift = self.call_audit_container(
            broker, shard_ranges,
            exc=internal_client.UnexpectedResponse('bad', 'resp'))
        lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn('Failed to get shard ranges', lines[0])
        self.assertIn('Audit warnings for shard', lines[1])
        self.assertIn('path: %s, db: %s' % (broker.path, broker.db_file),
                      lines[1])
        self.assertNotIn('account not in shards namespace', lines[1])
        self.assertNotIn('missing own shard range', lines[1])
        self.assertNotIn('root has no matching shard range', lines[1])
        self.assertIn('unable to get shard ranges from root', lines[1])
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertFalse(lines[2:])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertFalse(broker.is_deleted())
        params = {'format': 'json', 'marker': 'j', 'end_marker': 'k',
                  'states': 'auditing'}
        mock_swift.make_request.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)

        # make own shard range match one in root, but different state
        own_ts = next(self.ts_iter)
        shard_ranges[1].timestamp = own_ts
        own_shard_range = shard_ranges[1].copy()
        broker.merge_shard_ranges([own_shard_range])
        root_ts = next(self.ts_iter)
        shard_ranges[1].update_state(ShardRange.SHARDING,
                                     state_timestamp=root_ts)
        with mock_timestamp_now():
            sharder, mock_swift = self.call_audit_container(
                broker, shard_ranges)
        self.assert_no_audit_messages(sharder, mock_swift)
        self.assertFalse(broker.is_deleted())
        self.assertEqual(['Updating own shard range from root, path: '
                          '.shards_a/shard_c, db: %s' % broker.db_file],
                         sharder.logger.get_lines_for_level('debug'))
        expected = shard_ranges[1].copy()
        self.assertEqual(
            ['Updated own shard range from %s to %s, path: .shards_a/shard_c, '
             'db: %s' % (own_shard_range, expected, broker.db_file)],
            sharder.logger.get_lines_for_level('info'))
        # own shard range state is updated from root version
        own_shard_range = broker.get_own_shard_range()
        self.assertEqual(ShardRange.SHARDING, own_shard_range.state)
        self.assertEqual(root_ts, own_shard_range.state_timestamp)

        own_shard_range.update_state(ShardRange.SHARDED,
                                     state_timestamp=next(self.ts_iter))
        broker.merge_shard_ranges([own_shard_range])
        sharder, mock_swift = self.call_audit_container(broker, shard_ranges)
        self.assert_no_audit_messages(sharder, mock_swift)

        own_shard_range.deleted = 1
        own_shard_range.timestamp = next(self.ts_iter)
        broker.merge_shard_ranges([own_shard_range])
        # mocks for delete/reclaim time comparisons
        with mock_timestamp_now(next(self.ts_iter)):
            with mock.patch('swift.container.sharder.time.time',
                            lambda: float(next(self.ts_iter))):
                sharder, mock_swift = self.call_audit_container(broker,
                                                                shard_ranges)
        self.assert_no_audit_messages(sharder, mock_swift)
        self.assertTrue(broker.is_deleted())

    def test_audit_old_style_shard_container(self):
        self._do_test_audit_shard_container('Root', 'a/c')

    def test_audit_shard_container(self):
        self._do_test_audit_shard_container('Quoted-Root', 'a/c')

    def _do_test_audit_shard_container_merge_other_ranges(self, *args):
        # verify that shard only merges other ranges from root when it is
        # cleaving
        shard_bounds = (
            ('a', 'p'), ('k', 't'), ('p', 'u'))
        shard_states = (
            ShardRange.ACTIVE, ShardRange.ACTIVE, ShardRange.FOUND,
        )
        shard_ranges = self._make_shard_ranges(shard_bounds, shard_states)

        def check_audit(own_state, root_state):
            shard_container = 'shard_c_%s' % root_ts.normal
            broker = self._make_broker(account='.shards_a',
                                       container=shard_container)
            broker.set_sharding_sysmeta(*args)
            shard_ranges[1].name = broker.path

            # make shard's own shard range match shard_ranges[1]
            own_sr = shard_ranges[1]
            expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
            self.assertTrue(own_sr.update_state(own_state,
                                                state_timestamp=own_ts))
            own_sr.timestamp = own_ts
            broker.merge_shard_ranges([shard_ranges[1]])

            # bump state and timestamp of root shard_ranges[1] to be newer
            self.assertTrue(shard_ranges[1].update_state(
                root_state, state_timestamp=root_ts))
            shard_ranges[1].timestamp = root_ts
            sharder, mock_swift = self.call_audit_container(broker,
                                                            shard_ranges)
            self._assert_stats(expected_stats, sharder, 'audit_shard')
            debug_lines = sharder.logger.get_lines_for_level('debug')
            self.assertGreater(len(debug_lines), 0)
            self.assertEqual(
                'Updating own shard range from root, path: .shards_a/%s, '
                'db: %s' % (shard_container, broker.db_file),
                sharder.logger.get_lines_for_level('debug')[0])
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            self.assertFalse(broker.is_deleted())
            expected_headers = {'X-Backend-Record-Type': 'shard',
                                'X-Newest': 'true',
                                'X-Backend-Include-Deleted': 'True',
                                'X-Backend-Override-Deleted': 'true',
                                'X-Backend-Record-Shard-Format': 'full'}
            params = {'format': 'json', 'marker': 'k', 'end_marker': 't',
                      'states': 'auditing'}
            mock_swift.make_request.assert_called_once_with(
                'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
                params=params)
            return broker, shard_ranges

        # make root's copy of shard range newer than shard's local copy, so
        # shard will always update its own shard range from root, and may merge
        # other shard ranges
        for own_state in ShardRange.STATES:
            for root_state in ShardRange.STATES:
                with annotate_failure('own_state=%s, root_state=%s' %
                                      (own_state, root_state)):
                    own_ts = next(self.ts_iter)
                    root_ts = next(self.ts_iter)
                    broker, shard_ranges = check_audit(own_state, root_state)
                    # own shard range is updated from newer root version
                    own_shard_range = broker.get_own_shard_range()
                    self.assertEqual(root_state, own_shard_range.state)
                    self.assertEqual(root_ts, own_shard_range.state_timestamp)
                    updated_ranges = broker.get_shard_ranges(include_own=True)
                    if root_state in ShardRange.CLEAVING_STATES:
                        # check other shard ranges from root are merged
                        self.assertEqual(shard_ranges, updated_ranges)
                    else:
                        # check other shard ranges from root are not merged
                        self.assertEqual(shard_ranges[1:2], updated_ranges)

        # make root's copy of shard range older than shard's local copy, so
        # shard will never update its own shard range from root, but may merge
        # other shard ranges
        for own_state in ShardRange.STATES:
            for root_state in ShardRange.STATES:
                with annotate_failure('own_state=%s, root_state=%s' %
                                      (own_state, root_state)):
                    root_ts = next(self.ts_iter)
                    own_ts = next(self.ts_iter)
                    broker, shard_ranges = check_audit(own_state, root_state)
                    # own shard range is not updated from older root version
                    own_shard_range = broker.get_own_shard_range()
                    self.assertEqual(own_state, own_shard_range.state)
                    self.assertEqual(own_ts, own_shard_range.state_timestamp)
                    updated_ranges = broker.get_shard_ranges(include_own=True)
                    if own_state in ShardRange.CLEAVING_STATES:
                        # check other shard ranges from root are merged
                        self.assertEqual(shard_ranges, updated_ranges)
                    else:
                        # check other shard ranges from root are not merged
                        self.assertEqual(shard_ranges[1:2], updated_ranges)

    def test_audit_old_style_shard_container_merge_other_ranges(self):
        self._do_test_audit_shard_container_merge_other_ranges('Root', 'a/c')

    def test_audit_shard_container_merge_other_ranges(self):
        self._do_test_audit_shard_container_merge_other_ranges('Quoted-Root',
                                                               'a/c')

    def _assert_merge_into_shard(self, own_shard_range, shard_ranges,
                                 root_shard_ranges, expected, *args, **kwargs):
        # create a shard broker, initialise with shard_ranges, run audit on it
        # supplying given root_shard_ranges and verify that the broker ends up
        # with expected shard ranges.
        broker = self._make_broker(account=own_shard_range.account,
                                   container=own_shard_range.container)
        broker.set_sharding_sysmeta(*args)
        broker.merge_shard_ranges([own_shard_range] + shard_ranges)
        db_state = kwargs.get('db_state', UNSHARDED)
        if db_state == SHARDING:
            broker.set_sharding_state()
        if db_state == SHARDED:
            broker.set_sharding_state()
            broker.set_sharded_state()
        self.assertEqual(db_state, broker.get_db_state())
        self.assertFalse(broker.is_root_container())

        sharder, mock_swift = self.call_audit_container(
            broker, root_shard_ranges)
        expected_headers = {'X-Backend-Record-Type': 'shard',
                            'X-Newest': 'true',
                            'X-Backend-Include-Deleted': 'True',
                            'X-Backend-Override-Deleted': 'true',
                            'X-Backend-Record-Shard-Format': 'full'}
        params = {'format': 'json', 'marker': 'a', 'end_marker': 'b',
                  'states': 'auditing'}
        mock_swift.make_request.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)

        self._assert_shard_ranges_equal(expected, broker.get_shard_ranges())
        self.assertEqual(own_shard_range,
                         broker.get_own_shard_range(no_default=True))
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        return sharder

    def _do_test_audit_shard_root_ranges_not_merged(self, *args):
        # Make root and other ranges that fully contain the shard namespace...
        root_own_sr = ShardRange('a/c', next(self.ts_iter))
        acceptor = ShardRange(
            str(ShardName.create('.shards_a', 'c', 'c',
                                 next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c')

        def do_test(own_state, acceptor_state, root_state):
            acceptor_from_root = acceptor.copy(
                timestamp=next(self.ts_iter), state=acceptor_state)
            root_from_root = root_own_sr.copy(
                timestamp=next(self.ts_iter), state=root_state)
            with annotate_failure('with states %s %s %s'
                                  % (own_state, acceptor_state, root_state)):
                own_sr_name = ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)
                own_sr = ShardRange(
                    str(own_sr_name), next(self.ts_iter), state=own_state,
                    state_timestamp=next(self.ts_iter), lower='a', upper='b')
                expected = existing = []
                sharder = self._assert_merge_into_shard(
                    own_sr, existing,
                    [own_sr, acceptor_from_root, root_from_root],
                    expected, *args)
                self.assertFalse(sharder.logger.get_lines_for_level('warning'))
                self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.STATES:
            if own_state in ShardRange.CLEAVING_STATES:
                # cleaving states are covered by other tests
                continue
            for acceptor_state in ShardRange.STATES:
                for root_state in ShardRange.STATES:
                    do_test(own_state, acceptor_state, root_state)

    def test_audit_old_style_shard_root_ranges_not_merged_not_cleaving(self):
        # verify that other shard ranges from root are NOT merged into shard
        # when it is NOT in a cleaving state
        self._do_test_audit_shard_root_ranges_not_merged('Root', 'a/c')

    def test_audit_shard_root_ranges_not_merged_not_cleaving(self):
        # verify that other shard ranges from root are NOT merged into shard
        # when it is NOT in a cleaving state
        self._do_test_audit_shard_root_ranges_not_merged('Quoted-Root', 'a/c')

    def test_audit_shard_root_ranges_with_own_merged_while_shrinking(self):
        # Verify that shrinking shard will merge other ranges, but not
        # in-ACTIVE root range.
        # Make root and other ranges that fully contain the shard namespace...
        root_own_sr = ShardRange('a/c', next(self.ts_iter))
        acceptor = ShardRange(
            str(ShardName.create('.shards_a', 'c', 'c',
                                 next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c')

        def do_test(own_state, acceptor_state, root_state):
            acceptor_from_root = acceptor.copy(
                timestamp=next(self.ts_iter), state=acceptor_state)
            root_from_root = root_own_sr.copy(
                timestamp=next(self.ts_iter), state=root_state)
            ts = next(self.ts_iter)
            own_sr = ShardRange(
                str(ShardName.create('.shards_a', 'c', 'c', ts, 0)),
                ts, lower='a', upper='b', state=own_state, state_timestamp=ts)
            expected = [acceptor_from_root]
            with annotate_failure('with states %s %s %s'
                                  % (own_state, acceptor_state, root_state)):
                sharder = self._assert_merge_into_shard(
                    own_sr, [],
                    # own sr is in ranges fetched from root
                    [own_sr, acceptor_from_root, root_from_root],
                    expected, 'Quoted-Root', 'a/c')
                self.assertFalse(sharder.logger.get_lines_for_level('warning'))
                self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.SHRINKING_STATES:
            for acceptor_state in ShardRange.STATES:
                if acceptor_state in ShardRange.CLEAVING_STATES:
                    # special case covered in other tests
                    continue
                for root_state in ShardRange.STATES:
                    if root_state == ShardRange.ACTIVE:
                        # special case: ACTIVE root *is* merged
                        continue
                    with annotate_failure(
                            'with states %s %s %s'
                            % (own_state, acceptor_state, root_state)):
                        do_test(own_state, acceptor_state, root_state)

    def test_audit_shard_root_ranges_missing_own_merged_while_shrinking(self):
        # Verify that shrinking shard will merge other ranges, but not
        # in-ACTIVE root range, even when root does not have shard's own range.
        # Make root and other ranges that fully contain the shard namespace...
        root_own_sr = ShardRange('a/c', next(self.ts_iter))
        acceptor = ShardRange(
            str(ShardName.create('.shards_a', 'c', 'c',
                                 next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c')

        def do_test(own_state, acceptor_state, root_state):
            acceptor_from_root = acceptor.copy(
                timestamp=next(self.ts_iter), state=acceptor_state)
            root_from_root = root_own_sr.copy(
                timestamp=next(self.ts_iter), state=root_state)
            ts = next(self.ts_iter)
            own_sr = ShardRange(
                str(ShardName.create('.shards_a', 'c', 'c', ts, 0)),
                ts, lower='a', upper='b', state=own_state, state_timestamp=ts)
            expected = [acceptor_from_root]
            with annotate_failure('with states %s %s %s'
                                  % (own_state, acceptor_state, root_state)):
                sharder = self._assert_merge_into_shard(
                    own_sr, [],
                    # own sr is NOT in ranges fetched from root
                    [acceptor_from_root, root_from_root],
                    expected, 'Quoted-Root', 'a/c')
                warning_lines = sharder.logger.get_lines_for_level('warning')
                self.assertEqual(1, len(warning_lines))
                self.assertIn('root has no matching shard range',
                              warning_lines[0])
                self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.SHRINKING_STATES:
            for acceptor_state in ShardRange.STATES:
                if acceptor_state in ShardRange.CLEAVING_STATES:
                    # special case covered in other tests
                    continue
                for root_state in ShardRange.STATES:
                    if root_state == ShardRange.ACTIVE:
                        # special case: ACTIVE root *is* merged
                        continue
                    with annotate_failure(
                            'with states %s %s %s'
                            % (own_state, acceptor_state, root_state)):
                        do_test(own_state, acceptor_state, root_state)

    def test_audit_shard_root_range_not_merged_while_shrinking(self):
        # Verify that shrinking shard will not merge an in-active root range
        def do_test(own_state, root_state):
            root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                     state=ShardRange.SHARDED)
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            expected = []
            sharder = self._assert_merge_into_shard(
                own_sr, [], [own_sr, root_own_sr],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.SHRINKING_STATES:
            for root_state in ShardRange.STATES:
                if root_state == ShardRange.ACTIVE:
                    continue  # special case tested below
                with annotate_failure((own_state, root_state)):
                    do_test(own_state, root_state)

    def test_audit_shard_root_range_overlap_not_merged_while_shrinking(self):
        # Verify that shrinking shard will not merge an active root range that
        # overlaps with an exosting sub-shard
        def do_test(own_state):
            root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                     state=ShardRange.ACTIVE)
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            ts = next(self.ts_iter)
            sub_shard = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', own_sr.container, ts, 0)),
                ts, lower='a', upper='ab', state=ShardRange.ACTIVE)
            expected = [sub_shard]
            sharder = self._assert_merge_into_shard(
                own_sr, [sub_shard], [own_sr, root_own_sr],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.SHRINKING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_active_root_range_merged_while_shrinking(self):
        # Verify that shrinking shard will merge an active root range
        def do_test(own_state):
            root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                     state=ShardRange.ACTIVE)
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            expected = [root_own_sr]
            sharder = self._assert_merge_into_shard(
                own_sr, [], [own_sr, root_own_sr],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.SHRINKING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_root_ranges_fetch_fails_while_shrinking(self):
        # check audit copes with failed response while shard is shrinking
        broker = self._make_shrinking_broker(lower='a', upper='b')
        own_sr = broker.get_own_shard_range()
        sharder, mock_swift = self.call_audit_container(
            broker, [], exc=internal_client.UnexpectedResponse('bad', 'resp'))
        self.assertEqual([], broker.get_shard_ranges())
        self.assertEqual(own_sr, broker.get_own_shard_range(no_default=True))
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        warning_lines = sharder.logger.get_lines_for_level('warning')
        self.assertEqual(2, len(warning_lines))
        self.assertIn('Failed to get shard ranges from a/c: bad',
                      warning_lines[0])
        self.assertIn('unable to get shard ranges from root',
                      warning_lines[1])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))

    def test_audit_shard_root_ranges_merge_while_unsharded(self):
        # Verify that unsharded shard with no existing shard ranges will merge
        # other ranges, but not root range.
        root_own_sr = ShardRange('a/c', next(self.ts_iter))
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)

        def do_test(own_state, acceptor_state, root_state):
            acceptor_from_root = acceptor.copy(
                timestamp=next(self.ts_iter), state=acceptor_state)
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            root_from_root = root_own_sr.copy(
                timestamp=next(self.ts_iter), state=root_state)
            expected = [acceptor_from_root]
            sharder = self._assert_merge_into_shard(
                own_sr, [],
                [own_sr, acceptor_from_root, root_from_root],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.SHARDING_STATES:
            for acceptor_state in ShardRange.STATES:
                if acceptor_state in ShardRange.CLEAVING_STATES:
                    # special case covered in other tests
                    continue
                for root_state in ShardRange.STATES:
                    with annotate_failure(
                            'with states %s %s %s'
                            % (own_state, acceptor_state, root_state)):
                        do_test(own_state, acceptor_state, root_state)

    def test_audit_shard_root_ranges_merge_while_sharding(self):
        # Verify that sharding shard with no existing shard ranges will merge
        # other ranges, but not root range.
        root_own_sr = ShardRange('a/c', next(self.ts_iter))
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)

        def do_test(own_state, acceptor_state, root_state):
            acceptor_from_root = acceptor.copy(
                timestamp=next(self.ts_iter), state=acceptor_state)
            ts = next(self.ts_iter)
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', ts, 0)),
                ts, 'a', 'b', epoch=ts, state=own_state)
            root_from_root = root_own_sr.copy(
                timestamp=next(self.ts_iter), state=root_state)
            expected = [acceptor_from_root]
            sharder = self._assert_merge_into_shard(
                own_sr, [],
                [own_sr, acceptor_from_root, root_from_root],
                expected, 'Quoted-Root', 'a/c', db_state=SHARDING)
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.SHARDING_STATES:
            for acceptor_state in ShardRange.STATES:
                if acceptor_state in ShardRange.CLEAVING_STATES:
                    # special case covered in other tests
                    continue
                for root_state in ShardRange.STATES:
                    with annotate_failure(
                            'with states %s %s %s'
                            % (own_state, acceptor_state, root_state)):
                        do_test(own_state, acceptor_state, root_state)

    def test_audit_shard_root_ranges_not_merged_once_sharded(self):
        # Verify that sharded shard will not merge other ranges from root
        root_own_sr = ShardRange('a/c', next(self.ts_iter))
        # the acceptor complements the single existing sub-shard...
        other_sub_shard = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'ab', 'c', state=ShardRange.ACTIVE)

        def do_test(own_state, other_sub_shard_state, root_state):
            sub_shard_from_root = other_sub_shard.copy(
                timestamp=next(self.ts_iter), state=other_sub_shard_state)
            ts = next(self.ts_iter)
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', ts, 0)),
                ts, 'a', 'b', epoch=ts, state=own_state)
            ts = next(self.ts_iter)
            sub_shard = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', own_sr.container, ts, 0)),
                ts, lower='a', upper='ab', state=ShardRange.ACTIVE)
            root_from_root = root_own_sr.copy(
                timestamp=next(self.ts_iter), state=root_state)
            expected = [sub_shard]
            sharder = self._assert_merge_into_shard(
                own_sr, [sub_shard],
                [own_sr, sub_shard_from_root, root_from_root],
                expected, 'Quoted-Root', 'a/c', db_state=SHARDED)
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in (ShardRange.SHARDED, ShardRange.SHRUNK):
            for other_sub_shard_state in ShardRange.STATES:
                for root_state in ShardRange.STATES:
                    with annotate_failure(
                            'with states %s %s %s'
                            % (own_state, other_sub_shard_state, root_state)):
                        do_test(own_state, other_sub_shard_state, root_state)

    def test_audit_shard_root_ranges_replace_existing_while_cleaving(self):
        # Verify that sharding shard with stale existing sub-shard ranges will
        # merge other ranges, but not root range.
        root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)
        ts = next(self.ts_iter)
        acceptor_sub_shards = [ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', acceptor.container, ts, i)),
            ts, lower, upper, state=ShardRange.ACTIVE)
            for i, lower, upper in ((0, 'a', 'ab'), (1, 'ab', 'c'))]

        # shard has incomplete existing shard ranges, ranges from root delete
        # existing sub-shard and replace with other acceptor sub-shards
        def do_test(own_state):
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            ts = next(self.ts_iter)
            sub_shard = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', own_sr.container, ts, 0)),
                ts, lower='a', upper='ab', state=ShardRange.ACTIVE)
            deleted_sub_shard = sub_shard.copy(
                timestamp=next(self.ts_iter), state=ShardRange.SHARDED,
                deleted=1)
            expected = acceptor_sub_shards
            sharder = self._assert_merge_into_shard(
                own_sr, [sub_shard],
                [root_own_sr, own_sr, deleted_sub_shard] + acceptor_sub_shards,
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.CLEAVING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_root_ranges_supplement_deleted_while_cleaving(self):
        # Verify that sharding shard with deleted existing sub-shard ranges
        # will merge other ranges while sharding, but not root range.
        root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)
        ts = next(self.ts_iter)
        acceptor_sub_shards = [ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', acceptor.container, ts, i)),
            ts, lower, upper, state=ShardRange.ACTIVE)
            for i, lower, upper in ((0, 'a', 'ab'), (1, 'ab', 'c'))]

        # shard already has deleted existing shard ranges
        expected = acceptor_sub_shards

        def do_test(own_state):
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            ts = next(self.ts_iter)
            deleted_sub_shards = [ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', own_sr.container, ts, i)),
                ts, lower, upper, state=ShardRange.SHARDED, deleted=1)
                for i, lower, upper in ((0, 'a', 'ab'), (1, 'ab', 'b'))]
            sharder = self._assert_merge_into_shard(
                own_sr, deleted_sub_shards,
                [own_sr, root_own_sr] + acceptor_sub_shards,
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.CLEAVING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_root_ranges_supplement_existing_while_cleaving(self):
        # Verify that sharding shard with incomplete existing sub-shard ranges
        # will merge other ranges that fill the gap, but not root range.
        root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)
        ts = next(self.ts_iter)
        acceptor_sub_shards = [ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', acceptor.container, ts, i)),
            ts, lower, upper, state=ShardRange.ACTIVE)
            for i, lower, upper in ((0, 'a', 'ab'), (1, 'ab', 'c'))]

        # shard has incomplete existing shard ranges and range from root fills
        # the gap
        def do_test(own_state):
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            ts = next(self.ts_iter)
            sub_shard = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', own_sr.container, ts, 0)),
                ts, lower='a', upper='ab', state=ShardRange.ACTIVE)
            expected = [sub_shard] + acceptor_sub_shards[1:]
            sharder = self._assert_merge_into_shard(
                own_sr, [sub_shard],
                [own_sr, root_own_sr] + acceptor_sub_shards[1:],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.CLEAVING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_root_ranges_cleaving_not_merged_while_cleaving(self):
        # Verify that sharding shard will not merge other ranges that are in a
        # cleaving state.
        root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)

        def do_test(own_state, acceptor_state, root_state):
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            root_from_root = root_own_sr.copy(
                timestamp=next(self.ts_iter), state=root_state)
            acceptor_from_root = acceptor.copy(
                timestamp=next(self.ts_iter), state=acceptor_state)

            if (own_state in ShardRange.SHRINKING_STATES and
                    root_state == ShardRange.ACTIVE):
                # special case: when shrinking, ACTIVE root shard *is* merged
                expected = [root_from_root]
            else:
                expected = []

            sharder = self._assert_merge_into_shard(
                own_sr, [],
                [own_sr, acceptor_from_root, root_from_root],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        # ranges from root that are in a cleaving state are not merged...
        for own_state in ShardRange.CLEAVING_STATES:
            for acceptor_state in ShardRange.CLEAVING_STATES:
                for root_state in ShardRange.STATES:
                    with annotate_failure(
                            'with states %s %s %s'
                            % (own_state, acceptor_state, root_state)):
                        do_test(own_state, acceptor_state, root_state)

    def test_audit_shard_root_ranges_overlap_not_merged_while_cleaving_1(self):
        # Verify that sharding/shrinking shard will not merge other ranges that
        # would create an overlap; shard has complete existing shard ranges,
        # newer range from root ignored
        root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)

        def do_test(own_state):
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            ts = next(self.ts_iter)
            sub_shards = [ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', own_sr.container, ts, i)),
                ts, lower, upper, state=ShardRange.ACTIVE)
                for i, lower, upper in ((0, 'a', 'ab'), (1, 'ab', 'b'))]
            acceptor_from_root = acceptor.copy(timestamp=next(self.ts_iter))
            expected = sub_shards
            sharder = self._assert_merge_into_shard(
                own_sr, sub_shards,
                [own_sr, acceptor_from_root, root_own_sr],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.CLEAVING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_root_ranges_overlap_not_merged_while_cleaving_2(self):
        # Verify that sharding/shrinking shard will not merge other ranges that
        # would create an overlap; shard has incomplete existing shard ranges
        # but ranges from root overlaps
        root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)
        ts = next(self.ts_iter)
        acceptor_sub_shards = [ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', acceptor.container, ts, i)),
            ts, lower, upper, state=ShardRange.ACTIVE)
            for i, lower, upper in ((0, 'a', 'ab'), (1, 'ab', 'c'))]

        def do_test(own_state):
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            ts = next(self.ts_iter)
            sub_shard = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', own_sr.container, ts, 0)),
                ts, lower='a', upper='abc', state=ShardRange.ACTIVE)
            expected = [sub_shard]
            sharder = self._assert_merge_into_shard(
                own_sr, [sub_shard],
                acceptor_sub_shards[1:] + [own_sr, root_own_sr],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.CLEAVING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_root_ranges_with_gap_not_merged_while_cleaving(self):
        # Verify that sharding/shrinking shard will not merge other ranges that
        # would leave a gap.
        root_own_sr = ShardRange('a/c', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
        acceptor = ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', 'c', next(self.ts_iter), 1)),
            next(self.ts_iter), 'a', 'c', state=ShardRange.ACTIVE)
        ts = next(self.ts_iter)
        acceptor_sub_shards = [ShardRange(
            str(ShardRange.make_path(
                '.shards_a', 'c', acceptor.container, ts, i)),
            ts, lower, upper, state=ShardRange.ACTIVE)
            for i, lower, upper in ((0, 'a', 'ab'), (1, 'ab', 'c'))]

        def do_test(own_state):
            own_sr = ShardRange(
                str(ShardName.create(
                    '.shards_a', 'c', 'c', next(self.ts_iter), 0)),
                next(self.ts_iter), 'a', 'b', state=own_state)
            # root ranges have gaps w.r.t. the shard namespace
            existing = expected = []
            sharder = self._assert_merge_into_shard(
                own_sr, existing,
                acceptor_sub_shards[:1] + [own_sr, root_own_sr],
                expected, 'Quoted-Root', 'a/c')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for own_state in ShardRange.CLEAVING_STATES:
            with annotate_failure(own_state):
                do_test(own_state)

    def test_audit_shard_container_ancestors_not_merged_while_sharding(self):
        # Verify that sharding shard will not merge parent and root shard
        # ranges even when the sharding shard has no other ranges
        root_sr = ShardRange('a/root', next(self.ts_iter),
                             state=ShardRange.SHARDED)
        grandparent_path = ShardRange.make_path(
            '.shards_a', 'root', root_sr.container, next(self.ts_iter), 2)
        grandparent_sr = ShardRange(grandparent_path, next(self.ts_iter),
                                    '', 'd', state=ShardRange.ACTIVE)
        self.assertTrue(grandparent_sr.is_child_of(root_sr))
        parent_path = ShardRange.make_path(
            '.shards_a', 'root', grandparent_sr.container, next(self.ts_iter),
            2)
        parent_sr = ShardRange(parent_path, next(self.ts_iter), '', 'd',
                               state=ShardRange.ACTIVE)
        self.assertTrue(parent_sr.is_child_of(grandparent_sr))
        child_path = ShardRange.make_path(
            '.shards_a', 'root', parent_sr.container, next(self.ts_iter), 2)
        child_own_sr = ShardRange(child_path, next(self.ts_iter), 'a', 'b',
                                  state=ShardRange.SHARDING)
        self.assertTrue(child_own_sr.is_child_of(parent_sr))

        ranges_from_root = [grandparent_sr, parent_sr, root_sr, child_own_sr]
        expected = []
        sharder = self._assert_merge_into_shard(
            child_own_sr, [], ranges_from_root, expected, 'Quoted-Root', 'a/c')
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))

    def test_audit_shard_container_children_merged_while_sharding(self):
        # Verify that sharding shard will always merge children shard ranges
        def do_test(child_deleted, child_state):
            root_sr = ShardRange('a/root', next(self.ts_iter),
                                 state=ShardRange.SHARDED)
            parent_path = ShardRange.make_path(
                '.shards_a', 'root', root_sr.container,
                next(self.ts_iter), 2)
            parent_sr = ShardRange(
                parent_path, next(self.ts_iter), 'a', 'd',
                state=ShardRange.SHARDING)
            child_srs = []
            for i, lower, upper in ((0, 'a', 'b'), (0, 'b', 'd')):
                child_path = ShardRange.make_path(
                    '.shards_a', 'root', parent_sr.container,
                    next(self.ts_iter), i)
                child_sr = ShardRange(
                    child_path, next(self.ts_iter), lower, upper,
                    state=child_state, deleted=child_deleted)
                self.assertTrue(child_sr.is_child_of(parent_sr))
                child_srs.append(child_sr)
            other_path = ShardRange.make_path(
                '.shards_a', 'root', root_sr.container,
                next(self.ts_iter), 3)  # different index w.r.t. parent
            other_sr = ShardRange(
                other_path, next(self.ts_iter), 'a', 'd',
                state=ShardRange.ACTIVE)
            self.assertFalse(other_sr.is_child_of(parent_sr))

            # the parent is sharding...
            broker = self._make_broker(account=parent_sr.account,
                                       container=parent_sr.container)
            broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
            broker.merge_shard_ranges(parent_sr)
            self.assertEqual(UNSHARDED, broker.get_db_state())
            self.assertEqual([parent_sr],
                             broker.get_shard_ranges(include_own=True))

            ranges_from_root = child_srs + [parent_sr, root_sr, other_sr]
            sharder, mock_swift = self.call_audit_container(
                broker, ranges_from_root)
            expected_headers = {'X-Backend-Record-Type': 'shard',
                                'X-Newest': 'true',
                                'X-Backend-Include-Deleted': 'True',
                                'X-Backend-Override-Deleted': 'true',
                                'X-Backend-Record-Shard-Format': 'full'}
            params = {'format': 'json', 'marker': 'a', 'end_marker': 'd',
                      'states': 'auditing'}
            mock_swift.make_request.assert_called_once_with(
                'GET', '/v1/a/c', expected_headers,
                acceptable_statuses=(2,), params=params)

            expected = child_srs + [parent_sr]
            if child_deleted:
                expected.append(other_sr)
            self._assert_shard_ranges_equal(
                sorted(expected, key=ShardRange.sort_key),
                sorted(broker.get_shard_ranges(
                    include_own=True, include_deleted=True),
                    key=ShardRange.sort_key))
            expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
            self._assert_stats(expected_stats, sharder, 'audit_shard')
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))

        for child_deleted in (False, True):
            for child_state in ShardRange.STATES:
                with annotate_failure('deleted: %s, state: %s'
                                      % (child_deleted, child_state)):
                    do_test(child_deleted, child_state)

    def test_audit_shard_container_children_not_merged_once_sharded(self):
        # Verify that sharding shard will not merge children shard ranges
        # once the DB is sharded (but continues to merge own shard range
        # received from root)
        root_sr = ShardRange('a/root', next(self.ts_iter),
                             state=ShardRange.SHARDED)
        ts = next(self.ts_iter)
        parent_path = ShardRange.make_path(
            '.shards_a', 'root', root_sr.container, ts, 2)
        parent_sr = ShardRange(
            parent_path, ts, 'a', 'b', state=ShardRange.ACTIVE, epoch=ts)
        child_srs = []
        for i, lower, upper in ((0, 'a', 'ab'), (0, 'ab', 'b')):
            child_path = ShardRange.make_path(
                '.shards_a', 'root', parent_sr.container,
                next(self.ts_iter), i)
            child_sr = ShardRange(
                child_path, next(self.ts_iter), lower, upper,
                state=ShardRange.CLEAVED)
            self.assertTrue(child_sr.is_child_of(parent_sr))
            child_srs.append(child_sr)

        # DB is unsharded...
        broker = self._make_broker(account=parent_sr.account,
                                   container=parent_sr.container)
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        broker.merge_shard_ranges(parent_sr)
        self.assertEqual(UNSHARDED, broker.get_db_state())

        self.assertTrue(parent_sr.update_state(
            ShardRange.SHARDING, state_timestamp=next(self.ts_iter)))
        ranges_from_root = child_srs + [parent_sr, root_sr]
        sharder, _ = self.call_audit_container(broker, ranges_from_root)

        # children ranges from root are merged
        self._assert_shard_ranges_equal(child_srs, broker.get_shard_ranges())
        # own sr from root is merged
        self.assertEqual(dict(parent_sr), dict(broker.get_own_shard_range()))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))

        # DB is sharding...
        self.assertTrue(broker.set_sharding_state())
        self.assertEqual(SHARDING, broker.get_db_state())
        parent_sr.state_timestamp = next(self.ts_iter)
        for child_sr in child_srs:
            child_sr.update_state(ShardRange.ACTIVE,
                                  state_timestamp=next(self.ts_iter))

        sharder, _ = self.call_audit_container(broker, ranges_from_root)

        # children ranges from root are merged
        self._assert_shard_ranges_equal(child_srs, broker.get_shard_ranges())
        # own sr from root is merged
        self.assertEqual(dict(parent_sr), dict(broker.get_own_shard_range()))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))

        # DB is sharded...
        self.assertTrue(broker.set_sharded_state())
        self.assertEqual(SHARDED, broker.get_db_state())
        self.assertTrue(parent_sr.update_state(
            ShardRange.SHARDED, state_timestamp=next(self.ts_iter)))
        updated_child_srs = [
            child_sr.copy(state=ShardRange.SHARDING,
                          state_timestamp=next(self.ts_iter))
            for child_sr in child_srs]

        ranges_from_root = updated_child_srs + [parent_sr, root_sr]
        sharder, _ = self.call_audit_container(broker, ranges_from_root)

        # children ranges from root are NOT merged
        self._assert_shard_ranges_equal(child_srs, broker.get_shard_ranges())
        # own sr from root is merged
        self.assertEqual(dict(parent_sr), dict(broker.get_own_shard_range()))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))

    def test_audit_shard_deleted_range_in_root_container(self):
        # verify that shard DB is marked deleted when its own shard range is
        # updated with deleted version from root
        broker = self._make_broker(account='.shards_a', container='shard_c')
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):
            own_shard_range = broker.get_own_shard_range()
        own_shard_range.lower = 'k'
        own_shard_range.upper = 't'
        broker.merge_shard_ranges([own_shard_range])

        self.assertFalse(broker.is_deleted())
        self.assertFalse(broker.is_root_container())

        shard_bounds = (
            ('a', 'j'), ('k', 't'), ('k', 's'), ('l', 's'), ('s', 'z'))
        shard_ranges = self._make_shard_ranges(shard_bounds, ShardRange.ACTIVE,
                                               timestamp=next(self.ts_iter))
        shard_ranges[1].name = broker.path
        shard_ranges[1].update_state(ShardRange.SHARDED,
                                     state_timestamp=next(self.ts_iter))
        shard_ranges[1].deleted = 1

        # mocks for delete/reclaim time comparisons
        with mock_timestamp_now(next(self.ts_iter)):
            with mock.patch('swift.container.sharder.time.time',
                            lambda: float(next(self.ts_iter))):

                sharder, mock_swift = self.call_audit_container(broker,
                                                                shard_ranges)
        self.assert_no_audit_messages(sharder, mock_swift)
        self.assertTrue(broker.is_deleted())

    def test_audit_shard_deleted_range_missing_from_root_container(self):
        # verify that shard DB is marked deleted when its own shard range is
        # marked deleted, despite receiving nothing from root
        broker = self._make_broker(account='.shards_a', container='shard_c')
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        own_shard_range = broker.get_own_shard_range()
        own_shard_range.lower = 'k'
        own_shard_range.upper = 't'
        own_shard_range.update_state(ShardRange.SHARDED,
                                     state_timestamp=Timestamp.now())
        own_shard_range.deleted = 1
        broker.merge_shard_ranges([own_shard_range])

        self.assertFalse(broker.is_deleted())
        self.assertFalse(broker.is_root_container())

        sharder, mock_swift = self.call_audit_container(broker, [])
        self.assert_no_audit_messages(sharder, mock_swift)
        self.assertTrue(broker.is_deleted())

    def test_find_and_enable_sharding_candidates(self):
        broker = self._make_broker()
        broker.enable_sharding(next(self.ts_iter))
        shard_bounds = (('', 'here'), ('here', 'there'), ('there', ''))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CLEAVED)
        shard_ranges[0].state = ShardRange.ACTIVE
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())
        with self._mock_sharder() as sharder:
            sharder._find_and_enable_sharding_candidates(broker)

        # one range just below threshold
        shard_ranges[0].update_meta(sharder.shard_container_threshold - 1, 0)
        broker.merge_shard_ranges(shard_ranges[0])
        with self._mock_sharder() as sharder:
            sharder._find_and_enable_sharding_candidates(broker)
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())

        # two ranges above threshold, only one ACTIVE
        shard_ranges[0].update_meta(sharder.shard_container_threshold, 0)
        shard_ranges[2].update_meta(sharder.shard_container_threshold + 1, 0)
        broker.merge_shard_ranges([shard_ranges[0], shard_ranges[2]])
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._find_and_enable_sharding_candidates(broker)
        expected = shard_ranges[0].copy(state=ShardRange.SHARDING,
                                        state_timestamp=now, epoch=now)
        self._assert_shard_ranges_equal([expected] + shard_ranges[1:],
                                        broker.get_shard_ranges())

        # check idempotency
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._find_and_enable_sharding_candidates(broker)
        self._assert_shard_ranges_equal([expected] + shard_ranges[1:],
                                        broker.get_shard_ranges())

        # two ranges above threshold, both ACTIVE
        shard_ranges[2].update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges[2])
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._find_and_enable_sharding_candidates(broker)
        expected_2 = shard_ranges[2].copy(state=ShardRange.SHARDING,
                                          state_timestamp=now, epoch=now)
        self._assert_shard_ranges_equal(
            [expected, shard_ranges[1], expected_2], broker.get_shard_ranges())

        # check idempotency
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._find_and_enable_sharding_candidates(broker)
        self._assert_shard_ranges_equal(
            [expected, shard_ranges[1], expected_2], broker.get_shard_ranges())

    def test_find_and_enable_sharding_candidates_bootstrap(self):
        broker = self._make_broker()
        with self._mock_sharder(
                conf={'shard_container_threshold': 2}) as sharder:
            sharder._find_and_enable_sharding_candidates(broker)
        self.assertEqual(ShardRange.ACTIVE, broker.get_own_shard_range().state)
        broker.put_object('obj1', next(self.ts_iter).internal, 1, '', '')
        broker.put_object('obj2', next(self.ts_iter).internal, 1, '', '')
        self.assertEqual(2, broker.get_info()['object_count'])
        with self._mock_sharder(
                conf={'shard_container_threshold': 2}) as sharder:
            with mock_timestamp_now() as now:
                own_sr = update_own_shard_range_stats(
                    broker, broker.get_own_shard_range())
                sharder._find_and_enable_sharding_candidates(
                    broker, [own_sr])
        own_sr = broker.get_own_shard_range()
        self.assertEqual(ShardRange.SHARDING, own_sr.state)
        self.assertEqual(now, own_sr.state_timestamp)
        self.assertEqual(now, own_sr.epoch)

        # check idempotency
        with self._mock_sharder(
                conf={'shard_container_threshold': 2}) as sharder:
            with mock_timestamp_now():
                own_sr = update_own_shard_range_stats(
                    broker, broker.get_own_shard_range())
                sharder._find_and_enable_sharding_candidates(
                    broker, [own_sr])
        own_sr = broker.get_own_shard_range()
        self.assertEqual(ShardRange.SHARDING, own_sr.state)
        self.assertEqual(now, own_sr.state_timestamp)
        self.assertEqual(now, own_sr.epoch)

    def test_find_and_enable_shrinking_candidates(self):
        broker = self._make_broker()
        broker.enable_sharding(next(self.ts_iter))
        shard_bounds = (('', 'here'), ('here', 'there'), ('there', ''))
        size = (DEFAULT_SHARDER_CONF['shrink_threshold'])

        # all shard ranges too big to shrink
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE, object_count=size - 1,
            tombstones=1)
        own_sr = broker.get_own_shard_range()
        own_sr.update_state(ShardRange.SHARDED, Timestamp.now())
        broker.merge_shard_ranges(shard_ranges + [own_sr])
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())
        with self._mock_sharder() as sharder:
            sharder._find_and_enable_shrinking_candidates(broker)
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())

        # one range just below threshold
        shard_ranges[0].update_meta(size - 2, 0)
        broker.merge_shard_ranges(shard_ranges[0])
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._send_shard_ranges = mock.MagicMock()
                sharder._find_and_enable_shrinking_candidates(broker)
        acceptor = shard_ranges[1].copy(lower=shard_ranges[0].lower)
        acceptor.timestamp = now
        donor = shard_ranges[0].copy(state=ShardRange.SHRINKING,
                                     state_timestamp=now, epoch=now)
        self._assert_shard_ranges_equal([donor, acceptor, shard_ranges[2]],
                                        broker.get_shard_ranges())
        sharder._send_shard_ranges.assert_has_calls(
            [mock.call(broker, acceptor.account, acceptor.container,
                       [acceptor]),
             mock.call(broker, donor.account, donor.container,
                       [donor, acceptor])]
        )

        # check idempotency
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._send_shard_ranges = mock.MagicMock()
                sharder._find_and_enable_shrinking_candidates(broker)
        self._assert_shard_ranges_equal([donor, acceptor, shard_ranges[2]],
                                        broker.get_shard_ranges())
        sharder._send_shard_ranges.assert_has_calls(
            [mock.call(broker, acceptor.account, acceptor.container,
                       [acceptor]),
             mock.call(broker, donor.account, donor.container,
                       [donor, acceptor])]
        )

        # acceptor falls below threshold - not a candidate
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                acceptor.update_meta(0, 0, meta_timestamp=now)
                broker.merge_shard_ranges(acceptor)
                sharder._send_shard_ranges = mock.MagicMock()
                sharder._find_and_enable_shrinking_candidates(broker)
        self._assert_shard_ranges_equal([donor, acceptor, shard_ranges[2]],
                                        broker.get_shard_ranges())
        sharder._send_shard_ranges.assert_has_calls(
            [mock.call(broker, acceptor.account, acceptor.container,
                       [acceptor]),
             mock.call(broker, donor.account, donor.container,
                       [donor, acceptor])]
        )

        # ...until donor has shrunk
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                donor.update_state(ShardRange.SHARDED, state_timestamp=now)
                donor.set_deleted(timestamp=now)
                broker.merge_shard_ranges(donor)
                sharder._send_shard_ranges = mock.MagicMock()
                sharder._find_and_enable_shrinking_candidates(broker)
        new_acceptor = shard_ranges[2].copy(lower=acceptor.lower)
        new_acceptor.timestamp = now
        new_donor = acceptor.copy(state=ShardRange.SHRINKING,
                                  state_timestamp=now, epoch=now)
        self._assert_shard_ranges_equal(
            [donor, new_donor, new_acceptor],
            broker.get_shard_ranges(include_deleted=True))
        sharder._send_shard_ranges.assert_has_calls(
            [mock.call(broker, new_acceptor.account, new_acceptor.container,
                       [new_acceptor]),
             mock.call(broker, new_donor.account, new_donor.container,
                       [new_donor, new_acceptor])]
        )

        # ..finally last shard shrinks to root
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                new_donor.update_state(ShardRange.SHARDED, state_timestamp=now)
                new_donor.set_deleted(timestamp=now)
                new_acceptor.update_meta(0, 0, meta_timestamp=now)
                broker.merge_shard_ranges([new_donor, new_acceptor])
                sharder._send_shard_ranges = mock.MagicMock()
                sharder._find_and_enable_shrinking_candidates(broker)
        final_donor = new_acceptor.copy(state=ShardRange.SHRINKING,
                                        state_timestamp=now, epoch=now)
        self._assert_shard_ranges_equal(
            [donor, new_donor, final_donor],
            broker.get_shard_ranges(include_deleted=True))
        sharder._send_shard_ranges.assert_has_calls(
            [mock.call(broker, final_donor.account, final_donor.container,
                       [final_donor, broker.get_own_shard_range()])]
        )

    def test_find_and_enable_multiple_shrinking_candidates(self):
        broker = self._make_broker()
        broker.enable_sharding(next(self.ts_iter))
        shard_bounds = (('', 'a'), ('a', 'b'), ('b', 'c'),
                        ('c', 'd'), ('d', 'e'), ('e', ''))
        size = (DEFAULT_SHARDER_CONF['shrink_threshold'])
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE, object_count=size)
        own_sr = broker.get_own_shard_range()
        own_sr.update_state(ShardRange.SHARDED, Timestamp.now())
        broker.merge_shard_ranges(shard_ranges + [own_sr])
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())
        with self._mock_sharder() as sharder:
            sharder._find_and_enable_shrinking_candidates(broker)
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())

        # three ranges just below threshold
        shard_ranges = broker.get_shard_ranges()  # get timestamps updated
        shard_ranges[0].update_meta(size - 1, 0)
        shard_ranges[1].update_meta(size - 1, 0)
        shard_ranges[3].update_meta(size - 1, 0)
        broker.merge_shard_ranges(shard_ranges)
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._send_shard_ranges = mock.MagicMock()
                sharder._find_and_enable_shrinking_candidates(broker)
        # 0 shrinks into 1 (only one donor per acceptor is allowed)
        shard_ranges[0].update_state(ShardRange.SHRINKING, state_timestamp=now)
        shard_ranges[0].epoch = now
        shard_ranges[1].lower = shard_ranges[0].lower
        shard_ranges[1].timestamp = now
        # 3 shrinks into 4
        shard_ranges[3].update_state(ShardRange.SHRINKING, state_timestamp=now)
        shard_ranges[3].epoch = now
        shard_ranges[4].lower = shard_ranges[3].lower
        shard_ranges[4].timestamp = now
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())
        for donor, acceptor in (shard_ranges[:2], shard_ranges[3:5]):
            sharder._send_shard_ranges.assert_has_calls(
                [mock.call(broker, acceptor.account, acceptor.container,
                           [acceptor]),
                 mock.call(broker, donor.account, donor.container,
                           [donor, acceptor])]
            )

    def test_partition_and_device_filters(self):
        # verify partitions and devices kwargs result in filtering of processed
        # containers but not of the local device ids.
        ring = FakeRing()
        dev_ids = set()
        container_data = []
        for dev in ring.devs:
            dev_ids.add(dev['id'])
            part = str(dev['id'])
            broker = self._make_broker(
                container='c%s' % dev['id'], hash_='c%shash' % dev['id'],
                device=dev['device'], part=part)
            broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                    ('true', next(self.ts_iter).internal)})
            container_data.append((broker.path, dev['id'], part))

        with self._mock_sharder() as sharder:
            sharder.ring = ring
            sharder._check_node = lambda node: os.path.join(
                sharder.conf['devices'], node['device'])
            with mock.patch.object(
                    sharder, '_process_broker') as mock_process_broker:
                sharder.run_once()
        self.assertEqual(dev_ids, set(sharder._local_device_ids.keys()))
        self.assertEqual(set(container_data),
                         set((call[0][0].path, call[0][1]['id'], call[0][2])
                             for call in mock_process_broker.call_args_list))

        with self._mock_sharder() as sharder:
            sharder.ring = ring
            sharder._check_node = lambda node: os.path.join(
                sharder.conf['devices'], node['device'])
            with mock.patch.object(
                    sharder, '_process_broker') as mock_process_broker:
                sharder.run_once(partitions='0')
        self.assertEqual(dev_ids, set(sharder._local_device_ids.keys()))
        self.assertEqual(set([container_data[0]]),
                         set((call[0][0].path, call[0][1]['id'], call[0][2])
                             for call in mock_process_broker.call_args_list))

        with self._mock_sharder() as sharder:
            sharder.ring = ring
            sharder._check_node = lambda node: os.path.join(
                sharder.conf['devices'], node['device'])
            with mock.patch.object(
                    sharder, '_process_broker') as mock_process_broker:
                sharder.run_once(partitions='2,0')
        self.assertEqual(dev_ids, set(sharder._local_device_ids.keys()))
        self.assertEqual(set([container_data[0], container_data[2]]),
                         set((call[0][0].path, call[0][1]['id'], call[0][2])
                             for call in mock_process_broker.call_args_list))

        with self._mock_sharder() as sharder:
            sharder.ring = ring
            sharder._check_node = lambda node: os.path.join(
                sharder.conf['devices'], node['device'])
            with mock.patch.object(
                    sharder, '_process_broker') as mock_process_broker:
                sharder.run_once(partitions='2,0', devices='sdc')
        self.assertEqual(dev_ids, set(sharder._local_device_ids.keys()))
        self.assertEqual(set([container_data[2]]),
                         set((call[0][0].path, call[0][1]['id'], call[0][2])
                             for call in mock_process_broker.call_args_list))

        with self._mock_sharder() as sharder:
            sharder.ring = ring
            sharder._check_node = lambda node: os.path.join(
                sharder.conf['devices'], node['device'])
            with mock.patch.object(
                    sharder, '_process_broker') as mock_process_broker:
                sharder.run_once(devices='sdb,sdc')
        self.assertEqual(dev_ids, set(sharder._local_device_ids.keys()))
        self.assertEqual(set(container_data[1:]),
                         set((call[0][0].path, call[0][1]['id'], call[0][2])
                             for call in mock_process_broker.call_args_list))

    def test_audit_cleave_contexts(self):

        def add_cleave_context(id, last_modified, cleaving_done):
            params = {'ref': id,
                      'cursor': 'curs',
                      'max_row': 2,
                      'cleave_to_row': 2,
                      'last_cleave_to_row': 1,
                      'cleaving_done': cleaving_done,
                      'misplaced_done': True,
                      'ranges_done': 2,
                      'ranges_todo': 4}
            key = 'X-Container-Sysmeta-Shard-Context-%s' % id
            with mock_timestamp_now(last_modified):
                broker.update_metadata(
                    {key: (json.dumps(params),
                           last_modified.internal)})

        def get_context(id, broker):
            data = broker.get_sharding_sysmeta().get('Context-%s' % id)
            if data:
                return CleavingContext(**json.loads(data))
            return data

        reclaim_age = 100
        recon_sharded_timeout = 50
        broker = self._make_broker()

        # sanity check
        self.assertIsNone(broker.get_own_shard_range(no_default=True))
        self.assertEqual(UNSHARDED, broker.get_db_state())

        # Setup some cleaving contexts
        id_old, id_newish, id_complete = [str(uuid4()) for _ in range(3)]
        ts_old, ts_newish, ts_complete = (
            Timestamp(1),
            Timestamp(reclaim_age // 2),
            Timestamp(reclaim_age - recon_sharded_timeout))
        contexts = ((id_old, ts_old, False),
                    (id_newish, ts_newish, False),
                    (id_complete, ts_complete, True))
        for id, last_modified, cleaving_done in contexts:
            add_cleave_context(id, last_modified, cleaving_done)

        sharder_conf = {'reclaim_age': str(reclaim_age),
                        'recon_sharded_timeout': str(recon_sharded_timeout)}

        with self._mock_sharder(sharder_conf) as sharder:
            with mock_timestamp_now(Timestamp(reclaim_age + 2)):
                sharder._audit_cleave_contexts(broker)

        # old context is stale, ie last modified reached reclaim_age and was
        # never completed (done).
        old_ctx = get_context(id_old, broker)
        self.assertEqual(old_ctx, "")

        # Newish context is almost stale, as in it's been 1/2 reclaim age since
        # it was last modified yet it's not completed. So it haven't been
        # cleaned up.
        newish_ctx = get_context(id_newish, broker)
        self.assertEqual(newish_ctx.ref, id_newish)

        # Complete context is complete (done) and it's been
        # recon_sharded_timeout time since it was marked completed so it's
        # been removed
        complete_ctx = get_context(id_complete, broker)
        self.assertEqual(complete_ctx, "")

        # If we push time another reclaim age later, they are all removed
        with self._mock_sharder(sharder_conf) as sharder:
            with mock_timestamp_now(Timestamp(reclaim_age * 2)):
                sharder._audit_cleave_contexts(broker)

        newish_ctx = get_context(id_newish, broker)
        self.assertEqual(newish_ctx, "")

    def test_shrinking_candidate_recon_dump(self):
        conf = {'recon_cache_path': self.tempdir,
                'devices': self.tempdir}

        shard_bounds = (
            ('', 'd'), ('d', 'g'), ('g', 'l'), ('l', 'o'), ('o', 't'),
            ('t', 'x'), ('x', ''))

        with self._mock_sharder(conf) as sharder:
            brokers = []
            shard_ranges = []
            C1, C2, C3 = 0, 1, 2

            for container in ('c1', 'c2', 'c3'):
                broker = self._make_broker(
                    container=container, hash_=container + 'hash',
                    device=sharder.ring.devs[0]['device'], part=0)
                broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                        ('true', next(self.ts_iter).internal)})
                my_sr = broker.get_own_shard_range()
                my_sr.epoch = Timestamp.now()
                broker.merge_shard_ranges([my_sr])
                brokers.append(broker)
                shard_ranges.append(self._make_shard_ranges(
                    shard_bounds, state=ShardRange.ACTIVE,
                    object_count=(
                        DEFAULT_SHARDER_CONF['shard_container_threshold'] / 2),
                    timestamp=next(self.ts_iter)))

            # we want c2 to have 2 shrink pairs
            shard_ranges[C2][1].object_count = 0
            shard_ranges[C2][3].object_count = 0
            brokers[C2].merge_shard_ranges(shard_ranges[C2])
            brokers[C2].set_sharding_state()
            brokers[C2].set_sharded_state()

            # we want c1 to have the same, but one can't be shrunk
            shard_ranges[C1][1].object_count = 0
            shard_ranges[C1][2].object_count = \
                DEFAULT_SHARDER_CONF['shard_container_threshold'] - 1
            shard_ranges[C1][3].object_count = 0
            brokers[C1].merge_shard_ranges(shard_ranges[C1])
            brokers[C1].set_sharding_state()
            brokers[C1].set_sharded_state()

            # c3 we want to have more total_sharding donors then can be sharded
            # in one go.
            shard_ranges[C3][0].object_count = 0
            shard_ranges[C3][1].object_count = 0
            shard_ranges[C3][2].object_count = 0
            shard_ranges[C3][3].object_count = 0
            shard_ranges[C3][4].object_count = 0
            shard_ranges[C3][5].object_count = 0
            brokers[C3].merge_shard_ranges(shard_ranges[C3])
            brokers[C3].set_sharding_state()
            brokers[C3].set_sharded_state()

            node = {'ip': '10.0.0.0', 'replication_ip': '10.0.1.0',
                    'port': 1000, 'replication_port': 1100,
                    'device': 'sda', 'zone': 0, 'region': 0, 'id': 1,
                    'index': 0}

            for broker in brokers:
                sharder._identify_shrinking_candidate(broker, node)

            sharder._report_stats()
            expected_shrinking_candidates_data = {
                'found': 3,
                'top': [
                    {
                        'object_count': 500000,
                        'account': brokers[C3].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C3].container,
                        'file_size': os.stat(brokers[C3].db_file).st_size,
                        'path': brokers[C3].db_file,
                        'root': brokers[C3].path,
                        'node_index': 0,
                        'compactible_ranges': 3
                    }, {
                        'object_count': 2500000,
                        'account': brokers[C2].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C2].container,
                        'file_size': os.stat(brokers[1].db_file).st_size,
                        'path': brokers[C2].db_file,
                        'root': brokers[C2].path,
                        'node_index': 0,
                        'compactible_ranges': 2
                    }, {
                        'object_count': 2999999,
                        'account': brokers[C1].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C1].container,
                        'file_size': os.stat(brokers[C1].db_file).st_size,
                        'path': brokers[C1].db_file,
                        'root': brokers[C1].path,
                        'node_index': 0,
                        'compactible_ranges': 1
                    }
                ]}
            self._assert_recon_stats(expected_shrinking_candidates_data,
                                     sharder, 'shrinking_candidates')

            # check shrinking stats are reset
            sharder._zero_stats()
            for broker in brokers:
                sharder._identify_shrinking_candidate(broker, node)
            sharder._report_stats()
            self._assert_recon_stats(expected_shrinking_candidates_data,
                                     sharder, 'shrinking_candidates')

            # set some ranges to shrinking and check that stats are updated; in
            # this case the container C2 no longer has any shrinkable ranges
            # and no longer appears in stats
            def shrink_actionable_ranges(broker):
                compactible = find_compactible_shard_sequences(
                    broker, sharder.shrink_threshold, sharder.expansion_limit,
                    1, -1)
                self.assertNotEqual([], compactible)
                with mock_timestamp_now(next(self.ts_iter)):
                    process_compactible_shard_sequences(broker, compactible)

            shrink_actionable_ranges(brokers[C2])
            sharder._zero_stats()
            for broker in brokers:
                sharder._identify_shrinking_candidate(broker, node)
            sharder._report_stats()
            expected_shrinking_candidates_data = {
                'found': 2,
                'top': [
                    {
                        'object_count': mock.ANY,
                        'account': brokers[C3].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C3].container,
                        'file_size': os.stat(brokers[C3].db_file).st_size,
                        'path': brokers[C3].db_file,
                        'root': brokers[C3].path,
                        'node_index': 0,
                        'compactible_ranges': 3
                    }, {
                        'object_count': mock.ANY,
                        'account': brokers[C1].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C1].container,
                        'file_size': os.stat(brokers[C1].db_file).st_size,
                        'path': brokers[C1].db_file,
                        'root': brokers[C1].path,
                        'node_index': 0,
                        'compactible_ranges': 1
                    }
                ]}
            self._assert_recon_stats(expected_shrinking_candidates_data,
                                     sharder, 'shrinking_candidates')

            # set some ranges to shrinking and check that stats are updated; in
            # this case the container C3 no longer has any actionable ranges
            # and no longer appears in stats
            shrink_actionable_ranges(brokers[C3])
            sharder._zero_stats()
            for broker in brokers:
                sharder._identify_shrinking_candidate(broker, node)
            sharder._report_stats()
            expected_shrinking_candidates_data = {
                'found': 1,
                'top': [
                    {
                        'object_count': mock.ANY,
                        'account': brokers[C1].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C1].container,
                        'file_size': os.stat(brokers[C1].db_file).st_size,
                        'path': brokers[C1].db_file,
                        'root': brokers[C1].path,
                        'node_index': 0,
                        'compactible_ranges': 1
                    }
                ]}
            self._assert_recon_stats(expected_shrinking_candidates_data,
                                     sharder, 'shrinking_candidates')

            # set some ranges to shrunk in C3 so that other sequences become
            # compactible
            now = next(self.ts_iter)
            shard_ranges = brokers[C3].get_shard_ranges()
            for (donor, acceptor) in zip(shard_ranges, shard_ranges[1:]):
                if donor.state == ShardRange.SHRINKING:
                    donor.update_state(ShardRange.SHRUNK, state_timestamp=now)
                    donor.set_deleted(timestamp=now)
                    acceptor.lower = donor.lower
                    acceptor.timestamp = now
            brokers[C3].merge_shard_ranges(shard_ranges)
            sharder._zero_stats()
            for broker in brokers:
                sharder._identify_shrinking_candidate(broker, node)
            sharder._report_stats()
            expected_shrinking_candidates_data = {
                'found': 2,
                'top': [
                    {
                        'object_count': mock.ANY,
                        'account': brokers[C3].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C3].container,
                        'file_size': os.stat(brokers[C3].db_file).st_size,
                        'path': brokers[C3].db_file,
                        'root': brokers[C3].path,
                        'node_index': 0,
                        'compactible_ranges': 2
                    }, {
                        'object_count': mock.ANY,
                        'account': brokers[C1].account,
                        'meta_timestamp': mock.ANY,
                        'container': brokers[C1].container,
                        'file_size': os.stat(brokers[C1].db_file).st_size,
                        'path': brokers[C1].db_file,
                        'root': brokers[C1].path,
                        'node_index': 0,
                        'compactible_ranges': 1
                    }
                ]}
            self._assert_recon_stats(expected_shrinking_candidates_data,
                                     sharder, 'shrinking_candidates')

    @mock.patch('swift.common.ring.ring.Ring.get_part_nodes', return_value=[])
    @mock.patch('swift.common.ring.ring.Ring.get_more_nodes', return_value=[])
    def test_get_shard_broker_no_local_handoff_for_part(
            self, mock_part_nodes, mock_more_nodes):
        broker = self._make_broker()
        broker.enable_sharding(Timestamp.now())

        shard_bounds = (('', 'd'), ('d', 'x'), ('x', ''))
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)

        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())

        # first, let's assume there local_handoff_for_part fails because the
        # node we're on is at zero weight for all disks. So it wont appear in
        # the replica2part2dev table, meaning we wont get a node back.
        # in this case, we'll fall back to one of our own devices which we
        # determine from the ring.devs not the replica2part2dev table.
        with self._mock_sharder() as sharder:
            local_dev_ids = {dev['id']: dev for dev in sharder.ring.devs[-1:]}
            sharder._local_device_ids = local_dev_ids
            part, shard_broker, node_id, _ = sharder._get_shard_broker(
                shard_ranges[0], broker.root_path, 0)
            self.assertIn(node_id, local_dev_ids)

        # if there are more then 1 local_dev_id it'll randomly pick one
        selected_node_ids = set()
        for _ in range(10):
            with self._mock_sharder() as sharder:
                local_dev_ids = {dev['id']: dev
                                 for dev in sharder.ring.devs[-2:]}
                sharder._local_device_ids = local_dev_ids
                part, shard_broker, node_id, _ = sharder._get_shard_broker(
                    shard_ranges[0], broker.root_path, 0)
                self.assertIn(node_id, local_dev_ids)
                selected_node_ids.add(node_id)
            if len(selected_node_ids) == 2:
                break
        self.assertEqual(len(selected_node_ids), 2)

        # If there are also no local_dev_ids, then we'll get the RuntimeError
        with self._mock_sharder() as sharder:
            sharder._local_device_ids = {}
            with self.assertRaises(RuntimeError) as dev_err:
                sharder._get_shard_broker(shard_ranges[0], broker.root_path, 0)

        expected_error_string = 'Cannot find local handoff; no local devices'
        self.assertEqual(str(dev_err.exception), expected_error_string)


class TestCleavingContext(BaseTestSharder):
    def test_init(self):
        ctx = CleavingContext(ref='test')
        self.assertEqual('test', ctx.ref)
        self.assertEqual('', ctx.cursor)
        self.assertIsNone(ctx.max_row)
        self.assertIsNone(ctx.cleave_to_row)
        self.assertIsNone(ctx.last_cleave_to_row)
        self.assertFalse(ctx.misplaced_done)
        self.assertFalse(ctx.cleaving_done)

    def test_iter(self):
        ctx = CleavingContext('test', 'curs', 12, 11, 10, False, True, 0, 4)
        expected = {'ref': 'test',
                    'cursor': 'curs',
                    'max_row': 12,
                    'cleave_to_row': 11,
                    'last_cleave_to_row': 10,
                    'cleaving_done': False,
                    'misplaced_done': True,
                    'ranges_done': 0,
                    'ranges_todo': 4}
        self.assertEqual(expected, dict(ctx))

    def test_cursor(self):
        broker = self._make_broker()
        ref = CleavingContext._make_ref(broker)

        for curs in ('curs', u'curs\u00e4\u00fb'):
            with annotate_failure('%r' % curs):
                expected = curs
                ctx = CleavingContext(ref, curs, 12, 11, 10, False, True)
                self.assertEqual(dict(ctx), {
                    'cursor': expected,
                    'max_row': 12,
                    'cleave_to_row': 11,
                    'last_cleave_to_row': 10,
                    'cleaving_done': False,
                    'misplaced_done': True,
                    'ranges_done': 0,
                    'ranges_todo': 0,
                    'ref': ref,
                })
                self.assertEqual(expected, ctx.cursor)
                ctx.store(broker)
                reloaded_ctx = CleavingContext.load(broker)
                self.assertEqual(expected, reloaded_ctx.cursor)
                # Since we reloaded, the max row gets updated from the broker
                self.assertEqual(reloaded_ctx.max_row, -1)
                # reset it so the dict comparison will succeed
                reloaded_ctx.max_row = 12
                self.assertEqual(dict(ctx), dict(reloaded_ctx))

    def test_load(self):
        broker = self._make_broker()
        for i in range(6):
            broker.put_object('o%s' % i, next(self.ts_iter).internal, 10,
                              'text/plain', 'etag_a', 0)

        db_id = broker.get_info()['id']
        params = {'ref': db_id,
                  'cursor': 'curs',
                  'max_row': 2,
                  'cleave_to_row': 2,
                  'last_cleave_to_row': 1,
                  'cleaving_done': False,
                  'misplaced_done': True,
                  'ranges_done': 2,
                  'ranges_todo': 4}
        key = 'X-Container-Sysmeta-Shard-Context-%s' % db_id
        broker.update_metadata(
            {key: (json.dumps(params), Timestamp.now().internal)})
        ctx = CleavingContext.load(broker)
        self.assertEqual(db_id, ctx.ref)
        self.assertEqual('curs', ctx.cursor)
        # note max_row is dynamically updated during load
        self.assertEqual(6, ctx.max_row)
        self.assertEqual(2, ctx.cleave_to_row)
        self.assertEqual(1, ctx.last_cleave_to_row)
        self.assertTrue(ctx.misplaced_done)
        self.assertFalse(ctx.cleaving_done)
        self.assertEqual(2, ctx.ranges_done)
        self.assertEqual(4, ctx.ranges_todo)

    def test_load_all(self):
        broker = self._make_broker()
        last_ctx = None
        timestamp = Timestamp.now()

        db_ids = [str(uuid4()) for _ in range(6)]
        for db_id in db_ids:
            params = {'ref': db_id,
                      'cursor': 'curs',
                      'max_row': 2,
                      'cleave_to_row': 2,
                      'last_cleave_to_row': 1,
                      'cleaving_done': False,
                      'misplaced_done': True,
                      'ranges_done': 2,
                      'ranges_todo': 4}
            key = 'X-Container-Sysmeta-Shard-Context-%s' % db_id
            broker.update_metadata(
                {key: (json.dumps(params), timestamp.internal)})
        first_ctx = None
        for ctx, lm in CleavingContext.load_all(broker):
            if not first_ctx:
                first_ctx = ctx
            last_ctx = ctx
            self.assertIn(ctx.ref, db_ids)
            self.assertEqual(lm, timestamp.internal)

        # If a context is deleted (metadata is "") then it's skipped
        last_ctx.delete(broker)
        db_ids.remove(last_ctx.ref)

        # and let's modify the first
        with mock_timestamp_now() as new_timestamp:
            first_ctx.store(broker)

        for ctx, lm in CleavingContext.load_all(broker):
            self.assertIn(ctx.ref, db_ids)
            if ctx.ref == first_ctx.ref:
                self.assertEqual(lm, new_timestamp.internal)
            else:
                self.assertEqual(lm, timestamp.internal)

        # delete all contexts
        for ctx, lm in CleavingContext.load_all(broker):
            ctx.delete(broker)
        self.assertEqual([], CleavingContext.load_all(broker))

    def test_delete(self):
        broker = self._make_broker()

        db_id = broker.get_info()['id']
        params = {'ref': db_id,
                  'cursor': 'curs',
                  'max_row': 2,
                  'cleave_to_row': 2,
                  'last_cleave_to_row': 1,
                  'cleaving_done': False,
                  'misplaced_done': True,
                  'ranges_done': 2,
                  'ranges_todo': 4}
        key = 'X-Container-Sysmeta-Shard-Context-%s' % db_id
        broker.update_metadata(
            {key: (json.dumps(params), Timestamp.now().internal)})
        ctx = CleavingContext.load(broker)
        self.assertEqual(db_id, ctx.ref)

        # Now let's delete it. When deleted the metadata key will exist, but
        # the value will be "" as this means it'll be reaped later.
        ctx.delete(broker)

        sysmeta = broker.get_sharding_sysmeta()
        for key, val in sysmeta.items():
            if key == "Context-%s" % db_id:
                self.assertEqual(val, "")
                break
        else:
            self.fail("Deleted context 'Context-%s' not found")

    def test_store_old_style(self):
        broker = self._make_old_style_sharding_broker()
        old_db_id = broker.get_brokers()[0].get_info()['id']
        last_mod = Timestamp.now()
        ctx = CleavingContext(old_db_id, 'curs', 12, 11, 2, True, True, 2, 4)
        with mock_timestamp_now(last_mod):
            ctx.store(broker)
        key = 'X-Container-Sysmeta-Shard-Context-%s' % old_db_id
        data = json.loads(broker.metadata[key][0])
        expected = {'ref': old_db_id,
                    'cursor': 'curs',
                    'max_row': 12,
                    'cleave_to_row': 11,
                    'last_cleave_to_row': 2,
                    'cleaving_done': True,
                    'misplaced_done': True,
                    'ranges_done': 2,
                    'ranges_todo': 4}
        self.assertEqual(expected, data)
        # last modified is the metadata timestamp
        self.assertEqual(broker.metadata[key][1], last_mod.internal)

    def test_store_add_row_load_old_style(self):
        # adding row to older db changes only max_row in the context
        broker = self._make_old_style_sharding_broker()
        old_broker = broker.get_brokers()[0]
        old_db_id = old_broker.get_info()['id']
        old_broker.merge_items([old_broker._record_to_dict(
            ('obj', next(self.ts_iter).internal, 0, 'text/plain', 'etag', 1))])
        old_max_row = old_broker.get_max_row()
        self.assertEqual(1, old_max_row)  # sanity check
        ctx = CleavingContext(old_db_id, 'curs', 1, 1, 0, True, True)
        ctx.store(broker)

        # adding a row changes max row
        old_broker.merge_items([old_broker._record_to_dict(
            ('obj', next(self.ts_iter).internal, 0, 'text/plain', 'etag', 1))])

        new_ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, new_ctx.ref)
        self.assertEqual('curs', new_ctx.cursor)
        self.assertEqual(2, new_ctx.max_row)
        self.assertEqual(1, new_ctx.cleave_to_row)
        self.assertEqual(0, new_ctx.last_cleave_to_row)
        self.assertTrue(new_ctx.misplaced_done)
        self.assertTrue(new_ctx.cleaving_done)

    def test_store_reclaim_load_old_style(self):
        # reclaiming rows from older db does not change context
        broker = self._make_old_style_sharding_broker()
        old_broker = broker.get_brokers()[0]
        old_db_id = old_broker.get_info()['id']
        old_broker.merge_items([old_broker._record_to_dict(
            ('obj', next(self.ts_iter).internal, 0, 'text/plain', 'etag', 1))])
        old_max_row = old_broker.get_max_row()
        self.assertEqual(1, old_max_row)  # sanity check
        ctx = CleavingContext(old_db_id, 'curs', 1, 1, 0, True, True)
        ctx.store(broker)

        self.assertEqual(
            1, len(old_broker.get_objects()))
        now = next(self.ts_iter).internal
        broker.get_brokers()[0].reclaim(now, now)
        self.assertFalse(old_broker.get_objects())

        new_ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, new_ctx.ref)
        self.assertEqual('curs', new_ctx.cursor)
        self.assertEqual(1, new_ctx.max_row)
        self.assertEqual(1, new_ctx.cleave_to_row)
        self.assertEqual(0, new_ctx.last_cleave_to_row)
        self.assertTrue(new_ctx.misplaced_done)
        self.assertTrue(new_ctx.cleaving_done)

    def test_store_modify_db_id_load_old_style(self):
        # changing id changes ref, so results in a fresh context
        broker = self._make_old_style_sharding_broker()
        old_broker = broker.get_brokers()[0]
        old_db_id = old_broker.get_info()['id']
        ctx = CleavingContext(old_db_id, 'curs', 12, 11, 2, True, True)
        ctx.store(broker)

        old_broker.newid('fake_remote_id')
        new_db_id = old_broker.get_info()['id']
        self.assertNotEqual(old_db_id, new_db_id)

        new_ctx = CleavingContext.load(broker)
        self.assertEqual(new_db_id, new_ctx.ref)
        self.assertEqual('', new_ctx.cursor)
        # note max_row is dynamically updated during load
        self.assertEqual(-1, new_ctx.max_row)
        self.assertEqual(None, new_ctx.cleave_to_row)
        self.assertEqual(None, new_ctx.last_cleave_to_row)
        self.assertFalse(new_ctx.misplaced_done)
        self.assertFalse(new_ctx.cleaving_done)

    def test_load_modify_store_load_old_style(self):
        broker = self._make_old_style_sharding_broker()
        old_db_id = broker.get_brokers()[0].get_info()['id']
        ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, ctx.ref)
        self.assertEqual('', ctx.cursor)  # sanity check
        ctx.cursor = 'curs'
        ctx.misplaced_done = True
        ctx.store(broker)
        ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, ctx.ref)
        self.assertEqual('curs', ctx.cursor)
        self.assertTrue(ctx.misplaced_done)

    def test_store(self):
        broker = self._make_sharding_broker()
        old_db_id = broker.get_brokers()[0].get_info()['id']
        last_mod = Timestamp.now()
        ctx = CleavingContext(old_db_id, 'curs', 12, 11, 2, True, True, 2, 4)
        with mock_timestamp_now(last_mod):
            ctx.store(broker)
        key = 'X-Container-Sysmeta-Shard-Context-%s' % old_db_id
        data = json.loads(broker.metadata[key][0])
        expected = {'ref': old_db_id,
                    'cursor': 'curs',
                    'max_row': 12,
                    'cleave_to_row': 11,
                    'last_cleave_to_row': 2,
                    'cleaving_done': True,
                    'misplaced_done': True,
                    'ranges_done': 2,
                    'ranges_todo': 4}
        self.assertEqual(expected, data)
        # last modified is the metadata timestamp
        self.assertEqual(broker.metadata[key][1], last_mod.internal)

    def test_store_add_row_load(self):
        # adding row to older db changes only max_row in the context
        broker = self._make_sharding_broker()
        old_broker = broker.get_brokers()[0]
        old_db_id = old_broker.get_info()['id']
        old_broker.merge_items([old_broker._record_to_dict(
            ('obj', next(self.ts_iter).internal, 0, 'text/plain', 'etag', 1))])
        old_max_row = old_broker.get_max_row()
        self.assertEqual(1, old_max_row)  # sanity check
        ctx = CleavingContext(old_db_id, 'curs', 1, 1, 0, True, True)
        ctx.store(broker)

        # adding a row changes max row
        old_broker.merge_items([old_broker._record_to_dict(
            ('obj', next(self.ts_iter).internal, 0, 'text/plain', 'etag', 1))])

        new_ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, new_ctx.ref)
        self.assertEqual('curs', new_ctx.cursor)
        self.assertEqual(2, new_ctx.max_row)
        self.assertEqual(1, new_ctx.cleave_to_row)
        self.assertEqual(0, new_ctx.last_cleave_to_row)
        self.assertTrue(new_ctx.misplaced_done)
        self.assertTrue(new_ctx.cleaving_done)

    def test_store_reclaim_load(self):
        # reclaiming rows from older db does not change context
        broker = self._make_sharding_broker()
        old_broker = broker.get_brokers()[0]
        old_db_id = old_broker.get_info()['id']
        old_broker.merge_items([old_broker._record_to_dict(
            ('obj', next(self.ts_iter).internal, 0, 'text/plain', 'etag', 1))])
        old_max_row = old_broker.get_max_row()
        self.assertEqual(1, old_max_row)  # sanity check
        ctx = CleavingContext(old_db_id, 'curs', 1, 1, 0, True, True)
        ctx.store(broker)

        self.assertEqual(
            1, len(old_broker.get_objects()))
        now = next(self.ts_iter).internal
        broker.get_brokers()[0].reclaim(now, now)
        self.assertFalse(old_broker.get_objects())

        new_ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, new_ctx.ref)
        self.assertEqual('curs', new_ctx.cursor)
        self.assertEqual(1, new_ctx.max_row)
        self.assertEqual(1, new_ctx.cleave_to_row)
        self.assertEqual(0, new_ctx.last_cleave_to_row)
        self.assertTrue(new_ctx.misplaced_done)
        self.assertTrue(new_ctx.cleaving_done)

    def test_store_modify_db_id_load(self):
        # changing id changes ref, so results in a fresh context
        broker = self._make_sharding_broker()
        old_broker = broker.get_brokers()[0]
        old_db_id = old_broker.get_info()['id']
        ctx = CleavingContext(old_db_id, 'curs', 12, 11, 2, True, True)
        ctx.store(broker)

        old_broker.newid('fake_remote_id')
        new_db_id = old_broker.get_info()['id']
        self.assertNotEqual(old_db_id, new_db_id)

        new_ctx = CleavingContext.load(broker)
        self.assertEqual(new_db_id, new_ctx.ref)
        self.assertEqual('', new_ctx.cursor)
        # note max_row is dynamically updated during load
        self.assertEqual(-1, new_ctx.max_row)
        self.assertEqual(None, new_ctx.cleave_to_row)
        self.assertEqual(None, new_ctx.last_cleave_to_row)
        self.assertFalse(new_ctx.misplaced_done)
        self.assertFalse(new_ctx.cleaving_done)

    def test_load_modify_store_load(self):
        broker = self._make_sharding_broker()
        old_db_id = broker.get_brokers()[0].get_info()['id']
        ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, ctx.ref)
        self.assertEqual('', ctx.cursor)  # sanity check
        ctx.cursor = 'curs'
        ctx.misplaced_done = True
        ctx.store(broker)
        ctx = CleavingContext.load(broker)
        self.assertEqual(old_db_id, ctx.ref)
        self.assertEqual('curs', ctx.cursor)
        self.assertTrue(ctx.misplaced_done)

    def test_reset(self):
        ctx = CleavingContext('test', 'curs', 12, 11, 2, True, True)

        def check_context():
            self.assertEqual('test', ctx.ref)
            self.assertEqual('', ctx.cursor)
            self.assertEqual(12, ctx.max_row)
            self.assertEqual(11, ctx.cleave_to_row)
            self.assertEqual(11, ctx.last_cleave_to_row)
            self.assertFalse(ctx.misplaced_done)
            self.assertFalse(ctx.cleaving_done)
            self.assertEqual(0, ctx.ranges_done)
            self.assertEqual(0, ctx.ranges_todo)
        ctx.reset()
        check_context()
        # check idempotency
        ctx.reset()
        check_context()

    def test_start(self):
        ctx = CleavingContext('test', 'curs', 12, 11, 2, True, True)

        def check_context():
            self.assertEqual('test', ctx.ref)
            self.assertEqual('', ctx.cursor)
            self.assertEqual(12, ctx.max_row)
            self.assertEqual(12, ctx.cleave_to_row)
            self.assertEqual(2, ctx.last_cleave_to_row)
            self.assertTrue(ctx.misplaced_done)  # *not* reset here
            self.assertFalse(ctx.cleaving_done)
            self.assertEqual(0, ctx.ranges_done)
            self.assertEqual(0, ctx.ranges_todo)
        ctx.start()
        check_context()
        # check idempotency
        ctx.start()
        check_context()

    def test_range_done(self):
        ctx = CleavingContext('test', '', 12, 11, 2, True, True)
        self.assertEqual(0, ctx.ranges_done)
        self.assertEqual(0, ctx.ranges_todo)
        self.assertEqual('', ctx.cursor)

        ctx.ranges_todo = 5
        ctx.range_done('b')
        self.assertEqual(1, ctx.ranges_done)
        self.assertEqual(4, ctx.ranges_todo)
        self.assertEqual('b', ctx.cursor)

        ctx.ranges_todo = 9
        ctx.range_done('c')
        self.assertEqual(2, ctx.ranges_done)
        self.assertEqual(8, ctx.ranges_todo)
        self.assertEqual('c', ctx.cursor)

    def test_done(self):
        ctx = CleavingContext(
            'test', '', max_row=12, cleave_to_row=12, last_cleave_to_row=2,
            cleaving_done=True, misplaced_done=True)
        self.assertTrue(ctx.done())
        ctx = CleavingContext(
            'test', '', max_row=12, cleave_to_row=11, last_cleave_to_row=2,
            cleaving_done=True, misplaced_done=True)
        self.assertFalse(ctx.done())
        ctx = CleavingContext(
            'test', '', max_row=12, cleave_to_row=12, last_cleave_to_row=2,
            cleaving_done=True, misplaced_done=False)
        self.assertFalse(ctx.done())
        ctx = CleavingContext(
            'test', '', max_row=12, cleave_to_row=12, last_cleave_to_row=2,
            cleaving_done=False, misplaced_done=True)
        self.assertFalse(ctx.done())


class TestSharderFunctions(BaseTestSharder):
    def test_find_shrinking_candidates(self):
        broker = self._make_broker()
        shard_bounds = (('', 'a'), ('a', 'b'), ('b', 'c'), ('c', 'd'))
        threshold = (DEFAULT_SHARDER_CONF['shrink_threshold'])
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE, object_count=threshold,
            timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shard_ranges)
        pairs = find_shrinking_candidates(broker, threshold, threshold * 4)
        self.assertEqual({}, pairs)

        # one range just below threshold
        shard_ranges[0].update_meta(threshold - 1, 0,
                                    meta_timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shard_ranges[0])
        pairs = find_shrinking_candidates(broker, threshold, threshold * 4)
        self.assertEqual(1, len(pairs), pairs)
        for acceptor, donor in pairs.items():
            self.assertEqual(shard_ranges[1], acceptor)
            self.assertEqual(shard_ranges[0], donor)

        # two ranges just below threshold
        shard_ranges[2].update_meta(threshold - 1, 0,
                                    meta_timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shard_ranges[2])
        pairs = find_shrinking_candidates(broker, threshold, threshold * 4)

        # shenanigans to work around dicts with ShardRanges keys not comparing
        def check_pairs(pairs):
            acceptors = []
            donors = []
            for acceptor, donor in pairs.items():
                acceptors.append(acceptor)
                donors.append(donor)
            acceptors.sort(key=ShardRange.sort_key)
            donors.sort(key=ShardRange.sort_key)
            self.assertEqual([shard_ranges[1], shard_ranges[3]], acceptors)
            self.assertEqual([shard_ranges[0], shard_ranges[2]], donors)

        check_pairs(pairs)

        # repeat call after broker is updated and expect same pairs
        shard_ranges[0].update_state(ShardRange.SHRINKING, next(self.ts_iter))
        shard_ranges[2].update_state(ShardRange.SHRINKING, next(self.ts_iter))
        shard_ranges[1].lower = shard_ranges[0].lower
        shard_ranges[1].timestamp = next(self.ts_iter)
        shard_ranges[3].lower = shard_ranges[2].lower
        shard_ranges[3].timestamp = next(self.ts_iter)
        broker.merge_shard_ranges(shard_ranges)
        pairs = find_shrinking_candidates(broker, threshold, threshold * 4)
        check_pairs(pairs)

    def test_finalize_shrinking(self):
        broker = self._make_broker()
        broker.enable_sharding(next(self.ts_iter))
        shard_bounds = (('', 'here'), ('here', 'there'), ('there', ''))
        ts_0 = next(self.ts_iter)
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE, timestamp=ts_0)
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())
        ts_1 = next(self.ts_iter)
        finalize_shrinking(broker, shard_ranges[2:], shard_ranges[:2], ts_1)
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(
            [ShardRange.SHRINKING, ShardRange.SHRINKING, ShardRange.ACTIVE],
            [sr.state for sr in updated_ranges]
        )
        # acceptor is not updated...
        self.assertEqual(ts_0, updated_ranges[2].timestamp)
        # donors are updated...
        self.assertEqual([ts_1] * 2,
                         [sr.state_timestamp for sr in updated_ranges[:2]])
        self.assertEqual([ts_1] * 2,
                         [sr.epoch for sr in updated_ranges[:2]])
        # check idempotency
        ts_2 = next(self.ts_iter)
        finalize_shrinking(broker, shard_ranges[2:], shard_ranges[:2], ts_2)
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(
            [ShardRange.SHRINKING, ShardRange.SHRINKING, ShardRange.ACTIVE],
            [sr.state for sr in updated_ranges]
        )
        # acceptor is not updated...
        self.assertEqual(ts_0, updated_ranges[2].timestamp)
        # donors are not updated...
        self.assertEqual([ts_1] * 2,
                         [sr.state_timestamp for sr in updated_ranges[:2]])
        self.assertEqual([ts_1] * 2,
                         [sr.epoch for sr in updated_ranges[:2]])

    def test_process_compactible(self):
        # no sequences...
        broker = self._make_broker()
        with mock.patch('swift.container.sharder.finalize_shrinking') as fs:
            with mock_timestamp_now(next(self.ts_iter)) as now:
                process_compactible_shard_sequences(broker, [])
        fs.assert_called_once_with(broker, [], [], now)

        # two sequences with acceptor bounds needing to be updated
        ts_0 = next(self.ts_iter)
        sequence_1 = self._make_shard_ranges(
            (('a', 'b'), ('b', 'c'), ('c', 'd')),
            state=ShardRange.ACTIVE, timestamp=ts_0)
        sequence_2 = self._make_shard_ranges(
            (('x', 'y'), ('y', 'z')),
            state=ShardRange.ACTIVE, timestamp=ts_0)
        with mock.patch('swift.container.sharder.finalize_shrinking') as fs:
            with mock_timestamp_now(next(self.ts_iter)) as now:
                process_compactible_shard_sequences(
                    broker, [sequence_1, sequence_2])
        expected_donors = sequence_1[:-1] + sequence_2[:-1]
        expected_acceptors = [sequence_1[-1].copy(lower='a', timestamp=now),
                              sequence_2[-1].copy(lower='x', timestamp=now)]
        fs.assert_called_once_with(
            broker, expected_acceptors, expected_donors, now)
        self.assertEqual([dict(sr) for sr in expected_acceptors],
                         [dict(sr) for sr in fs.call_args[0][1]])
        self.assertEqual([dict(sr) for sr in expected_donors],
                         [dict(sr) for sr in fs.call_args[0][2]])

        # sequences have already been processed - acceptors expanded
        sequence_1 = self._make_shard_ranges(
            (('a', 'b'), ('b', 'c'), ('a', 'd')),
            state=ShardRange.ACTIVE, timestamp=ts_0)
        sequence_2 = self._make_shard_ranges(
            (('x', 'y'), ('x', 'z')),
            state=ShardRange.ACTIVE, timestamp=ts_0)
        with mock.patch('swift.container.sharder.finalize_shrinking') as fs:
            with mock_timestamp_now(next(self.ts_iter)) as now:
                process_compactible_shard_sequences(
                    broker, [sequence_1, sequence_2])
        expected_donors = sequence_1[:-1] + sequence_2[:-1]
        expected_acceptors = [sequence_1[-1], sequence_2[-1]]
        fs.assert_called_once_with(
            broker, expected_acceptors, expected_donors, now)

        self.assertEqual([dict(sr) for sr in expected_acceptors],
                         [dict(sr) for sr in fs.call_args[0][1]])
        self.assertEqual([dict(sr) for sr in expected_donors],
                         [dict(sr) for sr in fs.call_args[0][2]])

        # acceptor is root - needs state to be updated, but not bounds
        sequence_1 = self._make_shard_ranges(
            (('a', 'b'), ('b', 'c'), ('a', 'd'), ('d', ''), ('', '')),
            state=[ShardRange.ACTIVE] * 4 + [ShardRange.SHARDED],
            timestamp=ts_0)
        with mock.patch('swift.container.sharder.finalize_shrinking') as fs:
            with mock_timestamp_now(next(self.ts_iter)) as now:
                process_compactible_shard_sequences(broker, [sequence_1])
        expected_donors = sequence_1[:-1]
        expected_acceptors = [sequence_1[-1].copy(state=ShardRange.ACTIVE,
                                                  state_timestamp=now)]
        fs.assert_called_once_with(
            broker, expected_acceptors, expected_donors, now)

        self.assertEqual([dict(sr) for sr in expected_acceptors],
                         [dict(sr) for sr in fs.call_args[0][1]])
        self.assertEqual([dict(sr) for sr in expected_donors],
                         [dict(sr) for sr in fs.call_args[0][2]])

    def test_find_compactible_shard_ranges_in_found_state(self):
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.FOUND)
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([], sequences)

    def test_find_compactible_no_donors(self):
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('a', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE, object_count=10)
        broker.merge_shard_ranges(shard_ranges)
        # shards exceed shrink threshold
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([], sequences)
        # compacted shards would exceed merge size
        sequences = find_compactible_shard_sequences(broker, 11, 19, -1, -1)
        self.assertEqual([], sequences)
        # shards exceed merge size
        sequences = find_compactible_shard_sequences(broker, 11, 9, -1, -1)
        self.assertEqual([], sequences)
        # shards exceed merge size and shrink threshold
        sequences = find_compactible_shard_sequences(broker, 10, 9, -1, -1)
        self.assertEqual([], sequences)
        # shards exceed *zero'd* merge size and shrink threshold
        sequences = find_compactible_shard_sequences(broker, 0, 0, -1, -1)
        self.assertEqual([], sequences)
        # shards exceed *negative* merge size and shrink threshold
        sequences = find_compactible_shard_sequences(broker, -1, -2, -1, -1)
        self.assertEqual([], sequences)
        # weird case: shards object count less than threshold but compacted
        # shards would exceed merge size
        sequences = find_compactible_shard_sequences(broker, 20, 19, -1, -1)
        self.assertEqual([], sequences)

    def test_find_compactible_nine_donors_one_acceptor(self):
        # one sequence that spans entire namespace but does not shrink to root
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE)
        shard_ranges[9].object_count = 11  # final shard too big to shrink
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges], sequences)

    def test_find_compactible_four_donors_two_acceptors(self):
        small_ranges = (2, 3, 4, 7)
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE)
        for i, sr in enumerate(shard_ranges):
            if i not in small_ranges:
                sr.object_count = 100
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges[2:6], shard_ranges[7:9]], sequences)

    def test_find_compactible_all_donors_shrink_to_root(self):
        # by default all shard ranges are small enough to shrink so the root
        # becomes the acceptor
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        own_sr = broker.get_own_shard_range()
        own_sr.update_state(ShardRange.SHARDED)
        broker.merge_shard_ranges(own_sr)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges + [own_sr]], sequences)

    def test_find_compactible_single_donor_shrink_to_root(self):
        # single shard range small enough to shrink so the root becomes the
        # acceptor
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', ''),), state=ShardRange.ACTIVE, timestamp=next(self.ts_iter))
        broker.merge_shard_ranges(shard_ranges)
        own_sr = broker.get_own_shard_range()
        own_sr.update_state(ShardRange.SHARDED, next(self.ts_iter))
        broker.merge_shard_ranges(own_sr)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges + [own_sr]], sequences)

        # update broker with donor/acceptor
        shard_ranges[0].update_state(ShardRange.SHRINKING, next(self.ts_iter))
        own_sr.update_state(ShardRange.ACTIVE, next(self.ts_iter))
        broker.merge_shard_ranges([shard_ranges[0], own_sr])
        # we don't find the same sequence again...
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([], sequences)
        # ...unless explicitly requesting it
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1,
                                                     include_shrinking=True)
        self.assertEqual([shard_ranges + [own_sr]], sequences)

    def test_find_compactible_overlapping_ranges(self):
        # unexpected case: all shrinkable, two overlapping sequences, one which
        # spans entire namespace; should not shrink to root
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'),  # overlaps form one sequence
             ('', 'j'), ('j', '')),  # second sequence spans entire namespace
            state=ShardRange.ACTIVE)
        shard_ranges[1].object_count = 11  # cannot shrink, so becomes acceptor
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges[:2], shard_ranges[2:]], sequences)

    def test_find_compactible_overlapping_ranges_with_ineligible_state(self):
        # unexpected case: one ineligible state shard range overlapping one
        # sequence which spans entire namespace; should not shrink to root
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'),  # overlap in ineligible state
             ('', 'j'), ('j', '')),  # sequence spans entire namespace
            state=[ShardRange.CREATED, ShardRange.ACTIVE, ShardRange.ACTIVE])
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges[1:]], sequences)

    def test_find_compactible_donors_but_no_suitable_acceptor(self):
        # if shard ranges are already shrinking, check that the final one is
        # not made into an acceptor if a suitable adjacent acceptor is not
        # found (unexpected scenario but possible in an overlap situation)
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=([ShardRange.SHRINKING] * 3 +
                   [ShardRange.SHARDING] +
                   [ShardRange.ACTIVE] * 6))
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges[4:]], sequences)

    def test_find_compactible_no_gaps(self):
        # verify that compactible sequences do not include gaps
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('e', 'f'),  # gap d - e
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        own_sr = broker.get_own_shard_range()
        own_sr.update_state(ShardRange.SHARDED)
        broker.merge_shard_ranges(own_sr)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([shard_ranges[:3], shard_ranges[3:]], sequences)

    def test_find_compactible_eligible_states(self):
        # verify that compactible sequences only include shards in valid states
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=[ShardRange.SHRINKING, ShardRange.ACTIVE,  # ok, shrinking
                   ShardRange.CREATED,  # ineligible state
                   ShardRange.ACTIVE, ShardRange.ACTIVE,  # ok
                   ShardRange.FOUND,  # ineligible state
                   ShardRange.SHARDED,  # ineligible state
                   ShardRange.ACTIVE, ShardRange.SHRINKING,  # ineligible state
                   ShardRange.SHARDING,  # ineligible state
                   ])
        broker.merge_shard_ranges(shard_ranges)
        own_sr = broker.get_own_shard_range()
        own_sr.update_state(ShardRange.SHARDED)
        broker.merge_shard_ranges(own_sr)
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1,
                                                     include_shrinking=True)
        self.assertEqual([shard_ranges[:2], shard_ranges[3:5], ], sequences)

    def test_find_compactible_max_shrinking(self):
        # verify option to limit the number of shrinking shards per acceptor
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        # limit to 1 donor per acceptor
        sequences = find_compactible_shard_sequences(broker, 10, 999, 1, -1)
        self.assertEqual([shard_ranges[n:n + 2] for n in range(0, 9, 2)],
                         sequences)

    def test_find_compactible_max_expanding(self):
        # verify option to limit the number of expanding shards per acceptor
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        # note: max_shrinking is set to 3 so that there is opportunity for more
        # than 2 acceptors
        sequences = find_compactible_shard_sequences(broker, 10, 999, 3, 2)
        self.assertEqual([shard_ranges[:4], shard_ranges[4:8]], sequences)
        # relax max_expanding
        sequences = find_compactible_shard_sequences(broker, 10, 999, 3, 3)
        self.assertEqual(
            [shard_ranges[:4], shard_ranges[4:8], shard_ranges[8:]], sequences)

        # commit the first two sequences to the broker
        for sr in shard_ranges[:3] + shard_ranges[4:7]:
            sr.update_state(ShardRange.SHRINKING,
                            state_timestamp=next(self.ts_iter))
        shard_ranges[3].lower = shard_ranges[0].lower
        shard_ranges[3].timestamp = next(self.ts_iter)
        shard_ranges[7].lower = shard_ranges[4].lower
        shard_ranges[7].timestamp = next(self.ts_iter)
        broker.merge_shard_ranges(shard_ranges)
        # we don't find them again...
        sequences = find_compactible_shard_sequences(broker, 10, 999, 3, 2)
        self.assertEqual([], sequences)
        # ...unless requested explicitly
        sequences = find_compactible_shard_sequences(broker, 10, 999, 3, 2,
                                                     include_shrinking=True)
        self.assertEqual([shard_ranges[:4], shard_ranges[4:8]], sequences)
        # we could find another if max_expanding is increased
        sequences = find_compactible_shard_sequences(broker, 10, 999, 3, 3)
        self.assertEqual([shard_ranges[8:]], sequences)

    def _do_test_find_compactible_shrink_threshold(self, broker, shard_ranges):
        # verify option to set the shrink threshold for compaction;
        # (n-2)th shard range has one extra object
        shard_ranges[-2].object_count = 11
        broker.merge_shard_ranges(shard_ranges)
        # with threshold set to 10 no shard ranges can be shrunk
        sequences = find_compactible_shard_sequences(broker, 10, 999, -1, -1)
        self.assertEqual([], sequences)
        # with threshold == 11 all but the final 2 shard ranges can be shrunk;
        # note: the (n-1)th shard range is NOT shrunk to root
        sequences = find_compactible_shard_sequences(broker, 11, 999, -1, -1)
        self.assertEqual([shard_ranges[:9]], sequences)

    def test_find_compactible_shrink_threshold(self):
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE, object_count=10)
        self._do_test_find_compactible_shrink_threshold(broker, shard_ranges)

    def test_find_compactible_shrink_threshold_with_tombstones(self):
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE, object_count=7, tombstones=3)
        self._do_test_find_compactible_shrink_threshold(broker, shard_ranges)

    def _do_test_find_compactible_expansion_limit(self, broker, shard_ranges):
        # verify option to limit the size of each acceptor after compaction
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 33, -1, -1)
        self.assertEqual([shard_ranges[:5], shard_ranges[5:]], sequences)
        shard_ranges[4].update_meta(20, 2000)
        shard_ranges[6].update_meta(28, 2700)
        broker.merge_shard_ranges(shard_ranges)
        sequences = find_compactible_shard_sequences(broker, 10, 33, -1, -1)
        self.assertEqual([shard_ranges[:4], shard_ranges[7:]], sequences)

    def test_find_compactible_expansion_limit(self):
        # verify option to limit the size of each acceptor after compaction
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE, object_count=6)
        self._do_test_find_compactible_expansion_limit(broker, shard_ranges)

    def test_find_compactible_expansion_limit_with_tombstones(self):
        # verify option to limit the size of each acceptor after compaction
        broker = self._make_broker()
        shard_ranges = self._make_shard_ranges(
            (('', 'b'), ('b', 'c'), ('c', 'd'), ('d', 'e'), ('e', 'f'),
             ('f', 'g'), ('g', 'h'), ('h', 'i'), ('i', 'j'), ('j', '')),
            state=ShardRange.ACTIVE, object_count=1, tombstones=5)
        self._do_test_find_compactible_expansion_limit(broker, shard_ranges)

    def test_is_sharding_candidate(self):
        for state in ShardRange.STATES:
            for object_count in (9, 10, 11):
                sr = ShardRange('.shards_a/c', next(self.ts_iter), '', '',
                                state=state, object_count=object_count,
                                tombstones=100)  # tombstones not considered
                with annotate_failure('%s %s' % (state, object_count)):
                    if state == ShardRange.ACTIVE and object_count >= 10:
                        self.assertTrue(is_sharding_candidate(sr, 10))
                    else:
                        self.assertFalse(is_sharding_candidate(sr, 10))

    def test_is_shrinking_candidate(self):
        def do_check_true(state, ok_states):
            # shard range has 9 objects
            sr = ShardRange('.shards_a/c', next(self.ts_iter), '', '',
                            state=state, object_count=9)
            self.assertTrue(is_shrinking_candidate(sr, 10, 9, ok_states))
            # shard range has 9 rows
            sr = ShardRange('.shards_a/c', next(self.ts_iter), '', '',
                            state=state, object_count=4, tombstones=5)
            self.assertTrue(is_shrinking_candidate(sr, 10, 9, ok_states))

        do_check_true(ShardRange.ACTIVE, (ShardRange.ACTIVE,))
        do_check_true(ShardRange.ACTIVE,
                      (ShardRange.ACTIVE, ShardRange.SHRINKING))
        do_check_true(ShardRange.SHRINKING,
                      (ShardRange.ACTIVE, ShardRange.SHRINKING))

        def do_check_false(state, object_count, tombstones):
            states = (ShardRange.ACTIVE, ShardRange.SHRINKING)
            # shard range has 10 objects
            sr = ShardRange('.shards_a/c', next(self.ts_iter), '', '',
                            state=state, object_count=object_count,
                            tombstones=tombstones)
            self.assertFalse(is_shrinking_candidate(sr, 10, 20))
            self.assertFalse(is_shrinking_candidate(sr, 10, 20, states))
            self.assertFalse(is_shrinking_candidate(sr, 10, 9))
            self.assertFalse(is_shrinking_candidate(sr, 10, 9, states))
            self.assertFalse(is_shrinking_candidate(sr, 20, 9))
            self.assertFalse(is_shrinking_candidate(sr, 20, 9, states))

        for state in ShardRange.STATES:
            for object_count in (10, 11):
                with annotate_failure('%s %s' % (state, object_count)):
                    do_check_false(state, object_count, 0)
            for tombstones in (10, 11):
                with annotate_failure('%s %s' % (state, tombstones)):
                    do_check_false(state, 0, tombstones)
            for tombstones in (5, 6):
                with annotate_failure('%s %s' % (state, tombstones)):
                    do_check_false(state, 5, tombstones)

    def test_find_and_rank_whole_path_split(self):
        ts_0 = next(self.ts_iter)
        ts_1 = next(self.ts_iter)
        bounds_0 = (
            ('', 'f'),
            ('f', 'k'),
            ('k', 's'),
            ('s', 'x'),
            ('x', ''),
        )
        bounds_1 = (
            ('', 'g'),
            ('g', 'l'),
            ('l', 't'),
            ('t', 'y'),
            ('y', ''),
        )
        # path with newer timestamp wins
        ranges_0 = self._make_shard_ranges(bounds_0, ShardRange.ACTIVE,
                                           timestamp=ts_0)
        ranges_1 = self._make_shard_ranges(bounds_1, ShardRange.ACTIVE,
                                           timestamp=ts_1)

        paths = find_paths(ranges_0 + ranges_1)
        self.assertEqual(2, len(paths))
        self.assertIn(ranges_0, paths)
        self.assertIn(ranges_1, paths)
        own_sr = ShardRange('a/c', Timestamp.now())
        self.assertEqual(
            [
                ranges_1,  # complete and newer timestamp
                ranges_0,  # complete
            ],
            rank_paths(paths, own_sr))

        # but object_count trumps matching timestamp
        ranges_0 = self._make_shard_ranges(bounds_0, ShardRange.ACTIVE,
                                           timestamp=ts_1, object_count=1)
        paths = find_paths(ranges_0 + ranges_1)
        self.assertEqual(2, len(paths))
        self.assertIn(ranges_0, paths)
        self.assertIn(ranges_1, paths)
        self.assertEqual(
            [
                ranges_0,  # complete with more objects
                ranges_1,  # complete
            ],
            rank_paths(paths, own_sr))

    def test_find_and_rank_two_sub_path_splits(self):
        ts_0 = next(self.ts_iter)
        ts_1 = next(self.ts_iter)
        ts_2 = next(self.ts_iter)
        bounds_0 = (
            ('', 'a'),
            ('a', 'm'),
            ('m', 'p'),
            ('p', 't'),
            ('t', 'x'),
            ('x', 'y'),
            ('y', ''),
        )
        bounds_1 = (
            ('a', 'g'),  # split at 'a'
            ('g', 'l'),
            ('l', 'm'),  # rejoin at 'm'
        )
        bounds_2 = (
            ('t', 'y'),  # split at 't', rejoin at 'y'
        )
        ranges_0 = self._make_shard_ranges(bounds_0, ShardRange.ACTIVE,
                                           timestamp=ts_0)
        ranges_1 = self._make_shard_ranges(bounds_1, ShardRange.ACTIVE,
                                           timestamp=ts_1, object_count=1)
        ranges_2 = self._make_shard_ranges(bounds_2, ShardRange.ACTIVE,
                                           timestamp=ts_2, object_count=1)
        # all paths are complete
        mix_path_0 = ranges_0[:1] + ranges_1 + ranges_0[2:]  # 3 objects
        mix_path_1 = ranges_0[:4] + ranges_2 + ranges_0[6:]  # 1 object
        mix_path_2 = (ranges_0[:1] + ranges_1 + ranges_0[2:4] + ranges_2 +
                      ranges_0[6:])  # 4 objects
        paths = find_paths(ranges_0 + ranges_1 + ranges_2)
        self.assertEqual(4, len(paths))
        self.assertIn(ranges_0, paths)
        self.assertIn(mix_path_0, paths)
        self.assertIn(mix_path_1, paths)
        self.assertIn(mix_path_2, paths)
        own_sr = ShardRange('a/c', Timestamp.now())
        self.assertEqual(
            [
                mix_path_2,  # has 4 objects, 3 different timestamps
                mix_path_0,  # has 3 objects, 2 different timestamps
                mix_path_1,  # has 1 object, 2 different timestamps
                ranges_0,  # has 0 objects, 1 timestamp
            ],
            rank_paths(paths, own_sr)
        )

    def test_find_and_rank_most_cleave_progress(self):
        ts_0 = next(self.ts_iter)
        ts_1 = next(self.ts_iter)
        ts_2 = next(self.ts_iter)
        bounds_0 = (
            ('', 'f'),
            ('f', 'k'),
            ('k', 'p'),
            ('p', '')
        )
        bounds_1 = (
            ('', 'g'),
            ('g', 'l'),
            ('l', 'q'),
            ('q', '')
        )
        bounds_2 = (
            ('', 'r'),
            ('r', '')
        )
        ranges_0 = self._make_shard_ranges(
            bounds_0, [ShardRange.CLEAVED] * 3 + [ShardRange.CREATED],
            timestamp=ts_1, object_count=1)
        ranges_1 = self._make_shard_ranges(
            bounds_1, [ShardRange.CLEAVED] * 4,
            timestamp=ts_0)
        ranges_2 = self._make_shard_ranges(
            bounds_2, [ShardRange.CLEAVED, ShardRange.CREATED],
            timestamp=ts_2, object_count=1)
        paths = find_paths(ranges_0 + ranges_1 + ranges_2)
        self.assertEqual(3, len(paths))
        own_sr = ShardRange('a/c', Timestamp.now())
        self.assertEqual(
            [
                ranges_1,  # cleaved to end
                ranges_2,  # cleaved to r
                ranges_0,  # cleaved to p
            ],
            rank_paths(paths, own_sr)
        )
        ranges_2 = self._make_shard_ranges(
            bounds_2, [ShardRange.ACTIVE] * 2,
            timestamp=ts_2, object_count=1)
        paths = find_paths(ranges_0 + ranges_1 + ranges_2)
        self.assertEqual(
            [
                ranges_2,  # active to end, newer timestamp
                ranges_1,  # cleaved to r
                ranges_0,  # cleaved to p
            ],
            rank_paths(paths, own_sr)
        )

    def test_find_and_rank_no_complete_path(self):
        ts_0 = next(self.ts_iter)
        ts_1 = next(self.ts_iter)
        ts_2 = next(self.ts_iter)
        bounds_0 = (
            ('', 'f'),
            ('f', 'k'),
            ('k', 'm'),
        )
        bounds_1 = (
            ('', 'g'),
            ('g', 'l'),
            ('l', 'n'),
        )
        bounds_2 = (
            ('', 'l'),
        )
        ranges_0 = self._make_shard_ranges(bounds_0, ShardRange.ACTIVE,
                                           timestamp=ts_0)
        ranges_1 = self._make_shard_ranges(bounds_1, ShardRange.ACTIVE,
                                           timestamp=ts_1, object_count=1)
        ranges_2 = self._make_shard_ranges(bounds_2, ShardRange.ACTIVE,
                                           timestamp=ts_2, object_count=1)
        mix_path_0 = ranges_2 + ranges_1[2:]
        paths = find_paths(ranges_0 + ranges_1 + ranges_2)
        self.assertEqual(3, len(paths))
        self.assertIn(ranges_0, paths)
        self.assertIn(ranges_1, paths)
        self.assertIn(mix_path_0, paths)
        own_sr = ShardRange('a/c', Timestamp.now())
        self.assertEqual(
            [
                ranges_1,  # cleaved to n, one timestamp
                mix_path_0,  # cleaved to n, has two different timestamps
                ranges_0,  # cleaved to m
            ],
            rank_paths(paths, own_sr)
        )

    def test_find_paths_with_gaps(self):
        bounds = (
            # gap
            ('a', 'f'),
            ('f', 'm'),  # overlap
            ('k', 'p'),
            # gap
            ('q', 'y')
            # gap
        )
        ranges = self._make_shard_ranges(
            bounds, ShardRange.ACTIVE,
            timestamp=next(self.ts_iter), object_count=1)
        paths_with_gaps = find_paths_with_gaps(ranges)
        self.assertEqual(3, len(paths_with_gaps), paths_with_gaps)
        self.assertEqual(
            [(ShardRange.MIN, ShardRange.MIN),
             (ShardRange.MIN, 'a'),
             ('a', 'm')],
            [(r.lower, r.upper) for r in paths_with_gaps[0]]
        )
        self.assertEqual(
            [('k', 'p'),
             ('p', 'q'),
             ('q', 'y')],
            [(r.lower, r.upper) for r in paths_with_gaps[1]]
        )
        self.assertEqual(
            [('q', 'y'),
             ('y', ShardRange.MAX),
             (ShardRange.MAX, ShardRange.MAX)],
            [(r.lower, r.upper) for r in paths_with_gaps[2]]
        )

        range_of_interest = ShardRange('test/range', next(self.ts_iter))
        range_of_interest.lower = 'a'
        paths_with_gaps = find_paths_with_gaps(ranges, range_of_interest)
        self.assertEqual(2, len(paths_with_gaps), paths_with_gaps)
        self.assertEqual(
            [('k', 'p'),
             ('p', 'q'),
             ('q', 'y')],
            [(r.lower, r.upper) for r in paths_with_gaps[0]]
        )
        self.assertEqual(
            [('q', 'y'),
             ('y', ShardRange.MAX),
             (ShardRange.MAX, ShardRange.MAX)],
            [(r.lower, r.upper) for r in paths_with_gaps[1]]
        )

        range_of_interest.lower = 'b'
        range_of_interest.upper = 'x'
        paths_with_gaps = find_paths_with_gaps(ranges, range_of_interest)
        self.assertEqual(1, len(paths_with_gaps), paths_with_gaps)
        self.assertEqual(
            [('k', 'p'),
             ('p', 'q'),
             ('q', 'y')],
            [(r.lower, r.upper) for r in paths_with_gaps[0]]
        )

        range_of_interest.upper = 'c'
        paths_with_gaps = find_paths_with_gaps(ranges, range_of_interest)
        self.assertFalse(paths_with_gaps)

    def test_find_overlapping_ranges(self):
        now_ts = next(self.ts_iter)
        past_ts = Timestamp(float(now_ts) - 61)
        root_sr = ShardRange('a/c', past_ts, state=ShardRange.SHARDED)
        bounds = (
            ('', 'a'),
            ('a', 'f'),  # the 'parent_range' in this test.
            ('f', 'm'),  # shard range overlaps with the next.
            ('k', 'p'),
            ('p', 'y'),
            ('y', '')
        )
        ranges = [
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', root_sr.container, past_ts,
                    index),
                past_ts, lower, upper, object_count=1,
                state=ShardRange.SHARDED)
            for index, (lower, upper) in enumerate(bounds)]
        parent_range = ranges[1]
        child_ranges = [
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_range.container, past_ts, 0),
                past_ts, lower='a', upper='c', object_count=1,
                state=ShardRange.CLEAVED),
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_range.container, past_ts, 1),
                past_ts, lower='c', upper='f', object_count=1,
                state=ShardRange.CLEAVED)]
        overlapping_ranges = find_overlapping_ranges(ranges)
        self.assertEqual({(ranges[2], ranges[3])}, overlapping_ranges)
        overlapping_ranges = find_overlapping_ranges(
            [ranges[1]] + child_ranges)
        self.assertEqual(
            {(child_ranges[0], child_ranges[1], ranges[1])},
            overlapping_ranges)
        overlapping_ranges = find_overlapping_ranges(
            [ranges[1]] + child_ranges, exclude_parent_child=True)
        self.assertEqual(0, len(overlapping_ranges))
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=float(now_ts)):
            overlapping_ranges = find_overlapping_ranges(
                [ranges[1]] + child_ranges, exclude_parent_child=True,
                time_period=61)
            self.assertEqual(0, len(overlapping_ranges))
            overlapping_ranges = find_overlapping_ranges(
                [ranges[1]] + child_ranges, exclude_parent_child=True,
                time_period=60)
            self.assertEqual(
                {(child_ranges[0], child_ranges[1], ranges[1])},
                overlapping_ranges)
        overlapping_ranges = find_overlapping_ranges(
            ranges + child_ranges)
        self.assertEqual(
            {(child_ranges[0],
              child_ranges[1],
              ranges[1]),
             (ranges[2],
              ranges[3])},
            overlapping_ranges)
        overlapping_ranges = find_overlapping_ranges(
            ranges + child_ranges, exclude_parent_child=True)
        self.assertEqual({(ranges[2], ranges[3])}, overlapping_ranges)
        with mock.patch(
                'swift.container.sharder.time.time',
                return_value=float(now_ts)):
            overlapping_ranges = find_overlapping_ranges(
                ranges + child_ranges, exclude_parent_child=True,
                time_period=61)
            self.assertEqual({(ranges[2], ranges[3])}, overlapping_ranges)
            overlapping_ranges = find_overlapping_ranges(
                ranges + child_ranges, exclude_parent_child=True,
                time_period=60)
            self.assertEqual(
                {(child_ranges[0],
                  child_ranges[1],
                  ranges[1]),
                 (ranges[2],
                 ranges[3])},
                overlapping_ranges)

    def test_update_own_shard_range_stats(self):
        broker = self._make_broker()
        ts = next(self.ts_iter)
        broker.merge_items([
            {'name': 'obj%02d' % i, 'created_at': ts.internal, 'size': 9,
             'content_type': 'application/octet-stream', 'etag': 'not-really',
             'deleted': 0, 'storage_policy_index': 0,
             'ctype_timestamp': ts.internal, 'meta_timestamp': ts.internal}
            for i in range(100)])

        self.assertEqual(100, broker.get_info()['object_count'])
        self.assertEqual(900, broker.get_info()['bytes_used'])

        own_sr = broker.get_own_shard_range()
        self.assertEqual(0, own_sr.object_count)
        self.assertEqual(0, own_sr.bytes_used)
        # own_sr is updated...
        update_own_shard_range_stats(broker, own_sr)
        self.assertEqual(100, own_sr.object_count)
        self.assertEqual(900, own_sr.bytes_used)
        # ...but not persisted
        own_sr = broker.get_own_shard_range()
        self.assertEqual(0, own_sr.object_count)
        self.assertEqual(0, own_sr.bytes_used)


class TestContainerSharderConf(unittest.TestCase):
    def test_default(self):
        expected = {'shard_container_threshold': 1000000,
                    'max_shrinking': 1,
                    'max_expanding': -1,
                    'shard_scanner_batch_size': 10,
                    'cleave_batch_size': 2,
                    'cleave_row_batch_size': 10000,
                    'broker_timeout': 60,
                    'recon_candidates_limit': 5,
                    'recon_sharded_timeout': 43200,
                    'container_sharding_timeout': 172800,
                    'conn_timeout': 5.0,
                    'auto_shard': False,
                    'shrink_threshold': 100000,
                    'expansion_limit': 750000,
                    'rows_per_shard': 500000,
                    'minimum_shard_size': 100000}
        self.assertEqual(expected, vars(ContainerSharderConf()))
        self.assertEqual(expected, vars(ContainerSharderConf(None)))
        self.assertEqual(expected, DEFAULT_SHARDER_CONF)

    def test_conf(self):
        conf = {'shard_container_threshold': 2000000,
                'max_shrinking': 2,
                'max_expanding': 3,
                'shard_scanner_batch_size': 11,
                'cleave_batch_size': 4,
                'cleave_row_batch_size': 50000,
                'broker_timeout': 61,
                'recon_candidates_limit': 6,
                'recon_sharded_timeout': 43201,
                'container_sharding_timeout': 172801,
                'conn_timeout': 5.1,
                'auto_shard': True,
                'shrink_threshold': 100001,
                'expansion_limit': 750001,
                'rows_per_shard': 500001,
                'minimum_shard_size': 20}
        expected = dict(conf)
        conf.update({'unexpected': 'option'})
        self.assertEqual(expected, vars(ContainerSharderConf(conf)))

    def test_deprecated_percent_conf(self):
        base_conf = {'shard_container_threshold': 2000000,
                     'max_shrinking': 2,
                     'max_expanding': 3,
                     'shard_scanner_batch_size': 11,
                     'cleave_batch_size': 4,
                     'cleave_row_batch_size': 50000,
                     'broker_timeout': 61,
                     'recon_candidates_limit': 6,
                     'recon_sharded_timeout': 43201,
                     'container_sharding_timeout': 172801,
                     'conn_timeout': 5.1,
                     'auto_shard': True,
                     'minimum_shard_size': 1}

        # percent options work
        deprecated_conf = {'shard_shrink_point': 9,
                           'shard_shrink_merge_point': 71}
        expected = dict(base_conf, rows_per_shard=1000000,
                        shrink_threshold=180000, expansion_limit=1420000)
        conf = dict(base_conf)
        conf.update(deprecated_conf)
        self.assertEqual(expected, vars(ContainerSharderConf(conf)))

        # check absolute options override percent options
        conf.update({'shrink_threshold': 100001,
                     'expansion_limit': 750001})

        expected = dict(base_conf, rows_per_shard=1000000,
                        shrink_threshold=100001, expansion_limit=750001)
        conf.update(deprecated_conf)
        self.assertEqual(expected, vars(ContainerSharderConf(conf)))

    def test_bad_values(self):
        not_positive_int = [0, -1, 'bad']
        not_int = not_float = ['bad']
        not_percent = ['bad', -1, 101, -0.1, 100.1]
        bad = {'shard_container_threshold': not_positive_int,
               'max_shrinking': not_int,
               'max_expanding': not_int,
               'shard_scanner_batch_size': not_positive_int,
               'cleave_batch_size': not_positive_int,
               'cleave_row_batch_size': not_positive_int,
               'broker_timeout': not_positive_int,
               'recon_candidates_limit': not_int,
               'recon_sharded_timeout': not_int,
               'conn_timeout': not_float,
               # 'auto_shard': anything can be passed to config_true_value
               'shrink_threshold': not_int,
               'expansion_limit': not_int,
               'shard_shrink_point': not_percent,
               'shard_shrink_merge_point': not_percent,
               'minimum_shard_size': not_positive_int}

        for key, bad_values in bad.items():
            for bad_value in bad_values:
                with self.assertRaises(
                        ValueError, msg='{%s : %s}' % (key, bad_value)) as cm:
                    ContainerSharderConf({key: bad_value})
                self.assertIn('Error setting %s' % key, str(cm.exception))

    def test_validate(self):
        def assert_bad(conf):
            with self.assertRaises(ValueError):
                ContainerSharderConf.validate_conf(ContainerSharderConf(conf))

        def assert_ok(conf):
            try:
                ContainerSharderConf.validate_conf(ContainerSharderConf(conf))
            except ValueError as err:
                self.fail('Unexpected ValueError: %s' % err)

        assert_ok({})
        assert_ok({'minimum_shard_size': 100,
                   'shrink_threshold': 100,
                   'rows_per_shard': 100})
        assert_bad({'minimum_shard_size': 100})
        assert_bad({'shrink_threshold': 100001})
        assert_ok({'minimum_shard_size': 100,
                   'shrink_threshold': 100})
        assert_bad({'minimum_shard_size': 100,
                    'shrink_threshold': 100,
                    'rows_per_shard': 99})

        assert_ok({'shard_container_threshold': 100,
                   'rows_per_shard': 99})
        assert_bad({'shard_container_threshold': 100,
                    'rows_per_shard': 100})
        assert_bad({'rows_per_shard': 10000001})

        assert_ok({'shard_container_threshold': 100,
                   'expansion_limit': 99})
        assert_bad({'shard_container_threshold': 100,
                    'expansion_limit': 100})
        assert_bad({'expansion_limit': 100000001})

    def test_validate_subset(self):
        # verify that validation is only applied for keys that exist in the
        # given namespace
        def assert_bad(conf):
            with self.assertRaises(ValueError):
                ContainerSharderConf.validate_conf(argparse.Namespace(**conf))

        def assert_ok(conf):
            try:
                ContainerSharderConf.validate_conf(argparse.Namespace(**conf))
            except ValueError as err:
                self.fail('Unexpected ValueError: %s' % err)

        assert_ok({})
        assert_ok({'minimum_shard_size': 100,
                   'shrink_threshold': 100,
                   'rows_per_shard': 100})
        assert_ok({'minimum_shard_size': 100})
        assert_ok({'shrink_threshold': 100001})
        assert_ok({'minimum_shard_size': 100,
                   'shrink_threshold': 100})
        assert_bad({'minimum_shard_size': 100,
                    'shrink_threshold': 100,
                    'rows_per_shard': 99})

        assert_ok({'shard_container_threshold': 100,
                   'rows_per_shard': 99})
        assert_bad({'shard_container_threshold': 100,
                    'rows_per_shard': 100})
        assert_ok({'rows_per_shard': 10000001})

        assert_ok({'shard_container_threshold': 100,
                   'expansion_limit': 99})
        assert_bad({'shard_container_threshold': 100,
                    'expansion_limit': 100})
        assert_ok({'expansion_limit': 100000001})

    def test_combine_shard_ranges(self):
        ts_iter = make_timestamp_iter()
        this = ShardRange('a/o', next(ts_iter).internal)
        that = ShardRange('a/o', next(ts_iter).internal)
        actual = combine_shard_ranges([dict(this)], [dict(that)])
        self.assertEqual([dict(that)], [dict(sr) for sr in actual])
        actual = combine_shard_ranges([dict(that)], [dict(this)])
        self.assertEqual([dict(that)], [dict(sr) for sr in actual])

        ts = next(ts_iter).internal
        this = ShardRange('a/o', ts, state=ShardRange.ACTIVE,
                          state_timestamp=next(ts_iter))
        that = ShardRange('a/o', ts, state=ShardRange.CREATED,
                          state_timestamp=next(ts_iter))
        actual = combine_shard_ranges([dict(this)], [dict(that)])
        self.assertEqual([dict(that)], [dict(sr) for sr in actual])
        actual = combine_shard_ranges([dict(that)], [dict(this)])
        self.assertEqual([dict(that)], [dict(sr) for sr in actual])

        that.update_meta(1, 2, meta_timestamp=next(ts_iter))
        this.update_meta(3, 4, meta_timestamp=next(ts_iter))
        expected = that.copy(object_count=this.object_count,
                             bytes_used=this.bytes_used,
                             meta_timestamp=this.meta_timestamp)
        actual = combine_shard_ranges([dict(this)], [dict(that)])
        self.assertEqual([dict(expected)], [dict(sr) for sr in actual])
        actual = combine_shard_ranges([dict(that)], [dict(this)])
        self.assertEqual([dict(expected)], [dict(sr) for sr in actual])

        this = ShardRange('a/o', next(ts_iter).internal)
        that = ShardRange('a/o', next(ts_iter).internal, deleted=True)
        actual = combine_shard_ranges([dict(this)], [dict(that)])
        self.assertFalse(actual, [dict(sr) for sr in actual])
        actual = combine_shard_ranges([dict(that)], [dict(this)])
        self.assertFalse(actual, [dict(sr) for sr in actual])

        this = ShardRange('a/o', next(ts_iter).internal, deleted=True)
        that = ShardRange('a/o', next(ts_iter).internal)
        actual = combine_shard_ranges([dict(this)], [dict(that)])
        self.assertEqual([dict(that)], [dict(sr) for sr in actual])
        actual = combine_shard_ranges([dict(that)], [dict(this)])
        self.assertEqual([dict(that)], [dict(sr) for sr in actual])
