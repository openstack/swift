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
import hashlib
import json
import random

import eventlet
import os
import shutil
from contextlib import contextmanager
from tempfile import mkdtemp

import mock
import unittest

from collections import defaultdict

import time

from copy import deepcopy

import six

from swift.common import internal_client
from swift.container import replicator
from swift.container.backend import ContainerBroker, UNSHARDED, SHARDING, \
    SHARDED, DATADIR
from swift.container.sharder import ContainerSharder, sharding_enabled, \
    CleavingContext, DEFAULT_SHARD_SHRINK_POINT, \
    DEFAULT_SHARD_CONTAINER_THRESHOLD
from swift.common.utils import ShardRange, Timestamp, hash_path, \
    encode_timestamps, parse_db_filename, quorum_size, Everything
from test import annotate_failure

from test.unit import FakeLogger, debug_logger, FakeRing, \
    make_timestamp_iter, unlink_files, mocked_http_conn, mock_timestamp_now, \
    attach_fake_replication_rpc


class BaseTestSharder(unittest.TestCase):
    def setUp(self):
        self.tempdir = mkdtemp()
        self.ts_iter = make_timestamp_iter()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _assert_shard_ranges_equal(self, expected, actual):
        self.assertEqual([dict(sr) for sr in expected],
                         [dict(sr) for sr in actual])

    def _make_broker(self, account='a', container='c', epoch=None,
                     device='sda', part=0, hash_=None):
        hash_ = hash_ or hashlib.md5(container.encode('utf-8')).hexdigest()
        datadir = os.path.join(
            self.tempdir, device, 'containers', str(part), hash_[-3:], hash_)
        if epoch:
            filename = '%s_%s.db' % (hash, epoch)
        else:
            filename = hash_ + '.db'
        db_file = os.path.join(datadir, filename)
        broker = ContainerBroker(
            db_file, account=account, container=container,
            logger=debug_logger())
        broker.initialize()
        return broker

    def _make_sharding_broker(self, account='a', container='c',
                              shard_bounds=(('', 'middle'), ('middle', ''))):
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

    def _make_shard_ranges(self, bounds, state=None, object_count=0):
        return [ShardRange('.shards_a/c_%s' % upper, Timestamp.now(),
                           lower, upper, state=state,
                           object_count=object_count)
                for lower, upper in bounds]

    def ts_encoded(self):
        # make a unique timestamp string with multiple timestamps encoded;
        # use different deltas between component timestamps
        timestamps = [next(self.ts_iter) for i in range(4)]
        return encode_timestamps(
            timestamps[0], timestamps[1], timestamps[3])


class TestSharder(BaseTestSharder):
    def test_init(self):
        def do_test(conf, expected):
            with mock.patch(
                    'swift.container.sharder.internal_client.InternalClient') \
                    as mock_ic:
                with mock.patch('swift.common.db_replicator.ring.Ring') \
                        as mock_ring:
                    mock_ring.return_value = mock.MagicMock()
                    mock_ring.return_value.replica_count = 3
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
            'databases_per_second': 50,
            'cleave_row_batch_size': 10000,
            'node_timeout': 10, 'conn_timeout': 5,
            'rsync_compress': False,
            'rsync_module': '{replication_ip}::container',
            'reclaim_age': 86400 * 7,
            'shard_shrink_point': 0.25,
            'shrink_merge_point': 0.75,
            'shard_container_threshold': 1000000,
            'split_size': 500000,
            'cleave_batch_size': 2,
            'scanner_batch_size': 10,
            'rcache': '/var/cache/swift/container.recon',
            'shards_account_prefix': '.shards_',
            'auto_shard': False,
            'recon_candidates_limit': 5,
            'shard_replication_quorum': 2,
            'existing_shard_replication_quorum': 2
        }
        mock_ic = do_test({}, expected)
        mock_ic.assert_called_once_with(
            '/etc/swift/internal-client.conf', 'Swift Container Sharder', 3,
            allow_modify_pipeline=False)

        conf = {
            'mount_check': False, 'bind_ip': '10.11.12.13', 'bind_port': 62010,
            'per_diff': 2000, 'max_diffs': 200, 'interval': 60,
            'databases_per_second': 5,
            'cleave_row_batch_size': 3000,
            'node_timeout': 20, 'conn_timeout': 1,
            'rsync_compress': True,
            'rsync_module': '{replication_ip}::container_sda/',
            'reclaim_age': 86400 * 14,
            'shard_shrink_point': 35,
            'shard_shrink_merge_point': 85,
            'shard_container_threshold': 20000000,
            'cleave_batch_size': 4,
            'shard_scanner_batch_size': 8,
            'request_tries': 2,
            'internal_client_conf_path': '/etc/swift/my-sharder-ic.conf',
            'recon_cache_path': '/var/cache/swift-alt',
            'auto_create_account_prefix': '...',
            'auto_shard': 'yes',
            'recon_candidates_limit': 10,
            'shard_replication_quorum': 1,
            'existing_shard_replication_quorum': 0
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
            'shard_shrink_point': 0.35,
            'shrink_merge_point': 0.85,
            'shard_container_threshold': 20000000,
            'split_size': 10000000,
            'cleave_batch_size': 4,
            'scanner_batch_size': 8,
            'rcache': '/var/cache/swift-alt/container.recon',
            'shards_account_prefix': '...shards_',
            'auto_shard': True,
            'recon_candidates_limit': 10,
            'shard_replication_quorum': 1,
            'existing_shard_replication_quorum': 0
        }
        mock_ic = do_test(conf, expected)
        mock_ic.assert_called_once_with(
            '/etc/swift/my-sharder-ic.conf', 'Swift Container Sharder', 2,
            allow_modify_pipeline=False)

        expected.update({'shard_replication_quorum': 3,
                         'existing_shard_replication_quorum': 3})
        conf.update({'shard_replication_quorum': 4,
                     'existing_shard_replication_quorum': 4})
        do_test(conf, expected)

        with self.assertRaises(ValueError) as cm:
            do_test({'shard_shrink_point': 101}, {})
        self.assertIn(
            'greater than 0, less than 100, not "101"', str(cm.exception))
        self.assertIn('shard_shrink_point', str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            do_test({'shard_shrink_merge_point': 101}, {})
        self.assertIn(
            'greater than 0, less than 100, not "101"', str(cm.exception))
        self.assertIn('shard_shrink_merge_point', str(cm.exception))

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

    def _assert_stats(self, expected, sharder, category):
        # assertEqual doesn't work with a defaultdict
        stats = sharder.stats['sharding'][category]
        for k, v in expected.items():
            actual = stats[k]
            self.assertEqual(
                v, actual, 'Expected %s but got %s for %s in %s' %
                           (v, actual, k, stats))
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
            sharder._increment_stat('scanned', 'found', step=4)
        expected = {'success': 2,
                    'failure': 1,
                    'completed': 1}
        self._assert_stats(expected, sharder, 'visited')
        self._assert_stats({'success': 1}, sharder, 'cleaved')
        self._assert_stats({'found': 4}, sharder, 'scanned')

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
        counts = sharder.logger.get_increment_counts()
        self.assertEqual(2, counts.get('visited_success'))
        self.assertEqual(1, counts.get('visited_failure'))
        self.assertIsNone(counts.get('visited_completed'))

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
                'audit_root': {'attempted': 5, 'success': 4, 'failure': 1},
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
            sharder.logger = debug_logger()
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
                sharder._local_device_ids = {'stale_node_id'}
                sharder._one_shard_cycle(Everything(), Everything())

            lines = sharder.logger.get_lines_for_level('warning')
            expected = 'Skipping %s as it is not mounted' % \
                unmounted_dev['device']
            self.assertIn(expected, lines[0])
            self.assertEqual(device_ids, sharder._local_device_ids)
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
                sharder._local_device_ids = {'stale_node_id'}
                sharder._one_shard_cycle(Everything(), Everything())

            lines = sharder.logger.get_lines_for_level('warning')
            expected = 'Skipping %s as it is not mounted' % \
                unmounted_dev['device']
            self.assertIn(expected, lines[0])
            self.assertEqual(device_ids, sharder._local_device_ids)
            self.assertEqual(3, mock_process_broker.call_count)
            processed_paths = [call[0][0].path
                               for call in mock_process_broker.call_args_list]
            self.assertEqual({'a/c0', 'a/c1', 'a/c2'}, set(processed_paths))
            lines = sharder.logger.get_lines_for_level('error')
            self.assertIn('Unhandled exception while processing', lines[0])
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
                sharder._local_device_ids = {999}
                sharder._one_shard_cycle(Everything(), Everything())

            self.assertEqual(device_ids, sharder._local_device_ids)
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
        conf = conf or {}
        conf['devices'] = self.tempdir
        with mock.patch(
                'swift.container.sharder.internal_client.InternalClient'):
            with mock.patch(
                    'swift.common.db_replicator.ring.Ring',
                    lambda *args, **kwargs: FakeRing(replicas=replicas)):
                sharder = ContainerSharder(conf, logger=FakeLogger())
                sharder._local_device_ids = {0, 1, 2}
                sharder._replicate_object = mock.MagicMock(
                    return_value=(True, [True] * sharder.ring.replica_count))
                yield sharder

    def _get_raw_object_records(self, broker):
        # use list_objects_iter with no-op transform_func to get back actual
        # un-transformed rows with encoded timestamps
        return [list(obj) for obj in broker.list_objects_iter(
            10, '', '', '', '', include_deleted=None, all_policies=True,
            transform_func=lambda record: record)]

    def _check_objects(self, expected_objs, shard_db):
        shard_broker = ContainerBroker(shard_db)
        shard_objs = self._get_raw_object_records(shard_broker)
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
                            'X-Backend-Override-Deleted': 'true'}
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
                    'min_time': mock.ANY, 'max_time': mock.ANY}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])
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
                    'min_time': mock.ANY, 'max_time': mock.ANY}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])

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
        self.assertEqual('where', context.cursor)
        self.assertEqual(9, context.cleave_to_row)
        self.assertEqual(9, context.max_row)
        self.assertEqual(3, context.ranges_done)
        self.assertEqual(1, context.ranges_todo)

        unlink_files(expected_shard_dbs)

        # run cleave again - should process the fourth range
        with self._mock_sharder(conf=conf) as sharder:
            sharder.logger = debug_logger()
            self.assertFalse(sharder._cleave(broker))

        expected = {'attempted': 1, 'success': 1, 'failure': 0,
                    'min_time': mock.ANY, 'max_time': mock.ANY}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])

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
                    'min_time': mock.ANY, 'max_time': mock.ANY}
        stats = self._assert_stats(expected, sharder, 'cleaved')
        self.assertIsInstance(stats['min_time'], float)
        self.assertIsInstance(stats['max_time'], float)
        self.assertLessEqual(stats['min_time'], stats['max_time'])

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
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.CREATED)
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
        self.assertEqual(shard_ranges[1].upper_str, context.cursor)
        self.assertEqual(8, context.cleave_to_row)
        self.assertEqual(8, context.max_row)

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
        shard_ranges[0].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[0], updated_shard_ranges[0])
        shard_ranges[1].bytes_used = 103
        shard_ranges[1].object_count = 2
        shard_ranges[1].state = ShardRange.CLEAVED
        self._check_shard_range(shard_ranges[1], updated_shard_ranges[1])
        self._check_objects(objects[:4], expected_shard_dbs[0])
        self._check_objects(objects[4:7], expected_shard_dbs[1])
        self.assertFalse(os.path.exists(expected_shard_dbs[2]))

        # third shard range should be unchanged - not yet cleaved
        self.assertEqual(dict(shard_ranges[2]),
                         dict(updated_shard_ranges[2]))

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertFalse(context.cleaving_done)
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
        self.assertEqual(shard_ranges[2].upper_str, context.cursor)
        self.assertEqual(8, context.cleave_to_row)
        self.assertEqual(8, context.max_row)

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
        broker = self._make_broker(account='.shards_a', container='shard_c')
        own_shard_range = ShardRange(
            broker.path, next(self.ts_iter), 'here', 'where',
            state=ShardRange.SHRINKING, epoch=next(self.ts_iter))
        broker.merge_shard_ranges([own_shard_range])
        broker.set_sharding_sysmeta('Root', 'a/c')
        self.assertFalse(broker.is_root_container())  # sanity check

        objects = [
            ('there', self.ts_encoded(), 3, 'text/plain', 'etag_there', 0, 0),
            ('where', self.ts_encoded(), 100, 'text/plain', 'etag_where', 0,
             0),
        ]
        for obj in objects:
            broker.put_object(*obj)
        acceptor_epoch = next(self.ts_iter)
        acceptor = ShardRange('.shards_a/acceptor', Timestamp.now(),
                              'here', 'yonder', '1000', '11111',
                              state=ShardRange.ACTIVE, epoch=acceptor_epoch)
        db_hash = hash_path(acceptor.account, acceptor.container)
        # NB expected cleave db includes acceptor epoch
        expected_shard_db = os.path.join(
            self.tempdir, 'sda', 'containers', '0', db_hash[-3:], db_hash,
            '%s_%s.db' % (db_hash, acceptor_epoch.internal))

        broker.merge_shard_ranges([acceptor])
        broker.set_sharding_state()

        # run cleave
        with self._mock_sharder() as sharder:
            self.assertTrue(sharder._cleave(broker))

        context = CleavingContext.load(broker)
        self.assertTrue(context.misplaced_done)
        self.assertTrue(context.cleaving_done)
        self.assertEqual(acceptor.upper_str, context.cursor)
        self.assertEqual(2, context.cleave_to_row)
        self.assertEqual(2, context.max_row)

        self.assertEqual(SHARDING, broker.get_db_state())
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, expected_shard_db, 0)])
        shard_broker = ContainerBroker(expected_shard_db)
        # NB when cleaving a shard container to a larger acceptor namespace
        # then expect the shard broker's own shard range to reflect that of the
        # acceptor shard range rather than being set to CLEAVED.
        self.assertEqual(
            ShardRange.ACTIVE, shard_broker.get_own_shard_range().state)

        updated_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(1, len(updated_shard_ranges))
        self.assertEqual(dict(acceptor), dict(updated_shard_ranges[0]))

        # shard range should have unmodified acceptor, bytes used and
        # meta_timestamp
        self._check_objects(objects, expected_shard_db)

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
        self.assertTrue(broker.set_sharding_state())
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
        self.assertEqual('', context.cursor)
        self.assertEqual(10, context.cleave_to_row)
        self.assertEqual(12, context.max_row)  # note that max row increased
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
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

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

        # repeat - second shard range cleaves fully because its previously
        # cleaved shard db no longer exists
        unlink_files(expected_shard_dbs)
        merge_items_calls = []
        with mock.patch('swift.container.backend.ContainerBroker.merge_items',
                        mock_merge_items):
            with self._mock_sharder() as sharder:
                sharder._replicate_object = mock.MagicMock(
                    side_effect=[(True, [True, True, True]),  # misplaced obj
                                 (False, [False, True, True])])
                sharder._audit_container = mock.MagicMock()
                sharder.logger = debug_logger()
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
                sharder.logger = debug_logger()
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

    def _check_complete_sharding(self, account, container, shard_bounds):
        broker = self._make_sharding_broker(
            account=account, container=container, shard_bounds=shard_bounds)
        obj = {'name': 'obj', 'created_at': next(self.ts_iter).internal,
               'size': 14, 'content_type': 'text/plain', 'etag': 'an etag',
               'deleted': 0}
        broker.get_brokers()[0].merge_items([obj])
        self.assertEqual(2, len(broker.db_files))  # sanity check

        def check_not_complete():
            with self._mock_sharder() as sharder:
                self.assertFalse(sharder._complete_sharding(broker))
            warning_lines = sharder.logger.get_lines_for_level('warning')
            self.assertIn(
                'Repeat cleaving required for %r' % broker.db_files[0],
                warning_lines[0])
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

        # no cleave context progress
        check_not_complete()

        # cleaving_done is False
        context = CleavingContext.load(broker)
        self.assertEqual(1, context.max_row)
        context.cleave_to_row = 1  # pretend all rows have been cleaved
        context.cleaving_done = False
        context.misplaced_done = True
        context.store(broker)
        check_not_complete()

        # misplaced_done is False
        context.misplaced_done = False
        context.cleaving_done = True
        context.store(broker)
        check_not_complete()

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
        check_not_complete()

        # db id changes
        broker.get_brokers()[0].newid('fake_remote_id')
        context.cleave_to_row = 2  # pretend all rows have been cleaved, again
        context.store(broker)
        check_not_complete()

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

    def test_identify_sharding_candidate(self):
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

        # repeat with handoff node and db_file error
        with self._mock_sharder(conf=conf) as sharder:
            with mock.patch('os.stat', side_effect=OSError('test error')):
                with mock_timestamp_now(now):
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
                          'found': 0, 'placed': 0, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_found'))

        # sharding - no misplaced objects
        self.assertTrue(broker.set_sharding_state())
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_not_called()
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_found'))

        # pretend we cleaved up to end of second shard range
        context = CleavingContext.load(broker)
        context.cursor = 'there'
        context.store(broker)
        with self._mock_sharder() as sharder:
            sharder._move_misplaced_objects(broker)
        sharder._replicate_object.assert_not_called()
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_found'))

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
            sharder.logger.get_increment_counts().get('misplaced_found'))
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
                          'found': 1, 'placed': 2, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
                          'found': 1, 'placed': 4, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        sharder._replicate_object.assert_has_calls(
            [mock.call(0, db, 0) for db in expected_shard_dbs[2:4]],
            any_order=True
        )
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_found'])

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
                          'found': 0, 'placed': 0, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_found'))

        # and then more misplaced updates arrive
        newer_objects = [
            ['a', self.ts_encoded(), 51, 'text/plain', 'etag_a', 0, 0],
            ['z', self.ts_encoded(), 52, 'text/plain', 'etag_z', 0, 0],
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
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 1, 'placed': 2, 'unplaced': 0}
        self._assert_stats(expected_stats, sharder, 'misplaced')
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_found'])

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
        self.assertIn('Refused to remove misplaced objects', lines[0])
        self.assertIn('Refused to remove misplaced objects', lines[1])
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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
        # check misplaced objects were moved to shard dbs
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... but only removed from the source db if sufficiently replicated
        self._check_objects(objects[2:3], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_dbs[0]))
        self.assertFalse(os.path.exists(expected_dbs[4]))

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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
        # check misplaced objects were moved to shard dbs
        self._check_objects(objects[:2], expected_dbs[1])
        self._check_objects(objects[2:3], expected_dbs[2])
        self._check_objects(objects[3:], expected_dbs[3])
        # ... but only removed from the source db if sufficiently replicated
        self._check_objects(objects[3:], broker.db_file)
        # ... and nothing else moved
        self.assertFalse(os.path.exists(expected_dbs[0]))
        self.assertFalse(os.path.exists(expected_dbs[4]))

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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_found'))
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
        self.assertEqual(
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
        self.assertFalse(
            sharder.logger.get_increment_counts().get('misplaced_found'))
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))

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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
            1, sharder.logger.get_increment_counts()['misplaced_found'])
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
            sharder.logger = debug_logger()
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
            1, sharder.logger.get_increment_counts()['misplaced_found'])

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
            1, sharder.logger.get_increment_counts()['misplaced_found'])

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
            1, sharder.logger.get_increment_counts()['misplaced_found'])

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

    def _setup_find_ranges(self, account, cont, lower, upper):
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

    def _check_find_shard_ranges_none_found(self, broker, objects):
        with self._mock_sharder() as sharder:
            num_found = sharder._find_shard_ranges(broker)
        self.assertGreater(sharder.split_size, len(objects))
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
        self.assertEqual(sharder.split_size, len(objects))
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
                    ShardRange.make_path('.int_shards_a', 'c', cont, now, 0),
                    now, lower, objects[98][0], 99),
                ShardRange(
                    ShardRange.make_path('.int_shards_a', 'c', cont, now, 1),
                    now, objects[98][0], upper, 1),
            ]
            self._assert_shard_ranges_equal(expected_ranges,
                                            broker.get_shard_ranges())

        # first invocation finds both ranges
        broker, objects = self._setup_find_ranges(
            account, cont, lower, upper)
        with self._mock_sharder(conf={'shard_container_threshold': 199,
                                      'auto_create_account_prefix': '.int_'}
                                ) as sharder:
            with mock_timestamp_now() as now:
                num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(99, sharder.split_size)
        self.assertEqual(2, num_found)
        check_ranges()
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0,
                          'found': 2, 'min_time': mock.ANY,
                          'max_time': mock.ANY}
        stats = self._assert_stats(expected_stats, sharder, 'scanned')
        self.assertGreaterEqual(stats['max_time'], stats['min_time'])

        # second invocation finds none
        with self._mock_sharder(conf={'shard_container_threshold': 199,
                                      'auto_create_account_prefix': '.int_'}
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
        with self._mock_sharder(
                conf={'shard_container_threshold': 90,
                      'shard_scanner_batch_size': 2}) as sharder:
            with mock_timestamp_now(now):
                num_found = sharder._find_shard_ranges(broker)
        self.assertEqual(45, sharder.split_size)
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
        with self._mock_sharder(conf={'shard_container_threshold': 199,
                                      'shard_scanner_batch_size': 2}
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
                                      'shard_scanner_batch_size': 2}
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

    def test_sharding_enabled(self):
        broker = self._make_broker()
        self.assertFalse(sharding_enabled(broker))
        broker.update_metadata(
            {'X-Container-Sysmeta-Sharding':
             ('yes', Timestamp.now().internal)})
        self.assertTrue(sharding_enabled(broker))
        # deleting broker clears sharding sysmeta
        broker.delete_db(Timestamp.now().internal)
        self.assertFalse(sharding_enabled(broker))
        # but if broker has a shard range then sharding is enabled
        broker.merge_shard_ranges(
            ShardRange('acc/a_shard', Timestamp.now(), 'l', 'u'))
        self.assertTrue(sharding_enabled(broker))

    def test_send_shard_ranges(self):
        shard_ranges = self._make_shard_ranges((('', 'h'), ('h', '')))

        def do_test(replicas, *resp_codes):
            sent_data = defaultdict(bytes)

            def on_send(fake_conn, data):
                sent_data[fake_conn] += data

            with self._mock_sharder(replicas=replicas) as sharder:
                with mocked_http_conn(*resp_codes, give_send=on_send) as conn:
                    with mock_timestamp_now() as now:
                        res = sharder._send_shard_ranges(
                            'a', 'c', shard_ranges)

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
            return res, sharder

        replicas = 3
        res, sharder = do_test(replicas, 202, 202, 202)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, 202, 202, 404)
        self.assertTrue(res)
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, 202, 202, Exception)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder = do_test(replicas, 202, 404, 404)
        self.assertFalse(res)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, 500, 500, 500)
        self.assertFalse(res)
        self.assertEqual([True, True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, Exception, Exception, 202)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder = do_test(replicas, Exception, eventlet.Timeout(), 202)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])

        replicas = 2
        res, sharder = do_test(replicas, 202, 202)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, 202, 404)
        self.assertTrue(res)
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, 202, Exception)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder = do_test(replicas, 404, 404)
        self.assertFalse(res)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, Exception, Exception)
        self.assertFalse(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder = do_test(replicas, eventlet.Timeout(), Exception)
        self.assertFalse(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])

        replicas = 4
        res, sharder = do_test(replicas, 202, 202, 202, 202)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertTrue(res)
        res, sharder = do_test(replicas, 202, 202, 404, 404)
        self.assertTrue(res)
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, 202, 202, Exception, Exception)
        self.assertTrue(res)
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder = do_test(replicas, 202, 404, 404, 404)
        self.assertFalse(res)
        self.assertEqual([True, True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, 500, 500, 500, 202)
        self.assertFalse(res)
        self.assertEqual([True, True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('warning')])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        res, sharder = do_test(replicas, Exception, Exception, 202, 404)
        self.assertFalse(res)
        self.assertEqual([True], [
            all(msg in line for msg in ('Failed to put shard ranges', '404'))
            for line in sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
            sharder.logger.get_lines_for_level('error')])
        res, sharder = do_test(
            replicas, eventlet.Timeout(), eventlet.Timeout(), 202, 404)
        self.assertFalse(res)
        self.assertEqual([True], [
            all(msg in line for msg in ('Failed to put shard ranges', '404'))
            for line in sharder.logger.get_lines_for_level('warning')])
        self.assertEqual([True, True], [
            'Failed to put shard ranges' in line for line in
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
                    with mock_timestamp_now() as now:
                        with mock.patch.object(sharder, '_audit_container'):
                            sharder.logger = debug_logger()
                            sharder._process_broker(broker, node, 99)
                            own_shard_range = broker.get_own_shard_range(
                                no_default=True)
            mock_set_sharding_state.assert_not_called()
            self.assertEqual(dict(own_sr, meta_timestamp=now),
                             dict(own_shard_range))
            self.assertEqual(UNSHARDED, broker.get_db_state())
            self.assertFalse(broker.logger.get_lines_for_level('warning'))
            self.assertFalse(broker.logger.get_lines_for_level('error'))
            broker.logger.clear()

    def _check_process_broker_sharding_no_others(self, state):
        # verify that when existing own_shard_range has given state and there
        # are other shard ranges then the sharding process will begin
        broker = self._make_broker(hash_='hash%s' % state)
        node = {'ip': '1.2.3.4', 'port': 6040, 'device': 'sda5', 'id': '2',
                'index': 0}
        own_sr = broker.get_own_shard_range()
        self.assertTrue(own_sr.update_state(state))
        epoch = Timestamp.now()
        own_sr.epoch = epoch
        shard_ranges = self._make_shard_ranges((('', 'm'), ('m', '')))
        broker.merge_shard_ranges([own_sr] + shard_ranges)

        with self._mock_sharder() as sharder:
            with mock.patch.object(
                    sharder, '_create_shard_containers', return_value=0):
                with mock_timestamp_now() as now:
                    sharder._audit_container = mock.MagicMock()
                    sharder._process_broker(broker, node, 99)
                    final_own_sr = broker.get_own_shard_range(no_default=True)

        self.assertEqual(dict(own_sr, meta_timestamp=now),
                         dict(final_own_sr))
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(epoch.normal, parse_db_filename(broker.db_file)[1])
        self.assertFalse(broker.logger.get_lines_for_level('warning'))
        self.assertFalse(broker.logger.get_lines_for_level('error'))

    def test_process_broker_sharding_with_own_shard_range_no_others(self):
        self._check_process_broker_sharding_no_others(ShardRange.SHARDING)
        self._check_process_broker_sharding_no_others(ShardRange.SHRINKING)

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
                         ShardRange.SHARDED):
                epoch = None
            else:
                epoch = Timestamp.now()

            own_sr = broker.get_own_shard_range()  # returns the default
            own_sr.update_state(state)
            own_sr.epoch = epoch
            broker.merge_shard_ranges([own_sr])
            with self._mock_sharder() as sharder:
                with mock_timestamp_now() as now:
                    sharder._process_broker(broker, node, 99)
                    own_shard_range = broker.get_own_shard_range(
                        no_default=True)
            self.assertEqual(dict(own_sr, meta_timestamp=now),
                             dict(own_shard_range))
            self.assertEqual(UNSHARDED, broker.get_db_state())
            if epoch:
                self.assertFalse(broker.logger.get_lines_for_level('warning'))
            else:
                self.assertIn('missing epoch',
                              broker.logger.get_lines_for_level('warning')[0])
            self.assertFalse(broker.logger.get_lines_for_level('error'))
            broker.logger.clear()

    def _check_process_broker_sharding_others(self, state):
        # verify states in which own_shard_range will cause sharding
        # process to start when other shard ranges are in the db
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

            sharder.logger = debug_logger()
            with mock_timestamp_now() as now:
                # we're not testing rest of the process here so prevent any
                # attempt to progress shard range states
                sharder._create_shard_containers = lambda *args: 0
                sharder._process_broker(broker, node, 99)
                own_shard_range = broker.get_own_shard_range(no_default=True)

        self.assertEqual(dict(own_sr, meta_timestamp=now),
                         dict(own_shard_range))
        self.assertEqual(SHARDING, broker.get_db_state())
        self.assertEqual(epoch.normal, parse_db_filename(broker.db_file)[1])
        self.assertFalse(broker.logger.get_lines_for_level('warning'))
        self.assertFalse(broker.logger.get_lines_for_level('error'))

    def test_process_broker_sharding_with_own_shard_range_and_others(self):
        self._check_process_broker_sharding_others(ShardRange.SHARDING)
        self._check_process_broker_sharding_others(ShardRange.SHRINKING)
        self._check_process_broker_sharding_others(ShardRange.SHARDED)

    def check_shard_ranges_sent(self, broker, expected_sent):
        bodies = []

        def capture_send(conn, data):
            bodies.append(data)

        with self._mock_sharder() as sharder:
            with mocked_http_conn(204, 204, 204,
                                  give_send=capture_send) as mock_conn:
                sharder._update_root_container(broker)

        for req in mock_conn.requests:
            self.assertEqual('PUT', req['method'])
        self.assertEqual([expected_sent] * 3,
                         [json.loads(b) for b in bodies])

    def test_update_root_container_own_range(self):
        broker = self._make_broker()

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
            broker.put_object(str(own_shard_range.object_count + 1),
                              next(self.ts_iter).internal, 1, '', '')
            with mock_timestamp_now() as now:
                # force own shard range meta updates to be at fixed timestamp
                expected_sent = [
                    dict(own_shard_range,
                         meta_timestamp=now.internal,
                         object_count=own_shard_range.object_count + 1,
                         bytes_used=own_shard_range.bytes_used + 1)]
                self.check_shard_ranges_sent(broker, expected_sent)

        for state in ShardRange.STATES:
            with annotate_failure(state):
                check_only_own_shard_range_sent(state)

    def test_update_root_container_all_ranges(self):
        broker = self._make_broker()
        other_shard_ranges = self._make_shard_ranges((('', 'h'), ('h', '')))
        self.assertTrue(other_shard_ranges[0].set_deleted())
        broker.merge_shard_ranges(other_shard_ranges)

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
            broker.put_object(str(own_shard_range.object_count + 1),
                              next(self.ts_iter).internal, 1, '', '')
            with mock_timestamp_now() as now:
                shard_ranges = broker.get_shard_ranges(include_deleted=True)
                expected_sent = sorted([
                    own_shard_range.copy(
                        meta_timestamp=now.internal,
                        object_count=own_shard_range.object_count + 1,
                        bytes_used=own_shard_range.bytes_used + 1)] +
                    shard_ranges,
                    key=lambda sr: (sr.upper, sr.state, sr.lower))
                self.check_shard_ranges_sent(
                    broker, [dict(sr) for sr in expected_sent])

        for state in ShardRange.STATES.keys():
            with annotate_failure(state):
                check_all_shard_ranges_sent(state)

    def test_audit_root_container(self):
        broker = self._make_broker()

        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        with self._mock_sharder() as sharder:
            with mock.patch.object(
                    sharder, '_audit_shard_container') as mocked:
                sharder._audit_container(broker)
        self._assert_stats(expected_stats, sharder, 'audit_root')
        self.assertFalse(sharder.logger.get_lines_for_level('warning'))
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        mocked.assert_not_called()

        def assert_overlap_warning(line, state_text):
            self.assertIn(
                'Audit failed for root %s' % broker.db_file, line)
            self.assertIn(
                'overlapping ranges in state %s: k-t s-z' % state_text,
                line)

        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1}
        shard_bounds = (('a', 'j'), ('k', 't'), ('s', 'z'))
        for state, state_text in ShardRange.STATES.items():
            shard_ranges = self._make_shard_ranges(shard_bounds, state)
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

        def assert_missing_warning(line):
            self.assertIn(
                'Audit failed for root %s' % broker.db_file, line)
            self.assertIn('missing range(s): -a j-k z-', line)

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
            assert_overlap_warning(lines[0], state_text)
            self.assertFalse(lines[1:])
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            self._assert_stats(expected_stats, sharder, 'audit_root')
            mocked.assert_not_called()

    def test_audit_shard_container(self):
        broker = self._make_broker(account='.shards_a', container='shard_c')
        broker.set_sharding_sysmeta('Root', 'a/c')
        # include overlaps to verify correct match for updating own shard range
        shard_bounds = (
            ('a', 'j'), ('k', 't'), ('k', 's'), ('l', 's'), ('s', 'z'))
        shard_ranges = self._make_shard_ranges(shard_bounds, ShardRange.ACTIVE)
        shard_ranges[1].name = broker.path
        expected_stats = {'attempted': 1, 'success': 0, 'failure': 1}

        def call_audit_container(exc=None):
            with self._mock_sharder() as sharder:
                sharder.logger = debug_logger()
                with mock.patch.object(sharder, '_audit_root_container') \
                        as mocked, mock.patch.object(
                            sharder, 'int_client') as mock_swift:
                    mock_response = mock.MagicMock()
                    mock_response.headers = {'x-backend-record-type':
                                             'shard'}
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

        # bad account name
        broker.account = 'bad_account'
        sharder, mock_swift = call_audit_container()
        lines = sharder.logger.get_lines_for_level('warning')
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertIn('Audit warnings for shard %s' % broker.db_file, lines[0])
        self.assertIn('account not in shards namespace', lines[0])
        self.assertNotIn('root has no matching shard range', lines[0])
        self.assertNotIn('unable to get shard ranges from root', lines[0])
        self.assertIn('Audit failed for shard %s' % broker.db_file, lines[1])
        self.assertIn('missing own shard range', lines[1])
        self.assertFalse(lines[2:])
        self.assertFalse(broker.is_deleted())

        # missing own shard range
        broker.get_info()
        sharder, mock_swift = call_audit_container()
        lines = sharder.logger.get_lines_for_level('warning')
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertIn('Audit failed for shard %s' % broker.db_file, lines[0])
        self.assertIn('missing own shard range', lines[0])
        self.assertNotIn('unable to get shard ranges from root', lines[0])
        self.assertFalse(lines[1:])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertFalse(broker.is_deleted())

        # create own shard range, no match in root
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        own_shard_range = broker.get_own_shard_range()  # get the default
        own_shard_range.lower = 'j'
        own_shard_range.upper = 'k'
        broker.merge_shard_ranges([own_shard_range])
        sharder, mock_swift = call_audit_container()
        lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn('Audit warnings for shard %s' % broker.db_file, lines[0])
        self.assertNotIn('account not in shards namespace', lines[0])
        self.assertNotIn('missing own shard range', lines[0])
        self.assertIn('root has no matching shard range', lines[0])
        self.assertNotIn('unable to get shard ranges from root', lines[0])
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertFalse(lines[1:])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertFalse(broker.is_deleted())
        expected_headers = {'X-Backend-Record-Type': 'shard',
                            'X-Newest': 'true',
                            'X-Backend-Include-Deleted': 'True',
                            'X-Backend-Override-Deleted': 'true'}
        params = {'format': 'json', 'marker': 'j', 'end_marker': 'k'}
        mock_swift.make_request.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)

        # create own shard range, failed response from root
        expected_stats = {'attempted': 1, 'success': 1, 'failure': 0}
        own_shard_range = broker.get_own_shard_range()  # get the default
        own_shard_range.lower = 'j'
        own_shard_range.upper = 'k'
        broker.merge_shard_ranges([own_shard_range])
        sharder, mock_swift = call_audit_container(
            exc=internal_client.UnexpectedResponse('bad', 'resp'))
        lines = sharder.logger.get_lines_for_level('warning')
        self.assertIn('Failed to get shard ranges', lines[0])
        self.assertIn('Audit warnings for shard %s' % broker.db_file, lines[1])
        self.assertNotIn('account not in shards namespace', lines[1])
        self.assertNotIn('missing own shard range', lines[1])
        self.assertNotIn('root has no matching shard range', lines[1])
        self.assertIn('unable to get shard ranges from root', lines[1])
        self._assert_stats(expected_stats, sharder, 'audit_shard')
        self.assertFalse(lines[2:])
        self.assertFalse(sharder.logger.get_lines_for_level('error'))
        self.assertFalse(broker.is_deleted())
        mock_swift.make_request.assert_called_once_with(
            'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
            params=params)

        def assert_ok():
            sharder, mock_swift = call_audit_container()
            self.assertFalse(sharder.logger.get_lines_for_level('warning'))
            self.assertFalse(sharder.logger.get_lines_for_level('error'))
            self._assert_stats(expected_stats, sharder, 'audit_shard')
            params = {'format': 'json', 'marker': 'k', 'end_marker': 't'}
            mock_swift.make_request.assert_called_once_with(
                'GET', '/v1/a/c', expected_headers, acceptable_statuses=(2,),
                params=params)

        # make own shard range match one in root, but different state
        shard_ranges[1].timestamp = Timestamp.now()
        broker.merge_shard_ranges([shard_ranges[1]])
        now = Timestamp.now()
        shard_ranges[1].update_state(ShardRange.SHARDING, state_timestamp=now)
        assert_ok()
        self.assertFalse(broker.is_deleted())
        # own shard range state is updated from root version
        own_shard_range = broker.get_own_shard_range()
        self.assertEqual(ShardRange.SHARDING, own_shard_range.state)
        self.assertEqual(now, own_shard_range.state_timestamp)

        own_shard_range.update_state(ShardRange.SHARDED,
                                     state_timestamp=Timestamp.now())
        broker.merge_shard_ranges([own_shard_range])
        assert_ok()

        own_shard_range.deleted = 1
        own_shard_range.timestamp = Timestamp.now()
        broker.merge_shard_ranges([own_shard_range])
        assert_ok()
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
                conf={'shard_container_threshold': 1}) as sharder:
            sharder._find_and_enable_sharding_candidates(broker)
        self.assertEqual(ShardRange.ACTIVE, broker.get_own_shard_range().state)
        broker.put_object('obj', next(self.ts_iter).internal, 1, '', '')
        self.assertEqual(1, broker.get_info()['object_count'])
        with self._mock_sharder(
                conf={'shard_container_threshold': 1}) as sharder:
            with mock_timestamp_now() as now:
                sharder._find_and_enable_sharding_candidates(
                    broker, [broker.get_own_shard_range()])
        own_sr = broker.get_own_shard_range()
        self.assertEqual(ShardRange.SHARDING, own_sr.state)
        self.assertEqual(now, own_sr.state_timestamp)
        self.assertEqual(now, own_sr.epoch)

        # check idempotency
        with self._mock_sharder(
                conf={'shard_container_threshold': 1}) as sharder:
            with mock_timestamp_now():
                sharder._find_and_enable_sharding_candidates(
                    broker, [broker.get_own_shard_range()])
        own_sr = broker.get_own_shard_range()
        self.assertEqual(ShardRange.SHARDING, own_sr.state)
        self.assertEqual(now, own_sr.state_timestamp)
        self.assertEqual(now, own_sr.epoch)

    def test_find_and_enable_shrinking_candidates(self):
        broker = self._make_broker()
        broker.enable_sharding(next(self.ts_iter))
        shard_bounds = (('', 'here'), ('here', 'there'), ('there', ''))
        size = (DEFAULT_SHARD_SHRINK_POINT *
                DEFAULT_SHARD_CONTAINER_THRESHOLD / 100)
        shard_ranges = self._make_shard_ranges(
            shard_bounds, state=ShardRange.ACTIVE, object_count=size)
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())
        with self._mock_sharder() as sharder:
            sharder._find_and_enable_shrinking_candidates(broker)
        self._assert_shard_ranges_equal(shard_ranges,
                                        broker.get_shard_ranges())

        # one range just below threshold
        shard_ranges[0].update_meta(size - 1, 0)
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
            [mock.call(acceptor.account, acceptor.container, [acceptor]),
             mock.call(donor.account, donor.container, [donor, acceptor])]
        )

        # check idempotency
        with self._mock_sharder() as sharder:
            with mock_timestamp_now() as now:
                sharder._send_shard_ranges = mock.MagicMock()
                sharder._find_and_enable_shrinking_candidates(broker)
        self._assert_shard_ranges_equal([donor, acceptor, shard_ranges[2]],
                                        broker.get_shard_ranges())
        sharder._send_shard_ranges.assert_has_calls(
            [mock.call(acceptor.account, acceptor.container, [acceptor]),
             mock.call(donor.account, donor.container, [donor, acceptor])]
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
            [mock.call(acceptor.account, acceptor.container, [acceptor]),
             mock.call(donor.account, donor.container, [donor, acceptor])]
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
            [mock.call(new_acceptor.account, new_acceptor.container,
                       [new_acceptor]),
             mock.call(new_donor.account, new_donor.container,
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
            [mock.call(final_donor.account, final_donor.container,
                       [final_donor, broker.get_own_shard_range()])]
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
        self.assertEqual(dev_ids, set(sharder._local_device_ids))
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
        self.assertEqual(dev_ids, set(sharder._local_device_ids))
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
        self.assertEqual(dev_ids, set(sharder._local_device_ids))
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
        self.assertEqual(dev_ids, set(sharder._local_device_ids))
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
        self.assertEqual(dev_ids, set(sharder._local_device_ids))
        self.assertEqual(set(container_data[1:]),
                         set((call[0][0].path, call[0][1]['id'], call[0][2])
                             for call in mock_process_broker.call_args_list))


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
                expected = curs.encode('utf-8') if six.PY2 else curs
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

    def test_store(self):
        broker = self._make_sharding_broker()
        old_db_id = broker.get_brokers()[0].get_info()['id']
        ctx = CleavingContext(old_db_id, 'curs', 12, 11, 2, True, True, 2, 4)
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
        # check idempotency
        ctx.reset()

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
        # check idempotency
        ctx.start()
