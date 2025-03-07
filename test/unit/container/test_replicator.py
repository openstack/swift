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

import os
import time
import shutil
import itertools
import unittest
from unittest import mock
import random
import sqlite3

from eventlet import sleep

from swift.common import db_replicator
from swift.common.swob import HTTPServerError
from swift.container import replicator, backend, server, sync_store
from swift.container.reconciler import (
    MISPLACED_OBJECTS_ACCOUNT, get_reconciler_container_name)
from swift.common.utils import Timestamp, encode_timestamps, ShardRange, \
    get_db_files, make_db_file_path, MD5_OF_EMPTY_STRING
from swift.common.storage_policy import POLICIES
from test import annotate_failure

from test.debug_logger import debug_logger
from test.unit.common import test_db_replicator
from test.unit import patch_policies, make_timestamp_iter, mock_check_drive, \
    attach_fake_replication_rpc, FakeHTTPResponse
from contextlib import contextmanager


@patch_policies
class TestReplicatorSync(test_db_replicator.TestReplicatorSync):

    backend = backend.ContainerBroker
    datadir = server.DATADIR
    replicator_daemon = replicator.ContainerReplicator
    replicator_rpc = replicator.ContainerReplicatorRpc

    def assertShardRangesEqual(self, x, y):
        # ShardRange.__eq__ only compares lower and upper; here we generate
        # dict representations to compare all attributes
        self.assertEqual([dict(sr) for sr in x], [dict(sr) for sr in y])

    def assertShardRangesNotEqual(self, x, y):
        # ShardRange.__eq__ only compares lower and upper; here we generate
        # dict representations to compare all attributes
        self.assertNotEqual([dict(sr) for sr in x], [dict(sr) for sr in y])

    def test_report_up_to_date(self):
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(Timestamp(1).internal, int(POLICIES.default))
        info = broker.get_info()
        broker.reported(info['put_timestamp'],
                        info['delete_timestamp'],
                        info['object_count'],
                        info['bytes_used'])
        full_info = broker.get_replication_info()
        expected_info = {'put_timestamp': Timestamp(1).internal,
                         'delete_timestamp': '0',
                         'count': 0,
                         'bytes_used': 0,
                         'reported_put_timestamp': Timestamp(1).internal,
                         'reported_delete_timestamp': '0',
                         'reported_object_count': 0,
                         'reported_bytes_used': 0}
        for key, value in expected_info.items():
            msg = 'expected value for %r, %r != %r' % (
                key, full_info[key], value)
            self.assertEqual(full_info[key], value, msg)
        repl = replicator.ContainerReplicator({})
        self.assertTrue(repl.report_up_to_date(full_info))
        full_info['delete_timestamp'] = Timestamp(2).internal
        self.assertFalse(repl.report_up_to_date(full_info))
        full_info['reported_delete_timestamp'] = Timestamp(2).internal
        self.assertTrue(repl.report_up_to_date(full_info))
        full_info['count'] = 1
        self.assertFalse(repl.report_up_to_date(full_info))
        full_info['reported_object_count'] = 1
        self.assertTrue(repl.report_up_to_date(full_info))
        full_info['bytes_used'] = 1
        self.assertFalse(repl.report_up_to_date(full_info))
        full_info['reported_bytes_used'] = 1
        self.assertTrue(repl.report_up_to_date(full_info))
        full_info['put_timestamp'] = Timestamp(3).internal
        self.assertFalse(repl.report_up_to_date(full_info))
        full_info['reported_put_timestamp'] = Timestamp(3).internal
        self.assertTrue(repl.report_up_to_date(full_info))

    def test_sync_remote_in_sync(self):
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # "replicate" to same database
        node = {'device': 'sdb', 'replication_ip': '127.0.0.1'}
        daemon = replicator.ContainerReplicator({})
        # replicate
        part, node = self._get_broker_part_node(broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])

    def test_sync_remote_with_timings(self):
        ts_iter = make_timestamp_iter()
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = next(ts_iter)
        broker.initialize(put_timestamp.internal, POLICIES.default.idx)
        broker.update_metadata(
            {'x-container-meta-test': ('foo', put_timestamp.internal)})
        # setup remote container
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(next(ts_iter).internal, POLICIES.default.idx)
        timestamp = next(ts_iter)
        for db in (broker, remote_broker):
            db.put_object(
                '/a/c/o', timestamp.internal, 0, 'content-type', 'etag',
                storage_policy_index=db.storage_policy_index)
        # replicate
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        with mock.patch.object(db_replicator, 'DEBUG_TIMINGS_THRESHOLD', -1):
            success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])
        expected_timings = ('info', 'update_metadata', 'merge_timestamps',
                            'get_sync', 'merge_syncs')
        debug_lines = self.rpc.logger.logger.get_lines_for_level('debug')
        self.assertEqual(len(expected_timings), len(debug_lines),
                         'Expected %s debug lines but only got %s: %s' %
                         (len(expected_timings), len(debug_lines),
                          debug_lines))
        for metric in expected_timings:
            expected = 'replicator-rpc-sync time for %s:' % metric
            self.assertTrue(any(expected in line for line in debug_lines),
                            'debug timing %r was not in %r' % (
                                expected, debug_lines))

    def test_sync_remote_missing(self):
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)

        # "replicate"
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)

        # complete rsync to all other nodes
        self.assertEqual(2, daemon.stats['rsync'])
        for i in range(1, 3):
            remote_broker = self._get_broker('a', 'c', node_index=i)
            self.assertTrue(os.path.exists(remote_broker.db_file))
            remote_info = remote_broker.get_info()
            local_info = self._get_broker(
                'a', 'c', node_index=0).get_info()
            for k, v in local_info.items():
                if k == 'id':
                    continue
                self.assertEqual(remote_info[k], v,
                                 "mismatch remote %s %r != %r" % (
                                     k, remote_info[k], v))

    def test_rsync_failure(self):
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # "replicate" to different device
        daemon = replicator.ContainerReplicator({})

        def _rsync_file(*args, **kwargs):
            return False
        daemon._rsync_file = _rsync_file

        # replicate
        part, local_node = self._get_broker_part_node(broker)
        node = random.choice([n for n in self._ring.devs
                              if n['id'] != local_node['id']])
        info = broker.get_replication_info()
        with mock_check_drive(ismount=True):
            success = daemon._repl_to_node(node, broker, part, info)
        self.assertFalse(success)

    def test_sync_remote_missing_most_rows(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # add a row to "local" db
        broker.put_object('/a/c/o', time.time(), 0, 'content-type', 'etag',
                          storage_policy_index=broker.storage_policy_index)
        # replicate
        node = {'device': 'sdc', 'replication_ip': '127.0.0.1'}
        daemon = replicator.ContainerReplicator({'per_diff': 1})

        def _rsync_file(db_file, remote_file, **kwargs):
            remote_server, remote_path = remote_file.split('/', 1)
            dest_path = os.path.join(self.root, remote_path)
            shutil.copy(db_file, dest_path)
            return True
        daemon._rsync_file = _rsync_file
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # row merge
        self.assertEqual(1, daemon.stats['remote_merge'])
        local_info = self._get_broker(
            'a', 'c', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', 'c', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))

    def test_sync_remote_missing_one_rows(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # add some rows to both db
        for i in range(10):
            put_timestamp = time.time()
            for db in (broker, remote_broker):
                path = '/a/c/o_%s' % i
                db.put_object(path, put_timestamp, 0, 'content-type', 'etag',
                              storage_policy_index=db.storage_policy_index)
        # now a row to the "local" broker only
        broker.put_object('/a/c/o_missing', time.time(), 0,
                          'content-type', 'etag',
                          storage_policy_index=broker.storage_policy_index)
        # replicate
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # row merge
        self.assertEqual(1, daemon.stats['diff'])
        local_info = self._get_broker(
            'a', 'c', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', 'c', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))

    def test_sync_remote_can_not_keep_up(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # add some rows to both db's
        for i in range(10):
            put_timestamp = time.time()
            for db in (broker, remote_broker):
                obj_name = 'o_%s' % i
                db.put_object(obj_name, put_timestamp, 0,
                              'content-type', 'etag',
                              storage_policy_index=db.storage_policy_index)
        # setup REPLICATE callback to simulate adding rows during merge_items
        missing_counter = itertools.count()

        def put_more_objects(op, *args):
            if op != 'merge_items':
                return
            path = '/a/c/o_missing_%s' % next(missing_counter)
            broker.put_object(path, time.time(), 0, 'content-type', 'etag',
                              storage_policy_index=db.storage_policy_index)
        test_db_replicator.FakeReplConnection = \
            test_db_replicator.attach_fake_replication_rpc(
                self.rpc, replicate_hook=put_more_objects)
        db_replicator.ReplConnection = test_db_replicator.FakeReplConnection
        # and add one extra to local db to trigger merge_items
        put_more_objects('merge_items')
        # limit number of times we'll call merge_items
        daemon = replicator.ContainerReplicator({'max_diffs': 10})
        # replicate
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertFalse(success)
        # back off on the PUTs during replication...
        FakeReplConnection = test_db_replicator.attach_fake_replication_rpc(
            self.rpc, replicate_hook=None)
        db_replicator.ReplConnection = FakeReplConnection
        # retry replication
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # row merge
        self.assertEqual(2, daemon.stats['diff'])
        self.assertEqual(1, daemon.stats['diff_capped'])
        local_info = self._get_broker(
            'a', 'c', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', 'c', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))

    def test_diff_capped_sync(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        put_timestamp = next(ts)
        # start off with with a local db that is way behind
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        for i in range(50):
            broker.put_object(
                'o%s' % i, next(ts), 0, 'content-type-old', 'etag',
                storage_policy_index=broker.storage_policy_index)
        # remote primary db has all the new bits...
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        for i in range(100):
            remote_broker.put_object(
                'o%s' % i, next(ts), 0, 'content-type-new', 'etag',
                storage_policy_index=remote_broker.storage_policy_index)
        # except there's *one* tiny thing in our local broker that's newer
        broker.put_object(
            'o101', next(ts), 0, 'content-type-new', 'etag',
            storage_policy_index=broker.storage_policy_index)

        # setup daemon with smaller per_diff and max_diffs
        part, node = self._get_broker_part_node(broker)
        daemon = self._get_daemon(node, conf_updates={'per_diff': 10,
                                                      'max_diffs': 3})
        self.assertEqual(daemon.per_diff, 10)
        self.assertEqual(daemon.max_diffs, 3)
        # run once and verify diff capped
        self._run_once(node, daemon=daemon)
        self.assertEqual(1, daemon.stats['diff'])
        self.assertEqual(1, daemon.stats['diff_capped'])
        # run again and verify fully synced
        self._run_once(node, daemon=daemon)
        self.assertEqual(1, daemon.stats['diff'])
        self.assertEqual(0, daemon.stats['diff_capped'])
        # now that we're synced the new item should be in remote db
        remote_names = set()
        for item in remote_broker.list_objects_iter(500, '', '', '', ''):
            name, ts, size, content_type, etag = item
            remote_names.add(name)
            self.assertEqual(content_type, 'content-type-new')
        self.assertTrue('o101' in remote_names)
        self.assertEqual(len(remote_names), 101)
        self.assertEqual(remote_broker.get_info()['object_count'], 101)

    def test_sync_status_change(self):
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # setup remote container
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)
        # delete local container
        broker.delete_db(time.time())
        # replicate
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])
        # status in sync
        self.assertTrue(remote_broker.is_deleted())
        info = broker.get_info()
        remote_info = remote_broker.get_info()
        self.assertTrue(Timestamp(remote_info['status_changed_at']) >
                        Timestamp(remote_info['put_timestamp']),
                        'remote status_changed_at (%s) is not '
                        'greater than put_timestamp (%s)' % (
                            remote_info['status_changed_at'],
                            remote_info['put_timestamp']))
        self.assertTrue(Timestamp(remote_info['status_changed_at']) >
                        Timestamp(info['status_changed_at']),
                        'remote status_changed_at (%s) is not '
                        'greater than local status_changed_at (%s)' % (
                            remote_info['status_changed_at'],
                            info['status_changed_at']))

    @contextmanager
    def _wrap_merge_timestamps(self, broker, calls):
        def fake_merge_timestamps(*args, **kwargs):
            calls.append(args[0])
            orig_merge_timestamps(*args, **kwargs)

        orig_merge_timestamps = broker.merge_timestamps
        broker.merge_timestamps = fake_merge_timestamps
        try:
            yield True
        finally:
            broker.merge_timestamps = orig_merge_timestamps

    def test_sync_merge_timestamps(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = next(ts)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # setup remote container
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_put_timestamp = next(ts)
        remote_broker.initialize(remote_put_timestamp, POLICIES.default.idx)
        # replicate, expect call to merge_timestamps on remote and local
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        local_calls = []
        remote_calls = []
        with self._wrap_merge_timestamps(broker, local_calls):
            with self._wrap_merge_timestamps(broker, remote_calls):
                success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        self.assertEqual(1, len(remote_calls))
        self.assertEqual(1, len(local_calls))
        self.assertEqual(remote_put_timestamp,
                         broker.get_info()['put_timestamp'])
        self.assertEqual(remote_put_timestamp,
                         remote_broker.get_info()['put_timestamp'])

        # replicate again, no changes so expect no calls to merge_timestamps
        info = broker.get_replication_info()
        local_calls = []
        remote_calls = []
        with self._wrap_merge_timestamps(broker, local_calls):
            with self._wrap_merge_timestamps(broker, remote_calls):
                success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        self.assertEqual(0, len(remote_calls))
        self.assertEqual(0, len(local_calls))
        self.assertEqual(remote_put_timestamp,
                         broker.get_info()['put_timestamp'])
        self.assertEqual(remote_put_timestamp,
                         remote_broker.get_info()['put_timestamp'])

    def test_sync_bogus_db_quarantines(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        policy = random.choice(list(POLICIES))

        # create "local" broker
        local_broker = self._get_broker('a', 'c', node_index=0)
        local_broker.initialize(next(ts), policy.idx)

        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(next(ts), policy.idx)

        db_path = local_broker.db_file
        self.assertTrue(os.path.exists(db_path))  # sanity check
        old_inode = os.stat(db_path).st_ino

        _orig_get_info = backend.ContainerBroker.get_info

        def fail_like_bad_db(broker):
            if broker.db_file == local_broker.db_file:
                raise sqlite3.OperationalError("no such table: container_info")
            else:
                return _orig_get_info(broker)

        part, node = self._get_broker_part_node(remote_broker)
        with mock.patch('swift.container.backend.ContainerBroker.get_info',
                        fail_like_bad_db):
            # Have the remote node replicate to local; local should see its
            # corrupt DB, quarantine it, and act like the DB wasn't ever there
            # in the first place.
            daemon = self._run_once(node)

        self.assertTrue(os.path.exists(db_path))
        # Make sure we didn't just keep the old DB, but quarantined it and
        # made a fresh copy.
        new_inode = os.stat(db_path).st_ino
        self.assertNotEqual(old_inode, new_inode)
        self.assertEqual(daemon.stats['failure'], 0)

    def _replication_scenarios(self, *scenarios, **kwargs):
        remote_wins = kwargs.get('remote_wins', False)
        # these tests are duplicated because of the differences in replication
        # when row counts cause full rsync vs. merge
        scenarios = scenarios or (
            'no_row', 'local_row', 'remote_row', 'both_rows')
        for scenario_name in scenarios:
            ts = itertools.count(int(time.time()))
            policy = random.choice(list(POLICIES))
            remote_policy = random.choice(
                [p for p in POLICIES if p is not policy])
            broker = self._get_broker('a', 'c', node_index=0)
            remote_broker = self._get_broker('a', 'c', node_index=1)
            yield ts, policy, remote_policy, broker, remote_broker
            # variations on different replication scenarios
            variations = {
                'no_row': (),
                'local_row': (broker,),
                'remote_row': (remote_broker,),
                'both_rows': (broker, remote_broker),
            }
            dbs = variations[scenario_name]
            obj_ts = next(ts)
            for db in dbs:
                db.put_object('/a/c/o', obj_ts, 0, 'content-type', 'etag',
                              storage_policy_index=db.storage_policy_index)
            # replicate
            part, node = self._get_broker_part_node(broker)
            daemon = self._run_once(node)
            self.assertEqual(0, daemon.stats['failure'])

            # in sync
            local_info = self._get_broker(
                'a', 'c', node_index=0).get_info()
            remote_info = self._get_broker(
                'a', 'c', node_index=1).get_info()
            if remote_wins:
                expected = remote_policy.idx
                err = 'local policy did not change to match remote ' \
                    'for replication row scenario %s' % scenario_name
            else:
                expected = policy.idx
                err = 'local policy changed to match remote ' \
                    'for replication row scenario %s' % scenario_name
            self.assertEqual(local_info['storage_policy_index'], expected, err)
            self.assertEqual(remote_info['storage_policy_index'],
                             local_info['storage_policy_index'])
            test_db_replicator.TestReplicatorSync.tearDown(self)
            test_db_replicator.TestReplicatorSync.setUp(self)

    def test_sync_local_create_policy_over_newer_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)

    def test_sync_local_create_policy_over_newer_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # delete "remote" broker
            remote_broker.delete_db(next(ts))

    def test_sync_local_create_policy_over_older_remote_delete(self):
        # remote_row & both_rows cases are covered by
        # "test_sync_remote_half_delete_policy_over_newer_local_create"
        for setup in self._replication_scenarios(
                'no_row', 'local_row'):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # delete older "remote" broker
            remote_broker.delete_db(next(ts))
            # create "local" broker
            broker.initialize(next(ts), policy.idx)

    def test_sync_local_half_delete_policy_over_newer_remote_create(self):
        # no_row & remote_row cases are covered by
        # "test_sync_remote_create_policy_over_older_local_delete"
        for setup in self._replication_scenarios('local_row', 'both_rows'):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(next(ts), policy.idx)
            # half delete older "local" broker
            broker.delete_db(next(ts))
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)

    def test_sync_local_recreate_policy_over_newer_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # older recreate "local" broker
            broker.delete_db(next(ts))
            recreate_timestamp = next(ts)
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)

    def test_sync_local_recreate_policy_over_older_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # recreate "local" broker
            broker.delete_db(next(ts))
            recreate_timestamp = next(ts)
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)

    def test_sync_local_recreate_policy_over_newer_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # recreate "local" broker
            broker.delete_db(next(ts))
            recreate_timestamp = next(ts)
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)
            # older delete "remote" broker
            remote_broker.delete_db(next(ts))

    def test_sync_local_recreate_policy_over_older_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # older delete "remote" broker
            remote_broker.delete_db(next(ts))
            # recreate "local" broker
            broker.delete_db(next(ts))
            recreate_timestamp = next(ts)
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)

    def test_sync_local_recreate_policy_over_older_remote_recreate(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # older recreate "remote" broker
            remote_broker.delete_db(next(ts))
            remote_recreate_timestamp = next(ts)
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)
            # recreate "local" broker
            broker.delete_db(next(ts))
            local_recreate_timestamp = next(ts)
            broker.update_put_timestamp(local_recreate_timestamp)
            broker.update_status_changed_at(local_recreate_timestamp)

    def test_sync_remote_create_policy_over_newer_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # create "local" broker
            broker.initialize(next(ts), policy.idx)

    def test_sync_remote_create_policy_over_newer_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # delete "local" broker
            broker.delete_db(next(ts))

    def test_sync_remote_create_policy_over_older_local_delete(self):
        # local_row & both_rows cases are covered by
        # "test_sync_local_half_delete_policy_over_newer_remote_create"
        for setup in self._replication_scenarios(
                'no_row', 'remote_row', remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(next(ts), policy.idx)
            # delete older "local" broker
            broker.delete_db(next(ts))
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)

    def test_sync_remote_half_delete_policy_over_newer_local_create(self):
        # no_row & both_rows cases are covered by
        # "test_sync_local_create_policy_over_older_remote_delete"
        for setup in self._replication_scenarios('remote_row', 'both_rows',
                                                 remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # half delete older "remote" broker
            remote_broker.delete_db(next(ts))
            # create "local" broker
            broker.initialize(next(ts), policy.idx)

    def test_sync_remote_recreate_policy_over_newer_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # older recreate "remote" broker
            remote_broker.delete_db(next(ts))
            recreate_timestamp = next(ts)
            remote_broker.update_put_timestamp(recreate_timestamp)
            remote_broker.update_status_changed_at(recreate_timestamp)
            # create "local" broker
            broker.initialize(next(ts), policy.idx)

    def test_sync_remote_recreate_policy_over_older_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # recreate "remote" broker
            remote_broker.delete_db(next(ts))
            recreate_timestamp = next(ts)
            remote_broker.update_put_timestamp(recreate_timestamp)
            remote_broker.update_status_changed_at(recreate_timestamp)

    def test_sync_remote_recreate_policy_over_newer_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # recreate "remote" broker
            remote_broker.delete_db(next(ts))
            remote_recreate_timestamp = next(ts)
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)
            # older delete "local" broker
            broker.delete_db(next(ts))

    def test_sync_remote_recreate_policy_over_older_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # older delete "local" broker
            broker.delete_db(next(ts))
            # recreate "remote" broker
            remote_broker.delete_db(next(ts))
            remote_recreate_timestamp = next(ts)
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)

    def test_sync_remote_recreate_policy_over_older_local_recreate(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(next(ts), policy.idx)
            # create "remote" broker
            remote_broker.initialize(next(ts), remote_policy.idx)
            # older recreate "local" broker
            broker.delete_db(next(ts))
            local_recreate_timestamp = next(ts)
            broker.update_put_timestamp(local_recreate_timestamp)
            broker.update_status_changed_at(local_recreate_timestamp)
            # recreate "remote" broker
            remote_broker.delete_db(next(ts))
            remote_recreate_timestamp = next(ts)
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)

    def test_sync_to_remote_with_misplaced(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        # create "local" broker
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(next(ts), policy.idx)

        # create "remote" broker
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(next(ts), remote_policy.idx)
        # add misplaced row to remote_broker
        remote_broker.put_object(
            '/a/c/o', next(ts), 0, 'content-type',
            'etag', storage_policy_index=remote_broker.storage_policy_index)
        # since this row matches policy index or remote, it shows up in count
        self.assertEqual(remote_broker.get_info()['object_count'], 1)
        self.assertEqual([], remote_broker.get_misplaced_since(-1, 1))

        # replicate
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)
        # since our local broker has no rows to push it logs as no_change
        self.assertEqual(1, daemon.stats['no_change'])
        self.assertEqual(0, broker.get_info()['object_count'])

        # remote broker updates it's policy index; this makes the remote
        # broker's object count change
        info = remote_broker.get_info()
        expectations = {
            'object_count': 0,
            'storage_policy_index': policy.idx,
        }
        for key, value in expectations.items():
            self.assertEqual(info[key], value)
        # but it also knows those objects are misplaced now
        misplaced = remote_broker.get_misplaced_since(-1, 100)
        self.assertEqual(len(misplaced), 1)

        # we also pushed out to node 3 with rsync
        self.assertEqual(1, daemon.stats['rsync'])
        third_broker = self._get_broker('a', 'c', node_index=2)
        info = third_broker.get_info()
        for key, value in expectations.items():
            self.assertEqual(info[key], value)

    def test_misplaced_rows_replicate_and_enqueue(self):
        # force all timestamps to fall in same hour
        ts = (Timestamp(t) for t in
              itertools.count(int(time.time()) // 3600 * 3600))
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(next(ts).internal, policy.idx)
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(next(ts).internal, remote_policy.idx)

        # add a misplaced row to *local* broker
        obj_put_timestamp = next(ts).internal
        broker.put_object(
            'o', obj_put_timestamp, 0, 'content-type',
            'etag', storage_policy_index=remote_policy.idx)
        misplaced = broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 1)
        # since this row is misplaced it doesn't show up in count
        self.assertEqual(broker.get_info()['object_count'], 0)

        # add another misplaced row to *local* broker with composite timestamp
        ts_data = next(ts)
        ts_ctype = next(ts)
        ts_meta = next(ts)
        broker.put_object(
            'o2', ts_data.internal, 0, 'content-type',
            'etag', storage_policy_index=remote_policy.idx,
            ctype_timestamp=ts_ctype.internal, meta_timestamp=ts_meta.internal)
        misplaced = broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 2)
        # since this row is misplaced it doesn't show up in count
        self.assertEqual(broker.get_info()['object_count'], 0)

        # replicate
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)
        # push to remote, and third node was missing (also maybe reconciler)
        self.assertTrue(2 < daemon.stats['rsync'] <= 3, daemon.stats['rsync'])
        self.assertEqual(
            1, self.logger.statsd_client.get_stats_counts().get(
                'reconciler_db_created'))
        self.assertFalse(
            self.logger.statsd_client.get_stats_counts().get(
                'reconciler_db_exists'))

        # grab the rsynced instance of remote_broker
        remote_broker = self._get_broker('a', 'c', node_index=1)

        # remote has misplaced rows too now
        misplaced = remote_broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 2)

        # and the correct policy_index and object_count
        info = remote_broker.get_info()
        expectations = {
            'object_count': 0,
            'storage_policy_index': policy.idx,
        }
        for key, value in expectations.items():
            self.assertEqual(info[key], value)

        # and we should have also enqueued these rows in a single reconciler,
        # since we forced the object timestamps to be in the same hour.
        self.logger.clear()
        reconciler = daemon.get_reconciler_broker(misplaced[0]['created_at'])
        self.assertFalse(
            self.logger.statsd_client.get_stats_counts().get(
                'reconciler_db_created'))
        self.assertEqual(
            1, self.logger.statsd_client.get_stats_counts().get(
                'reconciler_db_exists'))
        # but it may not be on the same node as us anymore though...
        reconciler = self._get_broker(reconciler.account,
                                      reconciler.container, node_index=0)
        self.assertEqual(reconciler.get_info()['object_count'], 2)
        objects = reconciler.list_objects_iter(
            10, '', None, None, None, None, storage_policy_index=0)
        self.assertEqual(len(objects), 2)
        expected = ('%s:/a/c/o' % remote_policy.idx, obj_put_timestamp, 0,
                    'application/x-put', obj_put_timestamp)
        self.assertEqual(objects[0], expected)
        # the second object's listing has ts_meta as its last modified time
        # but its full composite timestamp is in the hash field.
        expected = ('%s:/a/c/o2' % remote_policy.idx, ts_meta.internal, 0,
                    'application/x-put',
                    encode_timestamps(ts_data, ts_ctype, ts_meta))
        self.assertEqual(objects[1], expected)

        # having safely enqueued to the reconciler we can advance
        # our sync pointer
        self.assertEqual(broker.get_reconciler_sync(), 2)

    def test_misplaced_rows_replicate_and_enqueue_from_old_style_shard(self):
        # force all timestamps to fall in same hour
        ts = (Timestamp(t) for t in
              itertools.count(int(time.time()) // 3600 * 3600))
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('.shards_a', 'some-other-c', node_index=0)
        broker.initialize(next(ts).internal, policy.idx)
        broker.set_sharding_sysmeta('Root', 'a/c')
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker(
            '.shards_a', 'some-other-c', node_index=1)
        remote_broker.initialize(next(ts).internal, remote_policy.idx)

        # add a misplaced row to *local* broker
        obj_put_timestamp = next(ts).internal
        broker.put_object(
            'o', obj_put_timestamp, 0, 'content-type',
            'etag', storage_policy_index=remote_policy.idx)
        misplaced = broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 1)
        # since this row is misplaced it doesn't show up in count
        self.assertEqual(broker.get_info()['object_count'], 0)

        # add another misplaced row to *local* broker with composite timestamp
        ts_data = next(ts)
        ts_ctype = next(ts)
        ts_meta = next(ts)
        broker.put_object(
            'o2', ts_data.internal, 0, 'content-type',
            'etag', storage_policy_index=remote_policy.idx,
            ctype_timestamp=ts_ctype.internal, meta_timestamp=ts_meta.internal)
        misplaced = broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 2)
        # since this row is misplaced it doesn't show up in count
        self.assertEqual(broker.get_info()['object_count'], 0)

        # replicate
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)
        # push to remote, and third node was missing (also maybe reconciler)
        self.assertTrue(2 < daemon.stats['rsync'] <= 3, daemon.stats['rsync'])

        # grab the rsynced instance of remote_broker
        remote_broker = self._get_broker(
            '.shards_a', 'some-other-c', node_index=1)

        # remote has misplaced rows too now
        misplaced = remote_broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 2)

        # and the correct policy_index and object_count
        info = remote_broker.get_info()
        expectations = {
            'object_count': 0,
            'storage_policy_index': policy.idx,
        }
        for key, value in expectations.items():
            self.assertEqual(info[key], value)

        # and we should have also enqueued these rows in a single reconciler,
        # since we forced the object timestamps to be in the same hour.
        reconciler = daemon.get_reconciler_broker(misplaced[0]['created_at'])
        # but it may not be on the same node as us anymore though...
        reconciler = self._get_broker(reconciler.account,
                                      reconciler.container, node_index=0)
        self.assertEqual(reconciler.get_info()['object_count'], 2)
        objects = reconciler.list_objects_iter(
            10, '', None, None, None, None, storage_policy_index=0)
        self.assertEqual(len(objects), 2)
        # NB: reconciler work is for the *root* container!
        expected = ('%s:/a/c/o' % remote_policy.idx, obj_put_timestamp, 0,
                    'application/x-put', obj_put_timestamp)
        self.assertEqual(objects[0], expected)
        # the second object's listing has ts_meta as its last modified time
        # but its full composite timestamp is in the hash field.
        expected = ('%s:/a/c/o2' % remote_policy.idx, ts_meta.internal, 0,
                    'application/x-put',
                    encode_timestamps(ts_data, ts_ctype, ts_meta))
        self.assertEqual(objects[1], expected)

        # having safely enqueued to the reconciler we can advance
        # our sync pointer
        self.assertEqual(broker.get_reconciler_sync(), 2)

    def test_misplaced_rows_replicate_and_enqueue_from_shard(self):
        # force all timestamps to fall in same hour
        ts = (Timestamp(t) for t in
              itertools.count(int(time.time()) // 3600 * 3600))
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('.shards_a', 'some-other-c', node_index=0)
        broker.initialize(next(ts).internal, policy.idx)
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker(
            '.shards_a', 'some-other-c', node_index=1)
        remote_broker.initialize(next(ts).internal, remote_policy.idx)

        # add a misplaced row to *local* broker
        obj_put_timestamp = next(ts).internal
        broker.put_object(
            'o', obj_put_timestamp, 0, 'content-type',
            'etag', storage_policy_index=remote_policy.idx)
        misplaced = broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 1)
        # since this row is misplaced it doesn't show up in count
        self.assertEqual(broker.get_info()['object_count'], 0)

        # add another misplaced row to *local* broker with composite timestamp
        ts_data = next(ts)
        ts_ctype = next(ts)
        ts_meta = next(ts)
        broker.put_object(
            'o2', ts_data.internal, 0, 'content-type',
            'etag', storage_policy_index=remote_policy.idx,
            ctype_timestamp=ts_ctype.internal, meta_timestamp=ts_meta.internal)
        misplaced = broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 2)
        # since this row is misplaced it doesn't show up in count
        self.assertEqual(broker.get_info()['object_count'], 0)

        # replicate
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)
        # push to remote, and third node was missing (also maybe reconciler)
        self.assertTrue(2 < daemon.stats['rsync'] <= 3, daemon.stats['rsync'])

        # grab the rsynced instance of remote_broker
        remote_broker = self._get_broker(
            '.shards_a', 'some-other-c', node_index=1)

        # remote has misplaced rows too now
        misplaced = remote_broker.get_misplaced_since(-1, 10)
        self.assertEqual(len(misplaced), 2)

        # and the correct policy_index and object_count
        info = remote_broker.get_info()
        expectations = {
            'object_count': 0,
            'storage_policy_index': policy.idx,
        }
        for key, value in expectations.items():
            self.assertEqual(info[key], value)

        # and we should have also enqueued these rows in a single reconciler,
        # since we forced the object timestamps to be in the same hour.
        reconciler = daemon.get_reconciler_broker(misplaced[0]['created_at'])
        # but it may not be on the same node as us anymore though...
        reconciler = self._get_broker(reconciler.account,
                                      reconciler.container, node_index=0)
        self.assertEqual(reconciler.get_info()['object_count'], 2)
        objects = reconciler.list_objects_iter(
            10, '', None, None, None, None, storage_policy_index=0)
        self.assertEqual(len(objects), 2)
        # NB: reconciler work is for the *root* container!
        expected = ('%s:/a/c/o' % remote_policy.idx, obj_put_timestamp, 0,
                    'application/x-put', obj_put_timestamp)
        self.assertEqual(objects[0], expected)
        # the second object's listing has ts_meta as its last modified time
        # but its full composite timestamp is in the hash field.
        expected = ('%s:/a/c/o2' % remote_policy.idx, ts_meta.internal, 0,
                    'application/x-put',
                    encode_timestamps(ts_data, ts_ctype, ts_meta))
        self.assertEqual(objects[1], expected)

        # having safely enqueued to the reconciler we can advance
        # our sync pointer
        self.assertEqual(broker.get_reconciler_sync(), 2)

    def test_multiple_out_sync_reconciler_enqueue_normalize(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(next(ts), policy.idx)
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(next(ts), remote_policy.idx)

        # add some rows to brokers
        for db in (broker, remote_broker):
            for p in (policy, remote_policy):
                db.put_object('o-%s' % p.name, next(ts), 0, 'content-type',
                              'etag', storage_policy_index=p.idx)
            db._commit_puts()

        expected_policy_stats = {
            policy.idx: {'object_count': 1, 'bytes_used': 0},
            remote_policy.idx: {'object_count': 1, 'bytes_used': 0},
        }
        for db in (broker, remote_broker):
            policy_stats = db.get_policy_stats()
            self.assertEqual(policy_stats, expected_policy_stats)

        # each db has 2 rows, 4 total
        all_items = set()
        for db in (broker, remote_broker):
            items = db.get_items_since(-1, 4)
            all_items.update(
                (item['name'], item['created_at']) for item in items)
        self.assertEqual(4, len(all_items))

        # replicate both ways
        part, node = self._get_broker_part_node(broker)
        self._run_once(node)
        part, node = self._get_broker_part_node(remote_broker)
        self._run_once(node)

        # only the latest timestamps should survive
        most_recent_items = {}
        for name, timestamp in all_items:
            most_recent_items[name] = max(
                timestamp, most_recent_items.get(name, ''))
        self.assertEqual(2, len(most_recent_items))

        for db in (broker, remote_broker):
            items = db.get_items_since(-1, 4)
            self.assertEqual(len(items), len(most_recent_items))
            for item in items:
                self.assertEqual(most_recent_items[item['name']],
                                 item['created_at'])

        # and the reconciler also collapses updates
        reconciler_containers = set()
        for item in all_items:
            _name, timestamp = item
            reconciler_containers.add(
                get_reconciler_container_name(timestamp))

        reconciler_items = set()
        for reconciler_container in reconciler_containers:
            for node_index in range(3):
                reconciler = self._get_broker(MISPLACED_OBJECTS_ACCOUNT,
                                              reconciler_container,
                                              node_index=node_index)
                items = reconciler.get_items_since(-1, 4)
                reconciler_items.update(
                    (item['name'], item['created_at']) for item in items)
        # they can't *both* be in the wrong policy ;)
        self.assertEqual(1, len(reconciler_items))
        for reconciler_name, timestamp in reconciler_items:
            _policy_index, path = reconciler_name.split(':', 1)
            a, c, name = path.lstrip('/').split('/')
            self.assertEqual(most_recent_items[name], timestamp)

    @contextmanager
    def _wrap_update_reconciler_sync(self, broker, calls):
        def wrapper_function(*args, **kwargs):
            calls.append(args)
            orig_function(*args, **kwargs)

        orig_function = broker.update_reconciler_sync
        broker.update_reconciler_sync = wrapper_function
        try:
            yield True
        finally:
            broker.update_reconciler_sync = orig_function

    def test_post_replicate_hook(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(next(ts), 0)
        broker.put_object('foo', next(ts), 0, 'text/plain', 'xyz', deleted=0,
                          storage_policy_index=0)
        info = broker.get_replication_info()
        self.assertEqual(1, info['max_row'])
        self.assertEqual(-1, broker.get_reconciler_sync())
        daemon = replicator.ContainerReplicator({})
        calls = []
        with self._wrap_update_reconciler_sync(broker, calls):
            daemon._post_replicate_hook(broker, info, [])
        self.assertEqual(1, len(calls))
        # repeated call to _post_replicate_hook with no change to info
        # should not call update_reconciler_sync
        calls = []
        with self._wrap_update_reconciler_sync(broker, calls):
            daemon._post_replicate_hook(broker, info, [])
        self.assertEqual(0, len(calls))

    def test_update_sync_store_exception(self):
        class FakeContainerSyncStore(object):
            def update_sync_store(self, broker):
                raise OSError(1, '1')

        daemon = replicator.ContainerReplicator({}, logger=self.logger)
        daemon.sync_store = FakeContainerSyncStore()
        ts_iter = make_timestamp_iter()
        broker = self._get_broker('a', 'c', node_index=0)
        timestamp = next(ts_iter)
        broker.initialize(timestamp.internal, POLICIES.default.idx)
        info = broker.get_replication_info()
        daemon._post_replicate_hook(broker, info, [])
        log_lines = self.logger.get_lines_for_level('error')
        self.assertEqual(1, len(log_lines))
        self.assertIn('Failed to update sync_store', log_lines[0])

    def test_update_sync_store(self):
        klass = 'swift.container.sync_store.ContainerSyncStore'
        daemon = replicator.ContainerReplicator({})
        daemon.sync_store = sync_store.ContainerSyncStore(
            daemon.root, daemon.logger, daemon.mount_check)
        ts_iter = make_timestamp_iter()
        broker = self._get_broker('a', 'c', node_index=0)
        timestamp = next(ts_iter)
        broker.initialize(timestamp.internal, POLICIES.default.idx)
        info = broker.get_replication_info()
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                daemon._post_replicate_hook(broker, info, [])
        self.assertEqual(0, mock_remove.call_count)
        self.assertEqual(0, mock_add.call_count)

        timestamp = next(ts_iter)
        # sync-to and sync-key empty - remove from store
        broker.update_metadata(
            {'X-Container-Sync-To': ('', timestamp.internal),
             'X-Container-Sync-Key': ('', timestamp.internal)})
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                daemon._post_replicate_hook(broker, info, [])
        self.assertEqual(0, mock_add.call_count)
        mock_remove.assert_called_once_with(broker)

        timestamp = next(ts_iter)
        # sync-to is not empty sync-key is empty - remove from store
        broker.update_metadata(
            {'X-Container-Sync-To': ('a', timestamp.internal)})
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                daemon._post_replicate_hook(broker, info, [])
        self.assertEqual(0, mock_add.call_count)
        mock_remove.assert_called_once_with(broker)

        timestamp = next(ts_iter)
        # sync-to is empty sync-key is not empty - remove from store
        broker.update_metadata(
            {'X-Container-Sync-To': ('', timestamp.internal),
             'X-Container-Sync-Key': ('secret', timestamp.internal)})
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                daemon._post_replicate_hook(broker, info, [])
        self.assertEqual(0, mock_add.call_count)
        mock_remove.assert_called_once_with(broker)

        timestamp = next(ts_iter)
        # sync-to, sync-key both not empty - add to store
        broker.update_metadata(
            {'X-Container-Sync-To': ('a', timestamp.internal),
             'X-Container-Sync-Key': ('secret', timestamp.internal)})
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                daemon._post_replicate_hook(broker, info, [])
        mock_add.assert_called_once_with(broker)
        self.assertEqual(0, mock_remove.call_count)

        timestamp = next(ts_iter)
        # container is removed - need to remove from store
        broker.delete_db(timestamp.internal)
        broker.update_metadata(
            {'X-Container-Sync-To': ('a', timestamp.internal),
             'X-Container-Sync-Key': ('secret', timestamp.internal)})
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                daemon._post_replicate_hook(broker, info, [])
        self.assertEqual(0, mock_add.call_count)
        mock_remove.assert_called_once_with(broker)

    def test_sync_triggers_sync_store_update(self):
        klass = 'swift.container.sync_store.ContainerSyncStore'
        ts_iter = make_timestamp_iter()
        # Create two containers as follows:
        # broker_1 which is not set for sync
        # broker_2 which is set for sync and then unset
        # test that while replicating both we see no activity
        # for broker_1, and the anticipated activity for broker_2
        broker_1 = self._get_broker('a', 'c', node_index=0)
        broker_1.initialize(next(ts_iter).internal, POLICIES.default.idx)
        broker_2 = self._get_broker('b', 'd', node_index=0)
        broker_2.initialize(next(ts_iter).internal, POLICIES.default.idx)
        broker_2.update_metadata(
            {'X-Container-Sync-To': ('a', next(ts_iter).internal),
             'X-Container-Sync-Key': ('secret', next(ts_iter).internal)})

        # replicate once according to broker_1
        # relying on the fact that FakeRing would place both
        # in the same partition.
        part, node = self._get_broker_part_node(broker_1)
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                self._run_once(node)
        self.assertEqual(1, mock_add.call_count)
        self.assertEqual(broker_2.db_file, mock_add.call_args[0][0].db_file)
        self.assertEqual(0, mock_remove.call_count)

        broker_2.update_metadata(
            {'X-Container-Sync-To': ('', next(ts_iter).internal)})
        # replicate once this time according to broker_2
        # relying on the fact that FakeRing would place both
        # in the same partition.
        part, node = self._get_broker_part_node(broker_2)
        with mock.patch(klass + '.remove_synced_container') as mock_remove:
            with mock.patch(klass + '.add_synced_container') as mock_add:
                self._run_once(node)
        self.assertEqual(0, mock_add.call_count)
        self.assertEqual(1, mock_remove.call_count)
        self.assertEqual(broker_2.db_file, mock_remove.call_args[0][0].db_file)

    def test_cleanup_post_replicate(self):
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = Timestamp.now()
        broker.initialize(put_timestamp.internal, POLICIES.default.idx)
        orig_info = broker.get_replication_info()
        daemon = replicator.ContainerReplicator({}, logger=self.logger)

        # db should not be here, replication ok, deleted
        res = daemon.cleanup_post_replicate(broker, orig_info, [True] * 3)
        self.assertTrue(res)
        self.assertFalse(os.path.exists(broker.db_file))
        self.assertEqual(['Successfully deleted db %s' % broker.db_file],
                         daemon.logger.get_lines_for_level('debug'))
        daemon.logger.clear()

        # failed replication, not deleted
        broker.initialize(put_timestamp.internal, POLICIES.default.idx)
        orig_info = broker.get_replication_info()
        res = daemon.cleanup_post_replicate(broker, orig_info,
                                            [False, True, True])
        self.assertTrue(res)
        self.assertTrue(os.path.exists(broker.db_file))
        self.assertEqual(['Not deleting db %s (2/3 success)' % broker.db_file],
                         daemon.logger.get_lines_for_level('debug'))
        daemon.logger.clear()

        # db has shard ranges, not deleted
        broker.enable_sharding(Timestamp.now())
        broker.merge_shard_ranges(
            [ShardRange('.shards_a/c', Timestamp.now(), '', 'm')])
        self.assertTrue(broker.sharding_required())  # sanity check
        res = daemon.cleanup_post_replicate(broker, orig_info, [True] * 3)
        self.assertTrue(res)
        self.assertTrue(os.path.exists(broker.db_file))
        self.assertEqual(
            ['Not deleting db %s (requires sharding, state unsharded)' %
             broker.db_file],
            daemon.logger.get_lines_for_level('debug'))
        daemon.logger.clear()

        # db sharding, not deleted
        self._goto_sharding_state(broker, Timestamp.now())
        self.assertTrue(broker.sharding_required())  # sanity check
        orig_info = broker.get_replication_info()
        res = daemon.cleanup_post_replicate(broker, orig_info, [True] * 3)
        self.assertTrue(res)
        self.assertTrue(os.path.exists(broker.db_file))
        self.assertEqual(
            ['Not deleting db %s (requires sharding, state sharding)' %
             broker.db_file],
            daemon.logger.get_lines_for_level('debug'))
        daemon.logger.clear()

        # db sharded, should not be here, failed replication, not deleted
        self._goto_sharded_state(broker)
        self.assertFalse(broker.sharding_required())  # sanity check
        res = daemon.cleanup_post_replicate(broker, orig_info,
                                            [True, False, True])
        self.assertTrue(res)
        self.assertTrue(os.path.exists(broker.db_file))
        self.assertEqual(['Not deleting db %s (2/3 success)' %
                          broker.db_file],
                         daemon.logger.get_lines_for_level('debug'))
        daemon.logger.clear()

        # db sharded, should not be here, new shard ranges (e.g. from reverse
        # replication), deleted
        broker.merge_shard_ranges(
            [ShardRange('.shards_a/c', Timestamp.now(), '', 'm')])
        res = daemon.cleanup_post_replicate(broker, orig_info, [True] * 3)
        self.assertTrue(res)
        self.assertFalse(os.path.exists(broker.db_file))
        daemon.logger.clear()

        # db sharded, should not be here, replication ok, deleted
        broker.initialize(put_timestamp.internal, POLICIES.default.idx)
        self.assertTrue(os.path.exists(broker.db_file))
        orig_info = broker.get_replication_info()
        res = daemon.cleanup_post_replicate(broker, orig_info, [True] * 3)
        self.assertTrue(res)
        self.assertFalse(os.path.exists(broker.db_file))
        self.assertEqual(['Successfully deleted db %s' % broker.db_file],
                         daemon.logger.get_lines_for_level('debug'))
        daemon.logger.clear()

    def test_sync_shard_ranges_merge_remote_osr(self):
        def do_test(local_osr, remote_osr, exp_merge, exp_warning,
                    exp_rpc_warning):
            put_timestamp = Timestamp.now().internal
            # create "local" broker
            broker = self._get_broker('a', 'c', node_index=0)
            broker.initialize(put_timestamp, POLICIES.default.idx)
            # create "remote" broker
            remote_broker = self._get_broker('a', 'c', node_index=1)
            remote_broker.initialize(put_timestamp, POLICIES.default.idx)

            bounds = (('', 'g'), ('g', 'r'), ('r', ''))
            shard_ranges = [
                ShardRange('.shards_a/sr-%s' % upper, Timestamp.now(), lower,
                           upper, i + 1, 10 * (i + 1))
                for i, (lower, upper) in enumerate(bounds)
            ]

            for db in (broker, remote_broker):
                db.merge_shard_ranges(shard_ranges)

            if local_osr:
                broker.merge_shard_ranges(ShardRange(**dict(local_osr)))
            if remote_osr:
                remote_broker.merge_shard_ranges(
                    ShardRange(**dict(remote_osr)))

            daemon = replicator.ContainerReplicator({}, logger=debug_logger())
            part, remote_node = self._get_broker_part_node(remote_broker)
            part, local_node = self._get_broker_part_node(broker)
            info = broker.get_replication_info()
            success = daemon._repl_to_node(remote_node, broker, part, info)
            self.assertTrue(success)
            local_info = self._get_broker(
                'a', 'c', node_index=0).get_info()
            remote_info = self._get_broker(
                'a', 'c', node_index=1).get_info()
            for k, v in local_info.items():
                if k == 'id':
                    continue
                self.assertEqual(remote_info[k], v,
                                 "mismatch remote %s %r != %r" % (
                                     k, remote_info[k], v))
            actual_osr = broker.get_own_shard_range(no_default=True)
            actual_osr = dict(actual_osr) if actual_osr else actual_osr
            if exp_merge:
                exp_osr = (dict(remote_osr, meta_timestamp=mock.ANY)
                           if remote_osr else remote_osr)
            else:
                exp_osr = (dict(local_osr, meta_timestamp=mock.ANY)
                           if local_osr else local_osr)
            self.assertEqual(exp_osr, actual_osr)
            lines = daemon.logger.get_lines_for_level('warning')
            if exp_warning:
                self.assertEqual(len(lines), 1, lines)
                self.assertIn("Ignoring remote osr w/o epoch", lines[0])
                self.assertIn("own_sr: ", lines[0])
                self.assertIn("'epoch': '%s'" % local_osr.epoch.normal,
                              lines[0])
                self.assertIn("remote_sr: ", lines[0])
                self.assertIn("'epoch': None", lines[0])
                hash_ = os.path.splitext(os.path.basename(broker.db_file))[0]
                url = "%s/%s/%s/%s" % (
                    remote_node['ip'], remote_node['device'], part, hash_)
                self.assertIn("source: %s" % url, lines[0])
            else:
                self.assertFalse(lines)
            lines = self.rpc.logger.get_lines_for_level('warning')
            if exp_rpc_warning:
                self.assertEqual(len(lines), 1, lines)
                self.assertIn("Ignoring remote osr w/o epoch", lines[0])
                self.assertIn("source: repl_req", lines[0])
            else:
                self.assertFalse(lines)

            os.remove(broker.db_file)
            os.remove(remote_broker.db_file)
            return daemon

        # we'll use other broker as a template to use the "default" osrs
        other_broker = self._get_broker('a', 'c', node_index=2)
        other_broker.initialize(Timestamp.now().internal, POLICIES.default.idx)
        default_osr = other_broker.get_own_shard_range()
        self.assertIsNone(default_osr.epoch)
        osr_with_epoch = other_broker.get_own_shard_range()
        osr_with_epoch.epoch = Timestamp.now()
        osr_with_different_epoch = other_broker.get_own_shard_range()
        osr_with_different_epoch.epoch = Timestamp.now()
        default_osr_newer = ShardRange(**dict(default_osr))
        default_osr_newer.timestamp = Timestamp.now()

        # local_osr, remote_osr, exp_merge, exp_warning, exp_rpc_warning
        tests = (
            # First the None case, ie no osrs
            (None, None, False, False, False),
            # Default and not the other
            (None, default_osr, True, False, False),
            (default_osr, None, False, False, False),
            (default_osr, default_osr, True, False, False),
            (default_osr, None, False, False, False),
            # With an epoch and no OSR is also fine
            (None, osr_with_epoch, True, False, False),
            (osr_with_epoch, None, False, False, False),
            # even with the same or different epochs
            (osr_with_epoch, osr_with_epoch, True, False, False),
            (osr_with_epoch, osr_with_different_epoch, True, False, False),
            # But if local does have an epoch but the remote doesn't: false
            # positive, nothing will merge anyway, no warning.
            (osr_with_epoch, default_osr, False, False, False),
            # It's also OK if the remote has an epoch but not the local,
            # this also works on the RPC side because merge_shards happen on
            # to local then sends updated shards to the remote. So if the
            # OSR on the remote is newer then the default the RPC side will
            # actually get a merged OSR, ie get the remote one back.
            (default_osr, osr_with_epoch, True, False, False),
            # But if the local default is newer then the epoched remote side
            # we'd get an error logged on the RPC side and the local is newer
            # so wil fail to merge
            (default_osr_newer, osr_with_epoch, False, False, True),
        )
        for i, params in enumerate(tests):
            with annotate_failure((i, params)):
                do_test(*params)

    def test_sync_shard_ranges(self):
        put_timestamp = Timestamp.now().internal
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_timestamp, POLICIES.default.idx)

        def check_replicate(expected_shard_ranges, from_broker, to_broker):
            daemon = replicator.ContainerReplicator({}, logger=debug_logger())
            part, node = self._get_broker_part_node(to_broker)
            info = broker.get_replication_info()
            success = daemon._repl_to_node(node, from_broker, part, info)
            self.assertTrue(success)
            self.assertEqual(
                expected_shard_ranges,
                to_broker.get_all_shard_range_data()
            )
            local_info = self._get_broker(
                'a', 'c', node_index=0).get_info()
            remote_info = self._get_broker(
                'a', 'c', node_index=1).get_info()
            for k, v in local_info.items():
                if k == 'id':
                    continue
                self.assertEqual(remote_info[k], v,
                                 "mismatch remote %s %r != %r" % (
                                     k, remote_info[k], v))
            return daemon

        bounds = (('', 'g'), ('g', 'r'), ('r', ''))
        shard_ranges = [
            ShardRange('.shards_a/sr-%s' % upper, Timestamp.now(), lower,
                       upper, i + 1, 10 * (i + 1))
            for i, (lower, upper) in enumerate(bounds)
        ]
        # add first two shard_ranges to both brokers
        for shard_range in shard_ranges[:2]:
            for db in (broker, remote_broker):
                db.merge_shard_ranges(shard_range)
        # now add a shard range and an object to the "local" broker only
        broker.merge_shard_ranges(shard_ranges[2])
        broker_ranges = broker.get_all_shard_range_data()
        self.assertShardRangesEqual(shard_ranges, broker_ranges)
        broker.put_object('obj', Timestamp.now().internal, 0, 'text/plain',
                          MD5_OF_EMPTY_STRING)
        # sharding not yet enabled so replication not deferred
        daemon = check_replicate(broker_ranges, broker, remote_broker)
        self.assertEqual(0, daemon.stats['deferred'])
        self.assertEqual(0, daemon.stats['no_change'])
        self.assertEqual(0, daemon.stats['rsync'])
        self.assertEqual(1, daemon.stats['diff'])
        self.assertEqual({'diffs': 1},
                         daemon.logger.statsd_client.get_increment_counts())

        # update one shard range
        shard_ranges[1].update_meta(50, 50)
        # sharding not yet enabled so replication not deferred, but the two
        # brokers' object tables are in sync so no rsync or usync either
        daemon = check_replicate(broker_ranges, broker, remote_broker)
        self.assertEqual(0, daemon.stats['deferred'])
        self.assertEqual(1, daemon.stats['no_change'])
        self.assertEqual(0, daemon.stats['rsync'])
        self.assertEqual(0, daemon.stats['diff'])
        self.assertEqual({'no_changes': 1},
                         daemon.logger.statsd_client.get_increment_counts())

        # now enable local broker for sharding
        own_sr = broker.enable_sharding(Timestamp.now())
        # update one shard range
        shard_ranges[1].update_meta(13, 123)
        broker.merge_shard_ranges(shard_ranges[1])
        broker_ranges = broker.get_all_shard_range_data()
        self.assertShardRangesEqual(shard_ranges + [own_sr], broker_ranges)

        def check_stats(daemon):
            self.assertEqual(1, daemon.stats['deferred'])
            self.assertEqual(0, daemon.stats['no_change'])
            self.assertEqual(0, daemon.stats['rsync'])
            self.assertEqual(0, daemon.stats['diff'])
            self.assertFalse(daemon.logger.statsd_client.get_increments())

        daemon = check_replicate(broker_ranges, broker, remote_broker)
        check_stats(daemon)

        # update one shard range
        shard_ranges[1].update_meta(99, 0)
        broker.merge_shard_ranges(shard_ranges[1])
        # sanity check
        broker_ranges = broker.get_all_shard_range_data()
        self.assertShardRangesEqual(shard_ranges + [own_sr], broker_ranges)
        daemon = check_replicate(broker_ranges, broker, remote_broker)
        check_stats(daemon)

        # delete one shard range
        shard_ranges[0].deleted = 1
        shard_ranges[0].timestamp = Timestamp.now()
        broker.merge_shard_ranges(shard_ranges[0])
        # sanity check
        broker_ranges = broker.get_all_shard_range_data()
        self.assertShardRangesEqual(shard_ranges + [own_sr], broker_ranges)
        daemon = check_replicate(broker_ranges, broker, remote_broker)
        check_stats(daemon)

        # put a shard range again
        shard_ranges[2].timestamp = Timestamp.now()
        shard_ranges[2].object_count = 0
        broker.merge_shard_ranges(shard_ranges[2])
        # sanity check
        broker_ranges = broker.get_all_shard_range_data()
        self.assertShardRangesEqual(shard_ranges + [own_sr], broker_ranges)
        daemon = check_replicate(broker_ranges, broker, remote_broker)
        check_stats(daemon)

        # update same shard range on local and remote, remote later
        shard_ranges[-1].meta_timestamp = Timestamp.now()
        shard_ranges[-1].bytes_used += 1000
        broker.merge_shard_ranges(shard_ranges[-1])
        remote_shard_ranges = remote_broker.get_shard_ranges(
            include_deleted=True)
        remote_shard_ranges[-1].meta_timestamp = Timestamp.now()
        remote_shard_ranges[-1].bytes_used += 2000
        remote_broker.merge_shard_ranges(remote_shard_ranges[-1])
        # sanity check
        remote_broker_ranges = remote_broker.get_all_shard_range_data()
        self.assertShardRangesEqual(remote_shard_ranges + [own_sr],
                                    remote_broker_ranges)
        self.assertShardRangesNotEqual(shard_ranges, remote_shard_ranges)
        daemon = check_replicate(remote_broker_ranges, broker, remote_broker)
        check_stats(daemon)

        # undelete shard range *on the remote*
        deleted_ranges = [sr for sr in remote_shard_ranges if sr.deleted]
        self.assertEqual([shard_ranges[0]], deleted_ranges)
        deleted_ranges[0].deleted = 0
        deleted_ranges[0].timestamp = Timestamp.now()
        remote_broker.merge_shard_ranges(deleted_ranges[0])
        # sanity check
        remote_broker_ranges = remote_broker.get_all_shard_range_data()
        self.assertShardRangesEqual(remote_shard_ranges + [own_sr],
                                    remote_broker_ranges)
        self.assertShardRangesNotEqual(shard_ranges, remote_shard_ranges)
        daemon = check_replicate(remote_broker_ranges, broker, remote_broker)
        check_stats(daemon)

        # reverse replication direction and expect syncs to propagate
        daemon = check_replicate(remote_broker_ranges, remote_broker, broker)
        check_stats(daemon)

    def test_sync_shard_ranges_error(self):
        # verify that replication is not considered successful if
        # merge_shard_ranges fails
        put_time = Timestamp.now().internal
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_time, POLICIES.default.idx)
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_time, POLICIES.default.idx)
        # put an object into local broker
        broker.put_object('obj', Timestamp.now().internal, 0, 'text/plain',
                          MD5_OF_EMPTY_STRING)
        # get an own shard range into local broker
        broker.enable_sharding(Timestamp.now())
        self.assertFalse(broker.sharding_initiated())

        replicate_hook = mock.MagicMock()
        fake_repl_connection = attach_fake_replication_rpc(
            self.rpc, errors={'merge_shard_ranges': [
                FakeHTTPResponse(HTTPServerError())]},
            replicate_hook=replicate_hook)
        db_replicator.ReplConnection = fake_repl_connection
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        daemon = replicator.ContainerReplicator({})
        daemon.logger = debug_logger()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertFalse(success)
        # broker only has its own shard range so expect objects to be sync'd
        self.assertEqual(
            ['sync', 'merge_shard_ranges', 'merge_items',
             'merge_syncs'],
            [call[0][0] for call in replicate_hook.call_args_list])
        error_lines = daemon.logger.get_lines_for_level('error')
        self.assertIn('Bad response 500', error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(1, daemon.stats['diff'])
        self.assertEqual(
            1, daemon.logger.statsd_client.get_increment_counts()['diffs'])

    def test_sync_shard_ranges_timeout_in_fetch(self):
        # verify that replication is not considered successful if
        # merge_shard_ranges fails
        put_time = Timestamp.now().internal
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_time, POLICIES.default.idx)
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_time, POLICIES.default.idx)
        # get an own shard range into remote broker
        remote_broker.enable_sharding(Timestamp.now())

        replicate_calls = []

        def replicate_hook(op, *args):
            replicate_calls.append(op)
            if op == 'get_shard_ranges':
                sleep(0.1)

        fake_repl_connection = attach_fake_replication_rpc(
            self.rpc, replicate_hook=replicate_hook)
        db_replicator.ReplConnection = fake_repl_connection
        part, node = self._get_broker_part_node(remote_broker)
        daemon = replicator.ContainerReplicator({'node_timeout': '0.001'})
        daemon.logger = debug_logger()
        with mock.patch.object(daemon.ring, 'get_part_nodes',
                               return_value=[node]), \
                mock.patch.object(daemon, '_post_replicate_hook'):
            success, _ = daemon._replicate_object(
                part, broker.db_file, node['id'])
        self.assertFalse(success)
        # broker only has its own shard range so expect objects to be sync'd
        self.assertEqual(['sync', 'get_shard_ranges'], replicate_calls)
        error_lines = daemon.logger.get_lines_for_level('error')
        self.assertIn('ERROR syncing /', error_lines[0])
        self.assertFalse(error_lines[1:])
        self.assertEqual(0, daemon.stats['diff'])
        self.assertNotIn(
            'diffs', daemon.logger.statsd_client.get_increment_counts())
        self.assertEqual(1, daemon.stats['failure'])
        self.assertEqual(
            1, daemon.logger.statsd_client.get_increment_counts()['failures'])

    def test_sync_shard_ranges_none_to_sync(self):
        # verify that merge_shard_ranges is not sent if there are no shard
        # ranges to sync
        put_time = Timestamp.now().internal
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_time, POLICIES.default.idx)
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_time, POLICIES.default.idx)
        # put an object into local broker
        broker.put_object('obj', Timestamp.now().internal, 0, 'text/plain',
                          MD5_OF_EMPTY_STRING)

        replicate_hook = mock.MagicMock()
        fake_repl_connection = attach_fake_replication_rpc(
            self.rpc, replicate_hook=replicate_hook)
        db_replicator.ReplConnection = fake_repl_connection
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        daemon = replicator.ContainerReplicator({})
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # NB: remote has no shard ranges, so no call to get_shard_ranges
        self.assertEqual(
            ['sync', 'merge_items', 'merge_syncs'],
            [call[0][0] for call in replicate_hook.call_args_list])

    def test_sync_shard_ranges_trouble_receiving_so_none_to_sync(self):
        # verify that merge_shard_ranges is not sent if local has no shard
        # ranges to sync
        put_time = Timestamp.now().internal
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(put_time, POLICIES.default.idx)
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(put_time, POLICIES.default.idx)
        # ensure the remote has at least one shard range
        remote_broker.enable_sharding(Timestamp.now())
        # put an object into local broker
        broker.put_object('obj', Timestamp.now().internal, 0, 'text/plain',
                          MD5_OF_EMPTY_STRING)

        replicate_hook = mock.MagicMock()
        fake_repl_connection = attach_fake_replication_rpc(
            self.rpc, errors={'get_shard_ranges': [
                FakeHTTPResponse(HTTPServerError())]},
            replicate_hook=replicate_hook)
        db_replicator.ReplConnection = fake_repl_connection
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        daemon = replicator.ContainerReplicator({})
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # NB: remote had shard ranges, but there was... some sort of issue
        # in getting them locally, so no call to merge_shard_ranges
        self.assertEqual(
            ['sync', 'get_shard_ranges', 'merge_items', 'merge_syncs'],
            [call[0][0] for call in replicate_hook.call_args_list])

    def test_sync_shard_ranges_with_rsync(self):
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)

        bounds = (('', 'g'), ('g', 'r'), ('r', ''))
        shard_ranges = [
            ShardRange('.shards_a/sr-%s' % upper, Timestamp.now(), lower,
                       upper, i + 1, 10 * (i + 1))
            for i, (lower, upper) in enumerate(bounds)
        ]
        # add first shard range
        own_sr = broker.enable_sharding(Timestamp.now())
        broker.merge_shard_ranges(shard_ranges[:1])

        # "replicate"
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)
        self.assertEqual(2, daemon.stats['rsync'])

        # complete rsync to all other nodes
        def check_replicate(expected_ranges):
            for i in range(1, 3):
                remote_broker = self._get_broker('a', 'c', node_index=i)
                self.assertTrue(os.path.exists(remote_broker.db_file))
                self.assertShardRangesEqual(
                    expected_ranges,
                    remote_broker.get_shard_ranges(include_deleted=True,
                                                   include_own=True)
                )
                remote_info = remote_broker.get_info()
                local_info = self._get_broker(
                    'a', 'c', node_index=0).get_info()
                for k, v in local_info.items():
                    if k == 'id':
                        continue
                    if k == 'hash':
                        self.assertEqual(remote_info[k], '0' * 32)
                        continue
                    if k == 'object_count':
                        self.assertEqual(remote_info[k], 0)
                        continue
                    self.assertEqual(remote_info[k], v,
                                     "mismatch remote %s %r != %r" % (
                                         k, remote_info[k], v))

        check_replicate([shard_ranges[0], own_sr])

        # delete and add some more shard ranges
        shard_ranges[0].deleted = 1
        shard_ranges[0].timestamp = Timestamp.now()
        for shard_range in shard_ranges:
            broker.merge_shard_ranges(shard_range)
        daemon = self._run_once(node)
        self.assertEqual(2, daemon.stats['deferred'])
        check_replicate(shard_ranges + [own_sr])

    def check_replicate(self, from_broker, remote_node_index, repl_conf=None,
                        expect_success=True):
        repl_conf = repl_conf or {}
        repl_calls = []
        rsync_calls = []

        def repl_hook(op, *sync_args):
            repl_calls.append((op, sync_args))

        fake_repl_connection = attach_fake_replication_rpc(
            self.rpc, replicate_hook=repl_hook, errors=None)
        db_replicator.ReplConnection = fake_repl_connection
        daemon = replicator.ContainerReplicator(
            repl_conf, logger=debug_logger())
        self._install_fake_rsync_file(daemon, rsync_calls)
        part, nodes = self._ring.get_nodes(from_broker.account,
                                           from_broker.container)

        def find_node(node_index):
            for node in nodes:
                if node['index'] == node_index:
                    return node
            else:
                self.fail('Failed to find node index %s' % remote_node_index)

        remote_node = find_node(remote_node_index)
        info = from_broker.get_replication_info()
        success = daemon._repl_to_node(remote_node, from_broker, part, info)
        self.assertEqual(expect_success, success)
        return daemon, repl_calls, rsync_calls

    def assert_synced_shard_ranges(self, expected, synced_items):
        expected.sort(key=lambda sr: (sr.lower, sr.upper))
        for item in synced_items:
            item.pop('record_type', None)
        self.assertEqual([dict(ex) for ex in expected], synced_items)

    def assert_info_synced(self, local, remote_node_index, mismatches=None):
        mismatches = mismatches or []
        mismatches.append('id')
        remote = self._get_broker(local.account, local.container,
                                  node_index=remote_node_index)
        local_info = local.get_info()
        remote_info = remote.get_info()
        errors = []
        for k, v in local_info.items():
            if remote_info.get(k) == v:
                if k in mismatches:
                    errors.append(
                        "unexpected match remote %s %r == %r" % (
                            k, remote_info[k], v))
                    continue
            else:
                if k not in mismatches:
                    errors.append(
                        "unexpected mismatch remote %s %r != %r" % (
                            k, remote_info[k], v))
        if errors:
            self.fail('Found sync errors:\n' + '\n'.join(errors))

    def assert_shard_ranges_synced(self, local_broker, remote_broker):
        self.assertShardRangesEqual(
            local_broker.get_shard_ranges(include_deleted=True,
                                          include_own=True),
            remote_broker.get_shard_ranges(include_deleted=True,
                                           include_own=True)
        )

    def _setup_replication_test(self, node_index):
        ts_iter = make_timestamp_iter()
        policy_idx = POLICIES.default.idx
        put_timestamp = Timestamp.now().internal
        # create "local" broker
        broker = self._get_broker('a', 'c', node_index=node_index)
        broker.initialize(put_timestamp, policy_idx)

        objs = [{'name': 'blah%03d' % i, 'created_at': next(ts_iter).internal,
                 'size': i, 'content_type': 'text/plain', 'etag': 'etag%s' % i,
                 'deleted': 0, 'storage_policy_index': policy_idx}
                for i in range(20)]
        bounds = (('', 'a'), ('a', 'b'), ('b', 'c'), ('c', ''))
        shard_ranges = [
            ShardRange(
                '.sharded_a/sr-%s' % upper, Timestamp.now(), lower, upper)
            for i, (lower, upper) in enumerate(bounds)
        ]
        return {'broker': broker,
                'objects': objs,
                'shard_ranges': shard_ranges}

    def _merge_object(self, broker, objects, index, **kwargs):
        if not isinstance(index, slice):
            index = slice(index, index + 1)
        objs = [dict(obj) for obj in objects[index]]
        broker.merge_items(objs)

    def _merge_shard_range(self, broker, shard_ranges, index, **kwargs):
        broker.merge_shard_ranges(shard_ranges[index:index + 1])

    def _goto_sharding_state(self, broker, epoch):
        broker.enable_sharding(epoch)
        self.assertTrue(broker.set_sharding_state())
        self.assertEqual(backend.SHARDING, broker.get_db_state())

    def _goto_sharded_state(self, broker):
        self.assertTrue(broker.set_sharded_state())
        self.assertEqual(backend.SHARDED, broker.get_db_state())

    def _assert_local_sharded_in_sync(self, local_broker, local_id):
        daemon, repl_calls, rsync_calls = self.check_replicate(local_broker, 1)
        self.assertEqual(['sync', 'get_shard_ranges', 'merge_shard_ranges'],
                         [call[0] for call in repl_calls])
        self.assertEqual(1, daemon.stats['deferred'])
        self.assertEqual(0, daemon.stats['rsync'])
        self.assertEqual(0, daemon.stats['diff'])
        self.assertFalse(rsync_calls)
        # new db sync
        self.assertEqual(local_id, repl_calls[0][1][2])
        # ...but we still get a merge_shard_ranges for shard ranges
        self.assert_synced_shard_ranges(
            local_broker.get_shard_ranges(include_own=True),
            repl_calls[2][1][0])
        self.assertEqual(local_id, repl_calls[2][1][1])

    def _check_only_shard_ranges_replicated(self, local_broker,
                                            remote_node_index,
                                            repl_conf,
                                            expected_shard_ranges,
                                            remote_has_shards=True,
                                            expect_success=True):
        # expected_shard_ranges is expected final list of sync'd ranges
        daemon, repl_calls, rsync_calls = self.check_replicate(
            local_broker, remote_node_index, repl_conf,
            expect_success=expect_success)

        # we always expect only shard ranges to end in abort
        self.assertEqual(1, daemon.stats['deferred'])
        self.assertEqual(0, daemon.stats['diff'])
        self.assertEqual(0, daemon.stats['rsync'])
        if remote_has_shards:
            exp_calls = ['sync', 'get_shard_ranges', 'merge_shard_ranges']
        else:
            exp_calls = ['sync', 'merge_shard_ranges']
        self.assertEqual(exp_calls, [call[0] for call in repl_calls])
        self.assertFalse(rsync_calls)
        # sync
        local_id = local_broker.get_info()['id']
        self.assertEqual(local_id, repl_calls[0][1][2])
        # get_shard_ranges
        if remote_has_shards:
            self.assertEqual((), repl_calls[1][1])
        # merge_shard_ranges for sending local shard ranges
        self.assertShardRangesEqual(expected_shard_ranges,
                                    repl_calls[-1][1][0])
        self.assertEqual(local_id, repl_calls[-1][1][1])
        remote_broker = self._get_broker(
            local_broker.account, local_broker.container, node_index=1)
        self.assertNotEqual(local_id, remote_broker.get_info()['id'])
        self.assert_shard_ranges_synced(remote_broker, local_broker)

    def test_replication_local_unsharded_remote_missing(self):
        context = self._setup_replication_test(0)
        local_broker = context['broker']
        local_id = local_broker.get_info()['id']
        objs = context['objects']
        self._merge_object(index=0, **context)

        daemon, repl_calls, rsync_calls = self.check_replicate(local_broker, 1)

        self.assert_info_synced(local_broker, 1)
        self.assertEqual(1, daemon.stats['rsync'])
        self.assertEqual(['sync', 'complete_rsync'],
                         [call[0] for call in repl_calls])
        self.assertEqual(local_id, repl_calls[1][1][0])
        self.assertEqual(os.path.basename(local_broker.db_file),
                         repl_calls[1][1][1])
        self.assertEqual(local_broker.db_file, rsync_calls[0][0])
        self.assertEqual(local_id, os.path.basename(rsync_calls[0][1]))
        self.assertFalse(rsync_calls[1:])
        remote_broker = self._get_broker('a', 'c', node_index=1)
        self.assert_shard_ranges_synced(local_broker, remote_broker)
        self.assertTrue(os.path.exists(remote_broker._db_file))
        self.assertNotEqual(local_id, remote_broker.get_info()['id'])
        self.assertEqual(objs[:1], remote_broker.get_objects())

    def _check_replication_local_unsharded_remote_sharded(self, repl_conf):
        context = self._setup_replication_test(0)
        local_broker = context['broker']
        local_id = local_broker.get_info()['id']
        self._merge_object(index=slice(0, 6), **context)

        remote_context = self._setup_replication_test(1)
        self._merge_object(index=4, **remote_context)
        remote_broker = remote_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(remote_broker, epoch=epoch)
        remote_context['shard_ranges'][0].object_count = 101
        remote_context['shard_ranges'][0].bytes_used = 1010
        remote_context['shard_ranges'][0].state = ShardRange.ACTIVE
        self._merge_shard_range(index=0, **remote_context)
        self._merge_object(index=5, **remote_context)
        self._goto_sharded_state(remote_broker)
        self.assertEqual(backend.SHARDED, remote_broker.get_db_state())

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            remote_broker.get_shard_ranges(include_own=True))

        remote_broker = self._get_broker(
            local_broker.account, local_broker.container, node_index=1)
        self.assertEqual(backend.SHARDED, remote_broker.get_db_state())
        self.assertFalse(os.path.exists(remote_broker._db_file))
        self.assertNotEqual(local_id, remote_broker.get_info()['id'])
        self.assertEqual(remote_context['objects'][5:6],
                         remote_broker.get_objects())

        # Now that we have shard ranges, we're never considered in-sync :-/
        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            remote_broker.get_shard_ranges(include_own=True))

    def test_replication_local_unsharded_remote_sharded(self):
        self._check_replication_local_unsharded_remote_sharded({})

    def test_replication_local_unsharded_remote_sharded_large_diff(self):
        self._check_replication_local_unsharded_remote_sharded({'per_diff': 1})

    def _check_replication_local_sharding_remote_missing(self, repl_conf):
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        self._merge_object(index=0, **local_context)
        self._merge_object(index=1, **local_context)
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch)
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(2, 8), **local_context)
        objs = local_context['objects']

        daemon, repl_calls, rsync_calls = self.check_replicate(
            local_broker, 1, repl_conf=repl_conf)

        self.assertEqual(['sync', 'complete_rsync'],
                         [call[0] for call in repl_calls])
        self.assertEqual(1, daemon.stats['rsync'])
        self.assertEqual(0, daemon.stats['deferred'])
        self.assertEqual(0, daemon.stats['diff'])

        # fresh db is sync'd first...
        fresh_id = local_broker.get_info()['id']
        self.assertEqual(fresh_id, repl_calls[0][1][2])
        self.assertEqual(fresh_id, repl_calls[1][1][0])
        # retired db is not sync'd at all
        old_broker = self.backend(
            local_broker._db_file, account=local_broker.account,
            container=local_broker.container, force_db_file=True)
        old_id = old_broker.get_info()['id']
        bad_calls = []
        for call in repl_calls:
            if old_id in call[1]:
                bad_calls.append(
                    'old db id %r in %r call args %r' % (
                        old_id, call[0], call[1]))
        if bad_calls:
            self.fail('Found some bad calls:\n' + '\n'.join(bad_calls))
        # complete_rsync
        self.assertEqual(os.path.basename(local_broker.db_file),
                         repl_calls[1][1][1])
        self.assertEqual(local_broker.db_file, rsync_calls[0][0])
        self.assertEqual(fresh_id, os.path.basename(rsync_calls[0][1]))
        self.assertFalse(rsync_calls[1:])

        # TODO: make these stats better; in sharding state local broker pulls
        # stats for 2 objects from old db, whereas remote thinks it's sharded
        # and has an empty shard range table
        self.assert_info_synced(local_broker, 1, mismatches=[
            'object_count', 'bytes_used', 'db_state'])

        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_id = remote_broker.get_info()['id']
        self.assertNotEqual(old_id, remote_id)
        self.assertNotEqual(fresh_id, remote_id)
        self.assertEqual(
            [remote_broker.db_file], get_db_files(remote_broker.db_file))
        self.assertEqual(os.path.basename(remote_broker.db_file),
                         os.path.basename(local_broker.db_file))
        self.assertEqual(epoch, remote_broker.db_epoch)
        # remote db has only the misplaced objects
        self.assertEqual(objs[2:8], remote_broker.get_objects())
        self.assert_shard_ranges_synced(local_broker, remote_broker)

        # replicate again, check asserts abort
        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            local_broker.get_shard_ranges(include_own=True))

        # sanity
        remote_broker = self._get_broker('a', 'c', node_index=1)
        self.assertEqual(
            [remote_broker.db_file], get_db_files(remote_broker.db_file))
        self.assertEqual(os.path.basename(remote_broker.db_file),
                         os.path.basename(local_broker.db_file))
        self.assertEqual(objs[2:8], remote_broker.get_objects())
        self.assertEqual(epoch, remote_broker.db_epoch)

    def test_replication_local_sharding_remote_missing(self):
        self._check_replication_local_sharding_remote_missing({})

    def test_replication_local_sharding_remote_missing_large_diff(self):
        # the local shard db has large diff with respect to the old db
        self._check_replication_local_sharding_remote_missing({'per_diff': 1})

    def _check_replication_local_sharding_remote_unsharded(self, repl_conf):
        local_context = self._setup_replication_test(0)
        self._merge_object(index=slice(0, 3), **local_context)
        local_broker = local_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch)
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(3, 11), **local_context)

        remote_context = self._setup_replication_test(1)
        self._merge_object(index=11, **remote_context)

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            local_broker.get_shard_ranges(include_own=True),
            remote_has_shards=False)

        remote_broker = self._get_broker('a', 'c', node_index=1)
        self.assertEqual(
            [remote_broker._db_file], get_db_files(remote_broker.db_file))
        self.assertEqual(remote_context['objects'][11:12],
                         remote_broker.get_objects())

        self.assert_info_synced(
            local_broker, 1,
            mismatches=['db_state', 'object_count', 'bytes_used',
                        'status_changed_at', 'hash'])

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            local_broker.get_shard_ranges(include_own=True))

    def test_replication_local_sharding_remote_unsharded(self):
        self._check_replication_local_sharding_remote_unsharded({})

    def test_replication_local_sharding_remote_unsharded_large_diff(self):
        self._check_replication_local_sharding_remote_unsharded(
            {'per_diff': 1})

    def _check_only_sync(self, local_broker, remote_node_index, repl_conf):
        daemon, repl_calls, rsync_calls = self.check_replicate(
            local_broker, remote_node_index, repl_conf,
            expect_success=False)

        # When talking to an old (pre-2.18.0) container server, abort
        # replication when we're sharding or sharded. Wait for the
        # rolling upgrade that's presumably in-progress to finish instead.
        self.assertEqual(1, daemon.stats['deferred'])
        self.assertEqual(0, daemon.stats['diff'])
        self.assertEqual(0, daemon.stats['rsync'])
        self.assertEqual(['sync'],
                         [call[0] for call in repl_calls])
        self.assertFalse(rsync_calls)
        lines = daemon.logger.get_lines_for_level('warning')
        self.assertIn('unable to replicate shard ranges', lines[0])
        self.assertIn('refusing to replicate objects', lines[1])
        self.assertFalse(lines[2:])
        # sync
        local_id = local_broker.get_info()['id']
        self.assertEqual(local_id, repl_calls[0][1][2])
        remote_broker = self._get_broker(
            local_broker.account, local_broker.container, node_index=1)
        self.assertNotEqual(local_id, remote_broker.get_info()['id'])
        self.assertEqual([], remote_broker.get_shard_ranges())

    def _check_replication_local_sharding_remote_presharding(self, repl_conf):
        local_context = self._setup_replication_test(0)
        self._merge_object(index=slice(0, 3), **local_context)
        local_broker = local_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch)
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(3, 11), **local_context)

        remote_context = self._setup_replication_test(1)
        self._merge_object(index=11, **remote_context)

        orig_get_remote_info = \
            replicator.ContainerReplicatorRpc._get_synced_replication_info

        def presharding_get_remote_info(*args):
            rinfo = orig_get_remote_info(*args)
            del rinfo['shard_max_row']
            return rinfo

        with mock.patch('swift.container.replicator.'
                        'ContainerReplicatorRpc._get_synced_replication_info',
                        presharding_get_remote_info):
            self._check_only_sync(local_broker, 1, repl_conf)

            remote_broker = self._get_broker('a', 'c', node_index=1)
            self.assertEqual(
                [remote_broker._db_file], get_db_files(remote_broker.db_file))
            self.assertEqual(remote_context['objects'][11:12],
                             remote_broker.get_objects())

            self.assert_info_synced(
                local_broker, 1,
                mismatches=['db_state', 'object_count', 'bytes_used',
                            'status_changed_at', 'hash'])

            self._check_only_sync(local_broker, 1, repl_conf)

    def test_replication_local_sharding_remote_presharding(self):
        self._check_replication_local_sharding_remote_presharding({})

    def test_replication_local_sharding_remote_presharding_large_diff(self):
        self._check_replication_local_sharding_remote_presharding(
            {'per_diff': 1})

    def _check_replication_local_sharding_remote_sharding(self, repl_conf):
        local_context = self._setup_replication_test(0)
        self._merge_object(index=slice(0, 5), **local_context)
        local_broker = local_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch)
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(5, 10), **local_context)

        remote_context = self._setup_replication_test(1)
        self._merge_object(index=12, **remote_context)
        # take snapshot of info now before transition to sharding...
        orig_remote_info = remote_context['broker'].get_info()
        remote_broker = remote_context['broker']
        self._goto_sharding_state(remote_broker, epoch)
        self._merge_shard_range(index=0, **remote_context)
        self._merge_object(index=13, **remote_context)

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            remote_broker.get_shard_ranges(include_own=True))

        # in sharding state brokers only reports object stats from old db, and
        # they are different
        self.assert_info_synced(
            local_broker, 1, mismatches=['object_count', 'bytes_used',
                                         'status_changed_at', 'hash'])

        remote_broker = self._get_broker('a', 'c', node_index=1)
        shard_db = make_db_file_path(remote_broker._db_file, epoch)
        self.assertEqual([remote_broker._db_file, shard_db],
                         get_db_files(remote_broker.db_file))
        shard_db = make_db_file_path(remote_broker._db_file, epoch)
        self.assertEqual([remote_broker._db_file, shard_db],
                         get_db_files(remote_broker.db_file))
        # no local objects have been sync'd to remote shard db
        self.assertEqual(remote_context['objects'][13:14],
                         remote_broker.get_objects())
        # remote *old db* is unchanged
        remote_old_broker = self.backend(
            remote_broker._db_file, account=remote_broker.account,
            container=remote_broker.container, force_db_file=True)
        self.assertEqual(remote_context['objects'][12:13],
                         remote_old_broker.get_objects())
        self.assertFalse(remote_old_broker.get_shard_ranges())
        remote_old_info = remote_old_broker.get_info()
        orig_remote_info.pop('db_state')
        remote_old_info.pop('db_state')
        self.assertEqual(orig_remote_info, remote_old_info)

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            local_broker.get_shard_ranges(include_own=True))

    def test_replication_local_sharding_remote_sharding(self):
        self._check_replication_local_sharding_remote_sharding({})

    def test_replication_local_sharding_remote_sharding_large_diff(self):
        self._check_replication_local_sharding_remote_sharding({'per_diff': 1})

    def test_replication_local_sharded_remote_missing(self):
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch)
        local_context['shard_ranges'][0].object_count = 99
        local_context['shard_ranges'][0].state = ShardRange.ACTIVE
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(0, 3), **local_context)
        self._goto_sharded_state(local_broker)
        objs = local_context['objects']

        daemon, repl_calls, rsync_calls = self.check_replicate(local_broker, 1)

        self.assertEqual(['sync', 'complete_rsync'],
                         [call[0] for call in repl_calls])
        self.assertEqual(1, daemon.stats['rsync'])

        # sync
        local_id = local_broker.get_info()['id']
        self.assertEqual(local_id, repl_calls[0][1][2])
        # complete_rsync
        self.assertEqual(local_id, repl_calls[1][1][0])
        self.assertEqual(
            os.path.basename(local_broker.db_file), repl_calls[1][1][1])
        self.assertEqual(local_broker.db_file, rsync_calls[0][0])
        self.assertEqual(local_id, os.path.basename(rsync_calls[0][1]))
        self.assertFalse(rsync_calls[1:])

        self.assert_info_synced(local_broker, 1)

        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_id = remote_broker.get_info()['id']
        self.assertNotEqual(local_id, remote_id)
        shard_db = make_db_file_path(remote_broker._db_file, epoch)
        self.assertEqual([shard_db],
                         get_db_files(remote_broker.db_file))
        self.assertEqual(objs[:3], remote_broker.get_objects())
        self.assertEqual(local_broker.get_shard_ranges(),
                         remote_broker.get_shard_ranges())

        # sanity check - in sync
        self._assert_local_sharded_in_sync(local_broker, local_id)

        remote_broker = self._get_broker('a', 'c', node_index=1)
        shard_db = make_db_file_path(remote_broker._db_file, epoch)
        self.assertEqual([shard_db],
                         get_db_files(remote_broker.db_file))
        # the remote broker object_count comes from replicated shard range...
        self.assertEqual(99, remote_broker.get_info()['object_count'])
        # these are replicated misplaced objects...
        self.assertEqual(objs[:3], remote_broker.get_objects())
        self.assertEqual(local_broker.get_shard_ranges(),
                         remote_broker.get_shard_ranges())

    def _check_replication_local_sharded_remote_unsharded(self, repl_conf):
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch)
        local_context['shard_ranges'][0].object_count = 99
        local_context['shard_ranges'][0].state = ShardRange.ACTIVE
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(0, 3), **local_context)
        self._goto_sharded_state(local_broker)

        remote_context = self._setup_replication_test(1)
        self._merge_object(index=4, **remote_context)

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            local_broker.get_shard_ranges(include_own=True),
            remote_has_shards=False,
            expect_success=True)

        # sharded broker takes object count from shard range whereas remote
        # unsharded broker takes it from object table
        self.assert_info_synced(
            local_broker, 1,
            mismatches=['db_state', 'object_count', 'bytes_used',
                        'status_changed_at', 'hash'])

        remote_broker = self._get_broker('a', 'c', node_index=1)
        self.assertEqual([remote_broker._db_file],
                         get_db_files(remote_broker.db_file))
        self.assertEqual(remote_context['objects'][4:5],
                         remote_broker.get_objects())

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            local_broker.get_shard_ranges(include_own=True),
            # We just sent shards, so of course remote has some
            remote_has_shards=True,
            expect_success=True)

        remote_broker = self._get_broker('a', 'c', node_index=1)
        self.assertEqual([remote_broker._db_file],
                         get_db_files(remote_broker.db_file))
        self.assertEqual(remote_context['objects'][4:5],
                         remote_broker.get_objects())

    def test_replication_local_sharded_remote_unsharded(self):
        self._check_replication_local_sharded_remote_unsharded({})

    def test_replication_local_sharded_remote_unsharded_large_diff(self):
        self._check_replication_local_sharded_remote_unsharded({'per_diff': 1})

    def _check_replication_local_sharded_remote_sharding(self, repl_conf):
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch=epoch)
        local_context['shard_ranges'][0].object_count = 99
        local_context['shard_ranges'][0].bytes_used = 999
        local_context['shard_ranges'][0].state = ShardRange.ACTIVE
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(0, 5), **local_context)
        self._goto_sharded_state(local_broker)

        remote_context = self._setup_replication_test(1)
        self._merge_object(index=6, **remote_context)
        remote_broker = remote_context['broker']
        remote_info_orig = remote_broker.get_info()
        self._goto_sharding_state(remote_broker, epoch=epoch)
        self._merge_shard_range(index=0, **remote_context)
        self._merge_object(index=7, **remote_context)

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            # remote has newer timestamp for shard range
            remote_broker.get_shard_ranges(include_own=True),
            expect_success=True)

        # sharded broker takes object count from shard range whereas remote
        # sharding broker takes it from object table
        self.assert_info_synced(
            local_broker, 1,
            mismatches=['db_state', 'object_count', 'bytes_used',
                        'status_changed_at', 'hash'])

        remote_broker = self._get_broker('a', 'c', node_index=1)
        shard_db = make_db_file_path(remote_broker._db_file, epoch)
        self.assertEqual([remote_broker._db_file, shard_db],
                         get_db_files(remote_broker.db_file))
        # remote fresh db objects are unchanged
        self.assertEqual(remote_context['objects'][7:8],
                         remote_broker.get_objects())
        # remote old hash.db objects are unchanged
        remote_old_broker = self.backend(
            remote_broker._db_file, account=remote_broker.account,
            container=remote_broker.container, force_db_file=True)
        self.assertEqual(
            remote_context['objects'][6:7],
            remote_old_broker.get_objects())
        remote_info = remote_old_broker.get_info()
        remote_info_orig.pop('db_state')
        remote_info.pop('db_state')
        self.assertEqual(remote_info_orig, remote_info)
        self.assertEqual(local_broker.get_shard_ranges(),
                         remote_broker.get_shard_ranges())

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            remote_broker.get_shard_ranges(include_own=True),
            expect_success=True)

    def test_replication_local_sharded_remote_sharding(self):
        self._check_replication_local_sharded_remote_sharding({})

    def test_replication_local_sharded_remote_sharding_large_diff(self):
        self._check_replication_local_sharded_remote_sharding({'per_diff': 1})

    def _check_replication_local_sharded_remote_sharded(self, repl_conf):
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        epoch = Timestamp.now()
        self._goto_sharding_state(local_broker, epoch)
        local_context['shard_ranges'][0].object_count = 99
        local_context['shard_ranges'][0].bytes_used = 999
        local_context['shard_ranges'][0].state = ShardRange.ACTIVE
        self._merge_shard_range(index=0, **local_context)
        self._merge_object(index=slice(0, 6), **local_context)
        self._goto_sharded_state(local_broker)

        remote_context = self._setup_replication_test(1)
        self._merge_object(index=6, **remote_context)
        remote_broker = remote_context['broker']
        self._goto_sharding_state(remote_broker, epoch)
        remote_context['shard_ranges'][0].object_count = 101
        remote_context['shard_ranges'][0].bytes_used = 1010
        remote_context['shard_ranges'][0].state = ShardRange.ACTIVE
        self._merge_shard_range(index=0, **remote_context)
        self._merge_object(index=7, **remote_context)
        self._goto_sharded_state(remote_broker)

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            # remote has newer timestamp for shard range
            remote_broker.get_shard_ranges(include_own=True),
            expect_success=True)

        self.assert_info_synced(
            local_broker, 1,
            mismatches=['status_changed_at', 'hash'])

        remote_broker = self._get_broker('a', 'c', node_index=1)
        shard_db = make_db_file_path(remote_broker._db_file, epoch)
        self.assertEqual([shard_db],
                         get_db_files(remote_broker.db_file))
        self.assertEqual(remote_context['objects'][7:8],
                         remote_broker.get_objects())
        # remote shard range was newer than local so object count is not
        # updated by sync'd shard range
        self.assertEqual(
            101, remote_broker.get_shard_ranges()[0].object_count)

        self._check_only_shard_ranges_replicated(
            local_broker, 1, repl_conf,
            # remote has newer timestamp for shard range
            remote_broker.get_shard_ranges(include_own=True),
            expect_success=True)

    def test_replication_local_sharded_remote_sharded(self):
        self._check_replication_local_sharded_remote_sharded({})

    def test_replication_local_sharded_remote_sharded_large_diff(self):
        self._check_replication_local_sharded_remote_sharded({'per_diff': 1})

    def test_replication_rsync_then_merge_aborts_before_merge_sharding(self):
        # verify that rsync_then_merge aborts if remote starts sharding during
        # the rsync
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        self._merge_object(index=slice(0, 3), **local_context)
        remote_context = self._setup_replication_test(1)
        remote_broker = remote_context['broker']
        remote_broker.logger = debug_logger()
        self._merge_object(index=5, **remote_context)

        orig_func = replicator.ContainerReplicatorRpc.rsync_then_merge

        def mock_rsync_then_merge(*args):
            remote_broker.merge_shard_ranges(
                ShardRange('.shards_a/cc', Timestamp.now()))
            self._goto_sharding_state(remote_broker, Timestamp.now())
            return orig_func(*args)

        with mock.patch(
                'swift.container.replicator.ContainerReplicatorRpc.'
                'rsync_then_merge',
                mock_rsync_then_merge):
            with mock.patch(
                    'swift.container.backend.ContainerBroker.'
                    'get_items_since') as mock_get_items_since:
                daemon, repl_calls, rsync_calls = self.check_replicate(
                    local_broker, 1, expect_success=False,
                    repl_conf={'per_diff': 1})

        mock_get_items_since.assert_not_called()
        # No call to get_shard_ranges because remote didn't have shard ranges
        # when the sync arrived
        self.assertEqual(['sync', 'rsync_then_merge'],
                         [call[0] for call in repl_calls])
        self.assertEqual(local_broker.db_file, rsync_calls[0][0])
        self.assertEqual(local_broker.get_info()['id'],
                         os.path.basename(rsync_calls[0][1]))
        self.assertFalse(rsync_calls[1:])

    def test_replication_rsync_then_merge_aborts_before_merge_sharded(self):
        # verify that rsync_then_merge aborts if remote completes sharding
        # during the rsync
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        self._merge_object(index=slice(0, 3), **local_context)
        remote_context = self._setup_replication_test(1)
        remote_broker = remote_context['broker']
        remote_broker.logger = debug_logger()
        self._merge_object(index=5, **remote_context)

        orig_func = replicator.ContainerReplicatorRpc.rsync_then_merge

        def mock_rsync_then_merge(*args):
            remote_broker.merge_shard_ranges(
                ShardRange('.shards_a/cc', Timestamp.now()))
            self._goto_sharding_state(remote_broker, Timestamp.now())
            self._goto_sharded_state(remote_broker)
            return orig_func(*args)

        with mock.patch(
                'swift.container.replicator.ContainerReplicatorRpc.'
                'rsync_then_merge',
                mock_rsync_then_merge):
            with mock.patch(
                    'swift.container.backend.ContainerBroker.'
                    'get_items_since') as mock_get_items_since:
                daemon, repl_calls, rsync_calls = self.check_replicate(
                    local_broker, 1, expect_success=False,
                    repl_conf={'per_diff': 1})

        mock_get_items_since.assert_not_called()
        # No call to get_shard_ranges because remote didn't have shard ranges
        # when the sync arrived
        self.assertEqual(['sync', 'rsync_then_merge'],
                         [call[0] for call in repl_calls])
        self.assertEqual(local_broker.db_file, rsync_calls[0][0])
        self.assertEqual(local_broker.get_info()['id'],
                         os.path.basename(rsync_calls[0][1]))
        self.assertFalse(rsync_calls[1:])

    def test_replication_rsync_then_merge_aborts_after_merge_sharding(self):
        # verify that rsync_then_merge aborts if remote starts sharding during
        # the merge
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        self._merge_object(index=slice(0, 3), **local_context)
        remote_context = self._setup_replication_test(1)
        remote_broker = remote_context['broker']
        remote_broker.logger = debug_logger()
        self._merge_object(index=5, **remote_context)

        orig_get_items_since = backend.ContainerBroker.get_items_since
        calls = []

        def fake_get_items_since(broker, *args):
            # remote starts sharding while rpc call is merging
            if not calls:
                remote_broker.merge_shard_ranges(
                    ShardRange('.shards_a/cc', Timestamp.now()))
                self._goto_sharding_state(remote_broker, Timestamp.now())
            calls.append(args)
            return orig_get_items_since(broker, *args)

        to_patch = 'swift.container.backend.ContainerBroker.get_items_since'
        with mock.patch(to_patch, fake_get_items_since), \
                mock.patch('swift.common.db_replicator.sleep'), \
                mock.patch('swift.container.backend.tpool.execute',
                           lambda func, *args: func(*args)):
            # For some reason, on py3 we start popping Timeouts
            # if we let eventlet trampoline...
            daemon, repl_calls, rsync_calls = self.check_replicate(
                local_broker, 1, expect_success=False,
                repl_conf={'per_diff': 1})

        self.assertEqual(['sync', 'rsync_then_merge'],
                         [call[0] for call in repl_calls])
        self.assertEqual(local_broker.db_file, rsync_calls[0][0])
        self.assertEqual(local_broker.get_info()['id'],
                         os.path.basename(rsync_calls[0][1]))
        self.assertFalse(rsync_calls[1:])

    def test_replication_rsync_then_merge_aborts_after_merge_sharded(self):
        # verify that rsync_then_merge aborts if remote completes sharding
        # during the merge
        local_context = self._setup_replication_test(0)
        local_broker = local_context['broker']
        self._merge_object(index=slice(0, 3), **local_context)
        remote_context = self._setup_replication_test(1)
        remote_broker = remote_context['broker']
        remote_broker.logger = debug_logger()
        self._merge_object(index=5, **remote_context)

        orig_get_items_since = backend.ContainerBroker.get_items_since
        calls = []

        def fake_get_items_since(broker, *args):
            # remote starts sharding while rpc call is merging
            result = orig_get_items_since(broker, *args)
            if calls:
                remote_broker.merge_shard_ranges(
                    ShardRange('.shards_a/cc', Timestamp.now()))
                self._goto_sharding_state(remote_broker, Timestamp.now())
                self._goto_sharded_state(remote_broker)
            calls.append(args)
            return result

        to_patch = 'swift.container.backend.ContainerBroker.get_items_since'
        with mock.patch(to_patch, fake_get_items_since), \
                mock.patch('swift.common.db_replicator.sleep'), \
                mock.patch('swift.container.backend.tpool.execute',
                           lambda func, *args: func(*args)):
            # For some reason, on py3 we start popping Timeouts
            # if we let eventlet trampoline...
            daemon, repl_calls, rsync_calls = self.check_replicate(
                local_broker, 1, expect_success=False,
                repl_conf={'per_diff': 1})

        self.assertEqual(['sync', 'rsync_then_merge'],
                         [call[0] for call in repl_calls])
        self.assertEqual(local_broker.db_file, rsync_calls[0][0])
        self.assertEqual(local_broker.get_info()['id'],
                         os.path.basename(rsync_calls[0][1]))
        self.assertFalse(rsync_calls[1:])

    @mock.patch('swift.common.ring.ring.Ring.get_part_nodes', return_value=[])
    def test_find_local_handoff_for_part(self, mock_part_nodes):

        with mock.patch(
                'swift.common.db_replicator.ring.Ring',
                return_value=self._ring):
            daemon = replicator.ContainerReplicator({}, logger=self.logger)

        # First let's assume we find a primary node
        ring_node1, ring_node2, ring_node3 = daemon.ring.devs[-3:]
        mock_part_nodes.return_value = [ring_node1, ring_node2]
        daemon._local_device_ids = {ring_node1['id']: ring_node1,
                                    ring_node3['id']: ring_node3}
        node = daemon.find_local_handoff_for_part(0)
        self.assertEqual(node['id'], ring_node1['id'])

        # And if we can't find one from the primaries get *some* local device
        mock_part_nodes.return_value = []
        daemon._local_device_ids = {ring_node3['id']: ring_node3}
        node = daemon.find_local_handoff_for_part(0)
        self.assertEqual(node['id'], ring_node3['id'])

        # if there are more then 1 local_dev_id it'll randomly pick one, but
        # not a zero-weight device
        ring_node3['weight'] = 0
        selected_node_ids = set()
        local_dev_ids = {dev['id']: dev for dev in daemon.ring.devs[-3:]}
        daemon._local_device_ids = local_dev_ids
        for _ in range(15):
            node = daemon.find_local_handoff_for_part(0)
            self.assertIn(node['id'], local_dev_ids)
            selected_node_ids.add(node['id'])
            if len(selected_node_ids) == 3:
                break  # unexpected
        self.assertEqual(len(selected_node_ids), 2)
        self.assertEqual([1, 1], [local_dev_ids[dev_id]['weight']
                                  for dev_id in selected_node_ids])
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertFalse(warning_lines)

        # ...unless all devices have zero-weight
        ring_node3['weight'] = 0
        ring_node2['weight'] = 0
        selected_node_ids = set()
        local_dev_ids = {dev['id']: dev for dev in daemon.ring.devs[-2:]}
        daemon._local_device_ids = local_dev_ids
        for _ in range(15):
            self.logger.clear()
            node = daemon.find_local_handoff_for_part(0)
            self.assertIn(node['id'], local_dev_ids)
            selected_node_ids.add(node['id'])
            if len(selected_node_ids) == 2:
                break  # expected
        self.assertEqual(len(selected_node_ids), 2)
        self.assertEqual([0, 0], [local_dev_ids[dev_id]['weight']
                                  for dev_id in selected_node_ids])
        warning_lines = self.logger.get_lines_for_level('warning')
        self.assertEqual(1, len(warning_lines), warning_lines)
        self.assertIn(
            'Could not find a non-zero weight device for handoff partition',
            warning_lines[0])

        # If there are also no local_dev_ids, then we'll get the RuntimeError
        daemon._local_device_ids = {}
        with self.assertRaises(RuntimeError) as dev_err:
            daemon.find_local_handoff_for_part(0)
        expected_error_string = 'Cannot find local handoff; no local devices'
        self.assertEqual(str(dev_err.exception), expected_error_string)


if __name__ == '__main__':
    unittest.main()
