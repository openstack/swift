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
import mock
import random
import sqlite3

from swift.common import db_replicator
from swift.container import replicator, backend, server
from swift.container.reconciler import (
    MISPLACED_OBJECTS_ACCOUNT, get_reconciler_container_name)
from swift.common.utils import Timestamp
from swift.common.storage_policy import POLICIES

from test.unit.common import test_db_replicator
from test.unit import patch_policies
from contextlib import contextmanager


@patch_policies
class TestReplicatorSync(test_db_replicator.TestReplicatorSync):

    backend = backend.ContainerBroker
    datadir = server.DATADIR
    replicator_daemon = replicator.ContainerReplicator
    replicator_rpc = replicator.ContainerReplicatorRpc

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
        # setup a local container
        broker = self._get_broker('a', 'c', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        broker.update_metadata(
            {'x-container-meta-test': ('foo', put_timestamp)})
        # setup remote container
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(time.time(), POLICIES.default.idx)
        timestamp = time.time()
        for db in (broker, remote_broker):
            db.put_object('/a/c/o', timestamp, 0, 'content-type', 'etag',
                          storage_policy_index=db.storage_policy_index)
        # replicate
        daemon = replicator.ContainerReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        with mock.patch.object(db_replicator, 'DEBUG_TIMINGS_THRESHOLD', 0):
            success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])
        expected_timings = ('info', 'update_metadata', 'merge_timestamps',
                            'get_sync', 'merge_syncs')
        debug_lines = self.rpc.logger.logger.get_lines_for_level('debug')
        self.assertEqual(len(expected_timings), len(debug_lines))
        for metric in expected_timings:
            expected = 'replicator-rpc-sync time for %s:' % metric
            self.assert_(any(expected in line for line in debug_lines),
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
        daemon = replicator.ContainerReplicator({})

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
            path = '/a/c/o_missing_%s' % missing_counter.next()
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
        self.assert_('o101' in remote_names)
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
        self.assert_(Timestamp(remote_info['status_changed_at']) >
                     Timestamp(remote_info['put_timestamp']),
                     'remote status_changed_at (%s) is not '
                     'greater than put_timestamp (%s)' % (
                         remote_info['status_changed_at'],
                         remote_info['put_timestamp']))
        self.assert_(Timestamp(remote_info['status_changed_at']) >
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
        put_timestamp = ts.next()
        broker.initialize(put_timestamp, POLICIES.default.idx)
        # setup remote container
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_put_timestamp = ts.next()
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
        local_broker.initialize(ts.next(), policy.idx)

        # create "remote" broker
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(ts.next(), policy.idx)

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
            obj_ts = ts.next()
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
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    def test_sync_local_create_policy_over_newer_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # delete "remote" broker
            remote_broker.delete_db(ts.next())

    def test_sync_local_create_policy_over_older_remote_delete(self):
        # remote_row & both_rows cases are covered by
        # "test_sync_remote_half_delete_policy_over_newer_local_create"
        for setup in self._replication_scenarios(
                'no_row', 'local_row'):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # delete older "remote" broker
            remote_broker.delete_db(ts.next())
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    def test_sync_local_half_delete_policy_over_newer_remote_create(self):
        # no_row & remote_row cases are covered by
        # "test_sync_remote_create_policy_over_older_local_delete"
        for setup in self._replication_scenarios('local_row', 'both_rows'):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # half delete older "local" broker
            broker.delete_db(ts.next())
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    def test_sync_local_recreate_policy_over_newer_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # older recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    def test_sync_local_recreate_policy_over_older_remote_create(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)

    def test_sync_local_recreate_policy_over_newer_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)
            # older delete "remote" broker
            remote_broker.delete_db(ts.next())

    def test_sync_local_recreate_policy_over_older_remote_delete(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older delete "remote" broker
            remote_broker.delete_db(ts.next())
            # recreate "local" broker
            broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            broker.update_put_timestamp(recreate_timestamp)
            broker.update_status_changed_at(recreate_timestamp)

    def test_sync_local_recreate_policy_over_older_remote_recreate(self):
        for setup in self._replication_scenarios():
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # older recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)
            # recreate "local" broker
            broker.delete_db(ts.next())
            local_recreate_timestamp = ts.next()
            broker.update_put_timestamp(local_recreate_timestamp)
            broker.update_status_changed_at(local_recreate_timestamp)

    def test_sync_remote_create_policy_over_newer_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    def test_sync_remote_create_policy_over_newer_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # delete "local" broker
            broker.delete_db(ts.next())

    def test_sync_remote_create_policy_over_older_local_delete(self):
        # local_row & both_rows cases are covered by
        # "test_sync_local_half_delete_policy_over_newer_remote_create"
        for setup in self._replication_scenarios(
                'no_row', 'remote_row', remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # delete older "local" broker
            broker.delete_db(ts.next())
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)

    def test_sync_remote_half_delete_policy_over_newer_local_create(self):
        # no_row & both_rows cases are covered by
        # "test_sync_local_create_policy_over_older_remote_delete"
        for setup in self._replication_scenarios('remote_row', 'both_rows',
                                                 remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # half delete older "remote" broker
            remote_broker.delete_db(ts.next())
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    def test_sync_remote_recreate_policy_over_newer_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older recreate "remote" broker
            remote_broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(recreate_timestamp)
            remote_broker.update_status_changed_at(recreate_timestamp)
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)

    def test_sync_remote_recreate_policy_over_older_local_create(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(recreate_timestamp)
            remote_broker.update_status_changed_at(recreate_timestamp)

    def test_sync_remote_recreate_policy_over_newer_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)
            # older delete "local" broker
            broker.delete_db(ts.next())

    def test_sync_remote_recreate_policy_over_older_local_delete(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older delete "local" broker
            broker.delete_db(ts.next())
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)

    def test_sync_remote_recreate_policy_over_older_local_recreate(self):
        for setup in self._replication_scenarios(remote_wins=True):
            ts, policy, remote_policy, broker, remote_broker = setup
            # create older "local" broker
            broker.initialize(ts.next(), policy.idx)
            # create "remote" broker
            remote_broker.initialize(ts.next(), remote_policy.idx)
            # older recreate "local" broker
            broker.delete_db(ts.next())
            local_recreate_timestamp = ts.next()
            broker.update_put_timestamp(local_recreate_timestamp)
            broker.update_status_changed_at(local_recreate_timestamp)
            # recreate "remote" broker
            remote_broker.delete_db(ts.next())
            remote_recreate_timestamp = ts.next()
            remote_broker.update_put_timestamp(remote_recreate_timestamp)
            remote_broker.update_status_changed_at(remote_recreate_timestamp)

    def test_sync_to_remote_with_misplaced(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        # create "local" broker
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(ts.next(), policy.idx)

        # create "remote" broker
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(ts.next(), remote_policy.idx)
        # add misplaced row to remote_broker
        remote_broker.put_object(
            '/a/c/o', ts.next(), 0, 'content-type',
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
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(ts.next(), policy.idx)
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(ts.next(), remote_policy.idx)

        # add a misplaced row to *local* broker
        obj_put_timestamp = ts.next()
        broker.put_object(
            'o', obj_put_timestamp, 0, 'content-type',
            'etag', storage_policy_index=remote_policy.idx)
        misplaced = broker.get_misplaced_since(-1, 1)
        self.assertEqual(len(misplaced), 1)
        # since this row is misplaced it doesn't show up in count
        self.assertEqual(broker.get_info()['object_count'], 0)

        # replicate
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)
        # push to remote, and third node was missing (also maybe reconciler)
        self.assert_(2 < daemon.stats['rsync'] <= 3)

        # grab the rsynced instance of remote_broker
        remote_broker = self._get_broker('a', 'c', node_index=1)

        # remote has misplaced rows too now
        misplaced = remote_broker.get_misplaced_since(-1, 1)
        self.assertEqual(len(misplaced), 1)

        # and the correct policy_index and object_count
        info = remote_broker.get_info()
        expectations = {
            'object_count': 0,
            'storage_policy_index': policy.idx,
        }
        for key, value in expectations.items():
            self.assertEqual(info[key], value)

        # and we should have also enqeued these rows in the reconciler
        reconciler = daemon.get_reconciler_broker(misplaced[0]['created_at'])
        # but it may not be on the same node as us anymore though...
        reconciler = self._get_broker(reconciler.account,
                                      reconciler.container, node_index=0)
        self.assertEqual(reconciler.get_info()['object_count'], 1)
        objects = reconciler.list_objects_iter(
            1, '', None, None, None, None, storage_policy_index=0)
        self.assertEqual(len(objects), 1)
        expected = ('%s:/a/c/o' % remote_policy.idx, obj_put_timestamp, 0,
                    'application/x-put', obj_put_timestamp)
        self.assertEqual(objects[0], expected)

        # having safely enqueued to the reconciler we can advance
        # our sync pointer
        self.assertEqual(broker.get_reconciler_sync(), 1)

    def test_multiple_out_sync_reconciler_enqueue_normalize(self):
        ts = (Timestamp(t).internal for t in
              itertools.count(int(time.time())))
        policy = random.choice(list(POLICIES))
        broker = self._get_broker('a', 'c', node_index=0)
        broker.initialize(ts.next(), policy.idx)
        remote_policy = random.choice([p for p in POLICIES if p is not
                                       policy])
        remote_broker = self._get_broker('a', 'c', node_index=1)
        remote_broker.initialize(ts.next(), remote_policy.idx)

        # add some rows to brokers
        for db in (broker, remote_broker):
            for p in (policy, remote_policy):
                db.put_object('o-%s' % p.name, ts.next(), 0, 'content-type',
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
                timestamp, most_recent_items.get(name, -1))
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
        broker.initialize(ts.next(), 0)
        broker.put_object('foo', ts.next(), 0, 'text/plain', 'xyz', deleted=0,
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


if __name__ == '__main__':
    unittest.main()
