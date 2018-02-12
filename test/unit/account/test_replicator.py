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
import unittest
import shutil

from swift.account import replicator, backend, server
from swift.common.utils import normalize_timestamp
from swift.common.storage_policy import POLICIES

from test.unit.common import test_db_replicator


class TestReplicatorSync(test_db_replicator.TestReplicatorSync):

    backend = backend.AccountBroker
    datadir = server.DATADIR
    replicator_daemon = replicator.AccountReplicator

    def test_sync(self):
        broker = self._get_broker('a', node_index=0)
        put_timestamp = normalize_timestamp(time.time())
        broker.initialize(put_timestamp)
        # "replicate" to same database
        daemon = replicator.AccountReplicator({})
        part, node = self._get_broker_part_node(broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        # nothing to do
        self.assertTrue(success)
        self.assertEqual(1, daemon.stats['no_change'])

    def test_sync_remote_missing(self):
        broker = self._get_broker('a', node_index=0)
        put_timestamp = time.time()
        broker.initialize(put_timestamp)
        # "replicate" to all other nodes
        part, node = self._get_broker_part_node(broker)
        daemon = self._run_once(node)
        # complete rsync
        self.assertEqual(2, daemon.stats['rsync'])
        local_info = self._get_broker(
            'a', node_index=0).get_info()
        for i in range(1, 3):
            remote_broker = self._get_broker('a', node_index=i)
            self.assertTrue(os.path.exists(remote_broker.db_file))
            remote_info = remote_broker.get_info()
            for k, v in local_info.items():
                if k == 'id':
                    continue
                self.assertEqual(remote_info[k], v,
                                 "mismatch remote %s %r != %r" % (
                                     k, remote_info[k], v))

    def test_sync_remote_missing_most_rows(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', node_index=0)
        broker.initialize(put_timestamp)
        # create "remote" broker
        remote_broker = self._get_broker('a', node_index=1)
        remote_broker.initialize(put_timestamp)
        # add a row to "local" db
        broker.put_container('/a/c', time.time(), 0, 0, 0,
                             POLICIES.default.idx)
        # replicate
        daemon = replicator.AccountReplicator({'per_diff': 1})

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
            'a', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))

    def test_sync_remote_missing_one_rows(self):
        put_timestamp = time.time()
        # create "local" broker
        broker = self._get_broker('a', node_index=0)
        broker.initialize(put_timestamp)
        # create "remote" broker
        remote_broker = self._get_broker('a', node_index=1)
        remote_broker.initialize(put_timestamp)
        # add some rows to both db
        for i in range(10):
            put_timestamp = time.time()
            for db in (broker, remote_broker):
                path = '/a/c_%s' % i
                db.put_container(path, put_timestamp, 0, 0, 0,
                                 POLICIES.default.idx)
        # now a row to the "local" broker only
        broker.put_container('/a/c_missing', time.time(), 0, 0, 0,
                             POLICIES.default.idx)
        # replicate
        daemon = replicator.AccountReplicator({})
        part, node = self._get_broker_part_node(remote_broker)
        info = broker.get_replication_info()
        success = daemon._repl_to_node(node, broker, part, info)
        self.assertTrue(success)
        # row merge
        self.assertEqual(1, daemon.stats['diff'])
        local_info = self._get_broker(
            'a', node_index=0).get_info()
        remote_info = self._get_broker(
            'a', node_index=1).get_info()
        for k, v in local_info.items():
            if k == 'id':
                continue
            self.assertEqual(remote_info[k], v,
                             "mismatch remote %s %r != %r" % (
                                 k, remote_info[k], v))


if __name__ == '__main__':
    unittest.main()
