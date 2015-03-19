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
import itertools
import time
from collections import defaultdict
from eventlet import Timeout

from swift.container.backend import ContainerBroker, DATADIR
from swift.container.reconciler import (
    MISPLACED_OBJECTS_ACCOUNT, incorrect_policy_index,
    get_reconciler_container_name, get_row_to_q_entry_translator)
from swift.common import db_replicator
from swift.common.storage_policy import POLICIES
from swift.common.exceptions import DeviceUnavailable
from swift.common.http import is_success
from swift.common.db import DatabaseAlreadyExists
from swift.common.utils import (json, Timestamp, hash_path,
                                storage_directory, quorum_size)


class ContainerReplicator(db_replicator.Replicator):
    server_type = 'container'
    brokerclass = ContainerBroker
    datadir = DATADIR
    default_port = 6001

    def report_up_to_date(self, full_info):
        reported_key_map = {
            'reported_put_timestamp': 'put_timestamp',
            'reported_delete_timestamp': 'delete_timestamp',
            'reported_bytes_used': 'bytes_used',
            'reported_object_count': 'count',
        }
        for reported, value_key in reported_key_map.items():
            if full_info[reported] != full_info[value_key]:
                return False
        return True

    def _gather_sync_args(self, replication_info):
        parent = super(ContainerReplicator, self)
        sync_args = parent._gather_sync_args(replication_info)
        if len(POLICIES) > 1:
            sync_args += tuple(replication_info[k] for k in
                               ('status_changed_at', 'count',
                                'storage_policy_index'))
        return sync_args

    def _handle_sync_response(self, node, response, info, broker, http):
        parent = super(ContainerReplicator, self)
        if is_success(response.status):
            remote_info = json.loads(response.data)
            if incorrect_policy_index(info, remote_info):
                status_changed_at = Timestamp(time.time())
                broker.set_storage_policy_index(
                    remote_info['storage_policy_index'],
                    timestamp=status_changed_at.internal)
            sync_timestamps = ('created_at', 'put_timestamp',
                               'delete_timestamp')
            if any(info[key] != remote_info[key] for key in sync_timestamps):
                broker.merge_timestamps(*(remote_info[key] for key in
                                          sync_timestamps))
        rv = parent._handle_sync_response(
            node, response, info, broker, http)
        return rv

    def find_local_handoff_for_part(self, part):
        """
        Look through devices in the ring for the first handoff device that was
        identified during job creation as available on this node.

        :returns: a node entry from the ring
        """
        nodes = self.ring.get_part_nodes(part)
        more_nodes = self.ring.get_more_nodes(part)

        for node in itertools.chain(nodes, more_nodes):
            if node['id'] in self._local_device_ids:
                return node
        return None

    def get_reconciler_broker(self, timestamp):
        """
        Get a local instance of the reconciler container broker that is
        appropriate to enqueue the given timestamp.

        :param timestamp: the timestamp of the row to be enqueued

        :returns: a local reconciler broker
        """
        container = get_reconciler_container_name(timestamp)
        if self.reconciler_containers and \
                container in self.reconciler_containers:
            return self.reconciler_containers[container][1]
        account = MISPLACED_OBJECTS_ACCOUNT
        part = self.ring.get_part(account, container)
        node = self.find_local_handoff_for_part(part)
        if not node:
            raise DeviceUnavailable(
                'No mounted devices found suitable to Handoff reconciler '
                'container %s in partition %s' % (container, part))
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, node['device'], db_dir, hsh + '.db')
        broker = ContainerBroker(db_path, account=account, container=container)
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(timestamp, 0)
            except DatabaseAlreadyExists:
                pass
        if self.reconciler_containers is not None:
            self.reconciler_containers[container] = part, broker, node['id']
        return broker

    def feed_reconciler(self, container, item_list):
        """
        Add queue entries for rows in item_list to the local reconciler
        container database.

        :param container: the name of the reconciler container
        :param item_list: the list of rows to enqueue

        :returns: True if successfully enqueued
        """

        try:
            reconciler = self.get_reconciler_broker(container)
        except DeviceUnavailable as e:
            self.logger.warning('DeviceUnavailable: %s', e)
            return False
        self.logger.debug('Adding %d objects to the reconciler at %s',
                          len(item_list), reconciler.db_file)
        try:
            reconciler.merge_items(item_list)
        except (Exception, Timeout):
            self.logger.exception('UNHANDLED EXCEPTION: trying to merge '
                                  '%d items to reconciler container %s',
                                  len(item_list), reconciler.db_file)
            return False
        return True

    def dump_to_reconciler(self, broker, point):
        """
        Look for object rows for objects updates in the wrong storage policy
        in broker with a ``ROWID`` greater than the rowid given as point.

        :param broker: the container broker with misplaced objects
        :param point: the last verified ``reconciler_sync_point``

        :returns: the last successful enqueued rowid
        """
        max_sync = broker.get_max_row()
        misplaced = broker.get_misplaced_since(point, self.per_diff)
        if not misplaced:
            return max_sync
        translator = get_row_to_q_entry_translator(broker)
        errors = False
        low_sync = point
        while misplaced:
            batches = defaultdict(list)
            for item in misplaced:
                container = get_reconciler_container_name(item['created_at'])
                batches[container].append(translator(item))
            for container, item_list in batches.items():
                success = self.feed_reconciler(container, item_list)
                if not success:
                    errors = True
            point = misplaced[-1]['ROWID']
            if not errors:
                low_sync = point
            misplaced = broker.get_misplaced_since(point, self.per_diff)
        return low_sync

    def _post_replicate_hook(self, broker, info, responses):
        if info['account'] == MISPLACED_OBJECTS_ACCOUNT:
            return
        point = broker.get_reconciler_sync()
        if not broker.has_multiple_policies() and info['max_row'] != point:
            broker.update_reconciler_sync(info['max_row'])
            return
        max_sync = self.dump_to_reconciler(broker, point)
        success = responses.count(True) >= quorum_size(len(responses))
        if max_sync > point and success:
            # to be safe, only slide up the sync point with a quorum on
            # replication
            broker.update_reconciler_sync(max_sync)

    def delete_db(self, broker):
        """
        Ensure that reconciler databases are only cleaned up at the end of the
        replication run.
        """
        if (self.reconciler_cleanups is not None and
                broker.account == MISPLACED_OBJECTS_ACCOUNT):
            # this container shouldn't be here, make sure it's cleaned up
            self.reconciler_cleanups[broker.container] = broker
            return
        return super(ContainerReplicator, self).delete_db(broker)

    def replicate_reconcilers(self):
        """
        Ensure any items merged to reconciler containers during replication
        are pushed out to correct nodes and any reconciler containers that do
        not belong on this node are removed.
        """
        self.logger.info('Replicating %d reconciler containers',
                         len(self.reconciler_containers))
        for part, reconciler, node_id in self.reconciler_containers.values():
            self.cpool.spawn_n(
                self._replicate_object, part, reconciler.db_file, node_id)
        self.cpool.waitall()
        # wipe out the cache do disable bypass in delete_db
        cleanups = self.reconciler_cleanups
        self.reconciler_cleanups = self.reconciler_containers = None
        self.logger.info('Cleaning up %d reconciler containers',
                         len(cleanups))
        for reconciler in cleanups.values():
            self.cpool.spawn_n(self.delete_db, reconciler)
        self.cpool.waitall()
        self.logger.info('Finished reconciler replication')

    def run_once(self, *args, **kwargs):
        self.reconciler_containers = {}
        self.reconciler_cleanups = {}
        rv = super(ContainerReplicator, self).run_once(*args, **kwargs)
        if any([self.reconciler_containers, self.reconciler_cleanups]):
            self.replicate_reconcilers()
        return rv


class ContainerReplicatorRpc(db_replicator.ReplicatorRpc):

    def _parse_sync_args(self, args):
        parent = super(ContainerReplicatorRpc, self)
        remote_info = parent._parse_sync_args(args)
        if len(args) > 9:
            remote_info['status_changed_at'] = args[7]
            remote_info['count'] = args[8]
            remote_info['storage_policy_index'] = args[9]
        return remote_info

    def _get_synced_replication_info(self, broker, remote_info):
        """
        Sync the remote_info storage_policy_index if needed and return the
        newly synced replication info.

        :param broker: the database broker
        :param remote_info: the remote replication info

        :returns: local broker replication info
        """
        info = broker.get_replication_info()
        if incorrect_policy_index(info, remote_info):
            status_changed_at = Timestamp(time.time()).internal
            broker.set_storage_policy_index(
                remote_info['storage_policy_index'],
                timestamp=status_changed_at)
            info = broker.get_replication_info()
        return info
