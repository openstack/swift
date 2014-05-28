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

import time

from swift.container.backend import ContainerBroker, DATADIR
from swift.container.reconciler import incorrect_policy_index
from swift.common import db_replicator
from swift.common.utils import json, normalize_timestamp
from swift.common.http import is_success
from swift.common.storage_policy import POLICIES


class ContainerReplicator(db_replicator.Replicator):
    server_type = 'container'
    brokerclass = ContainerBroker
    datadir = DATADIR
    default_port = 6001

    def report_up_to_date(self, full_info):
        for key in ('put_timestamp', 'delete_timestamp', 'object_count',
                    'bytes_used'):
            if full_info['reported_' + key] != full_info[key]:
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
                status_changed_at = normalize_timestamp(time.time())
                broker.set_storage_policy_index(
                    remote_info['storage_policy_index'],
                    timestamp=status_changed_at)
            broker.merge_timestamps(*(remote_info[key] for key in (
                'created_at', 'put_timestamp', 'delete_timestamp')))
        rv = parent._handle_sync_response(
            node, response, info, broker, http)
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
            status_changed_at = normalize_timestamp(time.time())
            broker.set_storage_policy_index(
                remote_info['storage_policy_index'],
                timestamp=status_changed_at)
            info = broker.get_replication_info()
        return info
