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
import json
from collections import defaultdict
from eventlet import Timeout
import optparse
from random import choice

from swift.container.sync_store import ContainerSyncStore
from swift.container.backend import ContainerBroker, DATADIR, SHARDED, \
    merge_shards
from swift.container.reconciler import (
    MISPLACED_OBJECTS_ACCOUNT, incorrect_policy_index,
    get_reconciler_container_name, get_row_to_q_entry_translator)
from swift.common import db_replicator
from swift.common.daemon import run_daemon
from swift.common.storage_policy import POLICIES
from swift.common.swob import HTTPOk, HTTPAccepted
from swift.common.http import is_success
from swift.common.utils import Timestamp, majority_size, get_db_files, \
    parse_options


def check_merge_own_shard_range(shards, broker, logger, source):
    """
    If broker has own_shard_range *with an epoch* then filter out an
    own_shard_range *without an epoch*, and log a warning about it.

    :param shards: a list of candidate ShardRanges to merge
    :param broker: a ContainerBroker
    :param logger: a logger
    :param source: string to log as source of shards
    :return: a list of ShardRanges to actually merge
    """
    # work-around for https://bugs.launchpad.net/swift/+bug/1980451
    own_sr = broker.get_own_shard_range()
    if own_sr.epoch is None:
        return shards
    to_merge = []
    for shard in shards:
        if shard['name'] == own_sr.name and not shard['epoch']:
            shard_copy = dict(shard)
            new_content = merge_shards(shard_copy, dict(own_sr))
            if new_content and shard_copy['epoch'] is None:
                logger.warning(
                    'Ignoring remote osr w/o epoch, own_sr: %r, remote_sr: %r,'
                    ' source: %s', dict(own_sr), shard, source)
            continue
        to_merge.append(shard)
    return to_merge


class ContainerReplicator(db_replicator.Replicator):
    server_type = 'container'
    brokerclass = ContainerBroker
    datadir = DATADIR
    default_port = 6201

    def __init__(self, conf, logger=None):
        super(ContainerReplicator, self).__init__(conf, logger=logger)
        self.reconciler_cleanups = self.sync_store = None

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

    def _handle_sync_response(self, node, response, info, broker, http,
                              different_region=False):
        if is_success(response.status):
            remote_info = json.loads(response.data.decode('ascii'))
            if incorrect_policy_index(info, remote_info):
                status_changed_at = Timestamp.now()
                broker.set_storage_policy_index(
                    remote_info['storage_policy_index'],
                    timestamp=status_changed_at.internal)
            sync_timestamps = ('created_at', 'put_timestamp',
                               'delete_timestamp')
            if any(info[key] != remote_info[key] for key in sync_timestamps):
                broker.merge_timestamps(*(remote_info[key] for key in
                                          sync_timestamps))

            if remote_info.get('shard_max_row', -1) >= 0:
                # Grab remote's shard ranges, too
                self._fetch_and_merge_shard_ranges(http, broker)

        return super(ContainerReplicator, self)._handle_sync_response(
            node, response, info, broker, http, different_region)

    def _sync_shard_ranges(self, broker, http, local_id):
        # TODO: currently the number of shard ranges is expected to be _much_
        # less than normal objects so all are sync'd on each cycle. However, in
        # future there should be sync points maintained much like for object
        # syncing so that only new shard range rows are sync'd.
        shard_range_data = broker.get_all_shard_range_data()
        if shard_range_data:
            if not self._send_replicate_request(
                    http, 'merge_shard_ranges', shard_range_data, local_id):
                return False
            self.logger.debug('%s synced %s shard ranges to %s',
                              broker.db_file, len(shard_range_data),
                              '%(ip)s:%(port)s/%(device)s' % http.node)
        return True

    def _choose_replication_mode(self, node, rinfo, info, local_sync, broker,
                                 http, different_region):
        if 'shard_max_row' in rinfo:
            # Always replicate shard ranges to new-enough swift
            shard_range_success = self._sync_shard_ranges(
                broker, http, info['id'])
        else:
            shard_range_success = False
            self.logger.warning(
                '%s is unable to replicate shard ranges to peer %s; '
                'peer may need upgrading', broker.db_file,
                '%(ip)s:%(port)s/%(device)s' % node)
        if broker.sharding_initiated():
            if info['db_state'] == SHARDED and len(
                    broker.get_objects(limit=1)) == 0:
                self.logger.debug('%s is sharded and has nothing more to '
                                  'replicate to peer %s',
                                  broker.db_file,
                                  '%(ip)s:%(port)s/%(device)s' % node)
            else:
                # Only print the scary warning if there was something that
                # didn't get replicated
                self.logger.warning(
                    '%s is able to shard -- refusing to replicate objects to '
                    'peer %s; have shard ranges and will wait for cleaving',
                    broker.db_file,
                    '%(ip)s:%(port)s/%(device)s' % node)
            self.stats['deferred'] += 1
            return shard_range_success

        success = super(ContainerReplicator, self)._choose_replication_mode(
            node, rinfo, info, local_sync, broker, http,
            different_region)
        return shard_range_success and success

    def _fetch_and_merge_shard_ranges(self, http, broker):
        with Timeout(self.node_timeout):
            response = http.replicate('get_shard_ranges')
        if response and is_success(response.status):
            shards = json.loads(response.data.decode('ascii'))
            shards = check_merge_own_shard_range(
                shards, broker, self.logger, '%s%s' % (http.host, http.path))
            broker.merge_shard_ranges(shards)

    def find_local_handoff_for_part(self, part):
        """
        Find a device in the ring that is on this node on which to place a
        partition. Preference is given to a device that is a primary location
        for the partition. If no such device is found then a local device with
        weight is chosen, and failing that any local device.

        :param part: a partition
        :returns: a node entry from the ring
        """
        if not self._local_device_ids:
            raise RuntimeError('Cannot find local handoff; no local devices')

        for node in self.ring.get_part_nodes(part):
            if node['id'] in self._local_device_ids:
                return node

        # don't attempt to minimize handoff depth: just choose any local
        # device, but start by only picking a device with a weight, just in
        # case some devices are being drained...
        local_devs_with_weight = [
            dev for dev in self._local_device_ids.values()
            if dev.get('weight', 0)]
        if local_devs_with_weight:
            return choice(local_devs_with_weight)

        # we have to return something, so choose any local device..
        node = choice(list(self._local_device_ids.values()))
        self.logger.warning(
            "Could not find a non-zero weight device for handoff partition "
            "%d, falling back device %s" %
            (part, node['device']))
        return node

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
        broker, initialized = ContainerBroker.create_broker(
            os.path.join(self.root, node['device']), part, account, container,
            logger=self.logger, put_timestamp=timestamp,
            storage_policy_index=0)
        self.logger.increment('reconciler_db_created' if initialized
                              else 'reconciler_db_exists')
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
        except Exception:
            self.logger.exception('Failed to get reconciler broker for '
                                  'container %s', container)
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

        try:
            self.sync_store.update_sync_store(broker)
        except Exception:
            self.logger.exception('Failed to update sync_store %s' %
                                  broker.db_file)

        point = broker.get_reconciler_sync()
        if not broker.has_multiple_policies() and info['max_row'] != point:
            broker.update_reconciler_sync(info['max_row'])
            return
        max_sync = self.dump_to_reconciler(broker, point)
        success = responses.count(True) >= majority_size(len(responses))
        if max_sync > point and success:
            # to be safe, only slide up the sync point with a majority on
            # replication
            broker.update_reconciler_sync(max_sync)

    def cleanup_post_replicate(self, broker, orig_info, responses):
        if broker.sharding_required():
            # despite being a handoff, since we're sharding we're not going to
            # do any cleanup so we can continue cleaving - this is still
            # considered "success"
            self.logger.debug(
                'Not deleting db %s (requires sharding, state %s)',
                broker.db_file, broker.get_db_state())
            return True
        return super(ContainerReplicator, self).cleanup_post_replicate(
            broker, orig_info, responses)

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
        if self.sync_store:
            try:
                # DB is going to get deleted. Be preemptive about it
                self.sync_store.remove_synced_container(broker)
            except Exception:
                self.logger.exception('Failed to remove sync_store entry %s' %
                                      broker.db_file)

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
        self.sync_store = ContainerSyncStore(self.root,
                                             self.logger,
                                             self.mount_check)
        rv = super(ContainerReplicator, self).run_once(*args, **kwargs)
        if any([self.reconciler_containers, self.reconciler_cleanups]):
            self.replicate_reconcilers()
        return rv


class ContainerReplicatorRpc(db_replicator.ReplicatorRpc):

    def _db_file_exists(self, db_path):
        return bool(get_db_files(db_path))

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
            status_changed_at = Timestamp.now().internal
            broker.set_storage_policy_index(
                remote_info['storage_policy_index'],
                timestamp=status_changed_at)
            info = broker.get_replication_info()
        return info

    def _abort_rsync_then_merge(self, db_file, old_filename):
        if super(ContainerReplicatorRpc, self)._abort_rsync_then_merge(
                db_file, old_filename):
            return True
        # if the local db has started sharding since the original 'sync'
        # request then abort object replication now; instantiate a fresh broker
        # each time this check if performed so to get latest state
        broker = ContainerBroker(db_file, logger=self.logger)
        return broker.sharding_initiated()

    def _post_rsync_then_merge_hook(self, existing_broker, new_broker):
        # Note the following hook will need to change to using a pointer and
        # limit in the future.
        shards = existing_broker.get_all_shard_range_data()
        shards = check_merge_own_shard_range(
            shards, new_broker, self.logger, 'rsync')
        new_broker.merge_shard_ranges(shards)

    def merge_shard_ranges(self, broker, args):
        shards = check_merge_own_shard_range(
            args[0], broker, self.logger, 'repl_req')
        broker.merge_shard_ranges(shards)
        return HTTPAccepted()

    def get_shard_ranges(self, broker, args):
        return HTTPOk(headers={'Content-Type': 'application/json'},
                      body=json.dumps(broker.get_all_shard_range_data()))


def main():
    parser = optparse.OptionParser("%prog CONFIG [options]")
    parser.add_option('-d', '--devices',
                      help=('Replicate only given devices. '
                            'Comma-separated list. '
                            'Only has effect if --once is used.'))
    parser.add_option('-p', '--partitions',
                      help=('Replicate only given partitions. '
                            'Comma-separated list. '
                            'Only has effect if --once is used.'))
    conf_file, options = parse_options(parser=parser, once=True)
    run_daemon(ContainerReplicator, conf_file, **options)


if __name__ == '__main__':
    main()
