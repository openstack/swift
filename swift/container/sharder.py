# Copyright (c) 2015 OpenStack Foundation
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

import errno
import json
import os
import time

from collections import defaultdict

from random import random
from hashlib import md5

from eventlet import Timeout

from swift.container.replicator import ContainerReplicator
from swift.container.backend import ContainerBroker, DATADIR, \
    RECORD_TYPE_SHARD_NODE, RECORD_TYPE_OBJECT, DB_STATE_NOTFOUND, \
    DB_STATE_UNSHARDED, DB_STATE_SHARDING, DB_STATE_SHARDED
from swift.common import internal_client, db_replicator
from swift.common.bufferedhttp import http_connect
from swift.common.db import DatabaseAlreadyExists
from swift.common.exceptions import DeviceUnavailable, ConnectionTimeout, \
    RangeAnalyserException
from swift.common.http import is_success
from swift.common.constraints import CONTAINER_LISTING_LIMIT
from swift.common.ring.utils import is_local_device
from swift.common.utils import get_logger, config_true_value, \
    dump_recon_cache, whataremyips, hash_path, \
    storage_directory, Timestamp, ShardRange, \
    find_shard_range, ismount, majority_size, GreenAsyncPile, \
    account_to_shard_account, config_float_value, config_positive_int_value
from swift.common.storage_policy import POLICIES


PR_NAME = 0
PR_CREATED_AT = 1
PR_LOWER = 2
PR_UPPER = 3
PR_OBJECT_COUNT = 4
PR_BYTES_USED = 5


class ContainerSharder(ContainerReplicator):
    """Shards containers."""

    def __init__(self, conf, logger=None):
        logger = logger or get_logger(conf, log_route='container-sharder')
        super(ContainerReplicator, self).__init__(conf, logger=logger)
        self.rcache = os.path.join(self.recon_cache_path,
                                   "container-sharder.recon")
        try:
            self.shard_shrink_point = config_float_value(
                conf.get('shard_shrink_point', 25), 0, 100) / 100.0
        except ValueError as err:
            raise ValueError(err.message + ": shard_shrink_point")
        try:
            self.shrink_merge_point = config_float_value(
                conf.get('shard_shrink_merge_point', 75), 0, 100) / 100.0
        except ValueError as err:
            raise ValueError(err.message + ": shard_shrink_merge_point")
        self.shard_container_size = config_positive_int_value(
            conf.get('shard_container_size', 10000000))
        self.split_size = self.shard_container_size // 2
        self.cpool = GreenAsyncPile(self.cpool)
        self.scanner_batch_size = config_positive_int_value(
            conf.get('shard_scanner_batch_size', 10))
        self.shard_batch_size = config_positive_int_value(
            conf.get('shard_batch_size', 2))
        self.reported = None

        # internal client
        self.conn_timeout = float(conf.get('conn_timeout', 5))
        request_tries = config_positive_int_value(
            conf.get('request_tries', 3))
        internal_client_conf_path = conf.get('internal_client_conf_path',
                                             '/etc/swift/internal-client.conf')
        try:
            self.swift = internal_client.InternalClient(
                internal_client_conf_path,
                'Swift Container Sharder',
                request_tries,
                allow_modify_pipeline=False)
        except IOError as err:
            # TODO: if sharder functions are moved into replicator then for
            # backwards compatibility we need this to simply log a warning that
            # sharding will be skipped due to missing internal client conf
            if err.errno != errno.ENOENT:
                raise
            raise SystemExit(
                'Unable to load internal client from config: %r (%s)' %
                (internal_client_conf_path, err))

    def _zero_stats(self):
        """Zero out the stats."""
        # TODO check that none of the replicator stats are useful
        super(ContainerSharder, self)._zero_stats()
        self.stats.update({
            'containers_scanned': 0,
            'containers_sharded': 0,
            'containers_shrunk': 0,
            'container_shard_ranges': 0,
            'containers_misplaced': 0,
            'containers_audit_failed': 0,
            'containers_failed': 0,
        })

    def _report_stats(self):
        stats = self.stats
        stats['since'] = time.ctime(self.reported)
        self.logger.info(
            'Since %(since)s Stats: %(containers_scanned)s scanned, '
            '%(containers_sharded)s sharded, '
            '%(containers_shrunk)s shrunk, '
            '%(container_shard_ranges)s shard ranges found, '
            '%(containers_misplaced)s contained misplaced items, '
            '%(containers_audit_failed)s failed audit, '
            '%(containers_failed)s containers failed.',
            stats)
        dump_recon_cache(stats, self.rcache, self.logger)
        self._zero_stats()
        self.reported = time.time()

    def _periodic_report_stats(self):
        if (time.time() - self.reported) >= 3600:  # once an hour
            return self._report_stats()

    def _get_local_devices(self):
        self._local_device_ids = set()
        results = set()
        self.ips = whataremyips()
        if not self.ips:
            self.logger.error('ERROR Failed to get my own IPs?')
            return
        for node in self.ring.devs:
            if node and is_local_device(self.ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                results.add(node['device'])
                self._local_device_ids.add(node['id'])
        return results

    def _get_shard_ranges(self, account, container, newest=False):
        # TODO: this surely doesn't work -- self.swift is an internal client,
        # and the proxy's container controller doesn't clients get pivots
        path = self.swift.make_path(account, container) + \
            '?items=shard&format=json'
        headers = dict()
        if newest:
            headers['X-Newest'] = 'true'
        try:
            resp = self.swift.make_request('GET', path, headers,
                                           acceptable_statuses=(2,))
        except internal_client.UnexpectedResponse:
            self.logger.error("Failed to get shard ranges from %s/%s",
                              account, container)
            return None

        ranges = list()
        try:
            for shard_range in json.loads(resp.body):
                lower = shard_range.get('lower') or None
                upper = shard_range.get('upper') or None
                created_at = shard_range.get('created_at') or None
                object_count = shard_range.get('object_count') or 0
                bytes_used = shard_range.get('bytes_used') or 0
                meta_timestamp = shard_range.get('meta_timestamp') or None
                ranges.append(ShardRange(shard_range['name'], created_at,
                                         lower, upper, object_count,
                                         bytes_used, meta_timestamp))
        except ValueError:
            # Failed to decode the json response
            return None
        return ranges

    def _get_shard_broker(self, account, container, policy_index):
        """
        Get a local instance of the shard container broker that will be
        pushed out.

        :param account: the account
        :param container: the container
        :returns: a local shard container broker
        """
        part = self.ring.get_part(account, container)
        node = self.find_local_handoff_for_part(part)
        if not node:
            raise DeviceUnavailable(
                'No mounted devices found suitable to Handoff sharded '
                'container %s in partition %s' % (container, part))
        hsh = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, hsh)
        db_path = os.path.join(self.root, node['device'], db_dir, hsh + '.db')
        broker = ContainerBroker(db_path, account=account, container=container,
                                 logger=self.logger)
        if not os.path.exists(broker.db_file):
            try:
                broker.initialize(storage_policy_index=policy_index)
            except DatabaseAlreadyExists:
                pass

        # Get the valid info into the broker.container, etc
        broker.get_info()
        return part, broker, node['id']

    def _generate_object_list(self, items, policy_index, delete=False):
        """
        Create a list of dictionary items ready to be consumed by
        Broker.merge_items()

        :param items: list of objects or pivots
        :param policy_index: the Policy index of the container
        :param delete: mark the objects as deleted; default False

        :return: A list of item dictionaries ready to be consumed by
                 merge_items.
        """
        objs = list()
        for item in items:
            try:
                if isinstance(item, ShardRange):
                    item = (
                        item.name, item.timestamp.internal, item.lower,
                        item.upper, item.object_count, item.bytes_used,
                        item.meta_timestamp.internal)
                if delete:
                    # Generate a new delete timestamp based off the existing
                    # created_at, this way we don't clobber other objects that
                    # may exist out there. The idea is, newer object out there
                    # will replace the deleted record, which will be picked up
                    # as a misplaced object and then be pushed and compared to
                    # where it needs to be.
                    created_at = Timestamp(item[1], offset=1).internal
                else:
                    created_at = item[1]
                if not isinstance(item[2], int):
                    # shard node
                    obj = {
                        'name': item[0],
                        'created_at': created_at,
                        'lower': item[2],
                        'upper': item[3],
                        'object_count': item[4],
                        'bytes_used': item[5],
                        'meta_timestamp': item[6],
                        'deleted': 1 if delete else 0,
                        'storage_policy_index': 0,
                        'record_type': RECORD_TYPE_SHARD_NODE}
                else:
                    # object item
                    obj = {
                        'name': item[0],
                        'created_at': created_at,
                        'size': item[2],
                        'content_type': item[3],
                        'etag': item[4],
                        'deleted': 1 if delete else 0,
                        'storage_policy_index': policy_index,
                        'record_type': RECORD_TYPE_OBJECT}
            except Exception:
                # TODO: narrow exception type
                # TODO: would this be a bug? is just logging appropriate?
                self.logger.warning("Failed to add object %s, not in the"
                                    'right format',
                                    item[0] if item[0] else str(item))
            else:
                objs.append(obj)
        return objs

    def _get_node_index(self):
        # TODO: stop having node_id hang off self
        if not hasattr(self, 'node_id'):
            return None

        nodes = self.ring.get_part_nodes(self.part)
        indices = [node['index'] for node in nodes
                   if node['id'] == self.node_id]
        return indices[0] if indices else None

    def _add_shard_metadata(self, broker, root_account, root_container,
                            shard_range, force=False):
        if not broker.metadata.get('X-Container-Sysmeta-Shard-Account') \
                and shard_range or force:
            timestamp = Timestamp.now().internal
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Account': (root_account, timestamp),
                'X-Container-Sysmeta-Shard-Container':
                    (root_container, timestamp),
                'X-Container-Sysmeta-Shard-Lower':
                    (shard_range.lower, timestamp),
                'X-Container-Sysmeta-Shard-Upper':
                    (shard_range.upper, timestamp),
                'X-Container-Sysmeta-Shard-Timestamp':
                    (shard_range.timestamp.internal, timestamp),
                'X-Container-Sysmeta-Shard-Meta-Timestamp':
                    (shard_range.meta_timestamp.internal, timestamp),
                'X-Container-Sysmeta-Sharding': (None, timestamp)})

    def _misplaced_objects(self, broker, root_account, root_container,
                           shard_range):
        """
        Search for objects in the current broker that don't belong, and move
        to the container shards they do.

        :param broker: The parent broker to update once misplaced objects have
            been moved.
        :param root_account: The root account
        :param root_container: The root container
        :param shard_range: The ShardRange appropriate for this broker, or None
            if the current broker is for an unsharded or root container
        """

        self.logger.info('Scanning %s/%s for misplaced objects',
                         broker.account, broker.container)
        queries = []
        policy_index = broker.storage_policy_index
        query = dict(marker='', end_marker='', prefix='', delimiter='',
                     storage_policy_index=policy_index)
        # TODO: what about records for objects in the wrong storage policy?
        state = broker.get_db_state()

        if state == DB_STATE_SHARDED or broker.is_deleted():
            # It's a sharded node or deleted, so anything in the object table
            # is treated as a misplaced object.
            if broker.get_info()['object_count'] > 0:
                queries.append(query.copy())
            else:
                return
        elif state == DB_STATE_SHARDING:
            # This state is a little more complicated. Only objects in the
            # object table that are less than (<) the shard range this node is
            # up to are considered misplaced, anything above is being held.
            last_shard = self.get_metadata_item(
                broker, 'X-Container-Sysmeta-Shard-Last-%d' % self.node_id)
            if not last_shard:
                # This node hasn't exploded/cleaved anything yet, so all
                # objects in object table are suppose to be there (in holding).
                return
            tmp_q = query.copy()
            tmp_q['end_marker'] = last_shard + '\x00'
            queries.append(tmp_q)

        elif shard_range is None or state == DB_STATE_NOTFOUND:
            # This is an unsharded root container, so we don't need to
            # query anything.
            return
        else:
            # it hasn't been sharded and isn't the root container, so we need
            # to look for objects that shouldn't be in the object table.
            if shard_range.upper:
                tmp_q = query.copy()
                tmp_q['marker'] = shard_range.upper
                queries.append(tmp_q)
            if shard_range.lower:
                tmp_q = query.copy()
                tmp_q['end_marker'] = shard_range.lower + '\x00'
                queries.append(tmp_q)

        def run_query(qry, found_misplaced_items):
            objs = broker.list_objects_iter(CONTAINER_LISTING_LIMIT, **qry)
            if not objs:
                return found_misplaced_items

            # We have a list of misplaced objects, so we better find a home
            # for them
            # TODO - ranges was an instance var - is that significant e.g. for
            # caching
            ranges = self._get_shard_ranges(
                root_account, root_container, newest=True)

            shard_to_obj = defaultdict(list)
            for obj in objs:
                shard = find_shard_range(obj, ranges)
                shard_to_obj[shard].append(obj)
                qry['marker'] = obj[0]

            self.logger.info('preparing to move %d misplaced objects found '
                             'in %s/%s', len(objs), broker.account,
                             broker.container)
            for shard, obj_list in shard_to_obj.items():
                acct = account_to_shard_account(root_account)
                part, new_broker, node_id = \
                    self._get_shard_broker(acct, shard.name, policy_index)

                self._add_shard_metadata(new_broker, root_account,
                                         root_container, shard)

                objects = self._generate_object_list(obj_list, policy_index)
                new_broker.merge_items(objects)

                self.cpool.spawn(
                    self._replicate_object, part, new_broker.db_file, node_id)

                # Remove the now relocated misplaced items.
                items = self._generate_object_list(obj_list, policy_index,
                                                   delete=True)
                broker.merge_items(items)
            # wait for one of these to error, or all to complete successfully
            any(self.cpool)

            # There could be more, so recurse my pretty
            # TODO: can we restore condition?
            # if len(objs) == CONTAINER_LISTING_LIMIT:
            return run_query(qry, True)

        misplaced_items = False
        for query in queries:
            misplaced_items = run_query(query, misplaced_items)

        if misplaced_items:
            self.logger.increment('misplaced_items_found')
            self.stats['containers_misplaced'] += 1

        # wipe out the cache to disable bypass in delete_db
        cleanups = self.shard_cleanups or {}
        self.shard_cleanups = None
        self.logger.info('Cleaning up %d replicated shard containers',
                         len(cleanups))
        for container in cleanups.values():
            self.cpool.spawn(self.delete_db, container)
        any(self.cpool)
        self.logger.info('Finished misplaced shard replication')

    def _post_replicate_hook(self, broker, info, responses):
        return

    def delete_db(self, broker):
        """
        Ensure that replicated sharded databases are only cleaned up at the end
        of the replication run.
        """
        if self.shard_cleanups is not None:
            # this container shouldn't be here, make sure it's cleaned up
            self.shard_cleanups[broker.container] = broker
            return
        return super(ContainerReplicator, self).delete_db(broker)

    def _audit_shard_container(self, broker, shard_range, root_account=None,
                               root_container=None):
        # TODO We will need to audit the root (make sure there are no missing
        #      gaps in the ranges.
        # TODO If the shard container has a sharding lock then see if it's
        #       needed. Maybe something as simple as sharding lock older then
        #       reclaim age.

        self.logger.info('Auditing %s/%s', broker.account, broker.container)
        continue_with_container = True

        # if the container has been marked as deleted, all metadata will
        # have been erased so no point auditing. But we want it to pass, in
        # case any objects exist inside it.
        if broker.is_deleted():
            return continue_with_container

        if not root_account or not root_container:
            root_account, root_container = broker.get_shard_root_path()

        if root_container == broker.container:
            # This is the root container, and therefore the tome of knowledge,
            # all we can do is check there is nothing screwy with the range
            ranges = broker.build_shard_ranges()
            overlaps = ContainerSharder.find_overlapping_ranges(ranges)
            for overlap in overlaps:
                self.logger.error('Range overlaps found, attempting to '
                                  'correct')
                newest = max(overlap, key=lambda x: x.timestamp)
                older = set(overlap).difference(set([newest]))

                # now delete the older overlaps, keeping only the newest
                timestamp = Timestamp(newest.timestamp, offset=1)
                for range in older:
                    range.timestamp = timestamp
                self._update_shard_ranges(root_account, root_container,
                                          'DELETE', older)
                continue_with_container = False
            missing_ranges = ContainerSharder.check_complete_ranges(ranges)
            if missing_ranges:
                self.logger.error('Missing range(s) dectected: %s',
                                  '-'.join(missing_ranges))
                continue_with_container = False

            if not continue_with_container:
                self.logger.increment('audit_failed')
                self.stats['containers_failed'] += 1
                self.stats['containers_audit_failed'] += 1
            return continue_with_container

        # Get the root view of the world.
        ranges = self._get_shard_ranges(root_account, root_container,
                                        newest=True)
        if ranges is None:
            # failed to get the root tree. Error out for now.. we may need to
            # quarantine the container.
            self.logger.warning("Failed to get a shard range tree from root "
                                "container %s/%s, it may not exist.",
                                root_account, root_container)
            self.logger.increment('audit_failed')
            self.stats['containers_failed'] += 1
            self.stats['containers_audit_failed'] += 1
            return False
        if shard_range in ranges:
            return continue_with_container

        # shard range isn't in ranges, if it overlaps with an item, we're in
        # trouble. If there is overlap let's see if it's newer than this
        # container, if so, it's safe to delete (quarantine this container).
        # TODO(tburke): is ^^^ right? or better to consider it all misplaced?
        # if it's newer, then it might not be updated yet, so just let it
        # continue (or maybe we shouldn't?).
        overlaps = [r for r in ranges if r.overlaps(shard_range)]
        if overlaps:
            if max(overlaps + [shard_range],
                   key=lambda x: x.timestamp) == shard_range:
                # shard range is newest so leave it alone for now  as the root
                # might not be updated  yet.
                self.logger.increment('audit_failed')
                self.stats['containers_failed'] += 1
                self.stats['containers_audit_failed'] += 1
                return False
            else:
                # There is a newer range that overlaps/covers this range.
                # so we are safe to quarantine it.
                # TODO Quarantine
                self.logger.error('The range of objects stored in this '
                                  'container (%s/%s) overlaps with another '
                                  'newer shard range',
                                  broker.account, broker.container)
                self.logger.increment('audit_failed')
                self.stats['containers_failed'] += 1
                self.stats['containers_audit_failed'] += 1
                return False
        # shard range doesn't exist in the root containers ranges, but doesn't
        # overlap with anything
        return continue_with_container

    def _update_shard_range_counts(self, root_account, root_container, broker):
        if broker.container == root_container:
            return
        shard_range = broker.get_shard_range()
        if not shard_range:
            return
        self._update_shard_ranges(root_account, root_container, 'PUT',
                                  [shard_range])

    @staticmethod
    def get_metadata_item(broker, header):
        # TODO: Since every broker.metadata is a query, this may not be
        # the best helper...
        item = broker.metadata.get(header)
        return None if item is None else item[0]

    def roundrobin_datadirs(self, datadirs):
        for part, path, node_id in db_replicator.roundrobin_datadirs(datadirs):
            index = [node['index'] for node in
                     self.ring.get_part_nodes(int(part))
                     if node['id'] == node_id]
            if not index:
                # TODO Should probably log something here
                continue
            yield part, path, node_id, index[0]

    def _one_shard_cycle(self):
        """
        The main function, everything the sharder does forks from this method.

        The sharder loops through each container with sharding enabled and each
        sharded container on the server, on each container it:
            - audits the container
            - checks and deals with misplaced items
            - 2 phase sharding (if no. objects > shard_container_size)
                - Phase 1, if there is no shard defined, find it, then move
                  to next container.
                - Phase 2, if there is a shard defined, shard it.
            - Shrinking (check to see if we need to shrink this container).
        """
        self.logger.info('Starting container sharding cycle')
        dirs = []
        self.shard_cleanups = dict()
        self.ips = whataremyips()
        for node in self.ring.devs:
            if node and is_local_device(self.ips, self.port,
                                        node['replication_ip'],
                                        node['replication_port']):
                if self.mount_check and not ismount(
                        os.path.join(self.root, node['device'])):
                    self.logger.warn(
                        'Skipping %(device)s as it is not mounted' % node)
                    continue
                datadir = os.path.join(self.root, node['device'], self.datadir)
                if os.path.isdir(datadir):
                    # TODO: do we need self._local_device_ids?
                    self._local_device_ids.add(node['id'])
                    dirs.append((datadir, node['id']))
        for part, path, node_id, node_idx in self.roundrobin_datadirs(dirs):
            broker = ContainerBroker(path)
            sharded = broker.metadata.get('X-Container-Sysmeta-Sharding') or \
                broker.metadata.get('X-Container-Sysmeta-Shard-Account')
            if not sharded:
                # Not a shard container
                continue
            self.node_idx = node_idx
            self.node_id = node_id
            self.part = int(part)
            root_account, root_container = broker.get_shard_root_path()
            shard_range = broker.get_shard_range()

            # Before we do any heavy lifting, lets do an audit on the shard
            # container. We grab the root's view of the shard_points and make
            # sure this container exists in it and in what should be it's
            # parent. If its in both great, If it exists in either but not the
            # other, then this needs to be fixed. If, however, it doesn't
            # exist in either then this container may not exist anymore so
            # quarantine it.
            # if not self._audit_shard_container(broker, shard_range,
            #                                    root_account,
            #                                    root_container):
            #     continue

            # now look and deal with misplaced objects.
            self._misplaced_objects(broker, root_account, root_container,
                                    shard_range)

            if broker.is_deleted():
                # This container is deleted so we can skip it. We still want
                # deleted containers to go via misplaced items, cause they may
                # have new objects sitting in them that may need to move.
                continue

            state = broker.get_db_state()
            if state in (DB_STATE_SHARDED, DB_STATE_NOTFOUND):
                continue

            self.shard_cleanups = dict()

            try:
                if broker.is_shrinking():
                    # No matter what, we want to finish this shrink stage
                    # before anything else.
                    self._shrink(broker, root_account, root_container)
                    continue

                # The sharding mechanism is rather interesting. If the
                # container requires sharding.. that is big enough and
                # sharding hasn't started yet. Then we need to identify a
                # primary node to scan for shards. The others will wait for
                # shards to appear in the shard_ranges table
                # (via replication). They will start the sharding process

                if state == DB_STATE_UNSHARDED:
                    skip_shrinking = self.get_metadata_item(
                        broker, 'X-Container-Sysmeta-Sharding')
                    obj_count = broker.get_info()['object_count']
                    if not skip_shrinking and obj_count <= \
                            (self.shard_container_size *
                             self.shard_shrink_point):
                        # TODO: Shrink
                        # self._shrink(broker, root_account, root_container)
                        continue
                    elif obj_count <= self.shard_container_size:
                        continue

                # We are either in the sharding state or we need to start
                # sharding. So find out if this is suppose to be the scanning
                # node, if defined
                scan_idx = self.get_metadata_item(
                    broker, 'X-Container-Sysmeta-Shard-Scanner')
                scan_complete = config_true_value(self.get_metadata_item(
                    broker, 'X-Container-Sysmeta-Sharding-Scan-Done'))

                if scan_idx is None and not scan_complete:
                    try:
                        scan_idx = self._find_scanner_node(broker)
                    except Exception:
                        # todo log and continue
                        self.stats['containers_failed'] += 1
                        continue

                if scan_idx is not None and int(scan_idx) == self.node_idx \
                        and not scan_complete:
                    scan_complete = self._find_shard_ranges(broker)
                    if scan_complete:
                        self._cleave(broker, root_account,
                                     root_container)
                else:
                    self._cleave(broker, root_account,
                                 root_container)
            finally:
                self._update_shard_range_counts(root_account, root_container,
                                                broker)
                self.logger.increment('scanned')
                self.stats['containers_scanned'] += 1
                self._periodic_report_stats()

        # wipe out the cache do disable bypass in delete_db
        cleanups = self.shard_cleanups
        self.shard_cleanups = None
        if cleanups:
            self.logger.info('Cleaning up %d replicated shard containers',
                             len(cleanups))

            for container in cleanups.values():
                self.cpool.spawn(self.delete_db, container)

        # Now we wait for all threads to finish.
        # TODO: wait, we used any() above... which is right?
        all(self.cpool)
        self.logger.info('Finished container sharding pass')

    def _send_request(self, ip, port, contdevice, partition, op, path,
                      headers_out=None, node_idx=None):
        headers_out = {} if headers_out is None else headers_out
        if 'user-agent' not in headers_out:
            headers_out['user-agent'] = 'container-sharder %s' % \
                                        os.getpid()
        if 'X-Timestamp' not in headers_out:
            headers_out['X-Timestamp'] = Timestamp(time.time()).normal
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(ip, port, contdevice, partition,
                                    op, path, headers_out)
            with Timeout(self.node_timeout):
                response = conn.getresponse()
                return response, node_idx
        except (Exception, Timeout) as x:
            self.logger.info(str(x))
            # Need to do something here.
            return None, node_idx

    def _update_shard_ranges(self, account, container, op, shard_ranges):
        path = "/%s/%s" % (account, container)
        part, nodes = self.ring.get_nodes(account, container)

        for shard_range in shard_ranges:
            obj = shard_range.name
            obj_path = '%s/%s' % (path, obj)
            headers = {
                'x-backend-record-type': RECORD_TYPE_SHARD_NODE,
                'x-backend-shard-objects': shard_range.object_count,
                'x-backend-shard-bytes': shard_range.bytes_used,
                'x-backend-shard-lower': shard_range.lower,
                'x-backend-shard-upper': shard_range.upper,
                'x-backend-timestamp': shard_range.timestamp,
                'x-meta-timestamp': shard_range.meta_timestamp,
                'x-size': 0}

            for node in nodes:
                self.cpool.spawn(
                    self._send_request, node['ip'], node['port'],
                    node['device'], part, op, obj_path, headers, node['index'])
            all(self.cpool)

    @staticmethod
    def check_complete_ranges(ranges):
        lower = set()
        upper = set()
        for r in ranges:
            lower.add(r.lower)
            upper.add(r.upper)
        l = lower.copy()
        lower.difference_update(upper)
        upper.difference_update(l)

        return zip(upper, lower)

    @staticmethod
    def find_overlapping_ranges(ranges):
        result = set()
        for range in ranges:
            res = [r for r in ranges if range != r and range.overlaps(r)]
            if res:
                res.append(range)
                res.sort()
                result.add(tuple(res))

        return result

    def _find_scanner_node(self, broker):
        self.logger.info('Started searching for best node to be shard point '
                         'scanner for %s/%s',
                         broker.account, broker.container)
        obj_count = [broker.get_info()['object_count']]
        scanner_id = [self._get_node_index()]
        scanner_ids = {}

        def on_success(resp, node_idx):
            if not resp or not is_success(resp.status):
                return False

            found_scan_id = resp.getheader('X-Container-Sysmeta-Shard-Scanner')
            found_obj_count = int(resp.getheader('X-Container-Object-Count'))

            if found_scan_id:
                if scanner_ids.get(found_scan_id):
                    scanner_ids[found_scan_id] += 1
                else:
                    scanner_ids[found_scan_id] = 1

            if found_obj_count > obj_count[0]:
                obj_count[0] = found_obj_count
                scanner_id[0] = node_idx

            return True

        if not self._get_quorum(broker, success=on_success):
            self.logger.info('Failed to reach quorum on a shard scanner for '
                             '%s/%s', broker.account, broker.container)
            return

        # We have a quorum of responses but if there is already a node out
        # there with a scan_id set, then this means that there was a quorum
        # previously, and most probably this node failed the POSTing of the
        # metadata OR the POSTing of the metadata failed to get quorum.
        # In either case, we'll just use it, as replication should have/will
        # move the scan_id around anyway. And a scanner will always ask for
        # quorum before adding an item to shard_ranges, so all should be good.
        if scanner_ids:
            if len(scanner_ids) > 1:
                # find the most occurring or if a tie wait for replication to
                # choose the best
                max_id = max(scanner_ids.items(), key=lambda x: x[1])

                # We need to make sure there isn't a tie, if there is we want
                # to bail and let replication decide
                max_ids = filter(lambda x: x == max_id[1], scanner_ids)
                if len(max_ids) > 1:
                    self.logger.warn("Cannot find a scanner node, too many "
                                     "potential scanners nodes (%s). "
                                     "Leaving for replication to "
                                     "determine.",
                                     ",".join([str(i) for i, c in max_ids]))
                    return
                scanner_id[0] = max_id[0]
            else:
                scanner_id[0] = scanner_ids.keys()[0]

        # Found a node to be the scanner
        headers = {'X-Container-Sysmeta-Shard-Scanner': scanner_id[0]}

        timestamp = Timestamp(time.time()).internal
        broker.update_metadata({
            'X-Container-Sysmeta-Shard-Scanner': (scanner_id[0], timestamp)})

        if not self._get_quorum(broker, op='POST', headers=headers):
            self.logger.info('Failed to set node %d as the shard scanner '
                             'for %s/%s on remote servers',
                             scanner_id, broker.account, broker.container)
            return

        self.logger.info('Best shard scanner for %s/%s is node %d',
                         broker.account, broker.container, scanner_id[0])
        self.logger.increment('scanner_searches')

        # return scanner_id[0]
        return self.get_metadata_item(broker,
                                      'X-Container-Sysmeta-Shard-Scanner')

    def _get_quorum(self, broker, success=None, quorum=None, op='HEAD',
                    headers=None, post_success=None, post_fail=None,
                    account=None, container=None):
        quorum = quorum if quorum else (majority_size(self.ring.replica_count))
        local = False

        if broker:
            local = True
            account = broker.account
            container = broker.container

        if not headers:
            headers = {}

        def default_success(resp, node_idx):
            return resp and is_success(resp.status)

        if not success:
            success = default_success

        path = "/%s/%s" % (account, container)
        part, nodes = self.ring.get_nodes(account, container)
        if local:
            nodes = [
                d for d in nodes if not is_local_device(
                    self.ips, self.port, d['ip'], d['port'])]

        for node in nodes:
            self.cpool.spawn(
                self._send_request, node['ip'], node['port'], node['device'],
                part, op, path, headers, node_idx=node['index'])

        successes = 1 if local else 0
        for resp, node_idx in self.cpool:
            if not resp:
                continue
            if success(resp, node_idx):
                successes += 1
                if post_success:
                    post_success(resp, node_idx)
            else:
                if post_fail:
                    post_fail(resp, node_idx)
                continue
        return successes >= quorum

    def _find_shard_ranges(self, broker):
        """
        This function is the main work horse of a scanner node, it:
          - look at the shard_ranges table to see where to continue on from.
          - Once it finds the next shard_range it'll ask for a quorum as to
             whether this node is still in fact a scanner node.
          - If it is still the scanner, it's time to add it to the shard_ranges
            table.

        :param broker:
        :return:
        """
        self.logger.info('Started searching for shard ranges on %s/%s',
                         broker.account, broker.container)

        # get the last shard found to continue from
        cont_range = broker.get_shard_range()
        cont_lower = cont_range.lower if cont_range else ''
        cont_upper = cont_range.upper if cont_range else ''

        shard_ranges = broker.build_shard_ranges()
        old_range = marker = shard_ranges[-1].upper if shard_ranges else ''
        if old_range and broker.get_db_state() == DB_STATE_UNSHARDED:
            broker.set_sharding_state()
        progress = len(shard_ranges) * self.split_size
        obj_count = broker.get_info().get('object_count', 0)
        last_found = False

        def found_last():
            return progress + self.split_size >= obj_count or last_found

        if found_last():
            self.logger.info("Already found all shard ranges")
            return

        found_ranges = []

        for i in range(self.scanner_batch_size):
            next_upper = broker.get_next_shard_range_upper(
                self.shard_container_size, marker)
            if next_upper is None:
                # We have hit passed the end of the container, make
                # the last container.
                last_found = True
                next_upper = cont_upper
            elif not next_upper:
                # something happened and we couldn't find a new upper. Stop
                # where we are but don't mark complete.
                break

            progress += self.split_size
            marker = next_upper
            found_ranges.append(next_upper)
            if found_last():
                break

        if not found_ranges:
            # we didn't find anything
            self.logger.warning("No shard ranges found, something went wrong. "
                                "We will try again next cycle.")
            return

        # make sure this node is still the scanner (a split brain might have
        # happened and now someone else is).
        def on_success(resp, node_idx):
            if not resp or not is_success(resp.status):
                return False

            found_scan_id = resp.getheader('X-Container-Sysmeta-Shard-Scanner')

            if found_scan_id:
                if found_scan_id != str(self.node_idx):
                    return False
            else:
                return False

            return True

        if not self._get_quorum(broker, success=on_success):
            self.logger.info('Failed to reach quorum node %d may not be the '
                             'scanner for %s/%s anymore. Aborting scan.',
                             self.node_idx, broker.account, broker.container)
            return

        # we are still the scanner, so lets write the shard points.
        shard_ranges = []
        root_account, root_container = broker.get_shard_root_path()
        shard_account = account_to_shard_account(root_account)
        lower = old_range if old_range else cont_lower
        policy = POLICIES.get_by_index(broker.storage_policy_index)
        headers = {
            'X-Storage-Policy': policy.name,
            'X-Container-Sysmeta-Shard-Account': root_account,
            'X-Container-Sysmeta-Shard-Container': root_container,
            'X-Container-Sysmeta-Sharding': True}
        for i, shard_range in enumerate(found_ranges):
            timestamp = Timestamp(time.time()).internal
            piv_name = self.generate_shard_range_name(
                root_container, self.node_idx, shard_range or '', timestamp)
            try:
                headers.update({
                    'X-Container-Sysmeta-Shard-Lower': lower,
                    'X-Container-Sysmeta-Shard-Upper': shard_range,
                    'X-Container-Sysmeta-Shard-Timestamp': timestamp,
                    'X-Container-Sysmeta-Shard-Meta-Timestamp': timestamp})
                self.swift.create_container(shard_account, piv_name,
                                            headers=headers)
            except internal_client.UnexpectedResponse as ex:
                self.logger.warning('Failed to put container: %s', str(ex))
                self.logger.error('PUT of new shard containers failed, '
                                  'cancelling split of %s/%s. '
                                  'Will try again next cycle',
                                  broker.account, broker.container)
                break
            new_range = ShardRange(piv_name, timestamp, lower, shard_range,
                                   0, 0, timestamp)
            shard_ranges.append(new_range)
            lower = shard_range
            self.logger.increment('shard_ranges_found')
            self.stats['container_shard_ranges'] += 1

        if i + 1 < len(found_ranges):
            found_ranges = found_ranges[:i]
            progress = ((len(shard_ranges) + len(found_ranges)) *
                        self.split_size)

        if found_last() and not last_found:
            # We need to add the final shard range range as well.
            timestamp = Timestamp(time.time()).internal
            piv_name = self.generate_shard_range_name(
                root_container, self.node_idx, '', timestamp)
            new_range = ShardRange(piv_name, timestamp, lower, cont_upper, 0,
                                   0, timestamp)
            shard_ranges.append(new_range)
            # add something the found_ranges so the stats will be correct.
            found_ranges.append('last one')
            try:
                headers.update({
                    'X-Container-Sysmeta-Shard-Lower': lower,
                    'X-Container-Sysmeta-Shard-Upper': cont_upper,
                    'X-Container-Sysmeta-Shard-Timestamp': timestamp,
                    'X-Container-Sysmeta-Shard-Meta-Timestamp': timestamp})
                self.swift.create_container(shard_account, piv_name,
                                            headers=headers)
            except internal_client.UnexpectedResponse as ex:
                self.logger.warning('Failed to put container: %s', str(ex))
                self.logger.error('PUT of new shard containers failed, '
                                  'cancelling split of %s/%s. '
                                  'Will try again next cycle',
                                  broker.account, broker.container)

            self.logger.increment('shard_ranges_found')
            self.stats['container_shard_ranges'] += 1

        items = self._generate_object_list(shard_ranges, 0)
        broker.merge_items(items)

        self.cpool.spawn(
            self._replicate_object, self.part, broker.db_file, self.node_id)

        if broker.get_db_state == DB_STATE_UNSHARDED:
            broker.set_sharding_state()

        self.logger.info("Scan cycle completed, found %d new shard ranges.",
                         len(found_ranges))
        if found_last():
            # We've found the last shard range, so mark that in metadata
            timestamp = Timestamp(time.time()).internal
            broker.update_metadata({
                'X-Container-Sysmeta-Sharding-Scan-Done': (True, timestamp)})
            self.logger.info(" Final shard range reached.")
            return True

    def generate_shard_range_name(self, container, node_id, shard_range,
                                  timestamp=None):
        if not timestamp:
            timestamp = Timestamp(time.time()).internal

        md5sum = md5()
        md5sum.update("%s-%s" % (shard_range, timestamp))
        return "%s-%d-%s" % (container, node_id, md5sum.hexdigest())

    def _shrink(self, broker, root_account, root_container):
        """shrinking is a 2 phase process

        Phase 1: If the container is small enough see if there is a neighbour
        to merge with. If so  then mark our intentions as metadata:
            X-Container-Sysmeta-Shard-Merge: <neighbour>
            X-Container-Sysmeta-Shard-Shrink: <this container>

        Phase 2: On a storage node running a sharder what contains a neighbour
        replica, create a new container for the new range. Move all objects in
        to it, delete self and shard-empty, update root and replicate.
        """

        if broker.metadata.get('X-Container-Sysmeta-Shard-Merge') or \
                broker.metadata.get('X-Container-Sysmeta-Shard-Shrink'):
            self._shrink_phase_2(broker, root_account, root_container)
        else:
            self._shrink_phase_1(broker, root_account, root_container)

    def _shrink_phase_1(self, broker, root_account, root_container):
        """Attempt to shrink the current sharded container.

        To shrink a container we need to:
            1. Find out if the container really has few enough objects, that is
               a quruom of counts below the threshold.
            2. Check the neighbours to see if it's possible to shrink/merge
               together, again this requires getting a quorom.
            3. Mark intentions with metadata.
        """
        if root_container == broker.container:
            # This is the root container, can't shrink it.
            return

        self.logger.info('Sharded container %s/%s is a candidate for '
                         'shrinking', broker.account, broker.container)
        shard_range = broker.get_shard_range()

        obj_count = [broker.get_info()['object_count']]

        def on_success(resp, node_idx):
            # We need to make sure that if this neighbour is in the middle
            # of shrinking, it isn't chosen as neighbour to merge into.
            shrink = resp.getheader('X-Container-Sysmeta-Shard-Shrink')
            merge = resp.getheader('X-Container-Sysmeta-Shard-Merge')
            if not shrink and not merge:
                oc = resp.getheader('X-Container-Object-Count')
                try:
                    oc = int(oc)
                except ValueError:
                    oc = self.shard_container_size
                obj_count[0] = \
                    max(obj_count[0], oc)
            else:
                obj_count[0] = self.shard_container_size

        if not self._get_quorum(broker, post_success=on_success):
            self.logger.info('Failed to get quorum on object count in '
                             '%s/%s', broker.account, broker.container)
            return

        # We have a quorum on the number of object can we still shrink?
        if obj_count[0] > self.shard_container_size * self.shard_shrink_point:
            self.logger.info('After quorum check there were too many objects '
                             ' (%d) to continue shrinking %s/%s',
                             obj_count[0], broker.account, broker.container)
            return
        curr_obj_count = obj_count[0]

        # Now we need to find a neighbour, if possible, to merge with.
        # since we know the current range, we can make some simple assumptions.
        #   1. We know if we are on either end (lower or upper is None).
        #   2. Anything to the left we can use the lower to find the one
        #      directly before, because the upper is always included in
        #      the range. (use find_shard_range(lower, ranges).
        #   3. Upper + something is in the next range.
        # TODO: ranges were previously held as an instance var - perhaps this
        # afforded some caching if shrink is called more than once for same set
        # of ranges? revisit, but don't hang ranges off self
        ranges = self._get_shard_ranges(root_account, root_container,
                                        newest=True)
        if ranges is None:
            self.logger.error(
                "Since the audit run of this container and "
                "now we can't access the root container "
                "%s/%s aborting.",
                root_account, root_container)
            return
        lower_n = upper_n = None
        lower_c = upper_c = self.shard_container_size
        if shard_range.lower:
            lower_n = find_shard_range(shard_range.lower, ranges)

            obj_count = [0]

            if self._get_quorum(None, post_success=on_success,
                                account=broker.account,
                                container=lower_n.name):
                lower_c = obj_count[0]

        if shard_range.upper:
            upper = str(shard_range.upper)[:-1] + \
                chr(ord(str(shard_range.upper)[-1]) + 1)
            upper_n = find_shard_range(upper, ranges)

            obj_count = [0]

            if self._get_quorum(None, post_success=on_success,
                                account=broker.account,
                                container=upper_n.name):
                upper_c = obj_count[0]

        # got counts. now need to compare.
        neighbours = {lower_c: lower_n, upper_c: upper_n}
        smallest = min(neighbours.keys())
        if smallest + curr_obj_count > self.shard_container_size * \
                self.shrink_merge_point:
            self.logger.info(
                'If this container merges with it\'s smallest neighbour (%s) '
                'there will be too many objects. %d (merged) > %d '
                '(shard_merge_point)', neighbours[smallest].name,
                smallest + curr_obj_count,
                self.shard_container_size * self.shrink_merge_point)
            return

        # So we now have valid neighbour, so we want to move our objects in to
        # it.
        n_shard_range = neighbours[smallest]

        # Now just need to update the metadata on both containers.
        shrink_meta = {
            'X-Container-Sysmeta-Shard-Merge': n_shard_range.name,
            'X-Container-Sysmeta-Shard-Shrink': broker.container}

        # TODO This exception handling needs to be cleaned up.
        try:
            for acct, cont in ((broker.account, broker.container),
                               (broker.account, n_shard_range.name)):
                self.swift.set_container_metadata(acct, cont, shrink_meta)
            self.logger.increment('shrunk_phase_1')
        except Exception:
            self.logger.exception('Could not add shrink metadata %r',
                                  shrink_meta)

    def _get_merge_range(self, account, container):
        path = self.swift.make_path(account, container)
        headers = dict()
        try:
            resp = self.swift.make_request('HEAD', path, headers,
                                           acceptable_statuses=(2,))
        except internal_client.UnexpectedResponse:
            self.logger.error("Failed to get range from %s/%s",
                              account, container)
            return None

        lower = resp.headers.get('X-Container-Sysmeta-Shard-Lower')
        upper = resp.headers.get('X-Container-Sysmeta-Shard-Upper')
        timestamp = resp.headers.get('X-Container-Sysmeta-Shard-Timestamp')

        if not lower and not upper:
            return None

        if not lower:
            lower = None
        if not upper:
            upper = None

        return ShardRange(container, timestamp, lower, upper,
                          meta_timestamp=Timestamp(time.time()).internal)

    def _shrink_phase_2(self, broker, root_account, root_container):
        # We've set metadata last phase. lets make sure it's still the case.
        shrink_containers = broker.get_shrinking_containers()
        shrink_shard = shrink_containers.get('shrink')
        merge_shard = shrink_containers.get('merge')

        # We move data from the shrinking container to the merge. So if this
        # isn't the shrink container, then continue.
        if not shrink_containers or broker.container != \
                shrink_containers['shrink']:
            return
        self.logger.info('Starting Shrinking phase 2 on %s/%s.',
                         broker.account, broker.container)

        # OK we have a shrink container, now lets make sure we have a quorum on
        # what the containers need to be, just in case.
        def is_success(resp, node_idx):
            return resp.getheader('X-Container-Sysmeta-Shard-Shrink') == \
                shrink_shard \
                and resp.getheader('X-Container-Sysmeta-Shard-Merge') == \
                merge_shard

        if not self._get_quorum(broker, success=is_success):
            self.logger.info('Failed to reach quorum on a empty/full shard '
                             'range for shrinking %s/%s', broker.account,
                             broker.container)
            # TODO What should be do in this situation? Just wait and hope it
            # still needs to replicate the missing or different metadata or
            # attempt to abort shrinking.
            return

        # OK so we agree, so now we can merge this container (shrink) into the
        # merge container neighbour. First lets build the new shard range
        shrink_range = broker.get_shard_range()
        merge_range = self._get_merge_range(broker.account, merge_shard)
        if merge_range > shrink_range:
            merge_range.lower = shrink_range.lower
        else:
            merge_range.upper = shrink_range.upper

        policy_index = broker.storage_policy_index
        query = dict(marker='', end_marker='', prefix='', delimiter='',
                     storage_policy_index=policy_index)

        try:
            new_part, new_broker, node_id = \
                self._get_shard_broker(broker.account, merge_range.name,
                                       policy_index)

            self._add_shard_metadata(new_broker, root_account, root_container,
                                     merge_range, force=True)

            with new_broker.sharding_lock():
                self._add_items(broker, new_broker, query)

        except DeviceUnavailable as duex:
            self.logger.warning(str(duex))
            return

        timestamp = Timestamp(time.time()).internal
        info = new_broker.get_info()
        merge_piv = ShardRange(
            merge_range.name, timestamp, merge_range.lower,
            merge_range.upper, "+%d" % info['object_count'],
            "+%d" % info['bytes_used'], timestamp)
        # Push the new shard range to the root container,
        # we do this so we can short circuit PUTs.
        self._update_shard_ranges(root_account, root_container, 'PUT',
                                  (merge_piv,))

        # We also need to remove the shrink shard range from the root container
        empty_piv = ShardRange(
            shrink_range.name, timestamp, shrink_range.lower,
            shrink_range.upper, 0, 0, timestamp)
        self._update_shard_ranges(root_account, root_container, 'DELETE',
                                  (empty_piv,))

        # Now we can delete the shrink container (it should be empty)
        broker.delete_db(timestamp)

        part = self.ring.get_part(broker.account, broker.container)
        self.logger.info('Replicating phase 2 shrunk containers %s/%s and '
                         '%s/%s', new_broker.account, new_broker.container,
                         broker.account, broker.container)

        # Remove shrinking headers from the merge container's new broker so
        # they get cleared after replication.
        new_broker.update_metadata({
            'X-Container-Sysmeta-Shard-Merge': ('', timestamp),
            'X-Container-Sysmeta-Shard-Shrink': ('', timestamp)})

        # replicate merge container
        self.cpool.spawn(
            self._replicate_object, new_part, new_broker.db_file, node_id)
        # replicate shrink
        self.cpool.spawn(
            self._replicate_object, part, broker.db_file, node_id)
        self.logger.increment('shrunk_phase_2')
        self.stats['containers_shrunk'] += 1
        any(self.cpool)

    def _add_items(self, broker, broker_to_update, qry, ignore_state=False):
        """
        Move items from one broker to another.

        The qry is a query dict in the form of:
            dict(marker='', end_marker='', prefix='', delimiter='',
                 storage_policy_index=policy_index)
        """
        if not ignore_state:
            db_state = broker.get_db_state()
            if db_state == DB_STATE_SHARDING:
                brokers = broker.get_brokers()
                for b in brokers:
                    q = qry.copy()
                    self._add_items(b, broker_to_update, q, ignore_state=True)
            return
        while True:
            new_items = broker.list_objects_iter(
                CONTAINER_LISTING_LIMIT, **qry)

            # Add new items
            objects = self._generate_object_list(
                new_items, broker.storage_policy_index)
            broker_to_update.merge_items(objects)

            # Delete existing (while we have the same view of the items)
            # delete_objs = self._generate_object_list(
            #     new_items, broker.storage_policy_index, delete=True)
            # broker.merge_items(delete_objs)

            if len(new_items) >= CONTAINER_LISTING_LIMIT:
                qry['marker'] = new_items[-1][0]
            else:
                break

    def _sharding_complete(self, root_account, root_container, broker):
        broker.set_sharded_state()
        if root_container != broker.container:
            # We aren't in the root container.
            self._update_shard_ranges(root_account, root_container, 'PUT',
                                      broker.build_shard_ranges())
            timestamp = Timestamp(time.time()).internal
            shard_range = broker.get_shard_range()
            shard_range.timestamp = timestamp
            self._update_shard_ranges(root_account, root_container, 'DELETE',
                                      [shard_range])
            broker.delete_db(timestamp)

    def _cleave(self, broker, root_account, root_container):
        last_range = self.get_metadata_item(
            broker, 'X-Container-Sysmeta-Shard-Last-%d' % self.node_idx)
        scan_complete = self.get_metadata_item(
            broker, 'X-Container-Sysmeta-Sharding-Scan-Done')

        shard_ranges = broker.build_shard_ranges()
        if not shard_ranges:
            # No shard ranges points yet defined.
            return
        elif broker.get_db_state() == DB_STATE_UNSHARDED:
            # We have a shard range, which means its time to start sharding
            broker.set_sharding_state()

        last_piv_exists = False
        if last_range:
            last_range = find_shard_range(last_range, shard_ranges)
            last_piv_exists = last_range and last_range.upper == last_range

        ranges_todo = [
            p for p in shard_ranges
            if p.upper > last_range or p.lower >= last_range]
        if not ranges_todo:
            # This means no new shard_ranges have been added since last cycle.
            # If the scanner is complete, then we have finished sharding.
            # However there is an edge case where the scan could be complete,
            # but we haven't finished but this node has been off and shrinking
            # or other sharding has taken place. If this is the case and the
            # last range doesn't exist, then we need find the last real shard
            # range and send the rest of the objects to it so nothing is lost.
            if scan_complete and last_piv_exists:
                self._sharding_complete(root_account, root_container, broker)
                return
            elif scan_complete and not last_piv_exists:
                last_range.lower = last_range
                last_range.dont_save = True
                ranges_todo.append(last_range)
            else:
                self.logger.info('No new shard range for %s/%s found, will '
                                 'try again next cycle',
                                 broker.account, broker.container)
                return

        if last_range:
            self.logger.info('Continuing to shard %s/%s',
                             broker.account, broker.container)
        else:
            self.logger.info('Starting to shard %s/%s',
                             broker.account, broker.container)

        ranges_done = []
        for i in range(self.shard_batch_size):
            if ranges_todo:
                shard_range = ranges_todo.pop(0)
            else:
                break

            self.logger.info(
                'Sharding %s/%s on shard range %s',
                broker.account, broker.container, shard_range.upper)

            policy_index = broker.storage_policy_index
            query = dict(marker='', end_marker='', prefix='', delimiter='',
                         storage_policy_index=policy_index)

            q = query.copy()
            q['marker'] = shard_range.lower or ''
            if shard_range.upper:
                q['end_marker'] = shard_range.upper + '\x00'
            try:
                acct = account_to_shard_account(root_account)
                new_part, new_broker, node_id = self._get_shard_broker(
                    acct, shard_range.name, policy_index)

                self._add_shard_metadata(new_broker, root_account,
                                         root_container, shard_range)

                with new_broker.sharding_lock():
                    self._add_items(broker, new_broker, q)

                info = new_broker.get_info()
                shard_range.object_count = info['object_count']
                shard_range.bytes_used = info['bytes_used']
                shard_range.meta_timestamp = Timestamp(time.time())
                if not hasattr(shard_range, 'dont_save'):
                    ranges_done.append(shard_range)

            except DeviceUnavailable as duex:
                self.logger.warning(str(duex))
                self.logger.increment('failure')
                self.stats['containers_failed'] += 1
                return

            self.logger.info('Replicating new shard container %s/%s',
                             new_broker.account, new_broker.container)
            self.cpool.spawn(
                self._replicate_object, new_part, new_broker.db_file, node_id)
            last_range = shard_range.upper
            self.logger.info('Node %d sharded %s/%s at shard range %s.',
                             self.node_id, broker.account, broker.container,
                             shard_range.upper)
            self.logger.increment('sharded')
            self.stats['containers_sharded'] += 1
        if ranges_done:
            broker.merge_items(broker.shard_nodes_to_items(ranges_done))
        any(self.cpool)

        if scan_complete and not ranges_todo:
            # we've finished sharding this container.
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Last-%d' % self.node_idx:
                    ('', Timestamp(time.time()).internal)})
            self._sharding_complete(root_account, root_container, broker)
            self.logger.increment('sharding_complete')
        else:
            broker.update_metadata({
                'X-Container-Sysmeta-Shard-Last-%d' % self.node_idx:
                    (last_range, Timestamp(time.time()).internal)})

    # TODO: unused method - ok to delete?
    def _push_shard_ranges_to_container(self, root_account,
                                        root_container, shard_range,
                                        storage_policy_index):
        # Push the new distributed node to the container.
        part, root_broker, node_id = \
            self._get_shard_broker(root_account, root_container,
                                   storage_policy_index)
        objects = self._generate_object_list(shard_range, storage_policy_index)
        root_broker.merge_items(objects)
        self.cpool.spawn(
            self._replicate_object, part, root_broker.db_file, node_id)
        any(self.cpool)

    def run_forever(self, *args, **kwargs):
        """Run the container sharder until stopped."""
        self._zero_stats()
        self.reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            begin = time.time()
            try:
                self._one_shard_cycle()
            except (Exception, Timeout):
                self.logger.increment('errors')
                self.logger.exception('ERROR sharding')
            elapsed = time.time() - begin
            self.logger.info(
                'Container sharder cycle completed: %.02fs', elapsed)
            dump_recon_cache({'container_sharder_cycle_completed': elapsed},
                             self.rcache, self.logger)
            self._report_stats()
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """Run the container sharder once."""
        self.logger.info('Begin container sharder "once" mode')
        self._zero_stats()
        begin = self.reported = time.time()
        self._one_shard_cycle()
        elapsed = time.time() - begin
        self.logger.info(
            'Container sharder "once" mode completed: %.02fs', elapsed)
        # TODO: why's the stat order different compared to above?
        self._report_stats()
        dump_recon_cache({'container_sharder_cycle_completed': elapsed},
                         self.rcache, self.logger)


class RangeLink(object):
    """
    A linked list link node but can store more then 1 link to the next
    range so we can follow forks
    """
    def __init__(self, shard_range, upper=None):
        self._shard_range = shard_range
        self._upper = upper or []

    @property
    def shard_range(self):
        return self._shard_range

    @property
    def upper(self):
        return self._upper

    @upper.setter
    def upper(self, item):
        self._upper = item


class RangeAnalyser(object):
    """Analyses a list of ShardRange objects to find the newest path,
    determine any gaps and will return a generated list of paths present.
    """
    def __init__(self):
        self._reset()

    def _reset(self):
        self.gaps = []
        self.complete = []
        self.incomplete = []
        self.newest = {}
        self.path = []
        self.paths = {'i': self.incomplete, 'c': self.complete}

    def _build(self, ranges):
        ranges.sort()
        self.path = []
        upto = {'': self.path}
        for r in ranges:
            rl = RangeLink(r)
            if r.lower not in upto:
                self.gaps.append(rl)
            else:
                upto[r.lower].append(rl)

            if r.upper:
                if r.upper in upto:
                    rl.upper = upto[r.upper]
                else:
                    upto[r.upper] = rl.upper

    def _post_result(self, newest, complete, result):
        idx = None
        if complete:
            idx = 'c%d' % len(self.complete)
            self.complete.append(result)
        else:
            matched = []
            if result[0].lower is not None:
                # This incomplete has a gap at the start, it _may_ be the
                # end of another incomplete.
                matched = [(i, r) for i, r in enumerate(self.incomplete)
                           if r[-1].upper and r[-1] < result[0]]
                if matched:
                    matched = sorted(matched, key=lambda x: x[-1][-1],
                                     reverse=True)
                    i, match = matched[0]
                    match.extend(result)
                    result = match
                    idx = 'i%d' % i
                    # if this segment makes it newer we need to update the
                    # paths newest value
                    current = [(ts, idxs) for ts, idxs in self.newest.items()
                               if idx in idxs]
                    if current:
                        ts = current[0][0]
                        newest = max(ts, newest)
                        if newest != ts:
                            # remove the existing entry
                            if len(self.newest[ts]) > 1:
                                self.newest[ts].remove(idx)
                            else:
                                self.newest.pop(ts)
                        else:
                            return
            if not matched:
                idx = 'i%d' % len(self.incomplete)
                self.incomplete.append(result)

        if self.newest.get(newest):
            self.newest[newest].append(idx)
        else:
            self.newest[newest] = [idx]

    def _walk(self, rangelink, ts, result):
        newest = max(ts, rangelink.shard_range.timestamp)
        result.append(rangelink.shard_range)
        if not rangelink.upper:
            complete = (rangelink.shard_range.upper == '')
            return newest, complete, result
        elif len(rangelink.upper) > 1:
            for i, rl in enumerate(rangelink.upper):
                if i == 0:
                    # We need 1 path to send back to potentially follow other
                    # branches
                    continue
                new_res = list(result)
                new, complete, res = self._walk(rl, newest, new_res)
                self._post_result(new, complete, res)
            return self._walk(rangelink.upper[0], newest, result)
        else:
            return self._walk(rangelink.upper[0], newest, result)

    def _scan(self):
        try:
            for r in self.path:
                newest, complete, result = self._walk(
                    r, r.shard_range.timestamp, [])
                self._post_result(newest, complete, result)

            for r in self.gaps:
                newest, complete, result = self._walk(
                    r, r.shard_range.timestamp, [])
                self._post_result(newest, False, result)

            self._break_ties()
        except Exception as ex:
            # TODO: either stop wrapping or include a traceback -- errors like
            #
            #   <lambda>() takes exactly 1 argument (2 given)
            #
            # aren't super helpful on their own
            raise RangeAnalyserException('Failed to find a correct set of '
                                         'ranges: %s' % str(ex))

    def _break_ties(self):
        # Idea here, is that the may be a newer change to the list of ranges
        # and this is where the same TS is coming from. So lets get the
        # difference and compare only whats different on tied paths.
        def to_timestamps(p):
            return [item.timestamp for item in p]

        tmp_newest = self.newest.copy()
        for ts, indexes in ((k, v) for k, v in tmp_newest.items()
                            if len(v) > 1):
            paths = [(idx, self.paths[idx[:1]][int(idx[1:])])
                     for idx in indexes]

            timestamps = []
            for idx, path in paths:
                dif = list(map(set(path).difference,
                               [v for p, v in paths if p != idx]))
                tss = list(map(to_timestamps, dif))
                max_tss = sorted(map(max, tss), reverse=True)
                timestamps.append((
                    idx, ''.join(ts.internal for ts in max_tss)))

            timestamps.sort(key=lambda x: x[-1])
            self.newest.pop(ts)
            timestamp = Timestamp(ts)
            for idx, _junk in timestamps:
                timestamp.offset += 1
                self.newest[timestamp.internal] = [idx]

    def _pick(self):
        newest_keys = sorted(self.newest.keys(), reverse=True)

        for key in newest_keys:
            idx = self.newest[key][0]
            path = self.paths[idx[:1]][int(idx[1:])]
            # yeild <path>, <set of items from ranges not in path>, completed
            yield sorted(path), self._ranges.difference(set(path)), \
                idx.startswith('c')

    def analyse(self, ranges):
        """Analyse the given list of ShardRanges for a list of paths,
        newest first, and gops.

        Yields the determined paths from newest to oldest, in each case
        returning a set of unused ShardRanges and whether the path is complete.

           (<path list>, <set of other ShardRanges>, <bool complete>)
        :param ranges:
        :return: Tuple of (path, other_shard_ranges, complete).
        """
        self._reset()
        self._ranges = set(ranges)
        self._build(ranges)
        self._scan()
        return self._pick()
