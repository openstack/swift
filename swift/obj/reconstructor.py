# Copyright (c) 2010-2015 OpenStack Foundation
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
import itertools
import json
import errno
from optparse import OptionParser
import os
from os.path import join
import random
import time
from collections import defaultdict
import pickle  # nosec: B403
import shutil

from eventlet import (GreenPile, GreenPool, Timeout, sleep, tpool, spawn)
from eventlet.support.greenlets import GreenletExit

from swift.common.utils import (
    whataremyips, unlink_older_than, compute_eta, get_logger,
    dump_recon_cache, mkdirs, config_true_value, parse_options,
    GreenAsyncPile, Timestamp, remove_file, node_to_string,
    load_recon_cache, parse_override_options, distribute_evenly,
    remove_directory, config_request_node_count_value,
    non_negative_int, get_prefixed_logger)
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon, run_daemon
from swift.common.recon import RECON_OBJECT_FILE, DEFAULT_RECON_CACHE_PATH
from swift.common.ring.utils import is_local_device
from swift.obj.ssync_sender import Sender as ssync_sender
from swift.common.http import HTTP_OK, HTTP_NOT_FOUND, \
    HTTP_INSUFFICIENT_STORAGE
from swift.obj.diskfile import DiskFileRouter, get_data_dir, \
    get_tmp_dir, DEFAULT_RECLAIM_AGE
from swift.common.storage_policy import POLICIES, EC_POLICY
from swift.common.exceptions import ConnectionTimeout, DiskFileError, \
    SuffixSyncError, PartitionLockTimeout, DiskFileNotExist

SYNC, REVERT = ('sync_only', 'sync_revert')
UNKNOWN_RESPONSE_STATUS = 0  # used as response status for timeouts, exceptions


def _get_partners(node_index, part_nodes):
    """
    Returns the left, right and far partners of the node whose index is equal
    to the given node_index.

    :param node_index: the primary index
    :param part_nodes: a list of primary nodes
    :returns: [<node-to-left>, <node-to-right>, <node-opposite>]
    """
    num_nodes = len(part_nodes)
    return [
        part_nodes[(node_index - 1) % num_nodes],
        part_nodes[(node_index + 1) % num_nodes],
        part_nodes[(
            node_index + (num_nodes // 2)
        ) % num_nodes],
    ]


def _full_path(node, part, relative_path, policy):
    """
    Combines the node properties, partition, relative-path and policy into a
    single string representation.

    :param node: a dict describing node properties
    :param part: partition number
    :param path: path of the desired EC archive relative to partition dir
    :param policy: an instance of
                   :class:`~swift.common.storage_policy.BaseStoragePolicy`
    :return: string representation of absolute path on node plus policy index
    """
    if not isinstance(relative_path, str):
        relative_path = relative_path.decode('utf8')
    return '%(node)s/%(part)s%(path)s policy#%(policy)d' % {
        'node': node_to_string(node, replication=True),
        'part': part, 'path': relative_path,
        'policy': policy,
    }


class ResponseBucket(object):
    """
    Encapsulates fragment GET response data related to a single timestamp.
    """
    def __init__(self):
        # count of all responses associated with this Bucket
        self.num_responses = 0
        # map {frag_index: response} for subset of responses that could be used
        # to rebuild the missing fragment
        self.useful_responses = {}
        # set if a durable timestamp was seen in responses
        self.durable = False
        # etag of the first response associated with the Bucket
        self.etag = None


class RebuildingECDiskFileStream(object):
    """
    This class wraps the reconstructed fragment archive data and
    metadata in the DiskFile interface for ssync.
    """

    def __init__(self, datafile_metadata, frag_index, rebuilt_fragment_iter):
        # start with metadata from a participating FA
        self.datafile_metadata = datafile_metadata

        # the new FA is going to have the same length as others in the set
        self._content_length = int(self.datafile_metadata['Content-Length'])
        # update the FI and delete the ETag, the obj server will
        # recalc on the other side...
        self.datafile_metadata['X-Object-Sysmeta-Ec-Frag-Index'] = frag_index
        for etag_key in ('ETag', 'Etag'):
            self.datafile_metadata.pop(etag_key, None)

        self.frag_index = frag_index
        self.rebuilt_fragment_iter = rebuilt_fragment_iter

    def get_metadata(self):
        return self.datafile_metadata

    def get_datafile_metadata(self):
        return self.datafile_metadata

    @property
    def content_length(self):
        return self._content_length

    def reader(self):
        for chunk in self.rebuilt_fragment_iter:
            yield chunk


class ObjectReconstructor(Daemon):
    """
    Reconstruct objects using erasure code.  And also rebalance EC Fragment
    Archive objects off handoff nodes.

    Encapsulates most logic and data needed by the object reconstruction
    process. Each call to .reconstruct() performs one pass.  It's up to the
    caller to do this in a loop.
    """

    def __init__(self, conf, logger=None):
        """
        :param conf: configuration object obtained from ConfigParser
        :param logger: an instance of ``SwiftLogAdapter``.
        """
        self.conf = conf
        self.logger = \
            logger or get_logger(conf, log_route='object-reconstructor')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring_ip = conf.get('ring_ip', conf.get('bind_ip', '0.0.0.0'))
        self.servers_per_port = int(conf.get('servers_per_port', '0') or 0)
        self.port = None if self.servers_per_port else \
            int(conf.get('bind_port', 6200))
        self.concurrency = int(conf.get('concurrency', 1))
        # N.B. to maintain compatibility with legacy configs this option can
        # not be named 'workers' because the object-server uses that option
        # name in the DEFAULT section
        self.reconstructor_workers = int(conf.get('reconstructor_workers', 0))
        self.policies = [policy for policy in POLICIES
                         if policy.policy_type == EC_POLICY]
        self.stats_interval = float(conf.get('stats_interval', '300'))
        self.ring_check_interval = float(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.partition_times = []
        self.interval = float(conf.get('interval') or
                              conf.get('run_pause') or 30)
        if 'run_pause' in conf:
            if 'interval' in conf:
                self.logger.warning(
                    'Option object-reconstructor/run_pause is deprecated and '
                    'object-reconstructor/interval is already configured. '
                    'You can safely remove run_pause; it is now ignored and '
                    'will be removed in a future version.')
            else:
                self.logger.warning(
                    'Option object-reconstructor/run_pause is deprecated '
                    'and will be removed in a future version. '
                    'Update your configuration to use option '
                    'object-reconstructor/interval.')
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.rcache = os.path.join(self.recon_cache_path, RECON_OBJECT_FILE)
        self._next_rcache_update = time.time() + self.stats_interval
        # defaults subject to change after beta
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.headers = {
            'Content-Length': '0',
            'user-agent': 'obj-reconstructor %s' % os.getpid()}
        if 'handoffs_first' in conf:
            self.logger.warning(
                'The handoffs_first option is deprecated in favor '
                'of handoffs_only. This option may be ignored in a '
                'future release.')
            # honor handoffs_first for backwards compatibility
            default_handoffs_only = config_true_value(conf['handoffs_first'])
        else:
            default_handoffs_only = False
        self.handoffs_only = config_true_value(
            conf.get('handoffs_only', default_handoffs_only))
        if self.handoffs_only:
            self.logger.warning(
                'Handoff only mode is not intended for normal '
                'operation, use handoffs_only with care.')
        elif default_handoffs_only:
            self.logger.warning('Ignored handoffs_first option in favor '
                                'of handoffs_only.')
        self.rebuild_handoff_node_count = int(conf.get(
            'rebuild_handoff_node_count', 2))
        self.quarantine_threshold = non_negative_int(
            conf.get('quarantine_threshold', 0))
        self.quarantine_age = int(
            conf.get('quarantine_age',
                     conf.get('reclaim_age', DEFAULT_RECLAIM_AGE)))
        self.request_node_count = config_request_node_count_value(
            conf.get('request_node_count', '2 * replicas'))
        self.max_objects_per_revert = non_negative_int(
            conf.get('max_objects_per_revert', 0))
        # When upgrading from liberasurecode<=1.5.0, you may want to continue
        # writing legacy CRCs until all nodes are upgraded and capabale of
        # reading fragments with zlib CRCs.
        # See https://bugs.launchpad.net/liberasurecode/+bug/1886088 for more
        # information.
        if 'write_legacy_ec_crc' in conf:
            os.environ['LIBERASURECODE_WRITE_LEGACY_CRC'] = \
                '1' if config_true_value(conf['write_legacy_ec_crc']) else '0'
        # else, assume operators know what they're doing and leave env alone

        self._df_router = DiskFileRouter(conf, self.logger)
        self.all_local_devices = self.get_local_devices()
        self.rings_mtime = None

    def get_worker_args(self, once=False, **kwargs):
        """
        Take the set of all local devices for this node from all the EC
        policies rings, and distribute them evenly into the number of workers
        to be spawned according to the configured worker count. If `devices` is
        given in `kwargs` then distribute only those devices.

        :param once: False if the worker(s) will be daemonized, True if the
            worker(s) will be run once
        :param kwargs: optional overrides from the command line
        """
        if self.reconstructor_workers < 1:
            return
        override_opts = parse_override_options(once=once, **kwargs)

        # Note that this get re-used when dumping stats and in is_healthy
        self.all_local_devices = self.get_local_devices()

        if override_opts.devices:
            devices = [d for d in override_opts.devices
                       if d in self.all_local_devices]
        else:
            devices = list(self.all_local_devices)
        if not devices:
            # we only need a single worker to do nothing until a ring change
            yield dict(override_devices=override_opts.devices,
                       override_partitions=override_opts.partitions,
                       multiprocess_worker_index=0)
            return

        # for somewhat uniform load per worker use same
        # max_devices_per_worker when handling all devices or just override
        # devices, but only use enough workers for the actual devices being
        # handled
        self.reconstructor_workers = min(self.reconstructor_workers,
                                         len(devices))
        for index, ods in enumerate(distribute_evenly(
                devices, self.reconstructor_workers)):
            yield dict(override_partitions=override_opts.partitions,
                       override_devices=ods,
                       multiprocess_worker_index=index)

    def is_healthy(self):
        """
        Check whether rings have changed, and maybe do a recon update.

        :returns: False if any ec ring has changed
        """
        now = time.time()
        if now > self._next_rcache_update:
            self._next_rcache_update = now + self.stats_interval
            self.aggregate_recon_update()
        rings_mtime = [os.path.getmtime(self.load_object_ring(
                       policy).serialized_path) for policy in self.policies]
        if self.rings_mtime == rings_mtime:
            return True
        self.rings_mtime = rings_mtime
        return self.get_local_devices() == self.all_local_devices

    def aggregate_recon_update(self):
        """
        Aggregate per-disk rcache updates from child workers.
        """
        existing_data = load_recon_cache(self.rcache)
        first_start = time.time()
        last_finish = 0
        all_devices_reporting = True
        for device in self.all_local_devices:
            per_disk_stats = existing_data.get(
                'object_reconstruction_per_disk', {}).get(device, {})
            try:
                start_time = per_disk_stats['object_reconstruction_last'] - \
                    (per_disk_stats['object_reconstruction_time'] * 60)
                finish_time = per_disk_stats['object_reconstruction_last']
            except KeyError:
                all_devices_reporting = False
                break
            first_start = min(first_start, start_time)
            last_finish = max(last_finish, finish_time)
        if all_devices_reporting and last_finish > 0:
            duration = last_finish - first_start
            recon_update = {
                'object_reconstruction_time': duration / 60.0,
                'object_reconstruction_last': last_finish
            }
        else:
            # if any current devices have not yet dropped stats, or the rcache
            # file does not yet exist, we may still clear out per device stats
            # for any devices that have been removed from local devices
            recon_update = {}
        found_devices = set(existing_data.get(
            'object_reconstruction_per_disk', {}).keys())
        clear_update = {d: {} for d in found_devices
                        if d not in self.all_local_devices}
        if clear_update:
            recon_update['object_reconstruction_per_disk'] = clear_update
        dump_recon_cache(recon_update, self.rcache, self.logger)

    def load_object_ring(self, policy):
        """
        Make sure the policy's rings are loaded.

        :param policy: the StoragePolicy instance
        :returns: appropriate ring object
        """
        policy.load_ring(self.swift_dir)
        return policy.object_ring

    def check_ring(self, object_ring):
        """
        Check to see if the ring has been updated

        :param object_ring: the ring to check
        :returns: boolean indicating whether or not the ring has changed
        """
        if time.time() > self.next_check:
            self.next_check = time.time() + self.ring_check_interval
            if object_ring.has_changed():
                return False
        return True

    def _get_response(self, node, policy, partition, path, headers):
        """
        Helper method for reconstruction that GETs a single EC fragment
        archive

        :param node: the node to GET from
        :param policy: the job policy
        :param partition: the partition
        :param path: path of the desired EC archive relative to partition dir
        :param headers: the headers to send
        :returns: response
        """
        full_path = _full_path(node, partition, path, policy)
        resp = None
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(
                    node['replication_ip'], node['replication_port'],
                    node['device'], partition, 'GET', path, headers=headers)
            with Timeout(self.node_timeout):
                resp = conn.getresponse()
                resp.full_path = full_path
                resp.node = node
        except (Exception, Timeout):
            self.logger.exception(
                "Trying to GET %(full_path)s", {
                    'full_path': full_path})
        return resp

    def _handle_fragment_response(self, node, policy, partition, fi_to_rebuild,
                                  path, buckets, error_responses, resp):
        """
        Place ok responses into a per-timestamp bucket. Append bad responses to
        a list per-status-code in error_responses.

        :return: the per-timestamp bucket if the response is ok, otherwise
            None.
        """
        if not resp:
            error_responses[UNKNOWN_RESPONSE_STATUS].append(resp)
            return None

        if resp.status not in [HTTP_OK, HTTP_NOT_FOUND]:
            self.logger.warning(
                "Invalid response %(resp)s from %(full_path)s",
                {'resp': resp.status, 'full_path': resp.full_path})
        if resp.status != HTTP_OK:
            error_responses[resp.status].append(resp)
            return None

        resp.headers = HeaderKeyDict(resp.getheaders())
        frag_index = resp.headers.get('X-Object-Sysmeta-Ec-Frag-Index')
        try:
            resp_frag_index = int(frag_index)
        except (TypeError, ValueError):
            # The successful response should include valid X-Object-
            # Sysmeta-Ec-Frag-Index but for safety, catching the case either
            # missing X-Object-Sysmeta-Ec-Frag-Index or invalid frag index to
            # reconstruct and dump warning log for that
            self.logger.warning(
                'Invalid resp from %s '
                '(invalid X-Object-Sysmeta-Ec-Frag-Index: %r)',
                resp.full_path, frag_index)
            error_responses[UNKNOWN_RESPONSE_STATUS].append(resp)
            return None

        timestamp = resp.headers.get('X-Backend-Data-Timestamp',
                                     resp.headers.get('X-Backend-Timestamp'))
        if not timestamp:
            self.logger.warning(
                'Invalid resp from %s, frag index %s (missing '
                'X-Backend-Data-Timestamp and X-Backend-Timestamp)',
                resp.full_path, resp_frag_index)
            error_responses[UNKNOWN_RESPONSE_STATUS].append(resp)
            return None
        timestamp = Timestamp(timestamp)

        etag = resp.headers.get('X-Object-Sysmeta-Ec-Etag')
        if not etag:
            self.logger.warning(
                'Invalid resp from %s, frag index %s (missing Etag)',
                resp.full_path, resp_frag_index)
            error_responses[UNKNOWN_RESPONSE_STATUS].append(resp)
            return None

        bucket = buckets[timestamp]
        bucket.num_responses += 1
        if bucket.etag is None:
            bucket.etag = etag
        elif bucket.etag != etag:
            self.logger.error('Mixed Etag (%s, %s) for %s frag#%s',
                              etag, bucket.etag,
                              _full_path(node, partition, path, policy),
                              fi_to_rebuild)
            return None

        durable_timestamp = resp.headers.get('X-Backend-Durable-Timestamp')
        if durable_timestamp:
            buckets[Timestamp(durable_timestamp)].durable = True

        if resp_frag_index == fi_to_rebuild:
            # TODO: With duplicated EC frags it's not unreasonable to find the
            # very fragment we're trying to rebuild exists on another primary
            # node.  In this case we should stream it directly from the remote
            # node to our target instead of rebuild.  But instead we ignore it.
            self.logger.debug(
                'Found existing frag #%s at %s while rebuilding to %s',
                fi_to_rebuild, resp.full_path,
                _full_path(node, partition, path, policy))
        elif resp_frag_index not in bucket.useful_responses:
            bucket.useful_responses[resp_frag_index] = resp
        # else: duplicate frag_index isn't useful for rebuilding

        return bucket

    def _is_quarantine_candidate(self, policy, buckets, error_responses, df):
        # This condition is deliberately strict because it determines if
        # more requests will be issued and ultimately if the fragment
        # will be quarantined.
        if list(error_responses.keys()) != [404]:
            # only quarantine if all other responses are 404 so we are
            # confident there are no other frags on queried nodes
            return False

        local_timestamp = Timestamp(df.get_datafile_metadata()['X-Timestamp'])
        if list(buckets.keys()) != [local_timestamp]:
            # don't quarantine if there's insufficient other timestamp
            # frags, or no response for the local frag timestamp: we
            # possibly could quarantine, but this unexpected case may be
            # worth more investigation
            return False

        if time.time() - float(local_timestamp) <= self.quarantine_age:
            # If the fragment has not yet passed reclaim age then it is
            # likely that a tombstone will be reverted to this node, or
            # neighbor frags will get reverted from handoffs to *other* nodes
            # and we'll discover we *do* have enough to reconstruct. Don't
            # quarantine it yet: better that it is cleaned up 'normally'.
            return False

        bucket = buckets[local_timestamp]
        return (bucket.num_responses <= self.quarantine_threshold and
                bucket.num_responses < policy.ec_ndata and
                df._frag_index in bucket.useful_responses)

    def _make_fragment_requests(self, job, node, df, buckets, error_responses):
        """
        Issue requests for fragments to the list of ``nodes`` and sort the
        responses into per-timestamp ``buckets`` or per-status
        ``error_responses``. If any bucket accumulates sufficient responses to
        rebuild the missing fragment then return that bucket.

        :param job: job from ssync_sender.
        :param node: node to which we're rebuilding.
        :param df: an instance of :class:`~swift.obj.diskfile.BaseDiskFile`.
        :param buckets: dict of per-timestamp buckets for ok responses.
        :param error_responses: dict of per-status lists of error responses.
        :return: A per-timestamp with sufficient responses, or None if
            there is no such bucket.
        """
        policy = job['policy']
        partition = job['partition']
        datafile_metadata = df.get_datafile_metadata()

        # the fragment index we need to reconstruct is the position index
        # of the node we're rebuilding to within the primary part list
        fi_to_rebuild = node['backend_index']

        # KISS send out connection requests to all nodes, see what sticks.
        # Use fragment preferences header to tell other nodes that we want
        # fragments at the same timestamp as our fragment, and that they don't
        # need to be durable. Accumulate responses into per-timestamp buckets
        # and if any buckets gets enough responses then use those responses to
        # rebuild.
        headers = self.headers.copy()
        headers['X-Backend-Storage-Policy-Index'] = int(policy)
        headers['X-Backend-Replication'] = 'True'
        local_timestamp = Timestamp(datafile_metadata['X-Timestamp'])
        frag_prefs = [{'timestamp': local_timestamp.normal, 'exclude': []}]
        headers['X-Backend-Fragment-Preferences'] = json.dumps(frag_prefs)
        path = datafile_metadata['name']

        ring = policy.object_ring
        primary_nodes = ring.get_part_nodes(partition)
        # primary_node_count is the maximum number of nodes to consume in a
        # normal rebuild attempt when there is no quarantine candidate,
        # including the node to which we are rebuilding
        primary_node_count = len(primary_nodes)
        # don't try and fetch a fragment from the node we're rebuilding to
        filtered_primary_nodes = [n for n in primary_nodes
                                  if n['id'] != node['id']]
        # concurrency is the number of requests fired off in initial batch
        concurrency = len(filtered_primary_nodes)
        # max_node_count is the maximum number of nodes to consume when
        # verifying a quarantine candidate and is at least primary_node_count
        max_node_count = max(primary_node_count,
                             self.request_node_count(primary_node_count))

        pile = GreenAsyncPile(concurrency)
        for primary_node in filtered_primary_nodes:
            pile.spawn(self._get_response, primary_node, policy, partition,
                       path, headers)

        useful_bucket = None
        for resp in pile:
            bucket = self._handle_fragment_response(
                node, policy, partition, fi_to_rebuild, path, buckets,
                error_responses, resp)
            if bucket and len(bucket.useful_responses) >= policy.ec_ndata:
                useful_bucket = bucket
                break

        # Once all rebuild nodes have responded, if we have a quarantine
        # candidate, go beyond primary_node_count and on to handoffs. The
        # first non-404 response will prevent quarantine, but the expected
        # common case is all 404 responses so we use some concurrency to get an
        # outcome faster at the risk of some unnecessary requests in the
        # uncommon case.
        if (not useful_bucket and
                self._is_quarantine_candidate(
                    policy, buckets, error_responses, df)):
            node_count = primary_node_count
            handoff_iter = itertools.islice(ring.get_more_nodes(partition),
                                            max_node_count - node_count)
            for handoff_node in itertools.islice(handoff_iter, concurrency):
                node_count += 1
                pile.spawn(self._get_response, handoff_node, policy, partition,
                           path, headers)
            for resp in pile:
                bucket = self._handle_fragment_response(
                    node, policy, partition, fi_to_rebuild, path, buckets,
                    error_responses, resp)
                if bucket and len(bucket.useful_responses) >= policy.ec_ndata:
                    useful_bucket = bucket
                    self.logger.debug(
                        'Reconstructing frag from handoffs, node_count=%d'
                        % node_count)
                    break
                elif self._is_quarantine_candidate(
                        policy, buckets, error_responses, df):
                    try:
                        handoff_node = next(handoff_iter)
                        node_count += 1
                        pile.spawn(self._get_response, handoff_node, policy,
                                   partition, path, headers)
                    except StopIteration:
                        pass
                # else: this frag is no longer a quarantine candidate, so we
                # could break right here and ignore any remaining responses,
                # but given that we may have actually found another frag we'll
                # optimistically wait for any remaining responses in case a
                # useful bucket is assembled.

        return useful_bucket

    def reconstruct_fa(self, job, node, df):
        """
        Reconstructs a fragment archive - this method is called from ssync
        after a remote node responds that is missing this object - the local
        diskfile is opened to provide metadata - but to reconstruct the
        missing fragment archive we must connect to multiple object servers.

        :param job: job from ssync_sender.
        :param node: node to which we're rebuilding.
        :param df: an instance of :class:`~swift.obj.diskfile.BaseDiskFile`.
        :returns: a DiskFile like class for use by ssync.
        :raises DiskFileQuarantined: if the fragment archive cannot be
            reconstructed and has as a result been quarantined.
        :raises DiskFileError: if the fragment archive cannot be reconstructed.
        """
        policy = job['policy']
        partition = job['partition']
        # the fragment index we need to reconstruct is the position index
        # of the node we're rebuilding to within the primary part list
        fi_to_rebuild = node['backend_index']
        datafile_metadata = df.get_datafile_metadata()
        if not df.validate_metadata():
            raise df._quarantine(
                df._data_file, "Invalid fragment #%s" % df._frag_index)
        local_timestamp = Timestamp(datafile_metadata['X-Timestamp'])
        path = datafile_metadata['name']

        buckets = defaultdict(ResponseBucket)  # map timestamp -> Bucket
        error_responses = defaultdict(list)  # map status code -> response list

        # don't try and fetch a fragment from the node we're rebuilding to
        useful_bucket = self._make_fragment_requests(
            job, node, df, buckets, error_responses)

        if useful_bucket:
            frag_indexes = list(useful_bucket.useful_responses.keys())
            self.logger.debug('Reconstruct frag #%s with frag indexes %s'
                              % (fi_to_rebuild, frag_indexes))
            responses = list(useful_bucket.useful_responses.values())
            rebuilt_fragment_iter = self.make_rebuilt_fragment_iter(
                responses[:policy.ec_ndata], path, policy, fi_to_rebuild)
            return RebuildingECDiskFileStream(datafile_metadata, fi_to_rebuild,
                                              rebuilt_fragment_iter)

        full_path = _full_path(node, partition, path, policy)
        for timestamp, bucket in sorted(buckets.items()):
            self.logger.error(
                'Unable to get enough responses (%s/%s from %s ok responses) '
                'to reconstruct %s %s frag#%s with ETag %s and timestamp %s' %
                (len(bucket.useful_responses), policy.ec_ndata,
                 bucket.num_responses,
                 'durable' if bucket.durable else 'non-durable',
                 full_path, fi_to_rebuild, bucket.etag, timestamp.internal))

        if error_responses:
            durable = buckets[local_timestamp].durable
            errors = ', '.join(
                '%s x %s' % (len(responses),
                             'unknown' if status == UNKNOWN_RESPONSE_STATUS
                             else status)
                for status, responses in sorted(error_responses.items()))
            self.logger.error(
                'Unable to get enough responses (%s error responses) '
                'to reconstruct %s %s frag#%s' % (
                    errors, 'durable' if durable else 'non-durable',
                    full_path, fi_to_rebuild))

        if self._is_quarantine_candidate(policy, buckets, error_responses, df):
            raise df._quarantine(
                df._data_file, "Solitary fragment #%s" % df._frag_index)

        raise DiskFileError('Unable to reconstruct EC archive')

    def _reconstruct(self, policy, fragment_payload, frag_index):
        return policy.pyeclib_driver.reconstruct(fragment_payload,
                                                 [frag_index])[0]

    def make_rebuilt_fragment_iter(self, responses, path, policy, frag_index):
        """
        Turn a set of connections from backend object servers into a generator
        that yields up the rebuilt fragment archive for frag_index.
        """

        def _get_one_fragment(resp):
            buff = []
            remaining_bytes = policy.fragment_size
            while remaining_bytes:
                chunk = resp.read(remaining_bytes)
                if not chunk:
                    break
                remaining_bytes -= len(chunk)
                buff.append(chunk)
            return b''.join(buff)

        def fragment_payload_iter():
            # We need a fragment from each connections, so best to
            # use a GreenPile to keep them ordered and in sync
            pile = GreenPile(len(responses))
            while True:
                for resp in responses:
                    pile.spawn(_get_one_fragment, resp)
                try:
                    with Timeout(self.node_timeout):
                        fragment_payload = [fragment for fragment in pile]
                except (Exception, Timeout):
                    self.logger.exception(
                        "Error trying to rebuild %(path)s "
                        "policy#%(policy)d frag#%(frag_index)s",
                        {'path': path,
                         'policy': policy,
                         'frag_index': frag_index,
                         })
                    break
                if not all(fragment_payload):
                    break
                rebuilt_fragment = self._reconstruct(
                    policy, fragment_payload, frag_index)
                yield rebuilt_fragment

        return fragment_payload_iter()

    def stats_line(self):
        """
        Logs various stats for the currently running reconstruction pass.
        """
        if (self.device_count and self.part_count):
            elapsed = (time.time() - self.start) or 0.000001
            rate = self.reconstruction_part_count / elapsed
            self.logger.info(
                "%(reconstructed)d/%(total)d (%(percentage).2f%%)"
                " partitions reconstructed in %(time).2fs "
                "(%(rate).2f/sec, %(remaining)s remaining)",
                {'reconstructed': self.reconstruction_part_count,
                 'total': self.part_count,
                 'percentage':
                 self.reconstruction_part_count * 100.0 / self.part_count,
                 'time': time.time() - self.start, 'rate': rate,
                 'remaining': '%d%s' %
                 compute_eta(self.start,
                             self.reconstruction_part_count,
                             self.part_count)})

            if self.suffix_count and self.partition_times:
                self.logger.info(
                    "%(checked)d suffixes checked - "
                    "%(hashed).2f%% hashed, %(synced).2f%% synced",
                    {'checked': self.suffix_count,
                     'hashed': (self.suffix_hash * 100.0) / self.suffix_count,
                     'synced': (self.suffix_sync * 100.0) / self.suffix_count})
                self.partition_times.sort()
                self.logger.info(
                    "Partition times: max %(max).4fs, "
                    "min %(min).4fs, med %(med).4fs",
                    {'max': self.partition_times[-1],
                     'min': self.partition_times[0],
                     'med': self.partition_times[
                         len(self.partition_times) // 2]})
        else:
            self.logger.info(
                "Nothing reconstructed for %s seconds.",
                (time.time() - self.start))

    def _emplace_log_prefix(self, worker_index):
        self.logger = get_prefixed_logger(
            self.logger, "[worker %d/%d pid=%s] " % (
                worker_index + 1,
                # use 1-based indexing for more readable logs
                self.reconstructor_workers,
                os.getpid()))

    def kill_coros(self):
        """Utility function that kills all coroutines currently running."""
        for coro in list(self.run_pool.coroutines_running):
            try:
                coro.kill(GreenletExit)
            except GreenletExit:
                pass

    def heartbeat(self):
        """
        Loop that runs in the background during reconstruction.  It
        periodically logs progress.
        """
        while True:
            sleep(self.stats_interval)
            self.stats_line()

    def detect_lockups(self):
        """
        In testing, the pool.waitall() call very occasionally failed to return.
        This is an attempt to make sure the reconstructor finishes its
        reconstruction pass in some eventuality.
        """
        while True:
            sleep(self.lockup_timeout)
            if self.reconstruction_count == self.last_reconstruction_count:
                self.logger.error("Lockup detected.. killing live coros.")
                self.kill_coros()
            self.last_reconstruction_count = self.reconstruction_count

    def _get_hashes(self, device, partition, policy, recalculate=None,
                    do_listdir=False):
        df_mgr = self._df_router[policy]
        hashed, suffix_hashes = tpool.execute(
            df_mgr._get_hashes, device, partition, policy,
            recalculate=recalculate, do_listdir=do_listdir)
        self.logger.update_stats('suffix.hashes', hashed)
        return suffix_hashes

    def get_suffix_delta(self, local_suff, local_index,
                         remote_suff, remote_index):
        """
        Compare the local suffix hashes with the remote suffix hashes
        for the given local and remote fragment indexes.  Return those
        suffixes which should be synced.

        :param local_suff: the local suffix hashes (from _get_hashes)
        :param local_index: the local fragment index for the job
        :param remote_suff: the remote suffix hashes (from remote
                            REPLICATE request)
        :param remote_index: the remote fragment index for the job

        :returns: a list of strings, the suffix dirs to sync
        """
        suffixes = []
        for suffix, sub_dict_local in local_suff.items():
            sub_dict_remote = remote_suff.get(suffix, {})
            if (sub_dict_local.get(None) != sub_dict_remote.get(None) or
                    sub_dict_local.get(local_index) !=
                    sub_dict_remote.get(remote_index)):
                suffixes.append(suffix)
        return suffixes

    def _iter_nodes_for_frag(self, policy, partition, node):
        """
        Generate a priority list of nodes that can sync to the given node.

        The primary node is always the highest priority, after that we'll use
        handoffs.

        To avoid conflicts placing frags we'll skip through the handoffs and
        only yield back those that are offset equal to the given primary
        node index.

        Nodes returned from this iterator will have 'backend_index' set.
        """
        node['backend_index'] = policy.get_backend_index(node['index'])
        yield node
        count = 0
        for handoff_node in policy.object_ring.get_more_nodes(partition):
            handoff_backend_index = policy.get_backend_index(
                handoff_node['handoff_index'])
            if handoff_backend_index == node['backend_index']:
                if (self.rebuild_handoff_node_count >= 0 and
                        count >= self.rebuild_handoff_node_count):
                    break
                handoff_node['backend_index'] = handoff_backend_index
                yield handoff_node
                count += 1

    def _get_suffixes_to_sync(self, job, node):
        """
        For SYNC jobs we need to make a remote REPLICATE request to get
        the remote node's current suffix's hashes and then compare to our
        local suffix's hashes to decide which suffixes (if any) are out
        of sync.

        :param job: the job dict, with the keys defined in ``_get_part_jobs``
        :param node: the remote node dict
        :returns: a (possibly empty) list of strings, the suffixes to be
                  synced and the remote node.
        """
        # get hashes from the remote node
        remote_suffixes = None
        attempts_remaining = 1
        headers = self.headers.copy()
        headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        possible_nodes = self._iter_nodes_for_frag(
            job['policy'], job['partition'], node)
        while remote_suffixes is None and attempts_remaining:
            try:
                node = next(possible_nodes)
            except StopIteration:
                break
            attempts_remaining -= 1
            conn = None
            try:
                with Timeout(self.http_timeout):
                    conn = http_connect(
                        node['replication_ip'], node['replication_port'],
                        node['device'], job['partition'], 'REPLICATE',
                        '', headers=headers)
                    resp = conn.getresponse()
                if resp.status == HTTP_INSUFFICIENT_STORAGE:
                    self.logger.error(
                        '%s responded as unmounted',
                        _full_path(node, job['partition'], '',
                                   job['policy']))
                    attempts_remaining += 1
                elif resp.status != HTTP_OK:
                    full_path = _full_path(node, job['partition'], '',
                                           job['policy'])
                    self.logger.error(
                        "Invalid response %(resp)s from %(full_path)s",
                        {'resp': resp.status, 'full_path': full_path})
                else:
                    remote_suffixes = pickle.loads(resp.read())  # nosec: B301
            except (Exception, Timeout):
                # all exceptions are logged here so that our caller can
                # safely catch our exception and continue to the next node
                # without logging
                self.logger.exception('Unable to get remote suffix hashes '
                                      'from %r' % _full_path(
                                          node, job['partition'], '',
                                          job['policy']))
            finally:
                if conn:
                    conn.close()
        if remote_suffixes is None:
            raise SuffixSyncError('Unable to get remote suffix hashes')

        suffixes = self.get_suffix_delta(job['hashes'],
                                         job['frag_index'],
                                         remote_suffixes,
                                         node['backend_index'])
        # now recalculate local hashes for suffixes that don't
        # match so we're comparing the latest
        local_suff = self._get_hashes(job['local_dev']['device'],
                                      job['partition'],
                                      job['policy'], recalculate=suffixes)

        suffixes = self.get_suffix_delta(local_suff,
                                         job['frag_index'],
                                         remote_suffixes,
                                         node['backend_index'])

        self.suffix_count += len(suffixes)
        return suffixes, node

    def delete_reverted_objs(self, job, objects):
        """
        For EC we can potentially revert only some of a partition
        so we'll delete reverted objects here. Note that we delete
        the fragment index of the file we sent to the remote node.

        :param job: the job being processed
        :param objects: a dict of objects to be deleted, each entry maps
                        hash=>timestamp
        """
        df_mgr = self._df_router[job['policy']]
        suffixes_to_delete = set()
        for object_hash, timestamps in objects.items():
            try:
                df, filenames = df_mgr.get_diskfile_and_filenames_from_hash(
                    job['local_dev']['device'], job['partition'],
                    object_hash, job['policy'],
                    frag_index=job['frag_index'])
                # legacy durable data files look like modern nondurable data
                # files; we therefore override nondurable_purge_delay when we
                # know the data file is durable so that legacy durable data
                # files get purged
                nondurable_purge_delay = (0 if timestamps.get('durable')
                                          else df_mgr.commit_window)
                data_files = [
                    f for f in filenames
                    if f.endswith('.data')]
                purgable_data_files = [
                    f for f in data_files
                    if f.startswith(timestamps['ts_data'].internal)]
                if (job['primary_frag_index'] is None
                        and len(purgable_data_files) == len(data_files) <= 1):
                    # pure handoff node, and we're about to purge the last
                    # .data file, so it's ok to remove any meta file that may
                    # have been reverted
                    meta_timestamp = timestamps.get('ts_meta')
                else:
                    meta_timestamp = None
                df.purge(timestamps['ts_data'], job['frag_index'],
                         nondurable_purge_delay, meta_timestamp)
            except DiskFileNotExist:
                # may have passed reclaim age since being reverted, or may have
                # raced with another reconstructor process trying the same
                pass
            except DiskFileError:
                self.logger.exception(
                    'Unable to purge DiskFile (%r %r %r)',
                    object_hash, timestamps['ts_data'], job['frag_index'])
            suffixes_to_delete.add(object_hash[-3:])

        for suffix in suffixes_to_delete:
            remove_directory(os.path.join(job['path'], suffix))

    def process_job(self, job):
        """
        Sync the local partition with the remote node(s) according to
        the parameters of the job.  For primary nodes, the SYNC job type
        will define both left and right hand sync_to nodes to ssync with
        as defined by this primary nodes index in the node list based on
        the fragment index found in the partition.  For non-primary
        nodes (either handoff revert, or rebalance) the REVERT job will
        define a single node in sync_to which is the proper/new home for
        the fragment index.

        N.B. ring rebalancing can be time consuming and handoff nodes'
        fragment indexes do not have a stable order, it's possible to
        have more than one REVERT job for a partition, and in some rare
        failure conditions there may even also be a SYNC job for the
        same partition - but each one will be processed separately
        because each job will define a separate list of node(s) to
        'sync_to'.

        :param job: the job dict, with the keys defined in ``_get_job_info``
        """
        begin = time.time()
        if job['job_type'] == REVERT:
            self._revert(job, begin)
        else:
            self._sync(job, begin)
        self.partition_times.append(time.time() - begin)
        self.reconstruction_count += 1

    def _sync(self, job, begin):
        """
        Process a SYNC job.
        """
        self.logger.increment(
            'partition.update.count.%s' % (job['local_dev']['device'],))
        for node in job['sync_to']:
            try:
                suffixes, node = self._get_suffixes_to_sync(job, node)
            except SuffixSyncError:
                continue

            if not suffixes:
                continue

            # ssync any out-of-sync suffixes with the remote node; do not limit
            # max_objects - we need to check them all because, unlike a revert
            # job, we don't purge any objects so start with the same set each
            # cycle
            success, _ = ssync_sender(
                self, node, job, suffixes, include_non_durable=False,
                max_objects=0)()
            # update stats for this attempt
            self.suffix_sync += len(suffixes)
            self.logger.update_stats('suffix.syncs', len(suffixes))
        self.logger.timing_since('partition.update.timing', begin)

    def _revert(self, job, begin):
        """
        Process a REVERT job.
        """
        self.logger.increment(
            'partition.delete.count.%s' % (job['local_dev']['device'],))
        syncd_with = 0
        reverted_objs = {}
        try:
            df_mgr = self._df_router[job['policy']]
            # Only object-server can take this lock if an incoming SSYNC is
            # running on the same partition. Taking the lock here ensure we
            # won't enter a race condition where both nodes try to
            # cross-replicate the same partition and both delete it.
            with df_mgr.partition_lock(job['device'], job['policy'],
                                       job['partition'], name='replication',
                                       timeout=0.2):
                limited_by_max_objects = False
                for node in job['sync_to']:
                    node['backend_index'] = job['policy'].get_backend_index(
                        node['index'])
                    sender = ssync_sender(
                        self, node, job, job['suffixes'],
                        include_non_durable=True,
                        max_objects=self.max_objects_per_revert)
                    success, in_sync_objs = sender()
                    limited_by_max_objects |= sender.limited_by_max_objects
                    if success:
                        syncd_with += 1
                        reverted_objs.update(in_sync_objs)
                if syncd_with >= len(job['sync_to']):
                    self.delete_reverted_objs(job, reverted_objs)
                if syncd_with < len(job['sync_to']) or limited_by_max_objects:
                    self.handoffs_remaining += 1
        except PartitionLockTimeout:
            self.logger.info("Unable to lock handoff partition %d for revert "
                             "on device %s policy %d",
                             job['partition'], job['device'], job['policy'])
            self.logger.increment('partition.lock-failure.count')
            self.handoffs_remaining += 1
        self.logger.timing_since('partition.delete.timing', begin)

    def _get_part_jobs(self, local_dev, part_path, partition, policy):
        """
        Helper function to build jobs for a partition, this method will
        read the suffix hashes and create job dictionaries to describe
        the needed work.  There will be one job for each fragment index
        discovered in the partition.

        For a fragment index which corresponds to this node's ring
        index, a job with job_type SYNC will be created to ensure that
        the left and right hand primary ring nodes for the part have the
        corresponding left and right hand fragment archives.

        A fragment index (or entire partition) for which this node is
        not the primary corresponding node, will create job(s) with
        job_type REVERT to ensure that fragment archives are pushed to
        the correct node and removed from this one.

        A partition may result in multiple jobs.  Potentially many
        REVERT jobs, and zero or one SYNC job.

        :param local_dev: the local device (node dict)
        :param part_path: full path to partition
        :param partition: partition number
        :param policy: the policy

        :returns: a list of dicts of job info

        N.B. If this function ever returns an empty list of jobs the entire
        partition will be deleted.
        """
        # find all the fi's in the part, and which suffixes have them
        try:
            hashes = self._get_hashes(local_dev['device'], partition, policy,
                                      do_listdir=True)
        except OSError as e:
            if e.errno != errno.ENOTDIR:
                raise
            self.logger.warning(
                'Unexpected entity %r is not a directory' % part_path)
            return []
        non_data_fragment_suffixes = []
        data_fi_to_suffixes = defaultdict(list)
        for suffix, fi_hash in hashes.items():
            if not fi_hash:
                # this is for sanity and clarity, normally an empty
                # suffix would get del'd from the hashes dict, but an
                # OSError trying to re-hash the suffix could leave the
                # value empty - it will log the exception; but there's
                # no way to properly address this suffix at this time.
                continue
            data_frag_indexes = [f for f in fi_hash if f is not None]
            if not data_frag_indexes:
                non_data_fragment_suffixes.append(suffix)
            else:
                for fi in data_frag_indexes:
                    data_fi_to_suffixes[fi].append(suffix)

        # helper to ensure consistent structure of jobs
        def build_job(job_type, frag_index, suffixes, sync_to,
                      primary_frag_index):
            return {
                'job_type': job_type,
                'frag_index': frag_index,
                'suffixes': suffixes,
                'sync_to': sync_to,
                'partition': partition,
                'path': part_path,
                'hashes': hashes,
                'policy': policy,
                'local_dev': local_dev,
                # ssync likes to have it handy
                'device': local_dev['device'],
                # provide a hint to revert jobs that the node is a primary for
                # one of the frag indexes
                'primary_frag_index': primary_frag_index,
            }

        # aggregate jobs for all the fragment index in this part
        jobs = []

        # check the primary nodes - to see if the part belongs here
        primary_frag_index = None
        part_nodes = policy.object_ring.get_part_nodes(partition)
        for node in part_nodes:
            if node['id'] == local_dev['id']:
                # this partition belongs here, we'll need a sync job
                primary_frag_index = policy.get_backend_index(node['index'])
                try:
                    suffixes = data_fi_to_suffixes.pop(primary_frag_index)
                except KeyError:
                    # N.B. If this function ever returns an empty list of jobs
                    # the entire partition will be deleted.
                    suffixes = []
                sync_job = build_job(
                    job_type=SYNC,
                    frag_index=primary_frag_index,
                    suffixes=suffixes,
                    sync_to=_get_partners(node['index'], part_nodes),
                    primary_frag_index=primary_frag_index
                )
                # ssync callback to rebuild missing fragment_archives
                sync_job['sync_diskfile_builder'] = self.reconstruct_fa
                jobs.append(sync_job)
                break

        # assign remaining data fragment suffixes to revert jobs
        ordered_fis = sorted((len(suffixes), fi) for fi, suffixes
                             in data_fi_to_suffixes.items())
        for count, fi in ordered_fis:
            # In single region EC a revert job must sync to the specific
            # primary who's node_index matches the data's frag_index.  With
            # duplicated EC frags a revert job must sync to all primary nodes
            # that should be holding this frag_index.
            if fi >= len(part_nodes):
                self.logger.warning(
                    'Bad fragment index %r for suffixes %r under %s',
                    fi, data_fi_to_suffixes[fi], part_path)
                continue
            nodes_sync_to = []
            node_index = fi
            for n in range(policy.ec_duplication_factor):
                nodes_sync_to.append(part_nodes[node_index])
                node_index += policy.ec_n_unique_fragments

            revert_job = build_job(
                job_type=REVERT,
                frag_index=fi,
                suffixes=data_fi_to_suffixes[fi],
                sync_to=nodes_sync_to,
                primary_frag_index=primary_frag_index
            )
            jobs.append(revert_job)

        # now we need to assign suffixes that have no data fragments
        if non_data_fragment_suffixes:
            if jobs:
                # the first job will be either the sync_job, or the
                # revert_job for the fragment index that is most common
                # among the suffixes
                jobs[0]['suffixes'].extend(non_data_fragment_suffixes)
            else:
                # this is an unfortunate situation, we need a revert job to
                # push partitions off this node, but none of the suffixes
                # have any data fragments to hint at which node would be a
                # good candidate to receive the tombstones.
                #
                # we'll check a sample of other primaries before we delete our
                # local tombstones, the exact number doesn't matter as long as
                # it's enough to ensure the tombstones are not lost and less
                # than *all the replicas*
                nsample = (policy.ec_n_unique_fragments *
                           policy.ec_duplication_factor) - policy.ec_ndata + 1
                jobs.append(build_job(
                    job_type=REVERT,
                    frag_index=None,
                    suffixes=non_data_fragment_suffixes,
                    sync_to=random.sample(part_nodes, nsample),
                    primary_frag_index=primary_frag_index
                ))
        # return a list of jobs for this part
        return jobs

    def get_policy2devices(self):
        ips = whataremyips(self.ring_ip)
        policy2devices = {}
        for policy in self.policies:
            self.load_object_ring(policy)
            local_devices = [
                dev for dev in policy.object_ring.devs
                if dev and is_local_device(
                    ips, self.port,
                    dev['replication_ip'], dev['replication_port'])]
            policy2devices[policy] = local_devices
        return policy2devices

    def get_local_devices(self):
        """Returns a set of all local devices in all EC policies."""
        policy2devices = self.get_policy2devices()
        local_devices = set()
        for devices in policy2devices.values():
            local_devices.update(d['device'] for d in devices)
        return local_devices

    def collect_parts(self, override_devices=None, override_partitions=None):
        """
        Helper for getting partitions in the top level reconstructor

        In handoffs_only mode primary partitions will not be included in the
        returned (possibly empty) list.
        """
        override_devices = override_devices or []
        override_partitions = override_partitions or []

        policy2devices = self.get_policy2devices()
        all_parts = []

        for policy, local_devices in policy2devices.items():
            # Skip replication if next_part_power is set. In this case
            # every object is hard-linked twice, but the replicator
            # can't detect them and would create a second copy of the
            # file if not yet existing - and this might double the
            # actual transferred and stored data
            next_part_power = getattr(
                policy.object_ring, 'next_part_power', None)
            if next_part_power is not None:
                self.logger.warning(
                    "next_part_power set in policy '%s'. Skipping",
                    policy.name)
                continue

            df_mgr = self._df_router[policy]
            for local_dev in local_devices:
                if override_devices and (
                        local_dev['device'] not in override_devices):
                    continue
                self.device_count += 1
                dev_path = df_mgr.get_dev_path(local_dev['device'])
                if not dev_path:
                    self.logger.warning('%s is not mounted',
                                        local_dev['device'])
                    continue
                data_dir = get_data_dir(policy)
                obj_path = join(dev_path, data_dir)
                tmp_path = join(dev_path, get_tmp_dir(int(policy)))
                unlink_older_than(tmp_path, time.time() -
                                  df_mgr.reclaim_age)
                if not os.path.exists(obj_path):
                    try:
                        mkdirs(obj_path)
                    except Exception:
                        self.logger.exception(
                            'Unable to create %s' % obj_path)
                    continue
                try:
                    partitions = os.listdir(obj_path)
                except OSError:
                    self.logger.exception(
                        'Unable to list partitions in %r' % obj_path)
                    continue

                self.part_count += len(partitions)
                for partition in partitions:
                    part_path = join(obj_path, partition)
                    if (partition.startswith('auditor_status_') and
                            partition.endswith('.json')):
                        # ignore auditor status files
                        continue
                    if not partition.isdigit():
                        self.logger.warning(
                            'Unexpected entity in data dir: %r' % part_path)
                        self.delete_partition(part_path)
                        self.reconstruction_part_count += 1
                        continue
                    partition = int(partition)
                    if override_partitions and (partition not in
                                                override_partitions):
                        continue
                    # N.B. At a primary node in handoffs_only mode may skip to
                    # sync misplaced (handoff) fragments in the primary
                    # partition. That may happen while rebalancing several
                    # times. (e.g. a node holding handoff fragment being a new
                    # primary) Those fragments will be synced (and revert) once
                    # handoffs_only mode turned off.
                    if self.handoffs_only and any(
                            local_dev['id'] == n['id']
                            for n in policy.object_ring.get_part_nodes(
                            partition)):
                        self.logger.debug('Skipping %s job for %s '
                                          'while in handoffs_only mode.',
                                          SYNC, part_path)
                        continue
                    part_info = {
                        'local_dev': local_dev,
                        'policy': policy,
                        'partition': partition,
                        'part_path': part_path,
                    }
                    all_parts.append(part_info)
        random.shuffle(all_parts)
        return all_parts

    def build_reconstruction_jobs(self, part_info):
        """
        Helper function for collect_jobs to build jobs for reconstruction
        using EC style storage policy

        N.B. If this function ever returns an empty list of jobs the entire
        partition will be deleted.
        """
        jobs = self._get_part_jobs(**part_info)
        random.shuffle(jobs)
        self.job_count += len(jobs)
        return jobs

    def _reset_stats(self):
        self.start = time.time()
        self.job_count = 0
        self.part_count = 0
        self.device_count = 0
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.reconstruction_count = 0
        self.reconstruction_part_count = 0
        self.last_reconstruction_count = -1
        self.handoffs_remaining = 0

    def delete_partition(self, path):
        def kill_it(path):
            shutil.rmtree(path, ignore_errors=True)
            remove_file(path)

        self.logger.info("Removing partition: %s", path)
        tpool.execute(kill_it, path)

    def reconstruct(self, **kwargs):
        """Run a reconstruction pass"""
        self._reset_stats()
        self.partition_times = []

        stats = spawn(self.heartbeat)
        lockup_detector = spawn(self.detect_lockups)
        changed_rings = set()

        try:
            self.run_pool = GreenPool(size=self.concurrency)
            for part_info in self.collect_parts(**kwargs):
                sleep()  # Give spawns a cycle
                if part_info['policy'] in changed_rings:
                    continue
                if not self.check_ring(part_info['policy'].object_ring):
                    changed_rings.add(part_info['policy'])
                    self.logger.info(
                        "Ring change detected for policy %d (%s). Aborting "
                        "current reconstruction pass for this policy.",
                        part_info['policy'].idx, part_info['policy'].name)
                    continue

                self.reconstruction_part_count += 1
                jobs = self.build_reconstruction_jobs(part_info)
                if not jobs:
                    # If this part belongs on this node, _get_part_jobs
                    # will *always* build a sync_job - even if there's
                    # no suffixes in the partition that needs to sync.
                    # If there's any suffixes in the partition then our
                    # job list would have *at least* one revert job.
                    # Therefore we know this part a) doesn't belong on
                    # this node and b) doesn't have any suffixes in it.
                    self.run_pool.spawn(self.delete_partition,
                                        part_info['part_path'])
                for job in jobs:
                    self.run_pool.spawn(self.process_job, job)
            with Timeout(self.lockup_timeout):
                self.run_pool.waitall()
        except (Exception, Timeout):
            self.logger.exception("Exception in top-level "
                                  "reconstruction loop")
            self.kill_coros()
        finally:
            stats.kill()
            lockup_detector.kill()
            self.stats_line()
        if self.handoffs_only:
            if self.handoffs_remaining > 0:
                self.logger.info(
                    "Handoffs only mode still has handoffs remaining. "
                    "Next pass will continue to revert handoffs.")
            else:
                self.logger.warning(
                    "Handoffs only mode found no handoffs remaining. "
                    "You should disable handoffs_only once all nodes "
                    "are reporting no handoffs remaining.")

    def final_recon_dump(self, total, override_devices=None, **kwargs):
        """
        Add stats for this worker's run to recon cache.

        When in worker mode (per_disk_stats == True) this worker's stats are
        added per device instead of in the top level keys (aggregation is
        serialized in the parent process).

        :param total: the runtime of cycle in minutes
        :param override_devices: (optional) list of device that are being
            reconstructed
        """
        recon_update = {
            'object_reconstruction_time': total,
            'object_reconstruction_last': time.time(),
        }

        devices = override_devices or self.all_local_devices
        if self.reconstructor_workers > 0 and devices:
            recon_update['pid'] = os.getpid()
            recon_update = {'object_reconstruction_per_disk': {
                d: recon_update for d in devices}}
        else:
            # if not running in worker mode, kill any per_disk stats
            recon_update['object_reconstruction_per_disk'] = {}
        dump_recon_cache(recon_update, self.rcache, self.logger)

    def post_multiprocess_run(self):
        # This method is called after run_once when using multiple workers.
        self.aggregate_recon_update()

    def run_once(self, multiprocess_worker_index=None, *args, **kwargs):
        if multiprocess_worker_index is not None:
            self._emplace_log_prefix(multiprocess_worker_index)
        start = time.time()
        self.logger.info("Running object reconstructor in script mode.")
        override_opts = parse_override_options(once=True, **kwargs)
        self.reconstruct(override_devices=override_opts.devices,
                         override_partitions=override_opts.partitions)
        total = (time.time() - start) / 60
        self.logger.info(
            "Object reconstruction complete (once). (%.02f minutes)", total)
        # Only dump stats if they would actually be meaningful -- i.e. we're
        # collecting per-disk stats and covering all partitions, or we're
        # covering all partitions, all disks.
        if not override_opts.partitions and (
                self.reconstructor_workers > 0 or not override_opts.devices):
            self.final_recon_dump(
                total, override_devices=override_opts.devices,
                override_partitions=override_opts.partitions)

    def run_forever(self, multiprocess_worker_index=None, *args, **kwargs):
        if multiprocess_worker_index is not None:
            self._emplace_log_prefix(multiprocess_worker_index)
        self.logger.info("Starting object reconstructor in daemon mode.")
        # Run the reconstructor continually
        while True:
            start = time.time()
            self.logger.info("Starting object reconstruction pass.")
            override_opts = parse_override_options(**kwargs)
            # Run the reconstructor
            self.reconstruct(override_devices=override_opts.devices,
                             override_partitions=override_opts.partitions)
            total = (time.time() - start) / 60
            self.logger.info(
                "Object reconstruction complete. (%.02f minutes)", total)
            self.final_recon_dump(
                total, override_devices=override_opts.devices,
                override_partitions=override_opts.partitions)
            self.logger.debug('reconstruction sleeping for %s seconds.',
                              self.interval)
            sleep(self.interval)


def main():
    parser = OptionParser("%prog CONFIG [options]")
    parser.add_option('-d', '--devices',
                      help='Reconstruct only given devices. '
                           'Comma-separated list. '
                           'Only has effect if --once is used.')
    parser.add_option('-p', '--partitions',
                      help='Reconstruct only given partitions. '
                           'Comma-separated list. '
                           'Only has effect if --once is used.')
    conf_file, options = parse_options(parser=parser, once=True)
    run_daemon(ObjectReconstructor, conf_file, **options)


if __name__ == '__main__':
    main()
