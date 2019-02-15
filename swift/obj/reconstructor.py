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

import json
import errno
import os
from os.path import join
import random
import time
from collections import defaultdict
import six
import six.moves.cPickle as pickle
import shutil

from eventlet import (GreenPile, GreenPool, Timeout, sleep, tpool, spawn)
from eventlet.support.greenlets import GreenletExit

from swift import gettext_ as _
from swift.common.utils import (
    whataremyips, unlink_older_than, compute_eta, get_logger,
    dump_recon_cache, mkdirs, config_true_value,
    GreenAsyncPile, Timestamp, remove_file,
    load_recon_cache, parse_override_options, distribute_evenly,
    PrefixLoggerAdapter, remove_directory)
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon
from swift.common.ring.utils import is_local_device
from swift.obj.ssync_sender import Sender as ssync_sender
from swift.common.http import HTTP_OK, HTTP_NOT_FOUND, \
    HTTP_INSUFFICIENT_STORAGE
from swift.obj.diskfile import DiskFileRouter, get_data_dir, \
    get_tmp_dir
from swift.common.storage_policy import POLICIES, EC_POLICY
from swift.common.exceptions import ConnectionTimeout, DiskFileError, \
    SuffixSyncError

SYNC, REVERT = ('sync_only', 'sync_revert')


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
    if not isinstance(relative_path, six.text_type):
        relative_path = relative_path.decode('utf8')
    return '%(replication_ip)s:%(replication_port)s' \
        '/%(device)s/%(part)s%(path)s ' \
        'policy#%(policy)d' % {
            'replication_ip': node['replication_ip'],
            'replication_port': node['replication_port'],
            'device': node['device'],
            'part': part, 'path': relative_path,
            'policy': policy,
        }


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
        :param logger: logging object
        """
        self.conf = conf
        self.logger = PrefixLoggerAdapter(
            logger or get_logger(conf, log_route='object-reconstructor'), {})
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.bind_ip = conf.get('bind_ip', '0.0.0.0')
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
        self.stats_interval = int(conf.get('stats_interval', '300'))
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.partition_times = []
        self.interval = int(conf.get('interval') or
                            conf.get('run_pause') or 30)
        if 'run_pause' in conf and 'interval' not in conf:
            self.logger.warning('Option object-reconstructor/run_pause '
                                'is deprecated and will be removed in a '
                                'future version. Update your configuration'
                                ' to use option object-reconstructor/'
                                'interval.')
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")
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
        self._df_router = DiskFileRouter(conf, self.logger)
        self.all_local_devices = self.get_local_devices()

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

    def _get_response(self, node, part, path, headers, full_path):
        """
        Helper method for reconstruction that GETs a single EC fragment
        archive

        :param node: the node to GET from
        :param part: the partition
        :param path: path of the desired EC archive relative to partition dir
        :param headers: the headers to send
        :param full_path: full path to desired EC archive
        :returns: response
        """
        resp = None
        try:
            with ConnectionTimeout(self.conn_timeout):
                conn = http_connect(node['ip'], node['port'], node['device'],
                                    part, 'GET', path, headers=headers)
            with Timeout(self.node_timeout):
                resp = conn.getresponse()
                resp.full_path = full_path
            if resp.status not in [HTTP_OK, HTTP_NOT_FOUND]:
                self.logger.warning(
                    _("Invalid response %(resp)s from %(full_path)s"),
                    {'resp': resp.status, 'full_path': full_path})
                resp = None
            elif resp.status == HTTP_NOT_FOUND:
                resp = None
        except (Exception, Timeout):
            self.logger.exception(
                _("Trying to GET %(full_path)s"), {
                    'full_path': full_path})
        return resp

    def reconstruct_fa(self, job, node, datafile_metadata):
        """
        Reconstructs a fragment archive - this method is called from ssync
        after a remote node responds that is missing this object - the local
        diskfile is opened to provide metadata - but to reconstruct the
        missing fragment archive we must connect to multiple object servers.

        :param job: job from ssync_sender
        :param node: node that we're rebuilding to
        :param datafile_metadata:  the datafile metadata to attach to
                                   the rebuilt fragment archive
        :returns: a DiskFile like class for use by ssync
        :raises DiskFileError: if the fragment archive cannot be reconstructed
        """
        # don't try and fetch a fragment from the node we're rebuilding to
        part_nodes = [n for n in job['policy'].object_ring.get_part_nodes(
            job['partition']) if n['id'] != node['id']]

        # the fragment index we need to reconstruct is the position index
        # of the node we're rebuilding to within the primary part list
        fi_to_rebuild = node['backend_index']

        # KISS send out connection requests to all nodes, see what sticks.
        # Use fragment preferences header to tell other nodes that we want
        # fragments at the same timestamp as our fragment, and that they don't
        # need to be durable.
        headers = self.headers.copy()
        headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        headers['X-Backend-Replication'] = 'True'
        frag_prefs = [{'timestamp': datafile_metadata['X-Timestamp'],
                       'exclude': []}]
        headers['X-Backend-Fragment-Preferences'] = json.dumps(frag_prefs)
        pile = GreenAsyncPile(len(part_nodes))
        path = datafile_metadata['name']
        for _node in part_nodes:
            full_get_path = _full_path(
                _node, job['partition'], path, job['policy'])
            pile.spawn(self._get_response, _node, job['partition'],
                       path, headers, full_get_path)

        buckets = defaultdict(dict)
        etag_buckets = {}
        error_resp_count = 0
        for resp in pile:
            if not resp:
                error_resp_count += 1
                continue
            resp.headers = HeaderKeyDict(resp.getheaders())
            frag_index = resp.headers.get('X-Object-Sysmeta-Ec-Frag-Index')
            try:
                resp_frag_index = int(frag_index)
            except (TypeError, ValueError):
                # The successful response should include valid X-Object-
                # Sysmeta-Ec-Frag-Index but for safety, catching the case
                # either missing X-Object-Sysmeta-Ec-Frag-Index or invalid
                # frag index to reconstruct and dump warning log for that
                self.logger.warning(
                    'Invalid resp from %s '
                    '(invalid X-Object-Sysmeta-Ec-Frag-Index: %r)',
                    resp.full_path, frag_index)
                continue

            if fi_to_rebuild == resp_frag_index:
                # TODO: With duplicated EC frags it's not unreasonable to find
                # the very fragment we're trying to rebuild exists on another
                # primary node.  In this case we should stream it directly from
                # the remote node to our target instead of rebuild.  But
                # instead we ignore it.
                self.logger.debug(
                    'Found existing frag #%s at %s while rebuilding to %s',
                    fi_to_rebuild, resp.full_path,
                    _full_path(
                        node, job['partition'], datafile_metadata['name'],
                        job['policy']))
                continue

            timestamp = resp.headers.get('X-Backend-Timestamp')
            if not timestamp:
                self.logger.warning('Invalid resp from %s, frag index %s '
                                    '(missing X-Backend-Timestamp)',
                                    resp.full_path, resp_frag_index)
                continue
            timestamp = Timestamp(timestamp)

            etag = resp.headers.get('X-Object-Sysmeta-Ec-Etag')
            if not etag:
                self.logger.warning('Invalid resp from %s, frag index %s '
                                    '(missing Etag)',
                                    resp.full_path, resp_frag_index)
                continue

            if etag != etag_buckets.setdefault(timestamp, etag):
                self.logger.error(
                    'Mixed Etag (%s, %s) for %s frag#%s',
                    etag, etag_buckets[timestamp],
                    _full_path(node, job['partition'],
                               datafile_metadata['name'], job['policy']),
                    fi_to_rebuild)
                continue

            if resp_frag_index not in buckets[timestamp]:
                buckets[timestamp][resp_frag_index] = resp
                if len(buckets[timestamp]) >= job['policy'].ec_ndata:
                    responses = buckets[timestamp].values()
                    self.logger.debug(
                        'Reconstruct frag #%s with frag indexes %s'
                        % (fi_to_rebuild, list(buckets[timestamp])))
                    break
        else:
            for timestamp, resp in sorted(buckets.items()):
                etag = etag_buckets[timestamp]
                self.logger.error(
                    'Unable to get enough responses (%s/%s) '
                    'to reconstruct %s frag#%s with ETag %s' % (
                        len(resp), job['policy'].ec_ndata,
                        _full_path(node, job['partition'],
                                   datafile_metadata['name'],
                                   job['policy']),
                        fi_to_rebuild, etag))

            if error_resp_count:
                self.logger.error(
                    'Unable to get enough responses (%s error responses) '
                    'to reconstruct %s frag#%s' % (
                        error_resp_count,
                        _full_path(node, job['partition'],
                                   datafile_metadata['name'],
                                   job['policy']),
                        fi_to_rebuild))

            raise DiskFileError('Unable to reconstruct EC archive')

        rebuilt_fragment_iter = self.make_rebuilt_fragment_iter(
            responses[:job['policy'].ec_ndata], path, job['policy'],
            fi_to_rebuild)
        return RebuildingECDiskFileStream(datafile_metadata, fi_to_rebuild,
                                          rebuilt_fragment_iter)

    def _reconstruct(self, policy, fragment_payload, frag_index):
        return policy.pyeclib_driver.reconstruct(fragment_payload,
                                                 [frag_index])[0]

    def make_rebuilt_fragment_iter(self, responses, path, policy, frag_index):
        """
        Turn a set of connections from backend object servers into a generator
        that yields up the rebuilt fragment archive for frag_index.
        """

        def _get_one_fragment(resp):
            buff = ''
            remaining_bytes = policy.fragment_size
            while remaining_bytes:
                chunk = resp.read(remaining_bytes)
                if not chunk:
                    break
                remaining_bytes -= len(chunk)
                buff += chunk
            return buff

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
                        _("Error trying to rebuild %(path)s "
                          "policy#%(policy)d frag#%(frag_index)s"),
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
                _("%(reconstructed)d/%(total)d (%(percentage).2f%%)"
                  " partitions reconstructed in %(time).2fs "
                  "(%(rate).2f/sec, %(remaining)s remaining)"),
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
                    _("%(checked)d suffixes checked - "
                      "%(hashed).2f%% hashed, %(synced).2f%% synced"),
                    {'checked': self.suffix_count,
                     'hashed': (self.suffix_hash * 100.0) / self.suffix_count,
                     'synced': (self.suffix_sync * 100.0) / self.suffix_count})
                self.partition_times.sort()
                self.logger.info(
                    _("Partition times: max %(max).4fs, "
                      "min %(min).4fs, med %(med).4fs"),
                    {'max': self.partition_times[-1],
                     'min': self.partition_times[0],
                     'med': self.partition_times[
                         len(self.partition_times) // 2]})
        else:
            self.logger.info(
                _("Nothing reconstructed for %s seconds."),
                (time.time() - self.start))

    def _emplace_log_prefix(self, worker_index):
        self.logger.set_prefix("[worker %d/%d pid=%s] " % (
            worker_index + 1,  # use 1-based indexing for more readable logs
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
                self.logger.error(_("Lockup detected.. killing live coros."))
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

    def rehash_remote(self, node, job, suffixes):
        headers = self.headers.copy()
        headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        try:
            with Timeout(self.http_timeout):
                conn = http_connect(
                    node['replication_ip'], node['replication_port'],
                    node['device'], job['partition'], 'REPLICATE',
                    '/' + '-'.join(sorted(suffixes)),
                    headers=headers)
                conn.getresponse().read()
        except (Exception, Timeout):
            self.logger.exception(
                _("Trying to sync suffixes with %s") % _full_path(
                    node, job['partition'], '', job['policy']))

    def _iter_nodes_for_frag(self, policy, partition, node):
        """
        Generate a priority list of nodes that can sync to the given node.

        The primary node is always the highest priority, after that we'll use
        handoffs.

        To avoid conflicts placing frags we'll skip through the handoffs and
        only yield back those that are offset equal to to the given primary
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

        :param: the job dict, with the keys defined in ``_get_part_jobs``
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
            try:
                with Timeout(self.http_timeout):
                    resp = http_connect(
                        node['replication_ip'], node['replication_port'],
                        node['device'], job['partition'], 'REPLICATE',
                        '', headers=headers).getresponse()
                if resp.status == HTTP_INSUFFICIENT_STORAGE:
                    self.logger.error(
                        _('%s responded as unmounted'),
                        _full_path(node, job['partition'], '',
                                   job['policy']))
                    attempts_remaining += 1
                elif resp.status != HTTP_OK:
                    full_path = _full_path(node, job['partition'], '',
                                           job['policy'])
                    self.logger.error(
                        _("Invalid response %(resp)s from %(full_path)s"),
                        {'resp': resp.status, 'full_path': full_path})
                else:
                    remote_suffixes = pickle.loads(resp.read())
            except (Exception, Timeout):
                # all exceptions are logged here so that our caller can
                # safely catch our exception and continue to the next node
                # without logging
                self.logger.exception('Unable to get remote suffix hashes '
                                      'from %r' % _full_path(
                                          node, job['partition'], '',
                                          job['policy']))
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

    def delete_reverted_objs(self, job, objects, frag_index):
        """
        For EC we can potentially revert only some of a partition
        so we'll delete reverted objects here. Note that we delete
        the fragment index of the file we sent to the remote node.

        :param job: the job being processed
        :param objects: a dict of objects to be deleted, each entry maps
                        hash=>timestamp
        :param frag_index: (int) the fragment index of data files to be deleted
        """
        df_mgr = self._df_router[job['policy']]
        suffixes_to_delete = set()
        for object_hash, timestamps in objects.items():
            try:
                df = df_mgr.get_diskfile_from_hash(
                    job['local_dev']['device'], job['partition'],
                    object_hash, job['policy'],
                    frag_index=frag_index)
                df.purge(timestamps['ts_data'], frag_index)
            except DiskFileError:
                self.logger.exception(
                    'Unable to purge DiskFile (%r %r %r)',
                    object_hash, timestamps['ts_data'], frag_index)
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

        :param: the job dict, with the keys defined in ``_get_job_info``
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

            # ssync any out-of-sync suffixes with the remote node
            success, _ = ssync_sender(
                self, node, job, suffixes)()
            # let remote end know to rehash it's suffixes
            self.rehash_remote(node, job, suffixes)
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
        for node in job['sync_to']:
            node['backend_index'] = job['policy'].get_backend_index(
                node['index'])
            success, in_sync_objs = ssync_sender(
                self, node, job, job['suffixes'])()
            if success:
                self.rehash_remote(node, job, job['suffixes'])
                syncd_with += 1
                reverted_objs.update(in_sync_objs)
        if syncd_with >= len(job['sync_to']):
            self.delete_reverted_objs(
                job, reverted_objs, job['frag_index'])
        else:
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
        def build_job(job_type, frag_index, suffixes, sync_to):
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
            }

        # aggregate jobs for all the fragment index in this part
        jobs = []

        # check the primary nodes - to see if the part belongs here
        part_nodes = policy.object_ring.get_part_nodes(partition)
        for node in part_nodes:
            if node['id'] == local_dev['id']:
                # this partition belongs here, we'll need a sync job
                frag_index = policy.get_backend_index(node['index'])
                try:
                    suffixes = data_fi_to_suffixes.pop(frag_index)
                except KeyError:
                    # N.B. If this function ever returns an empty list of jobs
                    # the entire partition will be deleted.
                    suffixes = []
                sync_job = build_job(
                    job_type=SYNC,
                    frag_index=frag_index,
                    suffixes=suffixes,
                    sync_to=_get_partners(node['index'], part_nodes),
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
                    sync_to=random.sample(part_nodes, nsample)
                ))
        # return a list of jobs for this part
        return jobs

    def get_policy2devices(self):
        ips = whataremyips(self.bind_ip)
        policy2devices = {}
        for policy in self.policies:
            self.load_object_ring(policy)
            local_devices = list(six.moves.filter(
                lambda dev: dev and is_local_device(
                    ips, self.port,
                    dev['replication_ip'], dev['replication_port']),
                policy.object_ring.devs))
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
                    _("next_part_power set in policy '%s'. Skipping"),
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
                    self.logger.warning(_('%s is not mounted'),
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

        self.logger.info(_("Removing partition: %s"), path)
        tpool.execute(kill_it, path)

    def reconstruct(self, **kwargs):
        """Run a reconstruction pass"""
        self._reset_stats()
        self.partition_times = []

        stats = spawn(self.heartbeat)
        lockup_detector = spawn(self.detect_lockups)

        try:
            self.run_pool = GreenPool(size=self.concurrency)
            for part_info in self.collect_parts(**kwargs):
                sleep()  # Give spawns a cycle
                if not self.check_ring(part_info['policy'].object_ring):
                    self.logger.info(_("Ring change detected. Aborting "
                                       "current reconstruction pass."))
                    return

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
            self.logger.exception(_("Exception in top-level "
                                    "reconstruction loop"))
            self.kill_coros()
        finally:
            stats.kill()
            lockup_detector.kill()
            self.stats_line()
        if self.handoffs_only:
            if self.handoffs_remaining > 0:
                self.logger.info(_(
                    "Handoffs only mode still has handoffs remaining. "
                    "Next pass will continue to revert handoffs."))
            else:
                self.logger.warning(_(
                    "Handoffs only mode found no handoffs remaining. "
                    "You should disable handoffs_only once all nodes "
                    "are reporting no handoffs remaining."))

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
        self.logger.info(_("Running object reconstructor in script mode."))
        override_opts = parse_override_options(once=True, **kwargs)
        self.reconstruct(override_devices=override_opts.devices,
                         override_partitions=override_opts.partitions)
        total = (time.time() - start) / 60
        self.logger.info(
            _("Object reconstruction complete (once). (%.02f minutes)"), total)
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
        self.logger.info(_("Starting object reconstructor in daemon mode."))
        # Run the reconstructor continually
        while True:
            start = time.time()
            self.logger.info(_("Starting object reconstruction pass."))
            override_opts = parse_override_options(**kwargs)
            # Run the reconstructor
            self.reconstruct(override_devices=override_opts.devices,
                             override_partitions=override_opts.partitions)
            total = (time.time() - start) / 60
            self.logger.info(
                _("Object reconstruction complete. (%.02f minutes)"), total)
            self.final_recon_dump(
                total, override_devices=override_opts.devices,
                override_partitions=override_opts.partitions)
            self.logger.debug('reconstruction sleeping for %s seconds.',
                              self.interval)
            sleep(self.interval)
