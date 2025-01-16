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

from collections import defaultdict
from optparse import OptionParser
import os
import errno
from os.path import isdir, isfile, join, dirname
import random
import shutil
import time
import itertools
import pickle  # nosec: B403

import eventlet
from eventlet import GreenPool, queue, tpool, Timeout, sleep
from eventlet.green import subprocess

from swift.common.constraints import check_drive
from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, unlink_older_than, \
    compute_eta, get_logger, dump_recon_cache, parse_options, \
    rsync_module_interpolation, mkdirs, config_true_value, \
    config_auto_int_value, storage_directory, load_recon_cache, EUCLEAN, \
    parse_override_options, distribute_evenly, listdir, node_to_string, \
    get_prefixed_logger
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon, run_daemon
from swift.common.http import HTTP_OK, HTTP_INSUFFICIENT_STORAGE
from swift.common.recon import RECON_OBJECT_FILE, DEFAULT_RECON_CACHE_PATH
from swift.obj import ssync_sender
from swift.obj.diskfile import get_data_dir, get_tmp_dir, DiskFileRouter
from swift.common.storage_policy import POLICIES, REPL_POLICY
from swift.common.exceptions import PartitionLockTimeout

DEFAULT_RSYNC_TIMEOUT = 900


def _do_listdir(partition, replication_cycle):
    return (((partition + replication_cycle) % 10) == 0)


class Stats(object):
    fields = ['attempted', 'failure', 'hashmatch', 'remove', 'rsync',
              'success', 'suffix_count', 'suffix_hash', 'suffix_sync',
              'failure_nodes']

    @classmethod
    def from_recon(cls, dct):
        return cls(**{k: v for k, v in dct.items() if k in cls.fields})

    def to_recon(self):
        return {k: getattr(self, k) for k in self.fields}

    def __init__(self, attempted=0, failure=0, hashmatch=0, remove=0, rsync=0,
                 success=0, suffix_count=0, suffix_hash=0,
                 suffix_sync=0, failure_nodes=None):
        self.attempted = attempted
        self.failure = failure
        self.hashmatch = hashmatch
        self.remove = remove
        self.rsync = rsync
        self.success = success
        self.suffix_count = suffix_count
        self.suffix_hash = suffix_hash
        self.suffix_sync = suffix_sync
        self.failure_nodes = defaultdict(lambda: defaultdict(int),
                                         (failure_nodes or {}))

    def __add__(self, other):
        total = type(self)()
        total.attempted = self.attempted + other.attempted
        total.failure = self.failure + other.failure
        total.hashmatch = self.hashmatch + other.hashmatch
        total.remove = self.remove + other.remove
        total.rsync = self.rsync + other.rsync
        total.success = self.success + other.success
        total.suffix_count = self.suffix_count + other.suffix_count
        total.suffix_hash = self.suffix_hash + other.suffix_hash
        total.suffix_sync = self.suffix_sync + other.suffix_sync

        all_failed_ips = (set(list(self.failure_nodes.keys()) +
                              list(other.failure_nodes.keys())))
        for ip in all_failed_ips:
            self_devs = self.failure_nodes.get(ip, {})
            other_devs = other.failure_nodes.get(ip, {})
            this_ip_failures = {}
            for dev in set(list(self_devs.keys()) + list(other_devs.keys())):
                this_ip_failures[dev] = (
                    self_devs.get(dev, 0) + other_devs.get(dev, 0))
            total.failure_nodes[ip] = this_ip_failures
        return total

    def add_failure_stats(self, failures):
        """
        Note the failure of one or more devices.

        :param failures: a list of (ip, device-name) pairs that failed
        """
        self.failure += len(failures)
        for ip, device in failures:
            self.failure_nodes[ip][device] += 1


class ObjectReplicator(Daemon):
    """
    Replicate objects.

    Encapsulates most logic and data needed by the object replication process.
    Each call to .replicate() performs one replication pass.  It's up to the
    caller to do this in a loop.
    """

    def __init__(self, conf, logger=None):
        """
        :param conf: configuration object obtained from ConfigParser
        :param logger: an instance of ``SwiftLogAdapter``.
        """
        self.conf = conf
        self.logger = \
            logger or get_logger(conf, log_route='object-replicator')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring_ip = conf.get('ring_ip', conf.get('bind_ip', '0.0.0.0'))
        self.servers_per_port = int(conf.get('servers_per_port', '0') or 0)
        self.port = None if self.servers_per_port else \
            int(conf.get('bind_port', 6200))
        self.concurrency = int(conf.get('concurrency', 1))
        self.replicator_workers = int(conf.get('replicator_workers', 0))
        self.policies = [policy for policy in POLICIES
                         if policy.policy_type == REPL_POLICY]
        self.stats_interval = float(conf.get('stats_interval', '300'))
        self.ring_check_interval = float(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.replication_cycle = random.randint(0, 9)
        self.partition_times = []
        self.interval = float(conf.get('interval') or
                              conf.get('run_pause') or 30)
        if 'run_pause' in conf:
            if 'interval' in conf:
                self.logger.warning(
                    'Option object-replicator/run_pause is deprecated and '
                    'object-replicator/interval is already configured. You '
                    'can safely remove run_pause; it is now ignored and will '
                    'be removed in a future version.')
            else:
                self.logger.warning(
                    'Option object-replicator/run_pause is deprecated and '
                    'will be removed in a future version. Update your '
                    'configuration to use option object-replicator/interval.')
        self.rsync_timeout = int(conf.get('rsync_timeout',
                                          DEFAULT_RSYNC_TIMEOUT))
        self.rsync_io_timeout = conf.get('rsync_io_timeout', '30')
        self.rsync_bwlimit = conf.get('rsync_bwlimit', '0')
        self.rsync_compress = config_true_value(
            conf.get('rsync_compress', 'no'))
        self.rsync_module = conf.get('rsync_module', '').rstrip('/')
        if not self.rsync_module:
            self.rsync_module = '{replication_ip}::object'
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.rcache = os.path.join(self.recon_cache_path, RECON_OBJECT_FILE)
        self._next_rcache_update = time.time() + self.stats_interval
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.sync_method = getattr(self, conf.get('sync_method') or 'rsync')
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.default_headers = {
            'Content-Length': '0',
            'user-agent': 'object-replicator %s' % os.getpid()}
        self.log_rsync_transfers = config_true_value(
            conf.get('log_rsync_transfers', True))
        self.rsync_error_log_line_length = \
            int(conf.get('rsync_error_log_line_length', 0))
        self.handoffs_first = config_true_value(conf.get('handoffs_first',
                                                         False))
        self.handoff_delete = config_auto_int_value(
            conf.get('handoff_delete', 'auto'), 0)
        if any((self.handoff_delete, self.handoffs_first)):
            self.logger.warning('Handoff only mode is not intended for normal '
                                'operation, please disable handoffs_first and '
                                'handoff_delete before the next '
                                'normal rebalance')
        if all(self.load_object_ring(p).replica_count <= self.handoff_delete
               for p in self.policies):
            self.logger.warning('No storage policies found for which '
                                'handoff_delete=%d would have an effect. '
                                'Disabling.', self.handoff_delete)
            self.handoff_delete = 0
        self.is_multiprocess_worker = None
        self._df_router = DiskFileRouter(conf, self.logger)
        self._child_process_reaper_queue = queue.LightQueue()
        self.rings_mtime = None

    def _zero_stats(self):
        self.stats_for_dev = defaultdict(Stats)

    @property
    def total_stats(self):
        return sum(self.stats_for_dev.values(), Stats())

    def _emplace_log_prefix(self, worker_index):
        self.logger = get_prefixed_logger(
            self.logger, "[worker %d/%d pid=%d] " % (
                worker_index + 1,
                # use 1-based indexing for more readable logs
                self.replicator_workers,
                os.getpid()))

    def _child_process_reaper(self):
        """
        Consume processes from self._child_process_reaper_queue and wait() for
        them
        """
        procs = set()
        done = False
        while not done:
            timeout = 60 if procs else None
            try:
                new_proc = self._child_process_reaper_queue.get(
                    timeout=timeout)
                if new_proc is not None:
                    procs.add(new_proc)
                else:
                    done = True
            except queue.Empty:
                pass

            reaped_procs = set()
            for proc in procs:
                # this will reap the process if it has exited, but
                # otherwise will not wait
                if proc.poll() is not None:
                    reaped_procs.add(proc)
            procs -= reaped_procs

    def get_worker_args(self, once=False, **kwargs):
        if self.replicator_workers < 1:
            return []

        override_opts = parse_override_options(once=once, **kwargs)
        have_overrides = bool(override_opts.devices or override_opts.partitions
                              or override_opts.policies)

        # save this off for ring-change detection later in is_healthy()
        self.all_local_devices = self.get_local_devices()

        if override_opts.devices:
            devices_to_replicate = [
                d for d in override_opts.devices
                if d in self.all_local_devices]
        else:
            # The sort isn't strictly necessary since we're just trying to
            # spread devices around evenly, but it makes testing easier.
            devices_to_replicate = sorted(self.all_local_devices)

        # Distribute devices among workers as evenly as possible
        self.replicator_workers = min(self.replicator_workers,
                                      len(devices_to_replicate))
        return [{'override_devices': devs,
                 'override_partitions': override_opts.partitions,
                 'override_policies': override_opts.policies,
                 'have_overrides': have_overrides,
                 'multiprocess_worker_index': index}
                for index, devs in enumerate(
                    distribute_evenly(devices_to_replicate,
                                      self.replicator_workers))]

    def is_healthy(self):
        """
        Check whether our set of local devices remains the same.

        If devices have been added or removed, then we return False here so
        that we can kill off any worker processes and then distribute the
        new set of local devices across a new set of workers so that all
        devices are, once again, being worked on.

        This function may also cause recon stats to be updated.

        :returns: False if any local devices have been added or removed,
          True otherwise
        """
        # We update recon here because this is the only function we have in
        # a multiprocess replicator that gets called periodically in the
        # parent process.
        if time.time() >= self._next_rcache_update:
            update = self.aggregate_recon_update()
            dump_recon_cache(update, self.rcache, self.logger)
        rings_mtime = [os.path.getmtime(self.load_object_ring(
                       policy).serialized_path) for policy in self.policies]
        if self.rings_mtime == rings_mtime:
            return True
        self.rings_mtime = rings_mtime
        return self.get_local_devices() == self.all_local_devices

    def get_local_devices(self):
        """
        Returns a set of all local devices in all replication-type storage
        policies.

        This is the device names, e.g. "sdq" or "d1234" or something, not
        the full ring entries.
        """
        ips = whataremyips(self.ring_ip)
        local_devices = set()
        for policy in self.policies:
            self.load_object_ring(policy)
            for device in policy.object_ring.devs:
                if device and is_local_device(
                        ips, self.port,
                        device['replication_ip'],
                        device['replication_port']):
                    local_devices.add(device['device'])
        return local_devices

    # Just exists for doc anchor point
    def sync(self, node, job, suffixes, *args, **kwargs):
        """
        Synchronize local suffix directories from a partition with a remote
        node.

        :param node: the "dev" entry for the remote node to sync with
        :param job: information about the partition being synced
        :param suffixes: a list of suffixes which need to be pushed

        :returns: boolean and dictionary, boolean indicating success or failure
        """
        return self.sync_method(node, job, suffixes, *args, **kwargs)

    def load_object_ring(self, policy):
        """
        Make sure the policy's rings are loaded.

        :param policy: the StoragePolicy instance
        :returns: appropriate ring object
        """
        policy.load_ring(self.swift_dir)
        return policy.object_ring

    def _limit_rsync_log(self, line):
        """
        If rsync_error_log_line_length is defined then
        limit the error to that length

        :param line: rsync log line
        :return: If enabled the line limited to rsync_error_log_line_length
                 otherwise the initial line.
        """
        if self.rsync_error_log_line_length:
            return line[:self.rsync_error_log_line_length]

        return line

    def _rsync(self, args):
        """
        Execute the rsync binary to replicate a partition.

        :returns: return code of rsync process. 0 is successful
        """
        start_time = time.time()
        proc = None

        try:
            with Timeout(self.rsync_timeout):
                proc = subprocess.Popen(args,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
                results = proc.stdout.read()
                ret_val = proc.wait()
        except Timeout:
            self.logger.error(
                self._limit_rsync_log(
                    "Killing long-running rsync after %ds: %s" % (
                        self.rsync_timeout, str(args))))
            if proc:
                proc.kill()
                try:
                    # Note: Python 2.7's subprocess.Popen class doesn't take
                    # any arguments for wait(), but Python 3's does.
                    # However, Eventlet's replacement Popen takes a timeout
                    # argument regardless of Python version, so we don't
                    # need any conditional code here.
                    proc.wait(timeout=1.0)
                except subprocess.TimeoutExpired:
                    # Sometimes a process won't die immediately even after a
                    # SIGKILL. This can be due to failing disks, high load,
                    # or other reasons. We can't wait for it forever since
                    # we're taking up a slot in the (green)thread pool, so
                    # we send it over to another greenthread, not part of
                    # our pool, whose sole duty is to wait for child
                    # processes to exit.
                    self._child_process_reaper_queue.put(proc)
            return 1  # failure response code

        total_time = time.time() - start_time
        for result in results.decode('utf8').split('\n'):
            if result == '':
                continue
            if result.startswith('cd+'):
                continue
            if result.startswith('<') and not self.log_rsync_transfers:
                continue
            if not ret_val:
                self.logger.debug(result)
            else:
                self.logger.error(result)
        if ret_val:
            self.logger.error(
                self._limit_rsync_log(
                    'Bad rsync return code: %(ret)d <- %(args)s' %
                    {'args': str(args), 'ret': ret_val}))
        else:
            log_method = self.logger.info if results else self.logger.debug
            log_method(
                "Successful rsync of %(src)s to %(dst)s (%(time).03f)",
                {'src': args[-2][:-3] + '...', 'dst': args[-1],
                 'time': total_time})
        return ret_val

    def rsync(self, node, job, suffixes):
        """
        Uses rsync to implement the sync method. This was the first
        sync method in Swift.
        """
        if not os.path.exists(job['path']):
            return False, {}
        args = [
            'rsync',
            '--recursive',
            '--whole-file',
            '--human-readable',
            '--xattrs',
            '--itemize-changes',
            '--ignore-existing',
            '--timeout=%s' % self.rsync_io_timeout,
            '--contimeout=%s' % self.rsync_io_timeout,
            '--bwlimit=%s' % self.rsync_bwlimit,
            '--exclude=.*.%s' % ''.join('[0-9a-zA-Z]' for i in range(6))
        ]
        if self.rsync_compress and \
                job['region'] != node['region']:
            # Allow for compression, but only if the remote node is in
            # a different region than the local one.
            args.append('--compress')
        rsync_module = rsync_module_interpolation(self.rsync_module, node)
        had_any = False
        for suffix in suffixes:
            spath = join(job['path'], suffix)
            if os.path.exists(spath):
                args.append(spath)
                had_any = True
        if not had_any:
            return False, {}
        data_dir = get_data_dir(job['policy'])
        args.append(join(rsync_module, node['device'],
                    data_dir, job['partition']))
        success = (self._rsync(args) == 0)

        # TODO: Catch and swallow (or at least minimize) timeouts when doing
        # an update job; if we don't manage to notify the remote, we should
        # catch it on the next pass
        if success or not job['delete']:
            headers = dict(self.default_headers)
            headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
            with Timeout(self.http_timeout):
                conn = http_connect(
                    node['replication_ip'], node['replication_port'],
                    node['device'], job['partition'], 'REPLICATE',
                    '/' + '-'.join(suffixes), headers=headers)
                try:
                    conn.getresponse().read()
                finally:
                    conn.close()
        return success, {}

    def ssync(self, node, job, suffixes, remote_check_objs=None):
        return ssync_sender.Sender(
            self, node, job, suffixes, remote_check_objs)()

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

    def revert(self, job):
        """
        High-level method that replicates a single partition that doesn't
        belong on this node.

        :param job: a dict containing info about the partition to be replicated
        """

        def tpool_get_suffixes(path):
            return [suff for suff in listdir(path)
                    if len(suff) == 3 and isdir(join(path, suff))]

        stats = self.stats_for_dev[job['device']]
        stats.attempted += 1
        self.logger.increment('partition.delete.count.%s' % (job['device'],))
        headers = dict(self.default_headers)
        headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        failure_devs_info = set()
        begin = time.time()
        handoff_partition_deleted = False
        try:
            df_mgr = self._df_router[job['policy']]
            # Only object-server can take this lock if an incoming SSYNC is
            # running on the same partition. Taking the lock here ensure we
            # won't enter a race condition where both nodes try to
            # cross-replicate the same partition and both delete it.
            with df_mgr.partition_lock(job['device'], job['policy'],
                                       job['partition'], name='replication',
                                       timeout=0.2):
                responses = []
                suffixes = tpool.execute(tpool_get_suffixes, job['path'])
                synced_remote_regions = {}
                delete_objs = None
                if suffixes:
                    for node in job['nodes']:
                        stats.rsync += 1
                        kwargs = {}
                        if self.conf.get('sync_method', 'rsync') == 'ssync' \
                                and node['region'] in synced_remote_regions:
                            kwargs['remote_check_objs'] = \
                                synced_remote_regions[node['region']]
                        # candidates is a dict(hash=>timestamp) of objects
                        # for deletion
                        success, candidates = self.sync(
                            node, job, suffixes, **kwargs)
                        if not success:
                            failure_devs_info.add((node['replication_ip'],
                                                   node['device']))
                        if success and node['region'] != job['region']:
                            synced_remote_regions[node['region']] = \
                                candidates.keys()
                        responses.append(success)
                    for cand_objs in synced_remote_regions.values():
                        if delete_objs is None:
                            delete_objs = cand_objs
                        else:
                            delete_objs = delete_objs & cand_objs

                if self.handoff_delete:
                    # delete handoff if we have had handoff_delete successes
                    successes_count = len([resp for resp in responses if resp])
                    delete_handoff = successes_count >= min(
                        self.handoff_delete, len(job['nodes']))
                else:
                    # delete handoff if all syncs were successful
                    delete_handoff = len(responses) == len(job['nodes']) and \
                        all(responses)
                if delete_handoff:
                    stats.remove += 1
                    if (self.conf.get('sync_method', 'rsync') == 'ssync' and
                            delete_objs is not None):
                        self.logger.info("Removing %s objects",
                                         len(delete_objs))
                        _junk, error_paths = self.delete_handoff_objs(
                            job, delete_objs)
                        # if replication works for a hand-off device and it
                        # failed, the remote devices which are target of the
                        # replication from the hand-off device will be marked.
                        # Because cleanup after replication failed means
                        # replicator needs to replicate again with the same
                        # info.
                        if error_paths:
                            failure_devs_info.update(
                                [(failure_dev['replication_ip'],
                                  failure_dev['device'])
                                 for failure_dev in job['nodes']])
                    else:
                        self.delete_partition(job['path'])
                        handoff_partition_deleted = True
                elif not suffixes:
                    self.delete_partition(job['path'])
                    handoff_partition_deleted = True
        except PartitionLockTimeout:
            self.logger.info("Unable to lock handoff partition %s for "
                             "replication on device %s policy %d",
                             job['partition'], job['device'],
                             job['policy'])
            self.logger.increment('partition.lock-failure.count')
        except (Exception, Timeout):
            self.logger.exception("Error syncing handoff partition")
        finally:
            stats.add_failure_stats(failure_devs_info)
            target_devs_info = set([(target_dev['replication_ip'],
                                     target_dev['device'])
                                    for target_dev in job['nodes']])
            stats.success += len(target_devs_info - failure_devs_info)
            if not handoff_partition_deleted:
                self.handoffs_remaining += 1
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.delete.timing', begin)

    def delete_partition(self, path):
        self.logger.info("Removing partition: %s", path)
        try:
            tpool.execute(shutil.rmtree, path)
        except OSError as e:
            if e.errno not in (errno.ENOENT, errno.ENOTEMPTY, errno.ENODATA,
                               EUCLEAN):
                # Don't worry if there was a race to create or delete,
                # or some disk corruption that happened after the sync
                raise

    def delete_handoff_objs(self, job, delete_objs):
        success_paths = []
        error_paths = []
        for object_hash in delete_objs:
            object_path = storage_directory(job['obj_path'], job['partition'],
                                            object_hash)
            tpool.execute(shutil.rmtree, object_path, ignore_errors=True)
            suffix_dir = dirname(object_path)
            try:
                os.rmdir(suffix_dir)
                success_paths.append(object_path)
            except OSError as e:
                if e.errno not in (errno.ENOENT, errno.ENOTEMPTY):
                    error_paths.append(object_path)
                    self.logger.exception(
                        "Unexpected error trying to cleanup suffix dir %r",
                        suffix_dir)
        return success_paths, error_paths

    def update(self, job):
        """
        High-level method that replicates a single partition.

        :param job: a dict containing info about the partition to be replicated
        """
        stats = self.stats_for_dev[job['device']]
        stats.attempted += 1
        self.logger.increment('partition.update.count.%s' % (job['device'],))
        headers = dict(self.default_headers)
        headers['X-Backend-Storage-Policy-Index'] = int(job['policy'])
        target_devs_info = set()
        failure_devs_info = set()
        begin = time.time()
        df_mgr = self._df_router[job['policy']]
        try:
            hashed, local_hash = tpool.execute(
                df_mgr._get_hashes, job['device'],
                job['partition'], job['policy'],
                do_listdir=_do_listdir(
                    int(job['partition']),
                    self.replication_cycle))
            stats.suffix_hash += hashed
            self.logger.update_stats('suffix.hashes', hashed)
            attempts_left = len(job['nodes'])
            synced_remote_regions = set()
            random.shuffle(job['nodes'])
            nodes = itertools.chain(
                job['nodes'],
                job['policy'].object_ring.get_more_nodes(
                    int(job['partition'])))
            while attempts_left > 0:
                # If this throws StopIteration it will be caught way below
                node = next(nodes)
                node_str = node_to_string(node, replication=True)
                target_devs_info.add((node['replication_ip'], node['device']))
                attempts_left -= 1
                # if we have already synced to this remote region,
                # don't sync again on this replication pass
                if node['region'] in synced_remote_regions:
                    continue
                try:
                    with Timeout(self.http_timeout):
                        conn = http_connect(
                            node['replication_ip'], node['replication_port'],
                            node['device'], job['partition'], 'REPLICATE',
                            '', headers=headers)
                        try:
                            resp = conn.getresponse()
                            if resp.status == HTTP_INSUFFICIENT_STORAGE:
                                self.logger.error('%s responded as unmounted',
                                                  node_str)
                                attempts_left += 1
                                failure_devs_info.add((node['replication_ip'],
                                                       node['device']))
                                continue
                            if resp.status != HTTP_OK:
                                self.logger.error(
                                    "Invalid response %(resp)s "
                                    "from %(remote)s",
                                    {'resp': resp.status, 'remote': node_str})
                                failure_devs_info.add((node['replication_ip'],
                                                       node['device']))
                                continue
                            remote_hash = pickle.loads(
                                resp.read())  # nosec: B301
                        finally:
                            conn.close()
                        del resp
                    suffixes = [suffix for suffix in local_hash if
                                local_hash[suffix] !=
                                remote_hash.get(suffix, -1)]
                    if not suffixes:
                        stats.hashmatch += 1
                        continue
                    hashed, recalc_hash = tpool.execute(
                        df_mgr._get_hashes,
                        job['device'], job['partition'], job['policy'],
                        recalculate=suffixes)
                    self.logger.update_stats('suffix.hashes', hashed)
                    local_hash = recalc_hash
                    suffixes = [suffix for suffix in local_hash if
                                local_hash[suffix] !=
                                remote_hash.get(suffix, -1)]
                    if not suffixes:
                        stats.hashmatch += 1
                        continue
                    stats.rsync += 1
                    success, _junk = self.sync(node, job, suffixes)
                    if not success:
                        failure_devs_info.add((node['replication_ip'],
                                               node['device']))
                    # add only remote region when replicate succeeded
                    if success and node['region'] != job['region']:
                        synced_remote_regions.add(node['region'])
                    stats.suffix_sync += len(suffixes)
                    self.logger.update_stats('suffix.syncs', len(suffixes))
                except (Exception, Timeout):
                    failure_devs_info.add((node['replication_ip'],
                                           node['device']))
                    self.logger.exception("Error syncing with node: %s",
                                          node_str)
            stats.suffix_count += len(local_hash)
        except StopIteration:
            self.logger.error('Ran out of handoffs while replicating '
                              'partition %s of policy %d',
                              job['partition'], int(job['policy']))
        except (Exception, Timeout):
            failure_devs_info.update(target_devs_info)
            self.logger.exception("Error syncing partition")
        finally:
            stats.add_failure_stats(failure_devs_info)
            stats.success += len(target_devs_info - failure_devs_info)
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.update.timing', begin)

    def stats_line(self):
        """
        Logs various stats for the currently running replication pass.
        """
        stats = self.total_stats
        replication_count = stats.attempted
        if replication_count > self.last_replication_count:
            self.last_replication_count = replication_count
            elapsed = (time.time() - self.start) or 0.000001
            rate = replication_count / elapsed
            self.logger.info(
                "%(replicated)d/%(total)d (%(percentage).2f%%)"
                " partitions replicated in %(time).2fs (%(rate).2f/sec, "
                "%(remaining)s remaining)",
                {'replicated': replication_count, 'total': self.job_count,
                 'percentage': replication_count * 100.0 / self.job_count,
                 'time': time.time() - self.start, 'rate': rate,
                 'remaining': '%d%s' % compute_eta(self.start,
                                                   replication_count,
                                                   self.job_count)})
            self.logger.info('%(success)s successes, %(failure)s failures',
                             dict(success=stats.success,
                                  failure=stats.failure))

            if stats.suffix_count:
                self.logger.info(
                    "%(checked)d suffixes checked - "
                    "%(hashed).2f%% hashed, %(synced).2f%% synced",
                    {'checked': stats.suffix_count,
                     'hashed':
                     (stats.suffix_hash * 100.0) / stats.suffix_count,
                     'synced':
                     (stats.suffix_sync * 100.0) / stats.suffix_count})
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
                "Nothing replicated for %s seconds.",
                (time.time() - self.start))

    def heartbeat(self):
        """
        Loop that runs in the background during replication.  It periodically
        logs progress.
        """
        while True:
            eventlet.sleep(self.stats_interval)
            self.stats_line()

    def build_replication_jobs(self, policy, ips, override_devices=None,
                               override_partitions=None):
        """
        Helper function for collect_jobs to build jobs for replication
        using replication style storage policy
        """
        jobs = []
        df_mgr = self._df_router[policy]
        self.all_devs_info.update(
            [(dev['replication_ip'], dev['device'])
             for dev in policy.object_ring.devs if dev])
        data_dir = get_data_dir(policy)
        found_local = False
        for local_dev in [dev for dev in policy.object_ring.devs
                          if (dev
                              and is_local_device(ips,
                                                  self.port,
                                                  dev['replication_ip'],
                                                  dev['replication_port'])
                              and (override_devices is None
                                   or dev['device'] in override_devices))]:
            found_local = True
            local_dev_stats = self.stats_for_dev[local_dev['device']]
            try:
                dev_path = check_drive(self.devices_dir, local_dev['device'],
                                       self.mount_check)
            except ValueError as err:
                local_dev_stats.add_failure_stats(
                    [(failure_dev['replication_ip'],
                      failure_dev['device'])
                     for failure_dev in policy.object_ring.devs
                     if failure_dev])
                self.logger.warning("%s", err)
                continue
            obj_path = join(dev_path, data_dir)
            tmp_path = join(dev_path, get_tmp_dir(policy))
            unlink_older_than(tmp_path, time.time() -
                              df_mgr.reclaim_age)
            if not os.path.exists(obj_path):
                try:
                    mkdirs(obj_path)
                except Exception:
                    self.logger.exception('ERROR creating %s' % obj_path)
                continue
            for partition in listdir(obj_path):
                if (override_partitions is not None and partition.isdigit()
                        and int(partition) not in override_partitions):
                    continue

                if (partition.startswith('auditor_status_') and
                        partition.endswith('.json')):
                    # ignore auditor status files
                    continue

                part_nodes = None
                try:
                    job_path = join(obj_path, partition)
                    part_nodes = policy.object_ring.get_part_nodes(
                        int(partition))
                    nodes = [node for node in part_nodes
                             if node['id'] != local_dev['id']]
                    jobs.append(
                        dict(path=job_path,
                             device=local_dev['device'],
                             obj_path=obj_path,
                             nodes=nodes,
                             delete=len(nodes) > len(part_nodes) - 1,
                             policy=policy,
                             partition=partition,
                             region=local_dev['region']))
                except ValueError:
                    if part_nodes:
                        local_dev_stats.add_failure_stats(
                            [(failure_dev['replication_ip'],
                              failure_dev['device'])
                             for failure_dev in nodes])
                    else:
                        local_dev_stats.add_failure_stats(
                            [(failure_dev['replication_ip'],
                              failure_dev['device'])
                             for failure_dev in policy.object_ring.devs
                             if failure_dev])
                    continue
        if not found_local:
            self.logger.error("Can't find itself in policy with index %d with"
                              " ips %s and with port %s in ring file, not"
                              " replicating",
                              int(policy), ", ".join(ips), self.port)
        return jobs

    def collect_jobs(self, override_devices=None, override_partitions=None,
                     override_policies=None):
        """
        Returns a sorted list of jobs (dictionaries) that specify the
        partitions, nodes, etc to be rsynced.

        :param override_devices: if set, only jobs on these devices
            will be returned
        :param override_partitions: if set, only jobs on these partitions
            will be returned
        :param override_policies: if set, only jobs in these storage
            policies will be returned
        """
        jobs = []
        ips = whataremyips(self.ring_ip)
        for policy in self.policies:
            # Skip replication if next_part_power is set. In this case
            # every object is hard-linked twice, but the replicator can't
            # detect them and would create a second copy of the file if not
            # yet existing - and this might double the actual transferred
            # and stored data
            next_part_power = getattr(
                policy.object_ring, 'next_part_power', None)
            if next_part_power is not None:
                self.logger.warning(
                    "next_part_power set in policy '%s'. Skipping",
                    policy.name)
                continue

            if (override_policies is not None and
                    policy.idx not in override_policies):
                continue
            # ensure rings are loaded for policy
            self.load_object_ring(policy)
            jobs += self.build_replication_jobs(
                policy, ips, override_devices=override_devices,
                override_partitions=override_partitions)
        random.shuffle(jobs)
        if self.handoffs_first:
            # Move the handoff parts to the front of the list
            jobs.sort(key=lambda job: not job['delete'])
        self.job_count = len(jobs)
        return jobs

    def replicate(self, override_devices=None, override_partitions=None,
                  override_policies=None, start_time=None):
        """Run a replication pass"""
        if start_time is None:
            start_time = time.time()
        self.start = start_time
        self.last_replication_count = 0
        self.replication_cycle = (self.replication_cycle + 1) % 10
        self.partition_times = []
        self.all_devs_info = set()
        self.handoffs_remaining = 0

        stats = eventlet.spawn(self.heartbeat)
        eventlet.sleep()  # Give spawns a cycle

        current_nodes = None
        dev_stats = None
        num_jobs = 0
        try:
            self.run_pool = GreenPool(size=self.concurrency)
            jobs = self.collect_jobs(override_devices=override_devices,
                                     override_partitions=override_partitions,
                                     override_policies=override_policies)
            for job in jobs:
                dev_stats = self.stats_for_dev[job['device']]
                num_jobs += 1
                current_nodes = job['nodes']
                try:
                    check_drive(self.devices_dir, job['device'],
                                self.mount_check)
                except ValueError as err:
                    dev_stats.add_failure_stats([
                        (failure_dev['replication_ip'], failure_dev['device'])
                        for failure_dev in job['nodes']])
                    self.logger.warning("%s", err)
                    continue
                if self.handoffs_first and not job['delete']:
                    # in handoffs first mode, we won't process primary
                    # partitions until rebalance was successful!
                    if self.handoffs_remaining:
                        self.logger.warning(
                            "Handoffs first mode still has handoffs "
                            "remaining.  Aborting current "
                            "replication pass.")
                        break
                if not self.check_ring(job['policy'].object_ring):
                    self.logger.info("Ring change detected. Aborting "
                                     "current replication pass.")
                    return

                try:
                    if isfile(job['path']):
                        # Clean up any (probably zero-byte) files where a
                        # partition should be.
                        self.logger.warning(
                            'Removing partition directory '
                            'which was a file: %s', job['path'])
                        os.remove(job['path'])
                        continue
                except OSError:
                    continue
                if job['delete']:
                    self.run_pool.spawn(self.revert, job)
                else:
                    self.run_pool.spawn(self.update, job)
            current_nodes = None
            self.run_pool.waitall()
        except (Exception, Timeout) as err:
            if dev_stats:
                if current_nodes:
                    dev_stats.add_failure_stats(
                        [(failure_dev['replication_ip'],
                          failure_dev['device'])
                         for failure_dev in current_nodes])
                else:
                    dev_stats.add_failure_stats(self.all_devs_info)
            self.logger.exception(
                "Exception in top-level replication loop: %s", err)
        finally:
            stats.kill()
            self.stats_line()

    def update_recon(self, total, end_time, override_devices):
        # Called at the end of a replication pass to update recon stats.
        if self.is_multiprocess_worker:
            # If it weren't for the failure_nodes field, we could do this as
            # a bunch of shared memory using multiprocessing.Value, which
            # would be nice because it'd avoid dealing with existing data
            # during an upgrade.
            update = {
                'object_replication_per_disk': {
                    od: {'replication_stats':
                         self.stats_for_dev[od].to_recon(),
                         'replication_time': total,
                         'replication_last': end_time,
                         'object_replication_time': total,
                         'object_replication_last': end_time}
                    for od in override_devices}}
        else:
            update = {'replication_stats': self.total_stats.to_recon(),
                      'replication_time': total,
                      'replication_last': end_time,
                      'object_replication_time': total,
                      'object_replication_last': end_time}
        dump_recon_cache(update, self.rcache, self.logger)

    def aggregate_recon_update(self):
        per_disk_stats = load_recon_cache(self.rcache).get(
            'object_replication_per_disk', {})
        recon_update = {}
        min_repl_last = float('inf')
        min_repl_time = float('inf')

        # If every child has reported some stats, then aggregate things.
        if all(ld in per_disk_stats for ld in self.all_local_devices):
            aggregated = Stats()
            for device_name, data in per_disk_stats.items():
                aggregated += Stats.from_recon(data['replication_stats'])
                min_repl_time = min(
                    min_repl_time, data['object_replication_time'])
                min_repl_last = min(
                    min_repl_last, data['object_replication_last'])
            recon_update['replication_stats'] = aggregated.to_recon()
            recon_update['replication_last'] = min_repl_last
            recon_update['replication_time'] = min_repl_time
            recon_update['object_replication_last'] = min_repl_last
            recon_update['object_replication_time'] = min_repl_time

        # Clear out entries for old local devices that we no longer have
        devices_to_remove = set(per_disk_stats) - set(self.all_local_devices)
        if devices_to_remove:
            recon_update['object_replication_per_disk'] = {
                dtr: {} for dtr in devices_to_remove}

        return recon_update

    def run_once(self, multiprocess_worker_index=None,
                 have_overrides=False, *args, **kwargs):
        if multiprocess_worker_index is not None:
            self.is_multiprocess_worker = True
            self._emplace_log_prefix(multiprocess_worker_index)

        rsync_reaper = eventlet.spawn(self._child_process_reaper)
        self._zero_stats()
        self.logger.info("Running object replicator in script mode.")

        override_opts = parse_override_options(once=True, **kwargs)
        devices = override_opts.devices or None
        partitions = override_opts.partitions or None
        policies = override_opts.policies or None

        start_time = time.time()
        self.replicate(
            override_devices=devices,
            override_partitions=partitions,
            override_policies=policies,
            start_time=start_time)
        end_time = time.time()
        total = (end_time - start_time) / 60
        self.logger.info(
            "Object replication complete (once). (%.02f minutes)", total)

        # If we've been manually run on a subset of
        # policies/devices/partitions, then our recon stats are not
        # representative of how replication is doing, so we don't publish
        # them.
        if self.is_multiprocess_worker:
            # The main process checked for overrides and determined that
            # there were none
            should_update_recon = not have_overrides
        else:
            # We are single-process, so update recon only if we worked on
            # everything
            should_update_recon = not (partitions or devices or policies)
        if should_update_recon:
            self.update_recon(total, end_time, devices)

        # Give rsync processes one last chance to exit, then bail out and
        # let them be init's problem
        self._child_process_reaper_queue.put(None)
        rsync_reaper.wait()

    def run_forever(self, multiprocess_worker_index=None,
                    override_devices=None, *args, **kwargs):
        if multiprocess_worker_index is not None:
            self.is_multiprocess_worker = True
            self._emplace_log_prefix(multiprocess_worker_index)
        self.logger.info("Starting object replicator in daemon mode.")
        eventlet.spawn_n(self._child_process_reaper)
        # Run the replicator continually
        while True:
            self._zero_stats()
            self.logger.info("Starting object replication pass.")
            # Run the replicator
            start = time.time()
            self.replicate(override_devices=override_devices)
            end = time.time()
            total = (end - start) / 60
            self.logger.info(
                "Object replication complete. (%.02f minutes)", total)
            self.update_recon(total, end, override_devices)
            self.logger.debug('Replication sleeping for %s seconds.',
                              self.interval)
            sleep(self.interval)

    def post_multiprocess_run(self):
        # This method is called after run_once using multiple workers.
        update = self.aggregate_recon_update()
        dump_recon_cache(update, self.rcache, self.logger)


def main():
    parser = OptionParser("%prog CONFIG [options]")
    parser.add_option('-d', '--devices',
                      help='Replicate only given devices. '
                           'Comma-separated list. '
                           'Only has effect if --once is used.')
    parser.add_option('-p', '--partitions',
                      help='Replicate only given partitions. '
                           'Comma-separated list. '
                           'Only has effect if --once is used.')
    parser.add_option('-i', '--policies',
                      help='Replicate only given policy indices. '
                           'Comma-separated list. '
                           'Only has effect if --once is used.')
    conf_file, options = parse_options(parser=parser, once=True)
    run_daemon(ObjectReplicator, conf_file, **options)


if __name__ == '__main__':
    main()
