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
from os.path import isdir, isfile, join
import random
import shutil
import time
import itertools
import cPickle as pickle
from swift import gettext_ as _

import eventlet
from eventlet import GreenPool, tpool, Timeout, sleep, hubs
from eventlet.green import subprocess
from eventlet.support.greenlets import GreenletExit

from swift.common.utils import whataremyips, unlink_older_than, \
    compute_eta, get_logger, dump_recon_cache, ismount, \
    rsync_ip, mkdirs, config_true_value, list_from_csv, get_hub, \
    tpool_reraise, config_auto_int_value
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon
from swift.common.http import HTTP_OK, HTTP_INSUFFICIENT_STORAGE
from swift.obj import ssync_sender
from swift.obj.diskfile import (DiskFileManager, get_hashes, get_data_dir,
                                get_tmp_dir)
from swift.common.storage_policy import POLICIES


hubs.use_hub(get_hub())


class ObjectReplicator(Daemon):
    """
    Replicate objects.

    Encapsulates most logic and data needed by the object replication process.
    Each call to .replicate() performs one replication pass.  It's up to the
    caller to do this in a loop.
    """

    def __init__(self, conf):
        """
        :param conf: configuration object obtained from ConfigParser
        :param logger: logging object
        """
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-replicator')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.vm_test_mode = config_true_value(conf.get('vm_test_mode', 'no'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.port = int(conf.get('bind_port', 6000))
        self.concurrency = int(conf.get('concurrency', 1))
        self.stats_interval = int(conf.get('stats_interval', '300'))
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.partition_times = []
        self.run_pause = int(conf.get('run_pause', 30))
        self.rsync_timeout = int(conf.get('rsync_timeout', 900))
        self.rsync_io_timeout = conf.get('rsync_io_timeout', '30')
        self.rsync_bwlimit = conf.get('rsync_bwlimit', '0')
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.sync_method = getattr(self, conf.get('sync_method') or 'rsync')
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.headers = {
            'Content-Length': '0',
            'user-agent': 'object-replicator %s' % os.getpid()}
        self.rsync_error_log_line_length = \
            int(conf.get('rsync_error_log_line_length', 0))
        self.handoffs_first = config_true_value(conf.get('handoffs_first',
                                                         False))
        self.handoff_delete = config_auto_int_value(
            conf.get('handoff_delete', 'auto'), 0)
        self._diskfile_mgr = DiskFileManager(conf, self.logger)

    def sync(self, node, job, suffixes):  # Just exists for doc anchor point
        """
        Synchronize local suffix directories from a partition with a remote
        node.

        :param node: the "dev" entry for the remote node to sync with
        :param job: information about the partition being synced
        :param suffixes: a list of suffixes which need to be pushed

        :returns: boolean indicating success or failure
        """
        return self.sync_method(node, job, suffixes)

    def get_object_ring(self, policy_idx):
        """
        Get the ring object to use to handle a request based on its policy.

        :policy_idx: policy index as defined in swift.conf
        :returns: appropriate ring object
        """
        return POLICIES.get_object_ring(policy_idx, self.swift_dir)

    def _rsync(self, args):
        """
        Execute the rsync binary to replicate a partition.

        :returns: return code of rsync process. 0 is successful
        """
        start_time = time.time()
        ret_val = None
        try:
            with Timeout(self.rsync_timeout):
                proc = subprocess.Popen(args,
                                        stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT)
                results = proc.stdout.read()
                ret_val = proc.wait()
        except Timeout:
            self.logger.error(_("Killing long-running rsync: %s"), str(args))
            proc.kill()
            return 1  # failure response code
        total_time = time.time() - start_time
        for result in results.split('\n'):
            if result == '':
                continue
            if result.startswith('cd+'):
                continue
            if not ret_val:
                self.logger.info(result)
            else:
                self.logger.error(result)
        if ret_val:
            error_line = _('Bad rsync return code: %(ret)d <- %(args)s') % \
                {'args': str(args), 'ret': ret_val}
            if self.rsync_error_log_line_length:
                error_line = error_line[:self.rsync_error_log_line_length]
            self.logger.error(error_line)
        elif results:
            self.logger.info(
                _("Successful rsync of %(src)s at %(dst)s (%(time).03f)"),
                {'src': args[-2], 'dst': args[-1], 'time': total_time})
        else:
            self.logger.debug(
                _("Successful rsync of %(src)s at %(dst)s (%(time).03f)"),
                {'src': args[-2], 'dst': args[-1], 'time': total_time})
        return ret_val

    def rsync(self, node, job, suffixes):
        """
        Uses rsync to implement the sync method. This was the first
        sync method in Swift.
        """
        if not os.path.exists(job['path']):
            return False
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
        ]
        node_ip = rsync_ip(node['replication_ip'])
        if self.vm_test_mode:
            rsync_module = '%s::object%s' % (node_ip, node['replication_port'])
        else:
            rsync_module = '%s::object' % node_ip
        had_any = False
        for suffix in suffixes:
            spath = join(job['path'], suffix)
            if os.path.exists(spath):
                args.append(spath)
                had_any = True
        if not had_any:
            return False
        data_dir = get_data_dir(job['policy_idx'])
        args.append(join(rsync_module, node['device'],
                    data_dir, job['partition']))
        return self._rsync(args) == 0

    def ssync(self, node, job, suffixes):
        return ssync_sender.Sender(self, node, job, suffixes)()

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

    def update_deleted(self, job):
        """
        High-level method that replicates a single partition that doesn't
        belong on this node.

        :param job: a dict containing info about the partition to be replicated
        """

        def tpool_get_suffixes(path):
            return [suff for suff in os.listdir(path)
                    if len(suff) == 3 and isdir(join(path, suff))]
        self.replication_count += 1
        self.logger.increment('partition.delete.count.%s' % (job['device'],))
        self.headers['X-Backend-Storage-Policy-Index'] = job['policy_idx']
        begin = time.time()
        try:
            responses = []
            suffixes = tpool.execute(tpool_get_suffixes, job['path'])
            if suffixes:
                for node in job['nodes']:
                    success = self.sync(node, job, suffixes)
                    if success:
                        with Timeout(self.http_timeout):
                            conn = http_connect(
                                node['replication_ip'],
                                node['replication_port'],
                                node['device'], job['partition'], 'REPLICATE',
                                '/' + '-'.join(suffixes), headers=self.headers)
                            conn.getresponse().read()
                    responses.append(success)
            if self.handoff_delete:
                # delete handoff if we have had handoff_delete successes
                delete_handoff = len([resp for resp in responses if resp]) >= \
                    self.handoff_delete
            else:
                # delete handoff if all syncs were successful
                delete_handoff = len(responses) == len(job['nodes']) and \
                    all(responses)
            if not suffixes or delete_handoff:
                self.logger.info(_("Removing partition: %s"), job['path'])
                tpool.execute(shutil.rmtree, job['path'], ignore_errors=True)
        except (Exception, Timeout):
            self.logger.exception(_("Error syncing handoff partition"))
        finally:
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.delete.timing', begin)

    def update(self, job):
        """
        High-level method that replicates a single partition.

        :param job: a dict containing info about the partition to be replicated
        """
        self.replication_count += 1
        self.logger.increment('partition.update.count.%s' % (job['device'],))
        self.headers['X-Backend-Storage-Policy-Index'] = job['policy_idx']
        begin = time.time()
        try:
            hashed, local_hash = tpool_reraise(
                get_hashes, job['path'],
                do_listdir=(self.replication_count % 10) == 0,
                reclaim_age=self.reclaim_age)
            self.suffix_hash += hashed
            self.logger.update_stats('suffix.hashes', hashed)
            attempts_left = len(job['nodes'])
            nodes = itertools.chain(
                job['nodes'],
                job['object_ring'].get_more_nodes(int(job['partition'])))
            while attempts_left > 0:
                # If this throws StopIterator it will be caught way below
                node = next(nodes)
                attempts_left -= 1
                try:
                    with Timeout(self.http_timeout):
                        resp = http_connect(
                            node['replication_ip'], node['replication_port'],
                            node['device'], job['partition'], 'REPLICATE',
                            '', headers=self.headers).getresponse()
                        if resp.status == HTTP_INSUFFICIENT_STORAGE:
                            self.logger.error(_('%(ip)s/%(device)s responded'
                                                ' as unmounted'), node)
                            attempts_left += 1
                            continue
                        if resp.status != HTTP_OK:
                            self.logger.error(_("Invalid response %(resp)s "
                                                "from %(ip)s"),
                                              {'resp': resp.status,
                                               'ip': node['replication_ip']})
                            continue
                        remote_hash = pickle.loads(resp.read())
                        del resp
                    suffixes = [suffix for suffix in local_hash if
                                local_hash[suffix] !=
                                remote_hash.get(suffix, -1)]
                    if not suffixes:
                        continue
                    hashed, recalc_hash = tpool_reraise(
                        get_hashes,
                        job['path'], recalculate=suffixes,
                        reclaim_age=self.reclaim_age)
                    self.logger.update_stats('suffix.hashes', hashed)
                    local_hash = recalc_hash
                    suffixes = [suffix for suffix in local_hash if
                                local_hash[suffix] !=
                                remote_hash.get(suffix, -1)]
                    self.sync(node, job, suffixes)
                    with Timeout(self.http_timeout):
                        conn = http_connect(
                            node['replication_ip'], node['replication_port'],
                            node['device'], job['partition'], 'REPLICATE',
                            '/' + '-'.join(suffixes),
                            headers=self.headers)
                        conn.getresponse().read()
                    self.suffix_sync += len(suffixes)
                    self.logger.update_stats('suffix.syncs', len(suffixes))
                except (Exception, Timeout):
                    self.logger.exception(_("Error syncing with node: %s") %
                                          node)
            self.suffix_count += len(local_hash)
        except (Exception, Timeout):
            self.logger.exception(_("Error syncing partition"))
        finally:
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.update.timing', begin)

    def stats_line(self):
        """
        Logs various stats for the currently running replication pass.
        """
        if self.replication_count:
            elapsed = (time.time() - self.start) or 0.000001
            rate = self.replication_count / elapsed
            self.logger.info(
                _("%(replicated)d/%(total)d (%(percentage).2f%%)"
                  " partitions replicated in %(time).2fs (%(rate).2f/sec, "
                  "%(remaining)s remaining)"),
                {'replicated': self.replication_count, 'total': self.job_count,
                 'percentage': self.replication_count * 100.0 / self.job_count,
                 'time': time.time() - self.start, 'rate': rate,
                 'remaining': '%d%s' % compute_eta(self.start,
                                                   self.replication_count,
                                                   self.job_count)})
            if self.suffix_count:
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
                _("Nothing replicated for %s seconds."),
                (time.time() - self.start))

    def kill_coros(self):
        """Utility function that kills all coroutines currently running."""
        for coro in list(self.run_pool.coroutines_running):
            try:
                coro.kill(GreenletExit)
            except GreenletExit:
                pass

    def heartbeat(self):
        """
        Loop that runs in the background during replication.  It periodically
        logs progress.
        """
        while True:
            eventlet.sleep(self.stats_interval)
            self.stats_line()

    def detect_lockups(self):
        """
        In testing, the pool.waitall() call very occasionally failed to return.
        This is an attempt to make sure the replicator finishes its replication
        pass in some eventuality.
        """
        while True:
            eventlet.sleep(self.lockup_timeout)
            if self.replication_count == self.last_replication_count:
                self.logger.error(_("Lockup detected.. killing live coros."))
                self.kill_coros()
            self.last_replication_count = self.replication_count

    def process_repl(self, policy, jobs, ips):
        """
        Helper function for collect_jobs to build jobs for replication
        using replication style storage policy
        """
        obj_ring = self.get_object_ring(policy.idx)
        data_dir = get_data_dir(policy.idx)
        for local_dev in [dev for dev in obj_ring.devs
                          if dev and dev['replication_ip'] in ips and
                          dev['replication_port'] == self.port]:
            dev_path = join(self.devices_dir, local_dev['device'])
            obj_path = join(dev_path, data_dir)
            tmp_path = join(dev_path, get_tmp_dir(int(policy)))
            if self.mount_check and not ismount(dev_path):
                self.logger.warn(_('%s is not mounted'), local_dev['device'])
                continue
            unlink_older_than(tmp_path, time.time() - self.reclaim_age)
            if not os.path.exists(obj_path):
                try:
                    mkdirs(obj_path)
                except Exception:
                    self.logger.exception('ERROR creating %s' % obj_path)
                continue
            for partition in os.listdir(obj_path):
                try:
                    job_path = join(obj_path, partition)
                    if isfile(job_path):
                        # Clean up any (probably zero-byte) files where a
                        # partition should be.
                        self.logger.warning(
                            'Removing partition directory '
                            'which was a file: %s', job_path)
                        os.remove(job_path)
                        continue
                    part_nodes = obj_ring.get_part_nodes(int(partition))
                    nodes = [node for node in part_nodes
                             if node['id'] != local_dev['id']]
                    jobs.append(
                        dict(path=job_path,
                             device=local_dev['device'],
                             nodes=nodes,
                             delete=len(nodes) > len(part_nodes) - 1,
                             policy_idx=policy.idx,
                             partition=partition,
                             object_ring=obj_ring))

                except (ValueError, OSError):
                    continue

    def collect_jobs(self):
        """
        Returns a sorted list of jobs (dictionaries) that specify the
        partitions, nodes, etc to be rsynced.
        """
        jobs = []
        ips = whataremyips()
        for policy in POLICIES:
            # may need to branch here for future policy types
            self.process_repl(policy, jobs, ips)
        random.shuffle(jobs)
        if self.handoffs_first:
            # Move the handoff parts to the front of the list
            jobs.sort(key=lambda job: not job['delete'])
        self.job_count = len(jobs)
        return jobs

    def replicate(self, override_devices=None, override_partitions=None):
        """Run a replication pass"""
        self.start = time.time()
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.replication_count = 0
        self.last_replication_count = -1
        self.partition_times = []

        if override_devices is None:
            override_devices = []
        if override_partitions is None:
            override_partitions = []

        stats = eventlet.spawn(self.heartbeat)
        lockup_detector = eventlet.spawn(self.detect_lockups)
        eventlet.sleep()  # Give spawns a cycle

        try:
            self.run_pool = GreenPool(size=self.concurrency)
            jobs = self.collect_jobs()
            for job in jobs:
                if override_devices and job['device'] not in override_devices:
                    continue
                if override_partitions and \
                        job['partition'] not in override_partitions:
                    continue
                dev_path = join(self.devices_dir, job['device'])
                if self.mount_check and not ismount(dev_path):
                    self.logger.warn(_('%s is not mounted'), job['device'])
                    continue
                if not self.check_ring(job['object_ring']):
                    self.logger.info(_("Ring change detected. Aborting "
                                       "current replication pass."))
                    return
                if job['delete']:
                    self.run_pool.spawn(self.update_deleted, job)
                else:
                    self.run_pool.spawn(self.update, job)
            with Timeout(self.lockup_timeout):
                self.run_pool.waitall()
        except (Exception, Timeout):
            self.logger.exception(_("Exception in top-level replication loop"))
            self.kill_coros()
        finally:
            stats.kill()
            lockup_detector.kill()
            self.stats_line()

    def run_once(self, *args, **kwargs):
        start = time.time()
        self.logger.info(_("Running object replicator in script mode."))
        override_devices = list_from_csv(kwargs.get('devices'))
        override_partitions = list_from_csv(kwargs.get('partitions'))
        self.replicate(
            override_devices=override_devices,
            override_partitions=override_partitions)
        total = (time.time() - start) / 60
        self.logger.info(
            _("Object replication complete (once). (%.02f minutes)"), total)
        if not (override_partitions or override_devices):
            dump_recon_cache({'object_replication_time': total,
                              'object_replication_last': time.time()},
                             self.rcache, self.logger)

    def run_forever(self, *args, **kwargs):
        self.logger.info(_("Starting object replicator in daemon mode."))
        # Run the replicator continually
        while True:
            start = time.time()
            self.logger.info(_("Starting object replication pass."))
            # Run the replicator
            self.replicate()
            total = (time.time() - start) / 60
            self.logger.info(
                _("Object replication complete. (%.02f minutes)"), total)
            dump_recon_cache({'object_replication_time': total,
                              'object_replication_last': time.time()},
                             self.rcache, self.logger)
            self.logger.debug('Replication sleeping for %s seconds.',
                              self.run_pause)
            sleep(self.run_pause)
