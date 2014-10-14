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
from eventlet.support.greenlets import GreenletExit

from swift.common.utils import whataremyips, unlink_older_than, \
    compute_eta, get_logger, dump_recon_cache, ismount, \
    mkdirs, config_true_value, list_from_csv, get_hub, \
    tpool_reraise, config_auto_int_value
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon
from swift.common.http import HTTP_OK, HTTP_INSUFFICIENT_STORAGE
from swift.obj import ssync_sender
from swift.obj.diskfile import (DiskFileManager, get_hashes, get_data_dir,
                                get_tmp_dir)
from swift.common.storage_policy import POLICIES, EC_POLICY


hubs.use_hub(get_hub())


class ObjectReconstructor(Daemon):
    """
    reconstruct objects using erasure code.

    Encapsulates most logic and data needed by the object reconstruction
    process. Each call to .reconstruct() performs one pass.  It's up to the
    caller to do this in a loop.
    """

    def __init__(self, conf):
        """
        :param conf: configuration object obtained from ConfigParser
        :param logger: logging object
        """
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-reconstructor')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.vm_test_mode = config_true_value(conf.get('vm_test_mode', 'no'))
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.port = int(conf.get('bind_port', 6000))
        self.concurrency = int(conf.get('concurrency', 1))
        self.stats_interval = int(conf.get('stats_interval', '300'))
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        # TODO:  related to cleanup of old files
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.partition_times = []
        self.run_pause = int(conf.get('run_pause', 30))
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.node_timeout = float(conf.get('node_timeout', 10))
        # TODO:  review these chunk size defaults, probably want to go
        # with 1MB at least
        self.network_chunk_size = int(conf.get('network_chunk_size', 65536))
        self.disk_chunk_size = int(conf.get('disk_chunk_size', 65536))
        self.headers = {
            'Content-Length': '0',
            'user-agent': 'obj-reconstructor %s' % os.getpid()}
        self.handoffs_first = config_true_value(conf.get('handoffs_first',
                                                         False))
        self.handoff_delete = config_auto_int_value(
            conf.get('handoff_delete', 'auto'), 0)
        self._diskfile_mgr = DiskFileManager(conf, self.logger)

    def get_object_ring(self, policy_idx):
        """
        Get the ring object to use to handle a request based on its policy.

        :policy_idx: policy index as defined in swift.conf
        :returns: appropriate ring object
        """
        return POLICIES.get_object_ring(policy_idx, self.swift_dir)

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

    def _get_primary(obj_ring, partition):
        """
        Helper for moving data from a handoff.  Returns the primary
        node that we should be moving a partition back to since
        it doesn't belong to us.  The decision is based on what
        fragment archives exist out there and what fragment archive
        indices we have locally so that each independent reconstructor
        chooses a unique primary.
        """
        # TODO

    def update_deleted(self, job):
        """
        High-level method that reconstructs a single partition that doesn't
        belong on this node, moves it to the correct primary based on
        current placement of fragment archive indicies and the index of
        the local fragment archive.

        :param job: a dictionary about the partition to be reconstructed
        """

        def tpool_get_suffixes(path):
            return [suff for suff in os.listdir(path)
                    if len(suff) == 3 and isdir(join(path, suff))]
        self.reconstruction_count += 1
        self.logger.increment('partition.delete.count.%s' % (job['device'],))
        self.headers['X-Backend-Storage-Policy-Index'] = job['policy_idx']
        begin = time.time()
        try:
            responses = []
            suffixes = tpool.execute(tpool_get_suffixes, job['path'])
            if suffixes:
                # figure out which node we need to sync/delete with
                node = self._get_primary(job['object_ring'], job['partition'])
                # TODO:  for this case the reconstructor needs to let
                # ssync now (new job parm maybe) that it's just putting
                # the archive in tact that ssync shoulnd't do a recon
                # this is related to trello 'revert handoff' card
                success = ssync_sender.Sender(self, node, job, suffixes)()
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
            self.logger.exception(_("Error (reconstructor) syncing handoff "
                                    "partition"))
        finally:
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.delete.timing', begin)

    def update(self, job):
        """
        High-level method that reconstructs a single partition.

        TODO:  So far this is identical to the replicator update(), may want
        to reuse some functions....

        :param job: a dict containing info about the partition to be replicated
        """
        self.reconstruction_count += 1
        self.logger.increment('partition.update.count.%s' % (job['device'],))
        self.headers['X-Backend-Storage-Policy-Index'] = job['policy_idx']
        begin = time.time()
        try:
            hashed, local_hash = tpool_reraise(
                get_hashes, job['path'],
                do_listdir=(self.reconstruction_count % 10) == 0,
                reclaim_age=self.reclaim_age)
            self.suffix_hash += hashed
            self.logger.update_stats('suffix.hashes', hashed)
            attempts_left = len(job['nodes'])
            nodes = itertools.chain(
                job['nodes'],
                job['object_ring'].get_more_nodes(int(job['partition'])))
            while attempts_left > 0:
                # If this throws StopIteration it will be caught way below
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
                    ssync_sender.Sender(self, node, job, suffixes)
                    with Timeout(self.http_timeout):
                        conn = http_connect(
                            node['replication_ip'], node['replication_port'],
                            node['device'], job['partition'], 'replicate',
                            '/' + '-'.join(suffixes),
                            headers=self.headers)
                        conn.getresponse().read()
                    self.suffix_sync += len(suffixes)
                    self.logger.update_stats('suffix.syncs', len(suffixes))
                except (Exception, Timeout):
                    self.logger.exception(_("Error w/reconstructor on "
                                            "node: %s") % node)
            self.suffix_count += len(local_hash)
        except (Exception, Timeout):
            self.logger.exception(_("Error reconstructing partition"))
        finally:
            self.partition_times.append(time.time() - begin)
            self.logger.timing_since('partition.update.timing', begin)

    def stats_line(self):
        """
        Logs various stats for the currently running reconstruction pass.
        """
        if self.reconstruction_count:
            elapsed = (time.time() - self.start) or 0.000001
            rate = self.reconstruction_count / elapsed
            self.logger.info(
                _("%(replicated)d/%(total)d (%(percentage).2f%%)"
                  " partitions reconstructed in %(time).2fs (%(rate).2f/sec, "
                  "%(remaining)s remaining)"),
                {'replicated': self.reconstruction_count,
                 'total': self.job_count,
                 'percentage':
                 self.reconstruction_count * 100.0 / self.job_count,
                 'time': time.time() - self.start, 'rate': rate,
                 'remaining': '%d%s' % compute_eta(self.start,
                                                   self.reconstruction_count,
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
                _("Nothing reconstructed for %s seconds."),
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
        Loop that runs in the background during reconstruction.  It
        periodically logs progress.
        """
        while True:
            eventlet.sleep(self.stats_interval)
            self.stats_line()

    def detect_lockups(self):
        """
        In testing, the pool.waitall() call very occasionally failed to return.
        This is an attempt to make sure the reconstructor finishes its
        reconstruction pass in some eventuality.
        """
        while True:
            eventlet.sleep(self.lockup_timeout)
            if self.reconstruction_count == self.last_reconstruction_count:
                self.logger.error(_("Lockup detected.. killing live coros."))
                self.kill_coros()
            self.last_reconstruction_count = self.reconstruction_count

    def _get_partners(self, local_id, obj_ring, partition):
        """
        Returns the left and right parnters if this nodes is a primary,
        otherwise returns a flag indicating that this node is a handoff

        The return value takes two forms depending on if local_id is a primary
        node id in the obj_ring.

        If primary:

            :returns: False, [<node-to-left>, <node-to-right>]

        If handoff:

            :returns: True, []
        """
        part_nodes = obj_ring.get_part_nodes(int(partition))
        for node in part_nodes:
            if node['id'] == local_id:
                handoff_node = False
                left = part_nodes[(node['index'] - 1) % len(part_nodes)]
                right = part_nodes[(node['index'] + 1) % len(part_nodes)]
                partners = [left, right]
                break
        else:
            # didn't find local_id in part_nodes
            handoff_node = True
            partners = []
        return handoff_node, partners

    def build_reconstruction_jobs(self, policy, jobs, ips):
        """
        Helper function for collect_jobs to build jobs for reconstruction
        using EC style storage policy
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
                    self.logger.exception('ERROR (reconstructor) creating %s'
                                          % obj_path)
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

                    # we only talk to our left and right ring parners, if we're
                    # in the role of handoff we'll let the job processor figure
                    # out the right node to sync/delete with
                    handoff, partners = self._get_partners(local_dev['id'],
                                                           obj_ring,
                                                           partition)
                    jobs.append(
                        dict(path=job_path,
                             device=local_dev['device'],
                             nodes=partners,
                             delete=handoff,
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
            if policy.policy_type == EC_POLICY:
                self.build_reconstruction_jobs(policy, jobs, ips)
        random.shuffle(jobs)
        if self.handoffs_first:
            # Move the handoff parts to the front of the list
            jobs.sort(key=lambda job: not job['delete'])
        self.job_count = len(jobs)
        return jobs

    def reconstruct(self, override_devices=None, override_partitions=None):
        """Run a reconstruction pass"""
        self.start = time.time()
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.reconstruction_count = 0
        self.last_reconstruction_count = -1
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
                                       "current reconstruction pass."))
                    return
                if job['delete']:
                    self.run_pool.spawn(self.update_deleted, job)
                else:
                    self.run_pool.spawn(self.update, job)
            with Timeout(self.lockup_timeout):
                self.run_pool.waitall()
        except (Exception, Timeout):
            self.logger.exception(_("Exception in top-level"
                                    "reconstruction loop"))
            self.kill_coros()
        finally:
            stats.kill()
            lockup_detector.kill()
            self.stats_line()

    def run_once(self, *args, **kwargs):
        start = time.time()
        self.logger.info(_("Running object reconstructor in script mode."))
        override_devices = list_from_csv(kwargs.get('devices'))
        override_partitions = list_from_csv(kwargs.get('partitions'))
        self.reconstruct(
            override_devices=override_devices,
            override_partitions=override_partitions)
        total = (time.time() - start) / 60
        self.logger.info(
            _("Object reconstruction complete (once). (%.02f minutes)"), total)
        if not (override_partitions or override_devices):
            dump_recon_cache({'object_reconstruction_time': total,
                              'object_reconstruction_last': time.time()},
                             self.rcache, self.logger)

    def run_forever(self, *args, **kwargs):
        self.logger.info(_("Starting object reconstructor in daemon mode."))
        # Run the reconstructor continually
        while True:
            start = time.time()
            self.logger.info(_("Starting object reconstruction pass."))
            # Run the reconstructor
            self.reconstruct()
            total = (time.time() - start) / 60
            self.logger.info(
                _("Object reconstruction complete. (%.02f minutes)"), total)
            dump_recon_cache({'object_reconstruction_time': total,
                              'object_reconstruction_last': time.time()},
                             self.rcache, self.logger)
            self.logger.debug('reconstruction sleeping for %s seconds.',
                              self.run_pause)
            sleep(self.run_pause)
