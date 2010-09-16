# Copyright (c) 2010 OpenStack, LLC.
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
from os.path import isdir, join
import random
import shutil
import time
import logging
import hashlib
import itertools
import cPickle as pickle

import eventlet
from eventlet import GreenPool, tpool, Timeout, sleep, hubs
from eventlet.green import subprocess
from eventlet.support.greenlets import GreenletExit

from swift.common.ring import Ring
from swift.common.utils import whataremyips, unlink_older_than, lock_path, \
        renamer, compute_eta, get_logger
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon

hubs.use_hub('poll')

PICKLE_PROTOCOL = 2
ONE_WEEK = 604800
HASH_FILE = 'hashes.pkl'


def hash_suffix(path, reclaim_age):
    """
    Performs reclamation and returns an md5 of all (remaining) files.

    :param reclaim_age: age in seconds at which to remove tombstones
    """
    md5 = hashlib.md5()
    for hsh in sorted(os.listdir(path)):
        hsh_path = join(path, hsh)
        files = os.listdir(hsh_path)
        if len(files) == 1:
            if files[0].endswith('.ts'):
                # remove tombstones older than reclaim_age
                ts = files[0].rsplit('.', 1)[0]
                if (time.time() - float(ts)) > reclaim_age:
                    os.unlink(join(hsh_path, files[0]))
                    files.remove(files[0])
        elif files:
            files.sort(reverse=True)
            meta = data = tomb = None
            for filename in files:
                if not meta and filename.endswith('.meta'):
                    meta = filename
                if not data and filename.endswith('.data'):
                    data = filename
                if not tomb and filename.endswith('.ts'):
                    tomb = filename
                if (filename < tomb or       # any file older than tomb
                    filename < data or       # any file older than data
                    (filename.endswith('.meta') and
                     filename < meta)):      # old meta
                    os.unlink(join(hsh_path, filename))
                    files.remove(filename)
        if not files:
            os.rmdir(hsh_path)
        for filename in files:
            md5.update(filename)
    try:
        os.rmdir(path)
    except OSError:
        pass
    return md5.hexdigest()


def recalculate_hashes(partition_dir, suffixes, reclaim_age=ONE_WEEK):
    """
    Recalculates hashes for the given suffixes in the partition and updates
    them in the partition's hashes file.

    :param partition_dir: directory of the partition in which to recalculate
    :param suffixes: list of suffixes to recalculate
    :param reclaim_age: age in seconds at which tombstones should be removed
    """

    def tpool_listdir(partition_dir):
        return dict(((suff, None) for suff in os.listdir(partition_dir)
                     if len(suff) == 3 and isdir(join(partition_dir, suff))))
    hashes_file = join(partition_dir, HASH_FILE)
    with lock_path(partition_dir):
        try:
            with open(hashes_file, 'rb') as fp:
                hashes = pickle.load(fp)
        except Exception:
            hashes = tpool.execute(tpool_listdir, partition_dir)
        for suffix in suffixes:
            suffix_dir = join(partition_dir, suffix)
            if os.path.exists(suffix_dir):
                hashes[suffix] = hash_suffix(suffix_dir, reclaim_age)
            elif suffix in hashes:
                del hashes[suffix]
        with open(hashes_file + '.tmp', 'wb') as fp:
            pickle.dump(hashes, fp, PICKLE_PROTOCOL)
        renamer(hashes_file + '.tmp', hashes_file)


def invalidate_hash(suffix_dir):
    """
    Invalidates the hash for a suffix_dir in the partition's hashes file.

    :param suffix_dir: absolute path to suffix dir whose hash needs
                       invalidating
    """

    suffix = os.path.basename(suffix_dir)
    partition_dir = os.path.dirname(suffix_dir)
    hashes_file = join(partition_dir, HASH_FILE)
    with lock_path(partition_dir):
        try:
            with open(hashes_file, 'rb') as fp:
                hashes = pickle.load(fp)
            if suffix in hashes and not hashes[suffix]:
                return
        except Exception:
            return
        hashes[suffix] = None
        with open(hashes_file + '.tmp', 'wb') as fp:
            pickle.dump(hashes, fp, PICKLE_PROTOCOL)
        renamer(hashes_file + '.tmp', hashes_file)


def get_hashes(partition_dir, do_listdir=True, reclaim_age=ONE_WEEK):
    """
    Get a list of hashes for the suffix dir.  do_listdir causes it to mistrust
    the hash cache for suffix existence at the (unexpectedly high) cost of a
    listdir.  reclaim_age is just passed on to hash_suffix.

    :param partition_dir: absolute path of partition to get hashes for
    :param do_listdir: force existence check for all hashes in the partition
    :param reclaim_age: age at which to remove tombstones

    :returns: tuple of (number of suffix dirs hashed, dictionary of hashes)
    """

    def tpool_listdir(hashes, partition_dir):
        return dict(((suff, hashes.get(suff, None))
                     for suff in os.listdir(partition_dir)
                     if len(suff) == 3 and isdir(join(partition_dir, suff))))
    hashed = 0
    hashes_file = join(partition_dir, HASH_FILE)
    with lock_path(partition_dir):
        modified = False
        hashes = {}
        try:
            with open(hashes_file, 'rb') as fp:
                hashes = pickle.load(fp)
        except Exception:
            do_listdir = True
        if do_listdir:
            hashes = tpool.execute(tpool_listdir, hashes, partition_dir)
            modified = True
        for suffix, hash_ in hashes.items():
            if not hash_:
                suffix_dir = join(partition_dir, suffix)
                if os.path.exists(suffix_dir):
                    try:
                        hashes[suffix] = hash_suffix(suffix_dir, reclaim_age)
                        hashed += 1
                    except OSError:
                        logging.exception('Error hashing suffix')
                        hashes[suffix] = None
                else:
                    del hashes[suffix]
                modified = True
                sleep()
        if modified:
            with open(hashes_file + '.tmp', 'wb') as fp:
                pickle.dump(hashes, fp, PICKLE_PROTOCOL)
            renamer(hashes_file + '.tmp', hashes_file)
        return hashed, hashes


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
        self.logger = get_logger(conf, 'object-replicator')
        self.devices_dir = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.vm_test_mode = conf.get(
                'vm_test_mode', 'no').lower() in ('yes', 'true', 'on', '1')
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.port = int(conf.get('bind_port', 6000))
        self.concurrency = int(conf.get('concurrency', 1))
        self.timeout = conf.get('timeout', '5')
        self.stats_interval = int(conf.get('stats_interval', '3600'))
        self.object_ring = Ring(join(self.swift_dir, 'object.ring.gz'))
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.partition_times = []
        self.run_pause = int(conf.get('run_pause', 30))

    def _rsync(self, args):
        """
        Execute the rsync binary to replicate a partition.

        :returns: a tuple of (rsync exit code, rsync standard output)
        """
        start_time = time.time()
        ret_val = None
        try:
            with Timeout(120):
                proc = subprocess.Popen(args, stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT)
                results = proc.stdout.read()
                ret_val = proc.wait()
        finally:
            if ret_val is None:
                proc.kill()
        total_time = time.time() - start_time
        if results:
            for result in results.split('\n'):
                if result == '':
                    continue
                if result.startswith('cd+'):
                    continue
                self.logger.info(result)
            self.logger.info(
                "Sync of %s at %s complete (%.03f) [%d]" % (
                args[-2], args[-1], total_time, ret_val))
        else:
            self.logger.debug(
                "Sync of %s at %s complete (%.03f) [%d]" % (
                args[-2], args[-1], total_time, ret_val))
        if ret_val:
            self.logger.error('Bad rsync return code: %d' % ret_val)
        return ret_val, results

    def rsync(self, node, job, suffixes):
        """
        Synchronize local suffix directories from a partition with a remote
        node.

        :param node: the "dev" entry for the remote node to sync with
        :param job: information about the partition being synced
        :param suffixes: a list of suffixes which need to be pushed

        :returns: boolean indicating success or failure
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
            '--timeout=%s' % self.timeout,
            '--contimeout=%s' % self.timeout,
        ]
        if self.vm_test_mode:
            rsync_module = '%s::object%s' % (node['ip'], node['port'])
        else:
            rsync_module = '%s::object' % node['ip']
        had_any = False
        for suffix in suffixes:
            spath = join(job['path'], suffix)
            if os.path.exists(spath):
                args.append(spath)
                had_any = True
        if not had_any:
            return False
        args.append(join(rsync_module, node['device'],
                    'objects', job['partition']))
        ret_val, results = self._rsync(args)
        return ret_val == 0

    def check_ring(self):
        """
        Check to see if the ring has been updated

        :returns: boolean indicating whether or not the ring has changed
        """
        if time.time() > self.next_check:
            self.next_check = time.time() + self.ring_check_interval
            if self.object_ring.has_changed():
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
        begin = time.time()
        try:
            responses = []
            suffixes = tpool.execute(tpool_get_suffixes, job['path'])
            if suffixes:
                for node in job['nodes']:
                    success = self.rsync(node, job, suffixes)
                    if success:
                        with Timeout(60):
                            http_connect(node['ip'],
                                node['port'],
                                node['device'], job['partition'], 'REPLICATE',
                                '/' + '-'.join(suffixes),
                          headers={'Content-Length': '0'}).getresponse().read()
                    responses.append(success)
            if not suffixes or (len(responses) == \
                        self.object_ring.replica_count and all(responses)):
                self.logger.info("Removing partition: %s" % job['path'])
                tpool.execute(shutil.rmtree, job['path'], ignore_errors=True)
        except (Exception, Timeout):
            self.logger.exception("Error syncing handoff partition")
        finally:
            self.partition_times.append(time.time() - begin)

    def update(self, job):
        """
        High-level method that replicates a single partition.

        :param job: a dict containing info about the partition to be replicated
        """
        self.replication_count += 1
        begin = time.time()
        try:
            hashed, local_hash = get_hashes(job['path'],
                    do_listdir=(self.replication_count % 10) == 0,
                    reclaim_age=self.reclaim_age)
            self.suffix_hash += hashed
            successes = 0
            nodes = itertools.chain(job['nodes'],
                        self.object_ring.get_more_nodes(int(job['partition'])))
            while successes < (self.object_ring.replica_count - 1):
                node = next(nodes)
                try:
                    with Timeout(60):
                        resp = http_connect(node['ip'], node['port'],
                                node['device'], job['partition'], 'REPLICATE',
                            '', headers={'Content-Length': '0'}).getresponse()
                        if resp.status != 200:
                            self.logger.error("Invalid response %s from %s" %
                                    (resp.status, node['ip']))
                            continue
                        remote_hash = pickle.loads(resp.read())
                        del resp
                    successes += 1
                    suffixes = [suffix for suffix in local_hash
                                  if local_hash[suffix] !=
                                     remote_hash.get(suffix, -1)]
                    if not suffixes:
                        continue
                    self.rsync(node, job, suffixes)
                    recalculate_hashes(job['path'], suffixes,
                                       reclaim_age=self.reclaim_age)
                    with Timeout(60):
                        conn = http_connect(node['ip'], node['port'],
                            node['device'], job['partition'], 'REPLICATE',
                            '/' + '-'.join(suffixes),
                            headers={'Content-Length': '0'})
                        conn.getresponse().read()
                    self.suffix_sync += len(suffixes)
                except (Exception, Timeout):
                    logging.exception("Error syncing with node: %s" % node)
            self.suffix_count += len(local_hash)
        except (Exception, Timeout):
            self.logger.exception("Error syncing partition")
        finally:
            self.partition_times.append(time.time() - begin)

    def stats_line(self):
        """
        Logs various stats for the currently running replication pass.
        """
        if self.replication_count:
            rate = self.replication_count / (time.time() - self.start)
            self.logger.info("%d/%d (%.2f%%) partitions replicated in %.2f "
                             "seconds (%.2f/sec, %s remaining)"
                    % (self.replication_count, self.job_count,
                       self.replication_count * 100.0 / self.job_count,
                       time.time() - self.start, rate,
                       '%d%s' % compute_eta(self.start,
                           self.replication_count, self.job_count)))
            if self.suffix_count:
                self.logger.info("%d suffixes checked - %.2f%% hashed, "
                                 "%.2f%% synced" %
                    (self.suffix_count,
                     (self.suffix_hash * 100.0) / self.suffix_count,
                     (self.suffix_sync * 100.0) / self.suffix_count))
                self.partition_times.sort()
                self.logger.info("Partition times: max %.4fs, min %.4fs, "
                                 "med %.4fs"
                    % (self.partition_times[-1], self.partition_times[0],
                       self.partition_times[len(self.partition_times) // 2]))
        else:
            self.logger.info("Nothing replicated for %s seconds."
                % (time.time() - self.start))

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
        logs progress and attempts to detect lockups, killing any running
        coroutines if the replicator hasn't made progress since last hearbeat.
        """
        while True:
            if self.replication_count == self.last_replication_count:
                self.logger.error("Lockup detected.. killing live coros.")
                self.kill_coros()
            self.last_replication_count = self.replication_count
            eventlet.sleep(300)
            self.stats_line()

    def replicate(self):
        """Run a replication pass"""
        self.start = time.time()
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.replication_count = 0
        self.last_replication_count = -1
        self.partition_times = []
        jobs = []
        stats = eventlet.spawn(self.heartbeat)
        try:
            ips = whataremyips()
            self.run_pool = GreenPool(size=self.concurrency)
            for local_dev in [
                    dev for dev in self.object_ring.devs
                    if dev and dev['ip'] in ips and dev['port'] == self.port]:
                dev_path = join(self.devices_dir, local_dev['device'])
                obj_path = join(dev_path, 'objects')
                tmp_path = join(dev_path, 'tmp')
                if self.mount_check and not os.path.ismount(dev_path):
                    self.logger.warn('%s is not mounted' % local_dev['device'])
                    continue
                unlink_older_than(tmp_path, time.time() - self.reclaim_age)
                if not os.path.exists(obj_path):
                    continue
                for partition in os.listdir(obj_path):
                    try:
                        nodes = [node for node in
                            self.object_ring.get_part_nodes(int(partition))
                                 if node['id'] != local_dev['id']]
                        jobs.append(dict(path=join(obj_path, partition),
                            nodes=nodes, delete=len(nodes) > 2,
                            partition=partition))
                    except ValueError:
                        continue
            random.shuffle(jobs)
            # Partititons that need to be deleted take priority
            jobs.sort(key=lambda job: not job['delete'])
            self.job_count = len(jobs)
            for job in jobs:
                if not self.check_ring():
                    self.logger.info(
                    "Ring change detected. Aborting current replication pass.")
                    return
                if job['delete']:
                    self.run_pool.spawn(self.update_deleted, job)
                else:
                    self.run_pool.spawn(self.update, job)
            with Timeout(120):
                self.run_pool.waitall()
        except (Exception, Timeout):
            self.logger.exception("Exception while replicating")
            self.kill_coros()
        self.stats_line()
        stats.kill()

    def run_once(self):
        start = time.time()
        self.logger.info("Running object replicator in script mode.")
        self.replicate()
        total = (time.time() - start) / 60
        self.logger.info(
            "Object replication complete. (%.02f minutes)" % total)

    def run_forever(self):
        self.logger.info("Starting object replicator in daemon mode.")
        # Run the replicator continually
        while True:
            start = time.time()
            self.logger.info("Starting object replication pass.")
            # Run the replicator
            self.replicate()
            total = (time.time() - start) / 60
            self.logger.info(
                "Object replication complete. (%.02f minutes)" % total)
            self.logger.debug('Replication sleeping for %s seconds.' %
                self.run_pause)
            sleep(self.run_pause)
