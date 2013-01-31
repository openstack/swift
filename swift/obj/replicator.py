# Copyright (c) 2010-2012 OpenStack, LLC.
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
from os.path import basename, dirname, isdir, isfile, join
import random
import shutil
import time
import logging
import hashlib
import itertools
import cPickle as pickle
import errno
import uuid

import eventlet
from eventlet import GreenPool, tpool, Timeout, sleep, hubs
from eventlet.green import subprocess
from eventlet.support.greenlets import GreenletExit

from swift.common.ring import Ring
from swift.common.utils import whataremyips, unlink_older_than, lock_path, \
    compute_eta, get_logger, write_pickle, renamer, dump_recon_cache, \
    rsync_ip, mkdirs, config_true_value, list_from_csv, get_hub
from swift.common.bufferedhttp import http_connect
from swift.common.daemon import Daemon
from swift.common.http import HTTP_OK, HTTP_INSUFFICIENT_STORAGE
from swift.common.exceptions import PathNotDir

hubs.use_hub(get_hub())

PICKLE_PROTOCOL = 2
ONE_WEEK = 604800
HASH_FILE = 'hashes.pkl'


def quarantine_renamer(device_path, corrupted_file_path):
    """
    In the case that a file is corrupted, move it to a quarantined
    area to allow replication to fix it.

    :params device_path: The path to the device the corrupted file is on.
    :params corrupted_file_path: The path to the file you want quarantined.

    :returns: path (str) of directory the file was moved to
    :raises OSError: re-raises non errno.EEXIST / errno.ENOTEMPTY
                     exceptions from rename
    """
    from_dir = dirname(corrupted_file_path)
    to_dir = join(device_path, 'quarantined', 'objects', basename(from_dir))
    invalidate_hash(dirname(from_dir))
    try:
        renamer(from_dir, to_dir)
    except OSError, e:
        if e.errno not in (errno.EEXIST, errno.ENOTEMPTY):
            raise
        to_dir = "%s-%s" % (to_dir, uuid.uuid4().hex)
        renamer(from_dir, to_dir)
    return to_dir


def hash_suffix(path, reclaim_age):
    """
    Performs reclamation and returns an md5 of all (remaining) files.

    :param reclaim_age: age in seconds at which to remove tombstones
    :raises PathNotDir: if given path is not a valid directory
    :raises OSError: for non-ENOTDIR errors
    """
    md5 = hashlib.md5()
    try:
        path_contents = sorted(os.listdir(path))
    except OSError, err:
        if err.errno in (errno.ENOTDIR, errno.ENOENT):
            raise PathNotDir()
        raise
    for hsh in path_contents:
        hsh_path = join(path, hsh)
        try:
            files = os.listdir(hsh_path)
        except OSError, err:
            if err.errno == errno.ENOTDIR:
                partition_path = dirname(path)
                objects_path = dirname(partition_path)
                device_path = dirname(objects_path)
                quar_path = quarantine_renamer(device_path, hsh_path)
                logging.exception(
                    _('Quarantined %s to %s because it is not a directory') %
                    (hsh_path, quar_path))
                continue
            raise
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
            for filename in list(files):
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
        write_pickle(hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)


def get_hashes(partition_dir, recalculate=[], do_listdir=False,
               reclaim_age=ONE_WEEK):
    """
    Get a list of hashes for the suffix dir.  do_listdir causes it to mistrust
    the hash cache for suffix existence at the (unexpectedly high) cost of a
    listdir.  reclaim_age is just passed on to hash_suffix.

    :param partition_dir: absolute path of partition to get hashes for
    :param recalculate: list of suffixes which should be recalculated when got
    :param do_listdir: force existence check for all hashes in the partition
    :param reclaim_age: age at which to remove tombstones

    :returns: tuple of (number of suffix dirs hashed, dictionary of hashes)
    """

    hashed = 0
    hashes_file = join(partition_dir, HASH_FILE)
    modified = False
    force_rewrite = False
    hashes = {}
    mtime = -1
    try:
        with open(hashes_file, 'rb') as fp:
            hashes = pickle.load(fp)
        mtime = os.path.getmtime(hashes_file)
    except Exception:
        do_listdir = True
        force_rewrite = True
    if do_listdir:
        for suff in os.listdir(partition_dir):
            if len(suff) == 3:
                hashes.setdefault(suff, None)
        modified = True
    hashes.update((hash_, None) for hash_ in recalculate)
    for suffix, hash_ in hashes.items():
        if not hash_:
            suffix_dir = join(partition_dir, suffix)
            try:
                hashes[suffix] = hash_suffix(suffix_dir, reclaim_age)
                hashed += 1
            except PathNotDir:
                del hashes[suffix]
            except OSError:
                logging.exception(_('Error hashing suffix'))
            modified = True
    if modified:
        with lock_path(partition_dir):
            if force_rewrite or not os.path.exists(hashes_file) or \
                    os.path.getmtime(hashes_file) == mtime:
                write_pickle(
                    hashes, hashes_file, partition_dir, PICKLE_PROTOCOL)
                return hashed, hashes
        return get_hashes(partition_dir, recalculate, do_listdir,
                          reclaim_age)
    else:
        return hashed, hashes


def tpool_reraise(func, *args, **kwargs):
    """
    Hack to work around Eventlet's tpool not catching and reraising Timeouts.
    """
    def inner():
        try:
            return func(*args, **kwargs)
        except BaseException, err:
            return err
    resp = tpool.execute(inner)
    if isinstance(resp, BaseException):
        raise resp
    return resp


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
        self.object_ring = Ring(self.swift_dir, ring_name='object')
        self.ring_check_interval = int(conf.get('ring_check_interval', 15))
        self.next_check = time.time() + self.ring_check_interval
        self.reclaim_age = int(conf.get('reclaim_age', 86400 * 7))
        self.partition_times = []
        self.run_pause = int(conf.get('run_pause', 30))
        self.rsync_timeout = int(conf.get('rsync_timeout', 900))
        self.rsync_io_timeout = conf.get('rsync_io_timeout', '30')
        self.http_timeout = int(conf.get('http_timeout', 60))
        self.lockup_timeout = int(conf.get('lockup_timeout', 1800))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")

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
            self.logger.error(_('Bad rsync return code: %(args)s -> %(ret)d'),
                              {'args': str(args), 'ret': ret_val})
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
            '--timeout=%s' % self.rsync_io_timeout,
            '--contimeout=%s' % self.rsync_io_timeout,
        ]
        node_ip = rsync_ip(node['ip'])
        if self.vm_test_mode:
            rsync_module = '%s::object%s' % (node_ip, node['port'])
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
        args.append(join(rsync_module, node['device'],
                    'objects', job['partition']))
        return self._rsync(args) == 0

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
        self.logger.increment('partition.delete.count.%s' % (job['device'],))
        begin = time.time()
        try:
            responses = []
            suffixes = tpool.execute(tpool_get_suffixes, job['path'])
            if suffixes:
                for node in job['nodes']:
                    success = self.rsync(node, job, suffixes)
                    if success:
                        with Timeout(self.http_timeout):
                            http_connect(
                                node['ip'], node['port'],
                                node['device'], job['partition'], 'REPLICATE',
                                '/' + '-'.join(suffixes),
                                headers={'Content-Length': '0'}).\
                                getresponse().read()
                    responses.append(success)
            if not suffixes or (len(responses) ==
                                len(job['nodes']) and all(responses)):
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
                self.object_ring.get_more_nodes(int(job['partition'])))
            while attempts_left > 0:
                # If this throws StopIterator it will be caught way below
                node = next(nodes)
                attempts_left -= 1
                try:
                    with Timeout(self.http_timeout):
                        resp = http_connect(
                            node['ip'], node['port'],
                            node['device'], job['partition'], 'REPLICATE',
                            '', headers={'Content-Length': '0'}).getresponse()
                        if resp.status == HTTP_INSUFFICIENT_STORAGE:
                            self.logger.error(_('%(ip)s/%(device)s responded'
                                                ' as unmounted'), node)
                            attempts_left += 1
                            continue
                        if resp.status != HTTP_OK:
                            self.logger.error(_("Invalid response %(resp)s "
                                                "from %(ip)s"),
                                              {'resp': resp.status,
                                               'ip': node['ip']})
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
                    self.rsync(node, job, suffixes)
                    with Timeout(self.http_timeout):
                        conn = http_connect(
                            node['ip'], node['port'],
                            node['device'], job['partition'], 'REPLICATE',
                            '/' + '-'.join(suffixes),
                            headers={'Content-Length': '0'})
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

    def collect_jobs(self):
        """
        Returns a sorted list of jobs (dictionaries) that specify the
        partitions, nodes, etc to be rsynced.
        """
        jobs = []
        ips = whataremyips()
        for local_dev in [dev for dev in self.object_ring.devs
                          if dev and dev['ip'] in ips and
                          dev['port'] == self.port]:
            dev_path = join(self.devices_dir, local_dev['device'])
            obj_path = join(dev_path, 'objects')
            tmp_path = join(dev_path, 'tmp')
            if self.mount_check and not os.path.ismount(dev_path):
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
                        self.logger.warning('Removing partition directory '
                                            'which was a file: %s', job_path)
                        os.remove(job_path)
                        continue
                    part_nodes = \
                        self.object_ring.get_part_nodes(int(partition))
                    nodes = [node for node in part_nodes
                             if node['id'] != local_dev['id']]
                    jobs.append(
                        dict(path=job_path,
                             device=local_dev['device'],
                             nodes=nodes,
                             delete=len(nodes) > len(part_nodes) - 1,
                             partition=partition))
                except (ValueError, OSError):
                    continue
        random.shuffle(jobs)
        self.job_count = len(jobs)
        return jobs

    def replicate(self, override_devices=[], override_partitions=[]):
        """Run a replication pass"""
        self.start = time.time()
        self.suffix_count = 0
        self.suffix_sync = 0
        self.suffix_hash = 0
        self.replication_count = 0
        self.last_replication_count = -1
        self.partition_times = []
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
                if self.mount_check and not os.path.ismount(dev_path):
                    self.logger.warn(_('%s is not mounted'), job['device'])
                    continue
                if not self.check_ring():
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
            self.logger.debug(_('Replication sleeping for %s seconds.'),
                              self.run_pause)
            sleep(self.run_pause)
