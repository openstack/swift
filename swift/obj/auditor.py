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

import json
import os
import sys
import time
import signal
from os.path import basename, dirname, join
from random import shuffle
from swift import gettext_ as _
from contextlib import closing
from eventlet import Timeout

from swift.obj import diskfile, replicator
from swift.common.utils import (
    get_logger, ratelimit_sleep, dump_recon_cache, list_from_csv, listdir,
    unlink_paths_older_than, readconf, config_auto_int_value, round_robin_iter)
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist,\
    DiskFileDeleted, DiskFileExpired
from swift.common.daemon import Daemon
from swift.common.storage_policy import POLICIES


class AuditorWorker(object):
    """Walk through file system to audit objects"""

    def __init__(self, conf, logger, rcache, devices, zero_byte_only_at_fps=0):
        self.conf = conf
        self.logger = logger
        self.devices = devices
        self.max_files_per_second = float(conf.get('files_per_second', 20))
        self.max_bytes_per_second = float(conf.get('bytes_per_second',
                                                   10000000))
        try:
            # ideally unless ops overrides the rsync_tempfile_timeout in the
            # auditor section we can base our behavior on whatever they
            # configure for their replicator
            replicator_config = readconf(self.conf['__file__'],
                                         'object-replicator')
        except (KeyError, ValueError, IOError):
            # if we can't parse the real config (generally a KeyError on
            # __file__, or ValueError on no object-replicator section, or
            # IOError if reading the file failed) we use
            # a very conservative default for rsync_timeout
            default_rsync_timeout = 86400
        else:
            replicator_rsync_timeout = int(replicator_config.get(
                'rsync_timeout', replicator.DEFAULT_RSYNC_TIMEOUT))
            # Here we can do some light math for ops and use the *replicator's*
            # rsync_timeout (plus 15 mins to avoid deleting local tempfiles
            # before the remote replicator kills it's rsync)
            default_rsync_timeout = replicator_rsync_timeout + 900
            # there's not really a good reason to assume the replicator
            # section's reclaim_age is more appropriate than the reconstructor
            # reclaim_age - but we're already parsing the config so we can set
            # the default value in our config if it's not already set
            if 'reclaim_age' in replicator_config:
                conf.setdefault('reclaim_age',
                                replicator_config['reclaim_age'])
        self.rsync_tempfile_timeout = config_auto_int_value(
            self.conf.get('rsync_tempfile_timeout'), default_rsync_timeout)
        self.diskfile_router = diskfile.DiskFileRouter(conf, self.logger)

        self.auditor_type = 'ALL'
        self.zero_byte_only_at_fps = zero_byte_only_at_fps
        if self.zero_byte_only_at_fps:
            self.max_files_per_second = float(self.zero_byte_only_at_fps)
            self.auditor_type = 'ZBF'
        self.log_time = int(conf.get('log_time', 3600))
        self.last_logged = 0
        self.files_running_time = 0
        self.bytes_running_time = 0
        self.bytes_processed = 0
        self.total_bytes_processed = 0
        self.total_files_processed = 0
        self.passes = 0
        self.quarantines = 0
        self.errors = 0
        self.rcache = rcache
        self.stats_sizes = sorted(
            [int(s) for s in list_from_csv(conf.get('object_size_stats'))])
        self.stats_buckets = dict(
            [(s, 0) for s in self.stats_sizes + ['OVER']])

    def create_recon_nested_dict(self, top_level_key, device_list, item):
        if device_list:
            device_key = ''.join(sorted(device_list))
            return {top_level_key: {device_key: item}}
        else:
            return {top_level_key: item}

    def audit_all_objects(self, mode='once', device_dirs=None):
        description = ''
        if device_dirs:
            device_dir_str = ','.join(sorted(device_dirs))
            if self.auditor_type == 'ALL':
                description = _(' - parallel, %s') % device_dir_str
            else:
                description = _(' - %s') % device_dir_str
        self.logger.info(_('Begin object audit "%(mode)s" mode (%(audi_type)s'
                           '%(description)s)') %
                         {'mode': mode, 'audi_type': self.auditor_type,
                          'description': description})
        begin = reported = time.time()
        self.total_bytes_processed = 0
        self.total_files_processed = 0
        total_quarantines = 0
        total_errors = 0
        time_auditing = 0

        # get AuditLocations for each policy
        loc_generators = []
        for policy in POLICIES:
            loc_generators.append(
                self.diskfile_router[policy]
                    .object_audit_location_generator(
                        policy, device_dirs=device_dirs,
                        auditor_type=self.auditor_type))

        all_locs = round_robin_iter(loc_generators)
        for location in all_locs:
            loop_time = time.time()
            self.failsafe_object_audit(location)
            self.logger.timing_since('timing', loop_time)
            self.files_running_time = ratelimit_sleep(
                self.files_running_time, self.max_files_per_second)
            self.total_files_processed += 1
            now = time.time()
            if now - self.last_logged >= self.log_time:
                self.logger.info(_(
                    'Object audit (%(type)s). '
                    'Since %(start_time)s: Locally: %(passes)d passed, '
                    '%(quars)d quarantined, %(errors)d errors, '
                    'files/sec: %(frate).2f, bytes/sec: %(brate).2f, '
                    'Total time: %(total).2f, Auditing time: %(audit).2f, '
                    'Rate: %(audit_rate).2f') % {
                        'type': '%s%s' % (self.auditor_type, description),
                        'start_time': time.ctime(reported),
                        'passes': self.passes, 'quars': self.quarantines,
                        'errors': self.errors,
                        'frate': self.passes / (now - reported),
                        'brate': self.bytes_processed / (now - reported),
                        'total': (now - begin), 'audit': time_auditing,
                        'audit_rate': time_auditing / (now - begin)})
                cache_entry = self.create_recon_nested_dict(
                    'object_auditor_stats_%s' % (self.auditor_type),
                    device_dirs,
                    {'errors': self.errors, 'passes': self.passes,
                     'quarantined': self.quarantines,
                     'bytes_processed': self.bytes_processed,
                     'start_time': reported, 'audit_time': time_auditing})
                dump_recon_cache(cache_entry, self.rcache, self.logger)
                reported = now
                total_quarantines += self.quarantines
                total_errors += self.errors
                self.passes = 0
                self.quarantines = 0
                self.errors = 0
                self.bytes_processed = 0
                self.last_logged = now
            time_auditing += (now - loop_time)
        # Avoid divide by zero during very short runs
        elapsed = (time.time() - begin) or 0.000001
        self.logger.info(_(
            'Object audit (%(type)s) "%(mode)s" mode '
            'completed: %(elapsed).02fs. Total quarantined: %(quars)d, '
            'Total errors: %(errors)d, Total files/sec: %(frate).2f, '
            'Total bytes/sec: %(brate).2f, Auditing time: %(audit).2f, '
            'Rate: %(audit_rate).2f') % {
                'type': '%s%s' % (self.auditor_type, description),
                'mode': mode, 'elapsed': elapsed,
                'quars': total_quarantines + self.quarantines,
                'errors': total_errors + self.errors,
                'frate': self.total_files_processed / elapsed,
                'brate': self.total_bytes_processed / elapsed,
                'audit': time_auditing, 'audit_rate': time_auditing / elapsed})
        if self.stats_sizes:
            self.logger.info(
                _('Object audit stats: %s') % json.dumps(self.stats_buckets))

        for policy in POLICIES:
            # Unset remaining partitions to not skip them in the next run
            self.diskfile_router[policy].clear_auditor_status(
                policy,
                self.auditor_type)

    def record_stats(self, obj_size):
        """
        Based on config's object_size_stats will keep track of how many objects
        fall into the specified ranges. For example with the following:

        object_size_stats = 10, 100, 1024

        and your system has 3 objects of sizes: 5, 20, and 10000 bytes the log
        will look like: {"10": 1, "100": 1, "1024": 0, "OVER": 1}
        """
        for size in self.stats_sizes:
            if obj_size <= size:
                self.stats_buckets[size] += 1
                break
        else:
            self.stats_buckets["OVER"] += 1

    def failsafe_object_audit(self, location):
        """
        Entrypoint to object_audit, with a failsafe generic exception handler.
        """
        try:
            self.object_audit(location)
        except (Exception, Timeout):
            self.logger.increment('errors')
            self.errors += 1
            self.logger.exception(_('ERROR Trying to audit %s'), location)

    def object_audit(self, location):
        """
        Audits the given object location.

        :param location: an audit location
                         (from diskfile.object_audit_location_generator)
        """
        def raise_dfq(msg):
            raise DiskFileQuarantined(msg)

        diskfile_mgr = self.diskfile_router[location.policy]
        # this method doesn't normally raise errors, even if the audit
        # location does not exist; if this raises an unexpected error it
        # will get logged in failsafe
        df = diskfile_mgr.get_diskfile_from_audit_location(location)
        reader = None
        try:
            with df.open(modernize=True):
                metadata = df.get_metadata()
                obj_size = int(metadata['Content-Length'])
                if self.stats_sizes:
                    self.record_stats(obj_size)
                if obj_size and not self.zero_byte_only_at_fps:
                    reader = df.reader(_quarantine_hook=raise_dfq)
            if reader:
                with closing(reader):
                    for chunk in reader:
                        chunk_len = len(chunk)
                        self.bytes_running_time = ratelimit_sleep(
                            self.bytes_running_time,
                            self.max_bytes_per_second,
                            incr_by=chunk_len)
                        self.bytes_processed += chunk_len
                        self.total_bytes_processed += chunk_len
        except DiskFileQuarantined as err:
            self.quarantines += 1
            self.logger.error(_('ERROR Object %(obj)s failed audit and was'
                                ' quarantined: %(err)s'),
                              {'obj': location, 'err': err})
        except DiskFileExpired:
            pass  # ignore expired objects
        except DiskFileDeleted:
            # If there is a reclaimable tombstone, we'll invalidate the hash
            # to trigger the replicator to rehash/cleanup this suffix
            ts = df._ondisk_info['ts_info']['timestamp']
            if (not self.zero_byte_only_at_fps and
                    (time.time() - float(ts)) > df.manager.reclaim_age):
                df.manager.invalidate_hash(dirname(df._datadir))
        except DiskFileNotExist:
            pass

        self.passes += 1
        # _ondisk_info attr is initialized to None and filled in by open
        ondisk_info_dict = df._ondisk_info or {}
        if 'unexpected' in ondisk_info_dict:
            is_rsync_tempfile = lambda fpath: (
                diskfile.RE_RSYNC_TEMPFILE.match(basename(fpath)))
            rsync_tempfile_paths = filter(is_rsync_tempfile,
                                          ondisk_info_dict['unexpected'])
            mtime = time.time() - self.rsync_tempfile_timeout
            unlink_paths_older_than(rsync_tempfile_paths, mtime)


class ObjectAuditor(Daemon):
    """Audit objects."""

    def __init__(self, conf, logger=None, **options):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='object-auditor')
        self.devices = conf.get('devices', '/srv/node')
        self.concurrency = int(conf.get('concurrency', 1))
        self.conf_zero_byte_fps = int(
            conf.get('zero_byte_files_per_second', 50))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = join(self.recon_cache_path, "object.recon")
        self.interval = int(conf.get('interval', 30))

    def _sleep(self):
        time.sleep(self.interval)

    def clear_recon_cache(self, auditor_type):
        """Clear recon cache entries"""
        dump_recon_cache({'object_auditor_stats_%s' % auditor_type: {}},
                         self.rcache, self.logger)

    def run_audit(self, **kwargs):
        """Run the object audit"""
        mode = kwargs.get('mode')
        zero_byte_only_at_fps = kwargs.get('zero_byte_fps', 0)
        device_dirs = kwargs.get('device_dirs')
        worker = AuditorWorker(self.conf, self.logger, self.rcache,
                               self.devices,
                               zero_byte_only_at_fps=zero_byte_only_at_fps)
        worker.audit_all_objects(mode=mode, device_dirs=device_dirs)

    def fork_child(self, zero_byte_fps=False, sleep_between_zbf_scanner=False,
                   **kwargs):
        """Child execution"""
        pid = os.fork()
        if pid:
            return pid
        else:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            if zero_byte_fps:
                kwargs['zero_byte_fps'] = self.conf_zero_byte_fps
                if sleep_between_zbf_scanner:
                    self._sleep()
            try:
                self.run_audit(**kwargs)
            except Exception as e:
                self.logger.exception(
                    _("ERROR: Unable to run auditing: %s") % e)
            finally:
                sys.exit()

    def audit_loop(self, parent, zbo_fps, override_devices=None, **kwargs):
        """Parallel audit loop"""
        self.clear_recon_cache('ALL')
        self.clear_recon_cache('ZBF')
        once = kwargs.get('mode') == 'once'
        kwargs['device_dirs'] = override_devices
        if parent:
            kwargs['zero_byte_fps'] = zbo_fps
            self.run_audit(**kwargs)
        else:
            pids = set()
            if self.conf_zero_byte_fps:
                zbf_pid = self.fork_child(zero_byte_fps=True, **kwargs)
                pids.add(zbf_pid)
            if self.concurrency == 1:
                # Audit all devices in 1 process
                pids.add(self.fork_child(**kwargs))
            else:
                # Divide devices amongst parallel processes set by
                # self.concurrency.  Total number of parallel processes
                # is self.concurrency + 1 if zero_byte_fps.
                parallel_proc = self.concurrency + 1 if \
                    self.conf_zero_byte_fps else self.concurrency
                device_list = list(override_devices) if override_devices else \
                    listdir(self.devices)
                shuffle(device_list)
                while device_list:
                    pid = None
                    if len(pids) == parallel_proc:
                        pid = os.wait()[0]
                        pids.discard(pid)

                    if self.conf_zero_byte_fps and pid == zbf_pid and once:
                        # If we're only running one pass and the ZBF scanner
                        # finished, don't bother restarting it.
                        zbf_pid = -100
                    elif self.conf_zero_byte_fps and pid == zbf_pid:
                        # When we're running forever, the ZBF scanner must
                        # be restarted as soon as it finishes.
                        kwargs['device_dirs'] = override_devices
                        # sleep between ZBF scanner forks
                        self._sleep()
                        zbf_pid = self.fork_child(zero_byte_fps=True, **kwargs)
                        pids.add(zbf_pid)
                    else:
                        kwargs['device_dirs'] = [device_list.pop()]
                        pids.add(self.fork_child(**kwargs))
            while pids:
                pid = os.wait()[0]
                # ZBF scanner must be restarted as soon as it finishes
                # unless we're in run-once mode
                if self.conf_zero_byte_fps and pid == zbf_pid and \
                   len(pids) > 1 and not once:
                    kwargs['device_dirs'] = override_devices
                    # sleep between ZBF scanner forks
                    zbf_pid = self.fork_child(zero_byte_fps=True,
                                              sleep_between_zbf_scanner=True,
                                              **kwargs)
                    pids.add(zbf_pid)
                pids.discard(pid)

    def run_forever(self, *args, **kwargs):
        """Run the object audit until stopped."""
        # zero byte only command line option
        zbo_fps = kwargs.get('zero_byte_fps', 0)
        parent = False
        if zbo_fps:
            # only start parent
            parent = True
        kwargs = {'mode': 'forever'}

        while True:
            try:
                self.audit_loop(parent, zbo_fps, **kwargs)
            except (Exception, Timeout) as err:
                self.logger.exception(_('ERROR auditing: %s'), err)
            self._sleep()

    def run_once(self, *args, **kwargs):
        """Run the object audit once"""
        # zero byte only command line option
        zbo_fps = kwargs.get('zero_byte_fps', 0)
        override_devices = list_from_csv(kwargs.get('devices'))
        # Remove bogus entries and duplicates from override_devices
        override_devices = list(
            set(listdir(self.devices)).intersection(set(override_devices)))
        parent = False
        if zbo_fps:
            # only start parent
            parent = True
        kwargs = {'mode': 'once'}

        try:
            self.audit_loop(parent, zbo_fps, override_devices=override_devices,
                            **kwargs)
        except (Exception, Timeout) as err:
            self.logger.exception(_('ERROR auditing: %s'), err)
