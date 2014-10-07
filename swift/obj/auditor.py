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
import sys
import time
import signal
from random import shuffle
from swift import gettext_ as _
from contextlib import closing
from eventlet import Timeout

from swift.obj import diskfile
from swift.common.utils import get_logger, ratelimit_sleep, dump_recon_cache, \
    list_from_csv, json, listdir
from swift.common.exceptions import DiskFileQuarantined, DiskFileNotExist
from swift.common.daemon import Daemon

SLEEP_BETWEEN_AUDITS = 30


class AuditorWorker(object):
    """Walk through file system to audit objects"""

    def __init__(self, conf, logger, rcache, devices, zero_byte_only_at_fps=0):
        self.conf = conf
        self.logger = logger
        self.devices = devices
        self.diskfile_mgr = diskfile.DiskFileManager(conf, self.logger)
        self.max_files_per_second = float(conf.get('files_per_second', 20))
        self.max_bytes_per_second = float(conf.get('bytes_per_second',
                                                   10000000))
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
        self.logger.info(_('Begin object audit "%s" mode (%s%s)') %
                        (mode, self.auditor_type, description))
        begin = reported = time.time()
        self.total_bytes_processed = 0
        self.total_files_processed = 0
        total_quarantines = 0
        total_errors = 0
        time_auditing = 0
        all_locs = self.diskfile_mgr.object_audit_location_generator(
            device_dirs=device_dirs)
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
                    '%(quars)d quarantined, %(errors)d errors '
                    'files/sec: %(frate).2f , bytes/sec: %(brate).2f, '
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

        try:
            df = self.diskfile_mgr.get_diskfile_from_audit_location(location)
            with df.open():
                metadata = df.get_metadata()
                obj_size = int(metadata['Content-Length'])
                if self.stats_sizes:
                    self.record_stats(obj_size)
                if self.zero_byte_only_at_fps and obj_size:
                    self.passes += 1
                    return
                reader = df.reader(_quarantine_hook=raise_dfq)
            with closing(reader):
                for chunk in reader:
                    chunk_len = len(chunk)
                    self.bytes_running_time = ratelimit_sleep(
                        self.bytes_running_time,
                        self.max_bytes_per_second,
                        incr_by=chunk_len)
                    self.bytes_processed += chunk_len
                    self.total_bytes_processed += chunk_len
        except DiskFileNotExist:
            return
        except DiskFileQuarantined as err:
            self.quarantines += 1
            self.logger.error(_('ERROR Object %(obj)s failed audit and was'
                                ' quarantined: %(err)s'),
                              {'obj': location, 'err': err})
        self.passes += 1


class ObjectAuditor(Daemon):
    """Audit objects."""

    def __init__(self, conf, **options):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-auditor')
        self.devices = conf.get('devices', '/srv/node')
        self.concurrency = int(conf.get('concurrency', 1))
        self.conf_zero_byte_fps = int(
            conf.get('zero_byte_files_per_second', 50))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")

    def _sleep(self):
        time.sleep(SLEEP_BETWEEN_AUDITS)

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

    def fork_child(self, zero_byte_fps=False, **kwargs):
        """Child execution"""
        pid = os.fork()
        if pid:
            return pid
        else:
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            if zero_byte_fps:
                kwargs['zero_byte_fps'] = self.conf_zero_byte_fps
            try:
                self.run_audit(**kwargs)
            except Exception as e:
                self.logger.error(_("ERROR: Unable to run auditing: %s") % e)
            finally:
                sys.exit()

    def audit_loop(self, parent, zbo_fps, override_devices=None, **kwargs):
        """Parallel audit loop"""
        self.clear_recon_cache('ALL')
        self.clear_recon_cache('ZBF')
        kwargs['device_dirs'] = override_devices
        if parent:
            kwargs['zero_byte_fps'] = zbo_fps
            self.run_audit(**kwargs)
        else:
            pids = []
            if self.conf_zero_byte_fps:
                zbf_pid = self.fork_child(zero_byte_fps=True, **kwargs)
                pids.append(zbf_pid)
            if self.concurrency == 1:
                # Audit all devices in 1 process
                pids.append(self.fork_child(**kwargs))
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
                        pids.remove(pid)
                    # ZBF scanner must be restarted as soon as it finishes
                    if self.conf_zero_byte_fps and pid == zbf_pid:
                        kwargs['device_dirs'] = override_devices
                        # sleep between ZBF scanner forks
                        self._sleep()
                        zbf_pid = self.fork_child(zero_byte_fps=True,
                                                  **kwargs)
                        pids.append(zbf_pid)
                    else:
                        kwargs['device_dirs'] = [device_list.pop()]
                        pids.append(self.fork_child(**kwargs))
            while pids:
                pid = os.wait()[0]
                # ZBF scanner must be restarted as soon as it finishes
                if self.conf_zero_byte_fps and pid == zbf_pid and \
                   len(pids) > 1:
                    kwargs['device_dirs'] = override_devices
                    # sleep between ZBF scanner forks
                    self._sleep()
                    zbf_pid = self.fork_child(zero_byte_fps=True, **kwargs)
                    pids.append(zbf_pid)
                pids.remove(pid)

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
                self.logger.exception(_('ERROR auditing: %s' % err))
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
            self.logger.exception(_('ERROR auditing: %s' % err))
