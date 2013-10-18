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
import time
from swift import gettext_ as _
from contextlib import closing

from eventlet import Timeout

from swift.obj import diskfile
from swift.common.utils import get_logger, audit_location_generator, \
    ratelimit_sleep, dump_recon_cache, list_from_csv, json
from swift.common.exceptions import AuditException, DiskFileQuarantined, \
    DiskFileNotExist
from swift.common.daemon import Daemon

SLEEP_BETWEEN_AUDITS = 30


class AuditorWorker(object):
    """Walk through file system to audit object"""

    def __init__(self, conf, logger, zero_byte_only_at_fps=0):
        self.conf = conf
        self.logger = logger
        self.devices = conf.get('devices', '/srv/node')
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
        self.files_running_time = 0
        self.bytes_running_time = 0
        self.bytes_processed = 0
        self.total_bytes_processed = 0
        self.total_files_processed = 0
        self.passes = 0
        self.quarantines = 0
        self.errors = 0
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "object.recon")
        self.stats_sizes = sorted(
            [int(s) for s in list_from_csv(conf.get('object_size_stats'))])
        self.stats_buckets = dict(
            [(s, 0) for s in self.stats_sizes + ['OVER']])

    def audit_all_objects(self, mode='once'):
        self.logger.info(_('Begin object audit "%s" mode (%s)') %
                         (mode, self.auditor_type))
        begin = reported = time.time()
        self.total_bytes_processed = 0
        self.total_files_processed = 0
        total_quarantines = 0
        total_errors = 0
        time_auditing = 0
        all_locs = audit_location_generator(
            self.devices, diskfile.DATADIR, '.data',
            mount_check=self.diskfile_mgr.mount_check,
            logger=self.logger)
        for path, device, partition in all_locs:
            loop_time = time.time()
            self.failsafe_object_audit(path, device, partition)
            self.logger.timing_since('timing', loop_time)
            self.files_running_time = ratelimit_sleep(
                self.files_running_time, self.max_files_per_second)
            self.total_files_processed += 1
            now = time.time()
            if now - reported >= self.log_time:
                self.logger.info(_(
                    'Object audit (%(type)s). '
                    'Since %(start_time)s: Locally: %(passes)d passed, '
                    '%(quars)d quarantined, %(errors)d errors '
                    'files/sec: %(frate).2f , bytes/sec: %(brate).2f, '
                    'Total time: %(total).2f, Auditing time: %(audit).2f, '
                    'Rate: %(audit_rate).2f') % {
                        'type': self.auditor_type,
                        'start_time': time.ctime(reported),
                        'passes': self.passes, 'quars': self.quarantines,
                        'errors': self.errors,
                        'frate': self.passes / (now - reported),
                        'brate': self.bytes_processed / (now - reported),
                        'total': (now - begin), 'audit': time_auditing,
                        'audit_rate': time_auditing / (now - begin)})
                dump_recon_cache({'object_auditor_stats_%s' %
                                  self.auditor_type: {
                                      'errors': self.errors,
                                      'passes': self.passes,
                                      'quarantined': self.quarantines,
                                      'bytes_processed': self.bytes_processed,
                                      'start_time': reported,
                                      'audit_time': time_auditing}},
                                 self.rcache, self.logger)
                reported = now
                total_quarantines += self.quarantines
                total_errors += self.errors
                self.passes = 0
                self.quarantines = 0
                self.errors = 0
                self.bytes_processed = 0
            time_auditing += (now - loop_time)
        # Avoid divide by zero during very short runs
        elapsed = (time.time() - begin) or 0.000001
        self.logger.info(_(
            'Object audit (%(type)s) "%(mode)s" mode '
            'completed: %(elapsed).02fs. Total quarantined: %(quars)d, '
            'Total errors: %(errors)d, Total files/sec: %(frate).2f , '
            'Total bytes/sec: %(brate).2f, Auditing time: %(audit).2f, '
            'Rate: %(audit_rate).2f') % {
                'type': self.auditor_type, 'mode': mode, 'elapsed': elapsed,
                'quars': total_quarantines, 'errors': total_errors,
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

    def failsafe_object_audit(self, path, device, partition):
        """
        Entrypoint to object_audit, with a failsafe generic exception handler.
        """
        try:
            self.object_audit(path, device, partition)
        except (Exception, Timeout):
            self.logger.increment('errors')
            self.errors += 1
            self.logger.exception(_('ERROR Trying to audit %s'), path)

    def object_audit(self, path, device, partition):
        """
        Audits the given object path.

        :param path: a path to an object
        :param device: the device the path is on
        :param partition: the partition the path is on
        """
        try:
            try:
                name = diskfile.read_metadata(path)['name']
            except (Exception, Timeout) as exc:
                raise AuditException('Error when reading metadata: %s' % exc)
            _junk, account, container, obj = name.split('/', 3)
            df = self.diskfile_mgr.get_diskfile(
                device, partition, account, container, obj)
            try:
                with df.open():
                    metadata = df.get_metadata()
                    obj_size = int(metadata['Content-Length'])
                    if self.stats_sizes:
                        self.record_stats(obj_size)
                    if self.zero_byte_only_at_fps and obj_size:
                        self.passes += 1
                        return
                    reader = df.reader()
                with closing(reader):
                    for chunk in reader:
                        chunk_len = len(chunk)
                        self.bytes_running_time = ratelimit_sleep(
                            self.bytes_running_time,
                            self.max_bytes_per_second,
                            incr_by=chunk_len)
                        self.bytes_processed += chunk_len
                        self.total_bytes_processed += chunk_len
                if reader.was_quarantined:
                    self.quarantines += 1
                    self.logger.error(_('ERROR Object %(obj)s failed audit and'
                                        ' was quarantined: %(err)s'),
                                      {'obj': path,
                                       'err': reader.was_quarantined})
                    return
            except DiskFileNotExist:
                return
        except DiskFileQuarantined as err:
            self.quarantines += 1
            self.logger.error(_('ERROR Object %(obj)s failed audit and was'
                                ' quarantined: %(err)s'),
                              {'obj': path, 'err': err})
        except AuditException as err:
            self.logger.increment('quarantines')
            self.quarantines += 1
            self.logger.error(_('ERROR Object %(obj)s failed audit and will'
                                ' be quarantined: %(err)s'),
                              {'obj': path, 'err': err})
            diskfile.quarantine_renamer(
                os.path.join(self.devices, device), path)
            return
        self.passes += 1


class ObjectAuditor(Daemon):
    """Audit objects."""

    def __init__(self, conf, **options):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-auditor')
        self.conf_zero_byte_fps = int(
            conf.get('zero_byte_files_per_second', 50))

    def _sleep(self):
        time.sleep(SLEEP_BETWEEN_AUDITS)

    def run_forever(self, *args, **kwargs):
        """Run the object audit until stopped."""
        # zero byte only command line option
        zbo_fps = kwargs.get('zero_byte_fps', 0)
        if zbo_fps:
            # only start parent
            parent = True
        else:
            parent = os.fork()  # child gets parent = 0
        kwargs = {'mode': 'forever'}
        if parent:
            kwargs['zero_byte_fps'] = zbo_fps or self.conf_zero_byte_fps
        while True:
            try:
                self.run_once(**kwargs)
            except (Exception, Timeout):
                self.logger.exception(_('ERROR auditing'))
            self._sleep()

    def run_once(self, *args, **kwargs):
        """Run the object audit once."""
        mode = kwargs.get('mode', 'once')
        zero_byte_only_at_fps = kwargs.get('zero_byte_fps', 0)
        worker = AuditorWorker(self.conf, self.logger,
                               zero_byte_only_at_fps=zero_byte_only_at_fps)
        worker.audit_all_objects(mode=mode)
