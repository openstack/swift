# Copyright (c) 2010-2011 OpenStack, LLC.
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
import uuid
import errno
from hashlib import md5
from random import random

from eventlet import Timeout

from swift.obj import server as object_server
from swift.obj.replicator import invalidate_hash
from swift.common.utils import get_logger, renamer, audit_location_generator, \
    ratelimit_sleep, TRUE_VALUES
from swift.common.exceptions import AuditException, DiskFileError, \
    DiskFileNotExist
from swift.common.daemon import Daemon

SLEEP_BETWEEN_AUDITS = 30


class AuditorWorker(object):
    """Walk through file system to audit object"""

    def __init__(self, conf, zero_byte_only_at_fps=0):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-auditor')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
            TRUE_VALUES
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

    def audit_all_objects(self, mode='once'):
        self.logger.info(_('Begin object audit "%s" mode (%s)' %
                           (mode, self.auditor_type)))
        begin = reported = time.time()
        self.total_bytes_processed = 0
        self.total_files_processed = 0
        total_quarantines = 0
        total_errors = 0
        files_running_time = 0
        time_auditing = 0
        all_locs = audit_location_generator(self.devices,
                                            object_server.DATADIR,
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            loop_time = time.time()
            self.object_audit(path, device, partition)
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

    def object_audit(self, path, device, partition):
        """
        Audits the given object path.

        :param path: a path to an object
        :param device: the device the path is on
        :param partition: the partition the path is on
        """
        try:
            if not path.endswith('.data'):
                return
            try:
                name = object_server.read_metadata(path)['name']
            except (Exception, Timeout), exc:
                raise AuditException('Error when reading metadata: %s' % exc)
            _junk, account, container, obj = name.split('/', 3)
            df = object_server.DiskFile(self.devices, device, partition,
                                        account, container, obj, self.logger,
                                        keep_data_fp=True)
            try:
                if df.data_file is None:
                    # file is deleted, we found the tombstone
                    return
                try:
                    obj_size = df.get_data_file_size()
                except DiskFileError, e:
                    raise AuditException(str(e))
                except DiskFileNotExist:
                    return
                if self.zero_byte_only_at_fps and obj_size:
                    self.passes += 1
                    return
                for chunk in df:
                    self.bytes_running_time = ratelimit_sleep(
                        self.bytes_running_time, self.max_bytes_per_second,
                        incr_by=len(chunk))
                    self.bytes_processed += len(chunk)
                    self.total_bytes_processed += len(chunk)
                df.close()
                if df.quarantined_dir:
                    self.quarantines += 1
                    self.logger.error(
                        _("ERROR Object %(path)s failed audit and will be "
                          "quarantined: ETag and file's md5 do not match"),
                        {'path': path})
            finally:
                df.close(verify_file=False)
        except AuditException, err:
            self.quarantines += 1
            self.logger.error(_('ERROR Object %(obj)s failed audit and will '
                'be quarantined: %(err)s'), {'obj': path, 'err': err})
            object_server.quarantine_renamer(
                os.path.join(self.devices, device), path)
            return
        except (Exception, Timeout):
            self.errors += 1
            self.logger.exception(_('ERROR Trying to audit %s'), path)
            return
        self.passes += 1


class ObjectAuditor(Daemon):
    """Audit objects."""

    def __init__(self, conf, **options):
        self.conf = conf
        self.logger = get_logger(conf, log_route='object-auditor')
        self.conf_zero_byte_fps = int(conf.get(
                'zero_byte_files_per_second', 50))

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
        worker = AuditorWorker(self.conf,
                               zero_byte_only_at_fps=zero_byte_only_at_fps)
        worker.audit_all_objects(mode=mode)
