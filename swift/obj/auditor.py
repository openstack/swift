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
from hashlib import md5
from random import random

from swift.obj import server as object_server
from swift.obj.replicator import invalidate_hash
from swift.common.utils import get_logger, renamer, audit_location_generator, \
    ratelimit_sleep
from swift.common.exceptions import AuditException
from swift.common.daemon import Daemon


class ObjectAuditor(Daemon):
    """Audit objects."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, 'object-auditor')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = conf.get('mount_check', 'true').lower() in \
                              ('true', 't', '1', 'on', 'yes', 'y')
        self.max_files_per_second = float(conf.get('files_per_second', 20))
        self.max_bytes_per_second = float(conf.get('bytes_per_second',
                                                   10000000))
        self.log_time = int(conf.get('log_time', 3600))
        self.files_running_time = 0
        self.bytes_running_time = 0
        self.bytes_processed = 0
        self.total_bytes_processed = 0
        self.total_files_processed = 0
        self.passes = 0
        self.quarantines = 0
        self.errors = 0

    def run_forever(self):
        """Run the object audit until stopped."""
        while True:
            self.run_once('forever')
            self.total_bytes_processed = 0
            self.total_files_processed = 0
            time.sleep(30)

    def run_once(self, mode='once'):
        """Run the object audit once."""
        self.logger.info(_('Begin object audit "%s" mode' % mode))
        begin = reported = time.time()
        all_locs = audit_location_generator(self.devices,
                                            object_server.DATADIR,
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.object_audit(path, device, partition)
            self.files_running_time = ratelimit_sleep(
                self.files_running_time, self.max_files_per_second)
            self.total_files_processed += 1
            if time.time() - reported >= self.log_time:
                self.logger.info(_(
                    'Since %(start_time)s: Locally: %(passes)d passed audit, '
                    '%(quars)d quarantined, %(errors)d errors '
                    'files/sec: %(frate).2f , bytes/sec: %(brate).2f') % {
                            'start_time': time.ctime(reported),
                            'passes': self.passes,
                            'quars': self.quarantines,
                            'errors': self.errors,
                            'frate': self.passes / (time.time() - reported),
                            'brate': self.bytes_processed /
                                     (time.time() - reported)})
                reported = time.time()
                self.passes = 0
                self.quarantines = 0
                self.errors = 0
                self.bytes_processed = 0
        elapsed = time.time() - begin
        self.logger.info(_(
                'Object audit "%(mode)s" mode completed: %(elapsed).02fs. '
                'Total files/sec: %(frate).2f , '
                'Total bytes/sec: %(brate).2f ') % {
                    'mode': mode,
                    'elapsed': elapsed,
                    'frate': self.total_files_processed / elapsed,
                    'brate': self.total_bytes_processed / elapsed})

    def object_audit(self, path, device, partition):
        """
        Audits the given object path

        :param path: a path to an object
        :param device: the device the path is on
        :param partition: the partition the path is on
        """
        try:
            if not path.endswith('.data'):
                return
            try:
                name = object_server.read_metadata(path)['name']
            except Exception, exc:
                raise AuditException('Error when reading metadata: %s' % exc)
            _junk, account, container, obj = name.split('/', 3)
            df = object_server.DiskFile(self.devices, device,
                                        partition, account,
                                        container, obj,
                                        keep_data_fp=True)
            if df.data_file is None:
                # file is deleted, we found the tombstone
                return
            if os.path.getsize(df.data_file) != \
                    int(df.metadata['Content-Length']):
                raise AuditException('Content-Length of %s does not match '
                    'file size of %s' % (int(df.metadata['Content-Length']),
                                         os.path.getsize(df.data_file)))
            etag = md5()
            for chunk in df:
                self.bytes_running_time = ratelimit_sleep(
                    self.bytes_running_time, self.max_bytes_per_second,
                    incr_by=len(chunk))
                etag.update(chunk)
                self.bytes_processed += len(chunk)
                self.total_bytes_processed += len(chunk)
            etag = etag.hexdigest()
            if etag != df.metadata['ETag']:
                raise AuditException("ETag of %s does not match file's md5 of "
                    "%s" % (df.metadata['ETag'], etag))
        except AuditException, err:
            self.quarantines += 1
            self.logger.error(_('ERROR Object %(obj)s failed audit and will '
                'be quarantined: %(err)s'), {'obj': path, 'err': err})
            invalidate_hash(os.path.dirname(path))
            renamer_path = os.path.dirname(path)
            renamer(renamer_path, os.path.join(self.devices, device,
                'quarantined', 'objects', os.path.basename(renamer_path)))
            return
        except Exception:
            self.errors += 1
            self.logger.exception(_('ERROR Trying to audit %s'), path)
            return
        self.passes += 1
