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
from swift.common.utils import get_logger, renamer, audit_location_generator
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
        self.interval = int(conf.get('interval', 1800))
        self.passes = 0
        self.quarantines = 0
        self.errors = 0

    def run_forever(self):    # pragma: no cover
        """Run the object audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            begin = time.time()
            all_locs = audit_location_generator(self.devices,
                                                object_server.DATADIR,
                                                mount_check=self.mount_check,
                                                logger=self.logger)
            for path, device, partition in all_locs:
                self.object_audit(path, device, partition)
                if time.time() - reported >= 3600:  # once an hour
                    self.logger.info(_('Since %(time)s: Locally: %(pass)d '
                        'passed audit, %(quar)d quarantined, %(error)d errors'),
                        {'time': time.ctime(reported), 'pass': self.passes,
                         'quar': self.quarantines, 'error': self.errors})
                    reported = time.time()
                    self.passes = 0
                    self.quarantines = 0
                    self.errors = 0
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self):
        """Run the object audit once."""
        self.logger.info(_('Begin object audit "once" mode'))
        begin = reported = time.time()
        all_locs = audit_location_generator(self.devices,
                                            object_server.DATADIR,
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.object_audit(path, device, partition)
            if time.time() - reported >= 3600:  # once an hour
                self.logger.info(_('Since %(time)s: Locally: %(pass)d '
                    'passed audit, %(quar)d quarantined, %(error)d errors'),
                    {'time': time.ctime(reported), 'pass': self.passes,
                     'quar': self.quarantines, 'error': self.errors})
                reported = time.time()
                self.passes = 0
                self.quarantines = 0
                self.errors = 0
        elapsed = time.time() - begin
        self.logger.info(
            _('Object audit "once" mode completed: %.02fs'), elapsed)

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
            _, account, container, obj = name.split('/', 3)
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
                etag.update(chunk)
            etag = etag.hexdigest()
            if etag != df.metadata['ETag']:
                raise AuditException("ETag of %s does not match file's md5 of "
                    "%s" % (df.metadata['ETag'], etag))
        except AuditException, err:
            self.quarantines += 1
            self.logger.error(_('ERROR Object %(obj)s failed audit and will be '
                'quarantined: %(err)s'), {'obj': path, 'err': err})
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
