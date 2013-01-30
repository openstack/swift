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
import time
from random import random

from eventlet import Timeout

import swift.common.db
from swift.container import server as container_server
from swift.common.db import ContainerBroker
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache
from swift.common.daemon import Daemon


class ContainerAuditor(Daemon):
    """Audit containers."""

    def __init__(self, conf):
        self.conf = conf
        self.logger = get_logger(conf, log_route='container-auditor')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 1800))
        self.container_passes = 0
        self.container_failures = 0
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "container.recon")

    def _one_audit_pass(self, reported):
        all_locs = audit_location_generator(self.devices,
                                            container_server.DATADIR,
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.container_audit(path)
            if time.time() - reported >= 3600:  # once an hour
                self.logger.info(
                    _('Since %(time)s: Container audits: %(pass)s passed '
                      'audit, %(fail)s failed audit'),
                    {'time': time.ctime(reported),
                     'pass': self.container_passes,
                     'fail': self.container_failures})
                dump_recon_cache(
                    {'container_audits_since': reported,
                     'container_audits_passed': self.container_passes,
                     'container_audits_failed': self.container_failures},
                    self.rcache, self.logger)
                reported = time.time()
                self.container_passes = 0
                self.container_failures = 0
        return reported

    def run_forever(self, *args, **kwargs):
        """Run the container audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin container audit pass.'))
            begin = time.time()
            try:
                reported = self._one_audit_pass(reported)
            except (Exception, Timeout):
                self.logger.increment('errors')
                self.logger.exception(_('ERROR auditing'))
            elapsed = time.time() - begin
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)
            self.logger.info(
                _('Container audit pass completed: %.02fs'), elapsed)
            dump_recon_cache({'container_auditor_pass_completed': elapsed},
                             self.rcache, self.logger)

    def run_once(self, *args, **kwargs):
        """Run the container audit once."""
        self.logger.info(_('Begin container audit "once" mode'))
        begin = reported = time.time()
        self._one_audit_pass(reported)
        elapsed = time.time() - begin
        self.logger.info(
            _('Container audit "once" mode completed: %.02fs'), elapsed)
        dump_recon_cache({'container_auditor_pass_completed': elapsed},
                         self.recon_container)

    def container_audit(self, path):
        """
        Audits the given container path

        :param path: the path to a container db
        """
        start_time = time.time()
        try:
            if not path.endswith('.db'):
                return
            broker = ContainerBroker(path)
            if not broker.is_deleted():
                broker.get_info()
                self.logger.increment('passes')
                self.container_passes += 1
                self.logger.debug(_('Audit passed for %s'), broker.db_file)
        except (Exception, Timeout):
            self.logger.increment('failures')
            self.container_failures += 1
            self.logger.exception(_('ERROR Could not get container info %s'),
                                  broker.db_file)
        self.logger.timing_since('timing', start_time)
