# Copyright (c) 2010-2018 OpenStack Foundation
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
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, EventletRateLimiter
from swift.common.daemon import Daemon
from swift.common.exceptions import DatabaseAuditorException
from swift.common.recon import DEFAULT_RECON_CACHE_PATH, \
    server_type_to_recon_file


class DatabaseAuditor(Daemon):
    """Base Database Auditor."""

    @property
    def rcache(self):
        return os.path.join(
            self.recon_cache_path,
            server_type_to_recon_file(self.server_type))

    @property
    def server_type(self):
        raise NotImplementedError

    @property
    def broker_class(self):
        raise NotImplementedError

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='{}-auditor'.format(
            self.server_type))
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = float(conf.get('interval', 1800))
        self.logging_interval = 3600  # once an hour
        self.passes = 0
        self.failures = 0
        self.max_dbs_per_second = \
            float(conf.get('{}s_per_second'.format(self.server_type), 200))
        self.rate_limiter = EventletRateLimiter(self.max_dbs_per_second)
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         DEFAULT_RECON_CACHE_PATH)
        self.datadir = '{}s'.format(self.server_type)

    def _one_audit_pass(self, reported):
        all_locs = audit_location_generator(self.devices, self.datadir, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.audit(path)
            if time.time() - reported >= self.logging_interval:
                self.logger.info(
                    'Since %(time)s: %(server_type)s audits: %(pass)s '
                    'passed audit, %(fail)s failed audit',
                    {'time': time.ctime(reported),
                     'pass': self.passes,
                     'fail': self.failures,
                     'server_type': self.server_type})
                dump_recon_cache(
                    {'{}_audits_since'.format(self.server_type): reported,
                     '{}_audits_passed'.format(self.server_type): self.passes,
                     '{}_audits_failed'.format(self.server_type):
                         self.failures},
                    self.rcache, self.logger)
                reported = time.time()
                self.passes = 0
                self.failures = 0
            self.rate_limiter.wait()
        return reported

    def run_forever(self, *args, **kwargs):
        """Run the database audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(
                'Begin %s audit pass.', self.server_type)
            begin = time.time()
            try:
                reported = self._one_audit_pass(reported)
            except (Exception, Timeout):
                self.logger.increment('errors')
                self.logger.exception('ERROR auditing')
            elapsed = time.time() - begin
            self.logger.info(
                '%(server_type)s audit pass completed: %(elapsed).02fs',
                {'elapsed': elapsed, 'server_type': self.server_type.title()})
            dump_recon_cache({
                '{}_auditor_pass_completed'.format(self.server_type): elapsed},
                self.rcache, self.logger)
            if elapsed < self.interval:
                time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """Run the database audit once."""
        self.logger.info(
            'Begin %s audit "once" mode', self.server_type)
        begin = reported = time.time()
        self._one_audit_pass(reported)
        elapsed = time.time() - begin
        self.logger.info(
            '%(server_type)s audit "once" mode completed: %(elapsed).02fs',
            {'elapsed': elapsed, 'server_type': self.server_type.title()})
        dump_recon_cache(
            {'{}_auditor_pass_completed'.format(self.server_type): elapsed},
            self.rcache, self.logger)

    def audit(self, path):
        """
        Audits the given database path

        :param path: the path to a db
        """
        start_time = time.time()
        try:
            broker = self.broker_class(path, logger=self.logger)
            if not broker.is_deleted():
                info = broker.get_info()
                err = self._audit(info, broker)
                if err:
                    raise err
                self.logger.increment('passes')
                self.passes += 1
                self.logger.debug('Audit passed for %s', broker)
        except DatabaseAuditorException as e:
            self.logger.increment('failures')
            self.failures += 1
            self.logger.error('Audit Failed for %(path)s: %(err)s',
                              {'path': path, 'err': str(e)})
        except (Exception, Timeout):
            self.logger.increment('failures')
            self.failures += 1
            self.logger.exception(
                'ERROR Could not get %(server_type)s info %(path)s',
                {'server_type': self.server_type, 'path': path})
        self.logger.timing_since('timing', start_time)

    def _audit(self, info, broker):
        """
        Run any additional audit checks in sub auditor classes

        :param info: The DB <account/container>_info
        :param broker: The broker
        :return: None on success, otherwise an exception to throw.
        """
        raise NotImplementedError
