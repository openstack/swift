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
from random import random

import swift.common.db
from swift.account.backend import AccountBroker, DATADIR
from swift.common.exceptions import InvalidAccountInfo
from swift.common.utils import get_logger, audit_location_generator, \
    config_true_value, dump_recon_cache, ratelimit_sleep
from swift.common.daemon import Daemon

from eventlet import Timeout


class AccountAuditor(Daemon):
    """Audit accounts."""

    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger or get_logger(conf, log_route='account-auditor')
        self.devices = conf.get('devices', '/srv/node')
        self.mount_check = config_true_value(conf.get('mount_check', 'true'))
        self.interval = int(conf.get('interval', 1800))
        self.logging_interval = 3600  # once an hour
        self.account_passes = 0
        self.account_failures = 0
        self.accounts_running_time = 0
        self.max_accounts_per_second = \
            float(conf.get('accounts_per_second', 200))
        swift.common.db.DB_PREALLOCATION = \
            config_true_value(conf.get('db_preallocation', 'f'))
        self.recon_cache_path = conf.get('recon_cache_path',
                                         '/var/cache/swift')
        self.rcache = os.path.join(self.recon_cache_path, "account.recon")

    def _one_audit_pass(self, reported):
        all_locs = audit_location_generator(self.devices, DATADIR, '.db',
                                            mount_check=self.mount_check,
                                            logger=self.logger)
        for path, device, partition in all_locs:
            self.account_audit(path)
            if time.time() - reported >= self.logging_interval:
                self.logger.info(_('Since %(time)s: Account audits: '
                                   '%(passed)s passed audit,'
                                   '%(failed)s failed audit'),
                                 {'time': time.ctime(reported),
                                  'passed': self.account_passes,
                                  'failed': self.account_failures})
                dump_recon_cache({'account_audits_since': reported,
                                  'account_audits_passed': self.account_passes,
                                  'account_audits_failed':
                                  self.account_failures},
                                 self.rcache, self.logger)
                reported = time.time()
                self.account_passes = 0
                self.account_failures = 0
            self.accounts_running_time = ratelimit_sleep(
                self.accounts_running_time, self.max_accounts_per_second)
        return reported

    def run_forever(self, *args, **kwargs):
        """Run the account audit until stopped."""
        reported = time.time()
        time.sleep(random() * self.interval)
        while True:
            self.logger.info(_('Begin account audit pass.'))
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
                _('Account audit pass completed: %.02fs'), elapsed)
            dump_recon_cache({'account_auditor_pass_completed': elapsed},
                             self.rcache, self.logger)

    def run_once(self, *args, **kwargs):
        """Run the account audit once."""
        self.logger.info(_('Begin account audit "once" mode'))
        begin = reported = time.time()
        self._one_audit_pass(reported)
        elapsed = time.time() - begin
        self.logger.info(
            _('Account audit "once" mode completed: %.02fs'), elapsed)
        dump_recon_cache({'account_auditor_pass_completed': elapsed},
                         self.rcache, self.logger)

    def validate_per_policy_counts(self, broker):
        info = broker.get_info()
        policy_stats = broker.get_policy_stats(do_migrations=True)
        policy_totals = {
            'container_count': 0,
            'object_count': 0,
            'bytes_used': 0,
        }
        for policy_stat in policy_stats.values():
            for key in policy_totals:
                policy_totals[key] += policy_stat[key]

        for key in policy_totals:
            if policy_totals[key] == info[key]:
                continue
            raise InvalidAccountInfo(_(
                'The total %(key)s for the container (%(total)s) does not '
                'match the sum of %(key)s across policies (%(sum)s)')
                % {'key': key,
                   'total': info[key],
                   'sum': policy_totals[key]})

    def account_audit(self, path):
        """
        Audits the given account path

        :param path: the path to an account db
        """
        start_time = time.time()
        try:
            broker = AccountBroker(path)
            if not broker.is_deleted():
                self.validate_per_policy_counts(broker)
                self.logger.increment('passes')
                self.account_passes += 1
                self.logger.debug(_('Audit passed for %s'), broker)
        except InvalidAccountInfo as e:
            self.logger.increment('failures')
            self.account_failures += 1
            self.logger.error(
                _('Audit Failed for %(path)s: %(err)s'),
                {'path': path, 'err': str(e)})
        except (Exception, Timeout):
            self.logger.increment('failures')
            self.account_failures += 1
            self.logger.exception(_('ERROR Could not get account info %s'),
                                  path)
        self.logger.timing_since('timing', start_time)
