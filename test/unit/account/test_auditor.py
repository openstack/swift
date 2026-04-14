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

from unittest import mock
from collections import defaultdict
import itertools
import unittest
import time
import os
import random

from swift.account import auditor
from swift.common.storage_policy import POLICIES
from swift.common.utils import Timestamp, hash_path
from test.debug_logger import debug_logger
from test.unit import patch_policies, with_tempdir
from test.unit.account.test_backend import (
    AccountBrokerPreTrackContainerCountSetup)
from swift.common.exceptions import DatabaseHashLocationMismatch


@patch_policies
class TestAuditorRealBrokerMigration(
        AccountBrokerPreTrackContainerCountSetup, unittest.TestCase):

    def test_db_migration(self):
        # add a few containers
        policies = itertools.cycle(POLICIES)
        num_containers = len(POLICIES) * 3
        per_policy_container_counts = defaultdict(int)
        for i in range(num_containers):
            name = 'test-container-%02d' % i
            policy = next(policies)
            self.broker.put_container(name, self.ts().internal,
                                      0, 0, 0, int(policy))
            per_policy_container_counts[int(policy)] += 1

        self.broker._commit_puts()
        self.assertEqual(num_containers,
                         self.broker.get_info()['container_count'])

        # still un-migrated
        self.assertUnmigrated(self.broker)

        # run auditor, and validate migration
        conf = {'devices': self.testdir, 'mount_check': False,
                'recon_cache_path': self.testdir}
        test_auditor = auditor.AccountAuditor(conf, logger=debug_logger())
        test_auditor.run_once()

        self.restore_account_broker()

        broker = auditor.AccountBroker(self.db_path, account='a',
                                       quarantine_ok=True)
        broker.initialize(Timestamp('1').internal, 0)
        # go after rows directly to avoid unintentional migration
        with broker.get() as conn:
            rows = conn.execute('''
                SELECT storage_policy_index, container_count
                FROM policy_stat
            ''').fetchall()
        for policy_index, container_count in rows:
            self.assertEqual(container_count,
                             per_policy_container_counts[policy_index])


class TestAuditorRealBroker(unittest.TestCase):

    def setUp(self):
        self.logger = debug_logger()
        # really, this would come by way of base_prefix/tail_prefix in
        # get_logger, ultimately tracing back to section_name in daemon...
        self.logger.logger.statsd_client._prefix = 'account-auditor.'

    @with_tempdir
    def test_db_validate_fails(self, tempdir):
        ts = (Timestamp(t).internal for t in itertools.count(int(time.time())))
        db_path = os.path.join(tempdir, 'sda', 'accounts',
                               '0', '0', '0', 'test.db')
        broker = auditor.AccountBroker(db_path, account='a',
                                       quarantine_ok=True)
        broker.initialize(next(ts))
        # add a few containers
        policies = itertools.cycle(POLICIES)
        num_containers = len(POLICIES) * 3
        per_policy_container_counts = defaultdict(int)
        for i in range(num_containers):
            name = 'test-container-%02d' % i
            policy = next(policies)
            broker.put_container(name, next(ts), 0, 0, 0, int(policy))
            per_policy_container_counts[int(policy)] += 1

        broker._commit_puts()
        self.assertEqual(broker.get_info()['container_count'], num_containers)

        messed_up_policy = random.choice(list(POLICIES))

        # now mess up a policy_stats table count
        with broker.get() as conn:
            conn.executescript('''
                UPDATE policy_stat
                SET container_count = container_count - 1
                WHERE storage_policy_index = %d;
            ''' % int(messed_up_policy))

        # validate it's messed up
        policy_stats = broker.get_policy_stats()
        self.assertEqual(
            policy_stats[int(messed_up_policy)]['container_count'],
            per_policy_container_counts[int(messed_up_policy)] - 1)

        # do an audit
        conf = {'devices': tempdir, 'mount_check': False,
                'recon_cache_path': tempdir}
        test_auditor = auditor.AccountAuditor(conf, logger=self.logger)
        test_auditor.run_once()

        # validate errors
        self.assertEqual(test_auditor.failures, 1)
        error_lines = test_auditor.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        error_message = error_lines[0]
        self.assertIn(broker.db_file, error_message)
        self.assertIn(
            'The total container_count for the account a (%d) does not match '
            'the sum of container_count across policies (%d)'
            % (num_containers, num_containers - 1), error_message)
        self.assertEqual(
            test_auditor.logger.statsd_client.get_stats_counts(),
            {'failures': 1})
        self.assertIn(
            (b'account-auditor.failures:1|c', ('host', 8125)),
            test_auditor.logger.statsd_client.sendto_calls)


class TestAccountAuditorHashLocationMismatch(unittest.TestCase):

    def test_hash_location_mismatch_raises_exception(self):
        mock_broker = mock.Mock()
        mock_broker.db_dir = ('/srv/accounts/0/000/'
                              '00000000000000000000000000000000')
        mock_broker.get_policy_stats.return_value = {}

        info = {
            'account': 'test_account',
            'container_count': 0,
            'object_count': 0,
            'bytes_used': 0,
        }

        auditor_instance = auditor.AccountAuditor({}, logger=debug_logger())
        with self.assertRaises(DatabaseHashLocationMismatch) as cm:
            auditor_instance._audit(info, mock_broker)

        self.assertIn('Hash of account name does not match', str(cm.exception))
        self.assertIn('test_account', str(cm.exception))

    def test_hash_location_match_passes(self):
        account = 'test_account'
        account_hash = hash_path(account)

        mock_broker = mock.Mock()
        mock_broker.db_dir = '/srv/accounts/0/%s/%s' % (account_hash[-3:],
                                                        account_hash)
        mock_broker.get_policy_stats.return_value = {}

        info = {
            'account': account,
            'container_count': 0,
            'object_count': 0,
            'bytes_used': 0,
        }

        auditor_instance = auditor.AccountAuditor({}, logger=debug_logger())

        result = auditor_instance._audit(info, mock_broker)
        self.assertIsNone(result)

    def test_audit_exception_handler_no_quarantine(self):
        auditor_instance = auditor.AccountAuditor({}, logger=debug_logger())

        mock_broker = mock.Mock()
        mock_broker.is_deleted.return_value = False
        mock_broker.db_dir = (
            '/srv/accounts/0/000/00000000000000000000000000000000')
        mock_broker.get_policy_stats.return_value = {}
        mock_broker.get_info.return_value = {
            'account': 'test_account',
            'container_count': 0,
            'object_count': 0,
            'bytes_used': 0,
        }

        with mock.patch.object(auditor_instance, 'broker_class',
                               return_value=mock_broker):
            auditor_instance.audit('/fake/path/test.db')

        self.assertEqual(auditor_instance.failures, 1)
        mock_broker.quarantine.assert_not_called()


if __name__ == '__main__':
    unittest.main()
