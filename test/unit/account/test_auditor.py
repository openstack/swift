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

from collections import defaultdict
import itertools
import unittest
import mock
import time
import os
import random
from tempfile import mkdtemp
from shutil import rmtree
from eventlet import Timeout

from swift.account import auditor
from swift.common.storage_policy import POLICIES
from swift.common.utils import Timestamp
from test.unit import debug_logger, patch_policies, with_tempdir
from test.unit.account.test_backend import (
    AccountBrokerPreTrackContainerCountSetup)


class FakeAccountBroker(object):
    def __init__(self, path):
        self.path = path
        self.db_file = path
        self.file = os.path.basename(path)

    def is_deleted(self):
        return False

    def get_info(self):
        if self.file.startswith('fail'):
            raise ValueError()
        if self.file.startswith('true'):
            return defaultdict(int)

    def get_policy_stats(self, **kwargs):
        if self.file.startswith('fail'):
            raise ValueError()
        if self.file.startswith('true'):
            return defaultdict(int)


class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_account_auditor')
        self.logger = debug_logger()
        rmtree(self.testdir, ignore_errors=1)
        os.mkdir(self.testdir)
        fnames = ['true1.db', 'true2.db', 'true3.db',
                  'fail1.db', 'fail2.db']
        for fn in fnames:
            with open(os.path.join(self.testdir, fn), 'w+') as f:
                f.write(' ')

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir), ignore_errors=1)

    @mock.patch('swift.account.auditor.AccountBroker', FakeAccountBroker)
    def test_run_forever(self):
        sleep_times = random.randint(5, 10)
        call_times = sleep_times - 1

        class FakeTime(object):
            def __init__(self):
                self.times = 0

            def sleep(self, sec):
                self.times += 1
                if self.times >= sleep_times:
                    # stop forever by an error
                    raise ValueError()

            def time(self):
                return time.time()

        conf = {}
        test_auditor = auditor.AccountAuditor(conf, logger=self.logger)

        with mock.patch('swift.account.auditor.time', FakeTime()):
            def fake_audit_location_generator(*args, **kwargs):
                files = os.listdir(self.testdir)
                return [(os.path.join(self.testdir, f), '', '') for f in files]

            with mock.patch('swift.account.auditor.audit_location_generator',
                            fake_audit_location_generator):
                self.assertRaises(ValueError, test_auditor.run_forever)
        self.assertEqual(test_auditor.account_failures, 2 * call_times)
        self.assertEqual(test_auditor.account_passes, 3 * call_times)

        # now force timeout path code coverage
        def fake_one_audit_pass(reported):
            raise Timeout()

        with mock.patch('swift.account.auditor.AccountAuditor._one_audit_pass',
                        fake_one_audit_pass):
            with mock.patch('swift.account.auditor.time', FakeTime()):
                self.assertRaises(ValueError, test_auditor.run_forever)
        self.assertEqual(test_auditor.account_failures, 2 * call_times)
        self.assertEqual(test_auditor.account_passes, 3 * call_times)

    @mock.patch('swift.account.auditor.AccountBroker', FakeAccountBroker)
    def test_run_once(self):
        conf = {}
        test_auditor = auditor.AccountAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = os.listdir(self.testdir)
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        with mock.patch('swift.account.auditor.audit_location_generator',
                        fake_audit_location_generator):
            test_auditor.run_once()
        self.assertEqual(test_auditor.account_failures, 2)
        self.assertEqual(test_auditor.account_passes, 3)

    @mock.patch('swift.account.auditor.AccountBroker', FakeAccountBroker)
    def test_one_audit_pass(self):
        conf = {}
        test_auditor = auditor.AccountAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = os.listdir(self.testdir)
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        # force code coverage for logging path
        test_auditor.logging_interval = 0
        with mock.patch('swift.account.auditor.audit_location_generator',
                        fake_audit_location_generator):
            test_auditor._one_audit_pass(test_auditor.logging_interval)
        self.assertEqual(test_auditor.account_failures, 0)
        self.assertEqual(test_auditor.account_passes, 0)

    @mock.patch('swift.account.auditor.AccountBroker', FakeAccountBroker)
    def test_account_auditor(self):
        conf = {}
        test_auditor = auditor.AccountAuditor(conf, logger=self.logger)
        files = os.listdir(self.testdir)
        for f in files:
            path = os.path.join(self.testdir, f)
            test_auditor.account_audit(path)
        self.assertEqual(test_auditor.account_failures, 2)
        self.assertEqual(test_auditor.account_passes, 3)


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
            self.broker.put_container(name, next(self.ts),
                                      0, 0, 0, int(policy))
            per_policy_container_counts[int(policy)] += 1

        self.broker._commit_puts()
        self.assertEqual(num_containers,
                         self.broker.get_info()['container_count'])

        # still un-migrated
        self.assertUnmigrated(self.broker)

        # run auditor, and validate migration
        conf = {'devices': self.tempdir, 'mount_check': False,
                'recon_cache_path': self.tempdir}
        test_auditor = auditor.AccountAuditor(conf, logger=debug_logger())
        test_auditor.run_once()

        self.restore_account_broker()

        broker = auditor.AccountBroker(self.db_path)
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

    @with_tempdir
    def test_db_validate_fails(self, tempdir):
        ts = (Timestamp(t).internal for t in itertools.count(int(time.time())))
        db_path = os.path.join(tempdir, 'sda', 'accounts',
                               '0', '0', '0', 'test.db')
        broker = auditor.AccountBroker(db_path, account='a')
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
        self.assertEqual(test_auditor.account_failures, 1)
        error_lines = test_auditor.logger.get_lines_for_level('error')
        self.assertEqual(len(error_lines), 1)
        error_message = error_lines[0]
        self.assertIn(broker.db_file, error_message)
        self.assertIn('container_count', error_message)
        self.assertIn('does not match', error_message)
        self.assertEqual(test_auditor.logger.get_increment_counts(),
                         {'failures': 1})


if __name__ == '__main__':
    unittest.main()
