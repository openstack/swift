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

import unittest
import mock
import time
import os
import random
from tempfile import mkdtemp
from shutil import rmtree

from swift.account import auditor
from test.unit import FakeLogger


class FakeAccountBroker(object):
    def __init__(self, path):
        self.path = path
        self.db_file = path
        self.file = os.path.basename(path)

    def is_deleted(self):
        return False

    def get_info(self):
        if self.file.startswith('fail'):
            raise ValueError
        if self.file.startswith('true'):
            return 'ok'


class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_account_auditor')
        self.logger = FakeLogger()
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
                if self.times < sleep_times:
                    time.sleep(0.1)
                else:
                    # stop forever by an error
                    raise ValueError()

            def time(self):
                return time.time()

        conf = {}
        test_auditor = auditor.AccountAuditor(conf)

        with mock.patch('swift.account.auditor.time', FakeTime()):
            def fake_audit_location_generator(*args, **kwargs):
                files = os.listdir(self.testdir)
                return [(os.path.join(self.testdir, f), '', '') for f in files]

            with mock.patch('swift.account.auditor.audit_location_generator',
                            fake_audit_location_generator):
                self.assertRaises(ValueError, test_auditor.run_forever)
        self.assertEqual(test_auditor.account_failures, 2 * call_times)
        self.assertEqual(test_auditor.account_passes, 3 * call_times)

    @mock.patch('swift.account.auditor.AccountBroker', FakeAccountBroker)
    def test_run_once(self):
        conf = {}
        test_auditor = auditor.AccountAuditor(conf)

        def fake_audit_location_generator(*args, **kwargs):
            files = os.listdir(self.testdir)
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        with mock.patch('swift.account.auditor.audit_location_generator',
                        fake_audit_location_generator):
            test_auditor.run_once()
        self.assertEqual(test_auditor.account_failures, 2)
        self.assertEqual(test_auditor.account_passes, 3)

    @mock.patch('swift.account.auditor.AccountBroker', FakeAccountBroker)
    def test_account_auditor(self):
        conf = {}
        test_auditor = auditor.AccountAuditor(conf)
        files = os.listdir(self.testdir)
        for f in files:
            path = os.path.join(self.testdir, f)
            test_auditor.account_audit(path)
        self.assertEqual(test_auditor.account_failures, 2)
        self.assertEqual(test_auditor.account_passes, 3)

if __name__ == '__main__':
    unittest.main()
