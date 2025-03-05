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

import unittest
from unittest import mock
import time
import os
import random
from tempfile import mkdtemp
from shutil import rmtree
from eventlet import Timeout

from swift.common.db_auditor import DatabaseAuditor
from test.debug_logger import debug_logger


class FakeDatabaseBroker(object):
    def __init__(self, path, logger):
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


class FakeDatabaseAuditor(DatabaseAuditor):
    server_type = "container"
    broker_class = FakeDatabaseBroker

    def _audit(self, info, broker):
        return None


class TestAuditor(unittest.TestCase):

    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_database_auditor')
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
        test_auditor = FakeDatabaseAuditor(conf, logger=self.logger)

        with mock.patch('swift.common.db_auditor.time', FakeTime()):
            def fake_audit_location_generator(*args, **kwargs):
                files = os.listdir(self.testdir)
                return [(os.path.join(self.testdir, f), '', '') for f in files]

            with mock.patch('swift.common.db_auditor.audit_location_generator',
                            fake_audit_location_generator):
                self.assertRaises(ValueError, test_auditor.run_forever)
        self.assertEqual(test_auditor.failures, 2 * call_times)
        self.assertEqual(test_auditor.passes, 3 * call_times)

        # now force timeout path code coverage
        with mock.patch('swift.common.db_auditor.DatabaseAuditor.'
                        '_one_audit_pass', side_effect=Timeout()):
            with mock.patch('swift.common.db_auditor.time', FakeTime()):
                self.assertRaises(ValueError, test_auditor.run_forever)

    def test_run_once(self):
        conf = {}
        test_auditor = FakeDatabaseAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = os.listdir(self.testdir)
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        with mock.patch('swift.common.db_auditor.audit_location_generator',
                        fake_audit_location_generator):
            test_auditor.run_once()
        self.assertEqual(test_auditor.failures, 2)
        self.assertEqual(test_auditor.passes, 3)

    def test_one_audit_pass(self):
        conf = {}
        test_auditor = FakeDatabaseAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = sorted(os.listdir(self.testdir))
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        # force code coverage for logging path
        with mock.patch('swift.common.db_auditor.audit_location_generator',
                        fake_audit_location_generator), \
                mock.patch('time.time',
                           return_value=(test_auditor.logging_interval * 2)):
            test_auditor._one_audit_pass(0)
        self.assertEqual(test_auditor.failures, 1)
        self.assertEqual(test_auditor.passes, 3)

    def test_database_auditor(self):
        conf = {}
        test_auditor = FakeDatabaseAuditor(conf, logger=self.logger)
        files = os.listdir(self.testdir)
        for f in files:
            path = os.path.join(self.testdir, f)
            test_auditor.audit(path)
        self.assertEqual(test_auditor.failures, 2)
        self.assertEqual(test_auditor.passes, 3)


if __name__ == '__main__':
    unittest.main()
