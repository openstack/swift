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
from eventlet import Timeout

from swift.common.utils import normalize_timestamp
from swift.container import auditor
from test.unit import debug_logger, with_tempdir
from test.unit.container import test_backend


class FakeContainerBroker(object):
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
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_container_auditor')
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

    @mock.patch('swift.container.auditor.dump_recon_cache')
    @mock.patch('swift.container.auditor.ContainerBroker', FakeContainerBroker)
    def test_run_forever(self, mock_recon):
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
        test_auditor = auditor.ContainerAuditor(conf, logger=self.logger)

        with mock.patch('swift.container.auditor.time', FakeTime()):
            def fake_audit_location_generator(*args, **kwargs):
                files = os.listdir(self.testdir)
                return [(os.path.join(self.testdir, f), '', '') for f in files]

            with mock.patch('swift.container.auditor.audit_location_generator',
                            fake_audit_location_generator):
                self.assertRaises(ValueError, test_auditor.run_forever)
        self.assertEqual(test_auditor.container_failures, 2 * call_times)
        self.assertEqual(test_auditor.container_passes, 3 * call_times)

        # now force timeout path code coverage
        with mock.patch('swift.container.auditor.ContainerAuditor.'
                        '_one_audit_pass', side_effect=Timeout()):
            with mock.patch('swift.container.auditor.time', FakeTime()):
                self.assertRaises(ValueError, test_auditor.run_forever)

    @mock.patch('swift.container.auditor.dump_recon_cache')
    @mock.patch('swift.container.auditor.ContainerBroker', FakeContainerBroker)
    def test_run_once(self, mock_recon):
        conf = {}
        test_auditor = auditor.ContainerAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = os.listdir(self.testdir)
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        with mock.patch('swift.container.auditor.audit_location_generator',
                        fake_audit_location_generator):
            test_auditor.run_once()
        self.assertEqual(test_auditor.container_failures, 2)
        self.assertEqual(test_auditor.container_passes, 3)

    @mock.patch('swift.container.auditor.dump_recon_cache')
    @mock.patch('swift.container.auditor.ContainerBroker', FakeContainerBroker)
    def test_one_audit_pass(self, mock_recon):
        conf = {}
        test_auditor = auditor.ContainerAuditor(conf, logger=self.logger)

        def fake_audit_location_generator(*args, **kwargs):
            files = sorted(os.listdir(self.testdir))
            return [(os.path.join(self.testdir, f), '', '') for f in files]

        # force code coverage for logging path
        test_auditor.logging_interval = 0
        with mock.patch('swift.container.auditor.audit_location_generator',
                        fake_audit_location_generator):
            test_auditor._one_audit_pass(test_auditor.logging_interval)
        self.assertEqual(test_auditor.container_failures, 1)
        self.assertEqual(test_auditor.container_passes, 3)

    @mock.patch('swift.container.auditor.ContainerBroker', FakeContainerBroker)
    def test_container_auditor(self):
        conf = {}
        test_auditor = auditor.ContainerAuditor(conf, logger=self.logger)
        files = os.listdir(self.testdir)
        for f in files:
            path = os.path.join(self.testdir, f)
            test_auditor.container_audit(path)
        self.assertEqual(test_auditor.container_failures, 2)
        self.assertEqual(test_auditor.container_passes, 3)


class TestAuditorMigrations(unittest.TestCase):

    @with_tempdir
    @mock.patch('swift.container.auditor.dump_recon_cache')
    def test_db_migration(self, tempdir, mock_recon):
        db_path = os.path.join(tempdir, 'sda', 'containers', '0', '0', '0',
                               'test.db')
        with test_backend.TestContainerBrokerBeforeSPI.old_broker() as \
                old_ContainerBroker:
            broker = old_ContainerBroker(db_path, account='a', container='c')
            broker.initialize(normalize_timestamp(0), -1)

        with broker.get() as conn:
            try:
                conn.execute('SELECT storage_policy_index '
                             'FROM container_stat')
            except Exception as err:
                self.assertTrue('no such column: storage_policy_index' in
                                str(err))
            else:
                self.fail('TestContainerBrokerBeforeSPI broker class '
                          'was already migrated')

        conf = {'devices': tempdir, 'mount_check': False}
        test_auditor = auditor.ContainerAuditor(conf, logger=debug_logger())
        test_auditor.run_once()

        broker = auditor.ContainerBroker(db_path, account='a', container='c')
        info = broker.get_info()
        expected = {
            'account': 'a',
            'container': 'c',
            'object_count': 0,
            'bytes_used': 0,
            'storage_policy_index': 0,
        }
        for k, v in expected.items():
            self.assertEqual(info[k], v)


if __name__ == '__main__':
    unittest.main()
