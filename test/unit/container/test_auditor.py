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
from unittest import mock
import os

from swift.container import auditor
from swift.common.utils import hash_path
from swift.common.exceptions import DatabaseHashLocationMismatch
from test.debug_logger import debug_logger
from test.unit import with_tempdir, BaseUnitTestCase
from test.unit.container import test_backend


class TestAuditorMigrations(BaseUnitTestCase):

    @with_tempdir
    @mock.patch('swift.common.db_auditor.dump_recon_cache')
    def test_db_migration(self, tempdir, mock_recon):
        db_path = os.path.join(tempdir, 'sda', 'containers', '0', '0', '0',
                               'test.db')
        with test_backend.TestContainerBrokerBeforeSPI.old_broker() as \
                old_ContainerBroker:
            broker = old_ContainerBroker(db_path, account='a', container='c',
                                         quarantine_ok=True)
            broker.initialize(self.ts().internal, -1)

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

        broker = auditor.ContainerBroker(db_path, account='a', container='c',
                                         quarantine_ok=True)
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


class TestContainerAuditorHashMismatch(unittest.TestCase):

    def test_hash_location_mismatch_raises_exception(self):
        """Test that _audit raises exception for hash mismatch"""
        mock_broker = mock.Mock()
        mock_broker.db_dir = ('/srv/containers/0/'
                              '000/00000000000000000000000000000000')

        info = {
            'account': 'test_account',
            'container': 'test_container',
            'object_count': 0,
            'bytes_used': 0,
        }

        auditor_instance = auditor.ContainerAuditor({}, logger=debug_logger())

        with self.assertRaises(DatabaseHashLocationMismatch) as cm:
            auditor_instance._audit(info, mock_broker)

        self.assertIn('Hash of account/container names does not match',
                      str(cm.exception))
        self.assertIn('test_account/test_container', str(cm.exception))

    def test_hash_location_match_passes(self):
        """Test that _audit passes when hash matches"""
        account = 'test_account'
        container = 'test_container'
        container_hash = hash_path(account, container)

        mock_broker = mock.Mock()
        mock_broker.db_dir = '/srv/containers/0/%s/%s' % (
            container_hash[-3:], container_hash)

        info = {
            'account': account,
            'container': container,
            'object_count': 0,
            'bytes_used': 0,
        }

        auditor_instance = auditor.ContainerAuditor({}, logger=debug_logger())

        result = auditor_instance._audit(info, mock_broker)
        self.assertIsNone(result)

    def test_audit_exception_handler_no_quarantine(self):
        auditor_instance = auditor.ContainerAuditor({}, logger=debug_logger())

        mock_broker = mock.Mock()
        mock_broker.is_deleted.return_value = False
        mock_broker.db_dir = (
            '/srv/containers/0/000/00000000000000000000000000000000')
        mock_broker.get_info.return_value = {
            'account': 'test_account',
            'container': 'test_container',
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
