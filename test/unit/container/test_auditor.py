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

from swift.common.utils import normalize_timestamp
from swift.container import auditor
from test.debug_logger import debug_logger
from test.unit import with_tempdir
from test.unit.container import test_backend


class TestAuditorMigrations(unittest.TestCase):

    @with_tempdir
    @mock.patch('swift.common.db_auditor.dump_recon_cache')
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
