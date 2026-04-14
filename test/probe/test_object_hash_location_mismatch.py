#!/usr/bin/python -u
# Copyright (c) 2010-2025 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import uuid
from pathlib import Path
from tempfile import TemporaryDirectory

from swiftclient import client
from swift.common.utils import hash_path, readconf
from swift.obj.auditor import ObjectAuditor
from test.debug_logger import debug_logger
from test.probe.common import ReplProbeTest


class TestObjectHashLocationMismatch(ReplProbeTest):

    def test_hash_location_mismatch_logged_not_quarantined(self):
        container = 'container-%s' % uuid.uuid4()
        obj = 'object-%s' % uuid.uuid4()
        client.put_container(self.url, self.token, container, headers={
            'X-Storage-Policy': self.policy.name})
        client.put_object(self.url, self.token, container, obj, b'test data')

        part, nodes = self.object_ring.get_nodes(self.account, container, obj)
        obj_hash = hash_path(self.account, container, obj)
        # PUT can return before every primary has finished writing.
        for node in nodes:
            suffix_dir = Path(self.storage_dir(node, part)) / obj_hash[-3:]
            hash_dir = suffix_dir / obj_hash
            data_file = next(hash_dir.glob('*.data'), None)
            if data_file is not None:
                break
        else:
            self.fail('Could not find a primary replica for the test object')

        conf_file = self.configs['object-server'][self.config_number(node)]
        conf = readconf(conf_file, 'object-auditor')
        conf['devices'] = str(Path(self.device_dir(node)).parent)
        conf['mount_check'] = 'false'
        conf.pop('quarantine_when_hash_location_mismatch', None)
        logger = debug_logger()
        misplaced = suffix_dir / (uuid.uuid4().hex[:-3] + obj_hash[-3:])
        self.assertFalse(misplaced.exists())
        hash_dir.rename(misplaced)
        try:
            with TemporaryDirectory() as recon_dir:
                conf['recon_cache_path'] = recon_dir
                ObjectAuditor(conf, logger).run_audit(
                    mode='once', device_dirs=[node['device']])
            self.assertIn('Object %s has hash path mismatch' % misplaced,
                          '\n'.join(logger.get_lines_for_level('error')))
            self.assertTrue((misplaced / data_file.name).is_file())
        finally:
            if misplaced.exists():
                misplaced.rename(hash_dir)


if __name__ == '__main__':
    unittest.main()
