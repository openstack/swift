# Copyright (c) 2010-2015 OpenStack Foundation
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
import os

from swift.common.base_storage_server import BaseStorageServer
from tempfile import mkdtemp
from swift import __version__ as swift_version
from swift.common.swob import Request
from swift.common.utils import get_logger, public
from shutil import rmtree


class FakeOPTIONS(BaseStorageServer):

    server_type = 'test-server'

    def __init__(self, conf, logger=None):
        super(FakeOPTIONS, self).__init__(conf)
        self.logger = logger or get_logger(conf, log_route='test-server')


class FakeANOTHER(FakeOPTIONS):

    @public
    def ANOTHER(self):
        """this is to test adding to allowed_methods"""
        pass


class TestBaseStorageServer(unittest.TestCase):
    """Test swift.common.base_storage_server"""

    def setUp(self):
        self.tmpdir = mkdtemp()
        self.testdir = os.path.join(self.tmpdir,
                                    'tmp_test_base_storage_server')

    def tearDown(self):
        """Tear down for testing swift.common.base_storage_server"""
        rmtree(self.tmpdir)

    def test_server_type(self):
        conf = {'devices': self.testdir, 'mount_check': 'false'}
        baseserver = BaseStorageServer(conf)
        msg = 'Storage nodes have not implemented the Server type.'
        try:
            baseserver.server_type
        except NotImplementedError as e:
            self.assertEqual(str(e), msg)

    def test_allowed_methods(self):
        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'false'}

        # test what's available in the base class
        allowed_methods_test = FakeOPTIONS(conf).allowed_methods
        self.assertEqual(allowed_methods_test, ['OPTIONS'])

        # test that a subclass can add allowed methods
        allowed_methods_test = FakeANOTHER(conf).allowed_methods
        allowed_methods_test.sort()
        self.assertEqual(allowed_methods_test, ['ANOTHER', 'OPTIONS'])

        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'true'}

        # test what's available in the base class
        allowed_methods_test = FakeOPTIONS(conf).allowed_methods
        self.assertEqual(allowed_methods_test, [])

        # test that a subclass can add allowed methods
        allowed_methods_test = FakeANOTHER(conf).allowed_methods
        self.assertEqual(allowed_methods_test, [])

        conf = {'devices': self.testdir, 'mount_check': 'false'}

        # test what's available in the base class
        allowed_methods_test = FakeOPTIONS(conf).allowed_methods
        self.assertEqual(allowed_methods_test, ['OPTIONS'])

        # test that a subclass can add allowed methods
        allowed_methods_test = FakeANOTHER(conf).allowed_methods
        allowed_methods_test.sort()
        self.assertEqual(allowed_methods_test, ['ANOTHER', 'OPTIONS'])

    def test_OPTIONS_error(self):
        msg = 'Storage nodes have not implemented the Server type.'
        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'false'}

        baseserver = BaseStorageServer(conf)
        req = Request.blank('/sda1/p/a/c/o', {'REQUEST_METHOD': 'OPTIONS'})
        req.content_length = 0

        try:
            baseserver.OPTIONS(req)
        except NotImplementedError as e:
            self.assertEqual(str(e), msg)

    def test_OPTIONS(self):
        conf = {'devices': self.testdir, 'mount_check': 'false',
                'replication_server': 'false'}
        req = Request.blank('/sda1/p/a/c/o', {'REQUEST_METHOD': 'OPTIONS'})
        req.content_length = 0
        resp = FakeOPTIONS(conf).OPTIONS(req)
        self.assertEqual(resp.headers['Allow'], 'OPTIONS')
        self.assertEqual(resp.headers['Server'],
                         'test-server/' + swift_version)
