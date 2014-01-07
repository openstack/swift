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

# TODO(creiht): Tests

import unittest
from swift.common import exceptions


class TestExceptions(unittest.TestCase):

    def test_replication_exception(self):
        self.assertEqual(str(exceptions.ReplicationException()), '')
        self.assertEqual(str(exceptions.ReplicationException('test')), 'test')

    def test_replication_lock_timeout(self):
        exc = exceptions.ReplicationLockTimeout(15, 'test')
        try:
            self.assertTrue(isinstance(exc, exceptions.MessageTimeout))
        finally:
            exc.cancel()

    def test_client_exception(self):
        strerror = 'test: HTTP://random:888/randompath?foo=1 666 reason: ' \
                   'device /sdb1   content'
        exc = exceptions.ClientException('test', http_scheme='HTTP',
                                         http_host='random',
                                         http_port=888,
                                         http_path='/randompath',
                                         http_query='foo=1',
                                         http_status=666,
                                         http_reason='reason',
                                         http_device='/sdb1',
                                         http_response_content='content')
        self.assertEqual(str(exc), strerror)

if __name__ == '__main__':
    unittest.main()
