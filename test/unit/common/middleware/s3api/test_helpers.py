# Copyright (c) 2013 OpenStack Foundation
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

# This stuff can't live in test/unit/__init__.py due to its swob dependency.

import unittest
from test.unit.common.middleware.s3api.helpers import FakeSwift
from swift.common.middleware.s3api.utils import sysmeta_header
from swift.common.swob import HeaderKeyDict
from mock import MagicMock


class S3ApiHelperTestCase(unittest.TestCase):
    def setUp(self):
        self.method = 'HEAD'
        self.path = '/v1/AUTH_test/bucket'

    def _check_headers(self, swift, method, path, headers):
        _, response_headers, _ = swift._responses[(method, path)]
        self.assertEqual(headers, response_headers)

    def test_fake_swift_sysmeta(self):
        swift = FakeSwift()
        orig_headers = HeaderKeyDict()
        orig_headers.update({sysmeta_header('container', 'acl'): 'test',
                             'x-container-meta-foo': 'bar'})

        swift.register(self.method, self.path, MagicMock(), orig_headers, None)

        self._check_headers(swift, self.method, self.path, orig_headers)

        new_headers = orig_headers.copy()
        del new_headers[sysmeta_header('container', 'acl').title()]
        swift.register(self.method, self.path, MagicMock(), new_headers, None)

        self._check_headers(swift, self.method, self.path, orig_headers)

    def test_fake_swift_sysmeta_overwrite(self):
        swift = FakeSwift()
        orig_headers = HeaderKeyDict()
        orig_headers.update({sysmeta_header('container', 'acl'): 'test',
                             'x-container-meta-foo': 'bar'})
        swift.register(self.method, self.path, MagicMock(), orig_headers, None)

        self._check_headers(swift, self.method, self.path, orig_headers)

        new_headers = orig_headers.copy()
        new_headers[sysmeta_header('container', 'acl').title()] = 'bar'

        swift.register(self.method, self.path, MagicMock(), new_headers, None)

        self.assertFalse(orig_headers == new_headers)
        self._check_headers(swift, self.method, self.path, new_headers)


if __name__ == '__main__':
    unittest.main()
