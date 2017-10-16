# Copyright (c) 2014 OpenStack Foundation
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

from swift.common.swob import Request

from swift3.test.unit import Swift3TestCase
from swift3.acl_utils import handle_acl_header


class TestSwift3AclUtils(Swift3TestCase):

    def setUp(self):
        super(TestSwift3AclUtils, self).setUp()

    def test_handle_acl_header(self):
        def check_generated_acl_header(acl, targets):
            req = Request.blank('/bucket',
                                headers={'X-Amz-Acl': acl})
            handle_acl_header(req)
            for target in targets:
                self.assertTrue(target[0] in req.headers)
                self.assertEqual(req.headers[target[0]], target[1])

        check_generated_acl_header('public-read',
                                   [('X-Container-Read', '.r:*,.rlistings')])
        check_generated_acl_header('public-read-write',
                                   [('X-Container-Read', '.r:*,.rlistings'),
                                    ('X-Container-Write', '.r:*')])
        check_generated_acl_header('private',
                                   [('X-Container-Read', '.'),
                                    ('X-Container-Write', '.')])


if __name__ == '__main__':
    unittest.main()
