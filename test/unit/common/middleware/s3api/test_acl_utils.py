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
from swift.common.middleware.s3api import s3response
from swift.common.middleware.s3api.acl_utils import handle_acl_header

from test.unit.common.middleware.s3api import S3ApiTestCase


class TestS3ApiAclUtils(S3ApiTestCase):

    def setUp(self):
        super(TestS3ApiAclUtils, self).setUp()

    def check_generated_acl_header(self, acl, expected):
        req = Request.blank('/bucket',
                            headers={'X-Amz-Acl': acl})
        try:
            handle_acl_header(req)
        except s3response.ErrorResponse as e:
            if isinstance(e, expected):
                self.assertEqual(expected._status, e._status)
            else:
                raise
        else:
            for target in expected:
                self.assertTrue(target[0] in req.headers)
                self.assertEqual(req.headers[target[0]], target[1])

    def test_canned_acl_header(self):
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl
        self.check_generated_acl_header(
            'private',
            [('X-Container-Read', '.'), ('X-Container-Write', '.')])
        self.check_generated_acl_header(
            'public-read', [('X-Container-Read', '.r:*,.rlistings')])
        self.check_generated_acl_header(
            'public-read-write', [('X-Container-Read', '.r:*,.rlistings'),
                                  ('X-Container-Write', '.r:*')])
        self.check_generated_acl_header(
            'aws-exec-read', s3response.InvalidArgument)
        self.check_generated_acl_header(
            'authenticated-read', s3response.S3NotImplemented)
        self.check_generated_acl_header(
            'bucket-owner-read', [('X-Container-Read', '.'),
                                  ('X-Container-Write', '.')])
        self.check_generated_acl_header(
            'bucket-owner-full-control', [('X-Container-Read', '.'),
                                          ('X-Container-Write', '.')])
        self.check_generated_acl_header(
            'log-delivery-write', s3response.S3NotImplemented)

        # the 400 response is the catch all
        self.check_generated_acl_header(
            'some-non-sense', s3response.InvalidArgument)


if __name__ == '__main__':
    unittest.main()
