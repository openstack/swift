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

from swift3.acl_handlers import S3AclHandler, BucketAclHandler, \
    ObjectAclHandler, BaseAclHandler, PartAclHandler, UploadAclHandler, \
    UploadsAclHandler, get_acl_handler


class TestAclHandlers(unittest.TestCase):
    def test_get_acl_handler(self):
        expected_handlers = (('Bucket', BucketAclHandler),
                             ('Object', ObjectAclHandler),
                             ('S3Acl', S3AclHandler),
                             ('Part', PartAclHandler),
                             ('Upload', UploadAclHandler),
                             ('Uploads', UploadsAclHandler),
                             ('Foo', BaseAclHandler))
        for name, expected in expected_handlers:
            handler = get_acl_handler(name)
            self.assertTrue(issubclass(handler, expected))

    def test_handle_acl(self):
        # we have already have tests for s3_acl checking at test_s3_acl.py
        pass


if __name__ == '__main__':
    unittest.main()
