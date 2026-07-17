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

from swift.common.middleware.s3api import acl_handlers
from swift.common.middleware.s3api import controllers


class TestAclHandlers(unittest.TestCase):
    def test_get_acl_handlers(self):
        class FooController(controllers.Controller):
            pass

        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(controllers.BucketController),
                acl_handlers.BucketAclHandler))
        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(controllers.ObjectController),
                acl_handlers.ObjectAclHandler))
        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(controllers.S3AclController),
                acl_handlers.S3AclHandler))
        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(controllers.PartController),
                acl_handlers.PartAclHandler))
        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(controllers.UploadController),
                acl_handlers.UploadAclHandler))
        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(controllers.UploadsController),
                acl_handlers.UploadsAclHandler))
        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(controllers.NativePartController),
                acl_handlers.NativePartAclHandler))
        self.assertTrue(
            issubclass(
                acl_handlers.get_acl_handler(
                    controllers.NativeUploadController),
                acl_handlers.NativeUploadAclHandler))
        self.assertTrue(
            issubclass(acl_handlers.get_acl_handler(
                controllers.NativeUploadsController),
                acl_handlers.NativeUploadsAclHandler))
        self.assertTrue(
            issubclass(acl_handlers.get_acl_handler(FooController),
                       acl_handlers.BaseAclHandler))

    def test_handle_acl(self):
        # we have already have tests for s3_acl checking at test_s3_acl.py
        pass


if __name__ == '__main__':
    unittest.main()
