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
    def test_controller_acl_handlers(self):
        expected_handlers = (
            (controllers.ServiceController, acl_handlers.BaseAclHandler),
            (controllers.BucketController, acl_handlers.BucketAclHandler),
            (controllers.ObjectController, acl_handlers.ObjectAclHandler),
            (controllers.AclController, acl_handlers.BaseAclHandler),
            (controllers.S3AclController, acl_handlers.S3AclHandler),
            (controllers.MultiObjectDeleteController,
             acl_handlers.MultiObjectDeleteAclHandler),
            (controllers.PartController, acl_handlers.PartAclHandler),
            (controllers.UploadController, acl_handlers.UploadAclHandler),
            (controllers.UploadsController, acl_handlers.UploadsAclHandler),
            (controllers.LocationController, acl_handlers.BaseAclHandler),
            (controllers.LoggingStatusController,
             acl_handlers.BaseAclHandler),
            (controllers.VersioningController, acl_handlers.BaseAclHandler),
            (controllers.TaggingController, acl_handlers.BaseAclHandler),
            (controllers.ObjectLockController, acl_handlers.BaseAclHandler),
            (controllers.UnsupportedController, None),
        )
        for controller, expected in expected_handlers:
            self.assertIs(controller.acl_handler, expected)

    def test_base_controller_has_no_acl_handler_default(self):
        self.assertFalse(hasattr(controllers.Controller, 'acl_handler'))

    def test_controller_requires_acl_handler(self):
        with self.assertRaises(TypeError) as caught:
            class MissingAclHandlerController(controllers.Controller):
                pass
        self.assertEqual(
            'MissingAclHandlerController must define acl_handler on the '
            'controller class; do not rely on inherited ACL handler lookup',
            str(caught.exception))

    def test_controller_cannot_inherit_acl_handler(self):
        class ParentController(controllers.Controller):
            acl_handler = acl_handlers.BaseAclHandler

        with self.assertRaises(TypeError) as caught:
            class ChildController(ParentController):
                pass
        self.assertEqual(
            'ChildController must define acl_handler on the controller class; '
            'do not rely on inherited ACL handler lookup',
            str(caught.exception))

    def test_controller_resource_type_uses_custom_name(self):
        class CopyObjectResultController(controllers.Controller):
            acl_handler = acl_handlers.BaseAclHandler

            @classmethod
            def name(cls):
                return 'CopyObjectResult'

        self.assertEqual('COPY_OBJECT_RESULT',
                         CopyObjectResultController.resource_type())

    def test_handle_acl(self):
        # we have already have tests for s3_acl checking at test_s3_acl.py
        pass


if __name__ == '__main__':
    unittest.main()
