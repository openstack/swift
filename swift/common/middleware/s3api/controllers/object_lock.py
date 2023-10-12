# Copyright (c) 2010-2023 OpenStack Foundation
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

from swift.common.utils import public

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation, S3NotImplemented
from swift.common.middleware.s3api.s3response import \
    ObjectLockConfigurationNotFoundError


class ObjectLockController(Controller):
    """
    Handles GET object-lock request, which always returns
    <ObjectLockEnabled>Disabled</ObjectLockEnabled>
    """
    @public
    @bucket_operation
    def GET(self, req):
        """
        Handles GET object-lock param calls.
        """
        raise ObjectLockConfigurationNotFoundError(req.container_name)

    @public
    @bucket_operation
    def PUT(self, req):
        """
        Handles PUT object-lock param calls.
        """
        # Basically we don't support it, so return a 501
        raise S3NotImplemented('The requested resource is not implemented')
