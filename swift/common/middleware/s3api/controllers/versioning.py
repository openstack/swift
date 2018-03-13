# Copyright (c) 2010-2014 OpenStack Foundation.
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
    bucket_operation
from swift.common.middleware.s3api.etree import Element, tostring
from swift.common.middleware.s3api.s3response import HTTPOk, S3NotImplemented


class VersioningController(Controller):
    """
    Handles the following APIs:

    * GET Bucket versioning
    * PUT Bucket versioning

    Those APIs are logged as VERSIONING operations in the S3 server log.
    """
    @public
    @bucket_operation
    def GET(self, req):
        """
        Handles GET Bucket versioning.
        """
        req.get_response(self.app, method='HEAD')

        # Just report there is no versioning configured here.
        elem = Element('VersioningConfiguration')
        body = tostring(elem)

        return HTTPOk(body=body, content_type="text/plain")

    @public
    @bucket_operation
    def PUT(self, req):
        """
        Handles PUT Bucket versioning.
        """
        raise S3NotImplemented()
