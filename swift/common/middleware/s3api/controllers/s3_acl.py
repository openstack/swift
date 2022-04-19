# Copyright (c) 2014 OpenStack Foundation.
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

from six.moves.urllib.parse import quote
from swift.common.utils import public

from swift.common.middleware.s3api.controllers.base import Controller
from swift.common.middleware.s3api.s3response import HTTPOk
from swift.common.middleware.s3api.etree import tostring


class S3AclController(Controller):
    """
    Handles the following APIs:

    * GET Bucket acl
    * PUT Bucket acl
    * GET Object acl
    * PUT Object acl

    Those APIs are logged as ACL operations in the S3 server log.
    """
    @public
    def GET(self, req):
        """
        Handles GET Bucket acl and GET Object acl.
        """
        resp = req.get_response(self.app)

        acl = resp.object_acl if req.is_object_request else resp.bucket_acl

        resp = HTTPOk()
        resp.body = tostring(acl.elem())

        return resp

    @public
    def PUT(self, req):
        """
        Handles PUT Bucket acl and PUT Object acl.
        """
        if req.is_object_request:
            headers = {}
            src_path = '/%s/%s' % (req.container_name, req.object_name)

            # object-sysmeta' can be updated by 'Copy' method,
            # but can not be by 'POST' method.
            # So headers['X-Copy-From'] for copy request is added here.
            headers['X-Copy-From'] = quote(src_path)
            headers['Content-Length'] = 0
            req.get_response(self.app, 'PUT', headers=headers)
        else:
            req.get_response(self.app, 'POST')

        return HTTPOk()
