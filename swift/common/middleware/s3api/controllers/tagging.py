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

from swift.common.utils import public

from swift.common.middleware.s3api.controllers.base import Controller, \
    S3NotImplemented
from swift.common.middleware.s3api.s3response import HTTPOk
from swift.common.middleware.s3api.etree import Element, tostring, \
    SubElement


class TaggingController(Controller):
    """
    Handles the following APIs:

    * GET Bucket and Object tagging
    * PUT Bucket and Object tagging
    * DELETE Bucket and Object tagging

    """
    @public
    def GET(self, req):
        """
        Handles GET Bucket and Object tagging.
        """
        elem = Element('Tagging')
        SubElement(elem, 'TagSet')
        body = tostring(elem)

        return HTTPOk(body=body, content_type=None)

    @public
    def PUT(self, req):
        """
        Handles PUT Bucket and Object tagging.
        """
        raise S3NotImplemented('The requested resource is not implemented')

    @public
    def DELETE(self, req):
        """
        Handles DELETE Bucket and Object tagging.
        """
        raise S3NotImplemented('The requested resource is not implemented')
