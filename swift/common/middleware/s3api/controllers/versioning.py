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

from swift.common.utils import public, config_true_value
from swift.common.registry import get_swift_info

from swift.common.middleware.s3api.controllers.base import Controller, \
    bucket_operation
from swift.common.middleware.s3api.etree import Element, tostring, \
    fromstring, XMLSyntaxError, DocumentInvalid, SubElement
from swift.common.middleware.s3api.s3response import HTTPOk, \
    S3NotImplemented, MalformedXML

MAX_PUT_VERSIONING_BODY_SIZE = 10240


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
        sysmeta = req.get_container_info(self.app).get('sysmeta', {})

        elem = Element('VersioningConfiguration')
        if sysmeta.get('versions-enabled'):
            SubElement(elem, 'Status').text = (
                'Enabled' if config_true_value(sysmeta['versions-enabled'])
                else 'Suspended')
        body = tostring(elem)

        return HTTPOk(body=body, content_type=None)

    @public
    @bucket_operation
    def PUT(self, req):
        """
        Handles PUT Bucket versioning.
        """
        if 'object_versioning' not in get_swift_info():
            raise S3NotImplemented()

        xml = req.xml(MAX_PUT_VERSIONING_BODY_SIZE)
        try:
            elem = fromstring(xml, 'VersioningConfiguration')
            status = elem.find('./Status').text
        except (XMLSyntaxError, DocumentInvalid):
            raise MalformedXML()
        except Exception as e:
            self.logger.error(e)
            raise

        if status not in ['Enabled', 'Suspended']:
            raise MalformedXML()

        # Set up versioning
        # NB: object_versioning responsible for ensuring its container exists
        req.headers['X-Versions-Enabled'] = str(status == 'Enabled').lower()
        req.get_response(self.app, 'POST')

        return HTTPOk()
