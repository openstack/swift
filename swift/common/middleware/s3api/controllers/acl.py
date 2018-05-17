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

from swift.common.http import HTTP_OK
from swift.common.middleware.acl import parse_acl, referrer_allowed
from swift.common.utils import public

from swift.common.middleware.s3api.exception import ACLError
from swift.common.middleware.s3api.controllers.base import Controller
from swift.common.middleware.s3api.s3response import HTTPOk, S3NotImplemented, \
    MalformedACLError, UnexpectedContent, MissingSecurityHeader
from swift.common.middleware.s3api.etree import Element, SubElement, tostring
from swift.common.middleware.s3api.acl_utils import swift_acl_translate, \
    XMLNS_XSI


MAX_ACL_BODY_SIZE = 200 * 1024


def get_acl(account_name, headers):
    """
    Attempts to construct an S3 ACL based on what is found in the swift headers
    """

    elem = Element('AccessControlPolicy')
    owner = SubElement(elem, 'Owner')
    SubElement(owner, 'ID').text = account_name
    SubElement(owner, 'DisplayName').text = account_name
    access_control_list = SubElement(elem, 'AccessControlList')

    # grant FULL_CONTROL to myself by default
    grant = SubElement(access_control_list, 'Grant')
    grantee = SubElement(grant, 'Grantee', nsmap={'xsi': XMLNS_XSI})
    grantee.set('{%s}type' % XMLNS_XSI, 'CanonicalUser')
    SubElement(grantee, 'ID').text = account_name
    SubElement(grantee, 'DisplayName').text = account_name
    SubElement(grant, 'Permission').text = 'FULL_CONTROL'

    referrers, _ = parse_acl(headers.get('x-container-read'))
    if referrer_allowed('unknown', referrers):
        # grant public-read access
        grant = SubElement(access_control_list, 'Grant')
        grantee = SubElement(grant, 'Grantee', nsmap={'xsi': XMLNS_XSI})
        grantee.set('{%s}type' % XMLNS_XSI, 'Group')
        SubElement(grantee, 'URI').text = \
            'http://acs.amazonaws.com/groups/global/AllUsers'
        SubElement(grant, 'Permission').text = 'READ'

    referrers, _ = parse_acl(headers.get('x-container-write'))
    if referrer_allowed('unknown', referrers):
        # grant public-write access
        grant = SubElement(access_control_list, 'Grant')
        grantee = SubElement(grant, 'Grantee', nsmap={'xsi': XMLNS_XSI})
        grantee.set('{%s}type' % XMLNS_XSI, 'Group')
        SubElement(grantee, 'URI').text = \
            'http://acs.amazonaws.com/groups/global/AllUsers'
        SubElement(grant, 'Permission').text = 'WRITE'

    body = tostring(elem)

    return HTTPOk(body=body, content_type="text/plain")


class AclController(Controller):
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
        resp = req.get_response(self.app, method='HEAD')

        return get_acl(req.user_id, resp.headers)

    @public
    def PUT(self, req):
        """
        Handles PUT Bucket acl and PUT Object acl.
        """
        if req.is_object_request:
            # Handle Object ACL
            raise S3NotImplemented()
        else:
            # Handle Bucket ACL
            xml = req.xml(MAX_ACL_BODY_SIZE)
            if all(['HTTP_X_AMZ_ACL' in req.environ, xml]):
                # S3 doesn't allow to give ACL with both ACL header and body.
                raise UnexpectedContent()
            elif not any(['HTTP_X_AMZ_ACL' in req.environ, xml]):
                # Both canned ACL header and xml body are missing
                raise MissingSecurityHeader(missing_header_name='x-amz-acl')
            else:
                # correct ACL exists in the request
                if xml:
                    # We very likely have an XML-based ACL request.
                    # let's try to translate to the request header
                    try:
                        translated_acl = swift_acl_translate(xml, xml=True)
                    except ACLError:
                        raise MalformedACLError()

                    for header, acl in translated_acl:
                        req.headers[header] = acl

            resp = req.get_response(self.app, 'POST')
            resp.status = HTTP_OK
            resp.headers.update({'Location': req.container_name})

            return resp
