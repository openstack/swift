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

from swift.common.middleware.s3api.exception import ACLError
from swift.common.middleware.s3api.etree import fromstring, XMLSyntaxError, \
    DocumentInvalid, XMLNS_XSI
from swift.common.middleware.s3api.s3response import S3NotImplemented, \
    MalformedACLError, InvalidArgument


def swift_acl_translate(acl, group='', user='', xml=False):
    """
    Takes an S3 style ACL and returns a list of header/value pairs that
    implement that ACL in Swift, or "NotImplemented" if there isn't a way to do
    that yet.
    """
    swift_acl = {}
    swift_acl['public-read'] = [['X-Container-Read', '.r:*,.rlistings']]
    # Swift does not support public write:
    # https://answers.launchpad.net/swift/+question/169541
    swift_acl['public-read-write'] = [['X-Container-Write', '.r:*'],
                                      ['X-Container-Read',
                                       '.r:*,.rlistings']]

    # TODO: if there's a way to get group and user, this should work for
    # private:
    # swift_acl['private'] = \
    #     [['HTTP_X_CONTAINER_WRITE',  group + ':' + user], \
    #      ['HTTP_X_CONTAINER_READ', group + ':' + user]]
    swift_acl['private'] = [['X-Container-Write', '.'],
                            ['X-Container-Read', '.']]
    if xml:
        # We are working with XML and need to parse it
        try:
            elem = fromstring(acl, 'AccessControlPolicy')
        except (XMLSyntaxError, DocumentInvalid):
            raise MalformedACLError()
        acl = 'unknown'
        for grant in elem.findall('./AccessControlList/Grant'):
            permission = grant.find('./Permission').text
            grantee = grant.find('./Grantee').get('{%s}type' % XMLNS_XSI)
            if permission == "FULL_CONTROL" and grantee == 'CanonicalUser' and\
                    acl != 'public-read' and acl != 'public-read-write':
                acl = 'private'
            elif permission == "READ" and grantee == 'Group' and\
                    acl != 'public-read-write':
                acl = 'public-read'
            elif permission == "WRITE" and grantee == 'Group':
                acl = 'public-read-write'
            else:
                acl = 'unsupported'

    if acl == 'authenticated-read':
        raise S3NotImplemented()
    elif acl not in swift_acl:
        raise ACLError()

    return swift_acl[acl]


def handle_acl_header(req):
    """
    Handle the x-amz-acl header.
    Note that this header currently used for only normal-acl
    (not implemented) on s3acl.
    TODO: add translation to swift acl like as x-container-read to s3acl
    """

    amz_acl = req.environ['HTTP_X_AMZ_ACL']
    # Translate the Amazon ACL to something that can be
    # implemented in Swift, 501 otherwise. Swift uses POST
    # for ACLs, whereas S3 uses PUT.
    del req.environ['HTTP_X_AMZ_ACL']
    if req.query_string:
        req.query_string = ''

    try:
        translated_acl = swift_acl_translate(amz_acl)
    except ACLError:
        raise InvalidArgument('x-amz-acl', amz_acl)

    for header, acl in translated_acl:
        req.headers[header] = acl
