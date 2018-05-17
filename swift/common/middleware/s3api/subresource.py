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
"""
---------------------------
s3api's ACLs implementation
---------------------------
s3api uses a different implementation approach to achieve S3 ACLs.

First, we should understand what we have to design to achieve real S3 ACLs.
Current s3api(real S3)'s ACLs Model is as follows::

    AccessControlPolicy:
        Owner:
        AccessControlList:
            Grant[n]:
                (Grantee, Permission)

Each bucket or object has its own acl consisting of Owner and
AcessControlList. AccessControlList can contain some Grants.
By default, AccessControlList has only one Grant to allow FULL
CONTROLL to owner. Each Grant includes single pair with Grantee,
Permission. Grantee is the user (or user group) allowed the given permission.

This module defines the groups and the relation tree.

If you wanna get more information about S3's ACLs model in detail,
please see official documentation here,

http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html

"""
from functools import partial

from swift.common.utils import json

from swift.common.middleware.s3api.s3response import InvalidArgument, \
    MalformedACLError, S3NotImplemented, InvalidRequest, AccessDenied
from swift.common.middleware.s3api.etree import Element, SubElement, tostring
from swift.common.middleware.s3api.utils import sysmeta_header
from swift.common.middleware.s3api.exception import InvalidSubresource

XMLNS_XSI = 'http://www.w3.org/2001/XMLSchema-instance'
PERMISSIONS = ['FULL_CONTROL', 'READ', 'WRITE', 'READ_ACP', 'WRITE_ACP']
LOG_DELIVERY_USER = '.log_delivery'


def encode_acl(resource, acl):
    """
    Encode an ACL instance to Swift metadata.

    Given a resource type and an ACL instance, this method returns HTTP
    headers, which can be used for Swift metadata.
    """
    header_value = {"Owner": acl.owner.id}
    grants = []
    for grant in acl.grants:
        grant = {"Permission": grant.permission,
                 "Grantee": str(grant.grantee)}
        grants.append(grant)
    header_value.update({"Grant": grants})
    headers = {}
    key = sysmeta_header(resource, 'acl')
    headers[key] = json.dumps(header_value, separators=(',', ':'))

    return headers


def decode_acl(resource, headers, allow_no_owner):
    """
    Decode Swift metadata to an ACL instance.

    Given a resource type and HTTP headers, this method returns an ACL
    instance.
    """
    value = ''

    key = sysmeta_header(resource, 'acl')
    if key in headers:
        value = headers[key]

    if value == '':
        # Fix me: In the case of value is empty or not dict instance,
        # I want an instance of Owner as None.
        # However, in the above process would occur error in reference
        # to an instance variable of Owner.
        return ACL(Owner(None, None), [], True, allow_no_owner)

    try:
        encode_value = json.loads(value)
        if not isinstance(encode_value, dict):
            return ACL(Owner(None, None), [], True, allow_no_owner)

        id = None
        name = None
        grants = []
        if 'Owner' in encode_value:
            id = encode_value['Owner']
            name = encode_value['Owner']
        if 'Grant' in encode_value:
            for grant in encode_value['Grant']:
                grantee = None
                # pylint: disable-msg=E1101
                for group in Group.__subclasses__():
                    if group.__name__ == grant['Grantee']:
                        grantee = group()
                if not grantee:
                    grantee = User(grant['Grantee'])
                permission = grant['Permission']
                grants.append(Grant(grantee, permission))
        return ACL(Owner(id, name), grants, True, allow_no_owner)
    except Exception as e:
        raise InvalidSubresource((resource, 'acl', value), e)


class Grantee(object):
    """
    Base class for grantee.

    Methods:

    * init: create a Grantee instance
    * elem: create an ElementTree from itself

    Static Methods:

    * from_header: convert a grantee string in the HTTP header
                   to an Grantee instance.
    * from_elem: convert a ElementTree to an Grantee instance.

    """
    # Needs confirmation whether we really need these methods or not.
    # * encode (method): create a JSON which includes whole own elements
    # * encode_from_elem (static method): convert from an ElementTree to a JSON
    # * elem_from_json (static method): convert from a JSON to an ElementTree
    # * from_json (static method): convert a Json string to an Grantee
    #                              instance.

    def __contains__(self, key):
        """
        The key argument is a S3 user id.  This method checks that the user id
        belongs to this class.
        """
        raise S3NotImplemented()

    def elem(self):
        """
        Get an etree element of this instance.
        """
        raise S3NotImplemented()

    @staticmethod
    def from_elem(elem):
        type = elem.get('{%s}type' % XMLNS_XSI)
        if type == 'CanonicalUser':
            value = elem.find('./ID').text
            return User(value)
        elif type == 'Group':
            value = elem.find('./URI').text
            subclass = get_group_subclass_from_uri(value)
            return subclass()
        elif type == 'AmazonCustomerByEmail':
            raise S3NotImplemented()
        else:
            raise MalformedACLError()

    @staticmethod
    def from_header(grantee):
        """
        Convert a grantee string in the HTTP header to an Grantee instance.
        """
        type, value = grantee.split('=', 1)
        value = value.strip('"\'')
        if type == 'id':
            return User(value)
        elif type == 'emailAddress':
            raise S3NotImplemented()
        elif type == 'uri':
            # return a subclass instance of Group class
            subclass = get_group_subclass_from_uri(value)
            return subclass()
        else:
            raise InvalidArgument(type, value,
                                  'Argument format not recognized')


class User(Grantee):
    """
    Canonical user class for S3 accounts.
    """
    type = 'CanonicalUser'

    def __init__(self, name):
        self.id = name
        self.display_name = name

    def __contains__(self, key):
        return key == self.id

    def elem(self):
        elem = Element('Grantee', nsmap={'xsi': XMLNS_XSI})
        elem.set('{%s}type' % XMLNS_XSI, self.type)
        SubElement(elem, 'ID').text = self.id
        SubElement(elem, 'DisplayName').text = self.display_name
        return elem

    def __str__(self):
        return self.display_name


class Owner(object):
    """
    Owner class for S3 accounts
    """
    def __init__(self, id, name):
        self.id = id
        self.name = name


def get_group_subclass_from_uri(uri):
    """
    Convert a URI to one of the predefined groups.
    """
    for group in Group.__subclasses__():  # pylint: disable-msg=E1101
        if group.uri == uri:
            return group
    raise InvalidArgument('uri', uri, 'Invalid group uri')


class Group(Grantee):
    """
    Base class for Amazon S3 Predefined Groups
    """
    type = 'Group'
    uri = ''

    def __init__(self):
        # Initialize method to clarify this has nothing to do
        pass

    def elem(self):
        elem = Element('Grantee', nsmap={'xsi': XMLNS_XSI})
        elem.set('{%s}type' % XMLNS_XSI, self.type)
        SubElement(elem, 'URI').text = self.uri

        return elem

    def __str__(self):
        return self.__class__.__name__


def canned_acl_grantees(bucket_owner, object_owner=None):
    """
    A set of predefined grants supported by AWS S3.
    """
    owner = object_owner or bucket_owner

    return {
        'private': [
            ('FULL_CONTROL', User(owner.name)),
        ],
        'public-read': [
            ('READ', AllUsers()),
            ('FULL_CONTROL', User(owner.name)),
        ],
        'public-read-write': [
            ('READ', AllUsers()),
            ('WRITE', AllUsers()),
            ('FULL_CONTROL', User(owner.name)),
        ],
        'authenticated-read': [
            ('READ', AuthenticatedUsers()),
            ('FULL_CONTROL', User(owner.name)),
        ],
        'bucket-owner-read': [
            ('READ', User(bucket_owner.name)),
            ('FULL_CONTROL', User(owner.name)),
        ],
        'bucket-owner-full-control': [
            ('FULL_CONTROL', User(owner.name)),
            ('FULL_CONTROL', User(bucket_owner.name)),
        ],
        'log-delivery-write': [
            ('WRITE', LogDelivery()),
            ('READ_ACP', LogDelivery()),
            ('FULL_CONTROL', User(owner.name)),
        ],
    }


class AuthenticatedUsers(Group):
    """
    This group represents all AWS accounts.  Access permission to this group
    allows any AWS account to access the resource.  However, all requests must
    be signed (authenticated).
    """
    uri = 'http://acs.amazonaws.com/groups/global/AuthenticatedUsers'

    def __contains__(self, key):
        # s3api handles only signed requests.
        return True


class AllUsers(Group):
    """
    Access permission to this group allows anyone to access the resource.  The
    requests can be signed (authenticated) or unsigned (anonymous).  Unsigned
    requests omit the Authentication header in the request.

    Note: s3api regards unsigned requests as Swift API accesses, and bypasses
    them to Swift.  As a result, AllUsers behaves completely same as
    AuthenticatedUsers.
    """
    uri = 'http://acs.amazonaws.com/groups/global/AllUsers'

    def __contains__(self, key):
        return True


class LogDelivery(Group):
    """
    WRITE and READ_ACP permissions on a bucket enables this group to write
    server access logs to the bucket.
    """
    uri = 'http://acs.amazonaws.com/groups/s3/LogDelivery'

    def __contains__(self, key):
        if ':' in key:
            tenant, user = key.split(':', 1)
        else:
            user = key
        return user == LOG_DELIVERY_USER


class Grant(object):
    """
    Grant Class which includes both Grantee and Permission
    """

    def __init__(self, grantee, permission):
        """
        :param grantee: a grantee class or its subclass
        :param permission: string
        """
        if permission.upper() not in PERMISSIONS:
            raise S3NotImplemented()
        if not isinstance(grantee, Grantee):
            raise ValueError()
        self.grantee = grantee
        self.permission = permission

    @classmethod
    def from_elem(cls, elem):
        """
        Convert an ElementTree to an ACL instance
        """
        grantee = Grantee.from_elem(elem.find('./Grantee'))
        permission = elem.find('./Permission').text
        return cls(grantee, permission)

    def elem(self):
        """
        Create an etree element.
        """
        elem = Element('Grant')
        elem.append(self.grantee.elem())
        SubElement(elem, 'Permission').text = self.permission

        return elem

    def allow(self, grantee, permission):
        return permission == self.permission and grantee in self.grantee


class ACL(object):
    """
    S3 ACL class.

    Refs (S3 API - acl-overview:
          http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html):

    The sample ACL includes an Owner element identifying the owner via the
    AWS account's canonical user ID. The Grant element identifies the grantee
    (either an AWS account or a predefined group), and the permission granted.
    This default ACL has one Grant element for the owner. You grant permissions
    by adding Grant elements, each grant identifying the grantee and the
    permission.
    """
    metadata_name = 'acl'
    root_tag = 'AccessControlPolicy'
    max_xml_length = 200 * 1024

    def __init__(self, owner, grants=None, s3_acl=False, allow_no_owner=False):
        """
        :param owner: Owner instance for ACL instance
        :param grants: a list of Grant instances
        :param s3_acl: boolean indicates whether this class is used under
            s3_acl is True or False (from s3api middleware configuration)
        :param allow_no_owner: boolean indicates this ACL instance can be
            handled when no owner information found
        """
        self.owner = owner
        self.grants = grants or []
        self.s3_acl = s3_acl
        self.allow_no_owner = allow_no_owner

    def __repr__(self):
        return tostring(self.elem())

    @classmethod
    def from_elem(cls, elem, s3_acl=False, allow_no_owner=False):
        """
        Convert an ElementTree to an ACL instance
        """
        id = elem.find('./Owner/ID').text
        try:
            name = elem.find('./Owner/DisplayName').text
        except AttributeError:
            name = id

        grants = [Grant.from_elem(e)
                  for e in elem.findall('./AccessControlList/Grant')]
        return cls(Owner(id, name), grants, s3_acl, allow_no_owner)

    def elem(self):
        """
        Decode the value to an ACL instance.
        """
        elem = Element(self.root_tag)

        owner = SubElement(elem, 'Owner')
        SubElement(owner, 'ID').text = self.owner.id
        SubElement(owner, 'DisplayName').text = self.owner.name

        SubElement(elem, 'AccessControlList').extend(
            g.elem() for g in self.grants
        )

        return elem

    def check_owner(self, user_id):
        """
        Check that the user is an owner.
        """
        if not self.s3_acl:
            # Ignore S3api ACL.
            return

        if not self.owner.id:
            if self.allow_no_owner:
                # No owner means public.
                return
            raise AccessDenied()

        if user_id != self.owner.id:
            raise AccessDenied()

    def check_permission(self, user_id, permission):
        """
        Check that the user has a permission.
        """
        if not self.s3_acl:
            # Ignore S3api ACL.
            return

        try:
            # owners have full control permission
            self.check_owner(user_id)
            return
        except AccessDenied:
            pass

        if permission in PERMISSIONS:
            for g in self.grants:
                if g.allow(user_id, 'FULL_CONTROL') or \
                        g.allow(user_id, permission):
                    return

        raise AccessDenied()

    @classmethod
    def from_headers(cls, headers, bucket_owner, object_owner=None,
                     as_private=True):
        """
        Convert HTTP headers to an ACL instance.
        """
        grants = []
        try:
            for key, value in headers.items():
                if key.lower().startswith('x-amz-grant-'):
                    permission = key[len('x-amz-grant-'):]
                    permission = permission.upper().replace('-', '_')
                    if permission not in PERMISSIONS:
                        continue
                    for grantee in value.split(','):
                        grants.append(
                            Grant(Grantee.from_header(grantee), permission))

            if 'x-amz-acl' in headers:
                try:
                    acl = headers['x-amz-acl']
                    if len(grants) > 0:
                        err_msg = 'Specifying both Canned ACLs and Header ' \
                            'Grants is not allowed'
                        raise InvalidRequest(err_msg)
                    grantees = canned_acl_grantees(
                        bucket_owner, object_owner)[acl]
                    for permission, grantee in grantees:
                        grants.append(Grant(grantee, permission))
                except KeyError:
                    # expects canned_acl_grantees()[] raises KeyError
                    raise InvalidArgument('x-amz-acl', headers['x-amz-acl'])
        except (KeyError, ValueError):
            # TODO: think about we really catch this except sequence
            raise InvalidRequest()

        if len(grants) == 0:
            # No ACL headers
            if as_private:
                return ACLPrivate(bucket_owner, object_owner)
            else:
                return None

        return cls(object_owner or bucket_owner, grants)


class CannedACL(object):
    """
    A dict-like object that returns canned ACL.
    """
    def __getitem__(self, key):
        def acl(key, bucket_owner, object_owner=None,
                s3_acl=False, allow_no_owner=False):
            grants = []
            grantees = canned_acl_grantees(bucket_owner, object_owner)[key]
            for permission, grantee in grantees:
                grants.append(Grant(grantee, permission))
            return ACL(object_owner or bucket_owner,
                       grants, s3_acl, allow_no_owner)

        return partial(acl, key)


canned_acl = CannedACL()

ACLPrivate = canned_acl['private']
ACLPublicRead = canned_acl['public-read']
ACLPublicReadWrite = canned_acl['public-read-write']
ACLAuthenticatedRead = canned_acl['authenticated-read']
ACLBucketOwnerRead = canned_acl['bucket-owner-read']
ACLBucketOwnerFullControl = canned_acl['bucket-owner-full-control']
ACLLogDeliveryWrite = canned_acl['log-delivery-write']
