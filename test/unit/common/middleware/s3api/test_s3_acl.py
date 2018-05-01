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
import functools
import sys
import traceback
from mock import patch, MagicMock

from swift.common import swob
from swift.common.swob import Request
from swift.common.utils import json

from swift.common.middleware.s3api.etree import tostring, Element, SubElement
from swift.common.middleware.s3api.subresource import ACL, ACLPrivate, User, \
    encode_acl, AuthenticatedUsers, AllUsers, Owner, Grant, PERMISSIONS
from test.unit.common.middleware.s3api.test_s3api import S3ApiTestCase
from test.unit.common.middleware.s3api.exceptions import NotMethodException
from test.unit.common.middleware.s3api import FakeSwift


XMLNS_XSI = 'http://www.w3.org/2001/XMLSchema-instance'


def s3acl(func=None, s3acl_only=False):
    """
    NOTE: s3acl decorator needs an instance of s3api testing framework.
          (i.e. An instance for first argument is necessary)
    """
    if func is None:
        return functools.partial(s3acl, s3acl_only=s3acl_only)

    @functools.wraps(func)
    def s3acl_decorator(*args, **kwargs):
        if not args and not kwargs:
            raise NotMethodException('Use s3acl decorator for a method')

        def call_func(failing_point=''):
            try:
                # For maintainability, we patch 204 status for every
                # get_container_info. if you want, we can rewrite the
                # statement easily with nested decorator like as:
                #
                #  @s3acl
                #  @patch(xxx)
                #  def test_xxxx(self)

                with patch('swift.common.middleware.s3api.s3request.'
                           'get_container_info',
                           return_value={'status': 204}):
                    func(*args, **kwargs)
            except AssertionError:
                # Make traceback message to clarify the assertion
                exc_type, exc_instance, exc_traceback = sys.exc_info()
                formatted_traceback = ''.join(traceback.format_tb(
                    exc_traceback))
                message = '\n%s\n%s:\n%s' % (formatted_traceback,
                                             exc_type.__name__,
                                             exc_instance.message)
                message += failing_point
                raise exc_type(message)

        instance = args[0]

        if not s3acl_only:
            call_func()
            instance.swift._calls = []

        instance.s3api.conf.s3_acl = True
        instance.swift.s3_acl = True
        owner = Owner('test:tester', 'test:tester')
        generate_s3acl_environ('test', instance.swift, owner)
        call_func(' (fail at s3_acl)')

    return s3acl_decorator


def _gen_test_headers(owner, grants=[], resource='container'):
    if not grants:
        grants = [Grant(User('test:tester'), 'FULL_CONTROL')]
    return encode_acl(resource, ACL(owner, grants))


def _make_xml(grantee):
    owner = 'test:tester'
    permission = 'READ'
    elem = Element('AccessControlPolicy')
    elem_owner = SubElement(elem, 'Owner')
    SubElement(elem_owner, 'ID').text = owner
    SubElement(elem_owner, 'DisplayName').text = owner
    acl_list_elem = SubElement(elem, 'AccessControlList')
    elem_grant = SubElement(acl_list_elem, 'Grant')
    elem_grant.append(grantee)
    SubElement(elem_grant, 'Permission').text = permission
    return tostring(elem)


def generate_s3acl_environ(account, swift, owner):

    def gen_grant(permission):
        # generate Grant with a grantee named by "permission"
        account_name = '%s:%s' % (account, permission.lower())
        return Grant(User(account_name), permission)

    grants = map(gen_grant, PERMISSIONS)
    container_headers = _gen_test_headers(owner, grants)
    object_headers = _gen_test_headers(owner, grants, 'object')
    object_body = 'hello'
    object_headers['Content-Length'] = len(object_body)

    # TEST method is used to resolve a tenant name
    swift.register('TEST', '/v1/AUTH_test', swob.HTTPMethodNotAllowed,
                   {}, None)
    swift.register('TEST', '/v1/AUTH_X', swob.HTTPMethodNotAllowed,
                   {}, None)

    # for bucket
    swift.register('HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
                   container_headers, None)
    swift.register('HEAD', '/v1/AUTH_test/bucket+segments', swob.HTTPNoContent,
                   container_headers, None)
    swift.register('PUT', '/v1/AUTH_test/bucket',
                   swob.HTTPCreated, {}, None)
    swift.register('GET', '/v1/AUTH_test/bucket', swob.HTTPNoContent,
                   container_headers, json.dumps([]))
    swift.register('POST', '/v1/AUTH_test/bucket',
                   swob.HTTPNoContent, {}, None)
    swift.register('DELETE', '/v1/AUTH_test/bucket',
                   swob.HTTPNoContent, {}, None)

    # necessary for canned-acl tests
    public_headers = _gen_test_headers(owner, [Grant(AllUsers(), 'READ')])
    swift.register('GET', '/v1/AUTH_test/public', swob.HTTPNoContent,
                   public_headers, json.dumps([]))
    authenticated_headers = _gen_test_headers(
        owner, [Grant(AuthenticatedUsers(), 'READ')], 'bucket')
    swift.register('GET', '/v1/AUTH_test/authenticated',
                   swob.HTTPNoContent, authenticated_headers,
                   json.dumps([]))

    # for object
    swift.register('HEAD', '/v1/AUTH_test/bucket/object', swob.HTTPOk,
                   object_headers, None)


class TestS3ApiS3Acl(S3ApiTestCase):

    def setUp(self):
        super(TestS3ApiS3Acl, self).setUp()

        self.s3api.conf.s3_acl = True
        self.swift.s3_acl = True

        account = 'test'
        owner_name = '%s:tester' % account
        self.default_owner = Owner(owner_name, owner_name)
        generate_s3acl_environ(account, self.swift, self.default_owner)

    def tearDown(self):
        self.s3api.conf.s3_acl = False

    def test_bucket_acl_PUT_with_other_owner(self):
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=tostring(
                                ACLPrivate(
                                    Owner(id='test:other',
                                          name='test:other')).elem()))
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_object_acl_PUT_xml_error(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body="invalid xml")
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedACLError')

    def test_canned_acl_private(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'private'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_canned_acl_public_read(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'public-read'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_canned_acl_public_read_write(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'public-read-write'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_canned_acl_authenticated_read(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'authenticated-read'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_canned_acl_bucket_owner_read(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'bucket-owner-read'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_canned_acl_bucket_owner_full_control(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'bucket-owner-full-control'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_invalid_canned_acl(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-acl': 'invalid'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def _test_grant_header(self, permission):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-' + permission:
                                     'id=test:tester'})
        return self.call_s3api(req)

    def test_grant_read(self):
        status, headers, body = self._test_grant_header('read')
        self.assertEqual(status.split()[0], '200')

    def test_grant_write(self):
        status, headers, body = self._test_grant_header('write')
        self.assertEqual(status.split()[0], '200')

    def test_grant_read_acp(self):
        status, headers, body = self._test_grant_header('read-acp')
        self.assertEqual(status.split()[0], '200')

    def test_grant_write_acp(self):
        status, headers, body = self._test_grant_header('write-acp')
        self.assertEqual(status.split()[0], '200')

    def test_grant_full_control(self):
        status, headers, body = self._test_grant_header('full-control')
        self.assertEqual(status.split()[0], '200')

    def test_grant_invalid_permission(self):
        status, headers, body = self._test_grant_header('invalid')
        self.assertEqual(self._get_error_code(body), 'MissingSecurityHeader')

    def test_grant_with_both_header_and_xml(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-full-control':
                                     'id=test:tester'},
                            body=tostring(
                                ACLPrivate(
                                    Owner(id='test:tester',
                                          name='test:tester')).elem()))
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'UnexpectedContent')

    def test_grant_with_both_header_and_canned_acl(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-full-control':
                                     'id=test:tester',
                                     'x-amz-acl': 'public-read'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    def test_grant_email(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-read': 'emailAddress=a@b.c'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')

    def test_grant_email_xml(self):
        grantee = Element('Grantee', nsmap={'xsi': XMLNS_XSI})
        grantee.set('{%s}type' % XMLNS_XSI, 'AmazonCustomerByEmail')
        SubElement(grantee, 'EmailAddress').text = 'Grantees@email.com'
        xml = _make_xml(grantee=grantee)
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')

    def test_grant_invalid_group_xml(self):
        grantee = Element('Grantee', nsmap={'xsi': XMLNS_XSI})
        grantee.set('{%s}type' % XMLNS_XSI, 'Invalid')
        xml = _make_xml(grantee=grantee)
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedACLError')

    def test_grant_authenticated_users(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-read':
                                     'uri="http://acs.amazonaws.com/groups/'
                                     'global/AuthenticatedUsers"'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_grant_all_users(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-read':
                                     'uri="http://acs.amazonaws.com/groups/'
                                     'global/AllUsers"'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_grant_invalid_uri(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-read':
                                     'uri="http://localhost/"'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_grant_invalid_uri_xml(self):
        grantee = Element('Grantee', nsmap={'xsi': XMLNS_XSI})
        grantee.set('{%s}type' % XMLNS_XSI, 'Group')
        SubElement(grantee, 'URI').text = 'invalid'
        xml = _make_xml(grantee)

        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_grant_invalid_target(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-grant-read': 'key=value'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def _test_bucket_acl_GET(self, account):
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

    def test_bucket_acl_GET_without_permission(self):
        status, headers, body = self._test_bucket_acl_GET('test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_bucket_acl_GET_with_read_acp_permission(self):
        status, headers, body = self._test_bucket_acl_GET('test:read_acp')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_acl_GET_with_fullcontrol_permission(self):
        status, headers, body = self._test_bucket_acl_GET('test:full_control')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_acl_GET_with_owner_permission(self):
        status, headers, body = self._test_bucket_acl_GET('test:tester')
        self.assertEqual(status.split()[0], '200')

    def _test_bucket_acl_PUT(self, account, permission='FULL_CONTROL'):
        acl = ACL(self.default_owner, [Grant(User(account), permission)])
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()},
                            body=tostring(acl.elem()))

        return self.call_s3api(req)

    def test_bucket_acl_PUT_without_permission(self):
        status, headers, body = self._test_bucket_acl_PUT('test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_bucket_acl_PUT_with_write_acp_permission(self):
        status, headers, body = self._test_bucket_acl_PUT('test:write_acp')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_acl_PUT_with_fullcontrol_permission(self):
        status, headers, body = self._test_bucket_acl_PUT('test:full_control')
        self.assertEqual(status.split()[0], '200')

    def test_bucket_acl_PUT_with_owner_permission(self):
        status, headers, body = self._test_bucket_acl_PUT('test:tester')
        self.assertEqual(status.split()[0], '200')

    def _test_object_acl_GET(self, account):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

    def test_object_acl_GET_without_permission(self):
        status, headers, body = self._test_object_acl_GET('test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_object_acl_GET_with_read_acp_permission(self):
        status, headers, body = self._test_object_acl_GET('test:read_acp')
        self.assertEqual(status.split()[0], '200')

    def test_object_acl_GET_with_fullcontrol_permission(self):
        status, headers, body = self._test_object_acl_GET('test:full_control')
        self.assertEqual(status.split()[0], '200')

    def test_object_acl_GET_with_owner_permission(self):
        status, headers, body = self._test_object_acl_GET('test:tester')
        self.assertEqual(status.split()[0], '200')

    def _test_object_acl_PUT(self, account, permission='FULL_CONTROL'):
        acl = ACL(self.default_owner, [Grant(User(account), permission)])
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()},
                            body=tostring(acl.elem()))

        return self.call_s3api(req)

    def test_object_acl_PUT_without_permission(self):
        status, headers, body = self._test_object_acl_PUT('test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_object_acl_PUT_with_write_acp_permission(self):
        status, headers, body = self._test_object_acl_PUT('test:write_acp')
        self.assertEqual(status.split()[0], '200')

    def test_object_acl_PUT_with_fullcontrol_permission(self):
        status, headers, body = self._test_object_acl_PUT('test:full_control')
        self.assertEqual(status.split()[0], '200')

    def test_object_acl_PUT_with_owner_permission(self):
        status, headers, body = self._test_object_acl_PUT('test:tester')
        self.assertEqual(status.split()[0], '200')

    def test_s3acl_decorator(self):
        @s3acl
        def non_class_s3acl_error():
            raise TypeError()

        class FakeClass(object):
            def __init__(self):
                self.s3api = MagicMock()
                self.swift = FakeSwift()

            @s3acl
            def s3acl_error(self):
                raise TypeError()

            @s3acl
            def s3acl_assert_fail(self):
                assert False

            @s3acl(s3acl_only=True)
            def s3acl_s3only_error(self):
                if self.s3api.conf.s3_acl:
                    raise TypeError()

            @s3acl(s3acl_only=True)
            def s3acl_s3only_no_error(self):
                if not self.s3api.conf.s3_acl:
                    raise TypeError()

        fake_class = FakeClass()

        self.assertRaises(NotMethodException, non_class_s3acl_error)
        self.assertRaises(TypeError, fake_class.s3acl_error)
        self.assertRaises(AssertionError, fake_class.s3acl_assert_fail)
        self.assertRaises(TypeError, fake_class.s3acl_s3only_error)
        self.assertIsNone(fake_class.s3acl_s3only_no_error())

if __name__ == '__main__':
    unittest.main()
