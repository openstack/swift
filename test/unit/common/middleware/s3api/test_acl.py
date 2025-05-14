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

import base64
import unittest
from unittest import mock

from io import BytesIO

from swift.common.swob import Request, HTTPAccepted
from swift.common.middleware.s3api.etree import fromstring, tostring, \
    Element, SubElement, XMLNS_XSI
from swift.common.middleware.s3api.s3response import InvalidArgument
from swift.common.middleware.s3api.acl_utils import handle_acl_header
from swift.common.utils import md5

from test.unit.common.middleware.s3api import S3ApiTestCase, S3ApiTestCaseAcl
from test.unit.common.middleware.s3api.helpers import UnreadableInput


class BaseS3ApiAcl(object):

    def setUp(self):
        super(BaseS3ApiAcl, self).setUp()
        # All ACL API should be called against to existing bucket.
        self.swift.register('PUT', '/v1/AUTH_test/bucket',
                            HTTPAccepted, {}, None)

    def _check_acl(self, owner, body):
        elem = fromstring(body, 'AccessControlPolicy')
        permission = elem.find('./AccessControlList/Grant/Permission').text
        self.assertEqual(permission, 'FULL_CONTROL')
        name = elem.find('./AccessControlList/Grant/Grantee/ID').text
        self.assertEqual(name, owner)

    def test_bucket_acl_GET(self):
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        if not self.s3api.conf.s3_acl:
            self._check_acl('test:tester', body)
        self.assertSetEqual(set((('HEAD', '/v1/AUTH_test/bucket'),)),
                            set(self.swift.calls))

    def _test_put_no_body(self, use_content_length=False,
                          use_transfer_encoding=False, string_to_md5=b''):
        content_md5 = base64.b64encode(
            md5(string_to_md5, usedforsecurity=False).digest()).strip()
        with UnreadableInput(self) as fake_input:
            req = Request.blank(
                '/bucket?acl',
                environ={
                    'REQUEST_METHOD': 'PUT',
                    'wsgi.input': fake_input},
                headers={
                    'Authorization': 'AWS test:tester:hmac',
                    'Date': self.get_date_header(),
                    'Content-MD5': content_md5},
                body='')
            if not use_content_length:
                req.environ.pop('CONTENT_LENGTH')
            if use_transfer_encoding:
                req.environ['HTTP_TRANSFER_ENCODING'] = 'chunked'
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertEqual(self._get_error_code(body), 'MissingSecurityHeader')
        self.assertEqual(self._get_error_message(body),
                         'Your request was missing a required header.')
        self.assertIn(b'<MissingHeaderName>x-amz-acl</MissingHeaderName>',
                      body)

    def test_bucket_fails_with_neither_acl_header_nor_xml_PUT(self):
        self._test_put_no_body()
        self._test_put_no_body(string_to_md5=b'test')
        self._test_put_no_body(use_content_length=True)
        self._test_put_no_body(use_content_length=True, string_to_md5=b'test')
        self._test_put_no_body(use_transfer_encoding=True)
        self._test_put_no_body(use_transfer_encoding=True, string_to_md5=b'zz')

    def test_object_acl_GET(self):
        req = Request.blank('/bucket/object?acl',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        if not self.s3api.conf.s3_acl:
            self._check_acl('test:tester', body)
        self.assertSetEqual(set((('HEAD', '/v1/AUTH_test/bucket/object'),)),
                            set(self.swift.calls))


class TestS3ApiAclNoSetup(BaseS3ApiAcl, S3ApiTestCase):

    def test_bucket_acl_PUT(self):
        elem = Element('AccessControlPolicy')
        owner = SubElement(elem, 'Owner')
        SubElement(owner, 'ID').text = 'id'
        acl = SubElement(elem, 'AccessControlList')
        grant = SubElement(acl, 'Grant')
        grantee = SubElement(grant, 'Grantee', nsmap={'xsi': XMLNS_XSI})
        grantee.set('{%s}type' % XMLNS_XSI, 'Group')
        SubElement(grantee, 'URI').text = \
            'http://acs.amazonaws.com/groups/global/AllUsers'
        SubElement(grant, 'Permission').text = 'READ'

        xml = tostring(elem)
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'wsgi.input': BytesIO(xml)},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Transfer-Encoding': 'chunked'})
        self.assertIsNone(req.content_length)
        self.assertIsNone(req.message_length())
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_bucket_canned_acl_PUT(self):
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-AMZ-ACL': 'public-read'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_bucket_fails_with_both_acl_header_and_xml_PUT(self):
        elem = Element('AccessControlPolicy')
        owner = SubElement(elem, 'Owner')
        SubElement(owner, 'ID').text = 'id'
        acl = SubElement(elem, 'AccessControlList')
        grant = SubElement(acl, 'Grant')
        grantee = SubElement(grant, 'Grantee', nsmap={'xsi': XMLNS_XSI})
        grantee.set('{%s}type' % XMLNS_XSI, 'Group')
        SubElement(grantee, 'URI').text = \
            'http://acs.amazonaws.com/groups/global/AllUsers'
        SubElement(grant, 'Permission').text = 'READ'

        xml = tostring(elem)
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-AMZ-ACL': 'public-read'},
                            body=xml)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body),
                         'UnexpectedContent')

    def test_invalid_xml(self):
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body='invalid')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedACLError')

    def test_handle_acl_header(self):
        def check_generated_acl_header(acl, targets):
            req = Request.blank('/bucket',
                                headers={'X-Amz-Acl': acl})
            handle_acl_header(req)
            for target in targets:
                self.assertTrue(target[0] in req.headers)
                self.assertEqual(req.headers[target[0]], target[1])

        check_generated_acl_header('public-read',
                                   [('X-Container-Read', '.r:*,.rlistings')])
        check_generated_acl_header('public-read-write',
                                   [('X-Container-Read', '.r:*,.rlistings'),
                                    ('X-Container-Write', '.r:*')])
        check_generated_acl_header('private',
                                   [('X-Container-Read', '.'),
                                    ('X-Container-Write', '.')])

    def test_handle_acl_with_invalid_header_string(self):
        req = Request.blank('/bucket', headers={'X-Amz-Acl': 'invalid'})
        with self.assertRaises(InvalidArgument) as cm:
            handle_acl_header(req)
        self.assertTrue('argument_name' in cm.exception.info)
        self.assertEqual(cm.exception.info['argument_name'], 'x-amz-acl')
        self.assertTrue('argument_value' in cm.exception.info)
        self.assertEqual(cm.exception.info['argument_value'], 'invalid')


class TestS3ApiAclCommonSetup(BaseS3ApiAcl, S3ApiTestCaseAcl):

    def test_bucket_canned_acl_PUT_with_s3acl(self):
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-AMZ-ACL': 'public-read'})
        with mock.patch('swift.common.middleware.s3api.s3request.'
                        'handle_acl_header') as mock_handler:
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '200')
            self.assertEqual(mock_handler.call_count, 0)

    def test_handle_acl_header_with_s3acl(self):
        def check_generated_acl_header(acl, targets):
            req = Request.blank('/bucket',
                                headers={'X-Amz-Acl': acl})
            for target in targets:
                self.assertTrue(target not in req.headers)
            self.assertTrue('HTTP_X_AMZ_ACL' in req.environ)
            # TODO: add transration and assertion for s3acl

        check_generated_acl_header('public-read',
                                   ['X-Container-Read'])
        check_generated_acl_header('public-read-write',
                                   ['X-Container-Read', 'X-Container-Write'])
        check_generated_acl_header('private',
                                   ['X-Container-Read', 'X-Container-Write'])


if __name__ == '__main__':
    unittest.main()
