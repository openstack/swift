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

import json
import unittest
from datetime import datetime
from hashlib import md5

from swift.common import swob
from swift.common.swob import Request

from test.unit.common.middleware.s3api import S3ApiTestCase
from test.unit.common.middleware.s3api.helpers import UnreadableInput
from swift.common.middleware.s3api.etree import fromstring, tostring, Element, \
    SubElement
from test.unit.common.middleware.s3api.test_s3_acl import s3acl


class TestS3ApiMultiDelete(S3ApiTestCase):

    def setUp(self):
        super(TestS3ApiMultiDelete, self).setUp()
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPOk, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)

    @s3acl
    def test_object_multi_DELETE_to_object(self):
        elem = Element('Delete')
        obj = SubElement(elem, 'Object')
        SubElement(obj, 'Key').text = 'object'
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket/object?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    @s3acl
    def test_object_multi_DELETE(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key3',
                            swob.HTTPOk,
                            {'x-static-large-object': 'True'},
                            None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)
        slo_delete_resp = {
            'Number Not Found': 0,
            'Response Status': '200 OK',
            'Errors': [],
            'Response Body': '',
            'Number Deleted': 8
        }
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key3',
                            swob.HTTPOk, {}, json.dumps(slo_delete_resp))

        elem = Element('Delete')
        for key in ['Key1', 'Key2', 'Key3']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Content-Type': 'multipart/form-data',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), 3)
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket/Key1'),
            ('DELETE', '/v1/AUTH_test/bucket/Key1'),
            ('HEAD', '/v1/AUTH_test/bucket/Key2'),
            ('HEAD', '/v1/AUTH_test/bucket/Key3'),
            ('DELETE', '/v1/AUTH_test/bucket/Key3?multipart-manifest=delete'),
        ])

    @s3acl
    def test_object_multi_DELETE_with_error(self):
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key3',
                            swob.HTTPForbidden, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key4',
                            swob.HTTPOk,
                            {'x-static-large-object': 'True'},
                            None)
        slo_delete_resp = {
            'Number Not Found': 0,
            'Response Status': '400 Bad Request',
            'Errors': [
                ["/bucket+segments/obj1", "403 Forbidden"],
                ["/bucket+segments/obj2", "403 Forbidden"]
            ],
            'Response Body': '',
            'Number Deleted': 8
        }
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key4',
                            swob.HTTPOk, {}, json.dumps(slo_delete_resp))

        elem = Element('Delete')
        for key in ['Key1', 'Key2', 'Key3', 'Key4']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Content-Type': 'multipart/form-data',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), 2)
        self.assertEqual(len(elem.findall('Error')), 2)
        self.assertEqual(
            [(el.find('Code').text, el.find('Message').text)
             for el in elem.findall('Error')],
            [('AccessDenied', 'Access Denied.'),
             ('SLODeleteError', '\n'.join([
                 '400 Bad Request',
                 '/bucket+segments/obj1: 403 Forbidden',
                 '/bucket+segments/obj2: 403 Forbidden']))]
        )
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket/Key1'),
            ('DELETE', '/v1/AUTH_test/bucket/Key1'),
            ('HEAD', '/v1/AUTH_test/bucket/Key2'),
            ('HEAD', '/v1/AUTH_test/bucket/Key3'),
            ('HEAD', '/v1/AUTH_test/bucket/Key4'),
            ('DELETE', '/v1/AUTH_test/bucket/Key4?multipart-manifest=delete'),
        ])

    @s3acl
    def test_object_multi_DELETE_quiet(self):
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)

        elem = Element('Delete')
        SubElement(elem, 'Quiet').text = 'true'
        for key in ['Key1', 'Key2']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), 0)

    @s3acl
    def test_object_multi_DELETE_no_key(self):
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)

        elem = Element('Delete')
        SubElement(elem, 'Quiet').text = 'true'
        for key in ['Key1', 'Key2']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key')
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'UserKeyMustBeSpecified')

    @s3acl
    def test_object_multi_DELETE_versioned(self):
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)

        elem = Element('Delete')
        SubElement(elem, 'Quiet').text = 'true'
        for key in ['Key1', 'Key2']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
            SubElement(obj, 'VersionId').text = 'not-supported'
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')

    @s3acl
    def test_object_multi_DELETE_with_invalid_md5(self):
        elem = Element('Delete')
        for key in ['Key1', 'Key2']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': 'XXXX'},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')

    @s3acl
    def test_object_multi_DELETE_without_md5(self):
        elem = Element('Delete')
        for key in ['Key1', 'Key2']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')

    @s3acl
    def test_object_multi_DELETE_lots_of_keys(self):
        elem = Element('Delete')
        for i in range(self.conf.max_multi_delete_objects):
            name = 'x' * 1000 + str(i)
            self.swift.register('HEAD', '/v1/AUTH_test/bucket/%s' % name,
                                swob.HTTPNotFound, {}, None)
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = name
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual('200 OK', status)

        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')),
                         self.conf.max_multi_delete_objects)

    @s3acl
    def test_object_multi_DELETE_too_many_keys(self):
        elem = Element('Delete')
        for i in range(self.conf.max_multi_delete_objects + 1):
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = 'x' * 1000 + str(i)
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedXML')

    def _test_object_multi_DELETE(self, account):
        self.keys = ['Key1', 'Key2']
        self.swift.register(
            'DELETE', '/v1/AUTH_test/bucket/%s' % self.keys[0],
            swob.HTTPNoContent, {}, None)
        self.swift.register(
            'DELETE', '/v1/AUTH_test/bucket/%s' % self.keys[1],
            swob.HTTPNotFound, {}, None)

        elem = Element('Delete')
        for key in self.keys:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)
        content_md5 = md5(body).digest().encode('base64').strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        req.date = datetime.now()
        req.content_type = 'text/plain'

        return self.call_s3api(req)

    @s3acl(s3acl_only=True)
    def test_object_multi_DELETE_without_permission(self):
        status, headers, body = self._test_object_multi_DELETE('test:other')
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body)
        errors = elem.findall('Error')
        self.assertEqual(len(errors), len(self.keys))
        for e in errors:
            self.assertTrue(e.find('Key').text in self.keys)
            self.assertEqual(e.find('Code').text, 'AccessDenied')
            self.assertEqual(e.find('Message').text, 'Access Denied.')

    @s3acl(s3acl_only=True)
    def test_object_multi_DELETE_with_write_permission(self):
        status, headers, body = self._test_object_multi_DELETE('test:write')
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), len(self.keys))

    @s3acl(s3acl_only=True)
    def test_object_multi_DELETE_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_object_multi_DELETE('test:full_control')
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), len(self.keys))

    def _test_no_body(self, use_content_length=False,
                      use_transfer_encoding=False, string_to_md5=''):
        content_md5 = md5(string_to_md5).digest().encode('base64').strip()
        with UnreadableInput(self) as fake_input:
            req = Request.blank(
                '/bucket?delete',
                environ={
                    'REQUEST_METHOD': 'POST',
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
        self.assertEqual(self._get_error_code(body), 'MissingRequestBodyError')

    @s3acl
    def test_object_multi_DELETE_empty_body(self):
        self._test_no_body()
        self._test_no_body(string_to_md5='test')
        self._test_no_body(use_content_length=True)
        self._test_no_body(use_content_length=True, string_to_md5='test')
        self._test_no_body(use_transfer_encoding=True)
        self._test_no_body(use_transfer_encoding=True, string_to_md5='test')

if __name__ == '__main__':
    unittest.main()
