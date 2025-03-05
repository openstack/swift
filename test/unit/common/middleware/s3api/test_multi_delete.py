# -*- coding: utf-8 -*-
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
import hashlib
import json
import unittest
from datetime import datetime
from unittest import mock

from swift.common import swob
from swift.common.swob import Request

from test.unit import make_timestamp_iter
from test.unit.common.middleware.s3api import S3ApiTestCase, S3ApiTestCaseAcl
from test.unit.common.middleware.s3api.helpers import UnreadableInput
from swift.common.middleware.s3api.etree import fromstring, tostring, \
    Element, SubElement
from swift.common.utils import md5


class BaseS3ApiMultiDelete(object):

    def setUp(self):
        super(BaseS3ApiMultiDelete, self).setUp()
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPOk, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)
        self.swift.register('HEAD',
                            '/v1/AUTH_test/bucket/business/caf\xc3\xa9',
                            swob.HTTPOk, {}, None)
        self.ts = make_timestamp_iter()

    def test_object_multi_DELETE_to_object(self):
        elem = Element('Delete')
        obj = SubElement(elem, 'Object')
        SubElement(obj, 'Key').text = 'object'
        body = tostring(elem, use_s3ns=False)
        content_md5 = base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip()

        req = Request.blank('/bucket/object?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_object_multi_DELETE_no_content_md5(self):
        elem = Element('Delete')
        obj = SubElement(elem, 'Object')
        SubElement(obj, 'Key').text = 'object'
        body = tostring(elem, use_s3ns=False)

        req = Request.blank('/bucket/object?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     },
                            body=body)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertIn(b'Missing required header', body)
        self.assertIn(b'Content-MD5', body)

    def test_object_multi_DELETE_sha256_invalid(self):
        elem = Element('Delete')
        obj = SubElement(elem, 'Object')
        SubElement(obj, 'Key').text = 'object'
        body = tostring(elem, use_s3ns=False)
        content_sha256 = 'invalid'

        req = Request.blank('/bucket/object?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-Amz-Content-SHA256': content_sha256,
                                     },
                            body=body)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body),
                         'XAmzContentSHA256Mismatch')
        self.assertIn(b"provided 'x-amz-content-sha256' header "
                      b"does not match", body)

    def test_object_multi_DELETE_sha256_bad(self):
        elem = Element('Delete')
        obj = SubElement(elem, 'Object')
        SubElement(obj, 'Key').text = 'object'
        body = tostring(elem, use_s3ns=False)
        content_sha256 = hashlib.sha256(body[:-1]).hexdigest()

        req = Request.blank('/bucket/object?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-Amz-Content-SHA256': content_sha256,
                                     },
                            body=body)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body),
                         'XAmzContentSHA256Mismatch')
        self.assertIn(b"provided 'x-amz-content-sha256' header "
                      b"does not match", body)

    def test_object_multi_DELETE_sha256_valid(self):
        elem = Element('Delete')
        obj = SubElement(elem, 'Object')
        SubElement(obj, 'Key').text = 'object'
        body = tostring(elem, use_s3ns=False)
        content_sha256 = hashlib.sha256(body).hexdigest()

        req = Request.blank('/bucket/object?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'X-Amz-Content-SHA256': content_sha256,
                                     },
                            body=body)

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_object_multi_DELETE(self):
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key3',
                            swob.HTTPOk,
                            {'x-static-large-object': 'True'},
                            None)
        self.swift.register('DELETE',
                            '/v1/AUTH_test/bucket/business/caf\xc3\xa9',
                            swob.HTTPNoContent, {}, None)
        slo_delete_resp = {
            'Number Not Found': 0,
            'Response Status': '200 OK',
            'Errors': [],
            'Response Body': '',
            'Number Deleted': 8
        }
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key3',
                            swob.HTTPOk, {}, json.dumps(slo_delete_resp))
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/Key4',
                            swob.HTTPOk,
                            {'x-static-large-object': 'True',
                             'x-object-sysmeta-s3api-etag': 'some-etag'},
                            None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key4',
                            swob.HTTPNoContent, {}, None)

        elem = Element('Delete')
        for key in ['Key1', 'Key2', 'Key3', 'Key4', 'business/café']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)
        content_md5 = base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Content-Type': 'multipart/form-data',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        with self.stubbed_container_info():
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), 5)
        self.assertEqual(len(elem.findall('Error')), 0)
        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket/Key1?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/Key1'),
            ('HEAD', '/v1/AUTH_test/bucket/Key2?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/Key2'),
            ('HEAD', '/v1/AUTH_test/bucket/Key3?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/Key3?multipart-manifest=delete'),
            ('HEAD', '/v1/AUTH_test/bucket/Key4?symlink=get'),
            ('DELETE',
             '/v1/AUTH_test/bucket/Key4?async=on&multipart-manifest=delete'),
            ('HEAD', '/v1/AUTH_test/bucket/business/caf\xc3\xa9?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/business/caf\xc3\xa9'),
        ])

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
        content_md5 = base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Content-Type': 'multipart/form-data',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        with self.stubbed_container_info():
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
            ('HEAD', '/v1/AUTH_test/bucket/Key1?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/Key1'),
            ('HEAD', '/v1/AUTH_test/bucket/Key2?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/Key2'),
            ('HEAD', '/v1/AUTH_test/bucket/Key3?symlink=get'),
            ('HEAD', '/v1/AUTH_test/bucket/Key4?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/Key4?multipart-manifest=delete'),
        ])

    def test_object_multi_DELETE_with_non_json(self):
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
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key4',
                            swob.HTTPOk, {}, b'asdf')

        elem = Element('Delete')
        for key in ['Key1', 'Key2', 'Key3', 'Key4']:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)
        content_md5 = base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip()

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
            [tuple(el.find(x).text for x in ('Key', 'Code', 'Message'))
             for el in elem.findall('Error')],
            [('Key3', 'AccessDenied', 'Access Denied.'),
             ('Key4', 'SLODeleteError', 'Unexpected swift response')])

        self.assertEqual(self.s3api.logger.get_lines_for_level('error'), [
            'Could not parse SLO delete response (200 OK): %s: ' % b'asdf'])
        self.s3api.logger.clear()

    def test_object_multi_DELETE_quiet(self):
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            swob.HTTPNoContent, {}, None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key2',
                            swob.HTTPNotFound, {}, None)

        for true_value in ('true', 'True', 'TRUE', 'trUE'):
            elem = Element('Delete')
            SubElement(elem, 'Quiet').text = true_value
            for key in ['Key1', 'Key2']:
                obj = SubElement(elem, 'Object')
                SubElement(obj, 'Key').text = key
            body = tostring(elem, use_s3ns=False)
            content_md5 = base64.b64encode(
                md5(body, usedforsecurity=False).digest()).strip()

            req = Request.blank('/bucket?delete',
                                environ={'REQUEST_METHOD': 'POST'},
                                headers={
                                    'Authorization': 'AWS test:tester:hmac',
                                    'Date': self.get_date_header(),
                                    'Content-MD5': content_md5},
                                body=body)
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '200')

            elem = fromstring(body)
            self.assertEqual(len(elem.findall('Deleted')), 0)

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
        content_md5 = base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'UserKeyMustBeSpecified')

    def test_object_multi_DELETE_versioned_enabled(self):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent, {
                'X-Container-Sysmeta-Versions-Enabled': 'True',
            }, None)
        t1 = next(self.ts)
        key1 = '/v1/AUTH_test/bucket/Key1' \
            '?symlink=get&version-id=%s' % t1.normal
        self.swift.register('HEAD', key1, swob.HTTPOk, {}, None)
        self.swift.register('DELETE', key1, swob.HTTPNoContent, {}, None)
        t2 = next(self.ts)
        key2 = '/v1/AUTH_test/bucket/Key2' \
            '?symlink=get&version-id=%s' % t2.normal
        # this 404 could just mean it's a delete marker
        self.swift.register('HEAD', key2, swob.HTTPNotFound, {}, None)
        self.swift.register('DELETE', key2, swob.HTTPNoContent, {}, None)
        key3 = '/v1/AUTH_test/bucket/Key3'
        self.swift.register('HEAD', key3 + '?symlink=get',
                            swob.HTTPOk, {}, None)
        self.swift.register('DELETE', key3, swob.HTTPNoContent, {}, None)
        key4 = '/v1/AUTH_test/bucket/Key4?symlink=get&version-id=null'
        self.swift.register('HEAD', key4, swob.HTTPOk, {}, None)
        self.swift.register('DELETE', key4, swob.HTTPNoContent, {}, None)

        elem = Element('Delete')
        items = (
            ('Key1', t1.normal),
            ('Key2', t2.normal),
            ('Key3', None),
            ('Key4', 'null'),
        )
        for key, version in items:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
            if version:
                SubElement(obj, 'VersionId').text = version
        body = tostring(elem, use_s3ns=False)
        content_md5 = base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        # XXX versioning_enabled=True not required?
        with self.stubbed_container_info():
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', key1),
            ('DELETE', key1),
            ('HEAD', key2),
            ('DELETE', key2),
            ('HEAD', key3 + '?symlink=get'),
            ('DELETE', key3),
            ('HEAD', key4),
            ('DELETE', key4),
        ])

        elem = fromstring(body)
        self.assertEqual({'Key1', 'Key2', 'Key3', 'Key4'}, set(
            e.findtext('Key') for e in elem.findall('Deleted')))

    def test_object_multi_DELETE_versioned_suspended(self):
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket', swob.HTTPNoContent, {}, None)
        t1 = next(self.ts)
        key1 = '/v1/AUTH_test/bucket/Key1' + \
            '?symlink=get&version-id=%s' % t1.normal
        self.swift.register('HEAD', key1, swob.HTTPOk, {}, None)
        self.swift.register('DELETE', key1, swob.HTTPNoContent, {}, None)
        t2 = next(self.ts)
        key2 = '/v1/AUTH_test/bucket/Key2' + \
            '?symlink=get&version-id=%s' % t2.normal
        self.swift.register('HEAD', key2, swob.HTTPNotFound, {}, None)
        self.swift.register('DELETE', key2, swob.HTTPNotFound, {}, None)
        key3 = '/v1/AUTH_test/bucket/Key3'
        self.swift.register('HEAD', key3, swob.HTTPOk, {}, None)
        self.swift.register('DELETE', key3, swob.HTTPNoContent, {}, None)

        elem = Element('Delete')
        items = (
            ('Key1', t1),
            ('Key2', t2),
            ('Key3', None),
        )
        for key, ts in items:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
            if ts:
                SubElement(obj, 'VersionId').text = ts.normal
        body = tostring(elem, use_s3ns=False)
        content_md5 = base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip()

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        # XXX versioning_enabled=True not required?
        with self.stubbed_container_info():
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), 3)

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('HEAD', '/v1/AUTH_test/bucket/Key1'
             '?symlink=get&version-id=%s' % t1.normal),
            ('DELETE', '/v1/AUTH_test/bucket/Key1'
             '?symlink=get&version-id=%s' % t1.normal),
            ('HEAD', '/v1/AUTH_test/bucket/Key2'
             '?symlink=get&version-id=%s' % t2.normal),
            ('DELETE', '/v1/AUTH_test/bucket/Key2'
             '?symlink=get&version-id=%s' % t2.normal),
            ('HEAD', '/v1/AUTH_test/bucket/Key3?symlink=get'),
            ('DELETE', '/v1/AUTH_test/bucket/Key3'),
        ])

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

    def test_object_multi_DELETE_lots_of_keys(self):
        elem = Element('Delete')
        for i in range(self.s3api.conf.max_multi_delete_objects):
            status = swob.HTTPOk if i % 2 else swob.HTTPNotFound
            name = 'x' * 1000 + str(i)
            self.swift.register('HEAD', '/v1/AUTH_test/bucket/%s' % name,
                                status, {}, None)
            self.swift.register('DELETE', '/v1/AUTH_test/bucket/%s' % name,
                                swob.HTTPNoContent, {}, None)
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = name
        body = tostring(elem, use_s3ns=False)
        content_md5 = (base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip())

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
                         self.s3api.conf.max_multi_delete_objects)

    def test_object_multi_DELETE_too_many_keys(self):
        elem = Element('Delete')
        for i in range(self.s3api.conf.max_multi_delete_objects + 1):
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = 'x' * 1000 + str(i)
        body = tostring(elem, use_s3ns=False)
        content_md5 = (base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip())

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MalformedXML')

    def test_object_multi_DELETE_unhandled_exception(self):
        exploding_resp = mock.MagicMock(
            side_effect=Exception('kaboom'))
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/Key1',
                            exploding_resp, {}, None)
        elem = Element('Delete')
        obj = SubElement(elem, 'Object')
        SubElement(obj, 'Key').text = 'Key1'
        body = tostring(elem, use_s3ns=False)
        content_md5 = (base64.b64encode(
            md5(body, usedforsecurity=False).digest()).strip())

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self.assertIn(b'<Error><Key>Key1</Key><Code>Server Error</Code>', body)

    def _test_no_body(self, use_content_length=False,
                      use_transfer_encoding=False, string_to_md5=b''):
        content_md5 = (base64.b64encode(
            md5(string_to_md5, usedforsecurity=False).digest())
            .strip())
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

    def test_object_multi_DELETE_empty_body(self):
        self._test_no_body()
        self._test_no_body(string_to_md5=b'test')
        self._test_no_body(use_content_length=True)
        self._test_no_body(use_content_length=True, string_to_md5=b'test')
        self._test_no_body(use_transfer_encoding=True)
        self._test_no_body(use_transfer_encoding=True, string_to_md5=b'test')


class TestS3ApiMultiDeleteNoAcl(BaseS3ApiMultiDelete, S3ApiTestCase):

    def test_object_multi_DELETE_with_system_entity(self):
        self.keys = ['Key1', 'Key2']
        self.swift.register(
            'DELETE', '/v1/AUTH_test/bucket/%s' % self.keys[0],
            swob.HTTPNotFound, {}, None)
        self.swift.register(
            'DELETE', '/v1/AUTH_test/bucket/%s' % self.keys[1],
            swob.HTTPNoContent, {}, None)

        elem = Element('Delete')
        for key in self.keys:
            obj = SubElement(elem, 'Object')
            SubElement(obj, 'Key').text = key
        body = tostring(elem, use_s3ns=False)
        body = body.replace(
            b'?>\n',
            b'?>\n<!DOCTYPE foo '
            b'[<!ENTITY ent SYSTEM "file:///etc/passwd"> ]>\n',
        ).replace(b'>Key1<', b'>Key1&ent;<')
        content_md5 = (
            base64.b64encode(md5(body, usedforsecurity=False).digest())
            .strip())

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={
                                'Authorization': 'AWS test:full_control:hmac',
                                'Date': self.get_date_header(),
                                'Content-MD5': content_md5},
                            body=body)
        req.date = datetime.now()
        req.content_type = 'text/plain'

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK', body)
        self.assertIn(b'<Deleted><Key>Key2</Key></Deleted>', body)
        self.assertNotIn(b'root:/root', body)
        self.assertIn(b'<Deleted><Key>Key1</Key></Deleted>', body)


class TestS3ApiMultiDeleteAcl(BaseS3ApiMultiDelete, S3ApiTestCaseAcl):

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
        content_md5 = (
            base64.b64encode(md5(body, usedforsecurity=False).digest())
            .strip())

        req = Request.blank('/bucket?delete',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header(),
                                     'Content-MD5': content_md5},
                            body=body)
        req.date = datetime.now()
        req.content_type = 'text/plain'

        return self.call_s3api(req)

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

    def test_object_multi_DELETE_with_write_permission(self):
        status, headers, body = self._test_object_multi_DELETE('test:write')
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), len(self.keys))

    def test_object_multi_DELETE_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_object_multi_DELETE('test:full_control')
        self.assertEqual(status.split()[0], '200')
        elem = fromstring(body)
        self.assertEqual(len(elem.findall('Deleted')), len(self.keys))


if __name__ == '__main__':
    unittest.main()
