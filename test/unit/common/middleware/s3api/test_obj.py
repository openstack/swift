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
from datetime import datetime
import hashlib
import os
from os.path import join
import time
from mock import patch

from swift.common import swob
from swift.common.swob import Request

from test.unit.common.middleware.s3api import S3ApiTestCase
from test.unit.common.middleware.s3api.test_s3_acl import s3acl
from swift.common.middleware.s3api.subresource import ACL, User, encode_acl, \
    Owner, Grant
from swift.common.middleware.s3api.etree import fromstring
from swift.common.middleware.s3api.utils import mktime, S3Timestamp


class TestS3ApiObj(S3ApiTestCase):

    def setUp(self):
        super(TestS3ApiObj, self).setUp()

        self.object_body = 'hello'
        self.etag = hashlib.md5(self.object_body).hexdigest()
        self.last_modified = 'Fri, 01 Apr 2014 12:00:00 GMT'

        self.response_headers = {'Content-Type': 'text/html',
                                 'Content-Length': len(self.object_body),
                                 'Content-Disposition': 'inline',
                                 'Content-Language': 'en',
                                 'x-object-meta-test': 'swift',
                                 'etag': self.etag,
                                 'last-modified': self.last_modified,
                                 'expires': 'Mon, 21 Sep 2015 12:00:00 GMT',
                                 'x-robots-tag': 'nofollow',
                                 'cache-control': 'private'}

        self.swift.register('GET', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers,
                            self.object_body)
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPCreated,
                            {'etag': self.etag,
                             'last-modified': self.last_modified,
                             'x-object-meta-something': 'oh hai'},
                            None)

    def _test_object_GETorHEAD(self, method):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        unexpected_headers = []
        for key, val in self.response_headers.items():
            if key in ('Content-Length', 'Content-Type', 'content-encoding',
                       'last-modified', 'cache-control', 'Content-Disposition',
                       'Content-Language', 'expires', 'x-robots-tag'):
                self.assertIn(key, headers)
                self.assertEqual(headers[key], str(val))

            elif key == 'etag':
                self.assertEqual(headers[key], '"%s"' % val)

            elif key.startswith('x-object-meta-'):
                self.assertIn('x-amz-meta-' + key[14:], headers)
                self.assertEqual(headers['x-amz-meta-' + key[14:]], val)

            else:
                unexpected_headers.append((key, val))

        if unexpected_headers:
                self.fail('unexpected headers: %r' % unexpected_headers)

        self.assertEqual(headers['etag'],
                         '"%s"' % self.response_headers['etag'])

        if method == 'GET':
            self.assertEqual(body, self.object_body)

    @s3acl
    def test_object_HEAD_error(self):
        # HEAD does not return the body even an error response in the
        # specifications of the REST API.
        # So, check the response code for error test of HEAD.
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPUnauthorized, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, '')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPForbidden, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, '')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNotFound, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(body, '')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPPreconditionFailed, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '412')
        self.assertEqual(body, '')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPServerError, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '500')
        self.assertEqual(body, '')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPServiceUnavailable, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '500')
        self.assertEqual(body, '')  # sanity

    def test_object_HEAD(self):
        self._test_object_GETorHEAD('HEAD')

    def _test_object_HEAD_Range(self, range_value):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Range': range_value,
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

    @s3acl
    def test_object_HEAD_Range_with_invalid_value(self):
        range_value = ''
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '200')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('content-range' not in headers)

        range_value = 'hoge'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '200')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('content-range' not in headers)

        range_value = 'bytes='
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '200')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('content-range' not in headers)

        range_value = 'bytes=1'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '200')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('content-range' not in headers)

        range_value = 'bytes=5-1'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '200')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('content-range' not in headers)

        range_value = 'bytes=5-10'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '416')

    @s3acl
    def test_object_HEAD_Range(self):
        # update response headers
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers,
                            self.object_body)
        range_value = 'bytes=0-3'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '206')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '4')
        self.assertTrue('content-range' in headers)
        self.assertTrue(headers['content-range'].startswith('bytes 0-3'))
        self.assertTrue('x-amz-meta-test' in headers)
        self.assertEqual('swift', headers['x-amz-meta-test'])

        range_value = 'bytes=3-3'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '206')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '1')
        self.assertTrue('content-range' in headers)
        self.assertTrue(headers['content-range'].startswith('bytes 3-3'))
        self.assertTrue('x-amz-meta-test' in headers)
        self.assertEqual('swift', headers['x-amz-meta-test'])

        range_value = 'bytes=1-'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '206')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '4')
        self.assertTrue('content-range' in headers)
        self.assertTrue(headers['content-range'].startswith('bytes 1-4'))
        self.assertTrue('x-amz-meta-test' in headers)
        self.assertEqual('swift', headers['x-amz-meta-test'])

        range_value = 'bytes=-3'
        status, headers, body = self._test_object_HEAD_Range(range_value)
        self.assertEqual(status.split()[0], '206')
        self.assertTrue('content-length' in headers)
        self.assertEqual(headers['content-length'], '3')
        self.assertTrue('content-range' in headers)
        self.assertTrue(headers['content-range'].startswith('bytes 2-4'))
        self.assertTrue('x-amz-meta-test' in headers)
        self.assertEqual('swift', headers['x-amz-meta-test'])

    @s3acl
    def test_object_GET_error(self):
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPUnauthorized)
        self.assertEqual(code, 'SignatureDoesNotMatch')
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPForbidden)
        self.assertEqual(code, 'AccessDenied')
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPNotFound)
        self.assertEqual(code, 'NoSuchKey')
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPServerError)
        self.assertEqual(code, 'InternalError')
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPPreconditionFailed)
        self.assertEqual(code, 'PreconditionFailed')
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPServiceUnavailable)
        self.assertEqual(code, 'InternalError')

    @s3acl
    def test_object_GET(self):
        self._test_object_GETorHEAD('GET')

    @s3acl(s3acl_only=True)
    def test_object_GET_with_s3acl_and_unknown_user(self):
        self.swift.remote_user = None
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '403 Forbidden')
        self.assertEqual(self._get_error_code(body), 'SignatureDoesNotMatch')

    @s3acl(s3acl_only=True)
    def test_object_GET_with_s3acl_and_keystone(self):
        # for passing keystone authentication root
        orig_auth = self.swift._fake_auth_middleware
        calls = []

        def wrapped_auth(env):
            calls.append((env['REQUEST_METHOD'], 's3api.auth_details' in env))
            orig_auth(env)

        with patch.object(self.swift, '_fake_auth_middleware', wrapped_auth):
            self._test_object_GETorHEAD('GET')
        self.assertEqual(calls, [
            ('TEST', True),
            ('HEAD', False),
            ('GET', False),
        ])

    @s3acl
    def test_object_GET_Range(self):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Range': 'bytes=0-3',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '206')

        self.assertTrue('content-range' in headers)
        self.assertTrue(headers['content-range'].startswith('bytes 0-3'))

    @s3acl
    def test_object_GET_Range_error(self):
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPRequestedRangeNotSatisfiable)
        self.assertEqual(code, 'InvalidRange')

    @s3acl
    def test_object_GET_Response(self):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'GET',
                                     'QUERY_STRING':
                                     'response-content-type=%s&'
                                     'response-content-language=%s&'
                                     'response-expires=%s&'
                                     'response-cache-control=%s&'
                                     'response-content-disposition=%s&'
                                     'response-content-encoding=%s&'
                                     % ('text/plain', 'en',
                                        'Fri, 01 Apr 2014 12:00:00 GMT',
                                        'no-cache',
                                        'attachment',
                                        'gzip')},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

        self.assertTrue('content-type' in headers)
        self.assertEqual(headers['content-type'], 'text/plain')
        self.assertTrue('content-language' in headers)
        self.assertEqual(headers['content-language'], 'en')
        self.assertTrue('expires' in headers)
        self.assertEqual(headers['expires'], 'Fri, 01 Apr 2014 12:00:00 GMT')
        self.assertTrue('cache-control' in headers)
        self.assertEqual(headers['cache-control'], 'no-cache')
        self.assertTrue('content-disposition' in headers)
        self.assertEqual(headers['content-disposition'],
                         'attachment')
        self.assertTrue('content-encoding' in headers)
        self.assertEqual(headers['content-encoding'], 'gzip')

    @s3acl
    def test_object_PUT_error(self):
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPUnauthorized)
        self.assertEqual(code, 'SignatureDoesNotMatch')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPForbidden)
        self.assertEqual(code, 'AccessDenied')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPNotFound)
        self.assertEqual(code, 'NoSuchBucket')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPRequestEntityTooLarge)
        self.assertEqual(code, 'EntityTooLarge')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPServerError)
        self.assertEqual(code, 'InternalError')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPUnprocessableEntity)
        self.assertEqual(code, 'BadDigest')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPLengthRequired)
        self.assertEqual(code, 'MissingContentLength')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPPreconditionFailed)
        self.assertEqual(code, 'InternalError')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPServiceUnavailable)
        self.assertEqual(code, 'InternalError')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPCreated,
                                       {'X-Amz-Copy-Source': ''})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPCreated,
                                       {'X-Amz-Copy-Source': '/'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPCreated,
                                       {'X-Amz-Copy-Source': '/bucket'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPCreated,
                                       {'X-Amz-Copy-Source': '/bucket/'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error(
            'PUT', '/bucket/object',
            swob.HTTPCreated,
            {'X-Amz-Copy-Source': '/bucket/src_obj?foo=bar'})
        self.assertEqual(code, 'InvalidArgument')
        # adding other query paramerters will cause an error
        code = self._test_method_error(
            'PUT', '/bucket/object',
            swob.HTTPCreated,
            {'X-Amz-Copy-Source': '/bucket/src_obj?versionId=foo&bar=baz'})
        self.assertEqual(code, 'InvalidArgument')
        # ...even versionId appears in the last
        code = self._test_method_error(
            'PUT', '/bucket/object',
            swob.HTTPCreated,
            {'X-Amz-Copy-Source': '/bucket/src_obj?bar=baz&versionId=foo'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error(
            'PUT', '/bucket/object',
            swob.HTTPCreated,
            {'X-Amz-Copy-Source': '/bucket/src_obj?versionId=foo'})
        self.assertEqual(code, 'NotImplemented')
        code = self._test_method_error(
            'PUT', '/bucket/object',
            swob.HTTPCreated,
            {'X-Amz-Copy-Source': '/src_bucket/src_object',
             'X-Amz-Copy-Source-Range': 'bytes=0-0'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPRequestTimeout)
        self.assertEqual(code, 'RequestTimeout')

    @s3acl
    def test_object_PUT(self):
        etag = self.response_headers['etag']
        content_md5 = etag.decode('hex').encode('base64').strip()

        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'x-amz-storage-class': 'STANDARD',
                     'Content-MD5': content_md5,
                     'Date': self.get_date_header()},
            body=self.object_body)
        req.date = datetime.now()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        # Check that s3api returns an etag header.
        self.assertEqual(headers['etag'], '"%s"' % etag)

        _, _, headers = self.swift.calls_with_headers[-1]
        # Check that s3api converts a Content-MD5 header into an etag.
        self.assertEqual(headers['etag'], etag)

    @s3acl
    def test_object_PUT_v4(self):
        body_sha = hashlib.sha256(self.object_body).hexdigest()
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={
                'Authorization':
                    'AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-east-1/s3/aws4_request, '
                    'SignedHeaders=host;x-amz-date, '
                    'Signature=hmac' % (
                        self.get_v4_amz_date_header().split('T', 1)[0]),
                'x-amz-date': self.get_v4_amz_date_header(),
                'x-amz-storage-class': 'STANDARD',
                'x-amz-content-sha256': body_sha,
                'Date': self.get_date_header()},
            body=self.object_body)
        req.date = datetime.now()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        # Check that s3api returns an etag header.
        self.assertEqual(headers['etag'],
                         '"%s"' % self.response_headers['etag'])

        _, _, headers = self.swift.calls_with_headers[-1]
        # No way to determine ETag to send
        self.assertNotIn('etag', headers)

    @s3acl
    def test_object_PUT_v4_bad_hash(self):
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={
                'Authorization':
                    'AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-east-1/s3/aws4_request, '
                    'SignedHeaders=host;x-amz-date, '
                    'Signature=hmac' % (
                        self.get_v4_amz_date_header().split('T', 1)[0]),
                'x-amz-date': self.get_v4_amz_date_header(),
                'x-amz-storage-class': 'STANDARD',
                'x-amz-content-sha256': 'not the hash',
                'Date': self.get_date_header()},
            body=self.object_body)
        req.date = datetime.now()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        print(body)
        self.assertEqual(self._get_error_code(body), 'BadDigest')

    def test_object_PUT_headers(self):
        content_md5 = self.etag.decode('hex').encode('base64').strip()

        self.swift.register('HEAD', '/v1/AUTH_test/some/source',
                            swob.HTTPOk, {'last-modified': self.last_modified},
                            None)
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'X-Amz-Storage-Class': 'STANDARD',
                     'X-Amz-Meta-Something': 'oh hai',
                     'X-Amz-Meta-Unreadable-Prefix': '\x04w',
                     'X-Amz-Meta-Unreadable-Suffix': 'h\x04',
                     'X-Amz-Meta-Lots-Of-Unprintable': 5 * '\x04',
                     'X-Amz-Copy-Source': '/some/source',
                     'Content-MD5': content_md5,
                     'Date': self.get_date_header()})
        req.date = datetime.now()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        # Check that s3api does not return an etag header,
        # specified copy source.
        self.assertTrue(headers.get('etag') is None)
        # Check that s3api does not return custom metadata in response
        self.assertTrue(headers.get('x-amz-meta-something') is None)

        _, _, headers = self.swift.calls_with_headers[-1]
        # Check that s3api converts a Content-MD5 header into an etag.
        self.assertEqual(headers['ETag'], self.etag)
        # Check that metadata is omited if no directive is specified
        self.assertIsNone(headers.get('X-Object-Meta-Something'))
        self.assertIsNone(headers.get('X-Object-Meta-Unreadable-Prefix'))
        self.assertIsNone(headers.get('X-Object-Meta-Unreadable-Suffix'))
        self.assertIsNone(headers.get('X-Object-Meta-Lots-Of-Unprintable'))

        self.assertEqual(headers['X-Copy-From'], '/some/source')
        self.assertEqual(headers['Content-Length'], '0')

    def _test_object_PUT_copy(self, head_resp, put_header=None,
                              src_path='/some/source', timestamp=None):
        account = 'test:tester'
        grants = [Grant(User(account), 'FULL_CONTROL')]
        head_headers = \
            encode_acl('object',
                       ACL(Owner(account, account), grants))
        head_headers.update({'last-modified': self.last_modified})
        self.swift.register('HEAD', '/v1/AUTH_test/some/source',
                            head_resp, head_headers, None)
        put_header = put_header or {}
        return self._call_object_copy(src_path, put_header, timestamp)

    def _test_object_PUT_copy_self(self, head_resp,
                                   put_header=None, timestamp=None):
        account = 'test:tester'
        grants = [Grant(User(account), 'FULL_CONTROL')]
        head_headers = \
            encode_acl('object',
                       ACL(Owner(account, account), grants))
        head_headers.update({'last-modified': self.last_modified})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            head_resp, head_headers, None)
        put_header = put_header or {}
        return self._call_object_copy('/bucket/object', put_header, timestamp)

    def _call_object_copy(self, src_path, put_header, timestamp=None):
        put_headers = {'Authorization': 'AWS test:tester:hmac',
                       'X-Amz-Copy-Source': src_path,
                       'Date': self.get_date_header()}
        put_headers.update(put_header)

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers=put_headers)

        req.date = datetime.now()
        req.content_type = 'text/plain'
        timestamp = timestamp or time.time()
        with patch('swift.common.middleware.s3api.utils.time.time',
                   return_value=timestamp):
            return self.call_s3api(req)

    @s3acl
    def test_object_PUT_copy(self):
        def do_test(src_path=None):
            date_header = self.get_date_header()
            timestamp = mktime(date_header)
            allowed_last_modified = [S3Timestamp(timestamp).s3xmlformat]
            status, headers, body = self._test_object_PUT_copy(
                swob.HTTPOk, put_header={'Date': date_header},
                timestamp=timestamp, src_path=src_path)
            # may have gotten unlucky and had the clock roll over
            date_header = self.get_date_header()
            timestamp = mktime(date_header)
            allowed_last_modified.append(S3Timestamp(timestamp).s3xmlformat)

            self.assertEqual(status.split()[0], '200')
            self.assertEqual(headers['Content-Type'], 'application/xml')

            self.assertTrue(headers.get('etag') is None)
            self.assertTrue(headers.get('x-amz-meta-something') is None)
            elem = fromstring(body, 'CopyObjectResult')
            self.assertIn(elem.find('LastModified').text,
                          allowed_last_modified)
            self.assertEqual(elem.find('ETag').text, '"%s"' % self.etag)

            _, _, headers = self.swift.calls_with_headers[-1]
            self.assertEqual(headers['X-Copy-From'], '/some/source')
            self.assertTrue(headers.get('X-Fresh-Metadata') is None)
            self.assertEqual(headers['Content-Length'], '0')

        do_test('/some/source')
        do_test('/some/source?')
        do_test('/some/source?versionId=null')
        # Some clients (like Boto) don't include the leading slash;
        # AWS seems to tolerate this so we should, too
        do_test('some/source')

    @s3acl
    def test_object_PUT_copy_metadata_replace(self):
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        allowed_last_modified = [S3Timestamp(timestamp).s3xmlformat]
        status, headers, body = \
            self._test_object_PUT_copy(
                swob.HTTPOk,
                {'X-Amz-Metadata-Directive': 'REPLACE',
                 'X-Amz-Meta-Something': 'oh hai',
                 'X-Amz-Meta-Unreadable-Prefix': '\x04w',
                 'X-Amz-Meta-Unreadable-Suffix': 'h\x04',
                 'X-Amz-Meta-Lots-Of-Unprintable': 5 * '\x04',
                 'Cache-Control': 'hello',
                 'content-disposition': 'how are you',
                 'content-encoding': 'good and you',
                 'content-language': 'great',
                 'content-type': 'so',
                 'expires': 'yeah',
                 'x-robots-tag': 'bye'})
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        allowed_last_modified.append(S3Timestamp(timestamp).s3xmlformat)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Content-Type'], 'application/xml')
        self.assertIsNone(headers.get('etag'))
        elem = fromstring(body, 'CopyObjectResult')
        self.assertIn(elem.find('LastModified').text, allowed_last_modified)
        self.assertEqual(elem.find('ETag').text, '"%s"' % self.etag)

        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(headers['X-Copy-From'], '/some/source')
        # Check that metadata is included if replace directive is specified
        # and that Fresh Metadata is set
        self.assertTrue(headers.get('X-Fresh-Metadata') == 'True')
        self.assertEqual(headers['X-Object-Meta-Something'], 'oh hai')
        self.assertEqual(headers['X-Object-Meta-Unreadable-Prefix'],
                         '=?UTF-8?Q?=04w?=')
        self.assertEqual(headers['X-Object-Meta-Unreadable-Suffix'],
                         '=?UTF-8?Q?h=04?=')
        self.assertEqual(headers['X-Object-Meta-Lots-Of-Unprintable'],
                         '=?UTF-8?B?BAQEBAQ=?=')
        # Check other metadata is set
        self.assertEqual(headers['Cache-Control'], 'hello')
        self.assertEqual(headers['Content-Disposition'], 'how are you')
        self.assertEqual(headers['Content-Encoding'], 'good and you')
        self.assertEqual(headers['Content-Language'], 'great')
        # Content-Type can't be set during an S3 copy operation
        self.assertIsNone(headers.get('Content-Type'))
        self.assertEqual(headers['Expires'], 'yeah')
        self.assertEqual(headers['X-Robots-Tag'], 'bye')

        self.assertEqual(headers['Content-Length'], '0')

    @s3acl
    def test_object_PUT_copy_metadata_copy(self):
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        allowed_last_modified = [S3Timestamp(timestamp).s3xmlformat]
        status, headers, body = \
            self._test_object_PUT_copy(
                swob.HTTPOk,
                {'X-Amz-Metadata-Directive': 'COPY',
                 'X-Amz-Meta-Something': 'oh hai',
                 'X-Amz-Meta-Unreadable-Prefix': '\x04w',
                 'X-Amz-Meta-Unreadable-Suffix': 'h\x04',
                 'X-Amz-Meta-Lots-Of-Unprintable': 5 * '\x04',
                 'Cache-Control': 'hello',
                 'content-disposition': 'how are you',
                 'content-encoding': 'good and you',
                 'content-language': 'great',
                 'content-type': 'so',
                 'expires': 'yeah',
                 'x-robots-tag': 'bye'})
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        allowed_last_modified.append(S3Timestamp(timestamp).s3xmlformat)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Content-Type'], 'application/xml')
        self.assertIsNone(headers.get('etag'))

        elem = fromstring(body, 'CopyObjectResult')
        self.assertIn(elem.find('LastModified').text, allowed_last_modified)
        self.assertEqual(elem.find('ETag').text, '"%s"' % self.etag)

        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(headers['X-Copy-From'], '/some/source')
        # Check that metadata is omited if COPY directive is specified
        self.assertIsNone(headers.get('X-Fresh-Metadata'))
        self.assertIsNone(headers.get('X-Object-Meta-Something'))
        self.assertIsNone(headers.get('X-Object-Meta-Unreadable-Prefix'))
        self.assertIsNone(headers.get('X-Object-Meta-Unreadable-Suffix'))
        self.assertIsNone(headers.get('X-Object-Meta-Lots-Of-Unprintable'))
        self.assertIsNone(headers.get('Cache-Control'))
        self.assertIsNone(headers.get('Content-Disposition'))
        self.assertIsNone(headers.get('Content-Encoding'))
        self.assertIsNone(headers.get('Content-Language'))
        self.assertIsNone(headers.get('Content-Type'))
        self.assertIsNone(headers.get('Expires'))
        self.assertIsNone(headers.get('X-Robots-Tag'))

        self.assertEqual(headers['Content-Length'], '0')

    @s3acl
    def test_object_PUT_copy_self(self):
        status, headers, body = \
            self._test_object_PUT_copy_self(swob.HTTPOk)
        self.assertEqual(status.split()[0], '400')
        elem = fromstring(body, 'Error')
        err_msg = ("This copy request is illegal because it is trying to copy "
                   "an object to itself without changing the object's "
                   "metadata, storage class, website redirect location or "
                   "encryption attributes.")
        self.assertEqual(elem.find('Code').text, 'InvalidRequest')
        self.assertEqual(elem.find('Message').text, err_msg)

    @s3acl
    def test_object_PUT_copy_self_metadata_copy(self):
        header = {'x-amz-metadata-directive': 'COPY'}
        status, headers, body = \
            self._test_object_PUT_copy_self(swob.HTTPOk, header)
        self.assertEqual(status.split()[0], '400')
        elem = fromstring(body, 'Error')
        err_msg = ("This copy request is illegal because it is trying to copy "
                   "an object to itself without changing the object's "
                   "metadata, storage class, website redirect location or "
                   "encryption attributes.")
        self.assertEqual(elem.find('Code').text, 'InvalidRequest')
        self.assertEqual(elem.find('Message').text, err_msg)

    @s3acl
    def test_object_PUT_copy_self_metadata_replace(self):
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        allowed_last_modified = [S3Timestamp(timestamp).s3xmlformat]
        header = {'x-amz-metadata-directive': 'REPLACE',
                  'Date': date_header}
        status, headers, body = self._test_object_PUT_copy_self(
            swob.HTTPOk, header, timestamp=timestamp)
        date_header = self.get_date_header()
        timestamp = mktime(date_header)
        allowed_last_modified.append(S3Timestamp(timestamp).s3xmlformat)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Content-Type'], 'application/xml')
        self.assertTrue(headers.get('etag') is None)
        elem = fromstring(body, 'CopyObjectResult')
        self.assertIn(elem.find('LastModified').text, allowed_last_modified)
        self.assertEqual(elem.find('ETag').text, '"%s"' % self.etag)

        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(headers['X-Copy-From'], '/bucket/object')
        self.assertEqual(headers['Content-Length'], '0')

    @s3acl
    def test_object_PUT_copy_headers_error(self):
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag,
                  'Date': self.get_date_header()}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPPreconditionFailed,
                                       header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = {'X-Amz-Copy-Source-If-None-Match': etag}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPNotModified,
                                       header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = {'X-Amz-Copy-Source-If-Modified-Since': last_modified_since}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPNotModified,
                                       header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

        header = \
            {'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPPreconditionFailed,
                                       header)
        self.assertEqual(self._get_error_code(body), 'PreconditionFailed')

    def test_object_PUT_copy_headers_with_match(self):
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 11:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag,
                  'X-Amz-Copy-Source-If-Modified-Since': last_modified_since,
                  'Date': self.get_date_header()}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPOk, header)
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 2)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertEqual(headers['If-Match'], etag)
        self.assertEqual(headers['If-Modified-Since'], last_modified_since)

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_headers_with_match_and_s3acl(self):
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 11:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-Match': etag,
                  'X-Amz-Copy-Source-If-Modified-Since': last_modified_since,
                  'Date': self.get_date_header()}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPOk, header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 3)
        # After the check of the copy source in the case of s3acl is valid,
        # s3api check the bucket write permissions of the destination.
        _, _, headers = self.swift.calls_with_headers[-2]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-Match') is None)
        self.assertTrue(headers.get('If-Modified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertEqual(headers['If-Match'], etag)
        self.assertEqual(headers['If-Modified-Since'], last_modified_since)

    def test_object_PUT_copy_headers_with_not_match(self):
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-None-Match': etag,
                  'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since,
                  'Date': self.get_date_header()}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPOk, header)

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(len(self.swift.calls_with_headers), 2)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertEqual(headers['If-None-Match'], etag)
        self.assertEqual(headers['If-Unmodified-Since'], last_modified_since)

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_headers_with_not_match_and_s3acl(self):
        etag = '7dfa07a8e59ddbcd1dc84d4c4f82aea1'
        last_modified_since = 'Fri, 01 Apr 2014 12:00:00 GMT'

        header = {'X-Amz-Copy-Source-If-None-Match': etag,
                  'X-Amz-Copy-Source-If-Unmodified-Since': last_modified_since,
                  'Date': self.get_date_header()}
        status, header, body = \
            self._test_object_PUT_copy(swob.HTTPOk, header)
        self.assertEqual(status.split()[0], '200')
        # After the check of the copy source in the case of s3acl is valid,
        # s3api check the bucket write permissions of the destination.
        self.assertEqual(len(self.swift.calls_with_headers), 3)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertTrue(headers.get('If-None-Match') is None)
        self.assertTrue(headers.get('If-Unmodified-Since') is None)
        _, _, headers = self.swift.calls_with_headers[0]
        self.assertEqual(headers['If-None-Match'], etag)
        self.assertEqual(headers['If-Unmodified-Since'], last_modified_since)

    @s3acl
    def test_object_POST_error(self):
        code = self._test_method_error('POST', '/bucket/object', None)
        self.assertEqual(code, 'NotImplemented')

    @s3acl
    def test_object_DELETE_error(self):
        code = self._test_method_error('DELETE', '/bucket/object',
                                       swob.HTTPUnauthorized)
        self.assertEqual(code, 'SignatureDoesNotMatch')
        code = self._test_method_error('DELETE', '/bucket/object',
                                       swob.HTTPForbidden)
        self.assertEqual(code, 'AccessDenied')
        code = self._test_method_error('DELETE', '/bucket/object',
                                       swob.HTTPServerError)
        self.assertEqual(code, 'InternalError')
        code = self._test_method_error('DELETE', '/bucket/object',
                                       swob.HTTPServiceUnavailable)
        self.assertEqual(code, 'InternalError')

        with patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                return_value={'status': 404}):
            code = self._test_method_error('DELETE', '/bucket/object',
                                           swob.HTTPNotFound)
            self.assertEqual(code, 'NoSuchBucket')

    @s3acl
    def test_object_DELETE_no_multipart(self):
        self.s3api.conf.allow_multipart_uploads = False
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

        self.assertNotIn(('HEAD', '/v1/AUTH_test/bucket/object'),
                         self.swift.calls)
        self.assertIn(('DELETE', '/v1/AUTH_test/bucket/object'),
                      self.swift.calls)
        _, path = self.swift.calls[-1]
        self.assertEqual(path.count('?'), 0)

    @s3acl
    def test_object_DELETE_multipart(self):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

        self.assertIn(('HEAD', '/v1/AUTH_test/bucket/object'),
                      self.swift.calls)
        self.assertEqual(('DELETE', '/v1/AUTH_test/bucket/object'),
                         self.swift.calls[-1])
        _, path = self.swift.calls[-1]
        self.assertEqual(path.count('?'), 0)

    @s3acl
    def test_object_DELETE_missing(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNotFound, {}, None)
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

        self.assertIn(('HEAD', '/v1/AUTH_test/bucket/object'),
                      self.swift.calls)
        self.assertNotIn(('DELETE', '/v1/AUTH_test/bucket/object'),
                         self.swift.calls)

    @s3acl
    def test_slo_object_DELETE(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk,
                            {'x-static-large-object': 'True'},
                            None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, {}, '<SLO delete results>')
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-Type': 'foo/bar'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual(body, '')

        self.assertIn(('HEAD', '/v1/AUTH_test/bucket/object'),
                      self.swift.calls)
        self.assertIn(('DELETE', '/v1/AUTH_test/bucket/object'
                                 '?multipart-manifest=delete'),
                      self.swift.calls)
        _, path, headers = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['multipart-manifest'], 'delete')
        self.assertNotIn('Content-Type', headers)

    def _test_object_for_s3acl(self, method, account):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

    def _test_set_container_permission(self, account, permission):
        grants = [Grant(User(account), permission)]
        headers = \
            encode_acl('container',
                       ACL(Owner('test:tester', 'test:tester'), grants))
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, headers, None)

    @s3acl(s3acl_only=True)
    def test_object_GET_without_permission(self):
        status, headers, body = self._test_object_for_s3acl('GET',
                                                            'test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    @s3acl(s3acl_only=True)
    def test_object_GET_with_read_permission(self):
        status, headers, body = self._test_object_for_s3acl('GET',
                                                            'test:read')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_GET_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_object_for_s3acl('GET', 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_PUT_without_permission(self):
        status, headers, body = self._test_object_for_s3acl('PUT',
                                                            'test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    @s3acl(s3acl_only=True)
    def test_object_PUT_with_owner_permission(self):
        status, headers, body = self._test_object_for_s3acl('PUT',
                                                            'test:tester')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_PUT_with_write_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'WRITE')
        status, headers, body = self._test_object_for_s3acl('PUT', account)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_PUT_with_fullcontrol_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'FULL_CONTROL')
        status, headers, body = \
            self._test_object_for_s3acl('PUT', account)
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_DELETE_without_permission(self):
        account = 'test:other'
        status, headers, body = self._test_object_for_s3acl('DELETE',
                                                            account)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    @s3acl(s3acl_only=True)
    def test_object_DELETE_with_owner_permission(self):
        status, headers, body = self._test_object_for_s3acl('DELETE',
                                                            'test:tester')
        self.assertEqual(status.split()[0], '204')

    @s3acl(s3acl_only=True)
    def test_object_DELETE_with_write_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'WRITE')
        status, headers, body = self._test_object_for_s3acl('DELETE',
                                                            account)
        self.assertEqual(status.split()[0], '204')

    @s3acl(s3acl_only=True)
    def test_object_DELETE_with_fullcontrol_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'FULL_CONTROL')
        status, headers, body = self._test_object_for_s3acl('DELETE', account)
        self.assertEqual(status.split()[0], '204')

    def _test_object_copy_for_s3acl(self, account, src_permission=None,
                                    src_path='/src_bucket/src_obj'):
        owner = 'test:tester'
        grants = [Grant(User(account), src_permission)] \
            if src_permission else [Grant(User(owner), 'FULL_CONTROL')]
        src_o_headers = \
            encode_acl('object', ACL(Owner(owner, owner), grants))
        src_o_headers.update({'last-modified': self.last_modified})
        self.swift.register(
            'HEAD', join('/v1/AUTH_test', src_path.lstrip('/')),
            swob.HTTPOk, src_o_headers, None)

        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS %s:hmac' % account,
                     'X-Amz-Copy-Source': src_path,
                     'Date': self.get_date_header()})

        return self.call_s3api(req)

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_with_owner_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:tester')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:full_control',
                                             'FULL_CONTROL')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_with_grantee_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:write', 'READ')
        self.assertEqual(status.split()[0], '200')

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_without_src_obj_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:write')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_without_dst_container_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:other', 'READ')
        self.assertEqual(status.split()[0], '403')

    @s3acl(s3acl_only=True)
    def test_object_PUT_copy_empty_src_path(self):
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPPreconditionFailed, {}, None)
        status, headers, body = self._test_object_copy_for_s3acl(
            'test:write', 'READ', src_path='')
        self.assertEqual(status.split()[0], '400')


class TestS3ApiObjNonUTC(TestS3ApiObj):
    def setUp(self):
        self.orig_tz = os.environ.get('TZ', '')
        os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
        time.tzset()
        super(TestS3ApiObjNonUTC, self).setUp()

    def tearDown(self):
        super(TestS3ApiObjNonUTC, self).tearDown()
        os.environ['TZ'] = self.orig_tz
        time.tzset()

if __name__ == '__main__':
    unittest.main()
