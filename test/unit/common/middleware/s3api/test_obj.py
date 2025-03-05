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

import binascii
import unittest
from datetime import datetime
import functools
from hashlib import sha256
import os
from os.path import join
import time
from unittest.mock import patch
import json

from swift.common import swob
from swift.common.storage_policy import StoragePolicy
from swift.common.swob import Request
from swift.common.middleware.proxy_logging import ProxyLoggingMiddleware
from test.unit import mock_timestamp_now, patch_policies

from test.unit.common.middleware.s3api import S3ApiTestCase, S3ApiTestCaseAcl
from swift.common.middleware.s3api.s3request import SigV4Request
from swift.common.middleware.s3api.subresource import ACL, User, encode_acl, \
    Owner, Grant
from swift.common.middleware.s3api.etree import fromstring
from swift.common.middleware.s3api.utils import mktime, S3Timestamp
from swift.common.middleware.versioned_writes.object_versioning import \
    DELETE_MARKER_CONTENT_TYPE
from swift.common.utils import md5


class BaseS3ApiObj(object):

    def setUp(self):
        super(BaseS3ApiObj, self).setUp()

        self.object_body = b'hello'
        self.etag = md5(self.object_body, usedforsecurity=False).hexdigest()
        self.last_modified = 'Fri, 01 Apr 2014 12:00:00 GMT'

        self.response_headers = {'Content-Type': 'text/html',
                                 'Content-Length': len(self.object_body),
                                 'Accept-Ranges': 'bytes',
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
        self.swift.register('GET', '/v1/AUTH_test/bucket/object?symlink=get',
                            swob.HTTPOk, self.response_headers,
                            self.object_body)
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPCreated,
                            {'etag': self.etag,
                             'last-modified': self.last_modified,
                             'x-object-meta-something': 'oh hai'},
                            None)

        self.bucket_policy_index = 1
        self._register_bucket_policy_index_head(
            'bucket', self.bucket_policy_index)

    def _test_object_GETorHEAD(self, method):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        # we'll want this for logging
        self._assert_policy_index(req.headers, headers,
                                  self.bucket_policy_index)

        unexpected_headers = []
        for key, val in self.response_headers.items():
            if key in ('Content-Length', 'Content-Type', 'content-encoding',
                       'last-modified', 'cache-control', 'Content-Disposition',
                       'Content-Language', 'expires', 'x-robots-tag',
                       'Accept-Ranges'):
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

    def test_object_GET(self):
        self._test_object_GETorHEAD('GET')

    def test_object_HEAD(self):
        self._test_object_GETorHEAD('HEAD')

    def test_object_HEAD_error(self):
        # HEAD does not return the body even an error response in the
        # specifications of the REST API.
        # So, check the response code for error test of HEAD.
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.s3acl_response_modified = True
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPUnauthorized, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, b'')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPForbidden, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, b'')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNotFound, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(body, b'')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPPreconditionFailed, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '412')
        self.assertEqual(body, b'')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPServerError, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '500')
        self.assertEqual(body, b'')  # sanity

        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPServiceUnavailable, {}, None)
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '503')
        self.assertEqual(body, b'')  # sanity

    def _do_test_object_policy_index_logging(self, bucket_policy_index):
        self.logger.clear()
        req = Request.blank('/bucket/object',
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.s3api = ProxyLoggingMiddleware(self.s3api, {}, logger=self.logger)
        status, headers, body = self.call_s3api(req)
        self._assert_policy_index(req.headers, headers, bucket_policy_index)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ['swift.backend_path'])
        access_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(1, len(access_lines))
        parts = access_lines[0].split()
        self.assertEqual(' '.join(parts[3:7]),
                         'GET /bucket/object HTTP/1.0 200')
        self.assertEqual(parts[-1], str(bucket_policy_index))

    def _test_object_HEAD_Range(self, range_value):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Range': range_value,
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

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

    def test_object_GET_error(self):
        self.s3acl_response_modified = True
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
        self.assertEqual(code, 'ServiceUnavailable')
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPConflict)
        self.assertEqual(code, 'BrokenMPU')

        code = self._test_method_error(
            'GET', '/bucket/object',
            functools.partial(swob.Response, status='498 Rate Limited'),
            expected_status='503 Slow Down')
        self.assertEqual(code, 'SlowDown')

        with patch.object(self.s3api.conf, 'ratelimit_as_client_error', True):
            code = self._test_method_error(
                'GET', '/bucket/object',
                functools.partial(swob.Response, status='498 Rate Limited'),
                expected_status='429 Slow Down')
            self.assertEqual(code, 'SlowDown')

    def _test_non_slo_object_GETorHEAD_part_num(self, method, part_number):
        req = Request.blank('/bucket/object?partNumber=%s' % part_number,
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '206')
        self.assertEqual(headers['content-length'], '5')
        self.assertTrue('content-range' in headers)
        self.assertEqual(headers['content-range'], 'bytes 0-4/5')
        self.assertEqual(headers['content-type'], 'text/html')
        # we'll want this for logging
        self._assert_policy_index(req.headers, headers,
                                  self.bucket_policy_index)
        self.assertEqual(headers['etag'],
                         '"%s"' % self.response_headers['etag'])

        if method == 'GET':
            self.assertEqual(body, self.object_body)

    def test_non_slo_object_GET_part_num(self):
        self._test_non_slo_object_GETorHEAD_part_num('GET', 1)

    def test_non_slo_object_HEAD_part_num(self):
        self._test_non_slo_object_GETorHEAD_part_num('HEAD', 1)

    def _do_test_non_slo_object_part_num_not_satisfiable(self, method,
                                                         part_number):
        req = Request.blank('/bucket/object',
                            params={'partNumber': part_number},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        req.method = method
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '416')
        return body

    def test_non_slo_object_GET_part_num_not_satisfiable(self):
        body = self._do_test_non_slo_object_part_num_not_satisfiable(
            'GET', '2')
        self.assertEqual(self._get_error_code(body), 'InvalidPartNumber')
        body = self._do_test_non_slo_object_part_num_not_satisfiable(
            'GET', '10000')
        self.assertEqual(self._get_error_code(body), 'InvalidPartNumber')

    def test_non_slo_object_HEAD_part_num_not_satisfiable(self):
        body = self._do_test_non_slo_object_part_num_not_satisfiable(
            'HEAD', '2')
        self.assertEqual(body, b'')
        body = self._do_test_non_slo_object_part_num_not_satisfiable(
            'HEAD', '10000')
        self.assertEqual(body, b'')

    def _do_test_non_slo_object_part_num_invalid(self, method, part_number):
        req = Request.blank('/bucket/object',
                            params={'partNumber': part_number},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        req.method = method
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        return body

    def test_non_slo_object_GET_part_num_invalid(self):
        body = self._do_test_non_slo_object_part_num_invalid('GET', '0')
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        body = self._do_test_non_slo_object_part_num_invalid('GET', '-1')
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        body = self._do_test_non_slo_object_part_num_invalid('GET', '10001')
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        with patch.object(self.s3api.conf, 'max_upload_part_num', 1000):
            body = self._do_test_non_slo_object_part_num_invalid('GET', '1001')
            self.assertEqual(self._get_error_code(body), 'InvalidArgument')
            self.assertEqual(
                self._get_error_message(body),
                'Part number must be an integer between 1 and 1000, inclusive')

        body = self._do_test_non_slo_object_part_num_invalid('GET', 'foo')
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        self.assertEqual(
            self._get_error_message(body),
            'Part number must be an integer between 1 and 10000, inclusive')

    def test_non_slo_object_HEAD_part_num_invalid(self):
        body = self._do_test_non_slo_object_part_num_invalid('HEAD', '0')
        self.assertEqual(body, b'')
        body = self._do_test_non_slo_object_part_num_invalid('HEAD', '-1')
        self.assertEqual(body, b'')
        body = self._do_test_non_slo_object_part_num_invalid('HEAD', '10001')
        self.assertEqual(body, b'')
        body = self._do_test_non_slo_object_part_num_invalid('HEAD', 'foo')
        self.assertEqual(body, b'')

    def test_non_slo_object_GET_part_num_and_range(self):
        req = Request.blank('/bucket/object',
                            params={'partNumber': '1'},
                            headers={'Range': 'bytes=1-2',
                                     'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        req.method = 'GET'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(
            self._get_error_message(body),
            'Cannot specify both Range header and partNumber query parameter')

        # partNumber + Range error trumps bad partNumber
        req = Request.blank('/bucket/object',
                            params={'partNumber': '0'},
                            headers={'Range': 'bytes=1-2',
                                     'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        req.method = 'GET'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(
            self._get_error_message(body),
            'Cannot specify both Range header and partNumber query parameter')

    def test_non_slo_object_HEAD_part_num_and_range(self):
        req = Request.blank('/bucket/object',
                            params={'partNumber': '1'},
                            headers={'Range': 'bytes=1-2',
                                     'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        req.method = 'HEAD'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')

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
        self.assertTrue('accept-ranges' in headers)
        self.assertEqual(headers['accept-ranges'], 'bytes')
        self.assertEqual(headers['content-disposition'],
                         'attachment')
        self.assertTrue('content-encoding' in headers)
        self.assertEqual(headers['content-encoding'], 'gzip')

    def test_object_GET_version_id_not_implemented(self):
        # GET version that is not null
        req = Request.blank('/bucket/object?versionId=2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        with patch('swift.common.middleware.s3api.controllers.obj.'
                   'get_swift_info', return_value={}):
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '501', body)

        # GET current version
        req = Request.blank('/bucket/object?versionId=null',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch('swift.common.middleware.s3api.controllers.obj.'
                   'get_swift_info', return_value={}):
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '200', body)
            self.assertEqual(body, self.object_body)
            self.assertTrue('accept-ranges' in headers)
            self.assertEqual(headers['accept-ranges'], 'bytes')

    def test_object_GET_version_id(self):
        # GET current version
        req = Request.blank('/bucket/object?versionId=null',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)
        self.assertEqual(body, self.object_body)

        # GET current version that is not null
        req = Request.blank('/bucket/object?versionId=2',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with self.stubbed_container_info(versioning_enabled=True):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)
        self.assertEqual(body, self.object_body)
        self.assertTrue('accept-ranges' in headers)
        self.assertEqual(headers['accept-ranges'], 'bytes')

        # GET version in archive
        headers = self.response_headers.copy()
        headers['Content-Length'] = 6
        account = 'test:tester'
        grants = [Grant(User(account), 'FULL_CONTROL')]
        headers.update(
            encode_acl('object', ACL(Owner(account, account), grants)))
        self.swift.register(
            'HEAD', '/v1/AUTH_test/bucket/object?version-id=1', swob.HTTPOk,
            headers, None)
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket/object?version-id=1', swob.HTTPOk,
            headers, 'hello1')
        req = Request.blank('/bucket/object?versionId=1',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with self.stubbed_container_info(versioning_enabled=True):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)
        self.assertEqual(body, b'hello1')

        # Version not found
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket/object?version-id=A',
            swob.HTTPNotFound, {}, None)
        req = Request.blank('/bucket/object?versionId=A',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with self.stubbed_container_info(versioning_enabled=True):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')

    def test_object_GET_with_version_id_but_not_enabled(self):
        req = Request.blank('/bucket/object?versionId=A',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with self.stubbed_container_info():
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        elem = fromstring(body, 'Error')
        self.assertEqual(elem.find('Code').text, 'NoSuchVersion')
        self.assertEqual(elem.find('Key').text, 'object')
        self.assertEqual(elem.find('VersionId').text, 'A')
        expected_calls = []
        # NB: No actual backend GET!
        self.assertEqual(expected_calls, self.swift.calls)

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
        code = self._test_method_error(
            'PUT', '/bucket/object',
            swob.HTTPUnprocessableEntity,
            headers={'Content-MD5': '1B2M2Y8AsgTpgAmY7PhCfg=='})
        self.assertEqual(code, 'BadDigest')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPLengthRequired)
        self.assertEqual(code, 'MissingContentLength')
        # Swift can 412 if the versions container is missing
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPPreconditionFailed)
        self.assertEqual(code, 'PreconditionFailed')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPServiceUnavailable)
        self.assertEqual(code, 'ServiceUnavailable')
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
            {'X-Amz-Copy-Source': '/src_bucket/src_object',
             'X-Amz-Copy-Source-Range': 'bytes=0-0'})
        self.assertEqual(code, 'InvalidArgument')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPRequestTimeout)
        self.assertEqual(code, 'RequestTimeout')
        code = self._test_method_error('PUT', '/bucket/object',
                                       swob.HTTPClientDisconnect,
                                       {})
        self.assertEqual(code, 'RequestTimeout')

    def test_object_PUT(self):
        etag = self.response_headers['etag']
        content_md5 = binascii.b2a_base64(
            binascii.a2b_hex(etag)).strip().decode('ascii')

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

    def test_object_PUT_bad_hash(self):
        # FakeSwift doesn't care if the etag matches, so we explicitly register
        # the 422 that the proxy would have passed on from the object servers
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPUnprocessableEntity, {},
                            'Unprocessable Entity')

        bad_etag = md5(b'not-same-content').hexdigest()
        content_md5 = binascii.b2a_base64(
            binascii.a2b_hex(bad_etag)).strip().decode('ascii')

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
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'BadDigest')
        self.assertIn(b'Content-MD5', body)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ.get('swift.backend_path'))

    def test_object_PUT_quota_exceeded(self):
        etag = self.response_headers['etag']
        content_md5 = binascii.b2a_base64(
            binascii.a2b_hex(etag)).strip().decode('ascii')

        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/object',
            swob.HTTPRequestEntityTooLarge, {}, 'Upload exceeds quota.')
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
        self.assertEqual(status.split()[0], '400')
        self.assertIn(b'<Code>EntityTooLarge</Code>', body)
        self.assertIn(b'<Message>Upload exceeds quota.</Message', body)

    def test_object_PUT_v4(self):
        body_sha = sha256(self.object_body).hexdigest()
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
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ.get('swift.backend_path'))

    def test_object_PUT_v4_bad_hash(self):
        orig_app = self.s3api.app

        def error_catching_app(env, start_response):
            try:
                return orig_app(env, start_response)
            except Exception:
                self.logger.exception('uh oh')
                start_response('599 Uh Oh', [])
                return [b'']

        self.s3api.app = error_catching_app

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
                'x-amz-content-sha256': '0' * 64,
                'Date': self.get_date_header()},
            body=self.object_body)
        req.date = datetime.now()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body),
                         'XAmzContentSHA256Mismatch')
        self.assertIn(b'x-amz-content-sha256', body)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ.get('swift.backend_path'))

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
        self.assertEqual(self._get_error_code(body),
                         'InvalidArgument')
        self.assertIn(b'<ArgumentName>x-amz-content-sha256</ArgumentName>',
                      body)
        self.assertNotIn('swift.backend_path', req.environ)

    def test_object_PUT_v4_unsigned_payload(self):
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
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD',
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
        self.assertIn(b'UNSIGNED-PAYLOAD', SigV4Request(
            req.environ, self.s3api.conf)._canonical_request())

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

    def test_object_PUT_copy(self):
        def do_test(src_path):
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

    def test_object_PUT_copy_metadata_replace(self):
        with mock_timestamp_now(klass=S3Timestamp) as now:
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

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Content-Type'], 'application/xml')
        self.assertIsNone(headers.get('etag'))
        elem = fromstring(body, 'CopyObjectResult')
        self.assertEqual(S3Timestamp(now.ceil()).s3xmlformat,
                         elem.find('LastModified').text)
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
        self.assertEqual(headers['Content-Type'], 'so')
        self.assertEqual(headers['Expires'], 'yeah')
        self.assertEqual(headers['X-Robots-Tag'], 'bye')

        self.assertEqual(headers['Content-Length'], '0')

    def test_object_PUT_copy_metadata_copy(self):
        with mock_timestamp_now(klass=S3Timestamp) as now:
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

        self.assertEqual(status.split()[0], '200')
        self.assertEqual(headers['Content-Type'], 'application/xml')
        self.assertIsNone(headers.get('etag'))

        elem = fromstring(body, 'CopyObjectResult')
        self.assertEqual(S3Timestamp(now.ceil()).s3xmlformat,
                         elem.find('LastModified').text)
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

    def test_object_POST_error(self):
        code = self._test_method_error('POST', '/bucket/object', None)
        self.assertEqual(code, 'NotImplemented')

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
        self.assertEqual(code, 'ServiceUnavailable')

        with patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                return_value={'status': 404}):
            code = self._test_method_error('DELETE', '/bucket/object',
                                           swob.HTTPNotFound)
            self.assertEqual(code, 'NoSuchBucket')

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

    def test_object_DELETE_with_version_id_but_not_enabled(self):
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})
        with self.stubbed_container_info():
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        expected_calls = []
        # NB: No actual backend DELETE!
        self.assertEqual(expected_calls, self.swift.calls)

    def test_object_DELETE_multipart(self):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

        self.assertIn(('HEAD', '/v1/AUTH_test/bucket/object?symlink=get'),
                      self.swift.calls)
        self.assertEqual(('DELETE', '/v1/AUTH_test/bucket/object'),
                         self.swift.calls[-1])
        _, path = self.swift.calls[-1]
        self.assertEqual(path.count('?'), 0)

    def test_object_DELETE_missing(self):
        self.s3acl_response_modified = True
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNotFound, {}, None)
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')

        self.assertEqual(('HEAD', '/v1/AUTH_test/bucket/object?symlink=get'),
                         self.swift.calls[0])
        # the s3acl retests w/ a get_container_info HEAD @ self.swift.calls[1]
        self.assertEqual(('DELETE', '/v1/AUTH_test/bucket/object'),
                         self.swift.calls[-1])

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
        self.assertEqual(body, b'')

        self.assertIn(('HEAD', '/v1/AUTH_test/bucket/object?symlink=get'),
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
        # HEAD did not indicate that it was an S3 MPU, so no async delete
        self.assertNotIn('async', query)
        self.assertNotIn('Content-Type', headers)

    def test_slo_object_async_DELETE(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk,
                            {'x-static-large-object': 'True',
                             'x-object-sysmeta-s3api-etag': 's3-style-etag'},
                            None)
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNoContent, {}, '')
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'Content-Type': 'foo/bar'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual(body, b'')

        self.assertIn(('HEAD', '/v1/AUTH_test/bucket/object?symlink=get'),
                      self.swift.calls)
        self.assertIn(('DELETE', '/v1/AUTH_test/bucket/object'
                                 '?async=on&multipart-manifest=delete'),
                      self.swift.calls)
        _, path, headers = self.swift.calls_with_headers[-1]
        path, query_string = path.split('?', 1)
        query = {}
        for q in query_string.split('&'):
            key, arg = q.split('=')
            query[key] = arg
        self.assertEqual(query['multipart-manifest'], 'delete')
        self.assertEqual(query['async'], 'on')
        self.assertNotIn('Content-Type', headers)

    def _test_set_container_permission(self, account, permission):
        self.s3acl_response_modified = True
        grants = [Grant(User(account), permission)]
        headers = \
            encode_acl('container',
                       ACL(Owner('test:tester', 'test:tester'), grants))
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, headers, None)


class TestS3ApiObj(BaseS3ApiObj, S3ApiTestCase):

    def test_object_GET_Range_error(self):
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPRequestedRangeNotSatisfiable)
        self.assertEqual(code, 'InvalidRange')

    @patch_policies([
        StoragePolicy(0, 'gold', is_default=True),
        StoragePolicy(1, 'silver')])
    def test_object_policy_index_logging(self):
        self._do_test_object_policy_index_logging(self.bucket_policy_index)
        self._register_bucket_policy_index_head('bucket', 0)
        self._do_test_object_policy_index_logging(0)

    def test_object_PUT_with_version(self):
        self.swift.register('GET',
                            '/v1/AUTH_test/bucket/src_obj?version-id=foo',
                            swob.HTTPOk, self.response_headers,
                            self.object_body)
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPCreated, {
                                'etag': self.etag,
                                'last-modified': self.last_modified,
                            }, None)

        req = Request.blank('/bucket/object', method='PUT', body='', headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header(),
            'X-Amz-Copy-Source': '/bucket/src_obj?versionId=foo',
        })
        status, headers, body = self.call_s3api(req)

        self.assertEqual('200 OK', status)
        elem = fromstring(body, 'CopyObjectResult')
        self.assertEqual(elem.find('ETag').text, '"%s"' % self.etag)

        self.assertEqual(self.swift.calls, [
            ('HEAD', '/v1/AUTH_test/bucket/src_obj?version-id=foo'),
            ('PUT', '/v1/AUTH_test/bucket/object?version-id=foo'),
        ])
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(headers['x-copy-from'], '/bucket/src_obj')

    def test_object_PUT_headers(self):
        content_md5 = binascii.b2a_base64(binascii.a2b_hex(
            self.etag)).strip().decode('ascii')

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
                     'Date': self.get_date_header()},
            body=self.object_body)
        req.date = datetime.now()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual('200 ', status[:4], body)
        # Check that s3api does not return an etag header,
        # specified copy source.
        self.assertNotIn('etag', headers)
        # Check that s3api does not return custom metadata in response
        self.assertNotIn('x-amz-meta-something', headers)

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

    @patch_policies([
        StoragePolicy(0, 'gold', is_default=True),
        StoragePolicy(1, 'silver')])
    def test_simple_object_copy(self):
        src_policy_index = 0
        self._register_bucket_policy_index_head('some', src_policy_index)
        dst_policy_index = 1
        self._register_bucket_policy_index_head('bucket', dst_policy_index)
        self.swift.register('HEAD', '/v1/AUTH_test/some/source',
                            swob.HTTPOk, {}, None)
        req = Request.blank(
            '/bucket/object', method='PUT',
            headers={
                'Authorization': 'AWS test:tester:hmac',
                'X-Amz-Copy-Source': '/some/source',
                'Date': self.get_date_header(),
            },
        )
        timestamp = time.time()
        with patch('swift.common.middleware.s3api.utils.time.time',
                   return_value=timestamp):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self._assert_policy_index(req.headers, headers, dst_policy_index)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ['swift.backend_path'])

        head_call, put_call = self.swift.call_list
        self.assertNotIn('x-backend-storage-policy-index', head_call.headers)
        self.assertNotIn('x-backend-storage-policy-index', put_call.headers)
        self.assertEqual(put_call.headers['x-copy-from'], '/some/source')

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

    def test_object_DELETE_old_version_id(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers, None)
        resp_headers = {'X-Object-Current-Version-Id': '1574360804.34906'}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object'
                            '?symlink=get&version-id=1574358170.12293',
                            swob.HTTPNoContent, resp_headers, None)
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})

        with self.stubbed_container_info(versioning_enabled=True):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('DELETE', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293')
        ], self.swift.calls)

    def test_object_DELETE_current_version_id(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers, None)
        resp_headers = {'X-Object-Current-Version-Id': 'null'}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object'
                            '?symlink=get&version-id=1574358170.12293',
                            swob.HTTPNoContent, resp_headers, None)
        old_versions = [{
            'name': 'object',
            'version_id': '1574341899.21751',
            'content_type': 'application/found',
        }, {
            'name': 'object',
            'version_id': '1574333192.15190',
            'content_type': 'application/older',
        }]
        self.swift.register('GET', '/v1/AUTH_test/bucket', swob.HTTPOk, {},
                            json.dumps(old_versions))
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})
        fake_info = {
            'status': 204,
            'sysmeta': {
                'versions-container': '\x00versions\x00bucket',
            }
        }
        with patch('swift.common.middleware.s3api.s3request.'
                   'get_container_info', return_value=fake_info):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293', None),
            ('DELETE', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293', None),
            ('GET', '/v1/AUTH_test/bucket'
             '?prefix=object&versions=True', '0'),
            ('PUT', '/v1/AUTH_test/bucket/object'
             '?version-id=1574341899.21751', '0'),
        ], [
            (method, path, headers.get('content-length'))
            for method, path, headers in self.swift.calls_with_headers])

    def test_object_DELETE_version_id_not_implemented(self):
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})

        with patch('swift.common.middleware.s3api.controllers.obj.'
                   'get_swift_info', return_value={}):
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '501', body)

    def test_object_DELETE_current_version_id_is_delete_marker(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers, None)
        resp_headers = {'X-Object-Current-Version-Id': 'null'}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object'
                            '?symlink=get&version-id=1574358170.12293',
                            swob.HTTPNoContent, resp_headers, None)
        old_versions = [{
            'name': 'object',
            'version_id': '1574341899.21751',
            'content_type': 'application/x-deleted;swift_versions_deleted=1',
        }]
        self.swift.register('GET', '/v1/AUTH_test/bucket', swob.HTTPOk, {},
                            json.dumps(old_versions))
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})
        fake_info = {
            'status': 204,
            'sysmeta': {
                'versions-container': '\x00versions\x00bucket',
            }
        }
        with patch('swift.common.middleware.s3api.s3request.'
                   'get_container_info', return_value=fake_info):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('DELETE', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('GET', '/v1/AUTH_test/bucket'
             '?prefix=object&versions=True'),
        ], self.swift.calls)

    def test_object_DELETE_current_version_id_is_missing(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers, None)
        resp_headers = {'X-Object-Current-Version-Id': 'null'}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object'
                            '?symlink=get&version-id=1574358170.12293',
                            swob.HTTPNoContent, resp_headers, None)
        old_versions = [{
            'name': 'object',
            'version_id': '1574341899.21751',
            'content_type': 'application/missing',
        }, {
            'name': 'object',
            'version_id': '1574333192.15190',
            'content_type': 'application/found',
        }]
        self.swift.register('GET', '/v1/AUTH_test/bucket', swob.HTTPOk, {},
                            json.dumps(old_versions))
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object'
                            '?version-id=1574341899.21751',
                            swob.HTTPPreconditionFailed, {}, None)
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object'
                            '?version-id=1574333192.15190',
                            swob.HTTPCreated, {}, None)
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})
        fake_info = {
            'status': 204,
            'sysmeta': {
                'versions-container': '\x00versions\x00bucket',
            }
        }
        with patch('swift.common.middleware.s3api.s3request.'
                   'get_container_info', return_value=fake_info):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('DELETE', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('GET', '/v1/AUTH_test/bucket'
             '?prefix=object&versions=True'),
            ('PUT', '/v1/AUTH_test/bucket/object'
             '?version-id=1574341899.21751'),
            ('PUT', '/v1/AUTH_test/bucket/object'
             '?version-id=1574333192.15190'),
        ], self.swift.calls)

    def test_object_DELETE_current_version_id_GET_error(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers, None)
        resp_headers = {'X-Object-Current-Version-Id': 'null'}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object'
                            '?symlink=get&version-id=1574358170.12293',
                            swob.HTTPNoContent, resp_headers, None)
        self.swift.register('GET', '/v1/AUTH_test/bucket',
                            swob.HTTPServerError, {}, '')
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})
        fake_info = {
            'status': 204,
            'sysmeta': {
                'versions-container': '\x00versions\x00bucket',
            }
        }
        with patch('swift.common.middleware.s3api.s3request.'
                   'get_container_info', return_value=fake_info):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '500')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('DELETE', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('GET', '/v1/AUTH_test/bucket'
             '?prefix=object&versions=True'),
        ], self.swift.calls)

    def test_object_DELETE_current_version_id_PUT_error(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, self.response_headers, None)
        resp_headers = {'X-Object-Current-Version-Id': 'null'}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object'
                            '?symlink=get&version-id=1574358170.12293',
                            swob.HTTPNoContent, resp_headers, None)
        old_versions = [{
            'name': 'object',
            'version_id': '1574341899.21751',
            'content_type': 'application/foo',
        }]
        self.swift.register('GET', '/v1/AUTH_test/bucket', swob.HTTPOk, {},
                            json.dumps(old_versions))
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object'
                            '?version-id=1574341899.21751',
                            swob.HTTPServerError, {}, None)
        req = Request.blank('/bucket/object?versionId=1574358170.12293',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})
        fake_info = {
            'status': 204,
            'sysmeta': {
                'versions-container': '\x00versions\x00bucket',
            }
        }
        with patch('swift.common.middleware.s3api.s3request.'
                   'get_container_info', return_value=fake_info):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '500')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('DELETE', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574358170.12293'),
            ('GET', '/v1/AUTH_test/bucket'
             '?prefix=object&versions=True'),
            ('PUT', '/v1/AUTH_test/bucket/object'
             '?version-id=1574341899.21751'),
        ], self.swift.calls)

    def test_object_DELETE_in_versioned_container_without_version(self):
        resp_headers = {
            'X-Object-Version-Id': '1574360804.34906',
            'X-Backend-Content-Type': DELETE_MARKER_CONTENT_TYPE}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNoContent, resp_headers, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket',
                            swob.HTTPNoContent, {
                                'X-Container-Sysmeta-Versions-Enabled': True},
                            None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNotFound, self.response_headers, None)
        req = Request.blank('/bucket/object', method='DELETE', headers={
            'Authorization': 'AWS test:tester:hmac',
            'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object?symlink=get'),
            ('HEAD', '/v1/AUTH_test'),
            ('HEAD', '/v1/AUTH_test/bucket'),
            ('DELETE', '/v1/AUTH_test/bucket/object'),
        ], self.swift.calls)

        self.assertEqual('1574360804.34906', headers.get('x-amz-version-id'))
        self.assertEqual('true', headers.get('x-amz-delete-marker'))

    def test_object_DELETE_in_versioned_container_with_version_id(self):
        resp_headers = {
            'X-Object-Version-Id': '1574701081.61553'}
        self.swift.register('DELETE', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNoContent, resp_headers, None)
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPNotFound, self.response_headers, None)
        req = Request.blank('/bucket/object?versionId=1574701081.61553',
                            method='DELETE', headers={
                                'Authorization': 'AWS test:tester:hmac',
                                'Date': self.get_date_header()})
        fake_info = {
            'status': 204,
            'sysmeta': {
                'versions-container': '\x00versions\x00bucket',
            }
        }
        with patch('swift.common.middleware.s3api.s3request.'
                   'get_container_info', return_value=fake_info):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '204')
        self.assertEqual([
            ('HEAD', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574701081.61553'),
            ('DELETE', '/v1/AUTH_test/bucket/object'
             '?symlink=get&version-id=1574701081.61553'),
        ], self.swift.calls)

        self.assertEqual('1574701081.61553', headers.get('x-amz-version-id'))

    def test_cors_preflight(self):
        req = Request.blank(
            '/bucket/cors-object',
            environ={'REQUEST_METHOD': 'OPTIONS'},
            headers={'Origin': 'http://example.com',
                     'Access-Control-Request-Method': 'GET',
                     'Access-Control-Request-Headers': 'authorization'})
        self.s3api.conf.cors_preflight_allow_origin = ['*']
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertDictEqual(headers, {
            'Allow': 'GET, HEAD, PUT, POST, DELETE, OPTIONS',
            'Access-Control-Allow-Origin': 'http://example.com',
            'Access-Control-Allow-Methods': ('GET, HEAD, PUT, POST, DELETE, '
                                             'OPTIONS'),
            'Access-Control-Allow-Headers': 'authorization',
            'Vary': 'Origin, Access-Control-Request-Headers',
        })

        # test more allow_origins
        self.s3api.conf.cors_preflight_allow_origin = ['http://example.com',
                                                       'http://other.com']
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertDictEqual(headers, {
            'Allow': 'GET, HEAD, PUT, POST, DELETE, OPTIONS',
            'Access-Control-Allow-Origin': 'http://example.com',
            'Access-Control-Allow-Methods': ('GET, HEAD, PUT, POST, DELETE, '
                                             'OPTIONS'),
            'Access-Control-Allow-Headers': 'authorization',
            'Vary': 'Origin, Access-Control-Request-Headers',
        })

        # test presigned urls
        req = Request.blank(
            '/bucket/cors-object?AWSAccessKeyId=test%3Atester&'
            'Expires=1621558415&Signature=MKMdW3FpYcoFEJlTLF3EhP7AJgc%3D',
            environ={'REQUEST_METHOD': 'OPTIONS'},
            headers={'Origin': 'http://example.com',
                     'Access-Control-Request-Method': 'PUT'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertDictEqual(headers, {
            'Allow': 'GET, HEAD, PUT, POST, DELETE, OPTIONS',
            'Access-Control-Allow-Origin': 'http://example.com',
            'Access-Control-Allow-Methods': ('GET, HEAD, PUT, POST, DELETE, '
                                             'OPTIONS'),
            'Vary': 'Origin, Access-Control-Request-Headers',
        })
        req = Request.blank(
            '/bucket/cors-object?X-Amz-Algorithm=AWS4-HMAC-SHA256&'
            'X-Amz-Credential=test%3Atester%2F20210521%2Fus-east-1%2Fs3%2F'
            'aws4_request&X-Amz-Date=20210521T003835Z&X-Amz-Expires=900&'
            'X-Amz-Signature=e413549f2cbeddb457c5fddb2d28820ce58de514bb900'
            '5d588800d7ebb1a6a2d&X-Amz-SignedHeaders=host',
            environ={'REQUEST_METHOD': 'OPTIONS'},
            headers={'Origin': 'http://example.com',
                     'Access-Control-Request-Method': 'DELETE'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertDictEqual(headers, {
            'Allow': 'GET, HEAD, PUT, POST, DELETE, OPTIONS',
            'Access-Control-Allow-Origin': 'http://example.com',
            'Access-Control-Allow-Methods': ('GET, HEAD, PUT, POST, DELETE, '
                                             'OPTIONS'),
            'Vary': 'Origin, Access-Control-Request-Headers',
        })

        # Wrong protocol
        self.s3api.conf.cors_preflight_allow_origin = ['https://example.com']
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '401 Unauthorized')
        self.assertEqual(headers, {
            'Allow': 'GET, HEAD, PUT, POST, DELETE, OPTIONS',
        })

    def test_cors_headers(self):
        # note: Access-Control-Allow-Methods would normally be expected in
        # response to an OPTIONS request but its included here in GET/PUT tests
        # to check that it is always passed back in S3Response
        cors_headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': ('GET, PUT, POST, COPY, '
                                             'DELETE, PUT, OPTIONS'),
            'Access-Control-Expose-Headers':
                'x-object-meta-test, x-object-meta-test=5funderscore, etag',
        }
        get_resp_headers = self.response_headers
        get_resp_headers['x-object-meta-test=5funderscore'] = 'underscored'
        self.swift.register(
            'GET', '/v1/AUTH_test/bucket/cors-object', swob.HTTPOk,
            dict(get_resp_headers, **cors_headers),
            self.object_body)
        self.swift.register(
            'PUT', '/v1/AUTH_test/bucket/cors-object', swob.HTTPCreated,
            dict({'etag': self.etag,
                  'last-modified': self.last_modified,
                  'x-object-meta-something': 'oh hai',
                  'x-object-meta-test=5funderscore': 'underscored'},
                 **cors_headers),
            None)

        req = Request.blank(
            '/bucket/cors-object',
            environ={'REQUEST_METHOD': 'GET'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header(),
                     'Origin': 'http://example.com',
                     'Access-Control-Request-Method': 'GET',
                     'Access-Control-Request-Headers': 'authorization'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertIn('Access-Control-Allow-Origin', headers)
        self.assertEqual(headers['Access-Control-Allow-Origin'], '*')
        self.assertIn('Access-Control-Expose-Headers', headers)
        self.assertEqual(
            headers['Access-Control-Expose-Headers'],
            'x-amz-meta-test, x-amz-meta-test_underscore, etag, '
            'x-amz-request-id, x-amz-id-2')
        self.assertIn('Access-Control-Allow-Methods', headers)
        self.assertEqual(
            headers['Access-Control-Allow-Methods'],
            'GET, PUT, POST, DELETE, PUT, OPTIONS')
        self.assertIn('x-amz-meta-test_underscore', headers)
        self.assertEqual('underscored', headers['x-amz-meta-test_underscore'])

        req = Request.blank(
            '/bucket/cors-object',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header(),
                     'Origin': 'http://example.com',
                     'Access-Control-Request-Method': 'PUT',
                     'Access-Control-Request-Headers': 'authorization'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        self.assertIn('Access-Control-Allow-Origin', headers)
        self.assertEqual(headers['Access-Control-Allow-Origin'], '*')
        self.assertIn('Access-Control-Expose-Headers', headers)
        self.assertEqual(
            headers['Access-Control-Expose-Headers'],
            'x-amz-meta-test, x-amz-meta-test_underscore, etag, '
            'x-amz-request-id, x-amz-id-2')
        self.assertIn('Access-Control-Allow-Methods', headers)
        self.assertEqual(
            headers['Access-Control-Allow-Methods'],
            'GET, PUT, POST, DELETE, PUT, OPTIONS')
        self.assertEqual('underscored', headers['x-amz-meta-test_underscore'])


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


class TestS3ApiObjAcl(BaseS3ApiObj, S3ApiTestCaseAcl):

    def _test_object_for_s3acl(self, method, account):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS %s:hmac' % account,
                                     'Date': self.get_date_header()})
        return self.call_s3api(req)

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

    def test_object_GET_with_s3acl_and_unknown_user(self):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch.object(self.app, 'remote_user', None):
            status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '403 Forbidden')
        self.assertEqual(self._get_error_code(body), 'SignatureDoesNotMatch')

    def test_object_GET_with_s3acl_and_keystone(self):
        # for passing keystone authentication root
        orig_auth = self.app.handle
        calls = []

        def wrapped_auth(env):
            calls.append((env['REQUEST_METHOD'], 's3api.auth_details' in env))
            orig_auth(env)

        with patch.object(self.app, 'handle', wrapped_auth):
            self._test_object_GETorHEAD('GET')
        self.assertEqual(calls, [
            ('TEST', True),
            ('HEAD', False),
            ('GET', False),
        ])

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

    def test_object_GET_Range_error(self):
        # needed for pre-flight ACL HEAD request, FakeSwift finds the 416
        # for the GET and returns it for HEAD but s3api won't error
        # correctly to 416 on HEAD
        self.swift.register('HEAD', '/v1/AUTH_test/bucket/object',
                            swob.HTTPOk, {}, None),
        code = self._test_method_error('GET', '/bucket/object',
                                       swob.HTTPRequestedRangeNotSatisfiable)
        self.assertEqual(code, 'InvalidRange')

    def test_object_GET_without_permission(self):
        status, headers, body = self._test_object_for_s3acl('GET',
                                                            'test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_object_GET_with_read_permission(self):
        status, headers, body = self._test_object_for_s3acl('GET',
                                                            'test:read')
        self.assertEqual(status.split()[0], '200')

    def test_object_GET_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_object_for_s3acl('GET', 'test:full_control')
        self.assertEqual(status.split()[0], '200')

    def test_object_PUT_without_permission(self):
        status, headers, body = self._test_object_for_s3acl('PUT',
                                                            'test:other')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_object_PUT_with_owner_permission(self):
        status, headers, body = self._test_object_for_s3acl('PUT',
                                                            'test:tester')
        self.assertEqual(status.split()[0], '200')

    def test_object_PUT_with_write_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'WRITE')
        status, headers, body = self._test_object_for_s3acl('PUT', account)
        self.assertEqual(status.split()[0], '200')

    def test_object_PUT_with_fullcontrol_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'FULL_CONTROL')
        status, headers, body = \
            self._test_object_for_s3acl('PUT', account)
        self.assertEqual(status.split()[0], '200')

    def test_object_DELETE_without_permission(self):
        account = 'test:other'
        status, headers, body = self._test_object_for_s3acl('DELETE',
                                                            account)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_object_DELETE_with_owner_permission(self):
        status, headers, body = self._test_object_for_s3acl('DELETE',
                                                            'test:tester')
        self.assertEqual(status.split()[0], '204')

    def test_object_DELETE_with_write_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'WRITE')
        status, headers, body = self._test_object_for_s3acl('DELETE',
                                                            account)
        self.assertEqual(status.split()[0], '204')

    def test_object_DELETE_with_fullcontrol_permission(self):
        account = 'test:other'
        self._test_set_container_permission(account, 'FULL_CONTROL')
        status, headers, body = self._test_object_for_s3acl('DELETE', account)
        self.assertEqual(status.split()[0], '204')

    def test_object_PUT_copy_with_owner_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:tester')
        self.assertEqual(status.split()[0], '200')

    def test_object_PUT_copy_with_fullcontrol_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:full_control',
                                             'FULL_CONTROL')
        self.assertEqual(status.split()[0], '200')

    def test_object_PUT_copy_with_grantee_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:write', 'READ')
        self.assertEqual(status.split()[0], '200')

    def test_object_PUT_copy_without_src_obj_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:write')
        self.assertEqual(status.split()[0], '403')

    def test_object_PUT_copy_without_dst_container_permission(self):
        status, headers, body = \
            self._test_object_copy_for_s3acl('test:other', 'READ')
        self.assertEqual(status.split()[0], '403')

    def test_object_PUT_copy_empty_src_path(self):
        self.swift.register('PUT', '/v1/AUTH_test/bucket/object',
                            swob.HTTPPreconditionFailed, {}, None)
        status, headers, body = self._test_object_copy_for_s3acl(
            'test:write', 'READ', src_path='')
        self.assertEqual(status.split()[0], '400')


class TestS3ApiObjNonUTCAcl(TestS3ApiObjAcl):
    def setUp(self):
        self.orig_tz = os.environ.get('TZ', '')
        os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
        time.tzset()
        super(TestS3ApiObjNonUTCAcl, self).setUp()

    def tearDown(self):
        super(TestS3ApiObjNonUTCAcl, self).tearDown()
        os.environ['TZ'] = self.orig_tz
        time.tzset()


if __name__ == '__main__':
    unittest.main()
