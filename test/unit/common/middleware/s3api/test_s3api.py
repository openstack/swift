# Copyright (c) 2011-2014 OpenStack Foundation.
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
from mock import patch, MagicMock
import calendar
from datetime import datetime
import hashlib
import mock
import requests
import json
import copy
from urllib import unquote, quote

import swift.common.middleware.s3api
from swift.common.middleware.keystoneauth import KeystoneAuth
from swift.common import swob, utils
from swift.common.swob import Request

from keystonemiddleware.auth_token import AuthProtocol
from keystoneauth1.access import AccessInfoV2

from test.unit.common.middleware.s3api import S3ApiTestCase
from test.unit.common.middleware.s3api.helpers import FakeSwift
from test.unit.common.middleware.s3api.test_s3token import \
    GOOD_RESPONSE_V2, GOOD_RESPONSE_V3
from swift.common.middleware.s3api.s3request import SigV4Request, S3Request
from swift.common.middleware.s3api.etree import fromstring
from swift.common.middleware.s3api.s3api import filter_factory, \
    S3ApiMiddleware
from swift.common.middleware.s3api.s3token import S3Token


class TestListingMiddleware(S3ApiTestCase):
    def test_s3_etag_in_json(self):
        # This translation happens all the time, even on normal swift requests
        body_data = json.dumps([
            {'name': 'obj1', 'hash': '0123456789abcdef0123456789abcdef'},
            {'name': 'obj2', 'hash': 'swiftetag; s3_etag=mu-etag'},
            {'name': 'obj2', 'hash': 'swiftetag; something=else'},
            {'subdir': 'path/'},
        ]).encode('ascii')
        self.swift.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {'Content-Type': 'application/json; charset=UTF-8'},
            body_data)

        req = Request.blank('/v1/a/c')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(json.loads(body), [
            {'name': 'obj1', 'hash': '0123456789abcdef0123456789abcdef'},
            {'name': 'obj2', 'hash': 'swiftetag', 's3_etag': '"mu-etag"'},
            {'name': 'obj2', 'hash': 'swiftetag; something=else'},
            {'subdir': 'path/'},
        ])

    def test_s3_etag_non_json(self):
        self.swift.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {'Content-Type': 'application/json; charset=UTF-8'},
            b'Not actually JSON')
        req = Request.blank('/v1/a/c')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(body, b'Not actually JSON')

        # Yes JSON, but wrong content-type
        body_data = json.dumps([
            {'name': 'obj1', 'hash': '0123456789abcdef0123456789abcdef'},
            {'name': 'obj2', 'hash': 'swiftetag; s3_etag=mu-etag'},
            {'name': 'obj2', 'hash': 'swiftetag; something=else'},
            {'subdir': 'path/'},
        ]).encode('ascii')
        self.swift.register(
            'GET', '/v1/a/c', swob.HTTPOk,
            {'Content-Type': 'text/plain; charset=UTF-8'},
            body_data)
        req = Request.blank('/v1/a/c')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(body, body_data)


class TestS3ApiMiddleware(S3ApiTestCase):
    def setUp(self):
        super(TestS3ApiMiddleware, self).setUp()

        self.swift.register('GET', '/something', swob.HTTPOk, {}, 'FAKE APP')

    def test_non_s3_request_passthrough(self):
        req = Request.blank('/something')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(body, 'FAKE APP')

    def test_bad_format_authorization(self):
        req = Request.blank('/something',
                            headers={'Authorization': 'hoge',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_bad_method(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MethodNotAllowed')

    def test_bad_method_but_method_exists_in_controller(self):
        req = Request.blank(
            '/bucket',
            environ={'REQUEST_METHOD': '_delete_segments_bucket'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MethodNotAllowed')

    def test_path_info_encode(self):
        bucket_name = 'b%75cket'
        object_name = 'ob%6aect:1'
        self.swift.register('GET', '/v1/AUTH_test/bucket/object:1',
                            swob.HTTPOk, {}, None)
        req = Request.blank('/%s/%s' % (bucket_name, object_name),
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        raw_path_info = "/%s/%s" % (bucket_name, object_name)
        path_info = req.environ['PATH_INFO']
        self.assertEqual(path_info, unquote(raw_path_info))
        self.assertEqual(req.path, quote(path_info))

    def test_canonical_string_v2(self):
        """
        The hashes here were generated by running the same requests against
        boto.utils.canonical_string
        """
        def canonical_string(path, headers):
            if '?' in path:
                path, query_string = path.split('?', 1)
            else:
                query_string = ''
            env = {
                'REQUEST_METHOD': 'GET',
                'PATH_INFO': path,
                'QUERY_STRING': query_string,
                'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
            }
            for header, value in headers.items():
                header = 'HTTP_' + header.replace('-', '_').upper()
                if header in ('HTTP_CONTENT_TYPE', 'HTTP_CONTENT_LENGTH'):
                    header = header[5:]
                env[header] = value

            with patch('swift.common.middleware.s3api.s3request.'
                       'S3Request._validate_headers'), \
                    patch('swift.common.middleware.s3api.s3request.'
                          'S3Request._validate_dates'):
                req = S3Request(env)
            return req.environ['s3api.auth_details']['string_to_sign']

        def verify(hash, path, headers):
            s = canonical_string(path, headers)
            self.assertEqual(hash, hashlib.md5(s).hexdigest())

        verify('6dd08c75e42190a1ce9468d1fd2eb787', '/bucket/object',
               {'Content-Type': 'text/plain', 'X-Amz-Something': 'test',
                'Date': 'whatever'})

        verify('c8447135da232ae7517328f3429df481', '/bucket/object',
               {'Content-Type': 'text/plain', 'X-Amz-Something': 'test'})

        verify('bf49304103a4de5c325dce6384f2a4a2', '/bucket/object',
               {'content-type': 'text/plain'})

        verify('be01bd15d8d47f9fe5e2d9248cc6f180', '/bucket/object', {})

        verify('e9ec7dca45eef3e2c7276af23135e896', '/bucket/object',
               {'Content-MD5': 'somestuff'})

        verify('a822deb31213ad09af37b5a7fe59e55e', '/bucket/object?acl', {})

        verify('cce5dd1016595cb706c93f28d3eaa18f', '/bucket/object',
               {'Content-Type': 'text/plain', 'X-Amz-A': 'test',
                'X-Amz-Z': 'whatever', 'X-Amz-B': 'lalala',
                'X-Amz-Y': 'lalalalalalala'})

        verify('7506d97002c7d2de922cc0ec34af8846', '/bucket/object',
               {'Content-Type': None, 'X-Amz-Something': 'test'})

        verify('28f76d6162444a193b612cd6cb20e0be', '/bucket/object',
               {'Content-Type': None,
                'X-Amz-Date': 'Mon, 11 Jul 2011 10:52:57 +0000',
                'Date': 'Tue, 12 Jul 2011 10:52:57 +0000'})

        verify('ed6971e3eca5af4ee361f05d7c272e49', '/bucket/object',
               {'Content-Type': None,
                'Date': 'Tue, 12 Jul 2011 10:52:57 +0000'})

        verify('41ecd87e7329c33fea27826c1c9a6f91', '/bucket/object?cors', {})

        verify('d91b062f375d8fab407d6dab41fd154e', '/bucket/object?tagging',
               {})

        verify('ebab878a96814b30eb178e27efb3973f', '/bucket/object?restore',
               {})

        verify('f6bf1b2d92b054350d3679d28739fc69', '/bucket/object?'
               'response-cache-control&response-content-disposition&'
               'response-content-encoding&response-content-language&'
               'response-content-type&response-expires', {})

        str1 = canonical_string('/', headers={'Content-Type': None,
                                              'X-Amz-Something': 'test'})
        str2 = canonical_string('/', headers={'Content-Type': '',
                                              'X-Amz-Something': 'test'})
        str3 = canonical_string('/', headers={'X-Amz-Something': 'test'})

        self.assertEqual(str1, str2)
        self.assertEqual(str2, str3)

        # Note that boto does not do proper stripping (as of 2.42.0).
        # These were determined by examining the StringToSignBytes element of
        # resulting SignatureDoesNotMatch errors from AWS.
        str1 = canonical_string('/', {'Content-Type': 'text/plain',
                                      'Content-MD5': '##'})
        str2 = canonical_string('/', {'Content-Type': '\x01\x02text/plain',
                                      'Content-MD5': '\x1f ##'})
        str3 = canonical_string('/', {'Content-Type': 'text/plain \x10',
                                      'Content-MD5': '##\x18'})

        self.assertEqual(str1, str2)
        self.assertEqual(str2, str3)

    def test_signed_urls_expired(self):
        expire = '1000000000'
        req = Request.blank('/bucket/object?Signature=X&Expires=%s&'
                            'AWSAccessKeyId=test:tester' % expire,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Date': self.get_date_header()})
        req.headers['Date'] = datetime.utcnow()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_signed_urls(self):
        # Set expire to last 32b timestamp value
        # This number can't be higher, because it breaks tests on 32b systems
        expire = '2147483647'  # 19 Jan 2038 03:14:07
        utc_date = datetime.utcnow()
        req = Request.blank('/bucket/object?Signature=X&Expires=%s&'
                            'AWSAccessKeyId=test:tester&Timestamp=%s' %
                            (expire, utc_date.isoformat().rsplit('.')[0]),
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Date': self.get_date_header()})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        for _, path, headers in self.swift.calls_with_headers:
            self.assertNotIn('Authorization', headers)

    def test_signed_urls_no_timestamp(self):
        expire = '2147483647'  # 19 Jan 2038 03:14:07
        req = Request.blank('/bucket/object?Signature=X&Expires=%s&'
                            'AWSAccessKeyId=test:tester' % expire,
                            environ={'REQUEST_METHOD': 'GET'})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        # Curious! But actually S3 doesn't verify any x-amz-date/date headers
        # for signed_url access and it also doesn't check timestamp
        self.assertEqual(status.split()[0], '200')
        for _, _, headers in self.swift.calls_with_headers:
            self.assertNotIn('Authorization', headers)

    def test_signed_urls_invalid_expire(self):
        expire = 'invalid'
        req = Request.blank('/bucket/object?Signature=X&Expires=%s&'
                            'AWSAccessKeyId=test:tester' % expire,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Date': self.get_date_header()})
        req.headers['Date'] = datetime.utcnow()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_signed_urls_no_sign(self):
        expire = '2147483647'  # 19 Jan 2038 03:14:07
        req = Request.blank('/bucket/object?Expires=%s&'
                            'AWSAccessKeyId=test:tester' % expire,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Date': self.get_date_header()})
        req.headers['Date'] = datetime.utcnow()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_signed_urls_no_access(self):
        expire = '2147483647'  # 19 Jan 2038 03:14:07
        req = Request.blank('/bucket/object?Expires=%s&'
                            'AWSAccessKeyId=' % expire,
                            environ={'REQUEST_METHOD': 'GET'})
        req.headers['Date'] = datetime.utcnow()
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_signed_urls_v4(self):
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
            '&X-Amz-Credential=test:tester/%s/us-east-1/s3/aws4_request'
            '&X-Amz-Date=%s'
            '&X-Amz-Expires=1000'
            '&X-Amz-SignedHeaders=host'
            '&X-Amz-Signature=X' % (
                self.get_v4_amz_date_header().split('T', 1)[0],
                self.get_v4_amz_date_header()),
            headers={'Date': self.get_date_header()},
            environ={'REQUEST_METHOD': 'GET'})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)
        for _, _, headers in self.swift.calls_with_headers:
            self.assertNotIn('Authorization', headers)
            self.assertIsNone(headers['X-Auth-Token'])

    def test_signed_urls_v4_bad_credential(self):
        def test(credential, message, extra=''):
            req = Request.blank(
                '/bucket/object'
                '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
                '&X-Amz-Credential=%s'
                '&X-Amz-Date=%s'
                '&X-Amz-Expires=1000'
                '&X-Amz-SignedHeaders=host'
                '&X-Amz-Signature=X' % (
                    credential,
                    self.get_v4_amz_date_header()),
                headers={'Date': self.get_date_header()},
                environ={'REQUEST_METHOD': 'GET'})
            req.content_type = 'text/plain'
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '400', body)
            self.assertEqual(self._get_error_code(body),
                             'AuthorizationQueryParametersError')
            self.assertEqual(self._get_error_message(body), message)
            self.assertIn(extra, body)

        dt = self.get_v4_amz_date_header().split('T', 1)[0]
        test('test:tester/not-a-date/us-east-1/s3/aws4_request',
             'Invalid credential date "not-a-date". This date is not the same '
             'as X-Amz-Date: "%s".' % dt)
        test('test:tester/%s/us-west-1/s3/aws4_request' % dt,
             "Error parsing the X-Amz-Credential parameter; the region "
             "'us-west-1' is wrong; expecting 'us-east-1'",
             '<Region>us-east-1</Region>')
        test('test:tester/%s/us-east-1/not-s3/aws4_request' % dt,
             'Error parsing the X-Amz-Credential parameter; incorrect service '
             '"not-s3". This endpoint belongs to "s3".')
        test('test:tester/%s/us-east-1/s3/not-aws4_request' % dt,
             'Error parsing the X-Amz-Credential parameter; incorrect '
             'terminal "not-aws4_request". This endpoint uses "aws4_request".')

    def test_signed_urls_v4_missing_x_amz_date(self):
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
            '&X-Amz-Credential=test/20T20Z/us-east-1/s3/aws4_request'
            '&X-Amz-Expires=1000'
            '&X-Amz-SignedHeaders=host'
            '&X-Amz-Signature=X',
            environ={'REQUEST_METHOD': 'GET'})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_signed_urls_v4_invalid_algorithm(self):
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=FAKE'
            '&X-Amz-Credential=test/20T20Z/us-east-1/s3/aws4_request'
            '&X-Amz-Date=%s'
            '&X-Amz-Expires=1000'
            '&X-Amz-SignedHeaders=host'
            '&X-Amz-Signature=X' %
            self.get_v4_amz_date_header(),
            environ={'REQUEST_METHOD': 'GET'})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_signed_urls_v4_missing_signed_headers(self):
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
            '&X-Amz-Credential=test/20T20Z/us-east-1/s3/aws4_request'
            '&X-Amz-Date=%s'
            '&X-Amz-Expires=1000'
            '&X-Amz-Signature=X' %
            self.get_v4_amz_date_header(),
            environ={'REQUEST_METHOD': 'GET'})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body),
                         'AuthorizationHeaderMalformed')

    def test_signed_urls_v4_invalid_credentials(self):
        req = Request.blank('/bucket/object'
                            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
                            '&X-Amz-Credential=test'
                            '&X-Amz-Date=%s'
                            '&X-Amz-Expires=1000'
                            '&X-Amz-SignedHeaders=host'
                            '&X-Amz-Signature=X' %
                            self.get_v4_amz_date_header(),
                            environ={'REQUEST_METHOD': 'GET'})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_signed_urls_v4_missing_signature(self):
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
            '&X-Amz-Credential=test/20T20Z/us-east-1/s3/aws4_request'
            '&X-Amz-Date=%s'
            '&X-Amz-Expires=1000'
            '&X-Amz-SignedHeaders=host' %
            self.get_v4_amz_date_header(),
            environ={'REQUEST_METHOD': 'GET'})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_bucket_virtual_hosted_style(self):
        req = Request.blank('/',
                            environ={'HTTP_HOST': 'bucket.localhost:80',
                                     'REQUEST_METHOD': 'HEAD',
                                     'HTTP_AUTHORIZATION':
                                     'AWS test:tester:hmac'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_object_virtual_hosted_style(self):
        req = Request.blank('/object',
                            environ={'HTTP_HOST': 'bucket.localhost:80',
                                     'REQUEST_METHOD': 'HEAD',
                                     'HTTP_AUTHORIZATION':
                                     'AWS test:tester:hmac'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')

    def test_token_generation(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/'
                                    'object/123456789abcdef',
                            swob.HTTPOk, {}, None)
        self.swift.register('PUT', '/v1/AUTH_test/bucket+segments/'
                                   'object/123456789abcdef/1',
                            swob.HTTPCreated, {}, None)
        req = Request.blank('/bucket/object?uploadId=123456789abcdef'
                            '&partNumber=1',
                            environ={'REQUEST_METHOD': 'PUT'})
        req.headers['Authorization'] = 'AWS test:tester:hmac'
        date_header = self.get_date_header()
        req.headers['Date'] = date_header
        with mock.patch('swift.common.middleware.s3api.s3request.'
                        'S3Request.check_signature') as mock_cs:
            status, headers, body = self.call_s3api(req)
        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req.environ['s3api.auth_details'], {
            'access_key': 'test:tester',
            'signature': 'hmac',
            'string_to_sign': '\n'.join([
                'PUT', '', '', date_header,
                '/bucket/object?partNumber=1&uploadId=123456789abcdef']),
            'check_signature': mock_cs})

    def test_invalid_uri(self):
        req = Request.blank('/bucket/invalid\xffname',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidURI')

    def test_object_create_bad_md5_unreadable(self):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                                     'HTTP_CONTENT_MD5': '#'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')

    def test_object_create_bad_md5_too_short(self):
        too_short_digest = hashlib.md5('hey').hexdigest()[:-1]
        md5_str = too_short_digest.encode('base64').strip()
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                     'HTTP_CONTENT_MD5': md5_str},
            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')

    def test_object_create_bad_md5_too_long(self):
        too_long_digest = hashlib.md5('hey').hexdigest() + 'suffix'
        md5_str = too_long_digest.encode('base64').strip()
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                     'HTTP_CONTENT_MD5': md5_str},
            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')

    def test_invalid_metadata_directive(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                                     'HTTP_X_AMZ_METADATA_DIRECTIVE':
                                     'invalid'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def test_invalid_storage_class(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                                     'HTTP_X_AMZ_STORAGE_CLASS': 'INVALID'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidStorageClass')

    def test_invalid_ssc(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z'},
                            headers={'x-amz-server-side-encryption': 'invalid',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')

    def _test_unsupported_header(self, header, value=None):
        if value is None:
            value = 'value'
        req = Request.blank('/error',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z'},
                            headers={header: value,
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')

    def test_mfa(self):
        self._test_unsupported_header('x-amz-mfa')

    @mock.patch.object(utils, '_swift_admin_info', new_callable=dict)
    def test_server_side_encryption(self, mock_info):
        sse_header = 'x-amz-server-side-encryption'
        self._test_unsupported_header(sse_header, 'AES256')
        self._test_unsupported_header(sse_header, 'aws:kms')
        utils.register_swift_info('encryption', admin=True, enabled=False)
        self._test_unsupported_header(sse_header, 'AES256')
        self._test_unsupported_header(sse_header, 'aws:kms')
        utils.register_swift_info('encryption', admin=True, enabled=True)
        # AES256 now works
        self.swift.register('PUT', '/v1/AUTH_X/bucket/object',
                            swob.HTTPCreated, {}, None)
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z'},
                            headers={sse_header: 'AES256',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')
        # ...but aws:kms continues to fail
        self._test_unsupported_header(sse_header, 'aws:kms')

    def test_website_redirect_location(self):
        self._test_unsupported_header('x-amz-website-redirect-location')

    def test_aws_chunked(self):
        self._test_unsupported_header('content-encoding', 'aws-chunked')
        # https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
        # has a multi-encoding example:
        #
        # > Amazon S3 supports multiple content encodings. For example:
        # >
        # >     Content-Encoding : aws-chunked,gzip
        # > That is, you can specify your custom content-encoding when using
        # > Signature Version 4 streaming API.
        self._test_unsupported_header('Content-Encoding', 'aws-chunked,gzip')
        # Some clients skip the content-encoding,
        # such as minio-go and aws-sdk-java
        self._test_unsupported_header('x-amz-content-sha256',
                                      'STREAMING-AWS4-HMAC-SHA256-PAYLOAD')
        self._test_unsupported_header('x-amz-decoded-content-length')

    def test_object_tagging(self):
        self._test_unsupported_header('x-amz-tagging')

    def _test_unsupported_resource(self, resource):
        req = Request.blank('/error?' + resource,
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')

    def test_notification(self):
        self._test_unsupported_resource('notification')

    def test_policy(self):
        self._test_unsupported_resource('policy')

    def test_request_payment(self):
        self._test_unsupported_resource('requestPayment')

    def test_torrent(self):
        self._test_unsupported_resource('torrent')

    def test_website(self):
        self._test_unsupported_resource('website')

    def test_cors(self):
        self._test_unsupported_resource('cors')

    def test_tagging(self):
        self._test_unsupported_resource('tagging')

    def test_restore(self):
        self._test_unsupported_resource('restore')

    def test_unsupported_method(self):
        req = Request.blank('/bucket?acl',
                            environ={'REQUEST_METHOD': 'POST'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        elem = fromstring(body, 'Error')
        self.assertEqual(elem.find('./Code').text, 'MethodNotAllowed')
        self.assertEqual(elem.find('./Method').text, 'POST')
        self.assertEqual(elem.find('./ResourceType').text, 'ACL')

    def test_registered_defaults(self):
        filter_factory(self.conf)
        swift_info = utils.get_swift_info()
        self.assertTrue('s3api' in swift_info)
        self.assertEqual(swift_info['s3api'].get('max_bucket_listing'),
                         self.conf.max_bucket_listing)
        self.assertEqual(swift_info['s3api'].get('max_parts_listing'),
                         self.conf.max_parts_listing)
        self.assertEqual(swift_info['s3api'].get('max_upload_part_num'),
                         self.conf.max_upload_part_num)
        self.assertEqual(swift_info['s3api'].get('max_multi_delete_objects'),
                         self.conf.max_multi_delete_objects)

    def test_check_pipeline(self):
        with patch("swift.common.middleware.s3api.s3api.loadcontext"), \
                patch("swift.common.middleware.s3api.s3api.PipelineWrapper") \
                as pipeline:
            self.conf.auth_pipeline_check = True
            self.conf.__file__ = ''

            pipeline.return_value = 's3api tempauth proxy-server'
            self.s3api.check_pipeline(self.conf)

            # This *should* still work; authtoken will remove our auth details,
            # but the X-Auth-Token we drop in will remain
            # if we found one in the response
            pipeline.return_value = 's3api s3token authtoken keystoneauth ' \
                'proxy-server'
            self.s3api.check_pipeline(self.conf)

            # This should work now; no more doubled-up requests to keystone!
            pipeline.return_value = 's3api s3token keystoneauth proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api swauth proxy-server'
            self.s3api.check_pipeline(self.conf)

            # Note that authtoken would need to have delay_auth_decision=True
            pipeline.return_value = 's3api authtoken s3token keystoneauth ' \
                'proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api proxy-server'
            with self.assertRaises(ValueError) as cm:
                self.s3api.check_pipeline(self.conf)
            self.assertIn('expected auth between s3api and proxy-server',
                          cm.exception.message)

            pipeline.return_value = 'proxy-server'
            with self.assertRaises(ValueError) as cm:
                self.s3api.check_pipeline(self.conf)
            self.assertIn("missing filters ['s3api']",
                          cm.exception.message)

    def test_s3api_initialization_with_disabled_pipeline_check(self):
        with patch("swift.common.middleware.s3api.s3api.loadcontext"), \
                patch("swift.common.middleware.s3api.s3api.PipelineWrapper") \
                as pipeline:
            # Disable pipeline check
            self.conf.auth_pipeline_check = False
            self.conf.__file__ = ''

            pipeline.return_value = 's3api tempauth proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api s3token authtoken keystoneauth ' \
                'proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api swauth proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api authtoken s3token keystoneauth ' \
                'proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 'proxy-server'
            with self.assertRaises(ValueError):
                self.s3api.check_pipeline(self.conf)

    def test_signature_v4(self):
        environ = {
            'REQUEST_METHOD': 'GET'}
        authz_header = 'AWS4-HMAC-SHA256 ' + ', '.join([
            'Credential=test:tester/%s/us-east-1/s3/aws4_request' %
            self.get_v4_amz_date_header().split('T', 1)[0],
            'SignedHeaders=host;x-amz-date',
            'Signature=X',
        ])
        headers = {
            'Authorization': authz_header,
            'X-Amz-Date': self.get_v4_amz_date_header(),
            'X-Amz-Content-SHA256': '0123456789'}
        req = Request.blank('/bucket/object', environ=environ, headers=headers)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)
        for _, _, headers in self.swift.calls_with_headers:
            self.assertEqual(authz_header, headers['Authorization'])
            self.assertIsNone(headers['X-Auth-Token'])

    def test_signature_v4_no_date(self):
        environ = {
            'REQUEST_METHOD': 'GET'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test:tester/20130524/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;range;x-amz-date,'
                'Signature=X',
            'X-Amz-Content-SHA256': '0123456789'}
        req = Request.blank('/bucket/object', environ=environ, headers=headers)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(self._get_error_code(body), 'AccessDenied')

    def test_signature_v4_no_payload(self):
        environ = {
            'REQUEST_METHOD': 'GET'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test:tester/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-Date': self.get_v4_amz_date_header()}
        req = Request.blank('/bucket/object', environ=environ, headers=headers)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '400')
        self.assertEqual(self._get_error_code(body), 'InvalidRequest')
        self.assertEqual(
            self._get_error_message(body),
            'Missing required header for this request: x-amz-content-sha256')

    def test_signature_v4_bad_authorization_string(self):
        def test(auth_str, error, msg, extra=''):
            environ = {
                'REQUEST_METHOD': 'GET'}
            headers = {
                'Authorization': auth_str,
                'X-Amz-Date': self.get_v4_amz_date_header(),
                'X-Amz-Content-SHA256': '0123456789'}
            req = Request.blank('/bucket/object', environ=environ,
                                headers=headers)
            req.content_type = 'text/plain'
            status, headers, body = self.call_s3api(req)
            self.assertEqual(self._get_error_code(body), error)
            self.assertEqual(self._get_error_message(body), msg)
            self.assertIn(extra, body)

        auth_str = ('AWS4-HMAC-SHA256 '
                    'SignedHeaders=host;x-amz-date,'
                    'Signature=X')
        test(auth_str, 'AccessDenied', 'Access Denied.')

        auth_str = (
            'AWS4-HMAC-SHA256 '
            'Credential=test:tester/20130524/us-east-1/s3/aws4_request, '
            'Signature=X')
        test(auth_str, 'AuthorizationHeaderMalformed',
             'The authorization header is malformed; the authorization '
             'header requires three components: Credential, SignedHeaders, '
             'and Signature.')

        auth_str = ('AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-west-2/s3/aws4_request, '
                    'Signature=X, SignedHeaders=host;x-amz-date' %
                    self.get_v4_amz_date_header().split('T', 1)[0])
        test(auth_str, 'AuthorizationHeaderMalformed',
             "The authorization header is malformed; "
             "the region 'us-west-2' is wrong; expecting 'us-east-1'",
             '<Region>us-east-1</Region>')

        auth_str = ('AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-east-1/not-s3/aws4_request, '
                    'Signature=X, SignedHeaders=host;x-amz-date' %
                    self.get_v4_amz_date_header().split('T', 1)[0])
        test(auth_str, 'AuthorizationHeaderMalformed',
             'The authorization header is malformed; '
             'incorrect service "not-s3". This endpoint belongs to "s3".')

        auth_str = ('AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-east-1/s3/not-aws4_request, '
                    'Signature=X, SignedHeaders=host;x-amz-date' %
                    self.get_v4_amz_date_header().split('T', 1)[0])
        test(auth_str, 'AuthorizationHeaderMalformed',
             'The authorization header is malformed; '
             'incorrect terminal "not-aws4_request". '
             'This endpoint uses "aws4_request".')

        auth_str = (
            'AWS4-HMAC-SHA256 '
            'Credential=test:tester/20130524/us-east-1/s3/aws4_request, '
            'SignedHeaders=host;x-amz-date')
        test(auth_str, 'AccessDenied', 'Access Denied.')

    def test_canonical_string_v4(self):
        def _get_req(path, environ):
            if '?' in path:
                path, query_string = path.split('?', 1)
            else:
                query_string = ''

            env = {
                'REQUEST_METHOD': 'GET',
                'PATH_INFO': path,
                'QUERY_STRING': query_string,
                'HTTP_DATE': 'Mon, 09 Sep 2011 23:36:00 GMT',
                'HTTP_X_AMZ_CONTENT_SHA256':
                    'e3b0c44298fc1c149afbf4c8996fb924'
                    '27ae41e4649b934ca495991b7852b855',
                'HTTP_AUTHORIZATION':
                    'AWS4-HMAC-SHA256 '
                    'Credential=X:Y/20110909/us-east-1/s3/aws4_request, '
                    'SignedHeaders=content-md5;content-type;date, '
                    'Signature=x',
            }
            fake_time = calendar.timegm((2011, 9, 9, 23, 36, 0))
            env.update(environ)
            with patch('swift.common.middleware.s3api.s3request.'
                       'S3Request._validate_headers'), \
                    patch('swift.common.middleware.s3api.utils.time.time',
                          return_value=fake_time):
                req = SigV4Request(env, location=self.conf.location)
            return req

        def canonical_string(path, environ):
            return _get_req(path, environ)._canonical_request()

        def verify(hash_val, path, environ):
            # See http://docs.aws.amazon.com/general/latest/gr
            # /signature-v4-test-suite.html for where location, service, and
            # signing key came from
            with patch.object(self.conf, 'location', 'us-east-1'), \
                    patch.object(swift.common.middleware.s3api.s3request,
                                 'SERVICE', 'host'):
                req = _get_req(path, environ)
                hash_in_sts = req._string_to_sign().split('\n')[3]
                self.assertEqual(hash_val, hash_in_sts)
                self.assertTrue(req.check_signature(
                    'wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY'))

        # all next data got from aws4_testsuite from Amazon
        # http://docs.aws.amazon.com/general/latest/gr/samples
        # /aws4_testsuite.zip
        # Each *expected* hash value is the 4th line in <test-name>.sts in the
        # test suite.

        # get-vanilla
        env = {
            'HTTP_AUTHORIZATION': (
                'AWS4-HMAC-SHA256 '
                'Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, '
                'SignedHeaders=date;host, '
                'Signature=b27ccfbfa7df52a200ff74193ca6e32d'
                '4b48b8856fab7ebf1c595d0670a7e470'),
            'HTTP_HOST': 'host.foo.com'}
        verify('366b91fb121d72a00f46bbe8d395f53a'
               '102b06dfb7e79636515208ed3fa606b1',
               '/', env)

        # get-header-value-trim
        env = {
            'REQUEST_METHOD': 'POST',
            'HTTP_AUTHORIZATION': (
                'AWS4-HMAC-SHA256 '
                'Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, '
                'SignedHeaders=date;host;p, '
                'Signature=debf546796015d6f6ded8626f5ce9859'
                '7c33b47b9164cf6b17b4642036fcb592'),
            'HTTP_HOST': 'host.foo.com',
            'HTTP_P': 'phfft'}
        verify('dddd1902add08da1ac94782b05f9278c'
               '08dc7468db178a84f8950d93b30b1f35',
               '/', env)

        # get-utf8 (not exact)
        env = {
            'HTTP_AUTHORIZATION': (
                'AWS4-HMAC-SHA256 '
                'Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, '
                'SignedHeaders=date;host, '
                'Signature=8d6634c189aa8c75c2e51e106b6b5121'
                'bed103fdb351f7d7d4381c738823af74'),
            'HTTP_HOST': 'host.foo.com',
            'RAW_PATH_INFO': '/%E1%88%B4'}

        # This might look weird because actually S3 doesn't care about utf-8
        # encoded multi-byte bucket name from bucket-in-host name constraint.
        # However, aws4_testsuite has only a sample hash with utf-8 *bucket*
        # name to make sure the correctness (probably it can be used in other
        # aws resource except s3) so, to test also utf-8, skip the bucket name
        # validation in the following test.

        # NOTE: eventlet's PATH_INFO is unquoted
        with patch('swift.common.middleware.s3api.s3request.'
                   'validate_bucket_name'):
            verify('27ba31df5dbc6e063d8f87d62eb07143'
                   'f7f271c5330a917840586ac1c85b6f6b',
                   unquote('/%E1%88%B4'), env)

        # get-vanilla-query-order-key
        env = {
            'HTTP_AUTHORIZATION': (
                'AWS4-HMAC-SHA256 '
                'Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, '
                'SignedHeaders=date;host, '
                'Signature=0dc122f3b28b831ab48ba65cb47300de'
                '53fbe91b577fe113edac383730254a3b'),
            'HTTP_HOST': 'host.foo.com'}
        verify('2f23d14fe13caebf6dfda346285c6d9c'
               '14f49eaca8f5ec55c627dd7404f7a727',
               '/?a=foo&b=foo', env)

        # post-header-value-case
        env = {
            'REQUEST_METHOD': 'POST',
            'HTTP_AUTHORIZATION': (
                'AWS4-HMAC-SHA256 '
                'Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, '
                'SignedHeaders=date;host;zoo, '
                'Signature=273313af9d0c265c531e11db70bbd653'
                'f3ba074c1009239e8559d3987039cad7'),
            'HTTP_HOST': 'host.foo.com',
            'HTTP_ZOO': 'ZOOBAR'}
        verify('3aae6d8274b8c03e2cc96fc7d6bda4b9'
               'bd7a0a184309344470b2c96953e124aa',
               '/', env)

        # post-x-www-form-urlencoded-parameters
        env = {
            'REQUEST_METHOD': 'POST',
            'HTTP_AUTHORIZATION': (
                'AWS4-HMAC-SHA256 '
                'Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, '
                'SignedHeaders=date;host;content-type, '
                'Signature=b105eb10c6d318d2294de9d49dd8b031'
                'b55e3c3fe139f2e637da70511e9e7b71'),
            'HTTP_HOST': 'host.foo.com',
            'HTTP_X_AMZ_CONTENT_SHA256':
                '3ba8907e7a252327488df390ed517c45'
                'b96dead033600219bdca7107d1d3f88a',
            'CONTENT_TYPE':
                'application/x-www-form-urlencoded; charset=utf8'}
        verify('c4115f9e54b5cecf192b1eaa23b8e88e'
               'd8dc5391bd4fde7b3fff3d9c9fe0af1f',
               '/', env)

        # post-x-www-form-urlencoded
        env = {
            'REQUEST_METHOD': 'POST',
            'HTTP_AUTHORIZATION': (
                'AWS4-HMAC-SHA256 '
                'Credential=AKIDEXAMPLE/20110909/us-east-1/host/aws4_request, '
                'SignedHeaders=date;host;content-type, '
                'Signature=5a15b22cf462f047318703b92e6f4f38'
                '884e4a7ab7b1d6426ca46a8bd1c26cbc'),
            'HTTP_HOST': 'host.foo.com',
            'HTTP_X_AMZ_CONTENT_SHA256':
                '3ba8907e7a252327488df390ed517c45'
                'b96dead033600219bdca7107d1d3f88a',
            'CONTENT_TYPE':
                'application/x-www-form-urlencoded'}
        verify('4c5c6e4b52fb5fb947a8733982a8a5a6'
               '1b14f04345cbfe6e739236c76dd48f74',
               '/', env)

        # Note that boto does not do proper stripping (as of 2.42.0).
        # These were determined by examining the StringToSignBytes element of
        # resulting SignatureDoesNotMatch errors from AWS.
        str1 = canonical_string('/', {'CONTENT_TYPE': 'text/plain',
                                      'HTTP_CONTENT_MD5': '##'})
        str2 = canonical_string('/', {'CONTENT_TYPE': '\x01\x02text/plain',
                                      'HTTP_CONTENT_MD5': '\x1f ##'})
        str3 = canonical_string('/', {'CONTENT_TYPE': 'text/plain \x10',
                                      'HTTP_CONTENT_MD5': '##\x18'})

        self.assertEqual(str1, str2)
        self.assertEqual(str2, str3)

    def test_mixture_param_v4(self):
        # now we have an Authorization header
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20130524/us-east-1/s3/aws4_request_A, '
                'SignedHeaders=hostA;rangeA;x-amz-dateA,'
                'Signature=X',
            'X-Amz-Date': self.get_v4_amz_date_header(),
            'X-Amz-Content-SHA256': '0123456789'}

        # and then, different auth info (Credential, SignedHeaders, Signature)
        # in query
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
            '&X-Amz-Credential=test/20T20Z/us-east-1/s3/aws4_requestB'
            '&X-Amz-SignedHeaders=hostB'
            '&X-Amz-Signature=Y',
            environ={'REQUEST_METHOD': 'GET'},
            headers=headers)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        # FIXME: should this failed as 400 or pass via query auth?
        # for now, 403 forbidden for safety
        self.assertEqual(status.split()[0], '403', body)

        # But if we are missing Signature in query param
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
            '&X-Amz-Credential=test/20T20Z/us-east-1/s3/aws4_requestB'
            '&X-Amz-SignedHeaders=hostB',
            environ={'REQUEST_METHOD': 'GET'},
            headers=headers)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403', body)

    def test_s3api_with_only_s3_token(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.s3_token = S3Token(
            self.keystone_auth, {'auth_uri': 'https://fakehost/identity'})
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        req = Request.blank(
            '/bucket',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS access:signature',
                     'Date': self.get_date_header()})
        self.swift.register('PUT', '/v1/AUTH_TENANT_ID/bucket',
                            swob.HTTPCreated, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_TENANT_ID',
                            swob.HTTPOk, {}, None)
        with patch.object(self.s3_token, '_json_request') as mock_req:
            mock_resp = requests.Response()
            mock_resp._content = json.dumps(GOOD_RESPONSE_V2)
            mock_resp.status_code = 201
            mock_req.return_value = mock_resp

            status, headers, body = self.call_s3api(req)
            self.assertEqual(body, '')
            self.assertEqual(1, mock_req.call_count)

    def test_s3api_with_only_s3_token_v3(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.s3_token = S3Token(
            self.keystone_auth, {'auth_uri': 'https://fakehost/identity'})
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        req = Request.blank(
            '/bucket',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS access:signature',
                     'Date': self.get_date_header()})
        self.swift.register('PUT', '/v1/AUTH_PROJECT_ID/bucket',
                            swob.HTTPCreated, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_PROJECT_ID',
                            swob.HTTPOk, {}, None)
        with patch.object(self.s3_token, '_json_request') as mock_req:
            mock_resp = requests.Response()
            mock_resp._content = json.dumps(GOOD_RESPONSE_V3)
            mock_resp.status_code = 200
            mock_req.return_value = mock_resp

            status, headers, body = self.call_s3api(req)
            self.assertEqual(body, '')
            self.assertEqual(1, mock_req.call_count)

    def test_s3api_with_s3_token_and_auth_token(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.auth_token = AuthProtocol(
            self.keystone_auth, {'delay_auth_decision': 'True'})
        self.s3_token = S3Token(
            self.auth_token, {'auth_uri': 'https://fakehost/identity'})
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        req = Request.blank(
            '/bucket',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS access:signature',
                     'Date': self.get_date_header()})
        self.swift.register('PUT', '/v1/AUTH_TENANT_ID/bucket',
                            swob.HTTPCreated, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_TENANT_ID',
                            swob.HTTPOk, {}, None)
        with patch.object(self.s3_token, '_json_request') as mock_req:
            with patch.object(self.auth_token,
                              '_do_fetch_token') as mock_fetch:
                mock_resp = requests.Response()
                mock_resp._content = json.dumps(GOOD_RESPONSE_V2)
                mock_resp.status_code = 201
                mock_req.return_value = mock_resp

                mock_access_info = AccessInfoV2(GOOD_RESPONSE_V2)
                mock_access_info.will_expire_soon = \
                    lambda stale_duration: False
                mock_fetch.return_value = (MagicMock(), mock_access_info)

                status, headers, body = self.call_s3api(req)
                self.assertEqual(body, '')
                self.assertEqual(1, mock_req.call_count)
                # With X-Auth-Token, auth_token will call _do_fetch_token to
                # connect to keystone in auth_token, again
                self.assertEqual(1, mock_fetch.call_count)

    def test_s3api_with_s3_token_no_pass_token_to_auth_token(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.auth_token = AuthProtocol(
            self.keystone_auth, {'delay_auth_decision': 'True'})
        self.s3_token = S3Token(
            self.auth_token, {'auth_uri': 'https://fakehost/identity'})
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        req = Request.blank(
            '/bucket',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS access:signature',
                     'Date': self.get_date_header()})
        self.swift.register('PUT', '/v1/AUTH_TENANT_ID/bucket',
                            swob.HTTPCreated, {}, None)
        self.swift.register('HEAD', '/v1/AUTH_TENANT_ID',
                            swob.HTTPOk, {}, None)
        with patch.object(self.s3_token, '_json_request') as mock_req:
            with patch.object(self.auth_token,
                              '_do_fetch_token') as mock_fetch:
                mock_resp = requests.Response()
                no_token_id_good_resp = copy.deepcopy(GOOD_RESPONSE_V2)
                # delete token id
                del no_token_id_good_resp['access']['token']['id']
                mock_resp._content = json.dumps(no_token_id_good_resp)
                mock_resp.status_code = 201
                mock_req.return_value = mock_resp

                mock_access_info = AccessInfoV2(GOOD_RESPONSE_V2)
                mock_access_info.will_expire_soon = \
                    lambda stale_duration: False
                mock_fetch.return_value = (MagicMock(), mock_access_info)

                status, headers, body = self.call_s3api(req)
                # No token provided from keystone result in 401 Unauthorized
                # at `swift.common.middleware.keystoneauth` because auth_token
                # will remove all auth headers including 'X-Identity-Status'[1]
                # and then, set X-Identity-Status: Invalid at [2]
                #
                # 1: https://github.com/openstack/keystonemiddleware/blob/
                #    master/keystonemiddleware/auth_token/__init__.py#L620
                # 2: https://github.com/openstack/keystonemiddleware/blob/
                #    master/keystonemiddleware/auth_token/__init__.py#L627-L629

                self.assertEqual('403 Forbidden', status)
                self.assertEqual(1, mock_req.call_count)
                # if no token provided from keystone, we can skip the call to
                # fetch the token
                self.assertEqual(0, mock_fetch.call_count)

if __name__ == '__main__':
    unittest.main()
