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
import io
from datetime import timedelta
import hashlib
from unittest.mock import patch, MagicMock
import unittest
import unittest.mock as mock

from io import BytesIO

from swift.common import swob
from swift.common.middleware.s3api import s3request, s3response, controllers
from swift.common.middleware.s3api.exception import S3InputChecksumMismatch
from swift.common.swob import Request, HTTPNoContent
from swift.common.middleware.s3api.utils import mktime, Config
from swift.common.middleware.s3api.acl_handlers import get_acl_handler
from swift.common.middleware.s3api.subresource import ACL, User, Owner, \
    Grant, encode_acl
from swift.common.middleware.s3api.s3request import S3Request, \
    S3AclRequest, SigV4Request, SIGV4_X_AMZ_DATE_FORMAT, HashingInput, \
    ChunkReader, StreamingInput, S3InputSHA256Mismatch, \
    S3InputChunkSignatureMismatch, _get_checksum_hasher
from swift.common.middleware.s3api.s3response import InvalidArgument, \
    NoSuchBucket, InternalError, ServiceUnavailable, \
    AccessDenied, SignatureDoesNotMatch, RequestTimeTooSkewed, \
    InvalidPartArgument, InvalidPartNumber, InvalidRequest, \
    XAmzContentSHA256Mismatch, S3NotImplemented
from swift.common.utils import checksum
from test.debug_logger import debug_logger
from test.unit import requires_crc32c, requires_crc64nvme
from test.unit.common.middleware.s3api.test_s3api import S3ApiTestCase

Fake_ACL_MAP = {
    # HEAD Bucket
    ('HEAD', 'HEAD', 'container'):
    {'Resource': 'container',
     'Permission': 'READ'},
    # GET Bucket
    ('GET', 'GET', 'container'):
    {'Resource': 'container',
     'Permission': 'READ'},
    # HEAD Object
    ('HEAD', 'HEAD', 'object'):
    {'Resource': 'object',
     'Permission': 'READ'},
    # GET Object
    ('GET', 'GET', 'object'):
    {'Resource': 'object',
     'Permission': 'READ'},
}


def _gen_test_acl_header(owner, permission=None, grantee=None,
                         resource='container'):
    if permission is None:
        return ACL(owner, [])

    if grantee is None:
        grantee = User('test:tester')
    return encode_acl(resource, ACL(owner, [Grant(grantee, permission)]))


class FakeResponse(object):
    def __init__(self, s3_acl):
        self.sysmeta_headers = {}
        if s3_acl:
            owner = Owner(id='test:tester', name='test:tester')
            self.sysmeta_headers.update(
                _gen_test_acl_header(owner, 'FULL_CONTROL',
                                     resource='container'))
            self.sysmeta_headers.update(
                _gen_test_acl_header(owner, 'FULL_CONTROL',
                                     resource='object'))


class FakeSwiftResponse(object):
    def __init__(self):
        self.environ = {
            'PATH_INFO': '/v1/AUTH_test',
            'HTTP_X_TENANT_NAME': 'test',
            'HTTP_X_USER_NAME': 'tester',
            'HTTP_X_AUTH_TOKEN': 'token',
        }


class TestRequest(S3ApiTestCase):

    def setUp(self):
        super(TestRequest, self).setUp()
        self.s3api.conf.s3_acl = True
        s3request.SIGV4_CHUNK_MIN_SIZE = 2

    @patch('swift.common.middleware.s3api.acl_handlers.ACL_MAP', Fake_ACL_MAP)
    @patch('swift.common.middleware.s3api.s3request.S3AclRequest.authenticate',
           lambda x, y: None)
    def _test_get_response(self, method, container='bucket', obj=None,
                           permission=None, skip_check=False,
                           req_klass=S3Request, fake_swift_resp=None):
        path = '/' + container + ('/' + obj if obj else '')
        req = Request.blank(path,
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        s3_req = req_klass(req.environ, conf=self.s3api.conf)
        s3_req.set_acl_handler(
            get_acl_handler(s3_req.controller_name)(s3_req, debug_logger()))
        with patch('swift.common.middleware.s3api.s3request.S3Request.'
                   '_get_response') as mock_get_resp, \
                patch('swift.common.middleware.s3api.subresource.ACL.'
                      'check_permission') as m_check_permission:
            mock_get_resp.return_value = fake_swift_resp \
                or FakeResponse(self.s3api.conf.s3_acl)
            return mock_get_resp, m_check_permission, \
                s3_req.get_response(self.s3api)

    def test_get_response_without_s3_acl(self):
        self.s3api.conf.s3_acl = False
        mock_get_resp, m_check_permission, s3_resp = \
            self._test_get_response('HEAD')
        self.assertFalse(hasattr(s3_resp, 'bucket_acl'))
        self.assertFalse(hasattr(s3_resp, 'object_acl'))
        self.assertEqual(mock_get_resp.call_count, 1)
        self.assertEqual(m_check_permission.call_count, 0)

    def test_get_response_without_match_ACL_MAP(self):
        with self.assertRaises(Exception) as e:
            self._test_get_response('POST', req_klass=S3AclRequest)
        self.assertEqual(e.exception.args[0],
                         'No permission to be checked exists')

    def test_get_response_without_duplication_HEAD_request(self):
        obj = 'object'
        mock_get_resp, m_check_permission, s3_resp = \
            self._test_get_response('HEAD', obj=obj,
                                    req_klass=S3AclRequest)
        self.assertTrue(s3_resp.bucket_acl is not None)
        self.assertTrue(s3_resp.object_acl is not None)
        self.assertEqual(mock_get_resp.call_count, 1)
        args, kargs = mock_get_resp.call_args_list[0]
        get_resp_obj = args[3]
        self.assertEqual(get_resp_obj, obj)
        self.assertEqual(m_check_permission.call_count, 1)
        args, kargs = m_check_permission.call_args
        permission = args[1]
        self.assertEqual(permission, 'READ')

    def test_get_response_with_check_object_permission(self):
        obj = 'object'
        mock_get_resp, m_check_permission, s3_resp = \
            self._test_get_response('GET', obj=obj,
                                    req_klass=S3AclRequest)
        self.assertTrue(s3_resp.bucket_acl is not None)
        self.assertTrue(s3_resp.object_acl is not None)
        self.assertEqual(mock_get_resp.call_count, 2)
        args, kargs = mock_get_resp.call_args_list[0]
        get_resp_obj = args[3]
        self.assertEqual(get_resp_obj, obj)
        self.assertEqual(m_check_permission.call_count, 1)
        args, kargs = m_check_permission.call_args
        permission = args[1]
        self.assertEqual(permission, 'READ')

    def test_get_response_with_check_container_permission(self):
        mock_get_resp, m_check_permission, s3_resp = \
            self._test_get_response('GET',
                                    req_klass=S3AclRequest)
        self.assertTrue(s3_resp.bucket_acl is not None)
        self.assertTrue(s3_resp.object_acl is not None)
        self.assertEqual(mock_get_resp.call_count, 2)
        args, kargs = mock_get_resp.call_args_list[0]
        get_resp_obj = args[3]
        self.assertEqual(get_resp_obj, '')
        self.assertEqual(m_check_permission.call_count, 1)
        args, kargs = m_check_permission.call_args
        permission = args[1]
        self.assertEqual(permission, 'READ')

    def test_get_validate_param(self):
        def create_s3request_with_param(param, value):
            req = Request.blank(
                '/bucket?%s=%s' % (param, value),
                environ={'REQUEST_METHOD': 'GET'},
                headers={'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()})
            return S3Request(req.environ)

        s3req = create_s3request_with_param('max-keys', '1')

        # a param in the range
        self.assertEqual(s3req.get_validated_param('max-keys', 1000, 1000), 1)
        self.assertEqual(s3req.get_validated_param('max-keys', 0, 1), 1)

        # a param in the out of the range
        self.assertEqual(s3req.get_validated_param('max-keys', 0, 0), 0)

        # a param in the out of the integer range
        s3req = create_s3request_with_param('max-keys', '1' * 30)
        with self.assertRaises(InvalidArgument) as result:
            s3req.get_validated_param('max-keys', 1)
        self.assertIn(
            b'not an integer or within integer range', result.exception.body)
        self.assertEqual(
            result.exception.headers['content-type'], 'application/xml')

        # a param is negative integer
        s3req = create_s3request_with_param('max-keys', '-1')
        with self.assertRaises(InvalidArgument) as result:
            s3req.get_validated_param('max-keys', 1)
        self.assertIn(
            b'must be an integer between 0 and', result.exception.body)
        self.assertEqual(
            result.exception.headers['content-type'], 'application/xml')

        # a param is not integer
        s3req = create_s3request_with_param('max-keys', 'invalid')
        with self.assertRaises(InvalidArgument) as result:
            s3req.get_validated_param('max-keys', 1)
        self.assertIn(
            b'not an integer or within integer range', result.exception.body)
        self.assertEqual(
            result.exception.headers['content-type'], 'application/xml')

    def test_authenticate_delete_Authorization_from_s3req(self):
        req = Request.blank('/bucket/obj',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch.object(Request, 'get_response') as m_swift_resp, \
                patch.object(Request, 'remote_user', 'authorized'):

            m_swift_resp.return_value = FakeSwiftResponse()
            s3_req = S3AclRequest(req.environ, self.s3api.conf, None)
            self.assertNotIn('s3api.auth_details', s3_req.environ)

    def test_to_swift_req_Authorization_not_exist_in_swreq(self):
        # the difference from
        # test_authenticate_delete_Authorization_from_s3req_headers above is
        # this method asserts *to_swift_req* method.
        container = 'bucket'
        obj = 'obj'
        method = 'GET'
        req = Request.blank('/%s/%s' % (container, obj),
                            environ={'REQUEST_METHOD': method},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch.object(Request, 'get_response') as m_swift_resp, \
                patch.object(Request, 'remote_user', 'authorized'):

            m_swift_resp.return_value = FakeSwiftResponse()
            s3_req = S3AclRequest(req.environ)
            # Yes, we *want* to assert this
            sw_req = s3_req.to_swift_req(method, container, obj)
            # So since the result of S3AclRequest init tests and with this
            # result to_swift_req doesn't add Authorization header and token
            self.assertNotIn('s3api.auth_details', sw_req.environ)
            self.assertNotIn('X-Auth-Token', sw_req.headers)

    def test_to_swift_req_subrequest_proxy_access_log(self):
        container = 'bucket'
        obj = 'obj'
        method = 'GET'

        # force_swift_request_proxy_log is True
        req = Request.blank('/%s/%s' % (container, obj),
                            environ={'REQUEST_METHOD': method,
                                     'swift.proxy_access_log_made': True},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch.object(Request, 'get_response') as m_swift_resp, \
                patch.object(Request, 'remote_user', 'authorized'):
            m_swift_resp.return_value = FakeSwiftResponse()
            s3_req = S3AclRequest(
                req.environ,
                conf=Config({'force_swift_request_proxy_log': True}))
            sw_req = s3_req.to_swift_req(method, container, obj)
            self.assertFalse(sw_req.environ['swift.proxy_access_log_made'])

        # force_swift_request_proxy_log is False
        req = Request.blank('/%s/%s' % (container, obj),
                            environ={'REQUEST_METHOD': method,
                                     'swift.proxy_access_log_made': True},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        with patch.object(Request, 'get_response') as m_swift_resp, \
                patch.object(Request, 'remote_user', 'authorized'):
            m_swift_resp.return_value = FakeSwiftResponse()
            s3_req = S3AclRequest(
                req.environ,
                conf=Config({'force_swift_request_proxy_log': False}))
            sw_req = s3_req.to_swift_req(method, container, obj)
            self.assertTrue(sw_req.environ['swift.proxy_access_log_made'])

    def test_get_container_info(self):
        s3api_acl = '{"Owner":"owner","Grant":'\
            '[{"Grantee":"owner","Permission":"FULL_CONTROL"}]}'
        self.swift.register('HEAD', '/v1/AUTH_test/bucket', HTTPNoContent,
                            {'x-container-read': 'foo',
                             'X-container-object-count': '5',
                             'x-container-sysmeta-versions-location':
                                'bucket2',
                             'x-container-sysmeta-s3api-acl': s3api_acl,
                             'X-container-meta-foo': 'bar'}, None)
        req = Request.blank('/bucket', environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        s3_req = S3Request(req.environ)
        # first, call get_response('HEAD')
        info = s3_req.get_container_info(self.app)
        self.assertTrue('status' in info)  # sanity
        self.assertEqual(204, info['status'])  # sanity
        self.assertEqual('foo', info['read_acl'])  # sanity
        self.assertEqual(5, info['object_count'])  # sanity
        self.assertEqual(
            'bucket2', info['sysmeta']['versions-location'])  # sanity
        self.assertEqual(s3api_acl, info['sysmeta']['s3api-acl'])  # sanity
        self.assertEqual({'foo': 'bar'}, info['meta'])  # sanity
        with patch(
                'swift.common.middleware.s3api.s3request.get_container_info',
                return_value={'status': 204}) as mock_info:
            # Then all calls goes to get_container_info
            for x in range(10):
                info = s3_req.get_container_info(self.swift)
                self.assertTrue('status' in info)  # sanity
                self.assertEqual(204, info['status'])  # sanity
            self.assertEqual(10, mock_info.call_count)

        expected_errors = [(404, NoSuchBucket), (0, InternalError),
                           (503, ServiceUnavailable)]
        for status, expected_error in expected_errors:
            with patch('swift.common.middleware.s3api.s3request.'
                       'get_container_info',
                       return_value={'status': status}):
                self.assertRaises(
                    expected_error, s3_req.get_container_info, MagicMock())

    def test_date_header_missing(self):
        self.swift.register('HEAD', '/v1/AUTH_test/nojunk', swob.HTTPNotFound,
                            {}, None)
        req = Request.blank('/nojunk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac'})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, b'')

    def test_date_header_expired(self):
        self.swift.register('HEAD', '/v1/AUTH_test/nojunk', swob.HTTPNotFound,
                            {}, None)
        req = Request.blank('/nojunk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': 'Fri, 01 Apr 2014 12:00:00 GMT'})

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, b'')

    def test_date_header_with_x_amz_date_valid(self):
        self.swift.register('HEAD', '/v1/AUTH_test/nojunk', swob.HTTPNotFound,
                            {}, None)
        req = Request.blank('/nojunk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': 'Fri, 01 Apr 2014 12:00:00 GMT',
                                     'x-amz-date': self.get_date_header()})

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '404')
        self.assertEqual(body, b'')

    def test_date_header_with_x_amz_date_expired(self):
        self.swift.register('HEAD', '/v1/AUTH_test/nojunk', swob.HTTPNotFound,
                            {}, None)
        req = Request.blank('/nojunk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header(),
                                     'x-amz-date':
                                     'Fri, 01 Apr 2014 12:00:00 GMT'})

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, b'')

    def _test_request_timestamp_sigv4(self, date_header):
        # signature v4 here
        environ = {
            'REQUEST_METHOD': 'GET'}

        if 'X-Amz-Date' in date_header:
            included_header = 'x-amz-date'
            scope_date = date_header['X-Amz-Date'].split('T', 1)[0]
        elif 'Date' in date_header:
            included_header = 'date'
            scope_date = self.get_v4_amz_date_header().split('T', 1)[0]
        else:
            self.fail('Invalid date header specified as test')

        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=%s,'
                'Signature=X' % (
                    scope_date,
                    ';'.join(sorted(['host', included_header]))),
            'X-Amz-Content-SHA256': '0' * 64}

        headers.update(date_header)
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ, conf=self.s3api.conf)

        if 'X-Amz-Date' in date_header:
            timestamp = mktime(
                date_header['X-Amz-Date'], SIGV4_X_AMZ_DATE_FORMAT)
        elif 'Date' in date_header:
            timestamp = mktime(date_header['Date'])

        self.assertEqual(timestamp, int(sigv4_req.timestamp))

    def test_request_timestamp_sigv4(self):
        access_denied_message = \
            b'AWS authentication requires a valid Date or x-amz-date header'

        # normal X-Amz-Date header
        date_header = {'X-Amz-Date': self.get_v4_amz_date_header()}
        self._test_request_timestamp_sigv4(date_header)

        # normal Date header
        date_header = {'Date': self.get_date_header()}
        self._test_request_timestamp_sigv4(date_header)

        # mangled X-Amz-Date header
        date_header = {'X-Amz-Date': self.get_v4_amz_date_header()[:-1]}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # mangled Date header
        date_header = {'Date': self.get_date_header()[20:]}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # Negative timestamp
        date_header = {'X-Amz-Date': '00160523T054055Z'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # far-past Date header
        date_header = {'Date': 'Tue, 07 Jul 999 21:53:04 GMT'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # near-past X-Amz-Date headers
        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            timedelta(minutes=-10)
        )}
        self._test_request_timestamp_sigv4(date_header)

        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            timedelta(minutes=-10)
        )}
        with self.assertRaises(RequestTimeTooSkewed) as cm, \
                patch.object(self.s3api.conf, 'allowable_clock_skew', 300):
            self._test_request_timestamp_sigv4(date_header)

        # near-future X-Amz-Date headers
        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            timedelta(minutes=10)
        )}
        self._test_request_timestamp_sigv4(date_header)

        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            timedelta(minutes=10)
        )}
        with self.assertRaises(RequestTimeTooSkewed) as cm, \
                patch.object(self.s3api.conf, 'allowable_clock_skew', 300):
            self._test_request_timestamp_sigv4(date_header)

        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            timedelta(days=1)
        )}
        with self.assertRaises(RequestTimeTooSkewed) as cm:
            self._test_request_timestamp_sigv4(date_header)

        # far-future Date header
        date_header = {'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'}
        with self.assertRaises(RequestTimeTooSkewed) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(b'The difference between the request time and the '
                      b'current time is too large.', cm.exception.body)

    def _test_request_timestamp_sigv2(self, date_header):
        # signature v4 here
        environ = {
            'REQUEST_METHOD': 'GET'}

        headers = {'Authorization': 'AWS test:tester:hmac'}
        headers.update(date_header)
        req = Request.blank('/', environ=environ, headers=headers)
        sigv2_req = S3Request(req.environ)

        if 'X-Amz-Date' in date_header:
            timestamp = mktime(req.headers.get('X-Amz-Date'))
        elif 'Date' in date_header:
            timestamp = mktime(req.headers.get('Date'))
        else:
            self.fail('Invalid date header specified as test')
        self.assertEqual(timestamp, int(sigv2_req.timestamp))

    def test_request_timestamp_sigv2(self):
        access_denied_message = \
            b'AWS authentication requires a valid Date or x-amz-date header'

        # In v2 format, normal X-Amz-Date header is same
        date_header = {'X-Amz-Date': self.get_date_header()}
        self._test_request_timestamp_sigv2(date_header)

        # normal Date header
        date_header = {'Date': self.get_date_header()}
        self._test_request_timestamp_sigv2(date_header)

        # mangled X-Amz-Date header
        date_header = {'X-Amz-Date': self.get_date_header()[:-20]}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # mangled Date header
        date_header = {'Date': self.get_date_header()[:-20]}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # Negative timestamp
        date_header = {'X-Amz-Date': '00160523T054055Z'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # far-past Date header
        date_header = {'Date': 'Tue, 07 Jul 999 21:53:04 GMT'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(access_denied_message, cm.exception.body)

        # far-future Date header
        date_header = {'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'}
        with self.assertRaises(RequestTimeTooSkewed) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.args[0])
        self.assertIn(b'The difference between the request time and the '
                      b'current time is too large.', cm.exception.body)

    def test_headers_to_sign_sigv4(self):
        environ = {
            'REQUEST_METHOD': 'GET'}

        # host and x-amz-date
        x_amz_date = self.get_v4_amz_date_header()
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-Content-SHA256': '0' * 64,
            'Date': self.get_date_header(),
            'X-Amz-Date': x_amz_date}

        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)

        headers_to_sign = sigv4_req._headers_to_sign()
        self.assertEqual(headers_to_sign, [
            ('host', 'localhost:80'),
            ('x-amz-content-sha256', '0' * 64),
            ('x-amz-date', x_amz_date)])

        # no x-amz-date
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-Content-SHA256': '1' * 64,
            'Date': self.get_date_header()}

        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)

        headers_to_sign = sigv4_req._headers_to_sign()
        self.assertEqual(headers_to_sign, [
            ('host', 'localhost:80'),
            ('x-amz-content-sha256', '1' * 64)])

        # SignedHeaders says, host and x-amz-date included but there is not
        # X-Amz-Date header
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-Content-SHA256': '2' * 64,
            'Date': self.get_date_header()}

        req = Request.blank('/', environ=environ, headers=headers)
        with self.assertRaises(SignatureDoesNotMatch):
            sigv4_req = SigV4Request(req.environ)
            sigv4_req._headers_to_sign()

    def test_canonical_uri_sigv2(self):
        environ = {
            'HTTP_HOST': 'bucket1.s3.test.com',
            'REQUEST_METHOD': 'GET'}

        headers = {'Authorization': 'AWS test:tester:hmac',
                   'X-Amz-Date': self.get_date_header()}

        # Virtual hosted-style
        req = Request.blank('/', environ=environ, headers=headers)
        sigv2_req = S3Request(
            req.environ, conf=Config({'storage_domains': ['s3.test.com']}))
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/obj1', environ=environ, headers=headers)
        sigv2_req = S3Request(
            req.environ, conf=Config({'storage_domains': ['s3.test.com']}))
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/obj1')
        self.assertEqual(req.environ['PATH_INFO'], '/obj1')

        req = Request.blank('/obj2', environ=environ, headers=headers)
        sigv2_req = S3Request(
            req.environ, conf=Config({
                'storage_domains': ['alternate.domain', 's3.test.com']}))
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/obj2')
        self.assertEqual(req.environ['PATH_INFO'], '/obj2')

        # Now check the other storage_domain
        environ = {
            'HTTP_HOST': 'bucket1.alternate.domain',
            'REQUEST_METHOD': 'GET'}
        req = Request.blank('/obj2', environ=environ, headers=headers)
        sigv2_req = S3Request(
            req.environ, conf=Config({
                'storage_domains': ['alternate.domain', 's3.test.com']}))
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/obj2')
        self.assertEqual(req.environ['PATH_INFO'], '/obj2')

        # Non existent storage_domain means we can't find the  container
        environ = {
            'HTTP_HOST': 'bucket1.incorrect.domain',
            'REQUEST_METHOD': 'GET'}
        req = Request.blank('/obj2', environ=environ, headers=headers)
        sigv2_req = S3Request(
            req.environ, conf=Config({
                'storage_domains': ['alternate.domain', 's3.test.com']}))
        uri = sigv2_req._canonical_uri()
        # uo oh, no bucket
        self.assertEqual(uri, '/obj2')
        self.assertEqual(sigv2_req.container_name, 'obj2')

        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'GET'}

        # Path-style
        req = Request.blank('/', environ=environ, headers=headers)
        sigv2_req = S3Request(req.environ)
        uri = sigv2_req._canonical_uri()

        self.assertEqual(uri, '/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/bucket1/obj1',
                            environ=environ,
                            headers=headers)
        sigv2_req = S3Request(req.environ)
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/obj1')
        self.assertEqual(req.environ['PATH_INFO'], '/bucket1/obj1')

    def test_canonical_uri_sigv4(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'GET'}

        # host and x-amz-date
        x_amz_date = self.get_v4_amz_date_header()
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-Content-SHA256': '0' * 64,
            'Date': self.get_date_header(),
            'X-Amz-Date': x_amz_date}

        # Virtual hosted-style
        self.s3api.conf.storage_domains = ['s3.test.com']
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, b'/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/obj1', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, b'/obj1')
        self.assertEqual(req.environ['PATH_INFO'], '/obj1')

        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'GET'}

        # Path-style
        self.s3api.conf.storage_domains = []
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, b'/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/bucket/obj1',
                            environ=environ,
                            headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, b'/bucket/obj1')
        self.assertEqual(req.environ['PATH_INFO'], '/bucket/obj1')

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def _test_check_signature_sigv2(self, secret):
        # See https://web.archive.org/web/20151226025049/http://
        # docs.aws.amazon.com//AmazonS3/latest/dev/RESTAuthentication.html
        req = Request.blank('/photos/puppy.jpg', headers={
            'Host': 'johnsmith.s3.amazonaws.com',
            'Date': 'Tue, 27 Mar 2007 19:36:42 +0000',
            'Authorization': ('AWS AKIAIOSFODNN7EXAMPLE:'
                              'bWq2s1WEIj+Ydj0vQ697zp+IXMU='),
        })
        sigv2_req = S3Request(req.environ, conf=Config({
            'storage_domains': ['s3.amazonaws.com']}))
        expected_sts = b'\n'.join([
            b'GET',
            b'',
            b'',
            b'Tue, 27 Mar 2007 19:36:42 +0000',
            b'/johnsmith/photos/puppy.jpg',
        ])
        self.assertEqual(expected_sts, sigv2_req.sig_checker.string_to_sign)
        self.assertTrue(sigv2_req.sig_checker.check_signature(secret))

        req = Request.blank('/photos/puppy.jpg', method='PUT', headers={
            'Content-Type': 'image/jpeg',
            'Content-Length': '94328',
            'Host': 'johnsmith.s3.amazonaws.com',
            'Date': 'Tue, 27 Mar 2007 21:15:45 +0000',
            'Authorization': ('AWS AKIAIOSFODNN7EXAMPLE:'
                              'MyyxeRY7whkBe+bq8fHCL/2kKUg='),
        })
        sigv2_req = S3Request(req.environ, conf=Config({
            'storage_domains': ['s3.amazonaws.com']}))
        expected_sts = b'\n'.join([
            b'PUT',
            b'',
            b'image/jpeg',
            b'Tue, 27 Mar 2007 21:15:45 +0000',
            b'/johnsmith/photos/puppy.jpg',
        ])
        self.assertEqual(expected_sts, sigv2_req.sig_checker.string_to_sign)
        self.assertTrue(sigv2_req.sig_checker.check_signature(secret))

        req = Request.blank(
            '/?prefix=photos&max-keys=50&marker=puppy',
            headers={
                'User-Agent': 'Mozilla/5.0',
                'Host': 'johnsmith.s3.amazonaws.com',
                'Date': 'Tue, 27 Mar 2007 19:42:41 +0000',
                'Authorization': ('AWS AKIAIOSFODNN7EXAMPLE:'
                                  'htDYFYduRNen8P9ZfE/s9SuKy0U='),
            })
        sigv2_req = S3Request(req.environ, conf=Config({
            'storage_domains': ['s3.amazonaws.com']}))
        expected_sts = b'\n'.join([
            b'GET',
            b'',
            b'',
            b'Tue, 27 Mar 2007 19:42:41 +0000',
            b'/johnsmith/',
        ])
        self.assertEqual(expected_sts, sigv2_req.sig_checker.string_to_sign)
        self.assertTrue(sigv2_req.sig_checker.check_signature(secret))

        with patch('swift.common.middleware.s3api.s3request.streq_const_time',
                   return_value=True) as mock_eq:
            self.assertTrue(sigv2_req.sig_checker.check_signature(secret))
        mock_eq.assert_called_once()

    def test_check_signature_sigv2(self):
        self._test_check_signature_sigv2(
            'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

    def test_check_signature_sigv2_unicode_string(self):
        self._test_check_signature_sigv2(
            u'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_multi_bytes_secret_failure(self):
        # Test v2 check_signature with multi bytes invalid secret
        req = Request.blank('/photos/puppy.jpg', headers={
            'Host': 'johnsmith.s3.amazonaws.com',
            'Date': 'Tue, 27 Mar 2007 19:36:42 +0000',
            'Authorization': ('AWS AKIAIOSFODNN7EXAMPLE:'
                              'bWq2s1WEIj+Ydj0vQ697zp+IXMU='),
        })
        sigv2_req = S3Request(req.environ, Config({
            'storage_domains': ['s3.amazonaws.com']}))
        # This is a failure case with utf-8 non-ascii multi-bytes charactor
        # but we expect to return just False instead of exceptions
        self.assertFalse(sigv2_req.sig_checker.check_signature(
            u'\u30c9\u30e9\u30b4\u30f3'))

        # Test v4 check_signature with multi bytes invalid secret
        amz_date_header = self.get_v4_amz_date_header()
        req = Request.blank('/photos/puppy.jpg', headers={
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % amz_date_header.split('T', 1)[0],
            'X-Amz-Content-SHA256': '0' * 64,
            'X-Amz-Date': amz_date_header
        })
        sigv4_req = SigV4Request(
            req.environ, Config({'storage_domains': ['s3.amazonaws.com']}))
        self.assertFalse(sigv4_req.sig_checker.check_signature(
            u'\u30c9\u30e9\u30b4\u30f3'))

        with patch('swift.common.middleware.s3api.s3request.streq_const_time',
                   return_value=False) as mock_eq:
            self.assertFalse(sigv4_req.sig_checker.check_signature(
                u'\u30c9\u30e9\u30b4\u30f3'))
        mock_eq.assert_called_once()

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_sigv4_unsigned_payload(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'GET'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=f721a7941d5b7710344bc62cc45f87e66f4bb1dd00d9075ee61'
                '5b1a5c72b0f8c',
            'X-Amz-Content-SHA256': 'UNSIGNED-PAYLOAD',
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z'}

        # Virtual hosted-style
        self.s3api.conf.storage_domains = ['s3.test.com']
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        self.assertTrue(
            sigv4_req._canonical_request().endswith(b'UNSIGNED-PAYLOAD'))
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_sigv4_url_encode(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/~/file,1_1:1-1'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=06559fbf839b7ceac19d69f510a2d3b7dcb569c8df310965cc1'
                '6a1dc55b3394a',
            'X-Amz-Content-SHA256': 'UNSIGNED-PAYLOAD',
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z'}

        # Virtual hosted-style
        self.s3api.conf.storage_domain = 's3.test.com'
        req = Request.blank(
            environ['RAW_PATH_INFO'], environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        canonical_req = sigv4_req._canonical_request()
        self.assertIn(b'PUT\n/test/~/file%2C1_1%3A1-1\n', canonical_req)
        self.assertTrue(canonical_req.endswith(b'UNSIGNED-PAYLOAD'))
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_sigv4_req_zero_content_length_sha256(self):
        # Virtual hosted-style
        self.s3api.conf.storage_domains = ['s3.test.com']

        # bad sha256 -- but note that SHAs are not checked for GET/HEAD!
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'PUT'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=5f31c77dbc63e7c6ffc84dae60a9261c57c44884fe7927baeb9'
                '84f418d4d511a',
            'X-Amz-Content-SHA256': '0' * 64,
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
        }

        # lowercase sha256
        req = Request.blank('/', environ=environ, headers=headers)
        self.assertRaises(XAmzContentSHA256Mismatch, SigV4Request, req.environ)
        sha256_of_nothing = hashlib.sha256().hexdigest().encode('ascii')
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=96df261d8f0b617b7c6368e0c5d96ee61f1ec84005e826ece65'
                'c0e0f97eba945',
            'X-Amz-Content-SHA256': sha256_of_nothing,
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
        }
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        self.assertTrue(
            sigv4_req._canonical_request().endswith(sha256_of_nothing))
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))

        # uppercase sha256 -- signature changes, but content's valid
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=7a3c396fd6043fb397888e6f4d6acc294a99636ff0bb57b283d'
                '9e075ed87fce2',
            'X-Amz-Content-SHA256': sha256_of_nothing.upper(),
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
        }
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        self.assertTrue(
            sigv4_req._canonical_request().endswith(sha256_of_nothing.upper()))
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_v4_req_xmz_content_sha256_mismatch(self):
        # Virtual hosted-style
        def fake_app(environ, start_response):
            environ['wsgi.input'].read()

        self.s3api.conf.storage_domains = ['s3.test.com']
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'PUT'}
        sha256_of_body = hashlib.sha256(b'body').hexdigest()
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-date,'
                'Signature=5f31c77dbc63e7c6ffc84dae60a9261c57c44884fe7927baeb9'
                '84f418d4d511a',
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 4,
            'X-Amz-Content-SHA256': sha256_of_body,
        }
        req = Request.blank('/', environ=environ, headers=headers,
                            body=b'not_body')
        with self.assertRaises(XAmzContentSHA256Mismatch) as caught:
            SigV4Request(req.environ).get_response(fake_app)
        self.assertIn(b'<Code>XAmzContentSHA256Mismatch</Code>',
                      caught.exception.body)
        self.assertIn(
            ('<ClientComputedContentSHA256>%s</ClientComputedContentSHA256>'
             % sha256_of_body).encode('ascii'),
            caught.exception.body)
        self.assertIn(
            ('<S3ComputedContentSHA256>%s</S3ComputedContentSHA256>'
             % hashlib.sha256(b'not_body').hexdigest()).encode('ascii'),
            caught.exception.body)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_v4_req_amz_content_sha256_missing(self):
        # Virtual hosted-style
        self.s3api.conf.storage_domains = ['s3.test.com']
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'PUT'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-date,'
                'Signature=5f31c77dbc63e7c6ffc84dae60a9261c57c44884fe7927baeb9'
                '84f418d4d511a',
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
        }
        req = Request.blank('/', environ=environ, headers=headers)
        self.assertRaises(InvalidRequest, SigV4Request, req.environ)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_v4_req_x_mz_content_sha256_bad_format(self):
        # Virtual hosted-style
        self.s3api.conf.storage_domains = ['s3.test.com']
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'PUT'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-date,'
                'Signature=5f31c77dbc63e7c6ffc84dae60a9261c57c44884fe7927baeb9'
                '84f418d4d511a',
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
            'X-Amz-Content-SHA256': '0' * 63  # too short
        }
        req = Request.blank('/', environ=environ, headers=headers)
        self.assertRaises(InvalidArgument, SigV4Request, req.environ)

        headers['X-Amz-Content-SHA256'] = '0' * 63 + 'x'  # bad character
        req = Request.blank('/', environ=environ, headers=headers)
        self.assertRaises(InvalidArgument, SigV4Request, req.environ)

    def test_validate_part_number(self):
        sw_req = Request.blank('/nojunk',
                               environ={'REQUEST_METHOD': 'GET'},
                               headers={
                                   'Authorization': 'AWS test:tester:hmac',
                                   'Date': self.get_date_header()})
        req = S3Request(sw_req.environ)
        self.assertIsNone(req.validate_part_number())

        # ok
        sw_req = Request.blank('/nojunk?partNumber=102',
                               environ={'REQUEST_METHOD': 'GET'},
                               headers={
                                   'Authorization': 'AWS test:tester:hmac',
                                   'Date': self.get_date_header()})
        req = S3Request(sw_req.environ)
        self.assertEqual(102, req.validate_part_number())
        req = S3Request(sw_req.environ,
                        conf=Config({'max_upload_part_num': 100}))
        self.assertEqual(102, req.validate_part_number(102))
        req = S3Request(sw_req.environ,
                        conf=Config({'max_upload_part_num': 102}))
        self.assertEqual(102, req.validate_part_number(102))

    def test_validate_part_number_invalid_argument(self):
        def check_invalid_argument(part_num, max_parts, parts_count, exp_max):
            sw_req = Request.blank('/nojunk?partNumber=%s' % part_num,
                                   environ={'REQUEST_METHOD': 'GET'},
                                   headers={
                                       'Authorization': 'AWS test:tester:hmac',
                                       'Date': self.get_date_header()})
            req = S3Request(sw_req.environ,
                            conf=Config({'max_upload_part_num': max_parts}))
            with self.assertRaises(InvalidPartArgument) as cm:
                req.validate_part_number(parts_count=parts_count)
            self.assertEqual('400 Bad Request', str(cm.exception))
            self.assertIn(
                b'Part number must be an integer between 1 and %d' % exp_max,
                cm.exception.body)

        check_invalid_argument(102, 99, None, 99)
        check_invalid_argument(102, 100, 99, 100)
        check_invalid_argument(102, 100, 101, 101)
        check_invalid_argument(102, 101, 100, 101)
        check_invalid_argument(102, 101, 101, 101)
        check_invalid_argument('banana', 1000, None, 1000)
        check_invalid_argument(0, 10000, None, 10000)

    def test_validate_part_number_invalid_part_number(self):
        def check_invalid_part_num(part_num, max_parts, parts_count):
            sw_req = Request.blank('/nojunk?partNumber=%s' % part_num,
                                   environ={'REQUEST_METHOD': 'GET'},
                                   headers={
                                       'Authorization': 'AWS test:tester:hmac',
                                       'Date': self.get_date_header()})
            req = S3Request(sw_req.environ,
                            conf=Config({'max_upload_part_num': max_parts}))
            with self.assertRaises(InvalidPartNumber) as cm:
                req.validate_part_number(parts_count=parts_count)
            self.assertEqual('416 Requested Range Not Satisfiable',
                             str(cm.exception))
            self.assertIn(b'The requested partnumber is not satisfiable',
                          cm.exception.body)

        check_invalid_part_num(102, 10000, 1)
        check_invalid_part_num(102, 102, 101)
        check_invalid_part_num(102, 10000, 101)

    def test_validate_part_number_with_range_header(self):
        sw_req = Request.blank('/nojunk?partNumber=1',
                               environ={'REQUEST_METHOD': 'GET'},
                               headers={
                                   'Range': 'bytes=1-2',
                                   'Authorization': 'AWS test:tester:hmac',
                                   'Date': self.get_date_header()})
        req = S3Request(sw_req.environ)
        with self.assertRaises(InvalidRequest) as cm:
            req.validate_part_number()
        self.assertEqual('400 Bad Request',
                         str(cm.exception))
        self.assertIn(b'Cannot specify both Range header and partNumber query '
                      b'parameter', cm.exception.body)

        # bad part number AND Range header
        sw_req = Request.blank('/nojunk?partNumber=0',
                               environ={'REQUEST_METHOD': 'GET'},
                               headers={
                                   'Range': 'bytes=1-2',
                                   'Authorization': 'AWS test:tester:hmac',
                                   'Date': self.get_date_header()})
        req = S3Request(sw_req.environ)
        with self.assertRaises(InvalidRequest) as cm:
            req.validate_part_number()
        self.assertEqual('400 Bad Request',
                         str(cm.exception))
        self.assertIn(b'Cannot specify both Range header and partNumber query '
                      b'parameter', cm.exception.body)

    @mock.patch('swift.common.middleware.s3api.subresource.ACL.check_owner')
    def test_sigv2_content_sha256_ok(self, mock_check_owner):
        good_sha_256 = hashlib.sha256(b'body').hexdigest()
        req = Request.blank('/bucket/object',
                            method='PUT',
                            body=b'body',
                            headers={'content-encoding': 'aws-chunked',
                                     'x-amz-content-sha256': good_sha_256,
                                     'Content-Length': '4',
                                     'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')

    @mock.patch('swift.common.middleware.s3api.subresource.ACL.check_owner')
    def test_sigv2_content_sha256_bad_value(self, mock_check_owner):
        good_sha_256 = hashlib.sha256(b'body').hexdigest()
        bad_sha_256 = hashlib.sha256(b'not body').hexdigest()
        req = Request.blank('/bucket/object',
                            method='PUT',
                            body=b'body',
                            headers={'content-encoding': 'aws-chunked',
                                     'x-amz-content-sha256':
                                         bad_sha_256,
                                     'Content-Length': '4',
                                     'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status, '400 Bad Request')
        self.assertIn(f'<ClientComputedContentSHA256>{bad_sha_256}'
                      '</ClientComputedContentSHA256>',
                      body.decode('utf8'))
        self.assertIn(f'<S3ComputedContentSHA256>{good_sha_256}'
                      '</S3ComputedContentSHA256>',
                      body.decode('utf8'))

    @mock.patch('swift.common.middleware.s3api.subresource.ACL.check_owner')
    def test_sigv2_content_encoding_aws_chunked_is_ignored(
            self, mock_check_owner):
        req = Request.blank('/bucket/object',
                            method='PUT',
                            headers={'content-encoding': 'aws-chunked',
                                     'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})

        status, _, body = self.call_s3api(req)
        self.assertEqual(status, '200 OK')

    def test_sigv2_content_sha256_streaming_is_bad_request(self):
        def do_test(sha256):
            req = Request.blank(
                '/bucket/object',
                method='PUT',
                headers={'content-encoding': 'aws-chunked',
                         'x-amz-content-sha256': sha256,
                         'Content-Length': '0',
                         'x-amz-decoded-content-length': '0',
                         'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()})
            status, _, body = self.call_s3api(req)
            # sig v2 wants that to actually be the SHA!
            self.assertEqual(status, '400 Bad Request', body)
            self.assertEqual(self._get_error_code(body),
                             'XAmzContentSHA256Mismatch')
            self.assertIn(f'<ClientComputedContentSHA256>{sha256}'
                          '</ClientComputedContentSHA256>',
                          body.decode('utf8'))

        do_test('STREAMING-UNSIGNED-PAYLOAD-TRAILER')
        do_test('STREAMING-AWS4-HMAC-SHA256-PAYLOAD')
        do_test('STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER')
        do_test('STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD')
        do_test('STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER')

    def test_sigv2_content_sha256_streaming_no_decoded_content_length(self):
        # MissingContentLength trumps XAmzContentSHA256Mismatch
        def do_test(sha256):
            req = Request.blank(
                '/bucket/object',
                method='PUT',
                headers={'content-encoding': 'aws-chunked',
                         'x-amz-content-sha256': sha256,
                         'Content-Length': '0',
                         'Authorization': 'AWS test:tester:hmac',
                         'Date': self.get_date_header()})
            status, _, body = self.call_s3api(req)
            self.assertEqual(status, '411 Length Required', body)
            self.assertEqual(self._get_error_code(body),
                             'MissingContentLength')

        do_test('STREAMING-UNSIGNED-PAYLOAD-TRAILER')
        do_test('STREAMING-AWS4-HMAC-SHA256-PAYLOAD')
        do_test('STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER')
        do_test('STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD')
        do_test('STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER')

    def _make_sig_v4_unsigned_payload_req(self, body=None, extra_headers=None):
        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/file'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20220330/us-east-1/s3/aws4_request,'
                'SignedHeaders=content-length;host;x-amz-content-sha256;'
                'x-amz-date,'
                'Signature=d14bba0da2bba545c8275cb75c99b326cbdfdad015465dbaeca'
                'e18c7647c73da',
            'Content-Length': '27',
            'Host': 's3.test.com',
            'X-Amz-Content-SHA256': 'UNSIGNED-PAYLOAD',
            'X-Amz-Date': '20220330T095351Z',
        }
        if extra_headers:
            headers.update(extra_headers)
        return Request.blank(environ['RAW_PATH_INFO'], environ=environ,
                             headers=headers, body=body)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def _test_sig_v4_unsigned_payload(self, body=None, extra_headers=None):
        req = self._make_sig_v4_unsigned_payload_req(
            body=body, extra_headers=extra_headers)
        sigv4_req = SigV4Request(req.environ)
        # Verify header signature
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))
        return sigv4_req

    def test_sig_v4_unsgnd_pyld_no_crc_ok(self):
        body = b'abcdefghijklmnopqrstuvwxyz\n'
        sigv4_req = self._test_sig_v4_unsigned_payload(body=body)
        resp_body = sigv4_req.environ['wsgi.input'].read()
        self.assertEqual(body, resp_body)

    def test_sig_v4_unsgnd_pyld_crc32_ok(self):
        body = b'abcdefghijklmnopqrstuvwxyz\n'
        crc = base64.b64encode(checksum.crc32(body).digest())
        sigv4_req = self._test_sig_v4_unsigned_payload(
            body=body,
            extra_headers={'X-Amz-Checksum-Crc32': crc}
        )
        resp_body = sigv4_req.environ['wsgi.input'].read()
        self.assertEqual(body, resp_body)

    def test_sig_v4_unsgnd_pyld_crc32_mismatch(self):
        body = b'abcdefghijklmnopqrstuvwxyz\n'
        crc = base64.b64encode(checksum.crc32(b'not the body').digest())
        sigv4_req = self._test_sig_v4_unsigned_payload(
            body=body,
            extra_headers={'X-Amz-Checksum-Crc32': crc}
        )
        with self.assertRaises(S3InputChecksumMismatch):
            sigv4_req.environ['wsgi.input'].read()

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_unsgnd_pyld_crc32_invalid(self):
        req = self._make_sig_v4_unsigned_payload_req(
            extra_headers={'X-Amz-Checksum-Crc32': 'not a crc'}
        )
        with self.assertRaises(s3request.InvalidRequest):
            SigV4Request(req.environ)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_unsgnd_pyld_declares_crc32_trailer(self):
        req = self._make_sig_v4_unsigned_payload_req(
            extra_headers={'X-Amz-Trailer': 'x-amz-checksum-crc32'})
        with self.assertRaises(s3request.MalformedTrailerError):
            SigV4Request(req.environ)

    def _make_valid_v4_streaming_hmac_sha256_payload_request(self):
        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/file'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20220330/us-east-1/s3/aws4_request,'
                'SignedHeaders=content-encoding;content-length;host;x-amz-con'
                'tent-sha256;x-amz-date;x-amz-decoded-content-length,'
                'Signature=aa1b67fc5bc4503d05a636e6e740dcb757d3aa2352f32e7493f'
                '261f71acbe1d5',
            'Content-Encoding': 'aws-chunked',
            'Content-Length': '369',
            'Host': 's3.test.com',
            'X-Amz-Content-SHA256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
            'X-Amz-Date': '20220330T095351Z',
            'X-Amz-Decoded-Content-Length': '25'}
        body = 'a;chunk-signature=4a397f01db2cd700402dc38931b462e789ae49911d' \
               'c229d93c9f9c46fd3e0b21\r\nabcdefghij\r\n' \
               'a;chunk-signature=49177768ee3e9b77c6353ab9f3b9747d188adc11d4' \
               '5b38be94a130616e6d64dc\r\nklmnopqrst\r\n' \
               '5;chunk-signature=c884ebbca35b923cf864854e2a906aa8f5895a7140' \
               '6c73cc6d4ee057527a8c23\r\nuvwz\n\r\n' \
               '0;chunk-signature=50f7c470d6bf6c59126eecc2cb020d532a69c92322' \
               'ddfbbd21811de45491022c\r\n\r\n'

        req = Request.blank(environ['RAW_PATH_INFO'], environ=environ,
                            headers=headers, body=body.encode('utf8'))
        return SigV4Request(req.environ)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_v4_hmac_sha256_payload_chunk_valid(self):
        s3req = self._make_valid_v4_streaming_hmac_sha256_payload_request()
        # Verify header signature
        self.assertTrue(s3req.sig_checker.check_signature('secret'))

        self.assertEqual(b'abcdefghij', s3req.environ['wsgi.input'].read(10))
        self.assertEqual(b'klmnopqrst', s3req.environ['wsgi.input'].read(10))
        self.assertEqual(b'uvwz\n', s3req.environ['wsgi.input'].read(10))
        self.assertEqual(b'', s3req.environ['wsgi.input'].read(10))
        self.assertTrue(s3req.sig_checker._all_chunk_signatures_valid)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_v4_hmac_sha256_payload_no_secret(self):
        # verify S3InputError if auth middleware does NOT call check_signature
        # before the stream is read
        s3req = self._make_valid_v4_streaming_hmac_sha256_payload_request()
        with self.assertRaises(s3request.S3InputMissingSecret) as cm:
            s3req.environ['wsgi.input'].read(10)

        # ...which in context gets translated to a 501 response
        s3req = self._make_valid_v4_streaming_hmac_sha256_payload_request()
        with self.assertRaises(s3response.S3NotImplemented) as cm, \
                s3req.translate_read_errors():
            s3req.environ['wsgi.input'].read(10)
        self.assertIn(
            'Transferring payloads in multiple chunks using aws-chunked is '
            'not supported.', str(cm.exception.body))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_v4_hmac_sha256_payload_chunk_invalid(self):
        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/file'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20220330/us-east-1/s3/aws4_request,'
                'SignedHeaders=content-encoding;content-length;host;x-amz-con'
                'tent-sha256;x-amz-date;x-amz-decoded-content-length,'
                'Signature=aa1b67fc5bc4503d05a636e6e740dcb757d3aa2352f32e7493f'
                '261f71acbe1d5',
            'Content-Encoding': 'aws-chunked',
            'Content-Length': '369',
            'Host': 's3.test.com',
            'X-Amz-Content-SHA256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
            'X-Amz-Date': '20220330T095351Z',
            'X-Amz-Decoded-Content-Length': '25'}
        # second chunk signature is incorrect, should be
        # 49177768ee3e9b77c6353ab9f3b9747d188adc11d45b38be94a130616e6d64dc
        body = 'a;chunk-signature=4a397f01db2cd700402dc38931b462e789ae49911d' \
               'c229d93c9f9c46fd3e0b21\r\nabcdefghij\r\n' \
               'a;chunk-signature=49177768ee3e9b77c6353ab0f3b9747d188adc11d4' \
               '5b38be94a130616e6d64dc\r\nklmnopqrst\r\n' \
               '5;chunk-signature=c884ebbca35b923cf864854e2a906aa8f5895a7140' \
               '6c73cc6d4ee057527a8c23\r\nuvwz\n\r\n' \
               '0;chunk-signature=50f7c470d6bf6c59126eecc2cb020d532a69c92322' \
               'ddfbbd21811de45491022c\r\n\r\n'

        req = Request.blank(environ['RAW_PATH_INFO'], environ=environ,
                            headers=headers, body=body.encode('utf8'))
        sigv4_req = SigV4Request(req.environ)
        # Verify header signature
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))

        self.assertEqual(b'abcdefghij', req.environ['wsgi.input'].read(10))
        with self.assertRaises(s3request.S3InputChunkSignatureMismatch):
            req.environ['wsgi.input'].read(10)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_v4_hmac_sha256_payload_chunk_wrong_size(self):
        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/file'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20220330/us-east-1/s3/aws4_request,'
                'SignedHeaders=content-encoding;content-length;host;x-amz-con'
                'tent-sha256;x-amz-date;x-amz-decoded-content-length,'
                'Signature=aa1b67fc5bc4503d05a636e6e740dcb757d3aa2352f32e7493f'
                '261f71acbe1d5',
            'Content-Encoding': 'aws-chunked',
            'Content-Length': '369',
            'Host': 's3.test.com',
            'X-Amz-Content-SHA256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
            'X-Amz-Date': '20220330T095351Z',
            'X-Amz-Decoded-Content-Length': '25'}
        # 2nd chunk contains an incorrect chunk size (9 should be a)...
        body = 'a;chunk-signature=4a397f01db2cd700402dc38931b462e789ae49911d' \
               'c229d93c9f9c46fd3e0b21\r\nabcdefghij\r\n' \
               '9;chunk-signature=49177768ee3e9b77c6353ab9f3b9747d188adc11d4' \
               '5b38be94a130616e6d64dc\r\nklmnopqrst\r\n' \
               '5;chunk-signature=c884ebbca35b923cf864854e2a906aa8f5895a7140' \
               '6c73cc6d4ee057527a8c23\r\nuvwz\n\r\n' \
               '0;chunk-signature=50f7c470d6bf6c59126eecc2cb020d532a69c92322' \
               'ddfbbd21811de45491022c\r\n\r\n'

        req = Request.blank(environ['RAW_PATH_INFO'], environ=environ,
                            headers=headers, body=body.encode('utf8'))
        sigv4_req = SigV4Request(req.environ)
        # Verify header signature
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))

        self.assertEqual(b'abcdefghij', req.environ['wsgi.input'].read(10))
        with self.assertRaises(s3request.S3InputChunkSignatureMismatch):
            req.environ['wsgi.input'].read(10)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_signature_v4_hmac_sha256_payload_chunk_no_last_chunk(self):
        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/file'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20220330/us-east-1/s3/aws4_request,'
                'SignedHeaders=content-encoding;content-length;host;x-amz-con'
                'tent-sha256;x-amz-date;x-amz-decoded-content-length,'
                'Signature=99759fb2823febb695950e6b75a7a1396b164742da9d204f71f'
                'db3a3a52216aa',
            'Content-Encoding': 'aws-chunked',
            'Content-Length': '283',
            'Host': 's3.test.com',
            'X-Amz-Content-SHA256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
            'X-Amz-Date': '20220330T095351Z',
            'X-Amz-Decoded-Content-Length': '25'}
        body = 'a;chunk-signature=9c35d0203ce923cb7837b5e4a2984f2c107b05ac45' \
               '80bafce7541c4b142b9712\r\nabcdefghij\r\n' \
               'a;chunk-signature=f514382beed5f287a5181b8293399fe006fd9398ee' \
               '4b8aed910238092a4d5ec7\r\nklmnopqrst\r\n' \
               '5;chunk-signature=ed6a54f035b920e7daa378ab2d255518c082573c98' \
               '60127c80d43697375324f4\r\nuvwz\n\r\n'

        req = Request.blank(environ['RAW_PATH_INFO'], environ=environ,
                            headers=headers, body=body.encode('utf8'))
        sigv4_req = SigV4Request(req.environ)
        # Verify header signature
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))
        self.assertEqual(b'abcdefghij', req.environ['wsgi.input'].read(10))
        self.assertEqual(b'klmnopqrst', req.environ['wsgi.input'].read(10))
        with self.assertRaises(s3request.S3InputIncomplete):
            req.environ['wsgi.input'].read(5)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def _test_sig_v4_streaming_aws_hmac_sha256_payload_trailer(
            self, body):
        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/file'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20220330/us-east-1/s3/aws4_request,'
                'SignedHeaders=content-encoding;content-length;host;x-amz-con'
                'tent-sha256;x-amz-date;x-amz-decoded-content-length,'
                'Signature=bee7ad4f1a4f16c22f3b24155ab749b2aca0773065ccf08bc41'
                'a1e8e84748311',
            'Content-Encoding': 'aws-chunked',
            'Content-Length': '369',
            'Host': 's3.test.com',
            'X-Amz-Content-SHA256':
                'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
            'X-Amz-Date': '20220330T095351Z',
            'X-Amz-Decoded-Content-Length': '27',
            'X-Amz-Trailer': 'x-amz-checksum-sha256',
        }
        req = Request.blank(environ['RAW_PATH_INFO'], environ=environ,
                            headers=headers, body=body.encode('utf8'))
        sigv4_req = SigV4Request(req.environ)
        # Verify header signature
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))
        return sigv4_req

    def test_check_sig_v4_streaming_aws_hmac_sha256_payload_trailer_ok(self):
        body = 'a;chunk-signature=c9dd07703599d3d0bd51c96193110756d4f7091d5a' \
               '4408314a53a802e635b1ad\r\nabcdefghij\r\n' \
               'a;chunk-signature=662dc18fb1a3ddad6abc2ce9ebb0748bedacd219eb' \
               '223a5e80721c2637d30240\r\nklmnopqrst\r\n' \
               '7;chunk-signature=b63f141c2012de9ac60b961795ef31ad3202b125aa' \
               '873b4142cf9d815360abc0\r\nuvwxyz\n\r\n' \
               '0;chunk-signature=b1ff1f86dccfbe9bcc80011e2b87b72e43e0c7f543' \
               'bb93612c06f9808ccb772e\r\n' \
               'x-amz-checksum-sha256:EBCn52FhCYCsWRNZyHH3JN4VDyNEDrtZWaxMBy' \
               'TJHZE=\r\n' \
               'x-amz-trailer-signature:1212d72cb487bf08ed25d1329dc93f65fde0' \
               'dcb21739a48f3182c86cfe79737b\r\n'
        req = self._test_sig_v4_streaming_aws_hmac_sha256_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         req.environ['wsgi.input'].read())

    def test_check_sig_v4_streaming_aws_hmac_sha256_missing_trailer_sig(self):
        body = 'a;chunk-signature=c9dd07703599d3d0bd51c96193110756d4f7091d5a' \
               '4408314a53a802e635b1ad\r\nabcdefghij\r\n' \
               'a;chunk-signature=662dc18fb1a3ddad6abc2ce9ebb0748bedacd219eb' \
               '223a5e80721c2637d30240\r\nklmnopqrst\r\n' \
               '7;chunk-signature=b63f141c2012de9ac60b961795ef31ad3202b125aa' \
               '873b4142cf9d815360abc0\r\nuvwxyz\n\r\n' \
               '0;chunk-signature=b1ff1f86dccfbe9bcc80011e2b87b72e43e0c7f543' \
               'bb93612c06f9808ccb772e\r\n' \
               'x-amz-checksum-sha256:foo\r\n'
        req = self._test_sig_v4_streaming_aws_hmac_sha256_payload_trailer(body)
        with self.assertRaises(s3request.S3InputIncomplete):
            req.environ['wsgi.input'].read()

    def test_check_sig_v4_streaming_aws_hmac_sha256_payload_trailer_bad(self):
        body = 'a;chunk-signature=c9dd07703599d3d0bd51c96193110756d4f7091d5a' \
               '4408314a53a802e635b1ad\r\nabcdefghij\r\n' \
               'a;chunk-signature=000000000000000000000000000000000000000000' \
               '0000000000000000000000\r\nklmnopqrst\r\n' \
               '7;chunk-signature=b63f141c2012de9ac60b961795ef31ad3202b125aa' \
               '873b4142cf9d815360abc0\r\nuvwxyz\n\r\n' \
               '0;chunk-signature=b1ff1f86dccfbe9bcc80011e2b87b72e43e0c7f543' \
               'bb93612c06f9808ccb772e\r\n' \
               'x-amz-checksum-sha256:foo\r\n'
        req = self._test_sig_v4_streaming_aws_hmac_sha256_payload_trailer(body)
        self.assertEqual(b'abcdefghij', req.environ['wsgi.input'].read(10))
        with self.assertRaises(s3request.S3InputChunkSignatureMismatch):
            req.environ['wsgi.input'].read(10)

    def _make_sig_v4_streaming_unsigned_payload_trailer_req(
            self, body=None, wsgi_input=None, extra_headers=None):
        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'PUT',
            'RAW_PATH_INFO': '/test/file'}
        if body:
            body = body.encode('utf8')
        elif wsgi_input:
            environ['wsgi.input'] = wsgi_input
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20220330/us-east-1/s3/aws4_request,'
                'SignedHeaders=content-encoding;content-length;host;x-amz-con'
                'tent-sha256;x-amz-date;x-amz-decoded-content-length,'
                'Signature=43727fcfa7765e97cd3cbfc112fed5fedc31e2b7930588ddbca'
                '3feaa1205a7f2',
            'Content-Encoding': 'aws-chunked',
            'Content-Length': '369',
            'Host': 's3.test.com',
            'X-Amz-Content-SHA256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
            'X-Amz-Date': '20220330T095351Z',
            'X-Amz-Decoded-Content-Length': '27',
        }
        if extra_headers:
            headers.update(extra_headers)
        return Request.blank(environ['RAW_PATH_INFO'], environ=environ,
                             headers=headers, body=body)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def _test_sig_v4_streaming_unsigned_payload_trailer(
            self, body=None, x_amz_trailer='x-amz-checksum-sha256'):
        if x_amz_trailer is None:
            headers = {}
        else:
            headers = {'X-Amz-Trailer': x_amz_trailer}

        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body, extra_headers=headers)
        sigv4_req = SigV4Request(req.environ)
        # Verify header signature
        self.assertTrue(sigv4_req.sig_checker.check_signature('secret'))
        return sigv4_req

    def test_sig_v4_strm_unsgnd_pyld_trl_ok(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256:EBCn52FhCYCsWRNZyHH3JN4VDyNEDrtZWaxMB' \
               'yTJHZE=\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         s3req.environ['wsgi.input'].read())

    def test_sig_v4_strm_unsgnd_pyld_trl_none_ok(self):
        # verify it's ok to not send any trailer
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(
            body, x_amz_trailer=None)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         s3req.environ['wsgi.input'].read())

    def test_sig_v4_strm_unsgnd_pyld_trl_undeclared(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256:undeclared\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(
            body, x_amz_trailer=None)
        self.assertEqual(b'abcdefghijklmnopqrst',
                         s3req.environ['wsgi.input'].read(20))
        with self.assertRaises(s3request.S3InputIncomplete):
            s3req.environ['wsgi.input'].read()

    def test_sig_v4_strm_unsgnd_pyld_trl_multiple(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256:undeclared\r\n'
        with self.assertRaises(s3request.InvalidRequest):
            self._test_sig_v4_streaming_unsigned_payload_trailer(
                body,
                x_amz_trailer='x-amz-checksum-sha256,x-amz-checksum-crc32')

    def test_sig_v4_strm_unsgnd_pyld_trl_with_commas_invalid(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256:undeclared\r\n'
        with self.assertRaises(s3request.InvalidRequest):
            self._test_sig_v4_streaming_unsigned_payload_trailer(
                body,
                x_amz_trailer=', x-amz-checksum-crc32, ,')
        with self.assertRaises(s3request.InvalidRequest):
            self._test_sig_v4_streaming_unsigned_payload_trailer(
                body,
                x_amz_trailer=', x-amz-checksum-crc32')
        with self.assertRaises(s3request.InvalidRequest):
            self._test_sig_v4_streaming_unsigned_payload_trailer(
                body,
                x_amz_trailer=',x-amz-checksum-crc32')
        with self.assertRaises(s3request.InvalidRequest):
            self._test_sig_v4_streaming_unsigned_payload_trailer(
                body,
                x_amz_trailer='x-amz-checksum-crc32, ,')

    def test_sig_v4_strm_unsgnd_pyld_trl_with_commas_ok(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256:EBCn52FhCYCsWRNZyHH3JN4VDyNEDrtZWaxMB' \
               'yTJHZE=\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(
            body, x_amz_trailer='x-amz-checksum-sha256, ')
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         s3req.environ['wsgi.input'].read())
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(
            body, x_amz_trailer='x-amz-checksum-sha256,,')
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         s3req.environ['wsgi.input'].read())

    def test_sig_v4_strm_unsgnd_pyld_trl_unrecognised(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        with self.assertRaises(s3request.InvalidRequest):
            self._test_sig_v4_streaming_unsigned_payload_trailer(
                body,
                x_amz_trailer='x-amz-content-sha256')

    def test_sig_v4_strm_unsgnd_pyld_trl_mismatch(self):
        # the unexpected footer is detected before the incomplete line
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-not-sha256:foo\r\n' \
               'x-'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrst',
                         s3req.environ['wsgi.input'].read(20))
        # trailers are read with penultimate chunk??
        with self.assertRaises(s3request.S3InputMalformedTrailer):
            s3req.environ['wsgi.input'].read()

    def test_sig_v4_strm_unsgnd_pyld_trl_missing(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               '\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrst',
                         s3req.environ['wsgi.input'].read(20))
        # trailers are read with penultimate chunk??
        with self.assertRaises(s3request.S3InputMalformedTrailer):
            s3req.environ['wsgi.input'].read()

    def test_sig_v4_strm_unsgnd_pyld_trl_extra(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-crc32:foo\r\n' \
               'x-amz-checksum-sha32:foo\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrst',
                         s3req.environ['wsgi.input'].read(20))
        # trailers are read with penultimate chunk??
        with self.assertRaises(s3request.S3InputMalformedTrailer):
            s3req.environ['wsgi.input'].read()

    def test_sig_v4_strm_unsgnd_pyld_trl_duplicate(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256:foo\r\n' \
               'x-amz-checksum-sha256:EBCn52FhCYCsWRNZyHH3JN4VDyNEDrtZWaxMB' \
               'yTJHZE=\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrst',
                         s3req.environ['wsgi.input'].read(20))
        # Reading the rest succeeds! AWS would complain about the checksum,
        # but we aren't looking at it (yet)
        s3req.environ['wsgi.input'].read()

    def test_sig_v4_strm_unsgnd_pyld_trl_short(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrst',
                         s3req.environ['wsgi.input'].read(20))
        # trailers are read with penultimate chunk??
        with self.assertRaises(s3request.S3InputIncomplete):
            s3req.environ['wsgi.input'].read()

    def test_sig_v4_strm_unsgnd_pyld_trl_invalid(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n' \
               'x-amz-checksum-sha256: not=base-64\r\n'
        s3req = self._test_sig_v4_streaming_unsigned_payload_trailer(body)
        self.assertEqual(b'abcdefghijklmnopqrst',
                         s3req.environ['wsgi.input'].read(20))
        with self.assertRaises(s3request.S3InputChecksumTrailerInvalid):
            s3req.environ['wsgi.input'].read()

        # ...which in context gets translated to a 400 response
        with self.assertRaises(s3response.InvalidRequest) as cm, \
                s3req.translate_read_errors():
            s3req.environ['wsgi.input'].read()
        self.assertIn(
            'Value for x-amz-checksum-sha256 trailing header is invalid.',
            str(cm.exception.body))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_sha256_ok(self):
        # TODO: do we already have coverage for this?
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        headers = {
            'x-amz-checksum-sha256':
                'EBCn52FhCYCsWRNZyHH3JN4VDyNEDrtZWaxMByTJHZE=',
        }
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers=headers
        )
        sigv4_req = SigV4Request(req.environ)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         sigv4_req.environ['wsgi.input'].read())

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_sha256_mismatch(self):
        # TODO: do we already have coverage for this?
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        headers = {
            'x-amz-sdk-checksum-algorithm': 'sha256',
            'x-amz-checksum-sha256':
                'BADBADBADBADWRNZyHH3JN4VDyNEDrtZWaxMByTJHZE=',
        }
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers=headers
        )
        sigv4_req = SigV4Request(req.environ)
        with self.assertRaises(s3request.BadDigest) as cm, \
                sigv4_req.translate_read_errors():
            sigv4_req.environ['wsgi.input'].read()
        self.assertIn('The SHA256 you specified did not match the calculated '
                      'checksum.', str(cm.exception.body))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_crc32_ok(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(
            checksum.crc32(b'abcdefghijklmnopqrstuvwxyz\n').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-crc32': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         sigv4_req.environ['wsgi.input'].read())

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_crc32_mismatch(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(checksum.crc32(b'not-the-body').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-crc32': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        with self.assertRaises(S3InputChecksumMismatch):
            sigv4_req.environ['wsgi.input'].read()

    @requires_crc32c
    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_crc32c_ok(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(
            checksum.crc32c(b'abcdefghijklmnopqrstuvwxyz\n').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-crc32c': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         sigv4_req.environ['wsgi.input'].read())

    @requires_crc32c
    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_crc32c_mismatch(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(checksum.crc32c(b'not-the-body').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-crc32c': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        with self.assertRaises(S3InputChecksumMismatch):
            sigv4_req.environ['wsgi.input'].read()

    @requires_crc64nvme
    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_crc64nvme_ok(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(
            checksum.crc64nvme(b'abcdefghijklmnopqrstuvwxyz\n').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-crc64nvme': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         sigv4_req.environ['wsgi.input'].read())

    @requires_crc64nvme
    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_crc64nvme_invalid(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(checksum.crc64nvme(b'not-the-body').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-crc64nvme': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        with self.assertRaises(S3InputChecksumMismatch):
            sigv4_req.environ['wsgi.input'].read()

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_sha1_ok(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(
            hashlib.sha1(b'abcdefghijklmnopqrstuvwxyz\n').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-sha1': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        self.assertEqual(b'abcdefghijklmnopqrstuvwxyz\n',
                         sigv4_req.environ['wsgi.input'].read())

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_sha1_mismatch(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(hashlib.sha1(b'not-the-body').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-sha1': crc}
        )
        sigv4_req = SigV4Request(req.environ)
        with self.assertRaises(S3InputChecksumMismatch):
            sigv4_req.environ['wsgi.input'].read()

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_unsupported(self):
        body = 'a\r\nabcdefghij\r\n' \
               'a\r\nklmnopqrst\r\n' \
               '7\r\nuvwxyz\n\r\n' \
               '0\r\n'
        crc = base64.b64encode(
            checksum.crc32c(b'abcdefghijklmnopqrstuvwxyz\n').digest())
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            body=body,
            extra_headers={'x-amz-checksum-crc32c': crc}
        )
        with patch('swift.common.middleware.s3api.s3request.checksum.'
                   '_select_crc32c_impl', side_effect=NotImplementedError):
            with self.assertRaises(S3NotImplemented):
                SigV4Request(req.environ)

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_hdr_and_trailer(self):
        wsgi_input = io.BytesIO(b'123')
        self.assertEqual(0, wsgi_input.tell())
        headers = {
            'x-amz-checksum-sha256':
                'EBCn52FhCYCsWRNZyHH3JN4VDyNEDrtZWaxMByTJHZE=',
            'x-amz-trailer': 'x-amz-checksum-sha256'
        }
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            wsgi_input=wsgi_input,
            extra_headers=headers
        )
        with self.assertRaises(InvalidRequest) as cm:
            SigV4Request(req.environ)
        self.assertIn('Expecting a single x-amz-checksum- header',
                      str(cm.exception.body))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_sig_v4_strm_unsgnd_pyld_trl_checksum_algo_mismatch(self):
        wsgi_input = io.BytesIO(b'123')
        self.assertEqual(0, wsgi_input.tell())
        headers = {
            'x-amz-sdk-checksum-algorithm': 'crc32',
            'x-amz-checksum-sha256':
                'EBCn52FhCYCsWRNZyHH3JN4VDyNEDrtZWaxMByTJHZE=',
        }
        req = self._make_sig_v4_streaming_unsigned_payload_trailer_req(
            wsgi_input=wsgi_input,
            extra_headers=headers
        )
        with self.assertRaises(InvalidRequest) as cm:
            SigV4Request(req.environ)
        self.assertIn('Value for x-amz-sdk-checksum-algorithm header is '
                      'invalid.', str(cm.exception.body))


class TestSigV4Request(S3ApiTestCase):
    def setUp(self):
        super(TestSigV4Request, self).setUp()
        self.s3api.conf.s3_acl = True

    def test_init_header_authorization(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'GET'}

        def do_check_ok(conf, auth):
            x_amz_date = self.get_v4_amz_date_header()
            headers = {
                'Authorization': auth,
                'X-Amz-Content-SHA256': '0' * 64,
                'Date': self.get_date_header(),
                'X-Amz-Date': x_amz_date}
            req = Request.blank('/', environ=environ, headers=headers)
            sigv4_req = SigV4Request(req.environ, conf=conf)
            self.assertEqual('X', sigv4_req.signature)
            self.assertEqual('test', sigv4_req.access_key)
            return sigv4_req

        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        # location lowercase matches
        sigv4_req = do_check_ok(Config({'location': 'us-east-1'}), auth)
        self.assertEqual('us-east-1', sigv4_req.location)
        # location case mis-matches
        sigv4_req = do_check_ok(Config({'location': 'US-East-1'}), auth)
        self.assertEqual('us-east-1', sigv4_req.location)
        # location uppercase matches
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/US-East-1/s3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        sigv4_req = do_check_ok(Config({'location': 'US-East-1'}), auth)
        self.assertEqual('US-East-1', sigv4_req.location)

        def do_check_bad(conf, auth, exc):
            x_amz_date = self.get_v4_amz_date_header()
            headers = {
                'Authorization': auth,
                'X-Amz-Content-SHA256': '0' * 64,
                'Date': self.get_date_header(),
                'X-Amz-Date': x_amz_date}
            req = Request.blank('/', environ=environ, headers=headers)
            self.assertRaises(exc, SigV4Request, req.environ, conf)

        # location case mismatch
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/US-East-1/s3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), auth,
                     s3response.AuthorizationHeaderMalformed)
        # bad location
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-west-1/s3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), auth,
                     s3response.AuthorizationHeaderMalformed)
        # bad service name
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/S3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), auth,
                     s3response.AuthorizationHeaderMalformed)
        # bad terminal name
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/AWS4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), auth,
                     s3response.AuthorizationHeaderMalformed)
        # bad Signature
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=' % self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), auth,
                     s3response.AccessDenied)
        # bad SignedHeaders
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request,'
                'SignedHeaders=,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), auth,
                     s3response.AuthorizationHeaderMalformed)

    def test_init_query_authorization(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'GET'}

        def do_check_ok(conf, params):
            x_amz_date = self.get_v4_amz_date_header()
            params['X-Amz-Date'] = x_amz_date
            signed_headers = {
                'X-Amz-Content-SHA256': '0' * 64,
                'Date': self.get_date_header(),
                'X-Amz-Date': x_amz_date}
            req = Request.blank('/', environ=environ, headers=signed_headers,
                                params=params)
            sigv4_req = SigV4Request(req.environ, conf=conf)
            self.assertEqual('X', sigv4_req.signature)
            self.assertEqual('test', sigv4_req.access_key)
            return sigv4_req

        ok_params = {
            'AWSAccessKeyId': 'test',
            'X-Amz-Expires': '3600',
            'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
            'X-Amz-Credential': 'test/%s/us-east-1/s3/aws4_request' %
            self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-SignedHeaders': 'host;x-amz-content-sha256;x-amz-date',
            'X-Amz-Signature': 'X'}
        # location lowercase matches
        sigv4_req = do_check_ok(Config({'location': 'us-east-1'}), ok_params)
        self.assertEqual('us-east-1', sigv4_req.location)
        # location case mis-matches
        sigv4_req = do_check_ok(Config({'location': 'US-East-1'}), ok_params)
        self.assertEqual('us-east-1', sigv4_req.location)
        # location uppercase matches
        ok_params['X-Amz-Credential'] = (
            'test/%s/US-East-1/s3/aws4_request' %
            self.get_v4_amz_date_header().split('T', 1)[0])
        sigv4_req = do_check_ok(Config({'location': 'US-East-1'}), ok_params)
        self.assertEqual('US-East-1', sigv4_req.location)

        def do_check_bad(conf, params, exc):
            x_amz_date = self.get_v4_amz_date_header()
            params['X-Amz-Date'] = x_amz_date
            signed_headers = {
                'X-Amz-Content-SHA256': '0' * 64,
                'Date': self.get_date_header(),
                'X-Amz-Date': x_amz_date}
            req = Request.blank('/', environ=environ, headers=signed_headers,
                                params=params)
            self.assertRaises(exc, SigV4Request, req.environ, conf)

        # location case mismatch
        bad_params = dict(ok_params)
        bad_params['X-Amz-Credential'] = (
            'test/%s/US-East-1/s3/aws4_request' %
            self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), bad_params,
                     s3response.AuthorizationQueryParametersError)
        # bad location
        bad_params = dict(ok_params)
        bad_params['X-Amz-Credential'] = (
            'test/%s/us-west-1/s3/aws4_request' %
            self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), bad_params,
                     s3response.AuthorizationQueryParametersError)
        # bad service name
        bad_params = dict(ok_params)
        bad_params['X-Amz-Credential'] = (
            'test/%s/us-east-1/S3/aws4_request' %
            self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), bad_params,
                     s3response.AuthorizationQueryParametersError)
        # bad terminal name
        bad_params = dict(ok_params)
        bad_params['X-Amz-Credential'] = (
            'test/%s/us-east-1/s3/AWS4_request' %
            self.get_v4_amz_date_header().split('T', 1)[0])
        do_check_bad(Config({'location': 'us-east-1'}), bad_params,
                     s3response.AuthorizationQueryParametersError)
        # bad Signature
        bad_params = dict(ok_params)
        bad_params['X-Amz-Signature'] = ''
        do_check_bad(Config({'location': 'us-east-1'}), bad_params,
                     s3response.AccessDenied)
        # bad SignedHeaders
        bad_params = dict(ok_params)
        bad_params['X-Amz-SignedHeaders'] = ''
        do_check_bad(Config({'location': 'us-east-1'}), bad_params,
                     s3response.AuthorizationQueryParametersError)

    def test_controller_allow_multipart_uploads(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'GET'}
        x_amz_date = self.get_v4_amz_date_header()
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        headers = {
            'Authorization': auth,
            'X-Amz-Content-SHA256': '0' * 64,
            'Date': self.get_date_header(),
            'X-Amz-Date': x_amz_date}

        def make_s3req(config, path, params):
            req = Request.blank(path, environ=environ, headers=headers,
                                params=params)
            return SigV4Request(req.environ, None, config)

        s3req = make_s3req(Config(), '/bkt', {'partNumber': '3'})
        self.assertEqual(controllers.ObjectController,
                         s3req.controller)

        s3req = make_s3req(Config(), '/bkt', {'uploadId': '4'})
        self.assertEqual(controllers.multi_upload.UploadController,
                         s3req.controller)

        s3req = make_s3req(Config(), '/bkt', {'uploads': '99'})
        self.assertEqual(controllers.multi_upload.UploadsController,
                         s3req.controller)

        # multi part requests require allow_multipart_uploads
        def do_check_slo_not_enabled(params):
            s3req = make_s3req(Config({
                'allow_multipart_uploads': False}), '/bkt', params)
            self.assertRaises(s3response.S3NotImplemented,
                              lambda: s3req.controller)

        do_check_slo_not_enabled({'partNumber': '3'})
        do_check_slo_not_enabled({'uploadId': '4'})
        do_check_slo_not_enabled({'uploads': '99'})

        # service requests not dependent on allow_multipart_uploads
        s3req = make_s3req(Config(), '/', {'partNumber': '3'})
        self.assertEqual(controllers.ServiceController,
                         s3req.controller)
        s3req = make_s3req(Config({'allow_multipart_uploads': False}), '/',
                           {'partNumber': '3'})
        self.assertEqual(controllers.ServiceController,
                         s3req.controller)

    def test_controller_for_multipart_upload_requests(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'PUT'}
        x_amz_date = self.get_v4_amz_date_header()
        auth = ('AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request,'
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0])
        headers = {
            'Authorization': auth,
            'X-Amz-Content-SHA256': '0' * 64,
            'Date': self.get_date_header(),
            'X-Amz-Date': x_amz_date}

        def make_s3req(config, path, params):
            req = Request.blank(path, environ=environ, headers=headers,
                                params=params)
            return SigV4Request(req.environ, None, config)

        s3req = make_s3req(Config(), '/bkt', {'partNumber': '3',
                                              'uploadId': '4'})
        self.assertEqual(controllers.multi_upload.PartController,
                         s3req.controller)

        s3req = make_s3req(Config(), '/bkt', {'partNumber': '3'})
        self.assertEqual(controllers.multi_upload.PartController,
                         s3req.controller)

        s3req = make_s3req(Config(), '/bkt', {'uploadId': '4',
                                              'partNumber': '3',
                                              'copySource': 'bkt2/obj2'})
        self.assertEqual(controllers.multi_upload.PartController,
                         s3req.controller)


class TestHashingInput(S3ApiTestCase):
    def test_good(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 9, hashlib.sha256(raw).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # trying to read past the end gets us whatever's left
        self.assertEqual(b'789', wrapped.read(4))
        # can continue trying to read -- but it'll be empty
        self.assertEqual(b'', wrapped.read(2))

        self.assertFalse(wrapped.wsgi_input.closed)
        wrapped.close()
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_good_readline(self):
        raw = b'12345\n6789'
        wrapped = HashingInput(
            BytesIO(raw), 10, hashlib.sha256(raw).hexdigest())
        self.assertEqual(b'12345\n', wrapped.readline())
        self.assertEqual(b'6789', wrapped.readline())
        self.assertEqual(b'', wrapped.readline())

    def test_empty(self):
        wrapped = HashingInput(
            BytesIO(b''), 0, hashlib.sha256(b'').hexdigest())
        self.assertEqual(b'', wrapped.read(4))
        self.assertEqual(b'', wrapped.read(2))

        self.assertFalse(wrapped.wsgi_input.closed)
        wrapped.close()
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_too_long(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 8, hashlib.sha256(raw).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # even though the hash matches, there was more data than we expected
        with self.assertRaises(S3InputSHA256Mismatch) as raised:
            wrapped.read(3)
        self.assertIsInstance(raised.exception, BaseException)
        # won't get caught by most things in a pipeline
        self.assertNotIsInstance(raised.exception, Exception)
        # the error causes us to close the input
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_too_short_read_piecemeal(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 10, hashlib.sha256(raw).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56789', wrapped.read(5))
        # even though the hash matches, there was less data than we expected
        with self.assertRaises(S3InputSHA256Mismatch):
            wrapped.read(1)
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_too_short_read_all(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 10, hashlib.sha256(raw).hexdigest())
        with self.assertRaises(S3InputSHA256Mismatch):
            wrapped.read()
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_bad_hash(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 9, hashlib.sha256().hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'5678', wrapped.read(4))
        with self.assertRaises(S3InputSHA256Mismatch):
            wrapped.read(4)
        self.assertTrue(wrapped.wsgi_input.closed)

    def test_empty_bad_hash(self):
        _input = BytesIO(b'')
        self.assertFalse(_input.closed)
        with self.assertRaises(XAmzContentSHA256Mismatch):
            # Don't even get a chance to try to read it
            HashingInput(_input, 0, 'nope')
        self.assertTrue(_input.closed)

    def test_bad_hash_readline(self):
        raw = b'12345\n6789'
        wrapped = HashingInput(
            BytesIO(raw), 10, hashlib.sha256(raw[:-3]).hexdigest())
        self.assertEqual(b'12345\n', wrapped.readline())
        with self.assertRaises(S3InputSHA256Mismatch):
            self.assertEqual(b'6789', wrapped.readline())


class TestChunkReader(unittest.TestCase):
    def test_read_sig_checker_ok(self):
        raw = '123456789\r\n0;chunk-signature=ok\r\n\r\n'.encode('utf8')

        mock_validator = MagicMock(return_value=True)
        bytes_input = BytesIO(raw)
        reader = ChunkReader(
            bytes_input, 9, mock_validator, 'chunk-signature=signature')
        self.assertEqual(9, reader.to_read)
        self.assertEqual(b'123456789', reader.read())
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'123456789').hexdigest(), 'signature')],
            mock_validator.call_args_list)
        self.assertFalse(bytes_input.closed)

        mock_validator = MagicMock(return_value=True)
        reader = ChunkReader(
            BytesIO(raw), 9, mock_validator, 'chunk-signature=signature')
        self.assertEqual(9, reader.to_read)
        self.assertEqual(b'12345678', reader.read(8))
        self.assertEqual(1, reader.to_read)
        self.assertEqual(b'9', reader.read(8))
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'123456789').hexdigest(), 'signature')],
            mock_validator.call_args_list)

        mock_validator = MagicMock(return_value=True)
        reader = ChunkReader(
            BytesIO(raw), 9, mock_validator, 'chunk-signature=signature')
        self.assertEqual(9, reader.to_read)
        self.assertEqual(b'123456789', reader.read(10))
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'123456789').hexdigest(), 'signature')],
            mock_validator.call_args_list)

        mock_validator = MagicMock(return_value=True)
        reader = ChunkReader(
            BytesIO(raw), 9, mock_validator, 'chunk-signature=signature')
        self.assertEqual(9, reader.to_read)
        self.assertEqual(b'123456789', reader.read(-1))
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'123456789').hexdigest(), 'signature')],
            mock_validator.call_args_list)

    def test_read_sig_checker_bad(self):
        raw = '123456789\r\n0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        mock_validator = MagicMock(return_value=False)
        bytes_input = BytesIO(raw)
        reader = ChunkReader(
            bytes_input, 9, mock_validator, 'chunk-signature=signature')
        reader.read(8)
        self.assertEqual(1, reader.to_read)
        with self.assertRaises(S3InputChunkSignatureMismatch):
            reader.read(1)
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'123456789').hexdigest(), 'signature')],
            mock_validator.call_args_list)
        self.assertTrue(bytes_input.closed)

    def test_read_no_sig_checker(self):
        raw = '123456789\r\n0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        bytes_input = BytesIO(raw)
        reader = ChunkReader(bytes_input, 9, None, None)
        self.assertEqual(9, reader.to_read)
        self.assertEqual(b'123456789', reader.read())
        self.assertEqual(0, reader.to_read)
        self.assertFalse(bytes_input.closed)

    def test_readline_sig_checker_ok_newline_is_midway_through_chunk(self):
        raw = '123456\n7\r\n0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        mock_validator = MagicMock(return_value=True)
        bytes_input = BytesIO(raw)
        reader = ChunkReader(
            bytes_input, 8, mock_validator, 'chunk-signature=signature')
        self.assertEqual(8, reader.to_read)
        self.assertEqual(b'123456\n', reader.readline())
        self.assertEqual(1, reader.to_read)
        self.assertEqual(b'7', reader.readline())
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'123456\n7').hexdigest(), 'signature')],
            mock_validator.call_args_list)
        self.assertFalse(bytes_input.closed)

    def test_readline_sig_checker_ok_newline_is_end_of_chunk(self):
        raw = '1234567\n\r\n0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        mock_validator = MagicMock(return_value=True)
        bytes_input = BytesIO(raw)
        reader = ChunkReader(
            bytes_input, 8, mock_validator, 'chunk-signature=signature')
        self.assertEqual(8, reader.to_read)
        self.assertEqual(b'1234567\n', reader.readline())
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'1234567\n').hexdigest(), 'signature')],
            mock_validator.call_args_list)
        self.assertFalse(bytes_input.closed)

    def test_readline_sig_checker_ok_partial_line_read(self):
        raw = '1234567\n\r\n0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        mock_validator = MagicMock(return_value=True)
        bytes_input = BytesIO(raw)
        reader = ChunkReader(
            bytes_input, 8, mock_validator, 'chunk-signature=signature')
        self.assertEqual(8, reader.to_read)
        self.assertEqual(b'12345', reader.readline(5))
        self.assertEqual(3, reader.to_read)
        self.assertEqual(b'67', reader.readline(2))
        self.assertEqual(1, reader.to_read)
        self.assertEqual(b'\n', reader.readline())
        self.assertEqual(0, reader.to_read)
        self.assertEqual(
            [mock.call(hashlib.sha256(b'1234567\n').hexdigest(), 'signature')],
            mock_validator.call_args_list)
        self.assertFalse(bytes_input.closed)


class TestStreamingInput(S3ApiTestCase):
    def setUp(self):
        super(TestStreamingInput, self).setUp()
        # Override chunk min size
        s3request.SIGV4_CHUNK_MIN_SIZE = 2
        self.fake_sig_checker = MagicMock()
        self.fake_sig_checker.check_chunk_signature = \
            lambda chunk, signature: signature == 'ok'

    def test_read(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        self.assertEqual(b'123456789', wrapped.read())
        self.assertFalse(wrapped._input.closed)
        wrapped.close()
        self.assertTrue(wrapped._input.closed)

    def test_read_with_size(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # trying to read past the end gets us whatever's left
        self.assertEqual(b'789', wrapped.read(4))
        # can continue trying to read -- but it'll be empty
        self.assertEqual(b'', wrapped.read(2))

        self.assertFalse(wrapped._input.closed)
        wrapped.close()
        self.assertTrue(wrapped._input.closed)

    def test_read_multiple_chunks(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '7;chunk-signature=ok\r\nabc\ndef\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 16, set(),
                                 self.fake_sig_checker)
        self.assertEqual(b'123456789abc\ndef', wrapped.read())
        self.assertEqual(b'', wrapped.read(2))

    def test_read_multiple_chunks_with_size(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '7;chunk-signature=ok\r\nabc\ndef\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 16, set(),
                                 self.fake_sig_checker)
        self.assertEqual(b'123456789a', wrapped.read(10))
        self.assertEqual(b'bc\n', wrapped.read(3))
        self.assertEqual(b'def', wrapped.read(4))
        self.assertEqual(b'', wrapped.read(2))

    def test_readline_newline_in_middle_and_at_end(self):
        raw = 'a;chunk-signature=ok\r\n123456\n789\r\n' \
              '4;chunk-signature=ok\r\nabc\n\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 14, set(),
                                 self.fake_sig_checker)
        self.assertEqual(b'123456\n', wrapped.readline())
        self.assertEqual(b'789abc\n', wrapped.readline())
        self.assertEqual(b'', wrapped.readline())

    def test_readline_newline_in_middle_not_at_end(self):
        raw = 'a;chunk-signature=ok\r\n123456\n789\r\n' \
              '3;chunk-signature=ok\r\nabc\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 13, set(),
                                 self.fake_sig_checker)
        self.assertEqual(b'123456\n', wrapped.readline())
        self.assertEqual(b'789abc', wrapped.readline())
        self.assertEqual(b'', wrapped.readline())

    def test_readline_no_newline(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '3;chunk-signature=ok\r\nabc\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 12, set(),
                                 self.fake_sig_checker)
        self.assertEqual(b'123456789abc', wrapped.readline())
        self.assertEqual(b'', wrapped.readline())

    def test_readline_line_spans_chunks(self):
        raw = '9;chunk-signature=ok\r\nblah\nblah\r\n' \
              '9;chunk-signature=ok\r\n123456789\r\n' \
              '7;chunk-signature=ok\r\nabc\ndef\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 25, set(),
                                 self.fake_sig_checker)
        self.assertEqual(b'blah\n', wrapped.readline())
        self.assertEqual(b'blah123456789abc\n', wrapped.readline())
        self.assertEqual(b'def', wrapped.readline())

    def test_readline_with_size_line_spans_chunks(self):
        raw = '9;chunk-signature=ok\r\nblah\nblah\r\n' \
              '9;chunk-signature=ok\r\n123456789\r\n' \
              '7;chunk-signature=ok\r\nabc\ndef\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 25, set(),
                                 self.fake_sig_checker)
        self.assertEqual(b'blah\n', wrapped.readline(8))
        self.assertEqual(b'blah123456789a', wrapped.readline(14))
        self.assertEqual(b'bc\n', wrapped.readline(99))
        self.assertEqual(b'def', wrapped.readline(99))

    def test_chunk_separator_missing(self):
        raw = '9;chunk-signature=ok\r\n123456789' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        with self.assertRaises(s3request.S3InputIncomplete):
            wrapped.read()
        self.assertTrue(wrapped._input.closed)

    def test_final_newline_missing(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '0;chunk-signature=ok\r\n\r'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        with self.assertRaises(s3request.S3InputIncomplete):
            wrapped.read()
        self.assertTrue(wrapped._input.closed)

    def test_trailing_garbage_ok(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '0;chunk-signature=ok\r\n\r\ngarbage'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        self.assertEqual(b'123456789', wrapped.read())

    def test_good_with_trailers(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '0;chunk-signature=ok\r\n' \
              'x-amz-checksum-crc32: AAAAAA==\r\n'.encode('utf8')
        wrapped = StreamingInput(
            BytesIO(raw), 9, {'x-amz-checksum-crc32'}, self.fake_sig_checker)
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # not at end, trailers haven't been read
        self.assertEqual({}, wrapped.trailers)
        # if we get exactly to the end, we go ahead and read the trailers
        self.assertEqual(b'789', wrapped.read(3))
        self.assertEqual({'x-amz-checksum-crc32': 'AAAAAA=='},
                         wrapped.trailers)
        # can continue trying to read -- but it'll be empty
        self.assertEqual(b'', wrapped.read(2))
        self.assertEqual({'x-amz-checksum-crc32': 'AAAAAA=='},
                         wrapped.trailers)

        self.assertFalse(wrapped._input.closed)
        wrapped.close()
        self.assertTrue(wrapped._input.closed)

    def test_unexpected_trailers(self):
        def do_test(raw):
            wrapped = StreamingInput(
                BytesIO(raw), 9, {'x-amz-checksum-crc32'},
                self.fake_sig_checker)
            with self.assertRaises(s3request.S3InputMalformedTrailer):
                wrapped.read()
            self.assertTrue(wrapped._input.closed)

        do_test('9;chunk-signature=ok\r\n123456789\r\n'
                '0;chunk-signature=ok\r\n'
                'x-amz-checksum-sha256: value\r\n'.encode('utf8'))
        do_test('9;chunk-signature=ok\r\n123456789\r\n'
                '0;chunk-signature=ok\r\n'
                'x-amz-checksum-crc32=value\r\n'.encode('utf8'))
        do_test('9;chunk-signature=ok\r\n123456789\r\n'
                '0;chunk-signature=ok\r\n'
                'x-amz-checksum-crc32\r\n'.encode('utf8'))

    def test_wrong_signature_first_chunk(self):
        raw = '9;chunk-signature=ko\r\n123456789\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        # Can read while in the chunk...
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'5678', wrapped.read(4))
        # But once we hit the end, bomb out
        with self.assertRaises(s3request.S3InputChunkSignatureMismatch):
            wrapped.read(4)
        self.assertTrue(wrapped._input.closed)

    def test_wrong_signature_middle_chunk(self):
        raw = '2;chunk-signature=ok\r\n12\r\n' \
              '2;chunk-signature=ok\r\n34\r\n' \
              '2;chunk-signature=ko\r\n56\r\n' \
              '2;chunk-signature=ok\r\n78\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        self.assertEqual(b'1234', wrapped.read(4))
        with self.assertRaises(s3request.S3InputChunkSignatureMismatch):
            wrapped.read(4)
        self.assertTrue(wrapped._input.closed)

    def test_wrong_signature_last_chunk(self):
        raw = '2;chunk-signature=ok\r\n12\r\n' \
              '2;chunk-signature=ok\r\n34\r\n' \
              '2;chunk-signature=ok\r\n56\r\n' \
              '2;chunk-signature=ok\r\n78\r\n' \
              '0;chunk-signature=ko\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        self.assertEqual(b'12345678', wrapped.read(8))
        with self.assertRaises(s3request.S3InputChunkSignatureMismatch):
            wrapped.read(4)
        self.assertTrue(wrapped._input.closed)

    def test_not_enough_content(self):
        raw = '9;chunk-signature=ok\r\n123456789\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(
            BytesIO(raw), 33, set(), self.fake_sig_checker)
        with self.assertRaises(s3request.S3InputSizeError) as cm:
            wrapped.read()
        self.assertEqual(33, cm.exception.expected)
        self.assertEqual(9, cm.exception.provided)
        self.assertTrue(wrapped._input.closed)

    def test_wrong_chunk_size(self):
        # first chunk should be size 9 not a
        raw = 'a;chunk-signature=ok\r\n123456789\r\n' \
            '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        with self.assertRaises(s3request.S3InputSizeError) as cm:
            wrapped.read(4)
        self.assertEqual(9, cm.exception.expected)
        self.assertEqual(10, cm.exception.provided)
        self.assertTrue(wrapped._input.closed)

    def test_small_first_chunk_size(self):
        raw = '1;chunk-signature=ok\r\n1\r\n' \
              '8;chunk-signature=ok\r\n23456789\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        with self.assertRaises(s3request.S3InputChunkTooSmall) as cm:
            wrapped.read(4)
        # note: the chunk number is the one *after* the short chunk
        self.assertEqual(2, cm.exception.chunk_number)
        self.assertEqual(1, cm.exception.bad_chunk_size)
        self.assertTrue(wrapped._input.closed)

    def test_small_final_chunk_size_ok(self):
        raw = '8;chunk-signature=ok\r\n12345678\r\n' \
              '1;chunk-signature=ok\r\n9\r\n' \
              '0;chunk-signature=ok\r\n\r\n'.encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), self.fake_sig_checker)
        self.assertEqual(b'123456789', wrapped.read())

    def test_invalid_chunk_size(self):
        # the actual chunk data doesn't need to match the length in the
        # chunk header for the test
        raw = ('-1;chunk-signature=ok\r\n123456789\r\n'
               '0;chunk-signature=ok\r\n\r\n').encode('utf8')
        wrapped = StreamingInput(BytesIO(raw), 9, set(), None)
        with self.assertRaises(s3request.S3InputIncomplete) as cm:
            wrapped.read(4)
        self.assertIn('invalid chunk header', str(cm.exception))
        self.assertTrue(wrapped._input.closed)

    def test_invalid_chunk_params(self):
        def do_test(params, exp_exception):
            raw = ('9;%s\r\n123456789\r\n'
                   '0;chunk-signature=ok\r\n\r\n' % params).encode('utf8')
            wrapped = StreamingInput(BytesIO(raw), 9, set(), MagicMock())
            with self.assertRaises(exp_exception):
                wrapped.read(4)
            self.assertTrue(wrapped._input.closed)

        do_test('chunk-signature=', s3request.S3InputIncomplete)
        do_test('chunk-signature=ok;not-ok', s3request.S3InputIncomplete)
        do_test('chunk-signature=ok;chunk-signature=ok',
                s3request.S3InputIncomplete)
        do_test('chunk-signature', s3request.S3InputIncomplete)
        # note: underscore not hyphen...
        do_test('chunk_signature=ok', s3request.S3InputChunkSignatureMismatch)
        do_test('skunk-cignature=ok', s3request.S3InputChunkSignatureMismatch)


class TestModuleFunctions(unittest.TestCase):
    def test_get_checksum_hasher(self):
        def do_test(crc):
            hasher = _get_checksum_hasher('x-amz-checksum-%s' % crc)
            self.assertEqual(crc, hasher.name)

        do_test('crc32')
        do_test('crc32c')
        do_test('sha1')
        do_test('sha256')
        try:
            checksum._select_crc64nvme_impl()
        except NotImplementedError:
            pass
        else:
            do_test('crc64nvme')

    def test_get_checksum_hasher_invalid(self):
        def do_test(crc):
            with self.assertRaises(s3response.S3NotImplemented):
                _get_checksum_hasher('x-amz-checksum-%s' % crc)

        with mock.patch.object(checksum, '_select_crc64nvme_impl',
                               side_effect=NotImplementedError):
            do_test('crc64nvme')
        do_test('nonsense')
        do_test('')


if __name__ == '__main__':
    unittest.main()
