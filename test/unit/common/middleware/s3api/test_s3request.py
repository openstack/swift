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

from datetime import datetime, timedelta
import hashlib
from mock import patch, MagicMock
import unittest

from io import BytesIO

from swift.common import swob
from swift.common.middleware.s3api import s3response, controllers
from swift.common.swob import Request, HTTPNoContent
from swift.common.middleware.s3api.utils import mktime, Config
from swift.common.middleware.s3api.acl_handlers import get_acl_handler
from swift.common.middleware.s3api.subresource import ACL, User, Owner, \
    Grant, encode_acl
from test.unit.common.middleware.s3api.test_s3api import S3ApiTestCase
from swift.common.middleware.s3api.s3request import S3Request, \
    S3AclRequest, SigV4Request, SIGV4_X_AMZ_DATE_FORMAT, HashingInput
from swift.common.middleware.s3api.s3response import InvalidArgument, \
    NoSuchBucket, InternalError, \
    AccessDenied, SignatureDoesNotMatch, RequestTimeTooSkewed, BadDigest
from swift.common.utils import md5

from test.debug_logger import debug_logger

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
        self.swift.s3_acl = True

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
            return mock_get_resp, m_check_permission,\
                s3_req.get_response(self.s3api)

    def test_get_response_without_s3_acl(self):
        self.s3api.conf.s3_acl = False
        self.swift.s3_acl = False
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

        expected_errors = [(404, NoSuchBucket), (0, InternalError)]
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
            'X-Amz-Content-SHA256': '0123456789'}

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
            datetime.utcnow() - timedelta(minutes=10)
        )}
        self._test_request_timestamp_sigv4(date_header)

        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            datetime.utcnow() - timedelta(minutes=10)
        )}
        with self.assertRaises(RequestTimeTooSkewed) as cm, \
                patch.object(self.s3api.conf, 'allowable_clock_skew', 300):
            self._test_request_timestamp_sigv4(date_header)

        # near-future X-Amz-Date headers
        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            datetime.utcnow() + timedelta(minutes=10)
        )}
        self._test_request_timestamp_sigv4(date_header)

        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            datetime.utcnow() + timedelta(minutes=10)
        )}
        with self.assertRaises(RequestTimeTooSkewed) as cm, \
                patch.object(self.s3api.conf, 'allowable_clock_skew', 300):
            self._test_request_timestamp_sigv4(date_header)

        date_header = {'X-Amz-Date': self.get_v4_amz_date_header(
            datetime.utcnow() + timedelta(days=1)
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
            'X-Amz-Content-SHA256': '0123456789',
            'Date': self.get_date_header(),
            'X-Amz-Date': x_amz_date}

        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)

        headers_to_sign = sigv4_req._headers_to_sign()
        self.assertEqual(headers_to_sign, [
            ('host', 'localhost:80'),
            ('x-amz-content-sha256', '0123456789'),
            ('x-amz-date', x_amz_date)])

        # no x-amz-date
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-Content-SHA256': '0123456789',
            'Date': self.get_date_header()}

        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)

        headers_to_sign = sigv4_req._headers_to_sign()
        self.assertEqual(headers_to_sign, [
            ('host', 'localhost:80'),
            ('x-amz-content-sha256', '0123456789')])

        # SignedHeaders says, host and x-amz-date included but there is not
        # X-Amz-Date header
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % self.get_v4_amz_date_header().split('T', 1)[0],
            'X-Amz-Content-SHA256': '0123456789',
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
            req.environ, conf=Config({'storage_domain': 's3.test.com'}))
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/obj1', environ=environ, headers=headers)
        sigv2_req = S3Request(
            req.environ, conf=Config({'storage_domain': 's3.test.com'}))
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/obj1')
        self.assertEqual(req.environ['PATH_INFO'], '/obj1')

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
            'X-Amz-Content-SHA256': '0123456789',
            'Date': self.get_date_header(),
            'X-Amz-Date': x_amz_date}

        # Virtual hosted-style
        self.s3api.conf.storage_domain = 's3.test.com'
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
        self.s3api.conf.storage_domain = ''
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
            'storage_domain': 's3.amazonaws.com'}))
        expected_sts = b'\n'.join([
            b'GET',
            b'',
            b'',
            b'Tue, 27 Mar 2007 19:36:42 +0000',
            b'/johnsmith/photos/puppy.jpg',
        ])
        self.assertEqual(expected_sts, sigv2_req._string_to_sign())
        self.assertTrue(sigv2_req.check_signature(secret))

        req = Request.blank('/photos/puppy.jpg', method='PUT', headers={
            'Content-Type': 'image/jpeg',
            'Content-Length': '94328',
            'Host': 'johnsmith.s3.amazonaws.com',
            'Date': 'Tue, 27 Mar 2007 21:15:45 +0000',
            'Authorization': ('AWS AKIAIOSFODNN7EXAMPLE:'
                              'MyyxeRY7whkBe+bq8fHCL/2kKUg='),
        })
        sigv2_req = S3Request(req.environ, conf=Config({
            'storage_domain': 's3.amazonaws.com'}))
        expected_sts = b'\n'.join([
            b'PUT',
            b'',
            b'image/jpeg',
            b'Tue, 27 Mar 2007 21:15:45 +0000',
            b'/johnsmith/photos/puppy.jpg',
        ])
        self.assertEqual(expected_sts, sigv2_req._string_to_sign())
        self.assertTrue(sigv2_req.check_signature(secret))

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
            'storage_domain': 's3.amazonaws.com'}))
        expected_sts = b'\n'.join([
            b'GET',
            b'',
            b'',
            b'Tue, 27 Mar 2007 19:42:41 +0000',
            b'/johnsmith/',
        ])
        self.assertEqual(expected_sts, sigv2_req._string_to_sign())
        self.assertTrue(sigv2_req.check_signature(secret))

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
            'storage_domain': 's3.amazonaws.com'}))
        # This is a failure case with utf-8 non-ascii multi-bytes charactor
        # but we expect to return just False instead of exceptions
        self.assertFalse(sigv2_req.check_signature(
            u'\u30c9\u30e9\u30b4\u30f3'))

        # Test v4 check_signature with multi bytes invalid secret
        amz_date_header = self.get_v4_amz_date_header()
        req = Request.blank('/photos/puppy.jpg', headers={
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=X' % amz_date_header.split('T', 1)[0],
            'X-Amz-Content-SHA256': '0123456789',
            'X-Amz-Date': amz_date_header
        })
        sigv4_req = SigV4Request(
            req.environ, Config({'storage_domain': 's3.amazonaws.com'}))
        self.assertFalse(sigv4_req.check_signature(
            u'\u30c9\u30e9\u30b4\u30f3'))

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
        self.s3api.conf.storage_domain = 's3.test.com'
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        self.assertTrue(
            sigv4_req._canonical_request().endswith(b'UNSIGNED-PAYLOAD'))
        self.assertTrue(sigv4_req.check_signature('secret'))

    @patch.object(S3Request, '_validate_dates', lambda *a: None)
    def test_check_sigv4_req_zero_content_length_sha256(self):
        # Virtual hosted-style
        self.s3api.conf.storage_domain = 's3.test.com'

        # bad sha256
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
            'X-Amz-Content-SHA256': 'bad',
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
        }

        # lowercase sha256
        req = Request.blank('/', environ=environ, headers=headers)
        self.assertRaises(BadDigest, SigV4Request, req.environ)
        sha256_of_nothing = hashlib.sha256().hexdigest().encode('ascii')
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=d90542e8b4c0d2f803162040a948e8e51db00b62a59ffb16682'
                'ef433718fde12',
            'X-Amz-Content-SHA256': sha256_of_nothing,
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
        }
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        self.assertTrue(
            sigv4_req._canonical_request().endswith(sha256_of_nothing))
        self.assertTrue(sigv4_req.check_signature('secret'))

        # uppercase sha256
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/20210104/us-east-1/s3/aws4_request, '
                'SignedHeaders=host;x-amz-content-sha256;x-amz-date,'
                'Signature=4aab5102e58e9e40f331417d322465c24cac68a7ce77260e9bf'
                '5ce9a6200862b',
            'X-Amz-Content-SHA256': sha256_of_nothing.upper(),
            'Date': 'Mon, 04 Jan 2021 10:26:23 -0000',
            'X-Amz-Date': '20210104T102623Z',
            'Content-Length': 0,
        }
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        self.assertTrue(
            sigv4_req._canonical_request().endswith(sha256_of_nothing.upper()))
        self.assertTrue(sigv4_req.check_signature('secret'))


class TestSigV4Request(S3ApiTestCase):
    def setUp(self):
        super(TestSigV4Request, self).setUp()
        self.s3api.conf.s3_acl = True
        self.swift.s3_acl = True

    def test_init_header_authorization(self):
        environ = {
            'HTTP_HOST': 'bucket.s3.test.com',
            'REQUEST_METHOD': 'GET'}

        def do_check_ok(conf, auth):
            x_amz_date = self.get_v4_amz_date_header()
            headers = {
                'Authorization': auth,
                'X-Amz-Content-SHA256': '0123456789',
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
                'X-Amz-Content-SHA256': '0123456789',
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
                'X-Amz-Content-SHA256': '0123456789',
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
                'X-Amz-Content-SHA256': '0123456789',
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
            'X-Amz-Content-SHA256': '0123456789',
            'Date': self.get_date_header(),
            'X-Amz-Date': x_amz_date}

        def make_s3req(config, path, params):
            req = Request.blank(path, environ=environ, headers=headers,
                                params=params)
            return SigV4Request(req.environ, None, config)

        s3req = make_s3req(Config(), '/bkt', {'partNumber': '3'})
        self.assertEqual(controllers.multi_upload.PartController,
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


class TestHashingInput(S3ApiTestCase):
    def test_good(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 9, lambda: md5(usedforsecurity=False),
            md5(raw, usedforsecurity=False).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # trying to read past the end gets us whatever's left
        self.assertEqual(b'789', wrapped.read(4))
        # can continue trying to read -- but it'll be empty
        self.assertEqual(b'', wrapped.read(2))

        self.assertFalse(wrapped._input.closed)
        wrapped.close()
        self.assertTrue(wrapped._input.closed)

    def test_empty(self):
        wrapped = HashingInput(BytesIO(b''), 0, hashlib.sha256,
                               hashlib.sha256(b'').hexdigest())
        self.assertEqual(b'', wrapped.read(4))
        self.assertEqual(b'', wrapped.read(2))

        self.assertFalse(wrapped._input.closed)
        wrapped.close()
        self.assertTrue(wrapped._input.closed)

    def test_too_long(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 8, lambda: md5(usedforsecurity=False),
            md5(raw, usedforsecurity=False).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # even though the hash matches, there was more data than we expected
        with self.assertRaises(swob.HTTPException) as raised:
            wrapped.read(3)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        # the error causes us to close the input
        self.assertTrue(wrapped._input.closed)

    def test_too_short(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 10, lambda: md5(usedforsecurity=False),
            md5(raw, usedforsecurity=False).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # even though the hash matches, there was more data than we expected
        with self.assertRaises(swob.HTTPException) as raised:
            wrapped.read(4)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        self.assertTrue(wrapped._input.closed)

    def test_bad_hash(self):
        raw = b'123456789'
        wrapped = HashingInput(
            BytesIO(raw), 9, hashlib.sha256,
            md5(raw, usedforsecurity=False).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'5678', wrapped.read(4))
        with self.assertRaises(swob.HTTPException) as raised:
            wrapped.read(4)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        self.assertTrue(wrapped._input.closed)

    def test_empty_bad_hash(self):
        wrapped = HashingInput(BytesIO(b''), 0, hashlib.sha256, 'nope')
        with self.assertRaises(swob.HTTPException) as raised:
            wrapped.read(3)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        # the error causes us to close the input
        self.assertTrue(wrapped._input.closed)


if __name__ == '__main__':
    unittest.main()
