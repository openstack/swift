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

import hashlib
from mock import patch, MagicMock
import unittest

from six import BytesIO

from swift.common import swob
from swift.common.swob import Request, HTTPNoContent
from swift.common.middleware.s3api.utils import mktime
from swift.common.middleware.s3api.acl_handlers import get_acl_handler
from swift.common.middleware.s3api.subresource import ACL, User, Owner, \
    Grant, encode_acl
from test.unit.common.middleware.s3api.test_s3api import S3ApiTestCase
from swift.common.middleware.s3api.s3request import S3Request, \
    S3AclRequest, SigV4Request, SIGV4_X_AMZ_DATE_FORMAT, HashingInput
from swift.common.middleware.s3api.s3response import InvalidArgument, \
    NoSuchBucket, InternalError, \
    AccessDenied, SignatureDoesNotMatch, RequestTimeTooSkewed

from test.unit import DebugLogger

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
        if issubclass(req_klass, S3AclRequest):
            s3_req = req_klass(
                req.environ, MagicMock(),
                True, self.conf.storage_domain,
                self.conf.location, self.conf.force_swift_request_proxy_log,
                self.conf.dns_compliant_bucket_names,
                self.conf.allow_multipart_uploads, self.conf.allow_no_owner)
        else:
            s3_req = req_klass(
                req.environ, MagicMock(),
                True, self.conf.storage_domain,
                self.conf.location, self.conf.force_swift_request_proxy_log,
                self.conf.dns_compliant_bucket_names,
                self.conf.allow_multipart_uploads, self.conf.allow_no_owner)
        s3_req.set_acl_handler(
            get_acl_handler(s3_req.controller_name)(s3_req, DebugLogger()))
        with patch('swift.common.middleware.s3api.s3request.S3Request.'
                   '_get_response') as mock_get_resp, \
                patch('swift.common.middleware.s3api.subresource.ACL.'
                      'check_permission') as m_check_permission:
            mock_get_resp.return_value = fake_swift_resp \
                or FakeResponse(self.conf.s3_acl)
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
        self.assertEqual(e.exception.message,
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
        self.assertTrue(get_resp_obj is '')
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
        self.assertTrue(
            'not an integer or within integer range' in result.exception.body)
        self.assertEqual(
            result.exception.headers['content-type'], 'application/xml')

        # a param is negative integer
        s3req = create_s3request_with_param('max-keys', '-1')
        with self.assertRaises(InvalidArgument) as result:
            s3req.get_validated_param('max-keys', 1)
        self.assertTrue(
            'must be an integer between 0 and' in result.exception.body)
        self.assertEqual(
            result.exception.headers['content-type'], 'application/xml')

        # a param is not integer
        s3req = create_s3request_with_param('max-keys', 'invalid')
        with self.assertRaises(InvalidArgument) as result:
            s3req.get_validated_param('max-keys', 1)
        self.assertTrue(
            'not an integer or within integer range' in result.exception.body)
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
            s3_req = S3AclRequest(req.environ, MagicMock())
            self.assertNotIn('s3api.auth_details', s3_req.environ)
            self.assertEqual(s3_req.token, 'token')

    def test_to_swift_req_Authorization_not_exist_in_swreq(self):
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
            s3_req = S3AclRequest(req.environ, MagicMock())
            sw_req = s3_req.to_swift_req(method, container, obj)
            self.assertNotIn('s3api.auth_details', sw_req.environ)
            self.assertEqual(sw_req.headers['X-Auth-Token'], 'token')

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
                req.environ, MagicMock(), force_request_log=True)
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
                req.environ, MagicMock(), force_request_log=False)
            sw_req = s3_req.to_swift_req(method, container, obj)
            self.assertTrue(sw_req.environ['swift.proxy_access_log_made'])

    def test_get_container_info(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket', HTTPNoContent,
                            {'x-container-read': 'foo',
                             'X-container-object-count': 5,
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
        self.assertEqual('5', info['object_count'])  # sanity
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
        self.assertEqual(body, '')

    def test_date_header_expired(self):
        self.swift.register('HEAD', '/v1/AUTH_test/nojunk', swob.HTTPNotFound,
                            {}, None)
        req = Request.blank('/nojunk',
                            environ={'REQUEST_METHOD': 'HEAD'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': 'Fri, 01 Apr 2014 12:00:00 GMT'})

        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403')
        self.assertEqual(body, '')

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
        self.assertEqual(body, '')

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
        self.assertEqual(body, '')

    def _test_request_timestamp_sigv4(self, date_header):
        # signature v4 here
        environ = {
            'REQUEST_METHOD': 'GET'}

        if 'X-Amz-Date' in date_header:
            included_header = 'x-amz-date'
        elif 'Date' in date_header:
            included_header = 'date'
        else:
            self.fail('Invalid date header specified as test')

        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 '
                'Credential=test/%s/us-east-1/s3/aws4_request, '
                'SignedHeaders=%s,'
                'Signature=X' % (
                    self.get_v4_amz_date_header().split('T', 1)[0],
                    ';'.join(sorted(['host', included_header]))),
            'X-Amz-Content-SHA256': '0123456789'}

        headers.update(date_header)
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)

        if 'X-Amz-Date' in date_header:
            timestamp = mktime(
                date_header['X-Amz-Date'], SIGV4_X_AMZ_DATE_FORMAT)
        elif 'Date' in date_header:
            timestamp = mktime(date_header['Date'])

        self.assertEqual(timestamp, int(sigv4_req.timestamp))

    def test_request_timestamp_sigv4(self):
        access_denied_message = \
            'AWS authentication requires a valid Date or x-amz-date header'

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

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # mangled Date header
        date_header = {'Date': self.get_date_header()[20:]}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # Negative timestamp
        date_header = {'X-Amz-Date': '00160523T054055Z'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # far-past Date header
        date_header = {'Date': 'Tue, 07 Jul 999 21:53:04 GMT'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # near-future X-Amz-Date header
        dt = self.get_v4_amz_date_header()
        date_header = {'X-Amz-Date': '%d%s' % (int(dt[:4]) + 1, dt[4:])}
        with self.assertRaises(RequestTimeTooSkewed) as cm:
            self._test_request_timestamp_sigv4(date_header)

        # far-future Date header
        date_header = {'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'}
        with self.assertRaises(RequestTimeTooSkewed) as cm:
            self._test_request_timestamp_sigv4(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn('The difference between the request time and the '
                      'current time is too large.', cm.exception.body)

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
            'AWS authentication requires a valid Date or x-amz-date header'

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

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # mangled Date header
        date_header = {'Date': self.get_date_header()[:-20]}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # Negative timestamp
        date_header = {'X-Amz-Date': '00160523T054055Z'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # far-past Date header
        date_header = {'Date': 'Tue, 07 Jul 999 21:53:04 GMT'}
        with self.assertRaises(AccessDenied) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn(access_denied_message, cm.exception.body)

        # far-future Date header
        date_header = {'Date': 'Tue, 07 Jul 9999 21:53:04 GMT'}
        with self.assertRaises(RequestTimeTooSkewed) as cm:
            self._test_request_timestamp_sigv2(date_header)

        self.assertEqual('403 Forbidden', cm.exception.message)
        self.assertIn('The difference between the request time and the '
                      'current time is too large.', cm.exception.body)

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
            req.environ, storage_domain='s3.test.com')
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/obj1', environ=environ, headers=headers)
        sigv2_req = S3Request(
            req.environ, storage_domain='s3.test.com')
        uri = sigv2_req._canonical_uri()
        self.assertEqual(uri, '/bucket1/obj1')
        self.assertEqual(req.environ['PATH_INFO'], '/obj1')

        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'GET'}

        # Path-style
        req = Request.blank('/', environ=environ, headers=headers)
        sigv2_req = S3Request(req.environ, storage_domain='')
        uri = sigv2_req._canonical_uri()

        self.assertEqual(uri, '/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/bucket1/obj1',
                            environ=environ,
                            headers=headers)
        sigv2_req = S3Request(req.environ, storage_domain='')
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
        self.conf.storage_domain = 's3.test.com'
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, '/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/obj1', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, '/obj1')
        self.assertEqual(req.environ['PATH_INFO'], '/obj1')

        environ = {
            'HTTP_HOST': 's3.test.com',
            'REQUEST_METHOD': 'GET'}

        # Path-style
        self.conf.storage_domain = ''
        req = Request.blank('/', environ=environ, headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, '/')
        self.assertEqual(req.environ['PATH_INFO'], '/')

        req = Request.blank('/bucket/obj1',
                            environ=environ,
                            headers=headers)
        sigv4_req = SigV4Request(req.environ)
        uri = sigv4_req._canonical_uri()

        self.assertEqual(uri, '/bucket/obj1')
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
        sigv2_req = S3Request(req.environ, storage_domain='s3.amazonaws.com')
        expected_sts = '\n'.join([
            'GET',
            '',
            '',
            'Tue, 27 Mar 2007 19:36:42 +0000',
            '/johnsmith/photos/puppy.jpg',
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
        sigv2_req = S3Request(req.environ, storage_domain='s3.amazonaws.com')
        expected_sts = '\n'.join([
            'PUT',
            '',
            'image/jpeg',
            'Tue, 27 Mar 2007 21:15:45 +0000',
            '/johnsmith/photos/puppy.jpg',
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
        sigv2_req = S3Request(req.environ, storage_domain='s3.amazonaws.com')
        expected_sts = '\n'.join([
            'GET',
            '',
            '',
            'Tue, 27 Mar 2007 19:42:41 +0000',
            '/johnsmith/',
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
        sigv2_req = S3Request(req.environ, storage_domain='s3.amazonaws.com')
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
            req.environ, storage_domain='s3.amazonaws.com')
        self.assertFalse(sigv4_req.check_signature(
            u'\u30c9\u30e9\u30b4\u30f3'))


class TestHashingInput(S3ApiTestCase):
    def test_good(self):
        raw = b'123456789'
        wrapped = HashingInput(BytesIO(raw), 9, hashlib.md5,
                               hashlib.md5(raw).hexdigest())
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
        wrapped = HashingInput(BytesIO(raw), 8, hashlib.md5,
                               hashlib.md5(raw).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # even though the hash matches, there was more data than we expected
        with self.assertRaises(swob.Response) as raised:
            wrapped.read(3)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        # the error causes us to close the input
        self.assertTrue(wrapped._input.closed)

    def test_too_short(self):
        raw = b'123456789'
        wrapped = HashingInput(BytesIO(raw), 10, hashlib.md5,
                               hashlib.md5(raw).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'56', wrapped.read(2))
        # even though the hash matches, there was more data than we expected
        with self.assertRaises(swob.Response) as raised:
            wrapped.read(4)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        self.assertTrue(wrapped._input.closed)

    def test_bad_hash(self):
        raw = b'123456789'
        wrapped = HashingInput(BytesIO(raw), 9, hashlib.sha256,
                               hashlib.md5(raw).hexdigest())
        self.assertEqual(b'1234', wrapped.read(4))
        self.assertEqual(b'5678', wrapped.read(4))
        with self.assertRaises(swob.Response) as raised:
            wrapped.read(4)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        self.assertTrue(wrapped._input.closed)

    def test_empty_bad_hash(self):
        wrapped = HashingInput(BytesIO(b''), 0, hashlib.sha256, 'nope')
        with self.assertRaises(swob.Response) as raised:
            wrapped.read(3)
        self.assertEqual(raised.exception.status, '422 Unprocessable Entity')
        # the error causes us to close the input
        self.assertTrue(wrapped._input.closed)


if __name__ == '__main__':
    unittest.main()
