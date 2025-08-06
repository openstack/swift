
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

import base64
import io
import unittest
from unittest.mock import patch, MagicMock
import calendar
from datetime import datetime
from unittest import mock
import requests
import json
from paste.deploy import loadwsgi
from urllib.parse import unquote, quote

import swift.common.middleware.s3api
from swift.common.middleware.proxy_logging import ProxyLoggingMiddleware
from swift.common.middleware.s3api.s3response import ErrorResponse, \
    AccessDenied
from swift.common.middleware.s3api.utils import Config
from swift.common.middleware.keystoneauth import KeystoneAuth
from swift.common import swob, registry
from swift.common.request_helpers import get_log_info
from swift.common.swob import Request
from swift.common.utils import md5, get_logger, UTC

from keystonemiddleware.auth_token import AuthProtocol
from keystoneauth1.access import AccessInfoV2

from test.debug_logger import debug_logger, FakeStatsdClient
from test.unit.common.middleware.s3api import S3ApiTestCase
from test.unit.common.middleware.helpers import FakeSwift
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

    def test_init_config(self):
        # verify config loading
        # note: test confs do not have __file__ attribute so check_pipeline
        # will be short-circuited

        # check all defaults
        expected = dict(Config())
        expected.update({
            'auth_pipeline_check': True,
            'check_bucket_owner': False,
            'max_bucket_listing': 1000,
            'max_multi_delete_objects': 1000,
            'max_parts_listing': 1000,
            'max_upload_part_num': 1000,
            'min_segment_size': 5242880,
            'multi_delete_concurrency': 2,
            's3_acl': False,
            'cors_preflight_allow_origin': [],
            'ratelimit_as_client_error': False,
        })
        s3api = S3ApiMiddleware(None, {})
        self.assertEqual(expected, s3api.conf)

        # check all non-defaults are loaded
        conf = {
            'storage_domain': 'somewhere,some.other.where',
            'location': 'us-west-1',
            'force_swift_request_proxy_log': True,
            'dns_compliant_bucket_names': False,
            'allow_multipart_uploads': False,
            'allow_no_owner': True,
            'allowable_clock_skew': 300,
            'auth_pipeline_check': False,
            'check_bucket_owner': True,
            'max_bucket_listing': 500,
            'max_multi_delete_objects': 600,
            'max_parts_listing': 70,
            'max_upload_part_num': 800,
            'min_segment_size': 1000000,
            'multi_delete_concurrency': 1,
            's3_acl': True,
            'cors_preflight_allow_origin': 'foo.example.com,bar.example.com',
            'ratelimit_as_client_error': True,
        }
        s3api = S3ApiMiddleware(None, conf)
        conf['cors_preflight_allow_origin'] = \
            conf['cors_preflight_allow_origin'].split(',')
        conf['storage_domains'] = conf.pop('storage_domain').split(',')
        self.assertEqual(conf, s3api.conf)

        # test allow_origin list with a '*' fails.
        conf = {
            'storage_domain': 'somewhere',
            'location': 'us-west-1',
            'force_swift_request_proxy_log': True,
            'dns_compliant_bucket_names': False,
            'allow_multipart_uploads': False,
            'allow_no_owner': True,
            'allowable_clock_skew': 300,
            'auth_pipeline_check': False,
            'check_bucket_owner': True,
            'max_bucket_listing': 500,
            'max_multi_delete_objects': 600,
            'max_parts_listing': 70,
            'max_upload_part_num': 800,
            'min_segment_size': 1000000,
            'multi_delete_concurrency': 1,
            's3_acl': True,
            'cors_preflight_allow_origin': 'foo.example.com,bar.example.com,*',
        }
        with self.assertRaises(ValueError) as ex:
            S3ApiMiddleware(None, conf)
        self.assertIn("if cors_preflight_allow_origin should include all "
                      "domains, * must be the only entry", str(ex.exception))

        def check_bad_positive_ints(**kwargs):
            bad_conf = dict(conf, **kwargs)
            self.assertRaises(ValueError, S3ApiMiddleware, None, bad_conf)

        check_bad_positive_ints(allowable_clock_skew=-100)
        check_bad_positive_ints(allowable_clock_skew=0)
        check_bad_positive_ints(max_bucket_listing=-100)
        check_bad_positive_ints(max_bucket_listing=0)
        check_bad_positive_ints(max_multi_delete_objects=-100)
        check_bad_positive_ints(max_multi_delete_objects=0)
        check_bad_positive_ints(max_parts_listing=-100)
        check_bad_positive_ints(max_parts_listing=0)
        check_bad_positive_ints(max_upload_part_num=-100)
        check_bad_positive_ints(max_upload_part_num=0)
        check_bad_positive_ints(min_segment_size=-100)
        check_bad_positive_ints(min_segment_size=0)
        check_bad_positive_ints(multi_delete_concurrency=-100)
        check_bad_positive_ints(multi_delete_concurrency=0)

    def test_init_passes_wsgi_conf_file_to_check_pipeline(self):
        # verify that check_pipeline is called during init: add __file__ attr
        # to test config to make it more representative of middleware being
        # init'd by wgsi
        context = mock.Mock()
        with patch("swift.common.middleware.s3api.s3api.loadcontext",
                   return_value=context) as loader, \
                patch("swift.common.middleware.s3api.s3api.PipelineWrapper") \
                as pipeline:
            conf = dict(self.conf,
                        auth_pipeline_check=True,
                        __file__='proxy-conf-file')
            pipeline.return_value = 's3api tempauth proxy-server'
            self.s3api = S3ApiMiddleware(None, conf)
            loader.assert_called_with(loadwsgi.APP, 'proxy-conf-file')
            pipeline.assert_called_with(context)

    def test_init_logger(self):
        proxy_logger = get_logger({}, log_route='proxy-server').logger

        s3api = S3ApiMiddleware(None, {})
        self.assertEqual('s3api', s3api.logger.name)
        self.assertEqual('s3api', s3api.logger.logger.name)
        self.assertIsNot(s3api.logger.logger, proxy_logger)
        self.assertEqual('swift', s3api.logger.server)
        # there's a stats client, but with no host, it can't send anything
        self.assertIsNone(s3api.logger.logger.statsd_client._host)

        with mock.patch('swift.common.statsd_client.StatsdClient',
                        FakeStatsdClient):
            s3api = S3ApiMiddleware(None, {'log_name': 'proxy-server',
                                           'log_statsd_host': '1.2.3.4'})
            s3api.logger.increment('test-metric')
        self.assertEqual('s3api', s3api.logger.name)
        self.assertEqual('s3api', s3api.logger.logger.name)
        self.assertIsNot(s3api.logger.logger, proxy_logger)
        self.assertEqual('proxy-server', s3api.logger.server)
        self.assertEqual('s3api.', s3api.logger.logger.statsd_client._prefix)
        client = s3api.logger.logger.statsd_client
        self.assertEqual({'test-metric': 1}, client.get_increment_counts())
        self.assertEqual([(b's3api.test-metric:1|c', ('1.2.3.4', 8125))],
                         client.sendto_calls)

    def test_init_logs_checksum_implementation(self):
        with mock.patch('swift.common.middleware.s3api.s3api.get_logger',
                        return_value=self.logger), \
                mock.patch('swift.common.utils.checksum.crc32c_isal') \
                as mock_crc32c, \
                mock.patch('swift.common.utils.checksum.crc64nvme_isal') \
                as mock_crc64nvme:
            mock_crc32c.__name__ = 'crc32c_isal'
            mock_crc64nvme.__name__ = 'crc64nvme_isal'
            S3ApiMiddleware(None, {})
        self.assertEqual(
            {'info': ['Using crc32c_isal implementation for CRC32C.',
                      'Using crc64nvme_isal implementation for CRC64NVME.']},
            self.logger.all_log_lines())

    def test_non_s3_request_passthrough(self):
        req = Request.blank('/something')
        status, headers, body = self.call_s3api(req)
        self.assertEqual(body, b'FAKE APP')

    def test_bad_format_authorization(self):
        req = Request.blank('/something',
                            headers={'Authorization': 'hoge',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        self.assertEqual(
            {'403.AccessDenied.invalid_header_auth': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_header_auth',
                         get_log_info(req.environ))

    def test_bad_method(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MethodNotAllowed')
        self.assertEqual(
            {'405.MethodNotAllowed': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:MethodNotAllowed',
                         get_log_info(req.environ))

    def test_bad_method_but_method_exists_in_controller(self):
        req = Request.blank(
            '/bucket',
            environ={'REQUEST_METHOD': '_delete_segments_bucket'},
            headers={'Authorization': 'AWS test:tester:hmac',
                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'MethodNotAllowed')
        self.assertEqual(
            {'405.MethodNotAllowed': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:MethodNotAllowed',
                         get_log_info(req.environ))

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
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_test/bucket/object:1',
                         req.environ['swift.backend_path'])

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
                'wsgi.input': io.BytesIO(),
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
            self.assertEqual(hash, md5(s, usedforsecurity=False).hexdigest())

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
        req.headers['Date'] = datetime.now(UTC)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        self.assertEqual(
            {'403.AccessDenied.expired': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.expired',
                         get_log_info(req.environ))

    def test_signed_urls(self):
        # Set expire to last 32b timestamp value
        # This number can't be higher, because it breaks tests on 32b systems
        expire = '2147483647'  # 19 Jan 2038 03:14:07
        utc_date = datetime.now(UTC)
        req = Request.blank('/bucket/object?Signature=X&Expires=%s&'
                            'AWSAccessKeyId=test:tester&Timestamp=%s' %
                            (expire, utc_date.isoformat().rsplit('.')[0]),
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Date': self.get_date_header()})
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ['swift.backend_path'])
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
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ['swift.backend_path'])
        for _, _, headers in self.swift.calls_with_headers:
            self.assertNotIn('Authorization', headers)

    def test_signed_urls_invalid_expire(self):
        expire = 'invalid'
        req = Request.blank('/bucket/object?Signature=X&Expires=%s&'
                            'AWSAccessKeyId=test:tester' % expire,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Date': self.get_date_header()})
        req.headers['Date'] = datetime.now(UTC)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        self.assertEqual(
            {'403.AccessDenied.invalid_expires': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_expires',
                         get_log_info(req.environ))

    def test_signed_urls_no_sign(self):
        expire = '2147483647'  # 19 Jan 2038 03:14:07
        req = Request.blank('/bucket/object?Expires=%s&'
                            'AWSAccessKeyId=test:tester' % expire,
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Date': self.get_date_header()})
        req.headers['Date'] = datetime.now(UTC)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        self.assertEqual(
            {'403.AccessDenied.invalid_query_auth': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_query_auth',
                         get_log_info(req.environ))

    def test_signed_urls_no_access(self):
        expire = '2147483647'  # 19 Jan 2038 03:14:07
        req = Request.blank('/bucket/object?Expires=%s&'
                            'AWSAccessKeyId=' % expire,
                            environ={'REQUEST_METHOD': 'GET'})
        req.headers['Date'] = datetime.now(UTC)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'AccessDenied')
        self.assertEqual(
            {'403.AccessDenied.invalid_query_auth': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_query_auth',
                         get_log_info(req.environ))

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
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ['swift.backend_path'])
        self.assertEqual(status.split()[0], '200', body)
        for _, _, headers in self.swift.calls_with_headers:
            self.assertNotIn('Authorization', headers)
            self.assertNotIn('X-Auth-Token', headers)

    def test_signed_urls_v4_bad_credential(self):
        def test(credential, message, extra=b''):
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
            self.s3api.logger.logger.clear()
            status, headers, body = self.call_s3api(req)
            self.assertEqual(status.split()[0], '400', body)
            self.assertEqual(self._get_error_code(body),
                             'AuthorizationQueryParametersError')
            self.assertEqual(self._get_error_message(body), message)
            self.assertIn(extra, body)
            self.assertEqual(
                {'400.AuthorizationQueryParametersError': 1},
                self.s3api.logger.logger.statsd_client.get_increment_counts())
            self.assertEqual('s3:err:AuthorizationQueryParametersError',
                             get_log_info(req.environ))

        dt = self.get_v4_amz_date_header().split('T', 1)[0]
        test('test:tester/not-a-date/us-east-1/s3/aws4_request',
             'Invalid credential date "not-a-date". This date is not the same '
             'as X-Amz-Date: "%s".' % dt)
        test('test:tester/%s/us-west-1/s3/aws4_request' % dt,
             "Error parsing the X-Amz-Credential parameter; the region "
             "'us-west-1' is wrong; expecting 'us-east-1'",
             b'<Region>us-east-1</Region>')
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
        self.assertEqual(
            {'403.AccessDenied.invalid_date': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_date',
                         get_log_info(req.environ))

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
        self.assertEqual(
            {'400.InvalidArgument': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidArgument',
                         get_log_info(req.environ))

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
        self.assertEqual(
            {'400.AuthorizationHeaderMalformed': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AuthorizationHeaderMalformed',
                         get_log_info(req.environ))

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
        self.assertEqual(
            {'403.AccessDenied.invalid_credential': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_credential',
                         get_log_info(req.environ))

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
        self.assertEqual(
            {'403.AccessDenied.invalid_query_auth': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_query_auth',
                         get_log_info(req.environ))

    def test_bucket_virtual_hosted_style(self):
        req = Request.blank('/',
                            environ={'HTTP_HOST': 'bucket.localhost:80',
                                     'REQUEST_METHOD': 'HEAD',
                                     'HTTP_AUTHORIZATION':
                                     'AWS test:tester:hmac'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_test/bucket',
                         req.environ['swift.backend_path'])

    def test_object_virtual_hosted_style(self):
        req = Request.blank('/object',
                            environ={'HTTP_HOST': 'bucket.localhost:80',
                                     'REQUEST_METHOD': 'HEAD',
                                     'HTTP_AUTHORIZATION':
                                     'AWS test:tester:hmac'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ['swift.backend_path'])

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
                        'SigCheckerV2.check_signature') as mock_cs:
            status, headers, body = self.call_s3api(req)
            self.assertIn('swift.backend_path', req.environ)
            self.assertEqual(
                '/v1/AUTH_test/bucket+segments/object/123456789abcdef/1',
                req.environ['swift.backend_path'])

        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req.environ['s3api.auth_details'], {
            'access_key': 'test:tester',
            'signature': 'hmac',
            'string_to_sign': b'\n'.join([
                b'PUT', b'', b'', date_header.encode('ascii'),
                b'/bucket/object?partNumber=1&uploadId=123456789abcdef']),
            'check_signature': mock_cs})

    def test_non_ascii_user(self):
        self.swift.register('HEAD', '/v1/AUTH_test/bucket+segments/'
                                    'object/123456789abcdef',
                            swob.HTTPOk, {}, None)
        self.swift.register('PUT', '/v1/AUTH_test/bucket+segments/'
                                   'object/123456789abcdef/1',
                            swob.HTTPCreated, {}, None)
        req = Request.blank('/bucket/object?uploadId=123456789abcdef'
                            '&partNumber=1',
                            environ={'REQUEST_METHOD': 'PUT'})
        # NB: WSGI string for a snowman
        req.headers['Authorization'] = 'AWS test:\xe2\x98\x83:sig'
        date_header = self.get_date_header()
        req.headers['Date'] = date_header
        with mock.patch('swift.common.middleware.s3api.s3request.'
                        'SigCheckerV2.check_signature') as mock_cs:
            status, headers, body = self.call_s3api(req)
            self.assertIn('swift.backend_path', req.environ)
            self.assertEqual(
                '/v1/AUTH_test/bucket+segments/object/123456789abcdef/1',
                req.environ['swift.backend_path'])

        _, _, headers = self.swift.calls_with_headers[-1]
        self.assertEqual(req.environ['s3api.auth_details'], {
            'access_key': u'test:\N{SNOWMAN}',
            'signature': 'sig',
            'string_to_sign': b'\n'.join([
                b'PUT', b'', b'', date_header.encode('ascii'),
                b'/bucket/object?partNumber=1&uploadId=123456789abcdef']),
            'check_signature': mock_cs})

    def test_invalid_uri(self):
        req = Request.blank('/bucket/invalid\xffname',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidURI')
        self.assertEqual(
            {'400.InvalidURI': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidURI', get_log_info(req.environ))

    def test_object_create_bad_md5_unreadable(self):
        req = Request.blank('/bucket/object',
                            environ={'REQUEST_METHOD': 'PUT',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                                     'HTTP_CONTENT_MD5': '#'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')
        self.assertEqual(
            {'400.InvalidDigest': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidDigest', get_log_info(req.environ))

    def test_object_create_bad_md5_too_short(self):
        too_short_digest = md5(b'hey', usedforsecurity=False).digest()[:-1]
        md5_str = base64.b64encode(too_short_digest).strip().decode('ascii')
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                     'HTTP_CONTENT_MD5': md5_str},
            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')
        self.assertEqual(
            {'400.InvalidDigest': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidDigest', get_log_info(req.environ))

    def test_object_create_bad_md5_bad_padding(self):
        too_short_digest = md5(b'hey', usedforsecurity=False).digest()
        md5_str = base64.b64encode(too_short_digest).strip(
            b'=\n').decode('ascii')
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                     'HTTP_CONTENT_MD5': md5_str},
            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')
        self.assertEqual(
            {'400.InvalidDigest': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidDigest', get_log_info(req.environ))

    def test_object_create_bad_md5_too_long(self):
        too_long_digest = md5(
            b'hey', usedforsecurity=False).digest() + b'suffix'
        md5_str = base64.b64encode(too_long_digest).strip().decode('ascii')
        req = Request.blank(
            '/bucket/object',
            environ={'REQUEST_METHOD': 'PUT',
                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                     'HTTP_CONTENT_MD5': md5_str},
            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidDigest')
        self.assertEqual(
            {'400.InvalidDigest': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidDigest', get_log_info(req.environ))

    def test_invalid_metadata_directive(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                                     'HTTP_X_AMZ_METADATA_DIRECTIVE':
                                     'invalid'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        self.assertEqual(
            {'400.InvalidArgument': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidArgument',
                         get_log_info(req.environ))

    def test_invalid_storage_class(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z',
                                     'HTTP_X_AMZ_STORAGE_CLASS': 'INVALID'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidStorageClass')
        self.assertEqual(
            {'400.InvalidStorageClass': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidStorageClass',
                         get_log_info(req.environ))

    def test_invalid_ssc(self):
        req = Request.blank('/',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z'},
                            headers={'x-amz-server-side-encryption': 'invalid',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'InvalidArgument')
        self.assertEqual(
            {'400.InvalidArgument': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidArgument',
                         get_log_info(req.environ))

    def _test_unsupported_header(self, header, value=None):
        if value is None:
            value = 'value'
        req = Request.blank('/error',
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z'},
                            headers={header: value,
                                     'Date': self.get_date_header()})
        self.s3api.logger.logger.clear()
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')
        self.assertEqual(
            {'501.NotImplemented': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:NotImplemented',
                         get_log_info(req.environ))

    def test_mfa(self):
        self._test_unsupported_header('x-amz-mfa')

    @mock.patch.object(registry, '_swift_admin_info', dict())
    def test_server_side_encryption(self):
        sse_header = 'x-amz-server-side-encryption'
        self._test_unsupported_header(sse_header, 'AES256')
        self._test_unsupported_header(sse_header, 'aws:kms')
        registry.register_swift_info('encryption', admin=True, enabled=False)
        self._test_unsupported_header(sse_header, 'AES256')
        self._test_unsupported_header(sse_header, 'aws:kms')
        registry.register_swift_info('encryption', admin=True, enabled=True)
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
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_X/bucket/object',
                         req.environ['swift.backend_path'])
        # ...but aws:kms continues to fail
        self._test_unsupported_header(sse_header, 'aws:kms')

    def test_website_redirect_location(self):
        self._test_unsupported_header('x-amz-website-redirect-location')

    def test_object_tagging(self):
        self._test_unsupported_header('x-amz-tagging')

    def _test_unsupported_resource(self, resource):
        req = Request.blank('/error?' + resource,
                            environ={'REQUEST_METHOD': 'GET',
                                     'HTTP_AUTHORIZATION': 'AWS X:Y:Z'},
                            headers={'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')
        self.assertEqual(
            {'501.NotImplemented': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:NotImplemented',
                         get_log_info(req.environ))

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
        req = Request.blank('/bucket?tagging',
                            environ={'REQUEST_METHOD': 'GET'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200')
        self.assertEqual(
            {},
            self.s3api.logger.logger.statsd_client.get_increment_counts())

        req = Request.blank('/bucket?tagging',
                            environ={'REQUEST_METHOD': 'PUT'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.s3api.logger.logger.clear()
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')
        self.assertEqual(
            {'501.NotImplemented': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:NotImplemented',
                         get_log_info(req.environ))

        req = Request.blank('/bucket?tagging',
                            environ={'REQUEST_METHOD': 'DELETE'},
                            headers={'Authorization': 'AWS test:tester:hmac',
                                     'Date': self.get_date_header()})
        self.s3api.logger.logger.clear()
        status, headers, body = self.call_s3api(req)
        self.assertEqual(self._get_error_code(body), 'NotImplemented')
        self.assertEqual(
            {'501.NotImplemented': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:NotImplemented',
                         get_log_info(req.environ))

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
        self.assertEqual(
            {'405.MethodNotAllowed': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:MethodNotAllowed',
                         get_log_info(req.environ))

    @mock.patch.object(registry, '_sensitive_headers', set())
    @mock.patch.object(registry, '_sensitive_params', set())
    def test_registered_sensitive_info(self):
        self.assertFalse(registry.get_sensitive_headers())
        self.assertFalse(registry.get_sensitive_params())
        filter_factory(self.conf)
        sensitive = registry.get_sensitive_headers()
        self.assertIn('authorization', sensitive)
        sensitive = registry.get_sensitive_params()
        self.assertIn('X-Amz-Signature', sensitive)
        self.assertIn('Signature', sensitive)

    @mock.patch.object(registry, '_swift_info', dict())
    def test_registered_defaults(self):
        conf_from_file = {k: str(v) for k, v in self.conf.items()}
        filter_factory(conf_from_file)
        swift_info = registry.get_swift_info()
        self.assertTrue('s3api' in swift_info)
        registered_keys = [
            'max_bucket_listing', 'max_parts_listing', 'max_upload_part_num',
            'max_multi_delete_objects', 'allow_multipart_uploads',
            'min_segment_size', 's3_acl']
        expected = dict((k, self.conf[k]) for k in registered_keys)
        self.assertEqual(expected, swift_info['s3api'])

    def test_check_pipeline(self):
        with patch("swift.common.middleware.s3api.s3api.loadcontext"), \
                patch("swift.common.middleware.s3api.s3api.PipelineWrapper") \
                as pipeline:
            # cause check_pipeline to not return early...
            self.conf['__file__'] = ''
            # ...and enable pipeline auth checking
            self.s3api.conf.auth_pipeline_check = True

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

            # Note that authtoken would need to have delay_auth_decision=True
            pipeline.return_value = 's3api authtoken s3token keystoneauth ' \
                'proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api proxy-server'
            with self.assertRaises(ValueError) as cm:
                self.s3api.check_pipeline(self.conf)
            self.assertIn('expected auth between s3api and proxy-server',
                          cm.exception.args[0])

            pipeline.return_value = 'proxy-server'
            with self.assertRaises(ValueError) as cm:
                self.s3api.check_pipeline(self.conf)
            self.assertIn("missing filters ['s3api']",
                          cm.exception.args[0])

    def test_s3api_initialization_with_disabled_pipeline_check(self):
        with patch("swift.common.middleware.s3api.s3api.loadcontext"), \
                patch("swift.common.middleware.s3api.s3api.PipelineWrapper") \
                as pipeline:
            # cause check_pipeline to not return early...
            self.conf['__file__'] = ''
            # ...but disable pipeline auth checking
            self.s3api.conf.auth_pipeline_check = False

            pipeline.return_value = 's3api tempauth proxy-server'
            self.s3api.check_pipeline(self.conf)

            pipeline.return_value = 's3api s3token authtoken keystoneauth ' \
                'proxy-server'
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
            'X-Amz-Content-SHA256': '0' * 64}
        req = Request.blank('/bucket/object', environ=environ, headers=headers)
        req.content_type = 'text/plain'
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '200', body)
        self.assertIn('swift.backend_path', req.environ)
        self.assertEqual('/v1/AUTH_test/bucket/object',
                         req.environ['swift.backend_path'])
        for _, _, headers in self.swift.calls_with_headers:
            self.assertEqual(authz_header, headers['Authorization'])
            self.assertNotIn('X-Auth-Token', headers)

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
        self.assertEqual(
            {'403.AccessDenied.invalid_date': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_date',
                         get_log_info(req.environ))

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
        self.assertEqual(
            {'400.InvalidRequest': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:InvalidRequest',
                         get_log_info(req.environ))

    def test_signature_v4_bad_authorization_string(self):
        def test(auth_str, error, msg, metric, extra=b''):
            environ = {
                'REQUEST_METHOD': 'GET'}
            headers = {
                'Authorization': auth_str,
                'X-Amz-Date': self.get_v4_amz_date_header(),
                'X-Amz-Content-SHA256': '0123456789'}
            req = Request.blank('/bucket/object', environ=environ,
                                headers=headers)
            req.content_type = 'text/plain'
            self.s3api.logger.logger.clear()
            status, headers, body = self.call_s3api(req)
            self.assertEqual(self._get_error_code(body), error)
            self.assertEqual(self._get_error_message(body), msg)
            self.assertIn(extra, body)
            self.assertEqual(
                {metric: 1},
                self.s3api.logger.logger.statsd_client.get_increment_counts())
            self.assertEqual('s3:err:%s' % metric[4:],
                             get_log_info(req.environ))

        auth_str = ('AWS4-HMAC-SHA256 '
                    'SignedHeaders=host;x-amz-date,'
                    'Signature=X')
        test(auth_str, 'AccessDenied', 'Access Denied.',
             '403.AccessDenied.invalid_credential')

        auth_str = (
            'AWS4-HMAC-SHA256 '
            'Credential=test:tester/20130524/us-east-1/s3/aws4_request, '
            'Signature=X')
        test(auth_str, 'AuthorizationHeaderMalformed',
             'The authorization header is malformed; the authorization '
             'header requires three components: Credential, SignedHeaders, '
             'and Signature.', '400.AuthorizationHeaderMalformed')

        auth_str = ('AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-west-2/s3/aws4_request, '
                    'Signature=X, SignedHeaders=host;x-amz-date' %
                    self.get_v4_amz_date_header().split('T', 1)[0])
        test(auth_str, 'AuthorizationHeaderMalformed',
             "The authorization header is malformed; "
             "the region 'us-west-2' is wrong; expecting 'us-east-1'",
             '400.AuthorizationHeaderMalformed', b'<Region>us-east-1</Region>')

        auth_str = ('AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-east-1/not-s3/aws4_request, '
                    'Signature=X, SignedHeaders=host;x-amz-date' %
                    self.get_v4_amz_date_header().split('T', 1)[0])
        test(auth_str, 'AuthorizationHeaderMalformed',
             'The authorization header is malformed; '
             'incorrect service "not-s3". This endpoint belongs to "s3".',
             '400.AuthorizationHeaderMalformed')

        auth_str = ('AWS4-HMAC-SHA256 '
                    'Credential=test:tester/%s/us-east-1/s3/not-aws4_request, '
                    'Signature=X, SignedHeaders=host;x-amz-date' %
                    self.get_v4_amz_date_header().split('T', 1)[0])
        test(auth_str, 'AuthorizationHeaderMalformed',
             'The authorization header is malformed; '
             'incorrect terminal "not-aws4_request". '
             'This endpoint uses "aws4_request".',
             '400.AuthorizationHeaderMalformed')

        auth_str = (
            'AWS4-HMAC-SHA256 '
            'Credential=test:tester/20130524/us-east-1/s3/aws4_request, '
            'SignedHeaders=host;x-amz-date')
        test(auth_str, 'AccessDenied', 'Access Denied.',
             '403.AccessDenied.invalid_header_auth')

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
                'wsgi.input': io.BytesIO(),
            }
            fake_time = calendar.timegm((2011, 9, 9, 23, 36, 0))
            env.update(environ)
            with patch('swift.common.middleware.s3api.s3request.'
                       'S3Request._validate_headers'), \
                    patch('swift.common.middleware.s3api.utils.time.time',
                          return_value=fake_time):
                req = SigV4Request(env, conf=self.s3api.conf)
            return req

        def canonical_string(path, environ):
            return _get_req(path, environ)._canonical_request()

        def verify(hash_val, path, environ):
            # See http://docs.aws.amazon.com/general/latest/gr
            # /signature-v4-test-suite.html for where location, service, and
            # signing key came from
            with patch.object(self.s3api.conf, 'location', 'us-east-1'), \
                    patch.object(swift.common.middleware.s3api.s3request,
                                 'SERVICE', 'host'):
                req = _get_req(path, environ)
                hash_in_sts = req.sig_checker._string_to_sign().split(b'\n')[3]
                self.assertEqual(hash_val, hash_in_sts.decode('ascii'))
                self.assertTrue(req.sig_checker.check_signature(
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
                   swob.wsgi_unquote('/%E1%88%B4'), env)

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
        self.assertEqual(
            {'403.AccessDenied.invalid_expires': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_expires',
                         get_log_info(req.environ))

        # But if we are missing Signature in query param
        req = Request.blank(
            '/bucket/object'
            '?X-Amz-Algorithm=AWS4-HMAC-SHA256'
            '&X-Amz-Credential=test/20T20Z/us-east-1/s3/aws4_requestB'
            '&X-Amz-SignedHeaders=hostB',
            environ={'REQUEST_METHOD': 'GET'},
            headers=headers)
        req.content_type = 'text/plain'
        self.s3api.logger.logger.clear()
        status, headers, body = self.call_s3api(req)
        self.assertEqual(status.split()[0], '403', body)
        self.assertEqual(
            {'403.AccessDenied.invalid_expires': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_expires',
                         get_log_info(req.environ))

    def test_s3api_with_only_s3_token(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.s3_token = S3Token(
            self.keystone_auth, {'auth_uri': 'https://fakehost/identity'})
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        self.s3api.logger = debug_logger()
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
            mock_resp._content = json.dumps(GOOD_RESPONSE_V2).encode('ascii')
            mock_resp.status_code = 201
            mock_req.return_value = mock_resp

            status, headers, body = self.call_s3api(req)
            self.assertEqual(body, b'')
            self.assertEqual(1, mock_req.call_count)
            self.assertIn('swift.backend_path', req.environ)
            self.assertEqual('/v1/AUTH_TENANT_ID/bucket',
                             req.environ['swift.backend_path'])

    def test_s3api_with_only_s3_token_v3(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.s3_token = S3Token(
            self.keystone_auth, {'auth_uri': 'https://fakehost/identity'})
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        self.s3api.logger = debug_logger()
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
            mock_resp._content = json.dumps(GOOD_RESPONSE_V3).encode('ascii')
            mock_resp.status_code = 200
            mock_req.return_value = mock_resp

            status, headers, body = self.call_s3api(req)
            self.assertEqual(body, b'')
            self.assertEqual(1, mock_req.call_count)
            self.assertIn('swift.backend_path', req.environ)
            self.assertEqual('/v1/AUTH_PROJECT_ID/bucket',
                             req.environ['swift.backend_path'])

    def test_s3api_with_s3_token_and_auth_token(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.auth_token = AuthProtocol(
            self.keystone_auth, {'delay_auth_decision': 'True'})
        self.s3_token = S3Token(
            self.auth_token, {'auth_uri': 'https://fakehost/identity'})
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        self.s3api.logger = debug_logger()
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
                # sanity check
                self.assertIn('id', GOOD_RESPONSE_V2['access']['token'])
                mock_resp = requests.Response()
                mock_resp._content = json.dumps(
                    GOOD_RESPONSE_V2).encode('ascii')
                mock_resp.status_code = 201
                mock_req.return_value = mock_resp

                mock_access_info = AccessInfoV2(GOOD_RESPONSE_V2)
                mock_access_info.will_expire_soon = \
                    lambda stale_duration: False
                mock_fetch.return_value = (MagicMock(), mock_access_info)

                status, headers, body = self.call_s3api(req)
                # Even though s3token got a token back from keystone, we drop
                # it on the floor, resulting in a 401 Unauthorized at
                # `swift.common.middleware.keystoneauth` because
                # keystonemiddleware's auth_token strips out all auth headers,
                # significantly 'X-Identity-Status'. Without a token, it then
                # sets 'X-Identity-Status: Invalid' and never contacts
                # Keystone.
                self.assertEqual('403 Forbidden', status)
                self.assertIn('swift.backend_path', req.environ)
                self.assertEqual('/v1/AUTH_TENANT_ID/bucket',
                                 req.environ['swift.backend_path'])
                self.assertEqual(1, mock_req.call_count)
                # it never even tries to contact keystone
                self.assertEqual(0, mock_fetch.call_count)
                statsd_client = self.s3api.logger.logger.statsd_client
                self.assertEqual(
                    {'403.SignatureDoesNotMatch': 1},
                    statsd_client.get_increment_counts())
            self.assertEqual('s3:err:SignatureDoesNotMatch',
                             get_log_info(req.environ))

    def test_s3api_with_only_s3_token_in_s3acl(self):
        self.swift = FakeSwift()
        self.keystone_auth = KeystoneAuth(
            self.swift, {'operator_roles': 'swift-user'})
        self.s3_token = S3Token(
            self.keystone_auth, {'auth_uri': 'https://fakehost/identity'})

        self.conf['s3_acl'] = True
        self.s3api = S3ApiMiddleware(self.s3_token, self.conf)
        self.s3api.logger = debug_logger()
        req = Request.blank(
            '/bucket',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'Authorization': 'AWS access:signature',
                     'Date': self.get_date_header()})
        self.swift.register('PUT', '/v1/AUTH_TENANT_ID/bucket',
                            swob.HTTPCreated, {}, None)
        # For now, s3 acl commits the bucket owner acl via POST
        # after PUT container so we need to register the resposne here
        self.swift.register('POST', '/v1/AUTH_TENANT_ID/bucket',
                            swob.HTTPNoContent, {}, None)
        with patch.object(self.s3_token, '_json_request') as mock_req:
            mock_resp = requests.Response()
            mock_resp._content = json.dumps(GOOD_RESPONSE_V2).encode('ascii')
            mock_resp.status_code = 201
            mock_req.return_value = mock_resp

            status, headers, body = self.call_s3api(req)
            self.assertEqual(body, b'')
            self.assertIn('swift.backend_path', req.environ)
            self.assertEqual('/v1/AUTH_TENANT_ID/bucket',
                             req.environ['swift.backend_path'])
            self.assertEqual(1, mock_req.call_count)

    def test_s3api_with_time_skew(self):
        def do_test(skew):
            req = Request.blank(
                '/object',
                environ={'HTTP_HOST': 'bucket.localhost:80',
                         'REQUEST_METHOD': 'GET',
                         'HTTP_AUTHORIZATION':
                             'AWS test:tester:hmac'},
                headers={'Date': self.get_date_header(skew=skew)})
            self.s3api.logger.logger.clear()
            status, headers, body = self.call_s3api(req)
            return req, status, headers, body

        req, status, _, body = do_test(800)
        self.assertEqual('200 OK', status)
        self.assertFalse(
            self.s3api.logger.logger.statsd_client.get_increment_counts())

        req, status, _, body = do_test(-800)
        self.assertEqual('200 OK', status)
        self.assertFalse(
            self.s3api.logger.logger.statsd_client.get_increment_counts())

        req, status, _, body = do_test(1000)
        self.assertEqual('403 Forbidden', status)
        self.assertEqual(self._get_error_code(body), 'RequestTimeTooSkewed')
        self.assertEqual(
            {'403.RequestTimeTooSkewed': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:RequestTimeTooSkewed',
                         get_log_info(req.environ))

        req, status, _, body = do_test(-1000)
        self.assertEqual('403 Forbidden', status)
        self.assertEqual(self._get_error_code(body), 'RequestTimeTooSkewed')
        self.assertEqual(
            {'403.RequestTimeTooSkewed': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:RequestTimeTooSkewed',
                         get_log_info(req.environ))

        self.s3api.conf.allowable_clock_skew = 100
        req, status, _, body = do_test(800)
        self.assertEqual('403 Forbidden', status)
        self.assertEqual(self._get_error_code(body), 'RequestTimeTooSkewed')
        self.assertEqual(
            {'403.RequestTimeTooSkewed': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:RequestTimeTooSkewed',
                         get_log_info(req.environ))

    def test_s3api_error_metric(self):
        class KaboomResponse(ErrorResponse):
            _code = 'ka boom'

        def do_test(err_response):
            req = Request.blank(
                '/object',
                environ={'HTTP_HOST': 'bucket.localhost:80',
                         'REQUEST_METHOD': 'GET',
                         'HTTP_AUTHORIZATION':
                             'AWS test:tester:hmac'},
                headers={'Date': self.get_date_header()})
            self.s3api.logger.logger.clear()
            with mock.patch.object(
                    self.s3api, 'handle_request', side_effect=err_response):
                self.call_s3api(req)
            return req

        req = do_test(ErrorResponse(status=403, msg='not good', reason='bad'))
        self.assertEqual(
            {'403.ErrorResponse.bad': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:ErrorResponse.bad',
                         get_log_info(req.environ))

        req = do_test(AccessDenied(msg='no entry', reason='invalid_date'))
        self.assertEqual(
            {'403.AccessDenied.invalid_date': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_date',
                         get_log_info(req.environ))

        # check whitespace replaced with underscore
        req = do_test(KaboomResponse(status=400, msg='boom',
                                     reason='boom boom'))
        self.assertEqual(
            {'400.ka_boom.boom_boom': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:ka_boom.boom_boom',
                         get_log_info(req.environ))

    def test_error_response_reason_logging(self):
        # verify that proxy logging gets error reason in log_info
        environ = {'REQUEST_METHOD': 'HEAD'}
        headers = {
            'Authorization':
                'AWS4-HMAC-SHA256 SignedHeaders=host;x-amz-date,Signature=X',
            'X-Amz-Date': self.get_v4_amz_date_header(),
            # invalid sha
            'X-Amz-Content-SHA256': '0123456789'}
        req = Request.blank('/bucket/object', environ=environ,
                            headers=headers)
        req.content_type = 'text/plain'
        log_conf = {'log_msg_template': '{method} {path} {log_info}'}
        app = ProxyLoggingMiddleware(self.s3api, log_conf, self.logger)
        status, headers, body = self.call_app(req, app=app)

        self.assertEqual(
            {'403.AccessDenied.invalid_credential': 1},
            self.s3api.logger.logger.statsd_client.get_increment_counts())
        self.assertEqual('s3:err:AccessDenied.invalid_credential',
                         get_log_info(req.environ))
        self.assertEqual(
            ['HEAD /bucket/object s3:err:AccessDenied.invalid_credential'],
            self.logger.get_lines_for_level('info'))


if __name__ == '__main__':
    unittest.main()
