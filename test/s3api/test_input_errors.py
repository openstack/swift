# Copyright (c) 2024 NVIDIA
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
import base64
import datetime
import hashlib
import hmac
import os
import requests
import requests.models
from urllib.parse import urlsplit, urlunsplit, quote

from swift.common import bufferedhttp
from swift.common.utils import UTC
from swift.common.utils.ipaddrs import parse_socket_string

from test.s3api import BaseS3TestCaseWithBucket, get_opt


def _hmac(key, message, digest):
    if not isinstance(key, bytes):
        key = key.encode('utf8')
    if not isinstance(message, bytes):
        message = message.encode('utf8')
    return hmac.new(key, message, digest).digest()


def _sha256(payload=b''):
    if not isinstance(payload, bytes):
        payload = payload.encode('utf8')
    return hashlib.sha256(payload).hexdigest()


def _md5(payload=b''):
    return base64.b64encode(
        hashlib.md5(payload).digest()
    ).decode('ascii')


EMPTY_SHA256 = _sha256()
EPOCH = datetime.datetime.fromtimestamp(0, UTC)


class S3Session(object):
    bucket_in_host = False
    default_expiration = 900  # 15 min

    def __init__(
        self,
        endpoint,
        access_key,
        secret_key,
        region='us-east-1',
        session_token='',
    ):
        parts = urlsplit(endpoint)
        self.https = (parts.scheme == 'https')
        self.host = parts.netloc  # note: may include port
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token
        self.session = requests.Session()

    def make_request(
        self,
        bucket=None,
        key=None,
        method='GET',
        query=None,
        headers=None,
        body=b'',
        stream=False,
    ):
        req = self.build_request(bucket, key, method, query, headers, stream)
        self.sign_request(req)
        return self.send_request(req, body)

    def build_request(
        self,
        bucket=None,
        key=None,
        method='GET',
        query=None,
        headers=None,
        stream=False,
    ):
        request = {
            'https': self.https,
            'host': self.host,
            'method': method,
            'path': '/',
            'query': query or {},
            'headers': requests.models.CaseInsensitiveDict(headers or {}),
            'stream_response': stream,  # set to True for large downloads
            'bucket': bucket,
            'key': key,
            'now': datetime.datetime.now(UTC),
        }

        if bucket:
            if self.bucket_in_host:
                request['host'] = bucket + '.' + request['host']
            else:
                request['path'] += bucket + '/'

        if key:
            if not bucket:
                raise ValueError('bucket required')
            request['path'] += key

        request['headers'].update({
            'Date': request['now'].strftime("%a, %d %b %Y %H:%M:%S GMT"),
        })

        return request

    def date_to_sign(self, request):
        raise NotImplementedError

    def sign_request(self, request):
        raise NotImplementedError

    def send_request(self, request, body):
        url = urlunsplit((
            'https' if request['https'] else 'http',
            request['host'],
            request['path'],
            '&'.join('%s=%s' % (k, v) for k, v in request['query'].items()),
            None,  # no fragment
        ))
        # Note that
        # * requests will automatically include a Content-Length header when
        #   sending a bytes body
        # * no signing method incorporates the value of any Content-Length
        #   header or even its existence/absence
        return self.session.request(
            request['method'],
            url,
            headers=request['headers'],
            data=body,
            stream=request['stream_response'],
        )


class S3SessionV2(S3Session):
    def build_request(
        self,
        bucket=None,
        key=None,
        method='GET',
        query=None,
        headers=None,
        stream=False,
    ):
        request = super().build_request(
            bucket,
            key,
            method,
            query,
            headers,
            stream,
        )
        if self.session_token:
            request['headers']['x-amz-security-token'] = self.session_token
        return request

    def sign_v2(self, request):
        string_to_sign_lines = [
            request['method'],
            request['headers'].get('content-md5', ''),
            request['headers'].get('Content-Type', ''),
            self.date_to_sign(request),
        ]

        amz_headers = sorted(
            (h.strip(), v.strip())
            for h, v in request['headers'].lower_items()
            if h.startswith('x-amz-')
        )
        string_to_sign_lines.extend('%s:%s' % (h, v)
                                    for h, v in amz_headers)

        string_to_sign_lines.append(
            ('/' + request['bucket'] if self.bucket_in_host else '')
            + request['path']
        )
        signature = base64.b64encode(_hmac(
            self.secret_key,
            '\n'.join(string_to_sign_lines),
            hashlib.sha1,
        )).decode('ascii')
        return {
            'credential': self.access_key,
            'signature': signature,
        }


class S3SessionV2Headers(S3SessionV2):
    def date_to_sign(self, request):
        if 'X-Amz-Date' in request['headers']:
            return ''
        else:
            return request['headers']['Date']

    def sign_request(self, request):
        bundle = self.sign_v2(request)
        request['headers']['Authorization'] = 'AWS ' + ':'.join([
            bundle['credential'],
            bundle['signature'],
        ])


class S3SessionV2Query(S3SessionV2):
    def build_request(
        self,
        bucket=None,
        key=None,
        method='GET',
        query=None,
        headers=None,
        stream=False,
    ):
        request = super().build_request(
            bucket,
            key,
            method,
            query,
            headers,
            stream,
        )

        expires = int((request['now'] - EPOCH).total_seconds()) \
            + self.default_expiration
        request['query'].update({
            'Expires': str(expires),
            'AWSAccessKeyId': self.access_key,
        })

        return request

    def date_to_sign(self, request):
        return request['query']['Expires']

    def sign_request(self, request):
        bundle = self.sign_v2(request)
        request['query'].update({
            'Signature': quote(bundle['signature'], safe='-_.~'),
        })


class S3SessionV4(S3Session):
    def sign_v4(self, request):
        canonical_request_lines = [
            request['method'],
            ('/' + request['bucket'] if self.bucket_in_host else '')
            + request['path'],
            '&'.join('%s=%s' % (k, v)
                     for k, v in sorted(request['query'].items())),
        ]
        canonical_request_lines.extend(
            '%s:%s' % (h, request['headers'][h])
            for h in request['signed_headers'])
        canonical_request_lines.extend([
            '',
            ';'.join(request['signed_headers']),
            request['headers'].get('x-amz-content-sha256', 'UNSIGNED-PAYLOAD')
        ])
        scope = [
            request['now'].strftime('%Y%m%d'),
            self.region,
            's3',
            'aws4_request',
        ]
        string_to_sign_lines = [
            'AWS4-HMAC-SHA256',
            self.date_to_sign(request),
            '/'.join(scope),
            _sha256('\n'.join(canonical_request_lines)),
        ]
        key = 'AWS4' + self.secret_key
        for piece in scope:
            key = _hmac(key, piece, hashlib.sha256)
        signature = binascii.hexlify(_hmac(
            key,
            '\n'.join(string_to_sign_lines),
            hashlib.sha256
        )).decode('ascii')
        return {
            'credential': self.access_key + '/' + '/'.join(scope),
            'signature': signature,
        }


class S3SessionV4Headers(S3SessionV4):
    def build_request(
        self,
        bucket=None,
        key=None,
        method='GET',
        query=None,
        headers=None,
        stream=False,
    ):
        request = super().build_request(
            bucket,
            key,
            method,
            query,
            headers,
            stream,
        )

        request['headers'].update({
            'Host': request['host'],
            'X-Amz-Date': request['now'].strftime('%Y%m%dT%H%M%SZ'),
        })

        if self.session_token:
            request['headers']['x-amz-security-token'] = self.session_token

        request['signed_headers'] = sorted(
            h.strip()
            for h, _ in request['headers'].lower_items()
            if h in ('host', 'content-type', 'content-md5')
            or h.startswith('x-amz-')
        )

        return request

    def date_to_sign(self, request):
        return request['headers']['X-Amz-Date']

    def sign_request(self, request):
        bundle = self.sign_v4(request)
        request['headers']['Authorization'] = 'AWS4-HMAC-SHA256 ' + \
            ','.join([
                'Credential=' + bundle['credential'],
                'SignedHeaders=' + ';'.join(request['signed_headers']),
                'Signature=' + quote(bundle['signature'], safe='-_.~'),
            ])


class S3SessionV4Query(S3SessionV4):
    def build_request(
        self,
        bucket=None,
        key=None,
        method='GET',
        query=None,
        headers=None,
        stream=False,
    ):
        request = super().build_request(
            bucket,
            key,
            method,
            query,
            headers,
            stream,
        )

        request['headers'].update({
            'Host': request['host'],
        })
        scope = [
            request['now'].strftime('%Y%m%d'),
            self.region,
            's3',
            'aws4_request',
        ]
        for k, v in {
            'X-Amz-Expires': str(self.default_expiration),
            'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
            'X-Amz-Credential': quote(
                self.access_key + '/' + '/'.join(scope),
                safe='-_.~'),
            'X-Amz-Date': request['now'].strftime('%Y%m%dT%H%M%SZ'),
        }.items():
            request['query'].setdefault(k, v)

        if self.session_token:
            request['query']['X-Amz-Security-Token'] = quote(
                self.session_token, safe='-_.~')

        request['signed_headers'] = sorted(
            h.strip()
            for h, _ in request['headers'].lower_items()
            if h in ('host', 'content-type', 'content-md5')
            or h.startswith('x-amz-')
        )

        return request

    def date_to_sign(self, request):
        return request['query']['X-Amz-Date']

    def sign_request(self, request):
        request['query'].setdefault(
            'X-Amz-SignedHeaders',
            '%3B'.join(request['signed_headers']),
        )
        bundle = self.sign_v4(request)
        request['query'].update({
            'X-Amz-Signature': bundle['signature'],
            'X-Amz-SignedHeaders': '%3B'.join(
                request['signed_headers']),
        })


TEST_BODY = os.urandom(32)


class InputErrorsMixin(object):
    session_cls = None

    @classmethod
    def setUpClass(cls):
        super(InputErrorsMixin, cls).setUpClass()
        cls.conn = cls.session_cls(
            get_opt('endpoint', None),
            get_opt('access_key1', None),
            get_opt('secret_key1', None),
            get_opt('region', 'us-east-1'),
            get_opt('session_token1', None),
        )

    def assertOK(self, resp, expected_body=b''):
        respbody = resp.content
        if not isinstance(respbody, str):
            try:
                respbody = respbody.decode('utf8')
            except UnicodeError:
                pass  # just trying to improve the error message
        self.assertEqual(
            (resp.status_code, resp.reason),
            (200, 'OK'),
            respbody)
        if expected_body is not None:
            self.assertEqual(resp.content, expected_body)

    def assertSHA256Mismatch(self, resp, sha_in_headers, sha_of_body):
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'),
            respbody)
        self.assertIn('<Code>XAmzContentSHA256Mismatch</Code>', respbody)
        self.assertIn("<Message>The provided 'x-amz-content-sha256' header "
                      "does not match what was computed.</Message>",
                      respbody)
        self.assertIn('<ClientComputedContentSHA256>%s'
                      '</ClientComputedContentSHA256>'
                      % sha_in_headers, respbody)
        self.assertIn('<S3ComputedContentSHA256>%s</S3ComputedContentSHA256>'
                      % sha_of_body, respbody)

    def assertInvalidDigest(self, resp, md5_in_headers):
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'),
            respbody)
        self.assertIn('<Code>InvalidDigest</Code>', respbody)
        self.assertIn("<Message>The Content-MD5 you specified was "
                      "invalid.</Message>",
                      respbody)
        # TODO: AWS provides this, but swift doesn't (yet)
        # self.assertIn('<Content-MD5>%s</Content-MD5>' % md5_in_headers,
        #               respbody)

    def assertBadDigest(self, resp, md5_in_headers, md5_of_body):
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'),
            respbody)
        self.assertIn('<Code>BadDigest</Code>', respbody)
        self.assertIn("<Message>The Content-MD5 you specified did not match "
                      "what we received.</Message>",
                      respbody)
        # Yes, really -- AWS needs b64 in headers, but reflects back hex
        self.assertIn('<ExpectedDigest>%s</ExpectedDigest>' % binascii.hexlify(
            base64.b64decode(md5_in_headers)).decode('ascii'), respbody)
        # TODO: AWS provides this, but swift doesn't (yet)
        # self.assertIn('<CalculatedDigest>%s</CalculatedDigest>'
        #               % md5_of_body, respbody)

    def assertIncompleteBody(
        self,
        resp,
        bytes_provided=None,
        bytes_expected=None,
    ):
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>IncompleteBody</Code>', resp.content)
        if bytes_provided is None:
            self.assertIn(b'<Message>The request body terminated '
                          b'unexpectedly</Message>',
                          resp.content)
            self.assertNotIn(b'<NumberBytesExpected>', resp.content)
            self.assertNotIn(b'<NumberBytesProvided>', resp.content)
        else:
            self.assertIn(b'<Message>You did not provide the number of bytes '
                          b'specified by the Content-Length HTTP header'
                          b'</Message>',
                          resp.content)
            self.assertIn(b'<NumberBytesExpected>%d</NumberBytesExpected>'
                          % bytes_expected,
                          resp.content)
            self.assertIn(b'<NumberBytesProvided>%d</NumberBytesProvided>'
                          % bytes_provided,
                          resp.content)

    def test_get_service_no_sha(self):
        resp = self.conn.make_request()
        self.assertOK(resp, None)

    def test_get_service_invalid_sha(self):
        resp = self.conn.make_request(headers={
            'x-amz-content-sha256': 'invalid'})
        # (!) invalid doesn't matter on GET, at least most of the time
        self.assertOK(resp, None)

    def test_get_service_bad_sha(self):
        resp = self.conn.make_request(headers={
            'x-amz-content-sha256': _sha256(b'not the body')})
        # (!) mismatch doesn't matter on GET, either
        self.assertOK(resp, None)

    def test_get_service_good_sha(self):
        resp = self.conn.make_request(headers={
            'x-amz-content-sha256': EMPTY_SHA256})
        self.assertOK(resp, None)

    def test_get_service_unsigned(self):
        resp = self.conn.make_request(headers={
            'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, None)

    def test_head_service_no_sha(self):
        resp = self.conn.make_request(method='HEAD')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (405, 'Method Not Allowed'))

    def test_head_service_invalid_sha(self):
        resp = self.conn.make_request(method='HEAD', headers={
            'x-amz-content-sha256': 'invalid'})
        self.assertEqual(
            (resp.status_code, resp.reason),
            (405, 'Method Not Allowed'))

    def test_head_service_bad_sha(self):
        resp = self.conn.make_request(method='HEAD', headers={
            'x-amz-content-sha256': _sha256(b'not the body')})
        self.assertEqual(
            (resp.status_code, resp.reason),
            (405, 'Method Not Allowed'))

    def test_head_service_good_sha(self):
        resp = self.conn.make_request(method='HEAD', headers={
            'x-amz-content-sha256': EMPTY_SHA256})
        self.assertEqual(
            (resp.status_code, resp.reason),
            (405, 'Method Not Allowed'))

    def test_head_service_unsigned(self):
        resp = self.conn.make_request(method='HEAD', headers={
            'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertEqual(
            (resp.status_code, resp.reason),
            (405, 'Method Not Allowed'))

    def test_get_bucket_no_md5_no_sha(self):
        resp = self.conn.make_request(self.bucket_name)
        self.assertOK(resp, None)

    def test_get_bucket_no_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'x-amz-content-sha256': 'invalid'})
        self.assertOK(resp, None)

    def test_get_bucket_no_md5_bad_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'x-amz-content-sha256': _sha256(b'not the body')})
        self.assertOK(resp, None)

    def test_get_bucket_no_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertOK(resp, None)

    def test_get_bucket_no_md5_unsigned(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, None)

    def test_get_bucket_good_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'content-md5': _md5(TEST_BODY)})
        self.assertOK(resp, None)

    def test_get_bucket_good_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertOK(resp, None)

    def test_head_bucket_no_md5_no_sha(self):
        resp = self.conn.make_request(self.bucket_name, method='HEAD')
        self.assertOK(resp)

    def test_head_bucket_no_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            method='HEAD',
            headers={
                'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertOK(resp)

    def test_head_bucket_good_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            method='HEAD',
            headers={
                'content-md5': _md5(TEST_BODY)})
        self.assertOK(resp)

    def test_head_bucket_good_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            method='HEAD',
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertOK(resp)

    def test_no_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY)
        self.assertOK(resp)

    def get_response_put_object_no_md5_no_sha_no_content_length(self):
        request = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
        )
        self.conn.sign_request(request)
        # requests is not our friend here; it's going to try *real hard* to
        # either send a "Content-Length" or "Transfer-encoding: chunked" header
        # so dip down to our bufferedhttp to do the sending/parsing
        host, port = parse_socket_string(request['host'], None)
        if port:
            port = int(port)
        conn = bufferedhttp.http_connect_raw(
            host,
            port,
            request['method'],
            request['path'],
            request['headers'],
            '&'.join('%s=%s' % (k, v) for k, v in request['query'].items()),
            request['https']
        )
        conn.send(TEST_BODY)
        return conn.getresponse()

    def test_no_md5_no_sha_no_content_length(self):
        resp = self.get_response_put_object_no_md5_no_sha_no_content_length()
        body = resp.read()
        self.assertEqual(resp.status, 411, body)
        self.assertIn(b'<Code>MissingContentLength</Code>', body)
        self.assertIn(b'<Message>You must provide the Content-Length HTTP '
                      b'header.</Message>', body)

    def test_no_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'invalid'})
        self.assertSHA256Mismatch(resp, 'invalid', _sha256(TEST_BODY))

    def test_no_md5_invalid_sha_ucase(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'X-AMZ-CONTENT-SHA256': 'INVALID'})
        # Despite the upper-cased header name in the request,
        # the error message has it lower
        self.assertSHA256Mismatch(resp, 'INVALID', _sha256(TEST_BODY))

    def test_no_md5_bad_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': EMPTY_SHA256})
        self.assertSHA256Mismatch(resp, EMPTY_SHA256, _sha256(TEST_BODY))

    def test_no_md5_bad_sha_ucase(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'X-AMZ-CONTENT-SHA256': EMPTY_SHA256.upper()})
        # Despite the upper-cased header name in the request,
        # the error message has it lower
        self.assertSHA256Mismatch(
            resp, EMPTY_SHA256.upper(), _sha256(TEST_BODY))

    def test_no_md5_bad_sha_empty_body(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={'x-amz-content-sha256': _sha256(b'not the body')})
        self.assertSHA256Mismatch(resp, _sha256(b'not the body'), EMPTY_SHA256)

    def test_no_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertOK(resp)

    def test_no_md5_good_sha_ucase(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': _sha256(TEST_BODY).upper()})
        self.assertOK(resp)

    def test_no_md5_good_sha_no_content_length(self):
        request = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={'x-amz-content-sha256': _sha256(TEST_BODY)},
        )
        self.conn.sign_request(request)
        # requests is not our friend here; it's going to try *real hard* to
        # either send a "Content-Length" or "Transfer-encoding: chunked" header
        # so dip down to our bufferedhttp to do the sending/parsing
        host, port = parse_socket_string(request['host'], None)
        if port:
            port = int(port)
        conn = bufferedhttp.http_connect_raw(
            host,
            port,
            request['method'],
            request['path'],
            request['headers'],
            '&'.join('%s=%s' % (k, v) for k, v in request['query'].items()),
            request['https']
        )
        conn.send(TEST_BODY)
        resp = conn.getresponse()
        body = resp.read()
        self.assertEqual(resp.status, 411, body)
        self.assertIn(b'<Code>MissingContentLength</Code>', body)
        self.assertIn(b'<Message>You must provide the Content-Length HTTP '
                      b'header.</Message>', body)

    def test_no_md5_unsigned(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

    def test_no_md5_unsigned_lcase(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'unsigned-payload'})
        self.assertSHA256Mismatch(resp, 'unsigned-payload', _sha256(TEST_BODY))

    def test_invalid_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': 'invalid'})
        self.assertInvalidDigest(resp, 'invalid')

    def test_invalid_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': 'invalid',
                     'x-amz-content-sha256': 'invalid'})
        self.assertInvalidDigest(resp, 'invalid')

    def test_invalid_md5_bad_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': 'invalid',
                     'x-amz-content-sha256': EMPTY_SHA256})
        self.assertInvalidDigest(resp, 'invalid')

    def test_invalid_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': 'invalid',
                     'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertInvalidDigest(resp, 'invalid')

    def test_bad_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': _md5(b'')})
        self.assertBadDigest(resp, _md5(b''), _md5(TEST_BODY))

    def test_bad_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(b''),
                'x-amz-content-sha256': 'invalid'})
        # Neither is right; "mismatched" sha256 trumps
        self.assertSHA256Mismatch(resp, 'invalid', _sha256(TEST_BODY))

    def test_bad_md5_bad_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(b''),
                'x-amz-content-sha256': EMPTY_SHA256})
        # Neither is right; bad sha256 trumps
        self.assertSHA256Mismatch(resp, EMPTY_SHA256, _sha256(TEST_BODY))

    def test_bad_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(b''),
                'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertBadDigest(resp, _md5(b''), _md5(TEST_BODY))

    def test_good_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': _md5(TEST_BODY)})
        self.assertOK(resp)

    def test_good_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': 'invalid'})
        self.assertSHA256Mismatch(resp, 'invalid', _sha256(TEST_BODY))

    def test_good_md5_bad_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': EMPTY_SHA256})
        self.assertSHA256Mismatch(resp, EMPTY_SHA256, _sha256(TEST_BODY))

    def test_good_md5_good_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY)})
        self.assertOK(resp)

    def test_get_object_no_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(self.bucket_name, obj_name)
        self.assertOK(resp, TEST_BODY)

    def test_get_object_invalid_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            headers={'x-amz-content-sha256': 'invalid'})
        self.assertOK(resp, TEST_BODY)

    def test_get_object_bad_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            headers={'x-amz-content-sha256': _sha256(b'not the body')})
        self.assertOK(resp, TEST_BODY)

    def test_get_object_good_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            headers={'x-amz-content-sha256': _sha256()})
        self.assertOK(resp, TEST_BODY)

    def test_get_object_unsigned(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, TEST_BODY)

    def test_head_object_no_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='HEAD')
        self.assertOK(resp)

    def test_head_object_invalid_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='HEAD',
            headers={'x-amz-content-sha256': 'invalid'})
        self.assertOK(resp)

    def test_head_object_bad_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='HEAD',
            headers={'x-amz-content-sha256': _sha256(b'not the body')})
        self.assertOK(resp)

    def test_head_object_good_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='HEAD',
            headers={'x-amz-content-sha256': _sha256()})
        self.assertOK(resp)

    def test_head_object_unsigned(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='HEAD',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)


class TestV4AuthHeaders(InputErrorsMixin, BaseS3TestCaseWithBucket):
    session_cls = S3SessionV4Headers

    def assertMissingSHA256(self, resp):
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Missing required header for this '
                      b'request: x-amz-content-sha256</Message>',
                      resp.content)

    def assertInvalidSHA256(self, resp, sha_in_headers):
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'),
            respbody)
        self.assertIn('<Code>InvalidArgument</Code>', respbody)
        self.assertIn('<Message>x-amz-content-sha256 must be '
                      'UNSIGNED-PAYLOAD', respbody)
        # There can be a whole list here, but Swift only supports these
        # two at the moment
        self.assertIn('or a valid sha256 value.</Message>', respbody)
        self.assertIn('<ArgumentName>x-amz-content-sha256</ArgumentName>',
                      respbody)
        self.assertIn('<ArgumentValue>%s</ArgumentValue>' % sha_in_headers,
                      respbody)

    def test_get_service_no_sha(self):
        resp = self.conn.make_request()
        self.assertMissingSHA256(resp)

    def test_get_service_invalid_sha(self):
        resp = self.conn.make_request(headers={
            'x-amz-content-sha256': 'invalid'})
        self.assertInvalidSHA256(resp, 'invalid')

    def test_head_service_no_sha(self):
        resp = self.conn.make_request(method='HEAD')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'))

    def test_head_service_invalid_sha(self):
        resp = self.conn.make_request(method='HEAD', headers={
            'x-amz-content-sha256': 'invalid'})
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'))

    def test_get_bucket_no_md5_no_sha(self):
        resp = self.conn.make_request(self.bucket_name)
        self.assertMissingSHA256(resp)

    def test_get_bucket_no_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'x-amz-content-sha256': 'invalid'})
        self.assertInvalidSHA256(resp, 'invalid')

    def test_get_bucket_good_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            headers={
                'content-md5': _md5(TEST_BODY)})
        self.assertMissingSHA256(resp)

    def test_head_bucket_no_md5_no_sha(self):
        resp = self.conn.make_request(self.bucket_name, method='HEAD')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'))

    def test_head_bucket_good_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            method='HEAD',
            headers={
                'content-md5': _md5(TEST_BODY)})
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'))

    def test_no_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY)
        self.assertMissingSHA256(resp)

    def test_no_md5_no_sha_no_content_length(self):
        resp = self.get_response_put_object_no_md5_no_sha_no_content_length()
        body = resp.read()
        self.assertEqual(resp.status, 400, body)
        self.assertIn(b'<Code>InvalidRequest</Code>', body)
        self.assertIn(b'<Message>Missing required header for this '
                      b'request: x-amz-content-sha256</Message>',
                      body)

    def test_no_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'invalid'})
        self.assertInvalidSHA256(resp, 'invalid')

    def test_no_md5_invalid_sha_ucase(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'X-AMZ-CONTENT-SHA256': 'INVALID'})
        # Despite the upper-cased header name in the request,
        # the error message has it lower
        self.assertInvalidSHA256(resp, 'INVALID')

    def test_no_md5_unsigned_lcase(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'unsigned-payload'})
        self.assertInvalidSHA256(resp, 'unsigned-payload')

    def test_invalid_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': 'invalid'})
        self.assertMissingSHA256(resp)

    def test_invalid_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': 'invalid',
                     'x-amz-content-sha256': 'invalid'})
        # Both invalid; sha256 trumps
        self.assertInvalidSHA256(resp, 'invalid')

    def test_bad_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': _md5(b'')})
        self.assertMissingSHA256(resp)

    def test_bad_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(b''),
                'x-amz-content-sha256': 'invalid'})
        # Neither is right; invalid sha256 trumps
        self.assertInvalidSHA256(resp, 'invalid')

    def test_good_md5_no_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'content-md5': _md5(TEST_BODY)})
        self.assertMissingSHA256(resp)

    def test_good_md5_invalid_sha(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': 'invalid'})
        self.assertInvalidSHA256(resp, 'invalid')

    def test_get_object_no_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(self.bucket_name, obj_name)
        self.assertMissingSHA256(resp)

    def test_get_object_invalid_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            headers={'x-amz-content-sha256': 'invalid'})
        self.assertInvalidSHA256(resp, 'invalid')

    def test_head_object_no_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='HEAD')
        # Since it's a HEAD, all we get is status
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'))

    def test_head_object_invalid_sha(self):
        obj_name = self.create_name('get-object')
        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            obj_name,
            method='HEAD',
            headers={'x-amz-content-sha256': 'invalid'})
        # Since it's a HEAD, all we get is status
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'))


class TestV4AuthQuery(InputErrorsMixin, BaseS3TestCaseWithBucket):
    session_cls = S3SessionV4Query


class TestV2AuthHeaders(InputErrorsMixin, BaseS3TestCaseWithBucket):
    session_cls = S3SessionV2Headers


class TestV2AuthQuery(InputErrorsMixin, BaseS3TestCaseWithBucket):
    session_cls = S3SessionV2Query
