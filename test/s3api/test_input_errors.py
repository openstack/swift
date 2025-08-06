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
import gzip
import hashlib
import hmac
import os
import requests
import requests.models
import struct
import zlib
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


def _crc32(payload=b''):
    return base64.b64encode(
        struct.pack('!I', zlib.crc32(payload))
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
            '%s:%s' % (h, request['headers'][h].strip())
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

    def sign_chunk(self, request, previous_signature, current_chunk_sha):
        scope = [
            request['now'].strftime('%Y%m%d'),
            self.region,
            's3',
            'aws4_request',
        ]
        string_to_sign_lines = [
            'AWS4-HMAC-SHA256-PAYLOAD',
            self.date_to_sign(request),
            '/'.join(scope),
            previous_signature,
            _sha256(),  # ??
            current_chunk_sha,
        ]
        key = 'AWS4' + self.secret_key
        for piece in scope:
            key = _hmac(key, piece, hashlib.sha256)
        return binascii.hexlify(_hmac(
            key,
            '\n'.join(string_to_sign_lines),
            hashlib.sha256
        )).decode('ascii')

    def sign_trailer(self, request, previous_signature, trailer):
        # rough canonicalization
        trailer = trailer.replace(b'\r', b'').replace(b' ', b'')
        # AWS always wants at least the newline
        if not trailer:
            trailer = b'\n'
        scope = [
            request['now'].strftime('%Y%m%d'),
            self.region,
            's3',
            'aws4_request',
        ]
        string_to_sign_lines = [
            'AWS4-HMAC-SHA256-TRAILER',
            self.date_to_sign(request),
            '/'.join(scope),
            previous_signature,
            _sha256(trailer),
        ]
        key = 'AWS4' + self.secret_key
        for piece in scope:
            key = _hmac(key, piece, hashlib.sha256)
        return binascii.hexlify(_hmac(
            key,
            '\n'.join(string_to_sign_lines),
            hashlib.sha256
        )).decode('ascii')


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

    def test_good_md5_good_sha_good_crc(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'good-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
            })
        self.assertOK(resp)

    def test_good_md5_good_sha_good_crc_declared(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'good-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY),
                # can flag that you're going to send it
                'x-amz-sdk-checksum-algorithm': 'CRC32',
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
            })
        self.assertOK(resp)

    def test_good_md5_good_sha_no_crc_but_declared(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'missing-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY),
                # but if you flag it, you gotta send it
                'x-amz-sdk-checksum-algorithm': 'CRC32',
            })
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>x-amz-sdk-checksum-algorithm specified, but '
                      b'no corresponding x-amz-checksum-* or x-amz-trailer '
                      b'headers were found.</Message>', resp.content)

    def test_good_md5_good_sha_good_crc_algo_mismatch(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'good-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-sdk-checksum-algorithm': 'CRC32C',
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
            })
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        # Note that if there's a mismatch between what you flag and what you
        # send, the message isn't super clear
        self.assertIn(b'<Message>Value for x-amz-sdk-checksum-algorithm '
                      b'header is invalid.</Message>', resp.content)

    def test_good_md5_good_sha_invalid_crc_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'invalid-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': 'bad'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Value for x-amz-checksum-crc32 header is '
                      b'invalid.</Message>', resp.content)

    def test_good_md5_good_sha_bad_crc_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(b'not the body')})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>BadDigest</Code>', resp.content)
        self.assertIn(b'<Message>The CRC32 you specified did not match the '
                      b'calculated checksum.</Message>', resp.content)

    def test_good_md5_bad_sha_bad_crc_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'content-md5': _md5(TEST_BODY),
                'x-amz-content-sha256': _sha256(b'not the body'),
                'x-amz-checksum-crc32': _crc32(b'not the body')})
        # SHA256 trumps checksum
        self.assertSHA256Mismatch(
            resp, _sha256(b'not the body'), _sha256(TEST_BODY))

    def test_no_md5_good_sha_good_crc_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY)})
        self.assertOK(resp)

    def test_no_md5_good_sha_unsupported_crc_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-bad': _crc32(TEST_BODY)})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>The algorithm type you specified in '
                      b'x-amz-checksum- header is invalid.</Message>',
                      resp.content)

    def test_no_md5_good_sha_multiple_crc_in_headers(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32c': _crc32(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY)})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

    def test_no_md5_good_sha_multiple_crc_in_headers_algo_mismatch(self):
        # repeats trump the algo mismatch
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'sha256',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32c': _crc32(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY)})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

    def test_no_md5_good_sha_crc_in_trailer_but_not_streaming(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'crc32',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>MalformedTrailerError</Code>', resp.content)
        self.assertIn(b'<Message>The request contained trailing data that was '
                      b'not well-formed or did not conform to our published '
                      b'schema.</Message>', resp.content)

    def test_no_md5_good_sha_duplicated_crc_in_trailer_algo_mismatch(self):
        # repeats trump the algo mismatch
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'sha256',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-trailer': 'x-amz-checksum-crc32, x-amz-checksum-crc32'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

    def test_no_md5_good_sha_multiple_crc_in_trailer_algo_mismatch(self):
        # repeats trump the algo mismatch
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'sha256',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-trailer': 'x-amz-checksum-crc32, x-amz-checksum-crc32c'}
        )
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

    def test_no_md5_good_sha_different_crc_in_trailer_and_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'crc32',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-trailer': 'x-amz-checksum-crc32c'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header'
                      b'</Message>', resp.content)

    def test_no_md5_good_sha_same_crc_in_trailer_and_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'crc32',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header'
                      b'</Message>', resp.content)

    def test_no_md5_good_sha_multiple_crc_in_trailer_and_header(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'crc32',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-trailer': 'x-amz-checksum-crc32, x-amz-checksum-crc32c'}
        )
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

    def test_no_md5_good_sha_multiple_crc_in_header_and_trailer(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-sdk-checksum-algorithm': 'crc32',
                'x-amz-content-sha256': _sha256(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-checksum-sha256': _sha256(TEST_BODY),
                'x-amz-trailer': 'x-amz-checksum-crc32'}
        )
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

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

    def test_no_md5_good_sha_chunk_encoding_declared_ok(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={'x-amz-content-sha256': _sha256(TEST_BODY),
                     'content-encoding': 'aws-chunked'})  # but not really
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, TEST_BODY)
        self.assertEqual(resp.headers.get('Content-Encoding'), 'aws-chunked')

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

    def test_no_md5_streaming_unsigned_no_encoding_no_length(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER'})
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (411, 'Length Required'),
            respbody)
        self.assertIn('<Code>MissingContentLength</Code>', respbody)
        # NB: we *do* provide Content-Length (or rather, urllib does)
        # they really mean X-Amz-Decoded-Content-Length
        self.assertIn("<Message>You must provide the Content-Length HTTP "
                      "header.</Message>",
                      respbody)

    def test_no_md5_streaming_unsigned_bad_decoded_content_length(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': 'not an int'})
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (411, 'Length Required'),
            respbody)
        self.assertIn('<Code>MissingContentLength</Code>', respbody)
        # NB: we *do* provide Content-Length (or rather, urllib does)
        # they really mean X-Amz-Decoded-Content-Length
        self.assertIn("<Message>You must provide the Content-Length HTTP "
                      "header.</Message>",
                      respbody)

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

    def assertSignatureMismatch(self, resp, sts_first_line='AWS4-HMAC-SHA256'):
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (403, 'Forbidden'),
            respbody)
        self.assertIn('<Code>SignatureDoesNotMatch</Code>', respbody)
        self.assertIn('<Message>The request signature we calculated does not '
                      'match the signature you provided. Check your key and '
                      'signing method.</Message>', respbody)
        self.assertIn('<AWSAccessKeyId>', respbody)
        self.assertIn(f'<StringToSign>{sts_first_line}\n', respbody)
        self.assertIn('<SignatureProvided>', respbody)
        self.assertIn('<StringToSignBytes>', respbody)
        self.assertIn('<CanonicalRequest>', respbody)
        self.assertIn('<CanonicalRequestBytes>', respbody)

    def assertMalformedTrailer(self, resp):
        respbody = resp.content
        if not isinstance(respbody, str):
            respbody = respbody.decode('utf8')
        self.assertEqual(
            (resp.status_code, resp.reason),
            (400, 'Bad Request'),
            respbody)
        self.assertIn('<Code>MalformedTrailerError</Code>', respbody)
        self.assertIn('<Message>The request contained trailing data that was '
                      'not well-formed or did not conform to our published '
                      'schema.</Message>', respbody)

    def assertUnsupportedTrailerHeader(self, resp):
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>The value specified in the x-amz-trailer '
                      b'header is not supported</Message>',
                      resp.content)

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

    def test_no_md5_no_sha_good_crc(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'bad-checksum',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-checksum-crc32': _crc32(TEST_BODY)})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Missing required header for this request: '
                      b'x-amz-content-sha256</Message>', resp.content)

    def test_strm_unsgnd_pyld_trl_not_encoded(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_encoding_declared_not_encoded(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_no_trailer_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, TEST_BODY)
        self.assertNotIn('Content-Encoding', resp.headers)

    def test_strm_unsgnd_pyld_trl_te_chunked_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        # Use iter(list-of-bytes) to force requests to send
        # Transfer-Encoding: chunked
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=iter([chunked_body]),
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_te_chunked_no_decoded_content_length(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        # Use iter(list-of-bytes) to force requests to send
        # Transfer-Encoding: chunked
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=iter([chunked_body]),
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked'})
        self.assertEqual(resp.status_code, 411, resp.content)
        self.assertIn(b'<Code>MissingContentLength</Code>', resp.content)
        self.assertIn(b'<Message>You must provide the Content-Length HTTP '
                      b'header.</Message>', resp.content)

    def test_strm_unsgnd_pyld_trl_crc_header_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, TEST_BODY)
        self.assertNotIn('Content-Encoding', resp.headers)

    def test_strm_unsgnd_pyld_trl_crc_header_x_amz_checksum_type_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                # unexpected with a PUT but tolerated...
                'x-amz-checksum-type': 'COMPOSITE',
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_crc_header_x_amz_checksum_algorithm_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                # unexpected with a PUT but tolerated...
                'x-amz-checksum-algorithm': 'crc32',
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_crc_header_algo_mismatch(self):
        chunked_body = b'nonsense ignored'
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-sdk-checksum-algorithm': 'sha256',
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Value for x-amz-sdk-checksum-algorithm '
                      b'header is invalid.</Message>', resp.content)

    def test_strm_unsgnd_pyld_trl_multiple_crc_header(self):
        chunked_body = b'nonsense ignored'
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-checksum-crc32c': _crc32(TEST_BODY),
                'x-amz-checksum-crc32': _crc32(TEST_BODY),
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

    def test_strm_unsgnd_pyld_trl_crc_header_mismatch(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-sdk-checksum-algorithm': 'crc32',
                'x-amz-checksum-crc32': _crc32(b'not the test body'),
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>BadDigest</Code>', resp.content)
        self.assertIn(b'<Message>The CRC32 you specified did not match the '
                      b'calculated checksum.</Message>', resp.content)

    def test_strm_unsgnd_pyld_trl_declared_algo_declared_no_trailer_sent(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-sdk-checksum-algorithm': 'crc32',
                'x-amz-trailer': 'x-amz-checksum-crc32',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertMalformedTrailer(resp)

    def test_strm_unsgnd_pyld_trl_declared_no_trailer_sent(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-trailer': 'x-amz-checksum-crc32',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertMalformedTrailer(resp)

    def test_strm_sgnd_pyld_trl_no_trailer(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256':
                    'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s%s' % (
                len(chunk), chunk_sig.encode('ascii'), chunk,
                b'\r\n' if chunk else b''))
            prev_sig = chunk_sig
        trailers = b''
        body_parts.append(trailers)
        trailer_sig = self.conn.sign_trailer(req, prev_sig, trailers)
        body_parts.append(
            b'x-amz-trailer-signature:%s\r\n' % trailer_sig.encode('ascii'))
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_no_trailer_tr_chunked_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=iter([chunked_body]),
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_with_trailer_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_with_comma_in_trailer_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32,'})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_with_commas_in_trailer_1(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': ', x-amz-checksum-crc32, ,'})
        self.assertUnsupportedTrailerHeader(resp)

    def test_strm_unsgnd_pyld_trl_with_commas_in_trailer_2(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': ', x-amz-checksum-crc32'})
        self.assertUnsupportedTrailerHeader(resp)

    def test_strm_unsgnd_pyld_trl_with_commas_in_trailer_3(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': ',x-amz-checksum-crc32'})
        self.assertUnsupportedTrailerHeader(resp)

    def test_strm_unsgnd_pyld_trl_with_commas_in_trailer_4(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32,,'})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_with_commas_in_trailer_5(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32, ,'})
        self.assertUnsupportedTrailerHeader(resp)

    def test_strm_unsgnd_pyld_trl_with_commas_in_trailer_6(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32, '})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_with_trailer_checksum_mismatch(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(b"not the body")}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>BadDigest</Code>', resp.content)
        self.assertIn(b'<Message>The CRC32 you specified did not match the '
                      b'calculated checksum.</Message>', resp.content)

    def test_strm_unsgnd_pyld_trl_with_trailer_checksum_invalid(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {"not=base-64"}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Value for x-amz-checksum-crc32 trailing '
                      b'header is invalid.</Message>', resp.content)

    def test_strm_unsgnd_pyld_trl_content_sha256_in_trailer(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-content-sha256: {_sha256(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-content-sha256'})
        self.assertUnsupportedTrailerHeader(resp)

    def test_strm_unsgnd_pyld_trl_with_trailer_no_cr(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_with_trailer_no_lf(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_with_trailer_no_crlf(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_with_trailer_extra_line_before(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            '\r\n',
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertMalformedTrailer(resp)

    def test_strm_unsgnd_pyld_trl_extra_line_after_trailer_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            '\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_with_trailer_extra_line_junk_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            '\r\n',
            '\xff\xde\xad\xbe\xef\xff',
        ]).encode('latin1')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertOK(resp)  # really??

    def test_strm_unsgnd_pyld_trl_extra_lines_after_trailer_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            '\r\n',
            '\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_mismatch_trailer(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32c'})
        self.assertMalformedTrailer(resp)

    def test_strm_unsgnd_pyld_trl_unsupported_trailer_sent(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-bad: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32c'})
        self.assertMalformedTrailer(resp)

    def test_strm_unsgnd_pyld_trl_non_checksum_trailer(self):
        def do_test(trailer, value):
            chunked_body = b''.join(
                b'%x\r\n%s\r\n' % (len(chunk), chunk)
                for chunk in [TEST_BODY, b''])[:-2]
            chunked_body += ''.join([
                f'{trailer}: {value}\r\n',
            ]).encode('ascii')
            resp = self.conn.make_request(
                self.bucket_name,
                'test-obj',
                method='PUT',
                body=chunked_body,
                headers={
                    'x-amz-content-sha256':
                        'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                    'content-encoding': 'aws-chunked',
                    'x-amz-decoded-content-length': str(len(TEST_BODY)),
                    'x-amz-trailer': trailer})
            self.assertUnsupportedTrailerHeader(resp)

        do_test('foo', 'bar')
        do_test('content-md5', _md5(TEST_BODY))
        do_test('x-amz-content-sha256', _sha256(TEST_BODY))

    def test_strm_unsgnd_pyld_trl_unsupported_trailer_declared(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-bad'})
        self.assertUnsupportedTrailerHeader(resp)

    def test_strm_unsgnd_pyld_trl_multiple_checksum_trailers(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            f'x-amz-checksum-sha256: {_sha256(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer':
                    'x-amz-checksum-crc32, x-amz-checksum-sha256'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>InvalidRequest</Code>', resp.content)
        self.assertIn(b'<Message>Expecting a single x-amz-checksum- header. '
                      b'Multiple checksum Types are not allowed.</Message>',
                      resp.content)

    def test_strm_unsgnd_pyld_trl_multiple_trailers_unsupported(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            'x-amz-foo: bar\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer':
                    'x-amz-checksum-crc32, x-amz-foo'})
        self.assertUnsupportedTrailerHeader(resp)

    def test_strm_unsgnd_pyld_trl_extra_trailer(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            'bonus: trailer\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertMalformedTrailer(resp)

    def test_strm_unsgnd_pyld_trl_bad_then_good_trailer_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY[:-1])}\r\n',
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertOK(resp)

    def test_strm_unsgnd_pyld_trl_good_then_bad_trailer(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY[:-1])}\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertEqual(resp.status_code, 400, resp.content)
        self.assertIn(b'<Code>BadDigest</Code>', resp.content)
        self.assertIn(b'<Message>The CRC32 you specified did not match the '
                      b'calculated checksum.</Message>', resp.content)

    def test_strm_unsgnd_pyld_trl_extra_line_then_trailer_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])[:-2]
        chunked_body += ''.join([
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n',
            '\r\n',
            'bonus: trailer\r\n',
        ]).encode('ascii')
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY)),
                'x-amz-trailer': 'x-amz-checksum-crc32'})
        self.assertOK(resp)  # ???

    def test_strm_unsgnd_pyld_trl_no_cr(self):
        chunked_body = b''.join(
            b'%x\n%s\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_no_lf(self):
        chunked_body = b''.join(
            b'%x\r%s\r' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_no_trailing_lf(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        chunked_body = chunked_body[:-1]
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_no_trailing_crlf_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        chunked_body = chunked_body[:-2]
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        # dafuk?
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, TEST_BODY)
        self.assertNotIn('Content-Encoding', resp.headers)

    def test_strm_unsgnd_pyld_trl_cl_matches_decoded_cl(self):
        chunked_body = b''.join(
            b'%x\r\n%s' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(chunked_body))})
        self.assertIncompleteBody(resp)

    def test_strm_sgnd_pyld_cl_matches_decoded_cl(self):
        # Used to calculate our bad decoded-content-length
        dummy_body = b''.join(
            b'%x;chunk-signature=%064x\r\n%s\r\n' % (len(chunk), 0, chunk)
            for chunk in [TEST_BODY, b''])
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256':
                    'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(dummy_body))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_no_zero_chunk(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_unsgnd_pyld_trl_zero_chunk_mid_stream(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY[:4], b'', TEST_BODY[4:], b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp, 4, len(TEST_BODY))

    def test_strm_unsgnd_pyld_trl_too_many_bytes(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY * 2, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp, 2 * len(TEST_BODY), len(TEST_BODY))

    def test_strm_unsgnd_pyld_trl_no_encoding_ok(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, TEST_BODY)
        self.assertNotIn('Content-Encoding', resp.headers)

    def test_strm_unsgnd_pyld_trl_custom_encoding_ok(self):
        # As best we can tell, AWS doesn't care at all about how
        # > If one or more encodings have been applied to a representation,
        # > the sender that applied the encodings MUST generate a
        # > Content-Encoding header field that lists the content codings in
        # > the order in which they were applied.
        # See https://www.rfc-editor.org/rfc/rfc9110.html#section-8.4
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'foo, aws-chunked, bar',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'})
        self.assertOK(resp, TEST_BODY)
        self.assertIn('Content-Encoding', resp.headers)
        self.assertEqual(resp.headers['Content-Encoding'], 'foo, bar')

    def test_strm_unsgnd_pyld_trl_gzipped_undeclared_ok(self):
        alt_body = gzip.compress(TEST_BODY)
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [alt_body, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'gzip',
                'x-amz-decoded-content-length': str(len(alt_body))})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'},
            stream=True)  # needed so requests won't try to be "helpful"
        read_body = resp.raw.read()
        self.assertEqual(read_body, alt_body)
        self.assertEqual(resp.headers['Content-Length'], str(len(alt_body)))
        self.assertOK(resp)  # already read body
        self.assertIn('Content-Encoding', resp.headers)
        self.assertEqual(resp.headers['Content-Encoding'], 'gzip')

    def test_strm_unsgnd_pyld_trl_gzipped_declared_swapped_ok(self):
        alt_body = gzip.compress(TEST_BODY)
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [alt_body, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked, gzip',
                'x-amz-decoded-content-length': str(len(alt_body))})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'},
            stream=True)
        read_body = resp.raw.read()
        self.assertEqual(read_body, alt_body)
        self.assertEqual(resp.headers['Content-Length'], str(len(alt_body)))
        self.assertOK(resp)  # already read body
        self.assertIn('Content-Encoding', resp.headers)
        self.assertEqual(resp.headers['Content-Encoding'], 'gzip')

    def test_strm_unsgnd_pyld_trl_gzipped_declared_ok(self):
        alt_body = gzip.compress(TEST_BODY)
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [alt_body, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'gzip, aws-chunked',
                'x-amz-decoded-content-length': str(len(alt_body))})
        self.assertOK(resp)

        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='GET',
            headers={'x-amz-content-sha256': 'UNSIGNED-PAYLOAD'},
            stream=True)
        read_body = resp.raw.read()
        self.assertEqual(read_body, alt_body)
        self.assertEqual(resp.headers['Content-Length'], str(len(alt_body)))
        self.assertOK(resp)  # already read body
        self.assertIn('Content-Encoding', resp.headers)
        self.assertEqual(resp.headers['Content-Encoding'], 'gzip')

    def test_strm_sgnd_pyld_no_signatures(self):
        chunked_body = b''.join(
            b'%x\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_sgnd_pyld_blank_signatures(self):
        chunked_body = b''.join(
            b'%x;chunk-signature=\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp)

    def test_strm_sgnd_pyld_invalid_signatures(self):
        chunked_body = b''.join(
            b'%x;chunk-signature=invalid\r\n%s\r\n' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertSignatureMismatch(resp, 'AWS4-HMAC-SHA256-PAYLOAD')

    def test_strm_sgnd_pyld_bad_signatures(self):
        chunked_body = b''.join(
            b'%x;chunk-signature=%064x\r\n%s\r\n' % (len(chunk), 0, chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertSignatureMismatch(resp, 'AWS4-HMAC-SHA256-PAYLOAD')

    def test_strm_sgnd_pyld_good_signatures_ok(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertOK(resp)

    def test_strm_sgnd_pyld_ragged_chunk_lengths_ok(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str((15 + 8 + 16) * 1024)})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [
                b'x' * 15 * 1024,
                b'y' * 8 * 1024,
                b'z' * 16 * 1024,
                b'',
        ]:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertOK(resp)

    def test_strm_sgnd_pyld_no_zero_chunk(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY]:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertIncompleteBody(resp)

    def test_strm_sgnd_pyld_negative_chunk_length(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'-%x;chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        # AWS reliably 500s at time of writing
        self.assertNotEqual(resp.status_code, 200)

    def test_strm_sgnd_pyld_too_small_chunks(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(9 * 1024)})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [b'x' * 1024, b'y' * 4 * 1024, b'z' * 3 * 1024, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertEqual(
            (resp.status_code, resp.reason),
            (403, 'Forbidden'))  # ???
        respbody = resp.content.decode('utf8')
        self.assertIn('<Code>InvalidChunkSizeError</Code>', respbody)
        self.assertIn("<Message>Only the last chunk is allowed to have a "
                      "size less than 8192 bytes</Message>",
                      respbody)
        # Yeah, it points at the wrong chunk number
        self.assertIn("<Chunk>2</Chunk>", respbody)
        # But at least it complains about the right size!
        self.assertIn("<BadChunkSize>%d</BadChunkSize>" % 1024,
                      respbody)

    def test_strm_sgnd_pyld_spaced_out_chunk_param_ok(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x ; chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertOK(resp)

    def test_strm_sgnd_pyld_spaced_out_chunk_param_value_ok(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature = %s \r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertOK(resp)

    def test_strm_sgnd_pyld_bad_final_signature(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(
                req, prev_sig, _sha256(chunk or b'x'))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s\r\n' % (
                len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertSignatureMismatch(resp, 'AWS4-HMAC-SHA256-PAYLOAD')

    def test_strm_sgnd_pyld_extra_param_before(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(
                b'%x;extra=param;chunk-signature=%s\r\n%s\r\n' % (
                    len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertSignatureMismatch(resp, 'AWS4-HMAC-SHA256-PAYLOAD')

    def test_strm_sgnd_pyld_extra_param_after(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(
                b'%x;chunk-signature=%s;extra=param\r\n%s\r\n' % (
                    len(chunk), chunk_sig.encode('ascii'), chunk))
            prev_sig = chunk_sig
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertIncompleteBody(resp)

    def test_strm_sgnd_pyld_missing_final_chunk(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(TEST_BODY))
        body = b'%x;chunk-signature=%s\r\n%s\r\n' % (
            len(TEST_BODY), chunk_sig.encode('ascii'), TEST_BODY)
        resp = self.conn.send_request(req, body)
        self.assertIncompleteBody(resp)

    def test_strm_sgnd_pyld_trl_ok(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256':
                    'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-trailer': 'x-amz-checksum-crc32',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s%s' % (
                len(chunk), chunk_sig.encode('ascii'), chunk,
                b'\r\n' if chunk else b''))
            prev_sig = chunk_sig
        trailers = (
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n'
        ).encode('ascii')
        body_parts.append(trailers)
        trailer_sig = self.conn.sign_trailer(req, prev_sig, trailers)
        body_parts.append(
            b'x-amz-trailer-signature:%s\r\n' % trailer_sig.encode('ascii'))
        body_parts.append(b'\r\n')
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertOK(resp)

    def test_strm_sgnd_pyld_trl_missing_trl_sig(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256':
                    'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-trailer': 'x-amz-checksum-crc32',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s%s' % (
                len(chunk), chunk_sig.encode('ascii'), chunk,
                b'\r\n' if chunk else b''))
            prev_sig = chunk_sig
        trailers = (
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n'
        ).encode('ascii')
        body_parts.append(trailers)
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertIncompleteBody(resp)

    def test_strm_sgnd_pyld_trl_bad_trl_sig(self):
        req = self.conn.build_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            headers={
                'x-amz-content-sha256':
                    'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-trailer': 'x-amz-checksum-crc32',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        prev_sig = self.conn.sign_v4(req)['signature']
        self.conn.sign_request(req)
        body_parts = []
        for chunk in [TEST_BODY, b'']:
            chunk_sig = self.conn.sign_chunk(req, prev_sig, _sha256(chunk))
            body_parts.append(b'%x;chunk-signature=%s\r\n%s%s' % (
                len(chunk), chunk_sig.encode('ascii'), chunk,
                b'\r\n' if chunk else b''))
            prev_sig = chunk_sig
        trailers = (
            f'x-amz-checksum-crc32: {_crc32(TEST_BODY)}\r\n'
        ).encode('ascii')
        body_parts.append(trailers)
        trailer_sig = self.conn.sign_trailer(req, prev_sig, trailers[:-1])
        body_parts.append(
            b'x-amz-trailer-signature:%s\r\n' % trailer_sig.encode('ascii'))
        resp = self.conn.send_request(req, b''.join(body_parts))
        self.assertSignatureMismatch(resp, 'AWS4-HMAC-SHA256-TRAILER')

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


class NotV4AuthHeadersMixin:
    def test_strm_unsgnd_pyld_trl_not_encoded(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertSHA256Mismatch(resp, 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                                  _sha256(TEST_BODY))

    def test_strm_unsgnd_pyld_trl_encoding_declared_not_encoded(self):
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=TEST_BODY,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertSHA256Mismatch(resp, 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                                  _sha256(TEST_BODY))

    def test_strm_unsgnd_pyld_trl_no_trailer(self):
        chunked_body = b''.join(
            b'%x\r\n%s' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp, len(chunked_body), len(TEST_BODY))

    def test_strm_unsgnd_pyld_trl_cl_matches_decoded_cl(self):
        chunked_body = b''.join(
            b'%x\r\n%s' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(chunked_body))})
        self.assertSHA256Mismatch(resp, 'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
                                  _sha256(chunked_body))

    def test_strm_sgnd_pyld_trl_no_trailer(self):
        chunked_body = b''.join(
            b'%x\r\n%s' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(TEST_BODY))})
        self.assertIncompleteBody(resp, len(chunked_body), len(TEST_BODY))

    def test_strm_sgnd_pyld_cl_matches_decoded_cl(self):
        chunked_body = b''.join(
            b'%x\r\n%s' % (len(chunk), chunk)
            for chunk in [TEST_BODY, b''])
        resp = self.conn.make_request(
            self.bucket_name,
            'test-obj',
            method='PUT',
            body=chunked_body,
            headers={
                'x-amz-content-sha256': 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                'content-encoding': 'aws-chunked',
                'x-amz-decoded-content-length': str(len(chunked_body))})
        self.assertSHA256Mismatch(resp, 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
                                  _sha256(chunked_body))


class TestV4AuthQuery(InputErrorsMixin,
                      NotV4AuthHeadersMixin,
                      BaseS3TestCaseWithBucket):
    session_cls = S3SessionV4Query


class TestV2AuthHeaders(InputErrorsMixin,
                        NotV4AuthHeadersMixin,
                        BaseS3TestCaseWithBucket):
    session_cls = S3SessionV2Headers


class TestV2AuthQuery(InputErrorsMixin,
                      NotV4AuthHeadersMixin,
                      BaseS3TestCaseWithBucket):
    session_cls = S3SessionV2Query
