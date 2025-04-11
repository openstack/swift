# Copyright (c) 2014 OpenStack Foundation.
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
import binascii
from collections import defaultdict, OrderedDict
import contextlib
from email.header import Header
from hashlib import sha1, sha256
import hmac
import re
# pylint: disable-msg=import-error
from urllib.parse import quote, unquote, parse_qsl
import string

from swift.common.utils import split_path, json, md5, streq_const_time, \
    close_if_possible, InputProxy, get_policy_index, list_from_csv, \
    strict_b64decode, base64_str, checksum
from swift.common.registry import get_swift_info
from swift.common import swob
from swift.common.http import HTTP_OK, HTTP_CREATED, HTTP_ACCEPTED, \
    HTTP_NO_CONTENT, HTTP_UNAUTHORIZED, HTTP_FORBIDDEN, HTTP_NOT_FOUND, \
    HTTP_CONFLICT, HTTP_UNPROCESSABLE_ENTITY, HTTP_REQUEST_ENTITY_TOO_LARGE, \
    HTTP_PARTIAL_CONTENT, HTTP_NOT_MODIFIED, HTTP_PRECONDITION_FAILED, \
    HTTP_REQUESTED_RANGE_NOT_SATISFIABLE, HTTP_LENGTH_REQUIRED, \
    HTTP_BAD_REQUEST, HTTP_REQUEST_TIMEOUT, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_TOO_MANY_REQUESTS, HTTP_RATE_LIMITED, is_success, \
    HTTP_CLIENT_CLOSED_REQUEST

from swift.common.constraints import check_utf8
from swift.proxy.controllers.base import get_container_info
from swift.common.request_helpers import check_path_header

from swift.common.middleware.s3api.controllers import ServiceController, \
    ObjectController, AclController, MultiObjectDeleteController, \
    LocationController, LoggingStatusController, PartController, \
    UploadController, UploadsController, VersioningController, \
    UnsupportedController, S3AclController, BucketController, \
    TaggingController, ObjectLockController
from swift.common.middleware.s3api.s3response import AccessDenied, \
    InvalidArgument, InvalidDigest, BucketAlreadyOwnedByYou, \
    RequestTimeTooSkewed, S3Response, SignatureDoesNotMatch, \
    BucketAlreadyExists, BucketNotEmpty, EntityTooLarge, \
    InternalError, NoSuchBucket, NoSuchKey, PreconditionFailed, InvalidRange, \
    MissingContentLength, InvalidStorageClass, S3NotImplemented, InvalidURI, \
    MalformedXML, InvalidRequest, RequestTimeout, InvalidBucketName, \
    BadDigest, AuthorizationHeaderMalformed, SlowDown, \
    AuthorizationQueryParametersError, ServiceUnavailable, BrokenMPU, \
    XAmzContentSHA256Mismatch, IncompleteBody, InvalidChunkSizeError, \
    InvalidPartNumber, InvalidPartArgument, MalformedTrailerError
from swift.common.middleware.s3api.exception import NotS3Request, \
    S3InputError, S3InputSizeError, S3InputIncomplete, \
    S3InputChunkSignatureMismatch, S3InputChunkTooSmall, \
    S3InputMalformedTrailer, S3InputMissingSecret, \
    S3InputSHA256Mismatch, S3InputChecksumMismatch, \
    S3InputChecksumTrailerInvalid
from swift.common.middleware.s3api.utils import utf8encode, \
    S3Timestamp, mktime, MULTIUPLOAD_SUFFIX
from swift.common.middleware.s3api.subresource import decode_acl, encode_acl
from swift.common.middleware.s3api.utils import sysmeta_header, \
    validate_bucket_name, Config
from swift.common.middleware.s3api.acl_utils import handle_acl_header


# List of sub-resources that must be maintained as part of the HMAC
# signature string.
ALLOWED_SUB_RESOURCES = sorted([
    'acl', 'delete', 'lifecycle', 'location', 'logging', 'notification',
    'partNumber', 'policy', 'requestPayment', 'torrent', 'uploads', 'uploadId',
    'versionId', 'versioning', 'versions', 'website',
    'response-cache-control', 'response-content-disposition',
    'response-content-encoding', 'response-content-language',
    'response-content-type', 'response-expires', 'cors', 'tagging', 'restore',
    'object-lock'
])


MAX_32BIT_INT = 2147483647
SIGV2_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S'
SIGV4_X_AMZ_DATE_FORMAT = '%Y%m%dT%H%M%SZ'
SIGV4_CHUNK_MIN_SIZE = 8192
SERVICE = 's3'  # useful for mocking out in tests


CHECKSUMS_BY_HEADER = {
    'x-amz-checksum-crc32': checksum.crc32,
    'x-amz-checksum-crc32c': checksum.crc32c,
    'x-amz-checksum-crc64nvme': checksum.crc64nvme,
    'x-amz-checksum-sha1': sha1,
    'x-amz-checksum-sha256': sha256,
}


def _get_checksum_hasher(header):
    try:
        return CHECKSUMS_BY_HEADER[header]()
    except (KeyError, NotImplementedError):
        raise S3NotImplemented('The %s algorithm is not supported.' % header)


def _validate_checksum_value(checksum_hasher, b64digest):
    return strict_b64decode(
        b64digest,
        exact_size=checksum_hasher.digest_size,
    )


def _validate_checksum_header_cardinality(num_checksum_headers,
                                          headers_and_trailer=False):
    if num_checksum_headers > 1:
        # inconsistent messaging for AWS compatibility...
        msg = 'Expecting a single x-amz-checksum- header'
        if not headers_and_trailer:
            msg += '. Multiple checksum Types are not allowed.'
        raise InvalidRequest(msg)


def _is_streaming(aws_sha256):
    return aws_sha256 in (
        'STREAMING-UNSIGNED-PAYLOAD-TRAILER',
        'STREAMING-AWS4-HMAC-SHA256-PAYLOAD',
        'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER',
        'STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD',
        'STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER',
    )


def _header_strip(value):
    # S3 seems to strip *all* control characters
    if value is None:
        return None
    stripped = _header_strip.re.sub('', value)
    if value and not stripped:
        # If there's nothing left after stripping,
        # behave as though it wasn't provided
        return None
    return stripped


_header_strip.re = re.compile('^[\x00-\x20]*|[\x00-\x20]*$')


def _header_acl_property(resource):
    """
    Set and retrieve the acl in self.headers
    """

    def getter(self):
        return getattr(self, '_%s' % resource)

    def setter(self, value):
        self.headers.update(encode_acl(resource, value))
        setattr(self, '_%s' % resource, value)

    def deleter(self):
        self.headers[sysmeta_header(resource, 'acl')] = ''

    return property(getter, setter, deleter,
                    doc='Get and set the %s acl property' % resource)


class HashingInput(InputProxy):
    """
    wsgi.input wrapper to verify the SHA256 of the input as it's read.
    """

    def __init__(self, wsgi_input, content_length, expected_hex_hash):
        super().__init__(wsgi_input)
        self._expected_length = content_length
        self._hasher = sha256()
        self._expected_hash = expected_hex_hash
        if content_length == 0 and \
                self._hasher.hexdigest() != self._expected_hash.lower():
            self.close()
            raise XAmzContentSHA256Mismatch(
                client_computed_content_s_h_a256=self._expected_hash,
                s3_computed_content_s_h_a256=self._hasher.hexdigest(),
            )

    def chunk_update(self, chunk, eof, *args, **kwargs):
        # Note that "chunk" is just whatever was read from the input; this
        # says nothing about whether the underlying stream uses aws-chunked
        self._hasher.update(chunk)

        if self.bytes_received < self._expected_length:
            error = eof
        elif self.bytes_received == self._expected_length:
            error = self._hasher.hexdigest() != self._expected_hash.lower()
        else:
            error = True

        if error:
            self.close()
            # Since we don't return the last chunk, the PUT never completes
            raise S3InputSHA256Mismatch(
                self._expected_hash,
                self._hasher.hexdigest())

        return chunk


class ChecksummingInput(InputProxy):
    """
    wsgi.input wrapper to calculate the X-Amz-Checksum-* of the input as it's
    read. The calculated value is checked against an expected value that is
    sent in either the request headers or trailers. To allow for the latter,
    the expected value is lazy fetched once the input has been read.

    :param wsgi_input: file-like object to be wrapped.
    :param content_length: the expected number of bytes to be read.
    :param checksum_hasher: a hasher to calculate the checksum of read bytes.
    :param checksum_key: the name of the header or trailer that will have
        the expected checksum value to be checked.
    :param checksum_source: a dict that will have the ``checksum_key``.
    """

    def __init__(self, wsgi_input, content_length, checksum_hasher,
                 checksum_key, checksum_source):
        super().__init__(wsgi_input)
        self._expected_length = content_length
        self._checksum_hasher = checksum_hasher
        self._checksum_key = checksum_key
        self._checksum_source = checksum_source

    def chunk_update(self, chunk, eof, *args, **kwargs):
        # Note that "chunk" is just whatever was read from the input; this
        # says nothing about whether the underlying stream uses aws-chunked
        self._checksum_hasher.update(chunk)
        if self.bytes_received < self._expected_length:
            error = eof
        elif self.bytes_received == self._expected_length:
            # Lazy fetch checksum value because it may have come in trailers
            b64digest = self._checksum_source.get(self._checksum_key)
            try:
                expected_raw_checksum = _validate_checksum_value(
                    self._checksum_hasher, b64digest)
            except ValueError:
                # If the checksum value came in a header then it would have
                # been validated before the body was read, so if the validation
                # fails here then we can infer that the checksum value came in
                # a trailer. The S3InputChecksumTrailerInvalid raised here will
                # propagate all the way back up the middleware stack to s3api
                # where it is caught and translated to an InvalidRequest.
                raise S3InputChecksumTrailerInvalid(self._checksum_key)
            error = self._checksum_hasher.digest() != expected_raw_checksum
        else:
            error = True

        if error:
            self.close()
            # Since we don't return the last chunk, the PUT never completes
            raise S3InputChecksumMismatch(self._checksum_hasher.name.upper())
        return chunk


class ChunkReader(InputProxy):
    """
    wsgi.input wrapper to read a single chunk from an aws-chunked input and
    validate its signature.

    :param wsgi_input: a wsgi input.
    :param chunk_size: number of bytes to read.
    :param validator: function to call to validate the chunk's content.
    :param chunk_params: string of params from the chunk's header.
    """
    def __init__(self, wsgi_input, chunk_size, validator, chunk_params):
        super().__init__(wsgi_input)
        self.chunk_size = chunk_size
        self._validator = validator
        if self._validator is None:
            self._signature = None
        else:
            self._signature = self._parse_chunk_signature(chunk_params)
        self._sha256 = sha256()

    def _parse_chunk_signature(self, chunk_params):
        if not chunk_params:
            raise S3InputIncomplete
        start, _, chunk_sig = chunk_params.partition('=')
        if start.strip() != 'chunk-signature':
            # Call the validator to update the string to sign
            self._validator('', '')
            raise S3InputChunkSignatureMismatch
        if ';' in chunk_sig:
            raise S3InputIncomplete
        chunk_sig = chunk_sig.strip()
        if not chunk_sig:
            raise S3InputIncomplete
        return chunk_sig

    @property
    def to_read(self):
        return self.chunk_size - self.bytes_received

    def read(self, size=None, *args, **kwargs):
        if size is None or size < 0 or size > self.to_read:
            size = self.to_read
        return super().read(size)

    def readline(self, size=None, *args, **kwargs):
        if size is None or size < 0 or size > self.to_read:
            size = self.to_read
        return super().readline(size)

    def chunk_update(self, chunk, eof, *args, **kwargs):
        # Note that "chunk" is just whatever was read from the input
        self._sha256.update(chunk)
        if self.bytes_received == self.chunk_size:
            if self._validator and not self._validator(
                    self._sha256.hexdigest(), self._signature):
                self.close()
                raise S3InputChunkSignatureMismatch
        return chunk


class StreamingInput:
    """
    wsgi.input wrapper to read a chunked input, verifying each chunk as it's
    read. Once all chunks have been read, any trailers are read.

    :param input: a wsgi input.
    :param decoded_content_length: the number of payload bytes expected to be
        extracted from chunks.
    :param expected_trailers: the set of trailer names expected.
    :param sig_checker: an instance of SigCheckerV4 that will be called to
        verify each chunk's signature.
    """
    def __init__(self, input, decoded_content_length,
                 expected_trailers, sig_checker):
        self._input = input
        self._decoded_content_length = decoded_content_length
        self._expected_trailers = expected_trailers
        self._sig_checker = sig_checker
        # Length of the payload remaining; i.e., number of bytes a caller
        # still expects to be able to read. Once exhausted, we should be
        # exactly at the trailers (if present)
        self._to_read = decoded_content_length
        # Reader for the current chunk that's in progress
        self._chunk_reader = None
        # Track the chunk number, for error messages
        self._chunk_number = 0
        # Track the size of the most recently read chunk. AWS enforces an 8k
        # min chunk size (except the final chunk)
        self._last_chunk_size = None
        # When True, we've read the payload, but not necessarily the trailers
        self._completed_payload = False
        # When True, we've read the trailers
        self._completed_trailers = False
        # Any trailers present after the payload (not available until after
        # caller has read full payload; i.e., until after _to_read is 0)
        self.trailers = {}

    def _read_chunk_header(self):
        """
        Read a chunk header, reading at most one line from the raw input.

        Parse out the next chunk size and any other params.

        :returns: a tuple of (chunk_size, chunk_params). chunk_size is an int,
            chunk_params is string.
        """
        self._chunk_number += 1
        chunk_header = swob.bytes_to_wsgi(self._input.readline())
        if chunk_header[-2:] != '\r\n':
            raise S3InputIncomplete('invalid chunk header: %s' % chunk_header)
        chunk_size, _, chunk_params = chunk_header[:-2].partition(';')

        try:
            chunk_size = int(chunk_size, 16)
            if chunk_size < 0:
                raise ValueError
        except ValueError:
            raise S3InputIncomplete('invalid chunk header: %s' % chunk_header)

        if self._last_chunk_size is not None and \
                self._last_chunk_size < SIGV4_CHUNK_MIN_SIZE and \
                chunk_size != 0:
            raise S3InputChunkTooSmall(self._last_chunk_size,
                                       self._chunk_number)
        self._last_chunk_size = chunk_size

        if chunk_size > self._to_read:
            raise S3InputSizeError(
                self._decoded_content_length,
                self._decoded_content_length - self._to_read + chunk_size)
        return chunk_size, chunk_params

    def _read_payload(self, size, readline=False):
        bufs = []
        bytes_read = 0
        while not self._completed_payload and (
                bytes_read < size
                # Make sure we read the trailing zero-byte chunk at the end
                or self._to_read == 0):
            if self._chunk_reader is None:
                # OK, we're at the start of a new chunk
                chunk_size, chunk_params = self._read_chunk_header()
                self._chunk_reader = ChunkReader(
                    self._input,
                    chunk_size,
                    self._sig_checker and
                    self._sig_checker.check_chunk_signature,
                    chunk_params)
            if readline:
                buf = self._chunk_reader.readline(size - bytes_read)
            else:
                buf = self._chunk_reader.read(size - bytes_read)
            bufs.append(buf)
            if self._chunk_reader.to_read == 0:
                # If it's the final chunk, we're in (possibly empty) trailers
                # Otherwise, there's a CRLF chunk-separator
                if self._chunk_reader.chunk_size == 0:
                    self._completed_payload = True
                elif self._input.read(2) != b'\r\n':
                    raise S3InputIncomplete
                self._chunk_reader = None
            bytes_read += len(buf)
            self._to_read -= len(buf)
            if readline and buf[-1:] == b'\n':
                break
        return b''.join(bufs)

    def _read_trailers(self):
        if self._expected_trailers:
            for line in iter(self._input.readline, b''):
                if not line.endswith(b'\r\n'):
                    raise S3InputIncomplete
                if line == b'\r\n':
                    break
                key, _, value = swob.bytes_to_wsgi(line).partition(':')
                if key.lower() not in self._expected_trailers:
                    raise S3InputMalformedTrailer
                self.trailers[key.strip()] = value.strip()
            if 'x-amz-trailer-signature' in self._expected_trailers \
                    and 'x-amz-trailer-signature' not in self.trailers:
                raise S3InputIncomplete
            if set(self.trailers.keys()) != self._expected_trailers:
                raise S3InputMalformedTrailer
            if 'x-amz-trailer-signature' in self._expected_trailers \
                    and self._sig_checker is not None:
                if not self._sig_checker.check_trailer_signature(
                        self.trailers):
                    raise S3InputChunkSignatureMismatch
                if len(self.trailers) == 1:
                    raise S3InputIncomplete
            # Now that we've read them, we expect no more
            self._expected_trailers = set()
        elif self._input.read(2) not in (b'', b'\r\n'):
            raise S3InputIncomplete

        self._completed_trailers = True

    def _read(self, size, readline=False):
        data = self._read_payload(size, readline)
        if self._completed_payload:
            if not self._completed_trailers:
                # read trailers, if present
                self._read_trailers()
            # At this point, we should have read everything; if we haven't,
            # that's an error
            if self._to_read:
                raise S3InputSizeError(
                    self._decoded_content_length,
                    self._decoded_content_length - self._to_read)
        return data

    def read(self, size=None):
        if size is None or size < 0 or size > self._to_read:
            size = self._to_read
        try:
            return self._read(size)
        except S3InputError:
            self.close()
            raise

    def readline(self, size=None):
        if size is None or size < 0 or size > self._to_read:
            size = self._to_read
        try:
            return self._read(size, True)
        except S3InputError:
            self.close()
            raise

    def close(self):
        close_if_possible(self._input)


class BaseSigChecker:
    def __init__(self, req):
        self.req = req
        self.signature = req.signature
        self.string_to_sign = self._string_to_sign()
        self._secret = None

    def _string_to_sign(self):
        raise NotImplementedError

    def _derive_secret(self, secret):
        return utf8encode(secret)

    def _check_signature(self):
        raise NotImplementedError

    def check_signature(self, secret):
        self._secret = self._derive_secret(secret)
        return self._check_signature()


class SigCheckerV2(BaseSigChecker):
    def _string_to_sign(self):
        """
        Create 'StringToSign' value in Amazon terminology for v2.
        """
        buf = [swob.wsgi_to_bytes(wsgi_str) for wsgi_str in [
            self.req.method,
            _header_strip(self.req.headers.get('Content-MD5')) or '',
            _header_strip(self.req.headers.get('Content-Type')) or '']]

        if 'headers_raw' in self.req.environ:  # eventlet >= 0.19.0
            # See https://github.com/eventlet/eventlet/commit/67ec999
            amz_headers = defaultdict(list)
            for key, value in self.req.environ['headers_raw']:
                key = key.lower()
                if not key.startswith('x-amz-'):
                    continue
                amz_headers[key.strip()].append(value.strip())
            amz_headers = dict((key, ','.join(value))
                               for key, value in amz_headers.items())
        else:  # mostly-functional fallback
            amz_headers = dict((key.lower(), value)
                               for key, value in self.req.headers.items()
                               if key.lower().startswith('x-amz-'))

        if self.req._is_header_auth:
            if 'x-amz-date' in amz_headers:
                buf.append(b'')
            elif 'Date' in self.req.headers:
                buf.append(swob.wsgi_to_bytes(self.req.headers['Date']))
        elif self.req._is_query_auth:
            buf.append(swob.wsgi_to_bytes(self.req.params['Expires']))
        else:
            # Should have already raised NotS3Request in _parse_auth_info,
            # but as a sanity check...
            raise AccessDenied(reason='not_s3')

        for key, value in sorted(amz_headers.items()):
            buf.append(swob.wsgi_to_bytes("%s:%s" % (key, value)))

        path = self.req._canonical_uri()
        if self.req.query_string:
            path += '?' + self.req.query_string
        params = []
        if '?' in path:
            path, args = path.split('?', 1)
            for key, value in sorted(self.req.params.items()):
                if key in ALLOWED_SUB_RESOURCES:
                    params.append('%s=%s' % (key, value) if value else key)
        if params:
            buf.append(swob.wsgi_to_bytes('%s?%s' % (path, '&'.join(params))))
        else:
            buf.append(swob.wsgi_to_bytes(path))
        return b'\n'.join(buf)

    def _check_signature(self):
        valid_signature = base64_str(
            hmac.new(self._secret, self.string_to_sign, sha1).digest())
        return streq_const_time(self.signature, valid_signature)


class SigCheckerV4(BaseSigChecker):
    def __init__(self, req):
        super().__init__(req)
        self._all_chunk_signatures_valid = True

    def _string_to_sign(self):
        return b'\n'.join([
            b'AWS4-HMAC-SHA256',
            self.req.timestamp.amz_date_format.encode('ascii'),
            '/'.join(self.req.scope.values()).encode('utf8'),
            sha256(self.req._canonical_request()).hexdigest().encode('ascii')])

    def _derive_secret(self, secret):
        derived_secret = b'AWS4' + super()._derive_secret(secret)
        for scope_piece in self.req.scope.values():
            derived_secret = hmac.new(
                derived_secret, scope_piece.encode('utf8'), sha256).digest()
        return derived_secret

    def _check_signature(self):
        if self._secret is None:
            raise S3InputMissingSecret
        valid_signature = hmac.new(
            self._secret, self.string_to_sign, sha256).hexdigest()
        return streq_const_time(self.signature, valid_signature)

    def _chunk_string_to_sign(self, data_sha256):
        """
        Create 'ChunkStringToSign' value in Amazon terminology for v4.
        """
        return b'\n'.join([
            b'AWS4-HMAC-SHA256-PAYLOAD',
            self.req.timestamp.amz_date_format.encode('ascii'),
            '/'.join(self.req.scope.values()).encode('utf8'),
            self.signature.encode('utf8'),
            sha256(b'').hexdigest().encode('utf8'),
            data_sha256.encode('utf8')
        ])

    def check_chunk_signature(self, chunk_sha256, signature):
        """
        Check the validity of a chunk's signature.

        This method verifies the signature of a given chunk using its SHA-256
        hash. It updates the string to sign and the current signature, then
        checks if the signature is valid. If any chunk signature is invalid,
        it returns False.

        :param chunk_sha256: (str) The SHA-256 hash of the chunk.
        :param signature: (str) The signature to be verified.
        :returns: True if all chunk signatures are valid, False otherwise.
        """
        if not self._all_chunk_signatures_valid:
            return False
        # NB: string_to_sign is calculated using the previous signature
        self.string_to_sign = self._chunk_string_to_sign(chunk_sha256)
        # So we have to update the signature to compare against *after*
        # the string-to-sign
        self.signature = signature
        self._all_chunk_signatures_valid &= self._check_signature()
        return self._all_chunk_signatures_valid

    def _trailer_string_to_sign(self, trailers):
        """
        Create 'TrailerChunkStringToSign' value in Amazon terminology for v4.
        """
        canonical_trailers = swob.wsgi_to_bytes(''.join(
            f'{key}:{value}\n'
            for key, value in sorted(
                trailers.items(),
                key=lambda kvp: swob.wsgi_to_bytes(kvp[0]).lower(),
            )
            if key != 'x-amz-trailer-signature'
        ))
        if not canonical_trailers:
            canonical_trailers = b'\n'
        return b'\n'.join([
            b'AWS4-HMAC-SHA256-TRAILER',
            self.req.timestamp.amz_date_format.encode('ascii'),
            '/'.join(self.req.scope.values()).encode('utf8'),
            self.signature.encode('utf8'),
            sha256(canonical_trailers).hexdigest().encode('utf8'),
        ])

    def check_trailer_signature(self, trailers):
        """
        Check the validity of a chunk's signature.

        This method verifies the trailers received after the main payload.

        :param trailers: (dict[str, str]) The trailers received.
        :returns: True if x-amz-trailer-signature is valid, False otherwise.
        """
        if not self._all_chunk_signatures_valid:
            # if there was a breakdown earlier, this can't be right
            return False
        # NB: string_to_sign is calculated using the previous signature
        self.string_to_sign = self._trailer_string_to_sign(trailers)
        # So we have to update the signature to compare against *after*
        # the string-to-sign
        self.signature = trailers['x-amz-trailer-signature']
        self._all_chunk_signatures_valid &= self._check_signature()
        return self._all_chunk_signatures_valid


def _parse_credential(credential_string):
    """
    Parse an AWS credential string into its components.

    This method splits the given credential string into its constituent parts:
    access key ID, date, AWS region, AWS service, and terminal identifier.
    The credential string must follow the format:
    <access-key-id>/<date>/<AWS-region>/<AWS-service>/aws4_request.

    :param credential_string: (str) The AWS credential string to be parsed.
    :raises AccessDenied: If the credential string is invalid or does not
        follow the required format.
    :returns: A dict containing the parsed components of the credential string.
    """
    parts = credential_string.split("/")
    # credential must be in following format:
    # <access-key-id>/<date>/<AWS-region>/<AWS-service>/aws4_request
    if not parts[0] or len(parts) != 5:
        raise AccessDenied(reason='invalid_credential')
    return dict(zip(['access', 'date', 'region', 'service', 'terminal'],
                    parts))


class SigV4Mixin(object):
    """
    A request class mixin to provide S3 signature v4 functionality
    """

    @property
    def _is_query_auth(self):
        return 'X-Amz-Credential' in self.params

    @property
    def _is_x_amz_content_sha256_required(self):
        return not self._is_query_auth

    @property
    def timestamp(self):
        """
        Return timestamp string according to the auth type
        The difference from v2 is v4 have to see 'X-Amz-Date' even though
        it's query auth type.
        """
        if not self._timestamp:
            try:
                if self._is_query_auth and 'X-Amz-Date' in self.params:
                    # NOTE(andrey-mp): Date in Signature V4 has different
                    # format
                    timestamp = mktime(
                        self.params['X-Amz-Date'], SIGV4_X_AMZ_DATE_FORMAT)
                else:
                    if self.headers.get('X-Amz-Date'):
                        timestamp = mktime(
                            self.headers.get('X-Amz-Date'),
                            SIGV4_X_AMZ_DATE_FORMAT)
                    else:
                        timestamp = mktime(self.headers.get('Date'))
            except (ValueError, TypeError):
                raise AccessDenied('AWS authentication requires a valid Date '
                                   'or x-amz-date header',
                                   reason='invalid_date')

            if timestamp < 0:
                raise AccessDenied('AWS authentication requires a valid Date '
                                   'or x-amz-date header',
                                   reason='invalid_date')

            try:
                self._timestamp = S3Timestamp(timestamp)
            except ValueError:
                # Must be far-future; blame clock skew
                raise RequestTimeTooSkewed()

        return self._timestamp

    def _validate_expire_param(self):
        """
        Validate X-Amz-Expires in query parameter
        :raises: AccessDenied
        :raises: AuthorizationQueryParametersError
        :raises: AccessDenined
        """
        err = None
        try:
            expires = int(self.params['X-Amz-Expires'])
        except KeyError:
            raise AccessDenied(reason='invalid_expires')
        except ValueError:
            err = 'X-Amz-Expires should be a number'
        else:
            if expires < 0:
                err = 'X-Amz-Expires must be non-negative'
            elif expires >= 2 ** 63:
                err = 'X-Amz-Expires should be a number'
            elif expires > 604800:
                err = ('X-Amz-Expires must be less than a week (in seconds); '
                       'that is, the given X-Amz-Expires must be less than '
                       '604800 seconds')
        if err:
            raise AuthorizationQueryParametersError(err)

        if int(self.timestamp) + expires < S3Timestamp.now():
            raise AccessDenied('Request has expired', reason='expired')

    def _parse_query_authentication(self):
        """
        Parse v4 query authentication
        - version 4:
            'X-Amz-Credential' and 'X-Amz-Signature' should be in param
        :raises: AccessDenied
        :raises: AuthorizationHeaderMalformed
        """
        if self.params.get('X-Amz-Algorithm') != 'AWS4-HMAC-SHA256':
            raise InvalidArgument('X-Amz-Algorithm',
                                  self.params.get('X-Amz-Algorithm'))
        try:
            cred_param = _parse_credential(
                swob.wsgi_to_str(self.params['X-Amz-Credential']))
            sig = swob.wsgi_to_str(self.params['X-Amz-Signature'])
            if not sig:
                raise AccessDenied(reason='invalid_query_auth')
        except KeyError:
            raise AccessDenied(reason='invalid_query_auth')

        try:
            signed_headers = swob.wsgi_to_str(
                self.params['X-Amz-SignedHeaders'])
        except KeyError:
            # TODO: make sure if is it malformed request?
            raise AuthorizationHeaderMalformed()

        self._signed_headers = set(signed_headers.split(';'))

        invalid_messages = {
            'date': 'Invalid credential date "%s". This date is not the same '
                    'as X-Amz-Date: "%s".',
            'region': "Error parsing the X-Amz-Credential parameter; "
                    "the region '%s' is wrong; expecting '%s'",
            'service': 'Error parsing the X-Amz-Credential parameter; '
                    'incorrect service "%s". This endpoint belongs to "%s".',
            'terminal': 'Error parsing the X-Amz-Credential parameter; '
                    'incorrect terminal "%s". This endpoint uses "%s".',
        }
        for key in ('date', 'region', 'service', 'terminal'):
            if cred_param[key] != self.scope[key]:
                kwargs = {}
                if key == 'region':
                    # Allow lowercase region name
                    # for AWS .NET SDK compatibility
                    if not self.scope[key].islower() and \
                            cred_param[key] == self.scope[key].lower():
                        self.location = self.location.lower()
                        continue
                    kwargs = {'region': self.scope['region']}
                raise AuthorizationQueryParametersError(
                    invalid_messages[key] % (cred_param[key], self.scope[key]),
                    **kwargs)

        return cred_param['access'], sig

    def _parse_header_authentication(self):
        """
        Parse v4 header authentication
        - version 4:
            'X-Amz-Credential' and 'X-Amz-Signature' should be in param
        :raises: AccessDenied
        :raises: AuthorizationHeaderMalformed
        """

        auth_str = swob.wsgi_to_str(self.headers['Authorization'])
        cred_param = _parse_credential(auth_str.partition(
            "Credential=")[2].split(',')[0])
        sig = auth_str.partition("Signature=")[2].split(',')[0]
        if not sig:
            raise AccessDenied(reason='invalid_header_auth')
        signed_headers = auth_str.partition(
            "SignedHeaders=")[2].split(',', 1)[0]
        if not signed_headers:
            # TODO: make sure if is it Malformed?
            raise AuthorizationHeaderMalformed()

        invalid_messages = {
            'date': 'Invalid credential date "%s". This date is not the same '
                    'as X-Amz-Date: "%s".',
            'region': "The authorization header is malformed; the region '%s' "
                    "is wrong; expecting '%s'",
            'service': 'The authorization header is malformed; incorrect '
                    'service "%s". This endpoint belongs to "%s".',
            'terminal': 'The authorization header is malformed; incorrect '
                    'terminal "%s". This endpoint uses "%s".',
        }
        for key in ('date', 'region', 'service', 'terminal'):
            if cred_param[key] != self.scope[key]:
                kwargs = {}
                if key == 'region':
                    # Allow lowercase region name
                    # for AWS .NET SDK compatibility
                    if not self.scope[key].islower() and \
                            cred_param[key] == self.scope[key].lower():
                        self.location = self.location.lower()
                        continue
                    kwargs = {'region': self.scope['region']}
                raise AuthorizationHeaderMalformed(
                    invalid_messages[key] % (cred_param[key], self.scope[key]),
                    **kwargs)

        self._signed_headers = set(signed_headers.split(';'))

        return cred_param['access'], sig

    def _canonical_query_string(self):
        return '&'.join(
            '%s=%s' % (swob.wsgi_quote(key, safe='-_.~'),
                       swob.wsgi_quote(value, safe='-_.~'))
            for key, value in sorted(self.params.items())
            if key not in ('Signature', 'X-Amz-Signature')).encode('ascii')

    def _headers_to_sign(self):
        """
        Select the headers from the request that need to be included
        in the StringToSign.

        :return : dict of headers to sign, the keys are all lower case
        """
        if 'headers_raw' in self.environ:  # eventlet >= 0.19.0
            # See https://github.com/eventlet/eventlet/commit/67ec999
            headers_lower_dict = defaultdict(list)
            for key, value in self.environ['headers_raw']:
                headers_lower_dict[key.lower().strip()].append(
                    ' '.join(_header_strip(value or '').split()))
            headers_lower_dict = {k: ','.join(v)
                                  for k, v in headers_lower_dict.items()}
        else:  # mostly-functional fallback
            headers_lower_dict = dict(
                (k.lower().strip(), ' '.join(_header_strip(v or '').split()))
                for (k, v) in self.headers.items())

        if 'host' in headers_lower_dict and re.match(
                'Boto/2.[0-9].[0-2]',
                headers_lower_dict.get('user-agent', '')):
            # Boto versions < 2.9.3 strip the port component of the host:port
            # header, so detect the user-agent via the header and strip the
            # port if we detect an old boto version.
            headers_lower_dict['host'] = \
                headers_lower_dict['host'].split(':')[0]

        headers_to_sign = [
            (key, value) for key, value in sorted(headers_lower_dict.items())
            if swob.wsgi_to_str(key) in self._signed_headers]

        if len(headers_to_sign) != len(self._signed_headers):
            # NOTE: if we are missing the header suggested via
            # signed_header in actual header, it results in
            # SignatureDoesNotMatch in actual S3 so we can raise
            # the error immediately here to save redundant check
            # process.
            raise SignatureDoesNotMatch()

        return headers_to_sign

    def _canonical_uri(self):
        """
        It won't require bucket name in canonical_uri for v4.
        """
        return swob.wsgi_to_bytes(swob.wsgi_quote(
            self.environ.get('PATH_INFO', self.path), safe='-_.~/'))

    def _canonical_request(self):
        # prepare 'canonical_request'
        # Example requests are like following:
        #
        # GET
        # /
        # Action=ListUsers&Version=2010-05-08
        # content-type:application/x-www-form-urlencoded; charset=utf-8
        # host:iam.amazonaws.com
        # x-amz-date:20150830T123600Z
        #
        # content-type;host;x-amz-date
        # e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        #

        # 1. Add verb like: GET
        cr = [swob.wsgi_to_bytes(self.method)]

        # 2. Add path like: /
        path = self._canonical_uri()
        cr.append(path)

        # 3. Add query like: Action=ListUsers&Version=2010-05-08
        cr.append(self._canonical_query_string())

        # 4. Add headers like:
        # content-type:application/x-www-form-urlencoded; charset=utf-8
        # host:iam.amazonaws.com
        # x-amz-date:20150830T123600Z
        headers_to_sign = self._headers_to_sign()
        cr.append(b''.join(swob.wsgi_to_bytes('%s:%s\n' % (key, value))
                           for key, value in headers_to_sign))

        # 5. Add signed headers into canonical request like
        # content-type;host;x-amz-date
        cr.append(b';'.join(swob.wsgi_to_bytes(k) for k, v in headers_to_sign))

        # 6. Add payload string at the tail
        hashed_payload = self.headers.get('X-Amz-Content-SHA256',
                                          'UNSIGNED-PAYLOAD')

        cr.append(swob.wsgi_to_bytes(hashed_payload))
        return b'\n'.join(cr)

    @property
    def scope(self):
        return OrderedDict([
            ('date', self.timestamp.amz_date_format.split('T')[0]),
            ('region', self.location),
            ('service', SERVICE),
            ('terminal', 'aws4_request'),
        ])

    def signature_does_not_match_kwargs(self):
        kwargs = super(SigV4Mixin, self).signature_does_not_match_kwargs()
        cr = self._canonical_request()
        kwargs.update({
            'canonical_request': cr,
            'canonical_request_bytes': ' '.join(
                format(ord(c), '02x') for c in cr.decode('latin1')),
        })
        return kwargs


def get_request_class(env, s3_acl):
    """
    Helper function to find a request class to use from Map
    """
    if s3_acl:
        request_classes = (S3AclRequest, SigV4S3AclRequest)
    else:
        request_classes = (S3Request, SigV4Request)

    req = swob.Request(env)
    if 'X-Amz-Credential' in req.params or \
            req.headers.get('Authorization', '').startswith(
                'AWS4-HMAC-SHA256 '):
        # This is an Amazon SigV4 request
        return request_classes[1]
    else:
        # The others using Amazon SigV2 class
        return request_classes[0]


class S3Request(swob.Request):
    """
    S3 request object.
    """

    bucket_acl = _header_acl_property('container')
    object_acl = _header_acl_property('object')

    def __init__(self, env, app=None, conf=None):
        # NOTE: app is not used by this class, need for compatibility of S3acl
        swob.Request.__init__(self, env)
        self.conf = conf or Config()
        self.location = self.conf.location
        self._timestamp = None
        self.access_key, self.signature = self._parse_auth_info()
        self.bucket_in_host = self._parse_host()
        self.container_name, self.object_name = self._parse_uri()
        self._validate_headers()
        if isinstance(self, SigV4Mixin):
            # this is a deliberate but only partial shift away from the
            # 'inherit and override from mixin' pattern towards a 'compose
            # adapters' pattern.
            self.sig_checker = SigCheckerV4(self)
        else:
            self.sig_checker = SigCheckerV2(self)
        aws_sha256 = self.headers.get('x-amz-content-sha256')
        if self.method in ('PUT', 'POST'):
            checksum_hasher, checksum_header, checksum_trailer = \
                self._validate_checksum_headers()
            if _is_streaming(aws_sha256):
                if checksum_trailer:
                    streaming_input = self._install_streaming_input_wrapper(
                        aws_sha256, checksum_trailer=checksum_trailer)
                    checksum_key = checksum_trailer
                    checksum_source = streaming_input.trailers
                else:
                    self._install_streaming_input_wrapper(aws_sha256)
                    checksum_key = checksum_header
                    checksum_source = self.headers
            elif checksum_trailer:
                raise MalformedTrailerError
            else:
                self._install_non_streaming_input_wrapper(aws_sha256)
                checksum_key = checksum_header
                checksum_source = self.headers

            # S3 doesn't check the checksum against the request body for at
            # least some POSTs (e.g. MPU complete) so restrict this to PUTs
            if checksum_key and self.method == 'PUT':
                self._install_checksumming_input_wrapper(
                    checksum_hasher, checksum_key, checksum_source)

        # Lock in string-to-sign now, before we start messing with query params
        self.environ['s3api.auth_details'] = {
            'access_key': self.access_key,
            'signature': self.signature,
            'string_to_sign': self.sig_checker.string_to_sign,
            'check_signature': self.sig_checker.check_signature,
        }
        self.account = None
        self.user_id = None
        self.policy_index = None

        # Avoids that swift.swob.Response replaces Location header value
        # by full URL when absolute path given. See swift.swob for more detail.
        self.environ['swift.leave_relative_location'] = True

    def validate_part_number(self, parts_count=None, check_max=True):
        """
        Get the partNumber param, if it exists, and check it is valid.

        To be valid, a partNumber must satisfy two criteria. First, it must be
        an integer between 1 and the maximum allowed parts, inclusive. The
        maximum allowed parts is the maximum of the configured
        ``max_upload_part_num`` and, if given, ``parts_count``. Second, the
        partNumber must be less than or equal to the ``parts_count``, if it is
        given.

        :param parts_count: if given, this is the number of parts in an
            existing object.
        :raises InvalidPartArgument: if the partNumber param is invalid i.e.
            less than 1 or greater than the maximum allowed parts.
        :raises InvalidPartNumber: if the partNumber param is valid but greater
            than ``num_parts``.
        :return: an integer part number if the partNumber param exists,
            otherwise ``None``.
        """
        part_number = self.params.get('partNumber')
        if part_number is None:
            return None

        if self.range:
            raise InvalidRequest('Cannot specify both Range header and '
                                 'partNumber query parameter')

        try:
            parts_count = int(parts_count)
        except (TypeError, ValueError):
            # an invalid/empty param is treated like parts_count=max_parts
            parts_count = self.conf.max_upload_part_num
        # max_parts may be raised to the number of existing parts
        max_parts = max(self.conf.max_upload_part_num, parts_count)

        try:
            part_number = int(part_number)
            if part_number < 1:
                raise ValueError
        except ValueError:
            raise InvalidPartArgument(max_parts, part_number)  # 400

        if check_max:
            if part_number > max_parts:
                raise InvalidPartArgument(max_parts, part_number)  # 400
            if part_number > parts_count:
                raise InvalidPartNumber()  # 416

        return part_number

    @property
    def timestamp(self):
        """
        S3Timestamp from Date header. If X-Amz-Date header specified, it
        will be prior to Date header.

        :return : S3Timestamp instance
        """
        if not self._timestamp:
            try:
                if self._is_query_auth and 'Timestamp' in self.params:
                    # If Timestamp specified in query, it should be prior
                    # to any Date header (is this right?)
                    timestamp = mktime(
                        self.params['Timestamp'], SIGV2_TIMESTAMP_FORMAT)
                else:
                    timestamp = mktime(
                        self.headers.get('X-Amz-Date',
                                         self.headers.get('Date')))
            except ValueError:
                raise AccessDenied('AWS authentication requires a valid Date '
                                   'or x-amz-date header',
                                   reason='invalid_date')

            if timestamp < 0:
                raise AccessDenied('AWS authentication requires a valid Date '
                                   'or x-amz-date header',
                                   reason='invalid_date')
            try:
                self._timestamp = S3Timestamp(timestamp)
            except ValueError:
                # Must be far-future; blame clock skew
                raise RequestTimeTooSkewed()

        return self._timestamp

    @property
    def _is_header_auth(self):
        return 'Authorization' in self.headers

    @property
    def _is_query_auth(self):
        return 'AWSAccessKeyId' in self.params

    @property
    def _is_x_amz_content_sha256_required(self):
        return False

    def _parse_host(self):
        if not self.conf.storage_domains:
            return None

        if 'HTTP_HOST' in self.environ:
            given_domain = self.environ['HTTP_HOST']
        elif 'SERVER_NAME' in self.environ:
            given_domain = self.environ['SERVER_NAME']
        else:
            return None
        port = ''
        if ':' in given_domain:
            given_domain, port = given_domain.rsplit(':', 1)

        for storage_domain in self.conf.storage_domains:
            if not storage_domain.startswith('.'):
                storage_domain = '.' + storage_domain

            if given_domain.endswith(storage_domain):
                return given_domain[:-len(storage_domain)]

        return None

    def _parse_uri(self):
        # NB: returns WSGI strings
        if not check_utf8(swob.wsgi_to_str(self.environ['PATH_INFO'])):
            raise InvalidURI(self.path)

        if self.bucket_in_host:
            obj = self.environ['PATH_INFO'][1:] or None
            return self.bucket_in_host, obj

        bucket, obj = self.split_path(0, 2, True)

        if bucket and not validate_bucket_name(
                bucket, self.conf.dns_compliant_bucket_names):
            # Ignore GET service case
            raise InvalidBucketName(bucket)
        return bucket, obj

    def _parse_query_authentication(self):
        """
        Parse v2 authentication query args
        TODO: make sure if 0, 1, 3 is supported?
        - version 0, 1, 2, 3:
            'AWSAccessKeyId' and 'Signature' should be in param

        :return: a tuple of access_key and signature
        :raises: AccessDenied
        """
        try:
            access = swob.wsgi_to_str(self.params['AWSAccessKeyId'])
            expires = swob.wsgi_to_str(self.params['Expires'])
            sig = swob.wsgi_to_str(self.params['Signature'])
        except KeyError:
            raise AccessDenied(reason='invalid_query_auth')

        if not all([access, sig, expires]):
            raise AccessDenied(reason='invalid_query_auth')

        return access, sig

    def _parse_header_authentication(self):
        """
        Parse v2 header authentication info

        :returns: a tuple of access_key and signature
        :raises: AccessDenied
        """
        auth_str = swob.wsgi_to_str(self.headers['Authorization'])
        if not auth_str.startswith('AWS ') or ':' not in auth_str:
            raise AccessDenied(reason='invalid_header_auth')
        # This means signature format V2
        access, sig = auth_str.split(' ', 1)[1].rsplit(':', 1)
        return access, sig

    def _parse_auth_info(self):
        """Extract the access key identifier and signature.

        :returns: a tuple of access_key and signature
        :raises: NotS3Request
        """
        if self._is_query_auth:
            self._validate_expire_param()
            return self._parse_query_authentication()
        elif self._is_header_auth:
            self._validate_dates()
            return self._parse_header_authentication()
        else:
            # if this request is neither query auth nor header auth
            # s3api regard this as not s3 request
            raise NotS3Request()

    def _validate_expire_param(self):
        """
        Validate Expires in query parameters
        :raises: AccessDenied
        """
        # Expires header is a float since epoch
        try:
            ex = S3Timestamp(float(self.params['Expires']))
        except (KeyError, ValueError):
            raise AccessDenied(reason='invalid_expires')

        if S3Timestamp.now() > ex:
            raise AccessDenied('Request has expired', reason='expired')

        if ex >= 2 ** 31:
            raise AccessDenied(
                'Invalid date (should be seconds since epoch): %s' %
                self.params['Expires'], reason='invalid_expires')

    def _validate_dates(self):
        """
        Validate Date/X-Amz-Date headers for signature v2
        :raises: AccessDenied
        :raises: RequestTimeTooSkewed
        """
        date_header = self.headers.get('Date')
        amz_date_header = self.headers.get('X-Amz-Date')
        if not date_header and not amz_date_header:
            raise AccessDenied('AWS authentication requires a valid Date '
                               'or x-amz-date header',
                               reason='invalid_date')

        # Anyways, request timestamp should be validated
        epoch = S3Timestamp(0)
        if self.timestamp < epoch:
            raise AccessDenied(reason='invalid_date')

        # If the standard date is too far ahead or behind, it is an
        # error
        delta = abs(int(self.timestamp) - int(S3Timestamp.now()))
        if delta > self.conf.allowable_clock_skew:
            raise RequestTimeTooSkewed()

    def _validate_sha256(self):
        aws_sha256 = self.headers.get('x-amz-content-sha256')
        if not aws_sha256:
            if self._is_x_amz_content_sha256_required:
                msg = 'Missing required header for this request: ' \
                      'x-amz-content-sha256'
                raise InvalidRequest(msg)
            else:
                return

        looks_like_sha256 = (
            aws_sha256 and len(aws_sha256) == 64 and
            all(c in '0123456789abcdef' for c in aws_sha256.lower()))
        if aws_sha256 == 'UNSIGNED-PAYLOAD':
            pass
        elif _is_streaming(aws_sha256):
            decoded_content_length = self.headers.get(
                'x-amz-decoded-content-length')
            try:
                decoded_content_length = int(decoded_content_length)
            except (ValueError, TypeError):
                raise MissingContentLength
            if decoded_content_length < 0:
                raise InvalidArgument('x-amz-decoded-content-length',
                                      decoded_content_length)

            if not isinstance(self, SigV4Mixin) or self._is_query_auth:
                if decoded_content_length < (self.content_length or 0):
                    raise IncompleteBody(
                        number_bytes_expected=decoded_content_length,
                        number_bytes_provided=self.content_length,
                    )
                body = self.body_file.read()
                raise XAmzContentSHA256Mismatch(
                    client_computed_content_s_h_a256=aws_sha256,
                    s3_computed_content_s_h_a256=sha256(body).hexdigest(),
                )
            elif aws_sha256 in (
                'STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD',
                'STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER',
            ):
                raise S3NotImplemented(
                    "Don't know how to validate %s streams"
                    % aws_sha256)

        elif not looks_like_sha256 and self._is_x_amz_content_sha256_required:
            raise InvalidArgument(
                'x-amz-content-sha256',
                aws_sha256,
                'x-amz-content-sha256 must be UNSIGNED-PAYLOAD, '
                'STREAMING-UNSIGNED-PAYLOAD-TRAILER, '
                'STREAMING-AWS4-HMAC-SHA256-PAYLOAD, '
                'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER or '
                'a valid sha256 value.')

        return aws_sha256

    def _cleanup_content_encoding(self):
        if 'aws-chunked' in self.headers.get('Content-Encoding', ''):
            new_enc = ', '.join(
                enc for enc in list_from_csv(
                    self.headers.pop('Content-Encoding'))
                # TODO: test what's stored w/ 'aws-chunked, aws-chunked'
                if enc != 'aws-chunked')
            if new_enc:
                # used to be, AWS would store '', but not any more
                self.headers['Content-Encoding'] = new_enc

    def _install_streaming_input_wrapper(self, aws_sha256,
                                         checksum_trailer=None):
        """
        Wrap the wsgi input with a reader that parses an aws-chunked body.

        :param aws_sha256: the value of the 'x-amz-content-sha256' header.
        :param checksum_trailer: the name of an 'x-amz-checksum-*' trailer
            (if any) that is to be expected at the end of the body.
        :return: an instance of StreamingInput.
        """
        self._cleanup_content_encoding()
        self.content_length = int(self.headers.get(
            'x-amz-decoded-content-length'))
        expected_trailers = set()
        if aws_sha256 == 'STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER':
            expected_trailers.add('x-amz-trailer-signature')
        if checksum_trailer:
            expected_trailers.add(checksum_trailer)
        streaming_input = StreamingInput(
            self.environ['wsgi.input'],
            self.content_length,
            expected_trailers,
            None if aws_sha256 == 'STREAMING-UNSIGNED-PAYLOAD-TRAILER'
            else self.sig_checker)
        self.environ['wsgi.input'] = streaming_input
        return streaming_input

    def _install_non_streaming_input_wrapper(self, aws_sha256):
        if (aws_sha256 not in (None, 'UNSIGNED-PAYLOAD') and
                self.content_length is not None):
            self.environ['wsgi.input'] = HashingInput(
                self.environ['wsgi.input'],
                self.content_length,
                aws_sha256)
        # If no content-length, either client's trying to do a HTTP chunked
        # transfer, or a HTTP/1.0-style transfer (in which case swift will
        # reject with length-required and we'll translate back to
        # MissingContentLength)

    def _validate_x_amz_checksum_headers(self):
        """
        Validate and return a header that specifies a checksum value. A valid
        header must be named x-amz-checksum-<algorithm> where <algorithm> is
        one of the supported checksum algorithms.

        :raises: InvalidRequest if more than one checksum header is found or if
            an invalid algorithm is specified.
        :return: a dict containing at most a single checksum header name:value
            pair.
        """
        checksum_headers = {
            h.lower(): v
            for h, v in self.headers.items()
            if (h.lower().startswith('x-amz-checksum-')
                and h.lower() not in ('x-amz-checksum-algorithm',
                                      'x-amz-checksum-type'))
        }
        if any(h not in CHECKSUMS_BY_HEADER
               for h in checksum_headers):
            raise InvalidRequest('The algorithm type you specified in '
                                 'x-amz-checksum- header is invalid.')
        _validate_checksum_header_cardinality(len(checksum_headers))
        return checksum_headers

    def _validate_x_amz_trailer_header(self):
        """
        Validate and return the name of a checksum trailer that is declared by
        an ``x-amz-trailer`` header. A valid trailer must be named
        x-amz-checksum-<algorithm> where <algorithm> is one of the supported
        checksum algorithms.

        :raises: InvalidRequest if more than one checksum trailer is declared
            by the ``x-amz-trailer`` header, or if an invalid algorithm is
            specified.
        :return: a list containing at most a single checksum header name.
        """
        header = self.headers.get('x-amz-trailer', '').strip()
        checksum_headers = [
            v.strip() for v in header.rstrip(',').split(',')
        ] if header else []
        if any(h not in CHECKSUMS_BY_HEADER
               for h in checksum_headers):
            raise InvalidRequest('The value specified in the x-amz-trailer '
                                 'header is not supported')
        _validate_checksum_header_cardinality(len(checksum_headers))
        return checksum_headers

    def _validate_checksum_headers(self):
        """
        A checksum for the request is specified by a checksum header of the
        form:

          x-amz-checksum-<algorithm>: <checksum>

        where <algorithm> is one of the supported checksum algorithms and
        <checksum> is the value to be checked. A checksum header may be sent in
        either the headers or the trailers. An ``x-amz-trailer`` header is used
        to declare that a checksum header is to be expected in the trailers.

        At most one checksum header is allowed in the headers or trailers. If
        this condition is met, this method returns the name of the checksum
        header or trailer and a hasher for the checksum algorithm that it
        declares.

        :raises InvalidRequest: if any of the following conditions occur: more
            than one checksum header is declared; the checksum header specifies
            an invalid algorithm; the algorithm does not match the value of any
            ``x-amz-sdk-checksum-algorithm`` header that is also present; the
            checksum value is invalid.
        :raises S3NotImplemented: if the declared algorithm is valid but not
            supported.
        :return: a tuple of
            (hasher, checksum header name, checksum trailer name) where at
            least one of (checksum header name, checksum trailer name) will be
            None.
        """
        checksum_headers = self._validate_x_amz_checksum_headers()
        checksum_trailer_headers = self._validate_x_amz_trailer_header()
        _validate_checksum_header_cardinality(
            len(checksum_headers) + len(checksum_trailer_headers),
            headers_and_trailer=True
        )

        if checksum_headers:
            checksum_trailer = None
            checksum_header, b64digest = list(checksum_headers.items())[0]
            checksum_hasher = _get_checksum_hasher(checksum_header)
            try:
                # early check on the value...
                _validate_checksum_value(checksum_hasher, b64digest)
            except ValueError:
                raise InvalidRequest(
                    'Value for %s header is invalid.' % checksum_header)
        elif checksum_trailer_headers:
            checksum_header = None
            checksum_trailer = checksum_trailer_headers[0]
            checksum_hasher = _get_checksum_hasher(checksum_trailer)
            # checksum should appear at end of request in trailers
        else:
            checksum_hasher = checksum_header = checksum_trailer = None

        checksum_algo = self.headers.get('x-amz-sdk-checksum-algorithm')
        if checksum_algo:
            if not checksum_hasher:
                raise InvalidRequest(
                    'x-amz-sdk-checksum-algorithm specified, but no '
                    'corresponding x-amz-checksum-* or x-amz-trailer '
                    'headers were found.')
            if checksum_algo.lower() != checksum_hasher.name:
                raise InvalidRequest('Value for x-amz-sdk-checksum-algorithm '
                                     'header is invalid.')

        return checksum_hasher, checksum_header, checksum_trailer

    def _install_checksumming_input_wrapper(
            self, checksum_hasher, checksum_key, checksum_source):
        self.environ['wsgi.input'] = ChecksummingInput(
            self.environ['wsgi.input'],
            self.content_length,
            checksum_hasher,
            checksum_key,
            checksum_source
        )

    def _validate_headers(self):
        if 'CONTENT_LENGTH' in self.environ:
            try:
                if self.content_length < 0:
                    raise InvalidArgument('Content-Length',
                                          self.content_length)
            except (ValueError, TypeError):
                raise InvalidArgument('Content-Length',
                                      self.environ['CONTENT_LENGTH'])

        if self.method == 'PUT' and any(h in self.headers for h in (
                'If-Match', 'If-None-Match',
                'If-Modified-Since', 'If-Unmodified-Since')):
            raise S3NotImplemented(
                'Conditional object PUTs are not supported.')

        if 'X-Amz-Copy-Source' in self.headers:
            try:
                check_path_header(self, 'X-Amz-Copy-Source', 2, '')
            except swob.HTTPException:
                msg = 'Copy Source must mention the source bucket and key: ' \
                      'sourcebucket/sourcekey'
                raise InvalidArgument('x-amz-copy-source',
                                      self.headers['X-Amz-Copy-Source'],
                                      msg)
        if 'x-amz-metadata-directive' in self.headers:
            value = self.headers['x-amz-metadata-directive']
            if value not in ('COPY', 'REPLACE'):
                err_msg = 'Unknown metadata directive.'
                raise InvalidArgument('x-amz-metadata-directive', value,
                                      err_msg)

        if 'x-amz-storage-class' in self.headers:
            # Only STANDARD is supported now.
            if self.headers['x-amz-storage-class'] != 'STANDARD':
                raise InvalidStorageClass()

        if 'x-amz-mfa' in self.headers:
            raise S3NotImplemented('MFA Delete is not supported.')

        sse_value = self.headers.get('x-amz-server-side-encryption')
        if sse_value is not None:
            if sse_value not in ('aws:kms', 'AES256'):
                raise InvalidArgument(
                    'x-amz-server-side-encryption', sse_value,
                    'The encryption method specified is not supported')
            encryption_enabled = get_swift_info(admin=True)['admin'].get(
                'encryption', {}).get('enabled')
            if not encryption_enabled or sse_value != 'AES256':
                raise S3NotImplemented(
                    'Server-side encryption is not supported.')

        if 'x-amz-website-redirect-location' in self.headers:
            raise S3NotImplemented('Website redirection is not supported.')

        self._validate_sha256()

        value = _header_strip(self.headers.get('Content-MD5'))
        if value is not None:
            if not re.match('^[A-Za-z0-9+/]+={0,2}$', value):
                # Non-base64-alphabet characters in value.
                raise InvalidDigest(content_md5=value)
            try:
                self.headers['ETag'] = binascii.b2a_hex(
                    binascii.a2b_base64(value))
            except binascii.Error:
                # incorrect padding, most likely
                raise InvalidDigest(content_md5=value)

            if len(self.headers['ETag']) != 32:
                raise InvalidDigest(content_md5=value)

        if 'x-amz-tagging' in self.headers:
            raise S3NotImplemented('Object tagging is not supported.')

    @property
    def body(self):
        """
        swob.Request.body is not secure against malicious input.  It consumes
        too much memory without any check when the request body is excessively
        large.  Use xml() instead.
        """
        raise AttributeError("No attribute 'body'")

    def xml(self, max_length):
        """
        Similar to swob.Request.body, but it checks the content length before
        creating a body string.
        """
        te = self.headers.get('transfer-encoding', '')
        te = [x.strip() for x in te.split(',') if x.strip()]
        if te and (len(te) > 1 or te[-1] != 'chunked'):
            raise S3NotImplemented('A header you provided implies '
                                   'functionality that is not implemented',
                                   header='Transfer-Encoding')

        ml = self.message_length()
        if ml and ml > max_length:
            raise MalformedXML()

        if te or ml:
            # Limit the read similar to how SLO handles manifests
            with self.translate_read_errors():
                body = self.body_file.read(max_length)
        else:
            # No (or zero) Content-Length provided, and not chunked transfer;
            # no body. Assume zero-length, and enforce a required body below.
            return None

        return body

    def check_md5(self, body):
        if 'HTTP_CONTENT_MD5' not in self.environ:
            raise InvalidRequest('Missing required header for this request: '
                                 'Content-MD5')

        digest = base64_str(md5(body, usedforsecurity=False).digest())
        if self.environ['HTTP_CONTENT_MD5'] != digest:
            raise BadDigest(content_md5=self.environ['HTTP_CONTENT_MD5'])

    def _copy_source_headers(self):
        env = {}
        for key, value in self.environ.items():
            if key.startswith('HTTP_X_AMZ_COPY_SOURCE_'):
                env[key.replace('X_AMZ_COPY_SOURCE_', '')] = value

        return swob.HeaderEnvironProxy(env)

    def check_copy_source(self, app):
        """
        check_copy_source checks the copy source existence and if copying an
        object to itself, for illegal request parameters

        :returns: the source HEAD response
        """
        try:
            src_path = self.headers['X-Amz-Copy-Source']
        except KeyError:
            return None

        src_path, qs = src_path.partition('?')[::2]
        parsed = parse_qsl(qs, True)
        if not parsed:
            query = {}
        elif len(parsed) == 1 and parsed[0][0] == 'versionId':
            query = {'version-id': parsed[0][1]}
        else:
            raise InvalidArgument('X-Amz-Copy-Source',
                                  self.headers['X-Amz-Copy-Source'],
                                  'Unsupported copy source parameter.')

        src_path = unquote(src_path)
        src_path = src_path if src_path.startswith('/') else ('/' + src_path)
        src_bucket, src_obj = split_path(src_path, 0, 2, True)

        headers = swob.HeaderKeyDict()
        headers.update(self._copy_source_headers())

        src_resp = self.get_response(app, 'HEAD', src_bucket,
                                     swob.str_to_wsgi(src_obj),
                                     headers=headers, query=query)
        if src_resp.status_int == 304:  # pylint: disable-msg=E1101
            raise PreconditionFailed()

        if (self.container_name == src_bucket and
                self.object_name == src_obj and
                self.headers.get('x-amz-metadata-directive',
                                 'COPY') == 'COPY' and
                not query):
            raise InvalidRequest("This copy request is illegal "
                                 "because it is trying to copy an "
                                 "object to itself without "
                                 "changing the object's metadata, "
                                 "storage class, website redirect "
                                 "location or encryption "
                                 "attributes.")
        # We've done some normalizing; write back so it's ready for
        # to_swift_req
        self.headers['X-Amz-Copy-Source'] = quote(src_path)
        if query:
            self.headers['X-Amz-Copy-Source'] += \
                '?versionId=' + query['version-id']
        return src_resp

    def _canonical_uri(self):
        """
        Require bucket name in canonical_uri for v2 in virtual hosted-style.
        """
        raw_path_info = self.environ.get('RAW_PATH_INFO', self.path)
        if self.bucket_in_host:
            raw_path_info = '/' + self.bucket_in_host + raw_path_info
        return raw_path_info

    def signature_does_not_match_kwargs(self):
        return {
            'a_w_s_access_key_id': self.access_key,
            'string_to_sign': self.sig_checker.string_to_sign,
            'signature_provided': self.signature,
            'string_to_sign_bytes': ' '.join(
                format(ord(c), '02x')
                for c in self.sig_checker.string_to_sign.decode('latin1')),
        }

    @property
    def controller_name(self):
        return self.controller.__name__[:-len('Controller')]

    @property
    def controller(self):
        if self.is_service_request:
            return ServiceController

        if not self.conf.allow_multipart_uploads:
            multi_part = ['partNumber', 'uploadId', 'uploads']
            if len([p for p in multi_part if p in self.params]):
                raise S3NotImplemented("Multi-part feature isn't support")

        if 'acl' in self.params:
            return AclController
        if 'delete' in self.params:
            return MultiObjectDeleteController
        if 'location' in self.params:
            return LocationController
        if 'logging' in self.params:
            return LoggingStatusController
        if 'partNumber' in self.params:
            if self.method == 'PUT':
                return PartController
            else:
                return ObjectController
        if 'uploadId' in self.params:
            return UploadController
        if 'uploads' in self.params:
            return UploadsController
        if 'versioning' in self.params:
            return VersioningController
        if 'tagging' in self.params:
            return TaggingController
        if 'object-lock' in self.params:
            return ObjectLockController

        unsupported = ('notification', 'policy', 'requestPayment', 'torrent',
                       'website', 'cors', 'restore')
        if set(unsupported) & set(self.params):
            return UnsupportedController

        if self.is_object_request:
            return ObjectController
        return BucketController

    @property
    def is_service_request(self):
        return not self.container_name

    @property
    def is_bucket_request(self):
        return self.container_name and not self.object_name

    @property
    def is_object_request(self):
        return self.container_name and self.object_name

    @property
    def is_authenticated(self):
        return self.account is not None

    def to_swift_req(self, method, container, obj, query=None,
                     body=None, headers=None):
        """
        Create a Swift request based on this request's environment.
        """
        if self.account is None:
            account = swob.str_to_wsgi(self.access_key)
        else:
            account = self.account

        env = self.environ.copy()
        env['swift.infocache'] = self.environ.setdefault('swift.infocache', {})

        def sanitize(value):
            if set(value).issubset(string.printable):
                return value

            value = Header(value, 'UTF-8').encode()
            if value.startswith('=?utf-8?q?'):
                return '=?UTF-8?Q?' + value[10:]
            elif value.startswith('=?utf-8?b?'):
                return '=?UTF-8?B?' + value[10:]
            else:
                return value

        if 'headers_raw' in env:  # eventlet >= 0.19.0
            # See https://github.com/eventlet/eventlet/commit/67ec999
            for key, value in env['headers_raw']:
                if not key.lower().startswith('x-amz-meta-'):
                    continue
                # AWS ignores user-defined headers with these characters
                if any(c in key for c in ' "),/;<=>?@[\\]{}'):
                    # NB: apparently, '(' *is* allowed
                    continue
                # Note that this may have already been deleted, e.g. if the
                # client sent multiple headers with the same name, or both
                # x-amz-meta-foo-bar and x-amz-meta-foo_bar
                env.pop('HTTP_' + key.replace('-', '_').upper(), None)
                # Need to preserve underscores. Since we know '=' can't be
                # present, quoted-printable seems appropriate.
                key = key.replace('_', '=5F').replace('-', '_').upper()
                key = 'HTTP_X_OBJECT_META_' + key[11:]
                if key in env:
                    env[key] += ',' + sanitize(value)
                else:
                    env[key] = sanitize(value)
        else:  # mostly-functional fallback
            for key in self.environ:
                if not key.startswith('HTTP_X_AMZ_META_'):
                    continue
                # AWS ignores user-defined headers with these characters
                if any(c in key for c in ' "),/;<=>?@[\\]{}'):
                    # NB: apparently, '(' *is* allowed
                    continue
                env['HTTP_X_OBJECT_META_' + key[16:]] = sanitize(env[key])
                del env[key]

        copy_from_version_id = ''
        if 'HTTP_X_AMZ_COPY_SOURCE' in env and env['REQUEST_METHOD'] == 'PUT':
            env['HTTP_X_COPY_FROM'], copy_from_version_id = env[
                'HTTP_X_AMZ_COPY_SOURCE'].partition('?versionId=')[::2]
            del env['HTTP_X_AMZ_COPY_SOURCE']
            env['CONTENT_LENGTH'] = '0'
            if env.pop('HTTP_X_AMZ_METADATA_DIRECTIVE', None) == 'REPLACE':
                env['HTTP_X_FRESH_METADATA'] = 'True'
            else:
                copy_exclude_headers = ('HTTP_CONTENT_DISPOSITION',
                                        'HTTP_CONTENT_ENCODING',
                                        'HTTP_CONTENT_LANGUAGE',
                                        'CONTENT_TYPE',
                                        'HTTP_EXPIRES',
                                        'HTTP_CACHE_CONTROL',
                                        'HTTP_X_ROBOTS_TAG')
                for key in copy_exclude_headers:
                    env.pop(key, None)
                for key in list(env.keys()):
                    if key.startswith('HTTP_X_OBJECT_META_'):
                        del env[key]

        if self.conf.force_swift_request_proxy_log:
            env['swift.proxy_access_log_made'] = False
        env['swift.source'] = 'S3'
        if method is not None:
            env['REQUEST_METHOD'] = method

        if obj:
            path = '/v1/%s/%s/%s' % (account, container, obj)
        elif container:
            path = '/v1/%s/%s' % (account, container)
        else:
            path = '/v1/%s' % (account)
        env['PATH_INFO'] = path

        params = []
        if query is not None:
            for key, value in sorted(query.items()):
                if value is not None:
                    params.append('%s=%s' % (key, quote(str(value))))
                else:
                    params.append(key)
        if copy_from_version_id and not (query and query.get('version-id')):
            params.append('version-id=' + copy_from_version_id)
        env['QUERY_STRING'] = '&'.join(params)

        return swob.Request.blank(quote(path), environ=env, body=body,
                                  headers=headers)

    def _swift_success_codes(self, method, container, obj):
        """
        Returns a list of expected success codes from Swift.
        """
        if not container:
            # Swift account access.
            code_map = {
                'GET': [
                    HTTP_OK,
                ],
            }
        elif not obj:
            # Swift container access.
            code_map = {
                'HEAD': [
                    HTTP_NO_CONTENT,
                ],
                'GET': [
                    HTTP_OK,
                    HTTP_NO_CONTENT,
                ],
                'PUT': [
                    HTTP_CREATED,
                ],
                'POST': [
                    HTTP_NO_CONTENT,
                ],
                'DELETE': [
                    HTTP_NO_CONTENT,
                ],
            }
        else:
            # Swift object access.
            code_map = {
                'HEAD': [
                    HTTP_OK,
                    HTTP_PARTIAL_CONTENT,
                    HTTP_NOT_MODIFIED,
                ],
                'GET': [
                    HTTP_OK,
                    HTTP_PARTIAL_CONTENT,
                    HTTP_NOT_MODIFIED,
                ],
                'PUT': [
                    HTTP_CREATED,
                    HTTP_ACCEPTED,  # For SLO with heartbeating
                ],
                'POST': [
                    HTTP_ACCEPTED,
                ],
                'DELETE': [
                    HTTP_OK,
                    HTTP_NO_CONTENT,
                ],
            }

        return code_map[method]

    def _bucket_put_accepted_error(self, container, app):
        sw_req = self.to_swift_req('HEAD', container, None)
        info = get_container_info(sw_req.environ, app, swift_source='S3')
        sysmeta = info.get('sysmeta', {})
        try:
            acl = json.loads(sysmeta.get('s3api-acl',
                                         sysmeta.get('swift3-acl', '{}')))
            owner = acl.get('Owner')
        except (ValueError, TypeError, KeyError):
            owner = None
        if owner is None or owner == self.user_id:
            raise BucketAlreadyOwnedByYou(container)
        raise BucketAlreadyExists(container)

    def _swift_error_codes(self, method, container, obj, env, app):
        """
        Returns a dict from expected Swift error codes to the corresponding S3
        error responses.
        """
        if not container:
            # Swift account access.
            code_map = {
                'GET': {
                },
            }
        elif not obj:
            # Swift container access.
            code_map = {
                'HEAD': {
                    HTTP_NOT_FOUND: (NoSuchBucket, container),
                },
                'GET': {
                    HTTP_NOT_FOUND: (NoSuchBucket, container),
                },
                'PUT': {
                    HTTP_ACCEPTED: (self._bucket_put_accepted_error, container,
                                    app),
                },
                'POST': {
                    HTTP_NOT_FOUND: (NoSuchBucket, container),
                },
                'DELETE': {
                    HTTP_NOT_FOUND: (NoSuchBucket, container),
                    HTTP_CONFLICT: BucketNotEmpty,
                },
            }
        else:
            # Swift object access.

            # 404s differ depending upon whether the bucket exists
            # Note that base-container-existence checks happen elsewhere for
            # multi-part uploads, and get_container_info should be pulling
            # from the env cache
            def not_found_handler():
                if container.endswith(MULTIUPLOAD_SUFFIX) or \
                        is_success(get_container_info(
                            env, app, swift_source='S3').get('status')):
                    return NoSuchKey(obj)
                return NoSuchBucket(container)

            # Since BadDigest ought to plumb in some client-provided values,
            # defer evaluation until we know they're provided
            def bad_digest_handler():
                etag = binascii.hexlify(base64.b64decode(
                    env['HTTP_CONTENT_MD5']))
                return BadDigest(
                    expected_digest=etag,  # yes, really hex
                    # TODO: plumb in calculated_digest, as b64
                )

            code_map = {
                'HEAD': {
                    HTTP_NOT_FOUND: not_found_handler,
                    HTTP_PRECONDITION_FAILED: PreconditionFailed,
                },
                'GET': {
                    HTTP_NOT_FOUND: not_found_handler,
                    HTTP_PRECONDITION_FAILED: PreconditionFailed,
                },
                'PUT': {
                    HTTP_NOT_FOUND: (NoSuchBucket, container),
                    HTTP_UNPROCESSABLE_ENTITY: bad_digest_handler,
                    HTTP_REQUEST_ENTITY_TOO_LARGE: EntityTooLarge,
                    HTTP_LENGTH_REQUIRED: MissingContentLength,
                    HTTP_REQUEST_TIMEOUT: RequestTimeout,
                    HTTP_PRECONDITION_FAILED: PreconditionFailed,
                    HTTP_CLIENT_CLOSED_REQUEST: RequestTimeout,
                },
                'POST': {
                    HTTP_NOT_FOUND: not_found_handler,
                    HTTP_PRECONDITION_FAILED: PreconditionFailed,
                },
                'DELETE': {
                    HTTP_NOT_FOUND: (NoSuchKey, obj),
                },
            }

        return code_map[method]

    @contextlib.contextmanager
    def translate_read_errors(self):
        try:
            yield
        except S3InputIncomplete:
            raise IncompleteBody('The request body terminated unexpectedly')
        except S3InputSHA256Mismatch as err:
            # hopefully by now any modifications to the path (e.g. tenant to
            # account translation) will have been made by auth middleware
            raise XAmzContentSHA256Mismatch(
                client_computed_content_s_h_a256=err.expected,
                s3_computed_content_s_h_a256=err.computed,
            )
        except S3InputChecksumMismatch as e:
            raise BadDigest(
                'The %s you specified did not '
                'match the calculated checksum.' % e.args[0])
        except S3InputChecksumTrailerInvalid as e:
            raise InvalidRequest(
                'Value for %s trailing header is invalid.' % e.trailer)
        except S3InputChunkSignatureMismatch:
            raise SignatureDoesNotMatch(
                **self.signature_does_not_match_kwargs())
        except S3InputSizeError as e:
            raise IncompleteBody(
                number_bytes_expected=e.expected,
                number_bytes_provided=e.provided,
            )
        except S3InputChunkTooSmall as e:
            raise InvalidChunkSizeError(
                chunk=e.chunk_number,
                bad_chunk_size=e.bad_chunk_size,
            )
        except S3InputMalformedTrailer:
            raise MalformedTrailerError
        except S3InputMissingSecret:
            # XXX: We should really log something here. The poor user can't do
            # anything about this; we need to notify the operator to notify the
            # auth middleware developer
            raise S3NotImplemented('Transferring payloads in multiple chunks '
                                   'using aws-chunked is not supported.')
        except S3InputError:
            # All cases should be covered above, but belt & braces
            # NB: general exception handler in s3api.py will log traceback
            raise InternalError

    def _get_response(self, app, method, container, obj,
                      headers=None, body=None, query=None):
        """
        Calls the application with this request's environment.  Returns a
        S3Response object that wraps up the application's result.
        """

        method = method or self.environ['REQUEST_METHOD']

        if container is None:
            container = self.container_name
        if obj is None:
            obj = self.object_name

        sw_req = self.to_swift_req(method, container, obj, headers=headers,
                                   body=body, query=query)

        try:
            with self.translate_read_errors():
                sw_resp = sw_req.get_response(app)
        finally:
            # reuse account
            _, self.account, _ = split_path(sw_req.environ['PATH_INFO'],
                                            2, 3, True)
            self.environ['s3api.backend_path'] = sw_req.environ['PATH_INFO']

        # keep a record of the backend policy index so that the s3api can add
        # it to the headers of whatever response it returns, which may not
        # necessarily be this resp.
        self.policy_index = get_policy_index(sw_req.headers, sw_resp.headers)
        resp = S3Response.from_swift_resp(sw_resp)
        status = resp.status_int  # pylint: disable-msg=E1101

        if not self.user_id:
            if 'HTTP_X_USER_NAME' in sw_resp.environ:
                # keystone
                self.user_id = "%s:%s" % (
                    sw_resp.environ['HTTP_X_TENANT_NAME'],
                    sw_resp.environ['HTTP_X_USER_NAME'])
            else:
                # tempauth
                self.user_id = self.access_key

        success_codes = self._swift_success_codes(method, container, obj)
        error_codes = self._swift_error_codes(method, container, obj,
                                              sw_req.environ, app)

        if status in success_codes:
            return resp

        err_msg = resp.body

        if status in error_codes:
            err_resp = \
                error_codes[sw_resp.status_int]  # pylint: disable-msg=E1101
            if isinstance(err_resp, tuple):
                raise err_resp[0](*err_resp[1:])
            elif b'quota' in err_msg:
                raise err_resp(err_msg)
            else:
                raise err_resp()

        if status == HTTP_BAD_REQUEST:
            err_str = err_msg.decode('utf8')
            if 'X-Delete-At' in err_str:
                raise InvalidArgument('X-Delete-At',
                                      self.headers['X-Delete-At'],
                                      err_str)
            if 'X-Delete-After' in err_str:
                raise InvalidArgument('X-Delete-After',
                                      self.headers['X-Delete-After'],
                                      err_str)
            else:
                raise InvalidRequest(msg=err_str)
        if status == HTTP_UNAUTHORIZED:
            raise SignatureDoesNotMatch(
                **self.signature_does_not_match_kwargs())
        if status == HTTP_FORBIDDEN:
            raise AccessDenied(reason='forbidden')
        if status == HTTP_REQUESTED_RANGE_NOT_SATISFIABLE:
            self.validate_part_number(
                parts_count=resp.headers.get('x-amz-mp-parts-count'))
            raise InvalidRange()
        if status == HTTP_SERVICE_UNAVAILABLE:
            raise ServiceUnavailable()
        if status in (HTTP_RATE_LIMITED, HTTP_TOO_MANY_REQUESTS):
            if self.conf.ratelimit_as_client_error:
                raise SlowDown(status='429 Slow Down')
            raise SlowDown()
        if resp.status_int == HTTP_CONFLICT:
            if self.method == 'GET':
                raise BrokenMPU()
            else:
                raise ServiceUnavailable()

        raise InternalError('unexpected status code %d' % status)

    def get_response(self, app, method=None, container=None, obj=None,
                     headers=None, body=None, query=None):
        """
        get_response is an entry point to be extended for child classes.
        If additional tasks needed at that time of getting swift response,
        we can override this method.
        swift.common.middleware.s3api.s3request.S3Request need to just call
        _get_response to get pure swift response.
        """

        if 'HTTP_X_AMZ_ACL' in self.environ:
            handle_acl_header(self)

        return self._get_response(app, method, container, obj,
                                  headers, body, query)

    def get_validated_param(self, param, default, limit=MAX_32BIT_INT):
        value = default
        if param in self.params:
            try:
                value = int(self.params[param])
                if value < 0:
                    err_msg = 'Argument %s must be an integer between 0 and' \
                              ' %d' % (param, MAX_32BIT_INT)
                    raise InvalidArgument(param, self.params[param], err_msg)

                if value > MAX_32BIT_INT:
                    # check the value because int() could build either a long
                    # instance or a 64bit integer.
                    raise ValueError()

                if limit < value:
                    value = limit

            except ValueError:
                err_msg = 'Provided %s not an integer or within ' \
                          'integer range' % param
                raise InvalidArgument(param, self.params[param], err_msg)

        return value

    def get_container_info(self, app):
        """
        get_container_info will return a result dict of get_container_info
        from the backend Swift.

        :returns: a dictionary of container info from
                  swift.controllers.base.get_container_info
        :raises: NoSuchBucket when the container doesn't exist
        :raises: InternalError when the request failed without 404
        """
        if not self.is_authenticated:
            sw_req = self.to_swift_req('TEST', None, None, body='')
            # don't show log message of this request
            sw_req.environ['swift.proxy_access_log_made'] = True

            sw_resp = sw_req.get_response(app)

            if not sw_req.remote_user:
                raise SignatureDoesNotMatch(
                    **self.signature_does_not_match_kwargs())

            _, self.account, _ = split_path(sw_resp.environ['PATH_INFO'],
                                            2, 3, True)
        sw_req = self.to_swift_req('TEST', self.container_name, None)
        info = get_container_info(sw_req.environ, app, swift_source='S3')
        if is_success(info['status']):
            return info
        elif info['status'] == HTTP_NOT_FOUND:
            raise NoSuchBucket(self.container_name)
        elif info['status'] == HTTP_SERVICE_UNAVAILABLE:
            raise ServiceUnavailable()
        else:
            raise InternalError(
                'unexpected status code %d' % info['status'])

    def gen_multipart_manifest_delete_query(self, app, obj=None, version=None):
        if not self.conf.allow_multipart_uploads:
            return {}
        if not obj:
            obj = self.object_name
        query = {'symlink': 'get'}
        if version is not None:
            query['version-id'] = version
        resp = self.get_response(app, 'HEAD', obj=obj, query=query)
        if not resp.is_slo:
            return {}
        elif resp.sysmeta_headers.get(sysmeta_header('object', 'etag')):
            # Even if allow_async_delete is turned off, SLO will just handle
            # the delete synchronously, so we don't need to check before
            # setting async=on
            return {'multipart-manifest': 'delete', 'async': 'on'}
        else:
            return {'multipart-manifest': 'delete'}

    def set_acl_handler(self, handler):
        pass


class S3AclRequest(S3Request):
    """
    S3Acl request object.
    """

    def __init__(self, env, app=None, conf=None):
        super(S3AclRequest, self).__init__(env, app, conf)
        self.authenticate(app)
        self.acl_handler = None

    @property
    def controller(self):
        if 'acl' in self.params and not self.is_service_request:
            return S3AclController
        return super(S3AclRequest, self).controller

    def authenticate(self, app):
        """
        authenticate method will run pre-authenticate request and retrieve
        account information.
        Note that it currently supports only keystone and tempauth.
        (no support for the third party authentication middleware)
        """
        sw_req = self.to_swift_req('TEST', None, None, body='')
        # don't show log message of this request
        sw_req.environ['swift.proxy_access_log_made'] = True

        sw_resp = sw_req.get_response(app)

        if not sw_req.remote_user:
            raise SignatureDoesNotMatch(
                **self.signature_does_not_match_kwargs())

        _, self.account, _ = split_path(sw_resp.environ['PATH_INFO'],
                                        2, 3, True)

        if 'HTTP_X_USER_NAME' in sw_resp.environ:
            # keystone
            self.user_id = "%s:%s" % (sw_resp.environ['HTTP_X_TENANT_NAME'],
                                      sw_resp.environ['HTTP_X_USER_NAME'])
        else:
            # tempauth
            self.user_id = self.access_key

        sw_req.environ.get('swift.authorize', lambda req: None)(sw_req)
        self.environ['swift_owner'] = sw_req.environ.get('swift_owner', False)
        if 'REMOTE_USER' in sw_req.environ:
            self.environ['REMOTE_USER'] = sw_req.environ['REMOTE_USER']

        # Need to skip S3 authorization on subsequent requests to prevent
        # overwriting the account in PATH_INFO
        del self.environ['s3api.auth_details']

    def to_swift_req(self, method, container, obj, query=None,
                     body=None, headers=None):
        sw_req = super(S3AclRequest, self).to_swift_req(
            method, container, obj, query, body, headers)
        if self.account:
            sw_req.environ['swift_owner'] = True  # needed to set ACL
            sw_req.environ['swift.authorize_override'] = True
            sw_req.environ['swift.authorize'] = lambda req: None
        return sw_req

    def get_acl_response(self, app, method=None, container=None, obj=None,
                         headers=None, body=None, query=None):
        """
        Wrapper method of _get_response to add s3 acl information
        from response sysmeta headers.
        """

        resp = self._get_response(
            app, method, container, obj, headers, body, query)
        resp.bucket_acl = decode_acl(
            'container', resp.sysmeta_headers, self.conf.allow_no_owner)
        resp.object_acl = decode_acl(
            'object', resp.sysmeta_headers, self.conf.allow_no_owner)

        return resp

    def get_response(self, app, method=None, container=None, obj=None,
                     headers=None, body=None, query=None):
        """
        Wrap up get_response call to hook with acl handling method.
        """
        if not self.acl_handler:
            # we should set acl_handler all time before calling get_response
            raise Exception('get_response called before set_acl_handler')
        resp = self.acl_handler.handle_acl(
            app, method, container, obj, headers)

        # possible to skip recalling get_response_acl if resp is not
        # None (e.g. HEAD)
        if resp:
            return resp
        return self.get_acl_response(app, method, container, obj,
                                     headers, body, query)

    def set_acl_handler(self, acl_handler):
        self.acl_handler = acl_handler


class SigV4Request(SigV4Mixin, S3Request):
    pass


class SigV4S3AclRequest(SigV4Mixin, S3AclRequest):
    pass
