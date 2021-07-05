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
from email.header import Header
from hashlib import sha1, sha256
import hmac
import re
import six
# pylint: disable-msg=import-error
from six.moves.urllib.parse import quote, unquote, parse_qsl
import string

from swift.common.utils import split_path, json, get_swift_info, \
    close_if_possible, md5
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
    TaggingController
from swift.common.middleware.s3api.s3response import AccessDenied, \
    InvalidArgument, InvalidDigest, BucketAlreadyOwnedByYou, \
    RequestTimeTooSkewed, S3Response, SignatureDoesNotMatch, \
    BucketAlreadyExists, BucketNotEmpty, EntityTooLarge, \
    InternalError, NoSuchBucket, NoSuchKey, PreconditionFailed, InvalidRange, \
    MissingContentLength, InvalidStorageClass, S3NotImplemented, InvalidURI, \
    MalformedXML, InvalidRequest, RequestTimeout, InvalidBucketName, \
    BadDigest, AuthorizationHeaderMalformed, SlowDown, \
    AuthorizationQueryParametersError, ServiceUnavailable
from swift.common.middleware.s3api.exception import NotS3Request, \
    BadSwiftRequest
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
    'response-content-type', 'response-expires', 'cors', 'tagging', 'restore'
])


MAX_32BIT_INT = 2147483647
SIGV2_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%S'
SIGV4_X_AMZ_DATE_FORMAT = '%Y%m%dT%H%M%SZ'
SERVICE = 's3'  # useful for mocking out in tests


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


class HashingInput(object):
    """
    wsgi.input wrapper to verify the hash of the input as it's read.
    """
    def __init__(self, reader, content_length, hasher, expected_hex_hash):
        self._input = reader
        self._to_read = content_length
        self._hasher = hasher()
        self._expected = expected_hex_hash

    def read(self, size=None):
        chunk = self._input.read(size)
        self._hasher.update(chunk)
        self._to_read -= len(chunk)
        short_read = bool(chunk) if size is None else (len(chunk) < size)
        if self._to_read < 0 or (short_read and self._to_read) or (
                self._to_read == 0 and
                self._hasher.hexdigest() != self._expected):
            self.close()
            # Since we don't return the last chunk, the PUT never completes
            raise swob.HTTPUnprocessableEntity(
                'The X-Amz-Content-SHA56 you specified did not match '
                'what we received.')
        return chunk

    def close(self):
        close_if_possible(self._input)


class SigV4Mixin(object):
    """
    A request class mixin to provide S3 signature v4 functionality
    """

    def check_signature(self, secret):
        secret = utf8encode(secret)
        user_signature = self.signature
        derived_secret = b'AWS4' + secret
        for scope_piece in self.scope.values():
            derived_secret = hmac.new(
                derived_secret, scope_piece.encode('utf8'), sha256).digest()
        valid_signature = hmac.new(
            derived_secret, self.string_to_sign, sha256).hexdigest()
        return user_signature == valid_signature

    @property
    def _is_query_auth(self):
        return 'X-Amz-Credential' in self.params

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
                                   'or x-amz-date header')

            if timestamp < 0:
                raise AccessDenied('AWS authentication requires a valid Date '
                                   'or x-amz-date header')

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
            raise AccessDenied()
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
            raise AccessDenied('Request has expired')

    def _parse_credential(self, credential_string):
        parts = credential_string.split("/")
        # credential must be in following format:
        # <access-key-id>/<date>/<AWS-region>/<AWS-service>/aws4_request
        if not parts[0] or len(parts) != 5:
            raise AccessDenied()
        return dict(zip(['access', 'date', 'region', 'service', 'terminal'],
                        parts))

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
            cred_param = self._parse_credential(
                swob.wsgi_to_str(self.params['X-Amz-Credential']))
            sig = swob.wsgi_to_str(self.params['X-Amz-Signature'])
            if not sig:
                raise AccessDenied()
        except KeyError:
            raise AccessDenied()

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
        cred_param = self._parse_credential(auth_str.partition(
            "Credential=")[2].split(',')[0])
        sig = auth_str.partition("Signature=")[2].split(',')[0]
        if not sig:
            raise AccessDenied()
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
                for (k, v) in six.iteritems(self.headers))

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
        return swob.wsgi_to_bytes(self.environ.get('RAW_PATH_INFO', self.path))

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
        cr = [swob.wsgi_to_bytes(self.method.upper())]

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
        if 'X-Amz-Credential' in self.params:
            # V4 with query parameters only
            hashed_payload = 'UNSIGNED-PAYLOAD'
        elif 'X-Amz-Content-SHA256' not in self.headers:
            msg = 'Missing required header for this request: ' \
                  'x-amz-content-sha256'
            raise InvalidRequest(msg)
        else:
            hashed_payload = self.headers['X-Amz-Content-SHA256']
            if hashed_payload != 'UNSIGNED-PAYLOAD':
                if self.content_length == 0:
                    if hashed_payload.lower() != sha256().hexdigest():
                        raise BadDigest(
                            'The X-Amz-Content-SHA56 you specified did not '
                            'match what we received.')
                elif self.content_length:
                    self.environ['wsgi.input'] = HashingInput(
                        self.environ['wsgi.input'],
                        self.content_length,
                        sha256,
                        hashed_payload.lower())
                # else, length not provided -- Swift will kick out a
                # 411 Length Required which will get translated back
                # to a S3-style response in S3Request._swift_error_codes
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

    def _string_to_sign(self):
        """
        Create 'StringToSign' value in Amazon terminology for v4.
        """
        return b'\n'.join([
            b'AWS4-HMAC-SHA256',
            self.timestamp.amz_date_format.encode('ascii'),
            '/'.join(self.scope.values()).encode('utf8'),
            sha256(self._canonical_request()).hexdigest().encode('ascii')])

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
        # Lock in string-to-sign now, before we start messing with query params
        self.string_to_sign = self._string_to_sign()
        self.environ['s3api.auth_details'] = {
            'access_key': self.access_key,
            'signature': self.signature,
            'string_to_sign': self.string_to_sign,
            'check_signature': self.check_signature,
        }
        self.account = None
        self.user_id = None

        # Avoids that swift.swob.Response replaces Location header value
        # by full URL when absolute path given. See swift.swob for more detail.
        self.environ['swift.leave_relative_location'] = True

    def check_signature(self, secret):
        secret = utf8encode(secret)
        user_signature = self.signature
        valid_signature = base64.b64encode(hmac.new(
            secret, self.string_to_sign, sha1).digest()).strip()
        if not six.PY2:
            valid_signature = valid_signature.decode('ascii')
        return user_signature == valid_signature

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
                                   'or x-amz-date header')

            if timestamp < 0:
                raise AccessDenied('AWS authentication requires a valid Date '
                                   'or x-amz-date header')
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

    def _parse_host(self):
        storage_domain = self.conf.storage_domain
        if not storage_domain:
            return None

        if not storage_domain.startswith('.'):
            storage_domain = '.' + storage_domain

        if 'HTTP_HOST' in self.environ:
            given_domain = self.environ['HTTP_HOST']
        elif 'SERVER_NAME' in self.environ:
            given_domain = self.environ['SERVER_NAME']
        else:
            return None

        port = ''
        if ':' in given_domain:
            given_domain, port = given_domain.rsplit(':', 1)
        if given_domain.endswith(storage_domain):
            return given_domain[:-len(storage_domain)]

        return None

    def _parse_uri(self):
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
        return (bucket, obj)

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
            raise AccessDenied()

        if not all([access, sig, expires]):
            raise AccessDenied()

        return access, sig

    def _parse_header_authentication(self):
        """
        Parse v2 header authentication info

        :returns: a tuple of access_key and signature
        :raises: AccessDenied
        """
        auth_str = swob.wsgi_to_str(self.headers['Authorization'])
        if not auth_str.startswith('AWS ') or ':' not in auth_str:
            raise AccessDenied()
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
            raise AccessDenied()

        if S3Timestamp.now() > ex:
            raise AccessDenied('Request has expired')

        if ex >= 2 ** 31:
            raise AccessDenied(
                'Invalid date (should be seconds since epoch): %s' %
                self.params['Expires'])

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
                               'or x-amz-date header')

        # Anyways, request timestamp should be validated
        epoch = S3Timestamp(0)
        if self.timestamp < epoch:
            raise AccessDenied()

        # If the standard date is too far ahead or behind, it is an
        # error
        delta = abs(int(self.timestamp) - int(S3Timestamp.now()))
        if delta > self.conf.allowable_clock_skew:
            raise RequestTimeTooSkewed()

    def _validate_headers(self):
        if 'CONTENT_LENGTH' in self.environ:
            try:
                if self.content_length < 0:
                    raise InvalidArgument('Content-Length',
                                          self.content_length)
            except (ValueError, TypeError):
                raise InvalidArgument('Content-Length',
                                      self.environ['CONTENT_LENGTH'])

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

        # https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-streaming.html
        # describes some of what would be required to support this
        if any(['aws-chunked' in self.headers.get('content-encoding', ''),
                'STREAMING-AWS4-HMAC-SHA256-PAYLOAD' == self.headers.get(
                    'x-amz-content-sha256', ''),
                'x-amz-decoded-content-length' in self.headers]):
            raise S3NotImplemented('Transfering payloads in multiple chunks '
                                   'using aws-chunked is not supported.')

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
            try:
                body = self.body_file.read(max_length)
            except swob.HTTPException as err:
                if err.status_int == HTTP_UNPROCESSABLE_ENTITY:
                    # Special case for HashingInput check
                    raise BadDigest(
                        'The X-Amz-Content-SHA56 you specified did not '
                        'match what we received.')
                raise
        else:
            # No (or zero) Content-Length provided, and not chunked transfer;
            # no body. Assume zero-length, and enforce a required body below.
            return None

        return body

    def check_md5(self, body):
        if 'HTTP_CONTENT_MD5' not in self.environ:
            raise InvalidRequest('Missing required header for this request: '
                                 'Content-MD5')

        digest = base64.b64encode(md5(
            body, usedforsecurity=False).digest()).strip().decode('ascii')
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

        src_resp = self.get_response(app, 'HEAD', src_bucket, src_obj,
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

    def _string_to_sign(self):
        """
        Create 'StringToSign' value in Amazon terminology for v2.
        """
        amz_headers = {}

        buf = [swob.wsgi_to_bytes(wsgi_str) for wsgi_str in [
            self.method,
            _header_strip(self.headers.get('Content-MD5')) or '',
            _header_strip(self.headers.get('Content-Type')) or '']]

        if 'headers_raw' in self.environ:  # eventlet >= 0.19.0
            # See https://github.com/eventlet/eventlet/commit/67ec999
            amz_headers = defaultdict(list)
            for key, value in self.environ['headers_raw']:
                key = key.lower()
                if not key.startswith('x-amz-'):
                    continue
                amz_headers[key.strip()].append(value.strip())
            amz_headers = dict((key, ','.join(value))
                               for key, value in amz_headers.items())
        else:  # mostly-functional fallback
            amz_headers = dict((key.lower(), value)
                               for key, value in self.headers.items()
                               if key.lower().startswith('x-amz-'))

        if self._is_header_auth:
            if 'x-amz-date' in amz_headers:
                buf.append(b'')
            elif 'Date' in self.headers:
                buf.append(swob.wsgi_to_bytes(self.headers['Date']))
        elif self._is_query_auth:
            buf.append(swob.wsgi_to_bytes(self.params['Expires']))
        else:
            # Should have already raised NotS3Request in _parse_auth_info,
            # but as a sanity check...
            raise AccessDenied()

        for key, value in sorted(amz_headers.items()):
            buf.append(swob.wsgi_to_bytes("%s:%s" % (key, value)))

        path = self._canonical_uri()
        if self.query_string:
            path += '?' + self.query_string
        params = []
        if '?' in path:
            path, args = path.split('?', 1)
            for key, value in sorted(self.params.items()):
                if key in ALLOWED_SUB_RESOURCES:
                    params.append('%s=%s' % (key, value) if value else key)
        if params:
            buf.append(swob.wsgi_to_bytes('%s?%s' % (path, '&'.join(params))))
        else:
            buf.append(swob.wsgi_to_bytes(path))
        return b'\n'.join(buf)

    def signature_does_not_match_kwargs(self):
        return {
            'a_w_s_access_key_id': self.access_key,
            'string_to_sign': self.string_to_sign,
            'signature_provided': self.signature,
            'string_to_sign_bytes': ' '.join(
                format(ord(c), '02x')
                for c in self.string_to_sign.decode('latin1')),
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
            return PartController
        if 'uploadId' in self.params:
            return UploadController
        if 'uploads' in self.params:
            return UploadsController
        if 'versioning' in self.params:
            return VersioningController
        if 'tagging' in self.params:
            return TaggingController

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
            account = self.access_key
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

            code_map = {
                'HEAD': {
                    HTTP_NOT_FOUND: not_found_handler,
                    HTTP_PRECONDITION_FAILED: PreconditionFailed,
                },
                'GET': {
                    HTTP_NOT_FOUND: not_found_handler,
                    HTTP_PRECONDITION_FAILED: PreconditionFailed,
                    HTTP_REQUESTED_RANGE_NOT_SATISFIABLE: InvalidRange,
                },
                'PUT': {
                    HTTP_NOT_FOUND: (NoSuchBucket, container),
                    HTTP_UNPROCESSABLE_ENTITY: BadDigest,
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
            sw_resp = sw_req.get_response(app)
        except swob.HTTPException as err:
            sw_resp = err
        else:
            # reuse account
            _, self.account, _ = split_path(sw_resp.environ['PATH_INFO'],
                                            2, 3, True)
            # Propagate swift.backend_path in environ for middleware
            # in pipeline that need Swift PATH_INFO like ceilometermiddleware.
            self.environ['s3api.backend_path'] = \
                sw_resp.environ['PATH_INFO']
            # Propogate backend headers back into our req headers for logging
            for k, v in sw_req.headers.items():
                if k.lower().startswith('x-backend-'):
                    self.headers.setdefault(k, v)

        resp = S3Response.from_swift_resp(sw_resp)
        status = resp.status_int  # pylint: disable-msg=E1101

        if not self.user_id:
            if 'HTTP_X_USER_NAME' in sw_resp.environ:
                # keystone
                self.user_id = "%s:%s" % (
                    sw_resp.environ['HTTP_X_TENANT_NAME'],
                    sw_resp.environ['HTTP_X_USER_NAME'])
                if six.PY2 and not isinstance(self.user_id, bytes):
                    self.user_id = self.user_id.encode('utf8')
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
            raise BadSwiftRequest(err_msg.decode('utf8'))
        if status == HTTP_UNAUTHORIZED:
            raise SignatureDoesNotMatch(
                **self.signature_does_not_match_kwargs())
        if status == HTTP_FORBIDDEN:
            raise AccessDenied()
        if status == HTTP_SERVICE_UNAVAILABLE:
            raise ServiceUnavailable()
        if status in (HTTP_RATE_LIMITED, HTTP_TOO_MANY_REQUESTS):
            if self.conf.ratelimit_as_client_error:
                raise SlowDown(status='429 Slow Down')
            raise SlowDown()

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
        sw_req = self.to_swift_req(app, self.container_name, None)
        info = get_container_info(sw_req.environ, app, swift_source='S3')
        if is_success(info['status']):
            return info
        elif info['status'] == 404:
            raise NoSuchBucket(self.container_name)
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
            if six.PY2 and not isinstance(self.user_id, bytes):
                self.user_id = self.user_id.encode('utf8')
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
