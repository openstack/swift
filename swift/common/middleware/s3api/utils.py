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
import calendar
import datetime
import email.utils
import re
import time
import uuid

from swift.common import utils
from swift.common.constraints import check_utf8
from swift.common.swob import wsgi_to_str
from swift.common.middleware.s3api.exception import \
    InvalidBucketNameParseError, InvalidURIParseError

MULTIUPLOAD_SUFFIX = '+segments'


def sysmeta_prefix(resource):
    """
    Returns the system metadata prefix for given resource type.
    """
    if resource.lower() == 'object':
        return 'x-object-sysmeta-s3api-'
    else:
        return 'x-container-sysmeta-s3api-'


def sysmeta_header(resource, name):
    """
    Returns the system metadata header for given resource type and name.
    """
    return sysmeta_prefix(resource) + name


def camel_to_snake(camel):
    return re.sub('(.)([A-Z])', r'\1_\2', camel).lower()


def snake_to_camel(snake):
    return snake.title().replace('_', '')


def unique_id():
    result = base64.urlsafe_b64encode(str(uuid.uuid4()).encode('ascii'))
    return result.decode('ascii')


def utf8encode(s):
    if s is None or isinstance(s, bytes):
        return s
    return s.encode('utf8')


def utf8decode(s):
    if isinstance(s, bytes):
        s = s.decode('utf8')
    return s


def validate_bucket_name(name, dns_compliant_bucket_names):
    """
    Validates the name of the bucket against S3 criteria,
    http://docs.amazonwebservices.com/AmazonS3/latest/BucketRestrictions.html
    True is valid, False is invalid.
    """
    valid_chars = '-.a-z0-9'
    if not dns_compliant_bucket_names:
        valid_chars += 'A-Z_'
    max_len = 63 if dns_compliant_bucket_names else 255

    if len(name) < 3 or len(name) > max_len or not name[0].isalnum():
        # Bucket names should be between 3 and 63 (or 255) characters long
        # Bucket names must start with a letter or a number
        return False
    elif dns_compliant_bucket_names and (
            '.-' in name or '-.' in name or '..' in name or
            not name[-1].isalnum()):
        # Bucket names cannot contain dashes next to periods
        # Bucket names cannot contain two adjacent periods
        # Bucket names must end with a letter or a number
        return False
    elif name.endswith('.'):
        # Bucket names must not end with dot
        return False
    elif re.match(r"^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.)"
                  r"{3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$",
                  name):
        # Bucket names cannot be formatted as an IP Address
        return False
    elif not re.match("^[%s]*$" % valid_chars, name):
        # Bucket names can contain lowercase letters, numbers, and hyphens.
        return False
    else:
        return True


def get_s3_access_key_id(req):
    """
    Return the S3 access_key_id user for the request,
    or None if it does not look like an S3 request.

    :param req: a swob.Request instance

    :returns: access_key_id if available, else None
    """

    authorization = req.headers.get('Authorization', '')
    if authorization.startswith('AWS '):
        # v2
        return authorization[4:].rsplit(':', 1)[0]
    if authorization.startswith('AWS4-HMAC-SHA256 '):
        # v4
        return authorization.partition('Credential=')[2].split('/', 1)[0]
    params = req.params
    if 'AWSAccessKeyId' in params:
        # v2
        return params['AWSAccessKeyId']
    if 'X-Amz-Credential' in params:
        # v4
        return params['X-Amz-Credential'].split('/', 1)[0]

    return None


def is_s3_req(req):
    """
    Check whether a request looks like it ought to be an S3 request.

    :param req: a swob.Request instance

    :returns: True if access_key_id is available, False if not
    """
    return bool(get_s3_access_key_id(req))


def parse_host(environ, storage_domains):
    """
    A bucket-in-host request has the bucket name as the first part of a
    ``.``-separated host. If the host ends with any of
    the given storage_domains then the bucket name is returned.
    Otherwise ``None`` is returned.

    :param environ: an environment dict
    :param storage_domains: a list of storage domains for which bucket-in-host
                            is supported.
    :returns: bucket name or None
    """

    if 'HTTP_HOST' in environ:
        given_domain = environ['HTTP_HOST']
    elif 'SERVER_NAME' in environ:
        given_domain = environ['SERVER_NAME']
    else:
        return None
    if ':' in given_domain:
        given_domain = given_domain.rsplit(':', 1)[0]

    for storage_domain in storage_domains:
        if not storage_domain.startswith('.'):
            storage_domain = '.' + storage_domain

        if given_domain.endswith(storage_domain):
            return given_domain[:-len(storage_domain)]

    return None


def parse_path(req, bucket_in_host, dns_compliant_bucket_names):
    """
    :params req: a swob.Request instance
    :params bucket_in_host: A bucket-in-host request has the bucket name as
                            the first part of a ``.``-separated host.
    :params dns_compliant_bucket_names: whether to validate that the bucket
                                        name must be dns compliant

    :returns: WSGI string
    """
    if not check_utf8(wsgi_to_str(req.environ['PATH_INFO'])):
        raise InvalidURIParseError(req.path)

    if bucket_in_host:
        obj = req.environ['PATH_INFO'][1:] or None
        return bucket_in_host, obj

    bucket, obj = req.split_path(0, 2, True)

    if bucket and not validate_bucket_name(
            bucket, dns_compliant_bucket_names):
        # Ignore GET service case
        raise InvalidBucketNameParseError(bucket)
    return bucket, obj


def extract_bucket_and_key(req, storage_domains,
                           dns_compliant_bucket_names):
    """
    Extract the bucket and object key from the request's PATH_INFO. Support
    bucket-in-host if storage_domains and HTTP_HOST or SERVER_NAME are
    specified. Otherwise the bucket is parsed from PATH_INFO.

    :param req: a swob.Request instance
    :param storage_domains: a list of storage domains for which bucket-in-host
                            is supported.
    :param dns_compliant_bucket_names: whether to validate that the bucket
                                       name must be dns compliant

    :returns: a tuple of (bucket, key). If the request path is invalid
              the tuple (None, None) is returned.
    """
    try:
        bucket_in_host = parse_host(req.environ, storage_domains)
        bucket, key = parse_path(
            req, bucket_in_host, dns_compliant_bucket_names)
    except (InvalidBucketNameParseError, InvalidURIParseError):
        bucket, key = None, None
    return bucket, key


class S3Timestamp(utils.Timestamp):
    S3_XML_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"

    @property
    def s3xmlformat(self):
        dt = datetime.datetime.fromtimestamp(
            self.ceil(), datetime.timezone.utc)
        return dt.strftime(self.S3_XML_FORMAT)

    @classmethod
    def from_s3xmlformat(cls, date_string):
        dt = datetime.datetime.strptime(date_string, cls.S3_XML_FORMAT)
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        seconds = calendar.timegm(dt.timetuple())
        return cls(seconds)

    @property
    def amz_date_format(self):
        """
        this format should be like 'YYYYMMDDThhmmssZ'
        """
        return self.isoformat.replace(
            '-', '').replace(':', '')[:-7] + 'Z'


def mktime(timestamp_str, time_format='%Y-%m-%dT%H:%M:%S'):
    """
    mktime creates a float instance in epoch time really like as time.mktime

    the difference from time.mktime is allowing to 2 formats string for the
    argument for the S3 testing usage.
    TODO: support

    :param timestamp_str: a string of timestamp formatted as
                          (a) RFC2822 (e.g. date header)
                          (b) %Y-%m-%dT%H:%M:%S (e.g. copy result)
    :param time_format: a string of format to parse in (b) process
    :returns: a float instance in epoch time
    """
    # time_tuple is the *remote* local time
    time_tuple = email.utils.parsedate_tz(timestamp_str)
    if time_tuple is None:
        time_tuple = time.strptime(timestamp_str, time_format)
        # add timezone info as utc (no time difference)
        time_tuple += (0, )

    # We prefer calendar.gmtime and a manual adjustment over
    # email.utils.mktime_tz because older versions of Python (<2.7.4) may
    # double-adjust for timezone in some situations (such when swift changes
    # os.environ['TZ'] without calling time.tzset()).
    epoch_time = calendar.timegm(time_tuple) - time_tuple[9]

    return epoch_time


class Config(dict):
    DEFAULTS = {
        'storage_domains': [],
        'location': 'us-east-1',
        'force_swift_request_proxy_log': False,
        'dns_compliant_bucket_names': True,
        'allow_multipart_uploads': True,
        'allow_no_owner': False,
        'allowable_clock_skew': 900,
        'ratelimit_as_client_error': False,
        'max_upload_part_num': 1000,
    }

    def __init__(self, base=None):
        self.update(self.DEFAULTS)
        if base is not None:
            self.update(base)

    def __getattr__(self, name):
        if name not in self:
            raise AttributeError("No attribute '%s'" % name)

        return self[name]

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        del self[name]

    def update(self, other):
        if hasattr(other, 'keys'):
            for key in other.keys():
                self[key] = other[key]
        else:
            for key, value in other:
                self[key] = value

    def __setitem__(self, key, value):
        if isinstance(self.get(key), bool):
            dict.__setitem__(self, key, utils.config_true_value(value))
        elif isinstance(self.get(key), int):
            try:
                dict.__setitem__(self, key, int(value))
            except ValueError:
                if value:  # No need to raise the error if value is ''
                    raise
        else:
            dict.__setitem__(self, key, value)
