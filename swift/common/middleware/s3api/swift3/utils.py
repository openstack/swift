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
import email.utils
import re
import socket
import time
from urllib import unquote
import uuid

from swift.common.utils import get_logger

# Need for check_path_header
from swift.common import utils
from swift.common.swob import HTTPPreconditionFailed

from swift3.cfg import CONF

LOGGER = get_logger(CONF, log_route='swift3')

MULTIUPLOAD_SUFFIX = '+segments'


def sysmeta_prefix(resource):
    """
    Returns the system metadata prefix for given resource type.
    """
    if resource == 'object':
        return 'x-object-sysmeta-swift3-'
    else:
        return 'x-container-sysmeta-swift3-'


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
    return base64.urlsafe_b64encode(str(uuid.uuid4()))


def utf8encode(s):
    if isinstance(s, unicode):
        s = s.encode('utf8')
    return s


def utf8decode(s):
    if isinstance(s, str):
        s = s.decode('utf8')
    return s


def check_path_header(req, name, length, error_msg):
    # FIXME: replace swift.common.constraints check_path_header
    #        when swift3 supports swift 2.2 or later
    """
    Validate that the value of path-like header is
    well formatted. We assume the caller ensures that
    specific header is present in req.headers.

    :param req: HTTP request object
    :param name: header name
    :param length: length of path segment check
    :param error_msg: error message for client
    :returns: A tuple with path parts according to length
    :raise: HTTPPreconditionFailed if header value
            is not well formatted.
    """
    src_header = unquote(req.headers.get(name))
    if not src_header.startswith('/'):
        src_header = '/' + src_header
    try:
        return utils.split_path(src_header, length, length, True)
    except ValueError:
        raise HTTPPreconditionFailed(
            request=req,
            body=error_msg)


def is_valid_ipv6(ip):
    # FIXME: replace with swift.common.ring.utils is_valid_ipv6
    #        when swift3 requires swift 2.3 or later
    #        --or--
    #        swift.common.utils is_valid_ipv6 when swift3 requires swift>2.9
    """
    Returns True if the provided ip is a valid IPv6-address
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except socket.error:  # not a valid IPv6 address
        return False
    return True


def validate_bucket_name(name):
        """
        Validates the name of the bucket against S3 criteria,
        http://docs.amazonwebservices.com/AmazonS3/latest/BucketRestrictions.html
        True is valid, False is invalid.
        """
        valid_chars = '-.a-z0-9'
        if not CONF.dns_compliant_bucket_names:
            valid_chars += 'A-Z_'
        max_len = 63 if CONF.dns_compliant_bucket_names else 255

        if len(name) < 3 or len(name) > max_len or not name[0].isalnum():
            # Bucket names should be between 3 and 63 (or 255) characters long
            # Bucket names must start with a letter or a number
            return False
        elif CONF.dns_compliant_bucket_names and (
                '.-' in name or '-.' in name or '..' in name or
                not name[-1].isalnum()):
            # Bucket names cannot contain dashes next to periods
            # Bucket names cannot contain two adjacent periods
            # Bucket names must end with a letter or a number
            return False
        elif name.endswith('.'):
            # Bucket names must not end with dot
            return False
        elif re.match("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.)"
                      "{3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$",
                      name):
            # Bucket names cannot be formatted as an IP Address
            return False
        elif not re.match("^[%s]*$" % valid_chars, name):
            # Bucket names can contain lowercase letters, numbers, and hyphens.
            return False
        else:
            return True


class S3Timestamp(utils.Timestamp):
    @property
    def s3xmlformat(self):
        return self.isoformat[:-7] + '.000Z'

    @property
    def amz_date_format(self):
        """
        this format should be like 'YYYYMMDDThhmmssZ'
        """
        return self.isoformat.replace(
            '-', '').replace(':', '')[:-7] + 'Z'

    @classmethod
    def now(cls):
        return cls(time.time())


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
    :return : a float instance in epoch time
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
