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
import time
import uuid

from swift.common import utils

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
    return base64.urlsafe_b64encode(str(uuid.uuid4()))


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
    def __init__(self, base=None):
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
