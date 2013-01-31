# Copyright (c) 2010-2012 OpenStack, LLC.
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

import os
import urllib
from ConfigParser import ConfigParser, NoSectionError, NoOptionError

from swift.common.swob import HTTPBadRequest, HTTPLengthRequired, \
    HTTPRequestEntityTooLarge

constraints_conf = ConfigParser()
constraints_conf.read('/etc/swift/swift.conf')


def constraints_conf_int(name, default):
    try:
        return int(constraints_conf.get('swift-constraints', name))
    except (NoSectionError, NoOptionError):
        return default


#: Max file size allowed for objects
MAX_FILE_SIZE = constraints_conf_int('max_file_size',
                                     5368709122)  # 5 * 1024 * 1024 * 1024 + 2
#: Max length of the name of a key for metadata
MAX_META_NAME_LENGTH = constraints_conf_int('max_meta_name_length', 128)
#: Max length of the value of a key for metadata
MAX_META_VALUE_LENGTH = constraints_conf_int('max_meta_value_length', 256)
#: Max number of metadata items
MAX_META_COUNT = constraints_conf_int('max_meta_count', 90)
#: Max overall size of metadata
MAX_META_OVERALL_SIZE = constraints_conf_int('max_meta_overall_size', 4096)
#: Max object name length
MAX_OBJECT_NAME_LENGTH = constraints_conf_int('max_object_name_length', 1024)
#: Max object list length of a get request for a container
CONTAINER_LISTING_LIMIT = constraints_conf_int('container_listing_limit',
                                               10000)
#: Max container list length of a get request for an account
ACCOUNT_LISTING_LIMIT = constraints_conf_int('account_listing_limit', 10000)
#: Max account name length
MAX_ACCOUNT_NAME_LENGTH = constraints_conf_int('max_account_name_length', 256)
#: Max container name length
MAX_CONTAINER_NAME_LENGTH = constraints_conf_int('max_container_name_length',
                                                 256)


#: Query string format= values to their corresponding content-type values
FORMAT2CONTENT_TYPE = {'plain': 'text/plain', 'json': 'application/json',
                       'xml': 'application/xml'}


def check_metadata(req, target_type):
    """
    Check metadata sent in the request headers.

    :param req: request object
    :param target_type: str: one of: object, container, or account: indicates
                        which type the target storage for the metadata is
    :raises HTTPBadRequest: bad metadata
    """
    prefix = 'x-%s-meta-' % target_type.lower()
    meta_count = 0
    meta_size = 0
    for key, value in req.headers.iteritems():
        if not key.lower().startswith(prefix):
            continue
        key = key[len(prefix):]
        if not key:
            return HTTPBadRequest(body='Metadata name cannot be empty',
                                  request=req, content_type='text/plain')
        meta_count += 1
        meta_size += len(key) + len(value)
        if len(key) > MAX_META_NAME_LENGTH:
            return HTTPBadRequest(
                body='Metadata name too long; max %d' % MAX_META_NAME_LENGTH,
                request=req, content_type='text/plain')
        elif len(value) > MAX_META_VALUE_LENGTH:
            return HTTPBadRequest(
                body='Metadata value too long; max %d' % MAX_META_VALUE_LENGTH,
                request=req, content_type='text/plain')
        elif meta_count > MAX_META_COUNT:
            return HTTPBadRequest(
                body='Too many metadata items; max %d' % MAX_META_COUNT,
                request=req, content_type='text/plain')
        elif meta_size > MAX_META_OVERALL_SIZE:
            return HTTPBadRequest(
                body='Total metadata too large; max %d'
                % MAX_META_OVERALL_SIZE,
                request=req, content_type='text/plain')
    return None


def check_object_creation(req, object_name):
    """
    Check to ensure that everything is alright about an object to be created.

    :param req: HTTP request object
    :param object_name: name of object to be created
    :raises HTTPRequestEntityTooLarge: the object is too large
    :raises HTTPLengthRequered: missing content-length header and not
                                a chunked request
    :raises HTTPBadRequest: missing or bad content-type header, or
                            bad metadata
    """
    if req.content_length and req.content_length > MAX_FILE_SIZE:
        return HTTPRequestEntityTooLarge(body='Your request is too large.',
                                         request=req,
                                         content_type='text/plain')
    if req.content_length is None and \
            req.headers.get('transfer-encoding') != 'chunked':
        return HTTPLengthRequired(request=req)
    if 'X-Copy-From' in req.headers and req.content_length:
        return HTTPBadRequest(body='Copy requests require a zero byte body',
                              request=req, content_type='text/plain')
    if len(object_name) > MAX_OBJECT_NAME_LENGTH:
        return HTTPBadRequest(body='Object name length of %d longer than %d' %
                              (len(object_name), MAX_OBJECT_NAME_LENGTH),
                              request=req, content_type='text/plain')
    if 'Content-Type' not in req.headers:
        return HTTPBadRequest(request=req, content_type='text/plain',
                              body='No content type')
    if not check_utf8(req.headers['Content-Type']):
        return HTTPBadRequest(request=req, body='Invalid Content-Type',
                              content_type='text/plain')
    if 'x-object-manifest' in req.headers:
        value = req.headers['x-object-manifest']
        container = prefix = None
        try:
            container, prefix = value.split('/', 1)
        except ValueError:
            pass
        if not container or not prefix or '?' in value or '&' in value or \
                prefix[0] == '/':
            return HTTPBadRequest(
                request=req,
                body='X-Object-Manifest must in the format container/prefix')
    return check_metadata(req, 'object')


def check_mount(root, drive):
    """
    Verify that the path to the device is a mount point and mounted.  This
    allows us to fast fail on drives that have been unmounted because of
    issues, and also prevents us for accidently filling up the root partition.

    :param root:  base path where the devices are mounted
    :param drive: drive name to be checked
    :returns: True if it is a valid mounted device, False otherwise
    """
    if not (urllib.quote_plus(drive) == drive):
        return False
    path = os.path.join(root, drive)
    return os.path.exists(path) and os.path.ismount(path)


def check_float(string):
    """
    Helper function for checking if a string can be converted to a float.

    :param string: string to be verified as a float
    :returns: True if the string can be converted to a float, False otherwise
    """
    try:
        float(string)
        return True
    except ValueError:
        return False


def check_utf8(string):
    """
    Validate if a string is valid UTF-8 str or unicode and that it
    does not contain any null character.

    :param string: string to be validated
    :returns: True if the string is valid utf-8 str or unicode and
              contains no null characters, False otherwise
    """
    if not string:
        return False
    try:
        if isinstance(string, unicode):
            string.encode('utf-8')
        else:
            string.decode('UTF-8')
        return '\x00' not in string
    # If string is unicode, decode() will raise UnicodeEncodeError
    # So, we should catch both UnicodeDecodeError & UnicodeEncodeError
    except UnicodeError:
        return False
