# Copyright (c) 2010-2012 OpenStack Foundation
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

import functools
import os
import time

import six
from six.moves.configparser import ConfigParser, NoSectionError, NoOptionError
from six.moves import urllib

from swift.common import utils, exceptions
from swift.common.swob import HTTPBadRequest, HTTPLengthRequired, \
    HTTPRequestEntityTooLarge, HTTPPreconditionFailed, HTTPNotImplemented, \
    HTTPException

MAX_FILE_SIZE = 5368709122
MAX_META_NAME_LENGTH = 128
MAX_META_VALUE_LENGTH = 256
MAX_META_COUNT = 90
MAX_META_OVERALL_SIZE = 4096
MAX_HEADER_SIZE = 8192
MAX_OBJECT_NAME_LENGTH = 1024
CONTAINER_LISTING_LIMIT = 10000
ACCOUNT_LISTING_LIMIT = 10000
MAX_ACCOUNT_NAME_LENGTH = 256
MAX_CONTAINER_NAME_LENGTH = 256
VALID_API_VERSIONS = ["v1", "v1.0"]
EXTRA_HEADER_COUNT = 0

# If adding an entry to DEFAULT_CONSTRAINTS, note that
# these constraints are automatically published by the
# proxy server in responses to /info requests, with values
# updated by reload_constraints()
DEFAULT_CONSTRAINTS = {
    'max_file_size': MAX_FILE_SIZE,
    'max_meta_name_length': MAX_META_NAME_LENGTH,
    'max_meta_value_length': MAX_META_VALUE_LENGTH,
    'max_meta_count': MAX_META_COUNT,
    'max_meta_overall_size': MAX_META_OVERALL_SIZE,
    'max_header_size': MAX_HEADER_SIZE,
    'max_object_name_length': MAX_OBJECT_NAME_LENGTH,
    'container_listing_limit': CONTAINER_LISTING_LIMIT,
    'account_listing_limit': ACCOUNT_LISTING_LIMIT,
    'max_account_name_length': MAX_ACCOUNT_NAME_LENGTH,
    'max_container_name_length': MAX_CONTAINER_NAME_LENGTH,
    'valid_api_versions': VALID_API_VERSIONS,
    'extra_header_count': EXTRA_HEADER_COUNT,
}

SWIFT_CONSTRAINTS_LOADED = False
OVERRIDE_CONSTRAINTS = {}  # any constraints overridden by SWIFT_CONF_FILE
EFFECTIVE_CONSTRAINTS = {}  # populated by reload_constraints


def reload_constraints():
    """
    Parse SWIFT_CONF_FILE and reset module level global constraint attrs,
    populating OVERRIDE_CONSTRAINTS AND EFFECTIVE_CONSTRAINTS along the way.
    """
    global SWIFT_CONSTRAINTS_LOADED, OVERRIDE_CONSTRAINTS
    SWIFT_CONSTRAINTS_LOADED = False
    OVERRIDE_CONSTRAINTS = {}
    constraints_conf = ConfigParser()
    if constraints_conf.read(utils.SWIFT_CONF_FILE):
        SWIFT_CONSTRAINTS_LOADED = True
        for name in DEFAULT_CONSTRAINTS:
            try:
                value = constraints_conf.get('swift-constraints', name)
            except NoOptionError:
                pass
            except NoSectionError:
                # We are never going to find the section for another option
                break
            else:
                try:
                    value = int(value)
                except ValueError:
                    value = utils.list_from_csv(value)
                OVERRIDE_CONSTRAINTS[name] = value
    for name, default in DEFAULT_CONSTRAINTS.items():
        value = OVERRIDE_CONSTRAINTS.get(name, default)
        EFFECTIVE_CONSTRAINTS[name] = value
        # "globals" in this context is module level globals, always.
        globals()[name.upper()] = value


reload_constraints()


# Maximum slo segments in buffer
MAX_BUFFERED_SLO_SEGMENTS = 10000


#: Query string format= values to their corresponding content-type values
FORMAT2CONTENT_TYPE = {'plain': 'text/plain', 'json': 'application/json',
                       'xml': 'application/xml'}


# By default the maximum number of allowed headers depends on the number of max
# allowed metadata settings plus a default value of 36 for swift internally
# generated headers and regular http headers.  If for some reason this is not
# enough (custom middleware for example) it can be increased with the
# extra_header_count constraint.
MAX_HEADER_COUNT = MAX_META_COUNT + 36 + max(EXTRA_HEADER_COUNT, 0)


def check_metadata(req, target_type):
    """
    Check metadata sent in the request headers.  This should only check
    that the metadata in the request given is valid.  Checks against
    account/container overall metadata should be forwarded on to its
    respective server to be checked.

    :param req: request object
    :param target_type: str: one of: object, container, or account: indicates
                        which type the target storage for the metadata is
    :returns: HTTPBadRequest with bad metadata otherwise None
    """
    target_type = target_type.lower()
    prefix = 'x-%s-meta-' % target_type
    meta_count = 0
    meta_size = 0
    for key, value in req.headers.items():
        if (isinstance(value, six.string_types)
           and len(value) > MAX_HEADER_SIZE):

            return HTTPBadRequest(body='Header value too long: %s' %
                                  key[:MAX_META_NAME_LENGTH],
                                  request=req, content_type='text/plain')
        if not key.lower().startswith(prefix):
            continue
        key = key[len(prefix):]
        if not key:
            return HTTPBadRequest(body='Metadata name cannot be empty',
                                  request=req, content_type='text/plain')
        bad_key = not check_utf8(key)
        bad_value = value and not check_utf8(value)
        if target_type in ('account', 'container') and (bad_key or bad_value):
            return HTTPBadRequest(body='Metadata must be valid UTF-8',
                                  request=req, content_type='text/plain')
        meta_count += 1
        meta_size += len(key) + len(value)
        if len(key) > MAX_META_NAME_LENGTH:
            return HTTPBadRequest(
                body='Metadata name too long: %s%s' % (prefix, key),
                request=req, content_type='text/plain')
        if len(value) > MAX_META_VALUE_LENGTH:
            return HTTPBadRequest(
                body='Metadata value longer than %d: %s%s' % (
                    MAX_META_VALUE_LENGTH, prefix, key),
                request=req, content_type='text/plain')
        if meta_count > MAX_META_COUNT:
            return HTTPBadRequest(
                body='Too many metadata items; max %d' % MAX_META_COUNT,
                request=req, content_type='text/plain')
        if meta_size > MAX_META_OVERALL_SIZE:
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
    :returns: HTTPRequestEntityTooLarge -- the object is too large
    :returns: HTTPLengthRequired -- missing content-length header and not
                                    a chunked request
    :returns: HTTPBadRequest -- missing or bad content-type header, or
                                bad metadata
    :returns: HTTPNotImplemented -- unsupported transfer-encoding header value
    """
    try:
        ml = req.message_length()
    except ValueError as e:
        return HTTPBadRequest(request=req, content_type='text/plain',
                              body=str(e))
    except AttributeError as e:
        return HTTPNotImplemented(request=req, content_type='text/plain',
                                  body=str(e))
    if ml is not None and ml > MAX_FILE_SIZE:
        return HTTPRequestEntityTooLarge(body='Your request is too large.',
                                         request=req,
                                         content_type='text/plain')
    if req.content_length is None and \
            req.headers.get('transfer-encoding') != 'chunked':
        return HTTPLengthRequired(body='Missing Content-Length header.',
                                  request=req,
                                  content_type='text/plain')

    if len(object_name) > MAX_OBJECT_NAME_LENGTH:
        return HTTPBadRequest(body='Object name length of %d longer than %d' %
                              (len(object_name), MAX_OBJECT_NAME_LENGTH),
                              request=req, content_type='text/plain')

    if 'Content-Type' not in req.headers:
        return HTTPBadRequest(request=req, content_type='text/plain',
                              body='No content type')

    try:
        req = check_delete_headers(req)
    except HTTPException as e:
        return HTTPBadRequest(request=req, body=e.body,
                              content_type='text/plain')

    if not check_utf8(req.headers['Content-Type']):
        return HTTPBadRequest(request=req, body='Invalid Content-Type',
                              content_type='text/plain')
    return check_metadata(req, 'object')


def check_dir(root, drive):
    """
    Verify that the path to the device is a directory and is a lesser
    constraint that is enforced when a full mount_check isn't possible
    with, for instance, a VM using loopback or partitions.

    :param root:  base path where the dir is
    :param drive: drive name to be checked
    :returns: True if it is a valid directoy, False otherwise
    """
    return os.path.isdir(os.path.join(root, drive))


def check_mount(root, drive):
    """
    Verify that the path to the device is a mount point and mounted.  This
    allows us to fast fail on drives that have been unmounted because of
    issues, and also prevents us for accidentally filling up the root
    partition.

    :param root:  base path where the devices are mounted
    :param drive: drive name to be checked
    :returns: True if it is a valid mounted device, False otherwise
    """
    if not (urllib.parse.quote_plus(drive) == drive):
        return False
    path = os.path.join(root, drive)
    return utils.ismount(path)


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


def valid_timestamp(request):
    """
    Helper function to extract a timestamp from requests that require one.

    :param request: the swob request object

    :returns: a valid Timestamp instance
    :raises: HTTPBadRequest on missing or invalid X-Timestamp
    """
    try:
        return request.timestamp
    except exceptions.InvalidTimestamp as e:
        raise HTTPBadRequest(body=str(e), request=request,
                             content_type='text/plain')


def check_delete_headers(request):
    """
    Validate if 'x-delete' headers are have correct values
    values should be positive integers and correspond to
    a time in the future.

    :param request: the swob request object

    :returns: HTTPBadRequest in case of invalid values
              or None if values are ok
    """
    if 'x-delete-after' in request.headers:
        try:
            x_delete_after = int(request.headers['x-delete-after'])
        except ValueError:
            raise HTTPBadRequest(request=request,
                                 content_type='text/plain',
                                 body='Non-integer X-Delete-After')
        actual_del_time = time.time() + x_delete_after
        if actual_del_time < time.time():
            raise HTTPBadRequest(request=request,
                                 content_type='text/plain',
                                 body='X-Delete-After in past')
        request.headers['x-delete-at'] = utils.normalize_delete_at_timestamp(
            actual_del_time)

    if 'x-delete-at' in request.headers:
        try:
            x_delete_at = int(utils.normalize_delete_at_timestamp(
                int(request.headers['x-delete-at'])))
        except ValueError:
            raise HTTPBadRequest(request=request, content_type='text/plain',
                                 body='Non-integer X-Delete-At')

        if x_delete_at < time.time():
            raise HTTPBadRequest(request=request, content_type='text/plain',
                                 body='X-Delete-At in past')
    return request


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
        if isinstance(string, six.text_type):
            string.encode('utf-8')
        else:
            decoded = string.decode('UTF-8')
            if decoded.encode('UTF-8') != string:
                return False
            # A UTF-8 string with surrogates in it is invalid.
            if any(0xD800 <= ord(codepoint) <= 0xDFFF
                   for codepoint in decoded):
                return False
        return '\x00' not in string
    # If string is unicode, decode() will raise UnicodeEncodeError
    # So, we should catch both UnicodeDecodeError & UnicodeEncodeError
    except UnicodeError:
        return False


def check_name_format(req, name, target_type):
    """
    Validate that the header contains valid account or container name.

    :param req: HTTP request object
    :param name: header value to validate
    :param target_type: which header is being validated (Account or Container)
    :returns: A properly encoded account name or container name
    :raise: HTTPPreconditionFailed if account header
            is not well formatted.
    """
    if not name:
        raise HTTPPreconditionFailed(
            request=req,
            body='%s name cannot be empty' % target_type)
    if isinstance(name, six.text_type):
        name = name.encode('utf-8')
    if '/' in name:
        raise HTTPPreconditionFailed(
            request=req,
            body='%s name cannot contain slashes' % target_type)
    return name

check_account_format = functools.partial(check_name_format,
                                         target_type='Account')
check_container_format = functools.partial(check_name_format,
                                           target_type='Container')


def valid_api_version(version):
    """ Checks if the requested version is valid.

    Currently Swift only supports "v1" and "v1.0". """
    global VALID_API_VERSIONS
    if not isinstance(VALID_API_VERSIONS, list):
        VALID_API_VERSIONS = [str(VALID_API_VERSIONS)]
    return version in VALID_API_VERSIONS
