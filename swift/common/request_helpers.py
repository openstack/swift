# Copyright (c) 2010-2013 OpenStack Foundation
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

"""
Miscellaneous utility functions for use in generating responses.

Why not swift.common.utils, you ask? Because this way we can import things
from swob in here without creating circular imports.
"""

from swift.common.constraints import FORMAT2CONTENT_TYPE
from swift.common.swob import HTTPBadRequest, HTTPNotAcceptable
from swift.common.utils import split_path, validate_device_partition
from urllib import unquote
from swift.common.storage_policy import POLICY_INDEX


def get_param(req, name, default=None):
    """
    Get parameters from an HTTP request ensuring proper handling UTF-8
    encoding.

    :param req: request object
    :param name: parameter name
    :param default: result to return if the parameter is not found
    :returns: HTTP request parameter value
              (as UTF-8 encoded str, not unicode object)
    :raises: HTTPBadRequest if param not valid UTF-8 byte sequence
    """
    value = req.params.get(name, default)
    if value and not isinstance(value, unicode):
        try:
            value.decode('utf8')    # Ensure UTF8ness
        except UnicodeDecodeError:
            raise HTTPBadRequest(
                request=req, content_type='text/plain',
                body='"%s" parameter not valid UTF-8' % name)
    return value


def get_listing_content_type(req):
    """
    Determine the content type to use for an account or container listing
    response.

    :param req: request object
    :returns: content type as a string (e.g. text/plain, application/json)
    :raises: HTTPNotAcceptable if the requested content type is not acceptable
    :raises: HTTPBadRequest if the 'format' query param is provided and
             not valid UTF-8
    """
    query_format = get_param(req, 'format')
    if query_format:
        req.accept = FORMAT2CONTENT_TYPE.get(
            query_format.lower(), FORMAT2CONTENT_TYPE['plain'])
    out_content_type = req.accept.best_match(
        ['text/plain', 'application/json', 'application/xml', 'text/xml'])
    if not out_content_type:
        raise HTTPNotAcceptable(request=req)
    return out_content_type


def get_name_and_placement(request, *args):
    policy_idx = request.headers.get(POLICY_INDEX, '0')
    try:
        policy_idx = int(policy_idx)
    except ValueError:
        results = HTTPBadRequest(
            "Policy index must be numeric (was %s)"
            % policy_idx)
    results = split_and_validate_path(request, *args)
    results.append(policy_idx)
    return results


def split_and_validate_path(request, minsegs=1, maxsegs=None,
                            rest_with_last=False):
    """
    Utility function to split and validate the request path.

    :returns: result of split_path if everything's okay
    :raises: HTTPBadRequest if something's not okay
    """
    try:
        segs = split_path(unquote(request.path),
                          minsegs, maxsegs, rest_with_last)
        validate_device_partition(segs[0], segs[1])
        return segs
    except ValueError as err:
        raise HTTPBadRequest(body=str(err), request=request,
                             content_type='text/plain')


def is_user_meta(server_type, key):
    """
    Tests if a header key starts with and is longer than the user
    metadata prefix for given server type.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: True if the key satisfies the test, False otherwise
    """
    if len(key) <= 8 + len(server_type):
        return False
    return key.lower().startswith(get_user_meta_prefix(server_type))


def is_sys_meta(server_type, key):
    """
    Tests if a header key starts with and is longer than the system
    metadata prefix for given server type.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: True if the key satisfies the test, False otherwise
    """
    if len(key) <= 11 + len(server_type):
        return False
    return key.lower().startswith(get_sys_meta_prefix(server_type))


def is_sys_or_user_meta(server_type, key):
    """
    Tests if a header key starts with and is longer than the user or system
    metadata prefix for given server type.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: True if the key satisfies the test, False otherwise
    """
    return is_user_meta(server_type, key) or is_sys_meta(server_type, key)


def strip_user_meta_prefix(server_type, key):
    """
    Removes the user metadata prefix for a given server type from the start
    of a header key.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: stripped header key
    """
    return key[len(get_user_meta_prefix(server_type)):]


def strip_sys_meta_prefix(server_type, key):
    """
    Removes the system metadata prefix for a given server type from the start
    of a header key.

    :param server_type: type of backend server i.e. [account|container|object]
    :param key: header key
    :returns: stripped header key
    """
    return key[len(get_sys_meta_prefix(server_type)):]


def get_user_meta_prefix(server_type):
    """
    Returns the prefix for user metadata headers for given server type.

    This prefix defines the namespace for headers that will be persisted
    by backend servers.

    :param server_type: type of backend server i.e. [account|container|object]
    :returns: prefix string for server type's user metadata headers
    """
    return 'x-%s-%s-' % (server_type.lower(), 'meta')


def get_sys_meta_prefix(server_type):
    """
    Returns the prefix for system metadata headers for given server type.

    This prefix defines the namespace for headers that will be persisted
    by backend servers.

    :param server_type: type of backend server i.e. [account|container|object]
    :returns: prefix string for server type's system metadata headers
    """
    return 'x-%s-%s-' % (server_type.lower(), 'sysmeta')


def remove_items(headers, condition):
    """
    Removes items from a dict whose keys satisfy
    the given condition.

    :param headers: a dict of headers
    :param condition: a function that will be passed the header key as a
    single argument and should return True if the header is to be removed.
    :returns: a dict, possibly empty, of headers that have been removed
    """
    removed = {}
    keys = filter(condition, headers)
    removed.update((key, headers.pop(key)) for key in keys)
    return removed
