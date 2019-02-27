# Copyright (c) 2010-2016 OpenStack Foundation
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

# NOTE: swift_conn
# You'll see swift_conn passed around a few places in this file. This is the
# source bufferedhttp connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

from six.moves.urllib.parse import quote

import os
import time
import json
import functools
import inspect
import itertools
import operator
from copy import deepcopy
from sys import exc_info
from swift import gettext_ as _

from eventlet import sleep
from eventlet.timeout import Timeout
import six

from swift.common.wsgi import make_pre_authed_env, make_pre_authed_request
from swift.common.utils import Timestamp, config_true_value, \
    public, split_path, list_from_csv, GreenthreadSafeIterator, \
    GreenAsyncPile, quorum_size, parse_content_type, \
    document_iters_to_http_response_body, ShardRange
from swift.common.bufferedhttp import http_connect
from swift.common import constraints
from swift.common.exceptions import ChunkReadTimeout, ChunkWriteTimeout, \
    ConnectionTimeout, RangeAlreadyComplete, ShortReadError
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import is_informational, is_success, is_redirection, \
    is_server_error, HTTP_OK, HTTP_PARTIAL_CONTENT, HTTP_MULTIPLE_CHOICES, \
    HTTP_BAD_REQUEST, HTTP_NOT_FOUND, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_INSUFFICIENT_STORAGE, HTTP_UNAUTHORIZED, HTTP_CONTINUE, HTTP_GONE
from swift.common.swob import Request, Response, Range, \
    HTTPException, HTTPRequestedRangeNotSatisfiable, HTTPServiceUnavailable, \
    status_map
from swift.common.request_helpers import strip_sys_meta_prefix, \
    strip_user_meta_prefix, is_user_meta, is_sys_meta, is_sys_or_user_meta, \
    http_response_to_document_iters, is_object_transient_sysmeta, \
    strip_object_transient_sysmeta_prefix
from swift.common.storage_policy import POLICIES


DEFAULT_RECHECK_ACCOUNT_EXISTENCE = 60  # seconds
DEFAULT_RECHECK_CONTAINER_EXISTENCE = 60  # seconds


def update_headers(response, headers):
    """
    Helper function to update headers in the response.

    :param response: swob.Response object
    :param headers: dictionary headers
    """
    if hasattr(headers, 'items'):
        headers = headers.items()
    for name, value in headers:
        if name.lower() == 'etag':
            response.headers[name] = value.replace('"', '')
        elif name.lower() not in (
                'date', 'content-length', 'content-type',
                'connection', 'x-put-timestamp', 'x-delete-after'):
            response.headers[name] = value


def source_key(resp):
    """
    Provide the timestamp of the swift http response as a floating
    point value.  Used as a sort key.

    :param resp: bufferedhttp response object
    """
    return Timestamp(resp.getheader('x-backend-data-timestamp') or
                     resp.getheader('x-backend-timestamp') or
                     resp.getheader('x-put-timestamp') or
                     resp.getheader('x-timestamp') or 0)


def delay_denial(func):
    """
    Decorator to declare which methods should have any swift.authorize call
    delayed. This is so the method can load the Request object up with
    additional information that may be needed by the authorization system.

    :param func: function for which authorization will be delayed
    """
    func.delay_denial = True
    return func


def _prep_headers_to_info(headers, server_type):
    """
    Helper method that iterates once over a dict of headers,
    converting all keys to lower case and separating
    into subsets containing user metadata, system metadata
    and other headers.
    """
    meta = {}
    sysmeta = {}
    other = {}
    for key, val in dict(headers).items():
        lkey = key.lower()
        if is_user_meta(server_type, lkey):
            meta[strip_user_meta_prefix(server_type, lkey)] = val
        elif is_sys_meta(server_type, lkey):
            sysmeta[strip_sys_meta_prefix(server_type, lkey)] = val
        else:
            other[lkey] = val
    return other, meta, sysmeta


def headers_to_account_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of account info based on response headers.
    """
    headers, meta, sysmeta = _prep_headers_to_info(headers, 'account')
    account_info = {
        'status': status_int,
        # 'container_count' anomaly:
        # Previous code sometimes expects an int sometimes a string
        # Current code aligns to str and None, yet translates to int in
        # deprecated functions as needed
        'container_count': headers.get('x-account-container-count'),
        'total_object_count': headers.get('x-account-object-count'),
        'bytes': headers.get('x-account-bytes-used'),
        'storage_policies': {policy.idx: {
            'container_count': int(headers.get(
                'x-account-storage-policy-{}-container-count'.format(
                    policy.name), 0)),
            'object_count': int(headers.get(
                'x-account-storage-policy-{}-object-count'.format(
                    policy.name), 0)),
            'bytes': int(headers.get(
                'x-account-storage-policy-{}-bytes-used'.format(
                    policy.name), 0))}
            for policy in POLICIES
        },
        'meta': meta,
        'sysmeta': sysmeta,
    }
    if is_success(status_int):
        account_info['account_really_exists'] = not config_true_value(
            headers.get('x-backend-fake-account-listing'))
    return account_info


def headers_to_container_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of container info based on response headers.
    """
    headers, meta, sysmeta = _prep_headers_to_info(headers, 'container')
    return {
        'status': status_int,
        'read_acl': headers.get('x-container-read'),
        'write_acl': headers.get('x-container-write'),
        'sync_key': headers.get('x-container-sync-key'),
        'object_count': headers.get('x-container-object-count'),
        'bytes': headers.get('x-container-bytes-used'),
        'versions': headers.get('x-versions-location'),
        'storage_policy': headers.get('x-backend-storage-policy-index', '0'),
        'cors': {
            'allow_origin': meta.get('access-control-allow-origin'),
            'expose_headers': meta.get('access-control-expose-headers'),
            'max_age': meta.get('access-control-max-age')
        },
        'meta': meta,
        'sysmeta': sysmeta,
        'sharding_state': headers.get('x-backend-sharding-state', 'unsharded'),
    }


def headers_to_object_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of object info based on response headers.
    """
    headers, meta, sysmeta = _prep_headers_to_info(headers, 'object')
    transient_sysmeta = {}
    for key, val in headers.items():
        if is_object_transient_sysmeta(key):
            key = strip_object_transient_sysmeta_prefix(key.lower())
            transient_sysmeta[key] = val
    info = {'status': status_int,
            'length': headers.get('content-length'),
            'type': headers.get('content-type'),
            'etag': headers.get('etag'),
            'meta': meta,
            'sysmeta': sysmeta,
            'transient_sysmeta': transient_sysmeta
            }
    return info


def cors_validation(func):
    """
    Decorator to check if the request is a CORS request and if so, if it's
    valid.

    :param func: function to check
    """
    @functools.wraps(func)
    def wrapped(*a, **kw):
        controller = a[0]
        req = a[1]

        # The logic here was interpreted from
        #    http://www.w3.org/TR/cors/#resource-requests

        # Is this a CORS request?
        req_origin = req.headers.get('Origin', None)
        if req_origin:
            # Yes, this is a CORS request so test if the origin is allowed
            container_info = \
                controller.container_info(controller.account_name,
                                          controller.container_name, req)
            cors_info = container_info.get('cors', {})

            # Call through to the decorated method
            resp = func(*a, **kw)

            if controller.app.strict_cors_mode and \
                    not controller.is_origin_allowed(cors_info, req_origin):
                return resp

            # Expose,
            #  - simple response headers,
            #    http://www.w3.org/TR/cors/#simple-response-header
            #  - swift specific: etag, x-timestamp, x-trans-id
            #  - headers provided by the operator in cors_expose_headers
            #  - user metadata headers
            #  - headers provided by the user in
            #    x-container-meta-access-control-expose-headers
            if 'Access-Control-Expose-Headers' not in resp.headers:
                expose_headers = set([
                    'cache-control', 'content-language', 'content-type',
                    'expires', 'last-modified', 'pragma', 'etag',
                    'x-timestamp', 'x-trans-id', 'x-openstack-request-id'])
                expose_headers.update(controller.app.cors_expose_headers)
                for header in resp.headers:
                    if header.startswith('X-Container-Meta') or \
                            header.startswith('X-Object-Meta'):
                        expose_headers.add(header.lower())
                if cors_info.get('expose_headers'):
                    expose_headers = expose_headers.union(
                        [header_line.strip().lower()
                         for header_line in
                         cors_info['expose_headers'].split(' ')
                         if header_line.strip()])
                resp.headers['Access-Control-Expose-Headers'] = \
                    ', '.join(expose_headers)

            # The user agent won't process the response if the Allow-Origin
            # header isn't included
            if 'Access-Control-Allow-Origin' not in resp.headers:
                if cors_info['allow_origin'] and \
                        cors_info['allow_origin'].strip() == '*':
                    resp.headers['Access-Control-Allow-Origin'] = '*'
                else:
                    resp.headers['Access-Control-Allow-Origin'] = req_origin

            return resp
        else:
            # Not a CORS request so make the call as normal
            return func(*a, **kw)

    return wrapped


def get_object_info(env, app, path=None, swift_source=None):
    """
    Get the info structure for an object, based on env and app.
    This is useful to middlewares.

    .. note::

        This call bypasses auth. Success does not imply that the request has
        authorization to the object.
    """
    (version, account, container, obj) = \
        split_path(path or env['PATH_INFO'], 4, 4, True)
    info = _get_object_info(app, env, account, container, obj,
                            swift_source=swift_source)
    if info:
        info = deepcopy(info)
    else:
        info = headers_to_object_info({}, 0)

    for field in ('length',):
        if info.get(field) is None:
            info[field] = 0
        else:
            info[field] = int(info[field])

    return info


def get_container_info(env, app, swift_source=None):
    """
    Get the info structure for a container, based on env and app.
    This is useful to middlewares.

    .. note::

        This call bypasses auth. Success does not imply that the request has
        authorization to the container.
    """
    (version, account, container, unused) = \
        split_path(env['PATH_INFO'], 3, 4, True)

    # Check in environment cache and in memcache (in that order)
    info = _get_info_from_caches(app, env, account, container)

    if not info:
        # Cache miss; go HEAD the container and populate the caches
        env.setdefault('swift.infocache', {})
        # Before checking the container, make sure the account exists.
        #
        # If it is an autocreateable account, just assume it exists; don't
        # HEAD the account, as a GET or HEAD response for an autocreateable
        # account is successful whether the account actually has .db files
        # on disk or not.
        is_autocreate_account = account.startswith(
            getattr(app, 'auto_create_account_prefix', '.'))
        if not is_autocreate_account:
            account_info = get_account_info(env, app, swift_source)
            if not account_info or not is_success(account_info['status']):
                return headers_to_container_info({}, 0)

        req = _prepare_pre_auth_info_request(
            env, ("/%s/%s/%s" % (version, account, container)),
            (swift_source or 'GET_CONTAINER_INFO'))
        resp = req.get_response(app)
        # Check in infocache to see if the proxy (or anyone else) already
        # populated the cache for us. If they did, just use what's there.
        #
        # See similar comment in get_account_info() for justification.
        info = _get_info_from_infocache(env, account, container)
        if info is None:
            info = set_info_cache(app, env, account, container, resp)

    if info:
        info = deepcopy(info)  # avoid mutating what's in swift.infocache
    else:
        info = headers_to_container_info({}, 0)

    # Old data format in memcache immediately after a Swift upgrade; clean
    # it up so consumers of get_container_info() aren't exposed to it.
    if 'object_count' not in info and 'container_size' in info:
        info['object_count'] = info.pop('container_size')

    for field in ('storage_policy', 'bytes', 'object_count'):
        if info.get(field) is None:
            info[field] = 0
        else:
            info[field] = int(info[field])

    if info.get('sharding_state') is None:
        info['sharding_state'] = 'unsharded'

    return info


def get_account_info(env, app, swift_source=None):
    """
    Get the info structure for an account, based on env and app.
    This is useful to middlewares.

    .. note::

        This call bypasses auth. Success does not imply that the request has
        authorization to the account.

    :raises ValueError: when path doesn't contain an account
    """
    (version, account, _junk, _junk) = \
        split_path(env['PATH_INFO'], 2, 4, True)

    # Check in environment cache and in memcache (in that order)
    info = _get_info_from_caches(app, env, account)

    # Cache miss; go HEAD the account and populate the caches
    if not info:
        env.setdefault('swift.infocache', {})
        req = _prepare_pre_auth_info_request(
            env, "/%s/%s" % (version, account),
            (swift_source or 'GET_ACCOUNT_INFO'))
        resp = req.get_response(app)
        # Check in infocache to see if the proxy (or anyone else) already
        # populated the cache for us. If they did, just use what's there.
        #
        # The point of this is to avoid setting the value in memcached
        # twice. Otherwise, we're needlessly sending requests across the
        # network.
        #
        # If the info didn't make it into the cache, we'll compute it from
        # the response and populate the cache ourselves.
        #
        # Note that this is taking "exists in infocache" to imply "exists in
        # memcache". That's because we're trying to avoid superfluous
        # network traffic, and checking in memcache prior to setting in
        # memcache would defeat the purpose.
        info = _get_info_from_infocache(env, account)
        if info is None:
            info = set_info_cache(app, env, account, None, resp)

    if info:
        info = info.copy()  # avoid mutating what's in swift.infocache
    else:
        info = headers_to_account_info({}, 0)

    for field in ('container_count', 'bytes', 'total_object_count'):
        if info.get(field) is None:
            info[field] = 0
        else:
            info[field] = int(info[field])

    return info


def get_cache_key(account, container=None, obj=None):
    """
    Get the keys for both memcache and env['swift.infocache'] (cache_key)
    where info about accounts, containers, and objects is cached

    :param account: The name of the account
    :param container: The name of the container (or None if account)
    :param obj: The name of the object (or None if account or container)
    :returns: a string cache_key
    """

    if obj:
        if not (account and container):
            raise ValueError('Object cache key requires account and container')
        cache_key = 'object/%s/%s/%s' % (account, container, obj)
    elif container:
        if not account:
            raise ValueError('Container cache key requires account')
        cache_key = 'container/%s/%s' % (account, container)
    else:
        cache_key = 'account/%s' % account
    # Use a unique environment cache key per account and one container.
    # This allows caching both account and container and ensures that when we
    # copy this env to form a new request, it won't accidentally reuse the
    # old container or account info
    return cache_key


def set_info_cache(app, env, account, container, resp):
    """
    Cache info in both memcache and env.

    :param  app: the application object
    :param  account: the unquoted account name
    :param  container: the unquoted container name or None
    :param  resp: the response received or None if info cache should be cleared

    :returns: the info that was placed into the cache, or None if the
              request status was not in (404, 410, 2xx).
    """
    infocache = env.setdefault('swift.infocache', {})

    cache_time = None
    if container and resp:
        cache_time = int(resp.headers.get(
            'X-Backend-Recheck-Container-Existence',
            DEFAULT_RECHECK_CONTAINER_EXISTENCE))
    elif resp:
        cache_time = int(resp.headers.get(
            'X-Backend-Recheck-Account-Existence',
            DEFAULT_RECHECK_ACCOUNT_EXISTENCE))
    cache_key = get_cache_key(account, container)

    if resp:
        if resp.status_int in (HTTP_NOT_FOUND, HTTP_GONE):
            cache_time *= 0.1
        elif not is_success(resp.status_int):
            cache_time = None

    # Next actually set both memcache and the env cache
    memcache = getattr(app, 'memcache', None) or env.get('swift.cache')
    if cache_time is None:
        infocache.pop(cache_key, None)
        if memcache:
            memcache.delete(cache_key)
        return

    if container:
        info = headers_to_container_info(resp.headers, resp.status_int)
    else:
        info = headers_to_account_info(resp.headers, resp.status_int)
    if memcache:
        memcache.set(cache_key, info, time=cache_time)
    infocache[cache_key] = info
    return info


def set_object_info_cache(app, env, account, container, obj, resp):
    """
    Cache object info in the WSGI environment, but not in memcache. Caching
    in memcache would lead to cache pressure and mass evictions due to the
    large number of objects in a typical Swift cluster. This is a
    per-request cache only.

    :param  app: the application object
    :param env: the environment used by the current request
    :param  account: the unquoted account name
    :param  container: the unquoted container name
    :param  obj: the unquoted object name
    :param  resp: a GET or HEAD response received from an object server, or
              None if info cache should be cleared
    :returns: the object info
    """

    cache_key = get_cache_key(account, container, obj)

    if 'swift.infocache' in env and not resp:
        env['swift.infocache'].pop(cache_key, None)
        return

    info = headers_to_object_info(resp.headers, resp.status_int)
    env.setdefault('swift.infocache', {})[cache_key] = info
    return info


def clear_info_cache(app, env, account, container=None):
    """
    Clear the cached info in both memcache and env

    :param  app: the application object
    :param  env: the WSGI environment
    :param  account: the account name
    :param  container: the containr name or None if setting info for containers
    """
    set_info_cache(app, env, account, container, None)


def _get_info_from_infocache(env, account, container=None):
    """
    Get cached account or container information from request-environment
    cache (swift.infocache).

    :param  env: the environment used by the current request
    :param  account: the account name
    :param  container: the container name

    :returns: a dictionary of cached info on cache hit, None on miss
    """
    cache_key = get_cache_key(account, container)
    if 'swift.infocache' in env and cache_key in env['swift.infocache']:
        return env['swift.infocache'][cache_key]
    return None


def _get_info_from_memcache(app, env, account, container=None):
    """
    Get cached account or container information from memcache

    :param  app: the application object
    :param  env: the environment used by the current request
    :param  account: the account name
    :param  container: the container name

    :returns: a dictionary of cached info on cache hit, None on miss. Also
      returns None if memcache is not in use.
    """
    cache_key = get_cache_key(account, container)
    memcache = getattr(app, 'memcache', None) or env.get('swift.cache')
    if memcache:
        info = memcache.get(cache_key)
        if info and six.PY2:
            # Get back to native strings
            for key in info:
                if isinstance(info[key], six.text_type):
                    info[key] = info[key].encode("utf-8")
                elif isinstance(info[key], dict):
                    for subkey, value in info[key].items():
                        if isinstance(value, six.text_type):
                            info[key][subkey] = value.encode("utf-8")
        if info:
            env.setdefault('swift.infocache', {})[cache_key] = info
        return info
    return None


def _get_info_from_caches(app, env, account, container=None):
    """
    Get the cached info from env or memcache (if used) in that order.
    Used for both account and container info.

    :param  app: the application object
    :param  env: the environment used by the current request
    :returns: the cached info or None if not cached
    """

    info = _get_info_from_infocache(env, account, container)
    if info is None:
        info = _get_info_from_memcache(app, env, account, container)
    return info


def _prepare_pre_auth_info_request(env, path, swift_source):
    """
    Prepares a pre authed request to obtain info using a HEAD.

    :param env: the environment used by the current request
    :param path: The unquoted request path
    :param swift_source: value for swift.source in WSGI environment
    :returns: the pre authed request
    """
    # Set the env for the pre_authed call without a query string
    newenv = make_pre_authed_env(env, 'HEAD', path, agent='Swift',
                                 query_string='', swift_source=swift_source)
    # This is a sub request for container metadata- drop the Origin header from
    # the request so the it is not treated as a CORS request.
    newenv.pop('HTTP_ORIGIN', None)

    # ACLs are only shown to account owners, so let's make sure this request
    # looks like it came from the account owner.
    newenv['swift_owner'] = True

    # Note that Request.blank expects quoted path
    return Request.blank(quote(path), environ=newenv)


def get_info(app, env, account, container=None, swift_source=None):
    """
    Get info about accounts or containers

    Note: This call bypasses auth. Success does not imply that the
          request has authorization to the info.

    :param app: the application object
    :param env: the environment used by the current request
    :param account: The unquoted name of the account
    :param container: The unquoted name of the container (or None if account)
    :param swift_source: swift source logged for any subrequests made while
                         retrieving the account or container info
    :returns: information about the specified entity in a dictionary. See
      get_account_info and get_container_info for details on what's in the
      dictionary.
    """
    env.setdefault('swift.infocache', {})

    if container:
        path = '/v1/%s/%s' % (account, container)
        path_env = env.copy()
        path_env['PATH_INFO'] = path
        return get_container_info(path_env, app, swift_source=swift_source)
    else:
        # account info
        path = '/v1/%s' % (account,)
        path_env = env.copy()
        path_env['PATH_INFO'] = path
        return get_account_info(path_env, app, swift_source=swift_source)


def _get_object_info(app, env, account, container, obj, swift_source=None):
    """
    Get the info about object

    Note: This call bypasses auth. Success does not imply that the
          request has authorization to the info.

    :param app: the application object
    :param env: the environment used by the current request
    :param account: The unquoted name of the account
    :param container: The unquoted name of the container
    :param obj: The unquoted name of the object
    :returns: the cached info or None if cannot be retrieved
    """
    cache_key = get_cache_key(account, container, obj)
    info = env.get('swift.infocache', {}).get(cache_key)
    if info:
        return info
    # Not in cache, let's try the object servers
    path = '/v1/%s/%s/%s' % (account, container, obj)
    req = _prepare_pre_auth_info_request(env, path, swift_source)
    resp = req.get_response(app)
    # Unlike get_account_info() and get_container_info(), we don't save
    # things in memcache, so we can store the info without network traffic,
    # *and* the proxy doesn't cache object info for us, so there's no chance
    # that the object info would be in the environment. Thus, we just
    # compute the object info based on the response and stash it in
    # swift.infocache.
    info = set_object_info_cache(app, env, account, container, obj, resp)
    return info


def close_swift_conn(src):
    """
    Force close the http connection to the backend.

    :param src: the response from the backend
    """
    try:
        # Since the backends set "Connection: close" in their response
        # headers, the response object (src) is solely responsible for the
        # socket. The connection object (src.swift_conn) has no references
        # to the socket, so calling its close() method does nothing, and
        # therefore we don't do it.
        #
        # Also, since calling the response's close() method might not
        # close the underlying socket but only decrement some
        # reference-counter, we have a special method here that really,
        # really kills the underlying socket with a close() syscall.
        src.nuke_from_orbit()  # it's the only way to be sure
    except Exception:
        pass


def bytes_to_skip(record_size, range_start):
    """
    Assume an object is composed of N records, where the first N-1 are all
    the same size and the last is at most that large, but may be smaller.

    When a range request is made, it might start with a partial record. This
    must be discarded, lest the consumer get bad data. This is particularly
    true of suffix-byte-range requests, e.g. "Range: bytes=-12345" where the
    size of the object is unknown at the time the request is made.

    This function computes the number of bytes that must be discarded to
    ensure only whole records are yielded. Erasure-code decoding needs this.

    This function could have been inlined, but it took enough tries to get
    right that some targeted unit tests were desirable, hence its extraction.
    """
    return (record_size - (range_start % record_size)) % record_size


class ByteCountEnforcer(object):
    """
    Enforces that successive calls to file_like.read() give at least
    <nbytes> bytes before exhaustion.

    If file_like fails to do so, ShortReadError is raised.

    If more than <nbytes> bytes are read, we don't care.
    """

    def __init__(self, file_like, nbytes):
        """
        :param file_like: file-like object
        :param nbytes: number of bytes expected, or None if length is unknown.
        """
        self.file_like = file_like
        self.nbytes = self.bytes_left = nbytes

    def read(self, amt=None):
        chunk = self.file_like.read(amt)
        if self.bytes_left is None:
            return chunk
        elif len(chunk) == 0 and self.bytes_left > 0:
            raise ShortReadError(
                "Too few bytes; read %d, expecting %d" % (
                    self.nbytes - self.bytes_left, self.nbytes))
        else:
            self.bytes_left -= len(chunk)
            return chunk


class ResumingGetter(object):
    def __init__(self, app, req, server_type, node_iter, partition, path,
                 backend_headers, concurrency=1, client_chunk_size=None,
                 newest=None, header_provider=None):
        self.app = app
        self.node_iter = node_iter
        self.server_type = server_type
        self.partition = partition
        self.path = path
        self.backend_headers = backend_headers
        self.client_chunk_size = client_chunk_size
        self.skip_bytes = 0
        self.bytes_used_from_backend = 0
        self.used_nodes = []
        self.used_source_etag = ''
        self.concurrency = concurrency
        self.node = None
        self.header_provider = header_provider
        self.latest_404_timestamp = Timestamp(0)

        # stuff from request
        self.req_method = req.method
        self.req_path = req.path
        self.req_query_string = req.query_string
        if newest is None:
            self.newest = config_true_value(req.headers.get('x-newest', 'f'))
        else:
            self.newest = newest

        # populated when finding source
        self.statuses = []
        self.reasons = []
        self.bodies = []
        self.source_headers = []
        self.sources = []

        # populated from response headers
        self.start_byte = self.end_byte = self.length = None

    def fast_forward(self, num_bytes):
        """
        Will skip num_bytes into the current ranges.

        :params num_bytes: the number of bytes that have already been read on
                           this request. This will change the Range header
                           so that the next req will start where it left off.

        :raises HTTPRequestedRangeNotSatisfiable: if begin + num_bytes
                                                  > end of range + 1
        :raises RangeAlreadyComplete: if begin + num_bytes == end of range + 1
        """
        try:
            req_range = Range(self.backend_headers.get('Range'))
        except ValueError:
            req_range = None

        if req_range:
            begin, end = req_range.ranges[0]
            if begin is None:
                # this is a -50 range req (last 50 bytes of file)
                end -= num_bytes
                if end == 0:
                    # we sent out exactly the first range's worth of bytes, so
                    # we're done with it
                    raise RangeAlreadyComplete()

                if end < 0:
                    raise HTTPRequestedRangeNotSatisfiable()

            else:
                begin += num_bytes
                if end is not None and begin == end + 1:
                    # we sent out exactly the first range's worth of bytes, so
                    # we're done with it
                    raise RangeAlreadyComplete()

                if end is not None and begin > end:
                    raise HTTPRequestedRangeNotSatisfiable()

            req_range.ranges = [(begin, end)] + req_range.ranges[1:]
            self.backend_headers['Range'] = str(req_range)
        else:
            self.backend_headers['Range'] = 'bytes=%d-' % num_bytes

        # Reset so if we need to do this more than once, we don't double-up
        self.bytes_used_from_backend = 0

    def pop_range(self):
        """
        Remove the first byterange from our Range header.

        This is used after a byterange has been completely sent to the
        client; this way, should we need to resume the download from another
        object server, we do not re-fetch byteranges that the client already
        has.

        If we have no Range header, this is a no-op.
        """
        if 'Range' in self.backend_headers:
            try:
                req_range = Range(self.backend_headers['Range'])
            except ValueError:
                # there's a Range header, but it's garbage, so get rid of it
                self.backend_headers.pop('Range')
                return
            begin, end = req_range.ranges.pop(0)
            if len(req_range.ranges) > 0:
                self.backend_headers['Range'] = str(req_range)
            else:
                self.backend_headers.pop('Range')

    def learn_size_from_content_range(self, start, end, length):
        """
        If client_chunk_size is set, makes sure we yield things starting on
        chunk boundaries based on the Content-Range header in the response.

        Sets our Range header's first byterange to the value learned from
        the Content-Range header in the response; if we were given a
        fully-specified range (e.g. "bytes=123-456"), this is a no-op.

        If we were given a half-specified range (e.g. "bytes=123-" or
        "bytes=-456"), then this changes the Range header to a
        semantically-equivalent one *and* it lets us resume on a proper
        boundary instead of just in the middle of a piece somewhere.
        """
        if length == 0:
            return

        if self.client_chunk_size:
            self.skip_bytes = bytes_to_skip(self.client_chunk_size, start)

        if 'Range' in self.backend_headers:
            try:
                req_range = Range(self.backend_headers['Range'])
                new_ranges = [(start, end)] + req_range.ranges[1:]
            except ValueError:
                new_ranges = [(start, end)]
        else:
            new_ranges = [(start, end)]

        self.backend_headers['Range'] = (
            "bytes=" + (",".join("%s-%s" % (s if s is not None else '',
                                            e if e is not None else '')
                                 for s, e in new_ranges)))

    def is_good_source(self, src):
        """
        Indicates whether or not the request made to the backend found
        what it was looking for.

        :param src: the response from the backend
        :returns: True if found, False if not
        """
        if self.server_type == 'Object' and src.status == 416:
            return True
        return is_success(src.status) or is_redirection(src.status)

    def response_parts_iter(self, req):
        source, node = self._get_source_and_node()
        it = None
        if source:
            it = self._get_response_parts_iter(req, node, source)
        return it

    def _get_response_parts_iter(self, req, node, source):
        # Someday we can replace this [mess] with python 3's "nonlocal"
        source = [source]
        node = [node]

        try:
            client_chunk_size = self.client_chunk_size
            node_timeout = self.app.node_timeout
            if self.server_type == 'Object':
                node_timeout = self.app.recoverable_node_timeout

            # This is safe; it sets up a generator but does not call next()
            # on it, so no IO is performed.
            parts_iter = [
                http_response_to_document_iters(
                    source[0], read_chunk_size=self.app.object_chunk_size)]

            def get_next_doc_part():
                while True:
                    try:
                        # This call to next() performs IO when we have a
                        # multipart/byteranges response; it reads the MIME
                        # boundary and part headers.
                        #
                        # If we don't have a multipart/byteranges response,
                        # but just a 200 or a single-range 206, then this
                        # performs no IO, and either just returns source or
                        # raises StopIteration.
                        with ChunkReadTimeout(node_timeout):
                            # if StopIteration is raised, it escapes and is
                            # handled elsewhere
                            start_byte, end_byte, length, headers, part = next(
                                parts_iter[0])
                        return (start_byte, end_byte, length, headers, part)
                    except ChunkReadTimeout:
                        new_source, new_node = self._get_source_and_node()
                        if new_source:
                            self.app.error_occurred(
                                node[0], _('Trying to read object during '
                                           'GET (retrying)'))
                            # Close-out the connection as best as possible.
                            if getattr(source[0], 'swift_conn', None):
                                close_swift_conn(source[0])
                            source[0] = new_source
                            node[0] = new_node
                            # This is safe; it sets up a generator but does
                            # not call next() on it, so no IO is performed.
                            parts_iter[0] = http_response_to_document_iters(
                                new_source,
                                read_chunk_size=self.app.object_chunk_size)
                        else:
                            raise StopIteration()

            def iter_bytes_from_response_part(part_file, nbytes):
                nchunks = 0
                buf = b''
                part_file = ByteCountEnforcer(part_file, nbytes)
                while True:
                    try:
                        with ChunkReadTimeout(node_timeout):
                            chunk = part_file.read(self.app.object_chunk_size)
                            nchunks += 1
                            # NB: this append must be *inside* the context
                            # manager for test.unit.SlowBody to do its thing
                            buf += chunk
                            if nbytes is not None:
                                nbytes -= len(chunk)
                    except (ChunkReadTimeout, ShortReadError):
                        exc_type, exc_value, exc_traceback = exc_info()
                        if self.newest or self.server_type != 'Object':
                            raise
                        try:
                            self.fast_forward(self.bytes_used_from_backend)
                        except (HTTPException, ValueError):
                            six.reraise(exc_type, exc_value, exc_traceback)
                        except RangeAlreadyComplete:
                            break
                        buf = b''
                        new_source, new_node = self._get_source_and_node()
                        if new_source:
                            self.app.error_occurred(
                                node[0], _('Trying to read object during '
                                           'GET (retrying)'))
                            # Close-out the connection as best as possible.
                            if getattr(source[0], 'swift_conn', None):
                                close_swift_conn(source[0])
                            source[0] = new_source
                            node[0] = new_node
                            # This is safe; it just sets up a generator but
                            # does not call next() on it, so no IO is
                            # performed.
                            parts_iter[0] = http_response_to_document_iters(
                                new_source,
                                read_chunk_size=self.app.object_chunk_size)

                            try:
                                _junk, _junk, _junk, _junk, part_file = \
                                    get_next_doc_part()
                            except StopIteration:
                                # Tried to find a new node from which to
                                # finish the GET, but failed. There's
                                # nothing more we can do here.
                                six.reraise(exc_type, exc_value, exc_traceback)
                            part_file = ByteCountEnforcer(part_file, nbytes)
                        else:
                            six.reraise(exc_type, exc_value, exc_traceback)
                    else:
                        if buf and self.skip_bytes:
                            if self.skip_bytes < len(buf):
                                buf = buf[self.skip_bytes:]
                                self.bytes_used_from_backend += self.skip_bytes
                                self.skip_bytes = 0
                            else:
                                self.skip_bytes -= len(buf)
                                self.bytes_used_from_backend += len(buf)
                                buf = b''

                        if not chunk:
                            if buf:
                                with ChunkWriteTimeout(
                                        self.app.client_timeout):
                                    self.bytes_used_from_backend += len(buf)
                                    yield buf
                                buf = b''
                            break

                        if client_chunk_size is not None:
                            while len(buf) >= client_chunk_size:
                                client_chunk = buf[:client_chunk_size]
                                buf = buf[client_chunk_size:]
                                with ChunkWriteTimeout(
                                        self.app.client_timeout):
                                    self.bytes_used_from_backend += \
                                        len(client_chunk)
                                    yield client_chunk
                        else:
                            with ChunkWriteTimeout(self.app.client_timeout):
                                self.bytes_used_from_backend += len(buf)
                                yield buf
                            buf = b''

                        # This is for fairness; if the network is outpacing
                        # the CPU, we'll always be able to read and write
                        # data without encountering an EWOULDBLOCK, and so
                        # eventlet will not switch greenthreads on its own.
                        # We do it manually so that clients don't starve.
                        #
                        # The number 5 here was chosen by making stuff up.
                        # It's not every single chunk, but it's not too big
                        # either, so it seemed like it would probably be an
                        # okay choice.
                        #
                        # Note that we may trampoline to other greenthreads
                        # more often than once every 5 chunks, depending on
                        # how blocking our network IO is; the explicit sleep
                        # here simply provides a lower bound on the rate of
                        # trampolining.
                        if nchunks % 5 == 0:
                            sleep()

            part_iter = None
            try:
                while True:
                    start_byte, end_byte, length, headers, part = \
                        get_next_doc_part()
                    # note: learn_size_from_content_range() sets
                    # self.skip_bytes
                    self.learn_size_from_content_range(
                        start_byte, end_byte, length)
                    self.bytes_used_from_backend = 0
                    # not length; that refers to the whole object, so is the
                    # wrong value to use for GET-range responses
                    byte_count = ((end_byte - start_byte + 1) - self.skip_bytes
                                  if (end_byte is not None
                                      and start_byte is not None)
                                  else None)
                    part_iter = iter_bytes_from_response_part(part, byte_count)
                    yield {'start_byte': start_byte, 'end_byte': end_byte,
                           'entity_length': length, 'headers': headers,
                           'part_iter': part_iter}
                    self.pop_range()
            except StopIteration:
                req.environ['swift.non_client_disconnect'] = True
            finally:
                if part_iter:
                    part_iter.close()

        except ChunkReadTimeout:
            self.app.exception_occurred(node[0], _('Object'),
                                        _('Trying to read during GET'))
            raise
        except ChunkWriteTimeout:
            self.app.logger.warning(
                _('Client did not read from proxy within %ss') %
                self.app.client_timeout)
            self.app.logger.increment('client_timeouts')
        except GeneratorExit:
            warn = True
            req_range = self.backend_headers['Range']
            if req_range:
                req_range = Range(req_range)
                if len(req_range.ranges) == 1:
                    begin, end = req_range.ranges[0]
                    if end is not None and begin is not None:
                        if end - begin + 1 == self.bytes_used_from_backend:
                            warn = False
            if not req.environ.get('swift.non_client_disconnect') and warn:
                self.app.logger.warning(_('Client disconnected on read'))
            raise
        except Exception:
            self.app.logger.exception(_('Trying to send to client'))
            raise
        finally:
            # Close-out the connection as best as possible.
            if getattr(source[0], 'swift_conn', None):
                close_swift_conn(source[0])

    @property
    def last_status(self):
        if self.statuses:
            return self.statuses[-1]
        else:
            return None

    @property
    def last_headers(self):
        if self.source_headers:
            return HeaderKeyDict(self.source_headers[-1])
        else:
            return None

    def _make_node_request(self, node, node_timeout, logger_thread_locals):
        self.app.logger.thread_locals = logger_thread_locals
        if node in self.used_nodes:
            return False
        req_headers = dict(self.backend_headers)
        # a request may be specialised with specific backend headers
        if self.header_provider:
            req_headers.update(self.header_provider())
        start_node_timing = time.time()
        try:
            with ConnectionTimeout(self.app.conn_timeout):
                conn = http_connect(
                    node['ip'], node['port'], node['device'],
                    self.partition, self.req_method, self.path,
                    headers=req_headers,
                    query_string=self.req_query_string)
            self.app.set_node_timing(node, time.time() - start_node_timing)

            with Timeout(node_timeout):
                possible_source = conn.getresponse()
                # See NOTE: swift_conn at top of file about this.
                possible_source.swift_conn = conn
        except (Exception, Timeout):
            self.app.exception_occurred(
                node, self.server_type,
                _('Trying to %(method)s %(path)s') %
                {'method': self.req_method, 'path': self.req_path})
            return False
        if self.is_good_source(possible_source):
            # 404 if we know we don't have a synced copy
            if not float(possible_source.getheader('X-PUT-Timestamp', 1)):
                self.statuses.append(HTTP_NOT_FOUND)
                self.reasons.append('')
                self.bodies.append('')
                self.source_headers.append([])
                close_swift_conn(possible_source)
            else:
                src_headers = dict(
                    (k.lower(), v) for k, v in
                    possible_source.getheaders())
                if self.used_source_etag and \
                    self.used_source_etag != src_headers.get(
                        'x-object-sysmeta-ec-etag',
                        src_headers.get('etag', '')).strip('"'):
                    self.statuses.append(HTTP_NOT_FOUND)
                    self.reasons.append('')
                    self.bodies.append('')
                    self.source_headers.append([])
                    return False

                # a possible source should only be added as a valid source
                # if its timestamp is newer than previously found tombstones
                ps_timestamp = Timestamp(
                    src_headers.get('x-backend-data-timestamp') or
                    src_headers.get('x-backend-timestamp') or
                    src_headers.get('x-put-timestamp') or
                    src_headers.get('x-timestamp') or 0)
                if ps_timestamp >= self.latest_404_timestamp:
                    self.statuses.append(possible_source.status)
                    self.reasons.append(possible_source.reason)
                    self.bodies.append(None)
                    self.source_headers.append(possible_source.getheaders())
                    self.sources.append((possible_source, node))
                    if not self.newest:  # one good source is enough
                        return True
        else:

            self.statuses.append(possible_source.status)
            self.reasons.append(possible_source.reason)
            self.bodies.append(possible_source.read())
            self.source_headers.append(possible_source.getheaders())

            # if 404, record the timestamp. If a good source shows up, its
            # timestamp will be compared to the latest 404.
            # For now checking only on objects, but future work could include
            # the same check for account and containers. See lp 1560574.
            if self.server_type == 'Object' and \
                    possible_source.status == HTTP_NOT_FOUND:
                hdrs = HeaderKeyDict(possible_source.getheaders())
                ts = Timestamp(hdrs.get('X-Backend-Timestamp', 0))
                if ts > self.latest_404_timestamp:
                    self.latest_404_timestamp = ts
            if possible_source.status == HTTP_INSUFFICIENT_STORAGE:
                self.app.error_limit(node, _('ERROR Insufficient Storage'))
            elif is_server_error(possible_source.status):
                self.app.error_occurred(
                    node, _('ERROR %(status)d %(body)s '
                            'From %(type)s Server') %
                    {'status': possible_source.status,
                     'body': self.bodies[-1][:1024],
                     'type': self.server_type})
        return False

    def _get_source_and_node(self):
        self.statuses = []
        self.reasons = []
        self.bodies = []
        self.source_headers = []
        self.sources = []

        nodes = GreenthreadSafeIterator(self.node_iter)

        node_timeout = self.app.node_timeout
        if self.server_type == 'Object' and not self.newest:
            node_timeout = self.app.recoverable_node_timeout

        pile = GreenAsyncPile(self.concurrency)

        for node in nodes:
            pile.spawn(self._make_node_request, node, node_timeout,
                       self.app.logger.thread_locals)
            _timeout = self.app.concurrency_timeout \
                if pile.inflight < self.concurrency else None
            if pile.waitfirst(_timeout):
                break
        else:
            # ran out of nodes, see if any stragglers will finish
            any(pile)

        # this helps weed out any sucess status that were found before a 404
        # and added to the list in the case of x-newest.
        if self.sources:
            self.sources = [s for s in self.sources
                            if source_key(s[0]) >= self.latest_404_timestamp]

        if self.sources:
            self.sources.sort(key=lambda s: source_key(s[0]))
            source, node = self.sources.pop()
            for src, _junk in self.sources:
                close_swift_conn(src)
            self.used_nodes.append(node)
            src_headers = dict(
                (k.lower(), v) for k, v in
                source.getheaders())

            # Save off the source etag so that, if we lose the connection
            # and have to resume from a different node, we can be sure that
            # we have the same object (replication) or a fragment archive
            # from the same object (EC). Otherwise, if the cluster has two
            # versions of the same object, we might end up switching between
            # old and new mid-stream and giving garbage to the client.
            self.used_source_etag = src_headers.get(
                'x-object-sysmeta-ec-etag',
                src_headers.get('etag', '')).strip('"')
            self.node = node
            return source, node
        return None, None


class GetOrHeadHandler(ResumingGetter):
    def _make_app_iter(self, req, node, source):
        """
        Returns an iterator over the contents of the source (via its read
        func).  There is also quite a bit of cleanup to ensure garbage
        collection works and the underlying socket of the source is closed.

        :param req: incoming request object
        :param source: The httplib.Response object this iterator should read
                       from.
        :param node: The node the source is reading from, for logging purposes.
        """

        ct = source.getheader('Content-Type')
        if ct:
            content_type, content_type_attrs = parse_content_type(ct)
            is_multipart = content_type == 'multipart/byteranges'
        else:
            is_multipart = False

        boundary = "dontcare"
        if is_multipart:
            # we need some MIME boundary; fortunately, the object server has
            # furnished one for us, so we'll just re-use it
            boundary = dict(content_type_attrs)["boundary"]

        parts_iter = self._get_response_parts_iter(req, node, source)

        def add_content_type(response_part):
            response_part["content_type"] = \
                HeaderKeyDict(response_part["headers"]).get("Content-Type")
            return response_part

        return document_iters_to_http_response_body(
            (add_content_type(pi) for pi in parts_iter),
            boundary, is_multipart, self.app.logger)

    def get_working_response(self, req):
        source, node = self._get_source_and_node()
        res = None
        if source:
            res = Response(request=req)
            res.status = source.status
            update_headers(res, source.getheaders())
            if req.method == 'GET' and \
                    source.status in (HTTP_OK, HTTP_PARTIAL_CONTENT):
                res.app_iter = self._make_app_iter(req, node, source)
                # See NOTE: swift_conn at top of file about this.
                res.swift_conn = source.swift_conn
            if not res.environ:
                res.environ = {}
            res.environ['swift_x_timestamp'] = \
                source.getheader('x-timestamp')
            res.accept_ranges = 'bytes'
            res.content_length = source.getheader('Content-Length')
            if source.getheader('Content-Type'):
                res.charset = None
                res.content_type = source.getheader('Content-Type')
        return res


class NodeIter(object):
    """
    Yields nodes for a ring partition, skipping over error
    limited nodes and stopping at the configurable number of nodes. If a
    node yielded subsequently gets error limited, an extra node will be
    yielded to take its place.

    Note that if you're going to iterate over this concurrently from
    multiple greenthreads, you'll want to use a
    swift.common.utils.GreenthreadSafeIterator to serialize access.
    Otherwise, you may get ValueErrors from concurrent access. (You also
    may not, depending on how logging is configured, the vagaries of
    socket IO and eventlet, and the phase of the moon.)

    :param app: a proxy app
    :param ring: ring to get yield nodes from
    :param partition: ring partition to yield nodes for
    :param node_iter: optional iterable of nodes to try. Useful if you
        want to filter or reorder the nodes.
    :param policy: an instance of :class:`BaseStoragePolicy`. This should be
        None for an account or container ring.
    """

    def __init__(self, app, ring, partition, node_iter=None, policy=None):
        self.app = app
        self.ring = ring
        self.partition = partition

        part_nodes = ring.get_part_nodes(partition)
        if node_iter is None:
            node_iter = itertools.chain(
                part_nodes, ring.get_more_nodes(partition))
        num_primary_nodes = len(part_nodes)
        self.nodes_left = self.app.request_node_count(num_primary_nodes)
        self.expected_handoffs = self.nodes_left - num_primary_nodes

        # Use of list() here forcibly yanks the first N nodes (the primary
        # nodes) from node_iter, so the rest of its values are handoffs.
        self.primary_nodes = self.app.sort_nodes(
            list(itertools.islice(node_iter, num_primary_nodes)),
            policy=policy)
        self.handoff_iter = node_iter
        self._node_provider = None

    def __iter__(self):
        self._node_iter = self._node_gen()
        return self

    def log_handoffs(self, handoffs):
        """
        Log handoff requests if handoff logging is enabled and the
        handoff was not expected.

        We only log handoffs when we've pushed the handoff count further
        than we would normally have expected under normal circumstances,
        that is (request_node_count - num_primaries), when handoffs goes
        higher than that it means one of the primaries must have been
        skipped because of error limiting before we consumed all of our
        nodes_left.
        """
        if not self.app.log_handoffs:
            return
        extra_handoffs = handoffs - self.expected_handoffs
        if extra_handoffs > 0:
            self.app.logger.increment('handoff_count')
            self.app.logger.warning(
                'Handoff requested (%d)' % handoffs)
            if (extra_handoffs == len(self.primary_nodes)):
                # all the primaries were skipped, and handoffs didn't help
                self.app.logger.increment('handoff_all_count')

    def set_node_provider(self, callback):
        """
        Install a callback function that will be used during a call to next()
        to get an alternate node instead of returning the next node from the
        iterator.

        :param callback: A no argument function that should return a node dict
                         or None.
        """
        self._node_provider = callback

    def _node_gen(self):
        for node in self.primary_nodes:
            if not self.app.error_limited(node):
                yield node
                if not self.app.error_limited(node):
                    self.nodes_left -= 1
                    if self.nodes_left <= 0:
                        return
        handoffs = 0
        for node in self.handoff_iter:
            if not self.app.error_limited(node):
                handoffs += 1
                self.log_handoffs(handoffs)
                yield node
                if not self.app.error_limited(node):
                    self.nodes_left -= 1
                    if self.nodes_left <= 0:
                        return

    def next(self):
        if self._node_provider:
            # give node provider the opportunity to inject a node
            node = self._node_provider()
            if node:
                return node
        return next(self._node_iter)

    def __next__(self):
        return self.next()


class Controller(object):
    """Base WSGI controller class for the proxy"""
    server_type = 'Base'

    # Ensure these are all lowercase
    pass_through_headers = []

    def __init__(self, app):
        """
        Creates a controller attached to an application instance

        :param app: the application instance
        """
        self.account_name = None
        self.app = app
        self.trans_id = '-'
        self._allowed_methods = None

    @property
    def allowed_methods(self):
        if self._allowed_methods is None:
            self._allowed_methods = set()
            all_methods = inspect.getmembers(self, predicate=inspect.ismethod)
            for name, m in all_methods:
                if getattr(m, 'publicly_accessible', False):
                    self._allowed_methods.add(name)
        return self._allowed_methods

    def _x_remove_headers(self):
        """
        Returns a list of headers that must not be sent to the backend

        :returns: a list of header
        """
        return []

    def transfer_headers(self, src_headers, dst_headers):
        """
        Transfer legal headers from an original client request to dictionary
        that will be used as headers by the backend request

        :param src_headers: A dictionary of the original client request headers
        :param dst_headers: A dictionary of the backend request headers
        """
        st = self.server_type.lower()

        x_remove = 'x-remove-%s-meta-' % st
        dst_headers.update((k.lower().replace('-remove', '', 1), '')
                           for k in src_headers
                           if k.lower().startswith(x_remove) or
                           k.lower() in self._x_remove_headers())

        dst_headers.update((k.lower(), v)
                           for k, v in src_headers.items()
                           if k.lower() in self.pass_through_headers or
                           is_sys_or_user_meta(st, k))

    def generate_request_headers(self, orig_req=None, additional=None,
                                 transfer=False):
        """
        Create a list of headers to be used in backend requests

        :param orig_req: the original request sent by the client to the proxy
        :param additional: additional headers to send to the backend
        :param transfer: If True, transfer headers from original client request
        :returns: a dictionary of headers
        """
        # Use the additional headers first so they don't overwrite the headers
        # we require.
        headers = HeaderKeyDict(additional) if additional else HeaderKeyDict()
        if transfer:
            self.transfer_headers(orig_req.headers, headers)
        headers.setdefault('x-timestamp', Timestamp.now().internal)
        if orig_req:
            referer = orig_req.as_referer()
        else:
            referer = ''
        headers['x-trans-id'] = self.trans_id
        headers['connection'] = 'close'
        headers['user-agent'] = 'proxy-server %s' % os.getpid()
        headers['referer'] = referer
        return headers

    def account_info(self, account, req=None):
        """
        Get account information, and also verify that the account exists.

        :param account: name of the account to get the info for
        :param req: caller's HTTP request context object (optional)
        :returns: tuple of (account partition, account nodes, container_count)
                  or (None, None, None) if it does not exist
        """
        partition, nodes = self.app.account_ring.get_nodes(account)
        if req:
            env = getattr(req, 'environ', {})
        else:
            env = {}
        env.setdefault('swift.infocache', {})
        path_env = env.copy()
        path_env['PATH_INFO'] = "/v1/%s" % (account,)

        info = get_account_info(path_env, self.app)
        if (not info
                or not is_success(info['status'])
                or not info.get('account_really_exists', True)):
            return None, None, None
        container_count = info['container_count']
        return partition, nodes, container_count

    def container_info(self, account, container, req=None):
        """
        Get container information and thusly verify container existence.
        This will also verify account existence.

        :param account: account name for the container
        :param container: container name to look up
        :param req: caller's HTTP request context object (optional)
        :returns: dict containing at least container partition ('partition'),
                  container nodes ('containers'), container read
                  acl ('read_acl'), container write acl ('write_acl'),
                  and container sync key ('sync_key').
                  Values are set to None if the container does not exist.
        """
        part, nodes = self.app.container_ring.get_nodes(account, container)
        if req:
            env = getattr(req, 'environ', {})
        else:
            env = {}
        env.setdefault('swift.infocache', {})
        path_env = env.copy()
        path_env['PATH_INFO'] = "/v1/%s/%s" % (account, container)
        info = get_container_info(path_env, self.app)
        if not info or not is_success(info.get('status')):
            info = headers_to_container_info({}, 0)
            info['partition'] = None
            info['nodes'] = None
        else:
            info['partition'] = part
            info['nodes'] = nodes
        return info

    def _make_request(self, nodes, part, method, path, headers, query,
                      logger_thread_locals):
        """
        Iterates over the given node iterator, sending an HTTP request to one
        node at a time.  The first non-informational, non-server-error
        response is returned.  If no non-informational, non-server-error
        response is received from any of the nodes, returns None.

        :param nodes: an iterator of the backend server and handoff servers
        :param part: the partition number
        :param method: the method to send to the backend
        :param path: the path to send to the backend
                     (full path ends up being /<$device>/<$part>/<$path>)
        :param headers: dictionary of headers
        :param query: query string to send to the backend.
        :param logger_thread_locals: The thread local values to be set on the
                                     self.app.logger to retain transaction
                                     logging information.
        :returns: a swob.Response object, or None if no responses were received
        """
        self.app.logger.thread_locals = logger_thread_locals
        for node in nodes:
            try:
                start_node_timing = time.time()
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                                        node['device'], part, method, path,
                                        headers=headers, query_string=query)
                    conn.node = node
                self.app.set_node_timing(node, time.time() - start_node_timing)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    if not is_informational(resp.status) and \
                            not is_server_error(resp.status):
                        return resp.status, resp.reason, resp.getheaders(), \
                            resp.read()
                    elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.app.error_limit(node,
                                             _('ERROR Insufficient Storage'))
                    elif is_server_error(resp.status):
                        self.app.error_occurred(
                            node, _('ERROR %(status)d '
                                    'Trying to %(method)s %(path)s'
                                    ' From %(type)s Server') % {
                                        'status': resp.status,
                                        'method': method,
                                        'path': path,
                                        'type': self.server_type})
            except (Exception, Timeout):
                self.app.exception_occurred(
                    node, self.server_type,
                    _('Trying to %(method)s %(path)s') %
                    {'method': method, 'path': path})

    def make_requests(self, req, ring, part, method, path, headers,
                      query_string='', overrides=None, node_count=None,
                      node_iterator=None):
        """
        Sends an HTTP request to multiple nodes and aggregates the results.
        It attempts the primary nodes concurrently, then iterates over the
        handoff nodes as needed.

        :param req: a request sent by the client
        :param ring: the ring used for finding backend servers
        :param part: the partition number
        :param method: the method to send to the backend
        :param path: the path to send to the backend
                     (full path ends up being  /<$device>/<$part>/<$path>)
        :param headers: a list of dicts, where each dict represents one
                        backend request that should be made.
        :param query_string: optional query string to send to the backend
        :param overrides: optional return status override map used to override
                          the returned status of a request.
        :param node_count: optional number of nodes to send request to.
        :param node_iterator: optional node iterator.
        :returns: a swob.Response object
        """
        nodes = GreenthreadSafeIterator(
            node_iterator or self.app.iter_nodes(ring, part)
        )
        node_number = node_count or len(ring.get_part_nodes(part))
        pile = GreenAsyncPile(node_number)

        for head in headers:
            pile.spawn(self._make_request, nodes, part, method, path,
                       head, query_string, self.app.logger.thread_locals)
        response = []
        statuses = []
        for resp in pile:
            if not resp:
                continue
            response.append(resp)
            statuses.append(resp[0])
            if self.have_quorum(statuses, node_number):
                break
        # give any pending requests *some* chance to finish
        finished_quickly = pile.waitall(self.app.post_quorum_timeout)
        for resp in finished_quickly:
            if not resp:
                continue
            response.append(resp)
            statuses.append(resp[0])
        while len(response) < node_number:
            response.append((HTTP_SERVICE_UNAVAILABLE, '', '', b''))
        statuses, reasons, resp_headers, bodies = zip(*response)
        return self.best_response(req, statuses, reasons, bodies,
                                  '%s %s' % (self.server_type, req.method),
                                  overrides=overrides, headers=resp_headers)

    def _quorum_size(self, n):
        """
        Number of successful backend responses needed for the proxy to
        consider the client request successful.
        """
        return quorum_size(n)

    def have_quorum(self, statuses, node_count, quorum=None):
        """
        Given a list of statuses from several requests, determine if
        a quorum response can already be decided.

        :param statuses: list of statuses returned
        :param node_count: number of nodes being queried (basically ring count)
        :param quorum: number of statuses required for quorum
        :returns: True or False, depending on if quorum is established
        """
        if quorum is None:
            quorum = self._quorum_size(node_count)
        if len(statuses) >= quorum:
            for hundred in (HTTP_CONTINUE, HTTP_OK, HTTP_MULTIPLE_CHOICES,
                            HTTP_BAD_REQUEST):
                if sum(1 for s in statuses
                       if hundred <= s < hundred + 100) >= quorum:
                    return True
        return False

    def best_response(self, req, statuses, reasons, bodies, server_type,
                      etag=None, headers=None, overrides=None,
                      quorum_size=None):
        """
        Given a list of responses from several servers, choose the best to
        return to the API.

        :param req: swob.Request object
        :param statuses: list of statuses returned
        :param reasons: list of reasons for each status
        :param bodies: bodies of each response
        :param server_type: type of server the responses came from
        :param etag: etag
        :param headers: headers of each response
        :param overrides: overrides to apply when lacking quorum
        :param quorum_size: quorum size to use
        :returns: swob.Response object with the correct status, body, etc. set
        """
        if quorum_size is None:
            quorum_size = self._quorum_size(len(statuses))

        resp = self._compute_quorum_response(
            req, statuses, reasons, bodies, etag, headers,
            quorum_size=quorum_size)
        if overrides and not resp:
            faked_up_status_indices = set()
            transformed = []
            for (i, (status, reason, hdrs, body)) in enumerate(zip(
                    statuses, reasons, headers, bodies)):
                if status in overrides:
                    faked_up_status_indices.add(i)
                    transformed.append((overrides[status], '', '', ''))
                else:
                    transformed.append((status, reason, hdrs, body))
            statuses, reasons, headers, bodies = zip(*transformed)
            resp = self._compute_quorum_response(
                req, statuses, reasons, bodies, etag, headers,
                indices_to_avoid=faked_up_status_indices,
                quorum_size=quorum_size)

        if not resp:
            resp = HTTPServiceUnavailable(request=req)
            self.app.logger.error(_('%(type)s returning 503 for %(statuses)s'),
                                  {'type': server_type, 'statuses': statuses})

        return resp

    def _compute_quorum_response(self, req, statuses, reasons, bodies, etag,
                                 headers, quorum_size, indices_to_avoid=()):
        if not statuses:
            return None
        for hundred in (HTTP_OK, HTTP_MULTIPLE_CHOICES, HTTP_BAD_REQUEST):
            hstatuses = \
                [(i, s) for i, s in enumerate(statuses)
                 if hundred <= s < hundred + 100]
            if len(hstatuses) >= quorum_size:
                try:
                    status_index, status = max(
                        ((i, stat) for i, stat in hstatuses
                            if i not in indices_to_avoid),
                        key=operator.itemgetter(1))
                except ValueError:
                    # All statuses were indices to avoid
                    continue
                resp = status_map[status](request=req)
                resp.status = '%s %s' % (status, reasons[status_index])
                resp.body = bodies[status_index]
                if headers:
                    update_headers(resp, headers[status_index])
                if etag:
                    resp.headers['etag'] = etag.strip('"')
                return resp
        return None

    @public
    def GET(self, req):
        """
        Handler for HTTP GET requests.

        :param req: The client request
        :returns: the response to the client
        """
        return self.GETorHEAD(req)

    @public
    def HEAD(self, req):
        """
        Handler for HTTP HEAD requests.

        :param req: The client request
        :returns: the response to the client
        """
        return self.GETorHEAD(req)

    def autocreate_account(self, req, account):
        """
        Autocreate an account

        :param req: request leading to this autocreate
        :param account: the unquoted account name
        """
        partition, nodes = self.app.account_ring.get_nodes(account)
        path = '/%s' % account
        headers = {'X-Timestamp': Timestamp.now().internal,
                   'X-Trans-Id': self.trans_id,
                   'X-Openstack-Request-Id': self.trans_id,
                   'Connection': 'close'}
        # transfer any x-account-sysmeta headers from original request
        # to the autocreate PUT
        headers.update((k, v)
                       for k, v in req.headers.items()
                       if is_sys_meta('account', k))
        resp = self.make_requests(Request.blank('/v1' + path),
                                  self.app.account_ring, partition, 'PUT',
                                  path, [headers] * len(nodes))
        if is_success(resp.status_int):
            self.app.logger.info(_('autocreate account %r'), path)
            clear_info_cache(self.app, req.environ, account)
            return True
        else:
            self.app.logger.warning(_('Could not autocreate account %r'),
                                    path)
            return False

    def GETorHEAD_base(self, req, server_type, node_iter, partition, path,
                       concurrency=1, client_chunk_size=None):
        """
        Base handler for HTTP GET or HEAD requests.

        :param req: swob.Request object
        :param server_type: server type used in logging
        :param node_iter: an iterator to obtain nodes from
        :param partition: partition
        :param path: path for the request
        :param concurrency: number of requests to run concurrently
        :param client_chunk_size: chunk size for response body iterator
        :returns: swob.Response object
        """
        backend_headers = self.generate_request_headers(
            req, additional=req.headers)

        handler = GetOrHeadHandler(self.app, req, self.server_type, node_iter,
                                   partition, path, backend_headers,
                                   concurrency,
                                   client_chunk_size=client_chunk_size)
        res = handler.get_working_response(req)

        if not res:
            res = self.best_response(
                req, handler.statuses, handler.reasons, handler.bodies,
                '%s %s' % (server_type, req.method),
                headers=handler.source_headers)

        # if a backend policy index is present in resp headers, translate it
        # here with the friendly policy name
        if 'X-Backend-Storage-Policy-Index' in res.headers and \
                is_success(res.status_int):
            policy = \
                POLICIES.get_by_index(
                    res.headers['X-Backend-Storage-Policy-Index'])
            if policy:
                res.headers['X-Storage-Policy'] = policy.name
            else:
                self.app.logger.error(
                    'Could not translate %s (%r) from %r to policy',
                    'X-Backend-Storage-Policy-Index',
                    res.headers['X-Backend-Storage-Policy-Index'], path)

        return res

    def is_origin_allowed(self, cors_info, origin):
        """
        Is the given Origin allowed to make requests to this resource

        :param cors_info: the resource's CORS related metadata headers
        :param origin: the origin making the request
        :return: True or False
        """
        allowed_origins = set()
        if cors_info.get('allow_origin'):
            allowed_origins.update(
                [a.strip()
                 for a in cors_info['allow_origin'].split(' ')
                 if a.strip()])
        if self.app.cors_allow_origin:
            allowed_origins.update(self.app.cors_allow_origin)
        return origin in allowed_origins or '*' in allowed_origins

    @public
    def OPTIONS(self, req):
        """
        Base handler for OPTIONS requests

        :param req: swob.Request object
        :returns: swob.Response object
        """
        # Prepare the default response
        headers = {'Allow': ', '.join(self.allowed_methods)}
        resp = Response(status=200, request=req, headers=headers)

        # If this isn't a CORS pre-flight request then return now
        req_origin_value = req.headers.get('Origin', None)
        if not req_origin_value:
            return resp

        # This is a CORS preflight request so check it's allowed
        try:
            container_info = \
                self.container_info(self.account_name,
                                    self.container_name, req)
        except AttributeError:
            # This should only happen for requests to the Account. A future
            # change could allow CORS requests to the Account level as well.
            return resp

        cors = container_info.get('cors', {})

        # If the CORS origin isn't allowed return a 401
        if not self.is_origin_allowed(cors, req_origin_value) or (
                req.headers.get('Access-Control-Request-Method') not in
                self.allowed_methods):
            resp.status = HTTP_UNAUTHORIZED
            return resp

        # Populate the response with the CORS preflight headers
        if cors.get('allow_origin') and \
                cors.get('allow_origin').strip() == '*':
            headers['access-control-allow-origin'] = '*'
        else:
            headers['access-control-allow-origin'] = req_origin_value
            if 'vary' in headers:
                headers['vary'] += ', Origin'
            else:
                headers['vary'] = 'Origin'

        if cors.get('max_age') is not None:
            headers['access-control-max-age'] = cors.get('max_age')

        headers['access-control-allow-methods'] = \
            ', '.join(self.allowed_methods)

        # Allow all headers requested in the request. The CORS
        # specification does leave the door open for this, as mentioned in
        # http://www.w3.org/TR/cors/#resource-preflight-requests
        # Note: Since the list of headers can be unbounded
        # simply returning headers can be enough.
        allow_headers = set(
            list_from_csv(req.headers.get('Access-Control-Request-Headers')))
        if allow_headers:
            headers['access-control-allow-headers'] = ', '.join(allow_headers)
            if 'vary' in headers:
                headers['vary'] += ', Access-Control-Request-Headers'
            else:
                headers['vary'] = 'Access-Control-Request-Headers'

        resp.headers = headers

        return resp

    def get_name_length_limit(self):
        if self.account_name.startswith(self.app.auto_create_account_prefix):
            multiplier = 2
        else:
            multiplier = 1

        if self.server_type == 'Account':
            return constraints.MAX_ACCOUNT_NAME_LENGTH * multiplier
        elif self.server_type == 'Container':
            return constraints.MAX_CONTAINER_NAME_LENGTH * multiplier
        else:
            raise ValueError(
                "server_type can only be 'account' or 'container'")

    def _get_container_listing(self, req, account, container, headers=None,
                               params=None):
        """
        Fetch container listing from given `account/container`.

        :param req: original Request instance.
        :param account: account in which `container` is stored.
        :param container: container from listing should be fetched.
        :param headers: headers to be included with the request
        :param params: query string parameters to be used.
        :return: a tuple of (deserialized json data structure, swob Response)
        """
        params = params or {}
        version, _a, _c, _other = req.split_path(3, 4, True)
        path = '/'.join(['', version, account, container])

        subreq = make_pre_authed_request(
            req.environ, method='GET', path=quote(path), headers=req.headers,
            swift_source='SH')
        if headers:
            subreq.headers.update(headers)
        subreq.params = params
        self.app.logger.debug(
            'Get listing from %s %s' % (subreq.path_qs, headers))
        response = self.app.handle_request(subreq)

        if not is_success(response.status_int):
            self.app.logger.warning(
                'Failed to get container listing from %s: %s',
                subreq.path_qs, response.status_int)
            return None, response

        try:
            data = json.loads(response.body)
            if not isinstance(data, list):
                raise ValueError('not a list')
            return data, response
        except ValueError as err:
            self.app.logger.error(
                'Problem with listing response from %s: %r',
                subreq.path_qs, err)
            return None, response

    def _get_shard_ranges(self, req, account, container, includes=None,
                          states=None):
        """
        Fetch shard ranges from given `account/container`. If `includes` is
        given then the shard range for that object name is requested, otherwise
        all shard ranges are requested.

        :param req: original Request instance.
        :param account: account from which shard ranges should be fetched.
        :param container: container from which shard ranges should be fetched.
        :param includes: (optional) restricts the list of fetched shard ranges
            to those which include the given name.
        :param states: (optional) the states of shard ranges to be fetched.
        :return: a list of instances of :class:`swift.common.utils.ShardRange`,
            or None if there was a problem fetching the shard ranges
        """
        params = req.params.copy()
        params.pop('limit', None)
        params['format'] = 'json'
        if includes:
            params['includes'] = includes
        if states:
            params['states'] = states
        headers = {'X-Backend-Record-Type': 'shard'}
        listing, response = self._get_container_listing(
            req, account, container, headers=headers, params=params)
        if listing is None:
            return None

        record_type = response.headers.get('x-backend-record-type')
        if record_type != 'shard':
            err = 'unexpected record type %r' % record_type
            self.app.logger.error("Failed to get shard ranges from %s: %s",
                                  req.path_qs, err)
            return None

        try:
            return [ShardRange.from_dict(shard_range)
                    for shard_range in listing]
        except (ValueError, TypeError, KeyError) as err:
            self.app.logger.error(
                "Failed to get shard ranges from %s: invalid data: %r",
                req.path_qs, err)
            return None
