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

from urllib.parse import quote

import time
import json
import functools
import inspect
import itertools
import operator
import random
from copy import deepcopy

from eventlet.timeout import Timeout

from swift.common.memcached import MemcacheConnectionError
from swift.common.wsgi import make_pre_authed_env, make_pre_authed_request
from swift.common.utils import Timestamp, WatchdogTimeout, config_true_value, \
    public, split_path, list_from_csv, GreenthreadSafeIterator, \
    GreenAsyncPile, quorum_size, parse_content_type, drain_and_close, \
    document_iters_to_http_response_body, cache_from_env, \
    CooperativeIterator, NamespaceBoundList, Namespace, ClosingMapper
from swift.common.bufferedhttp import http_connect
from swift.common import constraints
from swift.common.exceptions import ChunkReadTimeout, ChunkWriteTimeout, \
    ConnectionTimeout, RangeAlreadyComplete, ShortReadError
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import is_informational, is_success, is_redirection, \
    is_server_error, HTTP_OK, HTTP_PARTIAL_CONTENT, HTTP_MULTIPLE_CHOICES, \
    HTTP_BAD_REQUEST, HTTP_NOT_FOUND, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_UNAUTHORIZED, HTTP_CONTINUE, HTTP_GONE, \
    HTTP_REQUESTED_RANGE_NOT_SATISFIABLE
from swift.common.swob import Request, Response, Range, \
    HTTPException, HTTPRequestedRangeNotSatisfiable, HTTPServiceUnavailable, \
    status_map, wsgi_to_str, str_to_wsgi, wsgi_quote, wsgi_unquote, \
    normalize_etag
from swift.common.request_helpers import strip_sys_meta_prefix, \
    strip_user_meta_prefix, is_user_meta, is_sys_meta, is_sys_or_user_meta, \
    http_response_to_document_iters, is_object_transient_sysmeta, \
    strip_object_transient_sysmeta_prefix, get_ip_port, get_user_meta_prefix, \
    get_sys_meta_prefix, is_use_replication_network
from swift.common.storage_policy import POLICIES

DEFAULT_RECHECK_ACCOUNT_EXISTENCE = 60  # seconds
DEFAULT_RECHECK_CONTAINER_EXISTENCE = 60  # seconds
DEFAULT_RECHECK_UPDATING_SHARD_RANGES = 3600  # seconds
DEFAULT_RECHECK_LISTING_SHARD_RANGES = 600  # seconds


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
        lkey = wsgi_to_str(key).lower()
        val = wsgi_to_str(val) if isinstance(val, str) else val
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
        'sync_to': headers.get('x-container-sync-to'),
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
        # the 'internal' format version of timestamps is cached since the
        # normal format can be derived from this when required
        'created_at': headers.get('x-backend-timestamp'),
        'put_timestamp': headers.get('x-backend-put-timestamp'),
        'delete_timestamp': headers.get('x-backend-delete-timestamp'),
        'status_changed_at': headers.get('x-backend-status-changed-at'),
    }


def headers_from_container_info(info):
    """
    Construct a HeaderKeyDict from a container info dict.

    :param info: a dict of container metadata
    :returns: a HeaderKeyDict or None if info is None or any required headers
        could not be constructed
    """
    if not info:
        return None

    required = (
        ('x-backend-timestamp', 'created_at'),
        ('x-backend-put-timestamp', 'put_timestamp'),
        ('x-backend-delete-timestamp', 'delete_timestamp'),
        ('x-backend-status-changed-at', 'status_changed_at'),
        ('x-backend-storage-policy-index', 'storage_policy'),
        ('x-container-object-count', 'object_count'),
        ('x-container-bytes-used', 'bytes'),
        ('x-backend-sharding-state', 'sharding_state'),
    )
    required_normal_format_timestamps = (
        ('x-timestamp', 'created_at'),
        ('x-put-timestamp', 'put_timestamp'),
    )
    optional = (
        ('x-container-read', 'read_acl'),
        ('x-container-write', 'write_acl'),
        ('x-container-sync-key', 'sync_key'),
        ('x-container-sync-to', 'sync_to'),
        ('x-versions-location', 'versions'),
    )
    cors_optional = (
        ('access-control-allow-origin', 'allow_origin'),
        ('access-control-expose-headers', 'expose_headers'),
        ('access-control-max-age', 'max_age')
    )

    def lookup(info, key):
        # raises KeyError or ValueError
        val = info[key]
        if val is None:
            raise ValueError
        return val

    # note: required headers may be missing from info for example during
    # upgrade when stale info is still in cache
    headers = HeaderKeyDict()
    for hdr, key in required:
        try:
            headers[hdr] = lookup(info, key)
        except (KeyError, ValueError):
            return None

    for hdr, key in required_normal_format_timestamps:
        try:
            headers[hdr] = Timestamp(lookup(info, key)).normal
        except (KeyError, ValueError):
            return None

    for hdr, key in optional:
        try:
            headers[hdr] = lookup(info, key)
        except (KeyError, ValueError):
            pass

    policy_index = info.get('storage_policy')
    headers['x-storage-policy'] = POLICIES[int(policy_index)].name
    prefix = get_user_meta_prefix('container')
    headers.update(
        (prefix + k, v)
        for k, v in info.get('meta', {}).items())
    for hdr, key in cors_optional:
        try:
            headers[prefix + hdr] = lookup(info.get('cors'), key)
        except (KeyError, ValueError):
            pass
    prefix = get_sys_meta_prefix('container')
    headers.update(
        (prefix + k, v)
        for k, v in info.get('sysmeta', {}).items())

    return headers


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
                    if 'Vary' in resp.headers:
                        resp.headers['Vary'] += ', Origin'
                    else:
                        resp.headers['Vary'] = 'Origin'

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


def _record_ac_info_cache_metrics(
        app, cache_state, container=None, resp=None):
    """
    Record a single cache operation by account or container lookup into its
    corresponding metrics.

    :param  app: the application object
    :param  cache_state: the state of this cache operation, includes
                infocache_hit, memcache hit, miss, error, skip, force_skip
                and disabled.
    :param  container: the container name
    :param  resp: the response from either backend or cache hit.
    """
    try:
        proxy_app = app._pipeline_final_app
    except AttributeError:
        logger = None
    else:
        logger = proxy_app.logger
    server_type = 'container' if container else 'account'
    if logger:
        record_cache_op_metrics(logger, server_type, 'info', cache_state, resp)


def get_container_info(env, app, swift_source=None, cache_only=False):
    """
    Get the info structure for a container, based on env and app.
    This is useful to middlewares.

    :param env: the environment used by the current request
    :param app: the application object
    :param swift_source: Used to mark the request as originating out of
                         middleware. Will be logged in proxy logs.
    :param cache_only: If true, indicates that caller doesn't want to HEAD the
                       backend container when cache miss.
    :returns: the object info

    .. note::

        This call bypasses auth. Success does not imply that the request has
        authorization to the container.
    """
    (version, wsgi_account, wsgi_container, unused) = \
        split_path(env['PATH_INFO'], 3, 4, True)

    if not constraints.valid_api_version(version):
        # Not a valid Swift request; return 0 like we do
        # if there's an account failure
        return headers_to_container_info({}, 0)

    account = wsgi_to_str(wsgi_account)
    container = wsgi_to_str(wsgi_container)

    # Try to cut through all the layers to the proxy app
    # (while also preserving logging)
    try:
        logged_app = app._pipeline_request_logging_app
        proxy_app = app._pipeline_final_app
    except AttributeError:
        logged_app = proxy_app = app
    # Check in environment cache and in memcache (in that order)
    info, cache_state = _get_info_from_caches(
        proxy_app, env, account, container)

    resp = None
    if not info and not cache_only:
        # Cache miss; go HEAD the container and populate the caches
        env.setdefault('swift.infocache', {})
        # Before checking the container, make sure the account exists.
        #
        # If it is an autocreateable account, just assume it exists; don't
        # HEAD the account, as a GET or HEAD response for an autocreateable
        # account is successful whether the account actually has .db files
        # on disk or not.
        is_autocreate_account = account.startswith(
            constraints.AUTO_CREATE_ACCOUNT_PREFIX)
        if not is_autocreate_account:
            account_info = get_account_info(env, logged_app, swift_source)
            if not account_info or not is_success(account_info['status']):
                _record_ac_info_cache_metrics(
                    logged_app, cache_state, container)
                return headers_to_container_info({}, 0)

        req = _prepare_pre_auth_info_request(
            env, ("/%s/%s/%s" % (version, wsgi_account, wsgi_container)),
            (swift_source or 'GET_CONTAINER_INFO'))
        # *Always* allow reserved names for get-info requests -- it's on the
        # caller to keep the result private-ish
        req.headers['X-Backend-Allow-Reserved-Names'] = 'true'
        resp = req.get_response(logged_app)
        drain_and_close(resp)
        # Check in infocache to see if the proxy (or anyone else) already
        # populated the cache for us. If they did, just use what's there.
        #
        # See similar comment in get_account_info() for justification.
        info = _get_info_from_infocache(env, account, container)
        if info is None:
            info = set_info_cache(env, account, container, resp)

    if info:
        info = deepcopy(info)  # avoid mutating what's in swift.infocache
    else:
        status_int = 0 if cache_only else 503
        info = headers_to_container_info({}, status_int)

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

    versions_cont = info.get('sysmeta', {}).get('versions-container', '')
    if versions_cont:
        versions_cont = wsgi_unquote(str_to_wsgi(
            versions_cont)).split('/')[0]
        versions_req = _prepare_pre_auth_info_request(
            env, ("/%s/%s/%s" % (version, wsgi_account, versions_cont)),
            (swift_source or 'GET_CONTAINER_INFO'))
        versions_req.headers['X-Backend-Allow-Reserved-Names'] = 'true'
        versions_info = get_container_info(versions_req.environ, app)
        info['bytes'] = info['bytes'] + versions_info['bytes']

    _record_ac_info_cache_metrics(logged_app, cache_state, container, resp)
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
    (version, wsgi_account, _junk) = split_path(env['PATH_INFO'], 2, 3, True)

    if not constraints.valid_api_version(version):
        return headers_to_account_info({}, 0)

    account = wsgi_to_str(wsgi_account)

    # Try to cut through all the layers to the proxy app
    # (while also preserving logging)
    try:
        app = app._pipeline_request_logging_app
    except AttributeError:
        pass
    # Check in environment cache and in memcache (in that order)
    info, cache_state = _get_info_from_caches(app, env, account)

    # Cache miss; go HEAD the account and populate the caches
    if info:
        resp = None
    else:
        env.setdefault('swift.infocache', {})
        req = _prepare_pre_auth_info_request(
            env, "/%s/%s" % (version, wsgi_account),
            (swift_source or 'GET_ACCOUNT_INFO'))
        # *Always* allow reserved names for get-info requests -- it's on the
        # caller to keep the result private-ish
        req.headers['X-Backend-Allow-Reserved-Names'] = 'true'
        resp = req.get_response(app)
        drain_and_close(resp)
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
            info = set_info_cache(env, account, None, resp)

    if info:
        info = info.copy()  # avoid mutating what's in swift.infocache
    else:
        info = headers_to_account_info({}, 503)

    for field in ('container_count', 'bytes', 'total_object_count'):
        if info.get(field) is None:
            info[field] = 0
        else:
            info[field] = int(info[field])

    _record_ac_info_cache_metrics(app, cache_state, container=None, resp=resp)
    return info


def get_cache_key(account, container=None, obj=None, shard=None):
    """
    Get the keys for both memcache and env['swift.infocache'] (cache_key)
    where info about accounts, containers, and objects is cached

    :param account: The name of the account
    :param container: The name of the container (or None if account)
    :param obj: The name of the object (or None if account or container)
    :param shard: Sharding state for the container query; typically 'updating'
                  or 'listing' (Requires account and container; cannot use
                  with obj)
    :returns: a (native) string cache_key
    """
    def to_native(s):
        if s is None or isinstance(s, str):
            return s
        return s.decode('utf8', 'surrogateescape')

    account = to_native(account)
    container = to_native(container)
    obj = to_native(obj)

    if shard:
        if not (account and container):
            raise ValueError('Shard cache key requires account and container')
        if obj:
            raise ValueError('Shard cache key cannot have obj')
        cache_key = 'shard-%s-v2/%s/%s' % (shard, account, container)
    elif obj:
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


def set_info_cache(env, account, container, resp):
    """
    Cache info in both memcache and env.

    :param  env: the WSGI request environment
    :param  account: the unquoted account name
    :param  container: the unquoted container name or None
    :param  resp: the response received or None if info cache should be cleared

    :returns: the info that was placed into the cache, or None if the
              request status was not in (404, 410, 2xx).
    """
    cache_key = get_cache_key(account, container)
    infocache = env.setdefault('swift.infocache', {})
    memcache = cache_from_env(env, True)
    if resp is None:
        clear_info_cache(env, account, container)
        return

    if container:
        cache_time = int(resp.headers.get(
            'X-Backend-Recheck-Container-Existence',
            DEFAULT_RECHECK_CONTAINER_EXISTENCE))
    else:
        cache_time = int(resp.headers.get(
            'X-Backend-Recheck-Account-Existence',
            DEFAULT_RECHECK_ACCOUNT_EXISTENCE))

    if resp.status_int in (HTTP_NOT_FOUND, HTTP_GONE):
        cache_time *= 0.1
    elif not is_success(resp.status_int):
        # If we got a response, it was unsuccessful, and it wasn't an
        # "authoritative" failure, bail without touching caches.
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


def clear_info_cache(env, account, container=None, shard=None):
    """
    Clear the cached info in both memcache and env

    :param  env: the WSGI request environment
    :param  account: the account name
    :param  container: the container name if clearing info for containers, or
              None
    :param  shard: the sharding state if clearing info for container shard
              ranges, or None
    """
    cache_key = get_cache_key(account, container, shard=shard)
    infocache = env.setdefault('swift.infocache', {})
    memcache = cache_from_env(env, True)
    infocache.pop(cache_key, None)
    if memcache:
        memcache.delete(cache_key)


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


def record_cache_op_metrics(
        logger, server_type, op_type, cache_state, resp=None):
    """
    Record a single cache operation into its corresponding metrics.

    :param  logger: the metrics logger
    :param  server_type: 'account' or 'container'
    :param  op_type: the name of the operation type, includes 'shard_listing',
              'shard_updating', and etc.
    :param  cache_state: the state of this cache operation. When it's
              'infocache_hit' or memcache 'hit', expect it succeeded and 'resp'
              will be None; for all other cases like memcache 'miss' or 'skip'
              which will make to backend, expect a valid 'resp'.
    :param  resp: the response from backend for all cases except cache hits.
    """
    server_type = server_type.lower()
    if cache_state == 'infocache_hit':
        logger.increment('%s.%s.infocache.hit' % (server_type, op_type))
    elif cache_state == 'hit':
        # memcache hits.
        logger.increment('%s.%s.cache.hit' % (server_type, op_type))
    else:
        # the cases of cache_state is memcache miss, error, skip, force_skip
        # or disabled.
        if resp:
            logger.increment('%s.%s.cache.%s.%d' % (
                server_type, op_type, cache_state, resp.status_int))
        else:
            # In some situation, we choose not to lookup backend after cache
            # miss.
            logger.increment('%s.%s.cache.%s' % (
                server_type, op_type, cache_state))


def _get_info_from_memcache(app, env, account, container=None):
    """
    Get cached account or container information from memcache

    :param  app: the application object
    :param  env: the environment used by the current request
    :param  account: the account name
    :param  container: the container name

    :returns: a tuple of two values, the first is a dictionary of cached info
      on cache hit, None on miss or if memcache is not in use; the second is
      cache state.
    """
    memcache = cache_from_env(env, True)
    if not memcache:
        return None, 'disabled'

    try:
        proxy_app = app._pipeline_final_app
    except AttributeError:
        # Only the middleware entry-points get a reference to the
        # proxy-server app; if a middleware composes itself as multiple
        # filters, we'll just have to choose a reasonable default
        skip_chance = 0.0
    else:
        if container:
            skip_chance = proxy_app.container_existence_skip_cache
        else:
            skip_chance = proxy_app.account_existence_skip_cache

    cache_key = get_cache_key(account, container)
    if skip_chance and random.random() < skip_chance:
        info = None
        cache_state = 'skip'
    else:
        info = memcache.get(cache_key)
        cache_state = 'hit' if info else 'miss'
    if info:
        env.setdefault('swift.infocache', {})[cache_key] = info
    return info, cache_state


def _get_info_from_caches(app, env, account, container=None):
    """
    Get the cached info from env or memcache (if used) in that order.
    Used for both account and container info.

    :param  app: the application object
    :param  env: the environment used by the current request
    :returns: a tuple of (the cached info or None if not cached, cache state)
    """

    info = _get_info_from_infocache(env, account, container)
    if info:
        cache_state = 'infocache_hit'
    else:
        info, cache_state = _get_info_from_memcache(
            app, env, account, container)
    return info, cache_state


def get_namespaces_from_cache(req, cache_key, skip_chance):
    """
    Get cached namespaces from infocache or memcache.

    :param req: a :class:`swift.common.swob.Request` object.
    :param cache_key: the cache key for both infocache and memcache.
    :param skip_chance: the probability of skipping the memcache look-up.
    :return: a tuple of (value, cache state). Value is an instance of
        :class:`swift.common.utils.NamespaceBoundList` if a non-empty list is
        found in memcache. Otherwise value is ``None``, for example if memcache
        look-up was skipped, or no value was found, or an empty list was found.
    """
    # try get namespaces from infocache first
    infocache = req.environ.setdefault('swift.infocache', {})
    ns_bound_list = infocache.get(cache_key)
    if ns_bound_list:
        return ns_bound_list, 'infocache_hit'

    # then try get them from memcache
    memcache = cache_from_env(req.environ, True)
    if not memcache:
        return None, 'disabled'
    if skip_chance and random.random() < skip_chance:
        return None, 'skip'
    try:
        bounds = memcache.get(cache_key, raise_on_error=True)
        cache_state = 'hit' if bounds else 'miss'
    except MemcacheConnectionError:
        bounds = None
        cache_state = 'error'

    if bounds:
        ns_bound_list = NamespaceBoundList(bounds)
        infocache[cache_key] = ns_bound_list
    else:
        ns_bound_list = None
    return ns_bound_list, cache_state


def set_namespaces_in_cache(req, cache_key, ns_bound_list, time):
    """
    Set a list of namespace bounds in infocache and memcache.

    :param req: a :class:`swift.common.swob.Request` object.
    :param cache_key: the cache key for both infocache and memcache.
    :param ns_bound_list: a :class:`swift.common.utils.NamespaceBoundList`.
    :param time: how long the namespaces should remain in memcache.
    :return: the cache_state.
    """
    infocache = req.environ.setdefault('swift.infocache', {})
    infocache[cache_key] = ns_bound_list
    memcache = cache_from_env(req.environ, True)
    if memcache and ns_bound_list:
        try:
            memcache.set(cache_key, ns_bound_list.bounds, time=time,
                         raise_on_error=True)
        except MemcacheConnectionError:
            cache_state = 'set_error'
        else:
            cache_state = 'set'
    else:
        cache_state = 'disabled'
    return cache_state


def _prepare_pre_auth_info_request(env, path, swift_source):
    """
    Prepares a pre authed request to obtain info using a HEAD.

    :param env: the environment used by the current request
    :param path: The unquoted, WSGI-str request path
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
    return Request.blank(wsgi_quote(path), environ=newenv)


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
    :param account: The unquoted, WSGI-str name of the account
    :param container: The unquoted, WSGI-str name of the container
    :param obj: The unquoted, WSGI-str name of the object
    :returns: the cached info or None if cannot be retrieved
    """
    cache_key = get_cache_key(account, container, obj)
    info = env.get('swift.infocache', {}).get(cache_key)
    if info:
        return info
    # Not in cache, let's try the object servers
    path = '/v1/%s/%s/%s' % (account, container, obj)
    req = _prepare_pre_auth_info_request(env, path, swift_source)
    # *Always* allow reserved names for get-info requests -- it's on the
    # caller to keep the result private-ish
    req.headers['X-Backend-Allow-Reserved-Names'] = 'true'
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


def is_good_source(status, server_type):
    """
    Indicates whether or not the request made to the backend found
    what it was looking for.

    :param resp: the response from the backend.
    :param server_type: the type of server: 'Account', 'Container' or 'Object'.
    :returns: True if the response status code is acceptable, False if not.
    """
    if (server_type == 'Object' and
            status == HTTP_REQUESTED_RANGE_NOT_SATISFIABLE):
        return True
    return is_success(status) or is_redirection(status)


def is_useful_response(resp, node):
    if not resp:
        return False
    if ('handoff_index' in node
            and resp.status == 404
            and resp.getheader('x-backend-timestamp') is None):
        # a 404 from a handoff are not considered authoritative unless they
        # have an x-backend-timestamp that indicates that there is a tombstone
        return False
    return True


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


class GetterSource(object):
    """
    Encapsulates properties of a source from which a GET response is read.

    :param app: a proxy app.
    :param resp: an instance of ``HTTPResponse``.
    :param node: a dict describing the node from which the response was
        returned.
    """
    __slots__ = ('app', 'resp', 'node', '_parts_iter')

    def __init__(self, app, resp, node):
        self.app = app
        self.resp = resp
        self.node = node
        self._parts_iter = None

    @property
    def timestamp(self):
        """
        Provide the timestamp of the swift http response as a floating
        point value.  Used as a sort key.

        :return: an instance of ``utils.Timestamp``
        """
        return Timestamp(self.resp.getheader('x-backend-data-timestamp') or
                         self.resp.getheader('x-backend-timestamp') or
                         self.resp.getheader('x-put-timestamp') or
                         self.resp.getheader('x-timestamp') or 0)

    @property
    def parts_iter(self):
        # lazy load a source response body parts iter if and when the source is
        # actually read
        if self.resp and not self._parts_iter:
            self._parts_iter = http_response_to_document_iters(
                self.resp, read_chunk_size=self.app.object_chunk_size)
        return self._parts_iter

    def close(self):
        # Close-out the connection as best as possible.
        close_swift_conn(self.resp)


class GetterBase(object):
    """
    This base class provides helper methods for handling GET requests to
    backend servers.

    :param app: a proxy app.
    :param req: an instance of ``swob.Request``.
    :param node_iter: an iterator yielding nodes.
    :param partition: partition.
    :param policy: the policy instance, or None if Account or Container.
    :param path: path for the request.
    :param backend_headers: a dict of headers to be sent with backend requests.
    :param node_timeout: the timeout value for backend requests.
    :param resource_type: a string description of the type of resource being
        accessed; ``resource type`` is used in logs and isn't necessarily the
        server type.
    :param logger: a logger instance.
    """
    def __init__(self, app, req, node_iter, partition, policy,
                 path, backend_headers, node_timeout, resource_type,
                 logger=None):
        self.app = app
        self.req = req
        self.node_iter = node_iter
        self.partition = partition
        self.policy = policy
        self.path = path
        self.backend_headers = backend_headers
        # resource type is used in logs and isn't necessarily the server type
        self.resource_type = resource_type
        self.node_timeout = node_timeout
        self.logger = logger or app.logger
        self.bytes_used_from_backend = 0
        self.source = None

    def _find_source(self):
        """
        Look for a suitable new source and if one is found then set
        ``self.source``.

        :return: ``True`` if ``self.source`` has been updated, ``False``
            otherwise.
        """
        # Subclasses must implement this method, but _replace_source should be
        # called to get a source installed
        raise NotImplementedError()

    def _replace_source(self, err_msg=''):
        if self.source:
            self.app.error_occurred(self.source.node, err_msg)
            self.source.close()
        return self._find_source()

    def _get_next_response_part(self):
        # return the next part of the response body; there may only be one part
        # unless it's a multipart/byteranges response
        while True:
            # the loop here is to resume if trying to parse
            # multipart/byteranges response raises a ChunkReadTimeout
            # and resets the source_parts_iter
            try:
                with WatchdogTimeout(self.app.watchdog, self.node_timeout,
                                     ChunkReadTimeout):
                    # If we don't have a multipart/byteranges response,
                    # but just a 200 or a single-range 206, then this
                    # performs no IO, and either just returns source or
                    # raises StopIteration.
                    # Otherwise, this call to next() performs IO when
                    # we have a multipart/byteranges response, as it
                    # will read the MIME boundary and part headers. In this
                    # case, ChunkReadTimeout may also be raised.
                    # If StopIteration is raised, it escapes and is
                    # handled elsewhere.
                    start_byte, end_byte, length, headers, part = next(
                        self.source.parts_iter)
                return (start_byte, end_byte, length, headers, part)
            except ChunkReadTimeout:
                if not self._replace_source(
                        'Trying to read next part of %s multi-part GET '
                        '(retrying)' % self.resource_type):
                    raise

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
        self.backend_headers.pop(
            'X-Backend-Ignore-Range-If-Metadata-Present', None)

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


class GetOrHeadHandler(GetterBase):
    """
    Handles GET requests to backend servers.

    :param app: a proxy app.
    :param req: an instance of ``swob.Request``.
    :param server_type: server type used in logging
    :param node_iter: an iterator yielding nodes.
    :param partition: partition.
    :param path: path for the request.
    :param backend_headers: a dict of headers to be sent with backend requests.
    :param concurrency: number of requests to run concurrently.
    :param policy: the policy instance, or None if Account or Container.
    :param logger: a logger instance.
    """
    def __init__(self, app, req, server_type, node_iter, partition, path,
                 backend_headers, concurrency=1, policy=None, logger=None):
        newest = config_true_value(req.headers.get('x-newest', 'f'))
        if server_type == 'Object' and not newest:
            node_timeout = app.recoverable_node_timeout
        else:
            node_timeout = app.node_timeout
        super(GetOrHeadHandler, self).__init__(
            app=app, req=req, node_iter=node_iter, partition=partition,
            policy=policy, path=path, backend_headers=backend_headers,
            node_timeout=node_timeout, resource_type=server_type.lower(),
            logger=logger)
        self.newest = newest
        self.server_type = server_type
        self.used_nodes = []
        self.used_source_etag = None
        self.concurrency = concurrency
        self.latest_404_timestamp = Timestamp(0)
        policy_options = self.app.get_policy_options(self.policy)
        self.rebalance_missing_suppression_count = min(
            policy_options.rebalance_missing_suppression_count,
            node_iter.num_primary_nodes - 1)

        # populated when finding source
        self.statuses = []
        self.reasons = []
        self.bodies = []
        self.source_headers = []
        self.sources = []

        # populated from response headers
        self.start_byte = self.end_byte = self.length = None

    def _iter_bytes_from_response_part(self, part_file, nbytes):
        # yield chunks of bytes from a single response part; if an error
        # occurs, try to resume yielding bytes from a different source
        part_file = ByteCountEnforcer(part_file, nbytes)
        while True:
            try:
                with WatchdogTimeout(self.app.watchdog, self.node_timeout,
                                     ChunkReadTimeout):
                    chunk = part_file.read(self.app.object_chunk_size)
                    if nbytes is not None:
                        nbytes -= len(chunk)
            except (ChunkReadTimeout, ShortReadError) as e:
                if self.newest or self.server_type != 'Object':
                    raise
                try:
                    self.fast_forward(self.bytes_used_from_backend)
                except (HTTPException, ValueError):
                    raise e
                except RangeAlreadyComplete:
                    break
                if self._replace_source(
                        'Trying to read object during GET (retrying)'):
                    try:
                        _junk, _junk, _junk, _junk, part_file = \
                            self._get_next_response_part()
                    except StopIteration:
                        # Tried to find a new node from which to
                        # finish the GET, but failed. There's
                        # nothing more we can do here.
                        raise e
                    part_file = ByteCountEnforcer(part_file, nbytes)
                else:
                    raise e
            else:
                if not chunk:
                    break

                with WatchdogTimeout(self.app.watchdog,
                                     self.app.client_timeout,
                                     ChunkWriteTimeout):
                    self.bytes_used_from_backend += len(chunk)
                    yield chunk

    def _iter_parts_from_response(self):
        # iterate over potentially multiple response body parts; for each
        # part, yield an iterator over the part's bytes
        try:
            part_iter = None
            try:
                while True:
                    start_byte, end_byte, length, headers, part = \
                        self._get_next_response_part()
                    self.learn_size_from_content_range(
                        start_byte, end_byte, length)
                    self.bytes_used_from_backend = 0
                    # not length; that refers to the whole object, so is the
                    # wrong value to use for GET-range responses
                    byte_count = ((end_byte - start_byte + 1)
                                  if (end_byte is not None
                                      and start_byte is not None)
                                  else None)
                    part_iter = CooperativeIterator(
                        self._iter_bytes_from_response_part(part, byte_count))
                    yield {'start_byte': start_byte, 'end_byte': end_byte,
                           'entity_length': length, 'headers': headers,
                           'part_iter': part_iter}
                    self.pop_range()
            except StopIteration:
                self.req.environ['swift.non_client_disconnect'] = True
            finally:
                if part_iter:
                    part_iter.close()

        except ChunkWriteTimeout:
            self.logger.info(
                'Client did not read from proxy within %ss',
                self.app.client_timeout)
            self.logger.increment('%s.client_timeouts' %
                                  self.server_type.lower())
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
            if (warn and
                    not self.req.environ.get('swift.non_client_disconnect')):
                self.logger.info('Client disconnected on read of %r',
                                 self.path)
            raise
        except Exception:
            self.logger.exception('Trying to send to client')
            raise
        finally:
            self.source.close()

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

    def _make_node_request(self, node, logger_thread_locals):
        # make a backend request; return True if the response is deemed good
        # (has an acceptable status code), useful (matches any previously
        # discovered etag) and sufficient (a single good response is
        # insufficient when we're searching for the newest timestamp)
        self.logger.thread_locals = logger_thread_locals
        if node in self.used_nodes:
            return False

        req_headers = dict(self.backend_headers)
        ip, port = get_ip_port(node, req_headers)
        start_node_timing = time.time()
        try:
            with ConnectionTimeout(self.app.conn_timeout):
                conn = http_connect(
                    ip, port, node['device'],
                    self.partition, self.req.method, self.path,
                    headers=req_headers,
                    query_string=self.req.query_string)
            self.app.set_node_timing(node, time.time() - start_node_timing)

            with Timeout(self.node_timeout):
                possible_source = conn.getresponse()
                # See NOTE: swift_conn at top of file about this.
                possible_source.swift_conn = conn
        except (Exception, Timeout):
            self.app.exception_occurred(
                node, self.server_type,
                'Trying to %(method)s %(path)s' %
                {'method': self.req.method, 'path': self.req.path})
            return False

        src_headers = dict(
            (k.lower(), v) for k, v in
            possible_source.getheaders())
        if is_good_source(possible_source.status, self.server_type):
            # 404 if we know we don't have a synced copy
            if not float(possible_source.getheader('X-PUT-Timestamp', 1)):
                self.statuses.append(HTTP_NOT_FOUND)
                self.reasons.append('')
                self.bodies.append('')
                self.source_headers.append([])
                close_swift_conn(possible_source)
            else:
                if self.used_source_etag and \
                        self.used_source_etag != normalize_etag(
                            src_headers.get('etag', '')):
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
                    self.sources.append(
                        GetterSource(self.app, possible_source, node))
                    if not self.newest:  # one good source is enough
                        return True
        else:
            if 'handoff_index' in node and \
                    (is_server_error(possible_source.status) or
                     possible_source.status == HTTP_NOT_FOUND) and \
                    not Timestamp(src_headers.get('x-backend-timestamp', 0)):
                # throw out 5XX and 404s from handoff nodes unless the data is
                # really on disk and had been DELETEd
                return False

            if self.rebalance_missing_suppression_count > 0 and \
                    possible_source.status == HTTP_NOT_FOUND and \
                    not Timestamp(src_headers.get('x-backend-timestamp', 0)):
                self.rebalance_missing_suppression_count -= 1
                return False

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
            self.app.check_response(node, self.server_type, possible_source,
                                    self.req.method, self.path,
                                    self.bodies[-1])
        return False

    def _find_source(self):
        self.statuses = []
        self.reasons = []
        self.bodies = []
        self.source_headers = []
        self.sources = []

        nodes = GreenthreadSafeIterator(self.node_iter)

        pile = GreenAsyncPile(self.concurrency)

        for node in nodes:
            pile.spawn(self._make_node_request, node,
                       self.logger.thread_locals)
            _timeout = self.app.get_policy_options(
                self.policy).concurrency_timeout \
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
                            if s.timestamp >= self.latest_404_timestamp]

        if self.sources:
            self.sources.sort(key=operator.attrgetter('timestamp'))
            source = self.sources.pop()
            for unused_source in self.sources:
                unused_source.close()
            self.used_nodes.append(source.node)

            # Save off the source etag so that, if we lose the connection
            # and have to resume from a different node, we can be sure that
            # we have the same object (replication). Otherwise, if the cluster
            # has two versions of the same object, we might end up switching
            # between old and new mid-stream and giving garbage to the client.
            if self.used_source_etag is None:
                self.used_source_etag = normalize_etag(
                    source.resp.getheader('etag', ''))
            self.source = source
            return True
        return False

    def _make_app_iter(self):
        """
        Returns an iterator over the contents of the source (via its read
        func).  There is also quite a bit of cleanup to ensure garbage
        collection works and the underlying socket of the source is closed.

        :return: an iterator that yields chunks of response body bytes
        """

        ct = self.source.resp.getheader('Content-Type')
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

        parts_iter = self._iter_parts_from_response()

        def add_content_type(response_part):
            response_part["content_type"] = \
                HeaderKeyDict(response_part["headers"]).get("Content-Type")
            return response_part

        return document_iters_to_http_response_body(
            ClosingMapper(add_content_type, parts_iter),
            boundary, is_multipart, self.logger)

    def get_working_response(self):
        res = None
        if self._replace_source():
            res = Response(request=self.req)
            res.status = self.source.resp.status
            update_headers(res, self.source.resp.getheaders())
            if self.req.method == 'GET' and \
                    self.source.resp.status in (HTTP_OK, HTTP_PARTIAL_CONTENT):
                res.app_iter = self._make_app_iter()
                # See NOTE: swift_conn at top of file about this.
                res.swift_conn = self.source.resp.swift_conn
            if not res.environ:
                res.environ = {}
            res.environ['swift_x_timestamp'] = self.source.resp.getheader(
                'x-timestamp')
            res.accept_ranges = 'bytes'
            res.content_length = self.source.resp.getheader('Content-Length')
            if self.source.resp.getheader('Content-Type'):
                res.charset = None
                res.content_type = self.source.resp.getheader('Content-Type')
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

    :param server_type: one of 'account', 'container', or 'object'
    :param app: a proxy app
    :param ring: ring to get yield nodes from
    :param partition: ring partition to yield nodes for
    :param logger: a logger instance
    :param request: yielded nodes will be annotated with `use_replication`
        based on the `request` headers.
    :param node_iter: optional iterable of nodes to try. Useful if you
        want to filter or reorder the nodes.
    :param policy: an instance of :class:`BaseStoragePolicy`. This should be
        None for an account or container ring.
    """

    def __init__(self, server_type, app, ring, partition, logger, request,
                 node_iter=None, policy=None):
        self.server_type = server_type
        self.app = app
        self.ring = ring
        self.partition = partition
        self.logger = logger
        self.request = request

        part_nodes = ring.get_part_nodes(partition)
        if node_iter is None:
            node_iter = itertools.chain(
                part_nodes, ring.get_more_nodes(partition))
        self.num_primary_nodes = len(part_nodes)
        self.nodes_left = self.app.request_node_count(self.num_primary_nodes)
        self.expected_handoffs = self.nodes_left - self.num_primary_nodes

        # Use of list() here forcibly yanks the first N nodes (the primary
        # nodes) from node_iter, so the rest of its values are handoffs.
        self.primary_nodes = self.app.sort_nodes(
            list(itertools.islice(node_iter, self.num_primary_nodes)),
            policy=policy)
        self.handoff_iter = node_iter
        self._node_provider = None

    @property
    def primaries_left(self):
        return len(self.primary_nodes)

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
            self.logger.increment('%s.handoff_count' %
                                  self.server_type.lower())
            self.logger.warning(
                'Handoff requested (%d)' % handoffs)
            if (extra_handoffs == self.num_primary_nodes):
                # all the primaries were skipped, and handoffs didn't help
                self.logger.increment('%s.handoff_all_count' %
                                      self.server_type.lower())

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
        while self.primary_nodes:
            node = self.primary_nodes.pop(0)
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

    def _annotate_node(self, node):
        """
        Helper function to set use_replication dict value for a node by looking
        up the header value for x-backend-use-replication-network.

        :param node: node dictionary from the ring or node_iter.
        :returns: node dictionary with replication network enabled/disabled
        """
        # nodes may have come from a ring or a node_iter passed to the
        # constructor: be careful not to mutate them!
        return dict(node, use_replication=is_use_replication_network(
            self.request.headers))

    def __next__(self):
        node = None
        if self._node_provider:
            # give node provider the opportunity to inject a node
            node = self._node_provider()
        if not node:
            node = next(self._node_iter)
        return self._annotate_node(node)


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
        self._private_methods = None

    @property
    def logger(self):
        return self.app.logger

    @property
    def allowed_methods(self):
        if self._allowed_methods is None:
            self._allowed_methods = set()
            all_methods = inspect.getmembers(self, predicate=inspect.ismethod)
            for name, m in all_methods:
                if getattr(m, 'publicly_accessible', False):
                    self._allowed_methods.add(name)
        return self._allowed_methods

    @property
    def private_methods(self):
        if self._private_methods is None:
            self._private_methods = set()
            all_methods = inspect.getmembers(self, predicate=inspect.ismethod)
            for name, m in all_methods:
                if getattr(m, 'privately_accessible', False):
                    self._private_methods.add(name)
        return self._private_methods

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
        Create a dict of headers to be used in backend requests

        :param orig_req: the original request sent by the client to the proxy
        :param additional: additional headers to send to the backend
        :param transfer: If True, transfer headers from original client request
        :returns: a dictionary of headers
        """
        headers = HeaderKeyDict()
        if orig_req:
            headers.update((k.lower(), v)
                           for k, v in orig_req.headers.items()
                           if k.lower().startswith('x-backend-'))
            referer = orig_req.as_referer()
        else:
            referer = ''
        # additional headers can override x-backend-* headers from orig_req
        if additional:
            headers.update(additional)
        if orig_req and transfer:
            # transfer headers from orig_req can override additional headers
            self.transfer_headers(orig_req.headers, headers)
        headers.setdefault('x-timestamp', Timestamp.now().internal)
        # orig_req and additional headers cannot override the following...
        headers['x-trans-id'] = self.trans_id
        headers['connection'] = 'close'
        headers['user-agent'] = self.app.backend_user_agent
        headers['referer'] = referer
        return headers

    def account_info(self, account, req):
        """
        Get account information, and also verify that the account exists.

        :param account: native str name of the account to get the info for
        :param req: caller's HTTP request context object
        :returns: tuple of (account partition, account nodes, container_count)
                  or (None, None, None) if it does not exist
        """
        if req:
            env = getattr(req, 'environ', {})
        else:
            env = {}
        env.setdefault('swift.infocache', {})
        path_env = env.copy()
        path_env['PATH_INFO'] = "/v1/%s" % (str_to_wsgi(account),)

        info = get_account_info(path_env, self.app)
        if (not info
                or not is_success(info['status'])
                or not info.get('account_really_exists', True)):
            return None, None, None
        container_count = info['container_count']
        partition, nodes = self.app.account_ring.get_nodes(account)
        return partition, nodes, container_count

    def container_info(self, account, container, req):
        """
        Get container information and thusly verify container existence.
        This will also verify account existence.

        :param account: native-str account name for the container
        :param container: native-str container name to look up
        :param req: caller's HTTP request context object
        :returns: dict containing at least container partition ('partition'),
                  container nodes ('containers'), container read
                  acl ('read_acl'), container write acl ('write_acl'),
                  and container sync key ('sync_key').
                  Values are set to None if the container does not exist.
        """
        if req:
            env = getattr(req, 'environ', {})
        else:
            env = {}
        env.setdefault('swift.infocache', {})
        path_env = env.copy()
        path_env['PATH_INFO'] = "/v1/%s/%s" % (
            str_to_wsgi(account), str_to_wsgi(container))
        info = get_container_info(path_env, self.app)
        if not is_success(info.get('status')):
            info['partition'] = None
            info['nodes'] = None
        else:
            part, nodes = self.app.container_ring.get_nodes(account, container)
            info['partition'] = part
            info['nodes'] = nodes
        return info

    def _make_request(self, nodes, part, method, path, headers, query,
                      body, logger_thread_locals):
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
        :param body: byte string to use as the request body.
                     Try to keep it small.
        :param logger_thread_locals: The thread local values to be set on the
                                     self.logger to retain transaction
                                     logging information.
        :returns: a swob.Response object, or None if no responses were received
        """
        self.logger.thread_locals = logger_thread_locals
        if body:
            if not isinstance(body, bytes):
                raise TypeError('body must be bytes, not %s' % type(body))
            headers['Content-Length'] = str(len(body))
        for node in nodes:
            try:
                ip, port = get_ip_port(node, headers)
                start_node_timing = time.time()
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(
                        ip, port, node['device'], part, method, path,
                        headers=headers, query_string=query)
                    conn.node = node
                self.app.set_node_timing(node, time.time() - start_node_timing)
                if body:
                    with Timeout(self.app.node_timeout):
                        conn.send(body)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    if (self.app.check_response(node, self.server_type, resp,
                                                method, path)
                            and not is_informational(resp.status)):
                        return resp, resp.read(), node

            except (Exception, Timeout):
                self.app.exception_occurred(
                    node, self.server_type,
                    'Trying to %(method)s %(path)s' %
                    {'method': method, 'path': path})
        return None, None, None

    def make_requests(self, req, ring, part, method, path, headers,
                      query_string='', overrides=None, node_count=None,
                      node_iterator=None, body=None):
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
        :param body: byte string to use as the request body.
                     Try to keep it small.
        :returns: a swob.Response object
        """
        nodes = GreenthreadSafeIterator(node_iterator or NodeIter(
            self.server_type.lower(), self.app, ring, part, self.logger, req))
        node_number = node_count or len(ring.get_part_nodes(part))
        pile = GreenAsyncPile(node_number)

        for head in headers:
            pile.spawn(self._make_request, nodes, part, method, path,
                       head, query_string, body, self.logger.thread_locals)
        results = []
        statuses = []
        for resp, body, node in pile:
            if not is_useful_response(resp, node):
                continue
            results.append((resp.status, resp.reason, resp.getheaders(), body))
            statuses.append(resp.status)
            if self.have_quorum(statuses, node_number):
                break
        # give any pending requests *some* chance to finish
        finished_quickly = pile.waitall(self.app.post_quorum_timeout)
        for resp, body, node in finished_quickly:
            if not is_useful_response(resp, node):
                continue
            results.append((resp.status, resp.reason, resp.getheaders(), body))
            statuses.append(resp.status)
        while len(results) < node_number:
            results.append((HTTP_SERVICE_UNAVAILABLE, '', '', b''))
        statuses, reasons, resp_headers, bodies = zip(*results)
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
            self.logger.error('%(type)s returning 503 for %(statuses)s',
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
                    resp.headers['etag'] = normalize_etag(etag)
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
        resp = self.make_requests(Request.blank(str_to_wsgi('/v1' + path)),
                                  self.app.account_ring, partition, 'PUT',
                                  path, [headers] * len(nodes))
        if is_success(resp.status_int):
            self.logger.info('autocreate account %r', path)
            clear_info_cache(req.environ, account)
            return True
        else:
            self.logger.warning('Could not autocreate account %r', path)
            return False

    def GETorHEAD_base(self, req, server_type, node_iter, partition, path,
                       concurrency=1, policy=None):
        """
        Base handler for HTTP GET or HEAD requests.

        :param req: swob.Request object
        :param server_type: server type used in logging
        :param node_iter: an iterator to obtain nodes from
        :param partition: partition
        :param path: path for the request
        :param concurrency: number of requests to run concurrently
        :param policy: the policy instance, or None if Account or Container
        :returns: swob.Response object
        """
        backend_headers = self.generate_request_headers(
            req, additional=req.headers)

        handler = GetOrHeadHandler(self.app, req, self.server_type, node_iter,
                                   partition, path, backend_headers,
                                   concurrency, policy=policy,
                                   logger=self.logger)
        res = handler.get_working_response()

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
                self.logger.error(
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

    def _parse_listing_response(self, req, response):
        if not is_success(response.status_int):
            record_type = req.headers.get('X-Backend-Record-Type')
            self.logger.warning(
                'Failed to get container %s listing from %s: %s',
                record_type, req.path_qs, response.status_int)
            return None

        try:
            data = json.loads(response.body)
            if not isinstance(data, list):
                raise ValueError('not a list')
            return data
        except ValueError as err:
            record_type = response.headers.get('X-Backend-Record-Type')
            self.logger.error(
                'Problem with container %s listing response from %s: %r',
                record_type, req.path_qs, err)
            return None

    def _get_container_listing(self, req, account, container, headers=None,
                               params=None):
        """
        Fetch container listing from given `account/container`.

        :param req: original Request instance.
        :param account: account in which `container` is stored.
        :param container: container from which listing should be fetched.
        :param headers: extra headers to be included with the listing
            sub-request; these update the headers copied from the original
            request.
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
        self.logger.debug(
            'Get listing from %s %s' % (subreq.path_qs, headers))
        response = self.app.handle_request(subreq)
        data = self._parse_listing_response(subreq, response)
        return data, response

    def _parse_namespaces(self, req, listing, response):
        if listing is None:
            return None

        record_type = response.headers.get('x-backend-record-type')
        if record_type != 'shard':
            err = 'unexpected record type %r' % record_type
            self.logger.error("Failed to get shard ranges from %s: %s",
                              req.path_qs, err)
            return None

        try:
            # Note: a legacy container-server could return a list of
            # ShardRanges, but that's ok: namespaces just need 'name', 'lower'
            # and 'upper' keys. If we ever need to know we can look for a
            # 'x-backend-record-shard-format' header from newer container
            # servers.
            return [Namespace(data['name'], data['lower'], data['upper'])
                    for data in listing]
        except (ValueError, TypeError, KeyError) as err:
            self.logger.error(
                "Failed to get namespaces from %s: invalid data: %r",
                req.path_qs, err)
            return None
