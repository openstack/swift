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

# NOTE: swift_conn
# You'll see swift_conn passed around a few places in this file. This is the
# source httplib connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

import os
import time
import functools
import inspect
import itertools
from swift import gettext_ as _
from urllib import quote

from eventlet import spawn_n, GreenPile
from eventlet.queue import Queue, Empty, Full
from eventlet.timeout import Timeout

from swift.common.wsgi import make_pre_authed_env
from swift.common.utils import normalize_timestamp, config_true_value, \
    public, split_path, list_from_csv, GreenthreadSafeIterator, \
    quorum_size
from swift.common.bufferedhttp import http_connect
from swift.common.exceptions import ChunkReadTimeout, ConnectionTimeout
from swift.common.http import is_informational, is_success, is_redirection, \
    is_server_error, HTTP_OK, HTTP_PARTIAL_CONTENT, HTTP_MULTIPLE_CHOICES, \
    HTTP_BAD_REQUEST, HTTP_NOT_FOUND, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_INSUFFICIENT_STORAGE, HTTP_UNAUTHORIZED
from swift.common.swob import Request, Response, HeaderKeyDict


def update_headers(response, headers):
    """
    Helper function to update headers in the response.

    :param response: swob.Response object
    :param headers: dictionary headers
    """
    if hasattr(headers, 'items'):
        headers = headers.items()
    for name, value in headers:
        if name == 'etag':
            response.headers[name] = value.replace('"', '')
        elif name not in ('date', 'content-length', 'content-type',
                          'connection', 'x-put-timestamp', 'x-delete-after'):
            response.headers[name] = value


def source_key(resp):
    """
    Provide the timestamp of the swift http response as a floating
    point value.  Used as a sort key.

    :param resp: httplib response object
    """
    return float(resp.getheader('x-put-timestamp') or
                 resp.getheader('x-timestamp') or 0)


def delay_denial(func):
    """
    Decorator to declare which methods should have any swift.authorize call
    delayed. This is so the method can load the Request object up with
    additional information that may be needed by the authorization system.

    :param func: function for which authorization will be delayed
    """
    func.delay_denial = True

    @functools.wraps(func)
    def wrapped(*a, **kw):
        return func(*a, **kw)
    return wrapped


def get_account_memcache_key(account):
    cache_key, env_key = _get_cache_key(account, None)
    return cache_key


def get_container_memcache_key(account, container):
    if not container:
        raise ValueError("container not provided")
    cache_key, env_key = _get_cache_key(account, container)
    return cache_key


def headers_to_account_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of account info based on response headers.
    """
    headers = dict((k.lower(), v) for k, v in dict(headers).iteritems())
    return {
        'status': status_int,
        # 'container_count' anomaly:
        # Previous code sometimes expects an int sometimes a string
        # Current code aligns to str and None, yet translates to int in
        # deprecated functions as needed
        'container_count': headers.get('x-account-container-count'),
        'total_object_count': headers.get('x-account-object-count'),
        'bytes': headers.get('x-account-bytes-used'),
        'meta': dict((key[15:], value)
                     for key, value in headers.iteritems()
                     if key.startswith('x-account-meta-'))
    }


def headers_to_container_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of container info based on response headers.
    """
    headers = dict((k.lower(), v) for k, v in dict(headers).iteritems())
    return {
        'status': status_int,
        'read_acl': headers.get('x-container-read'),
        'write_acl': headers.get('x-container-write'),
        'sync_key': headers.get('x-container-sync-key'),
        'object_count': headers.get('x-container-object-count'),
        'bytes': headers.get('x-container-bytes-used'),
        'versions': headers.get('x-versions-location'),
        'cors': {
            'allow_origin': headers.get(
                'x-container-meta-access-control-allow-origin'),
            'expose_headers': headers.get(
                'x-container-meta-access-control-expose-headers'),
            'max_age': headers.get(
                'x-container-meta-access-control-max-age')
        },
        'meta': dict((key[17:], value)
                     for key, value in headers.iteritems()
                     if key.startswith('x-container-meta-'))
    }


def headers_to_object_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of object info based on response headers.
    """
    headers = dict((k.lower(), v) for k, v in dict(headers).iteritems())
    info = {'status': status_int,
            'length': headers.get('content-length'),
            'type': headers.get('content-type'),
            'etag': headers.get('etag'),
            'meta': dict((key[14:], value)
                         for key, value in headers.iteritems()
                         if key.startswith('x-object-meta-'))
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

            # Expose,
            #  - simple response headers,
            #    http://www.w3.org/TR/cors/#simple-response-header
            #  - swift specific: etag, x-timestamp, x-trans-id
            #  - user metadata headers
            #  - headers provided by the user in
            #    x-container-meta-access-control-expose-headers
            expose_headers = ['cache-control', 'content-language',
                              'content-type', 'expires', 'last-modified',
                              'pragma', 'etag', 'x-timestamp', 'x-trans-id']
            for header in resp.headers:
                if header.startswith('X-Container-Meta') or \
                        header.startswith('X-Object-Meta'):
                    expose_headers.append(header.lower())
            if cors_info.get('expose_headers'):
                expose_headers.extend(
                    [header_line.strip()
                     for header_line in cors_info['expose_headers'].split(' ')
                     if header_line.strip()])
            resp.headers['Access-Control-Expose-Headers'] = \
                ', '.join(expose_headers)

            # The user agent won't process the response if the Allow-Origin
            # header isn't included
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
    Note: This call bypasses auth. Success does not imply that the
          request has authorization to the object.
    """
    (version, account, container, obj) = \
        split_path(path or env['PATH_INFO'], 4, 4, True)
    info = _get_object_info(app, env, account, container, obj,
                            swift_source=swift_source)
    if not info:
        info = headers_to_object_info({}, 0)
    return info


def get_container_info(env, app, swift_source=None):
    """
    Get the info structure for a container, based on env and app.
    This is useful to middlewares.
    Note: This call bypasses auth. Success does not imply that the
          request has authorization to the account.
    """
    (version, account, container, unused) = \
        split_path(env['PATH_INFO'], 3, 4, True)
    info = get_info(app, env, account, container, ret_not_found=True,
                    swift_source=swift_source)
    if not info:
        info = headers_to_container_info({}, 0)
    return info


def get_account_info(env, app, swift_source=None):
    """
    Get the info structure for an account, based on env and app.
    This is useful to middlewares.
    Note: This call bypasses auth. Success does not imply that the
          request has authorization to the container.
    """
    (version, account, _junk, _junk) = \
        split_path(env['PATH_INFO'], 2, 4, True)
    info = get_info(app, env, account, ret_not_found=True,
                    swift_source=swift_source)
    if not info:
        info = headers_to_account_info({}, 0)
    if info.get('container_count') is None:
        info['container_count'] = 0
    else:
        info['container_count'] = int(info['container_count'])
    return info


def _get_cache_key(account, container):
    """
    Get the keys for both memcache (cache_key) and env (env_key)
    where info about accounts and containers is cached
    :param   account: The name of the account
    :param container: The name of the container (or None if account)
    :returns a tuple of (cache_key, env_key)
    """

    if container:
        cache_key = 'container/%s/%s' % (account, container)
    else:
        cache_key = 'account/%s' % account
    # Use a unique environment cache key per account and one container.
    # This allows caching both account and container and ensures that when we
    # copy this env to form a new request, it won't accidentally reuse the
    # old container or account info
    env_key = 'swift.%s' % cache_key
    return cache_key, env_key


def get_object_env_key(account, container, obj):
    """
    Get the keys for env (env_key) where info about object is cached
    :param   account: The name of the account
    :param container: The name of the container
    :param obj: The name of the object
    :returns a string env_key
    """
    env_key = 'swift.object/%s/%s/%s' % (account,
                                         container, obj)
    return env_key


def _set_info_cache(app, env, account, container, resp):
    """
    Cache info in both memcache and env.

    Caching is used to avoid unnecessary calls to account & container servers.
    This is a private function that is being called by GETorHEAD_base and
    by clear_info_cache.
    Any attempt to GET or HEAD from the container/account server should use
    the GETorHEAD_base interface which would than set the cache.

    :param  app: the application object
    :param  account: the unquoted account name
    :param  container: the unquoted containr name or None
    :param resp: the response received or None if info cache should be cleared
    """

    if container:
        cache_time = app.recheck_container_existence
    else:
        cache_time = app.recheck_account_existence
    cache_key, env_key = _get_cache_key(account, container)

    if resp:
        if resp.status_int == HTTP_NOT_FOUND:
            cache_time *= 0.1
        elif not is_success(resp.status_int):
            cache_time = None
    else:
        cache_time = None

    # Next actually set both memcache and the env chache
    memcache = getattr(app, 'memcache', None) or env.get('swift.cache')
    if not cache_time:
        env.pop(env_key, None)
        if memcache:
            memcache.delete(cache_key)
        return

    if container:
        info = headers_to_container_info(resp.headers, resp.status_int)
    else:
        info = headers_to_account_info(resp.headers, resp.status_int)
    if memcache:
        memcache.set(cache_key, info, time=cache_time)
    env[env_key] = info


def _set_object_info_cache(app, env, account, container, obj, resp):
    """
    Cache object info env. Do not cache object informations in
    memcache. This is an intentional omission as it would lead
    to cache pressure. This is a per-request cache.

    Caching is used to avoid unnecessary calls to object servers.
    This is a private function that is being called by GETorHEAD_base.
    Any attempt to GET or HEAD from the object server should use
    the GETorHEAD_base interface which would then set the cache.

    :param  app: the application object
    :param  account: the unquoted account name
    :param  container: the unquoted container name or None
    :param  object: the unquoted object name or None
    :param resp: the response received or None if info cache should be cleared
    """

    env_key = get_object_env_key(account, container, obj)

    if not resp:
        env.pop(env_key, None)
        return

    info = headers_to_object_info(resp.headers, resp.status_int)
    env[env_key] = info


def clear_info_cache(app, env, account, container=None):
    """
    Clear the cached info in both memcache and env

    :param  app: the application object
    :param  account: the account name
    :param  container: the containr name or None if setting info for containers
    """
    _set_info_cache(app, env, account, container, None)


def _get_info_cache(app, env, account, container=None):
    """
    Get the cached info from env or memcache (if used) in that order
    Used for both account and container info
    A private function used by get_info

    :param  app: the application object
    :param  env: the environment used by the current request
    :returns the cached info or None if not cached
    """

    cache_key, env_key = _get_cache_key(account, container)
    if env_key in env:
        return env[env_key]
    memcache = getattr(app, 'memcache', None) or env.get('swift.cache')
    if memcache:
        info = memcache.get(cache_key)
        if info:
            env[env_key] = info
        return info
    return None


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
    # Note that Request.blank expects quoted path
    return Request.blank(quote(path), environ=newenv)


def get_info(app, env, account, container=None, ret_not_found=False,
             swift_source=None):
    """
    Get the info about accounts or containers

    Note: This call bypasses auth. Success does not imply that the
          request has authorization to the info.

    :param app: the application object
    :param env: the environment used by the current request
    :param account: The unquoted name of the account
    :param container: The unquoted name of the container (or None if account)
    :returns: the cached info or None if cannot be retrieved
    """
    info = _get_info_cache(app, env, account, container)
    if info:
        if ret_not_found or is_success(info['status']):
            return info
        return None
    # Not in cache, let's try the account servers
    path = '/v1/%s' % account
    if container:
        # Stop and check if we have an account?
        if not get_info(app, env, account):
            return None
        path += '/' + container

    req = _prepare_pre_auth_info_request(
        env, path, (swift_source or 'GET_INFO'))
    # Whenever we do a GET/HEAD, the GETorHEAD_base will set the info in
    # the environment under environ[env_key] and in memcache. We will
    # pick the one from environ[env_key] and use it to set the caller env
    resp = req.get_response(app)
    cache_key, env_key = _get_cache_key(account, container)
    try:
        info = resp.environ[env_key]
        env[env_key] = info
        if ret_not_found or is_success(info['status']):
            return info
    except (KeyError, AttributeError):
        pass
    return None


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
    env_key = get_object_env_key(account, container, obj)
    info = env.get(env_key)
    if info:
        return info
    # Not in cached, let's try the object servers
    path = '/v1/%s/%s/%s' % (account, container, obj)
    req = _prepare_pre_auth_info_request(env, path, swift_source)
    # Whenever we do a GET/HEAD, the GETorHEAD_base will set the info in
    # the environment under environ[env_key]. We will
    # pick the one from environ[env_key] and use it to set the caller env
    resp = req.get_response(app)
    try:
        info = resp.environ[env_key]
        env[env_key] = info
        return info
    except (KeyError, AttributeError):
        pass
    return None


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

        x_meta = 'x-%s-meta-' % st
        dst_headers.update((k.lower(), v)
                           for k, v in src_headers.iteritems()
                           if k.lower() in self.pass_through_headers or
                           k.lower().startswith(x_meta))

    def generate_request_headers(self, orig_req=None, additional=None,
                                 transfer=False):
        """
        Create a list of headers to be used in backend requets

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
        headers.setdefault('x-timestamp', normalize_timestamp(time.time()))
        if orig_req:
            referer = orig_req.as_referer()
        else:
            referer = ''
        headers['x-trans-id'] = self.trans_id
        headers['connection'] = 'close'
        headers['user-agent'] = 'proxy-server %s' % os.getpid()
        headers['referer'] = referer
        return headers

    def error_occurred(self, node, msg):
        """
        Handle logging, and handling of errors.

        :param node: dictionary of node to handle errors for
        :param msg: error message
        """
        node['errors'] = node.get('errors', 0) + 1
        node['last_error'] = time.time()
        self.app.logger.error(_('%(msg)s %(ip)s:%(port)s/%(device)s'),
                              {'msg': msg, 'ip': node['ip'],
                              'port': node['port'], 'device': node['device']})

    def exception_occurred(self, node, typ, additional_info):
        """
        Handle logging of generic exceptions.

        :param node: dictionary of node to log the error for
        :param typ: server type
        :param additional_info: additional information to log
        """
        self.app.logger.exception(
            _('ERROR with %(type)s server %(ip)s:%(port)s/%(device)s re: '
              '%(info)s'),
            {'type': typ, 'ip': node['ip'], 'port': node['port'],
             'device': node['device'], 'info': additional_info})

    def error_limited(self, node):
        """
        Check if the node is currently error limited.

        :param node: dictionary of node to check
        :returns: True if error limited, False otherwise
        """
        now = time.time()
        if 'errors' not in node:
            return False
        if 'last_error' in node and node['last_error'] < \
                now - self.app.error_suppression_interval:
            del node['last_error']
            if 'errors' in node:
                del node['errors']
            return False
        limited = node['errors'] > self.app.error_suppression_limit
        if limited:
            self.app.logger.debug(
                _('Node error limited %(ip)s:%(port)s (%(device)s)'), node)
        return limited

    def error_limit(self, node, msg):
        """
        Mark a node as error limited. This immediately pretends the
        node received enough errors to trigger error suppression. Use
        this for errors like Insufficient Storage. For other errors
        use :func:`error_occurred`.

        :param node: dictionary of node to error limit
        :param msg: error message
        """
        node['errors'] = self.app.error_suppression_limit + 1
        node['last_error'] = time.time()
        self.app.logger.error(_('%(msg)s %(ip)s:%(port)s/%(device)s'),
                              {'msg': msg, 'ip': node['ip'],
                              'port': node['port'], 'device': node['device']})

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
        info = get_info(self.app, env, account)
        if not info:
            return None, None, None
        if info.get('container_count') is None:
            container_count = 0
        else:
            container_count = int(info['container_count'])
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
        info = get_info(self.app, env, account, container)
        if not info:
            info = headers_to_container_info({}, 0)
            info['partition'] = None
            info['nodes'] = None
        else:
            info['partition'] = part
            info['nodes'] = nodes
        return info

    def iter_nodes(self, ring, partition, node_iter=None):
        """
        Yields nodes for a ring partition, skipping over error
        limited nodes and stopping at the configurable number of
        nodes. If a node yielded subsequently gets error limited, an
        extra node will be yielded to take its place.

        Note that if you're going to iterate over this concurrently from
        multiple greenthreads, you'll want to use a
        swift.common.utils.GreenthreadSafeIterator to serialize access.
        Otherwise, you may get ValueErrors from concurrent access. (You also
        may not, depending on how logging is configured, the vagaries of
        socket IO and eventlet, and the phase of the moon.)

        :param ring: ring to get yield nodes from
        :param partition: ring partition to yield nodes for
        :param node_iter: optional iterable of nodes to try. Useful if you
            want to filter or reorder the nodes.
        """
        part_nodes = ring.get_part_nodes(partition)
        if node_iter is None:
            node_iter = itertools.chain(part_nodes,
                                        ring.get_more_nodes(partition))
        num_primary_nodes = len(part_nodes)

        # Use of list() here forcibly yanks the first N nodes (the primary
        # nodes) from node_iter, so the rest of its values are handoffs.
        primary_nodes = self.app.sort_nodes(
            list(itertools.islice(node_iter, num_primary_nodes)))
        handoff_nodes = node_iter
        nodes_left = self.app.request_node_count(ring)

        for node in primary_nodes:
            if not self.error_limited(node):
                yield node
                if not self.error_limited(node):
                    nodes_left -= 1
                    if nodes_left <= 0:
                        return

        handoffs = 0
        for node in handoff_nodes:
            if not self.error_limited(node):
                handoffs += 1
                if self.app.log_handoffs:
                    self.app.logger.increment('handoff_count')
                    self.app.logger.warning(
                        'Handoff requested (%d)' % handoffs)
                    if handoffs == len(primary_nodes):
                        self.app.logger.increment('handoff_all_count')
                yield node
                if not self.error_limited(node):
                    nodes_left -= 1
                    if nodes_left <= 0:
                        return

    def _make_request(self, nodes, part, method, path, headers, query,
                      logger_thread_locals):
        """
        Sends an HTTP request to a single node and aggregates the result.
        It attempts the primary node, then iterates over the handoff nodes
        as needed.

        :param nodes: an iterator of the backend server and handoff servers
        :param part: the partition number
        :param method: the method to send to the backend
        :param path: the path to send to the backend
        :param headers: a list of dicts, where each dict represents one
                        backend request that should be made.
        :param query: query string to send to the backend.
        :param logger_thread_locals: The thread local values to be set on the
                                     self.app.logger to retain transaction
                                     logging information.
        :returns: a swob.Response object
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
                        self.error_limit(node, _('ERROR Insufficient Storage'))
            except (Exception, Timeout):
                self.exception_occurred(node, self.server_type,
                                        _('Trying to %(method)s %(path)s') %
                                        {'method': method, 'path': path})

    def make_requests(self, req, ring, part, method, path, headers,
                      query_string=''):
        """
        Sends an HTTP request to multiple nodes and aggregates the results.
        It attempts the primary nodes concurrently, then iterates over the
        handoff nodes as needed.

        :param req: a request sent by the client
        :param ring: the ring used for finding backend servers
        :param part: the partition number
        :param method: the method to send to the backend
        :param path: the path to send to the backend
        :param headers: a list of dicts, where each dict represents one
                        backend request that should be made.
        :param query_string: optional query string to send to the backend
        :returns: a swob.Response object
        """
        start_nodes = ring.get_part_nodes(part)
        nodes = GreenthreadSafeIterator(self.iter_nodes(ring, part))
        pile = GreenPile(len(start_nodes))
        for head in headers:
            pile.spawn(self._make_request, nodes, part, method, path,
                       head, query_string, self.app.logger.thread_locals)
        response = [resp for resp in pile if resp]
        while len(response) < len(start_nodes):
            response.append((HTTP_SERVICE_UNAVAILABLE, '', '', ''))
        statuses, reasons, resp_headers, bodies = zip(*response)
        return self.best_response(req, statuses, reasons, bodies,
                                  '%s %s' % (self.server_type, req.method),
                                  headers=resp_headers)

    def best_response(self, req, statuses, reasons, bodies, server_type,
                      etag=None, headers=None):
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
        :returns: swob.Response object with the correct status, body, etc. set
        """
        resp = Response(request=req)
        if len(statuses):
            for hundred in (HTTP_OK, HTTP_MULTIPLE_CHOICES, HTTP_BAD_REQUEST):
                hstatuses = \
                    [s for s in statuses if hundred <= s < hundred + 100]
                if len(hstatuses) >= quorum_size(len(statuses)):
                    status = max(hstatuses)
                    status_index = statuses.index(status)
                    resp.status = '%s %s' % (status, reasons[status_index])
                    resp.body = bodies[status_index]
                    if headers:
                        update_headers(resp, headers[status_index])
                    if etag:
                        resp.headers['etag'] = etag.strip('"')
                    return resp
        self.app.logger.error(_('%(type)s returning 503 for %(statuses)s'),
                              {'type': server_type, 'statuses': statuses})
        resp.status = '503 Internal Server Error'
        return resp

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

    def _make_app_iter_reader(self, node, source, queue, logger_thread_locals):
        """
        Reads from the source and places data in the queue. It expects
        something else be reading from the queue and, if nothing does within
        self.app.client_timeout seconds, the process will be aborted.

        :param node: The node dict that the source is connected to, for
                     logging/error-limiting purposes.
        :param source: The httplib.Response object to read from.
        :param queue: The eventlet.queue.Queue to place read source data into.
        :param logger_thread_locals: The thread local values to be set on the
                                     self.app.logger to retain transaction
                                     logging information.
        """
        self.app.logger.thread_locals = logger_thread_locals
        success = True
        try:
            try:
                while True:
                    with ChunkReadTimeout(self.app.node_timeout):
                        chunk = source.read(self.app.object_chunk_size)
                    if not chunk:
                        break
                    queue.put(chunk, timeout=self.app.client_timeout)
            except Full:
                self.app.logger.warn(
                    _('Client did not read from queue within %ss') %
                    self.app.client_timeout)
                self.app.logger.increment('client_timeouts')
                success = False
            except (Exception, Timeout):
                self.exception_occurred(node, _('Object'),
                                        _('Trying to read during GET'))
                success = False
        finally:
            # Ensure the queue getter gets a terminator.
            queue.resize(2)
            queue.put(success)
            # Close-out the connection as best as possible.
            if getattr(source, 'swift_conn', None):
                self.close_swift_conn(source)

    def _make_app_iter(self, node, source):
        """
        Returns an iterator over the contents of the source (via its read
        func).  There is also quite a bit of cleanup to ensure garbage
        collection works and the underlying socket of the source is closed.

        :param source: The httplib.Response object this iterator should read
                       from.
        :param node: The node the source is reading from, for logging purposes.
        """
        try:
            # Spawn reader to read from the source and place in the queue.
            # We then drop any reference to the source or node, for garbage
            # collection purposes.
            queue = Queue(1)
            spawn_n(self._make_app_iter_reader, node, source, queue,
                    self.app.logger.thread_locals)
            source = node = None
            while True:
                chunk = queue.get(timeout=self.app.node_timeout)
                if isinstance(chunk, bool):  # terminator
                    success = chunk
                    if not success:
                        raise Exception(_('Failed to read all data'
                                          ' from the source'))
                    break
                yield chunk
        except Empty:
            raise ChunkReadTimeout()
        except (GeneratorExit, Timeout):
            self.app.logger.warn(_('Client disconnected on read'))
        except Exception:
            self.app.logger.exception(_('Trying to send to client'))
            raise

    def close_swift_conn(self, src):
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

    def is_good_source(self, src):
        """
        Indicates whether or not the request made to the backend found
        what it was looking for.

        :param src: the response from the backend
        :returns: True if found, False if not
        """
        return is_success(src.status) or is_redirection(src.status)

    def autocreate_account(self, env, account):
        """
        Autocreate an account

        :param env: the environment of the request leading to this autocreate
        :param account: the unquoted account name
        """
        partition, nodes = self.app.account_ring.get_nodes(account)
        path = '/%s' % account
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'X-Trans-Id': self.trans_id,
                   'Connection': 'close'}
        resp = self.make_requests(Request.blank('/v1' + path),
                                  self.app.account_ring, partition, 'PUT',
                                  path, [headers] * len(nodes))
        if is_success(resp.status_int):
            self.app.logger.info('autocreate account %r' % path)
            clear_info_cache(self.app, env, account)
        else:
            self.app.logger.warning('Could not autocreate account %r' % path)

    def GETorHEAD_base(self, req, server_type, ring, partition, path):
        """
        Base handler for HTTP GET or HEAD requests.

        :param req: swob.Request object
        :param server_type: server type
        :param ring: the ring to obtain nodes from
        :param partition: partition
        :param path: path for the request
        :returns: swob.Response object
        """
        statuses = []
        reasons = []
        bodies = []
        source_headers = []
        sources = []
        newest = config_true_value(req.headers.get('x-newest', 'f'))
        headers = self.generate_request_headers(req, additional=req.headers)
        for node in self.iter_nodes(ring, partition):
            start_node_timing = time.time()
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(
                        node['ip'], node['port'], node['device'], partition,
                        req.method, path, headers=headers,
                        query_string=req.query_string)
                self.app.set_node_timing(node, time.time() - start_node_timing)
                with Timeout(self.app.node_timeout):
                    possible_source = conn.getresponse()
                    # See NOTE: swift_conn at top of file about this.
                    possible_source.swift_conn = conn
            except (Exception, Timeout):
                self.exception_occurred(
                    node, server_type, _('Trying to %(method)s %(path)s') %
                    {'method': req.method, 'path': req.path})
                continue
            if self.is_good_source(possible_source):
                # 404 if we know we don't have a synced copy
                if not float(possible_source.getheader('X-PUT-Timestamp', 1)):
                    statuses.append(HTTP_NOT_FOUND)
                    reasons.append('')
                    bodies.append('')
                    source_headers.append('')
                    self.close_swift_conn(possible_source)
                else:
                    statuses.append(possible_source.status)
                    reasons.append(possible_source.reason)
                    bodies.append('')
                    source_headers.append('')
                    sources.append((possible_source, node))
                    if not newest:  # one good source is enough
                        break
            else:
                statuses.append(possible_source.status)
                reasons.append(possible_source.reason)
                bodies.append(possible_source.read())
                source_headers.append(possible_source.getheaders())
                if possible_source.status == HTTP_INSUFFICIENT_STORAGE:
                    self.error_limit(node, _('ERROR Insufficient Storage'))
                elif is_server_error(possible_source.status):
                    self.error_occurred(node, _('ERROR %(status)d %(body)s '
                                                'From %(type)s Server') %
                                        {'status': possible_source.status,
                                         'body': bodies[-1][:1024],
                                         'type': server_type})
        res = None
        if sources:
            sources.sort(key=lambda s: source_key(s[0]))
            source, node = sources.pop()
            for src, _junk in sources:
                self.close_swift_conn(src)
            res = Response(request=req)
            if req.method == 'GET' and \
                    source.status in (HTTP_OK, HTTP_PARTIAL_CONTENT):
                res.app_iter = self._make_app_iter(node, source)
                # See NOTE: swift_conn at top of file about this.
                res.swift_conn = source.swift_conn
            res.status = source.status
            update_headers(res, source.getheaders())
            if not res.environ:
                res.environ = {}
            res.environ['swift_x_timestamp'] = \
                source.getheader('x-timestamp')
            res.accept_ranges = 'bytes'
            res.content_length = source.getheader('Content-Length')
            if source.getheader('Content-Type'):
                res.charset = None
                res.content_type = source.getheader('Content-Type')
        if not res:
            res = self.best_response(req, statuses, reasons, bodies,
                                     '%s %s' % (server_type, req.method),
                                     headers=source_headers)
        try:
            (account, container) = split_path(req.path_info, 1, 2)
            _set_info_cache(self.app, req.environ, account, container, res)
        except ValueError:
            pass
        try:
            (account, container, obj) = split_path(req.path_info, 3, 3, True)
            _set_object_info_cache(self.app, req.environ, account,
                                   container, obj, res)
        except ValueError:
            pass
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

        # Allow all headers requested in the request. The CORS
        # specification does leave the door open for this, as mentioned in
        # http://www.w3.org/TR/cors/#resource-preflight-requests
        # Note: Since the list of headers can be unbounded
        # simply returning headers can be enough.
        allow_headers = set()
        if req.headers.get('Access-Control-Request-Headers'):
            allow_headers.update(
                list_from_csv(req.headers['Access-Control-Request-Headers']))

        # Populate the response with the CORS preflight headers
        headers['access-control-allow-origin'] = req_origin_value
        if cors.get('max_age') is not None:
            headers['access-control-max-age'] = cors.get('max_age')
        headers['access-control-allow-methods'] = \
            ', '.join(self.allowed_methods)
        if allow_headers:
            headers['access-control-allow-headers'] = ', '.join(allow_headers)
        resp.headers = headers

        return resp
