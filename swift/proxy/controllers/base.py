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

import time
import functools
import inspect

from eventlet import spawn_n, GreenPile
from eventlet.queue import Queue, Empty, Full
from eventlet.timeout import Timeout

from swift.common.utils import normalize_timestamp, config_true_value, public
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import MAX_ACCOUNT_NAME_LENGTH
from swift.common.exceptions import ChunkReadTimeout, ConnectionTimeout
from swift.common.http import is_informational, is_success, is_redirection, \
    is_server_error, HTTP_OK, HTTP_PARTIAL_CONTENT, HTTP_MULTIPLE_CHOICES, \
    HTTP_BAD_REQUEST, HTTP_NOT_FOUND, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_INSUFFICIENT_STORAGE, HTTP_UNAUTHORIZED
from swift.common.swob import Request, Response


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
    return 'account/%s' % account


def get_container_memcache_key(account, container):
    return 'container/%s/%s' % (account, container)


def headers_to_container_info(headers, status_int=HTTP_OK):
    """
    Construct a cacheable dict of container info based on response headers.
    """
    headers = dict(headers)
    return {
        'status': status_int,
        'read_acl': headers.get('x-container-read'),
        'write_acl': headers.get('x-container-write'),
        'sync_key': headers.get('x-container-sync-key'),
        'count': headers.get('x-container-object-count'),
        'bytes': headers.get('x-container-bytes-used'),
        'versions': headers.get('x-versions-location'),
        'cors': {
            'allow_origin': headers.get(
                'x-container-meta-access-control-allow-origin'),
            'allow_headers': headers.get(
                'x-container-meta-access-control-allow-headers'),
            'expose_headers': headers.get(
                'x-container-meta-access-control-expose-headers'),
            'max_age': headers.get(
                'x-container-meta-access-control-max-age')
        },
        'meta': dict((key.lower()[17:], value)
                     for key, value in headers.iteritems()
                     if key.lower().startswith('x-container-meta-'))
    }


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
                                          controller.container_name)
            cors_info = container_info.get('cors', {})
            if not controller.is_origin_allowed(cors_info, req_origin):
                # invalid CORS request
                return Response(status=HTTP_UNAUTHORIZED)

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
                if header.startswith('x-container-meta') or \
                        header.startswith('x-object-meta'):
                    expose_headers.append(header.lower())
            if cors_info.get('expose_headers'):
                expose_headers.extend(
                    [a.strip()
                     for a in cors_info['expose_headers'].split(' ')
                     if a.strip()])
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


class Controller(object):
    """Base WSGI controller class for the proxy"""
    server_type = 'Base'

    # Ensure these are all lowercase
    pass_through_headers = []

    def __init__(self, app):
        self.account_name = None
        self.app = app
        self.trans_id = '-'
        self.allowed_methods = set()
        all_methods = inspect.getmembers(self, predicate=inspect.ismethod)
        for name, m in all_methods:
            if getattr(m, 'publicly_accessible', False):
                self.allowed_methods.add(name)

    def transfer_headers(self, src_headers, dst_headers):

        st = self.server_type.lower()
        x_remove = 'x-remove-%s-meta-' % st
        x_remove_read = 'x-remove-%s-read' % st
        x_remove_write = 'x-remove-%s-write' % st
        x_meta = 'x-%s-meta-' % st
        dst_headers.update((k.lower().replace('-remove', '', 1), '')
                           for k in src_headers
                           if k.lower().startswith(x_remove) or
                           k.lower() in (x_remove_read, x_remove_write))

        dst_headers.update((k.lower(), v)
                           for k, v in src_headers.iteritems()
                           if k.lower() in self.pass_through_headers or
                           k.lower().startswith(x_meta))

    def error_increment(self, node):
        """
        Handles incrementing error counts when talking to nodes.

        :param node: dictionary of node to increment the error count for
        """
        node['errors'] = node.get('errors', 0) + 1
        node['last_error'] = time.time()

    def error_occurred(self, node, msg):
        """
        Handle logging, and handling of errors.

        :param node: dictionary of node to handle errors for
        :param msg: error message
        """
        self.error_increment(node)
        self.app.logger.error(_('%(msg)s %(ip)s:%(port)s'),
                              {'msg': msg, 'ip': node['ip'],
                              'port': node['port']})

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
        if not 'errors' in node:
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

    def error_limit(self, node):
        """
        Mark a node as error limited.

        :param node: dictionary of node to error limit
        """
        node['errors'] = self.app.error_suppression_limit + 1
        node['last_error'] = time.time()

    def account_info(self, account, autocreate=False):
        """
        Get account information, and also verify that the account exists.

        :param account: name of the account to get the info for
        :returns: tuple of (account partition, account nodes, container_count)
                  or (None, None, None) if it does not exist
        """
        partition, nodes = self.app.account_ring.get_nodes(account)
        # 0 = no responses, 200 = found, 404 = not found, -1 = mixed responses
        if self.app.memcache:
            cache_key = get_account_memcache_key(account)
            cache_value = self.app.memcache.get(cache_key)
            if not isinstance(cache_value, dict):
                result_code = cache_value
                container_count = 0
            else:
                result_code = cache_value['status']
                container_count = cache_value['container_count']
            if result_code == HTTP_OK:
                return partition, nodes, container_count
            elif result_code == HTTP_NOT_FOUND and not autocreate:
                return None, None, None
        result_code = 0
        container_count = 0
        attempts_left = len(nodes)
        path = '/%s' % account
        headers = {'x-trans-id': self.trans_id, 'Connection': 'close'}
        iternodes = self.iter_nodes(partition, nodes, self.app.account_ring)
        while attempts_left > 0:
            try:
                node = iternodes.next()
            except StopIteration:
                break
            attempts_left -= 1
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                                        node['device'], partition, 'HEAD',
                                        path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    resp.read()
                    if is_success(resp.status):
                        result_code = HTTP_OK
                        container_count = int(
                            resp.getheader('x-account-container-count') or 0)
                        break
                    elif resp.status == HTTP_NOT_FOUND:
                        if result_code == 0:
                            result_code = HTTP_NOT_FOUND
                        elif result_code != HTTP_NOT_FOUND:
                            result_code = -1
                    elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.error_limit(node)
                        continue
                    else:
                        result_code = -1
            except (Exception, Timeout):
                self.exception_occurred(node, _('Account'),
                                        _('Trying to get account info for %s')
                                        % path)
        if result_code == HTTP_NOT_FOUND and autocreate:
            if len(account) > MAX_ACCOUNT_NAME_LENGTH:
                return None, None, None
            headers = {'X-Timestamp': normalize_timestamp(time.time()),
                       'X-Trans-Id': self.trans_id,
                       'Connection': 'close'}
            resp = self.make_requests(Request.blank('/v1' + path),
                                      self.app.account_ring, partition, 'PUT',
                                      path, [headers] * len(nodes))
            if not is_success(resp.status_int):
                self.app.logger.warning('Could not autocreate account %r' %
                                        path)
                return None, None, None
            result_code = HTTP_OK
        if self.app.memcache and result_code in (HTTP_OK, HTTP_NOT_FOUND):
            if result_code == HTTP_OK:
                cache_timeout = self.app.recheck_account_existence
            else:
                cache_timeout = self.app.recheck_account_existence * 0.1
            self.app.memcache.set(cache_key,
                                  {'status': result_code,
                                  'container_count': container_count},
                                  timeout=cache_timeout)
        if result_code == HTTP_OK:
            return partition, nodes, container_count
        return None, None, None

    def container_info(self, account, container, account_autocreate=False):
        """
        Get container information and thusly verify container existance.
        This will also make a call to account_info to verify that the
        account exists.

        :param account: account name for the container
        :param container: container name to look up
        :returns: dict containing at least container partition ('partition'),
                  container nodes ('containers'), container read
                  acl ('read_acl'), container write acl ('write_acl'),
                  and container sync key ('sync_key').
                  Values are set to None if the container does not exist.
        """
        part, nodes = self.app.container_ring.get_nodes(account, container)
        path = '/%s/%s' % (account, container)
        container_info = {'status': 0, 'read_acl': None,
                          'write_acl': None, 'sync_key': None,
                          'count': None, 'bytes': None,
                          'versions': None, 'partition': None,
                          'nodes': None}
        if self.app.memcache:
            cache_key = get_container_memcache_key(account, container)
            cache_value = self.app.memcache.get(cache_key)
            if isinstance(cache_value, dict):
                if 'container_size' in cache_value:
                    cache_value['count'] = cache_value['container_size']
                if is_success(cache_value['status']):
                    container_info.update(cache_value)
                    container_info['partition'] = part
                    container_info['nodes'] = nodes
                return container_info
        if not self.account_info(account, autocreate=account_autocreate)[1]:
            return container_info
        attempts_left = len(nodes)
        headers = {'x-trans-id': self.trans_id, 'Connection': 'close'}
        for node in self.iter_nodes(part, nodes, self.app.container_ring):
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                                        node['device'], part, 'HEAD',
                                        path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    resp.read()
                if is_success(resp.status):
                    container_info.update(
                        headers_to_container_info(resp.getheaders()))
                    break
                elif resp.status == HTTP_NOT_FOUND:
                    container_info['status'] = HTTP_NOT_FOUND
                else:
                    container_info['status'] = -1
                    if resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.error_limit(node)
            except (Exception, Timeout):
                self.exception_occurred(
                    node, _('Container'),
                    _('Trying to get container info for %s') % path)
            attempts_left -= 1
            if attempts_left <= 0:
                break
        if self.app.memcache:
            if container_info['status'] == HTTP_OK:
                self.app.memcache.set(
                    cache_key, container_info,
                    timeout=self.app.recheck_container_existence)
            elif container_info['status'] == HTTP_NOT_FOUND:
                self.app.memcache.set(
                    cache_key, container_info,
                    timeout=self.app.recheck_container_existence * 0.1)
        if container_info['status'] == HTTP_OK:
            container_info['partition'] = part
            container_info['nodes'] = nodes
        return container_info

    def iter_nodes(self, partition, nodes, ring):
        """
        Node iterator that will first iterate over the normal nodes for a
        partition and then the handoff partitions for the node.

        :param partition: partition to iterate nodes for
        :param nodes: list of node dicts from the ring
        :param ring: ring to get handoff nodes from
        """
        for node in nodes:
            if not self.error_limited(node):
                yield node
        handoffs = 0
        for node in ring.get_more_nodes(partition):
            if not self.error_limited(node):
                handoffs += 1
                if self.app.log_handoffs:
                    self.app.logger.increment('handoff_count')
                    self.app.logger.warning(
                        'Handoff requested (%d)' % handoffs)
                    if handoffs == len(nodes):
                        self.app.logger.increment('handoff_all_count')
                yield node

    def _make_request(self, nodes, part, method, path, headers, query,
                      logger_thread_locals):
        self.app.logger.thread_locals = logger_thread_locals
        for node in nodes:
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                                        node['device'], part, method, path,
                                        headers=headers, query_string=query)
                    conn.node = node
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    if not is_informational(resp.status) and \
                            not is_server_error(resp.status):
                        return resp.status, resp.reason, resp.read()
                    elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                        self.error_limit(node)
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

        :param headers: a list of dicts, where each dict represents one
                        backend request that should be made.
        :returns: a swob.Response object
        """
        start_nodes = ring.get_part_nodes(part)
        nodes = self.iter_nodes(part, start_nodes, ring)
        pile = GreenPile(len(start_nodes))
        for head in headers:
            pile.spawn(self._make_request, nodes, part, method, path,
                       head, query_string, self.app.logger.thread_locals)
        response = [resp for resp in pile if resp]
        while len(response) < len(start_nodes):
            response.append((HTTP_SERVICE_UNAVAILABLE, '', ''))
        statuses, reasons, bodies = zip(*response)
        return self.best_response(req, statuses, reasons, bodies,
                                  '%s %s' % (self.server_type, req.method))

    def best_response(self, req, statuses, reasons, bodies, server_type,
                      etag=None):
        """
        Given a list of responses from several servers, choose the best to
        return to the API.

        :param req: swob.Request object
        :param statuses: list of statuses returned
        :param reasons: list of reasons for each status
        :param bodies: bodies of each response
        :param server_type: type of server the responses came from
        :param etag: etag
        :returns: swob.Response object with the correct status, body, etc. set
        """
        resp = Response(request=req)
        if len(statuses):
            for hundred in (HTTP_OK, HTTP_MULTIPLE_CHOICES, HTTP_BAD_REQUEST):
                hstatuses = \
                    [s for s in statuses if hundred <= s < hundred + 100]
                if len(hstatuses) > len(statuses) / 2:
                    status = max(hstatuses)
                    status_index = statuses.index(status)
                    resp.status = '%s %s' % (status, reasons[status_index])
                    resp.body = bodies[status_index]
                    if etag:
                        resp.headers['etag'] = etag.strip('"')
                    return resp
        self.app.logger.error(_('%(type)s returning 503 for %(statuses)s'),
                              {'type': server_type, 'statuses': statuses})
        resp.status = '503 Internal Server Error'
        return resp

    @public
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
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
        try:
            src.swift_conn.close()
        except Exception:
            pass
        src.swift_conn = None
        try:
            while src.read(self.app.object_chunk_size):
                pass
        except Exception:
            pass
        try:
            src.close()
        except Exception:
            pass

    def is_good_source(self, src):
        """
        Indicates whether or not the request made to the backend found
        what it was looking for.
        """
        return is_success(src.status) or is_redirection(src.status)

    def GETorHEAD_base(self, req, server_type, partition, nodes, path,
                       attempts):
        """
        Base handler for HTTP GET or HEAD requests.

        :param req: swob.Request object
        :param server_type: server type
        :param partition: partition
        :param nodes: nodes
        :param path: path for the request
        :param attempts: number of attempts to try
        :returns: swob.Response object
        """
        statuses = []
        reasons = []
        bodies = []
        sources = []
        newest = config_true_value(req.headers.get('x-newest', 'f'))
        nodes = iter(nodes)
        while len(statuses) < attempts:
            try:
                node = nodes.next()
            except StopIteration:
                break
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    headers = dict(req.headers)
                    headers['Connection'] = 'close'
                    conn = http_connect(
                        node['ip'], node['port'], node['device'], partition,
                        req.method, path, headers=headers,
                        query_string=req.query_string)
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
                    self.close_swift_conn(possible_source)
                else:
                    statuses.append(possible_source.status)
                    reasons.append(possible_source.reason)
                    bodies.append('')
                    sources.append(possible_source)
                    if not newest:  # one good source is enough
                        break
            else:
                statuses.append(possible_source.status)
                reasons.append(possible_source.reason)
                bodies.append(possible_source.read())
                if possible_source.status == HTTP_INSUFFICIENT_STORAGE:
                    self.error_limit(node)
                elif is_server_error(possible_source.status):
                    self.error_occurred(node, _('ERROR %(status)d %(body)s '
                                                'From %(type)s Server') %
                                        {'status': possible_source.status,
                                         'body': bodies[-1][:1024],
                                         'type': server_type})
        if sources:
            sources.sort(key=source_key)
            source = sources.pop()
            for src in sources:
                self.close_swift_conn(src)
            res = Response(request=req, conditional_response=True)
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
            return res
        return self.best_response(req, statuses, reasons, bodies,
                                  '%s %s' % (server_type, req.method))

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
                self.container_info(self.account_name, self.container_name)
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

        # Always allow the x-auth-token header. This ensures
        # clients can always make a request to the resource.
        allow_headers = set()
        if cors.get('allow_headers'):
            allow_headers.update(
                [a.strip()
                 for a in cors['allow_headers'].split(' ')
                 if a.strip()])
        allow_headers.add('x-auth-token')

        # Populate the response with the CORS preflight headers
        headers['access-control-allow-origin'] = req_origin_value
        if cors.get('max_age') is not None:
            headers['access-control-max-age'] = cors.get('max_age')
        headers['access-control-allow-methods'] = \
            ', '.join(self.allowed_methods)
        headers['access-control-allow-headers'] = ', '.join(allow_headers)
        resp.headers = headers

        return resp
