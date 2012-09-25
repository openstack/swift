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

from eventlet import spawn_n, GreenPile, Timeout
from eventlet.queue import Queue, Empty, Full
from eventlet.timeout import Timeout
from webob.exc import status_map
from webob import Request, Response

from swift.common.utils import normalize_timestamp, TRUE_VALUES, public
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import MAX_ACCOUNT_NAME_LENGTH
from swift.common.exceptions import ChunkReadTimeout, ConnectionTimeout
from swift.common.http import is_informational, is_success, is_redirection, \
    is_server_error, HTTP_OK, HTTP_PARTIAL_CONTENT, HTTP_MULTIPLE_CHOICES, \
    HTTP_BAD_REQUEST, HTTP_NOT_FOUND, HTTP_SERVICE_UNAVAILABLE, \
    HTTP_INSUFFICIENT_STORAGE


def update_headers(response, headers):
    """
    Helper function to update headers in the response.

    :param response: webob.Response object
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


class Controller(object):
    """Base WSGI controller class for the proxy"""
    server_type = 'Base'

    # Ensure these are all lowercase
    pass_through_headers = []

    def __init__(self, app):
        self.account_name = None
        self.app = app
        self.trans_id = '-'

    def transfer_headers(self, src_headers, dst_headers):
        x_remove = 'x-remove-%s-meta-' % self.server_type.lower()
        x_meta = 'x-%s-meta-' % self.server_type.lower()
        dst_headers.update((k.lower().replace('-remove', '', 1), '')
                           for k in src_headers
                           if k.lower().startswith(x_remove))
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
            {'msg': msg, 'ip': node['ip'], 'port': node['port']})

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
                            node['device'], partition, 'HEAD', path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
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
                    _('Trying to get account info for %s') % path)
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
                self.app.logger.warning('Could not autocreate account %r' % \
                                        path)
                return None, None, None
            result_code = HTTP_OK
        if self.app.memcache and result_code in (HTTP_OK, HTTP_NOT_FOUND):
            if result_code == HTTP_OK:
                cache_timeout = self.app.recheck_account_existence
            else:
                cache_timeout = self.app.recheck_account_existence * 0.1
            self.app.memcache.set(cache_key,
                {'status': result_code, 'container_count': container_count},
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
        :returns: tuple of (container partition, container nodes, container
                  read acl, container write acl, container sync key) or (None,
                  None, None, None, None) if the container does not exist
        """
        partition, nodes = self.app.container_ring.get_nodes(
                account, container)
        path = '/%s/%s' % (account, container)
        if self.app.memcache:
            cache_key = get_container_memcache_key(account, container)
            cache_value = self.app.memcache.get(cache_key)
            if isinstance(cache_value, dict):
                status = cache_value['status']
                read_acl = cache_value['read_acl']
                write_acl = cache_value['write_acl']
                sync_key = cache_value.get('sync_key')
                versions = cache_value.get('versions')
                if status == HTTP_OK:
                    return partition, nodes, read_acl, write_acl, sync_key, \
                            versions
                elif status == HTTP_NOT_FOUND:
                    return None, None, None, None, None, None
        if not self.account_info(account, autocreate=account_autocreate)[1]:
            return None, None, None, None, None, None
        result_code = 0
        read_acl = None
        write_acl = None
        sync_key = None
        container_size = None
        versions = None
        attempts_left = len(nodes)
        headers = {'x-trans-id': self.trans_id, 'Connection': 'close'}
        iternodes = self.iter_nodes(partition, nodes, self.app.container_ring)
        while attempts_left > 0:
            try:
                node = iternodes.next()
            except StopIteration:
                break
            attempts_left -= 1
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], partition, 'HEAD', path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
                    if is_success(resp.status):
                        result_code = HTTP_OK
                        read_acl = resp.getheader('x-container-read')
                        write_acl = resp.getheader('x-container-write')
                        sync_key = resp.getheader('x-container-sync-key')
                        container_size = \
                            resp.getheader('X-Container-Object-Count')
                        versions = resp.getheader('x-versions-location')
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
                self.exception_occurred(node, _('Container'),
                    _('Trying to get container info for %s') % path)
        if self.app.memcache and result_code in (HTTP_OK, HTTP_NOT_FOUND):
            if result_code == HTTP_OK:
                cache_timeout = self.app.recheck_container_existence
            else:
                cache_timeout = self.app.recheck_container_existence * 0.1
            self.app.memcache.set(cache_key,
                                  {'status': result_code,
                                   'read_acl': read_acl,
                                   'write_acl': write_acl,
                                   'sync_key': sync_key,
                                   'container_size': container_size,
                                   'versions': versions},
                                  timeout=cache_timeout)
        if result_code == HTTP_OK:
            return partition, nodes, read_acl, write_acl, sync_key, versions
        return None, None, None, None, None, None

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
        :returns: a webob Response object
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

        :param req: webob.Request object
        :param statuses: list of statuses returned
        :param reasons: list of reasons for each status
        :param bodies: bodies of each response
        :param server_type: type of server the responses came from
        :param etag: etag
        :returns: webob.Response object with the correct status, body, etc. set
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
                    resp.content_type = 'text/html'
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

    def GETorHEAD_base(self, req, server_type, partition, nodes, path,
                       attempts):
        """
        Base handler for HTTP GET or HEAD requests.

        :param req: webob.Request object
        :param server_type: server type
        :param partition: partition
        :param nodes: nodes
        :param path: path for the request
        :param attempts: number of attempts to try
        :returns: webob.Response object
        """
        statuses = []
        reasons = []
        bodies = []
        source = None
        sources = []
        newest = req.headers.get('x-newest', 'f').lower() in TRUE_VALUES
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
                    conn = http_connect(node['ip'], node['port'],
                        node['device'], partition, req.method, path,
                        headers=headers,
                        query_string=req.query_string)
                with Timeout(self.app.node_timeout):
                    possible_source = conn.getresponse()
                    # See NOTE: swift_conn at top of file about this.
                    possible_source.swift_conn = conn
            except (Exception, Timeout):
                self.exception_occurred(node, server_type,
                    _('Trying to %(method)s %(path)s') %
                    {'method': req.method, 'path': req.path})
                continue
            if possible_source.status == HTTP_INSUFFICIENT_STORAGE:
                self.error_limit(node)
                continue
            if is_success(possible_source.status) or \
               is_redirection(possible_source.status):
                # 404 if we know we don't have a synced copy
                if not float(possible_source.getheader('X-PUT-Timestamp', 1)):
                    statuses.append(HTTP_NOT_FOUND)
                    reasons.append('')
                    bodies.append('')
                    possible_source.read()
                    continue
                if newest:
                    if sources:
                        ts = float(source.getheader('x-put-timestamp') or
                                   source.getheader('x-timestamp') or 0)
                        pts = float(
                            possible_source.getheader('x-put-timestamp') or
                            possible_source.getheader('x-timestamp') or 0)
                        if pts > ts:
                            sources.insert(0, possible_source)
                        else:
                            sources.append(possible_source)
                    else:
                        sources.insert(0, possible_source)
                    source = sources[0]
                    statuses.append(source.status)
                    reasons.append(source.reason)
                    bodies.append('')
                    continue
                else:
                    source = possible_source
                    break
            statuses.append(possible_source.status)
            reasons.append(possible_source.reason)
            bodies.append(possible_source.read())
            if is_server_error(possible_source.status):
                self.error_occurred(node, _('ERROR %(status)d %(body)s ' \
                    'From %(type)s Server') %
                    {'status': possible_source.status,
                    'body': bodies[-1][:1024], 'type': server_type})
        if source:
            if req.method == 'GET' and \
               source.status in (HTTP_OK, HTTP_PARTIAL_CONTENT):
                if newest:
                    # we need to close all hanging swift_conns
                    sources.pop(0)
                    for src in sources:
                        self.close_swift_conn(src)

                res = Response(request=req, conditional_response=True)
                res.app_iter = self._make_app_iter(node, source)
                # See NOTE: swift_conn at top of file about this.
                res.swift_conn = source.swift_conn
                update_headers(res, source.getheaders())
                # Used by container sync feature
                if res.environ is None:
                    res.environ = dict()
                res.environ['swift_x_timestamp'] = \
                    source.getheader('x-timestamp')
                update_headers(res, {'accept-ranges': 'bytes'})
                res.status = source.status
                res.content_length = source.getheader('Content-Length')
                if source.getheader('Content-Type'):
                    res.charset = None
                    res.content_type = source.getheader('Content-Type')
                return res
            elif is_success(source.status) or is_redirection(source.status):
                res = status_map[source.status](request=req)
                update_headers(res, source.getheaders())
                # Used by container sync feature
                if res.environ is None:
                    res.environ = dict()
                res.environ['swift_x_timestamp'] = \
                    source.getheader('x-timestamp')
                update_headers(res, {'accept-ranges': 'bytes'})
                res.content_length = source.getheader('Content-Length')
                if source.getheader('Content-Type'):
                    res.charset = None
                    res.content_type = source.getheader('Content-Type')
                return res
        return self.best_response(req, statuses, reasons, bodies,
                                  '%s %s' % (server_type, req.method))
