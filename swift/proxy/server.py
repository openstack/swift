# Copyright (c) 2010 OpenStack, LLC.
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

from __future__ import with_statement
try:
    import simplejson as json
except ImportError:
    import json
import mimetypes
import os
import re
import time
import traceback
from ConfigParser import ConfigParser
from datetime import datetime
from urllib import unquote, quote
import uuid
import functools
from gettext import gettext as _
from hashlib import md5

from eventlet import sleep
from eventlet.timeout import Timeout
from webob.exc import HTTPBadRequest, HTTPMethodNotAllowed, \
    HTTPNotFound, HTTPPreconditionFailed, \
    HTTPRequestTimeout, HTTPServiceUnavailable, \
    HTTPUnprocessableEntity, HTTPRequestEntityTooLarge, HTTPServerError, \
    status_map
from webob import Request, Response

from swift.common.ring import Ring
from swift.common.utils import get_logger, normalize_timestamp, split_path, \
    cache_from_env
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_metadata, check_object_creation, \
    check_utf8, CONTAINER_LISTING_LIMIT, MAX_ACCOUNT_NAME_LENGTH, \
    MAX_CONTAINER_NAME_LENGTH, MAX_FILE_SIZE
from swift.common.exceptions import ChunkReadTimeout, \
    ChunkWriteTimeout, ConnectionTimeout


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
                          'connection', 'x-timestamp', 'x-put-timestamp'):
            response.headers[name] = value


def public(func):
    """
    Decorator to declare which methods are public accessible as HTTP requests

    :param func: function to make public
    """
    func.publicly_accessible = True

    @functools.wraps(func)
    def wrapped(*a, **kw):
        return func(*a, **kw)
    return wrapped


def delay_denial(func):
    """
    Decorator to declare which methods should have any swift.authorize call
    delayed. This is so the method can load the Request object up with
    additional information that may be needed by the authorization system.

    :param func: function to delay authorization on
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


class SegmentedIterable(object):
    """
    Iterable that returns the object contents for a segmented object in Swift.

    If set, the response's `bytes_transferred` value will be updated (used to
    log the size of the request). Also, if there's a failure that cuts the
    transfer short, the response's `status_int` will be updated (again, just
    for logging since the original status would have already been sent to the
    client).

    :param controller: The ObjectController instance to work with.
    :param container: The container the object segments are within.
    :param listing: The listing of object segments to iterate over; this may
                    be an iterator or list that returns dicts with 'name' and
                    'bytes' keys.
    :param response: The webob.Response this iterable is associated with, if
                     any (default: None)
    """

    def __init__(self, controller, container, listing, response=None):
        self.controller = controller
        self.container = container
        self.listing = iter(listing)
        self.segment = -1
        self.segment_dict = None
        self.segment_peek = None
        self.seek = 0
        self.segment_iter = None
        self.position = 0
        self.response = response
        if not self.response:
            self.response = Response()
        self.next_get_time = 0

    def _load_next_segment(self):
        """
        Loads the self.segment_iter with the next object segment's contents.

        :raises: StopIteration when there are no more object segments.
        """
        try:
            self.segment += 1
            self.segment_dict = self.segment_peek or self.listing.next()
            self.segment_peek = None
            partition, nodes = self.controller.app.object_ring.get_nodes(
                self.controller.account_name, self.container,
                self.segment_dict['name'])
            path = '/%s/%s/%s' % (self.controller.account_name, self.container,
                self.segment_dict['name'])
            req = Request.blank(path)
            if self.seek:
                req.range = 'bytes=%s-' % self.seek
                self.seek = 0
            if self.segment > 10:
                sleep(max(self.next_get_time - time.time(), 0))
                self.next_get_time = time.time() + 1
            resp = self.controller.GETorHEAD_base(req, 'Object', partition,
                self.controller.iter_nodes(partition, nodes,
                self.controller.app.object_ring), path,
                self.controller.app.object_ring.replica_count)
            if resp.status_int // 100 != 2:
                raise Exception('Could not load object segment %s: %s' % (path,
                    resp.status_int))
            self.segment_iter = resp.app_iter
        except StopIteration:
            raise
        except Exception, err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception('ERROR: While processing '
                    'manifest /%s/%s/%s %s' % (self.controller.account_name,
                    self.controller.container_name,
                    self.controller.object_name, self.controller.trans_id))
                err.swift_logged = True
                self.response.status_int = 503
            raise

    def next(self):
        return iter(self).next()

    def __iter__(self):
        """ Standard iterator function that returns the object's contents. """
        try:
            while True:
                if not self.segment_iter:
                    self._load_next_segment()
                while True:
                    with ChunkReadTimeout(self.controller.app.node_timeout):
                        try:
                            chunk = self.segment_iter.next()
                            break
                        except StopIteration:
                            self._load_next_segment()
                self.position += len(chunk)
                self.response.bytes_transferred = getattr(self.response,
                    'bytes_transferred', 0) + len(chunk)
                yield chunk
        except StopIteration:
            raise
        except Exception, err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception('ERROR: While processing '
                    'manifest /%s/%s/%s %s' % (self.controller.account_name,
                    self.controller.container_name,
                    self.controller.object_name, self.controller.trans_id))
                err.swift_logged = True
                self.response.status_int = 503
            raise

    def app_iter_range(self, start, stop):
        """
        Non-standard iterator function for use with Webob in serving Range
        requests more quickly. This will skip over segments and do a range
        request on the first segment to return data from, if needed.

        :param start: The first byte (zero-based) to return. None for 0.
        :param stop: The last byte (zero-based) to return. None for end.
        """
        try:
            if start:
                self.segment_peek = self.listing.next()
                while start >= self.position + self.segment_peek['bytes']:
                    self.segment += 1
                    self.position += self.segment_peek['bytes']
                    self.segment_peek = self.listing.next()
                self.seek = start - self.position
            else:
                start = 0
            if stop is not None:
                length = stop - start
            else:
                length = None
            for chunk in self:
                if length is not None:
                    length -= len(chunk)
                    if length < 0:
                        # Chop off the extra:
                        self.response.bytes_transferred = \
                           getattr(self.response, 'bytes_transferred', 0) \
                           + length
                        yield chunk[:length]
                        break
                yield chunk
        except StopIteration:
            raise
        except Exception, err:
            if not getattr(err, 'swift_logged', False):
                self.controller.app.logger.exception('ERROR: While processing '
                    'manifest /%s/%s/%s %s' % (self.controller.account_name,
                    self.controller.container_name,
                    self.controller.object_name, self.controller.trans_id))
                err.swift_logged = True
                self.response.status_int = 503
            raise


class Controller(object):
    """Base WSGI controller class for the proxy"""

    def __init__(self, app):
        self.account_name = None
        self.app = app
        self.trans_id = '-'

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
            _('ERROR with %(type)s server %(ip)s:%(port)s/%(device)s re: %(info)s'),
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

    def account_info(self, account):
        """
        Get account information, and also verify that the account exists.

        :param account: name of the account to get the info for
        :returns: tuple of (account partition, account nodes) or (None, None)
                  if it does not exist
        """
        partition, nodes = self.app.account_ring.get_nodes(account)
        # 0 = no responses, 200 = found, 404 = not found, -1 = mixed responses
        if self.app.memcache:
            cache_key = get_account_memcache_key(account)
            result_code = self.app.memcache.get(cache_key)
            if result_code == 200:
                return partition, nodes
            elif result_code == 404:
                 return None, None
        result_code = 0
        attempts_left = self.app.account_ring.replica_count
        path = '/%s' % account
        headers = {'x-cf-trans-id': self.trans_id}
        for node in self.iter_nodes(partition, nodes, self.app.account_ring):
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], partition, 'HEAD', path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
                    if 200 <= resp.status <= 299:
                        result_code = 200
                        break
                    elif resp.status == 404:
                        if result_code == 0:
                            result_code = 404
                        elif result_code != 404:
                            result_code = -1
                    elif resp.status == 507:
                        self.error_limit(node)
                        continue
                    else:
                        result_code = -1
                        attempts_left -= 1
                        if attempts_left <= 0:
                            break
            except:
                self.exception_occurred(node, 'Account',
                    'Trying to get account info for %s' % path)
        if self.app.memcache and result_code in (200, 404):
            if result_code == 200:
                cache_timeout = self.app.recheck_account_existence
            else:
                cache_timeout = self.app.recheck_account_existence * 0.1
            self.app.memcache.set(cache_key, result_code,
                                  timeout=cache_timeout)
        if result_code == 200:
            return partition, nodes
        return None, None

    def container_info(self, account, container):
        """
        Get container information and thusly verify container existance.
        This will also make a call to account_info to verify that the
        account exists.

        :param account: account name for the container
        :param container: container name to look up
        :returns: tuple of (container partition, container nodes, container
                  read acl, container write acl) or (None, None, None, None) if
                  the container does not exist
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
                if status == 200:
                    return partition, nodes, read_acl, write_acl
                elif status == 404:
                    return None, None, None, None
        if not self.account_info(account)[1]:
            return None, None, None, None
        result_code = 0
        read_acl = None
        write_acl = None
        container_size = None
        attempts_left = self.app.container_ring.replica_count
        headers = {'x-cf-trans-id': self.trans_id}
        for node in self.iter_nodes(partition, nodes, self.app.container_ring):
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], partition, 'HEAD', path, headers)
                with Timeout(self.app.node_timeout):
                    resp = conn.getresponse()
                    body = resp.read()
                    if 200 <= resp.status <= 299:
                        result_code = 200
                        read_acl = resp.getheader('x-container-read')
                        write_acl = resp.getheader('x-container-write')
                        container_size = \
                            resp.getheader('X-Container-Object-Count')
                        break
                    elif resp.status == 404:
                        if result_code == 0:
                            result_code = 404
                        elif result_code != 404:
                            result_code = -1
                    elif resp.status == 507:
                        self.error_limit(node)
                        continue
                    else:
                        result_code = -1
                        attempts_left -= 1
                        if attempts_left <= 0:
                            break
            except:
                self.exception_occurred(node, 'Container',
                    'Trying to get container info for %s' % path)
        if self.app.memcache and result_code in (200, 404):
            if result_code == 200:
                cache_timeout = self.app.recheck_container_existence
            else:
                cache_timeout = self.app.recheck_container_existence * 0.1
            self.app.memcache.set(cache_key,
                                  {'status': result_code,
                                   'read_acl': read_acl,
                                   'write_acl': write_acl,
                                   'container_size': container_size},
                                  timeout=cache_timeout)
        if result_code == 200:
            return partition, nodes, read_acl, write_acl
        return None, None, None, None

    def iter_nodes(self, partition, nodes, ring):
        """
        Node iterator that will first iterate over the normal nodes for a
        partition and then the handoff partitions for the node.

        :param partition: partition to iterate nodes for
        :param nodes: list of node dicts from the ring
        :param ring: ring to get handoff nodes from
        """
        for node in nodes:
            yield node
        for node in ring.get_more_nodes(partition):
            yield node

    def get_update_nodes(self, partition, nodes, ring):
        """ Returns ring.replica_count nodes; the nodes will not be error
            limited, if possible. """
        """
        Attempt to get a non error limited list of nodes.

        :param partition: partition for the nodes
        :param nodes: list of node dicts for the partition
        :param ring: ring to get handoff nodes from
        :returns: list of node dicts that are not error limited (if possible)
        """

        # make a copy so we don't modify caller's list
        nodes = list(nodes)
        update_nodes = []
        for node in self.iter_nodes(partition, nodes, ring):
            if self.error_limited(node):
                continue
            update_nodes.append(node)
            if len(update_nodes) >= ring.replica_count:
                break
        while len(update_nodes) < ring.replica_count:
            node = nodes.pop()
            if node not in update_nodes:
                update_nodes.append(node)
        return update_nodes

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
            for hundred in (200, 300, 400):
                hstatuses = \
                    [s for s in statuses if hundred <= s < hundred + 100]
                if len(hstatuses) > len(statuses) / 2:
                    status = max(hstatuses)
                    status_index = statuses.index(status)
                    resp.status = '%s %s' % (status, reasons[status_index])
                    resp.body = bodies[status_index]
                    resp.content_type = 'text/plain'
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
        for node in nodes:
            if len(statuses) >= attempts:
                break
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                        node['device'], partition, req.method, path,
                        headers=req.headers,
                        query_string=req.query_string)
                with Timeout(self.app.node_timeout):
                    source = conn.getresponse()
            except:
                self.exception_occurred(node, server_type,
                    'Trying to %s %s' % (req.method, req.path))
                continue
            if source.status == 507:
                self.error_limit(node)
                continue
            if 200 <= source.status <= 399:
                # 404 if we know we don't have a synced copy
                if not float(source.getheader('X-PUT-Timestamp', '1')):
                    statuses.append(404)
                    reasons.append('')
                    bodies.append('')
                    source.read()
                    continue
            if req.method == 'GET' and source.status in (200, 206):
                res = Response(request=req, conditional_response=True)
                res.bytes_transferred = 0

                def file_iter():
                    try:
                        while True:
                            with ChunkReadTimeout(self.app.node_timeout):
                                chunk = source.read(self.app.object_chunk_size)
                            if not chunk:
                                break
                            yield chunk
                            res.bytes_transferred += len(chunk)
                    except GeneratorExit:
                        res.client_disconnect = True
                        self.app.logger.info(_('Client disconnected on read'))
                    except:
                        self.exception_occurred(node, 'Object',
                            'Trying to read during GET of %s' % req.path)
                        raise
                res.app_iter = file_iter()
                update_headers(res, source.getheaders())
                res.status = source.status
                res.content_length = source.getheader('Content-Length')
                if source.getheader('Content-Type'):
                    res.charset = None
                    res.content_type = source.getheader('Content-Type')
                return res
            elif 200 <= source.status <= 399:
                res = status_map[source.status](request=req)
                update_headers(res, source.getheaders())
                if req.method == 'HEAD':
                    res.content_length = source.getheader('Content-Length')
                    if source.getheader('Content-Type'):
                        res.charset = None
                        res.content_type = source.getheader('Content-Type')
                return res
            statuses.append(source.status)
            reasons.append(source.reason)
            bodies.append(source.read())
            if source.status >= 500:
                self.error_occurred(node, 'ERROR %d %s From %s Server' %
                    (source.status, bodies[-1][:1024], server_type))
        return self.best_response(req, statuses, reasons, bodies,
                                  '%s %s' % (server_type, req.method))


class ObjectController(Controller):
    """WSGI controller for object requests."""

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)

    def node_post_or_delete(self, req, partition, node, path):
        """
        Handle common POST/DELETE functionality

        :param req: webob.Request object
        :param partition: partition for the object
        :param node: node dictionary for the object
        :param path: path to send for the request
        """
        if self.error_limited(node):
            return 500, '', ''
        try:
            with ConnectionTimeout(self.app.conn_timeout):
                conn = http_connect(node['ip'], node['port'], node['device'],
                        partition, req.method, path, req.headers)
            with Timeout(self.app.node_timeout):
                response = conn.getresponse()
                body = response.read()
                if response.status == 507:
                    self.error_limit(node)
                elif response.status >= 500:
                    self.error_occurred(node,
                        'ERROR %d %s From Object Server' %
                        (response.status, body[:1024]))
                return response.status, response.reason, body
        except:
            self.exception_occurred(node, 'Object',
                'Trying to %s %s' % (req.method, req.path))
        return 500, '', ''

    def GETorHEAD(self, req):
        """Handle HTTP GET or HEAD requests."""
        if 'swift.authorize' in req.environ:
            req.acl = \
                self.container_info(self.account_name, self.container_name)[2]
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        resp = self.GETorHEAD_base(req, 'Object', partition,
                self.iter_nodes(partition, nodes, self.app.object_ring),
                req.path_info, self.app.object_ring.replica_count)
        # If we get a 416 Requested Range Not Satisfiable we have to check if
        # we were actually requesting a manifest object and then redo the range
        # request on the whole object.
        if resp.status_int == 416:
            req_range = req.range
            req.range = None
            resp2 = self.GETorHEAD_base(req, 'Object', partition,
                    self.iter_nodes(partition, nodes, self.app.object_ring),
                    req.path_info, self.app.object_ring.replica_count)
            if 'x-object-manifest' not in resp2.headers:
                return resp
            resp = resp2
            req.range = req_range

        if 'x-object-manifest' in resp.headers:
            lcontainer, lprefix = \
                resp.headers['x-object-manifest'].split('/', 1)
            lpartition, lnodes = self.app.container_ring.get_nodes(
                self.account_name, lcontainer)
            marker = ''
            listing = []
            while True:
                lreq = Request.blank('/%s/%s?prefix=%s&format=json&marker=%s' %
                    (quote(self.account_name), quote(lcontainer),
                     quote(lprefix), quote(marker)))
                lresp = self.GETorHEAD_base(lreq, 'Container', lpartition,
                    lnodes, lreq.path_info,
                    self.app.container_ring.replica_count)
                if lresp.status_int // 100 != 2:
                    lresp = HTTPNotFound(request=req)
                    lresp.headers['X-Object-Manifest'] = \
                        resp.headers['x-object-manifest']
                    return lresp
                if 'swift.authorize' in req.environ:
                    req.acl = lresp.headers.get('x-container-read')
                    aresp = req.environ['swift.authorize'](req)
                    if aresp:
                        return aresp
                sublisting = json.loads(lresp.body)
                if not sublisting:
                    break
                listing.extend(sublisting)
                if len(listing) > CONTAINER_LISTING_LIMIT:
                    break
                marker = sublisting[-1]['name']

            if len(listing) > CONTAINER_LISTING_LIMIT:
                # We will serve large objects with a ton of segments with
                # chunked transfer encoding.

                def listing_iter():
                    marker = ''
                    while True:
                        lreq = Request.blank(
                            '/%s/%s?prefix=%s&format=json&marker=%s' %
                            (quote(self.account_name), quote(lcontainer),
                             quote(lprefix), quote(marker)))
                        lresp = self.GETorHEAD_base(lreq, 'Container',
                            lpartition, lnodes, lreq.path_info,
                            self.app.container_ring.replica_count)
                        if lresp.status_int // 100 != 2:
                            raise Exception('Object manifest GET could not '
                                'continue listing: %s %s' %
                                (req.path, lreq.path))
                        if 'swift.authorize' in req.environ:
                            req.acl = lresp.headers.get('x-container-read')
                            aresp = req.environ['swift.authorize'](req)
                            if aresp:
                                raise Exception('Object manifest GET could '
                                    'not continue listing: %s %s' %
                                    (req.path, aresp))
                        sublisting = json.loads(lresp.body)
                        if not sublisting:
                            break
                        for obj in sublisting:
                            yield obj
                        marker = sublisting[-1]['name']

                headers = {
                    'X-Object-Manifest': resp.headers['x-object-manifest'],
                    'Content-Type': resp.content_type}
                for key, value in resp.headers.iteritems():
                    if key.lower().startswith('x-object-meta-'):
                        headers[key] = value
                resp = Response(headers=headers, request=req,
                                conditional_response=True)
                if req.method == 'HEAD':
                    # These shenanigans are because webob translates the HEAD
                    # request into a webob EmptyResponse for the body, which
                    # has a len, which eventlet translates as needing a
                    # content-length header added. So we call the original
                    # webob resp for the headers but return an empty iterator
                    # for the body.

                    def head_response(environ, start_response):
                        resp(environ, start_response)
                        return iter([])

                    head_response.status_int = resp.status_int
                    return head_response
                else:
                    resp.app_iter = SegmentedIterable(self, lcontainer,
                                                      listing_iter(), resp)

            else:
                # For objects with a reasonable number of segments, we'll serve
                # them with a set content-length and computed etag.
                content_length = sum(o['bytes'] for o in listing)
                last_modified = max(o['last_modified'] for o in listing)
                last_modified = \
                    datetime(*map(int, re.split('[^\d]', last_modified)[:-1]))
                etag = md5('"'.join(o['hash'] for o in listing)).hexdigest()
                headers = {
                    'X-Object-Manifest': resp.headers['x-object-manifest'],
                    'Content-Type': resp.content_type,
                    'Content-Length': content_length,
                    'ETag': etag}
                for key, value in resp.headers.iteritems():
                    if key.lower().startswith('x-object-meta-'):
                        headers[key] = value
                resp = Response(headers=headers, request=req,
                                conditional_response=True)
                resp.app_iter = SegmentedIterable(self, lcontainer, listing,
                                                  resp)
                resp.content_length = content_length
                resp.last_modified = last_modified

        return resp

    @public
    @delay_denial
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    @delay_denial
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    @public
    @delay_denial
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = check_metadata(req, 'object')
        if error_response:
            return error_response
        container_partition, containers, _junk, req.acl = \
            self.container_info(self.account_name, self.container_name)
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not containers:
            return HTTPNotFound(request=req)
        containers = self.get_update_nodes(container_partition, containers,
                                           self.app.container_ring)
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        req.headers['X-Timestamp'] = normalize_timestamp(time.time())
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(partition, nodes, self.app.object_ring):
            container = containers.pop()
            req.headers['X-Container-Host'] = '%(ip)s:%(port)s' % container
            req.headers['X-Container-Partition'] = container_partition
            req.headers['X-Container-Device'] = container['device']
            status, reason, body = \
                self.node_post_or_delete(req, partition, node, req.path_info)
            if 200 <= status < 300 or 400 <= status < 500:
                statuses.append(status)
                reasons.append(reason)
                bodies.append(body)
            else:
                containers.insert(0, container)
            if not containers:
                break
        while len(statuses) < len(nodes):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        return self.best_response(req, statuses, reasons,
                bodies, 'Object POST')

    @public
    @delay_denial
    def PUT(self, req):
        """HTTP PUT request handler."""
        container_partition, containers, _junk, req.acl = \
            self.container_info(self.account_name, self.container_name)
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not containers:
            return HTTPNotFound(request=req)
        containers = self.get_update_nodes(container_partition, containers,
                                           self.app.container_ring)
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        req.headers['X-Timestamp'] = normalize_timestamp(time.time())
        # Sometimes the 'content-type' header exists, but is set to None.
        if not req.headers.get('content-type'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            if not guessed_type:
                req.headers['Content-Type'] = 'application/octet-stream'
            else:
                req.headers['Content-Type'] = guessed_type
        error_response = check_object_creation(req, self.object_name)
        if error_response:
            return error_response
        conns = []
        data_source = \
            iter(lambda: req.body_file.read(self.app.client_chunk_size), '')
        source_header = req.headers.get('X-Copy-From')
        if source_header:
            source_header = unquote(source_header)
            acct = req.path_info.split('/', 2)[1]
            if not source_header.startswith('/'):
                source_header = '/' + source_header
            source_header = '/' + acct + source_header
            try:
                src_container_name, src_obj_name = \
                    source_header.split('/', 3)[2:]
            except ValueError:
                return HTTPPreconditionFailed(request=req,
                    body='X-Copy-From header must be of the form'
                    '<container name>/<object name>')
            source_req = req.copy_get()
            source_req.path_info = source_header
            orig_obj_name = self.object_name
            orig_container_name = self.container_name
            self.object_name = src_obj_name
            self.container_name = src_container_name
            source_resp = self.GET(source_req)
            if source_resp.status_int >= 300:
                return source_resp
            self.object_name = orig_obj_name
            self.container_name = orig_container_name
            new_req = Request.blank(req.path_info,
                        environ=req.environ, headers=req.headers)
            if 'x-object-manifest' in source_resp.headers:
                data_source = iter([''])
                new_req.content_length = 0
                new_req.headers['X-Object-Manifest'] = \
                    source_resp.headers['x-object-manifest']
            else:
                data_source = source_resp.app_iter
                new_req.content_length = source_resp.content_length
                new_req.etag = source_resp.etag
            # we no longer need the X-Copy-From header
            del new_req.headers['X-Copy-From']
            for k, v in source_resp.headers.items():
                if k.lower().startswith('x-object-meta-'):
                    new_req.headers[k] = v
            for k, v in req.headers.items():
                if k.lower().startswith('x-object-meta-'):
                    new_req.headers[k] = v
            req = new_req
        for node in self.iter_nodes(partition, nodes, self.app.object_ring):
            container = containers.pop()
            req.headers['X-Container-Host'] = '%(ip)s:%(port)s' % container
            req.headers['X-Container-Partition'] = container_partition
            req.headers['X-Container-Device'] = container['device']
            req.headers['Expect'] = '100-continue'
            resp = conn = None
            if not self.error_limited(node):
                try:
                    with ConnectionTimeout(self.app.conn_timeout):
                        conn = http_connect(node['ip'], node['port'],
                                node['device'], partition, 'PUT',
                                req.path_info, req.headers)
                        conn.node = node
                    with Timeout(self.app.node_timeout):
                        resp = conn.getexpect()
                except:
                    self.exception_occurred(node, 'Object',
                        'Expect: 100-continue on %s' % req.path)
            if conn and resp:
                if resp.status == 100:
                    conns.append(conn)
                    if not containers:
                        break
                    continue
                elif resp.status == 507:
                    self.error_limit(node)
            containers.insert(0, container)
        if len(conns) <= len(nodes) / 2:
            self.app.logger.error(
                _('Object PUT returning 503, %(conns)s/%(nodes)s '
                'required connections'),
                {'conns': len(conns), 'nodes': len(nodes) // 2 + 1})
            return HTTPServiceUnavailable(request=req)
        try:
            req.bytes_transferred = 0
            while True:
                with ChunkReadTimeout(self.app.client_timeout):
                    try:
                        chunk = data_source.next()
                    except StopIteration:
                        if req.headers.get('transfer-encoding'):
                            chunk = ''
                        else:
                            break
                len_chunk = len(chunk)
                req.bytes_transferred += len_chunk
                if req.bytes_transferred > MAX_FILE_SIZE:
                    return HTTPRequestEntityTooLarge(request=req)
                for conn in list(conns):
                    try:
                        with ChunkWriteTimeout(self.app.node_timeout):
                            if req.headers.get('transfer-encoding'):
                                conn.send('%x\r\n%s\r\n' % (len_chunk, chunk))
                            else:
                                conn.send(chunk)
                    except:
                        self.exception_occurred(conn.node, 'Object',
                            'Trying to write to %s' % req.path)
                        conns.remove(conn)
                        if len(conns) <= len(nodes) / 2:
                            self.app.logger.error(
                                _('Object PUT exceptions during send, '
                                  '%(conns)s/%(nodes)s required connections'),
                                {'conns': len(conns),
                                 'nodes': len(nodes) // 2 + 1})
                            return HTTPServiceUnavailable(request=req)
                if req.headers.get('transfer-encoding') and chunk == '':
                    break
        except ChunkReadTimeout, err:
            self.app.logger.info(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            return HTTPRequestTimeout(request=req)
        except:
            req.client_disconnect = True
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            return Response(status='499 Client Disconnect')
        if req.content_length and req.bytes_transferred < req.content_length:
            req.client_disconnect = True
            self.app.logger.info(
                _('Client disconnected without sending enough data'))
            return Response(status='499 Client Disconnect')
        statuses = []
        reasons = []
        bodies = []
        etags = set()
        for conn in conns:
            try:
                with Timeout(self.app.node_timeout):
                    response = conn.getresponse()
                    statuses.append(response.status)
                    reasons.append(response.reason)
                    bodies.append(response.read())
                    if response.status >= 500:
                        self.error_occurred(conn.node,
                            'ERROR %d %s From Object Server re: %s' %
                            (response.status, bodies[-1][:1024], req.path))
                    elif 200 <= response.status < 300:
                        etags.add(response.getheader('etag').strip('"'))
            except:
                self.exception_occurred(conn.node, 'Object',
                    'Trying to get final status of PUT to %s' % req.path)
        if len(etags) > 1:
            self.app.logger.error(
                _('Object servers returned %s mismatched etags'), len(etags))
            return HTTPServerError(request=req)
        etag = len(etags) and etags.pop() or None
        while len(statuses) < len(nodes):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        resp = self.best_response(req, statuses, reasons, bodies, 'Object PUT',
                                  etag=etag)
        if source_header:
            resp.headers['X-Copied-From'] = quote(
                                                source_header.split('/', 2)[2])
            for k, v in req.headers.items():
                if k.lower().startswith('x-object-meta-'):
                    resp.headers[k] = v
            # reset the bytes, since the user didn't actually send anything
            req.bytes_transferred = 0
        resp.last_modified = float(req.headers['X-Timestamp'])
        return resp

    @public
    @delay_denial
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        container_partition, containers, _junk, req.acl = \
            self.container_info(self.account_name, self.container_name)
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not containers:
            return HTTPNotFound(request=req)
        containers = self.get_update_nodes(container_partition, containers,
                                           self.app.container_ring)
        partition, nodes = self.app.object_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        req.headers['X-Timestamp'] = normalize_timestamp(time.time())
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(partition, nodes, self.app.object_ring):
            container = containers.pop()
            req.headers['X-Container-Host'] = '%(ip)s:%(port)s' % container
            req.headers['X-Container-Partition'] = container_partition
            req.headers['X-Container-Device'] = container['device']
            status, reason, body = \
                self.node_post_or_delete(req, partition, node, req.path_info)
            if 200 <= status < 300 or 400 <= status < 500:
                statuses.append(status)
                reasons.append(reason)
                bodies.append(body)
            else:
                containers.insert(0, container)
            if not containers:
                break
        while len(statuses) < len(nodes):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        return self.best_response(req, statuses, reasons, bodies,
                                  'Object DELETE')

    @public
    @delay_denial
    def COPY(self, req):
        """HTTP COPY request handler."""
        dest = req.headers.get('Destination')
        if not dest:
            return HTTPPreconditionFailed(request=req,
                                          body='Destination header required')
        dest = unquote(dest)
        if not dest.startswith('/'):
            dest = '/' + dest
        try:
            _junk, dest_container, dest_object = dest.split('/', 2)
        except ValueError:
            return HTTPPreconditionFailed(request=req,
                    body='Destination header must be of the form '
                         '<container name>/<object name>')
        new_source = '/' + self.container_name + '/' + self.object_name
        self.container_name = dest_container
        self.object_name = dest_object
        new_headers = {}
        for k, v in req.headers.items():
            new_headers[k] = v
        new_headers['X-Copy-From'] = new_source
        new_headers['Content-Length'] = 0
        del new_headers['Destination']
        new_path = '/' + self.account_name + dest
        new_req = Request.blank(new_path, environ=req.environ,
                                headers=new_headers)
        return self.PUT(new_req)


class ContainerController(Controller):
    """WSGI controller for container requests"""

    # Ensure these are all lowercase
    pass_through_headers = ['x-container-read', 'x-container-write']

    def __init__(self, app, account_name, container_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)

    def clean_acls(self, req):
        if 'swift.clean_acl' in req.environ:
            for header in ('x-container-read', 'x-container-write'):
                if header in req.headers:
                    try:
                        req.headers[header] = \
                            req.environ['swift.clean_acl'](header,
                                                           req.headers[header])
                    except ValueError, err:
                        return HTTPBadRequest(request=req, body=str(err))
        return None

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        if not self.account_info(self.account_name)[1]:
            return HTTPNotFound(request=req)
        part, nodes = self.app.container_ring.get_nodes(
                        self.account_name, self.container_name)
        resp = self.GETorHEAD_base(req, 'Container', part, nodes,
                req.path_info, self.app.container_ring.replica_count)

        if self.app.memcache:
            # set the memcache container size for ratelimiting
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.set(cache_key,
              {'status': resp.status_int,
               'read_acl': resp.headers.get('x-container-read'),
               'write_acl': resp.headers.get('x-container-write'),
               'container_size': resp.headers.get('x-container-object-count')},
                                  timeout=self.app.recheck_container_existence)

        if 'swift.authorize' in req.environ:
            req.acl = resp.headers.get('x-container-read')
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        return resp

    @public
    @delay_denial
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    @delay_denial
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        if len(self.container_name) > MAX_CONTAINER_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Container name length of %d longer than %d' % \
                        (len(self.container_name), MAX_CONTAINER_NAME_LENGTH)
            return resp
        account_partition, accounts = self.account_info(self.account_name)
        if not accounts:
            return HTTPNotFound(request=req)
        accounts = self.get_update_nodes(account_partition, accounts,
                                         self.app.account_ring)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'x-cf-trans-id': self.trans_id}
        headers.update(value for value in req.headers.iteritems()
            if value[0].lower() in self.pass_through_headers or
               value[0].lower().startswith('x-container-meta-'))
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(container_partition, containers,
                                    self.app.container_ring):
            if self.error_limited(node):
                continue
            try:
                account = accounts.pop()
                headers['X-Account-Host'] = '%(ip)s:%(port)s' % account
                headers['X-Account-Partition'] = account_partition
                headers['X-Account-Device'] = account['device']
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], container_partition, 'PUT',
                            req.path_info, headers)
                with Timeout(self.app.node_timeout):
                    source = conn.getresponse()
                    body = source.read()
                    if 200 <= source.status < 300 \
                            or 400 <= source.status < 500:
                        statuses.append(source.status)
                        reasons.append(source.reason)
                        bodies.append(body)
                    else:
                        if source.status == 507:
                            self.error_limit(node)
                        accounts.insert(0, account)
            except:
                accounts.insert(0, account)
                self.exception_occurred(node, 'Container',
                    'Trying to PUT to %s' % req.path)
            if not accounts:
                break
        while len(statuses) < len(containers):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        return self.best_response(req, statuses, reasons, bodies,
                                  'Container PUT')

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = \
            self.clean_acls(req) or check_metadata(req, 'container')
        if error_response:
            return error_response
        account_partition, accounts = self.account_info(self.account_name)
        if not accounts:
            return HTTPNotFound(request=req)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'x-cf-trans-id': self.trans_id}
        headers.update(value for value in req.headers.iteritems()
            if value[0].lower() in self.pass_through_headers or
               value[0].lower().startswith('x-container-meta-'))
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(container_partition, containers,
                                    self.app.container_ring):
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], container_partition, 'POST',
                            req.path_info, headers)
                with Timeout(self.app.node_timeout):
                    source = conn.getresponse()
                    body = source.read()
                    if 200 <= source.status < 300 \
                            or 400 <= source.status < 500:
                        statuses.append(source.status)
                        reasons.append(source.reason)
                        bodies.append(body)
                    elif source.status == 507:
                        self.error_limit(node)
            except:
                self.exception_occurred(node, 'Container',
                    'Trying to POST %s' % req.path)
            if len(statuses) >= len(containers):
                break
        while len(statuses) < len(containers):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        return self.best_response(req, statuses, reasons, bodies,
                                  'Container POST')

    @public
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        account_partition, accounts = self.account_info(self.account_name)
        if not accounts:
            return HTTPNotFound(request=req)
        accounts = self.get_update_nodes(account_partition, accounts,
                                         self.app.account_ring)
        container_partition, containers = self.app.container_ring.get_nodes(
            self.account_name, self.container_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
             'x-cf-trans-id': self.trans_id}
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(container_partition, containers,
                                    self.app.container_ring):
            if self.error_limited(node):
                continue
            try:
                account = accounts.pop()
                headers['X-Account-Host'] = '%(ip)s:%(port)s' % account
                headers['X-Account-Partition'] = account_partition
                headers['X-Account-Device'] = account['device']
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], container_partition, 'DELETE',
                            req.path_info, headers)
                with Timeout(self.app.node_timeout):
                    source = conn.getresponse()
                    body = source.read()
                    if 200 <= source.status < 300 \
                            or 400 <= source.status < 500:
                        statuses.append(source.status)
                        reasons.append(source.reason)
                        bodies.append(body)
                    else:
                        if source.status == 507:
                            self.error_limit(node)
                        accounts.insert(0, account)
            except:
                accounts.insert(0, account)
                self.exception_occurred(node, 'Container',
                    'Trying to DELETE %s' % req.path)
            if not accounts:
                break
        while len(statuses) < len(containers):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        if self.app.memcache:
            cache_key = get_container_memcache_key(self.account_name,
                                                   self.container_name)
            self.app.memcache.delete(cache_key)
        resp = self.best_response(req, statuses, reasons, bodies,
                                  'Container DELETE')
        if 200 <= resp.status_int <= 299:
            for status in statuses:
                if status < 200 or status > 299:
                    # If even one node doesn't do the delete, we can't be sure
                    # what the outcome will be once everything is in sync; so
                    # we 503.
                    self.app.logger.error(_('Returning 503 because not all '
                        'container nodes confirmed DELETE'))
                    return HTTPServiceUnavailable(request=req)
        if resp.status_int == 202:  # Indicates no server had the container
            return HTTPNotFound(request=req)
        return resp


class AccountController(Controller):
    """WSGI controller for account requests"""

    def __init__(self, app, account_name, **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)

    def GETorHEAD(self, req):
        """Handler for HTTP GET/HEAD requests."""
        partition, nodes = self.app.account_ring.get_nodes(self.account_name)
        return self.GETorHEAD_base(req, 'Account', partition, nodes,
                req.path_info.rstrip('/'), self.app.account_ring.replica_count)

    @public
    def PUT(self, req):
        """HTTP PUT request handler."""
        if not self.app.allow_account_management:
            return HTTPMethodNotAllowed(request=req)
        error_response = check_metadata(req, 'account')
        if error_response:
            return error_response
        if len(self.account_name) > MAX_ACCOUNT_NAME_LENGTH:
            resp = HTTPBadRequest(request=req)
            resp.body = 'Account name length of %d longer than %d' % \
                        (len(self.account_name), MAX_ACCOUNT_NAME_LENGTH)
            return resp
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'x-cf-trans-id': self.trans_id}
        headers.update(value for value in req.headers.iteritems()
            if value[0].lower().startswith('x-account-meta-'))
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(account_partition, accounts,
                                    self.app.account_ring):
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], account_partition, 'PUT',
                            req.path_info, headers)
                with Timeout(self.app.node_timeout):
                    source = conn.getresponse()
                    body = source.read()
                    if 200 <= source.status < 300 \
                            or 400 <= source.status < 500:
                        statuses.append(source.status)
                        reasons.append(source.reason)
                        bodies.append(body)
                    else:
                        if source.status == 507:
                            self.error_limit(node)
            except:
                self.exception_occurred(node, 'Account',
                    'Trying to PUT to %s' % req.path)
            if len(statuses) >= len(accounts):
                break
        while len(statuses) < len(accounts):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        if self.app.memcache:
            self.app.memcache.delete('account%s' % req.path_info.rstrip('/'))
        return self.best_response(req, statuses, reasons, bodies,
                                  'Account PUT')

    @public
    def POST(self, req):
        """HTTP POST request handler."""
        error_response = check_metadata(req, 'account')
        if error_response:
            return error_response
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'X-CF-Trans-Id': self.trans_id}
        headers.update(value for value in req.headers.iteritems()
            if value[0].lower().startswith('x-account-meta-'))
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(account_partition, accounts,
                                    self.app.account_ring):
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], account_partition, 'POST',
                            req.path_info, headers)
                with Timeout(self.app.node_timeout):
                    source = conn.getresponse()
                    body = source.read()
                    if 200 <= source.status < 300 \
                            or 400 <= source.status < 500:
                        statuses.append(source.status)
                        reasons.append(source.reason)
                        bodies.append(body)
                    elif source.status == 507:
                        self.error_limit(node)
            except:
                self.exception_occurred(node, 'Account',
                    'Trying to POST %s' % req.path)
            if len(statuses) >= len(accounts):
                break
        while len(statuses) < len(accounts):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        if self.app.memcache:
            self.app.memcache.delete('account%s' % req.path_info.rstrip('/'))
        return self.best_response(req, statuses, reasons, bodies,
                                  'Account POST')

    @public
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        if not self.app.allow_account_management:
            return HTTPMethodNotAllowed(request=req)
        account_partition, accounts = \
            self.app.account_ring.get_nodes(self.account_name)
        headers = {'X-Timestamp': normalize_timestamp(time.time()),
                   'X-CF-Trans-Id': self.trans_id}
        statuses = []
        reasons = []
        bodies = []
        for node in self.iter_nodes(account_partition, accounts,
                                    self.app.account_ring):
            if self.error_limited(node):
                continue
            try:
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(node['ip'], node['port'],
                            node['device'], account_partition, 'DELETE',
                            req.path_info, headers)
                with Timeout(self.app.node_timeout):
                    source = conn.getresponse()
                    body = source.read()
                    if 200 <= source.status < 300 \
                            or 400 <= source.status < 500:
                        statuses.append(source.status)
                        reasons.append(source.reason)
                        bodies.append(body)
                    elif source.status == 507:
                        self.error_limit(node)
            except:
                self.exception_occurred(node, 'Account',
                    'Trying to DELETE %s' % req.path)
            if len(statuses) >= len(accounts):
                break
        while len(statuses) < len(accounts):
            statuses.append(503)
            reasons.append('')
            bodies.append('')
        if self.app.memcache:
            self.app.memcache.delete('account%s' % req.path_info.rstrip('/'))
        return self.best_response(req, statuses, reasons, bodies,
                                  'Account DELETE')


class BaseApplication(object):
    """Base WSGI application for the proxy server"""

    def __init__(self, conf, memcache=None, logger=None, account_ring=None,
                 container_ring=None, object_ring=None):
        if logger is None:
            self.logger = get_logger(conf)
        else:
            self.logger = logger
        if conf is None:
            conf = {}
        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.client_timeout = int(conf.get('client_timeout', 60))
        self.object_chunk_size = int(conf.get('object_chunk_size', 65536))
        self.client_chunk_size = int(conf.get('client_chunk_size', 65536))
        self.log_headers = conf.get('log_headers') == 'True'
        self.error_suppression_interval = \
            int(conf.get('error_suppression_interval', 60))
        self.error_suppression_limit = \
            int(conf.get('error_suppression_limit', 10))
        self.recheck_container_existence = \
            int(conf.get('recheck_container_existence', 60))
        self.recheck_account_existence = \
            int(conf.get('recheck_account_existence', 60))
        self.allow_account_management = \
            conf.get('allow_account_management', 'false').lower() == 'true'
        self.resellers_conf = ConfigParser()
        self.resellers_conf.read(os.path.join(swift_dir, 'resellers.conf'))
        self.object_ring = object_ring or \
            Ring(os.path.join(swift_dir, 'object.ring.gz'))
        self.container_ring = container_ring or \
            Ring(os.path.join(swift_dir, 'container.ring.gz'))
        self.account_ring = account_ring or \
            Ring(os.path.join(swift_dir, 'account.ring.gz'))
        self.memcache = memcache
        mimetypes.init(mimetypes.knownfiles + 
                       [os.path.join(swift_dir, 'mime.types')])

    def get_controller(self, path):
        """
        Get the controller to handle a request.

        :param path: path from request
        :returns: tuple of (controller class, path dictionary)

        :raises: ValueError (thrown by split_path) id given invalid path
        """
        version, account, container, obj = split_path(path, 1, 4, True)
        d = dict(version=version,
                account_name=account,
                container_name=container,
                object_name=obj)
        if obj and container and account:
            return ObjectController, d
        elif container and account:
            return ContainerController, d
        elif account and not container and not obj:
            return AccountController, d
        return None, d

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in webob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            if self.memcache is None:
                self.memcache = cache_from_env(env)
            req = self.update_request(Request(env))
            if 'eventlet.posthooks' in env:
                env['eventlet.posthooks'].append(
                    (self.posthooklogger, (req,), {}))
                return self.handle_request(req)(env, start_response)
            else:
                # Lack of posthook support means that we have to log on the
                # start of the response, rather than after all the data has
                # been sent. This prevents logging client disconnects
                # differently than full transmissions.
                response = self.handle_request(req)(env, start_response)
                self.posthooklogger(env, req)
                return response
        except:
            print "EXCEPTION IN __call__: %s: %s" % \
                  (traceback.format_exc(), env)
            start_response('500 Server Error',
                    [('Content-Type', 'text/plain')])
            return ['Internal server error.\n']

    def posthooklogger(self, env, req):
        pass

    def update_request(self, req):
        req.bytes_transferred = '-'
        req.client_disconnect = False
        req.headers['x-cf-trans-id'] = 'tx' + str(uuid.uuid4())
        if 'x-storage-token' in req.headers and \
                'x-auth-token' not in req.headers:
            req.headers['x-auth-token'] = req.headers['x-storage-token']
        return req

    def handle_request(self, req):
        """
        Entry point for proxy server.
        Should return a WSGI-style callable (such as webob.Response).

        :param req: webob.Request object
        """
        try:
            try:
                controller, path_parts = self.get_controller(req.path)
            except ValueError:
                return HTTPNotFound(request=req)
            if not check_utf8(req.path_info):
                return HTTPPreconditionFailed(request=req, body='Invalid UTF8')
            if not controller:
                return HTTPPreconditionFailed(request=req, body='Bad URL')

            controller = controller(self, **path_parts)
            controller.trans_id = req.headers.get('x-cf-trans-id', '-')
            self.logger.txn_id = req.headers.get('x-cf-trans-id', None)
            try:
                handler = getattr(controller, req.method)
                if not getattr(handler, 'publicly_accessible'):
                    handler = None
            except AttributeError:
                handler = None
            if not handler:
                return HTTPMethodNotAllowed(request=req)
            if path_parts['version']:
                req.path_info_pop()
            if 'swift.authorize' in req.environ:
                # We call authorize before the handler, always. If authorized,
                # we remove the swift.authorize hook so isn't ever called
                # again. If not authorized, we return the denial unless the
                # controller's method indicates it'd like to gather more
                # information and try again later.
                resp = req.environ['swift.authorize'](req)
                if not resp:
                    # No resp means authorized, no delayed recheck required.
                    del req.environ['swift.authorize']
                else:
                    # Response indicates denial, but we might delay the denial
                    # and recheck later. If not delayed, return the error now.
                    if not getattr(handler, 'delay_denial', None):
                        return resp
            return handler(req)
        except Exception:
            self.logger.exception(_('ERROR Unhandled exception in request'))
            return HTTPServerError(request=req)


class Application(BaseApplication):
    """WSGI application for the proxy server."""

    def handle_request(self, req):
        """
        Wraps the BaseApplication.handle_request and logs the request.
        """
        req.start_time = time.time()
        req.response = super(Application, self).handle_request(req)
        return req.response

    def posthooklogger(self, env, req):
        response = getattr(req, 'response', None)
        if not response:
            return
        trans_time = '%.4f' % (time.time() - req.start_time)
        the_request = quote(unquote(req.path))
        if req.query_string:
            the_request = the_request + '?' + req.query_string
        # remote user for zeus
        client = req.headers.get('x-cluster-client-ip')
        if not client and 'x-forwarded-for' in req.headers:
            # remote user for other lbs
            client = req.headers['x-forwarded-for'].split(',')[0].strip()
        logged_headers = None
        if self.log_headers:
            logged_headers = '\n'.join('%s: %s' % (k, v)
                for k, v in req.headers.items())
        status_int = response.status_int
        if getattr(req, 'client_disconnect', False) or \
                getattr(response, 'client_disconnect', False):
            status_int = 499
        self.logger.info(' '.join(quote(str(x)) for x in (
                client or '-',
                req.remote_addr or '-',
                time.strftime('%d/%b/%Y/%H/%M/%S', time.gmtime()),
                req.method,
                the_request,
                req.environ['SERVER_PROTOCOL'],
                status_int,
                req.referer or '-',
                req.user_agent or '-',
                req.headers.get('x-auth-token', '-'),
                getattr(req, 'bytes_transferred', 0) or '-',
                getattr(response, 'bytes_transferred', 0) or '-',
                req.headers.get('etag', '-'),
                req.headers.get('x-cf-trans-id', '-'),
                logged_headers or '-',
                trans_time,
            )))


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    return Application(conf)
