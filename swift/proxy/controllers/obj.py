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
# source bufferedhttp connection of whatever it is attached to.
#   It is used when early termination of reading from the connection should
# happen, such as when a range request is satisfied but there's still more the
# source connection would like to send. To prevent having to read all the data
# that could be left, the source connection can be .close() and then reads
# commence to empty out any buffers.
#   These shenanigans are to ensure all related objects can be garbage
# collected. We've seen objects hang around forever otherwise.

import collections
import itertools
import mimetypes
import time
import math
import random
from hashlib import md5
from swift import gettext_ as _
from urllib import unquote, quote

from greenlet import GreenletExit
from eventlet import GreenPile
from eventlet.queue import Queue
from eventlet.timeout import Timeout

from swift.common.utils import (
    clean_content_type, config_true_value, ContextPool, csv_append,
    GreenAsyncPile, GreenthreadSafeIterator, json, Timestamp,
    normalize_delete_at_timestamp, public, get_expirer_container,
    quorum_size)
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_metadata, check_object_creation, \
    check_copy_from_header, check_destination_header, \
    check_account_format
from swift.common import constraints
from swift.common.exceptions import ChunkReadTimeout, \
    ChunkWriteTimeout, ConnectionTimeout, ListingIterNotFound, \
    ListingIterNotAuthorized, ListingIterError, ResponseTimeout, \
    InsufficientStorage, FooterNotSupported, MultiphasePUTNotSupported, \
    PutterConnectError
from swift.common.http import (
    is_success, is_client_error, is_server_error, HTTP_CONTINUE, HTTP_CREATED,
    HTTP_MULTIPLE_CHOICES, HTTP_NOT_FOUND, HTTP_INTERNAL_SERVER_ERROR,
    HTTP_SERVICE_UNAVAILABLE, HTTP_INSUFFICIENT_STORAGE,
    HTTP_PRECONDITION_FAILED, HTTP_CONFLICT, is_informational)
from swift.common.storage_policy import (POLICIES, REPL_POLICY, EC_POLICY,
                                         ECDriverError, PolicyError)
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPRequestEntityTooLarge, HTTPRequestTimeout, \
    HTTPServerError, HTTPServiceUnavailable, Request, HeaderKeyDict, \
    HTTPClientDisconnect, HTTPUnprocessableEntity, Response, HTTPException
from swift.common.request_helpers import is_sys_or_user_meta, is_sys_meta, \
    remove_items, copy_header_subset, close_if_possible


def copy_headers_into(from_r, to_r):
    """
    Will copy desired headers from from_r to to_r
    :params from_r: a swob Request or Response
    :params to_r: a swob Request or Response
    """
    pass_headers = ['x-delete-at']
    for k, v in from_r.headers.items():
        if is_sys_or_user_meta('object', k) or k.lower() in pass_headers:
            to_r.headers[k] = v


def check_content_type(req):
    if not req.environ.get('swift.content_type_overridden') and \
            ';' in req.headers.get('content-type', ''):
        for param in req.headers['content-type'].split(';')[1:]:
            if param.lstrip().startswith('swift_'):
                return HTTPBadRequest("Invalid Content-Type, "
                                      "swift_* is not a valid parameter name.")
    return None


class ObjectControllerRouter(object):

    policy_type_to_controller_map = {}

    @classmethod
    def register(cls, policy_type):
        """
        Decorator for Storage Policy implemenations to register
        their ObjectController implementations.

        This also fills in a policy_type attribute on the class.
        """
        def register_wrapper(controller_cls):
            if policy_type in cls.policy_type_to_controller_map:
                raise PolicyError(
                    '%r is already registered for the policy_type %r' % (
                        cls.policy_type_to_controller_map[policy_type],
                        policy_type))
            cls.policy_type_to_controller_map[policy_type] = controller_cls
            controller_cls.policy_type = policy_type
            return controller_cls
        return register_wrapper

    def __init__(self):
        self.policy_to_controller_cls = {}
        for policy in POLICIES:
            self.policy_to_controller_cls[policy] = \
                self.policy_type_to_controller_map[policy.policy_type]

    def __getitem__(self, policy):
        return self.policy_to_controller_cls[policy]


class BaseObjectController(Controller):
    """Base WSGI controller for object requests."""
    server_type = 'Object'

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        Controller.__init__(self, app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)

    def _listing_iter(self, lcontainer, lprefix, env):
        for page in self._listing_pages_iter(lcontainer, lprefix, env):
            for item in page:
                yield item

    def _listing_pages_iter(self, lcontainer, lprefix, env):
        lpartition = self.app.container_ring.get_part(
            self.account_name, lcontainer)
        marker = ''
        while True:
            lreq = Request.blank('i will be overridden by env', environ=env)
            # Don't quote PATH_INFO, by WSGI spec
            lreq.environ['PATH_INFO'] = \
                '/v1/%s/%s' % (self.account_name, lcontainer)
            lreq.environ['REQUEST_METHOD'] = 'GET'
            lreq.environ['QUERY_STRING'] = \
                'format=json&prefix=%s&marker=%s' % (quote(lprefix),
                                                     quote(marker))
            container_node_iter = self.app.iter_nodes(self.app.container_ring,
                                                      lpartition)
            lresp = self.GETorHEAD_base(
                lreq, _('Container'), container_node_iter, lpartition,
                lreq.swift_entity_path)
            if 'swift.authorize' in env:
                lreq.acl = lresp.headers.get('x-container-read')
                aresp = env['swift.authorize'](lreq)
                if aresp:
                    raise ListingIterNotAuthorized(aresp)
            if lresp.status_int == HTTP_NOT_FOUND:
                raise ListingIterNotFound()
            elif not is_success(lresp.status_int):
                raise ListingIterError()
            if not lresp.body:
                break
            sublisting = json.loads(lresp.body)
            if not sublisting:
                break
            marker = sublisting[-1]['name'].encode('utf-8')
            yield sublisting

    def iter_nodes_local_first(self, ring, partition):
        """
        Yields nodes for a ring partition.

        If the 'write_affinity' setting is non-empty, then this will yield N
        local nodes (as defined by the write_affinity setting) first, then the
        rest of the nodes as normal. It is a re-ordering of the nodes such
        that the local ones come first; no node is omitted. The effect is
        that the request will be serviced by local object servers first, but
        nonlocal ones will be employed if not enough local ones are available.

        :param ring: ring to get nodes from
        :param partition: ring partition to yield nodes for
        """

        is_local = self.app.write_affinity_is_local_fn
        if is_local is None:
            return self.app.iter_nodes(ring, partition)

        primary_nodes = ring.get_part_nodes(partition)
        num_locals = self.app.write_affinity_node_count(len(primary_nodes))

        all_nodes = itertools.chain(primary_nodes,
                                    ring.get_more_nodes(partition))
        first_n_local_nodes = list(itertools.islice(
            itertools.ifilter(is_local, all_nodes), num_locals))

        # refresh it; it moved when we computed first_n_local_nodes
        all_nodes = itertools.chain(primary_nodes,
                                    ring.get_more_nodes(partition))
        local_first_node_iter = itertools.chain(
            first_n_local_nodes,
            itertools.ifilter(lambda node: node not in first_n_local_nodes,
                              all_nodes))

        return self.app.iter_nodes(
            ring, partition, node_iter=local_first_node_iter)

    def GETorHEAD(self, req):
        """Handle HTTP GET or HEAD requests."""
        container_info = self.container_info(
            self.account_name, self.container_name, req)
        req.acl = container_info['read_acl']
        # pass the policy index to storage nodes via req header
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                       container_info['storage_policy'])
        policy = POLICIES.get_by_index(policy_index)
        obj_ring = self.app.get_object_ring(policy_index)
        req.headers['X-Backend-Storage-Policy-Index'] = policy_index
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        partition = obj_ring.get_part(
            self.account_name, self.container_name, self.object_name)
        node_iter = self.app.iter_nodes(obj_ring, partition)

        resp = self._reroute(policy)._get_or_head_response(
            req, node_iter, partition, policy)

        if ';' in resp.headers.get('content-type', ''):
            resp.content_type = clean_content_type(
                resp.headers['content-type'])
        return resp

    @public
    @cors_validation
    @delay_denial
    def GET(self, req):
        """Handler for HTTP GET requests."""
        return self.GETorHEAD(req)

    @public
    @cors_validation
    @delay_denial
    def HEAD(self, req):
        """Handler for HTTP HEAD requests."""
        return self.GETorHEAD(req)

    @public
    @cors_validation
    @delay_denial
    def POST(self, req):
        """HTTP POST request handler."""
        if self.app.object_post_as_copy:
            req.method = 'PUT'
            req.path_info = '/v1/%s/%s/%s' % (
                self.account_name, self.container_name, self.object_name)
            req.headers['Content-Length'] = 0
            req.headers['X-Copy-From'] = quote('/%s/%s' % (self.container_name,
                                               self.object_name))
            req.headers['X-Fresh-Metadata'] = 'true'
            req.environ['swift_versioned_copy'] = True
            if req.environ.get('QUERY_STRING'):
                req.environ['QUERY_STRING'] += '&multipart-manifest=get'
            else:
                req.environ['QUERY_STRING'] = 'multipart-manifest=get'
            resp = self.PUT(req)
            # Older editions returned 202 Accepted on object POSTs, so we'll
            # convert any 201 Created responses to that for compatibility with
            # picky clients.
            if resp.status_int != HTTP_CREATED:
                return resp
            return HTTPAccepted(request=req)
        else:
            error_response = check_metadata(req, 'object')
            if error_response:
                return error_response
            container_info = self.container_info(
                self.account_name, self.container_name, req)
            container_partition = container_info['partition']
            containers = container_info['nodes']
            req.acl = container_info['write_acl']
            if 'swift.authorize' in req.environ:
                aresp = req.environ['swift.authorize'](req)
                if aresp:
                    return aresp
            if not containers:
                return HTTPNotFound(request=req)

            req, delete_at_container, delete_at_part, \
                delete_at_nodes = self._config_obj_expiration(req)

            # pass the policy index to storage nodes via req header
            policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                           container_info['storage_policy'])
            obj_ring = self.app.get_object_ring(policy_index)
            req.headers['X-Backend-Storage-Policy-Index'] = policy_index
            partition, nodes = obj_ring.get_nodes(
                self.account_name, self.container_name, self.object_name)

            req.headers['X-Timestamp'] = Timestamp(time.time()).internal

            headers = self._backend_requests(
                req, len(nodes), container_partition, containers,
                delete_at_container, delete_at_part, delete_at_nodes)

            resp = self.make_requests(req, obj_ring, partition,
                                      'POST', req.swift_entity_path, headers)
            return resp

    def _backend_requests(self, req, n_outgoing,
                          container_partition, containers,
                          delete_at_container=None, delete_at_partition=None,
                          delete_at_nodes=None):
        headers = [self.generate_request_headers(req, additional=req.headers)
                   for _junk in range(n_outgoing)]

        for i, container in enumerate(containers):
            i = i % len(headers)

            headers[i]['X-Container-Partition'] = container_partition
            headers[i]['X-Container-Host'] = csv_append(
                headers[i].get('X-Container-Host'),
                '%(ip)s:%(port)s' % container)
            headers[i]['X-Container-Device'] = csv_append(
                headers[i].get('X-Container-Device'),
                container['device'])

        for i, node in enumerate(delete_at_nodes or []):
            i = i % len(headers)

            headers[i]['X-Delete-At-Container'] = delete_at_container
            headers[i]['X-Delete-At-Partition'] = delete_at_partition
            headers[i]['X-Delete-At-Host'] = csv_append(
                headers[i].get('X-Delete-At-Host'),
                '%(ip)s:%(port)s' % node)
            headers[i]['X-Delete-At-Device'] = csv_append(
                headers[i].get('X-Delete-At-Device'),
                node['device'])

        return headers

    def _send_file(self, conn, path):
        """Method for a file PUT coro"""
        while True:
            chunk = conn.queue.get()
            if not conn.failed:
                try:
                    with ChunkWriteTimeout(self.app.node_timeout):
                        conn.send(chunk)
                except (Exception, ChunkWriteTimeout):
                    conn.failed = True
                    self.app.exception_occurred(
                        conn.node, _('Object'),
                        _('Trying to write to %s') % path)
            conn.queue.task_done()

    def _connect_put_node(self, nodes, part, path, headers,
                          logger_thread_locals):
        """
        Make a connection for a replicated object.

        Connects to the first working node that it finds in node_iter
        and sends over the request headers. Returns an HTTPConnection
        object to handle the rest of the streaming.
        """
        self.app.logger.thread_locals = logger_thread_locals
        for node in nodes:
            try:
                start_time = time.time()
                with ConnectionTimeout(self.app.conn_timeout):
                    conn = http_connect(
                        node['ip'], node['port'], node['device'], part, 'PUT',
                        path, headers)
                self.app.set_node_timing(node, time.time() - start_time)
                with Timeout(self.app.node_timeout):
                    resp = conn.getexpect()
                if resp.status == HTTP_CONTINUE:
                    conn.resp = None
                    conn.node = node
                    return conn
                elif is_success(resp.status) or resp.status == HTTP_CONFLICT:
                    conn.resp = resp
                    conn.node = node
                    return conn
                elif headers['If-None-Match'] is not None and \
                        resp.status == HTTP_PRECONDITION_FAILED:
                    conn.resp = resp
                    conn.node = node
                    return conn
                elif resp.status == HTTP_INSUFFICIENT_STORAGE:
                    self.app.error_limit(node, _('ERROR Insufficient Storage'))
                elif is_server_error(resp.status):
                    self.app.error_occurred(
                        node,
                        _('ERROR %(status)d Expect: 100-continue '
                          'From Object Server') % {
                              'status': resp.status})
            except (Exception, Timeout):
                self.app.exception_occurred(
                    node, _('Object'),
                    _('Expect: 100-continue on %s') % path)

    def _await_response(self, conn, **kwargs):
        with Timeout(self.app.node_timeout):
            if conn.resp:
                return conn.resp
            else:
                return conn.getresponse()

    def _get_conn_response(self, conn, req, **kwargs):
        try:
            resp = self._await_response(conn, **kwargs)
            return (conn, resp)
        except (Exception, Timeout):
            self.app.exception_occurred(
                conn.node, _('Object'),
                _('Trying to get final status of PUT to %s') % req.path)
        return (None, None)

    def _get_put_responses(self, req, conns, nodes, **kwargs):
        """
        Collect replicated object responses.
        """
        statuses = []
        reasons = []
        bodies = []
        etags = set()

        pile = GreenAsyncPile(len(conns))
        for conn in conns:
            pile.spawn(self._get_conn_response, conn, req)

        def _handle_response(conn, response):
            statuses.append(response.status)
            reasons.append(response.reason)
            bodies.append(response.read())
            if response.status == HTTP_INSUFFICIENT_STORAGE:
                self.app.error_limit(conn.node,
                                     _('ERROR Insufficient Storage'))
            elif response.status >= HTTP_INTERNAL_SERVER_ERROR:
                self.app.error_occurred(
                    conn.node,
                    _('ERROR %(status)d %(body)s From Object Server '
                      're: %(path)s') %
                    {'status': response.status,
                     'body': bodies[-1][:1024], 'path': req.path})
            elif is_success(response.status):
                etags.add(response.getheader('etag').strip('"'))

        for (conn, response) in pile:
            if response:
                _handle_response(conn, response)
                if self.have_quorum(statuses, len(nodes)):
                    break

        # give any pending requests *some* chance to finish
        finished_quickly = pile.waitall(self.app.post_quorum_timeout)
        for (conn, response) in finished_quickly:
            if response:
                _handle_response(conn, response)

        while len(statuses) < len(nodes):
            statuses.append(HTTP_SERVICE_UNAVAILABLE)
            reasons.append('')
            bodies.append('')
        return statuses, reasons, bodies, etags

    def _config_obj_expiration(self, req):
        delete_at_container = None
        delete_at_part = None
        delete_at_nodes = None

        req = constraints.check_delete_headers(req)

        if 'x-delete-at' in req.headers:
            x_delete_at = int(normalize_delete_at_timestamp(
                int(req.headers['x-delete-at'])))

            req.environ.setdefault('swift.log_info', []).append(
                'x-delete-at:%s' % x_delete_at)

            delete_at_container = get_expirer_container(
                x_delete_at, self.app.expiring_objects_container_divisor,
                self.account_name, self.container_name, self.object_name)

            delete_at_part, delete_at_nodes = \
                self.app.container_ring.get_nodes(
                    self.app.expiring_objects_account, delete_at_container)

        return req, delete_at_container, delete_at_part, delete_at_nodes

    def _handle_copy_request(self, req):
        """
        This method handles copying objects based on values set in the headers
        'X-Copy-From' and 'X-Copy-From-Account'

        This method was added as part of the refactoring of the PUT method and
        the functionality is expected to be moved to middleware
        """
        if req.environ.get('swift.orig_req_method', req.method) != 'POST':
            req.environ.setdefault('swift.log_info', []).append(
                'x-copy-from:%s' % req.headers['X-Copy-From'])
        ver, acct, _rest = req.split_path(2, 3, True)
        src_account_name = req.headers.get('X-Copy-From-Account', None)
        if src_account_name:
            src_account_name = check_account_format(req, src_account_name)
        else:
            src_account_name = acct
        src_container_name, src_obj_name = check_copy_from_header(req)
        source_header = '/%s/%s/%s/%s' % (
            ver, src_account_name, src_container_name, src_obj_name)
        source_req = req.copy_get()

        # make sure the source request uses it's container_info
        source_req.headers.pop('X-Backend-Storage-Policy-Index', None)
        source_req.path_info = source_header
        source_req.headers['X-Newest'] = 'true'

        orig_obj_name = self.object_name
        orig_container_name = self.container_name
        orig_account_name = self.account_name
        sink_req = Request.blank(req.path_info,
                                 environ=req.environ, headers=req.headers)

        self.object_name = src_obj_name
        self.container_name = src_container_name
        self.account_name = src_account_name

        source_resp = self.GET(source_req)

        # This gives middlewares a way to change the source; for example,
        # this lets you COPY a SLO manifest and have the new object be the
        # concatenation of the segments (like what a GET request gives
        # the client), not a copy of the manifest file.
        hook = req.environ.get(
            'swift.copy_hook',
            (lambda source_req, source_resp, sink_req: source_resp))
        source_resp = hook(source_req, source_resp, sink_req)

        # reset names
        self.object_name = orig_obj_name
        self.container_name = orig_container_name
        self.account_name = orig_account_name

        if source_resp.status_int >= HTTP_MULTIPLE_CHOICES:
            # this is a bit of ugly code, but I'm willing to live with it
            # until copy request handling moves to middleware
            return source_resp, None, None, None
        if source_resp.content_length is None:
            # This indicates a transfer-encoding: chunked source object,
            # which currently only happens because there are more than
            # CONTAINER_LISTING_LIMIT segments in a segmented object. In
            # this case, we're going to refuse to do the server-side copy.
            raise HTTPRequestEntityTooLarge(request=req)
        if source_resp.content_length > constraints.MAX_FILE_SIZE:
            raise HTTPRequestEntityTooLarge(request=req)

        data_source = iter(source_resp.app_iter)
        sink_req.content_length = source_resp.content_length
        sink_req.etag = source_resp.etag

        # we no longer need the X-Copy-From header
        del sink_req.headers['X-Copy-From']
        if 'X-Copy-From-Account' in sink_req.headers:
            del sink_req.headers['X-Copy-From-Account']
        if not req.content_type_manually_set:
            sink_req.headers['Content-Type'] = \
                source_resp.headers['Content-Type']
        if config_true_value(
                sink_req.headers.get('x-fresh-metadata', 'false')):
            # post-as-copy: ignore new sysmeta, copy existing sysmeta
            condition = lambda k: is_sys_meta('object', k)
            remove_items(sink_req.headers, condition)
            copy_header_subset(source_resp, sink_req, condition)
        else:
            # copy/update existing sysmeta and user meta
            copy_headers_into(source_resp, sink_req)
            copy_headers_into(req, sink_req)

        # copy over x-static-large-object for POSTs and manifest copies
        if 'X-Static-Large-Object' in source_resp.headers and \
                req.params.get('multipart-manifest') == 'get':
            sink_req.headers['X-Static-Large-Object'] = \
                source_resp.headers['X-Static-Large-Object']

        req = sink_req

        def update_response(req, resp):
            acct, path = source_resp.environ['PATH_INFO'].split('/', 3)[2:4]
            resp.headers['X-Copied-From-Account'] = quote(acct)
            resp.headers['X-Copied-From'] = quote(path)
            if 'last-modified' in source_resp.headers:
                resp.headers['X-Copied-From-Last-Modified'] = \
                    source_resp.headers['last-modified']
            copy_headers_into(req, resp)
            return resp

        # this is a bit of ugly code, but I'm willing to live with it
        # until copy request handling moves to middleware
        return None, req, data_source, update_response

    def _handle_object_versions(self, req):
        """
        This method handles versionining of objects in containers that
        have the feature enabled.

        When a new PUT request is sent, the proxy checks for previous versions
        of that same object name. If found, it is copied to a different
        container and the new version is stored in its place.

        This method was added as part of the PUT method refactoring and the
        functionality is expected to be moved to middleware
        """
        container_info = self.container_info(
            self.account_name, self.container_name, req)
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                       container_info['storage_policy'])
        obj_ring = self.app.get_object_ring(policy_index)
        partition, nodes = obj_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        object_versions = container_info['versions']

        # do a HEAD request for checking object versions
        if object_versions and not req.environ.get('swift_versioned_copy'):
            # make sure proxy-server uses the right policy index
            _headers = {'X-Backend-Storage-Policy-Index': policy_index,
                        'X-Newest': 'True'}
            hreq = Request.blank(req.path_info, headers=_headers,
                                 environ={'REQUEST_METHOD': 'HEAD'})
            hnode_iter = self.app.iter_nodes(obj_ring, partition)
            hresp = self.GETorHEAD_base(
                hreq, _('Object'), hnode_iter, partition,
                hreq.swift_entity_path)

            is_manifest = 'X-Object-Manifest' in req.headers or \
                          'X-Object-Manifest' in hresp.headers
            if hresp.status_int != HTTP_NOT_FOUND and not is_manifest:
                # This is a version manifest and needs to be handled
                # differently. First copy the existing data to a new object,
                # then write the data from this request to the version manifest
                # object.
                lcontainer = object_versions.split('/')[0]
                prefix_len = '%03x' % len(self.object_name)
                lprefix = prefix_len + self.object_name + '/'
                ts_source = hresp.environ.get('swift_x_timestamp')
                if ts_source is None:
                    ts_source = time.mktime(time.strptime(
                                            hresp.headers['last-modified'],
                                            '%a, %d %b %Y %H:%M:%S GMT'))
                new_ts = Timestamp(ts_source).internal
                vers_obj_name = lprefix + new_ts
                copy_headers = {
                    'Destination': '%s/%s' % (lcontainer, vers_obj_name)}
                copy_environ = {'REQUEST_METHOD': 'COPY',
                                'swift_versioned_copy': True
                                }
                copy_req = Request.blank(req.path_info, headers=copy_headers,
                                         environ=copy_environ)
                copy_resp = self.COPY(copy_req)
                if is_client_error(copy_resp.status_int):
                    # missing container or bad permissions
                    raise HTTPPreconditionFailed(request=req)
                elif not is_success(copy_resp.status_int):
                    # could not copy the data, bail
                    raise HTTPServiceUnavailable(request=req)

    def _update_content_type(self, req):
        # Sometimes the 'content-type' header exists, but is set to None.
        req.content_type_manually_set = True
        detect_content_type = \
            config_true_value(req.headers.get('x-detect-content-type'))
        if detect_content_type or not req.headers.get('content-type'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            req.headers['Content-Type'] = guessed_type or \
                'application/octet-stream'
            if detect_content_type:
                req.headers.pop('x-detect-content-type')
            else:
                req.content_type_manually_set = False

    def _update_x_timestamp(self, req):
        # Used by container sync feature
        if 'x-timestamp' in req.headers:
            try:
                req_timestamp = Timestamp(req.headers['X-Timestamp'])
            except ValueError:
                raise HTTPBadRequest(
                    request=req, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % req.headers['x-timestamp'])
            req.headers['X-Timestamp'] = req_timestamp.internal
        else:
            req.headers['X-Timestamp'] = Timestamp(time.time()).internal
        return None

    def _check_failure_put_connections(self, conns, req, nodes, min_conns):
        """
        Identify any failed connections and check minimum connection count.
        """
        if req.if_none_match is not None and '*' in req.if_none_match:
            statuses = [conn.resp.status for conn in conns if conn.resp]
            if HTTP_PRECONDITION_FAILED in statuses:
                # If we find any copy of the file, it shouldn't be uploaded
                self.app.logger.debug(
                    _('Object PUT returning 412, %(statuses)r'),
                    {'statuses': statuses})
                raise HTTPPreconditionFailed(request=req)

        if any(conn for conn in conns if conn.resp and
               conn.resp.status == HTTP_CONFLICT):
            timestamps = [HeaderKeyDict(conn.resp.getheaders()).get(
                'X-Backend-Timestamp') for conn in conns if conn.resp]
            self.app.logger.debug(
                _('Object PUT returning 202 for 409: '
                  '%(req_timestamp)s <= %(timestamps)r'),
                {'req_timestamp': req.timestamp.internal,
                 'timestamps': ', '.join(timestamps)})
            raise HTTPAccepted(request=req)

        self._check_min_conn(req, conns, min_conns)

    def _get_put_connections(self, req, nodes, partition, outgoing_headers,
                             policy, expect):
        """
        Establish connections to storage nodes for PUT request
        """
        obj_ring = policy.object_ring
        node_iter = GreenthreadSafeIterator(
            self.iter_nodes_local_first(obj_ring, partition))
        pile = GreenPile(len(nodes))

        for nheaders in outgoing_headers:
            if expect:
                nheaders['Expect'] = '100-continue'
            pile.spawn(self._connect_put_node, node_iter, partition,
                       req.swift_entity_path, nheaders,
                       self.app.logger.thread_locals)

        conns = [conn for conn in pile if conn]

        return conns

    def _check_min_conn(self, req, conns, min_conns, msg=None):
        msg = msg or 'Object PUT returning 503, %(conns)s/%(nodes)s ' \
            'required connections'

        if len(conns) < min_conns:
            self.app.logger.error((msg),
                                  {'conns': len(conns), 'nodes': min_conns})
            raise HTTPServiceUnavailable(request=req)

    def _transfer_data(self, req, data_source, conns, nodes):
        """
        Transfer data for a replicated object.

        This method was added in the PUT method extraction change
        """
        min_conns = quorum_size(len(nodes))
        bytes_transferred = 0
        try:
            with ContextPool(len(nodes)) as pool:
                for conn in conns:
                    conn.failed = False
                    conn.queue = Queue(self.app.put_queue_depth)
                    pool.spawn(self._send_file, conn, req.path)
                while True:
                    with ChunkReadTimeout(self.app.client_timeout):
                        try:
                            chunk = next(data_source)
                        except StopIteration:
                            if req.is_chunked:
                                for conn in conns:
                                    conn.queue.put('0\r\n\r\n')
                            break
                    bytes_transferred += len(chunk)
                    if bytes_transferred > constraints.MAX_FILE_SIZE:
                        raise HTTPRequestEntityTooLarge(request=req)
                    for conn in list(conns):
                        if not conn.failed:
                            conn.queue.put(
                                '%x\r\n%s\r\n' % (len(chunk), chunk)
                                if req.is_chunked else chunk)
                        else:
                            conn.close()
                            conns.remove(conn)
                    self._check_min_conn(
                        req, conns, min_conns,
                        msg='Object PUT exceptions during'
                            ' send, %(conns)s/%(nodes)s required connections')
                for conn in conns:
                    if conn.queue.unfinished_tasks:
                        conn.queue.join()
            conns = [conn for conn in conns if not conn.failed]
            self._check_min_conn(
                req, conns, min_conns,
                msg='Object PUT exceptions after last send, '
                '%(conns)s/%(nodes)s required connections')
        except ChunkReadTimeout as err:
            self.app.logger.warn(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            self.app.logger.increment('client_timeouts')
            raise HTTPRequestTimeout(request=req)
        except HTTPException:
            raise
        except (Exception, Timeout):
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            raise HTTPClientDisconnect(request=req)
        if req.content_length and bytes_transferred < req.content_length:
            req.client_disconnect = True
            self.app.logger.warn(
                _('Client disconnected without sending enough data'))
            self.app.logger.increment('client_disconnects')
            raise HTTPClientDisconnect(request=req)

    def _store_object(self, req, data_source, nodes, partition,
                      outgoing_headers):
        """
        Store a replicated object.

        This method is responsible for establishing connection
        with storage nodes and sending object to each one of those
        nodes. After sending the data, the "best" response will be
        returned based on statuses from all connections
        """
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index')
        policy = POLICIES.get_by_index(policy_index)
        if not nodes:
            return HTTPNotFound()

        # RFC2616:8.2.3 disallows 100-continue without a body
        if (req.content_length > 0) or req.is_chunked:
            expect = True
        else:
            expect = False
        conns = self._get_put_connections(req, nodes, partition,
                                          outgoing_headers, policy, expect)
        min_conns = quorum_size(len(nodes))
        try:
            # check that a minimum number of connections were established and
            # meet all the correct conditions set in the request
            self._check_failure_put_connections(conns, req, nodes, min_conns)

            # transfer data
            self._transfer_data(req, data_source, conns, nodes)

            # get responses
            statuses, reasons, bodies, etags = self._get_put_responses(
                req, conns, nodes)
        except HTTPException as resp:
            return resp
        finally:
            for conn in conns:
                conn.close()

        if len(etags) > 1:
            self.app.logger.error(
                _('Object servers returned %s mismatched etags'), len(etags))
            return HTTPServerError(request=req)
        etag = etags.pop() if len(etags) else None
        resp = self.best_response(req, statuses, reasons, bodies,
                                  _('Object PUT'), etag=etag)
        resp.last_modified = math.ceil(
            float(Timestamp(req.headers['X-Timestamp'])))
        return resp

    @public
    @cors_validation
    @delay_denial
    def PUT(self, req):
        """HTTP PUT request handler."""
        if req.if_none_match is not None and '*' not in req.if_none_match:
            # Sending an etag with if-none-match isn't currently supported
            return HTTPBadRequest(request=req, content_type='text/plain',
                                  body='If-None-Match only supports *')
        container_info = self.container_info(
            self.account_name, self.container_name, req)
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                       container_info['storage_policy'])
        obj_ring = self.app.get_object_ring(policy_index)
        container_nodes = container_info['nodes']
        container_partition = container_info['partition']
        partition, nodes = obj_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)

        # pass the policy index to storage nodes via req header
        req.headers['X-Backend-Storage-Policy-Index'] = policy_index
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']

        # is request authorized
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp

        if not container_info['nodes']:
            return HTTPNotFound(request=req)

        # update content type in case it is missing
        self._update_content_type(req)

        # check constraints on object name and request headers
        error_response = check_object_creation(req, self.object_name) or \
            check_content_type(req)
        if error_response:
            return error_response

        self._update_x_timestamp(req)

        # check if versioning is enabled and handle copying previous version
        self._handle_object_versions(req)

        # check if request is a COPY of an existing object
        source_header = req.headers.get('X-Copy-From')
        if source_header:
            error_response, req, data_source, update_response = \
                self._handle_copy_request(req)
            if error_response:
                return error_response
        else:
            reader = req.environ['wsgi.input'].read
            data_source = iter(lambda: reader(self.app.client_chunk_size), '')
            update_response = lambda req, resp: resp

        # check if object is set to be automaticaly deleted (i.e. expired)
        req, delete_at_container, delete_at_part, \
            delete_at_nodes = self._config_obj_expiration(req)

        # add special headers to be handled by storage nodes
        outgoing_headers = self._backend_requests(
            req, len(nodes), container_partition, container_nodes,
            delete_at_container, delete_at_part, delete_at_nodes)

        # send object to storage nodes
        resp = self._store_object(
            req, data_source, nodes, partition, outgoing_headers)
        return update_response(req, resp)

    @public
    @cors_validation
    @delay_denial
    def DELETE(self, req):
        """HTTP DELETE request handler."""
        container_info = self.container_info(
            self.account_name, self.container_name, req)
        # pass the policy index to storage nodes via req header
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                       container_info['storage_policy'])
        obj_ring = self.app.get_object_ring(policy_index)
        # pass the policy index to storage nodes via req header
        req.headers['X-Backend-Storage-Policy-Index'] = policy_index
        container_partition = container_info['partition']
        containers = container_info['nodes']
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']
        object_versions = container_info['versions']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if object_versions:
            # this is a version manifest and needs to be handled differently
            object_versions = unquote(object_versions)
            lcontainer = object_versions.split('/')[0]
            prefix_len = '%03x' % len(self.object_name)
            lprefix = prefix_len + self.object_name + '/'
            item_list = []
            try:
                for _item in self._listing_iter(lcontainer, lprefix,
                                                req.environ):
                    item_list.append(_item)
            except ListingIterNotFound:
                # no worries, last_item is None
                pass
            except ListingIterNotAuthorized as err:
                return err.aresp
            except ListingIterError:
                return HTTPServerError(request=req)

            while len(item_list) > 0:
                previous_version = item_list.pop()
                # there are older versions so copy the previous version to the
                # current object and delete the previous version
                orig_container = self.container_name
                orig_obj = self.object_name
                self.container_name = lcontainer
                self.object_name = previous_version['name'].encode('utf-8')

                copy_path = '/v1/' + self.account_name + '/' + \
                            self.container_name + '/' + self.object_name

                copy_headers = {'X-Newest': 'True',
                                'Destination': orig_container + '/' + orig_obj
                                }
                copy_environ = {'REQUEST_METHOD': 'COPY',
                                'swift_versioned_copy': True
                                }
                creq = Request.blank(copy_path, headers=copy_headers,
                                     environ=copy_environ)
                copy_resp = self.COPY(creq)
                if copy_resp.status_int == HTTP_NOT_FOUND:
                    # the version isn't there so we'll try with previous
                    self.container_name = orig_container
                    self.object_name = orig_obj
                    continue
                if is_client_error(copy_resp.status_int):
                    # some user error, maybe permissions
                    return HTTPPreconditionFailed(request=req)
                elif not is_success(copy_resp.status_int):
                    # could not copy the data, bail
                    return HTTPServiceUnavailable(request=req)
                # reset these because the COPY changed them
                self.container_name = lcontainer
                self.object_name = previous_version['name'].encode('utf-8')
                new_del_req = Request.blank(copy_path, environ=req.environ)
                container_info = self.container_info(
                    self.account_name, self.container_name, req)
                policy_idx = container_info['storage_policy']
                obj_ring = self.app.get_object_ring(policy_idx)
                # pass the policy index to storage nodes via req header
                new_del_req.headers['X-Backend-Storage-Policy-Index'] = \
                    policy_idx
                container_partition = container_info['partition']
                containers = container_info['nodes']
                new_del_req.acl = container_info['write_acl']
                new_del_req.path_info = copy_path
                req = new_del_req
                # remove 'X-If-Delete-At', since it is not for the older copy
                if 'X-If-Delete-At' in req.headers:
                    del req.headers['X-If-Delete-At']
                if 'swift.authorize' in req.environ:
                    aresp = req.environ['swift.authorize'](req)
                    if aresp:
                        return aresp
                break
        if not containers:
            return HTTPNotFound(request=req)
        partition, nodes = obj_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)
        # Used by container sync feature
        if 'x-timestamp' in req.headers:
            try:
                req_timestamp = Timestamp(req.headers['X-Timestamp'])
            except ValueError:
                return HTTPBadRequest(
                    request=req, content_type='text/plain',
                    body='X-Timestamp should be a UNIX timestamp float value; '
                         'was %r' % req.headers['x-timestamp'])
            req.headers['X-Timestamp'] = req_timestamp.internal
        else:
            req.headers['X-Timestamp'] = Timestamp(time.time()).internal

        headers = self._backend_requests(
            req, len(nodes), container_partition, containers)
        # When deleting objects treat a 404 status as 204.
        status_overrides = {404: 204}
        resp = self.make_requests(req, obj_ring,
                                  partition, 'DELETE', req.swift_entity_path,
                                  headers, overrides=status_overrides)
        return resp

    def _reroute(self, policy):
        """
        For COPY requests we need to make sure the controller instance the
        request is routed through is the correct type for the policy.
        """
        if not policy:
            raise HTTPServiceUnavailable('Unknown Storage Policy')
        if policy.policy_type != self.policy_type:
            controller = self.app.obj_controller_router[policy](
                self.app, self.account_name, self.container_name,
                self.object_name)
        else:
            controller = self
        return controller

    @public
    @cors_validation
    @delay_denial
    def COPY(self, req):
        """HTTP COPY request handler."""
        if not req.headers.get('Destination'):
            return HTTPPreconditionFailed(request=req,
                                          body='Destination header required')
        dest_account = self.account_name
        if 'Destination-Account' in req.headers:
            dest_account = req.headers.get('Destination-Account')
            dest_account = check_account_format(req, dest_account)
            req.headers['X-Copy-From-Account'] = self.account_name
            self.account_name = dest_account
            del req.headers['Destination-Account']
        dest_container, dest_object = check_destination_header(req)

        source = '/%s/%s' % (self.container_name, self.object_name)
        self.container_name = dest_container
        self.object_name = dest_object
        # re-write the existing request as a PUT instead of creating a new one
        # since this one is already attached to the posthooklogger
        req.method = 'PUT'
        req.path_info = '/v1/%s/%s/%s' % \
                        (dest_account, dest_container, dest_object)
        req.headers['Content-Length'] = 0
        req.headers['X-Copy-From'] = quote(source)
        del req.headers['Destination']

        container_info = self.container_info(
            dest_account, dest_container, req)
        dest_policy = POLICIES.get_by_index(container_info['storage_policy'])

        return self._reroute(dest_policy).PUT(req)


@ObjectControllerRouter.register(REPL_POLICY)
class ReplicatedObjectController(BaseObjectController):

    def _get_or_head_response(self, req, node_iter, partition, policy):
        resp = self.GETorHEAD_base(
            req, _('Object'), node_iter, partition,
            req.swift_entity_path)
        return resp


class ECAppIter(object):
    """
    WSGI iterable that decodes EC fragment archives (or portions thereof)
    into the original object (or portions thereof).

    :param path: path for the request

    :param policy: storage policy for this object

    :param internal_app_iters: list of the WSGI iterables from object server
        GET responses for fragment archives. For an M+K erasure code, the
        caller must supply M such iterables.

    :param range_specs: list of dictionaries describing the ranges requested
        by the client. Each dictionary contains the start and end of the
        client's requested byte range as well as the start and end of the EC
        segments containing that byte range.

    :param obj_length: length of the object, in bytes. Learned from the
        headers in the GET response from the object server.

    :param logger: a logger
    """
    def __init__(self, path, policy, internal_app_iters, range_specs,
                 obj_length, logger):
        self.path = path
        self.policy = policy
        self.internal_app_iters = internal_app_iters
        self.range_specs = range_specs
        self.obj_length = obj_length
        self.boundary = ''
        self.logger = logger

    def close(self):
        for it in self.internal_app_iters:
            close_if_possible(it)

    def __iter__(self):
        segments_iter = self.decode_segments_from_fragments()

        if len(self.range_specs) == 0:
            # plain GET; just yield up segments
            for seg in segments_iter:
                yield seg
            return

        if len(self.range_specs) > 1:
            raise NotImplementedError("multi-range GETs not done yet")

        for range_spec in self.range_specs:
            client_start = range_spec['client_start']
            client_end = range_spec['client_end']
            segment_start = range_spec['segment_start']
            segment_end = range_spec['segment_end']

            seg_size = self.policy.ec_segment_size
            is_suffix = client_start is None

            if is_suffix:
                # Suffix byte ranges (i.e. requests for the last N bytes of
                # an object) are likely to end up not on a segment boundary.
                client_range_len = client_end
                client_start = max(self.obj_length - client_range_len, 0)
                client_end = self.obj_length - 1

                # may be mid-segment; if it is, then everything up to the
                # first segment boundary is garbage, and is discarded before
                # ever getting into this function.
                unaligned_segment_start = max(self.obj_length - segment_end, 0)
                alignment_offset = (
                    (seg_size - (unaligned_segment_start % seg_size))
                    % seg_size)
                segment_start = unaligned_segment_start + alignment_offset
                segment_end = self.obj_length - 1
            else:
                # It's entirely possible that the client asked for a range that
                # includes some bytes we have and some we don't; for example, a
                # range of bytes 1000-20000000 on a 1500-byte object.
                segment_end = (min(segment_end, self.obj_length - 1)
                               if segment_end is not None
                               else self.obj_length - 1)
                client_end = (min(client_end, self.obj_length - 1)
                              if client_end is not None
                              else self.obj_length - 1)

            num_segments = int(
                math.ceil(float(segment_end + 1 - segment_start)
                          / self.policy.ec_segment_size))
            # We get full segments here, but the client may have requested a
            # byte range that begins or ends in the middle of a segment.
            # Thus, we have some amount of overrun (extra decoded bytes)
            # that we trim off so the client gets exactly what they
            # requested.
            start_overrun = client_start - segment_start
            end_overrun = segment_end - client_end

            for i, next_seg in enumerate(segments_iter):
                # We may have a start_overrun of more than one segment in
                # the case of suffix-byte-range requests. However, we never
                # have an end_overrun of more than one segment.
                if start_overrun > 0:
                    seglen = len(next_seg)
                    if seglen <= start_overrun:
                        start_overrun -= seglen
                        continue
                    else:
                        next_seg = next_seg[start_overrun:]
                        start_overrun = 0

                if i == (num_segments - 1) and end_overrun:
                    next_seg = next_seg[:-end_overrun]

                yield next_seg

    def decode_segments_from_fragments(self):
        # Decodes the fragments from the object servers and yields one
        # segment at a time.
        queues = [Queue(1) for _junk in range(len(self.internal_app_iters))]

        def put_fragments_in_queue(frag_iter, queue):
            try:
                for fragment in frag_iter:
                    if fragment[0] == ' ':
                        raise Exception('Leading whitespace on fragment.')
                    queue.put(fragment)
            except GreenletExit:
                # killed by contextpool
                pass
            except ChunkReadTimeout:
                # unable to resume in GetOrHeadHandler
                pass
            except:  # noqa
                self.logger.exception("Exception fetching fragments for %r" %
                                      self.path)
            finally:
                queue.resize(2)  # ensure there's room
                queue.put(None)

        with ContextPool(len(self.internal_app_iters)) as pool:
            for app_iter, queue in zip(
                    self.internal_app_iters, queues):
                pool.spawn(put_fragments_in_queue, app_iter, queue)

            while True:
                fragments = []
                for qi, queue in enumerate(queues):
                    fragment = queue.get()
                    queue.task_done()
                    fragments.append(fragment)

                # If any object server connection yields out a None; we're
                # done.  Either they are all None, and we've finished
                # successfully; or some un-recoverable failure has left us
                # with an un-reconstructible list of fragments - so we'll
                # break out of the iter so WSGI can tear down the broken
                # connection.
                if not all(fragments):
                    break
                try:
                    segment = self.policy.pyeclib_driver.decode(fragments)
                except ECDriverError:
                    self.logger.exception("Error decoding fragments for %r" %
                                          self.path)
                    raise

                yield segment

    def app_iter_range(self, start, end):
        return self

    def app_iter_ranges(self, content_type, boundary, content_size):
        self.boundary = boundary


def client_range_to_segment_range(client_start, client_end, segment_size):
    """
    Takes a byterange from the client and converts it into a byterange
    spanning the necessary segments.

    Handles prefix, suffix, and fully-specified byte ranges.

    Examples:
        client_range_to_segment_range(100, 700, 512) = (0, 1023)
        client_range_to_segment_range(100, 700, 256) = (0, 767)
        client_range_to_segment_range(300, None, 256) = (256, None)

    :param client_start: first byte of the range requested by the client
    :param client_end: last byte of the range requested by the client
    :param segment_size: size of an EC segment, in bytes

    :returns: a 2-tuple (seg_start, seg_end) where

      * seg_start is the first byte of the first segment, or None if this is
        a suffix byte range

      * seg_end is the last byte of the last segment, or None if this is a
        prefix byte range
    """
    # the index of the first byte of the first segment
    segment_start = (
        int(client_start // segment_size)
        * segment_size) if client_start is not None else None
    # the index of the last byte of the last segment
    segment_end = (
        # bytes M-
        None if client_end is None else
        # bytes M-N
        (((int(client_end // segment_size) + 1)
          * segment_size) - 1) if client_start is not None else
        # bytes -N: we get some extra bytes to make sure we
        # have all we need.
        #
        # To see why, imagine a 100-byte segment size, a
        # 340-byte object, and a request for the last 50
        # bytes. Naively requesting the last 100 bytes would
        # result in a truncated first segment and hence a
        # truncated download. (Of course, the actual
        # obj-server requests are for fragments, not
        # segments, but that doesn't change the
        # calculation.)
        #
        # This does mean that we fetch an extra segment if
        # the object size is an exact multiple of the
        # segment size. It's a little wasteful, but it's
        # better to be a little wasteful than to get some
        # range requests completely wrong.
        (int(math.ceil((
            float(client_end) / segment_size) + 1))  # nsegs
         * segment_size))
    return (segment_start, segment_end)


def segment_range_to_fragment_range(segment_start, segment_end, segment_size,
                                    fragment_size):
    """
    Takes a byterange spanning some segments and converts that into a
    byterange spanning the corresponding fragments within their fragment
    archives.

    Handles prefix, suffix, and fully-specified byte ranges.

    :param segment_start: first byte of the first segment
    :param segment_end: last byte of the last segment
    :param segment_size: size of an EC segment, in bytes
    :param fragment_size: size of an EC fragment, in bytes

    :returns: a 2-tuple (frag_start, frag_end) where

      * frag_start is the first byte of the first fragment, or None if this
        is a suffix byte range

      * frag_end is the last byte of the last fragment, or None if this is a
        prefix byte range
    """
    # Note: segment_start and (segment_end + 1) are
    # multiples of segment_size, so we don't have to worry
    # about integer math giving us rounding troubles.
    #
    # There's a whole bunch of +1 and -1 in here; that's because HTTP wants
    # byteranges to be inclusive of the start and end, so e.g. bytes 200-300
    # is a range containing 101 bytes. Python has half-inclusive ranges, of
    # course, so we have to convert back and forth. We try to keep things in
    # HTTP-style byteranges for consistency.

    # the index of the first byte of the first fragment
    fragment_start = ((
        segment_start / segment_size * fragment_size)
        if segment_start is not None else None)
    # the index of the last byte of the last fragment
    fragment_end = (
        # range unbounded on the right
        None if segment_end is None else
        # range unbounded on the left; no -1 since we're
        # asking for the last N bytes, not to have a
        # particular byte be the last one
        ((segment_end + 1) / segment_size
         * fragment_size) if segment_start is None else
        # range bounded on both sides; the -1 is because the
        # rest of the expression computes the length of the
        # fragment, and a range of N bytes starts at index M
        # and ends at M + N - 1.
        ((segment_end + 1) / segment_size * fragment_size) - 1)
    return (fragment_start, fragment_end)


NO_DATA_SENT = 1
SENDING_DATA = 2
DATA_SENT = 3
DATA_ACKED = 4
COMMIT_SENT = 5


class ECPutter(object):
    """
    This is here mostly to wrap up the fact that all EC PUTs are
    chunked because of the mime boundary footer trick and the first
    half of the two-phase PUT conversation handling.

    An HTTP PUT request that supports streaming.

    Probably deserves more docs than this, but meh.
    """
    def __init__(self, conn, node, resp, path, connect_duration,
                 mime_boundary):
        # Note: you probably want to call Putter.connect() instead of
        # instantiating one of these directly.
        self.conn = conn
        self.node = node
        self.resp = resp
        self.path = path
        self.connect_duration = connect_duration
        # for handoff nodes node_index is None
        self.node_index = node.get('index')
        self.mime_boundary = mime_boundary
        self.chunk_hasher = md5()

        self.failed = False
        self.queue = None
        self.state = NO_DATA_SENT

    def current_status(self):
        """
        Returns the current status of the response.

        A response starts off with no current status, then may or may not have
        a status of 100 for some time, and then ultimately has a final status
        like 200, 404, et cetera.
        """
        return self.resp.status

    def await_response(self, timeout, informational=False):
        """
        Get 100-continue response indicating the end of 1st phase of a 2-phase
        commit or the final response, i.e. the one with status >= 200.

        Might or might not actually wait for anything. If we said Expect:
        100-continue but got back a non-100 response, that'll be the thing
        returned, and we won't do any network IO to get it. OTOH, if we got
        a 100 Continue response and sent up the PUT request's body, then
        we'll actually read the 2xx-5xx response off the network here.

        :returns: HTTPResponse
        :raises: Timeout if the response took too long
        """
        conn = self.conn
        with Timeout(timeout):
            if not conn.resp:
                if informational:
                    self.resp = conn.getexpect()
                else:
                    self.resp = conn.getresponse()
            return self.resp

    def spawn_sender_greenthread(self, pool, queue_depth, write_timeout,
                                 exception_handler):
        """Call before sending the first chunk of request body"""
        self.queue = Queue(queue_depth)
        pool.spawn(self._send_file, write_timeout, exception_handler)

    def wait(self):
        if self.queue.unfinished_tasks:
            self.queue.join()

    def _start_mime_doc_object_body(self):
        self.queue.put("--%s\r\nX-Document: object body\r\n\r\n" %
                       (self.mime_boundary,))

    def send_chunk(self, chunk):
        if not chunk:
            # If we're not using chunked transfer-encoding, sending a 0-byte
            # chunk is just wasteful. If we *are* using chunked
            # transfer-encoding, sending a 0-byte chunk terminates the
            # request body. Neither one of these is good.
            return
        elif self.state == DATA_SENT:
            raise ValueError("called send_chunk after end_of_object_data")

        if self.state == NO_DATA_SENT and self.mime_boundary:
            # We're sending the object plus other stuff in the same request
            # body, all wrapped up in multipart MIME, so we'd better start
            # off the MIME document before sending any object data.
            self._start_mime_doc_object_body()
            self.state = SENDING_DATA

        self.queue.put(chunk)

    def end_of_object_data(self, footer_metadata):
        """
        Call when there is no more data to send.

        :param footer_metadata: dictionary of metadata items
        """
        if self.state == DATA_SENT:
            raise ValueError("called end_of_object_data twice")
        elif self.state == NO_DATA_SENT and self.mime_boundary:
            self._start_mime_doc_object_body()

        footer_body = json.dumps(footer_metadata)
        footer_md5 = md5(footer_body).hexdigest()

        tail_boundary = ("--%s" % (self.mime_boundary,))

        message_parts = [
            ("\r\n--%s\r\n" % self.mime_boundary),
            "X-Document: object metadata\r\n",
            "Content-MD5: %s\r\n" % footer_md5,
            "\r\n",
            footer_body, "\r\n",
            tail_boundary, "\r\n",
        ]
        self.queue.put("".join(message_parts))

        self.queue.put('')
        self.state = DATA_SENT

    def send_commit_confirmation(self):
        """
        Call when there are > quorum 2XX responses received.  Send commit
        confirmations to all object nodes to finalize the PUT.
        """
        if self.state == COMMIT_SENT:
            raise ValueError("called send_commit_confirmation twice")

        self.state = DATA_ACKED

        if self.mime_boundary:
            body = "put_commit_confirmation"
            tail_boundary = ("--%s--" % (self.mime_boundary,))
            message_parts = [
                "X-Document: put commit\r\n",
                "\r\n",
                body, "\r\n",
                tail_boundary,
            ]
            self.queue.put("".join(message_parts))

        self.queue.put('')
        self.state = COMMIT_SENT

    def _send_file(self, write_timeout, exception_handler):
        """
        Method for a file PUT coro. Takes chunks from a queue and sends them
        down a socket.

        If something goes wrong, the "failed" attribute will be set to true
        and the exception handler will be called.
        """
        while True:
            chunk = self.queue.get()
            if not self.failed:
                to_send = "%x\r\n%s\r\n" % (len(chunk), chunk)
                try:
                    with ChunkWriteTimeout(write_timeout):
                        self.conn.send(to_send)
                except (Exception, ChunkWriteTimeout):
                    self.failed = True
                    exception_handler(self.conn.node, _('Object'),
                                      _('Trying to write to %s') % self.path)
            self.queue.task_done()

    @classmethod
    def connect(cls, node, part, path, headers, conn_timeout, node_timeout,
                chunked=False):
        """
        Connect to a backend node and send the headers.

        :returns: Putter instance

        :raises: ConnectionTimeout if initial connection timed out
        :raises: ResponseTimeout if header retrieval timed out
        :raises: InsufficientStorage on 507 response from node
        :raises: PutterConnectError on non-507 server error response from node
        :raises: FooterNotSupported if need_metadata_footer is set but
                 backend node can't process footers
        :raises: MultiphasePUTNotSupported if need_multiphase_support is
                 set but backend node can't handle multiphase PUT
        """
        mime_boundary = "%.64x" % random.randint(0, 16 ** 64)
        headers = HeaderKeyDict(headers)
        # We're going to be adding some unknown amount of data to the
        # request, so we can't use an explicit content length, and thus
        # we must use chunked encoding.
        headers['Transfer-Encoding'] = 'chunked'
        headers['Expect'] = '100-continue'
        if 'Content-Length' in headers:
            headers['X-Backend-Obj-Content-Length'] = \
                headers.pop('Content-Length')

        headers['X-Backend-Obj-Multipart-Mime-Boundary'] = mime_boundary

        headers['X-Backend-Obj-Metadata-Footer'] = 'yes'

        headers['X-Backend-Obj-Multiphase-Commit'] = 'yes'

        start_time = time.time()
        with ConnectionTimeout(conn_timeout):
            conn = http_connect(node['ip'], node['port'], node['device'],
                                part, 'PUT', path, headers)
        connect_duration = time.time() - start_time

        with ResponseTimeout(node_timeout):
            resp = conn.getexpect()

        if resp.status == HTTP_INSUFFICIENT_STORAGE:
            raise InsufficientStorage

        if is_server_error(resp.status):
            raise PutterConnectError(resp.status)

        if is_informational(resp.status):
            continue_headers = HeaderKeyDict(resp.getheaders())
            can_send_metadata_footer = config_true_value(
                continue_headers.get('X-Obj-Metadata-Footer', 'no'))
            can_handle_multiphase_put = config_true_value(
                continue_headers.get('X-Obj-Multiphase-Commit', 'no'))

            if not can_send_metadata_footer:
                raise FooterNotSupported()

            if not can_handle_multiphase_put:
                raise MultiphasePUTNotSupported()

        conn.node = node
        conn.resp = None
        if is_success(resp.status) or resp.status == HTTP_CONFLICT:
            conn.resp = resp
        elif (headers.get('If-None-Match', None) is not None and
              resp.status == HTTP_PRECONDITION_FAILED):
            conn.resp = resp

        return cls(conn, node, resp, path, connect_duration, mime_boundary)


def chunk_transformer(policy, nstreams):
    segment_size = policy.ec_segment_size

    buf = collections.deque()
    total_buf_len = 0

    chunk = yield
    while chunk:
        buf.append(chunk)
        total_buf_len += len(chunk)
        if total_buf_len >= segment_size:
            chunks_to_encode = []
            # extract as many chunks as we can from the input buffer
            while total_buf_len >= segment_size:
                to_take = segment_size
                pieces = []
                while to_take > 0:
                    piece = buf.popleft()
                    if len(piece) > to_take:
                        buf.appendleft(piece[to_take:])
                        piece = piece[:to_take]
                    pieces.append(piece)
                    to_take -= len(piece)
                    total_buf_len -= len(piece)
                chunks_to_encode.append(''.join(pieces))

            frags_by_byte_order = []
            for chunk_to_encode in chunks_to_encode:
                frags_by_byte_order.append(
                    policy.pyeclib_driver.encode(chunk_to_encode))
            # Sequential calls to encode() have given us a list that
            # looks like this:
            #
            # [[frag_A1, frag_B1, frag_C1, ...],
            #  [frag_A2, frag_B2, frag_C2, ...], ...]
            #
            # What we need is a list like this:
            #
            # [(frag_A1 + frag_A2 + ...),  # destined for node A
            #  (frag_B1 + frag_B2 + ...),  # destined for node B
            #  (frag_C1 + frag_C2 + ...),  # destined for node C
            #  ...]
            obj_data = [''.join(frags)
                        for frags in zip(*frags_by_byte_order)]
            chunk = yield obj_data
        else:
            # didn't have enough data to encode
            chunk = yield None

    # Now we've gotten an empty chunk, which indicates end-of-input.
    # Take any leftover bytes and encode them.
    last_bytes = ''.join(buf)
    if last_bytes:
        last_frags = policy.pyeclib_driver.encode(last_bytes)
        yield last_frags
    else:
        yield [''] * nstreams


def trailing_metadata(policy, client_obj_hasher,
                      bytes_transferred_from_client,
                      fragment_archive_index):
    return {
        # etag and size values are being added twice here.
        # The container override header is used to update the container db
        # with these values as they represent the correct etag and size for
        # the whole object and not just the FA.
        # The object sysmeta headers will be saved on each FA of the object.
        'X-Object-Sysmeta-EC-Etag': client_obj_hasher.hexdigest(),
        'X-Object-Sysmeta-EC-Content-Length':
        str(bytes_transferred_from_client),
        'X-Backend-Container-Update-Override-Etag':
        client_obj_hasher.hexdigest(),
        'X-Backend-Container-Update-Override-Size':
        str(bytes_transferred_from_client),
        'X-Object-Sysmeta-Ec-Frag-Index': str(fragment_archive_index),
        # These fields are for debuggability,
        # AKA "what is this thing?"
        'X-Object-Sysmeta-EC-Scheme': policy.ec_scheme_description,
        'X-Object-Sysmeta-EC-Segment-Size': str(policy.ec_segment_size),
    }


@ObjectControllerRouter.register(EC_POLICY)
class ECObjectController(BaseObjectController):

    def _get_or_head_response(self, req, node_iter, partition, policy):
        req.headers.setdefault("X-Backend-Etag-Is-At",
                               "X-Object-Sysmeta-Ec-Etag")

        if req.method == 'HEAD':
            # no fancy EC decoding here, just one plain old HEAD request to
            # one object server because all fragments hold all metadata
            # information about the object.
            resp = self.GETorHEAD_base(
                req, _('Object'), node_iter, partition,
                req.swift_entity_path)
        else:  # GET request
            orig_range = None
            range_specs = []
            if req.range:
                orig_range = req.range
                # Since segments and fragments have different sizes, we need
                # to modify the Range header sent to the object servers to
                # make sure we get the right fragments out of the fragment
                # archives.
                segment_size = policy.ec_segment_size
                fragment_size = policy.fragment_size

                range_specs = []
                new_ranges = []
                for client_start, client_end in req.range.ranges:

                    segment_start, segment_end = client_range_to_segment_range(
                        client_start, client_end, segment_size)

                    fragment_start, fragment_end = \
                        segment_range_to_fragment_range(
                            segment_start, segment_end,
                            segment_size, fragment_size)

                    new_ranges.append((fragment_start, fragment_end))
                    range_specs.append({'client_start': client_start,
                                        'client_end': client_end,
                                        'segment_start': segment_start,
                                        'segment_end': segment_end})

                req.range = "bytes=" + ",".join(
                    "%s-%s" % (s if s is not None else "",
                               e if e is not None else "")
                    for s, e in new_ranges)

            node_iter = GreenthreadSafeIterator(node_iter)
            num_gets = policy.ec_ndata
            with ContextPool(num_gets) as pool:
                pile = GreenAsyncPile(pool)
                for _junk in range(num_gets):
                    pile.spawn(self.GETorHEAD_base,
                               req, 'Object', node_iter, partition,
                               req.swift_entity_path,
                               client_chunk_size=policy.fragment_size)

                responses = list(pile)
                good_responses = []
                bad_responses = []
                for response in responses:
                    if is_success(response.status_int):
                        good_responses.append(response)
                    else:
                        bad_responses.append(response)

            req.range = orig_range
            if len(good_responses) == num_gets:
                # If these aren't all for the same object, then error out so
                # at least the client doesn't get garbage. We can do a lot
                # better here with more work, but this'll work for now.
                found_obj_etags = set(
                    resp.headers['X-Object-Sysmeta-Ec-Etag']
                    for resp in good_responses)
                if len(found_obj_etags) > 1:
                    self.app.logger.debug(
                        "Returning 503 for %s; found too many etags (%s)",
                        req.path,
                        ", ".join(found_obj_etags))
                    return HTTPServiceUnavailable(request=req)

                # we found enough pieces to decode the object, so now let's
                # decode the object
                resp_headers = HeaderKeyDict(good_responses[0].headers.items())
                resp_headers.pop('Content-Range', None)
                eccl = resp_headers.get('X-Object-Sysmeta-Ec-Content-Length')
                obj_length = int(eccl) if eccl is not None else None

                resp = Response(
                    request=req,
                    headers=resp_headers,
                    conditional_response=True,
                    app_iter=ECAppIter(
                        req.swift_entity_path,
                        policy,
                        [r.app_iter for r in good_responses],
                        range_specs,
                        obj_length,
                        logger=self.app.logger))
            else:
                resp = self.best_response(
                    req,
                    [r.status_int for r in bad_responses],
                    [r.status.split(' ', 1)[1] for r in bad_responses],
                    [r.body for r in bad_responses],
                    'Object',
                    headers=[r.headers for r in bad_responses])

        self._fix_response_headers(resp)
        return resp

    def _fix_response_headers(self, resp):
        # EC fragment archives each have different bytes, hence different
        # etags. However, they all have the original object's etag stored in
        # sysmeta, so we copy that here so the client gets it.
        resp.headers['Etag'] = resp.headers.get(
            'X-Object-Sysmeta-Ec-Etag')
        resp.headers['Content-Length'] = resp.headers.get(
            'X-Object-Sysmeta-Ec-Content-Length')

        return resp

    def _connect_put_node(self, node_iter, part, path, headers,
                          logger_thread_locals):
        """
        Make a connection for a erasure encoded object.

        Connects to the first working node that it finds in node_iter and sends
        over the request headers. Returns a Putter to handle the rest of the
        streaming, or None if no working nodes were found.
        """
        # the object server will get different bytes, so these
        # values do not apply (Content-Length might, in general, but
        # in the specific case of replication vs. EC, it doesn't).
        headers.pop('Content-Length', None)
        headers.pop('Etag', None)

        self.app.logger.thread_locals = logger_thread_locals
        for node in node_iter:
            try:
                putter = ECPutter.connect(
                    node, part, path, headers,
                    conn_timeout=self.app.conn_timeout,
                    node_timeout=self.app.node_timeout)
                self.app.set_node_timing(node, putter.connect_duration)
                return putter
            except InsufficientStorage:
                self.app.error_limit(node, _('ERROR Insufficient Storage'))
            except PutterConnectError as e:
                self.app.error_occurred(
                    node, _('ERROR %(status)d Expect: 100-continue '
                            'From Object Server') % {
                                'status': e.status})
            except (Exception, Timeout):
                self.app.exception_occurred(
                    node, _('Object'),
                    _('Expect: 100-continue on %s') % path)

    def _determine_chunk_destinations(self, putters):
        """
        Given a list of putters, return a dict where the key is the putter
        and the value is the node index to use.

        This is done so that we line up handoffs using the same node index
        (in the primary part list) as the primary that the handoff is standing
        in for.  This lets erasure-code fragment archives wind up on the
        preferred local primary nodes when possible.
        """
        # Give each putter a "chunk index": the index of the
        # transformed chunk that we'll send to it.
        #
        # For primary nodes, that's just its index (primary 0 gets
        # chunk 0, primary 1 gets chunk 1, and so on). For handoffs,
        # we assign the chunk index of a missing primary.
        handoff_conns = []
        chunk_index = {}
        for p in putters:
            if p.node_index is not None:
                chunk_index[p] = p.node_index
            else:
                handoff_conns.append(p)

        # Note: we may have more holes than handoffs. This is okay; it
        # just means that we failed to connect to one or more storage
        # nodes. Holes occur when a storage node is down, in which
        # case the connection is not replaced, and when a storage node
        # returns 507, in which case a handoff is used to replace it.
        holes = [x for x in range(len(putters))
                 if x not in chunk_index.values()]

        for hole, p in zip(holes, handoff_conns):
            chunk_index[p] = hole
        return chunk_index

    def _transfer_data(self, req, policy, data_source, putters, nodes,
                       min_conns, etag_hasher):
        """
        Transfer data for an erasure coded object.

        This method was added in the PUT method extraction change
        """
        bytes_transferred = 0
        chunk_transform = chunk_transformer(policy, len(nodes))
        chunk_transform.send(None)

        def send_chunk(chunk):
            if etag_hasher:
                etag_hasher.update(chunk)
            backend_chunks = chunk_transform.send(chunk)
            if backend_chunks is None:
                # If there's not enough bytes buffered for erasure-encoding
                # or whatever we're doing, the transform will give us None.
                return

            for putter in list(putters):
                backend_chunk = backend_chunks[chunk_index[putter]]
                if not putter.failed:
                    putter.chunk_hasher.update(backend_chunk)
                    putter.send_chunk(backend_chunk)
                else:
                    putters.remove(putter)
            self._check_min_conn(
                req, putters, min_conns, msg='Object PUT exceptions during'
                ' send, %(conns)s/%(nodes)s required connections')

        try:
            with ContextPool(len(putters)) as pool:

                # build our chunk index dict to place handoffs in the
                # same part nodes index as the primaries they are covering
                chunk_index = self._determine_chunk_destinations(putters)

                for putter in putters:
                    putter.spawn_sender_greenthread(
                        pool, self.app.put_queue_depth, self.app.node_timeout,
                        self.app.exception_occurred)
                while True:
                    with ChunkReadTimeout(self.app.client_timeout):
                        try:
                            chunk = next(data_source)
                        except StopIteration:
                            computed_etag = (etag_hasher.hexdigest()
                                             if etag_hasher else None)
                            received_etag = req.headers.get(
                                'etag', '').strip('"')
                            if (computed_etag and received_etag and
                               computed_etag != received_etag):
                                raise HTTPUnprocessableEntity(request=req)

                            send_chunk('')  # flush out any buffered data

                            for putter in putters:
                                trail_md = trailing_metadata(
                                    policy, etag_hasher,
                                    bytes_transferred,
                                    chunk_index[putter])
                                trail_md['Etag'] = \
                                    putter.chunk_hasher.hexdigest()
                                putter.end_of_object_data(trail_md)
                            break
                    bytes_transferred += len(chunk)
                    if bytes_transferred > constraints.MAX_FILE_SIZE:
                        raise HTTPRequestEntityTooLarge(request=req)

                    send_chunk(chunk)

                for putter in putters:
                    putter.wait()

                # for storage policies requiring 2-phase commit (e.g.
                # erasure coding), enforce >= 'quorum' number of
                # 100-continue responses - this indicates successful
                # object data and metadata commit and is a necessary
                # condition to be met before starting 2nd PUT phase
                final_phase = False
                need_quorum = True
                statuses, reasons, bodies, _junk, quorum = \
                    self._get_put_responses(
                        req, putters, len(nodes), final_phase,
                        min_conns, need_quorum=need_quorum)
                if not quorum:
                    self.app.logger.error(
                        _('Not enough object servers ack\'ed (got %d)'),
                        statuses.count(HTTP_CONTINUE))
                    raise HTTPServiceUnavailable(request=req)
                # quorum achieved, start 2nd phase - send commit
                # confirmation to participating object servers
                # so they write a .durable state file indicating
                # a successful PUT
                for putter in putters:
                    putter.send_commit_confirmation()
                for putter in putters:
                    putter.wait()
        except ChunkReadTimeout as err:
            self.app.logger.warn(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            self.app.logger.increment('client_timeouts')
            raise HTTPRequestTimeout(request=req)
        except HTTPException:
            raise
        except (Exception, Timeout):
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            raise HTTPClientDisconnect(request=req)
        if req.content_length and bytes_transferred < req.content_length:
            req.client_disconnect = True
            self.app.logger.warn(
                _('Client disconnected without sending enough data'))
            self.app.logger.increment('client_disconnects')
            raise HTTPClientDisconnect(request=req)

    def _have_adequate_successes(self, statuses, min_responses):
        """
        Given a list of statuses from several requests, determine if a
        satisfactory number of nodes have responded with 2xx statuses to
        deem the transaction for a succssful response to the client.

        :param statuses: list of statuses returned so far
        :param min_responses: minimal pass criterion for number of successes
        :returns: True or False, depending on current number of successes
        """
        if sum(1 for s in statuses if is_success(s)) >= min_responses:
            return True
        return False

    def _await_response(self, conn, final_phase):
        return conn.await_response(
            self.app.node_timeout, not final_phase)

    def _get_conn_response(self, conn, req, final_phase, **kwargs):
        try:
            resp = self._await_response(conn, final_phase=final_phase,
                                        **kwargs)
        except (Exception, Timeout):
            resp = None
            if final_phase:
                status_type = 'final'
            else:
                status_type = 'commit'
            self.app.exception_occurred(
                conn.node, _('Object'),
                _('Trying to get %s status of PUT to %s') % (
                    status_type, req.path))
        return (conn, resp)

    def _get_put_responses(self, req, putters, num_nodes, final_phase,
                           min_responses, need_quorum=True):
        """
        Collect erasure coded object responses.

        Collect object responses to a PUT request and determine if
        satisfactory number of nodes have returned success.  Return
        statuses, quorum result if indicated by 'need_quorum' and
        etags if this is a final phase or a multiphase PUT transaction.

        :param req: the request
        :param putters: list of putters for the request
        :param num_nodes: number of nodes involved
        :param final_phase: boolean indicating if this is the last phase
        :param min_responses: minimum needed when not requiring quorum
        :param need_quorum: boolean indicating if quorum is required
        """
        statuses = []
        reasons = []
        bodies = []
        etags = set()

        pile = GreenAsyncPile(len(putters))
        for putter in putters:
            if putter.failed:
                continue
            pile.spawn(self._get_conn_response, putter, req,
                       final_phase=final_phase)

        def _handle_response(putter, response):
            statuses.append(response.status)
            reasons.append(response.reason)
            if final_phase:
                body = response.read()
                bodies.append(body)
            else:
                body = ''
            if response.status == HTTP_INSUFFICIENT_STORAGE:
                putter.failed = True
                self.app.error_limit(putter.node,
                                     _('ERROR Insufficient Storage'))
            elif response.status >= HTTP_INTERNAL_SERVER_ERROR:
                putter.failed = True
                self.app.error_occurred(
                    putter.node,
                    _('ERROR %(status)d %(body)s From Object Server '
                      're: %(path)s') %
                    {'status': response.status,
                     'body': body[:1024], 'path': req.path})
            elif is_success(response.status):
                etags.add(response.getheader('etag').strip('"'))

        quorum = False
        for (putter, response) in pile:
            if response:
                _handle_response(putter, response)
                if self._have_adequate_successes(statuses, min_responses):
                    break
            else:
                putter.failed = True

        # give any pending requests *some* chance to finish
        finished_quickly = pile.waitall(self.app.post_quorum_timeout)
        for (putter, response) in finished_quickly:
            if response:
                _handle_response(putter, response)

        if need_quorum:
            if final_phase:
                while len(statuses) < num_nodes:
                    statuses.append(HTTP_SERVICE_UNAVAILABLE)
                    reasons.append('')
                    bodies.append('')
            else:
                # intermediate response phase - set return value to true only
                # if there are enough 100-continue acknowledgements
                if self.have_quorum(statuses, num_nodes):
                    quorum = True

        return statuses, reasons, bodies, etags, quorum

    def _store_object(self, req, data_source, nodes, partition,
                      outgoing_headers):
        """
        Store an erasure coded object.
        """
        policy_index = int(req.headers.get('X-Backend-Storage-Policy-Index'))
        policy = POLICIES.get_by_index(policy_index)
        # Since the request body sent from client -> proxy is not
        # the same as the request body sent proxy -> object, we
        # can't rely on the object-server to do the etag checking -
        # so we have to do it here.
        etag_hasher = md5()

        min_conns = policy.quorum
        putters = self._get_put_connections(
            req, nodes, partition, outgoing_headers,
            policy, expect=True)

        try:
            # check that a minimum number of connections were established and
            # meet all the correct conditions set in the request
            self._check_failure_put_connections(putters, req, nodes, min_conns)

            self._transfer_data(req, policy, data_source, putters,
                                nodes, min_conns, etag_hasher)
            final_phase = True
            need_quorum = False
            min_resp = 2
            putters = [p for p in putters if not p.failed]
            # ignore response etags, and quorum boolean
            statuses, reasons, bodies, _etags, _quorum = \
                self._get_put_responses(req, putters, len(nodes),
                                        final_phase, min_resp,
                                        need_quorum=need_quorum)
        except HTTPException as resp:
            return resp

        etag = etag_hasher.hexdigest()
        resp = self.best_response(req, statuses, reasons, bodies,
                                  _('Object PUT'), etag=etag,
                                  quorum_size=min_conns)
        resp.last_modified = math.ceil(
            float(Timestamp(req.headers['X-Timestamp'])))
        return resp
