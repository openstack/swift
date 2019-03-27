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

from six.moves.urllib.parse import unquote
from six.moves import zip

import collections
import itertools
import json
import mimetypes
import time
import math
import random
from hashlib import md5
from swift import gettext_ as _

from greenlet import GreenletExit
from eventlet import GreenPile
from eventlet.queue import Queue
from eventlet.timeout import Timeout

from swift.common.utils import (
    clean_content_type, config_true_value, ContextPool, csv_append,
    GreenAsyncPile, GreenthreadSafeIterator, Timestamp,
    normalize_delete_at_timestamp, public, get_expirer_container,
    document_iters_to_http_response_body, parse_content_range,
    quorum_size, reiterate, close_if_possible, safe_json_loads)
from swift.common.bufferedhttp import http_connect
from swift.common.constraints import check_metadata, check_object_creation
from swift.common import constraints
from swift.common.exceptions import ChunkReadTimeout, \
    ChunkWriteTimeout, ConnectionTimeout, ResponseTimeout, \
    InsufficientStorage, FooterNotSupported, MultiphasePUTNotSupported, \
    PutterConnectError, ChunkReadError
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.http import (
    is_informational, is_success, is_client_error, is_server_error,
    is_redirection, HTTP_CONTINUE, HTTP_INTERNAL_SERVER_ERROR,
    HTTP_SERVICE_UNAVAILABLE, HTTP_INSUFFICIENT_STORAGE,
    HTTP_PRECONDITION_FAILED, HTTP_CONFLICT, HTTP_UNPROCESSABLE_ENTITY,
    HTTP_REQUESTED_RANGE_NOT_SATISFIABLE)
from swift.common.storage_policy import (POLICIES, REPL_POLICY, EC_POLICY,
                                         ECDriverError, PolicyError)
from swift.proxy.controllers.base import Controller, delay_denial, \
    cors_validation, ResumingGetter, update_headers
from swift.common.swob import HTTPAccepted, HTTPBadRequest, HTTPNotFound, \
    HTTPPreconditionFailed, HTTPRequestEntityTooLarge, HTTPRequestTimeout, \
    HTTPServerError, HTTPServiceUnavailable, HTTPClientDisconnect, \
    HTTPUnprocessableEntity, Response, HTTPException, \
    HTTPRequestedRangeNotSatisfiable, Range, HTTPInternalServerError
from swift.common.request_helpers import update_etag_is_at_header, \
    resolve_etag_is_at_header


def check_content_type(req):
    if not req.environ.get('swift.content_type_overridden') and \
            ';' in req.headers.get('content-type', ''):
        for param in req.headers['content-type'].split(';')[1:]:
            if param.lstrip().startswith('swift_'):
                return HTTPBadRequest("Invalid Content-Type, "
                                      "swift_* is not a valid parameter name.")
    return None


def num_container_updates(container_replicas, container_quorum,
                          object_replicas, object_quorum):
    """
    We need to send container updates via enough object servers such
    that, if the object PUT succeeds, then the container update is
    durable (either it's synchronously updated or written to async
    pendings).

    Define:
      Qc = the quorum size for the container ring
      Qo = the quorum size for the object ring
      Rc = the replica count for the container ring
      Ro = the replica count (or EC N+K) for the object ring

    A durable container update is one that's made it to at least Qc
    nodes. To always be durable, we have to send enough container
    updates so that, if only Qo object PUTs succeed, and all the
    failed object PUTs had container updates, at least Qc updates
    remain. Since (Ro - Qo) object PUTs may fail, we must have at
    least Qc + Ro - Qo container updates to ensure that Qc of them
    remain.

    Also, each container replica is named in at least one object PUT
    request so that, when all requests succeed, no work is generated
    for the container replicator. Thus, at least Rc updates are
    necessary.

    :param container_replicas: replica count for the container ring (Rc)
    :param container_quorum: quorum size for the container ring (Qc)
    :param object_replicas: replica count for the object ring (Ro)
    :param object_quorum: quorum size for the object ring (Qo)

    """
    return max(
        # Qc + Ro - Qo
        container_quorum + object_replicas - object_quorum,
        # Rc
        container_replicas)


class ObjectControllerRouter(object):

    policy_type_to_controller_map = {}

    @classmethod
    def register(cls, policy_type):
        """
        Decorator for Storage Policy implementations to register
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
            self.policy_to_controller_cls[int(policy)] = \
                self.policy_type_to_controller_map[policy.policy_type]

    def __getitem__(self, policy):
        return self.policy_to_controller_cls[int(policy)]


class BaseObjectController(Controller):
    """Base WSGI controller for object requests."""
    server_type = 'Object'

    def __init__(self, app, account_name, container_name, object_name,
                 **kwargs):
        super(BaseObjectController, self).__init__(app)
        self.account_name = unquote(account_name)
        self.container_name = unquote(container_name)
        self.object_name = unquote(object_name)

    def iter_nodes_local_first(self, ring, partition, policy=None,
                               local_handoffs_first=False):
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
        :param policy: optional, an instance of
            :class:`~swift.common.storage_policy.BaseStoragePolicy`
        :param local_handoffs_first: optional, if True prefer primaries and
            local handoff nodes first before looking elsewhere.
        """
        policy_options = self.app.get_policy_options(policy)
        is_local = policy_options.write_affinity_is_local_fn
        if is_local is None:
            return self.app.iter_nodes(ring, partition, policy=policy)

        primary_nodes = ring.get_part_nodes(partition)
        handoff_nodes = ring.get_more_nodes(partition)
        all_nodes = itertools.chain(primary_nodes, handoff_nodes)

        if local_handoffs_first:
            num_locals = policy_options.write_affinity_handoff_delete_count
            if num_locals is None:
                local_primaries = [node for node in primary_nodes
                                   if is_local(node)]
                num_locals = len(primary_nodes) - len(local_primaries)

            first_local_handoffs = list(itertools.islice(
                (node for node in handoff_nodes if is_local(node)), num_locals)
            )
            preferred_nodes = primary_nodes + first_local_handoffs
        else:
            num_locals = policy_options.write_affinity_node_count_fn(
                len(primary_nodes)
            )
            preferred_nodes = list(itertools.islice(
                (node for node in all_nodes if is_local(node)), num_locals)
            )
            # refresh it; it moved when we computed preferred_nodes
            handoff_nodes = ring.get_more_nodes(partition)
            all_nodes = itertools.chain(primary_nodes, handoff_nodes)

        node_iter = itertools.chain(
            preferred_nodes,
            (node for node in all_nodes if node not in preferred_nodes)
        )

        return self.app.iter_nodes(ring, partition, node_iter=node_iter,
                                   policy=policy)

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
        node_iter = self.app.iter_nodes(obj_ring, partition, policy=policy)

        resp = self._get_or_head_response(req, node_iter, partition, policy)

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

    def _get_update_target(self, req, container_info):
        # find the sharded container to which we'll send the update
        db_state = container_info.get('sharding_state', 'unsharded')
        if db_state in ('sharded', 'sharding'):
            shard_ranges = self._get_shard_ranges(
                req, self.account_name, self.container_name,
                includes=self.object_name, states='updating')
            if shard_ranges:
                partition, nodes = self.app.container_ring.get_nodes(
                    shard_ranges[0].account, shard_ranges[0].container)
                return partition, nodes, shard_ranges[0].name

        return container_info['partition'], container_info['nodes'], None

    @public
    @cors_validation
    @delay_denial
    def POST(self, req):
        """HTTP POST request handler."""
        container_info = self.container_info(
            self.account_name, self.container_name, req)
        container_partition, container_nodes, container_path = \
            self._get_update_target(req, container_info)
        req.acl = container_info['write_acl']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not container_nodes:
            return HTTPNotFound(request=req)
        error_response = check_metadata(req, 'object')
        if error_response:
            return error_response

        req.headers['X-Timestamp'] = Timestamp.now().internal

        req, delete_at_container, delete_at_part, \
            delete_at_nodes = self._config_obj_expiration(req)

        # pass the policy index to storage nodes via req header
        policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                       container_info['storage_policy'])
        obj_ring = self.app.get_object_ring(policy_index)
        req.headers['X-Backend-Storage-Policy-Index'] = policy_index
        next_part_power = getattr(obj_ring, 'next_part_power', None)
        if next_part_power:
            req.headers['X-Backend-Next-Part-Power'] = next_part_power
        partition, nodes = obj_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)

        headers = self._backend_requests(
            req, len(nodes), container_partition, container_nodes,
            delete_at_container, delete_at_part, delete_at_nodes,
            container_path=container_path)
        return self._post_object(req, obj_ring, partition, headers)

    def _backend_requests(self, req, n_outgoing,
                          container_partition, containers,
                          delete_at_container=None, delete_at_partition=None,
                          delete_at_nodes=None, container_path=None):
        policy_index = req.headers['X-Backend-Storage-Policy-Index']
        policy = POLICIES.get_by_index(policy_index)
        headers = [self.generate_request_headers(req, additional=req.headers)
                   for _junk in range(n_outgoing)]

        def set_container_update(index, container):
            headers[index]['X-Container-Partition'] = container_partition
            headers[index]['X-Container-Host'] = csv_append(
                headers[index].get('X-Container-Host'),
                '%(ip)s:%(port)s' % container)
            headers[index]['X-Container-Device'] = csv_append(
                headers[index].get('X-Container-Device'),
                container['device'])
            if container_path:
                headers[index]['X-Backend-Container-Path'] = container_path

        def set_delete_at_headers(index, delete_at_node):
            headers[index]['X-Delete-At-Container'] = delete_at_container
            headers[index]['X-Delete-At-Partition'] = delete_at_partition
            headers[index]['X-Delete-At-Host'] = csv_append(
                headers[index].get('X-Delete-At-Host'),
                '%(ip)s:%(port)s' % delete_at_node)
            headers[index]['X-Delete-At-Device'] = csv_append(
                headers[index].get('X-Delete-At-Device'),
                delete_at_node['device'])

        n_updates_needed = num_container_updates(
            len(containers), quorum_size(len(containers)),
            n_outgoing, policy.quorum)

        container_iter = itertools.cycle(containers)
        dan_iter = itertools.cycle(delete_at_nodes or [])
        existing_updates = 0
        while existing_updates < n_updates_needed:
            index = existing_updates % n_outgoing
            set_container_update(index, next(container_iter))
            if delete_at_nodes:
                # We reverse the index in order to distribute the updates
                # across all nodes.
                set_delete_at_headers(n_outgoing - 1 - index, next(dan_iter))
            existing_updates += 1

        # Keep the number of expirer-queue deletes to a reasonable number.
        #
        # In the best case, at least one object server writes out an
        # async_pending for an expirer-queue update. In the worst case, no
        # object server does so, and an expirer-queue row remains that
        # refers to an already-deleted object. In this case, upon attempting
        # to delete the object, the object expirer will notice that the
        # object does not exist and then remove the row from the expirer
        # queue.
        #
        # In other words: expirer-queue updates on object DELETE are nice to
        # have, but not strictly necessary for correct operation.
        #
        # Also, each queue update results in an async_pending record, which
        # causes the object updater to talk to all container servers. If we
        # have N async_pendings and Rc container replicas, we cause N * Rc
        # requests from object updaters to container servers (possibly more,
        # depending on retries). Thus, it is helpful to keep this number
        # small.
        n_desired_queue_updates = 2
        for i in range(len(headers)):
            headers[i].setdefault('X-Backend-Clean-Expiring-Object-Queue',
                                  't' if i < n_desired_queue_updates else 'f')

        return headers

    def _get_conn_response(self, putter, path, logger_thread_locals,
                           final_phase, **kwargs):
        self.app.logger.thread_locals = logger_thread_locals
        try:
            resp = putter.await_response(
                self.app.node_timeout, not final_phase)
        except (Exception, Timeout):
            resp = None
            if final_phase:
                status_type = 'final'
            else:
                status_type = 'commit'
            self.app.exception_occurred(
                putter.node, _('Object'),
                _('Trying to get %(status_type)s status of PUT to %(path)s') %
                {'status_type': status_type, 'path': path})
        return (putter, resp)

    def _have_adequate_put_responses(self, statuses, num_nodes, min_responses):
        """
        Test for sufficient PUT responses from backend nodes to proceed with
        PUT handling.

        :param statuses: a list of response statuses.
        :param num_nodes: number of backend nodes to which PUT requests may be
                          issued.
        :param min_responses: (optional) minimum number of nodes required to
                              have responded with satisfactory status code.
        :return: True if sufficient backend responses have returned a
                 satisfactory status code.
        """
        raise NotImplementedError

    def _get_put_responses(self, req, putters, num_nodes, final_phase=True,
                           min_responses=None):
        """
        Collect object responses to a PUT request and determine if a
        satisfactory number of nodes have returned success.  Returns
        lists of accumulated status codes, reasons, bodies and etags.

        :param req: the request
        :param putters: list of putters for the request
        :param num_nodes: number of nodes involved
        :param final_phase: boolean indicating if this is the last phase
        :param min_responses: minimum needed when not requiring quorum
        :return: a tuple of lists of status codes, reasons, bodies and etags.
                 The list of bodies and etags is only populated for the final
                 phase of a PUT transaction.
        """
        statuses = []
        reasons = []
        bodies = []
        etags = set()

        pile = GreenAsyncPile(len(putters))
        for putter in putters:
            if putter.failed:
                continue
            pile.spawn(self._get_conn_response, putter, req.path,
                       self.app.logger.thread_locals, final_phase=final_phase)

        def _handle_response(putter, response):
            statuses.append(response.status)
            reasons.append(response.reason)
            if final_phase:
                body = response.read()
            else:
                body = b''
            bodies.append(body)
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

        for (putter, response) in pile:
            if response:
                _handle_response(putter, response)
                if self._have_adequate_put_responses(
                        statuses, num_nodes, min_responses):
                    break
            else:
                putter.failed = True

        # give any pending requests *some* chance to finish
        finished_quickly = pile.waitall(self.app.post_quorum_timeout)
        for (putter, response) in finished_quickly:
            if response:
                _handle_response(putter, response)

        if final_phase:
            while len(statuses) < num_nodes:
                statuses.append(HTTP_SERVICE_UNAVAILABLE)
                reasons.append('')
                bodies.append(b'')

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

    def _update_content_type(self, req):
        # Sometimes the 'content-type' header exists, but is set to None.
        detect_content_type = \
            config_true_value(req.headers.get('x-detect-content-type'))
        if detect_content_type or not req.headers.get('content-type'):
            guessed_type, _junk = mimetypes.guess_type(req.path_info)
            req.headers['Content-Type'] = guessed_type or \
                'application/octet-stream'
            if detect_content_type:
                req.headers.pop('x-detect-content-type')

    def _update_x_timestamp(self, req):
        # The container sync feature includes an x-timestamp header with
        # requests. If present this is checked and preserved, otherwise a fresh
        # timestamp is added.
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
            req.headers['X-Timestamp'] = Timestamp.now().internal
        return None

    def _check_failure_put_connections(self, putters, req, min_conns):
        """
        Identify any failed connections and check minimum connection count.

        :param putters: a list of Putter instances
        :param req: request
        :param min_conns: minimum number of putter connections required
        """
        if req.if_none_match is not None and '*' in req.if_none_match:
            statuses = [
                putter.resp.status for putter in putters if putter.resp]
            if HTTP_PRECONDITION_FAILED in statuses:
                # If we find any copy of the file, it shouldn't be uploaded
                self.app.logger.debug(
                    _('Object PUT returning 412, %(statuses)r'),
                    {'statuses': statuses})
                raise HTTPPreconditionFailed(request=req)

        if any(putter for putter in putters if putter.resp and
               putter.resp.status == HTTP_CONFLICT):
            status_times = ['%(status)s (%(timestamp)s)' % {
                'status': putter.resp.status,
                'timestamp': HeaderKeyDict(
                    putter.resp.getheaders()).get(
                        'X-Backend-Timestamp', 'unknown')
            } for putter in putters if putter.resp]
            self.app.logger.debug(
                _('Object PUT returning 202 for 409: '
                  '%(req_timestamp)s <= %(timestamps)r'),
                {'req_timestamp': req.timestamp.internal,
                 'timestamps': ', '.join(status_times)})
            raise HTTPAccepted(request=req)

        self._check_min_conn(req, putters, min_conns)

    def _make_putter(self, node, part, req, headers):
        """
        Returns a putter object for handling streaming of object to object
        servers.

        Subclasses must implement this method.

        :param node: a storage node
        :param part: ring partition number
        :param req: a swob Request
        :param headers: request headers
        :return: an instance of a Putter
        """
        raise NotImplementedError

    def _connect_put_node(self, nodes, part, req, headers,
                          logger_thread_locals):
        """
        Make connection to storage nodes

        Connects to the first working node that it finds in nodes iter and
        sends over the request headers. Returns a Putter to handle the rest of
        the streaming, or None if no working nodes were found.

        :param nodes: an iterator of the target storage nodes
        :param part: ring partition number
        :param req: a swob Request
        :param headers: request headers
        :param logger_thread_locals: The thread local values to be set on the
                                     self.app.logger to retain transaction
                                     logging information.
        :return: an instance of a Putter
        """
        self.app.logger.thread_locals = logger_thread_locals
        for node in nodes:
            try:
                putter = self._make_putter(node, part, req, headers)
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
                    _('Expect: 100-continue on %s') % req.swift_entity_path)

    def _get_put_connections(self, req, nodes, partition, outgoing_headers,
                             policy):
        """
        Establish connections to storage nodes for PUT request
        """
        obj_ring = policy.object_ring
        node_iter = GreenthreadSafeIterator(
            self.iter_nodes_local_first(obj_ring, partition, policy=policy))
        pile = GreenPile(len(nodes))

        for nheaders in outgoing_headers:
            # RFC2616:8.2.3 disallows 100-continue without a body,
            # so switch to chunked request
            if nheaders.get('Content-Length') == '0':
                nheaders['Transfer-Encoding'] = 'chunked'
                del nheaders['Content-Length']
            nheaders['Expect'] = '100-continue'
            pile.spawn(self._connect_put_node, node_iter, partition,
                       req, nheaders, self.app.logger.thread_locals)

        putters = [putter for putter in pile if putter]

        return putters

    def _check_min_conn(self, req, putters, min_conns, msg=None):
        msg = msg or _('Object PUT returning 503, %(conns)s/%(nodes)s '
                       'required connections')

        if len(putters) < min_conns:
            self.app.logger.error((msg),
                                  {'conns': len(putters), 'nodes': min_conns})
            raise HTTPServiceUnavailable(request=req)

    def _get_footers(self, req):
        footers = HeaderKeyDict()
        footer_callback = req.environ.get(
            'swift.callback.update_footers', lambda _footer: None)
        footer_callback(footers)
        return footers

    def _store_object(self, req, data_source, nodes, partition,
                      outgoing_headers):
        """
        This method is responsible for establishing connection
        with storage nodes and sending the data to each one of those
        nodes. The process of transferring data is specific to each
        Storage Policy, thus it is required for each policy specific
        ObjectController to provide their own implementation of this method.

        :param req: the PUT Request
        :param data_source: an iterator of the source of the data
        :param nodes: an iterator of the target storage nodes
        :param partition: ring partition number
        :param outgoing_headers: system headers to storage nodes
        :return: Response object
        """
        raise NotImplementedError()

    def _delete_object(self, req, obj_ring, partition, headers,
                       node_count=None, node_iterator=None):
        """Delete object considering write-affinity.

        When deleting object in write affinity deployment, also take configured
        handoff nodes number into consideration, instead of just sending
        requests to primary nodes. Otherwise (write-affinity is disabled),
        go with the same way as before.

        :param req: the DELETE Request
        :param obj_ring: the object ring
        :param partition: ring partition number
        :param headers: system headers to storage nodes
        :return: Response object
        """
        status_overrides = {404: 204}
        resp = self.make_requests(req, obj_ring,
                                  partition, 'DELETE', req.swift_entity_path,
                                  headers, overrides=status_overrides,
                                  node_count=node_count,
                                  node_iterator=node_iterator)
        return resp

    def _post_object(self, req, obj_ring, partition, headers):
        """
        send object POST request to storage nodes.

        :param req: the POST Request
        :param obj_ring: the object ring
        :param partition: ring partition number
        :param headers: system headers to storage nodes
        :return: Response object
        """
        resp = self.make_requests(req, obj_ring, partition,
                                  'POST', req.swift_entity_path, headers)
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
        container_partition, container_nodes, container_path = \
            self._get_update_target(req, container_info)
        partition, nodes = obj_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)

        # pass the policy index to storage nodes via req header
        req.headers['X-Backend-Storage-Policy-Index'] = policy_index
        next_part_power = getattr(obj_ring, 'next_part_power', None)
        if next_part_power:
            req.headers['X-Backend-Next-Part-Power'] = next_part_power
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']

        # is request authorized
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp

        if not container_nodes:
            return HTTPNotFound(request=req)

        # update content type in case it is missing
        self._update_content_type(req)

        self._update_x_timestamp(req)

        # check constraints on object name and request headers
        error_response = check_object_creation(req, self.object_name) or \
            check_content_type(req)
        if error_response:
            return error_response

        def reader():
            try:
                return req.environ['wsgi.input'].read(
                    self.app.client_chunk_size)
            except (ValueError, IOError) as e:
                raise ChunkReadError(str(e))
        data_source = iter(reader, b'')

        # check if object is set to be automatically deleted (i.e. expired)
        req, delete_at_container, delete_at_part, \
            delete_at_nodes = self._config_obj_expiration(req)

        # add special headers to be handled by storage nodes
        outgoing_headers = self._backend_requests(
            req, len(nodes), container_partition, container_nodes,
            delete_at_container, delete_at_part, delete_at_nodes,
            container_path=container_path)

        # send object to storage nodes
        resp = self._store_object(
            req, data_source, nodes, partition, outgoing_headers)
        return resp

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
        next_part_power = getattr(obj_ring, 'next_part_power', None)
        if next_part_power:
            req.headers['X-Backend-Next-Part-Power'] = next_part_power
        container_partition, container_nodes, container_path = \
            self._get_update_target(req, container_info)
        req.acl = container_info['write_acl']
        req.environ['swift_sync_key'] = container_info['sync_key']
        if 'swift.authorize' in req.environ:
            aresp = req.environ['swift.authorize'](req)
            if aresp:
                return aresp
        if not container_nodes:
            return HTTPNotFound(request=req)
        partition, nodes = obj_ring.get_nodes(
            self.account_name, self.container_name, self.object_name)

        self._update_x_timestamp(req)

        # Include local handoff nodes if write-affinity is enabled.
        node_count = len(nodes)
        node_iterator = None
        policy = POLICIES.get_by_index(policy_index)
        policy_options = self.app.get_policy_options(policy)
        is_local = policy_options.write_affinity_is_local_fn
        if is_local is not None:
            local_handoffs = policy_options.write_affinity_handoff_delete_count
            if local_handoffs is None:
                local_primaries = [node for node in nodes if is_local(node)]
                local_handoffs = len(nodes) - len(local_primaries)
            node_count += local_handoffs
            node_iterator = self.iter_nodes_local_first(
                obj_ring, partition, policy=policy, local_handoffs_first=True
            )

        headers = self._backend_requests(
            req, node_count, container_partition, container_nodes,
            container_path=container_path)
        return self._delete_object(req, obj_ring, partition, headers,
                                   node_count=node_count,
                                   node_iterator=node_iterator)


@ObjectControllerRouter.register(REPL_POLICY)
class ReplicatedObjectController(BaseObjectController):

    def _get_or_head_response(self, req, node_iter, partition, policy):
        concurrency = self.app.get_object_ring(policy.idx).replica_count \
            if self.app.concurrent_gets else 1
        resp = self.GETorHEAD_base(
            req, _('Object'), node_iter, partition,
            req.swift_entity_path, concurrency)
        return resp

    def _make_putter(self, node, part, req, headers):
        if req.environ.get('swift.callback.update_footers'):
            putter = MIMEPutter.connect(
                node, part, req.swift_entity_path, headers,
                conn_timeout=self.app.conn_timeout,
                node_timeout=self.app.node_timeout,
                logger=self.app.logger,
                need_multiphase=False)
        else:
            te = ',' + headers.get('Transfer-Encoding', '')
            putter = Putter.connect(
                node, part, req.swift_entity_path, headers,
                conn_timeout=self.app.conn_timeout,
                node_timeout=self.app.node_timeout,
                logger=self.app.logger,
                chunked=te.endswith(',chunked'))
        return putter

    def _transfer_data(self, req, data_source, putters, nodes):
        """
        Transfer data for a replicated object.

        This method was added in the PUT method extraction change
        """
        bytes_transferred = 0

        def send_chunk(chunk):
            for putter in list(putters):
                if not putter.failed:
                    putter.send_chunk(chunk)
                else:
                    putter.close()
                    putters.remove(putter)
            self._check_min_conn(
                req, putters, min_conns,
                msg=_('Object PUT exceptions during send, '
                      '%(conns)s/%(nodes)s required connections'))

        min_conns = quorum_size(len(nodes))
        try:
            with ContextPool(len(nodes)) as pool:
                for putter in putters:
                    putter.spawn_sender_greenthread(
                        pool, self.app.put_queue_depth, self.app.node_timeout,
                        self.app.exception_occurred)
                while True:
                    with ChunkReadTimeout(self.app.client_timeout):
                        try:
                            chunk = next(data_source)
                        except StopIteration:
                            break
                    bytes_transferred += len(chunk)
                    if bytes_transferred > constraints.MAX_FILE_SIZE:
                        raise HTTPRequestEntityTooLarge(request=req)

                    send_chunk(chunk)

                if req.content_length and (
                        bytes_transferred < req.content_length):
                    req.client_disconnect = True
                    self.app.logger.warning(
                        _('Client disconnected without sending enough data'))
                    self.app.logger.increment('client_disconnects')
                    raise HTTPClientDisconnect(request=req)

                trail_md = self._get_footers(req)
                for putter in putters:
                    # send any footers set by middleware
                    putter.end_of_object_data(footer_metadata=trail_md)

                for putter in putters:
                    putter.wait()
                self._check_min_conn(
                    req, [p for p in putters if not p.failed], min_conns,
                    msg=_('Object PUT exceptions after last send, '
                          '%(conns)s/%(nodes)s required connections'))
        except ChunkReadTimeout as err:
            self.app.logger.warning(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            self.app.logger.increment('client_timeouts')
            raise HTTPRequestTimeout(request=req)
        except HTTPException:
            raise
        except ChunkReadError:
            req.client_disconnect = True
            self.app.logger.warning(
                _('Client disconnected without sending last chunk'))
            self.app.logger.increment('client_disconnects')
            raise HTTPClientDisconnect(request=req)
        except Timeout:
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            raise HTTPClientDisconnect(request=req)
        except Exception:
            self.app.logger.exception(
                _('ERROR Exception transferring data to object servers %s'),
                {'path': req.path})
            raise HTTPInternalServerError(request=req)

    def _have_adequate_put_responses(self, statuses, num_nodes, min_responses):
        return self.have_quorum(statuses, num_nodes)

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

        putters = self._get_put_connections(
            req, nodes, partition, outgoing_headers, policy)
        min_conns = quorum_size(len(nodes))
        try:
            # check that a minimum number of connections were established and
            # meet all the correct conditions set in the request
            self._check_failure_put_connections(putters, req, min_conns)

            # transfer data
            self._transfer_data(req, data_source, putters, nodes)

            # get responses
            statuses, reasons, bodies, etags = \
                self._get_put_responses(req, putters, len(nodes))
        except HTTPException as resp:
            return resp
        finally:
            for putter in putters:
                putter.close()

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


class ECAppIter(object):
    """
    WSGI iterable that decodes EC fragment archives (or portions thereof)
    into the original object (or portions thereof).

    :param path: object's path, sans v1 (e.g. /a/c/o)

    :param policy: storage policy for this object

    :param internal_parts_iters: list of the response-document-parts
        iterators for the backend GET responses. For an M+K erasure code,
        the caller must supply M such iterables.

    :param range_specs: list of dictionaries describing the ranges requested
        by the client. Each dictionary contains the start and end of the
        client's requested byte range as well as the start and end of the EC
        segments containing that byte range.

    :param fa_length: length of the fragment archive, in bytes, if the
        response is a 200. If it's a 206, then this is ignored.

    :param obj_length: length of the object, in bytes. Learned from the
        headers in the GET response from the object server.

    :param logger: a logger
    """
    def __init__(self, path, policy, internal_parts_iters, range_specs,
                 fa_length, obj_length, logger):
        self.path = path
        self.policy = policy
        self.internal_parts_iters = internal_parts_iters
        self.range_specs = range_specs
        self.fa_length = fa_length
        self.obj_length = obj_length if obj_length is not None else 0
        self.boundary = b''
        self.logger = logger

        self.mime_boundary = None
        self.learned_content_type = None
        self.stashed_iter = None

    def close(self):
        # close down the stashed iter first so the ContextPool can
        # cleanup the frag queue feeding coros that may be currently
        # executing the internal_parts_iters.
        if self.stashed_iter:
            self.stashed_iter.close()
        for it in self.internal_parts_iters:
            close_if_possible(it)

    def kickoff(self, req, resp):
        """
        Start pulling data from the backends so that we can learn things like
        the real Content-Type that might only be in the multipart/byteranges
        response body. Update our response accordingly.

        Also, this is the first point at which we can learn the MIME
        boundary that our response has in the headers. We grab that so we
        can also use it in the body.

        :returns: None
        :raises HTTPException: on error
        """
        self.mime_boundary = resp.boundary

        try:
            self.stashed_iter = reiterate(self._real_iter(req, resp.headers))
        except Exception:
            self.close()
            raise

        if self.learned_content_type is not None:
            resp.content_type = self.learned_content_type
        resp.content_length = self.obj_length

    def _next_ranges(self):
        # Each FA part should have approximately the same headers. We really
        # only care about Content-Range and Content-Type, and that'll be the
        # same for all the different FAs.
        for part_infos in zip(*self.internal_parts_iters):
            frag_iters = [pi['part_iter'] for pi in part_infos]
            headers = HeaderKeyDict(part_infos[0]['headers'])
            yield headers, frag_iters

    def _actual_range(self, req_start, req_end, entity_length):
        # Takes 3 args: (requested-first-byte, requested-last-byte,
        # actual-length).
        #
        # Returns a 3-tuple (first-byte, last-byte, satisfiable).
        #
        # It is possible to get (None, None, True). This means that the last
        # N>0 bytes of a 0-byte object were requested, and we are able to
        # satisfy that request by returning nothing.
        try:
            rng = Range("bytes=%s-%s" % (
                req_start if req_start is not None else '',
                req_end if req_end is not None else ''))
        except ValueError:
            return (None, None, False)

        rfl = rng.ranges_for_length(entity_length)
        if rfl and entity_length == 0:
            return (None, None, True)
        elif not rfl:
            return (None, None, False)
        else:
            # ranges_for_length() adds 1 to the last byte's position
            # because webob once made a mistake
            return (rfl[0][0], rfl[0][1] - 1, True)

    def _fill_out_range_specs_from_obj_length(self, range_specs):
        # Add a few fields to each range spec:
        #
        #  * resp_client_start, resp_client_end: the actual bytes that will
        #      be delivered to the client for the requested range. This may
        #      differ from the requested bytes if, say, the requested range
        #      overlaps the end of the object.
        #
        #  * resp_segment_start, resp_segment_end: the actual offsets of the
        #      segments that will be decoded for the requested range. These
        #      differ from resp_client_start/end in that these are aligned
        #      to segment boundaries, while resp_client_start/end are not
        #      necessarily so.
        #
        #  * satisfiable: a boolean indicating whether the range is
        #      satisfiable or not (i.e. the requested range overlaps the
        #      object in at least one byte).
        #
        # This is kept separate from _fill_out_range_specs_from_fa_length()
        # because this computation can be done with just the response
        # headers from the object servers (in particular
        # X-Object-Sysmeta-Ec-Content-Length), while the computation in
        # _fill_out_range_specs_from_fa_length() requires the beginnings of
        # the response bodies.
        for spec in range_specs:
            cstart, cend, csat = self._actual_range(
                spec['req_client_start'],
                spec['req_client_end'],
                self.obj_length)
            spec['resp_client_start'] = cstart
            spec['resp_client_end'] = cend
            spec['satisfiable'] = csat

            sstart, send, _junk = self._actual_range(
                spec['req_segment_start'],
                spec['req_segment_end'],
                self.obj_length)

            seg_size = self.policy.ec_segment_size
            if (spec['req_segment_start'] is None and
                    sstart is not None and
                    sstart % seg_size != 0):
                # Segment start may, in the case of a suffix request, need
                # to be rounded up (not down!) to the nearest segment boundary.
                # This reflects the trimming of leading garbage (partial
                # fragments) from the retrieved fragments.
                sstart += seg_size - (sstart % seg_size)

            spec['resp_segment_start'] = sstart
            spec['resp_segment_end'] = send

    def _fill_out_range_specs_from_fa_length(self, fa_length, range_specs):
        # Add two fields to each range spec:
        #
        #  * resp_fragment_start, resp_fragment_end: the start and end of
        #      the fragments that compose this byterange. These values are
        #      aligned to fragment boundaries.
        #
        # This way, ECAppIter has the knowledge it needs to correlate
        # response byteranges with requested ones for when some byteranges
        # are omitted from the response entirely and also to put the right
        # Content-Range headers in a multipart/byteranges response.
        for spec in range_specs:
            fstart, fend, _junk = self._actual_range(
                spec['req_fragment_start'],
                spec['req_fragment_end'],
                fa_length)
            spec['resp_fragment_start'] = fstart
            spec['resp_fragment_end'] = fend

    def __iter__(self):
        if self.stashed_iter is not None:
            return iter(self.stashed_iter)
        else:
            raise ValueError("Failed to call kickoff() before __iter__()")

    def _real_iter(self, req, resp_headers):
        if not self.range_specs:
            client_asked_for_range = False
            range_specs = [{
                'req_client_start': 0,
                'req_client_end': (None if self.obj_length is None
                                   else self.obj_length - 1),
                'resp_client_start': 0,
                'resp_client_end': (None if self.obj_length is None
                                    else self.obj_length - 1),
                'req_segment_start': 0,
                'req_segment_end': (None if self.obj_length is None
                                    else self.obj_length - 1),
                'resp_segment_start': 0,
                'resp_segment_end': (None if self.obj_length is None
                                     else self.obj_length - 1),
                'req_fragment_start': 0,
                'req_fragment_end': self.fa_length - 1,
                'resp_fragment_start': 0,
                'resp_fragment_end': self.fa_length - 1,
                'satisfiable': self.obj_length > 0,
            }]
        else:
            client_asked_for_range = True
            range_specs = self.range_specs

        self._fill_out_range_specs_from_obj_length(range_specs)

        multipart = (len([rs for rs in range_specs if rs['satisfiable']]) > 1)
        # Multipart responses are not required to be in the same order as
        # the Range header; the parts may be in any order the server wants.
        # Further, if multiple ranges are requested and only some are
        # satisfiable, then only the satisfiable ones appear in the response
        # at all. Thus, we cannot simply iterate over range_specs in order;
        # we must use the Content-Range header from each part to figure out
        # what we've been given.
        #
        # We do, however, make the assumption that all the object-server
        # responses have their ranges in the same order. Otherwise, a
        # streaming decode would be impossible.

        def convert_ranges_iter():
            seen_first_headers = False
            ranges_for_resp = {}

            for headers, frag_iters in self._next_ranges():
                content_type = headers['Content-Type']

                content_range = headers.get('Content-Range')
                if content_range is not None:
                    fa_start, fa_end, fa_length = parse_content_range(
                        content_range)
                elif self.fa_length <= 0:
                    fa_start = None
                    fa_end = None
                    fa_length = 0
                else:
                    fa_start = 0
                    fa_end = self.fa_length - 1
                    fa_length = self.fa_length

                if not seen_first_headers:
                    # This is the earliest we can possibly do this. On a
                    # 200 or 206-single-byterange response, we can learn
                    # the FA's length from the HTTP response headers.
                    # However, on a 206-multiple-byteranges response, we
                    # don't learn it until the first part of the
                    # response body, in the headers of the first MIME
                    # part.
                    #
                    # Similarly, the content type of a
                    # 206-multiple-byteranges response is
                    # "multipart/byteranges", not the object's actual
                    # content type.
                    self._fill_out_range_specs_from_fa_length(
                        fa_length, range_specs)

                    satisfiable = False
                    for range_spec in range_specs:
                        satisfiable |= range_spec['satisfiable']
                        key = (range_spec['resp_fragment_start'],
                               range_spec['resp_fragment_end'])
                        ranges_for_resp.setdefault(key, []).append(range_spec)

                    # The client may have asked for an unsatisfiable set of
                    # ranges, but when converted to fragments, the object
                    # servers see it as satisfiable. For example, imagine a
                    # request for bytes 800-900 of a 750-byte object with a
                    # 1024-byte segment size. The object servers will see a
                    # request for bytes 0-${fragsize-1}, and that's
                    # satisfiable, so they return 206. It's not until we
                    # learn the object size that we can check for this
                    # condition.
                    #
                    # Note that some unsatisfiable ranges *will* be caught
                    # by the object servers, like bytes 1800-1900 of a
                    # 100-byte object with 1024-byte segments. That's not
                    # what we're dealing with here, though.
                    if client_asked_for_range and not satisfiable:
                        req.environ[
                            'swift.non_client_disconnect'] = True
                        raise HTTPRequestedRangeNotSatisfiable(
                            request=req, headers=resp_headers)
                    self.learned_content_type = content_type
                    seen_first_headers = True

                range_spec = ranges_for_resp[(fa_start, fa_end)].pop(0)
                seg_iter = self._decode_segments_from_fragments(frag_iters)
                if not range_spec['satisfiable']:
                    # This'll be small; just a single small segment. Discard
                    # it.
                    for x in seg_iter:
                        pass
                    continue

                byterange_iter = self._iter_one_range(range_spec, seg_iter)

                converted = {
                    "start_byte": range_spec["resp_client_start"],
                    "end_byte": range_spec["resp_client_end"],
                    "content_type": content_type,
                    "part_iter": byterange_iter}

                if self.obj_length is not None:
                    converted["entity_length"] = self.obj_length
                yield converted

        return document_iters_to_http_response_body(
            convert_ranges_iter(), self.mime_boundary, multipart, self.logger)

    def _iter_one_range(self, range_spec, segment_iter):
        client_start = range_spec['resp_client_start']
        client_end = range_spec['resp_client_end']
        segment_start = range_spec['resp_segment_start']
        segment_end = range_spec['resp_segment_end']

        # It's entirely possible that the client asked for a range that
        # includes some bytes we have and some we don't; for example, a
        # range of bytes 1000-20000000 on a 1500-byte object.
        segment_end = (min(segment_end, self.obj_length - 1)
                       if segment_end is not None
                       else self.obj_length - 1)
        client_end = (min(client_end, self.obj_length - 1)
                      if client_end is not None
                      else self.obj_length - 1)
        if segment_start is None:
            num_segments = 0
            start_overrun = 0
            end_overrun = 0
        else:
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

        for i, next_seg in enumerate(segment_iter):
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

    def _decode_segments_from_fragments(self, fragment_iters):
        # Decodes the fragments from the object servers and yields one
        # segment at a time.
        queues = [Queue(1) for _junk in range(len(fragment_iters))]

        def put_fragments_in_queue(frag_iter, queue):
            try:
                for fragment in frag_iter:
                    if fragment.startswith(b' '):
                        raise Exception('Leading whitespace on fragment.')
                    queue.put(fragment)
            except GreenletExit:
                # killed by contextpool
                pass
            except ChunkReadTimeout:
                # unable to resume in GetOrHeadHandler
                self.logger.exception(_("Timeout fetching fragments for %r"),
                                      self.path)
            except:  # noqa
                self.logger.exception(_("Exception fetching fragments for"
                                        " %r"), self.path)
            finally:
                queue.resize(2)  # ensure there's room
                queue.put(None)
                frag_iter.close()

        with ContextPool(len(fragment_iters)) as pool:
            for frag_iter, queue in zip(fragment_iters, queues):
                pool.spawn(put_fragments_in_queue, frag_iter, queue)

            while True:
                fragments = []
                for queue in queues:
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
                    self.logger.exception(_("Error decoding fragments for"
                                            " %r"), self.path)
                    raise

                yield segment

    def app_iter_range(self, start, end):
        return self

    def app_iter_ranges(self, ranges, content_type, boundary, content_size):
        return self


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
        segment_start // segment_size * fragment_size)
        if segment_start is not None else None)
    # the index of the last byte of the last fragment
    fragment_end = (
        # range unbounded on the right
        None if segment_end is None else
        # range unbounded on the left; no -1 since we're
        # asking for the last N bytes, not to have a
        # particular byte be the last one
        ((segment_end + 1) // segment_size
         * fragment_size) if segment_start is None else
        # range bounded on both sides; the -1 is because the
        # rest of the expression computes the length of the
        # fragment, and a range of N bytes starts at index M
        # and ends at M + N - 1.
        ((segment_end + 1) // segment_size * fragment_size) - 1)
    return (fragment_start, fragment_end)


NO_DATA_SENT = 1
SENDING_DATA = 2
DATA_SENT = 3
DATA_ACKED = 4
COMMIT_SENT = 5


class Putter(object):
    """
    Putter for backend PUT requests.

    Encapsulates all the actions required to establish a connection with a
    storage node and stream data to that node.

    :param conn: an HTTPConnection instance
    :param node: dict describing storage node
    :param resp: an HTTPResponse instance if connect() received final response
    :param path: the object path to send to the storage node
    :param connect_duration: time taken to initiate the HTTPConnection
    :param logger: a Logger instance
    :param chunked: boolean indicating if the request encoding is chunked
    """
    def __init__(self, conn, node, resp, path, connect_duration, logger,
                 chunked=False):
        # Note: you probably want to call Putter.connect() instead of
        # instantiating one of these directly.
        self.conn = conn
        self.node = node
        self.resp = self.final_resp = resp
        self.path = path
        self.connect_duration = connect_duration
        # for handoff nodes node_index is None
        self.node_index = node.get('index')

        self.failed = False
        self.queue = None
        self.state = NO_DATA_SENT
        self.chunked = chunked
        self.logger = logger

    def await_response(self, timeout, informational=False):
        """
        Get 100-continue response indicating the end of 1st phase of a 2-phase
        commit or the final response, i.e. the one with status >= 200.

        Might or might not actually wait for anything. If we said Expect:
        100-continue but got back a non-100 response, that'll be the thing
        returned, and we won't do any network IO to get it. OTOH, if we got
        a 100 Continue response and sent up the PUT request's body, then
        we'll actually read the 2xx-5xx response off the network here.

        :param timeout: time to wait for a response
        :param informational: if True then try to get a 100-continue response,
                              otherwise try to get a final response.
        :returns: HTTPResponse
        :raises Timeout: if the response took too long
        """
        # don't do this update of self.resp if the Expect response during
        # connect() was actually a final response
        if not self.final_resp:
            with Timeout(timeout):
                if informational:
                    self.resp = self.conn.getexpect()
                else:
                    self.resp = self.conn.getresponse()
        return self.resp

    def spawn_sender_greenthread(self, pool, queue_depth, write_timeout,
                                 exception_handler):
        """Call before sending the first chunk of request body"""
        self.queue = Queue(queue_depth)
        pool.spawn(self._send_file, write_timeout, exception_handler)

    def wait(self):
        if self.queue.unfinished_tasks:
            self.queue.join()

    def _start_object_data(self):
        # Called immediately before the first chunk of object data is sent.
        # Subclasses may implement custom behaviour
        pass

    def send_chunk(self, chunk):
        if not chunk:
            # If we're not using chunked transfer-encoding, sending a 0-byte
            # chunk is just wasteful. If we *are* using chunked
            # transfer-encoding, sending a 0-byte chunk terminates the
            # request body. Neither one of these is good.
            return
        elif self.state == DATA_SENT:
            raise ValueError("called send_chunk after end_of_object_data")

        if self.state == NO_DATA_SENT:
            self._start_object_data()
            self.state = SENDING_DATA

        self.queue.put(chunk)

    def end_of_object_data(self, **kwargs):
        """
        Call when there is no more data to send.
        """
        if self.state == DATA_SENT:
            raise ValueError("called end_of_object_data twice")

        self.queue.put(b'')
        self.state = DATA_SENT

    def _send_file(self, write_timeout, exception_handler):
        """
        Method for a file PUT coroutine. Takes chunks from a queue and sends
        them down a socket.

        If something goes wrong, the "failed" attribute will be set to true
        and the exception handler will be called.
        """
        while True:
            chunk = self.queue.get()
            if not self.failed:
                if self.chunked:
                    to_send = b"%x\r\n%s\r\n" % (len(chunk), chunk)
                else:
                    to_send = chunk
                try:
                    with ChunkWriteTimeout(write_timeout):
                        self.conn.send(to_send)
                except (Exception, ChunkWriteTimeout):
                    self.failed = True
                    exception_handler(self.node, _('Object'),
                                      _('Trying to write to %s') % self.path)

            self.queue.task_done()

    def close(self):
        # release reference to response to ensure connection really does close,
        # see bug https://bugs.launchpad.net/swift/+bug/1594739
        self.resp = self.final_resp = None
        self.conn.close()

    @classmethod
    def _make_connection(cls, node, part, path, headers, conn_timeout,
                         node_timeout):
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

        final_resp = None
        if (is_success(resp.status) or
                resp.status in (HTTP_CONFLICT, HTTP_UNPROCESSABLE_ENTITY) or
                (headers.get('If-None-Match', None) is not None and
                 resp.status == HTTP_PRECONDITION_FAILED)):
            final_resp = resp

        return conn, resp, final_resp, connect_duration

    @classmethod
    def connect(cls, node, part, path, headers, conn_timeout, node_timeout,
                logger=None, chunked=False, **kwargs):
        """
        Connect to a backend node and send the headers.

        :returns: Putter instance

        :raises ConnectionTimeout: if initial connection timed out
        :raises ResponseTimeout: if header retrieval timed out
        :raises InsufficientStorage: on 507 response from node
        :raises PutterConnectError: on non-507 server error response from node
        """
        conn, expect_resp, final_resp, connect_duration = cls._make_connection(
            node, part, path, headers, conn_timeout, node_timeout)
        return cls(conn, node, final_resp, path, connect_duration, logger,
                   chunked=chunked)


class MIMEPutter(Putter):
    """
    Putter for backend PUT requests that use MIME.

    This is here mostly to wrap up the fact that all multipart PUTs are
    chunked because of the mime boundary footer trick and the first
    half of the two-phase PUT conversation handling.

    An HTTP PUT request that supports streaming.
    """
    def __init__(self, conn, node, resp, req, connect_duration,
                 logger, mime_boundary, multiphase=False):
        super(MIMEPutter, self).__init__(conn, node, resp, req,
                                         connect_duration, logger)
        # Note: you probably want to call MimePutter.connect() instead of
        # instantiating one of these directly.
        self.chunked = True  # MIME requests always send chunked body
        self.mime_boundary = mime_boundary
        self.multiphase = multiphase

    def _start_object_data(self):
        # We're sending the object plus other stuff in the same request
        # body, all wrapped up in multipart MIME, so we'd better start
        # off the MIME document before sending any object data.
        self.queue.put(b"--%s\r\nX-Document: object body\r\n\r\n" %
                       (self.mime_boundary,))

    def end_of_object_data(self, footer_metadata=None):
        """
        Call when there is no more data to send.

        Overrides superclass implementation to send any footer metadata
        after object data.

        :param footer_metadata: dictionary of metadata items
                                to be sent as footers.
        """
        if self.state == DATA_SENT:
            raise ValueError("called end_of_object_data twice")
        elif self.state == NO_DATA_SENT and self.mime_boundary:
            self._start_object_data()

        footer_body = json.dumps(footer_metadata).encode('ascii')
        footer_md5 = md5(footer_body).hexdigest().encode('ascii')

        tail_boundary = (b"--%s" % (self.mime_boundary,))
        if not self.multiphase:
            # this will be the last part sent
            tail_boundary = tail_boundary + b"--"

        message_parts = [
            (b"\r\n--%s\r\n" % self.mime_boundary),
            b"X-Document: object metadata\r\n",
            b"Content-MD5: %s\r\n" % footer_md5,
            b"\r\n",
            footer_body, b"\r\n",
            tail_boundary, b"\r\n",
        ]
        self.queue.put(b"".join(message_parts))

        self.queue.put(b'')
        self.state = DATA_SENT

    def send_commit_confirmation(self):
        """
        Call when there are > quorum 2XX responses received.  Send commit
        confirmations to all object nodes to finalize the PUT.
        """
        if not self.multiphase:
            raise ValueError(
                "called send_commit_confirmation but multiphase is False")
        if self.state == COMMIT_SENT:
            raise ValueError("called send_commit_confirmation twice")

        self.state = DATA_ACKED

        if self.mime_boundary:
            body = b"put_commit_confirmation"
            tail_boundary = (b"--%s--" % (self.mime_boundary,))
            message_parts = [
                b"X-Document: put commit\r\n",
                b"\r\n",
                body, b"\r\n",
                tail_boundary,
            ]
            self.queue.put(b"".join(message_parts))

        self.queue.put(b'')
        self.state = COMMIT_SENT

    @classmethod
    def connect(cls, node, part, req, headers, conn_timeout, node_timeout,
                logger=None, need_multiphase=True, **kwargs):
        """
        Connect to a backend node and send the headers.

        Override superclass method to notify object of need for support for
        multipart body with footers and optionally multiphase commit, and
        verify object server's capabilities.

        :param need_multiphase: if True then multiphase support is required of
                                the object server
        :raises FooterNotSupported: if need_metadata_footer is set but
                 backend node can't process footers
        :raises MultiphasePUTNotSupported: if need_multiphase is set but
                 backend node can't handle multiphase PUT
        """
        mime_boundary = b"%.64x" % random.randint(0, 16 ** 64)
        headers = HeaderKeyDict(headers)
        # when using a multipart mime request to backend the actual
        # content-length is not equal to the object content size, so move the
        # object content size to X-Backend-Obj-Content-Length if that has not
        # already been set by the EC PUT path.
        headers.setdefault('X-Backend-Obj-Content-Length',
                           headers.pop('Content-Length', None))
        # We're going to be adding some unknown amount of data to the
        # request, so we can't use an explicit content length, and thus
        # we must use chunked encoding.
        headers['Transfer-Encoding'] = 'chunked'
        headers['Expect'] = '100-continue'

        headers['X-Backend-Obj-Multipart-Mime-Boundary'] = mime_boundary

        headers['X-Backend-Obj-Metadata-Footer'] = 'yes'

        if need_multiphase:
            headers['X-Backend-Obj-Multiphase-Commit'] = 'yes'

        conn, expect_resp, final_resp, connect_duration = cls._make_connection(
            node, part, req, headers, conn_timeout, node_timeout)

        if is_informational(expect_resp.status):
            continue_headers = HeaderKeyDict(expect_resp.getheaders())
            can_send_metadata_footer = config_true_value(
                continue_headers.get('X-Obj-Metadata-Footer', 'no'))
            can_handle_multiphase_put = config_true_value(
                continue_headers.get('X-Obj-Multiphase-Commit', 'no'))

            if not can_send_metadata_footer:
                raise FooterNotSupported()

            if need_multiphase and not can_handle_multiphase_put:
                raise MultiphasePUTNotSupported()

        return cls(conn, node, final_resp, req, connect_duration, logger,
                   mime_boundary, multiphase=need_multiphase)


def chunk_transformer(policy):
    """
    A generator to transform a source chunk to erasure coded chunks for each
    `send` call. The number of erasure coded chunks is as
    policy.ec_n_unique_fragments.
    """
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
                chunks_to_encode.append(b''.join(pieces))

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
            obj_data = [b''.join(frags)
                        for frags in zip(*frags_by_byte_order)]
            chunk = yield obj_data
        else:
            # didn't have enough data to encode
            chunk = yield None

    # Now we've gotten an empty chunk, which indicates end-of-input.
    # Take any leftover bytes and encode them.
    last_bytes = b''.join(buf)
    if last_bytes:
        last_frags = policy.pyeclib_driver.encode(last_bytes)
        yield last_frags
    else:
        yield [b''] * policy.ec_n_unique_fragments


def trailing_metadata(policy, client_obj_hasher,
                      bytes_transferred_from_client,
                      fragment_archive_index):
    return HeaderKeyDict({
        # etag and size values are being added twice here.
        # The container override header is used to update the container db
        # with these values as they represent the correct etag and size for
        # the whole object and not just the FA.
        # The object sysmeta headers will be saved on each FA of the object.
        'X-Object-Sysmeta-EC-Etag': client_obj_hasher.hexdigest(),
        'X-Object-Sysmeta-EC-Content-Length':
        str(bytes_transferred_from_client),
        # older style x-backend-container-update-override-* headers are used
        # here (rather than x-object-sysmeta-container-update-override-*
        # headers) for backwards compatibility: the request may be to an object
        # server that has not yet been upgraded to accept the newer style
        # x-object-sysmeta-container-update-override- headers.
        'X-Backend-Container-Update-Override-Etag':
        client_obj_hasher.hexdigest(),
        'X-Backend-Container-Update-Override-Size':
        str(bytes_transferred_from_client),
        'X-Object-Sysmeta-Ec-Frag-Index': str(fragment_archive_index),
        # These fields are for debuggability,
        # AKA "what is this thing?"
        'X-Object-Sysmeta-EC-Scheme': policy.ec_scheme_description,
        'X-Object-Sysmeta-EC-Segment-Size': str(policy.ec_segment_size),
    })


class ECGetResponseBucket(object):
    """
    A helper class to encapsulate the properties of buckets in which fragment
    getters and alternate nodes are collected.
    """
    def __init__(self, policy, timestamp_str):
        """
        :param policy: an instance of ECStoragePolicy
        :param timestamp_str: a string representation of a timestamp
        """
        self.policy = policy
        self.timestamp_str = timestamp_str
        self.gets = collections.defaultdict(list)
        self.alt_nodes = collections.defaultdict(list)
        self._durable = False
        self.status = self.headers = None

    def set_durable(self):
        self._durable = True

    def add_response(self, getter, parts_iter):
        if not self.gets:
            self.status = getter.last_status
            # stash first set of backend headers, which will be used to
            # populate a client response
            # TODO: each bucket is for a single *data* timestamp, but sources
            # in the same bucket may have different *metadata* timestamps if
            # some backends have more recent .meta files than others. Currently
            # we just use the last received metadata headers - this behavior is
            # ok and is consistent with a replication policy GET which
            # similarly does not attempt to find the backend with the most
            # recent metadata. We could alternatively choose to the *newest*
            # metadata headers for self.headers by selecting the source with
            # the latest X-Timestamp.
            self.headers = getter.last_headers
        elif (getter.last_headers.get('X-Object-Sysmeta-Ec-Etag') !=
              self.headers.get('X-Object-Sysmeta-Ec-Etag')):
            # Fragments at the same timestamp with different etags are never
            # expected. If somehow it happens then ignore those fragments
            # to avoid mixing fragments that will not reconstruct otherwise
            # an exception from pyeclib is almost certain. This strategy leaves
            # a possibility that a set of consistent frags will be gathered.
            raise ValueError("ETag mismatch")

        frag_index = getter.last_headers.get('X-Object-Sysmeta-Ec-Frag-Index')
        frag_index = int(frag_index) if frag_index is not None else None
        self.gets[frag_index].append((getter, parts_iter))

    def get_responses(self):
        """
        Return a list of all useful sources. Where there are multiple sources
        associated with the same frag_index then only one is included.

        :return: a list of sources, each source being a tuple of form
                (ResumingGetter, iter)
        """
        all_sources = []
        for frag_index, sources in self.gets.items():
            if frag_index is None:
                # bad responses don't have a frag_index (and fake good
                # responses from some unit tests)
                all_sources.extend(sources)
            else:
                all_sources.extend(sources[:1])
        return all_sources

    def add_alternate_nodes(self, node, frag_indexes):
        for frag_index in frag_indexes:
            self.alt_nodes[frag_index].append(node)

    @property
    def shortfall(self):
        # A non-durable bucket always has a shortfall of at least 1
        result = self.policy.ec_ndata - len(self.get_responses())
        return max(result, 0 if self._durable else 1)

    @property
    def shortfall_with_alts(self):
        # The shortfall that we expect to have if we were to send requests
        # for frags on the alt nodes.
        alts = set(self.alt_nodes.keys()).difference(set(self.gets.keys()))
        result = self.policy.ec_ndata - (len(self.get_responses()) + len(alts))
        return max(result, 0 if self._durable else 1)

    def __str__(self):
        # return a string summarising bucket state, useful for debugging.
        return '<%s, %s, %s, %s(%s), %s>' \
               % (self.timestamp_str, self.status, self._durable,
                  self.shortfall, self.shortfall_with_alts, len(self.gets))


class ECGetResponseCollection(object):
    """
    Manages all successful EC GET responses gathered by ResumingGetters.

    A response comprises a tuple of (<getter instance>, <parts iterator>). All
    responses having the same data timestamp are placed in an
    ECGetResponseBucket for that timestamp. The buckets are stored in the
    'buckets' dict which maps timestamp-> bucket.

    This class encapsulates logic for selecting the best bucket from the
    collection, and for choosing alternate nodes.
    """
    def __init__(self, policy):
        """
        :param policy: an instance of ECStoragePolicy
        """
        self.policy = policy
        self.buckets = {}
        self.node_iter_count = 0

    def _get_bucket(self, timestamp_str):
        """
        :param timestamp_str: a string representation of a timestamp
        :return: ECGetResponseBucket for given timestamp
        """
        return self.buckets.setdefault(
            timestamp_str, ECGetResponseBucket(self.policy, timestamp_str))

    def add_response(self, get, parts_iter):
        """
        Add a response to the collection.

        :param get: An instance of
                    :class:`~swift.proxy.controllers.base.ResumingGetter`
        :param parts_iter: An iterator over response body parts
        :raises ValueError: if the response etag or status code values do not
            match any values previously received for the same timestamp
        """
        headers = get.last_headers
        # Add the response to the appropriate bucket keyed by data file
        # timestamp. Fall back to using X-Backend-Timestamp as key for object
        # servers that have not been upgraded.
        t_data_file = headers.get('X-Backend-Data-Timestamp')
        t_obj = headers.get('X-Backend-Timestamp', headers.get('X-Timestamp'))
        self._get_bucket(t_data_file or t_obj).add_response(get, parts_iter)

        # The node may also have alternate fragments indexes (possibly at
        # different timestamps). For each list of alternate fragments indexes,
        # find the bucket for their data file timestamp and add the node and
        # list to that bucket's alternate nodes.
        frag_sets = safe_json_loads(headers.get('X-Backend-Fragments')) or {}
        for t_frag, frag_set in frag_sets.items():
            self._get_bucket(t_frag).add_alternate_nodes(get.node, frag_set)
        # If the response includes a durable timestamp then mark that bucket as
        # durable. Note that this may be a different bucket than the one this
        # response got added to, and that we may never go and get a durable
        # frag from this node; it is sufficient that we have been told that a
        # durable frag exists, somewhere, at t_durable.
        t_durable = headers.get('X-Backend-Durable-Timestamp')
        if not t_durable and not t_data_file:
            # obj server not upgraded so assume this response's frag is durable
            t_durable = t_obj
        if t_durable:
            self._get_bucket(t_durable).set_durable()

    def _sort_buckets(self):
        def key_fn(bucket):
            # Returns a tuple to use for sort ordering:
            # buckets with no shortfall sort higher,
            # otherwise buckets with lowest shortfall_with_alts sort higher,
            # finally buckets with newer timestamps sort higher.
            return (bucket.shortfall <= 0,
                    (not (bucket.shortfall <= 0) and
                     (-1 * bucket.shortfall_with_alts)),
                    bucket.timestamp_str)

        return sorted(self.buckets.values(), key=key_fn, reverse=True)

    @property
    def best_bucket(self):
        """
        Return the best bucket in the collection.

        The "best" bucket is the newest timestamp with sufficient getters, or
        the closest to having sufficient getters, unless it is bettered by a
        bucket with potential alternate nodes.

        :return: An instance of :class:`~ECGetResponseBucket` or None if there
                 are no buckets in the collection.
        """
        sorted_buckets = self._sort_buckets()
        if sorted_buckets:
            return sorted_buckets[0]
        return None

    def _get_frag_prefs(self):
        # Construct the current frag_prefs list, with best_bucket prefs first.
        frag_prefs = []

        for bucket in self._sort_buckets():
            if bucket.timestamp_str:
                exclusions = [fi for fi in bucket.gets if fi is not None]
                prefs = {'timestamp': bucket.timestamp_str,
                         'exclude': exclusions}
                frag_prefs.append(prefs)

        return frag_prefs

    def get_extra_headers(self):
        frag_prefs = self._get_frag_prefs()
        return {'X-Backend-Fragment-Preferences': json.dumps(frag_prefs)}

    def _get_alternate_nodes(self):
        if self.node_iter_count <= self.policy.ec_ndata:
            # It makes sense to wait before starting to use alternate nodes,
            # because if we find sufficient frags on *distinct* nodes then we
            # spread work across mode nodes. There's no formal proof that
            # waiting for ec_ndata GETs is the right answer, but it seems
            # reasonable to try *at least* that many primary nodes before
            # resorting to alternate nodes.
            return None

        bucket = self.best_bucket
        if (bucket is None) or (bucket.shortfall <= 0):
            return None

        alt_frags = set(bucket.alt_nodes.keys())
        got_frags = set(bucket.gets.keys())
        wanted_frags = list(alt_frags.difference(got_frags))

        # We may have the same frag_index on more than one node so shuffle to
        # avoid using the same frag_index consecutively, since we may not get a
        # response from the last node provided before being asked to provide
        # another node.
        random.shuffle(wanted_frags)

        for frag_index in wanted_frags:
            nodes = bucket.alt_nodes.get(frag_index)
            if nodes:
                return nodes
        return None

    def has_alternate_node(self):
        return True if self._get_alternate_nodes() else False

    def provide_alternate_node(self):
        """
        Callback function that is installed in a NodeIter. Called on every call
        to NodeIter.next(), which means we can track the number of nodes to
        which GET requests have been made and selectively inject an alternate
        node, if we have one.

        :return: A dict describing a node to which the next GET request
                 should be made.
        """
        self.node_iter_count += 1
        nodes = self._get_alternate_nodes()
        if nodes:
            return nodes.pop(0).copy()


@ObjectControllerRouter.register(EC_POLICY)
class ECObjectController(BaseObjectController):
    def _fragment_GET_request(self, req, node_iter, partition, policy,
                              header_provider=None):
        """
        Makes a GET request for a fragment.
        """
        backend_headers = self.generate_request_headers(
            req, additional=req.headers)

        getter = ResumingGetter(self.app, req, 'Object', node_iter,
                                partition, req.swift_entity_path,
                                backend_headers,
                                client_chunk_size=policy.fragment_size,
                                newest=False, header_provider=header_provider)
        return (getter, getter.response_parts_iter(req))

    def _convert_range(self, req, policy):
        """
        Take the requested range(s) from the client and convert it to range(s)
        to be sent to the object servers.

        This includes widening requested ranges to full segments, then
        converting those ranges to fragments so that we retrieve the minimum
        number of fragments from the object server.

        Mutates the request passed in.

        Returns a list of range specs (dictionaries with the different byte
        indices in them).
        """
        # Since segments and fragments have different sizes, we need
        # to modify the Range header sent to the object servers to
        # make sure we get the right fragments out of the fragment
        # archives.
        segment_size = policy.ec_segment_size
        fragment_size = policy.fragment_size

        range_specs = []
        new_ranges = []
        for client_start, client_end in req.range.ranges:
            # TODO: coalesce ranges that overlap segments. For
            # example, "bytes=0-10,20-30,40-50" with a 64 KiB
            # segment size will result in a Range header in the
            # object request of "bytes=0-65535,0-65535,0-65535",
            # which is wasteful. We should be smarter and only
            # request that first segment once.
            segment_start, segment_end = client_range_to_segment_range(
                client_start, client_end, segment_size)

            fragment_start, fragment_end = \
                segment_range_to_fragment_range(
                    segment_start, segment_end,
                    segment_size, fragment_size)

            new_ranges.append((fragment_start, fragment_end))
            range_specs.append({'req_client_start': client_start,
                                'req_client_end': client_end,
                                'req_segment_start': segment_start,
                                'req_segment_end': segment_end,
                                'req_fragment_start': fragment_start,
                                'req_fragment_end': fragment_end})

        req.range = "bytes=" + ",".join(
            "%s-%s" % (s if s is not None else "",
                       e if e is not None else "")
            for s, e in new_ranges)
        return range_specs

    def _get_or_head_response(self, req, node_iter, partition, policy):
        update_etag_is_at_header(req, "X-Object-Sysmeta-Ec-Etag")

        if req.method == 'HEAD':
            # no fancy EC decoding here, just one plain old HEAD request to
            # one object server because all fragments hold all metadata
            # information about the object.
            concurrency = policy.ec_ndata if self.app.concurrent_gets else 1
            resp = self.GETorHEAD_base(
                req, _('Object'), node_iter, partition,
                req.swift_entity_path, concurrency)
            self._fix_response(req, resp)
            return resp

        # GET request
        orig_range = None
        range_specs = []
        if req.range:
            orig_range = req.range
            range_specs = self._convert_range(req, policy)

        safe_iter = GreenthreadSafeIterator(node_iter)

        # Sending the request concurrently to all nodes, and responding
        # with the first response isn't something useful for EC as all
        # nodes contain different fragments. Also EC has implemented it's
        # own specific implementation of concurrent gets to ec_ndata nodes.
        # So we don't need to  worry about plumbing and sending a
        # concurrency value to ResumingGetter.
        with ContextPool(policy.ec_ndata) as pool:
            pile = GreenAsyncPile(pool)
            buckets = ECGetResponseCollection(policy)
            node_iter.set_node_provider(buckets.provide_alternate_node)
            # include what may well be an empty X-Backend-Fragment-Preferences
            # header from the buckets.get_extra_headers to let the object
            # server know that it is ok to return non-durable fragments
            for _junk in range(policy.ec_ndata):
                pile.spawn(self._fragment_GET_request,
                           req, safe_iter, partition,
                           policy, buckets.get_extra_headers)

            bad_bucket = ECGetResponseBucket(policy, None)
            bad_bucket.set_durable()
            best_bucket = None
            extra_requests = 0
            # max_extra_requests is an arbitrary hard limit for spawning extra
            # getters in case some unforeseen scenario, or a misbehaving object
            # server, causes us to otherwise make endless requests e.g. if an
            # object server were to ignore frag_prefs and always respond with
            # a frag that is already in a bucket. Now we're assuming it should
            # be limit at most 2 * replicas.
            max_extra_requests = (
                (policy.object_ring.replica_count * 2) - policy.ec_ndata)

            for get, parts_iter in pile:
                if get.last_status is None:
                    # We may have spawned getters that find the node iterator
                    # has been exhausted. Ignore them.
                    # TODO: turns out that node_iter.nodes_left can bottom
                    # out at >0 when number of devs in ring is < 2* replicas,
                    # which definitely happens in tests and results in status
                    # of None. We should fix that but keep this guard because
                    # there is also a race between testing nodes_left/spawning
                    # a getter and an existing getter calling next(node_iter).
                    continue
                try:
                    if is_success(get.last_status):
                        # 2xx responses are managed by a response collection
                        buckets.add_response(get, parts_iter)
                    else:
                        # all other responses are lumped into a single bucket
                        bad_bucket.add_response(get, parts_iter)
                except ValueError as err:
                    self.app.logger.error(
                        _("Problem with fragment response: %s"), err)
                shortfall = bad_bucket.shortfall
                best_bucket = buckets.best_bucket
                if best_bucket:
                    shortfall = min(best_bucket.shortfall, shortfall)
                if (extra_requests < max_extra_requests and
                        shortfall > pile._pending and
                        (node_iter.nodes_left > 0 or
                         buckets.has_alternate_node())):
                    # we need more matching responses to reach ec_ndata
                    # than we have pending gets, as long as we still have
                    # nodes in node_iter we can spawn another
                    extra_requests += 1
                    pile.spawn(self._fragment_GET_request, req,
                               safe_iter, partition, policy,
                               buckets.get_extra_headers)

        req.range = orig_range
        if best_bucket and best_bucket.shortfall <= 0:
            # headers can come from any of the getters
            resp_headers = best_bucket.headers
            resp_headers.pop('Content-Range', None)
            eccl = resp_headers.get('X-Object-Sysmeta-Ec-Content-Length')
            obj_length = int(eccl) if eccl is not None else None

            # This is only true if we didn't get a 206 response, but
            # that's the only time this is used anyway.
            fa_length = int(resp_headers['Content-Length'])
            app_iter = ECAppIter(
                req.swift_entity_path,
                policy,
                [parts_iter for
                 _getter, parts_iter in best_bucket.get_responses()],
                range_specs, fa_length, obj_length,
                self.app.logger)
            resp = Response(
                request=req,
                conditional_response=True,
                app_iter=app_iter)
            update_headers(resp, resp_headers)
            try:
                app_iter.kickoff(req, resp)
            except HTTPException as err_resp:
                # catch any HTTPException response here so that we can
                # process response headers uniformly in _fix_response
                resp = err_resp
        else:
            # TODO: we can get here if all buckets are successful but none
            # have ec_ndata getters, so bad_bucket may have no gets and we will
            # return a 503 when a 404 may be more appropriate. We can also get
            # here with less than ec_ndata 416's and may then return a 416
            # which is also questionable because a non-range get for same
            # object would return 404 or 503.
            statuses = []
            reasons = []
            bodies = []
            headers = []
            for getter, _parts_iter in bad_bucket.get_responses():
                statuses.extend(getter.statuses)
                reasons.extend(getter.reasons)
                bodies.extend(getter.bodies)
                headers.extend(getter.source_headers)
            resp = self.best_response(
                req, statuses, reasons, bodies, 'Object',
                headers=headers)
        self._fix_response(req, resp)
        return resp

    def _fix_response(self, req, resp):
        # EC fragment archives each have different bytes, hence different
        # etags. However, they all have the original object's etag stored in
        # sysmeta, so we copy that here (if it exists) so the client gets it.
        resp.headers['Etag'] = resp.headers.get('X-Object-Sysmeta-Ec-Etag')
        # We're about to invoke conditional response checking so set the
        # correct conditional etag from wherever X-Backend-Etag-Is-At points,
        # if it exists at all.
        resp._conditional_etag = resolve_etag_is_at_header(req, resp.headers)
        if (is_success(resp.status_int) or is_redirection(resp.status_int) or
                resp.status_int == HTTP_REQUESTED_RANGE_NOT_SATISFIABLE):
            resp.accept_ranges = 'bytes'
        if is_success(resp.status_int):
            resp.headers['Content-Length'] = resp.headers.get(
                'X-Object-Sysmeta-Ec-Content-Length')
            resp.fix_conditional_response()
        if resp.status_int == HTTP_REQUESTED_RANGE_NOT_SATISFIABLE:
            resp.headers['Content-Range'] = 'bytes */%s' % resp.headers[
                'X-Object-Sysmeta-Ec-Content-Length']

    def _make_putter(self, node, part, req, headers):
        return MIMEPutter.connect(
            node, part, req.swift_entity_path, headers,
            conn_timeout=self.app.conn_timeout,
            node_timeout=self.app.node_timeout,
            logger=self.app.logger,
            need_multiphase=True)

    def _determine_chunk_destinations(self, putters, policy):
        """
        Given a list of putters, return a dict where the key is the putter
        and the value is the frag index to use.

        This is done so that we line up handoffs using the same frag index
        (in the primary part list) as the primary that the handoff is standing
        in for.  This lets erasure-code fragment archives wind up on the
        preferred local primary nodes when possible.

        :param putters: a list of swift.proxy.controllers.obj.MIMEPutter
                        instance
        :param policy: A policy instance which should be one of ECStoragePolicy
        """
        # Give each putter a "frag index": the index of the
        # transformed chunk that we'll send to it.
        #
        # For primary nodes, that's just its index (primary 0 gets
        # chunk 0, primary 1 gets chunk 1, and so on). For handoffs,
        # we assign the chunk index of a missing primary.
        handoff_conns = []
        putter_to_frag_index = {}
        for p in putters:
            if p.node_index is not None:
                putter_to_frag_index[p] = policy.get_backend_index(
                    p.node_index)
            else:
                handoff_conns.append(p)

        # Note: we may have more holes than handoffs. This is okay; it
        # just means that we failed to connect to one or more storage
        # nodes. Holes occur when a storage node is down, in which
        # case the connection is not replaced, and when a storage node
        # returns 507, in which case a handoff is used to replace it.

        # lack_list is a dict of list to keep hole indexes
        # e.g. if we have 2 holes for frag index 0 with ec_duplication_factor=2
        # lack_list is like {0: [0], 1: [0]}, and then, if 1 hole found
        # for frag index 1, lack_list will be {0: [0, 1], 1: [0]}.
        # After that, holes will be filled from bigger key
        # (i.e. 1:[0] at first)

        # Grouping all missing fragment indexes for each frag_index
        available_indexes = list(putter_to_frag_index.values())
        lack_list = collections.defaultdict(list)
        for frag_index in range(policy.ec_n_unique_fragments):
            # Set the missing index to lack_list
            available_count = available_indexes.count(frag_index)
            # N.B. it should be duplication_factor >= lack >= 0
            lack = policy.ec_duplication_factor - available_count
            # now we are missing one or more nodes to store the frag index
            for lack_tier in range(lack):
                lack_list[lack_tier].append(frag_index)

        # Extract the lack_list to a flat list
        holes = []
        for lack_tier, indexes in sorted(lack_list.items(), reverse=True):
            holes.extend(indexes)

        # Fill putter_to_frag_index list with the hole list
        for hole, p in zip(holes, handoff_conns):
            putter_to_frag_index[p] = hole
        return putter_to_frag_index

    def _transfer_data(self, req, policy, data_source, putters, nodes,
                       min_conns, etag_hasher):
        """
        Transfer data for an erasure coded object.

        This method was added in the PUT method extraction change
        """
        bytes_transferred = 0
        chunk_transform = chunk_transformer(policy)
        chunk_transform.send(None)
        frag_hashers = collections.defaultdict(md5)

        def send_chunk(chunk):
            # Note: there's two different hashers in here. etag_hasher is
            # hashing the original object so that we can validate the ETag
            # that the client sent (and etag_hasher is None if the client
            # didn't send one). The hasher in frag_hashers is hashing the
            # fragment archive being sent to the client; this lets us guard
            # against data corruption on the network between proxy and
            # object server.
            if etag_hasher:
                etag_hasher.update(chunk)
            backend_chunks = chunk_transform.send(chunk)
            if backend_chunks is None:
                # If there's not enough bytes buffered for erasure-encoding
                # or whatever we're doing, the transform will give us None.
                return

            updated_frag_indexes = set()
            for putter in list(putters):
                frag_index = putter_to_frag_index[putter]
                backend_chunk = backend_chunks[frag_index]
                if not putter.failed:
                    # N.B. same frag_index will appear when using
                    # ec_duplication_factor >= 2. So skip to feed the chunk
                    # to hasher if the frag was updated already.
                    if frag_index not in updated_frag_indexes:
                        frag_hashers[frag_index].update(backend_chunk)
                        updated_frag_indexes.add(frag_index)
                    putter.send_chunk(backend_chunk)
                else:
                    putter.close()
                    putters.remove(putter)
            self._check_min_conn(
                req, putters, min_conns,
                msg=_('Object PUT exceptions during send, '
                      '%(conns)s/%(nodes)s required connections'))

        try:
            with ContextPool(len(putters)) as pool:

                # build our putter_to_frag_index dict to place handoffs in the
                # same part nodes index as the primaries they are covering
                putter_to_frag_index = self._determine_chunk_destinations(
                    putters, policy)

                for putter in putters:
                    putter.spawn_sender_greenthread(
                        pool, self.app.put_queue_depth, self.app.node_timeout,
                        self.app.exception_occurred)
                while True:
                    with ChunkReadTimeout(self.app.client_timeout):
                        try:
                            chunk = next(data_source)
                        except StopIteration:
                            break
                    bytes_transferred += len(chunk)
                    if bytes_transferred > constraints.MAX_FILE_SIZE:
                        raise HTTPRequestEntityTooLarge(request=req)

                    send_chunk(chunk)

                if req.content_length and (
                        bytes_transferred < req.content_length):
                    req.client_disconnect = True
                    self.app.logger.warning(
                        _('Client disconnected without sending enough data'))
                    self.app.logger.increment('client_disconnects')
                    raise HTTPClientDisconnect(request=req)

                send_chunk(b'')  # flush out any buffered data

                computed_etag = (etag_hasher.hexdigest()
                                 if etag_hasher else None)
                footers = self._get_footers(req)
                received_etag = footers.get('etag', req.headers.get(
                    'etag', '')).strip('"')
                if (computed_etag and received_etag and
                   computed_etag != received_etag):
                    raise HTTPUnprocessableEntity(request=req)

                # Remove any EC reserved metadata names from footers
                footers = {(k, v) for k, v in footers.items()
                           if not k.lower().startswith('x-object-sysmeta-ec-')}
                for putter in putters:
                    frag_index = putter_to_frag_index[putter]
                    # Update any footers set by middleware with EC footers
                    trail_md = trailing_metadata(
                        policy, etag_hasher,
                        bytes_transferred, frag_index)
                    trail_md.update(footers)
                    # Etag footer must always be hash of what we sent
                    trail_md['Etag'] = frag_hashers[frag_index].hexdigest()
                    putter.end_of_object_data(footer_metadata=trail_md)

                for putter in putters:
                    putter.wait()

                # for storage policies requiring 2-phase commit (e.g.
                # erasure coding), enforce >= 'quorum' number of
                # 100-continue responses - this indicates successful
                # object data and metadata commit and is a necessary
                # condition to be met before starting 2nd PUT phase
                final_phase = False
                statuses, reasons, bodies, _junk = \
                    self._get_put_responses(
                        req, putters, len(nodes), final_phase=final_phase,
                        min_responses=min_conns)
                if not self.have_quorum(
                        statuses, len(nodes), quorum=min_conns):
                    self.app.logger.error(
                        _('Not enough object servers ack\'ed (got %d)'),
                        statuses.count(HTTP_CONTINUE))
                    raise HTTPServiceUnavailable(request=req)

                elif not self._have_adequate_informational(
                        statuses, min_conns):
                    resp = self.best_response(req, statuses, reasons, bodies,
                                              _('Object PUT'),
                                              quorum_size=min_conns)
                    if is_client_error(resp.status_int):
                        # if 4xx occurred in this state it is absolutely
                        # a bad conversation between proxy-server and
                        # object-server (even if it's
                        # HTTP_UNPROCESSABLE_ENTITY) so we should regard this
                        # as HTTPServiceUnavailable.
                        raise HTTPServiceUnavailable(request=req)
                    else:
                        # Other errors should use raw best_response
                        raise resp

                # quorum achieved, start 2nd phase - send commit
                # confirmation to participating object servers
                # so they write a .durable state file indicating
                # a successful PUT
                for putter in putters:
                    putter.send_commit_confirmation()
                for putter in putters:
                    putter.wait()
        except ChunkReadTimeout as err:
            self.app.logger.warning(
                _('ERROR Client read timeout (%ss)'), err.seconds)
            self.app.logger.increment('client_timeouts')
            raise HTTPRequestTimeout(request=req)
        except ChunkReadError:
            req.client_disconnect = True
            self.app.logger.warning(
                _('Client disconnected without sending last chunk'))
            self.app.logger.increment('client_disconnects')
            raise HTTPClientDisconnect(request=req)
        except HTTPException:
            raise
        except Timeout:
            self.app.logger.exception(
                _('ERROR Exception causing client disconnect'))
            raise HTTPClientDisconnect(request=req)
        except Exception:
            self.app.logger.exception(
                _('ERROR Exception transferring data to object servers %s'),
                {'path': req.path})
            raise HTTPInternalServerError(request=req)

    def _have_adequate_responses(
            self, statuses, min_responses, conditional_func):
        """
        Given a list of statuses from several requests, determine if a
        satisfactory number of nodes have responded with 1xx or 2xx statuses to
        deem the transaction for a successful response to the client.

        :param statuses: list of statuses returned so far
        :param min_responses: minimal pass criterion for number of successes
        :param conditional_func: a callable function to check http status code
        :returns: True or False, depending on current number of successes
        """
        if sum(1 for s in statuses if (conditional_func(s))) >= min_responses:
            return True
        return False

    def _have_adequate_successes(self, statuses, min_responses):
        """
        Partial method of _have_adequate_responses for 2xx
        """
        return self._have_adequate_responses(
            statuses, min_responses, is_success)

    def _have_adequate_informational(self, statuses, min_responses):
        """
        Partial method of _have_adequate_responses for 1xx
        """
        return self._have_adequate_responses(
            statuses, min_responses, is_informational)

    def _have_adequate_put_responses(self, statuses, num_nodes, min_responses):
        # For an EC PUT we require a quorum of responses with success statuses
        # in order to move on to next phase of PUT request handling without
        # having to wait for *all* responses.
        # TODO: this implies that in the first phase of the backend PUTs when
        # we are actually expecting 1xx responses that we will end up waiting
        # for *all* responses. That seems inefficient since we only need a
        # quorum of 1xx responses to proceed.
        return self._have_adequate_successes(statuses, min_responses)

    def _store_object(self, req, data_source, nodes, partition,
                      outgoing_headers):
        """
        Store an erasure coded object.
        """
        policy_index = int(req.headers.get('X-Backend-Storage-Policy-Index'))
        policy = POLICIES.get_by_index(policy_index)

        expected_frag_size = None
        if req.content_length:
            # TODO: PyECLib <= 1.2.0 looks to return the segment info
            # different from the input for aligned data efficiency but
            # Swift never does. So calculate the fragment length Swift
            # will actually send to object server by making two different
            # get_segment_info calls (until PyECLib fixed).
            # policy.fragment_size makes the call using segment size,
            # and the next call is to get info for the last segment

            # get number of fragments except the tail - use truncation //
            num_fragments = req.content_length // policy.ec_segment_size
            expected_frag_size = policy.fragment_size * num_fragments

            # calculate the tail fragment_size by hand and add it to
            # expected_frag_size
            last_segment_size = req.content_length % policy.ec_segment_size
            if last_segment_size:
                last_info = policy.pyeclib_driver.get_segment_info(
                    last_segment_size, policy.ec_segment_size)
                expected_frag_size += last_info['fragment_size']
        for headers in outgoing_headers:
            headers['X-Backend-Obj-Content-Length'] = expected_frag_size
            # the object server will get different bytes, so these
            # values do not apply.
            headers.pop('Content-Length', None)
            headers.pop('Etag', None)

        # Since the request body sent from client -> proxy is not
        # the same as the request body sent proxy -> object, we
        # can't rely on the object-server to do the etag checking -
        # so we have to do it here.
        etag_hasher = md5()

        min_conns = policy.quorum
        putters = self._get_put_connections(
            req, nodes, partition, outgoing_headers, policy)

        try:
            # check that a minimum number of connections were established and
            # meet all the correct conditions set in the request
            self._check_failure_put_connections(putters, req, min_conns)

            self._transfer_data(req, policy, data_source, putters,
                                nodes, min_conns, etag_hasher)
            # The durable state will propagate in a replicated fashion; if
            # one fragment is durable then the reconstructor will spread the
            # durable status around.
            # In order to avoid successfully writing an object, but refusing
            # to serve it on a subsequent GET because don't have enough
            # durable data fragments - we require the same number of durable
            # writes as quorum fragment writes.  If object servers are in the
            # future able to serve their non-durable fragment archives we may
            # be able to reduce this quorum count if needed.
            # ignore response etags
            statuses, reasons, bodies, _etags = \
                self._get_put_responses(req, putters, len(nodes),
                                        final_phase=True,
                                        min_responses=min_conns)
        except HTTPException as resp:
            return resp
        finally:
            for putter in putters:
                putter.close()

        etag = etag_hasher.hexdigest()
        resp = self.best_response(req, statuses, reasons, bodies,
                                  _('Object PUT'), etag=etag,
                                  quorum_size=min_conns)
        resp.last_modified = math.ceil(
            float(Timestamp(req.headers['X-Timestamp'])))
        return resp
