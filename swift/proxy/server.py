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

import mimetypes
import os
import socket
from swift import gettext_ as _
from random import shuffle
from time import time
import itertools
import functools
import sys

from eventlet import Timeout

from swift import __canonical_version__ as swift_version
from swift.common import constraints
from swift.common.storage_policy import POLICIES
from swift.common.ring import Ring
from swift.common.utils import cache_from_env, get_logger, \
    get_remote_client, split_path, config_true_value, generate_trans_id, \
    affinity_key_function, affinity_locality_predicate, list_from_csv, \
    register_swift_info
from swift.common.constraints import check_utf8, valid_api_version
from swift.proxy.controllers import AccountController, ContainerController, \
    ObjectControllerRouter, InfoController
from swift.proxy.controllers.base import get_container_info
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPMethodNotAllowed, HTTPNotFound, HTTPPreconditionFailed, \
    HTTPServerError, HTTPException, Request, HTTPServiceUnavailable
from swift.common.exceptions import APIVersionError


# List of entry points for mandatory middlewares.
#
# Fields:
#
# "name" (required) is the entry point name from setup.py.
#
# "after_fn" (optional) a function that takes a PipelineWrapper object as its
# single argument and returns a list of middlewares that this middleware
# should come after. Any middlewares in the returned list that are not present
# in the pipeline will be ignored, so you can safely name optional middlewares
# to come after. For example, ["catch_errors", "bulk"] would install this
# middleware after catch_errors and bulk if both were present, but if bulk
# were absent, would just install it after catch_errors.

required_filters = [
    {'name': 'catch_errors'},
    {'name': 'gatekeeper',
     'after_fn': lambda pipe: (['catch_errors']
                               if pipe.startswith('catch_errors')
                               else [])},
    {'name': 'dlo', 'after_fn': lambda _junk: [
        'staticweb', 'tempauth', 'keystoneauth',
        'catch_errors', 'gatekeeper', 'proxy_logging']}]


class Application(object):
    """WSGI application for the proxy server."""

    def __init__(self, conf, memcache=None, logger=None, account_ring=None,
                 container_ring=None):
        if conf is None:
            conf = {}
        if logger is None:
            self.logger = get_logger(conf, log_route='proxy-server')
        else:
            self.logger = logger

        self._error_limiting = {}

        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.swift_dir = swift_dir
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.recoverable_node_timeout = int(
            conf.get('recoverable_node_timeout', self.node_timeout))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.client_timeout = int(conf.get('client_timeout', 60))
        self.put_queue_depth = int(conf.get('put_queue_depth', 10))
        self.object_chunk_size = int(conf.get('object_chunk_size', 65536))
        self.client_chunk_size = int(conf.get('client_chunk_size', 65536))
        self.trans_id_suffix = conf.get('trans_id_suffix', '')
        self.post_quorum_timeout = float(conf.get('post_quorum_timeout', 0.5))
        self.error_suppression_interval = \
            int(conf.get('error_suppression_interval', 60))
        self.error_suppression_limit = \
            int(conf.get('error_suppression_limit', 10))
        self.recheck_container_existence = \
            int(conf.get('recheck_container_existence', 60))
        self.recheck_account_existence = \
            int(conf.get('recheck_account_existence', 60))
        self.allow_account_management = \
            config_true_value(conf.get('allow_account_management', 'no'))
        self.object_post_as_copy = \
            config_true_value(conf.get('object_post_as_copy', 'true'))
        self.container_ring = container_ring or Ring(swift_dir,
                                                     ring_name='container')
        self.account_ring = account_ring or Ring(swift_dir,
                                                 ring_name='account')
        # ensure rings are loaded for all configured storage policies
        for policy in POLICIES:
            policy.load_ring(swift_dir)
        self.obj_controller_router = ObjectControllerRouter()
        self.memcache = memcache
        mimetypes.init(mimetypes.knownfiles +
                       [os.path.join(swift_dir, 'mime.types')])
        self.account_autocreate = \
            config_true_value(conf.get('account_autocreate', 'no'))
        self.auto_create_account_prefix = (
            conf.get('auto_create_account_prefix') or '.')
        self.expiring_objects_account = self.auto_create_account_prefix + \
            (conf.get('expiring_objects_account_name') or 'expiring_objects')
        self.expiring_objects_container_divisor = \
            int(conf.get('expiring_objects_container_divisor') or 86400)
        self.max_containers_per_account = \
            int(conf.get('max_containers_per_account') or 0)
        self.max_containers_whitelist = [
            a.strip()
            for a in conf.get('max_containers_whitelist', '').split(',')
            if a.strip()]
        self.deny_host_headers = [
            host.strip() for host in
            conf.get('deny_host_headers', '').split(',') if host.strip()]
        self.log_handoffs = config_true_value(conf.get('log_handoffs', 'true'))
        self.cors_allow_origin = [
            a.strip()
            for a in conf.get('cors_allow_origin', '').split(',')
            if a.strip()]
        self.strict_cors_mode = config_true_value(
            conf.get('strict_cors_mode', 't'))
        self.node_timings = {}
        self.timing_expiry = int(conf.get('timing_expiry', 300))
        self.sorting_method = conf.get('sorting_method', 'shuffle').lower()
        self.max_large_object_get_time = float(
            conf.get('max_large_object_get_time', '86400'))
        value = conf.get('request_node_count', '2 * replicas').lower().split()
        if len(value) == 1:
            rnc_value = int(value[0])
            self.request_node_count = lambda replicas: rnc_value
        elif len(value) == 3 and value[1] == '*' and value[2] == 'replicas':
            rnc_value = int(value[0])
            self.request_node_count = lambda replicas: rnc_value * replicas
        else:
            raise ValueError(
                'Invalid request_node_count value: %r' % ''.join(value))
        try:
            self._read_affinity = read_affinity = conf.get('read_affinity', '')
            self.read_affinity_sort_key = affinity_key_function(read_affinity)
        except ValueError as err:
            # make the message a little more useful
            raise ValueError("Invalid read_affinity value: %r (%s)" %
                             (read_affinity, err.message))
        try:
            write_affinity = conf.get('write_affinity', '')
            self.write_affinity_is_local_fn \
                = affinity_locality_predicate(write_affinity)
        except ValueError as err:
            # make the message a little more useful
            raise ValueError("Invalid write_affinity value: %r (%s)" %
                             (write_affinity, err.message))
        value = conf.get('write_affinity_node_count',
                         '2 * replicas').lower().split()
        if len(value) == 1:
            wanc_value = int(value[0])
            self.write_affinity_node_count = lambda replicas: wanc_value
        elif len(value) == 3 and value[1] == '*' and value[2] == 'replicas':
            wanc_value = int(value[0])
            self.write_affinity_node_count = \
                lambda replicas: wanc_value * replicas
        else:
            raise ValueError(
                'Invalid write_affinity_node_count value: %r' % ''.join(value))
        # swift_owner_headers are stripped by the account and container
        # controllers; we should extend header stripping to object controller
        # when a privileged object header is implemented.
        swift_owner_headers = conf.get(
            'swift_owner_headers',
            'x-container-read, x-container-write, '
            'x-container-sync-key, x-container-sync-to, '
            'x-account-meta-temp-url-key, x-account-meta-temp-url-key-2, '
            'x-container-meta-temp-url-key, x-container-meta-temp-url-key-2, '
            'x-account-access-control')
        self.swift_owner_headers = [
            name.strip().title()
            for name in swift_owner_headers.split(',') if name.strip()]
        # Initialization was successful, so now apply the client chunk size
        # parameter as the default read / write buffer size for the network
        # sockets.
        #
        # NOTE WELL: This is a class setting, so until we get set this on a
        # per-connection basis, this affects reading and writing on ALL
        # sockets, those between the proxy servers and external clients, and
        # those between the proxy servers and the other internal servers.
        #
        # ** Because it affects the client as well, currently, we use the
        # client chunk size as the govenor and not the object chunk size.
        socket._fileobject.default_bufsize = self.client_chunk_size
        self.expose_info = config_true_value(
            conf.get('expose_info', 'yes'))
        self.disallowed_sections = list_from_csv(
            conf.get('disallowed_sections', 'swift.valid_api_versions'))
        self.admin_key = conf.get('admin_key', None)
        register_swift_info(
            version=swift_version,
            strict_cors_mode=self.strict_cors_mode,
            policies=POLICIES.get_policy_info(),
            allow_account_management=self.allow_account_management,
            account_autocreate=self.account_autocreate,
            **constraints.EFFECTIVE_CONSTRAINTS)

    def check_config(self):
        """
        Check the configuration for possible errors
        """
        if self._read_affinity and self.sorting_method != 'affinity':
            self.logger.warn("sorting_method is set to '%s', not 'affinity'; "
                             "read_affinity setting will have no effect." %
                             self.sorting_method)

    def get_object_ring(self, policy_idx):
        """
        Get the ring object to use to handle a request based on its policy.

        :param policy_idx: policy index as defined in swift.conf

        :returns: appropriate ring object
        """
        return POLICIES.get_object_ring(policy_idx, self.swift_dir)

    def get_controller(self, req):
        """
        Get the controller to handle a request.

        :param req: the request
        :returns: tuple of (controller class, path dictionary)

        :raises: ValueError (thrown by split_path) if given invalid path
        """
        if req.path == '/info':
            d = dict(version=None,
                     expose_info=self.expose_info,
                     disallowed_sections=self.disallowed_sections,
                     admin_key=self.admin_key)
            return InfoController, d

        version, account, container, obj = split_path(req.path, 1, 4, True)
        d = dict(version=version,
                 account_name=account,
                 container_name=container,
                 object_name=obj)
        if account and not valid_api_version(version):
            raise APIVersionError('Invalid path')
        if obj and container and account:
            info = get_container_info(req.environ, self)
            policy_index = req.headers.get('X-Backend-Storage-Policy-Index',
                                           info['storage_policy'])
            policy = POLICIES.get_by_index(policy_index)
            if not policy:
                # This indicates that a new policy has been created,
                # with rings, deployed, released (i.e. deprecated =
                # False), used by a client to create a container via
                # another proxy that was restarted after the policy
                # was released, and is now cached - all before this
                # worker was HUPed to stop accepting new
                # connections.  There should never be an "unknown"
                # index - but when there is - it's probably operator
                # error and hopefully temporary.
                raise HTTPServiceUnavailable('Unknown Storage Policy')
            return self.obj_controller_router[policy], d
        elif container and account:
            return ContainerController, d
        elif account and not container and not obj:
            return AccountController, d
        return None, d

    def __call__(self, env, start_response):
        """
        WSGI entry point.
        Wraps env in swob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            if self.memcache is None:
                self.memcache = cache_from_env(env, True)
            req = self.update_request(Request(env))
            return self.handle_request(req)(env, start_response)
        except UnicodeError:
            err = HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
            return err(env, start_response)
        except (Exception, Timeout):
            start_response('500 Server Error',
                           [('Content-Type', 'text/plain')])
            return ['Internal server error.\n']

    def update_request(self, req):
        if 'x-storage-token' in req.headers and \
                'x-auth-token' not in req.headers:
            req.headers['x-auth-token'] = req.headers['x-storage-token']
        return req

    def handle_request(self, req):
        """
        Entry point for proxy server.
        Should return a WSGI-style callable (such as swob.Response).

        :param req: swob.Request object
        """
        try:
            self.logger.set_statsd_prefix('proxy-server')
            if req.content_length and req.content_length < 0:
                self.logger.increment('errors')
                return HTTPBadRequest(request=req,
                                      body='Invalid Content-Length')

            try:
                if not check_utf8(req.path_info):
                    self.logger.increment('errors')
                    return HTTPPreconditionFailed(
                        request=req, body='Invalid UTF8 or contains NULL')
            except UnicodeError:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(
                    request=req, body='Invalid UTF8 or contains NULL')

            try:
                controller, path_parts = self.get_controller(req)
                p = req.path_info
                if isinstance(p, unicode):
                    p = p.encode('utf-8')
            except APIVersionError:
                self.logger.increment('errors')
                return HTTPBadRequest(request=req)
            except ValueError:
                self.logger.increment('errors')
                return HTTPNotFound(request=req)
            if not controller:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(request=req, body='Bad URL')
            if self.deny_host_headers and \
                    req.host.split(':')[0] in self.deny_host_headers:
                return HTTPForbidden(request=req, body='Invalid host header')

            self.logger.set_statsd_prefix('proxy-server.' +
                                          controller.server_type.lower())
            controller = controller(self, **path_parts)
            if 'swift.trans_id' not in req.environ:
                # if this wasn't set by an earlier middleware, set it now
                trans_id_suffix = self.trans_id_suffix
                trans_id_extra = req.headers.get('x-trans-id-extra')
                if trans_id_extra:
                    trans_id_suffix += '-' + trans_id_extra[:32]
                trans_id = generate_trans_id(trans_id_suffix)
                req.environ['swift.trans_id'] = trans_id
                self.logger.txn_id = trans_id
            req.headers['x-trans-id'] = req.environ['swift.trans_id']
            controller.trans_id = req.environ['swift.trans_id']
            self.logger.client_ip = get_remote_client(req)
            try:
                handler = getattr(controller, req.method)
                getattr(handler, 'publicly_accessible')
            except AttributeError:
                allowed_methods = getattr(controller, 'allowed_methods', set())
                return HTTPMethodNotAllowed(
                    request=req, headers={'Allow': ', '.join(allowed_methods)})
            if 'swift.authorize' in req.environ:
                # We call authorize before the handler, always. If authorized,
                # we remove the swift.authorize hook so isn't ever called
                # again. If not authorized, we return the denial unless the
                # controller's method indicates it'd like to gather more
                # information and try again later.
                resp = req.environ['swift.authorize'](req)
                if not resp and not req.headers.get('X-Copy-From-Account') \
                        and not req.headers.get('Destination-Account'):
                    # No resp means authorized, no delayed recheck required.
                    del req.environ['swift.authorize']
                else:
                    # Response indicates denial, but we might delay the denial
                    # and recheck later. If not delayed, return the error now.
                    if not getattr(handler, 'delay_denial', None):
                        return resp
            # Save off original request method (GET, POST, etc.) in case it
            # gets mutated during handling.  This way logging can display the
            # method the client actually sent.
            req.environ['swift.orig_req_method'] = req.method
            return handler(req)
        except HTTPException as error_response:
            return error_response
        except (Exception, Timeout):
            self.logger.exception(_('ERROR Unhandled exception in request'))
            return HTTPServerError(request=req)

    def sort_nodes(self, nodes):
        '''
        Sorts nodes in-place (and returns the sorted list) according to
        the configured strategy. The default "sorting" is to randomly
        shuffle the nodes. If the "timing" strategy is chosen, the nodes
        are sorted according to the stored timing data.
        '''
        # In the case of timing sorting, shuffling ensures that close timings
        # (ie within the rounding resolution) won't prefer one over another.
        # Python's sort is stable (http://wiki.python.org/moin/HowTo/Sorting/)
        shuffle(nodes)
        if self.sorting_method == 'timing':
            now = time()

            def key_func(node):
                timing, expires = self.node_timings.get(node['ip'], (-1.0, 0))
                return timing if expires > now else -1.0
            nodes.sort(key=key_func)
        elif self.sorting_method == 'affinity':
            nodes.sort(key=self.read_affinity_sort_key)
        return nodes

    def set_node_timing(self, node, timing):
        if self.sorting_method != 'timing':
            return
        now = time()
        timing = round(timing, 3)  # sort timings to the millisecond
        self.node_timings[node['ip']] = (timing, now + self.timing_expiry)

    def _error_limit_node_key(self, node):
        return "{ip}:{port}/{device}".format(**node)

    def error_limited(self, node):
        """
        Check if the node is currently error limited.

        :param node: dictionary of node to check
        :returns: True if error limited, False otherwise
        """
        now = time()
        node_key = self._error_limit_node_key(node)
        error_stats = self._error_limiting.get(node_key)

        if error_stats is None or 'errors' not in error_stats:
            return False
        if 'last_error' in error_stats and error_stats['last_error'] < \
                now - self.error_suppression_interval:
            self._error_limiting.pop(node_key, None)
            return False
        limited = error_stats['errors'] > self.error_suppression_limit
        if limited:
            self.logger.debug(
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
        node_key = self._error_limit_node_key(node)
        error_stats = self._error_limiting.setdefault(node_key, {})
        error_stats['errors'] = self.error_suppression_limit + 1
        error_stats['last_error'] = time()
        self.logger.error(_('%(msg)s %(ip)s:%(port)s/%(device)s'),
                          {'msg': msg, 'ip': node['ip'],
                          'port': node['port'], 'device': node['device']})

    def _incr_node_errors(self, node):
        node_key = self._error_limit_node_key(node)
        error_stats = self._error_limiting.setdefault(node_key, {})
        error_stats['errors'] = error_stats.get('errors', 0) + 1
        error_stats['last_error'] = time()

    def error_occurred(self, node, msg):
        """
        Handle logging, and handling of errors.

        :param node: dictionary of node to handle errors for
        :param msg: error message
        """
        self._incr_node_errors(node)
        self.logger.error(_('%(msg)s %(ip)s:%(port)s/%(device)s'),
                          {'msg': msg, 'ip': node['ip'],
                          'port': node['port'], 'device': node['device']})

    def iter_nodes(self, ring, partition, node_iter=None):
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
        primary_nodes = self.sort_nodes(
            list(itertools.islice(node_iter, num_primary_nodes)))
        handoff_nodes = node_iter
        nodes_left = self.request_node_count(len(primary_nodes))

        log_handoffs_threshold = nodes_left - len(primary_nodes)
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
                if self.log_handoffs and handoffs > log_handoffs_threshold:
                    self.logger.increment('handoff_count')
                    self.logger.warning(
                        'Handoff requested (%d)' % handoffs)
                    if handoffs - log_handoffs_threshold == len(primary_nodes):
                        self.logger.increment('handoff_all_count')
                yield node
                if not self.error_limited(node):
                    nodes_left -= 1
                    if nodes_left <= 0:
                        return

    def exception_occurred(self, node, typ, additional_info,
                           **kwargs):
        """
        Handle logging of generic exceptions.

        :param node: dictionary of node to log the error for
        :param typ: server type
        :param additional_info: additional information to log
        """
        self._incr_node_errors(node)
        if 'level' in kwargs:
            log = functools.partial(self.logger.log, kwargs.pop('level'))
            if 'exc_info' not in kwargs:
                kwargs['exc_info'] = sys.exc_info()
        else:
            log = self.logger.exception
        log(_('ERROR with %(type)s server %(ip)s:%(port)s/%(device)s'
              ' re: %(info)s'), {
                  'type': typ, 'ip': node['ip'], 'port':
                  node['port'], 'device': node['device'],
                  'info': additional_info
              }, **kwargs)

    def modify_wsgi_pipeline(self, pipe):
        """
        Called during WSGI pipeline creation. Modifies the WSGI pipeline
        context to ensure that mandatory middleware is present in the pipeline.

        :param pipe: A PipelineWrapper object
        """
        pipeline_was_modified = False
        for filter_spec in reversed(required_filters):
            filter_name = filter_spec['name']
            if filter_name not in pipe:
                afters = filter_spec.get('after_fn', lambda _junk: [])(pipe)
                insert_at = 0
                for after in afters:
                    try:
                        insert_at = max(insert_at, pipe.index(after) + 1)
                    except ValueError:  # not in pipeline; ignore it
                        pass
                self.logger.info(
                    'Adding required filter %s to pipeline at position %d' %
                    (filter_name, insert_at))
                ctx = pipe.create_filter(filter_name)
                pipe.insert_filter(ctx, index=insert_at)
                pipeline_was_modified = True

        if pipeline_was_modified:
            self.logger.info("Pipeline was modified. New pipeline is \"%s\".",
                             pipe)
        else:
            self.logger.debug("Pipeline is \"%s\"", pipe)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    app = Application(conf)
    app.check_config()
    return app
