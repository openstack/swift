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

import mimetypes
import os
from swift import gettext_ as _
from random import shuffle
from time import time

from eventlet import Timeout

from swift.common.ring import Ring
from swift.common.utils import cache_from_env, get_logger, \
    get_remote_client, split_path, config_true_value, generate_trans_id, \
    affinity_key_function, affinity_locality_predicate
from swift.common.constraints import check_utf8
from swift.proxy.controllers import AccountController, ObjectController, \
    ContainerController
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPMethodNotAllowed, HTTPNotFound, HTTPPreconditionFailed, \
    HTTPServerError, HTTPException, Request


class Application(object):
    """WSGI application for the proxy server."""

    def __init__(self, conf, memcache=None, logger=None, account_ring=None,
                 container_ring=None, object_ring=None):
        if conf is None:
            conf = {}
        if logger is None:
            self.logger = get_logger(conf, log_route='proxy-server')
        else:
            self.logger = logger

        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.node_timeout = int(conf.get('node_timeout', 10))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.client_timeout = int(conf.get('client_timeout', 60))
        self.put_queue_depth = int(conf.get('put_queue_depth', 10))
        self.object_chunk_size = int(conf.get('object_chunk_size', 65536))
        self.client_chunk_size = int(conf.get('client_chunk_size', 65536))
        self.trans_id_suffix = conf.get('trans_id_suffix', '')
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
        self.object_ring = object_ring or Ring(swift_dir, ring_name='object')
        self.container_ring = container_ring or Ring(swift_dir,
                                                     ring_name='container')
        self.account_ring = account_ring or Ring(swift_dir,
                                                 ring_name='account')
        self.memcache = memcache
        mimetypes.init(mimetypes.knownfiles +
                       [os.path.join(swift_dir, 'mime.types')])
        self.account_autocreate = \
            config_true_value(conf.get('account_autocreate', 'no'))
        self.expiring_objects_account = \
            (conf.get('auto_create_account_prefix') or '.') + \
            'expiring_objects'
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
        self.rate_limit_after_segment = \
            int(conf.get('rate_limit_after_segment', 10))
        self.rate_limit_segments_per_sec = \
            int(conf.get('rate_limit_segments_per_sec', 1))
        self.log_handoffs = config_true_value(conf.get('log_handoffs', 'true'))
        self.cors_allow_origin = [
            a.strip()
            for a in conf.get('cors_allow_origin', '').split(',')
            if a.strip()]
        self.node_timings = {}
        self.timing_expiry = int(conf.get('timing_expiry', 300))
        self.sorting_method = conf.get('sorting_method', 'shuffle').lower()
        self.allow_static_large_object = config_true_value(
            conf.get('allow_static_large_object', 'true'))
        self.max_large_object_get_time = float(
            conf.get('max_large_object_get_time', '86400'))
        value = conf.get('request_node_count', '2 * replicas').lower().split()
        if len(value) == 1:
            value = int(value[0])
            self.request_node_count = lambda r: value
        elif len(value) == 3 and value[1] == '*' and value[2] == 'replicas':
            value = int(value[0])
            self.request_node_count = lambda r: value * r.replica_count
        else:
            raise ValueError(
                'Invalid request_node_count value: %r' % ''.join(value))
        try:
            read_affinity = conf.get('read_affinity', '')
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
            value = int(value[0])
            self.write_affinity_node_count = lambda r: value
        elif len(value) == 3 and value[1] == '*' and value[2] == 'replicas':
            value = int(value[0])
            self.write_affinity_node_count = lambda r: value * r.replica_count
        else:
            raise ValueError(
                'Invalid write_affinity_node_count value: %r' % ''.join(value))
        swift_owner_headers = conf.get(
            'swift_owner_headers',
            'x-container-read, x-container-write, '
            'x-container-sync-key, x-container-sync-to, '
            'x-account-meta-temp-url-key, x-account-meta-temp-url-key-2')
        self.swift_owner_headers = [
            name.strip()
            for name in swift_owner_headers.split(',') if name.strip()]

    def get_controller(self, path):
        """
        Get the controller to handle a request.

        :param path: path from request
        :returns: tuple of (controller class, path dictionary)

        :raises: ValueError (thrown by split_path) if given invalid path
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
        Wraps env in swob.Request object and passes it down.

        :param env: WSGI environment dictionary
        :param start_response: WSGI callable
        """
        try:
            if self.memcache is None:
                self.memcache = cache_from_env(env)
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
                controller, path_parts = self.get_controller(req.path)
                p = req.path_info
                if isinstance(p, unicode):
                    p = p.encode('utf-8')
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
                trans_id = generate_trans_id(self.trans_id_suffix)
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


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    return Application(conf)
