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

from collections import defaultdict

from random import shuffle
from time import time
import functools
import sys

from eventlet import Timeout

from swift import __canonical_version__ as swift_version
from swift.common import constraints
from swift.common.http import is_server_error, HTTP_INSUFFICIENT_STORAGE
from swift.common.storage_policy import POLICIES
from swift.common.ring import Ring
from swift.common.error_limiter import ErrorLimiter
from swift.common.utils import Watchdog, get_logger, \
    get_remote_client, split_path, config_true_value, generate_trans_id, \
    affinity_key_function, affinity_locality_predicate, list_from_csv, \
    parse_prefixed_conf, config_auto_int_value, node_to_string, \
    config_request_node_count_value, config_percent_value, cap_length, \
    parse_options
from swift.common.registry import register_swift_info
from swift.common.constraints import check_utf8, valid_api_version
from swift.proxy.controllers import AccountController, ContainerController, \
    ObjectControllerRouter, InfoController
from swift.proxy.controllers.base import get_container_info, \
    DEFAULT_RECHECK_CONTAINER_EXISTENCE, DEFAULT_RECHECK_ACCOUNT_EXISTENCE, \
    DEFAULT_RECHECK_UPDATING_SHARD_RANGES, DEFAULT_RECHECK_LISTING_SHARD_RANGES
from swift.common.swob import HTTPBadRequest, HTTPForbidden, \
    HTTPMethodNotAllowed, HTTPNotFound, HTTPPreconditionFailed, \
    HTTPServerError, HTTPException, Request, HTTPServiceUnavailable, \
    wsgi_to_str
from swift.common.exceptions import APIVersionError
from swift.common.wsgi import run_wsgi
from swift.obj import expirer


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
    {'name': 'listing_formats', 'after_fn': lambda _junk: [
        'catch_errors', 'gatekeeper', 'proxy_logging', 'memcache']},
    # Put copy before dlo, slo and versioned_writes
    {'name': 'copy', 'after_fn': lambda _junk: [
        'staticweb', 'tempauth', 'keystoneauth',
        'catch_errors', 'gatekeeper', 'proxy_logging']},
    {'name': 'dlo', 'after_fn': lambda _junk: [
        'copy', 'staticweb', 'tempauth', 'keystoneauth',
        'catch_errors', 'gatekeeper', 'proxy_logging']},
    {'name': 'versioned_writes', 'after_fn': lambda _junk: [
        'slo', 'dlo', 'copy', 'staticweb', 'tempauth',
        'keystoneauth', 'catch_errors', 'gatekeeper', 'proxy_logging']},
]


def _label_for_policy(policy):
    if policy is not None:
        return 'policy %s (%s)' % (policy.idx, policy.name)
    return '(default)'


VALID_SORTING_METHODS = ('shuffle', 'timing', 'affinity')


class ProxyOverrideOptions(object):
    """
    Encapsulates proxy server options that may be overridden e.g. for
    policy specific configurations.

    :param conf: the proxy-server config dict.
    :param override_conf: a dict of overriding configuration options.
    """
    def __init__(self, base_conf, override_conf, app):

        def get(key, default):
            return override_conf.get(key, base_conf.get(key, default))

        self.sorting_method = get('sorting_method', 'shuffle').lower()
        if self.sorting_method not in VALID_SORTING_METHODS:
            raise ValueError(
                'Invalid sorting_method value; must be one of %s, not %r' % (
                    ', '.join(VALID_SORTING_METHODS), self.sorting_method))

        self.read_affinity = get('read_affinity', '')
        try:
            self.read_affinity_sort_key = affinity_key_function(
                self.read_affinity)
        except ValueError as err:
            # make the message a little more useful
            raise ValueError("Invalid read_affinity value: %r (%s)" %
                             (self.read_affinity, err.args[0]))

        self.write_affinity = get('write_affinity', '')
        try:
            self.write_affinity_is_local_fn \
                = affinity_locality_predicate(self.write_affinity)
        except ValueError as err:
            # make the message a little more useful
            raise ValueError("Invalid write_affinity value: %r (%s)" %
                             (self.write_affinity, err.args[0]))
        self.write_affinity_node_count = get(
            'write_affinity_node_count', '2 * replicas').lower()
        value = self.write_affinity_node_count.split()
        if len(value) == 1:
            wanc_value = int(value[0])
            self.write_affinity_node_count_fn = lambda replicas: wanc_value
        elif len(value) == 3 and value[1] == '*' and value[2] == 'replicas':
            wanc_value = int(value[0])
            self.write_affinity_node_count_fn = \
                lambda replicas: wanc_value * replicas
        else:
            raise ValueError(
                'Invalid write_affinity_node_count value: %r' %
                (' '.join(value)))

        self.write_affinity_handoff_delete_count = config_auto_int_value(
            get('write_affinity_handoff_delete_count', 'auto'), None
        )

        self.rebalance_missing_suppression_count = int(get(
            'rebalance_missing_suppression_count', 1))
        self.concurrent_gets = config_true_value(get('concurrent_gets', False))
        self.concurrency_timeout = float(get(
            'concurrency_timeout', app.conn_timeout))
        self.concurrent_ec_extra_requests = int(get(
            'concurrent_ec_extra_requests', 0))

    def __repr__(self):
        return '%s({}, {%s}, app)' % (
            self.__class__.__name__, ', '.join(
                '%r: %r' % (k, getattr(self, k)) for k in (
                    'sorting_method',
                    'read_affinity',
                    'write_affinity',
                    'write_affinity_node_count',
                    'write_affinity_handoff_delete_count',
                    'rebalance_missing_suppression_count',
                    'concurrent_gets',
                    'concurrency_timeout',
                    'concurrent_ec_extra_requests',
                )))

    def __eq__(self, other):
        if not isinstance(other, ProxyOverrideOptions):
            return False
        return all(getattr(self, k) == getattr(other, k) for k in (
            'sorting_method',
            'read_affinity',
            'write_affinity',
            'write_affinity_node_count',
            'write_affinity_handoff_delete_count',
            'rebalance_missing_suppression_count',
            'concurrent_gets',
            'concurrency_timeout',
            'concurrent_ec_extra_requests',
        ))


class Application(object):
    """WSGI application for the proxy server."""

    def __init__(self, conf, logger=None, account_ring=None,
                 container_ring=None):
        # This is for the sake of tests which instantiate an Application
        # directly rather than via loadapp().
        self._pipeline_final_app = self

        if conf is None:
            conf = {}
        if logger is None:
            self.logger = get_logger(conf, log_route='proxy-server',
                                     statsd_tail_prefix='proxy-server')
        else:
            self.logger = logger
        self.backend_user_agent = 'proxy-server %s' % os.getpid()

        swift_dir = conf.get('swift_dir', '/etc/swift')
        self.swift_dir = swift_dir
        self.node_timeout = float(conf.get('node_timeout', 10))
        self.recoverable_node_timeout = float(
            conf.get('recoverable_node_timeout', self.node_timeout))
        self.conn_timeout = float(conf.get('conn_timeout', 0.5))
        self.client_timeout = float(conf.get('client_timeout', 60))
        self.object_chunk_size = int(conf.get('object_chunk_size', 65536))
        self.client_chunk_size = int(conf.get('client_chunk_size', 65536))
        self.trans_id_suffix = conf.get('trans_id_suffix', '')
        self.post_quorum_timeout = float(conf.get('post_quorum_timeout', 0.5))
        error_suppression_interval = \
            float(conf.get('error_suppression_interval', 60))
        error_suppression_limit = \
            int(conf.get('error_suppression_limit', 10))
        self.error_limiter = ErrorLimiter(error_suppression_interval,
                                          error_suppression_limit)
        self.recheck_container_existence = \
            int(conf.get('recheck_container_existence',
                         DEFAULT_RECHECK_CONTAINER_EXISTENCE))
        self.recheck_updating_shard_ranges = \
            int(conf.get('recheck_updating_shard_ranges',
                         DEFAULT_RECHECK_UPDATING_SHARD_RANGES))
        self.recheck_listing_shard_ranges = \
            int(conf.get('recheck_listing_shard_ranges',
                         DEFAULT_RECHECK_LISTING_SHARD_RANGES))
        self.recheck_account_existence = \
            int(conf.get('recheck_account_existence',
                         DEFAULT_RECHECK_ACCOUNT_EXISTENCE))
        self.container_existence_skip_cache = config_percent_value(
            conf.get('container_existence_skip_cache_pct', 0))
        self.container_updating_shard_ranges_skip_cache = \
            config_percent_value(conf.get(
                'container_updating_shard_ranges_skip_cache_pct', 0))
        self.container_listing_shard_ranges_skip_cache = \
            config_percent_value(conf.get(
                'container_listing_shard_ranges_skip_cache_pct', 0))
        self.account_existence_skip_cache = config_percent_value(
            conf.get('account_existence_skip_cache_pct', 0))
        self.allow_account_management = \
            config_true_value(conf.get('allow_account_management', 'no'))
        self.container_ring = container_ring or Ring(swift_dir,
                                                     ring_name='container')
        self.account_ring = account_ring or Ring(swift_dir,
                                                 ring_name='account')
        # ensure rings are loaded for all configured storage policies
        for policy in POLICIES:
            policy.load_ring(swift_dir)
        self.obj_controller_router = ObjectControllerRouter()
        mimetypes.init(mimetypes.knownfiles +
                       [os.path.join(swift_dir, 'mime.types')])
        self.account_autocreate = \
            config_true_value(conf.get('account_autocreate', 'no'))
        self.auto_create_account_prefix = \
            constraints.AUTO_CREATE_ACCOUNT_PREFIX
        self.expirer_config = expirer.ExpirerConfig(
            conf, container_ring=self.container_ring, logger=self.logger)
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
        self.cors_expose_headers = [
            a.strip()
            for a in conf.get('cors_expose_headers', '').split(',')
            if a.strip()]
        self.strict_cors_mode = config_true_value(
            conf.get('strict_cors_mode', 't'))
        self.allow_open_expired = config_true_value(
            conf.get('allow_open_expired', 'f'))
        self.node_timings = {}
        self.timing_expiry = int(conf.get('timing_expiry', 300))
        value = conf.get('request_node_count', '2 * replicas')
        self.request_node_count = config_request_node_count_value(value)
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

        # When upgrading from liberasurecode<=1.5.0, you may want to continue
        # writing legacy CRCs until all nodes are upgraded and capabale of
        # reading fragments with zlib CRCs.
        # See https://bugs.launchpad.net/liberasurecode/+bug/1886088 for more
        # information.
        if 'write_legacy_ec_crc' in conf:
            os.environ['LIBERASURECODE_WRITE_LEGACY_CRC'] = \
                '1' if config_true_value(conf['write_legacy_ec_crc']) else '0'
        # else, assume operators know what they're doing and leave env alone

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
        if sys.version_info < (3,):
            socket._fileobject.default_bufsize = self.client_chunk_size
        # TODO: find a way to enable similar functionality in py3

        self.expose_info = config_true_value(
            conf.get('expose_info', 'yes'))
        self.disallowed_sections = list_from_csv(
            conf.get('disallowed_sections', ', '.join([
                'swift.auto_create_account_prefix',
                'swift.valid_api_versions',
            ])))
        self.admin_key = conf.get('admin_key', None)
        self._override_options = self._load_per_policy_config(conf)
        self.sorts_by_timing = any(pc.sorting_method == 'timing'
                                   for pc in self._override_options.values())

        register_swift_info(
            version=swift_version,
            strict_cors_mode=self.strict_cors_mode,
            policies=POLICIES.get_policy_info(),
            allow_account_management=self.allow_account_management,
            account_autocreate=self.account_autocreate,
            allow_open_expired=self.allow_open_expired,
            **constraints.EFFECTIVE_CONSTRAINTS)
        self.watchdog = Watchdog()
        self.watchdog.spawn()

    def _make_policy_override(self, policy, conf, override_conf):
        label_for_policy = _label_for_policy(policy)
        try:
            override = ProxyOverrideOptions(conf, override_conf, self)
            self.logger.debug("Loaded override config for %s: %r" %
                              (label_for_policy, override))
            return override
        except ValueError as err:
            raise ValueError('%s for %s' % (err, label_for_policy))

    def _load_per_policy_config(self, conf):
        """
        Loads per-policy config override values from proxy server conf file.

        :param conf: the proxy server local conf dict
        :return: a dict mapping :class:`BaseStoragePolicy` to an instance of
            :class:`ProxyOverrideOptions` that has policy-specific config
            attributes
        """
        # the default options will be used when looking up a policy that had no
        # override options
        default_options = self._make_policy_override(None, conf, {})
        overrides = defaultdict(lambda: default_options)
        # force None key to be set in the defaultdict so that it is found when
        # iterating over items in check_config
        overrides[None] = default_options
        for index, override_conf in conf.get('policy_config', {}).items():
            try:
                index = int(index)
            except ValueError:
                # require policies to be referenced by index; using index *or*
                # name isn't possible because names such as "3" are allowed
                raise ValueError(
                    'Override config must refer to policy index: %r' % index)
            try:
                policy = POLICIES[index]
            except KeyError:
                raise ValueError(
                    "No policy found for override config, index: %s" % index)
            override = self._make_policy_override(policy, conf, override_conf)
            overrides[index] = override
        return overrides

    def get_policy_options(self, policy):
        """
        Return policy specific options.

        :param policy: an instance of :class:`BaseStoragePolicy` or ``None``
        :return: an instance of :class:`ProxyOverrideOptions`
        """
        return self._override_options[policy and policy.idx]

    def check_config(self):
        """
        Check the configuration for possible errors
        """
        for policy_idx, options in self._override_options.items():
            policy = (None if policy_idx is None
                      else POLICIES.get_by_index(policy_idx))
            if options.read_affinity and options.sorting_method != 'affinity':
                self.logger.warning(
                    "sorting_method is set to '%(method)s', not 'affinity'; "
                    "%(label)s read_affinity setting will have no effect.",
                    {'label': _label_for_policy(policy),
                     'method': options.sorting_method})

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

        :raises ValueError: (thrown by split_path) if given invalid path
        """
        if req.path == '/info':
            d = dict(version=None,
                     expose_info=self.expose_info,
                     disallowed_sections=self.disallowed_sections,
                     admin_key=self.admin_key)
            return InfoController, d

        version, account, container, obj = split_path(
            wsgi_to_str(req.path), 1, 4, True)
        d = dict(version=version,
                 account_name=account,
                 container_name=container,
                 object_name=obj)
        if account and not valid_api_version(version):
            raise APIVersionError('Invalid path')
        if obj and container and account:
            info = get_container_info(req.environ, self)
            if is_server_error(info.get('status')):
                raise HTTPServiceUnavailable(request=req)
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
            req = self.update_request(Request(env))
            return self.handle_request(req)(env, start_response)
        except UnicodeError:
            err = HTTPPreconditionFailed(
                request=req, body='Invalid UTF8 or contains NULL')
            return err(env, start_response)
        except (Exception, Timeout):
            start_response('500 Server Error',
                           [('Content-Type', 'text/plain')])
            return [b'Internal server error.\n']

    def update_request(self, req):
        if 'x-storage-token' in req.headers and \
                'x-auth-token' not in req.headers:
            req.headers['x-auth-token'] = req.headers['x-storage-token']
        te = req.headers.get('transfer-encoding', '').lower()
        if te.rsplit(',', 1)[-1].strip() == 'chunked' and \
                'content-length' in req.headers:
            # RFC says if both are present, transfer-encoding wins.
            # Definitely *don't* forward on the header the backend
            # ought to ignore; that offers request-smuggling vectors.
            del req.headers['content-length']
        return req

    def handle_request(self, req):
        """
        Entry point for proxy server.
        Should return a WSGI-style callable (such as swob.Response).

        :param req: swob.Request object
        """
        try:
            if req.content_length and req.content_length < 0:
                self.logger.increment('errors')
                return HTTPBadRequest(request=req,
                                      body='Invalid Content-Length')

            try:
                if not check_utf8(wsgi_to_str(req.path_info),
                                  internal=req.allow_reserved_names):
                    self.logger.increment('errors')
                    return HTTPPreconditionFailed(
                        request=req, body='Invalid UTF8 or contains NULL')
            except UnicodeError:
                self.logger.increment('errors')
                return HTTPPreconditionFailed(
                    request=req, body='Invalid UTF8 or contains NULL')

            try:
                controller, path_parts = self.get_controller(req)
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

            allowed_methods = controller.allowed_methods
            if config_true_value(req.headers.get(
                    'X-Backend-Allow-Private-Methods', False)):
                allowed_methods = set(allowed_methods).union(
                    controller.private_methods)
            if req.method not in allowed_methods:
                return HTTPMethodNotAllowed(request=req, headers={
                    'Allow': ', '.join(allowed_methods)})
            handler = getattr(controller, req.method)

            old_authorize = None
            if 'swift.authorize' in req.environ:
                # We call authorize before the handler, always. If authorized,
                # we remove the swift.authorize hook so isn't ever called
                # again. If not authorized, we return the denial unless the
                # controller's method indicates it'd like to gather more
                # information and try again later.
                resp = req.environ['swift.authorize'](req)
                if not resp:
                    # No resp means authorized, no delayed recheck required.
                    old_authorize = req.environ['swift.authorize']
                else:
                    # Response indicates denial, but we might delay the denial
                    # and recheck later. If not delayed, return the error now.
                    if not getattr(handler, 'delay_denial', None):
                        return resp
            # Save off original request method (GET, POST, etc.) in case it
            # gets mutated during handling.  This way logging can display the
            # method the client actually sent.
            req.environ.setdefault('swift.orig_req_method', req.method)
            try:
                if old_authorize:
                    req.environ.pop('swift.authorize', None)
                return handler(req)
            finally:
                if old_authorize:
                    req.environ['swift.authorize'] = old_authorize
        except HTTPException as error_response:
            return error_response
        except (Exception, Timeout):
            self.logger.exception('ERROR Unhandled exception in request')
            return HTTPServerError(request=req)

    def sort_nodes(self, nodes, policy=None):
        """
        Sorts nodes in-place (and returns the sorted list) according to
        the configured strategy. The default "sorting" is to randomly
        shuffle the nodes. If the "timing" strategy is chosen, the nodes
        are sorted according to the stored timing data.

        :param nodes: a list of nodes
        :param policy: an instance of :class:`BaseStoragePolicy`
        """
        # In the case of timing sorting, shuffling ensures that close timings
        # (ie within the rounding resolution) won't prefer one over another.
        # Python's sort is stable (http://wiki.python.org/moin/HowTo/Sorting/)
        shuffle(nodes)
        policy_options = self.get_policy_options(policy)
        if policy_options.sorting_method == 'timing':
            now = time()

            def key_func(node):
                timing, expires = self.node_timings.get(node['ip'], (-1.0, 0))
                return timing if expires > now else -1.0
            nodes.sort(key=key_func)
        elif policy_options.sorting_method == 'affinity':
            nodes.sort(key=policy_options.read_affinity_sort_key)
        return nodes

    def set_node_timing(self, node, timing):
        if not self.sorts_by_timing:
            return
        now = time()
        timing = round(timing, 3)  # sort timings to the millisecond
        self.node_timings[node['ip']] = (timing, now + self.timing_expiry)

    def error_limited(self, node):
        """
        Check if the node is currently error limited.

        :param node: dictionary of node to check
        :returns: True if error limited, False otherwise
        """
        limited = self.error_limiter.is_limited(node)
        if limited:
            self.logger.increment('error_limiter.is_limited')
            self.logger.debug(
                'Node is error limited: %s', node_to_string(node))
        return limited

    def error_limit(self, node, msg):
        """
        Mark a node as error limited. This immediately pretends the
        node received enough errors to trigger error suppression. Use
        this for errors like Insufficient Storage. For other errors
        use :func:`increment`.

        :param node: dictionary of node to error limit
        :param msg: error message
        """
        self.error_limiter.limit(node)
        self.logger.increment('error_limiter.forced_limit')
        self.logger.error(
            'Node will be error limited for %.2fs: %s, error: %s',
            self.error_limiter.suppression_interval, node_to_string(node),
            msg)

    def _error_increment(self, node):
        """
        Call increment() on error limiter once, emit metrics and log if error
        suppression will be triggered.

        :param node: dictionary of node to handle errors for
        """
        if self.error_limiter.increment(node):
            self.logger.increment('error_limiter.incremented_limit')
            self.logger.error(
                'Node will be error limited for %.2fs: %s',
                self.error_limiter.suppression_interval, node_to_string(node))

    def error_occurred(self, node, msg):
        """
        Handle logging, and handling of errors.

        :param node: dictionary of node to handle errors for
        :param msg: error message
        """
        if isinstance(msg, bytes):
            msg = msg.decode('utf-8')
        self.logger.error('%(msg)s %(node)s',
                          {'msg': msg, 'node': node_to_string(node)})
        self._error_increment(node)

    def check_response(self, node, server_type, response, method, path,
                       body=None):
        """
        Check response for error status codes and update error limiters as
        required.

        :param node: a dict describing a node
        :param server_type: the type of server from which the response was
            received (e.g. 'Object').
        :param response: an instance of HTTPResponse.
        :param method: the request method.
        :param path: the request path.
        :param body: an optional response body. If given, up to 1024 of the
            start of the body will be included in any log message.
        :return True: if the response status code is less than 500, False
            otherwise.
        """
        ok = False
        if response.status == HTTP_INSUFFICIENT_STORAGE:
            self.error_limit(node, 'ERROR Insufficient Storage')
        elif is_server_error(response.status):
            values = {'status': response.status,
                      'method': method,
                      'path': path,
                      'type': server_type}
            if body is None:
                fmt = 'ERROR %(status)d Trying to %(method)s ' \
                      '%(path)s From %(type)s Server'
            else:
                fmt = 'ERROR %(status)d %(body)s Trying to %(method)s ' \
                      '%(path)s From %(type)s Server'
                values['body'] = cap_length(body, 1024)
            self.error_occurred(node, fmt % values)
        else:
            ok = True

        return ok

    def exception_occurred(self, node, typ, additional_info,
                           **kwargs):
        """
        Handle logging of generic exceptions.

        :param node: dictionary of node to log the error for
        :param typ: server type
        :param additional_info: additional information to log
        """
        if 'level' in kwargs:
            log = functools.partial(self.logger.log, kwargs.pop('level'))
            if 'exc_info' not in kwargs:
                kwargs['exc_info'] = sys.exc_info()
        else:
            log = self.logger.exception
        if isinstance(additional_info, bytes):
            additional_info = additional_info.decode('utf-8')
        log('ERROR with %(type)s server %(node)s'
            ' re: %(info)s',
            {'type': typ, 'node': node_to_string(node),
             'info': additional_info},
            **kwargs)
        self._error_increment(node)

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
                    'Adding required filter %(filter_name)s to pipeline at '
                    'position %(insert_at)d',
                    {'filter_name': filter_name, 'insert_at': insert_at})
                ctx = pipe.create_filter(filter_name)
                pipe.insert_filter(ctx, index=insert_at)
                pipeline_was_modified = True

        if pipeline_was_modified:
            self.logger.info("Pipeline was modified. "
                             "New pipeline is \"%s\".", pipe)
        else:
            self.logger.debug("Pipeline is \"%s\"", pipe)


def parse_per_policy_config(conf):
    """
    Search the config file for any per-policy config sections and load those
    sections to a dict mapping policy reference (name or index) to policy
    options.

    :param conf: the proxy server conf dict
    :return: a dict mapping policy reference -> dict of policy options
    :raises ValueError: if a policy config section has an invalid name
    """
    policy_section_prefix = conf['__name__'] + ':policy:'
    return parse_prefixed_conf(conf['__file__'], policy_section_prefix)


def app_factory(global_conf, **local_conf):
    """paste.deploy app factory for creating WSGI proxy apps."""
    conf = global_conf.copy()
    conf.update(local_conf)
    # Do this here so that the use of conf['__file__'] and conf['__name__'] is
    # isolated from the Application. This also simplifies tests that construct
    # an Application instance directly.
    conf['policy_config'] = parse_per_policy_config(conf)
    app = Application(conf)
    app.check_config()
    return app


def main():
    conf_file, options = parse_options(test_config=True)
    sys.exit(run_wsgi(conf_file, 'proxy-server', **options))


if __name__ == '__main__':
    main()
