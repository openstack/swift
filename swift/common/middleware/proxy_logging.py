# Copyright (c) 2010-2011 OpenStack Foundation
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

"""
Logging middleware for the Swift proxy.

This serves as both the default logging implementation and an example of how
to plug in your own logging format/method.

The logging format implemented below is as follows::

    client_ip remote_addr end_time.datetime method path protocol
        status_int referer user_agent auth_token bytes_recvd bytes_sent
        client_etag transaction_id headers request_time source log_info
        start_time end_time policy_index

These values are space-separated, and each is url-encoded, so that they can
be separated with a simple ``.split()``.

* ``remote_addr`` is the contents of the REMOTE_ADDR environment variable,
  while ``client_ip`` is swift's best guess at the end-user IP, extracted
  variously from the X-Forwarded-For header, X-Cluster-Ip header, or the
  REMOTE_ADDR environment variable.

* ``status_int`` is the integer part of the ``status`` string passed to this
  middleware's start_response function, unless the WSGI environment has an item
  with key ``swift.proxy_logging_status``, in which case the value of that item
  is used. Other middleware's may set ``swift.proxy_logging_status`` to
  override the logging of ``status_int``. In either case, the logged
  ``status_int`` value is forced to 499 if a client disconnect is detected
  while this middleware is handling a request, or 500 if an exception is caught
  while handling a request.

* ``source`` (``swift.source`` in the WSGI environment) indicates the code
  that generated the request, such as most middleware. (See below for
  more detail.)

* ``log_info`` (``swift.log_info`` in the WSGI environment) is for additional
  information that could prove quite useful, such as any ``x-delete-at``
  value or other "behind the scenes" activity that might not
  otherwise be detectable from the plain log information. Code that
  wishes to add additional log information should use code like
  ``env.setdefault('swift.log_info', []).append(your_info)`` so as to
  not disturb others' log information.

* Values that are missing (e.g. due to a header not being present) or zero
  are generally represented by a single hyphen ('-').

.. note::
   The message format may be configured using the ``log_msg_template`` option,
   allowing fields to be added, removed, re-ordered, and even anonymized. For
   more information, see https://docs.openstack.org/swift/latest/logs.html

The proxy-logging can be used twice in the proxy server's pipeline when there
is middleware installed that can return custom responses that don't follow the
standard pipeline to the proxy server.

For example, with staticweb, the middleware might intercept a request to
/v1/AUTH_acc/cont/, make a subrequest to the proxy to retrieve
/v1/AUTH_acc/cont/index.html and, in effect, respond to the client's original
request using the 2nd request's body. In this instance the subrequest will be
logged by the rightmost middleware (with a ``swift.source`` set) and the
outgoing request (with body overridden) will be logged by leftmost middleware.

Requests that follow the normal pipeline (use the same wsgi environment
throughout) will not be double logged because an environment variable
(``swift.proxy_access_log_made``) is checked/set when a log is made.

All middleware making subrequests should take care to set ``swift.source`` when
needed. With the doubled proxy logs, any consumer/processor of swift's proxy
logs should look at the ``swift.source`` field, the rightmost log value, to
decide if this is a middleware subrequest or not. A log processor calculating
bandwidth usage will want to only sum up logs with no ``swift.source``.
"""

import os
import time

from swift.common.constraints import valid_api_version
from swift.common.middleware.catch_errors import enforce_byte_count
from swift.common.request_helpers import get_log_info
from swift.common.swob import Request
from swift.common.utils import (get_logger, get_remote_client,
                                config_true_value, reiterate,
                                close_if_possible, cap_length,
                                InputProxy, list_from_csv, get_policy_index,
                                split_path, StrAnonymizer, StrFormatTime,
                                LogStringFormatter)

from swift.common.storage_policy import POLICIES
from swift.common.registry import get_sensitive_headers, \
    get_sensitive_params, register_sensitive_header


class ProxyLoggingMiddleware(object):
    """
    Middleware that logs Swift proxy requests in the swift log format.
    """

    def __init__(self, app, conf, logger=None):
        self.app = app
        self.pid = os.getpid()
        self.log_formatter = LogStringFormatter(default='-', quote=True)
        self.log_msg_template = conf.get(
            'log_msg_template', (
                '{client_ip} {remote_addr} {end_time.datetime} {method} '
                '{path} {protocol} {status_int} {referer} {user_agent} '
                '{auth_token} {bytes_recvd} {bytes_sent} {client_etag} '
                '{transaction_id} {headers} {request_time} {source} '
                '{log_info} {start_time} {end_time} {policy_index}'))
        # The salt is only used in StrAnonymizer. This class requires bytes,
        # convert it now to prevent useless convertion later.
        self.anonymization_method = conf.get('log_anonymization_method', 'md5')
        self.anonymization_salt = conf.get('log_anonymization_salt', '')
        self.log_hdrs = config_true_value(conf.get(
            'access_log_headers',
            conf.get('log_headers', 'no')))
        log_hdrs_only = list_from_csv(conf.get(
            'access_log_headers_only', ''))
        self.log_hdrs_only = [x.title() for x in log_hdrs_only]

        # The leading access_* check is in case someone assumes that
        # log_statsd_valid_http_methods behaves like the other log_statsd_*
        # settings.
        self.valid_methods = conf.get(
            'access_log_statsd_valid_http_methods',
            conf.get('log_statsd_valid_http_methods',
                     'GET,HEAD,POST,PUT,DELETE,COPY,OPTIONS,UPDATE'))
        self.valid_methods = [m.strip().upper() for m in
                              self.valid_methods.split(',') if m.strip()]
        access_log_conf = {}
        for key in ('log_facility', 'log_name', 'log_level', 'log_udp_host',
                    'log_udp_port', 'log_statsd_host', 'log_statsd_port',
                    'log_statsd_default_sample_rate',
                    'log_statsd_sample_rate_factor',
                    'log_statsd_metric_prefix'):
            value = conf.get('access_' + key, conf.get(key, None))
            if value:
                access_log_conf[key] = value
        self.access_logger = logger or get_logger(
            access_log_conf,
            log_route=conf.get('access_log_route', 'proxy-access'),
            statsd_tail_prefix='proxy-server')
        self.reveal_sensitive_prefix = int(
            conf.get('reveal_sensitive_prefix', 16))
        self.check_log_msg_template_validity()

    def check_log_msg_template_validity(self):
        replacements = {
            # Time information
            'end_time': StrFormatTime(1000001),
            'start_time': StrFormatTime(1000000),
            # Information worth to anonymize
            'client_ip': StrAnonymizer('1.2.3.4', self.anonymization_method,
                                       self.anonymization_salt),
            'remote_addr': StrAnonymizer('4.3.2.1', self.anonymization_method,
                                         self.anonymization_salt),
            'domain': StrAnonymizer('', self.anonymization_method,
                                    self.anonymization_salt),
            'path': StrAnonymizer('/', self.anonymization_method,
                                  self.anonymization_salt),
            'referer': StrAnonymizer('ref', self.anonymization_method,
                                     self.anonymization_salt),
            'user_agent': StrAnonymizer('swift', self.anonymization_method,
                                        self.anonymization_salt),
            'headers': StrAnonymizer('header', self.anonymization_method,
                                     self.anonymization_salt),
            'client_etag': StrAnonymizer('etag', self.anonymization_method,
                                         self.anonymization_salt),
            'account': StrAnonymizer('a', self.anonymization_method,
                                     self.anonymization_salt),
            'container': StrAnonymizer('c', self.anonymization_method,
                                       self.anonymization_salt),
            'object': StrAnonymizer('', self.anonymization_method,
                                    self.anonymization_salt),
            # Others information
            'method': 'GET',
            'protocol': '',
            'status_int': '0',
            'auth_token': '1234...',
            'bytes_recvd': '1',
            'bytes_sent': '0',
            'transaction_id': 'tx1234',
            'request_time': '0.05',
            'source': '',
            'log_info': '',
            'policy_index': '',
            'ttfb': '0.05',
            'pid': '42',
            'wire_status_int': '200',
        }
        try:
            self.log_formatter.format(self.log_msg_template, **replacements)
        except Exception as e:
            raise ValueError('Cannot interpolate log_msg_template: %s' % e)

    def method_from_req(self, req):
        return req.environ.get('swift.orig_req_method', req.method)

    def req_already_logged(self, env):
        return env.get('swift.proxy_access_log_made')

    def mark_req_logged(self, env):
        env['swift.proxy_access_log_made'] = True

    def obscure_sensitive(self, value):
        return cap_length(value, self.reveal_sensitive_prefix)

    def obscure_req(self, req):
        for header in get_sensitive_headers():
            if header in req.headers:
                req.headers[header] = \
                    self.obscure_sensitive(req.headers[header])

        obscure_params = get_sensitive_params()
        new_params = []
        any_obscured = False
        for k, v in req.params.items():
            if k in obscure_params:
                new_params.append((k, self.obscure_sensitive(v)))
                any_obscured = True
            else:
                new_params.append((k, v))
        if any_obscured:
            req.params = new_params

    def log_request(self, req, status_int, bytes_received, bytes_sent,
                    start_time, end_time, resp_headers=None, ttfb=0,
                    wire_status_int=None):
        """
        Log a request.

        :param req: swob.Request object for the request
        :param status_int: integer code for the response status
        :param bytes_received: bytes successfully read from the request body
        :param bytes_sent: bytes yielded to the WSGI server
        :param start_time: timestamp request started
        :param end_time: timestamp request completed
        :param resp_headers: dict of the response headers
        :param ttfb: time to first byte
        :param wire_status_int: the on the wire status int
        """
        self.obscure_req(req)
        domain = req.environ.get('HTTP_HOST',
                                 req.environ.get('SERVER_NAME', None))
        if ':' in domain:
            domain, port = domain.rsplit(':', 1)
        resp_headers = resp_headers or {}
        logged_headers = None
        if self.log_hdrs:
            if self.log_hdrs_only:
                logged_headers = '\n'.join('%s: %s' % (k, v)
                                           for k, v in req.headers.items()
                                           if k in self.log_hdrs_only)
            else:
                logged_headers = '\n'.join('%s: %s' % (k, v)
                                           for k, v in req.headers.items())

        method = self.method_from_req(req)
        duration_time_str = "%.4f" % (end_time - start_time)
        policy_index = get_policy_index(req.headers, resp_headers)

        swift_path = req.environ.get('swift.backend_path', req.path)
        acc, cont, obj = self.get_aco_from_path(swift_path)

        replacements = {
            # Time information
            'end_time': StrFormatTime(end_time),
            'start_time': StrFormatTime(start_time),
            # Information worth to anonymize
            'client_ip': StrAnonymizer(get_remote_client(req),
                                       self.anonymization_method,
                                       self.anonymization_salt),
            'remote_addr': StrAnonymizer(req.remote_addr,
                                         self.anonymization_method,
                                         self.anonymization_salt),
            'domain': StrAnonymizer(domain, self.anonymization_method,
                                    self.anonymization_salt),
            'path': StrAnonymizer(req.path_qs, self.anonymization_method,
                                  self.anonymization_salt),
            'referer': StrAnonymizer(req.referer, self.anonymization_method,
                                     self.anonymization_salt),
            'user_agent': StrAnonymizer(req.user_agent,
                                        self.anonymization_method,
                                        self.anonymization_salt),
            'headers': StrAnonymizer(logged_headers, self.anonymization_method,
                                     self.anonymization_salt),
            'client_etag': StrAnonymizer(req.headers.get('etag'),
                                         self.anonymization_method,
                                         self.anonymization_salt),
            'account': StrAnonymizer(acc, self.anonymization_method,
                                     self.anonymization_salt),
            'container': StrAnonymizer(cont, self.anonymization_method,
                                       self.anonymization_salt),
            'object': StrAnonymizer(obj, self.anonymization_method,
                                    self.anonymization_salt),
            # Others information
            'method': method,
            'protocol':
                req.environ.get('SERVER_PROTOCOL'),
            'status_int': status_int,
            'auth_token':
                req.headers.get('x-auth-token'),
            'bytes_recvd': bytes_received,
            'bytes_sent': bytes_sent,
            'transaction_id': req.environ.get('swift.trans_id'),
            'request_time': duration_time_str,
            'source': req.environ.get('swift.source'),
            'log_info': get_log_info(req.environ),
            'policy_index': policy_index,
            'ttfb': ttfb,
            'pid': self.pid,
            'wire_status_int': wire_status_int or status_int,
        }
        self.access_logger.info(
            self.log_formatter.format(self.log_msg_template,
                                      **replacements))

        # Log timing and bytes-transferred data to StatsD
        metric_name = self.statsd_metric_name(req, status_int, method)
        metric_name_policy = self.statsd_metric_name_policy(req, status_int,
                                                            method,
                                                            policy_index)
        # Only log data for valid controllers (or SOS) to keep the metric count
        # down (egregious errors will get logged by the proxy server itself).

        if metric_name:
            self.access_logger.timing(metric_name + '.timing',
                                      (end_time - start_time) * 1000)
            self.access_logger.update_stats(metric_name + '.xfer',
                                            bytes_received + bytes_sent)
        if metric_name_policy:
            self.access_logger.timing(metric_name_policy + '.timing',
                                      (end_time - start_time) * 1000)
            self.access_logger.update_stats(metric_name_policy + '.xfer',
                                            bytes_received + bytes_sent)

    def get_aco_from_path(self, swift_path):
        try:
            version, acc, cont, obj = split_path(swift_path, 1, 4, True)
            if not valid_api_version(version):
                raise ValueError
        except ValueError:
            acc, cont, obj = None, None, None
        return acc, cont, obj

    def get_metric_name_type(self, req):
        swift_path = req.environ.get('swift.backend_path', req.path)
        acc, cont, obj = self.get_aco_from_path(swift_path)
        if obj:
            return 'object'
        if cont:
            return 'container'
        if acc:
            return 'account'
        return req.environ.get('swift.source') or 'UNKNOWN'

    def statsd_metric_name(self, req, status_int, method):
        stat_type = self.get_metric_name_type(req)
        stat_method = method if method in self.valid_methods \
            else 'BAD_METHOD'
        return '.'.join((stat_type, stat_method, str(status_int)))

    def statsd_metric_name_policy(self, req, status_int, method, policy_index):
        if policy_index is None:
            return None
        stat_type = self.get_metric_name_type(req)
        if stat_type == 'object':
            stat_method = method if method in self.valid_methods \
                else 'BAD_METHOD'
            # The policy may not exist
            policy = POLICIES.get_by_index(policy_index)
            if policy:
                return '.'.join((stat_type, 'policy', str(policy_index),
                                 stat_method, str(status_int)))
            else:
                return None
        else:
            return None

    def __call__(self, env, start_response):
        if self.req_already_logged(env):
            return self.app(env, start_response)

        self.mark_req_logged(env)

        start_response_args = [None]
        input_proxy = InputProxy(env['wsgi.input'])
        env['wsgi.input'] = input_proxy
        start_time = time.time()

        def my_start_response(status, headers, exc_info=None):
            start_response_args[0] = (status, list(headers), exc_info)

        def status_int_for_logging():
            # log disconnected clients as '499' status code
            if input_proxy.client_disconnect:
                return 499
            return env.get('swift.proxy_logging_status')

        def iter_response(iterable):
            iterator = reiterate(iterable)
            content_length = None
            for h, v in start_response_args[0][1]:
                if h.lower() == 'content-length':
                    content_length = int(v)
                    break
                elif h.lower() == 'transfer-encoding':
                    break
            else:
                if isinstance(iterator, list):
                    content_length = sum(len(i) for i in iterator)
                    start_response_args[0][1].append(
                        ('Content-Length', str(content_length)))

            req = Request(env)
            method = self.method_from_req(req)
            if method == 'HEAD':
                content_length = 0
            if content_length is not None:
                iterator = enforce_byte_count(iterator, content_length)

            wire_status_int = int(start_response_args[0][0].split(' ', 1)[0])
            resp_headers = dict(start_response_args[0][1])
            start_response(*start_response_args[0])

            # Log timing information for time-to-first-byte (GET requests only)
            ttfb = 0.0
            if method == 'GET':
                policy_index = get_policy_index(req.headers, resp_headers)
                metric_name = self.statsd_metric_name(
                    req, wire_status_int, method)
                metric_name_policy = self.statsd_metric_name_policy(
                    req, wire_status_int, method, policy_index)
                ttfb = time.time() - start_time
                if metric_name:
                    self.access_logger.timing(
                        metric_name + '.first-byte.timing', ttfb * 1000)
                if metric_name_policy:
                    self.access_logger.timing(
                        metric_name_policy + '.first-byte.timing', ttfb * 1000)

            bytes_sent = 0
            try:
                for chunk in iterator:
                    bytes_sent += len(chunk)
                    yield chunk
            except GeneratorExit:  # generator was closed before we finished
                env['swift.proxy_logging_status'] = 499
                raise
            except Exception:
                env['swift.proxy_logging_status'] = 500
                raise
            finally:
                env.setdefault('swift.proxy_logging_status', wire_status_int)
                status_int = status_int_for_logging()
                self.log_request(
                    req, status_int, input_proxy.bytes_received, bytes_sent,
                    start_time, time.time(), resp_headers=resp_headers,
                    ttfb=ttfb, wire_status_int=wire_status_int)
                close_if_possible(iterator)

        try:
            iterable = self.app(env, my_start_response)
        except Exception:
            req = Request(env)
            env['swift.proxy_logging_status'] = 500
            status_int = status_int_for_logging()
            self.log_request(
                req, status_int, input_proxy.bytes_received, 0, start_time,
                time.time())
            raise
        else:
            return iter_response(iterable)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    # Normally it would be the middleware that uses the header that
    # would register it, but because there could be 3rd party auth middlewares
    # that use 'x-auth-token' or 'x-storage-token' we special case it here.
    register_sensitive_header('x-auth-token')
    register_sensitive_header('x-storage-token')

    def proxy_logger(app):
        return ProxyLoggingMiddleware(app, conf)
    return proxy_logger
