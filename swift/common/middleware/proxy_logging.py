# Copyright (c) 2010-2011 OpenStack, LLC.
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

The logging format implemented below is as follows:

client_ip remote_addr datetime request_method request_path protocol
    status_int referer user_agent auth_token bytes_recvd bytes_sent
    client_etag transaction_id headers request_time source

These values are space-separated, and each is url-encoded, so that they can
be separated with a simple .split()

* remote_addr is the contents of the REMOTE_ADDR environment variable, while
  client_ip is swift's best guess at the end-user IP, extracted variously
  from the X-Forwarded-For header, X-Cluster-Ip header, or the REMOTE_ADDR
  environment variable.

* Values that are missing (e.g. due to a header not being present) or zero
  are generally represented by a single hyphen ('-').
"""

import time
from urllib import quote, unquote

from swift.common.swob import Request
from swift.common.utils import (get_logger, get_remote_client,
                                get_valid_utf8_str, config_true_value,
                                InputProxy)


class ProxyLoggingMiddleware(object):
    """
    Middleware that logs Swift proxy requests in the swift log format.
    """

    def __init__(self, app, conf):
        self.app = app
        self.log_hdrs = config_true_value(conf.get(
            'access_log_headers',
            conf.get('log_headers', 'no')))

        # The leading access_* check is in case someone assumes that
        # log_statsd_valid_http_methods behaves like the other log_statsd_*
        # settings.
        self.valid_methods = conf.get(
            'access_log_statsd_valid_http_methods',
            conf.get('log_statsd_valid_http_methods',
                     'GET,HEAD,POST,PUT,DELETE,COPY,OPTIONS'))
        self.valid_methods = [m.strip().upper() for m in
                              self.valid_methods.split(',') if m.strip()]
        access_log_conf = {}
        for key in ('log_facility', 'log_name', 'log_level', 'log_udp_host',
                    'log_udp_port', 'log_statsd_host', 'log_statsd_port',
                    'log_statsd_default_sample_rate',
                    'log_statsd_metric_prefix'):
            value = conf.get('access_' + key, conf.get(key, None))
            if value:
                access_log_conf[key] = value
        self.access_logger = get_logger(access_log_conf,
                                        log_route='proxy-access')
        self.access_logger.set_statsd_prefix('proxy-server')

    def log_request(self, env, status_int, bytes_received, bytes_sent,
                    request_time, client_disconnect):
        """
        Log a request.

        :param env: WSGI environment
        :param status_int: integer code for the response status
        :param bytes_received: bytes successfully read from the request body
        :param bytes_sent: bytes yielded to the WSGI server
        :param request_time: time taken to satisfy the request, in seconds
        """
        req = Request(env)
        if client_disconnect:  # log disconnected clients as '499' status code
            status_int = 499
        req_path = get_valid_utf8_str(req.path)
        the_request = quote(unquote(req_path))
        if req.query_string:
            the_request = the_request + '?' + req.query_string
        logged_headers = None
        if self.log_hdrs:
            logged_headers = '\n'.join('%s: %s' % (k, v)
                                       for k, v in req.headers.items())
        method = req.environ.get('swift.orig_req_method', req.method)
        self.access_logger.info(' '.join(
            quote(str(x) if x else '-')
            for x in (
                get_remote_client(req),
                req.remote_addr,
                time.strftime('%d/%b/%Y/%H/%M/%S', time.gmtime()),
                method,
                the_request,
                req.environ.get('SERVER_PROTOCOL'),
                status_int,
                req.referer,
                req.user_agent,
                req.headers.get('x-auth-token'),
                bytes_received,
                bytes_sent,
                req.headers.get('etag', None),
                req.environ.get('swift.trans_id'),
                logged_headers,
                '%.4f' % request_time,
                req.environ.get('swift.source'),
            )))
        # Log timing and bytes-transfered data to StatsD
        if req.path.startswith('/v1/'):
            try:
                stat_type = [None, 'account', 'container',
                             'object'][req.path.strip('/').count('/')]
            except IndexError:
                stat_type = 'object'
        else:
            stat_type = env.get('swift.source')
        # Only log data for valid controllers (or SOS) to keep the metric count
        # down (egregious errors will get logged by the proxy server itself).
        if stat_type:
            stat_method = method if method in self.valid_methods \
                else 'BAD_METHOD'
            metric_name = '.'.join((stat_type, stat_method, str(status_int)))
            self.access_logger.timing(metric_name + '.timing',
                                      request_time * 1000)
            self.access_logger.update_stats(metric_name + '.xfer',
                                            bytes_received + bytes_sent)

    def __call__(self, env, start_response):
        start_response_args = [None]
        input_proxy = InputProxy(env['wsgi.input'])
        env['wsgi.input'] = input_proxy
        start_time = time.time()

        def my_start_response(status, headers, exc_info=None):
            start_response_args[0] = (status, list(headers), exc_info)

        def iter_response(iterable):
            iterator = iter(iterable)
            try:
                chunk = iterator.next()
                while not chunk:
                    chunk = iterator.next()
            except StopIteration:
                chunk = ''
            for h, v in start_response_args[0][1]:
                if h.lower() in ('content-length', 'transfer-encoding'):
                    break
            else:
                if not chunk:
                    start_response_args[0][1].append(('content-length', '0'))
                elif isinstance(iterable, list):
                    start_response_args[0][1].append(
                        ('content-length', str(sum(len(i) for i in iterable))))
            start_response(*start_response_args[0])
            bytes_sent = 0
            client_disconnect = False
            try:
                while chunk:
                    bytes_sent += len(chunk)
                    yield chunk
                    chunk = iterator.next()
            except GeneratorExit:  # generator was closed before we finished
                client_disconnect = True
                raise
            finally:
                status_int = int(start_response_args[0][0].split(' ', 1)[0])
                self.log_request(
                    env, status_int, input_proxy.bytes_received, bytes_sent,
                    time.time() - start_time,
                    client_disconnect or input_proxy.client_disconnect)

        try:
            iterable = self.app(env, my_start_response)
        except Exception:
            self.log_request(
                env, 500, input_proxy.bytes_received, 0,
                time.time() - start_time, input_proxy.client_disconnect)
            raise
        else:
            return iter_response(iterable)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def proxy_logger(app):
        return ProxyLoggingMiddleware(app, conf)
    return proxy_logger
