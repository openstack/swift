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

""" Statsd Client """

import time
import warnings
from contextlib import closing
from random import random

from eventlet.green import socket


def get_statsd_client(conf=None, tail_prefix='', logger=None):
    """
    Get an instance of StatsdClient using config settings.

    **config and defaults**::

        log_statsd_host = (disabled)
        log_statsd_port = 8125
        log_statsd_default_sample_rate = 1.0
        log_statsd_sample_rate_factor = 1.0
        log_statsd_metric_prefix = (empty-string)

    :param conf: Configuration dict to read settings from
    :param tail_prefix: tail prefix to pass to statsd client
    :param logger: stdlib logger instance used by statsd client for logging
    :return: an instance of ``StatsdClient``

    """
    conf = conf or {}

    host = conf.get('log_statsd_host')
    port = int(conf.get('log_statsd_port', 8125))
    base_prefix = conf.get('log_statsd_metric_prefix', '')
    default_sample_rate = float(
        conf.get('log_statsd_default_sample_rate', 1))
    sample_rate_factor = float(
        conf.get('log_statsd_sample_rate_factor', 1))

    return StatsdClient(host, port, base_prefix=base_prefix,
                        tail_prefix=tail_prefix,
                        default_sample_rate=default_sample_rate,
                        sample_rate_factor=sample_rate_factor, logger=logger)


class StatsdClient(object):
    def __init__(self, host, port, base_prefix='', tail_prefix='',
                 default_sample_rate=1, sample_rate_factor=1, logger=None):
        self._host = host
        self._port = port
        self._base_prefix = base_prefix
        self._default_sample_rate = default_sample_rate
        self._sample_rate_factor = sample_rate_factor
        self.random = random
        self.logger = logger
        self._set_prefix(tail_prefix)
        self._sock_family = self._target = None

        if self._host:
            self._set_sock_family_and_target(self._host, self._port)

    def _set_sock_family_and_target(self, host, port):
        # Determine if host is IPv4 or IPv6
        addr_info = None
        try:
            addr_info = socket.getaddrinfo(host, port, socket.AF_INET)
            self._sock_family = socket.AF_INET
        except socket.gaierror:
            try:
                addr_info = socket.getaddrinfo(host, port, socket.AF_INET6)
                self._sock_family = socket.AF_INET6
            except socket.gaierror:
                # Don't keep the server from starting from what could be a
                # transient DNS failure.  Any hostname will get re-resolved as
                # necessary in the .sendto() calls.
                # However, we don't know if we're IPv4 or IPv6 in this case, so
                # we assume legacy IPv4.
                self._sock_family = socket.AF_INET

        # NOTE: we use the original host value, not the DNS-resolved one
        # because if host is a hostname, we don't want to cache the DNS
        # resolution for the entire lifetime of this process.  Let standard
        # name resolution caching take effect.  This should help operators use
        # DNS trickery if they want.
        if addr_info is not None:
            # addr_info is a list of 5-tuples with the following structure:
            #     (family, socktype, proto, canonname, sockaddr)
            # where sockaddr is the only thing of interest to us, and we only
            # use the first result.  We want to use the originally supplied
            # host (see note above) and the remainder of the variable-length
            # sockaddr: IPv4 has (address, port) while IPv6 has (address,
            # port, flow info, scope id).
            sockaddr = addr_info[0][-1]
            self._target = (host,) + (sockaddr[1:])
        else:
            self._target = (host, port)

    def _set_prefix(self, tail_prefix):
        """
        Modifies the prefix that is added to metric names. The resulting prefix
        is the concatenation of the component parts `base_prefix` and
        `tail_prefix`. Only truthy components are included. Each included
        component is followed by a period, e.g.::

            <base_prefix>.<tail_prefix>.
            <tail_prefix>.
            <base_prefix>.
            <the empty string>

        Note: this method is expected to be called from the constructor only,
        but exists to provide backwards compatible functionality for the
        deprecated set_prefix() method.

        :param tail_prefix: The new value of tail_prefix
        """
        if tail_prefix and self._base_prefix:
            self._prefix = '.'.join([self._base_prefix, tail_prefix, ''])
        elif tail_prefix:
            self._prefix = tail_prefix + '.'
        elif self._base_prefix:
            self._prefix = self._base_prefix + '.'
        else:
            self._prefix = ''

    def set_prefix(self, tail_prefix):
        """
        This method is deprecated; use the ``tail_prefix`` argument of the
        constructor when instantiating the class instead.
        """
        warnings.warn(
            'set_prefix() is deprecated; use the ``tail_prefix`` argument of '
            'the constructor when instantiating the class instead.',
            DeprecationWarning, stacklevel=2
        )
        self._set_prefix(tail_prefix)

    def _send(self, m_name, m_value, m_type, sample_rate):
        if not self._host:
            # StatsD not configured
            return

        if sample_rate is None:
            sample_rate = self._default_sample_rate
        sample_rate = sample_rate * self._sample_rate_factor

        parts = ['%s%s:%s' % (self._prefix, m_name, m_value), m_type]
        if sample_rate < 1:
            if self.random() < sample_rate:
                parts.append('@%s' % (sample_rate,))
            else:
                return
        parts = [part.encode('utf-8') for part in parts]
        # Ideally, we'd cache a sending socket in self, but that
        # results in a socket getting shared by multiple green threads.
        with closing(self._open_socket()) as sock:
            try:
                return sock.sendto(b'|'.join(parts), self._target)
            except IOError as err:
                if self.logger:
                    self.logger.warning(
                        'Error sending UDP message to %(target)r: %(err)s',
                        {'target': self._target, 'err': err})

    def _open_socket(self):
        return socket.socket(self._sock_family, socket.SOCK_DGRAM)

    def update_stats(self, m_name, m_value, sample_rate=None):
        return self._send(m_name, m_value, 'c', sample_rate)

    def increment(self, metric, sample_rate=None):
        return self.update_stats(metric, 1, sample_rate)

    def decrement(self, metric, sample_rate=None):
        return self.update_stats(metric, -1, sample_rate)

    def _timing(self, metric, timing_ms, sample_rate):
        # This method was added to disagregate timing metrics when testing
        return self._send(metric, round(timing_ms, 4), 'ms', sample_rate)

    def timing(self, metric, timing_ms, sample_rate=None):
        return self._timing(metric, timing_ms, sample_rate)

    def timing_since(self, metric, orig_time, sample_rate=None):
        return self._timing(metric, (time.time() - orig_time) * 1000,
                            sample_rate)

    def transfer_rate(self, metric, elapsed_time, byte_xfer, sample_rate=None):
        if byte_xfer:
            return self.timing(metric,
                               elapsed_time * 1000 / byte_xfer * 1000,
                               sample_rate)
