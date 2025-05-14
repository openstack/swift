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
import re
from contextlib import closing
from random import random

from eventlet.green import socket

from swift.common.utils.config import config_true_value


STATSD_CONF_USER_LABEL_PREFIX = 'statsd_user_label_'
STATSD_USER_LABEL_NAMESPACE = 'user_'
USER_LABEL_PATTERN = re.compile(r"[^0-9a-zA-Z_]")
USER_VALUE_PATTERN = re.compile(r"[^0-9a-zA-Z_.]")


def _build_line_parts(metric, value, metric_type, sample_rate):
    line = '%s:%s|%s' % (metric, value, metric_type)
    if sample_rate < 1:
        line += '|@%s' % (sample_rate,)
    return line


def librato(metric, value, metric_type, sample_rate, labels):
    # https://www.librato.com/docs/kb/collect/collection_agents/stastd/#stat-level-tags
    if labels:
        metric += '#' + ','.join('%s=%s' % (k, v) for k, v in labels)
    line = _build_line_parts(metric, value, metric_type, sample_rate)
    return line


def influxdb(metric, value, metric_type, sample_rate, labels):
    # https://www.influxdata.com/blog/getting-started-with-sending-statsd-metrics-to-telegraf-influxdb/#introducing-influx-statsd
    if labels:
        metric += ''.join(',%s=%s' % (k, v) for k, v in labels)
    line = _build_line_parts(metric, value, metric_type, sample_rate)
    return line


def graphite(metric, value, metric_type, sample_rate, labels):
    # https://graphite.readthedocs.io/en/latest/tags.html#carbon
    if labels:
        metric += ''.join(';%s=%s' % (k, v) for k, v in labels)
    line = _build_line_parts(metric, value, metric_type, sample_rate)
    return line


def dogstatsd(metric, value, metric_type, sample_rate, labels):
    # https://docs.datadoghq.com/developers/dogstatsd/datagram_shell/?tab=metrics
    line = _build_line_parts(metric, value, metric_type, sample_rate)
    if labels:
        line += '|#' + ','.join('%s:%s' % (k, v) for k, v in labels)
    return line


LABEL_MODES = {
    'disabled': None,
    'librato': librato,
    'influxdb': influxdb,
    'graphite': graphite,
    'dogstatsd': dogstatsd,
}


def _get_labeled_statsd_formatter(label_mode):
    """
    Returns a label formatting function for the given ``label_mode``.

    :param label_mode: A label mode.
    :raises ValueError: if ``label_mode`` is not supported by ``LabelFormats``.
    :returns: a label formatting function.
    """
    try:
        return LABEL_MODES[label_mode]
    except KeyError:
        label_modes = LABEL_MODES.keys()
        raise ValueError(
            'unknown statsd_label_mode %r; '
            'expected one of %r' % (label_mode, label_modes))


def get_statsd_client(conf=None, tail_prefix='', logger=None):
    """
    Get an instance of StatsdClient using config settings.

    **config and defaults**::

        log_statsd_host = (disabled)
        log_statsd_port = 8125
        log_statsd_default_sample_rate = 1.0
        log_statsd_sample_rate_factor = 1.0
        log_statsd_metric_prefix = (empty-string)
        statsd_emit_legacy = true

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

    emit_legacy = config_true_value(conf.get(
        'statsd_emit_legacy', 'true'))

    return StatsdClient(
        host, port,
        base_prefix=base_prefix,
        tail_prefix=tail_prefix,
        default_sample_rate=default_sample_rate,
        sample_rate_factor=sample_rate_factor,
        emit_legacy=emit_legacy,
        logger=logger)


def get_labeled_statsd_client(conf=None, logger=None):
    """
    Get an instance of LabeledStatsdClient using config settings.

    **config and defaults**::

        log_statsd_host = (disabled)
        log_statsd_port = 8125
        log_statsd_default_sample_rate = 1.0
        log_statsd_sample_rate_factor = 1.0
        statsd_label_mode = disabled

    :param conf: Configuration dict to read settings from
    :param logger: stdlib logger instance used by statsd client for logging
    :return: an instance of ``LabeledStatsdClient``
    """
    conf = conf or {}

    host = conf.get('log_statsd_host')
    port = int(conf.get('log_statsd_port', 8125))
    default_sample_rate = float(
        conf.get('log_statsd_default_sample_rate', 1))
    sample_rate_factor = float(
        conf.get('log_statsd_sample_rate_factor', 1))

    label_mode = conf.get(
        'statsd_label_mode', 'disabled').lower()

    default_labels = {}
    for k, v in conf.items():
        if not k.startswith(STATSD_CONF_USER_LABEL_PREFIX):
            continue
        conf_label = k[len(STATSD_CONF_USER_LABEL_PREFIX):]
        result = USER_LABEL_PATTERN.search(conf_label)
        if result is not None:
            raise ValueError(
                'invalid character in statsd user label '
                'configuration {0!r}: {1!r}'.format(
                    k, result.group(0)))
        result = USER_VALUE_PATTERN.search(v)
        if result is not None:
            raise ValueError(
                'invalid character in configuration {0!r} '
                'value {1!r}: {2!r}'.format(
                    k, v, result.group(0)))
        conf_label = STATSD_USER_LABEL_NAMESPACE + conf_label
        default_labels[conf_label] = v

    return LabeledStatsdClient(
        host, port,
        default_sample_rate=default_sample_rate,
        sample_rate_factor=sample_rate_factor,
        label_mode=label_mode,
        default_labels=default_labels,
        logger=logger)


class AbstractStatsdClient:
    """
    Base class to facilitate sending metrics to a socket. Sub-classes are
    responsible for formatting metrics lines.

    :param host: Statsd host name. If ``None`` then metrics are not sent.
    :param port: Statsd host port.
    :param default_sample_rate: The default rate at which metrics should be
        sampled if no sample rate is otherwise specified. Should be a float
        value between 0 and 1.
    :param sample_rate_factor: A multiplier to apply to the rate at which
        metrics are sampled. Should be a float value between 0 and 1.
    :param logger: A stdlib logger instance.
    """
    def __init__(self, host, port, default_sample_rate=1,
                 sample_rate_factor=1, logger=None):
        self._host = host
        self._port = port
        self._default_sample_rate = default_sample_rate
        self._sample_rate_factor = sample_rate_factor
        self.random = random
        self.logger = logger
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

    def _is_emitted(self, sample_rate):
        """
        Adjust the given ``sample_rate`` by the configured
        ``sample_rate_factor`` and, based on the adjusted sample rate,
        determine if a stat should be emitted on this occasion.

        Sub-classes should call this method before sending a metric line with
        ``_send_line``.

        :param sample_rate: The sample_rate given in the call to emit a stat.
            If ``None`` then this will default to the configured
            ``default_sample_rate``.
        :returns: a tuple ``(<boolean>, <adjusted_sample_rate>)``. The boolean
            is ``True`` if a stat should be emitted on this occasion, ``False``
            otherwise.
        """
        if not self._host:
            # StatsD not configured
            return False, None

        if sample_rate is None:
            sample_rate = self._default_sample_rate
        adjusted_sample_rate = sample_rate * self._sample_rate_factor

        if adjusted_sample_rate < 1 and self.random() >= adjusted_sample_rate:
            return False, None

        return True, adjusted_sample_rate

    def _send_line(self, line):
        """
        Send a ``line`` of metrics to socket.

        Sub-classes should call ``_is_emitted`` before calling this method.

        :param line: The string to be sent to the socket. If ``None`` then
            nothing is sent.
        """

        if line is None:
            return

        # Ideally, we'd cache a sending socket in self, but that
        # results in a socket getting shared by multiple green threads.
        with closing(self._open_socket()) as sock:
            try:
                return sock.sendto(line.encode('utf-8'), self._target)
            except IOError as err:
                if self.logger:
                    self.logger.warning(
                        'Error sending UDP message to %(target)r: %(err)s',
                        {'target': self._target, 'err': err})

    def _open_socket(self):
        return socket.socket(self._sock_family, socket.SOCK_DGRAM)

    def _update_stats(self, metric, value, **kwargs):
        # This method was added to disaggregate *crement metrics when testing
        return self._send(metric, value, 'c', **kwargs)

    def update_stats(self, metric, value, **kwargs):
        self._update_stats(metric, value, **kwargs)

    def increment(self, metric, **kwargs):
        return self._update_stats(metric, 1, **kwargs)

    def decrement(self, metric, **kwargs):
        return self._update_stats(metric, -1, **kwargs)

    def _timing(self, metric, timing_ms, **kwargs):
        # This method was added to disaggregate timing metrics when testing
        return self._send(metric, round(timing_ms, 4), 'ms', **kwargs)

    def timing(self, metric, timing_ms, **kwargs):
        return self._timing(metric, timing_ms, **kwargs)

    def timing_since(self, metric, orig_time, **kwargs):
        return self._timing(
            metric, (time.time() - orig_time) * 1000, **kwargs)

    def transfer_rate(self, metric, elapsed_time, byte_xfer, **kwargs):
        if byte_xfer:
            return self._timing(
                metric, elapsed_time * 1000 / byte_xfer * 1000, **kwargs)


class StatsdClient(AbstractStatsdClient):
    """
    A legacy statsd client.  This client does not support labeled metrics.

    A prefix may be specified using the ``base_prefix`` and ``tail_prefix``
    arguments. The prefix is added to the name of every metric such that the
    emitted metric name has the form:

        [<base_prefix>.][tail_prefix.]<metric name>

    :param host: Statsd host name. If ``None`` then metrics are not sent.
    :param port: Statsd host port.
    :param base_prefix: (optional) A string that will form the first part of a
        prefix added to each metric name. The prefix is separated from the
        metric name by a '.' character.
    :param tail_prefix: (optional) A string that will form the second part of a
        prefix added to each metric name. The prefix is separated from the
        metric name by a '.' character.
    :param default_sample_rate: The default rate at which metrics should be
        sampled if no sample rate is otherwise specified. Should be a float
        value between 0 and 1.
    :param sample_rate_factor: A multiplier to apply to the rate at which
        metrics are sampled. Should be a float value between 0 and 1.
    :param emit_legacy: if ``True`` then the client will emit metrics; if
        ``False``  then the client will emit no metrics.
    :param logger: A stdlib logger instance.
    """
    def __init__(self, host, port,
                 base_prefix='', tail_prefix='',
                 default_sample_rate=1, sample_rate_factor=1,
                 emit_legacy=True,
                 logger=None):
        super().__init__(
            host, port, default_sample_rate, sample_rate_factor, logger)
        self._base_prefix = base_prefix

        self.emit_legacy = emit_legacy

        self._set_prefix(tail_prefix)

    def _send(self, metric, value, metric_type, sample_rate=None):
        is_emitted, adjusted_sample_rate = self._is_emitted(sample_rate)
        if self.emit_legacy and is_emitted:
            metric = self._prefix + metric
            line = _build_line_parts(
                metric, value, metric_type, adjusted_sample_rate)
            return self._send_line(line)

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

    # for backwards compat StatsdClient supports sample_rate as positional arg
    def update_stats(self, metric, value, sample_rate=None):
        """
        Update a counter, aggregated metric changed by value.

        :param metric: name of the metric
        :param value: int, the counter delta
        :param sample_rate: float, override default sample_rate
        """
        return super().update_stats(metric, value,
                                    sample_rate=sample_rate)

    def increment(self, metric, sample_rate=None):
        """
        Increment a counter, aggregated metric increased by one.

        :param metric: name of the metric
        :param sample_rate: float, override default sample_rate
        """
        return super().increment(metric, sample_rate=sample_rate)

    def decrement(self, metric, sample_rate=None):
        """
        Decrement a counter, aggregated metric decreased by one.

        :param metric: name of the metric
        :param sample_rate: float, override default sample_rate
        """
        return super().decrement(metric, sample_rate=sample_rate)

    def timing(self, metric, timing_ms, sample_rate=None):
        """
        Update a timing metric, aggregated percentiles recalculated.

        :param metric: name of the metric
        :param timing_ms: float, total timing of operation
        :param sample_rate: float, override default sample_rate
        """
        return super().timing(metric, timing_ms, sample_rate=sample_rate)

    def timing_since(self, metric, orig_time, sample_rate=None):
        """
        Update a timing metric, aggregated percentiles recalculated.

        This is an alternative spelling of timing which calculates
        timing_ms=(time.time() - orig_time) for you.

        :param metric: name of the metric
        :param orig_time: float, time.time() from start of operation
        :param sample_rate: float, override default sample_rate
        """
        return super().timing_since(metric, orig_time,
                                    sample_rate=sample_rate)

    def transfer_rate(self, metric, elapsed_time, byte_xfer,
                      sample_rate=None):
        """
        Update a timing metric, aggregated percentiles recalculated.

        This is a timing metric, but adjusts the timing data per kB transferred
        (ms/kB) for each non-zero-byte update.  Allegedly this could be used to
        monitor problematic devices, where higher is bad.

        :param metric: name of the metric
        :param elapsed_time: float, total timing of operation
        :param byte_xfer: int, number of bytes
        :param sample_rate: float, override default sample_rate
        """
        return super().transfer_rate(metric, elapsed_time, byte_xfer,
                                     sample_rate=sample_rate)


class LabeledStatsdClient(AbstractStatsdClient):
    """
    A statsd client that supports annotating metrics with labels.

    Labeled metrics can be emitted in the style of Graphite, Librato, InfluxDB,
    or DogStatsD, by specifying the corresponding ``label_mode`` when
    constructing a client. If ``label_mode`` is ``disabled`` then no metrics
    are emitted by the client.

    Label keys should contain only ASCII letters ('a-z', 'A-Z'), digits
    ('0-9') and the underscore character ('_'). Label values may also contain
    the period ('.') character.

    Callers should avoid using labels that have a high cardinality of values
    since this may result in an unreasonable number of distinct time series for
    collectors to maintain. For example, labels should NOT be used for object
    names or transaction ids.

    :param host: Statsd host name. If ``None`` then metrics are not sent.
    :param port: Statsd host port.
    :param default_sample_rate: The default rate at which metrics should be
        sampled if no sample rate is otherwise specified. Should be a float
        value between 0 and 1.
    :param sample_rate_factor: A multiplier to apply to the rate at which
        metrics are sampled. Should be a float value between 0 and 1.
    :param label_mode: one of 'graphite', 'dogstatsd', 'librato', 'influxdb'
        or 'disabled'.
    :param default_labels: a dictionary of labels that will be added to every
        metric emitted by the client.
    :param logger: A stdlib logger instance.
    """
    def __init__(self, host, port,
                 default_sample_rate=1, sample_rate_factor=1,
                 label_mode='disabled', default_labels=None,
                 logger=None):
        super().__init__(
            host, port, default_sample_rate, sample_rate_factor, logger)
        self.default_labels = default_labels or {}
        self.label_formatter = _get_labeled_statsd_formatter(label_mode)
        if self.logger:
            self.logger.debug('Labeled statsd mode: %s (%s)',
                              label_mode, self.logger.name)

    def _send(self, metric, value, metric_type, labels=None, sample_rate=None):
        if not self.label_formatter:
            return

        is_emitted, adjusted_sample_rate = self._is_emitted(sample_rate)
        if is_emitted:
            return self._send_line(self._build_line(
                metric, value, metric_type, labels, adjusted_sample_rate))

    def _build_line(self, metric, value, metric_type, labels, sample_rate):
        all_labels = dict(self.default_labels)
        if labels:
            all_labels.update(labels)
        return self.label_formatter(
            metric,
            value,
            metric_type,
            sample_rate,
            sorted(all_labels.items()))

    def update_stats(self, metric, value, *, labels=None, sample_rate=None):
        """
        Update a counter, aggregated metric changed by value.

        :param metric: name of the metric
        :param value: int, the counter delta
        :param labels: dict, metric labels
        :param sample_rate: float, override default sample_rate
        """
        return super().update_stats(metric, value, labels=labels,
                                    sample_rate=sample_rate)

    def increment(self, metric, *, labels=None, sample_rate=None):
        """
        Increment a counter, aggregated metric increased by one.

        :param metric: name of the metric
        :param labels: dict, metric labels
        :param sample_rate: float, override default sample_rate
        """
        return super().increment(metric, labels=labels,
                                 sample_rate=sample_rate)

    def decrement(self, metric, *, labels=None, sample_rate=None):
        """
        Decrement a counter, aggregated metric decreased by one.

        :param metric: name of the metric
        :param labels: dict, metric labels
        :param sample_rate: float, override default sample_rate
        """
        return super().decrement(metric, labels=labels,
                                 sample_rate=sample_rate)

    def timing(self, metric, timing_ms, *, labels=None, sample_rate=None):
        """
        Update a timing metric, aggregated percentiles recalculated.

        :param metric: name of the metric
        :param timing_ms: float, total timing of operation
        :param labels: dict, metric labels
        :param sample_rate: float, override default sample_rate
        """
        return super().timing(metric, timing_ms, labels=labels,
                              sample_rate=sample_rate)

    def timing_since(self, metric, orig_time, *,
                     labels=None, sample_rate=None):
        """
        Update a timing metric, aggregated percentiles recalculated.

        This is an alternative spelling of timing which calculates
        timing_ms=(time.time() - orig_time) for you.

        :param metric: name of the metric
        :param orig_time: float, time.time() from start of operation
        :param labels: dict, metric labels
        :param sample_rate: float, override default sample_rate
        """
        return super().timing_since(metric, orig_time, labels=labels,
                                    sample_rate=sample_rate)

    def transfer_rate(self, metric, elapsed_time, byte_xfer, *,
                      labels=None, sample_rate=None):
        """
        Update a timing metric, aggregated percentiles recalculated.

        This is a timing metric, but adjusts the timing data per kB transferred
        (ms/kB) for each non-zero-byte update.  Allegedly this could be used to
        monitor problematic devices, where higher is bad.

        :param metric: name of the metric
        :param elapsed_time: float, total timing of operation
        :param byte_xfer: int, number of bytes
        :param labels: dict, metric labels
        :param sample_rate: float, override default sample_rate
        """
        return super().transfer_rate(metric, elapsed_time, byte_xfer,
                                     labels=labels,
                                     sample_rate=sample_rate)
