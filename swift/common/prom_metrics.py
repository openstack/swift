# Copyright (c) 2026 NVidia Corporation
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

""" Prometheus Metrics """

import os
import re
import tempfile


PROM_CONF_USER_LABEL_PREFIX = 'prom_user_label_'
PROM_USER_LABEL_NAMESPACE = 'user_'
USER_LABEL_PATTERN = re.compile(r"[^0-9a-zA-Z_]")
_METRIC_NAME_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_:]*$')
_LABEL_KEY_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
# Note: the metric line regex intentionally supports only a small, well-
# defined subset of the Prometheus text exposition format — specifically
# the simple metric lines produced by this module (either
#   metric_name{label="value",...} <value>
# or
#   metric_name <value>)
# This is a deliberate design choice to provide a "can-read-what-we-write"
# parser rather than a fully-compliant Prometheus parser. It therefore
# does NOT handle HELP/TYPE metadata lines, timestamps, exemplars, or the
# multiple-sample forms emitted for histogram/summary metrics. If a more
# complete parser is needed, replace this with a dedicated Prometheus
# exposition-format parser.
_METRIC_LINE_PATTERN = re.compile(
    r'^(?P<name>[^{\s]+)(?:\{(?P<labels>.*)\})?\s+'
    r'(?P<value>\S+)$')
PROM_METRICS_FILE_MODE = 0o644


def metric_key(name, labels):
    return (name, frozenset((k, str(v)) for k, v in labels.items()))


def _escape_label_value(value):
    """
    Escape a label value per the Prometheus exposition format: backslash,
    double-quote and line feed become ``\\\\``, ``\\"`` and ``\\n``.
    """
    return str(value).replace('\\', r'\\').replace('"', r'\"').replace(
        '\n', r'\n')


def _unescape_label_value(value):
    """
    Unescape a Prometheus label value from the text exposition format.

    Only ``\\\\``, ``\\"`` and ``\\n`` are valid escape sequences; anything
    else — including a trailing lone backslash — is a spec violation.
    """
    chars = []
    escaped = False
    for char in value:
        if escaped:
            if char == 'n':
                chars.append('\n')
            elif char in ('\\', '"'):
                chars.append(char)
            else:
                raise ValueError(
                    f"invalid escape sequence in label value: '\\{char}'")
            escaped = False
            continue
        if char == '\\':
            escaped = True
        else:
            chars.append(char)
    if escaped:
        raise ValueError(
            "invalid escape sequence in label value: trailing '\\'")
    return ''.join(chars)


def _split_label_pairs(labels_str):
    """
    Split a Prometheus label block on commas outside quoted label values.
    """
    pairs = []
    start = 0
    in_quotes = False
    escaped = False
    for i, char in enumerate(labels_str):

        # Pass over the '\' and the escaped character
        if escaped:
            escaped = False
            continue
        if in_quotes and char == '\\':
            escaped = True
            continue

        if char == '"':
            in_quotes = not in_quotes
            continue
        # If we are not in quotes,
        # then this comma defines a new label pair
        if char == ',' and not in_quotes:
            pairs.append(labels_str[start:i])
            start = i + 1

    if in_quotes:
        raise ValueError("Unterminated label value quote")

    # Prometheus accepts trailing commas in label pairs, but we only add the
    # final fragment when there is one left after the last comma. If the string
    # ends with a comma, the final fragment is empty and should be ignored; if
    # there is any non-comma content left, append it as the final label/value
    # pair. This keeps the split logic consistent for both ordinary label sets
    # and the trailing-comma case.
    if start < len(labels_str):
        pairs.append(labels_str[start:])
    return pairs


def _parse_label_value(label_key, value):
    """
    Parse one quoted Prometheus label value.
    """
    value = value.strip()
    if not value.startswith('"'):
        raise ValueError(
            f"Invalid Prometheus label value for {label_key!r}: "
            f"must be quoted")

    escaped = False
    for i, char in enumerate(value[1:], 1):
        if escaped:
            escaped = False
            continue
        if char == '\\':
            escaped = True
            continue
        if char == '"':
            if value[i + 1:].strip():
                raise ValueError(
                    f"Invalid Prometheus label value for {label_key!r}: "
                    f"trailing characters after quoted value")
            return _unescape_label_value(value[1:i])

    raise ValueError("Unterminated label value quote")


def parse_metric(line):
    """
    Parse a Prometheus format metric string.

    Accepts formats:
    - With labels: metric_name{label1="value1",label2="value2"} 42.0
    - Without labels: metric_name 42.0

    The parser is intentionally lenient about harmless whitespace around
    label keys, the '=' separator, and commas (e.g. ``a = "1", b="2"``),
    and it tolerates leading/trailing whitespace on the whole line. This
    follows a "be liberal in what you accept" approach to improve
    interoperability with emitters that add optional spacing. It still
    enforces quoted label values, proper escaping, and unique label keys.

    :param line: String in Prometheus text format
    :returns: Tuple of (name, labels_dict, value)
    :raises ValueError: If format is incorrect or unparseable
    """
    line = line.strip()
    if not line:
        raise ValueError("Empty metric line")

    match = _METRIC_LINE_PATTERN.match(line)
    if not match:
        raise ValueError(
            f"Invalid Prometheus format: {line!r} - "
            f"expected format 'metric_name value' or "
            f"'metric_name{{labels}} value'"
        )

    name = match.group('name')
    if not _METRIC_NAME_PATTERN.match(name):
        raise ValueError(f"Invalid Prometheus metric name: {name!r}")

    labels = {}
    labels_str = match.group('labels')
    if labels_str:
        for pair in _split_label_pairs(labels_str):
            if '=' not in pair:
                raise ValueError(
                    f"Invalid label pair '{pair}': missing '='")
            k, v = pair.split('=', 1)
            k = k.strip()
            if not k:
                raise ValueError("Label key cannot be empty")
            if not _LABEL_KEY_PATTERN.match(k):
                raise ValueError(f"Invalid Prometheus label key: {k!r}")
            if k in labels:
                raise ValueError(f"Duplicate Prometheus label key: {k!r}")
            labels[k] = _parse_label_value(k, v)

    try:
        value_str = match.group('value')
        value = float(value_str)
    except ValueError:
        raise ValueError(
            f"Invalid metric value '{value_str}': must be a number. "
            f"Format: metric_name value or metric_name{{labels}} value"
        )

    return name, labels, value


def _build_line_parts(name, value, labels=None):
    """
    Format a single metric into Prometheus text format string.

    :param name: Metric name (string)
    :param value: Metric value (int or float)
    :param labels: Optional dict of label key-value pairs
    :returns: Prometheus-formatted metric string
    """

    if not isinstance(name, str):
        raise TypeError(f"Metric name must be string, got {type(name)}")
    if not _METRIC_NAME_PATTERN.match(name):
        raise ValueError(f"Invalid Prometheus metric name: {name!r}")

    if labels:
        for label_name in labels:
            if not _LABEL_KEY_PATTERN.match(label_name):
                raise ValueError(
                    f"Invalid Prometheus label key: {label_name!r}")
        # Sort labels for consistent output
        labels_str = ','.join(
            f'{k}="{_escape_label_value(v)}"' for k, v in sorted(
                labels.items())
        )
        return name + '{' + labels_str + '} ' + str(value) + '\n'
    else:
        return name + ' ' + str(value) + '\n'


def get_prometheus_client(conf):
    """
    Get an instance of PrometheusClient using config settings.

    Configuration supports user labels via 'prom_user_label_*' prefix:
        prom_user_label_hostname=myhost
        prom_user_label_region=us-west

    User labels are automatically added to all metrics as 'user_*' labels.

    :param conf: Configuration dictionary
    :returns: PrometheusClient instance
    :raises ValueError: If default label names contain invalid characters
    """
    conf = conf or {}
    filename = conf.get('metrics_filename')

    # Extract user labels from configuration
    user_labels = {}
    for k, v in conf.items():
        if not k.startswith(PROM_CONF_USER_LABEL_PREFIX):
            continue
        conf_label = k[len(PROM_CONF_USER_LABEL_PREFIX):]
        if USER_LABEL_PATTERN.search(conf_label):
            raise ValueError(
                f"invalid character in prom user label configuration {k!r}")
        user_labels[PROM_USER_LABEL_NAMESPACE + conf_label] = v

    return PrometheusClient(filename, user_labels=user_labels)


class PrometheusClient:

    def __init__(self, filename, user_labels=None):
        self._stats = {}
        self.filename = filename
        self.user_labels = user_labels or {}

    def _update_stats(self, key, value):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(
                f"Metric value must be a number, got {type(value)}")
        self._stats[key] = value

    def _make_key(self, name, labels):
        if not _METRIC_NAME_PATTERN.match(name):
            raise ValueError(f"Invalid Prometheus metric name: {name!r}")
        all_labels = dict(self.user_labels)
        if labels is not None:
            for key in labels:
                if not _LABEL_KEY_PATTERN.match(key):
                    raise ValueError(f"Invalid Prometheus label key: {key!r}")
                # Reserve the configured user label namespace to avoid callers
                # shadowing labels injected from configuration
                # (prom_user_label_*)
                if key.startswith(PROM_USER_LABEL_NAMESPACE):
                    raise ValueError(
                        f"Prometheus user label namespace "
                        f"{PROM_USER_LABEL_NAMESPACE} is reserved; "
                        f"label key {key!r} is not allowed")
                if key in all_labels:
                    raise ValueError(
                        f"duplicate Prometheus label key: {key!r}")
            all_labels.update(labels or {})
        return metric_key(name, all_labels)

    def get_all_stats(self):
        lines = []
        for (metric, labels), value in self._stats.items():
            lines.append(_build_line_parts(
                metric, labels=dict(labels), value=value))
        return ''.join(lines)

    def write_all_stats(self, filename=None):
        """
        Write all recorded metrics to a file or return as structured data.

        If a filename is provided (either via the argument or set on the client
        via conf['metrics_filename']), writes all metrics in Prometheus text
        exposition format to that file using atomic write (temp file + rename).

        If no filename is available, returns all metrics as a list of
        dictionaries suitable for programmatic use.

        :param filename: Optional explicit path to write metrics to. If
                         omitted, uses the filename set during client
                         initialization.
        :returns: None if a filename was written to; list of metric dicts
                  [{'name': str, 'labels': dict, 'value': float}, ...] if no
                  filename was available (sorted by name then labels).
        :raises OSError: If file operations fail (e.g. permission denied).
        """
        filename = filename or self.filename
        if not filename:
            return self.get_all_stats_data()
        stats_data = self.get_all_stats()
        tf = None
        try:
            with tempfile.NamedTemporaryFile(
                    mode='w', dir=os.path.dirname(filename),
                    delete=False) as tf:
                tf.write(stats_data)
            os.chmod(tf.name, PROM_METRICS_FILE_MODE)
            os.rename(tf.name, filename)
            tf = None
        finally:
            if tf:
                try:
                    os.unlink(tf.name)
                except FileNotFoundError:
                    pass

    def get_all_stats_data(self):
        """
        Return all recorded metrics as a list of dictionaries.

        Each dictionary contains the metric name, label mapping, and value.
        """
        return [
            {'name': name, 'labels': dict(labels), 'value': value}
            for (name, labels), value in sorted(
                self._stats.items(),
                key=lambda item: (item[0][0], sorted(item[0][1]))
            )
        ]

    def record(self, name, value, labels=None):
        """
        Record a metric with an explicit value, replacing any previous value.

        Sets the given metric to the provided value. If the metric already
        exists, it is overwritten. User-defined labels (from conf) are
        automatically merged with any caller-supplied labels.

        :param name: Prometheus metric name (must match
                     [a-zA-Z_][a-zA-Z0-9_:]*).
        :param value: Numeric metric value (int or float, not bool).
        :param labels: Optional dict of label key-value pairs to associate
                       with this metric. Keys must match
                       [a-zA-Z_][a-zA-Z0-9_]*.
        :returns: None
        :raises ValueError: If metric name or label key is invalid.
        :raises TypeError: If value is not numeric (or is bool).
        """
        self._update_stats(self._make_key(name, labels), value)

    def add(self, name, value, labels=None):
        """
        Accumulate a value to a metric (counter increment/decrement).

        Adds the given value to the current metric value. If the metric does
        not exist, treats the initial value as 0. User-defined labels are
        automatically merged with caller-supplied labels.

        :param name: Prometheus metric name (must match
                     [a-zA-Z_][a-zA-Z0-9_:]*).
        :param value: Numeric delta to add (int or float, positive or
                      negative).
        :param labels: Optional dict of label key-value pairs. Keys must match
                       [a-zA-Z_][a-zA-Z0-9_]*.
        :returns: The updated numeric value for the metric.
        :raises ValueError: If metric name or label key is invalid.
        :raises TypeError: If value is not numeric (or is bool).
        """
        key = self._make_key(name, labels)
        # Reject boolean inputs explicitly — booleans are subclasses of int in
        # Python and would silently coerce to 1/0 during arithmetic. Require a
        # true numeric (int/float) value to be provided to avoid the
        # antipattern where True/False is passed by mistake.
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(
                f"Metric delta must be a number, got {type(value)}")
        new_value = self._stats.get(key, 0) + value
        self._update_stats(key, new_value)
        return new_value

    def increment(self, name, labels=None):
        """
        Increment a metric by 1 (shorthand for add(name, 1, labels)).

        Adds 1 to the current metric value. If the metric does not exist,
        it is initialized to 1.

        :param name: Prometheus metric name (must match
                     [a-zA-Z_][a-zA-Z0-9_:]*).
        :param labels: Optional dict of label key-value pairs.
        :returns: The updated numeric value for the metric.
        :raises ValueError: If metric name or label key is invalid.
        """
        return self.add(name, 1, labels)

    def decrement(self, name, labels=None):
        """
        Decrement a metric by 1 (shorthand for add(name, -1, labels)).

        Subtracts 1 from the current metric value. If the metric does not
        exist, it is initialized to -1.

        :param name: Prometheus metric name (must match
                     [a-zA-Z_][a-zA-Z0-9_:]*).
        :param labels: Optional dict of label key-value pairs.
        :returns: The updated numeric value for the metric.
        :raises ValueError: If metric name or label key is invalid.
        """
        return self.add(name, -1, labels)
