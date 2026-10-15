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
import os
import unittest
from unittest import mock

from swift.common.prom_metrics import (
    get_prometheus_client,
    parse_metric,
    _build_line_parts,
    _unescape_label_value,
    metric_key,
)

from test.unit import with_tempdir


def read_metrics(filename):
    metrics = {}
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            name, labels, value = parse_metric(line)
            metrics[metric_key(name, labels)] = value
    return metrics


class TestReadMetrics(unittest.TestCase):

    @with_tempdir
    def test_read_metrics_with_label_key_name(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        with open(filename, 'w') as f:
            f.write('disk_usage_bytes{metric_name="disk",name="sda1"} 1234\n')

        metrics = read_metrics(filename)

        self.assertEqual(metrics, {
            metric_key('disk_usage_bytes',
                       {'metric_name': 'disk', 'name': 'sda1'}): 1234.0,
        })

    @with_tempdir
    def test_read_metrics_skips_comments(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        with open(filename, 'w') as f:
            f.write('# HELP http_requests_total Total HTTP requests\n')
            f.write('# TYPE http_requests_total counter\n')
            f.write('http_requests_total 100\n')

        metrics = read_metrics(filename)

        self.assertEqual(metrics, {
            metric_key('http_requests_total', {}): 100.0,
        })

    @with_tempdir
    def test_read_metrics_skips_empty_lines(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        with open(filename, 'w') as f:
            f.write('metric_one 1\n')
            f.write('\n')
            f.write('   \n')
            f.write('\n\n')
            f.write('\r\n')
            f.write('metric_two 2\n')

        metrics = read_metrics(filename)

        self.assertEqual(metrics, {
            metric_key('metric_one', {}): 1.0,
            metric_key('metric_two', {}): 2.0,
        })

    def test_read_metrics_missing_file(self):
        with self.assertRaises(FileNotFoundError):
            read_metrics('/nonexistent/path/metrics.prom')

    @with_tempdir
    def test_read_metrics_empty_file(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        with open(filename, 'w'):
            pass  # empty file

        metrics = read_metrics(filename)

        self.assertEqual(metrics, {})

    @with_tempdir
    def test_read_metrics_mixed_format(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        with open(filename, 'w') as f:
            f.write('# HELP my_counter A counter\n')
            f.write('# TYPE my_counter counter\n')
            f.write('my_counter{label="value"} 10\n')
            f.write('\n')
            f.write('my_gauge 3.14\n')

        metrics = read_metrics(filename)

        self.assertEqual(metrics, {
            metric_key('my_counter', {'label': 'value'}): 10.0,
            metric_key('my_gauge', {}): 3.14,
        })


class TestPrometheusParsing(unittest.TestCase):

    def test_parse_metric_with_labels(self):
        name, labels, value = parse_metric(
            'http_requests_total{endpoint="/api",method="GET"} 42.0')
        self.assertEqual(name, 'http_requests_total')
        self.assertEqual(labels, {'endpoint': '/api', 'method': 'GET'})
        self.assertEqual(value, 42.0)

    def test_parse_metric_with_quoted_comma_label(self):
        name, labels, value = parse_metric(
            'swift_metric{container="orange,pear",method="GET"} 1')
        self.assertEqual(name, 'swift_metric')
        self.assertEqual(labels, {
            'container': 'orange,pear',
            'method': 'GET',
        })
        self.assertEqual(value, 1.0)

    def test_parse_metric_with_trailing_label_comma(self):
        name, labels, value = parse_metric(
            'swift_metric{container="orange,pear",method="GET",} 1')
        self.assertEqual(name, 'swift_metric')
        self.assertEqual(labels, {
            'container': 'orange,pear',
            'method': 'GET',
        })
        self.assertEqual(value, 1.0)

    def test_parse_metric_with_escaped_label_value(self):
        name, labels, value = parse_metric(
            'swift_metric{reason="read \\"EOF\\"\\nretry \\\\ again"} 1')
        self.assertEqual(name, 'swift_metric')
        self.assertEqual(labels, {
            'reason': 'read "EOF"\nretry \\ again',
        })
        self.assertEqual(value, 1.0)

    def test_parse_metric_rejects_unknown_escape(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('swift_metric{k="foo\\zbar"} 1')
        self.assertIn("invalid escape sequence", str(ctx.exception))
        self.assertIn("\\z", str(ctx.exception))

    def test_unescape_label_value_rejects_unknown_escape(self):
        with self.assertRaises(ValueError) as ctx:
            _unescape_label_value('foo\\zbar')
        self.assertIn("invalid escape sequence", str(ctx.exception))

    def test_unescape_label_value_rejects_trailing_backslash(self):
        with self.assertRaises(ValueError) as ctx:
            _unescape_label_value('foo\\')
        self.assertIn("trailing", str(ctx.exception))

    def test_parse_metric_without_labels(self):
        name, labels, value = parse_metric('simple_metric 123.45')
        self.assertEqual(name, 'simple_metric')
        self.assertEqual(labels, {})
        self.assertEqual(value, 123.45)

    def test_parse_metric_with_empty_labels(self):
        name, labels, value = parse_metric('metric_with_empty_labels{} 10.0')
        self.assertEqual(name, 'metric_with_empty_labels')
        self.assertEqual(labels, {})
        self.assertEqual(value, 10.0)

    def test_parse_metric_empty_line(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('')
        self.assertIn('Empty metric line', str(ctx.exception))

    def test_parse_metric_missing_value(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric_name')
        self.assertIn('Invalid Prometheus format', str(ctx.exception))

    def test_parse_metric_invalid_value(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric_name invalid_value')
        self.assertIn('must be a number', str(ctx.exception))

    def test_parse_metric_missing_closing_brace(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{label="value" 42')
        self.assertIn('Invalid Prometheus format', str(ctx.exception))

    def test_parse_metric_invalid_label_format(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{invalid_label} 42')
        self.assertIn('missing', str(ctx.exception))

    def test_parse_metric_empty_label_pair_in_middle(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{label="value",,other="value"} 42')
        self.assertIn("Invalid label pair '': missing '='",
                      str(ctx.exception))

    def test_parse_metric_unquoted_label_value(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{label=value} 42')
        self.assertIn("Invalid Prometheus label value for 'label': "
                      "must be quoted", str(ctx.exception))

    def test_parse_metric_unterminated_label_value(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{label="value} 42')
        self.assertIn('Unterminated label value quote', str(ctx.exception))

    def test_parse_metric_allows_spaces_around_commas(self):
        name, labels, value = parse_metric(
            'metric{a="1", b="2"} 3')
        self.assertEqual(name, 'metric')
        self.assertEqual(labels, {'a': '1', 'b': '2'})
        self.assertEqual(value, 3.0)

    def test_parse_metric_allows_spaces_around_equal(self):
        name, labels, value = parse_metric(
            'metric{a = "1",b="2"} 3')
        self.assertEqual(name, 'metric')
        self.assertEqual(labels, {'a': '1', 'b': '2'})
        self.assertEqual(value, 3.0)

    def test_parse_metric_trailing_comma(self):
        name, labels, value = parse_metric('metric{a="1",} 3')
        self.assertEqual(name, 'metric')
        self.assertEqual(labels, {'a': '1'})
        self.assertEqual(value, 3.0)

    def test_parse_metric_leading_trailing_whitespace_line(self):
        name, labels, value = parse_metric('   metric{a="1"}   3   ')
        self.assertEqual(name, 'metric')
        self.assertEqual(labels, {'a': '1'})
        self.assertEqual(value, 3.0)

    def test_parse_metric_trailing_label_value_text(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{a="x"}{b="y"} 1')
        self.assertIn('trailing characters', str(ctx.exception))

    def test_parse_metric_duplicate_label_key(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{label="old",label="new"} 42')
        self.assertIn('Duplicate Prometheus label key', str(ctx.exception))

    def test_parse_metric_invalid_metric_name(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('bad-metric 42')
        self.assertIn('Invalid Prometheus metric name', str(ctx.exception))

    def test_parse_metric_invalid_label_name(self):
        with self.assertRaises(ValueError) as ctx:
            parse_metric('metric{bad-label="value"} 42')
        self.assertIn('Invalid Prometheus label key', str(ctx.exception))

    def test_build_line_parts_simple(self):
        self.assertEqual(
            _build_line_parts('http_requests_total', 42.1),
            'http_requests_total 42.1\n')

    def test_build_line_parts_with_labels(self):
        result = _build_line_parts(
            'http_requests_total', 42, {'method': 'GET', 'endpoint': '/api'})
        self.assertEqual(
            result,
            'http_requests_total{endpoint="/api",method="GET"} 42\n')

    def test_build_line_parts_invalid_name_type(self):
        with self.assertRaises(TypeError) as ctx:
            _build_line_parts(123, 42)
        self.assertIn('Metric name must be string', str(ctx.exception))

    def test_build_line_parts_invalid_metric_name(self):
        with self.assertRaises(ValueError) as ctx:
            _build_line_parts('bad-metric', 42)
        self.assertIn('Invalid Prometheus metric name', str(ctx.exception))

    def test_build_line_parts_invalid_label_name(self):
        with self.assertRaises(ValueError) as ctx:
            _build_line_parts('metric', 42, {'bad-label': 'value'})
        self.assertIn('Invalid Prometheus label key', str(ctx.exception))

    def test_build_line_parts_label_sorting(self):
        result = _build_line_parts(
            'metric', 10, {'z_label': 'z', 'a_label': 'a', 'm_label': 'm'})
        self.assertEqual(
            result,
            'metric{a_label="a",m_label="m",z_label="z"} 10\n')

    def test_build_line_parts_escapes_label_values(self):
        result = _build_line_parts(
            'errors_total', 1, {'reason': 'read "EOF"\nretrying'})
        self.assertEqual(
            result,
            'errors_total{reason="read \\"EOF\\"\\nretrying"} 1\n')


class TestPrometheusClient(unittest.TestCase):

    @with_tempdir
    def test_record_with_labels(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        labels = {'location': 'room1', 'unit': 'celsius'}
        client.record('room_temperature', 22.0, labels)
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('room_temperature',
                       {'location': 'room1', 'unit': 'celsius'}): 22.0,
        })

    @with_tempdir
    def test_record_with_label_key_name(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.record(
            'disk_usage_bytes', 1234,
            {'metric_name': 'disk', 'name': 'sda1'})
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key(
                'disk_usage_bytes',
                {'metric_name': 'disk', 'name': 'sda1'}): 1234.0,
        })

    @with_tempdir
    def test_add_accumulates(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        labels = {'endpoint': '/api', 'status': '200'}
        client.add('http_requests_total', 500, labels)
        client.add('http_requests_total', 100, labels)
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('http_requests_total',
                       {'endpoint': '/api', 'status': '200'}): 600.0,
        })

    @with_tempdir
    def test_add_with_label_key_name(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.add('disk_io_total', 5,
                   {'metric_name': 'disk', 'name': 'sda1'})
        client.add('disk_io_total', 7,
                   {'metric_name': 'disk', 'name': 'sda1'})
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('disk_io_total',
                       {'metric_name': 'disk', 'name': 'sda1'}): 12.0,
        })

    @with_tempdir
    def test_add_negative_delta(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.add('counter', 10)
        client.add('counter', -3)
        client.write_all_stats(filename=filename)
        self.assertEqual(
            read_metrics(filename)[metric_key('counter', {})], 7.0)

    @with_tempdir
    def test_increment_from_zero(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.increment('errors')
        client.increment('errors')
        key = metric_key('errors', {})
        self.assertEqual(client._stats[key], 2)
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename)[key], 2.0)

    @with_tempdir
    def test_decrement_below_zero(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.decrement('queue_depth')
        key = metric_key('queue_depth', {})
        self.assertEqual(client._stats[key], -1)
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename)[key], -1.0)

    @with_tempdir
    def test_write_all_stats(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.record('metric1', 100)
        client.record('metric2', 200, {'label': 'value'})
        client.record('metric3', 300)
        self.assertFalse(os.path.exists(filename))
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('metric1', {}): 100.0,
            metric_key('metric2', {'label': 'value'}): 200.0,
            metric_key('metric3', {}): 300.0,
        })
        self.assertEqual(os.stat(filename).st_mode & 0o777, 0o644)
        self.assertEqual(os.listdir(tempdir), ['metrics.prom'])

    @with_tempdir
    def test_write_all_stats_replaces_file_atomically(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.record('metric1', 100)
        client.write_all_stats(filename=filename)

        client.record('metric1', 200)
        with mock.patch('swift.common.prom_metrics.os.rename',
                        side_effect=OSError('nope')):
            with self.assertRaises(OSError):
                client.write_all_stats(filename=filename)

        self.assertEqual(read_metrics(filename), {
            metric_key('metric1', {}): 100.0,
        })
        # a half-written file must never be left where a scraper can glob it
        self.assertEqual(os.listdir(tempdir), ['metrics.prom'])

    @with_tempdir
    def test_write_all_stats_uses_configured_filename(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({'metrics_filename': filename})
        client.record('metric1', 100)
        self.assertIsNone(client.write_all_stats())
        self.assertEqual(read_metrics(filename), {
            metric_key('metric1', {}): 100.0,
        })

    @with_tempdir
    def test_write_all_stats_same_name_different_labels(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.record('http_requests_total', 10, {'method': 'GET'})
        client.record('http_requests_total', 20, {'method': 'POST'})
        client.record('http_requests_total', 30,
                      {'method': 'GET', 'status': '500'})
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('http_requests_total', {'method': 'GET'}): 10.0,
            metric_key('http_requests_total', {'method': 'POST'}): 20.0,
            metric_key('http_requests_total',
                       {'method': 'GET', 'status': '500'}): 30.0,
        })

    @with_tempdir
    def test_write_all_stats_same_name_labeled_and_unlabeled(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.record('cpu_seconds', 1.0)
        client.record('cpu_seconds', 2.5, {'cpu': '0'})
        client.record('cpu_seconds', 3.5, {'cpu': '1'})
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('cpu_seconds', {}): 1.0,
            metric_key('cpu_seconds', {'cpu': '0'}): 2.5,
            metric_key('cpu_seconds', {'cpu': '1'}): 3.5,
        })

    @with_tempdir
    def test_write_all_stats_with_internal_formats(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client._update_stats(metric_key('simple_metric', {}), 42)
        client._update_stats(metric_key('labeled_metric',
                                        {'label': 'value'}), 100)
        client._update_stats(metric_key('string_metric',
                                        {'type': 'test'}), 55.0)
        client._update_stats(metric_key('dict_metric', {'key': 'val'}), 99)
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('simple_metric', {}): 42.0,
            metric_key('labeled_metric', {'label': 'value'}): 100.0,
            metric_key('string_metric', {'type': 'test'}): 55.0,
            metric_key('dict_metric', {'key': 'val'}): 99.0,
        })

    @with_tempdir
    def test_write_all_stats_no_stats_creates_empty_file(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.write_all_stats(filename=filename)
        self.assertTrue(os.path.exists(filename))
        self.assertEqual(os.stat(filename).st_size, 0)

    def test_write_all_stats_no_filename_returns_data(self):
        client = get_prometheus_client({})
        client.record('cpu', 1.0)
        result = client.write_all_stats()
        self.assertEqual(result, [
            {'name': 'cpu', 'labels': {}, 'value': 1.0},
        ])

    def test_get_all_stats_data(self):
        client = get_prometheus_client({})
        client.record('metric_a', 1)
        client.record('metric_b', 2, {'label': 'value'})
        self.assertEqual(client.get_all_stats_data(), [
            {'name': 'metric_a', 'labels': {}, 'value': 1.0},
            {'name': 'metric_b', 'labels': {'label': 'value'}, 'value': 2.0},
        ])

    def test_user_labels_from_config(self):
        client = get_prometheus_client({
            'prom_user_label_hostname': 'server1',
            'prom_user_label_region': 'us-west',
        })
        self.assertEqual(client.user_labels, {
            'user_hostname': 'server1',
            'user_region': 'us-west',
        })

    @with_tempdir
    def test_record_merges_user_labels(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({
            'prom_user_label_hostname': 'myhost',
            'prom_user_label_region': 'us-west',
        })
        client.record('temperature', 22.5)
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('temperature',
                       {'user_hostname': 'myhost',
                        'user_region': 'us-west'}): 22.5,
        })

    @with_tempdir
    def test_record_caller_labels_extend_defaults(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({
            'prom_user_label_hostname': 'default-host',
        })
        client.record('cpu_usage', 75.0, labels={'type': 'system'})
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('cpu_usage',
                       {'user_hostname': 'default-host',
                        'type': 'system'}): 75.0,
        })

    def test_record_user_label_collision_raises(self):
        client = get_prometheus_client({
            'prom_user_label_hostname': 'default-host',
        })

        with self.assertRaises(ValueError) as ctx:
            client.record(
                'cpu_usage', 75.0,
                labels={'user_hostname': 'caller-host'})

        self.assertIn("Prometheus user label namespace user_ is reserved",
                      str(ctx.exception))
        self.assertEqual({}, client._stats)

    def test_duplicate_label_key_raises(self):
        # A caller-supplied label key can only collide with an existing
        # entry in all_labels if self.user_labels itself holds a
        # non-namespaced key; get_prometheus_client() always namespaces
        # configured user labels with the user_ prefix, so exercise the
        # generic duplicate-key check directly.
        client = get_prometheus_client({})
        client.user_labels['hostname'] = 'default-host'

        with self.assertRaises(ValueError) as ctx:
            client.record(
                'cpu_usage', 75.0, labels={'hostname': 'caller-host'})

        self.assertIn("duplicate Prometheus label key: 'hostname'",
                      str(ctx.exception))
        self.assertEqual({}, client._stats)

    @with_tempdir
    def test_record_invalid_name_or_label_does_not_break_write(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({})
        client.record('fine_metric', 5)

        with self.assertRaises(ValueError) as ctx:
            client.record('bad-name', 1)
        self.assertIn("Invalid Prometheus metric name: 'bad-name'",
                      str(ctx.exception))

        with self.assertRaises(ValueError) as ctx:
            client.record('fine_metric', 1, labels={'bad-label': 'value'})
        self.assertIn("Invalid Prometheus label key: 'bad-label'",
                      str(ctx.exception))

        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('fine_metric', {}): 5.0,
        })

    def test_record_non_numeric_value_raises(self):
        client = get_prometheus_client({})

        for value in (None, 'not-a-number', True):
            with self.assertRaises(TypeError) as ctx:
                client.record('bad_metric', value)
            self.assertIn('Metric value must be a number',
                          str(ctx.exception))

        self.assertEqual({}, client._stats)

    @with_tempdir
    def test_add_merges_user_labels(self, tempdir):
        filename = os.path.join(tempdir, 'metrics.prom')
        client = get_prometheus_client({
            'prom_user_label_service': 'api',
        })
        client.add('requests_total', 1000, labels={'endpoint': '/v2'})
        client.write_all_stats(filename=filename)
        self.assertEqual(read_metrics(filename), {
            metric_key('requests_total',
                       {'user_service': 'api', 'endpoint': '/v2'}): 1000.0,
        })

    def test_add_user_label_collision_raises(self):
        client = get_prometheus_client({
            'prom_user_label_service': 'api',
        })

        with self.assertRaises(ValueError) as ctx:
            client.add(
                'requests_total', 1000,
                labels={'user_service': 'caller-service'})

        self.assertIn("Prometheus user label namespace user_ is reserved",
                      str(ctx.exception))
        self.assertEqual({}, client._stats)

    def test_invalid_label_name_in_config(self):
        with self.assertRaises(ValueError) as ctx:
            get_prometheus_client({'prom_user_label_invalid-name': 'value'})
        self.assertIn('invalid character in prom user label',
                      str(ctx.exception))

    def test_user_label_values_from_config_are_escaped(self):
        client = get_prometheus_client({
            'prom_user_label_endpoint': '/api',
            'prom_user_label_reason': 'read "EOF" \\ retry',
            'prom_user_label_route': 'a,b',
        })
        client.record('requests_total', 1)
        self.assertEqual(
            client.get_all_stats(),
            'requests_total{user_endpoint="/api",'
            'user_reason="read \\"EOF\\" \\\\ retry",'
            'user_route="a,b"} 1\n')

    def test_record_no_filename_updates_stats_only(self):
        client = get_prometheus_client({})
        client.record('cpu', 99.0, {'host': 'h1'})
        self.assertEqual(client._stats[metric_key('cpu', {'host': 'h1'})],
                         99.0)

    def test_get_all_stats_simple_metric(self):
        client = get_prometheus_client({})
        client.record('simple_metric', 42)
        self.assertEqual(client.get_all_stats(), 'simple_metric 42\n')

    def test_get_all_stats_with_labels(self):
        client = get_prometheus_client({})
        client.record('http_requests_total', 100,
                      {'method': 'GET', 'status': '200'})
        self.assertEqual(
            client.get_all_stats(),
            'http_requests_total{method="GET",status="200"} 100\n')

    def test_get_all_stats_multiple_metrics(self):
        client = get_prometheus_client({})
        client.record('metric_a', 1)
        client.record('metric_b', 2, {'label': 'value'})
        client.record('metric_c', 3.5)
        result = client.get_all_stats()
        # parse round-trip avoids depending on dict iteration order
        parsed = {}
        for line in result.strip().split('\n'):
            name, labels, value = parse_metric(line)
            parsed[metric_key(name, labels)] = value
        self.assertEqual(parsed, {
            metric_key('metric_a', {}): 1.0,
            metric_key('metric_b', {'label': 'value'}): 2.0,
            metric_key('metric_c', {}): 3.5,
        })

    def test_get_all_stats_after_add_accumulates(self):
        client = get_prometheus_client({})
        client.add('counter', 5)
        client.add('counter', 7)
        self.assertEqual(client.get_all_stats(), 'counter 12\n')

    def test_parse_build_line_parts_roundtrip(self):
        metric = ('http_requests_total',
                  {'method': 'GET', 'endpoint': '/api'}, 42.0)
        built_metric = _build_line_parts(metric[0], metric[2], metric[1])
        parsed_metric = parse_metric(built_metric)
        self.assertEqual(metric, parsed_metric)

    def test_parse_build_line_parts_roundtrip_quoted_comma(self):
        metric = ('swift_metric', {'container': 'orange,pear'}, 1.0)
        built_metric = _build_line_parts(metric[0], metric[2], metric[1])
        parsed_metric = parse_metric(built_metric)
        self.assertEqual(metric, parsed_metric)

    def test_parse_build_line_parts_roundtrip_brace_label_value(self):
        metric = ('swift_metric', {'test': '{ brace } value'}, 1.0)
        built_metric = _build_line_parts(metric[0], metric[2], metric[1])
        parsed_metric = parse_metric(built_metric)
        self.assertEqual(metric, parsed_metric)

    def test_build_line_parts_parse_roundtrip(self):
        metric = 'http_requests_total{endpoint="/api",method="GET"} 42.0\n'
        parsed_metric = parse_metric(metric)
        built_metric = _build_line_parts(
            parsed_metric[0], parsed_metric[2], parsed_metric[1])
        self.assertEqual(metric, built_metric)
