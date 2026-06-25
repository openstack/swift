# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from io import StringIO
import unittest
from contextlib import ExitStack
from unittest.mock import patch
from unittest import mock

from swift.cli import dispersion_report
from test.unit import patch_policies


def make_nodes(count=3):
    return [
        {'ip': '1.2.3.%d' % i, 'port': 6201, 'device': 'sda%d' % i,
         'replication_ip': '1.2.3.%d' % i, 'replication_port': 6201}
        for i in range(1, count + 1)
    ]


def make_coropool():
    coropool = mock.Mock()
    coropool.spawn = lambda func, *args: func(*args)
    coropool.waitall = mock.Mock()
    return coropool


def make_connpool(containers=None, objects=None):
    conn = mock.Mock()
    conn.get_account.return_value = (None, containers or [])
    conn.get_container.return_value = (None, objects or [])
    connpool = mock.MagicMock()
    connpool.item.return_value.__enter__.return_value = conn
    return connpool


def make_ring(nodes=None):
    ring = mock.Mock()
    ring.get_nodes.return_value = (0, nodes or make_nodes())
    ring.replica_count = len(nodes) if nodes else 3
    ring.partition_count = 100
    return ring


class TestGetErrorLog(unittest.TestCase):
    """Tests for the get_error_log function"""

    def setUp(self):
        """Set up test fixtures"""
        dispersion_report.unmounted = []
        dispersion_report.notfound = []
        dispersion_report.json_output = False
        dispersion_report.debug = False

    def tearDown(self):
        """Clean up after tests"""
        dispersion_report.unmounted = []
        dispersion_report.notfound = []
        dispersion_report.json_output = False
        dispersion_report.debug = False

    def test_unmounted_device_is_reported_once_across_loggers(self):
        """Test unmounted devices are reported once per report run."""
        container_error_log = dispersion_report.get_error_log('container')
        object_error_log = dispersion_report.get_error_log('object')

        # Create a mock exception with http_status 507
        exc = mock.Mock()
        exc.http_status = 507
        exc.http_host = '192.168.1.1'
        exc.http_port = 6000
        exc.http_device = 'sda'

        err = StringIO()
        with mock.patch('swift.cli.dispersion_report.stderr', err):
            container_error_log(exc)
            object_error_log(exc)

        self.assertEqual('ERROR: 192.168.1.1:6000/sda is unmounted --'
                         ' This will cause replicas designated for'
                         ' that device to be considered missing until'
                         ' resolved or the ring is updated.\n',
                         err.getvalue())

    def test_error_log_404_not_logged_when_debug_off(self):
        """Test that 404 errors don't add to notfound when debug is off"""
        dispersion_report.debug = False
        error_log = dispersion_report.get_error_log('test_prefix')

        exc = mock.Mock()
        exc.http_status = 404
        exc.http_host = '192.168.1.1'
        exc.http_port = 6000
        exc.http_device = 'sda'

        # When debug is False and status is 404, nothing should be added
        err = StringIO()
        with mock.patch('swift.cli.dispersion_report.stderr', err):
            error_log(exc)
        self.assertNotIn('returned a 404', err.getvalue())
        self.assertEqual(err.getvalue(), '')

    def test_error_log_non_http_exception(self):
        """Test logging of non-HTTP exceptions"""
        error_log = dispersion_report.get_error_log('test_prefix')

        # Just verify it doesn't raise an exception
        err = StringIO()
        with mock.patch('swift.cli.dispersion_report.stderr', err):
            # Pass a regular string (no http_status attribute)
            error_log('Test error message')
        self.assertIn('ERROR: test_prefix: Test error message', err.getvalue())


class TestMissingString(unittest.TestCase):
    """Tests for the missing_string function"""

    def test_single_partition_single_copy_missing(self):
        result = dispersion_report.missing_string(1, 1, 3)
        self.assertEqual(result, 'There was 1 partition missing 1 copy.')

    def test_multiple_partitions_single_copy_missing(self):
        result = dispersion_report.missing_string(5, 1, 3)
        self.assertEqual(result, 'There were 5 partitions missing 1 copy.')

    def test_multiple_partitions_multiple_copies_missing(self):
        # copy_count - missing_copies == 1 -> single '!' prefix
        result = dispersion_report.missing_string(3, 2, 3)
        self.assertEqual(
            result, '! There were 3 partitions missing 2 copies.')

    def test_all_copies_missing(self):
        # missing_copies == copy_count -> '!!! ' prefix and 'all' instead
        # of the numeric count
        result = dispersion_report.missing_string(2, 3, 3)
        self.assertEqual(
            result, '!!! There were 2 partitions missing all copies.')


@patch_policies
class TestGenerateReport(unittest.TestCase):
    """Tests for the generate_report function"""

    def setUp(self):
        """Set up test fixtures"""
        dispersion_report.unmounted = []
        dispersion_report.notfound = []
        dispersion_report.json_output = False
        dispersion_report.debug = False

    def tearDown(self):
        """Clean up after tests"""
        dispersion_report.unmounted = []
        dispersion_report.notfound = []
        dispersion_report.json_output = False
        dispersion_report.debug = False

    def mock_report_functions(self):
        """Patch generate_report's external dependencies.

        Returns an ExitStack context manager that, when entered, patches
        out the per-policy report calls plus the network/ring/pool
        machinery so generate_report can run without I/O. The patched
        report functions are exposed on ``self`` so the test can assert
        which ones were invoked.
        """
        stack = ExitStack()
        self.mock_container_dispersion_report = stack.enter_context(
            patch.object(dispersion_report, 'container_dispersion_report',
                         return_value={}))
        self.mock_object_dispersion_report = stack.enter_context(
            patch.object(dispersion_report, 'object_dispersion_report',
                         return_value={}))
        stack.enter_context(patch.object(dispersion_report, 'GreenPool'))
        stack.enter_context(patch.object(dispersion_report, 'Pool'))
        stack.enter_context(patch.object(dispersion_report, 'Ring'))
        stack.enter_context(patch(
            'swiftclient.get_auth',
            return_value=('http://example.com/v1/AUTH_acct', 'tk')))
        return stack

    def _base_conf(self):
        return {
            'auth_url': 'http://example.com/auth',
            'auth_user': 'user',
            'auth_key': 'key',
        }

    def test_only_container(self):
        conf = dict(self._base_conf(), object_report='no')
        with self.mock_report_functions():
            dispersion_report.generate_report(conf)
        self.assertTrue(self.mock_container_dispersion_report.called)
        self.assertFalse(self.mock_object_dispersion_report.called)

    def test_only_object(self):
        conf = dict(self._base_conf(), container_report='no')
        with self.mock_report_functions():
            dispersion_report.generate_report(conf)
        self.assertFalse(self.mock_container_dispersion_report.called)
        self.assertTrue(self.mock_object_dispersion_report.called)

    def test_both_reports_by_default(self):
        conf = self._base_conf()
        with self.mock_report_functions():
            dispersion_report.generate_report(conf)
        self.assertTrue(self.mock_container_dispersion_report.called)
        self.assertTrue(self.mock_object_dispersion_report.called)

    def test_neither_report_exits(self):
        conf = dict(self._base_conf(),
                    container_report='no', object_report='no')
        with self.mock_report_functions():
            with self.assertRaises(SystemExit):
                dispersion_report.generate_report(conf)
        self.assertFalse(self.mock_container_dispersion_report.called)
        self.assertFalse(self.mock_object_dispersion_report.called)

    def test_dump_json_sets_global(self):
        conf = dict(self._base_conf(), dump_json='yes')
        with self.mock_report_functions():
            dispersion_report.generate_report(conf)
        self.assertTrue(dispersion_report.json_output)

    def test_unknown_policy_exits(self):
        conf = self._base_conf()
        with self.mock_report_functions():
            with self.assertRaises(SystemExit):
                dispersion_report.generate_report(conf,
                                                  policy_name='nonexistent')


class TestContainerDispersionReport(unittest.TestCase):

    def setUp(self):
        self._orig_json_output = dispersion_report.json_output
        dispersion_report.json_output = True

    def tearDown(self):
        dispersion_report.json_output = self._orig_json_output

    def _policy(self, name='default', idx=0):
        p = mock.Mock()
        p.name = name
        p.idx = idx
        return p

    def test_no_containers_returns_none(self):
        result = dispersion_report.container_dispersion_report(
            make_coropool(), make_connpool(containers=[]),
            'AUTH_test', make_ring(), 3, False, self._policy())
        self.assertIsNone(result)

    @mock.patch('swift.common.direct_client.retry')
    def test_all_copies_found(self, mock_retry):
        mock_retry.return_value = (1, None)
        nodes = make_nodes(3)
        containers = [{'name': 'dispersion_0_c1'},
                      {'name': 'dispersion_0_c2'}]
        result = dispersion_report.container_dispersion_report(
            make_coropool(), make_connpool(containers=containers),
            'AUTH_test', make_ring(nodes=nodes), 3, False, self._policy())
        self.assertEqual(result['copies_found'], result['copies_expected'])
        self.assertEqual(result['pct_found'], 100.0)
        self.assertEqual(result['retries'], 0)

    @mock.patch('swift.common.direct_client.retry')
    def test_missing_copies_tracked(self, mock_retry):
        mock_retry.side_effect = Exception('node down')
        containers = [{'name': 'dispersion_0_c1'}]
        result = dispersion_report.container_dispersion_report(
            make_coropool(), make_connpool(containers=containers),
            'AUTH_test', make_ring(nodes=make_nodes(3)), 3, False,
            self._policy())
        self.assertEqual(result['copies_found'], 0)
        self.assertIn('missing_3', result)


class TestObjectDispersionReport(unittest.TestCase):

    def setUp(self):
        self._orig_json_output = dispersion_report.json_output
        dispersion_report.json_output = True

    def tearDown(self):
        dispersion_report.json_output = self._orig_json_output

    def _policy(self, name='default', idx=0):
        p = mock.MagicMock()
        p.name = name
        p.idx = idx
        p.__int__ = mock.Mock(return_value=idx)
        return p

    def test_no_objects_returns_none(self):
        result = dispersion_report.object_dispersion_report(
            make_coropool(), make_connpool(objects=[]),
            'AUTH_test', make_ring(), 3, False, self._policy())
        self.assertIsNone(result)

    @mock.patch('swift.common.direct_client.retry')
    def test_all_copies_found(self, mock_retry):
        mock_retry.return_value = (1, None)
        nodes = make_nodes(3)
        objects = [{'name': 'dispersion_obj1'},
                   {'name': 'dispersion_obj2'}]
        result = dispersion_report.object_dispersion_report(
            make_coropool(), make_connpool(objects=objects),
            'AUTH_test', make_ring(nodes=nodes), 3, False, self._policy())
        self.assertEqual(result['copies_found'], result['copies_expected'])
        self.assertEqual(result['pct_found'], 100.0)

    @mock.patch('swift.common.direct_client.retry')
    def test_missing_copies_tracked(self, mock_retry):
        mock_retry.side_effect = Exception('node down')
        objects = [{'name': 'dispersion_obj1'}]
        result = dispersion_report.object_dispersion_report(
            make_coropool(), make_connpool(objects=objects),
            'AUTH_test', make_ring(nodes=make_nodes(3)), 3, False,
            self._policy())
        self.assertEqual(result['copies_found'], 0)
        self.assertIn('missing_3', result)


if __name__ == '__main__':
    unittest.main()
