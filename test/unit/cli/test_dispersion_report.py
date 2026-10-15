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
import os
import sys
import tempfile
import unittest
from contextlib import ExitStack
from unittest.mock import patch
from unittest import mock

from swift.cli import dispersion, dispersion_populate, dispersion_report
from swift.common.exceptions import ClientException
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


class TestContainerDispersionNames(unittest.TestCase):

    def setUp(self):
        self.account_listing = [
            {'name': 'dispersion_1'},
            {'name': 'dispersion_23'},
            {'name': 'dispersion_0_4'},
            {'name': 'dispersion_1_5'},
            {'name': 'dispersion_objects'},
            {'name': 'dispersion_objects_0'},
            {'name': 'dispersion_0_bad'},
            {'name': 'unrelated'},
        ]

    def test_policy_indexed_names_are_obsolete(self):
        self.assertEqual([
            'dispersion_0_4', 'dispersion_1_5'],
            dispersion.obsolete_container_dispersion_names(
                self.account_listing))

    def test_only_pre_policy_names_are_canonical(self):
        self.assertEqual(
            ['dispersion_1', 'dispersion_23'],
            dispersion.container_dispersion_names(self.account_listing))

    def test_all_container_sample_names_are_purge_candidates(self):
        self.assertEqual(
            ['dispersion_1', 'dispersion_23',
             'dispersion_0_4', 'dispersion_1_5'],
            dispersion.all_container_dispersion_names(
                self.account_listing))

    def test_only_expected_object_names_are_purge_candidates(self):
        self.assertEqual(
            ['dispersion_1', 'dispersion_23'],
            dispersion.object_dispersion_names(self.account_listing))

    def test_policy_zero_object_container_names(self):
        self.assertEqual(
            'dispersion_objects', dispersion.object_dispersion_container(0))
        self.assertEqual(
            ['dispersion_objects', 'dispersion_objects_0'],
            dispersion.object_dispersion_containers_to_purge(0))

    def test_nonzero_policy_object_container_names(self):
        self.assertEqual(
            'dispersion_objects_7',
            dispersion.object_dispersion_container(7))
        self.assertEqual(
            ['dispersion_objects_7'],
            dispersion.object_dispersion_containers_to_purge(7))


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
            with self.assertRaises(ValueError):
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

    def test_non_default_policy_skips_container_report(self):
        stderr = StringIO()
        with self.mock_report_functions(), \
                patch.object(dispersion_report, 'stderr', stderr):
            dispersion_report.generate_report(
                self._base_conf(), policy_name='unu')
        self.assertIn('Skipping container report', stderr.getvalue())
        self.assertFalse(self.mock_container_dispersion_report.called)
        self.assertTrue(self.mock_object_dispersion_report.called)

    def test_non_default_policy_runs_object_report_only(self):
        conf = dict(self._base_conf(), container_report='no')
        with self.mock_report_functions():
            dispersion_report.generate_report(conf, policy_name='unu')
        self.assertFalse(self.mock_container_dispersion_report.called)
        self.assertTrue(self.mock_object_dispersion_report.called)

    def test_section_policy_selects_object_report_policy(self):
        conf = dict(self._base_conf(), container_report='no',
                    policy_name='unu')
        with self.mock_report_functions():
            dispersion_report.generate_report(conf)
        self.assertFalse(self.mock_container_dispersion_report.called)
        self.assertEqual(
            self.mock_object_dispersion_report.call_args[0][-1].name,
            'unu')


@patch_policies
class TestDispersionCliMain(unittest.TestCase):
    """Tests for parsing dispersion command configuration and options."""

    def _write_config(self, contents):
        config_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        self.addCleanup(os.unlink, config_file.name)
        config_file.write(contents)
        config_file.close()
        return config_file.name

    def test_report_section_policy_merges_with_command_line_flags(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
container_report = yes

[object-unu]
policy_name = unu
object_report = yes
''')
        with patch.object(sys, 'argv', [
                'swift-dispersion-report', '--section', 'object-unu',
                '--object-only', '--partitions', config_file]), \
                patch.object(dispersion_report.patcher, 'monkey_patch'), \
                patch.object(dispersion_report.hubs.get_hub(),
                             'debug_exceptions', False), \
                patch.object(dispersion_report, 'generate_report') as report:
            dispersion_report.main()

        self.assertIsNone(report.call_args[0][1])
        self.assertEqual('unu', report.call_args[0][0]['policy_name'])
        self.assertEqual('no', report.call_args[0][0]['container_report'])
        self.assertEqual('yes', report.call_args[0][0]['object_report'])
        self.assertEqual('yes', report.call_args[0][0]['partitions'])

    def test_report_with_no_runnable_report_exits_cleanly(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
''')
        with patch.object(sys, 'argv', [
                'swift-dispersion-report', '--container-only',
                '--object-only', config_file]), \
                patch.object(dispersion_report.patcher, 'monkey_patch'), \
                patch.object(dispersion_report.hubs.get_hub(),
                             'debug_exceptions', False), \
                patch('swiftclient.get_auth') as get_auth:
            with self.assertRaises(SystemExit) as caught:
                dispersion_report.main()
        self.assertIn('Neither container nor object report',
                      str(caught.exception))
        get_auth.assert_not_called()

    def test_populate_non_default_container_exits_before_authentication(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
policy_name = unu
''')
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth') as get_auth:
            with self.assertRaises(SystemExit) as caught:
                dispersion_populate.main()

        self.assertIn('Container dispersion is shared', str(caught.exception))
        get_auth.assert_not_called()

    def test_populate_object_only_section_uses_selected_policy(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100

[object-unu]
policy_name = unu
''')
        connpool = make_connpool(containers=[
            {'name': 'dispersion_objects_0'},
            {'name': 'dispersion_objects_1'}])
        err = StringIO()
        object_ring = mock.Mock(partition_count=1)
        object_ring.get_part.return_value = 0
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--section', 'object-unu',
                '--object-only', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool'), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring',
                             return_value=object_ring) as ring, \
                patch.object(dispersion_populate, 'put_container') as put, \
                patch.object(sys, 'stderr', err):
            dispersion_populate.main()

        ring.assert_called_once_with('/etc/swift', ring_name='object-1')
        put.assert_called_once_with(
            mock.ANY, 'dispersion_objects_1', None,
            {'X-Storage-Policy': 'unu'})

        conn = connpool.item.return_value.__enter__.return_value
        conn.get_account.assert_called_once_with(
            prefix='dispersion_objects', full_listing=True)
        self.assertEqual('', err.getvalue())
        conn.get_container.assert_not_called()

    def test_populate_container_uses_pre_policy_name(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        connpool = make_connpool(containers=[{'name': 'dispersion_0_4'}])
        err = StringIO()
        container_ring = mock.Mock(partition_count=1)
        container_ring.get_part.return_value = 0
        coropool = mock.Mock()
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--container-only',
                config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=coropool), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring',
                             return_value=container_ring), \
                patch.object(dispersion_populate, 'sleep'), \
                patch.object(sys, 'stderr', err):
            dispersion_populate.main()

        coropool.spawn.assert_called_once_with(
            dispersion_populate.put_container, connpool,
            'dispersion_0', dispersion_populate.report, {})

        conn = connpool.item.return_value.__enter__.return_value
        conn.get_account.assert_called_once_with(
            prefix='dispersion_', full_listing=True)
        conn.delete_container.assert_not_called()
        self.assertIn('WARNING: Ignoring 1 obsolete container', err.getvalue())

    def test_populate_does_not_create_object_container_without_samples(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
''')
        object_ring = mock.Mock(partition_count=0)
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool'), \
                patch.object(dispersion_populate, 'Pool'), \
                patch.object(dispersion_populate, 'Ring',
                             return_value=object_ring), \
                patch.object(dispersion_populate, 'put_container') as put:
            dispersion_populate.main()

        put.assert_not_called()

    def test_purge_requires_one_resource_type(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
''')
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--purge', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth') as get_auth:
            with self.assertRaises(SystemExit) as caught:
                dispersion_populate.main()

        self.assertIn('exactly one', str(caught.exception))
        get_auth.assert_not_called()

    def test_purge_deletes_all_container_samples_before_population(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        account_listing = [
            {'name': 'dispersion_4'},
            {'name': 'dispersion_0_5'},
            {'name': 'dispersion_1_6'},
            {'name': 'dispersion_objects_0'},
        ]
        conn = mock.Mock()
        conn.get_account.return_value = (None, account_listing)
        conn.attempts = 1
        connpool = mock.MagicMock()
        connpool.item.return_value.__enter__.return_value = conn
        sample_ring = mock.Mock(partition_count=1)
        sample_ring.get_part.return_value = 0
        coropool = mock.Mock()
        coropool.spawn.side_effect = lambda func, *args: func(*args)
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--container-only',
                '--purge', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=coropool), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring',
                             return_value=sample_ring) as ring, \
                patch.object(dispersion_populate, 'sleep'):
            dispersion_populate.main()

        ring.assert_called_once_with('/etc/swift', ring_name='container')
        self.assertEqual([
            mock.call(dispersion_populate.delete_container, connpool,
                      'dispersion_4', mock.ANY),
            mock.call(dispersion_populate.delete_container, connpool,
                      'dispersion_0_5', mock.ANY),
            mock.call(dispersion_populate.delete_container, connpool,
                      'dispersion_1_6', mock.ANY),
            mock.call(dispersion_populate.put_container, connpool,
                      'dispersion_0', dispersion_populate.report, {}),
        ], coropool.spawn.call_args_list)
        self.assertEqual([
            mock.call('dispersion_4'),
            mock.call('dispersion_0_5'),
            mock.call('dispersion_1_6'),
        ], conn.delete_container.call_args_list)
        self.assertLess(
            conn.mock_calls.index(mock.call.delete_container(
                'dispersion_1_6')),
            conn.mock_calls.index(mock.call.put_container(
                'dispersion_0', headers={})))

        conn.get_account.assert_called_once_with(
            prefix='dispersion_', full_listing=True)
        sample_ring.get_nodes.assert_not_called()

    def test_purge_recreates_object_policy_samples_after_deletion(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        container_listing = [
            {'name': 'dispersion_4'},
            {'name': 'dispersion_19'},
            {'name': 'dispersion_bad'},
            {'name': 'unrelated'},
        ]
        conn = mock.Mock()
        conn.get_account.return_value = (None, [])
        conn.get_container.return_value = (None, container_listing)
        conn.attempts = 1
        connpool = mock.MagicMock()
        connpool.item.return_value.__enter__.return_value = conn
        sample_ring = mock.Mock(partition_count=1)
        sample_ring.get_part.return_value = 0
        coropool = mock.Mock()
        coropool.spawn.side_effect = lambda func, *args: func(*args)
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only',
                '--policy-name', 'unu', '--purge', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=coropool), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring',
                             return_value=sample_ring) as ring, \
                patch.object(dispersion_populate, 'sleep'):
            dispersion_populate.main()

        ring.assert_called_once_with('/etc/swift', ring_name='object-1')
        self.assertEqual([
            mock.call('dispersion_objects_1', 'dispersion_4'),
            mock.call('dispersion_objects_1', 'dispersion_19'),
        ], conn.delete_object.call_args_list)
        conn.delete_container.assert_called_once_with('dispersion_objects_1')
        conn.put_container.assert_called_once_with(
            'dispersion_objects_1', headers={'X-Storage-Policy': 'unu'})
        conn.put_object.assert_called_once_with(
            'dispersion_objects_1', 'dispersion_0', mock.ANY,
            headers={'x-object-meta-dispersion': 'dispersion_0'})
        self.assertLess(
            conn.mock_calls.index(mock.call.delete_container(
                'dispersion_objects_1')),
            conn.mock_calls.index(mock.call.put_container(
                'dispersion_objects_1', headers={'X-Storage-Policy': 'unu'})))

    def test_object_purge_requires_explicit_policy(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
''')
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only', '--purge',
                config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth') as get_auth:
            with self.assertRaises(SystemExit) as caught:
                dispersion_populate.main()

        self.assertIn('explicit policy_name', str(caught.exception))
        get_auth.assert_not_called()

    def test_policy_zero_purge_populates_canonical_samples(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        conn = mock.Mock()
        conn.get_account.return_value = (None, [])
        conn.get_container.side_effect = [
            (None, [{'name': 'dispersion_4'}]),
            (None, [{'name': 'dispersion_19'}]),
        ]
        conn.attempts = 1
        connpool = mock.MagicMock()
        connpool.item.return_value.__enter__.return_value = conn
        sample_ring = mock.Mock(partition_count=1)
        sample_ring.get_part.return_value = 0
        coropool = mock.Mock()
        coropool.spawn.side_effect = lambda func, *args: func(*args)
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only',
                '--policy-name', 'nulo', '--purge', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=coropool), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring',
                             return_value=sample_ring) as ring, \
                patch.object(dispersion_populate, 'sleep'):
            dispersion_populate.main()

        ring.assert_called_once_with('/etc/swift', ring_name='object')
        self.assertEqual([
            mock.call('dispersion_objects', 'dispersion_4'),
            mock.call('dispersion_objects_0', 'dispersion_19'),
        ], conn.delete_object.call_args_list)
        self.assertEqual([
            mock.call('dispersion_objects'),
            mock.call('dispersion_objects_0'),
        ], conn.delete_container.call_args_list)

        conn.put_container.assert_called_once_with(
            'dispersion_objects', headers={'X-Storage-Policy': 'nulo'})
        conn.put_object.assert_called_once_with(
            'dispersion_objects', 'dispersion_0', mock.ANY,
            headers={'x-object-meta-dispersion': 'dispersion_0'})
        self.assertLess(
            conn.mock_calls.index(mock.call.delete_container(
                'dispersion_objects_0')),
            conn.mock_calls.index(mock.call.put_container(
                'dispersion_objects', headers={'X-Storage-Policy': 'nulo'})))

        conn.get_account.assert_not_called()
        self.assertEqual([
            mock.call('dispersion_objects', prefix='dispersion_',
                      full_listing=True),
            mock.call('dispersion_objects_0', prefix='dispersion_',
                      full_listing=True),
        ], conn.get_container.call_args_list)

    def test_policy_zero_purge_populates_with_missing_obsolete_container(
            self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        conn = mock.Mock(attempts=1)
        conn.get_account.return_value = (None, [])
        conn.get_container.side_effect = [
            (None, [{'name': 'dispersion_4'}]),
            ClientException('missing', http_status=404),
        ]
        connpool = mock.MagicMock()
        connpool.item.return_value.__enter__.return_value = conn
        ring = mock.Mock(partition_count=1)
        ring.get_part.return_value = 0
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only',
                '--policy-name', 'nulo', '--purge', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=make_coropool()), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring', return_value=ring), \
                patch.object(dispersion_populate, 'sleep'):
            dispersion_populate.main()

        conn.delete_object.assert_called_once_with(
            'dispersion_objects', 'dispersion_4')
        conn.delete_container.assert_called_once_with('dispersion_objects')
        conn.put_container.assert_called_once_with(
            'dispersion_objects', headers={'X-Storage-Policy': 'nulo'})
        conn.put_object.assert_called_once_with(
            'dispersion_objects', 'dispersion_0', mock.ANY,
            headers={'x-object-meta-dispersion': 'dispersion_0'})

    def test_purge_does_not_populate_when_object_container_is_not_empty(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
''')
        conn = mock.Mock(attempts=1)
        conn.get_account.return_value = (None, [])
        conn.get_container.return_value = (None, [{'name': 'unexpected'}])
        conn.delete_container.side_effect = ClientException(
            'not empty', http_status=409)
        connpool = mock.MagicMock()
        connpool.item.return_value.__enter__.return_value = conn
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only',
                '--policy-name', 'unu', '--purge', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=make_coropool()), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring') as ring:
            with self.assertRaises(ClientException) as caught:
                dispersion_populate.main()

        self.assertEqual(409, caught.exception.http_status)
        conn.delete_object.assert_not_called()
        conn.put_container.assert_not_called()
        conn.put_object.assert_not_called()
        ring.assert_not_called()

    def test_purge_worker_failure_prevents_population(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
''')
        # Use the real pool: waitall may finish without raising a worker's
        # exception, so the purge callback must record failure for main.
        for resource in ('container', 'object'):
            with self.subTest(resource=resource):
                conn = mock.Mock(attempts=1)
                conn.get_account.return_value = (
                    None, [{'name': 'dispersion_0'}])
                conn.get_container.return_value = (
                    None, [{'name': 'dispersion_0'}])
                conn.delete_container.side_effect = ClientException(
                    'delete failed', http_status=503)
                conn.delete_object.side_effect = ClientException(
                    'delete failed', http_status=503)
                connpool = mock.MagicMock()
                connpool.item.return_value.__enter__.return_value = conn
                with patch.object(sys, 'argv', [
                        'swift-dispersion-populate', '--%s-only' % resource,
                        '--policy-name', 'nulo', '--purge', config_file]), \
                        patch.object(dispersion_populate.patcher,
                                     'monkey_patch'), \
                        patch('swiftclient.get_auth', return_value=(
                            'http://example.com/v1/AUTH_test', 'token')), \
                        patch.object(dispersion_populate, 'Pool',
                                     return_value=connpool), \
                        patch.object(dispersion_populate, 'Ring') as ring, \
                        patch.object(dispersion_populate.traceback,
                                     'print_exc'):
                    with self.assertRaises(SystemExit) as caught:
                        dispersion_populate.main()
                self.assertIn('Gave up', str(caught.exception))
                conn.put_container.assert_not_called()
                conn.put_object.assert_not_called()
                ring.assert_not_called()

    def test_container_no_overlap_populates_only_uncovered_partitions(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        connpool = make_connpool(containers=[
            {'name': 'dispersion_4'}, {'name': 'dispersion_0_5'}])
        ring = mock.Mock(partition_count=2)
        ring.get_nodes.return_value = (0, [])
        ring.get_part.side_effect = [0, 1]
        coropool = mock.Mock()
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--container-only',
                '--no-overlap', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=coropool), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring', return_value=ring), \
                patch.object(dispersion_populate, 'sleep'):
            dispersion_populate.main()
        ring.get_nodes.assert_called_once_with('AUTH_test', 'dispersion_4')
        coropool.spawn.assert_called_once_with(
            dispersion_populate.put_container, connpool, 'dispersion_1',
            dispersion_populate.report, {})

    def test_object_no_overlap_ignores_obsolete_samples(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        conn = mock.Mock()
        conn.get_account.return_value = (None, [
            {'name': 'dispersion_objects'}, {'name': 'dispersion_objects_0'}])
        conn.get_container.side_effect = [
            (None, [{'name': 'dispersion_4'}]),
            (None, [{'name': 'dispersion_19'}]),
        ]
        connpool = mock.MagicMock()
        connpool.item.return_value.__enter__.return_value = conn
        ring = mock.Mock(partition_count=3)
        ring.get_part.side_effect = [0, 0, 1, 2]
        coropool = mock.Mock()
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only',
                '--no-overlap', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=coropool), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring', return_value=ring), \
                patch.object(dispersion_populate, 'put_container') as put, \
                patch.object(dispersion_populate, 'sleep'):
            dispersion_populate.main()
        self.assertEqual(
            mock.call('AUTH_test', 'dispersion_objects', 'dispersion_4'),
            ring.get_part.call_args_list[0])
        conn.get_container.assert_called_once_with(
            'dispersion_objects', prefix='dispersion_', full_listing=True)
        put.assert_called_once_with(
            connpool, 'dispersion_objects', None,
            {'X-Storage-Policy': 'nulo'})
        self.assertEqual([
            mock.call(dispersion_populate.put_object, connpool,
                      'dispersion_objects', 'dispersion_1',
                      dispersion_populate.report),
            mock.call(dispersion_populate.put_object, connpool,
                      'dispersion_objects', 'dispersion_2',
                      dispersion_populate.report),
        ], coropool.spawn.call_args_list)

        conn.get_account.assert_called_once_with(
            prefix='dispersion_objects', full_listing=True)
        conn.delete_container.assert_not_called()
        conn.delete_object.assert_not_called()

    def test_purge_and_no_overlap_are_rejected_before_authentication(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
''')
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--container-only',
                '--purge', '--no-overlap', config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth') as get_auth:
            with self.assertRaises(SystemExit) as caught:
                dispersion_populate.main()
        self.assertIn('mutually exclusive', str(caught.exception))
        get_auth.assert_not_called()

    def test_zero_coverage_creates_nothing_and_purge_removes_existing_samples(
            self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 0
''')
        for resource in ('container', 'object'):
            for purge in (False, True):
                with self.subTest(resource=resource, purge=purge):
                    conn = mock.Mock(attempts=1)
                    names = (['dispersion_4', 'dispersion_1_5']
                             if resource == 'container' else
                             ['dispersion_objects', 'dispersion_objects_0'])
                    conn.get_account.return_value = (
                        None, [{'name': name} for name in names])
                    conn.get_container.return_value = (
                        None, [{'name': 'dispersion_4'}])
                    connpool = mock.MagicMock()
                    connpool.item.return_value.__enter__.return_value = conn
                    argv = ['swift-dispersion-populate',
                            '--%s-only' % resource, '--policy-name', 'nulo',
                            config_file]
                    if purge:
                        argv.append('--purge')
                    err = StringIO()
                    with patch.object(sys, 'argv', argv), \
                            patch.object(dispersion_populate.patcher,
                                         'monkey_patch'), \
                            patch('swiftclient.get_auth', return_value=(
                                'http://example.com/v1/AUTH_test', 'token')), \
                            patch.object(dispersion_populate, 'GreenPool',
                                         return_value=make_coropool()), \
                            patch.object(dispersion_populate, 'Pool',
                                         return_value=connpool), \
                            patch.object(dispersion_populate,
                                         'Ring') as ring, \
                            patch.object(dispersion_populate, 'sleep'), \
                            patch.object(sys, 'stderr', err):
                        dispersion_populate.main()
                    conn.put_container.assert_not_called()
                    conn.put_object.assert_not_called()
                    ring.assert_not_called()
                    if purge:
                        self.assertEqual('', err.getvalue())
                        if resource == 'object':
                            conn.get_account.assert_not_called()
                    else:
                        self.assertIn(
                            'WARNING: Ignoring 1 obsolete %s' % resource,
                            err.getvalue())
                    self.assertNotIn('swift-dispersion-populate',
                                     err.getvalue())
                    if purge:
                        self.assertEqual([mock.call(name) for name in names],
                                         conn.delete_container.call_args_list)
                        if resource == 'object':
                            self.assertEqual([
                                mock.call(name, 'dispersion_4')
                                for name in names
                            ], conn.delete_object.call_args_list)
                    else:
                        conn.delete_container.assert_not_called()
                        conn.delete_object.assert_not_called()

    def test_object_no_overlap_creates_no_container_when_fully_covered(self):
        config_file = self._write_config('''
[dispersion]
auth_url = http://example.com/auth
auth_user = user
auth_key = key
dispersion_coverage = 100
''')
        connpool = make_connpool(
            containers=[{'name': 'dispersion_objects'}],
            objects=[{'name': 'dispersion_4'}])
        ring = mock.Mock(partition_count=1)
        ring.get_part.return_value = 0
        coropool = mock.Mock()
        with patch.object(sys, 'argv', [
                'swift-dispersion-populate', '--object-only', '--no-overlap',
                config_file]), \
                patch.object(dispersion_populate.patcher, 'monkey_patch'), \
                patch('swiftclient.get_auth', return_value=(
                    'http://example.com/v1/AUTH_test', 'token')), \
                patch.object(dispersion_populate, 'GreenPool',
                             return_value=coropool), \
                patch.object(dispersion_populate, 'Pool',
                             return_value=connpool), \
                patch.object(dispersion_populate, 'Ring', return_value=ring), \
                patch.object(dispersion_populate, 'put_container') as put:
            dispersion_populate.main()
        put.assert_not_called()
        coropool.spawn.assert_not_called()


class TestContainerDispersionReport(unittest.TestCase):

    def setUp(self):
        self._orig_json_output = dispersion_report.json_output
        dispersion_report.json_output = True

    def tearDown(self):
        dispersion_report.json_output = self._orig_json_output

    def test_no_canonical_containers_exits_with_error(self):
        with self.assertRaises(SystemExit) as caught:
            dispersion_report.container_dispersion_report(
                make_coropool(), make_connpool(containers=[]),
                'AUTH_test', make_ring(), 3, False)
        self.assertIn('ERROR: No canonical container', str(caught.exception))

    @mock.patch('swift.common.direct_client.retry')
    def test_policy_indexed_samples_warn_without_migration_commands(
            self, mock_retry):
        mock_retry.return_value = (1, None)
        ring = make_ring()
        err = StringIO()
        with patch.object(sys, 'stderr', err):
            dispersion_report.container_dispersion_report(
                make_coropool(), make_connpool(
                    containers=[{'name': 'dispersion_1'},
                                {'name': 'dispersion_0_2'},
                                {'name': 'dispersion_1_3'}]),
                'AUTH_test', ring, 3, False)

        ring.get_nodes.assert_called_once_with('AUTH_test', 'dispersion_1')
        self.assertEqual(3, mock_retry.call_count)
        self.assertIn(
            'WARNING: Ignoring 2 obsolete container dispersion names;',
            err.getvalue())
        self.assertNotIn('swift-dispersion-populate', err.getvalue())
        self.assertIn(
            'use populate with --purge to replace the population and '
            'remove obsolete data.', err.getvalue())

    @mock.patch('swift.common.direct_client.retry')
    def test_canonical_samples_do_not_warn(self, mock_retry):
        mock_retry.return_value = (1, None)
        err = StringIO()
        with patch.object(sys, 'stderr', err):
            dispersion_report.container_dispersion_report(
                make_coropool(), make_connpool(
                    containers=[{'name': 'dispersion_1'}]),
                'AUTH_test', make_ring(), 3, False)

        self.assertEqual('', err.getvalue())

    @mock.patch('swift.common.direct_client.retry')
    def test_all_copies_found(self, mock_retry):
        mock_retry.return_value = (1, None)
        nodes = make_nodes(3)
        containers = [{'name': 'dispersion_1'},
                      {'name': 'dispersion_0_2'}]
        result = dispersion_report.container_dispersion_report(
            make_coropool(), make_connpool(containers=containers),
            'AUTH_test', make_ring(nodes=nodes), 3, False)
        self.assertEqual(result['copies_found'], result['copies_expected'])
        self.assertEqual(result['pct_found'], 100.0)
        self.assertEqual(result['retries'], 0)

    @mock.patch('swift.common.direct_client.retry')
    def test_missing_copies_tracked(self, mock_retry):
        mock_retry.side_effect = Exception('node down')
        containers = [{'name': 'dispersion_1'}]
        result = dispersion_report.container_dispersion_report(
            make_coropool(), make_connpool(containers=containers),
            'AUTH_test', make_ring(nodes=make_nodes(3)), 3, False)
        self.assertEqual(result['copies_found'], 0)
        self.assertIn('missing_3', result)

    @mock.patch('swift.common.direct_client.retry')
    def test_obsolete_only_population_warns_and_exits_without_backend_checks(
            self, mock_retry):
        err = StringIO()
        with patch.object(sys, 'stderr', err):
            with self.assertRaises(SystemExit) as caught:
                dispersion_report.container_dispersion_report(
                    make_coropool(), make_connpool(containers=[
                        {'name': 'dispersion_0_1'},
                        {'name': 'dispersion_7_2'}]),
                    'AUTH_test', make_ring(), 3, False)
        self.assertIn('WARNING: Ignoring 2 obsolete container', err.getvalue())
        self.assertIn('ERROR: No canonical container', str(caught.exception))
        mock_retry.assert_not_called()


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

    def test_no_canonical_objects_exits_with_error(self):
        with self.assertRaises(SystemExit) as caught:
            dispersion_report.object_dispersion_report(
                make_coropool(), make_connpool(objects=[]),
                'AUTH_test', make_ring(), 3, False, self._policy())
        self.assertIn('ERROR: No canonical object', str(caught.exception))

    @mock.patch('swift.common.direct_client.retry')
    def test_all_copies_found(self, mock_retry):
        mock_retry.return_value = (1, None)
        nodes = make_nodes(3)
        objects = [{'name': 'dispersion_1'},
                   {'name': 'dispersion_2'}]
        result = dispersion_report.object_dispersion_report(
            make_coropool(), make_connpool(objects=objects),
            'AUTH_test', make_ring(nodes=nodes), 3, False, self._policy())
        self.assertEqual(result['copies_found'], result['copies_expected'])
        self.assertEqual(result['pct_found'], 100.0)

    @mock.patch('swift.common.direct_client.retry')
    def test_missing_copies_tracked(self, mock_retry):
        mock_retry.side_effect = Exception('node down')
        objects = [{'name': 'dispersion_1'}]
        result = dispersion_report.object_dispersion_report(
            make_coropool(), make_connpool(objects=objects),
            'AUTH_test', make_ring(nodes=make_nodes(3)), 3, False,
            self._policy())
        self.assertEqual(result['copies_found'], 0)
        self.assertIn('missing_3', result)

    @mock.patch('swift.common.direct_client.retry')
    def test_policy_zero_reports_canonical_samples_and_warns_about_old_names(
            self, mock_retry):
        mock_retry.return_value = (1, None)
        conn = mock.Mock()
        conn.get_account.return_value = (None, [
            {'name': 'dispersion_objects'}, {'name': 'dispersion_objects_0'}])
        listings = {
            'dispersion_objects': [{'name': 'dispersion_1'}],
            'dispersion_objects_0': [{'name': 'dispersion_2'}],
        }
        conn.get_container.side_effect = lambda container, **kwargs: (
            None, listings[container])
        connpool = mock.MagicMock()
        connpool.item.return_value.__enter__.return_value = conn
        ring = make_ring()
        ring.get_nodes.side_effect = lambda account, container, obj: (
            0 if container == 'dispersion_objects' else 1, make_nodes())

        err = StringIO()
        with patch.object(sys, 'stderr', err):
            result = dispersion_report.object_dispersion_report(
                make_coropool(), connpool, 'AUTH_test', ring, 3, False,
                self._policy())

        conn.get_container.assert_called_once_with(
            'dispersion_objects', prefix='dispersion_', full_listing=True)
        self.assertEqual(3, result['copies_expected'])
        self.assertEqual(
            ['dispersion_objects'] * 3,
            [call[0][4] for call in mock_retry.call_args_list])
        self.assertIn(
            'WARNING: Ignoring 1 obsolete object dispersion names;',
            err.getvalue())
        self.assertNotIn('swift-dispersion-populate', err.getvalue())
        self.assertIn(
            'use populate with --purge to replace the population and '
            'remove obsolete data.', err.getvalue())

    @mock.patch('swift.common.direct_client.retry')
    def test_obsolete_only_population_warns_and_exits_without_backend_checks(
            self, mock_retry):
        connpool = make_connpool(
            containers=[{'name': 'dispersion_objects_0'}])
        conn = connpool.item.return_value.__enter__.return_value
        conn.get_container.side_effect = ClientException(
            'missing canonical container', http_status=404)
        err = StringIO()
        with patch.object(sys, 'stderr', err):
            with self.assertRaises(SystemExit) as caught:
                dispersion_report.object_dispersion_report(
                    make_coropool(), connpool, 'AUTH_test', make_ring(),
                    3, False, self._policy())
        self.assertIn('WARNING: Ignoring 1 obsolete object', err.getvalue())
        self.assertIn('ERROR: No canonical object', str(caught.exception))
        conn.get_container.assert_called_once_with(
            'dispersion_objects', prefix='dispersion_', full_listing=True)
        mock_retry.assert_not_called()

    @mock.patch('swift.common.direct_client.retry')
    def test_nonzero_policy_indexed_container_is_canonical(self, mock_retry):
        mock_retry.return_value = (1, None)
        connpool = make_connpool(
            containers=[{'name': 'dispersion_objects_0'},
                        {'name': 'dispersion_objects_7'}],
            objects=[{'name': 'dispersion_1'}])
        err = StringIO()
        with patch.object(sys, 'stderr', err):
            result = dispersion_report.object_dispersion_report(
                make_coropool(), connpool, 'AUTH_test', make_ring(),
                3, False, self._policy('ec', 7))
        self.assertEqual(3, result['copies_expected'])
        self.assertEqual('', err.getvalue())
        self.assertEqual(['dispersion_objects_7'] * 3,
                         [call[0][4] for call in mock_retry.call_args_list])


if __name__ == '__main__':
    unittest.main()
