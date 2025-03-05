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

import json
import os
import sys
import unittest
from argparse import Namespace
from textwrap import dedent

from unittest import mock
from shutil import rmtree
from tempfile import mkdtemp

from io import StringIO

from swift.cli.manage_shard_ranges import main
from swift.common import utils
from swift.common.utils import Timestamp, ShardRange
from swift.container.backend import ContainerBroker
from swift.container.sharder import make_shard_ranges
from test.unit import mock_timestamp_now, make_timestamp_iter, with_tempdir


class TestManageShardRanges(unittest.TestCase):
    def setUp(self):
        self.ts_iter = make_timestamp_iter()
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_cli_find_shards')
        utils.mkdirs(self.testdir)
        rmtree(self.testdir)
        self.shard_data = [
            {'index': 0, 'lower': '', 'upper': 'obj09',
             'object_count': 10},
            {'index': 1, 'lower': 'obj09', 'upper': 'obj19',
             'object_count': 10},
            {'index': 2, 'lower': 'obj19', 'upper': 'obj29',
             'object_count': 10},
            {'index': 3, 'lower': 'obj29', 'upper': 'obj39',
             'object_count': 10},
            {'index': 4, 'lower': 'obj39', 'upper': 'obj49',
             'object_count': 10},
            {'index': 5, 'lower': 'obj49', 'upper': 'obj59',
             'object_count': 10},
            {'index': 6, 'lower': 'obj59', 'upper': 'obj69',
             'object_count': 10},
            {'index': 7, 'lower': 'obj69', 'upper': 'obj79',
             'object_count': 10},
            {'index': 8, 'lower': 'obj79', 'upper': 'obj89',
             'object_count': 10},
            {'index': 9, 'lower': 'obj89', 'upper': '',
             'object_count': 10},
        ]

        self.overlap_shard_data_1 = [
            {'index': 0, 'lower': '', 'upper': 'obj10',
             'object_count': 1},
            {'index': 1, 'lower': 'obj10', 'upper': 'obj20',
             'object_count': 1},
            {'index': 2, 'lower': 'obj20', 'upper': 'obj30',
             'object_count': 1},
            {'index': 3, 'lower': 'obj30', 'upper': 'obj39',
             'object_count': 1},
            {'index': 4, 'lower': 'obj39', 'upper': 'obj49',
             'object_count': 1},
            {'index': 5, 'lower': 'obj49', 'upper': 'obj58',
             'object_count': 1},
            {'index': 6, 'lower': 'obj58', 'upper': 'obj68',
             'object_count': 1},
            {'index': 7, 'lower': 'obj68', 'upper': 'obj78',
             'object_count': 1},
            {'index': 8, 'lower': 'obj78', 'upper': 'obj88',
             'object_count': 1},
            {'index': 9, 'lower': 'obj88', 'upper': '',
             'object_count': 1},
        ]

        self.overlap_shard_data_2 = [
            {'index': 0, 'lower': '', 'upper': 'obj11', 'object_count': 1},
            {'index': 1, 'lower': 'obj11', 'upper': 'obj21',
             'object_count': 1},
        ]

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir))

    def assert_shard_ranges_equal(self, expected, actual):
        self.assertEqual([dict(sr) for sr in expected],
                         [dict(sr) for sr in actual])

    def assert_starts_with(self, value, prefix):
        self.assertTrue(value.startswith(prefix),
                        "%r does not start with %r" % (value, prefix))

    def assert_formatted_json(self, output, expected):
        try:
            loaded = json.loads(output)
        except ValueError as err:
            self.fail('Invalid JSON: %s\n%r' % (err, output))
        # Check this one first, for a prettier diff
        self.assertEqual(loaded, expected)
        formatted = json.dumps(expected, sort_keys=True, indent=2) + '\n'
        self.assertEqual(output, formatted)

    def _make_broker(self, account='a', container='c',
                     device='sda', part=0):
        datadir = os.path.join(
            self.testdir, device, 'containers', str(part), 'ash', 'hash')
        db_file = os.path.join(datadir, 'hash.db')
        broker = ContainerBroker(
            db_file, account=account, container=container)
        broker.initialize()
        return broker

    def _move_broker_to_sharded_state(self, broker):
        epoch = Timestamp.now()
        broker.enable_sharding(epoch)
        self.assertTrue(broker.set_sharding_state())
        self.assertTrue(broker.set_sharded_state())
        own_sr = broker.get_own_shard_range()
        own_sr.update_state(ShardRange.SHARDED, epoch)
        broker.merge_shard_ranges([own_sr])
        return epoch

    def test_conf_file_options(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file, account='a', container='c')
        broker.initialize()

        conf = """
        [container-sharder]
        shrink_threshold = 150
        expansion_limit = 650
        shard_container_threshold = 1000
        rows_per_shard = 600
        max_shrinking = 33
        max_expanding = 31
        minimum_shard_size = 88
        """

        conf_file = os.path.join(self.testdir, 'sharder.conf')
        with open(conf_file, 'w') as fd:
            fd.write(dedent(conf))

        # default values
        with mock.patch('swift.cli.manage_shard_ranges.find_ranges',
                        return_value=0) as mocked:
            ret = main([db_file, 'find'])
        self.assertEqual(0, ret)
        expected = Namespace(conf_file=None,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             rows_per_shard=500000,
                             subcommand='find',
                             force_commits=False,
                             verbose=0,
                             minimum_shard_size=100000)
        mocked.assert_called_once_with(mock.ANY, expected)

        # conf file
        with mock.patch('swift.cli.manage_shard_ranges.find_ranges',
                        return_value=0) as mocked:
            ret = main([db_file, '--config', conf_file, 'find'])
        self.assertEqual(0, ret)
        expected = Namespace(conf_file=conf_file,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             rows_per_shard=600,
                             subcommand='find',
                             force_commits=False,
                             verbose=0,
                             minimum_shard_size=88)
        mocked.assert_called_once_with(mock.ANY, expected)

        # cli options override conf file
        with mock.patch('swift.cli.manage_shard_ranges.find_ranges',
                        return_value=0) as mocked:
            ret = main([db_file, '--config', conf_file, 'find', '12345',
                        '--minimum-shard-size', '99'])
        self.assertEqual(0, ret)
        expected = Namespace(conf_file=conf_file,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             rows_per_shard=12345,
                             subcommand='find',
                             force_commits=False,
                             verbose=0,
                             minimum_shard_size=99)
        mocked.assert_called_once_with(mock.ANY, expected)

        # default values
        with mock.patch('swift.cli.manage_shard_ranges.compact_shard_ranges',
                        return_value=0) as mocked:
            ret = main([db_file, 'compact'])
        self.assertEqual(0, ret)
        expected = Namespace(conf_file=None,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             subcommand='compact',
                             force_commits=False,
                             verbose=0,
                             max_expanding=-1,
                             max_shrinking=1,
                             shrink_threshold=100000,
                             expansion_limit=750000,
                             yes=False,
                             dry_run=False)
        mocked.assert_called_once_with(mock.ANY, expected)

        # conf file
        with mock.patch('swift.cli.manage_shard_ranges.compact_shard_ranges',
                        return_value=0) as mocked:
            ret = main([db_file, '--config', conf_file, 'compact'])
        self.assertEqual(0, ret)
        expected = Namespace(conf_file=conf_file,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             subcommand='compact',
                             force_commits=False,
                             verbose=0,
                             max_expanding=31,
                             max_shrinking=33,
                             shrink_threshold=150,
                             expansion_limit=650,
                             yes=False,
                             dry_run=False)
        mocked.assert_called_once_with(mock.ANY, expected)

        # cli options
        with mock.patch('swift.cli.manage_shard_ranges.compact_shard_ranges',
                        return_value=0) as mocked:
            ret = main([db_file, '--config', conf_file, 'compact',
                        '--max-shrinking', '22',
                        '--max-expanding', '11',
                        '--expansion-limit', '3456',
                        '--shrink-threshold', '1234'])
        self.assertEqual(0, ret)
        expected = Namespace(conf_file=conf_file,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             subcommand='compact',
                             force_commits=False,
                             verbose=0,
                             max_expanding=11,
                             max_shrinking=22,
                             shrink_threshold=1234,
                             expansion_limit=3456,
                             yes=False,
                             dry_run=False)
        mocked.assert_called_once_with(mock.ANY, expected)

    def test_conf_file_deprecated_options(self):
        # verify that deprecated percent-based do get applied
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file, account='a', container='c')
        broker.initialize()

        conf = """
        [container-sharder]
        shard_shrink_point = 15
        shard_shrink_merge_point = 65
        shard_container_threshold = 1000
        max_shrinking = 33
        max_expanding = 31
        """

        conf_file = os.path.join(self.testdir, 'sharder.conf')
        with open(conf_file, 'w') as fd:
            fd.write(dedent(conf))

        with mock.patch('swift.cli.manage_shard_ranges.compact_shard_ranges',
                        return_value=0) as mocked:
            ret = main([db_file, '--config', conf_file, 'compact'])
        self.assertEqual(0, ret)
        expected = Namespace(conf_file=conf_file,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             subcommand='compact',
                             force_commits=False,
                             verbose=0,
                             max_expanding=31,
                             max_shrinking=33,
                             shrink_threshold=150,
                             expansion_limit=650,
                             yes=False,
                             dry_run=False)
        mocked.assert_called_once_with(mock.ANY, expected)

        # absolute value options take precedence if specified in the conf file
        conf = """
        [container-sharder]
        shard_shrink_point = 15
        shrink_threshold = 123
        shard_shrink_merge_point = 65
        expansion_limit = 456
        shard_container_threshold = 1000
        max_shrinking = 33
        max_expanding = 31
        """

        conf_file = os.path.join(self.testdir, 'sharder.conf')
        with open(conf_file, 'w') as fd:
            fd.write(dedent(conf))

        with mock.patch('swift.cli.manage_shard_ranges.compact_shard_ranges') \
                as mocked:
            main([db_file, '--config', conf_file, 'compact'])
        expected = Namespace(conf_file=conf_file,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             subcommand='compact',
                             force_commits=False,
                             verbose=0,
                             max_expanding=31,
                             max_shrinking=33,
                             shrink_threshold=123,
                             expansion_limit=456,
                             yes=False,
                             dry_run=False)
        mocked.assert_called_once_with(mock.ANY, expected)

        # conf file - small percentages resulting in zero absolute values
        # should be respected rather than falling back to defaults, to avoid
        # nasty surprises
        conf = """
        [container-sharder]
        shard_shrink_point = 1
        shard_shrink_merge_point = 2
        shard_container_threshold = 10
        max_shrinking = 33
        max_expanding = 31
        """
        conf_file = os.path.join(self.testdir, 'sharder.conf')
        with open(conf_file, 'w') as fd:
            fd.write(dedent(conf))

        with mock.patch('swift.cli.manage_shard_ranges.compact_shard_ranges') \
                as mocked:
            main([db_file, '--config', conf_file, 'compact'])
        expected = Namespace(conf_file=conf_file,
                             path_to_file=mock.ANY,
                             func=mock.ANY,
                             subcommand='compact',
                             force_commits=False,
                             verbose=0,
                             max_expanding=31,
                             max_shrinking=33,
                             shrink_threshold=0,
                             expansion_limit=0,
                             yes=False,
                             dry_run=False)
        mocked.assert_called_once_with(mock.ANY, expected)

    def test_conf_file_invalid(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file, account='a', container='c')
        broker.initialize()

        # conf file - invalid value for shard_container_threshold
        conf = """
        [container-sharder]
        shrink_threshold = 1
        expansion_limit = 2
        shard_container_threshold = 0
        max_shrinking = 33
        max_expanding = 31
        """
        conf_file = os.path.join(self.testdir, 'sharder.conf')
        with open(conf_file, 'w') as fd:
            fd.write(dedent(conf))

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([db_file, '--config', conf_file, 'compact'])
        self.assertEqual(2, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Error loading config')
        self.assertIn('shard_container_threshold', err_lines[0])

    def test_conf_file_invalid_deprecated_options(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file, account='a', container='c')
        broker.initialize()

        # conf file - invalid value for shard_container_threshold
        conf = """
        [container-sharder]
        shard_shrink_point = -1
        shard_shrink_merge_point = 2
        shard_container_threshold = 1000
        max_shrinking = 33
        max_expanding = 31
        """
        conf_file = os.path.join(self.testdir, 'sharder.conf')
        with open(conf_file, 'w') as fd:
            fd.write(dedent(conf))

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            main([db_file, '--config', conf_file, 'compact'])
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Error loading config')
        self.assertIn('shard_shrink_point', err_lines[0])

    def test_conf_file_does_not_exist(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file, account='a', container='c')
        broker.initialize()
        conf_file = os.path.join(self.testdir, 'missing_sharder.conf')
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([db_file, '--config', conf_file, 'compact'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Error opening config file')

    def test_find_shard_ranges(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file)
        broker.account = 'a'
        broker.container = 'c'
        broker.initialize()
        ts = utils.Timestamp.now()
        broker.merge_items([
            {'name': 'obj%02d' % i, 'created_at': ts.internal, 'size': 0,
             'content_type': 'application/octet-stream', 'etag': 'not-really',
             'deleted': 0, 'storage_policy_index': 0,
             'ctype_timestamp': ts.internal, 'meta_timestamp': ts.internal}
            for i in range(100)])

        # Default uses a large enough value that sharding isn't required
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([db_file, 'find'])
        self.assertEqual(0, ret)
        self.assert_formatted_json(out.getvalue(), [])
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assert_starts_with(err_lines[1], 'Found 0 ranges in ')

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([db_file, 'find', '100'])
        self.assertEqual(0, ret)
        self.assert_formatted_json(out.getvalue(), [])
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assert_starts_with(err_lines[1], 'Found 0 ranges in ')

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([db_file, 'find', '99', '--minimum-shard-size', '1'])
        self.assertEqual(0, ret)
        self.assert_formatted_json(out.getvalue(), [
            {'index': 0, 'lower': '', 'upper': 'obj98', 'object_count': 99},
            {'index': 1, 'lower': 'obj98', 'upper': '', 'object_count': 1},
        ])
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assert_starts_with(err_lines[1], 'Found 2 ranges in ')

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([db_file, 'find', '10'])
        self.assertEqual(0, ret)
        self.assert_formatted_json(out.getvalue(), [
            {'index': 0, 'lower': '', 'upper': 'obj09', 'object_count': 10},
            {'index': 1, 'lower': 'obj09', 'upper': 'obj19',
             'object_count': 10},
            {'index': 2, 'lower': 'obj19', 'upper': 'obj29',
             'object_count': 10},
            {'index': 3, 'lower': 'obj29', 'upper': 'obj39',
             'object_count': 10},
            {'index': 4, 'lower': 'obj39', 'upper': 'obj49',
             'object_count': 10},
            {'index': 5, 'lower': 'obj49', 'upper': 'obj59',
             'object_count': 10},
            {'index': 6, 'lower': 'obj59', 'upper': 'obj69',
             'object_count': 10},
            {'index': 7, 'lower': 'obj69', 'upper': 'obj79',
             'object_count': 10},
            {'index': 8, 'lower': 'obj79', 'upper': 'obj89',
             'object_count': 10},
            {'index': 9, 'lower': 'obj89', 'upper': '', 'object_count': 10},
        ])
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assert_starts_with(err_lines[1], 'Found 10 ranges in ')

    def test_find_shard_ranges_with_minimum_size(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file)
        broker.account = 'a'
        broker.container = 'c'
        broker.initialize()
        ts = utils.Timestamp.now()
        # with 105 objects and rows_per_shard = 50 there is the potential for a
        # tail shard of size 5
        broker.merge_items([
            {'name': 'obj%03d' % i, 'created_at': ts.internal, 'size': 0,
             'content_type': 'application/octet-stream', 'etag': 'not-really',
             'deleted': 0, 'storage_policy_index': 0,
             'ctype_timestamp': ts.internal, 'meta_timestamp': ts.internal}
            for i in range(105)])

        def assert_tail_shard_not_extended(minimum):
            out = StringIO()
            err = StringIO()
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                ret = main([db_file, 'find', '50',
                            '--minimum-shard-size', str(minimum)])
            self.assertEqual(0, ret)
            self.assert_formatted_json(out.getvalue(), [
                {'index': 0, 'lower': '', 'upper': 'obj049',
                 'object_count': 50},
                {'index': 1, 'lower': 'obj049', 'upper': 'obj099',
                 'object_count': 50},
                {'index': 2, 'lower': 'obj099', 'upper': '',
                 'object_count': 5},
            ])
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            self.assert_starts_with(err_lines[1], 'Found 3 ranges in ')

        # tail shard size > minimum
        assert_tail_shard_not_extended(1)
        assert_tail_shard_not_extended(4)
        assert_tail_shard_not_extended(5)

        def assert_tail_shard_extended(minimum):
            out = StringIO()
            err = StringIO()
            if minimum is not None:
                extra_args = ['--minimum-shard-size', str(minimum)]
            else:
                extra_args = []
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                ret = main([db_file, 'find', '50'] + extra_args)
            self.assertEqual(0, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_formatted_json(out.getvalue(), [
                {'index': 0, 'lower': '', 'upper': 'obj049',
                 'object_count': 50},
                {'index': 1, 'lower': 'obj049', 'upper': '',
                 'object_count': 55},
            ])
            self.assert_starts_with(err_lines[1], 'Found 2 ranges in ')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')

        # sanity check - no minimum specified, defaults to rows_per_shard/5
        assert_tail_shard_extended(None)
        assert_tail_shard_extended(6)
        assert_tail_shard_extended(50)

        def assert_too_large_value_handled(minimum):
            out = StringIO()
            err = StringIO()
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                ret = main([db_file, 'find', '50',
                            '--minimum-shard-size', str(minimum)])
            self.assertEqual(2, ret)
            self.assertEqual(
                'Invalid config: minimum_shard_size (%s) must be <= '
                'rows_per_shard (50)' % minimum, err.getvalue().strip())

        assert_too_large_value_handled(51)
        assert_too_large_value_handled(52)

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with self.assertRaises(SystemExit):
                main([db_file, 'find', '50', '--minimum-shard-size', '-1'])

    def test_info(self):
        broker = self._make_broker()
        ts = next(self.ts_iter)
        broker.merge_items([
            {'name': 'obj%02d' % i, 'created_at': ts.internal, 'size': 9,
             'content_type': 'application/octet-stream', 'etag': 'not-really',
             'deleted': 0, 'storage_policy_index': 0,
             'ctype_timestamp': ts.internal, 'meta_timestamp': ts.internal}
            for i in range(100)])
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'info'])
        self.assertEqual(0, ret)
        expected = ['Sharding enabled = True',
                    'Own shard range: None',
                    'db_state = unsharded',
                    'object_count = 100',
                    'bytes_used = 900',
                    'Metadata:',
                    '  X-Container-Sysmeta-Sharding = True']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())

        retiring_db_id = broker.get_info()['id']
        broker.merge_shard_ranges(ShardRange('.shards/cc', Timestamp.now()))
        epoch = Timestamp.now()
        with mock_timestamp_now(epoch) as now:
            broker.enable_sharding(epoch)
        self.assertTrue(broker.set_sharding_state())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with mock_timestamp_now(now):
                ret = main([broker.db_file, 'info'])
        self.assertEqual(0, ret)
        expected = ['Sharding enabled = True',
                    'Own shard range: {',
                    '  "bytes_used": 0,',
                    '  "deleted": 0,',
                    '  "epoch": "%s",' % epoch.internal,
                    '  "lower": "",',
                    '  "meta_timestamp": "%s",' % now.internal,
                    '  "name": "a/c",',
                    '  "object_count": 0,',
                    '  "reported": 0,',
                    '  "state": "sharding",',
                    '  "state_timestamp": "%s",' % now.internal,
                    '  "timestamp": "%s",' % now.internal,
                    '  "tombstones": -1,',
                    '  "upper": ""',
                    '}',
                    'db_state = sharding',
                    'object_count = 100',
                    'bytes_used = 900',
                    'Retiring db id: %s' % retiring_db_id,
                    'Cleaving context: {',
                    '  "cleave_to_row": null,',
                    '  "cleaving_done": false,',
                    '  "cursor": "",',
                    '  "last_cleave_to_row": null,',
                    '  "max_row": 100,',
                    '  "misplaced_done": false,',
                    '  "ranges_done": 0,',
                    '  "ranges_todo": 0,',
                    '  "ref": "%s"' % retiring_db_id,
                    '}',
                    'Metadata:',
                    '  X-Container-Sysmeta-Sharding = True']
        result = [x for x in out.getvalue().splitlines()]
        self.assertEqual(expected, result)
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())

        self.assertTrue(broker.set_sharded_state())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with mock_timestamp_now(now):
                ret = main([broker.db_file, 'info'])
        self.assertEqual(0, ret)
        expected = ['Sharding enabled = True',
                    'Own shard range: {',
                    '  "bytes_used": 0,',
                    '  "deleted": 0,',
                    '  "epoch": "%s",' % epoch.internal,
                    '  "lower": "",',
                    '  "meta_timestamp": "%s",' % now.internal,
                    '  "name": "a/c",',
                    '  "object_count": 0,',
                    '  "reported": 0,',
                    '  "state": "sharding",',
                    '  "state_timestamp": "%s",' % now.internal,
                    '  "timestamp": "%s",' % now.internal,
                    '  "tombstones": -1,',
                    '  "upper": ""',
                    '}',
                    'db_state = sharded',
                    # in sharded state the object stats are determined by the
                    # shard ranges, and we haven't created any in the test...
                    'object_count = 0',
                    'bytes_used = 0',
                    'Metadata:',
                    '  X-Container-Sysmeta-Sharding = True']
        self.assertEqual(expected,
                         [x.rstrip() for x in out.getvalue().splitlines()])
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())

    def test_show(self):
        broker = self._make_broker()
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'show'])
        self.assertEqual(0, ret)
        expected = [
            'Loaded db broker for a/c',
            'No shard data found.',
        ]
        self.assertEqual(expected, err.getvalue().splitlines())
        self.assertEqual('', out.getvalue())

        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        expected_shard_ranges = [
            dict(sr, state=ShardRange.STATES[sr.state])
            for sr in shard_ranges
        ]
        broker.merge_shard_ranges(shard_ranges)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'show'])
        self.assertEqual(0, ret)
        expected = [
            'Loaded db broker for a/c',
            'Existing shard ranges:',
        ]
        self.assertEqual(expected, err.getvalue().splitlines())
        self.assertEqual(expected_shard_ranges, json.loads(out.getvalue()))

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'show', '--includes', 'foo'])
        self.assertEqual(0, ret)
        expected = [
            'Loaded db broker for a/c',
            'Existing shard ranges:',
        ]
        self.assertEqual(expected, err.getvalue().splitlines())
        self.assertEqual(expected_shard_ranges[:1], json.loads(out.getvalue()))

    def test_merge(self):
        broker = self._make_broker()
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        good_shard_ranges = []
        for shard in self.shard_data[:3]:
            good_shard_ranges.append(ShardRange(name='a/c_' + shard['lower'],
                                                timestamp=next(self.ts_iter),
                                                state=ShardRange.ACTIVE,
                                                lower=shard['lower'],
                                                upper=shard['upper']))
        # insert an overlap..
        bad_shard_range = ShardRange(
            name='a/c_bad_' + self.shard_data[1]['lower'],
            timestamp=next(self.ts_iter),
            state=ShardRange.ACTIVE,
            lower=self.shard_data[1]['lower'],
            upper=self.shard_data[2]['upper'])
        broker.merge_shard_ranges(good_shard_ranges + [bad_shard_range])
        self.assertEqual(
            [('', 'obj09'),
             ('obj09', 'obj19'),
             ('obj09', 'obj29'),
             ('obj19', 'obj29')],
            [(sr.lower_str, sr.upper_str) for sr in broker.get_shard_ranges()])

        # use command to merge in a deleted version of the bad shard range
        bad_shard_range.update_state(ShardRange.SHRUNK,
                                     state_timestamp=next(self.ts_iter))
        bad_shard_range.set_deleted(next(self.ts_iter))
        bad_shard_range.update_meta(0, 0, next(self.ts_iter))
        input_file = os.path.join(self.testdir, 'shards')
        with open(input_file, 'w') as fd:
            json.dump([dict(bad_shard_range)], fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, '-v', 'merge', input_file,
                        '--replace-timeout', '1', '--yes'])
        self.assertEqual(0, ret)
        affected_shard_ranges = [dict(sr) for sr in good_shard_ranges]
        expected_msg = [
            'This change will result in the following shard ranges in the '
            'affected namespace:']
        expected_msg.extend(
            json.dumps(affected_shard_ranges, indent=2).splitlines())
        expected_msg.extend(
            ['Injected 1 shard ranges.',
             'Run container-replicator to replicate them to other nodes.'])
        self.assertEqual(expected_msg, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self.assertEqual(
            [dict(sr) for sr in good_shard_ranges],
            [dict(sr) for sr in broker.get_shard_ranges()])
        self.assertEqual(
            dict(bad_shard_range),
            dict(broker.get_shard_ranges(include_deleted=True)[3]))

    def test_merge_fills_gap(self):
        broker = self._make_broker()
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        old_shard_ranges = []
        for shard in self.shard_data[:1]:
            old_shard_ranges.append(ShardRange(name='a/c_' + shard['lower'],
                                               timestamp=next(self.ts_iter),
                                               state=ShardRange.ACTIVE,
                                               lower=shard['lower'],
                                               upper=shard['upper']))

        # use command to merge in a deleted version of the existing and two
        # new ranges
        new_shard_ranges = [
            old_shard_ranges[0].copy(deleted=True,
                                     timestamp=next(self.ts_iter)),
            ShardRange(
                name='a/c_1_' + self.shard_data[0]['lower'],
                timestamp=next(self.ts_iter),
                state=ShardRange.ACTIVE,
                lower=self.shard_data[0]['lower'],
                upper=self.shard_data[0]['upper'] + 'a'),
            ShardRange(
                name='a/c_1_' + self.shard_data[0]['upper'] + 'a',
                timestamp=next(self.ts_iter),
                state=ShardRange.ACTIVE,
                lower=self.shard_data[0]['upper'] + 'a',
                upper=self.shard_data[1]['upper'] + 'a'),
        ]

        input_file = os.path.join(self.testdir, 'shards')
        with open(input_file, 'w') as fd:
            json.dump([dict(sr) for sr in new_shard_ranges], fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, '-v', 'merge', input_file,
                        '--replace-timeout', '1', '--yes'])
        self.assertEqual(0, ret)
        affected_shard_ranges = [dict(sr) for sr in new_shard_ranges[1:]]
        expected_msg = [
            'This change will result in the following shard ranges in the '
            'affected namespace:']
        expected_msg.extend(
            json.dumps(affected_shard_ranges, indent=2).splitlines())
        expected_msg.extend(
            ['Injected 3 shard ranges.',
             'Run container-replicator to replicate them to other nodes.'])
        self.assertEqual(expected_msg, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self.assertEqual(
            [dict(sr) for sr in new_shard_ranges[1:]],
            [dict(sr) for sr in broker.get_shard_ranges()])
        self.assertEqual(
            [dict(sr) for sr in new_shard_ranges],
            [dict(sr) for sr in broker.get_shard_ranges(include_deleted=True)])

    def test_merge_warns_of_overlap(self):
        broker = self._make_broker()
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        old_shard_ranges = []
        for shard in self.shard_data[:3]:
            old_shard_ranges.append(ShardRange(name='a/c_' + shard['lower'],
                                               timestamp=next(self.ts_iter),
                                               state=ShardRange.ACTIVE,
                                               lower=shard['lower'],
                                               upper=shard['upper']))
        broker.merge_shard_ranges(old_shard_ranges)

        # use command to merge in a new range that overlaps...
        new_shard_range = ShardRange(
            name='a/c_bad_' + self.shard_data[1]['lower'],
            timestamp=next(self.ts_iter),
            state=ShardRange.ACTIVE,
            lower=self.shard_data[1]['lower'] + 'a',
            upper=self.shard_data[1]['upper'])
        input_file = os.path.join(self.testdir, 'shards')
        with open(input_file, 'w') as fd:
            json.dump([dict(new_shard_range)], fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, '-v', 'merge', input_file,
                        '--replace-timeout', '1', '-n'])
        self.assertEqual(3, ret)
        affected_shard_ranges = [
            dict(sr) for sr in [old_shard_ranges[0], old_shard_ranges[1],
                                new_shard_range, old_shard_ranges[2]]]
        expected_msg = [
            'This change will result in the following shard ranges in the '
            'affected namespace:']
        expected_msg.extend(
            json.dumps(affected_shard_ranges, indent=2).splitlines())
        expected_msg.extend(
            ['WARNING: this change will result in shard ranges overlaps!',
             'No changes applied'])
        self.assertEqual(expected_msg, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self.assertEqual(
            [dict(sr) for sr in old_shard_ranges],
            [dict(sr) for sr in broker.get_shard_ranges()])

        # repeat without -v flag
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'merge', input_file,
                        '--replace-timeout', '1', '-n'])
        self.assertEqual(3, ret)
        expected_msg = [
            'WARNING: this change will result in shard ranges overlaps!',
            'No changes applied']
        self.assertEqual(expected_msg, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self.assertEqual(
            [dict(sr) for sr in old_shard_ranges],
            [dict(sr) for sr in broker.get_shard_ranges()])

    def test_merge_warns_of_gap(self):
        broker = self._make_broker()
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        old_shard_ranges = []
        for shard in self.shard_data[:3]:
            old_shard_ranges.append(ShardRange(name='a/c_' + shard['lower'],
                                               timestamp=next(self.ts_iter),
                                               state=ShardRange.ACTIVE,
                                               lower=shard['lower'],
                                               upper=shard['upper']))
        broker.merge_shard_ranges(old_shard_ranges)

        # use command to merge in a deleted range that creates a gap...
        new_shard_range = old_shard_ranges[1].copy(
            timestamp=next(self.ts_iter), deleted=True)
        input_file = os.path.join(self.testdir, 'shards')
        with open(input_file, 'w') as fd:
            json.dump([dict(new_shard_range)], fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, '-v', 'merge', input_file,
                        '--replace-timeout', '1', '-n'])
        self.assertEqual(3, ret)
        affected_shard_ranges = [
            dict(sr) for sr in [old_shard_ranges[0], old_shard_ranges[2]]]
        expected_msg = [
            'This change will result in the following shard ranges in the '
            'affected namespace:']
        expected_msg.extend(
            json.dumps(affected_shard_ranges, indent=2).splitlines())
        expected_msg.extend(
            ['WARNING: this change will result in shard ranges gaps!',
             'No changes applied'])
        self.assertEqual(expected_msg, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self.assertEqual(
            [dict(sr) for sr in old_shard_ranges],
            [dict(sr) for sr in broker.get_shard_ranges()])

    def test_replace(self):
        broker = self._make_broker()
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        input_file = os.path.join(self.testdir, 'shards')
        with open(input_file, 'w') as fd:
            json.dump(self.shard_data, fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'replace', input_file])
        self.assertEqual(0, ret)
        expected = [
            'No shard ranges found to delete.',
            'Injected 10 shard ranges.',
            'Run container-replicator to replicate them to other nodes.',
            'Use the enable sub-command to enable sharding.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self.assertEqual(
            [(data['lower'], data['upper']) for data in self.shard_data],
            [(sr.lower_str, sr.upper_str) for sr in broker.get_shard_ranges()])

        def check_user_exit(user_input):
            # try again now db has shard ranges, simulate user quitting
            with open(input_file, 'w') as fd:
                json.dump(self.overlap_shard_data_1, fd)
            out = StringIO()
            err = StringIO()
            to_patch = 'swift.cli.manage_shard_ranges.input'
            with mock.patch('sys.stdout', out), \
                    mock.patch('sys.stderr', err), \
                    mock.patch(to_patch, side_effect=[user_input]):
                ret = main([broker.db_file, 'replace', input_file])
            self.assertEqual(3, ret)
            expected = ['This will delete existing 10 shard ranges.']
            self.assertEqual(expected, out.getvalue().splitlines())
            self.assertEqual(['Loaded db broker for a/c'],
                             err.getvalue().splitlines())
            self.assertEqual(
                [(data['lower'], data['upper']) for data in self.shard_data],
                [(sr.lower_str, sr.upper_str)
                 for sr in broker.get_shard_ranges()])

        check_user_exit('q')
        check_user_exit(EOFError)
        check_user_exit(KeyboardInterrupt)

    def test_analyze_stdin(self):
        out = StringIO()
        err = StringIO()
        stdin = StringIO()
        stdin.write(json.dumps([]))  # empty but valid json
        stdin.seek(0)
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err), \
                mock.patch('sys.stdin', stdin):
            ret = main(['-', 'analyze'])
        self.assertEqual(1, ret)
        expected = [
            'Found no complete sequence of shard ranges.',
            'Repairs necessary to fill gaps.',
            'Gap filling not supported by this tool. No repairs performed.',
        ]

        self.assertEqual(expected, out.getvalue().splitlines())
        broker = self._make_broker()
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        shard_ranges = [
            dict(sr, state=ShardRange.STATES[sr.state])
            for sr in make_shard_ranges(broker, self.shard_data, '.shards_')
        ]
        out = StringIO()
        err = StringIO()
        stdin = StringIO()
        stdin.write(json.dumps(shard_ranges))
        stdin.seek(0)
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err), \
                mock.patch('sys.stdin', stdin):
            ret = main(['-', 'analyze'])
        self.assertEqual(0, ret)
        expected = [
            'Found one complete sequence of 10 shard ranges '
            'and no overlapping shard ranges.',
            'No repairs necessary.',
        ]
        self.assertEqual(expected, out.getvalue().splitlines())

    def test_analyze_stdin_with_overlaps(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(next(self.ts_iter)):
            overlap_shard_ranges_1 = make_shard_ranges(
                broker, self.overlap_shard_data_1, '.shards_')
        broker.merge_shard_ranges(shard_ranges + overlap_shard_ranges_1)
        shard_ranges = [
            dict(sr, state=ShardRange.STATES[sr.state])
            for sr in broker.get_shard_ranges()
        ]
        out = StringIO()
        err = StringIO()
        stdin = StringIO()
        stdin.write(json.dumps(shard_ranges))
        stdin.seek(0)
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err), \
                mock.patch('sys.stdin', stdin):
            ret = main(['-', 'analyze'])
        self.assertEqual(0, ret)
        expected = [
            'Repairs necessary to remove overlapping shard ranges.',
            'Chosen a complete sequence of 10 shard ranges with '
            'current total of 100 object records to accept object records '
            'from 10 overlapping donor shard ranges.',
            'Once applied to the broker these changes will result in:',
            '    10 shard ranges being removed.',
            '    10 object records being moved to the chosen shard ranges.',
        ]
        self.assertEqual(expected, out.getvalue().splitlines())

    def _assert_enabled(self, broker, epoch):
        own_sr = broker.get_own_shard_range()
        self.assertEqual(ShardRange.SHARDING, own_sr.state)
        self.assertEqual(epoch, own_sr.epoch)
        self.assertEqual(ShardRange.MIN, own_sr.lower)
        self.assertEqual(ShardRange.MAX, own_sr.upper)
        self.assertEqual(
            'True', broker.metadata['X-Container-Sysmeta-Sharding'][0])

    def test_enable(self):
        broker = self._make_broker()
        ts = next(self.ts_iter)
        broker.merge_items([
            {'name': 'obj%02d' % i, 'created_at': ts.internal, 'size': 9,
             'content_type': 'application/octet-stream', 'etag': 'not-really',
             'deleted': 0, 'storage_policy_index': 0,
             'ctype_timestamp': ts.internal, 'meta_timestamp': ts.internal}
            for i in range(100)])
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        # no shard ranges
        out = StringIO()
        err = StringIO()
        with self.assertRaises(SystemExit) as cm:
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                main([broker.db_file, 'enable'])
        self.assertEqual(1, cm.exception.code)
        expected = ["WARNING: invalid shard ranges: ['No shard ranges.'].",
                    'Aborting.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())

        # success
        shard_ranges = []
        for data in self.shard_data:
            path = ShardRange.make_path(
                '.shards_a', 'c', 'c', Timestamp.now(), data['index'])
            shard_ranges.append(
                ShardRange(path, Timestamp.now(), data['lower'],
                           data['upper'], data['object_count'], bytes_used=9))
        broker.merge_shard_ranges(shard_ranges)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with mock_timestamp_now() as now:
                ret = main([broker.db_file, 'enable'])
        self.assertEqual(0, ret)
        expected = [
            "Container moved to state 'sharding' with epoch %s." %
            now.internal,
            'Run container-sharder on all nodes to shard the container.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self._assert_enabled(broker, now)
        self.assertEqual(100, broker.get_info()['object_count'])
        self.assertEqual(100, broker.get_own_shard_range().object_count)
        self.assertEqual(900, broker.get_info()['bytes_used'])
        self.assertEqual(900, broker.get_own_shard_range().bytes_used)

        # already enabled
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'enable'])
        self.assertEqual(0, ret)
        expected = [
            "Container already in state 'sharding' with epoch %s." %
            now.internal,
            'No action required.',
            'Run container-sharder on all nodes to shard the container.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self._assert_enabled(broker, now)

    def test_find_replace_enable(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file)
        broker.account = 'a'
        broker.container = 'c'
        broker.initialize()
        ts = utils.Timestamp.now()
        broker.merge_items([
            {'name': 'obj%02d' % i, 'created_at': ts.internal, 'size': 0,
             'content_type': 'application/octet-stream', 'etag': 'not-really',
             'deleted': 0, 'storage_policy_index': 0,
             'ctype_timestamp': ts.internal, 'meta_timestamp': ts.internal}
            for i in range(100)])
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with mock_timestamp_now() as now:
                ret = main([broker.db_file, 'find_and_replace', '10',
                            '--enable'])
        self.assertEqual(0, ret)
        expected = [
            'No shard ranges found to delete.',
            'Injected 10 shard ranges.',
            'Run container-replicator to replicate them to other nodes.',
            "Container moved to state 'sharding' with epoch %s." %
            now.internal,
            'Run container-sharder on all nodes to shard the container.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c'],
                         err.getvalue().splitlines())
        self._assert_enabled(broker, now)
        found_shard_ranges = broker.get_shard_ranges()
        self.assertEqual(
            [(data['lower'], data['upper']) for data in self.shard_data],
            [(sr.lower_str, sr.upper_str) for sr in found_shard_ranges])

        def check_user_exit(user_input):
            # Do another find & replace but quit when prompted about existing
            # shard ranges
            out = StringIO()
            err = StringIO()
            to_patch = 'swift.cli.manage_shard_ranges.input'
            with mock.patch('sys.stdout', out), mock_timestamp_now(), \
                    mock.patch('sys.stderr', err), \
                    mock.patch(to_patch, side_effect=[user_input]):
                ret = main([broker.db_file, 'find_and_replace', '10'])
            self.assertEqual(3, ret)
            # Shard ranges haven't changed at all
            self.assertEqual(found_shard_ranges, broker.get_shard_ranges())
            expected = ['This will delete existing 10 shard ranges.']
            self.assertEqual(expected, out.getvalue().splitlines())
            self.assertEqual(['Loaded db broker for a/c'],
                             err.getvalue().splitlines())

        check_user_exit('q')
        check_user_exit(EOFError)
        check_user_exit(KeyboardInterrupt)

    def test_compact_bad_args(self):
        broker = self._make_broker()
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with self.assertRaises(SystemExit) as cm:
                main([broker.db_file, 'compact', '--shrink-threshold', '0'])
            self.assertEqual(2, cm.exception.code)
            with self.assertRaises(SystemExit) as cm:
                main([broker.db_file, 'compact', '--expansion-limit', '0'])
            self.assertEqual(2, cm.exception.code)
            with self.assertRaises(SystemExit) as cm:
                main([broker.db_file, 'compact', '--max-shrinking', '0'])
            self.assertEqual(2, cm.exception.code)
            with self.assertRaises(SystemExit) as cm:
                main([broker.db_file, 'compact', '--max-expanding', '0'])
            self.assertEqual(2, cm.exception.code)

    def test_compact_not_root(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        # make broker appear to not be a root container
        out = StringIO()
        err = StringIO()
        broker.set_sharding_sysmeta('Quoted-Root', 'not_a/c')
        self.assertFalse(broker.is_root_container())
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['WARNING: Shard containers cannot be compacted.',
             'This command should be used on a root container.'],
            out_lines[:2]
        )
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.FOUND] * 10,
                         [sr.state for sr in updated_ranges])

    def test_compact_not_sharded(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        # make broker appear to be a root container but it isn't sharded
        out = StringIO()
        err = StringIO()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        self.assertTrue(broker.is_root_container())
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['WARNING: Container is not yet sharded so cannot be compacted.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.FOUND] * 10,
                         [sr.state for sr in updated_ranges])

    def test_compact_overlapping_shard_ranges(self):
        # verify that containers with overlaps will not be compacted
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        shard_ranges[3].upper = shard_ranges[4].upper
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file,
                        'compact', '--yes', '--max-expanding', '10'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['WARNING: Container has overlapping shard ranges so cannot be '
             'compacted.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.ACTIVE] * 10,
                         [sr.state for sr in updated_ranges])

    def test_compact_shard_ranges_in_found_state(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['No shards identified for compaction.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual([ShardRange.FOUND] * 10,
                         [sr.state for sr in updated_ranges])

    def test_compact_user_input(self):
        # verify user input 'yes' or 'n' is respected
        small_ranges = (3, 4, 7)
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.tombstones = 999
            if i not in small_ranges:
                sr.object_count = 100001
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)

        expected_base = [
            'Donor shard range(s) with total of 2018 rows:',
            "  '.shards_a",
            "    objects:        10, tombstones:       999, lower: 'obj29'",
            "      state:    active, deleted: 0             upper: 'obj39'",
            "  '.shards_a",
            "    objects:        10, tombstones:       999, lower: 'obj39'",
            "      state:    active, deleted: 0             upper: 'obj49'",
            'can be compacted into acceptor shard range:',
            "  '.shards_a",
            "    objects:    100001, tombstones:       999, lower: 'obj49'",
            "      state:    active, deleted: 0             upper: 'obj59'",
            'Donor shard range(s) with total of 1009 rows:',
            "  '.shards_a",
            "    objects:        10, tombstones:       999, lower: 'obj69'",
            "      state:    active, deleted: 0             upper: 'obj79'",
            'can be compacted into acceptor shard range:',
            "  '.shards_a",
            "    objects:    100001, tombstones:       999, lower: 'obj79'",
            "      state:    active, deleted: 0             upper: 'obj89'",
            'Total of 2 shard sequences identified for compaction.',
            'Once applied to the broker these changes will result in '
            'shard range compaction the next time the sharder runs.',
        ]

        def do_compact(user_input, options, exp_changes, exit_code):
            out = StringIO()
            err = StringIO()
            with mock.patch('sys.stdout', out), \
                    mock.patch('sys.stderr', err), \
                    mock.patch('swift.cli.manage_shard_ranges.input',
                               side_effect=[user_input]):
                ret = main([broker.db_file, 'compact',
                            '--max-shrinking', '99'] + options)
            self.assertEqual(exit_code, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            out_lines = out.getvalue().split('\n')
            expected = list(expected_base)
            if exp_changes:
                expected.extend([
                    'Updated 2 shard sequences for compaction.',
                    'Run container-replicator to replicate the changes to '
                    'other nodes.',
                    'Run container-sharder on all nodes to compact shards.',
                    '',
                ])
            else:
                expected.extend([
                    'No changes applied',
                    '',
                ])
            self.assertEqual(expected, [l.split('/', 1)[0] for l in out_lines])
            return broker.get_shard_ranges()

        broker_ranges = do_compact('n', [], False, 3)
        # expect no changes to shard ranges
        self.assertEqual(shard_ranges, broker_ranges)
        for i, sr in enumerate(broker_ranges):
            self.assertEqual(ShardRange.ACTIVE, sr.state)

        broker_ranges = do_compact(EOFError, [], False, 3)
        # expect no changes to shard ranges
        self.assertEqual(shard_ranges, broker_ranges)
        for i, sr in enumerate(broker_ranges):
            self.assertEqual(ShardRange.ACTIVE, sr.state)

        broker_ranges = do_compact(KeyboardInterrupt, [], False, 3)
        # expect no changes to shard ranges
        self.assertEqual(shard_ranges, broker_ranges)
        for i, sr in enumerate(broker_ranges):
            self.assertEqual(ShardRange.ACTIVE, sr.state)

        broker_ranges = do_compact('yes', ['--dry-run'], False, 3)
        # expect no changes to shard ranges
        self.assertEqual(shard_ranges, broker_ranges)
        for i, sr in enumerate(broker_ranges):
            self.assertEqual(ShardRange.ACTIVE, sr.state)

        broker_ranges = do_compact('yes', [], True, 0)
        # expect updated shard ranges
        shard_ranges[5].lower = shard_ranges[3].lower
        shard_ranges[8].lower = shard_ranges[7].lower
        self.assertEqual(shard_ranges, broker_ranges)
        for i, sr in enumerate(broker_ranges):
            if i in small_ranges:
                self.assertEqual(ShardRange.SHRINKING, sr.state)
            else:
                self.assertEqual(ShardRange.ACTIVE, sr.state)

    def test_compact_four_donors_two_acceptors(self):
        small_ranges = (2, 3, 4, 7)
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            if i not in small_ranges:
                sr.object_count = 100001
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--max-shrinking', '99'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertIn('Updated 2 shard sequences for compaction.', out_lines)
        updated_ranges = broker.get_shard_ranges()
        for i, sr in enumerate(updated_ranges):
            if i in small_ranges:
                self.assertEqual(ShardRange.SHRINKING, sr.state)
            else:
                self.assertEqual(ShardRange.ACTIVE, sr.state)
        shard_ranges[5].lower = shard_ranges[2].lower
        shard_ranges[8].lower = shard_ranges[7].lower
        self.assertEqual(shard_ranges, updated_ranges)
        for i in (5, 8):
            # acceptors should have updated timestamp
            self.assertLess(shard_ranges[i].timestamp,
                            updated_ranges[i].timestamp)
        # check idempotency
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--max-shrinking', '99'])

        self.assertEqual(0, ret)
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(shard_ranges, updated_ranges)
        for i, sr in enumerate(updated_ranges):
            if i in small_ranges:
                self.assertEqual(ShardRange.SHRINKING, sr.state)
            else:
                self.assertEqual(ShardRange.ACTIVE, sr.state)

    def test_compact_all_donors_shrink_to_root(self):
        # by default all shard ranges are small enough to shrink so the root
        # becomes the acceptor
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        epoch = self._move_broker_to_sharded_state(broker)
        own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(epoch, own_sr.state_timestamp)  # sanity check
        self.assertEqual(ShardRange.SHARDED, own_sr.state)  # sanity check

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--max-shrinking', '99'])
        self.assertEqual(0, ret, 'stdout:\n%s\nstderr\n%s' %
                         (out.getvalue(), err.getvalue()))
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertIn('Updated 1 shard sequences for compaction.', out_lines)
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 10,
                         [sr.state for sr in updated_ranges])
        updated_own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(own_sr.timestamp, updated_own_sr.timestamp)
        self.assertEqual(own_sr.epoch, updated_own_sr.epoch)
        self.assertLess(own_sr.state_timestamp,
                        updated_own_sr.state_timestamp)
        self.assertEqual(ShardRange.ACTIVE, updated_own_sr.state)

    def test_compact_single_donor_shrink_to_root(self):
        # single shard range small enough to shrink so the root becomes the
        # acceptor
        broker = self._make_broker()
        shard_data = [
            {'index': 0, 'lower': '', 'upper': '', 'object_count': 10}
        ]

        shard_ranges = make_shard_ranges(broker, shard_data, '.shards_')
        shard_ranges[0].update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        epoch = self._move_broker_to_sharded_state(broker)
        own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(epoch, own_sr.state_timestamp)  # sanity check
        self.assertEqual(ShardRange.SHARDED, own_sr.state)  # sanity check

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes'])
        self.assertEqual(0, ret, 'stdout:\n%s\nstderr\n%s' %
                         (out.getvalue(), err.getvalue()))
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertIn('Updated 1 shard sequences for compaction.', out_lines)
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING],
                         [sr.state for sr in updated_ranges])
        updated_own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(own_sr.timestamp, updated_own_sr.timestamp)
        self.assertEqual(own_sr.epoch, updated_own_sr.epoch)
        self.assertLess(own_sr.state_timestamp,
                        updated_own_sr.state_timestamp)
        self.assertEqual(ShardRange.ACTIVE, updated_own_sr.state)

    def test_compact_donors_but_no_suitable_acceptor(self):
        # if shard ranges are already shrinking, check that the final one is
        # not made into an acceptor if a suitable adjacent acceptor is not
        # found (unexpected scenario but possible in an overlap situation)
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, state in enumerate([ShardRange.SHRINKING] * 3 +
                                  [ShardRange.SHARDING] +
                                  [ShardRange.ACTIVE] * 6):
            shard_ranges[i].update_state(state)
        broker.merge_shard_ranges(shard_ranges)
        epoch = self._move_broker_to_sharded_state(broker)
        with mock_timestamp_now(epoch):
            own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(epoch, own_sr.state_timestamp)  # sanity check
        self.assertEqual(ShardRange.SHARDED, own_sr.state)  # sanity check

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--max-shrinking', '99'])
        self.assertEqual(0, ret, 'stdout:\n%s\nstderr\n%s' %
                         (out.getvalue(), err.getvalue()))
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertIn('Updated 1 shard sequences for compaction.', out_lines)
        updated_ranges = broker.get_shard_ranges()
        shard_ranges[9].lower = shard_ranges[4].lower  # expanded acceptor
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 3 +  # unchanged
                         [ShardRange.SHARDING] +  # unchanged
                         [ShardRange.SHRINKING] * 5 +  # moved to shrinking
                         [ShardRange.ACTIVE],  # unchanged
                         [sr.state for sr in updated_ranges])
        with mock_timestamp_now(epoch):  # force equal meta-timestamp
            updated_own_sr = broker.get_own_shard_range(no_default=True)
        self.assertEqual(dict(own_sr), dict(updated_own_sr))

    def test_compact_no_gaps(self):
        # verify that compactible sequences do not include gaps
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        gapped_ranges = shard_ranges[:3] + shard_ranges[4:]
        broker.merge_shard_ranges(gapped_ranges)
        self._move_broker_to_sharded_state(broker)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--max-shrinking', '99'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertIn('Updated 2 shard sequences for compaction.', out_lines)
        updated_ranges = broker.get_shard_ranges()
        gapped_ranges[2].lower = gapped_ranges[0].lower
        gapped_ranges[8].lower = gapped_ranges[3].lower
        self.assertEqual(gapped_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 2 + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] * 5 + [ShardRange.ACTIVE],
                         [sr.state for sr in updated_ranges])

    def test_compact_max_shrinking_default(self):
        # verify default limit on number of shrinking shards per acceptor
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)

        def do_compact(expect_msg):
            out = StringIO()
            err = StringIO()
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                ret = main([broker.db_file, 'compact', '--yes'])
            self.assertEqual(0, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            out_lines = out.getvalue().split('\n')
            self.assertIn(expect_msg, out_lines)
            return broker.get_shard_ranges()

        updated_ranges = do_compact(
            'Updated 5 shard sequences for compaction.')
        for acceptor in (1, 3, 5, 7, 9):
            shard_ranges[acceptor].lower = shard_ranges[acceptor - 1].lower
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING, ShardRange.ACTIVE] * 5,
                         [sr.state for sr in updated_ranges])

        # check idempotency
        updated_ranges = do_compact('No shards identified for compaction.')
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING, ShardRange.ACTIVE] * 5,
                         [sr.state for sr in updated_ranges])

    def test_compact_max_shrinking(self):
        # verify option to limit the number of shrinking shards per acceptor
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)

        def do_compact(expect_msg):
            out = StringIO()
            err = StringIO()
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                ret = main([broker.db_file, 'compact', '--yes',
                            '--max-shrinking', '7'])
            self.assertEqual(0, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            out_lines = out.getvalue().split('\n')
            self.assertIn(expect_msg, out_lines)
            return broker.get_shard_ranges()

        updated_ranges = do_compact(
            'Updated 2 shard sequences for compaction.')
        shard_ranges[7].lower = shard_ranges[0].lower
        shard_ranges[9].lower = shard_ranges[8].lower
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 7 + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] + [ShardRange.ACTIVE],
                         [sr.state for sr in updated_ranges])

        # check idempotency
        updated_ranges = do_compact('No shards identified for compaction.')
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 7 + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] + [ShardRange.ACTIVE],
                         [sr.state for sr in updated_ranges])

    def test_compact_max_expanding(self):
        # verify option to limit the number of expanding shards per acceptor
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)

        def do_compact(expect_msg):
            out = StringIO()
            err = StringIO()
            # note: max_shrinking is set to 3 so that there is opportunity for
            # more than 2 acceptors
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                ret = main([broker.db_file, 'compact', '--yes',
                            '--max-shrinking', '3', '--max-expanding', '2'])
            self.assertEqual(0, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            out_lines = out.getvalue().split('\n')
            self.assertIn(expect_msg, out_lines)
            return broker.get_shard_ranges()

        updated_ranges = do_compact(
            'Updated 2 shard sequences for compaction.')
        shard_ranges[3].lower = shard_ranges[0].lower
        shard_ranges[7].lower = shard_ranges[4].lower
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 3 + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] * 3 + [ShardRange.ACTIVE] * 3,
                         [sr.state for sr in updated_ranges])

        # check idempotency - no more sequences found while existing sequences
        # are shrinking
        updated_ranges = do_compact('No shards identified for compaction.')
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 3 + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] * 3 + [ShardRange.ACTIVE] * 3,
                         [sr.state for sr in updated_ranges])

    def test_compact_expansion_limit(self):
        # verify option to limit the size of each acceptor after compaction
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--expansion-limit', '20'])
        self.assertEqual(0, ret, err.getvalue())
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().rstrip('\n').split('\n')
        self.assertIn('Updated 5 shard sequences for compaction.', out_lines)
        updated_ranges = broker.get_shard_ranges()
        shard_ranges[1].lower = shard_ranges[0].lower
        shard_ranges[3].lower = shard_ranges[2].lower
        shard_ranges[5].lower = shard_ranges[4].lower
        shard_ranges[7].lower = shard_ranges[6].lower
        shard_ranges[9].lower = shard_ranges[8].lower
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] + [ShardRange.ACTIVE] +
                         [ShardRange.SHRINKING] + [ShardRange.ACTIVE],
                         [sr.state for sr in updated_ranges])

    def test_compact_expansion_limit_less_than_shrink_threshold(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            if i % 2:
                sr.object_count = 25
            else:
                sr.object_count = 3
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--shrink-threshold', '10',
                        '--expansion-limit', '5'])
        self.assertEqual(0, ret)
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['No shards identified for compaction.'],
            out_lines[:1])

    def test_compact_nothing_to_do(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)
        out = StringIO()
        err = StringIO()
        # all shards are too big to shrink
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--shrink-threshold', '5',
                        '--expansion-limit', '8'])
        self.assertEqual(0, ret)
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['No shards identified for compaction.'],
            out_lines[:1])

        # all shards could shrink but acceptors would be too large
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--shrink-threshold', '11',
                        '--expansion-limit', '12'])
        self.assertEqual(0, ret)
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['No shards identified for compaction.'],
            out_lines[:1])

    def _do_test_compact_shrink_threshold(self, broker, shard_ranges):
        # verify option to set the shrink threshold for compaction;
        for i, sr in enumerate(shard_ranges):
            sr.update_state(ShardRange.ACTIVE)
        # (n-2)th shard range has one extra object
        shard_ranges[-2].object_count = shard_ranges[-2].object_count + 1
        broker.merge_shard_ranges(shard_ranges)
        self._move_broker_to_sharded_state(broker)
        # with threshold set to 10 no shard ranges can be shrunk
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--max-shrinking', '99',
                        '--shrink-threshold', '10'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['No shards identified for compaction.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.ACTIVE] * 10,
                         [sr.state for sr in updated_ranges])

        # with threshold == 11 all but the final 2 shard ranges can be shrunk;
        # note: the (n-1)th shard range is NOT shrunk to root
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'compact', '--yes',
                        '--max-shrinking', '99',
                        '--shrink-threshold', '11'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertIn('Updated 1 shard sequences for compaction.', out_lines)
        updated_ranges = broker.get_shard_ranges()
        shard_ranges[8].lower = shard_ranges[0].lower
        self.assertEqual(shard_ranges, updated_ranges)
        self.assertEqual([ShardRange.SHRINKING] * 8 + [ShardRange.ACTIVE] * 2,
                         [sr.state for sr in updated_ranges])

    def test_compact_shrink_threshold(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        self._do_test_compact_shrink_threshold(broker, shard_ranges)

    def test_compact_shrink_threshold_with_tombstones(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        for i, sr in enumerate(shard_ranges):
            sr.object_count = sr.object_count - i
            sr.tombstones = i
        self._do_test_compact_shrink_threshold(broker, shard_ranges)

    def test_repair_not_root(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        # make broker appear to not be a root container
        out = StringIO()
        err = StringIO()
        broker.set_sharding_sysmeta('Quoted-Root', 'not_a/c')
        self.assertFalse(broker.is_root_container())
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['WARNING: Shard containers cannot be repaired.',
             'This command should be used on a root container.'],
            out_lines[:2]
        )
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(shard_ranges, updated_ranges)

    def test_repair_no_shard_ranges(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['No shards found, nothing to do.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal([], updated_ranges)

    def test_repair_one_incomplete_sequence(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data[:-1], '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Found no complete sequence of shard ranges.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(shard_ranges, updated_ranges)

    def test_repair_overlapping_incomplete_sequences(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data[:-1], '.shards_')
        with mock_timestamp_now(next(self.ts_iter)):
            # use new time to get distinct shard names
            overlap_shard_ranges = make_shard_ranges(
                broker,
                self.overlap_shard_data_1[:2] + self.overlap_shard_data_1[6:],
                '.shards_')
        broker.merge_shard_ranges(shard_ranges + overlap_shard_ranges)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Found no complete sequence of shard ranges.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(shard_ranges + overlap_shard_ranges,
                          key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_gaps(self):
        def do_test(missing_index, expander_index, missing_state=None):
            broker = self._make_broker()
            broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
            for shard in self.shard_data:
                shard['state'] = ShardRange.ACTIVE
            with mock_timestamp_now(next(self.ts_iter)):
                all_shard_ranges = make_shard_ranges(
                    broker, self.shard_data, '.shards_')
            shard_ranges = list(all_shard_ranges)
            if missing_state is None:
                missing_range = shard_ranges.pop(missing_index)
                exp_gap_contents = []
            else:
                missing_range = shard_ranges[missing_index]
                missing_range.state = missing_state
                exp_gap_contents = [
                    "      '%s'" % missing_range.name, mock.ANY, mock.ANY]
            broker.merge_shard_ranges(shard_ranges)
            self.assertTrue(broker.is_root_container())
            out = StringIO()
            err = StringIO()
            with mock_timestamp_now(next(self.ts_iter)) as ts_now, \
                    mock.patch('sys.stdout', out), \
                    mock.patch('sys.stderr', err):
                ret = main([broker.db_file, 'repair', '--gaps', '--yes'])
            self.assertEqual(0, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            out_lines = out.getvalue().split('\n')
            expander = all_shard_ranges[expander_index]
            if missing_index < expander_index:
                expander.lower = missing_range.lower
            else:
                expander.upper = missing_range.upper
            expander.state_timestamp = expander.timestamp
            expander.meta_timestamp = expander.timestamp
            expander.timestamp = ts_now
            self.assertEqual(
                ['Found 1 gaps:',
                 '  gap: %r - %r' % (missing_range.lower, missing_range.upper),
                 '    apparent gap contents:']
                + exp_gap_contents +
                ['    gap can be fixed by expanding neighbor range:',
                 "      '%s'" % expander.name] +
                [mock.ANY] * 2 +
                ['',
                 'Repairs necessary to fill gaps.',
                 'The following expanded shard range(s) will be applied to '
                 'the DB:',
                 "    '%s'" % expander.name] +
                [mock.ANY] * 2 +
                ['',
                 'It is recommended that no other concurrent changes are made '
                 'to the ',
                 'shard ranges while fixing gaps. If necessary, abort '
                 'this change ',
                 'and stop any auto-sharding processes before repeating '
                 'this command.',
                 '',
                 'Run container-replicator to replicate the changes to '
                 'other nodes.',
                 'Run container-sharder on all nodes to fill gaps.',
                 ''],
                out_lines)
            updated_ranges = broker.get_shard_ranges()
            self.assert_shard_ranges_equal(shard_ranges, updated_ranges)
            os.remove(broker.db_file)

        for i in range(len(self.shard_data) - 1):
            do_test(i, i + 1)

        do_test(len(self.shard_data) - 1, len(self.shard_data) - 2)

        for i in range(len(self.shard_data) - 1):
            do_test(i, i + 1, ShardRange.SHRINKING)

        do_test(len(self.shard_data) - 1, len(self.shard_data) - 2,
                ShardRange.SHRINKING)

    def test_repair_gaps_multiple_missing(self):
        def do_test(broker, max_expanding):
            broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
            states = [
                ShardRange.ACTIVE,
                ShardRange.SHRINKING,
                ShardRange.SHRUNK,
                ShardRange.ACTIVE,
                ShardRange.SHRUNK,
                ShardRange.SHRINKING,
                ShardRange.ACTIVE,
                ShardRange.SHRINKING,
                ShardRange.SHRUNK,
                ShardRange.SHARDED,
            ]
            for i, shard in enumerate(self.shard_data):
                shard['state'] = states[i]
                if states[i] in (ShardRange.SHRUNK, ShardRange.SHARDED):
                    shard['deleted'] = 1
            with mock_timestamp_now(next(self.ts_iter)):
                shard_ranges = make_shard_ranges(
                    broker, self.shard_data, '.shards_')
            broker.merge_shard_ranges(shard_ranges)
            self.assertTrue(broker.is_root_container())
            orig_shard_ranges = broker.get_shard_ranges(include_deleted=True)
            out = StringIO()
            err = StringIO()
            args = [broker.db_file, 'repair', '--gaps', '--yes']
            if max_expanding is not None:
                args.extend(['--max-expanding', str(max_expanding)])
            with mock_timestamp_now(next(self.ts_iter)) as ts_now, \
                    mock.patch('sys.stdout', out), \
                    mock.patch('sys.stderr', err):
                ret = main(args)
            self.assertEqual(0, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            out_lines = out.getvalue().split('\n')
            os.remove(broker.db_file)
            return orig_shard_ranges, out_lines, ts_now

        # max-expanding 1
        broker = self._make_broker()
        orig_shard_ranges, out_lines, ts_now = do_test(broker, 1)
        orig_shard_ranges[3].timestamp = ts_now
        orig_shard_ranges[3].lower = orig_shard_ranges[1].lower
        self.assertEqual(
            ['Found 3 gaps:',
             '  gap: %r - %r' % (orig_shard_ranges[1].lower,
                                 orig_shard_ranges[2].upper),
             '    apparent gap contents:']
            + [mock.ANY] * 6 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[3].name] +
            [mock.ANY] * 2 +
            ['  gap: %r - %r' % (orig_shard_ranges[4].lower,
                                 orig_shard_ranges[5].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 6 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 2 +
            ['  gap: %r - %r' % (orig_shard_ranges[7].lower,
                                 orig_shard_ranges[9].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 9 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 2 +
            ['',
             'Repairs necessary to fill gaps.',
             'The following expanded shard range(s) will be applied to the '
             'DB:',
             "    '%s'" % orig_shard_ranges[3].name] +
            [mock.ANY] * 6 +
            ['',
             'Run container-replicator to replicate the changes to '
             'other nodes.',
             'Run container-sharder on all nodes to fill gaps.',
             ''],
            out_lines)
        updated_ranges = broker.get_shard_ranges(include_deleted=True)
        self.assert_shard_ranges_equal(
            sorted(orig_shard_ranges, key=lambda s: s.name),
            sorted(updated_ranges, key=lambda s: s.name))

        # max-expanding 2
        broker = self._make_broker()
        orig_shard_ranges, out_lines, ts_now = do_test(broker, 2)
        orig_shard_ranges[3].timestamp = ts_now
        orig_shard_ranges[3].lower = orig_shard_ranges[1].lower
        orig_shard_ranges[6].timestamp = ts_now
        orig_shard_ranges[6].lower = orig_shard_ranges[4].lower
        self.assertEqual(
            ['Found 3 gaps:',
             '  gap: %r - %r' % (orig_shard_ranges[1].lower,
                                 orig_shard_ranges[2].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 6 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[3].name] +
            [mock.ANY] * 2 +
            ['  gap: %r - %r' % (orig_shard_ranges[4].lower,
                                 orig_shard_ranges[5].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 6 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 2 +
            ['  gap: %r - %r' % (orig_shard_ranges[7].lower,
                                 orig_shard_ranges[9].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 9 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 2 +
            ['',
             'Repairs necessary to fill gaps.',
             'The following expanded shard range(s) will be applied to the '
             'DB:',
             "    '%s'" % orig_shard_ranges[3].name] +
            [mock.ANY] * 2 +
            ["    '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 6 +
            ['',
             'Run container-replicator to replicate the changes to '
             'other nodes.',
             'Run container-sharder on all nodes to fill gaps.',
             ''],
            out_lines)
        updated_ranges = broker.get_shard_ranges(include_deleted=True)
        self.assert_shard_ranges_equal(
            sorted(orig_shard_ranges, key=lambda s: s.name),
            sorted(updated_ranges, key=lambda s: s.name))

        # max-expanding unlimited
        broker = self._make_broker()
        orig_shard_ranges, out_lines, ts_now = do_test(broker, None)
        orig_shard_ranges[3].timestamp = ts_now
        orig_shard_ranges[3].lower = orig_shard_ranges[1].lower
        orig_shard_ranges[6].timestamp = ts_now
        orig_shard_ranges[6].lower = orig_shard_ranges[4].lower
        orig_shard_ranges[6].upper = orig_shard_ranges[9].upper
        self.assertEqual(
            ['Found 3 gaps:',
             '  gap: %r - %r' % (orig_shard_ranges[1].lower,
                                 orig_shard_ranges[2].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 6 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[3].name] +
            [mock.ANY] * 2 +
            ['  gap: %r - %r' % (orig_shard_ranges[4].lower,
                                 orig_shard_ranges[5].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 6 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 2 +
            ['  gap: %r - %r' % (orig_shard_ranges[7].lower,
                                 orig_shard_ranges[9].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 9 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 2 +
            ['',
             'Repairs necessary to fill gaps.',
             'The following expanded shard range(s) will be applied to the '
             'DB:',
             "    '%s'" % orig_shard_ranges[3].name] +
            [mock.ANY] * 2 +
            ["    '%s'" % orig_shard_ranges[6].name] +
            [mock.ANY] * 6 +
            ['',
             'Run container-replicator to replicate the changes to '
             'other nodes.',
             'Run container-sharder on all nodes to fill gaps.',
             ''],
            out_lines)
        updated_ranges = broker.get_shard_ranges(include_deleted=True)
        self.assert_shard_ranges_equal(
            sorted(orig_shard_ranges, key=lambda s: s.name),
            sorted(updated_ranges, key=lambda s: s.name))

    def test_repair_gaps_complete_sequence(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        for shard in self.shard_data:
            shard['state'] = ShardRange.ACTIVE
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        with mock_timestamp_now(next(self.ts_iter)), \
                mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair', '--gaps', '--yes'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Found one complete sequence of %d shard ranges with no gaps.'
             % len(self.shard_data),
             'No repairs necessary.'], out_lines[:2])
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(shard_ranges, updated_ranges)

    def test_repair_gaps_with_overlap(self):
        # verify that overlaps don't look like gaps
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        for shard in self.shard_data:
            shard['state'] = ShardRange.ACTIVE
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        # create a gap
        shard_ranges[3].state = ShardRange.SHRINKING
        # create an overlap
        shard_ranges[5].lower = 'obj45'
        self.assertLess(shard_ranges[5].lower, shard_ranges[4].upper)
        broker.merge_shard_ranges(shard_ranges)
        orig_shard_ranges = broker.get_shard_ranges()
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        with mock_timestamp_now(next(self.ts_iter)) as ts_now, \
                mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair', '--gaps', '--yes'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Found 1 gaps:',
             '  gap: %r - %r' % (shard_ranges[3].lower,
                                 shard_ranges[3].upper),
             '    apparent gap contents:'] +
            [mock.ANY] * 3 +
            ['    gap can be fixed by expanding neighbor range:',
             "      '%s'" % shard_ranges[4].name] +
            [mock.ANY] * 2 +
            ['',
             'Repairs necessary to fill gaps.',
             'The following expanded shard range(s) will be applied to the '
             'DB:',
             "    '%s'" % shard_ranges[4].name] +
            [mock.ANY] * 6 +
            ['',
             'Run container-replicator to replicate the changes to '
             'other nodes.',
             'Run container-sharder on all nodes to fill gaps.',
             ''],
            out_lines)
        orig_shard_ranges[4].lower = shard_ranges[3].lower
        orig_shard_ranges[4].timestamp = ts_now
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(orig_shard_ranges, updated_ranges)

    def test_repair_gaps_not_root(self):
        broker = self._make_broker()
        shard_ranges = make_shard_ranges(broker, self.shard_data, '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        # make broker appear to not be a root container
        out = StringIO()
        err = StringIO()
        broker.set_sharding_sysmeta('Quoted-Root', 'not_a/c')
        self.assertFalse(broker.is_root_container())
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair', '--gaps'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['WARNING: Shard containers cannot be repaired.',
             'This command should be used on a root container.'],
            out_lines[:2]
        )
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(shard_ranges, updated_ranges)

    def test_repair_not_needed(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        shard_ranges = make_shard_ranges(
            broker, self.shard_data, '.shards_')
        broker.merge_shard_ranges(shard_ranges)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Found one complete sequence of 10 shard ranges and no '
             'overlapping shard ranges.',
             'No repairs necessary.'],
            out_lines[:2])
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(shard_ranges, updated_ranges)

    def _do_test_repair_exits_if_undesirable_state(self, undesirable_state):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        # make one shard be in an undesirable state
        shard_ranges[2].update_state(undesirable_state)
        with mock_timestamp_now(next(self.ts_iter)):
            overlap_shard_ranges_2 = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(shard_ranges + overlap_shard_ranges_2)
        self.assertTrue(broker.is_root_container())

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'repair'])
        self.assertEqual(1, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['WARNING: Found shard ranges in %s state'
             % ShardRange.STATES[undesirable_state]], out_lines[:1])
        # nothing changed in DB
        self.assert_shard_ranges_equal(
            sorted(shard_ranges + overlap_shard_ranges_2,
                   key=ShardRange.sort_key),
            broker.get_shard_ranges())

    def test_repair_exits_if_sharding_state(self):
        self._do_test_repair_exits_if_undesirable_state(ShardRange.SHARDING)

    def test_repair_exits_if_shrinking_state(self):
        self._do_test_repair_exits_if_undesirable_state(ShardRange.SHRINKING)

    def test_repair_one_complete_sequences_one_incomplete(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(next(self.ts_iter)):
            overlap_shard_ranges_2 = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(shard_ranges + overlap_shard_ranges_2)
        self.assertTrue(broker.is_root_container())

        def do_repair(user_input, ts_now, options, exit_code):
            options = options if options else []
            out = StringIO()
            err = StringIO()
            with mock.patch('sys.stdout', out), \
                    mock.patch('sys.stderr', err), \
                    mock_timestamp_now(ts_now), \
                    mock.patch('swift.cli.manage_shard_ranges.input',
                               side_effect=[user_input]):
                ret = main(
                    [broker.db_file, 'repair', '--min-shard-age', '0'] +
                    options)
            self.assertEqual(exit_code, ret)
            err_lines = err.getvalue().split('\n')
            self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
            out_lines = out.getvalue().split('\n')
            self.assertEqual(
                ['Repairs necessary to remove overlapping shard ranges.'],
                out_lines[:1])

        # user input 'n'
        ts_now = next(self.ts_iter)
        do_repair('n', ts_now, [], 3)
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(
            shard_ranges + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

        ts_now = next(self.ts_iter)
        do_repair(EOFError, ts_now, [], 3)
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(expected, updated_ranges)

        ts_now = next(self.ts_iter)
        do_repair(KeyboardInterrupt, ts_now, [], 3)
        updated_ranges = broker.get_shard_ranges()
        self.assert_shard_ranges_equal(expected, updated_ranges)

        # --dry-run
        ts_now = next(self.ts_iter)
        do_repair('y', ts_now, ['--dry-run'], 3)
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(
            shard_ranges + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

        # --n
        ts_now = next(self.ts_iter)
        do_repair('y', ts_now, ['-n'], 3)
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(
            shard_ranges + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

        # user input 'yes'
        ts_now = next(self.ts_iter)
        do_repair('yes', ts_now, [], 0)
        updated_ranges = broker.get_shard_ranges()
        for sr in overlap_shard_ranges_2:
            sr.update_state(ShardRange.SHRINKING, ts_now)
            sr.epoch = ts_now
        expected = sorted(
            shard_ranges + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_younger_overlapping_donor_shards(self):
        # test shard range repair on the normal acceptor ranges and young
        # overlapping shard ranges which are younger than '--min-shard-age',
        # expect them not to be repaired.
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        ts_now = next(self.ts_iter)
        with mock_timestamp_now(Timestamp(float(ts_now) - 61)):
            acceptor_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(ts_now):
            overlap_donor_ranges = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(acceptor_ranges + overlap_donor_ranges)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main(
                [broker.db_file, 'repair', '--min-shard-age', '60', '-y'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['2 overlapping donor shards ignored due to minimum age limit'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(
            acceptor_ranges + overlap_donor_ranges,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_younger_acceptor_with_overlapping_donor_shards(self):
        # test shard range repair on the overlapping normal donor ranges and
        # young acceptor shard ranges who are younger than '--min-shard-age',
        # expect no overlapping ranges to be repaired.
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        ts_now = next(self.ts_iter)
        with mock_timestamp_now(Timestamp(float(ts_now) + 3601)):
            acceptor_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(ts_now):
            overlap_donor_ranges = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(acceptor_ranges + overlap_donor_ranges)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err), \
                mock_timestamp_now(Timestamp(float(ts_now) + 3601 + 59)):
            ret = main(
                [broker.db_file, 'repair', '--min-shard-age', '60', '-y'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['2 donor shards ignored due to existence of overlapping young'
             ' acceptors'], out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(
            acceptor_ranges + overlap_donor_ranges,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_older_overlapping_donor_and_acceptor_shards(self):
        # test shard range repair on the overlapping donor and acceptor shard
        # ranges which all are older than '--min-shard-age', expect them to be
        # repaired.
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        ts_now = next(self.ts_iter)
        with mock_timestamp_now(ts_now):
            acceptor_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(Timestamp(float(ts_now) + 1800)):
            overlap_donor_ranges = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(acceptor_ranges + overlap_donor_ranges)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        ts_1hr_after = Timestamp(float(ts_now) + 3601)
        with mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err), \
                mock_timestamp_now(ts_1hr_after):
            ret = main(
                [broker.db_file, 'repair', '--min-shard-age', '60', '-y'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Repairs necessary to remove overlapping shard ranges.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        for sr in overlap_donor_ranges:
            sr.update_state(ShardRange.SHRINKING, ts_1hr_after)
            sr.epoch = ts_1hr_after
        expected = sorted(
            acceptor_ranges + overlap_donor_ranges,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_overlapping_donor_and_acceptor_shards_default(self):
        # test shard range repair on the overlapping donor and acceptor shard
        # ranges wth default '--min-shard-age' value.
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        ts_now = next(self.ts_iter)
        with mock_timestamp_now(ts_now):
            acceptor_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(Timestamp(int(ts_now) + 1)):
            overlap_donor_ranges = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(acceptor_ranges + overlap_donor_ranges)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        ts_repair = Timestamp(int(ts_now) + 4 * 3600 - 1)
        with mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err), \
                mock_timestamp_now(ts_repair):
            # default min-shard-age prevents repair...
            ret = main([broker.db_file, 'repair', '-y'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['2 overlapping donor shards ignored due to minimum age limit'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(
            acceptor_ranges + overlap_donor_ranges,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

        out = StringIO()
        err = StringIO()
        ts_repair = Timestamp(int(ts_now) + 4 * 3600 + 2)
        with mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err), \
                mock_timestamp_now(ts_repair):
            # default min-shard-age allows repair now...
            ret = main([broker.db_file, 'repair', '-y'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Repairs necessary to remove overlapping shard ranges.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        for sr in overlap_donor_ranges:
            sr.update_state(ShardRange.SHRINKING, ts_repair)
            sr.epoch = ts_repair
        expected = sorted(
            acceptor_ranges + overlap_donor_ranges,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_two_complete_sequences_one_incomplete(self):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):
            shard_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(next(self.ts_iter)):
            overlap_shard_ranges_1 = make_shard_ranges(
                broker, self.overlap_shard_data_1, '.shards_')
        with mock_timestamp_now(next(self.ts_iter)):
            overlap_shard_ranges_2 = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(shard_ranges + overlap_shard_ranges_1 +
                                  overlap_shard_ranges_2)
        self.assertTrue(broker.is_root_container())
        out = StringIO()
        err = StringIO()
        ts_now = next(self.ts_iter)
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err), \
                mock_timestamp_now(ts_now):
            ret = main([broker.db_file, 'repair', '--yes',
                        '--min-shard-age', '0'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Repairs necessary to remove overlapping shard ranges.'],
            out_lines[:1])
        updated_ranges = broker.get_shard_ranges()
        for sr in overlap_shard_ranges_1 + overlap_shard_ranges_2:
            sr.update_state(ShardRange.SHRINKING, ts_now)
            sr.epoch = ts_now
        expected = sorted(
            shard_ranges + overlap_shard_ranges_1 + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_parent_overlaps_with_children_donors(self):
        # Verify that the overlap repair command ignores expected transient
        # overlaps between parent shard acceptor and child donor shards.
        root_broker = self._make_broker()
        root_broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        self.assertTrue(root_broker.is_root_container())

        # The parent shard range would have been set to state SHARDING in the
        # shard container but is still showing as ACTIVE in the root container.
        # (note: it is valid for a single shard to span entire namespace)
        ts_parent = next(self.ts_iter)
        parent_shard = ShardRange(
            ShardRange.make_path('.shards_a', 'c', 'c', ts_parent, 0),
            ts_parent, lower='', upper='', object_count=10,
            state=ShardRange.ACTIVE)

        # Children shards have reported themselves to root as CLEAVING/
        # CREATED.
        ts_child = next(self.ts_iter)
        child_shards = [
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_shard.container, ts_child, 0),
                ts_child, lower='', upper='p', object_count=1,
                state=ShardRange.CLEAVED),
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_shard.container, ts_child, 1),
                ts_child, lower='p', upper='', object_count=1,
                state=ShardRange.CLEAVED)]
        root_broker.merge_shard_ranges([parent_shard] + child_shards)

        out = StringIO()
        err = StringIO()
        ts_now = next(self.ts_iter)
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err), \
                mock_timestamp_now(ts_now):
            ret = main([root_broker.db_file, 'repair',
                       '--yes', '--min-shard-age', '0'])
        err_lines = err.getvalue().split('\n')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(0, ret, err_lines + out_lines)
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assertNotIn(
            'Repairs necessary to remove overlapping shard ranges.',
            out_lines)
        self.assertEqual(
            ['2 donor shards ignored due to parent-child relationship'
             ' checks'], out_lines[:1])
        updated_ranges = root_broker.get_shard_ranges()
        # Expect no change to shard ranges.
        expected = sorted([parent_shard] + child_shards,
                          key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    def test_repair_children_overlaps_with_parent_donor(self):
        # Verify that the overlap repair command ignores expected transient
        # overlaps between child shard acceptors and parent donor shards.
        root_broker = self._make_broker()
        root_broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        self.assertTrue(root_broker.is_root_container())

        # The parent shard range would have been set to state SHARDING in the
        # shard container but is still showing as ACTIVE in the root container.
        # (note: it is valid for a single shard to span entire namespace)
        ts_parent = next(self.ts_iter)
        parent_shard = ShardRange(
            ShardRange.make_path('.shards_a', 'c', 'c', ts_parent, 0),
            ts_parent, lower='', upper='', object_count=5,
            state=ShardRange.ACTIVE)

        # Children shards have reported themselves to root as CLEAVING/CREATED,
        # but they will end up with becoming acceptor shards due to having more
        # objects than the above parent shard.
        ts_child = next(self.ts_iter)
        child_shards = [
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_shard.container, ts_child, 0),
                ts_child, lower='', upper='p', object_count=5,
                state=ShardRange.CLEAVED),
            ShardRange(
                ShardRange.make_path(
                    '.shards_a', 'c', parent_shard.container, ts_child, 1),
                ts_child, lower='p', upper='', object_count=5,
                state=ShardRange.CLEAVED)]
        root_broker.merge_shard_ranges([parent_shard] + child_shards)

        out = StringIO()
        err = StringIO()
        ts_now = next(self.ts_iter)
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err), \
                mock_timestamp_now(ts_now):
            ret = main([root_broker.db_file, 'repair',
                       '--yes', '--min-shard-age', '0'])
        err_lines = err.getvalue().split('\n')
        out_lines = out.getvalue().split('\n')
        self.assertEqual(0, ret, err_lines + out_lines)
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assertNotIn(
            'Repairs necessary to remove overlapping shard ranges.',
            out_lines)
        self.assertEqual(
            ['1 donor shards ignored due to parent-child relationship'
             ' checks'], out_lines[:1])
        updated_ranges = root_broker.get_shard_ranges()
        # Expect no change to shard ranges.
        expected = sorted([parent_shard] + child_shards,
                          key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

    @with_tempdir
    def test_show_and_analyze(self, tempdir):
        broker = self._make_broker()
        broker.set_sharding_sysmeta('Quoted-Root', 'a/c')
        with mock_timestamp_now(next(self.ts_iter)):  # t1
            shard_ranges = make_shard_ranges(
                broker, self.shard_data, '.shards_')
        with mock_timestamp_now(next(self.ts_iter)):
            overlap_shard_ranges_1 = make_shard_ranges(
                broker, self.overlap_shard_data_1, '.shards_')
        with mock_timestamp_now(next(self.ts_iter)):
            overlap_shard_ranges_2 = make_shard_ranges(
                broker, self.overlap_shard_data_2, '.shards_')
        broker.merge_shard_ranges(shard_ranges + overlap_shard_ranges_1 +
                                  overlap_shard_ranges_2)
        self.assertTrue(broker.is_root_container())

        # run show command
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'show'])
        self.assertEqual(0, ret)
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        shard_json = json.loads(out.getvalue())
        expected = sorted(
            shard_ranges + overlap_shard_ranges_1 + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(
            expected, [ShardRange.from_dict(data) for data in shard_json])

        # dump data to a file and then run analyze subcommand
        shard_file = os.path.join(tempdir, 'shards.json')
        with open(shard_file, 'w') as fd:
            json.dump(shard_json, fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([shard_file, 'analyze'])
        self.assertEqual(0, ret)
        self.assertEqual('', err.getvalue())
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Repairs necessary to remove overlapping shard ranges.'],
            out_lines[:1])

        # no changes made to broker
        updated_ranges = broker.get_shard_ranges()
        expected = sorted(
            shard_ranges + overlap_shard_ranges_1 + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(expected, updated_ranges)

        # tweak timestamps to make the preferred path include shards from two
        # sets, so that shards to remove have name-timestamps that are also in
        # shards to keep
        t4 = next(self.ts_iter)
        for sr in shard_ranges[:5] + overlap_shard_ranges_1[5:]:
            sr.timestamp = t4
        broker.merge_shard_ranges(shard_ranges + overlap_shard_ranges_1 +
                                  overlap_shard_ranges_2)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([broker.db_file, 'show'])
        self.assertEqual(0, ret)
        shard_json = json.loads(out.getvalue())
        expected = sorted(
            shard_ranges + overlap_shard_ranges_1 + overlap_shard_ranges_2,
            key=ShardRange.sort_key)
        self.assert_shard_ranges_equal(
            expected, [ShardRange.from_dict(data) for data in shard_json])
        with open(shard_file, 'w') as fd:
            json.dump(shard_json, fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([shard_file, 'analyze'])
        self.assertEqual(0, ret)
        self.assertEqual('', err.getvalue())
        out_lines = out.getvalue().split('\n')
        self.assertEqual(
            ['Repairs necessary to remove overlapping shard ranges.'],
            out_lines[:1])

        filtered_shard_json = [{k: v for k, v in sr.items() if k != 'epoch'}
                               for sr in shard_json]
        with open(shard_file, 'w') as fd:
            json.dump(filtered_shard_json, fd)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            ret = main([shard_file, 'analyze'])
        self.assertEqual(0, ret)
        self.assertEqual('', err.getvalue())
        new_out_lines = out.getvalue().split('\n')
        self.assertEqual(out_lines, new_out_lines)

    def test_subcommand_required(self):
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err):
            ret = main(['db file'])
            self.assertEqual(2, ret)
            err_lines = err.getvalue().split('\n')
            self.assertIn('A sub-command is required.', err_lines)

    def test_dry_run_and_yes_is_invalid(self):
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), \
                mock.patch('sys.stderr', err), \
                self.assertRaises(SystemExit) as cm:
            main(['db file', 'repair', '--dry-run', '--yes'])
        self.assertEqual(2, cm.exception.code)
        err_lines = err.getvalue().split('\n')
        runner = os.path.basename(sys.argv[0])
        self.assertIn(
            'usage: %s path_to_file repair [-h] [--yes | --dry-run]' % runner,
            err_lines[0])
        self.assertIn(
            "argument --yes/-y: not allowed with argument --dry-run/-n",
            err_lines[-2], err_lines)
