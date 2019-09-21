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
import unittest
import mock
from shutil import rmtree
from tempfile import mkdtemp

from six.moves import cStringIO as StringIO

from swift.cli.manage_shard_ranges import main
from swift.common import utils
from swift.common.utils import Timestamp, ShardRange
from swift.container.backend import ContainerBroker
from test.unit import mock_timestamp_now


class TestManageShardRanges(unittest.TestCase):
    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_cli_find_shards')
        utils.mkdirs(self.testdir)
        rmtree(self.testdir)
        self.shard_data = [
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
        ]

    def tearDown(self):
        rmtree(os.path.dirname(self.testdir))

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
            main([db_file, 'find'])
        self.assert_formatted_json(out.getvalue(), [])
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assert_starts_with(err_lines[1], 'Found 0 ranges in ')

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            main([db_file, 'find', '100'])
        self.assert_formatted_json(out.getvalue(), [])
        err_lines = err.getvalue().split('\n')
        self.assert_starts_with(err_lines[0], 'Loaded db broker for ')
        self.assert_starts_with(err_lines[1], 'Found 0 ranges in ')

        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            main([db_file, 'find', '99'])
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
            main([db_file, 'find', '10'])
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

    def test_info(self):
        broker = self._make_broker()
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            main([broker.db_file, 'info'])
        expected = ['Sharding enabled = True',
                    'Own shard range: None',
                    'db_state = unsharded',
                    'Metadata:',
                    '  X-Container-Sysmeta-Sharding = True']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c.'],
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
                main([broker.db_file, 'info'])
        expected = ['Sharding enabled = True',
                    'Own shard range: {',
                    '  "bytes_used": 0,',
                    '  "deleted": 0,',
                    '  "epoch": "%s",' % epoch.internal,
                    '  "lower": "",',
                    '  "meta_timestamp": "%s",' % now.internal,
                    '  "name": "a/c",',
                    '  "object_count": 0,',
                    '  "state": "sharding",',
                    '  "state_timestamp": "%s",' % now.internal,
                    '  "timestamp": "%s",' % now.internal,
                    '  "upper": ""',
                    '}',
                    'db_state = sharding',
                    'Retiring db id: %s' % retiring_db_id,
                    'Cleaving context: {',
                    '  "cleave_to_row": null,',
                    '  "cleaving_done": false,',
                    '  "cursor": "",',
                    '  "last_cleave_to_row": null,',
                    '  "max_row": -1,',
                    '  "misplaced_done": false,',
                    '  "ranges_done": 0,',
                    '  "ranges_todo": 0,',
                    '  "ref": "%s"' % retiring_db_id,
                    '}',
                    'Metadata:',
                    '  X-Container-Sysmeta-Sharding = True']
        # The json.dumps() in py2 produces trailing space, not in py3.
        result = [x.rstrip() for x in out.getvalue().splitlines()]
        self.assertEqual(expected, result)
        self.assertEqual(['Loaded db broker for a/c.'],
                         err.getvalue().splitlines())

        self.assertTrue(broker.set_sharded_state())
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with mock_timestamp_now(now):
                main([broker.db_file, 'info'])
        expected = ['Sharding enabled = True',
                    'Own shard range: {',
                    '  "bytes_used": 0,',
                    '  "deleted": 0,',
                    '  "epoch": "%s",' % epoch.internal,
                    '  "lower": "",',
                    '  "meta_timestamp": "%s",' % now.internal,
                    '  "name": "a/c",',
                    '  "object_count": 0,',
                    '  "state": "sharding",',
                    '  "state_timestamp": "%s",' % now.internal,
                    '  "timestamp": "%s",' % now.internal,
                    '  "upper": ""',
                    '}',
                    'db_state = sharded',
                    'Metadata:',
                    '  X-Container-Sysmeta-Sharding = True']
        self.assertEqual(expected,
                         [x.rstrip() for x in out.getvalue().splitlines()])
        self.assertEqual(['Loaded db broker for a/c.'],
                         err.getvalue().splitlines())

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
            main([broker.db_file, 'replace', input_file])
        expected = [
            'No shard ranges found to delete.',
            'Injected 10 shard ranges.',
            'Run container-replicator to replicate them to other nodes.',
            'Use the enable sub-command to enable sharding.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c.'],
                         err.getvalue().splitlines())
        self.assertEqual(
            [(data['lower'], data['upper']) for data in self.shard_data],
            [(sr.lower_str, sr.upper_str) for sr in broker.get_shard_ranges()])

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
        broker.update_metadata({'X-Container-Sysmeta-Sharding':
                                (True, Timestamp.now().internal)})
        # no shard ranges
        out = StringIO()
        err = StringIO()
        with self.assertRaises(SystemExit):
            with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
                main([broker.db_file, 'enable'])
        expected = ["WARNING: invalid shard ranges: ['No shard ranges.'].",
                    'Aborting.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c.'],
                         err.getvalue().splitlines())

        # success
        shard_ranges = []
        for data in self.shard_data:
            path = ShardRange.make_path(
                '.shards_a', 'c', 'c', Timestamp.now(), data['index'])
            shard_ranges.append(
                ShardRange(path, Timestamp.now(), data['lower'],
                           data['upper'], data['object_count']))
        broker.merge_shard_ranges(shard_ranges)
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            with mock_timestamp_now() as now:
                main([broker.db_file, 'enable'])
        expected = [
            "Container moved to state 'sharding' with epoch %s." %
            now.internal,
            'Run container-sharder on all nodes to shard the container.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c.'],
                         err.getvalue().splitlines())
        self._assert_enabled(broker, now)

        # already enabled
        out = StringIO()
        err = StringIO()
        with mock.patch('sys.stdout', out), mock.patch('sys.stderr', err):
            main([broker.db_file, 'enable'])
        expected = [
            "Container already in state 'sharding' with epoch %s." %
            now.internal,
            'No action required.',
            'Run container-sharder on all nodes to shard the container.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c.'],
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
                main([broker.db_file, 'find_and_replace', '10', '--enable'])
        expected = [
            'No shard ranges found to delete.',
            'Injected 10 shard ranges.',
            'Run container-replicator to replicate them to other nodes.',
            "Container moved to state 'sharding' with epoch %s." %
            now.internal,
            'Run container-sharder on all nodes to shard the container.']
        self.assertEqual(expected, out.getvalue().splitlines())
        self.assertEqual(['Loaded db broker for a/c.'],
                         err.getvalue().splitlines())
        self._assert_enabled(broker, now)
        self.assertEqual(
            [(data['lower'], data['upper']) for data in self.shard_data],
            [(sr.lower_str, sr.upper_str) for sr in broker.get_shard_ranges()])
