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

from __future__ import unicode_literals

import json
import os
import unittest
import mock
from shutil import rmtree
from tempfile import mkdtemp

from six.moves import cStringIO as StringIO

from swift.cli.manage_shard_ranges import main
from swift.common import utils
from swift.container.backend import ContainerBroker


class TestCliFindShardRanges(unittest.TestCase):
    def setUp(self):
        self.testdir = os.path.join(mkdtemp(), 'tmp_test_cli_find_shards')
        utils.mkdirs(self.testdir)
        rmtree(self.testdir)

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

    def test_find_shard_ranges(self):
        db_file = os.path.join(self.testdir, 'hash.db')
        broker = ContainerBroker(db_file)
        broker.account = 'a'
        broker.container = 'c'
        broker.initialize()
        ts = utils.Timestamp.now()
        broker.merge_objects([
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
