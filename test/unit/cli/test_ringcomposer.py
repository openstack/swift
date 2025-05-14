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
import io
import json

import os
import shutil
import tempfile
import unittest

from unittest import mock

from swift.cli import ringcomposer
from test.unit import write_stub_builder


class TestCommands(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.composite_builder_file = os.path.join(self.tmpdir,
                                                   'composite.builder')
        self.composite_ring_file = os.path.join(self.tmpdir,
                                                'composite.ring')

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _run_composer(self, args):
        mock_stdout = io.StringIO()
        mock_stderr = io.StringIO()
        with mock.patch("sys.stdout", mock_stdout):
            with mock.patch("sys.stderr", mock_stderr):
                with self.assertRaises(SystemExit) as cm:
                    ringcomposer.main(args)
        return (cm.exception.code,
                mock_stdout.getvalue(),
                mock_stderr.getvalue())

    def test_unknown_command(self):
        args = ('', self.composite_builder_file, 'unknown')
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(2, exit_code)
        self.assertIn('invalid choice', stderr)

        args = ('', 'non-existent-file', 'unknown')
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(2, exit_code)
        self.assertIn('invalid choice', stderr)

    def test_bad_composite_builder_file(self):
        cmds = (('', self.composite_builder_file, 'show'),
                ('', self.composite_builder_file, 'compose',
                 'b1_file', 'b2_file', '--output', self.composite_ring_file))
        for cmd in cmds:
            try:
                with open(self.composite_builder_file, 'wb') as fd:
                    fd.write(b'not json')
                exit_code, stdout, stderr = self._run_composer(cmd)
                self.assertEqual(2, exit_code)
                self.assertIn('An error occurred while loading the composite '
                              'builder file', stderr)
                self.assertIn(
                    'File does not contain valid composite ring data', stderr)
            except AssertionError as err:
                self.fail('Failed testing command %r due to: %s' % (cmd, err))

    def test_compose(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        b2, b2_file = write_stub_builder(self.tmpdir, 2)
        args = ('', self.composite_builder_file, 'compose', b1_file, b2_file,
                '--output', self.composite_ring_file)
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(0, exit_code)
        self.assertTrue(os.path.exists(self.composite_builder_file))
        self.assertTrue(os.path.exists(self.composite_ring_file))

    def test_compose_existing(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        b2, b2_file = write_stub_builder(self.tmpdir, 2)
        args = ('', self.composite_builder_file, 'compose', b1_file, b2_file,
                '--output', self.composite_ring_file)
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(0, exit_code)
        os.unlink(self.composite_ring_file)
        # no changes - expect failure
        args = ('', self.composite_builder_file, 'compose',
                '--output', self.composite_ring_file)
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(2, exit_code)
        self.assertFalse(os.path.exists(self.composite_ring_file))
        # --force should force output
        args = ('', self.composite_builder_file, 'compose',
                '--output', self.composite_ring_file, '--force')
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(0, exit_code)
        self.assertTrue(os.path.exists(self.composite_ring_file))

    def test_compose_insufficient_component_builder_files(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        args = ('', self.composite_builder_file, 'compose', b1_file,
                '--output', self.composite_ring_file)
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(2, exit_code)
        self.assertIn('An error occurred while composing the ring', stderr)
        self.assertIn('Two or more component builders are required', stderr)
        self.assertFalse(os.path.exists(self.composite_builder_file))
        self.assertFalse(os.path.exists(self.composite_ring_file))

    def test_compose_nonexistent_component_builder_file(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        bad_file = os.path.join(self.tmpdir, 'non-existent-file')
        args = ('', self.composite_builder_file, 'compose', b1_file, bad_file,
                '--output', self.composite_ring_file)
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertIn('An error occurred while composing the ring', stderr)
        self.assertIn('Ring Builder file does not exist', stderr)
        self.assertEqual(2, exit_code)
        self.assertFalse(os.path.exists(self.composite_builder_file))
        self.assertFalse(os.path.exists(self.composite_ring_file))

    def test_compose_fails_to_write_composite_ring_file(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        b2, b2_file = write_stub_builder(self.tmpdir, 2)
        args = ('', self.composite_builder_file, 'compose', b1_file, b2_file,
                '--output', self.composite_ring_file)
        with mock.patch('swift.common.ring.RingData.save',
                        side_effect=IOError('io error')):
            exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(2, exit_code)
        self.assertIn(
            'An error occurred while writing the composite ring file', stderr)
        self.assertIn('io error', stderr)
        self.assertFalse(os.path.exists(self.composite_builder_file))
        self.assertFalse(os.path.exists(self.composite_ring_file))

    def test_compose_fails_to_write_composite_builder_file(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        b2, b2_file = write_stub_builder(self.tmpdir, 2)
        args = ('', self.composite_builder_file, 'compose', b1_file, b2_file,
                '--output', self.composite_ring_file)
        func = 'swift.common.ring.composite_builder.CompositeRingBuilder.save'
        with mock.patch(func, side_effect=IOError('io error')):
            exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(2, exit_code)
        self.assertIn(
            'An error occurred while writing the composite builder file',
            stderr)
        self.assertIn('io error', stderr)
        self.assertFalse(os.path.exists(self.composite_builder_file))
        self.assertTrue(os.path.exists(self.composite_ring_file))

    def test_show(self):
        b1, b1_file = write_stub_builder(self.tmpdir, 1)
        b2, b2_file = write_stub_builder(self.tmpdir, 2)
        args = ('', self.composite_builder_file, 'compose', b1_file, b2_file,
                '--output', self.composite_ring_file)
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(0, exit_code)
        args = ('', self.composite_builder_file, 'show')
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(0, exit_code)
        expected = {'component_builder_files': {b1.id: b1_file,
                                                b2.id: b2_file},
                    'components': [
                        {'id': b1.id,
                         'replicas': b1.replicas,
                         # added replicas devices plus rebalance
                         'version': b1.replicas + 1},
                        {'id': b2.id,
                         'replicas': b2.replicas,
                         # added replicas devices plus rebalance
                         'version': b2.replicas + 1}],
                    'version': 1
                    }
        self.assertEqual(expected, json.loads(stdout))

    def test_show_nonexistent_composite_builder_file(self):
        args = ('', 'non-existent-file', 'show')
        exit_code, stdout, stderr = self._run_composer(args)
        self.assertEqual(2, exit_code)
        self.assertIn(
            'An error occurred while loading the composite builder file',
            stderr)
        self.assertIn("No such file or directory: 'non-existent-file'", stderr)
