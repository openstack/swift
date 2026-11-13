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

from contextlib import redirect_stdout
from io import StringIO
import os
import signal
import tempfile
import unittest
from unittest import mock

from swift.cli import orphans


class TestParseEtime(unittest.TestCase):
    def test_elapsed_time_is_converted_to_hours(self):
        for etime, hours in (
                ('00:00', 0),
                ('00:01', 1 / 3600),
                ('05:59', 359 / 3600),
                ('06:00', 0.1),
                ('06:01', 361 / 3600),
                ('59:59', 3599 / 3600),
                ('00:00:00', 0),
                ('01:00:00', 1),
                ('01:06:00', 1.1),
                ('23:59:59', 86399 / 3600),
                ('0-00:00:01', 1 / 3600),
                ('1-00:00:00', 24),
                ('1-00:06:00', 24.1),
                ('2-03:04:05', 183845 / 3600),
                ('999-00:00:00', 23976)):
            with self.subTest(etime=etime):
                self.assertEqual(orphans.parse_etime(etime), hours)

    def test_invalid_elapsed_time_raises_value_error(self):
        for etime in ('', '06', '1:02:03:04:05', ':', '::', '1-02::04',
                      'bad:06', '01:bad:06', 'bad-01:02:03', '1.5:00',
                      'nan:00', 'inf:00', '00:00\x00', '00/06'):
            with self.subTest(etime=etime):
                with self.assertRaises(ValueError):
                    orphans.parse_etime(etime)


class TestOrphans(unittest.TestCase):
    def setUp(self):
        super(TestOrphans, self).setUp()
        run_dir = tempfile.TemporaryDirectory()
        self.addCleanup(run_dir.cleanup)
        self.run_dir = run_dir.name
        patcher = mock.patch.object(orphans.subprocess, 'Popen')
        self.mock_popen = patcher.start()
        self.addCleanup(patcher.stop)
        patcher = mock.patch.object(orphans.os, 'kill')
        self.mock_kill = patcher.start()
        self.addCleanup(patcher.stop)

    def test_integer_age_includes_processes_at_least_one_hour_old(
            self):
        self.mock_popen.return_value.communicate.return_value = (
            b'59:59 101 /usr/bin/python /usr/bin/swift-object-auditor\n'
            b'01:00:00 102 /usr/bin/python /usr/bin/swift-object-auditor\n'
            b'01:06:00 103 /usr/bin/python /usr/bin/swift-object-auditor\n',
            None)
        with redirect_stdout(StringIO()) as out:
            orphans.main(['--run-dir', self.run_dir, '--age', '1'])
        self.assertEqual(out.getvalue().splitlines(), [
            'Hours PID Command',
            ' 1.00 102 object-auditor',
            ' 1.10 103 object-auditor',
        ])

    def test_listing_uses_two_decimal_places_for_all_process_ages(self):
        self.mock_popen.return_value.communicate.return_value = (
            b'00:01 101 /usr/bin/python /usr/bin/swift-object-auditor\n'
            b'01:59:00 102 /usr/bin/python /usr/bin/swift-object-auditor\n'
            b'41666-15:59:59 103 /usr/bin/python '
            b'/usr/bin/swift-object-auditor\n',
            None)
        with redirect_stdout(StringIO()) as out:
            orphans.main(['--run-dir', self.run_dir, '-a', '0'])
        self.assertEqual(out.getvalue().splitlines(), [
            '     Hours PID Command',
            '      0.00 101 object-auditor',
            '      1.98 102 object-auditor',
            '1000000.00 103 object-auditor',
        ])
        self.mock_kill.assert_not_called()

    def test_default_age_includes_processes_at_least_one_day_old(
            self):
        self.mock_popen.return_value.communicate.return_value = (
            b'23:59:59 101 /usr/bin/python /usr/bin/swift-object-auditor\n'
            b'1-00:00:00 102 /usr/bin/python /usr/bin/swift-object-auditor\n',
            None)
        with redirect_stdout(StringIO()) as out:
            orphans.main(['--run-dir', self.run_dir])
        self.assertEqual(out.getvalue().splitlines(), [
            'Hours PID Command',
            '24.00 102 object-auditor',
        ])

    def test_excludes_registered_process_and_its_child(
            self):
        with open(os.path.join(self.run_dir, 'object-auditor.pid'), 'w') as f:
            f.write('101\n')
        self.mock_popen.return_value.communicate.side_effect = [
            (b'102\n', None),
            (b'01:00:00 101 /usr/bin/python /usr/bin/swift-object-auditor\n'
             b'01:00:00 102 /usr/bin/python /usr/bin/swift-object-auditor\n'
             b'01:00:00 103 /usr/bin/python /usr/bin/swift-object-auditor\n',
             None),
        ]

        with redirect_stdout(StringIO()) as out:
            orphans.main(['--run-dir', self.run_dir, '-a', '1', '-k',
                          'TERM'])

        rows = [line.split() for line in out.getvalue().splitlines()]
        self.assertEqual(rows[:2], [
            ['Hours', 'PID', 'Command'],
            ['1.00', '103', 'object-auditor'],
        ])
        self.assertEqual(len(rows), 3)  # Header, orphan, and signal message
        self.mock_kill.assert_called_once_with(103, signal.SIGTERM)

    def test_invalid_elapsed_time_is_rejected(self):
        line = b'bad:06 101 /usr/bin/python /usr/bin/swift-object-auditor'
        self.mock_popen.return_value.communicate.return_value = (
            line + b'\n', None)
        with self.assertRaises(SystemExit) as caught:
            orphans.main(['--run-dir', self.run_dir, '-a', '1'])
        self.assertEqual(caught.exception.code,
                         'Could not process etime value from %r' % line)


if __name__ == '__main__':
    unittest.main()
