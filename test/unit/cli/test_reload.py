# Copyright (c) 2022 NVIDIA
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

from unittest import mock
import signal
import socket
import subprocess
import unittest

from io import StringIO
from swift.cli import reload


@mock.patch('sys.stderr', new_callable=StringIO)
class TestValidateManagerPid(unittest.TestCase):
    def test_good(self, mock_stderr):
        cmd_args = [
            '/usr/local/bin/python3.9',
            '/usr/local/bin/swift-proxy-server',
            '/etc/swift/proxy-server.conf',
            'some',
            'extra',
            'args',
        ]
        with mock.patch.object(reload, 'open', mock.mock_open(
            read_data='\x00'.join(cmd_args) + '\x00'
        )) as mock_open, mock.patch('os.getsid', return_value=123):
            self.assertEqual(reload.validate_manager_pid(123), (
                cmd_args,
                'swift-proxy-server',
            ))
        self.assertEqual(mock_open.mock_calls[0],
                         mock.call('/proc/123/cmdline', 'r'))

    def test_open_error(self, mock_stderr):
        with mock.patch.object(reload, 'open', side_effect=OSError), \
                self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(123)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        self.assertEqual(mock_stderr.getvalue(),
                         'Failed to get process information for 123\n')

    def test_non_python(self, mock_stderr):
        with mock.patch.object(reload, 'open', mock.mock_open(
            read_data='/usr/bin/rsync\x00'
        )), mock.patch('os.getsid', return_value=56), \
                self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(56)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        self.assertEqual(mock_stderr.getvalue(),
                         "Non-swift process: '/usr/bin/rsync'\n")

    def test_non_swift(self, mock_stderr):
        with mock.patch.object(reload, 'open', mock.mock_open(
            read_data='/usr/bin/python\x00some-script\x00'
        )), mock.patch('os.getsid', return_value=123), \
                self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(123)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        self.assertEqual(mock_stderr.getvalue(),
                         "Non-swift process: '/usr/bin/python some-script'\n")

    def test_worker(self, mock_stderr):
        cmd_args = [
            '/usr/bin/python3.9',
            '/usr/bin/swift-proxy-server',
            '/etc/swift/proxy-server.conf',
        ]
        with mock.patch.object(reload, 'open', mock.mock_open(
            read_data='\x00'.join(cmd_args) + '\x00'
        )) as mock_open, mock.patch('os.getsid', return_value=123), \
                self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(56)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        self.assertEqual(mock_stderr.getvalue(),
                         'Process appears to be a swift-proxy-server worker, '
                         'not a manager. Did you mean 123?\n')
        self.assertEqual(mock_open.mock_calls[0],
                         mock.call('/proc/56/cmdline', 'r'))

    def test_non_server(self, mock_stderr):
        cmd_args = [
            '/usr/bin/swift-ring-builder',
            '/etc/swift/object.builder',
            'rebalance',
        ]
        with mock.patch.object(reload, 'open', mock.mock_open(
            read_data='\x00'.join(cmd_args) + '\x00'
        )) as mock_open, mock.patch('os.getsid', return_value=123), \
                self.assertRaises(SystemExit) as caught:
            reload.validate_manager_pid(123)
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        self.assertEqual(mock_stderr.getvalue(),
                         'Process does not support config checks: '
                         'swift-ring-builder\n')
        self.assertEqual(mock_open.mock_calls[0],
                         mock.call('/proc/123/cmdline', 'r'))


class TestMain(unittest.TestCase):
    def setUp(self):
        patcher = mock.patch('sys.stderr', new_callable=StringIO)
        self.mock_stderr = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch('subprocess.check_call')
        self.mock_check_call = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch.object(reload, 'validate_manager_pid')
        self.mock_validate = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch.object(reload, 'NotificationServer')
        self.mock_notify_server = patcher.start()
        self.addCleanup(patcher.stop)

        patcher = mock.patch('os.kill')
        self.mock_kill = patcher.start()
        self.addCleanup(patcher.stop)

    def test_good(self):
        self.mock_validate.return_value = (
            [
                '/usr/bin/swift-proxy-server',
                '/etc/swift/proxy-server.conf'
            ],
            'swift-proxy-server',
        )
        self.mock_notify_server().__enter__().receive.side_effect = [
            b'RELOADING=1',
            b'READY=1',
        ]
        self.assertIsNone(reload.main(['123', '-v']))
        self.assertEqual(self.mock_check_call.mock_calls, [mock.call([
            '/usr/bin/swift-proxy-server',
            '/etc/swift/proxy-server.conf',
            '--test-config',
        ])])
        self.assertEqual(self.mock_kill.mock_calls, [
            mock.call(123, signal.SIGUSR1),
        ])

    @mock.patch('time.time', side_effect=[1, 10, 100, 400])
    def test_timeout(self, mock_time):
        self.mock_validate.return_value = (
            [
                '/usr/bin/python3',
                '/usr/bin/swift-proxy-server',
                '/etc/swift/proxy-server.conf'
            ],
            'swift-proxy-server',
        )
        self.mock_notify_server().__enter__().receive.side_effect = [
            b'RELOADING=1',
            socket.timeout,
        ]
        with self.assertRaises(SystemExit) as caught:
            reload.main(['123'])
        self.assertEqual(caught.exception.args, (reload.EXIT_RELOAD_TIMEOUT,))
        self.assertEqual(self.mock_check_call.mock_calls, [mock.call([
            '/usr/bin/python3',
            '/usr/bin/swift-proxy-server',
            '/etc/swift/proxy-server.conf',
            '--test-config',
        ])])
        self.assertEqual(self.mock_kill.mock_calls, [
            mock.call(123, signal.SIGUSR1),
        ])
        self.assertEqual(self.mock_stderr.getvalue(),
                         'Timed out reloading swift-proxy-server\n')

    def test_check_failed(self):
        self.mock_validate.return_value = (
            [
                '/usr/bin/python3',
                '/usr/bin/swift-object-server',
                '/etc/swift/object-server/1.conf'
            ],
            'swift-object-server',
        )
        self.mock_check_call.side_effect = subprocess.CalledProcessError(
            2, 'swift-object-server')
        with self.assertRaises(SystemExit) as caught:
            reload.main(['123'])
        self.assertEqual(caught.exception.args, (reload.EXIT_RELOAD_FAILED,))
        self.assertEqual(self.mock_check_call.mock_calls, [mock.call([
            '/usr/bin/python3',
            '/usr/bin/swift-object-server',
            '/etc/swift/object-server/1.conf',
            '--test-config',
        ])])
        self.assertEqual(self.mock_kill.mock_calls, [])

    def test_needs_pid(self):
        with self.assertRaises(SystemExit) as caught:
            reload.main([])
        self.assertEqual(caught.exception.args, (reload.EXIT_BAD_PID,))
        msg = 'usage: \nSafely reload WSGI servers'
        self.assertEqual(self.mock_stderr.getvalue()[:len(msg)], msg)
        msg = '\n: error: the following arguments are required: pid\n'
        self.assertEqual(self.mock_stderr.getvalue()[-len(msg):], msg)
