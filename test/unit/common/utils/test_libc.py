# Copyright (c) 2010-2023 OpenStack Foundation
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

"""Tests for swift.common.utils.libc"""

import ctypes
import errno
import os
import platform
import posix
import tempfile
import unittest

import mock

from swift.common.utils import libc

from test.debug_logger import debug_logger


@mock.patch('ctypes.get_errno')
@mock.patch.object(libc, '_sys_posix_fallocate')
@mock.patch.object(libc, '_sys_fallocate')
@mock.patch.object(libc, 'FALLOCATE_RESERVE', 0)
class TestFallocate(unittest.TestCase):
    def test_config_fallocate_value(self, sys_fallocate_mock,
                                    sys_posix_fallocate_mock, get_errno_mock):
        fallocate_value, is_percent = libc.config_fallocate_value('10%')
        self.assertEqual(fallocate_value, 10)
        self.assertTrue(is_percent)
        fallocate_value, is_percent = libc.config_fallocate_value('10')
        self.assertEqual(fallocate_value, 10)
        self.assertFalse(is_percent)
        try:
            fallocate_value, is_percent = libc.config_fallocate_value('ab%')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: ab% is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = libc.config_fallocate_value('ab')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: ab is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = libc.config_fallocate_value('1%%')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: 1%% is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = libc.config_fallocate_value('10.0')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: 10.0 is an invalid value for '
                                   'fallocate_reserve.')
        fallocate_value, is_percent = libc.config_fallocate_value('10.5%')
        self.assertEqual(fallocate_value, 10.5)
        self.assertTrue(is_percent)
        fallocate_value, is_percent = libc.config_fallocate_value('10.000%')
        self.assertEqual(fallocate_value, 10.000)
        self.assertTrue(is_percent)

    def test_fallocate(self, sys_fallocate_mock,
                       sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        libc.fallocate(1234, 5000 * 2 ** 20)

        # We can't use sys_fallocate_mock.assert_called_once_with because no
        # two ctypes.c_uint64 objects are equal even if their values are
        # equal. Yes, ctypes.c_uint64(123) != ctypes.c_uint64(123).
        calls = sys_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 4)
        self.assertEqual(args[0], 1234)
        self.assertEqual(args[1], libc.FALLOC_FL_KEEP_SIZE)
        self.assertEqual(args[2].value, 0)
        self.assertEqual(args[3].value, 5000 * 2 ** 20)

        sys_posix_fallocate_mock.assert_not_called()

    def test_fallocate_offset(self, sys_fallocate_mock,
                              sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        libc.fallocate(1234, 5000 * 2 ** 20, offset=3 * 2 ** 30)
        calls = sys_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 4)
        self.assertEqual(args[0], 1234)
        self.assertEqual(args[1], libc.FALLOC_FL_KEEP_SIZE)
        self.assertEqual(args[2].value, 3 * 2 ** 30)
        self.assertEqual(args[3].value, 5000 * 2 ** 20)

        sys_posix_fallocate_mock.assert_not_called()

    def test_fallocate_fatal_error(self, sys_fallocate_mock,
                                   sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = -1
        get_errno_mock.return_value = errno.EIO

        with self.assertRaises(OSError) as cm:
            libc.fallocate(1234, 5000 * 2 ** 20)
        self.assertEqual(cm.exception.errno, errno.EIO)

    def test_fallocate_silent_errors(self, sys_fallocate_mock,
                                     sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = -1

        for silent_error in (0, errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL):
            get_errno_mock.return_value = silent_error
            try:
                libc.fallocate(1234, 5678)
            except OSError:
                self.fail("fallocate() raised an error on %d", silent_error)

    def test_posix_fallocate_fallback(self, sys_fallocate_mock,
                                      sys_posix_fallocate_mock,
                                      get_errno_mock):
        sys_fallocate_mock.available = False
        sys_fallocate_mock.side_effect = NotImplementedError

        sys_posix_fallocate_mock.available = True
        sys_posix_fallocate_mock.return_value = 0

        libc.fallocate(1234, 567890)
        sys_fallocate_mock.assert_not_called()

        calls = sys_posix_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 3)
        self.assertEqual(args[0], 1234)
        self.assertEqual(args[1].value, 0)
        self.assertEqual(args[2].value, 567890)

    def test_posix_fallocate_offset(self, sys_fallocate_mock,
                                    sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = False
        sys_fallocate_mock.side_effect = NotImplementedError

        sys_posix_fallocate_mock.available = True
        sys_posix_fallocate_mock.return_value = 0

        libc.fallocate(1234, 5000 * 2 ** 20, offset=3 * 2 ** 30)
        calls = sys_posix_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 3)
        self.assertEqual(args[0], 1234)
        self.assertEqual(args[1].value, 3 * 2 ** 30)
        self.assertEqual(args[2].value, 5000 * 2 ** 20)

        sys_fallocate_mock.assert_not_called()

    def test_no_fallocates_available(self, sys_fallocate_mock,
                                     sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = False
        sys_posix_fallocate_mock.available = False

        with mock.patch("logging.warning") as warning_mock, \
                mock.patch.object(libc, "_fallocate_warned_about_missing",
                                  False):
            libc.fallocate(321, 654)
            libc.fallocate(321, 654)

        sys_fallocate_mock.assert_not_called()
        sys_posix_fallocate_mock.assert_not_called()
        get_errno_mock.assert_not_called()

        self.assertEqual(len(warning_mock.mock_calls), 1)

    def test_arg_bounds(self, sys_fallocate_mock,
                        sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0
        with self.assertRaises(ValueError):
            libc.fallocate(0, 1 << 64, 0)
        with self.assertRaises(ValueError):
            libc.fallocate(0, 0, -1)
        with self.assertRaises(ValueError):
            libc.fallocate(0, 0, 1 << 64)
        self.assertEqual([], sys_fallocate_mock.mock_calls)
        # sanity check
        libc.fallocate(0, 0, 0)
        self.assertEqual(
            [mock.call(0, libc.FALLOC_FL_KEEP_SIZE, mock.ANY, mock.ANY)],
            sys_fallocate_mock.mock_calls)
        # Go confirm the ctypes values separately; apparently == doesn't
        # work the way you'd expect with ctypes :-/
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][2].value, 0)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][3].value, 0)
        sys_fallocate_mock.reset_mock()

        # negative size will be adjusted as 0
        libc.fallocate(0, -1, 0)
        self.assertEqual(
            [mock.call(0, libc.FALLOC_FL_KEEP_SIZE, mock.ANY, mock.ANY)],
            sys_fallocate_mock.mock_calls)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][2].value, 0)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][3].value, 0)


@mock.patch.object(os, 'fstatvfs')
@mock.patch.object(libc, '_sys_fallocate', available=True, return_value=0)
@mock.patch.object(libc, 'FALLOCATE_RESERVE', 0)
@mock.patch.object(libc, 'FALLOCATE_IS_PERCENT', False)
@mock.patch.object(libc, '_fallocate_enabled', True)
class TestFallocateReserve(unittest.TestCase):
    def _statvfs_result(self, f_frsize, f_bavail):
        # Only 3 values are relevant to us, so use zeros for the rest
        f_blocks = 100
        return posix.statvfs_result((0, f_frsize, f_blocks, 0, f_bavail,
                                     0, 0, 0, 0, 0))

    def test_disabled(self, sys_fallocate_mock, fstatvfs_mock):
        libc.disable_fallocate()
        libc.fallocate(123, 456)

        sys_fallocate_mock.assert_not_called()
        fstatvfs_mock.assert_not_called()

    def test_zero_reserve(self, sys_fallocate_mock, fstatvfs_mock):
        libc.fallocate(123, 456)

        fstatvfs_mock.assert_not_called()
        self.assertEqual(len(sys_fallocate_mock.mock_calls), 1)

    def test_enough_space(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1023 allocated, and have 2 blocks
        # of size 1024 free, so succeed
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 2)
        libc.fallocate(88, 1023)

    def test_not_enough_space(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1024 allocated, and have 2 blocks
        # of size 1024 free, so fail
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 2)
        with self.assertRaises(OSError) as catcher:
            libc.fallocate(88, 1024)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 1024'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_not_enough_space_large(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1GB allocated, and have 2 blocks
        # of size 1024 free, so fail
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 2)
        with self.assertRaises(OSError) as catcher:
            libc.fallocate(88, 1 << 30)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail %g <= 1024'
            % (errno.ENOSPC, ((2 * 1024) - (1 << 30))))
        sys_fallocate_mock.assert_not_called()

    def test_enough_space_small_blocks(self, sys_fallocate_mock,
                                       fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1023 allocated, and have 4 blocks
        # of size 512 free, so succeed
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(512, 4)
        libc.fallocate(88, 1023)

    def test_not_enough_space_small_blocks(self, sys_fallocate_mock,
                                           fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1024 allocated, and have 4 blocks
        # of size 512 free, so fail
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(512, 4)
        with self.assertRaises(OSError) as catcher:
            libc.fallocate(88, 1024)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 1024'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_free_space_under_reserve(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 2048 bytes in reserve but have only 3 blocks of size 512, so
        # allocating even 0 bytes fails
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('2048')

        fstatvfs_mock.return_value = self._statvfs_result(512, 3)
        with self.assertRaises(OSError) as catcher:
            libc.fallocate(88, 0)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1536 <= 2048'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_all_reserved(self, sys_fallocate_mock, fstatvfs_mock):
        # Filesystem is empty, but our reserve is bigger than the
        # filesystem, so any allocation will fail
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('9999999999999')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 100)
        self.assertRaises(OSError, libc.fallocate, 88, 0)
        sys_fallocate_mock.assert_not_called()

    def test_enough_space_pct(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1% reserved, filesystem has 3/100 blocks of size 1024 free
        # and file size is 2047, so succeed
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('1%')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 3)
        libc.fallocate(88, 2047)

    def test_not_enough_space_pct(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1% reserved, filesystem has 3/100 blocks of size 1024 free
        # and file size is 2048, so fail
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('1%')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 3)
        with self.assertRaises(OSError) as catcher:
            libc.fallocate(88, 2048)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1 <= 1'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_all_space_reserved_pct(self, sys_fallocate_mock, fstatvfs_mock):
        # Filesystem is empty, but our reserve is the whole filesystem, so
        # any allocation will fail
        libc.FALLOCATE_RESERVE, libc.FALLOCATE_IS_PERCENT = \
            libc.config_fallocate_value('100%')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 100)
        with self.assertRaises(OSError) as catcher:
            libc.fallocate(88, 0)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 100 <= 100'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()


@mock.patch('ctypes.get_errno')
@mock.patch.object(libc, '_sys_fallocate')
class TestPunchHole(unittest.TestCase):
    def test_punch_hole(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        libc.punch_hole(123, 456, 789)

        calls = sys_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 4)
        self.assertEqual(args[0], 123)
        self.assertEqual(
            args[1], libc.FALLOC_FL_PUNCH_HOLE | libc.FALLOC_FL_KEEP_SIZE)
        self.assertEqual(args[2].value, 456)
        self.assertEqual(args[3].value, 789)

    def test_error(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = -1
        get_errno_mock.return_value = errno.EISDIR

        with self.assertRaises(OSError) as cm:
            libc.punch_hole(123, 456, 789)
        self.assertEqual(cm.exception.errno, errno.EISDIR)

    def test_arg_bounds(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        with self.assertRaises(ValueError):
            libc.punch_hole(0, 1, -1)
        with self.assertRaises(ValueError):
            libc.punch_hole(0, 1 << 64, 1)
        with self.assertRaises(ValueError):
            libc.punch_hole(0, -1, 1)
        with self.assertRaises(ValueError):
            libc.punch_hole(0, 1, 0)
        with self.assertRaises(ValueError):
            libc.punch_hole(0, 1, 1 << 64)
        self.assertEqual([], sys_fallocate_mock.mock_calls)

        # sanity check
        libc.punch_hole(0, 0, 1)
        self.assertEqual(
            [mock.call(
                0, libc.FALLOC_FL_PUNCH_HOLE | libc.FALLOC_FL_KEEP_SIZE,
                mock.ANY, mock.ANY)],
            sys_fallocate_mock.mock_calls)
        # Go confirm the ctypes values separately; apparently == doesn't
        # work the way you'd expect with ctypes :-/
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][2].value, 0)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][3].value, 1)

    def test_no_fallocate(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = False

        with self.assertRaises(OSError) as cm:
            libc.punch_hole(123, 456, 789)
        self.assertEqual(cm.exception.errno, errno.ENOTSUP)


class TestPunchHoleReally(unittest.TestCase):
    def setUp(self):
        if not libc._sys_fallocate.available:
            raise unittest.SkipTest("libc._sys_fallocate not available")

    def test_punch_a_hole(self):
        with tempfile.TemporaryFile() as tf:
            tf.write(b"x" * 64 + b"y" * 64 + b"z" * 64)
            tf.flush()

            # knock out the first half of the "y"s
            libc.punch_hole(tf.fileno(), 64, 32)

            tf.seek(0)
            contents = tf.read(4096)
            self.assertEqual(
                contents,
                b"x" * 64 + b"\0" * 32 + b"y" * 32 + b"z" * 64)


class Test_LibcWrapper(unittest.TestCase):
    def test_available_function(self):
        # This should pretty much always exist
        getpid_wrapper = libc._LibcWrapper('getpid')
        self.assertTrue(getpid_wrapper.available)
        self.assertEqual(getpid_wrapper(), os.getpid())

    def test_unavailable_function(self):
        # This won't exist
        no_func_wrapper = libc._LibcWrapper('diffractively_protectorship')
        self.assertFalse(no_func_wrapper.available)
        self.assertRaises(NotImplementedError, no_func_wrapper)

    def test_argument_plumbing(self):
        lseek_wrapper = libc._LibcWrapper('lseek')
        with tempfile.TemporaryFile() as tf:
            tf.write(b"abcdefgh")
            tf.flush()
            lseek_wrapper(tf.fileno(),
                          ctypes.c_uint64(3),
                          # 0 is SEEK_SET
                          0)
            self.assertEqual(tf.read(100), b"defgh")


class TestModifyPriority(unittest.TestCase):
    def test_modify_priority(self):
        pid = os.getpid()
        logger = debug_logger()
        called = {}

        def _fake_setpriority(*args):
            called['setpriority'] = args

        def _fake_syscall(*args):
            called['syscall'] = args

        # Test if current architecture supports changing of priority
        try:
            libc.NR_ioprio_set()
        except OSError as e:
            raise unittest.SkipTest(e)

        with mock.patch('swift.common.utils.libc._libc_setpriority',
                        _fake_setpriority), \
                mock.patch('swift.common.utils.libc._posix_syscall',
                           _fake_syscall):
            called = {}
            # not set / default
            libc.modify_priority({}, logger)
            self.assertEqual(called, {})
            called = {}
            # just nice
            libc.modify_priority({'nice_priority': '1'}, logger)
            self.assertEqual(called, {'setpriority': (0, pid, 1)})
            called = {}
            # just ionice class uses default priority 0
            libc.modify_priority({'ionice_class': 'IOPRIO_CLASS_RT'}, logger)
            architecture = os.uname()[4]
            arch_bits = platform.architecture()[0]
            if architecture == 'x86_64' and arch_bits == '64bit':
                self.assertEqual(called, {'syscall': (251, 1, pid, 1 << 13)})
            elif architecture == 'aarch64' and arch_bits == '64bit':
                self.assertEqual(called, {'syscall': (30, 1, pid, 1 << 13)})
            else:
                self.fail("Unexpected call: %r" % called)
            called = {}
            # just ionice priority is ignored
            libc.modify_priority({'ionice_priority': '4'}, logger)
            self.assertEqual(called, {})
            called = {}
            # bad ionice class
            libc.modify_priority({'ionice_class': 'class_foo'}, logger)
            self.assertEqual(called, {})
            called = {}
            # ionice class & priority
            libc.modify_priority({
                'ionice_class': 'IOPRIO_CLASS_BE',
                'ionice_priority': '4',
            }, logger)
            if architecture == 'x86_64' and arch_bits == '64bit':
                self.assertEqual(called, {
                    'syscall': (251, 1, pid, 2 << 13 | 4)
                })
            elif architecture == 'aarch64' and arch_bits == '64bit':
                self.assertEqual(called, {
                    'syscall': (30, 1, pid, 2 << 13 | 4)
                })
            else:
                self.fail("Unexpected call: %r" % called)
            called = {}
            # all
            libc.modify_priority({
                'nice_priority': '-15',
                'ionice_class': 'IOPRIO_CLASS_IDLE',
                'ionice_priority': '6',
            }, logger)
            if architecture == 'x86_64' and arch_bits == '64bit':
                self.assertEqual(called, {
                    'setpriority': (0, pid, -15),
                    'syscall': (251, 1, pid, 3 << 13 | 6),
                })
            elif architecture == 'aarch64' and arch_bits == '64bit':
                self.assertEqual(called, {
                    'setpriority': (0, pid, -15),
                    'syscall': (30, 1, pid, 3 << 13 | 6),
                })
            else:
                self.fail("Unexpected call: %r" % called)

    def test__NR_ioprio_set(self):
        with mock.patch('os.uname', return_value=('', '', '', '', 'x86_64')), \
                mock.patch('platform.architecture',
                           return_value=('64bit', '')):
            self.assertEqual(251, libc.NR_ioprio_set())

        with mock.patch('os.uname', return_value=('', '', '', '', 'x86_64')), \
                mock.patch('platform.architecture',
                           return_value=('32bit', '')):
            self.assertRaises(OSError, libc.NR_ioprio_set)

        with mock.patch('os.uname',
                        return_value=('', '', '', '', 'aarch64')), \
                mock.patch('platform.architecture',
                           return_value=('64bit', '')):
            self.assertEqual(30, libc.NR_ioprio_set())

        with mock.patch('os.uname',
                        return_value=('', '', '', '', 'aarch64')), \
                mock.patch('platform.architecture',
                           return_value=('32bit', '')):
            self.assertRaises(OSError, libc.NR_ioprio_set)

        with mock.patch('os.uname', return_value=('', '', '', '', 'alpha')), \
                mock.patch('platform.architecture',
                           return_value=('64bit', '')):
            self.assertRaises(OSError, libc.NR_ioprio_set)
