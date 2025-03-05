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
import os
import platform
import tempfile
import unittest

from unittest import mock

from swift.common.utils import libc

from test.debug_logger import debug_logger


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
