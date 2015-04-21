# Copyright (c) 2010-2012 OpenStack Foundation
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

"""Tests for swift.common.utils"""

from test.unit import temptree

import ctypes
import errno
import eventlet
import eventlet.event
import functools
import grp
import logging
import os
import mock
import random
import re
import socket
import stat
import sys
import json
import math

from textwrap import dedent

import tempfile
import threading
import time
import traceback
import unittest
import fcntl
import shutil
from contextlib import nested

from Queue import Queue, Empty
from getpass import getuser
from shutil import rmtree
from StringIO import StringIO
from functools import partial
from tempfile import TemporaryFile, NamedTemporaryFile, mkdtemp
from netifaces import AF_INET6
from mock import MagicMock, patch

from swift.common.exceptions import (Timeout, MessageTimeout,
                                     ConnectionTimeout, LockTimeout,
                                     ReplicationLockTimeout,
                                     MimeInvalid, ThreadPoolDead)
from swift.common import utils
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.swob import Request, Response
from test.unit import FakeLogger


class MockOs(object):

    def __init__(self, pass_funcs=None, called_funcs=None, raise_funcs=None):
        if pass_funcs is None:
            pass_funcs = []
        if called_funcs is None:
            called_funcs = []
        if raise_funcs is None:
            raise_funcs = []

        self.closed_fds = []
        for func in pass_funcs:
            setattr(self, func, self.pass_func)
        self.called_funcs = {}
        for func in called_funcs:
            c_func = partial(self.called_func, func)
            setattr(self, func, c_func)
        for func in raise_funcs:
            r_func = partial(self.raise_func, func)
            setattr(self, func, r_func)

    def pass_func(self, *args, **kwargs):
        pass

    setgroups = chdir = setsid = setgid = setuid = umask = pass_func

    def called_func(self, name, *args, **kwargs):
        self.called_funcs[name] = True

    def raise_func(self, name, *args, **kwargs):
        self.called_funcs[name] = True
        raise OSError()

    def dup2(self, source, target):
        self.closed_fds.append(target)

    def geteuid(self):
        '''Pretend we are running as root.'''
        return 0

    def __getattr__(self, name):
        # I only over-ride portions of the os module
        try:
            return object.__getattr__(self, name)
        except AttributeError:
            return getattr(os, name)


class MockUdpSocket(object):
    def __init__(self, sendto_errno=None):
        self.sent = []
        self.sendto_errno = sendto_errno

    def sendto(self, data, target):
        if self.sendto_errno:
            raise socket.error(self.sendto_errno,
                               'test errno %s' % self.sendto_errno)
        self.sent.append((data, target))

    def close(self):
        pass


class MockSys(object):

    def __init__(self):
        self.stdin = TemporaryFile('w')
        self.stdout = TemporaryFile('r')
        self.stderr = TemporaryFile('r')
        self.__stderr__ = self.stderr
        self.stdio_fds = [self.stdin.fileno(), self.stdout.fileno(),
                          self.stderr.fileno()]

    @property
    def version_info(self):
        return sys.version_info


def reset_loggers():
    if hasattr(utils.get_logger, 'handler4logger'):
        for logger, handler in utils.get_logger.handler4logger.items():
            logger.removeHandler(handler)
        delattr(utils.get_logger, 'handler4logger')
    if hasattr(utils.get_logger, 'console_handler4logger'):
        for logger, h in utils.get_logger.console_handler4logger.items():
            logger.removeHandler(h)
        delattr(utils.get_logger, 'console_handler4logger')
    # Reset the LogAdapter class thread local state. Use get_logger() here
    # to fetch a LogAdapter instance because the items from
    # get_logger.handler4logger above are the underlying logger instances,
    # not the LogAdapter.
    utils.get_logger(None).thread_locals = (None, None)


def reset_logger_state(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        reset_loggers()
        try:
            return f(self, *args, **kwargs)
        finally:
            reset_loggers()
    return wrapper


class TestTimestamp(unittest.TestCase):
    """Tests for swift.common.utils.Timestamp"""

    def test_invalid_input(self):
        self.assertRaises(ValueError, utils.Timestamp, time.time(), offset=-1)

    def test_invalid_string_conversion(self):
        t = utils.Timestamp(time.time())
        self.assertRaises(TypeError, str, t)

    def test_offset_limit(self):
        t = 1417462430.78693
        # can't have a offset above MAX_OFFSET
        self.assertRaises(ValueError, utils.Timestamp, t,
                          offset=utils.MAX_OFFSET + 1)
        # exactly max offset is fine
        ts = utils.Timestamp(t, offset=utils.MAX_OFFSET)
        self.assertEqual(ts.internal, '1417462430.78693_ffffffffffffffff')
        # but you can't offset it further
        self.assertRaises(ValueError, utils.Timestamp, ts.internal, offset=1)
        # unless you start below it
        ts = utils.Timestamp(t, offset=utils.MAX_OFFSET - 1)
        self.assertEqual(utils.Timestamp(ts.internal, offset=1),
                         '1417462430.78693_ffffffffffffffff')

    def test_normal_format_no_offset(self):
        expected = '1402436408.91203'
        test_values = (
            '1402436408.91203',
            '1402436408.91203_00000000',
            '1402436408.912030000',
            '1402436408.912030000_0000000000000',
            '000001402436408.912030000',
            '000001402436408.912030000_0000000000',
            1402436408.91203,
            1402436408.912029,
            1402436408.9120300000000000,
            1402436408.91202999999999999,
            utils.Timestamp(1402436408.91203),
            utils.Timestamp(1402436408.91203, offset=0),
            utils.Timestamp(1402436408.912029),
            utils.Timestamp(1402436408.912029, offset=0),
            utils.Timestamp('1402436408.91203'),
            utils.Timestamp('1402436408.91203', offset=0),
            utils.Timestamp('1402436408.91203_00000000'),
            utils.Timestamp('1402436408.91203_00000000', offset=0),
        )
        for value in test_values:
            timestamp = utils.Timestamp(value)
            self.assertEqual(timestamp.normal, expected)
            # timestamp instance can also compare to string or float
            self.assertEqual(timestamp, expected)
            self.assertEqual(timestamp, float(expected))
            self.assertEqual(timestamp, utils.normalize_timestamp(expected))

    def test_isoformat(self):
        expected = '2014-06-10T22:47:32.054580'
        test_values = (
            '1402440452.05458',
            '1402440452.054579',
            '1402440452.05458_00000000',
            '1402440452.054579_00000000',
            '1402440452.054580000',
            '1402440452.054579999',
            '1402440452.054580000_0000000000000',
            '1402440452.054579999_0000ff00',
            '000001402440452.054580000',
            '000001402440452.0545799',
            '000001402440452.054580000_0000000000',
            '000001402440452.054579999999_00000fffff',
            1402440452.05458,
            1402440452.054579,
            1402440452.0545800000000000,
            1402440452.054579999,
            utils.Timestamp(1402440452.05458),
            utils.Timestamp(1402440452.0545799),
            utils.Timestamp(1402440452.05458, offset=0),
            utils.Timestamp(1402440452.05457999999, offset=0),
            utils.Timestamp(1402440452.05458, offset=100),
            utils.Timestamp(1402440452.054579, offset=100),
            utils.Timestamp('1402440452.05458'),
            utils.Timestamp('1402440452.054579999'),
            utils.Timestamp('1402440452.05458', offset=0),
            utils.Timestamp('1402440452.054579', offset=0),
            utils.Timestamp('1402440452.05458', offset=300),
            utils.Timestamp('1402440452.05457999', offset=300),
            utils.Timestamp('1402440452.05458_00000000'),
            utils.Timestamp('1402440452.05457999_00000000'),
            utils.Timestamp('1402440452.05458_00000000', offset=0),
            utils.Timestamp('1402440452.05457999_00000aaa', offset=0),
            utils.Timestamp('1402440452.05458_00000000', offset=400),
            utils.Timestamp('1402440452.054579_0a', offset=400),
        )
        for value in test_values:
            self.assertEqual(utils.Timestamp(value).isoformat, expected)
        expected = '1970-01-01T00:00:00.000000'
        test_values = (
            '0',
            '0000000000.00000',
            '0000000000.00000_ffffffffffff',
            0,
            0.0,
        )
        for value in test_values:
            self.assertEqual(utils.Timestamp(value).isoformat, expected)

    def test_not_equal(self):
        ts = '1402436408.91203_0000000000000001'
        test_values = (
            utils.Timestamp('1402436408.91203_0000000000000002'),
            utils.Timestamp('1402436408.91203'),
            utils.Timestamp(1402436408.91203),
            utils.Timestamp(1402436408.91204),
            utils.Timestamp(1402436408.91203, offset=0),
            utils.Timestamp(1402436408.91203, offset=2),
        )
        for value in test_values:
            self.assertTrue(value != ts)

    def test_no_force_internal_no_offset(self):
        """Test that internal is the same as normal with no offset"""
        with mock.patch('swift.common.utils.FORCE_INTERNAL', new=False):
            self.assertEqual(utils.Timestamp(0).internal, '0000000000.00000')
            self.assertEqual(utils.Timestamp(1402437380.58186).internal,
                             '1402437380.58186')
            self.assertEqual(utils.Timestamp(1402437380.581859).internal,
                             '1402437380.58186')
            self.assertEqual(utils.Timestamp(0).internal,
                             utils.normalize_timestamp(0))

    def test_no_force_internal_with_offset(self):
        """Test that internal always includes the offset if significant"""
        with mock.patch('swift.common.utils.FORCE_INTERNAL', new=False):
            self.assertEqual(utils.Timestamp(0, offset=1).internal,
                             '0000000000.00000_0000000000000001')
            self.assertEqual(
                utils.Timestamp(1402437380.58186, offset=16).internal,
                '1402437380.58186_0000000000000010')
            self.assertEqual(
                utils.Timestamp(1402437380.581859, offset=240).internal,
                '1402437380.58186_00000000000000f0')
            self.assertEqual(
                utils.Timestamp('1402437380.581859_00000001',
                                offset=240).internal,
                '1402437380.58186_00000000000000f1')

    def test_force_internal(self):
        """Test that internal always includes the offset if forced"""
        with mock.patch('swift.common.utils.FORCE_INTERNAL', new=True):
            self.assertEqual(utils.Timestamp(0).internal,
                             '0000000000.00000_0000000000000000')
            self.assertEqual(utils.Timestamp(1402437380.58186).internal,
                             '1402437380.58186_0000000000000000')
            self.assertEqual(utils.Timestamp(1402437380.581859).internal,
                             '1402437380.58186_0000000000000000')
            self.assertEqual(utils.Timestamp(0, offset=1).internal,
                             '0000000000.00000_0000000000000001')
            self.assertEqual(
                utils.Timestamp(1402437380.58186, offset=16).internal,
                '1402437380.58186_0000000000000010')
            self.assertEqual(
                utils.Timestamp(1402437380.581859, offset=16).internal,
                '1402437380.58186_0000000000000010')

    def test_internal_format_no_offset(self):
        expected = '1402436408.91203_0000000000000000'
        test_values = (
            '1402436408.91203',
            '1402436408.91203_00000000',
            '1402436408.912030000',
            '1402436408.912030000_0000000000000',
            '000001402436408.912030000',
            '000001402436408.912030000_0000000000',
            1402436408.91203,
            1402436408.9120300000000000,
            1402436408.912029,
            1402436408.912029999999999999,
            utils.Timestamp(1402436408.91203),
            utils.Timestamp(1402436408.91203, offset=0),
            utils.Timestamp(1402436408.912029),
            utils.Timestamp(1402436408.91202999999999999, offset=0),
            utils.Timestamp('1402436408.91203'),
            utils.Timestamp('1402436408.91203', offset=0),
            utils.Timestamp('1402436408.912029'),
            utils.Timestamp('1402436408.912029', offset=0),
            utils.Timestamp('1402436408.912029999999999'),
            utils.Timestamp('1402436408.912029999999999', offset=0),
        )
        for value in test_values:
            # timestamp instance is always equivalent
            self.assertEqual(utils.Timestamp(value), expected)
            if utils.FORCE_INTERNAL:
                # the FORCE_INTERNAL flag makes the internal format always
                # include the offset portion of the timestamp even when it's
                # not significant and would be bad during upgrades
                self.assertEqual(utils.Timestamp(value).internal, expected)
            else:
                # unless we FORCE_INTERNAL, when there's no offset the
                # internal format is equivalent to the normalized format
                self.assertEqual(utils.Timestamp(value).internal,
                                 '1402436408.91203')

    def test_internal_format_with_offset(self):
        expected = '1402436408.91203_00000000000000f0'
        test_values = (
            '1402436408.91203_000000f0',
            '1402436408.912030000_0000000000f0',
            '1402436408.912029_000000f0',
            '1402436408.91202999999_0000000000f0',
            '000001402436408.912030000_000000000f0',
            '000001402436408.9120299999_000000000f0',
            utils.Timestamp(1402436408.91203, offset=240),
            utils.Timestamp(1402436408.912029, offset=240),
            utils.Timestamp('1402436408.91203', offset=240),
            utils.Timestamp('1402436408.91203_00000000', offset=240),
            utils.Timestamp('1402436408.91203_0000000f', offset=225),
            utils.Timestamp('1402436408.9120299999', offset=240),
            utils.Timestamp('1402436408.9120299999_00000000', offset=240),
            utils.Timestamp('1402436408.9120299999_00000010', offset=224),
        )
        for value in test_values:
            timestamp = utils.Timestamp(value)
            self.assertEqual(timestamp.internal, expected)
            # can compare with offset if the string is internalized
            self.assertEqual(timestamp, expected)
            # if comparison value only includes the normalized portion and the
            # timestamp includes an offset, it is considered greater
            normal = utils.Timestamp(expected).normal
            self.assertTrue(timestamp > normal,
                            '%r is not bigger than %r given %r' % (
                                timestamp, normal, value))
            self.assertTrue(timestamp > float(normal),
                            '%r is not bigger than %f given %r' % (
                                timestamp, float(normal), value))

    def test_int(self):
        expected = 1402437965
        test_values = (
            '1402437965.91203',
            '1402437965.91203_00000000',
            '1402437965.912030000',
            '1402437965.912030000_0000000000000',
            '000001402437965.912030000',
            '000001402437965.912030000_0000000000',
            1402437965.91203,
            1402437965.9120300000000000,
            1402437965.912029,
            1402437965.912029999999999999,
            utils.Timestamp(1402437965.91203),
            utils.Timestamp(1402437965.91203, offset=0),
            utils.Timestamp(1402437965.91203, offset=500),
            utils.Timestamp(1402437965.912029),
            utils.Timestamp(1402437965.91202999999999999, offset=0),
            utils.Timestamp(1402437965.91202999999999999, offset=300),
            utils.Timestamp('1402437965.91203'),
            utils.Timestamp('1402437965.91203', offset=0),
            utils.Timestamp('1402437965.91203', offset=400),
            utils.Timestamp('1402437965.912029'),
            utils.Timestamp('1402437965.912029', offset=0),
            utils.Timestamp('1402437965.912029', offset=200),
            utils.Timestamp('1402437965.912029999999999'),
            utils.Timestamp('1402437965.912029999999999', offset=0),
            utils.Timestamp('1402437965.912029999999999', offset=100),
        )
        for value in test_values:
            timestamp = utils.Timestamp(value)
            self.assertEqual(int(timestamp), expected)
            self.assertTrue(timestamp > expected)

    def test_float(self):
        expected = 1402438115.91203
        test_values = (
            '1402438115.91203',
            '1402438115.91203_00000000',
            '1402438115.912030000',
            '1402438115.912030000_0000000000000',
            '000001402438115.912030000',
            '000001402438115.912030000_0000000000',
            1402438115.91203,
            1402438115.9120300000000000,
            1402438115.912029,
            1402438115.912029999999999999,
            utils.Timestamp(1402438115.91203),
            utils.Timestamp(1402438115.91203, offset=0),
            utils.Timestamp(1402438115.91203, offset=500),
            utils.Timestamp(1402438115.912029),
            utils.Timestamp(1402438115.91202999999999999, offset=0),
            utils.Timestamp(1402438115.91202999999999999, offset=300),
            utils.Timestamp('1402438115.91203'),
            utils.Timestamp('1402438115.91203', offset=0),
            utils.Timestamp('1402438115.91203', offset=400),
            utils.Timestamp('1402438115.912029'),
            utils.Timestamp('1402438115.912029', offset=0),
            utils.Timestamp('1402438115.912029', offset=200),
            utils.Timestamp('1402438115.912029999999999'),
            utils.Timestamp('1402438115.912029999999999', offset=0),
            utils.Timestamp('1402438115.912029999999999', offset=100),
        )
        tolerance = 0.00001
        minimum = expected - tolerance
        maximum = expected + tolerance
        for value in test_values:
            timestamp = utils.Timestamp(value)
            self.assertTrue(float(timestamp) > minimum,
                            '%f is not bigger than %f given %r' % (
                                timestamp, minimum, value))
            self.assertTrue(float(timestamp) < maximum,
                            '%f is not smaller than %f given %r' % (
                                timestamp, maximum, value))
            # direct comparison of timestamp works too
            self.assertTrue(timestamp > minimum,
                            '%s is not bigger than %f given %r' % (
                                timestamp.normal, minimum, value))
            self.assertTrue(timestamp < maximum,
                            '%s is not smaller than %f given %r' % (
                                timestamp.normal, maximum, value))
            # ... even against strings
            self.assertTrue(timestamp > '%f' % minimum,
                            '%s is not bigger than %s given %r' % (
                                timestamp.normal, minimum, value))
            self.assertTrue(timestamp < '%f' % maximum,
                            '%s is not smaller than %s given %r' % (
                                timestamp.normal, maximum, value))

    def test_false(self):
        self.assertFalse(utils.Timestamp(0))
        self.assertFalse(utils.Timestamp(0, offset=0))
        self.assertFalse(utils.Timestamp('0'))
        self.assertFalse(utils.Timestamp('0', offset=0))
        self.assertFalse(utils.Timestamp(0.0))
        self.assertFalse(utils.Timestamp(0.0, offset=0))
        self.assertFalse(utils.Timestamp('0.0'))
        self.assertFalse(utils.Timestamp('0.0', offset=0))
        self.assertFalse(utils.Timestamp(00000000.00000000))
        self.assertFalse(utils.Timestamp(00000000.00000000, offset=0))
        self.assertFalse(utils.Timestamp('00000000.00000000'))
        self.assertFalse(utils.Timestamp('00000000.00000000', offset=0))

    def test_true(self):
        self.assertTrue(utils.Timestamp(1))
        self.assertTrue(utils.Timestamp(1, offset=1))
        self.assertTrue(utils.Timestamp(0, offset=1))
        self.assertTrue(utils.Timestamp('1'))
        self.assertTrue(utils.Timestamp('1', offset=1))
        self.assertTrue(utils.Timestamp('0', offset=1))
        self.assertTrue(utils.Timestamp(1.1))
        self.assertTrue(utils.Timestamp(1.1, offset=1))
        self.assertTrue(utils.Timestamp(0.0, offset=1))
        self.assertTrue(utils.Timestamp('1.1'))
        self.assertTrue(utils.Timestamp('1.1', offset=1))
        self.assertTrue(utils.Timestamp('0.0', offset=1))
        self.assertTrue(utils.Timestamp(11111111.11111111))
        self.assertTrue(utils.Timestamp(11111111.11111111, offset=1))
        self.assertTrue(utils.Timestamp(00000000.00000000, offset=1))
        self.assertTrue(utils.Timestamp('11111111.11111111'))
        self.assertTrue(utils.Timestamp('11111111.11111111', offset=1))
        self.assertTrue(utils.Timestamp('00000000.00000000', offset=1))

    def test_greater_no_offset(self):
        now = time.time()
        older = now - 1
        timestamp = utils.Timestamp(now)
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443112.213252, '1402443112.213252', '1402443112.213252_ffff',
            older, '%f' % older, '%f_0000ffff' % older,
        )
        for value in test_values:
            other = utils.Timestamp(value)
            self.assertNotEqual(timestamp, other)  # sanity
            self.assertTrue(timestamp > value,
                            '%r is not greater than %r given %r' % (
                                timestamp, value, value))
            self.assertTrue(timestamp > other,
                            '%r is not greater than %r given %r' % (
                                timestamp, other, value))
            self.assertTrue(timestamp > other.normal,
                            '%r is not greater than %r given %r' % (
                                timestamp, other.normal, value))
            self.assertTrue(timestamp > other.internal,
                            '%r is not greater than %r given %r' % (
                                timestamp, other.internal, value))
            self.assertTrue(timestamp > float(other),
                            '%r is not greater than %r given %r' % (
                                timestamp, float(other), value))
            self.assertTrue(timestamp > int(other),
                            '%r is not greater than %r given %r' % (
                                timestamp, int(other), value))

    def test_greater_with_offset(self):
        now = time.time()
        older = now - 1
        test_values = (
            0, '0', 0.0, '0.0', '0000.0000', '000.000_000',
            1, '1', 1.1, '1.1', '1111.1111', '111.111_111',
            1402443346.935174, '1402443346.93517', '1402443346.935169_ffff',
            older, '%f' % older, '%f_0000ffff' % older,
            now, '%f' % now, '%f_00000000' % now,
        )
        for offset in range(1, 1000, 100):
            timestamp = utils.Timestamp(now, offset=offset)
            for value in test_values:
                other = utils.Timestamp(value)
                self.assertNotEqual(timestamp, other)  # sanity
                self.assertTrue(timestamp > value,
                                '%r is not greater than %r given %r' % (
                                    timestamp, value, value))
                self.assertTrue(timestamp > other,
                                '%r is not greater than %r given %r' % (
                                    timestamp, other, value))
                self.assertTrue(timestamp > other.normal,
                                '%r is not greater than %r given %r' % (
                                    timestamp, other.normal, value))
                self.assertTrue(timestamp > other.internal,
                                '%r is not greater than %r given %r' % (
                                    timestamp, other.internal, value))
                self.assertTrue(timestamp > float(other),
                                '%r is not greater than %r given %r' % (
                                    timestamp, float(other), value))
                self.assertTrue(timestamp > int(other),
                                '%r is not greater than %r given %r' % (
                                    timestamp, int(other), value))

    def test_smaller_no_offset(self):
        now = time.time()
        newer = now + 1
        timestamp = utils.Timestamp(now)
        test_values = (
            9999999999.99999, '9999999999.99999', '9999999999.99999_ffff',
            newer, '%f' % newer, '%f_0000ffff' % newer,
        )
        for value in test_values:
            other = utils.Timestamp(value)
            self.assertNotEqual(timestamp, other)  # sanity
            self.assertTrue(timestamp < value,
                            '%r is not smaller than %r given %r' % (
                                timestamp, value, value))
            self.assertTrue(timestamp < other,
                            '%r is not smaller than %r given %r' % (
                                timestamp, other, value))
            self.assertTrue(timestamp < other.normal,
                            '%r is not smaller than %r given %r' % (
                                timestamp, other.normal, value))
            self.assertTrue(timestamp < other.internal,
                            '%r is not smaller than %r given %r' % (
                                timestamp, other.internal, value))
            self.assertTrue(timestamp < float(other),
                            '%r is not smaller than %r given %r' % (
                                timestamp, float(other), value))
            self.assertTrue(timestamp < int(other),
                            '%r is not smaller than %r given %r' % (
                                timestamp, int(other), value))

    def test_smaller_with_offset(self):
        now = time.time()
        newer = now + 1
        test_values = (
            9999999999.99999, '9999999999.99999', '9999999999.99999_ffff',
            newer, '%f' % newer, '%f_0000ffff' % newer,
        )
        for offset in range(1, 1000, 100):
            timestamp = utils.Timestamp(now, offset=offset)
            for value in test_values:
                other = utils.Timestamp(value)
                self.assertNotEqual(timestamp, other)  # sanity
                self.assertTrue(timestamp < value,
                                '%r is not smaller than %r given %r' % (
                                    timestamp, value, value))
                self.assertTrue(timestamp < other,
                                '%r is not smaller than %r given %r' % (
                                    timestamp, other, value))
                self.assertTrue(timestamp < other.normal,
                                '%r is not smaller than %r given %r' % (
                                    timestamp, other.normal, value))
                self.assertTrue(timestamp < other.internal,
                                '%r is not smaller than %r given %r' % (
                                    timestamp, other.internal, value))
                self.assertTrue(timestamp < float(other),
                                '%r is not smaller than %r given %r' % (
                                    timestamp, float(other), value))
                self.assertTrue(timestamp < int(other),
                                '%r is not smaller than %r given %r' % (
                                    timestamp, int(other), value))

    def test_ordering(self):
        given = [
            '1402444820.62590_000000000000000a',
            '1402444820.62589_0000000000000001',
            '1402444821.52589_0000000000000004',
            '1402444920.62589_0000000000000004',
            '1402444821.62589_000000000000000a',
            '1402444821.72589_000000000000000a',
            '1402444920.62589_0000000000000002',
            '1402444820.62589_0000000000000002',
            '1402444820.62589_000000000000000a',
            '1402444820.62590_0000000000000004',
            '1402444920.62589_000000000000000a',
            '1402444820.62590_0000000000000002',
            '1402444821.52589_0000000000000002',
            '1402444821.52589_0000000000000000',
            '1402444920.62589',
            '1402444821.62589_0000000000000004',
            '1402444821.72589_0000000000000001',
            '1402444820.62590',
            '1402444820.62590_0000000000000001',
            '1402444820.62589_0000000000000004',
            '1402444821.72589_0000000000000000',
            '1402444821.52589_000000000000000a',
            '1402444821.72589_0000000000000004',
            '1402444821.62589',
            '1402444821.52589_0000000000000001',
            '1402444821.62589_0000000000000001',
            '1402444821.62589_0000000000000002',
            '1402444821.72589_0000000000000002',
            '1402444820.62589',
            '1402444920.62589_0000000000000001']
        expected = [
            '1402444820.62589',
            '1402444820.62589_0000000000000001',
            '1402444820.62589_0000000000000002',
            '1402444820.62589_0000000000000004',
            '1402444820.62589_000000000000000a',
            '1402444820.62590',
            '1402444820.62590_0000000000000001',
            '1402444820.62590_0000000000000002',
            '1402444820.62590_0000000000000004',
            '1402444820.62590_000000000000000a',
            '1402444821.52589',
            '1402444821.52589_0000000000000001',
            '1402444821.52589_0000000000000002',
            '1402444821.52589_0000000000000004',
            '1402444821.52589_000000000000000a',
            '1402444821.62589',
            '1402444821.62589_0000000000000001',
            '1402444821.62589_0000000000000002',
            '1402444821.62589_0000000000000004',
            '1402444821.62589_000000000000000a',
            '1402444821.72589',
            '1402444821.72589_0000000000000001',
            '1402444821.72589_0000000000000002',
            '1402444821.72589_0000000000000004',
            '1402444821.72589_000000000000000a',
            '1402444920.62589',
            '1402444920.62589_0000000000000001',
            '1402444920.62589_0000000000000002',
            '1402444920.62589_0000000000000004',
            '1402444920.62589_000000000000000a',
        ]
        # less visual version
        """
        now = time.time()
        given = [
            utils.Timestamp(now + i, offset=offset).internal
            for i in (0, 0.00001, 0.9, 1.0, 1.1, 100.0)
            for offset in (0, 1, 2, 4, 10)
        ]
        expected = [t for t in given]
        random.shuffle(given)
        """
        self.assertEqual(len(given), len(expected))  # sanity
        timestamps = [utils.Timestamp(t) for t in given]
        # our expected values don't include insignificant offsets
        with mock.patch('swift.common.utils.FORCE_INTERNAL', new=False):
            self.assertEqual(
                [t.internal for t in sorted(timestamps)], expected)
            # string sorting works as well
            self.assertEqual(
                sorted([t.internal for t in timestamps]), expected)


class TestUtils(unittest.TestCase):
    """Tests for swift.common.utils """

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'

    def test_lock_path(self):
        tmpdir = mkdtemp()
        try:
            with utils.lock_path(tmpdir, 0.1):
                exc = None
                success = False
                try:
                    with utils.lock_path(tmpdir, 0.1):
                        success = True
                except LockTimeout as err:
                    exc = err
                self.assertTrue(exc is not None)
                self.assertTrue(not success)
        finally:
            shutil.rmtree(tmpdir)

    def test_lock_path_num_sleeps(self):
        tmpdir = mkdtemp()
        num_short_calls = [0]
        exception_raised = [False]

        def my_sleep(to_sleep):
            if to_sleep == 0.01:
                num_short_calls[0] += 1
            else:
                raise Exception('sleep time changed: %s' % to_sleep)

        try:
            with mock.patch('swift.common.utils.sleep', my_sleep):
                with utils.lock_path(tmpdir):
                    with utils.lock_path(tmpdir):
                        pass
        except Exception as e:
            exception_raised[0] = True
            self.assertTrue('sleep time changed' in str(e))
        finally:
            shutil.rmtree(tmpdir)
        self.assertEqual(num_short_calls[0], 11)
        self.assertTrue(exception_raised[0])

    def test_lock_path_class(self):
        tmpdir = mkdtemp()
        try:
            with utils.lock_path(tmpdir, 0.1, ReplicationLockTimeout):
                exc = None
                exc2 = None
                success = False
                try:
                    with utils.lock_path(tmpdir, 0.1, ReplicationLockTimeout):
                        success = True
                except ReplicationLockTimeout as err:
                    exc = err
                except LockTimeout as err:
                    exc2 = err
                self.assertTrue(exc is not None)
                self.assertTrue(exc2 is None)
                self.assertTrue(not success)
                exc = None
                exc2 = None
                success = False
                try:
                    with utils.lock_path(tmpdir, 0.1):
                        success = True
                except ReplicationLockTimeout as err:
                    exc = err
                except LockTimeout as err:
                    exc2 = err
                self.assertTrue(exc is None)
                self.assertTrue(exc2 is not None)
                self.assertTrue(not success)
        finally:
            shutil.rmtree(tmpdir)

    def test_normalize_timestamp(self):
        # Test swift.common.utils.normalize_timestamp
        self.assertEquals(utils.normalize_timestamp('1253327593.48174'),
                          "1253327593.48174")
        self.assertEquals(utils.normalize_timestamp(1253327593.48174),
                          "1253327593.48174")
        self.assertEquals(utils.normalize_timestamp('1253327593.48'),
                          "1253327593.48000")
        self.assertEquals(utils.normalize_timestamp(1253327593.48),
                          "1253327593.48000")
        self.assertEquals(utils.normalize_timestamp('253327593.48'),
                          "0253327593.48000")
        self.assertEquals(utils.normalize_timestamp(253327593.48),
                          "0253327593.48000")
        self.assertEquals(utils.normalize_timestamp('1253327593'),
                          "1253327593.00000")
        self.assertEquals(utils.normalize_timestamp(1253327593),
                          "1253327593.00000")
        self.assertRaises(ValueError, utils.normalize_timestamp, '')
        self.assertRaises(ValueError, utils.normalize_timestamp, 'abc')

    def test_normalize_delete_at_timestamp(self):
        self.assertEquals(
            utils.normalize_delete_at_timestamp(1253327593),
            '1253327593')
        self.assertEquals(
            utils.normalize_delete_at_timestamp(1253327593.67890),
            '1253327593')
        self.assertEquals(
            utils.normalize_delete_at_timestamp('1253327593'),
            '1253327593')
        self.assertEquals(
            utils.normalize_delete_at_timestamp('1253327593.67890'),
            '1253327593')
        self.assertEquals(
            utils.normalize_delete_at_timestamp(-1253327593),
            '0000000000')
        self.assertEquals(
            utils.normalize_delete_at_timestamp(-1253327593.67890),
            '0000000000')
        self.assertEquals(
            utils.normalize_delete_at_timestamp('-1253327593'),
            '0000000000')
        self.assertEquals(
            utils.normalize_delete_at_timestamp('-1253327593.67890'),
            '0000000000')
        self.assertEquals(
            utils.normalize_delete_at_timestamp(71253327593),
            '9999999999')
        self.assertEquals(
            utils.normalize_delete_at_timestamp(71253327593.67890),
            '9999999999')
        self.assertEquals(
            utils.normalize_delete_at_timestamp('71253327593'),
            '9999999999')
        self.assertEquals(
            utils.normalize_delete_at_timestamp('71253327593.67890'),
            '9999999999')
        self.assertRaises(ValueError, utils.normalize_timestamp, '')
        self.assertRaises(ValueError, utils.normalize_timestamp, 'abc')

    def test_last_modified_date_to_timestamp(self):
        expectations = {
            '1970-01-01T00:00:00.000000': 0.0,
            '2014-02-28T23:22:36.698390': 1393629756.698390,
            '2011-03-19T04:03:00.604554': 1300507380.604554,
        }
        for last_modified, ts in expectations.items():
            real = utils.last_modified_date_to_timestamp(last_modified)
            self.assertEqual(real, ts, "failed for %s" % last_modified)

    def test_last_modified_date_to_timestamp_when_system_not_UTC(self):
        try:
            old_tz = os.environ.get('TZ')
            # Western Argentina Summer Time. Found in glibc manual; this
            # timezone always has a non-zero offset from UTC, so this test is
            # always meaningful.
            os.environ['TZ'] = 'WART4WARST,J1/0,J365/25'

            self.assertEqual(utils.last_modified_date_to_timestamp(
                '1970-01-01T00:00:00.000000'),
                0.0)

        finally:
            if old_tz is not None:
                os.environ['TZ'] = old_tz
            else:
                os.environ.pop('TZ')

    def test_backwards(self):
        # Test swift.common.utils.backward

        # The lines are designed so that the function would encounter
        # all of the boundary conditions and typical conditions.
        # Block boundaries are marked with '<>' characters
        blocksize = 25
        lines = ['123456789x12345678><123456789\n',  # block larger than rest
                 '123456789x123>\n',  # block ends just before \n character
                 '123423456789\n',
                 '123456789x\n',  # block ends at the end of line
                 '<123456789x123456789x123\n',
                 '<6789x123\n',  # block ends at the beginning of the line
                 '6789x1234\n',
                 '1234><234\n',  # block ends typically in the middle of line
                 '123456789x123456789\n']

        with TemporaryFile('r+w') as f:
            for line in lines:
                f.write(line)

            count = len(lines) - 1
            for line in utils.backward(f, blocksize):
                self.assertEquals(line, lines[count].split('\n')[0])
                count -= 1

        # Empty file case
        with TemporaryFile('r') as f:
            self.assertEquals([], list(utils.backward(f)))

    def test_mkdirs(self):
        testdir_base = mkdtemp()
        testroot = os.path.join(testdir_base, 'mkdirs')
        try:
            self.assert_(not os.path.exists(testroot))
            utils.mkdirs(testroot)
            self.assert_(os.path.exists(testroot))
            utils.mkdirs(testroot)
            self.assert_(os.path.exists(testroot))
            rmtree(testroot, ignore_errors=1)

            testdir = os.path.join(testroot, 'one/two/three')
            self.assert_(not os.path.exists(testdir))
            utils.mkdirs(testdir)
            self.assert_(os.path.exists(testdir))
            utils.mkdirs(testdir)
            self.assert_(os.path.exists(testdir))
            rmtree(testroot, ignore_errors=1)

            open(testroot, 'wb').close()
            self.assert_(not os.path.exists(testdir))
            self.assertRaises(OSError, utils.mkdirs, testdir)
            os.unlink(testroot)
        finally:
            rmtree(testdir_base)

    def test_split_path(self):
        # Test swift.common.utils.split_account_path
        self.assertRaises(ValueError, utils.split_path, '')
        self.assertRaises(ValueError, utils.split_path, '/')
        self.assertRaises(ValueError, utils.split_path, '//')
        self.assertEquals(utils.split_path('/a'), ['a'])
        self.assertRaises(ValueError, utils.split_path, '//a')
        self.assertEquals(utils.split_path('/a/'), ['a'])
        self.assertRaises(ValueError, utils.split_path, '/a/c')
        self.assertRaises(ValueError, utils.split_path, '//c')
        self.assertRaises(ValueError, utils.split_path, '/a/c/')
        self.assertRaises(ValueError, utils.split_path, '/a//')
        self.assertRaises(ValueError, utils.split_path, '/a', 2)
        self.assertRaises(ValueError, utils.split_path, '/a', 2, 3)
        self.assertRaises(ValueError, utils.split_path, '/a', 2, 3, True)
        self.assertEquals(utils.split_path('/a/c', 2), ['a', 'c'])
        self.assertEquals(utils.split_path('/a/c/o', 3), ['a', 'c', 'o'])
        self.assertRaises(ValueError, utils.split_path, '/a/c/o/r', 3, 3)
        self.assertEquals(utils.split_path('/a/c/o/r', 3, 3, True),
                          ['a', 'c', 'o/r'])
        self.assertEquals(utils.split_path('/a/c', 2, 3, True),
                          ['a', 'c', None])
        self.assertRaises(ValueError, utils.split_path, '/a', 5, 4)
        self.assertEquals(utils.split_path('/a/c/', 2), ['a', 'c'])
        self.assertEquals(utils.split_path('/a/c/', 2, 3), ['a', 'c', ''])
        try:
            utils.split_path('o\nn e', 2)
        except ValueError as err:
            self.assertEquals(str(err), 'Invalid path: o%0An%20e')
        try:
            utils.split_path('o\nn e', 2, 3, True)
        except ValueError as err:
            self.assertEquals(str(err), 'Invalid path: o%0An%20e')

    def test_validate_device_partition(self):
        # Test swift.common.utils.validate_device_partition
        utils.validate_device_partition('foo', 'bar')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, '', '')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, '', 'foo')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, 'foo', '')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, 'foo/bar', 'foo')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, 'foo', 'foo/bar')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, '.', 'foo')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, '..', 'foo')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, 'foo', '.')
        self.assertRaises(ValueError,
                          utils.validate_device_partition, 'foo', '..')
        try:
            utils.validate_device_partition('o\nn e', 'foo')
        except ValueError as err:
            self.assertEquals(str(err), 'Invalid device: o%0An%20e')
        try:
            utils.validate_device_partition('foo', 'o\nn e')
        except ValueError as err:
            self.assertEquals(str(err), 'Invalid partition: o%0An%20e')

    def test_NullLogger(self):
        # Test swift.common.utils.NullLogger
        sio = StringIO()
        nl = utils.NullLogger()
        nl.write('test')
        self.assertEquals(sio.getvalue(), '')

    def test_LoggerFileObject(self):
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        sio = StringIO()
        handler = logging.StreamHandler(sio)
        logger = logging.getLogger()
        logger.addHandler(handler)
        lfo_stdout = utils.LoggerFileObject(logger)
        lfo_stderr = utils.LoggerFileObject(logger)
        lfo_stderr = utils.LoggerFileObject(logger, 'STDERR')
        print 'test1'
        self.assertEquals(sio.getvalue(), '')
        sys.stdout = lfo_stdout
        print 'test2'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\n')
        sys.stderr = lfo_stderr
        print >> sys.stderr, 'test4'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n')
        sys.stdout = orig_stdout
        print 'test5'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n')
        print >> sys.stderr, 'test6'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                          'STDERR: test6\n')
        sys.stderr = orig_stderr
        print 'test8'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                          'STDERR: test6\n')
        lfo_stdout.writelines(['a', 'b', 'c'])
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                          'STDERR: test6\nSTDOUT: a#012b#012c\n')
        lfo_stdout.close()
        lfo_stderr.close()
        lfo_stdout.write('d')
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                          'STDERR: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
        lfo_stdout.flush()
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                          'STDERR: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
        for lfo in (lfo_stdout, lfo_stderr):
            got_exc = False
            try:
                for line in lfo:
                    pass
            except Exception:
                got_exc = True
            self.assert_(got_exc)
            got_exc = False
            try:
                for line in lfo.xreadlines():
                    pass
            except Exception:
                got_exc = True
            self.assert_(got_exc)
            self.assertRaises(IOError, lfo.read)
            self.assertRaises(IOError, lfo.read, 1024)
            self.assertRaises(IOError, lfo.readline)
            self.assertRaises(IOError, lfo.readline, 1024)
            lfo.tell()

    def test_parse_options(self):
        # Get a file that is definitely on disk
        with NamedTemporaryFile() as f:
            conf_file = f.name
            conf, options = utils.parse_options(test_args=[conf_file])
            self.assertEquals(conf, conf_file)
            # assert defaults
            self.assertEquals(options['verbose'], False)
            self.assert_('once' not in options)
            # assert verbose as option
            conf, options = utils.parse_options(test_args=[conf_file, '-v'])
            self.assertEquals(options['verbose'], True)
            # check once option
            conf, options = utils.parse_options(test_args=[conf_file],
                                                once=True)
            self.assertEquals(options['once'], False)
            test_args = [conf_file, '--once']
            conf, options = utils.parse_options(test_args=test_args, once=True)
            self.assertEquals(options['once'], True)
            # check options as arg parsing
            test_args = [conf_file, 'once', 'plugin_name', 'verbose']
            conf, options = utils.parse_options(test_args=test_args, once=True)
            self.assertEquals(options['verbose'], True)
            self.assertEquals(options['once'], True)
            self.assertEquals(options['extra_args'], ['plugin_name'])

    def test_parse_options_errors(self):
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        stdo = StringIO()
        stde = StringIO()
        utils.sys.stdout = stdo
        utils.sys.stderr = stde
        self.assertRaises(SystemExit, utils.parse_options, once=True,
                          test_args=[])
        self.assert_('missing config' in stdo.getvalue())

        # verify conf file must exist, context manager will delete temp file
        with NamedTemporaryFile() as f:
            conf_file = f.name
        self.assertRaises(SystemExit, utils.parse_options, once=True,
                          test_args=[conf_file])
        self.assert_('unable to locate' in stdo.getvalue())

        # reset stdio
        utils.sys.stdout = orig_stdout
        utils.sys.stderr = orig_stderr

    def test_dump_recon_cache(self):
        testdir_base = mkdtemp()
        testcache_file = os.path.join(testdir_base, 'cache.recon')
        logger = utils.get_logger(None, 'server', log_route='server')
        try:
            submit_dict = {'key1': {'value1': 1, 'value2': 2}}
            utils.dump_recon_cache(submit_dict, testcache_file, logger)
            fd = open(testcache_file)
            file_dict = json.loads(fd.readline())
            fd.close()
            self.assertEquals(submit_dict, file_dict)
            # Use a nested entry
            submit_dict = {'key1': {'key2': {'value1': 1, 'value2': 2}}}
            result_dict = {'key1': {'key2': {'value1': 1, 'value2': 2},
                           'value1': 1, 'value2': 2}}
            utils.dump_recon_cache(submit_dict, testcache_file, logger)
            fd = open(testcache_file)
            file_dict = json.loads(fd.readline())
            fd.close()
            self.assertEquals(result_dict, file_dict)
        finally:
            rmtree(testdir_base)

    def test_get_logger(self):
        sio = StringIO()
        logger = logging.getLogger('server')
        logger.addHandler(logging.StreamHandler(sio))
        logger = utils.get_logger(None, 'server', log_route='server')
        logger.warn('test1')
        self.assertEquals(sio.getvalue(), 'test1\n')
        logger.debug('test2')
        self.assertEquals(sio.getvalue(), 'test1\n')
        logger = utils.get_logger({'log_level': 'DEBUG'}, 'server',
                                  log_route='server')
        logger.debug('test3')
        self.assertEquals(sio.getvalue(), 'test1\ntest3\n')
        # Doesn't really test that the log facility is truly being used all the
        # way to syslog; but exercises the code.
        logger = utils.get_logger({'log_facility': 'LOG_LOCAL3'}, 'server',
                                  log_route='server')
        logger.warn('test4')
        self.assertEquals(sio.getvalue(),
                          'test1\ntest3\ntest4\n')
        # make sure debug doesn't log by default
        logger.debug('test5')
        self.assertEquals(sio.getvalue(),
                          'test1\ntest3\ntest4\n')
        # make sure notice lvl logs by default
        logger.notice('test6')
        self.assertEquals(sio.getvalue(),
                          'test1\ntest3\ntest4\ntest6\n')

    def test_get_logger_sysloghandler_plumbing(self):
        orig_sysloghandler = utils.SysLogHandler
        syslog_handler_args = []

        def syslog_handler_catcher(*args, **kwargs):
            syslog_handler_args.append((args, kwargs))
            return orig_sysloghandler(*args, **kwargs)

        syslog_handler_catcher.LOG_LOCAL0 = orig_sysloghandler.LOG_LOCAL0
        syslog_handler_catcher.LOG_LOCAL3 = orig_sysloghandler.LOG_LOCAL3

        try:
            utils.SysLogHandler = syslog_handler_catcher
            utils.get_logger({
                'log_facility': 'LOG_LOCAL3',
            }, 'server', log_route='server')
            expected_args = [((), {'address': '/dev/log',
                                   'facility': orig_sysloghandler.LOG_LOCAL3})]
            if not os.path.exists('/dev/log') or \
                    os.path.isfile('/dev/log') or \
                    os.path.isdir('/dev/log'):
                # Since socket on OSX is in /var/run/syslog, there will be
                # a fallback to UDP.
                expected_args.append(
                    ((), {'facility': orig_sysloghandler.LOG_LOCAL3}))
            self.assertEquals(expected_args, syslog_handler_args)

            syslog_handler_args = []
            utils.get_logger({
                'log_facility': 'LOG_LOCAL3',
                'log_address': '/foo/bar',
            }, 'server', log_route='server')
            self.assertEquals([
                ((), {'address': '/foo/bar',
                      'facility': orig_sysloghandler.LOG_LOCAL3}),
                # Second call is because /foo/bar didn't exist (and wasn't a
                # UNIX domain socket).
                ((), {'facility': orig_sysloghandler.LOG_LOCAL3})],
                syslog_handler_args)

            # Using UDP with default port
            syslog_handler_args = []
            utils.get_logger({
                'log_udp_host': 'syslog.funtimes.com',
            }, 'server', log_route='server')
            self.assertEquals([
                ((), {'address': ('syslog.funtimes.com',
                                  logging.handlers.SYSLOG_UDP_PORT),
                      'facility': orig_sysloghandler.LOG_LOCAL0})],
                syslog_handler_args)

            # Using UDP with non-default port
            syslog_handler_args = []
            utils.get_logger({
                'log_udp_host': 'syslog.funtimes.com',
                'log_udp_port': '2123',
            }, 'server', log_route='server')
            self.assertEquals([
                ((), {'address': ('syslog.funtimes.com', 2123),
                      'facility': orig_sysloghandler.LOG_LOCAL0})],
                syslog_handler_args)
        finally:
            utils.SysLogHandler = orig_sysloghandler

    @reset_logger_state
    def test_clean_logger_exception(self):
        # setup stream logging
        sio = StringIO()
        logger = utils.get_logger(None)
        handler = logging.StreamHandler(sio)
        logger.logger.addHandler(handler)

        def strip_value(sio):
            v = sio.getvalue()
            sio.truncate(0)
            return v

        def log_exception(exc):
            try:
                raise exc
            except (Exception, Timeout):
                logger.exception('blah')
        try:
            # establish base case
            self.assertEquals(strip_value(sio), '')
            logger.info('test')
            self.assertEquals(strip_value(sio), 'test\n')
            self.assertEquals(strip_value(sio), '')
            logger.info('test')
            logger.info('test')
            self.assertEquals(strip_value(sio), 'test\ntest\n')
            self.assertEquals(strip_value(sio), '')

            # test OSError
            for en in (errno.EIO, errno.ENOSPC):
                log_exception(OSError(en, 'my %s error message' % en))
                log_msg = strip_value(sio)
                self.assert_('Traceback' not in log_msg)
                self.assert_('my %s error message' % en in log_msg)
            # unfiltered
            log_exception(OSError())
            self.assert_('Traceback' in strip_value(sio))

            # test socket.error
            log_exception(socket.error(errno.ECONNREFUSED,
                                       'my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' not in log_msg)
            self.assert_('errno.ECONNREFUSED message test' not in log_msg)
            self.assert_('Connection refused' in log_msg)
            log_exception(socket.error(errno.EHOSTUNREACH,
                                       'my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' not in log_msg)
            self.assert_('my error message' not in log_msg)
            self.assert_('Host unreachable' in log_msg)
            log_exception(socket.error(errno.ETIMEDOUT, 'my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' not in log_msg)
            self.assert_('my error message' not in log_msg)
            self.assert_('Connection timeout' in log_msg)
            # unfiltered
            log_exception(socket.error(0, 'my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' in log_msg)
            self.assert_('my error message' in log_msg)

            # test eventlet.Timeout
            connection_timeout = ConnectionTimeout(42, 'my error message')
            log_exception(connection_timeout)
            log_msg = strip_value(sio)
            self.assert_('Traceback' not in log_msg)
            self.assert_('ConnectionTimeout' in log_msg)
            self.assert_('(42s)' in log_msg)
            self.assert_('my error message' not in log_msg)
            connection_timeout.cancel()

            message_timeout = MessageTimeout(42, 'my error message')
            log_exception(message_timeout)
            log_msg = strip_value(sio)
            self.assert_('Traceback' not in log_msg)
            self.assert_('MessageTimeout' in log_msg)
            self.assert_('(42s)' in log_msg)
            self.assert_('my error message' in log_msg)
            message_timeout.cancel()

            # test unhandled
            log_exception(Exception('my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' in log_msg)
            self.assert_('my error message' in log_msg)

        finally:
            logger.logger.removeHandler(handler)

    @reset_logger_state
    def test_swift_log_formatter_max_line_length(self):
        # setup stream logging
        sio = StringIO()
        logger = utils.get_logger(None)
        handler = logging.StreamHandler(sio)
        formatter = utils.SwiftLogFormatter(max_line_length=10)
        handler.setFormatter(formatter)
        logger.logger.addHandler(handler)

        def strip_value(sio):
            v = sio.getvalue()
            sio.truncate(0)
            return v

        try:
            logger.info('12345')
            self.assertEqual(strip_value(sio), '12345\n')
            logger.info('1234567890')
            self.assertEqual(strip_value(sio), '1234567890\n')
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '12 ... de\n')
            formatter.max_line_length = 11
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '123 ... cde\n')
            formatter.max_line_length = 0
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '1234567890abcde\n')
            formatter.max_line_length = 1
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '1\n')
            formatter.max_line_length = 2
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '12\n')
            formatter.max_line_length = 3
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '123\n')
            formatter.max_line_length = 4
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '1234\n')
            formatter.max_line_length = 5
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '12345\n')
            formatter.max_line_length = 6
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '123456\n')
            formatter.max_line_length = 7
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '1 ... e\n')
            formatter.max_line_length = -10
            logger.info('1234567890abcde')
            self.assertEqual(strip_value(sio), '1234567890abcde\n')
        finally:
            logger.logger.removeHandler(handler)

    @reset_logger_state
    def test_swift_log_formatter(self):
        # setup stream logging
        sio = StringIO()
        logger = utils.get_logger(None)
        handler = logging.StreamHandler(sio)
        handler.setFormatter(utils.SwiftLogFormatter())
        logger.logger.addHandler(handler)

        def strip_value(sio):
            v = sio.getvalue()
            sio.truncate(0)
            return v

        try:
            self.assertFalse(logger.txn_id)
            logger.error('my error message')
            log_msg = strip_value(sio)
            self.assert_('my error message' in log_msg)
            self.assert_('txn' not in log_msg)
            logger.txn_id = '12345'
            logger.error('test')
            log_msg = strip_value(sio)
            self.assert_('txn' in log_msg)
            self.assert_('12345' in log_msg)
            # test no txn on info message
            self.assertEquals(logger.txn_id, '12345')
            logger.info('test')
            log_msg = strip_value(sio)
            self.assert_('txn' not in log_msg)
            self.assert_('12345' not in log_msg)
            # test txn already in message
            self.assertEquals(logger.txn_id, '12345')
            logger.warn('test 12345 test')
            self.assertEquals(strip_value(sio), 'test 12345 test\n')
            # Test multi line collapsing
            logger.error('my\nerror\nmessage')
            log_msg = strip_value(sio)
            self.assert_('my#012error#012message' in log_msg)

            # test client_ip
            self.assertFalse(logger.client_ip)
            logger.error('my error message')
            log_msg = strip_value(sio)
            self.assert_('my error message' in log_msg)
            self.assert_('client_ip' not in log_msg)
            logger.client_ip = '1.2.3.4'
            logger.error('test')
            log_msg = strip_value(sio)
            self.assert_('client_ip' in log_msg)
            self.assert_('1.2.3.4' in log_msg)
            # test no client_ip on info message
            self.assertEquals(logger.client_ip, '1.2.3.4')
            logger.info('test')
            log_msg = strip_value(sio)
            self.assert_('client_ip' not in log_msg)
            self.assert_('1.2.3.4' not in log_msg)
            # test client_ip (and txn) already in message
            self.assertEquals(logger.client_ip, '1.2.3.4')
            logger.warn('test 1.2.3.4 test 12345')
            self.assertEquals(strip_value(sio), 'test 1.2.3.4 test 12345\n')
        finally:
            logger.logger.removeHandler(handler)

    def test_storage_directory(self):
        self.assertEquals(utils.storage_directory('objects', '1', 'ABCDEF'),
                          'objects/1/DEF/ABCDEF')

    def test_expand_ipv6(self):
        expanded_ipv6 = "fe80::204:61ff:fe9d:f156"
        upper_ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertEqual(expanded_ipv6, utils.expand_ipv6(upper_ipv6))
        omit_ipv6 = "fe80:0000:0000::0204:61ff:fe9d:f156"
        self.assertEqual(expanded_ipv6, utils.expand_ipv6(omit_ipv6))
        less_num_ipv6 = "fe80:0:00:000:0204:61ff:fe9d:f156"
        self.assertEqual(expanded_ipv6, utils.expand_ipv6(less_num_ipv6))

    def test_whataremyips(self):
        myips = utils.whataremyips()
        self.assert_(len(myips) > 1)
        self.assert_('127.0.0.1' in myips)

    def test_whataremyips_error(self):
        def my_interfaces():
            return ['eth0']

        def my_ifaddress_error(interface):
            raise ValueError

        with nested(
                patch('netifaces.interfaces', my_interfaces),
                patch('netifaces.ifaddresses', my_ifaddress_error)):
            self.assertEquals(utils.whataremyips(), [])

    def test_whataremyips_ipv6(self):
        test_ipv6_address = '2001:6b0:dead:beef:2::32'
        test_interface = 'eth0'

        def my_ipv6_interfaces():
            return ['eth0']

        def my_ipv6_ifaddresses(interface):
            return {AF_INET6:
                    [{'netmask': 'ffff:ffff:ffff:ffff::',
                      'addr': '%s%%%s' % (test_ipv6_address, test_interface)}]}
        with nested(
                patch('netifaces.interfaces', my_ipv6_interfaces),
                patch('netifaces.ifaddresses', my_ipv6_ifaddresses)):
            myips = utils.whataremyips()
            self.assertEquals(len(myips), 1)
            self.assertEquals(myips[0], test_ipv6_address)

    def test_hash_path(self):
        _prefix = utils.HASH_PATH_PREFIX
        utils.HASH_PATH_PREFIX = ''
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results hash_path produces, they know it
        try:
            self.assertEquals(utils.hash_path('a'),
                              '1c84525acb02107ea475dcd3d09c2c58')
            self.assertEquals(utils.hash_path('a', 'c'),
                              '33379ecb053aa5c9e356c68997cbb59e')
            self.assertEquals(utils.hash_path('a', 'c', 'o'),
                              '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEquals(utils.hash_path('a', 'c', 'o', raw_digest=False),
                              '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEquals(utils.hash_path('a', 'c', 'o', raw_digest=True),
                              '\x06\xfb\xf0\xb5\x14\xe5\x19\x9d\xfcN'
                              '\x00\xf4.\xb5\xea\x83')
            self.assertRaises(ValueError, utils.hash_path, 'a', object='o')
            utils.HASH_PATH_PREFIX = 'abcdef'
            self.assertEquals(utils.hash_path('a', 'c', 'o', raw_digest=False),
                              '363f9b535bfb7d17a43a46a358afca0e')
        finally:
            utils.HASH_PATH_PREFIX = _prefix

    def test_load_libc_function(self):
        self.assert_(callable(
            utils.load_libc_function('printf')))
        self.assert_(callable(
            utils.load_libc_function('some_not_real_function')))
        self.assertRaises(AttributeError,
                          utils.load_libc_function, 'some_not_real_function',
                          fail_if_missing=True)

    def test_readconf(self):
        conf = '''[section1]
foo = bar

[section2]
log_name = yarr'''
        # setup a real file
        fd, temppath = tempfile.mkstemp(dir='/tmp')
        with os.fdopen(fd, 'wb') as f:
            f.write(conf)
        make_filename = lambda: temppath
        # setup a file stream
        make_fp = lambda: StringIO(conf)
        for conf_object_maker in (make_filename, make_fp):
            conffile = conf_object_maker()
            result = utils.readconf(conffile)
            expected = {'__file__': conffile,
                        'log_name': None,
                        'section1': {'foo': 'bar'},
                        'section2': {'log_name': 'yarr'}}
            self.assertEquals(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile, 'section1')
            expected = {'__file__': conffile, 'log_name': 'section1',
                        'foo': 'bar'}
            self.assertEquals(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile,
                                    'section2').get('log_name')
            expected = 'yarr'
            self.assertEquals(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile, 'section1',
                                    log_name='foo').get('log_name')
            expected = 'foo'
            self.assertEquals(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile, 'section1',
                                    defaults={'bar': 'baz'})
            expected = {'__file__': conffile, 'log_name': 'section1',
                        'foo': 'bar', 'bar': 'baz'}
            self.assertEquals(result, expected)
        self.assertRaises(SystemExit, utils.readconf, temppath, 'section3')
        os.unlink(temppath)
        self.assertRaises(SystemExit, utils.readconf, temppath)

    def test_readconf_raw(self):
        conf = '''[section1]
foo = bar

[section2]
log_name = %(yarr)s'''
        # setup a real file
        fd, temppath = tempfile.mkstemp(dir='/tmp')
        with os.fdopen(fd, 'wb') as f:
            f.write(conf)
        make_filename = lambda: temppath
        # setup a file stream
        make_fp = lambda: StringIO(conf)
        for conf_object_maker in (make_filename, make_fp):
            conffile = conf_object_maker()
            result = utils.readconf(conffile, raw=True)
            expected = {'__file__': conffile,
                        'log_name': None,
                        'section1': {'foo': 'bar'},
                        'section2': {'log_name': '%(yarr)s'}}
            self.assertEquals(result, expected)
        os.unlink(temppath)
        self.assertRaises(SystemExit, utils.readconf, temppath)

    def test_readconf_dir(self):
        config_dir = {
            'server.conf.d/01.conf': """
            [DEFAULT]
            port = 8080
            foo = bar

            [section1]
            name=section1
            """,
            'server.conf.d/section2.conf': """
            [DEFAULT]
            port = 8081
            bar = baz

            [section2]
            name=section2
            """,
            'other-server.conf.d/01.conf': """
            [DEFAULT]
            port = 8082

            [section3]
            name=section3
            """
        }
        # strip indent from test config contents
        config_dir = dict((f, dedent(c)) for (f, c) in config_dir.items())
        with temptree(*zip(*config_dir.items())) as path:
            conf_dir = os.path.join(path, 'server.conf.d')
            conf = utils.readconf(conf_dir)
        expected = {
            '__file__': os.path.join(path, 'server.conf.d'),
            'log_name': None,
            'section1': {
                'port': '8081',
                'foo': 'bar',
                'bar': 'baz',
                'name': 'section1',
            },
            'section2': {
                'port': '8081',
                'foo': 'bar',
                'bar': 'baz',
                'name': 'section2',
            },
        }
        self.assertEquals(conf, expected)

    def test_readconf_dir_ignores_hidden_and_nondotconf_files(self):
        config_dir = {
            'server.conf.d/01.conf': """
            [section1]
            port = 8080
            """,
            'server.conf.d/.01.conf.swp': """
            [section]
            port = 8081
            """,
            'server.conf.d/01.conf-bak': """
            [section]
            port = 8082
            """,
        }
        # strip indent from test config contents
        config_dir = dict((f, dedent(c)) for (f, c) in config_dir.items())
        with temptree(*zip(*config_dir.items())) as path:
            conf_dir = os.path.join(path, 'server.conf.d')
            conf = utils.readconf(conf_dir)
        expected = {
            '__file__': os.path.join(path, 'server.conf.d'),
            'log_name': None,
            'section1': {
                'port': '8080',
            },
        }
        self.assertEquals(conf, expected)

    def test_drop_privileges(self):
        user = getuser()
        # over-ride os with mock
        required_func_calls = ('setgroups', 'setgid', 'setuid', 'setsid',
                               'chdir', 'umask')
        utils.os = MockOs(called_funcs=required_func_calls)
        # exercise the code
        utils.drop_privileges(user)
        for func in required_func_calls:
            self.assert_(utils.os.called_funcs[func])
        import pwd
        self.assertEquals(pwd.getpwnam(user)[5], utils.os.environ['HOME'])

        groups = [g.gr_gid for g in grp.getgrall() if user in g.gr_mem]
        groups.append(pwd.getpwnam(user).pw_gid)
        self.assertEquals(set(groups), set(os.getgroups()))

        # reset; test same args, OSError trying to get session leader
        utils.os = MockOs(called_funcs=required_func_calls,
                          raise_funcs=('setsid',))
        for func in required_func_calls:
            self.assertFalse(utils.os.called_funcs.get(func, False))
        utils.drop_privileges(user)
        for func in required_func_calls:
            self.assert_(utils.os.called_funcs[func])

    @reset_logger_state
    def test_capture_stdio(self):
        # stubs
        logger = utils.get_logger(None, 'dummy')

        # mock utils system modules
        _orig_sys = utils.sys
        _orig_os = utils.os
        try:
            utils.sys = MockSys()
            utils.os = MockOs()

            # basic test
            utils.capture_stdio(logger)
            self.assert_(utils.sys.excepthook is not None)
            self.assertEquals(utils.os.closed_fds, utils.sys.stdio_fds)
            self.assert_(isinstance(utils.sys.stdout, utils.LoggerFileObject))
            self.assert_(isinstance(utils.sys.stderr, utils.LoggerFileObject))

            # reset; test same args, but exc when trying to close stdio
            utils.os = MockOs(raise_funcs=('dup2',))
            utils.sys = MockSys()

            # test unable to close stdio
            utils.capture_stdio(logger)
            self.assert_(utils.sys.excepthook is not None)
            self.assertEquals(utils.os.closed_fds, [])
            self.assert_(isinstance(utils.sys.stdout, utils.LoggerFileObject))
            self.assert_(isinstance(utils.sys.stderr, utils.LoggerFileObject))

            # reset; test some other args
            utils.os = MockOs()
            utils.sys = MockSys()
            logger = utils.get_logger(None, log_to_console=True)

            # test console log
            utils.capture_stdio(logger, capture_stdout=False,
                                capture_stderr=False)
            self.assert_(utils.sys.excepthook is not None)
            # when logging to console, stderr remains open
            self.assertEquals(utils.os.closed_fds, utils.sys.stdio_fds[:2])
            reset_loggers()

            # stdio not captured
            self.assertFalse(isinstance(utils.sys.stdout,
                                        utils.LoggerFileObject))
            self.assertFalse(isinstance(utils.sys.stderr,
                                        utils.LoggerFileObject))
        finally:
            utils.sys = _orig_sys
            utils.os = _orig_os

    @reset_logger_state
    def test_get_logger_console(self):
        logger = utils.get_logger(None)
        console_handlers = [h for h in logger.logger.handlers if
                            isinstance(h, logging.StreamHandler)]
        self.assertFalse(console_handlers)
        logger = utils.get_logger(None, log_to_console=True)
        console_handlers = [h for h in logger.logger.handlers if
                            isinstance(h, logging.StreamHandler)]
        self.assert_(console_handlers)
        # make sure you can't have two console handlers
        self.assertEquals(len(console_handlers), 1)
        old_handler = console_handlers[0]
        logger = utils.get_logger(None, log_to_console=True)
        console_handlers = [h for h in logger.logger.handlers if
                            isinstance(h, logging.StreamHandler)]
        self.assertEquals(len(console_handlers), 1)
        new_handler = console_handlers[0]
        self.assertNotEquals(new_handler, old_handler)

    def verify_under_pseudo_time(
            self, func, target_runtime_ms=1, *args, **kwargs):
        curr_time = [42.0]

        def my_time():
            curr_time[0] += 0.001
            return curr_time[0]

        def my_sleep(duration):
            curr_time[0] += 0.001
            curr_time[0] += duration

        with nested(
                patch('time.time', my_time),
                patch('time.sleep', my_sleep),
                patch('eventlet.sleep', my_sleep)):
            start = time.time()
            func(*args, **kwargs)
            # make sure it's accurate to 10th of a second, converting the time
            # difference to milliseconds, 100 milliseconds is 1/10 of a second
            diff_from_target_ms = abs(
                target_runtime_ms - ((time.time() - start) * 1000))
            self.assertTrue(diff_from_target_ms < 100,
                            "Expected %d < 100" % diff_from_target_ms)

    def test_ratelimit_sleep(self):

        def testfunc():
            running_time = 0
            for i in range(100):
                running_time = utils.ratelimit_sleep(running_time, -5)

        self.verify_under_pseudo_time(testfunc, target_runtime_ms=1)

        def testfunc():
            running_time = 0
            for i in range(100):
                running_time = utils.ratelimit_sleep(running_time, 0)

        self.verify_under_pseudo_time(testfunc, target_runtime_ms=1)

        def testfunc():
            running_time = 0
            for i in range(50):
                running_time = utils.ratelimit_sleep(running_time, 200)

        self.verify_under_pseudo_time(testfunc, target_runtime_ms=250)

    def test_ratelimit_sleep_with_incr(self):

        def testfunc():
            running_time = 0
            vals = [5, 17, 0, 3, 11, 30,
                    40, 4, 13, 2, -1] * 2  # adds up to 248
            total = 0
            for i in vals:
                running_time = utils.ratelimit_sleep(running_time,
                                                     500, incr_by=i)
                total += i
            self.assertEquals(248, total)

        self.verify_under_pseudo_time(testfunc, target_runtime_ms=500)

    def test_ratelimit_sleep_with_sleep(self):

        def testfunc():
            running_time = 0
            sleeps = [0] * 7 + [.2] * 3 + [0] * 30
            for i in sleeps:
                running_time = utils.ratelimit_sleep(running_time, 40,
                                                     rate_buffer=1)
                time.sleep(i)

        self.verify_under_pseudo_time(testfunc, target_runtime_ms=900)

    def test_urlparse(self):
        parsed = utils.urlparse('http://127.0.0.1/')
        self.assertEquals(parsed.scheme, 'http')
        self.assertEquals(parsed.hostname, '127.0.0.1')
        self.assertEquals(parsed.path, '/')

        parsed = utils.urlparse('http://127.0.0.1:8080/')
        self.assertEquals(parsed.port, 8080)

        parsed = utils.urlparse('https://127.0.0.1/')
        self.assertEquals(parsed.scheme, 'https')

        parsed = utils.urlparse('http://[::1]/')
        self.assertEquals(parsed.hostname, '::1')

        parsed = utils.urlparse('http://[::1]:8080/')
        self.assertEquals(parsed.hostname, '::1')
        self.assertEquals(parsed.port, 8080)

        parsed = utils.urlparse('www.example.com')
        self.assertEquals(parsed.hostname, '')

    def test_search_tree(self):
        # file match & ext miss
        with temptree(['asdf.conf', 'blarg.conf', 'asdf.cfg']) as t:
            asdf = utils.search_tree(t, 'a*', '.conf')
            self.assertEquals(len(asdf), 1)
            self.assertEquals(asdf[0],
                              os.path.join(t, 'asdf.conf'))

        # multi-file match & glob miss & sort
        with temptree(['application.bin', 'apple.bin', 'apropos.bin']) as t:
            app_bins = utils.search_tree(t, 'app*', 'bin')
            self.assertEquals(len(app_bins), 2)
            self.assertEquals(app_bins[0],
                              os.path.join(t, 'apple.bin'))
            self.assertEquals(app_bins[1],
                              os.path.join(t, 'application.bin'))

        # test file in folder & ext miss & glob miss
        files = (
            'sub/file1.ini',
            'sub/file2.conf',
            'sub.bin',
            'bus.ini',
            'bus/file3.ini',
        )
        with temptree(files) as t:
            sub_ini = utils.search_tree(t, 'sub*', '.ini')
            self.assertEquals(len(sub_ini), 1)
            self.assertEquals(sub_ini[0],
                              os.path.join(t, 'sub/file1.ini'))

        # test multi-file in folder & sub-folder & ext miss & glob miss
        files = (
            'folder_file.txt',
            'folder/1.txt',
            'folder/sub/2.txt',
            'folder2/3.txt',
            'Folder3/4.txt'
            'folder.rc',
        )
        with temptree(files) as t:
            folder_texts = utils.search_tree(t, 'folder*', '.txt')
            self.assertEquals(len(folder_texts), 4)
            f1 = os.path.join(t, 'folder_file.txt')
            f2 = os.path.join(t, 'folder/1.txt')
            f3 = os.path.join(t, 'folder/sub/2.txt')
            f4 = os.path.join(t, 'folder2/3.txt')
            for f in [f1, f2, f3, f4]:
                self.assert_(f in folder_texts)

    def test_search_tree_with_directory_ext_match(self):
        files = (
            'object-server/object-server.conf-base',
            'object-server/1.conf.d/base.conf',
            'object-server/1.conf.d/1.conf',
            'object-server/2.conf.d/base.conf',
            'object-server/2.conf.d/2.conf',
            'object-server/3.conf.d/base.conf',
            'object-server/3.conf.d/3.conf',
            'object-server/4.conf.d/base.conf',
            'object-server/4.conf.d/4.conf',
        )
        with temptree(files) as t:
            conf_dirs = utils.search_tree(t, 'object-server', '.conf',
                                          dir_ext='conf.d')
        self.assertEquals(len(conf_dirs), 4)
        for i in range(4):
            conf_dir = os.path.join(t, 'object-server/%d.conf.d' % (i + 1))
            self.assert_(conf_dir in conf_dirs)

    def test_search_tree_conf_dir_with_named_conf_match(self):
        files = (
            'proxy-server/proxy-server.conf.d/base.conf',
            'proxy-server/proxy-server.conf.d/pipeline.conf',
            'proxy-server/proxy-noauth.conf.d/base.conf',
            'proxy-server/proxy-noauth.conf.d/pipeline.conf',
        )
        with temptree(files) as t:
            conf_dirs = utils.search_tree(t, 'proxy-server', 'noauth.conf',
                                          dir_ext='noauth.conf.d')
        self.assertEquals(len(conf_dirs), 1)
        conf_dir = conf_dirs[0]
        expected = os.path.join(t, 'proxy-server/proxy-noauth.conf.d')
        self.assertEqual(conf_dir, expected)

    def test_search_tree_conf_dir_pid_with_named_conf_match(self):
        files = (
            'proxy-server/proxy-server.pid.d',
            'proxy-server/proxy-noauth.pid.d',
        )
        with temptree(files) as t:
            pid_files = utils.search_tree(t, 'proxy-server',
                                          exts=['noauth.pid', 'noauth.pid.d'])
        self.assertEquals(len(pid_files), 1)
        pid_file = pid_files[0]
        expected = os.path.join(t, 'proxy-server/proxy-noauth.pid.d')
        self.assertEqual(pid_file, expected)

    def test_write_file(self):
        with temptree([]) as t:
            file_name = os.path.join(t, 'test')
            utils.write_file(file_name, 'test')
            with open(file_name, 'r') as f:
                contents = f.read()
            self.assertEquals(contents, 'test')
            # and also subdirs
            file_name = os.path.join(t, 'subdir/test2')
            utils.write_file(file_name, 'test2')
            with open(file_name, 'r') as f:
                contents = f.read()
            self.assertEquals(contents, 'test2')
            # but can't over-write files
            file_name = os.path.join(t, 'subdir/test2/test3')
            self.assertRaises(IOError, utils.write_file, file_name,
                              'test3')

    def test_remove_file(self):
        with temptree([]) as t:
            file_name = os.path.join(t, 'blah.pid')
            # assert no raise
            self.assertEquals(os.path.exists(file_name), False)
            self.assertEquals(utils.remove_file(file_name), None)
            with open(file_name, 'w') as f:
                f.write('1')
            self.assert_(os.path.exists(file_name))
            self.assertEquals(utils.remove_file(file_name), None)
            self.assertFalse(os.path.exists(file_name))

    def test_human_readable(self):
        self.assertEquals(utils.human_readable(0), '0')
        self.assertEquals(utils.human_readable(1), '1')
        self.assertEquals(utils.human_readable(10), '10')
        self.assertEquals(utils.human_readable(100), '100')
        self.assertEquals(utils.human_readable(999), '999')
        self.assertEquals(utils.human_readable(1024), '1Ki')
        self.assertEquals(utils.human_readable(1535), '1Ki')
        self.assertEquals(utils.human_readable(1536), '2Ki')
        self.assertEquals(utils.human_readable(1047552), '1023Ki')
        self.assertEquals(utils.human_readable(1048063), '1023Ki')
        self.assertEquals(utils.human_readable(1048064), '1Mi')
        self.assertEquals(utils.human_readable(1048576), '1Mi')
        self.assertEquals(utils.human_readable(1073741824), '1Gi')
        self.assertEquals(utils.human_readable(1099511627776), '1Ti')
        self.assertEquals(utils.human_readable(1125899906842624), '1Pi')
        self.assertEquals(utils.human_readable(1152921504606846976), '1Ei')
        self.assertEquals(utils.human_readable(1180591620717411303424), '1Zi')
        self.assertEquals(utils.human_readable(1208925819614629174706176),
                          '1Yi')
        self.assertEquals(utils.human_readable(1237940039285380274899124224),
                          '1024Yi')

    def test_validate_sync_to(self):
        fname = 'container-sync-realms.conf'
        fcontents = '''
[US]
key = 9ff3b71c849749dbaec4ccdd3cbab62b
cluster_dfw1 = http://dfw1.host/v1/
'''
        with temptree([fname], [fcontents]) as tempdir:
            logger = FakeLogger()
            fpath = os.path.join(tempdir, fname)
            csr = ContainerSyncRealms(fpath, logger)
            for realms_conf in (None, csr):
                for goodurl, result in (
                        ('http://1.1.1.1/v1/a/c',
                         (None, 'http://1.1.1.1/v1/a/c', None, None)),
                        ('http://1.1.1.1:8080/a/c',
                         (None, 'http://1.1.1.1:8080/a/c', None, None)),
                        ('http://2.2.2.2/a/c',
                         (None, 'http://2.2.2.2/a/c', None, None)),
                        ('https://1.1.1.1/v1/a/c',
                         (None, 'https://1.1.1.1/v1/a/c', None, None)),
                        ('//US/DFW1/a/c',
                         (None, 'http://dfw1.host/v1/a/c', 'US',
                          '9ff3b71c849749dbaec4ccdd3cbab62b')),
                        ('//us/DFW1/a/c',
                         (None, 'http://dfw1.host/v1/a/c', 'US',
                          '9ff3b71c849749dbaec4ccdd3cbab62b')),
                        ('//us/dfw1/a/c',
                         (None, 'http://dfw1.host/v1/a/c', 'US',
                          '9ff3b71c849749dbaec4ccdd3cbab62b')),
                        ('//',
                         (None, None, None, None)),
                        ('',
                         (None, None, None, None))):
                    if goodurl.startswith('//') and not realms_conf:
                        self.assertEquals(
                            utils.validate_sync_to(
                                goodurl, ['1.1.1.1', '2.2.2.2'], realms_conf),
                            (None, None, None, None))
                    else:
                        self.assertEquals(
                            utils.validate_sync_to(
                                goodurl, ['1.1.1.1', '2.2.2.2'], realms_conf),
                            result)
                for badurl, result in (
                        ('http://1.1.1.1',
                         ('Path required in X-Container-Sync-To', None, None,
                          None)),
                        ('httpq://1.1.1.1/v1/a/c',
                         ('Invalid scheme \'httpq\' in X-Container-Sync-To, '
                          'must be "//", "http", or "https".', None, None,
                          None)),
                        ('http://1.1.1.1/v1/a/c?query',
                         ('Params, queries, and fragments not allowed in '
                          'X-Container-Sync-To', None, None, None)),
                        ('http://1.1.1.1/v1/a/c#frag',
                         ('Params, queries, and fragments not allowed in '
                          'X-Container-Sync-To', None, None, None)),
                        ('http://1.1.1.1/v1/a/c?query#frag',
                         ('Params, queries, and fragments not allowed in '
                          'X-Container-Sync-To', None, None, None)),
                        ('http://1.1.1.1/v1/a/c?query=param',
                         ('Params, queries, and fragments not allowed in '
                          'X-Container-Sync-To', None, None, None)),
                        ('http://1.1.1.1/v1/a/c?query=param#frag',
                         ('Params, queries, and fragments not allowed in '
                          'X-Container-Sync-To', None, None, None)),
                        ('http://1.1.1.2/v1/a/c',
                         ("Invalid host '1.1.1.2' in X-Container-Sync-To",
                          None, None, None)),
                        ('//us/invalid/a/c',
                         ("No cluster endpoint for 'us' 'invalid'", None,
                          None, None)),
                        ('//invalid/dfw1/a/c',
                         ("No realm key for 'invalid'", None, None, None)),
                        ('//us/invalid1/a/',
                         ("Invalid X-Container-Sync-To format "
                          "'//us/invalid1/a/'", None, None, None)),
                        ('//us/invalid1/a',
                         ("Invalid X-Container-Sync-To format "
                          "'//us/invalid1/a'", None, None, None)),
                        ('//us/invalid1/',
                         ("Invalid X-Container-Sync-To format "
                          "'//us/invalid1/'", None, None, None)),
                        ('//us/invalid1',
                         ("Invalid X-Container-Sync-To format "
                          "'//us/invalid1'", None, None, None)),
                        ('//us/',
                         ("Invalid X-Container-Sync-To format "
                          "'//us/'", None, None, None)),
                        ('//us',
                         ("Invalid X-Container-Sync-To format "
                          "'//us'", None, None, None))):
                    if badurl.startswith('//') and not realms_conf:
                        self.assertEquals(
                            utils.validate_sync_to(
                                badurl, ['1.1.1.1', '2.2.2.2'], realms_conf),
                            (None, None, None, None))
                    else:
                        self.assertEquals(
                            utils.validate_sync_to(
                                badurl, ['1.1.1.1', '2.2.2.2'], realms_conf),
                            result)

    def test_TRUE_VALUES(self):
        for v in utils.TRUE_VALUES:
            self.assertEquals(v, v.lower())

    def test_config_true_value(self):
        orig_trues = utils.TRUE_VALUES
        try:
            utils.TRUE_VALUES = 'hello world'.split()
            for val in 'hello world HELLO WORLD'.split():
                self.assertTrue(utils.config_true_value(val) is True)
            self.assertTrue(utils.config_true_value(True) is True)
            self.assertTrue(utils.config_true_value('foo') is False)
            self.assertTrue(utils.config_true_value(False) is False)
        finally:
            utils.TRUE_VALUES = orig_trues

    def test_config_auto_int_value(self):
        expectations = {
            # (value, default) : expected,
            ('1', 0): 1,
            (1, 0): 1,
            ('asdf', 0): ValueError,
            ('auto', 1): 1,
            ('AutO', 1): 1,
            ('Aut0', 1): ValueError,
            (None, 1): 1,
        }
        for (value, default), expected in expectations.items():
            try:
                rv = utils.config_auto_int_value(value, default)
            except Exception as e:
                if e.__class__ is not expected:
                    raise
            else:
                self.assertEquals(expected, rv)

    def test_streq_const_time(self):
        self.assertTrue(utils.streq_const_time('abc123', 'abc123'))
        self.assertFalse(utils.streq_const_time('a', 'aaaaa'))
        self.assertFalse(utils.streq_const_time('ABC123', 'abc123'))

    def test_replication_quorum_size(self):
        expected_sizes = {1: 1,
                          2: 2,
                          3: 2,
                          4: 3,
                          5: 3}
        got_sizes = dict([(n, utils.quorum_size(n))
                          for n in expected_sizes])
        self.assertEqual(expected_sizes, got_sizes)

    def test_rsync_ip_ipv4_localhost(self):
        self.assertEqual(utils.rsync_ip('127.0.0.1'), '127.0.0.1')

    def test_rsync_ip_ipv6_random_ip(self):
        self.assertEqual(
            utils.rsync_ip('fe80:0000:0000:0000:0202:b3ff:fe1e:8329'),
            '[fe80:0000:0000:0000:0202:b3ff:fe1e:8329]')

    def test_rsync_ip_ipv6_ipv4_compatible(self):
        self.assertEqual(
            utils.rsync_ip('::ffff:192.0.2.128'), '[::ffff:192.0.2.128]')

    def test_fallocate_reserve(self):

        class StatVFS(object):
            f_frsize = 1024
            f_bavail = 1

        def fstatvfs(fd):
            return StatVFS()

        orig_FALLOCATE_RESERVE = utils.FALLOCATE_RESERVE
        orig_fstatvfs = utils.os.fstatvfs
        try:
            fallocate = utils.FallocateWrapper(noop=True)
            utils.os.fstatvfs = fstatvfs
            # Want 1023 reserved, have 1024 * 1 free, so succeeds
            utils.FALLOCATE_RESERVE = 1023
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            self.assertEquals(fallocate(0, 1, 0, ctypes.c_uint64(0)), 0)
            # Want 1023 reserved, have 512 * 2 free, so succeeds
            utils.FALLOCATE_RESERVE = 1023
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            self.assertEquals(fallocate(0, 1, 0, ctypes.c_uint64(0)), 0)
            # Want 1024 reserved, have 1024 * 1 free, so fails
            utils.FALLOCATE_RESERVE = 1024
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError as err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 1024')
            # Want 1024 reserved, have 512 * 2 free, so fails
            utils.FALLOCATE_RESERVE = 1024
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError as err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 1024')
            # Want 2048 reserved, have 1024 * 1 free, so fails
            utils.FALLOCATE_RESERVE = 2048
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError as err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 2048')
            # Want 2048 reserved, have 512 * 2 free, so fails
            utils.FALLOCATE_RESERVE = 2048
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError as err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 2048')
            # Want 1023 reserved, have 1024 * 1 free, but file size is 1, so
            # fails
            utils.FALLOCATE_RESERVE = 1023
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(1))
            except OSError as err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1023 <= 1023')
            # Want 1022 reserved, have 1024 * 1 free, and file size is 1, so
            # succeeds
            utils.FALLOCATE_RESERVE = 1022
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            self.assertEquals(fallocate(0, 1, 0, ctypes.c_uint64(1)), 0)
            # Want 1023 reserved, have 1024 * 1 free, and file size is 0, so
            # succeeds
            utils.FALLOCATE_RESERVE = 1023
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            self.assertEquals(fallocate(0, 1, 0, ctypes.c_uint64(0)), 0)
            # Want 1024 reserved, have 1024 * 1 free, and even though
            # file size is 0, since we're under the reserve, fails
            utils.FALLOCATE_RESERVE = 1024
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError as err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 1024')
        finally:
            utils.FALLOCATE_RESERVE = orig_FALLOCATE_RESERVE
            utils.os.fstatvfs = orig_fstatvfs

    def test_fallocate_func(self):

        class FallocateWrapper(object):

            def __init__(self):
                self.last_call = None

            def __call__(self, *args):
                self.last_call = list(args)
                self.last_call[-1] = self.last_call[-1].value
                return 0

        orig__sys_fallocate = utils._sys_fallocate
        try:
            utils._sys_fallocate = FallocateWrapper()
            # Ensure fallocate calls _sys_fallocate even with 0 bytes
            utils._sys_fallocate.last_call = None
            utils.fallocate(1234, 0)
            self.assertEquals(utils._sys_fallocate.last_call,
                              [1234, 1, 0, 0])
            # Ensure fallocate calls _sys_fallocate even with negative bytes
            utils._sys_fallocate.last_call = None
            utils.fallocate(1234, -5678)
            self.assertEquals(utils._sys_fallocate.last_call,
                              [1234, 1, 0, 0])
            # Ensure fallocate calls _sys_fallocate properly with positive
            # bytes
            utils._sys_fallocate.last_call = None
            utils.fallocate(1234, 1)
            self.assertEquals(utils._sys_fallocate.last_call,
                              [1234, 1, 0, 1])
            utils._sys_fallocate.last_call = None
            utils.fallocate(1234, 10 * 1024 * 1024 * 1024)
            self.assertEquals(utils._sys_fallocate.last_call,
                              [1234, 1, 0, 10 * 1024 * 1024 * 1024])
        finally:
            utils._sys_fallocate = orig__sys_fallocate

    def test_generate_trans_id(self):
        fake_time = 1366428370.5163341
        with patch.object(utils.time, 'time', return_value=fake_time):
            trans_id = utils.generate_trans_id('')
            self.assertEquals(len(trans_id), 34)
            self.assertEquals(trans_id[:2], 'tx')
            self.assertEquals(trans_id[23], '-')
            self.assertEquals(int(trans_id[24:], 16), int(fake_time))
        with patch.object(utils.time, 'time', return_value=fake_time):
            trans_id = utils.generate_trans_id('-suffix')
            self.assertEquals(len(trans_id), 41)
            self.assertEquals(trans_id[:2], 'tx')
            self.assertEquals(trans_id[34:], '-suffix')
            self.assertEquals(trans_id[23], '-')
            self.assertEquals(int(trans_id[24:34], 16), int(fake_time))

    def test_get_trans_id_time(self):
        ts = utils.get_trans_id_time('tx8c8bc884cdaf499bb29429aa9c46946e')
        self.assertEquals(ts, None)
        ts = utils.get_trans_id_time('tx1df4ff4f55ea45f7b2ec2-0051720c06')
        self.assertEquals(ts, 1366428678)
        self.assertEquals(
            time.asctime(time.gmtime(ts)) + ' UTC',
            'Sat Apr 20 03:31:18 2013 UTC')
        ts = utils.get_trans_id_time(
            'tx1df4ff4f55ea45f7b2ec2-0051720c06-suffix')
        self.assertEquals(ts, 1366428678)
        self.assertEquals(
            time.asctime(time.gmtime(ts)) + ' UTC',
            'Sat Apr 20 03:31:18 2013 UTC')
        ts = utils.get_trans_id_time('')
        self.assertEquals(ts, None)
        ts = utils.get_trans_id_time('garbage')
        self.assertEquals(ts, None)
        ts = utils.get_trans_id_time('tx1df4ff4f55ea45f7b2ec2-almostright')
        self.assertEquals(ts, None)

    def test_tpool_reraise(self):
        with patch.object(utils.tpool, 'execute', lambda f: f()):
            self.assertTrue(
                utils.tpool_reraise(MagicMock(return_value='test1')), 'test1')
            self.assertRaises(
                Exception,
                utils.tpool_reraise, MagicMock(side_effect=Exception('test2')))
            self.assertRaises(
                BaseException,
                utils.tpool_reraise,
                MagicMock(side_effect=BaseException('test3')))

    def test_lock_file(self):
        flags = os.O_CREAT | os.O_RDWR
        with NamedTemporaryFile(delete=False) as nt:
            nt.write("test string")
            nt.flush()
            nt.close()
            with utils.lock_file(nt.name, unlink=False) as f:
                self.assertEqual(f.read(), "test string")
                # we have a lock, now let's try to get a newer one
                fd = os.open(nt.name, flags)
                self.assertRaises(IOError, fcntl.flock, fd,
                                  fcntl.LOCK_EX | fcntl.LOCK_NB)

            with utils.lock_file(nt.name, unlink=False, append=True) as f:
                self.assertEqual(f.read(), "test string")
                f.seek(0)
                f.write("\nanother string")
                f.flush()
                f.seek(0)
                self.assertEqual(f.read(), "test string\nanother string")

                # we have a lock, now let's try to get a newer one
                fd = os.open(nt.name, flags)
                self.assertRaises(IOError, fcntl.flock, fd,
                                  fcntl.LOCK_EX | fcntl.LOCK_NB)

            with utils.lock_file(nt.name, timeout=3, unlink=False) as f:
                try:
                    with utils.lock_file(
                            nt.name, timeout=1, unlink=False) as f:
                        self.assertTrue(
                            False, "Expected LockTimeout exception")
                except LockTimeout:
                    pass

            with utils.lock_file(nt.name, unlink=True) as f:
                self.assertEqual(f.read(), "test string\nanother string")
                # we have a lock, now let's try to get a newer one
                fd = os.open(nt.name, flags)
                self.assertRaises(
                    IOError, fcntl.flock, fd, fcntl.LOCK_EX | fcntl.LOCK_NB)

            self.assertRaises(OSError, os.remove, nt.name)

    def test_lock_file_unlinked_after_open(self):
        os_open = os.open
        first_pass = [True]

        def deleting_open(filename, flags):
            # unlink the file after it's opened.  once.
            fd = os_open(filename, flags)
            if first_pass[0]:
                os.unlink(filename)
                first_pass[0] = False
            return fd

        with NamedTemporaryFile(delete=False) as nt:
            with mock.patch('os.open', deleting_open):
                with utils.lock_file(nt.name, unlink=True) as f:
                    self.assertNotEqual(os.fstat(nt.fileno()).st_ino,
                                        os.fstat(f.fileno()).st_ino)
        first_pass = [True]

        def recreating_open(filename, flags):
            # unlink and recreate the file after it's opened
            fd = os_open(filename, flags)
            if first_pass[0]:
                os.unlink(filename)
                os.close(os_open(filename, os.O_CREAT | os.O_RDWR))
                first_pass[0] = False
            return fd

        with NamedTemporaryFile(delete=False) as nt:
            with mock.patch('os.open', recreating_open):
                with utils.lock_file(nt.name, unlink=True) as f:
                    self.assertNotEqual(os.fstat(nt.fileno()).st_ino,
                                        os.fstat(f.fileno()).st_ino)

    def test_lock_file_held_on_unlink(self):
        os_unlink = os.unlink

        def flocking_unlink(filename):
            # make sure the lock is held when we unlink
            fd = os.open(filename, os.O_RDWR)
            self.assertRaises(
                IOError, fcntl.flock, fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.close(fd)
            os_unlink(filename)

        with NamedTemporaryFile(delete=False) as nt:
            with mock.patch('os.unlink', flocking_unlink):
                with utils.lock_file(nt.name, unlink=True):
                    pass

    def test_lock_file_no_unlink_if_fail(self):
        os_open = os.open
        with NamedTemporaryFile(delete=True) as nt:

            def lock_on_open(filename, flags):
                # lock the file on another fd after it's opened.
                fd = os_open(filename, flags)
                fd2 = os_open(filename, flags)
                fcntl.flock(fd2, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd

            try:
                timedout = False
                with mock.patch('os.open', lock_on_open):
                    with utils.lock_file(nt.name, unlink=False, timeout=0.01):
                        pass
            except LockTimeout:
                timedout = True
            self.assert_(timedout)
            self.assert_(os.path.exists(nt.name))

    def test_ismount_path_does_not_exist(self):
        tmpdir = mkdtemp()
        try:
            self.assertFalse(utils.ismount(os.path.join(tmpdir, 'bar')))
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_path_not_mount(self):
        tmpdir = mkdtemp()
        try:
            self.assertFalse(utils.ismount(tmpdir))
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_path_error(self):

        def _mock_os_lstat(path):
            raise OSError(13, "foo")

        tmpdir = mkdtemp()
        try:
            with patch("os.lstat", _mock_os_lstat):
                # Raises exception with _raw -- see next test.
                utils.ismount(tmpdir)
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_raw_path_error(self):

        def _mock_os_lstat(path):
            raise OSError(13, "foo")

        tmpdir = mkdtemp()
        try:
            with patch("os.lstat", _mock_os_lstat):
                self.assertRaises(OSError, utils.ismount_raw, tmpdir)
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_path_is_symlink(self):
        tmpdir = mkdtemp()
        try:
            link = os.path.join(tmpdir, "tmp")
            os.symlink("/tmp", link)
            self.assertFalse(utils.ismount(link))
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_path_is_root(self):
        self.assertTrue(utils.ismount('/'))

    def test_ismount_parent_path_error(self):

        _os_lstat = os.lstat

        def _mock_os_lstat(path):
            if path.endswith(".."):
                raise OSError(13, "foo")
            else:
                return _os_lstat(path)

        tmpdir = mkdtemp()
        try:
            with patch("os.lstat", _mock_os_lstat):
                # Raises exception with _raw -- see next test.
                utils.ismount(tmpdir)
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_raw_parent_path_error(self):

        _os_lstat = os.lstat

        def _mock_os_lstat(path):
            if path.endswith(".."):
                raise OSError(13, "foo")
            else:
                return _os_lstat(path)

        tmpdir = mkdtemp()
        try:
            with patch("os.lstat", _mock_os_lstat):
                self.assertRaises(OSError, utils.ismount_raw, tmpdir)
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_successes_dev(self):

        _os_lstat = os.lstat

        class MockStat(object):
            def __init__(self, mode, dev, ino):
                self.st_mode = mode
                self.st_dev = dev
                self.st_ino = ino

        def _mock_os_lstat(path):
            if path.endswith(".."):
                parent = _os_lstat(path)
                return MockStat(parent.st_mode, parent.st_dev + 1,
                                parent.st_ino)
            else:
                return _os_lstat(path)

        tmpdir = mkdtemp()
        try:
            with patch("os.lstat", _mock_os_lstat):
                self.assertTrue(utils.ismount(tmpdir))
        finally:
            shutil.rmtree(tmpdir)

    def test_ismount_successes_ino(self):

        _os_lstat = os.lstat

        class MockStat(object):
            def __init__(self, mode, dev, ino):
                self.st_mode = mode
                self.st_dev = dev
                self.st_ino = ino

        def _mock_os_lstat(path):
            if path.endswith(".."):
                return _os_lstat(path)
            else:
                parent_path = os.path.join(path, "..")
                child = _os_lstat(path)
                parent = _os_lstat(parent_path)
                return MockStat(child.st_mode, parent.st_ino,
                                child.st_dev)

        tmpdir = mkdtemp()
        try:
            with patch("os.lstat", _mock_os_lstat):
                self.assertTrue(utils.ismount(tmpdir))
        finally:
            shutil.rmtree(tmpdir)

    def test_parse_content_type(self):
        self.assertEquals(utils.parse_content_type('text/plain'),
                          ('text/plain', []))
        self.assertEquals(utils.parse_content_type('text/plain;charset=utf-8'),
                          ('text/plain', [('charset', 'utf-8')]))
        self.assertEquals(
            utils.parse_content_type('text/plain;hello="world";charset=utf-8'),
            ('text/plain', [('hello', '"world"'), ('charset', 'utf-8')]))
        self.assertEquals(
            utils.parse_content_type('text/plain; hello="world"; a=b'),
            ('text/plain', [('hello', '"world"'), ('a', 'b')]))
        self.assertEquals(
            utils.parse_content_type(r'text/plain; x="\""; a=b'),
            ('text/plain', [('x', r'"\""'), ('a', 'b')]))
        self.assertEquals(
            utils.parse_content_type(r'text/plain; x; a=b'),
            ('text/plain', [('x', ''), ('a', 'b')]))
        self.assertEquals(
            utils.parse_content_type(r'text/plain; x="\""; a'),
            ('text/plain', [('x', r'"\""'), ('a', '')]))

    def test_override_bytes_from_content_type(self):
        listing_dict = {
            'bytes': 1234, 'hash': 'asdf', 'name': 'zxcv',
            'content_type': 'text/plain; hello="world"; swift_bytes=15'}
        utils.override_bytes_from_content_type(listing_dict,
                                               logger=FakeLogger())
        self.assertEquals(listing_dict['bytes'], 15)
        self.assertEquals(listing_dict['content_type'],
                          'text/plain;hello="world"')

        listing_dict = {
            'bytes': 1234, 'hash': 'asdf', 'name': 'zxcv',
            'content_type': 'text/plain; hello="world"; swift_bytes=hey'}
        utils.override_bytes_from_content_type(listing_dict,
                                               logger=FakeLogger())
        self.assertEquals(listing_dict['bytes'], 1234)
        self.assertEquals(listing_dict['content_type'],
                          'text/plain;hello="world"')

    def test_clean_content_type(self):
        subtests = {
            '': '', 'text/plain': 'text/plain',
            'text/plain; someother=thing': 'text/plain; someother=thing',
            'text/plain; swift_bytes=123': 'text/plain',
            'text/plain; someother=thing; swift_bytes=123':
                'text/plain; someother=thing',
            # Since Swift always tacks on the swift_bytes, clean_content_type()
            # only strips swift_bytes if it's last. The next item simply shows
            # that if for some other odd reason it's not last,
            # clean_content_type() will not remove it from the header.
            'text/plain; swift_bytes=123; someother=thing':
                'text/plain; swift_bytes=123; someother=thing'}
        for before, after in subtests.items():
            self.assertEqual(utils.clean_content_type(before), after)

    def test_quote(self):
        res = utils.quote('/v1/a/c3/subdirx/')
        assert res == '/v1/a/c3/subdirx/'
        res = utils.quote('/v1/a&b/c3/subdirx/')
        assert res == '/v1/a%26b/c3/subdirx/'
        res = utils.quote('/v1/a&b/c3/subdirx/', safe='&')
        assert res == '%2Fv1%2Fa&b%2Fc3%2Fsubdirx%2F'
        unicode_sample = u'\uc77c\uc601'
        account = 'abc_' + unicode_sample
        valid_utf8_str = utils.get_valid_utf8_str(account)
        account = 'abc_' + unicode_sample.encode('utf-8')[::-1]
        invalid_utf8_str = utils.get_valid_utf8_str(account)
        self.assertEquals('abc_%EC%9D%BC%EC%98%81',
                          utils.quote(valid_utf8_str))
        self.assertEquals('abc_%EF%BF%BD%EF%BF%BD%EC%BC%9D%EF%BF%BD',
                          utils.quote(invalid_utf8_str))

    def test_get_hmac(self):
        self.assertEquals(
            utils.get_hmac('GET', '/path', 1, 'abc'),
            'b17f6ff8da0e251737aa9e3ee69a881e3e092e2f')

    def test_get_policy_index(self):
        # Account has no information about a policy
        req = Request.blank(
            '/sda1/p/a',
            environ={'REQUEST_METHOD': 'GET'})
        res = Response()
        self.assertEquals(None, utils.get_policy_index(req.headers,
                                                       res.headers))

        # The policy of a container can be specified by the response header
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'})
        res = Response(headers={'X-Backend-Storage-Policy-Index': '1'})
        self.assertEquals('1', utils.get_policy_index(req.headers,
                                                      res.headers))

        # The policy of an object to be created can be specified by the request
        # header
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Backend-Storage-Policy-Index': '2'})
        res = Response()
        self.assertEquals('2', utils.get_policy_index(req.headers,
                                                      res.headers))

    def test_get_log_line(self):
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'HEAD', 'REMOTE_ADDR': '1.2.3.4'})
        res = Response()
        trans_time = 1.2
        additional_info = 'some information'
        server_pid = 1234
        exp_line = '1.2.3.4 - - [01/Jan/1970:02:46:41 +0000] "HEAD ' \
            '/sda1/p/a/c/o" 200 - "-" "-" "-" 1.2000 "some information" 1234 -'
        with mock.patch(
                'time.gmtime',
                mock.MagicMock(side_effect=[time.gmtime(10001.0)])):
            with mock.patch(
                    'os.getpid', mock.MagicMock(return_value=server_pid)):
                self.assertEquals(
                    exp_line,
                    utils.get_log_line(req, res, trans_time, additional_info))

    def test_cache_from_env(self):
        # should never get logging when swift.cache is found
        env = {'swift.cache': 42}
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(42, utils.cache_from_env(env))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(42, utils.cache_from_env(env, False))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(42, utils.cache_from_env(env, True))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))

        # check allow_none controls logging when swift.cache is not found
        err_msg = 'ERROR: swift.cache could not be found in env!'
        env = {}
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(None, utils.cache_from_env(env))
            self.assertTrue(err_msg in logger.get_lines_for_level('error'))
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(None, utils.cache_from_env(env, False))
            self.assertTrue(err_msg in logger.get_lines_for_level('error'))
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(None, utils.cache_from_env(env, True))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))

    def test_fsync_dir(self):

        tempdir = None
        fd = None
        try:
            tempdir = mkdtemp(dir='/tmp')
            fd, temppath = tempfile.mkstemp(dir=tempdir)

            _mock_fsync = mock.Mock()
            _mock_close = mock.Mock()

            with patch('swift.common.utils.fsync', _mock_fsync):
                with patch('os.close', _mock_close):
                    utils.fsync_dir(tempdir)
            self.assertTrue(_mock_fsync.called)
            self.assertTrue(_mock_close.called)
            self.assertTrue(isinstance(_mock_fsync.call_args[0][0], int))
            self.assertEqual(_mock_fsync.call_args[0][0],
                             _mock_close.call_args[0][0])

            # Not a directory - arg is file path
            self.assertRaises(OSError, utils.fsync_dir, temppath)

            logger = FakeLogger()

            def _mock_fsync(fd):
                raise OSError(errno.EBADF, os.strerror(errno.EBADF))

            with patch('swift.common.utils.fsync', _mock_fsync):
                with mock.patch('swift.common.utils.logging', logger):
                    utils.fsync_dir(tempdir)
            self.assertEqual(1, len(logger.get_lines_for_level('warning')))

        finally:
            if fd is not None:
                os.close(fd)
                os.unlink(temppath)
            if tempdir:
                os.rmdir(tempdir)

    def test_renamer_with_fsync_dir(self):
        tempdir = None
        try:
            tempdir = mkdtemp(dir='/tmp')
            # Simulate part of object path already existing
            part_dir = os.path.join(tempdir, 'objects/1234/')
            os.makedirs(part_dir)
            obj_dir = os.path.join(part_dir, 'aaa', 'a' * 32)
            obj_path = os.path.join(obj_dir, '1425276031.12345.data')

            # Object dir had to be created
            _m_os_rename = mock.Mock()
            _m_fsync_dir = mock.Mock()
            with patch('os.rename', _m_os_rename):
                with patch('swift.common.utils.fsync_dir', _m_fsync_dir):
                    utils.renamer("fake_path", obj_path)
            _m_os_rename.assert_called_once_with('fake_path', obj_path)
            # fsync_dir on parents of all newly create dirs
            self.assertEqual(_m_fsync_dir.call_count, 3)

            # Object dir existed
            _m_os_rename.reset_mock()
            _m_fsync_dir.reset_mock()
            with patch('os.rename', _m_os_rename):
                with patch('swift.common.utils.fsync_dir', _m_fsync_dir):
                    utils.renamer("fake_path", obj_path)
            _m_os_rename.assert_called_once_with('fake_path', obj_path)
            # fsync_dir only on the leaf dir
            self.assertEqual(_m_fsync_dir.call_count, 1)
        finally:
            if tempdir:
                shutil.rmtree(tempdir)

    def test_renamer_when_fsync_is_false(self):
        _m_os_rename = mock.Mock()
        _m_fsync_dir = mock.Mock()
        _m_makedirs_count = mock.Mock(return_value=2)
        with patch('os.rename', _m_os_rename):
            with patch('swift.common.utils.fsync_dir', _m_fsync_dir):
                with patch('swift.common.utils.makedirs_count',
                           _m_makedirs_count):
                    utils.renamer("fake_path", "/a/b/c.data", fsync=False)
        _m_makedirs_count.assert_called_once_with("/a/b")
        _m_os_rename.assert_called_once_with('fake_path', "/a/b/c.data")
        self.assertFalse(_m_fsync_dir.called)

    def test_makedirs_count(self):
        tempdir = None
        fd = None
        try:
            tempdir = mkdtemp(dir='/tmp')
            os.makedirs(os.path.join(tempdir, 'a/b'))
            # 4 new dirs created
            dirpath = os.path.join(tempdir, 'a/b/1/2/3/4')
            ret = utils.makedirs_count(dirpath)
            self.assertEqual(ret, 4)
            # no new dirs created - dir already exists
            ret = utils.makedirs_count(dirpath)
            self.assertEqual(ret, 0)
            # path exists and is a file
            fd, temppath = tempfile.mkstemp(dir=dirpath)
            os.close(fd)
            self.assertRaises(OSError, utils.makedirs_count, temppath)
        finally:
            if tempdir:
                shutil.rmtree(tempdir)


class ResellerConfReader(unittest.TestCase):

    def setUp(self):
        self.default_rules = {'operator_roles': ['admin', 'swiftoperator'],
                              'service_roles': [],
                              'require_group': ''}

    def test_defaults(self):
        conf = {}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_'])
        self.assertEqual(options['AUTH_'], self.default_rules)

    def test_same_as_default(self):
        conf = {'reseller_prefix': 'AUTH',
                'operator_roles': 'admin, swiftoperator'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_'])
        self.assertEqual(options['AUTH_'], self.default_rules)

    def test_single_blank_reseller(self):
        conf = {'reseller_prefix': ''}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])
        self.assertEqual(options[''], self.default_rules)

    def test_single_blank_reseller_with_conf(self):
        conf = {'reseller_prefix': '',
                "''operator_roles": 'role1, role2'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])
        self.assertEqual(options[''].get('operator_roles'),
                         ['role1', 'role2'])
        self.assertEqual(options[''].get('service_roles'),
                         self.default_rules.get('service_roles'))
        self.assertEqual(options[''].get('require_group'),
                         self.default_rules.get('require_group'))

    def test_multiple_same_resellers(self):
        conf = {'reseller_prefix': " '' , '' "}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])

        conf = {'reseller_prefix': '_, _'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['_'])

        conf = {'reseller_prefix': 'AUTH, PRE2, AUTH, PRE2'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', 'PRE2_'])

    def test_several_resellers_with_conf(self):
        conf = {'reseller_prefix': 'PRE1, PRE2',
                'PRE1_operator_roles': 'role1, role2',
                'PRE1_service_roles': 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['PRE1_', 'PRE2_'])

        self.assertEquals(set(['role1', 'role2']),
                          set(options['PRE1_'].get('operator_roles')))
        self.assertEquals(['role5'],
                          options['PRE2_'].get('operator_roles'))
        self.assertEquals(set(['role3', 'role4']),
                          set(options['PRE1_'].get('service_roles')))
        self.assertEquals(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEquals('', options['PRE1_'].get('require_group'))
        self.assertEquals('pre2_group', options['PRE2_'].get('require_group'))

    def test_several_resellers_first_blank(self):
        conf = {'reseller_prefix': " '' , PRE2",
                "''operator_roles": 'role1, role2',
                "''service_roles": 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['', 'PRE2_'])

        self.assertEquals(set(['role1', 'role2']),
                          set(options[''].get('operator_roles')))
        self.assertEquals(['role5'],
                          options['PRE2_'].get('operator_roles'))
        self.assertEquals(set(['role3', 'role4']),
                          set(options[''].get('service_roles')))
        self.assertEquals(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEquals('', options[''].get('require_group'))
        self.assertEquals('pre2_group', options['PRE2_'].get('require_group'))

    def test_several_resellers_with_blank_comma(self):
        conf = {'reseller_prefix': "AUTH , '', PRE2",
                "''operator_roles": 'role1, role2',
                "''service_roles": 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', '', 'PRE2_'])
        self.assertEquals(set(['admin', 'swiftoperator']),
                          set(options['AUTH_'].get('operator_roles')))
        self.assertEquals(set(['role1', 'role2']),
                          set(options[''].get('operator_roles')))
        self.assertEquals(['role5'],
                          options['PRE2_'].get('operator_roles'))
        self.assertEquals([],
                          options['AUTH_'].get('service_roles'))
        self.assertEquals(set(['role3', 'role4']),
                          set(options[''].get('service_roles')))
        self.assertEquals(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEquals('', options['AUTH_'].get('require_group'))
        self.assertEquals('', options[''].get('require_group'))
        self.assertEquals('pre2_group', options['PRE2_'].get('require_group'))

    def test_stray_comma(self):
        conf = {'reseller_prefix': "AUTH ,, PRE2",
                "''operator_roles": 'role1, role2',
                "''service_roles": 'role3, role4',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', 'PRE2_'])
        self.assertEquals(set(['admin', 'swiftoperator']),
                          set(options['AUTH_'].get('operator_roles')))
        self.assertEquals(['role5'],
                          options['PRE2_'].get('operator_roles'))
        self.assertEquals([],
                          options['AUTH_'].get('service_roles'))
        self.assertEquals(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEquals('', options['AUTH_'].get('require_group'))
        self.assertEquals('pre2_group', options['PRE2_'].get('require_group'))

    def test_multiple_stray_commas_resellers(self):
        conf = {'reseller_prefix': ' , , ,'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, [''])
        self.assertEqual(options[''], self.default_rules)

    def test_unprefixed_options(self):
        conf = {'reseller_prefix': "AUTH , '', PRE2",
                "operator_roles": 'role1, role2',
                "service_roles": 'role3, role4',
                'require_group': 'auth_blank_group',
                'PRE2_operator_roles': 'role5',
                'PRE2_service_roles': 'role6',
                'PRE2_require_group': 'pre2_group'}
        prefixes, options = utils.config_read_reseller_options(
            conf, self.default_rules)
        self.assertEqual(prefixes, ['AUTH_', '', 'PRE2_'])
        self.assertEquals(set(['role1', 'role2']),
                          set(options['AUTH_'].get('operator_roles')))
        self.assertEquals(set(['role1', 'role2']),
                          set(options[''].get('operator_roles')))
        self.assertEquals(['role5'],
                          options['PRE2_'].get('operator_roles'))
        self.assertEquals(set(['role3', 'role4']),
                          set(options['AUTH_'].get('service_roles')))
        self.assertEquals(set(['role3', 'role4']),
                          set(options[''].get('service_roles')))
        self.assertEquals(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEquals('auth_blank_group',
                          options['AUTH_'].get('require_group'))
        self.assertEquals('auth_blank_group', options[''].get('require_group'))
        self.assertEquals('pre2_group', options['PRE2_'].get('require_group'))


class TestSwiftInfo(unittest.TestCase):

    def tearDown(self):
        utils._swift_info = {}
        utils._swift_admin_info = {}

    def test_register_swift_info(self):
        utils.register_swift_info(foo='bar')
        utils.register_swift_info(lorem='ipsum')
        utils.register_swift_info('cap1', cap1_foo='cap1_bar')
        utils.register_swift_info('cap1', cap1_lorem='cap1_ipsum')

        self.assertTrue('swift' in utils._swift_info)
        self.assertTrue('foo' in utils._swift_info['swift'])
        self.assertEqual(utils._swift_info['swift']['foo'], 'bar')
        self.assertTrue('lorem' in utils._swift_info['swift'])
        self.assertEqual(utils._swift_info['swift']['lorem'], 'ipsum')

        self.assertTrue('cap1' in utils._swift_info)
        self.assertTrue('cap1_foo' in utils._swift_info['cap1'])
        self.assertEqual(utils._swift_info['cap1']['cap1_foo'], 'cap1_bar')
        self.assertTrue('cap1_lorem' in utils._swift_info['cap1'])
        self.assertEqual(utils._swift_info['cap1']['cap1_lorem'], 'cap1_ipsum')

        self.assertRaises(ValueError,
                          utils.register_swift_info, 'admin', foo='bar')

        self.assertRaises(ValueError,
                          utils.register_swift_info, 'disallowed_sections',
                          disallowed_sections=None)

        utils.register_swift_info('goodkey', foo='5.6')
        self.assertRaises(ValueError,
                          utils.register_swift_info, 'bad.key', foo='5.6')
        data = {'bad.key': '5.6'}
        self.assertRaises(ValueError,
                          utils.register_swift_info, 'goodkey', **data)

    def test_get_swift_info(self):
        utils._swift_info = {'swift': {'foo': 'bar'},
                             'cap1': {'cap1_foo': 'cap1_bar'}}
        utils._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = utils.get_swift_info()

        self.assertTrue('admin' not in info)

        self.assertTrue('swift' in info)
        self.assertTrue('foo' in info['swift'])
        self.assertEqual(utils._swift_info['swift']['foo'], 'bar')

        self.assertTrue('cap1' in info)
        self.assertTrue('cap1_foo' in info['cap1'])
        self.assertEqual(utils._swift_info['cap1']['cap1_foo'], 'cap1_bar')

    def test_get_swift_info_with_disallowed_sections(self):
        utils._swift_info = {'swift': {'foo': 'bar'},
                             'cap1': {'cap1_foo': 'cap1_bar'},
                             'cap2': {'cap2_foo': 'cap2_bar'},
                             'cap3': {'cap3_foo': 'cap3_bar'}}
        utils._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = utils.get_swift_info(disallowed_sections=['cap1', 'cap3'])

        self.assertTrue('admin' not in info)

        self.assertTrue('swift' in info)
        self.assertTrue('foo' in info['swift'])
        self.assertEqual(info['swift']['foo'], 'bar')

        self.assertTrue('cap1' not in info)

        self.assertTrue('cap2' in info)
        self.assertTrue('cap2_foo' in info['cap2'])
        self.assertEqual(info['cap2']['cap2_foo'], 'cap2_bar')

        self.assertTrue('cap3' not in info)

    def test_register_swift_admin_info(self):
        utils.register_swift_info(admin=True, admin_foo='admin_bar')
        utils.register_swift_info(admin=True, admin_lorem='admin_ipsum')
        utils.register_swift_info('cap1', admin=True, ac1_foo='ac1_bar')
        utils.register_swift_info('cap1', admin=True, ac1_lorem='ac1_ipsum')

        self.assertTrue('swift' in utils._swift_admin_info)
        self.assertTrue('admin_foo' in utils._swift_admin_info['swift'])
        self.assertEqual(
            utils._swift_admin_info['swift']['admin_foo'], 'admin_bar')
        self.assertTrue('admin_lorem' in utils._swift_admin_info['swift'])
        self.assertEqual(
            utils._swift_admin_info['swift']['admin_lorem'], 'admin_ipsum')

        self.assertTrue('cap1' in utils._swift_admin_info)
        self.assertTrue('ac1_foo' in utils._swift_admin_info['cap1'])
        self.assertEqual(
            utils._swift_admin_info['cap1']['ac1_foo'], 'ac1_bar')
        self.assertTrue('ac1_lorem' in utils._swift_admin_info['cap1'])
        self.assertEqual(
            utils._swift_admin_info['cap1']['ac1_lorem'], 'ac1_ipsum')

        self.assertTrue('swift' not in utils._swift_info)
        self.assertTrue('cap1' not in utils._swift_info)

    def test_get_swift_admin_info(self):
        utils._swift_info = {'swift': {'foo': 'bar'},
                             'cap1': {'cap1_foo': 'cap1_bar'}}
        utils._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = utils.get_swift_info(admin=True)

        self.assertTrue('admin' in info)
        self.assertTrue('admin_cap1' in info['admin'])
        self.assertTrue('ac1_foo' in info['admin']['admin_cap1'])
        self.assertEqual(info['admin']['admin_cap1']['ac1_foo'], 'ac1_bar')

        self.assertTrue('swift' in info)
        self.assertTrue('foo' in info['swift'])
        self.assertEqual(utils._swift_info['swift']['foo'], 'bar')

        self.assertTrue('cap1' in info)
        self.assertTrue('cap1_foo' in info['cap1'])
        self.assertEqual(utils._swift_info['cap1']['cap1_foo'], 'cap1_bar')

    def test_get_swift_admin_info_with_disallowed_sections(self):
        utils._swift_info = {'swift': {'foo': 'bar'},
                             'cap1': {'cap1_foo': 'cap1_bar'},
                             'cap2': {'cap2_foo': 'cap2_bar'},
                             'cap3': {'cap3_foo': 'cap3_bar'}}
        utils._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = utils.get_swift_info(
            admin=True, disallowed_sections=['cap1', 'cap3'])

        self.assertTrue('admin' in info)
        self.assertTrue('admin_cap1' in info['admin'])
        self.assertTrue('ac1_foo' in info['admin']['admin_cap1'])
        self.assertEqual(info['admin']['admin_cap1']['ac1_foo'], 'ac1_bar')
        self.assertTrue('disallowed_sections' in info['admin'])
        self.assertTrue('cap1' in info['admin']['disallowed_sections'])
        self.assertTrue('cap2' not in info['admin']['disallowed_sections'])
        self.assertTrue('cap3' in info['admin']['disallowed_sections'])

        self.assertTrue('swift' in info)
        self.assertTrue('foo' in info['swift'])
        self.assertEqual(info['swift']['foo'], 'bar')

        self.assertTrue('cap1' not in info)

        self.assertTrue('cap2' in info)
        self.assertTrue('cap2_foo' in info['cap2'])
        self.assertEqual(info['cap2']['cap2_foo'], 'cap2_bar')

        self.assertTrue('cap3' not in info)

    def test_get_swift_admin_info_with_disallowed_sub_sections(self):
        utils._swift_info = {'swift': {'foo': 'bar'},
                             'cap1': {'cap1_foo': 'cap1_bar',
                                      'cap1_moo': 'cap1_baa'},
                             'cap2': {'cap2_foo': 'cap2_bar'},
                             'cap3': {'cap2_foo': 'cap2_bar'},
                             'cap4': {'a': {'b': {'c': 'c'},
                                            'b.c': 'b.c'}}}
        utils._swift_admin_info = {'admin_cap1': {'ac1_foo': 'ac1_bar'}}

        info = utils.get_swift_info(
            admin=True, disallowed_sections=['cap1.cap1_foo', 'cap3',
                                             'cap4.a.b.c'])
        self.assertTrue('cap3' not in info)
        self.assertEquals(info['cap1']['cap1_moo'], 'cap1_baa')
        self.assertTrue('cap1_foo' not in info['cap1'])
        self.assertTrue('c' not in info['cap4']['a']['b'])
        self.assertEqual(info['cap4']['a']['b.c'], 'b.c')

    def test_get_swift_info_with_unmatched_disallowed_sections(self):
        cap1 = {'cap1_foo': 'cap1_bar',
                'cap1_moo': 'cap1_baa'}
        utils._swift_info = {'swift': {'foo': 'bar'},
                             'cap1': cap1}
        # expect no exceptions
        info = utils.get_swift_info(
            disallowed_sections=['cap2.cap1_foo', 'cap1.no_match',
                                 'cap1.cap1_foo.no_match.no_match'])
        self.assertEquals(info['cap1'], cap1)


class TestFileLikeIter(unittest.TestCase):

    def test_iter_file_iter(self):
        in_iter = ['abc', 'de', 'fghijk', 'l']
        chunks = []
        for chunk in utils.FileLikeIter(in_iter):
            chunks.append(chunk)
        self.assertEquals(chunks, in_iter)

    def test_next(self):
        in_iter = ['abc', 'de', 'fghijk', 'l']
        chunks = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            try:
                chunk = iter_file.next()
            except StopIteration:
                break
            chunks.append(chunk)
        self.assertEquals(chunks, in_iter)

    def test_read(self):
        in_iter = ['abc', 'de', 'fghijk', 'l']
        iter_file = utils.FileLikeIter(in_iter)
        self.assertEquals(iter_file.read(), ''.join(in_iter))

    def test_read_with_size(self):
        in_iter = ['abc', 'de', 'fghijk', 'l']
        chunks = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            chunk = iter_file.read(2)
            if not chunk:
                break
            self.assertTrue(len(chunk) <= 2)
            chunks.append(chunk)
        self.assertEquals(''.join(chunks), ''.join(in_iter))

    def test_read_with_size_zero(self):
        # makes little sense, but file supports it, so...
        self.assertEquals(utils.FileLikeIter('abc').read(0), '')

    def test_readline(self):
        in_iter = ['abc\n', 'd', '\nef', 'g\nh', '\nij\n\nk\n', 'trailing.']
        lines = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            line = iter_file.readline()
            if not line:
                break
            lines.append(line)
        self.assertEquals(
            lines,
            [v if v == 'trailing.' else v + '\n'
             for v in ''.join(in_iter).split('\n')])

    def test_readline2(self):
        self.assertEquals(
            utils.FileLikeIter(['abc', 'def\n']).readline(4),
            'abcd')

    def test_readline3(self):
        self.assertEquals(
            utils.FileLikeIter(['a' * 1111, 'bc\ndef']).readline(),
            ('a' * 1111) + 'bc\n')

    def test_readline_with_size(self):

        in_iter = ['abc\n', 'd', '\nef', 'g\nh', '\nij\n\nk\n', 'trailing.']
        lines = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            line = iter_file.readline(2)
            if not line:
                break
            lines.append(line)
        self.assertEquals(
            lines,
            ['ab', 'c\n', 'd\n', 'ef', 'g\n', 'h\n', 'ij', '\n', '\n', 'k\n',
             'tr', 'ai', 'li', 'ng', '.'])

    def test_readlines(self):
        in_iter = ['abc\n', 'd', '\nef', 'g\nh', '\nij\n\nk\n', 'trailing.']
        lines = utils.FileLikeIter(in_iter).readlines()
        self.assertEquals(
            lines,
            [v if v == 'trailing.' else v + '\n'
             for v in ''.join(in_iter).split('\n')])

    def test_readlines_with_size(self):
        in_iter = ['abc\n', 'd', '\nef', 'g\nh', '\nij\n\nk\n', 'trailing.']
        iter_file = utils.FileLikeIter(in_iter)
        lists_of_lines = []
        while True:
            lines = iter_file.readlines(2)
            if not lines:
                break
            lists_of_lines.append(lines)
        self.assertEquals(
            lists_of_lines,
            [['ab'], ['c\n'], ['d\n'], ['ef'], ['g\n'], ['h\n'], ['ij'],
             ['\n', '\n'], ['k\n'], ['tr'], ['ai'], ['li'], ['ng'], ['.']])

    def test_close(self):
        iter_file = utils.FileLikeIter('abcdef')
        self.assertEquals(iter_file.next(), 'a')
        iter_file.close()
        self.assertTrue(iter_file.closed)
        self.assertRaises(ValueError, iter_file.next)
        self.assertRaises(ValueError, iter_file.read)
        self.assertRaises(ValueError, iter_file.readline)
        self.assertRaises(ValueError, iter_file.readlines)
        # Just make sure repeated close calls don't raise an Exception
        iter_file.close()
        self.assertTrue(iter_file.closed)


class TestStatsdLogging(unittest.TestCase):
    def test_get_logger_statsd_client_not_specified(self):
        logger = utils.get_logger({}, 'some-name', log_route='some-route')
        # white-box construction validation
        self.assertEqual(None, logger.logger.statsd_client)

    def test_get_logger_statsd_client_defaults(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com'},
                                  'some-name', log_route='some-route')
        # white-box construction validation
        self.assert_(isinstance(logger.logger.statsd_client,
                                utils.StatsdClient))
        self.assertEqual(logger.logger.statsd_client._host, 'some.host.com')
        self.assertEqual(logger.logger.statsd_client._port, 8125)
        self.assertEqual(logger.logger.statsd_client._prefix, 'some-name.')
        self.assertEqual(logger.logger.statsd_client._default_sample_rate, 1)

        logger.set_statsd_prefix('some-name.more-specific')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'some-name.more-specific.')
        logger.set_statsd_prefix('')
        self.assertEqual(logger.logger.statsd_client._prefix, '')

    def test_get_logger_statsd_client_non_defaults(self):
        logger = utils.get_logger({
            'log_statsd_host': 'another.host.com',
            'log_statsd_port': '9876',
            'log_statsd_default_sample_rate': '0.75',
            'log_statsd_sample_rate_factor': '0.81',
            'log_statsd_metric_prefix': 'tomato.sauce',
        }, 'some-name', log_route='some-route')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'tomato.sauce.some-name.')
        logger.set_statsd_prefix('some-name.more-specific')
        self.assertEqual(logger.logger.statsd_client._prefix,
                         'tomato.sauce.some-name.more-specific.')
        logger.set_statsd_prefix('')
        self.assertEqual(logger.logger.statsd_client._prefix, 'tomato.sauce.')
        self.assertEqual(logger.logger.statsd_client._host, 'another.host.com')
        self.assertEqual(logger.logger.statsd_client._port, 9876)
        self.assertEqual(logger.logger.statsd_client._default_sample_rate,
                         0.75)
        self.assertEqual(logger.logger.statsd_client._sample_rate_factor,
                         0.81)

    def test_no_exception_when_cant_send_udp_packet(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com'})
        statsd_client = logger.logger.statsd_client
        fl = FakeLogger()
        statsd_client.logger = fl
        mock_socket = MockUdpSocket(sendto_errno=errno.EPERM)
        statsd_client._open_socket = lambda *_: mock_socket
        logger.increment('tunafish')
        expected = ["Error sending UDP message to ('some.host.com', 8125): "
                    "[Errno 1] test errno 1"]
        self.assertEqual(fl.get_lines_for_level('warning'), expected)

    def test_sample_rates(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com'})

        mock_socket = MockUdpSocket()
        # encapsulation? what's that?
        statsd_client = logger.logger.statsd_client
        self.assertTrue(statsd_client.random is random.random)

        statsd_client._open_socket = lambda *_: mock_socket
        statsd_client.random = lambda: 0.50001

        logger.increment('tribbles', sample_rate=0.5)
        self.assertEqual(len(mock_socket.sent), 0)

        statsd_client.random = lambda: 0.49999
        logger.increment('tribbles', sample_rate=0.5)
        self.assertEqual(len(mock_socket.sent), 1)

        payload = mock_socket.sent[0][0]
        self.assertTrue(payload.endswith("|@0.5"))

    def test_sample_rates_with_sample_rate_factor(self):
        logger = utils.get_logger({
            'log_statsd_host': 'some.host.com',
            'log_statsd_default_sample_rate': '0.82',
            'log_statsd_sample_rate_factor': '0.91',
        })
        effective_sample_rate = 0.82 * 0.91

        mock_socket = MockUdpSocket()
        # encapsulation? what's that?
        statsd_client = logger.logger.statsd_client
        self.assertTrue(statsd_client.random is random.random)

        statsd_client._open_socket = lambda *_: mock_socket
        statsd_client.random = lambda: effective_sample_rate + 0.001

        logger.increment('tribbles')
        self.assertEqual(len(mock_socket.sent), 0)

        statsd_client.random = lambda: effective_sample_rate - 0.001
        logger.increment('tribbles')
        self.assertEqual(len(mock_socket.sent), 1)

        payload = mock_socket.sent[0][0]
        self.assertTrue(payload.endswith("|@%s" % effective_sample_rate),
                        payload)

        effective_sample_rate = 0.587 * 0.91
        statsd_client.random = lambda: effective_sample_rate - 0.001
        logger.increment('tribbles', sample_rate=0.587)
        self.assertEqual(len(mock_socket.sent), 2)

        payload = mock_socket.sent[1][0]
        self.assertTrue(payload.endswith("|@%s" % effective_sample_rate),
                        payload)

    def test_timing_stats(self):
        class MockController(object):
            def __init__(self, status):
                self.status = status
                self.logger = self
                self.args = ()
                self.called = 'UNKNOWN'

            def timing_since(self, *args):
                self.called = 'timing'
                self.args = args

        @utils.timing_stats()
        def METHOD(controller):
            return Response(status=controller.status)

        mock_controller = MockController(200)
        METHOD(mock_controller)
        self.assertEquals(mock_controller.called, 'timing')
        self.assertEquals(len(mock_controller.args), 2)
        self.assertEquals(mock_controller.args[0], 'METHOD.timing')
        self.assert_(mock_controller.args[1] > 0)

        mock_controller = MockController(404)
        METHOD(mock_controller)
        self.assertEquals(len(mock_controller.args), 2)
        self.assertEquals(mock_controller.called, 'timing')
        self.assertEquals(mock_controller.args[0], 'METHOD.timing')
        self.assert_(mock_controller.args[1] > 0)

        mock_controller = MockController(412)
        METHOD(mock_controller)
        self.assertEquals(len(mock_controller.args), 2)
        self.assertEquals(mock_controller.called, 'timing')
        self.assertEquals(mock_controller.args[0], 'METHOD.timing')
        self.assert_(mock_controller.args[1] > 0)

        mock_controller = MockController(416)
        METHOD(mock_controller)
        self.assertEquals(len(mock_controller.args), 2)
        self.assertEquals(mock_controller.called, 'timing')
        self.assertEquals(mock_controller.args[0], 'METHOD.timing')
        self.assert_(mock_controller.args[1] > 0)

        mock_controller = MockController(401)
        METHOD(mock_controller)
        self.assertEquals(len(mock_controller.args), 2)
        self.assertEquals(mock_controller.called, 'timing')
        self.assertEquals(mock_controller.args[0], 'METHOD.errors.timing')
        self.assert_(mock_controller.args[1] > 0)


class UnsafeXrange(object):
    """
    Like xrange(limit), but with extra context switching to screw things up.
    """
    def __init__(self, upper_bound):
        self.current = 0
        self.concurrent_calls = 0
        self.upper_bound = upper_bound
        self.concurrent_call = False

    def __iter__(self):
        return self

    def next(self):
        if self.concurrent_calls > 0:
            self.concurrent_call = True

        self.concurrent_calls += 1
        try:
            if self.current >= self.upper_bound:
                raise StopIteration
            else:
                val = self.current
                self.current += 1
                eventlet.sleep()   # yield control
                return val
        finally:
            self.concurrent_calls -= 1


class TestAffinityKeyFunction(unittest.TestCase):
    def setUp(self):
        self.nodes = [dict(id=0, region=1, zone=1),
                      dict(id=1, region=1, zone=2),
                      dict(id=2, region=2, zone=1),
                      dict(id=3, region=2, zone=2),
                      dict(id=4, region=3, zone=1),
                      dict(id=5, region=3, zone=2),
                      dict(id=6, region=4, zone=0),
                      dict(id=7, region=4, zone=1)]

    def test_single_region(self):
        keyfn = utils.affinity_key_function("r3=1")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([4, 5, 0, 1, 2, 3, 6, 7], ids)

    def test_bogus_value(self):
        self.assertRaises(ValueError,
                          utils.affinity_key_function, "r3")
        self.assertRaises(ValueError,
                          utils.affinity_key_function, "r3=elephant")

    def test_empty_value(self):
        # Empty's okay, it just means no preference
        keyfn = utils.affinity_key_function("")
        self.assert_(callable(keyfn))
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([0, 1, 2, 3, 4, 5, 6, 7], ids)

    def test_all_whitespace_value(self):
        # Empty's okay, it just means no preference
        keyfn = utils.affinity_key_function("  \n")
        self.assert_(callable(keyfn))
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([0, 1, 2, 3, 4, 5, 6, 7], ids)

    def test_with_zone_zero(self):
        keyfn = utils.affinity_key_function("r4z0=1")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([6, 0, 1, 2, 3, 4, 5, 7], ids)

    def test_multiple(self):
        keyfn = utils.affinity_key_function("r1=100, r4=200, r3z1=1")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([4, 0, 1, 6, 7, 2, 3, 5], ids)

    def test_more_specific_after_less_specific(self):
        keyfn = utils.affinity_key_function("r2=100, r2z2=50")
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([3, 2, 0, 1, 4, 5, 6, 7], ids)


class TestAffinityLocalityPredicate(unittest.TestCase):
    def setUp(self):
        self.nodes = [dict(id=0, region=1, zone=1),
                      dict(id=1, region=1, zone=2),
                      dict(id=2, region=2, zone=1),
                      dict(id=3, region=2, zone=2),
                      dict(id=4, region=3, zone=1),
                      dict(id=5, region=3, zone=2),
                      dict(id=6, region=4, zone=0),
                      dict(id=7, region=4, zone=1)]

    def test_empty(self):
        pred = utils.affinity_locality_predicate('')
        self.assert_(pred is None)

    def test_region(self):
        pred = utils.affinity_locality_predicate('r1')
        self.assert_(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0, 1], ids)

    def test_zone(self):
        pred = utils.affinity_locality_predicate('r1z1')
        self.assert_(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0], ids)

    def test_multiple(self):
        pred = utils.affinity_locality_predicate('r1, r3, r4z0')
        self.assert_(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0, 1, 4, 5, 6], ids)

    def test_invalid(self):
        self.assertRaises(ValueError,
                          utils.affinity_locality_predicate, 'falafel')
        self.assertRaises(ValueError,
                          utils.affinity_locality_predicate, 'r8zQ')
        self.assertRaises(ValueError,
                          utils.affinity_locality_predicate, 'r2d2')
        self.assertRaises(ValueError,
                          utils.affinity_locality_predicate, 'r1z1=1')


class TestRateLimitedIterator(unittest.TestCase):

    def run_under_pseudo_time(
            self, func, *args, **kwargs):
        curr_time = [42.0]

        def my_time():
            curr_time[0] += 0.001
            return curr_time[0]

        def my_sleep(duration):
            curr_time[0] += 0.001
            curr_time[0] += duration

        with nested(
                patch('time.time', my_time),
                patch('eventlet.sleep', my_sleep)):
            return func(*args, **kwargs)

    def test_rate_limiting(self):

        def testfunc():
            limited_iterator = utils.RateLimitedIterator(xrange(9999), 100)
            got = []
            started_at = time.time()
            try:
                while time.time() - started_at < 0.1:
                    got.append(limited_iterator.next())
            except StopIteration:
                pass
            return got

        got = self.run_under_pseudo_time(testfunc)
        # it's 11, not 10, because ratelimiting doesn't apply to the very
        # first element.
        self.assertEquals(len(got), 11)

    def test_limit_after(self):

        def testfunc():
            limited_iterator = utils.RateLimitedIterator(
                xrange(9999), 100, limit_after=5)
            got = []
            started_at = time.time()
            try:
                while time.time() - started_at < 0.1:
                    got.append(limited_iterator.next())
            except StopIteration:
                pass
            return got

        got = self.run_under_pseudo_time(testfunc)
        # it's 16, not 15, because ratelimiting doesn't apply to the very
        # first element.
        self.assertEquals(len(got), 16)


class TestGreenthreadSafeIterator(unittest.TestCase):

    def increment(self, iterable):
        plus_ones = []
        for n in iterable:
            plus_ones.append(n + 1)
        return plus_ones

    def test_setup_works(self):
        # it should work without concurrent access
        self.assertEquals([0, 1, 2, 3], list(UnsafeXrange(4)))

        iterable = UnsafeXrange(10)
        pile = eventlet.GreenPile(2)
        for _ in xrange(2):
            pile.spawn(self.increment, iterable)

        sorted([resp for resp in pile])
        self.assertTrue(
            iterable.concurrent_call, 'test setup is insufficiently crazy')

    def test_access_is_serialized(self):
        pile = eventlet.GreenPile(2)
        unsafe_iterable = UnsafeXrange(10)
        iterable = utils.GreenthreadSafeIterator(unsafe_iterable)
        for _ in xrange(2):
            pile.spawn(self.increment, iterable)
        response = sorted(sum([resp for resp in pile], []))
        self.assertEquals(range(1, 11), response)
        self.assertTrue(
            not unsafe_iterable.concurrent_call, 'concurrent call occurred')


class TestStatsdLoggingDelegation(unittest.TestCase):

    def setUp(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        self.port = self.sock.getsockname()[1]
        self.queue = Queue()
        self.reader_thread = threading.Thread(target=self.statsd_reader)
        self.reader_thread.setDaemon(1)
        self.reader_thread.start()

    def tearDown(self):
        # The "no-op when disabled" test doesn't set up a real logger, so
        # create one here so we can tell the reader thread to stop.
        if not getattr(self, 'logger', None):
            self.logger = utils.get_logger({
                'log_statsd_host': 'localhost',
                'log_statsd_port': str(self.port),
            }, 'some-name')
        self.logger.increment('STOP')
        self.reader_thread.join(timeout=4)
        self.sock.close()
        del self.logger

    def statsd_reader(self):
        while True:
            try:
                payload = self.sock.recv(4096)
                if payload and 'STOP' in payload:
                    return 42
                self.queue.put(payload)
            except Exception as e:
                sys.stderr.write('statsd_reader thread: %r' % (e,))
                break

    def _send_and_get(self, sender_fn, *args, **kwargs):
        """
        Because the client library may not actually send a packet with
        sample_rate < 1, we keep trying until we get one through.
        """
        got = None
        while not got:
            sender_fn(*args, **kwargs)
            try:
                got = self.queue.get(timeout=0.5)
            except Empty:
                pass
        return got

    def assertStat(self, expected, sender_fn, *args, **kwargs):
        got = self._send_and_get(sender_fn, *args, **kwargs)
        return self.assertEqual(expected, got)

    def assertStatMatches(self, expected_regexp, sender_fn, *args, **kwargs):
        got = self._send_and_get(sender_fn, *args, **kwargs)
        return self.assert_(re.search(expected_regexp, got),
                            [got, expected_regexp])

    def test_methods_are_no_ops_when_not_enabled(self):
        logger = utils.get_logger({
            # No "log_statsd_host" means "disabled"
            'log_statsd_port': str(self.port),
        }, 'some-name')
        # Delegate methods are no-ops
        self.assertEqual(None, logger.update_stats('foo', 88))
        self.assertEqual(None, logger.update_stats('foo', 88, 0.57))
        self.assertEqual(None, logger.update_stats('foo', 88,
                                                   sample_rate=0.61))
        self.assertEqual(None, logger.increment('foo'))
        self.assertEqual(None, logger.increment('foo', 0.57))
        self.assertEqual(None, logger.increment('foo', sample_rate=0.61))
        self.assertEqual(None, logger.decrement('foo'))
        self.assertEqual(None, logger.decrement('foo', 0.57))
        self.assertEqual(None, logger.decrement('foo', sample_rate=0.61))
        self.assertEqual(None, logger.timing('foo', 88.048))
        self.assertEqual(None, logger.timing('foo', 88.57, 0.34))
        self.assertEqual(None, logger.timing('foo', 88.998, sample_rate=0.82))
        self.assertEqual(None, logger.timing_since('foo', 8938))
        self.assertEqual(None, logger.timing_since('foo', 8948, 0.57))
        self.assertEqual(None, logger.timing_since('foo', 849398,
                                                   sample_rate=0.61))
        # Now, the queue should be empty (no UDP packets sent)
        self.assertRaises(Empty, self.queue.get_nowait)

    def test_delegate_methods_with_no_default_sample_rate(self):
        self.logger = utils.get_logger({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
        }, 'some-name')
        self.assertStat('some-name.some.counter:1|c', self.logger.increment,
                        'some.counter')
        self.assertStat('some-name.some.counter:-1|c', self.logger.decrement,
                        'some.counter')
        self.assertStat('some-name.some.operation:4900.0|ms',
                        self.logger.timing, 'some.operation', 4.9 * 1000)
        self.assertStatMatches('some-name\.another\.operation:\d+\.\d+\|ms',
                               self.logger.timing_since, 'another.operation',
                               time.time())
        self.assertStat('some-name.another.counter:42|c',
                        self.logger.update_stats, 'another.counter', 42)

        # Each call can override the sample_rate (also, bonus prefix test)
        self.logger.set_statsd_prefix('pfx')
        self.assertStat('pfx.some.counter:1|c|@0.972', self.logger.increment,
                        'some.counter', sample_rate=0.972)
        self.assertStat('pfx.some.counter:-1|c|@0.972', self.logger.decrement,
                        'some.counter', sample_rate=0.972)
        self.assertStat('pfx.some.operation:4900.0|ms|@0.972',
                        self.logger.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.972)
        self.assertStatMatches('pfx\.another\.op:\d+\.\d+\|ms|@0.972',
                               self.logger.timing_since, 'another.op',
                               time.time(), sample_rate=0.972)
        self.assertStat('pfx.another.counter:3|c|@0.972',
                        self.logger.update_stats, 'another.counter', 3,
                        sample_rate=0.972)

        # Can override sample_rate with non-keyword arg
        self.logger.set_statsd_prefix('')
        self.assertStat('some.counter:1|c|@0.939', self.logger.increment,
                        'some.counter', 0.939)
        self.assertStat('some.counter:-1|c|@0.939', self.logger.decrement,
                        'some.counter', 0.939)
        self.assertStat('some.operation:4900.0|ms|@0.939',
                        self.logger.timing, 'some.operation',
                        4.9 * 1000, 0.939)
        self.assertStatMatches('another\.op:\d+\.\d+\|ms|@0.939',
                               self.logger.timing_since, 'another.op',
                               time.time(), 0.939)
        self.assertStat('another.counter:3|c|@0.939',
                        self.logger.update_stats, 'another.counter', 3, 0.939)

    def test_delegate_methods_with_default_sample_rate(self):
        self.logger = utils.get_logger({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_default_sample_rate': '0.93',
        }, 'pfx')
        self.assertStat('pfx.some.counter:1|c|@0.93', self.logger.increment,
                        'some.counter')
        self.assertStat('pfx.some.counter:-1|c|@0.93', self.logger.decrement,
                        'some.counter')
        self.assertStat('pfx.some.operation:4760.0|ms|@0.93',
                        self.logger.timing, 'some.operation', 4.76 * 1000)
        self.assertStatMatches('pfx\.another\.op:\d+\.\d+\|ms|@0.93',
                               self.logger.timing_since, 'another.op',
                               time.time())
        self.assertStat('pfx.another.counter:3|c|@0.93',
                        self.logger.update_stats, 'another.counter', 3)

        # Each call can override the sample_rate
        self.assertStat('pfx.some.counter:1|c|@0.9912', self.logger.increment,
                        'some.counter', sample_rate=0.9912)
        self.assertStat('pfx.some.counter:-1|c|@0.9912', self.logger.decrement,
                        'some.counter', sample_rate=0.9912)
        self.assertStat('pfx.some.operation:4900.0|ms|@0.9912',
                        self.logger.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.9912)
        self.assertStatMatches('pfx\.another\.op:\d+\.\d+\|ms|@0.9912',
                               self.logger.timing_since, 'another.op',
                               time.time(), sample_rate=0.9912)
        self.assertStat('pfx.another.counter:3|c|@0.9912',
                        self.logger.update_stats, 'another.counter', 3,
                        sample_rate=0.9912)

        # Can override sample_rate with non-keyword arg
        self.logger.set_statsd_prefix('')
        self.assertStat('some.counter:1|c|@0.987654', self.logger.increment,
                        'some.counter', 0.987654)
        self.assertStat('some.counter:-1|c|@0.987654', self.logger.decrement,
                        'some.counter', 0.987654)
        self.assertStat('some.operation:4900.0|ms|@0.987654',
                        self.logger.timing, 'some.operation',
                        4.9 * 1000, 0.987654)
        self.assertStatMatches('another\.op:\d+\.\d+\|ms|@0.987654',
                               self.logger.timing_since, 'another.op',
                               time.time(), 0.987654)
        self.assertStat('another.counter:3|c|@0.987654',
                        self.logger.update_stats, 'another.counter',
                        3, 0.987654)

    def test_delegate_methods_with_metric_prefix(self):
        self.logger = utils.get_logger({
            'log_statsd_host': 'localhost',
            'log_statsd_port': str(self.port),
            'log_statsd_metric_prefix': 'alpha.beta',
        }, 'pfx')
        self.assertStat('alpha.beta.pfx.some.counter:1|c',
                        self.logger.increment, 'some.counter')
        self.assertStat('alpha.beta.pfx.some.counter:-1|c',
                        self.logger.decrement, 'some.counter')
        self.assertStat('alpha.beta.pfx.some.operation:4760.0|ms',
                        self.logger.timing, 'some.operation', 4.76 * 1000)
        self.assertStatMatches(
            'alpha\.beta\.pfx\.another\.op:\d+\.\d+\|ms',
            self.logger.timing_since, 'another.op', time.time())
        self.assertStat('alpha.beta.pfx.another.counter:3|c',
                        self.logger.update_stats, 'another.counter', 3)

        self.logger.set_statsd_prefix('')
        self.assertStat('alpha.beta.some.counter:1|c|@0.9912',
                        self.logger.increment, 'some.counter',
                        sample_rate=0.9912)
        self.assertStat('alpha.beta.some.counter:-1|c|@0.9912',
                        self.logger.decrement, 'some.counter', 0.9912)
        self.assertStat('alpha.beta.some.operation:4900.0|ms|@0.9912',
                        self.logger.timing, 'some.operation', 4.9 * 1000,
                        sample_rate=0.9912)
        self.assertStatMatches('alpha\.beta\.another\.op:\d+\.\d+\|ms|@0.9912',
                               self.logger.timing_since, 'another.op',
                               time.time(), sample_rate=0.9912)
        self.assertStat('alpha.beta.another.counter:3|c|@0.9912',
                        self.logger.update_stats, 'another.counter', 3,
                        sample_rate=0.9912)

    def test_get_valid_utf8_str(self):
        unicode_sample = u'\uc77c\uc601'
        valid_utf8_str = unicode_sample.encode('utf-8')
        invalid_utf8_str = unicode_sample.encode('utf-8')[::-1]
        self.assertEquals(valid_utf8_str,
                          utils.get_valid_utf8_str(valid_utf8_str))
        self.assertEquals(valid_utf8_str,
                          utils.get_valid_utf8_str(unicode_sample))
        self.assertEquals('\xef\xbf\xbd\xef\xbf\xbd\xec\xbc\x9d\xef\xbf\xbd',
                          utils.get_valid_utf8_str(invalid_utf8_str))

    @reset_logger_state
    def test_thread_locals(self):
        logger = utils.get_logger(None)
        # test the setter
        logger.thread_locals = ('id', 'ip')
        self.assertEquals(logger.thread_locals, ('id', 'ip'))
        # reset
        logger.thread_locals = (None, None)
        self.assertEquals(logger.thread_locals, (None, None))
        logger.txn_id = '1234'
        logger.client_ip = '1.2.3.4'
        self.assertEquals(logger.thread_locals, ('1234', '1.2.3.4'))
        logger.txn_id = '5678'
        logger.client_ip = '5.6.7.8'
        self.assertEquals(logger.thread_locals, ('5678', '5.6.7.8'))

    def test_no_fdatasync(self):
        called = []

        class NoFdatasync(object):
            pass

        def fsync(fd):
            called.append(fd)

        with patch('swift.common.utils.os', NoFdatasync()):
            with patch('swift.common.utils.fsync', fsync):
                utils.fdatasync(12345)
                self.assertEquals(called, [12345])

    def test_yes_fdatasync(self):
        called = []

        class YesFdatasync(object):

            def fdatasync(self, fd):
                called.append(fd)

        with patch('swift.common.utils.os', YesFdatasync()):
            utils.fdatasync(12345)
            self.assertEquals(called, [12345])

    def test_fsync_bad_fullsync(self):

        class FCNTL(object):

            F_FULLSYNC = 123

            def fcntl(self, fd, op):
                raise IOError(18)

        with patch('swift.common.utils.fcntl', FCNTL()):
            self.assertRaises(OSError, lambda: utils.fsync(12345))

    def test_fsync_f_fullsync(self):
        called = []

        class FCNTL(object):

            F_FULLSYNC = 123

            def fcntl(self, fd, op):
                called[:] = [fd, op]
                return 0

        with patch('swift.common.utils.fcntl', FCNTL()):
            utils.fsync(12345)
            self.assertEquals(called, [12345, 123])

    def test_fsync_no_fullsync(self):
        called = []

        class FCNTL(object):
            pass

        def fsync(fd):
            called.append(fd)

        with patch('swift.common.utils.fcntl', FCNTL()):
            with patch('os.fsync', fsync):
                utils.fsync(12345)
                self.assertEquals(called, [12345])


class TestThreadPool(unittest.TestCase):

    def setUp(self):
        self.tp = None

    def tearDown(self):
        if self.tp:
            self.tp.terminate()

    def _pipe_count(self):
        # Counts the number of pipes that this process owns.
        fd_dir = "/proc/%d/fd" % os.getpid()

        def is_pipe(path):
            try:
                stat_result = os.stat(path)
                return stat.S_ISFIFO(stat_result.st_mode)
            except OSError:
                return False

        return len([fd for fd in os.listdir(fd_dir)
                    if is_pipe(os.path.join(fd_dir, fd))])

    def _thread_id(self):
        return threading.current_thread().ident

    def _capture_args(self, *args, **kwargs):
        return {'args': args, 'kwargs': kwargs}

    def _raise_valueerror(self):
        return int('fishcakes')

    def test_run_in_thread_with_threads(self):
        tp = self.tp = utils.ThreadPool(1)

        my_id = self._thread_id()
        other_id = tp.run_in_thread(self._thread_id)
        self.assertNotEquals(my_id, other_id)

        result = tp.run_in_thread(self._capture_args, 1, 2, bert='ernie')
        self.assertEquals(result, {'args': (1, 2),
                                   'kwargs': {'bert': 'ernie'}})

        caught = False
        try:
            tp.run_in_thread(self._raise_valueerror)
        except ValueError:
            caught = True
        self.assertTrue(caught)

    def test_force_run_in_thread_with_threads(self):
        # with nthreads > 0, force_run_in_thread looks just like run_in_thread
        tp = self.tp = utils.ThreadPool(1)

        my_id = self._thread_id()
        other_id = tp.force_run_in_thread(self._thread_id)
        self.assertNotEquals(my_id, other_id)

        result = tp.force_run_in_thread(self._capture_args, 1, 2, bert='ernie')
        self.assertEquals(result, {'args': (1, 2),
                                   'kwargs': {'bert': 'ernie'}})
        self.assertRaises(ValueError, tp.force_run_in_thread,
                          self._raise_valueerror)

    def test_run_in_thread_without_threads(self):
        # with zero threads, run_in_thread doesn't actually do so
        tp = utils.ThreadPool(0)

        my_id = self._thread_id()
        other_id = tp.run_in_thread(self._thread_id)
        self.assertEquals(my_id, other_id)

        result = tp.run_in_thread(self._capture_args, 1, 2, bert='ernie')
        self.assertEquals(result, {'args': (1, 2),
                                   'kwargs': {'bert': 'ernie'}})
        self.assertRaises(ValueError, tp.run_in_thread,
                          self._raise_valueerror)

    def test_force_run_in_thread_without_threads(self):
        # with zero threads, force_run_in_thread uses eventlet.tpool
        tp = utils.ThreadPool(0)

        my_id = self._thread_id()
        other_id = tp.force_run_in_thread(self._thread_id)
        self.assertNotEquals(my_id, other_id)

        result = tp.force_run_in_thread(self._capture_args, 1, 2, bert='ernie')
        self.assertEquals(result, {'args': (1, 2),
                                   'kwargs': {'bert': 'ernie'}})
        self.assertRaises(ValueError, tp.force_run_in_thread,
                          self._raise_valueerror)

    def test_preserving_stack_trace_from_thread(self):
        def gamma():
            return 1 / 0  # ZeroDivisionError

        def beta():
            return gamma()

        def alpha():
            return beta()

        tp = self.tp = utils.ThreadPool(1)
        try:
            tp.run_in_thread(alpha)
        except ZeroDivisionError:
            # NB: format is (filename, line number, function name, text)
            tb_func = [elem[2] for elem
                       in traceback.extract_tb(sys.exc_traceback)]
        else:
            self.fail("Expected ZeroDivisionError")

        self.assertEqual(tb_func[-1], "gamma")
        self.assertEqual(tb_func[-2], "beta")
        self.assertEqual(tb_func[-3], "alpha")
        # omit the middle; what's important is that the start and end are
        # included, not the exact names of helper methods
        self.assertEqual(tb_func[1], "run_in_thread")
        self.assertEqual(tb_func[0], "test_preserving_stack_trace_from_thread")

    def test_terminate(self):
        initial_thread_count = threading.activeCount()
        initial_pipe_count = self._pipe_count()

        tp = utils.ThreadPool(4)
        # do some work to ensure any lazy initialization happens
        tp.run_in_thread(os.path.join, 'foo', 'bar')
        tp.run_in_thread(os.path.join, 'baz', 'quux')

        # 4 threads in the ThreadPool, plus one pipe for IPC; this also
        # serves as a sanity check that we're actually allocating some
        # resources to free later
        self.assertEqual(initial_thread_count, threading.activeCount() - 4)
        self.assertEqual(initial_pipe_count, self._pipe_count() - 2)

        tp.terminate()
        self.assertEqual(initial_thread_count, threading.activeCount())
        self.assertEqual(initial_pipe_count, self._pipe_count())

    def test_cant_run_after_terminate(self):
        tp = utils.ThreadPool(0)
        tp.terminate()
        self.assertRaises(ThreadPoolDead, tp.run_in_thread, lambda: 1)
        self.assertRaises(ThreadPoolDead, tp.force_run_in_thread, lambda: 1)

    def test_double_terminate_doesnt_crash(self):
        tp = utils.ThreadPool(0)
        tp.terminate()
        tp.terminate()

        tp = utils.ThreadPool(1)
        tp.terminate()
        tp.terminate()

    def test_terminate_no_threads_doesnt_crash(self):
        tp = utils.ThreadPool(0)
        tp.terminate()


class TestAuditLocationGenerator(unittest.TestCase):

    def test_drive_tree_access(self):
        orig_listdir = utils.listdir

        def _mock_utils_listdir(path):
            if 'bad_part' in path:
                raise OSError(errno.EACCES)
            elif 'bad_suffix' in path:
                raise OSError(errno.EACCES)
            elif 'bad_hash' in path:
                raise OSError(errno.EACCES)
            else:
                return orig_listdir(path)

        #Check Raise on Bad partition
        tmpdir = mkdtemp()
        data = os.path.join(tmpdir, "drive", "data")
        os.makedirs(data)
        obj_path = os.path.join(data, "bad_part")
        with open(obj_path, "w"):
            pass
        part1 = os.path.join(data, "partition1")
        os.makedirs(part1)
        part2 = os.path.join(data, "partition2")
        os.makedirs(part2)
        with patch('swift.common.utils.listdir', _mock_utils_listdir):
            audit = lambda: list(utils.audit_location_generator(
                tmpdir, "data", mount_check=False))
            self.assertRaises(OSError, audit)
        rmtree(tmpdir)

        #Check Raise on Bad Suffix
        tmpdir = mkdtemp()
        data = os.path.join(tmpdir, "drive", "data")
        os.makedirs(data)
        part1 = os.path.join(data, "partition1")
        os.makedirs(part1)
        part2 = os.path.join(data, "partition2")
        os.makedirs(part2)
        obj_path = os.path.join(part1, "bad_suffix")
        with open(obj_path, 'w'):
            pass
        suffix = os.path.join(part2, "suffix")
        os.makedirs(suffix)
        with patch('swift.common.utils.listdir', _mock_utils_listdir):
            audit = lambda: list(utils.audit_location_generator(
                tmpdir, "data", mount_check=False))
            self.assertRaises(OSError, audit)
        rmtree(tmpdir)

        #Check Raise on Bad Hash
        tmpdir = mkdtemp()
        data = os.path.join(tmpdir, "drive", "data")
        os.makedirs(data)
        part1 = os.path.join(data, "partition1")
        os.makedirs(part1)
        suffix = os.path.join(part1, "suffix")
        os.makedirs(suffix)
        hash1 = os.path.join(suffix, "hash1")
        os.makedirs(hash1)
        obj_path = os.path.join(suffix, "bad_hash")
        with open(obj_path, 'w'):
            pass
        with patch('swift.common.utils.listdir', _mock_utils_listdir):
            audit = lambda: list(utils.audit_location_generator(
                tmpdir, "data", mount_check=False))
            self.assertRaises(OSError, audit)
        rmtree(tmpdir)

    def test_non_dir_drive(self):
        with temptree([]) as tmpdir:
            logger = FakeLogger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            #Create a file, that represents a non-dir drive
            open(os.path.join(tmpdir, 'asdf'), 'w')
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=False, logger=logger
            )
            self.assertEqual(list(locations), [])
            self.assertEqual(1, len(logger.get_lines_for_level('warning')))
            #Test without the logger
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=False
            )
            self.assertEqual(list(locations), [])

    def test_mount_check_drive(self):
        with temptree([]) as tmpdir:
            logger = FakeLogger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            #Create a file, that represents a non-dir drive
            open(os.path.join(tmpdir, 'asdf'), 'w')
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=True, logger=logger
            )
            self.assertEqual(list(locations), [])
            self.assertEqual(2, len(logger.get_lines_for_level('warning')))

            #Test without the logger
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=True
            )
            self.assertEqual(list(locations), [])

    def test_non_dir_contents(self):
        with temptree([]) as tmpdir:
            logger = FakeLogger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            with open(os.path.join(data, "partition1"), "w"):
                pass
            partition = os.path.join(data, "partition2")
            os.makedirs(partition)
            with open(os.path.join(partition, "suffix1"), "w"):
                pass
            suffix = os.path.join(partition, "suffix2")
            os.makedirs(suffix)
            with open(os.path.join(suffix, "hash1"), "w"):
                pass
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=False, logger=logger
            )
            self.assertEqual(list(locations), [])

    def test_find_objects(self):
        with temptree([]) as tmpdir:
            expected_objs = list()
            logger = FakeLogger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            #Create a file, that represents a non-dir drive
            open(os.path.join(tmpdir, 'asdf'), 'w')
            partition = os.path.join(data, "partition1")
            os.makedirs(partition)
            suffix = os.path.join(partition, "suffix")
            os.makedirs(suffix)
            hash_path = os.path.join(suffix, "hash")
            os.makedirs(hash_path)
            obj_path = os.path.join(hash_path, "obj1.db")
            with open(obj_path, "w"):
                pass
            expected_objs.append((obj_path, 'drive', 'partition1'))
            partition = os.path.join(data, "partition2")
            os.makedirs(partition)
            suffix = os.path.join(partition, "suffix2")
            os.makedirs(suffix)
            hash_path = os.path.join(suffix, "hash2")
            os.makedirs(hash_path)
            obj_path = os.path.join(hash_path, "obj2.db")
            with open(obj_path, "w"):
                pass
            expected_objs.append((obj_path, 'drive', 'partition2'))
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=False, logger=logger
            )
            got_objs = list(locations)
            self.assertEqual(len(got_objs), len(expected_objs))
            self.assertEqual(sorted(got_objs), sorted(expected_objs))
            self.assertEqual(1, len(logger.get_lines_for_level('warning')))

    def test_ignore_metadata(self):
        with temptree([]) as tmpdir:
            logger = FakeLogger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            partition = os.path.join(data, "partition2")
            os.makedirs(partition)
            suffix = os.path.join(partition, "suffix2")
            os.makedirs(suffix)
            hash_path = os.path.join(suffix, "hash2")
            os.makedirs(hash_path)
            obj_path = os.path.join(hash_path, "obj1.dat")
            with open(obj_path, "w"):
                pass
            meta_path = os.path.join(hash_path, "obj1.meta")
            with open(meta_path, "w"):
                pass
            locations = utils.audit_location_generator(
                tmpdir, "data", ".dat", mount_check=False, logger=logger
            )
            self.assertEqual(list(locations),
                             [(obj_path, "drive", "partition2")])


class TestGreenAsyncPile(unittest.TestCase):
    def test_runs_everything(self):
        def run_test():
            tests_ran[0] += 1
            return tests_ran[0]
        tests_ran = [0]
        pile = utils.GreenAsyncPile(3)
        for x in xrange(3):
            pile.spawn(run_test)
        self.assertEqual(sorted(x for x in pile), [1, 2, 3])

    def test_is_asynchronous(self):
        def run_test(index):
            events[index].wait()
            return index

        pile = utils.GreenAsyncPile(3)
        for order in ((1, 2, 0), (0, 1, 2), (2, 1, 0), (0, 2, 1)):
            events = [eventlet.event.Event(), eventlet.event.Event(),
                      eventlet.event.Event()]
            for x in xrange(3):
                pile.spawn(run_test, x)
            for x in order:
                events[x].send()
                self.assertEqual(next(pile), x)

    def test_next_when_empty(self):
        def run_test():
            pass
        pile = utils.GreenAsyncPile(3)
        pile.spawn(run_test)
        self.assertEqual(next(pile), None)
        self.assertRaises(StopIteration, lambda: next(pile))

    def test_waitall_timeout_timesout(self):
        def run_test(sleep_duration):
            eventlet.sleep(sleep_duration)
            completed[0] += 1
            return sleep_duration

        completed = [0]
        pile = utils.GreenAsyncPile(3)
        pile.spawn(run_test, 0.1)
        pile.spawn(run_test, 1.0)
        self.assertEqual(pile.waitall(0.2), [0.1])
        self.assertEqual(completed[0], 1)

    def test_waitall_timeout_completes(self):
        def run_test(sleep_duration):
            eventlet.sleep(sleep_duration)
            completed[0] += 1
            return sleep_duration

        completed = [0]
        pile = utils.GreenAsyncPile(3)
        pile.spawn(run_test, 0.1)
        pile.spawn(run_test, 0.1)
        self.assertEqual(pile.waitall(0.5), [0.1, 0.1])
        self.assertEqual(completed[0], 2)


class TestLRUCache(unittest.TestCase):

    def test_maxsize(self):
        @utils.LRUCache(maxsize=10)
        def f(*args):
            return math.sqrt(*args)
        _orig_math_sqrt = math.sqrt
        # setup cache [0-10)
        for i in range(10):
            self.assertEqual(math.sqrt(i), f(i))
        self.assertEqual(f.size(), 10)
        # validate cache [0-10)
        with patch('math.sqrt'):
            for i in range(10):
                self.assertEqual(_orig_math_sqrt(i), f(i))
        self.assertEqual(f.size(), 10)
        # update cache [10-20)
        for i in range(10, 20):
            self.assertEqual(math.sqrt(i), f(i))
        # cache size is fixed
        self.assertEqual(f.size(), 10)
        # validate cache [10-20)
        with patch('math.sqrt'):
            for i in range(10, 20):
                self.assertEqual(_orig_math_sqrt(i), f(i))
        # validate un-cached [0-10)
        with patch('math.sqrt', new=None):
            for i in range(10):
                self.assertRaises(TypeError, f, i)
        # cache unchanged
        self.assertEqual(f.size(), 10)
        with patch('math.sqrt'):
            for i in range(10, 20):
                self.assertEqual(_orig_math_sqrt(i), f(i))
        self.assertEqual(f.size(), 10)

    def test_maxtime(self):
        @utils.LRUCache(maxtime=30)
        def f(*args):
            return math.sqrt(*args)
        self.assertEqual(30, f.maxtime)
        _orig_math_sqrt = math.sqrt

        now = time.time()
        the_future = now + 31
        # setup cache [0-10)
        with patch('time.time', lambda: now):
            for i in range(10):
                self.assertEqual(math.sqrt(i), f(i))
            self.assertEqual(f.size(), 10)
            # validate cache [0-10)
            with patch('math.sqrt'):
                for i in range(10):
                    self.assertEqual(_orig_math_sqrt(i), f(i))
            self.assertEqual(f.size(), 10)

        # validate expired [0-10)
        with patch('math.sqrt', new=None):
            with patch('time.time', lambda: the_future):
                for i in range(10):
                    self.assertRaises(TypeError, f, i)

        # validate repopulates [0-10)
        with patch('time.time', lambda: the_future):
            for i in range(10):
                self.assertEqual(math.sqrt(i), f(i))
        # reuses cache space
        self.assertEqual(f.size(), 10)

    def test_set_maxtime(self):
        @utils.LRUCache(maxtime=30)
        def f(*args):
            return math.sqrt(*args)
        self.assertEqual(30, f.maxtime)
        self.assertEqual(2, f(4))
        self.assertEqual(1, f.size())
        # expire everything
        f.maxtime = -1
        # validate un-cached [0-10)
        with patch('math.sqrt', new=None):
            self.assertRaises(TypeError, f, 4)

    def test_set_maxsize(self):
        @utils.LRUCache(maxsize=10)
        def f(*args):
            return math.sqrt(*args)
        for i in range(12):
            f(i)
        self.assertEqual(f.size(), 10)
        f.maxsize = 4
        for i in range(12):
            f(i)
        self.assertEqual(f.size(), 4)


class TestParseContentRange(unittest.TestCase):
    def test_good(self):
        start, end, total = utils.parse_content_range("bytes 100-200/300")
        self.assertEqual(start, 100)
        self.assertEqual(end, 200)
        self.assertEqual(total, 300)

    def test_bad(self):
        self.assertRaises(ValueError, utils.parse_content_range,
                          "100-300/500")
        self.assertRaises(ValueError, utils.parse_content_range,
                          "bytes 100-200/aardvark")
        self.assertRaises(ValueError, utils.parse_content_range,
                          "bytes bulbous-bouffant/4994801")


class TestParseContentDisposition(unittest.TestCase):

    def test_basic_content_type(self):
        name, attrs = utils.parse_content_disposition('text/plain')
        self.assertEquals(name, 'text/plain')
        self.assertEquals(attrs, {})

    def test_content_type_with_charset(self):
        name, attrs = utils.parse_content_disposition(
            'text/plain; charset=UTF8')
        self.assertEquals(name, 'text/plain')
        self.assertEquals(attrs, {'charset': 'UTF8'})

    def test_content_disposition(self):
        name, attrs = utils.parse_content_disposition(
            'form-data; name="somefile"; filename="test.html"')
        self.assertEquals(name, 'form-data')
        self.assertEquals(attrs, {'name': 'somefile', 'filename': 'test.html'})


class TestIterMultipartMimeDocuments(unittest.TestCase):

    def test_bad_start(self):
        it = utils.iter_multipart_mime_documents(StringIO('blah'), 'unique')
        exc = None
        try:
            it.next()
        except MimeInvalid as err:
            exc = err
        self.assertTrue('invalid starting boundary' in str(exc))
        self.assertTrue('--unique' in str(exc))

    def test_empty(self):
        it = utils.iter_multipart_mime_documents(StringIO('--unique'),
                                                 'unique')
        fp = it.next()
        self.assertEquals(fp.read(), '')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_basic(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique--'), 'unique')
        fp = it.next()
        self.assertEquals(fp.read(), 'abcdefg')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_basic2(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = it.next()
        self.assertEquals(fp.read(), 'abcdefg')
        fp = it.next()
        self.assertEquals(fp.read(), 'hijkl')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_tiny_reads(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = it.next()
        self.assertEquals(fp.read(2), 'ab')
        self.assertEquals(fp.read(2), 'cd')
        self.assertEquals(fp.read(2), 'ef')
        self.assertEquals(fp.read(2), 'g')
        self.assertEquals(fp.read(2), '')
        fp = it.next()
        self.assertEquals(fp.read(), 'hijkl')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_big_reads(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = it.next()
        self.assertEquals(fp.read(65536), 'abcdefg')
        self.assertEquals(fp.read(), '')
        fp = it.next()
        self.assertEquals(fp.read(), 'hijkl')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_broken_mid_stream(self):
        # We go ahead and accept whatever is sent instead of rejecting the
        # whole request, in case the partial form is still useful.
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabc'), 'unique')
        fp = it.next()
        self.assertEquals(fp.read(), 'abc')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_readline(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nab\r\ncd\ref\ng\r\n--unique\r\nhi\r\n\r\n'
                     'jkl\r\n\r\n--unique--'), 'unique')
        fp = it.next()
        self.assertEquals(fp.readline(), 'ab\r\n')
        self.assertEquals(fp.readline(), 'cd\ref\ng')
        self.assertEquals(fp.readline(), '')
        fp = it.next()
        self.assertEquals(fp.readline(), 'hi\r\n')
        self.assertEquals(fp.readline(), '\r\n')
        self.assertEquals(fp.readline(), 'jkl\r\n')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_readline_with_tiny_chunks(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nab\r\ncd\ref\ng\r\n--unique\r\nhi\r\n'
                     '\r\njkl\r\n\r\n--unique--'),
            'unique',
            read_chunk_size=2)
        fp = it.next()
        self.assertEquals(fp.readline(), 'ab\r\n')
        self.assertEquals(fp.readline(), 'cd\ref\ng')
        self.assertEquals(fp.readline(), '')
        fp = it.next()
        self.assertEquals(fp.readline(), 'hi\r\n')
        self.assertEquals(fp.readline(), '\r\n')
        self.assertEquals(fp.readline(), 'jkl\r\n')
        exc = None
        try:
            it.next()
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)


class TestPairs(unittest.TestCase):
    def test_pairs(self):
        items = [10, 20, 30, 40, 50, 60]
        got_pairs = set(utils.pairs(items))
        self.assertEqual(got_pairs,
                         set([(10, 20), (10, 30), (10, 40), (10, 50), (10, 60),
                              (20, 30), (20, 40), (20, 50), (20, 60),
                              (30, 40), (30, 50), (30, 60),
                              (40, 50), (40, 60),
                              (50, 60)]))


if __name__ == '__main__':
    unittest.main()
