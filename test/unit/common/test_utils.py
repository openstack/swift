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
from __future__ import print_function
from test.unit import temptree, debug_logger

import ctypes
import contextlib
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
import sys
import json
import math

import six
from six import BytesIO, StringIO
from six.moves.queue import Queue, Empty
from six.moves import range
from textwrap import dedent

import tempfile
import time
import unittest
import fcntl
import shutil

from getpass import getuser
from shutil import rmtree
from functools import partial
from tempfile import TemporaryFile, NamedTemporaryFile, mkdtemp
from netifaces import AF_INET6
from mock import MagicMock, patch
from six.moves.configparser import NoSectionError, NoOptionError
from uuid import uuid4

from swift.common.exceptions import Timeout, MessageTimeout, \
    ConnectionTimeout, LockTimeout, ReplicationLockTimeout, \
    MimeInvalid
from swift.common import utils
from swift.common.utils import is_valid_ip, is_valid_ipv4, is_valid_ipv6
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.swob import Request, Response
from test.unit import FakeLogger, requires_o_tmpfile_support

threading = eventlet.patcher.original('threading')


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

        self.assertIs(True, utils.Timestamp(ts) == ts)  # sanity
        self.assertIs(False, utils.Timestamp(ts) != utils.Timestamp(ts))
        self.assertIs(False, utils.Timestamp(ts) != ts)
        self.assertIs(False, utils.Timestamp(ts) is None)
        self.assertIs(True, utils.Timestamp(ts) is not None)

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

    def test_short_format_with_offset(self):
        expected = '1402436408.91203_f0'
        timestamp = utils.Timestamp(1402436408.91203, 0xf0)
        self.assertEqual(expected, timestamp.short)

        expected = '1402436408.91203'
        timestamp = utils.Timestamp(1402436408.91203)
        self.assertEqual(expected, timestamp.short)

    def test_raw(self):
        expected = 140243640891203
        timestamp = utils.Timestamp(1402436408.91203)
        self.assertEqual(expected, timestamp.raw)

        # 'raw' does not include offset
        timestamp = utils.Timestamp(1402436408.91203, 0xf0)
        self.assertEqual(expected, timestamp.raw)

    def test_delta(self):
        def _assertWithinBounds(expected, timestamp):
            tolerance = 0.00001
            minimum = expected - tolerance
            maximum = expected + tolerance
            self.assertTrue(float(timestamp) > minimum)
            self.assertTrue(float(timestamp) < maximum)

        timestamp = utils.Timestamp(1402436408.91203, delta=100)
        _assertWithinBounds(1402436408.91303, timestamp)
        self.assertEqual(140243640891303, timestamp.raw)

        timestamp = utils.Timestamp(1402436408.91203, delta=-100)
        _assertWithinBounds(1402436408.91103, timestamp)
        self.assertEqual(140243640891103, timestamp.raw)

        timestamp = utils.Timestamp(1402436408.91203, delta=0)
        _assertWithinBounds(1402436408.91203, timestamp)
        self.assertEqual(140243640891203, timestamp.raw)

        # delta is independent of offset
        timestamp = utils.Timestamp(1402436408.91203, offset=42, delta=100)
        self.assertEqual(140243640891303, timestamp.raw)
        self.assertEqual(42, timestamp.offset)

        # cannot go negative
        self.assertRaises(ValueError, utils.Timestamp, 1402436408.91203,
                          delta=-140243640891203)

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

    def test_cmp_with_none(self):
        self.assertGreater(utils.Timestamp(0), None)
        self.assertGreater(utils.Timestamp(1.0), None)
        self.assertGreater(utils.Timestamp(1.0, 42), None)

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

    def test_hashable(self):
        ts_0 = utils.Timestamp('1402444821.72589')
        ts_0_also = utils.Timestamp('1402444821.72589')
        self.assertEqual(ts_0, ts_0_also)  # sanity
        self.assertEqual(hash(ts_0), hash(ts_0_also))
        d = {ts_0: 'whatever'}
        self.assertIn(ts_0, d)  # sanity
        self.assertIn(ts_0_also, d)


class TestTimestampEncoding(unittest.TestCase):

    def setUp(self):
        t0 = utils.Timestamp(0.0)
        t1 = utils.Timestamp(997.9996)
        t2 = utils.Timestamp(999)
        t3 = utils.Timestamp(1000, 24)
        t4 = utils.Timestamp(1001)
        t5 = utils.Timestamp(1002.00040)

        # encodings that are expected when explicit = False
        self.non_explicit_encodings = (
            ('0000001000.00000_18', (t3, t3, t3)),
            ('0000001000.00000_18', (t3, t3, None)),
        )

        # mappings that are expected when explicit = True
        self.explicit_encodings = (
            ('0000001000.00000_18+0+0', (t3, t3, t3)),
            ('0000001000.00000_18+0', (t3, t3, None)),
        )

        # mappings that are expected when explicit = True or False
        self.encodings = (
            ('0000001000.00000_18+0+186a0', (t3, t3, t4)),
            ('0000001000.00000_18+186a0+186c8', (t3, t4, t5)),
            ('0000001000.00000_18-186a0+0', (t3, t2, t2)),
            ('0000001000.00000_18+0-186a0', (t3, t3, t2)),
            ('0000001000.00000_18-186a0-186c8', (t3, t2, t1)),
            ('0000001000.00000_18', (t3, None, None)),
            ('0000001000.00000_18+186a0', (t3, t4, None)),
            ('0000001000.00000_18-186a0', (t3, t2, None)),
            ('0000001000.00000_18', (t3, None, t1)),
            ('0000001000.00000_18-5f5e100', (t3, t0, None)),
            ('0000001000.00000_18+0-5f5e100', (t3, t3, t0)),
            ('0000001000.00000_18-5f5e100+5f45a60', (t3, t0, t2)),
        )

        # decodings that are expected when explicit = False
        self.non_explicit_decodings = (
            ('0000001000.00000_18', (t3, t3, t3)),
            ('0000001000.00000_18+186a0', (t3, t4, t4)),
            ('0000001000.00000_18-186a0', (t3, t2, t2)),
            ('0000001000.00000_18+186a0', (t3, t4, t4)),
            ('0000001000.00000_18-186a0', (t3, t2, t2)),
            ('0000001000.00000_18-5f5e100', (t3, t0, t0)),
        )

        # decodings that are expected when explicit = True
        self.explicit_decodings = (
            ('0000001000.00000_18+0+0', (t3, t3, t3)),
            ('0000001000.00000_18+0', (t3, t3, None)),
            ('0000001000.00000_18', (t3, None, None)),
            ('0000001000.00000_18+186a0', (t3, t4, None)),
            ('0000001000.00000_18-186a0', (t3, t2, None)),
            ('0000001000.00000_18-5f5e100', (t3, t0, None)),
        )

        # decodings that are expected when explicit = True or False
        self.decodings = (
            ('0000001000.00000_18+0+186a0', (t3, t3, t4)),
            ('0000001000.00000_18+186a0+186c8', (t3, t4, t5)),
            ('0000001000.00000_18-186a0+0', (t3, t2, t2)),
            ('0000001000.00000_18+0-186a0', (t3, t3, t2)),
            ('0000001000.00000_18-186a0-186c8', (t3, t2, t1)),
            ('0000001000.00000_18-5f5e100+5f45a60', (t3, t0, t2)),
        )

    def _assertEqual(self, expected, actual, test):
        self.assertEqual(expected, actual,
                         'Got %s but expected %s for parameters %s'
                         % (actual, expected, test))

    def test_encoding(self):
        for test in self.explicit_encodings:
            actual = utils.encode_timestamps(test[1][0], test[1][1],
                                             test[1][2], True)
            self._assertEqual(test[0], actual, test[1])
        for test in self.non_explicit_encodings:
            actual = utils.encode_timestamps(test[1][0], test[1][1],
                                             test[1][2], False)
            self._assertEqual(test[0], actual, test[1])
        for explicit in (True, False):
            for test in self.encodings:
                actual = utils.encode_timestamps(test[1][0], test[1][1],
                                                 test[1][2], explicit)
                self._assertEqual(test[0], actual, test[1])

    def test_decoding(self):
        for test in self.explicit_decodings:
            actual = utils.decode_timestamps(test[0], True)
            self._assertEqual(test[1], actual, test[0])
        for test in self.non_explicit_decodings:
            actual = utils.decode_timestamps(test[0], False)
            self._assertEqual(test[1], actual, test[0])
        for explicit in (True, False):
            for test in self.decodings:
                actual = utils.decode_timestamps(test[0], explicit)
                self._assertEqual(test[1], actual, test[0])


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
        self.assertEqual(utils.normalize_timestamp('1253327593.48174'),
                         "1253327593.48174")
        self.assertEqual(utils.normalize_timestamp(1253327593.48174),
                         "1253327593.48174")
        self.assertEqual(utils.normalize_timestamp('1253327593.48'),
                         "1253327593.48000")
        self.assertEqual(utils.normalize_timestamp(1253327593.48),
                         "1253327593.48000")
        self.assertEqual(utils.normalize_timestamp('253327593.48'),
                         "0253327593.48000")
        self.assertEqual(utils.normalize_timestamp(253327593.48),
                         "0253327593.48000")
        self.assertEqual(utils.normalize_timestamp('1253327593'),
                         "1253327593.00000")
        self.assertEqual(utils.normalize_timestamp(1253327593),
                         "1253327593.00000")
        self.assertRaises(ValueError, utils.normalize_timestamp, '')
        self.assertRaises(ValueError, utils.normalize_timestamp, 'abc')

    def test_normalize_delete_at_timestamp(self):
        self.assertEqual(
            utils.normalize_delete_at_timestamp(1253327593),
            '1253327593')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(1253327593.67890),
            '1253327593')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('1253327593'),
            '1253327593')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('1253327593.67890'),
            '1253327593')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(-1253327593),
            '0000000000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(-1253327593.67890),
            '0000000000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('-1253327593'),
            '0000000000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('-1253327593.67890'),
            '0000000000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(71253327593),
            '9999999999')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(71253327593.67890),
            '9999999999')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('71253327593'),
            '9999999999')
        self.assertEqual(
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
        lines = [b'123456789x12345678><123456789\n',  # block larger than rest
                 b'123456789x123>\n',  # block ends just before \n character
                 b'123423456789\n',
                 b'123456789x\n',  # block ends at the end of line
                 b'<123456789x123456789x123\n',
                 b'<6789x123\n',  # block ends at the beginning of the line
                 b'6789x1234\n',
                 b'1234><234\n',  # block ends typically in the middle of line
                 b'123456789x123456789\n']

        with TemporaryFile() as f:
            for line in lines:
                f.write(line)

            count = len(lines) - 1
            for line in utils.backward(f, blocksize):
                self.assertEqual(line, lines[count].split(b'\n')[0])
                count -= 1

        # Empty file case
        with TemporaryFile('r') as f:
            self.assertEqual([], list(utils.backward(f)))

    def test_mkdirs(self):
        testdir_base = mkdtemp()
        testroot = os.path.join(testdir_base, 'mkdirs')
        try:
            self.assertTrue(not os.path.exists(testroot))
            utils.mkdirs(testroot)
            self.assertTrue(os.path.exists(testroot))
            utils.mkdirs(testroot)
            self.assertTrue(os.path.exists(testroot))
            rmtree(testroot, ignore_errors=1)

            testdir = os.path.join(testroot, 'one/two/three')
            self.assertTrue(not os.path.exists(testdir))
            utils.mkdirs(testdir)
            self.assertTrue(os.path.exists(testdir))
            utils.mkdirs(testdir)
            self.assertTrue(os.path.exists(testdir))
            rmtree(testroot, ignore_errors=1)

            open(testroot, 'wb').close()
            self.assertTrue(not os.path.exists(testdir))
            self.assertRaises(OSError, utils.mkdirs, testdir)
            os.unlink(testroot)
        finally:
            rmtree(testdir_base)

    def test_split_path(self):
        # Test swift.common.utils.split_account_path
        self.assertRaises(ValueError, utils.split_path, '')
        self.assertRaises(ValueError, utils.split_path, '/')
        self.assertRaises(ValueError, utils.split_path, '//')
        self.assertEqual(utils.split_path('/a'), ['a'])
        self.assertRaises(ValueError, utils.split_path, '//a')
        self.assertEqual(utils.split_path('/a/'), ['a'])
        self.assertRaises(ValueError, utils.split_path, '/a/c')
        self.assertRaises(ValueError, utils.split_path, '//c')
        self.assertRaises(ValueError, utils.split_path, '/a/c/')
        self.assertRaises(ValueError, utils.split_path, '/a//')
        self.assertRaises(ValueError, utils.split_path, '/a', 2)
        self.assertRaises(ValueError, utils.split_path, '/a', 2, 3)
        self.assertRaises(ValueError, utils.split_path, '/a', 2, 3, True)
        self.assertEqual(utils.split_path('/a/c', 2), ['a', 'c'])
        self.assertEqual(utils.split_path('/a/c/o', 3), ['a', 'c', 'o'])
        self.assertRaises(ValueError, utils.split_path, '/a/c/o/r', 3, 3)
        self.assertEqual(utils.split_path('/a/c/o/r', 3, 3, True),
                         ['a', 'c', 'o/r'])
        self.assertEqual(utils.split_path('/a/c', 2, 3, True),
                         ['a', 'c', None])
        self.assertRaises(ValueError, utils.split_path, '/a', 5, 4)
        self.assertEqual(utils.split_path('/a/c/', 2), ['a', 'c'])
        self.assertEqual(utils.split_path('/a/c/', 2, 3), ['a', 'c', ''])
        try:
            utils.split_path('o\nn e', 2)
        except ValueError as err:
            self.assertEqual(str(err), 'Invalid path: o%0An%20e')
        try:
            utils.split_path('o\nn e', 2, 3, True)
        except ValueError as err:
            self.assertEqual(str(err), 'Invalid path: o%0An%20e')

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
            self.assertEqual(str(err), 'Invalid device: o%0An%20e')
        try:
            utils.validate_device_partition('foo', 'o\nn e')
        except ValueError as err:
            self.assertEqual(str(err), 'Invalid partition: o%0An%20e')

    def test_NullLogger(self):
        # Test swift.common.utils.NullLogger
        sio = StringIO()
        nl = utils.NullLogger()
        nl.write('test')
        self.assertEqual(sio.getvalue(), '')

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
        print('test1')
        self.assertEqual(sio.getvalue(), '')
        sys.stdout = lfo_stdout
        print('test2')
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\n')
        sys.stderr = lfo_stderr
        print('test4', file=sys.stderr)
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n')
        sys.stdout = orig_stdout
        print('test5')
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n')
        print('test6', file=sys.stderr)
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                         'STDERR: test6\n')
        sys.stderr = orig_stderr
        print('test8')
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                         'STDERR: test6\n')
        lfo_stdout.writelines(['a', 'b', 'c'])
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                         'STDERR: test6\nSTDOUT: a#012b#012c\n')
        lfo_stdout.close()
        lfo_stderr.close()
        lfo_stdout.write('d')
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                         'STDERR: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
        lfo_stdout.flush()
        self.assertEqual(sio.getvalue(), 'STDOUT: test2\nSTDERR: test4\n'
                         'STDERR: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
        for lfo in (lfo_stdout, lfo_stderr):
            got_exc = False
            try:
                for line in lfo:
                    pass
            except Exception:
                got_exc = True
            self.assertTrue(got_exc)
            got_exc = False
            try:
                for line in lfo:
                    pass
            except Exception:
                got_exc = True
            self.assertTrue(got_exc)
            self.assertRaises(IOError, lfo.read)
            self.assertRaises(IOError, lfo.read, 1024)
            self.assertRaises(IOError, lfo.readline)
            self.assertRaises(IOError, lfo.readline, 1024)
            lfo.tell()

    def test_LoggerFileObject_recursion(self):
        crashy_calls = [0]

        class CrashyLogger(logging.Handler):
            def emit(self, record):
                crashy_calls[0] += 1
                try:
                    # Pretend to be trying to send to syslog, but syslogd is
                    # dead. We need the raise here to set sys.exc_info.
                    raise socket.error(errno.ENOTCONN, "This is an ex-syslog")
                except socket.error:
                    self.handleError(record)

        logger = logging.getLogger()
        logger.addHandler(CrashyLogger())

        # Set up some real file descriptors for stdio. If you run
        # nosetests with "-s", you already have real files there, but
        # otherwise they're StringIO objects.
        #
        # In any case, since capture_stdio() closes sys.stdin and friends,
        # we'd want to set up some sacrificial files so as to not goof up
        # the testrunner.
        new_stdin = open(os.devnull, 'r+b')
        new_stdout = open(os.devnull, 'w+b')
        new_stderr = open(os.devnull, 'w+b')

        with contextlib.closing(new_stdin), contextlib.closing(new_stdout), \
                contextlib.closing(new_stderr):
            # logging.raiseExceptions is set to False in test/__init__.py, but
            # is True in Swift daemons, and the error doesn't manifest without
            # it.
            with mock.patch('sys.stdin', new_stdin), \
                    mock.patch('sys.stdout', new_stdout), \
                    mock.patch('sys.stderr', new_stderr), \
                    mock.patch.object(logging, 'raiseExceptions', True):
                # Note: since stdio is hooked up to /dev/null in here, using
                # pdb is basically impossible. Sorry about that.
                utils.capture_stdio(logger)
                logger.info("I like ham")
                self.assertTrue(crashy_calls[0], 1)

    def test_parse_options(self):
        # Get a file that is definitely on disk
        with NamedTemporaryFile() as f:
            conf_file = f.name
            conf, options = utils.parse_options(test_args=[conf_file])
            self.assertEqual(conf, conf_file)
            # assert defaults
            self.assertEqual(options['verbose'], False)
            self.assertTrue('once' not in options)
            # assert verbose as option
            conf, options = utils.parse_options(test_args=[conf_file, '-v'])
            self.assertEqual(options['verbose'], True)
            # check once option
            conf, options = utils.parse_options(test_args=[conf_file],
                                                once=True)
            self.assertEqual(options['once'], False)
            test_args = [conf_file, '--once']
            conf, options = utils.parse_options(test_args=test_args, once=True)
            self.assertEqual(options['once'], True)
            # check options as arg parsing
            test_args = [conf_file, 'once', 'plugin_name', 'verbose']
            conf, options = utils.parse_options(test_args=test_args, once=True)
            self.assertEqual(options['verbose'], True)
            self.assertEqual(options['once'], True)
            self.assertEqual(options['extra_args'], ['plugin_name'])

    def test_parse_options_errors(self):
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        stdo = StringIO()
        stde = StringIO()
        utils.sys.stdout = stdo
        utils.sys.stderr = stde
        self.assertRaises(SystemExit, utils.parse_options, once=True,
                          test_args=[])
        self.assertTrue('missing config' in stdo.getvalue())

        # verify conf file must exist, context manager will delete temp file
        with NamedTemporaryFile() as f:
            conf_file = f.name
        self.assertRaises(SystemExit, utils.parse_options, once=True,
                          test_args=[conf_file])
        self.assertTrue('unable to locate' in stdo.getvalue())

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
            self.assertEqual(submit_dict, file_dict)
            # Use a nested entry
            submit_dict = {'key1': {'key2': {'value1': 1, 'value2': 2}}}
            result_dict = {'key1': {'key2': {'value1': 1, 'value2': 2},
                           'value1': 1, 'value2': 2}}
            utils.dump_recon_cache(submit_dict, testcache_file, logger)
            fd = open(testcache_file)
            file_dict = json.loads(fd.readline())
            fd.close()
            self.assertEqual(result_dict, file_dict)
        finally:
            rmtree(testdir_base)

    def test_dump_recon_cache_permission_denied(self):
        testdir_base = mkdtemp()
        testcache_file = os.path.join(testdir_base, 'cache.recon')

        class MockLogger(object):
            def __init__(self):
                self._excs = []

            def exception(self, message):
                _junk, exc, _junk = sys.exc_info()
                self._excs.append(exc)

        logger = MockLogger()
        try:
            submit_dict = {'key1': {'value1': 1, 'value2': 2}}
            with mock.patch(
                    'swift.common.utils.NamedTemporaryFile',
                    side_effect=IOError(13, 'Permission Denied')):
                utils.dump_recon_cache(submit_dict, testcache_file, logger)
            self.assertIsInstance(logger._excs[0], IOError)
        finally:
            rmtree(testdir_base)

    def test_get_logger(self):
        sio = StringIO()
        logger = logging.getLogger('server')
        logger.addHandler(logging.StreamHandler(sio))
        logger = utils.get_logger(None, 'server', log_route='server')
        logger.warning('test1')
        self.assertEqual(sio.getvalue(), 'test1\n')
        logger.debug('test2')
        self.assertEqual(sio.getvalue(), 'test1\n')
        logger = utils.get_logger({'log_level': 'DEBUG'}, 'server',
                                  log_route='server')
        logger.debug('test3')
        self.assertEqual(sio.getvalue(), 'test1\ntest3\n')
        # Doesn't really test that the log facility is truly being used all the
        # way to syslog; but exercises the code.
        logger = utils.get_logger({'log_facility': 'LOG_LOCAL3'}, 'server',
                                  log_route='server')
        logger.warning('test4')
        self.assertEqual(sio.getvalue(),
                         'test1\ntest3\ntest4\n')
        # make sure debug doesn't log by default
        logger.debug('test5')
        self.assertEqual(sio.getvalue(),
                         'test1\ntest3\ntest4\n')
        # make sure notice lvl logs by default
        logger.notice('test6')
        self.assertEqual(sio.getvalue(),
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
            self.assertEqual(expected_args, syslog_handler_args)

            syslog_handler_args = []
            utils.get_logger({
                'log_facility': 'LOG_LOCAL3',
                'log_address': '/foo/bar',
            }, 'server', log_route='server')
            self.assertEqual([
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
            self.assertEqual([
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
            self.assertEqual([
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
            sio.seek(0)
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
            self.assertEqual(strip_value(sio), '')
            logger.info('test')
            self.assertEqual(strip_value(sio), 'test\n')
            self.assertEqual(strip_value(sio), '')
            logger.info('test')
            logger.info('test')
            self.assertEqual(strip_value(sio), 'test\ntest\n')
            self.assertEqual(strip_value(sio), '')

            # test OSError
            for en in (errno.EIO, errno.ENOSPC):
                log_exception(OSError(en, 'my %s error message' % en))
                log_msg = strip_value(sio)
                self.assertTrue('Traceback' not in log_msg)
                self.assertTrue('my %s error message' % en in log_msg)
            # unfiltered
            log_exception(OSError())
            self.assertTrue('Traceback' in strip_value(sio))

            # test socket.error
            log_exception(socket.error(errno.ECONNREFUSED,
                                       'my error message'))
            log_msg = strip_value(sio)
            self.assertTrue('Traceback' not in log_msg)
            self.assertTrue('errno.ECONNREFUSED message test' not in log_msg)
            self.assertTrue('Connection refused' in log_msg)
            log_exception(socket.error(errno.EHOSTUNREACH,
                                       'my error message'))
            log_msg = strip_value(sio)
            self.assertTrue('Traceback' not in log_msg)
            self.assertTrue('my error message' not in log_msg)
            self.assertTrue('Host unreachable' in log_msg)
            log_exception(socket.error(errno.ETIMEDOUT, 'my error message'))
            log_msg = strip_value(sio)
            self.assertTrue('Traceback' not in log_msg)
            self.assertTrue('my error message' not in log_msg)
            self.assertTrue('Connection timeout' in log_msg)
            # unfiltered
            log_exception(socket.error(0, 'my error message'))
            log_msg = strip_value(sio)
            self.assertTrue('Traceback' in log_msg)
            self.assertTrue('my error message' in log_msg)

            # test eventlet.Timeout
            connection_timeout = ConnectionTimeout(42, 'my error message')
            log_exception(connection_timeout)
            log_msg = strip_value(sio)
            self.assertTrue('Traceback' not in log_msg)
            self.assertTrue('ConnectionTimeout' in log_msg)
            self.assertTrue('(42s)' in log_msg)
            self.assertTrue('my error message' not in log_msg)
            connection_timeout.cancel()

            message_timeout = MessageTimeout(42, 'my error message')
            log_exception(message_timeout)
            log_msg = strip_value(sio)
            self.assertTrue('Traceback' not in log_msg)
            self.assertTrue('MessageTimeout' in log_msg)
            self.assertTrue('(42s)' in log_msg)
            self.assertTrue('my error message' in log_msg)
            message_timeout.cancel()

            # test unhandled
            log_exception(Exception('my error message'))
            log_msg = strip_value(sio)
            self.assertTrue('Traceback' in log_msg)
            self.assertTrue('my error message' in log_msg)

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
            sio.seek(0)
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
            sio.seek(0)
            v = sio.getvalue()
            sio.truncate(0)
            return v

        try:
            self.assertFalse(logger.txn_id)
            logger.error('my error message')
            log_msg = strip_value(sio)
            self.assertTrue('my error message' in log_msg)
            self.assertTrue('txn' not in log_msg)
            logger.txn_id = '12345'
            logger.error('test')
            log_msg = strip_value(sio)
            self.assertTrue('txn' in log_msg)
            self.assertTrue('12345' in log_msg)
            # test txn in info message
            self.assertEqual(logger.txn_id, '12345')
            logger.info('test')
            log_msg = strip_value(sio)
            self.assertTrue('txn' in log_msg)
            self.assertTrue('12345' in log_msg)
            # test txn already in message
            self.assertEqual(logger.txn_id, '12345')
            logger.warning('test 12345 test')
            self.assertEqual(strip_value(sio), 'test 12345 test\n')
            # Test multi line collapsing
            logger.error('my\nerror\nmessage')
            log_msg = strip_value(sio)
            self.assertTrue('my#012error#012message' in log_msg)

            # test client_ip
            self.assertFalse(logger.client_ip)
            logger.error('my error message')
            log_msg = strip_value(sio)
            self.assertTrue('my error message' in log_msg)
            self.assertTrue('client_ip' not in log_msg)
            logger.client_ip = '1.2.3.4'
            logger.error('test')
            log_msg = strip_value(sio)
            self.assertTrue('client_ip' in log_msg)
            self.assertTrue('1.2.3.4' in log_msg)
            # test no client_ip on info message
            self.assertEqual(logger.client_ip, '1.2.3.4')
            logger.info('test')
            log_msg = strip_value(sio)
            self.assertTrue('client_ip' not in log_msg)
            self.assertTrue('1.2.3.4' not in log_msg)
            # test client_ip (and txn) already in message
            self.assertEqual(logger.client_ip, '1.2.3.4')
            logger.warning('test 1.2.3.4 test 12345')
            self.assertEqual(strip_value(sio), 'test 1.2.3.4 test 12345\n')
        finally:
            logger.logger.removeHandler(handler)

    def test_storage_directory(self):
        self.assertEqual(utils.storage_directory('objects', '1', 'ABCDEF'),
                         'objects/1/DEF/ABCDEF')

    def test_is_valid_ip(self):
        self.assertTrue(is_valid_ip("127.0.0.1"))
        self.assertTrue(is_valid_ip("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "fe80::"
        self.assertTrue(is_valid_ip(ipv6))
        ipv6 = "::1"
        self.assertTrue(is_valid_ip(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(is_valid_ip(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(is_valid_ip(not_ipv6))

    def test_is_valid_ipv4(self):
        self.assertTrue(is_valid_ipv4("127.0.0.1"))
        self.assertTrue(is_valid_ipv4("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "fe80::"
        self.assertFalse(is_valid_ipv4(ipv6))
        ipv6 = "::1"
        self.assertFalse(is_valid_ipv4(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(is_valid_ipv4(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(is_valid_ipv4(not_ipv6))

    def test_is_valid_ipv6(self):
        self.assertFalse(is_valid_ipv6("127.0.0.1"))
        self.assertFalse(is_valid_ipv6("10.0.0.1"))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80:0:0:0:204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80::204:61ff:fe9d:f156"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80:0000:0000:0000:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80:0:0:0:0204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80::204:61ff:254.157.241.86"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "fe80::"
        self.assertTrue(is_valid_ipv6(ipv6))
        ipv6 = "::1"
        self.assertTrue(is_valid_ipv6(ipv6))
        not_ipv6 = "3ffe:0b00:0000:0001:0000:0000:000a"
        self.assertFalse(is_valid_ipv6(not_ipv6))
        not_ipv6 = "1:2:3:4:5:6::7:8"
        self.assertFalse(is_valid_ipv6(not_ipv6))

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
        self.assertTrue(len(myips) > 1)
        self.assertTrue('127.0.0.1' in myips)

    def test_whataremyips_bind_to_all(self):
        for any_addr in ('0.0.0.0', '0000:0000:0000:0000:0000:0000:0000:0000',
                         '::0', '::0000', '::',
                         # Wacky parse-error input produces all IPs
                         'I am a bear'):
            myips = utils.whataremyips(any_addr)
            self.assertTrue(len(myips) > 1)
            self.assertTrue('127.0.0.1' in myips)

    def test_whataremyips_bind_ip_specific(self):
        self.assertEqual(['1.2.3.4'], utils.whataremyips('1.2.3.4'))

    def test_whataremyips_error(self):
        def my_interfaces():
            return ['eth0']

        def my_ifaddress_error(interface):
            raise ValueError

        with patch('netifaces.interfaces', my_interfaces), \
                patch('netifaces.ifaddresses', my_ifaddress_error):
            self.assertEqual(utils.whataremyips(), [])

    def test_whataremyips_ipv6(self):
        test_ipv6_address = '2001:6b0:dead:beef:2::32'
        test_interface = 'eth0'

        def my_ipv6_interfaces():
            return ['eth0']

        def my_ipv6_ifaddresses(interface):
            return {AF_INET6:
                    [{'netmask': 'ffff:ffff:ffff:ffff::',
                      'addr': '%s%%%s' % (test_ipv6_address, test_interface)}]}
        with patch('netifaces.interfaces', my_ipv6_interfaces), \
                patch('netifaces.ifaddresses', my_ipv6_ifaddresses):
            myips = utils.whataremyips()
            self.assertEqual(len(myips), 1)
            self.assertEqual(myips[0], test_ipv6_address)

    def test_hash_path(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results hash_path produces, they know it
        with mock.patch('swift.common.utils.HASH_PATH_PREFIX', ''):
            self.assertEqual(utils.hash_path('a'),
                             '1c84525acb02107ea475dcd3d09c2c58')
            self.assertEqual(utils.hash_path('a', 'c'),
                             '33379ecb053aa5c9e356c68997cbb59e')
            self.assertEqual(utils.hash_path('a', 'c', 'o'),
                             '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEqual(utils.hash_path('a', 'c', 'o', raw_digest=False),
                             '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEqual(utils.hash_path('a', 'c', 'o', raw_digest=True),
                             '\x06\xfb\xf0\xb5\x14\xe5\x19\x9d\xfcN'
                             '\x00\xf4.\xb5\xea\x83')
            self.assertRaises(ValueError, utils.hash_path, 'a', object='o')
            utils.HASH_PATH_PREFIX = 'abcdef'
            self.assertEqual(utils.hash_path('a', 'c', 'o', raw_digest=False),
                             '363f9b535bfb7d17a43a46a358afca0e')

    def test_validate_hash_conf(self):
        # no section causes InvalidHashPathConfigError
        self._test_validate_hash_conf([], [], True)

        # 'swift-hash' section is there but no options causes
        # InvalidHashPathConfigError
        self._test_validate_hash_conf(['swift-hash'], [], True)

        # if we have the section and either of prefix or suffix,
        # InvalidHashPathConfigError doesn't occur
        self._test_validate_hash_conf(
            ['swift-hash'], ['swift_hash_path_prefix'], False)
        self._test_validate_hash_conf(
            ['swift-hash'], ['swift_hash_path_suffix'], False)

        # definitely, we have the section and both of them,
        # InvalidHashPathConfigError doesn't occur
        self._test_validate_hash_conf(
            ['swift-hash'],
            ['swift_hash_path_suffix', 'swift_hash_path_prefix'], False)

        # But invalid section name should make an error even if valid
        # options are there
        self._test_validate_hash_conf(
            ['swift-hash-xxx'],
            ['swift_hash_path_suffix', 'swift_hash_path_prefix'], True)

    def _test_validate_hash_conf(self, sections, options, should_raise_error):

        class FakeConfigParser(object):
            def read(self, conf_path):
                return True

            def get(self, section, option):
                if section not in sections:
                    raise NoSectionError('section error')
                elif option not in options:
                    raise NoOptionError('option error', 'this option')
                else:
                    return 'some_option_value'

        with mock.patch('swift.common.utils.HASH_PATH_PREFIX', ''), \
                mock.patch('swift.common.utils.HASH_PATH_SUFFIX', ''), \
                mock.patch('swift.common.utils.ConfigParser',
                           FakeConfigParser):
            try:
                utils.validate_hash_conf()
            except utils.InvalidHashPathConfigError:
                if not should_raise_error:
                    self.fail('validate_hash_conf should not raise an error')
            else:
                if should_raise_error:
                    self.fail('validate_hash_conf should raise an error')

    def test_load_libc_function(self):
        self.assertTrue(callable(
            utils.load_libc_function('printf')))
        self.assertTrue(callable(
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
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile, 'section1')
            expected = {'__file__': conffile, 'log_name': 'section1',
                        'foo': 'bar'}
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile,
                                    'section2').get('log_name')
            expected = 'yarr'
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile, 'section1',
                                    log_name='foo').get('log_name')
            expected = 'foo'
            self.assertEqual(result, expected)
            conffile = conf_object_maker()
            result = utils.readconf(conffile, 'section1',
                                    defaults={'bar': 'baz'})
            expected = {'__file__': conffile, 'log_name': 'section1',
                        'foo': 'bar', 'bar': 'baz'}
            self.assertEqual(result, expected)
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
            self.assertEqual(result, expected)
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
        self.assertEqual(conf, expected)

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
        self.assertEqual(conf, expected)

    def test_drop_privileges(self):
        user = getuser()
        # over-ride os with mock
        required_func_calls = ('setgroups', 'setgid', 'setuid', 'setsid',
                               'chdir', 'umask')
        utils.os = MockOs(called_funcs=required_func_calls)
        # exercise the code
        utils.drop_privileges(user)
        for func in required_func_calls:
            self.assertTrue(utils.os.called_funcs[func])
        import pwd
        self.assertEqual(pwd.getpwnam(user)[5], utils.os.environ['HOME'])

        groups = [g.gr_gid for g in grp.getgrall() if user in g.gr_mem]
        groups.append(pwd.getpwnam(user).pw_gid)
        self.assertEqual(set(groups), set(os.getgroups()))

        # reset; test same args, OSError trying to get session leader
        utils.os = MockOs(called_funcs=required_func_calls,
                          raise_funcs=('setsid',))
        for func in required_func_calls:
            self.assertFalse(utils.os.called_funcs.get(func, False))
        utils.drop_privileges(user)
        for func in required_func_calls:
            self.assertTrue(utils.os.called_funcs[func])

    def test_drop_privileges_no_call_setsid(self):
        user = getuser()
        # over-ride os with mock
        required_func_calls = ('setgroups', 'setgid', 'setuid', 'chdir',
                               'umask')
        bad_func_calls = ('setsid',)
        utils.os = MockOs(called_funcs=required_func_calls,
                          raise_funcs=bad_func_calls)
        # exercise the code
        utils.drop_privileges(user, call_setsid=False)
        for func in required_func_calls:
            self.assertTrue(utils.os.called_funcs[func])
        for func in bad_func_calls:
            self.assertTrue(func not in utils.os.called_funcs)

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
            self.assertTrue(utils.sys.excepthook is not None)
            self.assertEqual(utils.os.closed_fds, utils.sys.stdio_fds)
            self.assertTrue(
                isinstance(utils.sys.stdout, utils.LoggerFileObject))
            self.assertTrue(
                isinstance(utils.sys.stderr, utils.LoggerFileObject))

            # reset; test same args, but exc when trying to close stdio
            utils.os = MockOs(raise_funcs=('dup2',))
            utils.sys = MockSys()

            # test unable to close stdio
            utils.capture_stdio(logger)
            self.assertTrue(utils.sys.excepthook is not None)
            self.assertEqual(utils.os.closed_fds, [])
            self.assertTrue(
                isinstance(utils.sys.stdout, utils.LoggerFileObject))
            self.assertTrue(
                isinstance(utils.sys.stderr, utils.LoggerFileObject))

            # reset; test some other args
            utils.os = MockOs()
            utils.sys = MockSys()
            logger = utils.get_logger(None, log_to_console=True)

            # test console log
            utils.capture_stdio(logger, capture_stdout=False,
                                capture_stderr=False)
            self.assertTrue(utils.sys.excepthook is not None)
            # when logging to console, stderr remains open
            self.assertEqual(utils.os.closed_fds, utils.sys.stdio_fds[:2])
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
        self.assertTrue(console_handlers)
        # make sure you can't have two console handlers
        self.assertEqual(len(console_handlers), 1)
        old_handler = console_handlers[0]
        logger = utils.get_logger(None, log_to_console=True)
        console_handlers = [h for h in logger.logger.handlers if
                            isinstance(h, logging.StreamHandler)]
        self.assertEqual(len(console_handlers), 1)
        new_handler = console_handlers[0]
        self.assertNotEqual(new_handler, old_handler)

    def verify_under_pseudo_time(
            self, func, target_runtime_ms=1, *args, **kwargs):
        curr_time = [42.0]

        def my_time():
            curr_time[0] += 0.001
            return curr_time[0]

        def my_sleep(duration):
            curr_time[0] += 0.001
            curr_time[0] += duration

        with patch('time.time', my_time), \
                patch('time.sleep', my_sleep), \
                patch('eventlet.sleep', my_sleep):
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
            self.assertEqual(248, total)

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
        self.assertEqual(parsed.scheme, 'http')
        self.assertEqual(parsed.hostname, '127.0.0.1')
        self.assertEqual(parsed.path, '/')

        parsed = utils.urlparse('http://127.0.0.1:8080/')
        self.assertEqual(parsed.port, 8080)

        parsed = utils.urlparse('https://127.0.0.1/')
        self.assertEqual(parsed.scheme, 'https')

        parsed = utils.urlparse('http://[::1]/')
        self.assertEqual(parsed.hostname, '::1')

        parsed = utils.urlparse('http://[::1]:8080/')
        self.assertEqual(parsed.hostname, '::1')
        self.assertEqual(parsed.port, 8080)

        parsed = utils.urlparse('www.example.com')
        self.assertEqual(parsed.hostname, '')

    def test_search_tree(self):
        # file match & ext miss
        with temptree(['asdf.conf', 'blarg.conf', 'asdf.cfg']) as t:
            asdf = utils.search_tree(t, 'a*', '.conf')
            self.assertEqual(len(asdf), 1)
            self.assertEqual(asdf[0],
                             os.path.join(t, 'asdf.conf'))

        # multi-file match & glob miss & sort
        with temptree(['application.bin', 'apple.bin', 'apropos.bin']) as t:
            app_bins = utils.search_tree(t, 'app*', 'bin')
            self.assertEqual(len(app_bins), 2)
            self.assertEqual(app_bins[0],
                             os.path.join(t, 'apple.bin'))
            self.assertEqual(app_bins[1],
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
            self.assertEqual(len(sub_ini), 1)
            self.assertEqual(sub_ini[0],
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
            self.assertEqual(len(folder_texts), 4)
            f1 = os.path.join(t, 'folder_file.txt')
            f2 = os.path.join(t, 'folder/1.txt')
            f3 = os.path.join(t, 'folder/sub/2.txt')
            f4 = os.path.join(t, 'folder2/3.txt')
            for f in [f1, f2, f3, f4]:
                self.assertTrue(f in folder_texts)

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
        self.assertEqual(len(conf_dirs), 4)
        for i in range(4):
            conf_dir = os.path.join(t, 'object-server/%d.conf.d' % (i + 1))
            self.assertTrue(conf_dir in conf_dirs)

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
        self.assertEqual(len(conf_dirs), 1)
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
        self.assertEqual(len(pid_files), 1)
        pid_file = pid_files[0]
        expected = os.path.join(t, 'proxy-server/proxy-noauth.pid.d')
        self.assertEqual(pid_file, expected)

    def test_write_file(self):
        with temptree([]) as t:
            file_name = os.path.join(t, 'test')
            utils.write_file(file_name, 'test')
            with open(file_name, 'r') as f:
                contents = f.read()
            self.assertEqual(contents, 'test')
            # and also subdirs
            file_name = os.path.join(t, 'subdir/test2')
            utils.write_file(file_name, 'test2')
            with open(file_name, 'r') as f:
                contents = f.read()
            self.assertEqual(contents, 'test2')
            # but can't over-write files
            file_name = os.path.join(t, 'subdir/test2/test3')
            self.assertRaises(IOError, utils.write_file, file_name,
                              'test3')

    def test_remove_file(self):
        with temptree([]) as t:
            file_name = os.path.join(t, 'blah.pid')
            # assert no raise
            self.assertEqual(os.path.exists(file_name), False)
            self.assertEqual(utils.remove_file(file_name), None)
            with open(file_name, 'w') as f:
                f.write('1')
            self.assertTrue(os.path.exists(file_name))
            self.assertEqual(utils.remove_file(file_name), None)
            self.assertFalse(os.path.exists(file_name))

    def test_human_readable(self):
        self.assertEqual(utils.human_readable(0), '0')
        self.assertEqual(utils.human_readable(1), '1')
        self.assertEqual(utils.human_readable(10), '10')
        self.assertEqual(utils.human_readable(100), '100')
        self.assertEqual(utils.human_readable(999), '999')
        self.assertEqual(utils.human_readable(1024), '1Ki')
        self.assertEqual(utils.human_readable(1535), '1Ki')
        self.assertEqual(utils.human_readable(1536), '2Ki')
        self.assertEqual(utils.human_readable(1047552), '1023Ki')
        self.assertEqual(utils.human_readable(1048063), '1023Ki')
        self.assertEqual(utils.human_readable(1048064), '1Mi')
        self.assertEqual(utils.human_readable(1048576), '1Mi')
        self.assertEqual(utils.human_readable(1073741824), '1Gi')
        self.assertEqual(utils.human_readable(1099511627776), '1Ti')
        self.assertEqual(utils.human_readable(1125899906842624), '1Pi')
        self.assertEqual(utils.human_readable(1152921504606846976), '1Ei')
        self.assertEqual(utils.human_readable(1180591620717411303424), '1Zi')
        self.assertEqual(utils.human_readable(1208925819614629174706176),
                         '1Yi')
        self.assertEqual(utils.human_readable(1237940039285380274899124224),
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
                        self.assertEqual(
                            utils.validate_sync_to(
                                goodurl, ['1.1.1.1', '2.2.2.2'], realms_conf),
                            (None, None, None, None))
                    else:
                        self.assertEqual(
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
                        self.assertEqual(
                            utils.validate_sync_to(
                                badurl, ['1.1.1.1', '2.2.2.2'], realms_conf),
                            (None, None, None, None))
                    else:
                        self.assertEqual(
                            utils.validate_sync_to(
                                badurl, ['1.1.1.1', '2.2.2.2'], realms_conf),
                            result)

    def test_TRUE_VALUES(self):
        for v in utils.TRUE_VALUES:
            self.assertEqual(v, v.lower())

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
                self.assertEqual(expected, rv)

    def test_streq_const_time(self):
        self.assertTrue(utils.streq_const_time('abc123', 'abc123'))
        self.assertFalse(utils.streq_const_time('a', 'aaaaa'))
        self.assertFalse(utils.streq_const_time('ABC123', 'abc123'))

    def test_quorum_size(self):
        expected_sizes = {1: 1,
                          2: 1,
                          3: 2,
                          4: 2,
                          5: 3}
        got_sizes = dict([(n, utils.quorum_size(n))
                          for n in expected_sizes])
        self.assertEqual(expected_sizes, got_sizes)

    def test_majority_size(self):
        expected_sizes = {1: 1,
                          2: 2,
                          3: 2,
                          4: 3,
                          5: 3}
        got_sizes = dict([(n, utils.majority_size(n))
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

    def test_rsync_module_interpolation(self):
        fake_device = {'ip': '127.0.0.1', 'port': 11,
                       'replication_ip': '127.0.0.2', 'replication_port': 12,
                       'region': '1', 'zone': '2', 'device': 'sda1',
                       'meta': 'just_a_string'}

        self.assertEqual(
            utils.rsync_module_interpolation('{ip}', fake_device),
            '127.0.0.1')
        self.assertEqual(
            utils.rsync_module_interpolation('{port}', fake_device),
            '11')
        self.assertEqual(
            utils.rsync_module_interpolation('{replication_ip}', fake_device),
            '127.0.0.2')
        self.assertEqual(
            utils.rsync_module_interpolation('{replication_port}',
                                             fake_device),
            '12')
        self.assertEqual(
            utils.rsync_module_interpolation('{region}', fake_device),
            '1')
        self.assertEqual(
            utils.rsync_module_interpolation('{zone}', fake_device),
            '2')
        self.assertEqual(
            utils.rsync_module_interpolation('{device}', fake_device),
            'sda1')
        self.assertEqual(
            utils.rsync_module_interpolation('{meta}', fake_device),
            'just_a_string')

        self.assertEqual(
            utils.rsync_module_interpolation('{replication_ip}::object',
                                             fake_device),
            '127.0.0.2::object')
        self.assertEqual(
            utils.rsync_module_interpolation('{ip}::container{port}',
                                             fake_device),
            '127.0.0.1::container11')
        self.assertEqual(
            utils.rsync_module_interpolation(
                '{replication_ip}::object_{device}', fake_device),
            '127.0.0.2::object_sda1')
        self.assertEqual(
            utils.rsync_module_interpolation(
                '127.0.0.3::object_{replication_port}', fake_device),
            '127.0.0.3::object_12')

        self.assertRaises(ValueError, utils.rsync_module_interpolation,
                          '{replication_ip}::object_{deivce}', fake_device)

    def test_fallocate_reserve(self):

        class StatVFS(object):
            f_frsize = 1024
            f_bavail = 1
            f_blocks = 100

        def fstatvfs(fd):
            return StatVFS()

        orig_FALLOCATE_RESERVE = utils.FALLOCATE_RESERVE
        orig_fstatvfs = utils.os.fstatvfs
        try:
            fallocate = utils.FallocateWrapper(noop=True)
            utils.os.fstatvfs = fstatvfs

            # Make sure setting noop, which disables fallocate, also stops the
            # fallocate_reserve check.
            # Set the fallocate_reserve to 99% and request an object that is
            # about 50% the size. With fallocate_reserve off this will succeed.
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('99%')
            self.assertEqual(fallocate(0, 1, 0, ctypes.c_uint64(500)), 0)

            # Setting noop to False after the constructor allows us to use
            # a noop fallocate syscall and still test fallocate_reserve.
            fallocate.noop = False

            # Want 1023 reserved, have 1024 * 1 free, so succeeds
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1023')
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            self.assertEqual(fallocate(0, 1, 0, ctypes.c_uint64(0)), 0)
            # Want 1023 reserved, have 512 * 2 free, so succeeds
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1023')
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            self.assertEqual(fallocate(0, 1, 0, ctypes.c_uint64(0)), 0)
            # Want 1024 reserved, have 1024 * 1 free, so fails
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1024')
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1

            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 1024'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # Want 1024 reserved, have 512 * 2 free, so fails
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1024')
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 1024'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # Want 2048 reserved, have 1024 * 1 free, so fails
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('2048')
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 2048'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # Want 2048 reserved, have 512 * 2 free, so fails
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('2048')
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 2048'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # Want 1023 reserved, have 1024 * 1 free, but file size is 1, so
            # fails
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1023')
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(1))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 1023 <= 1023'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # Want 1022 reserved, have 1024 * 1 free, and file size is 1, so
            # succeeds
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1022')
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            self.assertEqual(fallocate(0, 1, 0, ctypes.c_uint64(1)), 0)

            # Want 1% reserved, have 100 bytes * 2/100 free, and file size is
            # 99, so succeeds
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1%')
            StatVFS.f_frsize = 100
            StatVFS.f_bavail = 2
            StatVFS.f_blocks = 100
            self.assertEqual(fallocate(0, 1, 0, ctypes.c_uint64(99)), 0)

            # Want 2% reserved, have 50 bytes * 2/50 free, and file size is 49,
            # so succeeds
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('2%')
            StatVFS.f_frsize = 50
            StatVFS.f_bavail = 2
            StatVFS.f_blocks = 50
            self.assertEqual(fallocate(0, 1, 0, ctypes.c_uint64(49)), 0)

            # Want 100% reserved, have  100 * 100/100 free, and file size is 0,
            # so fails.
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('100%')
            StatVFS.f_frsize = 100
            StatVFS.f_bavail = 100
            StatVFS.f_blocks = 100
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 100.0 <= 100.0'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # Want 1% reserved, have 100 * 2/100 free, and file size is 101,
            # so fails.
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('1%')
            StatVFS.f_frsize = 100
            StatVFS.f_bavail = 2
            StatVFS.f_blocks = 100
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(101))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 0.99 <= 1.0'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # is 100, so fails
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('98%')
            StatVFS.f_frsize = 100
            StatVFS.f_bavail = 99
            StatVFS.f_blocks = 100
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(100))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 98.0 <= 98.0'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

            # Want 2% reserved, have 1000 bytes * 21/1000 free, and file size
            # is 999, so succeeds.
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('2%')
            StatVFS.f_frsize = 1000
            StatVFS.f_bavail = 21
            StatVFS.f_blocks = 1000
            self.assertEqual(fallocate(0, 1, 0, ctypes.c_uint64(999)), 0)

            # Want 2% resereved, have 1000 bytes * 21/1000 free, and file size
            # is 1000, so fails.
            utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
                utils.config_fallocate_value('2%')
            StatVFS.f_frsize = 1000
            StatVFS.f_bavail = 21
            StatVFS.f_blocks = 1000
            with self.assertRaises(OSError) as catcher:
                fallocate(0, 1, 0, ctypes.c_uint64(1000))
            self.assertEqual(
                str(catcher.exception),
                '[Errno %d] FALLOCATE_RESERVE fail 2.0 <= 2.0'
                % errno.ENOSPC)
            self.assertEqual(catcher.exception.errno, errno.ENOSPC)

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
            self.assertEqual(utils._sys_fallocate.last_call,
                             [1234, 1, 0, 0])
            # Ensure fallocate calls _sys_fallocate even with negative bytes
            utils._sys_fallocate.last_call = None
            utils.fallocate(1234, -5678)
            self.assertEqual(utils._sys_fallocate.last_call,
                             [1234, 1, 0, 0])
            # Ensure fallocate calls _sys_fallocate properly with positive
            # bytes
            utils._sys_fallocate.last_call = None
            utils.fallocate(1234, 1)
            self.assertEqual(utils._sys_fallocate.last_call,
                             [1234, 1, 0, 1])
            utils._sys_fallocate.last_call = None
            utils.fallocate(1234, 10 * 1024 * 1024 * 1024)
            self.assertEqual(utils._sys_fallocate.last_call,
                             [1234, 1, 0, 10 * 1024 * 1024 * 1024])
        finally:
            utils._sys_fallocate = orig__sys_fallocate

    def test_generate_trans_id(self):
        fake_time = 1366428370.5163341
        with patch.object(utils.time, 'time', return_value=fake_time):
            trans_id = utils.generate_trans_id('')
            self.assertEqual(len(trans_id), 34)
            self.assertEqual(trans_id[:2], 'tx')
            self.assertEqual(trans_id[23], '-')
            self.assertEqual(int(trans_id[24:], 16), int(fake_time))
        with patch.object(utils.time, 'time', return_value=fake_time):
            trans_id = utils.generate_trans_id('-suffix')
            self.assertEqual(len(trans_id), 41)
            self.assertEqual(trans_id[:2], 'tx')
            self.assertEqual(trans_id[34:], '-suffix')
            self.assertEqual(trans_id[23], '-')
            self.assertEqual(int(trans_id[24:34], 16), int(fake_time))

    def test_get_trans_id_time(self):
        ts = utils.get_trans_id_time('tx8c8bc884cdaf499bb29429aa9c46946e')
        self.assertEqual(ts, None)
        ts = utils.get_trans_id_time('tx1df4ff4f55ea45f7b2ec2-0051720c06')
        self.assertEqual(ts, 1366428678)
        self.assertEqual(
            time.asctime(time.gmtime(ts)) + ' UTC',
            'Sat Apr 20 03:31:18 2013 UTC')
        ts = utils.get_trans_id_time(
            'tx1df4ff4f55ea45f7b2ec2-0051720c06-suffix')
        self.assertEqual(ts, 1366428678)
        self.assertEqual(
            time.asctime(time.gmtime(ts)) + ' UTC',
            'Sat Apr 20 03:31:18 2013 UTC')
        ts = utils.get_trans_id_time('')
        self.assertEqual(ts, None)
        ts = utils.get_trans_id_time('garbage')
        self.assertEqual(ts, None)
        ts = utils.get_trans_id_time('tx1df4ff4f55ea45f7b2ec2-almostright')
        self.assertEqual(ts, None)

    def test_config_fallocate_value(self):
        fallocate_value, is_percent = utils.config_fallocate_value('10%')
        self.assertEqual(fallocate_value, 10)
        self.assertTrue(is_percent)
        fallocate_value, is_percent = utils.config_fallocate_value('10')
        self.assertEqual(fallocate_value, 10)
        self.assertFalse(is_percent)
        try:
            fallocate_value, is_percent = utils.config_fallocate_value('ab%')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: ab% is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = utils.config_fallocate_value('ab')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: ab is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = utils.config_fallocate_value('1%%')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: 1%% is an invalid value for '
                                   'fallocate_reserve.')
        try:
            fallocate_value, is_percent = utils.config_fallocate_value('10.0')
        except ValueError as err:
            exc = err
        self.assertEqual(str(exc), 'Error: 10.0 is an invalid value for '
                                   'fallocate_reserve.')
        fallocate_value, is_percent = utils.config_fallocate_value('10.5%')
        self.assertEqual(fallocate_value, 10.5)
        self.assertTrue(is_percent)
        fallocate_value, is_percent = utils.config_fallocate_value('10.000%')
        self.assertEqual(fallocate_value, 10.000)
        self.assertTrue(is_percent)

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
                f.seek(0)
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
            self.assertTrue(timedout)
            self.assertTrue(os.path.exists(nt.name))

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
        self.assertEqual(utils.parse_content_type('text/plain'),
                         ('text/plain', []))
        self.assertEqual(utils.parse_content_type('text/plain;charset=utf-8'),
                         ('text/plain', [('charset', 'utf-8')]))
        self.assertEqual(
            utils.parse_content_type('text/plain;hello="world";charset=utf-8'),
            ('text/plain', [('hello', '"world"'), ('charset', 'utf-8')]))
        self.assertEqual(
            utils.parse_content_type('text/plain; hello="world"; a=b'),
            ('text/plain', [('hello', '"world"'), ('a', 'b')]))
        self.assertEqual(
            utils.parse_content_type(r'text/plain; x="\""; a=b'),
            ('text/plain', [('x', r'"\""'), ('a', 'b')]))
        self.assertEqual(
            utils.parse_content_type(r'text/plain; x; a=b'),
            ('text/plain', [('x', ''), ('a', 'b')]))
        self.assertEqual(
            utils.parse_content_type(r'text/plain; x="\""; a'),
            ('text/plain', [('x', r'"\""'), ('a', '')]))

    def test_override_bytes_from_content_type(self):
        listing_dict = {
            'bytes': 1234, 'hash': 'asdf', 'name': 'zxcv',
            'content_type': 'text/plain; hello="world"; swift_bytes=15'}
        utils.override_bytes_from_content_type(listing_dict,
                                               logger=FakeLogger())
        self.assertEqual(listing_dict['bytes'], 15)
        self.assertEqual(listing_dict['content_type'],
                         'text/plain;hello="world"')

        listing_dict = {
            'bytes': 1234, 'hash': 'asdf', 'name': 'zxcv',
            'content_type': 'text/plain; hello="world"; swift_bytes=hey'}
        utils.override_bytes_from_content_type(listing_dict,
                                               logger=FakeLogger())
        self.assertEqual(listing_dict['bytes'], 1234)
        self.assertEqual(listing_dict['content_type'],
                         'text/plain;hello="world"')

    def test_extract_swift_bytes(self):
        scenarios = {
            # maps input value -> expected returned tuple
            '': ('', None),
            'text/plain': ('text/plain', None),
            'text/plain; other=thing': ('text/plain;other=thing', None),
            'text/plain; swift_bytes=123': ('text/plain', '123'),
            'text/plain; other=thing;swift_bytes=123':
                ('text/plain;other=thing', '123'),
            'text/plain; swift_bytes=123; other=thing':
                ('text/plain;other=thing', '123'),
            'text/plain; swift_bytes=123; swift_bytes=456':
                ('text/plain', '456'),
            'text/plain; swift_bytes=123; other=thing;swift_bytes=456':
                ('text/plain;other=thing', '456')}
        for test_value, expected in scenarios.items():
            self.assertEqual(expected, utils.extract_swift_bytes(test_value))

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
        self.assertEqual('abc_%EC%9D%BC%EC%98%81',
                         utils.quote(valid_utf8_str))
        self.assertEqual('abc_%EF%BF%BD%EF%BF%BD%EC%BC%9D%EF%BF%BD',
                         utils.quote(invalid_utf8_str))

    def test_get_hmac(self):
        self.assertEqual(
            utils.get_hmac('GET', '/path', 1, 'abc'),
            'b17f6ff8da0e251737aa9e3ee69a881e3e092e2f')

    def test_get_policy_index(self):
        # Account has no information about a policy
        req = Request.blank(
            '/sda1/p/a',
            environ={'REQUEST_METHOD': 'GET'})
        res = Response()
        self.assertIsNone(utils.get_policy_index(req.headers,
                                                 res.headers))

        # The policy of a container can be specified by the response header
        req = Request.blank(
            '/sda1/p/a/c',
            environ={'REQUEST_METHOD': 'GET'})
        res = Response(headers={'X-Backend-Storage-Policy-Index': '1'})
        self.assertEqual('1', utils.get_policy_index(req.headers,
                                                     res.headers))

        # The policy of an object to be created can be specified by the request
        # header
        req = Request.blank(
            '/sda1/p/a/c/o',
            environ={'REQUEST_METHOD': 'PUT'},
            headers={'X-Backend-Storage-Policy-Index': '2'})
        res = Response()
        self.assertEqual('2', utils.get_policy_index(req.headers,
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
                self.assertEqual(
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
            self.assertIsNone(utils.cache_from_env(env))
            self.assertTrue(err_msg in logger.get_lines_for_level('error'))
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertIsNone(utils.cache_from_env(env, False))
            self.assertTrue(err_msg in logger.get_lines_for_level('error'))
        logger = FakeLogger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertIsNone(utils.cache_from_env(env, True))
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

    def test_modify_priority(self):
        pid = os.getpid()
        logger = debug_logger()
        called = {}

        def _fake_setpriority(*args):
            called['setpriority'] = args

        def _fake_syscall(*args):
            called['syscall'] = args

        with patch('swift.common.utils._libc_setpriority',
                   _fake_setpriority), \
                patch('swift.common.utils._posix_syscall', _fake_syscall):
            called = {}
            # not set / default
            utils.modify_priority({}, logger)
            self.assertEqual(called, {})
            called = {}
            # just nice
            utils.modify_priority({'nice_priority': '1'}, logger)
            self.assertEqual(called, {'setpriority': (0, pid, 1)})
            called = {}
            # just ionice class uses default priority 0
            utils.modify_priority({'ionice_class': 'IOPRIO_CLASS_RT'}, logger)
            self.assertEqual(called, {'syscall': (251, 1, pid, 1 << 13)})
            called = {}
            # just ionice priority is ignored
            utils.modify_priority({'ionice_priority': '4'}, logger)
            self.assertEqual(called, {})
            called = {}
            # bad ionice class
            utils.modify_priority({'ionice_class': 'class_foo'}, logger)
            self.assertEqual(called, {})
            called = {}
            # ionice class & priority
            utils.modify_priority({
                'ionice_class': 'IOPRIO_CLASS_BE',
                'ionice_priority': '4',
            }, logger)
            self.assertEqual(called, {'syscall': (251, 1, pid, 2 << 13 | 4)})
            called = {}
            # all
            utils.modify_priority({
                'nice_priority': '-15',
                'ionice_class': 'IOPRIO_CLASS_IDLE',
                'ionice_priority': '6',
            }, logger)
            self.assertEqual(called, {
                'setpriority': (0, pid, -15),
                'syscall': (251, 1, pid, 3 << 13 | 6),
            })

    def test__NR_ioprio_set(self):
        with patch('os.uname', return_value=('', '', '', '', 'x86_64')), \
                patch('platform.architecture', return_value=('64bit', '')):
            self.assertEqual(251, utils.NR_ioprio_set())

        with patch('os.uname', return_value=('', '', '', '', 'x86_64')), \
                patch('platform.architecture', return_value=('32bit', '')):
            self.assertRaises(OSError, utils.NR_ioprio_set)

        with patch('os.uname', return_value=('', '', '', '', 'alpha')), \
                patch('platform.architecture', return_value=('64bit', '')):
            self.assertRaises(OSError, utils.NR_ioprio_set)

    @requires_o_tmpfile_support
    def test_link_fd_to_path_linkat_success(self):
        tempdir = mkdtemp(dir='/tmp')
        fd = os.open(tempdir, utils.O_TMPFILE | os.O_WRONLY)
        data = "I'm whatever Gotham needs me to be"
        _m_fsync_dir = mock.Mock()
        try:
            os.write(fd, data)
            # fd is O_WRONLY
            self.assertRaises(OSError, os.read, fd, 1)
            file_path = os.path.join(tempdir, uuid4().hex)
            with mock.patch('swift.common.utils.fsync_dir', _m_fsync_dir):
                    utils.link_fd_to_path(fd, file_path, 1)
            with open(file_path, 'r') as f:
                self.assertEqual(f.read(), data)
            self.assertEqual(_m_fsync_dir.call_count, 2)
        finally:
            os.close(fd)
            shutil.rmtree(tempdir)

    @requires_o_tmpfile_support
    def test_link_fd_to_path_target_exists(self):
        tempdir = mkdtemp(dir='/tmp')
        # Create and write to a file
        fd, path = tempfile.mkstemp(dir=tempdir)
        os.write(fd, "hello world")
        os.fsync(fd)
        os.close(fd)
        self.assertTrue(os.path.exists(path))

        fd = os.open(tempdir, utils.O_TMPFILE | os.O_WRONLY)
        try:
            os.write(fd, "bye world")
            os.fsync(fd)
            utils.link_fd_to_path(fd, path, 0, fsync=False)
            # Original file now should have been over-written
            with open(path, 'r') as f:
                self.assertEqual(f.read(), "bye world")
        finally:
            os.close(fd)
            shutil.rmtree(tempdir)

    @requires_o_tmpfile_support
    def test_link_fd_to_path_errno_not_EEXIST_or_ENOENT(self):
        _m_linkat = mock.Mock(
            side_effect=IOError(errno.EACCES, os.strerror(errno.EACCES)))
        with mock.patch('swift.common.utils.linkat', _m_linkat):
            try:
                utils.link_fd_to_path(0, '/path', 1)
            except IOError as err:
                self.assertEqual(err.errno, errno.EACCES)
            else:
                self.fail("Expecting IOError exception")
        self.assertTrue(_m_linkat.called)

    @requires_o_tmpfile_support
    def test_linkat_race_dir_not_exists(self):
        tempdir = mkdtemp(dir='/tmp')
        target_dir = os.path.join(tempdir, uuid4().hex)
        target_path = os.path.join(target_dir, uuid4().hex)
        os.mkdir(target_dir)
        fd = os.open(target_dir, utils.O_TMPFILE | os.O_WRONLY)
        # Simulating directory deletion by other backend process
        os.rmdir(target_dir)
        self.assertFalse(os.path.exists(target_dir))
        try:
            utils.link_fd_to_path(fd, target_path, 1)
            self.assertTrue(os.path.exists(target_dir))
            self.assertTrue(os.path.exists(target_path))
        finally:
            os.close(fd)
            shutil.rmtree(tempdir)

    def test_safe_json_loads(self):
        expectations = {
            None: None,
            '': None,
            0: None,
            1: None,
            '"asdf"': 'asdf',
            '[]': [],
            '{}': {},
            "{'foo': 'bar'}": None,
            '{"foo": "bar"}': {'foo': 'bar'},
        }

        failures = []
        for value, expected in expectations.items():
            try:
                result = utils.safe_json_loads(value)
            except Exception as e:
                # it's called safe, if it blows up the test blows up
                self.fail('%r caused safe method to throw %r!' % (
                    value, e))
            try:
                self.assertEqual(expected, result)
            except AssertionError:
                failures.append('%r => %r (expected %r)' % (
                    value, result, expected))
        if failures:
            self.fail('Invalid results from pure function:\n%s' %
                      '\n'.join(failures))


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

        self.assertEqual(set(['role1', 'role2']),
                         set(options['PRE1_'].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options['PRE1_'].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options['PRE1_'].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

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

        self.assertEqual(set(['role1', 'role2']),
                         set(options[''].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options[''].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options[''].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

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
        self.assertEqual(set(['admin', 'swiftoperator']),
                         set(options['AUTH_'].get('operator_roles')))
        self.assertEqual(set(['role1', 'role2']),
                         set(options[''].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual([],
                         options['AUTH_'].get('service_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options[''].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options['AUTH_'].get('require_group'))
        self.assertEqual('', options[''].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

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
        self.assertEqual(set(['admin', 'swiftoperator']),
                         set(options['AUTH_'].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual([],
                         options['AUTH_'].get('service_roles'))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('', options['AUTH_'].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))

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
        self.assertEqual(set(['role1', 'role2']),
                         set(options['AUTH_'].get('operator_roles')))
        self.assertEqual(set(['role1', 'role2']),
                         set(options[''].get('operator_roles')))
        self.assertEqual(['role5'],
                         options['PRE2_'].get('operator_roles'))
        self.assertEqual(set(['role3', 'role4']),
                         set(options['AUTH_'].get('service_roles')))
        self.assertEqual(set(['role3', 'role4']),
                         set(options[''].get('service_roles')))
        self.assertEqual(['role6'], options['PRE2_'].get('service_roles'))
        self.assertEqual('auth_blank_group',
                         options['AUTH_'].get('require_group'))
        self.assertEqual('auth_blank_group', options[''].get('require_group'))
        self.assertEqual('pre2_group', options['PRE2_'].get('require_group'))


class TestUnlinkOlder(unittest.TestCase):

    def setUp(self):
        self.tempdir = mkdtemp()
        self.mtime = {}

    def tearDown(self):
        rmtree(self.tempdir, ignore_errors=True)

    def touch(self, fpath, mtime=None):
        self.mtime[fpath] = mtime or time.time()
        open(fpath, 'w')

    @contextlib.contextmanager
    def high_resolution_getmtime(self):
        orig_getmtime = os.path.getmtime

        def mock_getmtime(fpath):
            mtime = self.mtime.get(fpath)
            if mtime is None:
                mtime = orig_getmtime(fpath)
            return mtime

        with mock.patch('os.path.getmtime', mock_getmtime):
            yield

    def test_unlink_older_than_path_not_exists(self):
        path = os.path.join(self.tempdir, 'does-not-exist')
        # just make sure it doesn't blow up
        utils.unlink_older_than(path, time.time())

    def test_unlink_older_than_file(self):
        path = os.path.join(self.tempdir, 'some-file')
        self.touch(path)
        with self.assertRaises(OSError) as ctx:
            utils.unlink_older_than(path, time.time())
        self.assertEqual(ctx.exception.errno, errno.ENOTDIR)

    def test_unlink_older_than_now(self):
        self.touch(os.path.join(self.tempdir, 'test'))
        with self.high_resolution_getmtime():
            utils.unlink_older_than(self.tempdir, time.time())
        self.assertEqual([], os.listdir(self.tempdir))

    def test_unlink_not_old_enough(self):
        start = time.time()
        self.touch(os.path.join(self.tempdir, 'test'))
        with self.high_resolution_getmtime():
            utils.unlink_older_than(self.tempdir, start)
        self.assertEqual(['test'], os.listdir(self.tempdir))

    def test_unlink_mixed(self):
        self.touch(os.path.join(self.tempdir, 'first'))
        cutoff = time.time()
        self.touch(os.path.join(self.tempdir, 'second'))
        with self.high_resolution_getmtime():
            utils.unlink_older_than(self.tempdir, cutoff)
        self.assertEqual(['second'], os.listdir(self.tempdir))

    def test_unlink_paths(self):
        paths = []
        for item in ('first', 'second', 'third'):
            path = os.path.join(self.tempdir, item)
            self.touch(path)
            paths.append(path)
        # don't unlink everyone
        with self.high_resolution_getmtime():
            utils.unlink_paths_older_than(paths[:2], time.time())
        self.assertEqual(['third'], os.listdir(self.tempdir))

    def test_unlink_empty_paths(self):
        # just make sure it doesn't blow up
        utils.unlink_paths_older_than([], time.time())

    def test_unlink_not_exists_paths(self):
        path = os.path.join(self.tempdir, 'does-not-exist')
        # just make sure it doesn't blow up
        utils.unlink_paths_older_than([path], time.time())


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
        self.assertEqual(info['cap1']['cap1_moo'], 'cap1_baa')
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
        self.assertEqual(info['cap1'], cap1)


class TestFileLikeIter(unittest.TestCase):

    def test_iter_file_iter(self):
        in_iter = [b'abc', b'de', b'fghijk', b'l']
        chunks = []
        for chunk in utils.FileLikeIter(in_iter):
            chunks.append(chunk)
        self.assertEqual(chunks, in_iter)

    def test_next(self):
        in_iter = [b'abc', b'de', b'fghijk', b'l']
        chunks = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            try:
                chunk = next(iter_file)
            except StopIteration:
                break
            chunks.append(chunk)
        self.assertEqual(chunks, in_iter)

    def test_read(self):
        in_iter = [b'abc', b'de', b'fghijk', b'l']
        iter_file = utils.FileLikeIter(in_iter)
        self.assertEqual(iter_file.read(), b''.join(in_iter))

    def test_read_with_size(self):
        in_iter = [b'abc', b'de', b'fghijk', b'l']
        chunks = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            chunk = iter_file.read(2)
            if not chunk:
                break
            self.assertTrue(len(chunk) <= 2)
            chunks.append(chunk)
        self.assertEqual(b''.join(chunks), b''.join(in_iter))

    def test_read_with_size_zero(self):
        # makes little sense, but file supports it, so...
        self.assertEqual(utils.FileLikeIter(b'abc').read(0), b'')

    def test_readline(self):
        in_iter = [b'abc\n', b'd', b'\nef', b'g\nh', b'\nij\n\nk\n',
                   b'trailing.']
        lines = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            line = iter_file.readline()
            if not line:
                break
            lines.append(line)
        self.assertEqual(
            lines,
            [v if v == b'trailing.' else v + b'\n'
             for v in b''.join(in_iter).split(b'\n')])

    def test_readline2(self):
        self.assertEqual(
            utils.FileLikeIter([b'abc', b'def\n']).readline(4),
            b'abcd')

    def test_readline3(self):
        self.assertEqual(
            utils.FileLikeIter([b'a' * 1111, b'bc\ndef']).readline(),
            (b'a' * 1111) + b'bc\n')

    def test_readline_with_size(self):

        in_iter = [b'abc\n', b'd', b'\nef', b'g\nh', b'\nij\n\nk\n',
                   b'trailing.']
        lines = []
        iter_file = utils.FileLikeIter(in_iter)
        while True:
            line = iter_file.readline(2)
            if not line:
                break
            lines.append(line)
        self.assertEqual(
            lines,
            [b'ab', b'c\n', b'd\n', b'ef', b'g\n', b'h\n', b'ij', b'\n', b'\n',
             b'k\n', b'tr', b'ai', b'li', b'ng', b'.'])

    def test_readlines(self):
        in_iter = [b'abc\n', b'd', b'\nef', b'g\nh', b'\nij\n\nk\n',
                   b'trailing.']
        lines = utils.FileLikeIter(in_iter).readlines()
        self.assertEqual(
            lines,
            [v if v == b'trailing.' else v + b'\n'
             for v in b''.join(in_iter).split(b'\n')])

    def test_readlines_with_size(self):
        in_iter = [b'abc\n', b'd', b'\nef', b'g\nh', b'\nij\n\nk\n',
                   b'trailing.']
        iter_file = utils.FileLikeIter(in_iter)
        lists_of_lines = []
        while True:
            lines = iter_file.readlines(2)
            if not lines:
                break
            lists_of_lines.append(lines)
        self.assertEqual(
            lists_of_lines,
            [[b'ab'], [b'c\n'], [b'd\n'], [b'ef'], [b'g\n'], [b'h\n'], [b'ij'],
             [b'\n', b'\n'], [b'k\n'], [b'tr'], [b'ai'], [b'li'], [b'ng'],
             [b'.']])

    def test_close(self):
        iter_file = utils.FileLikeIter([b'a', b'b', b'c'])
        self.assertEqual(next(iter_file), b'a')
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
    def setUp(self):

        def fake_getaddrinfo(host, port, *args):
            # this is what a real getaddrinfo('localhost', port,
            # socket.AF_INET) returned once
            return [(socket.AF_INET,      # address family
                     socket.SOCK_STREAM,  # socket type
                     socket.IPPROTO_TCP,  # socket protocol
                     '',                  # canonical name,
                     ('127.0.0.1', port)),  # socket address
                    (socket.AF_INET,
                     socket.SOCK_DGRAM,
                     socket.IPPROTO_UDP,
                     '',
                     ('127.0.0.1', port))]

        self.real_getaddrinfo = utils.socket.getaddrinfo
        self.getaddrinfo_patcher = mock.patch.object(
            utils.socket, 'getaddrinfo', fake_getaddrinfo)
        self.mock_getaddrinfo = self.getaddrinfo_patcher.start()
        self.addCleanup(self.getaddrinfo_patcher.stop)

    def test_get_logger_statsd_client_not_specified(self):
        logger = utils.get_logger({}, 'some-name', log_route='some-route')
        # white-box construction validation
        self.assertIsNone(logger.logger.statsd_client)

    def test_get_logger_statsd_client_defaults(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com'},
                                  'some-name', log_route='some-route')
        # white-box construction validation
        self.assertTrue(isinstance(logger.logger.statsd_client,
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

    def test_ipv4_or_ipv6_hostname_defaults_to_ipv4(self):
        def stub_getaddrinfo_both_ipv4_and_ipv6(host, port, family, *rest):
            if family == socket.AF_INET:
                return [(socket.AF_INET, 'blah', 'blah', 'blah',
                        ('127.0.0.1', int(port)))]
            elif family == socket.AF_INET6:
                # Implemented so an incorrectly ordered implementation (IPv6
                # then IPv4) would realistically fail.
                return [(socket.AF_INET6, 'blah', 'blah', 'blah',
                        ('::1', int(port), 0, 0))]

        with mock.patch.object(utils.socket, 'getaddrinfo',
                               new=stub_getaddrinfo_both_ipv4_and_ipv6):
            logger = utils.get_logger({
                'log_statsd_host': 'localhost',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        statsd_client = logger.logger.statsd_client

        self.assertEqual(statsd_client._sock_family, socket.AF_INET)
        self.assertEqual(statsd_client._target, ('localhost', 9876))

        got_sock = statsd_client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET)

    def test_ipv4_instantiation_and_socket_creation(self):
        logger = utils.get_logger({
            'log_statsd_host': '127.0.0.1',
            'log_statsd_port': '9876',
        }, 'some-name', log_route='some-route')
        statsd_client = logger.logger.statsd_client

        self.assertEqual(statsd_client._sock_family, socket.AF_INET)
        self.assertEqual(statsd_client._target, ('127.0.0.1', 9876))

        got_sock = statsd_client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET)

    def test_ipv6_instantiation_and_socket_creation(self):
        # We have to check the given hostname or IP for IPv4/IPv6 on logger
        # instantiation so we don't call getaddrinfo() too often and don't have
        # to call bind() on our socket to detect IPv4/IPv6 on every send.
        #
        # This test uses the real getaddrinfo, so we patch over the mock to
        # put the real one back. If we just stop the mock, then
        # unittest.exit() blows up, but stacking real-fake-real works okay.
        with mock.patch.object(utils.socket, 'getaddrinfo',
                               self.real_getaddrinfo):
            logger = utils.get_logger({
                'log_statsd_host': '::1',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        statsd_client = logger.logger.statsd_client

        self.assertEqual(statsd_client._sock_family, socket.AF_INET6)
        self.assertEqual(statsd_client._target, ('::1', 9876, 0, 0))

        got_sock = statsd_client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET6)

    def test_bad_hostname_instantiation(self):
        with mock.patch.object(utils.socket, 'getaddrinfo',
                               side_effect=utils.socket.gaierror("whoops")):
            logger = utils.get_logger({
                'log_statsd_host': 'i-am-not-a-hostname-or-ip',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        statsd_client = logger.logger.statsd_client

        self.assertEqual(statsd_client._sock_family, socket.AF_INET)
        self.assertEqual(statsd_client._target,
                         ('i-am-not-a-hostname-or-ip', 9876))

        got_sock = statsd_client._open_socket()
        self.assertEqual(got_sock.family, socket.AF_INET)
        # Maybe the DNS server gets fixed in a bit and it starts working... or
        # maybe the DNS record hadn't propagated yet.  In any case, failed
        # statsd sends will warn in the logs until the DNS failure or invalid
        # IP address in the configuration is fixed.

    def test_sending_ipv6(self):
        def fake_getaddrinfo(host, port, *args):
            # this is what a real getaddrinfo('::1', port,
            # socket.AF_INET6) returned once
            return [(socket.AF_INET6,
                     socket.SOCK_STREAM,
                     socket.IPPROTO_TCP,
                     '', ('::1', port, 0, 0)),
                    (socket.AF_INET6,
                     socket.SOCK_DGRAM,
                     socket.IPPROTO_UDP,
                     '',
                     ('::1', port, 0, 0))]

        with mock.patch.object(utils.socket, 'getaddrinfo', fake_getaddrinfo):
            logger = utils.get_logger({
                'log_statsd_host': '::1',
                'log_statsd_port': '9876',
            }, 'some-name', log_route='some-route')
        statsd_client = logger.logger.statsd_client

        fl = FakeLogger()
        statsd_client.logger = fl
        mock_socket = MockUdpSocket()

        statsd_client._open_socket = lambda *_: mock_socket
        logger.increment('tunafish')
        self.assertEqual(fl.get_lines_for_level('warning'), [])
        self.assertEqual(mock_socket.sent,
                         [(b'some-name.tunafish:1|c', ('::1', 9876, 0, 0))])

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
        self.assertTrue(payload.endswith(b"|@0.5"))

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
        suffix = "|@%s" % effective_sample_rate
        if six.PY3:
            suffix = suffix.encode('utf-8')
        self.assertTrue(payload.endswith(suffix), payload)

        effective_sample_rate = 0.587 * 0.91
        statsd_client.random = lambda: effective_sample_rate - 0.001
        logger.increment('tribbles', sample_rate=0.587)
        self.assertEqual(len(mock_socket.sent), 2)

        payload = mock_socket.sent[1][0]
        suffix = "|@%s" % effective_sample_rate
        if six.PY3:
            suffix = suffix.encode('utf-8')
        self.assertTrue(payload.endswith(suffix), payload)

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
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(404)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(412)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(416)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.timing')
        self.assertTrue(mock_controller.args[1] > 0)

        mock_controller = MockController(401)
        METHOD(mock_controller)
        self.assertEqual(len(mock_controller.args), 2)
        self.assertEqual(mock_controller.called, 'timing')
        self.assertEqual(mock_controller.args[0], 'METHOD.errors.timing')
        self.assertTrue(mock_controller.args[1] > 0)


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
    __next__ = next


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
        self.assertTrue(callable(keyfn))
        ids = [n['id'] for n in sorted(self.nodes, key=keyfn)]
        self.assertEqual([0, 1, 2, 3, 4, 5, 6, 7], ids)

    def test_all_whitespace_value(self):
        # Empty's okay, it just means no preference
        keyfn = utils.affinity_key_function("  \n")
        self.assertTrue(callable(keyfn))
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
        self.assertTrue(pred is None)

    def test_region(self):
        pred = utils.affinity_locality_predicate('r1')
        self.assertTrue(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0, 1], ids)

    def test_zone(self):
        pred = utils.affinity_locality_predicate('r1z1')
        self.assertTrue(callable(pred))
        ids = [n['id'] for n in self.nodes if pred(n)]
        self.assertEqual([0], ids)

    def test_multiple(self):
        pred = utils.affinity_locality_predicate('r1, r3, r4z0')
        self.assertTrue(callable(pred))
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

        with patch('time.time', my_time), \
                patch('eventlet.sleep', my_sleep):
            return func(*args, **kwargs)

    def test_rate_limiting(self):

        def testfunc():
            limited_iterator = utils.RateLimitedIterator(range(9999), 100)
            got = []
            started_at = time.time()
            try:
                while time.time() - started_at < 0.1:
                    got.append(next(limited_iterator))
            except StopIteration:
                pass
            return got

        got = self.run_under_pseudo_time(testfunc)
        # it's 11, not 10, because ratelimiting doesn't apply to the very
        # first element.
        self.assertEqual(len(got), 11)

    def test_rate_limiting_sometimes(self):

        def testfunc():
            limited_iterator = utils.RateLimitedIterator(
                range(9999), 100,
                ratelimit_if=lambda item: item % 23 != 0)
            got = []
            started_at = time.time()
            try:
                while time.time() - started_at < 0.5:
                    got.append(next(limited_iterator))
            except StopIteration:
                pass
            return got

        got = self.run_under_pseudo_time(testfunc)
        # we'd get 51 without the ratelimit_if, but because 0, 23 and 46
        # weren't subject to ratelimiting, we get 54 instead
        self.assertEqual(len(got), 54)

    def test_limit_after(self):

        def testfunc():
            limited_iterator = utils.RateLimitedIterator(
                range(9999), 100, limit_after=5)
            got = []
            started_at = time.time()
            try:
                while time.time() - started_at < 0.1:
                    got.append(next(limited_iterator))
            except StopIteration:
                pass
            return got

        got = self.run_under_pseudo_time(testfunc)
        # it's 16, not 15, because ratelimiting doesn't apply to the very
        # first element.
        self.assertEqual(len(got), 16)


class TestGreenthreadSafeIterator(unittest.TestCase):

    def increment(self, iterable):
        plus_ones = []
        for n in iterable:
            plus_ones.append(n + 1)
        return plus_ones

    def test_setup_works(self):
        # it should work without concurrent access
        self.assertEqual([0, 1, 2, 3], list(UnsafeXrange(4)))

        iterable = UnsafeXrange(10)
        pile = eventlet.GreenPile(2)
        for _ in range(2):
            pile.spawn(self.increment, iterable)

        sorted([resp for resp in pile])
        self.assertTrue(
            iterable.concurrent_call, 'test setup is insufficiently crazy')

    def test_access_is_serialized(self):
        pile = eventlet.GreenPile(2)
        unsafe_iterable = UnsafeXrange(10)
        iterable = utils.GreenthreadSafeIterator(unsafe_iterable)
        for _ in range(2):
            pile.spawn(self.increment, iterable)
        response = sorted(sum([resp for resp in pile], []))
        self.assertEqual(list(range(1, 11)), response)
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
                if payload and b'STOP' in payload:
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
        if six.PY3:
            got = got.decode('utf-8')
        return self.assertEqual(expected, got)

    def assertStatMatches(self, expected_regexp, sender_fn, *args, **kwargs):
        got = self._send_and_get(sender_fn, *args, **kwargs)
        if six.PY3:
            got = got.decode('utf-8')
        return self.assertTrue(re.search(expected_regexp, got),
                               [got, expected_regexp])

    def test_methods_are_no_ops_when_not_enabled(self):
        logger = utils.get_logger({
            # No "log_statsd_host" means "disabled"
            'log_statsd_port': str(self.port),
        }, 'some-name')
        # Delegate methods are no-ops
        self.assertIsNone(logger.update_stats('foo', 88))
        self.assertIsNone(logger.update_stats('foo', 88, 0.57))
        self.assertIsNone(logger.update_stats('foo', 88,
                                              sample_rate=0.61))
        self.assertIsNone(logger.increment('foo'))
        self.assertIsNone(logger.increment('foo', 0.57))
        self.assertIsNone(logger.increment('foo', sample_rate=0.61))
        self.assertIsNone(logger.decrement('foo'))
        self.assertIsNone(logger.decrement('foo', 0.57))
        self.assertIsNone(logger.decrement('foo', sample_rate=0.61))
        self.assertIsNone(logger.timing('foo', 88.048))
        self.assertIsNone(logger.timing('foo', 88.57, 0.34))
        self.assertIsNone(logger.timing('foo', 88.998, sample_rate=0.82))
        self.assertIsNone(logger.timing_since('foo', 8938))
        self.assertIsNone(logger.timing_since('foo', 8948, 0.57))
        self.assertIsNone(logger.timing_since('foo', 849398,
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
        self.assertEqual(valid_utf8_str,
                         utils.get_valid_utf8_str(valid_utf8_str))
        self.assertEqual(valid_utf8_str,
                         utils.get_valid_utf8_str(unicode_sample))
        self.assertEqual(b'\xef\xbf\xbd\xef\xbf\xbd\xec\xbc\x9d\xef\xbf\xbd',
                         utils.get_valid_utf8_str(invalid_utf8_str))

    @reset_logger_state
    def test_thread_locals(self):
        logger = utils.get_logger(None)
        # test the setter
        logger.thread_locals = ('id', 'ip')
        self.assertEqual(logger.thread_locals, ('id', 'ip'))
        # reset
        logger.thread_locals = (None, None)
        self.assertEqual(logger.thread_locals, (None, None))
        logger.txn_id = '1234'
        logger.client_ip = '1.2.3.4'
        self.assertEqual(logger.thread_locals, ('1234', '1.2.3.4'))
        logger.txn_id = '5678'
        logger.client_ip = '5.6.7.8'
        self.assertEqual(logger.thread_locals, ('5678', '5.6.7.8'))

    def test_no_fdatasync(self):
        called = []

        class NoFdatasync(object):
            pass

        def fsync(fd):
            called.append(fd)

        with patch('swift.common.utils.os', NoFdatasync()):
            with patch('swift.common.utils.fsync', fsync):
                utils.fdatasync(12345)
                self.assertEqual(called, [12345])

    def test_yes_fdatasync(self):
        called = []

        class YesFdatasync(object):

            def fdatasync(self, fd):
                called.append(fd)

        with patch('swift.common.utils.os', YesFdatasync()):
            utils.fdatasync(12345)
            self.assertEqual(called, [12345])

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
            self.assertEqual(called, [12345, 123])

    def test_fsync_no_fullsync(self):
        called = []

        class FCNTL(object):
            pass

        def fsync(fd):
            called.append(fd)

        with patch('swift.common.utils.fcntl', FCNTL()):
            with patch('os.fsync', fsync):
                utils.fsync(12345)
                self.assertEqual(called, [12345])


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

        # Check Raise on Bad partition
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

        # Check Raise on Bad Suffix
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

        # Check Raise on Bad Hash
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
            # Create a file, that represents a non-dir drive
            open(os.path.join(tmpdir, 'asdf'), 'w')
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=False, logger=logger
            )
            self.assertEqual(list(locations), [])
            self.assertEqual(1, len(logger.get_lines_for_level('warning')))
            # Test without the logger
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=False
            )
            self.assertEqual(list(locations), [])

    def test_mount_check_drive(self):
        with temptree([]) as tmpdir:
            logger = FakeLogger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            # Create a file, that represents a non-dir drive
            open(os.path.join(tmpdir, 'asdf'), 'w')
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=True, logger=logger
            )
            self.assertEqual(list(locations), [])
            self.assertEqual(2, len(logger.get_lines_for_level('warning')))

            # Test without the logger
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
            # Create a file, that represents a non-dir drive
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
        for x in range(3):
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
            for x in range(3):
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
        self.assertEqual(pile.waitall(0.5), [0.1])
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

    def test_waitfirst_only_returns_first(self):
        def run_test(name):
            eventlet.sleep(0)
            completed.append(name)
            return name

        completed = []
        pile = utils.GreenAsyncPile(3)
        pile.spawn(run_test, 'first')
        pile.spawn(run_test, 'second')
        pile.spawn(run_test, 'third')
        self.assertEqual(pile.waitfirst(0.5), completed[0])
        # 3 still completed, but only the first was returned.
        self.assertEqual(3, len(completed))

    def test_wait_with_firstn(self):
        def run_test(name):
            eventlet.sleep(0)
            completed.append(name)
            return name

        for first_n in [None] + list(range(6)):
            completed = []
            pile = utils.GreenAsyncPile(10)
            for i in range(10):
                pile.spawn(run_test, i)
            actual = pile._wait(1, first_n)
            expected_n = first_n if first_n else 10
            self.assertEqual(completed[:expected_n], actual)
            self.assertEqual(10, len(completed))

    def test_pending(self):
        pile = utils.GreenAsyncPile(3)
        self.assertEqual(0, pile._pending)
        for repeats in range(2):
            # repeat to verify that pending will go again up after going down
            for i in range(4):
                pile.spawn(lambda: i)
            self.assertEqual(4, pile._pending)
            for i in range(3, -1, -1):
                next(pile)
                self.assertEqual(i, pile._pending)
            # sanity check - the pile is empty
            self.assertRaises(StopIteration, pile.next)
            # pending remains 0
            self.assertEqual(0, pile._pending)


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


class TestSpliterator(unittest.TestCase):
    def test_string(self):
        input_chunks = ["coun", "ter-", "b", "ra", "nch-mater",
                        "nit", "y-fungusy", "-nummular"]
        si = utils.Spliterator(input_chunks)

        self.assertEqual(''.join(si.take(8)), "counter-")
        self.assertEqual(''.join(si.take(7)), "branch-")
        self.assertEqual(''.join(si.take(10)), "maternity-")
        self.assertEqual(''.join(si.take(8)), "fungusy-")
        self.assertEqual(''.join(si.take(8)), "nummular")

    def test_big_input_string(self):
        input_chunks = ["iridium"]
        si = utils.Spliterator(input_chunks)

        self.assertEqual(''.join(si.take(2)), "ir")
        self.assertEqual(''.join(si.take(1)), "i")
        self.assertEqual(''.join(si.take(2)), "di")
        self.assertEqual(''.join(si.take(1)), "u")
        self.assertEqual(''.join(si.take(1)), "m")

    def test_chunk_boundaries(self):
        input_chunks = ["soylent", "green", "is", "people"]
        si = utils.Spliterator(input_chunks)

        self.assertEqual(''.join(si.take(7)), "soylent")
        self.assertEqual(''.join(si.take(5)), "green")
        self.assertEqual(''.join(si.take(2)), "is")
        self.assertEqual(''.join(si.take(6)), "people")

    def test_no_empty_strings(self):
        input_chunks = ["soylent", "green", "is", "people"]
        si = utils.Spliterator(input_chunks)

        outputs = (list(si.take(7))     # starts and ends on chunk boundary
                   + list(si.take(2))   # spans two chunks
                   + list(si.take(3))   # begins but does not end chunk
                   + list(si.take(2))   # ends but does not begin chunk
                   + list(si.take(6)))  # whole chunk + EOF
        self.assertNotIn('', outputs)

    def test_running_out(self):
        input_chunks = ["not much"]
        si = utils.Spliterator(input_chunks)

        self.assertEqual(''.join(si.take(4)), "not ")
        self.assertEqual(''.join(si.take(99)), "much")  # short
        self.assertEqual(''.join(si.take(4)), "")
        self.assertEqual(''.join(si.take(4)), "")

    def test_overlap(self):
        input_chunks = ["one fish", "two fish", "red fish", "blue fish"]

        si = utils.Spliterator(input_chunks)
        t1 = si.take(20)  # longer than first chunk
        self.assertLess(len(next(t1)), 20)  # it's not exhausted

        t2 = si.take(20)
        self.assertRaises(ValueError, next, t2)

    def test_closing(self):
        input_chunks = ["abcd", "efg", "hij"]

        si = utils.Spliterator(input_chunks)
        it = si.take(3)  # shorter than first chunk
        self.assertEqual(next(it), 'abc')
        it.close()
        self.assertEqual(list(si.take(20)), ['d', 'efg', 'hij'])

        si = utils.Spliterator(input_chunks)
        self.assertEqual(list(si.take(1)), ['a'])
        it = si.take(1)  # still shorter than first chunk
        self.assertEqual(next(it), 'b')
        it.close()
        self.assertEqual(list(si.take(20)), ['cd', 'efg', 'hij'])

        si = utils.Spliterator(input_chunks)
        it = si.take(6)  # longer than first chunk, shorter than first + second
        self.assertEqual(next(it), 'abcd')
        self.assertEqual(next(it), 'ef')
        it.close()
        self.assertEqual(list(si.take(20)), ['g', 'hij'])

        si = utils.Spliterator(input_chunks)
        self.assertEqual(list(si.take(2)), ['ab'])
        it = si.take(3)  # longer than rest of chunk
        self.assertEqual(next(it), 'cd')
        it.close()
        self.assertEqual(list(si.take(20)), ['efg', 'hij'])


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
        self.assertEqual(name, 'text/plain')
        self.assertEqual(attrs, {})

    def test_content_type_with_charset(self):
        name, attrs = utils.parse_content_disposition(
            'text/plain; charset=UTF8')
        self.assertEqual(name, 'text/plain')
        self.assertEqual(attrs, {'charset': 'UTF8'})

    def test_content_disposition(self):
        name, attrs = utils.parse_content_disposition(
            'form-data; name="somefile"; filename="test.html"')
        self.assertEqual(name, 'form-data')
        self.assertEqual(attrs, {'name': 'somefile', 'filename': 'test.html'})

    def test_content_disposition_without_white_space(self):
        name, attrs = utils.parse_content_disposition(
            'form-data;name="somefile";filename="test.html"')
        self.assertEqual(name, 'form-data')
        self.assertEqual(attrs, {'name': 'somefile', 'filename': 'test.html'})


class TestIterMultipartMimeDocuments(unittest.TestCase):

    def test_bad_start(self):
        it = utils.iter_multipart_mime_documents(StringIO('blah'), 'unique')
        exc = None
        try:
            next(it)
        except MimeInvalid as err:
            exc = err
        self.assertTrue('invalid starting boundary' in str(exc))
        self.assertTrue('--unique' in str(exc))

    def test_empty(self):
        it = utils.iter_multipart_mime_documents(StringIO('--unique'),
                                                 'unique')
        fp = next(it)
        self.assertEqual(fp.read(), '')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_basic(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique--'), 'unique')
        fp = next(it)
        self.assertEqual(fp.read(), 'abcdefg')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_basic2(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = next(it)
        self.assertEqual(fp.read(), 'abcdefg')
        fp = next(it)
        self.assertEqual(fp.read(), 'hijkl')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_tiny_reads(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = next(it)
        self.assertEqual(fp.read(2), 'ab')
        self.assertEqual(fp.read(2), 'cd')
        self.assertEqual(fp.read(2), 'ef')
        self.assertEqual(fp.read(2), 'g')
        self.assertEqual(fp.read(2), '')
        fp = next(it)
        self.assertEqual(fp.read(), 'hijkl')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_big_reads(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = next(it)
        self.assertEqual(fp.read(65536), 'abcdefg')
        self.assertEqual(fp.read(), '')
        fp = next(it)
        self.assertEqual(fp.read(), 'hijkl')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_leading_crlfs(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('\r\n\r\n\r\n--unique\r\nabcdefg\r\n'
                     '--unique\r\nhijkl\r\n--unique--'),
            'unique')
        fp = next(it)
        self.assertEqual(fp.read(65536), 'abcdefg')
        self.assertEqual(fp.read(), '')
        fp = next(it)
        self.assertEqual(fp.read(), 'hijkl')
        self.assertRaises(StopIteration, it.next)

    def test_broken_mid_stream(self):
        # We go ahead and accept whatever is sent instead of rejecting the
        # whole request, in case the partial form is still useful.
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nabc'), 'unique')
        fp = next(it)
        self.assertEqual(fp.read(), 'abc')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_readline(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nab\r\ncd\ref\ng\r\n--unique\r\nhi\r\n\r\n'
                     'jkl\r\n\r\n--unique--'), 'unique')
        fp = next(it)
        self.assertEqual(fp.readline(), 'ab\r\n')
        self.assertEqual(fp.readline(), 'cd\ref\ng')
        self.assertEqual(fp.readline(), '')
        fp = next(it)
        self.assertEqual(fp.readline(), 'hi\r\n')
        self.assertEqual(fp.readline(), '\r\n')
        self.assertEqual(fp.readline(), 'jkl\r\n')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)

    def test_readline_with_tiny_chunks(self):
        it = utils.iter_multipart_mime_documents(
            StringIO('--unique\r\nab\r\ncd\ref\ng\r\n--unique\r\nhi\r\n'
                     '\r\njkl\r\n\r\n--unique--'),
            'unique',
            read_chunk_size=2)
        fp = next(it)
        self.assertEqual(fp.readline(), 'ab\r\n')
        self.assertEqual(fp.readline(), 'cd\ref\ng')
        self.assertEqual(fp.readline(), '')
        fp = next(it)
        self.assertEqual(fp.readline(), 'hi\r\n')
        self.assertEqual(fp.readline(), '\r\n')
        self.assertEqual(fp.readline(), 'jkl\r\n')
        exc = None
        try:
            next(it)
        except StopIteration as err:
            exc = err
        self.assertTrue(exc is not None)


class TestParseMimeHeaders(unittest.TestCase):

    def test_parse_mime_headers(self):
        doc_file = BytesIO(b"""Content-Disposition: form-data; name="file_size"
Foo: Bar
NOT-title-cAsED: quux
Connexion: =?iso8859-1?q?r=E9initialis=E9e_par_l=27homologue?=
Status: =?utf-8?b?5byA5aeL6YCa6L+H5a+56LGh5aSN5Yi2?=
Latin-1: Resincronizaci\xf3n realizada con \xe9xito
Utf-8: \xd0\xba\xd0\xbe\xd0\xbd\xd1\x82\xd0\xb5\xd0\xb9\xd0\xbd\xd0\xb5\xd1\x80

This is the body
""")
        headers = utils.parse_mime_headers(doc_file)
        utf8 = u'\u043a\u043e\u043d\u0442\u0435\u0439\u043d\u0435\u0440'
        if six.PY2:
            utf8 = utf8.encode('utf-8')

        expected_headers = {
            'Content-Disposition': 'form-data; name="file_size"',
            'Foo': "Bar",
            'Not-Title-Cased': "quux",
            # Encoded-word or non-ASCII values are treated just like any other
            # bytestring (at least for now)
            'Connexion': "=?iso8859-1?q?r=E9initialis=E9e_par_l=27homologue?=",
            'Status': "=?utf-8?b?5byA5aeL6YCa6L+H5a+56LGh5aSN5Yi2?=",
            'Latin-1': "Resincronizaci\xf3n realizada con \xe9xito",
            'Utf-8': utf8,
        }
        self.assertEqual(expected_headers, headers)
        self.assertEqual(b"This is the body\n", doc_file.read())


class FakeResponse(object):
    def __init__(self, status, headers, body):
        self.status = status
        self.headers = HeaderKeyDict(headers)
        self.body = StringIO(body)

    def getheader(self, header_name):
        return str(self.headers.get(header_name, ''))

    def getheaders(self):
        return self.headers.items()

    def read(self, length=None):
        return self.body.read(length)

    def readline(self, length=None):
        return self.body.readline(length)


class TestDocumentItersToHTTPResponseBody(unittest.TestCase):
    def test_no_parts(self):
        body = utils.document_iters_to_http_response_body(
            iter([]), 'dontcare',
            multipart=False, logger=FakeLogger())
        self.assertEqual(body, '')

    def test_single_part(self):
        body = "time flies like an arrow; fruit flies like a banana"
        doc_iters = [{'part_iter': iter(StringIO(body).read, '')}]

        resp_body = ''.join(
            utils.document_iters_to_http_response_body(
                iter(doc_iters), 'dontcare',
                multipart=False, logger=FakeLogger()))
        self.assertEqual(resp_body, body)

    def test_multiple_parts(self):
        part1 = "two peanuts were walking down a railroad track"
        part2 = "and one was a salted. ... peanut."

        doc_iters = [{
            'start_byte': 88,
            'end_byte': 133,
            'content_type': 'application/peanut',
            'entity_length': 1024,
            'part_iter': iter(StringIO(part1).read, ''),
        }, {
            'start_byte': 500,
            'end_byte': 532,
            'content_type': 'application/salted',
            'entity_length': 1024,
            'part_iter': iter(StringIO(part2).read, ''),
        }]

        resp_body = ''.join(
            utils.document_iters_to_http_response_body(
                iter(doc_iters), 'boundaryboundary',
                multipart=True, logger=FakeLogger()))
        self.assertEqual(resp_body, (
            "--boundaryboundary\r\n" +
            # This is a little too strict; we don't actually care that the
            # headers are in this order, but the test is much more legible
            # this way.
            "Content-Type: application/peanut\r\n" +
            "Content-Range: bytes 88-133/1024\r\n" +
            "\r\n" +
            part1 + "\r\n" +
            "--boundaryboundary\r\n"
            "Content-Type: application/salted\r\n" +
            "Content-Range: bytes 500-532/1024\r\n" +
            "\r\n" +
            part2 + "\r\n" +
            "--boundaryboundary--"))

    def test_closed_part_iterator(self):
        print('test')
        useful_iter_mock = mock.MagicMock()
        useful_iter_mock.__iter__.return_value = ['']
        body_iter = utils.document_iters_to_http_response_body(
            iter([{'part_iter': useful_iter_mock}]), 'dontcare',
            multipart=False, logger=FakeLogger())
        body = ''
        for s in body_iter:
            body += s
        self.assertEqual(body, '')
        useful_iter_mock.close.assert_called_once_with()

        # Calling "close" on the mock will now raise an AttributeError
        del useful_iter_mock.close
        body_iter = utils.document_iters_to_http_response_body(
            iter([{'part_iter': useful_iter_mock}]), 'dontcare',
            multipart=False, logger=FakeLogger())
        body = ''
        for s in body_iter:
            body += s


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


class TestSocketStringParser(unittest.TestCase):
    def test_socket_string_parser(self):
        default = 1337
        addrs = [('1.2.3.4', '1.2.3.4', default),
                 ('1.2.3.4:5000', '1.2.3.4', 5000),
                 ('[dead:beef::1]', 'dead:beef::1', default),
                 ('[dead:beef::1]:5000', 'dead:beef::1', 5000),
                 ('example.com', 'example.com', default),
                 ('example.com:5000', 'example.com', 5000),
                 ('foo.1-2-3.bar.com:5000', 'foo.1-2-3.bar.com', 5000),
                 ('1.2.3.4:10:20', None, None),
                 ('dead:beef::1:5000', None, None)]

        for addr, expected_host, expected_port in addrs:
            if expected_host:
                host, port = utils.parse_socket_string(addr, default)
                self.assertEqual(expected_host, host)
                self.assertEqual(expected_port, int(port))
            else:
                with self.assertRaises(ValueError):
                    utils.parse_socket_string(addr, default)


if __name__ == '__main__':
    unittest.main()
