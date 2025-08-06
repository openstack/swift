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

import argparse
import hashlib
import io
import itertools

from swift.common.statsd_client import StatsdClient
from test.debug_logger import debug_logger, FakeStatsdClient
from test.unit import temptree, make_timestamp_iter, with_tempdir, \
    mock_timestamp_now, FakeIterable

import contextlib
import errno
import eventlet
import eventlet.debug
import eventlet.event
import eventlet.patcher
import grp
import logging
import os
from unittest import mock
import posix
import pwd
import random
import socket
import string
import sys
import json
import math
import inspect
import warnings

import tempfile
import time
import unittest
import fcntl
import shutil

from getpass import getuser
from io import BytesIO, StringIO
from shutil import rmtree
from functools import partial
from tempfile import TemporaryFile, NamedTemporaryFile, mkdtemp
from unittest.mock import MagicMock, patch
from configparser import NoSectionError, NoOptionError
from uuid import uuid4

from swift.common.exceptions import Timeout, LockTimeout, \
    ReplicationLockTimeout, MimeInvalid
from swift.common import utils
from swift.common.utils import set_swift_dir, md5, ShardRangeList
from swift.common.container_sync_realms import ContainerSyncRealms
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.storage_policy import POLICIES, reload_storage_policies
from swift.common.swob import Response
from test.unit import requires_o_tmpfile_support_in_tmp

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
        self.called_funcs[name] = args

    def raise_func(self, name, *args, **kwargs):
        self.called_funcs[name] = args
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


class MockSys(object):

    def __init__(self):
        self.stdin = TemporaryFile('w')
        self.stdout = TemporaryFile('r')
        self.stderr = TemporaryFile('r')
        self.__stderr__ = self.stderr
        self.stdio_fds = [self.stdin.fileno(), self.stdout.fileno(),
                          self.stderr.fileno()]


class TestUTC(unittest.TestCase):
    def test_tzname(self):
        self.assertEqual(utils.UTC.tzname(None), 'UTC')


class TestUtils(unittest.TestCase):
    """Tests for swift.common.utils """

    def setUp(self):
        utils.HASH_PATH_SUFFIX = b'endcap'
        utils.HASH_PATH_PREFIX = b'startcap'
        self.md5_test_data = "Openstack forever".encode('utf-8')
        try:
            self.md5_digest = hashlib.md5(self.md5_test_data).hexdigest()
            self.fips_enabled = False
        except ValueError:
            self.md5_digest = '0d6dc3c588ae71a04ce9a6beebbbba06'
            self.fips_enabled = True

    def test_monkey_patch(self):
        def take_and_release(lock):
            try:
                lock.acquire()
            finally:
                lock.release()

        def do_test():
            res = 0
            try:
                # this module imports eventlet original threading, so re-import
                # locally...
                import threading
                import traceback
                logging_lock_before = logging._lock
                my_lock_before = threading.RLock()
                self.assertIsInstance(logging_lock_before,
                                      type(my_lock_before))

                utils.monkey_patch()

                logging_lock_after = logging._lock
                my_lock_after = threading.RLock()
                self.assertIsInstance(logging_lock_after,
                                      type(my_lock_after))

                self.assertTrue(logging_lock_after.acquire())
                thread = threading.Thread(target=take_and_release,
                                          args=(logging_lock_after,))
                thread.start()
                self.assertTrue(thread.isAlive())
                # we should timeout while the thread is still blocking on lock
                eventlet.sleep()
                thread.join(timeout=0.1)
                self.assertTrue(thread.isAlive())

                logging._lock.release()
                thread.join(timeout=0.1)
                self.assertFalse(thread.isAlive())
            except AssertionError:
                traceback.print_exc()
                res = 1
            finally:
                os._exit(res)

        pid = os.fork()
        if pid == 0:
            # run the test in an isolated environment to avoid monkey patching
            # in this one
            do_test()
        else:
            child_pid, errcode = os.waitpid(pid, 0)
            self.assertEqual(0, os.WEXITSTATUS(errcode),
                             'Forked do_test failed')

    def test_get_zero_indexed_base_string(self):
        self.assertEqual(utils.get_zero_indexed_base_string('something', 0),
                         'something')
        self.assertEqual(utils.get_zero_indexed_base_string('something', None),
                         'something')
        self.assertEqual(utils.get_zero_indexed_base_string('something', 1),
                         'something-1')
        self.assertRaises(ValueError, utils.get_zero_indexed_base_string,
                          'something', 'not_integer')

    @with_tempdir
    def test_lock_path(self, tmpdir):
        # 2 locks with limit=1 must fail
        success = False
        with utils.lock_path(tmpdir, 0.1):
            with self.assertRaises(LockTimeout):
                with utils.lock_path(tmpdir, 0.1):
                    success = True
        self.assertFalse(success)

        # 2 locks with limit=2 must succeed
        success = False
        with utils.lock_path(tmpdir, 0.1, limit=2):
            try:
                with utils.lock_path(tmpdir, 0.1, limit=2):
                    success = True
            except LockTimeout as exc:
                self.fail('Unexpected exception %s' % exc)
        self.assertTrue(success)

        # 3 locks with limit=2 must fail
        success = False
        with utils.lock_path(tmpdir, 0.1, limit=2):
            with utils.lock_path(tmpdir, 0.1, limit=2):
                with self.assertRaises(LockTimeout):
                    with utils.lock_path(tmpdir, 0.1):
                        success = True
        self.assertFalse(success)

    @with_tempdir
    def test_lock_path_invalid_limit(self, tmpdir):
        success = False
        with self.assertRaises(ValueError):
            with utils.lock_path(tmpdir, 0.1, limit=0):
                success = True
        self.assertFalse(success)
        with self.assertRaises(ValueError):
            with utils.lock_path(tmpdir, 0.1, limit=-1):
                success = True
        self.assertFalse(success)
        with self.assertRaises(TypeError):
            with utils.lock_path(tmpdir, 0.1, limit='1'):
                success = True
        self.assertFalse(success)
        with self.assertRaises(TypeError):
            with utils.lock_path(tmpdir, 0.1, limit=1.1):
                success = True
        self.assertFalse(success)

    @with_tempdir
    def test_lock_path_num_sleeps(self, tmpdir):
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
        self.assertEqual(num_short_calls[0], 11)
        self.assertTrue(exception_raised[0])

    @with_tempdir
    def test_lock_path_class(self, tmpdir):
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

    @with_tempdir
    def test_lock_path_name(self, tmpdir):
        # With default limit (1), can't take the same named lock twice
        success = False
        with utils.lock_path(tmpdir, 0.1, name='foo'):
            with self.assertRaises(LockTimeout):
                with utils.lock_path(tmpdir, 0.1, name='foo'):
                    success = True
        self.assertFalse(success)
        # With default limit (1), can take two differently named locks
        success = False
        with utils.lock_path(tmpdir, 0.1, name='foo'):
            with utils.lock_path(tmpdir, 0.1, name='bar'):
                success = True
        self.assertTrue(success)
        # With default limit (1), can take a named lock and the default lock
        success = False
        with utils.lock_path(tmpdir, 0.1, name='foo'):
            with utils.lock_path(tmpdir, 0.1):
                success = True
        self.assertTrue(success)

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
        with self.assertRaises(TypeError):
            utils.normalize_delete_at_timestamp(None)
        with self.assertRaises(ValueError):
            utils.normalize_delete_at_timestamp('')
        with self.assertRaises(ValueError):
            utils.normalize_delete_at_timestamp('abc')

    def test_normalize_delete_at_timestamp_high_precision(self):
        self.assertEqual(
            utils.normalize_delete_at_timestamp(1253327593, True),
            '1253327593.00000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(1253327593.67890, True),
            '1253327593.67890')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('1253327593', True),
            '1253327593.00000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('1253327593.67890', True),
            '1253327593.67890')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(-1253327593, True),
            '0000000000.00000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(-1253327593.67890, True),
            '0000000000.00000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('-1253327593', True),
            '0000000000.00000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('-1253327593.67890', True),
            '0000000000.00000')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(71253327593, True),
            '9999999999.99999')
        self.assertEqual(
            utils.normalize_delete_at_timestamp(71253327593.67890, True),
            '9999999999.99999')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('71253327593', True),
            '9999999999.99999')
        self.assertEqual(
            utils.normalize_delete_at_timestamp('71253327593.67890', True),
            '9999999999.99999')
        with self.assertRaises(TypeError):
            utils.normalize_delete_at_timestamp(None, True)
        with self.assertRaises(ValueError):
            utils.normalize_delete_at_timestamp('', True)
        with self.assertRaises(ValueError):
            utils.normalize_delete_at_timestamp('abc', True)

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

    def test_drain_and_close(self):
        utils.drain_and_close([])
        utils.drain_and_close(iter([]))
        drained = [False]

        def gen():
            yield 'x'
            yield 'y'
            drained[0] = True

        g = gen()
        utils.drain_and_close(g)
        self.assertTrue(drained[0])
        self.assertIsNone(g.gi_frame)

        utils.drain_and_close(Response(status=200, body=b'Some body'))
        drained = [False]
        utils.drain_and_close(Response(status=200, app_iter=gen()))
        self.assertTrue(drained[0])

    def test_drain_and_close_with_limit(self):

        def gen():
            yield 'a' * 5
            yield 'a' * 4
            yield 'a' * 3
            drained[0] = True

        drained = [False]
        g = gen()
        utils.drain_and_close(g, read_limit=13)
        self.assertTrue(drained[0])
        self.assertIsNone(g.gi_frame)

        drained = [False]
        g = gen()
        utils.drain_and_close(g, read_limit=12)
        # this would need *one more* call to next
        self.assertFalse(drained[0])
        self.assertIsNone(g.gi_frame)

        drained = [False]
        # not even close to the whole thing
        g = gen()
        utils.drain_and_close(g, read_limit=3)
        self.assertFalse(drained[0])
        self.assertIsNone(g.gi_frame)

        drained = [False]
        # default is to drain; no limit!
        g = gen()
        utils.drain_and_close(g)
        self.assertIsNone(g.gi_frame)
        self.assertTrue(drained[0])

    def test_friendly_close_small_body(self):

        def small_body_iter():
            yield 'a small body'
            drained[0] = True

        drained = [False]
        utils.friendly_close(small_body_iter())
        self.assertTrue(drained[0])

    def test_friendly_close_large_body(self):
        def large_body_iter():
            for i in range(10):
                chunk = chr(97 + i) * 64 * 2 ** 10
                yielded_chunks.append(chunk)
                yield chunk
            drained[0] = True

        drained = [False]
        yielded_chunks = []
        utils.friendly_close(large_body_iter())
        self.assertFalse(drained[0])
        self.assertEqual(['a' * 65536], yielded_chunks)

    def test_friendly_close_exploding_body(self):

        class ExplodingBody(object):

            def __init__(self):
                self.yielded_chunks = []
                self.close_calls = []
                self._body = self._exploding_iter()

            def _exploding_iter(self):
                chunk = 'a' * 63 * 2 ** 10
                self.yielded_chunks.append(chunk)
                yield chunk
                raise Exception('kaboom!')

            def __iter__(self):
                return self

            def __next__(self):
                return next(self._body)

            def close(self):
                self.close_calls.append(True)

        body = ExplodingBody()
        with self.assertRaises(Exception) as ctx:
            utils.friendly_close(body)
        self.assertEqual('kaboom!', str(ctx.exception))
        self.assertEqual(['a' * 64512], body.yielded_chunks)
        self.assertEqual([True], body.close_calls)

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

    @with_tempdir
    def test_mkdirs(self, testdir_base):
        testroot = os.path.join(testdir_base, 'mkdirs')
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
        self.assertRaises(ValueError, utils.split_path, '/v/a//o', 1, 4, True)
        self.assertRaises(ValueError, utils.split_path, '/v/a//o', 2, 4, True)
        self.assertEqual(utils.split_path('/a/c', 2), ['a', 'c'])
        self.assertEqual(utils.split_path('/a/c/o', 3), ['a', 'c', 'o'])
        self.assertRaises(ValueError, utils.split_path, '/a/c/o/r', 3, 3)
        self.assertEqual(utils.split_path('/a/c/o/r', 3, 3, True),
                         ['a', 'c', 'o/r'])
        self.assertEqual(utils.split_path('/a/c//o', 1, 3, True),
                         ['a', 'c', '/o'])
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

    def test_parse_options(self):
        # Get a file that is definitely on disk
        with NamedTemporaryFile() as f:
            conf_file = f.name
            conf, options = utils.parse_options(test_args=[conf_file])
            self.assertEqual(conf, conf_file)
            # assert defaults
            self.assertEqual(options['verbose'], False)
            self.assertNotIn('once', options)
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
        with mock.patch.object(utils.sys, 'stdout', StringIO()) as stdo:
            self.assertRaises(SystemExit, utils.parse_options, once=True,
                              test_args=[])
            self.assertTrue('missing config' in stdo.getvalue())

            # verify conf file must exist -- context manager will delete
            # temp file
            with NamedTemporaryFile() as f:
                conf_file = f.name
            self.assertRaises(SystemExit, utils.parse_options, once=True,
                              test_args=[conf_file])
            self.assertTrue('unable to locate' in stdo.getvalue())

    @with_tempdir
    def test_dump_recon_cache(self, testdir_base):
        testcache_file = os.path.join(testdir_base, 'cache.recon')
        logger = utils.get_logger(None, 'server', log_route='server')
        submit_dict = {'key0': 99,
                       'key1': {'value1': 1, 'value2': 2}}
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(submit_dict, file_dict)
        # Use a nested entry
        submit_dict = {'key0': 101,
                       'key1': {'key2': {'value1': 1, 'value2': 2}}}
        expect_dict = {'key0': 101,
                       'key1': {'key2': {'value1': 1, 'value2': 2},
                                'value1': 1, 'value2': 2}}
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(expect_dict, file_dict)
        # nested dict items are not sticky
        submit_dict = {'key1': {'key2': {'value3': 3}}}
        expect_dict = {'key0': 101,
                       'key1': {'key2': {'value3': 3},
                                'value1': 1, 'value2': 2}}
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(expect_dict, file_dict)
        # cached entries are sticky
        submit_dict = {}
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(expect_dict, file_dict)
        # nested dicts can be erased...
        submit_dict = {'key1': {'key2': {}}}
        expect_dict = {'key0': 101,
                       'key1': {'value1': 1, 'value2': 2}}
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(expect_dict, file_dict)
        # ... and erasure is idempotent
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(expect_dict, file_dict)
        # top level dicts can be erased...
        submit_dict = {'key1': {}}
        expect_dict = {'key0': 101}
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(expect_dict, file_dict)
        # ... and erasure is idempotent
        utils.dump_recon_cache(submit_dict, testcache_file, logger)
        with open(testcache_file) as fd:
            file_dict = json.loads(fd.readline())
        self.assertEqual(expect_dict, file_dict)

    @with_tempdir
    def test_dump_recon_cache_set_owner(self, testdir_base):
        testcache_file = os.path.join(testdir_base, 'cache.recon')
        logger = utils.get_logger(None, 'server', log_route='server')
        submit_dict = {'key1': {'value1': 1, 'value2': 2}}

        _ret = lambda: None
        _ret.pw_uid = 100
        _mock_getpwnam = MagicMock(return_value=_ret)
        _mock_chown = mock.Mock()

        with patch('os.chown', _mock_chown), \
                patch('pwd.getpwnam', _mock_getpwnam):
            utils.dump_recon_cache(submit_dict, testcache_file,
                                   logger, set_owner="swift")

        _mock_getpwnam.assert_called_once_with("swift")
        self.assertEqual(_mock_chown.call_args[0][1], 100)

    @with_tempdir
    def test_dump_recon_cache_permission_denied(self, testdir_base):
        testcache_file = os.path.join(testdir_base, 'cache.recon')

        class MockLogger(object):
            def __init__(self):
                self._excs = []

            def exception(self, message):
                _junk, exc, _junk = sys.exc_info()
                self._excs.append(exc)

        logger = MockLogger()
        submit_dict = {'key1': {'value1': 1, 'value2': 2}}
        with mock.patch(
                'swift.common.utils.NamedTemporaryFile',
                side_effect=IOError(13, 'Permission Denied')):
            utils.dump_recon_cache(submit_dict, testcache_file, logger)
        self.assertIsInstance(logger._excs[0], IOError)

    def test_load_recon_cache(self):
        stub_data = {'test': 'foo'}
        with NamedTemporaryFile() as f:
            f.write(json.dumps(stub_data).encode("utf-8"))
            f.flush()
            self.assertEqual(stub_data, utils.load_recon_cache(f.name))

        # missing files are treated as empty
        self.assertFalse(os.path.exists(f.name))  # sanity
        self.assertEqual({}, utils.load_recon_cache(f.name))

        # Corrupt files are treated as empty. We could crash and make an
        # operator fix the corrupt file, but they'll "fix" it with "rm -f
        # /var/cache/swift/*.recon", so let's just do it for them.
        with NamedTemporaryFile() as f:
            f.write(b"{not [valid (json")
            f.flush()
            self.assertEqual({}, utils.load_recon_cache(f.name))

    def test_storage_directory(self):
        self.assertEqual(utils.storage_directory('objects', '1', 'ABCDEF'),
                         'objects/1/DEF/ABCDEF')

    def test_select_node_ip(self):
        dev = {
            'ip': '127.0.0.1',
            'port': 6200,
            'replication_ip': '127.0.1.1',
            'replication_port': 6400,
            'device': 'sdb',
        }
        self.assertEqual(('127.0.0.1', 6200), utils.select_ip_port(dev))
        self.assertEqual(('127.0.1.1', 6400),
                         utils.select_ip_port(dev, use_replication=True))
        dev['use_replication'] = False
        self.assertEqual(('127.0.1.1', 6400),
                         utils.select_ip_port(dev, use_replication=True))
        dev['use_replication'] = True
        self.assertEqual(('127.0.1.1', 6400), utils.select_ip_port(dev))
        self.assertEqual(('127.0.1.1', 6400),
                         utils.select_ip_port(dev, use_replication=False))

    def test_node_to_string(self):
        dev = {
            'id': 3,
            'region': 1,
            'zone': 1,
            'ip': '127.0.0.1',
            'port': 6200,
            'replication_ip': '127.0.1.1',
            'replication_port': 6400,
            'device': 'sdb',
            'meta': '',
            'weight': 8000.0,
            'index': 0,
        }
        self.assertEqual(utils.node_to_string(dev), '127.0.0.1:6200/sdb')
        self.assertEqual(utils.node_to_string(dev, replication=True),
                         '127.0.1.1:6400/sdb')
        dev['use_replication'] = False
        self.assertEqual(utils.node_to_string(dev), '127.0.0.1:6200/sdb')
        self.assertEqual(utils.node_to_string(dev, replication=True),
                         '127.0.1.1:6400/sdb')
        dev['use_replication'] = True
        self.assertEqual(utils.node_to_string(dev), '127.0.1.1:6400/sdb')
        # Node dict takes precedence
        self.assertEqual(utils.node_to_string(dev, replication=False),
                         '127.0.1.1:6400/sdb')

        dev = {
            'id': 3,
            'region': 1,
            'zone': 1,
            'ip': "fe80::0204:61ff:fe9d:f156",
            'port': 6200,
            'replication_ip': "fe80::0204:61ff:ff9d:1234",
            'replication_port': 6400,
            'device': 'sdb',
            'meta': '',
            'weight': 8000.0,
            'index': 0,
        }
        self.assertEqual(utils.node_to_string(dev),
                         '[fe80::0204:61ff:fe9d:f156]:6200/sdb')
        self.assertEqual(utils.node_to_string(dev, replication=True),
                         '[fe80::0204:61ff:ff9d:1234]:6400/sdb')

    def test_hash_path(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results hash_path produces, they know it
        with mock.patch('swift.common.utils.HASH_PATH_PREFIX', b''):
            self.assertEqual(utils.hash_path('a'),
                             '1c84525acb02107ea475dcd3d09c2c58')
            self.assertEqual(utils.hash_path('a', 'c'),
                             '33379ecb053aa5c9e356c68997cbb59e')
            self.assertEqual(utils.hash_path('a', 'c', 'o'),
                             '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEqual(utils.hash_path('a', 'c', 'o', raw_digest=False),
                             '06fbf0b514e5199dfc4e00f42eb5ea83')
            self.assertEqual(utils.hash_path('a', 'c', 'o', raw_digest=True),
                             b'\x06\xfb\xf0\xb5\x14\xe5\x19\x9d\xfcN'
                             b'\x00\xf4.\xb5\xea\x83')
            self.assertRaises(ValueError, utils.hash_path, 'a', object='o')
            utils.HASH_PATH_PREFIX = b'abcdef'
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

        # Unreadable/missing swift.conf causes IOError
        # We mock in case the unit tests are run on a laptop with SAIO,
        # which does have a natural /etc/swift/swift.conf.
        with mock.patch('swift.common.utils.HASH_PATH_PREFIX', b''), \
                mock.patch('swift.common.utils.HASH_PATH_SUFFIX', b''), \
                mock.patch('swift.common.utils.SWIFT_CONF_FILE',
                           '/nosuchfile'), \
                self.assertRaises(IOError):
            utils.validate_hash_conf()

    def _test_validate_hash_conf(self, sections, options, should_raise_error):

        class FakeConfigParser(object):
            def read_file(self, fp):
                pass

            readfp = read_file

            def get(self, section, option):
                if section not in sections:
                    raise NoSectionError('section error')
                elif option not in options:
                    raise NoOptionError('option error', 'this option')
                else:
                    return 'some_option_value'

        with mock.patch('swift.common.utils.HASH_PATH_PREFIX', b''), \
                mock.patch('swift.common.utils.HASH_PATH_SUFFIX', b''), \
                mock.patch('swift.common.utils.SWIFT_CONF_FILE',
                           '/dev/null'), \
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

    def test_drop_privileges(self):
        required_func_calls = ('setgroups', 'setgid', 'setuid')
        mock_os = MockOs(called_funcs=required_func_calls)
        user = getuser()
        user_data = pwd.getpwnam(user)
        self.assertFalse(mock_os.called_funcs)  # sanity check
        # over-ride os with mock
        with mock.patch('swift.common.utils.os', mock_os):
            # exercise the code
            utils.drop_privileges(user)

        for func in required_func_calls:
            self.assertIn(func, mock_os.called_funcs)
        self.assertEqual(user_data[5], mock_os.environ['HOME'])
        groups = {g.gr_gid for g in grp.getgrall() if user in g.gr_mem}
        self.assertEqual(groups, set(mock_os.called_funcs['setgroups'][0]))
        self.assertEqual(user_data[3], mock_os.called_funcs['setgid'][0])
        self.assertEqual(user_data[2], mock_os.called_funcs['setuid'][0])

    def test_drop_privileges_no_setgroups(self):
        required_func_calls = ('geteuid', 'setgid', 'setuid')
        mock_os = MockOs(called_funcs=required_func_calls)
        user = getuser()
        user_data = pwd.getpwnam(user)
        self.assertFalse(mock_os.called_funcs)  # sanity check
        # over-ride os with mock
        with mock.patch('swift.common.utils.os', mock_os):
            # exercise the code
            utils.drop_privileges(user)

        for func in required_func_calls:
            self.assertIn(func, mock_os.called_funcs)
        self.assertNotIn('setgroups', mock_os.called_funcs)
        self.assertEqual(user_data[5], mock_os.environ['HOME'])
        self.assertEqual(user_data[3], mock_os.called_funcs['setgid'][0])
        self.assertEqual(user_data[2], mock_os.called_funcs['setuid'][0])

    def test_clean_up_daemon_hygene(self):
        required_func_calls = ('chdir', 'umask')
        # OSError if trying to get session leader, but setsid() OSError is
        # ignored by the code under test.
        bad_func_calls = ('setsid',)
        mock_os = MockOs(called_funcs=required_func_calls,
                         raise_funcs=bad_func_calls)
        with mock.patch('swift.common.utils.os', mock_os):
            # exercise the code
            utils.clean_up_daemon_hygiene()
        for func in required_func_calls:
            self.assertIn(func, mock_os.called_funcs)
        for func in bad_func_calls:
            self.assertIn(func, mock_os.called_funcs)
        self.assertEqual('/', mock_os.called_funcs['chdir'][0])
        self.assertEqual(0o22, mock_os.called_funcs['umask'][0])

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
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'ratelimit_sleep\(\) is deprecated')

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
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'ratelimit_sleep\(\) is deprecated')

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
        with warnings.catch_warnings():
            warnings.filterwarnings(
                'ignore', r'ratelimit_sleep\(\) is deprecated')

            def testfunc():
                running_time = 0
                sleeps = [0] * 7 + [.2] * 3 + [0] * 30
                for i in sleeps:
                    running_time = utils.ratelimit_sleep(running_time, 40,
                                                         rate_buffer=1)
                    time.sleep(i)

            self.verify_under_pseudo_time(testfunc, target_runtime_ms=900)

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
            self.assertIsNone(utils.remove_file(file_name))
            with open(file_name, 'w') as f:
                f.write('1')
            self.assertTrue(os.path.exists(file_name))
            self.assertIsNone(utils.remove_file(file_name))
            self.assertFalse(os.path.exists(file_name))

    def test_remove_directory(self):
        with temptree([]) as t:
            dir_name = os.path.join(t, 'subdir')

            os.mkdir(dir_name)
            self.assertTrue(os.path.isdir(dir_name))
            self.assertIsNone(utils.remove_directory(dir_name))
            self.assertFalse(os.path.exists(dir_name))

            # assert no raise only if it does not exist, or is not empty
            self.assertEqual(os.path.exists(dir_name), False)
            self.assertIsNone(utils.remove_directory(dir_name))

            _m_rmdir = mock.Mock(
                side_effect=OSError(errno.ENOTEMPTY,
                                    os.strerror(errno.ENOTEMPTY)))
            with mock.patch('swift.common.utils.os.rmdir', _m_rmdir):
                self.assertIsNone(utils.remove_directory(dir_name))

            _m_rmdir = mock.Mock(
                side_effect=OSError(errno.EPERM, os.strerror(errno.EPERM)))
            with mock.patch('swift.common.utils.os.rmdir', _m_rmdir):
                self.assertRaises(OSError, utils.remove_directory, dir_name)

    @with_tempdir
    def test_is_file_older(self, tempdir):
        ts = utils.Timestamp(time.time() - 100000)
        file_name = os.path.join(tempdir, '%s.data' % ts.internal)
        # assert no raise
        self.assertFalse(os.path.exists(file_name))
        self.assertTrue(utils.is_file_older(file_name, 0))
        self.assertFalse(utils.is_file_older(file_name, 1))

        with open(file_name, 'w') as f:
            f.write('1')
        self.assertTrue(os.path.exists(file_name))
        self.assertTrue(utils.is_file_older(file_name, 0))
        # check that timestamp in file name is not relevant
        self.assertFalse(utils.is_file_older(file_name, 50000))
        time.sleep(0.01)
        self.assertTrue(utils.is_file_older(file_name, 0.009))

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
            logger = debug_logger()
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
        self.assertIsNone(ts)
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
        self.assertIsNone(ts)
        ts = utils.get_trans_id_time('garbage')
        self.assertIsNone(ts)
        ts = utils.get_trans_id_time('tx1df4ff4f55ea45f7b2ec2-almostright')
        self.assertIsNone(ts)

    def test_lock_file(self):
        flags = os.O_CREAT | os.O_RDWR
        with NamedTemporaryFile(delete=False) as nt:
            nt.write(b"test string")
            nt.flush()
            nt.close()
            with utils.lock_file(nt.name, unlink=False) as f:
                self.assertEqual(f.read(), b"test string")
                # we have a lock, now let's try to get a newer one
                fd = os.open(nt.name, flags)
                self.assertRaises(IOError, fcntl.flock, fd,
                                  fcntl.LOCK_EX | fcntl.LOCK_NB)

            with utils.lock_file(nt.name, unlink=False, append=True) as f:
                f.seek(0)
                self.assertEqual(f.read(), b"test string")
                f.seek(0)
                f.write(b"\nanother string")
                f.flush()
                f.seek(0)
                self.assertEqual(f.read(), b"test string\nanother string")

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
                self.assertEqual(f.read(), b"test string\nanother string")
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

    @with_tempdir
    def test_ismount_path_does_not_exist(self, tmpdir):
        self.assertFalse(utils.ismount(os.path.join(tmpdir, 'bar')))

    @with_tempdir
    def test_ismount_path_not_mount(self, tmpdir):
        self.assertFalse(utils.ismount(tmpdir))

    @with_tempdir
    def test_ismount_path_error(self, tmpdir):

        def _mock_os_lstat(path):
            raise OSError(13, "foo")

        with patch("os.lstat", _mock_os_lstat):
            # Raises exception with _raw -- see next test.
            utils.ismount(tmpdir)

    @with_tempdir
    def test_ismount_raw_path_error(self, tmpdir):

        def _mock_os_lstat(path):
            raise OSError(13, "foo")

        with patch("os.lstat", _mock_os_lstat):
            self.assertRaises(OSError, utils.ismount_raw, tmpdir)

    @with_tempdir
    def test_ismount_path_is_symlink(self, tmpdir):
        link = os.path.join(tmpdir, "tmp")
        rdir = os.path.join(tmpdir, "realtmp")
        os.mkdir(rdir)
        os.symlink(rdir, link)
        self.assertFalse(utils.ismount(link))

        # Can add a stubfile to make it pass
        with open(os.path.join(link, ".ismount"), "w"):
            pass
        self.assertTrue(utils.ismount(link))

    def test_ismount_path_is_root(self):
        self.assertTrue(utils.ismount('/'))

    @with_tempdir
    def test_ismount_parent_path_error(self, tmpdir):

        _os_lstat = os.lstat

        def _mock_os_lstat(path):
            if path.endswith(".."):
                raise OSError(13, "foo")
            else:
                return _os_lstat(path)

        with patch("os.lstat", _mock_os_lstat):
            # Raises exception with _raw -- see next test.
            utils.ismount(tmpdir)

    @with_tempdir
    def test_ismount_raw_parent_path_error(self, tmpdir):

        _os_lstat = os.lstat

        def _mock_os_lstat(path):
            if path.endswith(".."):
                raise OSError(13, "foo")
            else:
                return _os_lstat(path)

        with patch("os.lstat", _mock_os_lstat):
            self.assertRaises(OSError, utils.ismount_raw, tmpdir)

    @with_tempdir
    def test_ismount_successes_dev(self, tmpdir):

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

        with patch("os.lstat", _mock_os_lstat):
            self.assertTrue(utils.ismount(tmpdir))

    @with_tempdir
    def test_ismount_successes_ino(self, tmpdir):

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

        with patch("os.lstat", _mock_os_lstat):
            self.assertTrue(utils.ismount(tmpdir))

    @with_tempdir
    def test_ismount_successes_stubfile(self, tmpdir):
        fname = os.path.join(tmpdir, ".ismount")
        with open(fname, "w") as stubfile:
            stubfile.write("")
        self.assertTrue(utils.ismount(tmpdir))

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
        self.assertEqual(
            utils.parse_content_type(r'text/plain; x=a/b; y'),
            ('text/plain', [('x', 'a'), ('y', '')]))

        self.assertEqual(
            utils.parse_content_type(r'text/plain; x=a/b; y', strict=True),
            ('text/plain', [('x', 'a'), ('y', '')]))
        self.assertEqual(
            utils.parse_content_type(r'text/plain; x=a/b; y', strict=False),
            ('text/plain', [('x', 'a/b'), ('y', '')]))

    def test_parse_header(self):
        self.assertEqual(
            utils.parse_header('text/plain'), ('text/plain', {}))
        self.assertEqual(
            utils.parse_header('text/plain;'), ('text/plain', {}))
        self.assertEqual(
            utils.parse_header(r'text/plain; x=a/b; y  =  z'),
            ('text/plain', {'x': 'a/b', 'y': 'z'}))
        self.assertEqual(
            utils.parse_header(r'text/plain; x=a/b; y'),
            ('text/plain', {'x': 'a/b', 'y': ''}))
        self.assertEqual(
            utils.parse_header('etag; x=a/b; y'),
            ('etag', {'x': 'a/b', 'y': ''}))

    def test_parse_headers_chars_in_params(self):
        def do_test(val):
            self.assertEqual(
                utils.parse_header('text/plain; x=a%sb' % val),
                ('text/plain', {'x': 'a%sb' % val}))

        do_test('\N{SNOWMAN}')
        do_test('\\')
        do_test('%')
        do_test('-')
        do_test('-')
        do_test('&')
        # wsgi_quote'd null character is ok...
        do_test('%00')

    def test_parse_header_non_token_chars_in_params(self):
        def do_test(val):
            # character terminates individual param parsing...
            self.assertEqual(
                utils.parse_header('text/plain; x=a%sb; y=z' % val),
                ('text/plain', {'x': 'a', 'y': 'z'}),
                'val=%s' % val
            )

        non_token_chars = '()<>@,:[]?={}\x00"'

        for ch in non_token_chars:
            do_test(ch)

        do_test(' space  oddity ')

    def test_parse_header_quoted_string_in_params(self):
        def do_test(val):
            self.assertEqual(
                utils.parse_header('text/plain; x="%s"; y=z' % val),
                ('text/plain', {'x': '"%s"' % val, 'y': 'z'}),
                'val=%s' % val
            )

        non_token_chars = '()<>@,:[]?={}\x00'

        for ch in non_token_chars:
            do_test(ch)

        do_test(' space  oddity ')

    def test_override_bytes_from_content_type(self):
        listing_dict = {
            'bytes': 1234, 'hash': 'asdf', 'name': 'zxcv',
            'content_type': 'text/plain; hello="world"; swift_bytes=15'}
        utils.override_bytes_from_content_type(listing_dict,
                                               logger=debug_logger())
        self.assertEqual(listing_dict['bytes'], 15)
        self.assertEqual(listing_dict['content_type'],
                         'text/plain;hello="world"')

        listing_dict = {
            'bytes': 1234, 'hash': 'asdf', 'name': 'zxcv',
            'content_type': 'text/plain; hello="world"; swift_bytes=hey'}
        utils.override_bytes_from_content_type(listing_dict,
                                               logger=debug_logger())
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

    def test_get_valid_utf8_str(self):
        def do_test(input_value, expected):
            actual = utils.get_valid_utf8_str(input_value)
            self.assertEqual(expected, actual)
            self.assertIsInstance(actual, bytes)
            actual.decode('utf-8')

        do_test(b'abc', b'abc')
        do_test(u'abc', b'abc')
        do_test(u'\uc77c\uc601', b'\xec\x9d\xbc\xec\x98\x81')
        do_test(b'\xec\x9d\xbc\xec\x98\x81', b'\xec\x9d\xbc\xec\x98\x81')

        # test some invalid UTF-8
        do_test(b'\xec\x9d\xbc\xec\x98', b'\xec\x9d\xbc\xef\xbf\xbd')

        # check surrogate pairs, too
        do_test(u'\U0001f0a1', b'\xf0\x9f\x82\xa1'),
        do_test(u'\uD83C\uDCA1', b'\xf0\x9f\x82\xa1'),
        do_test(b'\xf0\x9f\x82\xa1', b'\xf0\x9f\x82\xa1'),
        do_test(b'\xed\xa0\xbc\xed\xb2\xa1', b'\xf0\x9f\x82\xa1'),

    def test_quote_bytes(self):
        self.assertEqual(b'/v1/a/c3/subdirx/',
                         utils.quote(b'/v1/a/c3/subdirx/'))
        self.assertEqual(b'/v1/a%26b/c3/subdirx/',
                         utils.quote(b'/v1/a&b/c3/subdirx/'))
        self.assertEqual(b'%2Fv1%2Fa&b%2Fc3%2Fsubdirx%2F',
                         utils.quote(b'/v1/a&b/c3/subdirx/', safe='&'))
        self.assertEqual(b'abc_%EC%9D%BC%EC%98%81',
                         utils.quote(u'abc_\uc77c\uc601'.encode('utf8')))
        # Invalid utf8 is parsed as latin1, then re-encoded as utf8??
        self.assertEqual(b'%EF%BF%BD%EF%BF%BD%EC%BC%9D%EF%BF%BD',
                         utils.quote(u'\uc77c\uc601'.encode('utf8')[::-1]))

    def test_quote_unicode(self):
        self.assertEqual(u'/v1/a/c3/subdirx/',
                         utils.quote(u'/v1/a/c3/subdirx/'))
        self.assertEqual(u'/v1/a%26b/c3/subdirx/',
                         utils.quote(u'/v1/a&b/c3/subdirx/'))
        self.assertEqual(u'%2Fv1%2Fa&b%2Fc3%2Fsubdirx%2F',
                         utils.quote(u'/v1/a&b/c3/subdirx/', safe='&'))
        self.assertEqual(u'abc_%EC%9D%BC%EC%98%81',
                         utils.quote(u'abc_\uc77c\uc601'))

    def test_parse_override_options(self):
        # When override_<thing> is passed in, it takes precedence.
        opts = utils.parse_override_options(
            override_policies=[0, 1],
            override_devices=['sda', 'sdb'],
            override_partitions=[100, 200],
            policies='0,1,2,3',
            devices='sda,sdb,sdc,sdd',
            partitions='100,200,300,400')
        self.assertEqual(opts.policies, [0, 1])
        self.assertEqual(opts.devices, ['sda', 'sdb'])
        self.assertEqual(opts.partitions, [100, 200])

        # When override_<thing> is passed in, it applies even in run-once
        # mode.
        opts = utils.parse_override_options(
            once=True,
            override_policies=[0, 1],
            override_devices=['sda', 'sdb'],
            override_partitions=[100, 200],
            policies='0,1,2,3',
            devices='sda,sdb,sdc,sdd',
            partitions='100,200,300,400')
        self.assertEqual(opts.policies, [0, 1])
        self.assertEqual(opts.devices, ['sda', 'sdb'])
        self.assertEqual(opts.partitions, [100, 200])

        # In run-once mode, we honor the passed-in overrides.
        opts = utils.parse_override_options(
            once=True,
            policies='0,1,2,3',
            devices='sda,sdb,sdc,sdd',
            partitions='100,200,300,400')
        self.assertEqual(opts.policies, [0, 1, 2, 3])
        self.assertEqual(opts.devices, ['sda', 'sdb', 'sdc', 'sdd'])
        self.assertEqual(opts.partitions, [100, 200, 300, 400])

        # In run-forever mode, we ignore the passed-in overrides.
        opts = utils.parse_override_options(
            policies='0,1,2,3',
            devices='sda,sdb,sdc,sdd',
            partitions='100,200,300,400')
        self.assertEqual(opts.policies, [])
        self.assertEqual(opts.devices, [])
        self.assertEqual(opts.partitions, [])

    def test_cache_from_env(self):
        # should never get logging when swift.cache is found
        env = {'swift.cache': 42}
        logger = debug_logger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(42, utils.cache_from_env(env))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))
        logger = debug_logger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(42, utils.cache_from_env(env, False))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))
        logger = debug_logger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertEqual(42, utils.cache_from_env(env, True))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))

        # check allow_none controls logging when swift.cache is not found
        err_msg = 'ERROR: swift.cache could not be found in env!'
        env = {}
        logger = debug_logger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertIsNone(utils.cache_from_env(env))
            self.assertTrue(err_msg in logger.get_lines_for_level('error'))
        logger = debug_logger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertIsNone(utils.cache_from_env(env, False))
            self.assertTrue(err_msg in logger.get_lines_for_level('error'))
        logger = debug_logger()
        with mock.patch('swift.common.utils.logging', logger):
            self.assertIsNone(utils.cache_from_env(env, True))
            self.assertEqual(0, len(logger.get_lines_for_level('error')))

    @with_tempdir
    def test_fsync_dir(self, tempdir):

        fd = None
        try:
            fd, temppath = tempfile.mkstemp(dir=tempdir)

            _mock_fsync = mock.Mock()
            _mock_close = mock.Mock()

            with patch('swift.common.utils.fsync', _mock_fsync):
                with patch('os.close', _mock_close):
                    utils.fsync_dir(tempdir)
            self.assertTrue(_mock_fsync.called)
            self.assertTrue(_mock_close.called)
            self.assertIsInstance(_mock_fsync.call_args[0][0], int)
            self.assertEqual(_mock_fsync.call_args[0][0],
                             _mock_close.call_args[0][0])

            # Not a directory - arg is file path
            self.assertRaises(OSError, utils.fsync_dir, temppath)

            logger = debug_logger()

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

    @with_tempdir
    def test_renamer_with_fsync_dir(self, tempdir):
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

    @with_tempdir
    def test_makedirs_count(self, tempdir):
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

    def test_find_namespace(self):
        ts = utils.Timestamp.now().internal
        start = utils.ShardRange('a/-a', ts, '', 'a')
        atof = utils.ShardRange('a/a-f', ts, 'a', 'f')
        ftol = utils.ShardRange('a/f-l', ts, 'f', 'l')
        ltor = utils.ShardRange('a/l-r', ts, 'l', 'r')
        rtoz = utils.ShardRange('a/r-z', ts, 'r', 'z')
        end = utils.ShardRange('a/z-', ts, 'z', '')
        ranges = [start, atof, ftol, ltor, rtoz, end]

        found = utils.find_namespace('', ranges)
        self.assertEqual(found, None)
        found = utils.find_namespace(' ', ranges)
        self.assertEqual(found, start)
        found = utils.find_namespace(' ', ranges[1:])
        self.assertEqual(found, None)
        found = utils.find_namespace('b', ranges)
        self.assertEqual(found, atof)
        found = utils.find_namespace('f', ranges)
        self.assertEqual(found, atof)
        found = utils.find_namespace('f\x00', ranges)
        self.assertEqual(found, ftol)
        found = utils.find_namespace('x', ranges)
        self.assertEqual(found, rtoz)
        found = utils.find_namespace('r', ranges)
        self.assertEqual(found, ltor)
        found = utils.find_namespace('}', ranges)
        self.assertEqual(found, end)
        found = utils.find_namespace('}', ranges[:-1])
        self.assertEqual(found, None)
        # remove l-r from list of ranges and try and find a shard range for an
        # item in that range.
        found = utils.find_namespace('p', ranges[:-3] + ranges[-2:])
        self.assertEqual(found, None)

        # add some sub-shards; a sub-shard's state is less than its parent
        # while the parent is undeleted, so insert these ahead of the
        # overlapping parent in the list of ranges
        ftoh = utils.ShardRange('a/f-h', ts, 'f', 'h')
        htok = utils.ShardRange('a/h-k', ts, 'h', 'k')

        overlapping_ranges = ranges[:2] + [ftoh, htok] + ranges[2:]
        found = utils.find_namespace('g', overlapping_ranges)
        self.assertEqual(found, ftoh)
        found = utils.find_namespace('h', overlapping_ranges)
        self.assertEqual(found, ftoh)
        found = utils.find_namespace('k', overlapping_ranges)
        self.assertEqual(found, htok)
        found = utils.find_namespace('l', overlapping_ranges)
        self.assertEqual(found, ftol)
        found = utils.find_namespace('m', overlapping_ranges)
        self.assertEqual(found, ltor)

        ktol = utils.ShardRange('a/k-l', ts, 'k', 'l')
        overlapping_ranges = ranges[:2] + [ftoh, htok, ktol] + ranges[2:]
        found = utils.find_namespace('l', overlapping_ranges)
        self.assertEqual(found, ktol)

    def test_parse_db_filename(self):
        actual = utils.parse_db_filename('hash.db')
        self.assertEqual(('hash', None, '.db'), actual)
        actual = utils.parse_db_filename('hash_1234567890.12345.db')
        self.assertEqual(('hash', '1234567890.12345', '.db'), actual)
        actual = utils.parse_db_filename(
            '/dev/containers/part/ash/hash/hash_1234567890.12345.db')
        self.assertEqual(('hash', '1234567890.12345', '.db'), actual)
        self.assertRaises(ValueError, utils.parse_db_filename, '/path/to/dir/')
        # These shouldn't come up in practice; included for completeness
        self.assertEqual(utils.parse_db_filename('hashunder_.db'),
                         ('hashunder', '', '.db'))
        self.assertEqual(utils.parse_db_filename('lots_of_underscores.db'),
                         ('lots', 'of', '.db'))

    def test_make_db_file_path(self):
        epoch = utils.Timestamp.now()
        actual = utils.make_db_file_path('hash.db', epoch)
        self.assertEqual('hash_%s.db' % epoch.internal, actual)

        actual = utils.make_db_file_path('hash_oldepoch.db', epoch)
        self.assertEqual('hash_%s.db' % epoch.internal, actual)

        actual = utils.make_db_file_path('/path/to/hash.db', epoch)
        self.assertEqual('/path/to/hash_%s.db' % epoch.internal, actual)

        epoch = utils.Timestamp.now()
        actual = utils.make_db_file_path(actual, epoch)
        self.assertEqual('/path/to/hash_%s.db' % epoch.internal, actual)

        # None strips epoch
        self.assertEqual('hash.db', utils.make_db_file_path('hash.db', None))
        self.assertEqual('/path/to/hash.db', utils.make_db_file_path(
            '/path/to/hash_withepoch.db', None))

        # epochs shouldn't have offsets
        epoch = utils.Timestamp.now(offset=10)
        actual = utils.make_db_file_path(actual, epoch)
        self.assertEqual('/path/to/hash_%s.db' % epoch.normal, actual)

        self.assertRaises(ValueError, utils.make_db_file_path,
                          '/path/to/hash.db', 'bad epoch')

    @requires_o_tmpfile_support_in_tmp
    @with_tempdir
    def test_link_fd_to_path_linkat_success(self, tempdir):
        fd = os.open(tempdir, utils.O_TMPFILE | os.O_WRONLY)
        data = b"I'm whatever Gotham needs me to be"
        _m_fsync_dir = mock.Mock()
        try:
            os.write(fd, data)
            # fd is O_WRONLY
            self.assertRaises(OSError, os.read, fd, 1)
            file_path = os.path.join(tempdir, uuid4().hex)
            with mock.patch('swift.common.utils.fsync_dir', _m_fsync_dir):
                utils.link_fd_to_path(fd, file_path, 1)
            with open(file_path, 'rb') as f:
                self.assertEqual(f.read(), data)
            self.assertEqual(_m_fsync_dir.call_count, 2)
        finally:
            os.close(fd)

    @requires_o_tmpfile_support_in_tmp
    @with_tempdir
    def test_link_fd_to_path_target_exists(self, tempdir):
        # Create and write to a file
        fd, path = tempfile.mkstemp(dir=tempdir)
        os.write(fd, b"hello world")
        os.fsync(fd)
        os.close(fd)
        self.assertTrue(os.path.exists(path))

        fd = os.open(tempdir, utils.O_TMPFILE | os.O_WRONLY)
        try:
            os.write(fd, b"bye world")
            os.fsync(fd)
            utils.link_fd_to_path(fd, path, 0, fsync=False)
            # Original file now should have been over-written
            with open(path, 'rb') as f:
                self.assertEqual(f.read(), b"bye world")
        finally:
            os.close(fd)

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

    def test_link_fd_to_path_runs_out_of_retries(self):
        _m_linkat = mock.Mock(
            side_effect=IOError(errno.ENOENT, os.strerror(errno.ENOENT)))
        with mock.patch('swift.common.utils.linkat', _m_linkat), \
                self.assertRaises(IOError) as caught:
            utils.link_fd_to_path(0, '/path', 1)
        self.assertEqual(caught.exception.errno, errno.ENOENT)
        self.assertEqual(3, len(_m_linkat.mock_calls))

    @requires_o_tmpfile_support_in_tmp
    @with_tempdir
    def test_linkat_race_dir_not_exists(self, tempdir):
        target_dir = os.path.join(tempdir, uuid4().hex)
        target_path = os.path.join(target_dir, uuid4().hex)
        os.mkdir(target_dir)
        fd = os.open(target_dir, utils.O_TMPFILE | os.O_WRONLY)
        try:
            # Simulating directory deletion by other backend process
            os.rmdir(target_dir)
            self.assertFalse(os.path.exists(target_dir))
            utils.link_fd_to_path(fd, target_path, 1)
            self.assertTrue(os.path.exists(target_dir))
            self.assertTrue(os.path.exists(target_path))
        finally:
            os.close(fd)

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

    def test_strict_b64decode(self):
        expectations = {
            None: ValueError,
            0: ValueError,
            b'': b'',
            u'': b'',
            b'A': ValueError,
            b'AA': ValueError,
            b'AAA': ValueError,
            b'AAAA': b'\x00\x00\x00',
            u'AAAA': b'\x00\x00\x00',
            b'////': b'\xff\xff\xff',
            u'////': b'\xff\xff\xff',
            b'A===': ValueError,
            b'AA==': b'\x00',
            b'AAA=': b'\x00\x00',
            b' AAAA': ValueError,
            b'AAAA ': ValueError,
            b'AAAA============': b'\x00\x00\x00',
            b'AA&AA==': ValueError,
            b'====': b'',
        }

        failures = []
        for value, expected in expectations.items():
            try:
                result = utils.strict_b64decode(value)
            except Exception as e:
                if inspect.isclass(expected) and issubclass(
                        expected, Exception):
                    if not isinstance(e, expected):
                        failures.append('%r raised %r (expected to raise %r)' %
                                        (value, e, expected))
                else:
                    failures.append('%r raised %r (expected to return %r)' %
                                    (value, e, expected))
            else:
                if inspect.isclass(expected) and issubclass(
                        expected, Exception):
                    failures.append('%r => %r (expected to raise %r)' %
                                    (value, result, expected))
                elif result != expected:
                    failures.append('%r => %r (expected %r)' % (
                        value, result, expected))
        if failures:
            self.fail('Invalid results from pure function:\n%s' %
                      '\n'.join(failures))

    def test_strict_b64decode_allow_line_breaks(self):
        with self.assertRaises(ValueError):
            utils.strict_b64decode(b'AA\nA=')
        self.assertEqual(
            b'\x00\x00',
            utils.strict_b64decode(b'AA\nA=', allow_line_breaks=True))

    def test_strict_b64decode_exact_size(self):
        self.assertEqual(b'\x00\x00',
                         utils.strict_b64decode(b'AAA='))
        self.assertEqual(b'\x00\x00',
                         utils.strict_b64decode(b'AAA=', exact_size=2))
        with self.assertRaises(ValueError):
            utils.strict_b64decode(b'AAA=', exact_size=1)
        with self.assertRaises(ValueError):
            utils.strict_b64decode(b'AAA=', exact_size=3)

    def test_base64_str(self):
        self.assertEqual('Zm9v', utils.base64_str(b'foo'))
        self.assertEqual('Zm9vZA==', utils.base64_str(b'food'))
        self.assertEqual('IGZvbw==', utils.base64_str(b' foo'))

    def test_cap_length(self):
        self.assertEqual(utils.cap_length(None, 3), None)
        self.assertEqual(utils.cap_length('', 3), '')
        self.assertEqual(utils.cap_length('asdf', 3), 'asd...')
        self.assertEqual(utils.cap_length('asdf', 5), 'asdf')

        self.assertEqual(utils.cap_length(b'asdf', 3), b'asd...')
        self.assertEqual(utils.cap_length(b'asdf', 5), b'asdf')

    def test_get_partition_for_hash(self):
        hex_hash = 'af088baea4806dcaba30bf07d9e64c77'
        self.assertEqual(43, utils.get_partition_for_hash(hex_hash, 6))
        self.assertEqual(87, utils.get_partition_for_hash(hex_hash, 7))
        self.assertEqual(350, utils.get_partition_for_hash(hex_hash, 9))
        self.assertEqual(700, utils.get_partition_for_hash(hex_hash, 10))
        self.assertEqual(1400, utils.get_partition_for_hash(hex_hash, 11))
        self.assertEqual(0, utils.get_partition_for_hash(hex_hash, 0))
        self.assertEqual(0, utils.get_partition_for_hash(hex_hash, -1))

    def test_get_partition_from_path(self):
        def do_test(path):
            self.assertEqual(utils.get_partition_from_path('/s/n', path), 70)
            self.assertEqual(utils.get_partition_from_path('/s/n/', path), 70)
            path += '/'
            self.assertEqual(utils.get_partition_from_path('/s/n', path), 70)
            self.assertEqual(utils.get_partition_from_path('/s/n/', path), 70)

        do_test('/s/n/d/o/70/c77/af088baea4806dcaba30bf07d9e64c77/f')
        # also works with a hashdir
        do_test('/s/n/d/o/70/c77/af088baea4806dcaba30bf07d9e64c77')
        # or suffix dir
        do_test('/s/n/d/o/70/c77')
        # or even the part dir itself
        do_test('/s/n/d/o/70')

    def test_replace_partition_in_path(self):
        # Check for new part = part * 2
        old = '/s/n/d/o/700/c77/af088baea4806dcaba30bf07d9e64c77/f'
        new = '/s/n/d/o/1400/c77/af088baea4806dcaba30bf07d9e64c77/f'
        # Expected outcome
        self.assertEqual(utils.replace_partition_in_path('/s/n/', old, 11),
                         new)

        # Make sure there is no change if the part power didn't change
        self.assertEqual(utils.replace_partition_in_path('/s/n', old, 10), old)
        self.assertEqual(utils.replace_partition_in_path('/s/n/', new, 11),
                         new)

        # Check for new part = part * 2 + 1
        old = '/s/n/d/o/693/c77/ad708baea4806dcaba30bf07d9e64c77/f'
        new = '/s/n/d/o/1387/c77/ad708baea4806dcaba30bf07d9e64c77/f'

        # Expected outcome
        self.assertEqual(utils.replace_partition_in_path('/s/n', old, 11), new)

        # Make sure there is no change if the part power didn't change
        self.assertEqual(utils.replace_partition_in_path('/s/n', old, 10), old)
        self.assertEqual(utils.replace_partition_in_path('/s/n/', new, 11),
                         new)

        # check hash_dir
        old = '/s/n/d/o/700/c77/af088baea4806dcaba30bf07d9e64c77'
        exp = '/s/n/d/o/1400/c77/af088baea4806dcaba30bf07d9e64c77'
        actual = utils.replace_partition_in_path('/s/n', old, 11)
        self.assertEqual(exp, actual)
        actual = utils.replace_partition_in_path('/s/n', exp, 11)
        self.assertEqual(exp, actual)

        # check longer devices path
        old = '/s/n/1/2/d/o/700/c77/af088baea4806dcaba30bf07d9e64c77'
        exp = '/s/n/1/2/d/o/1400/c77/af088baea4806dcaba30bf07d9e64c77'
        actual = utils.replace_partition_in_path('/s/n/1/2', old, 11)
        self.assertEqual(exp, actual)
        actual = utils.replace_partition_in_path('/s/n/1/2', exp, 11)
        self.assertEqual(exp, actual)

        # check empty devices path
        old = '/d/o/700/c77/af088baea4806dcaba30bf07d9e64c77'
        exp = '/d/o/1400/c77/af088baea4806dcaba30bf07d9e64c77'
        actual = utils.replace_partition_in_path('', old, 11)
        self.assertEqual(exp, actual)
        actual = utils.replace_partition_in_path('', exp, 11)
        self.assertEqual(exp, actual)

        # check path validation
        path = '/s/n/d/o/693/c77/ad708baea4806dcaba30bf07d9e64c77/f'
        with self.assertRaises(ValueError) as cm:
            utils.replace_partition_in_path('/s/n1', path, 11)
        self.assertEqual(
            "Path '/s/n/d/o/693/c77/ad708baea4806dcaba30bf07d9e64c77/f' "
            "is not under device dir '/s/n1'", str(cm.exception))

        # check path validation - path lacks leading /
        path = 's/n/d/o/693/c77/ad708baea4806dcaba30bf07d9e64c77/f'
        with self.assertRaises(ValueError) as cm:
            utils.replace_partition_in_path('/s/n', path, 11)
        self.assertEqual(
            "Path 's/n/d/o/693/c77/ad708baea4806dcaba30bf07d9e64c77/f' "
            "is not under device dir '/s/n'", str(cm.exception))

    def test_round_robin_iter(self):
        it1 = iter([1, 2, 3])
        it2 = iter([4, 5])
        it3 = iter([6, 7, 8, 9])
        it4 = iter([])

        rr_its = utils.round_robin_iter([it1, it2, it3, it4])
        got = list(rr_its)

        # Expect that items get fetched in a round-robin fashion from the
        # iterators
        self.assertListEqual([1, 4, 6, 2, 5, 7, 3, 8, 9], got)

    @with_tempdir
    def test_get_db_files(self, tempdir):
        dbdir = os.path.join(tempdir, 'dbdir')
        self.assertEqual([], utils.get_db_files(dbdir))
        path_1 = os.path.join(dbdir, 'dbfile.db')
        self.assertEqual([], utils.get_db_files(path_1))
        os.mkdir(dbdir)
        self.assertEqual([], utils.get_db_files(path_1))
        with open(path_1, 'wb'):
            pass
        self.assertEqual([path_1], utils.get_db_files(path_1))

        path_2 = os.path.join(dbdir, 'dbfile_2.db')
        self.assertEqual([path_1], utils.get_db_files(path_2))

        with open(path_2, 'wb'):
            pass

        self.assertEqual([path_1, path_2], utils.get_db_files(path_1))
        self.assertEqual([path_1, path_2], utils.get_db_files(path_2))

        path_3 = os.path.join(dbdir, 'dbfile_3.db')
        self.assertEqual([path_1, path_2], utils.get_db_files(path_3))

        with open(path_3, 'wb'):
            pass

        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(path_1))
        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(path_2))
        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(path_3))

        other_hash = os.path.join(dbdir, 'other.db')
        self.assertEqual([], utils.get_db_files(other_hash))
        other_hash = os.path.join(dbdir, 'other_1.db')
        self.assertEqual([], utils.get_db_files(other_hash))

        pending = os.path.join(dbdir, 'dbfile.pending')
        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(pending))

        with open(pending, 'wb'):
            pass
        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(pending))

        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(path_1))
        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(path_2))
        self.assertEqual([path_1, path_2, path_3], utils.get_db_files(path_3))
        self.assertEqual([], utils.get_db_files(dbdir))

        os.unlink(path_1)
        self.assertEqual([path_2, path_3], utils.get_db_files(path_1))
        self.assertEqual([path_2, path_3], utils.get_db_files(path_2))
        self.assertEqual([path_2, path_3], utils.get_db_files(path_3))

        os.unlink(path_2)
        self.assertEqual([path_3], utils.get_db_files(path_1))
        self.assertEqual([path_3], utils.get_db_files(path_2))
        self.assertEqual([path_3], utils.get_db_files(path_3))

        os.unlink(path_3)
        self.assertEqual([], utils.get_db_files(path_1))
        self.assertEqual([], utils.get_db_files(path_2))
        self.assertEqual([], utils.get_db_files(path_3))
        self.assertEqual([], utils.get_db_files('/path/to/nowhere'))

    def test_get_redirect_data(self):
        ts_now = utils.Timestamp.now()
        headers = {'X-Backend-Redirect-Timestamp': ts_now.internal}
        response = FakeResponse(200, headers, b'')
        self.assertIsNone(utils.get_redirect_data(response))

        headers = {'Location': '/a/c/o',
                   'X-Backend-Redirect-Timestamp': ts_now.internal}
        response = FakeResponse(200, headers, b'')
        path, ts = utils.get_redirect_data(response)
        self.assertEqual('a/c', path)
        self.assertEqual(ts_now, ts)

        headers = {'Location': '/a/c',
                   'X-Backend-Redirect-Timestamp': ts_now.internal}
        response = FakeResponse(200, headers, b'')
        path, ts = utils.get_redirect_data(response)
        self.assertEqual('a/c', path)
        self.assertEqual(ts_now, ts)

        def do_test(headers):
            response = FakeResponse(200, headers, b'')
            with self.assertRaises(ValueError) as cm:
                utils.get_redirect_data(response)
            return cm.exception

        exc = do_test({'Location': '/a',
                       'X-Backend-Redirect-Timestamp': ts_now.internal})
        self.assertIn('Invalid path', str(exc))

        exc = do_test({'Location': '',
                       'X-Backend-Redirect-Timestamp': ts_now.internal})
        self.assertIn('Invalid path', str(exc))

        exc = do_test({'Location': '/a/c',
                       'X-Backend-Redirect-Timestamp': 'bad'})
        self.assertIn('Invalid timestamp', str(exc))

        exc = do_test({'Location': '/a/c'})
        self.assertIn('Invalid timestamp', str(exc))

        exc = do_test({'Location': '/a/c',
                       'X-Backend-Redirect-Timestamp': '-1'})
        self.assertIn('Invalid timestamp', str(exc))

    @unittest.skipIf(sys.version_info >= (3, 8),
                     'pkg_resources loading is only available on python 3.7 '
                     'and earlier')
    @mock.patch('pkg_resources.load_entry_point')
    def test_load_pkg_resource(self, mock_driver):
        tests = {
            ('swift.diskfile', 'egg:swift#replication.fs'):
                ('swift', 'swift.diskfile', 'replication.fs'),
            ('swift.diskfile', 'egg:swift#erasure_coding.fs'):
                ('swift', 'swift.diskfile', 'erasure_coding.fs'),
            ('swift.section', 'egg:swift#thing.other'):
                ('swift', 'swift.section', 'thing.other'),
            ('swift.section', 'swift#thing.other'):
                ('swift', 'swift.section', 'thing.other'),
            ('swift.section', 'thing.other'):
                ('swift', 'swift.section', 'thing.other'),
        }
        for args, expected in tests.items():
            utils.load_pkg_resource(*args)
            mock_driver.assert_called_with(*expected)

        with self.assertRaises(TypeError) as cm:
            args = ('swift.diskfile', 'nog:swift#replication.fs')
            utils.load_pkg_resource(*args)
        self.assertEqual("Unhandled URI scheme: 'nog'", str(cm.exception))

    @unittest.skipIf(sys.version_info < (3, 8),
                     'importlib loading is only available on python 3.8 '
                     'and later')
    @mock.patch('importlib.metadata.distribution')
    def test_load_pkg_resource_importlib(self, mock_driver):
        import importlib.metadata

        class TestEntryPoint(importlib.metadata.EntryPoint):
            def load(self):
                return self.value

        repl_obj = object()
        ec_obj = object()
        other_obj = object()
        mock_driver.return_value.entry_points = [
            TestEntryPoint(group='swift.diskfile',
                           name='replication.fs',
                           value=repl_obj),
            TestEntryPoint(group='swift.diskfile',
                           name='erasure_coding.fs',
                           value=ec_obj),
            TestEntryPoint(group='swift.section',
                           name='thing.other',
                           value=other_obj),
        ]
        tests = {
            ('swift.diskfile', 'egg:swift#replication.fs'): repl_obj,
            ('swift.diskfile', 'egg:swift#erasure_coding.fs'): ec_obj,
            ('swift.section', 'egg:swift#thing.other'): other_obj,
            ('swift.section', 'swift#thing.other'): other_obj,
            ('swift.section', 'thing.other'): other_obj,
        }
        for args, expected in tests.items():
            self.assertIs(expected, utils.load_pkg_resource(*args))
            self.assertEqual(mock_driver.mock_calls, [mock.call('swift')])
            mock_driver.reset_mock()

        with self.assertRaises(TypeError) as cm:
            args = ('swift.diskfile', 'nog:swift#replication.fs')
            utils.load_pkg_resource(*args)
        self.assertEqual("Unhandled URI scheme: 'nog'", str(cm.exception))

        with self.assertRaises(ImportError) as cm:
            args = ('swift.diskfile', 'other.fs')
            utils.load_pkg_resource(*args)
        self.assertEqual(
            "Entry point ('swift.diskfile', 'other.fs') not found",
            str(cm.exception))

        with self.assertRaises(ImportError) as cm:
            args = ('swift.missing', 'thing.other')
            utils.load_pkg_resource(*args)
        self.assertEqual(
            "Entry point ('swift.missing', 'thing.other') not found",
            str(cm.exception))

    @with_tempdir
    def test_systemd_notify(self, tempdir):
        m_sock = mock.Mock(connect=mock.Mock(), sendall=mock.Mock())
        with mock.patch('swift.common.utils.socket.socket',
                        return_value=m_sock) as m_socket:
            # No notification socket
            m_socket.reset_mock()
            m_sock.reset_mock()
            utils.systemd_notify()
            self.assertEqual(m_socket.mock_calls, [
                mock.call(socket.AF_UNIX, socket.SOCK_DGRAM)])
            self.assertEqual(m_sock.connect.mock_calls, [
                mock.call(utils.get_pid_notify_socket())])
            self.assertEqual(m_sock.sendall.mock_calls, [
                mock.call(b'READY=1')])

            # File notification socket
            m_socket.reset_mock()
            m_sock.reset_mock()
            os.environ['NOTIFY_SOCKET'] = 'foobar'
            utils.systemd_notify()
            self.assertEqual(m_socket.mock_calls, [
                mock.call(socket.AF_UNIX, socket.SOCK_DGRAM),
                mock.call(socket.AF_UNIX, socket.SOCK_DGRAM)])
            self.assertEqual(m_sock.connect.mock_calls, [
                mock.call(utils.get_pid_notify_socket()),
                mock.call('foobar')])
            self.assertEqual(m_sock.sendall.mock_calls, [
                mock.call(b'READY=1'),
                mock.call(b'READY=1')])
            # Still there, so we can send STOPPING/RELOADING messages
            self.assertIn('NOTIFY_SOCKET', os.environ)

            m_socket.reset_mock()
            m_sock.reset_mock()
            logger = debug_logger()
            utils.systemd_notify(logger, "RELOADING=1")
            self.assertEqual(m_socket.mock_calls, [
                mock.call(socket.AF_UNIX, socket.SOCK_DGRAM),
                mock.call(socket.AF_UNIX, socket.SOCK_DGRAM)])
            self.assertEqual(m_sock.connect.mock_calls, [
                mock.call(utils.get_pid_notify_socket()),
                mock.call('foobar')])
            self.assertEqual(m_sock.sendall.mock_calls, [
                mock.call(b'RELOADING=1'),
                mock.call(b'RELOADING=1')])

            # Abstract notification socket
            m_socket.reset_mock()
            m_sock.reset_mock()
            os.environ['NOTIFY_SOCKET'] = '@foobar'
            utils.systemd_notify()
            self.assertEqual(m_socket.mock_calls, [
                mock.call(socket.AF_UNIX, socket.SOCK_DGRAM),
                mock.call(socket.AF_UNIX, socket.SOCK_DGRAM)])
            self.assertEqual(m_sock.connect.mock_calls, [
                mock.call(utils.get_pid_notify_socket()),
                mock.call('\x00foobar')])
            self.assertEqual(m_sock.sendall.mock_calls, [
                mock.call(b'READY=1'),
                mock.call(b'READY=1')])
            self.assertIn('NOTIFY_SOCKET', os.environ)

        # Test logger with connection error
        m_sock = mock.Mock(connect=mock.Mock(side_effect=EnvironmentError),
                           sendall=mock.Mock())
        m_logger = mock.Mock(debug=mock.Mock())
        with mock.patch('swift.common.utils.socket.socket',
                        return_value=m_sock) as m_socket:
            os.environ['NOTIFY_SOCKET'] = '@foobar'
            m_sock.reset_mock()
            m_logger.reset_mock()
            utils.systemd_notify()
            self.assertEqual(0, m_sock.sendall.call_count)
            self.assertEqual(0, m_logger.debug.call_count)

            m_sock.reset_mock()
            m_logger.reset_mock()
            utils.systemd_notify(logger=m_logger)
            self.assertEqual(0, m_sock.sendall.call_count)
            self.assertEqual(m_logger.debug.mock_calls, [
                mock.call("Systemd notification failed", exc_info=True),
                mock.call("Systemd notification failed", exc_info=True)])

        # Test it for real
        def do_test_real_socket(socket_address, notify_socket):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            sock.settimeout(5)
            sock.bind(socket_address)
            os.environ['NOTIFY_SOCKET'] = notify_socket
            utils.systemd_notify()
            msg = sock.recv(512)
            sock.close()
            self.assertEqual(msg, b'READY=1')
            self.assertIn('NOTIFY_SOCKET', os.environ)

        # test file socket address
        socket_path = os.path.join(tempdir, 'foobar')
        do_test_real_socket(socket_path, socket_path)
        if sys.platform.startswith('linux'):
            # test abstract socket address
            do_test_real_socket('\0foobar', '@foobar')

            with utils.NotificationServer(os.getpid(), 1) as swift_listener:
                do_test_real_socket('\0foobar', '@foobar')
                self.assertEqual(swift_listener.receive(),
                                 b'READY=1')

    def test_md5_with_data(self):
        if not self.fips_enabled:
            digest = md5(self.md5_test_data).hexdigest()
            self.assertEqual(digest, self.md5_digest)
        else:
            # on a FIPS enabled system, this throws a ValueError:
            # [digital envelope routines: EVP_DigestInit_ex] disabled for FIPS
            self.assertRaises(ValueError, md5, self.md5_test_data)

        if not self.fips_enabled:
            digest = md5(self.md5_test_data, usedforsecurity=True).hexdigest()
            self.assertEqual(digest, self.md5_digest)
        else:
            self.assertRaises(
                ValueError, md5, self.md5_test_data, usedforsecurity=True)

        digest = md5(self.md5_test_data, usedforsecurity=False).hexdigest()
        self.assertEqual(digest, self.md5_digest)

    def test_md5_without_data(self):
        if not self.fips_enabled:
            test_md5 = md5()
            test_md5.update(self.md5_test_data)
            digest = test_md5.hexdigest()
            self.assertEqual(digest, self.md5_digest)
        else:
            self.assertRaises(ValueError, md5)

        if not self.fips_enabled:
            test_md5 = md5(usedforsecurity=True)
            test_md5.update(self.md5_test_data)
            digest = test_md5.hexdigest()
            self.assertEqual(digest, self.md5_digest)
        else:
            self.assertRaises(ValueError, md5, usedforsecurity=True)

        test_md5 = md5(usedforsecurity=False)
        test_md5.update(self.md5_test_data)
        digest = test_md5.hexdigest()
        self.assertEqual(digest, self.md5_digest)

    def test_string_data_raises_type_error(self):
        if not self.fips_enabled:
            self.assertRaises(TypeError, hashlib.md5, u'foo')
            self.assertRaises(TypeError, md5, u'foo')
            self.assertRaises(
                TypeError, md5, u'foo', usedforsecurity=True)
        else:
            self.assertRaises(ValueError, hashlib.md5, u'foo')
            self.assertRaises(ValueError, md5, u'foo')
            self.assertRaises(
                ValueError, md5, u'foo', usedforsecurity=True)

        self.assertRaises(
            TypeError, md5, u'foo', usedforsecurity=False)

    def test_none_data_raises_type_error(self):
        if not self.fips_enabled:
            self.assertRaises(TypeError, hashlib.md5, None)
            self.assertRaises(TypeError, md5, None)
            self.assertRaises(
                TypeError, md5, None, usedforsecurity=True)
        else:
            self.assertRaises(ValueError, hashlib.md5, None)
            self.assertRaises(ValueError, md5, None)
            self.assertRaises(
                ValueError, md5, None, usedforsecurity=True)

        self.assertRaises(
            TypeError, md5, None, usedforsecurity=False)

    def test_get_my_ppid(self):
        self.assertEqual(os.getppid(), utils.get_ppid(os.getpid()))


class TestUnlinkOlder(unittest.TestCase):

    def setUp(self):
        self.tempdir = mkdtemp()
        self.mtime = {}
        self.ts = make_timestamp_iter()

    def tearDown(self):
        rmtree(self.tempdir, ignore_errors=True)

    def touch(self, fpath, mtime=None):
        self.mtime[fpath] = mtime or next(self.ts)
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
        utils.unlink_older_than(path, next(self.ts))

    def test_unlink_older_than_file(self):
        path = os.path.join(self.tempdir, 'some-file')
        self.touch(path)
        with self.assertRaises(OSError) as ctx:
            utils.unlink_older_than(path, next(self.ts))
        self.assertEqual(ctx.exception.errno, errno.ENOTDIR)

    def test_unlink_older_than_now(self):
        self.touch(os.path.join(self.tempdir, 'test'))
        with self.high_resolution_getmtime():
            utils.unlink_older_than(self.tempdir, next(self.ts))
        self.assertEqual([], os.listdir(self.tempdir))

    def test_unlink_not_old_enough(self):
        start = next(self.ts)
        self.touch(os.path.join(self.tempdir, 'test'))
        with self.high_resolution_getmtime():
            utils.unlink_older_than(self.tempdir, start)
        self.assertEqual(['test'], os.listdir(self.tempdir))

    def test_unlink_mixed(self):
        self.touch(os.path.join(self.tempdir, 'first'))
        cutoff = next(self.ts)
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
            utils.unlink_paths_older_than(paths[:2], next(self.ts))
        self.assertEqual(['third'], os.listdir(self.tempdir))

    def test_unlink_empty_paths(self):
        # just make sure it doesn't blow up
        utils.unlink_paths_older_than([], next(self.ts))

    def test_unlink_not_exists_paths(self):
        path = os.path.join(self.tempdir, 'does-not-exist')
        # just make sure it doesn't blow up
        utils.unlink_paths_older_than([path], next(self.ts))


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
        expected = b''.join(in_iter)
        self.assertEqual(utils.FileLikeIter(in_iter).read(), expected)
        self.assertEqual(utils.FileLikeIter(in_iter).read(-1), expected)
        self.assertEqual(utils.FileLikeIter(in_iter).read(None), expected)

    def test_read_empty(self):
        in_iter = [b'abc']
        ip = utils.FileLikeIter(in_iter)
        self.assertEqual(b'abc', ip.read())
        self.assertEqual(b'', ip.read())

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

    def test_readline_size_unlimited(self):
        in_iter = [b'abc', b'd\nef']
        self.assertEqual(
            utils.FileLikeIter(in_iter).readline(-1),
            b'abcd\n')
        self.assertEqual(
            utils.FileLikeIter(in_iter).readline(None),
            b'abcd\n')

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
        lines = utils.FileLikeIter(in_iter).readlines(sizehint=-1)
        self.assertEqual(
            lines,
            [v if v == b'trailing.' else v + b'\n'
             for v in b''.join(in_iter).split(b'\n')])
        lines = utils.FileLikeIter(in_iter).readlines(sizehint=None)
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
        self.assertRaises(ValueError, next, iter_file)
        self.assertRaises(ValueError, iter_file.read)
        self.assertRaises(ValueError, iter_file.readline)
        self.assertRaises(ValueError, iter_file.readlines)
        # Just make sure repeated close calls don't raise an Exception
        iter_file.close()
        self.assertTrue(iter_file.closed)

    def test_get_hub(self):
        # This test mock the eventlet.green.select module without poll
        # as in eventlet > 0.20
        # https://github.com/eventlet/eventlet/commit/614a20462
        # We add __original_module_select to sys.modules to mock usage
        # of eventlet.patcher.original

        class SelectWithPoll(object):
            def poll():
                pass

        class SelectWithoutPoll(object):
            pass

        # Platform with poll() that call get_hub before eventlet patching
        with mock.patch.dict('sys.modules',
                             {'select': SelectWithPoll,
                              '__original_module_select': SelectWithPoll}):
            self.assertEqual(utils.get_hub(), 'poll')

        # Platform with poll() that call get_hub after eventlet patching
        with mock.patch.dict('sys.modules',
                             {'select': SelectWithoutPoll,
                              '__original_module_select': SelectWithPoll}):
            self.assertEqual(utils.get_hub(), 'poll')

        # Platform without poll() -- before or after patching doesn't matter
        with mock.patch.dict('sys.modules',
                             {'select': SelectWithoutPoll,
                              '__original_module_select': SelectWithoutPoll}):
            self.assertEqual(utils.get_hub(), 'selects')


class TestInputProxy(unittest.TestCase):
    def test_read_all(self):
        self.assertEqual(utils.InputProxy(io.BytesIO(b'abc')).read(), b'abc')
        self.assertEqual(utils.InputProxy(io.BytesIO(b'abc')).read(-1), b'abc')
        self.assertEqual(
            utils.InputProxy(io.BytesIO(b'abc')).read(None), b'abc')

    def test_read_size(self):
        self.assertEqual(utils.InputProxy(io.BytesIO(b'abc')).read(0), b'')
        self.assertEqual(utils.InputProxy(io.BytesIO(b'abc')).read(2), b'ab')
        self.assertEqual(utils.InputProxy(io.BytesIO(b'abc')).read(4), b'abc')

    def test_readline(self):
        ip = utils.InputProxy(io.BytesIO(b'ab\nc'))
        self.assertEqual(ip.readline(), b'ab\n')
        self.assertFalse(ip.client_disconnect)

    def test_bytes_received(self):
        ip = utils.InputProxy(io.BytesIO(b'ab\ncdef'))
        ip.readline()
        self.assertEqual(3, ip.bytes_received)
        ip.read(2)
        self.assertEqual(5, ip.bytes_received)
        ip.read(99)
        self.assertEqual(7, ip.bytes_received)

    def test_close(self):
        utils.InputProxy(object()).close()  # safe

        fake = mock.MagicMock()
        fake.close = mock.MagicMock()
        ip = (utils.InputProxy(fake))
        ip.close()
        self.assertEqual([mock.call()], fake.close.call_args_list)
        self.assertFalse(ip.client_disconnect)

    def test_read_piecemeal_chunk_update(self):
        ip = utils.InputProxy(io.BytesIO(b'abc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.read(1)
            ip.read(2)
            ip.read(1)
            ip.read(1)
        self.assertEqual([mock.call(b'a', False),
                          mock.call(b'bc', False),
                          mock.call(b'', True),
                          mock.call(b'', True)], mocked.call_args_list)

    def test_read_unlimited_chunk_update(self):
        ip = utils.InputProxy(io.BytesIO(b'abc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.read()
            ip.read()
        self.assertEqual([mock.call(b'abc', True),
                          mock.call(b'', True)], mocked.call_args_list)
        ip = utils.InputProxy(io.BytesIO(b'abc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.read(None)
            ip.read(None)
        self.assertEqual([mock.call(b'abc', True),
                          mock.call(b'', True)], mocked.call_args_list)
        ip = utils.InputProxy(io.BytesIO(b'abc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.read(-1)
            ip.read(-1)
        self.assertEqual([mock.call(b'abc', True),
                          mock.call(b'', True)], mocked.call_args_list)

    def test_readline_piecemeal_chunk_update(self):
        ip = utils.InputProxy(io.BytesIO(b'ab\nc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.readline(3)
            ip.readline(1)  # read to exact length
            ip.readline(1)
        self.assertEqual([mock.call(b'ab\n', False),
                          mock.call(b'c', False),
                          mock.call(b'', True)], mocked.call_args_list)
        ip = utils.InputProxy(io.BytesIO(b'ab\nc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.readline(3)
            ip.readline(2)  # read beyond exact length
            ip.readline(1)
        self.assertEqual([mock.call(b'ab\n', False),
                          mock.call(b'c', True),
                          mock.call(b'', True)], mocked.call_args_list)

    def test_readline_unlimited_chunk_update(self):
        ip = utils.InputProxy(io.BytesIO(b'ab\nc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.readline()
            ip.readline()
        self.assertEqual([mock.call(b'ab\n', False),
                          mock.call(b'c', True)], mocked.call_args_list)
        ip = utils.InputProxy(io.BytesIO(b'ab\nc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.readline(None)
            ip.readline(None)
        self.assertEqual([mock.call(b'ab\n', False),
                          mock.call(b'c', True)], mocked.call_args_list)
        ip = utils.InputProxy(io.BytesIO(b'ab\nc'))
        with mock.patch.object(ip, 'chunk_update') as mocked:
            ip.readline(-1)
            ip.readline(-1)
        self.assertEqual([mock.call(b'ab\n', False),
                          mock.call(b'c', True)], mocked.call_args_list)

    def test_chunk_update_modifies_chunk(self):
        ip = utils.InputProxy(io.BytesIO(b'abc'))
        with mock.patch.object(ip, 'chunk_update', return_value='modified'):
            actual = ip.read()
        self.assertEqual('modified', actual)

    def test_read_client_disconnect(self):
        fake = mock.MagicMock()
        fake.read = mock.MagicMock(side_effect=ValueError('boom'))
        ip = utils.InputProxy(fake)
        with self.assertRaises(ValueError) as cm:
            ip.read()
        self.assertTrue(ip.client_disconnect)
        self.assertEqual('boom', str(cm.exception))

    def test_readline_client_disconnect(self):
        fake = mock.MagicMock()
        fake.readline = mock.MagicMock(side_effect=ValueError('boom'))
        ip = utils.InputProxy(fake)
        with self.assertRaises(ValueError) as cm:
            ip.readline()
        self.assertTrue(ip.client_disconnect)
        self.assertEqual('boom', str(cm.exception))


class UnsafeXrange(object):
    """
    Like range(limit), but with extra context switching to screw things up.
    """

    def __init__(self, upper_bound):
        self.current = 0
        self.concurrent_calls = 0
        self.upper_bound = upper_bound
        self.concurrent_call = False

    def __iter__(self):
        return self

    def __next__(self):
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


class TestEventletRateLimiter(unittest.TestCase):
    def test_init(self):
        rl = utils.EventletRateLimiter(0.1)
        self.assertEqual(0.1, rl.max_rate)
        self.assertEqual(0.0, rl.running_time)
        self.assertEqual(5000, rl.rate_buffer_ms)

        rl = utils.EventletRateLimiter(
            0.2, rate_buffer=2, running_time=1234567.8)
        self.assertEqual(0.2, rl.max_rate)
        self.assertEqual(1234567.8, rl.running_time)
        self.assertEqual(2000, rl.rate_buffer_ms)

    def test_set_max_rate(self):
        rl = utils.EventletRateLimiter(0.1)
        self.assertEqual(0.1, rl.max_rate)
        self.assertEqual(10000, rl.time_per_incr)
        rl.set_max_rate(2)
        self.assertEqual(2, rl.max_rate)
        self.assertEqual(500, rl.time_per_incr)

    def test_set_rate_buffer(self):
        rl = utils.EventletRateLimiter(0.1)
        self.assertEqual(5000.0, rl.rate_buffer_ms)
        rl.set_rate_buffer(2.3)
        self.assertEqual(2300, rl.rate_buffer_ms)

    def test_non_blocking(self):
        rate_limiter = utils.EventletRateLimiter(0.1, rate_buffer=0)
        with patch('time.time',) as mock_time:
            with patch('eventlet.sleep') as mock_sleep:
                mock_time.return_value = 0
                self.assertTrue(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()
                self.assertFalse(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()

                mock_time.return_value = 9.99
                self.assertFalse(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()
                mock_time.return_value = 10.0
                self.assertTrue(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()
                self.assertFalse(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()

        rate_limiter = utils.EventletRateLimiter(0.1, rate_buffer=20)
        with patch('time.time',) as mock_time:
            with patch('eventlet.sleep') as mock_sleep:
                mock_time.return_value = 20.0
                self.assertTrue(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()
                self.assertTrue(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()
                self.assertTrue(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()
                self.assertFalse(rate_limiter.is_allowed())
                mock_sleep.assert_not_called()

    def test_non_blocking_max_rate_adjusted(self):
        rate_limiter = utils.EventletRateLimiter(0.1, rate_buffer=0)
        with patch('time.time',) as mock_time:
            with patch('eventlet.sleep') as mock_sleep:
                mock_time.return_value = 0
                self.assertTrue(rate_limiter.is_allowed())
                self.assertFalse(rate_limiter.is_allowed())
                mock_time.return_value = 9.99
                self.assertFalse(rate_limiter.is_allowed())
                mock_time.return_value = 10.0
                self.assertTrue(rate_limiter.is_allowed())
                self.assertFalse(rate_limiter.is_allowed())
                # increase max_rate...but the new max_rate won't have impact
                # until the running time is next incremented, i.e. when
                # a call to is_allowed() next returns True
                rate_limiter.set_max_rate(0.2)
                self.assertFalse(rate_limiter.is_allowed())
                mock_time.return_value = 19.99
                self.assertFalse(rate_limiter.is_allowed())
                mock_time.return_value = 20.0
                self.assertTrue(rate_limiter.is_allowed())
                # now we can go faster...
                self.assertFalse(rate_limiter.is_allowed())
                mock_time.return_value = 24.99
                self.assertFalse(rate_limiter.is_allowed())
                mock_time.return_value = 25.0
                self.assertTrue(rate_limiter.is_allowed())
                self.assertFalse(rate_limiter.is_allowed())

        mock_sleep.assert_not_called()

    def _do_test(self, max_rate, running_time, start_time, rate_buffer,
                 burst_after_idle=False, incr_by=1.0):
        rate_limiter = utils.EventletRateLimiter(
            max_rate,
            running_time=1000 * running_time,  # msecs
            rate_buffer=rate_buffer,
            burst_after_idle=burst_after_idle)
        grant_times = []
        current_time = [start_time]

        def mock_time():
            return current_time[0]

        def mock_sleep(duration):
            current_time[0] += duration

        with patch('time.time', mock_time):
            with patch('eventlet.sleep', mock_sleep):
                for i in range(5):
                    rate_limiter.wait(incr_by=incr_by)
                    grant_times.append(current_time[0])
        return [round(t, 6) for t in grant_times]

    def test_ratelimit(self):
        grant_times = self._do_test(1, 0, 1, 0)
        self.assertEqual([1, 2, 3, 4, 5], grant_times)

        grant_times = self._do_test(10, 0, 1, 0)
        self.assertEqual([1, 1.1, 1.2, 1.3, 1.4], grant_times)

        grant_times = self._do_test(.1, 0, 1, 0)
        self.assertEqual([1, 11, 21, 31, 41], grant_times)

        grant_times = self._do_test(.1, 11, 1, 0)
        self.assertEqual([11, 21, 31, 41, 51], grant_times)

    def test_incr_by(self):
        grant_times = self._do_test(1, 0, 1, 0, incr_by=2.5)
        self.assertEqual([1, 3.5, 6, 8.5, 11], grant_times)

    def test_burst(self):
        grant_times = self._do_test(1, 1, 4, 0)
        self.assertEqual([4, 5, 6, 7, 8], grant_times)

        grant_times = self._do_test(1, 1, 4, 1)
        self.assertEqual([4, 5, 6, 7, 8], grant_times)

        grant_times = self._do_test(1, 1, 4, 2)
        self.assertEqual([4, 5, 6, 7, 8], grant_times)

        grant_times = self._do_test(1, 1, 4, 3)
        self.assertEqual([4, 4, 4, 4, 5], grant_times)

        grant_times = self._do_test(1, 1, 4, 4)
        self.assertEqual([4, 4, 4, 4, 5], grant_times)

        grant_times = self._do_test(1, 1, 3, 3)
        self.assertEqual([3, 3, 3, 4, 5], grant_times)

        grant_times = self._do_test(1, 0, 2, 3)
        self.assertEqual([2, 2, 2, 3, 4], grant_times)

        grant_times = self._do_test(1, 1, 3, 3)
        self.assertEqual([3, 3, 3, 4, 5], grant_times)

        grant_times = self._do_test(1, 0, 3, 3)
        self.assertEqual([3, 3, 3, 3, 4], grant_times)

        grant_times = self._do_test(1, 1, 3, 3)
        self.assertEqual([3, 3, 3, 4, 5], grant_times)

        grant_times = self._do_test(1, 0, 4, 3)
        self.assertEqual([4, 5, 6, 7, 8], grant_times)

    def test_burst_after_idle(self):
        grant_times = self._do_test(1, 1, 4, 1, burst_after_idle=True)
        self.assertEqual([4, 4, 5, 6, 7], grant_times)

        grant_times = self._do_test(1, 1, 4, 2, burst_after_idle=True)
        self.assertEqual([4, 4, 4, 5, 6], grant_times)

        grant_times = self._do_test(1, 0, 4, 3, burst_after_idle=True)
        self.assertEqual([4, 4, 4, 4, 5], grant_times)

        # running_time = start_time prevents burst on start-up
        grant_times = self._do_test(1, 4, 4, 3, burst_after_idle=True)
        self.assertEqual([4, 5, 6, 7, 8], grant_times)


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
            logger = debug_logger()
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
            logger = debug_logger()
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
            logger = debug_logger()
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
            expected_dirs = list()
            logger = debug_logger()
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
            expected_dirs.append((hash_path, 'drive', 'partition1'))
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
            expected_dirs.append((hash_path, 'drive', 'partition2'))
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

            # check yield_hash_dirs option
            locations = utils.audit_location_generator(
                tmpdir, "data", mount_check=False, logger=logger,
                yield_hash_dirs=True,
            )
            got_dirs = list(locations)
            self.assertEqual(sorted(got_dirs), sorted(expected_dirs))

    def test_ignore_metadata(self):
        with temptree([]) as tmpdir:
            logger = debug_logger()
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

    def test_hooks(self):
        with temptree([]) as tmpdir:
            logger = debug_logger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            partition = os.path.join(data, "partition1")
            os.makedirs(partition)
            suffix = os.path.join(partition, "suffix1")
            os.makedirs(suffix)
            hash_path = os.path.join(suffix, "hash1")
            os.makedirs(hash_path)
            obj_path = os.path.join(hash_path, "obj1.dat")
            with open(obj_path, "w"):
                pass
            meta_path = os.path.join(hash_path, "obj1.meta")
            with open(meta_path, "w"):
                pass
            hook_pre_device = MagicMock()
            hook_post_device = MagicMock()
            hook_pre_partition = MagicMock()
            hook_post_partition = MagicMock()
            hook_pre_suffix = MagicMock()
            hook_post_suffix = MagicMock()
            hook_pre_hash = MagicMock()
            hook_post_hash = MagicMock()
            locations = utils.audit_location_generator(
                tmpdir, "data", ".dat", mount_check=False, logger=logger,
                hook_pre_device=hook_pre_device,
                hook_post_device=hook_post_device,
                hook_pre_partition=hook_pre_partition,
                hook_post_partition=hook_post_partition,
                hook_pre_suffix=hook_pre_suffix,
                hook_post_suffix=hook_post_suffix,
                hook_pre_hash=hook_pre_hash,
                hook_post_hash=hook_post_hash
            )
            list(locations)
            hook_pre_device.assert_called_once_with(os.path.join(tmpdir,
                                                                 "drive"))
            hook_post_device.assert_called_once_with(os.path.join(tmpdir,
                                                                  "drive"))
            hook_pre_partition.assert_called_once_with(partition)
            hook_post_partition.assert_called_once_with(partition)
            hook_pre_suffix.assert_called_once_with(suffix)
            hook_post_suffix.assert_called_once_with(suffix)
            hook_pre_hash.assert_called_once_with(hash_path)
            hook_post_hash.assert_called_once_with(hash_path)

    def test_filters(self):
        with temptree([]) as tmpdir:
            logger = debug_logger()
            data = os.path.join(tmpdir, "drive", "data")
            os.makedirs(data)
            partition = os.path.join(data, "partition1")
            os.makedirs(partition)
            suffix = os.path.join(partition, "suffix1")
            os.makedirs(suffix)
            hash_path = os.path.join(suffix, "hash1")
            os.makedirs(hash_path)
            obj_path = os.path.join(hash_path, "obj1.dat")
            with open(obj_path, "w"):
                pass
            meta_path = os.path.join(hash_path, "obj1.meta")
            with open(meta_path, "w"):
                pass

            def audit_location_generator(**kwargs):
                return utils.audit_location_generator(
                    tmpdir, "data", ".dat", mount_check=False, logger=logger,
                    **kwargs)

            # Return the list of devices

            with patch('os.listdir', side_effect=os.listdir) as m_listdir:
                # devices_filter
                m_listdir.reset_mock()
                devices_filter = MagicMock(return_value=["drive"])
                list(audit_location_generator(devices_filter=devices_filter))
                devices_filter.assert_called_once_with(tmpdir, ["drive"])
                self.assertIn(((data,),), m_listdir.call_args_list)

                m_listdir.reset_mock()
                devices_filter = MagicMock(return_value=[])
                list(audit_location_generator(devices_filter=devices_filter))
                devices_filter.assert_called_once_with(tmpdir, ["drive"])
                self.assertNotIn(((data,),), m_listdir.call_args_list)

                # partitions_filter
                m_listdir.reset_mock()
                partitions_filter = MagicMock(return_value=["partition1"])
                list(audit_location_generator(
                    partitions_filter=partitions_filter))
                partitions_filter.assert_called_once_with(data,
                                                          ["partition1"])
                self.assertIn(((partition,),), m_listdir.call_args_list)

                m_listdir.reset_mock()
                partitions_filter = MagicMock(return_value=[])
                list(audit_location_generator(
                    partitions_filter=partitions_filter))
                partitions_filter.assert_called_once_with(data,
                                                          ["partition1"])
                self.assertNotIn(((partition,),), m_listdir.call_args_list)

                # suffixes_filter
                m_listdir.reset_mock()
                suffixes_filter = MagicMock(return_value=["suffix1"])
                list(audit_location_generator(suffixes_filter=suffixes_filter))
                suffixes_filter.assert_called_once_with(partition, ["suffix1"])
                self.assertIn(((suffix,),), m_listdir.call_args_list)

                m_listdir.reset_mock()
                suffixes_filter = MagicMock(return_value=[])
                list(audit_location_generator(suffixes_filter=suffixes_filter))
                suffixes_filter.assert_called_once_with(partition, ["suffix1"])
                self.assertNotIn(((suffix,),), m_listdir.call_args_list)

                # hashes_filter
                m_listdir.reset_mock()
                hashes_filter = MagicMock(return_value=["hash1"])
                list(audit_location_generator(hashes_filter=hashes_filter))
                hashes_filter.assert_called_once_with(suffix, ["hash1"])
                self.assertIn(((hash_path,),), m_listdir.call_args_list)

                m_listdir.reset_mock()
                hashes_filter = MagicMock(return_value=[])
                list(audit_location_generator(hashes_filter=hashes_filter))
                hashes_filter.assert_called_once_with(suffix, ["hash1"])
                self.assertNotIn(((hash_path,),), m_listdir.call_args_list)

    @with_tempdir
    def test_error_counter(self, tmpdir):
        def assert_no_errors(devices, mount_check=False):
            logger = debug_logger()
            error_counter = {}
            locations = utils.audit_location_generator(
                devices, "data", mount_check=mount_check, logger=logger,
                error_counter=error_counter
            )
            self.assertEqual([], list(locations))
            self.assertEqual([], logger.get_lines_for_level('warning'))
            self.assertEqual([], logger.get_lines_for_level('error'))
            self.assertEqual({}, error_counter)

        # no devices, no problem
        devices = os.path.join(tmpdir, 'devices1')
        os.makedirs(devices)
        assert_no_errors(devices)

        # empty dir under devices/
        devices = os.path.join(tmpdir, 'devices2')
        os.makedirs(devices)
        dev_dir = os.path.join(devices, 'device_is_empty_dir')
        os.makedirs(dev_dir)

        def assert_listdir_error(devices, expected):
            logger = debug_logger()
            error_counter = {}
            locations = utils.audit_location_generator(
                devices, "data", mount_check=False, logger=logger,
                error_counter=error_counter
            )
            self.assertEqual([], list(locations))
            self.assertEqual(1, len(logger.get_lines_for_level('warning')))
            self.assertEqual({'unlistable_partitions': expected},
                             error_counter)

        # file under devices/
        devices = os.path.join(tmpdir, 'devices3')
        os.makedirs(devices)
        with open(os.path.join(devices, 'device_is_file'), 'w'):
            pass
        listdir_error_data_dir = os.path.join(devices, 'device_is_file',
                                              'data')
        assert_listdir_error(devices, [listdir_error_data_dir])

        # dir under devices/
        devices = os.path.join(tmpdir, 'devices4')
        device = os.path.join(devices, 'device')
        os.makedirs(device)
        expected_datadir = os.path.join(devices, 'device', 'data')
        assert_no_errors(devices)

        # error for dir under devices/
        orig_listdir = utils.listdir

        def mocked(path):
            if path.endswith('data'):
                raise OSError
            return orig_listdir(path)

        with mock.patch('swift.common.utils.listdir', mocked):
            assert_listdir_error(devices, [expected_datadir])

        # mount check error
        devices = os.path.join(tmpdir, 'devices5')
        device = os.path.join(devices, 'device')
        os.makedirs(device)

        # no check
        with mock.patch('swift.common.utils.ismount', return_value=False):
            assert_no_errors(devices, mount_check=False)

        # check passes
        with mock.patch('swift.common.utils.ismount', return_value=True):
            assert_no_errors(devices, mount_check=True)

        # check fails
        logger = debug_logger()
        error_counter = {}
        with mock.patch('swift.common.utils.ismount', return_value=False):
            locations = utils.audit_location_generator(
                devices, "data", mount_check=True, logger=logger,
                error_counter=error_counter
            )
        self.assertEqual([], list(locations))
        self.assertEqual(1, len(logger.get_lines_for_level('warning')))
        self.assertEqual({'unmounted': ['device']}, error_counter)


class TestGreenAsyncPile(unittest.TestCase):

    def setUp(self):
        self.timeout = Timeout(5.0)

    def tearDown(self):
        self.timeout.cancel()

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
        self.assertIsNone(next(pile))
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
            self.assertRaises(StopIteration, next, pile)
            # pending remains 0
            self.assertEqual(0, pile._pending)

    def _exploder(self, arg):
        if isinstance(arg, Exception):
            raise arg
        else:
            return arg

    def test_blocking_last_next_explodes(self):
        pile = utils.GreenAsyncPile(2)
        pile.spawn(self._exploder, 1)
        pile.spawn(self._exploder, 2)
        pile.spawn(self._exploder, Exception('kaboom'))
        self.assertEqual(1, next(pile))
        self.assertEqual(2, next(pile))
        with mock.patch('sys.stderr', StringIO()) as mock_stderr, \
                self.assertRaises(StopIteration):
            next(pile)
        self.assertEqual(pile.inflight, 0)
        self.assertEqual(pile._pending, 0)
        self.assertIn('Exception: kaboom', mock_stderr.getvalue())
        self.assertIn('Traceback (most recent call last):',
                      mock_stderr.getvalue())

    def test_no_blocking_last_next_explodes(self):
        pile = utils.GreenAsyncPile(10)
        pile.spawn(self._exploder, 1)
        self.assertEqual(1, next(pile))
        pile.spawn(self._exploder, 2)
        self.assertEqual(2, next(pile))
        pile.spawn(self._exploder, Exception('kaboom'))
        with mock.patch('sys.stderr', StringIO()) as mock_stderr, \
                self.assertRaises(StopIteration):
            next(pile)
        self.assertEqual(pile.inflight, 0)
        self.assertEqual(pile._pending, 0)
        self.assertIn('Exception: kaboom', mock_stderr.getvalue())
        self.assertIn('Traceback (most recent call last):',
                      mock_stderr.getvalue())

    def test_exceptions_in_streaming_pile(self):
        with mock.patch('sys.stderr', StringIO()) as mock_stderr, \
                utils.StreamingPile(2) as pile:
            results = list(pile.asyncstarmap(self._exploder, [
                (1,),
                (Exception('kaboom'),),
                (3,),
            ]))
        self.assertEqual(results, [1, 3])
        self.assertEqual(pile.inflight, 0)
        self.assertEqual(pile._pending, 0)
        self.assertIn('Exception: kaboom', mock_stderr.getvalue())
        self.assertIn('Traceback (most recent call last):',
                      mock_stderr.getvalue())

    def test_exceptions_at_end_of_streaming_pile(self):
        with mock.patch('sys.stderr', StringIO()) as mock_stderr, \
                utils.StreamingPile(2) as pile:
            results = list(pile.asyncstarmap(self._exploder, [
                (1,),
                (2,),
                (Exception('kaboom'),),
            ]))
        self.assertEqual(results, [1, 2])
        self.assertEqual(pile.inflight, 0)
        self.assertEqual(pile._pending, 0)
        self.assertIn('Exception: kaboom', mock_stderr.getvalue())
        self.assertIn('Traceback (most recent call last):',
                      mock_stderr.getvalue())


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
        it = utils.iter_multipart_mime_documents(BytesIO(b'blah'), b'unique')
        exc = None
        try:
            next(it)
        except MimeInvalid as err:
            exc = err
        self.assertTrue('invalid starting boundary' in str(exc))
        self.assertTrue('--unique' in str(exc))

    def test_empty(self):
        it = utils.iter_multipart_mime_documents(BytesIO(b'--unique'),
                                                 b'unique')
        fp = next(it)
        self.assertEqual(fp.read(), b'')
        self.assertRaises(StopIteration, next, it)

    def test_basic(self):
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'--unique\r\nabcdefg\r\n--unique--'), b'unique')
        fp = next(it)
        self.assertEqual(fp.read(), b'abcdefg')
        self.assertRaises(StopIteration, next, it)

    def test_basic2(self):
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            b'unique')
        fp = next(it)
        self.assertEqual(fp.read(), b'abcdefg')
        fp = next(it)
        self.assertEqual(fp.read(), b'hijkl')
        self.assertRaises(StopIteration, next, it)

    def test_tiny_reads(self):
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            b'unique')
        fp = next(it)
        self.assertEqual(fp.read(2), b'ab')
        self.assertEqual(fp.read(2), b'cd')
        self.assertEqual(fp.read(2), b'ef')
        self.assertEqual(fp.read(2), b'g')
        self.assertEqual(fp.read(2), b'')
        fp = next(it)
        self.assertEqual(fp.read(), b'hijkl')
        self.assertRaises(StopIteration, next, it)

    def test_big_reads(self):
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'--unique\r\nabcdefg\r\n--unique\r\nhijkl\r\n--unique--'),
            b'unique')
        fp = next(it)
        self.assertEqual(fp.read(65536), b'abcdefg')
        self.assertEqual(fp.read(), b'')
        fp = next(it)
        self.assertEqual(fp.read(), b'hijkl')
        self.assertRaises(StopIteration, next, it)

    def test_leading_crlfs(self):
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'\r\n\r\n\r\n--unique\r\nabcdefg\r\n'
                    b'--unique\r\nhijkl\r\n--unique--'),
            b'unique')
        fp = next(it)
        self.assertEqual(fp.read(65536), b'abcdefg')
        self.assertEqual(fp.read(), b'')
        fp = next(it)
        self.assertEqual(fp.read(), b'hijkl')
        self.assertRaises(StopIteration, next, it)

    def test_broken_mid_stream(self):
        # We go ahead and accept whatever is sent instead of rejecting the
        # whole request, in case the partial form is still useful.
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'--unique\r\nabc'), b'unique')
        fp = next(it)
        self.assertEqual(fp.read(), b'abc')
        self.assertRaises(StopIteration, next, it)

    def test_readline(self):
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'--unique\r\nab\r\ncd\ref\ng\r\n--unique\r\nhi\r\n\r\n'
                    b'jkl\r\n\r\n--unique--'), b'unique')
        fp = next(it)
        self.assertEqual(fp.readline(), b'ab\r\n')
        self.assertEqual(fp.readline(), b'cd\ref\ng')
        self.assertEqual(fp.readline(), b'')
        fp = next(it)
        self.assertEqual(fp.readline(), b'hi\r\n')
        self.assertEqual(fp.readline(), b'\r\n')
        self.assertEqual(fp.readline(), b'jkl\r\n')
        self.assertRaises(StopIteration, next, it)

    def test_readline_with_tiny_chunks(self):
        it = utils.iter_multipart_mime_documents(
            BytesIO(b'--unique\r\nab\r\ncd\ref\ng\r\n--unique\r\nhi\r\n'
                    b'\r\njkl\r\n\r\n--unique--'),
            b'unique',
            read_chunk_size=2)
        fp = next(it)
        self.assertEqual(fp.readline(), b'ab\r\n')
        self.assertEqual(fp.readline(), b'cd\ref\ng')
        self.assertEqual(fp.readline(), b'')
        fp = next(it)
        self.assertEqual(fp.readline(), b'hi\r\n')
        self.assertEqual(fp.readline(), b'\r\n')
        self.assertEqual(fp.readline(), b'jkl\r\n')
        self.assertRaises(StopIteration, next, it)


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
        self.body = BytesIO(body)

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
        logger = debug_logger()
        body = utils.document_iters_to_http_response_body(
            iter([]), 'dontcare', multipart=False, logger=logger)
        self.assertEqual(body, '')
        self.assertFalse(logger.all_log_lines())

    def test_single_part(self):
        body = b"time flies like an arrow; fruit flies like a banana"
        doc_iters = [{'part_iter': iter(BytesIO(body).read, b'')}]
        logger = debug_logger()

        resp_body = b''.join(
            utils.document_iters_to_http_response_body(
                iter(doc_iters), b'dontcare', multipart=False, logger=logger))
        self.assertEqual(resp_body, body)
        self.assertFalse(logger.all_log_lines())

    def test_single_part_unexpected_ranges(self):
        body = b"time flies like an arrow; fruit flies like a banana"
        doc_iters = [{'part_iter': iter(BytesIO(body).read, b'')}, 'junk']
        logger = debug_logger()

        resp_body = b''.join(
            utils.document_iters_to_http_response_body(
                iter(doc_iters), b'dontcare', multipart=False, logger=logger))
        self.assertEqual(resp_body, body)
        self.assertEqual(['More than one part in a single-part response?'],
                         logger.get_lines_for_level('warning'))

    def test_multiple_parts(self):
        part1 = b"two peanuts were walking down a railroad track"
        part2 = b"and one was a salted. ... peanut."

        doc_iters = [{
            'start_byte': 88,
            'end_byte': 133,
            'content_type': 'application/peanut',
            'entity_length': 1024,
            'part_iter': iter(BytesIO(part1).read, b''),
        }, {
            'start_byte': 500,
            'end_byte': 532,
            'content_type': 'application/salted',
            'entity_length': 1024,
            'part_iter': iter(BytesIO(part2).read, b''),
        }]

        resp_body = b''.join(
            utils.document_iters_to_http_response_body(
                iter(doc_iters), b'boundaryboundary',
                multipart=True, logger=debug_logger()))
        self.assertEqual(resp_body, (
            b"--boundaryboundary\r\n" +
            # This is a little too strict; we don't actually care that the
            # headers are in this order, but the test is much more legible
            # this way.
            b"Content-Type: application/peanut\r\n" +
            b"Content-Range: bytes 88-133/1024\r\n" +
            b"\r\n" +
            part1 + b"\r\n" +
            b"--boundaryboundary\r\n"
            b"Content-Type: application/salted\r\n" +
            b"Content-Range: bytes 500-532/1024\r\n" +
            b"\r\n" +
            part2 + b"\r\n" +
            b"--boundaryboundary--"))

    def test_closed_part_iterator(self):
        useful_iter_mock = mock.MagicMock()
        useful_iter_mock.__iter__.return_value = ['']
        body_iter = utils.document_iters_to_http_response_body(
            iter([{'part_iter': useful_iter_mock}]), 'dontcare',
            multipart=False, logger=debug_logger())
        body = ''
        for s in body_iter:
            body += s
        self.assertEqual(body, '')
        useful_iter_mock.close.assert_called_once_with()

        # Calling "close" on the mock will now raise an AttributeError
        del useful_iter_mock.close
        body_iter = utils.document_iters_to_http_response_body(
            iter([{'part_iter': useful_iter_mock}]), 'dontcare',
            multipart=False, logger=debug_logger())
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


class TestHashForFileFunction(unittest.TestCase):
    def setUp(self):
        self.tempfilename = tempfile.mktemp()

    def tearDown(self):
        try:
            os.unlink(self.tempfilename)
        except OSError:
            pass

    def test_hash_for_file_smallish(self):
        stub_data = b'some data'
        with open(self.tempfilename, 'wb') as fd:
            fd.write(stub_data)
        with mock.patch('swift.common.utils.md5') as mock_md5:
            mock_hasher = mock_md5.return_value
            rv = utils.md5_hash_for_file(self.tempfilename)
        self.assertTrue(mock_hasher.hexdigest.called)
        self.assertEqual(rv, mock_hasher.hexdigest.return_value)
        self.assertEqual([mock.call(stub_data)],
                         mock_hasher.update.call_args_list)

    def test_hash_for_file_big(self):
        num_blocks = 10
        block_size = utils.MD5_BLOCK_READ_BYTES
        truncate = 523
        start_char = ord('a')
        expected_blocks = [chr(i).encode('utf8') * block_size
                           for i in range(start_char, start_char + num_blocks)]
        full_data = b''.join(expected_blocks)
        trimmed_data = full_data[:-truncate]
        # sanity
        self.assertEqual(len(trimmed_data), block_size * num_blocks - truncate)
        with open(self.tempfilename, 'wb') as fd:
            fd.write(trimmed_data)
        with mock.patch('swift.common.utils.md5') as mock_md5:
            mock_hasher = mock_md5.return_value
            rv = utils.md5_hash_for_file(self.tempfilename)
        self.assertTrue(mock_hasher.hexdigest.called)
        self.assertEqual(rv, mock_hasher.hexdigest.return_value)
        self.assertEqual(num_blocks, len(mock_hasher.update.call_args_list))
        found_blocks = []
        for i, (expected_block, call) in enumerate(zip(
                expected_blocks, mock_hasher.update.call_args_list)):
            args, kwargs = call
            self.assertEqual(kwargs, {})
            self.assertEqual(1, len(args))
            block = args[0]
            if i < num_blocks - 1:
                self.assertEqual(block, expected_block)
            else:
                self.assertEqual(block, expected_block[:-truncate])
            found_blocks.append(block)
        self.assertEqual(b''.join(found_blocks), trimmed_data)

    def test_hash_for_file_empty(self):
        with open(self.tempfilename, 'wb'):
            pass
        with mock.patch('swift.common.utils.md5') as mock_md5:
            mock_hasher = mock_md5.return_value
            rv = utils.md5_hash_for_file(self.tempfilename)
        self.assertTrue(mock_hasher.hexdigest.called)
        self.assertIs(rv, mock_hasher.hexdigest.return_value)
        self.assertEqual([], mock_hasher.update.call_args_list)

    def test_hash_for_file_brittle(self):
        data_to_expected_hash = {
            b'': 'd41d8cd98f00b204e9800998ecf8427e',
            b'some data': '1e50210a0202497fb79bc38b6ade6c34',
            (b'a' * 4096 * 10)[:-523]: '06a41551609656c85f14f659055dc6d3',
        }
        # unlike some other places where the concrete implementation really
        # matters for backwards compatibility these brittle tests are probably
        # not needed or justified, if a future maintainer rips them out later
        # they're probably doing the right thing
        failures = []
        for stub_data, expected_hash in data_to_expected_hash.items():
            with open(self.tempfilename, 'wb') as fd:
                fd.write(stub_data)
            rv = utils.md5_hash_for_file(self.tempfilename)
            try:
                self.assertEqual(expected_hash, rv)
            except AssertionError:
                trim_cap = 80
                if len(stub_data) > trim_cap:
                    stub_data = '%s...<truncated>' % stub_data[:trim_cap]
                failures.append('hash for %r was %s instead of expected %s' % (
                    stub_data, rv, expected_hash))
        if failures:
            self.fail('Some data did not compute expected hash:\n' +
                      '\n'.join(failures))


class TestFsHasFreeSpace(unittest.TestCase):
    def setUp(self):
        self.fake_result = posix.statvfs_result([
            4096,     # f_bsize
            4096,     # f_frsize
            2854907,  # f_blocks
            1984802,  # f_bfree   (free blocks for root)
            1728089,  # f_bavail  (free blocks for non-root)
            1280000,  # f_files
            1266040,  # f_ffree,
            1266040,  # f_favail,
            4096,     # f_flag
            255,      # f_namemax
        ])

    def test_bytes(self):
        with mock.patch(
                'os.statvfs', return_value=self.fake_result) as mock_statvfs:
            self.assertTrue(utils.fs_has_free_space("/", 0, False))
            self.assertTrue(utils.fs_has_free_space("/", 1, False))
            # free space left = f_bavail * f_bsize = 7078252544
            self.assertTrue(utils.fs_has_free_space("/", 7078252544, False))
            self.assertFalse(utils.fs_has_free_space("/", 7078252545, False))
            self.assertFalse(utils.fs_has_free_space("/", 2 ** 64, False))
        mock_statvfs.assert_has_calls([mock.call("/")] * 5)

    def test_bytes_using_file_descriptor(self):
        with mock.patch(
                'os.fstatvfs', return_value=self.fake_result) as mock_fstatvfs:
            self.assertTrue(utils.fs_has_free_space(99, 0, False))
            self.assertTrue(utils.fs_has_free_space(99, 1, False))
            # free space left = f_bavail * f_bsize = 7078252544
            self.assertTrue(utils.fs_has_free_space(99, 7078252544, False))
            self.assertFalse(utils.fs_has_free_space(99, 7078252545, False))
            self.assertFalse(utils.fs_has_free_space(99, 2 ** 64, False))
        mock_fstatvfs.assert_has_calls([mock.call(99)] * 5)

    def test_percent(self):
        with mock.patch('os.statvfs', return_value=self.fake_result):
            self.assertTrue(utils.fs_has_free_space("/", 0, True))
            self.assertTrue(utils.fs_has_free_space("/", 1, True))
            # percentage of free space for the faked statvfs is 60%
            self.assertTrue(utils.fs_has_free_space("/", 60, True))
            self.assertFalse(utils.fs_has_free_space("/", 61, True))
            self.assertFalse(utils.fs_has_free_space("/", 100, True))
            self.assertFalse(utils.fs_has_free_space("/", 110, True))


class TestSetSwiftDir(unittest.TestCase):
    def setUp(self):
        self.swift_dir = tempfile.mkdtemp()
        self.swift_conf = os.path.join(self.swift_dir, 'swift.conf')
        self.policy_name = ''.join(random.sample(string.ascii_letters, 20))
        with open(self.swift_conf, "wt") as sc:
            sc.write('''
[swift-hash]
swift_hash_path_suffix = changeme

[storage-policy:0]
name = default
default = yes

[storage-policy:1]
name = %s
''' % self.policy_name)

    def tearDown(self):
        shutil.rmtree(self.swift_dir, ignore_errors=True)

    def test_set_swift_dir(self):
        set_swift_dir(None)
        reload_storage_policies()
        self.assertIsNone(POLICIES.get_by_name(self.policy_name))

        set_swift_dir(self.swift_dir)
        reload_storage_policies()
        self.assertIsNotNone(POLICIES.get_by_name(self.policy_name))


class TestDistributeEvenly(unittest.TestCase):
    def test_evenly_divided(self):
        out = utils.distribute_evenly(range(12), 3)
        self.assertEqual(out, [
            [0, 3, 6, 9],
            [1, 4, 7, 10],
            [2, 5, 8, 11],
        ])

        out = utils.distribute_evenly(range(12), 4)
        self.assertEqual(out, [
            [0, 4, 8],
            [1, 5, 9],
            [2, 6, 10],
            [3, 7, 11],
        ])

    def test_uneven(self):
        out = utils.distribute_evenly(range(11), 3)
        self.assertEqual(out, [
            [0, 3, 6, 9],
            [1, 4, 7, 10],
            [2, 5, 8],
        ])

    def test_just_one(self):
        out = utils.distribute_evenly(range(5), 1)
        self.assertEqual(out, [[0, 1, 2, 3, 4]])

    def test_more_buckets_than_items(self):
        out = utils.distribute_evenly(range(5), 7)
        self.assertEqual(out, [[0], [1], [2], [3], [4], [], []])


@mock.patch('swift.common.utils.open')
class TestGetPpid(unittest.TestCase):
    def test_happy_path(self, mock_open):
        mock_open.return_value.__enter__().read.return_value = \
            'pid comm stat 456 see the procfs(5) man page for more info\n'
        self.assertEqual(utils.get_ppid(123), 456)
        self.assertIn(mock.call('/proc/123/stat'), mock_open.mock_calls)

    def test_not_found(self, mock_open):
        mock_open.side_effect = IOError(errno.ENOENT, "Not there")
        with self.assertRaises(OSError) as caught:
            utils.get_ppid(123)
        self.assertEqual(caught.exception.errno, errno.ESRCH)
        self.assertEqual(mock_open.mock_calls[0], mock.call('/proc/123/stat'))

    def test_not_allowed(self, mock_open):
        mock_open.side_effect = OSError(errno.EPERM, "Not for you")
        with self.assertRaises(OSError) as caught:
            utils.get_ppid(123)
        self.assertEqual(caught.exception.errno, errno.EPERM)
        self.assertEqual(mock_open.mock_calls[0], mock.call('/proc/123/stat'))


class TestShardName(unittest.TestCase):
    def test(self):
        ts = utils.Timestamp.now()
        created = utils.ShardName.create('a', 'root', 'parent', ts, 1)
        parent_hash = md5(b'parent', usedforsecurity=False).hexdigest()
        expected = 'a/root-%s-%s-1' % (parent_hash, ts.internal)
        actual = str(created)
        self.assertEqual(expected, actual)
        parsed = utils.ShardName.parse(actual)
        # normally a ShardName will be in the .shards prefix
        self.assertEqual('a', parsed.account)
        self.assertEqual('root', parsed.root_container)
        self.assertEqual(parent_hash, parsed.parent_container_hash)
        self.assertEqual(ts, parsed.timestamp)
        self.assertEqual(1, parsed.index)
        self.assertEqual(actual, str(parsed))

    def test_root_has_hyphens(self):
        parsed = utils.ShardName.parse(
            'a/root-has-some-hyphens-hash-1234-99')
        self.assertEqual('a', parsed.account)
        self.assertEqual('root-has-some-hyphens', parsed.root_container)
        self.assertEqual('hash', parsed.parent_container_hash)
        self.assertEqual(utils.Timestamp(1234), parsed.timestamp)
        self.assertEqual(99, parsed.index)

    def test_realistic_shard_range_names(self):
        parsed = utils.ShardName.parse(
            '.shards_a1/r1-'
            '7c92cf1eee8d99cc85f8355a3d6e4b86-'
            '1662475499.00000-1')
        self.assertEqual('.shards_a1', parsed.account)
        self.assertEqual('r1', parsed.root_container)
        self.assertEqual('7c92cf1eee8d99cc85f8355a3d6e4b86',
                         parsed.parent_container_hash)
        self.assertEqual(utils.Timestamp(1662475499), parsed.timestamp)
        self.assertEqual(1, parsed.index)

        parsed = utils.ShardName('.shards_a', 'c', 'hash',
                                 utils.Timestamp(1234), 42)
        self.assertEqual(
            '.shards_a/c-hash-0000001234.00000-42',
            str(parsed))

        parsed = utils.ShardName.create('.shards_a', 'c', 'c',
                                        utils.Timestamp(1234), 42)
        self.assertEqual(
            '.shards_a/c-4a8a08f09d37b73795649038408b5f33-0000001234.00000-42',
            str(parsed))

    def test_bad_parse(self):
        with self.assertRaises(ValueError) as cm:
            utils.ShardName.parse('a')
        self.assertEqual('invalid name: a', str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            utils.ShardName.parse('a/c')
        self.assertEqual('invalid name: a/c', str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            utils.ShardName.parse('a/root-hash-bad')
        self.assertEqual('invalid name: a/root-hash-bad', str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            utils.ShardName.parse('a/root-hash-bad-0')
        self.assertEqual('invalid name: a/root-hash-bad-0',
                         str(cm.exception))
        with self.assertRaises(ValueError) as cm:
            utils.ShardName.parse('a/root-hash-12345678.12345-bad')
        self.assertEqual('invalid name: a/root-hash-12345678.12345-bad',
                         str(cm.exception))

    def test_bad_create(self):
        with self.assertRaises(ValueError):
            utils.ShardName.create('a', 'root', 'hash', 'bad', '0')
        with self.assertRaises(ValueError):
            utils.ShardName.create('a', 'root', None, '1235678', 'bad')


class BaseNamespaceShardRange(object):

    def _check_name_account_container(self, nsr, exp_name):
        # check that the name, account, container properties are consistent
        exp_account, exp_container = exp_name.split('/')
        self.assertEqual(exp_name, nsr.name)
        self.assertEqual(exp_account, nsr.account)
        self.assertEqual(exp_container, nsr.container)


class TestNamespace(unittest.TestCase, BaseNamespaceShardRange):

    def test_lower_setter(self):
        ns = utils.Namespace('a/c', 'b', '')
        # sanity checks
        self.assertEqual('b', ns.lower_str)
        self.assertEqual(ns.MAX, ns.upper)

        def do_test(good_value, expected):
            ns.lower = good_value
            self.assertEqual(expected, ns.lower)
            self.assertEqual(ns.MAX, ns.upper)

        do_test(utils.Namespace.MIN, utils.Namespace.MIN)
        do_test(utils.Namespace.MAX, utils.Namespace.MAX)
        do_test(b'', utils.Namespace.MIN)
        do_test(u'', utils.Namespace.MIN)
        do_test(None, utils.Namespace.MIN)
        do_test(b'a', 'a')
        do_test(b'y', 'y')
        do_test(u'a', 'a')
        do_test(u'y', 'y')

        expected = u'\N{SNOWMAN}'
        with warnings.catch_warnings(record=True) as captured_warnings:
            do_test(u'\N{SNOWMAN}', expected)
            do_test(u'\N{SNOWMAN}'.encode('utf-8'), expected)
        self.assertFalse(captured_warnings)

        ns = utils.Namespace('a/c', 'b', 'y')
        ns.lower = ''
        self.assertEqual(ns.MIN, ns.lower)

        ns = utils.Namespace('a/c', 'b', 'y')
        with self.assertRaises(ValueError) as cm:
            ns.lower = 'z'
        self.assertIn("must be less than or equal to upper", str(cm.exception))
        self.assertEqual('b', ns.lower_str)
        self.assertEqual('y', ns.upper_str)

        def do_test(bad_value):
            with self.assertRaises(TypeError) as cm:
                ns.lower = bad_value
            self.assertIn("lower must be a string", str(cm.exception))
            self.assertEqual('b', ns.lower_str)
            self.assertEqual('y', ns.upper_str)

        do_test(1)
        do_test(1.234)

    def test_upper_setter(self):
        ns = utils.Namespace('a/c', '', 'y')
        # sanity checks
        self.assertEqual(ns.MIN, ns.lower)
        self.assertEqual('y', ns.upper_str)

        def do_test(good_value, expected):
            ns.upper = good_value
            self.assertEqual(expected, ns.upper)
            self.assertEqual(ns.MIN, ns.lower)

        do_test(utils.Namespace.MIN, utils.Namespace.MIN)
        do_test(utils.Namespace.MAX, utils.Namespace.MAX)
        do_test(b'', utils.Namespace.MAX)
        do_test(u'', utils.Namespace.MAX)
        do_test(None, utils.Namespace.MAX)
        do_test(b'z', 'z')
        do_test(b'b', 'b')
        do_test(u'z', 'z')
        do_test(u'b', 'b')

        expected = u'\N{SNOWMAN}'
        with warnings.catch_warnings(record=True) as captured_warnings:
            do_test(u'\N{SNOWMAN}', expected)
            do_test(u'\N{SNOWMAN}'.encode('utf-8'), expected)
        self.assertFalse(captured_warnings)

        ns = utils.Namespace('a/c', 'b', 'y')
        ns.upper = ''
        self.assertEqual(ns.MAX, ns.upper)

        ns = utils.Namespace('a/c', 'b', 'y')
        with self.assertRaises(ValueError) as cm:
            ns.upper = 'a'
        self.assertIn(
            "must be greater than or equal to lower",
            str(cm.exception))
        self.assertEqual('b', ns.lower_str)
        self.assertEqual('y', ns.upper_str)

        def do_test(bad_value):
            with self.assertRaises(TypeError) as cm:
                ns.upper = bad_value
            self.assertIn("upper must be a string", str(cm.exception))
            self.assertEqual('b', ns.lower_str)
            self.assertEqual('y', ns.upper_str)

        do_test(1)
        do_test(1.234)

    def test_end_marker(self):
        ns = utils.Namespace('a/c', '', 'y')
        self.assertEqual('y\x00', ns.end_marker)
        ns = utils.Namespace('a/c', '', '')
        self.assertEqual('', ns.end_marker)

    def test_bounds_serialization(self):
        ns = utils.Namespace('a/c', None, None)
        self.assertEqual('a/c', ns.name)
        self.assertEqual(utils.Namespace.MIN, ns.lower)
        self.assertEqual('', ns.lower_str)
        self.assertEqual(utils.Namespace.MAX, ns.upper)
        self.assertEqual('', ns.upper_str)
        self.assertEqual('', ns.end_marker)

        lower = u'\u00e4'
        upper = u'\u00fb'
        ns = utils.Namespace('a/%s-%s' % (lower, upper), lower, upper)
        exp_lower = lower
        exp_upper = upper
        self.assertEqual(exp_lower, ns.lower)
        self.assertEqual(exp_lower, ns.lower_str)
        self.assertEqual(exp_upper, ns.upper)
        self.assertEqual(exp_upper, ns.upper_str)
        self.assertEqual(exp_upper + '\x00', ns.end_marker)

    def test_name(self):
        # constructor
        path = 'a/c'
        ns = utils.Namespace(path, 'l', 'u')
        self._check_name_account_container(ns, path)

        # constructor
        path = u'\u1234a/\N{SNOWMAN}'
        ns = utils.Namespace(path, 'l', 'u')
        self._check_name_account_container(ns, path)
        ns = utils.Namespace(path.encode('utf8'), 'l', 'u')
        self._check_name_account_container(ns, path)

    def test_name_unexpected_format(self):
        # name is not a/c format
        ns = utils.Namespace('foo', 'l', 'u')
        self.assertEqual('foo', ns.name)
        self.assertEqual('foo', ns.account)
        with self.assertRaises(IndexError):
            ns.container

    def test_unicode_name(self):
        shard_bounds = ('', 'ham', 'pie', u'\N{SNOWMAN}', u'\U0001F334', '')
        exp_bounds = [(l, u)
                      for l, u in zip(shard_bounds[:-1], shard_bounds[1:])]
        namespaces = [utils.Namespace('.shards_a/c_%s' % upper, lower, upper)
                      for lower, upper in exp_bounds]
        for i in range(len(exp_bounds)):
            self.assertEqual(namespaces[i].name,
                             '.shards_a/c_%s' % exp_bounds[i][1])
            self.assertEqual(namespaces[i].lower_str, exp_bounds[i][0])

            self.assertEqual(namespaces[i].upper_str, exp_bounds[i][1])

    def test_entire_namespace(self):
        # test entire range (no boundaries)
        entire = utils.Namespace('a/test', None, None)
        self.assertEqual(utils.Namespace.MAX, entire.upper)
        self.assertEqual(utils.Namespace.MIN, entire.lower)
        self.assertIs(True, entire.entire_namespace())

        for x in range(100):
            self.assertTrue(str(x) in entire)
            self.assertTrue(chr(x) in entire)

        for x in ('a', 'z', 'zzzz', '124fsdf', u'\u00e4'):
            self.assertTrue(x in entire, '%r should be in %r' % (x, entire))

        entire.lower = 'a'
        self.assertIs(False, entire.entire_namespace())

    def test_comparisons(self):
        # upper (if provided) *must* be greater than lower
        with self.assertRaises(ValueError):
            utils.Namespace('f-a', 'f', 'a')

        # test basic boundaries
        btoc = utils.Namespace('a/b-c', 'b', 'c')
        atof = utils.Namespace('a/a-f', 'a', 'f')
        ftol = utils.Namespace('a/f-l', 'f', 'l')
        ltor = utils.Namespace('a/l-r', 'l', 'r')
        rtoz = utils.Namespace('a/r-z', 'r', 'z')
        lower = utils.Namespace('a/lower', '', 'mid')
        upper = utils.Namespace('a/upper', 'mid', '')
        entire = utils.Namespace('a/test', None, None)

        # overlapping ranges
        dtof = utils.Namespace('a/d-f', 'd', 'f')
        dtom = utils.Namespace('a/d-m', 'd', 'm')

        # test range > and <
        # non-adjacent
        self.assertFalse(rtoz < atof)
        self.assertTrue(atof < ltor)
        self.assertTrue(ltor > atof)
        self.assertFalse(ftol > rtoz)

        # adjacent
        self.assertFalse(rtoz < ltor)
        self.assertTrue(ltor < rtoz)
        self.assertFalse(ltor > rtoz)
        self.assertTrue(rtoz > ltor)

        # wholly within
        self.assertFalse(btoc < atof)
        self.assertFalse(btoc > atof)
        self.assertFalse(atof < btoc)
        self.assertFalse(atof > btoc)

        self.assertFalse(atof < dtof)
        self.assertFalse(dtof > atof)
        self.assertFalse(atof > dtof)
        self.assertFalse(dtof < atof)

        self.assertFalse(dtof < dtom)
        self.assertFalse(dtof > dtom)
        self.assertFalse(dtom > dtof)
        self.assertFalse(dtom < dtof)

        # overlaps
        self.assertFalse(atof < dtom)
        self.assertFalse(atof > dtom)
        self.assertFalse(ltor > dtom)

        # ranges including min/max bounds
        self.assertTrue(upper > lower)
        self.assertTrue(lower < upper)
        self.assertFalse(upper < lower)
        self.assertFalse(lower > upper)

        self.assertFalse(lower < entire)
        self.assertFalse(entire > lower)
        self.assertFalse(lower > entire)
        self.assertFalse(entire < lower)

        self.assertFalse(upper < entire)
        self.assertFalse(entire > upper)
        self.assertFalse(upper > entire)
        self.assertFalse(entire < upper)

        self.assertFalse(entire < entire)
        self.assertFalse(entire > entire)

        # test range < and > to an item
        # range is > lower and <= upper to lower boundary isn't
        # actually included
        self.assertTrue(ftol > 'f')
        self.assertFalse(atof < 'f')
        self.assertTrue(ltor < 'y')

        self.assertFalse(ftol < 'f')
        self.assertFalse(atof > 'f')
        self.assertFalse(ltor > 'y')

        self.assertTrue('f' < ftol)
        self.assertFalse('f' > atof)
        self.assertTrue('y' > ltor)

        self.assertFalse('f' > ftol)
        self.assertFalse('f' < atof)
        self.assertFalse('y' < ltor)

        # Now test ranges with only 1 boundary
        start_to_l = utils.Namespace('a/None-l', '', 'l')
        l_to_end = utils.Namespace('a/l-None', 'l', '')

        for x in ('l', 'm', 'z', 'zzz1231sd'):
            if x == 'l':
                self.assertFalse(x in l_to_end)
                self.assertFalse(start_to_l < x)
                self.assertFalse(x > start_to_l)
            else:
                self.assertTrue(x in l_to_end)
                self.assertTrue(start_to_l < x)
                self.assertTrue(x > start_to_l)

        # Now test some of the range to range checks with missing boundaries
        self.assertFalse(atof < start_to_l)
        self.assertFalse(start_to_l < entire)

        # Now test overlaps(other)
        self.assertTrue(atof.overlaps(atof))
        self.assertFalse(atof.overlaps(ftol))
        self.assertFalse(ftol.overlaps(atof))
        self.assertTrue(atof.overlaps(dtof))
        self.assertTrue(dtof.overlaps(atof))
        self.assertFalse(dtof.overlaps(ftol))
        self.assertTrue(dtom.overlaps(ftol))
        self.assertTrue(ftol.overlaps(dtom))
        self.assertFalse(start_to_l.overlaps(l_to_end))

    def test_contains(self):
        lower = utils.Namespace('a/-h', '', 'h')
        mid = utils.Namespace('a/h-p', 'h', 'p')
        upper = utils.Namespace('a/p-', 'p', '')
        entire = utils.Namespace('a/all', '', '')

        self.assertTrue('a' in entire)
        self.assertTrue('x' in entire)

        # the empty string is not a valid object name, so it cannot be in any
        # range
        self.assertFalse('' in lower)
        self.assertFalse('' in upper)
        self.assertFalse('' in entire)

        self.assertTrue('a' in lower)
        self.assertTrue('h' in lower)
        self.assertFalse('i' in lower)

        self.assertFalse('h' in mid)
        self.assertTrue('p' in mid)

        self.assertFalse('p' in upper)
        self.assertTrue('x' in upper)

        self.assertIn(utils.Namespace.MAX, entire)
        self.assertNotIn(utils.Namespace.MAX, lower)
        self.assertIn(utils.Namespace.MAX, upper)

        # lower bound is excluded so MIN cannot be in any range.
        self.assertNotIn(utils.Namespace.MIN, entire)
        self.assertNotIn(utils.Namespace.MIN, upper)
        self.assertNotIn(utils.Namespace.MIN, lower)

    def test_includes(self):
        _to_h = utils.Namespace('a/-h', '', 'h')
        d_to_t = utils.Namespace('a/d-t', 'd', 't')
        d_to_k = utils.Namespace('a/d-k', 'd', 'k')
        e_to_l = utils.Namespace('a/e-l', 'e', 'l')
        k_to_t = utils.Namespace('a/k-t', 'k', 't')
        p_to_ = utils.Namespace('a/p-', 'p', '')
        t_to_ = utils.Namespace('a/t-', 't', '')
        entire = utils.Namespace('a/all', '', '')

        self.assertTrue(entire.includes(entire))
        self.assertTrue(d_to_t.includes(d_to_t))
        self.assertTrue(_to_h.includes(_to_h))
        self.assertTrue(p_to_.includes(p_to_))

        self.assertTrue(entire.includes(_to_h))
        self.assertTrue(entire.includes(d_to_t))
        self.assertTrue(entire.includes(p_to_))

        self.assertTrue(d_to_t.includes(d_to_k))
        self.assertTrue(d_to_t.includes(e_to_l))
        self.assertTrue(d_to_t.includes(k_to_t))
        self.assertTrue(p_to_.includes(t_to_))

        self.assertFalse(_to_h.includes(d_to_t))
        self.assertFalse(p_to_.includes(d_to_t))
        self.assertFalse(k_to_t.includes(d_to_k))
        self.assertFalse(d_to_k.includes(e_to_l))
        self.assertFalse(k_to_t.includes(e_to_l))
        self.assertFalse(t_to_.includes(p_to_))

        self.assertFalse(_to_h.includes(entire))
        self.assertFalse(p_to_.includes(entire))
        self.assertFalse(d_to_t.includes(entire))

    def test_expand(self):
        bounds = (('', 'd'), ('d', 'k'), ('k', 't'), ('t', ''))
        donors = [
            utils.Namespace('a/c-%d' % i, b[0], b[1])
            for i, b in enumerate(bounds)
        ]
        acceptor = utils.Namespace('a/c-acc', 'f', 's')
        self.assertTrue(acceptor.expand(donors[:1]))
        self.assertEqual((utils.Namespace.MIN, 's'),
                         (acceptor.lower, acceptor.upper))

        acceptor = utils.Namespace('a/c-acc', 'f', 's')
        self.assertTrue(acceptor.expand(donors[:2]))
        self.assertEqual((utils.Namespace.MIN, 's'),
                         (acceptor.lower, acceptor.upper))

        acceptor = utils.Namespace('a/c-acc', 'f', 's')
        self.assertTrue(acceptor.expand(donors[1:3]))
        self.assertEqual(('d', 't'),
                         (acceptor.lower, acceptor.upper))

        acceptor = utils.Namespace('a/c-acc', 'f', 's')
        self.assertTrue(acceptor.expand(donors))
        self.assertEqual((utils.Namespace.MIN, utils.Namespace.MAX),
                         (acceptor.lower, acceptor.upper))

        acceptor = utils.Namespace('a/c-acc', 'f', 's')
        self.assertTrue(acceptor.expand(donors[1:2] + donors[3:]))
        self.assertEqual(('d', utils.Namespace.MAX),
                         (acceptor.lower, acceptor.upper))

        acceptor = utils.Namespace('a/c-acc', '', 'd')
        self.assertFalse(acceptor.expand(donors[:1]))
        self.assertEqual((utils.Namespace.MIN, 'd'),
                         (acceptor.lower, acceptor.upper))

        acceptor = utils.Namespace('a/c-acc', 'b', 'v')
        self.assertFalse(acceptor.expand(donors[1:3]))
        self.assertEqual(('b', 'v'),
                         (acceptor.lower, acceptor.upper))

    def test_total_ordering(self):
        a_start_ns = utils.Namespace('a/-a', '', 'a')
        a_atob_ns = utils.Namespace('a/a-b', 'a', 'b')
        a_atof_ns = utils.Namespace('a/a-f', 'a', 'f')
        a_ftol_ns = utils.Namespace('a/f-l', 'f', 'l')
        a_ltor_ns = utils.Namespace('a/l-r', 'l', 'r')
        a_rtoz_ns = utils.Namespace('a/r-z', 'r', 'z')
        a_end_ns = utils.Namespace('a/z-', 'z', '')
        b_start_ns = utils.Namespace('b/-a', '', 'a')
        self.assertEqual(a_start_ns, b_start_ns)
        self.assertNotEqual(a_start_ns, a_atob_ns)
        self.assertLess(a_start_ns, a_atob_ns)
        self.assertLess(a_atof_ns, a_ftol_ns)
        self.assertLess(a_ftol_ns, a_ltor_ns)
        self.assertLess(a_ltor_ns, a_rtoz_ns)
        self.assertLess(a_rtoz_ns, a_end_ns)
        self.assertLessEqual(a_start_ns, a_atof_ns)
        self.assertLessEqual(a_atof_ns, a_rtoz_ns)
        self.assertLessEqual(a_atof_ns, a_atof_ns)
        self.assertGreater(a_end_ns, a_atof_ns)
        self.assertGreater(a_rtoz_ns, a_ftol_ns)
        self.assertGreater(a_end_ns, a_start_ns)
        self.assertGreaterEqual(a_atof_ns, a_atof_ns)
        self.assertGreaterEqual(a_end_ns, a_atof_ns)
        self.assertGreaterEqual(a_rtoz_ns, a_start_ns)


class TestNamespaceBoundList(unittest.TestCase):
    def setUp(self):
        start = ['', 'a/-a']
        self.start_ns = utils.Namespace('a/-a', '', 'a')
        atof = ['a', 'a/a-f']
        self.atof_ns = utils.Namespace('a/a-f', 'a', 'f')
        ftol = ['f', 'a/f-l']
        self.ftol_ns = utils.Namespace('a/f-l', 'f', 'l')
        ltor = ['l', 'a/l-r']
        self.ltor_ns = utils.Namespace('a/l-r', 'l', 'r')
        rtoz = ['r', 'a/r-z']
        self.rtoz_ns = utils.Namespace('a/r-z', 'r', 'z')
        end = ['z', 'a/z-']
        self.end_ns = utils.Namespace('a/z-', 'z', '')
        self.lowerbounds = [start, atof, ftol, ltor, rtoz, end]

    def test_eq(self):
        this = utils.NamespaceBoundList(self.lowerbounds)
        that = utils.NamespaceBoundList(self.lowerbounds)
        self.assertEqual(this, that)
        that = utils.NamespaceBoundList(self.lowerbounds[:1])
        self.assertNotEqual(this, that)
        self.assertNotEqual(this, None)
        self.assertNotEqual(this, self.lowerbounds)

    def test_get_namespace(self):
        namespace_list = utils.NamespaceBoundList(self.lowerbounds)
        self.assertEqual(namespace_list.bounds, self.lowerbounds)
        self.assertEqual(namespace_list.get_namespace('1'), self.start_ns)
        self.assertEqual(namespace_list.get_namespace('a'), self.start_ns)
        self.assertEqual(namespace_list.get_namespace('b'), self.atof_ns)
        self.assertEqual(namespace_list.get_namespace('f'), self.atof_ns)
        self.assertEqual(namespace_list.get_namespace('f\x00'), self.ftol_ns)
        self.assertEqual(namespace_list.get_namespace('l'), self.ftol_ns)
        self.assertEqual(namespace_list.get_namespace('x'), self.rtoz_ns)
        self.assertEqual(namespace_list.get_namespace('r'), self.ltor_ns)
        self.assertEqual(namespace_list.get_namespace('}'), self.end_ns)

    def test_parse(self):
        namespaces_list = utils.NamespaceBoundList.parse(None)
        self.assertEqual(namespaces_list, None)
        namespaces = [self.start_ns, self.atof_ns, self.ftol_ns,
                      self.ltor_ns, self.rtoz_ns, self.end_ns]
        namespace_list = utils.NamespaceBoundList.parse(namespaces)
        self.assertEqual(namespace_list.bounds, self.lowerbounds)
        self.assertEqual(namespace_list.get_namespace('1'), self.start_ns)
        self.assertEqual(namespace_list.get_namespace('l'), self.ftol_ns)
        self.assertEqual(namespace_list.get_namespace('x'), self.rtoz_ns)
        self.assertEqual(namespace_list.get_namespace('r'), self.ltor_ns)
        self.assertEqual(namespace_list.get_namespace('}'), self.end_ns)
        self.assertEqual(namespace_list.bounds, self.lowerbounds)
        overlap_f_ns = utils.Namespace('a/-f', '', 'f')
        overlapping_namespaces = [self.start_ns, self.atof_ns, overlap_f_ns,
                                  self.ftol_ns, self.ltor_ns, self.rtoz_ns,
                                  self.end_ns]
        namespace_list = utils.NamespaceBoundList.parse(
            overlapping_namespaces)
        self.assertEqual(namespace_list.bounds, self.lowerbounds)
        overlap_l_ns = utils.Namespace('a/a-l', 'a', 'l')
        overlapping_namespaces = [self.start_ns, self.atof_ns, self.ftol_ns,
                                  overlap_l_ns, self.ltor_ns, self.rtoz_ns,
                                  self.end_ns]
        namespace_list = utils.NamespaceBoundList.parse(
            overlapping_namespaces)
        self.assertEqual(namespace_list.bounds, self.lowerbounds)


class TestShardRange(unittest.TestCase, BaseNamespaceShardRange):
    def setUp(self):
        self.ts_iter = make_timestamp_iter()

    def test_constants(self):
        self.assertEqual({utils.ShardRange.SHARDING,
                          utils.ShardRange.SHARDED,
                          utils.ShardRange.SHRINKING,
                          utils.ShardRange.SHRUNK},
                         set(utils.ShardRange.CLEAVING_STATES))
        self.assertEqual({utils.ShardRange.SHARDING,
                          utils.ShardRange.SHARDED},
                         set(utils.ShardRange.SHARDING_STATES))
        self.assertEqual({utils.ShardRange.SHRINKING,
                          utils.ShardRange.SHRUNK},
                         set(utils.ShardRange.SHRINKING_STATES))

    def test_min_max_bounds(self):
        with self.assertRaises(TypeError):
            utils.NamespaceOuterBound()

        # max
        self.assertEqual(utils.ShardRange.MAX, utils.ShardRange.MAX)
        self.assertFalse(utils.ShardRange.MAX > utils.ShardRange.MAX)
        self.assertFalse(utils.ShardRange.MAX < utils.ShardRange.MAX)

        for val in 'z', u'\u00e4':
            self.assertFalse(utils.ShardRange.MAX == val)
            self.assertFalse(val > utils.ShardRange.MAX)
            self.assertTrue(val < utils.ShardRange.MAX)
            self.assertTrue(utils.ShardRange.MAX > val)
            self.assertFalse(utils.ShardRange.MAX < val)

        self.assertEqual('', str(utils.ShardRange.MAX))
        self.assertFalse(utils.ShardRange.MAX)
        self.assertTrue(utils.ShardRange.MAX == utils.ShardRange.MAX)
        self.assertFalse(utils.ShardRange.MAX != utils.ShardRange.MAX)
        self.assertTrue(
            utils.ShardRange.MaxBound() == utils.ShardRange.MaxBound())
        self.assertTrue(
            utils.ShardRange.MaxBound() is utils.ShardRange.MaxBound())
        self.assertTrue(
            utils.ShardRange.MaxBound() is utils.ShardRange.MAX)
        self.assertFalse(
            utils.ShardRange.MaxBound() != utils.ShardRange.MaxBound())

        # min
        self.assertEqual(utils.ShardRange.MIN, utils.ShardRange.MIN)
        self.assertFalse(utils.ShardRange.MIN > utils.ShardRange.MIN)
        self.assertFalse(utils.ShardRange.MIN < utils.ShardRange.MIN)

        for val in 'z', u'\u00e4':
            self.assertFalse(utils.ShardRange.MIN == val)
            self.assertFalse(val < utils.ShardRange.MIN)
            self.assertTrue(val > utils.ShardRange.MIN)
            self.assertTrue(utils.ShardRange.MIN < val)
            self.assertFalse(utils.ShardRange.MIN > val)
            self.assertFalse(utils.ShardRange.MIN)

        self.assertEqual('', str(utils.ShardRange.MIN))
        self.assertFalse(utils.ShardRange.MIN)
        self.assertTrue(utils.ShardRange.MIN == utils.ShardRange.MIN)
        self.assertFalse(utils.ShardRange.MIN != utils.ShardRange.MIN)
        self.assertTrue(
            utils.ShardRange.MinBound() == utils.ShardRange.MinBound())
        self.assertTrue(
            utils.ShardRange.MinBound() is utils.ShardRange.MinBound())
        self.assertTrue(
            utils.ShardRange.MinBound() is utils.ShardRange.MIN)
        self.assertFalse(
            utils.ShardRange.MinBound() != utils.ShardRange.MinBound())

        self.assertFalse(utils.ShardRange.MAX == utils.ShardRange.MIN)
        self.assertFalse(utils.ShardRange.MIN == utils.ShardRange.MAX)
        self.assertTrue(utils.ShardRange.MAX != utils.ShardRange.MIN)
        self.assertTrue(utils.ShardRange.MIN != utils.ShardRange.MAX)
        self.assertFalse(utils.ShardRange.MAX is utils.ShardRange.MIN)

        self.assertEqual(utils.ShardRange.MAX,
                         max(utils.ShardRange.MIN, utils.ShardRange.MAX))
        self.assertEqual(utils.ShardRange.MIN,
                         min(utils.ShardRange.MIN, utils.ShardRange.MAX))

        # check the outer bounds are hashable
        hashmap = {utils.ShardRange.MIN: 'min',
                   utils.ShardRange.MAX: 'max'}
        self.assertEqual(hashmap[utils.ShardRange.MIN], 'min')
        self.assertEqual(hashmap[utils.ShardRange.MinBound()], 'min')
        self.assertEqual(hashmap[utils.ShardRange.MAX], 'max')
        self.assertEqual(hashmap[utils.ShardRange.MaxBound()], 'max')

    def test_shard_range_initialisation(self):
        def assert_initialisation_ok(params, expected):
            pr = utils.ShardRange(**params)
            self.assertDictEqual(dict(pr), expected)

        def assert_initialisation_fails(params, err_type=ValueError):
            with self.assertRaises(err_type):
                utils.ShardRange(**params)

        ts_1 = next(self.ts_iter)
        ts_2 = next(self.ts_iter)
        ts_3 = next(self.ts_iter)
        ts_4 = next(self.ts_iter)
        empty_run = dict(name=None, timestamp=None, lower=None,
                         upper=None, object_count=0, bytes_used=0,
                         meta_timestamp=None, deleted=0,
                         state=utils.ShardRange.FOUND, state_timestamp=None,
                         epoch=None)
        # name, timestamp must be given
        assert_initialisation_fails(empty_run.copy())
        assert_initialisation_fails(dict(empty_run, name='a/c'), TypeError)
        assert_initialisation_fails(dict(empty_run, timestamp=ts_1))
        # name must be form a/c
        assert_initialisation_fails(dict(empty_run, name='c', timestamp=ts_1))
        assert_initialisation_fails(dict(empty_run, name='', timestamp=ts_1))
        assert_initialisation_fails(dict(empty_run, name='/a/c',
                                         timestamp=ts_1))
        assert_initialisation_fails(dict(empty_run, name='/c',
                                         timestamp=ts_1))
        # lower, upper can be None
        expect = dict(name='a/c', timestamp=ts_1.internal, lower='',
                      upper='', object_count=0, bytes_used=0,
                      meta_timestamp=ts_1.internal, deleted=0,
                      state=utils.ShardRange.FOUND,
                      state_timestamp=ts_1.internal, epoch=None,
                      reported=0, tombstones=-1)
        assert_initialisation_ok(dict(empty_run, name='a/c', timestamp=ts_1),
                                 expect)
        assert_initialisation_ok(dict(name='a/c', timestamp=ts_1), expect)

        good_run = dict(name='a/c', timestamp=ts_1, lower='l',
                        upper='u', object_count=2, bytes_used=10,
                        meta_timestamp=ts_2, deleted=0,
                        state=utils.ShardRange.CREATED,
                        state_timestamp=ts_3.internal, epoch=ts_4,
                        reported=0, tombstones=11)
        expect.update({'lower': 'l', 'upper': 'u', 'object_count': 2,
                       'bytes_used': 10, 'meta_timestamp': ts_2.internal,
                       'state': utils.ShardRange.CREATED,
                       'state_timestamp': ts_3.internal, 'epoch': ts_4,
                       'reported': 0, 'tombstones': 11})
        assert_initialisation_ok(good_run.copy(), expect)

        # obj count, tombstones and bytes used as int strings
        good_str_run = good_run.copy()
        good_str_run.update({'object_count': '2', 'bytes_used': '10',
                             'tombstones': '11'})
        assert_initialisation_ok(good_str_run, expect)

        good_no_meta = good_run.copy()
        good_no_meta.pop('meta_timestamp')
        assert_initialisation_ok(good_no_meta,
                                 dict(expect, meta_timestamp=ts_1.internal))

        good_deleted = good_run.copy()
        good_deleted['deleted'] = 1
        assert_initialisation_ok(good_deleted,
                                 dict(expect, deleted=1))

        good_reported = good_run.copy()
        good_reported['reported'] = 1
        assert_initialisation_ok(good_reported,
                                 dict(expect, reported=1))

        assert_initialisation_fails(dict(good_run, timestamp='water balloon'))

        assert_initialisation_fails(
            dict(good_run, meta_timestamp='water balloon'))

        assert_initialisation_fails(dict(good_run, lower='water balloon'))

        assert_initialisation_fails(dict(good_run, upper='balloon'))

        assert_initialisation_fails(
            dict(good_run, object_count='water balloon'))

        assert_initialisation_fails(dict(good_run, bytes_used='water ballon'))

        assert_initialisation_fails(dict(good_run, object_count=-1))

        assert_initialisation_fails(dict(good_run, bytes_used=-1))
        assert_initialisation_fails(dict(good_run, state=-1))
        assert_initialisation_fails(dict(good_run, state_timestamp='not a ts'))
        assert_initialisation_fails(dict(good_run, name='/a/c'))
        assert_initialisation_fails(dict(good_run, name='/a/c/'))
        assert_initialisation_fails(dict(good_run, name='a/c/'))
        assert_initialisation_fails(dict(good_run, name='a'))
        assert_initialisation_fails(dict(good_run, name=''))

    def _check_to_from_dict(self, lower, upper):
        ts_1 = next(self.ts_iter)
        ts_2 = next(self.ts_iter)
        ts_3 = next(self.ts_iter)
        ts_4 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, lower, upper, 10, 100, ts_2,
                              state=None, state_timestamp=ts_3, epoch=ts_4)
        sr_dict = dict(sr)
        expected = {
            'name': 'a/test', 'timestamp': ts_1.internal, 'lower': lower,
            'upper': upper, 'object_count': 10, 'bytes_used': 100,
            'meta_timestamp': ts_2.internal, 'deleted': 0,
            'state': utils.ShardRange.FOUND, 'state_timestamp': ts_3.internal,
            'epoch': ts_4, 'reported': 0, 'tombstones': -1}
        self.assertEqual(expected, sr_dict)
        self.assertIsInstance(sr_dict['lower'], str)
        self.assertIsInstance(sr_dict['upper'], str)
        sr_new = utils.ShardRange.from_dict(sr_dict)
        self.assertEqual(sr, sr_new)
        self.assertEqual(sr_dict, dict(sr_new))

        sr_new = utils.ShardRange(**sr_dict)
        self.assertEqual(sr, sr_new)
        self.assertEqual(sr_dict, dict(sr_new))

        for key in sr_dict:
            bad_dict = dict(sr_dict)
            bad_dict.pop(key)
            if key in ('reported', 'tombstones'):
                # These were added after the fact, and we need to be able to
                # eat data from old servers
                utils.ShardRange.from_dict(bad_dict)
                utils.ShardRange(**bad_dict)
                continue

            # The rest were present from the beginning
            with self.assertRaises(KeyError):
                utils.ShardRange.from_dict(bad_dict)
            # But __init__ still (generally) works!
            if key != 'name':
                utils.ShardRange(**bad_dict)
            else:
                with self.assertRaises(TypeError):
                    utils.ShardRange(**bad_dict)

    def test_to_from_dict(self):
        self._check_to_from_dict('l', 'u')
        self._check_to_from_dict('', '')

    def test_name(self):
        # constructor
        path = 'a/c'
        sr = utils.ShardRange(path, 0, 'l', 'u')
        self._check_name_account_container(sr, path)
        # name setter
        path = 'a2/c2'
        sr.name = path
        self._check_name_account_container(sr, path)

        # constructor
        path = u'\u1234a/\N{SNOWMAN}'
        sr = utils.ShardRange(path, 0, 'l', 'u')
        self._check_name_account_container(sr, path)
        sr = utils.ShardRange(path.encode('utf8'), 0, 'l', 'u')
        self._check_name_account_container(sr, path)
        # name setter
        path = u'\N{SNOWMAN}/\u1234c'
        sr.name = path
        self._check_name_account_container(sr, path)
        sr.name = path.encode('utf-8')
        self._check_name_account_container(sr, path)

    def test_name_validation(self):
        def check_invalid(call, *args):
            with self.assertRaises(ValueError) as cm:
                call(*args)
            self.assertIn(
                "Name must be of the form '<account>/<container>'",
                str(cm.exception))

        ts = next(self.ts_iter)
        check_invalid(utils.ShardRange, '', ts, 'l', 'u')
        check_invalid(utils.ShardRange, 'a', ts, 'l', 'u')
        check_invalid(utils.ShardRange, b'a', ts, 'l', 'u')
        check_invalid(utils.ShardRange, 'a/', ts, 'l', 'u')
        check_invalid(utils.ShardRange, b'a/', ts, 'l', 'u')
        check_invalid(utils.ShardRange, '/', ts, 'l', 'u')
        check_invalid(utils.ShardRange, '/c', ts, 'l', 'u')
        check_invalid(utils.ShardRange, b'/c', ts, 'l', 'u')
        check_invalid(utils.ShardRange, None, ts, 'l', 'u')

        ns = utils.ShardRange('a/c', ts, 'l', 'u')
        check_invalid(setattr, ns, 'name', b'')
        check_invalid(setattr, ns, 'name', b'a')
        check_invalid(setattr, ns, 'name', b'a/')
        check_invalid(setattr, ns, 'name', b'/')
        check_invalid(setattr, ns, 'name', b'/c')
        check_invalid(setattr, ns, 'name', None)

    def test_timestamp_setter(self):
        ts_1 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, 'l', 'u', 0, 0, None)
        self.assertEqual(ts_1, sr.timestamp)

        ts_2 = next(self.ts_iter)
        sr.timestamp = ts_2
        self.assertEqual(ts_2, sr.timestamp)

        sr.timestamp = 0
        self.assertEqual(utils.Timestamp(0), sr.timestamp)

        with self.assertRaises(TypeError):
            sr.timestamp = None

    def test_meta_timestamp_setter(self):
        ts_1 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, 'l', 'u', 0, 0, None)
        self.assertEqual(ts_1, sr.timestamp)
        self.assertEqual(ts_1, sr.meta_timestamp)

        ts_2 = next(self.ts_iter)
        sr.meta_timestamp = ts_2
        self.assertEqual(ts_1, sr.timestamp)
        self.assertEqual(ts_2, sr.meta_timestamp)

        ts_3 = next(self.ts_iter)
        sr.timestamp = ts_3
        self.assertEqual(ts_3, sr.timestamp)
        self.assertEqual(ts_2, sr.meta_timestamp)

        # meta_timestamp defaults to tracking timestamp
        sr.meta_timestamp = None
        self.assertEqual(ts_3, sr.timestamp)
        self.assertEqual(ts_3, sr.meta_timestamp)
        ts_4 = next(self.ts_iter)
        sr.timestamp = ts_4
        self.assertEqual(ts_4, sr.timestamp)
        self.assertEqual(ts_4, sr.meta_timestamp)

        sr.meta_timestamp = 0
        self.assertEqual(ts_4, sr.timestamp)
        self.assertEqual(utils.Timestamp(0), sr.meta_timestamp)

    def test_update_meta(self):
        ts_1 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, 'l', 'u', 0, 0, None)
        with mock_timestamp_now(next(self.ts_iter)) as now:
            sr.update_meta(9, 99)
        self.assertEqual(9, sr.object_count)
        self.assertEqual(99, sr.bytes_used)
        self.assertEqual(now, sr.meta_timestamp)

        with mock_timestamp_now(next(self.ts_iter)) as now:
            sr.update_meta(99, 999, None)
        self.assertEqual(99, sr.object_count)
        self.assertEqual(999, sr.bytes_used)
        self.assertEqual(now, sr.meta_timestamp)

        ts_2 = next(self.ts_iter)
        sr.update_meta(21, 2112, ts_2)
        self.assertEqual(21, sr.object_count)
        self.assertEqual(2112, sr.bytes_used)
        self.assertEqual(ts_2, sr.meta_timestamp)

        sr.update_meta('11', '12')
        self.assertEqual(11, sr.object_count)
        self.assertEqual(12, sr.bytes_used)

        def check_bad_args(*args):
            with self.assertRaises(ValueError):
                sr.update_meta(*args)
        check_bad_args('bad', 10)
        check_bad_args(10, 'bad')
        check_bad_args(10, 11, 'bad')

    def test_increment_meta(self):
        ts_1 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, 'l', 'u', 1, 2, None)
        with mock_timestamp_now(next(self.ts_iter)) as now:
            sr.increment_meta(9, 99)
        self.assertEqual(10, sr.object_count)
        self.assertEqual(101, sr.bytes_used)
        self.assertEqual(now, sr.meta_timestamp)

        sr.increment_meta('11', '12')
        self.assertEqual(21, sr.object_count)
        self.assertEqual(113, sr.bytes_used)

        def check_bad_args(*args):
            with self.assertRaises(ValueError):
                sr.increment_meta(*args)
        check_bad_args('bad', 10)
        check_bad_args(10, 'bad')

    def test_update_tombstones(self):
        ts_1 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, 'l', 'u', 0, 0, None)
        self.assertEqual(-1, sr.tombstones)
        self.assertFalse(sr.reported)

        with mock_timestamp_now(next(self.ts_iter)) as now:
            sr.update_tombstones(1)
        self.assertEqual(1, sr.tombstones)
        self.assertEqual(now, sr.meta_timestamp)
        self.assertFalse(sr.reported)

        sr.reported = True
        with mock_timestamp_now(next(self.ts_iter)) as now:
            sr.update_tombstones(3, None)
        self.assertEqual(3, sr.tombstones)
        self.assertEqual(now, sr.meta_timestamp)
        self.assertFalse(sr.reported)

        sr.reported = True
        ts_2 = next(self.ts_iter)
        sr.update_tombstones(5, ts_2)
        self.assertEqual(5, sr.tombstones)
        self.assertEqual(ts_2, sr.meta_timestamp)
        self.assertFalse(sr.reported)

        # no change in value -> no change in reported
        sr.reported = True
        ts_3 = next(self.ts_iter)
        sr.update_tombstones(5, ts_3)
        self.assertEqual(5, sr.tombstones)
        self.assertEqual(ts_3, sr.meta_timestamp)
        self.assertTrue(sr.reported)

        sr.update_meta('11', '12')
        self.assertEqual(11, sr.object_count)
        self.assertEqual(12, sr.bytes_used)

        def check_bad_args(*args):
            with self.assertRaises(ValueError):
                sr.update_tombstones(*args)
        check_bad_args('bad')
        check_bad_args(10, 'bad')

    def test_row_count(self):
        ts_1 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, 'l', 'u', 0, 0, None)
        self.assertEqual(0, sr.row_count)

        sr.update_meta(11, 123)
        self.assertEqual(11, sr.row_count)
        sr.update_tombstones(13)
        self.assertEqual(24, sr.row_count)
        sr.update_meta(0, 0)
        self.assertEqual(13, sr.row_count)

    def test_state_timestamp_setter(self):
        ts_1 = next(self.ts_iter)
        sr = utils.ShardRange('a/test', ts_1, 'l', 'u', 0, 0, None)
        self.assertEqual(ts_1, sr.timestamp)
        self.assertEqual(ts_1, sr.state_timestamp)

        ts_2 = next(self.ts_iter)
        sr.state_timestamp = ts_2
        self.assertEqual(ts_1, sr.timestamp)
        self.assertEqual(ts_2, sr.state_timestamp)

        ts_3 = next(self.ts_iter)
        sr.timestamp = ts_3
        self.assertEqual(ts_3, sr.timestamp)
        self.assertEqual(ts_2, sr.state_timestamp)

        # state_timestamp defaults to tracking timestamp
        sr.state_timestamp = None
        self.assertEqual(ts_3, sr.timestamp)
        self.assertEqual(ts_3, sr.state_timestamp)
        ts_4 = next(self.ts_iter)
        sr.timestamp = ts_4
        self.assertEqual(ts_4, sr.timestamp)
        self.assertEqual(ts_4, sr.state_timestamp)

        sr.state_timestamp = 0
        self.assertEqual(ts_4, sr.timestamp)
        self.assertEqual(utils.Timestamp(0), sr.state_timestamp)

    def test_state_setter(self):
        for state, state_name in utils.ShardRange.STATES.items():
            for test_value in (
                    state, str(state), state_name, state_name.upper()):
                sr = utils.ShardRange('a/test', next(self.ts_iter), 'l', 'u')
                sr.state = test_value
                actual = sr.state
                self.assertEqual(
                    state, actual,
                    'Expected %s but got %s for %s' %
                    (state, actual, test_value)
                )

        for bad_state in (max(utils.ShardRange.STATES) + 1,
                          -1, 99, None, 'stringy', 1.1):
            sr = utils.ShardRange('a/test', next(self.ts_iter), 'l', 'u')
            with self.assertRaises(ValueError) as cm:
                sr.state = bad_state
            self.assertIn('Invalid state', str(cm.exception))

    def test_update_state(self):
        sr = utils.ShardRange('a/c', next(self.ts_iter))
        old_sr = sr.copy()
        self.assertEqual(utils.ShardRange.FOUND, sr.state)
        self.assertEqual(dict(sr), dict(old_sr))  # sanity check

        for state in utils.ShardRange.STATES:
            if state == utils.ShardRange.FOUND:
                continue
            self.assertTrue(sr.update_state(state))
            self.assertEqual(dict(old_sr, state=state), dict(sr))
            self.assertFalse(sr.update_state(state))
            self.assertEqual(dict(old_sr, state=state), dict(sr))

        sr = utils.ShardRange('a/c', next(self.ts_iter))
        old_sr = sr.copy()
        for state in utils.ShardRange.STATES:
            ts = next(self.ts_iter)
            self.assertTrue(sr.update_state(state, state_timestamp=ts))
            self.assertEqual(dict(old_sr, state=state, state_timestamp=ts),
                             dict(sr))

    def test_resolve_state(self):
        for name, number in utils.ShardRange.STATES_BY_NAME.items():
            self.assertEqual(
                (number, name), utils.ShardRange.resolve_state(name))
            self.assertEqual(
                (number, name), utils.ShardRange.resolve_state(name.upper()))
            self.assertEqual(
                (number, name), utils.ShardRange.resolve_state(name.title()))
            self.assertEqual(
                (number, name), utils.ShardRange.resolve_state(number))
            self.assertEqual(
                (number, name), utils.ShardRange.resolve_state(str(number)))

        def check_bad_value(value):
            with self.assertRaises(ValueError) as cm:
                utils.ShardRange.resolve_state(value)
            self.assertIn('Invalid state %r' % value, str(cm.exception))

        check_bad_value(min(utils.ShardRange.STATES) - 1)
        check_bad_value(max(utils.ShardRange.STATES) + 1)
        check_bad_value('badstate')

    def test_epoch_setter(self):
        sr = utils.ShardRange('a/c', next(self.ts_iter))
        self.assertIsNone(sr.epoch)
        ts = next(self.ts_iter)
        sr.epoch = ts
        self.assertEqual(ts, sr.epoch)
        ts = next(self.ts_iter)
        sr.epoch = ts.internal
        self.assertEqual(ts, sr.epoch)
        sr.epoch = None
        self.assertIsNone(sr.epoch)
        with self.assertRaises(ValueError):
            sr.epoch = 'bad'

    def test_deleted_setter(self):
        sr = utils.ShardRange('a/c', next(self.ts_iter))
        for val in (True, 1):
            sr.deleted = val
            self.assertIs(True, sr.deleted)
        for val in (False, 0, None):
            sr.deleted = val
            self.assertIs(False, sr.deleted)

    def test_set_deleted(self):
        sr = utils.ShardRange('a/c', next(self.ts_iter))
        # initialise other timestamps
        sr.update_state(utils.ShardRange.ACTIVE,
                        state_timestamp=utils.Timestamp.now())
        sr.update_meta(1, 2)
        old_sr = sr.copy()
        self.assertIs(False, sr.deleted)  # sanity check
        self.assertEqual(dict(sr), dict(old_sr))  # sanity check

        with mock_timestamp_now(next(self.ts_iter)) as now:
            self.assertTrue(sr.set_deleted())
        self.assertEqual(now, sr.timestamp)
        self.assertIs(True, sr.deleted)
        old_sr_dict = dict(old_sr)
        old_sr_dict.pop('deleted')
        old_sr_dict.pop('timestamp')
        sr_dict = dict(sr)
        sr_dict.pop('deleted')
        sr_dict.pop('timestamp')
        self.assertEqual(old_sr_dict, sr_dict)

        # no change
        self.assertFalse(sr.set_deleted())
        self.assertEqual(now, sr.timestamp)
        self.assertIs(True, sr.deleted)

        # force timestamp change
        with mock_timestamp_now(next(self.ts_iter)) as now:
            self.assertTrue(sr.set_deleted(timestamp=now))
        self.assertEqual(now, sr.timestamp)
        self.assertIs(True, sr.deleted)

    def test_repr(self):
        ts = next(self.ts_iter)
        ts.offset = 1234
        meta_ts = next(self.ts_iter)
        state_ts = next(self.ts_iter)
        sr = utils.ShardRange('a/c', ts, 'l', 'u', 100, 1000,
                              meta_timestamp=meta_ts,
                              state=utils.ShardRange.ACTIVE,
                              state_timestamp=state_ts)
        self.assertEqual(
            "ShardRange<%r to %r as of %s, (100, 1000) as of %s, "
            "active as of %s>"
            % ('l', 'u',
               ts.internal, meta_ts.internal, state_ts.internal), str(sr))

        ts.offset = 0
        meta_ts.offset = 2
        state_ts.offset = 3
        sr = utils.ShardRange('a/c', ts, '', '', 100, 1000,
                              meta_timestamp=meta_ts,
                              state=utils.ShardRange.FOUND,
                              state_timestamp=state_ts)
        self.assertEqual(
            "ShardRange<MinBound to MaxBound as of %s, (100, 1000) as of %s, "
            "found as of %s>"
            % (ts.internal, meta_ts.internal, state_ts.internal), str(sr))

    def test_copy(self):
        sr = utils.ShardRange('a/c', next(self.ts_iter), 'x', 'y', 99, 99000,
                              meta_timestamp=next(self.ts_iter),
                              state=utils.ShardRange.CREATED,
                              state_timestamp=next(self.ts_iter))
        new = sr.copy()
        self.assertEqual(dict(sr), dict(new))

        new = sr.copy(deleted=1)
        self.assertEqual(dict(sr, deleted=1), dict(new))

        new_timestamp = next(self.ts_iter)
        new = sr.copy(timestamp=new_timestamp)
        self.assertEqual(dict(sr, timestamp=new_timestamp.internal,
                              meta_timestamp=new_timestamp.internal,
                              state_timestamp=new_timestamp.internal),
                         dict(new))

        new = sr.copy(timestamp=new_timestamp, object_count=99)
        self.assertEqual(dict(sr, timestamp=new_timestamp.internal,
                              meta_timestamp=new_timestamp.internal,
                              state_timestamp=new_timestamp.internal,
                              object_count=99),
                         dict(new))

    def test_make_path(self):
        ts = utils.Timestamp.now()
        actual = utils.ShardRange.make_path('a', 'root', 'parent', ts, 0)
        parent_hash = md5(b'parent', usedforsecurity=False).hexdigest()
        self.assertEqual('a/root-%s-%s-0' % (parent_hash, ts.internal), actual)
        actual = utils.ShardRange.make_path('a', 'root', 'parent', ts, 3)
        self.assertEqual('a/root-%s-%s-3' % (parent_hash, ts.internal), actual)
        actual = utils.ShardRange.make_path('a', 'root', 'parent', ts, '3')
        self.assertEqual('a/root-%s-%s-3' % (parent_hash, ts.internal), actual)
        actual = utils.ShardRange.make_path(
            'a', 'root', 'parent', ts.internal, '3')
        self.assertEqual('a/root-%s-%s-3' % (parent_hash, ts.internal), actual)

    def test_sort_key_order(self):
        self.assertEqual(
            utils.ShardRange.sort_key_order(
                name="a/c",
                lower='lower',
                upper='upper',
                state=utils.ShardRange.ACTIVE),
            ('upper', utils.ShardRange.ACTIVE, 'lower', "a/c"))

    def test_sort_key(self):
        orig_shard_ranges = [
            utils.ShardRange('a/c', next(self.ts_iter), '', '',
                             state=utils.ShardRange.SHARDED),
            utils.ShardRange('.a/c1', next(self.ts_iter), 'a', 'd',
                             state=utils.ShardRange.CREATED),
            utils.ShardRange('.a/c0', next(self.ts_iter), '', 'a',
                             state=utils.ShardRange.CREATED),
            utils.ShardRange('.a/c2b', next(self.ts_iter), 'd', 'f',
                             state=utils.ShardRange.SHARDING),
            utils.ShardRange('.a/c2', next(self.ts_iter), 'c', 'f',
                             state=utils.ShardRange.SHARDING),
            utils.ShardRange('.a/c2a', next(self.ts_iter), 'd', 'f',
                             state=utils.ShardRange.SHARDING),
            utils.ShardRange('.a/c4', next(self.ts_iter), 'f', '',
                             state=utils.ShardRange.ACTIVE)
        ]
        shard_ranges = list(orig_shard_ranges)
        shard_ranges.sort(key=utils.ShardRange.sort_key)
        self.assertEqual(shard_ranges[0], orig_shard_ranges[2])
        self.assertEqual(shard_ranges[1], orig_shard_ranges[1])
        self.assertEqual(shard_ranges[2], orig_shard_ranges[4])
        self.assertEqual(shard_ranges[3], orig_shard_ranges[5])
        self.assertEqual(shard_ranges[4], orig_shard_ranges[3])
        self.assertEqual(shard_ranges[5], orig_shard_ranges[6])
        self.assertEqual(shard_ranges[6], orig_shard_ranges[0])

    def test_is_child_of(self):
        # Set up some shard ranges in relational hierarchy:
        # account -> root -> grandparent -> parent -> child
        # using abbreviated names a_r_gp_p_c

        # account 1
        ts = next(self.ts_iter)
        a1_r1 = utils.ShardRange('a1/r1', ts)
        ts = next(self.ts_iter)
        a1_r1_gp1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', 'r1', ts, 1), ts)
        ts = next(self.ts_iter)
        a1_r1_gp1_p1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1.container, ts, 1), ts)
        ts = next(self.ts_iter)
        a1_r1_gp1_p1_c1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1_p1.container, ts, 1), ts)
        ts = next(self.ts_iter)
        a1_r1_gp1_p1_c2 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1_p1.container, ts, 2), ts)
        ts = next(self.ts_iter)
        a1_r1_gp1_p2 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1.container, ts, 2), ts)
        ts = next(self.ts_iter)
        a1_r1_gp2 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', 'r1', ts, 2), ts)  # different index
        ts = next(self.ts_iter)
        a1_r1_gp2_p1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp2.container, ts, 1), ts)
        # drop the index from grandparent name
        ts = next(self.ts_iter)
        rogue_a1_r1_gp = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', 'r1', ts, 1)[:-2], ts)

        # account 1, root 2
        ts = next(self.ts_iter)
        a1_r2 = utils.ShardRange('a1/r2', ts)
        ts = next(self.ts_iter)
        a1_r2_gp1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r2', a1_r2.container, ts, 1), ts)
        ts = next(self.ts_iter)
        a1_r2_gp1_p1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r2', a1_r2_gp1.container, ts, 3), ts)

        # account 2, root1
        a2_r1 = utils.ShardRange('a2/r1', ts)
        ts = next(self.ts_iter)
        a2_r1_gp1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a2', 'r1', a2_r1.container, ts, 1), ts)
        ts = next(self.ts_iter)
        a2_r1_gp1_p1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a2', 'r1', a2_r1_gp1.container, ts, 3), ts)

        # verify parent-child within same account.
        self.assertTrue(a1_r1_gp1.is_child_of(a1_r1))
        self.assertTrue(a1_r1_gp1_p1.is_child_of(a1_r1_gp1))
        self.assertTrue(a1_r1_gp1_p1_c1.is_child_of(a1_r1_gp1_p1))
        self.assertTrue(a1_r1_gp1_p1_c2.is_child_of(a1_r1_gp1_p1))
        self.assertTrue(a1_r1_gp1_p2.is_child_of(a1_r1_gp1))

        self.assertTrue(a1_r1_gp2.is_child_of(a1_r1))
        self.assertTrue(a1_r1_gp2_p1.is_child_of(a1_r1_gp2))

        self.assertTrue(a1_r2_gp1.is_child_of(a1_r2))
        self.assertTrue(a1_r2_gp1_p1.is_child_of(a1_r2_gp1))

        self.assertTrue(a2_r1_gp1.is_child_of(a2_r1))
        self.assertTrue(a2_r1_gp1_p1.is_child_of(a2_r1_gp1))

        # verify not parent-child within same account.
        self.assertFalse(a1_r1.is_child_of(a1_r1))
        self.assertFalse(a1_r1.is_child_of(a1_r2))

        self.assertFalse(a1_r1_gp1.is_child_of(a1_r2))
        self.assertFalse(a1_r1_gp1.is_child_of(a1_r1_gp1))
        self.assertFalse(a1_r1_gp1.is_child_of(a1_r1_gp1_p1))
        self.assertFalse(a1_r1_gp1.is_child_of(a1_r1_gp1_p1_c1))

        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r1))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r2))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r1_gp2))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r2_gp1))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(rogue_a1_r1_gp))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r1_gp1_p1))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r1_gp1_p2))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r2_gp1_p1))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r1_gp1_p1_c1))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a1_r1_gp1_p1_c2))

        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a1_r1))
        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a1_r1_gp1))
        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a1_r1_gp1_p2))
        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a1_r1_gp2_p1))
        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a1_r1_gp1_p1_c1))
        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a1_r1_gp1_p1_c2))
        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a1_r2_gp1_p1))
        self.assertFalse(a1_r1_gp1_p1_c1.is_child_of(a2_r1_gp1_p1))

        self.assertFalse(a1_r2_gp1.is_child_of(a1_r1))
        self.assertFalse(a1_r2_gp1_p1.is_child_of(a1_r1_gp1))

        # across different accounts, 'is_child_of' works in some cases but not
        # all, so don't use it for shard ranges in different accounts.
        self.assertFalse(a1_r1.is_child_of(a2_r1))
        self.assertFalse(a2_r1_gp1_p1.is_child_of(a1_r1_gp1))
        self.assertFalse(a1_r1_gp1_p1.is_child_of(a2_r1))
        self.assertTrue(a1_r1_gp1.is_child_of(a2_r1))
        self.assertTrue(a2_r1_gp1.is_child_of(a1_r1))

    def test_find_root(self):
        # account 1
        ts = next(self.ts_iter)
        a1_r1 = utils.ShardRange('a1/r1', ts)
        ts = next(self.ts_iter)
        a1_r1_gp1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', 'r1', ts, 1), ts, '', 'l')
        ts = next(self.ts_iter)
        a1_r1_gp1_p1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1.container, ts, 1), ts, 'a', 'k')
        ts = next(self.ts_iter)
        a1_r1_gp1_p1_c1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1_p1.container, ts, 1), ts, 'a', 'j')
        ts = next(self.ts_iter)
        a1_r1_gp1_p2 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1.container, ts, 2), ts, 'k', 'l')
        ts = next(self.ts_iter)
        a1_r1_gp2 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', 'r1', ts, 2), ts, 'l', '')  # different index

        # full ancestry plus some others
        all_shard_ranges = [a1_r1, a1_r1_gp1, a1_r1_gp1_p1, a1_r1_gp1_p1_c1,
                            a1_r1_gp1_p2, a1_r1_gp2]
        random.shuffle(all_shard_ranges)
        self.assertIsNone(a1_r1.find_root(all_shard_ranges))
        self.assertEqual(a1_r1, a1_r1_gp1.find_root(all_shard_ranges))
        self.assertEqual(a1_r1, a1_r1_gp1_p1.find_root(all_shard_ranges))
        self.assertEqual(a1_r1, a1_r1_gp1_p1_c1.find_root(all_shard_ranges))

        # missing a1_r1_gp1_p1
        all_shard_ranges = [a1_r1, a1_r1_gp1, a1_r1_gp1_p1_c1,
                            a1_r1_gp1_p2, a1_r1_gp2]
        random.shuffle(all_shard_ranges)
        self.assertIsNone(a1_r1.find_root(all_shard_ranges))
        self.assertEqual(a1_r1, a1_r1_gp1.find_root(all_shard_ranges))
        self.assertEqual(a1_r1, a1_r1_gp1_p1.find_root(all_shard_ranges))
        self.assertEqual(a1_r1, a1_r1_gp1_p1_c1.find_root(all_shard_ranges))

        # empty list
        self.assertIsNone(a1_r1_gp1_p1_c1.find_root([]))

        # double entry
        all_shard_ranges = [a1_r1, a1_r1, a1_r1_gp1, a1_r1_gp1]
        random.shuffle(all_shard_ranges)
        self.assertEqual(a1_r1, a1_r1_gp1_p1.find_root(all_shard_ranges))
        self.assertEqual(a1_r1, a1_r1_gp1_p1_c1.find_root(all_shard_ranges))

    def test_find_ancestors(self):
        # account 1
        ts = next(self.ts_iter)
        a1_r1 = utils.ShardRange('a1/r1', ts)
        ts = next(self.ts_iter)
        a1_r1_gp1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', 'r1', ts, 1), ts, '', 'l')
        ts = next(self.ts_iter)
        a1_r1_gp1_p1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1.container, ts, 1), ts, 'a', 'k')
        ts = next(self.ts_iter)
        a1_r1_gp1_p1_c1 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1_p1.container, ts, 1), ts, 'a', 'j')
        ts = next(self.ts_iter)
        a1_r1_gp1_p2 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', a1_r1_gp1.container, ts, 2), ts, 'k', 'l')
        ts = next(self.ts_iter)
        a1_r1_gp2 = utils.ShardRange(utils.ShardRange.make_path(
            '.shards_a1', 'r1', 'r1', ts, 2), ts, 'l', '')  # different index

        # full ancestry plus some others
        all_shard_ranges = [a1_r1, a1_r1_gp1, a1_r1_gp1_p1, a1_r1_gp1_p1_c1,
                            a1_r1_gp1_p2, a1_r1_gp2]
        random.shuffle(all_shard_ranges)
        self.assertEqual([], a1_r1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1], a1_r1_gp1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1_gp1, a1_r1],
                         a1_r1_gp1_p1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1_gp1_p1, a1_r1_gp1, a1_r1],
                         a1_r1_gp1_p1_c1.find_ancestors(all_shard_ranges))

        # missing a1_r1_gp1_p1
        all_shard_ranges = [a1_r1, a1_r1_gp1, a1_r1_gp1_p1_c1,
                            a1_r1_gp1_p2, a1_r1_gp2]
        random.shuffle(all_shard_ranges)
        self.assertEqual([], a1_r1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1], a1_r1_gp1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1_gp1, a1_r1],
                         a1_r1_gp1_p1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1],
                         a1_r1_gp1_p1_c1.find_ancestors(all_shard_ranges))

        # missing a1_r1_gp1
        all_shard_ranges = [a1_r1, a1_r1_gp1_p1, a1_r1_gp1_p1_c1,
                            a1_r1_gp1_p2, a1_r1_gp2]
        random.shuffle(all_shard_ranges)
        self.assertEqual([], a1_r1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1], a1_r1_gp1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1],
                         a1_r1_gp1_p1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1_gp1_p1, a1_r1],
                         a1_r1_gp1_p1_c1.find_ancestors(all_shard_ranges))

        # empty list
        self.assertEqual([], a1_r1_gp1_p1_c1.find_ancestors([]))
        # double entry
        all_shard_ranges = [a1_r1, a1_r1, a1_r1_gp1, a1_r1_gp1]
        random.shuffle(all_shard_ranges)
        self.assertEqual([a1_r1_gp1, a1_r1],
                         a1_r1_gp1_p1.find_ancestors(all_shard_ranges))
        self.assertEqual([a1_r1],
                         a1_r1_gp1_p1_c1.find_ancestors(all_shard_ranges))
        all_shard_ranges = [a1_r1, a1_r1, a1_r1_gp1_p1, a1_r1_gp1_p1]
        random.shuffle(all_shard_ranges)
        self.assertEqual([a1_r1_gp1_p1, a1_r1],
                         a1_r1_gp1_p1_c1.find_ancestors(all_shard_ranges))


class TestShardRangeList(unittest.TestCase):
    def setUp(self):
        self.ts_iter = make_timestamp_iter()
        self.t1 = next(self.ts_iter)
        self.t2 = next(self.ts_iter)
        self.ts_iter = make_timestamp_iter()
        self.shard_ranges = [
            utils.ShardRange('a/b', self.t1, 'a', 'b',
                             object_count=2, bytes_used=22, tombstones=222),
            utils.ShardRange('b/c', self.t2, 'b', 'c',
                             object_count=4, bytes_used=44, tombstones=444),
            utils.ShardRange('c/y', self.t1, 'c', 'y',
                             object_count=6, bytes_used=66),
        ]

    def test_init(self):
        srl = ShardRangeList()
        self.assertEqual(0, len(srl))
        self.assertEqual(utils.ShardRange.MIN, srl.lower)
        self.assertEqual(utils.ShardRange.MIN, srl.upper)
        self.assertEqual(0, srl.object_count)
        self.assertEqual(0, srl.bytes_used)
        self.assertEqual(0, srl.row_count)

    def test_init_with_list(self):
        srl = ShardRangeList(self.shard_ranges[:2])
        self.assertEqual(2, len(srl))
        self.assertEqual('a', srl.lower)
        self.assertEqual('c', srl.upper)
        self.assertEqual(6, srl.object_count)
        self.assertEqual(66, srl.bytes_used)
        self.assertEqual(672, srl.row_count)

        srl.append(self.shard_ranges[2])
        self.assertEqual(3, len(srl))
        self.assertEqual('a', srl.lower)
        self.assertEqual('y', srl.upper)
        self.assertEqual(12, srl.object_count)
        self.assertEqual(132, srl.bytes_used)
        self.assertEqual(-1, self.shard_ranges[2].tombstones)  # sanity check
        self.assertEqual(678, srl.row_count)  # NB: tombstones=-1 not counted

    def test_pop(self):
        srl = ShardRangeList(self.shard_ranges[:2])
        srl.pop()
        self.assertEqual(1, len(srl))
        self.assertEqual('a', srl.lower)
        self.assertEqual('b', srl.upper)
        self.assertEqual(2, srl.object_count)
        self.assertEqual(22, srl.bytes_used)
        self.assertEqual(224, srl.row_count)

    def test_slice(self):
        srl = ShardRangeList(self.shard_ranges)
        sublist = srl[:1]
        self.assertIsInstance(sublist, ShardRangeList)
        self.assertEqual(1, len(sublist))
        self.assertEqual('a', sublist.lower)
        self.assertEqual('b', sublist.upper)
        self.assertEqual(2, sublist.object_count)
        self.assertEqual(22, sublist.bytes_used)
        self.assertEqual(224, sublist.row_count)

        sublist = srl[1:]
        self.assertIsInstance(sublist, ShardRangeList)
        self.assertEqual(2, len(sublist))
        self.assertEqual('b', sublist.lower)
        self.assertEqual('y', sublist.upper)
        self.assertEqual(10, sublist.object_count)
        self.assertEqual(110, sublist.bytes_used)
        self.assertEqual(454, sublist.row_count)

    def test_includes(self):
        srl = ShardRangeList(self.shard_ranges)

        for sr in self.shard_ranges:
            self.assertTrue(srl.includes(sr))

        self.assertTrue(srl.includes(srl))

        sr = utils.ShardRange('a/a', utils.Timestamp.now(), '', 'a')
        self.assertFalse(srl.includes(sr))
        sr = utils.ShardRange('a/a', utils.Timestamp.now(), '', 'b')
        self.assertFalse(srl.includes(sr))
        sr = utils.ShardRange('a/z', utils.Timestamp.now(), 'x', 'z')
        self.assertFalse(srl.includes(sr))
        sr = utils.ShardRange('a/z', utils.Timestamp.now(), 'y', 'z')
        self.assertFalse(srl.includes(sr))
        sr = utils.ShardRange('a/entire', utils.Timestamp.now(), '', '')
        self.assertFalse(srl.includes(sr))

        # entire range
        srl_entire = ShardRangeList([sr])
        self.assertFalse(srl.includes(srl_entire))
        # make a fresh instance
        sr = utils.ShardRange('a/entire', utils.Timestamp.now(), '', '')
        self.assertTrue(srl_entire.includes(sr))

    def test_timestamps(self):
        srl = ShardRangeList(self.shard_ranges)
        self.assertEqual({self.t1, self.t2}, srl.timestamps)
        t3 = next(self.ts_iter)
        self.shard_ranges[2].timestamp = t3
        self.assertEqual({self.t1, self.t2, t3}, srl.timestamps)
        srl.pop(0)
        self.assertEqual({self.t2, t3}, srl.timestamps)

    def test_states(self):
        srl = ShardRangeList()
        self.assertEqual(set(), srl.states)

        srl = ShardRangeList(self.shard_ranges)
        self.shard_ranges[0].update_state(
            utils.ShardRange.CREATED, next(self.ts_iter))
        self.shard_ranges[1].update_state(
            utils.ShardRange.CLEAVED, next(self.ts_iter))
        self.shard_ranges[2].update_state(
            utils.ShardRange.ACTIVE, next(self.ts_iter))

        self.assertEqual({utils.ShardRange.CREATED,
                          utils.ShardRange.CLEAVED,
                          utils.ShardRange.ACTIVE},
                         srl.states)

    def test_filter(self):
        srl = ShardRangeList(self.shard_ranges)
        self.assertEqual(self.shard_ranges, srl.filter())
        self.assertEqual(self.shard_ranges,
                         srl.filter(marker='', end_marker=''))
        self.assertEqual(self.shard_ranges,
                         srl.filter(marker=utils.ShardRange.MIN,
                                    end_marker=utils.ShardRange.MAX))
        self.assertEqual([], srl.filter(marker=utils.ShardRange.MAX,
                                        end_marker=utils.ShardRange.MIN))
        self.assertEqual([], srl.filter(marker=utils.ShardRange.MIN,
                                        end_marker=utils.ShardRange.MIN))
        self.assertEqual([], srl.filter(marker=utils.ShardRange.MAX,
                                        end_marker=utils.ShardRange.MAX))
        self.assertEqual(self.shard_ranges[:1],
                         srl.filter(marker='', end_marker='b'))
        self.assertEqual(self.shard_ranges[1:3],
                         srl.filter(marker='b', end_marker='y'))
        self.assertEqual([],
                         srl.filter(marker='y', end_marker='y'))
        self.assertEqual([],
                         srl.filter(marker='y', end_marker='x'))
        # includes trumps marker & end_marker
        self.assertEqual(self.shard_ranges[0:1],
                         srl.filter(includes='b', marker='c', end_marker='y'))
        self.assertEqual(self.shard_ranges[0:1],
                         srl.filter(includes='b', marker='', end_marker=''))
        self.assertEqual([], srl.filter(includes='z'))

    def test_find_lower(self):
        srl = ShardRangeList(self.shard_ranges)
        self.shard_ranges[0].update_state(
            utils.ShardRange.CREATED, next(self.ts_iter))
        self.shard_ranges[1].update_state(
            utils.ShardRange.CLEAVED, next(self.ts_iter))
        self.shard_ranges[2].update_state(
            utils.ShardRange.ACTIVE, next(self.ts_iter))

        def do_test(states):
            return srl.find_lower(lambda sr: sr.state in states)

        self.assertEqual(srl.upper,
                         do_test([utils.ShardRange.FOUND]))
        self.assertEqual(self.shard_ranges[0].lower,
                         do_test([utils.ShardRange.CREATED]))
        self.assertEqual(self.shard_ranges[0].lower,
                         do_test((utils.ShardRange.CREATED,
                                  utils.ShardRange.CLEAVED)))
        self.assertEqual(self.shard_ranges[1].lower,
                         do_test((utils.ShardRange.ACTIVE,
                                  utils.ShardRange.CLEAVED)))
        self.assertEqual(self.shard_ranges[2].lower,
                         do_test([utils.ShardRange.ACTIVE]))


class TestFsync(unittest.TestCase):

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


@patch('ctypes.get_errno')
@patch.object(utils, '_sys_posix_fallocate')
@patch.object(utils, '_sys_fallocate')
@patch.object(utils, 'FALLOCATE_RESERVE', 0)
class TestFallocate(unittest.TestCase):
    def test_fallocate(self, sys_fallocate_mock,
                       sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        utils.fallocate(1234, 5000 * 2 ** 20)

        # We can't use sys_fallocate_mock.assert_called_once_with because no
        # two ctypes.c_uint64 objects are equal even if their values are
        # equal. Yes, ctypes.c_uint64(123) != ctypes.c_uint64(123).
        calls = sys_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 4)
        self.assertEqual(args[0], 1234)
        self.assertEqual(args[1], utils.FALLOC_FL_KEEP_SIZE)
        self.assertEqual(args[2].value, 0)
        self.assertEqual(args[3].value, 5000 * 2 ** 20)

        sys_posix_fallocate_mock.assert_not_called()

    def test_fallocate_offset(self, sys_fallocate_mock,
                              sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        utils.fallocate(1234, 5000 * 2 ** 20, offset=3 * 2 ** 30)
        calls = sys_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 4)
        self.assertEqual(args[0], 1234)
        self.assertEqual(args[1], utils.FALLOC_FL_KEEP_SIZE)
        self.assertEqual(args[2].value, 3 * 2 ** 30)
        self.assertEqual(args[3].value, 5000 * 2 ** 20)

        sys_posix_fallocate_mock.assert_not_called()

    def test_fallocate_fatal_error(self, sys_fallocate_mock,
                                   sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = -1
        get_errno_mock.return_value = errno.EIO

        with self.assertRaises(OSError) as cm:
            utils.fallocate(1234, 5000 * 2 ** 20)
        self.assertEqual(cm.exception.errno, errno.EIO)

    def test_fallocate_silent_errors(self, sys_fallocate_mock,
                                     sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = -1

        for silent_error in (0, errno.ENOSYS, errno.EOPNOTSUPP, errno.EINVAL):
            get_errno_mock.return_value = silent_error
            try:
                utils.fallocate(1234, 5678)
            except OSError:
                self.fail("fallocate() raised an error on %d", silent_error)

    def test_posix_fallocate_fallback(self, sys_fallocate_mock,
                                      sys_posix_fallocate_mock,
                                      get_errno_mock):
        sys_fallocate_mock.available = False
        sys_fallocate_mock.side_effect = NotImplementedError

        sys_posix_fallocate_mock.available = True
        sys_posix_fallocate_mock.return_value = 0

        utils.fallocate(1234, 567890)
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

        utils.fallocate(1234, 5000 * 2 ** 20, offset=3 * 2 ** 30)
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
                mock.patch.object(utils, "_fallocate_warned_about_missing",
                                  False):
            utils.fallocate(321, 654)
            utils.fallocate(321, 654)

        sys_fallocate_mock.assert_not_called()
        sys_posix_fallocate_mock.assert_not_called()
        get_errno_mock.assert_not_called()

        self.assertEqual(len(warning_mock.mock_calls), 1)

    def test_arg_bounds(self, sys_fallocate_mock,
                        sys_posix_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0
        with self.assertRaises(ValueError):
            utils.fallocate(0, 1 << 64, 0)
        with self.assertRaises(ValueError):
            utils.fallocate(0, 0, -1)
        with self.assertRaises(ValueError):
            utils.fallocate(0, 0, 1 << 64)
        self.assertEqual([], sys_fallocate_mock.mock_calls)
        # sanity check
        utils.fallocate(0, 0, 0)
        self.assertEqual(
            [mock.call(0, utils.FALLOC_FL_KEEP_SIZE, mock.ANY, mock.ANY)],
            sys_fallocate_mock.mock_calls)
        # Go confirm the ctypes values separately; apparently == doesn't
        # work the way you'd expect with ctypes :-/
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][2].value, 0)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][3].value, 0)
        sys_fallocate_mock.reset_mock()

        # negative size will be adjusted as 0
        utils.fallocate(0, -1, 0)
        self.assertEqual(
            [mock.call(0, utils.FALLOC_FL_KEEP_SIZE, mock.ANY, mock.ANY)],
            sys_fallocate_mock.mock_calls)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][2].value, 0)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][3].value, 0)


@patch.object(os, 'fstatvfs')
@patch.object(utils, '_sys_fallocate', available=True, return_value=0)
@patch.object(utils, 'FALLOCATE_RESERVE', 0)
@patch.object(utils, 'FALLOCATE_IS_PERCENT', False)
@patch.object(utils, '_fallocate_enabled', True)
class TestFallocateReserve(unittest.TestCase):
    def _statvfs_result(self, f_frsize, f_bavail):
        # Only 3 values are relevant to us, so use zeros for the rest
        f_blocks = 100
        return posix.statvfs_result((0, f_frsize, f_blocks, 0, f_bavail,
                                     0, 0, 0, 0, 0))

    def test_disabled(self, sys_fallocate_mock, fstatvfs_mock):
        utils.disable_fallocate()
        utils.fallocate(123, 456)

        sys_fallocate_mock.assert_not_called()
        fstatvfs_mock.assert_not_called()

    def test_zero_reserve(self, sys_fallocate_mock, fstatvfs_mock):
        utils.fallocate(123, 456)

        fstatvfs_mock.assert_not_called()
        self.assertEqual(len(sys_fallocate_mock.mock_calls), 1)

    def test_enough_space(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1023 allocated, and have 2 blocks
        # of size 1024 free, so succeed
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 2)
        utils.fallocate(88, 1023)

    def test_not_enough_space(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1024 allocated, and have 2 blocks
        # of size 1024 free, so fail
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 2)
        with self.assertRaises(OSError) as catcher:
            utils.fallocate(88, 1024)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 1024'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_not_enough_space_large(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1GB allocated, and have 2 blocks
        # of size 1024 free, so fail
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 2)
        with self.assertRaises(OSError) as catcher:
            utils.fallocate(88, 1 << 30)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail %g <= 1024'
            % (errno.ENOSPC, ((2 * 1024) - (1 << 30))))
        sys_fallocate_mock.assert_not_called()

    def test_enough_space_small_blocks(self, sys_fallocate_mock,
                                       fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1023 allocated, and have 4 blocks
        # of size 512 free, so succeed
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(512, 4)
        utils.fallocate(88, 1023)

    def test_not_enough_space_small_blocks(self, sys_fallocate_mock,
                                           fstatvfs_mock):
        # Want 1024 bytes in reserve plus 1024 allocated, and have 4 blocks
        # of size 512 free, so fail
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('1024')

        fstatvfs_mock.return_value = self._statvfs_result(512, 4)
        with self.assertRaises(OSError) as catcher:
            utils.fallocate(88, 1024)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1024 <= 1024'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_free_space_under_reserve(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 2048 bytes in reserve but have only 3 blocks of size 512, so
        # allocating even 0 bytes fails
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('2048')

        fstatvfs_mock.return_value = self._statvfs_result(512, 3)
        with self.assertRaises(OSError) as catcher:
            utils.fallocate(88, 0)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1536 <= 2048'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_all_reserved(self, sys_fallocate_mock, fstatvfs_mock):
        # Filesystem is empty, but our reserve is bigger than the
        # filesystem, so any allocation will fail
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('9999999999999')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 100)
        self.assertRaises(OSError, utils.fallocate, 88, 0)
        sys_fallocate_mock.assert_not_called()

    def test_enough_space_pct(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1% reserved, filesystem has 3/100 blocks of size 1024 free
        # and file size is 2047, so succeed
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('1%')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 3)
        utils.fallocate(88, 2047)

    def test_not_enough_space_pct(self, sys_fallocate_mock, fstatvfs_mock):
        # Want 1% reserved, filesystem has 3/100 blocks of size 1024 free
        # and file size is 2048, so fail
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('1%')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 3)
        with self.assertRaises(OSError) as catcher:
            utils.fallocate(88, 2048)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 1 <= 1'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()

    def test_all_space_reserved_pct(self, sys_fallocate_mock, fstatvfs_mock):
        # Filesystem is empty, but our reserve is the whole filesystem, so
        # any allocation will fail
        utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
            utils.config_fallocate_value('100%')

        fstatvfs_mock.return_value = self._statvfs_result(1024, 100)
        with self.assertRaises(OSError) as catcher:
            utils.fallocate(88, 0)
        self.assertEqual(
            str(catcher.exception),
            '[Errno %d] FALLOCATE_RESERVE fail 100 <= 100'
            % errno.ENOSPC)
        sys_fallocate_mock.assert_not_called()


@patch('ctypes.get_errno')
@patch.object(utils, '_sys_fallocate')
class TestPunchHole(unittest.TestCase):
    def test_punch_hole(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        utils.punch_hole(123, 456, 789)

        calls = sys_fallocate_mock.mock_calls
        self.assertEqual(len(calls), 1)
        args = calls[0][1]
        self.assertEqual(len(args), 4)
        self.assertEqual(args[0], 123)
        self.assertEqual(
            args[1], utils.FALLOC_FL_PUNCH_HOLE | utils.FALLOC_FL_KEEP_SIZE)
        self.assertEqual(args[2].value, 456)
        self.assertEqual(args[3].value, 789)

    def test_error(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = -1
        get_errno_mock.return_value = errno.EISDIR

        with self.assertRaises(OSError) as cm:
            utils.punch_hole(123, 456, 789)
        self.assertEqual(cm.exception.errno, errno.EISDIR)

    def test_arg_bounds(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = True
        sys_fallocate_mock.return_value = 0

        with self.assertRaises(ValueError):
            utils.punch_hole(0, 1, -1)
        with self.assertRaises(ValueError):
            utils.punch_hole(0, 1 << 64, 1)
        with self.assertRaises(ValueError):
            utils.punch_hole(0, -1, 1)
        with self.assertRaises(ValueError):
            utils.punch_hole(0, 1, 0)
        with self.assertRaises(ValueError):
            utils.punch_hole(0, 1, 1 << 64)
        self.assertEqual([], sys_fallocate_mock.mock_calls)

        # sanity check
        utils.punch_hole(0, 0, 1)
        self.assertEqual(
            [mock.call(
                0, utils.FALLOC_FL_PUNCH_HOLE | utils.FALLOC_FL_KEEP_SIZE,
                mock.ANY, mock.ANY)],
            sys_fallocate_mock.mock_calls)
        # Go confirm the ctypes values separately; apparently == doesn't
        # work the way you'd expect with ctypes :-/
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][2].value, 0)
        self.assertEqual(sys_fallocate_mock.mock_calls[0][1][3].value, 1)

    def test_no_fallocate(self, sys_fallocate_mock, get_errno_mock):
        sys_fallocate_mock.available = False

        with self.assertRaises(OSError) as cm:
            utils.punch_hole(123, 456, 789)
        self.assertEqual(cm.exception.errno, errno.ENOTSUP)


class TestPunchHoleReally(unittest.TestCase):
    def setUp(self):
        if not utils._sys_fallocate.available:
            raise unittest.SkipTest("utils._sys_fallocate not available")

    def test_punch_a_hole(self):
        with TemporaryFile() as tf:
            tf.write(b"x" * 64 + b"y" * 64 + b"z" * 64)
            tf.flush()

            # knock out the first half of the "y"s
            utils.punch_hole(tf.fileno(), 64, 32)

            tf.seek(0)
            contents = tf.read(4096)
            self.assertEqual(
                contents,
                b"x" * 64 + b"\0" * 32 + b"y" * 32 + b"z" * 64)


class TestWatchdog(unittest.TestCase):
    def test_start_stop(self):
        w = utils.Watchdog()
        w._evt.send = mock.Mock(side_effect=w._evt.send)
        gth = object()

        now = time.time()
        timeout_value = 1.0
        with patch('eventlet.greenthread.getcurrent', return_value=gth), \
                patch('time.time', return_value=now):
            # On first call, _next_expiration is None, it should unblock
            # greenthread that is blocked for ever
            key = w.start(timeout_value, Timeout)
            self.assertIn(key, w._timeouts)
            self.assertEqual(w._timeouts[key], (
                timeout_value, now + timeout_value, gth, Timeout, now))
            w._evt.send.assert_called_once()

            w.stop(key)
            self.assertNotIn(key, w._timeouts)

    def test_timeout_concurrency(self):
        w = utils.Watchdog()
        w._evt.send = mock.Mock(side_effect=w._evt.send)
        w._evt.wait = mock.Mock()
        gth = object()

        w._run()
        w._evt.wait.assert_called_once_with(None)

        with patch('eventlet.greenthread.getcurrent', return_value=gth):
            w._evt.send.reset_mock()
            w._evt.wait.reset_mock()
            with patch('time.time', return_value=10.00):
                # On first call, _next_expiration is None, it should unblock
                # greenthread that is blocked for ever
                w.start(5.0, Timeout)  # Will end at 15.0
                w._evt.send.assert_called_once()

            with patch('time.time', return_value=10.01):
                w._run()
                self.assertEqual(15.0, w._next_expiration)
                w._evt.wait.assert_called_once_with(15.0 - 10.01)

            w._evt.send.reset_mock()
            w._evt.wait.reset_mock()
            with patch('time.time', return_value=12.00):
                # Now _next_expiration is 15.0, it won't unblock greenthread
                # because this expiration is later
                w.start(5.0, Timeout)  # Will end at 17.0
                w._evt.send.assert_not_called()

            w._evt.send.reset_mock()
            w._evt.wait.reset_mock()
            with patch('time.time', return_value=14.00):
                # Now _next_expiration is still 15.0, it will unblock
                # greenthread because this new expiration is 14.5
                w.start(0.5, Timeout)  # Will end at 14.5
                w._evt.send.assert_called_once()

            with patch('time.time', return_value=14.01):
                w._run()
                w._evt.wait.assert_called_once_with(14.5 - 14.01)
                self.assertEqual(14.5, w._next_expiration)
                # Should wakeup at 14.5

    def test_timeout_expire(self):
        w = utils.Watchdog()
        w._evt.send = mock.Mock()  # To avoid it to call get_hub()
        w._evt.wait = mock.Mock()  # To avoid it to call get_hub()

        with patch('eventlet.hubs.get_hub') as m_gh:
            with patch('time.time', return_value=10.0):
                w.start(5.0, Timeout)  # Will end at 15.0

            with patch('time.time', return_value=16.0):
                w._run()
                m_gh.assert_called_once()
                m_gh.return_value.schedule_call_global.assert_called_once()
                exc = m_gh.return_value.schedule_call_global.call_args[0][2]
                self.assertIsInstance(exc, Timeout)
                self.assertEqual(exc.seconds, 5.0)
                self.assertEqual(None, w._next_expiration)
                w._evt.wait.assert_called_once_with(None)


class TestReiterate(unittest.TestCase):
    def test_reiterate_consumes_first(self):
        test_iter = FakeIterable([1, 2, 3])
        reiterated = utils.reiterate(test_iter)
        self.assertEqual(1, test_iter.next_call_count)
        self.assertEqual(1, next(reiterated))
        self.assertEqual(1, test_iter.next_call_count)
        self.assertEqual(2, next(reiterated))
        self.assertEqual(2, test_iter.next_call_count)
        self.assertEqual(3, next(reiterated))
        self.assertEqual(3, test_iter.next_call_count)

    def test_reiterate_closes(self):
        test_iter = FakeIterable([1, 2, 3])
        self.assertEqual(0, test_iter.close_call_count)
        reiterated = utils.reiterate(test_iter)
        self.assertEqual(0, test_iter.close_call_count)
        self.assertTrue(hasattr(reiterated, 'close'))
        self.assertTrue(callable(reiterated.close))
        reiterated.close()
        self.assertEqual(1, test_iter.close_call_count)

        # empty iter gets closed when reiterated
        test_iter = FakeIterable([])
        self.assertEqual(0, test_iter.close_call_count)
        reiterated = utils.reiterate(test_iter)
        self.assertFalse(hasattr(reiterated, 'close'))
        self.assertEqual(1, test_iter.close_call_count)

    def test_reiterate_list_or_tuple(self):
        test_list = [1, 2]
        reiterated = utils.reiterate(test_list)
        self.assertIs(test_list, reiterated)
        test_tuple = (1, 2)
        reiterated = utils.reiterate(test_tuple)
        self.assertIs(test_tuple, reiterated)


class TestClosingIterator(unittest.TestCase):
    def _make_gen(self, items, captured_exit):
        def gen():
            try:
                for it in items:
                    if isinstance(it, Exception):
                        raise it
                    yield it
            except GeneratorExit as e:
                captured_exit.append(e)
                raise
        return gen()

    def test_close(self):
        wrapped = FakeIterable([1, 2, 3])
        # note: iter(FakeIterable) is the same object
        self.assertIs(wrapped, iter(wrapped))
        it = utils.ClosingIterator(wrapped)
        actual = [x for x in it]
        self.assertEqual([1, 2, 3], actual)
        self.assertEqual(1, wrapped.close_call_count)
        it.close()
        self.assertEqual(1, wrapped.close_call_count)

    def test_close_others(self):
        wrapped = FakeIterable([1, 2, 3])
        others = [FakeIterable([4, 5, 6]), FakeIterable([])]
        self.assertIs(wrapped, iter(wrapped))
        it = utils.ClosingIterator(wrapped, others)
        actual = [x for x in it]
        self.assertEqual([1, 2, 3], actual)
        self.assertEqual([1, 1, 1],
                         [i.close_call_count for i in others + [wrapped]])
        it.close()
        self.assertEqual([1, 1, 1],
                         [i.close_call_count for i in others + [wrapped]])

    def test_close_gen(self):
        # explicitly check generator closing
        captured_exit = []
        gen = self._make_gen([1, 2], captured_exit)
        it = utils.ClosingIterator(gen)
        self.assertFalse(captured_exit)
        it.close()
        self.assertFalse(captured_exit)  # the generator didn't start

        captured_exit = []
        gen = self._make_gen([1, 2], captured_exit)
        it = utils.ClosingIterator(gen)
        self.assertFalse(captured_exit)
        self.assertEqual(1, next(it))  # start the generator
        it.close()
        self.assertEqual(1, len(captured_exit))

    def test_close_wrapped_is_not_same_as_iter(self):
        class AltFakeIterable(FakeIterable):
            def __iter__(self):
                return (x for x in self.values)

        wrapped = AltFakeIterable([1, 2, 3])
        # note: iter(AltFakeIterable) is a generator, not the same object
        self.assertIsNot(wrapped, iter(wrapped))
        it = utils.ClosingIterator(wrapped)
        actual = [x for x in it]
        self.assertEqual([1, 2, 3], actual)
        self.assertEqual(1, wrapped.close_call_count)
        it.close()
        self.assertEqual(1, wrapped.close_call_count)

    def test_init_with_iterable(self):
        wrapped = [1, 2, 3]  # list is iterable but not an iterator
        it = utils.ClosingIterator(wrapped)
        actual = [x for x in it]
        self.assertEqual([1, 2, 3], actual)
        it.close()  # safe to call even though list has no close

    def test_nested_iters(self):
        wrapped = FakeIterable([1, 2, 3])
        it = utils.ClosingIterator(utils.ClosingIterator(wrapped))
        actual = [x for x in it]
        self.assertEqual([1, 2, 3], actual)
        self.assertEqual(1, wrapped.close_call_count)
        it.close()
        self.assertEqual(1, wrapped.close_call_count)

    def test_close_on_stop_iteration(self):
        wrapped = FakeIterable([1, 2, 3])
        others = [FakeIterable([4, 5, 6]), FakeIterable([])]
        self.assertIs(wrapped, iter(wrapped))
        it = utils.ClosingIterator(wrapped, others)
        actual = [x for x in it]
        self.assertEqual([1, 2, 3], actual)
        self.assertEqual([1, 1, 1],
                         [i.close_call_count for i in others + [wrapped]])
        it.close()
        self.assertEqual([1, 1, 1],
                         [i.close_call_count for i in others + [wrapped]])

    def test_close_on_exception(self):
        # sanity check: generator exits on raising exception without executing
        # GeneratorExit
        captured_exit = []
        gen = self._make_gen([1, ValueError(), 2], captured_exit)
        self.assertEqual(1, next(gen))
        with self.assertRaises(ValueError):
            next(gen)
        self.assertFalse(captured_exit)
        gen.close()
        self.assertFalse(captured_exit)  # gen already exited

        captured_exit = []
        gen = self._make_gen([1, ValueError(), 2], captured_exit)
        self.assertEqual(1, next(gen))
        with self.assertRaises(ValueError):
            next(gen)
        self.assertFalse(captured_exit)
        with self.assertRaises(StopIteration):
            next(gen)  # gen already exited

        # wrapped gen does the same...
        captured_exit = []
        gen = self._make_gen([1, ValueError(), 2], captured_exit)
        others = [FakeIterable([4, 5, 6]), FakeIterable([])]
        it = utils.ClosingIterator(gen, others)
        self.assertEqual(1, next(it))
        with self.assertRaises(ValueError):
            next(it)
        self.assertFalse(captured_exit)
        # but other iters are closed :)
        self.assertEqual([1, 1], [i.close_call_count for i in others])


class TestClosingMapper(unittest.TestCase):
    def test_close(self):
        calls = []

        def func(args):
            calls.append(args)
            return sum(args)

        wrapped = FakeIterable([(2, 3), (4, 5)])
        other = FakeIterable([])
        it = utils.ClosingMapper(func, wrapped, [other])
        actual = [x for x in it]
        self.assertEqual([(2, 3), (4, 5)], calls)
        self.assertEqual([5, 9], actual)
        self.assertEqual(1, wrapped.close_call_count)
        self.assertEqual(1, other.close_call_count)
        # check against result of map()
        wrapped = FakeIterable([(2, 3), (4, 5)])
        mapped = [x for x in map(func, wrapped)]
        self.assertEqual(mapped, actual)

    def test_function_raises_exception(self):
        calls = []

        class TestExc(Exception):
            pass

        def func(args):
            calls.append(args)
            if len(calls) > 1:
                raise TestExc('boom')
            else:
                return sum(args)

        wrapped = FakeIterable([(2, 3), (4, 5), (6, 7)])
        it = utils.ClosingMapper(func, wrapped)
        self.assertEqual(5, next(it))
        with self.assertRaises(TestExc) as cm:
            next(it)
        self.assertIn('boom', str(cm.exception))
        self.assertEqual(1, wrapped.close_call_count)
        with self.assertRaises(StopIteration) as cm:
            next(it)


class TestCloseableChain(unittest.TestCase):
    def test_closeable_chain_iterates(self):
        test_iter1 = FakeIterable([1])
        test_iter2 = FakeIterable([2, 3])
        chain = utils.CloseableChain(test_iter1, test_iter2)
        self.assertEqual([1, 2, 3], [x for x in chain])

        chain = utils.CloseableChain([1, 2], [3])
        self.assertEqual([1, 2, 3], [x for x in chain])

    def test_closeable_chain_closes(self):
        test_iter1 = FakeIterable([1])
        test_iter2 = FakeIterable([2, 3])
        chain = utils.CloseableChain(test_iter1, test_iter2)
        self.assertEqual(0, test_iter1.close_call_count)
        self.assertEqual(0, test_iter2.close_call_count)
        chain.close()
        self.assertEqual(1, test_iter1.close_call_count)
        self.assertEqual(1, test_iter2.close_call_count)

        # check that close is safe to call even when component iters have no
        # close
        chain = utils.CloseableChain([1, 2], [3])
        chain.close()
        # read after close raises StopIteration
        self.assertEqual([], [x for x in chain])

        # check with generator in the chain
        generator_closed = [False]

        def gen():
            try:
                yield 2
                yield 3
            except GeneratorExit:
                generator_closed[0] = True
                raise

        test_iter1 = FakeIterable([1])
        chain = utils.CloseableChain(test_iter1, gen())
        self.assertEqual(0, test_iter1.close_call_count)
        self.assertFalse(generator_closed[0])
        chain.close()
        self.assertEqual(1, test_iter1.close_call_count)
        # Generator never kicked off, so there's no GeneratorExit
        self.assertFalse(generator_closed[0])

        test_iter1 = FakeIterable([1])
        chain = utils.CloseableChain(gen(), test_iter1)
        self.assertEqual(2, next(chain))  # Kick off the generator
        self.assertEqual(0, test_iter1.close_call_count)
        self.assertFalse(generator_closed[0])
        chain.close()
        self.assertEqual(1, test_iter1.close_call_count)
        self.assertTrue(generator_closed[0])


class TestStringAlong(unittest.TestCase):
    def test_happy(self):
        logger = debug_logger()
        it = FakeIterable([1, 2, 3])
        other_it = FakeIterable([])
        string_along = utils.StringAlong(
            it, other_it, lambda: logger.warning('boom'))
        for i, x in enumerate(string_along):
            self.assertEqual(i + 1, x)
            self.assertEqual(0, other_it.next_call_count, x)
            self.assertEqual(0, other_it.close_call_count, x)
        self.assertEqual(1, other_it.next_call_count, x)
        self.assertEqual(1, other_it.close_call_count, x)
        lines = logger.get_lines_for_level('warning')
        self.assertFalse(lines)

    def test_unhappy(self):
        logger = debug_logger()
        it = FakeIterable([1, 2, 3])
        other_it = FakeIterable([1])
        string_along = utils.StringAlong(
            it, other_it, lambda: logger.warning('boom'))
        for i, x in enumerate(string_along):
            self.assertEqual(i + 1, x)
            self.assertEqual(0, other_it.next_call_count, x)
            self.assertEqual(0, other_it.close_call_count, x)
        self.assertEqual(1, other_it.next_call_count, x)
        self.assertEqual(1, other_it.close_call_count, x)
        lines = logger.get_lines_for_level('warning')
        self.assertEqual(1, len(lines))
        self.assertIn('boom', lines[0])


class TestCooperativeIterator(unittest.TestCase):
    def test_init(self):
        wrapped = itertools.count()
        it = utils.CooperativeIterator(wrapped, period=3)
        self.assertIs(wrapped, it.wrapped_iter)
        self.assertEqual(0, it.count)
        self.assertEqual(3, it.period)

    def test_iter(self):
        it = utils.CooperativeIterator(itertools.count())
        actual = []
        with mock.patch('swift.common.utils.sleep') as mock_sleep:
            for i in it:
                if i >= 100:
                    break
                actual.append(i)
        self.assertEqual(list(range(100)), actual)
        self.assertEqual(20, mock_sleep.call_count)

    def test_close(self):
        it = utils.CooperativeIterator(range(5))
        it.close()

        closeable = mock.MagicMock()
        closeable.close = mock.MagicMock()
        it = utils.CooperativeIterator(closeable)
        it.close()
        self.assertTrue(closeable.close.called)

    def test_sleeps(self):
        def do_test(it, period):
            results = []
            for i in range(period):
                with mock.patch('swift.common.utils.sleep') as mock_sleep:
                    results.append(next(it))
                self.assertFalse(mock_sleep.called, i)

            with mock.patch('swift.common.utils.sleep') as mock_sleep:
                results.append(next(it))
            self.assertTrue(mock_sleep.called)

            for i in range(period - 1):
                with mock.patch('swift.common.utils.sleep') as mock_sleep:
                    results.append(next(it))
                self.assertFalse(mock_sleep.called, i)

            with mock.patch('swift.common.utils.sleep') as mock_sleep:
                results.append(next(it))
            self.assertTrue(mock_sleep.called)

            return results

        actual = do_test(utils.CooperativeIterator(itertools.count()), 5)
        self.assertEqual(list(range(11)), actual)
        actual = do_test(utils.CooperativeIterator(itertools.count(), 5), 5)
        self.assertEqual(list(range(11)), actual)
        actual = do_test(utils.CooperativeIterator(itertools.count(), 3), 3)
        self.assertEqual(list(range(7)), actual)
        actual = do_test(utils.CooperativeIterator(itertools.count(), 1), 1)
        self.assertEqual(list(range(3)), actual)

    def test_no_sleeps(self):
        def do_test(period):
            it = utils.CooperativeIterator(itertools.count(), period)
            results = []
            with mock.patch('swift.common.utils.sleep') as mock_sleep:
                for i in range(100):
                    results.append(next(it))
                    self.assertFalse(mock_sleep.called, i)
            self.assertEqual(list(range(100)), results)

        do_test(0)
        do_test(-1)
        do_test(-111)
        do_test(None)


class TestContextPool(unittest.TestCase):
    def test_context_manager(self):
        size = 5
        pool = utils.ContextPool(size)
        with pool:
            for _ in range(size):
                pool.spawn(eventlet.sleep, 10)
            self.assertEqual(pool.running(), size)
        self.assertEqual(pool.running(), 0)

    def test_close(self):
        size = 10
        pool = utils.ContextPool(size)
        for _ in range(size):
            pool.spawn(eventlet.sleep, 10)
        self.assertEqual(pool.running(), size)
        pool.close()
        self.assertEqual(pool.running(), 0)


class TestLoggerStatsdClientDelegation(unittest.TestCase):
    def setUp(self):
        self.logger_name = 'server'

    def tearDown(self):
        # Avoid test coupling by removing any StatsdClient instance
        # that may have been patched on to a Logger.
        core_logger = logging.getLogger(self.logger_name)
        if hasattr(core_logger, 'statsd_client'):
            del core_logger.statsd_client

    def test_patch_statsd_methods(self):
        client = FakeStatsdClient('host.com', 1234)
        source = argparse.Namespace()
        source.statsd_client = client

        target = argparse.Namespace()
        utils._patch_statsd_methods(target, source)
        target.increment('a')
        target.decrement('b')
        target.update_stats('c', 4)
        target.timing('d', 23.4)
        target.timing_since('e', 23.4)
        target.transfer_rate('f', 56.7, 1234.5)
        exp = {
            'decrement': [(('b',), {})],
            'increment': [(('a',), {})],
            'timing': [(('d', 23.4), {}),
                       (('f', 45929.52612393682, None), {})],
            'timing_since': [(('e', 23.4), {})],
            'transfer_rate': [(('f', 56.7, 1234.5), {})],
            'update_stats': [(('a', 1, None), {}),
                             (('b', -1, None), {}),
                             (('c', 4), {})]
        }
        self.assertEqual(exp, client.calls)

    def test_patch_statsd_methods_source_is_none(self):
        with self.assertRaises(ValueError) as cm:
            utils._patch_statsd_methods(object, None)
        self.assertEqual(
            'statsd_client_source must have a statsd_client attribute',
            str(cm.exception))

    def test_patch_statsd_methods_source_no_statsd_client(self):
        source = argparse.Namespace()
        with self.assertRaises(ValueError) as cm:
            utils._patch_statsd_methods(object, source)
        self.assertEqual(
            'statsd_client_source must have a statsd_client attribute',
            str(cm.exception))

    def test_patch_statsd_methods_source_statsd_client_is_none(self):
        source = argparse.Namespace()
        source.statsd_client = None
        with self.assertRaises(ValueError) as cm:
            utils._patch_statsd_methods(object, source)
        self.assertEqual(
            'statsd_client_source must have a statsd_client attribute',
            str(cm.exception))

    def test_patch_statsd_methods_client_deleted_from_source(self):
        client = FakeStatsdClient('host.com', 1234)
        source = argparse.Namespace()
        source.statsd_client = client
        target = argparse.Namespace()
        utils._patch_statsd_methods(target, source)
        target.increment('a')
        exp = {
            'increment': [(('a',), {})],
            'update_stats': [(('a', 1, None), {})],
        }
        self.assertEqual(exp, client.calls)

        # if the statsd_client is deleted you will blow up...
        del source.statsd_client
        try:
            target.increment('b')
        except AttributeError as err:
            self.assertEqual(
                str(err),
                "'Namespace' object has no attribute 'statsd_client'")

    def test_get_logger_provides_a_swift_log_adapter(self):
        orig_get_swift_logger = utils.logs.get_swift_logger
        calls = []

        def fake_get_swift_logger(*args, **kwargs):
            result = orig_get_swift_logger(*args, **kwargs)
            calls.append((args, kwargs, result))
            return result

        conf = {}
        fmt = 'test %(message)s'
        with mock.patch(
                'swift.common.utils.get_swift_logger', fake_get_swift_logger):
            logger = utils.get_logger(
                conf, name=self.logger_name, log_to_console=True,
                log_route='test', fmt=fmt)
        self.assertEqual(1, len(calls))
        self.assertEqual(
            ((conf, self.logger_name, True, 'test', fmt), {}),
            calls[0][:2])
        self.assertIs(calls[0][2], logger)

    def test_get_logger_provides_statsd_client(self):
        with mock.patch(
                'swift.common.statsd_client.StatsdClient', FakeStatsdClient):
            swift_logger = utils.get_logger(None, name=self.logger_name)
        self.assertTrue(hasattr(swift_logger.logger, 'statsd_client'))
        self.assertIsInstance(swift_logger.logger.statsd_client,
                              FakeStatsdClient)
        swift_logger.increment('a')
        swift_logger.decrement('b')
        swift_logger.update_stats('c', 4)
        swift_logger.timing('d', 23.4)
        swift_logger.timing_since('e', 23.4)
        swift_logger.transfer_rate('f', 56.7, 1234.5)
        exp = {
            'decrement': [(('b',), {})],
            'increment': [(('a',), {})],
            'timing': [(('d', 23.4), {}),
                       (('f', 45929.52612393682, None), {})],
            'timing_since': [(('e', 23.4), {})],
            'transfer_rate': [(('f', 56.7, 1234.5), {})],
            'update_stats': [(('a', 1, None), {}),
                             (('b', -1, None), {}),
                             (('c', 4), {})]
        }
        self.assertTrue(hasattr(swift_logger.logger, 'statsd_client'))
        client = swift_logger.logger.statsd_client
        self.assertEqual(exp, client.calls)

    def test_get_logger_statsd_client_default_conf(self):
        logger = utils.get_logger({}, 'some-name', log_route='some-route')
        # white-box construction validation
        self.assertIsInstance(logger.logger.statsd_client, StatsdClient)
        self.assertIsNone(logger.logger.statsd_client._host)
        self.assertEqual(logger.logger.statsd_client._port, 8125)
        self.assertEqual(logger.logger.statsd_client._prefix, 'some-name.')
        self.assertEqual(logger.logger.statsd_client._default_sample_rate, 1)

    def test_get_logger_statsd_client_non_default_conf(self):
        logger = utils.get_logger({'log_statsd_host': 'some.host.com',
                                   'log_statsd_port': 1234,
                                   'log_statsd_default_sample_rate': 1.2,
                                   'log_statsd_sample_rate_factor': 3.4},
                                  'some-name', log_route='some-route')
        # white-box construction validation
        self.assertIsInstance(logger.logger.statsd_client, StatsdClient)
        self.assertEqual(logger.logger.statsd_client._host, 'some.host.com')
        self.assertEqual(logger.logger.statsd_client._port, 1234)
        self.assertEqual(logger.logger.statsd_client._default_sample_rate, 1.2)
        self.assertEqual(logger.logger.statsd_client._sample_rate_factor, 3.4)
        self.assertEqual(logger.logger.statsd_client._prefix, 'some-name.')

    def test_get_logger_statsd_client_prefix(self):
        def call_get_logger(conf, name, statsd_tail_prefix, log_route=None):
            swift_logger = utils.get_logger(
                conf, name=name,
                log_route=log_route,
                statsd_tail_prefix=statsd_tail_prefix)
            self.assertTrue(hasattr(swift_logger.logger, 'statsd_client'))
            self.assertIsInstance(swift_logger.logger.statsd_client,
                                  StatsdClient)
            return swift_logger

        # tail prefix defaults to swift
        logger = call_get_logger(None, None, None)
        self.assertEqual('swift.', logger.logger.statsd_client._prefix)
        self.assertEqual('swift', logger.name)
        self.assertEqual('swift', logger.server)

        # tail prefix defaults to swift, log_route is ignored for stats
        logger = call_get_logger(None, None, None, log_route='route')
        self.assertEqual('swift.', logger.logger.statsd_client._prefix)
        self.assertEqual('route', logger.name)
        self.assertEqual('swift', logger.server)

        # tail prefix defaults to conf log_name
        conf = {'log_name': 'bar'}
        logger = call_get_logger(conf, None, None)
        self.assertEqual('bar.', logger.logger.statsd_client._prefix)
        self.assertEqual('bar', logger.name)
        self.assertEqual('bar', logger.server)

        # tail prefix defaults to conf log_name, log_route is ignored for stats
        conf = {'log_name': 'bar'}
        logger = call_get_logger(conf, None, None, log_route='route')
        self.assertEqual('bar.', logger.logger.statsd_client._prefix)
        self.assertEqual('route', logger.name)
        self.assertEqual('bar', logger.server)

        # tail prefix defaults to name arg which overrides conf log_name
        logger = call_get_logger(conf, '', None)
        self.assertEqual('', logger.logger.statsd_client._prefix)

        # tail prefix defaults to name arg which overrides conf log_name
        logger = call_get_logger(conf, 'baz', None)
        self.assertEqual('baz.', logger.logger.statsd_client._prefix)

        # tail prefix set to statsd_tail_prefix arg which overrides name arg
        logger = call_get_logger(conf, 'baz', '')
        self.assertEqual('', logger.logger.statsd_client._prefix)

        # tail prefix set to statsd_tail_prefix arg which overrides name arg
        logger = call_get_logger(conf, 'baz', 'boo')
        self.assertEqual('boo.', logger.logger.statsd_client._prefix)

        # base prefix is configured, tail prefix defaults to swift
        conf = {'log_statsd_metric_prefix': 'foo'}
        logger = call_get_logger(conf, None, None)
        self.assertEqual('foo.swift.', logger.logger.statsd_client._prefix)

        # base prefix is configured, tail prefix defaults to conf log_name
        conf = {'log_statsd_metric_prefix': 'foo', 'log_name': 'bar'}
        logger = call_get_logger(conf, None, None)
        self.assertEqual('foo.bar.', logger.logger.statsd_client._prefix)

        # base prefix is configured, tail prefix defaults to name arg
        logger = call_get_logger(conf, 'baz', None)
        self.assertEqual('foo.baz.', logger.logger.statsd_client._prefix)

        # base prefix is configured, tail prefix set to statsd_tail_prefix arg
        logger = call_get_logger(conf, None, '')
        self.assertEqual('foo.', logger.logger.statsd_client._prefix)

        # base prefix is configured, tail prefix set to statsd_tail_prefix arg
        logger = call_get_logger(conf, 'baz', 'boo')
        self.assertEqual('foo.boo.', logger.logger.statsd_client._prefix)

    def test_get_logger_replaces_statsd_client(self):
        # Each call to get_logger creates a *new* StatsdClient instance and
        # sets it as an attribute of the potentially *shared* Logger instance.
        # This is a questionable pattern but the test at least reminds us.
        orig_logger = utils.get_logger(
            {'log_statsd_port': 1234},
            name=self.logger_name,
            statsd_tail_prefix='orig')
        self.assertTrue(hasattr(orig_logger.logger, 'statsd_client'))
        orig_client = orig_logger.logger.statsd_client
        self.assertEqual('orig.', orig_client._prefix)
        self.assertEqual(1234, orig_client._port)

        new_adapted_logger = utils.get_logger(
            {'log_statsd_port': 5678},
            name=self.logger_name,
            statsd_tail_prefix='new')
        self.assertTrue(hasattr(new_adapted_logger.logger, 'statsd_client'))
        new_client = new_adapted_logger.logger.statsd_client
        # same core Logger...
        self.assertIs(orig_logger.logger, new_adapted_logger.logger)
        # ... different StatsdClient !
        self.assertIsNot(new_client, orig_client)
        self.assertIs(new_client, orig_logger.logger.statsd_client)
        self.assertEqual('new.', new_client._prefix)
        self.assertEqual(5678, new_client._port)

    def test_get_prefixed_logger_calls_get_prefixed_swift_logger(self):
        orig_get_prefixed_swift_logger = utils.logs.get_prefixed_swift_logger
        base_logger = utils.logs.get_swift_logger(None)
        calls = []

        def fake_get_prefixed_swift_logger(*args, **kwargs):
            result = orig_get_prefixed_swift_logger(*args, **kwargs)
            calls.append((args, kwargs, result))
            return result

        with mock.patch(
                'swift.common.utils.get_prefixed_swift_logger',
                fake_get_prefixed_swift_logger):
            logger = utils.get_prefixed_logger(base_logger, 'boo')
        self.assertEqual(1, len(calls))
        self.assertEqual(((base_logger,), {'prefix': 'boo'}), calls[0][:2])
        self.assertEqual(calls[0][2], logger)
        self.assertEqual('boo', logger.prefix)

    def test_get_prefixed_logger_adopts_statsd_client(self):
        # verify that get_prefixed_logger installs an interface to any existing
        # StatsdClient that the source logger has
        with mock.patch(
                'swift.common.statsd_client.StatsdClient', FakeStatsdClient):
            adapted_logger = utils.get_logger(None, name=self.logger_name)
        self.assertTrue(hasattr(adapted_logger.logger, 'statsd_client'))
        self.assertIsInstance(adapted_logger.logger.statsd_client,
                              FakeStatsdClient)

        prefixed_logger = utils.get_prefixed_logger(adapted_logger, 'test')
        self.assertTrue(hasattr(prefixed_logger.logger, 'statsd_client'))
        self.assertIs(prefixed_logger.logger.statsd_client,
                      adapted_logger.logger.statsd_client)

        prefixed_logger.increment('foo')
        prefixed_logger.decrement('boo')

        exp = {
            'increment': [(('foo',), {})],
            'decrement': [(('boo',), {})],
            'update_stats': [(('foo', 1, None), {}),
                             (('boo', -1, None), {})]
        }
        self.assertEqual(exp, prefixed_logger.logger.statsd_client.calls)
        self.assertEqual(exp, adapted_logger.logger.statsd_client.calls)

    def test_get_prefixed_logger_with_mutilated_statsd_client(self):
        with mock.patch(
                'swift.common.statsd_client.StatsdClient', FakeStatsdClient):
            adapted_logger = utils.get_logger(None, name=self.logger_name)
        self.assertTrue(hasattr(adapted_logger, 'statsd_client_source'))
        self.assertIsInstance(
            adapted_logger.statsd_client_source.statsd_client,
            FakeStatsdClient)
        # sanity
        adapted_logger.increment('foo')
        fake_statsd_client = adapted_logger.statsd_client_source.statsd_client
        self.assertEqual(fake_statsd_client.get_increments(), ['foo'])

        # kill and maim!
        adapted_logger.statsd_client_source.statsd_client = None
        # bro, what are you DOING!?  you're crazy!?
        self.assertEqual(adapted_logger.logger.statsd_client, None)
        # you can't do that, it *breaks* the statsd_client patch methods
        with self.assertRaises(AttributeError):
            adapted_logger.increment('bar')

        # you can't get a prefixed logger from a *broken* adapter
        with self.assertRaises(ValueError):
            utils.get_prefixed_logger(adapted_logger, 'test')

    def test_get_prefixed_logger_no_statsd_client(self):
        # verify get_prefixed_logger can be used to mutate the prefix of a
        # SwiftLogAdapter that does *not* have a StatsdClient interface
        adapted_logger = utils.logs.get_swift_logger(
            None, name=self.logger_name)
        self.assertFalse(
            hasattr(adapted_logger.logger, 'statsd_client'))
        self.assertFalse(hasattr(adapted_logger, 'statsd_client_source'))
        self.assertFalse(hasattr(adapted_logger, 'increment'))

        prefixed_logger = utils.get_prefixed_logger(adapted_logger, 'test')
        self.assertFalse(hasattr(prefixed_logger, 'statsd_client_source'))
        self.assertFalse(hasattr(prefixed_logger.logger, 'statsd_client'))
        self.assertFalse(hasattr(prefixed_logger, 'increment'))

    def test_statsd_set_prefix_deprecation(self):
        conf = {'log_statsd_host': 'another.host.com'}

        with warnings.catch_warnings(record=True) as cm:
            warnings.resetwarnings()
            warnings.simplefilter('always', DeprecationWarning)
            logger = utils.get_logger(
                conf, 'some-name', log_route='some-route')
            logger.set_statsd_prefix('some-name.more-specific')
        msgs = [str(warning.message)
                for warning in cm
                if str(warning.message).startswith('set_statsd_prefix')]
        self.assertEqual(
            ['set_statsd_prefix() is deprecated; use the '
             '``statsd_tail_prefix`` argument to ``get_logger`` instead.'],
            msgs)


class TestTimingStatsDecorators(unittest.TestCase):
    def test_timing_stats(self):
        class MockController(object):
            def __init__(mock_self, status):
                mock_self.status = status
                mock_self.logger = debug_logger()

            @utils.timing_stats()
            def METHOD(mock_self):
                return Response(status=mock_self.status)

        now = time.time()
        mock_controller = MockController(200)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(400)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(404)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(412)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(416)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual({'timing_since': [(('METHOD.timing', now), {})]},
                         mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(500)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual(
            {'timing_since': [(('METHOD.errors.timing', now), {})]},
            mock_controller.logger.statsd_client.calls)

        mock_controller = MockController(507)
        with mock.patch('swift.common.utils.time.time', return_value=now):
            mock_controller.METHOD()
        self.assertEqual(
            {'timing_since': [(('METHOD.errors.timing', now), {})]},
            mock_controller.logger.statsd_client.calls)

    def test_memcached_timing_stats(self):
        class MockMemcached(object):
            def __init__(mock_self):
                mock_self.logger = debug_logger()

            @utils.memcached_timing_stats()
            def set(mock_self):
                pass

            @utils.memcached_timing_stats()
            def get(mock_self):
                pass

        mock_cache = MockMemcached()
        with patch('time.time', return_value=1000.99):
            mock_cache.set()
        self.assertEqual(
            {'timing_since': [(('memcached.set.timing', 1000.99), {})]},
            mock_cache.logger.statsd_client.calls)

        mock_cache = MockMemcached()
        with patch('time.time', return_value=2000.99):
            mock_cache.get()
        self.assertEqual(
            {'timing_since': [(('memcached.get.timing', 2000.99), {})]},
            mock_cache.logger.statsd_client.calls)
