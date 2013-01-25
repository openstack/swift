# Copyright (c) 2010-2012 OpenStack, LLC.
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

""" Tests for swift.common.utils """

from __future__ import with_statement
from test.unit import temptree
import ctypes
import errno
import logging
import mimetools
import os
import random
import re
import socket
import sys
import time
import unittest
from threading import Thread
from Queue import Queue, Empty
from getpass import getuser
from shutil import rmtree
from StringIO import StringIO
from functools import partial
from tempfile import TemporaryFile, NamedTemporaryFile

from eventlet import sleep

from swift.common.exceptions import (Timeout, MessageTimeout,
                                     ConnectionTimeout)
from swift.common import utils
from swift.common.swob import Response


class MockOs():

    def __init__(self, pass_funcs=[], called_funcs=[], raise_funcs=[]):
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


class MockUdpSocket():
    def __init__(self):
        self.sent = []

    def sendto(self, data, target):
        self.sent.append((data, target))

    def close(self):
        pass


class MockSys():

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
            logger.thread_locals = (None, None)
            logger.removeHandler(handler)
        delattr(utils.get_logger, 'handler4logger')
    if hasattr(utils.get_logger, 'console_handler4logger'):
        for logger, h in utils.get_logger.console_handler4logger.items():
            logger.thread_locals = (None, None)
            logger.removeHandler(h)
        delattr(utils.get_logger, 'console_handler4logger')


class TestUtils(unittest.TestCase):
    """ Tests for swift.common.utils """

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'

    def test_normalize_timestamp(self):
        """ Test swift.common.utils.normalize_timestamp """
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

    def test_mkdirs(self):
        testroot = os.path.join(os.path.dirname(__file__), 'mkdirs')
        try:
            os.unlink(testroot)
        except Exception:
            pass
        rmtree(testroot, ignore_errors=1)
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

    def test_split_path(self):
        """ Test swift.common.utils.split_account_path """
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
        except ValueError, err:
            self.assertEquals(str(err), 'Invalid path: o%0An%20e')
        try:
            utils.split_path('o\nn e', 2, 3, True)
        except ValueError, err:
            self.assertEquals(str(err), 'Invalid path: o%0An%20e')

    def test_validate_device_partition(self):
        """ Test swift.common.utils.validate_device_partition """
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
        except ValueError, err:
            self.assertEquals(str(err), 'Invalid device: o%0An%20e')
        try:
            utils.validate_device_partition('foo', 'o\nn e')
        except ValueError, err:
            self.assertEquals(str(err), 'Invalid partition: o%0An%20e')

    def test_NullLogger(self):
        """ Test swift.common.utils.NullLogger """
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
        lfo = utils.LoggerFileObject(logger)
        print 'test1'
        self.assertEquals(sio.getvalue(), '')
        sys.stdout = lfo
        print 'test2'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\n')
        sys.stderr = lfo
        print >> sys.stderr, 'test4'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n')
        sys.stdout = orig_stdout
        print 'test5'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n')
        print >> sys.stderr, 'test6'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
                          'STDOUT: test6\n')
        sys.stderr = orig_stderr
        print 'test8'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
                          'STDOUT: test6\n')
        lfo.writelines(['a', 'b', 'c'])
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
                          'STDOUT: test6\nSTDOUT: a#012b#012c\n')
        lfo.close()
        lfo.write('d')
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
                          'STDOUT: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
        lfo.flush()
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
                          'STDOUT: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
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
        # use mkstemp to get a file that is definately on disk
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
        self.assert_('missing config file' in stdo.getvalue())

        # verify conf file must exist, context manager will delete temp file
        with NamedTemporaryFile() as f:
            conf_file = f.name
        self.assertRaises(SystemExit, utils.parse_options, once=True,
                          test_args=[conf_file])
        self.assert_('unable to locate' in stdo.getvalue())

        # reset stdio
        utils.sys.stdout = orig_stdout
        utils.sys.stderr = orig_stderr

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
            log_exception(ConnectionTimeout(42, 'my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' not in log_msg)
            self.assert_('ConnectionTimeout' in log_msg)
            self.assert_('(42s)' in log_msg)
            self.assert_('my error message' not in log_msg)
            log_exception(MessageTimeout(42, 'my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' not in log_msg)
            self.assert_('MessageTimeout' in log_msg)
            self.assert_('(42s)' in log_msg)
            self.assert_('my error message' in log_msg)

            # test unhandled
            log_exception(Exception('my error message'))
            log_msg = strip_value(sio)
            self.assert_('Traceback' in log_msg)
            self.assert_('my error message' in log_msg)

        finally:
            logger.logger.removeHandler(handler)
            reset_loggers()

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
            reset_loggers()

    def test_storage_directory(self):
        self.assertEquals(utils.storage_directory('objects', '1', 'ABCDEF'),
                          'objects/1/DEF/ABCDEF')

    def test_whataremyips(self):
        myips = utils.whataremyips()
        self.assert_(len(myips) > 1)
        self.assert_('127.0.0.1' in myips)

    def test_hash_path(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results hash_path produces, they know it
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

    def test_load_libc_function(self):
        self.assert_(callable(
            utils.load_libc_function('printf')))
        self.assert_(callable(
            utils.load_libc_function('some_not_real_function')))

    def test_readconf(self):
        conf = '''[section1]
foo = bar

[section2]
log_name = yarr'''
        # setup a real file
        with open('/tmp/test', 'wb') as f:
            f.write(conf)
        make_filename = lambda: '/tmp/test'
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
        self.assertRaises(SystemExit, utils.readconf, '/tmp/test', 'section3')
        os.unlink('/tmp/test')
        self.assertRaises(SystemExit, utils.readconf, '/tmp/test')

    def test_readconf_raw(self):
        conf = '''[section1]
foo = bar

[section2]
log_name = %(yarr)s'''
        # setup a real file
        with open('/tmp/test', 'wb') as f:
            f.write(conf)
        make_filename = lambda: '/tmp/test'
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
        os.unlink('/tmp/test')
        self.assertRaises(SystemExit, utils.readconf, '/tmp/test')

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

        # reset; test same args, OSError trying to get session leader
        utils.os = MockOs(called_funcs=required_func_calls,
                          raise_funcs=('setsid',))
        for func in required_func_calls:
            self.assertFalse(utils.os.called_funcs.get(func, False))
        utils.drop_privileges(user)
        for func in required_func_calls:
            self.assert_(utils.os.called_funcs[func])

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
            reset_loggers()
        finally:
            utils.sys = _orig_sys
            utils.os = _orig_os

    def test_get_logger_console(self):
        reset_loggers()
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
        reset_loggers()

    def test_ratelimit_sleep(self):
        running_time = 0
        start = time.time()
        for i in range(100):
            running_time = utils.ratelimit_sleep(running_time, 0)
        self.assertTrue(abs((time.time() - start) * 100) < 1)

        running_time = 0
        start = time.time()
        for i in range(50):
            running_time = utils.ratelimit_sleep(running_time, 200)
        # make sure it's accurate to 10th of a second
        self.assertTrue(abs(25 - (time.time() - start) * 100) < 10)

    def test_ratelimit_sleep_with_incr(self):
        running_time = 0
        start = time.time()
        vals = [5, 17, 0, 3, 11, 30,
                40, 4, 13, 2, -1] * 2  # adds up to 250 (with no -1)
        total = 0
        for i in vals:
            running_time = utils.ratelimit_sleep(running_time,
                                                 500, incr_by=i)
            total += i
        self.assertTrue(abs(50 - (time.time() - start) * 100) < 10)

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

    def test_ratelimit_sleep_with_sleep(self):
        running_time = 0
        start = time.time()
        sleeps = [0] * 7 + [.2] * 3 + [0] * 30
        for i in sleeps:
            running_time = utils.ratelimit_sleep(running_time, 40,
                                                 rate_buffer=1)
            time.sleep(i)
        # make sure it's accurate to 10th of a second
        self.assertTrue(abs(100 - (time.time() - start) * 100) < 10)

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
        for goodurl in ('http://1.1.1.1/v1/a/c/o',
                        'http://1.1.1.1:8080/a/c/o',
                        'http://2.2.2.2/a/c/o',
                        'https://1.1.1.1/v1/a/c/o',
                        ''):
            self.assertEquals(utils.validate_sync_to(goodurl,
                                                     ['1.1.1.1', '2.2.2.2']),
                              None)
        for badurl in ('http://1.1.1.1',
                       'httpq://1.1.1.1/v1/a/c/o',
                       'http://1.1.1.1/v1/a/c/o?query',
                       'http://1.1.1.1/v1/a/c/o#frag',
                       'http://1.1.1.1/v1/a/c/o?query#frag',
                       'http://1.1.1.1/v1/a/c/o?query=param',
                       'http://1.1.1.1/v1/a/c/o?query=param#frag',
                       'http://1.1.1.2/v1/a/c/o'):
            self.assertNotEquals(
                utils.validate_sync_to(badurl, ['1.1.1.1', '2.2.2.2']),
                None)

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

    def test_streq_const_time(self):
        self.assertTrue(utils.streq_const_time('abc123', 'abc123'))
        self.assertFalse(utils.streq_const_time('a', 'aaaaa'))
        self.assertFalse(utils.streq_const_time('ABC123', 'abc123'))

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
            except OSError, err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 1024')
            # Want 1024 reserved, have 512 * 2 free, so fails
            utils.FALLOCATE_RESERVE = 1024
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError, err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 1024')
            # Want 2048 reserved, have 1024 * 1 free, so fails
            utils.FALLOCATE_RESERVE = 2048
            StatVFS.f_frsize = 1024
            StatVFS.f_bavail = 1
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError, err:
                exc = err
            self.assertEquals(str(exc), 'FALLOCATE_RESERVE fail 1024 <= 2048')
            # Want 2048 reserved, have 512 * 2 free, so fails
            utils.FALLOCATE_RESERVE = 2048
            StatVFS.f_frsize = 512
            StatVFS.f_bavail = 2
            exc = None
            try:
                fallocate(0, 1, 0, ctypes.c_uint64(0))
            except OSError, err:
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
            except OSError, err:
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
            except OSError, err:
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
            'log_statsd_port': 9876,
            'log_statsd_default_sample_rate': 0.75,
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

        @utils.timing_stats
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

        mock_controller = MockController(401)
        METHOD(mock_controller)
        self.assertEquals(len(mock_controller.args), 2)
        self.assertEquals(mock_controller.called, 'timing')
        self.assertEquals(mock_controller.args[0], 'METHOD.errors.timing')
        self.assert_(mock_controller.args[1] > 0)


class TestStatsdLoggingDelegation(unittest.TestCase):
    def setUp(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('localhost', 0))
        self.port = self.sock.getsockname()[1]
        self.queue = Queue()
        self.reader_thread = Thread(target=self.statsd_reader)
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
            except Exception, e:
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

    def test_thread_locals(self):
        logger = utils.get_logger(None)
        orig_thread_locals = logger.thread_locals
        try:
            self.assertEquals(logger.thread_locals, (None, None))
            logger.txn_id = '1234'
            logger.client_ip = '1.2.3.4'
            self.assertEquals(logger.thread_locals, ('1234', '1.2.3.4'))
            logger.txn_id = '5678'
            logger.client_ip = '5.6.7.8'
            self.assertEquals(logger.thread_locals, ('5678', '5.6.7.8'))
        finally:
            logger.thread_locals = orig_thread_locals


if __name__ == '__main__':
    unittest.main()
