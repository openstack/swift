# Copyright (c) 2010-2011 OpenStack, LLC.
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
import logging
import mimetools
import os
import socket
import sys
import time
import unittest
from getpass import getuser
from shutil import rmtree
from StringIO import StringIO
from functools import partial
from tempfile import NamedTemporaryFile

from eventlet import sleep

from swift.common import utils


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

    chdir = setsid = setgid = setuid = umask = pass_func

    def called_func(self, name, *args, **kwargs):
        self.called_funcs[name] = True

    def raise_func(self, name, *args, **kwargs):
        self.called_funcs[name] = True
        raise OSError()

    def dup2(self, source, target):
        self.closed_fds.append(target)

    def __getattr__(self, name):
        # I only over-ride portions of the os module
        try:
            return object.__getattr__(self, name)
        except AttributeError:
            return getattr(os, name)


class MockSys():

    __stderr__ = sys.__stderr__


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
        err_msg = """Usage: test usage

Error: missing config file argument
"""
        test_args = []
        self.assertRaises(SystemExit, utils.parse_options, 'test usage', True,
                          test_args)
        self.assertEquals(stdo.getvalue(), err_msg)

        # verify conf file must exist, context manager will delete temp file
        with NamedTemporaryFile() as f:
            conf_file = f.name
        err_msg += """Usage: test usage

Error: unable to locate %s
""" % conf_file
        test_args = [conf_file]
        self.assertRaises(SystemExit, utils.parse_options, 'test usage', True,
                          test_args)
        self.assertEquals(stdo.getvalue(), err_msg)

        # reset stdio
        utils.sys.stdout = orig_stdout
        utils.sys.stderr = orig_stderr

    def test_get_logger(self):
        sio = StringIO()
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sio))
        logger = utils.get_logger(None, 'server')
        logger.warn('test1')
        self.assertEquals(sio.getvalue(), 'test1\n')
        logger.debug('test2')
        self.assertEquals(sio.getvalue(), 'test1\n')
        logger = utils.get_logger({'log_level': 'DEBUG'}, 'server')
        logger.debug('test3')
        self.assertEquals(sio.getvalue(), 'test1\ntest3\n')
        # Doesn't really test that the log facility is truly being used all the
        # way to syslog; but exercises the code.
        logger = utils.get_logger({'log_facility': 'LOG_LOCAL3'}, 'server')
        logger.warn('test4')
        self.assertEquals(sio.getvalue(),
                          'test1\ntest3\ntest4\n')
        logger.debug('test5')
        self.assertEquals(sio.getvalue(),
                          'test1\ntest3\ntest4\n')

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
                  '\x06\xfb\xf0\xb5\x14\xe5\x19\x9d\xfcN\x00\xf4.\xb5\xea\x83')
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
        f = open('/tmp/test', 'wb')
        f.write(conf)
        f.close()
        result = utils.readconf('/tmp/test')
        expected = {'log_name': None,
                    'section1': {'foo': 'bar'},
                    'section2': {'log_name': 'yarr'}}
        self.assertEquals(result, expected)
        result = utils.readconf('/tmp/test', 'section1')
        expected = {'log_name': 'section1', 'foo': 'bar'}
        self.assertEquals(result, expected)
        result = utils.readconf('/tmp/test', 'section2').get('log_name')
        expected = 'yarr'
        self.assertEquals(result, expected)
        result = utils.readconf('/tmp/test', 'section1',
                                log_name='foo').get('log_name')
        expected = 'foo'
        self.assertEquals(result, expected)
        result = utils.readconf('/tmp/test', 'section1',
                                defaults={'bar': 'baz'})
        expected = {'log_name': 'section1', 'foo': 'bar', 'bar': 'baz'}
        self.assertEquals(result, expected)
        os.unlink('/tmp/test')

    def test_drop_privileges(self):
        user = getuser()
        # over-ride os with mock
        required_func_calls = ('setgid', 'setuid', 'setsid', 'chdir', 'umask')
        utils.os = MockOs(called_funcs=required_func_calls)
        # exercise the code
        utils.drop_privileges(user)
        for func in required_func_calls:
            self.assert_(utils.os.called_funcs[func])

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
        utils.sys = MockSys()
        utils.os = MockOs()

        # basic test
        utils.capture_stdio(logger)
        self.assert_(utils.sys.excepthook is not None)
        self.assertEquals(utils.os.closed_fds, [0, 1, 2])
        self.assert_(utils.sys.stdout is not None)
        self.assert_(utils.sys.stderr is not None)

        # reset; test same args, but exc when trying to close stdio
        utils.os = MockOs(raise_funcs=('dup2',))
        utils.sys = MockSys()

        # test unable to close stdio
        utils.capture_stdio(logger)
        self.assert_(utils.sys.excepthook is not None)
        self.assertEquals(utils.os.closed_fds, [])
        self.assert_(utils.sys.stdout is not None)
        self.assert_(utils.sys.stderr is not None)

        # reset; test some other args
        logger = utils.get_logger(None, log_to_console=True)
        utils.os = MockOs()
        utils.sys = MockSys()

        # test console log
        utils.capture_stdio(logger, capture_stdout=False,
                            capture_stderr=False)
        self.assert_(utils.sys.excepthook is not None)
        # when logging to console, stderr remains open
        self.assertEquals(utils.os.closed_fds, [0, 1])
        logger.logger.removeHandler(utils.get_logger.console)
        # stdio not captured
        self.assertFalse(hasattr(utils.sys, 'stdout'))
        self.assertFalse(hasattr(utils.sys, 'stderr'))

    def test_get_logger_console(self):
        reload(utils)  # reset get_logger attrs
        logger = utils.get_logger(None)
        self.assertFalse(hasattr(utils.get_logger, 'console'))
        logger = utils.get_logger(None, log_to_console=True)
        self.assert_(hasattr(utils.get_logger, 'console'))
        self.assert_(isinstance(utils.get_logger.console,
                                logging.StreamHandler))
        # make sure you can't have two console handlers
        old_handler = utils.get_logger.console
        logger = utils.get_logger(None, log_to_console=True)
        self.assertNotEquals(utils.get_logger.console, old_handler)
        logger.logger.removeHandler(utils.get_logger.console)

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
        # make sure its accurate to 10th of a second
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

    def test_ratelimit_sleep_with_sleep(self):
        running_time = 0
        start = time.time()
        sleeps = [0] * 7 + [.2] * 3 + [0] * 30
        for i in sleeps:
            running_time = utils.ratelimit_sleep(running_time, 40,
                                                 rate_buffer=1)
            time.sleep(i)
        # make sure its accurate to 10th of a second
        self.assertTrue(abs(100 - (time.time() - start) * 100) < 10)


if __name__ == '__main__':
    unittest.main()
