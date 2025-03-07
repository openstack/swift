# Copyright (c) 2010-2024 OpenStack Foundation
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

"""Tests for swift.common.utils.logs"""

import contextlib
import errno
import logging
import os
import socket
import sys
import time
import unittest
import eventlet
import functools
from unittest import mock

from io import StringIO
import http.client

from test.unit import with_tempdir
from test.unit import quiet_eventlet_exceptions
from test.unit.common.test_utils import MockOs, MockSys
from swift.common.exceptions import Timeout, MessageTimeout, ConnectionTimeout

import eventlet.green.http.client as green_http_client

from swift.common import utils

from swift.common.swob import Request, Response
from swift.common.utils.logs import SwiftLogFormatter, SwiftLogAdapter, \
    get_swift_logger, get_prefixed_swift_logger


def reset_loggers():
    if hasattr(get_swift_logger, 'handler4logger'):
        for logger, handler in get_swift_logger.handler4logger.items():
            logger.removeHandler(handler)
        delattr(get_swift_logger, 'handler4logger')
    if hasattr(get_swift_logger, 'console_handler4logger'):
        for logger, h in \
                get_swift_logger.console_handler4logger.items():
            logger.removeHandler(h)
        delattr(get_swift_logger, 'console_handler4logger')
    # Reset the LogAdapter class thread local state. Use get_swift_logger()
    # here to fetch a LogAdapter instance because the items from
    # get_swift_logger.handler4logger above are the underlying logger
    # instances not the LogAdapter.
    get_swift_logger(None).thread_locals = (None, None)


def reset_logger_state(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        reset_loggers()
        try:
            return f(self, *args, **kwargs)
        finally:
            reset_loggers()
    return wrapper


class TestUtilsLogs(unittest.TestCase):

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
        logger.setLevel(logging.DEBUG)
        handler = CrashyLogger()
        logger.addHandler(handler)

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
                self.assertGreater(crashy_calls[0], 1)

        logger.removeHandler(handler)

    def test_get_swift_logger(self):
        sio = StringIO()
        logger = logging.getLogger('server')
        logger.addHandler(logging.StreamHandler(sio))
        logger = get_swift_logger(None, 'server', log_route='server')
        logger.warning('test1')
        self.assertEqual(sio.getvalue(), 'test1\n')
        logger.debug('test2')
        self.assertEqual(sio.getvalue(), 'test1\n')
        logger = get_swift_logger({'log_level': 'DEBUG'}, 'server',
                                  log_route='server')
        logger.debug('test3')
        self.assertEqual(sio.getvalue(), 'test1\ntest3\n')
        # Doesn't really test that the log facility is truly being used all the
        # way to syslog; but exercises the code.
        logger = get_swift_logger({'log_facility': 'LOG_LOCAL3'}, 'server',
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

    def test_get_swift_logger_name_and_route(self):
        @contextlib.contextmanager
        def add_log_handler(logger):
            # install a handler to capture log messages formatted as per swift
            sio = StringIO()
            handler = logging.StreamHandler(sio)
            handler.setFormatter(SwiftLogFormatter(
                fmt="%(server)s: %(message)s", max_line_length=20)
            )
            logger.logger.addHandler(handler)
            yield sio
            logger.logger.removeHandler(handler)

        logger = utils.get_swift_logger({}, name='name', log_route='route')
        # log_route becomes the LogAdapter.name and logging.Logger.name
        self.assertEqual('route', logger.name)
        self.assertEqual('route', logger.logger.name)
        # name becomes the LogAdapter.server!
        self.assertEqual('name', logger.server)
        # LogAdapter.server is used when formatting a log message
        with add_log_handler(logger) as sio:
            logger.info('testing')
            self.assertEqual('name: testing\n', sio.getvalue())

        logger = utils.get_swift_logger({'log_name': 'conf-name'},
                                        name='name', log_route='route')
        self.assertEqual('route', logger.name)
        self.assertEqual('name', logger.server)
        with add_log_handler(logger) as sio:
            logger.info('testing')
            self.assertEqual('name: testing\n', sio.getvalue())

        logger = utils.get_swift_logger({'log_name': 'conf-name'},
                                        log_route='route')
        self.assertEqual('route', logger.name)
        self.assertEqual('conf-name', logger.server)
        with add_log_handler(logger) as sio:
            logger.info('testing')
            self.assertEqual('conf-name: testing\n', sio.getvalue())

        logger = utils.get_swift_logger({'log_name': 'conf-name'})
        self.assertEqual('conf-name', logger.name)
        self.assertEqual('conf-name', logger.server)
        with add_log_handler(logger) as sio:
            logger.info('testing')
            self.assertEqual('conf-name: testing\n', sio.getvalue())

        logger = utils.get_swift_logger({})
        self.assertEqual('swift', logger.name)
        self.assertEqual('swift', logger.server)
        with add_log_handler(logger) as sio:
            logger.info('testing')
            self.assertEqual('swift: testing\n', sio.getvalue())

        logger = utils.get_swift_logger({}, log_route='route')
        self.assertEqual('route', logger.name)
        self.assertEqual('swift', logger.server)
        with add_log_handler(logger) as sio:
            logger.info('testing')
            self.assertEqual('swift: testing\n', sio.getvalue())

        # same log_route, different names...
        logger1 = utils.get_swift_logger({}, name='name1', log_route='route')
        logger2 = utils.get_swift_logger({}, name='name2', log_route='route')
        self.assertEqual('route', logger1.name)
        self.assertEqual('route', logger1.logger.name)
        self.assertEqual('name1', logger1.server)
        self.assertEqual('route', logger2.name)
        self.assertEqual('route', logger2.logger.name)
        self.assertEqual('name2', logger2.server)
        self.assertIs(logger2.logger, logger1.logger)
        with add_log_handler(logger1) as sio:
            logger1.info('testing')
            self.assertEqual('name1: testing\n', sio.getvalue())
        with add_log_handler(logger2) as sio:
            logger2.info('testing')
            self.assertEqual('name2: testing\n', sio.getvalue())

        # different log_route, different names...
        logger1 = utils.get_swift_logger({}, name='name1', log_route='route1')
        logger2 = utils.get_swift_logger({}, name='name2', log_route='route2')
        self.assertEqual('route1', logger1.name)
        self.assertEqual('route1', logger1.logger.name)
        self.assertEqual('name1', logger1.server)
        self.assertEqual('route2', logger2.name)
        self.assertEqual('route2', logger2.logger.name)
        self.assertEqual('name2', logger2.server)
        self.assertIsNot(logger2.logger, logger1.logger)
        with add_log_handler(logger1) as sio:
            logger1.info('testing')
            self.assertEqual('name1: testing\n', sio.getvalue())
        with add_log_handler(logger2) as sio:
            logger2.info('testing')
            self.assertEqual('name2: testing\n', sio.getvalue())

    @with_tempdir
    def test_get_swift_logger_sysloghandler_plumbing(self, tempdir):
        orig_sysloghandler = utils.logs.ThreadSafeSysLogHandler
        syslog_handler_args = []

        def syslog_handler_catcher(*args, **kwargs):
            syslog_handler_args.append((args, kwargs))
            return orig_sysloghandler(*args, **kwargs)

        syslog_handler_catcher.LOG_LOCAL0 = orig_sysloghandler.LOG_LOCAL0
        syslog_handler_catcher.LOG_LOCAL3 = orig_sysloghandler.LOG_LOCAL3

        # Some versions of python perform host resolution while initializing
        # the handler. See https://bugs.python.org/issue30378
        orig_getaddrinfo = socket.getaddrinfo

        def fake_getaddrinfo(host, *args):
            return orig_getaddrinfo('localhost', *args)

        with mock.patch.object(utils.logs, 'ThreadSafeSysLogHandler',
                               syslog_handler_catcher), \
                mock.patch.object(socket, 'getaddrinfo', fake_getaddrinfo):
            # default log_address
            get_swift_logger({
                'log_facility': 'LOG_LOCAL3',
            }, 'server', log_route='server')
            expected_args = [((), {'address': '/dev/log',
                                   'facility': orig_sysloghandler.LOG_LOCAL3})]
            if not os.path.exists('/dev/log') or \
                    os.path.isfile('/dev/log') or \
                    os.path.isdir('/dev/log'):
                # Since socket on OSX is in /var/run/syslog, there will be
                # a fallback to UDP.
                expected_args = [
                    ((), {'facility': orig_sysloghandler.LOG_LOCAL3})]
            self.assertEqual(expected_args, syslog_handler_args)

            # custom log_address - file doesn't exist: fallback to UDP
            log_address = os.path.join(tempdir, 'foo')
            syslog_handler_args = []
            get_swift_logger({
                'log_facility': 'LOG_LOCAL3',
                'log_address': log_address,
            }, 'server', log_route='server')
            expected_args = [
                ((), {'facility': orig_sysloghandler.LOG_LOCAL3})]
            self.assertEqual(
                expected_args, syslog_handler_args)

            # custom log_address - file exists, not a socket: fallback to UDP
            with open(log_address, 'w'):
                pass
            syslog_handler_args = []
            get_swift_logger({
                'log_facility': 'LOG_LOCAL3',
                'log_address': log_address,
            }, 'server', log_route='server')
            expected_args = [
                ((), {'facility': orig_sysloghandler.LOG_LOCAL3})]
            self.assertEqual(
                expected_args, syslog_handler_args)

            # custom log_address - file exists, is a socket: use it
            os.unlink(log_address)
            with contextlib.closing(
                    socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)) as sock:
                sock.settimeout(5)
                sock.bind(log_address)
                syslog_handler_args = []
                get_swift_logger({
                    'log_facility': 'LOG_LOCAL3',
                    'log_address': log_address,
                }, 'server', log_route='server')
            expected_args = [
                ((), {'address': log_address,
                      'facility': orig_sysloghandler.LOG_LOCAL3})]
            self.assertEqual(
                expected_args, syslog_handler_args)

            # Using UDP with default port
            syslog_handler_args = []
            get_swift_logger({
                'log_udp_host': 'syslog.funtimes.com',
            }, 'server', log_route='server')
            self.assertEqual([
                ((), {'address': ('syslog.funtimes.com',
                                  logging.handlers.SYSLOG_UDP_PORT),
                      'facility': orig_sysloghandler.LOG_LOCAL0})],
                syslog_handler_args)

            # Using UDP with non-default port
            syslog_handler_args = []
            get_swift_logger({
                'log_udp_host': 'syslog.funtimes.com',
                'log_udp_port': '2123',
            }, 'server', log_route='server')
            self.assertEqual([
                ((), {'address': ('syslog.funtimes.com', 2123),
                      'facility': orig_sysloghandler.LOG_LOCAL0})],
                syslog_handler_args)

        with mock.patch.object(utils.logs, 'ThreadSafeSysLogHandler',
                               side_effect=OSError(errno.EPERM, 'oops')):
            with self.assertRaises(OSError) as cm:
                get_swift_logger({
                    'log_facility': 'LOG_LOCAL3',
                    'log_address': 'log_address',
                }, 'server', log_route='server')
        self.assertEqual(errno.EPERM, cm.exception.errno)

    def test_get_swift_logger_custom_log_handlers(self):
        def custom_log_handler(conf, name, log_to_console, log_route, fmt,
                               logger, adapted_logger):
            adapted_logger.server = adapted_logger.server.upper()

        sio = StringIO()
        logger = logging.getLogger('my_logger_name')
        handler = logging.StreamHandler(sio)
        logger.addHandler(handler)
        formatter = logging.Formatter('%(levelname)s: %(server)s %(message)s')
        handler.setFormatter(formatter)

        # sanity check...
        conf = {}
        adapted_logger = get_swift_logger(
            conf, 'my_server', log_route='my_logger_name')
        adapted_logger.warning('test')
        self.assertEqual(sio.getvalue(),
                         'WARNING: my_server test\n')

        # custom log handler...
        sio = StringIO()
        handler.stream = sio
        patch_target = 'test.unit.common.utils.custom_log_handler'
        conf = {'log_custom_handlers': patch_target}
        with mock.patch(patch_target, custom_log_handler, create=True):
            adapted_logger = get_swift_logger(
                conf, 'my_server', log_route='my_logger_name')
            adapted_logger.warning('test')
        self.assertEqual(sio.getvalue(),
                         'WARNING: MY_SERVER test\n')

    @reset_logger_state
    def test_clean_logger_exception(self):
        # setup stream logging
        sio = StringIO()
        logger = get_swift_logger(None)
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
                self.assertNotIn('Traceback', log_msg)
                self.assertIn('my %s error message' % en, log_msg)
            # unfiltered
            log_exception(OSError())
            self.assertTrue('Traceback' in strip_value(sio))

            # test socket.error
            log_exception(socket.error(errno.ECONNREFUSED,
                                       'my error message'))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertNotIn('errno.ECONNREFUSED message test', log_msg)
            self.assertIn('Connection refused', log_msg)
            log_exception(socket.error(errno.EHOSTUNREACH,
                                       'my error message'))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertNotIn('my error message', log_msg)
            self.assertIn('Host unreachable', log_msg)
            log_exception(socket.error(errno.ETIMEDOUT, 'my error message'))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertNotIn('my error message', log_msg)
            self.assertIn('Connection timeout', log_msg)

            log_exception(socket.error(errno.ENETUNREACH, 'my error message'))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertNotIn('my error message', log_msg)
            self.assertIn('Network unreachable', log_msg)

            log_exception(socket.error(errno.EPIPE, 'my error message'))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertNotIn('my error message', log_msg)
            self.assertIn('Broken pipe', log_msg)
            # unfiltered
            log_exception(socket.error(0, 'my error message'))
            log_msg = strip_value(sio)
            self.assertIn('Traceback', log_msg)
            self.assertIn('my error message', log_msg)

            # test eventlet.Timeout
            with ConnectionTimeout(42, 'my error message') \
                    as connection_timeout:
                now = time.time()
                connection_timeout.created_at = now - 123.456
                with mock.patch('swift.common.utils.time.time',
                                return_value=now):
                    log_exception(connection_timeout)
                log_msg = strip_value(sio)
                self.assertNotIn('Traceback', log_msg)
                self.assertTrue('ConnectionTimeout' in log_msg)
                self.assertTrue('(42s after 123.46s)' in log_msg)
                self.assertNotIn('my error message', log_msg)

            with MessageTimeout(42, 'my error message') as message_timeout:
                log_exception(message_timeout)
                log_msg = strip_value(sio)
                self.assertNotIn('Traceback', log_msg)
                self.assertTrue('MessageTimeout' in log_msg)
                self.assertTrue('(42s)' in log_msg)
                self.assertTrue('my error message' in log_msg)

            # test BadStatusLine
            log_exception(http.client.BadStatusLine(''))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertIn('''BadStatusLine("''"''', log_msg)

            # green version is separate :-(
            log_exception(green_http_client.BadStatusLine(''))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertIn('''BadStatusLine("''"''', log_msg)

            # py3 introduced RemoteDisconnected exceptions which inherit
            # from both BadStatusLine *and* OSError; make sure those are
            # handled as BadStatusLine, not OSError
            log_exception(http.client.RemoteDisconnected(
                'Remote end closed connection'))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertIn(
                "RemoteDisconnected('Remote end closed connection'",
                log_msg)

            log_exception(green_http_client.RemoteDisconnected(
                'Remote end closed connection'))
            log_msg = strip_value(sio)
            self.assertNotIn('Traceback', log_msg)
            self.assertIn(
                "RemoteDisconnected('Remote end closed connection'",
                log_msg)

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
        logger = get_swift_logger(None)
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
        logger = get_swift_logger(None)
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
            self.assertIn('my error message', log_msg)
            self.assertNotIn('txn', log_msg)
            logger.txn_id = '12345'
            logger.error('test')
            log_msg = strip_value(sio)
            self.assertIn('txn', log_msg)
            self.assertIn('12345', log_msg)
            # test txn in info message
            self.assertEqual(logger.txn_id, '12345')
            logger.info('test')
            log_msg = strip_value(sio)
            self.assertIn('txn', log_msg)
            self.assertIn('12345', log_msg)
            # test txn already in message
            self.assertEqual(logger.txn_id, '12345')
            logger.warning('test 12345 test')
            self.assertEqual(strip_value(sio), 'test 12345 test\n')
            # Test multi line collapsing
            logger.error('my\nerror\nmessage')
            log_msg = strip_value(sio)
            self.assertIn('my#012error#012message', log_msg)

            # test client_ip
            self.assertFalse(logger.client_ip)
            logger.error('my error message')
            log_msg = strip_value(sio)
            self.assertIn('my error message', log_msg)
            self.assertNotIn('client_ip', log_msg)
            logger.client_ip = '1.2.3.4'
            logger.error('test')
            log_msg = strip_value(sio)
            self.assertIn('client_ip', log_msg)
            self.assertIn('1.2.3.4', log_msg)
            # test no client_ip on info message
            self.assertEqual(logger.client_ip, '1.2.3.4')
            logger.info('test')
            log_msg = strip_value(sio)
            self.assertNotIn('client_ip', log_msg)
            self.assertNotIn('1.2.3.4', log_msg)
            # test client_ip (and txn) already in message
            self.assertEqual(logger.client_ip, '1.2.3.4')
            logger.warning('test 1.2.3.4 test 12345')
            self.assertEqual(strip_value(sio), 'test 1.2.3.4 test 12345\n')
        finally:
            logger.logger.removeHandler(handler)

    @reset_logger_state
    def test_get_prefixed_swift_logger(self):
        # setup stream logging
        sio = StringIO()
        base_logger = get_swift_logger(None)
        handler = logging.StreamHandler(sio)
        base_logger.logger.addHandler(handler)
        logger = get_prefixed_swift_logger(base_logger, 'some prefix: ')

        def strip_value(sio):
            sio.seek(0)
            v = sio.getvalue()
            sio.truncate(0)
            return v

        try:
            # establish base case
            self.assertEqual(strip_value(sio), '')
            logger.info('test')
            self.assertEqual(strip_value(sio), 'some prefix: test\n')

            self.assertEqual(strip_value(sio), '')
            logger.info('test')
            logger.info('test')
            self.assertEqual(
                strip_value(sio),
                'some prefix: test\nsome prefix: test\n')
            self.assertEqual(strip_value(sio), '')
        finally:
            base_logger.logger.removeHandler(handler)

    @reset_logger_state
    def test_get_prefixed_swift_logger_exception_method(self):
        # setup stream logging
        sio = StringIO()
        base_logger = get_swift_logger(None)
        handler = logging.StreamHandler(sio)
        base_logger.logger.addHandler(handler)
        logger = get_prefixed_swift_logger(base_logger, 'some prefix: ')

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
            msg_lines = strip_value(sio).strip().split('\n')
            return msg_lines

        try:
            # test OSError
            for en in (errno.EIO, errno.ENOSPC):
                exc = OSError(en, 'my %s error message' % en)
                log_msg_lines = log_exception(exc)
                self.assertEqual(1, len(log_msg_lines))
                self.assertEqual('some prefix: blah: %s' % exc,
                                 log_msg_lines[0])

            # BadStatusLine
            exc = http.client.BadStatusLine('my error message')
            log_msg_lines = log_exception(exc)
            self.assertEqual(1, len(log_msg_lines))
            self.assertEqual("some prefix: blah: %r" % exc, log_msg_lines[0])

            # Timeout
            with ConnectionTimeout(99) as exc:
                log_msg_lines = log_exception(exc)
            self.assertEqual(1, len(log_msg_lines))
            self.assertNotIn('Traceback', log_msg_lines[0])
            self.assertEqual("some prefix: blah: ConnectionTimeout (99s)",
                             log_msg_lines[0])

            # unfiltered
            for exc in (OSError(), ValueError()):
                log_msg_lines = log_exception(exc)
                self.assertEqual(2, len(log_msg_lines), log_msg_lines)
                self.assertEqual('some prefix: blah: ', log_msg_lines[0])
                traceback_lines = log_msg_lines[1].split('#012')
                self.assertEqual('Traceback (most recent call last):',
                                 traceback_lines[0])
        finally:
            base_logger.logger.removeHandler(handler)

    @reset_logger_state
    def test_get_prefixed_swift_logger_non_string_values(self):
        # setup stream logging
        sio = StringIO()
        base_logger = get_swift_logger(None)
        handler = logging.StreamHandler(sio)
        base_logger.logger.addHandler(handler)
        logger = get_prefixed_swift_logger(base_logger, 'some prefix: ')
        exc = Exception('blah')

        def strip_value(sio):
            sio.seek(0)
            v = sio.getvalue()
            sio.truncate(0)
            return v

        try:
            logger = get_prefixed_swift_logger(logger, 'abc')
            self.assertEqual('abc', logger.prefix)
            logger.info('test')
            self.assertEqual(strip_value(sio), 'abctest\n')
            logger.info(exc)
            self.assertEqual(strip_value(sio), 'abcblah\n')

            logger = get_prefixed_swift_logger(logger, '')
            self.assertEqual('', logger.prefix)
            logger.info('test')
            self.assertEqual(strip_value(sio), 'test\n')
            logger.info(exc)
            self.assertEqual(strip_value(sio), 'blah\n')

            logger = get_prefixed_swift_logger(logger, 0)
            self.assertEqual(0, logger.prefix)
            logger.info('test')
            self.assertEqual(strip_value(sio), '0test\n')
            logger.info(exc)
            self.assertEqual(strip_value(sio), '0blah\n')
        finally:
            logger.logger.removeHandler(handler)

    @reset_logger_state
    def test_get_prefixed_swift_logger_replaces_prefix(self):
        # setup stream logging
        sio = StringIO()
        base_logger = get_swift_logger(None)
        handler = logging.StreamHandler(sio)
        base_logger.logger.addHandler(handler)
        logger1 = get_prefixed_swift_logger(base_logger, 'one: ')
        logger2 = get_prefixed_swift_logger(logger1, 'two: ')

        def strip_value(sio):
            sio.seek(0)
            v = sio.getvalue()
            sio.truncate(0)
            return v

        try:
            self.assertEqual(strip_value(sio), '')
            base_logger.info('test')
            self.assertEqual(strip_value(sio), 'test\n')

            self.assertEqual(strip_value(sio), '')
            logger1.info('test')
            self.assertEqual(strip_value(sio), 'one: test\n')

            self.assertEqual(strip_value(sio), '')
            logger2.info('test')
            self.assertEqual(strip_value(sio), 'two: test\n')
        finally:
            base_logger.logger.removeHandler(handler)

    def test_get_prefixed_swift_logger_isolation(self):
        # verify that the new instance's attributes are copied by value
        # from the old (except prefix), but the thread_locals are still shared
        adapted_logger = get_swift_logger(None, name='server')
        adapted_logger.thread_locals = ('id', 'ip')
        adapted_logger = get_prefixed_swift_logger(adapted_logger, 'foo')
        self.assertEqual(adapted_logger.server, 'server')
        self.assertEqual(adapted_logger.thread_locals, ('id', 'ip'))
        self.assertEqual(adapted_logger.prefix, 'foo')

        cloned_adapted_logger = get_prefixed_swift_logger(
            adapted_logger, 'boo')
        self.assertEqual(cloned_adapted_logger.server, 'server')
        self.assertEqual(cloned_adapted_logger.thread_locals, ('id', 'ip'))
        self.assertEqual(cloned_adapted_logger.txn_id, 'id')
        self.assertEqual(cloned_adapted_logger.client_ip, 'ip')
        self.assertEqual(adapted_logger.thread_locals, ('id', 'ip'))
        self.assertEqual(cloned_adapted_logger.prefix, 'boo')
        self.assertEqual(adapted_logger.prefix, 'foo')
        self.assertIs(adapted_logger.logger, cloned_adapted_logger.logger)

        cloned_adapted_logger = get_prefixed_swift_logger(
            adapted_logger, adapted_logger.prefix + 'bar')
        adapted_logger.server = 'waiter'
        self.assertEqual(adapted_logger.server, 'waiter')
        self.assertEqual(cloned_adapted_logger.server, 'server')
        self.assertEqual(adapted_logger.prefix, 'foo')
        self.assertEqual(cloned_adapted_logger.prefix, 'foobar')

        adapted_logger.thread_locals = ('x', 'y')
        self.assertEqual(adapted_logger.thread_locals, ('x', 'y'))
        self.assertEqual(cloned_adapted_logger.thread_locals, ('x', 'y'))
        self.assertIs(adapted_logger.logger, cloned_adapted_logger.logger)

    @reset_logger_state
    def test_capture_stdio(self):
        # stubs
        logger = get_swift_logger(None, 'dummy')

        # mock utils system modules
        mock_os = MockOs()
        mock_sys = MockSys()
        with mock.patch.object(utils.logs, 'os', mock_os), \
                mock.patch.object(utils.logs, 'sys', mock_sys):
            # basic test
            utils.logs.capture_stdio(logger)
            self.assertTrue(mock_sys.excepthook is not None)
            self.assertEqual(mock_os.closed_fds, mock_sys.stdio_fds)
            self.assertIsInstance(mock_sys.stdout,
                                  utils.logs.LoggerFileObject)
            self.assertIsInstance(mock_sys.stderr,
                                  utils.logs.LoggerFileObject)

        # reset; test same args, but exc when trying to close stdio
        mock_os = MockOs(raise_funcs=('dup2',))
        mock_sys = MockSys()
        with mock.patch.object(utils.logs, 'os', mock_os), \
                mock.patch.object(utils.logs, 'sys', mock_sys):
            # test unable to close stdio
            utils.logs.capture_stdio(logger)
            self.assertTrue(utils.logs.sys.excepthook is not None)
            self.assertEqual(utils.logs.os.closed_fds, [])
            self.assertIsInstance(mock_sys.stdout,
                                  utils.logs.LoggerFileObject)
            self.assertIsInstance(mock_sys.stderr,
                                  utils.logs.LoggerFileObject)

        # reset; test some other args
        mock_os = MockOs()
        mock_sys = MockSys()
        with mock.patch.object(utils.logs, 'os', mock_os), \
                mock.patch.object(utils.logs, 'sys', mock_sys):
            logger = get_swift_logger(None, log_to_console=True)

            # test console log
            utils.logs.capture_stdio(logger, capture_stdout=False,
                                     capture_stderr=False)
            self.assertTrue(utils.logs.sys.excepthook is not None)
            # when logging to console, stderr remains open
            self.assertEqual(mock_os.closed_fds,
                             mock_sys.stdio_fds[:2])
            reset_loggers()

            # stdio not captured
            self.assertFalse(isinstance(mock_sys.stdout,
                                        utils.logs.LoggerFileObject))
            self.assertFalse(isinstance(mock_sys.stderr,
                                        utils.logs.LoggerFileObject))

    @reset_logger_state
    def test_get_swift_logger_console(self):
        logger = get_swift_logger(None)
        console_handlers = [h for h in logger.logger.handlers if
                            isinstance(h, logging.StreamHandler)]
        self.assertFalse(console_handlers)
        logger = get_swift_logger(None, log_to_console=True)
        console_handlers = [h for h in logger.logger.handlers if
                            isinstance(h, logging.StreamHandler)]
        self.assertTrue(console_handlers)
        # make sure you can't have two console handlers
        self.assertEqual(len(console_handlers), 1)
        old_handler = console_handlers[0]
        logger = get_swift_logger(None, log_to_console=True)
        console_handlers = [h for h in logger.logger.handlers if
                            isinstance(h, logging.StreamHandler)]
        self.assertEqual(len(console_handlers), 1)
        new_handler = console_handlers[0]
        self.assertNotEqual(new_handler, old_handler)

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

    def test_log_string_formatter(self):
        # Plain ASCII
        lf = utils.LogStringFormatter()
        self.assertEqual(lf.format('{a} {b}', a='Swift is', b='great'),
                         'Swift is great')

        lf = utils.LogStringFormatter()
        self.assertEqual(lf.format('{a} {b}', a='', b='great'),
                         ' great')

        lf = utils.LogStringFormatter(default='-')
        self.assertEqual(lf.format('{a} {b}', a='', b='great'),
                         '- great')

        lf = utils.LogStringFormatter(default='-', quote=True)
        self.assertEqual(lf.format('{a} {b}', a='', b='great'),
                         '- great')

        lf = utils.LogStringFormatter(quote=True)
        self.assertEqual(lf.format('{a} {b}', a='Swift is', b='great'),
                         'Swift%20is great')

        # Unicode & co
        lf = utils.LogStringFormatter()
        self.assertEqual(lf.format('{a} {b}', a='Swift est',
                                   b=u'g\u00e9nial ^^'),
                         u'Swift est g\u00e9nial ^^')

        lf = utils.LogStringFormatter(quote=True)
        self.assertEqual(lf.format('{a} {b}', a='Swift est',
                                   b=u'g\u00e9nial ^^'),
                         'Swift%20est g%C3%A9nial%20%5E%5E')

    def test_str_anonymizer(self):
        anon = utils.StrAnonymizer('Swift is great!', 'md5', '')
        self.assertEqual(anon, 'Swift is great!')
        self.assertEqual(anon.anonymized,
                         '{MD5}45e6f00d48fdcf86213602a87df18772')

        anon = utils.StrAnonymizer('Swift is great!', 'sha1', '')
        self.assertEqual(anon, 'Swift is great!')
        self.assertEqual(anon.anonymized,
                         '{SHA1}0010a3df215495d8bfa0ae4b66acc2afcc8f4c5c')

        anon = utils.StrAnonymizer('Swift is great!', 'md5', 'salty_secret')
        self.assertEqual(anon, 'Swift is great!')
        self.assertEqual(anon.anonymized,
                         '{SMD5}ef4ce28fe3bdd10b6659458ceb1f3f0c')

        anon = utils.StrAnonymizer('Swift is great!', 'sha1', 'salty_secret')
        self.assertEqual(anon, 'Swift is great!')
        self.assertEqual(anon.anonymized,
                         '{SSHA1}a4968f76acaddff0eb4069ebe8805d9cab44c9fe')

        self.assertRaises(ValueError, utils.StrAnonymizer,
                          'Swift is great!', 'sha257', '')

    def test_str_anonymizer_python_maddness(self):
        utils.StrAnonymizer('Swift is great!', 'sha1', '')
        self.assertRaises(ValueError, utils.StrAnonymizer,
                          'Swift is great!', 'sha257', '')

    def test_str_format_time(self):
        dt = utils.StrFormatTime(10000.123456789)
        self.assertEqual(str(dt), '10000.123456789')
        self.assertEqual(dt.datetime, '01/Jan/1970/02/46/40')
        self.assertEqual(dt.iso8601, '1970-01-01T02:46:40')
        self.assertEqual(dt.asctime, 'Thu Jan  1 02:46:40 1970')
        self.assertEqual(dt.s, '10000')
        self.assertEqual(dt.ms, '123')
        self.assertEqual(dt.us, '123456')
        self.assertEqual(dt.ns, '123456789')
        self.assertEqual(dt.a, 'Thu')
        self.assertEqual(dt.A, 'Thursday')
        self.assertEqual(dt.b, 'Jan')
        self.assertEqual(dt.B, 'January')
        self.assertEqual(dt.c, 'Thu Jan  1 02:46:40 1970')
        self.assertEqual(dt.d, '01')
        self.assertEqual(dt.H, '02')
        self.assertEqual(dt.I, '02')
        self.assertEqual(dt.j, '001')
        self.assertEqual(dt.m, '01')
        self.assertEqual(dt.M, '46')
        self.assertEqual(dt.p, 'AM')
        self.assertEqual(dt.S, '40')
        self.assertEqual(dt.U, '00')
        self.assertEqual(dt.w, '4')
        self.assertEqual(dt.W, '00')
        self.assertEqual(dt.x, '01/01/70')
        self.assertEqual(dt.X, '02:46:40')
        self.assertEqual(dt.y, '70')
        self.assertEqual(dt.Y, '1970')
        self.assertIn(dt.Z, ('GMT', 'UTC'))  # It depends of Python 2/3
        self.assertRaises(ValueError, getattr, dt, 'z')

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
        with mock.patch('time.time', mock.MagicMock(side_effect=[10001.0])):
            with mock.patch(
                    'os.getpid', mock.MagicMock(return_value=server_pid)):
                self.assertEqual(
                    exp_line,
                    utils.get_log_line(req, res, trans_time, additional_info,
                                       utils.LOG_LINE_DEFAULT_FORMAT,
                                       'md5', '54LT'))


class TestSwiftLogAdapter(unittest.TestCase):

    def setUp(self):
        self.core_logger = logging.getLogger('test')
        self.core_logger.setLevel(logging.INFO)
        self.sio = StringIO()
        self.handler = logging.StreamHandler(self.sio)
        self.core_logger.addHandler(self.handler)

    def tearDown(self):
        self.core_logger.removeHandler(self.handler)

    def read_sio(self):
        self.sio.seek(0)
        v = self.sio.getvalue()
        self.sio.truncate(0)
        return v

    def test_init(self):
        adapter = SwiftLogAdapter(self.core_logger, 'my-server')
        self.assertIs(self.core_logger, adapter.logger)
        self.assertEqual('my-server', adapter.server)
        self.assertEqual('', adapter.prefix)
        adapter.info('hello')
        self.assertEqual('hello\n', self.read_sio())

    def test_init_with_prefix(self):
        adapter = SwiftLogAdapter(self.core_logger, 'my-server', 'my-prefix: ')
        self.assertIs(self.core_logger, adapter.logger)
        self.assertEqual('my-server', adapter.server)
        self.assertEqual('my-prefix: ', adapter.prefix)
        adapter.info('hello')
        self.assertEqual('my-prefix: hello\n', self.read_sio())

    def test_formatter_extras(self):
        formatter = logging.Formatter(
            '%(levelname)s: %(server)s %(message)s %(txn_id)s %(client_ip)s')
        self.handler.setFormatter(formatter)
        adapter = SwiftLogAdapter(self.core_logger, 'my-server')
        adapter.txn_id = 'my-txn-id'
        adapter.client_ip = '1.2.3.4'
        adapter.info('hello')
        self.assertEqual('INFO: my-server hello my-txn-id 1.2.3.4\n',
                         self.read_sio())

    @reset_logger_state
    def test_thread_locals(self):
        adapter1 = SwiftLogAdapter(self.core_logger, 'foo')
        adapter2 = SwiftLogAdapter(self.core_logger, 'foo')
        locals1 = ('tx_123', '1.2.3.4')
        adapter1.thread_locals = locals1
        self.assertEqual(adapter1.thread_locals, locals1)
        self.assertEqual(adapter2.thread_locals, locals1)

        locals2 = ('tx_456', '1.2.3.456')
        adapter2.thread_locals = locals2
        self.assertEqual(adapter1.thread_locals, locals2)
        self.assertEqual(adapter2.thread_locals, locals2)

        adapter1.thread_locals = (None, None)
        self.assertEqual(adapter1.thread_locals, (None, None))
        self.assertEqual(adapter2.thread_locals, (None, None))

        adapter1.txn_id = '5678'
        self.assertEqual('5678', adapter1.txn_id)
        self.assertEqual('5678', adapter2.txn_id)
        self.assertIsNone(adapter1.client_ip)
        self.assertIsNone(adapter2.client_ip)
        self.assertEqual(('5678', None), adapter1.thread_locals)
        self.assertEqual(('5678', None), adapter2.thread_locals)

        adapter1.client_ip = '5.6.7.8'
        self.assertEqual('5678', adapter1.txn_id)
        self.assertEqual('5678', adapter2.txn_id)
        self.assertEqual('5.6.7.8', adapter1.client_ip)
        self.assertEqual('5.6.7.8', adapter2.client_ip)
        self.assertEqual(('5678', '5.6.7.8'), adapter1.thread_locals)
        self.assertEqual(('5678', '5.6.7.8'), adapter2.thread_locals)

    @reset_logger_state
    def test_thread_locals_stacked_adapter(self):
        adapter1 = SwiftLogAdapter(self.core_logger, 'foo')
        # adapter2 is stacked on adapter1
        adapter2 = SwiftLogAdapter(adapter1, 'foo')
        self.assertIs(adapter1, adapter2.logger)
        # test the setter
        adapter1.thread_locals = ('id', 'ip')
        self.assertEqual(adapter1.thread_locals, ('id', 'ip'))
        self.assertEqual(adapter2.thread_locals, ('id', 'ip'))
        # reset
        adapter1.thread_locals = (None, None)
        self.assertEqual(adapter1.thread_locals, (None, None))
        self.assertEqual(adapter2.thread_locals, (None, None))

        adapter1.txn_id = '1234'
        adapter1.client_ip = '1.2.3.4'
        self.assertEqual(adapter1.thread_locals, ('1234', '1.2.3.4'))
        self.assertEqual(adapter2.thread_locals, ('1234', '1.2.3.4'))
        adapter2.txn_id = '5678'
        adapter2.client_ip = '5.6.7.8'
        self.assertEqual(adapter1.thread_locals, ('5678', '5.6.7.8'))
        self.assertEqual(adapter2.thread_locals, ('5678', '5.6.7.8'))

    def test_exception(self):
        # verify that the adapter routes exception calls to SwiftLogAdapter
        # for special case handling
        adapter = SwiftLogAdapter(self.core_logger, 'foo')
        try:
            raise OSError(errno.ECONNREFUSED, 'oserror')
        except OSError:
            with mock.patch('logging.LoggerAdapter.error') as mocked:
                adapter.exception('Caught')
        mocked.assert_called_with('Caught: Connection refused')


class TestPipeMutex(unittest.TestCase):
    def setUp(self):
        self.mutex = utils.PipeMutex()

    def tearDown(self):
        self.mutex.close()

    def test_nonblocking(self):
        evt_lock1 = eventlet.event.Event()
        evt_lock2 = eventlet.event.Event()
        evt_unlock = eventlet.event.Event()

        def get_the_lock():
            self.mutex.acquire()
            evt_lock1.send('got the lock')
            evt_lock2.wait()
            self.mutex.release()
            evt_unlock.send('released the lock')

        eventlet.spawn(get_the_lock)
        evt_lock1.wait()  # Now, the other greenthread has the lock.

        self.assertFalse(self.mutex.acquire(blocking=False))
        evt_lock2.send('please release the lock')
        evt_unlock.wait()  # The other greenthread has released the lock.
        self.assertTrue(self.mutex.acquire(blocking=False))

    def test_recursive(self):
        self.assertTrue(self.mutex.acquire(blocking=False))
        self.assertTrue(self.mutex.acquire(blocking=False))

        def try_acquire_lock():
            return self.mutex.acquire(blocking=False)

        self.assertFalse(eventlet.spawn(try_acquire_lock).wait())
        self.mutex.release()
        self.assertFalse(eventlet.spawn(try_acquire_lock).wait())
        self.mutex.release()
        self.assertTrue(eventlet.spawn(try_acquire_lock).wait())

    def test_context_manager_api(self):
        def try_acquire_lock():
            return self.mutex.acquire(blocking=False)

        with self.mutex as ref:
            self.assertIs(ref, self.mutex)
            self.assertFalse(eventlet.spawn(try_acquire_lock).wait())
        self.assertTrue(eventlet.spawn(try_acquire_lock).wait())

    def test_release_without_acquire(self):
        self.assertRaises(RuntimeError, self.mutex.release)

    def test_too_many_releases(self):
        self.mutex.acquire()
        self.mutex.release()
        self.assertRaises(RuntimeError, self.mutex.release)

    def test_wrong_releaser(self):
        self.mutex.acquire()
        with quiet_eventlet_exceptions():
            self.assertRaises(RuntimeError,
                              eventlet.spawn(self.mutex.release).wait)

    def test_blocking(self):
        evt = eventlet.event.Event()

        sequence = []

        def coro1():
            eventlet.sleep(0)  # let coro2 go

            self.mutex.acquire()
            sequence.append('coro1 acquire')
            evt.send('go')
            self.mutex.release()
            sequence.append('coro1 release')

        def coro2():
            evt.wait()  # wait for coro1 to start us
            self.mutex.acquire()
            sequence.append('coro2 acquire')
            self.mutex.release()
            sequence.append('coro2 release')

        c1 = eventlet.spawn(coro1)
        c2 = eventlet.spawn(coro2)

        c1.wait()
        c2.wait()

        self.assertEqual(sequence, [
            'coro1 acquire',
            'coro1 release',
            'coro2 acquire',
            'coro2 release'])

    def test_blocking_tpool(self):
        # Note: this test's success isn't a guarantee that the mutex is
        # working. However, this test's failure means that the mutex is
        # definitely broken.
        sequence = []

        def do_stuff():
            n = 10
            while n > 0:
                self.mutex.acquire()
                sequence.append("<")
                eventlet.sleep(0.0001)
                sequence.append(">")
                self.mutex.release()
                n -= 1

        greenthread1 = eventlet.spawn(do_stuff)
        greenthread2 = eventlet.spawn(do_stuff)

        real_thread1 = eventlet.patcher.original('threading').Thread(
            target=do_stuff)
        real_thread1.start()

        real_thread2 = eventlet.patcher.original('threading').Thread(
            target=do_stuff)
        real_thread2.start()

        greenthread1.wait()
        greenthread2.wait()
        real_thread1.join()
        real_thread2.join()

        self.assertEqual(''.join(sequence), "<>" * 40)

    def test_blocking_preserves_ownership(self):
        pthread1_event = eventlet.patcher.original('threading').Event()
        pthread2_event1 = eventlet.patcher.original('threading').Event()
        pthread2_event2 = eventlet.patcher.original('threading').Event()
        thread_id = []
        owner = []

        def pthread1():
            thread_id.append(id(eventlet.greenthread.getcurrent()))
            self.mutex.acquire()
            owner.append(self.mutex.owner)
            pthread2_event1.set()

            orig_os_write = utils.os.write

            def patched_os_write(*a, **kw):
                try:
                    return orig_os_write(*a, **kw)
                finally:
                    pthread1_event.wait()

            with mock.patch.object(utils.os, 'write', patched_os_write):
                self.mutex.release()
            pthread2_event2.set()

        def pthread2():
            pthread2_event1.wait()  # ensure pthread1 acquires lock first
            thread_id.append(id(eventlet.greenthread.getcurrent()))
            self.mutex.acquire()
            pthread1_event.set()
            pthread2_event2.wait()
            owner.append(self.mutex.owner)
            self.mutex.release()

        real_thread1 = eventlet.patcher.original('threading').Thread(
            target=pthread1)
        real_thread1.start()

        real_thread2 = eventlet.patcher.original('threading').Thread(
            target=pthread2)
        real_thread2.start()

        real_thread1.join()
        real_thread2.join()
        self.assertEqual(thread_id, owner)
        self.assertIsNone(self.mutex.owner)

    @classmethod
    def tearDownClass(cls):
        # PipeMutex turns this off when you instantiate one
        eventlet.debug.hub_prevent_multiple_readers(True)


class TestNoopMutex(unittest.TestCase):
    def setUp(self):
        self.mutex = utils.NoopMutex()

    def test_acquire_release_api(self):
        # Prior to 3.13, logging called these explicitly
        self.mutex.acquire()
        self.mutex.release()

    def test_context_manager_api(self):
        # python 3.13 started using it as a context manager
        def try_acquire_lock():
            return self.mutex.acquire(blocking=False)

        with self.mutex as ref:
            self.assertIs(ref, self.mutex)
