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


import errno
import hashlib
import logging
from logging.handlers import SysLogHandler
import os
import socket
import stat
import string
import sys
import time
import fcntl
import eventlet
import datetime

from swift.common.utils.base import md5, quote, split_path
from swift.common.utils.timestamp import UTC
from swift.common.utils.config import config_true_value
# common.utils imports a fully qualified common.exceptions so that
# common.exceptions can import common.utils with out a circular import error
# (if we only make reference to attributes of a module w/i our function/method
# bodies fully qualifed module names can have their attributes lazily
# evaluated); as the only other module with-in utils that imports exceptions:
# we do the same here
import swift.common.exceptions

from eventlet.green.http import client as green_http_client
import http.client
from eventlet.green import threading


NOTICE = 25

LOG_LINE_DEFAULT_FORMAT = '{remote_addr} - - [{time.d}/{time.b}/{time.Y}' \
                          ':{time.H}:{time.M}:{time.S} +0000] ' \
                          '"{method} {path}" {status} {content_length} ' \
                          '"{referer}" "{txn_id}" "{user_agent}" ' \
                          '{trans_time:.4f} "{additional_info}" {pid} ' \
                          '{policy_index}'


def logging_monkey_patch():
    # explicitly patch the logging lock
    logging._lock = logging.threading.RLock()
    # setup notice level logging
    logging.addLevelName(NOTICE, 'NOTICE')
    SysLogHandler.priority_map['NOTICE'] = 'notice'
    # Trying to log threads while monkey-patched can lead to deadlocks; see
    # https://bugs.launchpad.net/swift/+bug/1895739
    logging.logThreads = 0


class PipeMutex(object):
    """
    Mutex using a pipe. Works across both greenlets and real threads, even
    at the same time.
    """

    def __init__(self):
        self.rfd, self.wfd = os.pipe()

        # You can't create a pipe in non-blocking mode; you must set it
        # later.
        rflags = fcntl.fcntl(self.rfd, fcntl.F_GETFL)
        fcntl.fcntl(self.rfd, fcntl.F_SETFL, rflags | os.O_NONBLOCK)
        os.write(self.wfd, b'-')  # start unlocked

        self.owner = None
        self.recursion_depth = 0

        # Usually, it's an error to have multiple greenthreads all waiting
        # to read the same file descriptor. It's often a sign of inadequate
        # concurrency control; for example, if you have two greenthreads
        # trying to use the same memcache connection, they'll end up writing
        # interleaved garbage to the socket or stealing part of each others'
        # responses.
        #
        # In this case, we have multiple greenthreads waiting on the same
        # file descriptor by design. This lets greenthreads in real thread A
        # wait with greenthreads in real thread B for the same mutex.
        # Therefore, we must turn off eventlet's multiple-reader detection.
        #
        # It would be better to turn off multiple-reader detection for only
        # our calls to trampoline(), but eventlet does not support that.
        eventlet.debug.hub_prevent_multiple_readers(False)

    def acquire(self, blocking=True):
        """
        Acquire the mutex.

        If called with blocking=False, returns True if the mutex was
        acquired and False if it wasn't. Otherwise, blocks until the mutex
        is acquired and returns True.

        This lock is recursive; the same greenthread may acquire it as many
        times as it wants to, though it must then release it that many times
        too.
        """
        current_greenthread_id = id(eventlet.greenthread.getcurrent())
        if self.owner == current_greenthread_id:
            self.recursion_depth += 1
            return True

        while True:
            try:
                # If there is a byte available, this will read it and remove
                # it from the pipe. If not, this will raise OSError with
                # errno=EAGAIN.
                os.read(self.rfd, 1)
                self.owner = current_greenthread_id
                return True
            except OSError as err:
                if err.errno != errno.EAGAIN:
                    raise

                if not blocking:
                    return False

                # Tell eventlet to suspend the current greenthread until
                # self.rfd becomes readable. This will happen when someone
                # else writes to self.wfd.
                eventlet.hubs.trampoline(self.rfd, read=True)

    def release(self):
        """
        Release the mutex.
        """
        current_greenthread_id = id(eventlet.greenthread.getcurrent())
        if self.owner != current_greenthread_id:
            raise RuntimeError("cannot release un-acquired lock")

        if self.recursion_depth > 0:
            self.recursion_depth -= 1
            return

        self.owner = None
        os.write(self.wfd, b'X')

    def close(self):
        """
        Close the mutex. This releases its file descriptors.

        You can't use a mutex after it's been closed.
        """
        if self.wfd is not None:
            os.close(self.rfd)
            self.rfd = None
            os.close(self.wfd)
            self.wfd = None
        self.owner = None
        self.recursion_depth = 0

    def __del__(self):
        # We need this so we don't leak file descriptors. Otherwise, if you
        # call get_swift_logger() and don't explicitly dispose of it by calling
        # logger.logger.handlers[0].lock.close() [1], the pipe file
        # descriptors are leaked.
        #
        # This only really comes up in tests. Swift processes tend to call
        # get_swift_logger() once and then hang on to it until they exit,
        # but the test suite calls get_swift_logger() a lot.
        #
        # [1] and that's a completely ridiculous thing to expect callers to
        # do, so nobody does it and that's okay.
        self.close()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()


class NoopMutex(object):
    """
    "Mutex" that doesn't lock anything.

    We only allow our syslog logging to be configured via UDS or UDP, neither
    of which have the message-interleaving trouble you'd expect from TCP or
    file handlers.
    """

    def __init__(self):
        # Usually, it's an error to have multiple greenthreads all waiting
        # to write to the same file descriptor. It's often a sign of inadequate
        # concurrency control; for example, if you have two greenthreads
        # trying to use the same memcache connection, they'll end up writing
        # interleaved garbage to the socket or stealing part of each others'
        # responses.
        #
        # In this case, we have multiple greenthreads waiting on the same
        # (logging) file descriptor by design. So, similar to the PipeMutex,
        # we must turn off eventlet's multiple-waiter detection.
        #
        # It would be better to turn off multiple-reader detection for only
        # the logging socket fd, but eventlet does not support that.
        eventlet.debug.hub_prevent_multiple_readers(False)

    def acquire(self, blocking=True):
        pass

    def release(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


class ThreadSafeSysLogHandler(SysLogHandler):
    def createLock(self):
        if config_true_value(os.environ.get(
                'SWIFT_NOOP_LOGGING_MUTEX') or 'true'):
            self.lock = NoopMutex()
        else:
            self.lock = PipeMutex()


# double inheritance to support property with setter
class SwiftLogAdapter(logging.LoggerAdapter, object):
    """
    A LogAdapter that modifies the adapted ``Logger`` instance
    in the following ways:

    * Performs some reformatting on calls to :meth:`exception`.
    * Provides threadlocal txn_id and client_ip attributes.
    * Adds the txn_id, client_ip and server attributes to the ``extras`` dict
      when a message is processed.
    * Adds the given prefix to the start of each log message.
    * Provides a notice method for logging at NOTICE level.
    """

    _cls_thread_local = threading.local()

    def __init__(self, logger, server, prefix=''):
        logging.LoggerAdapter.__init__(self, logger, {})
        self.prefix = prefix
        self.server = server
        self.warn = self.warning

    @property
    def txn_id(self):
        if hasattr(self._cls_thread_local, 'txn_id'):
            return self._cls_thread_local.txn_id

    @txn_id.setter
    def txn_id(self, value):
        self._cls_thread_local.txn_id = value

    @property
    def client_ip(self):
        if hasattr(self._cls_thread_local, 'client_ip'):
            return self._cls_thread_local.client_ip

    @client_ip.setter
    def client_ip(self, value):
        self._cls_thread_local.client_ip = value

    @property
    def thread_locals(self):
        return (self.txn_id, self.client_ip)

    @thread_locals.setter
    def thread_locals(self, value):
        self.txn_id, self.client_ip = value

    def process(self, msg, kwargs):
        """
        Add extra info to message
        """
        kwargs['extra'] = {'server': self.server, 'txn_id': self.txn_id,
                           'client_ip': self.client_ip}
        msg = '%s%s' % (self.prefix, msg)
        return msg, kwargs

    def notice(self, msg, *args, **kwargs):
        """
        Convenience function for syslog priority LOG_NOTICE. The python
        logging lvl is set to 25, just above info.  SysLogHandler is
        monkey patched to map this log lvl to the LOG_NOTICE syslog
        priority.
        """
        self.log(NOTICE, msg, *args, **kwargs)

    def _exception(self, msg, *args, **kwargs):
        # We up-call to exception() where stdlib uses error() so we can get
        # some of the traceback suppression from LogAdapter, below
        logging.LoggerAdapter.exception(self, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        _junk, exc, _junk = sys.exc_info()
        call = self.error
        emsg = ''
        if isinstance(exc, (http.client.BadStatusLine,
                            green_http_client.BadStatusLine)):
            # Use error(); not really exceptional
            emsg = repr(exc)
            # Note that on py3, we've seen a RemoteDisconnected error getting
            # raised, which inherits from *both* BadStatusLine and OSError;
            # we want it getting caught here
        elif isinstance(exc, (OSError, socket.error)):
            if exc.errno in (errno.EIO, errno.ENOSPC):
                emsg = str(exc)
            elif exc.errno == errno.ECONNREFUSED:
                emsg = 'Connection refused'
            elif exc.errno == errno.ECONNRESET:
                emsg = 'Connection reset'
            elif exc.errno == errno.EHOSTUNREACH:
                emsg = 'Host unreachable'
            elif exc.errno == errno.ENETUNREACH:
                emsg = 'Network unreachable'
            elif exc.errno == errno.ETIMEDOUT:
                emsg = 'Connection timeout'
            elif exc.errno == errno.EPIPE:
                emsg = 'Broken pipe'
            else:
                call = self._exception
        elif isinstance(exc, eventlet.Timeout):
            emsg = exc.__class__.__name__
            detail = '%ss' % exc.seconds
            if hasattr(exc, 'created_at'):
                detail += ' after %0.2fs' % (time.time() - exc.created_at)
            emsg += ' (%s)' % detail
            if isinstance(exc, swift.common.exceptions.MessageTimeout):
                if exc.msg:
                    emsg += ' %s' % exc.msg
        else:
            call = self._exception
        call('%s: %s' % (msg, emsg), *args, **kwargs)


class SwiftLogFormatter(logging.Formatter):
    """
    Custom logging.Formatter will append txn_id to a log message if the
    record has one and the message does not. Optionally it can shorten
    overly long log lines.
    """

    def __init__(self, fmt=None, datefmt=None, max_line_length=0):
        logging.Formatter.__init__(self, fmt=fmt, datefmt=datefmt)
        self.max_line_length = max_line_length

    def format(self, record):
        if not hasattr(record, 'server'):
            # Catch log messages that were not initiated by swift
            # (for example, the keystone auth middleware)
            record.server = record.name

        # Included from Python's logging.Formatter and then altered slightly to
        # replace \n with #012
        record.message = record.getMessage()
        if self._fmt.find('%(asctime)') >= 0:
            record.asctime = self.formatTime(record, self.datefmt)
        msg = (self._fmt % record.__dict__).replace('\n', '#012')
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = self.formatException(
                    record.exc_info).replace('\n', '#012')
        if record.exc_text:
            if not msg.endswith('#012'):
                msg = msg + '#012'
            msg = msg + record.exc_text

        if (hasattr(record, 'txn_id') and record.txn_id and
                record.txn_id not in msg):
            msg = "%s (txn: %s)" % (msg, record.txn_id)
        if (hasattr(record, 'client_ip') and record.client_ip and
                record.levelno != logging.INFO and
                record.client_ip not in msg):
            msg = "%s (client_ip: %s)" % (msg, record.client_ip)
        if self.max_line_length > 0 and len(msg) > self.max_line_length:
            if self.max_line_length < 7:
                msg = msg[:self.max_line_length]
            else:
                approxhalf = (self.max_line_length - 5) // 2
                msg = msg[:approxhalf] + " ... " + msg[-approxhalf:]
        return msg


class LoggerFileObject(object):

    # Note: this is greenthread-local storage
    _cls_thread_local = threading.local()

    def __init__(self, logger, log_type='STDOUT'):
        self.logger = logger
        self.log_type = log_type

    def write(self, value):
        # We can get into a nasty situation when logs are going to syslog
        # and syslog dies.
        #
        # It's something like this:
        #
        # (A) someone logs something
        #
        # (B) there's an exception in sending to /dev/log since syslog is
        #     not working
        #
        # (C) logging takes that exception and writes it to stderr (see
        #     logging.Handler.handleError)
        #
        # (D) stderr was replaced with a LoggerFileObject at process start,
        #     so the LoggerFileObject takes the provided string and tells
        #     its logger to log it (to syslog, naturally).
        #
        # Then, steps B through D repeat until we run out of stack.
        if getattr(self._cls_thread_local, 'already_called_write', False):
            return

        self._cls_thread_local.already_called_write = True
        try:
            value = value.strip()
            if value:
                if 'Connection reset by peer' in value:
                    self.logger.error(
                        '%s: Connection reset by peer', self.log_type)
                else:
                    self.logger.error('%(type)s: %(value)s',
                                      {'type': self.log_type, 'value': value})
        finally:
            self._cls_thread_local.already_called_write = False

    def writelines(self, values):
        if getattr(self._cls_thread_local, 'already_called_writelines', False):
            return

        self._cls_thread_local.already_called_writelines = True
        try:
            self.logger.error('%(type)s: %(value)s',
                              {'type': self.log_type,
                               'value': '#012'.join(values)})
        finally:
            self._cls_thread_local.already_called_writelines = False

    def close(self):
        pass

    def flush(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        raise IOError(errno.EBADF, 'Bad file descriptor')

    def read(self, size=-1):
        raise IOError(errno.EBADF, 'Bad file descriptor')

    def readline(self, size=-1):
        raise IOError(errno.EBADF, 'Bad file descriptor')

    def tell(self):
        return 0

    def xreadlines(self):
        return self


class LogLevelFilter(object):
    """
    Drop messages for the logger based on level.

    This is useful when dependencies log too much information.

    :param level: All messages at or below this level are dropped
                  (DEBUG < INFO < WARN < ERROR < CRITICAL|FATAL)
                  Default: DEBUG
    """

    def __init__(self, level=logging.DEBUG):
        self.level = level

    def filter(self, record):
        if record.levelno <= self.level:
            return 0
        return 1


def get_swift_logger(conf, name=None, log_to_console=False, log_route=None,
                     fmt="%(server)s: %(message)s"):
    """
    Get the current system logger using config settings.

    **Log config and defaults**::

        log_facility = LOG_LOCAL0
        log_level = INFO
        log_name = swift
        log_max_line_length = 0
        log_udp_host = (disabled)
        log_udp_port = logging.handlers.SYSLOG_UDP_PORT
        log_address = /dev/log

    :param conf: Configuration dict to read settings from
    :param name: This value is used to populate the ``server`` field in
                 the log format, as the default value for ``log_route``;
                 defaults to the ``log_name`` value in ``conf``, if it exists,
                 or to 'swift'.
    :param log_to_console: Add handler which writes to console on stderr
    :param log_route: Route for the logging, not emitted to the log, just used
                      to separate logging configurations; defaults to the value
                      of ``name`` or whatever ``name`` defaults to. This value
                      is used as the name attribute of the
                      ``logging.LogAdapter`` that is returned.
    :param fmt: Override log format
    :return: an instance of ``SwiftLogAdapter``
    """
    # note: log_name is typically specified in conf (i.e. defined by
    # operators), whereas log_route is typically hard-coded in callers of
    # get_swift_logger (i.e. defined by developers)
    if not conf:
        conf = {}
    if name is None:
        name = conf.get('log_name', 'swift')
    if not log_route:
        log_route = name
    logger = logging.getLogger(log_route)
    logger.propagate = False
    # all new handlers will get the same formatter
    formatter = SwiftLogFormatter(
        fmt=fmt, max_line_length=int(conf.get('log_max_line_length', 0)))

    # get_swift_logger will only ever add one SysLog Handler to a logger
    if not hasattr(get_swift_logger, 'handler4logger'):
        get_swift_logger.handler4logger = {}
    if logger in get_swift_logger.handler4logger:
        logger.removeHandler(get_swift_logger.handler4logger[logger])

    # facility for this logger will be set by last call wins
    facility = getattr(SysLogHandler, conf.get('log_facility', 'LOG_LOCAL0'),
                       SysLogHandler.LOG_LOCAL0)
    udp_host = conf.get('log_udp_host')
    if udp_host:
        udp_port = int(conf.get('log_udp_port',
                                logging.handlers.SYSLOG_UDP_PORT))
        handler = ThreadSafeSysLogHandler(address=(udp_host, udp_port),
                                          facility=facility)
    else:
        log_address = conf.get('log_address', '/dev/log')
        handler = None
        try:
            mode = os.stat(log_address).st_mode
            if stat.S_ISSOCK(mode):
                handler = ThreadSafeSysLogHandler(address=log_address,
                                                  facility=facility)
        except (OSError, socket.error) as e:
            # If either /dev/log isn't a UNIX socket or it does not exist at
            # all then py2 would raise an error
            if e.errno not in [errno.ENOTSOCK, errno.ENOENT]:
                raise
        if handler is None:
            # fallback to default UDP
            handler = ThreadSafeSysLogHandler(facility=facility)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    get_swift_logger.handler4logger[logger] = handler

    # setup console logging
    if log_to_console or hasattr(get_swift_logger, 'console_handler4logger'):
        # remove pre-existing console handler for this logger
        if not hasattr(get_swift_logger, 'console_handler4logger'):
            get_swift_logger.console_handler4logger = {}
        if logger in get_swift_logger.console_handler4logger:
            logger.removeHandler(
                get_swift_logger.console_handler4logger[logger])

        console_handler = logging.StreamHandler(sys.__stderr__)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        get_swift_logger.console_handler4logger[logger] = console_handler

    # set the level for the logger
    logger.setLevel(
        getattr(logging, conf.get('log_level', 'INFO').upper(), logging.INFO))

    adapted_logger = SwiftLogAdapter(logger, name)
    other_handlers = conf.get('log_custom_handlers', None)
    if other_handlers:
        log_custom_handlers = [s.strip() for s in other_handlers.split(',')
                               if s.strip()]
        for hook in log_custom_handlers:
            try:
                mod, fnc = hook.rsplit('.', 1)
                logger_hook = getattr(__import__(mod, fromlist=[fnc]), fnc)
                logger_hook(conf, name, log_to_console, log_route, fmt,
                            logger, adapted_logger)
            except (AttributeError, ImportError):
                print('Error calling custom handler [%s]' % hook,
                      file=sys.stderr)
            except ValueError:
                print('Invalid custom handler format [%s]' % hook,
                      file=sys.stderr)

    return adapted_logger


def get_prefixed_swift_logger(swift_logger, prefix):
    """
    Return a clone of the given ``swift_logger`` with a new prefix string
    that replaces the prefix string of the given ``swift_logger``.

    :param swift_logger: an instance of ``SwiftLogAdapter``.
    :param prefix: a string prefix.
    :returns: a new instance of ``SwiftLogAdapter``.
    """
    return SwiftLogAdapter(
        swift_logger.logger, swift_logger.server, prefix=prefix)


class NullLogger(object):
    """A no-op logger for eventlet wsgi."""

    def write(self, *args):
        # "Logs" the args to nowhere
        pass

    def exception(self, *args):
        pass

    def critical(self, *args):
        pass

    def error(self, *args):
        pass

    def warning(self, *args):
        pass

    def info(self, *args):
        pass

    def debug(self, *args):
        pass

    def log(self, *args):
        pass


def capture_stdio(logger, **kwargs):
    """
    Log unhandled exceptions, close stdio, capture stdout and stderr.

    param logger: Logger object to use
    """
    # log uncaught exceptions
    sys.excepthook = lambda * exc_info: \
        logger.critical('UNCAUGHT EXCEPTION', exc_info=exc_info)

    # collect stdio file desc not in use for logging
    stdio_files = [sys.stdin, sys.stdout, sys.stderr]
    console_fds = [h.stream.fileno() for _junk, h in getattr(
        get_swift_logger, 'console_handler4logger', {}).items()]
    stdio_files = [f for f in stdio_files if f.fileno() not in console_fds]

    with open(os.devnull, 'r+b') as nullfile:
        # close stdio (excludes fds open for logging)
        for f in stdio_files:
            # some platforms throw an error when attempting an stdin flush
            try:
                f.flush()
            except IOError:
                pass

            try:
                os.dup2(nullfile.fileno(), f.fileno())
            except OSError:
                pass

    # redirect stdio
    if kwargs.pop('capture_stdout', True):
        sys.stdout = LoggerFileObject(logger)
    if kwargs.pop('capture_stderr', True):
        sys.stderr = LoggerFileObject(logger, 'STDERR')


class StrAnonymizer(str):
    """
    Class that permits to get a string anonymized or simply quoted.
    """

    def __new__(cls, data, method, salt):
        method = method.lower()
        if method not in hashlib.algorithms_guaranteed:
            raise ValueError('Unsupported hashing method: %r' % method)
        s = str.__new__(cls, data or '')
        s.method = method
        s.salt = salt
        return s

    @property
    def anonymized(self):
        if not self:
            return self
        else:
            if self.method == 'md5':
                h = md5(usedforsecurity=False)
            else:
                h = getattr(hashlib, self.method)()
            if self.salt:
                h.update(self.salt.encode('latin1'))
            h.update(self.encode('latin1'))
            return '{%s%s}%s' % ('S' if self.salt else '', self.method.upper(),
                                 h.hexdigest())


class StrFormatTime(object):
    """
    Class that permits to get formats or parts of a time.
    """

    def __init__(self, ts):
        self.time = ts
        self.time_struct = time.gmtime(ts)

    def __str__(self):
        return "%.9f" % self.time

    def __getattr__(self, attr):
        if attr not in ['a', 'A', 'b', 'B', 'c', 'd', 'H',
                        'I', 'j', 'm', 'M', 'p', 'S', 'U',
                        'w', 'W', 'x', 'X', 'y', 'Y', 'Z']:
            raise ValueError(("The attribute %s is not a correct directive "
                              "for time.strftime formater.") % attr)
        return datetime.datetime(*self.time_struct[:-2],
                                 tzinfo=UTC).strftime('%' + attr)

    @property
    def asctime(self):
        return time.asctime(self.time_struct)

    @property
    def datetime(self):
        return time.strftime('%d/%b/%Y/%H/%M/%S', self.time_struct)

    @property
    def iso8601(self):
        return time.strftime('%Y-%m-%dT%H:%M:%S', self.time_struct)

    @property
    def ms(self):
        return self.__str__().split('.')[1][:3]

    @property
    def us(self):
        return self.__str__().split('.')[1][:6]

    @property
    def ns(self):
        return self.__str__().split('.')[1]

    @property
    def s(self):
        return self.__str__().split('.')[0]


def get_log_line(req, res, trans_time, additional_info, fmt,
                 anonymization_method, anonymization_salt):
    """
    Make a line for logging that matches the documented log line format
    for backend servers.

    :param req: the request.
    :param res: the response.
    :param trans_time: the time the request took to complete, a float.
    :param additional_info: a string to log at the end of the line

    :returns: a properly formatted line for logging.
    """

    policy_index = get_policy_index(req.headers, res.headers)
    if req.path.startswith('/'):
        disk, partition, account, container, obj = split_path(req.path, 0, 5,
                                                              True)
    else:
        disk, partition, account, container, obj = (None, ) * 5
    replacements = {
        'remote_addr': StrAnonymizer(req.remote_addr, anonymization_method,
                                     anonymization_salt),
        'time': StrFormatTime(time.time()),
        'method': req.method,
        'path': StrAnonymizer(req.path, anonymization_method,
                              anonymization_salt),
        'disk': disk,
        'partition': partition,
        'account': StrAnonymizer(account, anonymization_method,
                                 anonymization_salt),
        'container': StrAnonymizer(container, anonymization_method,
                                   anonymization_salt),
        'object': StrAnonymizer(obj, anonymization_method,
                                anonymization_salt),
        'status': res.status.split()[0],
        'content_length': res.content_length,
        'referer': StrAnonymizer(req.referer, anonymization_method,
                                 anonymization_salt),
        'txn_id': req.headers.get('x-trans-id'),
        'user_agent': StrAnonymizer(req.user_agent, anonymization_method,
                                    anonymization_salt),
        'trans_time': trans_time,
        'additional_info': additional_info,
        'pid': os.getpid(),
        'policy_index': policy_index,
    }
    return LogStringFormatter(default='-').format(fmt, **replacements)


def get_policy_index(req_headers, res_headers):
    """
    Returns the appropriate index of the storage policy for the request from
    a proxy server

    :param req_headers: dict of the request headers.
    :param res_headers: dict of the response headers.

    :returns: string index of storage policy, or None
    """
    header = 'X-Backend-Storage-Policy-Index'
    policy_index = res_headers.get(header, req_headers.get(header))
    if isinstance(policy_index, bytes):
        policy_index = policy_index.decode('ascii')
    return str(policy_index) if policy_index is not None else None


class LogStringFormatter(string.Formatter):
    def __init__(self, default='', quote=False):
        super(LogStringFormatter, self).__init__()
        self.default = default
        self.quote = quote

    def format_field(self, value, spec):
        if not value:
            return self.default
        else:
            log = super(LogStringFormatter, self).format_field(value, spec)
            if self.quote:
                return quote(log, ':/{}')
            else:
                return log
