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

"""Miscellaneous utility functions for use with Swift."""

import errno
import fcntl
import os
import pwd
import sys
import time
import functools
from hashlib import md5
from random import random, shuffle
from urllib import quote
from contextlib import contextmanager, closing
import ctypes
import ctypes.util
from ConfigParser import ConfigParser, NoSectionError, NoOptionError, \
    RawConfigParser
from optparse import OptionParser
from tempfile import mkstemp, NamedTemporaryFile
try:
    import simplejson as json
except ImportError:
    import json
import cPickle as pickle
import glob
from urlparse import urlparse as stdlib_urlparse, ParseResult
import itertools

import eventlet
from eventlet import GreenPool, sleep, Timeout
from eventlet.green import socket, threading
import netifaces
import codecs
utf8_decoder = codecs.getdecoder('utf-8')
utf8_encoder = codecs.getencoder('utf-8')

from swift.common.exceptions import LockTimeout, MessageTimeout
from swift.common.http import is_success, is_redirection, HTTP_NOT_FOUND

# logging doesn't import patched as cleanly as one would like
from logging.handlers import SysLogHandler
import logging
logging.thread = eventlet.green.thread
logging.threading = eventlet.green.threading
logging._lock = logging.threading.RLock()
# setup notice level logging
NOTICE = 25
logging._levelNames[NOTICE] = 'NOTICE'
SysLogHandler.priority_map['NOTICE'] = 'notice'

# These are lazily pulled from libc elsewhere
_sys_fsync = None
_sys_fallocate = None
_posix_fadvise = None

# If set to non-zero, fallocate routines will fail based on free space
# available being at or below this amount, in bytes.
FALLOCATE_RESERVE = 0

# Used by hash_path to offer a bit more security when generating hashes for
# paths. It simply appends this value to all paths; guessing the hash a path
# will end up with would also require knowing this suffix.
hash_conf = ConfigParser()
HASH_PATH_SUFFIX = ''
if hash_conf.read('/etc/swift/swift.conf'):
    try:
        HASH_PATH_SUFFIX = hash_conf.get('swift-hash',
                                         'swift_hash_path_suffix')
    except (NoSectionError, NoOptionError):
        pass

# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))


def config_true_value(value):
    """
    Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    """
    return value is True or \
        (isinstance(value, basestring) and value.lower() in TRUE_VALUES)


def noop_libc_function(*args):
    return 0


def validate_configuration():
    if HASH_PATH_SUFFIX == '':
        sys.exit("Error: [swift-hash]: swift_hash_path_suffix missing "
                 "from /etc/swift/swift.conf")


def load_libc_function(func_name, log_error=True):
    """
    Attempt to find the function in libc, otherwise return a no-op func.

    :param func_name: name of the function to pull from libc.
    """
    try:
        libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
        return getattr(libc, func_name)
    except AttributeError:
        if log_error:
            logging.warn(_("Unable to locate %s in libc.  Leaving as a "
                         "no-op."), func_name)
        return noop_libc_function


def get_param(req, name, default=None):
    """
    Get parameters from an HTTP request ensuring proper handling UTF-8
    encoding.

    :param req: request object
    :param name: parameter name
    :param default: result to return if the parameter is not found
    :returns: HTTP request parameter value
    """
    value = req.params.get(name, default)
    if value and not isinstance(value, unicode):
        value.decode('utf8')    # Ensure UTF8ness
    return value


class FallocateWrapper(object):

    def __init__(self, noop=False):
        if noop:
            self.func_name = 'posix_fallocate'
            self.fallocate = noop_libc_function
            return
        ## fallocate is prefered because we need the on-disk size to match
        ## the allocated size. Older versions of sqlite require that the
        ## two sizes match. However, fallocate is Linux only.
        for func in ('fallocate', 'posix_fallocate'):
            self.func_name = func
            self.fallocate = load_libc_function(func, log_error=False)
            if self.fallocate is not noop_libc_function:
                break
        if self.fallocate is noop_libc_function:
            logging.warn(_("Unable to locate fallocate, posix_fallocate in "
                         "libc.  Leaving as a no-op."))

    def __call__(self, fd, mode, offset, length):
        """ The length parameter must be a ctypes.c_uint64 """
        if FALLOCATE_RESERVE > 0:
            st = os.fstatvfs(fd)
            free = st.f_frsize * st.f_bavail - length.value
            if free <= FALLOCATE_RESERVE:
                raise OSError('FALLOCATE_RESERVE fail %s <= %s' % (
                    free, FALLOCATE_RESERVE))
        args = {
            'fallocate': (fd, mode, offset, length),
            'posix_fallocate': (fd, offset, length)
        }
        return self.fallocate(*args[self.func_name])


def disable_fallocate():
    global _sys_fallocate
    _sys_fallocate = FallocateWrapper(noop=True)


def fallocate(fd, size):
    """
    Pre-allocate disk space for a file.

    :param fd: file descriptor
    :param size: size to allocate (in bytes)
    """
    global _sys_fallocate
    if _sys_fallocate is None:
        _sys_fallocate = FallocateWrapper()
    if size < 0:
        size = 0
    # 1 means "FALLOC_FL_KEEP_SIZE", which means it pre-allocates invisibly
    ret = _sys_fallocate(fd, 1, 0, ctypes.c_uint64(size))
    err = ctypes.get_errno()
    if ret and err not in (0, errno.ENOSYS, errno.EOPNOTSUPP,
                           errno.EINVAL):
        raise OSError(err, 'Unable to fallocate(%s)' % size)


class FsyncWrapper(object):

    def __init__(self):
        if hasattr(os, 'fdatasync'):
            self.func_name = 'fdatasync'
            self.fsync = os.fdatasync
            self.fcntl_flag = None
        elif hasattr(fcntl, 'F_FULLFSYNC'):
            self.func_name = 'fcntl'
            self.fsync = fcntl.fcntl
            self.fcntl_flag = fcntl.F_FULLFSYNC
        else:
            self.func_name = 'fsync'
            self.fsync = os.fsync
            self.fcntl_flag = None

    def __call__(self, fd):
        args = {
            'fdatasync': (fd, ),
            'fsync': (fd, ),
            'fcntl': (fd, self.fcntl_flag)
        }
        return self.fsync(*args[self.func_name])


def fsync(fd):
    """
    Write buffered changes to disk.

    :param fd: file descriptor
    """

    global _sys_fsync
    if _sys_fsync is None:
        _sys_fsync = FsyncWrapper()

    ret = _sys_fsync(fd)
    err = ctypes.get_errno()
    if ret and err != 0:
        raise OSError(err, 'Unable to fsync(%s)' % fd)


def drop_buffer_cache(fd, offset, length):
    """
    Drop 'buffer' cache for the given range of the given file.

    :param fd: file descriptor
    :param offset: start offset
    :param length: length
    """
    global _posix_fadvise
    if _posix_fadvise is None:
        _posix_fadvise = load_libc_function('posix_fadvise64')
    # 4 means "POSIX_FADV_DONTNEED"
    ret = _posix_fadvise(fd, ctypes.c_uint64(offset),
                         ctypes.c_uint64(length), 4)
    if ret != 0:
        logging.warn("posix_fadvise64(%s, %s, %s, 4) -> %s"
                     % (fd, offset, length, ret))


def normalize_timestamp(timestamp):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx.xxxxx format.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    return "%016.05f" % (float(timestamp))


def mkdirs(path):
    """
    Ensures the path is a directory or makes it if not. Errors if the path
    exists but is a file or on permissions failure.

    :param path: path to create
    """
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError, err:
            if err.errno != errno.EEXIST or not os.path.isdir(path):
                raise


def renamer(old, new):
    """
    Attempt to fix / hide race conditions like empty object directories
    being removed by backend processes during uploads, by retrying.

    :param old: old path to be renamed
    :param new: new path to be renamed to
    """
    try:
        mkdirs(os.path.dirname(new))
        os.rename(old, new)
    except OSError:
        mkdirs(os.path.dirname(new))
        os.rename(old, new)


def split_path(path, minsegs=1, maxsegs=None, rest_with_last=False):
    """
    Validate and split the given HTTP request path.

    **Examples**::

        ['a'] = split_path('/a')
        ['a', None] = split_path('/a', 1, 2)
        ['a', 'c'] = split_path('/a/c', 1, 2)
        ['a', 'c', 'o/r'] = split_path('/a/c/o/r', 1, 3, True)

    :param path: HTTP Request path to be split
    :param minsegs: Minimum number of segments to be extracted
    :param maxsegs: Maximum number of segments to be extracted
    :param rest_with_last: If True, trailing data will be returned as part
                           of last segment.  If False, and there is
                           trailing data, raises ValueError.
    :returns: list of segments with a length of maxsegs (non-existant
              segments will return as None)
    :raises: ValueError if given an invalid path
    """
    if not maxsegs:
        maxsegs = minsegs
    if minsegs > maxsegs:
        raise ValueError('minsegs > maxsegs: %d > %d' % (minsegs, maxsegs))
    if rest_with_last:
        segs = path.split('/', maxsegs)
        minsegs += 1
        maxsegs += 1
        count = len(segs)
        if (segs[0] or count < minsegs or count > maxsegs or
                '' in segs[1:minsegs]):
            raise ValueError('Invalid path: %s' % quote(path))
    else:
        minsegs += 1
        maxsegs += 1
        segs = path.split('/', maxsegs)
        count = len(segs)
        if (segs[0] or count < minsegs or count > maxsegs + 1 or
                '' in segs[1:minsegs] or
                (count == maxsegs + 1 and segs[maxsegs])):
            raise ValueError('Invalid path: %s' % quote(path))
    segs = segs[1:maxsegs]
    segs.extend([None] * (maxsegs - 1 - len(segs)))
    return segs


def validate_device_partition(device, partition):
    """
    Validate that a device and a partition are valid and won't lead to
    directory traversal when used.

    :param device: device to validate
    :param partition: partition to validate
    :raises: ValueError if given an invalid device or partition
    """
    invalid_device = False
    invalid_partition = False
    if not device or '/' in device or device in ['.', '..']:
        invalid_device = True
    if not partition or '/' in partition or partition in ['.', '..']:
        invalid_partition = True

    if invalid_device:
        raise ValueError('Invalid device: %s' % quote(device or ''))
    elif invalid_partition:
        raise ValueError('Invalid partition: %s' % quote(partition or ''))


class NullLogger():
    """A no-op logger for eventlet wsgi."""

    def write(self, *args):
        #"Logs" the args to nowhere
        pass


class LoggerFileObject(object):

    def __init__(self, logger):
        self.logger = logger

    def write(self, value):
        value = value.strip()
        if value:
            if 'Connection reset by peer' in value:
                self.logger.error(_('STDOUT: Connection reset by peer'))
            else:
                self.logger.error(_('STDOUT: %s'), value)

    def writelines(self, values):
        self.logger.error(_('STDOUT: %s'), '#012'.join(values))

    def close(self):
        pass

    def flush(self):
        pass

    def __iter__(self):
        return self

    def next(self):
        raise IOError(errno.EBADF, 'Bad file descriptor')

    def read(self, size=-1):
        raise IOError(errno.EBADF, 'Bad file descriptor')

    def readline(self, size=-1):
        raise IOError(errno.EBADF, 'Bad file descriptor')

    def tell(self):
        return 0

    def xreadlines(self):
        return self


class StatsdClient(object):
    def __init__(self, host, port, base_prefix='', tail_prefix='',
                 default_sample_rate=1):
        self._host = host
        self._port = port
        self._base_prefix = base_prefix
        self.set_prefix(tail_prefix)
        self._default_sample_rate = default_sample_rate
        self._target = (self._host, self._port)
        self.random = random

    def set_prefix(self, new_prefix):
        if new_prefix and self._base_prefix:
            self._prefix = '.'.join([self._base_prefix, new_prefix, ''])
        elif new_prefix:
            self._prefix = new_prefix + '.'
        elif self._base_prefix:
            self._prefix = self._base_prefix + '.'
        else:
            self._prefix = ''

    def _send(self, m_name, m_value, m_type, sample_rate):
        if sample_rate is None:
            sample_rate = self._default_sample_rate
        parts = ['%s%s:%s' % (self._prefix, m_name, m_value), m_type]
        if sample_rate < 1:
            if self.random() < sample_rate:
                parts.append('@%s' % (sample_rate,))
            else:
                return
        # Ideally, we'd cache a sending socket in self, but that
        # results in a socket getting shared by multiple green threads.
        with closing(self._open_socket()) as sock:
            return sock.sendto('|'.join(parts), self._target)

    def _open_socket(self):
        return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def update_stats(self, m_name, m_value, sample_rate=None):
        return self._send(m_name, m_value, 'c', sample_rate)

    def increment(self, metric, sample_rate=None):
        return self.update_stats(metric, 1, sample_rate)

    def decrement(self, metric, sample_rate=None):
        return self.update_stats(metric, -1, sample_rate)

    def timing(self, metric, timing_ms, sample_rate=None):
        return self._send(metric, timing_ms, 'ms', sample_rate)

    def timing_since(self, metric, orig_time, sample_rate=None):
        return self.timing(metric, (time.time() - orig_time) * 1000,
                           sample_rate)


def timing_stats(func):
    """
    Decorator that logs timing events or errors for public methods in swift's
    wsgi server controllers, based on response code.
    """
    method = func.func_name

    @functools.wraps(func)
    def _timing_stats(ctrl, *args, **kwargs):
        start_time = time.time()
        resp = func(ctrl, *args, **kwargs)
        if is_success(resp.status_int) or is_redirection(resp.status_int) or \
                resp.status_int == HTTP_NOT_FOUND:
            ctrl.logger.timing_since(method + '.timing', start_time)
        else:
            ctrl.logger.timing_since(method + '.errors.timing', start_time)
        return resp

    return _timing_stats


# double inheritance to support property with setter
class LogAdapter(logging.LoggerAdapter, object):
    """
    A Logger like object which performs some reformatting on calls to
    :meth:`exception`.  Can be used to store a threadlocal transaction id and
    client ip.
    """

    _cls_thread_local = threading.local()

    def __init__(self, logger, server):
        logging.LoggerAdapter.__init__(self, logger, {})
        self.server = server
        setattr(self, 'warn', self.warning)

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

    def getEffectiveLevel(self):
        return self.logger.getEffectiveLevel()

    def process(self, msg, kwargs):
        """
        Add extra info to message
        """
        kwargs['extra'] = {'server': self.server, 'txn_id': self.txn_id,
                           'client_ip': self.client_ip}
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
        logging.LoggerAdapter.exception(self, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        _junk, exc, _junk = sys.exc_info()
        call = self.error
        emsg = ''
        if isinstance(exc, OSError):
            if exc.errno in (errno.EIO, errno.ENOSPC):
                emsg = str(exc)
            else:
                call = self._exception
        elif isinstance(exc, socket.error):
            if exc.errno == errno.ECONNREFUSED:
                emsg = _('Connection refused')
            elif exc.errno == errno.EHOSTUNREACH:
                emsg = _('Host unreachable')
            elif exc.errno == errno.ETIMEDOUT:
                emsg = _('Connection timeout')
            else:
                call = self._exception
        elif isinstance(exc, eventlet.Timeout):
            emsg = exc.__class__.__name__
            if hasattr(exc, 'seconds'):
                emsg += ' (%ss)' % exc.seconds
            if isinstance(exc, MessageTimeout):
                if exc.msg:
                    emsg += ' %s' % exc.msg
        else:
            call = self._exception
        call('%s: %s' % (msg, emsg), *args, **kwargs)

    def set_statsd_prefix(self, prefix):
        """
        The StatsD client prefix defaults to the "name" of the logger.  This
        method may override that default with a specific value.  Currently used
        in the proxy-server to differentiate the Account, Container, and Object
        controllers.
        """
        if self.logger.statsd_client:
            self.logger.statsd_client.set_prefix(prefix)

    def statsd_delegate(statsd_func_name):
        """
        Factory which creates methods which delegate to methods on
        self.logger.statsd_client (an instance of StatsdClient).  The
        created methods conditionally delegate to a method whose name is given
        in 'statsd_func_name'.  The created delegate methods are a no-op when
        StatsD logging is not configured.  The created delegate methods also
        handle the defaulting of sample_rate (to either the default specified
        in the config with 'log_statsd_default_sample_rate' or the value passed
        into delegate function).

        :param statsd_func_name: the name of a method on StatsdClient.
        """

        func = getattr(StatsdClient, statsd_func_name)

        @functools.wraps(func)
        def wrapped(self, *a, **kw):
            if getattr(self.logger, 'statsd_client'):
                return func(self.logger.statsd_client, *a, **kw)
        return wrapped

    update_stats = statsd_delegate('update_stats')
    increment = statsd_delegate('increment')
    decrement = statsd_delegate('decrement')
    timing = statsd_delegate('timing')
    timing_since = statsd_delegate('timing_since')


class SwiftLogFormatter(logging.Formatter):
    """
    Custom logging.Formatter will append txn_id to a log message if the record
    has one and the message does not.
    """

    def format(self, record):
        if not hasattr(record, 'server'):
            # Catch log messages that were not initiated by swift
            # (for example, the keystone auth middleware)
            record.server = record.name
        msg = logging.Formatter.format(self, record)
        if (hasattr(record, 'txn_id') and record.txn_id and
                record.levelno != logging.INFO and
                record.txn_id not in msg):
            msg = "%s (txn: %s)" % (msg, record.txn_id)
        if (hasattr(record, 'client_ip') and record.client_ip and
                record.levelno != logging.INFO and
                record.client_ip not in msg):
            msg = "%s (client_ip: %s)" % (msg, record.client_ip)
        return msg


def get_logger(conf, name=None, log_to_console=False, log_route=None,
               fmt="%(server)s %(message)s"):
    """
    Get the current system logger using config settings.

    **Log config and defaults**::

        log_facility = LOG_LOCAL0
        log_level = INFO
        log_name = swift
        log_udp_host = (disabled)
        log_udp_port = logging.handlers.SYSLOG_UDP_PORT
        log_address = /dev/log
        log_statsd_host = (disabled)
        log_statsd_port = 8125
        log_statsd_default_sample_rate = 1
        log_statsd_metric_prefix = (empty-string)

    :param conf: Configuration dict to read settings from
    :param name: Name of the logger
    :param log_to_console: Add handler which writes to console on stderr
    :param log_route: Route for the logging, not emitted to the log, just used
                      to separate logging configurations
    :param fmt: Override log format
    """
    if not conf:
        conf = {}
    if name is None:
        name = conf.get('log_name', 'swift')
    if not log_route:
        log_route = name
    logger = logging.getLogger(log_route)
    logger.propagate = False
    # all new handlers will get the same formatter
    formatter = SwiftLogFormatter(fmt)

    # get_logger will only ever add one SysLog Handler to a logger
    if not hasattr(get_logger, 'handler4logger'):
        get_logger.handler4logger = {}
    if logger in get_logger.handler4logger:
        logger.removeHandler(get_logger.handler4logger[logger])

    # facility for this logger will be set by last call wins
    facility = getattr(SysLogHandler, conf.get('log_facility', 'LOG_LOCAL0'),
                       SysLogHandler.LOG_LOCAL0)
    udp_host = conf.get('log_udp_host')
    if udp_host:
        udp_port = conf.get('log_udp_port', logging.handlers.SYSLOG_UDP_PORT)
        handler = SysLogHandler(address=(udp_host, udp_port),
                                facility=facility)
    else:
        log_address = conf.get('log_address', '/dev/log')
        try:
            handler = SysLogHandler(address=log_address, facility=facility)
        except socket.error, e:
            # Either /dev/log isn't a UNIX socket or it does not exist at all
            if e.errno not in [errno.ENOTSOCK, errno.ENOENT]:
                raise e
            handler = SysLogHandler(facility=facility)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    get_logger.handler4logger[logger] = handler

    # setup console logging
    if log_to_console or hasattr(get_logger, 'console_handler4logger'):
        # remove pre-existing console handler for this logger
        if not hasattr(get_logger, 'console_handler4logger'):
            get_logger.console_handler4logger = {}
        if logger in get_logger.console_handler4logger:
            logger.removeHandler(get_logger.console_handler4logger[logger])

        console_handler = logging.StreamHandler(sys.__stderr__)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        get_logger.console_handler4logger[logger] = console_handler

    # set the level for the logger
    logger.setLevel(
        getattr(logging, conf.get('log_level', 'INFO').upper(), logging.INFO))

    # Setup logger with a StatsD client if so configured
    statsd_host = conf.get('log_statsd_host')
    if statsd_host:
        statsd_port = int(conf.get('log_statsd_port', 8125))
        base_prefix = conf.get('log_statsd_metric_prefix', '')
        default_sample_rate = float(conf.get(
            'log_statsd_default_sample_rate', 1))
        statsd_client = StatsdClient(statsd_host, statsd_port, base_prefix,
                                     name, default_sample_rate)
        logger.statsd_client = statsd_client
    else:
        logger.statsd_client = None

    adapted_logger = LogAdapter(logger, name)
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
                print >>sys.stderr, 'Error calling custom handler [%s]' % hook
            except ValueError:
                print >>sys.stderr, 'Invalid custom handler format [%s]' % hook
    return adapted_logger


def get_hub():
    """
    Checks whether poll is available and falls back
    on select if it isn't.

    Note about epoll:

    Review: https://review.openstack.org/#/c/18806/

    There was a problem where once out of every 30 quadrillion
    connections, a coroutine wouldn't wake up when the client
    closed its end. Epoll was not reporting the event or it was
    getting swallowed somewhere. Then when that file descriptor
    was re-used, eventlet would freak right out because it still
    thought it was waiting for activity from it in some other coro.
    """
    try:
        import select
        if hasattr(select, "poll"):
            return "poll"
        return "selects"
    except ImportError:
        return None


def drop_privileges(user):
    """
    Sets the userid/groupid of the current process, get session leader, etc.

    :param user: User name to change privileges to
    """
    user = pwd.getpwnam(user)
    if os.geteuid() == 0:
        os.setgroups([])
    os.setgid(user[3])
    os.setuid(user[2])
    os.environ['HOME'] = user[5]
    try:
        os.setsid()
    except OSError:
        pass
    os.chdir('/')  # in case you need to rmdir on where you started the daemon
    os.umask(022)  # ensure files are created with the correct privileges


def capture_stdio(logger, **kwargs):
    """
    Log unhandled exceptions, close stdio, capture stdout and stderr.

    param logger: Logger object to use
    """
    # log uncaught exceptions
    sys.excepthook = lambda * exc_info: \
        logger.critical(_('UNCAUGHT EXCEPTION'), exc_info=exc_info)

    # collect stdio file desc not in use for logging
    stdio_files = [sys.stdin, sys.stdout, sys.stderr]
    console_fds = [h.stream.fileno() for _junk, h in getattr(
        get_logger, 'console_handler4logger', {}).items()]
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
        sys.stderr = LoggerFileObject(logger)


def parse_options(parser=None, once=False, test_args=None):
    """
    Parse standard swift server/daemon options with optparse.OptionParser.

    :param parser: OptionParser to use. If not sent one will be created.
    :param once: Boolean indicating the "once" option is available
    :param test_args: Override sys.argv; used in testing

    :returns : Tuple of (config, options); config is an absolute path to the
               config file, options is the parser options as a dictionary.

    :raises SystemExit: First arg (CONFIG) is required, file must exist
    """
    if not parser:
        parser = OptionParser(usage="%prog CONFIG [options]")
    parser.add_option("-v", "--verbose", default=False, action="store_true",
                      help="log to console")
    if once:
        parser.add_option("-o", "--once", default=False, action="store_true",
                          help="only run one pass of daemon")

    # if test_args is None, optparse will use sys.argv[:1]
    options, args = parser.parse_args(args=test_args)

    if not args:
        parser.print_usage()
        print _("Error: missing config file argument")
        sys.exit(1)
    config = os.path.abspath(args.pop(0))
    if not os.path.exists(config):
        parser.print_usage()
        print _("Error: unable to locate %s") % config
        sys.exit(1)

    extra_args = []
    # if any named options appear in remaining args, set the option to True
    for arg in args:
        if arg in options.__dict__:
            setattr(options, arg, True)
        else:
            extra_args.append(arg)

    options = vars(options)
    if extra_args:
        options['extra_args'] = extra_args
    return config, options


def whataremyips():
    """
    Get the machine's ip addresses

    :returns: list of Strings of ip addresses
    """
    addresses = []
    for interface in netifaces.interfaces():
        try:
            iface_data = netifaces.ifaddresses(interface)
            for family in iface_data:
                if family not in (netifaces.AF_INET, netifaces.AF_INET6):
                    continue
                for address in iface_data[family]:
                    addresses.append(address['addr'])
        except ValueError:
            pass
    return addresses


def storage_directory(datadir, partition, hash):
    """
    Get the storage directory

    :param datadir: Base data directory
    :param partition: Partition
    :param hash: Account, container or object hash
    :returns: Storage directory
    """
    return os.path.join(datadir, str(partition), hash[-3:], hash)


def hash_path(account, container=None, object=None, raw_digest=False):
    """
    Get the connonical hash for an account/container/object

    :param account: Account
    :param container: Container
    :param object: Object
    :param raw_digest: If True, return the raw version rather than a hex digest
    :returns: hash string
    """
    if object and not container:
        raise ValueError('container is required if object is provided')
    paths = [account]
    if container:
        paths.append(container)
    if object:
        paths.append(object)
    if raw_digest:
        return md5('/' + '/'.join(paths) + HASH_PATH_SUFFIX).digest()
    else:
        return md5('/' + '/'.join(paths) + HASH_PATH_SUFFIX).hexdigest()


@contextmanager
def lock_path(directory, timeout=10):
    """
    Context manager that acquires a lock on a directory.  This will block until
    the lock can be acquired, or the timeout time has expired (whichever occurs
    first).

    For locking exclusively, file or directory has to be opened in Write mode.
    Python doesn't allow directories to be opened in Write Mode. So we
    workaround by locking a hidden file in the directory.

    :param directory: directory to be locked
    :param timeout: timeout (in seconds)
    """
    mkdirs(directory)
    lockpath = '%s/.lock' % directory
    fd = os.open(lockpath, os.O_WRONLY | os.O_CREAT)
    try:
        with LockTimeout(timeout, lockpath):
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except IOError, err:
                    if err.errno != errno.EAGAIN:
                        raise
                sleep(0.01)
        yield True
    finally:
        os.close(fd)


@contextmanager
def lock_file(filename, timeout=10, append=False, unlink=True):
    """
    Context manager that acquires a lock on a file.  This will block until
    the lock can be acquired, or the timeout time has expired (whichever occurs
    first).

    :param filename: file to be locked
    :param timeout: timeout (in seconds)
    :param append: True if file should be opened in append mode
    :param unlink: True if the file should be unlinked at the end
    """
    flags = os.O_CREAT | os.O_RDWR
    if append:
        flags |= os.O_APPEND
        mode = 'a+'
    else:
        mode = 'r+'
    fd = os.open(filename, flags)
    file_obj = os.fdopen(fd, mode)
    try:
        with LockTimeout(timeout, filename):
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except IOError, err:
                    if err.errno != errno.EAGAIN:
                        raise
                sleep(0.01)
        yield file_obj
    finally:
        try:
            file_obj.close()
        except UnboundLocalError:
            pass  # may have not actually opened the file
        if unlink:
            os.unlink(filename)


def lock_parent_directory(filename, timeout=10):
    """
    Context manager that acquires a lock on the parent directory of the given
    file path.  This will block until the lock can be acquired, or the timeout
    time has expired (whichever occurs first).

    :param filename: file path of the parent directory to be locked
    :param timeout: timeout (in seconds)
    """
    return lock_path(os.path.dirname(filename), timeout=timeout)


def get_time_units(time_amount):
    """
    Get a nomralized length of time in the largest unit of time (hours,
    minutes, or seconds.)

    :param time_amount: length of time in seconds
    :returns: A touple of (length of time, unit of time) where unit of time is
              one of ('h', 'm', 's')
    """
    time_unit = 's'
    if time_amount > 60:
        time_amount /= 60
        time_unit = 'm'
        if time_amount > 60:
            time_amount /= 60
            time_unit = 'h'
    return time_amount, time_unit


def compute_eta(start_time, current_value, final_value):
    """
    Compute an ETA.  Now only if we could also have a progress bar...

    :param start_time: Unix timestamp when the operation began
    :param current_value: Current value
    :param final_value: Final value
    :returns: ETA as a tuple of (length of time, unit of time) where unit of
              time is one of ('h', 'm', 's')
    """
    elapsed = time.time() - start_time
    completion = (float(current_value) / final_value) or 0.00001
    return get_time_units(1.0 / completion * elapsed - elapsed)


def iter_devices_partitions(devices_dir, item_type):
    """
    Iterate over partitions accross all devices.

    :param devices_dir: Path to devices
    :param item_type: One of 'accounts', 'containers', or 'objects'
    :returns: Each iteration returns a tuple of (device, partition)
    """
    devices = listdir(devices_dir)
    shuffle(devices)
    devices_partitions = []
    for device in devices:
        partitions = listdir(os.path.join(devices_dir, device, item_type))
        shuffle(partitions)
        devices_partitions.append((device, iter(partitions)))
    yielded = True
    while yielded:
        yielded = False
        for device, partitions in devices_partitions:
            try:
                yield device, partitions.next()
                yielded = True
            except StopIteration:
                pass


def unlink_older_than(path, mtime):
    """
    Remove any file in a given path that that was last modified before mtime.

    :param path: path to remove file from
    :mtime: timestamp of oldest file to keep
    """
    if os.path.exists(path):
        for fname in listdir(path):
            fpath = os.path.join(path, fname)
            try:
                if os.path.getmtime(fpath) < mtime:
                    os.unlink(fpath)
            except OSError:
                pass


def item_from_env(env, item_name):
    """
    Get a value from the wsgi environment

    :param env: wsgi environment dict
    :param item_name: name of item to get

    :returns: the value from the environment
    """
    item = env.get(item_name, None)
    if item is None:
        logging.error("ERROR: %s could not be found in env!" % item_name)
    return item


def cache_from_env(env):
    """
    Get memcache connection pool from the environment (which had been
    previously set by the memcache middleware

    :param env: wsgi environment dict

    :returns: swift.common.memcached.MemcacheRing from environment
    """
    return item_from_env(env, 'swift.cache')


def readconf(conffile, section_name=None, log_name=None, defaults=None,
             raw=False):
    """
    Read config file and return config items as a dict

    :param conffile: path to config file, or a file-like object (hasattr
                     readline)
    :param section_name: config section to read (will return all sections if
                     not defined)
    :param log_name: name to be used with logging (will use section_name if
                     not defined)
    :param defaults: dict of default values to pre-populate the config with
    :returns: dict of config items
    """
    if defaults is None:
        defaults = {}
    if raw:
        c = RawConfigParser(defaults)
    else:
        c = ConfigParser(defaults)
    if hasattr(conffile, 'readline'):
        c.readfp(conffile)
    else:
        if not c.read(conffile):
            print _("Unable to read config file %s") % conffile
            sys.exit(1)
    if section_name:
        if c.has_section(section_name):
            conf = dict(c.items(section_name))
        else:
            print _("Unable to find %s config section in %s") % \
                (section_name, conffile)
            sys.exit(1)
        if "log_name" not in conf:
            if log_name is not None:
                conf['log_name'] = log_name
            else:
                conf['log_name'] = section_name
    else:
        conf = {}
        for s in c.sections():
            conf.update({s: dict(c.items(s))})
        if 'log_name' not in conf:
            conf['log_name'] = log_name
    conf['__file__'] = conffile
    return conf


def write_pickle(obj, dest, tmp=None, pickle_protocol=0):
    """
    Ensure that a pickle file gets written to disk.  The file
    is first written to a tmp location, ensure it is synced to disk, then
    perform a move to its final location

    :param obj: python object to be pickled
    :param dest: path of final destination file
    :param tmp: path to tmp to use, defaults to None
    :param pickle_protocol: protocol to pickle the obj with, defaults to 0
    """
    if tmp is None:
        tmp = os.path.dirname(dest)
    fd, tmppath = mkstemp(dir=tmp, suffix='.tmp')
    with os.fdopen(fd, 'wb') as fo:
        pickle.dump(obj, fo, pickle_protocol)
        fo.flush()
        os.fsync(fd)
        renamer(tmppath, dest)


def search_tree(root, glob_match, ext):
    """Look in root, for any files/dirs matching glob, recurively traversing
    any found directories looking for files ending with ext

    :param root: start of search path
    :param glob_match: glob to match in root, matching dirs are traversed with
                       os.walk
    :param ext: only files that end in ext will be returned

    :returns: list of full paths to matching files, sorted

    """
    found_files = []
    for path in glob.glob(os.path.join(root, glob_match)):
        if path.endswith(ext):
            found_files.append(path)
        else:
            for root, dirs, files in os.walk(path):
                for file in files:
                    if file.endswith(ext):
                        found_files.append(os.path.join(root, file))
    return sorted(found_files)


def write_file(path, contents):
    """Write contents to file at path

    :param path: any path, subdirs will be created as needed
    :param contents: data to write to file, will be converted to string

    """
    dirname, name = os.path.split(path)
    if not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
        except OSError, err:
            if err.errno == errno.EACCES:
                sys.exit('Unable to create %s.  Running as '
                         'non-root?' % dirname)
    with open(path, 'w') as f:
        f.write('%s' % contents)


def remove_file(path):
    """Quiet wrapper for os.unlink, OSErrors are suppressed

    :param path: first and only argument passed to os.unlink
    """
    try:
        os.unlink(path)
    except OSError:
        pass


def audit_location_generator(devices, datadir, mount_check=True, logger=None):
    '''
    Given a devices path and a data directory, yield (path, device,
    partition) for all files in that directory

    :param devices: parent directory of the devices to be audited
    :param datadir: a directory located under self.devices. This should be
                    one of the DATADIR constants defined in the account,
                    container, and object servers.
    :param mount_check: Flag to check if a mount check should be performed
                    on devices
    :param logger: a logger object
    '''
    device_dir = listdir(devices)
    # randomize devices in case of process restart before sweep completed
    shuffle(device_dir)
    for device in device_dir:
        if mount_check and not \
                os.path.ismount(os.path.join(devices, device)):
            if logger:
                logger.debug(
                    _('Skipping %s as it is not mounted'), device)
            continue
        datadir_path = os.path.join(devices, device, datadir)
        if not os.path.exists(datadir_path):
            continue
        partitions = listdir(datadir_path)
        for partition in partitions:
            part_path = os.path.join(datadir_path, partition)
            if not os.path.isdir(part_path):
                continue
            suffixes = listdir(part_path)
            for suffix in suffixes:
                suff_path = os.path.join(part_path, suffix)
                if not os.path.isdir(suff_path):
                    continue
                hashes = listdir(suff_path)
                for hsh in hashes:
                    hash_path = os.path.join(suff_path, hsh)
                    if not os.path.isdir(hash_path):
                        continue
                    for fname in sorted(listdir(hash_path),
                                        reverse=True):
                        path = os.path.join(hash_path, fname)
                        yield path, device, partition


def ratelimit_sleep(running_time, max_rate, incr_by=1, rate_buffer=5):
    '''
    Will eventlet.sleep() for the appropriate time so that the max_rate
    is never exceeded.  If max_rate is 0, will not ratelimit.  The
    maximum recommended rate should not exceed (1000 * incr_by) a second
    as eventlet.sleep() does involve some overhead.  Returns running_time
    that should be used for subsequent calls.

    :param running_time: the running time of the next allowable request. Best
                         to start at zero.
    :param max_rate: The maximum rate per second allowed for the process.
    :param incr_by: How much to increment the counter.  Useful if you want
                    to ratelimit 1024 bytes/sec and have differing sizes
                    of requests. Must be >= 0.
    :param rate_buffer: Number of seconds the rate counter can drop and be
                        allowed to catch up (at a faster than listed rate).
                        A larger number will result in larger spikes in rate
                        but better average accuracy.
    '''
    if not max_rate or incr_by <= 0:
        return running_time
    clock_accuracy = 1000.0
    now = time.time() * clock_accuracy
    time_per_request = clock_accuracy * (float(incr_by) / max_rate)
    if now - running_time > rate_buffer * clock_accuracy:
        running_time = now
    elif running_time - now > time_per_request:
        eventlet.sleep((running_time - now) / clock_accuracy)
    return running_time + time_per_request


class ContextPool(GreenPool):
    "GreenPool subclassed to kill its coros when it gets gc'ed"

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for coro in list(self.coroutines_running):
            coro.kill()


class ModifiedParseResult(ParseResult):
    "Parse results class for urlparse."

    @property
    def hostname(self):
        netloc = self.netloc.split('@', 1)[-1]
        if netloc.startswith('['):
            return netloc[1:].split(']')[0]
        elif ':' in netloc:
            return netloc.rsplit(':')[0]
        return netloc

    @property
    def port(self):
        netloc = self.netloc.split('@', 1)[-1]
        if netloc.startswith('['):
            netloc = netloc.rsplit(']')[1]
        if ':' in netloc:
            return int(netloc.rsplit(':')[1])
        return None


def urlparse(url):
    """
    urlparse augmentation.
    This is necessary because urlparse can't handle RFC 2732 URLs.

    :param url: URL to parse.
    """
    return ModifiedParseResult(*stdlib_urlparse(url))


def validate_sync_to(value, allowed_sync_hosts):
    if not value:
        return None
    p = urlparse(value)
    if p.scheme not in ('http', 'https'):
        return _('Invalid scheme %r in X-Container-Sync-To, must be "http" '
                 'or "https".') % p.scheme
    if not p.path:
        return _('Path required in X-Container-Sync-To')
    if p.params or p.query or p.fragment:
        return _('Params, queries, and fragments not allowed in '
                 'X-Container-Sync-To')
    if p.hostname not in allowed_sync_hosts:
        return _('Invalid host %r in X-Container-Sync-To') % p.hostname
    return None


def get_remote_client(req):
    # remote host for zeus
    client = req.headers.get('x-cluster-client-ip')
    if not client and 'x-forwarded-for' in req.headers:
        # remote host for other lbs
        client = req.headers['x-forwarded-for'].split(',')[0].strip()
    if not client:
        client = req.remote_addr
    return client


def human_readable(value):
    """
    Returns the number in a human readable format; for example 1048576 = "1Mi".
    """
    value = float(value)
    index = -1
    suffixes = 'KMGTPEZY'
    while value >= 1024 and index + 1 < len(suffixes):
        index += 1
        value = round(value / 1024)
    if index == -1:
        return '%d' % value
    return '%d%si' % (round(value), suffixes[index])


def dump_recon_cache(cache_dict, cache_file, logger, lock_timeout=2):
    """Update recon cache values

    :param cache_dict: Dictionary of cache key/value pairs to write out
    :param cache_file: cache file to update
    :param logger: the logger to use to log an encountered error
    :param lock_timeout: timeout (in seconds)
    """
    try:
        with lock_file(cache_file, lock_timeout, unlink=False) as cf:
            cache_entry = {}
            try:
                existing_entry = cf.readline()
                if existing_entry:
                    cache_entry = json.loads(existing_entry)
            except ValueError:
                #file doesn't have a valid entry, we'll recreate it
                pass
            for cache_key, cache_value in cache_dict.items():
                cache_entry[cache_key] = cache_value
            try:
                with NamedTemporaryFile(dir=os.path.dirname(cache_file),
                                        delete=False) as tf:
                    tf.write(json.dumps(cache_entry) + '\n')
                os.rename(tf.name, cache_file)
            finally:
                try:
                    os.unlink(tf.name)
                except OSError, err:
                    if err.errno != errno.ENOENT:
                        raise
    except (Exception, Timeout):
        logger.exception(_('Exception dumping recon cache'))


def listdir(path):
    try:
        return os.listdir(path)
    except OSError, err:
        if err.errno != errno.ENOENT:
            raise
    return []


def streq_const_time(s1, s2):
    """Constant-time string comparison.

    :params s1: the first string
    :params s2: the second string

    :return: True if the strings are equal.

    This function takes two strings and compares them.  It is intended to be
    used when doing a comparison for authentication purposes to help guard
    against timing attacks.
    """
    if len(s1) != len(s2):
        return False
    result = 0
    for (a, b) in zip(s1, s2):
        result |= ord(a) ^ ord(b)
    return result == 0


def public(func):
    """
    Decorator to declare which methods are publicly accessible as HTTP
    requests

    :param func: function to make public
    """
    func.publicly_accessible = True

    @functools.wraps(func)
    def wrapped(*a, **kw):
        return func(*a, **kw)
    return wrapped


def rsync_ip(ip):
    """
    Transform ip string to an rsync-compatible form

    Will return ipv4 addresses unchanged, but will nest ipv6 addresses
    inside square brackets.

    :param ip: an ip string (ipv4 or ipv6)

    :returns: a string ip address
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except socket.error:  # it's IPv4
        return ip
    else:
        return '[%s]' % ip


def get_valid_utf8_str(str_or_unicode):
    """
    Get valid parts of utf-8 str from str, unicode and even invalid utf-8 str

    :param str_or_unicode: a string or an unicode which can be invalid utf-8
    """
    if isinstance(str_or_unicode, unicode):
        (str_or_unicode, _len) = utf8_encoder(str_or_unicode, 'replace')
    (valid_utf8_str, _len) = utf8_decoder(str_or_unicode, 'replace')
    return valid_utf8_str.encode('utf-8')


def list_from_csv(comma_separated_str):
    """
    Splits the str given and returns a properly stripped list of the comma
    separated values.
    """
    if comma_separated_str:
        return [v.strip() for v in comma_separated_str.split(',') if v.strip()]
    return []


def csv_append(csv_string, item):
    """
    Appends an item to a comma-separated string.

    If the comma-separated string is empty/None, just returns item.
    """
    if csv_string:
        return ",".join((csv_string, item))
    else:
        return item


def reiterate(iterable):
    """
    Consume the first item from an iterator, then re-chain it to the rest of
    the iterator.  This is useful when you want to make sure the prologue to
    downstream generators have been executed before continuing.

    :param iterable: an iterable object
    """
    if isinstance(iterable, (list, tuple)):
        return iterable
    else:
        iterator = iter(iterable)
        try:
            chunk = ''
            while not chunk:
                chunk = next(iterator)
            return itertools.chain([chunk], iterator)
        except StopIteration:
            return []


class InputProxy(object):
    """
    File-like object that counts bytes read.
    To be swapped in for wsgi.input for accounting purposes.
    """
    def __init__(self, wsgi_input):
        """
        :param wsgi_input: file-like object to wrap the functionality of
        """
        self.wsgi_input = wsgi_input
        self.bytes_received = 0
        self.client_disconnect = False

    def read(self, *args, **kwargs):
        """
        Pass read request to the underlying file-like object and
        add bytes read to total.
        """
        try:
            chunk = self.wsgi_input.read(*args, **kwargs)
        except Exception:
            self.client_disconnect = True
            raise
        self.bytes_received += len(chunk)
        return chunk

    def readline(self, *args, **kwargs):
        """
        Pass readline request to the underlying file-like object and
        add bytes read to total.
        """
        try:
            line = self.wsgi_input.readline(*args, **kwargs)
        except Exception:
            self.client_disconnect = True
            raise
        self.bytes_received += len(line)
        return line
