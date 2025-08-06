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

"""Miscellaneous utility functions for use with Swift."""


import base64
import binascii
import bisect
import collections
import errno
import fcntl
import grp
import json
import os
import pwd
import re
import string
import struct
import sys
import time
import uuid
import functools
import email.parser
from random import shuffle
from contextlib import contextmanager, closing
import ctypes
import ctypes.util
from optparse import OptionParser
import traceback
import warnings

from tempfile import gettempdir, mkstemp, NamedTemporaryFile
import glob
import itertools
import stat

import eventlet
import eventlet.debug
import eventlet.greenthread
import eventlet.patcher
import eventlet.semaphore
try:
    import importlib.metadata
    pkg_resources = None
except ImportError:
    # python < 3.8
    import pkg_resources
from eventlet import GreenPool, sleep, Timeout
from eventlet.event import Event
from eventlet.green import socket
import eventlet.hubs
import eventlet.queue

import pickle  # nosec: B403
from configparser import (ConfigParser, NoSectionError,
                          NoOptionError)
from urllib.parse import unquote, urlparse
from collections import UserList

import swift.common.exceptions
from swift.common.http import is_server_error
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.linkat import linkat

# For backwards compatability with 3rd party middlewares
from swift.common.registry import register_swift_info, get_swift_info  # noqa

from .base import (  # noqa
    md5, get_valid_utf8_str, quote, split_path)
from swift.common.utils.logs import (   # noqa
    SysLogHandler,  # t.u.helpers.setup_servers monkey patch is sketch
    logging_monkey_patch,
    get_swift_logger,
    get_prefixed_swift_logger,
    LogLevelFilter,
    NullLogger,
    capture_stdio,
    SwiftLogFormatter,
    LoggerFileObject,
    PipeMutex,
    NoopMutex,
    ThreadSafeSysLogHandler,
    StrAnonymizer,
    get_log_line,
    StrFormatTime,
    LogStringFormatter,
    get_policy_index,
    LOG_LINE_DEFAULT_FORMAT,
    NOTICE,
)
from swift.common.utils.config import ( # noqa
    TRUE_VALUES,
    NicerInterpolation,
    config_true_value,
    append_underscore,
    non_negative_float,
    non_negative_int,
    config_positive_int_value,
    config_float_value,
    config_auto_int_value,
    config_percent_value,
    config_request_node_count_value,
    config_fallocate_value,
    config_read_prefixed_options,
    config_read_reseller_options,
    parse_prefixed_conf,
    affinity_locality_predicate,
    affinity_key_function,
    readconf,
    read_conf_dir,
)
from swift.common.utils.libc import (  # noqa
    F_SETPIPE_SZ,
    load_libc_function,
    drop_buffer_cache,
    get_md5_socket,
    modify_priority,
    _LibcWrapper,
)
from swift.common.utils.timestamp import (  # noqa
    NORMAL_FORMAT,
    INTERNAL_FORMAT,
    SHORT_FORMAT,
    MAX_OFFSET,
    PRECISION,
    Timestamp,
    encode_timestamps,
    decode_timestamps,
    normalize_timestamp,
    EPOCH,
    last_modified_date_to_timestamp,
    normalize_delete_at_timestamp,
    UTC,
)
from swift.common.utils.ipaddrs import (  # noqa
    is_valid_ip,
    is_valid_ipv4,
    is_valid_ipv6,
    expand_ipv6,
    parse_socket_string,
    whataremyips,
)
from swift.common.statsd_client import StatsdClient, get_statsd_client
import logging

EUCLEAN = getattr(errno, 'EUCLEAN', 117)  # otherwise not present on osx

# These are lazily pulled from libc elsewhere
_sys_fallocate = None

# If set to non-zero, fallocate routines will fail based on free space
# available being at or below this amount, in bytes.
FALLOCATE_RESERVE = 0
# Indicates if FALLOCATE_RESERVE is the percentage of free space (True) or
# the number of bytes (False).
FALLOCATE_IS_PERCENT = False

# from /usr/include/linux/falloc.h
FALLOC_FL_KEEP_SIZE = 1
FALLOC_FL_PUNCH_HOLE = 2


# Used by hash_path to offer a bit more security when generating hashes for
# paths. It simply appends this value to all paths; guessing the hash a path
# will end up with would also require knowing this suffix.
HASH_PATH_SUFFIX = b''
HASH_PATH_PREFIX = b''

SWIFT_CONF_FILE = '/etc/swift/swift.conf'

# These constants are Linux-specific, and Python doesn't seem to know
# about them. We ask anyway just in case that ever gets fixed.
#
# The values were copied from the Linux 3.x kernel headers.
O_TMPFILE = getattr(os, 'O_TMPFILE', 0o20000000 | os.O_DIRECTORY)

MD5_OF_EMPTY_STRING = 'd41d8cd98f00b204e9800998ecf8427e'
RESERVED_BYTE = b'\x00'
RESERVED_STR = u'\x00'
RESERVED = '\x00'


DEFAULT_LOCK_TIMEOUT = 10
# this is coupled with object-server.conf's network_chunk_size; if someone is
# running that unreasonably small they may find this number inefficient, but in
# the more likely case they've increased the value to optimize high througput
# transfers this will still cut off the transfer after the first chunk.
DEFAULT_DRAIN_LIMIT = 65536


def _patch_statsd_methods(target, statsd_client_source):
    """
    Note: this function is only used to create backwards compatible
    legacy "hybrid" loggers that also have a StatsdClient interface.
    It should not otherwise be used to patch arbitrary objects to
    have a StatsdClient interface.

    Patch the ``target`` object with methods that present an interface to a
    ``StatsdClient`` instance that is an attribute ``statsd_client`` of
    ``statsd_client_source``.

    Note: ``statsd_client_source`` is an object that *has a* ``StatsdClient``
        and not an object that *is a* ``StatsdClient`` instance, because the
        actual ``StatsdClient`` instance may change. The patched target
        therefore forwards its methods to whatever instance of ``StatsdClient``
        the ``statsd_client_source`` currently has.

    :param target: an object that will be patched to present an interface to a
        ``StatsdClient``.
    :param statsd_client_source: an object that must have an attribute
        ``statsd_client`` that must be an instance of a ``StatsdClient``.
        This is typically a core ``logging.Logger`` that has been patched with
        a ``StatsdClient`` by ``get_logger()``.
    """
    try:
        if not isinstance(statsd_client_source.statsd_client, StatsdClient):
            raise ValueError()
    except (AttributeError, ValueError):
        raise ValueError(
            'statsd_client_source must have a statsd_client attribute')

    def set_statsd_prefix(prefix):
        """
        This method is deprecated. Callers should use the
        ``statsd_tail_prefix`` argument of ``get_logger`` when instantiating a
        logger.

        The StatsD client prefix defaults to the "name" of the logger.  This
        method may override that default with a specific value.  Currently used
        in the proxy-server to differentiate the Account, Container, and Object
        controllers.
        """
        warnings.warn(
            'set_statsd_prefix() is deprecated; use the '
            '``statsd_tail_prefix`` argument to ``get_logger`` instead.',
            DeprecationWarning, stacklevel=2
        )
        if getattr(statsd_client_source, 'statsd_client'):
            statsd_client_source.statsd_client._set_prefix(prefix)

    def statsd_delegate(statsd_func_name):
        """
        Factory to create methods which delegate to methods on
        ``statsd_client_source.statsd_client`` (an instance of StatsdClient).
        The created methods conditionally delegate to a method whose name is
        given in 'statsd_func_name'.  The created delegate methods are a no-op
        when StatsD logging is not configured.

        :param statsd_func_name: the name of a method on ``StatsdClient``.
        """
        func = getattr(StatsdClient, statsd_func_name)

        @functools.wraps(func)
        def wrapped(*a, **kw):
            func = getattr(statsd_client_source.statsd_client,
                           statsd_func_name)
            return func(*a, **kw)
        return wrapped

    target.update_stats = statsd_delegate('update_stats')
    target.increment = statsd_delegate('increment')
    target.decrement = statsd_delegate('decrement')
    target.timing = statsd_delegate('timing')
    target.timing_since = statsd_delegate('timing_since')
    target.transfer_rate = statsd_delegate('transfer_rate')
    target.set_statsd_prefix = set_statsd_prefix
    target.statsd_client_source = statsd_client_source


def get_logger(conf, name=None, log_to_console=False, log_route=None,
               fmt="%(server)s: %(message)s", statsd_tail_prefix=None):
    """
    Returns a ``SwiftLogAdapter`` that has been patched to also provide an
        interface to a ``StatsdClient``.

    :param conf: Configuration dict to read settings from
    :param name: This value is used to populate the ``server`` field in the log
                 format, as the prefix for statsd messages, and as the default
                 value for ``log_route``; defaults to the ``log_name`` value in
                 ``conf``, if it exists, or to 'swift'.
    :param log_to_console: Add handler which writes to console on stderr.
    :param log_route: Route for the logging, not emitted to the log, just used
                      to separate logging configurations; defaults to the value
                      of ``name`` or whatever ``name`` defaults to. This value
                      is used as the name attribute of the
                      ``SwiftLogAdapter`` that is returned.
    :param fmt: Override log format.
    :param statsd_tail_prefix: tail prefix to pass to ``StatsdClient``; if None
        then the tail prefix defaults to the value of ``name``.
    :return: an instance of ``SwiftLogAdapter``.
    """
    conf = conf or {}
    swift_logger = get_swift_logger(
        conf, name, log_to_console, log_route, fmt)
    name = conf.get('log_name', 'swift') if name is None else name
    tail_prefix = name if statsd_tail_prefix is None else statsd_tail_prefix
    statsd_client = get_statsd_client(conf, tail_prefix, swift_logger.logger)
    swift_logger.logger.statsd_client = statsd_client
    _patch_statsd_methods(swift_logger, swift_logger.logger)
    return swift_logger


def get_prefixed_logger(swift_logger, prefix):
    """
    Return a clone of the given ``swift_logger`` with a new prefix string
    that replaces the prefix string of the given ``swift_logger``

    If the given ``swift_logger`` has been patched with an interface to a
    ``StatsdClient`` instance then the returned ``SwiftLogAdapter`` will also
    be patched with an interface to the same ``StatsdClient`` instance.

    :param swift_logger: an instance of ``SwiftLogAdapter``.
    :param prefix: a string prefix.
    :returns: a new instance of ``SwiftLogAdapter``.
    """
    new_logger = get_prefixed_swift_logger(swift_logger, prefix=prefix)
    if hasattr(swift_logger, 'statsd_client_source'):
        _patch_statsd_methods(
            new_logger, swift_logger.statsd_client_source)
    return new_logger


class InvalidHashPathConfigError(ValueError):

    def __str__(self):
        return "[swift-hash]: both swift_hash_path_suffix and " \
            "swift_hash_path_prefix are missing from %s" % SWIFT_CONF_FILE


def set_swift_dir(swift_dir):
    """
    Sets the directory from which swift config files will be read. If the given
    directory differs from that already set then the swift.conf file in the new
    directory will be validated and storage policies will be reloaded from the
    new swift.conf file.

    :param swift_dir: non-default directory to read swift.conf from
    """
    global HASH_PATH_SUFFIX
    global HASH_PATH_PREFIX
    global SWIFT_CONF_FILE
    if (swift_dir is not None and
            swift_dir != os.path.dirname(SWIFT_CONF_FILE)):
        SWIFT_CONF_FILE = os.path.join(
            swift_dir, os.path.basename(SWIFT_CONF_FILE))
        HASH_PATH_PREFIX = b''
        HASH_PATH_SUFFIX = b''
        validate_configuration()
        return True
    return False


def validate_hash_conf():
    global HASH_PATH_SUFFIX
    global HASH_PATH_PREFIX
    if not HASH_PATH_SUFFIX and not HASH_PATH_PREFIX:
        hash_conf = ConfigParser()

        # Use Latin1 to accept arbitrary bytes in the hash prefix/suffix
        with open(SWIFT_CONF_FILE, encoding='latin1') as swift_conf_file:
            hash_conf.read_file(swift_conf_file)

        try:
            HASH_PATH_SUFFIX = hash_conf.get(
                'swift-hash', 'swift_hash_path_suffix').encode('latin1')
        except (NoSectionError, NoOptionError):
            pass
        try:
            HASH_PATH_PREFIX = hash_conf.get(
                'swift-hash', 'swift_hash_path_prefix').encode('latin1')
        except (NoSectionError, NoOptionError):
            pass

        if not HASH_PATH_SUFFIX and not HASH_PATH_PREFIX:
            raise InvalidHashPathConfigError()


try:
    validate_hash_conf()
except (InvalidHashPathConfigError, IOError):
    # could get monkey patched or lazy loaded
    pass


def backward(f, blocksize=4096):
    """
    A generator returning lines from a file starting with the last line,
    then the second last line, etc. i.e., it reads lines backwards.
    Stops when the first line (if any) is read.
    This is useful when searching for recent activity in very
    large files.

    :param f: file object to read
    :param blocksize: no of characters to go backwards at each block
    """
    f.seek(0, os.SEEK_END)
    if f.tell() == 0:
        return
    last_row = b''
    while f.tell() != 0:
        try:
            f.seek(-blocksize, os.SEEK_CUR)
        except IOError:
            blocksize = f.tell()
            f.seek(-blocksize, os.SEEK_CUR)
        block = f.read(blocksize)
        f.seek(-blocksize, os.SEEK_CUR)
        rows = block.split(b'\n')
        rows[-1] = rows[-1] + last_row
        while rows:
            last_row = rows.pop(-1)
            if rows and last_row:
                yield last_row
    yield last_row


def eventlet_monkey_patch():
    """
    Install the appropriate Eventlet monkey patches.
    """
    # NOTE(sileht):
    #     monkey-patching thread is required by python-keystoneclient;
    #     monkey-patching select is required by oslo.messaging pika driver
    #         if thread is monkey-patched.
    eventlet.patcher.monkey_patch(all=False, socket=True, select=True,
                                  thread=True)


def monkey_patch():
    """
    Apply all swift monkey patching consistently in one place.
    """
    eventlet_monkey_patch()
    logging_monkey_patch()


def validate_configuration():
    try:
        validate_hash_conf()
    except InvalidHashPathConfigError as e:
        sys.exit("Error: %s" % e)


def generate_trans_id(trans_id_suffix):
    return 'tx%s-%010x%s' % (
        uuid.uuid4().hex[:21], int(time.time()), quote(trans_id_suffix))


def get_trans_id_time(trans_id):
    if len(trans_id) >= 34 and \
       trans_id.startswith('tx') and trans_id[23] == '-':
        try:
            return int(trans_id[24:34], 16)
        except ValueError:
            pass
    return None


class FileLikeIter(object):

    def __init__(self, iterable):
        """
        Wraps an iterable to behave as a file-like object.

        The iterable must be a byte string or yield byte strings.
        """
        if isinstance(iterable, bytes):
            iterable = (iterable, )
        self.iterator = iter(iterable)
        self.buf = None
        self.closed = False

    def __iter__(self):
        return self

    def __next__(self):
        """
        :raise StopIteration: if there are no more values to iterate.
        :raise ValueError: if the close() method has been called.
        :return: the next value.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self.buf:
            rv = self.buf
            self.buf = None
            return rv
        else:
            return next(self.iterator)

    def read(self, size=-1):
        """
        :param size: (optional) the maximum number of bytes to read. The
        default value of ``-1`` means 'unlimited' i.e. read until the wrapped
        iterable is exhausted.
        :raise ValueError: if the close() method has been called.
        :return: a bytes literal; if the wrapped iterable has been exhausted
            then a zero-length bytes literal is returned.
        """
        size = -1 if size is None else size
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if size < 0:
            return b''.join(self)
        elif not size:
            chunk = b''
        elif self.buf:
            chunk = self.buf
            self.buf = None
        else:
            try:
                chunk = next(self.iterator)
            except StopIteration:
                return b''
        if len(chunk) > size:
            self.buf = chunk[size:]
            chunk = chunk[:size]
        return chunk

    def readline(self, size=-1):
        """
        Read the next line.

        :param size: (optional) the maximum number of bytes of the next line to
            read. The default value of ``-1`` means 'unlimited' i.e. read to
            the end of the line or until the wrapped iterable is exhausted,
            whichever is first.
        :raise ValueError: if the close() method has been called.
        :return: a bytes literal; if the wrapped iterable has been exhausted
            then a zero-length bytes literal is returned.
        """
        size = -1 if size is None else size
        if self.closed:
            raise ValueError('I/O operation on closed file')
        data = b''
        while b'\n' not in data and (size < 0 or len(data) < size):
            if size < 0:
                chunk = self.read(1024)
            else:
                chunk = self.read(size - len(data))
            if not chunk:
                break
            data += chunk
        if b'\n' in data:
            data, sep, rest = data.partition(b'\n')
            data += sep
            if self.buf:
                self.buf = rest + self.buf
            else:
                self.buf = rest
        return data

    def readlines(self, sizehint=-1):
        """
        Call readline() repeatedly and return a list of the lines so read.

        :param sizehint: (optional) an approximate bound on the total number of
            bytes in the lines returned. Lines are read until ``sizehint`` has
            been exceeded but complete lines are always returned, so the total
            bytes read may exceed ``sizehint``.
        :raise ValueError: if the close() method has been called.
        :return: a list of bytes literals, each a line from the file.
        """
        sizehint = -1 if sizehint is None else sizehint
        if self.closed:
            raise ValueError('I/O operation on closed file')
        lines = []
        while True:
            line = self.readline(sizehint)
            if not line:
                break
            lines.append(line)
            if sizehint >= 0:
                sizehint -= len(line)
                if sizehint <= 0:
                    break
        return lines

    def close(self):
        """
        Close the iter.

        Once close() has been called the iter cannot be used for further I/O
        operations. close() may be called more than once without error.
        """
        self.iterator = None
        self.closed = True


def fs_has_free_space(fs_path_or_fd, space_needed, is_percent):
    """
    Check to see whether or not a filesystem has the given amount of space
    free. Unlike fallocate(), this does not reserve any space.

    :param fs_path_or_fd: path to a file or directory on the filesystem, or an
        open file descriptor; if a directory, typically the path to the
        filesystem's mount point

    :param space_needed: minimum bytes or percentage of free space

    :param is_percent: if True, then space_needed is treated as a percentage
        of the filesystem's capacity; if False, space_needed is a number of
        free bytes.

    :returns: True if the filesystem has at least that much free space,
        False otherwise

    :raises OSError: if fs_path does not exist
    """
    if isinstance(fs_path_or_fd, int):
        st = os.fstatvfs(fs_path_or_fd)
    else:
        st = os.statvfs(fs_path_or_fd)
    free_bytes = st.f_frsize * st.f_bavail
    if is_percent:
        size_bytes = st.f_frsize * st.f_blocks
        free_percent = float(free_bytes) / float(size_bytes) * 100
        return free_percent >= space_needed
    else:
        return free_bytes >= space_needed


_fallocate_enabled = True
_fallocate_warned_about_missing = False
_sys_fallocate = _LibcWrapper('fallocate')
_sys_posix_fallocate = _LibcWrapper('posix_fallocate')


def disable_fallocate():
    global _fallocate_enabled
    _fallocate_enabled = False


def fallocate(fd, size, offset=0):
    """
    Pre-allocate disk space for a file.

    This function can be disabled by calling disable_fallocate(). If no
    suitable C function is available in libc, this function is a no-op.

    :param fd: file descriptor
    :param size: size to allocate (in bytes)
    """
    global _fallocate_enabled
    if not _fallocate_enabled:
        return

    if size < 0:
        size = 0  # Done historically; not really sure why
    if size >= (1 << 63):
        raise ValueError('size must be less than 2 ** 63')
    if offset < 0:
        raise ValueError('offset must be non-negative')
    if offset >= (1 << 63):
        raise ValueError('offset must be less than 2 ** 63')

    # Make sure there's some (configurable) amount of free space in
    # addition to the number of bytes we're allocating.
    if FALLOCATE_RESERVE:
        st = os.fstatvfs(fd)
        free = st.f_frsize * st.f_bavail - size
        if FALLOCATE_IS_PERCENT:
            free = (float(free) / float(st.f_frsize * st.f_blocks)) * 100
        if float(free) <= float(FALLOCATE_RESERVE):
            raise OSError(
                errno.ENOSPC,
                'FALLOCATE_RESERVE fail %g <= %g' %
                (free, FALLOCATE_RESERVE))

    if _sys_fallocate.available:
        # Parameters are (fd, mode, offset, length).
        #
        # mode=FALLOC_FL_KEEP_SIZE pre-allocates invisibly (without
        # affecting the reported file size).
        ret = _sys_fallocate(
            fd, FALLOC_FL_KEEP_SIZE, ctypes.c_uint64(offset),
            ctypes.c_uint64(size))
        err = ctypes.get_errno()
    elif _sys_posix_fallocate.available:
        # Parameters are (fd, offset, length).
        ret = _sys_posix_fallocate(fd, ctypes.c_uint64(offset),
                                   ctypes.c_uint64(size))
        err = ctypes.get_errno()
    else:
        # No suitable fallocate-like function is in our libc. Warn about it,
        # but just once per process, and then do nothing.
        global _fallocate_warned_about_missing
        if not _fallocate_warned_about_missing:
            logging.warning("Unable to locate fallocate, posix_fallocate in "
                            "libc.  Leaving as a no-op.")
            _fallocate_warned_about_missing = True
        return

    if ret and err not in (0, errno.ENOSYS, errno.EOPNOTSUPP,
                           errno.EINVAL):
        raise OSError(err, 'Unable to fallocate(%s)' % size)


def punch_hole(fd, offset, length):
    """
    De-allocate disk space in the middle of a file.

    :param fd: file descriptor
    :param offset: index of first byte to de-allocate
    :param length: number of bytes to de-allocate
    """
    if offset < 0:
        raise ValueError('offset must be non-negative')
    if offset >= (1 << 63):
        raise ValueError('offset must be less than 2 ** 63')
    if length <= 0:
        raise ValueError('length must be positive')
    if length >= (1 << 63):
        raise ValueError('length must be less than 2 ** 63')

    if _sys_fallocate.available:
        # Parameters are (fd, mode, offset, length).
        ret = _sys_fallocate(
            fd,
            FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE,
            ctypes.c_uint64(offset),
            ctypes.c_uint64(length))
        err = ctypes.get_errno()
        if ret and err:
            mode_str = "FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE"
            raise OSError(err, "Unable to fallocate(%d, %s, %d, %d)" % (
                fd, mode_str, offset, length))
    else:
        raise OSError(errno.ENOTSUP,
                      'No suitable C function found for hole punching')


def fsync(fd):
    """
    Sync modified file data and metadata to disk.

    :param fd: file descriptor
    """
    if hasattr(fcntl, 'F_FULLSYNC'):
        try:
            fcntl.fcntl(fd, fcntl.F_FULLSYNC)
        except IOError as e:
            raise OSError(e.errno, 'Unable to F_FULLSYNC(%s)' % fd)
    else:
        os.fsync(fd)


def fdatasync(fd):
    """
    Sync modified file data to disk.

    :param fd: file descriptor
    """
    try:
        os.fdatasync(fd)
    except AttributeError:
        fsync(fd)


def fsync_dir(dirpath):
    """
    Sync directory entries to disk.

    :param dirpath: Path to the directory to be synced.
    """
    dirfd = None
    try:
        dirfd = os.open(dirpath, os.O_DIRECTORY | os.O_RDONLY)
        fsync(dirfd)
    except OSError as err:
        if err.errno == errno.ENOTDIR:
            # Raise error if someone calls fsync_dir on a non-directory
            raise
        logging.warning('Unable to perform fsync() on directory %(dir)s:'
                        ' %(err)s',
                        {'dir': dirpath, 'err': os.strerror(err.errno)})
    finally:
        if dirfd:
            os.close(dirfd)


def mkdirs(path):
    """
    Ensures the path is a directory or makes it if not. Errors if the path
    exists but is a file or on permissions failure.

    :param path: path to create
    """
    if not os.path.isdir(path):
        try:
            os.makedirs(path)
        except OSError as err:
            if err.errno != errno.EEXIST or not os.path.isdir(path):
                raise


def makedirs_count(path, count=0):
    """
    Same as os.makedirs() except that this method returns the number of
    new directories that had to be created.

    Also, this does not raise an error if target directory already exists.
    This behaviour is similar to Python 3.x's os.makedirs() called with
    exist_ok=True. Also similar to swift.common.utils.mkdirs()

    https://hg.python.org/cpython/file/v3.4.2/Lib/os.py#l212
    """
    head, tail = os.path.split(path)
    if not tail:
        head, tail = os.path.split(head)
    if head and tail and not os.path.exists(head):
        count = makedirs_count(head, count)
        if tail == os.path.curdir:
            return
    try:
        os.mkdir(path)
    except OSError as e:
        # EEXIST may also be raised if path exists as a file
        # Do not let that pass.
        if e.errno != errno.EEXIST or not os.path.isdir(path):
            raise
    else:
        count += 1
    return count


def renamer(old, new, fsync=True):
    """
    Attempt to fix / hide race conditions like empty object directories
    being removed by backend processes during uploads, by retrying.

    The containing directory of 'new' and of all newly created directories are
    fsync'd by default. This _will_ come at a performance penalty. In cases
    where these additional fsyncs are not necessary, it is expected that the
    caller of renamer() turn it off explicitly.

    :param old: old path to be renamed
    :param new: new path to be renamed to
    :param fsync: fsync on containing directory of new and also all
                  the newly created directories.
    """
    dirpath = os.path.dirname(new)
    try:
        count = makedirs_count(dirpath)
        os.rename(old, new)
    except OSError:
        count = makedirs_count(dirpath)
        os.rename(old, new)
    if fsync:
        # If count=0, no new directories were created. But we still need to
        # fsync leaf dir after os.rename().
        # If count>0, starting from leaf dir, fsync parent dirs of all
        # directories created by makedirs_count()
        for i in range(0, count + 1):
            fsync_dir(dirpath)
            dirpath = os.path.dirname(dirpath)


def link_fd_to_path(fd, target_path, dirs_created=0, retries=2, fsync=True):
    """
    Creates a link to file descriptor at target_path specified. This method
    does not close the fd for you. Unlike rename, as linkat() cannot
    overwrite target_path if it exists, we unlink and try again.

    Attempts to fix / hide race conditions like empty object directories
    being removed by backend processes during uploads, by retrying.

    :param fd: File descriptor to be linked
    :param target_path: Path in filesystem where fd is to be linked
    :param dirs_created: Number of newly created directories that needs to
                         be fsync'd.
    :param retries: number of retries to make
    :param fsync: fsync on containing directory of target_path and also all
                  the newly created directories.
    """
    dirpath = os.path.dirname(target_path)
    attempts = 0
    while True:
        attempts += 1
        try:
            linkat(linkat.AT_FDCWD, "/proc/self/fd/%d" % (fd),
                   linkat.AT_FDCWD, target_path, linkat.AT_SYMLINK_FOLLOW)
            break
        except IOError as err:
            if attempts > retries:
                raise
            if err.errno == errno.ENOENT:
                dirs_created = makedirs_count(dirpath)
            elif err.errno == errno.EEXIST:
                try:
                    os.unlink(target_path)
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise
            else:
                raise

    if fsync:
        for i in range(0, dirs_created + 1):
            fsync_dir(dirpath)
            dirpath = os.path.dirname(dirpath)


def validate_device_partition(device, partition):
    """
    Validate that a device and a partition are valid and won't lead to
    directory traversal when used.

    :param device: device to validate
    :param partition: partition to validate
    :raises ValueError: if given an invalid device or partition
    """
    if not device or '/' in device or device in ['.', '..']:
        raise ValueError('Invalid device: %s' % quote(device or ''))
    if not partition or '/' in partition or partition in ['.', '..']:
        raise ValueError('Invalid partition: %s' % quote(partition or ''))


class RateLimitedIterator(object):
    """
    Wrap an iterator to only yield elements at a rate of N per second.

    :param iterable: iterable to wrap
    :param elements_per_second: the rate at which to yield elements
    :param limit_after: rate limiting kicks in only after yielding
                        this many elements; default is 0 (rate limit
                        immediately)
    """

    def __init__(self, iterable, elements_per_second, limit_after=0,
                 ratelimit_if=lambda _junk: True):
        self.iterator = iter(iterable)
        self.elements_per_second = elements_per_second
        self.limit_after = limit_after
        self.rate_limiter = EventletRateLimiter(elements_per_second)
        self.ratelimit_if = ratelimit_if

    def __iter__(self):
        return self

    def __next__(self):
        next_value = next(self.iterator)

        if self.ratelimit_if(next_value):
            if self.limit_after > 0:
                self.limit_after -= 1
            else:
                self.rate_limiter.wait()
        return next_value


class GreenthreadSafeIterator(object):
    """
    Wrap an iterator to ensure that only one greenthread is inside its next()
    method at a time.

    This is useful if an iterator's next() method may perform network IO, as
    that may trigger a greenthread context switch (aka trampoline), which can
    give another greenthread a chance to call next(). At that point, you get
    an error like "ValueError: generator already executing". By wrapping calls
    to next() with a mutex, we avoid that error.
    """

    def __init__(self, unsafe_iterable):
        self.unsafe_iter = iter(unsafe_iterable)
        self.semaphore = eventlet.semaphore.Semaphore(value=1)

    def __iter__(self):
        return self

    def __next__(self):
        with self.semaphore:
            return next(self.unsafe_iter)


def timing_stats(**dec_kwargs):
    """
    Returns a decorator that logs timing events or errors for public methods in
    swift's wsgi server controllers, based on response code.
    """
    def decorating_func(func):
        method = func.__name__

        @functools.wraps(func)
        def _timing_stats(ctrl, *args, **kwargs):
            start_time = time.time()
            resp = func(ctrl, *args, **kwargs)
            # .timing is for successful responses *or* error codes that are
            # not Swift's fault. For example, 500 is definitely the server's
            # fault, but 412 is an error code (4xx are all errors) that is
            # due to a header the client sent.
            #
            # .errors.timing is for failures that *are* Swift's fault.
            # Examples include 507 for an unmounted drive or 500 for an
            # unhandled exception.
            if not is_server_error(resp.status_int):
                ctrl.logger.timing_since(method + '.timing',
                                         start_time, **dec_kwargs)
            else:
                ctrl.logger.timing_since(method + '.errors.timing',
                                         start_time, **dec_kwargs)
            return resp

        return _timing_stats
    return decorating_func


def memcached_timing_stats(**dec_kwargs):
    """
    Returns a decorator that logs timing events or errors for public methods in
    MemcacheRing class, such as memcached set, get and etc.
    """
    def decorating_func(func):
        method = func.__name__

        @functools.wraps(func)
        def _timing_stats(cache, *args, **kwargs):
            start_time = time.time()
            result = func(cache, *args, **kwargs)
            cache.logger.timing_since(
                'memcached.' + method + '.timing', start_time, **dec_kwargs)
            return result

        return _timing_stats
    return decorating_func


def get_hub():
    """
    Checks whether poll is available and falls back
    on select if it isn't.

    Note about epoll:

    Review: https://review.opendev.org/#/c/18806/

    There was a problem where once out of every 30 quadrillion
    connections, a coroutine wouldn't wake up when the client
    closed its end. Epoll was not reporting the event or it was
    getting swallowed somewhere. Then when that file descriptor
    was re-used, eventlet would freak right out because it still
    thought it was waiting for activity from it in some other coro.

    Another note about epoll: it's hard to use when forking. epoll works
    like so:

    * create an epoll instance: ``efd = epoll_create(...)``

    * register file descriptors of interest with
      ``epoll_ctl(efd, EPOLL_CTL_ADD, fd, ...)``

    * wait for events with ``epoll_wait(efd, ...)``

    If you fork, you and all your child processes end up using the same
    epoll instance, and everyone becomes confused. It is possible to use
    epoll and fork and still have a correct program as long as you do the
    right things, but eventlet doesn't do those things. Really, it can't
    even try to do those things since it doesn't get notified of forks.

    In contrast, both poll() and select() specify the set of interesting
    file descriptors with each call, so there's no problem with forking.

    As eventlet monkey patching is now done before call get_hub() in wsgi.py
    if we use 'import select' we get the eventlet version, but since version
    0.20.0 eventlet removed select.poll() function in patched select (see:
    http://eventlet.net/doc/changelog.html and
    https://github.com/eventlet/eventlet/commit/614a20462).

    We use eventlet.patcher.original function to get python select module
    to test if poll() is available on platform.
    """
    try:
        select = eventlet.patcher.original('select')
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
    if os.geteuid() == 0:
        groups = [g.gr_gid for g in grp.getgrall() if user in g.gr_mem]
        os.setgroups(groups)
    user = pwd.getpwnam(user)
    os.setgid(user[3])
    os.setuid(user[2])
    os.environ['HOME'] = user[5]


def clean_up_daemon_hygiene():
    try:
        os.setsid()
    except OSError:
        pass
    os.chdir('/')   # in case you need to rmdir on where you started the daemon
    os.umask(0o22)  # ensure files are created with the correct privileges


def parse_options(parser=None, once=False, test_config=False, test_args=None):
    """Parse standard swift server/daemon options with optparse.OptionParser.

    :param parser: OptionParser to use. If not sent one will be created.
    :param once: Boolean indicating the "once" option is available
    :param test_config: Boolean indicating the "test-config" option is
                        available
    :param test_args: Override sys.argv; used in testing

    :returns: Tuple of (config, options); config is an absolute path to the
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
    if test_config:
        parser.add_option("-t", "--test-config",
                          default=False, action="store_true",
                          help="exit after loading and validating config; "
                               "do not run the daemon")

    # if test_args is None, optparse will use sys.argv[:1]
    options, args = parser.parse_args(args=test_args)

    if not args:
        parser.print_usage()
        print("Error: missing config path argument")
        sys.exit(1)
    config = os.path.abspath(args.pop(0))
    if not os.path.exists(config):
        parser.print_usage()
        print("Error: unable to locate %s" % config)
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


def select_ip_port(node_dict, use_replication=False):
    """
    Get the ip address and port that should be used for the given
    ``node_dict``.

    If ``use_replication`` is True then the replication ip address and port are
    returned.

    If ``use_replication`` is False (the default) and the ``node`` dict has an
    item with key ``use_replication`` then that item's value will determine if
    the replication ip address and port are returned.

    If neither ``use_replication`` nor ``node_dict['use_replication']``
    indicate otherwise then the normal ip address and port are returned.

    :param node_dict: a dict describing a node
    :param use_replication: if True then the replication ip address and port
        are returned.
    :return: a tuple of (ip address, port)
    """
    if use_replication or node_dict.get('use_replication', False):
        node_ip = node_dict['replication_ip']
        node_port = node_dict['replication_port']
    else:
        node_ip = node_dict['ip']
        node_port = node_dict['port']
    return node_ip, node_port


def node_to_string(node_dict, replication=False):
    """
    Get a string representation of a node's location.

    :param node_dict: a dict describing a node
    :param replication: if True then the replication ip address and port are
        used, otherwise the normal ip address and port are used.
    :return: a string of the form <ip address>:<port>/<device>
    """
    node_ip, node_port = select_ip_port(node_dict, use_replication=replication)
    if ':' in node_ip:
        # IPv6
        node_ip = '[%s]' % node_ip
    return '{}:{}/{}'.format(node_ip, node_port, node_dict['device'])


def storage_directory(datadir, partition, name_hash):
    """
    Get the storage directory

    :param datadir: Base data directory
    :param partition: Partition
    :param name_hash: Account, container or object name hash
    :returns: Storage directory
    """
    return os.path.join(datadir, str(partition), name_hash[-3:], name_hash)


def hash_path(account, container=None, object=None, raw_digest=False):
    """
    Get the canonical hash for an account/container/object

    :param account: Account
    :param container: Container
    :param object: Object
    :param raw_digest: If True, return the raw version rather than a hex digest
    :returns: hash string
    """
    if object and not container:
        raise ValueError('container is required if object is provided')
    paths = [account if isinstance(account, bytes)
             else account.encode('utf8')]
    if container:
        paths.append(container if isinstance(container, bytes)
                     else container.encode('utf8'))
    if object:
        paths.append(object if isinstance(object, bytes)
                     else object.encode('utf8'))
    if raw_digest:
        return md5(HASH_PATH_PREFIX + b'/' + b'/'.join(paths)
                   + HASH_PATH_SUFFIX, usedforsecurity=False).digest()
    else:
        return md5(HASH_PATH_PREFIX + b'/' + b'/'.join(paths)
                   + HASH_PATH_SUFFIX, usedforsecurity=False).hexdigest()


def get_zero_indexed_base_string(base, index):
    """
    This allows the caller to make a list of things with indexes, where the
    first item (zero indexed) is just the bare base string, and subsequent
    indexes are appended '-1', '-2', etc.

    e.g.::

      'lock', None => 'lock'
      'lock', 0    => 'lock'
      'lock', 1    => 'lock-1'
      'object', 2  => 'object-2'

    :param base: a string, the base string; when ``index`` is 0 (or None) this
                 is the identity function.
    :param index: a digit, typically an integer (or None); for values other
                  than 0 or None this digit is appended to the base string
                  separated by a hyphen.
    """
    if index == 0 or index is None:
        return_string = base
    else:
        return_string = base + "-%d" % int(index)
    return return_string


def _get_any_lock(fds):
    for fd in fds:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except IOError as err:
            if err.errno != errno.EAGAIN:
                raise
    return False


@contextmanager
def lock_path(directory, timeout=None, timeout_class=None,
              limit=1, name=None):
    """
    Context manager that acquires a lock on a directory.  This will block until
    the lock can be acquired, or the timeout time has expired (whichever occurs
    first).

    For locking exclusively, file or directory has to be opened in Write mode.
    Python doesn't allow directories to be opened in Write Mode. So we
    workaround by locking a hidden file in the directory.

    :param directory: directory to be locked
    :param timeout: timeout (in seconds). If None, defaults to
        DEFAULT_LOCK_TIMEOUT
    :param timeout_class: The class of the exception to raise if the
        lock cannot be granted within the timeout. Will be
        constructed as timeout_class(timeout, lockpath). Default:
        LockTimeout
    :param limit: The maximum number of locks that may be held concurrently on
        the same directory at the time this method is called. Note that this
        limit is only applied during the current call to this method and does
        not prevent subsequent calls giving a larger limit. Defaults to 1.
    :param name: A string to distinguishes different type of locks in a
        directory
    :raises TypeError: if limit is not an int.
    :raises ValueError: if limit is less than 1.
    """
    if timeout is None:
        timeout = DEFAULT_LOCK_TIMEOUT
    if timeout_class is None:
        timeout_class = swift.common.exceptions.LockTimeout
    if limit < 1:
        raise ValueError('limit must be greater than or equal to 1')
    mkdirs(directory)
    lockpath = '%s/.lock' % directory
    if name:
        lockpath += '-%s' % str(name)
    fds = [os.open(get_zero_indexed_base_string(lockpath, i),
                   os.O_WRONLY | os.O_CREAT)
           for i in range(limit)]
    sleep_time = 0.01
    slower_sleep_time = max(timeout * 0.01, sleep_time)
    slowdown_at = timeout * 0.01
    time_slept = 0
    try:
        with timeout_class(timeout, lockpath):
            while True:
                if _get_any_lock(fds):
                    break
                if time_slept > slowdown_at:
                    sleep_time = slower_sleep_time
                sleep(sleep_time)
                time_slept += sleep_time
        yield True
    finally:
        for fd in fds:
            os.close(fd)


@contextmanager
def lock_file(filename, timeout=None, append=False, unlink=True):
    """
    Context manager that acquires a lock on a file.  This will block until
    the lock can be acquired, or the timeout time has expired (whichever occurs
    first).

    :param filename: file to be locked
    :param timeout: timeout (in seconds). If None, defaults to
        DEFAULT_LOCK_TIMEOUT
    :param append: True if file should be opened in append mode
    :param unlink: True if the file should be unlinked at the end
    """
    if timeout is None:
        timeout = DEFAULT_LOCK_TIMEOUT
    flags = os.O_CREAT | os.O_RDWR
    if append:
        flags |= os.O_APPEND
        mode = 'a+b'
    else:
        mode = 'r+b'
    while True:
        fd = os.open(filename, flags)
        file_obj = os.fdopen(fd, mode)
        try:
            with swift.common.exceptions.LockTimeout(timeout, filename):
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except IOError as err:
                        if err.errno != errno.EAGAIN:
                            raise
                    sleep(0.01)
            try:
                if os.stat(filename).st_ino != os.fstat(fd).st_ino:
                    continue
            except OSError as err:
                if err.errno == errno.ENOENT:
                    continue
                raise
            yield file_obj
            if unlink:
                os.unlink(filename)
            break
        finally:
            file_obj.close()


def lock_parent_directory(filename, timeout=None):
    """
    Context manager that acquires a lock on the parent directory of the given
    file path.  This will block until the lock can be acquired, or the timeout
    time has expired (whichever occurs first).

    :param filename: file path of the parent directory to be locked
    :param timeout: timeout (in seconds). If None, defaults to
        DEFAULT_LOCK_TIMEOUT
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


def unlink_older_than(path, mtime):
    """
    Remove any file in a given path that was last modified before mtime.

    :param path: path to remove file from
    :param mtime: timestamp of oldest file to keep
    """
    filepaths = map(functools.partial(os.path.join, path), listdir(path))
    return unlink_paths_older_than(filepaths, mtime)


def unlink_paths_older_than(filepaths, mtime):
    """
    Remove any files from the given list that were
    last modified before mtime.

    :param filepaths: a list of strings, the full paths of files to check
    :param mtime: timestamp of oldest file to keep
    """
    for fpath in filepaths:
        try:
            if os.path.getmtime(fpath) < mtime:
                os.unlink(fpath)
        except OSError:
            pass


def item_from_env(env, item_name, allow_none=False):
    """
    Get a value from the wsgi environment

    :param env: wsgi environment dict
    :param item_name: name of item to get

    :returns: the value from the environment
    """
    item = env.get(item_name, None)
    if item is None and not allow_none:
        logging.error("ERROR: %s could not be found in env!", item_name)
    return item


def cache_from_env(env, allow_none=False):
    """
    Get memcache connection pool from the environment (which had been
    previously set by the memcache middleware

    :param env: wsgi environment dict

    :returns: swift.common.memcached.MemcacheRing from environment
    """
    return item_from_env(env, 'swift.cache', allow_none)


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
    mkdirs(tmp)
    fd, tmppath = mkstemp(dir=tmp, suffix='.tmp')
    with os.fdopen(fd, 'wb') as fo:
        pickle.dump(obj, fo, pickle_protocol)
        fo.flush()
        os.fsync(fd)
        renamer(tmppath, dest)


def search_tree(root, glob_match, ext='', exts=None, dir_ext=None):
    """Look in root, for any files/dirs matching glob, recursively traversing
    any found directories looking for files ending with ext

    :param root: start of search path
    :param glob_match: glob to match in root, matching dirs are traversed with
                       os.walk
    :param ext: only files that end in ext will be returned
    :param exts: a list of file extensions; only files that end in one of these
                 extensions will be returned; if set this list overrides any
                 extension specified using the 'ext' param.
    :param dir_ext: if present directories that end with dir_ext will not be
                    traversed and instead will be returned as a matched path

    :returns: list of full paths to matching files, sorted

    """
    exts = exts or [ext]
    found_files = []
    for path in glob.glob(os.path.join(root, glob_match)):
        if os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                if dir_ext and root.endswith(dir_ext):
                    found_files.append(root)
                    # the root is a config dir, descend no further
                    break
                for file_ in files:
                    if any(exts) and not any(file_.endswith(e) for e in exts):
                        continue
                    found_files.append(os.path.join(root, file_))
                found_dir = False
                for dir_ in dirs:
                    if dir_ext and dir_.endswith(dir_ext):
                        found_dir = True
                        found_files.append(os.path.join(root, dir_))
                if found_dir:
                    # do not descend further into matching directories
                    break
        else:
            if ext and not path.endswith(ext):
                continue
            found_files.append(path)
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
        except OSError as err:
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


def remove_directory(path):
    """Wrapper for os.rmdir, ENOENT and ENOTEMPTY are ignored

    :param path: first and only argument passed to os.rmdir
    """
    try:
        os.rmdir(path)
    except OSError as e:
        if e.errno not in (errno.ENOENT, errno.ENOTEMPTY):
            raise


def is_file_older(path, age):
    """
    Test if a file mtime is older than the given age, suppressing any OSErrors.

    :param path: first and only argument passed to os.stat
    :param age: age in seconds
    :return: True if age is less than or equal to zero or if the file mtime is
        more than ``age`` in the past; False if age is greater than zero and
        the file mtime is less than or equal to ``age`` in the past or if there
        is an OSError while stat'ing the file.
    """
    if age <= 0:
        return True
    try:
        return time.time() - os.stat(path).st_mtime > age
    except OSError:
        return False


def audit_location_generator(devices, datadir, suffix='',
                             mount_check=True, logger=None,
                             devices_filter=None, partitions_filter=None,
                             suffixes_filter=None, hashes_filter=None,
                             hook_pre_device=None, hook_post_device=None,
                             hook_pre_partition=None, hook_post_partition=None,
                             hook_pre_suffix=None, hook_post_suffix=None,
                             hook_pre_hash=None, hook_post_hash=None,
                             error_counter=None, yield_hash_dirs=False):
    """
    Given a devices path and a data directory, yield (path, device,
    partition) for all files in that directory

    (devices|partitions|suffixes|hashes)_filter are meant to modify the list of
    elements that will be iterated. eg: they can be used to exclude some
    elements based on a custom condition defined by the caller.

    hook_pre_(device|partition|suffix|hash) are called before yielding the
    element, hook_pos_(device|partition|suffix|hash) are called after the
    element was yielded. They are meant to do some pre/post processing.
    eg: saving a progress status.

    :param devices: parent directory of the devices to be audited
    :param datadir: a directory located under self.devices. This should be
                    one of the DATADIR constants defined in the account,
                    container, and object servers.
    :param suffix: path name suffix required for all names returned
                   (ignored if yield_hash_dirs is True)
    :param mount_check: Flag to check if a mount check should be performed
                    on devices
    :param logger: a logger object
    :param devices_filter: a callable taking (devices, [list of devices]) as
                           parameters and returning a [list of devices]
    :param partitions_filter: a callable taking (datadir_path, [list of parts])
                              as parameters and returning a [list of parts]
    :param suffixes_filter: a callable taking (part_path, [list of suffixes])
                            as parameters and returning a [list of suffixes]
    :param hashes_filter: a callable taking (suff_path, [list of hashes]) as
                          parameters and returning a [list of hashes]
    :param hook_pre_device: a callable taking device_path as parameter
    :param hook_post_device: a callable taking device_path as parameter
    :param hook_pre_partition: a callable taking part_path as parameter
    :param hook_post_partition: a callable taking part_path as parameter
    :param hook_pre_suffix: a callable taking suff_path as parameter
    :param hook_post_suffix: a callable taking suff_path as parameter
    :param hook_pre_hash: a callable taking hash_path as parameter
    :param hook_post_hash: a callable taking hash_path as parameter
    :param error_counter: a dictionary used to accumulate error counts; may
                          add keys 'unmounted' and 'unlistable_partitions'
    :param yield_hash_dirs: if True, yield hash dirs instead of individual
                            files
    """
    device_dir = listdir(devices)
    # randomize devices in case of process restart before sweep completed
    shuffle(device_dir)
    if devices_filter:
        device_dir = devices_filter(devices, device_dir)
    for device in device_dir:
        if mount_check and not ismount(os.path.join(devices, device)):
            if error_counter is not None:
                error_counter.setdefault('unmounted', [])
                error_counter['unmounted'].append(device)
            if logger:
                logger.warning(
                    'Skipping %s as it is not mounted', device)
            continue
        if hook_pre_device:
            hook_pre_device(os.path.join(devices, device))
        datadir_path = os.path.join(devices, device, datadir)
        try:
            partitions = listdir(datadir_path)
        except OSError as e:
            # NB: listdir ignores non-existent datadir_path
            if error_counter is not None:
                error_counter.setdefault('unlistable_partitions', [])
                error_counter['unlistable_partitions'].append(datadir_path)
            if logger:
                logger.warning('Skipping %(datadir)s because %(err)s',
                               {'datadir': datadir_path, 'err': e})
            continue
        if partitions_filter:
            partitions = partitions_filter(datadir_path, partitions)
        for partition in partitions:
            part_path = os.path.join(datadir_path, partition)
            if hook_pre_partition:
                hook_pre_partition(part_path)
            try:
                suffixes = listdir(part_path)
            except OSError as e:
                if e.errno != errno.ENOTDIR:
                    raise
                continue
            if suffixes_filter:
                suffixes = suffixes_filter(part_path, suffixes)
            for asuffix in suffixes:
                suff_path = os.path.join(part_path, asuffix)
                if hook_pre_suffix:
                    hook_pre_suffix(suff_path)
                try:
                    hashes = listdir(suff_path)
                except OSError as e:
                    if e.errno != errno.ENOTDIR:
                        raise
                    continue
                if hashes_filter:
                    hashes = hashes_filter(suff_path, hashes)
                for hsh in hashes:
                    hash_path = os.path.join(suff_path, hsh)
                    if hook_pre_hash:
                        hook_pre_hash(hash_path)
                    if yield_hash_dirs:
                        if os.path.isdir(hash_path):
                            yield hash_path, device, partition
                    else:
                        try:
                            files = sorted(listdir(hash_path), reverse=True)
                        except OSError as e:
                            if e.errno != errno.ENOTDIR:
                                raise
                            continue
                        for fname in files:
                            if suffix and not fname.endswith(suffix):
                                continue
                            path = os.path.join(hash_path, fname)
                            yield path, device, partition
                    if hook_post_hash:
                        hook_post_hash(hash_path)
                if hook_post_suffix:
                    hook_post_suffix(suff_path)
            if hook_post_partition:
                hook_post_partition(part_path)
        if hook_post_device:
            hook_post_device(os.path.join(devices, device))


class AbstractRateLimiter(object):
    # 1,000 milliseconds = 1 second
    clock_accuracy = 1000.0

    def __init__(self, max_rate, rate_buffer=5, burst_after_idle=False,
                 running_time=0):
        """
        :param max_rate: The maximum rate per second allowed for the process.
            Must be > 0 to engage rate-limiting behavior.
        :param rate_buffer: Number of seconds the rate counter can drop and be
            allowed to catch up (at a faster than listed rate). A larger number
            will result in larger spikes in rate but better average accuracy.
        :param burst_after_idle: If False (the default) then the rate_buffer
            allowance is lost after the rate limiter has not been called for
            more than rate_buffer seconds. If True then the rate_buffer
            allowance is preserved during idle periods which means that a burst
            of requests may be granted immediately after the idle period.
        :param running_time: The running time in milliseconds of the next
            allowable request. Setting this to any time in the past will cause
            the rate limiter to immediately allow requests; setting this to a
            future time will cause the rate limiter to deny requests until that
            time. If ``burst_after_idle`` is True then this can
            be set to current time (ms) to avoid an initial burst, or set to
            running_time < (current time - rate_buffer ms) to allow an initial
            burst.
        """
        self.set_max_rate(max_rate)
        self.set_rate_buffer(rate_buffer)
        self.burst_after_idle = burst_after_idle
        self.running_time = running_time

    def set_max_rate(self, max_rate):
        self.max_rate = max_rate
        self.time_per_incr = (self.clock_accuracy / self.max_rate
                              if self.max_rate else 0)

    def set_rate_buffer(self, rate_buffer):
        self.rate_buffer_ms = rate_buffer * self.clock_accuracy

    def _sleep(self, seconds):
        # subclasses should override to implement a sleep
        raise NotImplementedError

    def is_allowed(self, incr_by=1, now=None, block=False):
        """
        Check if the calling process is allowed to proceed according to the
        rate limit.

        :param incr_by: How much to increment the counter.  Useful if you want
                        to ratelimit 1024 bytes/sec and have differing sizes
                        of requests. Must be > 0 to engage rate-limiting
                        behavior.
        :param now: The time in seconds; defaults to time.time()
        :param block: if True, the call will sleep until the calling process
            is allowed to proceed; otherwise the call returns immediately.
        :return: True if the the calling process is allowed to proceed, False
            otherwise.
        """
        if self.max_rate <= 0 or incr_by <= 0:
            return True

        now = now or time.time()
        # Convert seconds to milliseconds
        now = now * self.clock_accuracy

        # Calculate time per request in milliseconds
        time_per_request = self.time_per_incr * float(incr_by)

        # Convert rate_buffer to milliseconds and compare
        if now - self.running_time > self.rate_buffer_ms:
            self.running_time = now
            if self.burst_after_idle:
                self.running_time -= self.rate_buffer_ms

        if now >= self.running_time:
            self.running_time += time_per_request
            allowed = True
        elif block:
            sleep_time = (self.running_time - now) / self.clock_accuracy
            # increment running time before sleeping in case the sleep allows
            # another thread to inspect the rate limiter state
            self.running_time += time_per_request
            # Convert diff to a floating point number of seconds and sleep
            self._sleep(sleep_time)
            allowed = True
        else:
            allowed = False

        return allowed

    def wait(self, incr_by=1, now=None):
        self.is_allowed(incr_by=incr_by, now=now, block=True)


class EventletRateLimiter(AbstractRateLimiter):
    def __init__(self, max_rate, rate_buffer=5, running_time=0,
                 burst_after_idle=False):
        super(EventletRateLimiter, self).__init__(
            max_rate, rate_buffer=rate_buffer, running_time=running_time,
            burst_after_idle=burst_after_idle)

    def _sleep(self, seconds):
        eventlet.sleep(seconds)


def ratelimit_sleep(running_time, max_rate, incr_by=1, rate_buffer=5):
    """
    Will eventlet.sleep() for the appropriate time so that the max_rate
    is never exceeded.  If max_rate is 0, will not ratelimit.  The
    maximum recommended rate should not exceed (1000 * incr_by) a second
    as eventlet.sleep() does involve some overhead.  Returns running_time
    that should be used for subsequent calls.

    :param running_time: the running time in milliseconds of the next
                         allowable request. Best to start at zero.
    :param max_rate: The maximum rate per second allowed for the process.
    :param incr_by: How much to increment the counter.  Useful if you want
                    to ratelimit 1024 bytes/sec and have differing sizes
                    of requests. Must be > 0 to engage rate-limiting
                    behavior.
    :param rate_buffer: Number of seconds the rate counter can drop and be
                        allowed to catch up (at a faster than listed rate).
                        A larger number will result in larger spikes in rate
                        but better average accuracy. Must be > 0 to engage
                        rate-limiting behavior.
    :return: The absolute time for the next interval in milliseconds; note
        that time could have passed well beyond that point, but the next call
        will catch that and skip the sleep.
    """
    warnings.warn(
        'ratelimit_sleep() is deprecated; use the ``EventletRateLimiter`` '
        'class instead.', DeprecationWarning, stacklevel=2
    )
    rate_limit = EventletRateLimiter(max_rate, rate_buffer=rate_buffer,
                                     running_time=running_time)
    rate_limit.wait(incr_by=incr_by)
    return rate_limit.running_time


class ContextPool(GreenPool):
    """GreenPool subclassed to kill its coros when it gets gc'ed"""

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        for coro in list(self.coroutines_running):
            coro.kill()


class GreenAsyncPileWaitallTimeout(Timeout):
    pass


DEAD = object()


class GreenAsyncPile(object):
    """
    Runs jobs in a pool of green threads, and the results can be retrieved by
    using this object as an iterator.

    This is very similar in principle to eventlet.GreenPile, except it returns
    results as they become available rather than in the order they were
    launched.

    Correlating results with jobs (if necessary) is left to the caller.
    """

    def __init__(self, size_or_pool):
        """
        :param size_or_pool: thread pool size or a pool to use
        """
        if isinstance(size_or_pool, GreenPool):
            self._pool = size_or_pool
            size = self._pool.size
        else:
            self._pool = GreenPool(size_or_pool)
            size = size_or_pool
        self._responses = eventlet.queue.LightQueue(size)
        self._inflight = 0
        self._pending = 0

    def _run_func(self, func, args, kwargs):
        try:
            self._responses.put(func(*args, **kwargs))
        except Exception:
            if eventlet.hubs.get_hub().debug_exceptions:
                traceback.print_exception(*sys.exc_info())
            self._responses.put(DEAD)
        finally:
            self._inflight -= 1

    @property
    def inflight(self):
        return self._inflight

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a job in a green thread on the pile.
        """
        self._pending += 1
        self._inflight += 1
        self._pool.spawn(self._run_func, func, args, kwargs)

    def waitfirst(self, timeout):
        """
        Wait up to timeout seconds for first result to come in.

        :param timeout: seconds to wait for results
        :returns: first item to come back, or None
        """
        for result in self._wait(timeout, first_n=1):
            return result

    def waitall(self, timeout):
        """
        Wait timeout seconds for any results to come in.

        :param timeout: seconds to wait for results
        :returns: list of results accrued in that time
        """
        return self._wait(timeout)

    def _wait(self, timeout, first_n=None):
        results = []
        try:
            with GreenAsyncPileWaitallTimeout(timeout):
                while True:
                    results.append(next(self))
                    if first_n and len(results) >= first_n:
                        break
        except (GreenAsyncPileWaitallTimeout, StopIteration):
            pass
        return results

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                rv = self._responses.get_nowait()
            except eventlet.queue.Empty:
                if self._inflight == 0:
                    raise StopIteration()
                rv = self._responses.get()
            self._pending -= 1
            if rv is DEAD:
                continue
            return rv


class StreamingPile(GreenAsyncPile):
    """
    Runs jobs in a pool of green threads, spawning more jobs as results are
    retrieved and worker threads become available.

    When used as a context manager, has the same worker-killing properties as
    :class:`ContextPool`.
    """

    def __init__(self, size):
        """:param size: number of worker threads to use"""
        self.pool = ContextPool(size)
        super(StreamingPile, self).__init__(self.pool)

    def asyncstarmap(self, func, args_iter):
        """
        This is the same as :func:`itertools.starmap`, except that *func* is
        executed in a separate green thread for each item, and results won't
        necessarily have the same order as inputs.
        """
        args_iter = iter(args_iter)

        # Initialize the pile
        for args in itertools.islice(args_iter, self.pool.size):
            self.spawn(func, *args)

        # Keep populating the pile as greenthreads become available
        for args in args_iter:
            try:
                to_yield = next(self)
            except StopIteration:
                break
            yield to_yield
            self.spawn(func, *args)

        # Drain the pile
        for result in self:
            yield result

    def __enter__(self):
        self.pool.__enter__()
        return self

    def __exit__(self, type, value, traceback):
        self.pool.__exit__(type, value, traceback)


def validate_sync_to(value, allowed_sync_hosts, realms_conf):
    """
    Validates an X-Container-Sync-To header value, returning the
    validated endpoint, realm, and realm_key, or an error string.

    :param value: The X-Container-Sync-To header value to validate.
    :param allowed_sync_hosts: A list of allowed hosts in endpoints,
        if realms_conf does not apply.
    :param realms_conf: An instance of
        swift.common.container_sync_realms.ContainerSyncRealms to
        validate against.
    :returns: A tuple of (error_string, validated_endpoint, realm,
        realm_key). The error_string will None if the rest of the
        values have been validated. The validated_endpoint will be
        the validated endpoint to sync to. The realm and realm_key
        will be set if validation was done through realms_conf.
    """
    orig_value = value
    value = value.rstrip('/')
    if not value:
        return (None, None, None, None)
    if value.startswith('//'):
        if not realms_conf:
            return (None, None, None, None)
        data = value[2:].split('/')
        if len(data) != 4:
            return (
                'Invalid X-Container-Sync-To format %r' % orig_value,
                None, None, None)
        realm, cluster, account, container = data
        realm_key = realms_conf.key(realm)
        if not realm_key:
            return ('No realm key for %r' % realm, None, None, None)
        endpoint = realms_conf.endpoint(realm, cluster)
        if not endpoint:
            return (
                'No cluster endpoint for %(realm)r %(cluster)r'
                % {'realm': realm, 'cluster': cluster},
                None, None, None)
        return (
            None,
            '%s/%s/%s' % (endpoint.rstrip('/'), account, container),
            realm.upper(), realm_key)
    p = urlparse(value)
    if p.scheme not in ('http', 'https'):
        return (
            'Invalid scheme %r in X-Container-Sync-To, must be "//", '
            '"http", or "https".' % p.scheme,
            None, None, None)
    if not p.path:
        return ('Path required in X-Container-Sync-To', None, None, None)
    if p.params or p.query or p.fragment:
        return (
            'Params, queries, and fragments not allowed in '
            'X-Container-Sync-To',
            None, None, None)
    if p.hostname not in allowed_sync_hosts:
        return (
            'Invalid host %r in X-Container-Sync-To' % p.hostname,
            None, None, None)
    return (None, value, None, None)


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


def put_recon_cache_entry(cache_entry, key, item):
    """
    Update a recon cache entry item.

    If ``item`` is an empty dict then any existing ``key`` in ``cache_entry``
    will be deleted. Similarly if ``item`` is a dict and any of its values are
    empty dicts then the corresponding key will be deleted from the nested dict
    in ``cache_entry``.

    We use nested recon cache entries when the object auditor
    runs in parallel or else in 'once' mode with a specified subset of devices.

    :param cache_entry: a dict of existing cache entries
    :param key: key for item to update
    :param item: value for item to update
    """
    if isinstance(item, dict):
        if not item:
            cache_entry.pop(key, None)
            return
        if key not in cache_entry or key in cache_entry and not \
                isinstance(cache_entry[key], dict):
            cache_entry[key] = {}
        for k, v in item.items():
            if v == {}:
                cache_entry[key].pop(k, None)
            else:
                cache_entry[key][k] = v
    else:
        cache_entry[key] = item


def dump_recon_cache(cache_dict, cache_file, logger, lock_timeout=2,
                     set_owner=None):
    """Update recon cache values

    :param cache_dict: Dictionary of cache key/value pairs to write out
    :param cache_file: cache file to update
    :param logger: the logger to use to log an encountered error
    :param lock_timeout: timeout (in seconds)
    :param set_owner: Set owner of recon cache file
    """
    try:
        with lock_file(cache_file, lock_timeout, unlink=False) as cf:
            cache_entry = {}
            try:
                existing_entry = cf.readline()
                if existing_entry:
                    cache_entry = json.loads(existing_entry)
            except ValueError:
                # file doesn't have a valid entry, we'll recreate it
                pass
            for cache_key, cache_value in cache_dict.items():
                put_recon_cache_entry(cache_entry, cache_key, cache_value)
            tf = None
            try:
                with NamedTemporaryFile(dir=os.path.dirname(cache_file),
                                        delete=False) as tf:
                    cache_data = json.dumps(cache_entry, ensure_ascii=True,
                                            sort_keys=True)
                    tf.write(cache_data.encode('ascii') + b'\n')
                if set_owner:
                    os.chown(tf.name, pwd.getpwnam(set_owner).pw_uid, -1)
                renamer(tf.name, cache_file, fsync=False)
            finally:
                if tf is not None:
                    try:
                        os.unlink(tf.name)
                    except OSError as err:
                        if err.errno != errno.ENOENT:
                            raise
    except (Exception, Timeout) as err:
        logger.exception('Exception dumping recon cache: %s' % err)


def load_recon_cache(cache_file):
    """
    Load a recon cache file. Treats missing file as empty.
    """
    try:
        with open(cache_file) as fh:
            return json.load(fh)
    except IOError as e:
        if e.errno == errno.ENOENT:
            return {}
        else:
            raise
    except ValueError:  # invalid JSON
        return {}


def listdir(path):
    try:
        return os.listdir(path)
    except OSError as err:
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


def pairs(item_list):
    """
    Returns an iterator of all pairs of elements from item_list.

    :param item_list: items (no duplicates allowed)
    """
    for i, item1 in enumerate(item_list):
        for item2 in item_list[(i + 1):]:
            yield (item1, item2)


def replication(func):
    """
    Decorator to declare which methods are accessible for different
    type of servers:

    * If option replication_server is None then this decorator
      doesn't matter.
    * If option replication_server is True then ONLY decorated with
      this decorator methods will be started.
    * If option replication_server is False then decorated with this
      decorator methods will NOT be started.

    :param func: function to mark accessible for replication
    """
    func.replication = True

    return func


def public(func):
    """
    Decorator to declare which methods are publicly accessible as HTTP
    requests

    :param func: function to make public
    """
    func.publicly_accessible = True
    return func


def private(func):
    """
    Decorator to declare which methods are privately accessible as HTTP
    requests with an ``X-Backend-Allow-Private-Methods: True`` override

    :param func: function to make private
    """
    func.privately_accessible = True
    return func


def majority_size(n):
    return (n // 2) + 1


def quorum_size(n):
    """
    quorum size as it applies to services that use 'replication' for data
    integrity  (Account/Container services).  Object quorum_size is defined
    on a storage policy basis.

    Number of successful backend requests needed for the proxy to consider
    the client request successful.
    """
    return (n + 1) // 2


def rsync_ip(ip):
    """
    Transform ip string to an rsync-compatible form

    Will return ipv4 addresses unchanged, but will nest ipv6 addresses
    inside square brackets.

    :param ip: an ip string (ipv4 or ipv6)

    :returns: a string ip address
    """
    return '[%s]' % ip if is_valid_ipv6(ip) else ip


def rsync_module_interpolation(template, device):
    """
    Interpolate devices variables inside a rsync module template

    :param template: rsync module template as a string
    :param device: a device from a ring

    :returns: a string with all variables replaced by device attributes
    """
    replacements = {
        'ip': rsync_ip(device.get('ip', '')),
        'port': device.get('port', ''),
        'replication_ip': rsync_ip(device.get('replication_ip', '')),
        'replication_port': device.get('replication_port', ''),
        'region': device.get('region', ''),
        'zone': device.get('zone', ''),
        'device': device.get('device', ''),
        'meta': device.get('meta', ''),
    }
    try:
        module = template.format(**replacements)
    except KeyError as e:
        raise ValueError('Cannot interpolate rsync_module, invalid variable: '
                         '%s' % e)
    return module


class Everything(object):
    """
    A container that contains everything. If "e" is an instance of
    Everything, then "x in e" is true for all x.
    """

    def __contains__(self, element):
        return True


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


class ClosingIterator(object):
    """
    Wrap another iterator and close it, if possible, on completion/exception.

    If other closeable objects are given then they will also be closed when
    this iterator is closed.

    This is particularly useful for ensuring a generator properly closes its
    resources, even if the generator was never started.

    This class may be subclassed to override the behavior of
    ``_get_next_item``.

    :param iterable: iterator to wrap.
    :param other_closeables: other resources to attempt to close.
    """
    __slots__ = ('closeables', 'wrapped_iter', 'closed')

    def __init__(self, iterable, other_closeables=None):
        self.closeables = [iterable]
        if other_closeables:
            self.closeables.extend(other_closeables)
        # this is usually, but not necessarily, the same object
        self.wrapped_iter = iter(iterable)
        self.closed = False

    def __iter__(self):
        return self

    def _get_next_item(self):
        return next(self.wrapped_iter)

    def __next__(self):
        try:
            return self._get_next_item()
        except Exception:
            # note: if wrapped_iter is a generator then the exception
            # already caused it to exit (without raising a GeneratorExit)
            # but we still need to close any other closeables.
            self.close()
            raise

    def close(self):
        if not self.closed:
            for wrapped in self.closeables:
                close_if_possible(wrapped)
            # clear it out so they get GC'ed
            self.closeables = []
            self.wrapped_iter = iter([])
            self.closed = True


class ClosingMapper(ClosingIterator):
    """
    A closing iterator that yields the result of ``function`` as it is applied
    to each item of ``iterable``.

    Note that while this behaves similarly to the built-in ``map`` function,
    ``other_closeables`` does not have the same semantic as the ``iterables``
    argument of ``map``.

    :param function: a function that will be called with each item of
        ``iterable`` before yielding its result.
    :param iterable: iterator to wrap.
    :param other_closeables: other resources to attempt to close.
    """
    __slots__ = ('func',)

    def __init__(self, function, iterable, other_closeables=None):
        self.func = function
        super(ClosingMapper, self).__init__(iterable, other_closeables)

    def _get_next_item(self):
        return self.func(super(ClosingMapper, self)._get_next_item())


class CloseableChain(ClosingIterator):
    """
    Like itertools.chain, but with a close method that will attempt to invoke
    its sub-iterators' close methods, if any.
    """

    def __init__(self, *iterables):
        chained_iter = itertools.chain(*iterables)
        super(CloseableChain, self).__init__(chained_iter, iterables)


def reiterate(iterable):
    """
    Consume the first truthy item from an iterator, then re-chain it to the
    rest of the iterator.  This is useful when you want to make sure the
    prologue to downstream generators have been executed before continuing.
    :param iterable: an iterable object
    """
    if isinstance(iterable, (list, tuple)):
        return iterable
    else:
        iterator = iter(iterable)
        try:
            chunk = next(iterator)
            while not chunk:
                chunk = next(iterator)
            return CloseableChain([chunk], iterator)
        except StopIteration:
            close_if_possible(iterable)
            return iter([])


class InputProxy(object):
    """
    File-like object that counts bytes read.
    To be swapped in for wsgi.input for accounting purposes.

    :param wsgi_input: file-like object to be wrapped
    """

    def __init__(self, wsgi_input):
        self.wsgi_input = wsgi_input
        #: total number of bytes read from the wrapped input
        self.bytes_received = 0
        #: ``True`` if an exception is raised by ``read()`` or ``readline()``,
        #: ``False`` otherwise
        self.client_disconnect = False

    def chunk_update(self, chunk, eof, *args, **kwargs):
        """
        Called each time a chunk of bytes is read from the wrapped input.

        :param chunk: the chunk of bytes that has been read.
        :param eof: ``True`` if there are no more bytes to read from the
            wrapped input, ``False`` otherwise. If ``read()`` has been called
            this will be ``True`` when the size of ``chunk`` is less than the
            requested size or the requested size is None. If ``readline`` has
            been called this will be ``True`` when an incomplete line is read
            (i.e. not ending with ``b'\\n'``) whose length is less than the
            requested size or the requested size is None. If ``read()`` or
            ``readline()`` are called with a requested size that exactly
            matches the number of bytes remaining in the wrapped input then
            ``eof`` will be ``False``. A subsequent call to ``read()`` or
            ``readline()`` with non-zero ``size`` would result in ``eof`` being
            ``True``. Alternatively, the end of the input could be inferred
            by comparing ``bytes_received`` with the expected length of the
            input.
        """
        # subclasses may override this method; either the given chunk or an
        # alternative chunk value should be returned
        return chunk

    def read(self, size=None, *args, **kwargs):
        """
        Pass read request to the underlying file-like object and
        add bytes read to total.

        :param size: (optional) maximum number of bytes to read; the default
            ``None`` means unlimited.
        """
        try:
            chunk = self.wsgi_input.read(size, *args, **kwargs)
        except Exception:
            self.client_disconnect = True
            raise
        self.bytes_received += len(chunk)
        eof = size is None or size < 0 or len(chunk) < size
        return self.chunk_update(chunk, eof)

    def readline(self, size=None, *args, **kwargs):
        """
        Pass readline request to the underlying file-like object and
        add bytes read to total.

        :param size: (optional) maximum number of bytes to read from the
            current line; the default ``None`` means unlimited.
        """
        try:
            line = self.wsgi_input.readline(size, *args, **kwargs)
        except Exception:
            self.client_disconnect = True
            raise
        self.bytes_received += len(line)
        eof = ((size is None or size < 0 or len(line) < size)
               and (line[-1:] != b'\n'))
        return self.chunk_update(line, eof)

    def close(self):
        close_if_possible(self.wsgi_input)


class LRUCache(object):
    """
    Decorator for size/time bound memoization that evicts the least
    recently used members.
    """

    PREV, NEXT, KEY, CACHED_AT, VALUE = 0, 1, 2, 3, 4  # link fields

    def __init__(self, maxsize=1000, maxtime=3600):
        self.maxsize = maxsize
        self.maxtime = maxtime
        self.reset()

    def reset(self):
        self.mapping = {}
        self.head = [None, None, None, None, None]  # oldest
        self.tail = [self.head, None, None, None, None]  # newest
        self.head[self.NEXT] = self.tail

    def set_cache(self, value, *key):
        while len(self.mapping) >= self.maxsize:
            old_next, old_key = self.head[self.NEXT][self.NEXT:self.NEXT + 2]
            self.head[self.NEXT], old_next[self.PREV] = old_next, self.head
            del self.mapping[old_key]
        last = self.tail[self.PREV]
        link = [last, self.tail, key, time.time(), value]
        self.mapping[key] = last[self.NEXT] = self.tail[self.PREV] = link
        return value

    def get_cached(self, link, *key):
        link_prev, link_next, key, cached_at, value = link
        if cached_at + self.maxtime < time.time():
            raise KeyError('%r has timed out' % (key,))
        link_prev[self.NEXT] = link_next
        link_next[self.PREV] = link_prev
        last = self.tail[self.PREV]
        last[self.NEXT] = self.tail[self.PREV] = link
        link[self.PREV] = last
        link[self.NEXT] = self.tail
        return value

    def __call__(self, f):

        class LRUCacheWrapped(object):

            @functools.wraps(f)
            def __call__(im_self, *key):
                link = self.mapping.get(key, self.head)
                if link is not self.head:
                    try:
                        return self.get_cached(link, *key)
                    except KeyError:
                        pass
                value = f(*key)
                self.set_cache(value, *key)
                return value

            def size(im_self):
                """
                Return the size of the cache
                """
                return len(self.mapping)

            def reset(im_self):
                return self.reset()

            def get_maxsize(im_self):
                return self.maxsize

            def set_maxsize(im_self, i):
                self.maxsize = i

            def get_maxtime(im_self):
                return self.maxtime

            def set_maxtime(im_self, i):
                self.maxtime = i

            maxsize = property(get_maxsize, set_maxsize)
            maxtime = property(get_maxtime, set_maxtime)

            def __repr__(im_self):
                return '<%s %r>' % (im_self.__class__.__name__, f)

        return LRUCacheWrapped()


class Spliterator(object):
    """
    Takes an iterator yielding sliceable things (e.g. strings or lists) and
    yields subiterators, each yielding up to the requested number of items
    from the source.

    >>> si = Spliterator(["abcde", "fg", "hijkl"])
    >>> ''.join(si.take(4))
    "abcd"
    >>> ''.join(si.take(3))
    "efg"
    >>> ''.join(si.take(1))
    "h"
    >>> ''.join(si.take(3))
    "ijk"
    >>> ''.join(si.take(3))
    "l"  # shorter than requested; this can happen with the last iterator

    """

    def __init__(self, source_iterable):
        self.input_iterator = iter(source_iterable)
        self.leftovers = None
        self.leftovers_index = 0
        self._iterator_in_progress = False

    def take(self, n):
        if self._iterator_in_progress:
            raise ValueError(
                "cannot call take() again until the first iterator is"
                " exhausted (has raised StopIteration)")
        self._iterator_in_progress = True

        try:
            if self.leftovers:
                # All this string slicing is a little awkward, but it's for
                # a good reason. Consider a length N string that someone is
                # taking k bytes at a time.
                #
                # With this implementation, we create one new string of
                # length k (copying the bytes) on each call to take(). Once
                # the whole input has been consumed, each byte has been
                # copied exactly once, giving O(N) bytes copied.
                #
                # If, instead of this, we were to set leftovers =
                # leftovers[k:] and omit leftovers_index, then each call to
                # take() would copy k bytes to create the desired substring,
                # then copy all the remaining bytes to reset leftovers,
                # resulting in an overall O(N^2) bytes copied.
                llen = len(self.leftovers) - self.leftovers_index
                if llen <= n:
                    n -= llen
                    to_yield = self.leftovers[self.leftovers_index:]
                    self.leftovers = None
                    self.leftovers_index = 0
                    yield to_yield
                else:
                    to_yield = self.leftovers[
                        self.leftovers_index:(self.leftovers_index + n)]
                    self.leftovers_index += n
                    n = 0
                    yield to_yield

            while n > 0:
                try:
                    chunk = next(self.input_iterator)
                except StopIteration:
                    return
                cl = len(chunk)
                if cl <= n:
                    n -= cl
                    yield chunk
                else:
                    self.leftovers = chunk
                    self.leftovers_index = n
                    yield chunk[:n]
                    n = 0
        finally:
            self._iterator_in_progress = False


def ismount(path):
    """
    Test whether a path is a mount point. This will catch any
    exceptions and translate them into a False return value
    Use ismount_raw to have the exceptions raised instead.
    """
    try:
        return ismount_raw(path)
    except OSError:
        return False


def ismount_raw(path):
    """
    Test whether a path is a mount point. Whereas ismount will catch
    any exceptions and just return False, this raw version will not
    catch exceptions.

    This is code hijacked from C Python 2.6.8, adapted to remove the extra
    lstat() system call.
    """
    try:
        s1 = os.lstat(path)
    except os.error as err:
        if err.errno == errno.ENOENT:
            # It doesn't exist -- so not a mount point :-)
            return False
        raise

    if stat.S_ISLNK(s1.st_mode):
        # Some environments (like vagrant-swift-all-in-one) use a symlink at
        # the device level but could still provide a stubfile in the target
        # to indicate that it should be treated as a mount point for swift's
        # purposes.
        if os.path.isfile(os.path.join(path, ".ismount")):
            return True
        # Otherwise, a symlink can never be a mount point
        return False

    s2 = os.lstat(os.path.join(path, '..'))
    dev1 = s1.st_dev
    dev2 = s2.st_dev
    if dev1 != dev2:
        # path/.. on a different device as path
        return True

    ino1 = s1.st_ino
    ino2 = s2.st_ino
    if ino1 == ino2:
        # path/.. is the same i-node as path
        return True

    # Device and inode checks are not properly working inside containerized
    # environments, therefore using a workaround to check if there is a
    # stubfile placed by an operator
    if os.path.isfile(os.path.join(path, ".ismount")):
        return True

    return False


def close_if_possible(maybe_closable):
    close_method = getattr(maybe_closable, 'close', None)
    if callable(close_method):
        return close_method()


@contextmanager
def closing_if_possible(maybe_closable):
    """
    Like contextlib.closing(), but doesn't crash if the object lacks a close()
    method.

    PEP 333 (WSGI) says: "If the iterable returned by the application has a
    close() method, the server or gateway must call that method upon
    completion of the current request[.]" This function makes that easier.
    """
    try:
        yield maybe_closable
    finally:
        close_if_possible(maybe_closable)


def drain_and_close(response_or_app_iter, read_limit=None):
    """
    Drain and close a swob or WSGI response.

    This ensures we don't log a 499 in the proxy just because we realized we
    don't care about the body of an error.
    """
    app_iter = getattr(response_or_app_iter, 'app_iter', response_or_app_iter)
    if app_iter is None:  # for example, if we used the Response.body property
        return
    bytes_read = 0
    with closing_if_possible(app_iter):
        for chunk in app_iter:
            bytes_read += len(chunk)
            if read_limit is not None and bytes_read >= read_limit:
                break


def friendly_close(resp):
    """
    Close a swob or WSGI response and maybe drain it.

    It's basically free to "read" a HEAD or HTTPException response - the bytes
    are probably already in our network buffers.  For a larger response we
    could possibly burn a lot of CPU/network trying to drain an un-used
    response.  This method will read up to DEFAULT_DRAIN_LIMIT bytes to avoid
    logging a 499 in the proxy when it would otherwise be easy to just throw
    away the small/empty body.
    """
    return drain_and_close(resp, read_limit=DEFAULT_DRAIN_LIMIT)


_rfc_token = r'[^()<>@,;:\"/\[\]?={}\x00-\x20\x7f]+'  # nosec B105
_rfc_extension_pattern = re.compile(
    r'(?:\s*;\s*(' + _rfc_token + r")\s*(?:=\s*(" + _rfc_token +
    r'|"(?:[^"\\]|\\.)*"))?)')

_loose_token = r'[^()<>@,;:\"\[\]?={}\x00-\x20\x7f]+'  # nosec B105
_loose_extension_pattern = re.compile(
    r'(?:\s*;\s*(' + _loose_token + r")\s*(?:=\s*(" + _loose_token +
    r'|"(?:[^"\\]|\\.)*"))?)')

_content_range_pattern = re.compile(r'^bytes (\d+)-(\d+)/(\d+)$')


def parse_content_range(content_range):
    """
    Parse a content-range header into (first_byte, last_byte, total_size).

    See RFC 7233 section 4.2 for details on the header format, but it's
    basically "Content-Range: bytes ${start}-${end}/${total}".

    :param content_range: Content-Range header value to parse,
        e.g. "bytes 100-1249/49004"
    :returns: 3-tuple (start, end, total)
    :raises ValueError: if malformed
    """
    found = re.search(_content_range_pattern, content_range)
    if not found:
        raise ValueError("malformed Content-Range %r" % (content_range,))
    return tuple(int(x) for x in found.groups())


def parse_content_type(content_type, strict=True):
    """
    Parse a content-type and its parameters into values.
    RFC 2616 sec 14.17 and 3.7 are pertinent.

    **Examples**::

        'text/plain; charset=UTF-8' -> ('text/plain', [('charset, 'UTF-8')])
        'text/plain; charset=UTF-8; level=1' ->
            ('text/plain', [('charset, 'UTF-8'), ('level', '1')])

    :param content_type: content_type to parse
    :param strict: ignore ``/`` and any following characters in parameter
        tokens. If ``strict`` is True a parameter such as ``x=a/b`` will be
        parsed as ``x=a``. If ``strict`` is False a parameter such as ``x=a/b``
        will be parsed as ``x=a/b``. The default is True.
    :returns: a tuple containing (content type, list of k, v parameter tuples)
    """
    parm_list = []
    if ';' in content_type:
        content_type, parms = content_type.split(';', 1)
        parms = ';' + parms
        pat = _rfc_extension_pattern if strict else _loose_extension_pattern
        for m in pat.findall(parms):
            key = m[0].strip()
            value = m[1].strip()
            parm_list.append((key, value))
    return content_type, parm_list


def parse_header(value):
    """
    Parse a header value to extract the first part and a dict of any
    following parameters.

    The ``value`` to parse should be of the form:

        ``<first part>[;<key>=<value>][; <key>=<value>]...``

    ``<first part>`` should be of the form ``<token>[/<token>]``, ``<key>``
    should be a ``token``, and ``<value>`` should be either a ``token`` or
    ``quoted-string``, where ``token`` and ``quoted-string`` are defined by RFC
    2616 section 2.2.

    :param value: the header value to parse.
    :return: a tuple (first part, dict(params)).
    """
    # note: this does not behave *exactly* like cgi.parse_header (which this
    # replaces) w.r.t. parsing non-token characters in param values (e.g. the
    # null character) , but it's sufficient for our use cases.
    token, params = parse_content_type(value, strict=False)
    return token, dict(params)


def extract_swift_bytes(content_type):
    """
    Parse a content-type and return a tuple containing:
        - the content_type string minus any swift_bytes param,
        -  the swift_bytes value or None if the param was not found

    :param content_type: a content-type string
    :return: a tuple of (content-type, swift_bytes or None)
    """
    content_type, params = parse_content_type(content_type)
    swift_bytes = None
    for k, v in params:
        if k == 'swift_bytes':
            swift_bytes = v
        else:
            content_type += ';%s=%s' % (k, v)
    return content_type, swift_bytes


def override_bytes_from_content_type(listing_dict, logger=None):
    """
    Takes a dict from a container listing and overrides the content_type,
    bytes fields if swift_bytes is set.
    """
    listing_dict['content_type'], swift_bytes = extract_swift_bytes(
        listing_dict['content_type'])
    if swift_bytes is not None:
        try:
            listing_dict['bytes'] = int(swift_bytes)
        except ValueError:
            if logger:
                logger.exception("Invalid swift_bytes")


def clean_content_type(value):
    if ';' in value:
        left, right = value.rsplit(';', 1)
        if right.lstrip().startswith('swift_bytes='):
            return left
    return value


class _MultipartMimeFileLikeObject(object):

    def __init__(self, wsgi_input, boundary, input_buffer, read_chunk_size):
        self.no_more_data_for_this_file = False
        self.no_more_files = False
        self.wsgi_input = wsgi_input
        self.boundary = boundary
        self.input_buffer = input_buffer
        self.read_chunk_size = read_chunk_size

    def read(self, length=None):
        if not length:
            length = self.read_chunk_size
        if self.no_more_data_for_this_file:
            return b''

        # read enough data to know whether we're going to run
        # into a boundary in next [length] bytes
        if len(self.input_buffer) < length + len(self.boundary) + 2:
            to_read = length + len(self.boundary) + 2
            while to_read > 0:
                try:
                    chunk = self.wsgi_input.read(to_read)
                except (IOError, ValueError) as e:
                    raise swift.common.exceptions.ChunkReadError(str(e))
                to_read -= len(chunk)
                self.input_buffer += chunk
                if not chunk:
                    self.no_more_files = True
                    break

        boundary_pos = self.input_buffer.find(self.boundary)

        # boundary does not exist in the next (length) bytes
        if boundary_pos == -1 or boundary_pos > length:
            ret = self.input_buffer[:length]
            self.input_buffer = self.input_buffer[length:]
        # if it does, just return data up to the boundary
        else:
            ret, self.input_buffer = self.input_buffer.split(self.boundary, 1)
            self.no_more_files = self.input_buffer.startswith(b'--')
            self.no_more_data_for_this_file = True
            self.input_buffer = self.input_buffer[2:]
        return ret

    def readline(self):
        if self.no_more_data_for_this_file:
            return b''
        boundary_pos = newline_pos = -1
        while newline_pos < 0 and boundary_pos < 0:
            try:
                chunk = self.wsgi_input.read(self.read_chunk_size)
            except (IOError, ValueError) as e:
                raise swift.common.exceptions.ChunkReadError(str(e))
            self.input_buffer += chunk
            newline_pos = self.input_buffer.find(b'\r\n')
            boundary_pos = self.input_buffer.find(self.boundary)
            if not chunk:
                self.no_more_files = True
                break
        # found a newline
        if newline_pos >= 0 and \
                (boundary_pos < 0 or newline_pos < boundary_pos):
            # Use self.read to ensure any logic there happens...
            ret = b''
            to_read = newline_pos + 2
            while to_read > 0:
                chunk = self.read(to_read)
                # Should never happen since we're reading from input_buffer,
                # but just for completeness...
                if not chunk:
                    break
                to_read -= len(chunk)
                ret += chunk
            return ret
        else:  # no newlines, just return up to next boundary
            return self.read(len(self.input_buffer))


def iter_multipart_mime_documents(wsgi_input, boundary, read_chunk_size=4096):
    """
    Given a multi-part-mime-encoded input file object and boundary,
    yield file-like objects for each part. Note that this does not
    split each part into headers and body; the caller is responsible
    for doing that if necessary.

    :param wsgi_input: The file-like object to read from.
    :param boundary: The mime boundary to separate new file-like objects on.
    :returns: A generator of file-like objects for each part.
    :raises MimeInvalid: if the document is malformed
    """
    boundary = b'--' + boundary
    blen = len(boundary) + 2  # \r\n
    try:
        got = wsgi_input.readline(blen)
        while got == b'\r\n':
            got = wsgi_input.readline(blen)
    except (IOError, ValueError) as e:
        raise swift.common.exceptions.ChunkReadError(str(e))

    if got.strip() != boundary:
        raise swift.common.exceptions.MimeInvalid(
            'invalid starting boundary: wanted %r, got %r' % (boundary, got))
    boundary = b'\r\n' + boundary
    input_buffer = b''
    done = False
    while not done:
        it = _MultipartMimeFileLikeObject(wsgi_input, boundary, input_buffer,
                                          read_chunk_size)
        yield it
        done = it.no_more_files
        input_buffer = it.input_buffer


def parse_mime_headers(doc_file):
    """
    Takes a file-like object containing a MIME document and returns a
    HeaderKeyDict containing the headers. The body of the message is not
    consumed: the position in doc_file is left at the beginning of the body.

    This function was inspired by the Python standard library's
    http.client.parse_headers.

    :param doc_file: binary file-like object containing a MIME document
    :returns: a swift.common.swob.HeaderKeyDict containing the headers
    """
    headers = []
    while True:
        line = doc_file.readline()
        done = line in (b'\r\n', b'\n', b'')
        try:
            line = line.decode('utf-8')
        except UnicodeDecodeError:
            line = line.decode('latin1')
        headers.append(line)
        if done:
            break
    header_string = ''.join(headers)
    headers = email.parser.Parser().parsestr(header_string)
    return HeaderKeyDict(headers)


def mime_to_document_iters(input_file, boundary, read_chunk_size=4096):
    """
    Takes a file-like object containing a multipart MIME document and
    returns an iterator of (headers, body-file) tuples.

    :param input_file: file-like object with the MIME doc in it
    :param boundary: MIME boundary, sans dashes
        (e.g. "divider", not "--divider")
    :param read_chunk_size: size of strings read via input_file.read()
    """
    if isinstance(boundary, str):
        # Since the boundary is in client-supplied headers, it can contain
        # garbage that trips us and we don't like client-induced 500.
        boundary = boundary.encode('latin-1', errors='replace')
    doc_files = iter_multipart_mime_documents(input_file, boundary,
                                              read_chunk_size)
    for i, doc_file in enumerate(doc_files):
        # this consumes the headers and leaves just the body in doc_file
        headers = parse_mime_headers(doc_file)
        yield (headers, doc_file)


def maybe_multipart_byteranges_to_document_iters(app_iter, content_type):
    """
    Takes an iterator that may or may not contain a multipart MIME document
    as well as content type and returns an iterator of body iterators.

    :param app_iter: iterator that may contain a multipart MIME document
    :param content_type: content type of the app_iter, used to determine
                         whether it conains a multipart document and, if
                         so, what the boundary is between documents
    """
    content_type, params_list = parse_content_type(content_type)
    if content_type != 'multipart/byteranges':
        yield app_iter
        return

    body_file = FileLikeIter(app_iter)
    boundary = dict(params_list)['boundary']
    for _headers, body in mime_to_document_iters(body_file, boundary):
        yield (chunk for chunk in iter(lambda: body.read(65536), b''))


def document_iters_to_multipart_byteranges(ranges_iter, boundary):
    """
    Takes an iterator of range iters and yields a multipart/byteranges MIME
    document suitable for sending as the body of a multi-range 206 response.

    See document_iters_to_http_response_body for parameter descriptions.
    """
    if not isinstance(boundary, bytes):
        boundary = boundary.encode('ascii')

    divider = b"--" + boundary + b"\r\n"
    terminator = b"--" + boundary + b"--"

    for range_spec in ranges_iter:
        start_byte = range_spec["start_byte"]
        end_byte = range_spec["end_byte"]
        entity_length = range_spec.get("entity_length", "*")
        content_type = range_spec["content_type"]
        part_iter = range_spec["part_iter"]
        if not isinstance(content_type, bytes):
            content_type = str(content_type).encode('utf-8')
        if not isinstance(entity_length, bytes):
            entity_length = str(entity_length).encode('utf-8')

        part_header = b''.join((
            divider,
            b"Content-Type: ", content_type, b"\r\n",
            b"Content-Range: ", b"bytes %d-%d/%s\r\n" % (
                start_byte, end_byte, entity_length),
            b"\r\n"
        ))
        yield part_header

        for chunk in part_iter:
            yield chunk
        yield b"\r\n"
    yield terminator


class StringAlong(ClosingIterator):
    """
    This iterator wraps and iterates over a first iterator until it stops, and
    then iterates a second iterator, expecting it to stop immediately. This
    "stringing along" of the second iterator is useful when the exit of the
    second iterator must be delayed until the first iterator has stopped. For
    example, when the second iterator has already yielded its item(s) but
    has resources that mustn't be garbage collected until the first iterator
    has stopped.

    The second iterator is expected to have no more items and raise
    StopIteration when called. If this is not the case then
    ``unexpected_items_func`` is called.

    :param iterable: a first iterator that is wrapped and iterated.
    :param other_iter: a second iterator that is stopped once the first
        iterator has stopped.
    :param unexpected_items_func: a no-arg function that will be called if the
        second iterator is found to have remaining items.
    """
    __slots__ = ('other_iter', 'unexpected_items_func')

    def __init__(self, iterable, other_iter, unexpected_items_func):
        super(StringAlong, self).__init__(iterable, [other_iter])
        self.other_iter = other_iter
        self.unexpected_items_func = unexpected_items_func

    def _get_next_item(self):
        try:
            return super(StringAlong, self)._get_next_item()
        except StopIteration:
            try:
                next(self.other_iter)
            except StopIteration:
                pass
            else:
                self.unexpected_items_func()
            finally:
                raise


def document_iters_to_http_response_body(ranges_iter, boundary, multipart,
                                         logger):
    """
    Takes an iterator of range iters and turns it into an appropriate
    HTTP response body, whether that's multipart/byteranges or not.

    This is almost, but not quite, the inverse of
    request_helpers.http_response_to_document_iters(). This function only
    yields chunks of the body, not any headers.

    :param ranges_iter: an iterator of dictionaries, one per range.
        Each dictionary must contain at least the following key:
        "part_iter": iterator yielding the bytes in the range

        Additionally, if multipart is True, then the following other keys
        are required:

        "start_byte": index of the first byte in the range
        "end_byte": index of the last byte in the range
        "content_type": value for the range's Content-Type header

        Finally, there is one optional key that is used in the
            multipart/byteranges case:

        "entity_length": length of the requested entity (not necessarily
            equal to the response length). If omitted, "*" will be used.

        Each part_iter will be exhausted prior to calling next(ranges_iter).

    :param boundary: MIME boundary to use, sans dashes (e.g. "boundary", not
        "--boundary").
    :param multipart: True if the response should be multipart/byteranges,
        False otherwise. This should be True if and only if you have 2 or
        more ranges.
    :param logger: a logger
    """
    if multipart:
        return document_iters_to_multipart_byteranges(ranges_iter, boundary)
    else:
        try:
            response_body_iter = next(ranges_iter)['part_iter']
        except StopIteration:
            return ''

        # We need to make sure ranges_iter does not get garbage-collected
        # before response_body_iter is exhausted. The reason is that
        # ranges_iter has a finally block that calls close_swift_conn, and
        # so if that finally block fires before we read response_body_iter,
        # there's nothing there.
        result = StringAlong(
            response_body_iter, ranges_iter,
            lambda: logger.warning(
                "More than one part in a single-part response?"))
        return result


def multipart_byteranges_to_document_iters(input_file, boundary,
                                           read_chunk_size=4096):
    """
    Takes a file-like object containing a multipart/byteranges MIME document
    (see RFC 7233, Appendix A) and returns an iterator of (first-byte,
    last-byte, length, document-headers, body-file) 5-tuples.

    :param input_file: file-like object with the MIME doc in it
    :param boundary: MIME boundary, sans dashes
        (e.g. "divider", not "--divider")
    :param read_chunk_size: size of strings read via input_file.read()
    """
    for headers, body in mime_to_document_iters(input_file, boundary,
                                                read_chunk_size):
        first_byte, last_byte, length = parse_content_range(
            headers.get('content-range'))
        yield (first_byte, last_byte, length, headers.items(), body)


#: Regular expression to match form attributes.
ATTRIBUTES_RE = re.compile(r'(\w+)=(".*?"|[^";]+)(; ?|$)')


def parse_content_disposition(header):
    """
    Given the value of a header like:
    Content-Disposition: form-data; name="somefile"; filename="test.html"

    Return data like
    ("form-data", {"name": "somefile", "filename": "test.html"})

    :param header: Value of a header (the part after the ': ').
    :returns: (value name, dict) of the attribute data parsed (see above).
    """
    attributes = {}
    attrs = ''
    if ';' in header:
        header, attrs = [x.strip() for x in header.split(';', 1)]
    m = True
    while m:
        m = ATTRIBUTES_RE.match(attrs)
        if m:
            attrs = attrs[len(m.group(0)):]
            attributes[m.group(1)] = m.group(2).strip('"')
    return header, attributes


class NamespaceOuterBound(object):
    """
    A custom singleton type to be subclassed for the outer bounds of
    Namespaces.
    """
    _singleton = None

    def __new__(cls):
        if cls is NamespaceOuterBound:
            raise TypeError('NamespaceOuterBound is an abstract class; '
                            'only subclasses should be instantiated')
        if cls._singleton is None:
            cls._singleton = super(NamespaceOuterBound, cls).__new__(cls)
        return cls._singleton

    def __str__(self):
        return ''

    def __repr__(self):
        return type(self).__name__

    def __bool__(self):
        return False


@functools.total_ordering
class Namespace(object):
    """
    A Namespace encapsulates parameters that define a range of the object
    namespace.

    :param name: the name of the ``Namespace``; this SHOULD take the form of a
        path to a container i.e. <account_name>/<container_name>.
    :param lower: the lower bound of object names contained in the namespace;
        the lower bound *is not* included in the namespace.
    :param upper: the upper bound of object names contained in the namespace;
        the upper bound *is* included in the namespace.
    """
    __slots__ = ('_lower', '_upper', '_name')

    @functools.total_ordering
    class MaxBound(NamespaceOuterBound):
        # singleton for maximum bound
        def __ge__(self, other):
            return True

    @functools.total_ordering
    class MinBound(NamespaceOuterBound):
        # singleton for minimum bound
        def __le__(self, other):
            return True

    MIN = MinBound()
    MAX = MaxBound()

    def __init__(self, name, lower, upper):
        self._lower = Namespace.MIN
        self._upper = Namespace.MAX
        # We deliberately do not validate that the name has the form 'a/c'
        # because we want Namespace instantiation to be fast. Namespaces are
        # typically created using state that has previously been serialized
        # from a ShardRange instance, and the ShardRange will have validated
        # the name format.
        self._name = self._encode(name)
        self.lower = lower
        self.upper = upper

    def __iter__(self):
        yield 'name', str(self.name)
        yield 'lower', self.lower_str
        yield 'upper', self.upper_str

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            '%s=%r' % prop for prop in self))

    def __lt__(self, other):
        # a Namespace is less than other if its entire namespace is less than
        # other; if other is another Namespace that implies that this
        # Namespace's upper must be less than or equal to the other
        # Namespace's lower
        if self.upper == Namespace.MAX:
            return False
        if isinstance(other, Namespace):
            return self.upper <= other.lower
        elif other is None:
            return True
        else:
            return self.upper < self._encode(other)

    def __gt__(self, other):
        # a Namespace is greater than other if its entire namespace is greater
        # than other; if other is another Namespace that implies that this
        # Namespace's lower must be greater than or equal to the other
        # Namespace's upper
        if self.lower == Namespace.MIN:
            return False
        if isinstance(other, Namespace):
            return self.lower >= other.upper
        elif other is None:
            return False
        else:
            return self.lower >= self._encode(other)

    def __eq__(self, other):
        # test for equality of range bounds only
        if not isinstance(other, Namespace):
            return False
        return self.lower == other.lower and self.upper == other.upper

    def __ne__(self, other):
        return not (self == other)

    def __contains__(self, item):
        # test if the given item is within the namespace
        if item == '':
            return False
        item = self._encode_bound(item)
        return self.lower < item <= self.upper

    @classmethod
    def _encode(cls, value):
        if isinstance(value, bytes):
            # This should never fail -- the value should always be coming from
            # valid swift paths, which means UTF-8
            return value.decode('utf-8')
        return value

    def _encode_bound(self, bound):
        if isinstance(bound, NamespaceOuterBound):
            return bound
        if not (isinstance(bound, str) or
                isinstance(bound, bytes)):
            raise TypeError('must be a string type')
        return self._encode(bound)

    @property
    def account(self):
        return self._name.split('/')[0]

    @property
    def container(self):
        # note: this may raise an IndexError if name does not have the expected
        # form 'a/c'; that is a deliberate trade-off against the overhead of
        # validating the name every time a Namespace is instantiated.
        return self._name.split('/')[1]

    @property
    def name(self):
        return self._name

    @property
    def lower(self):
        return self._lower

    @property
    def lower_str(self):
        return str(self.lower)

    @lower.setter
    def lower(self, value):
        if value is None or (value == b"" if isinstance(value, bytes) else
                             value == u""):
            value = Namespace.MIN
        try:
            value = self._encode_bound(value)
        except TypeError as err:
            raise TypeError('lower %s' % err)
        if value > self._upper:
            raise ValueError(
                'lower (%r) must be less than or equal to upper (%r)' %
                (value, self.upper))
        self._lower = value

    @property
    def upper(self):
        return self._upper

    @property
    def upper_str(self):
        return str(self.upper)

    @upper.setter
    def upper(self, value):
        if value is None or (value == b"" if isinstance(value, bytes) else
                             value == u""):
            value = Namespace.MAX
        try:
            value = self._encode_bound(value)
        except TypeError as err:
            raise TypeError('upper %s' % err)
        if value < self._lower:
            raise ValueError(
                'upper (%r) must be greater than or equal to lower (%r)' %
                (value, self.lower))
        self._upper = value

    @property
    def end_marker(self):
        return self.upper_str + '\x00' if self.upper else ''

    def entire_namespace(self):
        """
        Returns True if this namespace includes the entire namespace, False
        otherwise.
        """
        return (self.lower == Namespace.MIN and
                self.upper == Namespace.MAX)

    def overlaps(self, other):
        """
        Returns True if this namespace overlaps with the other namespace.

        :param other: an instance of :class:`~swift.common.utils.Namespace`
        """
        if not isinstance(other, Namespace):
            return False
        return max(self.lower, other.lower) < min(self.upper, other.upper)

    def includes(self, other):
        """
        Returns True if this namespace includes the whole of the other
        namespace, False otherwise.

        :param other: an instance of :class:`~swift.common.utils.Namespace`
        """
        return (self.lower <= other.lower) and (other.upper <= self.upper)

    def expand(self, donors):
        """
        Expands the bounds as necessary to match the minimum and maximum bounds
        of the given donors.

        :param donors: A list of :class:`~swift.common.utils.Namespace`
        :return: True if the bounds have been modified, False otherwise.
        """
        modified = False
        new_lower = self.lower
        new_upper = self.upper
        for donor in donors:
            new_lower = min(new_lower, donor.lower)
            new_upper = max(new_upper, donor.upper)
        if self.lower > new_lower or self.upper < new_upper:
            self.lower = new_lower
            self.upper = new_upper
            modified = True
        return modified


class NamespaceBoundList(object):
    def __init__(self, bounds):
        """
        Encapsulate a compact representation of namespaces. Each item in the
        list is a list [lower bound, name].

        :param bounds: a list of lists ``[lower bound, name]``. The list
            should be ordered by ``lower bound``.
        """
        self.bounds = [] if bounds is None else bounds

    def __eq__(self, other):
        # test for equality of NamespaceBoundList objects only
        if not isinstance(other, NamespaceBoundList):
            return False
        return self.bounds == other.bounds

    @classmethod
    def parse(cls, namespaces):
        """
        Create a NamespaceBoundList object by parsing a list of Namespaces or
        shard ranges and only storing the compact bounds list.

        Each Namespace in the given list of ``namespaces`` provides the next
        [lower bound, name] list to append to the NamespaceBoundList. The
        given ``namespaces`` should be contiguous because the
        NamespaceBoundList only stores lower bounds; if ``namespaces`` has
        overlaps then at least one of the overlapping namespaces may be
        ignored; similarly, gaps between namespaces are not represented in the
        NamespaceBoundList.

        :param namespaces: A list of Namespace instances. The list should be
            ordered by namespace bounds.
        :return: a NamespaceBoundList.
        """
        if not namespaces:
            return None
        bounds = []
        upper = namespaces[0].lower
        for ns in namespaces:
            if ns.lower < upper:
                # Discard overlapping namespace.
                # Overlapping namespaces are expected in lists of shard ranges
                # fetched from the backend. For example, while a parent
                # container is in the process of sharding, the parent shard
                # range and its children shard ranges may be returned in the
                # list of shard ranges. However, the backend sorts the list by
                # (upper, state, lower, name) such that the children precede
                # the parent, and it is the children that we prefer to retain
                # in the NamespaceBoundList. For example, these namespaces:
                #   (a-b, "child1"), (b-c, "child2"), (a-c, "parent")
                # would result in a NamespaceBoundList:
                #   (a, "child1"), (b, "child2")
                # Unexpected overlaps or gaps may result in namespaces being
                # 'extended' because only lower bounds are stored. For example,
                # these namespaces:
                #   (a-b, "ns1"), (d-e, "ns2")
                # would result in a NamespaceBoundList:
                #   (a, "ns1"), (d, "ns2")
                # When used to find a target namespace for an object update
                # that lies in a gap, the NamespaceBoundList will map the
                # object name to the preceding namespace. In the example, an
                # object named "c" would be mapped to "ns1". (In previous
                # versions, an object update lying in a gap would have been
                # mapped to the root container.)
                continue
            bounds.append([ns.lower_str, str(ns.name)])
            upper = ns.upper
        return cls(bounds)

    def get_namespace(self, item):
        """
        Get a Namespace instance that contains ``item`` by bisecting on the
        lower bounds directly. This function is used for performance sensitive
        path, for example, '_get_update_shard' in proxy object controller. For
        normal paths, convert NamespaceBoundList to a list of Namespaces, and
        use `~swift.common.utils.find_namespace` or
        `~swift.common.utils.filter_namespaces`.

        :param item: The item for a which a Namespace is to be found.
        :return: the Namespace that contains ``item``.
        """
        pos = bisect.bisect(self.bounds, [item]) - 1
        lower, name = self.bounds[pos]
        upper = ('' if pos + 1 == len(self.bounds)
                 else self.bounds[pos + 1][0])
        return Namespace(name, lower, upper)

    def get_namespaces(self):
        """
        Get the contained namespaces as a list of contiguous Namespaces ordered
        by lower bound.

        :return: A list of Namespace objects which are ordered by
            ``lower bound``.
        """
        if not self.bounds:
            return []
        namespaces = []
        num_ns = len(self.bounds)
        for i in range(num_ns):
            lower, name = self.bounds[i]
            upper = ('' if i + 1 == num_ns else self.bounds[i + 1][0])
            namespaces.append(Namespace(name, lower, upper))
        return namespaces


class ShardName(object):
    """
    Encapsulates the components of a shard name.

    Instances of this class would typically be constructed via the create() or
    parse() class methods.

    Shard names have the form:

        <account>/<root_container>-<parent_container_hash>-<timestamp>-<index>

    Note: some instances of :class:`~swift.common.utils.ShardRange` have names
    that will NOT parse as a :class:`~swift.common.utils.ShardName`; e.g. a
    root container's own shard range will have a name format of
    <account>/<root_container> which will raise ValueError if passed to parse.
    """

    def __init__(self, account, root_container,
                 parent_container_hash,
                 timestamp,
                 index):
        self.account = self._validate(account)
        self.root_container = self._validate(root_container)
        self.parent_container_hash = self._validate(parent_container_hash)
        self.timestamp = Timestamp(timestamp)
        self.index = int(index)

    @classmethod
    def _validate(cls, arg):
        if arg is None:
            raise ValueError('arg must not be None')
        return arg

    def __str__(self):
        return '%s/%s-%s-%s-%s' % (self.account,
                                   self.root_container,
                                   self.parent_container_hash,
                                   self.timestamp.internal,
                                   self.index)

    @classmethod
    def hash_container_name(cls, container_name):
        """
        Calculates the hash of a container name.

        :param container_name: name to be hashed.
        :return: the hexdigest of the md5 hash of ``container_name``.
        :raises ValueError: if ``container_name`` is None.
        """
        cls._validate(container_name)
        if not isinstance(container_name, bytes):
            container_name = container_name.encode('utf-8')
        hash = md5(container_name, usedforsecurity=False).hexdigest()
        return hash

    @classmethod
    def create(cls, account, root_container, parent_container,
               timestamp, index):
        """
        Create an instance of :class:`~swift.common.utils.ShardName`.

        :param account: the hidden internal account to which the shard
            container belongs.
        :param root_container: the name of the root container for the shard.
        :param parent_container: the name of the parent container for the
            shard; for initial first generation shards this should be the same
            as ``root_container``; for shards of shards this should be the name
            of the sharding shard container.
        :param timestamp: an instance of :class:`~swift.common.utils.Timestamp`
        :param index: a unique index that will distinguish the path from any
            other path generated using the same combination of
            ``account``, ``root_container``, ``parent_container`` and
            ``timestamp``.

        :return: an instance of :class:`~swift.common.utils.ShardName`.
        :raises ValueError: if any argument is None
        """
        # we make the shard name unique with respect to other shards names by
        # embedding a hash of the parent container name; we use a hash (rather
        # than the actual parent container name) to prevent shard names become
        # longer with every generation.
        parent_container_hash = cls.hash_container_name(parent_container)
        return cls(account, root_container, parent_container_hash, timestamp,
                   index)

    @classmethod
    def parse(cls, name):
        """
        Parse ``name`` to an instance of
        :class:`~swift.common.utils.ShardName`.

        :param name: a shard name which should have the form:
            <account>/
            <root_container>-<parent_container_hash>-<timestamp>-<index>

        :return: an instance of :class:`~swift.common.utils.ShardName`.
        :raises ValueError: if ``name`` is not a valid shard name.
        """
        try:
            account, container = name.split('/', 1)
            root_container, parent_container_hash, timestamp, index = \
                container.rsplit('-', 3)
            return cls(account, root_container, parent_container_hash,
                       timestamp, index)
        except ValueError:
            raise ValueError('invalid name: %s' % name)


class ShardRange(Namespace):
    """
    A ShardRange encapsulates sharding state related to a container including
    lower and upper bounds that define the object namespace for which the
    container is responsible.

    Shard ranges may be persisted in a container database. Timestamps
    associated with subsets of the shard range attributes are used to resolve
    conflicts when a shard range needs to be merged with an existing shard
    range record and the most recent version of an attribute should be
    persisted.

    :param name: the name of the shard range; this MUST take the form of a
        path to a container i.e. <account_name>/<container_name>.
    :param timestamp: a timestamp that represents the time at which the
        shard range's ``lower``, ``upper`` or ``deleted`` attributes were
        last modified.
    :param lower: the lower bound of object names contained in the shard range;
        the lower bound *is not* included in the shard range namespace.
    :param upper: the upper bound of object names contained in the shard range;
        the upper bound *is* included in the shard range namespace.
    :param object_count: the number of objects in the shard range; defaults to
        zero.
    :param bytes_used: the number of bytes in the shard range; defaults to
        zero.
    :param meta_timestamp: a timestamp that represents the time at which the
        shard range's ``object_count`` and ``bytes_used`` were last updated;
        defaults to the value of ``timestamp``.
    :param deleted: a boolean; if True the shard range is considered to be
        deleted.
    :param state: the state; must be one of ShardRange.STATES; defaults to
        CREATED.
    :param state_timestamp: a timestamp that represents the time at which
        ``state`` was forced to its current value; defaults to the value of
        ``timestamp``. This timestamp is typically not updated with every
        change of ``state`` because in general conflicts in ``state``
        attributes are resolved by choosing the larger ``state`` value.
        However, when this rule does not apply, for example when changing state
        from ``SHARDED`` to ``ACTIVE``, the ``state_timestamp`` may be advanced
        so that the new ``state`` value is preferred over any older ``state``
        value.
    :param epoch: optional epoch timestamp which represents the time at which
        sharding was enabled for a container.
    :param reported: optional indicator that this shard and its stats have
        been reported to the root container.
    :param tombstones: the number of tombstones in the shard range; defaults to
        -1 to indicate that the value is unknown.
    """
    FOUND = 10
    CREATED = 20
    CLEAVED = 30
    ACTIVE = 40
    SHRINKING = 50
    SHARDING = 60
    SHARDED = 70
    SHRUNK = 80
    STATES = {FOUND: 'found',
              CREATED: 'created',
              CLEAVED: 'cleaved',
              ACTIVE: 'active',
              SHRINKING: 'shrinking',
              SHARDING: 'sharding',
              SHARDED: 'sharded',
              SHRUNK: 'shrunk'}
    STATES_BY_NAME = dict((v, k) for k, v in STATES.items())
    SHRINKING_STATES = (SHRINKING, SHRUNK)
    SHARDING_STATES = (SHARDING, SHARDED)
    CLEAVING_STATES = SHRINKING_STATES + SHARDING_STATES

    __slots__ = (
        '_timestamp', '_meta_timestamp', '_state_timestamp', '_epoch',
        '_deleted', '_state', '_count', '_bytes',
        '_tombstones', '_reported')

    def __init__(self, name, timestamp=0,
                 lower=Namespace.MIN, upper=Namespace.MAX,
                 object_count=0, bytes_used=0, meta_timestamp=None,
                 deleted=False, state=None, state_timestamp=None, epoch=None,
                 reported=False, tombstones=-1, **kwargs):
        super(ShardRange, self).__init__(name=name, lower=lower, upper=upper)
        self._validate_name(self.name)
        self._timestamp = self._meta_timestamp = self._state_timestamp = \
            self._epoch = None
        self._deleted = False
        self._state = None

        self.timestamp = timestamp
        self.deleted = deleted
        self.object_count = object_count
        self.bytes_used = bytes_used
        self.meta_timestamp = meta_timestamp
        self.state = self.FOUND if state is None else state
        self.state_timestamp = state_timestamp
        self.epoch = epoch
        self.reported = reported
        self.tombstones = tombstones

    @classmethod
    def sort_key(cls, sr):
        return cls.sort_key_order(sr.name, sr.lower, sr.upper, sr.state)

    @staticmethod
    def sort_key_order(name, lower, upper, state):
        # Use Namespace.MaxBound() for upper bound '', this will allow this
        # record to be sorted correctly by upper.
        upper = upper if upper else Namespace.MaxBound()
        # defines the sort order for shard ranges
        # note if this ever changes to *not* sort by upper first then it breaks
        # a key assumption for bisect, which is used by utils.find_namespace
        # with shard ranges.
        return upper, state, lower, name

    def is_child_of(self, parent):
        """
        Test if this shard range is a child of another shard range. The
        parent-child relationship is inferred from the names of the shard
        ranges. This method is limited to work only within the scope of the
        same user-facing account (with and without shard prefix).

        :param parent: an instance of ``ShardRange``.
        :return: True if ``parent`` is the parent of this shard range, False
            otherwise, assuming that they are within the same account.
        """
        # note: We limit the usages of this method to be within the same
        # account, because account shard prefix is configurable and it's hard
        # to perform checking without breaking backward-compatibility.
        try:
            self_parsed_name = ShardName.parse(self.name)
        except ValueError:
            # self is not a shard and therefore not a child.
            return False

        try:
            parsed_parent_name = ShardName.parse(parent.name)
            parent_root_container = parsed_parent_name.root_container
        except ValueError:
            # parent is a root container.
            parent_root_container = parent.container

        return (
            self_parsed_name.root_container == parent_root_container
            and self_parsed_name.parent_container_hash
            == ShardName.hash_container_name(parent.container)
        )

    def _find_root(self, parsed_name, shard_ranges):
        for sr in shard_ranges:
            if parsed_name.root_container == sr.container:
                return sr
        return None

    def find_root(self, shard_ranges):
        """
        Find this shard range's root shard range in the given ``shard_ranges``.

        :param shard_ranges: a list of instances of
            :class:`~swift.common.utils.ShardRange`
        :return: this shard range's root shard range if it is found in the
            list, otherwise None.
        """
        try:
            self_parsed_name = ShardName.parse(self.name)
        except ValueError:
            # not a shard
            return None
        return self._find_root(self_parsed_name, shard_ranges)

    def find_ancestors(self, shard_ranges):
        """
        Find this shard range's ancestor ranges in the given ``shard_ranges``.

        This method makes a best-effort attempt to identify this shard range's
        parent shard range, the parent's parent, etc., up to and including the
        root shard range. It is only possible to directly identify the parent
        of a particular shard range, so the search is recursive; if any member
        of the ancestry is not found then the search ends and older ancestors
        that may be in the list are not identified. The root shard range,
        however, will always be identified if it is present in the list.

        For example, given a list that contains parent, grandparent,
        great-great-grandparent and root shard ranges, but is missing the
        great-grandparent shard range, only the parent, grand-parent and root
        shard ranges will be identified.

        :param shard_ranges: a list of instances of
            :class:`~swift.common.utils.ShardRange`
        :return: a list of instances of
            :class:`~swift.common.utils.ShardRange` containing items in the
            given ``shard_ranges`` that can be identified as ancestors of this
            shard range. The list may not be complete if there are gaps in the
            ancestry, but is guaranteed to contain at least the parent and
            root shard ranges if they are present.
        """
        if not shard_ranges:
            return []

        try:
            self_parsed_name = ShardName.parse(self.name)
        except ValueError:
            # not a shard
            return []

        ancestors = []
        for sr in shard_ranges:
            if self.is_child_of(sr):
                ancestors.append(sr)
                break
        if ancestors:
            ancestors.extend(ancestors[0].find_ancestors(shard_ranges))
        else:
            root_sr = self._find_root(self_parsed_name, shard_ranges)
            if root_sr:
                ancestors.append(root_sr)
        return ancestors

    @classmethod
    def make_path(cls, shards_account, root_container, parent_container,
                  timestamp, index):
        """
        Returns a path for a shard container that is valid to use as a name
        when constructing a :class:`~swift.common.utils.ShardRange`.

        :param shards_account: the hidden internal account to which the shard
            container belongs.
        :param root_container: the name of the root container for the shard.
        :param parent_container: the name of the parent container for the
            shard; for initial first generation shards this should be the same
            as ``root_container``; for shards of shards this should be the name
            of the sharding shard container.
        :param timestamp: an instance of :class:`~swift.common.utils.Timestamp`
        :param index: a unique index that will distinguish the path from any
            other path generated using the same combination of
            ``shards_account``, ``root_container``, ``parent_container`` and
            ``timestamp``.
        :return: a string of the form <account_name>/<container_name>
        """
        timestamp = cls._to_timestamp(timestamp)
        return str(ShardName.create(shards_account,
                                    root_container,
                                    parent_container,
                                    timestamp,
                                    index))

    @classmethod
    def _to_timestamp(cls, timestamp):
        if timestamp is None or isinstance(timestamp, Timestamp):
            return timestamp
        return Timestamp(timestamp)

    @property
    def name(self):
        return self._name

    @staticmethod
    def _validate_name(name):
        # Validate the name format is 'a/c'. The ShardRange class is typically
        # used when shard state is created (e.g. by the sharder or
        # swift-manage-shard-ranges), but it is not typically used in
        # performance sensitive paths (e.g. listing namespaces), so we can
        # afford the overhead of being more defensive here.
        if not name or len(name.split('/')) != 2 or not all(name.split('/')):
            raise ValueError(
                "Name must be of the form '<account>/<container>', got %r" %
                name)
        return name

    @name.setter
    def name(self, name):
        self._name = self._validate_name(self._encode(name))

    @property
    def timestamp(self):
        return self._timestamp

    @timestamp.setter
    def timestamp(self, ts):
        if ts is None:
            raise TypeError('timestamp cannot be None')
        self._timestamp = self._to_timestamp(ts)

    @property
    def meta_timestamp(self):
        if self._meta_timestamp is None:
            return self.timestamp
        return self._meta_timestamp

    @meta_timestamp.setter
    def meta_timestamp(self, ts):
        self._meta_timestamp = self._to_timestamp(ts)

    @property
    def object_count(self):
        return self._count

    @object_count.setter
    def object_count(self, count):
        count = int(count)
        if count < 0:
            raise ValueError('object_count cannot be < 0')
        self._count = count

    @property
    def bytes_used(self):
        return self._bytes

    @bytes_used.setter
    def bytes_used(self, bytes_used):
        bytes_used = int(bytes_used)
        if bytes_used < 0:
            raise ValueError('bytes_used cannot be < 0')
        self._bytes = bytes_used

    @property
    def tombstones(self):
        return self._tombstones

    @tombstones.setter
    def tombstones(self, tombstones):
        self._tombstones = int(tombstones)

    @property
    def row_count(self):
        """
        Returns the total number of rows in the shard range i.e. the sum of
        objects and tombstones.

        :return: the row count
        """
        return self.object_count + max(self.tombstones, 0)

    def update_meta(self, object_count, bytes_used, meta_timestamp=None):
        """
        Set the object stats metadata to the given values and update the
        meta_timestamp to the current time.

        :param object_count: should be an integer
        :param bytes_used: should be an integer
        :param meta_timestamp: timestamp for metadata; if not given the
            current time will be set.
        :raises ValueError: if ``object_count`` or ``bytes_used`` cannot be
            cast to an int, or if meta_timestamp is neither None nor can be
            cast to a :class:`~swift.common.utils.Timestamp`.
        """
        if self.object_count != int(object_count):
            self.object_count = int(object_count)
            self.reported = False

        if self.bytes_used != int(bytes_used):
            self.bytes_used = int(bytes_used)
            self.reported = False

        if meta_timestamp is None:
            self.meta_timestamp = Timestamp.now()
        else:
            self.meta_timestamp = meta_timestamp

    def update_tombstones(self, tombstones, meta_timestamp=None):
        """
        Set the tombstones metadata to the given values and update the
        meta_timestamp to the current time.

        :param tombstones: should be an integer
        :param meta_timestamp: timestamp for metadata; if not given the
            current time will be set.
        :raises ValueError: if ``tombstones`` cannot be cast to an int, or
            if meta_timestamp is neither None nor can be cast to a
            :class:`~swift.common.utils.Timestamp`.
        """
        tombstones = int(tombstones)
        if 0 <= tombstones != self.tombstones:
            self.tombstones = tombstones
            self.reported = False
        if meta_timestamp is None:
            self.meta_timestamp = Timestamp.now()
        else:
            self.meta_timestamp = meta_timestamp

    def increment_meta(self, object_count, bytes_used):
        """
        Increment the object stats metadata by the given values and update the
        meta_timestamp to the current time.

        :param object_count: should be an integer
        :param bytes_used: should be an integer
        :raises ValueError: if ``object_count`` or ``bytes_used`` cannot be
            cast to an int.
        """
        self.update_meta(self.object_count + int(object_count),
                         self.bytes_used + int(bytes_used))

    @classmethod
    def resolve_state(cls, state):
        """
        Given a value that may be either the name or the number of a state
        return a tuple of (state number, state name).

        :param state: Either a string state name or an integer state number.
        :return: A tuple (state number, state name)
        :raises ValueError: if ``state`` is neither a valid state name nor a
            valid state number.
        """
        try:
            try:
                # maybe it's a number
                float_state = float(state)
                state_num = int(float_state)
                if state_num != float_state:
                    raise ValueError('Invalid state %r' % state)
                state_name = cls.STATES[state_num]
            except (ValueError, TypeError):
                # maybe it's a state name
                state_name = state.lower()
                state_num = cls.STATES_BY_NAME[state_name]
        except (KeyError, AttributeError):
            raise ValueError('Invalid state %r' % state)
        return state_num, state_name

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = self.resolve_state(state)[0]

    @property
    def state_text(self):
        return self.STATES[self.state]

    @property
    def state_timestamp(self):
        if self._state_timestamp is None:
            return self.timestamp
        return self._state_timestamp

    @state_timestamp.setter
    def state_timestamp(self, ts):
        self._state_timestamp = self._to_timestamp(ts)

    @property
    def epoch(self):
        return self._epoch

    @epoch.setter
    def epoch(self, epoch):
        self._epoch = self._to_timestamp(epoch)

    @property
    def reported(self):
        return self._reported

    @reported.setter
    def reported(self, value):
        self._reported = bool(value)

    def update_state(self, state, state_timestamp=None):
        """
        Set state to the given value and optionally update the state_timestamp
        to the given time.

        :param state: new state, should be an integer
        :param state_timestamp: timestamp for state; if not given the
            state_timestamp will not be changed.
        :return: True if the state or state_timestamp was changed, False
            otherwise
        """
        if state_timestamp is None and self.state == state:
            return False
        self.state = state
        if state_timestamp is not None:
            self.state_timestamp = state_timestamp
        self.reported = False
        return True

    @property
    def deleted(self):
        return self._deleted

    @deleted.setter
    def deleted(self, value):
        self._deleted = bool(value)

    def set_deleted(self, timestamp=None):
        """
        Mark the shard range deleted and set timestamp to the current time.

        :param timestamp: optional timestamp to set; if not given the
            current time will be set.
        :return: True if the deleted attribute or timestamp was changed, False
            otherwise
        """
        if timestamp is None and self.deleted:
            return False
        self.deleted = True
        self.timestamp = timestamp or Timestamp.now()
        return True

    # A by-the-book implementation should probably hash the value, which
    # in our case would be account+container+lower+upper (+timestamp ?).
    # But we seem to be okay with just the identity.
    def __hash__(self):
        return id(self)

    def __repr__(self):
        return '%s<%r to %r as of %s, (%d, %d) as of %s, %s as of %s>' % (
            self.__class__.__name__, self.lower, self.upper,
            self.timestamp.internal, self.object_count, self.bytes_used,
            self.meta_timestamp.internal, self.state_text,
            self.state_timestamp.internal)

    def __iter__(self):
        yield 'name', self.name
        yield 'timestamp', self.timestamp.internal
        yield 'lower', str(self.lower)
        yield 'upper', str(self.upper)
        yield 'object_count', self.object_count
        yield 'bytes_used', self.bytes_used
        yield 'meta_timestamp', self.meta_timestamp.internal
        yield 'deleted', 1 if self.deleted else 0
        yield 'state', self.state
        yield 'state_timestamp', self.state_timestamp.internal
        yield 'epoch', self.epoch.internal if self.epoch is not None else None
        yield 'reported', 1 if self.reported else 0
        yield 'tombstones', self.tombstones

    def copy(self, timestamp=None, **kwargs):
        """
        Creates a copy of the ShardRange.

        :param timestamp: (optional) If given, the returned ShardRange will
            have all of its timestamps set to this value. Otherwise the
            returned ShardRange will have the original timestamps.
        :return: an instance of :class:`~swift.common.utils.ShardRange`
        """
        new = ShardRange.from_dict(dict(self, **kwargs))
        if timestamp:
            new.timestamp = timestamp
            new.meta_timestamp = new.state_timestamp = None
        return new

    @classmethod
    def from_dict(cls, params):
        """
        Return an instance constructed using the given dict of params. This
        method is deliberately less flexible than the class `__init__()` method
        and requires all of the `__init__()` args to be given in the dict of
        params.

        :param params: a dict of parameters
        :return: an instance of this class
        """
        return cls(
            params['name'], params['timestamp'], params['lower'],
            params['upper'], params['object_count'], params['bytes_used'],
            params['meta_timestamp'], params['deleted'], params['state'],
            params['state_timestamp'], params['epoch'],
            params.get('reported', 0), params.get('tombstones', -1))


class ShardRangeList(UserList):
    """
    This class provides some convenience functions for working with lists of
    :class:`~swift.common.utils.ShardRange`.

    This class does not enforce ordering or continuity of the list items:
    callers should ensure that items are added in order as appropriate.
    """

    def __getitem__(self, index):
        # workaround for py36,py37 - not needed for py3.8+
        # see https://github.com/python/cpython/commit/b1c3167c
        result = self.data[index]
        return ShardRangeList(result) if type(result) is list else result

    @property
    def lower(self):
        """
        Returns the lower bound of the first item in the list. Note: this will
        only be equal to the lowest bound of all items in the list if the list
        contents has been sorted.

        :return: lower bound of first item in the list, or Namespace.MIN
                 if the list is empty.
        """
        if not self:
            # empty list has range MIN->MIN
            return Namespace.MIN
        return self[0].lower

    @property
    def upper(self):
        """
        Returns the upper bound of the last item in the list. Note: this will
        only be equal to the uppermost bound of all items in the list if the
        list has previously been sorted.

        :return: upper bound of last item in the list, or Namespace.MIN
                 if the list is empty.
        """
        if not self:
            # empty list has range MIN->MIN
            return Namespace.MIN
        return self[-1].upper

    @property
    def object_count(self):
        """
        Returns the total number of objects of all items in the list.

        :return: total object count
        """
        return sum(sr.object_count for sr in self)

    @property
    def row_count(self):
        """
        Returns the total number of rows of all items in the list.

        :return: total row count
        """
        return sum(sr.row_count for sr in self)

    @property
    def bytes_used(self):
        """
        Returns the total number of bytes in all items in the list.

        :return: total bytes used
        """
        return sum(sr.bytes_used for sr in self)

    @property
    def timestamps(self):
        return set(sr.timestamp for sr in self)

    @property
    def states(self):
        return set(sr.state for sr in self)

    def includes(self, other):
        """
        Check if another ShardRange namespace is enclosed between the list's
        ``lower`` and ``upper`` properties. Note: the list's ``lower`` and
        ``upper`` properties will only equal the outermost bounds of all items
        in the list if the list has previously been sorted.

        Note: the list does not need to contain an item matching ``other`` for
        this method to return True, although if the list has been sorted and
        does contain an item matching ``other`` then the method will return
        True.

        :param other: an instance of :class:`~swift.common.utils.ShardRange`
        :return: True if other's namespace is enclosed, False otherwise.
        """
        return self.lower <= other.lower and self.upper >= other.upper

    def filter(self, includes=None, marker=None, end_marker=None):
        """
        Filter the list for those shard ranges whose namespace includes the
        ``includes`` name or any part of the namespace between ``marker`` and
        ``end_marker``. If none of ``includes``, ``marker`` or ``end_marker``
        are specified then all shard ranges will be returned.

        :param includes: a string; if not empty then only the shard range, if
            any, whose namespace includes this string will be returned, and
            ``marker`` and ``end_marker`` will be ignored.
        :param marker: if specified then only shard ranges whose upper bound is
            greater than this value will be returned.
        :param end_marker: if specified then only shard ranges whose lower
            bound is less than this value will be returned.
        :return: A new instance of :class:`~swift.common.utils.ShardRangeList`
            containing the filtered shard ranges.
        """
        return ShardRangeList(
            filter_namespaces(self, includes, marker, end_marker))

    def find_lower(self, condition):
        """
        Finds the first shard range satisfies the given condition and returns
        its lower bound.

        :param condition: A function that must accept a single argument of type
            :class:`~swift.common.utils.ShardRange` and return True if the
            shard range satisfies the condition or False otherwise.
        :return: The lower bound of the first shard range to satisfy the
            condition, or the ``upper`` value of this list if no such shard
            range is found.

        """
        for sr in self:
            if condition(sr):
                return sr.lower
        return self.upper


def find_namespace(item, namespaces):
    """
    Find a Namespace/ShardRange in given list of ``namespaces`` whose namespace
    contains ``item``.

    :param item: The item for a which a Namespace is to be found.
    :param ranges: a sorted list of Namespaces.
    :return: the Namespace/ShardRange whose namespace contains ``item``, or
        None if no suitable Namespace is found.
    """
    index = bisect.bisect_left(namespaces, item)
    if index != len(namespaces) and item in namespaces[index]:
        return namespaces[index]
    return None


def filter_namespaces(namespaces, includes, marker, end_marker):
    """
    Filter the given Namespaces/ShardRanges to those whose namespace includes
    the ``includes`` name or any part of the namespace between ``marker`` and
    ``end_marker``. If none of ``includes``, ``marker`` or ``end_marker`` are
    specified then all Namespaces will be returned.

    :param namespaces: A list of :class:`~swift.common.utils.Namespace` or
        :class:`~swift.common.utils.ShardRange`.
    :param includes: a string; if not empty then only the Namespace,
        if any, whose namespace includes this string will be returned,
        ``marker`` and ``end_marker`` will be ignored.
    :param marker: if specified then only shard ranges whose upper bound is
        greater than this value will be returned.
    :param end_marker: if specified then only shard ranges whose lower bound is
        less than this value will be returned.
    :return: A filtered list of :class:`~swift.common.utils.Namespace`.
    """
    if includes:
        namespace = find_namespace(includes, namespaces)
        return [namespace] if namespace else []

    def namespace_filter(sr):
        end = start = True
        if end_marker:
            end = end_marker > sr.lower
        if marker:
            start = marker < sr.upper
        return start and end

    if marker or end_marker:
        return list(filter(namespace_filter, namespaces))

    if marker == Namespace.MAX or end_marker == Namespace.MIN:
        # MIN and MAX are both Falsy so not handled by namespace_filter
        return []

    return namespaces


def o_tmpfile_in_path_supported(dirpath):
    fd = None
    try:
        fd = os.open(dirpath, os.O_WRONLY | O_TMPFILE)
        return True
    except OSError as e:
        if e.errno in (errno.EINVAL, errno.EISDIR, errno.EOPNOTSUPP):
            return False
        else:
            raise Exception("Error on '%(path)s' while checking "
                            "O_TMPFILE: '%(ex)s'" %
                            {'path': dirpath, 'ex': e})
    finally:
        if fd is not None:
            os.close(fd)


def o_tmpfile_in_tmpdir_supported():
    return o_tmpfile_in_path_supported(gettempdir())


def safe_json_loads(value):
    if value:
        try:
            return json.loads(value)
        except (TypeError, ValueError):
            pass
    return None


def strict_b64decode(value, allow_line_breaks=False, exact_size=None):
    '''
    Validate and decode Base64-encoded data.

    The stdlib base64 module silently discards bad characters, but we often
    want to treat them as an error.

    :param value: some base64-encoded data
    :param allow_line_breaks: if True, ignore carriage returns and newlines
    :param exact_size: if provided, the exact size of the decoded bytes
        expected; also enforces round-trip checks
    :returns: the decoded data
    :raises ValueError: if ``value`` is not a string, contains invalid
                        characters, or has insufficient padding
    '''
    if isinstance(value, bytes):
        try:
            value = value.decode('ascii')
        except UnicodeDecodeError:
            raise ValueError
    if not isinstance(value, str):
        raise ValueError
    # b64decode will silently discard bad characters, but we want to
    # treat them as an error
    valid_chars = string.digits + string.ascii_letters + '/+'
    strip_chars = '='
    if allow_line_breaks:
        valid_chars += '\r\n'
        strip_chars += '\r\n'
    if any(c not in valid_chars for c in value.strip(strip_chars)):
        raise ValueError
    ret_val = base64.b64decode(value)
    if exact_size is not None:
        if len(ret_val) != exact_size:
            raise ValueError
        if base64_str(ret_val) != value:
            raise ValueError
    return ret_val


def base64_str(value):
    return base64.b64encode(value).decode('ascii')


def cap_length(value, max_length):
    if value and len(value) > max_length:
        if isinstance(value, bytes):
            return value[:max_length] + b'...'
        else:
            return value[:max_length] + '...'
    return value


MD5_BLOCK_READ_BYTES = 4096


def md5_hash_for_file(fname):
    """
    Get the MD5 checksum of a file.

    :param fname: path to file
    :returns: MD5 checksum, hex encoded
    """
    with open(fname, 'rb') as f:
        md5sum = md5(usedforsecurity=False)
        for block in iter(lambda: f.read(MD5_BLOCK_READ_BYTES), b''):
            md5sum.update(block)
    return md5sum.hexdigest()


def get_partition_for_hash(hex_hash, part_power):
    """
    Return partition number for given hex hash and partition power.
    :param hex_hash: A hash string
    :param part_power: partition power
    :returns: partition number
    """
    raw_hash = binascii.unhexlify(hex_hash)
    part_shift = 32 - int(part_power)
    return struct.unpack_from('>I', raw_hash)[0] >> part_shift


def get_partition_from_path(devices, path):
    """
    :param devices: directory where devices are mounted (e.g. /srv/node)
    :param path: full path to a object file or hashdir
    :returns: the (integer) partition from the path
    """
    offset_parts = devices.rstrip(os.sep).split(os.sep)
    path_components = path.split(os.sep)
    if offset_parts == path_components[:len(offset_parts)]:
        offset = len(offset_parts)
    else:
        raise ValueError('Path %r is not under device dir %r' % (
            path, devices))
    return int(path_components[offset + 2])


def replace_partition_in_path(devices, path, part_power):
    """
    Takes a path and a partition power and returns the same path, but with the
    correct partition number. Most useful when increasing the partition power.

    :param devices: directory where devices are mounted (e.g. /srv/node)
    :param path: full path to a object file or hashdir
    :param part_power: partition power to compute correct partition number
    :returns: Path with re-computed partition power
    """
    offset_parts = devices.rstrip(os.sep).split(os.sep)
    path_components = path.split(os.sep)
    if offset_parts == path_components[:len(offset_parts)]:
        offset = len(offset_parts)
    else:
        raise ValueError('Path %r is not under device dir %r' % (
            path, devices))
    part = get_partition_for_hash(path_components[offset + 4], part_power)
    path_components[offset + 2] = "%d" % part
    return os.sep.join(path_components)


def load_pkg_resource(group, uri):
    if '#' in uri:
        uri, name = uri.split('#', 1)
    else:
        name = uri
        uri = 'egg:swift'

    if ':' in uri:
        scheme, dist = uri.split(':', 1)
        scheme = scheme.lower()
    else:
        scheme = 'egg'
        dist = uri

    if scheme != 'egg':
        raise TypeError('Unhandled URI scheme: %r' % scheme)

    if pkg_resources:
        # python < 3.8
        return pkg_resources.load_entry_point(dist, group, name)

    # May raise importlib.metadata.PackageNotFoundError
    meta = importlib.metadata.distribution(dist)

    entry_points = [ep for ep in meta.entry_points
                    if ep.group == group and ep.name == name]
    if not entry_points:
        raise ImportError("Entry point %r not found" % ((group, name),))
    return entry_points[0].load()


def round_robin_iter(its):
    """
    Takes a list of iterators, yield an element from each in a round-robin
    fashion until all of them are exhausted.
    :param its: list of iterators
    """
    while its:
        for it in its:
            try:
                yield next(it)
            except StopIteration:
                its.remove(it)


OverrideOptions = collections.namedtuple(
    'OverrideOptions', ['devices', 'partitions', 'policies'])


def parse_override_options(**kwargs):
    """
    Figure out which policies, devices, and partitions we should operate on,
    based on kwargs.

    If 'override_policies' is already present in kwargs, then return that
    value. This happens when using multiple worker processes; the parent
    process supplies override_policies=X to each child process.

    Otherwise, in run-once mode, look at the 'policies' keyword argument.
    This is the value of the "--policies" command-line option. In
    run-forever mode or if no --policies option was provided, an empty list
    will be returned.

    The procedures for devices and partitions are similar.

    :returns: a named tuple with fields "devices", "partitions", and
      "policies".
    """
    run_once = kwargs.get('once', False)

    if 'override_policies' in kwargs:
        policies = kwargs['override_policies']
    elif run_once:
        policies = [
            int(p) for p in list_from_csv(kwargs.get('policies'))]
    else:
        policies = []

    if 'override_devices' in kwargs:
        devices = kwargs['override_devices']
    elif run_once:
        devices = list_from_csv(kwargs.get('devices'))
    else:
        devices = []

    if 'override_partitions' in kwargs:
        partitions = kwargs['override_partitions']
    elif run_once:
        partitions = [
            int(p) for p in list_from_csv(kwargs.get('partitions'))]
    else:
        partitions = []

    return OverrideOptions(devices=devices, partitions=partitions,
                           policies=policies)


def distribute_evenly(items, num_buckets):
    """
    Distribute items as evenly as possible into N buckets.
    """
    out = [[] for _ in range(num_buckets)]
    for index, item in enumerate(items):
        out[index % num_buckets].append(item)
    return out


def get_redirect_data(response):
    """
    Extract a redirect location from a response's headers.

    :param response: a response
    :return: a tuple of (path, Timestamp) if a Location header is found,
        otherwise None
    :raises ValueError: if the Location header is found but a
        X-Backend-Redirect-Timestamp is not found, or if there is a problem
        with the format of etiher header
    """
    headers = HeaderKeyDict(response.getheaders())
    if 'Location' not in headers:
        return None
    location = urlparse(headers['Location']).path
    if config_true_value(headers.get('X-Backend-Location-Is-Quoted',
                                     'false')):
        location = unquote(location)
    account, container, _junk = split_path(location, 2, 3, True)
    timestamp_val = headers.get('X-Backend-Redirect-Timestamp')
    try:
        timestamp = Timestamp(timestamp_val)
    except (TypeError, ValueError):
        raise ValueError('Invalid timestamp value: %s' % timestamp_val)
    return '%s/%s' % (account, container), timestamp


def parse_db_filename(filename):
    """
    Splits a db filename into three parts: the hash, the epoch, and the
    extension.

    >>> parse_db_filename("ab2134.db")
    ('ab2134', None, '.db')
    >>> parse_db_filename("ab2134_1234567890.12345.db")
    ('ab2134', '1234567890.12345', '.db')

    :param filename: A db file basename or path to a db file.
    :return: A tuple of (hash , epoch, extension). ``epoch`` may be None.
    :raises ValueError: if ``filename`` is not a path to a file.
    """
    filename = os.path.basename(filename)
    if not filename:
        raise ValueError('Path to a file required.')
    name, ext = os.path.splitext(filename)
    parts = name.split('_')
    hash_ = parts.pop(0)
    epoch = parts[0] if parts else None
    return hash_, epoch, ext


def make_db_file_path(db_path, epoch):
    """
    Given a path to a db file, return a modified path whose filename part has
    the given epoch.

    A db filename takes the form ``<hash>[_<epoch>].db``; this method replaces
    the ``<epoch>`` part of the given ``db_path`` with the given ``epoch``
    value, or drops the epoch part if the given ``epoch`` is ``None``.

    :param db_path: Path to a db file that does not necessarily exist.
    :param epoch: A string (or ``None``) that will be used as the epoch
        in the new path's filename; non-``None`` values will be
        normalized to the normal string representation of a
        :class:`~swift.common.utils.Timestamp`.
    :return: A modified path to a db file.
    :raises ValueError: if the ``epoch`` is not valid for constructing a
        :class:`~swift.common.utils.Timestamp`.
    """
    hash_, _, ext = parse_db_filename(db_path)
    db_dir = os.path.dirname(db_path)
    if epoch is None:
        return os.path.join(db_dir, hash_ + ext)
    epoch = Timestamp(epoch).normal
    return os.path.join(db_dir, '%s_%s%s' % (hash_, epoch, ext))


def get_db_files(db_path):
    """
    Given the path to a db file, return a sorted list of all valid db files
    that actually exist in that path's dir. A valid db filename has the form::

        <hash>[_<epoch>].db

    where <hash> matches the <hash> part of the given db_path as would be
    parsed by :meth:`~swift.utils.common.parse_db_filename`.

    :param db_path: Path to a db file that does not necessarily exist.
    :return: List of valid db files that do exist in the dir of the
        ``db_path``. This list may be empty.
    """
    db_dir, db_file = os.path.split(db_path)
    try:
        files = os.listdir(db_dir)
    except OSError as err:
        if err.errno == errno.ENOENT:
            return []
        raise
    if not files:
        return []
    match_hash, epoch, ext = parse_db_filename(db_file)
    results = []
    for f in files:
        hash_, epoch, ext = parse_db_filename(f)
        if ext != '.db':
            continue
        if hash_ != match_hash:
            continue
        results.append(os.path.join(db_dir, f))
    return sorted(results)


def get_pid_notify_socket(pid=None):
    """
    Get a pid-specific abstract notification socket.

    This is used by the ``swift-reload`` command.
    """
    if pid is None:
        pid = os.getpid()
    return '\0swift-notifications\0' + str(pid)


class NotificationServer(object):
    RECV_SIZE = 1024

    def __init__(self, pid, read_timeout):
        self.pid = pid
        self.read_timeout = read_timeout
        self.sock = None

    def receive(self):
        return self.sock.recv(self.RECV_SIZE)

    def close(self):
        self.sock.close()
        self.sock = None

    def start(self):
        if self.sock is not None:
            raise RuntimeError('notification server already started')

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        started = False
        try:
            self.sock.bind(get_pid_notify_socket(self.pid))
            self.sock.settimeout(self.read_timeout)
            started = True
        finally:
            if not started:
                self.close()

    def __enter__(self):
        if self.sock is None:
            self.start()
        return self

    def __exit__(self, *args):
        self.close()


def systemd_notify(logger=None, msg=b"READY=1"):
    """
    Send systemd-compatible notifications.

    Attempt to send the message to swift's pid-specific notification socket;
    see :func:`get_pid_notify_socket`. This is used by the ``swift-reload``
    command.

    Additionally, notify the service manager that started this process, if
    it has set the NOTIFY_SOCKET environment variable. For example, systemd
    will set this when the unit has ``Type=notify``. More information can
    be found in systemd documentation:
    https://www.freedesktop.org/software/systemd/man/sd_notify.html

    Common messages include::

       READY=1
       RELOADING=1
       STOPPING=1

    :param logger: a logger object
    :param msg: the message to send
    """
    if not isinstance(msg, bytes):
        msg = msg.encode('utf8')

    notify_sockets = [get_pid_notify_socket()]
    systemd_socket = os.getenv('NOTIFY_SOCKET')
    if systemd_socket:
        notify_sockets.append(systemd_socket)
    for notify_socket in notify_sockets:
        if notify_socket.startswith('@'):
            # abstract namespace socket
            notify_socket = '\0%s' % notify_socket[1:]
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        with closing(sock):
            try:
                sock.connect(notify_socket)
                sock.sendall(msg)
            except EnvironmentError as e:
                if logger and not (notify_socket == notify_sockets[0] and
                                   e.errno == errno.ECONNREFUSED):
                    logger.debug("Systemd notification failed", exc_info=True)


class Watchdog(object):
    """
    Implements a watchdog to efficiently manage concurrent timeouts.

    Compared to eventlet.timeouts.Timeout, it reduces the number of context
    switching in eventlet by avoiding to schedule actions (throw an Exception),
    then unschedule them if the timeouts are cancelled.

    1. at T+0, request timeout(10)
        => wathdog greenlet sleeps 10 seconds
    2. at T+1, request timeout(15)
        => the timeout will expire after the current, no need to wake up the
           watchdog greenlet
    3. at T+2, request timeout(5)
        => the timeout will expire before the first timeout, wake up the
           watchdog greenlet to calculate a new sleep period
    4. at T+7, the 3rd timeout expires
        => the exception is raised, then the greenlet watchdog sleep(3) to
           wake up for the 1st timeout expiration
    """

    def __init__(self):
        # key => (timeout, timeout_at, caller_greenthread, exception)
        self._timeouts = dict()
        self._evt = Event()
        self._next_expiration = None
        self._run_gth = None

    def start(self, timeout, exc, timeout_at=None):
        """
        Schedule a timeout action

        :param timeout: duration before the timeout expires
        :param exc: exception to throw when the timeout expire, must inherit
                    from eventlet.Timeout
        :param timeout_at: allow to force the expiration timestamp
        :return: id of the scheduled timeout, needed to cancel it
        """
        now = time.time()
        if not timeout_at:
            timeout_at = now + timeout
        gth = eventlet.greenthread.getcurrent()
        timeout_definition = (timeout, timeout_at, gth, exc, now)
        key = id(timeout_definition)
        self._timeouts[key] = timeout_definition

        # Wake up the watchdog loop only when there is a new shorter timeout
        if (self._next_expiration is None
                or self._next_expiration > timeout_at):
            # There could be concurrency on .send(), so wrap it in a try
            try:
                if not self._evt.ready():
                    self._evt.send()
            except AssertionError:
                pass

        return key

    def stop(self, key):
        """
        Cancel a scheduled timeout

        :param key: timeout id, as returned by start()
        """
        try:
            del self._timeouts[key]
        except KeyError:
            pass

    def spawn(self):
        """
        Start the watchdog greenthread.
        """
        if self._run_gth is None:
            self._run_gth = eventlet.spawn(self.run)

    def kill(self):
        """
        Stop the watchdog greenthread.
        """
        if self._run_gth is not None:
            self._run_gth.kill()
            self._run_gth = None

    def run(self):
        while True:
            self._run()

    def _run(self):
        now = time.time()
        self._next_expiration = None
        if self._evt.ready():
            self._evt.reset()
        for k, (timeout, timeout_at, gth, exc,
                created_at) in list(self._timeouts.items()):
            if timeout_at <= now:
                self.stop(k)
                e = exc()
                # set this after __init__ to keep it off the eventlet scheduler
                e.seconds = timeout
                e.created_at = created_at
                eventlet.hubs.get_hub().schedule_call_global(0, gth.throw, e)
            else:
                if (self._next_expiration is None
                        or self._next_expiration > timeout_at):
                    self._next_expiration = timeout_at
        if self._next_expiration is None:
            sleep_duration = self._next_expiration
        else:
            sleep_duration = self._next_expiration - now
        self._evt.wait(sleep_duration)


class WatchdogTimeout(object):
    """
    Context manager to schedule a timeout in a Watchdog instance
    """

    def __init__(self, watchdog, timeout, exc, timeout_at=None):
        """
        Schedule a timeout in a Watchdog instance

        :param watchdog: Watchdog instance
        :param timeout: duration before the timeout expires
        :param exc: exception to throw when the timeout expire, must inherit
                    from eventlet.timeouts.Timeout
        :param timeout_at: allow to force the expiration timestamp
        """
        self.watchdog = watchdog
        self.key = watchdog.start(timeout, exc, timeout_at=timeout_at)

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        self.watchdog.stop(self.key)


class CooperativeIterator(ClosingIterator):
    """
    Wrapper to make a deliberate periodic call to ``sleep()`` while iterating
    over wrapped iterator, providing an opportunity to switch greenthreads.

    This is for fairness; if the network is outpacing the CPU, we'll always be
    able to read and write data without encountering an EWOULDBLOCK, and so
    eventlet will not switch greenthreads on its own. We do it manually so that
    clients don't starve.

    The number 5 here was chosen by making stuff up. It's not every single
    chunk, but it's not too big either, so it seemed like it would probably be
    an okay choice.

    Note that we may trampoline to other greenthreads more often than once
    every 5 chunks, depending on how blocking our network IO is; the explicit
    sleep here simply provides a lower bound on the rate of trampolining.

    :param iterable: iterator to wrap.
    :param period: number of items yielded from this iterator between calls to
        ``sleep()``; a negative value or 0 mean that cooperative sleep will be
        disabled.
    """
    __slots__ = ('period', 'count')

    def __init__(self, iterable, period=5):
        super(CooperativeIterator, self).__init__(iterable)
        self.count = 0
        self.period = max(0, period or 0)

    def _get_next_item(self):
        if self.period:
            if self.count >= self.period:
                self.count = 0
                sleep()
            self.count += 1
        return super(CooperativeIterator, self)._get_next_item()


def get_ppid(pid):
    """
    Get the parent process's PID given a child pid.

    :raises OSError: if the child pid cannot be found
    """
    try:
        with open('/proc/%d/stat' % pid) as fp:
            stats = fp.read().split()
        return int(stats[3])
    except IOError as e:
        if e.errno == errno.ENOENT:
            raise OSError(errno.ESRCH, 'No such process')
        raise
