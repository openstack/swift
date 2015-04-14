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

from __future__ import print_function

import errno
import fcntl
import grp
import hmac
import operator
import os
import pwd
import re
import sys
import threading as stdlib_threading
import time
import uuid
import functools
import weakref
from hashlib import md5, sha1
from random import random, shuffle
from urllib import quote as _quote
from contextlib import contextmanager, closing
import ctypes
import ctypes.util
from ConfigParser import ConfigParser, NoSectionError, NoOptionError, \
    RawConfigParser
from optparse import OptionParser
from Queue import Queue, Empty
from tempfile import mkstemp, NamedTemporaryFile
try:
    import simplejson as json
except ImportError:
    import json
import cPickle as pickle
import glob
from urlparse import urlparse as stdlib_urlparse, ParseResult
import itertools
import stat
import datetime

import eventlet
import eventlet.semaphore
from eventlet import GreenPool, sleep, Timeout, tpool, greenthread, \
    greenio, event
from eventlet.green import socket, threading
import eventlet.queue
import netifaces
import codecs
utf8_decoder = codecs.getdecoder('utf-8')
utf8_encoder = codecs.getencoder('utf-8')

from swift import gettext_ as _
import swift.common.exceptions
from swift.common.http import is_success, is_redirection, HTTP_NOT_FOUND, \
    HTTP_PRECONDITION_FAILED, HTTP_REQUESTED_RANGE_NOT_SATISFIABLE


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
_sys_fallocate = None
_posix_fadvise = None
_libc_socket = None
_libc_bind = None
_libc_accept = None

# If set to non-zero, fallocate routines will fail based on free space
# available being at or below this amount, in bytes.
FALLOCATE_RESERVE = 0

# Used by hash_path to offer a bit more security when generating hashes for
# paths. It simply appends this value to all paths; guessing the hash a path
# will end up with would also require knowing this suffix.
HASH_PATH_SUFFIX = ''
HASH_PATH_PREFIX = ''

SWIFT_CONF_FILE = '/etc/swift/swift.conf'

# These constants are Linux-specific, and Python doesn't seem to know
# about them. We ask anyway just in case that ever gets fixed.
#
# The values were copied from the Linux 3.0 kernel headers.
AF_ALG = getattr(socket, 'AF_ALG', 38)
F_SETPIPE_SZ = getattr(fcntl, 'F_SETPIPE_SZ', 1031)


class InvalidHashPathConfigError(ValueError):

    def __str__(self):
        return "[swift-hash]: both swift_hash_path_suffix and " \
            "swift_hash_path_prefix are missing from %s" % SWIFT_CONF_FILE


def validate_hash_conf():
    global HASH_PATH_SUFFIX
    global HASH_PATH_PREFIX
    if not HASH_PATH_SUFFIX and not HASH_PATH_PREFIX:
        hash_conf = ConfigParser()
        if hash_conf.read(SWIFT_CONF_FILE):
            try:
                HASH_PATH_SUFFIX = hash_conf.get('swift-hash',
                                                 'swift_hash_path_suffix')
            except (NoSectionError, NoOptionError):
                pass
            try:
                HASH_PATH_PREFIX = hash_conf.get('swift-hash',
                                                 'swift_hash_path_prefix')
            except (NoSectionError, NoOptionError):
                pass
        if not HASH_PATH_SUFFIX and not HASH_PATH_PREFIX:
            raise InvalidHashPathConfigError()


try:
    validate_hash_conf()
except InvalidHashPathConfigError:
    # could get monkey patched or lazy loaded
    pass


def get_hmac(request_method, path, expires, key):
    """
    Returns the hexdigest string of the HMAC-SHA1 (RFC 2104) for
    the request.

    :param request_method: Request method to allow.
    :param path: The path to the resource to allow access to.
    :param expires: Unix timestamp as an int for when the URL
                    expires.
    :param key: HMAC shared secret.

    :returns: hexdigest str of the HMAC-SHA1 for the request.
    """
    return hmac.new(
        key, '%s\n%s\n%s' % (request_method, expires, path), sha1).hexdigest()


# Used by get_swift_info and register_swift_info to store information about
# the swift cluster.
_swift_info = {}
_swift_admin_info = {}


def get_swift_info(admin=False, disallowed_sections=None):
    """
    Returns information about the swift cluster that has been previously
    registered with the register_swift_info call.

    :param admin: boolean value, if True will additionally return an 'admin'
                  section with information previously registered as admin
                  info.
    :param disallowed_sections: list of section names to be withheld from the
                                information returned.
    :returns: dictionary of information about the swift cluster.
    """
    disallowed_sections = disallowed_sections or []
    info = dict(_swift_info)
    for section in disallowed_sections:
        key_to_pop = None
        sub_section_dict = info
        for sub_section in section.split('.'):
            if key_to_pop:
                sub_section_dict = sub_section_dict.get(key_to_pop, {})
                if not isinstance(sub_section_dict, dict):
                    sub_section_dict = {}
                    break
            key_to_pop = sub_section
        sub_section_dict.pop(key_to_pop, None)

    if admin:
        info['admin'] = dict(_swift_admin_info)
        info['admin']['disallowed_sections'] = list(disallowed_sections)
    return info


def register_swift_info(name='swift', admin=False, **kwargs):
    """
    Registers information about the swift cluster to be retrieved with calls
    to get_swift_info.

    NOTE: Do not use "." in the param: name or any keys in kwargs. "." is used
          in the disallowed_sections to remove unwanted keys from /info.

    :param name: string, the section name to place the information under.
    :param admin: boolean, if True, information will be registered to an
                  admin section which can optionally be withheld when
                  requesting the information.
    :param kwargs: key value arguments representing the information to be
                   added.
    :raises ValueError: if name or any of the keys in kwargs has "." in it
    """
    if name == 'admin' or name == 'disallowed_sections':
        raise ValueError('\'{0}\' is reserved name.'.format(name))

    if admin:
        dict_to_use = _swift_admin_info
    else:
        dict_to_use = _swift_info
    if name not in dict_to_use:
        if "." in name:
            raise ValueError('Cannot use "." in a swift_info key: %s' % name)
        dict_to_use[name] = {}
    for key, val in kwargs.iteritems():
        if "." in key:
            raise ValueError('Cannot use "." in a swift_info key: %s' % key)
        dict_to_use[name][key] = val


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
    last_row = ''
    while f.tell() != 0:
        try:
            f.seek(-blocksize, os.SEEK_CUR)
        except IOError:
            blocksize = f.tell()
            f.seek(-blocksize, os.SEEK_CUR)
        block = f.read(blocksize)
        f.seek(-blocksize, os.SEEK_CUR)
        rows = block.split('\n')
        rows[-1] = rows[-1] + last_row
        while rows:
            last_row = rows.pop(-1)
            if rows and last_row:
                yield last_row
    yield last_row


# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))


def config_true_value(value):
    """
    Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    """
    return value is True or \
        (isinstance(value, basestring) and value.lower() in TRUE_VALUES)


def config_auto_int_value(value, default):
    """
    Returns default if value is None or 'auto'.
    Returns value as an int or raises ValueError otherwise.
    """
    if value is None or \
       (isinstance(value, basestring) and value.lower() == 'auto'):
        return default
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise ValueError('Config option must be an integer or the '
                         'string "auto", not "%s".' % value)
    return value


def append_underscore(prefix):
    if prefix and prefix[-1] != '_':
        prefix += '_'
    return prefix


def config_read_reseller_options(conf, defaults):
    """
    Read reseller_prefix option and associated options from configuration

    Reads the reseller_prefix option, then reads options that may be
    associated with a specific reseller prefix. Reads options such that an
    option without a prefix applies to all reseller prefixes unless an option
    has an explicit prefix.

    :param conf: the configuration
    :param defaults: a dict of default values. The key is the option
                     name. The value is either an array of strings or a string
    :return: tuple of an array of reseller prefixes and a dict of option values
    """
    reseller_prefix_opt = conf.get('reseller_prefix', 'AUTH').split(',')
    reseller_prefixes = []
    for prefix in [pre.strip() for pre in reseller_prefix_opt if pre.strip()]:
        if prefix == "''":
            prefix = ''
        prefix = append_underscore(prefix)
        if prefix not in reseller_prefixes:
            reseller_prefixes.append(prefix)
    if len(reseller_prefixes) == 0:
        reseller_prefixes.append('')

    # Get prefix-using config options
    associated_options = {}
    for prefix in reseller_prefixes:
        associated_options[prefix] = dict(defaults)
        associated_options[prefix].update(
            config_read_prefixed_options(conf, '', defaults))
        prefix_name = prefix if prefix != '' else "''"
        associated_options[prefix].update(
            config_read_prefixed_options(conf, prefix_name, defaults))
    return reseller_prefixes, associated_options


def config_read_prefixed_options(conf, prefix_name, defaults):
    """
    Read prefixed options from configuration

    :param conf: the configuration
    :param prefix_name: the prefix (including, if needed, an underscore)
    :param defaults: a dict of default values. The dict supplies the
                     option name and type (string or comma separated string)
    :return: a dict containing the options
    """
    params = {}
    for option_name in defaults.keys():
        value = conf.get('%s%s' % (prefix_name, option_name))
        if value:
            if isinstance(defaults.get(option_name), list):
                params[option_name] = []
                for role in value.lower().split(','):
                    params[option_name].append(role.strip())
            else:
                params[option_name] = value.strip()
    return params


def noop_libc_function(*args):
    return 0


def validate_configuration():
    try:
        validate_hash_conf()
    except InvalidHashPathConfigError as e:
        sys.exit("Error: %s" % e)


def load_libc_function(func_name, log_error=True,
                       fail_if_missing=False):
    """
    Attempt to find the function in libc, otherwise return a no-op func.

    :param func_name: name of the function to pull from libc.
    :param log_error: log an error when a function can't be found
    :param fail_if_missing: raise an exception when a function can't be found.
                            Default behavior is to return a no-op function.
    """
    try:
        libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
        return getattr(libc, func_name)
    except AttributeError:
        if fail_if_missing:
            raise
        if log_error:
            logging.warn(_("Unable to locate %s in libc.  Leaving as a "
                         "no-op."), func_name)
        return noop_libc_function


def generate_trans_id(trans_id_suffix):
    return 'tx%s-%010x%s' % (
        uuid.uuid4().hex[:21], time.time(), quote(trans_id_suffix))


def get_policy_index(req_headers, res_headers):
    """
    Returns the appropriate index of the storage policy for the request from
    a proxy server

    :param req: dict of the request headers.
    :param res: dict of the response headers.

    :returns: string index of storage policy, or None
    """
    header = 'X-Backend-Storage-Policy-Index'
    policy_index = res_headers.get(header, req_headers.get(header))
    return str(policy_index) if policy_index is not None else None


def get_log_line(req, res, trans_time, additional_info):
    """
    Make a line for logging that matches the documented log line format
    for backend servers.

    :param req: the request.
    :param res: the response.
    :param trans_time: the time the request took to complete, a float.
    :param additional_info: a string to log at the end of the line

    :returns: a properly formated line for logging.
    """

    policy_index = get_policy_index(req.headers, res.headers)
    return '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %.4f "%s" %d %s' % (
        req.remote_addr,
        time.strftime('%d/%b/%Y:%H:%M:%S +0000', time.gmtime()),
        req.method, req.path, res.status.split()[0],
        res.content_length or '-', req.referer or '-',
        req.headers.get('x-trans-id', '-'),
        req.user_agent or '-', trans_time, additional_info or '-',
        os.getpid(), policy_index or '-')


def get_trans_id_time(trans_id):
    if len(trans_id) >= 34 and trans_id[:2] == 'tx' and trans_id[23] == '-':
        try:
            return int(trans_id[24:34], 16)
        except ValueError:
            pass
    return None


class FileLikeIter(object):

    def __init__(self, iterable):
        """
        Wraps an iterable to behave as a file-like object.
        """
        self.iterator = iter(iterable)
        self.buf = None
        self.closed = False

    def __iter__(self):
        return self

    def next(self):
        """
        x.next() -> the next value, or raise StopIteration
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self.buf:
            rv = self.buf
            self.buf = None
            return rv
        else:
            return self.iterator.next()

    def read(self, size=-1):
        """
        read([size]) -> read at most size bytes, returned as a string.

        If the size argument is negative or omitted, read until EOF is reached.
        Notice that when in non-blocking mode, less data than what was
        requested may be returned, even if no size parameter was given.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if size < 0:
            return ''.join(self)
        elif not size:
            chunk = ''
        elif self.buf:
            chunk = self.buf
            self.buf = None
        else:
            try:
                chunk = self.iterator.next()
            except StopIteration:
                return ''
        if len(chunk) > size:
            self.buf = chunk[size:]
            chunk = chunk[:size]
        return chunk

    def readline(self, size=-1):
        """
        readline([size]) -> next line from the file, as a string.

        Retain newline.  A non-negative size argument limits the maximum
        number of bytes to return (an incomplete line may be returned then).
        Return an empty string at EOF.
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        data = ''
        while '\n' not in data and (size < 0 or len(data) < size):
            if size < 0:
                chunk = self.read(1024)
            else:
                chunk = self.read(size - len(data))
            if not chunk:
                break
            data += chunk
        if '\n' in data:
            data, sep, rest = data.partition('\n')
            data += sep
            if self.buf:
                self.buf = rest + self.buf
            else:
                self.buf = rest
        return data

    def readlines(self, sizehint=-1):
        """
        readlines([size]) -> list of strings, each a line from the file.

        Call readline() repeatedly and return a list of the lines so read.
        The optional size argument, if given, is an approximate bound on the
        total number of bytes in the lines returned.
        """
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
        close() -> None or (perhaps) an integer.  Close the file.

        Sets data attribute .closed to True.  A closed file cannot be used for
        further I/O operations.  close() may be called more than once without
        error.  Some kinds of file objects (for example, opened by popen())
        may return an exit status upon closing.
        """
        self.iterator = None
        self.closed = True


class FallocateWrapper(object):

    def __init__(self, noop=False):
        if noop:
            self.func_name = 'posix_fallocate'
            self.fallocate = noop_libc_function
            return
        ## fallocate is preferred because we need the on-disk size to match
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
        """The length parameter must be a ctypes.c_uint64."""
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
        logging.warn(_("Unable to perform fsync() on directory %s: %s"),
                     dirpath, os.strerror(err.errno))
    finally:
        if dirfd:
            os.close(dirfd)


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
        logging.warn("posix_fadvise64(%(fd)s, %(offset)s, %(length)s, 4) "
                     "-> %(ret)s", {'fd': fd, 'offset': offset,
                                    'length': length, 'ret': ret})


NORMAL_FORMAT = "%016.05f"
INTERNAL_FORMAT = NORMAL_FORMAT + '_%016x'
MAX_OFFSET = (16 ** 16) - 1
# Setting this to True will cause the internal format to always display
# extended digits - even when the value is equivalent to the normalized form.
# This isn't ideal during an upgrade when some servers might not understand
# the new time format - but flipping it to True works great for testing.
FORCE_INTERNAL = False  # or True


class Timestamp(object):
    """
    Internal Representation of Swift Time.

    The normalized form of the X-Timestamp header looks like a float
    with a fixed width to ensure stable string sorting - normalized
    timestamps look like "1402464677.04188"

    To support overwrites of existing data without modifying the original
    timestamp but still maintain consistency a second internal offset vector
    is append to the normalized timestamp form which compares and sorts
    greater than the fixed width float format but less than a newer timestamp.
    The internalized format of timestamps looks like
    "1402464677.04188_0000000000000000" - the portion after the underscore is
    the offset and is a formatted hexadecimal integer.

    The internalized form is not exposed to clients in responses from
    Swift.  Normal client operations will not create a timestamp with an
    offset.

    The Timestamp class in common.utils supports internalized and
    normalized formatting of timestamps and also comparison of timestamp
    values.  When the offset value of a Timestamp is 0 - it's considered
    insignificant and need not be represented in the string format; to
    support backwards compatibility during a Swift upgrade the
    internalized and normalized form of a Timestamp with an
    insignificant offset are identical.  When a timestamp includes an
    offset it will always be represented in the internalized form, but
    is still excluded from the normalized form.  Timestamps with an
    equivalent timestamp portion (the float part) will compare and order
    by their offset.  Timestamps with a greater timestamp portion will
    always compare and order greater than a Timestamp with a lesser
    timestamp regardless of it's offset.  String comparison and ordering
    is guaranteed for the internalized string format, and is backwards
    compatible for normalized timestamps which do not include an offset.
    """

    def __init__(self, timestamp, offset=0):
        if isinstance(timestamp, basestring):
            parts = timestamp.split('_', 1)
            self.timestamp = float(parts.pop(0))
            if parts:
                self.offset = int(parts[0], 16)
            else:
                self.offset = 0
        else:
            self.timestamp = float(timestamp)
            self.offset = getattr(timestamp, 'offset', 0)
        # increment offset
        if offset >= 0:
            self.offset += offset
        else:
            raise ValueError('offset must be non-negative')
        if self.offset > MAX_OFFSET:
            raise ValueError('offset must be smaller than %d' % MAX_OFFSET)

    def __repr__(self):
        return INTERNAL_FORMAT % (self.timestamp, self.offset)

    def __str__(self):
        raise TypeError('You must specify which string format is required')

    def __float__(self):
        return self.timestamp

    def __int__(self):
        return int(self.timestamp)

    def __nonzero__(self):
        return bool(self.timestamp or self.offset)

    @property
    def normal(self):
        return NORMAL_FORMAT % self.timestamp

    @property
    def internal(self):
        if self.offset or FORCE_INTERNAL:
            return INTERNAL_FORMAT % (self.timestamp, self.offset)
        else:
            return self.normal

    @property
    def isoformat(self):
        isoformat = datetime.datetime.utcfromtimestamp(
            float(self.normal)).isoformat()
        # python isoformat() doesn't include msecs when zero
        if len(isoformat) < len("1970-01-01T00:00:00.000000"):
            isoformat += ".000000"
        return isoformat

    def __eq__(self, other):
        if not isinstance(other, Timestamp):
            other = Timestamp(other)
        return self.internal == other.internal

    def __ne__(self, other):
        if not isinstance(other, Timestamp):
            other = Timestamp(other)
        return self.internal != other.internal

    def __cmp__(self, other):
        if not isinstance(other, Timestamp):
            other = Timestamp(other)
        return cmp(self.internal, other.internal)


def normalize_timestamp(timestamp):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx.xxxxx (10.5) format.

    Note that timestamps using values greater than or equal to November 20th,
    2286 at 17:46 UTC will use 11 digits to represent the number of
    seconds.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    return Timestamp(timestamp).normal


EPOCH = datetime.datetime(1970, 1, 1)


def last_modified_date_to_timestamp(last_modified_date_str):
    """
    Convert a last modified date (like you'd get from a container listing,
    e.g. 2014-02-28T23:22:36.698390) to a float.
    """
    start = datetime.datetime.strptime(last_modified_date_str,
                                       '%Y-%m-%dT%H:%M:%S.%f')
    delta = start - EPOCH
    # TODO(sam): after we no longer support py2.6, this expression can
    # simplify to Timestamp(delta.total_seconds()).
    #
    # This calculation is based on Python 2.7's Modules/datetimemodule.c,
    # function delta_to_microseconds(), but written in Python.
    return Timestamp(delta.days * 86400 +
                     delta.seconds +
                     delta.microseconds / 1000000.0)


def normalize_delete_at_timestamp(timestamp):
    """
    Format a timestamp (string or numeric) into a standardized
    xxxxxxxxxx (10) format.

    Note that timestamps less than 0000000000 are raised to
    0000000000 and values greater than November 20th, 2286 at
    17:46:39 UTC will be capped at that date and time, resulting in
    no return value exceeding 9999999999.

    This cap is because the expirer is already working through a
    sorted list of strings that were all a length of 10. Adding
    another digit would mess up the sort and cause the expirer to
    break from processing early. By 2286, this problem will need to
    be fixed, probably by creating an additional .expiring_objects
    account to work from with 11 (or more) digit container names.

    :param timestamp: unix timestamp
    :returns: normalized timestamp as a string
    """
    return '%010d' % min(max(0, float(timestamp)), 9999999999)


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
    :returns: list of segments with a length of maxsegs (non-existent
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
    def __init__(self, iterable, elements_per_second, limit_after=0):
        self.iterator = iter(iterable)
        self.elements_per_second = elements_per_second
        self.limit_after = limit_after
        self.running_time = 0

    def __iter__(self):
        return self

    def next(self):
        if self.limit_after > 0:
            self.limit_after -= 1
        else:
            self.running_time = ratelimit_sleep(self.running_time,
                                                self.elements_per_second)
        return self.iterator.next()


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

    def next(self):
        with self.semaphore:
            return self.unsafe_iter.next()


class NullLogger(object):
    """A no-op logger for eventlet wsgi."""

    def write(self, *args):
        # "Logs" the args to nowhere
        pass


class LoggerFileObject(object):

    def __init__(self, logger, log_type='STDOUT'):
        self.logger = logger
        self.log_type = log_type

    def write(self, value):
        value = value.strip()
        if value:
            if 'Connection reset by peer' in value:
                self.logger.error(
                    _('%s: Connection reset by peer'), self.log_type)
            else:
                self.logger.error(_('%s: %s'), self.log_type, value)

    def writelines(self, values):
        self.logger.error(_('%s: %s'), self.log_type, '#012'.join(values))

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
                 default_sample_rate=1, sample_rate_factor=1, logger=None):
        self._host = host
        self._port = port
        self._base_prefix = base_prefix
        self.set_prefix(tail_prefix)
        self._default_sample_rate = default_sample_rate
        self._sample_rate_factor = sample_rate_factor
        self._target = (self._host, self._port)
        self.random = random
        self.logger = logger

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
        sample_rate = sample_rate * self._sample_rate_factor
        parts = ['%s%s:%s' % (self._prefix, m_name, m_value), m_type]
        if sample_rate < 1:
            if self.random() < sample_rate:
                parts.append('@%s' % (sample_rate,))
            else:
                return
        # Ideally, we'd cache a sending socket in self, but that
        # results in a socket getting shared by multiple green threads.
        with closing(self._open_socket()) as sock:
            try:
                return sock.sendto('|'.join(parts), self._target)
            except IOError as err:
                if self.logger:
                    self.logger.warn(
                        'Error sending UDP message to %r: %s',
                        self._target, err)

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

    def transfer_rate(self, metric, elapsed_time, byte_xfer, sample_rate=None):
        if byte_xfer:
            return self.timing(metric,
                               elapsed_time * 1000 / byte_xfer * 1000,
                               sample_rate)


def server_handled_successfully(status_int):
    """
    True for successful responses *or* error codes that are not Swift's fault,
    False otherwise. For example, 500 is definitely the server's fault, but
    412 is an error code (4xx are all errors) that is due to a header the
    client sent.

    If one is tracking error rates to monitor server health, one would be
    advised to use a function like this one, lest a client cause a flurry of
    404s or 416s and make a spurious spike in your errors graph.
    """
    return (is_success(status_int) or
            is_redirection(status_int) or
            status_int == HTTP_NOT_FOUND or
            status_int == HTTP_PRECONDITION_FAILED or
            status_int == HTTP_REQUESTED_RANGE_NOT_SATISFIABLE)


def timing_stats(**dec_kwargs):
    """
    Returns a decorator that logs timing events or errors for public methods in
    swift's wsgi server controllers, based on response code.
    """
    def decorating_func(func):
        method = func.func_name

        @functools.wraps(func)
        def _timing_stats(ctrl, *args, **kwargs):
            start_time = time.time()
            resp = func(ctrl, *args, **kwargs)
            if server_handled_successfully(resp.status_int):
                ctrl.logger.timing_since(method + '.timing',
                                         start_time, **dec_kwargs)
            else:
                ctrl.logger.timing_since(method + '.errors.timing',
                                         start_time, **dec_kwargs)
            return resp

        return _timing_stats
    return decorating_func


class LoggingHandlerWeakRef(weakref.ref):
    """
    Like a weak reference, but passes through a couple methods that logging
    handlers need.
    """

    def close(self):
        referent = self()
        try:
            if referent:
                referent.close()
        except KeyError:
            # This is to catch an issue with old py2.6 versions
            pass

    def flush(self):
        referent = self()
        if referent:
            referent.flush()


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
            if isinstance(exc, swift.common.exceptions.MessageTimeout):
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
        Factory to create methods which delegate to methods on
        self.logger.statsd_client (an instance of StatsdClient).  The
        created methods conditionally delegate to a method whose name is given
        in 'statsd_func_name'.  The created delegate methods are a no-op when
        StatsD logging is not configured.

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
    transfer_rate = statsd_delegate('transfer_rate')


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
            if msg[-3:] != '#012':
                msg = msg + '#012'
            msg = msg + record.exc_text

        if (hasattr(record, 'txn_id') and record.txn_id and
                record.levelno != logging.INFO and
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
                approxhalf = (self.max_line_length - 5) / 2
                msg = msg[:approxhalf] + " ... " + msg[-approxhalf:]
        return msg


def get_logger(conf, name=None, log_to_console=False, log_route=None,
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
        log_statsd_host = (disabled)
        log_statsd_port = 8125
        log_statsd_default_sample_rate = 1.0
        log_statsd_sample_rate_factor = 1.0
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
    formatter = SwiftLogFormatter(
        fmt=fmt, max_line_length=int(conf.get('log_max_line_length', 0)))

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
        udp_port = int(conf.get('log_udp_port',
                                logging.handlers.SYSLOG_UDP_PORT))
        handler = SysLogHandler(address=(udp_host, udp_port),
                                facility=facility)
    else:
        log_address = conf.get('log_address', '/dev/log')
        try:
            handler = SysLogHandler(address=log_address, facility=facility)
        except socket.error as e:
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
        sample_rate_factor = float(conf.get(
            'log_statsd_sample_rate_factor', 1))
        statsd_client = StatsdClient(statsd_host, statsd_port, base_prefix,
                                     name, default_sample_rate,
                                     sample_rate_factor, logger=logger)
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
                print('Error calling custom handler [%s]' % hook,
                      file=sys.stderr)
            except ValueError:
                print('Invalid custom handler format [%s]' % hook,
                      file=sys.stderr)

    # Python 2.6 has the undesirable property of keeping references to all log
    # handlers around forever in logging._handlers and logging._handlerList.
    # Combine that with handlers that keep file descriptors, and you get an fd
    # leak.
    #
    # And no, we can't share handlers; a SyslogHandler has a socket, and if
    # two greenthreads end up logging at the same time, you could get message
    # overlap that garbles the logs and makes eventlet complain.
    #
    # Python 2.7 uses weakrefs to avoid the leak, so let's do that too.
    if sys.version_info[0] == 2 and sys.version_info[1] <= 6:
        try:
            logging._acquireLock()  # some thread-safety thing
            for handler in adapted_logger.logger.handlers:
                if handler in logging._handlers:
                    wr = LoggingHandlerWeakRef(handler)
                    del logging._handlers[handler]
                    logging._handlers[wr] = 1
                for i, handler_ref in enumerate(logging._handlerList):
                    if handler_ref is handler:
                        logging._handlerList[i] = LoggingHandlerWeakRef(
                            handler)
        finally:
            logging._releaseLock()

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
    if os.geteuid() == 0:
        groups = [g.gr_gid for g in grp.getgrall() if user in g.gr_mem]
        os.setgroups(groups)
    user = pwd.getpwnam(user)
    os.setgid(user[3])
    os.setuid(user[2])
    os.environ['HOME'] = user[5]
    try:
        os.setsid()
    except OSError:
        pass
    os.chdir('/')   # in case you need to rmdir on where you started the daemon
    os.umask(0o22)  # ensure files are created with the correct privileges


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
        sys.stderr = LoggerFileObject(logger, 'STDERR')


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
        print(_("Error: missing config path argument"))
        sys.exit(1)
    config = os.path.abspath(args.pop(0))
    if not os.path.exists(config):
        parser.print_usage()
        print(_("Error: unable to locate %s") % config)
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


def expand_ipv6(address):
    """
    Expand ipv6 address.
    :param address: a string indicating valid ipv6 address
    :returns: a string indicating fully expanded ipv6 address

    """
    packed_ip = socket.inet_pton(socket.AF_INET6, address)
    return socket.inet_ntop(socket.AF_INET6, packed_ip)


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
                    addr = address['addr']

                    # If we have an ipv6 address remove the
                    # %ether_interface at the end
                    if family == netifaces.AF_INET6:
                        addr = expand_ipv6(addr.split('%')[0])
                    addresses.append(addr)
        except ValueError:
            pass
    return addresses


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
    paths = [account]
    if container:
        paths.append(container)
    if object:
        paths.append(object)
    if raw_digest:
        return md5(HASH_PATH_PREFIX + '/' + '/'.join(paths)
                   + HASH_PATH_SUFFIX).digest()
    else:
        return md5(HASH_PATH_PREFIX + '/' + '/'.join(paths)
                   + HASH_PATH_SUFFIX).hexdigest()


@contextmanager
def lock_path(directory, timeout=10, timeout_class=None):
    """
    Context manager that acquires a lock on a directory.  This will block until
    the lock can be acquired, or the timeout time has expired (whichever occurs
    first).

    For locking exclusively, file or directory has to be opened in Write mode.
    Python doesn't allow directories to be opened in Write Mode. So we
    workaround by locking a hidden file in the directory.

    :param directory: directory to be locked
    :param timeout: timeout (in seconds)
    :param timeout_class: The class of the exception to raise if the
        lock cannot be granted within the timeout. Will be
        constructed as timeout_class(timeout, lockpath). Default:
        LockTimeout
    """
    if timeout_class is None:
        timeout_class = swift.common.exceptions.LockTimeout
    mkdirs(directory)
    lockpath = '%s/.lock' % directory
    fd = os.open(lockpath, os.O_WRONLY | os.O_CREAT)
    sleep_time = 0.01
    slower_sleep_time = max(timeout * 0.01, sleep_time)
    slowdown_at = timeout * 0.01
    time_slept = 0
    try:
        with timeout_class(timeout, lockpath):
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except IOError as err:
                    if err.errno != errno.EAGAIN:
                        raise
                if time_slept > slowdown_at:
                    sleep_time = slower_sleep_time
                sleep(sleep_time)
                time_slept += sleep_time
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


def unlink_older_than(path, mtime):
    """
    Remove any file in a given path that that was last modified before mtime.

    :param path: path to remove file from
    :mtime: timestamp of oldest file to keep
    """
    for fname in listdir(path):
        fpath = os.path.join(path, fname)
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


def read_conf_dir(parser, conf_dir):
    conf_files = []
    for f in os.listdir(conf_dir):
        if f.endswith('.conf') and not f.startswith('.'):
            conf_files.append(os.path.join(conf_dir, f))
    return parser.read(sorted(conf_files))


def readconf(conf_path, section_name=None, log_name=None, defaults=None,
             raw=False):
    """
    Read config file(s) and return config items as a dict

    :param conf_path: path to config file/directory, or a file-like object
                     (hasattr readline)
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
    if hasattr(conf_path, 'readline'):
        c.readfp(conf_path)
    else:
        if os.path.isdir(conf_path):
            # read all configs in directory
            success = read_conf_dir(c, conf_path)
        else:
            success = c.read(conf_path)
        if not success:
            print(_("Unable to read config from %s") % conf_path)
            sys.exit(1)
    if section_name:
        if c.has_section(section_name):
            conf = dict(c.items(section_name))
        else:
            print(_("Unable to find %s config section in %s") %
                  (section_name, conf_path))
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
    conf['__file__'] = conf_path
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


def search_tree(root, glob_match, ext='', exts=None, dir_ext=None):
    """Look in root, for any files/dirs matching glob, recursively traversing
    any found directories looking for files ending with ext

    :param root: start of search path
    :param glob_match: glob to match in root, matching dirs are traversed with
                       os.walk
    :param ext: only files that end in ext will be returned
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


def audit_location_generator(devices, datadir, suffix='',
                             mount_check=True, logger=None):
    '''
    Given a devices path and a data directory, yield (path, device,
    partition) for all files in that directory

    :param devices: parent directory of the devices to be audited
    :param datadir: a directory located under self.devices. This should be
                    one of the DATADIR constants defined in the account,
                    container, and object servers.
    :param suffix: path name suffix required for all names returned
    :param mount_check: Flag to check if a mount check should be performed
                    on devices
    :param logger: a logger object
    '''
    device_dir = listdir(devices)
    # randomize devices in case of process restart before sweep completed
    shuffle(device_dir)
    for device in device_dir:
        if mount_check and not ismount(os.path.join(devices, device)):
            if logger:
                logger.warning(
                    _('Skipping %s as it is not mounted'), device)
            continue
        datadir_path = os.path.join(devices, device, datadir)
        try:
            partitions = listdir(datadir_path)
        except OSError as e:
            if logger:
                logger.warning('Skipping %s because %s', datadir_path, e)
            continue
        for partition in partitions:
            part_path = os.path.join(datadir_path, partition)
            try:
                suffixes = listdir(part_path)
            except OSError as e:
                if e.errno != errno.ENOTDIR:
                    raise
                continue
            for asuffix in suffixes:
                suff_path = os.path.join(part_path, asuffix)
                try:
                    hashes = listdir(suff_path)
                except OSError as e:
                    if e.errno != errno.ENOTDIR:
                        raise
                    continue
                for hsh in hashes:
                    hash_path = os.path.join(suff_path, hsh)
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


def ratelimit_sleep(running_time, max_rate, incr_by=1, rate_buffer=5):
    '''
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
    '''
    if max_rate <= 0 or incr_by <= 0:
        return running_time

    # 1,000 milliseconds = 1 second
    clock_accuracy = 1000.0

    # Convert seconds to milliseconds
    now = time.time() * clock_accuracy

    # Calculate time per request in milliseconds
    time_per_request = clock_accuracy * (float(incr_by) / max_rate)

    # Convert rate_buffer to milliseconds and compare
    if now - running_time > rate_buffer * clock_accuracy:
        running_time = now
    elif running_time - now > time_per_request:
        # Convert diff back to a floating point number of seconds and sleep
        eventlet.sleep((running_time - now) / clock_accuracy)

    # Return the absolute time for the next interval in milliseconds; note
    # that time could have passed well beyond that point, but the next call
    # will catch that and skip the sleep.
    return running_time + time_per_request


class ContextPool(GreenPool):
    "GreenPool subclassed to kill its coros when it gets gc'ed"

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        for coro in list(self.coroutines_running):
            coro.kill()


class GreenAsyncPileWaitallTimeout(Timeout):
    pass


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

    def _run_func(self, func, args, kwargs):
        try:
            self._responses.put(func(*args, **kwargs))
        finally:
            self._inflight -= 1

    def spawn(self, func, *args, **kwargs):
        """
        Spawn a job in a green thread on the pile.
        """
        self._inflight += 1
        self._pool.spawn(self._run_func, func, args, kwargs)

    def waitall(self, timeout):
        """
        Wait timeout seconds for any results to come in.

        :param timeout: seconds to wait for results
        :returns: list of results accrued in that time
        """
        results = []
        try:
            with GreenAsyncPileWaitallTimeout(timeout):
                while True:
                    results.append(self.next())
        except (GreenAsyncPileWaitallTimeout, StopIteration):
            pass
        return results

    def __iter__(self):
        return self

    def next(self):
        try:
            return self._responses.get_nowait()
        except Empty:
            if self._inflight == 0:
                raise StopIteration()
            else:
                return self._responses.get()


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


def validate_sync_to(value, allowed_sync_hosts, realms_conf):
    """
    Validates an X-Container-Sync-To header value, returning the
    validated endpoint, realm, and realm_key, or an error string.

    :param value: The X-Container-Sync-To header value to validate.
    :param allowed_sync_hosts: A list of allowed hosts in endpoints,
        if realms_conf does not apply.
    :param realms_conf: A instance of
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
                _('Invalid X-Container-Sync-To format %r') % orig_value,
                None, None, None)
        realm, cluster, account, container = data
        realm_key = realms_conf.key(realm)
        if not realm_key:
            return (_('No realm key for %r') % realm, None, None, None)
        endpoint = realms_conf.endpoint(realm, cluster)
        if not endpoint:
            return (
                _('No cluster endpoint for %r %r') % (realm, cluster),
                None, None, None)
        return (
            None,
            '%s/%s/%s' % (endpoint.rstrip('/'), account, container),
            realm.upper(), realm_key)
    p = urlparse(value)
    if p.scheme not in ('http', 'https'):
        return (
            _('Invalid scheme %r in X-Container-Sync-To, must be "//", '
              '"http", or "https".') % p.scheme,
            None, None, None)
    if not p.path:
        return (_('Path required in X-Container-Sync-To'), None, None, None)
    if p.params or p.query or p.fragment:
        return (
            _('Params, queries, and fragments not allowed in '
              'X-Container-Sync-To'),
            None, None, None)
    if p.hostname not in allowed_sync_hosts:
        return (
            _('Invalid host %r in X-Container-Sync-To') % p.hostname,
            None, None, None)
    return (None, value, None, None)


def affinity_key_function(affinity_str):
    """Turns an affinity config value into a function suitable for passing to
    sort(). After doing so, the array will be sorted with respect to the given
    ordering.

    For example, if affinity_str is "r1=1, r2z7=2, r2z8=2", then the array
    will be sorted with all nodes from region 1 (r1=1) first, then all the
    nodes from region 2 zones 7 and 8 (r2z7=2 and r2z8=2), then everything
    else.

    Note that the order of the pieces of affinity_str is irrelevant; the
    priority values are what comes after the equals sign.

    If affinity_str is empty or all whitespace, then the resulting function
    will not alter the ordering of the nodes.

    :param affinity_str: affinity config value, e.g. "r1z2=3"
                         or "r1=1, r2z1=2, r2z2=2"
    :returns: single-argument function
    :raises: ValueError if argument invalid
    """
    affinity_str = affinity_str.strip()

    if not affinity_str:
        return lambda x: 0

    priority_matchers = []
    pieces = [s.strip() for s in affinity_str.split(',')]
    for piece in pieces:
        # matches r<number>=<number> or r<number>z<number>=<number>
        match = re.match("r(\d+)(?:z(\d+))?=(\d+)$", piece)
        if match:
            region, zone, priority = match.groups()
            region = int(region)
            priority = int(priority)
            zone = int(zone) if zone else None

            matcher = {'region': region, 'priority': priority}
            if zone is not None:
                matcher['zone'] = zone
            priority_matchers.append(matcher)
        else:
            raise ValueError("Invalid affinity value: %r" % affinity_str)

    priority_matchers.sort(key=operator.itemgetter('priority'))

    def keyfn(ring_node):
        for matcher in priority_matchers:
            if (matcher['region'] == ring_node['region']
                and ('zone' not in matcher
                     or matcher['zone'] == ring_node['zone'])):
                return matcher['priority']
        return 4294967296  # 2^32, i.e. "a big number"
    return keyfn


def affinity_locality_predicate(write_affinity_str):
    """
    Turns a write-affinity config value into a predicate function for nodes.
    The returned value will be a 1-arg function that takes a node dictionary
    and returns a true value if it is "local" and a false value otherwise. The
    definition of "local" comes from the affinity_str argument passed in here.

    For example, if affinity_str is "r1, r2z2", then only nodes where region=1
    or where (region=2 and zone=2) are considered local.

    If affinity_str is empty or all whitespace, then the resulting function
    will consider everything local

    :param affinity_str: affinity config value, e.g. "r1z2"
        or "r1, r2z1, r2z2"
    :returns: single-argument function, or None if affinity_str is empty
    :raises: ValueError if argument invalid
    """
    affinity_str = write_affinity_str.strip()

    if not affinity_str:
        return None

    matchers = []
    pieces = [s.strip() for s in affinity_str.split(',')]
    for piece in pieces:
        # matches r<number> or r<number>z<number>
        match = re.match("r(\d+)(?:z(\d+))?$", piece)
        if match:
            region, zone = match.groups()
            region = int(region)
            zone = int(zone) if zone else None

            matcher = {'region': region}
            if zone is not None:
                matcher['zone'] = zone
            matchers.append(matcher)
        else:
            raise ValueError("Invalid write-affinity value: %r" % affinity_str)

    def is_local(ring_node):
        for matcher in matchers:
            if (matcher['region'] == ring_node['region']
                and ('zone' not in matcher
                     or matcher['zone'] == ring_node['zone'])):
                return True
        return False
    return is_local


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
    Function that will check if item is a dict, and if so put it under
    cache_entry[key].  We use nested recon cache entries when the object
    auditor runs in parallel or else in 'once' mode with a specified
    subset of devices.
    """
    if isinstance(item, dict):
        if key not in cache_entry or key in cache_entry and not \
                isinstance(cache_entry[key], dict):
            cache_entry[key] = {}
        elif key in cache_entry and item == {}:
            cache_entry.pop(key, None)
            return
        for k, v in item.items():
            if v == {}:
                cache_entry[key].pop(k, None)
            else:
                cache_entry[key][k] = v
    else:
        cache_entry[key] = item


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
                # file doesn't have a valid entry, we'll recreate it
                pass
            for cache_key, cache_value in cache_dict.items():
                put_recon_cache_entry(cache_entry, cache_key, cache_value)
            try:
                with NamedTemporaryFile(dir=os.path.dirname(cache_file),
                                        delete=False) as tf:
                    tf.write(json.dumps(cache_entry) + '\n')
                renamer(tf.name, cache_file, fsync=False)
            finally:
                try:
                    os.unlink(tf.name)
                except OSError as err:
                    if err.errno != errno.ENOENT:
                        raise
    except (Exception, Timeout):
        logger.exception(_('Exception dumping recon cache'))


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

    :param items: items (no duplicates allowed)
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

    @functools.wraps(func)
    def wrapped(*a, **kw):
        return func(*a, **kw)
    return wrapped


def quorum_size(n):
    """
    quorum size as it applies to services that use 'replication' for data
    integrity  (Account/Container services).  Object quorum_size is defined
    on a storage policy basis.

    Number of successful backend requests needed for the proxy to consider
    the client request successful.
    """
    return (n // 2) + 1


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


class CloseableChain(object):
    """
    Like itertools.chain, but with a close method that will attempt to invoke
    its sub-iterators' close methods, if any.
    """
    def __init__(self, *iterables):
        self.iterables = iterables

    def __iter__(self):
        return iter(itertools.chain(*(self.iterables)))

    def close(self):
        for it in self.iterables:
            close_method = getattr(it, 'close', None)
            if close_method:
                close_method()


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
            return CloseableChain([chunk], iterator)
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


def tpool_reraise(func, *args, **kwargs):
    """
    Hack to work around Eventlet's tpool not catching and reraising Timeouts.
    """
    def inner():
        try:
            return func(*args, **kwargs)
        except BaseException as err:
            return err
    resp = tpool.execute(inner)
    if isinstance(resp, BaseException):
        raise resp
    return resp


class ThreadPool(object):
    """
    Perform blocking operations in background threads.

    Call its methods from within greenlets to green-wait for results without
    blocking the eventlet reactor (hopefully).
    """

    BYTE = 'a'.encode('utf-8')

    def __init__(self, nthreads=2):
        self.nthreads = nthreads
        self._run_queue = Queue()
        self._result_queue = Queue()
        self._threads = []
        self._alive = True

        if nthreads <= 0:
            return

        # We spawn a greenthread whose job it is to pull results from the
        # worker threads via a real Queue and send them to eventlet Events so
        # that the calling greenthreads can be awoken.
        #
        # Since each OS thread has its own collection of greenthreads, it
        # doesn't work to have the worker thread send stuff to the event, as
        # it then notifies its own thread-local eventlet hub to wake up, which
        # doesn't do anything to help out the actual calling greenthread over
        # in the main thread.
        #
        # Thus, each worker sticks its results into a result queue and then
        # writes a byte to a pipe, signaling the result-consuming greenlet (in
        # the main thread) to wake up and consume results.
        #
        # This is all stuff that eventlet.tpool does, but that code can't have
        # multiple instances instantiated. Since the object server uses one
        # pool per disk, we have to reimplement this stuff.
        _raw_rpipe, self.wpipe = os.pipe()
        self.rpipe = greenio.GreenPipe(_raw_rpipe, 'rb', bufsize=0)

        for _junk in xrange(nthreads):
            thr = stdlib_threading.Thread(
                target=self._worker,
                args=(self._run_queue, self._result_queue))
            thr.daemon = True
            thr.start()
            self._threads.append(thr)

        # This is the result-consuming greenthread that runs in the main OS
        # thread, as described above.
        self._consumer_coro = greenthread.spawn_n(self._consume_results,
                                                  self._result_queue)

    def _worker(self, work_queue, result_queue):
        """
        Pulls an item from the queue and runs it, then puts the result into
        the result queue. Repeats forever.

        :param work_queue: queue from which to pull work
        :param result_queue: queue into which to place results
        """
        while True:
            item = work_queue.get()
            if item is None:
                break
            ev, func, args, kwargs = item
            try:
                result = func(*args, **kwargs)
                result_queue.put((ev, True, result))
            except BaseException:
                result_queue.put((ev, False, sys.exc_info()))
            finally:
                work_queue.task_done()
                os.write(self.wpipe, self.BYTE)

    def _consume_results(self, queue):
        """
        Runs as a greenthread in the same OS thread as callers of
        run_in_thread().

        Takes results from the worker OS threads and sends them to the waiting
        greenthreads.
        """
        while True:
            try:
                self.rpipe.read(1)
            except ValueError:
                # can happen at process shutdown when pipe is closed
                break

            while True:
                try:
                    ev, success, result = queue.get(block=False)
                except Empty:
                    break

                try:
                    if success:
                        ev.send(result)
                    else:
                        ev.send_exception(*result)
                finally:
                    queue.task_done()

    def run_in_thread(self, func, *args, **kwargs):
        """
        Runs func(*args, **kwargs) in a thread. Blocks the current greenlet
        until results are available.

        Exceptions thrown will be reraised in the calling thread.

        If the threadpool was initialized with nthreads=0, it invokes
        func(*args, **kwargs) directly, followed by eventlet.sleep() to ensure
        the eventlet hub has a chance to execute. It is more likely the hub
        will be invoked when queuing operations to an external thread.

        :returns: result of calling func
        :raises: whatever func raises
        """
        if not self._alive:
            raise swift.common.exceptions.ThreadPoolDead()

        if self.nthreads <= 0:
            result = func(*args, **kwargs)
            sleep()
            return result

        ev = event.Event()
        self._run_queue.put((ev, func, args, kwargs), block=False)

        # blocks this greenlet (and only *this* greenlet) until the real
        # thread calls ev.send().
        result = ev.wait()
        return result

    def _run_in_eventlet_tpool(self, func, *args, **kwargs):
        """
        Really run something in an external thread, even if we haven't got any
        threads of our own.
        """
        def inner():
            try:
                return (True, func(*args, **kwargs))
            except (Timeout, BaseException) as err:
                return (False, err)

        success, result = tpool.execute(inner)
        if success:
            return result
        else:
            raise result

    def force_run_in_thread(self, func, *args, **kwargs):
        """
        Runs func(*args, **kwargs) in a thread. Blocks the current greenlet
        until results are available.

        Exceptions thrown will be reraised in the calling thread.

        If the threadpool was initialized with nthreads=0, uses eventlet.tpool
        to run the function. This is in contrast to run_in_thread(), which
        will (in that case) simply execute func in the calling thread.

        :returns: result of calling func
        :raises: whatever func raises
        """
        if not self._alive:
            raise swift.common.exceptions.ThreadPoolDead()

        if self.nthreads <= 0:
            return self._run_in_eventlet_tpool(func, *args, **kwargs)
        else:
            return self.run_in_thread(func, *args, **kwargs)

    def terminate(self):
        """
        Releases the threadpool's resources (OS threads, greenthreads, pipes,
        etc.) and renders it unusable.

        Don't call run_in_thread() or force_run_in_thread() after calling
        terminate().
        """
        self._alive = False
        if self.nthreads <= 0:
            return

        for _junk in range(self.nthreads):
            self._run_queue.put(None)
        for thr in self._threads:
            thr.join()
        self._threads = []
        self.nthreads = 0

        greenthread.kill(self._consumer_coro)

        self.rpipe.close()
        os.close(self.wpipe)


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
        # A symlink can never be a mount point
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

    return False


_rfc_token = r'[^()<>@,;:\"/\[\]?={}\x00-\x20\x7f]+'
_rfc_extension_pattern = re.compile(
    r'(?:\s*;\s*(' + _rfc_token + r")\s*(?:=\s*(" + _rfc_token +
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
    :raises: ValueError if malformed
    """
    found = re.search(_content_range_pattern, content_range)
    if not found:
        raise ValueError("malformed Content-Range %r" % (content_range,))
    return tuple(int(x) for x in found.groups())


def parse_content_type(content_type):
    """
    Parse a content-type and its parameters into values.
    RFC 2616 sec 14.17 and 3.7 are pertinent.

    **Examples**::

        'text/plain; charset=UTF-8' -> ('text/plain', [('charset, 'UTF-8')])
        'text/plain; charset=UTF-8; level=1' ->
            ('text/plain', [('charset, 'UTF-8'), ('level', '1')])

    :param content_type: content_type to parse
    :returns: a typle containing (content type, list of k, v parameter tuples)
    """
    parm_list = []
    if ';' in content_type:
        content_type, parms = content_type.split(';', 1)
        parms = ';' + parms
        for m in _rfc_extension_pattern.findall(parms):
            key = m[0].strip()
            value = m[1].strip()
            parm_list.append((key, value))
    return content_type, parm_list


def override_bytes_from_content_type(listing_dict, logger=None):
    """
    Takes a dict from a container listing and overrides the content_type,
    bytes fields if swift_bytes is set.
    """
    content_type, params = parse_content_type(listing_dict['content_type'])
    for key, value in params:
        if key == 'swift_bytes':
            try:
                listing_dict['bytes'] = int(value)
            except ValueError:
                if logger:
                    logger.exception("Invalid swift_bytes")
        else:
            content_type += ';%s=%s' % (key, value)
    listing_dict['content_type'] = content_type


def clean_content_type(value):
    if ';' in value:
        left, right = value.rsplit(';', 1)
        if right.lstrip().startswith('swift_bytes='):
            return left
    return value


def quote(value, safe='/'):
    """
    Patched version of urllib.quote that encodes utf-8 strings before quoting
    """
    return _quote(get_valid_utf8_str(value), safe)


def get_expirer_container(x_delete_at, expirer_divisor, acc, cont, obj):
    """
    Returns a expiring object container name for given X-Delete-At and
    a/c/o.
    """
    shard_int = int(hash_path(acc, cont, obj), 16) % 100
    return normalize_delete_at_timestamp(
        int(x_delete_at) / expirer_divisor * expirer_divisor - shard_int)


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
            return ''

        # read enough data to know whether we're going to run
        # into a boundary in next [length] bytes
        if len(self.input_buffer) < length + len(self.boundary) + 2:
            to_read = length + len(self.boundary) + 2
            while to_read > 0:
                chunk = self.wsgi_input.read(to_read)
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
            self.no_more_files = self.input_buffer.startswith('--')
            self.no_more_data_for_this_file = True
            self.input_buffer = self.input_buffer[2:]
        return ret

    def readline(self):
        if self.no_more_data_for_this_file:
            return ''
        boundary_pos = newline_pos = -1
        while newline_pos < 0 and boundary_pos < 0:
            chunk = self.wsgi_input.read(self.read_chunk_size)
            self.input_buffer += chunk
            newline_pos = self.input_buffer.find('\r\n')
            boundary_pos = self.input_buffer.find(self.boundary)
            if not chunk:
                self.no_more_files = True
                break
        # found a newline
        if newline_pos >= 0 and \
                (boundary_pos < 0 or newline_pos < boundary_pos):
            # Use self.read to ensure any logic there happens...
            ret = ''
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
    yield file-like objects for each part.

    :param wsgi_input: The file-like object to read from.
    :param boundary: The mime boundary to separate new file-like
                     objects on.
    :returns: A generator of file-like objects for each part.
    :raises: MimeInvalid if the document is malformed
    """
    boundary = '--' + boundary
    blen = len(boundary) + 2  # \r\n
    got = wsgi_input.readline(blen)
    if got.strip() != boundary:
        raise swift.common.exceptions.MimeInvalid(
            'invalid starting boundary: wanted %r, got %r', (boundary, got))
    boundary = '\r\n' + boundary
    input_buffer = ''
    done = False
    while not done:
        it = _MultipartMimeFileLikeObject(wsgi_input, boundary, input_buffer,
                                          read_chunk_size)
        yield it
        done = it.no_more_files
        input_buffer = it.input_buffer


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
    if '; ' in header:
        header, attrs = header.split('; ', 1)
    m = True
    while m:
        m = ATTRIBUTES_RE.match(attrs)
        if m:
            attrs = attrs[len(m.group(0)):]
            attributes[m.group(1)] = m.group(2).strip('"')
    return header, attributes


class sockaddr_alg(ctypes.Structure):
    _fields_ = [("salg_family", ctypes.c_ushort),
                ("salg_type", ctypes.c_ubyte * 14),
                ("salg_feat", ctypes.c_uint),
                ("salg_mask", ctypes.c_uint),
                ("salg_name", ctypes.c_ubyte * 64)]


_bound_md5_sockfd = None


def get_md5_socket():
    """
    Get an MD5 socket file descriptor. One can MD5 data with it by writing it
    to the socket with os.write, then os.read the 16 bytes of the checksum out
    later.

    NOTE: It is the caller's responsibility to ensure that os.close() is
    called on the returned file descriptor. This is a bare file descriptor,
    not a Python object. It doesn't close itself.
    """

    # Linux's AF_ALG sockets work like this:
    #
    # First, initialize a socket with socket() and bind(). This tells the
    # socket what algorithm to use, as well as setting up any necessary bits
    # like crypto keys. Of course, MD5 doesn't need any keys, so it's just the
    # algorithm name.
    #
    # Second, to hash some data, get a second socket by calling accept() on
    # the first socket. Write data to the socket, then when finished, read the
    # checksum from the socket and close it. This lets you checksum multiple
    # things without repeating all the setup code each time.
    #
    # Since we only need to bind() one socket, we do that here and save it for
    # future re-use. That way, we only use one file descriptor to get an MD5
    # socket instead of two, and we also get to save some syscalls.

    global _bound_md5_sockfd
    global _libc_socket
    global _libc_bind
    global _libc_accept

    if _libc_accept is None:
        _libc_accept = load_libc_function('accept', fail_if_missing=True)
    if _libc_socket is None:
        _libc_socket = load_libc_function('socket', fail_if_missing=True)
    if _libc_bind is None:
        _libc_bind = load_libc_function('bind', fail_if_missing=True)

    # Do this at first call rather than at import time so that we don't use a
    # file descriptor on systems that aren't using any MD5 sockets.
    if _bound_md5_sockfd is None:
        sockaddr_setup = sockaddr_alg(
            AF_ALG,
            (ord('h'), ord('a'), ord('s'), ord('h'), 0),
            0, 0,
            (ord('m'), ord('d'), ord('5'), 0))
        hash_sockfd = _libc_socket(ctypes.c_int(AF_ALG),
                                   ctypes.c_int(socket.SOCK_SEQPACKET),
                                   ctypes.c_int(0))
        if hash_sockfd < 0:
            raise IOError(ctypes.get_errno(),
                          "Failed to initialize MD5 socket")

        bind_result = _libc_bind(ctypes.c_int(hash_sockfd),
                                 ctypes.pointer(sockaddr_setup),
                                 ctypes.c_int(ctypes.sizeof(sockaddr_alg)))
        if bind_result < 0:
            os.close(hash_sockfd)
            raise IOError(ctypes.get_errno(), "Failed to bind MD5 socket")

        _bound_md5_sockfd = hash_sockfd

    md5_sockfd = _libc_accept(ctypes.c_int(_bound_md5_sockfd), None, 0)
    if md5_sockfd < 0:
        raise IOError(ctypes.get_errno(), "Failed to accept MD5 socket")

    return md5_sockfd
