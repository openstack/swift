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

import base64
import binascii
import bisect
import collections
import errno
import fcntl
import grp
import hashlib
import hmac
import json
import math
import operator
import os
import pwd
import re
import string
import struct
import sys
import time
import uuid
import functools
import platform
import email.parser
from hashlib import md5, sha1
from random import random, shuffle
from contextlib import contextmanager, closing
import ctypes
import ctypes.util
from optparse import OptionParser

from tempfile import gettempdir, mkstemp, NamedTemporaryFile
import glob
import itertools
import stat
import datetime

import eventlet
import eventlet.debug
import eventlet.greenthread
import eventlet.patcher
import eventlet.semaphore
import pkg_resources
from eventlet import GreenPool, sleep, Timeout
from eventlet.green import socket, threading
from eventlet.hubs import trampoline
import eventlet.queue
import netifaces
import codecs
utf8_decoder = codecs.getdecoder('utf-8')
utf8_encoder = codecs.getencoder('utf-8')
import six
if not six.PY2:
    utf16_decoder = codecs.getdecoder('utf-16')
    utf16_encoder = codecs.getencoder('utf-16')
from six.moves import cPickle as pickle
from six.moves.configparser import (ConfigParser, NoSectionError,
                                    NoOptionError, RawConfigParser)
from six.moves import range, http_client
from six.moves.urllib.parse import ParseResult
from six.moves.urllib.parse import quote as _quote
from six.moves.urllib.parse import urlparse as stdlib_urlparse

from swift import gettext_ as _
import swift.common.exceptions
from swift.common.http import is_server_error
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.linkat import linkat

# logging doesn't import patched as cleanly as one would like
from logging.handlers import SysLogHandler
import logging
logging.thread = eventlet.green.thread
logging.threading = eventlet.green.threading
logging._lock = logging.threading.RLock()
# setup notice level logging
NOTICE = 25
logging.addLevelName(NOTICE, 'NOTICE')
SysLogHandler.priority_map['NOTICE'] = 'notice'

# These are lazily pulled from libc elsewhere
_sys_fallocate = None
_posix_fadvise = None
_libc_socket = None
_libc_bind = None
_libc_accept = None
# see man -s 2 setpriority
_libc_setpriority = None
# see man -s 2 syscall
_posix_syscall = None

# If set to non-zero, fallocate routines will fail based on free space
# available being at or below this amount, in bytes.
FALLOCATE_RESERVE = 0
# Indicates if FALLOCATE_RESERVE is the percentage of free space (True) or
# the number of bytes (False).
FALLOCATE_IS_PERCENT = False

# from /usr/include/linux/falloc.h
FALLOC_FL_KEEP_SIZE = 1
FALLOC_FL_PUNCH_HOLE = 2

# from /usr/src/linux-headers-*/include/uapi/linux/resource.h
PRIO_PROCESS = 0


# /usr/include/x86_64-linux-gnu/asm/unistd_64.h defines syscalls there
# are many like it, but this one is mine, see man -s 2 ioprio_set
def NR_ioprio_set():
    """Give __NR_ioprio_set value for your system."""
    architecture = os.uname()[4]
    arch_bits = platform.architecture()[0]
    # check if supported system, now support x86_64 and AArch64
    if architecture == 'x86_64' and arch_bits == '64bit':
        return 251
    elif architecture == 'aarch64' and arch_bits == '64bit':
        return 30
    raise OSError("Swift doesn't support ionice priority for %s %s" %
                  (architecture, arch_bits))

# this syscall integer probably only works on x86_64 linux systems, you
# can check if it's correct on yours with something like this:
"""
#include <stdio.h>
#include <sys/syscall.h>

int main(int argc, const char* argv[]) {
    printf("%d\n", __NR_ioprio_set);
    return 0;
}
"""

# this is the value for "which" that says our who value will be a pid
# pulled out of /usr/src/linux-headers-*/include/linux/ioprio.h
IOPRIO_WHO_PROCESS = 1


IO_CLASS_ENUM = {
    'IOPRIO_CLASS_RT': 1,
    'IOPRIO_CLASS_BE': 2,
    'IOPRIO_CLASS_IDLE': 3,
}

# the IOPRIO_PRIO_VALUE "macro" is also pulled from
# /usr/src/linux-headers-*/include/linux/ioprio.h
IOPRIO_CLASS_SHIFT = 13


def IOPRIO_PRIO_VALUE(class_, data):
    return (((class_) << IOPRIO_CLASS_SHIFT) | data)

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
AF_ALG = getattr(socket, 'AF_ALG', 38)
F_SETPIPE_SZ = getattr(fcntl, 'F_SETPIPE_SZ', 1031)
O_TMPFILE = getattr(os, 'O_TMPFILE', 0o20000000 | os.O_DIRECTORY)

# Used by the parse_socket_string() function to validate IPv6 addresses
IPV6_RE = re.compile("^\[(?P<address>.*)\](:(?P<port>[0-9]+))?$")

MD5_OF_EMPTY_STRING = 'd41d8cd98f00b204e9800998ecf8427e'


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

        if six.PY3:
            # Use Latin1 to accept arbitrary bytes in the hash prefix/suffix
            confs_read = hash_conf.read(SWIFT_CONF_FILE, encoding='latin1')
        else:
            confs_read = hash_conf.read(SWIFT_CONF_FILE)

        if confs_read:
            try:
                HASH_PATH_SUFFIX = hash_conf.get('swift-hash',
                                                 'swift_hash_path_suffix')
                if six.PY3:
                    HASH_PATH_SUFFIX = HASH_PATH_SUFFIX.encode('latin1')
            except (NoSectionError, NoOptionError):
                pass
            try:
                HASH_PATH_PREFIX = hash_conf.get('swift-hash',
                                                 'swift_hash_path_prefix')
                if six.PY3:
                    HASH_PATH_PREFIX = HASH_PATH_PREFIX.encode('latin1')
            except (NoSectionError, NoOptionError):
                pass

        if not HASH_PATH_SUFFIX and not HASH_PATH_PREFIX:
            raise InvalidHashPathConfigError()


try:
    validate_hash_conf()
except InvalidHashPathConfigError:
    # could get monkey patched or lazy loaded
    pass


def get_hmac(request_method, path, expires, key, digest=sha1,
             ip_range=None):
    """
    Returns the hexdigest string of the HMAC (see RFC 2104) for
    the request.

    :param request_method: Request method to allow.
    :param path: The path to the resource to allow access to.
    :param expires: Unix timestamp as an int for when the URL
                    expires.
    :param key: HMAC shared secret.
    :param digest: constructor for the digest to use in calculating the HMAC
                   Defaults to SHA1
    :param ip_range: The ip range from which the resource is allowed
                     to be accessed. We need to put the ip_range as the
                     first argument to hmac to avoid manipulation of the path
                     due to newlines being valid in paths
                     e.g. /v1/a/c/o\\n127.0.0.1
    :returns: hexdigest str of the HMAC for the request using the specified
              digest algorithm.
    """
    # These are the three mandatory fields.
    parts = [request_method, str(expires), path]
    formats = [b"%s", b"%s", b"%s"]

    if ip_range:
        parts.insert(0, ip_range)
        formats.insert(0, b"ip=%s")

    if not isinstance(key, six.binary_type):
        key = key.encode('utf8')

    message = b'\n'.join(
        fmt % (part if isinstance(part, six.binary_type)
               else part.encode("utf-8"))
        for fmt, part in zip(formats, parts))

    return hmac.new(key, message, digest).hexdigest()


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
    for key, val in kwargs.items():
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


# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'on', 't', 'y'))


def config_true_value(value):
    """
    Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    """
    return value is True or \
        (isinstance(value, six.string_types) and value.lower() in TRUE_VALUES)


def config_positive_int_value(value):
    """
    Returns positive int value if it can be cast by int() and it's an
    integer > 0. (not including zero) Raises ValueError otherwise.
    """
    try:
        result = int(value)
        if result < 1:
            raise ValueError()
    except (TypeError, ValueError):
        raise ValueError(
            'Config option must be an positive int number, not "%s".' % value)
    return result


def config_float_value(value, minimum=None, maximum=None):
    try:
        val = float(value)
        if minimum is not None and val < minimum:
            raise ValueError()
        if maximum is not None and val > maximum:
            raise ValueError()
        return val
    except (TypeError, ValueError):
        min_ = ', greater than %s' % minimum if minimum is not None else ''
        max_ = ', less than %s' % maximum if maximum is not None else ''
        raise ValueError('Config option must be a number%s%s, not "%s".' %
                         (min_, max_, value))


def config_auto_int_value(value, default):
    """
    Returns default if value is None or 'auto'.
    Returns value as an int or raises ValueError otherwise.
    """
    if value is None or \
       (isinstance(value, six.string_types) and value.lower() == 'auto'):
        return default
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise ValueError('Config option must be an integer or the '
                         'string "auto", not "%s".' % value)
    return value


def append_underscore(prefix):
    if prefix and not prefix.endswith('_'):
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


def noop_libc_function(*args):
    return 0


def validate_configuration():
    try:
        validate_hash_conf()
    except InvalidHashPathConfigError as e:
        sys.exit("Error: %s" % e)


def load_libc_function(func_name, log_error=True,
                       fail_if_missing=False, errcheck=False):
    """
    Attempt to find the function in libc, otherwise return a no-op func.

    :param func_name: name of the function to pull from libc.
    :param log_error: log an error when a function can't be found
    :param fail_if_missing: raise an exception when a function can't be found.
                            Default behavior is to return a no-op function.
    :param errcheck: boolean, if true install a wrapper on the function
                     to check for a return values of -1 and call
                     ctype.get_errno and raise an OSError
    """
    try:
        libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)
        func = getattr(libc, func_name)
    except AttributeError:
        if fail_if_missing:
            raise
        if log_error:
            logging.warning(_("Unable to locate %s in libc.  Leaving as a "
                            "no-op."), func_name)
        return noop_libc_function
    if errcheck:
        def _errcheck(result, f, args):
            if result == -1:
                errcode = ctypes.get_errno()
                raise OSError(errcode, os.strerror(errcode))
            return result
        func.errcheck = _errcheck
    return func


def generate_trans_id(trans_id_suffix):
    return 'tx%s-%010x%s' % (
        uuid.uuid4().hex[:21], int(time.time()), quote(trans_id_suffix))


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
    if isinstance(policy_index, six.binary_type) and not six.PY2:
        policy_index = policy_index.decode('ascii')
    return str(policy_index) if policy_index is not None else None


def get_log_line(req, res, trans_time, additional_info):
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
    return '%s - - [%s] "%s %s" %s %s "%s" "%s" "%s" %.4f "%s" %d %s' % (
        req.remote_addr,
        time.strftime('%d/%b/%Y:%H:%M:%S +0000', time.gmtime()),
        req.method, req.path, res.status.split()[0],
        res.content_length or '-', req.referer or '-',
        req.headers.get('x-trans-id', '-'),
        req.user_agent or '-', trans_time, additional_info or '-',
        os.getpid(), policy_index or '-')


def get_trans_id_time(trans_id):
    if len(trans_id) >= 34 and \
       trans_id.startswith('tx') and trans_id[23] == '-':
        try:
            return int(trans_id[24:34], 16)
        except ValueError:
            pass
    return None


def config_fallocate_value(reserve_value):
    """
    Returns fallocate reserve_value as an int or float.
    Returns is_percent as a boolean.
    Returns a ValueError on invalid fallocate value.
    """
    try:
        if str(reserve_value[-1:]) == '%':
            reserve_value = float(reserve_value[:-1])
            is_percent = True
        else:
            reserve_value = int(reserve_value)
            is_percent = False
    except ValueError:
        raise ValueError('Error: %s is an invalid value for fallocate'
                         '_reserve.' % reserve_value)
    return reserve_value, is_percent


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

    def next(self):
        """
        next(x) -> the next value, or raise StopIteration
        """
        if self.closed:
            raise ValueError('I/O operation on closed file')
        if self.buf:
            rv = self.buf
            self.buf = None
            return rv
        else:
            return next(self.iterator)
    __next__ = next

    def read(self, size=-1):
        """
        read([size]) -> read at most size bytes, returned as a bytes string.

        If the size argument is negative or omitted, read until EOF is reached.
        Notice that when in non-blocking mode, less data than what was
        requested may be returned, even if no size parameter was given.
        """
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
        readline([size]) -> next line from the file, as a bytes string.

        Retain newline.  A non-negative size argument limits the maximum
        number of bytes to return (an incomplete line may be returned then).
        Return an empty string at EOF.
        """
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
        readlines([size]) -> list of bytes strings, each a line from the file.

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


def fs_has_free_space(fs_path, space_needed, is_percent):
    """
    Check to see whether or not a filesystem has the given amount of space
    free. Unlike fallocate(), this does not reserve any space.

    :param fs_path: path to a file or directory on the filesystem; typically
        the path to the filesystem's mount point

    :param space_needed: minimum bytes or percentage of free space

    :param is_percent: if True, then space_needed is treated as a percentage
        of the filesystem's capacity; if False, space_needed is a number of
        free bytes.

    :returns: True if the filesystem has at least that much free space,
        False otherwise

    :raises OSError: if fs_path does not exist
    """
    st = os.statvfs(fs_path)
    free_bytes = st.f_frsize * st.f_bavail
    if is_percent:
        size_bytes = st.f_frsize * st.f_blocks
        free_percent = float(free_bytes) / float(size_bytes) * 100
        return free_percent >= space_needed
    else:
        return free_bytes >= space_needed


class _LibcWrapper(object):
    """
    A callable object that forwards its calls to a C function from libc.

    These objects are lazy. libc will not be checked until someone tries to
    either call the function or check its availability.

    _LibcWrapper objects have an "available" property; if true, then libc
    has the function of that name. If false, then calls will fail with a
    NotImplementedError.
    """
    def __init__(self, func_name):
        self._func_name = func_name
        self._func_handle = None
        self._loaded = False

    def _ensure_loaded(self):
        if not self._loaded:
            func_name = self._func_name
            try:
                # Keep everything in this try-block in local variables so
                # that a typo in self.some_attribute_name doesn't raise a
                # spurious AttributeError.
                func_handle = load_libc_function(
                    func_name, fail_if_missing=True)
            except AttributeError:
                # We pass fail_if_missing=True to load_libc_function and
                # then ignore the error. It's weird, but otherwise we have
                # to check if self._func_handle is noop_libc_function, and
                # that's even weirder.
                pass
            else:
                self._func_handle = func_handle
            self._loaded = True

    @property
    def available(self):
        self._ensure_loaded()
        return bool(self._func_handle)

    def __call__(self, *args):
        if self.available:
            return self._func_handle(*args)
        else:
            raise NotImplementedError(
                "No function %r found in libc" % self._func_name)


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
            free = \
                (float(free) / float(st.f_frsize * st.f_blocks)) * 100
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
            logging.warning(_("Unable to locate fallocate, posix_fallocate in "
                              "libc.  Leaving as a no-op."))
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
        logging.warning(_('Unable to perform fsync() on directory %(dir)s:'
                          ' %(err)s'),
                        {'dir': dirpath, 'err': os.strerror(err.errno)})
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
        logging.warning("posix_fadvise64(%(fd)s, %(offset)s, %(length)s, 4) "
                        "-> %(ret)s", {'fd': fd, 'offset': offset,
                                       'length': length, 'ret': ret})


NORMAL_FORMAT = "%016.05f"
INTERNAL_FORMAT = NORMAL_FORMAT + '_%016x'
SHORT_FORMAT = NORMAL_FORMAT + '_%x'
MAX_OFFSET = (16 ** 16) - 1
PRECISION = 1e-5
# Setting this to True will cause the internal format to always display
# extended digits - even when the value is equivalent to the normalized form.
# This isn't ideal during an upgrade when some servers might not understand
# the new time format - but flipping it to True works great for testing.
FORCE_INTERNAL = False  # or True


@functools.total_ordering
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

    def __init__(self, timestamp, offset=0, delta=0):
        """
        Create a new Timestamp.

        :param timestamp: time in seconds since the Epoch, may be any of:

            * a float or integer
            * normalized/internalized string
            * another instance of this class (offset is preserved)

        :param offset: the second internal offset vector, an int
        :param delta: deca-microsecond difference from the base timestamp
                      param, an int
        """
        if isinstance(timestamp, bytes):
            timestamp = timestamp.decode('ascii')
        if isinstance(timestamp, six.string_types):
            base, base_offset = timestamp.partition('_')[::2]
            self.timestamp = float(base)
            if '_' in base_offset:
                raise ValueError('invalid literal for int() with base 16: '
                                 '%r' % base_offset)
            if base_offset:
                self.offset = int(base_offset, 16)
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
        self.raw = int(round(self.timestamp / PRECISION))
        # add delta
        if delta:
            self.raw = self.raw + delta
            if self.raw <= 0:
                raise ValueError(
                    'delta must be greater than %d' % (-1 * self.raw))
            self.timestamp = float(self.raw * PRECISION)
        if self.timestamp < 0:
            raise ValueError('timestamp cannot be negative')
        if self.timestamp >= 10000000000:
            raise ValueError('timestamp too large')

    @classmethod
    def now(cls, offset=0, delta=0):
        return cls(time.time(), offset=offset, delta=delta)

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

    def __bool__(self):
        return self.__nonzero__()

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
    def short(self):
        if self.offset or FORCE_INTERNAL:
            return SHORT_FORMAT % (self.timestamp, self.offset)
        else:
            return self.normal

    @property
    def isoformat(self):
        t = float(self.normal)
        if six.PY3:
            # On Python 3, round manually using ROUND_HALF_EVEN rounding
            # method, to use the same rounding method than Python 2. Python 3
            # used a different rounding method, but Python 3.4.4 and 3.5.1 use
            # again ROUND_HALF_EVEN as Python 2.
            # See https://bugs.python.org/issue23517
            frac, t = math.modf(t)
            us = round(frac * 1e6)
            if us >= 1000000:
                t += 1
                us -= 1000000
            elif us < 0:
                t -= 1
                us += 1000000
            dt = datetime.datetime.utcfromtimestamp(t)
            dt = dt.replace(microsecond=us)
        else:
            dt = datetime.datetime.utcfromtimestamp(t)

        isoformat = dt.isoformat()
        # python isoformat() doesn't include msecs when zero
        if len(isoformat) < len("1970-01-01T00:00:00.000000"):
            isoformat += ".000000"
        return isoformat

    def __eq__(self, other):
        if other is None:
            return False
        if not isinstance(other, Timestamp):
            other = Timestamp(other)
        return self.internal == other.internal

    def __ne__(self, other):
        if other is None:
            return True
        if not isinstance(other, Timestamp):
            other = Timestamp(other)
        return self.internal != other.internal

    def __lt__(self, other):
        if other is None:
            return False
        if not isinstance(other, Timestamp):
            other = Timestamp(other)
        return self.internal < other.internal

    def __hash__(self):
        return hash(self.internal)


def encode_timestamps(t1, t2=None, t3=None, explicit=False):
    """
    Encode up to three timestamps into a string. Unlike a Timestamp object, the
    encoded string does NOT used fixed width fields and consequently no
    relative chronology of the timestamps can be inferred from lexicographic
    sorting of encoded timestamp strings.

    The format of the encoded string is:
        <t1>[<+/-><t2 - t1>[<+/-><t3 - t2>]]

    i.e. if t1 = t2 = t3 then just the string representation of t1 is returned,
    otherwise the time offsets for t2 and t3 are appended. If explicit is True
    then the offsets for t2 and t3 are always appended even if zero.

    Note: any offset value in t1 will be preserved, but offsets on t2 and t3
    are not preserved. In the anticipated use cases for this method (and the
    inverse decode_timestamps method) the timestamps passed as t2 and t3 are
    not expected to have offsets as they will be timestamps associated with a
    POST request. In the case where the encoding is used in a container objects
    table row, t1 could be the PUT or DELETE time but t2 and t3 represent the
    content type and metadata times (if different from the data file) i.e.
    correspond to POST timestamps. In the case where the encoded form is used
    in a .meta file name, t1 and t2 both correspond to POST timestamps.
    """
    form = '{0}'
    values = [t1.short]
    if t2 is not None:
        t2_t1_delta = t2.raw - t1.raw
        explicit = explicit or (t2_t1_delta != 0)
        values.append(t2_t1_delta)
        if t3 is not None:
            t3_t2_delta = t3.raw - t2.raw
            explicit = explicit or (t3_t2_delta != 0)
            values.append(t3_t2_delta)
        if explicit:
            form += '{1:+x}'
            if t3 is not None:
                form += '{2:+x}'
    return form.format(*values)


def decode_timestamps(encoded, explicit=False):
    """
    Parses a string of the form generated by encode_timestamps and returns
    a tuple of the three component timestamps. If explicit is False, component
    timestamps that are not explicitly encoded will be assumed to have zero
    delta from the previous component and therefore take the value of the
    previous component. If explicit is True, component timestamps that are
    not explicitly encoded will be returned with value None.
    """
    # TODO: some tests, e.g. in test_replicator, put float timestamps values
    # into container db's, hence this defensive check, but in real world
    # this may never happen.
    if not isinstance(encoded, six.string_types):
        ts = Timestamp(encoded)
        return ts, ts, ts

    parts = []
    signs = []
    pos_parts = encoded.split('+')
    for part in pos_parts:
        # parse time components and their signs
        # e.g. x-y+z --> parts = [x, y, z] and signs = [+1, -1, +1]
        neg_parts = part.split('-')
        parts = parts + neg_parts
        signs = signs + [1] + [-1] * (len(neg_parts) - 1)
    t1 = Timestamp(parts[0])
    t2 = t3 = None
    if len(parts) > 1:
        t2 = t1
        delta = signs[1] * int(parts[1], 16)
        # if delta = 0 we want t2 = t3 = t1 in order to
        # preserve any offset in t1 - only construct a distinct
        # timestamp if there is a non-zero delta.
        if delta:
            t2 = Timestamp((t1.raw + delta) * PRECISION)
    elif not explicit:
        t2 = t1
    if len(parts) > 2:
        t3 = t2
        delta = signs[2] * int(parts[2], 16)
        if delta:
            t3 = Timestamp((t2.raw + delta) * PRECISION)
    elif not explicit:
        t3 = t2
    return t1, t2, t3


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

    # This calculation is based on Python 2.7's Modules/datetimemodule.c,
    # function delta_to_microseconds(), but written in Python.
    return Timestamp(delta.total_seconds())


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
    for _junk in range(0, retries):
        try:
            linkat(linkat.AT_FDCWD, "/proc/self/fd/%d" % (fd),
                   linkat.AT_FDCWD, target_path, linkat.AT_SYMLINK_FOLLOW)
            break
        except IOError as err:
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
    :raises ValueError: if given an invalid path
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
        self.running_time = 0
        self.ratelimit_if = ratelimit_if

    def __iter__(self):
        return self

    def next(self):
        next_value = next(self.iterator)

        if self.ratelimit_if(next_value):
            if self.limit_after > 0:
                self.limit_after -= 1
            else:
                self.running_time = ratelimit_sleep(self.running_time,
                                                    self.elements_per_second)
        return next_value
    __next__ = next


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
            return next(self.unsafe_iter)
    __next__ = next


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
                        _('%s: Connection reset by peer'), self.log_type)
                else:
                    self.logger.error(_('%(type)s: %(value)s'),
                                      {'type': self.log_type, 'value': value})
        finally:
            self._cls_thread_local.already_called_write = False

    def writelines(self, values):
        if getattr(self._cls_thread_local, 'already_called_writelines', False):
            return

        self._cls_thread_local.already_called_writelines = True
        try:
            self.logger.error(_('%(type)s: %(value)s'),
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

    def next(self):
        raise IOError(errno.EBADF, 'Bad file descriptor')
    __next__ = next

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
        self.random = random
        self.logger = logger

        # Determine if host is IPv4 or IPv6
        addr_info = None
        try:
            addr_info = socket.getaddrinfo(host, port, socket.AF_INET)
            self._sock_family = socket.AF_INET
        except socket.gaierror:
            try:
                addr_info = socket.getaddrinfo(host, port, socket.AF_INET6)
                self._sock_family = socket.AF_INET6
            except socket.gaierror:
                # Don't keep the server from starting from what could be a
                # transient DNS failure.  Any hostname will get re-resolved as
                # necessary in the .sendto() calls.
                # However, we don't know if we're IPv4 or IPv6 in this case, so
                # we assume legacy IPv4.
                self._sock_family = socket.AF_INET

        # NOTE: we use the original host value, not the DNS-resolved one
        # because if host is a hostname, we don't want to cache the DNS
        # resolution for the entire lifetime of this process.  Let standard
        # name resolution caching take effect.  This should help operators use
        # DNS trickery if they want.
        if addr_info is not None:
            # addr_info is a list of 5-tuples with the following structure:
            #     (family, socktype, proto, canonname, sockaddr)
            # where sockaddr is the only thing of interest to us, and we only
            # use the first result.  We want to use the originally supplied
            # host (see note above) and the remainder of the variable-length
            # sockaddr: IPv4 has (address, port) while IPv6 has (address,
            # port, flow info, scope id).
            sockaddr = addr_info[0][-1]
            self._target = (host,) + (sockaddr[1:])
        else:
            self._target = (host, port)

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
        if six.PY3:
            parts = [part.encode('utf-8') for part in parts]
        # Ideally, we'd cache a sending socket in self, but that
        # results in a socket getting shared by multiple green threads.
        with closing(self._open_socket()) as sock:
            try:
                return sock.sendto(b'|'.join(parts), self._target)
            except IOError as err:
                if self.logger:
                    self.logger.warning(
                        _('Error sending UDP message to %(target)r: %(err)s'),
                        {'target': self._target, 'err': err})

    def _open_socket(self):
        return socket.socket(self._sock_family, socket.SOCK_DGRAM)

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


class SwiftLoggerAdapter(logging.LoggerAdapter):
    """
    A logging.LoggerAdapter subclass that also passes through StatsD method
    calls.

    Like logging.LoggerAdapter, you have to subclass this and override the
    process() method to accomplish anything useful.
    """
    def update_stats(self, *a, **kw):
        return self.logger.update_stats(*a, **kw)

    def increment(self, *a, **kw):
        return self.logger.increment(*a, **kw)

    def decrement(self, *a, **kw):
        return self.logger.decrement(*a, **kw)

    def timing(self, *a, **kw):
        return self.logger.timing(*a, **kw)

    def timing_since(self, *a, **kw):
        return self.logger.timing_since(*a, **kw)

    def transfer_rate(self, *a, **kw):
        return self.logger.transfer_rate(*a, **kw)


class PrefixLoggerAdapter(SwiftLoggerAdapter):
    """
    Adds an optional prefix to all its log messages. When the prefix has not
    been set, messages are unchanged.
    """
    def set_prefix(self, prefix):
        self.extra['prefix'] = prefix

    def exception(self, *a, **kw):
        self.logger.exception(*a, **kw)

    def process(self, msg, kwargs):
        msg, kwargs = super(PrefixLoggerAdapter, self).process(msg, kwargs)
        if 'prefix' in self.extra:
            msg = self.extra['prefix'] + msg
        return (msg, kwargs)


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
        self.warn = self.warning

    # There are a few properties needed for py35; see
    # - https://bugs.python.org/issue31457
    # - https://github.com/python/cpython/commit/1bbd482
    # - https://github.com/python/cpython/commit/0b6a118
    # - https://github.com/python/cpython/commit/ce9e625
    def _log(self, level, msg, args, exc_info=None, extra=None,
             stack_info=False):
        """
        Low-level log implementation, proxied to allow nested logger adapters.
        """
        return self.logger._log(
            level,
            msg,
            args,
            exc_info=exc_info,
            extra=extra,
            stack_info=stack_info,
        )

    @property
    def manager(self):
        return self.logger.manager

    @manager.setter
    def manager(self, value):
        self.logger.manager = value

    @property
    def name(self):
        return self.logger.name

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
        if isinstance(exc, (OSError, socket.error)):
            if exc.errno in (errno.EIO, errno.ENOSPC):
                emsg = str(exc)
            elif exc.errno == errno.ECONNREFUSED:
                emsg = _('Connection refused')
            elif exc.errno == errno.ECONNRESET:
                emsg = _('Connection reset')
            elif exc.errno == errno.EHOSTUNREACH:
                emsg = _('Host unreachable')
            elif exc.errno == errno.ENETUNREACH:
                emsg = _('Network unreachable')
            elif exc.errno == errno.ETIMEDOUT:
                emsg = _('Connection timeout')
            else:
                call = self._exception
        elif isinstance(exc, http_client.BadStatusLine):
            # Use error(); not really exceptional
            emsg = '%s: %s' % (exc.__class__.__name__, exc.line)
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
        handler = ThreadSafeSysLogHandler(address=(udp_host, udp_port),
                                          facility=facility)
    else:
        log_address = conf.get('log_address', '/dev/log')
        try:
            handler = ThreadSafeSysLogHandler(address=log_address,
                                              facility=facility)
        except socket.error as e:
            # Either /dev/log isn't a UNIX socket or it does not exist at all
            if e.errno not in [errno.ENOTSOCK, errno.ENOENT]:
                raise
            handler = ThreadSafeSysLogHandler(facility=facility)
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

    Another note about epoll: it's hard to use when forking. epoll works
    like so:

       * create an epoll instance: efd = epoll_create(...)

       * register file descriptors of interest with epoll_ctl(efd,
             EPOLL_CTL_ADD, fd, ...)

       * wait for events with epoll_wait(efd, ...)

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


def drop_privileges(user, call_setsid=True):
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
    if call_setsid:
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
    """Parse standard swift server/daemon options with optparse.OptionParser.

    :param parser: OptionParser to use. If not sent one will be created.
    :param once: Boolean indicating the "once" option is available
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


def is_valid_ip(ip):
    """
    Return True if the provided ip is a valid IP-address
    """
    return is_valid_ipv4(ip) or is_valid_ipv6(ip)


def is_valid_ipv4(ip):
    """
    Return True if the provided ip is a valid IPv4-address
    """
    try:
        socket.inet_pton(socket.AF_INET, ip)
    except socket.error:  # not a valid IPv4 address
        return False
    return True


def is_valid_ipv6(ip):
    """
    Returns True if the provided ip is a valid IPv6-address
    """
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except socket.error:  # not a valid IPv6 address
        return False
    return True


def expand_ipv6(address):
    """
    Expand ipv6 address.
    :param address: a string indicating valid ipv6 address
    :returns: a string indicating fully expanded ipv6 address

    """
    packed_ip = socket.inet_pton(socket.AF_INET6, address)
    return socket.inet_ntop(socket.AF_INET6, packed_ip)


def whataremyips(bind_ip=None):
    """
    Get "our" IP addresses ("us" being the set of services configured by
    one `*.conf` file). If our REST listens on a specific address, return it.
    Otherwise, if listen on '0.0.0.0' or '::' return all addresses, including
    the loopback.

    :param str bind_ip: Optional bind_ip from a config file; may be IP address
                        or hostname.
    :returns: list of Strings of ip addresses
    """
    if bind_ip:
        # See if bind_ip is '0.0.0.0'/'::'
        try:
            _, _, _, _, sockaddr = socket.getaddrinfo(
                bind_ip, None, 0, socket.SOCK_STREAM, 0,
                socket.AI_NUMERICHOST)[0]
            if sockaddr[0] not in ('0.0.0.0', '::'):
                return [bind_ip]
        except socket.gaierror:
            pass

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


def parse_socket_string(socket_string, default_port):
    """
    Given a string representing a socket, returns a tuple of (host, port).
    Valid strings are DNS names, IPv4 addresses, or IPv6 addresses, with an
    optional port. If an IPv6 address is specified it **must** be enclosed in
    [], like *[::1]* or *[::1]:11211*. This follows the accepted prescription
    for `IPv6 host literals`_.

    Examples::

        server.org
        server.org:1337
        127.0.0.1:1337
        [::1]:1337
        [::1]

    .. _IPv6 host literals: https://tools.ietf.org/html/rfc3986#section-3.2.2
    """
    port = default_port
    # IPv6 addresses must be between '[]'
    if socket_string.startswith('['):
        match = IPV6_RE.match(socket_string)
        if not match:
            raise ValueError("Invalid IPv6 address: %s" % socket_string)
        host = match.group('address')
        port = match.group('port') or port
    else:
        if ':' in socket_string:
            tokens = socket_string.split(':')
            if len(tokens) > 2:
                raise ValueError("IPv6 addresses must be between '[]'")
            host, port = tokens
        else:
            host = socket_string
    return (host, port)


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
    paths = [account if isinstance(account, six.binary_type)
             else account.encode('utf8')]
    if container:
        paths.append(container if isinstance(container, six.binary_type)
                     else container.encode('utf8'))
    if object:
        paths.append(object if isinstance(object, six.binary_type)
                     else object.encode('utf8'))
    if raw_digest:
        return md5(HASH_PATH_PREFIX + b'/' + b'/'.join(paths)
                   + HASH_PATH_SUFFIX).digest()
    else:
        return md5(HASH_PATH_PREFIX + b'/' + b'/'.join(paths)
                   + HASH_PATH_SUFFIX).hexdigest()


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
def lock_path(directory, timeout=10, timeout_class=None, limit=1, name=None):
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
    :param limit: The maximum number of locks that may be held concurrently on
        the same directory at the time this method is called. Note that this
        limit is only applied during the current call to this method and does
        not prevent subsequent calls giving a larger limit. Defaults to 1.
    :param name: A string to distinguishes different type of locks in a
        directory
    :raises TypeError: if limit is not an int.
    :raises ValueError: if limit is less than 1.
    """
    if limit < 1:
        raise ValueError('limit must be greater than or equal to 1')
    if timeout_class is None:
        timeout_class = swift.common.exceptions.LockTimeout
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
    :raises ValueError: if section_name does not exist
    :raises IOError: if reading the file failed
    """
    if defaults is None:
        defaults = {}
    if raw:
        c = RawConfigParser(defaults)
    else:
        c = ConfigParser(defaults)
    if hasattr(conf_path, 'readline'):
        if hasattr(conf_path, 'seek'):
            conf_path.seek(0)
        c.readfp(conf_path)
    else:
        if os.path.isdir(conf_path):
            # read all configs in directory
            success = read_conf_dir(c, conf_path)
        else:
            success = c.read(conf_path)
        if not success:
            raise IOError(_("Unable to read config from %s") %
                          conf_path)
    if section_name:
        if c.has_section(section_name):
            conf = dict(c.items(section_name))
        else:
            raise ValueError(
                _("Unable to find %(section)s config section in %(conf)s") %
                {'section': section_name, 'conf': conf_path})
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


def audit_location_generator(devices, datadir, suffix='',
                             mount_check=True, logger=None):
    """
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
    """
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
                logger.warning(_('Skipping %(datadir)s because %(err)s'),
                               {'datadir': datadir_path, 'err': e})
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
    """
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
    """GreenPool subclassed to kill its coros when it gets gc'ed"""

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
        self._pending = 0

    def _run_func(self, func, args, kwargs):
        try:
            self._responses.put(func(*args, **kwargs))
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

    def next(self):
        try:
            rv = self._responses.get_nowait()
        except eventlet.queue.Empty:
            if self._inflight == 0:
                raise StopIteration()
            rv = self._responses.get()
        self._pending -= 1
        return rv
    __next__ = next


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
            yield next(self)
            self.spawn(func, *args)

        # Drain the pile
        for result in self:
            yield result

    def __enter__(self):
        self.pool.__enter__()
        return self

    def __exit__(self, type, value, traceback):
        self.pool.__exit__(type, value, traceback)


class ModifiedParseResult(ParseResult):
    """Parse results class for urlparse."""

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
                _('Invalid X-Container-Sync-To format %r') % orig_value,
                None, None, None)
        realm, cluster, account, container = data
        realm_key = realms_conf.key(realm)
        if not realm_key:
            return (_('No realm key for %r') % realm, None, None, None)
        endpoint = realms_conf.endpoint(realm, cluster)
        if not endpoint:
            return (
                _('No cluster endpoint for %(realm)r %(cluster)r')
                % {'realm': realm, 'cluster': cluster},
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
    :raises ValueError: if argument invalid
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

    :param write_affinity_str: affinity config value, e.g. "r1z2"
        or "r1, r2z1, r2z2"
    :returns: single-argument function, or None if affinity_str is empty
    :raises ValueError: if argument invalid
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
    Update a recon cache entry item.

    If ``item`` is an empty dict then any existing ``key`` in ``cache_entry``
    will be deleted. Similarly if ``item`` is a dict and any of its values are
    empty dicts then the corrsponsing key will be deleted from the nested dict
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


def get_valid_utf8_str(str_or_unicode):
    """
    Get valid parts of utf-8 str from str, unicode and even invalid utf-8 str

    :param str_or_unicode: a string or an unicode which can be invalid utf-8
    """
    if six.PY2:
        if isinstance(str_or_unicode, six.text_type):
            (str_or_unicode, _len) = utf8_encoder(str_or_unicode, 'replace')
        (valid_unicode_str, _len) = utf8_decoder(str_or_unicode, 'replace')
    else:
        # Apparently under py3 we need to go to utf-16 to collapse surrogates?
        if isinstance(str_or_unicode, six.binary_type):
            try:
                (str_or_unicode, _len) = utf8_decoder(str_or_unicode,
                                                      'surrogatepass')
            except UnicodeDecodeError:
                (str_or_unicode, _len) = utf8_decoder(str_or_unicode,
                                                      'replace')
        (str_or_unicode, _len) = utf16_encoder(str_or_unicode, 'surrogatepass')
        (valid_unicode_str, _len) = utf16_decoder(str_or_unicode, 'replace')
    return valid_unicode_str.encode('utf-8')


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
    :raises ValueError: if malformed
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
    :returns: a tuple containing (content type, list of k, v parameter tuples)
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
                logger.exception(_("Invalid swift_bytes"))


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
    quoted = _quote(get_valid_utf8_str(value), safe)
    if isinstance(value, six.binary_type):
        quoted = quoted.encode('utf-8')
    return quoted


def get_expirer_container(x_delete_at, expirer_divisor, acc, cont, obj):
    """
    Returns an expiring object container name for given X-Delete-At and
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
            'invalid starting boundary: wanted %r, got %r', (boundary, got))
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
        if six.PY3:
            try:
                line = line.decode('utf-8')
            except UnicodeDecodeError:
                line = line.decode('latin1')
        headers.append(line)
        if done:
            break
    if six.PY3:
        header_string = ''.join(headers)
    else:
        header_string = b''.join(headers)
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
        def string_along(useful_iter, useless_iter_iter, logger):
            with closing_if_possible(useful_iter):
                for x in useful_iter:
                    yield x

            try:
                next(useless_iter_iter)
            except StopIteration:
                pass
            else:
                logger.warning(
                    _("More than one part in a single-part response?"))

        return string_along(response_body_iter, ranges_iter, logger)


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


class ShardRange(object):
    """
    A ShardRange encapsulates sharding state related to a container including
    lower and upper bounds that define the object namespace for which the
    container is responsible.

    Shard ranges may be persisted in a container database. Timestamps
    associated with subsets of the shard range attributes are used to resolve
    conflicts when a shard range needs to be merged with an existing shard
    range record and the most recent version of an attribute should be
    persisted.

    :param name: the name of the shard range; this should take the form of a
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
    """
    FOUND = 10
    CREATED = 20
    CLEAVED = 30
    ACTIVE = 40
    SHRINKING = 50
    SHARDING = 60
    SHARDED = 70
    STATES = {FOUND: 'found',
              CREATED: 'created',
              CLEAVED: 'cleaved',
              ACTIVE: 'active',
              SHRINKING: 'shrinking',
              SHARDING: 'sharding',
              SHARDED: 'sharded'}
    STATES_BY_NAME = dict((v, k) for k, v in STATES.items())

    class OuterBound(object):
        def __eq__(self, other):
            return isinstance(other, type(self))

        def __ne__(self, other):
            return not self.__eq__(other)

        def __str__(self):
            return ''

        def __repr__(self):
            return type(self).__name__

        def __bool__(self):
            return False

        __nonzero__ = __bool__

    @functools.total_ordering
    class MaxBound(OuterBound):
        def __ge__(self, other):
            return True

    @functools.total_ordering
    class MinBound(OuterBound):
        def __le__(self, other):
            return True

    MIN = MinBound()
    MAX = MaxBound()

    def __init__(self, name, timestamp, lower=MIN, upper=MAX,
                 object_count=0, bytes_used=0, meta_timestamp=None,
                 deleted=False, state=None, state_timestamp=None, epoch=None):
        self.account = self.container = self._timestamp = \
            self._meta_timestamp = self._state_timestamp = self._epoch = None
        self._lower = ShardRange.MIN
        self._upper = ShardRange.MAX
        self._deleted = False
        self._state = None

        self.name = name
        self.timestamp = timestamp
        self.lower = lower
        self.upper = upper
        self.deleted = deleted
        self.object_count = object_count
        self.bytes_used = bytes_used
        self.meta_timestamp = meta_timestamp
        self.state = self.FOUND if state is None else state
        self.state_timestamp = state_timestamp
        self.epoch = epoch

    @classmethod
    def _encode(cls, value):
        if six.PY2 and isinstance(value, six.text_type):
            return value.encode('utf-8')
        if six.PY3 and isinstance(value, six.binary_type):
            # This should never fail -- the value should always be coming from
            # valid swift paths, which means UTF-8
            return value.decode('utf-8')
        return value

    def _encode_bound(self, bound):
        if isinstance(bound, ShardRange.OuterBound):
            return bound
        if not (isinstance(bound, six.text_type) or
                isinstance(bound, six.binary_type)):
            raise TypeError('must be a string type')
        return self._encode(bound)

    @classmethod
    def _make_container_name(cls, root_container, parent_container, timestamp,
                             index):
        if not isinstance(parent_container, bytes):
            parent_container = parent_container.encode('utf-8')
        return "%s-%s-%s-%s" % (root_container,
                                hashlib.md5(parent_container).hexdigest(),
                                cls._to_timestamp(timestamp).internal,
                                index)

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
        shard_container = cls._make_container_name(
            root_container, parent_container, timestamp, index)
        return '%s/%s' % (shards_account, shard_container)

    @classmethod
    def _to_timestamp(cls, timestamp):
        if timestamp is None or isinstance(timestamp, Timestamp):
            return timestamp
        return Timestamp(timestamp)

    @property
    def name(self):
        return '%s/%s' % (self.account, self.container)

    @name.setter
    def name(self, path):
        path = self._encode(path)
        if not path or len(path.split('/')) != 2 or not all(path.split('/')):
            raise ValueError(
                "Name must be of the form '<account>/<container>', got %r" %
                path)
        self.account, self.container = path.split('/')

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
    def lower(self):
        return self._lower

    @property
    def lower_str(self):
        return str(self.lower)

    @lower.setter
    def lower(self, value):
        if value in (None, b'', u''):
            value = ShardRange.MIN
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
    def end_marker(self):
        return self.upper_str + '\x00' if self.upper else ''

    @property
    def upper(self):
        return self._upper

    @property
    def upper_str(self):
        return str(self.upper)

    @upper.setter
    def upper(self, value):
        if value in (None, b'', u''):
            value = ShardRange.MAX
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
        self.object_count = int(object_count)
        self.bytes_used = int(bytes_used)
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
            state = state.lower()
            state_num = cls.STATES_BY_NAME[state]
        except (KeyError, AttributeError):
            try:
                state_name = cls.STATES[state]
            except KeyError:
                raise ValueError('Invalid state %r' % state)
            else:
                state_num = state
        else:
            state_name = state
        return state_num, state_name

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        try:
            float_state = float(state)
            int_state = int(float_state)
        except (ValueError, TypeError):
            raise ValueError('Invalid state %r' % state)
        if int_state != float_state or int_state not in self.STATES:
            raise ValueError('Invalid state %r' % state)
        self._state = int_state

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

    def __contains__(self, item):
        # test if the given item is within the namespace
        if item == '':
            return False
        item = self._encode_bound(item)
        return self.lower < item <= self.upper

    def __lt__(self, other):
        # a ShardRange is less than other if its entire namespace is less than
        # other; if other is another ShardRange that implies that this
        # ShardRange's upper must be less than or equal to the other
        # ShardRange's lower
        if self.upper == ShardRange.MAX:
            return False
        if isinstance(other, ShardRange):
            return self.upper <= other.lower
        elif other is None:
            return True
        else:
            return self.upper < self._encode(other)

    def __gt__(self, other):
        # a ShardRange is greater than other if its entire namespace is greater
        # than other; if other is another ShardRange that implies that this
        # ShardRange's lower must be less greater than or equal to the other
        # ShardRange's upper
        if self.lower == ShardRange.MIN:
            return False
        if isinstance(other, ShardRange):
            return self.lower >= other.upper
        elif other is None:
            return False
        else:
            return self.lower >= self._encode(other)

    def __eq__(self, other):
        # test for equality of range bounds only
        if not isinstance(other, ShardRange):
            return False
        return self.lower == other.lower and self.upper == other.upper

    # A by-the-book implementation should probably hash the value, which
    # in our case would be account+container+lower+upper (+timestamp ?).
    # But we seem to be okay with just the identity.
    def __hash__(self):
        return id(self)

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return '%s<%r to %r as of %s, (%d, %d) as of %s, %s as of %s>' % (
            self.__class__.__name__, self.lower, self.upper,
            self.timestamp.internal, self.object_count, self.bytes_used,
            self.meta_timestamp.internal, self.state_text,
            self.state_timestamp.internal)

    def entire_namespace(self):
        """
        Returns True if the ShardRange includes the entire namespace, False
        otherwise.
        """
        return (self.lower == ShardRange.MIN and
                self.upper == ShardRange.MAX)

    def overlaps(self, other):
        """
        Returns True if the ShardRange namespace overlaps with the other
        ShardRange's namespace.

        :param other: an instance of :class:`~swift.common.utils.ShardRange`
        """
        if not isinstance(other, ShardRange):
            return False
        return max(self.lower, other.lower) < min(self.upper, other.upper)

    def includes(self, other):
        """
        Returns True if this namespace includes the whole of the other
        namespace, False otherwise.

        :param other: an instance of :class:`~swift.common.utils.ShardRange`
        """
        return (self.lower <= other.lower) and (other.upper <= self.upper)

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
            params['state_timestamp'], params['epoch'])


def find_shard_range(item, ranges):
    """
    Find a ShardRange in given list of ``shard_ranges`` whose namespace
    contains ``item``.

    :param item: The item for a which a ShardRange is to be found.
    :param ranges: a sorted list of ShardRanges.
    :return: the ShardRange whose namespace contains ``item``, or None if
        no suitable range is found.
    """
    index = bisect.bisect_left(ranges, item)
    if index != len(ranges) and item in ranges[index]:
        return ranges[index]
    return None


def modify_priority(conf, logger):
    """
    Modify priority by nice and ionice.
    """

    global _libc_setpriority
    if _libc_setpriority is None:
        _libc_setpriority = load_libc_function('setpriority',
                                               errcheck=True)

    def _setpriority(nice_priority):
        """
        setpriority for this pid

        :param nice_priority: valid values are -19 to 20
        """
        try:
            _libc_setpriority(PRIO_PROCESS, os.getpid(),
                              int(nice_priority))
        except (ValueError, OSError):
            print(_("WARNING: Unable to modify scheduling priority of process."
                    " Keeping unchanged! Check logs for more info. "))
            logger.exception('Unable to modify nice priority')
        else:
            logger.debug('set nice priority to %s' % nice_priority)

    nice_priority = conf.get('nice_priority')
    if nice_priority is not None:
        _setpriority(nice_priority)

    global _posix_syscall
    if _posix_syscall is None:
        _posix_syscall = load_libc_function('syscall', errcheck=True)

    def _ioprio_set(io_class, io_priority):
        """
        ioprio_set for this process

        :param io_class: the I/O class component, can be
                         IOPRIO_CLASS_RT, IOPRIO_CLASS_BE,
                         or IOPRIO_CLASS_IDLE
        :param io_priority: priority value in the I/O class
        """
        try:
            io_class = IO_CLASS_ENUM[io_class]
            io_priority = int(io_priority)
            _posix_syscall(NR_ioprio_set(),
                           IOPRIO_WHO_PROCESS,
                           os.getpid(),
                           IOPRIO_PRIO_VALUE(io_class, io_priority))
        except (KeyError, ValueError, OSError):
            print(_("WARNING: Unable to modify I/O scheduling class "
                    "and priority of process. Keeping unchanged! "
                    "Check logs for more info."))
            logger.exception("Unable to modify ionice priority")
        else:
            logger.debug('set ionice class %s priority %s',
                         io_class, io_priority)

    io_class = conf.get("ionice_class")
    if io_class is None:
        return
    io_priority = conf.get("ionice_priority", 0)
    _ioprio_set(io_class, io_priority)


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
                            "O_TMPFILE: '%(ex)s'",
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


def strict_b64decode(value, allow_line_breaks=False):
    '''
    Validate and decode Base64-encoded data.

    The stdlib base64 module silently discards bad characters, but we often
    want to treat them as an error.

    :param value: some base64-encoded data
    :param allow_line_breaks: if True, ignore carriage returns and newlines
    :returns: the decoded data
    :raises ValueError: if ``value`` is not a string, contains invalid
                        characters, or has insufficient padding
    '''
    if isinstance(value, bytes):
        try:
            value = value.decode('ascii')
        except UnicodeDecodeError:
            raise ValueError
    if not isinstance(value, six.text_type):
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
    try:
        return base64.b64decode(value)
    except (TypeError, binascii.Error):  # (py2 error, py3 error)
        raise ValueError


MD5_BLOCK_READ_BYTES = 4096


def md5_hash_for_file(fname):
    """
    Get the MD5 checksum of a file.

    :param fname: path to file
    :returns: MD5 checksum, hex encoded
    """
    with open(fname, 'rb') as f:
        md5sum = md5()
        for block in iter(lambda: f.read(MD5_BLOCK_READ_BYTES), b''):
            md5sum.update(block)
    return md5sum.hexdigest()


def replace_partition_in_path(path, part_power):
    """
    Takes a full path to a file and a partition power and returns
    the same path, but with the correct partition number. Most useful when
    increasing the partition power.

    :param path: full path to a file, for example object .data file
    :param part_power: partition power to compute correct partition number
    :returns: Path with re-computed partition power
    """

    path_components = path.split(os.sep)
    digest = binascii.unhexlify(path_components[-2])

    part_shift = 32 - int(part_power)
    part = struct.unpack_from('>I', digest)[0] >> part_shift

    path_components[-4] = "%d" % part

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
    return pkg_resources.load_entry_point(dist, group, name)


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
                trampoline(self.rfd, read=True)

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
        # call get_logger() and don't explicitly dispose of it by calling
        # logger.logger.handlers[0].lock.close() [1], the pipe file
        # descriptors are leaked.
        #
        # This only really comes up in tests. Swift processes tend to call
        # get_logger() once and then hang on to it until they exit, but the
        # test suite calls get_logger() a lot.
        #
        # [1] and that's a completely ridiculous thing to expect callers to
        # do, so nobody does it and that's okay.
        self.close()


class ThreadSafeSysLogHandler(SysLogHandler):
    def createLock(self):
        self.lock = PipeMutex()


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
    that actually exist in that path's dir. A valid db filename has the form:

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
