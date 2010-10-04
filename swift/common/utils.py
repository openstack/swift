# Copyright (c) 2010 OpenStack, LLC.
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
import signal
import sys
import time
import mimetools
from hashlib import md5
from random import shuffle
from urllib import quote
from contextlib import contextmanager
import ctypes
import ctypes.util
import fcntl
import struct
from ConfigParser import ConfigParser
from tempfile import mkstemp
import cPickle as pickle


import eventlet
from eventlet import greenio, GreenPool, sleep, Timeout, listen
from eventlet.green import socket, subprocess, ssl, thread, threading

from swift.common.exceptions import LockTimeout, MessageTimeout

# logging doesn't import patched as cleanly as one would like
from logging.handlers import SysLogHandler
import logging
logging.thread = eventlet.green.thread
logging.threading = eventlet.green.threading
logging._lock = logging.threading.RLock()

# These are lazily pulled from libc elsewhere
_sys_fallocate = None
_posix_fadvise = None

# Used by hash_path to offer a bit more security when generating hashes for
# paths. It simply appends this value to all paths; guessing the hash a path
# will end up with would also require knowing this suffix.
HASH_PATH_SUFFIX = os.environ.get('SWIFT_HASH_PATH_SUFFIX', 'endcap')

# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'True', 'Yes', 'on', 'On'))


def load_libc_function(func_name):
    """
    Attempt to find the function in libc, otherwise return a no-op func.

    :param func_name: name of the function to pull from libc.
    """
    try:
        libc = ctypes.CDLL(ctypes.util.find_library('c'))
        return getattr(libc, func_name)
    except AttributeError:
        logging.warn("Unable to locate %s in libc.  Leaving as a no-op."
                     % func_name)

        def noop_libc_function(*args):
            return 0
        return noop_libc_function


def get_param(req, name, default=None):
    """
    Get parameters from an HTTP request ensuring proper handling UTF-8
    encoding.

    :param req: Webob request object
    :param name: parameter name
    :param default: result to return if the parameter is not found
    :returns: HTTP request parameter value
    """
    value = req.str_params.get(name, default)
    if value:
        value.decode('utf8')    # Ensure UTF8ness
    return value


def fallocate(fd, size):
    """
    Pre-allocate disk space for a file file.

    :param fd: file descriptor
    :param size: size to allocate (in bytes)
    """
    global _sys_fallocate
    if _sys_fallocate is None:
        _sys_fallocate = load_libc_function('fallocate')
    if size > 0:
        # 1 means "FALLOC_FL_KEEP_SIZE", which means it pre-allocates invisibly
        ret = _sys_fallocate(fd, 1, 0, ctypes.c_uint64(size))
        # XXX: in (not very thorough) testing, errno always seems to be 0?
        err = ctypes.get_errno()
        if ret and err not in (0, errno.ENOSYS):
            raise OSError(err, 'Unable to fallocate(%s)' % size)


def drop_buffer_cache(fd, offset, length):
    """
    Drop 'buffer' cache for the given range of the given file.

    :param fd: file descriptor
    :param offset: start offset
    :param length: length
    """
    global _posix_fadvise
    if _posix_fadvise is None:
        _posix_fadvise = load_libc_function('posix_fadvise')
    # 4 means "POSIX_FADV_DONTNEED"
    ret = _posix_fadvise(fd, ctypes.c_uint64(offset),
                        ctypes.c_uint64(length), 4)
    if ret != 0:
        logging.warn("posix_fadvise(%s, %s, %s, 4) -> %s"
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


def renamer(old, new):  # pragma: no cover
    """
    Attempt to fix^H^H^Hhide race conditions like empty object directories
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
        if segs[0] or count < minsegs or count > maxsegs or \
           '' in segs[1:minsegs]:
            raise ValueError('Invalid path: %s' % quote(path))
    else:
        minsegs += 1
        maxsegs += 1
        segs = path.split('/', maxsegs)
        count = len(segs)
        if segs[0] or count < minsegs or count > maxsegs + 1 or \
           '' in segs[1:minsegs] or (count == maxsegs + 1 and segs[maxsegs]):
            raise ValueError('Invalid path: %s' % quote(path))
    segs = segs[1:maxsegs]
    segs.extend([None] * (maxsegs - 1 - len(segs)))
    return segs


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
                self.logger.error('STDOUT: Connection reset by peer')
            else:
                self.logger.error('STDOUT: %s' % value)

    def writelines(self, values):
        self.logger.error('STDOUT: %s' % '#012'.join(values))

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


def drop_privileges(user):
    """
    Sets the userid of the current process

    :param user: User id to change privileges to
    """
    user = pwd.getpwnam(user)
    os.setgid(user[3])
    os.setuid(user[2])


class NamedLogger(object):
    """Cheesy version of the LoggerAdapter available in Python 3"""

    def __init__(self, logger, server):
        self.logger = logger
        self.server = server
        for proxied_method in ('debug', 'info', 'log', 'warn', 'warning',
                               'error', 'critical'):
            setattr(self, proxied_method,
                    self._proxy(getattr(logger, proxied_method)))

    def _proxy(self, logger_meth):

        def _inner_proxy(msg, *args, **kwargs):
            msg = '%s %s' % (self.server, msg)
            logger_meth(msg, *args, **kwargs)
        return _inner_proxy

    def getEffectiveLevel(self):
        return self.logger.getEffectiveLevel()

    def exception(self, msg, *args):
        _, exc, _ = sys.exc_info()
        call = self.logger.error
        emsg = ''
        if isinstance(exc, OSError):
            if exc.errno in (errno.EIO, errno.ENOSPC):
                emsg = str(exc)
            else:
                call = self.logger.exception
        elif isinstance(exc, socket.error):
            if exc.errno == errno.ECONNREFUSED:
                emsg = 'Connection refused'
            elif exc.errno == errno.EHOSTUNREACH:
                emsg = 'Host unreachable'
            else:
                call = self.logger.exception
        elif isinstance(exc, eventlet.Timeout):
            emsg = exc.__class__.__name__
            if hasattr(exc, 'seconds'):
                emsg += ' (%ss)' % exc.seconds
            if isinstance(exc, MessageTimeout):
                if exc.msg:
                    emsg += ' %s' % exc.msg
        else:
            call = self.logger.exception
        call('%s %s: %s' % (self.server, msg, emsg), *args)


def get_logger(conf, name=None):
    """
    Get the current system logger using config settings.

    **Log config and defaults**::

        log_facility = LOG_LOCAL0
        log_level = INFO
        log_name = swift

    :param conf: Configuration dict to read settings from
    :param name: Name of the logger
    """
    root_logger = logging.getLogger()
    if hasattr(get_logger, 'handler') and get_logger.handler:
        root_logger.removeHandler(get_logger.handler)
        get_logger.handler = None
    if conf is None:
        root_logger.setLevel(logging.INFO)
        return NamedLogger(root_logger, name)
    if name is None:
        name = conf.get('log_name', 'swift')
    get_logger.handler = SysLogHandler(address='/dev/log',
        facility=getattr(SysLogHandler,
                         conf.get('log_facility', 'LOG_LOCAL0'),
                         SysLogHandler.LOG_LOCAL0))
    root_logger.addHandler(get_logger.handler)
    root_logger.setLevel(
        getattr(logging, conf.get('log_level', 'INFO').upper(), logging.INFO))
    return NamedLogger(root_logger, name)


def whataremyips():
    """
    Get the machine's ip addresses using ifconfig

    :returns: list of Strings of IPv4 ip addresses
    """
    proc = subprocess.Popen(['/sbin/ifconfig'], stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
    ret_val = proc.wait()
    results = proc.stdout.read().split('\n')
    return [x.split(':')[1].split()[0] for x in results if 'inet addr' in x]


def storage_directory(datadir, partition, hash):
    """
    Get the storage directory

    :param datadir: Base data directory
    :param partition: Partition
    :param hash: Account, container or object hash
    :returns: Storage directory
    """
    return os.path.join(datadir, partition, hash[-3:], hash)


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

    :param directory: directory to be locked
    :param timeout: timeout (in seconds)
    """
    mkdirs(directory)
    fd = os.open(directory, os.O_RDONLY)
    try:
        with LockTimeout(timeout, directory):
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
    devices = os.listdir(devices_dir)
    shuffle(devices)
    devices_partitions = []
    for device in devices:
        partitions = os.listdir(os.path.join(devices_dir, device, item_type))
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
        for fname in os.listdir(path):
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


def readconf(conf, section_name, log_name=None, defaults=None):
    """
    Read config file and return config items as a dict

    :param conf: path to config file
    :param section_name: config section to read (will return all sections if
                     not defined)
    :param log_name: name to be used with logging (will use section_name if
                     not defined)
    :param defaults: dict of default values to pre-populate the config with
    :returns: dict of config items
    """
    if defaults is None:
        defaults = {}
    c = ConfigParser(defaults)
    if not c.read(conf):
        print "Unable to read config file %s" % conf
        sys.exit(1)
    if section_name:
        if c.has_section(section_name):
            conf = dict(c.items(section_name))
        else:
            print "Unable to find %s config section in %s" % (section_name,
                                                              conf)
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
    return conf


def write_pickle(obj, dest, tmp):
    """
    Ensure that a pickle file gets written to disk.  The file
    is first written to a tmp location, ensure it is synced to disk, then
    perform a move to its final location

    :param obj: python object to be pickled
    :param dest: path of final destination file
    :param tmp: path to tmp to use
    """
    fd, tmppath = mkstemp(dir=tmp)
    with os.fdopen(fd, 'wb') as fo:
        pickle.dump(obj, fo)
        fo.flush()
        os.fsync(fd)
        renamer(tmppath, dest)
