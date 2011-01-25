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
import struct
from ConfigParser import ConfigParser, NoSectionError, NoOptionError
from optparse import OptionParser
from tempfile import mkstemp
import cPickle as pickle

import eventlet
from eventlet import greenio, GreenPool, sleep, Timeout, listen
from eventlet.green import socket, subprocess, ssl, thread, threading
import netifaces

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
hash_conf = ConfigParser()
HASH_PATH_SUFFIX = ''
if hash_conf.read('/etc/swift/swift.conf'):
    try:
        HASH_PATH_SUFFIX = hash_conf.get('swift-hash',
                                         'swift_hash_path_suffix')
    except (NoSectionError, NoOptionError):
        pass

# Used when reading config values
TRUE_VALUES = set(('true', '1', 'yes', 'True', 'Yes', 'on', 'On'))


def validate_configuration():
    if HASH_PATH_SUFFIX == '':
        sys.exit("Error: [swift-hash]: swift_hash_path_suffix missing "
                 "from /etc/swift/swift.conf")


def load_libc_function(func_name):
    """
    Attempt to find the function in libc, otherwise return a no-op func.

    :param func_name: name of the function to pull from libc.
    """
    try:
        libc = ctypes.CDLL(ctypes.util.find_library('c'))
        return getattr(libc, func_name)
    except AttributeError:
        logging.warn(_("Unable to locate %s in libc.  Leaving as a no-op."),
                     func_name)

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


class LogAdapter(object):
    """
    A Logger like object which performs some reformatting on calls to
    :meth:`exception`.  Can be used to store a threadlocal transaction id.
    """

    _txn_id = threading.local()

    def __init__(self, logger):
        self.logger = logger
        for proxied_method in ('debug', 'log', 'warn', 'warning', 'error',
                               'critical', 'info'):
            setattr(self, proxied_method, getattr(logger, proxied_method))

    @property
    def txn_id(self):
        if hasattr(self._txn_id, 'value'):
            return self._txn_id.value

    @txn_id.setter
    def txn_id(self, value):
        self._txn_id.value = value

    def getEffectiveLevel(self):
        return self.logger.getEffectiveLevel()

    def exception(self, msg, *args):
        _junk, exc, _junk = sys.exc_info()
        call = self.logger.error
        emsg = ''
        if isinstance(exc, OSError):
            if exc.errno in (errno.EIO, errno.ENOSPC):
                emsg = str(exc)
            else:
                call = self.logger.exception
        elif isinstance(exc, socket.error):
            if exc.errno == errno.ECONNREFUSED:
                emsg = _('Connection refused')
            elif exc.errno == errno.EHOSTUNREACH:
                emsg = _('Host unreachable')
            elif exc.errno == errno.ETIMEDOUT:
                emsg = _('Connection timeout')
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
        call('%s: %s' % (msg, emsg), *args)


class NamedFormatter(logging.Formatter):
    """
    NamedFormatter is used to add additional information to log messages.
    Normally it will simply add the server name as an attribute on the
    LogRecord and the default format string will include it at the
    begining of the log message.  Additionally, if the transaction id is
    available and not already included in the message, NamedFormatter will
    add it.

    NamedFormatter may be initialized with a format string which makes use
    of the standard LogRecord attributes.  In addition the format string
    may include the following mapping key:

    +----------------+---------------------------------------------+
    | Format         | Description                                 |
    +================+=============================================+
    | %(server)s     | Name of the swift server doing logging      |
    +----------------+---------------------------------------------+

    :param server: the swift server name, a string.
    :param logger: a Logger or :class:`LogAdapter` instance, additional
                   context may be pulled from attributes on this logger if
                   available.
    :param fmt: the format string used to construct the message, if none is
                supplied it defaults to ``"%(server)s %(message)s"``
    """

    def __init__(self, server, logger,
                 fmt="%(server)s %(message)s"):
        logging.Formatter.__init__(self, fmt)
        self.server = server
        self.logger = logger

    def format(self, record):
        record.server = self.server
        msg = logging.Formatter.format(self, record)
        if self.logger.txn_id and (record.levelno != logging.INFO or
                                   self.logger.txn_id not in msg):
            msg = "%s (txn: %s)" % (msg, self.logger.txn_id)
        return msg


def get_logger(conf, name=None, log_to_console=False, log_route=None):
    """
    Get the current system logger using config settings.

    **Log config and defaults**::

        log_facility = LOG_LOCAL0
        log_level = INFO
        log_name = swift

    :param conf: Configuration dict to read settings from
    :param name: Name of the logger
    :param log_to_console: Add handler which writes to console on stderr
    """
    if not conf:
        conf = {}
    if not hasattr(get_logger, 'root_logger_configured'):
        get_logger.root_logger_configured = True
        get_logger(conf, name, log_to_console, log_route='root')
    if name is None:
        name = conf.get('log_name', 'swift')
    if not log_route:
        log_route = name
    if log_route == 'root':
        logger = logging.getLogger()
    else:
        logger = logging.getLogger(log_route)
    if not hasattr(get_logger, 'handlers'):
        get_logger.handlers = {}
    facility = getattr(SysLogHandler, conf.get('log_facility', 'LOG_LOCAL0'),
                       SysLogHandler.LOG_LOCAL0)
    if facility in get_logger.handlers:
        logger.removeHandler(get_logger.handlers[facility])
        get_logger.handlers[facility].close()
        del get_logger.handlers[facility]
    if log_to_console:
        # check if a previous call to get_logger already added a console logger
        if hasattr(get_logger, 'console') and get_logger.console:
            logger.removeHandler(get_logger.console)
        get_logger.console = logging.StreamHandler(sys.__stderr__)
        logger.addHandler(get_logger.console)
    get_logger.handlers[facility] = \
        SysLogHandler(address='/dev/log', facility=facility)
    logger.addHandler(get_logger.handlers[facility])
    logger.setLevel(
        getattr(logging, conf.get('log_level', 'INFO').upper(), logging.INFO))
    adapted_logger = LogAdapter(logger)
    formatter = NamedFormatter(name, adapted_logger)
    get_logger.handlers[facility].setFormatter(formatter)
    if hasattr(get_logger, 'console'):
        get_logger.console.setFormatter(formatter)
    return adapted_logger


def drop_privileges(user):
    """
    Sets the userid/groupid of the current process, get session leader, etc.

    :param user: User name to change privileges to
    """
    user = pwd.getpwnam(user)
    os.setgid(user[3])
    os.setuid(user[2])
    try:
        os.setsid()
    except OSError:
        pass
    os.chdir('/')  # in case you need to rmdir on where you started the daemon
    os.umask(0)  # ensure files are created with the correct privileges


def capture_stdio(logger, **kwargs):
    """
    Log unhandled exceptions, close stdio, capture stdout and stderr.

    param logger: Logger object to use
    """
    # log uncaught exceptions
    sys.excepthook = lambda * exc_info: \
        logger.critical(_('UNCAUGHT EXCEPTION'), exc_info=exc_info)

    # collect stdio file desc not in use for logging
    stdio_fds = [0, 1, 2]
    if hasattr(get_logger, 'console'):
        stdio_fds.remove(get_logger.console.stream.fileno())

    with open(os.devnull, 'r+b') as nullfile:
        # close stdio (excludes fds open for logging)
        for desc in stdio_fds:
            try:
                os.dup2(nullfile.fileno(), desc)
            except OSError:
                pass

    # redirect stdio
    if kwargs.pop('capture_stdout', True):
        sys.stdout = LoggerFileObject(logger)
    if kwargs.pop('capture_stderr', True):
        sys.stderr = LoggerFileObject(logger)


def parse_options(usage="%prog CONFIG [options]", once=False, test_args=None):
    """
    Parse standard swift server/daemon options with optparse.OptionParser.

    :param usage: String describing usage
    :param once: Boolean indicating the "once" option is available
    :param test_args: Override sys.argv; used in testing

    :returns : Tuple of (config, options); config is an absolute path to the
               config file, options is the parser options as a dictionary.

    :raises SystemExit: First arg (CONFIG) is required, file must exist
    """
    parser = OptionParser(usage)
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
    options['extra_args'] = extra_args
    return config, options


def whataremyips():
    """
    Get the machine's ip addresses

    :returns: list of Strings of ip addresses
    """
    addresses = []
    for interface in netifaces.interfaces():
        iface_data = netifaces.ifaddresses(interface)
        for family in iface_data:
            if family not in (netifaces.AF_INET, netifaces.AF_INET6):
                continue
            for address in iface_data[family]:
                addresses.append(address['addr'])
    return addresses


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


def readconf(conf, section_name=None, log_name=None, defaults=None):
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
        print _("Unable to read config file %s") % conf
        sys.exit(1)
    if section_name:
        if c.has_section(section_name):
            conf = dict(c.items(section_name))
        else:
            print _("Unable to find %s config section in %s") % \
                 (section_name, conf)
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
    device_dir = os.listdir(devices)
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
        partitions = os.listdir(datadir_path)
        for partition in partitions:
            part_path = os.path.join(datadir_path, partition)
            if not os.path.isdir(part_path):
                continue
            suffixes = os.listdir(part_path)
            for suffix in suffixes:
                suff_path = os.path.join(part_path, suffix)
                if not os.path.isdir(suff_path):
                    continue
                hashes = os.listdir(suff_path)
                for hsh in hashes:
                    hash_path = os.path.join(suff_path, hsh)
                    if not os.path.isdir(hash_path):
                        continue
                    for fname in sorted(os.listdir(hash_path),
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
