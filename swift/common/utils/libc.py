# Copyright (c) 2010-2023 OpenStack Foundation
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

"""Functions Swift uses to interact with libc and other low-level APIs."""

import ctypes
import ctypes.util
import errno
import fcntl
import logging
import os
import platform
import socket


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


# These constants are Linux-specific, and Python doesn't seem to know
# about them. We ask anyway just in case that ever gets fixed.
#
# The values were copied from the Linux 3.x kernel headers.
AF_ALG = getattr(socket, 'AF_ALG', 38)
F_SETPIPE_SZ = getattr(fcntl, 'F_SETPIPE_SZ', 1031)


def noop_libc_function(*args):
    return 0


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
            logging.warning("Unable to locate %s in libc.  Leaving as a "
                            "no-op.", func_name)
        return noop_libc_function
    if errcheck:
        def _errcheck(result, f, args):
            if result == -1:
                errcode = ctypes.get_errno()
                raise OSError(errcode, os.strerror(errcode))
            return result
        func.errcheck = _errcheck
    return func


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
                self._func_handle = func_handle
            except AttributeError:
                # We pass fail_if_missing=True to load_libc_function and
                # then ignore the error. It's weird, but otherwise we have
                # to check if self._func_handle is noop_libc_function, and
                # that's even weirder.
                pass
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
            print("WARNING: Unable to modify scheduling priority of process."
                  " Keeping unchanged! Check logs for more info. ")
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
            print("WARNING: Unable to modify I/O scheduling class "
                  "and priority of process. Keeping unchanged! "
                  "Check logs for more info.")
            logger.exception("Unable to modify ionice priority")
        else:
            logger.debug('set ionice class %s priority %s',
                         io_class, io_priority)

    io_class = conf.get("ionice_class")
    if io_class is None:
        return
    io_priority = conf.get("ionice_priority", 0)
    _ioprio_set(io_class, io_priority)
