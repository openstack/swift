# Copyright (c) 2014 OpenStack Foundation
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

'''
Bindings to the `tee` and `splice` system calls
'''

import os
import ctypes
import ctypes.util

__all__ = ['tee', 'splice']


c_loff_t = ctypes.c_long


class Tee(object):
    '''Binding to `tee`'''

    __slots__ = '_c_tee',

    def __init__(self):
        libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)

        try:
            c_tee = libc.tee
        except AttributeError:
            self._c_tee = None
            return

        c_tee.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_size_t,
            ctypes.c_uint
        ]

        c_tee.restype = ctypes.c_ssize_t

        def errcheck(result, func, arguments):
            if result == -1:
                errno = ctypes.set_errno(0)

                raise IOError(errno, 'tee: %s' % os.strerror(errno))
            else:
                return result

        c_tee.errcheck = errcheck

        self._c_tee = c_tee

    def __call__(self, fd_in, fd_out, len_, flags):
        '''See `man 2 tee`

        File-descriptors can be file-like objects with a `fileno` method, or
        integers.

        Flags can be an integer value, or a list of flags (exposed on
        `splice`).

        This function returns the number of bytes transferred (i.e. the actual
        result of the call to `tee`).

        Upon other errors, an `IOError` is raised with the proper `errno` set.
        '''

        if not self.available:
            raise EnvironmentError('tee not available')

        if not isinstance(flags, int):
            c_flags = 0
            for flag in flags:
                c_flags |= flag
        else:
            c_flags = flags

        c_fd_in = getattr(fd_in, 'fileno', lambda: fd_in)()
        c_fd_out = getattr(fd_out, 'fileno', lambda: fd_out)()

        return self._c_tee(c_fd_in, c_fd_out, len_, c_flags)

    @property
    def available(self):
        '''Availability of `tee`'''

        return self._c_tee is not None


tee = Tee()
del Tee


class Splice(object):
    '''Binding to `splice`'''

    # From `bits/fcntl-linux.h`
    SPLICE_F_MOVE = 1
    SPLICE_F_NONBLOCK = 2
    SPLICE_F_MORE = 4
    SPLICE_F_GIFT = 8

    __slots__ = '_c_splice',

    def __init__(self):
        libc = ctypes.CDLL(ctypes.util.find_library('c'), use_errno=True)

        try:
            c_splice = libc.splice
        except AttributeError:
            self._c_splice = None
            return

        c_loff_t_p = ctypes.POINTER(c_loff_t)

        c_splice.argtypes = [
            ctypes.c_int, c_loff_t_p,
            ctypes.c_int, c_loff_t_p,
            ctypes.c_size_t,
            ctypes.c_uint
        ]

        c_splice.restype = ctypes.c_ssize_t

        def errcheck(result, func, arguments):
            if result == -1:
                errno = ctypes.set_errno(0)

                raise IOError(errno, 'splice: %s' % os.strerror(errno))
            else:
                off_in = arguments[1]
                off_out = arguments[3]

                return (
                    result,
                    off_in.contents.value if off_in is not None else None,
                    off_out.contents.value if off_out is not None else None)

        c_splice.errcheck = errcheck

        self._c_splice = c_splice

    def __call__(self, fd_in, off_in, fd_out, off_out, len_, flags):
        '''See `man 2 splice`

        File-descriptors can be file-like objects with a `fileno` method, or
        integers.

        Flags can be an integer value, or a list of flags (exposed on this
        object).

        Returns a tuple of the result of the `splice` call, the output value of
        `off_in` and the output value of `off_out` (or `None` for any of these
        output values, if applicable).

        Upon other errors, an `IOError` is raised with the proper `errno` set.

        Note: if you want to pass `NULL` as value for `off_in` or `off_out` to
        the system call, you must pass `None`, *not* 0!
        '''

        if not self.available:
            raise EnvironmentError('splice not available')

        if not isinstance(flags, int):
            c_flags = 0
            for flag in flags:
                c_flags |= flag
        else:
            c_flags = flags

        c_fd_in = getattr(fd_in, 'fileno', lambda: fd_in)()
        c_fd_out = getattr(fd_out, 'fileno', lambda: fd_out)()

        c_off_in = \
            ctypes.pointer(c_loff_t(off_in)) if off_in is not None else None
        c_off_out = \
            ctypes.pointer(c_loff_t(off_out)) if off_out is not None else None

        return self._c_splice(
            c_fd_in, c_off_in, c_fd_out, c_off_out, len_, c_flags)

    @property
    def available(self):
        '''Availability of `splice`'''

        return self._c_splice is not None


splice = Splice()
del Splice
