# Copyright (c) 2016 OpenStack Foundation
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

import os
import ctypes
from ctypes.util import find_library

__all__ = ['linkat']


class Linkat(object):

    # From include/uapi/linux/fcntl.h
    AT_FDCWD = -100
    AT_SYMLINK_FOLLOW = 0x400

    __slots__ = '_c_linkat'

    def __init__(self):
        libc = ctypes.CDLL(find_library('c'), use_errno=True)

        try:
            c_linkat = libc.linkat
        except AttributeError:
            self._c_linkat = None
            return

        c_linkat.argtypes = [ctypes.c_int, ctypes.c_char_p,
                             ctypes.c_int, ctypes.c_char_p,
                             ctypes.c_int]
        c_linkat.restype = ctypes.c_int

        def errcheck(result, func, arguments):
            if result == -1:
                errno = ctypes.set_errno(0)
                raise IOError(errno, 'linkat: %s' % os.strerror(errno))
            else:
                return result

        c_linkat.errcheck = errcheck

        self._c_linkat = c_linkat

    @property
    def available(self):
        return self._c_linkat is not None

    def __call__(self, olddirfd, oldpath, newdirfd, newpath, flags):
        """
        linkat() creates a new link (also known as a hard link)
        to an existing file.

        See `man 2 linkat` for more info.
        """
        if not self.available:
            raise EnvironmentError('linkat not available')

        if not isinstance(olddirfd, int) or not isinstance(newdirfd, int):
            raise TypeError("fd must be an integer.")

        if isinstance(oldpath, str):
            oldpath = oldpath.encode('utf8')
        if isinstance(newpath, str):
            newpath = newpath.encode('utf8')

        return self._c_linkat(olddirfd, oldpath, newdirfd, newpath, flags)


linkat = Linkat()
del Linkat
