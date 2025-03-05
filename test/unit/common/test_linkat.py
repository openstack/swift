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

'''Tests for `swift.common.linkat`'''

import ctypes
import unittest
import os
from unittest import mock
from uuid import uuid4
from tempfile import gettempdir

from swift.common.linkat import linkat
from swift.common.utils import O_TMPFILE

from test.unit import requires_o_tmpfile_support_in_tmp


class TestLinkat(unittest.TestCase):

    def test_flags(self):
        self.assertTrue(hasattr(linkat, 'AT_FDCWD'))
        self.assertTrue(hasattr(linkat, 'AT_SYMLINK_FOLLOW'))

    @mock.patch('swift.common.linkat.linkat._c_linkat', None)
    def test_available(self):
        self.assertFalse(linkat.available)

    @requires_o_tmpfile_support_in_tmp
    def test_errno(self):
        with open('/dev/null', 'r') as fd:
            self.assertRaises(IOError, linkat,
                              linkat.AT_FDCWD, "/proc/self/fd/%s" % (fd),
                              linkat.AT_FDCWD, "%s/testlinkat" % gettempdir(),
                              linkat.AT_SYMLINK_FOLLOW)
        self.assertEqual(ctypes.get_errno(), 0)

    @mock.patch('swift.common.linkat.linkat._c_linkat', None)
    def test_unavailable(self):
        self.assertRaises(EnvironmentError, linkat, 0, None, 0, None, 0)

    def test_unavailable_in_libc(self):

        class LibC(object):

            def __init__(self):
                self.linkat_retrieved = False

            @property
            def linkat(self):
                self.linkat_retrieved = True
                raise AttributeError

        libc = LibC()
        mock_cdll = mock.Mock(return_value=libc)

        with mock.patch('ctypes.CDLL', new=mock_cdll):
            # Force re-construction of a `Linkat` instance
            # Something you're not supposed to do in actual code
            new_linkat = type(linkat)()
            self.assertFalse(new_linkat.available)

        libc_name = ctypes.util.find_library('c')

        mock_cdll.assert_called_once_with(libc_name, use_errno=True)
        self.assertTrue(libc.linkat_retrieved)

    @requires_o_tmpfile_support_in_tmp
    def test_linkat_success(self):

        fd = None
        path = None
        ret = -1
        try:
            fd = os.open(gettempdir(), O_TMPFILE | os.O_WRONLY)
            path = os.path.join(gettempdir(), uuid4().hex)
            ret = linkat(linkat.AT_FDCWD, "/proc/self/fd/%d" % (fd),
                         linkat.AT_FDCWD, path, linkat.AT_SYMLINK_FOLLOW)
            self.assertEqual(ret, 0)
            self.assertTrue(os.path.exists(path))
        finally:
            if fd:
                os.close(fd)
            if path and ret == 0:
                # if linkat succeeded, remove file
                os.unlink(path)

    @mock.patch('swift.common.linkat.linkat._c_linkat')
    def test_linkat_fd_not_integer(self, _mock_linkat):
        self.assertRaises(TypeError, linkat,
                          "not_int", None, "not_int", None, 0)
        self.assertFalse(_mock_linkat.called)
