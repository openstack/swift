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

'''Tests for `swift.common.splice`'''

import os
import errno
import ctypes
import logging
import tempfile
import unittest
import contextlib
import re

import mock
import six

from swift.common.splice import splice, tee

LOGGER = logging.getLogger(__name__)


def NamedTemporaryFile():
    '''Wrapper to tempfile.NamedTemporaryFile() disabling bufferring.

    The wrapper is used to support Python 2 and Python 3 in the same
    code base.
    '''

    if six.PY3:
        return tempfile.NamedTemporaryFile(buffering=0)
    else:
        return tempfile.NamedTemporaryFile(bufsize=0)


def safe_close(fd):
    '''Close a file descriptor, ignoring any exceptions'''

    try:
        os.close(fd)
    except Exception:
        LOGGER.exception('Error while closing FD')


@contextlib.contextmanager
def pipe():
    '''Context-manager providing 2 ends of a pipe, closing them at exit'''

    fds = os.pipe()

    try:
        yield fds
    finally:
        safe_close(fds[0])
        safe_close(fds[1])


class TestSplice(unittest.TestCase):
    '''Tests for `splice`'''

    def setUp(self):
        if not splice.available:
            raise unittest.SkipTest('splice not available')

    def test_flags(self):
        '''Test flag attribute availability'''

        self.assertTrue(hasattr(splice, 'SPLICE_F_MOVE'))
        self.assertTrue(hasattr(splice, 'SPLICE_F_NONBLOCK'))
        self.assertTrue(hasattr(splice, 'SPLICE_F_MORE'))
        self.assertTrue(hasattr(splice, 'SPLICE_F_GIFT'))

    @mock.patch('swift.common.splice.splice._c_splice', None)
    def test_available(self):
        '''Test `available` attribute correctness'''

        self.assertFalse(splice.available)

    def test_splice_pipe_to_pipe(self):
        '''Test `splice` from a pipe to a pipe'''

        with pipe() as (p1a, p1b):
            with pipe() as (p2a, p2b):
                os.write(p1b, b'abcdef')
                res = splice(p1a, None, p2b, None, 3, 0)
                self.assertEqual(res, (3, None, None))
                self.assertEqual(os.read(p2a, 3), b'abc')
                self.assertEqual(os.read(p1a, 3), b'def')

    def test_splice_file_to_pipe(self):
        '''Test `splice` from a file to a pipe'''

        with NamedTemporaryFile() as fd:
            with pipe() as (pa, pb):
                fd.write(b'abcdef')
                fd.seek(0, os.SEEK_SET)

                res = splice(fd, None, pb, None, 3, 0)
                self.assertEqual(res, (3, None, None))
                # `fd.tell()` isn't updated...
                self.assertEqual(os.lseek(fd.fileno(), 0, os.SEEK_CUR), 3)

                fd.seek(0, os.SEEK_SET)
                res = splice(fd, 3, pb, None, 3, 0)
                self.assertEqual(res, (3, 6, None))
                self.assertEqual(os.lseek(fd.fileno(), 0, os.SEEK_CUR), 0)

                self.assertEqual(os.read(pa, 6), b'abcdef')

    def test_splice_pipe_to_file(self):
        '''Test `splice` from a pipe to a file'''

        with NamedTemporaryFile() as fd:
            with pipe() as (pa, pb):
                os.write(pb, b'abcdef')

                res = splice(pa, None, fd, None, 3, 0)
                self.assertEqual(res, (3, None, None))
                self.assertEqual(fd.tell(), 3)

                fd.seek(0, os.SEEK_SET)

                res = splice(pa, None, fd, 3, 3, 0)
                self.assertEqual(res, (3, None, 6))
                self.assertEqual(fd.tell(), 0)

                self.assertEqual(fd.read(6), b'abcdef')

    @mock.patch.object(splice, '_c_splice')
    def test_fileno(self, mock_splice):
        '''Test handling of file-descriptors'''

        splice(1, None, 2, None, 3, 0)
        self.assertEqual(mock_splice.call_args,
                         ((1, None, 2, None, 3, 0), {}))

        mock_splice.reset_mock()

        with open('/dev/zero', 'r') as fd:
            splice(fd, None, fd, None, 3, 0)
            self.assertEqual(mock_splice.call_args,
                             ((fd.fileno(), None, fd.fileno(), None, 3, 0),
                              {}))

    @mock.patch.object(splice, '_c_splice')
    def test_flags_list(self, mock_splice):
        '''Test handling of flag lists'''

        splice(1, None, 2, None, 3,
               [splice.SPLICE_F_MOVE, splice.SPLICE_F_NONBLOCK])

        flags = splice.SPLICE_F_MOVE | splice.SPLICE_F_NONBLOCK
        self.assertEqual(mock_splice.call_args,
                         ((1, None, 2, None, 3, flags), {}))

        mock_splice.reset_mock()

        splice(1, None, 2, None, 3, [])
        self.assertEqual(mock_splice.call_args,
                         ((1, None, 2, None, 3, 0), {}))

    def test_errno(self):
        '''Test handling of failures'''

        # Invoke EBADF by using a read-only FD as fd_out
        with open('/dev/null', 'r') as fd:
            err = errno.EBADF
            msg = r'\[Errno %d\] splice: %s' % (err, os.strerror(err))
            try:
                splice(fd, None, fd, None, 3, 0)
            except IOError as e:
                self.assertTrue(re.match(msg, str(e)))
            else:
                self.fail('Expected IOError was not raised')

        self.assertEqual(ctypes.get_errno(), 0)

    @mock.patch('swift.common.splice.splice._c_splice', None)
    def test_unavailable(self):
        '''Test exception when unavailable'''

        self.assertRaises(EnvironmentError, splice, 1, None, 2, None, 2, 0)

    def test_unavailable_in_libc(self):
        '''Test `available` attribute when `libc` has no `splice` support'''

        class LibC(object):
            '''A fake `libc` object tracking `splice` attribute access'''

            def __init__(self):
                self.splice_retrieved = False

            @property
            def splice(self):
                self.splice_retrieved = True
                raise AttributeError

        libc = LibC()
        mock_cdll = mock.Mock(return_value=libc)

        with mock.patch('ctypes.CDLL', new=mock_cdll):
            # Force re-construction of a `Splice` instance
            # Something you're not supposed to do in actual code
            new_splice = type(splice)()
            self.assertFalse(new_splice.available)

        libc_name = ctypes.util.find_library('c')

        mock_cdll.assert_called_once_with(libc_name, use_errno=True)
        self.assertTrue(libc.splice_retrieved)


class TestTee(unittest.TestCase):
    '''Tests for `tee`'''

    def setUp(self):
        if not tee.available:
            raise unittest.SkipTest('tee not available')

    @mock.patch('swift.common.splice.tee._c_tee', None)
    def test_available(self):
        '''Test `available` attribute correctness'''

        self.assertFalse(tee.available)

    def test_tee_pipe_to_pipe(self):
        '''Test `tee` from a pipe to a pipe'''

        with pipe() as (p1a, p1b):
            with pipe() as (p2a, p2b):
                os.write(p1b, b'abcdef')
                res = tee(p1a, p2b, 3, 0)
                self.assertEqual(res, 3)
                self.assertEqual(os.read(p2a, 3), b'abc')
                self.assertEqual(os.read(p1a, 6), b'abcdef')

    @mock.patch.object(tee, '_c_tee')
    def test_fileno(self, mock_tee):
        '''Test handling of file-descriptors'''

        with pipe() as (pa, pb):
            tee(pa, pb, 3, 0)
            self.assertEqual(mock_tee.call_args, ((pa, pb, 3, 0), {}))

            mock_tee.reset_mock()

            tee(os.fdopen(pa, 'r'), os.fdopen(pb, 'w'), 3, 0)
            self.assertEqual(mock_tee.call_args, ((pa, pb, 3, 0), {}))

    @mock.patch.object(tee, '_c_tee')
    def test_flags_list(self, mock_tee):
        '''Test handling of flag lists'''

        tee(1, 2, 3, [splice.SPLICE_F_MOVE | splice.SPLICE_F_NONBLOCK])
        flags = splice.SPLICE_F_MOVE | splice.SPLICE_F_NONBLOCK
        self.assertEqual(mock_tee.call_args, ((1, 2, 3, flags), {}))

        mock_tee.reset_mock()

        tee(1, 2, 3, [])
        self.assertEqual(mock_tee.call_args, ((1, 2, 3, 0), {}))

    def test_errno(self):
        '''Test handling of failures'''

        # Invoke EBADF by using a read-only FD as fd_out
        with open('/dev/null', 'r') as fd:
            err = errno.EBADF
            msg = r'\[Errno %d\] tee: %s' % (err, os.strerror(err))
            try:
                tee(fd, fd, 3, 0)
            except IOError as e:
                self.assertTrue(re.match(msg, str(e)))
            else:
                self.fail('Expected IOError was not raised')

        self.assertEqual(ctypes.get_errno(), 0)

    @mock.patch('swift.common.splice.tee._c_tee', None)
    def test_unavailable(self):
        '''Test exception when unavailable'''

        self.assertRaises(EnvironmentError, tee, 1, 2, 2, 0)

    def test_unavailable_in_libc(self):
        '''Test `available` attribute when `libc` has no `tee` support'''

        class LibC(object):
            '''A fake `libc` object tracking `tee` attribute access'''

            def __init__(self):
                self.tee_retrieved = False

            @property
            def tee(self):
                self.tee_retrieved = True
                raise AttributeError

        libc = LibC()
        mock_cdll = mock.Mock(return_value=libc)

        with mock.patch('ctypes.CDLL', new=mock_cdll):
            # Force re-construction of a `Tee` instance
            # Something you're not supposed to do in actual code
            new_tee = type(tee)()
            self.assertFalse(new_tee.available)

        libc_name = ctypes.util.find_library('c')

        mock_cdll.assert_called_once_with(libc_name, use_errno=True)
        self.assertTrue(libc.tee_retrieved)
