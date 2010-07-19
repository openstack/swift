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

""" Tests for swift.common.utils """

from __future__ import with_statement
import logging
import mimetools
import os
import socket
import sys
import unittest
from getpass import getuser
from shutil import rmtree
from StringIO import StringIO

from eventlet import sleep

from swift.common import utils


class TestUtils(unittest.TestCase):
    """ Tests for swift.common.utils """

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'

    def test_normalize_timestamp(self):
        """ Test swift.common.utils.normalize_timestamp """
        self.assertEquals(utils.normalize_timestamp('1253327593.48174'),
                          "1253327593.48174")
        self.assertEquals(utils.normalize_timestamp(1253327593.48174),
                          "1253327593.48174")
        self.assertEquals(utils.normalize_timestamp('1253327593.48'),
                          "1253327593.48000")
        self.assertEquals(utils.normalize_timestamp(1253327593.48),
                          "1253327593.48000")
        self.assertEquals(utils.normalize_timestamp('253327593.48'),
                          "0253327593.48000")
        self.assertEquals(utils.normalize_timestamp(253327593.48),
                          "0253327593.48000")
        self.assertEquals(utils.normalize_timestamp('1253327593'),
                          "1253327593.00000")
        self.assertEquals(utils.normalize_timestamp(1253327593),
                          "1253327593.00000")
        self.assertRaises(ValueError, utils.normalize_timestamp, '')
        self.assertRaises(ValueError, utils.normalize_timestamp, 'abc')

    def test_mkdirs(self):
        testroot = os.path.join(os.path.dirname(__file__), 'mkdirs')
        try:
            os.unlink(testroot)
        except:
            pass
        rmtree(testroot, ignore_errors=1)
        self.assert_(not os.path.exists(testroot))
        utils.mkdirs(testroot)
        self.assert_(os.path.exists(testroot))
        utils.mkdirs(testroot)
        self.assert_(os.path.exists(testroot))
        rmtree(testroot, ignore_errors=1)

        testdir = os.path.join(testroot, 'one/two/three')
        self.assert_(not os.path.exists(testdir))
        utils.mkdirs(testdir)
        self.assert_(os.path.exists(testdir))
        utils.mkdirs(testdir)
        self.assert_(os.path.exists(testdir))
        rmtree(testroot, ignore_errors=1)

        open(testroot, 'wb').close()
        self.assert_(not os.path.exists(testdir))
        self.assertRaises(OSError, utils.mkdirs, testdir)
        os.unlink(testroot)

    def test_split_path(self):
        """ Test swift.common.utils.split_account_path """
        self.assertRaises(ValueError, utils.split_path, '')
        self.assertRaises(ValueError, utils.split_path, '/')
        self.assertRaises(ValueError, utils.split_path, '//')
        self.assertEquals(utils.split_path('/a'), ['a'])
        self.assertRaises(ValueError, utils.split_path, '//a')
        self.assertEquals(utils.split_path('/a/'), ['a'])
        self.assertRaises(ValueError, utils.split_path, '/a/c')
        self.assertRaises(ValueError, utils.split_path, '//c')
        self.assertRaises(ValueError, utils.split_path, '/a/c/')
        self.assertRaises(ValueError, utils.split_path, '/a//')
        self.assertRaises(ValueError, utils.split_path, '/a', 2)
        self.assertRaises(ValueError, utils.split_path, '/a', 2, 3)
        self.assertRaises(ValueError, utils.split_path, '/a', 2, 3, True)
        self.assertEquals(utils.split_path('/a/c', 2), ['a', 'c'])
        self.assertEquals(utils.split_path('/a/c/o', 3), ['a', 'c', 'o'])
        self.assertRaises(ValueError, utils.split_path, '/a/c/o/r', 3, 3)
        self.assertEquals(utils.split_path('/a/c/o/r', 3, 3, True),
                          ['a', 'c', 'o/r'])
        self.assertEquals(utils.split_path('/a/c', 2, 3, True),
                          ['a', 'c', None])
        self.assertRaises(ValueError, utils.split_path, '/a', 5, 4)
        self.assertEquals(utils.split_path('/a/c/', 2), ['a', 'c'])
        self.assertEquals(utils.split_path('/a/c/', 2, 3), ['a', 'c', ''])
        try:
            utils.split_path('o\nn e', 2)
        except ValueError, err:
            self.assertEquals(str(err), 'Invalid path: o%0An%20e')
        try:
            utils.split_path('o\nn e', 2, 3, True)
        except ValueError, err:
            self.assertEquals(str(err), 'Invalid path: o%0An%20e')

    def test_NullLogger(self):
        """ Test swift.common.utils.NullLogger """
        sio = StringIO()
        nl = utils.NullLogger()
        nl.write('test')
        self.assertEquals(sio.getvalue(), '')

    def test_LoggerFileObject(self):
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        sio = StringIO()
        handler = logging.StreamHandler(sio)
        logger = logging.getLogger()
        logger.addHandler(handler)
        lfo = utils.LoggerFileObject(logger)
        print 'test1'
        self.assertEquals(sio.getvalue(), '')
        sys.stdout = lfo
        print 'test2'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\n')
        sys.stderr = lfo
        print >>sys.stderr, 'test4'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n')
        sys.stdout = orig_stdout
        print 'test5'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n')
        print >>sys.stderr, 'test6'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
            'STDOUT: test6\n')
        sys.stderr = orig_stderr
        print 'test8'
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
            'STDOUT: test6\n')
        lfo.writelines(['a', 'b', 'c'])
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
            'STDOUT: test6\nSTDOUT: a#012b#012c\n')
        lfo.close()
        lfo.write('d')
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
            'STDOUT: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
        lfo.flush()
        self.assertEquals(sio.getvalue(), 'STDOUT: test2\nSTDOUT: test4\n'
            'STDOUT: test6\nSTDOUT: a#012b#012c\nSTDOUT: d\n')
        got_exc = False
        try:
            for line in lfo:
                pass
        except:
            got_exc = True
        self.assert_(got_exc)
        got_exc = False
        try:
            for line in lfo.xreadlines():
                pass
        except:
            got_exc = True
        self.assert_(got_exc)
        self.assertRaises(IOError, lfo.read)
        self.assertRaises(IOError, lfo.read, 1024)
        self.assertRaises(IOError, lfo.readline)
        self.assertRaises(IOError, lfo.readline, 1024)
        lfo.tell()

    def test_drop_privileges(self):
        # Note that this doesn't really drop privileges as it just sets them to
        # what they already are; but it exercises the code at least.
        utils.drop_privileges(getuser())

    def test_NamedLogger(self):
        sio = StringIO()
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sio))
        nl = utils.NamedLogger(logger, 'server')
        nl.warn('test')
        self.assertEquals(sio.getvalue(), 'server test\n')

    def test_get_logger(self):
        sio = StringIO()
        logger = logging.getLogger()
        logger.addHandler(logging.StreamHandler(sio))
        logger = utils.get_logger(None, 'server')
        logger.warn('test1')
        self.assertEquals(sio.getvalue(), 'server test1\n')
        logger.debug('test2')
        self.assertEquals(sio.getvalue(), 'server test1\n')
        logger = utils.get_logger({'log_level': 'DEBUG'}, 'server')
        logger.debug('test3')
        self.assertEquals(sio.getvalue(), 'server test1\nserver test3\n')
        # Doesn't really test that the log facility is truly being used all the
        # way to syslog; but exercises the code.
        logger = utils.get_logger({'log_facility': 'LOG_LOCAL3'}, 'server')
        logger.warn('test4')
        self.assertEquals(sio.getvalue(),
                          'server test1\nserver test3\nserver test4\n')
        logger.debug('test5')
        self.assertEquals(sio.getvalue(),
                          'server test1\nserver test3\nserver test4\n')

    def test_storage_directory(self):
        self.assertEquals(utils.storage_directory('objects', '1', 'ABCDEF'),
                'objects/1/DEF/ABCDEF')

    def test_whataremyips(self):
        myips = utils.whataremyips()
        self.assert_(len(myips) > 1)
        self.assert_('127.0.0.1' in myips)

    def test_hash_path(self):
        # Yes, these tests are deliberately very fragile. We want to make sure
        # that if someones changes the results hash_path produces, they know it.
        self.assertEquals(utils.hash_path('a'),
                          '1c84525acb02107ea475dcd3d09c2c58')
        self.assertEquals(utils.hash_path('a', 'c'),
                          '33379ecb053aa5c9e356c68997cbb59e')
        self.assertEquals(utils.hash_path('a', 'c', 'o'),
                          '06fbf0b514e5199dfc4e00f42eb5ea83')
        self.assertEquals(utils.hash_path('a', 'c', 'o', raw_digest=False),
                          '06fbf0b514e5199dfc4e00f42eb5ea83')
        self.assertEquals(utils.hash_path('a', 'c', 'o', raw_digest=True),
                  '\x06\xfb\xf0\xb5\x14\xe5\x19\x9d\xfcN\x00\xf4.\xb5\xea\x83')
        self.assertRaises(ValueError, utils.hash_path, 'a', object='o')


if __name__ == '__main__':
    unittest.main()
