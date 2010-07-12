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

from swift.common import wsgi


class TestWSGI(unittest.TestCase):
    """ Tests for swift.common.wsgi """

    def test_monkey_patch_mimetools(self):
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).type, 'text/plain')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).plisttext, '')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).maintype, 'text')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).subtype, 'plain')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).type, 'text/html')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).plisttext,
                          '; charset=ISO-8859-4')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).maintype, 'text')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).subtype, 'html')

        wsgi.monkey_patch_mimetools()
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).type, None)
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).plisttext, '')
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).maintype, None)
        sio = StringIO('blah')
        self.assertEquals(mimetools.Message(sio).subtype, None)
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).type, 'text/html')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).plisttext,
                          '; charset=ISO-8859-4')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).maintype, 'text')
        sio = StringIO('Content-Type: text/html; charset=ISO-8859-4')
        self.assertEquals(mimetools.Message(sio).subtype, 'html')

if __name__ == '__main__':
    unittest.main()
