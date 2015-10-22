#!/usr/bin/env python
# coding: utf-8
# Copyright (c) 2013 OpenStack Foundation
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

from __future__ import print_function
import eventlet
import os
import unittest
import sys

threading = eventlet.patcher.original('threading')

try:
    from subprocess import check_output
except ImportError:
    from subprocess import Popen, PIPE, CalledProcessError

    def check_output(*popenargs, **kwargs):
        """Lifted from python 2.7 stdlib."""
        if 'stdout' in kwargs:
            raise ValueError('stdout argument not allowed, it will be '
                             'overridden.')
        process = Popen(stdout=PIPE, *popenargs, **kwargs)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            cmd = kwargs.get("args")
            if cmd is None:
                cmd = popenargs[0]
            raise CalledProcessError(retcode, cmd, output=output)
        return output


class TestTranslations(unittest.TestCase):

    def setUp(self):
        self.orig_env = {}
        for var in 'LC_ALL', 'SWIFT_LOCALEDIR', 'LANGUAGE':
            self.orig_env[var] = os.environ.get(var)
        os.environ['LC_ALL'] = 'eo'
        os.environ['SWIFT_LOCALEDIR'] = os.path.dirname(__file__)
        os.environ['LANGUAGE'] = ''
        self.orig_stop = threading._DummyThread._Thread__stop
        # See http://stackoverflow.com/questions/13193278/\
        #     understand-python-threading-bug
        threading._DummyThread._Thread__stop = lambda x: 42

    def tearDown(self):
        for var, val in self.orig_env.items():
            if val is not None:
                os.environ[var] = val
            else:
                del os.environ[var]
        threading._DummyThread._Thread__stop = self.orig_stop

    def test_translations(self):
        path = ':'.join(sys.path)
        translated_message = check_output(['python', __file__, path])
        self.assertEqual(translated_message, 'prova mesaĝo\n')


if __name__ == "__main__":
    os.environ['LC_ALL'] = 'eo'
    os.environ['SWIFT_LOCALEDIR'] = os.path.dirname(__file__)
    sys.path = sys.argv[1].split(':')
    from swift import gettext_ as _
    print(_('test message'))
