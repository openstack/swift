#!/usr/bin/env python
#-*- coding:utf-8 -*-

import os
import unittest
import string
import sys

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


os.environ['LC_ALL'] = 'eo'
os.environ['SWIFT_LOCALEDIR'] = os.path.dirname(__file__)


class TestTranslations(unittest.TestCase):

    def test_translations(self):
        path = ':'.join(sys.path)
        translated_message = check_output(['python', __file__, path])
        self.assertEquals(translated_message, 'testo mesaĝon\n')


if __name__ == "__main__":
    sys.path = string.split(sys.argv[1], ':')
    from swift import gettext_ as _
    print _('test message')
