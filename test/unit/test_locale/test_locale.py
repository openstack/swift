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
import os
import unittest
import sys

from subprocess import check_output


class TestTranslations(unittest.TestCase):
    def test_translations(self):
        translated_message = check_output([sys.executable, __file__], env={
            # Need to set this so py36 can do UTF-8, but we override later
            'LC_ALL': 'en_US.UTF-8',
            # Nothing else should be in the env, so we won't taint our test
        })
        self.assertEqual(translated_message,
                         u'prova mesa\u011do\n'.encode('utf-8'))


if __name__ == "__main__":
    path = os.path.realpath(__file__)
    # Override the language and localedir *before* importing swift
    # so we get translations
    os.environ['LC_ALL'] = 'eo'
    os.environ['SWIFT_LOCALEDIR'] = os.path.dirname(path)
    # Make sure we can find swift
    sys.path.insert(0, os.path.sep.join(path.split(os.path.sep)[:-4]))

    from swift import gettext_ as _
    print(_('test message'))
