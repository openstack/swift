# Copyright (c) 2010-2012 OpenStack Foundation
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

import sys
from contextlib import contextmanager

import os

from unittest.util import safe_repr

import warnings
warnings.filterwarnings('ignore', module='cryptography|OpenSSL', message=(
    'Python 2 is no longer supported by the Python core team. '
    'Support for it is now deprecated in cryptography, '
    'and will be removed in a future release.'))
warnings.filterwarnings('ignore', module='cryptography|OpenSSL', message=(
    'Python 2 is no longer supported by the Python core team. '
    'Support for it is now deprecated in cryptography, '
    'and will be removed in the next release.'))
warnings.filterwarnings('ignore', message=(
    'Python 3.6 is no longer supported by the Python core team. '
    'Therefore, support for it is deprecated in cryptography '
    'and will be removed in a future release.'))

import unittest

from eventlet.green import socket

from swift.common.utils import readconf


# Work around what seems to be a Python bug.
# c.f. https://bugs.launchpad.net/swift/+bug/820185.
import logging
logging.raiseExceptions = False


def get_config(section_name=None, defaults=None):
    """
    Attempt to get a test config dictionary.

    :param section_name: the section to read (all sections if not defined)
    :param defaults: an optional dictionary namespace of defaults
    """
    config = {}
    if defaults is not None:
        config.update(defaults)

    config_file = os.environ.get('SWIFT_TEST_CONFIG_FILE',
                                 '/etc/swift/test.conf')
    try:
        config = readconf(config_file, section_name)
    except IOError:
        if not os.path.exists(config_file):
            print('Unable to read test config %s - file not found'
                  % config_file, file=sys.stderr)
        elif not os.access(config_file, os.R_OK):
            print('Unable to read test config %s - permission denied'
                  % config_file, file=sys.stderr)
    except ValueError as e:
        print(e)
    return config


def listen_zero():
    """
    The eventlet.listen() always sets SO_REUSEPORT, so when called with
    ("localhost",0), instead of returning unique ports it can return the
    same port twice. That causes our tests to fail, so open-code it here
    without SO_REUSEPORT.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    sock.listen(50)
    return sock


@contextmanager
def annotate_failure(msg):
    """
    Catch AssertionError and annotate it with a message. Useful when making
    assertions in a loop where the message can indicate the loop index or
    richer context about the failure.

    :param msg: A message to be prefixed to the AssertionError message.
    """
    try:
        yield
    except AssertionError as err:
        if err.args:
            msg = '%s Failed with %s' % (msg, err.args[0])
            err.args = (msg, ) + err.args[1:]
            raise err
        else:
            # workaround for some IDE's raising custom AssertionErrors
            raise AssertionError(
                '%s Failed with %s' % (msg, err)
            ).with_traceback(err.__traceback__) from err.__cause__


class BaseTestCase(unittest.TestCase):
    def _assertDictContainsSubset(self, subset, dictionary, msg=None):
        """Checks whether dictionary is a superset of subset."""
        # This is almost identical to the method in python3.4 version of
        # unitest.case.TestCase.assertDictContainsSubset, reproduced here to
        # avoid the deprecation warning in the original when using python3.
        missing = []
        mismatched = []
        for key, value in subset.items():
            if key not in dictionary:
                missing.append(key)
            elif value != dictionary[key]:
                mismatched.append('%s, expected: %s, actual: %s' %
                                  (safe_repr(key), safe_repr(value),
                                   safe_repr(dictionary[key])))

        if not (missing or mismatched):
            return

        standardMsg = ''
        if missing:
            standardMsg = 'Missing: %s' % ','.join(safe_repr(m) for m in
                                                   missing)
        if mismatched:
            if standardMsg:
                standardMsg += '; '
            standardMsg += 'Mismatched values: %s' % ','.join(mismatched)

        self.fail(self._formatMessage(msg, standardMsg))
