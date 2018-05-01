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

# See http://code.google.com/p/python-nose/issues/detail?id=373
# The code below enables nosetests to work with i18n _() blocks
from __future__ import print_function
import sys
from contextlib import contextmanager

import os
from six import reraise

try:
    from unittest.util import safe_repr
except ImportError:
    # Probably py26
    _MAX_LENGTH = 80

    def safe_repr(obj, short=False):
        try:
            result = repr(obj)
        except Exception:
            result = object.__repr__(obj)
        if not short or len(result) < _MAX_LENGTH:
            return result
        return result[:_MAX_LENGTH] + ' [truncated]...'

from eventlet.green import socket

# make unittests pass on all locale
import swift
setattr(swift, 'gettext_', lambda x: x)

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
        err_typ, err_val, err_tb = sys.exc_info()
        if err_val.args:
            msg = '%s Failed with %s' % (msg, err_val.args[0])
            err_val.args = (msg, ) + err_val.args[1:]
        else:
            # workaround for some IDE's raising custom AssertionErrors
            err_val = '%s Failed with %s' % (msg, err)
            err_typ = AssertionError
        reraise(err_typ, err_val, err_tb)
