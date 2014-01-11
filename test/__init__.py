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

import sys
import os
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
    except SystemExit:
        if not os.path.exists(config_file):
            print >>sys.stderr, \
                'Unable to read test config %s - file not found' \
                % config_file
        elif not os.access(config_file, os.R_OK):
            print >>sys.stderr, \
                'Unable to read test config %s - permission denied' \
                % config_file
        else:
            print >>sys.stderr, \
                'Unable to read test config %s - section %s not found' \
                % (config_file, section_name)
    return config
