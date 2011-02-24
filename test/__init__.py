# See http://code.google.com/p/python-nose/issues/detail?id=373
# The code below enables nosetests to work with i18n _() blocks

import __builtin__
import sys
import os
from ConfigParser import MissingSectionHeaderError
from StringIO import StringIO

from swift.common.utils import readconf

setattr(__builtin__, '_', lambda x: x)

def get_config():
    config_file = os.environ.get('SWIFT_TEST_CONFIG_FILE',
                                 '/etc/swift/func_test.conf')
    config = {}
    try:
        try:
            config = readconf(config_file, 'func_test')
        except MissingSectionHeaderError:
            config_fp = StringIO('[func_test]\n' + open(config_file).read())
            config = readconf(config_fp, 'func_test')
    except SystemExit:
        print >>sys.stderr, 'UNABLE TO READ FUNCTIONAL TESTS CONFIG FILE'
    return config

