# See http://code.google.com/p/python-nose/issues/detail?id=373
# The code below enables nosetests to work with i18n _() blocks

import __builtin__

setattr(__builtin__, '_', lambda x: x)

