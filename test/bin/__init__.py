'''Tests for packaged "bin" scripts
'''

import os
import imp  # access the import internals

bin_test_dir = os.path.dirname(__file__)

bin_dir = os.path.join(bin_test_dir, '../../bin')

# build up logical mapping of bin 'file-name' to (module, path)
modules = {}
for file_name in os.listdir(bin_dir):
    module = '.'.join(['test', 'bin', file_name.replace('-', '_')])
    path = os.path.join(bin_dir, file_name)
    modules[file_name] = (module, path)

def get_bin_module(bin_file_name):
    name, path = modules[bin_file_name]
    try:
        module = imp.load_source(name, path)
    finally:
        # another option would be adding bin/*c to .bzrignore
        try:
            os.unlink(path + 'c')
        except OSError:
            pass
    return module

# this might not be safe on source files that don't test __name__=="__main__"
swift_init = get_bin_module('swift-init')
st = get_bin_module('st')
