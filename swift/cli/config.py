#!/usr/bin/env python
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

import optparse
import os
import sys

from swift.common.manager import Server
from swift.common.utils import readconf
from swift.common.wsgi import appconfig

parser = optparse.OptionParser('%prog [options] SERVER')
parser.add_option('-c', '--config-num', metavar="N", type="int",
                  dest="number", default=0,
                  help="parse config for the Nth server only")
parser.add_option('-s', '--section', help="only display matching sections")
parser.add_option('-w', '--wsgi', action='store_true',
                  help="use wsgi/paste parser instead of readconf")


def _context_name(context):
    return ':'.join((context.object_type.name, context.name))


def inspect_app_config(app_config):
    conf = {}
    context = app_config.context
    section_name = _context_name(context)
    conf[section_name] = context.config()
    if context.object_type.name == 'pipeline':
        filters = context.filter_contexts
        pipeline = []
        for filter_context in filters:
            conf[_context_name(filter_context)] = filter_context.config()
            pipeline.append(filter_context.entry_point_name)
        app_context = context.app_context
        conf[_context_name(app_context)] = app_context.config()
        pipeline.append(app_context.entry_point_name)
        conf[section_name]['pipeline'] = ' '.join(pipeline)
    return conf


def main():
    options, args = parser.parse_args()
    options = dict(vars(options))

    if not args:
        return 'ERROR: specify type of server or conf_path'
    conf_files = []
    for arg in args:
        if os.path.exists(arg):
            conf_files.append(arg)
        else:
            conf_files += Server(arg).conf_files(**options)
    for conf_file in conf_files:
        print('# %s' % conf_file)
        if options['wsgi']:
            app_config = appconfig(conf_file)
            conf = inspect_app_config(app_config)
        else:
            conf = readconf(conf_file)
        flat_vars = {}
        for k, v in conf.items():
            if options['section'] and k != options['section']:
                continue
            if not isinstance(v, dict):
                flat_vars[k] = v
                continue
            print('[%s]' % k)
            for opt, value in v.items():
                print('%s = %s' % (opt, value))
            print()
        for k, v in flat_vars.items():
            print('# %s = %s' % (k, v))
        print()


if __name__ == "__main__":
    sys.exit(main())
