#!/usr/bin/python
# Copyright (c) 2010 OpenStack, LLC.
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

import os
import signal
import sys
from ConfigParser import ConfigParser

from swift.obj.updater import ObjectUpdater
from swift.common import utils

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: object-updater CONFIG_FILE [once]"
        sys.exit()

    once = len(sys.argv) > 2 and sys.argv[2] == 'once'

    c = ConfigParser()
    if not c.read(sys.argv[1]):
        print "Unable to read config file."
        sys.exit(1)

    server_conf = dict(c.items('object-server'))
    if c.has_section('object-updater'):
        updater_conf = dict(c.items('object-updater'))
    else:
        print "Unable to find object-updater config section in %s." % \
                sys.argv[1]
        sys.exit(1)

    utils.drop_privileges(server_conf.get('user', 'swift'))

    try:
        os.setsid()
    except OSError:
        pass

    def kill_children(*args):
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        os.killpg(0, signal.SIGTERM)
        sys.exit()

    signal.signal(signal.SIGTERM, kill_children)

    updater = ObjectUpdater(server_conf, updater_conf)
    if once:
        updater.update_once_single_threaded()
    else:
        updater.update_forever()

