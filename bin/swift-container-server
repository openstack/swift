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

from ConfigParser import ConfigParser
import sys

from swift.common.wsgi import run_wsgi
from swift.container.server import ContainerController

if __name__ == '__main__':
    c = ConfigParser()
    if not c.read(sys.argv[1]):
        print "Unable to read config file."
        sys.exit(1)
    conf = dict(c.items('container-server'))
    run_wsgi(ContainerController, conf, default_port=6001)

