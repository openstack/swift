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
import os
import sys

from swift.common.wsgi import run_wsgi
from swift.common.auth import DevAuthMiddleware
from swift.common.memcached import MemcacheRing
from swift.common.utils import get_logger
from swift.proxy.server import Application

if __name__ == '__main__':
    c = ConfigParser()
    if not c.read(sys.argv[1]):
        print "Unable to read config file."
        sys.exit(1)
    conf = dict(c.items('proxy-server'))
    swift_dir = conf.get('swift_dir', '/etc/swift')
    c = ConfigParser()
    c.read(os.path.join(swift_dir, 'auth-server.conf'))
    auth_conf = dict(c.items('auth-server'))

    memcache = MemcacheRing([s.strip() for s in
        conf.get('memcache_servers', '127.0.0.1:11211').split(',')
        if s.strip()])
    logger = get_logger(conf, 'proxy')
    app = Application(conf, memcache, logger)
    # Wrap the app with auth
    app = DevAuthMiddleware(app, auth_conf, memcache, logger)
    run_wsgi(app, conf, logger=logger, default_port=80)
