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
from sys import argv, exit

from swift.common.bufferedhttp import http_connect_raw as http_connect


if __name__ == '__main__':
    f = '/etc/swift/auth-server.conf'
    if len(argv) == 5:
        f = argv[4]
    elif len(argv) != 4:
        exit('Syntax: %s <new_account> <new_user> <new_password> [conf_file]' %
             argv[0])
    new_account = argv[1]
    new_user = argv[2]
    new_password = argv[3]
    c = ConfigParser()
    if not c.read(f):
        exit('Unable to read conf file: %s' % f)
    conf = dict(c.items('auth-server'))
    host = conf.get('bind_ip', '127.0.0.1')
    port = int(conf.get('bind_port', 11000))
    path = '/account/%s/%s' % (new_account, new_user)
    conn = http_connect(host, port, 'PUT', path, {'x-auth-key':new_password})
    resp = conn.getresponse()
    if resp.status == 204:
        print resp.getheader('x-storage-url')
    else:
        print 'Account creation failed. (%d)' % resp.status
