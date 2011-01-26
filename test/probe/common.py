# Copyright (c) 2010-2011 OpenStack, LLC.
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

from os import environ, kill
from signal import SIGTERM
from subprocess import call, Popen
from time import sleep
from ConfigParser import ConfigParser

from swift.common.bufferedhttp import http_connect_raw as http_connect
from swift.common.client import get_auth
from swift.common.ring import Ring


SUPER_ADMIN_KEY = None
AUTH_TYPE = None

c = ConfigParser()
AUTH_SERVER_CONF_FILE = environ.get('SWIFT_AUTH_SERVER_CONF_FILE',
                                    '/etc/swift/auth-server.conf')
if c.read(AUTH_SERVER_CONF_FILE):
    conf = dict(c.items('app:auth-server'))
    SUPER_ADMIN_KEY = conf.get('super_admin_key', 'devauth')
    AUTH_TYPE = 'devauth'
else:
    PROXY_SERVER_CONF_FILE = environ.get('SWIFT_PROXY_SERVER_CONF_FILE',
                                         '/etc/swift/proxy-server.conf')
    if c.read(PROXY_SERVER_CONF_FILE):
        conf = dict(c.items('filter:swauth'))
        SUPER_ADMIN_KEY = conf.get('super_admin_key', 'swauthkey')
        AUTH_TYPE = 'swauth'
    else:
        exit('Unable to read config file: %s' % AUTH_SERVER_CONF_FILE)


def kill_pids(pids):
    for pid in pids.values():
        try:
            kill(pid, SIGTERM)
        except Exception:
            pass


def reset_environment():
    call(['resetswift'])
    pids = {}
    try:
        if AUTH_TYPE == 'devauth':
            pids['auth'] = Popen(['swift-auth-server',
                                  '/etc/swift/auth-server.conf']).pid
        pids['proxy'] = Popen(['swift-proxy-server',
                               '/etc/swift/proxy-server.conf']).pid
        port2server = {}
        for s, p in (('account', 6002), ('container', 6001), ('object', 6000)):
            for n in xrange(1, 5):
                pids['%s%d' % (s, n)] = \
                    Popen(['swift-%s-server' % s,
                           '/etc/swift/%s-server/%d.conf' % (s, n)]).pid
                port2server[p + (n * 10)] = '%s%d' % (s, n)
        account_ring = Ring('/etc/swift/account.ring.gz')
        container_ring = Ring('/etc/swift/container.ring.gz')
        object_ring = Ring('/etc/swift/object.ring.gz')
        sleep(5)
        if AUTH_TYPE == 'devauth':
            conn = http_connect('127.0.0.1', '11000', 'POST',
                    '/recreate_accounts',
                    headers={'X-Auth-Admin-User': '.super_admin',
                             'X-Auth-Admin-Key': SUPER_ADMIN_KEY})
            resp = conn.getresponse()
            if resp.status != 200:
                raise Exception('Recreating accounts failed. (%d)' %
                                resp.status)
            url, token = get_auth('http://127.0.0.1:11000/auth', 'test:tester',
                                  'testing')
        elif AUTH_TYPE == 'swauth':
            call(['recreateaccounts'])
            url, token = get_auth('http://127.0.0.1:8080/auth/v1.0',
                                  'test:tester', 'testing')
        account = url.split('/')[-1]
    except BaseException, err:
        kill_pids(pids)
        raise err
    return pids, port2server, account_ring, container_ring, object_ring, url, \
           token, account


def get_to_final_state():
    ps = []
    for job in ('account-replicator', 'container-replicator',
                'object-replicator'):
        for n in xrange(1, 5):
            ps.append(Popen(['swift-%s' % job,
                             '/etc/swift/%s-server/%d.conf' %
                                (job.split('-')[0], n),
                             'once']))
    for p in ps:
        p.wait()
    ps = []
    for job in ('container-updater', 'object-updater'):
        for n in xrange(1, 5):
            ps.append(Popen(['swift-%s' % job,
                             '/etc/swift/%s-server/%d.conf' %
                                (job.split('-')[0], n),
                             'once']))
    for p in ps:
        p.wait()
    ps = []
    for job in ('account-replicator', 'container-replicator',
                'object-replicator'):
        for n in xrange(1, 5):
            ps.append(Popen(['swift-%s' % job,
                             '/etc/swift/%s-server/%d.conf' %
                                (job.split('-')[0], n),
                             'once']))
    for p in ps:
        p.wait()
