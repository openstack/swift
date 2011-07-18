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

from os import kill
from signal import SIGTERM
from subprocess import call, Popen
from time import sleep

from swift.common.bufferedhttp import http_connect_raw as http_connect
from swift.common.client import get_auth
from swift.common.ring import Ring


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
        port2server = {}
        for s, p in (('account', 6002), ('container', 6001), ('object', 6000)):
            for n in xrange(1, 5):
                pids['%s%d' % (s, n)] = \
                    Popen(['swift-%s-server' % s,
                           '/etc/swift/%s-server/%d.conf' % (s, n)]).pid
                port2server[p + (n * 10)] = '%s%d' % (s, n)
        pids['proxy'] = Popen(['swift-proxy-server',
                               '/etc/swift/proxy-server.conf']).pid
        account_ring = Ring('/etc/swift/account.ring.gz')
        container_ring = Ring('/etc/swift/container.ring.gz')
        object_ring = Ring('/etc/swift/object.ring.gz')
        attempt = 0
        while True:
            attempt += 1
            try:
                url, token = get_auth('http://127.0.0.1:8080/auth/v1.0',
                                      'test:tester', 'testing')
                account = url.split('/')[-1]
                break
            except Exception, err:
                if attempt > 9:
                    print err
                    print 'Giving up after %s retries.' % attempt
                    raise err
                print err
                print 'Retrying in 2 seconds...'
                sleep(2)
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
