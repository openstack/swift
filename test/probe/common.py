# Copyright (c) 2010-2012 OpenStack, LLC.
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

from httplib import HTTPConnection
from os import kill
from signal import SIGTERM
from subprocess import Popen, PIPE, STDOUT
from time import sleep, time

from swiftclient import get_auth, head_account

from swift.common.ring import Ring

from test.probe import CHECK_SERVER_TIMEOUT


def start_server(port, port2server, pids, check=True):
    server = port2server[port]
    if server[:-1] in ('account', 'container', 'object'):
        pids[server] = Popen([
            'swift-%s-server' % server[:-1],
            '/etc/swift/%s-server/%s.conf' % (server[:-1], server[-1])]).pid
        if check:
            return check_server(port, port2server, pids)
    else:
        pids[server] = Popen(['swift-%s-server' % server,
                              '/etc/swift/%s-server.conf' % server]).pid
        if check:
            return check_server(port, port2server, pids)
    return None


def check_server(port, port2server, pids, timeout=CHECK_SERVER_TIMEOUT):
    server = port2server[port]
    if server[:-1] in ('account', 'container', 'object'):
        path = '/connect/1/2'
        if server[:-1] == 'container':
            path += '/3'
        elif server[:-1] == 'object':
            path += '/3/4'
        try_until = time() + timeout
        while True:
            try:
                conn = HTTPConnection('127.0.0.1', port)
                conn.request('GET', path)
                resp = conn.getresponse()
                if resp.status != 404:
                    raise Exception(
                        'Unexpected status %s' % resp.status)
                break
            except Exception, err:
                if time() > try_until:
                    print err
                    print 'Giving up on %s:%s after %s seconds.' % (
                        server, port, timeout)
                    raise err
                sleep(0.1)
    else:
        try_until = time() + timeout
        while True:
            try:
                url, token = get_auth('http://127.0.0.1:8080/auth/v1.0',
                                      'test:tester', 'testing')
                account = url.split('/')[-1]
                head_account(url, token)
                return url, token, account
            except Exception, err:
                if time() > try_until:
                    print err
                    print 'Giving up on proxy:8080 after 30 seconds.'
                    raise err
                sleep(0.1)
    return None


def kill_server(port, port2server, pids):
    try:
        kill(pids[port2server[port]], SIGTERM)
    except Exception, err:
        print err
    try_until = time() + 30
    while True:
        try:
            conn = HTTPConnection('127.0.0.1', port)
            conn.request('GET', '/')
            conn.getresponse()
        except Exception, err:
            break
        if time() > try_until:
            raise Exception(
                'Still answering on port %s after 30 seconds' % port)
        sleep(0.1)


def kill_servers(port2server, pids):
    for port in port2server:
        kill_server(port, port2server, pids)


def kill_nonprimary_server(primary_nodes, port2server, pids):
    primary_ports = [n['port'] for n in primary_nodes]
    for port, server in port2server.iteritems():
        if port in primary_ports:
            server_type = server[:-1]
            break
    else:
        raise Exception('Cannot figure out server type for %r' % primary_nodes)
    for port, server in list(port2server.iteritems()):
        if server[:-1] == server_type and port not in primary_ports:
            kill_server(port, port2server, pids)
            return port


def reset_environment():
    p = Popen("resetswift 2>&1", shell=True, stdout=PIPE)
    stdout, _stderr = p.communicate()
    print stdout
    pids = {}
    try:
        port2server = {}
        for server, port in [('account', 6002), ('container', 6001),
                             ('object', 6000)]:
            for number in xrange(1, 5):
                port2server[port + (number * 10)] = '%s%d' % (server, number)
        for port in port2server:
            start_server(port, port2server, pids, check=False)
        for port in port2server:
            check_server(port, port2server, pids)
        port2server[8080] = 'proxy'
        url, token, account = start_server(8080, port2server, pids)
        account_ring = Ring('/etc/swift/account.ring.gz')
        container_ring = Ring('/etc/swift/container.ring.gz')
        object_ring = Ring('/etc/swift/object.ring.gz')
    except BaseException:
        try:
            raise
        finally:
            try:
                kill_servers(port2server, pids)
            except Exception:
                pass
    return pids, port2server, account_ring, container_ring, object_ring, url, \
        token, account


def get_to_final_state():
    processes = []
    for job in ('account-replicator', 'container-replicator',
                'object-replicator'):
        for number in xrange(1, 5):
            processes.append(Popen([
                'swift-%s' % job,
                '/etc/swift/%s-server/%d.conf' % (job.split('-')[0], number),
                'once']))
    for process in processes:
        process.wait()
    processes = []
    for job in ('container-updater', 'object-updater'):
        for number in xrange(1, 5):
            processes.append(Popen([
                'swift-%s' % job,
                '/etc/swift/%s-server/%d.conf' % (job.split('-')[0], number),
                'once']))
    for process in processes:
        process.wait()
    processes = []
    for job in ('account-replicator', 'container-replicator',
                'object-replicator'):
        for number in xrange(1, 5):
            processes.append(Popen([
                'swift-%s' % job,
                '/etc/swift/%s-server/%d.conf' % (job.split('-')[0], number),
                'once']))
    for process in processes:
        process.wait()
