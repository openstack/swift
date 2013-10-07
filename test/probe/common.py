# Copyright (c) 2010-2012 OpenStack Foundation
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
import os
from os import kill, path
from signal import SIGTERM
from subprocess import Popen, PIPE
import sys
from time import sleep, time

from swiftclient import get_auth, head_account

from swift.common.ring import Ring
from swift.common.utils import readconf

from test.probe import CHECK_SERVER_TIMEOUT, VALIDATE_RSYNC


def start_server(port, port2server, pids, check=True):
    server = port2server[port]
    if server[:-1] in ('account', 'container', 'object'):
        if not path.exists('/etc/swift/%s-server/%s.conf' %
                           (server[:-1], server[-1])):
            return None
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
        if int(server[-1]) > 4:
            return None
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
                # 404 because it's a nonsense path (and mount_check is false)
                # 507 in case the test target is a VM using mount_check
                if resp.status not in (404, 507):
                    raise Exception(
                        'Unexpected status %s' % resp.status)
                break
            except Exception as err:
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
            except Exception as err:
                if time() > try_until:
                    print err
                    print 'Giving up on proxy:8080 after 30 seconds.'
                    raise err
                sleep(0.1)
    return None


def kill_server(port, port2server, pids):
    try:
        kill(pids[port2server[port]], SIGTERM)
    except Exception as err:
        print err
    try_until = time() + 30
    while True:
        try:
            conn = HTTPConnection('127.0.0.1', port)
            conn.request('GET', '/')
            conn.getresponse()
        except Exception as err:
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


def get_ring(server, force_validate=None):
    ring = Ring('/etc/swift/%s.ring.gz' % server)
    if not VALIDATE_RSYNC and not force_validate:
        return ring
    # easy sanity checks
    assert 3 == ring.replica_count, '%s has %s replicas instead of 3' % (
        ring.serialized_path, ring.replica_count)
    assert 4 == len(ring.devs), '%s has %s devices instead of 4' % (
        ring.serialized_path, len(ring.devs))
    # map server to config by port
    port_to_config = {}
    for node_id in range(1, 5):
        conf = readconf('/etc/swift/%s-server/%d.conf' % (server, node_id),
                        section_name='%s-replicator' % server)
        port_to_config[int(conf['bind_port'])] = conf
    for dev in ring.devs:
        # verify server is exposing mounted device
        conf = port_to_config[dev['port']]
        for device in os.listdir(conf['devices']):
            if device == dev['device']:
                full_path = path.realpath(path.join(conf['devices'], device))
                assert path.ismount(full_path), \
                    'device %s in %s was not mounted (%s)' % (
                        device, conf['devices'], full_path)
                break
        else:
            raise AssertionError(
                "unable to find ring device %s under %s's devices (%s)" % (
                    dev['device'], server, conf['devices']))
        # verify server is exposing rsync device
        rsync_export = '%s%s' % (server, dev['replication_port'])
        cmd = "rsync rsync://localhost/%s" % rsync_export
        p = Popen(cmd, shell=True, stdout=PIPE)
        stdout, _stderr = p.communicate()
        if p.returncode:
            raise AssertionError('unable to connect to rsync '
                                 'export %s (%s)' % (rsync_export, cmd))
        for line in stdout.splitlines():
            if line.rsplit(None, 1)[-1] == dev['device']:
                break
        else:
            raise AssertionError("unable to find ring device %s under rsync's "
                                 "exported devices for %s (%s)" % (
                                     dev['device'], rsync_export, cmd))
    return ring


def reset_environment():
    p = Popen("resetswift 2>&1", shell=True, stdout=PIPE)
    stdout, _stderr = p.communicate()
    print stdout
    pids = {}
    try:
        account_ring = get_ring('account')
        container_ring = get_ring('container')
        object_ring = get_ring('object')
        port2server = {}
        config_dict = {}
        for server, port in [('account', 6002), ('container', 6001),
                             ('object', 6000)]:
            for number in xrange(1, 9):
                port2server[port + (number * 10)] = '%s%d' % (server, number)
        for port in port2server:
            start_server(port, port2server, pids, check=False)
        for port in port2server:
            check_server(port, port2server, pids)
        port2server[8080] = 'proxy'
        url, token, account = start_server(8080, port2server, pids)
        for name in ('account', 'container', 'object'):
            for server in (name, '%s-replicator' % name):
                config_dict[server] = '/etc/swift/%s-server/%%d.conf' % name
    except BaseException:
        try:
            raise
        except AssertionError as e:
            print >>sys.stderr, 'ERROR: %s' % e
            os._exit(1)
        finally:
            try:
                kill_servers(port2server, pids)
            except Exception:
                pass
    return pids, port2server, account_ring, container_ring, object_ring, url, \
        token, account, config_dict


def get_to_final_state():
    processes = []
    for job in ('account-replicator', 'container-replicator',
                'object-replicator'):
        for number in xrange(1, 9):
            if not path.exists('/etc/swift/%s-server/%d.conf' %
                               (job.split('-')[0], number)):
                continue
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
        for number in xrange(1, 9):
            if not path.exists('/etc/swift/%s-server/%d.conf' %
                               (job.split('-')[0], number)):
                continue
            processes.append(Popen([
                'swift-%s' % job,
                '/etc/swift/%s-server/%d.conf' % (job.split('-')[0], number),
                'once']))
    for process in processes:
        process.wait()


if __name__ == "__main__":
    for server in ('account', 'container', 'object'):
        get_ring(server, force_validate=True)
        print '%s OK' % server
