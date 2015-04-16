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
from subprocess import Popen, PIPE
import sys
from time import sleep, time
from collections import defaultdict
import unittest
from nose import SkipTest

from swiftclient import get_auth, head_account

from swift.obj.diskfile import get_data_dir
from swift.common.ring import Ring
from swift.common.utils import readconf
from swift.common.manager import Manager
from swift.common.storage_policy import POLICIES, EC_POLICY, REPL_POLICY

from test.probe import CHECK_SERVER_TIMEOUT, VALIDATE_RSYNC


ENABLED_POLICIES = [p for p in POLICIES if not p.is_deprecated]
POLICIES_BY_TYPE = defaultdict(list)
for p in POLICIES:
    POLICIES_BY_TYPE[p.policy_type].append(p)


def get_server_number(port, port2server):
    server_number = port2server[port]
    server, number = server_number[:-1], server_number[-1:]
    try:
        number = int(number)
    except ValueError:
        # probably the proxy
        return server_number, None
    return server, number


def start_server(port, port2server, pids, check=True):
    server, number = get_server_number(port, port2server)
    err = Manager([server]).start(number=number, wait=False)
    if err:
        raise Exception('unable to start %s' % (
            server if not number else '%s%s' % (server, number)))
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
    server, number = get_server_number(port, port2server)
    err = Manager([server]).kill(number=number)
    if err:
        raise Exception('unable to kill %s' % (server if not number else
                                               '%s%s' % (server, number)))
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


def build_port_to_conf(server):
    # map server to config by port
    port_to_config = {}
    for server_ in Manager([server]):
        for config_path in server_.conf_files():
            conf = readconf(config_path,
                            section_name='%s-replicator' % server_.type)
            port_to_config[int(conf['bind_port'])] = conf
    return port_to_config


def get_ring(ring_name, required_replicas, required_devices,
             server=None, force_validate=None):
    if not server:
        server = ring_name
    ring = Ring('/etc/swift', ring_name=ring_name)
    if not VALIDATE_RSYNC and not force_validate:
        return ring
    # easy sanity checks
    if ring.replica_count != required_replicas:
        raise SkipTest('%s has %s replicas instead of %s' % (
            ring.serialized_path, ring.replica_count, required_replicas))
    if len(ring.devs) != required_devices:
        raise SkipTest('%s has %s devices instead of %s' % (
            ring.serialized_path, len(ring.devs), required_devices))
    port_to_config = build_port_to_conf(server)
    for dev in ring.devs:
        # verify server is exposing mounted device
        conf = port_to_config[dev['port']]
        for device in os.listdir(conf['devices']):
            if device == dev['device']:
                dev_path = os.path.join(conf['devices'], device)
                full_path = os.path.realpath(dev_path)
                if not os.path.exists(full_path):
                    raise SkipTest(
                        'device %s in %s was not found (%s)' %
                        (device, conf['devices'], full_path))
                break
        else:
            raise SkipTest(
                "unable to find ring device %s under %s's devices (%s)" % (
                    dev['device'], server, conf['devices']))
        # verify server is exposing rsync device
        if port_to_config[dev['port']].get('vm_test_mode', False):
            rsync_export = '%s%s' % (server, dev['replication_port'])
        else:
            rsync_export = server
        cmd = "rsync rsync://localhost/%s" % rsync_export
        p = Popen(cmd, shell=True, stdout=PIPE)
        stdout, _stderr = p.communicate()
        if p.returncode:
            raise SkipTest('unable to connect to rsync '
                           'export %s (%s)' % (rsync_export, cmd))
        for line in stdout.splitlines():
            if line.rsplit(None, 1)[-1] == dev['device']:
                break
        else:
            raise SkipTest("unable to find ring device %s under rsync's "
                           "exported devices for %s (%s)" %
                           (dev['device'], rsync_export, cmd))
    return ring


def get_policy(**kwargs):
    kwargs.setdefault('is_deprecated', False)
    # go through the policies and make sure they match the
    # requirements of kwargs
    for policy in POLICIES:
        # TODO: for EC, pop policy type here and check it first
        matches = True
        for key, value in kwargs.items():
            try:
                if getattr(policy, key) != value:
                    matches = False
            except AttributeError:
                matches = False
        if matches:
            return policy
    raise SkipTest('No policy matching %s' % kwargs)


class ProbeTest(unittest.TestCase):
    """
    Don't instantiate this directly, use a child class instead.
    """

    def setUp(self):
        p = Popen("resetswift 2>&1", shell=True, stdout=PIPE)
        stdout, _stderr = p.communicate()
        print stdout
        Manager(['all']).stop()
        self.pids = {}
        try:
            self.account_ring = get_ring(
                'account',
                self.acct_cont_required_replicas,
                self.acct_cont_required_devices)
            self.container_ring = get_ring(
                'container',
                self.acct_cont_required_replicas,
                self.acct_cont_required_devices)
            self.policy = get_policy(**self.policy_requirements)
            self.object_ring = get_ring(
                self.policy.ring_name,
                self.obj_required_replicas,
                self.obj_required_devices,
                server='object')
            Manager(['main']).start(wait=False)
            self.port2server = {}
            for server, port in [('account', 6002), ('container', 6001),
                                 ('object', 6000)]:
                for number in xrange(1, 9):
                    self.port2server[port + (number * 10)] = \
                        '%s%d' % (server, number)
            for port in self.port2server:
                check_server(port, self.port2server, self.pids)
            self.port2server[8080] = 'proxy'
            self.url, self.token, self.account = \
                check_server(8080, self.port2server, self.pids)
            self.configs = defaultdict(dict)
            for name in ('account', 'container', 'object'):
                for server_name in (name, '%s-replicator' % name):
                    for server in Manager([server_name]):
                        for i, conf in enumerate(server.conf_files(), 1):
                            self.configs[server.server][i] = conf
            self.replicators = Manager(
                ['account-replicator', 'container-replicator',
                 'object-replicator'])
            self.updaters = Manager(['container-updater', 'object-updater'])
            self.server_port_to_conf = {}
            # get some configs backend daemon configs loaded up
            for server in ('account', 'container', 'object'):
                self.server_port_to_conf[server] = build_port_to_conf(server)
        except BaseException:
            try:
                raise
            finally:
                try:
                    Manager(['all']).kill()
                except Exception:
                    pass

    def tearDown(self):
        Manager(['all']).kill()

    def device_dir(self, server, node):
        conf = self.server_port_to_conf[server][node['port']]
        return os.path.join(conf['devices'], node['device'])

    def storage_dir(self, server, node, part=None, policy=None):
        policy = policy or self.policy
        device_path = self.device_dir(server, node)
        path_parts = [device_path, get_data_dir(policy)]
        if part is not None:
            path_parts.append(str(part))
        return os.path.join(*path_parts)

    def config_number(self, node):
        _server_type, config_number = get_server_number(
            node['port'], self.port2server)
        return config_number

    def get_to_final_state(self):
        # these .stop()s are probably not strictly necessary,
        # but may prevent race conditions
        self.replicators.stop()
        self.updaters.stop()

        self.replicators.once()
        self.updaters.once()
        self.replicators.once()


class ReplProbeTest(ProbeTest):

    acct_cont_required_replicas = 3
    acct_cont_required_devices = 4
    obj_required_replicas = 3
    obj_required_devices = 4
    policy_requirements = {'policy_type': REPL_POLICY}


class ECProbeTest(ProbeTest):

    acct_cont_required_replicas = 3
    acct_cont_required_devices = 4
    obj_required_replicas = 6
    obj_required_devices = 8
    policy_requirements = {'policy_type': EC_POLICY}


if __name__ == "__main__":
    for server in ('account', 'container'):
        try:
            get_ring(server, 3, 4,
                     force_validate=True)
        except SkipTest as err:
            sys.exit('%s ERROR: %s' % (server, err))
        print '%s OK' % server
    for policy in POLICIES:
        try:
            get_ring(policy.ring_name, 3, 4,
                     server='object', force_validate=True)
        except SkipTest as err:
            sys.exit('object ERROR (%s): %s' % (policy.name, err))
        print 'object OK (%s)' % policy.name
