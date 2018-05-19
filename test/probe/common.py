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

from __future__ import print_function

import errno
import os
from subprocess import Popen, PIPE
import sys
from tempfile import mkdtemp
from textwrap import dedent
from time import sleep, time
from collections import defaultdict
import unittest
from hashlib import md5
from uuid import uuid4
import shutil
from six.moves.http_client import HTTPConnection
from six.moves.urllib.parse import urlparse

from swiftclient import get_auth, head_account, client
from swift.common import internal_client
from swift.obj.diskfile import get_data_dir
from swift.common.ring import Ring
from swift.common.utils import readconf, renamer, rsync_module_interpolation
from swift.common.manager import Manager
from swift.common.storage_policy import POLICIES, EC_POLICY, REPL_POLICY

from test.probe import CHECK_SERVER_TIMEOUT, VALIDATE_RSYNC


ENABLED_POLICIES = [p for p in POLICIES if not p.is_deprecated]
POLICIES_BY_TYPE = defaultdict(list)
for p in POLICIES:
    POLICIES_BY_TYPE[p.policy_type].append(p)


def get_server_number(ipport, ipport2server):
    server_number = ipport2server[ipport]
    server, number = server_number[:-1], server_number[-1:]
    try:
        number = int(number)
    except ValueError:
        # probably the proxy
        return server_number, None
    return server, number


def start_server(ipport, ipport2server):
    server, number = get_server_number(ipport, ipport2server)
    err = Manager([server]).start(number=number, wait=True)
    if err:
        raise Exception('unable to start %s' % (
            server if not number else '%s%s' % (server, number)))
    return check_server(ipport, ipport2server)


def _check_storage(ipport, path):
    conn = HTTPConnection(*ipport)
    conn.request('GET', path)
    resp = conn.getresponse()
    # 404 because it's a nonsense path (and mount_check is false)
    # 507 in case the test target is a VM using mount_check
    if resp.status not in (404, 507):
        raise Exception(
            'Unexpected status %s' % resp.status)
    return resp


def _check_proxy(ipport, user, key):
    url, token = get_auth('http://%s:%d/auth/v1.0' % ipport,
                          user, key)
    account = url.split('/')[-1]
    head_account(url, token)
    return url, token, account


def _retry_timeout(f, args=None, kwargs=None, timeout=CHECK_SERVER_TIMEOUT):
    args = args or ()
    kwargs = kwargs or {}
    try_until = time() + timeout
    while True:
        try:
            return f(*args, **kwargs)
        except Exception as err:
            if time() > try_until:
                print(err)
                fsignature = '%s(*%r, **%r)' % (f.__name__, args, kwargs)
                print('Giving up on %s after %s seconds.' % (
                    fsignature, timeout))
                raise err
            sleep(0.1)


def check_server(ipport, ipport2server):
    server = ipport2server[ipport]
    if server[:-1] in ('account', 'container', 'object'):
        if int(server[-1]) > 4:
            return None
        path = '/connect/1/2'
        if server[:-1] == 'container':
            path += '/3'
        elif server[:-1] == 'object':
            path += '/3/4'
        rv = _retry_timeout(_check_storage, args=(ipport, path))
    else:
        rv = _retry_timeout(_check_proxy, args=(
            ipport, 'test:tester', 'testing'))
    return rv


def kill_server(ipport, ipport2server):
    server, number = get_server_number(ipport, ipport2server)
    err = Manager([server]).kill(number=number)
    if err:
        raise Exception('unable to kill %s' % (server if not number else
                                               '%s%s' % (server, number)))
    return wait_for_server_to_hangup(ipport)


def wait_for_server_to_hangup(ipport):
    try_until = time() + 30
    while True:
        try:
            conn = HTTPConnection(*ipport)
            conn.request('GET', '/')
            conn.getresponse()
        except Exception:
            break
        if time() > try_until:
            raise Exception(
                'Still answering on %s:%s after 30 seconds' % ipport)
        sleep(0.1)


def kill_nonprimary_server(primary_nodes, ipport2server):
    primary_ipports = [(n['ip'], n['port']) for n in primary_nodes]
    for ipport, server in ipport2server.items():
        if ipport in primary_ipports:
            server_type = server[:-1]
            break
    else:
        raise Exception('Cannot figure out server type for %r' % primary_nodes)
    for ipport, server in list(ipport2server.items()):
        if server[:-1] == server_type and ipport not in primary_ipports:
            kill_server(ipport, ipport2server)
            return ipport


def add_ring_devs_to_ipport2server(ring, server_type, ipport2server,
                                   servers_per_port=0):
    # We'll number the servers by order of unique occurrence of:
    #   IP, if servers_per_port > 0 OR there > 1 IP in ring
    #   ipport, otherwise
    unique_ip_count = len(set(dev['ip'] for dev in ring.devs if dev))
    things_to_number = {}
    number = 0
    for dev in filter(None, ring.devs):
        ip = dev['ip']
        ipport = (ip, dev['port'])
        unique_by = ip if servers_per_port or unique_ip_count > 1 else ipport
        if unique_by not in things_to_number:
            number += 1
            things_to_number[unique_by] = number
        ipport2server[ipport] = '%s%d' % (server_type,
                                          things_to_number[unique_by])


def store_config_paths(name, configs):
    for server_name in (name, '%s-replicator' % name):
        for server in Manager([server_name]):
            for i, conf in enumerate(server.conf_files(), 1):
                configs[server.server][i] = conf


def get_ring(ring_name, required_replicas, required_devices,
             server=None, force_validate=None, ipport2server=None,
             config_paths=None):
    if not server:
        server = ring_name
    ring = Ring('/etc/swift', ring_name=ring_name)
    if ipport2server is None:
        ipport2server = {}  # used internally, even if not passed in
    if config_paths is None:
        config_paths = defaultdict(dict)
    store_config_paths(server, config_paths)

    repl_name = '%s-replicator' % server
    repl_configs = {i: readconf(c, section_name=repl_name)
                    for i, c in config_paths[repl_name].items()}
    servers_per_port = any(int(c.get('servers_per_port', '0'))
                           for c in repl_configs.values())

    add_ring_devs_to_ipport2server(ring, server, ipport2server,
                                   servers_per_port=servers_per_port)
    if not VALIDATE_RSYNC and not force_validate:
        return ring
    # easy sanity checks
    if ring.replica_count != required_replicas:
        raise unittest.SkipTest('%s has %s replicas instead of %s' % (
            ring.serialized_path, ring.replica_count, required_replicas))

    devs = [dev for dev in ring.devs if dev is not None]
    if len(devs) != required_devices:
        raise unittest.SkipTest('%s has %s devices instead of %s' % (
            ring.serialized_path, len(devs), required_devices))
    for dev in devs:
        # verify server is exposing mounted device
        ipport = (dev['ip'], dev['port'])
        _, server_number = get_server_number(ipport, ipport2server)
        conf = repl_configs[server_number]
        for device in os.listdir(conf['devices']):
            if device == dev['device']:
                dev_path = os.path.join(conf['devices'], device)
                full_path = os.path.realpath(dev_path)
                if not os.path.exists(full_path):
                    raise unittest.SkipTest(
                        'device %s in %s was not found (%s)' %
                        (device, conf['devices'], full_path))
                break
        else:
            raise unittest.SkipTest(
                "unable to find ring device %s under %s's devices (%s)" % (
                    dev['device'], server, conf['devices']))
        # verify server is exposing rsync device
        rsync_export = conf.get('rsync_module', '').rstrip('/')
        if not rsync_export:
            rsync_export = '{replication_ip}::%s' % server
        cmd = "rsync %s" % rsync_module_interpolation(rsync_export, dev)
        p = Popen(cmd, shell=True, stdout=PIPE)
        stdout, _stderr = p.communicate()
        if p.returncode:
            raise unittest.SkipTest('unable to connect to rsync '
                                    'export %s (%s)' % (rsync_export, cmd))
        for line in stdout.splitlines():
            if line.rsplit(None, 1)[-1] == dev['device']:
                break
        else:
            raise unittest.SkipTest("unable to find ring device %s under "
                                    "rsync's exported devices for %s (%s)" %
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
    raise unittest.SkipTest('No policy matching %s' % kwargs)


def run_cleanup(cmd):
    p = Popen(cmd + " 2>&1", shell=True, stdout=PIPE)
    stdout, _stderr = p.communicate()
    if p.returncode:
        raise AssertionError(
            'Cleanup with %r failed: stdout: %s, stderr: %s'
            % (cmd, stdout, _stderr))

    print(stdout)
    Manager(['all']).stop()


def resetswift():
    run_cleanup("resetswift")


def kill_orphans():
    run_cleanup("swift-orphans -a 0 -k 9")


class Body(object):

    def __init__(self, total=3.5 * 2 ** 20):
        self.length = total
        self.hasher = md5()
        self.read_amount = 0
        self.chunk = uuid4().hex * 2 ** 10
        self.buff = ''

    @property
    def etag(self):
        return self.hasher.hexdigest()

    def __len__(self):
        return self.length

    def read(self, amount):
        if len(self.buff) < amount:
            try:
                self.buff += next(self)
            except StopIteration:
                pass
        rv, self.buff = self.buff[:amount], self.buff[amount:]
        return rv

    def __iter__(self):
        return self

    def next(self):
        if self.buff:
            rv, self.buff = self.buff, ''
            return rv
        if self.read_amount >= self.length:
            raise StopIteration()
        rv = self.chunk[:int(self.length - self.read_amount)]
        self.read_amount += len(rv)
        self.hasher.update(rv)
        return rv

    def __next__(self):
        return next(self)


class ProbeTest(unittest.TestCase):
    """
    Don't instantiate this directly, use a child class instead.
    """

    def _load_rings_and_configs(self):
        self.ipport2server = {}
        self.configs = defaultdict(dict)
        self.account_ring = get_ring(
            'account',
            self.acct_cont_required_replicas,
            self.acct_cont_required_devices,
            ipport2server=self.ipport2server,
            config_paths=self.configs)
        self.container_ring = get_ring(
            'container',
            self.acct_cont_required_replicas,
            self.acct_cont_required_devices,
            ipport2server=self.ipport2server,
            config_paths=self.configs)
        self.policy = get_policy(**self.policy_requirements)
        self.object_ring = get_ring(
            self.policy.ring_name,
            self.obj_required_replicas,
            self.obj_required_devices,
            server='object',
            ipport2server=self.ipport2server,
            config_paths=self.configs)

    def setUp(self):
        resetswift()
        kill_orphans()
        self._load_rings_and_configs()
        try:
            self.servers_per_port = any(
                int(readconf(c, section_name='object-replicator').get(
                    'servers_per_port', '0'))
                for c in self.configs['object-replicator'].values())

            Manager(['main']).start(wait=True)
            for ipport in self.ipport2server:
                check_server(ipport, self.ipport2server)
            proxy_ipport = ('127.0.0.1', 8080)
            self.ipport2server[proxy_ipport] = 'proxy'
            self.url, self.token, self.account = check_server(
                proxy_ipport, self.ipport2server)
            self.account_1 = {
                'url': self.url, 'token': self.token, 'account': self.account}

            rv = _retry_timeout(_check_proxy, args=(
                proxy_ipport, 'test2:tester2', 'testing2'))
            self.account_2 = {
                k: v for (k, v) in zip(('url', 'token', 'account'), rv)}

            self.replicators = Manager(
                ['account-replicator', 'container-replicator',
                 'object-replicator'])
            self.updaters = Manager(['container-updater', 'object-updater'])
        except BaseException:
            try:
                raise
            finally:
                try:
                    Manager(['all']).kill()
                except Exception:
                    pass
        info_url = "%s://%s/info" % (urlparse(self.url).scheme,
                                     urlparse(self.url).netloc)
        proxy_conn = client.http_connection(info_url)
        self.cluster_info = client.get_capabilities(proxy_conn)

    def tearDown(self):
        Manager(['all']).kill()

    def device_dir(self, server, node):
        server_type, config_number = get_server_number(
            (node['ip'], node['port']), self.ipport2server)
        repl_server = '%s-replicator' % server_type
        conf = readconf(self.configs[repl_server][config_number],
                        section_name=repl_server)
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
            (node['ip'], node['port']), self.ipport2server)
        return config_number

    def is_local_to(self, node1, node2):
        """
        Return True if both ring devices are "local" to each other (on the same
        "server".
        """
        if self.servers_per_port:
            return node1['ip'] == node2['ip']

        # Without a disambiguating IP, for SAIOs, we have to assume ports
        # uniquely identify "servers".  SAIOs should be configured to *either*
        # have unique IPs per node (e.g. 127.0.0.1, 127.0.0.2, etc.) OR unique
        # ports per server (i.e. sdb1 & sdb5 would have same port numbers in
        # the 8-disk EC ring).
        return node1['port'] == node2['port']

    def get_to_final_state(self):
        # these .stop()s are probably not strictly necessary,
        # but may prevent race conditions
        self.replicators.stop()
        self.updaters.stop()

        self.replicators.once()
        self.updaters.once()
        self.replicators.once()

    def kill_drive(self, device):
        if os.path.ismount(device):
            os.system('sudo umount %s' % device)
        else:
            renamer(device, device + "X")

    def revive_drive(self, device):
        disabled_name = device + "X"
        if os.path.isdir(disabled_name):
            renamer(disabled_name, device)
        else:
            os.system('sudo mount %s' % device)

    def make_internal_client(self):
        tempdir = mkdtemp()
        try:
            conf_path = os.path.join(tempdir, 'internal_client.conf')
            conf_body = """
            [DEFAULT]
            swift_dir = /etc/swift

            [pipeline:main]
            pipeline = catch_errors cache copy proxy-server

            [app:proxy-server]
            use = egg:swift#proxy

            [filter:copy]
            use = egg:swift#copy

            [filter:cache]
            use = egg:swift#memcache

            [filter:catch_errors]
            use = egg:swift#catch_errors
            """
            with open(conf_path, 'w') as f:
                f.write(dedent(conf_body))
            return internal_client.InternalClient(conf_path, 'test', 1)
        finally:
            shutil.rmtree(tempdir)

    def get_all_object_nodes(self):
        """
        Returns a list of all nodes in all object storage policies.

        :return: a list of node dicts.
        """
        all_obj_nodes = {}
        for policy in ENABLED_POLICIES:
            for dev in policy.object_ring.devs:
                all_obj_nodes[dev['device']] = dev
        return all_obj_nodes.values()

    def gather_async_pendings(self, onodes):
        """
        Returns a list of paths to async pending files found on given nodes.

        :param onodes: a list of nodes.
        :return: a list of file paths.
        """
        async_pendings = []
        for onode in onodes:
            device_dir = self.device_dir('', onode)
            for ap_pol_dir in os.listdir(device_dir):
                if not ap_pol_dir.startswith('async_pending'):
                    # skip 'objects', 'containers', etc.
                    continue
                async_pending_dir = os.path.join(device_dir, ap_pol_dir)
                try:
                    ap_dirs = os.listdir(async_pending_dir)
                except OSError as err:
                    if err.errno == errno.ENOENT:
                        pass
                    else:
                        raise
                else:
                    for ap_dir in ap_dirs:
                        ap_dir_fullpath = os.path.join(
                            async_pending_dir, ap_dir)
                        async_pendings.extend([
                            os.path.join(ap_dir_fullpath, ent)
                            for ent in os.listdir(ap_dir_fullpath)])
        return async_pendings


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
        except unittest.SkipTest as err:
            sys.exit('%s ERROR: %s' % (server, err))
        print('%s OK' % server)
    for policy in POLICIES:
        try:
            get_ring(policy.ring_name, 3, 4,
                     server='object', force_validate=True)
        except unittest.SkipTest as err:
            sys.exit('object ERROR (%s): %s' % (policy.name, err))
        print('object OK (%s)' % policy.name)
