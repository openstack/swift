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


import errno
import gc
import json
from unittest import mock
import os
from subprocess import Popen, PIPE
import sys
from tempfile import mkdtemp
from textwrap import dedent
from time import sleep, time
from collections import defaultdict
import unittest
from uuid import uuid4
import shutil
from http.client import HTTPConnection
from urllib.parse import urlparse

from swiftclient import get_auth, head_account, client
from swift.common import internal_client, direct_client, utils
from swift.common.direct_client import DirectClientException
from swift.common.ring import Ring
from swift.common.utils import hash_path, md5, \
    readconf, renamer, rsync_module_interpolation
from swift.common.manager import Manager
from swift.common.storage_policy import POLICIES, EC_POLICY, REPL_POLICY
from swift.obj.diskfile import get_data_dir
from test.debug_logger import capture_logger

from test.probe import CHECK_SERVER_TIMEOUT, VALIDATE_RSYNC, PROXY_BASE_URL


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


def _check_proxy(user, key):
    url, token = get_auth(PROXY_BASE_URL + '/auth/v1.0',
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
            'test:tester', 'testing'))
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
    unique_ip_count = len({dev['ip'] for dev in ring.devs if dev})
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
    server_names = [name, '%s-replicator' % name]
    if name == 'container':
        server_names.append('container-sharder')
    elif name == 'object':
        server_names.append('object-reconstructor')
    for server_name in server_names:
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
        for line in stdout.decode().splitlines():
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
        self.length = int(total)
        self.hasher = md5(usedforsecurity=False)
        self.read_amount = 0
        self.chunk = uuid4().hex.encode('ascii') * 2 ** 10
        self.buff = b''

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

    def __next__(self):
        if self.buff:
            rv, self.buff = self.buff, b''
            return rv
        if self.read_amount >= self.length:
            raise StopIteration()
        rv = self.chunk[:int(self.length - self.read_amount)]
        self.read_amount += len(rv)
        self.hasher.update(rv)
        return rv


def exclude_nodes(nodes, *excludes):
    """
    Iterate over ``nodes`` yielding only those not in ``excludes``.

    The index key of the node dicts is ignored when matching nodes against the
    ``excludes`` nodes. Index is not a fundamental property of a node but a
    variable annotation added by the Ring depending upon the partition for
    which the nodes were generated.

    :param nodes: an iterable of node dicts.
    :param *excludes: one or more node dicts that should not be yielded.
    :return: yields node dicts.
    """
    for node in nodes:
        match_node = {k: mock.ANY if k == 'index' else v
                      for k, v in node.items()}
        if any(exclude == match_node for exclude in excludes):
            continue
        yield node


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
        for server in Manager(['proxy-server']):
            for conf in server.conf_files():
                self.configs['proxy-server'] = conf

    def setUp(self):
        # previous test may have left DatabaseBroker instances in garbage with
        # open connections to db files which will prevent unmounting devices in
        # resetswift, so collect garbage now
        gc.collect()
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
            proxy_conf = readconf(self.configs['proxy-server'],
                                  section_name='app:proxy-server')
            proxy_ipport = (proxy_conf.get('bind_ip', '127.0.0.1'),
                            int(proxy_conf.get('bind_port', 8080)))
            self.ipport2server[proxy_ipport] = 'proxy'
            self.url, self.token, self.account = check_server(
                proxy_ipport, self.ipport2server)
            self.account_1 = {
                'url': self.url, 'token': self.token, 'account': self.account}

            rv = _retry_timeout(_check_proxy, args=(
                'test2:tester2', 'testing2'))
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

    def assertLengthEqual(self, obj, length):
        obj_len = len(obj)
        self.assertEqual(obj_len, length, 'len(%r) == %d, not %d' % (
            obj, obj_len, length))

    def device_dir(self, node):
        server_type, config_number = get_server_number(
            (node['ip'], node['port']), self.ipport2server)
        repl_server = '%s-replicator' % server_type
        conf = readconf(self.configs[repl_server][config_number],
                        section_name=repl_server)
        return os.path.join(conf['devices'], node['device'])

    def storage_dir(self, node, part=None, policy=None):
        policy = policy or self.policy
        device_path = self.device_dir(node)
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
            allow_account_management = True

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
        return list(all_obj_nodes.values())

    def gather_async_pendings(self, onodes=None):
        """
        Returns a list of paths to async pending files found on given nodes.

        :param onodes: a list of nodes. If None, check all object nodes.
        :return: a list of file paths.
        """
        async_pendings = []
        if onodes is None:
            onodes = self.get_all_object_nodes()
        for onode in onodes:
            device_dir = self.device_dir(onode)
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

    def run_custom_daemon(self, klass, conf_section, conf_index,
                          custom_conf, **kwargs):
        conf_file = self.configs[conf_section][conf_index]
        conf = utils.readconf(conf_file, conf_section)
        conf.update(custom_conf)
        # Use a CaptureLogAdapter in order to preserve the pattern of tests
        # calling the log accessor methods (e.g. get_lines_for_level) directly
        # on the logger instance
        with capture_logger(conf, conf.get('log_name', conf_section),
                            log_to_console=kwargs.pop('verbose', False),
                            log_route=conf_section) as log_adapter:
            daemon = klass(conf, log_adapter)
            daemon.run_once(**kwargs)
        return daemon


def _get_db_file_path(obj_dir):
    files = sorted(os.listdir(obj_dir), reverse=True)
    for filename in files:
        if filename.endswith('db'):
            return os.path.join(obj_dir, filename)


class ReplProbeTest(ProbeTest):

    acct_cont_required_replicas = 3
    acct_cont_required_devices = 4
    obj_required_replicas = 3
    obj_required_devices = 4
    policy_requirements = {'policy_type': REPL_POLICY}

    def direct_container_op(self, func, account=None, container=None,
                            expect_failure=False):
        account = account if account else self.account
        container = container if container else self.container_to_shard
        cpart, cnodes = self.container_ring.get_nodes(account, container)
        unexpected_responses = []
        results = {}
        for cnode in cnodes:
            try:
                results[cnode['id']] = func(cnode, cpart, account, container)
            except DirectClientException as err:
                if not expect_failure:
                    unexpected_responses.append((cnode, err))
            else:
                if expect_failure:
                    unexpected_responses.append((cnode, 'success'))
        if unexpected_responses:
            self.fail('Unexpected responses: %s' % unexpected_responses)
        return results

    def direct_delete_container(self, account=None, container=None,
                                expect_failure=False):
        self.direct_container_op(direct_client.direct_delete_container,
                                 account, container, expect_failure)

    def direct_head_container(self, account=None, container=None,
                              expect_failure=False):
        return self.direct_container_op(direct_client.direct_head_container,
                                        account, container, expect_failure)

    def direct_get_container(self, account=None, container=None,
                             expect_failure=False):
        return self.direct_container_op(direct_client.direct_get_container,
                                        account, container, expect_failure)

    def get_container_db_files(self, container):
        opart, onodes = self.container_ring.get_nodes(self.account, container)
        db_files = []
        for onode in onodes:
            node_id = self.config_number(onode)
            device = onode['device']
            hash_str = hash_path(self.account, container)
            server_conf = readconf(self.configs['container-server'][node_id])
            devices = server_conf['app:container-server']['devices']
            obj_dir = '%s/%s/containers/%s/%s/%s/' % (devices,
                                                      device, opart,
                                                      hash_str[-3:], hash_str)
            db_files.append(_get_db_file_path(obj_dir))

        return db_files


class ECProbeTest(ProbeTest):

    acct_cont_required_replicas = 3
    acct_cont_required_devices = 4
    obj_required_replicas = 6
    obj_required_devices = 8
    policy_requirements = {'policy_type': EC_POLICY}

    def _make_name(self, prefix):
        return ('%s%s' % (prefix, uuid4())).encode()

    def setUp(self):
        super(ECProbeTest, self).setUp()
        self.container_name = self._make_name('container-')
        self.object_name = self._make_name('object-')
        # sanity
        self.assertEqual(self.policy.policy_type, EC_POLICY)
        self.reconstructor = Manager(["object-reconstructor"])

    def proxy_put(self, extra_headers=None):
        contents = Body()
        headers = {
            self._make_name('x-object-meta-').decode('utf8'):
                self._make_name('meta-foo-').decode('utf8'),
        }
        if extra_headers:
            headers.update(extra_headers)
        self.etag = client.put_object(self.url, self.token,
                                      self.container_name,
                                      self.object_name,
                                      contents=contents, headers=headers)

    def proxy_get(self):
        # GET object
        headers, body = client.get_object(self.url, self.token,
                                          self.container_name,
                                          self.object_name,
                                          resp_chunk_size=64 * 2 ** 10)
        resp_checksum = md5(usedforsecurity=False)
        for chunk in body:
            resp_checksum.update(chunk)
        return headers, resp_checksum.hexdigest()

    def direct_get(self, node, part, require_durable=True, extra_headers=None):
        req_headers = {'X-Backend-Storage-Policy-Index': int(self.policy)}
        if extra_headers:
            req_headers.update(extra_headers)
        if not require_durable:
            req_headers.update(
                {'X-Backend-Fragment-Preferences': json.dumps([])})
        headers, data = direct_client.direct_get_object(
            node, part, self.account, self.container_name, self.object_name,
            headers=req_headers, resp_chunk_size=64 * 2 ** 20)
        hasher = md5(usedforsecurity=False)
        for chunk in data:
            hasher.update(chunk)
        return headers, hasher.hexdigest()

    def assert_direct_get_fails(self, onode, opart, status,
                                require_durable=True):
        try:
            self.direct_get(onode, opart, require_durable=require_durable)
        except direct_client.DirectClientException as err:
            self.assertEqual(err.http_status, status)
            return err
        else:
            self.fail('Node data on %r was not fully destroyed!' % (onode,))

    def assert_direct_get_succeeds(self, onode, opart, require_durable=True,
                                   extra_headers=None):
        try:
            return self.direct_get(onode, opart,
                                   require_durable=require_durable,
                                   extra_headers=extra_headers)
        except direct_client.DirectClientException as err:
            self.fail('Node data on %r was not available: %s' % (onode, err))

    def break_nodes(self, nodes, opart, failed, non_durable):
        # delete partitions on the failed nodes and remove durable marker from
        # non-durable nodes
        made_non_durable = 0
        for i, node in enumerate(nodes):
            part_dir = self.storage_dir(node, part=opart)
            if i in failed:
                shutil.rmtree(part_dir, True)
                try:
                    self.direct_get(node, opart)
                except direct_client.DirectClientException as err:
                    self.assertEqual(err.http_status, 404)
            elif i in non_durable:
                for dirs, subdirs, files in os.walk(part_dir):
                    for fname in sorted(files, reverse=True):
                        # make the newest durable be non-durable
                        if fname.endswith('.data'):
                            made_non_durable += 1
                            non_durable_fname = fname.replace('#d', '')
                            os.rename(os.path.join(dirs, fname),
                                      os.path.join(dirs, non_durable_fname))

                            break
                headers, etag = self.direct_get(node, opart,
                                                require_durable=False)
                self.assertNotIn('X-Backend-Durable-Timestamp', headers)
            try:
                os.remove(os.path.join(part_dir, 'hashes.pkl'))
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
        return made_non_durable

    def make_durable(self, nodes, opart):
        # ensure all data files on the specified nodes are durable
        made_durable = 0
        for i, node in enumerate(nodes):
            part_dir = self.storage_dir(node, part=opart)
            for dirs, subdirs, files in os.walk(part_dir):
                for fname in sorted(files, reverse=True):
                    # make the newest non-durable be durable
                    if (fname.endswith('.data') and
                            not fname.endswith('#d.data')):
                        made_durable += 1
                        non_durable_fname = fname.replace('.data', '#d.data')
                        os.rename(os.path.join(dirs, fname),
                                  os.path.join(dirs, non_durable_fname))

                        break
            headers, etag = self.assert_direct_get_succeeds(node, opart)
            self.assertIn('X-Backend-Durable-Timestamp', headers)
            try:
                os.remove(os.path.join(part_dir, 'hashes.pkl'))
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
        return made_durable


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
