#!/usr/bin/python -u
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

import unittest

from contextlib import contextmanager
import eventlet
import json
import os
import random
import shutil
import time
from uuid import uuid4

import http.client
from urllib.parse import urlparse

from swift.common.ring import Ring
from swift.common.manager import Manager

from test.probe import PROXY_BASE_URL
from test.probe.common import resetswift, ReplProbeTest, client


def putrequest(conn, method, path, headers):

    conn.putrequest(method, path, skip_host=(headers and 'Host' in headers))
    if headers:
        for header, value in headers.items():
            conn.putheader(header, str(value))
    conn.endheaders()


def get_server_and_worker_pids(manager, old_workers=None):
    # Gets all the server parent pids, as well as the set of all worker PIDs
    # (i.e. any PID whose PPID is in the set of parent pids).
    server_pid_set = {pid for server in manager.servers
                      for (_, pid) in server.iter_pid_files()}
    children_pid_set = set()
    old_worker_pid_set = set(old_workers or [])
    all_pids = [int(f) for f in os.listdir('/proc') if f.isdigit()]
    for pid in all_pids:
        try:
            with open('/proc/%d/status' % pid, 'r') as fh:
                for line in fh:
                    if line.startswith('PPid:\t'):
                        ppid = int(line[6:])
                        if ppid in server_pid_set or pid in old_worker_pid_set:
                            children_pid_set.add(pid)
                        break
        except Exception:
            # No big deal, a process could have exited since we listed /proc,
            # so we just ignore errors
            pass
    return {'server': server_pid_set, 'worker': children_pid_set}


def wait_for_pids(manager, callback, timeout=15, old_workers=None):
    # Waits up to `timeout` seconds for the supplied callback to return True
    # when passed in the manager's pid set.
    start_time = time.time()

    pid_sets = get_server_and_worker_pids(manager, old_workers=old_workers)
    got = callback(pid_sets)
    while not got and time.time() - start_time < timeout:
        time.sleep(0.1)
        pid_sets = get_server_and_worker_pids(manager, old_workers=old_workers)
        got = callback(pid_sets)
    if time.time() - start_time >= timeout:
        raise AssertionError('timed out waiting for PID state; got %r' % (
            pid_sets))
    return pid_sets


class TestWSGIServerProcessHandling(ReplProbeTest):
    # Subclasses need to define SERVER_NAME
    HAS_INFO = False
    PID_TIMEOUT = 25

    def setUp(self):
        super(TestWSGIServerProcessHandling, self).setUp()
        self.container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, self.container,
                             headers={'X-Storage-Policy':
                                      self.policy.name})
        self.manager = Manager([self.SERVER_NAME])
        for server in self.manager.servers:
            self.assertTrue(server.get_running_pids,
                            'No running PIDs for %s' % server.cmd)
        self.starting_pids = get_server_and_worker_pids(self.manager)

    def assert4xx(self, resp):
        self.assertEqual(resp.status // 100, 4)
        got_body = resp.read()
        try:
            self.assertIn('resource could not be found', got_body)
        except AssertionError:
            self.assertIn('Invalid path: blah', got_body)

    def get_conn(self):
        scheme, ip, port = self.get_scheme_ip_port()
        if scheme == 'https':
            return http.client.HTTPSConnection('%s:%s' % (ip, port))
        return http.client.HTTPConnection('%s:%s' % (ip, port))

    def _check_reload(self):
        conn = self.get_conn()
        self.addCleanup(conn.close)

        # sanity request
        self.start_write_req(conn, 'sanity')
        resp = self.finish_write_req(conn)
        self.check_write_resp(resp)

        if self.HAS_INFO:
            self.check_info_value(8192)

        # Start another write request before reloading...
        self.start_write_req(conn, 'across-reload')

        if self.HAS_INFO:
            self.swap_configs()  # new server's max_header_size == 8191

        self.do_reload()

        wait_for_pids(self.manager, self.make_post_reload_pid_cb(),
                      old_workers=self.starting_pids['worker'],
                      timeout=self.PID_TIMEOUT)

        # ... and make sure we can finish what we were doing
        resp = self.finish_write_req(conn)
        self.check_write_resp(resp)

        # After this, we're in a funny spot. With eventlet 0.22.0, the
        # connection's now closed, but with prior versions we could keep
        # going indefinitely. See https://bugs.launchpad.net/swift/+bug/1792615

        # Close our connections, to make sure old eventlet shuts down
        conn.close()

        # sanity
        wait_for_pids(self.manager, self.make_post_close_pid_cb(),
                      old_workers=self.starting_pids['worker'],
                      timeout=self.PID_TIMEOUT)

        if self.HAS_INFO:
            self.check_info_value(8191)


class OldReloadMixin(object):
    def make_post_reload_pid_cb(self):
        def _cb(post_reload_pids):
            # We expect all old server PIDs to be gone, a new server present,
            # and for there to be exactly 1 old worker PID plus additional new
            # worker PIDs.
            old_servers_dead = not (self.starting_pids['server'] &
                                    post_reload_pids['server'])
            one_old_worker = 1 == len(self.starting_pids['worker'] &
                                      post_reload_pids['worker'])
            new_workers_present = (post_reload_pids['worker'] -
                                   self.starting_pids['worker'])
            return (post_reload_pids['server'] and old_servers_dead and
                    one_old_worker and new_workers_present)
        return _cb

    def make_post_close_pid_cb(self):
        def _cb(post_close_pids):
            # We expect all old server PIDs to be gone, a new server present,
            # no old worker PIDs, and additional new worker PIDs.
            old_servers_dead = not (self.starting_pids['server'] &
                                    post_close_pids['server'])
            old_workers_dead = not (self.starting_pids['worker'] &
                                    post_close_pids['worker'])
            new_workers_present = (post_close_pids['worker'] -
                                   self.starting_pids['worker'])
            return (post_close_pids['server'] and old_servers_dead and
                    old_workers_dead and new_workers_present)
        return _cb

    def do_reload(self):
        self.manager.reload()


class SeamlessReloadMixin(object):
    def make_post_reload_pid_cb(self):
        def _cb(post_reload_pids):
            # We expect all orig server PIDs to STILL BE PRESENT, no new server
            # present, and for there to be exactly 1 old worker PID plus
            # additional new worker PIDs.
            same_servers = (self.starting_pids['server'] ==
                            post_reload_pids['server'])
            one_old_worker = 1 == len(self.starting_pids['worker'] &
                                      post_reload_pids['worker'])
            new_workers_present = (post_reload_pids['worker'] -
                                   self.starting_pids['worker'])
            return (post_reload_pids['server'] and same_servers and
                    one_old_worker and new_workers_present)
        return _cb

    def make_post_close_pid_cb(self):
        def _cb(post_close_pids):
            # We expect all orig server PIDs to STILL BE PRESENT, no new server
            # present, no old worker PIDs, and additional new worker PIDs.
            same_servers = (self.starting_pids['server'] ==
                            post_close_pids['server'])
            old_workers_dead = not (self.starting_pids['worker'] &
                                    post_close_pids['worker'])
            new_workers_present = (post_close_pids['worker'] -
                                   self.starting_pids['worker'])
            return (post_close_pids['server'] and same_servers and
                    old_workers_dead and new_workers_present)
        return _cb

    def do_reload(self):
        self.manager.reload_seamless()


class ChildReloadMixin(object):
    def make_post_reload_pid_cb(self):
        def _cb(post_reload_pids):
            # We expect all orig server PIDs to STILL BE PRESENT, no new server
            # present, and for there to be exactly 1 old worker PID plus
            # all but one additional new worker PIDs.
            num_workers = len(self.starting_pids['worker'])
            same_servers = (self.starting_pids['server'] ==
                            post_reload_pids['server'])
            one_old_worker = 1 == len(self.starting_pids['worker'] &
                                      post_reload_pids['worker'])
            new_workers_present = (post_reload_pids['worker'] -
                                   self.starting_pids['worker'])
            return (post_reload_pids['server'] and same_servers and
                    one_old_worker and
                    len(new_workers_present) == num_workers - 1)
        return _cb

    def make_post_close_pid_cb(self):
        def _cb(post_close_pids):
            # We expect all orig server PIDs to STILL BE PRESENT, no new server
            # present, no old worker PIDs, and all new worker PIDs.
            same_servers = (self.starting_pids['server'] ==
                            post_close_pids['server'])
            old_workers_dead = not (self.starting_pids['worker'] &
                                    post_close_pids['worker'])
            new_workers_present = (post_close_pids['worker'] -
                                   self.starting_pids['worker'])
            return (post_close_pids['server'] and same_servers and
                    old_workers_dead and new_workers_present)
        return _cb

    def do_reload(self):
        self.manager.kill_child_pids(seamless=True)


class TestObjectServerReloadBase(TestWSGIServerProcessHandling):
    SERVER_NAME = 'object'
    PID_TIMEOUT = 35

    def get_scheme_ip_port(self):
        self.policy.load_ring('/etc/swift')
        self.ring_node = random.choice(
            self.policy.object_ring.get_part_nodes(1))
        return 'http', self.ring_node['ip'], self.ring_node['port']

    def start_write_req(self, conn, suffix):
        putrequest(conn, 'PUT', '/%s/123/%s/%s/blah-%s' % (
            self.ring_node['device'], self.account, self.container, suffix),
            headers={'X-Timestamp': str(time.time()),
                     'Content-Type': 'application/octet-string',
                     'Content-Length': len(self.BODY),
                     'X-Backend-Storage-Policy-Index': str(self.policy.idx)})

    def finish_write_req(self, conn):
        conn.send(self.BODY)
        return conn.getresponse()

    def check_write_resp(self, resp):
        got_body = resp.read()
        self.assertEqual(resp.status // 100, 2, 'Got status %d; %r' %
                         (resp.status, got_body))
        self.assertEqual(b'', got_body)
        return resp


class TestObjectServerReload(OldReloadMixin, TestObjectServerReloadBase):
    BODY = b'test-object' * 10

    def test_object_reload(self):
        self._check_reload()


class TestObjectServerReloadSeamless(SeamlessReloadMixin,
                                     TestObjectServerReloadBase):
    BODY = b'test-object' * 10

    def test_object_reload_seamless(self):
        self._check_reload()


class TestObjectServerReloadChild(ChildReloadMixin,
                                  TestObjectServerReloadBase):
    BODY = b'test-object' * 10

    def test_object_reload_child(self):
        self._check_reload()


class TestProxyServerReloadBase(TestWSGIServerProcessHandling):
    SERVER_NAME = 'proxy-server'
    HAS_INFO = True

    def setUp(self):
        super(TestProxyServerReloadBase, self).setUp()
        self.swift_conf_path = '/etc/swift/swift.conf'
        self.new_swift_conf_path = self.swift_conf_path + '.new'
        self.saved_swift_conf_path = self.swift_conf_path + '.orig'
        shutil.copy(self.swift_conf_path, self.saved_swift_conf_path)
        with open(self.swift_conf_path, 'r') as rfh:
            config = rfh.read()
            section_header = '\n[swift-constraints]\n'
        if section_header in config:
            config = config.replace(
                section_header,
                section_header + 'max_header_size = 8191\n',
                1)
        else:
            config += section_header + 'max_header_size = 8191\n'
        with open(self.new_swift_conf_path, 'w') as wfh:
            wfh.write(config)
            wfh.flush()

    def tearDown(self):
        shutil.move(self.saved_swift_conf_path, self.swift_conf_path)
        try:
            os.unlink(self.new_swift_conf_path)
        except OSError:
            pass
        super(TestProxyServerReloadBase, self).tearDown()

    def swap_configs(self):
        shutil.copy(self.new_swift_conf_path, self.swift_conf_path)

    def get_scheme_ip_port(self):
        parsed = urlparse(PROXY_BASE_URL)
        host, port = parsed.netloc.partition(':')[::2]
        if not port:
            port = '443' if parsed.scheme == 'https' else '80'
        return parsed.scheme, host, int(port)

    def assertMaxHeaderSize(self, resp, exp_max_header_size):
        self.assertEqual(resp.status // 100, 2)
        info_dict = json.loads(resp.read())
        self.assertEqual(exp_max_header_size,
                         info_dict['swift']['max_header_size'])

    def check_info_value(self, expected_value):
        # show that we're talking to the original server with the default
        # max_header_size == 8192
        conn2 = self.get_conn()
        putrequest(conn2, 'GET', '/info',
                   headers={'Content-Length': '0',
                            'Accept': 'application/json'})
        conn2.send('')
        resp = conn2.getresponse()
        self.assertMaxHeaderSize(resp, expected_value)
        conn2.close()

    def start_write_req(self, conn, suffix):
        putrequest(conn, 'PUT', '/v1/%s/%s/blah-%s' % (
            self.account, self.container, suffix),
            headers={'X-Auth-Token': self.token,
                     'Content-Length': len(self.BODY)})

    def finish_write_req(self, conn):
        conn.send(self.BODY)
        return conn.getresponse()

    def check_write_resp(self, resp):
        got_body = resp.read()
        self.assertEqual(resp.status // 100, 2, 'Got status %d; %r' %
                         (resp.status, got_body))
        self.assertEqual(b'', got_body)
        return resp


class TestProxyServerReload(OldReloadMixin, TestProxyServerReloadBase):
    BODY = b'proxy' * 10

    def test_proxy_reload(self):
        self._check_reload()


class TestProxyServerReloadSeamless(SeamlessReloadMixin,
                                    TestProxyServerReloadBase):
    BODY = b'proxy-seamless' * 10

    def test_proxy_reload_seamless(self):
        self._check_reload()


class TestProxyServerReloadChild(ChildReloadMixin,
                                 TestProxyServerReloadBase):
    BODY = b'proxy-seamless' * 10
    # A bit of a lie, but the respawned child won't pick up the updated config
    HAS_INFO = False

    def test_proxy_reload_child(self):
        self._check_reload()


@contextmanager
def spawn_services(ip_ports, timeout=10):
    q = eventlet.Queue()

    def service(sock):
        try:
            conn, address = sock.accept()
            q.put(address)
            eventlet.sleep(timeout)
            conn.close()
        finally:
            sock.close()

    pool = eventlet.GreenPool()
    for ip, port in ip_ports:
        sock = eventlet.listen((ip, port))
        pool.spawn(service, sock)

    try:
        yield q
    finally:
        for gt in list(pool.coroutines_running):
            gt.kill()


class TestHungDaemon(unittest.TestCase):

    def setUp(self):
        resetswift()
        self.ip_ports = [
            (dev['ip'], dev['port'])
            for dev in Ring('/etc/swift', ring_name='account').devs
            if dev
        ]

    def test_main(self):
        reconciler = Manager(['container-reconciler'])
        with spawn_services(self.ip_ports) as q:
            reconciler.start()
            # wait for the reconciler to connect
            q.get()
            # once it's hung in our connection - send it sig term
            print('Attempting to stop reconciler!')
            reconciler.stop()
        self.assertEqual(1, reconciler.status())


if __name__ == '__main__':
    unittest.main()
