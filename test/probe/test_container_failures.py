#!/usr/bin/python -u
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

from os import listdir
from os.path import join as path_join
from unittest import main, TestCase
from uuid import uuid4

from eventlet import GreenPool, Timeout
from sqlite3 import connect
from swiftclient import client

from swift.common import direct_client
from swift.common.utils import hash_path, readconf
from test.probe.common import get_to_final_state, kill_nonprimary_server, \
    kill_server, kill_servers, reset_environment, start_server


def get_db_file_path(obj_dir):
    files = sorted(listdir(obj_dir), reverse=True)
    for filename in files:
        if filename.endswith('db'):
            return path_join(obj_dir, filename)


class TestContainerFailures(TestCase):

    def setUp(self):
        (self.pids, self.port2server, self.account_ring, self.container_ring,
         self.object_ring, self.url, self.token,
         self.account) = reset_environment()

    def tearDown(self):
        kill_servers(self.port2server, self.pids)

    def test_one_node_fails(self):
        # Create container1
        # Kill container1 servers excepting two of the primaries
        # Delete container1
        # Restart other container1 primary server
        # Create container1/object1 (allowed because at least server thinks the
        #   container exists)
        # Get to a final state
        # Assert all container1 servers indicate container1 is alive and
        #   well with object1
        # Assert account level also indicates container1 is alive and
        #   well with object1
        container1 = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container1)
        client.put_container(self.url, self.token, container1)
        kill_nonprimary_server(cnodes, self.port2server, self.pids)
        kill_server(cnodes[0]['port'], self.port2server, self.pids)
        client.delete_container(self.url, self.token, container1)
        start_server(cnodes[0]['port'], self.port2server, self.pids)
        client.put_object(self.url, self.token, container1, 'object1', '123')
        get_to_final_state()
        for cnode in cnodes:
            self.assertEquals(
                [o['name'] for o in direct_client.direct_get_container(
                    cnode, cpart, self.account, container1)[1]],
                ['object1'])
        headers, containers = client.get_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '1')
        self.assertEquals(headers['x-account-object-count'], '1')
        self.assertEquals(headers['x-account-bytes-used'], '3')

    def test_two_nodes_fail(self):
        # Create container1
        # Kill container1 servers excepting one of the primaries
        # Delete container1 directly to the one primary still up
        # Restart other container1 servers
        # Get to a final state
        # Assert all container1 servers indicate container1 is gone (happens
        #   because the one node that knew about the delete replicated to the
        #   others.)
        # Assert account level also indicates container1 is gone
        container1 = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container1)
        client.put_container(self.url, self.token, container1)
        cnp_port = kill_nonprimary_server(cnodes, self.port2server, self.pids)
        kill_server(cnodes[0]['port'], self.port2server, self.pids)
        kill_server(cnodes[1]['port'], self.port2server, self.pids)
        direct_client.direct_delete_container(cnodes[2], cpart, self.account,
                                              container1)
        start_server(cnodes[0]['port'], self.port2server, self.pids)
        start_server(cnodes[1]['port'], self.port2server, self.pids)
        start_server(cnp_port, self.port2server, self.pids)
        get_to_final_state()
        for cnode in cnodes:
            exc = None
            try:
                direct_client.direct_get_container(cnode, cpart, self.account,
                                                   container1)
            except client.ClientException, err:
                exc = err
            self.assertEquals(exc.http_status, 404)
        headers, containers = client.get_account(self.url, self.token)
        self.assertEquals(headers['x-account-container-count'], '0')
        self.assertEquals(headers['x-account-object-count'], '0')
        self.assertEquals(headers['x-account-bytes-used'], '0')

    def _get_container_db_files(self, container):
        opart, onodes = self.container_ring.get_nodes(self.account, container)
        onode = onodes[0]
        db_files = []
        for onode in onodes:
            node_id = (onode['port'] - 6000) / 10
            device = onode['device']
            hash_str = hash_path(self.account, container)
            server_conf = readconf('/etc/swift/container-server/%s.conf' %
                                   node_id)
            devices = server_conf['app:container-server']['devices']
            obj_dir = '%s/%s/containers/%s/%s/%s/' % (devices,
                                                      device, opart,
                                                      hash_str[-3:], hash_str)
            db_files.append(get_db_file_path(obj_dir))

        return db_files

    def test_locked_container_dbs(self):

        def run_test(num_locks, catch_503):
            container = 'container-%s' % uuid4()
            client.put_container(self.url, self.token, container)
            db_files = self._get_container_db_files(container)
            db_conns = []
            for i in range(num_locks):
                db_conn = connect(db_files[i])
                db_conn.execute('begin exclusive transaction')
                db_conns.append(db_conn)
            if catch_503:
                exc = None
                try:
                    client.delete_container(self.url, self.token, container)
                except client.ClientException, err:
                    exc = err
                self.assertEquals(exc.http_status, 503)
            else:
                client.delete_container(self.url, self.token, container)

        pool = GreenPool()
        try:
            with Timeout(15):
                pool.spawn(run_test, 1, False)
                pool.spawn(run_test, 2, True)
                pool.spawn(run_test, 3, True)
                pool.waitall()
        except Timeout, err:
            raise Exception(
                "The server did not return a 503 on container db locks, "
                "it just hangs: %s" % err)


if __name__ == '__main__':
    main()
