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

from os import listdir
from os.path import join as path_join
from unittest import main
from uuid import uuid4

from eventlet import GreenPool, Timeout
import eventlet
from sqlite3 import connect
from swiftclient import client

from swift.common import direct_client
from swift.common.exceptions import ClientException
from swift.common.utils import hash_path, readconf
from test.probe.common import kill_nonprimary_server, \
    kill_server, ReplProbeTest, start_server

eventlet.monkey_patch(all=False, socket=True)


def get_db_file_path(obj_dir):
    files = sorted(listdir(obj_dir), reverse=True)
    for filename in files:
        if filename.endswith('db'):
            return path_join(obj_dir, filename)


class TestContainerFailures(ReplProbeTest):

    def test_one_node_fails(self):
        # Create container1
        container1 = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container1)
        client.put_container(self.url, self.token, container1)

        # Kill container1 servers excepting two of the primaries
        kill_nonprimary_server(cnodes, self.ipport2server)
        kill_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)

        # Delete container1
        client.delete_container(self.url, self.token, container1)

        # Restart other container1 primary server
        start_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)

        # Create container1/object1 (allowed because at least server thinks the
        #   container exists)
        client.put_object(self.url, self.token, container1, 'object1', '123')

        # Get to a final state
        self.get_to_final_state()

        # Assert all container1 servers indicate container1 is alive and
        #   well with object1
        for cnode in cnodes:
            self.assertEqual(
                [o['name'] for o in direct_client.direct_get_container(
                    cnode, cpart, self.account, container1)[1]],
                ['object1'])

        # Assert account level also indicates container1 is alive and
        #   well with object1
        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(headers['x-account-container-count'], '1')
        self.assertEqual(headers['x-account-object-count'], '1')
        self.assertEqual(headers['x-account-bytes-used'], '3')

    def test_two_nodes_fail(self):
        # Create container1
        container1 = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container1)
        client.put_container(self.url, self.token, container1)

        # Kill container1 servers excepting one of the primaries
        cnp_ipport = kill_nonprimary_server(cnodes, self.ipport2server)
        kill_server((cnodes[0]['ip'], cnodes[0]['port']),
                    self.ipport2server)
        kill_server((cnodes[1]['ip'], cnodes[1]['port']),
                    self.ipport2server)

        # Delete container1 directly to the one primary still up
        direct_client.direct_delete_container(cnodes[2], cpart, self.account,
                                              container1)

        # Restart other container1 servers
        start_server((cnodes[0]['ip'], cnodes[0]['port']),
                     self.ipport2server)
        start_server((cnodes[1]['ip'], cnodes[1]['port']),
                     self.ipport2server)
        start_server(cnp_ipport, self.ipport2server)

        # Get to a final state
        self.get_to_final_state()

        # Assert all container1 servers indicate container1 is gone (happens
        #   because the one node that knew about the delete replicated to the
        #   others.)
        for cnode in cnodes:
            try:
                direct_client.direct_get_container(cnode, cpart, self.account,
                                                   container1)
            except ClientException as err:
                self.assertEqual(err.http_status, 404)
            else:
                self.fail("Expected ClientException but didn't get it")

        # Assert account level also indicates container1 is gone
        headers, containers = client.get_account(self.url, self.token)
        self.assertEqual(headers['x-account-container-count'], '0')
        self.assertEqual(headers['x-account-object-count'], '0')
        self.assertEqual(headers['x-account-bytes-used'], '0')

    def _get_container_db_files(self, container):
        opart, onodes = self.container_ring.get_nodes(self.account, container)
        onode = onodes[0]
        db_files = []
        for onode in onodes:
            node_id = (onode['port'] - 6000) / 10
            device = onode['device']
            hash_str = hash_path(self.account, container)
            server_conf = readconf(self.configs['container-server'][node_id])
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
                try:
                    client.delete_container(self.url, self.token, container)
                except client.ClientException as err:
                    self.assertEqual(err.http_status, 503)
                else:
                    self.fail("Expected ClientException but didn't get it")
            else:
                client.delete_container(self.url, self.token, container)

        pool = GreenPool()
        try:
            with Timeout(15):
                pool.spawn(run_test, 1, False)
                pool.spawn(run_test, 2, True)
                pool.spawn(run_test, 3, True)
                pool.waitall()
        except Timeout as err:
            raise Exception(
                "The server did not return a 503 on container db locks, "
                "it just hangs: %s" % err)


if __name__ == '__main__':
    main()
