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
import time
from unittest import main
from uuid import uuid4

from eventlet import GreenPool, Timeout
import eventlet
from sqlite3 import connect

from swift.common.manager import Manager
from swiftclient import client

from swift.common import direct_client
from swift.common.exceptions import ClientException
from swift.common.utils import readconf
from test.probe.common import kill_nonprimary_server, \
    kill_server, ReplProbeTest, start_server

eventlet.monkey_patch(all=False, socket=True)


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

    def test_metadata_replicated_with_no_timestamp_update(self):
        self.maxDiff = None
        # Create container1
        container1 = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container1)
        client.put_container(self.url, self.token, container1)
        Manager(['container-replicator']).once()

        exp_hdrs = None
        for cnode in cnodes:
            hdrs = direct_client.direct_head_container(
                cnode, cpart, self.account, container1)
            hdrs.pop('Date')
            if exp_hdrs:
                self.assertEqual(exp_hdrs, hdrs)
            exp_hdrs = hdrs
        self.assertIsNotNone(exp_hdrs)
        self.assertIn('Last-Modified', exp_hdrs)
        put_time = float(exp_hdrs['X-Backend-Put-Timestamp'])

        # Post to only one replica of container1 at least 1 second after the
        # put (to reveal any unexpected change in Last-Modified which is
        # rounded to seconds)
        time.sleep(put_time + 1 - time.time())
        post_hdrs = {'x-container-meta-foo': 'bar',
                     'x-backend-no-timestamp-update': 'true'}
        direct_client.direct_post_container(
            cnodes[1], cpart, self.account, container1, headers=post_hdrs)

        # verify that put_timestamp was not modified
        exp_hdrs.update({'x-container-meta-foo': 'bar'})
        hdrs = direct_client.direct_head_container(
            cnodes[1], cpart, self.account, container1)
        hdrs.pop('Date')
        self.assertDictEqual(exp_hdrs, hdrs)

        # Get to a final state
        Manager(['container-replicator']).once()

        # Assert all container1 servers have consistent metadata
        for cnode in cnodes:
            hdrs = direct_client.direct_head_container(
                cnode, cpart, self.account, container1)
            hdrs.pop('Date')
            self.assertDictEqual(exp_hdrs, hdrs)

        # sanity check: verify the put_timestamp is modified without
        # x-backend-no-timestamp-update
        post_hdrs = {'x-container-meta-foo': 'baz'}
        exp_hdrs.update({'x-container-meta-foo': 'baz'})
        direct_client.direct_post_container(
            cnodes[1], cpart, self.account, container1, headers=post_hdrs)

        # verify that put_timestamp was modified
        hdrs = direct_client.direct_head_container(
            cnodes[1], cpart, self.account, container1)
        self.assertLess(exp_hdrs['x-backend-put-timestamp'],
                        hdrs['x-backend-put-timestamp'])
        self.assertNotEqual(exp_hdrs['last-modified'], hdrs['last-modified'])
        hdrs.pop('Date')
        for key in ('x-backend-put-timestamp',
                    'x-put-timestamp',
                    'last-modified'):
            self.assertNotEqual(exp_hdrs[key], hdrs[key])
            exp_hdrs.pop(key)
            hdrs.pop(key)

        self.assertDictEqual(exp_hdrs, hdrs)

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

    def test_all_nodes_fail(self):
        # Create container1
        container1 = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container1)
        client.put_container(self.url, self.token, container1)
        client.put_object(self.url, self.token, container1, 'obj1', 'data1')

        # All primaries go down
        for cnode in cnodes:
            kill_server((cnode['ip'], cnode['port']), self.ipport2server)

        # Can't GET the container
        with self.assertRaises(client.ClientException) as caught:
            client.get_container(self.url, self.token, container1)
        self.assertEqual(caught.exception.http_status, 503)

        # But we can still write objects! The old info is still in memcache
        client.put_object(self.url, self.token, container1, 'obj2', 'data2')

        # Can't POST the container, either
        with self.assertRaises(client.ClientException) as caught:
            client.post_container(self.url, self.token, container1, {})
        self.assertEqual(caught.exception.http_status, 503)

        # Though it *does* evict the cache
        with self.assertRaises(client.ClientException) as caught:
            client.put_object(self.url, self.token, container1, 'obj3', 'x')
        self.assertEqual(caught.exception.http_status, 503)

    def test_locked_container_dbs(self):

        def run_test(num_locks, catch_503):
            container = 'container-%s' % uuid4()
            client.put_container(self.url, self.token, container)
            # Get the container info into memcache (so no stray
            # get_container_info calls muck up our timings)
            client.get_container(self.url, self.token, container)
            db_files = self.get_container_db_files(container)
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

        proxy_conf = readconf(self.configs['proxy-server'],
                              section_name='app:proxy-server')
        node_timeout = int(proxy_conf.get('node_timeout', 10))
        pool = GreenPool()
        try:
            with Timeout(node_timeout + 5):
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
