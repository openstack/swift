#!/usr/bin/python -u
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

from io import BytesIO
from time import sleep
import uuid
import unittest

from swiftclient import client

from swift.account import reaper
from swift.common import utils
from swift.common.manager import Manager
from swift.common.direct_client import direct_delete_account, \
    direct_get_object, direct_head_container, ClientException
from swift.common.request_helpers import get_reserved_name
from test.probe.common import ReplProbeTest, ENABLED_POLICIES


class TestAccountReaper(ReplProbeTest):
    def setUp(self):
        super(TestAccountReaper, self).setUp()
        self.all_objects = []
        int_client = self.make_internal_client()
        # upload some containers
        body = b'test-body'
        for policy in ENABLED_POLICIES:
            container = 'container-%s-%s' % (policy.name, uuid.uuid4())
            client.put_container(self.url, self.token, container,
                                 headers={'X-Storage-Policy': policy.name})
            obj = 'object-%s' % uuid.uuid4()
            client.put_object(self.url, self.token, container, obj, body)
            self.all_objects.append((policy, container, obj))

            # Also create some reserved names
            container = get_reserved_name(
                'reserved', policy.name, str(uuid.uuid4()))
            int_client.create_container(
                self.account, container,
                headers={'X-Storage-Policy': policy.name})
            obj = get_reserved_name('object', str(uuid.uuid4()))
            int_client.upload_object(
                BytesIO(body), self.account, container, obj)
            self.all_objects.append((policy, container, obj))

            policy.load_ring('/etc/swift')

        Manager(['container-updater']).once()

        headers = client.head_account(self.url, self.token)

        self.assertEqual(int(headers['x-account-container-count']),
                         len(self.all_objects))
        self.assertEqual(int(headers['x-account-object-count']),
                         len(self.all_objects))
        self.assertEqual(int(headers['x-account-bytes-used']),
                         len(self.all_objects) * len(body))

        part, nodes = self.account_ring.get_nodes(self.account)

        for node in nodes:
            direct_delete_account(node, part, self.account)

    def _verify_account_reaped(self):
        for policy, container, obj in self.all_objects:
            # verify that any container deletes were at same timestamp
            cpart, cnodes = self.container_ring.get_nodes(
                self.account, container)
            delete_times = set()
            for cnode in cnodes:
                try:
                    direct_head_container(cnode, cpart, self.account,
                                          container)
                except ClientException as err:
                    self.assertEqual(err.http_status, 404)
                    delete_time = err.http_headers.get(
                        'X-Backend-DELETE-Timestamp')
                    # 'X-Backend-DELETE-Timestamp' confirms it was deleted
                    self.assertTrue(delete_time)
                    delete_times.add(delete_time)
                else:
                    # Container replicas may not yet be deleted if we have a
                    # policy with object replicas < container replicas, so
                    # ignore successful HEAD. We'll check for all replicas to
                    # be deleted again after running the replicators.
                    pass
            self.assertEqual(1, len(delete_times), delete_times)

            # verify that all object deletes were at same timestamp
            part, nodes = policy.object_ring.get_nodes(self.account,
                                                       container, obj)
            headers = {'X-Backend-Storage-Policy-Index': int(policy)}
            delete_times = set()
            for node in nodes:
                try:
                    direct_get_object(node, part, self.account,
                                      container, obj, headers=headers)
                except ClientException as err:
                    self.assertEqual(err.http_status, 404)
                    delete_time = err.http_headers.get('X-Backend-Timestamp')
                    # 'X-Backend-Timestamp' confirms obj was deleted
                    self.assertTrue(delete_time)
                    delete_times.add(delete_time)
                else:
                    self.fail('Found un-reaped /%s/%s/%s on %r in %s!' %
                              (self.account, container, obj, node, policy))
            self.assertEqual(1, len(delete_times))

        # run replicators and updaters
        self.get_to_final_state()

        for policy, container, obj in self.all_objects:
            # verify that ALL container replicas are now deleted
            cpart, cnodes = self.container_ring.get_nodes(
                self.account, container)
            delete_times = set()
            for cnode in cnodes:
                try:
                    direct_head_container(cnode, cpart, self.account,
                                          container)
                except ClientException as err:
                    self.assertEqual(err.http_status, 404)
                    delete_time = err.http_headers.get(
                        'X-Backend-DELETE-Timestamp')
                    # 'X-Backend-DELETE-Timestamp' confirms it was deleted
                    self.assertTrue(delete_time)
                    delete_times.add(delete_time)
                else:
                    self.fail('Found un-reaped /%s/%s on %r' %
                              (self.account, container, cnode))
            self.assertEqual(1, len(delete_times))

            # sanity check that object state is still consistent...
            part, nodes = policy.object_ring.get_nodes(self.account,
                                                       container, obj)
            headers = {'X-Backend-Storage-Policy-Index': int(policy)}
            delete_times = set()
            for node in nodes:
                try:
                    direct_get_object(node, part, self.account,
                                      container, obj, headers=headers)
                except ClientException as err:
                    self.assertEqual(err.http_status, 404)
                    delete_time = err.http_headers.get('X-Backend-Timestamp')
                    # 'X-Backend-Timestamp' confirms obj was deleted
                    self.assertTrue(delete_time)
                    delete_times.add(delete_time)
                else:
                    self.fail('Found un-reaped /%s/%s/%s on %r in %s!' %
                              (self.account, container, obj, node, policy))
            self.assertEqual(1, len(delete_times))

    def test_reap(self):
        # run the reaper
        Manager(['account-reaper']).once()

        self._verify_account_reaped()

    def test_delayed_reap(self):
        # define reapers which are supposed to operate 3 seconds later
        account_reapers = []
        for conf_file in self.configs['account-server'].values():
            conf = utils.readconf(conf_file, 'account-reaper')
            conf['delay_reaping'] = '3'
            account_reapers.append(reaper.AccountReaper(conf))

        self.assertTrue(account_reapers)

        # run reaper, and make sure that nothing is reaped
        for account_reaper in account_reapers:
            account_reaper.run_once()

        for policy, container, obj in self.all_objects:
            cpart, cnodes = self.container_ring.get_nodes(
                self.account, container)
            for cnode in cnodes:
                try:
                    direct_head_container(cnode, cpart, self.account,
                                          container)
                except ClientException:
                    self.fail(
                        "Nothing should be reaped. Container should exist")

            part, nodes = policy.object_ring.get_nodes(self.account,
                                                       container, obj)
            headers = {'X-Backend-Storage-Policy-Index': int(policy)}
            for node in nodes:
                try:
                    direct_get_object(node, part, self.account,
                                      container, obj, headers=headers)
                except ClientException:
                    self.fail("Nothing should be reaped. Object should exist")

        # wait 3 seconds, run reaper, and make sure that all is reaped
        sleep(3)
        for account_reaper in account_reapers:
            account_reaper.run_once()

        self._verify_account_reaped()


if __name__ == "__main__":
    unittest.main()
