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

import uuid
import unittest

from swiftclient import client

from swift.common.storage_policy import POLICIES
from swift.common.manager import Manager
from swift.common.direct_client import direct_delete_account, \
    direct_get_object, direct_head_container, ClientException
from test.probe.common import ReplProbeTest, ENABLED_POLICIES


class TestAccountReaper(ReplProbeTest):

    def test_sync(self):
        all_objects = []
        # upload some containers
        for policy in ENABLED_POLICIES:
            container = 'container-%s-%s' % (policy.name, uuid.uuid4())
            client.put_container(self.url, self.token, container,
                                 headers={'X-Storage-Policy': policy.name})
            obj = 'object-%s' % uuid.uuid4()
            body = 'test-body'
            client.put_object(self.url, self.token, container, obj, body)
            all_objects.append((policy, container, obj))

        Manager(['container-updater']).once()

        headers = client.head_account(self.url, self.token)

        self.assertEqual(int(headers['x-account-container-count']),
                         len(ENABLED_POLICIES))
        self.assertEqual(int(headers['x-account-object-count']),
                         len(ENABLED_POLICIES))
        self.assertEqual(int(headers['x-account-bytes-used']),
                         len(ENABLED_POLICIES) * len(body))

        part, nodes = self.account_ring.get_nodes(self.account)
        for node in nodes:
            direct_delete_account(node, part, self.account)

        Manager(['account-reaper']).once()

        self.get_to_final_state()

        for policy, container, obj in all_objects:
            cpart, cnodes = self.container_ring.get_nodes(
                self.account, container)
            for cnode in cnodes:
                try:
                    direct_head_container(cnode, cpart, self.account,
                                          container)
                except ClientException as err:
                    self.assertEquals(err.http_status, 404)
                else:
                    self.fail('Found un-reaped /%s/%s on %r' %
                              (self.account, container, node))
            object_ring = POLICIES.get_object_ring(policy.idx, '/etc/swift/')
            part, nodes = object_ring.get_nodes(self.account, container, obj)
            for node in nodes:
                try:
                    direct_get_object(node, part, self.account,
                                      container, obj)
                except ClientException as err:
                    self.assertEquals(err.http_status, 404)
                else:
                    self.fail('Found un-reaped /%s/%s/%s on %r in %s!' %
                              (self.account, container, obj, node, policy))


if __name__ == "__main__":
    unittest.main()
