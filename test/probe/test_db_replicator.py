# Copyright (c) 2010-2016 OpenStack Foundation
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

from unittest import main
from uuid import uuid4

from swiftclient import client, ClientException

from test.probe.common import kill_server, ReplProbeTest, start_server
from swift.common import direct_client, utils
from swift.common.manager import Manager


class TestDbUsyncReplicator(ReplProbeTest):
    object_puts = 1  # Overridden in subclass to force rsync

    def test_metadata_sync(self):
        # Create container
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container,
                             headers={'X-Storage-Policy': self.policy.name,
                                      'X-Container-Meta-A': '1',
                                      'X-Container-Meta-B': '1',
                                      'X-Container-Meta-C': '1'})

        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        cnode = cnodes.pop()
        # 2 of 3 container servers are temporarily down
        for node in cnodes:
            kill_server((node['ip'], node['port']),
                        self.ipport2server)

        # Put some meta on the lone server, to make sure it's merged properly
        # This will 503 (since we don't have a quorum), but we don't care (!)
        try:
            client.post_container(self.url, self.token, container,
                                  headers={'X-Container-Meta-A': '2',
                                           'X-Container-Meta-B': '2',
                                           'X-Container-Meta-D': '2'})
        except ClientException:
            pass

        # object updates come to only one container server
        for _ in range(self.object_puts):
            obj = 'object-%s' % uuid4()
            client.put_object(self.url, self.token, container, obj, 'VERIFY')

        # 2 container servers make comeback
        for node in cnodes:
            start_server((node['ip'], node['port']),
                         self.ipport2server)
        # But, container-server which got object updates is down
        kill_server((cnode['ip'], cnode['port']),
                    self.ipport2server)

        # Metadata update will be applied to 2 container servers
        # (equal to quorum)
        client.post_container(self.url, self.token, container,
                              headers={'X-Container-Meta-B': '3',
                                       'X-Container-Meta-E': '3'})
        # container-server which got object updates makes comeback
        start_server((cnode['ip'], cnode['port']),
                     self.ipport2server)

        # other nodes have no objects
        for node in cnodes:
            resp_headers = direct_client.direct_head_container(
                node, cpart, self.account, container)
            self.assertIn(resp_headers.get('x-container-object-count'),
                          (None, '0', 0))

        # If container-replicator on the node which got the object updates
        # runs in first, db file may be replicated by rsync to other
        # containers. In that case, the db file does not information about
        # metadata, so metadata should be synced before replication
        Manager(['container-replicator']).once(
            number=self.config_number(cnode))

        expected_meta = {
            'x-container-meta-a': '2',
            'x-container-meta-b': '3',
            'x-container-meta-c': '1',
            'x-container-meta-d': '2',
            'x-container-meta-e': '3',
        }

        # node that got the object updates now has the meta
        resp_headers = direct_client.direct_head_container(
            cnode, cpart, self.account, container)
        for header, value in expected_meta.items():
            self.assertIn(header, resp_headers)
            self.assertEqual(value, resp_headers[header])
        self.assertNotIn(resp_headers.get('x-container-object-count'),
                         (None, '0', 0))

        # other nodes still have the meta, as well as objects
        for node in cnodes:
            resp_headers = direct_client.direct_head_container(
                node, cpart, self.account, container)
            for header, value in expected_meta.items():
                self.assertIn(header, resp_headers)
                self.assertEqual(value, resp_headers[header])
            self.assertNotIn(resp_headers.get('x-container-object-count'),
                             (None, '0', 0))

        # and after full pass on remaining nodes
        for node in cnodes:
            Manager(['container-replicator']).once(
                number=self.config_number(node))

        # ... all is right
        for node in cnodes + [cnode]:
            resp_headers = direct_client.direct_head_container(
                node, cpart, self.account, container)
            for header, value in expected_meta.items():
                self.assertIn(header, resp_headers)
                self.assertEqual(value, resp_headers[header])
            self.assertNotIn(resp_headers.get('x-container-object-count'),
                             (None, '0', 0))


class TestDbRsyncReplicator(TestDbUsyncReplicator):
    def setUp(self):
        super(TestDbRsyncReplicator, self).setUp()
        cont_configs = [utils.readconf(p, 'container-replicator')
                        for p in self.configs['container-replicator'].values()]
        # Do more than per_diff object PUTs, to force rsync instead of usync
        self.object_puts = 1 + max(int(c.get('per_diff', '1000'))
                                   for c in cont_configs)


if __name__ == '__main__':
    main()
