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

import os
import shutil

from io import StringIO
from tempfile import mkdtemp
from textwrap import dedent
from unittest import main
from uuid import uuid4

from swiftclient import client

from swift.common import direct_client, internal_client
from swift.common.manager import Manager
from test.probe.common import kill_nonprimary_server, \
    kill_server, ReplProbeTest, start_server


class TestObjectAsyncUpdate(ReplProbeTest):

    def test_main(self):
        # Create container
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)

        # Kill container servers excepting two of the primaries
        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        cnode = cnodes[0]
        kill_nonprimary_server(cnodes, self.port2server, self.pids)
        kill_server(cnode['port'], self.port2server, self.pids)

        # Create container/obj
        obj = 'object-%s' % uuid4()
        client.put_object(self.url, self.token, container, obj, '')

        # Restart other primary server
        start_server(cnode['port'], self.port2server, self.pids)

        # Assert it does not know about container/obj
        self.assert_(not direct_client.direct_get_container(
            cnode, cpart, self.account, container)[1])

        # Run the object-updaters
        Manager(['object-updater']).once()

        # Assert the other primary server now knows about container/obj
        objs = [o['name'] for o in direct_client.direct_get_container(
            cnode, cpart, self.account, container)[1]]
        self.assert_(obj in objs)


class TestUpdateOverrides(ReplProbeTest):
    """
    Use an internal client to PUT an object to proxy server,
    bypassing gatekeeper so that X-Backend- headers can be included.
    Verify that the update override headers take effect and override
    values propagate to the container server.
    """
    def setUp(self):
        """
        Reset all environment and start all servers.
        """
        super(TestUpdateOverrides, self).setUp()
        self.tempdir = mkdtemp()
        conf_path = os.path.join(self.tempdir, 'internal_client.conf')
        conf_body = """
        [DEFAULT]
        swift_dir = /etc/swift

        [pipeline:main]
        pipeline = catch_errors cache proxy-server

        [app:proxy-server]
        use = egg:swift#proxy

        [filter:cache]
        use = egg:swift#memcache

        [filter:catch_errors]
        use = egg:swift#catch_errors
        """
        with open(conf_path, 'w') as f:
            f.write(dedent(conf_body))
        self.int_client = internal_client.InternalClient(conf_path, 'test', 1)

    def tearDown(self):
        super(TestUpdateOverrides, self).tearDown()
        shutil.rmtree(self.tempdir)

    def test(self):
        headers = {
            'Content-Type': 'text/plain',
            'X-Backend-Container-Update-Override-Etag': 'override-etag',
            'X-Backend-Container-Update-Override-Content-Type': 'override-type'
        }
        client.put_container(self.url, self.token, 'c1',
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        self.int_client.upload_object(StringIO(u'stuff'), self.account,
                                      'c1', 'o1', headers)

        # Run the object-updaters to be sure updates are done
        Manager(['object-updater']).once()

        meta = self.int_client.get_object_metadata(self.account, 'c1', 'o1')

        self.assertEqual('text/plain', meta['content-type'])
        self.assertEqual('c13d88cb4cb02003daedb8a84e5d272a', meta['etag'])

        obj_iter = self.int_client.iter_objects(self.account, 'c1')
        for obj in obj_iter:
            if obj['name'] == 'o1':
                self.assertEqual('override-etag', obj['hash'])
                self.assertEqual('override-type', obj['content_type'])
                break
        else:
            self.fail('Failed to find object o1 in listing')


if __name__ == '__main__':
    main()
