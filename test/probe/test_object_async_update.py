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

from io import StringIO
from unittest import main
from uuid import uuid4

from swiftclient import client

from swift.common import direct_client
from swift.common.manager import Manager
from test.probe.common import kill_nonprimary_server, \
    kill_server, ReplProbeTest, start_server, ECProbeTest


class TestObjectAsyncUpdate(ReplProbeTest):

    def test_main(self):
        # Create container
        container = 'container-%s' % uuid4()
        client.put_container(self.url, self.token, container)

        # Kill container servers excepting two of the primaries
        cpart, cnodes = self.container_ring.get_nodes(self.account, container)
        cnode = cnodes[0]
        kill_nonprimary_server(cnodes, self.ipport2server)
        kill_server((cnode['ip'], cnode['port']), self.ipport2server)

        # Create container/obj
        obj = 'object-%s' % uuid4()
        client.put_object(self.url, self.token, container, obj, '')

        # Restart other primary server
        start_server((cnode['ip'], cnode['port']), self.ipport2server)

        # Assert it does not know about container/obj
        self.assertFalse(direct_client.direct_get_container(
            cnode, cpart, self.account, container)[1])

        # Run the object-updaters
        Manager(['object-updater']).once()

        # Assert the other primary server now knows about container/obj
        objs = [o['name'] for o in direct_client.direct_get_container(
            cnode, cpart, self.account, container)[1]]
        self.assertTrue(obj in objs)


class TestUpdateOverrides(ReplProbeTest):
    """
    Use an internal client to PUT an object to proxy server,
    bypassing gatekeeper so that X-Backend- headers can be included.
    Verify that the update override headers take effect and override
    values propagate to the container server.
    """
    def test_update_during_PUT(self):
        # verify that update sent during a PUT has override values
        int_client = self.make_internal_client()
        headers = {
            'Content-Type': 'text/plain',
            'X-Backend-Container-Update-Override-Etag': 'override-etag',
            'X-Backend-Container-Update-Override-Content-Type':
                'override-type',
            'X-Backend-Container-Update-Override-Size': '1999'
        }
        client.put_container(self.url, self.token, 'c1',
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        int_client.upload_object(
            StringIO(u'stuff'), self.account, 'c1', 'o1', headers)

        # Run the object-updaters to be sure updates are done
        Manager(['object-updater']).once()

        meta = int_client.get_object_metadata(self.account, 'c1', 'o1')

        self.assertEqual('text/plain', meta['content-type'])
        self.assertEqual('c13d88cb4cb02003daedb8a84e5d272a', meta['etag'])
        self.assertEqual('5', meta['content-length'])

        obj_iter = int_client.iter_objects(self.account, 'c1')
        for obj in obj_iter:
            if obj['name'] == 'o1':
                self.assertEqual('override-etag', obj['hash'])
                self.assertEqual('override-type', obj['content_type'])
                self.assertEqual(1999, obj['bytes'])
                break
        else:
            self.fail('Failed to find object o1 in listing')


class TestUpdateOverridesEC(ECProbeTest):
    # verify that the container update overrides used with EC policies make
    # it to the container servers when container updates are sync or async
    # and possibly re-ordered with respect to object PUT and POST requests.
    def test_async_update_after_PUT(self):
        cpart, cnodes = self.container_ring.get_nodes(self.account, 'c1')
        client.put_container(self.url, self.token, 'c1',
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        # put an object while one container server is stopped so that we force
        # an async update to it
        kill_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        content = u'stuff'
        client.put_object(self.url, self.token, 'c1', 'o1', contents=content)
        meta = client.head_object(self.url, self.token, 'c1', 'o1')

        # re-start the container server and assert that it does not yet know
        # about the object
        start_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        self.assertFalse(direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1])

        # Run the object-updaters to be sure updates are done
        Manager(['object-updater']).once()

        # check the re-started container server has update with override values
        obj = direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1][0]
        self.assertEqual(meta['etag'], obj['hash'])
        self.assertEqual(len(content), obj['bytes'])

    def test_update_during_POST_only(self):
        # verify correct update values when PUT update is missed but then a
        # POST update succeeds *before* the PUT async pending update is sent
        cpart, cnodes = self.container_ring.get_nodes(self.account, 'c1')
        client.put_container(self.url, self.token, 'c1',
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        # put an object while one container server is stopped so that we force
        # an async update to it
        kill_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        content = u'stuff'
        client.put_object(self.url, self.token, 'c1', 'o1', contents=content)
        meta = client.head_object(self.url, self.token, 'c1', 'o1')

        # re-start the container server and assert that it does not yet know
        # about the object
        start_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        self.assertFalse(direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1])

        # use internal client for POST so we can force fast-post mode
        int_client = self.make_internal_client(object_post_as_copy=False)
        int_client.set_object_metadata(
            self.account, 'c1', 'o1', {'X-Object-Meta-Fruit': 'Tomato'})
        self.assertEqual(
            'Tomato',
            int_client.get_object_metadata(self.account, 'c1', 'o1')
            ['x-object-meta-fruit'])  # sanity

        # check the re-started container server has update with override values
        obj = direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1][0]
        self.assertEqual(meta['etag'], obj['hash'])
        self.assertEqual(len(content), obj['bytes'])

        # Run the object-updaters to send the async pending from the PUT
        Manager(['object-updater']).once()

        # check container listing metadata is still correct
        obj = direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1][0]
        self.assertEqual(meta['etag'], obj['hash'])
        self.assertEqual(len(content), obj['bytes'])

    def test_async_updates_after_PUT_and_POST(self):
        # verify correct update values when PUT update and POST updates are
        # missed but then async updates are sent
        cpart, cnodes = self.container_ring.get_nodes(self.account, 'c1')
        client.put_container(self.url, self.token, 'c1',
                             headers={'X-Storage-Policy':
                                      self.policy.name})

        # PUT and POST to object while one container server is stopped so that
        # we force async updates to it
        kill_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        content = u'stuff'
        client.put_object(self.url, self.token, 'c1', 'o1', contents=content)
        meta = client.head_object(self.url, self.token, 'c1', 'o1')

        # use internal client for POST so we can force fast-post mode
        int_client = self.make_internal_client(object_post_as_copy=False)
        int_client.set_object_metadata(
            self.account, 'c1', 'o1', {'X-Object-Meta-Fruit': 'Tomato'})
        self.assertEqual(
            'Tomato',
            int_client.get_object_metadata(self.account, 'c1', 'o1')
            ['x-object-meta-fruit'])  # sanity

        # re-start the container server and assert that it does not yet know
        # about the object
        start_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        self.assertFalse(direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1])

        # Run the object-updaters to send the async pendings
        Manager(['object-updater']).once()

        # check container listing metadata is still correct
        obj = direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1][0]
        self.assertEqual(meta['etag'], obj['hash'])
        self.assertEqual(len(content), obj['bytes'])


if __name__ == '__main__':
    main()
