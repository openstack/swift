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
from unittest import main, SkipTest
from uuid import uuid4

from swiftclient import client
from swiftclient.exceptions import ClientException

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
        self.assertIn(obj, objs)

    def test_missing_container(self):
        # In this test, we need to put container at handoff devices, so we
        # need container devices more than replica count
        if len(self.container_ring.devs) <= self.container_ring.replica_count:
            raise SkipTest("Need devices more that replica count")

        container = 'container-%s' % uuid4()
        cpart, cnodes = self.container_ring.get_nodes(self.account, container)

        # Kill all primary container servers
        for cnode in cnodes:
            kill_server((cnode['ip'], cnode['port']), self.ipport2server)

        # Create container, and all of its replicas are placed at handoff
        # device
        try:
            client.put_container(self.url, self.token, container)
        except ClientException as err:
            # if the cluster doesn't have enough devices, swift may return
            # error (ex. When we only have 4 devices in 3-replica cluster).
            self.assertEqual(err.http_status, 503)

        # Assert handoff device has a container replica
        another_cnode = self.container_ring.get_more_nodes(cpart).next()
        direct_client.direct_get_container(
            another_cnode, cpart, self.account, container)

        # Restart all primary container servers
        for cnode in cnodes:
            start_server((cnode['ip'], cnode['port']), self.ipport2server)

        # Create container/obj
        obj = 'object-%s' % uuid4()
        client.put_object(self.url, self.token, container, obj, '')

        # Run the object-updater
        Manager(['object-updater']).once()

        # Run the container-replicator, and now, container replicas
        # at handoff device get moved to primary servers
        Manager(['container-replicator']).once()

        # Assert container replicas in primary servers, just moved by
        # replicator don't know about the object
        for cnode in cnodes:
            self.assertFalse(direct_client.direct_get_container(
                cnode, cpart, self.account, container)[1])

        # since the container is empty - we can delete it!
        client.delete_container(self.url, self.token, container)

        # Re-run the object-updaters and now container replicas in primary
        # container servers should get updated
        Manager(['object-updater']).once()

        # Assert all primary container servers know about container/obj
        for cnode in cnodes:
            objs = [o['name'] for o in direct_client.direct_get_container(
                    cnode, cpart, self.account, container)[1]]
            self.assertIn(obj, objs)


class TestUpdateOverrides(ReplProbeTest):
    """
    Use an internal client to PUT an object to proxy server,
    bypassing gatekeeper so that X-Object-Sysmeta- headers can be included.
    Verify that the update override headers take effect and override
    values propagate to the container server.
    """
    def test_update_during_PUT(self):
        # verify that update sent during a PUT has override values
        int_client = self.make_internal_client()
        headers = {
            'Content-Type': 'text/plain',
            'X-Object-Sysmeta-Container-Update-Override-Etag': 'override-etag',
            'X-Object-Sysmeta-Container-Update-Override-Content-Type':
                'override-type',
            'X-Object-Sysmeta-Container-Update-Override-Size': '1999'
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
        client.put_object(self.url, self.token, 'c1', 'o1', contents=content,
                          content_type='test/ctype')
        meta = client.head_object(self.url, self.token, 'c1', 'o1')

        # re-start the container server and assert that it does not yet know
        # about the object
        start_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        self.assertFalse(direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1])

        # Run the object-updaters to be sure updates are done
        Manager(['object-updater']).once()

        # check the re-started container server got same update as others.
        # we cannot assert the actual etag value because it may be encrypted
        listing_etags = set()
        for cnode in cnodes:
            listing = direct_client.direct_get_container(
                cnode, cpart, self.account, 'c1')[1]
            self.assertEqual(1, len(listing))
            self.assertEqual(len(content), listing[0]['bytes'])
            self.assertEqual('test/ctype', listing[0]['content_type'])
            listing_etags.add(listing[0]['hash'])
        self.assertEqual(1, len(listing_etags))

        # check that listing meta returned to client is consistent with object
        # meta returned to client
        hdrs, listing = client.get_container(self.url, self.token, 'c1')
        self.assertEqual(1, len(listing))
        self.assertEqual('o1', listing[0]['name'])
        self.assertEqual(len(content), listing[0]['bytes'])
        self.assertEqual(meta['etag'], listing[0]['hash'])
        self.assertEqual('test/ctype', listing[0]['content_type'])

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
        client.put_object(self.url, self.token, 'c1', 'o1', contents=content,
                          content_type='test/ctype')
        meta = client.head_object(self.url, self.token, 'c1', 'o1')

        # re-start the container server and assert that it does not yet know
        # about the object
        start_server((cnodes[0]['ip'], cnodes[0]['port']), self.ipport2server)
        self.assertFalse(direct_client.direct_get_container(
            cnodes[0], cpart, self.account, 'c1')[1])

        int_client = self.make_internal_client()
        int_client.set_object_metadata(
            self.account, 'c1', 'o1', {'X-Object-Meta-Fruit': 'Tomato'})
        self.assertEqual(
            'Tomato',
            int_client.get_object_metadata(self.account, 'c1', 'o1')
            ['x-object-meta-fruit'])  # sanity

        # check the re-started container server got same update as others.
        # we cannot assert the actual etag value because it may be encrypted
        listing_etags = set()
        for cnode in cnodes:
            listing = direct_client.direct_get_container(
                cnode, cpart, self.account, 'c1')[1]
            self.assertEqual(1, len(listing))
            self.assertEqual(len(content), listing[0]['bytes'])
            self.assertEqual('test/ctype', listing[0]['content_type'])
            listing_etags.add(listing[0]['hash'])
        self.assertEqual(1, len(listing_etags))

        # check that listing meta returned to client is consistent with object
        # meta returned to client
        hdrs, listing = client.get_container(self.url, self.token, 'c1')
        self.assertEqual(1, len(listing))
        self.assertEqual('o1', listing[0]['name'])
        self.assertEqual(len(content), listing[0]['bytes'])
        self.assertEqual(meta['etag'], listing[0]['hash'])
        self.assertEqual('test/ctype', listing[0]['content_type'])

        # Run the object-updaters to send the async pending from the PUT
        Manager(['object-updater']).once()

        # check container listing metadata is still correct
        for cnode in cnodes:
            listing = direct_client.direct_get_container(
                cnode, cpart, self.account, 'c1')[1]
            self.assertEqual(1, len(listing))
            self.assertEqual(len(content), listing[0]['bytes'])
            self.assertEqual('test/ctype', listing[0]['content_type'])
            listing_etags.add(listing[0]['hash'])
        self.assertEqual(1, len(listing_etags))

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
        client.put_object(self.url, self.token, 'c1', 'o1', contents=content,
                          content_type='test/ctype')
        meta = client.head_object(self.url, self.token, 'c1', 'o1')

        int_client = self.make_internal_client()
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

        # check the re-started container server got same update as others.
        # we cannot assert the actual etag value because it may be encrypted
        listing_etags = set()
        for cnode in cnodes:
            listing = direct_client.direct_get_container(
                cnode, cpart, self.account, 'c1')[1]
            self.assertEqual(1, len(listing))
            self.assertEqual(len(content), listing[0]['bytes'])
            self.assertEqual('test/ctype', listing[0]['content_type'])
            listing_etags.add(listing[0]['hash'])
        self.assertEqual(1, len(listing_etags))

        # check that listing meta returned to client is consistent with object
        # meta returned to client
        hdrs, listing = client.get_container(self.url, self.token, 'c1')
        self.assertEqual(1, len(listing))
        self.assertEqual('o1', listing[0]['name'])
        self.assertEqual(len(content), listing[0]['bytes'])
        self.assertEqual(meta['etag'], listing[0]['hash'])
        self.assertEqual('test/ctype', listing[0]['content_type'])


if __name__ == '__main__':
    main()
