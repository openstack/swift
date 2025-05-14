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
import time
import uuid

from datetime import datetime
from io import BytesIO
from unittest import main, SkipTest
from uuid import uuid4
import random

from unittest import mock

from swiftclient import client
from swiftclient.exceptions import ClientException

from configparser import ConfigParser
from swift.common import direct_client, manager
from swift.common.manager import Manager, Server
from swift.common.swob import normalize_etag
from swift.common.utils import readconf
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
        another_cnode = next(self.container_ring.get_more_nodes(cpart))
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
            BytesIO(b'stuff'), self.account, 'c1', 'o1', headers)

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
        self.assertEqual(normalize_etag(meta['etag']), listing[0]['hash'])
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
        self.assertEqual(normalize_etag(meta['etag']), listing[0]['hash'])
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
        self.assertEqual(normalize_etag(meta['etag']), listing[0]['hash'])
        self.assertEqual('test/ctype', listing[0]['content_type'])


class UpdaterStatsMixIn(object):

    def setUp(self):
        super(UpdaterStatsMixIn, self).setUp()
        self.int_client = self.make_internal_client()
        self.container_servers = Manager(['container-server'])
        self.object_updater = Manager(['object-updater'])
        self._post_setup_config()

    def _post_setup_config(self):
        pass

    def _create_lots_of_asyncs(self):
        # Create some (acct, cont) pairs
        num_accounts = 3
        num_conts_per_a = 4
        self.ac_pairs = ac_pairs = []
        for a in range(num_accounts):
            acct = 'AUTH_user-%s-%03d' % (uuid.uuid4(), a)
            self.int_client.create_account(acct)
            for c in range(num_conts_per_a):
                cont = 'cont-%s-%03d' % (uuid.uuid4(), c)
                self.int_client.create_container(acct, cont)
                ac_pairs.append((acct, cont))

        # Shut down a couple container servers
        for n in random.sample([1, 2, 3, 4], 2):
            self.container_servers.stop(number=n)

        # Create a bunch of objects
        num_objs_per_ac = 10
        for acct, cont in ac_pairs:
            for o in range(num_objs_per_ac):
                obj = 'obj-%s-%03d' % (uuid.uuid4(), o)
                self.int_client.upload_object(BytesIO(b''), acct, cont, obj)

        all_asyncs = self.gather_async_pendings()
        # Between 1-2 asyncs per object
        total_objs = num_objs_per_ac * len(ac_pairs)
        self.assertGreater(len(all_asyncs), total_objs)
        self.assertLess(len(all_asyncs), total_objs * 2)

    def _gather_recon(self):
        # We'll collect recon only once from each node
        dev_to_node_dict = {}
        for onode in self.object_ring.devs:
            # We can skip any devices that are already covered by one of the
            # other nodes we found
            if any(self.is_local_to(node, onode)
                    for node in dev_to_node_dict.values()):
                continue
            dev_to_node_dict[onode["device"]] = onode
        self.assertEqual(4, len(dev_to_node_dict))  # sanity

        timeout = 20
        polling_interval = 2
        recon_data = []
        start = time.time()
        while True:
            for onode in list(dev_to_node_dict.values()):
                recon = direct_client.direct_get_recon(
                    onode, 'updater/object')
                if (recon.get('object_updater_stats') is not None and
                        recon.get('object_updater_sweep') is not None):
                    del dev_to_node_dict[onode["device"]]
                    recon_data.append(recon)
            if not dev_to_node_dict:
                break
            elapsed = time.time() - start
            if elapsed > timeout:
                self.fail(
                    "Updates did not process within {timeout} seconds".format(
                        timeout=timeout)
                )
            time.sleep(polling_interval)
        self.assertEqual(4, len(recon_data))  # sanity
        return recon_data

    def run_updater(self):
        raise NotImplementedError()

    def _check_recon_data(self, recon_data):
        ac_pairs = self.ac_pairs
        ac_set = set()
        for recon in recon_data:
            updater_stats = recon['object_updater_stats']

            found_count = updater_stats['failures_account_container_count']
            # No node should find MORE unique ac than we created
            self.assertLessEqual(found_count, len(ac_pairs))
            # and generally we'd expect them to have "at least" one from
            # significanly MORE than the "majority" of ac_pairs
            self.assertGreaterEqual(found_count, len(ac_pairs) / 2)

            oldest_count = updater_stats[
                'failures_oldest_timestamp_account_containers'
            ]['oldest_count']
            self.assertEqual(oldest_count, 5)

            ts_ac_entries = updater_stats[
                'failures_oldest_timestamp_account_containers'
            ]['oldest_entries']
            self.assertEqual(len(ts_ac_entries), oldest_count)

            for entry in ts_ac_entries:
                account = entry['account']
                container = entry['container']
                timestamp = entry['timestamp']
                self.assertIsNotNone(timestamp)
                ac_set.add((account, container))

            object_updater_last = recon.get('object_updater_last')
            self.assertIsNotNone(object_updater_last,
                                 "object_updater_last is missing")
            self.assertGreater(object_updater_last, 0,
                               "Invalid object_updater_last time")
        # All the collected ac_set are from the ac_pairs we created
        for ac in ac_set:
            self.assertIn(ac, set(ac_pairs))
        # Specifically, the ac_pairs we created failures for *first*
        # are represented by the oldest ac_set across nodes
        for ac in ac_pairs[:5]:
            self.assertIn(ac, ac_set)
        # Where as the more recent failures are NOT!
        for ac in ac_pairs[-3:]:
            self.assertNotIn(ac, ac_set)

    def test_stats(self):
        self._create_lots_of_asyncs()
        recon_data = self.run_updater()
        self._check_recon_data(recon_data)


class TestObjectUpdaterStatsRunOnce(UpdaterStatsMixIn, ReplProbeTest):

    def run_updater(self):
        # Run the updater and check stats
        Manager(['object-updater']).once()
        return self._gather_recon()


class TestObjectUpdaterStatsRunForever(UpdaterStatsMixIn, ECProbeTest):

    def _post_setup_config(self):
        CONF_SECTION = 'object-updater'
        self.conf_dest = os.path.join(
            '/tmp/',
            datetime.now().strftime('swift-%Y-%m-%d_%H-%M-%S-%f')
        )
        os.mkdir(self.conf_dest)
        object_server_dir = os.path.join(self.conf_dest, 'object-server')
        os.mkdir(object_server_dir)
        for conf_file in Server('object-updater').conf_files():
            config = readconf(conf_file)
            if CONF_SECTION not in config:
                continue  # Ensure the object-updater is set up to run
            config[CONF_SECTION].update({'interval': '1'})

            parser = ConfigParser()
            parser.add_section(CONF_SECTION)
            for option, value in config[CONF_SECTION].items():
                parser.set(CONF_SECTION, option, value)

            file_name = os.path.basename(conf_file)
            if file_name.endswith('.d'):
                # Work around conf.d setups (like you might see with VSAIO)
                file_name = file_name[:-2]
            with open(os.path.join(object_server_dir, file_name), 'w') as fp:
                parser.write(fp)

    def tearDown(self):
        shutil.rmtree(self.conf_dest)

    def run_updater(self):
        # Start the updater
        with mock.patch.object(manager, 'SWIFT_DIR', self.conf_dest):
            updater_status = self.object_updater.start()
        self.assertEqual(
            updater_status, 0, "Object updater failed to start")
        recon_data = self._gather_recon()
        # Stop the updater
        stop_status = self.object_updater.stop()
        self.assertEqual(stop_status, 0, "Object updater failed to stop")
        return recon_data


if __name__ == '__main__':
    main()
