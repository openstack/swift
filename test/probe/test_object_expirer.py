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

from collections import Counter
import json
import random
import time
import uuid
import unittest
from io import BytesIO

from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.manager import Manager
from swift.common.utils import Timestamp, config_true_value
from swift.common import direct_client
from swift.obj.expirer import extract_expirer_bytes_from_ctype

from test.probe.common import ReplProbeTest, ENABLED_POLICIES
from test.probe.brain import BrainSplitter

from swiftclient import client
from swiftclient.exceptions import ClientException


class TestObjectExpirer(ReplProbeTest):

    def setUp(self):
        self.expirer = Manager(['object-expirer'])
        self.expirer.start()
        err = self.expirer.stop()
        if err:
            raise unittest.SkipTest('Unable to verify object-expirer service')

        conf_files = []
        for server in self.expirer.servers:
            conf_files.extend(server.conf_files())
        conf_file = conf_files[0]
        self.client = InternalClient(conf_file, 'probe-test', 3)

        super(TestObjectExpirer, self).setUp()
        self.container_name = 'container-%s' % uuid.uuid4()
        self.object_name = 'object-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   self.object_name)

    def _check_obj_in_container_listing(self):
        for obj in self.client.iter_objects(self.account,
                                            self.container_name):

            if self.object_name == obj['name']:
                return True

        return False

    @unittest.skipIf(len(ENABLED_POLICIES) < 2, "Need more than one policy")
    def test_expirer_object_split_brain(self):
        old_policy = random.choice(ENABLED_POLICIES)
        wrong_policy = random.choice([p for p in ENABLED_POLICIES
                                      if p != old_policy])
        # create an expiring object and a container with the wrong policy
        self.brain.stop_primary_half()
        self.brain.put_container(int(old_policy))
        self.brain.put_object(headers={'X-Delete-After': 2})
        # get the object timestamp
        metadata = self.client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})
        create_timestamp = Timestamp(metadata['x-timestamp'])
        self.brain.start_primary_half()
        # get the expiring object updates in their queue, while we have all
        # the servers up
        Manager(['object-updater']).once()
        self.brain.stop_handoff_half()
        self.brain.put_container(int(wrong_policy))
        # don't start handoff servers, only wrong policy is available

        # make sure auto-created containers get in the account listing
        Manager(['container-updater']).once()
        # this guy should no-op since it's unable to expire the object
        self.expirer.once()

        self.brain.start_handoff_half()
        self.get_to_final_state()

        # validate object is expired
        found_in_policy = None
        metadata = self.client.get_object_metadata(
            self.account, self.container_name, self.object_name,
            acceptable_statuses=(4,),
            headers={'X-Backend-Storage-Policy-Index': int(old_policy)})
        self.assertIn('x-backend-timestamp', metadata)
        self.assertEqual(Timestamp(metadata['x-backend-timestamp']),
                         create_timestamp)

        # but it is still in the listing
        self.assertTrue(self._check_obj_in_container_listing(),
                        msg='Did not find listing for %s' % self.object_name)

        # clear proxy cache
        client.post_container(self.url, self.token, self.container_name, {})
        # run the expirer again after replication
        self.expirer.once()

        # object is not in the listing
        self.assertFalse(self._check_obj_in_container_listing(),
                         msg='Found listing for %s' % self.object_name)

        # and validate object is tombstoned
        found_in_policy = None
        for policy in ENABLED_POLICIES:
            metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name,
                acceptable_statuses=(4,),
                headers={'X-Backend-Storage-Policy-Index': int(policy)})
            if 'x-backend-timestamp' in metadata:
                if found_in_policy:
                    self.fail('found object in %s and also %s' %
                              (found_in_policy, policy))
                found_in_policy = policy
                self.assertIn('x-backend-timestamp', metadata)
                self.assertGreater(Timestamp(metadata['x-backend-timestamp']),
                                   create_timestamp)

    def test_expirer_doesnt_make_async_pendings(self):
        # The object expirer cleans up its own queue. The inner loop
        # basically looks like this:
        #
        #    for obj in stuff_to_delete:
        #        delete_the_object(obj)
        #        remove_the_queue_entry(obj)
        #
        # By default, upon receipt of a DELETE request for an expiring
        # object, the object servers will create async_pending records to
        # clean the expirer queue. Since the expirer cleans its own queue,
        # this is unnecessary. The expirer can make requests in such a way
        # tha the object server does not write out any async pendings; this
        # test asserts that this is the case.

        # Make an expiring object in each policy
        for policy in ENABLED_POLICIES:
            container_name = "expirer-test-%d" % policy.idx
            container_headers = {'X-Storage-Policy': policy.name}
            client.put_container(self.url, self.token, container_name,
                                 headers=container_headers)

            now = time.time()
            delete_at = int(now + 2.0)
            client.put_object(
                self.url, self.token, container_name, "some-object",
                headers={'X-Delete-At': str(delete_at),
                         'X-Timestamp': Timestamp(now).normal},
                contents='dontcare')

        time.sleep(2.0)
        # make sure auto-created expirer-queue containers get in the account
        # listing so the expirer can find them
        Manager(['container-updater']).once()

        # Make sure there's no async_pendings anywhere. Probe tests only run
        # on single-node installs anyway, so this set should be small enough
        # that an exhaustive check doesn't take too long.
        pendings_before = self.gather_async_pendings()

        # expire the objects
        Manager(['object-expirer']).once()
        pendings_after = self.gather_async_pendings()
        self.assertEqual(pendings_after, pendings_before)

    def test_expirer_object_should_not_be_expired(self):

        # Current object-expirer checks the correctness via x-if-delete-at
        # header that it can be deleted by expirer. If there are objects
        # either which doesn't have x-delete-at header as metadata or which
        # has different x-delete-at value from x-if-delete-at value,
        # object-expirer's delete will fail as 412 PreconditionFailed.
        # However, if some of the objects are in handoff nodes, the expirer
        # can put the tombstone with the timestamp as same as x-delete-at and
        # the object consistency will be resolved as the newer timestamp will
        # be winner (in particular, overwritten case w/o x-delete-at). This
        # test asserts such a situation that, at least, the overwriten object
        # which have larger timestamp than the original expirered date should
        # be safe.

        def put_object(headers):
            # use internal client to PUT objects so that X-Timestamp in headers
            # is effective
            headers['Content-Length'] = '0'
            path = self.client.make_path(
                self.account, self.container_name, self.object_name)
            try:
                self.client.make_request('PUT', path, headers, (2,))
            except UnexpectedResponse as e:
                self.fail(
                    'Expected 201 for PUT object but got %s' % e.resp.status)

        obj_brain = BrainSplitter(self.url, self.token, self.container_name,
                                  self.object_name, 'object', self.policy)

        # T(obj_created) < T(obj_deleted with x-delete-at) < T(obj_recreated)
        #   < T(expirer_executed)
        # Recreated obj should be appeared in any split brain case

        obj_brain.put_container()

        # T(obj_deleted with x-delete-at)
        # object-server accepts req only if X-Delete-At is later than 'now'
        # so here, T(obj_created) < T(obj_deleted with x-delete-at)
        now = time.time()
        delete_at = int(now + 2.0)
        recreate_at = delete_at + 1.0
        put_object(headers={'X-Delete-At': str(delete_at),
                            'X-Timestamp': Timestamp(now).normal})

        # some object servers stopped to make a situation that the
        # object-expirer can put tombstone in the primary nodes.
        obj_brain.stop_primary_half()

        # increment the X-Timestamp explicitly
        # (will be T(obj_deleted with x-delete-at) < T(obj_recreated))
        put_object(headers={'X-Object-Meta-Expired': 'False',
                            'X-Timestamp': Timestamp(recreate_at).normal})

        # make sure auto-created containers get in the account listing
        Manager(['container-updater']).once()
        # sanity, the newer object is still there
        try:
            metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name)
        except UnexpectedResponse as e:
            self.fail(
                'Expected 200 for HEAD object but got %s' % e.resp.status)

        self.assertIn('x-object-meta-expired', metadata)

        # some object servers recovered
        obj_brain.start_primary_half()

        # sleep until after recreated_at
        while time.time() <= recreate_at:
            time.sleep(0.1)
        # Now, expirer runs at the time after obj is recreated
        self.expirer.once()

        # verify that original object was deleted by expirer
        obj_brain.stop_handoff_half()
        try:
            metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name,
                acceptable_statuses=(4,))
        except UnexpectedResponse as e:
            self.fail(
                'Expected 404 for HEAD object but got %s' % e.resp.status)
        obj_brain.start_handoff_half()

        # and inconsistent state of objects is recovered by replicator
        Manager(['object-replicator']).once()

        # check if you can get recreated object
        try:
            metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name)
        except UnexpectedResponse as e:
            self.fail(
                'Expected 200 for HEAD object but got %s' % e.resp.status)

        self.assertIn('x-object-meta-expired', metadata)

    def _setup_test_open_expired(self):
        obj_brain = BrainSplitter(self.url, self.token, self.container_name,
                                  self.object_name, 'object', self.policy)

        obj_brain.put_container()

        now = time.time()
        delete_at = int(now + 2)
        try:
            path = self.client.make_path(
                self.account, self.container_name, self.object_name)
            self.client.make_request('PUT', path, {
                'X-Delete-At': str(delete_at),
                'X-Timestamp': Timestamp(now).normal,
                'Content-Length': '3',
                'X-Object-Meta-Test': 'foo',
            }, (2,), BytesIO(b'foo'))
        except UnexpectedResponse as e:
            self.fail(
                'Expected 201 for PUT object but got %s' % e.resp.status)

        # sanity: check that the object was created
        try:
            resp = client.head_object(self.url, self.token,
                                      self.container_name, self.object_name)
            self.assertEqual('foo', resp.get('x-object-meta-test'))
        except ClientException as e:
            self.fail(
                'Expected 200 for HEAD object but got %s' % e.http_status)

        # make sure auto-created containers get in the account listing
        Manager(['container-updater']).once()

        # sleep until after expired but not reaped
        while time.time() <= delete_at:
            time.sleep(0.1)

        # should get a 404, object is expired
        with self.assertRaises(ClientException) as e:
            client.head_object(self.url, self.token,
                               self.container_name, self.object_name)
        self.assertEqual(e.exception.http_status, 404)

    def test_open_expired_enabled(self):

        # When the global configuration option allow_open_expired is set to
        # true, the client should be able to access expired objects that have
        # not yet been reaped using the x-open-expired flag. However, after
        # they have been reaped, it should return 404.

        allow_open_expired = config_true_value(
            self.cluster_info['swift'].get('allow_open_expired')
        )

        if not allow_open_expired:
            raise unittest.SkipTest(
                "allow_open_expired is disabled in this swift cluster")

        self._setup_test_open_expired()

        # since allow_open_expired is enabled, ensure object can be accessed
        # with x-open-expired header
        # HEAD request should succeed
        try:
            resp = client.head_object(self.url, self.token,
                                      self.container_name, self.object_name,
                                      headers={'X-Open-Expired': True})
            self.assertEqual('foo', resp.get('x-object-meta-test'))
        except ClientException as e:
            self.fail(
                'Expected 200 for HEAD object but got %s' % e.http_status)

        # GET request should succeed
        try:
            _, body = client.get_object(self.url, self.token,
                                        self.container_name, self.object_name,
                                        headers={'X-Open-Expired': True})
            self.assertEqual(body, b'foo')
        except ClientException as e:
            self.fail(
                'Expected 200 for GET object but got %s' % e.http_status)

        # POST request should succeed, update x-delete-at
        now = time.time()
        new_delete_at = int(now + 5)
        try:
            client.post_object(self.url, self.token,
                               self.container_name, self.object_name,
                               headers={
                                   'X-Open-Expired': True,
                                   'X-Delete-At': str(new_delete_at),
                                   'X-Object-Meta-Test': 'bar'
                               })
        except ClientException as e:
            self.fail(
                'Expected 200 for POST object but got %s' % e.http_status)

        # GET requests succeed again, even without the magic header
        try:
            _, body = client.get_object(self.url, self.token,
                                        self.container_name, self.object_name)
            self.assertEqual(body, b'foo')
        except ClientException as e:
            self.fail(
                'Expected 200 for GET object but got %s' % e.http_status)

        # make sure auto-created containers get in the account listing
        Manager(['container-updater']).once()

        # run the expirer, but the object expiry time is now in the future
        self.expirer.once()
        try:
            resp = client.head_object(self.url, self.token,
                                      self.container_name, self.object_name,
                                      headers={'X-Open-Expired': True})
            self.assertEqual('bar', resp.get('x-object-meta-test'))
        except ClientException as e:
            self.fail(
                'Expected 200 for HEAD object but got %s' % e.http_status)

        # wait for the object to expire
        while time.time() <= new_delete_at:
            time.sleep(0.1)

        # expirer runs to reap the object
        self.expirer.once()

        # should get a 404 even with x-open-expired since object is reaped
        with self.assertRaises(ClientException) as e:
            client.head_object(self.url, self.token,
                               self.container_name, self.object_name,
                               headers={'X-Open-Expired': True})
        self.assertEqual(e.exception.http_status, 404)

    def _setup_test_slo_object(self):
        segment_container = self.container_name + '_segments'
        client.put_container(self.url, self.token, self.container_name, {})
        client.put_container(self.url, self.token, segment_container, {})
        client.put_object(self.url, self.token,
                          segment_container, 'segment_1', b'12')
        client.put_object(self.url, self.token,
                          segment_container, 'segment_2', b'5678')
        client.put_object(
            self.url, self.token, self.container_name, 'slo', json.dumps([
                {'path': segment_container + '/segment_1'},
                {'data': 'Cg=='},
                {'path': segment_container + '/segment_2'},
            ]), query_string='multipart-manifest=put')
        _, body = client.get_object(self.url, self.token,
                                    self.container_name, 'slo')
        self.assertEqual(body, b'12\n5678')

        return segment_container, self.container_name

    def test_open_expired_enabled_with_part_num(self):
        allow_open_expired = config_true_value(
            self.cluster_info['swift'].get('allow_open_expired')
        )

        if not allow_open_expired:
            raise unittest.SkipTest(
                "allow_open_expired is disabled in this swift cluster"
            )

        seg_container, container_name = self._setup_test_slo_object()
        now = time.time()
        delete_at = int(now + 1)

        client.post_object(
            self.url, self.token, container_name, 'slo',
            headers={
                'X-Delete-At': str(delete_at),
                'X-Object-Meta-Test': 'foo'
            }
        )

        # make sure auto-created containers get in the account listing
        Manager(['container-updater']).once()

        # sleep until after expired but not reaped
        while time.time() <= delete_at:
            time.sleep(0.1)

        # should get a 404, object is expired
        while True:
            try:
                client.head_object(self.url, self.token, container_name, 'slo')
                time.sleep(1)  # Wait for a short period before trying again
            except ClientException as e:
                # check if the object is expired
                if e.http_status == 404:
                    break  # The object is expired, so we can exit the loop

        resp_headers = client.head_object(
            self.url, self.token, container_name, 'slo',
            headers={'X-Open-Expired': True},
            query_string='part-number=1'
        )

        self.assertEqual(resp_headers.get('x-object-meta-test'), 'foo')
        self.assertEqual(resp_headers.get('content-range'), 'bytes 0-1/7')
        self.assertEqual(resp_headers.get('content-length'), '2')
        self.assertEqual(resp_headers.get('x-parts-count'), '3')
        self.assertEqual(resp_headers.get('x-static-large-object'), 'True')
        self.assertEqual(resp_headers.get('accept-ranges'), 'bytes')

        with self.assertRaises(ClientException) as e:
            client.head_object(self.url, self.token, container_name, 'slo')
        self.assertEqual(e.exception.http_status, 404)

        now = time.time()
        delete_at = int(now + 2)
        for seg_obj_name in ['segment_1', 'segment_2']:
            client.post_object(
                self.url, self.token, seg_container, seg_obj_name,
                headers={
                    'X-Open-Expired': True,
                    'X-Segment-Meta-Test': 'segment-foo',
                    'X-Delete-At': str(delete_at)
                }
            )

        # make sure auto-created containers get in the account listing
        Manager(['container-updater']).once()
        while time.time() <= delete_at:
            time.sleep(0.1)

        # should get a 404, segment object is expired
        with self.assertRaises(ClientException) as e:
            client.head_object(self.url, self.token, seg_container,
                               'segment_2')
        self.assertEqual(e.exception.http_status, 404)

        # magic of x-open-expired
        resp_headers = client.head_object(
            self.url, self.token, seg_container, 'segment_2',
            headers={'X-Open-Expired': True},
            query_string='part-number=1'
        )

        # keep in mind that the segment object is expired
        self.assertEqual(resp_headers.get('content-length'), '4')
        self.assertTrue(time.time() > delete_at)

        # expirer runs to reap the whichever object was set for expiry
        self.expirer.once()

        for seg_obj_name in ['segment_1', 'segment_2']:
            # should get a 404 even with x-open-expired since object is reaped
            with self.assertRaises(ClientException) as e:
                client.head_object(
                    self.url, self.token, seg_container, seg_obj_name,
                    headers={'X-Open-Expired': True},
                    query_string='part-number=1'
                )
            self.assertEqual(e.exception.http_status, 404)

    def test_open_expired_disabled(self):

        # When the global configuration option allow_open_expired is set to
        # false or not configured, the client should not be able to access
        # expired objects that have not yet been reaped using the
        # x-open-expired flag.

        allow_open_expired = config_true_value(
            self.cluster_info['swift'].get('allow_open_expired')
        )

        if allow_open_expired:
            raise unittest.SkipTest(
                "allow_open_expired is enabled in this swift cluster")

        self._setup_test_open_expired()

        # since allow_open_expired is disabled, should get 404 even
        # with x-open-expired header
        # HEAD request should fail
        with self.assertRaises(ClientException) as e:
            client.head_object(self.url, self.token,
                               self.container_name, self.object_name,
                               headers={'X-Open-Expired': True})
        self.assertEqual(e.exception.http_status, 404)

        # POST request should fail
        with self.assertRaises(ClientException) as e:
            client.post_object(self.url, self.token,
                               self.container_name, self.object_name,
                               headers={'X-Open-Expired': True})
        self.assertEqual(e.exception.http_status, 404)

        # GET request should fail
        with self.assertRaises(ClientException) as e:
            client.get_object(self.url, self.token,
                              self.container_name, self.object_name,
                              headers={'X-Open-Expired': True})
        self.assertEqual(e.exception.http_status, 404)

        # But with internal client, can GET with X-Backend-Open-Expired
        # Since object still exists on disk
        try:
            object_metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name,
                acceptable_statuses=(2,),
                headers={'X-Backend-Open-Expired': True})
        except UnexpectedResponse as e:
            self.fail(
                'Expected 200 for GET object but got %s' % e.resp.status)
        self.assertEqual('foo', object_metadata.get('x-object-meta-test'))

        # expirer runs to reap the object
        self.expirer.once()

        # should get a 404 even with X-Backend-Open-Expired
        # since object is reaped
        with self.assertRaises(UnexpectedResponse) as e:
            object_metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name,
                acceptable_statuses=(2,),
                headers={'X-Backend-Open-Expired': True})
        self.assertEqual(e.exception.resp.status_int, 404)

    def test_expirer_object_bytes_eventual_consistency(self):
        obj_brain = BrainSplitter(self.url, self.token, self.container_name,
                                  self.object_name, 'object', self.policy)

        obj_brain.put_container()

        def put_object(content_length=0):
            try:
                self.client.upload_object(BytesIO(bytes(content_length)),
                                          self.account, self.container_name,
                                          self.object_name)
            except UnexpectedResponse as e:
                self.fail(
                    'Expected 201 for PUT object but got %s' % e.resp.status)

        t0_content_length = 24
        put_object(content_length=t0_content_length)

        try:
            metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name)
        except UnexpectedResponse as e:
            self.fail(
                'Expected 200 for HEAD object but got %s' % e.resp.status)

        assert metadata['content-length'] == str(t0_content_length)
        t0 = metadata['x-timestamp']

        obj_brain.stop_primary_half()

        t1_content_length = 32
        put_object(content_length=t1_content_length)

        try:
            metadata = self.client.get_object_metadata(
                self.account, self.container_name, self.object_name)
        except UnexpectedResponse as e:
            self.fail(
                'Expected 200 for HEAD object but got %s' % e.resp.status)

        assert metadata['content-length'] == str(t1_content_length)
        t1 = metadata['x-timestamp']

        # some object servers recovered
        obj_brain.start_primary_half()

        head_responses = []

        for node in obj_brain.ring.devs:
            metadata = direct_client.direct_head_object(
                node, obj_brain.part, self.account, self.container_name,
                self.object_name)
            head_responses.append(metadata)

        timestamp_counts = Counter([
            resp['X-Timestamp'] for resp in head_responses
        ])
        expected_counts = {t0: 2, t1: 2}
        self.assertEqual(expected_counts, timestamp_counts)

        # Do a POST to update object metadata (timestamp x-delete-at)
        # POST will create an expiry queue entry with 2 landing on t0, 1 on t1
        self.client.set_object_metadata(
            self.account, self.container_name, self.object_name,
            metadata={'X-Delete-After': '5'}, acceptable_statuses=(2,)
        )

        # Run the container updater once to register new container containing
        # expirey queue entry
        Manager(['container-updater']).once()

        # Find the name of the container containing the expiring object
        expiring_containers = list(
            self.client.iter_containers('.expiring_objects')
        )
        self.assertEqual(1, len(expiring_containers))

        expiring_container = expiring_containers[0]
        expiring_container_name = expiring_container['name']

        # Verify that there is one expiring object
        expiring_objects = list(
            self.client.iter_objects('.expiring_objects',
                                     expiring_container_name)
        )
        self.assertEqual(1, len(expiring_objects))

        # Get the nodes of the expiring container
        expiring_container_part_num, expiring_container_nodes = \
            self.client.container_ring.get_nodes('.expiring_objects',
                                                 expiring_container_name)

        # Verify that there are only 3 such nodes
        self.assertEqual(3, len(expiring_container_nodes))

        listing_records = []
        for node in expiring_container_nodes:
            metadata, container_data = direct_client.direct_get_container(
                node, expiring_container_part_num, '.expiring_objects',
                expiring_container_name)
            # Verify there is metadata for only one object
            self.assertEqual(1, len(container_data))
            listing_records.append(container_data[0])

        # Check for inconsistent metadata
        byte_size_counts = Counter([
            extract_expirer_bytes_from_ctype(resp['content_type'])
            for resp in listing_records
        ])
        expected_byte_size_counts = {
            t0_content_length: 2,
            t1_content_length: 1
        }

        self.assertEqual(expected_byte_size_counts, byte_size_counts)

        # Run the replicator to update expirey queue entries
        Manager(['container-replicator']).once()

        listing_records = []
        for node in expiring_container_nodes:
            metadata, container_data = direct_client.direct_get_container(
                node, expiring_container_part_num, '.expiring_objects',
                expiring_container_name)
            self.assertEqual(1, len(container_data))
            listing_records.append(container_data[0])

        # Ensure that metadata is now consistent
        byte_size_counts = Counter([
            extract_expirer_bytes_from_ctype(resp['content_type'])
            for resp in listing_records
        ])
        expected_byte_size_counts = {t1_content_length: 3}

        self.assertEqual(expected_byte_size_counts, byte_size_counts)

    def _test_expirer_delete_outdated_object_version(self, object_exists):
        # This test simulates a case where the expirer tries to delete
        # an outdated version of an object.
        # One case is where the expirer gets a 404, whereas the newest version
        # of the object is offline.
        # Another case is where the expirer gets a 412, since the old version
        # of the object mismatches the expiration time sent by the expirer.
        # In any of these cases, the expirer should retry deleting the object
        # later, for as long as a reclaim age has not passed.
        obj_brain = BrainSplitter(self.url, self.token, self.container_name,
                                  self.object_name, 'object', self.policy)

        obj_brain.put_container()

        if object_exists:
            obj_brain.put_object()

        # currently, the object either doesn't exist, or does not have
        # an expiration

        # stop primary servers and put a newer version of the object, this
        # time with an expiration. only the handoff servers will have
        # the new version
        obj_brain.stop_primary_half()
        now = time.time()
        delete_at = int(now + 2.0)
        obj_brain.put_object({'X-Delete-At': str(delete_at)})

        # make sure auto-created containers get in the account listing
        Manager(['container-updater']).once()

        # update object record in the container listing
        Manager(['container-replicator']).once()

        # take handoff servers down, and bring up the outdated primary servers
        obj_brain.start_primary_half()
        obj_brain.stop_handoff_half()

        # wait until object expiration time
        while time.time() <= delete_at:
            time.sleep(0.1)

        # run expirer against the outdated servers. it should fail since
        # the outdated version does not match the expiration time
        self.expirer.once()

        # bring all servers up, and run replicator to update servers
        obj_brain.start_handoff_half()
        Manager(['object-replicator']).once()

        # verify the deletion has failed by checking the container listing
        self.assertTrue(self._check_obj_in_container_listing(),
                        msg='Did not find listing for %s' % self.object_name)

        # run expirer again, delete should now succeed
        self.expirer.once()

        # verify the deletion by checking the container listing
        self.assertFalse(self._check_obj_in_container_listing(),
                         msg='Found listing for %s' % self.object_name)

    def test_expirer_delete_returns_outdated_404(self):
        self._test_expirer_delete_outdated_object_version(object_exists=False)

    def test_expirer_delete_returns_outdated_412(self):
        self._test_expirer_delete_outdated_object_version(object_exists=True)

    def test_slo_async_delete(self):
        if not self.cluster_info.get('slo', {}).get('allow_async_delete'):
            raise unittest.SkipTest('allow_async_delete not enabled')

        segment_container, _ = self._setup_test_slo_object()

        client.delete_object(
            self.url, self.token, self.container_name, 'slo',
            query_string='multipart-manifest=delete&async=true')

        # Object's deleted
        _, objects = client.get_container(self.url, self.token,
                                          self.container_name)
        self.assertEqual(objects, [])
        with self.assertRaises(client.ClientException) as caught:
            client.get_object(self.url, self.token, self.container_name, 'slo')
        self.assertEqual(404, caught.exception.http_status)

        # But segments are still around and accessible
        _, objects = client.get_container(self.url, self.token,
                                          segment_container)
        self.assertEqual([o['name'] for o in objects],
                         ['segment_1', 'segment_2'])
        _, body = client.get_object(self.url, self.token,
                                    segment_container, 'segment_1')
        self.assertEqual(body, b'12')
        _, body = client.get_object(self.url, self.token,
                                    segment_container, 'segment_2')
        self.assertEqual(body, b'5678')

        # make sure auto-created expirer-queue containers get in the account
        # listing so the expirer can find them
        Manager(['container-updater']).once()
        self.expirer.once()

        # Now the expirer has cleaned up the segments
        _, objects = client.get_container(self.url, self.token,
                                          segment_container)
        self.assertEqual(objects, [])
        with self.assertRaises(client.ClientException) as caught:
            client.get_object(self.url, self.token,
                              segment_container, 'segment_1')
        self.assertEqual(404, caught.exception.http_status)
        with self.assertRaises(client.ClientException) as caught:
            client.get_object(self.url, self.token,
                              segment_container, 'segment_2')
        self.assertEqual(404, caught.exception.http_status)


if __name__ == "__main__":
    unittest.main()
