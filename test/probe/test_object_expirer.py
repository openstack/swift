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

import random
import time
import uuid
import unittest

from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.manager import Manager
from swift.common.utils import Timestamp

from test.probe.common import ReplProbeTest, ENABLED_POLICIES
from test.probe.brain import BrainSplitter

from swiftclient import client


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
        all_obj_nodes = self.get_all_object_nodes()
        pendings_before = self.gather_async_pendings(all_obj_nodes)

        # expire the objects
        Manager(['object-expirer']).once()
        pendings_after = self.gather_async_pendings(all_obj_nodes)
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


if __name__ == "__main__":
    unittest.main()
