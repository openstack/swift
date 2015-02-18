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
import uuid
import unittest

from nose import SkipTest

from swift.common.internal_client import InternalClient
from swift.common.manager import Manager
from swift.common.utils import Timestamp

from test.probe.common import ReplProbeTest, ENABLED_POLICIES
from test.probe.test_container_merge_policy_index import BrainSplitter

from swiftclient import client


class TestObjectExpirer(ReplProbeTest):

    def setUp(self):
        if len(ENABLED_POLICIES) < 2:
            raise SkipTest('Need more than one policy')

        self.expirer = Manager(['object-expirer'])
        self.expirer.start()
        err = self.expirer.stop()
        if err:
            raise SkipTest('Unable to verify object-expirer service')

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
        self.assert_('x-backend-timestamp' in metadata)
        self.assertEqual(Timestamp(metadata['x-backend-timestamp']),
                         create_timestamp)

        # but it is still in the listing
        for obj in self.client.iter_objects(self.account,
                                            self.container_name):
            if self.object_name == obj['name']:
                break
        else:
            self.fail('Did not find listing for %s' % self.object_name)

        # clear proxy cache
        client.post_container(self.url, self.token, self.container_name, {})
        # run the expirier again after replication
        self.expirer.once()

        # object is not in the listing
        for obj in self.client.iter_objects(self.account,
                                            self.container_name):
            if self.object_name == obj['name']:
                self.fail('Found listing for %s' % self.object_name)

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
                self.assert_('x-backend-timestamp' in metadata)
                self.assert_(Timestamp(metadata['x-backend-timestamp']) >
                             create_timestamp)

if __name__ == "__main__":
    unittest.main()
