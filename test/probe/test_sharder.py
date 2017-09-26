# Copyright (c) 2017 OpenStack Foundation
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
import uuid

from nose import SkipTest

from swift.container.backend import ContainerBroker, DB_STATE
from swift.common import utils
from swift.common.manager import Manager
from swiftclient import client, get_auth

from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, get_server_number


MAX_SHARD_CONTAINER_SIZE = 100


def get_shard_ranges(broker, include_meta=False):
    shard_ranges = broker.get_shard_ranges()
    if not include_meta:
        shard_ranges = [row[:-1] for row in shard_ranges]
    return shard_ranges


class TestContainerSharding(ReplProbeTest):
    def setUp(self):
        super(TestContainerSharding, self).setUp()
        _, self.admin_token = get_auth(
            'http://127.0.0.1:8080/auth/v1.0', 'admin:admin', 'admin')
        self.container_name = 'container-%s' % uuid.uuid4()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   None, 'container')
        try:
            cont_configs = [utils.readconf(p, 'container-sharder')
                            for p in self.configs['container-server'].values()]
        except ValueError:
            self.fail('No [container-sharder] section found in '
                      'container-server configs!')

        if cont_configs:
            self.max_shard_size = max(
                int(c.get('shard_container_size', '1000000'))
                for c in cont_configs)
        else:
            self.max_shard_size = 1000000

        if self.max_shard_size > MAX_SHARD_CONTAINER_SIZE:
            raise SkipTest('shard_container_size is too big! %d > %d' %
                           self.max_shard_size, MAX_SHARD_CONTAINER_SIZE)

    def categorize_container_dir_content(self, container=None):
        container = container or self.container_name
        part, nodes = self.brain.ring.get_nodes(self.brain.account, container)
        datadirs = []
        for node in nodes:
            server_type, config_number = get_server_number(
                (node['ip'], node['port']), self.ipport2server)
            assert server_type == 'container'
            repl_server = '%s-replicator' % server_type
            conf = utils.readconf(self.configs[repl_server][config_number],
                                  section_name=repl_server)
            datadirs.append(os.path.join(conf['devices'], node['device'],
                                         'containers'))
        container_hash = utils.hash_path(self.brain.account, container)
        storage_dirs = [utils.storage_directory(datadir, part, container_hash)
                        for datadir in datadirs]
        result = {
            'shard_dbs': [],
            'normal_dbs': [],
            'pendings': [],
            'locks': [],
            'other': [],
        }
        for storage_dir in storage_dirs:
            for f in os.listdir(storage_dir):
                path = os.path.join(storage_dir, f)
                if path.endswith('_shard.db'):
                    result['shard_dbs'].append(path)
                elif path.endswith('.db'):
                    result['normal_dbs'].append(path)
                elif path.endswith('.db.pending'):
                    result['pendings'].append(path)
                elif path.endswith('/.lock'):
                    result['locks'].append(path)
                else:
                    result['other'].append(path)
        if result['other']:
            self.fail('Found unexpected files in storage directory:\n  %s' %
                      '\n  '.join(result['other']))
        return result

    def assertLengthEqual(self, obj, length):
        obj_len = len(obj)
        self.assertEqual(obj_len, length, 'len(%r) == %d, not %d' % (
            obj, obj_len, length))

    def _test_sharded_listing(self, run_replicators=False):
        sharders = Manager(['container-sharder'])
        obj_names = ['obj%03d' % x for x in range(self.max_shard_size)]

        self.brain.put_container()

        for obj in obj_names:
            client.put_object(self.url, self.token, self.container_name, obj)

        # Verify that we start out with normal DBs, no shards
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['normal_dbs'], 3)
        self.assertLengthEqual(found['shard_dbs'], 0)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual('unsharded', DB_STATE[broker.get_db_state()])
            self.assertLengthEqual(get_shard_ranges(broker), 0)

        headers, pre_sharding_listing = client.get_container(
            self.url, self.token, self.container_name)
        self.assertEqual(obj_names, [x['name'].encode('utf-8')
                                     for x in pre_sharding_listing])  # sanity

        # Shard it
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        headers = client.head_container(self.url, self.admin_token,
                                        self.container_name)
        self.assertEqual('True', headers.get('x-container-sharding'))

        # Only run the one in charge of scanning
        sharders.once(number=self.brain.node_numbers[0])

        # Verify that we have one shard db -- though the other normal DBs
        # received the shard ranges that got defined
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 1)
        broker = ContainerBroker(found['shard_dbs'][0])
        # TODO: assert the shard db is on replica 0
        self.assertIs(True, broker.is_root_container())
        self.assertEqual('sharded', DB_STATE[broker.get_db_state()])
        expected_shard_ranges = get_shard_ranges(broker)
        self.assertLengthEqual(expected_shard_ranges, 2)

        self.assertLengthEqual(found['normal_dbs'], 2)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual('unsharded', DB_STATE[broker.get_db_state()])
            self.assertEqual(expected_shard_ranges, get_shard_ranges(broker))

        if run_replicators:
            Manager(['container-replicator']).once()
            # This moves the normal DB, but *not* the shard DB
            found = self.categorize_container_dir_content()
            self.assertLengthEqual(found['shard_dbs'], 1)
            self.assertLengthEqual(found['normal_dbs'], 3)

        # Now that everyone has shard ranges, run *everyone*
        sharders.once()

        # Verify that we only have shard dbs now
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 0)
        # Shards stayed the same
        for db_file in found['shard_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual('sharded', DB_STATE[broker.get_db_state()])
            self.assertEqual(expected_shard_ranges, get_shard_ranges(broker))

        # Check that entire listing is available
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name)
        self.assertEqual(pre_sharding_listing, listing)
        # ... and has the right object count
        self.assertIn('x-container-object-count', headers)
        self.assertEqual(headers['x-container-object-count'],
                         str(len(obj_names)))

        # It even works in reverse!
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name,
                                                query_string='reverse=on')
        self.assertEqual(pre_sharding_listing[::-1], listing)

    def test_sharded_listing_no_replicators(self):
        self._test_sharded_listing()

    def test_sharded_listing_with_replicators(self):
        self._test_sharded_listing(run_replicators=True)

    def test_async_pendings(self):
        sharders = Manager(['container-sharder'])
        obj_names = ['obj%03d' % x for x in range(self.max_shard_size * 2)]

        self.brain.put_container()

        # There are some updates *everyone* gets
        for obj in obj_names[::5]:
            client.put_object(self.url, self.token, self.container_name, obj)
        # But roll some outages so each container only get ~3/4 object records
        # and async pendings pile up
        for i, n in enumerate(self.brain.node_numbers, start=1):
            self.brain.servers.stop(number=n)
            for o in obj_names[i::5]:
                client.put_object(self.url, self.token, self.container_name, o)
            self.brain.servers.start(number=n)

        # But there are also some updates *no one* gets
        self.brain.servers.stop()
        for obj in obj_names[4::5]:
            client.put_object(self.url, self.token, self.container_name, obj)
        self.brain.servers.start()

        # Shard it
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        headers = client.head_container(self.url, self.admin_token,
                                        self.container_name)
        self.assertEqual('True', headers.get('x-container-sharding'))

        # Only run the one in charge of scanning
        sharders.once(number=self.brain.node_numbers[0])

        # Verify that we have one shard db -- though the other normal DBs
        # received the shard ranges that got defined
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 1)
        node_index_zero_db = found['shard_dbs'][0]
        broker = ContainerBroker(node_index_zero_db)
        # TODO: assert the shard db is on replica 0
        self.assertIs(True, broker.is_root_container())
        # TODO: not sharding! shard DB thinks he's done!?
        self.assertEqual('sharded', DB_STATE[broker.get_db_state()])
        expected_shard_ranges = get_shard_ranges(broker)
        self.assertLengthEqual(expected_shard_ranges, 3)

        # Still have all three big DBs -- we've only worked our way through
        # 2 of the 3 ranges that got defined
        self.assertLengthEqual(found['normal_dbs'], 3)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual(expected_shard_ranges, get_shard_ranges(broker))
            if db_file.startswith(os.path.dirname(node_index_zero_db)):
                self.assertEqual('sharding', DB_STATE[broker.get_db_state()])
            else:
                self.assertEqual('unsharded', DB_STATE[broker.get_db_state()])

        # Run the other sharders so we're all in (roughly) the same state
        for n in self.brain.node_numbers[1:]:
            sharders.once(number=n)
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 3)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertEqual('sharding', DB_STATE[broker.get_db_state()])
            if db_file.startswith(os.path.dirname(node_index_zero_db)):
                # Original scanner has no new info
                self.assertEqual(len(obj_names) * 3 // 5,
                                 broker.get_info()['object_count'])
            else:
                # TODO: But I guess we replicated to the other guys!?
                # 'Cause everyone else is only missing the updates that didn't
                # make it to any container server
                self.assertEqual(len(obj_names) * 4 // 5,
                                 broker.get_info()['object_count'])

        # Run updaters to clear the async pendings
        Manager(['object-updater']).once()

        # Our "big" dbs didn't take updates
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            if db_file.startswith(os.path.dirname(node_index_zero_db)):
                self.assertEqual(len(obj_names) * 3 // 5,
                                 broker.get_info()['object_count'])
            else:
                self.assertEqual(len(obj_names) * 4 // 5,
                                 broker.get_info()['object_count'])

        # TODO: confirm that the updates got redirected to the shards

        # Check that entire listing is available -- this requires splicing
        # together the first two complete shards, the old records from the
        # root, *and* the new records from the shard that only has missed
        # updates
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name)
        self.assertEqual([x['name'].encode('utf-8') for x in listing],
                         obj_names)
        self.assertIn('x-container-object-count', headers)
        # Object count is hard to reason about though!
        # TODO: nail down what this *should* be and make sure all containers
        # respond with it! Depending on what you're looking at, this
        # could be 0, 1/2, 7/12 (!?), 3/5, 2/3, or 4/5 or all objects!
        # self.assertEqual(headers['x-container-object-count'],
        #                  str(len(obj_names) - len(obj_names) // 6))

        # TODO: Doesn't work in reverse, yet
        # headers, listing = client.get_container(self.url, self.token,
        #                                         self.container_name,
        #                                         query_string='reverse=on')
        # self.assertEqual([x['name'].encode('utf-8') for x in listing],
        #                  obj_names[::-1])

        # Run the sharders again to get everything to settle
        sharders.once()
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 0)
