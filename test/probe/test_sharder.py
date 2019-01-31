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
import hashlib
import json
import os
import shutil
import uuid

from nose import SkipTest

from swift.common import direct_client
from swift.common.direct_client import DirectClientException
from swift.common.utils import ShardRange, parse_db_filename, get_db_files, \
    quorum_size, config_true_value, Timestamp
from swift.container.backend import ContainerBroker, UNSHARDED, SHARDING
from swift.common import utils
from swift.common.manager import Manager
from swiftclient import client, get_auth, ClientException

from swift.proxy.controllers.obj import num_container_updates
from test import annotate_failure
from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, get_server_number, \
    wait_for_server_to_hangup


MIN_SHARD_CONTAINER_THRESHOLD = 4
MAX_SHARD_CONTAINER_THRESHOLD = 100


class ShardCollector(object):
    """
    Returns map of node to tuples of (headers, shard ranges) returned from node
    """
    def __init__(self):
        self.ranges = {}

    def __call__(self, cnode, cpart, account, container):
        self.ranges[cnode['id']] = direct_client.direct_get_container(
            cnode, cpart, account, container,
            headers={'X-Backend-Record-Type': 'shard'})


class BaseTestContainerSharding(ReplProbeTest):

    def _maybe_skip_test(self):
        try:
            cont_configs = [utils.readconf(p, 'container-sharder')
                            for p in self.configs['container-server'].values()]
        except ValueError:
            raise SkipTest('No [container-sharder] section found in '
                           'container-server configs')

        skip_reasons = []
        auto_shard = all([config_true_value(c.get('auto_shard', False))
                          for c in cont_configs])
        if not auto_shard:
            skip_reasons.append(
                'auto_shard must be true in all container_sharder configs')

        self.max_shard_size = max(
            int(c.get('shard_container_threshold', '1000000'))
            for c in cont_configs)

        if not (MIN_SHARD_CONTAINER_THRESHOLD <= self.max_shard_size
                <= MAX_SHARD_CONTAINER_THRESHOLD):
            skip_reasons.append(
                'shard_container_threshold %d must be between %d and %d' %
                (self.max_shard_size, MIN_SHARD_CONTAINER_THRESHOLD,
                 MAX_SHARD_CONTAINER_THRESHOLD))

        def skip_check(reason_list, option, required):
            values = set([int(c.get(option, required)) for c in cont_configs])
            if values != {required}:
                reason_list.append('%s must be %s' % (option, required))

        skip_check(skip_reasons, 'shard_scanner_batch_size', 10)
        skip_check(skip_reasons, 'shard_batch_size', 2)

        if skip_reasons:
            raise SkipTest(', '.join(skip_reasons))

    def _load_rings_and_configs(self):
        super(BaseTestContainerSharding, self)._load_rings_and_configs()
        # perform checks for skipping test before starting services
        self._maybe_skip_test()

    def _make_object_names(self, number):
        return ['obj-%04d' % x for x in range(number)]

    def _setup_container_name(self):
        self.container_name = 'container-%s' % uuid.uuid4()

    def setUp(self):
        client.logger.setLevel(client.logging.WARNING)
        client.requests.logging.getLogger().setLevel(
            client.requests.logging.WARNING)
        super(BaseTestContainerSharding, self).setUp()
        _, self.admin_token = get_auth(
            'http://127.0.0.1:8080/auth/v1.0', 'admin:admin', 'admin')
        self._setup_container_name()
        self.brain = BrainSplitter(self.url, self.token, self.container_name,
                                   None, 'container')
        self.brain.put_container(policy_index=int(self.policy))
        self.sharders = Manager(['container-sharder'])
        self.internal_client = self.make_internal_client()

    def stop_container_servers(self, node_numbers=None):
        if node_numbers:
            ipports = []
            server2ipport = {v: k for k, v in self.ipport2server.items()}
            for number in self.brain.node_numbers[node_numbers]:
                self.brain.servers.stop(number=number)
                server = 'container%d' % number
                ipports.append(server2ipport[server])
        else:
            ipports = [k for k, v in self.ipport2server.items()
                       if v.startswith('container')]
            self.brain.servers.stop()
        for ipport in ipports:
            wait_for_server_to_hangup(ipport)

    def put_objects(self, obj_names, contents=None):
        for obj in obj_names:
            client.put_object(self.url, token=self.token,
                              container=self.container_name, name=obj,
                              contents=contents)

    def delete_objects(self, obj_names):
        for obj in obj_names:
            client.delete_object(
                self.url, self.token, self.container_name, obj)

    def get_container_shard_ranges(self, account=None, container=None):
        account = account if account else self.account
        container = container if container else self.container_name
        path = self.internal_client.make_path(account, container)
        resp = self.internal_client.make_request(
            'GET', path + '?format=json', {'X-Backend-Record-Type': 'shard'},
            [200])
        return [ShardRange.from_dict(sr) for sr in json.loads(resp.body)]

    def direct_container_op(self, func, account=None, container=None,
                            expect_failure=False):
        account = account if account else self.account
        container = container if container else self.container_name
        cpart, cnodes = self.container_ring.get_nodes(account, container)
        unexpected_responses = []
        results = {}
        for cnode in cnodes:
            try:
                results[cnode['id']] = func(cnode, cpart, account, container)
            except DirectClientException as err:
                if not expect_failure:
                    unexpected_responses.append((cnode, err))
            else:
                if expect_failure:
                    unexpected_responses.append((cnode, 'success'))
        if unexpected_responses:
            self.fail('Unexpected responses: %s' % unexpected_responses)
        return results

    def direct_get_container_shard_ranges(self, account=None, container=None,
                                          expect_failure=False):
        collector = ShardCollector()
        self.direct_container_op(
            collector, account, container, expect_failure)
        return collector.ranges

    def direct_delete_container(self, account=None, container=None,
                                expect_failure=False):
        self.direct_container_op(direct_client.direct_delete_container,
                                 account, container, expect_failure)

    def direct_head_container(self, account=None, container=None,
                              expect_failure=False):
        return self.direct_container_op(direct_client.direct_head_container,
                                        account, container, expect_failure)

    def direct_get_container(self, account=None, container=None,
                             expect_failure=False):
        return self.direct_container_op(direct_client.direct_get_container,
                                        account, container, expect_failure)

    def get_storage_dir(self, part, node, account=None, container=None):
        account = account or self.brain.account
        container = container or self.container_name
        server_type, config_number = get_server_number(
            (node['ip'], node['port']), self.ipport2server)
        assert server_type == 'container'
        repl_server = '%s-replicator' % server_type
        conf = utils.readconf(self.configs[repl_server][config_number],
                              section_name=repl_server)
        datadir = os.path.join(conf['devices'], node['device'], 'containers')
        container_hash = utils.hash_path(account, container)
        return (utils.storage_directory(datadir, part, container_hash),
                container_hash)

    def get_broker(self, part, node, account=None, container=None):
        container_dir, container_hash = self.get_storage_dir(
            part, node, account=account, container=container)
        db_file = os.path.join(container_dir, container_hash + '.db')
        self.assertTrue(get_db_files(db_file))  # sanity check
        return ContainerBroker(db_file)

    def categorize_container_dir_content(self, account=None, container=None):
        account = account or self.brain.account
        container = container or self.container_name
        part, nodes = self.brain.ring.get_nodes(account, container)
        storage_dirs = [
            self.get_storage_dir(part, node, account=account,
                                 container=container)[0]
            for node in nodes]
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
                if path.endswith('.db'):
                    hash_, epoch, ext = parse_db_filename(path)
                    if epoch:
                        result['shard_dbs'].append(path)
                    else:
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

    def assert_dict_contains(self, expected_items, actual_dict):
        ignored = set(expected_items) ^ set(actual_dict)
        filtered_actual = dict((k, actual_dict[k])
                               for k in actual_dict if k not in ignored)
        self.assertEqual(expected_items, filtered_actual)

    def assert_shard_ranges_contiguous(self, expected_number, shard_ranges,
                                       first_lower='', last_upper=''):
        if shard_ranges and isinstance(shard_ranges[0], ShardRange):
            actual_shard_ranges = sorted(shard_ranges)
        else:
            actual_shard_ranges = sorted([ShardRange.from_dict(d)
                                          for d in shard_ranges])
        self.assertLengthEqual(actual_shard_ranges, expected_number)
        if expected_number:
            with annotate_failure('Ranges %s.' % actual_shard_ranges):
                self.assertEqual(first_lower, actual_shard_ranges[0].lower_str)
                for x, y in zip(actual_shard_ranges, actual_shard_ranges[1:]):
                    self.assertEqual(x.upper, y.lower)
                self.assertEqual(last_upper, actual_shard_ranges[-1].upper_str)

    def assert_shard_range_equal(self, expected, actual, excludes=None):
        excludes = excludes or []
        expected_dict = dict(expected)
        actual_dict = dict(actual)
        for k in excludes:
            expected_dict.pop(k, None)
            actual_dict.pop(k, None)
        self.assertEqual(expected_dict, actual_dict)

    def assert_shard_range_lists_equal(self, expected, actual, excludes=None):
        self.assertEqual(len(expected), len(actual))
        for expected, actual in zip(expected, actual):
            self.assert_shard_range_equal(expected, actual, excludes=excludes)

    def assert_shard_range_state(self, expected_state, shard_ranges):
        if shard_ranges and not isinstance(shard_ranges[0], ShardRange):
            shard_ranges = [ShardRange.from_dict(data)
                            for data in shard_ranges]
        self.assertEqual([expected_state] * len(shard_ranges),
                         [sr.state for sr in shard_ranges])

    def assert_total_object_count(self, expected_object_count, shard_ranges):
        actual = sum([sr['object_count'] for sr in shard_ranges])
        self.assertEqual(expected_object_count, actual)

    def assert_container_listing(self, expected_listing):
        headers, actual_listing = client.get_container(
            self.url, self.token, self.container_name)
        self.assertIn('x-container-object-count', headers)
        expected_obj_count = len(expected_listing)
        self.assertEqual(expected_listing, [
            x['name'].encode('utf-8') for x in actual_listing])
        self.assertEqual(str(expected_obj_count),
                         headers['x-container-object-count'])
        return headers, actual_listing

    def assert_container_object_count(self, expected_obj_count):
        headers = client.head_container(
            self.url, self.token, self.container_name)
        self.assertIn('x-container-object-count', headers)
        self.assertEqual(str(expected_obj_count),
                         headers['x-container-object-count'])

    def assert_container_post_ok(self, meta_value):
        key = 'X-Container-Meta-Assert-Post-Works'
        headers = {key: meta_value}
        client.post_container(
            self.url, self.token, self.container_name, headers=headers)
        resp_headers = client.head_container(
            self.url, self.token, self.container_name)
        self.assertEqual(meta_value, resp_headers.get(key.lower()))

    def assert_container_post_fails(self, meta_value):
        key = 'X-Container-Meta-Assert-Post-Works'
        headers = {key: meta_value}
        with self.assertRaises(ClientException) as cm:
            client.post_container(
                self.url, self.token, self.container_name, headers=headers)
        self.assertEqual(404, cm.exception.http_status)

    def assert_container_delete_fails(self):
        with self.assertRaises(ClientException) as cm:
            client.delete_container(self.url, self.token, self.container_name)
        self.assertEqual(409, cm.exception.http_status)

    def assert_container_not_found(self):
        with self.assertRaises(ClientException) as cm:
            client.get_container(self.url, self.token, self.container_name)
        self.assertEqual(404, cm.exception.http_status)
        # check for headers leaking out while deleted
        resp_headers = cm.exception.http_response_headers
        self.assertNotIn('X-Container-Object-Count', resp_headers)
        self.assertNotIn('X-Container-Bytes-Used', resp_headers)
        self.assertNotIn('X-Timestamp', resp_headers)
        self.assertNotIn('X-PUT-Timestamp', resp_headers)

    def assert_container_has_shard_sysmeta(self):
        node_headers = self.direct_head_container()
        for node_id, headers in node_headers.items():
            with annotate_failure('%s in %s' % (node_id, node_headers.keys())):
                for k, v in headers.items():
                    if k.lower().startswith('x-container-sysmeta-shard'):
                        break
                else:
                    self.fail('No shard sysmeta found in %s' % headers)

    def assert_container_state(self, node, expected_state, num_shard_ranges):
        headers, shard_ranges = direct_client.direct_get_container(
            node, self.brain.part, self.account, self.container_name,
            headers={'X-Backend-Record-Type': 'shard'})
        self.assertEqual(num_shard_ranges, len(shard_ranges))
        self.assertIn('X-Backend-Sharding-State', headers)
        self.assertEqual(
            expected_state, headers['X-Backend-Sharding-State'])
        return [ShardRange.from_dict(sr) for sr in shard_ranges]

    def get_part_and_node_numbers(self, shard_range):
        """Return the partition and node numbers for a shard range."""
        part, nodes = self.brain.ring.get_nodes(
            shard_range.account, shard_range.container)
        return part, [n['id'] + 1 for n in nodes]

    def run_sharders(self, shard_ranges):
        """Run the sharder on partitions for given shard ranges."""
        if not isinstance(shard_ranges, (list, tuple, set)):
            shard_ranges = (shard_ranges,)
        partitions = ','.join(str(self.get_part_and_node_numbers(sr)[0])
                              for sr in shard_ranges)
        self.sharders.once(additional_args='--partitions=%s' % partitions)

    def run_sharder_sequentially(self, shard_range=None):
        """Run sharder node by node on partition for given shard range."""
        if shard_range:
            part, node_numbers = self.get_part_and_node_numbers(shard_range)
        else:
            part, node_numbers = self.brain.part, self.brain.node_numbers
        for node_number in node_numbers:
            self.sharders.once(number=node_number,
                               additional_args='--partitions=%s' % part)


class TestContainerShardingNonUTF8(BaseTestContainerSharding):
    def test_sharding_listing(self):
        # verify parameterised listing of a container during sharding
        all_obj_names = self._make_object_names(4 * self.max_shard_size)
        obj_names = all_obj_names[::2]
        self.put_objects(obj_names)
        # choose some names approx in middle of each expected shard range
        markers = [
            obj_names[i] for i in range(self.max_shard_size / 4,
                                        2 * self.max_shard_size,
                                        self.max_shard_size / 2)]

        def check_listing(objects, **params):
            qs = '&'.join(['%s=%s' % param for param in params.items()])
            headers, listing = client.get_container(
                self.url, self.token, self.container_name, query_string=qs)
            listing = [x['name'].encode('utf-8') for x in listing]
            if params.get('reverse'):
                marker = params.get('marker', ShardRange.MAX)
                end_marker = params.get('end_marker', ShardRange.MIN)
                expected = [o for o in objects if end_marker < o < marker]
                expected.reverse()
            else:
                marker = params.get('marker', ShardRange.MIN)
                end_marker = params.get('end_marker', ShardRange.MAX)
                expected = [o for o in objects if marker < o < end_marker]
            if 'limit' in params:
                expected = expected[:params['limit']]
            self.assertEqual(expected, listing)

        def check_listing_precondition_fails(**params):
            qs = '&'.join(['%s=%s' % param for param in params.items()])
            with self.assertRaises(ClientException) as cm:
                client.get_container(
                    self.url, self.token, self.container_name, query_string=qs)
            self.assertEqual(412, cm.exception.http_status)
            return cm.exception

        def do_listing_checks(objects):
            check_listing(objects)
            check_listing(objects, marker=markers[0], end_marker=markers[1])
            check_listing(objects, marker=markers[0], end_marker=markers[2])
            check_listing(objects, marker=markers[1], end_marker=markers[3])
            check_listing(objects, marker=markers[1], end_marker=markers[3],
                          limit=self.max_shard_size / 4)
            check_listing(objects, marker=markers[1], end_marker=markers[3],
                          limit=self.max_shard_size / 4)
            check_listing(objects, marker=markers[1], end_marker=markers[2],
                          limit=self.max_shard_size / 2)
            check_listing(objects, marker=markers[1], end_marker=markers[1])
            check_listing(objects, reverse=True)
            check_listing(objects, reverse=True, end_marker=markers[1])
            check_listing(objects, reverse=True, marker=markers[3],
                          end_marker=markers[1], limit=self.max_shard_size / 4)
            check_listing(objects, reverse=True, marker=markers[3],
                          end_marker=markers[1], limit=0)
            check_listing([], marker=markers[0], end_marker=markers[0])
            check_listing([], marker=markers[0], end_marker=markers[1],
                          reverse=True)
            check_listing(objects, prefix='obj')
            check_listing([], prefix='zzz')
            # delimiter
            headers, listing = client.get_container(
                self.url, self.token, self.container_name,
                query_string='delimiter=-')
            self.assertEqual([{'subdir': 'obj-'}], listing)

            limit = self.cluster_info['swift']['container_listing_limit']
            exc = check_listing_precondition_fails(limit=limit + 1)
            self.assertIn('Maximum limit', exc.http_response_content)
            exc = check_listing_precondition_fails(delimiter='ab')
            self.assertIn('Bad delimiter', exc.http_response_content)

        # sanity checks
        do_listing_checks(obj_names)

        # Shard the container
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        # First run the 'leader' in charge of scanning, which finds all shard
        # ranges and cleaves first two
        self.sharders.once(number=self.brain.node_numbers[0],
                           additional_args='--partitions=%s' % self.brain.part)
        # Then run sharder on other nodes which will also cleave first two
        # shard ranges
        for n in self.brain.node_numbers[1:]:
            self.sharders.once(
                number=n, additional_args='--partitions=%s' % self.brain.part)

        # sanity check shard range states
        for node in self.brain.nodes:
            self.assert_container_state(node, 'sharding', 4)
        shard_ranges = self.get_container_shard_ranges()
        self.assertLengthEqual(shard_ranges, 4)
        self.assert_shard_range_state(ShardRange.CLEAVED, shard_ranges[:2])
        self.assert_shard_range_state(ShardRange.CREATED, shard_ranges[2:])

        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()  # confirm no sysmeta deleted
        self.assert_container_post_ok('sharding')
        do_listing_checks(obj_names)

        # put some new objects spread through entire namespace
        new_obj_names = all_obj_names[1::4]
        self.put_objects(new_obj_names)

        # new objects that fell into the first two cleaved shard ranges are
        # reported in listing, new objects in the yet-to-be-cleaved shard
        # ranges are not yet included in listing
        exp_obj_names = [o for o in obj_names + new_obj_names
                         if o <= shard_ranges[1].upper]
        exp_obj_names += [o for o in obj_names
                          if o > shard_ranges[1].upper]
        exp_obj_names.sort()
        do_listing_checks(exp_obj_names)

        # run all the sharders again and the last two shard ranges get cleaved
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        for node in self.brain.nodes:
            self.assert_container_state(node, 'sharded', 4)
        shard_ranges = self.get_container_shard_ranges()
        self.assert_shard_range_state(ShardRange.ACTIVE, shard_ranges)

        exp_obj_names = obj_names + new_obj_names
        exp_obj_names.sort()
        do_listing_checks(exp_obj_names)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')

        # delete original objects
        self.delete_objects(obj_names)
        do_listing_checks(new_obj_names)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')


class TestContainerShardingUTF8(TestContainerShardingNonUTF8):
    def _make_object_names(self, number):
        # override default with names that include non-ascii chars
        name_length = self.cluster_info['swift']['max_object_name_length']
        obj_names = []
        for x in range(number):
            name = (u'obj-\u00e4\u00ea\u00ec\u00f2\u00fb-%04d' % x)
            name = name.encode('utf8').ljust(name_length, 'o')
            obj_names.append(name)
        return obj_names

    def _setup_container_name(self):
        # override default with max length name that includes non-ascii chars
        super(TestContainerShardingUTF8, self)._setup_container_name()
        name_length = self.cluster_info['swift']['max_container_name_length']
        cont_name = self.container_name + u'-\u00e4\u00ea\u00ec\u00f2\u00fb'
        self.conainer_name = cont_name.encode('utf8').ljust(name_length, 'x')


class TestContainerSharding(BaseTestContainerSharding):
    def _test_sharded_listing(self, run_replicators=False):
        obj_names = self._make_object_names(self.max_shard_size)
        self.put_objects(obj_names)

        # Verify that we start out with normal DBs, no shards
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['normal_dbs'], 3)
        self.assertLengthEqual(found['shard_dbs'], 0)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual('unsharded', broker.get_db_state())
            self.assertLengthEqual(broker.get_shard_ranges(), 0)

        headers, pre_sharding_listing = client.get_container(
            self.url, self.token, self.container_name)
        self.assertEqual(obj_names, [x['name'].encode('utf-8')
                                     for x in pre_sharding_listing])  # sanity

        # Shard it
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        pre_sharding_headers = client.head_container(
            self.url, self.admin_token, self.container_name)
        self.assertEqual('True',
                         pre_sharding_headers.get('x-container-sharding'))

        # Only run the one in charge of scanning
        self.sharders.once(number=self.brain.node_numbers[0],
                           additional_args='--partitions=%s' % self.brain.part)

        # Verify that we have one sharded db -- though the other normal DBs
        # received the shard ranges that got defined
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 1)
        broker = self.get_broker(self.brain.part, self.brain.nodes[0])
        # sanity check - the shard db is on replica 0
        self.assertEqual(found['shard_dbs'][0], broker.db_file)
        self.assertIs(True, broker.is_root_container())
        self.assertEqual('sharded', broker.get_db_state())
        orig_root_shard_ranges = [dict(sr) for sr in broker.get_shard_ranges()]
        self.assertLengthEqual(orig_root_shard_ranges, 2)
        self.assert_total_object_count(len(obj_names), orig_root_shard_ranges)
        self.assert_shard_ranges_contiguous(2, orig_root_shard_ranges)
        self.assertEqual([ShardRange.ACTIVE, ShardRange.ACTIVE],
                         [sr['state'] for sr in orig_root_shard_ranges])
        self.direct_delete_container(expect_failure=True)

        self.assertLengthEqual(found['normal_dbs'], 2)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual('unsharded', broker.get_db_state())
            shard_ranges = [dict(sr) for sr in broker.get_shard_ranges()]
            self.assertEqual([ShardRange.CREATED, ShardRange.CREATED],
                             [sr['state'] for sr in shard_ranges])
            # the sharded db had shard range meta_timestamps and state updated
            # during cleaving, so we do not expect those to be equal on other
            # nodes
            self.assert_shard_range_lists_equal(
                orig_root_shard_ranges, shard_ranges,
                excludes=['meta_timestamp', 'state', 'state_timestamp'])

        if run_replicators:
            Manager(['container-replicator']).once()
            # replication doesn't change the db file names
            found = self.categorize_container_dir_content()
            self.assertLengthEqual(found['shard_dbs'], 1)
            self.assertLengthEqual(found['normal_dbs'], 2)

        # Now that everyone has shard ranges, run *everyone*
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)

        # Verify that we only have shard dbs now
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 0)
        # Shards stayed the same
        for db_file in found['shard_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual('sharded', broker.get_db_state())
            # Well, except for meta_timestamps, since the shards each reported
            self.assert_shard_range_lists_equal(
                orig_root_shard_ranges, broker.get_shard_ranges(),
                excludes=['meta_timestamp', 'state_timestamp'])
            for orig, updated in zip(orig_root_shard_ranges,
                                     broker.get_shard_ranges()):
                self.assertGreaterEqual(updated.state_timestamp,
                                        orig['state_timestamp'])
                self.assertGreaterEqual(updated.meta_timestamp,
                                        orig['meta_timestamp'])

        # Check that entire listing is available
        headers, actual_listing = self.assert_container_listing(obj_names)
        # ... and check some other container properties
        self.assertEqual(headers['last-modified'],
                         pre_sharding_headers['last-modified'])

        # It even works in reverse!
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name,
                                                query_string='reverse=on')
        self.assertEqual(pre_sharding_listing[::-1], listing)

        # Now put some new objects into first shard, taking its count to
        # 3 shard ranges' worth
        more_obj_names = [
            'beta%03d' % x for x in range(self.max_shard_size)]
        self.put_objects(more_obj_names)

        # The listing includes new objects...
        headers, listing = self.assert_container_listing(
            more_obj_names + obj_names)
        self.assertEqual(pre_sharding_listing, listing[len(more_obj_names):])

        # ...but root object count is out of date until the sharders run and
        # update the root
        self.assert_container_object_count(len(obj_names))

        # run sharders on the shard to get root updated
        shard_1 = ShardRange.from_dict(orig_root_shard_ranges[0])
        self.run_sharders(shard_1)
        self.assert_container_object_count(len(more_obj_names + obj_names))

        # we've added objects enough that we need to shard the first shard
        # *again* into three new sub-shards, but nothing happens until the root
        # leader identifies shard candidate...
        root_shard_ranges = self.direct_get_container_shard_ranges()
        for node, (hdrs, root_shards) in root_shard_ranges.items():
            self.assertLengthEqual(root_shards, 2)
            with annotate_failure('node %s. ' % node):
                self.assertEqual(
                    [ShardRange.ACTIVE] * 2,
                    [sr['state'] for sr in root_shards])
                # orig shards 0, 1 should be contiguous
                self.assert_shard_ranges_contiguous(2, root_shards)

        # Now run the root leader to identify shard candidate...while one of
        # the shard container servers is down
        shard_1_part, shard_1_nodes = self.get_part_and_node_numbers(shard_1)
        self.brain.servers.stop(number=shard_1_nodes[2])
        self.sharders.once(number=self.brain.node_numbers[0],
                           additional_args='--partitions=%s' % self.brain.part)

        # ... so third replica of first shard state is not moved to sharding
        found_for_shard = self.categorize_container_dir_content(
            shard_1.account, shard_1.container)
        self.assertLengthEqual(found_for_shard['normal_dbs'], 3)
        self.assertEqual(
            [ShardRange.SHARDING, ShardRange.SHARDING, ShardRange.ACTIVE],
            [ContainerBroker(db_file).get_own_shard_range().state
             for db_file in found_for_shard['normal_dbs']])

        # ...then run first cycle of first shard sharders in order, leader
        # first, to get to predictable state where all nodes have cleaved 2 out
        # of 3 ranges...starting with first two nodes
        for node_number in shard_1_nodes[:2]:
            self.sharders.once(
                number=node_number,
                additional_args='--partitions=%s' % shard_1_part)

        # ... first two replicas start sharding to sub-shards
        found_for_shard = self.categorize_container_dir_content(
            shard_1.account, shard_1.container)
        self.assertLengthEqual(found_for_shard['shard_dbs'], 2)
        for db_file in found_for_shard['shard_dbs'][:2]:
            broker = ContainerBroker(db_file)
            with annotate_failure('shard db file %s. ' % db_file):
                self.assertIs(False, broker.is_root_container())
                self.assertEqual('sharding', broker.get_db_state())
                self.assertEqual(
                    ShardRange.SHARDING, broker.get_own_shard_range().state)
                shard_shards = broker.get_shard_ranges()
                self.assertEqual(
                    [ShardRange.CLEAVED, ShardRange.CLEAVED,
                     ShardRange.CREATED],
                    [sr.state for sr in shard_shards])
                self.assert_shard_ranges_contiguous(
                    3, shard_shards,
                    first_lower=orig_root_shard_ranges[0]['lower'],
                    last_upper=orig_root_shard_ranges[0]['upper'])

        # but third replica still has no idea it should be sharding
        self.assertLengthEqual(found_for_shard['normal_dbs'], 3)
        self.assertEqual(
            ShardRange.ACTIVE,
            ContainerBroker(
                found_for_shard['normal_dbs'][2]).get_own_shard_range().state)

        # ...but once sharder runs on third replica it will learn its state;
        # note that any root replica on the stopped container server also won't
        # know about the shards being in sharding state, so leave that server
        # stopped for now so that shard fetches its state from an up-to-date
        # root replica
        self.sharders.once(
            number=shard_1_nodes[2],
            additional_args='--partitions=%s' % shard_1_part)

        # third replica is sharding but has no sub-shard ranges yet...
        found_for_shard = self.categorize_container_dir_content(
            shard_1.account, shard_1.container)
        self.assertLengthEqual(found_for_shard['shard_dbs'], 2)
        self.assertLengthEqual(found_for_shard['normal_dbs'], 3)
        broker = ContainerBroker(found_for_shard['normal_dbs'][2])
        self.assertEqual('unsharded', broker.get_db_state())
        self.assertEqual(
            ShardRange.SHARDING, broker.get_own_shard_range().state)
        self.assertFalse(broker.get_shard_ranges())

        # ...until sub-shard ranges are replicated from another shard replica;
        # there may also be a sub-shard replica missing so run replicators on
        # all nodes to fix that if necessary
        self.brain.servers.start(number=shard_1_nodes[2])
        self.replicators.once()

        # now run sharder again on third replica
        self.sharders.once(
            number=shard_1_nodes[2],
            additional_args='--partitions=%s' % shard_1_part)

        # check original first shard range state and sub-shards - all replicas
        # should now be in consistent state
        found_for_shard = self.categorize_container_dir_content(
            shard_1.account, shard_1.container)
        self.assertLengthEqual(found_for_shard['shard_dbs'], 3)
        self.assertLengthEqual(found_for_shard['normal_dbs'], 3)
        for db_file in found_for_shard['shard_dbs']:
            broker = ContainerBroker(db_file)
            with annotate_failure('shard db file %s. ' % db_file):
                self.assertIs(False, broker.is_root_container())
                self.assertEqual('sharding', broker.get_db_state())
                self.assertEqual(
                    ShardRange.SHARDING, broker.get_own_shard_range().state)
                shard_shards = broker.get_shard_ranges()
                self.assertEqual(
                    [ShardRange.CLEAVED, ShardRange.CLEAVED,
                     ShardRange.CREATED],
                    [sr.state for sr in shard_shards])
                self.assert_shard_ranges_contiguous(
                    3, shard_shards,
                    first_lower=orig_root_shard_ranges[0]['lower'],
                    last_upper=orig_root_shard_ranges[0]['upper'])

        # check third sub-shard is in created state
        sub_shard = shard_shards[2]
        found_for_sub_shard = self.categorize_container_dir_content(
            sub_shard.account, sub_shard.container)
        self.assertFalse(found_for_sub_shard['shard_dbs'])
        self.assertLengthEqual(found_for_sub_shard['normal_dbs'], 3)
        for db_file in found_for_sub_shard['normal_dbs']:
            broker = ContainerBroker(db_file)
            with annotate_failure('sub shard db file %s. ' % db_file):
                self.assertIs(False, broker.is_root_container())
                self.assertEqual('unsharded', broker.get_db_state())
                self.assertEqual(
                    ShardRange.CREATED, broker.get_own_shard_range().state)
                self.assertFalse(broker.get_shard_ranges())

        # check root shard ranges
        root_shard_ranges = self.direct_get_container_shard_ranges()
        for node, (hdrs, root_shards) in root_shard_ranges.items():
            self.assertLengthEqual(root_shards, 5)
            with annotate_failure('node %s. ' % node):
                # shard ranges are sorted by upper, state, lower, so expect:
                # sub-shards, orig shard 0, orig shard 1
                self.assertEqual(
                    [ShardRange.CLEAVED, ShardRange.CLEAVED,
                     ShardRange.CREATED, ShardRange.SHARDING,
                     ShardRange.ACTIVE],
                    [sr['state'] for sr in root_shards])
                # sub-shards 0, 1, 2, orig shard 1 should be contiguous
                self.assert_shard_ranges_contiguous(
                    4, root_shards[:3] + root_shards[4:])
                # orig shards 0, 1 should be contiguous
                self.assert_shard_ranges_contiguous(2, root_shards[3:])

        self.assert_container_listing(more_obj_names + obj_names)
        self.assert_container_object_count(len(more_obj_names + obj_names))

        # add another object that lands in the first of the new sub-shards
        self.put_objects(['alpha'])

        # check that alpha object is in the first new shard
        shard_listings = self.direct_get_container(shard_shards[0].account,
                                                   shard_shards[0].container)
        for node, (hdrs, listing) in shard_listings.items():
            with annotate_failure(node):
                self.assertIn('alpha', [o['name'] for o in listing])
        self.assert_container_listing(['alpha'] + more_obj_names + obj_names)
        # Run sharders again so things settle.
        self.run_sharders(shard_1)

        # check original first shard range shards
        for db_file in found_for_shard['shard_dbs']:
            broker = ContainerBroker(db_file)
            with annotate_failure('shard db file %s. ' % db_file):
                self.assertIs(False, broker.is_root_container())
                self.assertEqual('sharded', broker.get_db_state())
                self.assertEqual(
                    [ShardRange.ACTIVE] * 3,
                    [sr.state for sr in broker.get_shard_ranges()])
        # check root shard ranges
        root_shard_ranges = self.direct_get_container_shard_ranges()
        for node, (hdrs, root_shards) in root_shard_ranges.items():
            # old first shard range should have been deleted
            self.assertLengthEqual(root_shards, 4)
            with annotate_failure('node %s. ' % node):
                self.assertEqual(
                    [ShardRange.ACTIVE] * 4,
                    [sr['state'] for sr in root_shards])
                self.assert_shard_ranges_contiguous(4, root_shards)

        headers, final_listing = self.assert_container_listing(
            ['alpha'] + more_obj_names + obj_names)

        # check root
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 0)
        new_shard_ranges = None
        for db_file in found['shard_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual('sharded', broker.get_db_state())
            if new_shard_ranges is None:
                new_shard_ranges = broker.get_shard_ranges(
                    include_deleted=True)
                self.assertLengthEqual(new_shard_ranges, 5)
                # Second half is still there, and unchanged
                self.assertIn(
                    dict(orig_root_shard_ranges[1], meta_timestamp=None,
                         state_timestamp=None),
                    [dict(sr, meta_timestamp=None, state_timestamp=None)
                     for sr in new_shard_ranges])
                # But the first half split in three, then deleted
                by_name = {sr.name: sr for sr in new_shard_ranges}
                self.assertIn(orig_root_shard_ranges[0]['name'], by_name)
                old_shard_range = by_name.pop(
                    orig_root_shard_ranges[0]['name'])
                self.assertTrue(old_shard_range.deleted)
                self.assert_shard_ranges_contiguous(4, by_name.values())
            else:
                # Everyone's on the same page. Well, except for
                # meta_timestamps, since the shards each reported
                other_shard_ranges = broker.get_shard_ranges(
                    include_deleted=True)
                self.assert_shard_range_lists_equal(
                    new_shard_ranges, other_shard_ranges,
                    excludes=['meta_timestamp', 'state_timestamp'])
                for orig, updated in zip(orig_root_shard_ranges,
                                         other_shard_ranges):
                    self.assertGreaterEqual(updated.meta_timestamp,
                                            orig['meta_timestamp'])

        self.assert_container_delete_fails()

        for obj in final_listing:
            client.delete_object(
                self.url, self.token, self.container_name, obj['name'])

        # the objects won't be listed anymore
        self.assert_container_listing([])
        # but root container stats will not yet be aware of the deletions
        self.assert_container_delete_fails()

        # One server was down while the shard sharded its first two sub-shards,
        # so there may be undeleted handoff db(s) for sub-shard(s) that were
        # not fully replicated; run replicators now to clean up so they no
        # longer report bogus stats to root.
        self.replicators.once()

        # Run sharder so that shard containers update the root. Do not run
        # sharder on root container because that triggers shrinks which can
        # cause root object count to temporarily be non-zero and prevent the
        # final delete.
        self.run_sharders(self.get_container_shard_ranges())
        # then root is empty and can be deleted
        self.assert_container_listing([])
        self.assert_container_object_count(0)
        client.delete_container(self.url, self.token, self.container_name)

    def test_sharded_listing_no_replicators(self):
        self._test_sharded_listing()

    def test_sharded_listing_with_replicators(self):
        self._test_sharded_listing(run_replicators=True)

    def test_async_pendings(self):
        obj_names = self._make_object_names(self.max_shard_size * 2)

        # There are some updates *everyone* gets
        self.put_objects(obj_names[::5])
        # But roll some outages so each container only get ~2/5 more object
        # records i.e. total of 3/5 updates per container; and async pendings
        # pile up
        for i, n in enumerate(self.brain.node_numbers, start=1):
            self.brain.servers.stop(number=n)
            self.put_objects(obj_names[i::5])
            self.brain.servers.start(number=n)

        # But there are also 1/5 updates *no one* gets
        self.brain.servers.stop()
        self.put_objects(obj_names[4::5])
        self.brain.servers.start()

        # Shard it
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        headers = client.head_container(self.url, self.admin_token,
                                        self.container_name)
        self.assertEqual('True', headers.get('x-container-sharding'))

        # sanity check
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 0)
        self.assertLengthEqual(found['normal_dbs'], 3)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            self.assertEqual(len(obj_names) * 3 // 5,
                             broker.get_info()['object_count'])

        # Only run the 'leader' in charge of scanning.
        # Each container has ~2 * max * 3/5 objects
        # which are distributed from obj000 to obj<2 * max - 1>,
        # so expect 3 shard ranges to be found: the first two will be complete
        # shards with max/2 objects and lower/upper bounds spaced by approx:
        #     (2 * max - 1)/(2 * max * 3/5) * (max/2) =~ 5/6 * max
        #
        # Note that during this shard cycle the leader replicates to other
        # nodes so they will end up with ~2 * max * 4/5 objects.
        self.sharders.once(number=self.brain.node_numbers[0],
                           additional_args='--partitions=%s' % self.brain.part)

        # Verify that we have one shard db -- though the other normal DBs
        # received the shard ranges that got defined
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 1)
        node_index_zero_db = found['shard_dbs'][0]
        broker = ContainerBroker(node_index_zero_db)
        self.assertIs(True, broker.is_root_container())
        self.assertEqual(SHARDING, broker.get_db_state())
        expected_shard_ranges = broker.get_shard_ranges()
        self.assertLengthEqual(expected_shard_ranges, 3)
        self.assertEqual(
            [ShardRange.CLEAVED, ShardRange.CLEAVED, ShardRange.CREATED],
            [sr.state for sr in expected_shard_ranges])

        # Still have all three big DBs -- we've only cleaved 2 of the 3 shard
        # ranges that got defined
        self.assertLengthEqual(found['normal_dbs'], 3)
        db_states = []
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertIs(True, broker.is_root_container())
            db_states.append(broker.get_db_state())
            # the sharded db had shard range meta_timestamps updated during
            # cleaving, so we do not expect those to be equal on other nodes
            self.assert_shard_range_lists_equal(
                expected_shard_ranges, broker.get_shard_ranges(),
                excludes=['meta_timestamp', 'state_timestamp', 'state'])
            self.assertEqual(len(obj_names) * 3 // 5,
                             broker.get_info()['object_count'])
        self.assertEqual([SHARDING, UNSHARDED, UNSHARDED], sorted(db_states))

        # Run the other sharders so we're all in (roughly) the same state
        for n in self.brain.node_numbers[1:]:
            self.sharders.once(
                number=n,
                additional_args='--partitions=%s' % self.brain.part)
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 3)
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertEqual(SHARDING, broker.get_db_state())
            # no new rows
            self.assertEqual(len(obj_names) * 3 // 5,
                             broker.get_info()['object_count'])

        # Run updaters to clear the async pendings
        Manager(['object-updater']).once()

        # Our "big" dbs didn't take updates
        for db_file in found['normal_dbs']:
            broker = ContainerBroker(db_file)
            self.assertEqual(len(obj_names) * 3 // 5,
                             broker.get_info()['object_count'])

        # confirm that the async pending updates got redirected to the shards
        for sr in expected_shard_ranges:
            shard_listings = self.direct_get_container(sr.account,
                                                       sr.container)
            for node, (hdrs, listing) in shard_listings.items():
                shard_listing_names = [o['name'] for o in listing]
                for obj in obj_names[4::5]:
                    if obj in sr:
                        self.assertIn(obj, shard_listing_names)
                    else:
                        self.assertNotIn(obj, shard_listing_names)

        # The entire listing is not yet available - we have two cleaved shard
        # ranges, complete with async updates, but for the remainder of the
        # namespace only what landed in the original container
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name)
        start_listing = [
            o for o in obj_names if o <= expected_shard_ranges[1].upper]
        self.assertEqual(
            [x['name'].encode('utf-8') for x in listing[:len(start_listing)]],
            start_listing)
        # we can't assert much about the remaining listing, other than that
        # there should be something
        self.assertTrue(
            [x['name'].encode('utf-8') for x in listing[len(start_listing):]])
        self.assertIn('x-container-object-count', headers)
        self.assertEqual(str(len(listing)),
                         headers['x-container-object-count'])
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name,
                                                query_string='reverse=on')
        self.assertEqual([x['name'].encode('utf-8')
                          for x in listing[-len(start_listing):]],
                         list(reversed(start_listing)))
        self.assertIn('x-container-object-count', headers)
        self.assertEqual(str(len(listing)),
                         headers['x-container-object-count'])
        self.assertTrue(
            [x['name'].encode('utf-8') for x in listing[:-len(start_listing)]])

        # Run the sharders again to get everything to settle
        self.sharders.once()
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 0)
        # now all shards have been cleaved we should get the complete listing
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name)
        self.assertEqual([x['name'].encode('utf-8') for x in listing],
                         obj_names)

    def test_shrinking(self):
        int_client = self.make_internal_client()

        def check_node_data(node_data, exp_hdrs, exp_obj_count, exp_shards):
            hdrs, range_data = node_data
            self.assert_dict_contains(exp_hdrs, hdrs)
            self.assert_shard_ranges_contiguous(exp_shards, range_data)
            self.assert_total_object_count(exp_obj_count, range_data)

        def check_shard_nodes_data(node_data, expected_state='unsharded',
                                   expected_shards=0, exp_obj_count=0):
            # checks that shard range is consistent on all nodes
            root_path = '%s/%s' % (self.account, self.container_name)
            exp_shard_hdrs = {'X-Container-Sysmeta-Shard-Root': root_path,
                              'X-Backend-Sharding-State': expected_state}
            object_counts = []
            bytes_used = []
            for node_id, node_data in node_data.items():
                with annotate_failure('Node id %s.' % node_id):
                    check_node_data(
                        node_data, exp_shard_hdrs, exp_obj_count,
                        expected_shards)
                hdrs = node_data[0]
                object_counts.append(int(hdrs['X-Container-Object-Count']))
                bytes_used.append(int(hdrs['X-Container-Bytes-Used']))
            if len(set(object_counts)) != 1:
                self.fail('Inconsistent object counts: %s' % object_counts)
            if len(set(bytes_used)) != 1:
                self.fail('Inconsistent bytes used: %s' % bytes_used)
            return object_counts[0], bytes_used[0]

        repeat = [0]

        def do_shard_then_shrink():
            repeat[0] += 1
            obj_names = ['obj-%s-%03d' % (repeat[0], x)
                         for x in range(self.max_shard_size)]
            self.put_objects(obj_names)
            # these two object names will fall at start of first shard range...
            alpha = 'alpha-%s' % repeat[0]
            beta = 'beta-%s' % repeat[0]

            # Enable sharding
            client.post_container(
                self.url, self.admin_token, self.container_name,
                headers={'X-Container-Sharding': 'on'})

            # sanity check
            self.assert_container_listing(obj_names)

            # Only run the one in charge of scanning
            self.sharders.once(
                number=self.brain.node_numbers[0],
                additional_args='--partitions=%s' % self.brain.part)

            # check root container
            root_nodes_data = self.direct_get_container_shard_ranges()
            self.assertEqual(3, len(root_nodes_data))

            # nodes on which sharder has not run are still in unsharded state
            # but have had shard ranges replicated to them
            exp_obj_count = len(obj_names)
            exp_hdrs = {'X-Backend-Sharding-State': 'unsharded',
                        'X-Container-Object-Count': str(exp_obj_count)}
            node_id = self.brain.node_numbers[1] - 1
            check_node_data(
                root_nodes_data[node_id], exp_hdrs, exp_obj_count, 2)
            node_id = self.brain.node_numbers[2] - 1
            check_node_data(
                root_nodes_data[node_id], exp_hdrs, exp_obj_count, 2)

            # only one that ran sharder is in sharded state
            exp_hdrs['X-Backend-Sharding-State'] = 'sharded'
            node_id = self.brain.node_numbers[0] - 1
            check_node_data(
                root_nodes_data[node_id], exp_hdrs, exp_obj_count, 2)

            orig_range_data = root_nodes_data[node_id][1]
            orig_shard_ranges = [ShardRange.from_dict(r)
                                 for r in orig_range_data]

            # check first shard
            shard_nodes_data = self.direct_get_container_shard_ranges(
                orig_shard_ranges[0].account, orig_shard_ranges[0].container)
            obj_count, bytes_used = check_shard_nodes_data(shard_nodes_data)
            total_shard_object_count = obj_count

            # check second shard
            shard_nodes_data = self.direct_get_container_shard_ranges(
                orig_shard_ranges[1].account, orig_shard_ranges[1].container)
            obj_count, bytes_used = check_shard_nodes_data(shard_nodes_data)
            total_shard_object_count += obj_count
            self.assertEqual(exp_obj_count, total_shard_object_count)

            # Now that everyone has shard ranges, run *everyone*
            self.sharders.once(
                additional_args='--partitions=%s' % self.brain.part)

            # all root container nodes should now be in sharded state
            root_nodes_data = self.direct_get_container_shard_ranges()
            self.assertEqual(3, len(root_nodes_data))
            for node_id, node_data in root_nodes_data.items():
                with annotate_failure('Node id %s.' % node_id):
                    check_node_data(node_data, exp_hdrs, exp_obj_count, 2)

            # run updaters to update .sharded account; shard containers have
            # not updated account since having objects replicated to them
            self.updaters.once()
            shard_cont_count, shard_obj_count = int_client.get_account_info(
                orig_shard_ranges[0].account, [204])
            self.assertEqual(2 * repeat[0], shard_cont_count)
            # the shards account should always have zero object count to avoid
            # double accounting
            self.assertEqual(0, shard_obj_count)

            # checking the listing also refreshes proxy container info cache so
            # that the proxy becomes aware that container is sharded and will
            # now look up the shard target for subsequent updates
            self.assert_container_listing(obj_names)

            # delete objects from first shard range
            first_shard_objects = [obj_name for obj_name in obj_names
                                   if obj_name <= orig_shard_ranges[0].upper]
            for obj in first_shard_objects:
                client.delete_object(
                    self.url, self.token, self.container_name, obj)
                with self.assertRaises(ClientException):
                    client.get_object(
                        self.url, self.token, self.container_name, obj)

            second_shard_objects = [obj_name for obj_name in obj_names
                                    if obj_name > orig_shard_ranges[1].lower]
            self.assert_container_listing(second_shard_objects)

            self.put_objects([alpha])
            second_shard_objects = [obj_name for obj_name in obj_names
                                    if obj_name > orig_shard_ranges[1].lower]
            self.assert_container_listing([alpha] + second_shard_objects)

            # while container servers are down, but proxy has container info in
            # cache from recent listing, put another object; this update will
            # lurk in async pending until the updaters run again; because all
            # the root container servers are down and therefore cannot respond
            # to a GET for a redirect target, the object update will default to
            # being targeted at the root container
            self.stop_container_servers()
            self.put_objects([beta])
            self.brain.servers.start()
            async_pendings = self.gather_async_pendings(
                self.get_all_object_nodes())
            num_container_replicas = len(self.brain.nodes)
            num_obj_replicas = self.policy.object_ring.replica_count
            expected_num_updates = num_container_updates(
                num_container_replicas, quorum_size(num_container_replicas),
                num_obj_replicas, self.policy.quorum)
            expected_num_pendings = min(expected_num_updates, num_obj_replicas)
            # sanity check
            with annotate_failure('policy %s. ' % self.policy):
                self.assertLengthEqual(async_pendings, expected_num_pendings)

            # root object count is not updated...
            self.assert_container_object_count(len(obj_names))
            self.assert_container_listing([alpha] + second_shard_objects)
            root_nodes_data = self.direct_get_container_shard_ranges()
            self.assertEqual(3, len(root_nodes_data))
            for node_id, node_data in root_nodes_data.items():
                with annotate_failure('Node id %s.' % node_id):
                    check_node_data(node_data, exp_hdrs, exp_obj_count, 2)
                range_data = node_data[1]
                self.assert_shard_range_lists_equal(
                    orig_range_data, range_data,
                    excludes=['meta_timestamp', 'state_timestamp'])

            # ...until the sharders run and update root
            self.run_sharders(orig_shard_ranges[0])
            exp_obj_count = len(second_shard_objects) + 1
            self.assert_container_object_count(exp_obj_count)
            self.assert_container_listing([alpha] + second_shard_objects)

            # root sharder finds donor, acceptor pair and pushes changes
            self.sharders.once(
                additional_args='--partitions=%s' % self.brain.part)
            self.assert_container_listing([alpha] + second_shard_objects)
            # run sharder on donor to shrink and replicate to acceptor
            self.run_sharders(orig_shard_ranges[0])
            self.assert_container_listing([alpha] + second_shard_objects)
            # run sharder on acceptor to update root with stats
            self.run_sharders(orig_shard_ranges[1])
            self.assert_container_listing([alpha] + second_shard_objects)
            self.assert_container_object_count(len(second_shard_objects) + 1)

            # check root container
            root_nodes_data = self.direct_get_container_shard_ranges()
            self.assertEqual(3, len(root_nodes_data))
            exp_hdrs['X-Container-Object-Count'] = str(exp_obj_count)
            for node_id, node_data in root_nodes_data.items():
                with annotate_failure('Node id %s.' % node_id):
                    # NB now only *one* shard range in root
                    check_node_data(node_data, exp_hdrs, exp_obj_count, 1)

            # the acceptor shard is intact..
            shard_nodes_data = self.direct_get_container_shard_ranges(
                orig_shard_ranges[1].account, orig_shard_ranges[1].container)
            obj_count, bytes_used = check_shard_nodes_data(shard_nodes_data)
            # all objects should now be in this shard
            self.assertEqual(exp_obj_count, obj_count)

            # the donor shard is also still intact
            donor = orig_shard_ranges[0]
            shard_nodes_data = self.direct_get_container_shard_ranges(
                donor.account, donor.container)
            # the donor's shard range will have the acceptor's projected stats
            obj_count, bytes_used = check_shard_nodes_data(
                shard_nodes_data, expected_state='sharded', expected_shards=1,
                exp_obj_count=len(second_shard_objects) + 1)
            # but the donor is empty and so reports zero stats
            self.assertEqual(0, obj_count)
            self.assertEqual(0, bytes_used)
            # check the donor own shard range state
            part, nodes = self.brain.ring.get_nodes(
                donor.account, donor.container)
            for node in nodes:
                with annotate_failure(node):
                    broker = self.get_broker(
                        part, node, donor.account, donor.container)
                    own_sr = broker.get_own_shard_range()
                    self.assertEqual(ShardRange.SHARDED, own_sr.state)
                    self.assertTrue(own_sr.deleted)

            # delete all the second shard's object apart from 'alpha'
            for obj in second_shard_objects:
                client.delete_object(
                    self.url, self.token, self.container_name, obj)

            self.assert_container_listing([alpha])

            # runs sharders so second range shrinks away, requires up to 3
            # cycles
            self.sharders.once()  # shard updates root stats
            self.assert_container_listing([alpha])
            self.sharders.once()  # root finds shrinkable shard
            self.assert_container_listing([alpha])
            self.sharders.once()  # shards shrink themselves
            self.assert_container_listing([alpha])

            # the second shard range has sharded and is empty
            shard_nodes_data = self.direct_get_container_shard_ranges(
                orig_shard_ranges[1].account, orig_shard_ranges[1].container)
            check_shard_nodes_data(
                shard_nodes_data, expected_state='sharded', expected_shards=1,
                exp_obj_count=1)

            # check root container
            root_nodes_data = self.direct_get_container_shard_ranges()
            self.assertEqual(3, len(root_nodes_data))
            exp_hdrs = {'X-Backend-Sharding-State': 'collapsed',
                        # just the alpha object
                        'X-Container-Object-Count': '1'}
            for node_id, node_data in root_nodes_data.items():
                with annotate_failure('Node id %s.' % node_id):
                    # NB now no shard ranges in root
                    check_node_data(node_data, exp_hdrs, 0, 0)

            # delete the alpha object
            client.delete_object(
                self.url, self.token, self.container_name, alpha)
            # should now be able to delete the *apparently* empty container
            client.delete_container(self.url, self.token, self.container_name)
            self.assert_container_not_found()
            self.direct_head_container(expect_failure=True)

            # and the container stays deleted even after sharders run and shard
            # send updates
            self.sharders.once()
            self.assert_container_not_found()
            self.direct_head_container(expect_failure=True)

            # now run updaters to deal with the async pending for the beta
            # object
            self.updaters.once()
            # and the container is revived!
            self.assert_container_listing([beta])

            # finally, clear out the container
            client.delete_object(
                self.url, self.token, self.container_name, beta)

        do_shard_then_shrink()
        # repeat from starting point of a collapsed and previously deleted
        # container
        do_shard_then_shrink()

    def _setup_replication_scenario(self, num_shards, extra_objs=('alpha',)):
        # Get cluster to state where 2 replicas are sharding or sharded but 3rd
        # replica is unsharded and has an object that the first 2 are missing.

        # put objects while all servers are up
        obj_names = self._make_object_names(
            num_shards * self.max_shard_size / 2)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        node_numbers = self.brain.node_numbers

        # run replicators first time to get sync points set
        self.replicators.once()

        # stop the leader node and one other server
        self.stop_container_servers(slice(0, 2))

        # ...then put one more object in first shard range namespace
        self.put_objects(extra_objs)

        # start leader and first other server, stop third server
        for number in node_numbers[:2]:
            self.brain.servers.start(number=number)
        self.brain.servers.stop(number=node_numbers[2])
        self.assert_container_listing(obj_names)  # sanity check

        # shard the container - first two shard ranges are cleaved
        for number in node_numbers[:2]:
            self.sharders.once(
                number=number,
                additional_args='--partitions=%s' % self.brain.part)

        self.assert_container_listing(obj_names)  # sanity check
        return obj_names

    def test_replication_to_sharding_container(self):
        # verify that replication from an unsharded replica to a sharding
        # replica does not replicate rows but does replicate shard ranges
        obj_names = self._setup_replication_scenario(3)
        for node in self.brain.nodes[:2]:
            self.assert_container_state(node, 'sharding', 3)

        # bring third server back up, run replicator
        node_numbers = self.brain.node_numbers
        self.brain.servers.start(number=node_numbers[2])
        # sanity check...
        self.assert_container_state(self.brain.nodes[2], 'unsharded', 0)
        self.replicators.once(number=node_numbers[2])
        # check db files unchanged
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 2)
        self.assertLengthEqual(found['normal_dbs'], 3)

        # the 'alpha' object is NOT replicated to the two sharded nodes
        for node in self.brain.nodes[:2]:
            broker = self.get_broker(self.brain.part, node)
            with annotate_failure(
                    'Node id %s in %s' % (node['id'], self.brain.nodes[:2])):
                self.assertFalse(broker.get_objects())
                self.assert_container_state(node, 'sharding', 3)
        self.brain.servers.stop(number=node_numbers[2])
        self.assert_container_listing(obj_names)

        # all nodes now have shard ranges
        self.brain.servers.start(number=node_numbers[2])
        node_data = self.direct_get_container_shard_ranges()
        for node, (hdrs, shard_ranges) in node_data.items():
            with annotate_failure(node):
                self.assert_shard_ranges_contiguous(3, shard_ranges)

        # complete cleaving third shard range on first two nodes
        self.brain.servers.stop(number=node_numbers[2])
        for number in node_numbers[:2]:
            self.sharders.once(
                number=number,
                additional_args='--partitions=%s' % self.brain.part)
        # ...and now they are in sharded state
        self.assert_container_state(self.brain.nodes[0], 'sharded', 3)
        self.assert_container_state(self.brain.nodes[1], 'sharded', 3)
        # ...still no 'alpha' object in listing
        self.assert_container_listing(obj_names)

        # run the sharder on the third server, alpha object is included in
        # shards that it cleaves
        self.brain.servers.start(number=node_numbers[2])
        self.assert_container_state(self.brain.nodes[2], 'unsharded', 3)
        self.sharders.once(number=node_numbers[2],
                           additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(self.brain.nodes[2], 'sharding', 3)
        self.sharders.once(number=node_numbers[2],
                           additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(self.brain.nodes[2], 'sharded', 3)
        self.assert_container_listing(['alpha'] + obj_names)

    def test_replication_to_sharded_container(self):
        # verify that replication from an unsharded replica to a sharded
        # replica does not replicate rows but does replicate shard ranges
        obj_names = self._setup_replication_scenario(2)
        for node in self.brain.nodes[:2]:
            self.assert_container_state(node, 'sharded', 2)

        # sanity check
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 2)
        self.assertLengthEqual(found['normal_dbs'], 1)
        for node in self.brain.nodes[:2]:
            broker = self.get_broker(self.brain.part, node)
            info = broker.get_info()
            with annotate_failure(
                    'Node id %s in %s' % (node['id'], self.brain.nodes[:2])):
                self.assertEqual(len(obj_names), info['object_count'])
                self.assertFalse(broker.get_objects())

        # bring third server back up, run replicator
        node_numbers = self.brain.node_numbers
        self.brain.servers.start(number=node_numbers[2])
        # sanity check...
        self.assert_container_state(self.brain.nodes[2], 'unsharded', 0)
        self.replicators.once(number=node_numbers[2])
        # check db files unchanged
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 2)
        self.assertLengthEqual(found['normal_dbs'], 1)

        # the 'alpha' object is NOT replicated to the two sharded nodes
        for node in self.brain.nodes[:2]:
            broker = self.get_broker(self.brain.part, node)
            with annotate_failure(
                    'Node id %s in %s' % (node['id'], self.brain.nodes[:2])):
                self.assertFalse(broker.get_objects())
                self.assert_container_state(node, 'sharded', 2)
        self.brain.servers.stop(number=node_numbers[2])
        self.assert_container_listing(obj_names)

        # all nodes now have shard ranges
        self.brain.servers.start(number=node_numbers[2])
        node_data = self.direct_get_container_shard_ranges()
        for node, (hdrs, shard_ranges) in node_data.items():
            with annotate_failure(node):
                self.assert_shard_ranges_contiguous(2, shard_ranges)

        # run the sharder on the third server, alpha object is included in
        # shards that it cleaves
        self.assert_container_state(self.brain.nodes[2], 'unsharded', 2)
        self.sharders.once(number=node_numbers[2],
                           additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(self.brain.nodes[2], 'sharded', 2)
        self.assert_container_listing(['alpha'] + obj_names)

    def test_sharding_requires_sufficient_replication(self):
        # verify that cleaving only progresses if each cleaved shard range is
        # sufficiently replicated

        # put enough objects for 4 shard ranges
        obj_names = self._make_object_names(2 * self.max_shard_size)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        node_numbers = self.brain.node_numbers
        leader_node = self.brain.nodes[0]
        leader_num = node_numbers[0]

        # run replicators first time to get sync points set
        self.replicators.once()

        # start sharding on the leader node
        self.sharders.once(number=leader_num,
                           additional_args='--partitions=%s' % self.brain.part)
        shard_ranges = self.assert_container_state(leader_node, 'sharding', 4)
        self.assertEqual([ShardRange.CLEAVED] * 2 + [ShardRange.CREATED] * 2,
                         [sr.state for sr in shard_ranges])

        # stop *all* container servers for third shard range
        sr_part, sr_node_nums = self.get_part_and_node_numbers(shard_ranges[2])
        for node_num in sr_node_nums:
            self.brain.servers.stop(number=node_num)

        # attempt to continue sharding on the leader node
        self.sharders.once(number=leader_num,
                           additional_args='--partitions=%s' % self.brain.part)

        # no cleaving progress was made
        for node_num in sr_node_nums:
            self.brain.servers.start(number=node_num)
        shard_ranges = self.assert_container_state(leader_node, 'sharding', 4)
        self.assertEqual([ShardRange.CLEAVED] * 2 + [ShardRange.CREATED] * 2,
                         [sr.state for sr in shard_ranges])

        # stop two of the servers for third shard range, not including any
        # server that happens to be the leader node
        stopped = []
        for node_num in sr_node_nums:
            if node_num != leader_num:
                self.brain.servers.stop(number=node_num)
                stopped.append(node_num)
                if len(stopped) >= 2:
                    break
        self.assertLengthEqual(stopped, 2)  # sanity check

        # attempt to continue sharding on the leader node
        self.sharders.once(number=leader_num,
                           additional_args='--partitions=%s' % self.brain.part)

        # no cleaving progress was made
        for node_num in stopped:
            self.brain.servers.start(number=node_num)
        shard_ranges = self.assert_container_state(leader_node, 'sharding', 4)
        self.assertEqual([ShardRange.CLEAVED] * 2 + [ShardRange.CREATED] * 2,
                         [sr.state for sr in shard_ranges])

        # stop just one of the servers for third shard range
        stopped = []
        for node_num in sr_node_nums:
            if node_num != leader_num:
                self.brain.servers.stop(number=node_num)
                stopped.append(node_num)
                break
        self.assertLengthEqual(stopped, 1)  # sanity check

        # attempt to continue sharding the container
        self.sharders.once(number=leader_num,
                           additional_args='--partitions=%s' % self.brain.part)

        # this time cleaving completed
        self.brain.servers.start(number=stopped[0])
        shard_ranges = self.assert_container_state(leader_node, 'sharded', 4)
        self.assertEqual([ShardRange.ACTIVE] * 4,
                         [sr.state for sr in shard_ranges])

    def test_sharded_delete(self):
        all_obj_names = self._make_object_names(self.max_shard_size)
        self.put_objects(all_obj_names)
        # Shard the container
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        for n in self.brain.node_numbers:
            self.sharders.once(
                number=n, additional_args='--partitions=%s' % self.brain.part)
        # sanity checks
        for node in self.brain.nodes:
            self.assert_container_state(node, 'sharded', 2)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')
        self.assert_container_listing(all_obj_names)

        # delete all objects - updates redirected to shards
        self.delete_objects(all_obj_names)
        self.assert_container_listing([])
        self.assert_container_post_ok('has objects')
        # root not yet updated with shard stats
        self.assert_container_object_count(len(all_obj_names))
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()

        # run sharder on shard containers to update root stats
        shard_ranges = self.get_container_shard_ranges()
        self.assertLengthEqual(shard_ranges, 2)
        self.run_sharders(shard_ranges)
        self.assert_container_listing([])
        self.assert_container_post_ok('empty')
        self.assert_container_object_count(0)

        # put a new object - update redirected to shard
        self.put_objects(['alpha'])
        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(0)

        # before root learns about new object in shard, delete the container
        client.delete_container(self.url, self.token, self.container_name)
        self.assert_container_post_fails('deleted')
        self.assert_container_not_found()

        # run the sharders to update root with shard stats
        self.run_sharders(shard_ranges)

        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(1)
        self.assert_container_delete_fails()
        self.assert_container_post_ok('revived')

    def test_object_update_redirection(self):
        all_obj_names = self._make_object_names(self.max_shard_size)
        self.put_objects(all_obj_names)
        # Shard the container
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        for n in self.brain.node_numbers:
            self.sharders.once(
                number=n, additional_args='--partitions=%s' % self.brain.part)
        # sanity checks
        for node in self.brain.nodes:
            self.assert_container_state(node, 'sharded', 2)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')
        self.assert_container_listing(all_obj_names)

        # delete all objects - updates redirected to shards
        self.delete_objects(all_obj_names)
        self.assert_container_listing([])
        self.assert_container_post_ok('has objects')

        # run sharder on shard containers to update root stats
        shard_ranges = self.get_container_shard_ranges()
        self.assertLengthEqual(shard_ranges, 2)
        self.run_sharders(shard_ranges)
        self.assert_container_object_count(0)

        # First, test a misplaced object moving from one shard to another.
        # with one shard server down, put a new 'alpha' object...
        shard_part, shard_nodes = self.get_part_and_node_numbers(
            shard_ranges[0])
        self.brain.servers.stop(number=shard_nodes[2])
        self.put_objects(['alpha'])
        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(0)
        self.assertLengthEqual(
            self.gather_async_pendings(self.get_all_object_nodes()), 1)
        self.brain.servers.start(number=shard_nodes[2])

        # run sharder on root to discover first shrink candidate
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        # then run sharder on the shard node without the alpha object
        self.sharders.once(additional_args='--partitions=%s' % shard_part,
                           number=shard_nodes[2])
        # root sees first shard has shrunk, only second shard range used for
        # listing so alpha object not in listing
        self.assertLengthEqual(self.get_container_shard_ranges(), 1)
        self.assert_container_listing([])
        self.assert_container_object_count(0)

        # run the updaters: the async pending update will be redirected from
        # shrunk shard to second shard
        self.updaters.once()
        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(0)  # root not yet updated

        # then run sharder on other shard nodes to complete shrinking
        for number in shard_nodes[:2]:
            self.sharders.once(additional_args='--partitions=%s' % shard_part,
                               number=number)
        # and get root updated
        self.run_sharders(shard_ranges[1])
        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(1)
        self.assertLengthEqual(self.get_container_shard_ranges(), 1)

        # Now we have just one active shard, test a misplaced object moving
        # from that shard to the root.
        # with one shard server down, delete 'alpha' and put a 'beta' object...
        shard_part, shard_nodes = self.get_part_and_node_numbers(
            shard_ranges[1])
        self.brain.servers.stop(number=shard_nodes[2])
        self.delete_objects(['alpha'])
        self.put_objects(['beta'])
        self.assert_container_listing(['beta'])
        self.assert_container_object_count(1)
        self.assertLengthEqual(
            self.gather_async_pendings(self.get_all_object_nodes()), 2)
        self.brain.servers.start(number=shard_nodes[2])

        # run sharder on root to discover second shrink candidate - root is not
        # yet aware of the beta object
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        # then run sharder on the shard node without the beta object, to shrink
        # it to root - note this moves stale copy of alpha to the root db
        self.sharders.once(additional_args='--partitions=%s' % shard_part,
                           number=shard_nodes[2])
        # now there are no active shards
        self.assertFalse(self.get_container_shard_ranges())

        # with other two shard servers down, listing won't find beta object
        for number in shard_nodes[:2]:
            self.brain.servers.stop(number=number)
        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(1)

        # run the updaters: the async pending update will be redirected from
        # shrunk shard to the root
        self.updaters.once()
        self.assert_container_listing(['beta'])
        self.assert_container_object_count(1)

    def test_misplaced_object_movement(self):
        def merge_object(shard_range, name, deleted=0):
            # it's hard to get a test to put a misplaced object into a shard,
            # so this hack is used force an object record directly into a shard
            # container db. Note: the actual object won't exist, we're just
            # using this to test object records in container dbs.
            shard_part, shard_nodes = self.brain.ring.get_nodes(
                shard_range.account, shard_range.container)
            shard_broker = self.get_broker(
                shard_part, shard_nodes[0], shard_range.account,
                shard_range.container)
            shard_broker.merge_items(
                [{'name': name, 'created_at': Timestamp.now().internal,
                  'size': 0, 'content_type': 'text/plain',
                  'etag': hashlib.md5().hexdigest(), 'deleted': deleted,
                  'storage_policy_index': shard_broker.storage_policy_index}])
            return shard_nodes[0]

        all_obj_names = self._make_object_names(self.max_shard_size)
        self.put_objects(all_obj_names)
        # Shard the container
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        for n in self.brain.node_numbers:
            self.sharders.once(
                number=n, additional_args='--partitions=%s' % self.brain.part)
        # sanity checks
        for node in self.brain.nodes:
            self.assert_container_state(node, 'sharded', 2)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')
        self.assert_container_listing(all_obj_names)

        # delete all objects in first shard range - updates redirected to shard
        shard_ranges = self.get_container_shard_ranges()
        self.assertLengthEqual(shard_ranges, 2)
        shard_0_objects = [name for name in all_obj_names
                           if name in shard_ranges[0]]
        shard_1_objects = [name for name in all_obj_names
                           if name in shard_ranges[1]]
        self.delete_objects(shard_0_objects)
        self.assert_container_listing(shard_1_objects)
        self.assert_container_post_ok('has objects')

        # run sharder on first shard container to update root stats
        self.run_sharders(shard_ranges[0])
        self.assert_container_object_count(len(shard_1_objects))

        # First, test a misplaced object moving from one shard to another.
        # run sharder on root to discover first shrink candidate
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        # then run sharder on first shard range to shrink it
        self.run_sharders(shard_ranges[0])
        # force a misplaced object into the shrunken shard range to simulate
        # a client put that was in flight when it started to shrink
        misplaced_node = merge_object(shard_ranges[0], 'alpha', deleted=0)
        # root sees first shard has shrunk, only second shard range used for
        # listing so alpha object not in listing
        self.assertLengthEqual(self.get_container_shard_ranges(), 1)
        self.assert_container_listing(shard_1_objects)
        self.assert_container_object_count(len(shard_1_objects))
        # until sharder runs on that node to move the misplaced object to the
        # second shard range
        shard_part, shard_nodes_numbers = self.get_part_and_node_numbers(
            shard_ranges[0])
        self.sharders.once(additional_args='--partitions=%s' % shard_part,
                           number=misplaced_node['id'] + 1)
        self.assert_container_listing(['alpha'] + shard_1_objects)
        # root not yet updated
        self.assert_container_object_count(len(shard_1_objects))

        # run sharder to get root updated
        self.run_sharders(shard_ranges[1])
        self.assert_container_listing(['alpha'] + shard_1_objects)
        self.assert_container_object_count(len(shard_1_objects) + 1)
        self.assertLengthEqual(self.get_container_shard_ranges(), 1)

        # Now we have just one active shard, test a misplaced object moving
        # from that shard to the root.
        # delete most objects from second shard range and run sharder on root
        # to discover second shrink candidate
        self.delete_objects(shard_1_objects)
        self.run_sharders(shard_ranges[1])
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        # then run sharder on the shard node to shrink it to root - note this
        # moves alpha to the root db
        self.run_sharders(shard_ranges[1])
        # now there are no active shards
        self.assertFalse(self.get_container_shard_ranges())

        # force some misplaced object updates into second shrunk shard range
        merge_object(shard_ranges[1], 'alpha', deleted=1)
        misplaced_node = merge_object(shard_ranges[1], 'beta', deleted=0)
        # root is not yet aware of them
        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(1)
        # until sharder runs on that node to move the misplaced object
        shard_part, shard_nodes_numbers = self.get_part_and_node_numbers(
            shard_ranges[1])
        self.sharders.once(additional_args='--partitions=%s' % shard_part,
                           number=misplaced_node['id'] + 1)
        self.assert_container_listing(['beta'])
        self.assert_container_object_count(1)
        self.assert_container_delete_fails()

    def test_replication_to_sharded_container_from_unsharded_old_primary(self):
        primary_ids = [n['id'] for n in self.brain.nodes]
        handoff_node = next(n for n in self.brain.ring.devs
                            if n['id'] not in primary_ids)

        # start with two sharded replicas and one unsharded with extra object
        obj_names = self._setup_replication_scenario(2)
        for node in self.brain.nodes[:2]:
            self.assert_container_state(node, 'sharded', 2)

        # Fake a ring change - copy unsharded db which has no shard ranges to a
        # handoff to create illusion of a new unpopulated primary node
        node_numbers = self.brain.node_numbers
        new_primary_node = self.brain.nodes[2]
        new_primary_node_number = node_numbers[2]
        new_primary_dir, container_hash = self.get_storage_dir(
            self.brain.part, new_primary_node)
        old_primary_dir, container_hash = self.get_storage_dir(
            self.brain.part, handoff_node)
        utils.mkdirs(os.path.dirname(old_primary_dir))
        shutil.move(new_primary_dir, old_primary_dir)

        # make the cluster more or less "healthy" again
        self.brain.servers.start(number=new_primary_node_number)

        # get a db on every node...
        client.put_container(self.url, self.token, self.container_name)
        self.assertTrue(os.path.exists(os.path.join(
            new_primary_dir, container_hash + '.db')))
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['normal_dbs'], 1)  # "new" primary
        self.assertLengthEqual(found['shard_dbs'], 2)  # existing primaries

        # catastrophic failure! drive dies and is replaced on unchanged primary
        failed_node = self.brain.nodes[0]
        failed_dir, _container_hash = self.get_storage_dir(
            self.brain.part, failed_node)
        shutil.rmtree(failed_dir)

        # replicate the "old primary" to everybody except the "new primary"
        self.brain.servers.stop(number=new_primary_node_number)
        self.replicators.once(number=handoff_node['id'] + 1)

        # We're willing to rsync the retiring db to the failed primary.
        # This may or may not have shard ranges, depending on the order in
        # which we hit the primaries, but it definitely *doesn't* have an
        # epoch in its name yet. All objects are replicated.
        self.assertTrue(os.path.exists(os.path.join(
            failed_dir, container_hash + '.db')))
        self.assertLengthEqual(os.listdir(failed_dir), 1)
        broker = self.get_broker(self.brain.part, failed_node)
        self.assertLengthEqual(broker.get_objects(), len(obj_names) + 1)

        # The other out-of-date primary is within usync range but objects are
        # not replicated to it because the handoff db learns about shard ranges
        broker = self.get_broker(self.brain.part, self.brain.nodes[1])
        self.assertLengthEqual(broker.get_objects(), 0)

        # Handoff db still exists and now has shard ranges!
        self.assertTrue(os.path.exists(os.path.join(
            old_primary_dir, container_hash + '.db')))
        broker = self.get_broker(self.brain.part, handoff_node)
        shard_ranges = broker.get_shard_ranges()
        self.assertLengthEqual(shard_ranges, 2)
        self.assert_container_state(handoff_node, 'unsharded', 2)

        # Replicate again, this time *including* "new primary"
        self.brain.servers.start(number=new_primary_node_number)
        self.replicators.once(number=handoff_node['id'] + 1)

        # Ordinarily, we would have rsync_then_merge'd to "new primary"
        # but instead we wait
        broker = self.get_broker(self.brain.part, new_primary_node)
        self.assertLengthEqual(broker.get_objects(), 0)
        shard_ranges = broker.get_shard_ranges()
        self.assertLengthEqual(shard_ranges, 2)

        # so the next time the sharder comes along, it can push rows out
        # and delete the big db
        self.sharders.once(number=handoff_node['id'] + 1,
                           additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(handoff_node, 'sharded', 2)
        self.assertFalse(os.path.exists(os.path.join(
            old_primary_dir, container_hash + '.db')))
        # the sharded db hangs around until replication confirms durability
        # first attempt is not sufficiently successful
        self.brain.servers.stop(number=node_numbers[0])
        self.replicators.once(number=handoff_node['id'] + 1)
        self.assertTrue(os.path.exists(old_primary_dir))
        self.assert_container_state(handoff_node, 'sharded', 2)
        # second attempt is successful and handoff db is deleted
        self.brain.servers.start(number=node_numbers[0])
        self.replicators.once(number=handoff_node['id'] + 1)
        self.assertFalse(os.path.exists(old_primary_dir))

        # run all the sharders, get us into a consistent state
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_listing(['alpha'] + obj_names)

    def test_replication_to_empty_new_primary_from_sharding_old_primary(self):
        primary_ids = [n['id'] for n in self.brain.nodes]
        handoff_node = next(n for n in self.brain.ring.devs
                            if n['id'] not in primary_ids)
        num_shards = 3
        obj_names = self._make_object_names(
            num_shards * self.max_shard_size / 2)
        self.put_objects(obj_names)
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})

        # run replicators first time to get sync points set
        self.replicators.once()
        # start sharding on only the leader node
        leader_node = self.brain.nodes[0]
        leader_node_number = self.brain.node_numbers[0]
        self.sharders.once(number=leader_node_number)
        self.assert_container_state(leader_node, 'sharding', 3)
        for node in self.brain.nodes[1:]:
            self.assert_container_state(node, 'unsharded', 3)

        # Fake a ring change - copy leader node db to a handoff to create
        # illusion of a new unpopulated primary leader node
        new_primary_dir, container_hash = self.get_storage_dir(
            self.brain.part, leader_node)
        old_primary_dir, container_hash = self.get_storage_dir(
            self.brain.part, handoff_node)
        utils.mkdirs(os.path.dirname(old_primary_dir))
        shutil.move(new_primary_dir, old_primary_dir)
        self.assert_container_state(handoff_node, 'sharding', 3)

        # run replicator on handoff node to create a fresh db on new primary
        self.assertFalse(os.path.exists(new_primary_dir))
        self.replicators.once(number=handoff_node['id'] + 1)
        self.assertTrue(os.path.exists(new_primary_dir))
        self.assert_container_state(leader_node, 'sharded', 3)
        broker = self.get_broker(self.brain.part, leader_node)
        shard_ranges = broker.get_shard_ranges()
        self.assertLengthEqual(shard_ranges, 3)
        self.assertEqual(
            [ShardRange.CLEAVED, ShardRange.CLEAVED, ShardRange.CREATED],
            [sr.state for sr in shard_ranges])

        # db still exists on handoff
        self.assertTrue(os.path.exists(old_primary_dir))
        self.assert_container_state(handoff_node, 'sharding', 3)
        # continue sharding it...
        self.sharders.once(number=handoff_node['id'] + 1)
        self.assert_container_state(leader_node, 'sharded', 3)
        # now handoff is fully sharded the replicator will delete it
        self.replicators.once(number=handoff_node['id'] + 1)
        self.assertFalse(os.path.exists(old_primary_dir))

        # all primaries now have active shard ranges but only one is in sharded
        # state
        self.assert_container_state(leader_node, 'sharded', 3)
        for node in self.brain.nodes[1:]:
            self.assert_container_state(node, 'unsharded', 3)
        node_data = self.direct_get_container_shard_ranges()
        for node_id, (hdrs, shard_ranges) in node_data.items():
            with annotate_failure(
                    'node id %s from %s' % (node_id, node_data.keys)):
                self.assert_shard_range_state(ShardRange.ACTIVE, shard_ranges)

        # check handoff cleaved all objects before it was deleted - stop all
        # but leader node so that listing is fetched from shards
        for number in self.brain.node_numbers[1:3]:
            self.brain.servers.stop(number=number)

        self.assert_container_listing(obj_names)

        for number in self.brain.node_numbers[1:3]:
            self.brain.servers.start(number=number)

        self.sharders.once()
        self.assert_container_state(leader_node, 'sharded', 3)
        for node in self.brain.nodes[1:]:
            self.assert_container_state(node, 'sharding', 3)
        self.sharders.once()
        for node in self.brain.nodes:
            self.assert_container_state(node, 'sharded', 3)

        self.assert_container_listing(obj_names)

    def test_sharded_account_updates(self):
        # verify that .shards account updates have zero object count and bytes
        # to avoid double accounting
        all_obj_names = self._make_object_names(self.max_shard_size)
        self.put_objects(all_obj_names, contents='xyz')
        # Shard the container into 2 shards
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        for n in self.brain.node_numbers:
            self.sharders.once(
                number=n, additional_args='--partitions=%s' % self.brain.part)
        # sanity checks
        for node in self.brain.nodes:
            shard_ranges = self.assert_container_state(node, 'sharded', 2)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')
        self.assert_container_listing(all_obj_names)
        # run the updaters to get account stats updated
        self.updaters.once()
        # check user account stats
        metadata = self.internal_client.get_account_metadata(self.account)
        self.assertEqual(1, int(metadata.get('x-account-container-count')))
        self.assertEqual(self.max_shard_size,
                         int(metadata.get('x-account-object-count')))
        self.assertEqual(3 * self.max_shard_size,
                         int(metadata.get('x-account-bytes-used')))
        # check hidden .shards account stats
        metadata = self.internal_client.get_account_metadata(
            shard_ranges[0].account)
        self.assertEqual(2, int(metadata.get('x-account-container-count')))
        self.assertEqual(0, int(metadata.get('x-account-object-count')))
        self.assertEqual(0, int(metadata.get('x-account-bytes-used')))
