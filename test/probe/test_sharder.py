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
import json
import os
import pickle
import shutil
import subprocess
import unittest
import uuid

from unittest import SkipTest
from urllib.parse import quote

from swift.common import direct_client, utils
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.internal_client import UnexpectedResponse
from swift.common.manager import Manager
from swift.common.memcached import MemcacheRing
from swift.common.utils import ShardRange, parse_db_filename, quorum_size, \
    config_true_value, Timestamp, md5, Namespace
from swift.container.backend import ContainerBroker, UNSHARDED, SHARDING, \
    SHARDED
from swift.container.sharder import CleavingContext, ContainerSharder
from swift.container.replicator import ContainerReplicator
from swiftclient import client, get_auth, ClientException

from swift.proxy.controllers.base import get_cache_key
from swift.proxy.controllers.obj import num_container_updates
from test import annotate_failure
from test.debug_logger import debug_logger
from test.probe import PROXY_BASE_URL
from test.probe.brain import BrainSplitter
from test.probe.common import ReplProbeTest, get_server_number, \
    wait_for_server_to_hangup, ENABLED_POLICIES, exclude_nodes
from unittest import mock

try:
    from swiftclient.requests_compat import requests as client_requests
except ImportError:
    # legacy location
    from swiftclient.client import requests as client_requests

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
    DELIM = '-'

    def _maybe_skip_test(self):
        try:
            self.cont_configs = [
                utils.readconf(p, 'container-sharder')
                for p in self.configs['container-sharder'].values()]
        except ValueError:
            raise SkipTest('No [container-sharder] section found in '
                           'container-server configs')

        self.max_shard_size = max(
            int(c.get('shard_container_threshold', '1000000'))
            for c in self.cont_configs)

        skip_reasons = []
        if not (MIN_SHARD_CONTAINER_THRESHOLD <= self.max_shard_size
                <= MAX_SHARD_CONTAINER_THRESHOLD):
            skip_reasons.append(
                'shard_container_threshold %d must be between %d and %d' %
                (self.max_shard_size, MIN_SHARD_CONTAINER_THRESHOLD,
                 MAX_SHARD_CONTAINER_THRESHOLD))

        def skip_check(reason_list, option, required):
            values = {int(c.get(option, required)) for c in self.cont_configs}
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

    def _make_object_names(self, number, start=0):
        return ['obj%s%04d' % (self.DELIM, x)
                for x in range(start, start + number)]

    def _setup_container_name(self):
        # Container where we're PUTting objects
        self.container_name = 'container%s%s' % (self.DELIM, uuid.uuid4())

    def setUp(self):
        client.logger.setLevel(client.logging.WARNING)
        client_requests.logging.getLogger().setLevel(
            client_requests.logging.WARNING)
        super(BaseTestContainerSharding, self).setUp()
        _, self.admin_token = get_auth(
            PROXY_BASE_URL + '/auth/v1.0', 'admin:admin', 'admin')
        self._setup_container_name()
        self.init_brain(self.container_name)
        self.sharders = Manager(['container-sharder'])
        self.internal_client = self.make_internal_client()
        self.logger = debug_logger('sharder-test')
        self.memcache = MemcacheRing(['127.0.0.1:11211'], logger=self.logger)
        self.container_replicators = Manager(['container-replicator'])

    def init_brain(self, container_name):
        self.container_to_shard = container_name
        self.brain = BrainSplitter(
            self.url, self.token, self.container_to_shard,
            None, 'container')
        self.brain.put_container(policy_index=int(self.policy))

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
        conn = client.Connection(preauthurl=self.url, preauthtoken=self.token)
        results = []
        for obj in obj_names:
            rdict = {}
            conn.put_object(self.container_name, obj,
                            contents=contents, response_dict=rdict)
            results.append((obj, rdict['headers'].get('x-object-version-id')))
        return results

    def delete_objects(self, obj_names_and_versions):
        conn = client.Connection(preauthurl=self.url, preauthtoken=self.token)
        for obj in obj_names_and_versions:
            if isinstance(obj, tuple):
                obj, version = obj
                conn.delete_object(self.container_name, obj,
                                   query_string='version-id=%s' % version)
            else:
                conn.delete_object(self.container_name, obj)

    def get_container_listing(self, account=None, container=None,
                              headers=None, params=None):
        account = account if account else self.account
        container = container if container else self.container_to_shard
        path = self.internal_client.make_path(account, container)
        headers = headers or {}
        return self.internal_client.make_request(
            'GET', path + '?format=json', headers, [200], params=params)

    def get_container_objects(self, account=None, container=None,
                              headers=None, params=None):
        headers = HeaderKeyDict(headers) if headers else {}
        resp = self.get_container_listing(account, container, headers,
                                          params=params)
        req_record_type = headers.get('X-Backend-Record-Type')
        resp_record_type = resp.headers.get('X-Backend-Record-Type')
        if req_record_type and req_record_type.lower() == 'object':
            self.assertEqual('object', resp_record_type)
        else:
            self.assertIsNone(resp_record_type)
        self.assertNotIn('X-Backend-Record-Shard-Format', resp.headers)
        return json.loads(resp.body)

    def get_container_shard_ranges(self, account=None, container=None,
                                   headers=None, params=None):
        headers = dict(headers) if headers else {}
        headers.update({'X-Backend-Record-Type': 'shard'})
        resp = self.get_container_listing(account, container, headers,
                                          params=params)
        self.assertEqual('shard', resp.headers.get('X-Backend-Record-Type'))
        self.assertEqual('full',
                         resp.headers.get('X-Backend-Record-Shard-Format'))
        return [ShardRange.from_dict(sr) for sr in json.loads(resp.body)]

    def get_container_namespaces(self, account=None, container=None,
                                 headers=None, params=None):
        headers = dict(headers) if headers else {}
        headers.update({'X-Backend-Record-Type': 'shard',
                        'X-Backend-Record-Shard-Format': 'namespace'})
        resp = self.get_container_listing(account, container, headers,
                                          params=params)
        self.assertEqual('shard', resp.headers.get('X-Backend-Record-Type'))
        self.assertEqual('namespace',
                         resp.headers.get('X-Backend-Record-Shard-Format'))
        return [Namespace(**ns) for ns in json.loads(resp.body)]

    def direct_get_container_shard_ranges(self, account=None, container=None,
                                          expect_failure=False):
        collector = ShardCollector()
        self.direct_container_op(
            collector, account, container, expect_failure)
        return collector.ranges

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

    def get_db_file(self, part, node, account=None, container=None):
        container_dir, container_hash = self.get_storage_dir(
            part, node, account=account, container=container)
        for f in os.listdir(container_dir):
            path = os.path.join(container_dir, f)
            if path.endswith('.db'):
                return path

    def get_broker(self, part, node, account=None, container=None):
        return ContainerBroker(
            self.get_db_file(part, node, account, container))

    def get_shard_broker(self, shard_range, node_index=0):
        shard_part, shard_nodes = self.brain.ring.get_nodes(
            shard_range.account, shard_range.container)
        return self.get_broker(
            shard_part, shard_nodes[node_index], shard_range.account,
            shard_range.container)

    def categorize_container_dir_content(self, account=None, container=None,
                                         more_nodes=False):
        account = account or self.brain.account
        container = container or self.container_name
        part, nodes = self.brain.ring.get_nodes(account, container)
        if more_nodes:
            nodes.extend(self.brain.ring.get_more_nodes(part))
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

    def assert_dict_contains(self, expected_items, actual_dict):
        ignored = set(expected_items) ^ set(actual_dict)
        filtered_actual = {k: actual_dict[k]
                           for k in actual_dict if k not in ignored}
        self.assertEqual(expected_items, filtered_actual)

    def assert_shard_ranges_contiguous(self, expected_number, shard_ranges,
                                       first_lower='', last_upper=''):
        if shard_ranges and isinstance(shard_ranges[0], ShardRange):
            actual_shard_ranges = sorted(shard_ranges)
        else:
            actual_shard_ranges = sorted(ShardRange.from_dict(d)
                                         for d in shard_ranges)
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
        actual = sum(sr['object_count'] for sr in shard_ranges)
        self.assertEqual(expected_object_count, actual)

    def assert_container_listing(self, expected_listing, req_hdrs=None):
        req_hdrs = req_hdrs if req_hdrs else {}
        headers, actual_listing = client.get_container(
            self.url, self.token, self.container_name, headers=req_hdrs)
        self.assertIn('x-container-object-count', headers)
        expected_obj_count = len(expected_listing)
        self.assertEqual(expected_listing, [
            x['name'] for x in actual_listing])
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

    def assert_container_state(self, node, expected_state, num_shard_ranges,
                               account=None, container=None, part=None,
                               override_deleted=False):
        account = account or self.account
        container = container or self.container_to_shard
        part = part or self.brain.part
        headers = {'X-Backend-Record-Type': 'shard'}
        if override_deleted:
            headers['x-backend-override-deleted'] = True
        headers, shard_ranges = direct_client.direct_get_container(
            node, part, account, container,
            headers=headers)
        self.assertEqual(num_shard_ranges, len(shard_ranges))
        self.assertIn('X-Backend-Sharding-State', headers)
        self.assertEqual(
            expected_state, headers['X-Backend-Sharding-State'])
        return [ShardRange.from_dict(sr) for sr in shard_ranges]

    def assert_container_states(self, expected_state, num_shard_ranges):
        for node in self.brain.nodes:
            self.assert_container_state(node, expected_state, num_shard_ranges)

    def assert_subprocess_success(self, cmd_args):
        try:
            return subprocess.check_output(cmd_args, stderr=subprocess.STDOUT)
        except Exception as exc:
            # why not 'except CalledProcessError'? because in my py3.6 tests
            # the CalledProcessError wasn't caught by that! despite type(exc)
            # being a CalledProcessError, isinstance(exc, CalledProcessError)
            # is False and the type has a different hash - could be
            # related to https://github.com/eventlet/eventlet/issues/413
            try:
                # assume this is a CalledProcessError
                self.fail('%s with output:\n%s' % (exc, exc.output))
            except AttributeError:
                raise exc

    def get_part_and_node_numbers(self, shard_range):
        """Return the partition and node numbers for a shard range."""
        part, nodes = self.brain.ring.get_nodes(
            shard_range.account, shard_range.container)
        return part, [n['id'] + 1 for n in nodes]

    def run_sharders(self, shard_ranges, exclude_partitions=None):
        """Run the sharder on partitions for given shard ranges."""
        if not isinstance(shard_ranges, (list, tuple, set)):
            shard_ranges = (shard_ranges,)
        exclude_partitions = exclude_partitions or []
        shard_parts = []
        for sr in shard_ranges:
            sr_part = self.get_part_and_node_numbers(sr)[0]
            if sr_part not in exclude_partitions:
                shard_parts.append(str(sr_part))
        partitions = ','.join(shard_parts)
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

    def run_custom_sharder(self, conf_index, custom_conf, **kwargs):
        return self.run_custom_daemon(ContainerSharder, 'container-sharder',
                                      conf_index, custom_conf, **kwargs)

    def sharders_once_non_auto(self, **kwargs):
        # inhibit auto_sharding regardless of the config setting
        additional_args = kwargs.get('additional_args', [])
        if not isinstance(additional_args, list):
            additional_args = [additional_args]
        additional_args.append('--no-auto-shard')
        kwargs['additional_args'] = additional_args
        self.sharders.once(**kwargs)


class BaseAutoContainerSharding(BaseTestContainerSharding):

    def _maybe_skip_test(self):
        super(BaseAutoContainerSharding, self)._maybe_skip_test()
        auto_shard = all(config_true_value(c.get('auto_shard', False))
                         for c in self.cont_configs)
        if not auto_shard:
            raise SkipTest('auto_shard must be true '
                           'in all container_sharder configs')


class TestContainerShardingNonUTF8(BaseAutoContainerSharding):
    def test_sharding_listing(self):
        # verify parameterised listing of a container during sharding
        all_obj_names = self._make_object_names(4 * self.max_shard_size)
        obj_names = all_obj_names[::2]
        obj_content = 'testing'
        self.put_objects(obj_names, contents=obj_content)
        # choose some names approx in middle of each expected shard range
        markers = [
            obj_names[i] for i in range(self.max_shard_size // 4,
                                        2 * self.max_shard_size,
                                        self.max_shard_size // 2)]

        def check_listing(objects, req_hdrs=None, **params):
            req_hdrs = req_hdrs if req_hdrs else {}
            qs = '&'.join('%s=%s' % (k, quote(str(v)))
                          for k, v in params.items())
            headers, listing = client.get_container(
                self.url, self.token, self.container_name, query_string=qs,
                headers=req_hdrs)
            listing = [x['name'] for x in listing]
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
            self.assertIn('x-timestamp', headers)
            self.assertIn('last-modified', headers)
            self.assertIn('x-trans-id', headers)
            self.assertEqual('bytes', headers.get('accept-ranges'))
            self.assertEqual('application/json; charset=utf-8',
                             headers.get('content-type'))

        def check_listing_fails(exp_status, **params):
            qs = '&'.join(['%s=%s' % param for param in params.items()])
            with self.assertRaises(ClientException) as cm:
                client.get_container(
                    self.url, self.token, self.container_name, query_string=qs)
            self.assertEqual(exp_status, cm.exception.http_status)
            return cm.exception

        def do_listing_checks(objs, hdrs=None):
            hdrs = hdrs if hdrs else {}
            check_listing(objs, hdrs)
            check_listing(objs, hdrs, marker=markers[0], end_marker=markers[1])
            check_listing(objs, hdrs, marker=markers[0], end_marker=markers[2])
            check_listing(objs, hdrs, marker=markers[1], end_marker=markers[3])
            check_listing(objs, hdrs, marker=markers[1], end_marker=markers[3],
                          limit=self.max_shard_size // 4)
            check_listing(objs, hdrs, marker=markers[1], end_marker=markers[3],
                          limit=self.max_shard_size // 4)
            check_listing(objs, hdrs, marker=markers[1], end_marker=markers[2],
                          limit=self.max_shard_size // 2)
            check_listing(objs, hdrs, marker=markers[1], end_marker=markers[1])
            check_listing(objs, hdrs, reverse=True)
            check_listing(objs, hdrs, reverse=True, end_marker=markers[1])
            check_listing(objs, hdrs, reverse=True, marker=markers[3],
                          end_marker=markers[1],
                          limit=self.max_shard_size // 4)
            check_listing(objs, hdrs, reverse=True, marker=markers[3],
                          end_marker=markers[1], limit=0)
            check_listing([], hdrs, marker=markers[0], end_marker=markers[0])
            check_listing([], hdrs, marker=markers[0], end_marker=markers[1],
                          reverse=True)
            check_listing(objs, hdrs, prefix='obj')
            check_listing([], hdrs, prefix='zzz')
            # delimiter
            headers, listing = client.get_container(
                self.url, self.token, self.container_name,
                query_string='delimiter=' + quote(self.DELIM), headers=hdrs)
            self.assertEqual([{'subdir': 'obj' + self.DELIM}], listing)
            headers, listing = client.get_container(
                self.url, self.token, self.container_name,
                query_string='delimiter=j' + quote(self.DELIM), headers=hdrs)
            self.assertEqual([{'subdir': 'obj' + self.DELIM}], listing)

            limit = self.cluster_info['swift']['container_listing_limit']
            exc = check_listing_fails(412, limit=limit + 1)
            self.assertIn(b'Maximum limit', exc.http_response_content)
            exc = check_listing_fails(400, delimiter='%ff')
            self.assertIn(b'not valid UTF-8', exc.http_response_content)

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
        self.assert_container_states('sharding', 4)
        shard_ranges = self.get_container_shard_ranges()
        self.assertLengthEqual(shard_ranges, 4)
        self.assert_shard_range_state(ShardRange.CLEAVED, shard_ranges[:2])
        self.assert_shard_range_state(ShardRange.CREATED, shard_ranges[2:])

        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()  # confirm no sysmeta deleted
        self.assert_container_post_ok('sharding')
        do_listing_checks(obj_names)

        # put some new objects spread through entire namespace; object updates
        # should be directed to the shard container (both the cleaved and the
        # created shards)
        new_obj_names = all_obj_names[1::4]
        self.put_objects(new_obj_names, obj_content)

        # new objects that fell into the first two cleaved shard ranges are
        # reported in listing; new objects in the yet-to-be-cleaved shard
        # ranges are not yet included in listing because listings prefer the
        # root over the final two shards that are not yet-cleaved
        exp_obj_names = [o for o in obj_names + new_obj_names
                         if o <= shard_ranges[1].upper]
        exp_obj_names += [o for o in obj_names
                          if o > shard_ranges[1].upper]
        exp_obj_names.sort()
        do_listing_checks(exp_obj_names)

        # run all the sharders again and the last two shard ranges get cleaved
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_states('sharded', 4)
        shard_ranges = self.get_container_shard_ranges()
        self.assert_shard_range_state(ShardRange.ACTIVE, shard_ranges)

        # listings are now gathered from all four shard ranges so should have
        # all the specified objects
        exp_obj_names = obj_names + new_obj_names
        exp_obj_names.sort()
        do_listing_checks(exp_obj_names)
        # shard ranges may now be cached by proxy so do listings checks again
        # forcing backend request
        do_listing_checks(exp_obj_names, hdrs={'X-Newest': 'true'})

        # post more metadata to the container and check that it is read back
        # correctly from backend (using x-newest) and cache
        test_headers = {'x-container-meta-test': 'testing',
                        'x-container-read': 'read_acl',
                        'x-container-write': 'write_acl',
                        'x-container-sync-key': 'sync_key',
                        # 'x-container-sync-to': 'sync_to',
                        'x-versions-location': 'versions',
                        'x-container-meta-access-control-allow-origin': 'aa',
                        'x-container-meta-access-control-expose-headers': 'bb',
                        'x-container-meta-access-control-max-age': '123'}
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers=test_headers)
        headers, listing = client.get_container(
            self.url, self.token, self.container_name,
            headers={'X-Newest': 'true'})
        exp_headers = dict(test_headers)
        exp_headers.update({
            'x-container-object-count': str(len(exp_obj_names)),
            'x-container-bytes-used':
            str(len(exp_obj_names) * len(obj_content))
        })
        for k, v in exp_headers.items():
            self.assertIn(k, headers)
            self.assertEqual(v, headers[k], dict(headers))

        cache_headers, listing = client.get_container(
            self.url, self.token, self.container_name)
        for k, v in exp_headers.items():
            self.assertIn(k, cache_headers)
            self.assertEqual(v, cache_headers[k], dict(exp_headers))
        # we don't expect any of these headers to be equal...
        for k in ('x-timestamp', 'last-modified', 'date', 'x-trans-id',
                  'x-openstack-request-id'):
            headers.pop(k, None)
            cache_headers.pop(k, None)
        self.assertEqual(headers, cache_headers)

        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')

        # delete original objects
        self.delete_objects(obj_names)
        do_listing_checks(new_obj_names)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')


class TestContainerShardingFunkyNames(TestContainerShardingNonUTF8):
    DELIM = '\n'

    def _make_object_names(self, number, start=0):
        return ['obj\n%04d%%Ff' % x for x in range(start, start + number)]

    def _setup_container_name(self):
        self.container_name = 'container\n%%Ff\n%s' % uuid.uuid4()


class TestContainerShardingUTF8(TestContainerShardingNonUTF8):
    def _make_object_names(self, number, start=0):
        # override default with names that include non-ascii chars
        name_length = self.cluster_info['swift']['max_object_name_length']
        obj_names = []
        for x in range(start, start + number):
            name = (u'obj-\u00e4\u00ea\u00ec\u00f2\u00fb\u1234-%04d' % x)
            name = name.encode('utf8').ljust(name_length, b'o').decode('utf8')
            obj_names.append(name)
        return obj_names

    def _setup_container_name(self):
        # override default with max length name that includes non-ascii chars
        super(TestContainerShardingUTF8, self)._setup_container_name()
        name_length = self.cluster_info['swift']['max_container_name_length']
        cont_name = \
            self.container_name + u'-\u00e4\u00ea\u00ec\u00f2\u00fb\u1234'
        self.container_name = cont_name.encode('utf8').ljust(
            name_length, b'x').decode('utf8')


class TestContainerShardingObjectVersioning(BaseAutoContainerSharding):
    def _maybe_skip_test(self):
        super(TestContainerShardingObjectVersioning, self)._maybe_skip_test()
        try:
            vw_config = utils.readconf(self.configs['proxy-server'],
                                       'filter:versioned_writes')
        except ValueError:
            raise SkipTest('No [filter:versioned_writes] section found in '
                           'proxy-server configs')
        allow_object_versioning = config_true_value(
            vw_config.get('allow_object_versioning', False))
        if not allow_object_versioning:
            raise SkipTest('allow_object_versioning must be true '
                           'in all versioned_writes configs')

    def init_brain(self, container_name):
        client.put_container(self.url, self.token, container_name, headers={
            'X-Storage-Policy': self.policy.name,
            'X-Versions-Enabled': 'true',
        })
        self.container_to_shard = '\x00versions\x00' + container_name
        self.brain = BrainSplitter(
            self.url, self.token, self.container_to_shard,
            None, 'container')

    def test_sharding_listing(self):
        # verify parameterised listing of a container during sharding
        all_obj_names = self._make_object_names(3) * self.max_shard_size
        all_obj_names.extend(self._make_object_names(self.max_shard_size,
                                                     start=3))
        obj_names = all_obj_names[::2]
        obj_names_and_versions = self.put_objects(obj_names)

        def sort_key(obj_and_ver):
            obj, ver = obj_and_ver
            return obj, ~Timestamp(ver)

        obj_names_and_versions.sort(key=sort_key)
        # choose some names approx in middle of each expected shard range
        markers = [
            obj_names_and_versions[i]
            for i in range(self.max_shard_size // 4,
                           2 * self.max_shard_size,
                           self.max_shard_size // 2)]

        def check_listing(objects, **params):
            params['versions'] = ''
            qs = '&'.join('%s=%s' % param for param in params.items())
            headers, listing = client.get_container(
                self.url, self.token, self.container_name, query_string=qs)
            listing = [(x['name'], x['version_id'])
                       for x in listing]
            if params.get('reverse'):
                marker = (
                    params.get('marker', ShardRange.MAX),
                    ~Timestamp(params['version_marker'])
                    if 'version_marker' in params else ~Timestamp('0'),
                )
                end_marker = (
                    params.get('end_marker', ShardRange.MIN),
                    Timestamp('0'),
                )
                expected = [o for o in objects
                            if end_marker < sort_key(o) < marker]
                expected.reverse()
            else:
                marker = (
                    params.get('marker', ShardRange.MIN),
                    ~Timestamp(params['version_marker'])
                    if 'version_marker' in params else Timestamp('0'),
                )
                end_marker = (
                    params.get('end_marker', ShardRange.MAX),
                    ~Timestamp('0'),
                )
                expected = [o for o in objects
                            if marker < sort_key(o) < end_marker]
            if 'limit' in params:
                expected = expected[:params['limit']]
            self.assertEqual(expected, listing)

        def check_listing_fails(exp_status, **params):
            params['versions'] = ''
            qs = '&'.join('%s=%s' % param for param in params.items())
            with self.assertRaises(ClientException) as cm:
                client.get_container(
                    self.url, self.token, self.container_name, query_string=qs)
            self.assertEqual(exp_status, cm.exception.http_status)
            return cm.exception

        def do_listing_checks(objects):
            check_listing(objects)
            check_listing(objects,
                          marker=markers[0][0], version_marker=markers[0][1])
            check_listing(objects,
                          marker=markers[0][0], version_marker=markers[0][1],
                          limit=self.max_shard_size // 10)
            check_listing(objects,
                          marker=markers[0][0], version_marker=markers[0][1],
                          limit=self.max_shard_size // 4)
            check_listing(objects,
                          marker=markers[0][0], version_marker=markers[0][1],
                          limit=self.max_shard_size // 2)
            check_listing(objects,
                          marker=markers[1][0], version_marker=markers[1][1])
            check_listing(objects,
                          marker=markers[1][0], version_marker=markers[1][1],
                          limit=self.max_shard_size // 10)
            check_listing(objects,
                          marker=markers[2][0], version_marker=markers[2][1],
                          limit=self.max_shard_size // 4)
            check_listing(objects,
                          marker=markers[2][0], version_marker=markers[2][1],
                          limit=self.max_shard_size // 2)
            check_listing(objects, reverse=True)
            check_listing(objects, reverse=True,
                          marker=markers[1][0], version_marker=markers[1][1])

            check_listing(objects, prefix='obj')
            check_listing([], prefix='zzz')
            # delimiter
            headers, listing = client.get_container(
                self.url, self.token, self.container_name,
                query_string='delimiter=-')
            self.assertEqual([{'subdir': 'obj-'}], listing)
            headers, listing = client.get_container(
                self.url, self.token, self.container_name,
                query_string='delimiter=j-')
            self.assertEqual([{'subdir': 'obj-'}], listing)

            limit = self.cluster_info['swift']['container_listing_limit']
            exc = check_listing_fails(412, limit=limit + 1)
            self.assertIn(b'Maximum limit', exc.http_response_content)
            exc = check_listing_fails(400, delimiter='%ff')
            self.assertIn(b'not valid UTF-8', exc.http_response_content)

        # sanity checks
        do_listing_checks(obj_names_and_versions)

        # Shard the container. Use an internal_client so we get an implicit
        # X-Backend-Allow-Reserved-Names header
        self.internal_client.set_container_metadata(
            self.account, self.container_to_shard, {
                'X-Container-Sysmeta-Sharding': 'True',
            })
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
        self.assert_container_states('sharding', 4)
        shard_ranges = self.get_container_shard_ranges()
        self.assertLengthEqual(shard_ranges, 4)
        self.assert_shard_range_state(ShardRange.CLEAVED, shard_ranges[:2])
        self.assert_shard_range_state(ShardRange.CREATED, shard_ranges[2:])

        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()  # confirm no sysmeta deleted
        self.assert_container_post_ok('sharding')
        do_listing_checks(obj_names_and_versions)

        # put some new objects spread through entire namespace
        new_obj_names = all_obj_names[1::4]
        new_obj_names_and_versions = self.put_objects(new_obj_names)

        # new objects that fell into the first two cleaved shard ranges are
        # reported in listing, new objects in the yet-to-be-cleaved shard
        # ranges are not yet included in listing
        exp_obj_names_and_versions = [
            o for o in obj_names_and_versions + new_obj_names_and_versions
            if '\x00' + o[0] <= shard_ranges[1].upper]
        exp_obj_names_and_versions += [
            o for o in obj_names_and_versions
            if '\x00' + o[0] > shard_ranges[1].upper]
        exp_obj_names_and_versions.sort(key=sort_key)
        do_listing_checks(exp_obj_names_and_versions)

        # run all the sharders again and the last two shard ranges get cleaved
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_states('sharded', 4)
        shard_ranges = self.get_container_shard_ranges()
        self.assert_shard_range_state(ShardRange.ACTIVE, shard_ranges)

        exp_obj_names_and_versions = \
            obj_names_and_versions + new_obj_names_and_versions
        exp_obj_names_and_versions.sort(key=sort_key)
        do_listing_checks(exp_obj_names_and_versions)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')

        # delete original objects
        self.delete_objects(obj_names_and_versions)
        new_obj_names_and_versions.sort(key=sort_key)
        do_listing_checks(new_obj_names_and_versions)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')


class TestContainerSharding(BaseAutoContainerSharding):
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
        self.assertEqual(obj_names, [
            x['name'] for x in pre_sharding_listing])  # sanity

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
        # Contexts should still be there, and should be complete
        contexts = set([ctx.done()
                        for ctx, _ in CleavingContext.load_all(broker)])
        self.assertEqual({True}, contexts)
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

            contexts = list(CleavingContext.load_all(broker))
            self.assertEqual([], contexts)  # length check

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
            # Contexts should still be there, and should be complete
            contexts = set([ctx.done()
                            for ctx, _ in CleavingContext.load_all(broker)])
            self.assertEqual({True}, contexts)

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

        # and repeat checks to use shard ranges now cached in proxy
        headers, actual_listing = self.assert_container_listing(obj_names)
        self.assertEqual(headers['last-modified'],
                         pre_sharding_headers['last-modified'])
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name,
                                                query_string='reverse=on')
        self.assertEqual(pre_sharding_listing[::-1], listing)

        # Now put some new objects into first shard, taking its count to
        # 3 shard ranges' worth
        more_obj_names = [
            'beta%03d' % x for x in range(self.max_shard_size)]
        self.put_objects(more_obj_names)

        # The listing includes new objects (shard ranges haven't changed, just
        # their object content, so cached shard ranges are still correct)...
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

                contexts = list(CleavingContext.load_all(broker))
                self.assertEqual(len(contexts), 1)
                context, _lm = contexts[0]
                self.assertIs(context.cleaving_done, False)
                self.assertIs(context.misplaced_done, True)
                self.assertEqual(context.ranges_done, 2)
                self.assertEqual(context.ranges_todo, 1)
                self.assertEqual(context.max_row,
                                 self.max_shard_size * 3 // 2)

        # but third replica still has no idea it should be sharding
        self.assertLengthEqual(found_for_shard['normal_dbs'], 3)
        broker = ContainerBroker(found_for_shard['normal_dbs'][2])
        self.assertEqual(ShardRange.ACTIVE, broker.get_own_shard_range().state)

        # ...but once sharder runs on third replica it will learn its state and
        # fetch its sub-shard ranges durng audit; note that any root replica on
        # the stopped container server also won't know about the shards being
        # in sharding state, so leave that server stopped for now so that shard
        # fetches its state from an up-to-date root replica
        self.sharders.once(
            number=shard_1_nodes[2],
            additional_args='--partitions=%s' % shard_1_part)

        # third replica is sharding and has sub-shard ranges so can start
        # cleaving...
        found_for_shard = self.categorize_container_dir_content(
            shard_1.account, shard_1.container)
        self.assertLengthEqual(found_for_shard['shard_dbs'], 3)
        self.assertLengthEqual(found_for_shard['normal_dbs'], 3)
        sharding_broker = ContainerBroker(found_for_shard['normal_dbs'][2])
        self.assertEqual('sharding', sharding_broker.get_db_state())
        self.assertEqual(
            ShardRange.SHARDING, sharding_broker.get_own_shard_range().state)
        self.assertEqual(3, len(sharding_broker.get_shard_ranges()))

        # there may also be a sub-shard replica missing so run replicators on
        # all nodes to fix that if necessary
        self.brain.servers.start(number=shard_1_nodes[2])
        self.replicators.once()

        # Now that the replicators have all run, third replica sees cleaving
        # contexts for the first two (plus its own cleaving context)
        contexts = list(CleavingContext.load_all(sharding_broker))
        self.assertEqual(len(contexts), 3)
        broker_id = broker.get_info()['id']
        self.assertIn(broker_id, [ctx[0].ref for ctx in contexts])

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

        # Before writing, kill the cache
        self.memcache.delete(get_cache_key(
            self.account, self.container_name, shard='updating'))
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
        # Also run replicators to settle cleaving contexts
        self.replicators.once()

        # check original first shard range shards
        for db_file in found_for_shard['shard_dbs']:
            broker = ContainerBroker(db_file)
            with annotate_failure('shard db file %s. ' % db_file):
                self.assertIs(False, broker.is_root_container())
                self.assertEqual('sharded', broker.get_db_state())
                self.assertEqual(
                    [ShardRange.ACTIVE] * 3,
                    [sr.state for sr in broker.get_shard_ranges()])

                # Contexts should still be there, and should be complete
                contexts = set([ctx.done()
                                for ctx, _
                                in CleavingContext.load_all(broker)])
                self.assertEqual({True}, contexts)

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
                self.assert_shard_ranges_contiguous(4, list(by_name.values()))
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

    def test_listing_under_populated_replica(self):
        # the leader node and one other primary have all the objects and will
        # cleave to 4 shard ranges, but the third primary only has 1 object in
        # the final shard range
        obj_names = self._make_object_names(2 * self.max_shard_size)
        self.brain.servers.stop(number=self.brain.node_numbers[2])
        self.put_objects(obj_names)
        self.brain.servers.start(number=self.brain.node_numbers[2])
        subset_obj_names = [obj_names[-1]]
        self.put_objects(subset_obj_names)
        self.brain.servers.stop(number=self.brain.node_numbers[2])

        # sanity check: the first 2 primaries will list all objects
        self.assert_container_listing(obj_names, req_hdrs={'x-newest': 'true'})

        # Run sharder on the fully populated nodes, starting with the leader
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        self.sharders.once(number=self.brain.node_numbers[0],
                           additional_args='--partitions=%s' % self.brain.part)
        self.sharders.once(number=self.brain.node_numbers[1],
                           additional_args='--partitions=%s' % self.brain.part)

        # Verify that the first 2 primary nodes have cleaved the first batch of
        # 2 shard ranges
        broker = self.get_broker(self.brain.part, self.brain.nodes[0])
        self.assertEqual('sharding', broker.get_db_state())
        shard_ranges = [dict(sr) for sr in broker.get_shard_ranges()]
        self.assertLengthEqual(shard_ranges, 4)
        self.assertEqual([ShardRange.CLEAVED, ShardRange.CLEAVED,
                          ShardRange.CREATED, ShardRange.CREATED],
                         [sr['state'] for sr in shard_ranges])
        self.assertEqual(
            {False},
            set([ctx.done() for ctx, _ in CleavingContext.load_all(broker)]))

        # listing is complete (from the fully populated primaries at least);
        # the root serves the listing parts for the last 2 shard ranges which
        # are not yet cleaved
        self.assert_container_listing(obj_names, req_hdrs={'x-newest': 'true'})

        # Run the sharder on the under-populated node to get it fully
        # cleaved.
        self.brain.servers.start(number=self.brain.node_numbers[2])
        Manager(['container-replicator']).once(
            number=self.brain.node_numbers[2])
        self.sharders.once(number=self.brain.node_numbers[2],
                           additional_args='--partitions=%s' % self.brain.part)

        broker = self.get_broker(self.brain.part, self.brain.nodes[2])
        self.assertEqual('sharded', broker.get_db_state())
        shard_ranges = [dict(sr) for sr in broker.get_shard_ranges()]
        self.assertLengthEqual(shard_ranges, 4)
        self.assertEqual([ShardRange.ACTIVE, ShardRange.ACTIVE,
                          ShardRange.ACTIVE, ShardRange.ACTIVE],
                         [sr['state'] for sr in shard_ranges])
        self.assertEqual(
            {True, False},
            set([ctx.done() for ctx, _ in CleavingContext.load_all(broker)]))

        # Get a consistent view of shard range states then check listing
        Manager(['container-replicator']).once(
            number=self.brain.node_numbers[2])
        # oops, the listing is incomplete because the last 2 listing parts are
        # now served by the under-populated shard ranges.
        self.assert_container_listing(
            obj_names[:self.max_shard_size] + subset_obj_names,
            req_hdrs={'x-newest': 'true'})

        # but once another replica has completed cleaving the listing is
        # complete again
        self.sharders.once(number=self.brain.node_numbers[1],
                           additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_listing(obj_names, req_hdrs={'x-newest': 'true'})

    def assertInAsyncFile(self, async_path, expected):
        with open(async_path, 'rb') as fd:
            async_data = pickle.load(fd)

        errors = []
        for k, v in expected.items():
            if k not in async_data:
                errors.append("Key '%s' does not exist" % k)
                continue
            if async_data[k] != v:
                errors.append(
                    "Exp value %s != %s" % (str(v), str(async_data[k])))
                continue

        if errors:
            self.fail('\n'.join(errors))

    def assertNotInAsyncFile(self, async_path, not_expect_keys):
        with open(async_path, 'rb') as fd:
            async_data = pickle.load(fd)

        errors = []
        for k in not_expect_keys:
            if k in async_data:
                errors.append(
                    "Key '%s' exists with value '%s'" % (k, async_data[k]))
                continue

        if errors:
            self.fail('\n'.join(errors))

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

        # Check the async pendings, they are unsharded so that's the db_state
        async_files = self.gather_async_pendings()
        self.assertTrue(async_files)
        for af in async_files:
            self.assertInAsyncFile(af, {'db_state': 'unsharded'})
            self.assertNotInAsyncFile(af, ['container_path'])

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

        async_files = self.gather_async_pendings()
        self.assertFalse(async_files)

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
            [x['name'] for x in listing[:len(start_listing)]],
            start_listing)
        # we can't assert much about the remaining listing, other than that
        # there should be something
        self.assertTrue(
            [x['name'] for x in listing[len(start_listing):]])
        self.assertIn('x-container-object-count', headers)
        self.assertEqual(str(len(listing)),
                         headers['x-container-object-count'])
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name,
                                                query_string='reverse=on')
        self.assertEqual([x['name'] for x in listing[-len(start_listing):]],
                         list(reversed(start_listing)))
        self.assertIn('x-container-object-count', headers)
        self.assertEqual(str(len(listing)),
                         headers['x-container-object-count'])
        self.assertTrue(
            [x['name'] for x in listing[:-len(start_listing)]])

        # Run the sharders again to get everything to settle
        self.sharders.once()
        found = self.categorize_container_dir_content()
        self.assertLengthEqual(found['shard_dbs'], 3)
        self.assertLengthEqual(found['normal_dbs'], 0)
        # now all shards have been cleaved we should get the complete listing
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name)
        self.assertEqual([x['name'] for x in listing],
                         obj_names)

        # Create a few more objects in async pending. Check them, they should
        # now have the correct db_state as sharded
        more_obj_names = self._make_object_names(10, self.max_shard_size * 2)

        # No one should get these updates
        self.brain.servers.stop()
        self.put_objects(more_obj_names)
        self.brain.servers.start()

        async_files = self.gather_async_pendings()
        self.assertTrue(async_files)
        for af in async_files:
            # They should have a sharded db_state
            self.assertInAsyncFile(af, {'db_state': 'sharded'})
            # But because the container-servers were down, they wont have
            # container-path (because it couldn't get a shard range back)
            self.assertNotInAsyncFile(af, ['container_path'])

        # they don't exist yet
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name)
        self.assertEqual([x['name'] for x in listing], obj_names)

        # Now clear them out and they should now exist where we expect.
        Manager(['object-updater']).once()
        headers, listing = client.get_container(self.url, self.token,
                                                self.container_name)
        self.assertEqual([x['name'] for x in listing],
                         obj_names + more_obj_names)

        # And they're cleared up
        async_files = self.gather_async_pendings()
        self.assertFalse(async_files)

        # If we take 1/2 the nodes offline when we add some more objects,
        # we should get async pendings with container-path because there
        # was a container-server to respond.
        even_more_obj_names = self._make_object_names(
            10, self.max_shard_size * 2 + 10)

        self.brain.stop_primary_half()
        self.put_objects(even_more_obj_names)
        self.brain.start_primary_half()

        async_files = self.gather_async_pendings()
        self.assertTrue(async_files)
        for af in async_files:
            # They should have a sharded db_state AND container_path
            self.assertInAsyncFile(af, {'db_state': 'sharded',
                                        'container_path': mock.ANY})

        Manager(['object-updater']).once()

        # And they're cleared up
        async_files = self.gather_async_pendings()
        self.assertFalse(async_files)

    def test_shrinking(self):
        int_client = self.make_internal_client()

        def check_node_data(node_data, exp_hdrs, exp_obj_count, exp_shards,
                            exp_sharded_root_range=False):
            hdrs, range_data = node_data
            self.assert_dict_contains(exp_hdrs, hdrs)
            sharded_root_range = False
            other_range_data = []
            for data in range_data:
                sr = ShardRange.from_dict(data)
                if (sr.account == self.account and
                        sr.container == self.container_name and
                        sr.state == ShardRange.SHARDED):
                    # only expect one root range
                    self.assertFalse(sharded_root_range, range_data)
                    sharded_root_range = True
                    self.assertEqual(ShardRange.MIN, sr.lower, sr)
                    self.assertEqual(ShardRange.MAX, sr.upper, sr)
                else:
                    # include active root range in further assertions
                    other_range_data.append(data)
            self.assertEqual(exp_sharded_root_range, sharded_root_range)
            self.assert_shard_ranges_contiguous(exp_shards, other_range_data)
            self.assert_total_object_count(exp_obj_count, other_range_data)

        def check_shard_nodes_data(node_data, expected_state='unsharded',
                                   expected_shards=0, exp_obj_count=0,
                                   exp_sharded_root_range=False):
            # checks that shard range is consistent on all nodes
            root_path = '%s/%s' % (self.account, self.container_name)
            exp_shard_hdrs = {
                'X-Container-Sysmeta-Shard-Quoted-Root': quote(root_path),
                'X-Backend-Sharding-State': expected_state}
            object_counts = []
            bytes_used = []
            for node_id, node_data in node_data.items():
                with annotate_failure('Node id %s.' % node_id):
                    check_node_data(
                        node_data, exp_shard_hdrs, exp_obj_count,
                        expected_shards, exp_sharded_root_range)
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

            # Before writing, kill the cache
            self.memcache.delete(get_cache_key(
                self.account, self.container_name, shard='updating'))

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

            # put a new object 'alpha' in first shard range
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

            # Before writing, kill the cache
            self.memcache.delete(get_cache_key(
                self.account, self.container_name, shard='updating'))

            self.put_objects([beta])
            self.brain.servers.start()
            async_pendings = self.gather_async_pendings()
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

            # ...until the sharders run and update root; reclaim tombstones so
            # that the shard is shrinkable
            shard_0_part = self.get_part_and_node_numbers(
                orig_shard_ranges[0])[0]
            for conf_index in self.configs['container-sharder'].keys():
                self.run_custom_sharder(conf_index, {'reclaim_age': 0},
                                        override_partitions=[shard_0_part])
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
            # donor has the acceptor shard range but not the root shard range
            # because the root is still in ACTIVE state;
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
                    self.assertEqual(ShardRange.SHRUNK, own_sr.state)
                    self.assertTrue(own_sr.deleted)

            # delete all the second shard's object apart from 'alpha'
            for obj in second_shard_objects:
                client.delete_object(
                    self.url, self.token, self.container_name, obj)

            self.assert_container_listing([alpha])

            # run sharders: second range should not shrink away yet because it
            # has tombstones
            self.sharders.once()  # second shard updates root stats
            self.assert_container_listing([alpha])
            self.sharders.once()  # root finds shrinkable shard
            self.assert_container_listing([alpha])
            self.sharders.once()  # shards shrink themselves
            self.assert_container_listing([alpha])

            # the acceptor shard is intact...
            shard_nodes_data = self.direct_get_container_shard_ranges(
                orig_shard_ranges[1].account, orig_shard_ranges[1].container)
            obj_count, bytes_used = check_shard_nodes_data(shard_nodes_data)
            self.assertEqual(1, obj_count)

            # run sharders to reclaim tombstones so that the second shard is
            # shrinkable
            shard_1_part = self.get_part_and_node_numbers(
                orig_shard_ranges[1])[0]
            for conf_index in self.configs['container-sharder'].keys():
                self.run_custom_sharder(conf_index, {'reclaim_age': 0},
                                        override_partitions=[shard_1_part])
            self.assert_container_listing([alpha])

            # run sharders so second range shrinks away, requires up to 2
            # cycles
            self.sharders.once()  # root finds shrinkable shard
            self.assert_container_listing([alpha])
            self.sharders.once()  # shards shrink themselves
            self.assert_container_listing([alpha])

            # the second shard range has sharded and is empty
            shard_nodes_data = self.direct_get_container_shard_ranges(
                orig_shard_ranges[1].account, orig_shard_ranges[1].container)
            check_shard_nodes_data(
                shard_nodes_data, expected_state='sharded', expected_shards=1,
                exp_obj_count=0)

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

    def test_delete_root_reclaim(self):
        all_obj_names = self._make_object_names(self.max_shard_size)
        self.put_objects(all_obj_names)
        # Shard the container
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        for n in self.brain.node_numbers:
            self.sharders.once(
                number=n, additional_args='--partitions=%s' % self.brain.part)
        # sanity checks
        self.assert_container_states('sharded', 2)
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

        # and now we can delete it!
        client.delete_container(self.url, self.token, self.container_name)
        self.assert_container_post_fails('deleted')
        self.assert_container_not_found()

        # see if it will reclaim
        Manager(['container-updater']).once()
        for conf_file in self.configs['container-replicator'].values():
            conf = utils.readconf(conf_file, 'container-replicator')
            conf['reclaim_age'] = 0
            ContainerReplicator(conf).run_once()

        # we don't expect warnings from sharder root audits
        for conf_index in self.configs['container-sharder'].keys():
            sharder = self.run_custom_sharder(conf_index, {})
            self.assertEqual([], sharder.logger.get_lines_for_level('warning'))

        # until the root wants to start reclaiming but we haven't shrunk yet!
        found_warning = False
        for conf_index in self.configs['container-sharder'].keys():
            sharder = self.run_custom_sharder(conf_index, {'reclaim_age': 0})
            warnings = sharder.logger.get_lines_for_level('warning')
            if warnings:
                self.assertTrue(warnings[0].startswith(
                    'Reclaimable db stuck waiting for shrinking'))
                self.assertEqual(1, len(warnings))
                found_warning = True
        self.assertTrue(found_warning)

        # TODO: shrink empty shards and assert everything reclaims

    def _setup_replication_scenario(self, num_shards, extra_objs=('alpha',)):
        # Get cluster to state where 2 replicas are sharding or sharded but 3rd
        # replica is unsharded and has an object that the first 2 are missing.

        # put objects while all servers are up
        obj_names = self._make_object_names(
            num_shards * self.max_shard_size // 2)
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

        # Check the current progress. It shouldn't be complete.
        recon = direct_client.direct_get_recon(leader_node, "sharding")
        expected_in_progress = {'all': [{'account': 'AUTH_test',
                                         'active': 0,
                                         'cleaved': 2,
                                         'created': 2,
                                         'found': 0,
                                         'db_state': 'sharding',
                                         'state': 'sharding',
                                         'error': None,
                                         'file_size': mock.ANY,
                                         'meta_timestamp': mock.ANY,
                                         'node_index': 0,
                                         'object_count': len(obj_names),
                                         'container': mock.ANY,
                                         'path': mock.ANY,
                                         'root': mock.ANY}]}
        actual = recon['sharding_stats']['sharding']['sharding_in_progress']
        self.assertEqual(expected_in_progress, actual)

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

        # Check the leader's progress again, this time is should be complete
        recon = direct_client.direct_get_recon(leader_node, "sharding")
        expected_in_progress = {'all': [{'account': 'AUTH_test',
                                         'active': 4,
                                         'cleaved': 0,
                                         'created': 0,
                                         'found': 0,
                                         'db_state': 'sharded',
                                         'state': 'sharded',
                                         'error': None,
                                         'file_size': mock.ANY,
                                         'meta_timestamp': mock.ANY,
                                         'node_index': 0,
                                         'object_count': len(obj_names),
                                         'container': mock.ANY,
                                         'path': mock.ANY,
                                         'root': mock.ANY}]}
        actual = recon['sharding_stats']['sharding']['sharding_in_progress']
        self.assertEqual(expected_in_progress, actual)

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
        self.assert_container_states('sharded', 2)
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

    def _do_test_sharded_can_get_objects_different_policy(self,
                                                          policy_idx,
                                                          new_policy_idx):
        # create sharded container
        client.delete_container(self.url, self.token, self.container_name)
        self.brain.put_container(policy_index=int(policy_idx))
        all_obj_names = self._make_object_names(self.max_shard_size)
        self.put_objects(all_obj_names)
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        for n in self.brain.node_numbers:
            self.sharders.once(
                number=n, additional_args='--partitions=%s' % self.brain.part)
        # empty and delete
        self.delete_objects(all_obj_names)
        shard_ranges = self.get_container_shard_ranges()
        self.run_sharders(shard_ranges)
        client.delete_container(self.url, self.token, self.container_name)

        # re-create with new_policy_idx
        self.brain.put_container(policy_index=int(new_policy_idx))

        # we re-use shard ranges
        new_shard_ranges = self.get_container_shard_ranges()
        self.assertEqual(shard_ranges, new_shard_ranges)
        self.put_objects(all_obj_names)

        # The shard is still on the old policy index, but the root spi
        # is passed to shard container server and is used to pull objects
        # of that index out.
        self.assert_container_listing(all_obj_names)
        # although a head request is getting object count for the shard spi
        self.assert_container_object_count(0)

        # we can force the listing to use the old policy index in which case we
        # expect no objects to be listed
        try:
            resp = self.internal_client.make_request(
                'GET',
                path=self.internal_client.make_path(
                    self.account, self.container_name),
                headers={'X-Backend-Storage-Policy-Index': str(policy_idx)},
                acceptable_statuses=(2,),
                params={'format': 'json'}
            )
        except UnexpectedResponse as exc:
            self.fail('Listing failed with %s' % exc.resp.status)

        self.assertEqual([], json.loads(b''.join(resp.app_iter)))

    @unittest.skipIf(len(ENABLED_POLICIES) < 2, "Need more than one policy")
    def test_sharded_can_get_objects_different_policy(self):
        policy_idx = self.policy.idx
        new_policy_idx = [pol.idx for pol in ENABLED_POLICIES
                          if pol != self.policy.idx][0]
        self._do_test_sharded_can_get_objects_different_policy(
            policy_idx, new_policy_idx)

    @unittest.skipIf(len(ENABLED_POLICIES) < 2, "Need more than one policy")
    def test_sharded_can_get_objects_different_policy_reversed(self):
        policy_idx = [pol.idx for pol in ENABLED_POLICIES
                      if pol != self.policy][0]
        new_policy_idx = self.policy.idx
        self._do_test_sharded_can_get_objects_different_policy(
            policy_idx, new_policy_idx)

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
        self.assert_container_states('sharded', 2)
        self.assert_container_delete_fails()
        self.assert_container_has_shard_sysmeta()
        self.assert_container_post_ok('sharded')
        self.assert_container_listing(all_obj_names)

        # delete all objects - updates redirected to shards
        self.delete_objects(all_obj_names)
        self.assert_container_listing([])
        self.assert_container_post_ok('has objects')

        # run sharder on shard containers to update root stats; reclaim
        # the tombstones so that the shards appear to be shrinkable
        shard_ranges = self.get_container_shard_ranges()
        self.assertLengthEqual(shard_ranges, 2)
        shard_partitions = [self.get_part_and_node_numbers(sr)[0]
                            for sr in shard_ranges]
        for conf_index in self.configs['container-sharder'].keys():
            self.run_custom_sharder(conf_index, {'reclaim_age': 0},
                                    override_partitions=shard_partitions)
        self.assert_container_object_count(0)

        # First, test a misplaced object moving from one shard to another.
        # with one shard server down, put a new 'alpha' object...
        shard_part, shard_nodes = self.get_part_and_node_numbers(
            shard_ranges[0])
        self.brain.servers.stop(number=shard_nodes[2])
        self.put_objects(['alpha'])
        self.assert_container_listing(['alpha'])
        self.assert_container_object_count(0)
        self.assertLengthEqual(self.gather_async_pendings(), 1)
        self.brain.servers.start(number=shard_nodes[2])

        # run sharder on root to discover first shrink candidate
        self.sharders.once(additional_args='--partitions=%s' % self.brain.part)
        # then run sharder on the shard node without the alpha object
        self.sharders.once(additional_args='--partitions=%s' % shard_part,
                           number=shard_nodes[2])
        # root sees first shard has shrunk
        self.assertLengthEqual(self.get_container_shard_ranges(), 1)
        # cached shard ranges still show first shard range as active so listing
        # will include 'alpha' if the shard listing is fetched from node (0,1)
        # but not if fetched from node 2; to achieve predictability we use
        # x-newest to use shard ranges from the root so that only the second
        # shard range is used for listing, so alpha object not in listing
        self.assert_container_listing([], req_hdrs={'x-newest': 'true'})
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
        # Before writing, kill the cache
        self.memcache.delete(get_cache_key(
            self.account, self.container_name, shard='updating'))
        self.delete_objects(['alpha'])
        self.put_objects(['beta'])
        self.assert_container_listing(['beta'])
        self.assert_container_object_count(1)
        self.assertLengthEqual(self.gather_async_pendings(), 2)
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
                  'etag': md5(usedforsecurity=False).hexdigest(),
                  'deleted': deleted,
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
        self.assert_container_states('sharded', 2)
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

        # run sharder on first shard container to update root stats; reclaim
        # the tombstones so that the shard appears to be shrinkable
        shard_0_part = self.get_part_and_node_numbers(shard_ranges[0])[0]
        for conf_index in self.configs['container-sharder'].keys():
            self.run_custom_sharder(conf_index, {'reclaim_age': 0},
                                    override_partitions=[shard_0_part])
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
        # delete most objects from second shard range, reclaim the tombstones,
        # and run sharder on root to discover second shrink candidate
        self.delete_objects(shard_1_objects)
        shard_1_part = self.get_part_and_node_numbers(shard_ranges[1])[0]
        for conf_index in self.configs['container-sharder'].keys():
            self.run_custom_sharder(conf_index, {'reclaim_age': 0},
                                    override_partitions=[shard_1_part])
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

    def test_misplaced_object_movement_from_deleted_shard(self):
        def merge_object(shard_range, name, deleted=0):
            # it's hard to get a test to put a misplaced object into a shard,
            # so this hack is used force an object record directly into a shard
            # container db. Note: the actual object won't exist, we're just
            # using this to test object records in container dbs.
            shard_part, shard_nodes = self.brain.ring.get_nodes(
                shard_range.account, shard_range.container)
            shard_broker = self.get_shard_broker(shard_range)
            # In this test we want to merge into a deleted container shard
            shard_broker.delete_db(Timestamp.now().internal)
            shard_broker.merge_items(
                [{'name': name, 'created_at': Timestamp.now().internal,
                  'size': 0, 'content_type': 'text/plain',
                  'etag': md5(usedforsecurity=False).hexdigest(),
                  'deleted': deleted,
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
        self.assert_container_states('sharded', 2)
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
        shard_0_part = self.get_part_and_node_numbers(shard_ranges[0])[0]
        for conf_index in self.configs['container-sharder'].keys():
            self.run_custom_sharder(conf_index, {'reclaim_age': 0},
                                    override_partitions=[shard_0_part])
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

        # check the deleted shard did not push the wrong root path into the
        # other container
        for replica in 0, 1, 2:
            shard_x_broker = self.get_shard_broker(shard_ranges[1], replica)
            self.assertEqual("%s/%s" % (self.account, self.container_name),
                             shard_x_broker.root_path)

        # run the sharder of the existing shard to update the root stats
        # to prove the misplaced object was moved to the other shard _and_
        # the other shard still has the correct root because it updates root's
        # stats
        self.run_sharders(shard_ranges[1])
        self.assert_container_object_count(len(shard_1_objects) + 1)

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
            num_shards * self.max_shard_size // 2)
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
        self.assert_container_states('sharded', 3)

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


class TestShardedAPI(BaseTestContainerSharding):
    def _assert_namespace_equivalence(
            self, namespaces_list, other_namespaces_list):
        # verify given lists are equivalent when cast to Namespaces
        self.assertEqual(len(namespaces_list), len(other_namespaces_list))
        self.assertEqual(
            [Namespace(sr.name, sr.lower, sr.upper)
             for sr in namespaces_list],
            [Namespace(sr.name, sr.lower, sr.upper)
             for sr in other_namespaces_list])

    def test_GET(self):
        all_obj_names = self._make_object_names(10)
        self.put_objects(all_obj_names)

        # unsharded container
        objs = self.get_container_objects()
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        objs = self.get_container_objects(
            headers={'X-Backend-Record-Type': 'auto'})
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        objs = self.get_container_objects(
            headers={'X-Backend-Record-Type': 'object'})
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        objs = self.get_container_objects(
            headers={'X-Backend-Record-Type': 'banana'})
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        shard_ranges = self.get_container_shard_ranges()
        self.assertFalse(shard_ranges)

        # Shard the container
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '5', '--enable', '--minimum-shard-size', '5'])
        self.replicators.once()
        # "Run container-sharder on all nodes to shard the container."
        # first pass cleaves 2 shards
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        # sanity check
        self.assert_container_states('sharded', 2)

        orig_shard_ranges = self.get_container_shard_ranges()
        self.assertEqual(2, len(orig_shard_ranges))
        namespaces = self.get_container_namespaces()
        self._assert_namespace_equivalence(orig_shard_ranges, namespaces)

        # the container is sharded so *all* shard ranges should satisfy
        # updating and listing state aliases
        shard_ranges = self.get_container_shard_ranges(
            params={'states': 'updating'})
        self._assert_namespace_equivalence(orig_shard_ranges, shard_ranges)

        shard_ranges = self.get_container_shard_ranges(
            params={'states': 'listing'})
        self._assert_namespace_equivalence(orig_shard_ranges, shard_ranges)

        shard_ranges = self.get_container_shard_ranges(
            headers={'X-Newest': 'true'},
            params={'states': 'listing'})
        self._assert_namespace_equivalence(orig_shard_ranges, shard_ranges)

        # this is what the sharder requests...
        shard_ranges = self.get_container_shard_ranges(
            headers={'X-Newest': 'true'},
            params={'states': 'auditing'})
        own_ns = Namespace('%s/%s' % (self.account, self.container_name),
                           lower='', upper='')
        self._assert_namespace_equivalence(orig_shard_ranges + [own_ns],
                                           shard_ranges)

        shard_ranges = self.get_container_shard_ranges(
            params={'includes': all_obj_names[1]})
        self._assert_namespace_equivalence(orig_shard_ranges[:1], shard_ranges)
        namespaces = self.get_container_namespaces(
            params={'includes': all_obj_names[1]})
        self._assert_namespace_equivalence(shard_ranges, namespaces)

        shard_ranges = self.get_container_shard_ranges(
            # override 'includes'
            headers={'X-Backend-Override-Shard-Name-Filter': 'sharded'},
            params={'includes': all_obj_names[1]})
        self._assert_namespace_equivalence(orig_shard_ranges, shard_ranges)
        namespaces = self.get_container_namespaces(
            # override 'includes'
            headers={'X-Backend-Override-Shard-Name-Filter': 'sharded'},
            params={'includes': all_obj_names[1]})
        self._assert_namespace_equivalence(shard_ranges, namespaces)

        shard_ranges = self.get_container_shard_ranges(
            params={'end_marker': all_obj_names[1]})
        self._assert_namespace_equivalence(orig_shard_ranges[:1], shard_ranges)
        namespaces = self.get_container_namespaces(
            params={'end_marker': all_obj_names[1]})
        self._assert_namespace_equivalence(shard_ranges, namespaces)

        shard_ranges = self.get_container_shard_ranges(
            # override 'end_marker'
            headers={'X-Backend-Override-Shard-Name-Filter': 'sharded'},
            params={'end_marker': all_obj_names[1]})
        self._assert_namespace_equivalence(orig_shard_ranges, shard_ranges)
        namespaces = self.get_container_namespaces(
            # override 'end_marker'
            headers={'X-Backend-Override-Shard-Name-Filter': 'sharded'},
            params={'end_marker': all_obj_names[1]})
        self._assert_namespace_equivalence(shard_ranges, namespaces)

        shard_ranges = self.get_container_shard_ranges(
            params={'reverse': 'true'})
        self._assert_namespace_equivalence(list(reversed(orig_shard_ranges)),
                                           shard_ranges)
        namespaces = self.get_container_namespaces(
            params={'reverse': 'true'})
        self._assert_namespace_equivalence(shard_ranges, namespaces)

        shard_ranges = self.get_container_shard_ranges(
            # override 'reverse'
            headers={'X-Backend-Override-Shard-Name-Filter': 'sharded'},
            params={'reverse': 'true'})
        self._assert_namespace_equivalence(orig_shard_ranges, shard_ranges)
        namespaces = self.get_container_namespaces(
            # override 'reverse'
            headers={'X-Backend-Override-Shard-Name-Filter': 'sharded'},
            params={'reverse': 'true'})
        self._assert_namespace_equivalence(shard_ranges, namespaces)

        objs = self.get_container_objects()
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        objs = self.get_container_objects(
            headers={'X-Newest': 'true'})
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        objs = self.get_container_objects(
            headers={'X-Backend-Record-Type': 'auto'})
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        objs = self.get_container_objects(
            headers={'X-Backend-Record-Type': 'banana'})
        self.assertEqual(all_obj_names, [obj['name'] for obj in objs])

        # note: explicitly asking for the root object rows, but it has None
        objs = self.get_container_objects(
            headers={'X-Backend-Record-Type': 'object'})
        self.assertEqual([], objs)


class TestContainerShardingMoreUTF8(TestContainerSharding):
    def _make_object_names(self, number, start=0):
        # override default with names that include non-ascii chars
        name_length = self.cluster_info['swift']['max_object_name_length']
        obj_names = []
        for x in range(start, start + number):
            name = (u'obj-\u00e4\u00ea\u00ec\u00f2\u00fb-%04d' % x)
            name = name.encode('utf8').ljust(name_length, b'o').decode('utf8')
            obj_names.append(name)
        return obj_names

    def _setup_container_name(self):
        # override default with max length name that includes non-ascii chars
        super(TestContainerShardingMoreUTF8, self)._setup_container_name()
        name_length = self.cluster_info['swift']['max_container_name_length']
        cont_name = \
            self.container_name + u'-\u00e4\u00ea\u00ec\u00f2\u00fb\u1234'
        self.container_name = cont_name.encode('utf8').ljust(
            name_length, b'x').decode('utf8')


class TestManagedContainerSharding(BaseTestContainerSharding):
    """Test sharding using swift-manage-shard-ranges"""

    def test_manage_shard_ranges(self):
        obj_names = self._make_object_names(10)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})

        # run replicators first time to get sync points set
        self.replicators.once()

        # sanity check: we don't have nearly enough objects for this to shard
        # automatically
        self.sharders_once_non_auto(
            number=self.brain.node_numbers[0],
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(self.brain.nodes[0], 'unsharded', 0)

        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '3', '--enable', '--minimum-shard-size', '2'])
        self.assert_container_state(self.brain.nodes[0], 'unsharded', 3)

        # "Run container-replicator to replicate them to other nodes."
        self.replicators.once()
        # "Run container-sharder on all nodes to shard the container."
        # first pass cleaves 2 shards
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(self.brain.nodes[0], 'sharding', 3)
        self.assert_container_state(self.brain.nodes[1], 'sharding', 3)
        shard_ranges = self.assert_container_state(
            self.brain.nodes[2], 'sharding', 3)
        self.assert_container_listing(obj_names)

        # make the un-cleaved shard update the root container...
        self.assertEqual([3, 3, 4], [sr.object_count for sr in shard_ranges])
        shard_part, nodes = self.get_part_and_node_numbers(shard_ranges[2])
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % shard_part)
        shard_ranges = self.assert_container_state(
            self.brain.nodes[2], 'sharding', 3)
        # ...it does not report zero-stats despite being empty, because it has
        # not yet reached CLEAVED state
        self.assertEqual([3, 3, 4], [sr.object_count for sr in shard_ranges])

        # second pass cleaves final shard
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)

        # Everybody's settled
        self.assert_container_state(self.brain.nodes[0], 'sharded', 3)
        self.assert_container_state(self.brain.nodes[1], 'sharded', 3)
        shard_ranges = self.assert_container_state(
            self.brain.nodes[2], 'sharded', 3)
        self.assertEqual([3, 3, 4], [sr.object_count for sr in shard_ranges])
        self.assert_container_listing(obj_names)

    def test_manage_shard_ranges_compact(self):
        # verify shard range compaction using swift-manage-shard-ranges
        obj_names = self._make_object_names(8)
        self.put_objects(obj_names)
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        # run replicators first time to get sync points set, and get container
        # sharded into 4 shards
        self.replicators.once()
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '2', '--enable'])
        self.assert_container_state(self.brain.nodes[0], 'unsharded', 4)
        self.replicators.once()
        # run sharders twice to cleave all 4 shard ranges
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_states('sharded', 4)
        self.assert_container_listing(obj_names)

        # now compact some ranges; use --max-shrinking to allow 2 shrinking
        # shards
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'compact', '--max-expanding', '1', '--max-shrinking', '2',
            '--yes'])
        shard_ranges = self.assert_container_state(
            self.brain.nodes[0], 'sharded', 4)
        self.assertEqual([ShardRange.SHRINKING] * 2 + [ShardRange.ACTIVE] * 2,
                         [sr.state for sr in shard_ranges])
        self.replicators.once()
        self.sharders_once_non_auto()
        # check there's now just 2 remaining shard ranges
        shard_ranges = self.assert_container_state(
            self.brain.nodes[0], 'sharded', 2)
        self.assertEqual([ShardRange.ACTIVE] * 2,
                         [sr.state for sr in shard_ranges])
        self.assert_container_listing(obj_names, req_hdrs={'X-Newest': 'True'})

        # root container own shard range should still be SHARDED
        for i, node in enumerate(self.brain.nodes):
            with annotate_failure('node[%d]' % i):
                broker = self.get_broker(self.brain.part, self.brain.nodes[0])
                self.assertEqual(ShardRange.SHARDED,
                                 broker.get_own_shard_range().state)

        # now compact the final two shard ranges to the root; use
        # --max-shrinking to allow 2 shrinking shards
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'compact', '--yes', '--max-shrinking', '2'])
        shard_ranges = self.assert_container_state(
            self.brain.nodes[0], 'sharded', 2)
        self.assertEqual([ShardRange.SHRINKING] * 2,
                         [sr.state for sr in shard_ranges])
        self.replicators.once()
        self.sharders_once_non_auto()
        self.assert_container_state(self.brain.nodes[0], 'collapsed', 0)
        self.assert_container_listing(obj_names, req_hdrs={'X-Newest': 'True'})

        # root container own shard range should now be ACTIVE
        for i, node in enumerate(self.brain.nodes):
            with annotate_failure('node[%d]' % i):
                broker = self.get_broker(self.brain.part, self.brain.nodes[0])
                self.assertEqual(ShardRange.ACTIVE,
                                 broker.get_own_shard_range().state)

    def test_manage_shard_ranges_repair_root(self):
        # provoke overlaps in root container and repair
        obj_names = self._make_object_names(16)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})

        # run replicators first time to get sync points set
        self.replicators.once()

        # find 4 shard ranges on nodes[0] - let's denote these ranges 0.0, 0.1,
        # 0.2 and 0.3 that are installed with epoch_0
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '4', '--enable'])
        shard_ranges_0 = self.assert_container_state(self.brain.nodes[0],
                                                     'unsharded', 4)

        # *Also* go find 3 shard ranges on *another node*, like a dumb-dumb -
        # let's denote these ranges 1.0, 1.1 and 1.2 that are installed with
        # epoch_1
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[1]),
            'find_and_replace', '7', '--enable'])
        shard_ranges_1 = self.assert_container_state(self.brain.nodes[1],
                                                     'unsharded', 3)

        # Run sharder in specific order so that the replica with the older
        # epoch_0 starts sharding first - this will prove problematic later!
        # On first pass the first replica passes audit, creates shards and then
        # syncs shard ranges with the other replicas, so it has a mix of 0.*
        # shard ranges in CLEAVED state and 1.* ranges in FOUND state. It
        # proceeds to cleave shard 0.0, but after 0.0 cleaving stalls because
        # next in iteration is shard range 1.0 in FOUND state from the other
        # replica that it cannot yet cleave.
        self.sharders_once_non_auto(
            number=self.brain.node_numbers[0],
            additional_args='--partitions=%s' % self.brain.part)

        # On first pass the second replica passes audit (it has its own found
        # ranges and the first replica's created shard ranges but none in the
        # same state overlap), creates its shards and then syncs shard ranges
        # with the other replicas. All of the 7 shard ranges on this replica
        # are now in CREATED state so it proceeds to cleave the first two shard
        # ranges, 0.1 and 1.0.
        self.sharders_once_non_auto(
            number=self.brain.node_numbers[1],
            additional_args='--partitions=%s' % self.brain.part)
        self.replicators.once()

        # Uh-oh
        self.assert_container_state(self.brain.nodes[0], 'sharding', 7)
        self.assert_container_state(self.brain.nodes[1], 'sharding', 7)
        # There's a race: the third replica may be sharding, may be unsharded

        # Try it again a few times
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        self.replicators.once()
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)

        # It's not really fixing itself... the sharder audit will detect
        # overlapping ranges which prevents cleaving proceeding; expect the
        # shard ranges to be mostly still in created state, with one or two
        # possibly cleaved during first pass before the sharding got stalled
        shard_ranges = self.assert_container_state(self.brain.nodes[0],
                                                   'sharding', 7)
        self.assertEqual([ShardRange.CLEAVED] * 2 + [ShardRange.CREATED] * 5,
                         [sr.state for sr in shard_ranges])
        shard_ranges = self.assert_container_state(self.brain.nodes[1],
                                                   'sharding', 7)
        self.assertEqual([ShardRange.CLEAVED] * 2 + [ShardRange.CREATED] * 5,
                         [sr.state for sr in shard_ranges])
        # But hey, at least listings still work! They're just going to get
        # horribly out of date as more objects are added
        self.assert_container_listing(obj_names)

        # 'swift-manage-shard-ranges repair' will choose the second set of 3
        # shard ranges (1.*) over the first set of 4 (0.*) because that's the
        # path with most cleaving progress, and so shrink shard ranges 0.*.
        db_file = self.get_db_file(self.brain.part, self.brain.nodes[0])
        self.assert_subprocess_success(
            ['swift-manage-shard-ranges', db_file, 'repair', '--yes',
             '--min-shard-age', '0'])

        # make sure all root replicas now sync their shard ranges
        self.replicators.once()
        # Run sharder on the shrinking shards. This should not change the state
        # of any of the acceptors, particularly the ones that have yet to have
        # object cleaved from the roots, because we don't want the as yet
        # uncleaved acceptors becoming prematurely active and creating 'holes'
        # in listings. The shrinking shard ranges should however get deleted in
        # root container table.
        self.run_sharders(shard_ranges_0)

        shard_ranges = self.assert_container_state(self.brain.nodes[1],
                                                   'sharding', 3)
        self.assertEqual([ShardRange.CLEAVED] * 1 + [ShardRange.CREATED] * 2,
                         [sr.state for sr in shard_ranges])
        self.assert_container_listing(obj_names)
        # check the unwanted shards did shrink away...
        for shard_range in shard_ranges_0:
            with annotate_failure(shard_range):
                found_for_shard = self.categorize_container_dir_content(
                    shard_range.account, shard_range.container)
                self.assertLengthEqual(found_for_shard['shard_dbs'], 3)
                actual = []
                for shard_db in found_for_shard['shard_dbs']:
                    broker = ContainerBroker(shard_db)
                    own_sr = broker.get_own_shard_range()
                    actual.append(
                        (broker.get_db_state(), own_sr.state, own_sr.deleted))
                self.assertEqual([(SHARDED, ShardRange.SHRUNK, True)] * 3,
                                 actual)

        # At this point one of the first two replicas may have done some useful
        # cleaving of 1.* shards, the other may have only cleaved 0.* shards,
        # and the third replica may have cleaved no shards. We therefore need
        # two more passes of the sharder to get to a predictable state where
        # all replicas have cleaved all three 0.* shards.
        self.sharders_once_non_auto()
        self.sharders_once_non_auto()

        # now we expect all replicas to have just the three 1.* shards, with
        # the 0.* shards all deleted
        brokers = {}
        exp_shard_ranges = sorted(
            [sr.copy(state=ShardRange.SHRUNK, deleted=True)
             for sr in shard_ranges_0] +
            [sr.copy(state=ShardRange.ACTIVE)
             for sr in shard_ranges_1],
            key=ShardRange.sort_key)
        for node in (0, 1, 2):
            with annotate_failure('node %s' % node):
                broker = self.get_broker(self.brain.part,
                                         self.brain.nodes[node])
                brokers[node] = broker
                shard_ranges = broker.get_shard_ranges()
                self.assertEqual(shard_ranges_1, shard_ranges)
                shard_ranges = broker.get_shard_ranges(include_deleted=True)
                self.assertLengthEqual(shard_ranges, len(exp_shard_ranges))
                self.maxDiff = None
                self.assertEqual(exp_shard_ranges, shard_ranges)
                self.assertEqual(ShardRange.SHARDED,
                                 broker.get_own_shard_range().state)

        # Sadly, the first replica to start sharding is still reporting its db
        # state to be 'unsharded' because, although it has sharded, its shard
        # db epoch (epoch_0) does not match its own shard range epoch
        # (epoch_1), and that is because the second replica (with epoch_1)
        # updated the own shard range and replicated it to all other replicas.
        # If we had run the sharder on the second replica before the first
        # replica, then by the time the first replica started sharding it would
        # have learnt the newer epoch_1 and we wouldn't see this inconsistency.
        self.assertEqual(UNSHARDED, brokers[0].get_db_state())
        self.assertEqual(SHARDED, brokers[1].get_db_state())
        self.assertEqual(SHARDED, brokers[2].get_db_state())
        epoch_1 = brokers[1].db_epoch
        self.assertEqual(epoch_1, brokers[2].db_epoch)
        self.assertLess(brokers[0].db_epoch, epoch_1)
        # the root replica that thinks it is unsharded is problematic - it will
        # not return shard ranges for listings, but has no objects, so it's
        # luck of the draw whether we get a listing or not at this point :(

        # Run the sharders again: the first replica that is still 'unsharded'
        # because of the older epoch_0 in its db filename will now start to
        # shard again with a newer epoch_1 db, and will start to re-cleave the
        # 3 active shards, albeit with zero objects to cleave.
        self.sharders_once_non_auto()
        for node in (0, 1, 2):
            with annotate_failure('node %s' % node):
                broker = self.get_broker(self.brain.part,
                                         self.brain.nodes[node])
                brokers[node] = broker
                shard_ranges = broker.get_shard_ranges()
                self.assertEqual(shard_ranges_1, shard_ranges)
                shard_ranges = broker.get_shard_ranges(include_deleted=True)
                self.assertLengthEqual(shard_ranges, len(exp_shard_ranges))
                self.assertEqual(exp_shard_ranges, shard_ranges)
                self.assertEqual(ShardRange.SHARDED,
                                 broker.get_own_shard_range().state)
                self.assertEqual(epoch_1, broker.db_epoch)
        self.assertIn(brokers[0].get_db_state(), (SHARDING, SHARDED))
        self.assertEqual(SHARDED, brokers[1].get_db_state())
        self.assertEqual(SHARDED, brokers[2].get_db_state())

        # This cycle of the sharders also guarantees that all shards have had
        # their state updated to ACTIVE from the root; this was not necessarily
        # true at end of the previous sharder pass because a shard audit (when
        # the shard is updated from a root) may have happened before all roots
        # have had their shard ranges transitioned to ACTIVE.
        for shard_range in shard_ranges_1:
            with annotate_failure(shard_range):
                found_for_shard = self.categorize_container_dir_content(
                    shard_range.account, shard_range.container)
                self.assertLengthEqual(found_for_shard['normal_dbs'], 3)
                actual = []
                for shard_db in found_for_shard['normal_dbs']:
                    broker = ContainerBroker(shard_db)
                    own_sr = broker.get_own_shard_range()
                    actual.append(
                        (broker.get_db_state(), own_sr.state, own_sr.deleted))
                self.assertEqual([(UNSHARDED, ShardRange.ACTIVE, False)] * 3,
                                 actual)

        # We may need one more pass of the sharder before all three shard
        # ranges are cleaved (2 per pass) and all the root replicas are
        # predictably in sharded state. Note: the accelerated cleaving of >2
        # zero-object shard ranges per cycle is defeated if a shard happens
        # to exist on the same node as the root because the roots cleaving
        # process doesn't think that it created the shard db and will therefore
        # replicate it as per a normal cleave.
        self.sharders_once_non_auto()
        for node in (0, 1, 2):
            with annotate_failure('node %s' % node):
                broker = self.get_broker(self.brain.part,
                                         self.brain.nodes[node])
                brokers[node] = broker
                shard_ranges = broker.get_shard_ranges()
                self.assertEqual(shard_ranges_1, shard_ranges)
                shard_ranges = broker.get_shard_ranges(include_deleted=True)
                self.assertLengthEqual(shard_ranges, len(exp_shard_ranges))
                self.assertEqual(exp_shard_ranges, shard_ranges)
                self.assertEqual(ShardRange.SHARDED,
                                 broker.get_own_shard_range().state)
                self.assertEqual(epoch_1, broker.db_epoch)
                self.assertEqual(SHARDED, broker.get_db_state())

        # Finally, with all root replicas in a consistent state, the listing
        # will be be predictably correct
        self.assert_container_listing(obj_names)

    def test_manage_shard_ranges_repair_shard(self):
        # provoke overlaps in a shard container and repair them
        obj_names = self._make_object_names(24)
        initial_obj_names = obj_names[::2]
        # put 12 objects in container
        self.put_objects(initial_obj_names)
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        # run replicators first time to get sync points set
        self.replicators.once()
        # find 3 shard ranges on root nodes[0] and get the root sharded
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '4', '--enable'])
        self.replicators.once()
        # cleave first two shards
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        # cleave third shard
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        # ensure all shards learn their ACTIVE state from root
        self.sharders_once_non_auto()
        for node in (0, 1, 2):
            with annotate_failure('node %d' % node):
                shard_ranges = self.assert_container_state(
                    self.brain.nodes[node], 'sharded', 3)
                for sr in shard_ranges:
                    self.assertEqual(ShardRange.ACTIVE, sr.state)
        self.assert_container_listing(initial_obj_names)

        # add objects to second shard range so it has 8 objects ; this range
        # has bounds (obj-0006,obj-0014]
        root_shard_ranges = self.get_container_shard_ranges()
        self.assertEqual(3, len(root_shard_ranges))
        shard_1 = root_shard_ranges[1]
        self.assertEqual(obj_names[6], shard_1.lower)
        self.assertEqual(obj_names[14], shard_1.upper)
        more_obj_names = obj_names[7:15:2]
        self.put_objects(more_obj_names)
        expected_obj_names = sorted(initial_obj_names + more_obj_names)
        self.assert_container_listing(expected_obj_names)

        shard_1_part, shard_1_nodes = self.brain.ring.get_nodes(
            shard_1.account, shard_1.container)

        # find 3 sub-shards on one shard node; use --force-commits to ensure
        # the recently PUT objects are included when finding the shard range
        # pivot points
        self.assert_subprocess_success([
            'swift-manage-shard-ranges', '--force-commits',
            self.get_db_file(shard_1_part, shard_1_nodes[1], shard_1.account,
                             shard_1.container),
            'find_and_replace', '3', '--enable'])
        # ... and mistakenly find 4 shard ranges on a different shard node :(
        self.assert_subprocess_success([
            'swift-manage-shard-ranges', '--force-commits',
            self.get_db_file(shard_1_part, shard_1_nodes[2], shard_1.account,
                             shard_1.container),
            'find_and_replace', '2', '--enable'])
        # replicate the muddle of shard ranges between shard replicas, merged
        # result is:
        # '' - 6  shard     ACTIVE
        #  6 - 8  sub-shard FOUND
        #  6 - 9  sub-shard FOUND
        #  8 - 10 sub-shard FOUND
        #  9 - 12 sub-shard FOUND
        # 10 - 12 sub-shard FOUND
        # 12 - 14 sub-shard FOUND
        # 12 - 14 sub-shard FOUND
        #  6 - 14 shard     SHARDING
        # 14 - '' shard     ACTIVE
        self.replicators.once()

        # try hard to shard the shard...
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % shard_1_part)
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % shard_1_part)
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % shard_1_part)
        # sharding hasn't completed and there's overlaps in the shard and root:
        # the sub-shards will have been cleaved in the order listed above, but
        # sub-shards (10 -12) and one of (12 - 14) will be overlooked because
        # the cleave cursor will have moved past their namespace before they
        # were yielded by the shard range iterator, so we now have:
        # '' - 6  shard     ACTIVE
        #  6 - 8  sub-shard ACTIVE
        #  6 - 9  sub-shard ACTIVE
        #  8 - 10 sub-shard ACTIVE
        # 10 - 12 sub-shard CREATED
        #  9 - 12 sub-shard ACTIVE
        # 12 - 14 sub-shard CREATED
        # 12 - 14 sub-shard ACTIVE
        # 14 - '' shard     ACTIVE
        sub_shard_ranges = self.get_container_shard_ranges(
            shard_1.account, shard_1.container)
        self.assertEqual(7, len(sub_shard_ranges), sub_shard_ranges)
        root_shard_ranges = self.get_container_shard_ranges()
        self.assertEqual(9, len(root_shard_ranges), root_shard_ranges)
        self.assertEqual([ShardRange.ACTIVE] * 4 +
                         [ShardRange.CREATED, ShardRange.ACTIVE] * 2 +
                         [ShardRange.ACTIVE],
                         [sr.state for sr in root_shard_ranges])

        # fix the overlaps - a set of 3 ACTIVE sub-shards will be chosen and 4
        # other sub-shards will be shrunk away; apply the fix at the root
        # container
        db_file = self.get_db_file(self.brain.part, self.brain.nodes[0])
        self.assert_subprocess_success(
            ['swift-manage-shard-ranges', db_file, 'repair', '--yes',
             '--min-shard-age', '0'])
        self.replicators.once()
        self.sharders_once_non_auto()
        self.sharders_once_non_auto()

        # check root now has just 5 shard ranges
        root_shard_ranges = self.get_container_shard_ranges()
        self.assertEqual(5, len(root_shard_ranges), root_shard_ranges)
        self.assertEqual([ShardRange.ACTIVE] * 5,
                         [sr.state for sr in root_shard_ranges])
        # check there are 1 sharded shard and 4 shrunk sub-shard ranges in the
        # root (note, shard_1's shard ranges aren't updated once it has sharded
        # because the sub-shards report their state to the root; we cannot make
        # assertions about shrunk states in shard_1's shard range table)
        root_shard_ranges = self.get_container_shard_ranges(
            headers={'X-Backend-Include-Deleted': 'true'})
        self.assertEqual(10, len(root_shard_ranges), root_shard_ranges)
        shrunk_shard_ranges = [sr for sr in root_shard_ranges
                               if sr.state == ShardRange.SHRUNK]
        self.assertEqual(4, len(shrunk_shard_ranges), root_shard_ranges)
        self.assertEqual([True] * 4,
                         [sr.deleted for sr in shrunk_shard_ranges])
        sharded_shard_ranges = [sr for sr in root_shard_ranges
                                if sr.state == ShardRange.SHARDED]
        self.assertEqual(1, len(sharded_shard_ranges), root_shard_ranges)

        self.assert_container_listing(expected_obj_names)

    def test_manage_shard_ranges_repair_parent_child_ranges(self):
        # Test repairing a transient parent-child shard range overlap in the
        # root container, expect no repairs to be done.
        # note: be careful not to add a container listing to this test which
        # would get shard ranges into memcache
        obj_names = self._make_object_names(4)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})

        # run replicators first time to get sync points set
        self.container_replicators.once(
            additional_args='--partitions=%s' % self.brain.part)

        # shard root
        root_0_db_file = self.get_db_file(self.brain.part, self.brain.nodes[0])
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            root_0_db_file,
            'find_and_replace', '2', '--enable'])
        self.container_replicators.once(
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_states('unsharded', 2)
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        # get shards to update state from parent...
        self.sharders_once_non_auto()
        self.assert_container_states('sharded', 2)

        # sanity check, all is well
        msg = self.assert_subprocess_success([
            'swift-manage-shard-ranges', root_0_db_file, 'repair', '--gaps',
            '--dry-run'])
        self.assertIn(b'No repairs necessary.', msg)

        # shard first shard into 2 sub-shards while root node 0 is disabled
        self.stop_container_servers(node_numbers=slice(0, 1))
        shard_ranges = self.get_container_shard_ranges()
        shard_brokers = [self.get_shard_broker(shard_ranges[0], node_index=i)
                         for i in range(3)]
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            shard_brokers[0].db_file,
            'find_and_replace', '1', '--enable'])
        shard_part, shard_nodes = self.brain.ring.get_nodes(
            shard_ranges[0].account, shard_ranges[0].container)
        self.container_replicators.once(
            additional_args='--partitions=%s' % shard_part)
        for node in exclude_nodes(shard_nodes, self.brain.nodes[0]):
            self.assert_container_state(
                node, 'unsharded', 2, account=shard_ranges[0].account,
                container=shard_ranges[0].container, part=shard_part)
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % shard_part)
        # get shards to update state from parent...
        self.sharders_once_non_auto()
        for node in exclude_nodes(shard_nodes, self.brain.nodes[0]):
            self.assert_container_state(
                node, 'sharded', 2, account=shard_ranges[0].account,
                container=shard_ranges[0].container, part=shard_part)

        # put an object into the second of the 2 sub-shards so that the shard
        # will update the root next time the sharder is run; do this before
        # restarting root node 0 so that the object update is definitely
        # redirected to a sub-shard by root node 1 or 2.
        new_obj_name = obj_names[0] + 'a'
        self.put_objects([new_obj_name])

        # restart root node 0
        self.brain.servers.start(number=self.brain.node_numbers[0])
        # node 0 DB doesn't know about the sub-shards
        root_brokers = [self.get_broker(self.brain.part, node)
                        for node in self.brain.nodes]
        broker = root_brokers[0]
        self.assertEqual(
            [(ShardRange.ACTIVE, False, ShardRange.MIN, obj_names[1]),
             (ShardRange.ACTIVE, False, obj_names[1], ShardRange.MAX)],
            [(sr.state, sr.deleted, sr.lower, sr.upper)
             for sr in broker.get_shard_ranges(include_deleted=True)])

        for broker in root_brokers[1:]:
            self.assertEqual(
                [(ShardRange.ACTIVE, False, ShardRange.MIN, obj_names[0]),
                 (ShardRange.ACTIVE, False, obj_names[0], obj_names[1]),
                 (ShardRange.SHARDED, True, ShardRange.MIN, obj_names[1]),
                 (ShardRange.ACTIVE, False, obj_names[1], ShardRange.MAX)],
                [(sr.state, sr.deleted, sr.lower, sr.upper)
                 for sr in broker.get_shard_ranges(include_deleted=True)])

        sub_shard = root_brokers[1].get_shard_ranges()[1]
        self.assertEqual(obj_names[0], sub_shard.lower)
        self.assertEqual(obj_names[1], sub_shard.upper)
        sub_shard_part, nodes = self.get_part_and_node_numbers(sub_shard)
        # we want the sub-shard to update root node 0 but not the sharded
        # shard, but there is a small chance the two will be in same partition
        # TODO: how can we work around this?
        self.assertNotEqual(sub_shard_part, shard_part,
                            'You were unlucky, try again')
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % sub_shard_part)

        # now root node 0 has the original shards plus one of the sub-shards
        # but all are active :(
        self.assertEqual(
            [(ShardRange.ACTIVE, False, ShardRange.MIN, obj_names[1]),
             # note: overlap!
             (ShardRange.ACTIVE, False, obj_names[0], obj_names[1]),
             (ShardRange.ACTIVE, False, obj_names[1], ShardRange.MAX)],
            [(sr.state, sr.deleted, sr.lower, sr.upper)
             for sr in root_brokers[0].get_shard_ranges(include_deleted=True)])

        # try to fix the overlap and expect no repair has been done.
        msg = self.assert_subprocess_success(
            ['swift-manage-shard-ranges', root_0_db_file, 'repair', '--yes',
             '--min-shard-age', '0'])
        self.assertIn(
            b'1 donor shards ignored due to parent-child relationship checks',
            msg)

        # verify parent-child checks has prevented repair to be done.
        self.assertEqual(
            [(ShardRange.ACTIVE, False, ShardRange.MIN, obj_names[1]),
             # note: overlap!
             (ShardRange.ACTIVE, False, obj_names[0], obj_names[1]),
             (ShardRange.ACTIVE, False, obj_names[1], ShardRange.MAX)],
            [(sr.state, sr.deleted, sr.lower, sr.upper)
             for sr in root_brokers[0].get_shard_ranges(include_deleted=True)])

        # the transient overlap is 'fixed' in subsequent sharder cycles...
        self.sharders_once_non_auto()
        self.sharders_once_non_auto()
        self.container_replicators.once()

        for broker in root_brokers:
            self.assertEqual(
                [(ShardRange.ACTIVE, False, ShardRange.MIN, obj_names[0]),
                 (ShardRange.ACTIVE, False, obj_names[0], obj_names[1]),
                 (ShardRange.SHARDED, True, ShardRange.MIN, obj_names[1]),
                 (ShardRange.ACTIVE, False, obj_names[1], ShardRange.MAX)],
                [(sr.state, sr.deleted, sr.lower, sr.upper)
                 for sr in broker.get_shard_ranges(include_deleted=True)])

    def test_manage_shard_ranges_repair_root_gap(self):
        # create a gap in root container; repair the gap.
        # note: be careful not to add a container listing to this test which
        # would get shard ranges into memcache
        obj_names = self._make_object_names(8)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})

        # run replicators first time to get sync points set
        self.container_replicators.once(
            additional_args='--partitions=%s' % self.brain.part)

        # shard root
        root_0_db_file = self.get_db_file(self.brain.part, self.brain.nodes[0])
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            root_0_db_file,
            'find_and_replace', '2', '--enable'])
        self.container_replicators.once(
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_states('unsharded', 4)
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        # get shards to update state from parent...
        self.sharders_once_non_auto()
        self.assert_container_states('sharded', 4)

        # sanity check, all is well
        msg = self.assert_subprocess_success([
            'swift-manage-shard-ranges', root_0_db_file, 'repair', '--gaps',
            '--dry-run'])
        self.assertIn(b'No repairs necessary.', msg)

        # deliberately create a gap in root shard ranges (don't ever do this
        # for real)
        # TODO: replace direct broker modification with s-m-s-r merge
        root_brokers = [self.get_broker(self.brain.part, node)
                        for node in self.brain.nodes]
        shard_ranges = root_brokers[0].get_shard_ranges()
        self.assertEqual(4, len(shard_ranges))
        shard_ranges[2].set_deleted()
        root_brokers[0].merge_shard_ranges(shard_ranges)
        shard_ranges = root_brokers[0].get_shard_ranges()
        self.assertEqual(3, len(shard_ranges))
        self.container_replicators.once()

        # confirm that we made a gap.
        for broker in root_brokers:
            self.assertEqual(
                [(ShardRange.ACTIVE, False, ShardRange.MIN, obj_names[1]),
                 (ShardRange.ACTIVE, False, obj_names[1], obj_names[3]),
                 (ShardRange.ACTIVE, True, obj_names[3], obj_names[5]),
                 (ShardRange.ACTIVE, False, obj_names[5], ShardRange.MAX)],
                [(sr.state, sr.deleted, sr.lower, sr.upper)
                 for sr in broker.get_shard_ranges(include_deleted=True)])

        msg = self.assert_subprocess_success([
            'swift-manage-shard-ranges', root_0_db_file, 'repair', '--gaps',
            '--yes'])
        self.assertIn(b'Repairs necessary to fill gaps.', msg)

        self.sharders_once_non_auto()
        self.sharders_once_non_auto()
        self.container_replicators.once()

        # yay! we fixed the gap (without creating an overlap)
        for broker in root_brokers:
            self.assertEqual(
                [(ShardRange.ACTIVE, False, ShardRange.MIN, obj_names[1]),
                 (ShardRange.ACTIVE, False, obj_names[1], obj_names[3]),
                 (ShardRange.ACTIVE, True, obj_names[3], obj_names[5]),
                 (ShardRange.ACTIVE, False, obj_names[3], ShardRange.MAX)],
                [(sr.state, sr.deleted, sr.lower, sr.upper)
                 for sr in broker.get_shard_ranges(include_deleted=True)])

        msg = self.assert_subprocess_success([
            'swift-manage-shard-ranges', root_0_db_file, 'repair',
            '--dry-run', '--min-shard-age', '0'])
        self.assertIn(b'No repairs necessary.', msg)
        msg = self.assert_subprocess_success([
            'swift-manage-shard-ranges', root_0_db_file, 'repair', '--gaps',
            '--dry-run'])
        self.assertIn(b'No repairs necessary.', msg)

        # put an object into the gap namespace
        new_objs = [obj_names[4] + 'a']
        self.put_objects(new_objs)
        # get root stats up to date
        self.sharders_once_non_auto()
        # new object is in listing but old objects in the gap have been lost -
        # don't delete shard ranges!
        self.assert_container_listing(obj_names[:4] + new_objs + obj_names[6:])

    def test_manage_shard_ranges_unsharded_deleted_root(self):
        # verify that a deleted DB will still be sharded

        # choose a node that will not be sharded initially
        sharded_nodes = []
        unsharded_node = None
        for node in self.brain.nodes:
            if self.brain.node_numbers[node['index']] \
                    in self.brain.handoff_numbers:
                unsharded_node = node
            else:
                sharded_nodes.append(node)

        # put some objects - not enough to trigger auto-sharding
        obj_names = self._make_object_names(MIN_SHARD_CONTAINER_THRESHOLD - 1)
        self.put_objects(obj_names)

        # run replicators first time to get sync points set and commit updates
        self.replicators.once()

        # setup sharding...
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, sharded_nodes[0]),
            'find_and_replace', '2', '--enable', '--minimum-shard-size', '1'])

        # Run container-replicator to replicate shard ranges
        self.container_replicators.once()
        self.assert_container_state(sharded_nodes[0], 'unsharded', 2)
        self.assert_container_state(sharded_nodes[1], 'unsharded', 2)
        self.assert_container_state(unsharded_node, 'unsharded', 2)

        # Run container-sharder to shard the 2 primary replicas that did
        # receive the object PUTs
        for num in self.brain.primary_numbers:
            self.sharders_once_non_auto(
                number=num,
                additional_args='--partitions=%s' % self.brain.part)

        # delete the objects - the proxy's will have cached container info with
        # out-of-date db_state=unsharded, so updates go to the root DBs
        self.delete_objects(obj_names)
        # deal with DELETE's being misplaced in root db's...
        for num in self.brain.primary_numbers:
            self.sharders_once_non_auto(
                number=num,
                additional_args='--partitions=%s' % self.brain.part)

        self.assert_container_state(sharded_nodes[0], 'sharded', 2)
        self.assert_container_state(sharded_nodes[1], 'sharded', 2)
        shard_ranges = self.assert_container_state(
            unsharded_node, 'unsharded', 2)

        # get root stats updated - but avoid sharding the remaining root DB
        self.run_sharders(shard_ranges, exclude_partitions=[self.brain.part])
        self.assert_container_listing([])

        # delete the empty container
        client.delete_container(self.url, self.admin_token,
                                self.container_name)

        # sanity check - unsharded DB is deleted
        broker = self.get_broker(self.brain.part, unsharded_node,
                                 self.account, self.container_name)
        self.assertEqual(UNSHARDED, broker.get_db_state())
        self.assertTrue(broker.is_deleted())
        self.assertEqual(0, broker.get_info()['object_count'])
        self.assertEqual(0, broker.get_shard_usage()['object_count'])

        # now shard the final DB
        for num in self.brain.handoff_numbers:
            self.sharders_once_non_auto(
                number=num,
                additional_args='--partitions=%s' % self.brain.part)

        # all DBs should now be sharded and still deleted
        for node in self.brain.nodes:
            with annotate_failure(
                    'node %s in %s'
                    % (node['index'], [n['index'] for n in self.brain.nodes])):
                self.assert_container_state(node, 'sharded', 2,
                                            override_deleted=True)
                broker = self.get_broker(self.brain.part, node,
                                         self.account, self.container_name)
                self.assertEqual(SHARDED, broker.get_db_state())
                self.assertEqual(0, broker.get_info()['object_count'])
                self.assertEqual(0,
                                 broker.get_shard_usage()['object_count'])
                self.assertTrue(broker.is_deleted())

    def test_manage_shard_ranges_unsharded_deleted_root_gets_undeleted(self):
        # verify that an apparently deleted DB (no object rows in root db) will
        # still be sharded and also become undeleted when objects are
        # discovered in the shards

        # choose a node that will not be sharded initially
        sharded_nodes = []
        unsharded_node = None
        for node in self.brain.nodes:
            if self.brain.node_numbers[node['index']] \
                    in self.brain.handoff_numbers:
                unsharded_node = node
            else:
                sharded_nodes.append(node)

        # put some objects, but only to 2 replicas - not enough to trigger
        # auto-sharding
        self.brain.stop_handoff_half()

        obj_names = self._make_object_names(MIN_SHARD_CONTAINER_THRESHOLD - 1)
        self.put_objects(obj_names)
        # run replicators first time to get sync points set and commit puts
        self.replicators.once()

        # setup sharding...
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, sharded_nodes[0]),
            'find_and_replace', '2', '--enable', '--minimum-shard-size', '1'])

        # Run container-replicator to replicate shard ranges - object rows will
        # not be sync'd now there are shard ranges
        for num in self.brain.primary_numbers:
            self.container_replicators.once(number=num)
        self.assert_container_state(sharded_nodes[0], 'unsharded', 2)
        self.assert_container_state(sharded_nodes[1], 'unsharded', 2)

        # revive the stopped node
        self.brain.start_handoff_half()
        self.assert_container_state(unsharded_node, 'unsharded', 0)

        # delete the empty replica
        direct_client.direct_delete_container(
            unsharded_node, self.brain.part, self.account,
            self.container_name)

        # Run container-sharder to shard the 2 primary replicas that did
        # receive the object PUTs
        for num in self.brain.primary_numbers:
            self.sharders_once_non_auto(
                number=num,
                additional_args='--partitions=%s' % self.brain.part)

        self.assert_container_state(sharded_nodes[0], 'sharded', 2)
        self.assert_container_state(sharded_nodes[1], 'sharded', 2)
        # the sharder syncs shard ranges ...
        self.assert_container_state(unsharded_node, 'unsharded', 2,
                                    override_deleted=True)

        # sanity check - unsharded DB is empty and deleted
        broker = self.get_broker(self.brain.part, unsharded_node,
                                 self.account, self.container_name)
        self.assertEqual(UNSHARDED, broker.get_db_state())
        self.assertEqual(0, broker.get_info()['object_count'])
        # the shard ranges do have object count but are in CREATED state so
        # not reported in shard usage...
        self.assertEqual(0, broker.get_shard_usage()['object_count'])
        self.assertTrue(broker.is_deleted())

        # now shard the final DB
        for num in self.brain.handoff_numbers:
            self.sharders_once_non_auto(
                number=num,
                additional_args='--partitions=%s' % self.brain.part)
        shard_ranges = self.assert_container_state(
            unsharded_node, 'sharded', 2, override_deleted=True)

        # and get roots updated and sync'd
        self.container_replicators.once()
        self.run_sharders(shard_ranges, exclude_partitions=[self.brain.part])

        # all DBs should now be sharded and NOT deleted
        for node in self.brain.nodes:
            with annotate_failure(
                    'node %s in %s'
                    % (node['index'], [n['index'] for n in self.brain.nodes])):
                broker = self.get_broker(self.brain.part, node,
                                         self.account, self.container_name)
                self.assertEqual(SHARDED, broker.get_db_state())
                self.assertEqual(3, broker.get_info()['object_count'])
                self.assertEqual(3,
                                 broker.get_shard_usage()['object_count'])
                self.assertFalse(broker.is_deleted())

    def test_handoff_replication_does_not_cause_reset_epoch(self):
        obj_names = self._make_object_names(100)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})

        # run replicators first time to get sync points set
        self.replicators.once()

        # sanity check: we don't have nearly enough objects for this to shard
        # automatically
        self.sharders_once_non_auto(
            number=self.brain.node_numbers[0],
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(self.brain.nodes[0], 'unsharded', 0)

        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '50', '--enable',
            '--minimum-shard-size', '40'])
        self.assert_container_state(self.brain.nodes[0], 'unsharded', 2)

        # "Run container-replicator to replicate them to other nodes."
        self.replicators.once()
        # "Run container-sharder on all nodes to shard the container."
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)

        # Everybody's settled
        self.assert_container_state(self.brain.nodes[0], 'sharded', 2)
        self.assert_container_state(self.brain.nodes[1], 'sharded', 2)
        self.assert_container_state(self.brain.nodes[2], 'sharded', 2)
        self.assert_container_listing(obj_names)

        # now lets put the container again and make sure it lands on a handoff
        self.brain.stop_primary_half()
        self.brain.put_container(policy_index=int(self.policy))
        self.brain.start_primary_half()

        dir_content = self.categorize_container_dir_content(more_nodes=True)
        # the handoff node is considered normal because it doesn't have an
        # epoch
        self.assertEqual(len(dir_content['normal_dbs']), 1)
        self.assertEqual(len(dir_content['shard_dbs']), 3)

        # let's replicate
        self.replicators.once()
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)

        # let's now check the handoff broker it should have all the shards
        handoff_broker = ContainerBroker(dir_content['normal_dbs'][0])
        self.assertEqual(len(handoff_broker.get_shard_ranges()), 2)
        handoff_osr = handoff_broker.get_own_shard_range(no_default=True)
        self.assertIsNotNone(handoff_osr.epoch)

    def test_force_replication_of_a_reset_own_shard_range(self):
        obj_names = self._make_object_names(100)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})

        # run replicators first time to get sync points set
        self.replicators.once()

        # sanity check: we don't have nearly enough objects for this to shard
        # automatically
        self.sharders_once_non_auto(
            number=self.brain.node_numbers[0],
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_state(self.brain.nodes[0], 'unsharded', 0)

        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '50', '--enable',
            '--minimum-shard-size', '40'])
        self.assert_container_state(self.brain.nodes[0], 'unsharded', 2)

        # "Run container-replicator to replicate them to other nodes."
        self.replicators.once()
        # "Run container-sharder on all nodes to shard the container."
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)

        # Everybody's settled
        self.assert_container_state(self.brain.nodes[0], 'sharded', 2)
        self.assert_container_state(self.brain.nodes[1], 'sharded', 2)
        self.assert_container_state(self.brain.nodes[2], 'sharded', 2)
        self.assert_container_listing(obj_names)

        # Lets delete a primary to simulate a new primary and force an
        # own_shard_range reset.
        new_primary = self.brain.nodes[2]
        db_file = self.get_db_file(self.brain.part, new_primary)
        os.remove(db_file)

        # issue a new PUT to create the "new" primary container
        self.brain.put_container(policy_index=int(self.policy))

        # put a bunch of objects that should land in the primary so it'll be
        # shardable (in case this makes any kind of difference).
        self.put_objects(obj_names)

        # The new primary isn't considered a shard_db because it hasn't
        # sunk with the other primaries yet.
        dir_content = self.categorize_container_dir_content()
        self.assertEqual(len(dir_content['normal_dbs']), 1)
        self.assertEqual(len(dir_content['shard_dbs']), 2)

        # run the sharders incase this will trigger a reset osr
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        new_primary_broker = self.get_broker(self.brain.part, new_primary)
        # Nope, still no default/reset osr
        self.assertIsNone(
            new_primary_broker.get_own_shard_range(no_default=True))

        # Let's reset the osr by hand.
        reset_osr = new_primary_broker.get_own_shard_range()
        self.assertIsNone(reset_osr.epoch)
        self.assertEqual(reset_osr.state, ShardRange.ACTIVE)
        new_primary_broker.merge_shard_ranges(reset_osr)

        # now let's replicate with the old primaries
        self.replicators.once()
        # Pull an old primary own_shard_range
        dir_content = self.categorize_container_dir_content()
        old_broker = ContainerBroker(dir_content['shard_dbs'][0])
        old_osr = old_broker.get_own_shard_range()
        new_primary_broker = ContainerBroker(dir_content['normal_dbs'][0])
        new_osr = new_primary_broker.get_own_shard_range()

        # This version stops replicating a remote non-epoch osr over a local
        # epoched osr. But it doesn't do the other way. So it means the
        # primary with non-epoched OSR get's stuck with it, if it is newer then
        # the other epoched versions.
        self.assertIsNotNone(old_osr.epoch)
        self.assertEqual(old_osr.state, ShardRange.SHARDED)

        self.assertIsNone(new_osr.epoch)
        self.assertGreater(new_osr.timestamp, old_osr.timestamp)

    def test_manage_shard_ranges_missing_epoch_no_false_positives(self):
        # when one replica of a shard is sharding before the others, it's epoch
        # is not None but it is normal for the other replica to replicate to it
        # sending their own shard ranges with epoch=None until they also shard
        obj_names = self._make_object_names(4)
        self.put_objects(obj_names)
        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        # run replicators first time to get sync points set, and get container
        # sharded into 4 shards
        self.replicators.once()
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            self.get_db_file(self.brain.part, self.brain.nodes[0]),
            'find_and_replace', '2', '--enable'])
        ranges = self.assert_container_state(
            self.brain.nodes[0], 'unsharded', 2)

        # "Run container-replicator to replicate them to other nodes."
        self.replicators.once()
        # "Run container-sharder on all nodes to shard the container."
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        # Run them again, just so the shards themselves can pull down the
        # latest sharded versions of their OSRs.
        self.sharders_once_non_auto()

        # Everybody's settled
        self.assert_container_state(self.brain.nodes[0], 'sharded', 2)
        self.assert_container_state(self.brain.nodes[1], 'sharded', 2)
        ranges = self.assert_container_state(self.brain.nodes[2], 'sharded', 2)
        self.assert_container_listing(obj_names)

        # Now we need to shard a shard. A shard's OSR always exist and should
        # have an epoch of None, so we should get some false positives.
        # we'll shard ranges[1] which have a range of objs-0002 - MAX
        shard_obj_names = ['objs-0001%d' % i for i in range(2)]
        self.put_objects(shard_obj_names)

        part, shard_node_numbers = self.get_part_and_node_numbers(ranges[1])
        shard_nodes = self.brain.ring.get_part_nodes(part)
        shard_broker = self.get_shard_broker(ranges[1], 0)
        # set the account, container instance variables
        shard_broker.get_info()
        self.replicators.once()
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            shard_broker.db_file,
            'find_and_replace', '2', '--enable'])
        self.assert_container_state(
            shard_nodes[0], 'unsharded', 2,
            shard_broker.account, shard_broker.container, part)

        # index 0 has an epoch now but 1 and 2 don't
        for idx in 1, 2:
            sb = self.get_shard_broker(ranges[1], idx)
            osr = sb.get_own_shard_range(no_default=True)
            self.assertIsNone(osr.epoch)

        expected_false_positive_line_snippet = 'Ignoring remote osr w/o epoch:'
        # run the replicator on the node with an epoch and it'll complain the
        # others dont have an epoch and not set it.
        replicator = self.run_custom_daemon(
            ContainerReplicator, 'container-replicator',
            shard_node_numbers[0], {})
        warnings = replicator.logger.get_lines_for_level('warning')

        self.assertFalse([w for w in warnings
                          if expected_false_positive_line_snippet in w])

        # But it does send the new OSR with an epoch so the others should all
        # have it now.
        for idx in 1, 2:
            sb = self.get_shard_broker(ranges[1], idx)
            osr = sb.get_own_shard_range(no_default=True)
            self.assertIsNotNone(osr.epoch)

    def test_manage_shard_ranges_deleted_child_and_parent_gap(self):
        # Test to produce a scenario where a parent container is stuck at
        # sharding because of a gap in shard ranges. And the gap is caused by
        # deleted child shard range which finishes sharding before its parent
        # does.
        # note: be careful not to add a container listing to this test which
        # would get shard ranges into memcache.
        obj_names = self._make_object_names(20)
        self.put_objects(obj_names)

        client.post_container(self.url, self.admin_token, self.container_name,
                              headers={'X-Container-Sharding': 'on'})
        # run replicators first time to get sync points set.
        self.container_replicators.once(
            additional_args='--partitions=%s' % self.brain.part)

        # shard root into two child-shards.
        root_0_db_file = self.get_db_file(self.brain.part, self.brain.nodes[0])
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            root_0_db_file,
            'find_and_replace', '10', '--enable'])
        # Run container-replicator to replicate them to other nodes.
        self.container_replicators.once(
            additional_args='--partitions=%s' % self.brain.part)
        self.assert_container_states('unsharded', 2)
        # Run container-sharder on all nodes to shard the container.
        self.sharders_once_non_auto(
            additional_args='--partitions=%s' % self.brain.part)
        # get shards to update state from parent...
        self.sharders_once_non_auto()
        self.assert_container_states('sharded', 2)

        # shard first child shard into 2 grand-child-shards.
        c_shard_ranges = self.get_container_shard_ranges()
        c_shard_brokers = [self.get_shard_broker(
            c_shard_ranges[0], node_index=i) for i in range(3)]
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            c_shard_brokers[0].db_file,
            'find_and_replace', '5', '--enable'])
        child_shard_part, c_shard_nodes = self.brain.ring.get_nodes(
            c_shard_ranges[0].account, c_shard_ranges[0].container)
        self.container_replicators.once(
            additional_args='--partitions=%s' % child_shard_part)
        for node in c_shard_nodes:
            self.assert_container_state(
                node, 'unsharded', 2, account=c_shard_ranges[0].account,
                container=c_shard_ranges[0].container, part=child_shard_part)

        # run sharder on only 2 of the child replicas by renaming the third
        # replica's DB file directory.
        # NOTE: if we only rename the retiring DB file, other replicas will
        # create a "fresh" DB with timestamp during replication, and then
        # after we restore the retiring DB back, there will be two DB files
        # in the same folder, and container state will appear to be "sharding"
        # instead of "unsharded".
        c_shard_dir = os.path.dirname(c_shard_brokers[2].db_file)
        c_shard_tmp_dir = c_shard_dir + ".tmp"
        os.rename(c_shard_dir, c_shard_tmp_dir)
        self.sharders_once_non_auto(additional_args='--partitions=%s' %
                                                    child_shard_part)
        for node in c_shard_nodes[:2]:
            self.assert_container_state(
                node, 'sharded', 2, account=c_shard_ranges[0].account,
                container=c_shard_ranges[0].container, part=child_shard_part)
        # get updates done...
        self.sharders_once_non_auto()

        # shard first grand-child shard into 2 grand-grand-child-shards.
        gc_shard_ranges = self.get_container_shard_ranges(
            account=c_shard_ranges[0].account,
            container=c_shard_ranges[0].container)
        shard_brokers = [self.get_shard_broker(
            gc_shard_ranges[0],
            node_index=i) for i in range(3)]
        self.assert_subprocess_success([
            'swift-manage-shard-ranges',
            shard_brokers[0].db_file,
            'find_and_replace', '3', '--enable'])
        grandchild_shard_part, gc_shard_nodes = self.brain.ring.get_nodes(
            gc_shard_ranges[0].account, gc_shard_ranges[0].container)
        self.container_replicators.once(
            additional_args='--partitions=%s' % grandchild_shard_part)
        self.sharders_once_non_auto(additional_args='--partitions=%s' %
                                                    grandchild_shard_part)

        # get shards to update state from parent...
        self.sharders_once_non_auto()
        self.sharders_once_non_auto()
        self.container_replicators.once(
            additional_args='--partitions=%s' % child_shard_part)

        # restore back the DB file directory of the disable child replica.
        shutil.rmtree(c_shard_dir, ignore_errors=True)
        os.rename(c_shard_tmp_dir, c_shard_dir)

        # the 2 child shards that sharded earlier still have their original
        # grand-child shards because they stopped updating form root once
        # sharded.
        for node in c_shard_nodes[:2]:
            self.assert_container_state(
                node, 'sharded', 2, account=c_shard_ranges[0].account,
                container=c_shard_ranges[0].container, part=child_shard_part)
        # the child shard that did not shard earlier has not been touched by
        # the sharder since, so still has two grand-child shards.
        self.assert_container_state(
            c_shard_nodes[2],
            'unsharded', 2, account=c_shard_ranges[0].account,
            container=c_shard_ranges[0].container, part=child_shard_part)

        # now, finally, run the sharder on the child that is still waiting to
        # shard. It will get 2 great-grandchild ranges from root to replace
        # deleted grandchild.
        self.sharders_once_non_auto(
            additional_args=['--partitions=%s' %
                             child_shard_part, '--devices=%s' %
                             c_shard_nodes[2]['device']])
        # batch size is 2 but this replicas has 3 shard ranges so we need two
        # runs of the sharder
        self.sharders_once_non_auto(
            additional_args=['--partitions=%s' %
                             child_shard_part, '--devices=%s' %
                             c_shard_nodes[2]['device']])
        self.assert_container_state(
            c_shard_nodes[2], 'sharded', 3, account=c_shard_ranges[0].account,
            container=c_shard_ranges[0].container, part=child_shard_part)
