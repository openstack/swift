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

import itertools
import time
import unittest

import mock

from swift.account import utils, backend
from swift.common.storage_policy import POLICIES
from swift.common.utils import Timestamp
from swift.common.header_key_dict import HeaderKeyDict

from test.unit import patch_policies


class TestFakeAccountBroker(unittest.TestCase):

    def test_fake_broker_get_info(self):
        broker = utils.FakeAccountBroker()
        now = time.time()
        with mock.patch('time.time', new=lambda: now):
            info = broker.get_info()
        timestamp = Timestamp(now)
        expected = {
            'container_count': 0,
            'object_count': 0,
            'bytes_used': 0,
            'created_at': timestamp.internal,
            'put_timestamp': timestamp.internal,
        }
        self.assertEqual(info, expected)

    def test_fake_broker_list_containers_iter(self):
        broker = utils.FakeAccountBroker()
        self.assertEqual(broker.list_containers_iter(), [])

    def test_fake_broker_metadata(self):
        broker = utils.FakeAccountBroker()
        self.assertEqual(broker.metadata, {})

    def test_fake_broker_get_policy_stats(self):
        broker = utils.FakeAccountBroker()
        self.assertEqual(broker.get_policy_stats(), {})


class TestAccountUtils(unittest.TestCase):

    def test_get_response_headers_fake_broker(self):
        broker = utils.FakeAccountBroker()
        now = time.time()
        expected = {
            'X-Account-Container-Count': 0,
            'X-Account-Object-Count': 0,
            'X-Account-Bytes-Used': 0,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': Timestamp(now).normal,
        }
        with mock.patch('time.time', new=lambda: now):
            resp_headers = utils.get_response_headers(broker)
        self.assertEqual(resp_headers, expected)

    def test_get_response_headers_empty_memory_broker(self):
        broker = backend.AccountBroker(':memory:', account='a')
        now = time.time()
        with mock.patch('time.time', new=lambda: now):
            broker.initialize(Timestamp(now).internal)
        expected = {
            'X-Account-Container-Count': 0,
            'X-Account-Object-Count': 0,
            'X-Account-Bytes-Used': 0,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': Timestamp(now).normal,
        }
        resp_headers = utils.get_response_headers(broker)
        self.assertEqual(resp_headers, expected)

    @patch_policies
    def test_get_response_headers_with_data(self):
        broker = backend.AccountBroker(':memory:', account='a')
        now = time.time()
        with mock.patch('time.time', new=lambda: now):
            broker.initialize(Timestamp(now).internal)
        # add some container data
        ts = (Timestamp(t).internal for t in itertools.count(int(now)))
        total_containers = 0
        total_objects = 0
        total_bytes = 0
        for policy in POLICIES:
            delete_timestamp = next(ts)
            put_timestamp = next(ts)
            object_count = int(policy)
            bytes_used = int(policy) * 10
            broker.put_container('c-%s' % policy.name, put_timestamp,
                                 delete_timestamp, object_count, bytes_used,
                                 int(policy))
            total_containers += 1
            total_objects += object_count
            total_bytes += bytes_used
        expected = HeaderKeyDict({
            'X-Account-Container-Count': total_containers,
            'X-Account-Object-Count': total_objects,
            'X-Account-Bytes-Used': total_bytes,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': Timestamp(now).normal,
        })
        for policy in POLICIES:
            prefix = 'X-Account-Storage-Policy-%s-' % policy.name
            expected[prefix + 'Container-Count'] = 1
            expected[prefix + 'Object-Count'] = int(policy)
            expected[prefix + 'Bytes-Used'] = int(policy) * 10
        resp_headers = utils.get_response_headers(broker)
        per_policy_container_headers = [
            h for h in resp_headers if
            h.lower().startswith('x-account-storage-policy-') and
            h.lower().endswith('-container-count')]
        self.assertTrue(per_policy_container_headers)
        for key, value in resp_headers.items():
            expected_value = expected.pop(key)
            self.assertEqual(expected_value, str(value),
                             'value for %r was %r not %r' % (
                                 key, value, expected_value))
        self.assertFalse(expected)

    @patch_policies
    def test_get_response_headers_with_legacy_data(self):
        broker = backend.AccountBroker(':memory:', account='a')
        now = time.time()
        with mock.patch('time.time', new=lambda: now):
            broker.initialize(Timestamp(now).internal)
        # add some container data
        ts = (Timestamp(t).internal for t in itertools.count(int(now)))
        total_containers = 0
        total_objects = 0
        total_bytes = 0
        for policy in POLICIES:
            delete_timestamp = next(ts)
            put_timestamp = next(ts)
            object_count = int(policy)
            bytes_used = int(policy) * 10
            broker.put_container('c-%s' % policy.name, put_timestamp,
                                 delete_timestamp, object_count, bytes_used,
                                 int(policy))
            total_containers += 1
            total_objects += object_count
            total_bytes += bytes_used
        expected = HeaderKeyDict({
            'X-Account-Container-Count': total_containers,
            'X-Account-Object-Count': total_objects,
            'X-Account-Bytes-Used': total_bytes,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': Timestamp(now).normal,
        })
        for policy in POLICIES:
            prefix = 'X-Account-Storage-Policy-%s-' % policy.name
            expected[prefix + 'Object-Count'] = int(policy)
            expected[prefix + 'Bytes-Used'] = int(policy) * 10
        orig_policy_stats = broker.get_policy_stats

        def stub_policy_stats(*args, **kwargs):
            policy_stats = orig_policy_stats(*args, **kwargs)
            for stats in policy_stats.values():
                # legacy db's won't return container_count
                del stats['container_count']
            return policy_stats
        broker.get_policy_stats = stub_policy_stats
        resp_headers = utils.get_response_headers(broker)
        per_policy_container_headers = [
            h for h in resp_headers if
            h.lower().startswith('x-account-storage-policy-') and
            h.lower().endswith('-container-count')]
        self.assertFalse(per_policy_container_headers)
        for key, value in resp_headers.items():
            expected_value = expected.pop(key)
            self.assertEqual(expected_value, str(value),
                             'value for %r was %r not %r' % (
                                 key, value, expected_value))
        self.assertFalse(expected)
