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
import json

from unittest import mock

from swift.account import utils, backend
from swift.common.storage_policy import POLICIES, StoragePolicy
from swift.common.swob import Request
from swift.common.utils import Timestamp
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.request_helpers import get_reserved_name

from test.unit import patch_policies, make_timestamp_iter
from test.unit.common.test_db import TestDbBase


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


class TestAccountUtils(TestDbBase):
    server_type = 'account'

    def setUp(self):
        super(TestAccountUtils, self).setUp()
        self.ts = make_timestamp_iter()

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
        broker = backend.AccountBroker(self.db_path, account='a')
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
        broker = backend.AccountBroker(self.db_path, account='a')
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
        broker = backend.AccountBroker(self.db_path, account='a')
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

    def test_account_listing_response(self):
        req = Request.blank('')
        now = time.time()
        with mock.patch('time.time', new=lambda: now):
            resp = utils.account_listing_response('a', req, 'text/plain')
        self.assertEqual(resp.status_int, 204)
        expected = HeaderKeyDict({
            'Content-Type': 'text/plain; charset=utf-8',
            'X-Account-Container-Count': 0,
            'X-Account-Object-Count': 0,
            'X-Account-Bytes-Used': 0,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': Timestamp(now).normal,
        })
        self.assertEqual(expected, resp.headers)
        self.assertEqual(b'', resp.body)

    @patch_policies([StoragePolicy(0, 'zero', is_default=True),
                     StoragePolicy(1, 'one', is_default=False)])
    def test_account_listing_with_containers(self):
        broker = backend.AccountBroker(self.db_path, account='a')
        put_timestamp = next(self.ts)
        now = time.time()
        with mock.patch('time.time', new=lambda: now):
            broker.initialize(put_timestamp.internal)
        container_timestamp = next(self.ts)
        broker.put_container('foo',
                             container_timestamp.internal, 0, 10, 100, 0)
        broker.put_container('bar',
                             container_timestamp.internal, 0, 10, 100, 1)
        # Can eat rows for policies not in POLICIES
        broker.put_container('baz',
                             container_timestamp.internal, 0, 10, 100, 2)

        req = Request.blank('')
        resp = utils.account_listing_response(
            'a', req, 'application/json', broker)
        self.assertEqual(resp.status_int, 200)
        expected = HeaderKeyDict({
            'Content-Type': 'application/json; charset=utf-8',
            'Content-Length': str(len(resp.body)),
            'X-Account-Container-Count': 3,
            'X-Account-Object-Count': 30,
            'X-Account-Bytes-Used': 300,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': put_timestamp.normal,
            'X-Account-Storage-Policy-Zero-Container-Count': 1,
            'X-Account-Storage-Policy-Zero-Object-Count': 10,
            'X-Account-Storage-Policy-Zero-Bytes-Used': 100,
            'X-Account-Storage-Policy-One-Container-Count': 1,
            'X-Account-Storage-Policy-One-Object-Count': 10,
            'X-Account-Storage-Policy-One-Bytes-Used': 100,
            # No POLICIES[2], so only account totals can include baz
        })
        self.assertEqual(expected, resp.headers)
        expected = [{
            "last_modified": container_timestamp.isoformat,
            "count": 10,
            "bytes": 100,
            "name": 'bar',
            'storage_policy': POLICIES[1].name,
        }, {
            "last_modified": container_timestamp.isoformat,
            "count": 10,
            "bytes": 100,
            "name": 'baz',
        }, {
            "last_modified": container_timestamp.isoformat,
            "count": 10,
            "bytes": 100,
            "name": 'foo',
            'storage_policy': POLICIES[0].name,
        }]
        self.assertEqual(expected, json.loads(resp.body))

        req = Request.blank('')
        resp = utils.account_listing_response(
            'a', req, 'application/json', broker, delimiter='a')
        self.assertEqual(resp.status_int, 200)
        expected = [{
            "subdir": "ba",
        }, {
            "last_modified": container_timestamp.isoformat,
            "count": 10,
            "bytes": 100,
            "name": 'foo',
            'storage_policy': POLICIES[0].name,
        }]
        self.assertEqual(expected, json.loads(resp.body))

    @patch_policies([StoragePolicy(0, 'zero', is_default=True),
                     StoragePolicy(1, 'one', is_default=False)])
    def test_account_listing_reserved_names(self):
        broker = backend.AccountBroker(self.db_path, account='a')
        put_timestamp = next(self.ts)
        now = time.time()
        with mock.patch('time.time', new=lambda: now):
            broker.initialize(put_timestamp.internal)
        container_timestamp = next(self.ts)
        broker.put_container(get_reserved_name('foo'),
                             container_timestamp.internal, 0, 10, 100, 0)
        broker.put_container(get_reserved_name('bar'),
                             container_timestamp.internal, 0, 10, 100, 1)

        req = Request.blank('')
        resp = utils.account_listing_response(
            'a', req, 'application/json', broker)
        self.assertEqual(resp.status_int, 200)
        expected = HeaderKeyDict({
            'Content-Type': 'application/json; charset=utf-8',
            'Content-Length': 2,
            'X-Account-Container-Count': 2,
            'X-Account-Object-Count': 20,
            'X-Account-Bytes-Used': 200,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': put_timestamp.normal,
            'X-Account-Storage-Policy-Zero-Container-Count': 1,
            'X-Account-Storage-Policy-Zero-Object-Count': 10,
            'X-Account-Storage-Policy-Zero-Bytes-Used': 100,
            'X-Account-Storage-Policy-One-Container-Count': 1,
            'X-Account-Storage-Policy-One-Object-Count': 10,
            'X-Account-Storage-Policy-One-Bytes-Used': 100,
        })
        self.assertEqual(expected, resp.headers)
        self.assertEqual(b'[]', resp.body)

        req = Request.blank('', headers={
            'X-Backend-Allow-Reserved-Names': 'true'})
        resp = utils.account_listing_response(
            'a', req, 'application/json', broker)
        self.assertEqual(resp.status_int, 200)
        expected = HeaderKeyDict({
            'Content-Type': 'application/json; charset=utf-8',
            'Content-Length': 245,
            'X-Account-Container-Count': 2,
            'X-Account-Object-Count': 20,
            'X-Account-Bytes-Used': 200,
            'X-Timestamp': Timestamp(now).normal,
            'X-PUT-Timestamp': put_timestamp.normal,
            'X-Account-Storage-Policy-Zero-Container-Count': 1,
            'X-Account-Storage-Policy-Zero-Object-Count': 10,
            'X-Account-Storage-Policy-Zero-Bytes-Used': 100,
            'X-Account-Storage-Policy-One-Container-Count': 1,
            'X-Account-Storage-Policy-One-Object-Count': 10,
            'X-Account-Storage-Policy-One-Bytes-Used': 100,
        })
        self.assertEqual(expected, resp.headers)
        expected = [{
            "last_modified": container_timestamp.isoformat,
            "count": 10,
            "bytes": 100,
            "name": get_reserved_name('bar'),
            'storage_policy': POLICIES[1].name,
        }, {
            "last_modified": container_timestamp.isoformat,
            "count": 10,
            "bytes": 100,
            "name": get_reserved_name('foo'),
            'storage_policy': POLICIES[0].name,
        }]
        self.assertEqual(expected, json.loads(resp.body))
