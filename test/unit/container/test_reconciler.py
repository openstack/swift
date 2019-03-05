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
import numbers

import mock
import operator
import time
import unittest
import socket
import os
import errno
import itertools
import random

from collections import defaultdict
from datetime import datetime
import six
from six.moves import urllib
from swift.container import reconciler
from swift.container.server import gen_resp_headers
from swift.common.direct_client import ClientException
from swift.common import swob
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import split_path, Timestamp, encode_timestamps

from test.unit import debug_logger, FakeRing, fake_http_connect
from test.unit.common.middleware.helpers import FakeSwift


def timestamp_to_last_modified(timestamp):
    return datetime.utcfromtimestamp(
        float(Timestamp(timestamp))).strftime('%Y-%m-%dT%H:%M:%S.%f')


def container_resp_headers(**kwargs):
    return HeaderKeyDict(gen_resp_headers(kwargs))


class FakeStoragePolicySwift(object):

    def __init__(self):
        self.storage_policy = defaultdict(FakeSwift)
        self._mock_oldest_spi_map = {}

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return getattr(self.storage_policy[None], name)

    def __call__(self, env, start_response):
        method = env['REQUEST_METHOD']
        path = env['PATH_INFO']
        _, acc, cont, obj = split_path(env['PATH_INFO'], 0, 4,
                                       rest_with_last=True)
        if not obj:
            policy_index = None
        else:
            policy_index = self._mock_oldest_spi_map.get(cont, 0)
            # allow backend policy override
            if 'HTTP_X_BACKEND_STORAGE_POLICY_INDEX' in env:
                policy_index = int(env['HTTP_X_BACKEND_STORAGE_POLICY_INDEX'])

        try:
            return self.storage_policy[policy_index].__call__(
                env, start_response)
        except KeyError:
            pass

        if method == 'PUT':
            resp_class = swob.HTTPCreated
        else:
            resp_class = swob.HTTPNotFound
        self.storage_policy[policy_index].register(
            method, path, resp_class, {}, '')

        return self.storage_policy[policy_index].__call__(
            env, start_response)


class FakeInternalClient(reconciler.InternalClient):
    def __init__(self, listings):
        self.app = FakeStoragePolicySwift()
        self.user_agent = 'fake-internal-client'
        self.request_tries = 1
        self.parse(listings)

    def parse(self, listings):
        self.accounts = defaultdict(lambda: defaultdict(list))
        for item, timestamp in listings.items():
            # XXX this interface is stupid
            if isinstance(timestamp, tuple):
                timestamp, content_type = timestamp
            else:
                timestamp, content_type = timestamp, 'application/x-put'
            storage_policy_index, path = item
            if six.PY2 and isinstance(path, six.text_type):
                path = path.encode('utf-8')
            account, container_name, obj_name = split_path(
                path, 0, 3, rest_with_last=True)
            self.accounts[account][container_name].append(
                (obj_name, storage_policy_index, timestamp, content_type))
        for account_name, containers in self.accounts.items():
            for con in containers:
                self.accounts[account_name][con].sort(key=lambda t: t[0])
        for account, containers in self.accounts.items():
            account_listing_data = []
            account_path = '/v1/%s' % account
            for container, objects in containers.items():
                container_path = account_path + '/' + container
                container_listing_data = []
                for entry in objects:
                    (obj_name, storage_policy_index,
                     timestamp, content_type) = entry
                    if storage_policy_index is None and not obj_name:
                        # empty container
                        continue
                    obj_path = swob.str_to_wsgi(
                        container_path + '/' + obj_name)
                    ts = Timestamp(timestamp)
                    headers = {'X-Timestamp': ts.normal,
                               'X-Backend-Timestamp': ts.internal}
                    # register object response
                    self.app.storage_policy[storage_policy_index].register(
                        'GET', obj_path, swob.HTTPOk, headers)
                    self.app.storage_policy[storage_policy_index].register(
                        'DELETE', obj_path, swob.HTTPNoContent, {})
                    # container listing entry
                    last_modified = timestamp_to_last_modified(timestamp)
                    # some tests setup mock listings using floats, some use
                    # strings, so normalize here
                    if isinstance(timestamp, numbers.Number):
                        timestamp = '%f' % timestamp
                    if six.PY2:
                        obj_name = obj_name.decode('utf-8')
                        timestamp = timestamp.decode('utf-8')
                    obj_data = {
                        'bytes': 0,
                        # listing data is unicode
                        'name': obj_name,
                        'last_modified': last_modified,
                        'hash': timestamp,
                        'content_type': content_type,
                    }
                    container_listing_data.append(obj_data)
                container_listing_data.sort(key=operator.itemgetter('name'))
                # register container listing response
                container_headers = {}
                container_qry_string = \
                    '?format=json&marker=&end_marker=&prefix='
                self.app.register('GET', container_path + container_qry_string,
                                  swob.HTTPOk, container_headers,
                                  json.dumps(container_listing_data))
                if container_listing_data:
                    obj_name = container_listing_data[-1]['name']
                    # client should quote and encode marker
                    end_qry_string = \
                        '?format=json&marker=%s&end_marker=&prefix=' % (
                            urllib.parse.quote(obj_name.encode('utf-8')))
                    self.app.register('GET', container_path + end_qry_string,
                                      swob.HTTPOk, container_headers,
                                      json.dumps([]))
                self.app.register('DELETE', container_path,
                                  swob.HTTPConflict, {}, '')
                # simple account listing entry
                container_data = {'name': container}
                account_listing_data.append(container_data)
            # register account response
            account_listing_data.sort(key=operator.itemgetter('name'))
            account_headers = {}
            account_qry_string = '?format=json&marker=&end_marker=&prefix='
            self.app.register('GET', account_path + account_qry_string,
                              swob.HTTPOk, account_headers,
                              json.dumps(account_listing_data))
            end_qry_string = '?format=json&marker=%s&end_marker=&prefix=' % (
                urllib.parse.quote(account_listing_data[-1]['name']))
            self.app.register('GET', account_path + end_qry_string,
                              swob.HTTPOk, account_headers,
                              json.dumps([]))


class TestReconcilerUtils(unittest.TestCase):

    def setUp(self):
        self.fake_ring = FakeRing()
        reconciler.direct_get_container_policy_index.reset()

    def test_parse_raw_obj(self):
        got = reconciler.parse_raw_obj({
            'name': "2:/AUTH_bob/con/obj",
            'hash': Timestamp(2017551.49350).internal,
            'last_modified': timestamp_to_last_modified(2017551.49352),
            'content_type': 'application/x-delete',
        })
        self.assertEqual(got['q_policy_index'], 2)
        self.assertEqual(got['account'], 'AUTH_bob')
        self.assertEqual(got['container'], 'con')
        self.assertEqual(got['obj'], 'obj')
        self.assertEqual(got['q_ts'], 2017551.49350)
        self.assertEqual(got['q_record'], 2017551.49352)
        self.assertEqual(got['q_op'], 'DELETE')

        got = reconciler.parse_raw_obj({
            'name': "1:/AUTH_bob/con/obj",
            'hash': Timestamp(1234.20190).internal,
            'last_modified': timestamp_to_last_modified(1234.20192),
            'content_type': 'application/x-put',
        })
        self.assertEqual(got['q_policy_index'], 1)
        self.assertEqual(got['account'], 'AUTH_bob')
        self.assertEqual(got['container'], 'con')
        self.assertEqual(got['obj'], 'obj')
        self.assertEqual(got['q_ts'], 1234.20190)
        self.assertEqual(got['q_record'], 1234.20192)
        self.assertEqual(got['q_op'], 'PUT')

        # the 'hash' field in object listing has the raw 'created_at' value
        # which could be a composite of timestamps
        timestamp_str = encode_timestamps(Timestamp(1234.20190),
                                          Timestamp(1245.20190),
                                          Timestamp(1256.20190),
                                          explicit=True)
        got = reconciler.parse_raw_obj({
            'name': "1:/AUTH_bob/con/obj",
            'hash': timestamp_str,
            'last_modified': timestamp_to_last_modified(1234.20192),
            'content_type': 'application/x-put',
        })
        self.assertEqual(got['q_policy_index'], 1)
        self.assertEqual(got['account'], 'AUTH_bob')
        self.assertEqual(got['container'], 'con')
        self.assertEqual(got['obj'], 'obj')
        self.assertEqual(got['q_ts'], 1234.20190)
        self.assertEqual(got['q_record'], 1234.20192)
        self.assertEqual(got['q_op'], 'PUT')

        # negative test
        obj_info = {
            'name': "1:/AUTH_bob/con/obj",
            'hash': Timestamp(1234.20190).internal,
            'last_modified': timestamp_to_last_modified(1234.20192),
        }
        self.assertRaises(ValueError, reconciler.parse_raw_obj, obj_info)
        obj_info['content_type'] = 'foo'
        self.assertRaises(ValueError, reconciler.parse_raw_obj, obj_info)
        obj_info['content_type'] = 'appliation/x-post'
        self.assertRaises(ValueError, reconciler.parse_raw_obj, obj_info)
        self.assertRaises(ValueError, reconciler.parse_raw_obj,
                          {'name': 'bogus'})
        self.assertRaises(ValueError, reconciler.parse_raw_obj,
                          {'name': '-1:/AUTH_test/container'})
        self.assertRaises(ValueError, reconciler.parse_raw_obj,
                          {'name': 'asdf:/AUTH_test/c/obj'})
        self.assertRaises(KeyError, reconciler.parse_raw_obj,
                          {'name': '0:/AUTH_test/c/obj',
                           'content_type': 'application/x-put'})

    def test_get_container_policy_index(self):
        ts = itertools.count(int(time.time()))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=0,
            ),
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=1,
            ),
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=0,
            ),
        ]
        for permutation in itertools.permutations((0, 1, 2)):
            reconciler.direct_get_container_policy_index.reset()
            resp_headers = [stub_resp_headers[i] for i in permutation]
            with mock.patch(mock_path) as direct_head:
                direct_head.side_effect = resp_headers
                oldest_spi = reconciler.direct_get_container_policy_index(
                    self.fake_ring, 'a', 'con')
            test_values = [(info['x-storage-policy-index'],
                            info['x-backend-status-changed-at']) for
                           info in resp_headers]
            self.assertEqual(oldest_spi, 0,
                             "oldest policy index wrong "
                             "for permutation %r" % test_values)

    def test_get_container_policy_index_with_error(self):
        ts = itertools.count(int(time.time()))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            container_resp_headers(
                status_change_at=next(ts),
                storage_policy_index=2,
            ),
            container_resp_headers(
                status_changed_at=next(ts),
                storage_policy_index=1,
            ),
            # old timestamp, but 500 should be ignored...
            ClientException(
                'Container Server blew up',
                http_status=500, http_reason='Server Error',
                http_headers=container_resp_headers(
                    status_changed_at=Timestamp(0).internal,
                    storage_policy_index=0,
                ),
            ),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 2)

    def test_get_container_policy_index_with_socket_error(self):
        ts = itertools.count(int(time.time()))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=1,
            ),
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=0,
            ),
            socket.error(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED)),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 1)

    def test_get_container_policy_index_with_too_many_errors(self):
        ts = itertools.count(int(time.time()))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=0,
            ),
            socket.error(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED)),
            ClientException(
                'Container Server blew up',
                http_status=500, http_reason='Server Error',
                http_headers=container_resp_headers(
                    status_changed_at=Timestamp(next(ts)).internal,
                    storage_policy_index=1,
                ),
            ),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertIsNone(oldest_spi)

    def test_get_container_policy_index_for_deleted(self):
        mock_path = 'swift.container.reconciler.direct_head_container'
        headers = container_resp_headers(
            status_changed_at=Timestamp.now().internal,
            storage_policy_index=1,
        )
        stub_resp_headers = [
            ClientException(
                'Container Not Found',
                http_status=404, http_reason='Not Found',
                http_headers=headers,
            ),
            ClientException(
                'Container Not Found',
                http_status=404, http_reason='Not Found',
                http_headers=headers,
            ),
            ClientException(
                'Container Not Found',
                http_status=404, http_reason='Not Found',
                http_headers=headers,
            ),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 1)

    def test_get_container_policy_index_for_recently_deleted(self):
        ts = itertools.count(int(time.time()))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            ClientException(
                'Container Not Found',
                http_status=404, http_reason='Not Found',
                http_headers=container_resp_headers(
                    put_timestamp=next(ts),
                    delete_timestamp=next(ts),
                    status_changed_at=next(ts),
                    storage_policy_index=0,
                ),
            ),
            ClientException(
                'Container Not Found',
                http_status=404, http_reason='Not Found',
                http_headers=container_resp_headers(
                    put_timestamp=next(ts),
                    delete_timestamp=next(ts),
                    status_changed_at=next(ts),
                    storage_policy_index=1,
                ),
            ),
            ClientException(
                'Container Not Found',
                http_status=404, http_reason='Not Found',
                http_headers=container_resp_headers(
                    put_timestamp=next(ts),
                    delete_timestamp=next(ts),
                    status_changed_at=next(ts),
                    storage_policy_index=2,
                ),
            ),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 2)

    def test_get_container_policy_index_for_recently_recreated(self):
        ts = itertools.count(int(time.time()))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            # old put, no recreate
            container_resp_headers(
                delete_timestamp=0,
                put_timestamp=next(ts),
                status_changed_at=next(ts),
                storage_policy_index=0,
            ),
            # recently deleted
            ClientException(
                'Container Not Found',
                http_status=404, http_reason='Not Found',
                http_headers=container_resp_headers(
                    put_timestamp=next(ts),
                    delete_timestamp=next(ts),
                    status_changed_at=next(ts),
                    storage_policy_index=1,
                ),
            ),
            # recently recreated
            container_resp_headers(
                delete_timestamp=next(ts),
                put_timestamp=next(ts),
                status_changed_at=next(ts),
                storage_policy_index=2,
            ),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 2)

    def test_get_container_policy_index_for_recently_split_brain(self):
        ts = itertools.count(int(time.time()))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            # oldest put
            container_resp_headers(
                delete_timestamp=0,
                put_timestamp=next(ts),
                status_changed_at=next(ts),
                storage_policy_index=0,
            ),
            # old recreate
            container_resp_headers(
                delete_timestamp=next(ts),
                put_timestamp=next(ts),
                status_changed_at=next(ts),
                storage_policy_index=1,
            ),
            # recently put
            container_resp_headers(
                delete_timestamp=0,
                put_timestamp=next(ts),
                status_changed_at=next(ts),
                storage_policy_index=2,
            ),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 1)

    def test_get_container_policy_index_cache(self):
        now = time.time()
        ts = itertools.count(int(now))
        mock_path = 'swift.container.reconciler.direct_head_container'
        stub_resp_headers = [
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=0,
            ),
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=1,
            ),
            container_resp_headers(
                status_changed_at=Timestamp(next(ts)).internal,
                storage_policy_index=0,
            ),
        ]
        random.shuffle(stub_resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = stub_resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        self.assertEqual(oldest_spi, 0)
        # re-mock with errors
        stub_resp_headers = [
            socket.error(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED)),
            socket.error(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED)),
            socket.error(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED)),
        ]
        with mock.patch('time.time', new=lambda: now):
            with mock.patch(mock_path) as direct_head:
                direct_head.side_effect = stub_resp_headers
                oldest_spi = reconciler.direct_get_container_policy_index(
                    self.fake_ring, 'a', 'con')
        # still cached
        self.assertEqual(oldest_spi, 0)
        # propel time forward
        the_future = now + 31
        with mock.patch('time.time', new=lambda: the_future):
            with mock.patch(mock_path) as direct_head:
                direct_head.side_effect = stub_resp_headers
                oldest_spi = reconciler.direct_get_container_policy_index(
                    self.fake_ring, 'a', 'con')
        # expired
        self.assertIsNone(oldest_spi)

    def test_direct_delete_container_entry(self):
        mock_path = 'swift.common.direct_client.http_connect'
        connect_args = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            connect_args.append({
                'ipaddr': ipaddr, 'port': port, 'device': device,
                'partition': partition, 'method': method, 'path': path,
                'headers': headers, 'query_string': query_string})

        x_timestamp = Timestamp.now()
        headers = {'x-timestamp': x_timestamp.internal}
        fake_hc = fake_http_connect(200, 200, 200, give_connect=test_connect)
        with mock.patch(mock_path, fake_hc):
            reconciler.direct_delete_container_entry(
                self.fake_ring, 'a', 'c', 'o', headers=headers)

        self.assertEqual(len(connect_args), 3)
        for args in connect_args:
            self.assertEqual(args['method'], 'DELETE')
            self.assertEqual(args['path'], '/a/c/o')
            self.assertEqual(args['headers'].get('x-timestamp'),
                             headers['x-timestamp'])

    def test_direct_delete_container_entry_with_errors(self):
        # setup mock direct_delete
        mock_path = \
            'swift.container.reconciler.direct_delete_container_object'
        stub_resp = [
            None,
            socket.error(errno.ECONNREFUSED, os.strerror(errno.ECONNREFUSED)),
            ClientException(
                'Container Server blew up',
                '10.0.0.12', 6201, 'sdj', 404, 'Not Found'
            ),
        ]
        mock_direct_delete = mock.MagicMock()
        mock_direct_delete.side_effect = stub_resp

        with mock.patch(mock_path, mock_direct_delete), \
                mock.patch('eventlet.greenpool.DEBUG', False):
            rv = reconciler.direct_delete_container_entry(
                self.fake_ring, 'a', 'c', 'o')
        self.assertIsNone(rv)
        self.assertEqual(len(mock_direct_delete.mock_calls), 3)

    def test_add_to_reconciler_queue(self):
        mock_path = 'swift.common.direct_client.http_connect'
        connect_args = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            connect_args.append({
                'ipaddr': ipaddr, 'port': port, 'device': device,
                'partition': partition, 'method': method, 'path': path,
                'headers': headers, 'query_string': query_string})

        fake_hc = fake_http_connect(200, 200, 200, give_connect=test_connect)
        with mock.patch(mock_path, fake_hc):
            ret = reconciler.add_to_reconciler_queue(
                self.fake_ring, 'a', 'c', 'o', 17, 5948918.63946, 'DELETE')

        self.assertTrue(ret)
        self.assertEqual(ret, str(int(5948918.63946 // 3600 * 3600)))
        self.assertEqual(len(connect_args), 3)

        required_headers = ('x-content-type', 'x-etag')

        for args in connect_args:
            self.assertEqual(args['headers']['X-Timestamp'], '5948918.63946')
            self.assertEqual(args['path'],
                             '/.misplaced_objects/5947200/17:/a/c/o')
            self.assertEqual(args['headers']['X-Content-Type'],
                             'application/x-delete')
            for header in required_headers:
                self.assertTrue(header in args['headers'],
                                '%r was missing request headers %r' % (
                                    header, args['headers']))

    def test_add_to_reconciler_queue_force(self):
        mock_path = 'swift.common.direct_client.http_connect'
        connect_args = []

        def test_connect(ipaddr, port, device, partition, method, path,
                         headers=None, query_string=None):
            connect_args.append({
                'ipaddr': ipaddr, 'port': port, 'device': device,
                'partition': partition, 'method': method, 'path': path,
                'headers': headers, 'query_string': query_string})

        fake_hc = fake_http_connect(200, 200, 200, give_connect=test_connect)
        now = time.time()
        with mock.patch(mock_path, fake_hc), \
                mock.patch('swift.container.reconciler.time.time',
                           lambda: now):
            ret = reconciler.add_to_reconciler_queue(
                self.fake_ring, 'a', 'c', 'o', 17, 5948918.63946, 'PUT',
                force=True)

        self.assertTrue(ret)
        self.assertEqual(ret, str(int(5948918.63946 // 3600 * 3600)))
        self.assertEqual(len(connect_args), 3)

        required_headers = ('x-size', 'x-content-type')

        for args in connect_args:
            self.assertEqual(args['headers']['X-Timestamp'],
                             Timestamp(now).internal)
            self.assertEqual(args['headers']['X-Etag'], '5948918.63946')
            self.assertEqual(args['path'],
                             '/.misplaced_objects/5947200/17:/a/c/o')
            for header in required_headers:
                self.assertTrue(header in args['headers'],
                                '%r was missing request headers %r' % (
                                    header, args['headers']))

    def test_add_to_reconciler_queue_fails(self):
        mock_path = 'swift.common.direct_client.http_connect'

        fake_connects = [fake_http_connect(200),
                         fake_http_connect(200, raise_timeout_exc=True),
                         fake_http_connect(507)]

        def fake_hc(*a, **kw):
            return fake_connects.pop()(*a, **kw)

        with mock.patch(mock_path, fake_hc):
            ret = reconciler.add_to_reconciler_queue(
                self.fake_ring, 'a', 'c', 'o', 17, 5948918.63946, 'PUT')
        self.assertFalse(ret)

    def test_add_to_reconciler_queue_socket_error(self):
        mock_path = 'swift.common.direct_client.http_connect'

        exc = socket.error(errno.ECONNREFUSED,
                           os.strerror(errno.ECONNREFUSED))
        fake_connects = [fake_http_connect(200),
                         fake_http_connect(200, raise_timeout_exc=True),
                         fake_http_connect(500, raise_exc=exc)]

        def fake_hc(*a, **kw):
            return fake_connects.pop()(*a, **kw)

        with mock.patch(mock_path, fake_hc):
            ret = reconciler.add_to_reconciler_queue(
                self.fake_ring, 'a', 'c', 'o', 17, 5948918.63946, 'DELETE')
        self.assertFalse(ret)


def listing_qs(marker):
    return "?format=json&marker=%s&end_marker=&prefix=" % \
        urllib.parse.quote(marker.encode('utf-8'))


class TestReconciler(unittest.TestCase):

    maxDiff = None

    def setUp(self):
        self.logger = debug_logger()
        conf = {}
        with mock.patch('swift.container.reconciler.InternalClient'):
            self.reconciler = reconciler.ContainerReconciler(conf)
        self.reconciler.logger = self.logger
        self.start_interval = int(time.time() // 3600 * 3600)
        self.current_container_path = '/v1/.misplaced_objects/%d' % (
            self.start_interval) + listing_qs('')

    def _mock_listing(self, objects):
        self.reconciler.swift = FakeInternalClient(objects)
        self.fake_swift = self.reconciler.swift.app

    def _mock_oldest_spi(self, container_oldest_spi_map):
        self.fake_swift._mock_oldest_spi_map = container_oldest_spi_map

    def _run_once(self):
        """
        Helper method to run the reconciler once with appropriate direct-client
        mocks in place.

        Returns the list of direct-deleted container entries in the format
        [(acc1, con1, obj1), ...]
        """

        def mock_oldest_spi(ring, account, container_name):
            return self.fake_swift._mock_oldest_spi_map.get(container_name, 0)

        items = {
            'direct_get_container_policy_index': mock_oldest_spi,
            'direct_delete_container_entry': mock.DEFAULT,
        }

        mock_time_iter = itertools.count(self.start_interval)
        with mock.patch.multiple(reconciler, **items) as mocks:
            self.mock_delete_container_entry = \
                mocks['direct_delete_container_entry']
            with mock.patch('time.time', lambda: next(mock_time_iter)):
                self.reconciler.run_once()

        return [c[1][1:4] for c in
                mocks['direct_delete_container_entry'].mock_calls]

    def test_invalid_queue_name(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/bogus"): 3618.84187,
        })
        deleted_container_entries = self._run_once()
        # we try to find something useful
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('bogus'))])
        # but only get the bogus record
        self.assertEqual(self.reconciler.stats['invalid_record'], 1)
        # and just leave it on the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 0)
        self.assertFalse(deleted_container_entries)

    def test_invalid_queue_name_marches_onward(self):
        # there's something useful there on the queue
        self._mock_listing({
            (None, "/.misplaced_objects/3600/00000bogus"): 3600.0000,
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3618.84187,
            (1, "/AUTH_bob/c/o1"): 3618.84187,
        })
        self._mock_oldest_spi({'c': 1})  # already in the right spot!
        deleted_container_entries = self._run_once()
        # we get all the queue entries we can
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # and one is garbage
        self.assertEqual(self.reconciler.stats['invalid_record'], 1)
        # but the other is workable
        self.assertEqual(self.reconciler.stats['noop_object'], 1)
        # so pop the queue for that one
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_queue_name_with_policy_index_delimiter_in_name(self):
        q_path = '.misplaced_objects/3600'
        obj_path = "AUTH_bob/c:sneaky/o1:sneaky"
        # there's something useful there on the queue
        self._mock_listing({
            (None, "/%s/1:/%s" % (q_path, obj_path)): 3618.84187,
            (1, '/%s' % obj_path): 3618.84187,
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()
        # we find the misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/%s' % obj_path))])
        # move it
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(self.reconciler.stats['copy_success'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/%s' % obj_path),
             ('DELETE', '/v1/%s' % obj_path)])
        delete_headers = self.fake_swift.storage_policy[1].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/%s' % obj_path),
             ('PUT', '/v1/%s' % obj_path)])
        # clean up the source
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # we DELETE the object from the wrong place with source_ts + offset 1
        # timestamp to make sure the change takes effect
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=1).internal)
        # and pop the queue for that one
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries, [(
            '.misplaced_objects', '3600', '1:/%s' % obj_path)])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_unable_to_direct_get_oldest_storage_policy(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3618.84187,
        })
        # the reconciler gets "None" if we can't quorum the container
        self._mock_oldest_spi({'c': None})
        deleted_container_entries = self._run_once()
        # we look for misplaced objects
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # but can't really say where to go looking
        self.assertEqual(self.reconciler.stats['unavailable_container'], 1)
        # we don't clean up anything
        self.assertEqual(self.reconciler.stats['cleanup_object'], 0)
        # and we definitely should not pop_queue
        self.assertFalse(deleted_container_entries)
        self.assertEqual(self.reconciler.stats['retry'], 1)

    def test_object_move(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3618.84187,
            (1, "/AUTH_bob/c/o1"): 3618.84187,
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # moves it
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(self.reconciler.stats['copy_success'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1'),
             ('DELETE', '/v1/AUTH_bob/c/o1')])
        delete_headers = self.fake_swift.storage_policy[1].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),
             ('PUT', '/v1/AUTH_bob/c/o1')])
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        # we PUT the object in the right place with q_ts + offset 2
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=2))
        # cleans up the old
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # we DELETE the object from the wrong place with source_ts + offset 1
        # timestamp to make sure the change takes effect
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=1))
        # and when we're done, we pop the entry from the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_the_other_direction(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/0:/AUTH_bob/c/o1"): 3618.84187,
            (0, "/AUTH_bob/c/o1"): 3618.84187,
        })
        self._mock_oldest_spi({'c': 1})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('0:/AUTH_bob/c/o1'))])
        # moves it
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(self.reconciler.stats['copy_success'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('GET', '/v1/AUTH_bob/c/o1'),  # 2
             ('DELETE', '/v1/AUTH_bob/c/o1')])  # 4
        delete_headers = self.fake_swift.storage_policy[0].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),  # 1
             ('PUT', '/v1/AUTH_bob/c/o1')])  # 3
        put_headers = self.fake_swift.storage_policy[1].headers[1]
        # we PUT the object in the right place with q_ts + offset 2
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=2).internal)
        # cleans up the old
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # we DELETE the object from the wrong place with source_ts + offset 1
        # timestamp to make sure the change takes effect
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=1).internal)
        # and when we're done, we pop the entry from the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '0:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_with_unicode_and_spaces(self):
        # the "name" in listings and the unicode string passed to all
        # functions where we call them with (account, container, obj)
        obj_name = u"AUTH_bob/c \u062a/o1 \u062a"
        # anytime we talk about a call made to swift for a path
        if six.PY2:
            obj_path = obj_name.encode('utf-8')
        else:
            obj_path = obj_name.encode('utf-8').decode('latin-1')
        # this mock expects unquoted unicode because it handles container
        # listings as well as paths
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/%s" % obj_name): 3618.84187,
            (1, "/%s" % obj_name): 3618.84187,
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        # listing_qs encodes and quotes - so give it name
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/%s' % obj_name))])
        # moves it
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(self.reconciler.stats['copy_success'], 1)
        # these calls are to the real path
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/%s' % obj_path),  # 2
             ('DELETE', '/v1/%s' % obj_path)])  # 4
        delete_headers = self.fake_swift.storage_policy[1].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/%s' % obj_path),  # 1
             ('PUT', '/v1/%s' % obj_path)])  # 3
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        # we PUT the object in the right place with q_ts + offset 2
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=2).internal)
        # cleans up the old
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # we DELETE the object from the wrong place with source_ts + offset 1
        # timestamp to make sure the change takes effect
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=1).internal)
        self.assertEqual(
            delete_headers.get('X-Backend-Storage-Policy-Index'), '1')
        # and when we're done, we pop the entry from the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        # this mock received the name, it's encoded down in buffered_http
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/%s' % obj_name)])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_delete(self):
        q_ts = time.time()
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): (
                Timestamp(q_ts).internal, 'application/x-delete'),
            # object exists in "correct" storage policy - slightly older
            (0, "/AUTH_bob/c/o1"): Timestamp(q_ts - 1).internal,
        })
        self._mock_oldest_spi({'c': 0})
        # the tombstone exists in the enqueued storage policy
        self.fake_swift.storage_policy[1].register(
            'GET', '/v1/AUTH_bob/c/o1', swob.HTTPNotFound,
            {'X-Backend-Timestamp': Timestamp(q_ts).internal})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # delete it
        self.assertEqual(self.reconciler.stats['delete_attempt'], 1)
        self.assertEqual(self.reconciler.stats['delete_success'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1'),
             ('DELETE', '/v1/AUTH_bob/c/o1')])
        delete_headers = self.fake_swift.storage_policy[1].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),
             ('DELETE', '/v1/AUTH_bob/c/o1')])
        reconcile_headers = self.fake_swift.storage_policy[0].headers[1]
        # we DELETE the object in the right place with q_ts + offset 2
        self.assertEqual(reconcile_headers.get('X-Timestamp'),
                         Timestamp(q_ts, offset=2).internal)
        # cleans up the old
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # we DELETE the object from the wrong place with source_ts + offset 1
        # timestamp to make sure the change takes effect
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(q_ts, offset=1))
        # and when we're done, we pop the entry from the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_enqueued_for_the_correct_dest_noop(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3618.84187,
            (1, "/AUTH_bob/c/o1"): 3618.84187,
        })
        self._mock_oldest_spi({'c': 1})  # already in the right spot!
        deleted_container_entries = self._run_once()

        # nothing to see here
        self.assertEqual(self.reconciler.stats['noop_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # so we just pop the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_src_object_newer_than_queue_entry(self):
        # setup the cluster
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3600.123456,
            (1, '/AUTH_bob/c/o1'): 3600.234567,  # slightly newer
        })
        self._mock_oldest_spi({'c': 0})  # destination
        # turn the crank
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # proceed with the move
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(self.reconciler.stats['copy_success'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1'),  # 2
             ('DELETE', '/v1/AUTH_bob/c/o1')])  # 4
        delete_headers = self.fake_swift.storage_policy[1].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),  # 1
             ('PUT', '/v1/AUTH_bob/c/o1')])  # 3
        # .. with source timestamp + offset 2
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3600.234567, offset=2))
        # src object is cleaned up
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # ... with q_ts + offset 1
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3600.123456, offset=1))
        # and queue is popped
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_src_object_older_than_queue_entry(self):
        # should be some sort of retry case
        q_ts = time.time()
        container = str(int(q_ts // 3600 * 3600))
        q_path = '.misplaced_objects/%s' % container
        self._mock_listing({
            (None, "/%s/1:/AUTH_bob/c/o1" % q_path): q_ts,
            (1, '/AUTH_bob/c/o1'): q_ts - 0.00001,  # slightly older
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/%s' % q_path + listing_qs('')),
             ('GET', '/v1/%s' % q_path +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container))])
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1')])
        # but no object copy is attempted
        self.assertEqual(self.reconciler.stats['unavailable_source'], 1)
        self.assertEqual(self.reconciler.stats['copy_attempt'], 0)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1')])
        # src object is un-modified
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 0)
        # queue is un-changed, we'll have to retry
        self.assertEqual(self.reconciler.stats['pop_queue'], 0)
        self.assertEqual(deleted_container_entries, [])
        self.assertEqual(self.reconciler.stats['retry'], 1)

    def test_src_object_unavailable_with_slightly_newer_tombstone(self):
        # should be some sort of retry case
        q_ts = float(Timestamp.now())
        container = str(int(q_ts // 3600 * 3600))
        q_path = '.misplaced_objects/%s' % container
        self._mock_listing({
            (None, "/%s/1:/AUTH_bob/c/o1" % q_path): q_ts,
        })
        self._mock_oldest_spi({'c': 0})
        self.fake_swift.storage_policy[1].register(
            'GET', '/v1/AUTH_bob/c/o1', swob.HTTPNotFound,
            {'X-Backend-Timestamp': Timestamp(q_ts, offset=2).internal})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/%s' % q_path + listing_qs('')),
             ('GET', '/v1/%s' % q_path +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container))])
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1')])
        # but no object copy is attempted
        self.assertEqual(self.reconciler.stats['unavailable_source'], 1)
        self.assertEqual(self.reconciler.stats['copy_attempt'], 0)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1')])
        # src object is un-modified
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 0)
        # queue is un-changed, we'll have to retry
        self.assertEqual(self.reconciler.stats['pop_queue'], 0)
        self.assertEqual(deleted_container_entries, [])
        self.assertEqual(self.reconciler.stats['retry'], 1)

    def test_src_object_unavailable_server_error(self):
        # should be some sort of retry case
        q_ts = float(Timestamp.now())
        container = str(int(q_ts // 3600 * 3600))
        q_path = '.misplaced_objects/%s' % container
        self._mock_listing({
            (None, "/%s/1:/AUTH_bob/c/o1" % q_path): q_ts,
        })
        self._mock_oldest_spi({'c': 0})
        self.fake_swift.storage_policy[1].register(
            'GET', '/v1/AUTH_bob/c/o1', swob.HTTPServiceUnavailable, {})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/%s' % q_path + listing_qs('')),
             ('GET', '/v1/%s' % q_path +
              listing_qs('1:/AUTH_bob/c/o1')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container))])
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1')])
        # but no object copy is attempted
        self.assertEqual(self.reconciler.stats['unavailable_source'], 1)
        self.assertEqual(self.reconciler.stats['copy_attempt'], 0)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1')])
        # src object is un-modified
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 0)
        # queue is un-changed, we'll have to retry
        self.assertEqual(self.reconciler.stats['pop_queue'], 0)
        self.assertEqual(deleted_container_entries, [])
        self.assertEqual(self.reconciler.stats['retry'], 1)

    def test_object_move_fails_cleanup(self):
        # setup the cluster
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3600.123456,
            (1, '/AUTH_bob/c/o1'): 3600.123457,  # slightly newer
        })
        self._mock_oldest_spi({'c': 0})  # destination

        # make the DELETE blow up
        self.fake_swift.storage_policy[1].register(
            'DELETE', '/v1/AUTH_bob/c/o1', swob.HTTPServiceUnavailable, {})
        # turn the crank
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # proceed with the move
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(self.reconciler.stats['copy_success'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1'),  # 2
             ('DELETE', '/v1/AUTH_bob/c/o1')])  # 4
        delete_headers = self.fake_swift.storage_policy[1].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),  # 1
             ('PUT', '/v1/AUTH_bob/c/o1')])  # 3
        # .. with source timestamp + offset 2
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3600.123457, offset=2))
        # we try to cleanup
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        # ... with q_ts + offset 1
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3600.12346, offset=1))
        # but cleanup fails!
        self.assertEqual(self.reconciler.stats['cleanup_failed'], 1)
        # so the queue is not popped
        self.assertEqual(self.reconciler.stats['pop_queue'], 0)
        self.assertEqual(deleted_container_entries, [])
        # and we'll have to retry
        self.assertEqual(self.reconciler.stats['retry'], 1)

    def test_object_move_src_object_is_forever_gone(self):
        # oh boy, hate to be here - this is an oldy
        q_ts = self.start_interval - self.reconciler.reclaim_age - 1
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): q_ts,
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1')])
        # but it's gone :\
        self.assertEqual(self.reconciler.stats['lost_source'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1')])
        # gah, look, even if it was out there somewhere - we've been at this
        # two weeks and haven't found it.  We can't just keep looking forever,
        # so... we're done
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        # dunno if this is helpful, but FWIW we don't throw tombstones?
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 0)
        self.assertEqual(self.reconciler.stats['success'], 1)  # lol

    def test_object_move_dest_already_moved(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3679.2019,
            (1, "/AUTH_bob/c/o1"): 3679.2019,
            (0, "/AUTH_bob/c/o1"): 3679.2019,
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # we look for misplaced objects
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # but we found it already in the right place!
        self.assertEqual(self.reconciler.stats['found_object'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1')])
        # so no attempt to read the source is made, but we do cleanup
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('DELETE', '/v1/AUTH_bob/c/o1')])
        delete_headers = self.fake_swift.storage_policy[1].headers[0]
        # rather we just clean up the dark matter
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3679.2019, offset=1))
        # and wipe our hands of it
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_dest_object_newer_than_queue_entry(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3679.2019,
            (1, "/AUTH_bob/c/o1"): 3679.2019,
            (0, "/AUTH_bob/c/o1"): 3679.2019 + 0.00001,  # slightly newer
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # we look for misplaced objects...
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # but we found it already in the right place!
        self.assertEqual(self.reconciler.stats['found_object'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1')])
        # so not attempt to read is made, but we do cleanup
        self.assertEqual(self.reconciler.stats['copy_attempt'], 0)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('DELETE', '/v1/AUTH_bob/c/o1')])
        delete_headers = self.fake_swift.storage_policy[1].headers[0]
        # rather we just clean up the dark matter
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)

        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3679.2019, offset=1))
        # and since we cleaned up the old object, so this counts as done
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_dest_object_older_than_queue_entry(self):
        self._mock_listing({
            (None, "/.misplaced_objects/36000/1:/AUTH_bob/c/o1"): 36123.38393,
            (1, "/AUTH_bob/c/o1"): 36123.38393,
            (0, "/AUTH_bob/c/o1"): 36123.38393 - 0.00001,  # slightly older
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # we found a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('36000')),
             ('GET', '/v1/.misplaced_objects/36000' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/36000' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # and since our version is *newer*, we overwrite
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(self.reconciler.stats['copy_success'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1'),  # 2
             ('DELETE', '/v1/AUTH_bob/c/o1')])  # 4
        delete_headers = self.fake_swift.storage_policy[1].headers[1]
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),  # 1
             ('PUT', '/v1/AUTH_bob/c/o1')])  # 3
        # ... with a q_ts + offset 2
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(36123.38393, offset=2))
        # then clean the dark matter
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # ... with a q_ts + offset 1
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(36123.38393, offset=1))

        # and pop the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '36000', '1:/AUTH_bob/c/o1')])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_put_fails(self):
        # setup the cluster
        self._mock_listing({
            (None, "/.misplaced_objects/36000/1:/AUTH_bob/c/o1"): 36123.383925,
            (1, "/AUTH_bob/c/o1"): 36123.383925,
        })
        self._mock_oldest_spi({'c': 0})

        # make the put to dest fail!
        self.fake_swift.storage_policy[0].register(
            'PUT', '/v1/AUTH_bob/c/o1', swob.HTTPServiceUnavailable, {})

        # turn the crank
        deleted_container_entries = self._run_once()

        # we find a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('36000')),
             ('GET', '/v1/.misplaced_objects/36000' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/36000' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # and try to move it, but it fails
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1')])  # 2
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),  # 1
             ('PUT', '/v1/AUTH_bob/c/o1')])  # 3
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        # ...with q_ts + offset 2 (20-microseconds)
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(36123.383925, offset=2))
        # but it failed
        self.assertEqual(self.reconciler.stats['copy_success'], 0)
        self.assertEqual(self.reconciler.stats['copy_failed'], 1)
        # ... so we don't clean up the source
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 0)
        # and we don't pop the queue
        self.assertEqual(deleted_container_entries, [])
        self.assertEqual(self.reconciler.stats['unhandled_errors'], 0)
        self.assertEqual(self.reconciler.stats['retry'], 1)

    def test_object_move_put_blows_up_crazy_town(self):
        # setup the cluster
        self._mock_listing({
            (None, "/.misplaced_objects/36000/1:/AUTH_bob/c/o1"): 36123.383925,
            (1, "/AUTH_bob/c/o1"): 36123.383925,
        })
        self._mock_oldest_spi({'c': 0})

        # make the put to dest blow up crazy town
        def blow_up(*args, **kwargs):
            raise Exception('kaboom!')

        self.fake_swift.storage_policy[0].register(
            'PUT', '/v1/AUTH_bob/c/o1', blow_up, {})

        # turn the crank
        deleted_container_entries = self._run_once()

        # we find a misplaced object
        self.assertEqual(self.reconciler.stats['misplaced_object'], 1)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('36000')),
             ('GET', '/v1/.misplaced_objects/36000' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/36000' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # and attempt to move it
        self.assertEqual(self.reconciler.stats['copy_attempt'], 1)
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_bob/c/o1')])  # 2
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1'),  # 1
             ('PUT', '/v1/AUTH_bob/c/o1')])  # 3
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        # ...with q_ts + offset 2 (20-microseconds)
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(36123.383925, offset=2))
        # but it blows up hard
        self.assertEqual(self.reconciler.stats['unhandled_error'], 1)
        # so we don't cleanup
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 0)
        # and we don't pop the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 0)
        self.assertEqual(deleted_container_entries, [])
        self.assertEqual(self.reconciler.stats['retry'], 1)

    def test_object_move_no_such_object_no_tombstone_recent(self):
        q_ts = float(Timestamp.now())
        container = str(int(q_ts // 3600 * 3600))
        q_path = '.misplaced_objects/%s' % container

        self._mock_listing({
            (None, "/%s/1:/AUTH_jeb/c/o1" % q_path): q_ts
        })
        self._mock_oldest_spi({'c': 0})

        deleted_container_entries = self._run_once()

        self.assertEqual(
            self.fake_swift.calls,
            [('GET', '/v1/.misplaced_objects/%s' % container + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/%s' % container +
              listing_qs('1:/AUTH_jeb/c/o1')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container))])
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_jeb/c/o1')],
        )
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_jeb/c/o1')],
        )
        # the queue entry is recent enough that there could easily be
        # tombstones on offline nodes or something, so we'll just leave it
        # here and try again later
        self.assertEqual(deleted_container_entries, [])

    def test_object_move_no_such_object_no_tombstone_ancient(self):
        queue_ts = float(Timestamp.now()) - \
            self.reconciler.reclaim_age * 1.1
        container = str(int(queue_ts // 3600 * 3600))

        self._mock_listing({
            (
                None, "/.misplaced_objects/%s/1:/AUTH_jeb/c/o1" % container
            ): queue_ts
        })
        self._mock_oldest_spi({'c': 0})

        deleted_container_entries = self._run_once()

        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container)),
             ('GET', '/v1/.misplaced_objects/%s' % container + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/%s' % container +
              listing_qs('1:/AUTH_jeb/c/o1'))])
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_jeb/c/o1')],
        )
        self.assertEqual(
            self.fake_swift.storage_policy[1].calls,
            [('GET', '/v1/AUTH_jeb/c/o1')],
        )

        # the queue entry is old enough that the tombstones, if any, have
        # probably been reaped, so we'll just give up
        self.assertEqual(
            deleted_container_entries,
            [('.misplaced_objects', container, '1:/AUTH_jeb/c/o1')])

    def test_delete_old_empty_queue_containers(self):
        ts = time.time() - self.reconciler.reclaim_age * 1.1
        container = str(int(ts // 3600 * 3600))
        older_ts = ts - 3600
        older_container = str(int(older_ts // 3600 * 3600))
        self._mock_listing({
            (None, "/.misplaced_objects/%s/" % container): 0,
            (None, "/.misplaced_objects/%s/something" % older_container): 0,
        })
        deleted_container_entries = self._run_once()
        self.assertEqual(deleted_container_entries, [])
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs(container)),
             ('GET', '/v1/.misplaced_objects/%s' % container + listing_qs('')),
             ('DELETE', '/v1/.misplaced_objects/%s' % container),
             ('GET', '/v1/.misplaced_objects/%s' % older_container +
              listing_qs('')),
             ('GET', '/v1/.misplaced_objects/%s' % older_container +
              listing_qs('something'))])
        self.assertEqual(self.reconciler.stats['invalid_record'], 1)

    def test_iter_over_old_containers_in_reverse(self):
        step = reconciler.MISPLACED_OBJECTS_CONTAINER_DIVISOR
        now = self.start_interval
        containers = []
        for i in range(10):
            container_ts = int(now - step * i)
            container_name = str(container_ts // 3600 * 3600)
            containers.append(container_name)
        # add some old containers too
        now -= self.reconciler.reclaim_age
        old_containers = []
        for i in range(10):
            container_ts = int(now - step * i)
            container_name = str(container_ts // 3600 * 3600)
            old_containers.append(container_name)
        containers.sort()
        old_containers.sort()
        all_containers = old_containers + containers
        self._mock_listing(dict((
            (None, "/.misplaced_objects/%s/" % container), 0
        ) for container in all_containers))
        deleted_container_entries = self._run_once()
        self.assertEqual(deleted_container_entries, [])
        last_container = all_containers[-1]
        account_listing_calls = [
            ('GET', '/v1/.misplaced_objects' + listing_qs('')),
            ('GET', '/v1/.misplaced_objects' + listing_qs(last_container)),
        ]
        new_container_calls = [
            ('GET', '/v1/.misplaced_objects/%s' % container +
             listing_qs('')) for container in reversed(containers)
        ][1:]  # current_container get's skipped the second time around...
        old_container_listings = [
            ('GET', '/v1/.misplaced_objects/%s' % container +
             listing_qs('')) for container in reversed(old_containers)
        ]
        old_container_deletes = [
            ('DELETE', '/v1/.misplaced_objects/%s' % container)
            for container in reversed(old_containers)
        ]
        old_container_calls = list(itertools.chain(*zip(
            old_container_listings, old_container_deletes)))
        self.assertEqual(self.fake_swift.calls,
                         [('GET', self.current_container_path)] +
                         account_listing_calls + new_container_calls +
                         old_container_calls)

    def test_error_in_iter_containers(self):
        self._mock_listing({})

        # make the listing return an error
        self.fake_swift.storage_policy[None].register(
            'GET', '/v1/.misplaced_objects' + listing_qs(''),
            swob.HTTPServiceUnavailable, {})

        self._run_once()
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs(''))])
        self.assertEqual(self.reconciler.stats, {})
        errors = self.reconciler.logger.get_lines_for_level('error')
        self.assertEqual(errors, [
            'Error listing containers in account '
            '.misplaced_objects (Unexpected response: '
            '503 Service Unavailable)'])

    def test_unhandled_exception_in_reconcile(self):
        self._mock_listing({})

        # make the listing blow up
        def blow_up(*args, **kwargs):
            raise Exception('kaboom!')

        self.fake_swift.storage_policy[None].register(
            'GET', '/v1/.misplaced_objects' + listing_qs(''),
            blow_up, {})
        self._run_once()
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs(''))])
        self.assertEqual(self.reconciler.stats, {})
        errors = self.reconciler.logger.get_lines_for_level('error')
        self.assertEqual(errors,
                         ['Unhandled Exception trying to reconcile: '])


if __name__ == '__main__':
    unittest.main()
