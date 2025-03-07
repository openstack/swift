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
import shutil
from functools import partial
from tempfile import mkdtemp

from unittest import mock
import operator
import time
import unittest
import socket
import os
import errno
import itertools
import random
import eventlet

from collections import defaultdict
from datetime import datetime
import urllib.parse
from swift.common.storage_policy import StoragePolicy, ECStoragePolicy
from swift.common.swob import Request

from swift.container import reconciler
from swift.container.server import gen_resp_headers, ContainerController
from swift.common.direct_client import ClientException
from swift.common import swob
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import split_path, Timestamp, encode_timestamps, \
    mkdirs, UTC

from test.debug_logger import debug_logger
from test.unit import FakeRing, fake_http_connect, patch_policies, \
    DEFAULT_TEST_EC_TYPE, make_timestamp_iter
from test.unit.common.middleware import helpers


def timestamp_to_last_modified(timestamp):
    dt = datetime.fromtimestamp(float(Timestamp(timestamp)), UTC)
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%f')


def container_resp_headers(**kwargs):
    return HeaderKeyDict(gen_resp_headers(kwargs))


class FakeStoragePolicySwift(object):

    def __init__(self):
        self.storage_policy = defaultdict(
            partial(helpers.FakeSwift, capture_unexpected_calls=False))
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
    def __init__(self, listings=None):
        self.app = FakeStoragePolicySwift()
        self.user_agent = 'fake-internal-client'
        self.request_tries = 1
        self.use_replication_network = True
        self.parse(listings)
        self.container_ring = FakeRing()

    def parse(self, listings):
        listings = listings or {}
        self.accounts = defaultdict(lambda: defaultdict(list))
        for item, timestamp in listings.items():
            # XXX this interface is stupid
            if isinstance(timestamp, tuple):
                timestamp, content_type = timestamp
            else:
                timestamp, content_type = timestamp, 'application/x-put'
            storage_policy_index, path = item
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
                    # some tests setup mock listings using floats, some use
                    # strings, so normalize here
                    ts = Timestamp(timestamp)
                    headers = {'X-Timestamp': ts.normal,
                               'X-Backend-Timestamp': ts.internal}
                    # register object response
                    self.app.storage_policy[storage_policy_index].register(
                        'GET', obj_path, swob.HTTPOk, headers)
                    self.app.storage_policy[storage_policy_index].register(
                        'DELETE', obj_path, swob.HTTPNoContent, {})
                    # container listing entry
                    obj_data = {
                        'bytes': 0,
                        # listing data is unicode
                        'name': obj_name,
                        'last_modified': ts.isoformat,
                        'hash': ts.internal,
                        'content_type': content_type,
                    }
                    container_listing_data.append(obj_data)
                container_listing_data.sort(key=operator.itemgetter('name'))
                # register container listing response
                container_headers = {}
                container_qry_string = helpers.normalize_query_string(
                    '?format=json&marker=&end_marker=&prefix=')
                self.app.register('GET', container_path + container_qry_string,
                                  swob.HTTPOk, container_headers,
                                  json.dumps(container_listing_data))
                if container_listing_data:
                    obj_name = container_listing_data[-1]['name']
                    # client should quote and encode marker
                    end_qry_string = helpers.normalize_query_string(
                        '?format=json&marker=%s&end_marker=&prefix=' % (
                            urllib.parse.quote(obj_name.encode('utf-8'))))
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
        self.tempdir = mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

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

    @patch_policies(
        [StoragePolicy(0, 'zero', is_default=True),
         StoragePolicy(1, 'one'),
         StoragePolicy(2, 'two')])
    def test_get_container_policy_index_for_recently_split_recreated(self):
        # verify that get_container_policy_index reaches same conclusion as a
        # container server that receives all requests in chronological order
        ts_iter = make_timestamp_iter()
        ts = [next(ts_iter) for _ in range(8)]

        # make 3 container replicas
        device_dirs = [os.path.join(self.tempdir, str(i)) for i in range(3)]
        for device_dir in device_dirs:
            mkdirs(os.path.join(device_dir, 'sda1'))
        controllers = [ContainerController(
            {'devices': devices,
             'mount_check': 'false',
             'replication_server': 'true'})
            for devices in device_dirs]

        # initial PUT goes to all 3 replicas
        responses = []
        for controller in controllers:
            req = Request.blank('/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': ts[0].internal,
                'X-Backend-Storage-Policy-Index': 0,
            })
            responses.append(req.get_response(controller))
        self.assertEqual([resp.status_int for resp in responses],
                         [201, 201, 201])

        # DELETE to all 3 replicas
        responses = []
        for controller in controllers:
            req = Request.blank('/sda1/p/a/c', method='DELETE', headers={
                'X-Timestamp': ts[2].internal,
            })
            responses.append(req.get_response(controller))
        self.assertEqual([resp.status_int for resp in responses],
                         [204, 204, 204])

        # first recreate PUT, SPI=1, goes to replicas 0 and 1
        responses = []
        for controller in controllers[:2]:
            req = Request.blank('/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': ts[3].internal,
                'X-Backend-Storage-Policy-Index': 1,
            })
            responses.append(req.get_response(controller))
        # all ok, PUT follows DELETE
        self.assertEqual([resp.status_int for resp in responses],
                         [201, 201])

        # second recreate PUT, SPI=2, goes to replicas 0 and 2
        responses = []
        for controller in [controllers[0], controllers[2]]:
            req = Request.blank('/sda1/p/a/c', method='PUT', headers={
                'X-Timestamp': ts[5].internal,
                'X-Backend-Storage-Policy-Index': 2,
            })
            responses.append(req.get_response(controller))
        # note: 409 from replica 0 because PUT follows previous PUT
        self.assertEqual([resp.status_int for resp in responses],
                         [409, 201])

        # now do a HEAD on all replicas
        responses = []
        for controller in controllers:
            req = Request.blank('/sda1/p/a/c', method='HEAD')
            responses.append(req.get_response(controller))
        self.assertEqual([resp.status_int for resp in responses],
                         [204, 204, 204])

        resp_headers = [resp.headers for resp in responses]
        # replica 0 should be authoritative because it received all requests
        self.assertEqual(ts[3].internal, resp_headers[0]['X-Put-Timestamp'])
        self.assertEqual('1',
                         resp_headers[0]['X-Backend-Storage-Policy-Index'])
        self.assertEqual(ts[3].internal, resp_headers[1]['X-Put-Timestamp'])
        self.assertEqual('1',
                         resp_headers[1]['X-Backend-Storage-Policy-Index'])
        self.assertEqual(ts[5].internal, resp_headers[2]['X-Put-Timestamp'])
        self.assertEqual('2',
                         resp_headers[2]['X-Backend-Storage-Policy-Index'])

        # now feed the headers from each replica to
        # direct_get_container_policy_index
        mock_path = 'swift.container.reconciler.direct_head_container'
        random.shuffle(resp_headers)
        with mock.patch(mock_path) as direct_head:
            direct_head.side_effect = resp_headers
            oldest_spi = reconciler.direct_get_container_policy_index(
                self.fake_ring, 'a', 'con')
        # expect the same outcome as the authoritative replica 0
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
    return helpers.normalize_query_string(
        "?format=json&marker=%s&end_marker=&prefix=" %
        urllib.parse.quote(marker.encode('utf-8')))


@patch_policies(
    [StoragePolicy(0, 'zero', is_default=True),
     ECStoragePolicy(1, 'one', ec_type=DEFAULT_TEST_EC_TYPE,
                     ec_ndata=6, ec_nparity=2), ],
    fake_ring_args=[{}, {'replicas': 8}])
class TestReconciler(unittest.TestCase):

    maxDiff = None

    def setUp(self):
        self.logger = debug_logger()
        conf = {}
        self.swift = FakeInternalClient()
        self.reconciler = reconciler.ContainerReconciler(
            conf, logger=self.logger, swift=self.swift)
        self.start_interval = int(time.time() // 3600 * 3600)
        self.current_container_path = '/v1/.misplaced_objects/%d' % (
            self.start_interval) + listing_qs('')

    def test_concurrency_config(self):
        conf = {}
        r = reconciler.ContainerReconciler(conf, self.logger, self.swift)
        self.assertEqual(r.concurrency, 1)

        conf = {'concurrency': '10'}
        r = reconciler.ContainerReconciler(conf, self.logger, self.swift)
        self.assertEqual(r.concurrency, 10)

        conf = {'concurrency': 48}
        r = reconciler.ContainerReconciler(conf, self.logger, self.swift)
        self.assertEqual(r.concurrency, 48)

        conf = {'concurrency': 0}
        self.assertRaises(ValueError, reconciler.ContainerReconciler,
                          conf, self.logger, self.swift)

        conf = {'concurrency': '-1'}
        self.assertRaises(ValueError, reconciler.ContainerReconciler,
                          conf, self.logger, self.swift)

    def test_processes_config(self):
        conf = {}
        r = reconciler.ContainerReconciler(conf, self.logger, self.swift)
        self.assertEqual(r.process, 0)
        self.assertEqual(r.processes, 0)

        conf = {'processes': '1'}
        r = reconciler.ContainerReconciler(conf, self.logger, self.swift)
        self.assertEqual(r.process, 0)
        self.assertEqual(r.processes, 1)

        conf = {'processes': 10, 'process': '9'}
        r = reconciler.ContainerReconciler(conf, self.logger, self.swift)
        self.assertEqual(r.process, 9)
        self.assertEqual(r.processes, 10)

        conf = {'processes': -1}
        self.assertRaises(ValueError, reconciler.ContainerReconciler,
                          conf, self.logger, self.swift)

        conf = {'process': -1}
        self.assertRaises(ValueError, reconciler.ContainerReconciler,
                          conf, self.logger, self.swift)

        conf = {'processes': 9, 'process': 9}
        self.assertRaises(ValueError, reconciler.ContainerReconciler,
                          conf, self.logger, self.swift)

    def test_init_internal_client_log_name(self):
        def _do_test_init_ic_log_name(conf, exp_internal_client_log_name):
            with mock.patch(
                    'swift.container.reconciler.InternalClient') \
                    as mock_ic:
                reconciler.ContainerReconciler(conf)
            mock_ic.assert_called_once_with(
                '/etc/swift/container-reconciler.conf',
                'Swift Container Reconciler', 3,
                global_conf={'log_name': exp_internal_client_log_name},
                use_replication_network=True)

        _do_test_init_ic_log_name({}, 'container-reconciler-ic')
        _do_test_init_ic_log_name({'log_name': 'my-container-reconciler'},
                                  'my-container-reconciler-ic')

    def _mock_listing(self, objects):
        self.swift.parse(objects)
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

        return [tuple(list(c[1][1:4]) + [c[2]['headers']['X-Timestamp']])
                for c in mocks['direct_delete_container_entry'].mock_calls]

    def test_no_concurrency(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3618.84187,
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o2"): 3724.23456,
            (1, "/AUTH_bob/c/o1"): 3618.84187,
            (1, "/AUTH_bob/c/o2"): 3724.23456,
        })

        order_recieved = []

        def fake_reconcile_object(account, container, obj, q_policy_index,
                                  q_ts, q_op, path, **kwargs):
            order_recieved.append(obj)
            return True

        self.reconciler._reconcile_object = fake_reconcile_object
        self.assertEqual(self.reconciler.concurrency, 1)  # sanity
        deleted_container_entries = self._run_once()
        self.assertEqual(order_recieved, ['o1', 'o2'])
        # process in order recieved
        self.assertEqual(deleted_container_entries, [
            ('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
             Timestamp(3618.84187, offset=2).internal),
            ('.misplaced_objects', '3600', '1:/AUTH_bob/c/o2',
             Timestamp(3724.23456, offset=2).internal)
        ])

    def test_concurrency(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3618.84187,
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o2"): 3724.23456,
            (1, "/AUTH_bob/c/o1"): 3618.84187,
            (1, "/AUTH_bob/c/o2"): 3724.23456,
        })

        order_recieved = []

        def fake_reconcile_object(account, container, obj, q_policy_index,
                                  q_ts, q_op, path, **kwargs):
            order_recieved.append(obj)
            if obj == 'o1':
                # o1 takes longer than o2 for some reason
                for i in range(10):
                    eventlet.sleep(0.0)
            return True

        self.reconciler._reconcile_object = fake_reconcile_object
        self.reconciler.concurrency = 2
        deleted_container_entries = self._run_once()
        self.assertEqual(order_recieved, ['o1', 'o2'])
        # ... and so we finish o2 first
        self.assertEqual(deleted_container_entries, [
            ('.misplaced_objects', '3600', '1:/AUTH_bob/c/o2',
             Timestamp(3724.23456, offset=2).internal),
            ('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
             Timestamp(3618.84187, offset=2).internal),
        ])

    def test_multi_process_should_process(self):
        def mkqi(a, c, o):
            "make queue item"
            return {
                'account': a,
                'container': c,
                'obj': o,
            }
        queue = [
            mkqi('a', 'c', 'o1'),
            mkqi('a', 'c', 'o2'),
            mkqi('a', 'c', 'o3'),
            mkqi('a', 'c', 'o4'),
        ]

        def map_should_process(process, processes):
            self.reconciler.process = process
            self.reconciler.processes = processes
            with mock.patch('swift.common.utils.HASH_PATH_SUFFIX',
                            b'endcap'), \
                    mock.patch('swift.common.utils.HASH_PATH_PREFIX', b''):
                return [self.reconciler.should_process(q_item)
                        for q_item in queue]

        def check_process(process, processes, expected):
            should_process = map_should_process(process, processes)
            try:
                self.assertEqual(should_process, expected)
            except AssertionError as e:
                self.fail('unexpected items processed for %s/%s\n%s' % (
                    process, processes, e))

        check_process(0, 0, [True] * 4)
        check_process(0, 1, [True] * 4)
        check_process(0, 2, [False, True, False, False])
        check_process(1, 2, [True, False, True, True])

        check_process(0, 4, [False, True, False, False])
        check_process(1, 4, [True, False, False, False])
        check_process(2, 4, [False] * 4)  # lazy
        check_process(3, 4, [False, False, True, True])

        queue = [mkqi('a%s' % i, 'c%s' % i, 'o%s' % i) for i in range(1000)]
        items_handled = [0] * 1000
        for process in range(100):
            should_process = map_should_process(process, 100)
            for i, handled in enumerate(should_process):
                if handled:
                    items_handled[i] += 1
        self.assertEqual([1] * 1000, items_handled)

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
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(3618.84187, offset=2).internal)])
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
            '.misplaced_objects', '3600', '1:/%s' % obj_path,
            Timestamp(3618.84187, offset=2).internal)])
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

    @patch_policies(
        [StoragePolicy(0, 'zero', is_default=True),
         StoragePolicy(1, 'one'),
         ECStoragePolicy(2, 'two', ec_type=DEFAULT_TEST_EC_TYPE,
                         ec_ndata=6, ec_nparity=2)],
        fake_ring_args=[
            {'next_part_power': 1}, {}, {'next_part_power': 1}])
    def test_can_reconcile_policy(self):
        for policy_index, expected in ((0, False), (1, True), (2, False),
                                       (3, False), ('apple', False),
                                       (None, False)):
            self.assertEqual(
                self.reconciler.can_reconcile_policy(policy_index), expected)

    @patch_policies(
        [StoragePolicy(0, 'zero', is_default=True),
         ECStoragePolicy(1, 'one', ec_type=DEFAULT_TEST_EC_TYPE,
                         ec_ndata=6, ec_nparity=2), ],
        fake_ring_args=[{'next_part_power': 1}, {}])
    def test_fail_to_move_if_ppi(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3618.84187,
            (1, "/AUTH_bob/c/o1"): 3618.84187,
        })
        self._mock_oldest_spi({'c': 0})
        deleted_container_entries = self._run_once()

        # skipped sending because policy_index 0 is in the middle of a PPI
        self.assertFalse(deleted_container_entries)
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        self.assertEqual(self.reconciler.stats['ppi_skip'], 1)
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
        # we PUT the object in the right place with q_ts + offset 3
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=3))
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
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(3618.84187, offset=2).internal)])
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
        # we PUT the object in the right place with q_ts + offset 3
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=3).internal)
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
                         [('.misplaced_objects', '3600', '0:/AUTH_bob/c/o1',
                           Timestamp(3618.84187, offset=2).internal)])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_with_unicode_and_spaces(self):
        # the "name" in listings and the unicode string passed to all
        # functions where we call them with (account, container, obj)
        obj_name = u"AUTH_bob/c \u062a/o1 \u062a"
        # anytime we talk about a call made to swift for a path
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
        # we PUT the object in the right place with q_ts + offset 3
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3618.84187, offset=3).internal)
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
                         [('.misplaced_objects', '3600', '1:/%s' % obj_name,
                           Timestamp(3618.84187, offset=2).internal)])
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
        # we DELETE the object in the right place with q_ts + offset 3
        self.assertEqual(reconcile_headers.get('X-Timestamp'),
                         Timestamp(q_ts, offset=3).internal)
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
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(q_ts, offset=2).internal)])
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
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(3618.84187, offset=2).internal)])
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
        # .. with source timestamp + offset 3
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3600.234567, offset=3))
        # src object is cleaned up
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # ... with q_ts + offset 1
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(3600.123456, offset=1))
        # and queue is popped
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(3600.123456, offset=2).internal)])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_src_object_older_than_queue_entry(self):
        # should be some sort of retry case
        q_ts = time.time()
        container = str(int(q_ts // 3600 * 3600))
        q_path = '.misplaced_objects/%s' % container
        self._mock_listing({
            (None, "/%s/1:/AUTH_bob/c/o1" % q_path): q_ts,
            (1, '/AUTH_bob/c/o1'): q_ts - 1,  # slightly older
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
            {'X-Backend-Timestamp': Timestamp(q_ts, offset=3).internal})
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

    def test_object_move_fails_preflight(self):
        # setup the cluster
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3600.123456,
            (1, '/AUTH_bob/c/o1'): 3600.123457,  # slightly newer
        })
        self._mock_oldest_spi({'c': 0})  # destination

        # make the HEAD blow up
        self.fake_swift.storage_policy[0].register(
            'HEAD', '/v1/AUTH_bob/c/o1', swob.HTTPServiceUnavailable, {})
        # turn the crank
        deleted_container_entries = self._run_once()

        # we did some listings...
        self.assertEqual(
            self.fake_swift.calls,
            [('GET', self.current_container_path),
             ('GET', '/v1/.misplaced_objects' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects' + listing_qs('3600')),
             ('GET', '/v1/.misplaced_objects/3600' + listing_qs('')),
             ('GET', '/v1/.misplaced_objects/3600' +
              listing_qs('1:/AUTH_bob/c/o1'))])
        # ...but we can't even tell whether anything's misplaced or not
        self.assertEqual(self.reconciler.stats['misplaced_object'], 0)
        self.assertEqual(self.reconciler.stats['unavailable_destination'], 1)
        # so we don't try to do any sort of move or cleanup
        self.assertEqual(self.reconciler.stats['copy_attempt'], 0)
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 0)
        self.assertEqual(self.reconciler.stats['pop_queue'], 0)
        self.assertEqual(deleted_container_entries, [])
        # and we'll have to try again later
        self.assertEqual(self.reconciler.stats['retry'], 1)
        self.assertEqual(self.fake_swift.storage_policy[1].calls, [])
        self.assertEqual(
            self.fake_swift.storage_policy[0].calls,
            [('HEAD', '/v1/AUTH_bob/c/o1')])

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
        # .. with source timestamp + offset 3
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(3600.123457, offset=3))
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
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(q_ts, offset=2).internal)])
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
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(3679.2019, offset=2).internal)])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_dest_object_newer_than_queue_entry(self):
        self._mock_listing({
            (None, "/.misplaced_objects/3600/1:/AUTH_bob/c/o1"): 3679.2019,
            (1, "/AUTH_bob/c/o1"): 3679.2019,
            (0, "/AUTH_bob/c/o1"): 3679.2019 + 1,  # slightly newer
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
                         [('.misplaced_objects', '3600', '1:/AUTH_bob/c/o1',
                           Timestamp(3679.2019, offset=2).internal)])
        self.assertEqual(self.reconciler.stats['success'], 1)

    def test_object_move_dest_object_older_than_queue_entry(self):
        self._mock_listing({
            (None, "/.misplaced_objects/36000/1:/AUTH_bob/c/o1"): 36123.38393,
            (1, "/AUTH_bob/c/o1"): 36123.38393,
            (0, "/AUTH_bob/c/o1"): 36123.38393 - 1,  # slightly older
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
        # ... with a q_ts + offset 3
        put_headers = self.fake_swift.storage_policy[0].headers[1]
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(36123.38393, offset=3))
        # then clean the dark matter
        self.assertEqual(self.reconciler.stats['cleanup_attempt'], 1)
        self.assertEqual(self.reconciler.stats['cleanup_success'], 1)
        # ... with a q_ts + offset 1
        self.assertEqual(delete_headers.get('X-Timestamp'),
                         Timestamp(36123.38393, offset=1))

        # and pop the queue
        self.assertEqual(self.reconciler.stats['pop_queue'], 1)
        self.assertEqual(deleted_container_entries,
                         [('.misplaced_objects', '36000', '1:/AUTH_bob/c/o1',
                           Timestamp(36123.38393, offset=2).internal)])
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
        # ...with q_ts + offset 3 (20-microseconds)
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(36123.383925, offset=3))
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
        # ...with q_ts + offset 3 (20-microseconds)
        self.assertEqual(put_headers.get('X-Timestamp'),
                         Timestamp(36123.383925, offset=3))
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
            [('.misplaced_objects', container, '1:/AUTH_jeb/c/o1',
              Timestamp(queue_ts, offset=2).internal)])

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
