# Copyright (c) 2010-2012 OpenStack Foundation
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
import unittest
import os
from contextlib import contextmanager
from hashlib import md5
import time
import pickle

import mock
import six
from six.moves import urllib

from swift.common import direct_client
from swift.common.direct_client import DirectClientException
from swift.common.exceptions import ClientException
from swift.common.header_key_dict import HeaderKeyDict
from swift.common.utils import Timestamp, quote
from swift.common.swob import RESPONSE_REASONS
from swift.common.storage_policy import POLICIES
from six.moves.http_client import HTTPException

from test.unit import patch_policies, debug_logger


class FakeConn(object):

    def __init__(self, status, headers=None, body='', **kwargs):
        self.status = status
        try:
            self.reason = RESPONSE_REASONS[self.status][0]
        except Exception:
            self.reason = 'Fake'
        self.body = body
        self.resp_headers = HeaderKeyDict()
        if headers:
            self.resp_headers.update(headers)
        self.etag = None

    def _update_raw_call_args(self, *args, **kwargs):
        capture_attrs = ('host', 'port', 'method', 'path', 'req_headers',
                         'query_string')
        for attr, value in zip(capture_attrs, args[:len(capture_attrs)]):
            setattr(self, attr, value)
        return self

    def getresponse(self):
        if self.etag:
            self.resp_headers['etag'] = str(self.etag.hexdigest())
        if isinstance(self.status, Exception):
            raise self.status
        return self

    def getheader(self, header, default=None):
        return self.resp_headers.get(header, default)

    def getheaders(self):
        return self.resp_headers.items()

    def read(self, amt=None):
        if isinstance(self.body, six.BytesIO):
            return self.body.read(amt)
        elif amt is None:
            return self.body
        else:
            return Exception('Not a StringIO entry')

    def send(self, data):
        if not self.etag:
            self.etag = md5()
        self.etag.update(data)


@contextmanager
def mocked_http_conn(*args, **kwargs):
    fake_conn = FakeConn(*args, **kwargs)
    mock_http_conn = lambda *args, **kwargs: \
        fake_conn._update_raw_call_args(*args, **kwargs)
    with mock.patch('swift.common.bufferedhttp.http_connect_raw',
                    new=mock_http_conn):
        yield fake_conn


@patch_policies
class TestDirectClient(unittest.TestCase):

    def setUp(self):
        self.node = json.loads(json.dumps({  # json roundtrip to ring-like
            'ip': '1.2.3.4', 'port': '6200', 'device': 'sda',
            'replication_ip': '1.2.3.5', 'replication_port': '7000'}))
        self.part = '0'

        self.account = u'\u062a account'
        self.container = u'\u062a container'
        self.obj = u'\u062a obj/name'
        self.account_path = '/sda/0/%s' % urllib.parse.quote(
            self.account.encode('utf-8'))
        self.container_path = '/sda/0/%s/%s' % tuple(
            urllib.parse.quote(p.encode('utf-8')) for p in (
                self.account, self.container))
        self.obj_path = '/sda/0/%s/%s/%s' % tuple(
            urllib.parse.quote(p.encode('utf-8')) for p in (
                self.account, self.container, self.obj))
        self.user_agent = 'direct-client %s' % os.getpid()

        class FakeTimeout(BaseException):
            def __enter__(self):
                return self

            def __exit__(self, typ, value, tb):
                pass

        patcher = mock.patch.object(direct_client, 'Timeout', FakeTimeout)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_gen_headers(self):
        stub_user_agent = 'direct-client %s' % os.getpid()

        headers = direct_client.gen_headers(add_ts=False)
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assertEqual(1, len(headers))

        now = time.time()
        headers = direct_client.gen_headers()
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assertTrue(now - 1 < Timestamp(headers['x-timestamp']) < now + 1)
        self.assertEqual(headers['x-timestamp'],
                         Timestamp(headers['x-timestamp']).internal)
        self.assertEqual(2, len(headers))

        headers = direct_client.gen_headers(hdrs_in={'x-timestamp': '15'})
        self.assertEqual(headers['x-timestamp'], '15')
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assertEqual(2, len(headers))

        headers = direct_client.gen_headers(hdrs_in={'foo-bar': '63'})
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assertEqual(headers['foo-bar'], '63')
        self.assertTrue(now - 1 < Timestamp(headers['x-timestamp']) < now + 1)
        self.assertEqual(headers['x-timestamp'],
                         Timestamp(headers['x-timestamp']).internal)
        self.assertEqual(3, len(headers))

        hdrs_in = {'foo-bar': '55'}
        headers = direct_client.gen_headers(hdrs_in, add_ts=False)
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assertEqual(headers['foo-bar'], '55')
        self.assertEqual(2, len(headers))

        headers = direct_client.gen_headers(hdrs_in={'user-agent': '32'})
        self.assertEqual(headers['user-agent'], '32')
        self.assertTrue(now - 1 < Timestamp(headers['x-timestamp']) < now + 1)
        self.assertEqual(headers['x-timestamp'],
                         Timestamp(headers['x-timestamp']).internal)
        self.assertEqual(2, len(headers))

        hdrs_in = {'user-agent': '47'}
        headers = direct_client.gen_headers(hdrs_in, add_ts=False)
        self.assertEqual(headers['user-agent'], '47')
        self.assertEqual(1, len(headers))

        for policy in POLICIES:
            for add_ts in (True, False):
                now = time.time()
                headers = direct_client.gen_headers(
                    {'X-Backend-Storage-Policy-Index': policy.idx},
                    add_ts=add_ts)
                self.assertEqual(headers['user-agent'], stub_user_agent)
                self.assertEqual(headers['X-Backend-Storage-Policy-Index'],
                                 str(policy.idx))
                expected_header_count = 2
                if add_ts:
                    expected_header_count += 1
                    self.assertEqual(
                        headers['x-timestamp'],
                        Timestamp(headers['x-timestamp']).internal)
                    self.assertTrue(
                        now - 1 < Timestamp(headers['x-timestamp']) < now + 1)
                self.assertEqual(expected_header_count, len(headers))

    def test_direct_get_account(self):
        def do_test(req_params):
            stub_headers = HeaderKeyDict({
                'X-Account-Container-Count': '1',
                'X-Account-Object-Count': '1',
                'X-Account-Bytes-Used': '1',
                'X-Timestamp': '1234567890',
                'X-PUT-Timestamp': '1234567890'})

            body = b'[{"count": 1, "bytes": 20971520, "name": "c1"}]'

            with mocked_http_conn(200, stub_headers, body) as conn:
                resp_headers, resp = direct_client.direct_get_account(
                    self.node, self.part, self.account, **req_params)
            try:
                self.assertEqual(conn.method, 'GET')
                self.assertEqual(conn.path, self.account_path)
                self.assertEqual(conn.req_headers['user-agent'],
                                 self.user_agent)
                self.assertEqual(resp_headers, stub_headers)
                self.assertEqual(json.loads(body), resp)
                self.assertIn('format=json', conn.query_string)
                for k, v in req_params.items():
                    if v is None:
                        self.assertNotIn('&%s' % k, conn.query_string)
                    else:
                        self.assertIn('&%s=%s' % (k, v), conn.query_string)

            except AssertionError as err:
                self.fail('Failed with params %s: %s' % (req_params, err))

        test_params = (dict(marker=marker, prefix=prefix, delimiter=delimiter,
                       limit=limit, end_marker=end_marker, reverse=reverse)
                       for marker in (None, 'my-marker')
                       for prefix in (None, 'my-prefix')
                       for delimiter in (None, 'my-delimiter')
                       for limit in (None, 1000)
                       for end_marker in (None, 'my-endmarker')
                       for reverse in (None, 'on'))

        for params in test_params:
            do_test(params)

    def test_direct_client_exception(self):
        stub_headers = {'X-Trans-Id': 'txb5f59485c578460f8be9e-0053478d09'}
        body = 'a server error has occurred'
        with mocked_http_conn(500, stub_headers, body):
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_get_account(self.node, self.part,
                                                 self.account)
        self.assertEqual(raised.exception.http_status, 500)
        expected_err_msg_parts = (
            'Account server %s:%s' % (self.node['ip'], self.node['port']),
            'GET %r' % self.account_path,
            'status 500',
        )
        for item in expected_err_msg_parts:
            self.assertIn(item, str(raised.exception))
        self.assertEqual(raised.exception.http_host, self.node['ip'])
        self.assertEqual(raised.exception.http_port, self.node['port'])
        self.assertEqual(raised.exception.http_device, self.node['device'])
        self.assertEqual(raised.exception.http_status, 500)
        self.assertEqual(raised.exception.http_reason, 'Internal Error')
        self.assertEqual(raised.exception.http_headers, stub_headers)

    def test_direct_get_account_no_content_does_not_parse_body(self):
        headers = {
            'X-Account-Container-Count': '1',
            'X-Account-Object-Count': '1',
            'X-Account-Bytes-Used': '1',
            'X-Timestamp': '1234567890',
            'X-Put-Timestamp': '1234567890'}
        with mocked_http_conn(204, headers) as conn:
            resp_headers, resp = direct_client.direct_get_account(
                self.node, self.part, self.account)
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.account_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertDictEqual(resp_headers, headers)
        self.assertEqual([], resp)

    def test_direct_get_account_error(self):
        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_get_account(
                    self.node, self.part, self.account)
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.account_path)
        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue('GET' in str(raised.exception))

    def test_direct_delete_account(self):
        part = '0'
        account = 'a'

        mock_path = 'swift.common.bufferedhttp.http_connect_raw'
        with mock.patch(mock_path) as fake_connect:
            fake_connect.return_value.getresponse.return_value.status = 200
            direct_client.direct_delete_account(self.node, part, account)
            args, kwargs = fake_connect.call_args
            ip = args[0]
            self.assertEqual(self.node['ip'], ip)
            port = args[1]
            self.assertEqual(self.node['port'], port)
            method = args[2]
            self.assertEqual('DELETE', method)
            path = args[3]
            self.assertEqual('/sda/0/a', path)
            headers = args[4]
            self.assertIn('X-Timestamp', headers)
            self.assertIn('User-Agent', headers)

    def test_direct_delete_account_failure(self):
        part = '0'
        account = 'a'

        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_delete_account(self.node, part, account)
            self.assertEqual(self.node['ip'], conn.host)
            self.assertEqual(self.node['port'], conn.port)
            self.assertEqual('DELETE', conn.method)
            self.assertEqual('/sda/0/a', conn.path)
            self.assertIn('X-Timestamp', conn.req_headers)
            self.assertIn('User-Agent', conn.req_headers)
            self.assertEqual(raised.exception.http_status, 500)

    def test_direct_head_container(self):
        headers = HeaderKeyDict(key='value')

        with mocked_http_conn(200, headers) as conn:
            resp = direct_client.direct_head_container(
                self.node, self.part, self.account, self.container)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(conn.req_headers['user-agent'],
                         self.user_agent)
        self.assertEqual(headers, resp)

    def test_direct_head_container_error(self):
        headers = HeaderKeyDict(key='value')

        with mocked_http_conn(503, headers) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_head_container(
                    self.node, self.part, self.account, self.container)
            # check request
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(raised.exception.http_status, 503)
        self.assertEqual(raised.exception.http_headers, headers)
        self.assertTrue('HEAD' in str(raised.exception))

    def test_direct_head_container_deleted(self):
        important_timestamp = Timestamp.now().internal
        headers = HeaderKeyDict({'X-Backend-Important-Timestamp':
                                 important_timestamp})

        with mocked_http_conn(404, headers) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_head_container(
                    self.node, self.part, self.account, self.container)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(raised.exception.http_status, 404)
        self.assertEqual(raised.exception.http_headers, headers)

    def test_direct_get_container(self):
        def do_test(req_params):
            headers = HeaderKeyDict({'key': 'value'})
            body = (b'[{"hash": "8f4e3", "last_modified": "317260", '
                    b'"bytes": 209}]')

            with mocked_http_conn(200, headers, body) as conn:
                resp_headers, resp = direct_client.direct_get_container(
                    self.node, self.part, self.account, self.container,
                    **req_params)

            try:
                self.assertEqual(conn.method, 'GET')
                self.assertEqual(conn.path, self.container_path)
                self.assertEqual(conn.req_headers['user-agent'],
                                 self.user_agent)
                self.assertEqual(headers, resp_headers)
                self.assertEqual(json.loads(body), resp)
                self.assertIn('format=json', conn.query_string)
                for k, v in req_params.items():
                    if v is None:
                        self.assertNotIn('&%s' % k, conn.query_string)
                    else:
                        self.assertIn('&%s=%s' % (k, v), conn.query_string)
            except AssertionError as err:
                self.fail('Failed with params %s: %s' % (req_params, err))

        test_params = (dict(marker=marker, prefix=prefix, delimiter=delimiter,
                       limit=limit, end_marker=end_marker, reverse=reverse)
                       for marker in (None, 'my-marker')
                       for prefix in (None, 'my-prefix')
                       for delimiter in (None, 'my-delimiter')
                       for limit in (None, 1000)
                       for end_marker in (None, 'my-endmarker')
                       for reverse in (None, 'on'))

        for params in test_params:
            do_test(params)

    def test_direct_get_container_no_content_does_not_decode_body(self):
        headers = {}
        body = ''
        with mocked_http_conn(204, headers, body) as conn:
            resp_headers, resp = direct_client.direct_get_container(
                self.node, self.part, self.account, self.container)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(headers, resp_headers)
        self.assertEqual([], resp)

    def test_direct_delete_container(self):
        with mocked_http_conn(200) as conn:
            direct_client.direct_delete_container(
                self.node, self.part, self.account, self.container)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.container_path)

    def test_direct_delete_container_with_timestamp(self):
        # ensure timestamp is different from any that might be auto-generated
        timestamp = Timestamp(time.time() - 100)
        headers = {'X-Timestamp': timestamp.internal}
        with mocked_http_conn(200) as conn:
            direct_client.direct_delete_container(
                self.node, self.part, self.account, self.container,
                headers=headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.container_path)
            self.assertTrue('X-Timestamp' in conn.req_headers)
            self.assertEqual(timestamp, conn.req_headers['X-Timestamp'])

    def test_direct_delete_container_error(self):
        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_delete_container(
                    self.node, self.part, self.account, self.container)

            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue('DELETE' in str(raised.exception))

    def test_direct_put_container(self):
        body = b'Let us begin with a quick introduction'
        headers = {'x-foo': 'bar', 'Content-Length': str(len(body)),
                   'Content-Type': 'application/json',
                   'User-Agent': 'my UA'}

        with mocked_http_conn(204) as conn:
            rv = direct_client.direct_put_container(
                self.node, self.part, self.account, self.container,
                contents=body, headers=headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.container_path)
            self.assertEqual(conn.req_headers['Content-Length'],
                             str(len(body)))
            self.assertEqual(conn.req_headers['Content-Type'],
                             'application/json')
            self.assertEqual(conn.req_headers['User-Agent'], 'my UA')
            self.assertTrue('x-timestamp' in conn.req_headers)
            self.assertEqual('bar', conn.req_headers.get('x-foo'))
            self.assertEqual(md5(body).hexdigest(), conn.etag.hexdigest())
        self.assertIsNone(rv)

    def test_direct_put_container_chunked(self):
        body = b'Let us begin with a quick introduction'
        headers = {'x-foo': 'bar', 'Content-Type': 'application/json'}

        with mocked_http_conn(204) as conn:
            rv = direct_client.direct_put_container(
                self.node, self.part, self.account, self.container,
                contents=body, headers=headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.container_path)
            self.assertEqual(conn.req_headers['Transfer-Encoding'], 'chunked')
            self.assertEqual(conn.req_headers['Content-Type'],
                             'application/json')
            self.assertTrue('x-timestamp' in conn.req_headers)
            self.assertEqual('bar', conn.req_headers.get('x-foo'))
            self.assertNotIn('Content-Length', conn.req_headers)
            expected_sent = b'%0x\r\n%s\r\n0\r\n\r\n' % (len(body), body)
            self.assertEqual(md5(expected_sent).hexdigest(),
                             conn.etag.hexdigest())
        self.assertIsNone(rv)

    def test_direct_put_container_fail(self):
        with mock.patch('swift.common.bufferedhttp.http_connect_raw',
                        side_effect=Exception('conn failed')):
            with self.assertRaises(Exception) as cm:
                direct_client.direct_put_container(
                    self.node, self.part, self.account, self.container)
        self.assertEqual('conn failed', str(cm.exception))

        with mocked_http_conn(Exception('resp failed')):
            with self.assertRaises(Exception) as cm:
                direct_client.direct_put_container(
                    self.node, self.part, self.account, self.container)
        self.assertEqual('resp failed', str(cm.exception))

    def test_direct_put_container_object(self):
        headers = {'x-foo': 'bar'}

        with mocked_http_conn(204) as conn:
            rv = direct_client.direct_put_container_object(
                self.node, self.part, self.account, self.container, self.obj,
                headers=headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
            self.assertTrue('x-timestamp' in conn.req_headers)
            self.assertEqual('bar', conn.req_headers.get('x-foo'))

        self.assertIsNone(rv)

    def test_direct_put_container_object_error(self):
        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_put_container_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)

            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue('PUT' in str(raised.exception))

    def test_direct_delete_container_object(self):
        with mocked_http_conn(204) as conn:
            rv = direct_client.direct_delete_container_object(
                self.node, self.part, self.account, self.container, self.obj)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)

        self.assertIsNone(rv)

    def test_direct_delete_container_obj_error(self):
        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_delete_container_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)

            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue('DELETE' in str(raised.exception))

    def test_direct_head_object(self):
        headers = HeaderKeyDict({'x-foo': 'bar'})

        with mocked_http_conn(200, headers) as conn:
            resp = direct_client.direct_head_object(
                self.node, self.part, self.account, self.container,
                self.obj, headers=headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual('bar', conn.req_headers.get('x-foo'))
        self.assertIn('x-timestamp', conn.req_headers)
        self.assertEqual(headers, resp)

    def test_direct_head_object_error(self):
        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_head_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue('HEAD' in str(raised.exception))

    def test_direct_head_object_not_found(self):
        important_timestamp = Timestamp.now().internal
        stub_headers = {'X-Backend-Important-Timestamp': important_timestamp}
        with mocked_http_conn(404, headers=stub_headers) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_head_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(raised.exception.http_status, 404)
        self.assertEqual(
            raised.exception.http_headers['x-backend-important-timestamp'],
            important_timestamp)

    def test_direct_get_object(self):
        contents = six.BytesIO(b'123456')

        with mocked_http_conn(200, body=contents) as conn:
            resp_header, obj_body = direct_client.direct_get_object(
                self.node, self.part, self.account, self.container, self.obj)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(obj_body, contents.getvalue())

    def test_direct_get_object_error(self):
        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_get_object(
                    self.node, self.part,
                    self.account, self.container, self.obj)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue('GET' in str(raised.exception))

    def test_direct_get_object_chunks(self):
        contents = six.BytesIO(b'123456')

        with mocked_http_conn(200, body=contents) as conn:
            resp_header, obj_body = direct_client.direct_get_object(
                self.node, self.part, self.account, self.container, self.obj,
                resp_chunk_size=2)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual('GET', conn.method)
            self.assertEqual(self.obj_path, conn.path)
            self.assertEqual([b'12', b'34', b'56'], list(obj_body))

    def test_direct_post_object(self):
        headers = {'Key': 'value'}

        resp_headers = []

        with mocked_http_conn(200, resp_headers) as conn:
            direct_client.direct_post_object(
                self.node, self.part, self.account, self.container, self.obj,
                headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'POST')
            self.assertEqual(conn.path, self.obj_path)

        for header in headers:
            self.assertEqual(conn.req_headers[header], headers[header])

    def test_direct_post_object_error(self):
        headers = {'Key': 'value'}

        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_post_object(
                    self.node, self.part, self.account, self.container,
                    self.obj, headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'POST')
            self.assertEqual(conn.path, self.obj_path)
            for header in headers:
                self.assertEqual(conn.req_headers[header], headers[header])
            self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
            self.assertTrue('x-timestamp' in conn.req_headers)

        self.assertEqual(raised.exception.http_status, 500)
        self.assertTrue('POST' in str(raised.exception))

    def test_direct_delete_object(self):
        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_delete_object(
                self.node, self.part, self.account, self.container, self.obj)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)
        self.assertIsNone(resp)

    def test_direct_delete_object_with_timestamp(self):
        # ensure timestamp is different from any that might be auto-generated
        timestamp = Timestamp(time.time() - 100)
        headers = {'X-Timestamp': timestamp.internal}
        with mocked_http_conn(200) as conn:
            direct_client.direct_delete_object(
                self.node, self.part, self.account, self.container, self.obj,
                headers=headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)
            self.assertTrue('X-Timestamp' in conn.req_headers)
            self.assertEqual(timestamp, conn.req_headers['X-Timestamp'])

    def test_direct_delete_object_error(self):
        with mocked_http_conn(503) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_delete_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(raised.exception.http_status, 503)
        self.assertTrue('DELETE' in str(raised.exception))

    def test_direct_get_suffix_hashes(self):
        data = {'a83': 'c130a2c17ed45102aada0f4eee69494ff'}
        body = pickle.dumps(data)
        with mocked_http_conn(200, {}, body) as conn:
            resp = direct_client.direct_get_suffix_hashes(self.node,
                                                          self.part, ['a83'])
            self.assertEqual(conn.method, 'REPLICATE')
            self.assertEqual(conn.path, '/sda/0/a83')
            self.assertEqual(conn.host, self.node['replication_ip'])
            self.assertEqual(conn.port, self.node['replication_port'])
            self.assertEqual(data, resp)

    def _test_direct_get_suffix_hashes_fail(self, status_code):
        with mocked_http_conn(status_code):
            with self.assertRaises(DirectClientException) as cm:
                direct_client.direct_get_suffix_hashes(
                    self.node, self.part, ['a83', 'b52'])
        self.assertIn('REPLICATE', cm.exception.args[0])
        self.assertIn(quote('/%s/%s/a83-b52'
                            % (self.node['device'], self.part)),
                      cm.exception.args[0])
        self.assertIn(self.node['replication_ip'], cm.exception.args[0])
        self.assertIn(self.node['replication_port'], cm.exception.args[0])
        self.assertEqual(self.node['replication_ip'], cm.exception.http_host)
        self.assertEqual(self.node['replication_port'], cm.exception.http_port)
        self.assertEqual(self.node['device'], cm.exception.http_device)
        self.assertEqual(status_code, cm.exception.http_status)

    def test_direct_get_suffix_hashes_503(self):
        self._test_direct_get_suffix_hashes_fail(503)

    def test_direct_get_suffix_hashes_507(self):
        self._test_direct_get_suffix_hashes_fail(507)

    def test_direct_put_object_with_content_length(self):
        contents = six.BytesIO(b'123456')

        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_put_object(
                self.node, self.part, self.account, self.container, self.obj,
                contents, 6)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(md5(b'123456').hexdigest(), resp)

    def test_direct_put_object_fail(self):
        contents = six.BytesIO(b'123456')

        with mocked_http_conn(500) as conn:
            with self.assertRaises(ClientException) as raised:
                direct_client.direct_put_object(
                    self.node, self.part, self.account, self.container,
                    self.obj, contents)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(raised.exception.http_status, 500)

    def test_direct_put_object_chunked(self):
        contents = six.BytesIO(b'123456')

        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_put_object(
                self.node, self.part, self.account, self.container, self.obj,
                contents)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(md5(b'6\r\n123456\r\n0\r\n\r\n').hexdigest(), resp)

    def test_direct_put_object_args(self):
        # One test to cover all missing checks
        contents = ""
        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_put_object(
                self.node, self.part, self.account, self.container, self.obj,
                contents, etag="testing-etag", content_type='Text')
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual('PUT', conn.method)
            self.assertEqual(self.obj_path, conn.path)
            self.assertEqual(conn.req_headers['Content-Length'], '0')
            self.assertEqual(conn.req_headers['Content-Type'], 'Text')
        self.assertEqual(md5(b'0\r\n\r\n').hexdigest(), resp)

    def test_direct_put_object_header_content_length(self):
        contents = six.BytesIO(b'123456')
        stub_headers = HeaderKeyDict({
            'Content-Length': '6'})

        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_put_object(
                self.node, self.part, self.account, self.container, self.obj,
                contents, headers=stub_headers)
            self.assertEqual(conn.host, self.node['ip'])
            self.assertEqual(conn.port, self.node['port'])
            self.assertEqual('PUT', conn.method)
            self.assertEqual(conn.req_headers['Content-length'], '6')
        self.assertEqual(md5(b'123456').hexdigest(), resp)

    def test_retry(self):
        headers = HeaderKeyDict({'key': 'value'})

        with mocked_http_conn(200, headers) as conn:
            attempts, resp = direct_client.retry(
                direct_client.direct_head_object, self.node, self.part,
                self.account, self.container, self.obj)
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(headers, resp)
        self.assertEqual(attempts, 1)

    def test_retry_client_exception(self):
        logger = debug_logger('direct-client-test')

        with mock.patch('swift.common.direct_client.sleep') as mock_sleep, \
                mocked_http_conn(500) as conn:
            with self.assertRaises(direct_client.ClientException) as err_ctx:
                direct_client.retry(direct_client.direct_delete_object,
                                    self.node, self.part,
                                    self.account, self.container, self.obj,
                                    retries=2, error_log=logger.error)
        self.assertEqual('DELETE', conn.method)
        self.assertEqual(err_ctx.exception.http_status, 500)
        self.assertIn('DELETE', err_ctx.exception.args[0])
        self.assertIn(self.obj_path,
                      err_ctx.exception.args[0])
        self.assertIn(self.node['ip'], err_ctx.exception.args[0])
        self.assertIn(self.node['port'], err_ctx.exception.args[0])
        self.assertEqual(self.node['ip'], err_ctx.exception.http_host)
        self.assertEqual(self.node['port'], err_ctx.exception.http_port)
        self.assertEqual(self.node['device'], err_ctx.exception.http_device)
        self.assertEqual(500, err_ctx.exception.http_status)
        self.assertEqual([mock.call(1), mock.call(2)],
                         mock_sleep.call_args_list)
        error_lines = logger.get_lines_for_level('error')
        self.assertEqual(3, len(error_lines))
        for line in error_lines:
            self.assertIn('500 Internal Error', line)

    def test_retry_http_exception(self):
        logger = debug_logger('direct-client-test')

        with mock.patch('swift.common.direct_client.sleep') as mock_sleep, \
                mocked_http_conn(HTTPException('Kaboom!')) as conn:
            with self.assertRaises(HTTPException) as err_ctx:
                direct_client.retry(direct_client.direct_delete_object,
                                    self.node, self.part,
                                    self.account, self.container, self.obj,
                                    retries=2, error_log=logger.error)
        self.assertEqual('DELETE', conn.method)
        self.assertEqual('Kaboom!', str(err_ctx.exception))
        self.assertEqual([mock.call(1), mock.call(2)],
                         mock_sleep.call_args_list)
        error_lines = logger.get_lines_for_level('error')
        self.assertEqual(3, len(error_lines))
        for line in error_lines:
            self.assertIn('Kaboom!', line)


class TestUTF8DirectClient(TestDirectClient):

    def setUp(self):
        super(TestUTF8DirectClient, self).setUp()
        self.account = self.account.encode('utf-8')
        self.container = self.container.encode('utf-8')
        self.obj = self.obj.encode('utf-8')

if __name__ == '__main__':
    unittest.main()
