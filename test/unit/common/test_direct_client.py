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

import unittest
import os
import urllib
from contextlib import contextmanager
import StringIO
from hashlib import md5
import time

import mock

from swift.common import direct_client
from swift.common.exceptions import ClientException
from swift.common.utils import json, Timestamp
from swift.common.swob import HeaderKeyDict, RESPONSE_REASONS
from swift.common.storage_policy import POLICIES

from test.unit import patch_policies


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
        self.with_exc = False
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
        if self.with_exc:
            raise Exception('test')
        return self

    def getheader(self, header, default=None):
        return self.resp_headers.get(header, default)

    def getheaders(self):
        return self.resp_headers.items()

    def read(self):
        return self.body

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
        self.node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        self.part = '0'

        self.account = u'\u062a account'
        self.container = u'\u062a container'
        self.obj = u'\u062a obj/name'
        self.account_path = '/sda/0/%s' % urllib.quote(
            self.account.encode('utf-8'))
        self.container_path = '/sda/0/%s/%s' % tuple(
            urllib.quote(p.encode('utf-8')) for p in (
                self.account, self.container))
        self.obj_path = '/sda/0/%s/%s/%s' % tuple(
            urllib.quote(p.encode('utf-8')) for p in (
                self.account, self.container, self.obj))
        self.user_agent = 'direct-client %s' % os.getpid()

    def test_gen_headers(self):
        stub_user_agent = 'direct-client %s' % os.getpid()

        headers = direct_client.gen_headers()
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assertEqual(1, len(headers))

        now = time.time()
        headers = direct_client.gen_headers(add_ts=True)
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assert_(now - 1 < Timestamp(headers['x-timestamp']) < now + 1)
        self.assertEqual(headers['x-timestamp'],
                         Timestamp(headers['x-timestamp']).internal)
        self.assertEqual(2, len(headers))

        headers = direct_client.gen_headers(hdrs_in={'foo-bar': '47'})
        self.assertEqual(headers['user-agent'], stub_user_agent)
        self.assertEqual(headers['foo-bar'], '47')
        self.assertEqual(2, len(headers))

        headers = direct_client.gen_headers(hdrs_in={'user-agent': '47'})
        self.assertEqual(headers['user-agent'], stub_user_agent)
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
                    self.assert_(
                        now - 1 < Timestamp(headers['x-timestamp']) < now + 1)
                self.assertEqual(expected_header_count, len(headers))

    def test_direct_get_account(self):
        stub_headers = HeaderKeyDict({
            'X-Account-Container-Count': '1',
            'X-Account-Object-Count': '1',
            'X-Account-Bytes-Used': '1',
            'X-Timestamp': '1234567890',
            'X-PUT-Timestamp': '1234567890'})

        body = '[{"count": 1, "bytes": 20971520, "name": "c1"}]'

        with mocked_http_conn(200, stub_headers, body) as conn:
            resp_headers, resp = direct_client.direct_get_account(
                self.node, self.part, self.account, marker='marker',
                prefix='prefix', delimiter='delimiter', limit=1000)
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.account_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(resp_headers, stub_headers)
        self.assertEqual(json.loads(body), resp)
        self.assertTrue('marker=marker' in conn.query_string)
        self.assertTrue('delimiter=delimiter' in conn.query_string)
        self.assertTrue('limit=1000' in conn.query_string)
        self.assertTrue('prefix=prefix' in conn.query_string)
        self.assertTrue('format=json' in conn.query_string)

    def test_direct_client_exception(self):
        stub_headers = {'X-Trans-Id': 'txb5f59485c578460f8be9e-0053478d09'}
        body = 'a server error has occurred'
        with mocked_http_conn(500, stub_headers, body):
            try:
                direct_client.direct_get_account(self.node, self.part,
                                                 self.account)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
        self.assertEqual(err.http_status, 500)
        expected_err_msg_parts = (
            'Account server %s:%s' % (self.node['ip'], self.node['port']),
            'GET %r' % self.account_path,
            'status 500',
        )
        for item in expected_err_msg_parts:
            self.assert_(item in str(err), '%r was not in "%s"' % (item, err))
        self.assertEqual(err.http_host, self.node['ip'])
        self.assertEqual(err.http_port, self.node['port'])
        self.assertEqual(err.http_device, self.node['device'])
        self.assertEqual(err.http_status, 500)
        self.assertEqual(err.http_reason, 'Internal Error')
        self.assertEqual(err.http_headers, stub_headers)

    def test_direct_get_account_no_content_does_not_parse_body(self):
        headers = {
            'X-Account-Container-Count': '1',
            'X-Account-Object-Count': '1',
            'X-Account-Bytes-Used': '1',
            'X-Timestamp': '1234567890',
            'X-PUT-Timestamp': '1234567890'}
        with mocked_http_conn(204, headers) as conn:
            resp_headers, resp = direct_client.direct_get_account(
                self.node, self.part, self.account)
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.account_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(resp_headers, resp_headers)
        self.assertEqual([], resp)

    def test_direct_get_account_error(self):
        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_get_account(
                    self.node, self.part, self.account)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.account_path)
        self.assertEqual(err.http_status, 500)
        self.assert_('GET' in str(err))

    def test_direct_delete_account(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'

        mock_path = 'swift.common.bufferedhttp.http_connect_raw'
        with mock.patch(mock_path) as fake_connect:
            fake_connect.return_value.getresponse.return_value.status = 200
            direct_client.direct_delete_account(node, part, account)
            args, kwargs = fake_connect.call_args
            method = args[2]
            self.assertEqual('DELETE', method)
            path = args[3]
            self.assertEqual('/sda/0/a', path)
            headers = args[4]
            self.assert_('X-Timestamp' in headers)

    def test_direct_head_container(self):
        headers = HeaderKeyDict(key='value')

        with mocked_http_conn(200, headers) as conn:
            resp = direct_client.direct_head_container(
                self.node, self.part, self.account, self.container)
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(conn.req_headers['user-agent'],
                         self.user_agent)
        self.assertEqual(headers, resp)

    def test_direct_head_container_error(self):
        headers = HeaderKeyDict(key='value')

        with mocked_http_conn(503, headers) as conn:
            try:
                direct_client.direct_head_container(
                    self.node, self.part, self.account, self.container)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            # check request
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(err.http_status, 503)
        self.assertEqual(err.http_headers, headers)
        self.assert_('HEAD' in str(err))

    def test_direct_head_container_deleted(self):
        important_timestamp = Timestamp(time.time()).internal
        headers = HeaderKeyDict({'X-Backend-Important-Timestamp':
                                 important_timestamp})

        with mocked_http_conn(404, headers) as conn:
            try:
                direct_client.direct_head_container(
                    self.node, self.part, self.account, self.container)
            except Exception as err:
                self.assert_(isinstance(err, ClientException))
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual(err.http_status, 404)
        self.assertEqual(err.http_headers, headers)

    def test_direct_get_container(self):
        headers = HeaderKeyDict({'key': 'value'})
        body = '[{"hash": "8f4e3", "last_modified": "317260", "bytes": 209}]'

        with mocked_http_conn(200, headers, body) as conn:
            resp_headers, resp = direct_client.direct_get_container(
                self.node, self.part, self.account, self.container,
                marker='marker', prefix='prefix', delimiter='delimiter',
                limit=1000)

        self.assertEqual(conn.req_headers['user-agent'],
                         'direct-client %s' % os.getpid())
        self.assertEqual(headers, resp_headers)
        self.assertEqual(json.loads(body), resp)
        self.assertTrue('marker=marker' in conn.query_string)
        self.assertTrue('delimiter=delimiter' in conn.query_string)
        self.assertTrue('limit=1000' in conn.query_string)
        self.assertTrue('prefix=prefix' in conn.query_string)
        self.assertTrue('format=json' in conn.query_string)

    def test_direct_get_container_no_content_does_not_decode_body(self):
        headers = {}
        body = ''
        with mocked_http_conn(204, headers, body) as conn:
            resp_headers, resp = direct_client.direct_get_container(
                self.node, self.part, self.account, self.container)

        self.assertEqual(conn.req_headers['user-agent'],
                         'direct-client %s' % os.getpid())
        self.assertEqual(headers, resp_headers)
        self.assertEqual([], resp)

    def test_direct_delete_container(self):
        with mocked_http_conn(200) as conn:
            direct_client.direct_delete_container(
                self.node, self.part, self.account, self.container)
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.container_path)

    def test_direct_delete_container_error(self):
        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_delete_container(
                    self.node, self.part, self.account, self.container)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')

            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.container_path)

        self.assertEqual(err.http_status, 500)
        self.assert_('DELETE' in str(err))

    def test_direct_put_container_object(self):
        headers = {'x-foo': 'bar'}

        with mocked_http_conn(204) as conn:
            rv = direct_client.direct_put_container_object(
                self.node, self.part, self.account, self.container, self.obj,
                headers=headers)
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
            self.assert_('x-timestamp' in conn.req_headers)
            self.assertEqual('bar', conn.req_headers.get('x-foo'))

        self.assertEqual(rv, None)

    def test_direct_put_container_object_error(self):
        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_put_container_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')

            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(err.http_status, 500)
        self.assert_('PUT' in str(err))

    def test_direct_delete_container_object(self):
        with mocked_http_conn(204) as conn:
            rv = direct_client.direct_delete_container_object(
                self.node, self.part, self.account, self.container, self.obj)
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(rv, None)

    def test_direct_delete_container_obj_error(self):
        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_delete_container_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')

            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(err.http_status, 500)
        self.assert_('DELETE' in str(err))

    def test_direct_head_object(self):
        headers = HeaderKeyDict({'x-foo': 'bar'})

        with mocked_http_conn(200, headers) as conn:
            resp = direct_client.direct_head_object(
                self.node, self.part, self.account, self.container,
                self.obj, headers=headers)
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
        self.assertEqual('bar', conn.req_headers.get('x-foo'))
        self.assert_('x-timestamp' not in conn.req_headers,
                     'x-timestamp was in HEAD request headers')
        self.assertEqual(headers, resp)

    def test_direct_head_object_error(self):
        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_head_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(err.http_status, 500)
        self.assert_('HEAD' in str(err))

    def test_direct_head_object_not_found(self):
        important_timestamp = Timestamp(time.time()).internal
        stub_headers = {'X-Backend-Important-Timestamp': important_timestamp}
        with mocked_http_conn(404, headers=stub_headers) as conn:
            try:
                direct_client.direct_head_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'HEAD')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(err.http_status, 404)
        self.assertEqual(err.http_headers['x-backend-important-timestamp'],
                         important_timestamp)

    def test_direct_get_object(self):
        contents = StringIO.StringIO('123456')

        with mocked_http_conn(200, body=contents) as conn:
            resp_header, obj_body = direct_client.direct_get_object(
                self.node, self.part, self.account, self.container, self.obj)
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(obj_body, contents)

    def test_direct_get_object_error(self):
        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_get_object(
                    self.node, self.part,
                    self.account, self.container, self.obj)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'GET')
            self.assertEqual(conn.path, self.obj_path)

        self.assertEqual(err.http_status, 500)
        self.assert_('GET' in str(err))

    def test_direct_post_object(self):
        headers = {'Key': 'value'}

        resp_headers = []

        with mocked_http_conn(200, resp_headers) as conn:
            direct_client.direct_post_object(
                self.node, self.part, self.account, self.container, self.obj,
                headers)
            self.assertEqual(conn.method, 'POST')
            self.assertEqual(conn.path, self.obj_path)

        for header in headers:
            self.assertEqual(conn.req_headers[header], headers[header])

    def test_direct_post_object_error(self):
        headers = {'Key': 'value'}

        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_post_object(
                    self.node, self.part, self.account, self.container,
                    self.obj, headers)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'POST')
            self.assertEqual(conn.path, self.obj_path)
            for header in headers:
                self.assertEqual(conn.req_headers[header], headers[header])
            self.assertEqual(conn.req_headers['user-agent'], self.user_agent)
            self.assert_('x-timestamp' in conn.req_headers)

        self.assertEqual(err.http_status, 500)
        self.assert_('POST' in str(err))

    def test_direct_delete_object(self):
        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_delete_object(
                self.node, self.part, self.account, self.container, self.obj)
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(resp, None)

    def test_direct_delete_object_error(self):
        with mocked_http_conn(503) as conn:
            try:
                direct_client.direct_delete_object(
                    self.node, self.part, self.account, self.container,
                    self.obj)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'DELETE')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(err.http_status, 503)
        self.assert_('DELETE' in str(err))

    def test_direct_put_object_with_content_length(self):
        contents = StringIO.StringIO('123456')

        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_put_object(
                self.node, self.part, self.account, self.container, self.obj,
                contents, 6)
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(md5('123456').hexdigest(), resp)

    def test_direct_put_object_fail(self):
        contents = StringIO.StringIO('123456')

        with mocked_http_conn(500) as conn:
            try:
                direct_client.direct_put_object(
                    self.node, self.part, self.account, self.container,
                    self.obj, contents)
            except ClientException as err:
                pass
            else:
                self.fail('ClientException not raised')
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(err.http_status, 500)

    def test_direct_put_object_chunked(self):
        contents = StringIO.StringIO('123456')

        with mocked_http_conn(200) as conn:
            resp = direct_client.direct_put_object(
                self.node, self.part, self.account, self.container, self.obj,
                contents)
            self.assertEqual(conn.method, 'PUT')
            self.assertEqual(conn.path, self.obj_path)
        self.assertEqual(md5('6\r\n123456\r\n0\r\n\r\n').hexdigest(), resp)

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


if __name__ == '__main__':
    unittest.main()
