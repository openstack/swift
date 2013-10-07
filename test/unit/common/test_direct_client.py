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

import StringIO
from hashlib import md5

from swift.common import direct_client
from swiftclient import json_loads


def mock_http_connect(status, fake_headers=None, body=None):

    class FakeConn(object):

        def __init__(self, status, fake_headers, body, *args, **kwargs):
            self.status = status
            self.reason = 'Fake'
            self.body = body
            self.host = args[0]
            self.port = args[1]
            self.method = args[4]
            self.path = args[5]
            self.with_exc = False
            self.headers = kwargs.get('headers', {})
            self.fake_headers = fake_headers
            self.etag = md5()

        def getresponse(self):
            if self.with_exc:
                raise Exception('test')

            if self.fake_headers is not None and self.method == 'POST':
                self.fake_headers.append(self.headers)
            return self

        def getheader(self, header, default=None):
            return self.headers.get(header.lower(), default)

        def getheaders(self):
            if self.fake_headers is not None:
                for key in self.fake_headers:
                    self.headers.update({key: self.fake_headers[key]})
            return self.headers.items()

        def read(self):
            return self.body

        def send(self, data):
            self.etag.update(data)
            self.headers['etag'] = str(self.etag.hexdigest())

        def close(self):
            return
    return lambda *args, **kwargs: FakeConn(status, fake_headers, body,
                                            *args, **kwargs)


class TestDirectClient(unittest.TestCase):

    def test_gen_headers(self):
        hdrs = direct_client.gen_headers()
        assert 'user-agent' in hdrs
        assert hdrs['user-agent'] == 'direct-client %s' % os.getpid()
        assert len(hdrs.keys()) == 1

        hdrs = direct_client.gen_headers(add_ts=True)
        assert 'user-agent' in hdrs
        assert 'x-timestamp' in hdrs
        assert len(hdrs.keys()) == 2

        hdrs = direct_client.gen_headers(hdrs_in={'foo-bar': '47'})
        assert 'user-agent' in hdrs
        assert 'foo-bar' in hdrs
        assert hdrs['foo-bar'] == '47'
        assert len(hdrs.keys()) == 2

        hdrs = direct_client.gen_headers(hdrs_in={'user-agent': '47'})
        assert 'user-agent' in hdrs
        assert hdrs['user-agent'] == 'direct-client %s' % os.getpid()
        assert len(hdrs.keys()) == 1

    def test_direct_get_account(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        headers = {
            'X-Account-Container-Count': '1',
            'X-Account-Object-Count': '1',
            'X-Account-Bytes-Used': '1',
            'X-Timestamp': '1234567890',
            'X-PUT-Timestamp': '1234567890'}

        body = '[{"count": 1, "bytes": 20971520, "name": "c1"}]'

        fake_headers = {}
        for header, value in headers.items():
            fake_headers[header.lower()] = value

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200, fake_headers, body)

        resp_headers, resp = direct_client.direct_get_account(node, part,
                                                              account)

        fake_headers.update({'user-agent': 'direct-client %s' % os.getpid()})
        self.assertEqual(fake_headers, resp_headers)
        self.assertEqual(json_loads(body), resp)

        direct_client.http_connect = mock_http_connect(204, fake_headers, body)

        resp_headers, resp = direct_client.direct_get_account(node, part,
                                                              account)

        fake_headers.update({'user-agent': 'direct-client %s' % os.getpid()})
        self.assertEqual(fake_headers, resp_headers)
        self.assertEqual([], resp)

        direct_client.http_connect = was_http_connector

    def test_direct_head_container(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        headers = {'key': 'value'}

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200, headers)

        resp = direct_client.direct_head_container(node, part, account,
                                                   container)

        headers.update({'user-agent': 'direct-client %s' % os.getpid()})
        self.assertEqual(headers, resp)

        direct_client.http_connect = was_http_connector

    def test_direct_get_container(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        headers = {'key': 'value'}
        body = '[{"hash": "8f4e3", "last_modified": "317260", "bytes": 209}]'

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200, headers, body)

        resp_headers, resp = (
            direct_client.direct_get_container(node, part, account, container))

        headers.update({'user-agent': 'direct-client %s' % os.getpid()})
        self.assertEqual(headers, resp_headers)
        self.assertEqual(json_loads(body), resp)

        direct_client.http_connect = mock_http_connect(204, headers, body)

        resp_headers, resp = (
            direct_client.direct_get_container(node, part, account, container))

        headers.update({'user-agent': 'direct-client %s' % os.getpid()})
        self.assertEqual(headers, resp_headers)
        self.assertEqual([], resp)

        direct_client.http_connect = was_http_connector

    def test_direct_delete_container(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200)

        direct_client.direct_delete_container(node, part, account, container)

        direct_client.http_connect = was_http_connector

    def test_direct_head_object(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'
        headers = {'key': 'value'}

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200, headers)

        resp = direct_client.direct_head_object(node, part, account,
                                                container, name)
        headers.update({'user-agent': 'direct-client %s' % os.getpid()})
        self.assertEqual(headers, resp)

        direct_client.http_connect = was_http_connector

    def test_direct_get_object(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'
        contents = StringIO.StringIO('123456')

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200, body=contents)

        resp_header, obj_body = (
            direct_client.direct_get_object(node, part, account, container,
                                            name))
        self.assertEqual(obj_body, contents)

        direct_client.http_connect = was_http_connector

        pass

    def test_direct_post_object(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'
        headers = {'Key': 'value'}

        fake_headers = []

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200, fake_headers)

        direct_client.direct_post_object(node, part, account,
                                         container, name, headers)
        self.assertEqual(headers['Key'], fake_headers[0].get('Key'))

        direct_client.http_connect = was_http_connector

    def test_direct_delete_object(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200)

        direct_client.direct_delete_object(node, part, account, container,
                                           name)

        direct_client.http_connect = was_http_connector

    def test_direct_put_object(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'
        contents = StringIO.StringIO('123456')

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200)

        resp = direct_client.direct_put_object(node, part, account,
                                               container, name, contents, 6)
        self.assertEqual(md5('123456').hexdigest(), resp)

        direct_client.http_connect = was_http_connector

    def test_direct_put_object_fail(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'
        contents = StringIO.StringIO('123456')

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(500)

        self.assertRaises(direct_client.ClientException,
                          direct_client.direct_put_object, node, part, account,
                          container, name, contents)

        direct_client.http_connect = was_http_connector

    def test_direct_put_object_chunked(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'
        contents = StringIO.StringIO('123456')

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200)

        resp = direct_client.direct_put_object(node, part, account,
                                               container, name, contents)
        self.assertEqual(md5('6\r\n123456\r\n0\r\n\r\n').hexdigest(), resp)

        direct_client.http_connect = was_http_connector

    def test_retry(self):
        node = {'ip': '1.2.3.4', 'port': '6000', 'device': 'sda'}
        part = '0'
        account = 'a'
        container = 'c'
        name = 'o'
        headers = {'key': 'value'}

        was_http_connector = direct_client.http_connect
        direct_client.http_connect = mock_http_connect(200, headers)

        attempts, resp = direct_client.retry(direct_client.direct_head_object,
                                             node, part, account, container,
                                             name)
        headers.update({'user-agent': 'direct-client %s' % os.getpid()})
        self.assertEqual(headers, resp)
        self.assertEqual(attempts, 1)

        direct_client.http_connect = was_http_connector

if __name__ == '__main__':
    unittest.main()
