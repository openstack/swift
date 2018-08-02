# -*- coding: utf-8 -*-
#  Copyright (c) 2010-2012 OpenStack Foundation
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
import mock

import unittest

import socket

from eventlet import spawn, Timeout

from swift.common import bufferedhttp

from test import listen_zero


class MockHTTPSConnection(object):

    def __init__(self, hostport):
        pass

    def putrequest(self, method, path, skip_host=0):
        self.path = path
        pass

    def putheader(self, header, *values):
        # Verify that path and values can be safely joined
        # Essentially what Python 2.7 does that caused us problems.
        '\r\n\t'.join((self.path,) + values)

    def endheaders(self):
        pass


class TestBufferedHTTP(unittest.TestCase):

    def test_http_connect(self):
        bindsock = listen_zero()

        def accept(expected_par):
            try:
                with Timeout(3):
                    sock, addr = bindsock.accept()
                    fp = sock.makefile()
                    fp.write('HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n'
                             'RESPONSE')
                    fp.flush()
                    self.assertEqual(
                        fp.readline(),
                        'PUT /dev/%s/path/..%%25/?omg&no=%%7f HTTP/1.1\r\n' %
                        expected_par)
                    headers = {}
                    line = fp.readline()
                    while line and line != '\r\n':
                        headers[line.split(':')[0].lower()] = \
                            line.split(':')[1].strip()
                        line = fp.readline()
                    self.assertEqual(headers['content-length'], '7')
                    self.assertEqual(headers['x-header'], 'value')
                    self.assertEqual(fp.readline(), 'REQUEST\r\n')
            except BaseException as err:
                return err
            return None
        for par in ('par', 1357):
            event = spawn(accept, par)
            try:
                with Timeout(3):
                    conn = bufferedhttp.http_connect(
                        '127.0.0.1', bindsock.getsockname()[1], 'dev', par,
                        'PUT', '/path/..%/', {
                            'content-length': 7,
                            'x-header': 'value'},
                        query_string='omg&no=%7f')
                    conn.send('REQUEST\r\n')
                    self.assertTrue(conn.sock.getsockopt(socket.IPPROTO_TCP,
                                                         socket.TCP_NODELAY))
                    resp = conn.getresponse()
                    body = resp.read()
                    conn.close()
                    self.assertEqual(resp.status, 200)
                    self.assertEqual(resp.reason, 'OK')
                    self.assertEqual(body, 'RESPONSE')
            finally:
                err = event.wait()
                if err:
                    raise Exception(err)

    def test_nonstr_header_values(self):
        with mock.patch('swift.common.bufferedhttp.HTTPSConnection',
                        MockHTTPSConnection):
            bufferedhttp.http_connect(
                '127.0.0.1', 8080, 'sda', 1, 'GET', '/',
                headers={'x-one': '1', 'x-two': 2, 'x-three': 3.0,
                         'x-four': {'crazy': 'value'}}, ssl=True)
            bufferedhttp.http_connect_raw(
                '127.0.0.1', 8080, 'GET', '/',
                headers={'x-one': '1', 'x-two': 2, 'x-three': 3.0,
                         'x-four': {'crazy': 'value'}}, ssl=True)

    def test_unicode_values(self):
        with mock.patch('swift.common.bufferedhttp.HTTPSConnection',
                        MockHTTPSConnection):
            for dev in ('sda', u'sda', u'sdá', u'sdá'.encode('utf-8')):
                for path in (
                        '/v1/a', u'/v1/a', u'/v1/á', u'/v1/á'.encode('utf-8')):
                    for header in ('abc', u'abc', u'ábc'.encode('utf-8')):
                        try:
                            bufferedhttp.http_connect(
                                '127.0.0.1', 8080, dev, 1, 'GET', path,
                                headers={'X-Container-Meta-Whatever': header},
                                ssl=True)
                        except Exception as e:
                            self.fail(
                                'Exception %r for device=%r path=%r header=%r'
                                % (e, dev, path, header))


if __name__ == '__main__':
    unittest.main()
