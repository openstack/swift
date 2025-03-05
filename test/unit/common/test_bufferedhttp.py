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
import io
from http.client import parse_headers

from unittest import mock
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
                    fp = sock.makefile('rwb')
                    fp.write(b'HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n'
                             b'RESPONSE')
                    fp.flush()
                    line = fp.readline()
                    path = (b'/dev/' + expected_par +
                            b'/path/..%25/?omg=&no=%7F&%FF=%FF&no=%25ff')
                    self.assertEqual(
                        line,
                        b'PUT ' + path + b' HTTP/1.1\r\n')
                    headers = {}
                    line = fp.readline()
                    while line and line != b'\r\n':
                        headers[line.split(b':')[0].lower()] = \
                            line.split(b':')[1].strip()
                        line = fp.readline()
                    self.assertEqual(headers[b'content-length'], b'7')
                    self.assertEqual(headers[b'x-header'], b'value')
                    self.assertEqual(fp.readline(), b'REQUEST\r\n')
            except BaseException as err:
                return err
            return None
        for spawn_par, par in (
                (b'par', b'par'), (b'up%C3%A5r', u'up\xe5r'),
                (b'%C3%BCpar', b'\xc3\xbcpar'), (b'1357', 1357)):
            event = spawn(accept, spawn_par)
            try:
                with Timeout(3):
                    conn = bufferedhttp.http_connect(
                        '127.0.0.1', bindsock.getsockname()[1], 'dev', par,
                        'PUT', '/path/..%/', {
                            'content-length': 7,
                            'x-header': 'value'},
                        query_string='omg&no=%7f&\xff=%ff&no=%25ff')
                    conn.send(b'REQUEST\r\n')
                    self.assertTrue(conn.sock.getsockopt(socket.IPPROTO_TCP,
                                                         socket.TCP_NODELAY))
                    resp = conn.getresponse()
                    body = resp.read()
                    conn.close()
                    self.assertEqual(resp.status, 200)
                    self.assertEqual(resp.reason, 'OK')
                    self.assertEqual(body, b'RESPONSE')
            finally:
                err = event.wait()
                if err:
                    raise Exception(err)

    def test_get_expect(self):
        bindsock = listen_zero()
        request = []

        def accept():
            with Timeout(3):
                sock, addr = bindsock.accept()
                fp = sock.makefile('rwb')
                request.append(fp.readline())
                fp.write(b'HTTP/1.1 100 Continue\r\n\r\n')
                fp.flush()
                fp.write(b'HTTP/1.1 200 OK\r\nContent-Length: 8\r\n\r\n'
                         b'RESPONSE')
                fp.flush()

        server = spawn(accept)
        try:
            address = '%s:%s' % ('127.0.0.1', bindsock.getsockname()[1])
            conn = bufferedhttp.BufferedHTTPConnection(address)
            conn.putrequest('GET', '/path')
            conn.endheaders()
            resp = conn.getexpect()
            self.assertIsInstance(resp, bufferedhttp.BufferedHTTPResponse)
            self.assertEqual(resp.status, 100)
            self.assertEqual(resp.version, 11)
            self.assertEqual(resp.reason, 'Continue')
            # I don't think you're supposed to "read" a continue response
            self.assertRaises(AssertionError, resp.read)

            resp = conn.getresponse()
            self.assertIsInstance(resp, bufferedhttp.BufferedHTTPResponse)
            self.assertEqual(resp.read(), b'RESPONSE')

        finally:
            server.wait()
        self.assertEqual(request[0], b'GET /path HTTP/1.1\r\n')

    def test_get_with_non_ascii(self):
        bindsock = listen_zero()
        request = []

        def accept():
            with Timeout(3):
                sock, addr = bindsock.accept()
                fp = sock.makefile('rwb')
                request.append(fp.readline())
                # Ignore request headers
                while fp.readline() != b'\r\n':
                    pass
                fp.write(b'HTTP/1.1 100 Continue\r\n\r\n')
                fp.flush()
                fp.write(b'\r\n'.join([
                    b'HTTP/1.1 200 OK',
                    b'X-Non-Ascii-M\xc3\xa9ta: \xe1\x88\xb4',
                    b'Content-Length: 8',
                    b'',
                    b'RESPONSE']))
                fp.flush()
                # Server can look for pipelined requests
                request.append(fp.readline())

        server = spawn(accept)
        try:
            address = '%s:%s' % ('127.0.0.1', bindsock.getsockname()[1])
            conn = bufferedhttp.BufferedHTTPConnection(address)
            conn.putrequest('GET', '/path')
            conn.endheaders()
            resp = conn.getexpect()
            self.assertIsInstance(resp, bufferedhttp.BufferedHTTPResponse)
            self.assertEqual(resp.status, 100)
            self.assertEqual(resp.version, 11)
            self.assertEqual(resp.reason, 'Continue')
            # I don't think you're supposed to "read" a continue response
            self.assertRaises(AssertionError, resp.read)

            resp = conn.getresponse()
            self.assertIsInstance(resp, bufferedhttp.BufferedHTTPResponse)
            self.assertEqual(resp.length, 8)
            self.assertEqual(resp.read(), b'RESPONSE')
            self.assertEqual(resp.read(), b'')
            self.assertEqual(resp.headers['X-Non-Ascii-M\xc3\xa9ta'],
                             '\xe1\x88\xb4')
            # it's all HTTP/1.1 so we *could* pipeline, but we won't
            conn.close()
        finally:
            server.wait()
        self.assertEqual(request, [b'GET /path HTTP/1.1\r\n', b''])

    def test_closed_response(self):
        resp = bufferedhttp.BufferedHTTPResponse(None)
        self.assertEqual(resp.status, 'UNKNOWN')
        self.assertEqual(resp.version, 'UNKNOWN')
        self.assertEqual(resp.reason, 'UNKNOWN')
        self.assertEqual(resp.read(), b'')

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

    def test_headers_setter_with_dict(self):
        resp = bufferedhttp.BufferedHTTPResponse(None)
        resp.headers = {'a': 'b', 'c': 'd'}
        self.assertEqual('b', resp.headers.get('a'))
        self.assertEqual('d', resp.headers.get('c'))
        resp.headers = {'a': 'b', 'c': 'd'}
        self.assertEqual('b', resp.headers.get('a'))
        self.assertEqual('d', resp.headers.get('c'))
        # XXX: AttributeError: 'dict' object has no attribute 'get_all'
        # self.assertEqual(['b'], resp.headers.get_all('a'))

    def test_headers_setter_with_message(self):
        msg = parse_headers(io.BytesIO(b'a: b\na: bb\nc: d\n\n'))
        self.assertEqual('', msg.get_payload())
        resp = bufferedhttp.BufferedHTTPResponse(None)
        resp.headers = msg
        self.assertEqual('b', resp.headers.get('a'))
        self.assertEqual(['b', 'bb'], resp.headers.get_all('a'))
        self.assertEqual('d', resp.headers.get('c'))
        self.assertEqual([('a', 'b'), ('a', 'bb'), ('c', 'd')],
                         resp.headers.items())
        resp.headers = msg
        self.assertEqual([('a', 'b'), ('a', 'bb'), ('c', 'd')],
                         resp.headers.items())

    def test_headers_setter_with_message_with_payload(self):
        msg = parse_headers(io.BytesIO(b'\xc3: b\n\xc3: bb\nc: d\n\n'))
        self.assertEqual('Ã: b\nÃ: bb\nc: d\n\n', msg.get_payload())
        resp = bufferedhttp.BufferedHTTPResponse(None)
        resp.headers = resp.msg = msg
        self.assertEqual('b', resp.headers.get('\xc3'))
        self.assertEqual(['b', 'bb'], resp.headers.get_all('\xc3'))
        self.assertEqual('d', resp.headers.get('c'))
        self.assertEqual([('\xc3', 'b'), ('\xc3', 'bb'), ('c', 'd')],
                         resp.headers.items())

        resp.headers = msg
        self.assertEqual('b', resp.headers.get('\xc3'))
        self.assertEqual(['b', 'bb'], resp.headers.get_all('\xc3'))
        self.assertEqual('d', resp.headers.get('c'))
        self.assertEqual([('\xc3', 'b'), ('\xc3', 'bb'), ('c', 'd')],
                         resp.headers.items())
        self.assertIs(resp.headers, resp.msg)
        self.assertIs(resp._headers, resp.headers)


if __name__ == '__main__':
    unittest.main()
