# Copyright (c) 2010-2022 OpenStack Foundation
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

from argparse import Namespace
from io import BytesIO
import json
from unittest import mock
import types
import unittest
import eventlet.wsgi as wsgi

from test.debug_logger import debug_logger
from swift.common import http_protocol, swob


class TestSwiftHttpProtocol(unittest.TestCase):
    def _proto_obj(self):
        # Make an object we can exercise... note the base class's __init__()
        # does a bunch of work, so we just new up an object like eventlet.wsgi
        # does.
        proto_class = http_protocol.SwiftHttpProtocol
        try:
            the_obj = types.InstanceType(proto_class)
        except AttributeError:
            the_obj = proto_class.__new__(proto_class)
        # Install some convenience mocks
        the_obj.server = Namespace(app=Namespace(logger=mock.Mock()),
                                   url_length_limit=777,
                                   log=mock.Mock())
        the_obj.send_error = mock.Mock()

        return the_obj

    def test_swift_http_protocol_log_request(self):
        proto_obj = self._proto_obj()
        self.assertEqual(None, proto_obj.log_request('ignored'))

    def test_swift_http_protocol_log_message(self):
        proto_obj = self._proto_obj()

        proto_obj.log_message('a%sc', 'b')
        self.assertEqual([mock.call.error('ERROR WSGI: a%sc', 'b')],
                         proto_obj.server.app.logger.mock_calls)

    def test_swift_http_protocol_log_message_no_logger(self):
        # If the app somehow had no logger attribute or it was None, don't blow
        # up
        proto_obj = self._proto_obj()
        delattr(proto_obj.server.app, 'logger')

        proto_obj.log_message('a%sc', 'b')
        self.assertEqual([mock.call.info('ERROR WSGI: a%sc', 'b')],
                         proto_obj.server.log.mock_calls)

        proto_obj.server.log.reset_mock()
        proto_obj.server.app.logger = None

        proto_obj.log_message('a%sc', 'b')
        self.assertEqual([mock.call.info('ERROR WSGI: a%sc', 'b')],
                         proto_obj.server.log.mock_calls)

    def test_swift_http_protocol_parse_request_no_proxy(self):
        proto_obj = self._proto_obj()
        proto_obj.raw_requestline = b'jimmy jam'
        proto_obj.client_address = ('a', '123')

        self.assertEqual(False, proto_obj.parse_request())

        self.assertEqual([
            mock.call(400, "Bad HTTP/0.9 request type ('jimmy')"),
        ], proto_obj.send_error.mock_calls)
        self.assertEqual(('a', '123'), proto_obj.client_address)

    def test_bad_request_line(self):
        proto_obj = self._proto_obj()
        proto_obj.raw_requestline = b'None //'
        self.assertEqual(False, proto_obj.parse_request())


class ProtocolTest(unittest.TestCase):
    def _run_bytes_through_protocol(self, bytes_from_client, app=None):
        rfile = BytesIO(bytes_from_client)
        wfile = BytesIO()

        # All this fakery is needed to make the WSGI server process one
        # connection, possibly with multiple requests, in the main
        # greenthread. It doesn't hurt correctness if the function is called
        # in a separate greenthread, but it makes using the debugger harder.
        class FakeGreenthread(object):
            def link(self, a_callable, *args):
                a_callable(self, *args)

        class FakePool(object):
            def spawn(self, a_callable, *args, **kwargs):
                a_callable(*args, **kwargs)
                return FakeGreenthread()

            def spawn_n(self, a_callable, *args, **kwargs):
                a_callable(*args, **kwargs)

            def waitall(self):
                pass

        addr = ('127.0.0.1', 8359)
        fake_tcp_socket = mock.Mock(
            setsockopt=lambda *a: None,
            makefile=lambda mode, bufsize: rfile if 'r' in mode else wfile,
            getsockname=lambda *a: addr
        )
        fake_listen_socket = mock.Mock(
            accept=mock.MagicMock(
                side_effect=[[fake_tcp_socket, addr],
                             # KeyboardInterrupt breaks the WSGI server out of
                             # its infinite accept-process-close loop.
                             KeyboardInterrupt]),
            getsockname=lambda *a: addr)
        del fake_listen_socket.do_handshake

        # If we let the WSGI server close rfile/wfile then we can't access
        # their contents any more.
        self.logger = debug_logger('proxy')
        with mock.patch.object(wfile, 'close', lambda: None), \
                mock.patch.object(rfile, 'close', lambda: None):
            wsgi.server(
                fake_listen_socket, app or self.app,
                protocol=self.protocol_class,
                custom_pool=FakePool(),
                log=self.logger,
                log_output=True,
            )
        return wfile.getvalue()


class TestSwiftHttpProtocolSomeMore(ProtocolTest):
    protocol_class = http_protocol.SwiftHttpProtocol

    @staticmethod
    def app(env, start_response):
        start_response("200 OK", [])
        return [swob.wsgi_to_bytes(env['RAW_PATH_INFO'])]

    def test_simple(self):
        bytes_out = self._run_bytes_through_protocol(
            b"GET /someurl HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        )

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check
        self.assertEqual(lines[-1], b'/someurl')

    def test_quoted(self):
        bytes_out = self._run_bytes_through_protocol(
            b"GET /some%fFpath%D8%AA HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        )

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check
        self.assertEqual(lines[-1], b'/some%fFpath%D8%AA')

    def test_messy(self):
        bytes_out = self._run_bytes_through_protocol(
            b"GET /oh\xffboy%what$now%E2%80%bd HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        )

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[-1], b'/oh\xffboy%what$now%E2%80%bd')

    def test_absolute_target(self):
        bytes_out = self._run_bytes_through_protocol((
            b"GET https://cluster.domain/bucket/key HTTP/1.0\r\n"
            b"\r\n"
        ))

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[-1], b'/bucket/key')

        bytes_out = self._run_bytes_through_protocol((
            b"GET http://cluster.domain/v1/acct/cont/obj HTTP/1.0\r\n"
            b"\r\n"
        ))

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[-1], b'/v1/acct/cont/obj')

        # clients talking nonsense
        bytes_out = self._run_bytes_through_protocol((
            b"GET ftp://cluster.domain/bucket/key HTTP/1.0\r\n"
            b"\r\n"
        ))

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[-1], b'ftp://cluster.domain/bucket/key')

        bytes_out = self._run_bytes_through_protocol((
            b"GET https://cluster.domain HTTP/1.0\r\n"
            b"\r\n"
        ))

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[-1], b'https://cluster.domain')

        bytes_out = self._run_bytes_through_protocol((
            b"GET http:omg//wtf/bbq HTTP/1.0\r\n"
            b"\r\n"
        ))

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[-1], b'http:omg//wtf/bbq')

    def test_bad_request(self):
        bytes_out = self._run_bytes_through_protocol((
            b"ONLY-METHOD\r\n"
            b"Server: example.com\r\n"
            b"\r\n"
        ))
        lines = [l for l in bytes_out.split(b"\r\n") if l]
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(
            lines[0], b"HTTP/1.1 400 Bad request syntax ('ONLY-METHOD')")
        self.assertIn(b"Bad request syntax or unsupported method.", lines[-1])
        self.assertIn(b"X-Trans-Id", lines[6])
        self.assertIn(b"X-Openstack-Request-Id", lines[7])
        self.assertIn("wsgi starting up", info_lines[0])
        self.assertIn("ERROR WSGI: code 400", info_lines[1])
        self.assertIn("txn:", info_lines[1])

    def test_bad_request_server_logging(self):
        with mock.patch('swift.common.http_protocol.generate_trans_id',
                        return_value='test-trans-id'):
            bytes_out = self._run_bytes_through_protocol(
                b"ONLY-METHOD\r\n"
                b"Server: example.com\r\n"
                b"\r\n"
            )
        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(
            lines[0], b"HTTP/1.1 400 Bad request syntax ('ONLY-METHOD')")
        self.assertIn(b"Bad request syntax or unsupported method.", lines[-1])
        self.assertIn(b"X-Trans-Id: test-trans-id", lines[6])
        self.assertIn(b"X-Openstack-Request-Id: test-trans-id", lines[7])
        info_lines = self.logger.get_lines_for_level('info')
        self.assertEqual(
            "ERROR WSGI: code 400, message "
            "Bad request syntax ('ONLY-METHOD'), (txn: test-trans-id)",
            info_lines[1])

    def test_bad_request_app_logging(self):
        app_logger = debug_logger()
        app = mock.MagicMock()
        app.logger = app_logger
        with mock.patch('swift.common.http_protocol.generate_trans_id',
                        return_value='test-trans-id'):
            bytes_out = self._run_bytes_through_protocol((
                b"ONLY-METHOD\r\n"
                b"Server: example.com\r\n"
                b"\r\n"
            ), app=app)
        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(
            lines[0], b"HTTP/1.1 400 Bad request syntax ('ONLY-METHOD')")
        self.assertIn(b"Bad request syntax or unsupported method.", lines[-1])
        self.assertIn(b"X-Trans-Id: test-trans-id", lines[6])
        self.assertIn(b"X-Openstack-Request-Id: test-trans-id", lines[7])
        self.assertEqual(1, len(app_logger.records.get('ERROR', [])))
        self.assertIn(
            "ERROR WSGI: code 400, message Bad request syntax ('ONLY-METHOD') "
            "(txn: test-trans-id)",
            app_logger.records.get('ERROR')[0])
        # but we can at least assert that the logger txn_id was set
        self.assertEqual('test-trans-id', app_logger.txn_id)

    def test_leading_slashes(self):
        bytes_out = self._run_bytes_through_protocol((
            b"GET ///some-leading-slashes HTTP/1.0\r\n"
            b"User-Agent: blah blah blah\r\n"
            b"\r\n"
        ))
        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[-1], b'///some-leading-slashes')

    def test_chunked_with_content_length(self):
        def reflecting_app(env, start_response):
            start_response('200 OK', [])
            return [env['wsgi.input'].read()]

        # This is more of a test of eventlet, but we've seen issues with it
        # before that were only caught in unit tests that require an XFS
        # tempdir, and so were skipped on the requirements job
        bytes_out = self._run_bytes_through_protocol((
            b"PUT /path HTTP/1.0\r\n"
            b"Content-Length: 10\r\n"
            b"Transfer-Encoding: chunked\r\n"
            b"\r\n"
            b"a\r\n"
            b"some text\n"
            b"\r\n"
            b"0\r\n"
            b"\r\n"
        ), app=reflecting_app)
        body = bytes_out.partition(b"\r\n\r\n")[2]
        self.assertEqual(body, b'some text\n')

    def test_request_lines(self):
        def app(env, start_response):
            start_response("200 OK", [])
            return [json.dumps({
                'RAW_PATH_INFO': env['RAW_PATH_INFO'],
                'QUERY_STRING': env.get('QUERY_STRING'),
            }).encode('ascii')]

        def do_test(request_line, expected):
            bytes_out = self._run_bytes_through_protocol(
                request_line + b'\r\n\r\n',
                app,
            )
            print(bytes_out)
            resp_body = bytes_out.partition(b'\r\n\r\n')[2]
            self.assertEqual(json.loads(resp_body), expected)

        do_test(b'GET / HTTP/1.1', {
            'RAW_PATH_INFO': u'/',
            'QUERY_STRING': None,
        })
        do_test(b'GET /%FF HTTP/1.1', {
            'RAW_PATH_INFO': u'/%FF',
            'QUERY_STRING': None,
        })

        do_test(b'GET /\xff HTTP/1.1', {
            'RAW_PATH_INFO': u'/\xff',
            'QUERY_STRING': None,
        })
        do_test(b'PUT /Here%20Is%20A%20SnowMan:\xe2\x98\x83 HTTP/1.0', {
            'RAW_PATH_INFO': u'/Here%20Is%20A%20SnowMan:\xe2\x98\x83',
            'QUERY_STRING': None,
        })
        do_test(
            b'POST /?and%20it=does+nothing+to+params&'
            b'PALMTREE=\xf0%9f\x8c%b4 HTTP/1.1', {
                'RAW_PATH_INFO': u'/',
                'QUERY_STRING': (u'and%20it=does+nothing+to+params'
                                 u'&PALMTREE=\xf0%9f\x8c%b4'),
            }
        )
        do_test(b'GET // HTTP/1.1', {
            'RAW_PATH_INFO': u'//',
            'QUERY_STRING': None,
        })
        do_test(b'GET //bar HTTP/1.1', {
            'RAW_PATH_INFO': u'//bar',
            'QUERY_STRING': None,
        })
        do_test(b'GET //////baz HTTP/1.1', {
            'RAW_PATH_INFO': u'//////baz',
            'QUERY_STRING': None,
        })


class TestProxyProtocol(ProtocolTest):
    protocol_class = http_protocol.SwiftHttpProxiedProtocol

    @staticmethod
    def app(env, start_response):
        start_response("200 OK", [])
        body = '\r\n'.join([
            'got addr: %s %s' % (
                env.get("REMOTE_ADDR", "<missing>"),
                env.get("REMOTE_PORT", "<missing>")),
            'on addr: %s %s' % (
                env.get("SERVER_ADDR", "<missing>"),
                env.get("SERVER_PORT", "<missing>")),
            'https is %s (scheme %s)' % (
                env.get("HTTPS", "<missing>"),
                env.get("wsgi.url_scheme", "<missing>")),
        ]) + '\r\n'
        return [body.encode("utf-8")]

    def test_request_with_proxy(self):
        bytes_out = self._run_bytes_through_protocol(
            b"PROXY TCP4 192.168.0.1 192.168.0.11 56423 4433\r\n"
            b"GET /someurl HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        )

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check
        self.assertEqual(lines[-3:], [
            b"got addr: 192.168.0.1 56423",
            b"on addr: 192.168.0.11 4433",
            b"https is <missing> (scheme http)",
        ])

    def test_request_with_proxy_https(self):
        bytes_out = self._run_bytes_through_protocol(
            b"PROXY TCP4 192.168.0.1 192.168.0.11 56423 443\r\n"
            b"GET /someurl HTTP/1.0\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
        )

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check
        self.assertEqual(lines[-3:], [
            b"got addr: 192.168.0.1 56423",
            b"on addr: 192.168.0.11 443",
            b"https is on (scheme https)",
        ])

    def test_multiple_requests_with_proxy(self):
        bytes_out = self._run_bytes_through_protocol(
            b"PROXY TCP4 192.168.0.1 192.168.0.11 56423 443\r\n"
            b"GET /someurl HTTP/1.1\r\n"
            b"User-Agent: something or other\r\n"
            b"\r\n"
            b"GET /otherurl HTTP/1.1\r\n"
            b"User-Agent: something or other\r\n"
            b"Connection: close\r\n"
            b"\r\n"
        )

        lines = bytes_out.split(b"\r\n")
        self.assertEqual(lines[0], b"HTTP/1.1 200 OK")  # sanity check

        # the address in the PROXY line is applied to every request
        addr_lines = [l for l in lines if l.startswith(b"got addr")]
        self.assertEqual(addr_lines, [b"got addr: 192.168.0.1 56423"] * 2)
        addr_lines = [l for l in lines if l.startswith(b"on addr")]
        self.assertEqual(addr_lines, [b"on addr: 192.168.0.11 443"] * 2)
        addr_lines = [l for l in lines if l.startswith(b"https is")]
        self.assertEqual(addr_lines, [b"https is on (scheme https)"] * 2)

    def test_missing_proxy_line(self):
        with mock.patch('swift.common.http_protocol.generate_trans_id',
                        return_value='test-bad-req-trans-id'):
            bytes_out = self._run_bytes_through_protocol(
                # whoops, no PROXY line here
                b"GET /someurl HTTP/1.0\r\n"
                b"User-Agent: something or other\r\n"
                b"\r\n"
            )

        lines = [l for l in bytes_out.split(b"\r\n") if l]
        info_lines = self.logger.get_lines_for_level('info')

        self.assertEqual(
            lines[0],
            b"HTTP/1.1 400 Invalid PROXY line 'GET /someurl HTTP/1.0\\r\\n'")
        self.assertIn(b"X-Trans-Id: test-bad-req-trans-id", lines[6])
        self.assertIn(b"X-Openstack-Request-Id: test-bad-req-trans-id",
                      lines[7])
        self.assertEqual(
            "ERROR WSGI: code 400, message Invalid PROXY line "
            "'GET /someurl HTTP/1.0\\r\\n', "
            "(txn: test-bad-req-trans-id)",
            info_lines[1])

    def test_malformed_proxy_lines(self):
        for bad_line in [b'PROXY jojo',
                         b'PROXYjojo a b c d e',
                         b'PROXY a b c d e',  # bad INET protocol and family
                         ]:
            bytes_out = self._run_bytes_through_protocol(bad_line)
            lines = [l for l in bytes_out.split(b"\r\n") if l]
            info_lines = self.logger.get_lines_for_level('info')
            self.assertIn(b"400 Invalid PROXY line", lines[0])
            self.assertIn(b"X-Trans-Id", lines[6])
            self.assertIn(b"X-Openstack-Request-Id", lines[7])
            self.assertIn("wsgi starting up", info_lines[0])
            self.assertIn("txn:", info_lines[1])

    def test_unknown_client_addr(self):
        # For "UNKNOWN", the rest of the line before the CRLF may be omitted by
        # the sender, and the receiver must ignore anything presented before
        # the CRLF is found.
        for unknown_line in [b'PROXY UNKNOWN',  # mimimal valid unknown
                             b'PROXY UNKNOWNblahblah',  # also valid
                             b'PROXY UNKNOWN a b c d']:
            bytes_out = self._run_bytes_through_protocol(
                unknown_line + (b"\r\n"
                                b"GET /someurl HTTP/1.0\r\n"
                                b"User-Agent: something or other\r\n"
                                b"\r\n")
            )
            lines = [l for l in bytes_out.split(b"\r\n") if l]
            self.assertIn(b"200 OK", lines[0])

    def test_address_and_environ(self):
        # Make an object we can exercise... note the base class's __init__()
        # does a bunch of work, so we just new up an object like eventlet.wsgi
        # does.
        dummy_env = {'OTHER_ENV_KEY': 'OTHER_ENV_VALUE'}
        mock_protocol = mock.Mock(get_environ=lambda s: dummy_env)
        patcher = mock.patch(
            'swift.common.http_protocol.SwiftHttpProtocol', mock_protocol
        )
        self.mock_super = patcher.start()
        self.addCleanup(patcher.stop)

        proto_class = http_protocol.SwiftHttpProxiedProtocol
        try:
            proxy_obj = types.InstanceType(proto_class)
        except AttributeError:
            proxy_obj = proto_class.__new__(proto_class)

        # Install some convenience mocks
        proxy_obj.server = Namespace(app=Namespace(logger=mock.Mock()),
                                     url_length_limit=777,
                                     log=mock.Mock())
        proxy_obj.send_error = mock.Mock()

        proxy_obj.rfile = BytesIO(
            b'PROXY TCP4 111.111.111.111 222.222.222.222 111 222'
        )

        assert proxy_obj.handle()

        self.assertEqual(proxy_obj.client_address, ('111.111.111.111', '111'))
        self.assertEqual(proxy_obj.proxy_address, ('222.222.222.222', '222'))
        expected_env = {
            'SERVER_PORT': '222',
            'SERVER_ADDR': '222.222.222.222',
            'OTHER_ENV_KEY': 'OTHER_ENV_VALUE'
        }
        self.assertEqual(proxy_obj.get_environ(), expected_env)
