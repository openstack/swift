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

from eventlet import wsgi, websocket
import six

from swift.common.swob import wsgi_quote, wsgi_unquote, \
    wsgi_quote_plus, wsgi_unquote_plus, wsgi_to_bytes, bytes_to_wsgi


class SwiftHttpProtocol(wsgi.HttpProtocol):
    default_request_version = "HTTP/1.0"

    def __init__(self, *args, **kwargs):
        # See https://github.com/eventlet/eventlet/pull/590
        self.pre_shutdown_bugfix_eventlet = not getattr(
            websocket.WebSocketWSGI, '_WSGI_APP_ALWAYS_IDLE', None)
        # Note this is not a new-style class, so super() won't work
        wsgi.HttpProtocol.__init__(self, *args, **kwargs)

    def log_request(self, *a):
        """
        Turn off logging requests by the underlying WSGI software.
        """
        pass

    def log_message(self, f, *a):
        """
        Redirect logging other messages by the underlying WSGI software.
        """
        logger = getattr(self.server.app, 'logger', None)
        if logger:
            logger.error('ERROR WSGI: ' + f, *a)
        else:
            # eventlet<=0.17.4 doesn't have an error method, and in newer
            # versions the output from error is same as info anyway
            self.server.log.info('ERROR WSGI: ' + f, *a)

    class MessageClass(wsgi.HttpProtocol.MessageClass):
        '''Subclass to see when the client didn't provide a Content-Type'''
        # for py2:
        def parsetype(self):
            if self.typeheader is None:
                self.typeheader = ''
            wsgi.HttpProtocol.MessageClass.parsetype(self)

        # for py3:
        def get_default_type(self):
            '''If the client didn't provide a content type, leave it blank.'''
            return ''

    def parse_request(self):
        # Need to track the bytes-on-the-wire for S3 signatures -- eventlet
        # would do it for us, but since we rewrite the path on py3, we need to
        # fix it ourselves later.
        self.__raw_path_info = None

        if not six.PY2:
            # request lines *should* be ascii per the RFC, but historically
            # we've allowed (and even have func tests that use) arbitrary
            # bytes. This breaks on py3 (see https://bugs.python.org/issue33973
            # ) but the work-around is simple: munge the request line to be
            # properly quoted.
            if self.raw_requestline.count(b' ') >= 2:
                parts = self.raw_requestline.split(b' ', 2)
                path, q, query = parts[1].partition(b'?')
                self.__raw_path_info = path
                # unquote first, so we don't over-quote something
                # that was *correctly* quoted
                path = wsgi_to_bytes(wsgi_quote(wsgi_unquote(
                    bytes_to_wsgi(path))))
                query = b'&'.join(
                    sep.join([
                        wsgi_to_bytes(wsgi_quote_plus(wsgi_unquote_plus(
                            bytes_to_wsgi(key)))),
                        wsgi_to_bytes(wsgi_quote_plus(wsgi_unquote_plus(
                            bytes_to_wsgi(val))))
                    ])
                    for part in query.split(b'&')
                    for key, sep, val in (part.partition(b'='), ))
                parts[1] = path + q + query
                self.raw_requestline = b' '.join(parts)
            # else, mangled protocol, most likely; let base class deal with it
        return wsgi.HttpProtocol.parse_request(self)

    if not six.PY2:
        def get_environ(self, *args, **kwargs):
            environ = wsgi.HttpProtocol.get_environ(self, *args, **kwargs)
            environ['RAW_PATH_INFO'] = bytes_to_wsgi(
                self.__raw_path_info)
            header_payload = self.headers.get_payload()
            if isinstance(header_payload, list) and len(header_payload) == 1:
                header_payload = header_payload[0].get_payload()
            if header_payload:
                # This shouldn't be here. We must've bumped up against
                # https://bugs.python.org/issue37093
                headers_raw = list(environ['headers_raw'])
                for line in header_payload.rstrip('\r\n').split('\n'):
                    if ':' not in line or line[:1] in ' \t':
                        # Well, we're no more broken than we were before...
                        # Should we support line folding?
                        # Should we 400 a bad header line?
                        break
                    header, value = line.split(':', 1)
                    value = value.strip(' \t\n\r')
                    # NB: Eventlet looks at the headers obj to figure out
                    # whether the client said the connection should close;
                    # see https://github.com/eventlet/eventlet/blob/v0.25.0/
                    # eventlet/wsgi.py#L504
                    self.headers.add_header(header, value)
                    headers_raw.append((header, value))
                    wsgi_key = 'HTTP_' + header.replace('-', '_').encode(
                        'latin1').upper().decode('latin1')
                    if wsgi_key in ('HTTP_CONTENT_LENGTH',
                                    'HTTP_CONTENT_TYPE'):
                        wsgi_key = wsgi_key[5:]
                    environ[wsgi_key] = value
                environ['headers_raw'] = tuple(headers_raw)
                # Since we parsed some more headers, check to see if they
                # change how our wsgi.input should behave
                te = environ.get('HTTP_TRANSFER_ENCODING', '').lower()
                if te.rsplit(',', 1)[-1].strip() == 'chunked':
                    environ['wsgi.input'].chunked_input = True
                else:
                    length = environ.get('CONTENT_LENGTH')
                    if length:
                        length = int(length)
                    environ['wsgi.input'].content_length = length
                if environ.get('HTTP_EXPECT', '').lower() == '100-continue':
                    environ['wsgi.input'].wfile = self.wfile
                    environ['wsgi.input'].wfile_line = \
                        b'HTTP/1.1 100 Continue\r\n'
            return environ

    def _read_request_line(self):
        # Note this is not a new-style class, so super() won't work
        got = wsgi.HttpProtocol._read_request_line(self)
        # See https://github.com/eventlet/eventlet/pull/590
        if self.pre_shutdown_bugfix_eventlet:
            self.conn_state[2] = wsgi.STATE_REQUEST
        return got

    def handle_one_request(self):
        # Note this is not a new-style class, so super() won't work
        got = wsgi.HttpProtocol.handle_one_request(self)
        # See https://github.com/eventlet/eventlet/pull/590
        if self.pre_shutdown_bugfix_eventlet:
            if self.conn_state[2] != wsgi.STATE_CLOSE:
                self.conn_state[2] = wsgi.STATE_IDLE
        return got


class SwiftHttpProxiedProtocol(SwiftHttpProtocol):
    """
    Protocol object that speaks HTTP, including multiple requests, but with
    a single PROXY line as the very first thing coming in over the socket.
    This is so we can learn what the client's IP address is when Swift is
    behind a TLS terminator, like hitch, that does not understand HTTP and
    so cannot add X-Forwarded-For or other similar headers.

    See http://www.haproxy.org/download/1.7/doc/proxy-protocol.txt for
    protocol details.
    """
    def __init__(self, *a, **kw):
        self.proxy_address = None
        SwiftHttpProtocol.__init__(self, *a, **kw)

    def handle_error(self, connection_line):
        if not six.PY2:
            connection_line = connection_line.decode('latin-1')

        # No further processing will proceed on this connection under any
        # circumstances.  We always send the request into the superclass to
        # handle any cleanup - this ensures that the request will not be
        # processed.
        self.rfile.close()
        # We don't really have any confidence that an HTTP Error will be
        # processable by the client as our transmission broken down between
        # ourselves and our gateway proxy before processing the client
        # protocol request.  Hopefully the operator will know what to do!
        msg = 'Invalid PROXY line %r' % connection_line
        self.log_message(msg)
        # Even assuming HTTP we don't even known what version of HTTP the
        # client is sending?  This entire endeavor seems questionable.
        self.request_version = self.default_request_version
        # appease http.server
        self.command = 'PROXY'
        self.send_error(400, msg)

    def handle(self):
        """Handle multiple requests if necessary."""
        # ensure the opening line for the connection is a valid PROXY protcol
        # line; this is the only IO we do on this connection before any
        # additional wrapping further pollutes the raw socket.
        connection_line = self.rfile.readline(self.server.url_length_limit)

        if not connection_line.startswith(b'PROXY '):
            return self.handle_error(connection_line)

        proxy_parts = connection_line.strip(b'\r\n').split(b' ')
        if proxy_parts[1].startswith(b'UNKNOWN'):
            # "UNKNOWN", in PROXY protocol version 1, means "not
            # TCP4 or TCP6". This includes completely legitimate
            # things like QUIC or Unix domain sockets. The PROXY
            # protocol (section 2.1) states that the receiver
            # (that's us) MUST ignore anything after "UNKNOWN" and
            # before the CRLF, essentially discarding the first
            # line.
            pass
        elif proxy_parts[1] in (b'TCP4', b'TCP6') and len(proxy_parts) == 6:
            if six.PY2:
                self.client_address = (proxy_parts[2], proxy_parts[4])
                self.proxy_address = (proxy_parts[3], proxy_parts[5])
            else:
                self.client_address = (
                    proxy_parts[2].decode('latin-1'),
                    proxy_parts[4].decode('latin-1'))
                self.proxy_address = (
                    proxy_parts[3].decode('latin-1'),
                    proxy_parts[5].decode('latin-1'))
        else:
            self.handle_error(connection_line)

        return SwiftHttpProtocol.handle(self)

    def get_environ(self, *args, **kwargs):
        environ = SwiftHttpProtocol.get_environ(self, *args, **kwargs)
        if self.proxy_address:
            environ['SERVER_ADDR'] = self.proxy_address[0]
            environ['SERVER_PORT'] = self.proxy_address[1]
            if self.proxy_address[1] == '443':
                environ['wsgi.url_scheme'] = 'https'
                environ['HTTPS'] = 'on'
        return environ
