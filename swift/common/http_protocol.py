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

from swift.common.utils import generate_trans_id
from swift.common.http import HTTP_NO_CONTENT, HTTP_RESET_CONTENT, \
    HTTP_NOT_MODIFIED

from eventlet.green.http import client as http_client
from html import escape


class SwiftHttpProtocol(wsgi.HttpProtocol):
    default_request_version = "HTTP/1.0"
    reject_bad_requests = False

    def __init__(self, *args, **kwargs):
        # See https://github.com/eventlet/eventlet/pull/590
        self.pre_shutdown_bugfix_eventlet = not getattr(
            websocket.WebSocketWSGI, '_WSGI_APP_ALWAYS_IDLE', None)
        super().__init__(*args, **kwargs)

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
        """Subclass to see when the client didn't provide a Content-Type"""
        def get_default_type(self):
            """If the client didn't provide a content type, leave it blank."""
            return ''

    def parse_request(self):
        """Parse a request (inlined from cpython@7e293984).

        The request should be stored in self.raw_requestline; the results
        are in self.command, self.path, self.request_version and
        self.headers.

        Return True for success, False for failure; on failure, any relevant
        error response has already been sent back.

        """
        self.command = None  # set in case of error on the first line
        self.request_version = version = self.default_request_version
        self.close_connection = True
        requestline = self.raw_requestline.decode('iso-8859-1')
        requestline = requestline.rstrip('\r\n')
        self.requestline = requestline
        # Split off \x20 explicitly (see https://bugs.python.org/issue33973)
        words = requestline.split(' ')
        if len(words) == 0:
            return False

        if len(words) >= 3:  # Enough to determine protocol version
            version = words[-1]
            try:
                if not version.startswith('HTTP/'):
                    raise ValueError
                base_version_number = version.split('/', 1)[1]
                version_number = base_version_number.split(".")
                # RFC 2145 section 3.1 says there can be only one "." and
                #   - major and minor numbers MUST be treated as
                #      separate integers;
                #   - HTTP/2.4 is a lower version than HTTP/2.13, which in
                #      turn is lower than HTTP/12.3;
                #   - Leading zeros MUST be ignored by recipients.
                if len(version_number) != 2:
                    raise ValueError
                version_number = int(version_number[0]), int(version_number[1])
            except (ValueError, IndexError):
                self.send_error(
                    400,
                    "Bad request version (%r)" % version)
                return False
            if version_number >= (1, 1) and \
                    self.protocol_version >= "HTTP/1.1":
                self.close_connection = False
            if version_number >= (2, 0):
                self.send_error(
                    505,
                    "Invalid HTTP version (%s)" % base_version_number)
                return False
            self.request_version = version

        if not 2 <= len(words) <= 3:
            self.send_error(
                400,
                "Bad request syntax (%r)" % requestline)
            return False
        command, path = words[:2]
        if len(words) == 2:
            self.close_connection = True
            if command != 'GET':
                self.send_error(
                    400,
                    "Bad HTTP/0.9 request type (%r)" % command)
                return False

        if path.startswith(('http://', 'https://')):
            host, sep, rest = path.partition('//')[2].partition('/')
            if sep:
                path = '/' + rest

        self.command, self.path = command, path

        # Examine the headers and look for a Connection directive.
        try:
            self.headers = http_client.parse_headers(
                self.rfile,
                _class=self.MessageClass)
        except http_client.LineTooLong as err:
            self.send_error(
                431,
                "Line too long",
                str(err))
            return False
        except http_client.HTTPException as err:
            self.send_error(
                431,
                "Too many headers",
                str(err)
            )
            return False

        conntype = self.headers.get('Connection', "")
        if conntype.lower() == 'close':
            self.close_connection = True
        elif (conntype.lower() == 'keep-alive' and
              self.protocol_version >= "HTTP/1.1"):
            self.close_connection = False
        # Examine the headers and look for an Expect directive
        expect = self.headers.get('Expect', "")
        if (expect.lower() == "100-continue" and
                self.protocol_version >= "HTTP/1.1" and
                self.request_version >= "HTTP/1.1"):
            if not self.handle_expect_100():
                return False
        return True

    def get_environ(self, *args, **kwargs):
        environ = wsgi.HttpProtocol.get_environ(self, *args, **kwargs)
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
        got = super()._read_request_line()
        # See https://github.com/eventlet/eventlet/pull/590
        if self.pre_shutdown_bugfix_eventlet:
            self.conn_state[2] = wsgi.STATE_REQUEST
        return got

    def handle_one_request(self):
        got = super().handle_one_request()
        # See https://github.com/eventlet/eventlet/pull/590
        if self.pre_shutdown_bugfix_eventlet:
            if self.conn_state[2] != wsgi.STATE_CLOSE:
                self.conn_state[2] = wsgi.STATE_IDLE
        return got

    def send_error(self, code, message=None, explain=None):
        """Send and log an error reply, we are overriding the cpython parent
        class method, so we can have logger generate txn_id's for error
        response from wsgi since we are at the edge of the proxy server.
        This sends an error response (so it must be called before any output
        has been generated), logs the error, and finally sends a piece of HTML
        explaining the error to the user.

        :param code:    an HTTP error code
                   3 digits
        :param message: a simple optional 1 line reason phrase.
                   *( HTAB / SP / VCHAR / %x80-FF )
                   defaults to short entry matching the response code
        :param explain: a detailed message defaults to the long entry
                   matching the response code.
        """

        try:
            shortmsg, longmsg = self.responses[code]
        except KeyError:
            shortmsg, longmsg = '???', '???'
        if message is None:
            message = shortmsg
        if explain is None:
            explain = longmsg

        try:
            # assume we have a LogAdapter
            txn_id = self.server.app.logger.txn_id  # just in case it was set
        except AttributeError:
            # turns out we don't have a LogAdapter, so go direct
            txn_id = generate_trans_id('')
            self.log_error("code %d, message %s, (txn: %s)", code,
                           message, txn_id)
        else:
            # we do have a LogAdapter, but likely not yet a txn_id
            txn_id = txn_id or generate_trans_id('')
            self.server.app.logger.txn_id = txn_id
            self.log_error("code %d, message %s", code, message)
        self.send_response(code, message)
        self.send_header('Connection', 'close')

        # Message body is omitted for cases described in:
        #  - RFC7230: 3.3. 1xx, 204(No Content), 304(Not Modified)
        #  - RFC7231: 6.3.6. 205(Reset Content)
        body = None
        exclude_status = (HTTP_NO_CONTENT,
                          HTTP_RESET_CONTENT,
                          HTTP_NOT_MODIFIED)
        if (code >= 200 and
                code not in exclude_status):
            # HTML encode to prevent Cross Site Scripting attacks
            # (see bug https://bugs.python.org/issue1100201)
            content = (self.error_message_format % {
                'code': code,
                'message': escape(message, quote=False),
                'explain': escape(explain, quote=False)
            })
            body = content.encode('UTF-8', 'replace')
            self.send_header("Content-Type", self.error_content_type)
            self.send_header('Content-Length', str(len(body)))
            self.send_header('X-Trans-Id', txn_id)
            self.send_header('X-Openstack-Request-Id', txn_id)
        self.end_headers()

        if self.command != 'HEAD' and body:
            self.wfile.write(body)


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
