# Copyright (c) 2010-2026 OpenStack Foundation
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
#
# Portions of this module are derived from Gunicorn
# (https://gunicorn.org/), which is released under the MIT license:
#
#   2009-2026 (c) Benoît Chesneau <benoitc@gunicorn.org>
#   2009-2015 (c) Paul J. Davis <paul.joseph.davis@gmail.com>
#
#   Permission is hereby granted, free of charge, to any person
#   obtaining a copy of this software and associated documentation
#   files (the "Software"), to deal in the Software without
#   restriction, including without limitation the rights to use,
#   copy, modify, merge, publish, distribute, sublicense, and/or sell
#   copies of the Software, and to permit persons to whom the
#   Software is furnished to do so, subject to the following
#   conditions:
#
#   The above copyright notice and this permission notice shall be
#   included in all copies or substantial portions of the Software.
#
#   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
#   OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
#   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
#   HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
#   WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#   FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
#   OTHER DEALINGS IN THE SOFTWARE.

import errno
import os
import re
import socket
import string
import time
from io import BytesIO
from urllib.parse import unquote

try:
    import multiprocessing
    CPU_COUNT = multiprocessing.cpu_count() or 1
except (ImportError, NotImplementedError):
    CPU_COUNT = 1

import pwd

from swift.common import constraints, utils
from swift.common.storage_policy import BindPortsCache, \
    reload_storage_policies
from swift.common.wsgi import _initrp, loadapp, appconfig, ConfigFileError
from swift.common.utils import capture_stdio, config_fallocate_value, \
    clean_up_daemon_hygiene, systemd_notify, config_auto_int_value, \
    config_true_value

import gunicorn.util
import gunicorn.sock
import gunicorn.app.base
import gunicorn.http.message
import gunicorn.http.wsgi
import gunicorn.http.body
from gunicorn.config import Config
from gunicorn.glogging import Logger
from gunicorn.workers.gthread import ThreadWorker, TConn
from gunicorn.http.unreader import SocketUnreader
from gunicorn.http.body import Body, ChunkedReader
from gunicorn.http.errors import (
    NoMoreData, ChunkMissingTerminator, InvalidChunkSize,
    LimitRequestLine, LimitRequestHeaders, ForbiddenProxyRequest)
# Deliberate chunk-parse rejections (all IOError/OSError subclasses, so they
# must be re-raised before the EOF/socket-error handling below catches them).
# InvalidChunkExtension was added in gunicorn 26.0.0; on our >=24.1.1 floor
# older gunicorn never raises it, so include it only when available.
_CHUNK_PARSE_ERRORS = (InvalidChunkSize, ChunkMissingTerminator)
try:
    from gunicorn.http.errors import InvalidChunkExtension
    _CHUNK_PARSE_ERRORS += (InvalidChunkExtension,)
except ImportError:
    pass
# ChunkReadError is what wsgi-input readers expect: eventlet raises
# eventlet.wsgi.ChunkReadError; concurrency re-exports it (with a
# ValueError-based stand-in when eventlet is absent). ssync's Receiver
# catches it directly and the proxy PUT reader catches ValueError, so
# raising it satisfies both.
from swift.common.concurrency import ChunkReadError, register_kill_hook

# Compatibility with eventlet.wsgi.MINIMUM_CHUNK_SIZE, referenced by
# EventletPlungerString in swift/obj/server.py for zero-copy sends
MINIMUM_CHUNK_SIZE = 4096


_GUNICORN_PATCHED = False

# Seconds a gthread worker will block reading a request line + headers before
# giving up on a stalled client; set from client_timeout by build_cfg (a
# reload refreshes it) and read by the patched TConn.init below. None disables.
_CLIENT_TIMEOUT = None


def _request_body_undrained(req, inp):
    # True if the app responded without reading the whole request body: fewer
    # bytes than Content-Length, or a chunked body whose terminating chunk
    # (clean EOF) was never reached.
    content_length = None
    chunked = False
    for h, v in req.headers:
        if h == "CONTENT-LENGTH":
            try:
                content_length = int(v)
            except (TypeError, ValueError):
                content_length = None
        elif h == "TRANSFER-ENCODING":
            chunked = True
    if content_length is not None:
        return getattr(inp, 'bytes_read', content_length) < content_length
    return chunked and not getattr(inp, 'saw_eof', True)


def patch_gunicorn():
    # Idempotent: build_cfg() calls this on every (re)load, but the wrapping
    # patches below must not stack.
    global _GUNICORN_PATCHED
    if _GUNICORN_PATCHED:
        return

    # We wrap specific gthread internals below; on a too-old gunicorn these
    # are absent. Check up front and fail with an actionable message naming the
    # version and what's missing, rather than an opaque request-time
    # AttributeError.
    _required = [
        (ThreadWorker, 'wait_for_and_dispatch_events'),
        (ThreadWorker, 'murder_keepalived'),
        (TConn, 'init'),
    ]
    _missing = ['%s.%s' % (cls.__name__, attr)
                for cls, attr in _required if not hasattr(cls, attr)]
    if _missing:
        raise RuntimeError(
            'gunicorn %s is too old for the threading WSGI server: missing '
            'gthread internals %s. Upgrade gunicorn.'
            % (gunicorn.__version__, ', '.join(_missing)))

    # Prevent gunicorn from URL-decoding and let Swift handle this
    gunicorn.util.unquote_to_wsgi_str = \
        lambda s: unquote(s, encoding='latin-1')

    # Relax gunicorn's strict RFC 9110 token validation to match eventlet:
    # S3 clients put bytes like \x80-\xff in metadata header names, which
    # eventlet passed through and Swift's S3 layer expects to see.
    TOKEN_SPECIALS = r'!"#$%&\'()*+-./<=>?@[\\]^`{|}~_'  # nosec: B105
    TOKEN_RE = re.compile(r"[%s0-9a-zA-Z\x80-\xff]+" % (
        re.escape(TOKEN_SPECIALS)))
    gunicorn.http.message.TOKEN_RE = TOKEN_RE
    gunicorn.http.wsgi.TOKEN_RE = TOKEN_RE

    # Gunicorn uses [a-z#] to reject HTTP methods containing lowercase letters,
    # but Swift uses GETorHEAD quite a bit thus allowing lowercase chars
    gunicorn.http.message.METHOD_BADCHAR_RE = re.compile('[#]')

    # gunicorn rejects control octets in header VALUES (RFC 9110 5.5);
    # eventlet did not. Swift's reserved namespace puts NUL (\x00) in values
    # (e.g. X-Symlink-Target), so allow NUL while keeping the rest rejected.
    gunicorn.http.message.RFC9110_5_5_INVALID_AND_DANGEROUS = re.compile(
        r"[\x01-\x08\x0a-\x1f\x7f]")

    # gunicorn rejects a request-target that isn't origin/absolute/authority
    # form or "*" with a 400; eventlet passes any target through as the path
    # (Swift 404s an unknown path). Mirror gunicorn's parse_request_line but
    # drop that rejection so threading matches eventlet.
    msg = gunicorn.http.message

    def swift_parse_request_line(self, line_bytes):
        bits = [msg.bytes_to_str(bit) for bit in line_bytes.split(b" ", 2)]
        if len(bits) != 3:
            raise msg.InvalidRequestLine(msg.bytes_to_str(line_bytes))
        self.method = bits[0]
        if not self.cfg.permit_unconventional_http_method:
            if msg.METHOD_BADCHAR_RE.search(self.method):
                raise msg.InvalidRequestMethod(self.method)
            if not 3 <= len(bits[0]) <= 20:
                raise msg.InvalidRequestMethod(self.method)
        if not msg.TOKEN_RE.fullmatch(self.method):
            raise msg.InvalidRequestMethod(self.method)
        if self.cfg.casefold_http_method:
            self.method = self.method.upper()
        self.uri = bits[1]
        if len(self.uri) == 0:
            raise msg.InvalidRequestLine(msg.bytes_to_str(line_bytes))
        if self.uri == "*" and self.method != "OPTIONS":
            raise msg.InvalidRequestLine(msg.bytes_to_str(line_bytes))
        # NB: gunicorn rejects non-origin/absolute/authority/asterisk targets
        # here; Swift (like eventlet) accepts any target as the path.
        try:
            parts = msg.split_request_uri(self.uri)
        except ValueError:
            raise msg.InvalidRequestLine(msg.bytes_to_str(line_bytes))
        self.path = parts.path or ""
        self.query = parts.query or ""
        self.fragment = parts.fragment or ""
        match = msg.VERSION_RE.fullmatch(bits[2])
        if match is None:
            raise msg.InvalidHTTPVersion(bits[2])
        self.version = (int(match.group(1)), int(match.group(2)))
        if not (1, 0) <= self.version < (2, 0):
            if not self.cfg.permit_unconventional_http_version:
                raise msg.InvalidHTTPVersion(self.version)

    gunicorn.http.message.Request.parse_request_line = swift_parse_request_line

    # Gunicorn always sends a "Connection:" header in responses, but Swift
    # only echoes a Connection header when the client included one
    orig_default_headers = gunicorn.http.wsgi.Response.default_headers

    def swift_default_headers(self):
        headers = orig_default_headers(self)
        req_has_connection = any(
            h == "CONNECTION" for h, v in self.req.headers)
        if not req_has_connection and not self.should_close():
            headers = [h for h in headers if not h.startswith("Connection:")]
        return headers

    gunicorn.http.wsgi.Response.default_headers = swift_default_headers

    # Gunicorn drops hop-by-hop Connection from app responses, but Swift uses
    # an app-set "Connection: close" to force the socket shut (e.g. the
    # RequestTimeout answer); honor it before it is dropped.
    orig_process_headers = gunicorn.http.wsgi.Response.process_headers

    def swift_process_headers(self, headers):
        if any(n.lower() == 'connection' and v.strip().lower() == 'close'
               for n, v in headers):
            self.force_close()
        return orig_process_headers(self, headers)

    gunicorn.http.wsgi.Response.process_headers = swift_process_headers

    # Close the connection if the app finished without draining the request
    # body (client read timeout / abort). Checked once the response iterator is
    # exhausted -- not at header time -- so full-duplex apps like ssync, which
    # read the body while responding, are not cut off early.
    orig_response_close = gunicorn.http.wsgi.Response.close

    def swift_response_close(self):
        inp = getattr(self.req, 'swift_input', None)
        if inp is not None and _request_body_undrained(self.req, inp):
            self.force_close()
        return orig_response_close(self)

    gunicorn.http.wsgi.Response.close = swift_response_close

    # Add additional data to the request environ
    orig_default_environ = gunicorn.http.wsgi.default_environ

    def swift_default_environ(req, sock, cfg):
        # Strip Expect: 100-continue so gunicorn won't answer the handshake
        # itself; Swift sends the 100 Continue later, via ChunkedInput.
        headers = []
        expect_continue = False
        for name, value in req.headers:
            if name == 'EXPECT' and value.lower() == '100-continue':
                expect_continue = True
            else:
                headers.append((name, value))
        if expect_continue:
            req.headers = headers
            req._expected_100_continue = False  # Required for gunicorn >= 25

        env = orig_default_environ(req, sock, cfg)
        env.update({"headers_raw": req.headers})  # needed by s3api
        env['gunicorn.socket'] = sock
        env['wsgi.input'].get_socket = lambda: sock  # used by obj/server.py

        if expect_continue:
            env['wsgi.input'] = ChunkedInput(env['wsgi.input'], sock, req)
        else:
            # A request without Expect: 100-continue keeps gunicorn's raw body
            # rather than ChunkedInput. Wrap it so a chunked client that closes
            # early or sends a malformed chunk surfaces as ChunkReadError, the
            # way eventlet's wsgi.Input reports it, not gunicorn's NoMoreData/
            # ChunkMissingTerminator/InvalidChunk*.
            env['wsgi.input'] = _ChunkReadErrorInput(env['wsgi.input'])

        # Expose the wrapped input so swift_default_headers can check whether
        # the body was drained.
        req.swift_input = env['wsgi.input']
        return env

    gunicorn.http.wsgi.default_environ = swift_default_environ

    # Gunicorn uppercases header names via str.upper(), which corrupts some
    # non-ASCII octets (e.g. \xf0 -> \xd0; gunicorn commit f5501111a). Run the
    # original parse_headers, then fix the header names.
    orig_parse_headers = gunicorn.http.message.Request.parse_headers

    def swift_parse_headers(self, data, from_trailer=False):
        headers = orig_parse_headers(self, data, from_trailer)

        mapping = str.maketrans(string.ascii_lowercase, string.ascii_uppercase)
        name_map = {}
        for line in data.split(b"\r\n"):
            i = line.find(b":")
            if i > 0:
                name = line[:i].decode('latin-1')
                upper_name = name.translate(mapping)
                name_map[name.upper()] = upper_name
        return [(name_map.get(name, name), value) for name, value in headers]

    gunicorn.http.message.Request.parse_headers = swift_parse_headers

    # eventlet debug-logs when it disconnects an idle keep-alive client past
    # the timeout; gunicorn does it silently. The murder_keepalived patch below
    # restores that log via the Swift app logger (so test_GET_pipeline
    # matches). gunicorn also only reaps idle keep-alive connections once per
    # ~1s poll, so a sub-second keepalive deadline would slip to the next tick
    # (eventlet enforces it precisely via the socket recv timeout). Cap the
    # poll by the nearest deadline.
    orig_wait_for_events = ThreadWorker.wait_for_and_dispatch_events

    def swift_wait_for_and_dispatch_events(self, timeout):
        # Only needed when keepalive is sub-second (test servers from
        # socket_timeout); a multi-second production keepalive is fine at the
        # default poll granularity, and capping it would perturb timing.
        if self.keepalived_conns and self.cfg.keepalive < 1:
            nearest = min(c.timeout for c in self.keepalived_conns)
            timeout = max(0, min(timeout, nearest - time.monotonic()))
        return orig_wait_for_events(self, timeout)

    ThreadWorker.wait_for_and_dispatch_events = \
        swift_wait_for_and_dispatch_events

    # Bound the request-read phase (request line + headers) by client_timeout.
    # gunicorn's gthread reads blocking in a pool thread with no socket
    # timeout, so a client that stalls mid-request would pin the thread forever
    # (eventlet bounded this via the socket's client_timeout). Apply it in
    # conn.init(), which runs per request; init()'s own setblocking(True) would
    # otherwise clear it. Body reads are bounded separately by Swift's
    # WatchdogTimeout on gunicorn.socket.
    orig_tconn_init = TConn.init

    def swift_tconn_init(self):
        orig_tconn_init(self)
        if _CLIENT_TIMEOUT is not None:
            self.sock.settimeout(_CLIENT_TIMEOUT)

    TConn.init = swift_tconn_init

    # Bound the TLS handshake by client_timeout. TConn.init wraps the socket
    # and runs the handshake (inside orig_tconn_init, on a blocking socket)
    # before swift_tconn_init sets the read timeout, so a client that connects
    # but never sends a ClientHello would pin the gthread forever. Apply the
    # timeout to the raw socket before wrapping; the handshake then honours it.
    orig_ssl_wrap_socket = gunicorn.sock.ssl_wrap_socket

    def swift_ssl_wrap_socket(raw_sock, cfg):
        if _CLIENT_TIMEOUT is not None:
            raw_sock.settimeout(_CLIENT_TIMEOUT)
        return orig_ssl_wrap_socket(raw_sock, cfg)

    gunicorn.sock.ssl_wrap_socket = swift_ssl_wrap_socket

    orig_murder_keepalived = ThreadWorker.murder_keepalived

    def swift_murder_keepalived(self):
        swift_logger = getattr(self, 'swift_logger', None)
        if swift_logger is not None:
            now = time.monotonic()
            for conn in list(self.keepalived_conns):
                if conn.timeout - now <= 0:
                    swift_logger.debug(
                        'Client timed out: %s', getattr(conn, 'client', conn))
        return orig_murder_keepalived(self)

    ThreadWorker.murder_keepalived = swift_murder_keepalived

    # Gunicorn rejects requests with both Content-Length and Transfer-Encoding
    # (RFC 9112), but Swift sends both (e.g. Content-Length: 0 + chunked PUT).
    # Strip Content-Length when Transfer-Encoding is present.
    orig_set_body_reader = gunicorn.http.message.Message.set_body_reader

    def swift_set_body_reader(self):
        if any(n == "TRANSFER-ENCODING" for n, v in self.headers):
            self.headers = [
                (n, v) for n, v in self.headers if n != "CONTENT-LENGTH"]
        orig_set_body_reader(self)

    gunicorn.http.message.Message.set_body_reader = swift_set_body_reader

    # Copy of gunicorn.http.body.Body.read, changing self.reader.read(1024) to
    # the actual requested size -- avoids useless copying and speeds up chunked
    # transfers. Remove when github.com/benoitc/gunicorn/issues/2596 is closed.
    def swift_read(self, size=None):
        size = self.getsize(size)
        if size == 0:
            return b""

        if size < self.buf.tell():
            data = self.buf.getvalue()
            ret, rest = data[:size], data[size:]
            self.buf = BytesIO()
            self.buf.write(rest)
            return ret

        while size > self.buf.tell():
            data = self.reader.read(size)  # changed to size from 1024
            if not data:
                break
            self.buf.write(data)

        data = self.buf.getvalue()
        ret, rest = data[:size], data[size:]
        self.buf = BytesIO()
        self.buf.write(rest)
        return ret

    gunicorn.http.body.Body.read = swift_read

    # swift_read passes the real size down, but gunicorn's LengthReader.read()
    # calls unreader.read() with no size, returning one recv(max_chunk=8192).
    # So a 64 KiB read still costs ~8 recvs plus redundant BytesIO copies that
    # eventlet's wsgi.Input avoids. Recv in bigger chunks; recv() returns
    # whatever is available, so this only coarsens granularity.
    # (SWIFT_UNREADER_CHUNK is a measurement override.)
    _unreader_chunk = int(os.environ.get('SWIFT_UNREADER_CHUNK', 65536))
    orig_su_init = SocketUnreader.__init__

    def swift_su_init(self, sock, max_chunk=_unreader_chunk):
        orig_su_init(self, sock, max_chunk=max_chunk)

    SocketUnreader.__init__ = swift_su_init

    # Copy of gunicorn.http.body.ChunkedReader.read that reads at most one
    # chunk per call, so a response can be sent without draining the body.
    def swift_chunked_reader_read(self, size):
        if not isinstance(size, int):
            raise TypeError("size must be an integer type")
        if size < 0:
            raise ValueError("Size must be positive.")
        if size == 0:
            return b""

        if self.parser and self.buf.tell() < size:
            try:
                self.buf.write(next(self.parser))
            except StopIteration:
                self.parser = None

        data = self.buf.getvalue()
        ret, rest = data[:size], data[size:]
        self.buf = BytesIO()
        self.buf.write(rest)
        return ret

    gunicorn.http.body.ChunkedReader.read = swift_chunked_reader_read

    # eventlet and gunicorn report a mid-chunked-body disconnect differently.
    # On a missing/garbage chunk-SIZE line eventlet raises ChunkReadError; on
    # EOF partway through chunk DATA it raises OSError. gunicorn raises
    # NoMoreData for both, and keepalive may close the socket from the poll
    # thread mid-read, so an OSError(EBADF) can surface instead of an empty
    # recv. Reimplement parse_chunked so the two cases raise distinct
    # exceptions (InvalidChunkSize for size, NoMoreData for data) and a socket
    # error is treated as EOF; the wrappers map these to eventlet's messages.
    def swift_parse_chunked(self, unreader):
        try:
            (size, rest) = self.parse_chunk_size(unreader)
        except _CHUNK_PARSE_ERRORS:
            raise
        except socket.timeout:
            # a read timeout is an OSError; re-raise so WatchdogTimeout maps it
            # to ChunkReadTimeout, not the disconnect below
            raise
        except (NoMoreData, OSError):
            # stream ended/failed before a chunk-size line: mirror eventlet
            # reading an empty line and failing int(b'', 16).
            raise InvalidChunkSize(b'')
        while size > 0:
            while size > len(rest):
                size -= len(rest)
                # Don't yield an empty leading chunk: swift's patched
                # ChunkedReader.read breaks after the first next() so an empty
                # yield would make it return b'' (a false clean EOF) before the
                # premature-EOF NoMoreData below could be raised.
                if rest:
                    yield rest
                try:
                    rest = unreader.read()
                except socket.timeout:
                    raise  # read timeout -> ChunkReadTimeout, not EOF
                except OSError:
                    rest = b''
                if not rest:
                    raise NoMoreData()
            yield rest[:size]
            # Remove \r\n after chunk
            rest = rest[size:]
            while len(rest) < 2:
                try:
                    new_data = unreader.read()
                except socket.timeout:
                    raise  # read timeout -> ChunkReadTimeout, not EOF
                except OSError:
                    new_data = b''
                if not new_data:
                    break
                rest += new_data
            if rest[:2] != b'\r\n':
                raise ChunkMissingTerminator(rest[:2])
            try:
                (size, rest) = self.parse_chunk_size(unreader, data=rest[2:])
            except _CHUNK_PARSE_ERRORS:
                raise
            except socket.timeout:
                raise  # read timeout -> ChunkReadTimeout, not EOF
            except (NoMoreData, OSError):
                raise NoMoreData()

    gunicorn.http.body.ChunkedReader.parse_chunked = swift_parse_chunked

    # eventlet returned 414 for an over-long request line and 400 for an
    # over-long header; gunicorn maps these to 400 and 431. Restore parity.
    orig_handle_error = ThreadWorker.handle_error

    def swift_handle_error(self, req, client, addr, exc):
        if isinstance(exc, LimitRequestLine):
            gunicorn.util.write_error(
                client, 414, "Request-URI Too Long", str(exc))
            return
        if isinstance(exc, LimitRequestHeaders):
            gunicorn.util.write_error(client, 400, "Bad Request", str(exc))
            return
        return orig_handle_error(self, req, client, addr, exc)

    ThreadWorker.handle_error = swift_handle_error

    # gunicorn's proxy_protocol="auto" (what True maps to) only *detects* a
    # PROXY preamble; a connection without one is served as ordinary HTTP.
    # eventlet's SwiftHttpProxiedProtocol instead *requires* it, making
    # require_proxy_protocol a real trust boundary (a direct client must not
    # bypass the proxy/TLS terminator). Restore that: reject a first request
    # with no preamble. Only the first request parses proxy protocol
    # (req_number == 1), so keepalive follow-ups are unaffected.
    orig_handle_pp = gunicorn.http.message.Request._handle_proxy_protocol

    def swift_handle_proxy_protocol(self, unreader, buf, mode):
        buf = orig_handle_pp(self, unreader, buf, mode)
        if self.proxy_protocol_info is None:
            peer = self.peer_addr[0] if isinstance(self.peer_addr, tuple) \
                else self.peer_addr
            raise ForbiddenProxyRequest(peer)
        return buf

    gunicorn.http.message.Request._handle_proxy_protocol = \
        swift_handle_proxy_protocol

    # Only after every patch above succeeded: a partial failure must not
    # leave the flag claiming the module is patched.
    _GUNICORN_PATCHED = True


def _map_chunk_read_error(err):
    """Translate a gunicorn chunked-stream parse error into the same
    ChunkReadError (message included) that eventlet's wsgi.Input raises for a
    client that disconnects mid-body, so swift readers report it identically.
    """
    if isinstance(err, InvalidChunkSize):
        # eventlet fails int(line, 16) on an empty/garbage chunk-size line
        return ChunkReadError(
            'invalid literal for int() with base 16: %r' % (err.data,))
    if isinstance(err, NoMoreData):
        return ChunkReadError(
            'unexpected end of file while parsing chunked data')
    return ChunkReadError(str(err))


class SwiftGunicornApp(gunicorn.app.base.BaseApplication):
    def __init__(self, load_app, build_cfg):
        self.load_app = load_app
        self.build_cfg = build_cfg
        super().__init__()

    def load_config(self):
        # Rebuild the full config on every (re)load. gunicorn's reload() calls
        # load_default_config() first, resetting bind to its default
        # 127.0.0.1:8000; a no-op here would lose our real bind/workers and
        # reloaded masters would collide trying to rebind 8000.
        self.cfg = self.build_cfg()

    def load(self):
        return self.load_app()


class _CountingInput:
    """Shared byte/EOF accounting for the wsgi.input wrappers below, so
    swift_default_headers can tell whether the body was fully drained.
    Subclasses feed each read()/readline() result through _account().
    """

    def _init_accounting(self):
        self.bytes_read = 0
        self.saw_eof = False

    def _account(self, data, size, whole_stream=False):
        if data:
            self.bytes_read += len(data)
        if whole_stream:
            # an unlimited read returns only after draining to EOF
            self.saw_eof = True
        elif not data and size != 0:
            # a nonzero read hitting b'' is EOF (read(0) returns b'' but isn't)
            self.saw_eof = True
        return data

    def __iter__(self):
        return self

    def __next__(self):
        # iterate via the counted readline() so iteration is accounted too
        line = self.readline()
        if not line:
            raise StopIteration
        return line

    def readlines(self, hint=-1):
        # honour a positive size hint like io.IOBase.readlines; else drain
        lines = []
        total = 0
        for line in self:
            lines.append(line)
            if hint is not None and hint > 0:
                total += len(line)
                if total >= hint:
                    break
        return lines


class _ChunkReadErrorInput(_CountingInput):
    """Thin wsgi.input wrapper that maps gunicorn's premature chunked-stream
    errors to swift's ChunkReadError on read()/readline(), delegating
    everything else to the wrapped input. Wraps every request not handled by
    ChunkedInput (i.e. without Expect: 100-continue); a no-op passthrough
    unless the body is a chunked stream that errors mid-read.
    """

    def __init__(self, inp):
        self._inp = inp
        self._init_accounting()

    def __getattr__(self, name):
        return getattr(self._inp, name)

    def read(self, size=-1):
        try:
            data = self._inp.read(size)
        except _CHUNK_PARSE_ERRORS + (NoMoreData,) as err:
            raise _map_chunk_read_error(err)
        whole = size is None or size < 0
        return self._account(data, size, whole_stream=whole)

    def readline(self, size=-1):
        try:
            data = self._inp.readline(size)
        except _CHUNK_PARSE_ERRORS + (NoMoreData,) as err:
            raise _map_chunk_read_error(err)
        return self._account(data, size)


class ChunkedInput(_CountingInput):
    """Wrapper around gunicorn.http.body.Body for chunked requests. Uses the
    same approach as in eventlet.wsgi.Input: throw a 100 Continue header
    into the HTTP stream.
    """

    def __init__(self, body, sock, req):
        self.body = body
        self.sock = sock
        self.req = req
        self.headers = []
        self.continue_sent = False
        self._init_accounting()

    def set_hundred_continue_response_headers(self, headers):
        self.headers = headers

    def send_hundred_continue_response(self):
        parts = [b'HTTP/1.1 100 Continue\r\n']
        for header in self.headers:
            parts.append(('%s: %s\r\n' % header).encode('latin-1'))
        parts.append(b'\r\n')
        self.sock.sendall(b''.join(parts))

        self.headers = []
        if self.continue_sent:
            # new body for the next phase: reset accounting so an undrained
            # later phase isn't treated as complete
            unreader = SocketUnreader(self.sock)
            self.body = Body(ChunkedReader(self.req, unreader))
            self._init_accounting()
        self.continue_sent = True

    def read(self, size=-1):
        if not self.continue_sent:
            self.send_hundred_continue_response()
        try:
            data = self.body.read(size)
        except _CHUNK_PARSE_ERRORS + (NoMoreData,) as err:
            raise _map_chunk_read_error(err)
        whole = size is None or size < 0
        return self._account(data, size, whole_stream=whole)

    def readline(self, size=-1):
        if not self.continue_sent:
            self.send_hundred_continue_response()
        try:
            data = self.body.readline(size)
        except _CHUNK_PARSE_ERRORS + (NoMoreData,) as err:
            raise _map_chunk_read_error(err)
        return self._account(data, size)


def _tune_malloc():
    """glibc's malloc dynamically shrinks its trim/mmap thresholds in
    multithreaded processes, making it return every large per-chunk buffer
    to the kernel on free and page-fault it back (re-zeroed) on the next
    allocation -- measured ~835 minor faults per EC fragment GET vs 2 in
    the single-threaded eventlet worker, costing ~40% object-tier CPU per
    byte and a large slice of request latency. Pin the thresholds (an
    explicit mallopt also disables the dynamic shrinking) so the arena
    recycles the buffers instead. Called in the master pre-fork; the
    settings are process state, so workers inherit them. Non-glibc
    platforms lack mallopt and are silently skipped.
    SWIFT_GTHREAD_NO_MALLOC_TUNE=1 disables (measurement).
    """
    if config_true_value(os.environ.get('SWIFT_GTHREAD_NO_MALLOC_TUNE')):
        return False
    try:
        import ctypes
        libc = ctypes.CDLL(None)
        m_trim_threshold, m_mmap_threshold = -1, -3
        return bool(libc.mallopt(m_trim_threshold, 128 * 1024 * 1024) and
                    libc.mallopt(m_mmap_threshold, 4 * 1024 * 1024))
    except (ImportError, OSError, AttributeError, TypeError):
        return False


def check_config_gunicorn(conf_path, app_section, *args, **kwargs):
    """
    Load and validate configuration for gunicorn mode. Mostly borrowed from
    swift.common.wsgi.check_config

    :param conf_path: Path to paste.deploy style configuration file/directory
    :param app_section: App name from conf file to load config from
    :returns: tuple of (conf, logger, global_conf)
    :raises ConfigFileError: if configuration is invalid
    """
    (conf, logger, log_name) = \
        _initrp(conf_path, app_section, *args, **kwargs)

    # optional nice/ionice priority scheduling
    utils.modify_priority(conf, logger)

    # servers_per_port (object-server only) listens on the local ring ports
    # rather than a single bind_port, so skip the bind_port check there -- as
    # the eventlet ServersPerPortStrategy does.
    servers_per_port = int(conf.get('servers_per_port', '0') or 0)
    if not (servers_per_port and app_section == 'object-server'):
        try:
            if not (1 <= int(conf['bind_port']) <= 2 ** 16 - 1):
                raise ValueError
        except (ValueError, KeyError, TypeError):
            error_msg = 'bind_port wasn\'t properly set in the config file. ' \
                        'It must be explicitly set to a valid port number.'
            logger.error(error_msg)
            raise ConfigFileError(error_msg)

    # Ensure the configuration and application can be loaded before
    # proceeding.
    global_conf = {'log_name': log_name}
    loadapp(conf_path, global_conf=global_conf)
    if 'global_conf_callback' in kwargs:
        kwargs['global_conf_callback'](conf, global_conf)

    # set utils.FALLOCATE_RESERVE if desired
    utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
        config_fallocate_value(conf.get('fallocate_reserve', '1%'))

    return conf, logger, global_conf


def common_config():
    """Common config used both by production server and unittest server """
    patch_gunicorn()
    cfg = Config()
    cfg.set('worker_class', 'gthread')

    # Allow headers with underscores through
    cfg.set('header_map', 'dangerous')

    # Defaults to 30 seconds, should be less than common.manager.KILL_WAIT
    cfg.set('graceful_timeout', 5)

    cfg.set('limit_request_fields', int(constraints.MAX_HEADER_COUNT * 1.6))
    # eventlet rejected a header line >= MAX_HEADER_SIZE (400) and a request
    # line >= MAX_REQUEST_LINE (414); gunicorn defaults to 4094 and rejects
    # with '>'. patch_gunicorn remaps the statuses; size the limits to match
    # eventlet's >= boundary. gunicorn measures the header field length with
    # its CRLF but the request line without, so the offsets differ (-1 / -3).
    cfg.set('limit_request_field_size', constraints.MAX_HEADER_SIZE - 1)
    cfg.set('limit_request_line', constraints.MAX_REQUEST_LINE - 3)

    return cfg


def _bind_str(ip, port):
    # gunicorn's address parser needs IPv6 hosts bracketed ([::1]:6200);
    # a bare "::1:6200" or ":::6200" fails to parse.
    if ip and ':' in ip:
        return '[%s]:%d' % (ip, int(port))
    return '%s:%d' % (ip, int(port))


def _check_can_bind(addr):
    # Bind the address gunicorn will bind (then release it), so a bind failure
    # is reported before capture_stdio() signals start-success to swift-init.
    # gunicorn binds with SO_REUSEADDR (not SO_REUSEPORT), so this matches its
    # semantics: an active listener on the same address makes bind() raise.
    host, port = gunicorn.util.parse_address(addr)
    family = socket.AF_INET6 if host and ':' in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((host, port))
    finally:
        sock.close()


def _listener_owned_by_session(port, sid):
    # Best-effort: is a LISTEN socket on `port` held by a process in session
    # `sid` (the reload target's manager + workers share its sid)? Returns True
    # when it is -- or when ownership can't be determined, so a reload is never
    # blocked by missing/unreadable /proc -- and False only when a listener is
    # positively owned by a different session.
    try:
        inodes = set()
        for proto in ('tcp', 'tcp6'):
            try:
                with open('/proc/net/' + proto) as fh:
                    next(fh, None)  # header
                    for line in fh:
                        f = line.split()
                        if len(f) > 9 and f[3] == '0A' and \
                                int(f[1].rsplit(':', 1)[1], 16) == port:
                            inodes.add('socket:[%s]' % f[9])
            except OSError:
                pass
        if not inodes:
            return True  # nothing we can see listening; let run-path catch it
        for pid in os.listdir('/proc'):
            if not pid.isdigit():
                continue
            try:
                if os.getsid(int(pid)) != sid:
                    continue
                fddir = '/proc/%s/fd' % pid
                for fd in os.listdir(fddir):
                    try:
                        if os.readlink(fddir + '/' + fd) in inodes:
                            return True
                    except OSError:
                        continue
            except OSError:
                continue
        return False
    except Exception:
        return True


def _check_binds_bindable(binds):
    # Probe each configured bind address for --test-config. On SIGHUP gunicorn
    # closes its current listeners before binding a changed address, so an
    # unbindable bind (a bind_ip not on this host, or a changed bind_port that
    # another process already owns) would pass validation and then destroy the
    # live service. EADDRINUSE is tolerated ONLY for the reload target's own
    # (unchanged) listener -- identified by session via SWIFT_RELOAD_OWNER_SID,
    # set by swift-reload; any other occupant fails validation.
    owner = os.environ.get('SWIFT_RELOAD_OWNER_SID')
    owner_sid = int(owner) if owner and owner.isdigit() else None
    for addr in (binds if isinstance(binds, list) else [binds]):
        try:
            _check_can_bind(addr)
        except OSError as err:
            if err.errno != errno.EADDRINUSE:
                raise
            if owner_sid is None:
                continue  # not a reload (no live server to own it); tolerate
            _, port = gunicorn.util.parse_address(addr)
            if not _listener_owned_by_session(port, owner_sid):
                raise  # in use by another process -> reload would break


def _resolve_worker_count(conf, logger):
    # Swift treats workers=0 as "run in the foreground, no fork" under eventlet
    # (a single in-process worker, used for profiling). gunicorn has no
    # equivalent: workers=0 makes its arbiter bind the socket but fork no
    # workers, so the port accepts connections that are never served. Coerce to
    # one worker so the server actually serves.
    workers = config_auto_int_value(conf.get('workers'), CPU_COUNT)
    if workers < 1:
        logger.warning(
            'workers=%s is not supported under gunicorn (no foreground/'
            'no-fork mode); running with 1 worker instead.', workers)
        workers = 1
    return workers


def _binds_and_workers(conf, app_section, logger):
    # (bind, workers) for a freshly read conf; bind is a list of "ip:port"
    # for servers_per_port, else a single "ip:port". The mode is derived
    # from the conf passed in -- not from startup state -- so a reload can
    # switch servers_per_port on or off.
    #
    # servers_per_port (object-server only) listens on every local ring port;
    # a plain server listens on a single bind_port. gunicorn's single arbiter
    # serves all bound sockets, so this restores listeners on every port but
    # does NOT give the per-port process isolation of the eventlet
    # ServersPerPortStrategy, and changed ring ports take effect on a reload
    # (SIGHUP) rather than the eventlet ring_check_interval poll.
    ip = conf.get('bind_ip', '0.0.0.0')
    spp = (app_section == 'object-server'
           and int(conf.get('servers_per_port', '0') or 0))
    if spp:
        ports = sorted(BindPortsCache(
            conf.get('swift_dir', '/etc/swift'),
            conf.get('ring_ip', ip)).all_bind_ports_for_node())
        return ([_bind_str(ip, p) for p in ports],
                spp * max(1, len(ports)))
    return (_bind_str(ip, int(conf['bind_port'])),
            _resolve_worker_count(conf, logger))


def run_wsgi(conf_path, app_section, *args, **kwargs):
    """
    Runs the server using gunicorn with gthread workers instead of eventlet.
    Partially borrowed from swift.common.wsgi.

    :param conf_path: Path to paste.deploy style configuration file/directory
    :param app_section: App name from conf file to load config from
    :param test_config: if True, load and validate config but do not run
    :returns: 0 if successful, nonzero otherwise
    """
    try:
        conf, logger, global_conf = check_config_gunicorn(
            conf_path, app_section, *args, **kwargs)
    except ConfigFileError as err:
        print(err)
        return 1

    # Load the WSGI application. load_app is re-invoked on every (re)load (see
    # SwiftGunicornApp.load) so a reload rebuilds the pipeline and re-registers
    # /info with the freshly read swift.conf constraints.
    allow_modify_pipeline = kwargs.get('allow_modify_pipeline', True)

    def load_app():
        # Reuse the global_conf built once in check_config_gunicorn: its
        # global_conf_callback creates resources that must be SHARED across
        # workers (e.g. the object-server's replication_semaphore, a
        # multiprocessing.Semaphore inherited by the forked workers).
        # Rebuilding it here would re-run the callback in every worker, giving
        # each its own semaphore and multiplying replication_concurrency by the
        # worker count. Reload still refreshes constraints + storage policies
        # via build_cfg.
        return loadapp(conf['__file__'], global_conf=global_conf,
                       allow_modify_pipeline=allow_modify_pipeline)

    # Configure gunicorn. build_cfg is re-invoked on every (re)load (see
    # SwiftGunicornApp.load_config), so re-read the conf file here: a reload
    # (SIGHUP) then picks up changed bind/workers/threads/TLS/user, not just
    # the reloaded app pipeline. reload_constraints() re-reads swift.conf.
    def build_cfg():
        global _CLIENT_TIMEOUT
        constraints.reload_constraints()
        # Also re-read storage policies from swift.conf so a reload picks up
        # policy changes, not just [swift-constraints] (reload_constraints
        # only reloads the latter).
        reload_storage_policies()
        rconf = appconfig(conf_path, name=app_section)
        # Bound the gthread request-read phase by client_timeout (read by the
        # patched TConn.init); gunicorn has no equivalent socket-read timeout.
        _CLIENT_TIMEOUT = float(rconf.get('client_timeout', 60))
        bind, workers = _binds_and_workers(rconf, app_section, logger)
        cfg = common_config()
        cfg.set('bind', bind)
        cfg.set('workers', workers)
        # Each gthread thread serves one request for its lifetime, so the
        # thread count caps concurrent (incl. slow/stalled) clients -- unlike
        # eventlet, where a blocked request only parks a cheap greenthread.
        # Too small a pool lets a few slow clients starve the rest; default
        # generously (conf overridable).
        cfg.set('threads', int(rconf.get('threads', 16)))
        cfg.set('keepalive', int(rconf.get('keepalive_timeout', 5) or 5))
        cfg.set('backlog', int(rconf.get('backlog', 4096)))
        cfg.set('loglevel', rconf.get('log_level', 'info').lower())
        cfg.set('timeout', 3600)  # Long -- Swift manages its own timeouts
        cert_file = rconf.get('cert_file')
        if cert_file:
            cfg.set('certfile', cert_file)
            cfg.set('keyfile', rconf.get('key_file'))
            # Build the SSLContext now, in the still-root arbiter, before any
            # worker forks and drops to user/group. gunicorn otherwise builds
            # it lazily in the worker AFTER the drop, so a root-only key (0600
            # root) would be unreadable and TLS would fail. Hand workers the
            # pre-built context via the ssl_context hook. Matches eventlet's
            # load-cert-before-drop flow; rebuilt as root on each reload, so
            # cert rotation keeps working.
            ssl_ctx = gunicorn.sock.ssl_context(cfg)

            def use_prebuilt_ssl(c, default_factory):
                return ssl_ctx
            cfg.set('ssl_context', use_prebuilt_ssl)
        # gunicorn's master binds the socket as root, then drops each worker to
        # user/group -- so privileged ports keep working, like the eventlet
        # bind-before-drop flow. (We deliberately do NOT drop_privileges() in
        # this master process.) initgroups mirrors drop_privileges()'s
        # supplementary-group handling. Only meaningful when started as root.
        if os.geteuid() == 0:
            pw = pwd.getpwnam(rconf.get('user', 'swift'))
            cfg.set('user', pw.pw_uid)
            cfg.set('group', pw.pw_gid)
            cfg.set('initgroups', True)
        # Honour require_proxy_protocol like the eventlet
        # SwiftHttpProxiedProtocol: require the PROXY preamble and reject a
        # connection without one (enforced by the _handle_proxy_protocol patch
        # in patch_gunicorn -- gunicorn's "auto" mode would only detect it).
        # eventlet did not restrict by source IP, so neither do we.
        if config_true_value(rconf.get('require_proxy_protocol', 'no')):
            cfg.set('proxy_protocol', True)
            cfg.set('proxy_allow_ips', '*')
        # Notify swift-reload once a worker has booted and loaded the app.
        # post_worker_init runs in each worker after the app is loaded and just
        # before it accepts, so the first to fire means a worker can serve;
        # address the master's pid-socket (the one swift-reload bound), and it
        # re-fires for new workers after a SIGHUP reload (which swift-reload
        # --wait depends on). systemd is handled separately: gunicorn's arbiter
        # already sd_notify's READY=1 from the master (the unit's main PID), so
        # drop NOTIFY_SOCKET here -- a worker's systemd datagram would be
        # dropped by NotifyAccess=main anyway (matches the eventlet workers,
        # which also pop NOTIFY_SOCKET so only the master notifies systemd).
        # KNOWN LIMITATION: gunicorn's arbiter sends that systemd READY=1 when
        # it has bound the socket but before the workers have booted, so under
        # Type=notify systemd may consider the unit ready slightly before a
        # worker can serve a request -- unlike eventlet, which notifies only
        # after the workers are up. swift-reload --wait is unaffected (it waits
        # on the per-pid socket fired from post_worker_init above).
        master_pid = os.getpid()

        def notify_ready(worker):
            os.environ.pop('NOTIFY_SOCKET', None)
            systemd_notify(logger=logger, pid=master_pid)
        cfg.set('post_worker_init', notify_ready)
        return cfg

    if kwargs.get('test_config'):
        # Validate gunicorn-specific configuration too (unknown settings, bad
        # TLS paths, a missing user, etc.), not just the paste config and
        # bind_port that check_config_gunicorn already covered.
        try:
            build_cfg()
            bind, _ = _binds_and_workers(conf, app_section, logger)
            _check_binds_bindable(bind)
        except Exception as err:
            print(err)
            return 1
        return 0

    # Do some daemonization process hygiene before running the server.
    clean_up_daemon_hygiene()

    # Pin glibc malloc thresholds before forking workers (see _tune_malloc).
    _tune_malloc()

    # Ensure TZ environment variable exists to avoid stat('/etc/localtime')
    # on some platforms. This locks in reported times to UTC.
    os.environ['TZ'] = 'UTC+0'
    time.tzset()

    bind, workers = _binds_and_workers(conf, app_section, logger)

    # Validate the bind before capture_stdio() closes stdout -- swift-init
    # reads that EOF as start-success, but gunicorn doesn't bind until run()
    # below, so a bind failure (e.g. address already in use) would otherwise be
    # reported only after we'd already signalled success. The app itself was
    # already loaded by check_config_gunicorn above.
    for addr in (bind if isinstance(bind, list) else [bind]):
        try:
            _check_can_bind(addr)
        except OSError as err:
            print('Unable to bind to %s: %s' % (addr, err))
            logger.error('Unable to bind to %s: %s', addr, err)
            return 1

    # Validate the full gunicorn config (version guard, TLS cert/key load,
    # storage policies, user/group) before capture_stdio closes stdout;
    # gunicorn rebuilds this config inside run() after capture_stdio, so a
    # failure there would otherwise surface only after we'd signalled success
    # (see the bind check above).
    try:
        build_cfg()
    except Exception as err:
        print('Invalid gunicorn configuration: %s' % err)
        logger.error('Invalid gunicorn configuration: %s', err)
        return 1

    # Redirect stdio to logger and close underlying file descriptors
    capture_stdio(logger)

    logger.notice('Starting gunicorn/gthread server on %s with %d workers',
                  bind, workers)

    SwiftGunicornApp(load_app, build_cfg).run()

    logger.notice('Exited (%s)', os.getpid())
    return 0


# In-process test servers, keyed by listening-socket fileno -> (worker,
# sock). Unlike eventlet greenthreads (interrupted by spawn().kill()),
# gunicorn ThreadWorkers keep running threads that must be stopped
# explicitly; spawn().kill() does so via the cooperative hook registered in
# server() (see register_kill_hook). The mapping also lets a test look up
# its own worker (e.g. ssync's rx-server connection-drain check).
_test_workers = {}


def server(sock, site, log=None, **kwargs):
    """Drop-in replacement for wsgi.server used in unittests"""
    cfg = common_config()

    # Size the test server's pool generously too (see common_config): one
    # thread deadlocks an SLO/DLO copy's self-subrequest, and a small pool
    # starves the in-process proxy/backends under a full test run (e.g. an
    # EC GET whose fragments stall on busy backend threads).
    cfg.set('threads', 16)
    # A stopped test server enters gunicorn's graceful shutdown, blocking up
    # to graceful_timeout (5s) for in-flight connections to drain. A
    # torn-down test server needn't wait; exit immediately so worker threads
    # don't linger and accumulate across the test run.
    cfg.set('graceful_timeout', 0)

    # eventlet's wsgi.server treats socket_timeout as the idle keep-alive
    # timeout; map it onto gunicorn's keepalive so the threading-mode test
    # server behaves the same (e.g. test_GET_pipeline).
    socket_timeout = kwargs.get('socket_timeout')
    if socket_timeout is not None:
        # gunicorn validates keepalive as an integer, but socket_timeout is
        # typically sub-second in tests; set it directly (bypassing the int
        # validator) so the idle timeout can be a float, like eventlet.
        cfg.settings['keepalive'].value = socket_timeout

    # Keep the Swift app logger so the worker can log idle-connection timeouts
    # to it, as eventlet's server does (see the murder_keepalived patch).
    swift_logger = getattr(site, 'logger', None)

    if not isinstance(log, Logger):
        log = Logger(cfg)

    worker = ThreadWorker(age=1, ppid=os.getppid(), sockets=[sock],
                          app=site, timeout=30, cfg=cfg, log=log)
    worker.swift_logger = swift_logger
    site.wsgi = lambda: site
    worker.init_signals = lambda: None
    # Register before the blocking serve loop so teardown can stop us.
    # Capture the fd once: teardown may close the socket before the finally
    # below, and fileno() on a closed socket returns -1.
    sock_fd = sock.fileno()
    _test_workers[sock_fd] = (worker, sock)
    # spawn().kill() (called by teardown_servers and the test helpers) stops
    # us via this cooperative hook -- the threading analogue of killing an
    # eventlet greenthread.
    register_kill_hook(lambda: _stop_worker(worker, sock))
    try:
        worker.init_process()
    finally:
        _test_workers.pop(sock_fd, None)


def _stop_worker(worker, sock):
    # Stop accepting and break the serve loop.
    worker.alive = False
    tpool = getattr(worker, 'tpool', None)
    if tpool is not None:
        try:
            tpool.shutdown(wait=False)
        except Exception:
            pass
    # Closing the listening socket unblocks the worker's poller immediately.
    try:
        sock.close()
    except Exception:
        pass
