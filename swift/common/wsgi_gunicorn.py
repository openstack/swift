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
import functools
import os
import signal
import re
import select
import socket
import string
import sys
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
import gunicorn.arbiter
from gunicorn import debug
import gunicorn.http.message
import gunicorn.http.wsgi
import gunicorn.http.body
from gunicorn.config import Config
from gunicorn.glogging import Logger
from gunicorn.workers.gthread import ThreadWorker, TConn
# gunicorn 25.2.0 added _DEFER. patch_gunicorn() reports an older gunicorn
# with a RuntimeError that names the missing attributes.
try:
    from gunicorn.workers.gthread import _DEFER
except ImportError:
    _DEFER = None
from gunicorn.http.unreader import SocketUnreader
from gunicorn.http.body import Body, ChunkedReader
from gunicorn.http.errors import (
    NoMoreData, ChunkMissingTerminator, InvalidChunkSize,
    LimitRequestLine, LimitRequestHeaders, ForbiddenProxyRequest)
# Deliberate chunk-parse rejections (all IOError/OSError subclasses, so they
# must be re-raised before the EOF/socket-error handling below catches them).
# InvalidChunkExtension was added in gunicorn 26.0.0; on our >=25.2.0 floor
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
        (ThreadWorker, 'enqueue_req'),
        (ThreadWorker, 'finish_request'),
        (TConn, 'init'),
    ]
    _missing = ['%s.%s' % (cls.__name__, attr)
                for cls, attr in _required if not hasattr(cls, attr)]
    if _DEFER is None:
        _missing.append('gthread._DEFER')
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
        # Keep every header for the app. An S3 client can include Expect in
        # the signed headers. Remove Expect: 100-continue from the list that
        # gunicorn uses, or gunicorn sends the 100 Continue response itself.
        # Swift sends it later, from ChunkedInput.
        headers_raw = req.headers
        headers = []
        expect_value = None
        for name, value in headers_raw:
            if name == 'EXPECT' and value.lower() == '100-continue':
                expect_value = value
            else:
                headers.append((name, value))
        expect_continue = expect_value is not None
        if expect_continue:
            req.headers = headers
            req._expected_100_continue = False  # Required for gunicorn >= 25

        env = orig_default_environ(req, sock, cfg)
        env['headers_raw'] = headers_raw  # needed by s3api
        if expect_continue:
            env['HTTP_EXPECT'] = expect_value
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
        if not self.alive:
            # Draining (see swift_run below). The stock drain loop never
            # calls notify(), so the arbiter would murder a long drain.
            # Wake once per second to notify and to reap idle keepalives.
            self.notify()
            timeout = min(timeout, 1.0)
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

    # eventlet drained a replaced worker until its last request completed;
    # gthread's run() stops the drain at cfg.graceful_timeout. Raise the
    # deadline in the worker's post-fork copy of the config only: the
    # arbiter's copy keeps graceful_timeout, so a stop still ends in
    # SIGKILL after it. Zero (the unittest server) keeps its no-drain
    # meaning.
    _DRAIN_UNBOUNDED = 10 ** 9  # seconds

    orig_run = ThreadWorker.run

    def swift_run(self):
        if self.cfg.graceful_timeout > 0:
            self.cfg.set('graceful_timeout', _DRAIN_UNBOUNDED)
        return orig_run(self)

    ThreadWorker.run = swift_run

    # Every request ends by deferring finish_request() to the main thread:
    # the worker thread writes a byte to the wake-up pipe, the poller wakes,
    # takes the GIL and runs it. But only two of finish_request's three
    # outcomes actually need the main thread -- the ones that hand the
    # connection back to the poller (keepalive, or a new connection with no
    # data yet). The third just closes the socket and decrements a counter,
    # which the worker thread can do itself.
    #
    # That third case is the common one here: Swift's proxy opens a fresh
    # backend connection per request and closes it, so backend servers take
    # the close path on essentially every request and pay a pipe write, a
    # poller wake and an epoll re-registration for nothing.
    #
    # Waking the main thread is also what makes it expensive out of
    # proportion to the syscalls: a second runnable thread turns each of the
    # ~50 GIL releases in a request into a potential futex handoff.
    #
    # Closing on the worker thread has a second benefit. util.close_graceful()
    # sends FIN then blocks reading until the peer closes or 2s elapse; on the
    # main thread that stalls the single accept/dispatch loop for every other
    # connection, which is why server-side closes (keepalive_timeout = 0) are
    # so costly today.
    def swift_enqueue_req(self, conn):
        fs = self.tpool.submit(self.handle, conn)

        def done(fut):
            # Runs on the worker thread that finished the request.
            needs_poller = False
            if self.alive and not fut.cancelled() and fut.exception() is None:
                result = fut.result()
                needs_poller = result is _DEFER or bool(result)
            if needs_poller:
                self.method_queue.defer(self.finish_request, conn, fut)
            else:
                # same call, just not via the main thread; it re-reads the
                # future and takes its close/error branch
                self.finish_request(conn, fut)

        fs.add_done_callback(done)

    ThreadWorker.enqueue_req = swift_enqueue_req

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


class TopologyChanged(Exception):
    """servers_per_port was turned on or off in the config file. How many
    processes run and which sockets they bind is settled at startup, so this
    needs a restart rather than a reload.
    """


class _SwiftArbiter(gunicorn.arbiter.Arbiter):
    def reload(self):
        # Arbiter.reload() asks the app for its new config before it touches
        # anything else, so refusing there and catching it here leaves the
        # running server alone. Returning from SwiftGunicornApp.reload()
        # would not: the arbiter would carry on and replace every worker.
        try:
            super().reload()
        except TopologyChanged as err:
            self.log.error('Ignoring reload: %s', err)
            self.app.report_failure(err)


class SwiftGunicornApp(gunicorn.app.base.BaseApplication):
    def __init__(self, load_app, build_cfg, logger, report_failure=None):
        self.load_app = load_app
        self.build_cfg = build_cfg
        self.swift_logger = logger
        # swift-reload waits for a readiness a refused reload never sends,
        # so say so instead of letting it sit until its timeout
        self.report_failure = report_failure or (lambda reason: None)
        super().__init__()

    def load_config(self):
        # BaseApplication calls this only while creating the app. Keep a bad
        # initial configuration fatal, rather than starting a broken service.
        self.cfg = self.build_cfg()

    def reload(self):
        # BaseApplication.reload() first replaces self.cfg with Gunicorn's
        # defaults. Build the replacement before discarding the live config so
        # a bad edit on SIGHUP leaves the running listener and worker settings
        # intact instead of making the arbiter rebind its default address.
        try:
            cfg = self.build_cfg()
        except TopologyChanged:
            raise               # _SwiftArbiter.reload() stops the reload
        except Exception as err:
            self.swift_logger.exception(
                'Ignoring invalid configuration during Gunicorn reload')
            self.report_failure(err)
            return
        self.cfg = cfg
        if self.cfg.spew:
            debug.spew()

    def load(self):
        return self.load_app()

    def run(self):
        # as BaseApplication.run(), but with the arbiter that can refuse a
        # reload outright
        try:
            _SwiftArbiter(self).run()
        except RuntimeError as err:
            print('\nError: %s\n' % err, file=sys.stderr)
            sys.stderr.flush()
            sys.exit(1)


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

    # Bounds the arbiter's stop() path: SIGTERM, this window, SIGKILL. Keep
    # it below common.manager.KILL_WAIT. A worker replaced by a reload is
    # not bound by it (see swift_run in patch_gunicorn).
    cfg.set('graceful_timeout', 5)

    _set_request_limits(cfg)

    return cfg


def _set_request_limits(cfg):
    """Size the request limits from the current constraints. Re-applied once
    a reload has read new ones out of swift.conf.
    """
    cfg.set('limit_request_fields', int(constraints.MAX_HEADER_COUNT * 1.6))
    # eventlet rejected a header line >= MAX_HEADER_SIZE (400) and a request
    # line >= MAX_REQUEST_LINE (414); gunicorn defaults to 4094 and rejects
    # with '>'. patch_gunicorn remaps the statuses; size the limits to match
    # eventlet's >= boundary. gunicorn measures the header field length with
    # its CRLF but the request line without, so the offsets differ (-1 / -3).
    cfg.set('limit_request_field_size', constraints.MAX_HEADER_SIZE - 1)
    cfg.set('limit_request_line', constraints.MAX_REQUEST_LINE - 3)


_ARBITER_RETRY_MAX = 60.0

# marks a refused reload on an arbiter's report pipe
_REPORT_REFUSED = b'refused:'


def _one_line(reason):
    """Reports are newline framed, so a reason has to fit on one line."""
    text = str(reason).encode('utf8', 'replace')
    return b' '.join(text.split()) + b'\n'


def _notify_reload_refused(logger, pid, reason):
    """Tell swift-reload the reload did not happen. Without this it waits
    for a readiness that is never coming, and times out minutes later.
    """
    systemd_notify(logger=logger, pid=pid,
                   msg=b'ERRNO=%d\nSTATUS=%s' % (
                       errno.EINVAL, _one_line(reason).strip()))


def _supervise_per_port(get_desired, logger, run_one_port, on_ready,
                        on_failure, ring_check_interval=15.0):
    """Run one gunicorn arbiter per port and keep the set matching the ring.

    Each arbiter binds a single socket, so a wedged disk can only stall its
    own port's workers -- the isolation eventlet gets by forking per port,
    with no gunicorn internals patched.

    :param get_desired: callable returning (enabled, ports) to serve now
    :param run_one_port: callable(port, ready_fd) run in the child
    :param on_failure: called with a reason when a reload is refused
    :param on_ready: called once every wanted arbiter has reported ready,
                     and again after each reload
    :param ring_check_interval: seconds between ring re-reads, as the
                                eventlet ServersPerPortStrategy polled
    """
    children = {}                      # pid -> port
    ready_fds = {}                     # pid -> read end of its ready pipe
    ready_gen = {}                     # pid -> generation its report must be
    ready_buf = {}                     # pid -> unparsed bytes from its pipe
    reported = set()                   # pids that have reported ready
    desired = set()                    # ports we should be serving
    retry_at = {}                      # port -> monotonic time to retry
    failures = {}                      # port -> consecutive failed starts
    stop_signal = []
    last_ring_check = [time.monotonic()]  # a list, so closures can update it
    reload_signals = []
    passthrough_signals = []

    # Signals only set a flag; PEP 475 retries an interrupted wait or read,
    # so the loop is woken through this pipe instead.
    wake_r, wake_w = os.pipe()
    os.set_blocking(wake_r, False)
    os.set_blocking(wake_w, False)
    old_wakeup_fd = signal.set_wakeup_fd(wake_w)

    def note_stop(signum, _frame):
        stop_signal.append(signum)

    def note_reload(signum, _frame):
        reload_signals.append(signum)

    def note_passthrough(signum, _frame):
        passthrough_signals.append(signum)

    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
        signal.signal(signum, note_stop)
    for signum in (signal.SIGHUP, signal.SIGUSR1):
        signal.signal(signum, note_reload)
    # TTIN/TTOU used to reach a gunicorn arbiter directly; keep them working
    for signum in (signal.SIGTTIN, signal.SIGTTOU):
        signal.signal(signum, note_passthrough)

    def forward(signum):
        for pid in list(children):
            try:
                os.kill(pid, signum)
            except OSError:
                pass

    def drop(pid):
        children.pop(pid, None)
        reported.discard(pid)
        ready_gen.pop(pid, None)
        ready_buf.pop(pid, None)
        fd = ready_fds.pop(pid, None)
        if fd is not None:
            try:
                os.close(fd)
            except OSError:
                pass

    def spawn(port):
        read_fd, write_fd = os.pipe()
        try:
            pid = os.fork()
        except OSError:
            os.close(read_fd)
            os.close(write_fd)
            logger.exception('Could not fork an arbiter for port %d', port)
            note_failure(port)
            return
        if pid == 0:
            os.close(read_fd)
            signal.set_wakeup_fd(-1)
            # the arbiter installs its own handlers; drop the parent's
            for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT,
                           signal.SIGHUP, signal.SIGUSR1, signal.SIGTTIN,
                           signal.SIGTTOU):
                signal.signal(signum, signal.SIG_DFL)
            status = 1
            try:
                status = run_one_port(port, write_fd) or 0
            finally:
                os._exit(status)
        os.close(write_fd)
        os.set_blocking(read_fd, False)
        children[pid] = port
        ready_fds[pid] = read_fd
        ready_gen[pid] = 1                 # its first config counts as ready
        logger.notice('Started arbiter for port %d (PID %d)', port, pid)

    def note_failure(port):
        """Back off a port that keeps dying, so a permanently broken config
        cannot become a fork/exit loop.
        """
        failures[port] = failures.get(port, 0) + 1
        # cap the exponent too: 2.0 ** 1024 raises OverflowError, and a
        # port can fail that often in a day at the 60s ceiling
        delay = min(2.0 ** min(failures[port] - 1, 16), _ARBITER_RETRY_MAX)
        retry_at[port] = time.monotonic() + delay
        logger.error('Arbiter for port %d failed %d time(s); next try in %ss',
                     port, failures[port], delay)

    def apply_desired():
        """Move towards `desired`. A port that cannot start yet is retried
        later, and readiness waits until every wanted port is up.
        """
        for pid, port in list(children.items()):
            if port not in desired:
                logger.notice('Port %d left the ring; stopping PID %d',
                              port, pid)
                try:
                    os.kill(pid, signal.SIGTERM)
                except OSError:
                    pass
                drop(pid)
        now = time.monotonic()
        for port in sorted(desired - set(children.values())):
            if retry_at.get(port, 0) > now:
                continue
            spawn(port)

    def next_timeout():
        """Sleep only until the soonest deadline: a port retry, the next
        ring check, or the 1s housekeeping tick.
        """
        deadlines = [t - time.monotonic() for port, t in retry_at.items()
                     if port in desired
                     and port not in set(children.values())]
        deadlines.append(
            last_ring_check[0] + ring_check_interval - time.monotonic())
        deadlines.append(1.0)
        return max(0.0, min(deadlines))

    def refresh_desired():
        """Re-read the ring. Returns a reason when the reload is refused,
        else None.
        """
        enabled, ports = get_desired()
        if not enabled:
            reason = ('servers_per_port cannot be turned off by a reload; '
                      'restart the server to change topology')
            logger.error(reason)
            return reason
        desired.clear()
        desired.update(ports)
        return None

    def refusal():
        """Why this SIGHUP is refused, or None to go ahead. A refused
        reload keeps the running arbiters rather than half-applying a change
        they should not see.
        """
        try:
            return refresh_desired()
        except Exception as err:
            logger.exception('Ignoring failed reload; keeping the running '
                             'arbiters')
            return 'could not read the new configuration: %s' % err

    def poll_desired():
        """Re-read the ring, as eventlet polled at ring_check_interval.
        Nobody asked for a reload, so a bad read is not a refusal: keep
        the current set and retry next interval. A topology flip on disk
        waits for a restart or an explicit reload.
        """
        try:
            enabled, ports = get_desired()
        except Exception:
            logger.exception('Ignoring failed ring check; keeping the '
                             'running arbiters')
            return
        if not enabled:
            return
        if set(ports) != desired:
            logger.notice('Ring check changed the port set to %s',
                          sorted(ports))
            desired.clear()
            desired.update(ports)

    def arm_reports():
        """Ask every arbiter for a fresh report. Each one reloads exactly
        once per SIGHUP, so a worker still starting up under the old config
        reports the old generation and no longer counts.
        """
        for pid in children:
            ready_gen[pid] = ready_gen.get(pid, 1) + 1

    def collect_ready():
        for pid, fd in list(ready_fds.items()):
            while True:
                try:
                    data = os.read(fd, 64)
                except BlockingIOError:
                    break
                except OSError:
                    data = b''
                if not data:
                    break
                ready_buf[pid] = ready_buf.get(pid, b'') + data
            # reports are newline terminated; hold on to any partial tail
            done, _, ready_buf[pid] = ready_buf.get(pid, b'').rpartition(b'\n')
            for line in done.split(b'\n'):
                if line.startswith(_REPORT_REFUSED):
                    on_failure('port %s: %s' % (
                        children.get(pid),
                        line[len(_REPORT_REFUSED):].decode('utf8', 'replace')))
                    continue
                try:
                    generation = int(line)
                except ValueError:
                    continue
                if generation >= ready_gen.get(pid, 1):
                    reported.add(pid)

    def reap():
        while True:
            try:
                pid, status = os.waitpid(-1, os.WNOHANG)
            except ChildProcessError:
                return
            if pid == 0:
                return
            port = children.get(pid)
            was_ready = pid in reported
            drop(pid)
            if port is None or stop_signal:
                continue
            logger.error('Arbiter for port %d (PID %d) exited with %s',
                         port, pid, status)
            if was_ready:
                failures.pop(port, None)      # it had been serving; retry now
                retry_at.pop(port, None)
            else:
                note_failure(port)

    def shut_down(signum):
        forward(signum)
        while children:
            try:
                pid, _status = os.waitpid(-1, 0)
            except ChildProcessError:
                break
            except OSError as err:
                if err.errno == errno.EINTR:
                    continue
                raise
            drop(pid)

    try:
        refresh_desired()
        apply_desired()
        notified = False
        while not stop_signal:
            try:
                select.select([wake_r] + list(ready_fds.values()), [], [],
                              next_timeout())
            except OSError as err:
                if err.errno != errno.EINTR:
                    raise
            try:
                while os.read(wake_r, 64):
                    pass
            except (BlockingIOError, OSError):
                pass
            while passthrough_signals:
                forward(passthrough_signals.pop(0))
            hangup = False
            while reload_signals:
                signum = reload_signals.pop(0)
                if signum == signal.SIGHUP:
                    # Signals do not queue, so a child can see one delivery
                    # for a burst of them. Reload once for the lot, or the
                    # supervisor waits for a generation no arbiter reaches.
                    hangup = True
                else:
                    forward(signum)     # SIGUSR1 just reopens logs
            if hangup:
                refused = refusal()
                last_ring_check[0] = time.monotonic()
                if refused:
                    on_failure(refused)
                else:
                    arm_reports()
                    reported.clear()
                    notified = False
                    forward(signal.SIGHUP)
            elif time.monotonic() - last_ring_check[0] >= ring_check_interval:
                last_ring_check[0] = time.monotonic()
                poll_desired()
            reap()
            if set(children.values()) != desired:
                apply_desired()
            collect_ready()
            if (not notified and set(children.values()) == desired
                    and reported >= set(children)):
                on_ready()              # an empty topology is ready too
                notified = True
        shut_down(stop_signal[0])
    except BaseException:
        # never leave arbiters running with nothing supervising them
        shut_down(signal.SIGTERM)
        raise
    finally:
        signal.set_wakeup_fd(old_wakeup_fd)
        for pid in list(ready_fds):
            drop(pid)
        for fd in (wake_r, wake_w):
            try:
                os.close(fd)
            except OSError:
                pass
    return 0


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


def _servers_per_port_enabled(conf, app_section):
    """True when this server should run one arbiter per ring port."""
    return (app_section == 'object-server'
            and bool(int(conf.get('servers_per_port', '0') or 0)))


def _servers_per_port_ports(conf, app_section, memo=None):
    """(enabled, sorted local ring ports). The ports may legitimately be
    empty -- this node may have none in the ring -- which is not the same as
    the mode being off.

    :param memo: dict that keeps the BindPortsCache between calls, so a
                 poll re-reads only a changed ring. Rebuilt when swift_dir
                 or ring_ip changes.
    """
    if not _servers_per_port_enabled(conf, app_section):
        return False, []
    ip = conf.get('bind_ip', '0.0.0.0')
    key = (conf.get('swift_dir', '/etc/swift'), conf.get('ring_ip', ip))
    if memo is None:
        cache = BindPortsCache(*key)
    else:
        if memo.get('key') != key:
            memo['key'] = key
            memo['cache'] = BindPortsCache(*key)
        cache = memo['cache']
    return True, sorted(cache.all_bind_ports_for_node())


def _binds_and_workers(conf, app_section, logger):
    # (bind, workers) for a freshly read conf; bind is a list of "ip:port"
    # for servers_per_port, else a single "ip:port". The mode is derived
    # from the conf passed in -- not from startup state -- so a reload can
    # switch servers_per_port on or off.
    #
    # servers_per_port (object-server only) listens on every local ring port;
    # a plain server listens on a single bind_port. At run time the per-port
    # supervisor owns that port set (see _supervise_per_port); this function's
    # servers_per_port branch only feeds the startup bind pre-check and
    # --test-config.
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

    # The process topology is fixed at startup: one arbiter per port, or a
    # single arbiter. build_cfg rejects a reload that flips it.
    started_spp = _servers_per_port_enabled(conf, app_section)

    # Configure gunicorn. build_cfg is re-invoked on every (re)load (see
    # SwiftGunicornApp.load_config), so re-read the conf file here: a reload
    # (SIGHUP) then picks up changed bind/workers/threads/TLS/user, not just
    # the reloaded app pipeline. reload_constraints() re-reads swift.conf.
    generation = [0]

    def build_cfg(port=None, ready_fd=None):
        global _CLIENT_TIMEOUT
        rconf = appconfig(conf_path, name=app_section)
        # Bound the gthread request-read phase by client_timeout (read by the
        # patched TConn.init); gunicorn has no equivalent socket-read timeout.
        client_timeout = float(rconf.get('client_timeout', 60))
        if port is None:
            if _servers_per_port_enabled(rconf, app_section) != started_spp:
                # a single arbiter would bind every ring port and hand them
                # all to every worker, so the mode cannot be switched by a
                # reload; keep the running config (SwiftGunicornApp.reload)
                raise TopologyChanged(
                    'servers_per_port cannot be turned on or off by a '
                    'reload; restart the server to change topology')
            bind, workers = _binds_and_workers(rconf, app_section, logger)
        else:
            # one arbiter of a servers_per_port set: only its own socket
            bind = _bind_str(rconf.get('bind_ip', '0.0.0.0'), port)
            workers = max(1, int(rconf.get('servers_per_port', '0') or 0))
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
            if ready_fd is None:
                systemd_notify(logger=logger, pid=master_pid)
                return
            # a per-port arbiter is not the service's main pid: tell the
            # supervisor, which notifies systemd once every port is up
            try:
                os.write(ready_fd, b'%d\n' % generation[0])
            except OSError:
                pass
        cfg.set('post_worker_init', notify_ready)

        # Everything above only built this config, so a file the server
        # refuses leaves the running one alone. It is known good now, so
        # apply the parts of it that live outside the config.
        constraints.reload_constraints()
        # Storage policies come from swift.conf too, and reload_constraints()
        # only reloads [swift-constraints].
        reload_storage_policies()
        _set_request_limits(cfg)
        _CLIENT_TIMEOUT = client_timeout
        # A config that raised is not a new generation: gunicorn respawns
        # the workers from the one it kept, and they must not look like the
        # reload being waited for.
        generation[0] += 1
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

    # servers_per_port: one arbiter per port, each bound to a single socket,
    # so a wedged disk stalls only its own port. A single arbiter would hand
    # every worker every listener.
    if _servers_per_port_enabled(conf, app_section):
        supervisor_pid = os.getpid()

        ports_memo = {}

        def get_desired():
            # called on SIGHUP and on the ring_check_interval poll; the
            # memo keeps the BindPortsCache across polls
            return _servers_per_port_ports(
                appconfig(conf_path, name=app_section), app_section,
                ports_memo)

        def run_one_port(port, ready_fd):
            # this arbiter counts its own reloads; the config built above in
            # the supervisor to validate the file is not one of them
            generation[0] = 0

            def refused(reason):
                # not the service pid: report up to the supervisor, which
                # holds the socket swift-reload is listening on
                try:
                    os.write(ready_fd, _REPORT_REFUSED + _one_line(reason))
                except OSError:
                    pass

            return SwiftGunicornApp(
                load_app, functools.partial(build_cfg, port, ready_fd),
                logger, refused).run()

        def all_ready():
            systemd_notify(logger=logger, pid=supervisor_pid)

        def reload_refused(reason):
            _notify_reload_refused(logger, supervisor_pid, reason)

        _supervise_per_port(get_desired, logger, run_one_port, all_ready,
                            reload_refused,
                            float(conf.get('ring_check_interval', 15)))
    else:
        service_pid = os.getpid()

        def reload_refused(reason):
            _notify_reload_refused(logger, service_pid, reason)

        SwiftGunicornApp(load_app, build_cfg, logger, reload_refused).run()

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
    # A torn-down test server must not wait for a drain (swift_run leaves
    # zero alone); exit at once so worker threads do not linger and
    # accumulate across the test run.
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
