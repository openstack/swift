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

import os
import re
import string
import time
from io import BytesIO
from urllib.parse import unquote

try:
    import multiprocessing
    CPU_COUNT = multiprocessing.cpu_count() or 1
except (ImportError, NotImplementedError):
    CPU_COUNT = 1

from swift.common import constraints, utils
from swift.common.wsgi import _initrp, loadapp, ConfigFileError
from swift.common.utils import capture_stdio, config_fallocate_value, \
    clean_up_daemon_hygiene, drop_privileges, systemd_notify, \
    config_auto_int_value

import gunicorn.util
import gunicorn.app.base
import gunicorn.http.message
import gunicorn.http.wsgi
import gunicorn.http.body
from gunicorn.config import Config
from gunicorn.glogging import Logger
from gunicorn.workers.gthread import ThreadWorker
from gunicorn.http.unreader import SocketUnreader
from gunicorn.http.body import Body, ChunkedReader

# Compatibility with eventlet.wsgi.MINIMUM_CHUNK_SIZE, referenced by
# EventletPlungerString in swift/obj/server.py for zero-copy sends
MINIMUM_CHUNK_SIZE = 4096


_GUNICORN_PATCHED = False


def patch_gunicorn():
    # Idempotent: repeated calls must not stack the wrapping patches below.
    global _GUNICORN_PATCHED
    if _GUNICORN_PATCHED:
        return

    # Prevent gunicorn from URL-decoding and let Swift handle this
    gunicorn.util.unquote_to_wsgi_str = \
        lambda s: unquote(s, encoding='latin-1')

    # Relax Gunicorn's strict RFC 9110 validation to pass functional tests.
    # Source: TestS3ApiObject::test_put_object_weird_metadata in
    # test/functional/s3api/test_object.py; additionally x80-\xff to match
    # testing
    TOKEN_SPECIALS = r'!"#$%&\'()*+-./<=>?@[\\]^`{|}~_'  # nosec: B105
    TOKEN_RE = re.compile(r"[%s0-9a-zA-Z\x80-\xff]+" % (
        re.escape(TOKEN_SPECIALS)))
    gunicorn.http.message.TOKEN_RE = TOKEN_RE
    gunicorn.http.wsgi.TOKEN_RE = TOKEN_RE

    # Gunicorn uses [a-z#] to reject HTTP methods containing lowercase letters,
    # but Swift uses GETorHEAD quite a bit thus allowing lowercase chars
    gunicorn.http.message.METHOD_BADCHAR_RE = re.compile('[#]')

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

    # Add additional data to the request environ
    orig_default_environ = gunicorn.http.wsgi.default_environ

    def swift_default_environ(req, sock, cfg):
        # Check if this is a chunked request and if so, remove "Expect:
        # 100-continue" header before calling default_environ()
        headers = []
        chunked = False
        for name, value in req.headers:
            if name == 'EXPECT' and value.lower() == '100-continue':
                chunked = True
            else:
                headers.append((name, value))
        if chunked:
            req.headers = headers
            req._expected_100_continue = False  # Required for gunicorn >= 25

        env = orig_default_environ(req, sock, cfg)
        env.update({"headers_raw": req.headers})  # needed by s3api
        env['gunicorn.socket'] = sock
        env['wsgi.input'].get_socket = lambda: sock  # used by obj/server.py

        if chunked:
            env['wsgi.input'] = ChunkedInput(env['wsgi.input'], sock, req)

        return env

    gunicorn.http.wsgi.default_environ = swift_default_environ

    # Gunicorn uppercases header names via str.upper(), but this corrupts some
    # non-ASCII octets (e.g. \xf0 -> \xd0; see commit f5501111a in gunicorn).
    # Instead of copy/pasting gunicorn.http.message.Request.parse_headers and
    # modifying it, apply the original parse_headers first and then fix the
    # header names
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

    # Gunicorn rejects requests with both "Content-Length" and
    # "Transfer-Encoding" headers present, following RFC9112; Swift
    # uses this (e.g. Content-Length: 0 + chunked PUTs). Patch to strip
    # Content-Length if Transfer-Encoding is present to prevent rejection
    orig_set_body_reader = gunicorn.http.message.Message.set_body_reader

    def swift_set_body_reader(self):
        if any(n == "TRANSFER-ENCODING" for n, v in self.headers):
            self.headers = [
                (n, v) for n, v in self.headers if n != "CONTENT-LENGTH"]
        orig_set_body_reader(self)

    gunicorn.http.message.Message.set_body_reader = swift_set_body_reader

    # This is a copy from gunicorn.http.body.Body.read and only changes the
    # self.reader.read(1024) call to use the actual requested size. This avoids
    # useless copying of data and speeds up chunked transfers significantly.
    # Remove when https://github.com/benoitc/gunicorn/issues/2596 is closed
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

    # Only after every patch above succeeded: a partial failure must not
    # leave the flag claiming the module is patched.
    _GUNICORN_PATCHED = True


class SwiftGunicornApp(gunicorn.app.base.BaseApplication):
    def __init__(self, wsgi_app, cfg):
        super().__init__()
        self.wsgi_app = wsgi_app
        self.cfg = cfg

    def load_config(self):
        # self.cfg already set
        pass

    def load(self):
        return self.wsgi_app


class ChunkedInput:
    """Wrapper around gunicorn.http.body.Body for chunked requests. Uses the
    same approach as in eventlet.wsgi.Input: throw in a 100 Continue header
    into the HTTP stream.
    """

    def __init__(self, body, sock, req):
        self.body = body
        self.sock = sock
        self.req = req
        self.headers = []
        self.continue_sent = False

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
            # New chunked reader to read next stream from socket
            unreader = SocketUnreader(self.sock)
            self.body = Body(ChunkedReader(self.req, unreader))
        self.continue_sent = True

    def read(self, size=-1):
        if not self.continue_sent:
            self.send_hundred_continue_response()
        return self.body.read(size)

    def readline(self, size=-1):
        if not self.continue_sent:
            self.send_hundred_continue_response()
        return self.body.readline(size)


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

    try:
        # Quick sanity check
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

    # From swift/common/bufferedhttp.py
    # Gunicorn uses this to decide when/whether to send a 431.  Give it
    # some slack, so the app is more likely to get the chance to reject
    # with a 400 instead.
    cfg.set('limit_request_fields', int(constraints.MAX_HEADER_COUNT * 1.6))
    cfg.set('limit_request_field_size', int(constraints.MAX_HEADER_SIZE * 1.6))

    return cfg


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

    if kwargs.get('test_config'):
        return 0

    cfg = common_config()

    # Do some daemonization process hygiene before running the server.
    clean_up_daemon_hygiene()

    # Ensure TZ environment variable exists to avoid stat('/etc/localtime')
    # on some platforms. This locks in reported times to UTC.
    os.environ['TZ'] = 'UTC+0'
    time.tzset()

    # Load the WSGI application
    allow_modify_pipeline = kwargs.get('allow_modify_pipeline', True)
    app = loadapp(conf['__file__'], global_conf=global_conf,
                  allow_modify_pipeline=allow_modify_pipeline)

    # Extract server configuration
    bind_ip = conf.get('bind_ip', '0.0.0.0')
    bind_port = int(conf['bind_port'])
    workers = config_auto_int_value(conf.get('workers'), CPU_COUNT)

    # Drop privileges before starting.
    user = conf.get('user', 'swift')
    drop_privileges(user)

    # Redirect stdio to logger and close underlying file descriptors
    capture_stdio(logger)

    # Signal systemd that we're ready
    systemd_notify(logger=logger)

    logger.notice('Starting gunicorn/gthread server on %s:%d with %d workers',
                  bind_ip, bind_port, workers)

    # Configure and run gunicorn
    cfg.set('bind', '%s:%s' % (bind_ip, bind_port))
    cfg.set('workers', workers)
    cfg.set('threads', int(conf.get('threads', 4)))
    cfg.set('keepalive', int(conf.get('keepalive_timeout', 5) or 5))
    cfg.set('backlog', int(conf.get('backlog', 4096)))
    cfg.set('loglevel', conf.get('log_level', 'info').lower())
    cfg.set('timeout', 3600)  # Long -- Swift manages its own timeouts
    cfg.set('keyfile', conf.get('key_file'))
    cfg.set('certfile', conf.get('cert_file'))

    SwiftGunicornApp(app, cfg).run()

    logger.notice('Exited (%s)', os.getpid())
    return 0


def server(sock, site, log=None, **kwargs):
    """Drop-in replacement for wsgi.server used in unittests"""
    cfg = common_config()

    # SLO/DLO copy need two threads in in-process func tests
    cfg.set('threads', 2)

    if not isinstance(log, Logger):
        log = Logger(cfg)

    worker = ThreadWorker(age=1, ppid=os.getppid(), sockets=[sock],
                          app=site, timeout=30, cfg=cfg, log=log)
    site.wsgi = lambda: site
    worker.init_signals = lambda: None
    worker.init_process()
