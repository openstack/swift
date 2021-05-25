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

"""WSGI tools for use with swift."""

from __future__ import print_function

import errno
import fcntl
import os
import signal
from swift import gettext_ as _
import sys
from textwrap import dedent
import time

import eventlet
import eventlet.debug
from eventlet import greenio, GreenPool, sleep, wsgi, listen, Timeout, \
    websocket
from paste.deploy import loadwsgi
from eventlet.green import socket, ssl, os as green_os
from io import BytesIO

import six
from six import StringIO

from swift.common import utils, constraints
from swift.common.storage_policy import BindPortsCache
from swift.common.swob import Request, wsgi_quote, wsgi_unquote, \
    wsgi_quote_plus, wsgi_unquote_plus, wsgi_to_bytes, bytes_to_wsgi
from swift.common.utils import capture_stdio, disable_fallocate, \
    drop_privileges, get_logger, NullLogger, config_true_value, \
    validate_configuration, get_hub, config_auto_int_value, \
    reiterate, clean_up_daemon_hygiene, systemd_notify, NicerInterpolation

SIGNUM_TO_NAME = {getattr(signal, n): n for n in dir(signal)
                  if n.startswith('SIG') and '_' not in n}
NOTIFY_FD_ENV_KEY = '__SWIFT_SERVER_NOTIFY_FD'

# Set maximum line size of message headers to be accepted.
wsgi.MAX_HEADER_LINE = constraints.MAX_HEADER_SIZE

try:
    import multiprocessing
    CPU_COUNT = multiprocessing.cpu_count() or 1
except (ImportError, NotImplementedError):
    CPU_COUNT = 1


class NamedConfigLoader(loadwsgi.ConfigLoader):
    """
    Patch paste.deploy's ConfigLoader so each context object will know what
    config section it came from.
    """

    def get_context(self, object_type, name=None, global_conf=None):
        if not six.PY2:
            self.parser._interpolation = NicerInterpolation()
        context = super(NamedConfigLoader, self).get_context(
            object_type, name=name, global_conf=global_conf)
        context.name = name
        context.local_conf['__name__'] = name
        return context


loadwsgi.ConfigLoader = NamedConfigLoader


class ConfigDirLoader(NamedConfigLoader):
    """
    Read configuration from multiple files under the given path.
    """

    def __init__(self, conf_dir):
        # parent class uses filename attribute when building error messages
        self.filename = conf_dir = conf_dir.strip()
        defaults = {
            'here': os.path.normpath(os.path.abspath(conf_dir)),
            '__file__': os.path.abspath(conf_dir)
        }
        self.parser = loadwsgi.NicerConfigParser(conf_dir, defaults=defaults)
        self.parser.optionxform = str  # Don't lower-case keys
        utils.read_conf_dir(self.parser, conf_dir)


def _loadconfigdir(object_type, uri, path, name, relative_to, global_conf):
    if relative_to:
        path = os.path.normpath(os.path.join(relative_to, path))
    loader = ConfigDirLoader(path)
    if global_conf:
        loader.update_defaults(global_conf, overwrite=False)
    return loader.get_context(object_type, name, global_conf)


# add config_dir parsing to paste.deploy
loadwsgi._loaders['config_dir'] = _loadconfigdir


class ConfigString(NamedConfigLoader):
    """
    Wrap a raw config string up for paste.deploy.

    If you give one of these to our loadcontext (e.g. give it to our
    appconfig) we'll intercept it and get it routed to the right loader.
    """

    def __init__(self, config_string):
        self.contents = StringIO(dedent(config_string))
        self.filename = "string"
        defaults = {
            'here': "string",
            '__file__': self,
        }
        self.parser = loadwsgi.NicerConfigParser("string", defaults=defaults)
        self.parser.optionxform = str  # Don't lower-case keys
        # Defaults don't need interpolation (crazy PasteDeploy...)
        self.parser.defaults = lambda: dict(self.parser._defaults, **defaults)
        self.parser.readfp(self.contents)

    def readline(self, *args, **kwargs):
        return self.contents.readline(*args, **kwargs)

    def seek(self, *args, **kwargs):
        return self.contents.seek(*args, **kwargs)

    def __iter__(self):
        return iter(self.contents)


def wrap_conf_type(f):
    """
    Wrap a function whos first argument is a paste.deploy style config uri,
    such that you can pass it an un-adorned raw filesystem path (or config
    string) and the config directive (either config:, config_dir:, or
    config_str:) will be added automatically based on the type of entity
    (either a file or directory, or if no such entity on the file system -
    just a string) before passing it through to the paste.deploy function.
    """
    def wrapper(conf_path, *args, **kwargs):
        if os.path.isdir(conf_path):
            conf_type = 'config_dir'
        else:
            conf_type = 'config'
        conf_uri = '%s:%s' % (conf_type, conf_path)
        return f(conf_uri, *args, **kwargs)
    return wrapper


appconfig = wrap_conf_type(loadwsgi.appconfig)


def get_socket(conf):
    """Bind socket to bind ip:port in conf

    :param conf: Configuration dict to read settings from

    :returns: a socket object as returned from socket.listen or
              ssl.wrap_socket if conf specifies cert_file
    """
    try:
        bind_port = int(conf['bind_port'])
    except (ValueError, KeyError, TypeError):
        raise ConfigFilePortError()
    bind_addr = (conf.get('bind_ip', '0.0.0.0'), bind_port)
    address_family = [addr[0] for addr in socket.getaddrinfo(
        bind_addr[0], bind_addr[1], socket.AF_UNSPEC, socket.SOCK_STREAM)
        if addr[0] in (socket.AF_INET, socket.AF_INET6)][0]
    sock = None
    bind_timeout = int(conf.get('bind_timeout', 30))
    retry_until = time.time() + bind_timeout
    warn_ssl = False

    try:
        keepidle = int(conf.get('keep_idle', 600))
        if keepidle <= 0 or keepidle >= 2 ** 15 - 1:
            raise ValueError()
    except (ValueError, KeyError, TypeError):
        raise ConfigFileError()

    while not sock and time.time() < retry_until:
        try:
            sock = listen(bind_addr, backlog=int(conf.get('backlog', 4096)),
                          family=address_family)
            if 'cert_file' in conf:
                warn_ssl = True
                sock = ssl.wrap_socket(sock, certfile=conf['cert_file'],
                                       keyfile=conf['key_file'])
        except socket.error as err:
            if err.args[0] != errno.EADDRINUSE:
                raise
            sleep(0.1)
    if not sock:
        raise Exception(_('Could not bind to %(addr)s:%(port)s '
                          'after trying for %(timeout)s seconds') % {
                              'addr': bind_addr[0], 'port': bind_addr[1],
                              'timeout': bind_timeout})
    # in my experience, sockets can hang around forever without keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, keepidle)
    if warn_ssl:
        ssl_warning_message = _('WARNING: SSL should only be enabled for '
                                'testing purposes. Use external SSL '
                                'termination for a production deployment.')
        get_logger(conf).warning(ssl_warning_message)
        print(ssl_warning_message)
    return sock


class RestrictedGreenPool(GreenPool):
    """
    Works the same as GreenPool, but if the size is specified as one, then the
    spawn_n() method will invoke waitall() before returning to prevent the
    caller from doing any other work (like calling accept()).
    """
    def __init__(self, size=1024):
        super(RestrictedGreenPool, self).__init__(size=size)
        self._rgp_do_wait = (size == 1)

    def spawn_n(self, *args, **kwargs):
        super(RestrictedGreenPool, self).spawn_n(*args, **kwargs)
        if self._rgp_do_wait:
            self.waitall()


def pipeline_property(name, **kwargs):
    """
    Create a property accessor for the given name.  The property will
    dig through the bound instance on which it was accessed for an
    attribute "app" and check that object for an attribute of the given
    name.  If the "app" object does not have such an attribute, it will
    look for an attribute "app" on THAT object and continue it's search
    from there.  If the named attribute cannot be found accessing the
    property will raise AttributeError.

    If a default kwarg is provided you get that instead of the
    AttributeError.  When found the attribute will be cached on instance
    with the property accessor using the same name as the attribute
    prefixed with a leading underscore.
    """

    cache_attr_name = '_%s' % name

    def getter(self):
        cached_value = getattr(self, cache_attr_name, None)
        if cached_value:
            return cached_value
        app = self  # first app is on self
        while True:
            app = getattr(app, 'app', None)
            if not app:
                break
            try:
                value = getattr(app, name)
            except AttributeError:
                continue
            setattr(self, cache_attr_name, value)
            return value
        if 'default' in kwargs:
            return kwargs['default']
        raise AttributeError('No apps in pipeline have a '
                             '%s attribute' % name)

    return property(getter)


class PipelineWrapper(object):
    """
    This class provides a number of utility methods for
    modifying the composition of a wsgi pipeline.
    """

    def __init__(self, context):
        self.context = context

    def __contains__(self, entry_point_name):
        try:
            self.index(entry_point_name)
            return True
        except ValueError:
            return False

    def startswith(self, entry_point_name):
        """
        Tests if the pipeline starts with the given entry point name.

        :param entry_point_name: entry point of middleware or app (Swift only)

        :returns: True if entry_point_name is first in pipeline, False
                  otherwise
        """
        try:
            first_ctx = self.context.filter_contexts[0]
        except IndexError:
            first_ctx = self.context.app_context
        return first_ctx.entry_point_name == entry_point_name

    def _format_for_display(self, ctx):
        # Contexts specified by pipeline= have .name set in NamedConfigLoader.
        if hasattr(ctx, 'name'):
            return ctx.name
        # This should not happen: a foreign context. Let's not crash.
        return "<unknown>"

    def __str__(self):
        parts = [self._format_for_display(ctx)
                 for ctx in self.context.filter_contexts]
        parts.append(self._format_for_display(self.context.app_context))
        return " ".join(parts)

    def create_filter(self, entry_point_name):
        """
        Creates a context for a filter that can subsequently be added
        to a pipeline context.

        :param entry_point_name: entry point of the middleware (Swift only)

        :returns: a filter context
        """
        spec = 'egg:swift#' + entry_point_name
        ctx = loadwsgi.loadcontext(loadwsgi.FILTER, spec,
                                   global_conf=self.context.global_conf)
        ctx.protocol = 'paste.filter_factory'
        ctx.name = entry_point_name
        return ctx

    def index(self, entry_point_name):
        """
        Returns the first index of the given entry point name in the pipeline.

        Raises ValueError if the given module is not in the pipeline.
        """
        for i, ctx in enumerate(self.context.filter_contexts):
            if ctx.entry_point_name == entry_point_name:
                return i
        raise ValueError("%s is not in pipeline" % (entry_point_name,))

    def insert_filter(self, ctx, index=0):
        """
        Inserts a filter module into the pipeline context.

        :param ctx: the context to be inserted
        :param index: (optional) index at which filter should be
                      inserted in the list of pipeline filters. Default
                      is 0, which means the start of the pipeline.
        """
        self.context.filter_contexts.insert(index, ctx)


def loadcontext(object_type, uri, name=None, relative_to=None,
                global_conf=None):
    if isinstance(uri, loadwsgi.ConfigLoader):
        # bypass loadcontext's uri parsing and loader routing and
        # just directly return the context
        if global_conf:
            uri.update_defaults(global_conf, overwrite=False)
        return uri.get_context(object_type, name, global_conf)
    add_conf_type = wrap_conf_type(lambda x: x)
    return loadwsgi.loadcontext(object_type, add_conf_type(uri), name=name,
                                relative_to=relative_to,
                                global_conf=global_conf)


def _add_pipeline_properties(app, *names):
    for property_name in names:
        if not hasattr(app, property_name):
            setattr(app.__class__, property_name,
                    pipeline_property(property_name))


def loadapp(conf_file, global_conf=None, allow_modify_pipeline=True):
    """
    Loads a context from a config file, and if the context is a pipeline
    then presents the app with the opportunity to modify the pipeline.
    """
    global_conf = global_conf or {}
    ctx = loadcontext(loadwsgi.APP, conf_file, global_conf=global_conf)
    if ctx.object_type.name == 'pipeline':
        # give app the opportunity to modify the pipeline context
        app = ctx.app_context.create()
        func = getattr(app, 'modify_wsgi_pipeline', None)
        if func and allow_modify_pipeline:
            func(PipelineWrapper(ctx))
        # cache the freshly created app so we don't have to redo
        # initialization checks and log startup messages again
        ctx.app_context.create = lambda: app
    return ctx.create()


def load_app_config(conf_file):
    """
    Read the app config section from a config file.

    :param conf_file: path to a config file
    :return: a dict
    """
    app_conf = {}
    try:
        ctx = loadcontext(loadwsgi.APP, conf_file)
    except LookupError:
        pass
    else:
        app_conf.update(ctx.app_context.global_conf)
        app_conf.update(ctx.app_context.local_conf)
    return app_conf


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


def run_server(conf, logger, sock, global_conf=None, ready_callback=None):
    # Ensure TZ environment variable exists to avoid stat('/etc/localtime') on
    # some platforms. This locks in reported times to UTC.
    os.environ['TZ'] = 'UTC+0'
    time.tzset()

    eventlet.hubs.use_hub(get_hub())
    eventlet_debug = config_true_value(conf.get('eventlet_debug', 'no'))
    eventlet.debug.hub_exceptions(eventlet_debug)
    wsgi_logger = NullLogger()
    if eventlet_debug:
        # let eventlet.wsgi.server log to stderr
        wsgi_logger = None
    # utils.LogAdapter stashes name in server; fallback on unadapted loggers
    if not global_conf:
        if hasattr(logger, 'server'):
            log_name = logger.server
        else:
            log_name = logger.name
        global_conf = {'log_name': log_name}
    app = loadapp(conf['__file__'], global_conf=global_conf)
    max_clients = int(conf.get('max_clients', '1024'))
    pool = RestrictedGreenPool(size=max_clients)

    # Select which protocol class to use (normal or one expecting PROXY
    # protocol)
    if config_true_value(conf.get('require_proxy_protocol', 'no')):
        protocol_class = SwiftHttpProxiedProtocol
    else:
        protocol_class = SwiftHttpProtocol

    server_kwargs = {
        'custom_pool': pool,
        'protocol': protocol_class,
        'socket_timeout': float(conf.get('client_timeout') or 60),
        # Disable capitalizing headers in Eventlet. This is necessary for
        # the AWS SDK to work with s3api middleware (it needs an "ETag"
        # header; "Etag" just won't do).
        'capitalize_response_headers': False,
    }
    if ready_callback:
        ready_callback()
    try:
        wsgi.server(sock, app, wsgi_logger, **server_kwargs)
    except socket.error as err:
        if err.errno != errno.EINVAL:
            raise
    pool.waitall()


class StrategyBase(object):
    """
    Some operations common to all strategy classes.
    """
    def __init__(self, conf, logger):
        self.conf = conf
        self.logger = logger
        self.signaled_ready = False

        # Each strategy is welcome to track data however it likes, but all
        # socket refs should be somewhere in this dict. This allows forked-off
        # children to easily drop refs to sibling sockets in post_fork_hook().
        self.tracking_data = {}

    def post_fork_hook(self):
        """
        Called in each forked-off child process, prior to starting the actual
        wsgi server, to perform any initialization such as drop privileges.
        """

        if not self.signaled_ready:
            capture_stdio(self.logger)
        drop_privileges(self.conf.get('user', 'swift'))
        del self.tracking_data  # children don't need to track siblings

    def shutdown_sockets(self):
        """
        Shutdown any listen sockets.
        """

        for sock in self.iter_sockets():
            greenio.shutdown_safe(sock)
            sock.close()

    def set_close_on_exec_on_listen_sockets(self):
        """
        Set the close-on-exec flag on any listen sockets.
        """

        for sock in self.iter_sockets():
            if six.PY2:
                fcntl.fcntl(sock.fileno(), fcntl.F_SETFD, fcntl.FD_CLOEXEC)
            else:
                # Python 3.4 and later default to sockets having close-on-exec
                # set (what PEP 0446 calls "non-inheritable").  This new method
                # on socket objects is provided to toggle it.
                sock.set_inheritable(False)

    def signal_ready(self):
        """
        Signal that the server is up and accepting connections.
        """
        if self.signaled_ready:
            return  # Already did it

        # Redirect errors to logger and close stdio. swift-init (for example)
        # uses this to know that the service is ready to accept connections.
        capture_stdio(self.logger)

        # If necessary, signal an old copy of us that it's okay to shutdown
        # its listen sockets now because ours are up and ready to receive
        # connections. This is used for seamless reloading using SIGUSR1.
        reexec_signal_fd = os.getenv(NOTIFY_FD_ENV_KEY)
        if reexec_signal_fd:
            reexec_signal_fd = int(reexec_signal_fd)
            os.write(reexec_signal_fd, str(os.getpid()).encode('utf8'))
            os.close(reexec_signal_fd)

        # Finally, signal systemd (if appropriate) that process started
        # properly.
        systemd_notify(logger=self.logger)

        self.signaled_ready = True


class WorkersStrategy(StrategyBase):
    """
    WSGI server management strategy object for a single bind port and listen
    socket shared by a configured number of forked-off workers.

    Tracking data is a map of ``pid -> socket``.

    Used in :py:func:`run_wsgi`.

    :param dict conf: Server configuration dictionary.
    :param logger: The server's :py:class:`~swift.common.utils.LogAdaptor`
                   object.
    """

    def __init__(self, conf, logger):
        super(WorkersStrategy, self).__init__(conf, logger)
        self.worker_count = config_auto_int_value(conf.get('workers'),
                                                  CPU_COUNT)

    def loop_timeout(self):
        """
        We want to keep from busy-waiting, but we also need a non-None value so
        the main loop gets a chance to tell whether it should keep running or
        not (e.g. SIGHUP received).

        So we return 0.5.
        """

        return 0.5

    def no_fork_sock(self):
        """
        Return a server listen socket if the server should run in the
        foreground (no fork).
        """

        # Useful for profiling [no forks].
        if self.worker_count == 0:
            return get_socket(self.conf)

    def new_worker_socks(self):
        """
        Yield a sequence of (socket, opqaue_data) tuples for each server which
        should be forked-off and started.

        The opaque_data item for each socket will passed into the
        :py:meth:`log_sock_exit` and :py:meth:`register_worker_start` methods
        where it will be ignored.
        """

        while len(self.tracking_data) < self.worker_count:
            yield get_socket(self.conf), None

    def log_sock_exit(self, sock, _unused):
        """
        Log a server's exit.

        :param socket sock: The listen socket for the worker just started.
        :param _unused: The socket's opaque_data yielded by
                        :py:meth:`new_worker_socks`.
        """

        self.logger.notice('Child %d exiting normally' % os.getpid())

    def register_worker_start(self, sock, _unused, pid):
        """
        Called when a new worker is started.

        :param socket sock: The listen socket for the worker just started.
        :param _unused: The socket's opaque_data yielded by new_worker_socks().
        :param int pid: The new worker process' PID
        """

        self.logger.notice('Started child %s from parent %s',
                           pid, os.getpid())
        self.tracking_data[pid] = sock

    def register_worker_exit(self, pid):
        """
        Called when a worker has exited.

        NOTE: a re-exec'ed server can reap the dead worker PIDs from the old
        server process that is being replaced as part of a service reload
        (SIGUSR1).  So we need to be robust to getting some unknown PID here.

        :param int pid: The PID of the worker that exited.
        """

        sock = self.tracking_data.pop(pid, None)
        if sock is None:
            self.logger.info('Ignoring wait() result from unknown PID %s', pid)
        else:
            self.logger.error('Removing dead child %s from parent %s',
                              pid, os.getpid())
            greenio.shutdown_safe(sock)
            sock.close()

    def iter_sockets(self):
        """
        Yields all known listen sockets.
        """

        for sock in self.tracking_data.values():
            yield sock


class ServersPerPortStrategy(StrategyBase):
    """
    WSGI server management strategy object for an object-server with one listen
    port per unique local port in the storage policy rings.  The
    `servers_per_port` integer config setting determines how many workers are
    run per port.

    Tracking data is a map like ``port -> [(pid, socket), ...]``.

    Used in :py:func:`run_wsgi`.

    :param dict conf: Server configuration dictionary.
    :param logger: The server's :py:class:`~swift.common.utils.LogAdaptor`
                   object.
    :param int servers_per_port: The number of workers to run per port.
    """

    def __init__(self, conf, logger, servers_per_port):
        super(ServersPerPortStrategy, self).__init__(conf, logger)
        self.servers_per_port = servers_per_port
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.ring_check_interval = float(conf.get('ring_check_interval', 15))

        bind_ip = conf.get('bind_ip', '0.0.0.0')
        self.cache = BindPortsCache(self.swift_dir, bind_ip)

    def _reload_bind_ports(self):
        self.bind_ports = self.cache.all_bind_ports_for_node()

    def _bind_port(self, port):
        new_conf = self.conf.copy()
        new_conf['bind_port'] = port
        return get_socket(new_conf)

    def loop_timeout(self):
        """
        Return timeout before checking for reloaded rings.

        :returns: The time to wait for a child to exit before checking for
                  reloaded rings (new ports).
        """

        return self.ring_check_interval

    def no_fork_sock(self):
        """
        This strategy does not support running in the foreground.
        """

        pass

    def new_worker_socks(self):
        """
        Yield a sequence of (socket, (port, server_idx)) tuples for each server
        which should be forked-off and started.

        Any sockets for "orphaned" ports no longer in any ring will be closed
        (causing their associated workers to gracefully exit) after all new
        sockets have been yielded.

        The server_idx item for each socket will passed into the
        :py:meth:`log_sock_exit` and :py:meth:`register_worker_start` methods.
        """

        self._reload_bind_ports()
        desired_port_index_pairs = {
            (p, i) for p in self.bind_ports
            for i in range(self.servers_per_port)}

        current_port_index_pairs = {
            (p, i)
            for p, port_data in self.tracking_data.items()
            for i, (pid, sock) in enumerate(port_data)
            if pid is not None}

        if desired_port_index_pairs != current_port_index_pairs:
            # Orphan ports are ports which had object-server processes running,
            # but which no longer appear in the ring.  We'll kill them after we
            # start missing workers.
            orphan_port_index_pairs = current_port_index_pairs - \
                desired_port_index_pairs

            # Fork off worker(s) for every port that's supposed to have
            # worker(s) but doesn't
            missing_port_index_pairs = desired_port_index_pairs - \
                current_port_index_pairs
            for port, server_idx in sorted(missing_port_index_pairs):
                try:
                    sock = self._bind_port(port)
                except Exception as e:
                    self.logger.critical('Unable to bind to port %d: %s',
                                         port, e)
                    continue
                yield sock, (port, server_idx)

            for port, idx in orphan_port_index_pairs:
                # For any port in orphan_port_index_pairs, it is guaranteed
                # that there should be no listen socket for that port, so we
                # can close and forget them.
                pid, sock = self.tracking_data[port][idx]
                greenio.shutdown_safe(sock)
                sock.close()
                self.logger.notice(
                    'Closing unnecessary sock for port %d (child pid %d)',
                    port, pid)
                self.tracking_data[port][idx] = (None, None)
                if all(sock is None
                       for _pid, sock in self.tracking_data[port]):
                    del self.tracking_data[port]

    def log_sock_exit(self, sock, data):
        """
        Log a server's exit.
        """

        port, server_idx = data
        self.logger.notice('Child %d (PID %d, port %d) exiting normally',
                           server_idx, os.getpid(), port)

    def register_worker_start(self, sock, data, pid):
        """
        Called when a new worker is started.

        :param socket sock: The listen socket for the worker just started.
        :param server_idx: The socket's server_idx as yielded by
                           :py:meth:`new_worker_socks`.
        :param int pid: The new worker process' PID
        """

        port, server_idx = data
        self.logger.notice('Started child %d (PID %d) for port %d',
                           server_idx, pid, port)
        if port not in self.tracking_data:
            self.tracking_data[port] = [(None, None)] * self.servers_per_port
        self.tracking_data[port][server_idx] = (pid, sock)

    def register_worker_exit(self, pid):
        """
        Called when a worker has exited.

        :param int pid: The PID of the worker that exited.
        """

        for port_data in self.tracking_data.values():
            for idx, (child_pid, sock) in enumerate(port_data):
                if child_pid == pid:
                    port_data[idx] = (None, None)
                    greenio.shutdown_safe(sock)
                    sock.close()
                    return

    def iter_sockets(self):
        """
        Yields all known listen sockets.
        """

        for port_data in self.tracking_data.values():
            for _pid, sock in port_data:
                yield sock


def run_wsgi(conf_path, app_section, *args, **kwargs):
    """
    Runs the server according to some strategy.  The default strategy runs a
    specified number of workers in pre-fork model.  The object-server (only)
    may use a servers-per-port strategy if its config has a servers_per_port
    setting with a value greater than zero.

    :param conf_path: Path to paste.deploy style configuration file/directory
    :param app_section: App name from conf file to load config from
    :returns: 0 if successful, nonzero otherwise
    """
    # Load configuration, Set logger and Load request processor
    try:
        (conf, logger, log_name) = \
            _initrp(conf_path, app_section, *args, **kwargs)
    except ConfigFileError as e:
        print(e)
        return 1

    # optional nice/ionice priority scheduling
    utils.modify_priority(conf, logger)

    servers_per_port = int(conf.get('servers_per_port', '0') or 0)

    # NOTE: for now servers_per_port is object-server-only; future work could
    # be done to test and allow it to be used for account and container
    # servers, but that has not been done yet.
    if servers_per_port and app_section == 'object-server':
        strategy = ServersPerPortStrategy(
            conf, logger, servers_per_port=servers_per_port)
    else:
        strategy = WorkersStrategy(conf, logger)
        try:
            # Quick sanity check
            int(conf['bind_port'])
        except (ValueError, KeyError, TypeError):
            error_msg = 'bind_port wasn\'t properly set in the config file. ' \
                'It must be explicitly set to a valid port number.'
            logger.error(error_msg)
            print(error_msg)
            return 1

    # patch event before loadapp
    utils.eventlet_monkey_patch()

    # Ensure the configuration and application can be loaded before proceeding.
    global_conf = {'log_name': log_name}
    loadapp(conf_path, global_conf=global_conf)
    if 'global_conf_callback' in kwargs:
        kwargs['global_conf_callback'](conf, global_conf)

    # set utils.FALLOCATE_RESERVE if desired
    utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
        utils.config_fallocate_value(conf.get('fallocate_reserve', '1%'))

    # Do some daemonization process hygene before we fork any children or run a
    # server without forking.
    clean_up_daemon_hygiene()

    no_fork_sock = strategy.no_fork_sock()
    if no_fork_sock:
        run_server(conf, logger, no_fork_sock, global_conf=global_conf,
                   ready_callback=strategy.signal_ready)
        return 0

    def stop_with_signal(signum, *args):
        """Set running flag to False and capture the signum"""
        running_context[0] = False
        running_context[1] = signum

    # context to hold boolean running state and stop signum
    running_context = [True, None]
    signal.signal(signal.SIGTERM, stop_with_signal)
    signal.signal(signal.SIGHUP, stop_with_signal)
    signal.signal(signal.SIGUSR1, stop_with_signal)

    while running_context[0]:
        for sock, sock_info in strategy.new_worker_socks():
            read_fd, write_fd = os.pipe()
            pid = os.fork()
            if pid == 0:
                os.close(read_fd)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)

                def shutdown_my_listen_sock(signum, *args):
                    greenio.shutdown_safe(sock)

                signal.signal(signal.SIGHUP, shutdown_my_listen_sock)
                signal.signal(signal.SIGUSR1, shutdown_my_listen_sock)
                strategy.post_fork_hook()

                def notify():
                    os.write(write_fd, b'ready')
                    os.close(write_fd)

                run_server(conf, logger, sock, ready_callback=notify)
                strategy.log_sock_exit(sock, sock_info)
                return 0
            else:
                os.close(write_fd)
                worker_status = os.read(read_fd, 30)
                os.close(read_fd)
                # TODO: delay this status checking until after we've tried
                # to start all workers. But, we currently use the register
                # event to know when we've got enough workers :-/
                if worker_status == b'ready':
                    strategy.register_worker_start(sock, sock_info, pid)
                else:
                    raise Exception(
                        'worker did not start normally: %r' % worker_status)

        # TODO: signal_ready() as soon as we have at least one new worker for
        # each port, instead of waiting for all of them
        strategy.signal_ready()

        # The strategy may need to pay attention to something in addition to
        # child process exits (like new ports showing up in a ring).
        #
        # NOTE: a timeout value of None will just instantiate the Timeout
        # object and not actually schedule it, which is equivalent to no
        # timeout for the green_os.wait().
        loop_timeout = strategy.loop_timeout()

        with Timeout(loop_timeout, exception=False):
            try:
                try:
                    pid, status = green_os.wait()
                    if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                        strategy.register_worker_exit(pid)
                except OSError as err:
                    if err.errno not in (errno.EINTR, errno.ECHILD):
                        raise
                    if err.errno == errno.ECHILD:
                        # If there are no children at all (ECHILD), then
                        # there's nothing to actually wait on. We sleep
                        # for a little bit to avoid a tight CPU spin
                        # and still are able to catch any KeyboardInterrupt
                        # events that happen. The value of 0.01 matches the
                        # value in eventlet's waitpid().
                        sleep(0.01)
            except KeyboardInterrupt:
                logger.notice('User quit')
                running_context[0] = False
                break

    if running_context[1] is not None:
        try:
            signame = SIGNUM_TO_NAME[running_context[1]]
        except KeyError:
            logger.error('Stopping with unexpected signal %r' %
                         running_context[1])
        else:
            logger.error('%s received (%s)', signame, os.getpid())
    if running_context[1] == signal.SIGTERM:
        os.killpg(0, signal.SIGTERM)
    elif running_context[1] == signal.SIGUSR1:
        # set up a pipe, fork off a child to handle cleanup later,
        # and rexec ourselves with an environment variable set which will
        # indicate which fd (one of the pipe ends) to write a byte to
        # to indicate listen socket setup is complete.  That will signal
        # the forked-off child to complete its listen socket shutdown.
        #
        # NOTE: all strategies will now require the parent process to retain
        # superuser privileges so that the re'execd process can bind a new
        # socket to the configured IP & port(s).  We can't just reuse existing
        # listen sockets because then the bind IP couldn't be changed.
        #
        # NOTE: we need to set all our listen sockets close-on-exec so the only
        # open reference to those file descriptors will be in the forked-off
        # child here who waits to shutdown the old server's listen sockets.  If
        # the re-exec'ed server's old listen sockets aren't closed-on-exec,
        # then the old server can't actually ever exit.
        strategy.set_close_on_exec_on_listen_sockets()
        read_fd, write_fd = os.pipe()
        orig_server_pid = os.getpid()
        child_pid = os.fork()
        if child_pid:
            # parent; set env var for fds and reexec ourselves
            os.close(read_fd)
            os.putenv(NOTIFY_FD_ENV_KEY, str(write_fd))
            myself = os.path.realpath(sys.argv[0])
            logger.info("Old server PID=%d re'execing as: %r",
                        orig_server_pid, [myself] + list(sys.argv))
            if hasattr(os, 'set_inheritable'):
                # See https://www.python.org/dev/peps/pep-0446/
                os.set_inheritable(write_fd, True)
            os.execv(myself, sys.argv)
            logger.error('Somehow lived past os.execv()?!')
            exit('Somehow lived past os.execv()?!')
        elif child_pid == 0:
            # child
            os.close(write_fd)
            logger.info('Old server temporary child PID=%d waiting for '
                        "re-exec'ed PID=%d to signal readiness...",
                        os.getpid(), orig_server_pid)
            try:
                got_pid = os.read(read_fd, 30)
            except Exception:
                logger.warning('Unexpected exception while reading from '
                               'pipe:', exc_info=True)
            else:
                got_pid = got_pid.decode('ascii')
                if got_pid:
                    logger.info('Old server temporary child PID=%d notified '
                                'to shutdown old listen sockets by PID=%s',
                                os.getpid(), got_pid)
                else:
                    logger.warning('Old server temporary child PID=%d *NOT* '
                                   'notified to shutdown old listen sockets; '
                                   'the pipe just *died*.', os.getpid())
            try:
                os.close(read_fd)
            except Exception:
                pass

    strategy.shutdown_sockets()
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    logger.notice('Exited (%s)', os.getpid())
    return 0


class ConfigFileError(Exception):
    pass


class ConfigFilePortError(ConfigFileError):
    pass


def _initrp(conf_path, app_section, *args, **kwargs):
    try:
        conf = appconfig(conf_path, name=app_section)
    except Exception as e:
        raise ConfigFileError("Error trying to load config from %s: %s" %
                              (conf_path, e))

    validate_configuration()

    # pre-configure logger
    log_name = conf.get('log_name', app_section)
    if 'logger' in kwargs:
        logger = kwargs.pop('logger')
    else:
        logger = get_logger(conf, log_name,
                            log_to_console=kwargs.pop('verbose', False),
                            log_route='wsgi')

    # disable fallocate if desired
    if config_true_value(conf.get('disable_fallocate', 'no')):
        disable_fallocate()

    return (conf, logger, log_name)


def init_request_processor(conf_path, app_section, *args, **kwargs):
    """
    Loads common settings from conf
    Sets the logger
    Loads the request processor

    :param conf_path: Path to paste.deploy style configuration file/directory
    :param app_section: App name from conf file to load config from
    :returns: the loaded application entry point
    :raises ConfigFileError: Exception is raised for config file error
    """
    (conf, logger, log_name) = _initrp(conf_path, app_section, *args, **kwargs)
    app = loadapp(conf_path, global_conf={'log_name': log_name})
    return (app, conf, logger, log_name)


class WSGIContext(object):
    """
    This class provides a means to provide context (scope) for a middleware
    filter to have access to the wsgi start_response results like the request
    status and headers.
    """
    def __init__(self, wsgi_app):
        self.app = wsgi_app

    def _start_response(self, status, headers, exc_info=None):
        """
        Saves response info without sending it to the remote client.
        Uses the same semantics as the usual WSGI start_response.
        """
        self._response_status = status
        self._response_headers = \
            headers if isinstance(headers, list) else list(headers)
        self._response_exc_info = exc_info

    def _app_call(self, env):
        """
        Ensures start_response has been called before returning.
        """
        self._response_status = None
        self._response_headers = None
        self._response_exc_info = None
        resp = self.app(env, self._start_response)
        # if start_response has not been called, iterate until we've got a
        # non-empty chunk, by which time the app *should* have called it
        if self._response_status is None:
            resp = reiterate(resp)
        return resp

    def _get_status_int(self):
        """
        Returns the HTTP status int from the last called self._start_response
        result.
        """
        return int(self._response_status.split(' ', 1)[0])

    def _response_header_value(self, key):
        "Returns str of value for given header key or None"
        for h_key, val in self._response_headers:
            if h_key.lower() == key.lower():
                return val
        return None

    def update_content_length(self, new_total_len):
        self._response_headers = [
            (h, v) for h, v in self._response_headers
            if h.lower() != 'content-length']
        self._response_headers.append(('Content-Length', str(new_total_len)))


def make_env(env, method=None, path=None, agent='Swift', query_string=None,
             swift_source=None):
    """
    Returns a new fresh WSGI environment.

    :param env: The WSGI environment to base the new environment on.
    :param method: The new REQUEST_METHOD or None to use the
                   original.
    :param path: The new path_info or none to use the original. path
                 should NOT be quoted. When building a url, a Webob
                 Request (in accordance with wsgi spec) will quote
                 env['PATH_INFO'].  url += quote(environ['PATH_INFO'])
    :param query_string: The new query_string or none to use the original.
                         When building a url, a Webob Request will append
                         the query string directly to the url.
                         url += '?' + env['QUERY_STRING']
    :param agent: The HTTP user agent to use; default 'Swift'. You
                  can put %(orig)s in the agent to have it replaced
                  with the original env's HTTP_USER_AGENT, such as
                  '%(orig)s StaticWeb'. You also set agent to None to
                  use the original env's HTTP_USER_AGENT or '' to
                  have no HTTP_USER_AGENT.
    :param swift_source: Used to mark the request as originating out of
                         middleware. Will be logged in proxy logs.
    :returns: Fresh WSGI environment.
    """
    newenv = {}
    for name in ('HTTP_USER_AGENT', 'HTTP_HOST', 'PATH_INFO',
                 'QUERY_STRING', 'REMOTE_USER', 'REQUEST_METHOD',
                 'SCRIPT_NAME', 'SERVER_NAME', 'SERVER_PORT',
                 'HTTP_ORIGIN', 'HTTP_ACCESS_CONTROL_REQUEST_METHOD',
                 'SERVER_PROTOCOL', 'swift.cache', 'swift.source',
                 'swift.trans_id', 'swift.authorize_override',
                 'swift.authorize', 'HTTP_X_USER_ID', 'HTTP_X_PROJECT_ID',
                 'HTTP_REFERER', 'swift.infocache',
                 'swift.shard_listing_history'):
        if name in env:
            newenv[name] = env[name]
    if method:
        newenv['REQUEST_METHOD'] = method
    if path:
        newenv['PATH_INFO'] = path
        newenv['SCRIPT_NAME'] = ''
    if query_string is not None:
        newenv['QUERY_STRING'] = query_string
    if agent:
        newenv['HTTP_USER_AGENT'] = (
            agent % {'orig': env.get('HTTP_USER_AGENT', '')}).strip()
    elif agent == '' and 'HTTP_USER_AGENT' in newenv:
        del newenv['HTTP_USER_AGENT']
    if swift_source:
        newenv['swift.source'] = swift_source
    newenv['wsgi.input'] = BytesIO()
    if 'SCRIPT_NAME' not in newenv:
        newenv['SCRIPT_NAME'] = ''
    return newenv


def make_subrequest(env, method=None, path=None, body=None, headers=None,
                    agent='Swift', swift_source=None, make_env=make_env):
    """
    Makes a new swob.Request based on the current env but with the
    parameters specified.

    :param env: The WSGI environment to base the new request on.
    :param method: HTTP method of new request; default is from
                   the original env.
    :param path: HTTP path of new request; default is from the
                 original env. path should be compatible with what you
                 would send to Request.blank. path should be quoted and it
                 can include a query string. for example:
                 '/a%20space?unicode_str%E8%AA%9E=y%20es'
    :param body: HTTP body of new request; empty by default.
    :param headers: Extra HTTP headers of new request; None by
                    default.
    :param agent: The HTTP user agent to use; default 'Swift'. You
                  can put %(orig)s in the agent to have it replaced
                  with the original env's HTTP_USER_AGENT, such as
                  '%(orig)s StaticWeb'. You also set agent to None to
                  use the original env's HTTP_USER_AGENT or '' to
                  have no HTTP_USER_AGENT.
    :param swift_source: Used to mark the request as originating out of
                         middleware. Will be logged in proxy logs.
    :param make_env: make_subrequest calls this make_env to help build the
        swob.Request.
    :returns: Fresh swob.Request object.
    """
    query_string = None
    path = path or ''
    if path and '?' in path:
        path, query_string = path.split('?', 1)
    newenv = make_env(env, method, path=wsgi_unquote(path), agent=agent,
                      query_string=query_string, swift_source=swift_source)
    if not headers:
        headers = {}
    if body:
        return Request.blank(path, environ=newenv, body=body, headers=headers)
    else:
        return Request.blank(path, environ=newenv, headers=headers)


def make_pre_authed_env(env, method=None, path=None, agent='Swift',
                        query_string=None, swift_source=None):
    """Same as :py:func:`make_env` but with preauthorization."""
    newenv = make_env(
        env, method=method, path=path, agent=agent, query_string=query_string,
        swift_source=swift_source)
    newenv['swift.authorize'] = lambda req: None
    newenv['swift.authorize_override'] = True
    newenv['REMOTE_USER'] = '.wsgi.pre_authed'
    return newenv


def make_pre_authed_request(env, method=None, path=None, body=None,
                            headers=None, agent='Swift', swift_source=None):
    """Same as :py:func:`make_subrequest` but with preauthorization."""
    return make_subrequest(
        env, method=method, path=path, body=body, headers=headers, agent=agent,
        swift_source=swift_source, make_env=make_pre_authed_env)
