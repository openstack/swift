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


import errno
import json
import os
import signal
import struct
import sys
from textwrap import dedent
import time
import warnings

import eventlet
import eventlet.debug
from eventlet import greenio, GreenPool, sleep, wsgi, listen, Timeout
from paste.deploy import loadwsgi
from eventlet.green import socket, ssl, os as green_os
from io import BytesIO, StringIO

from swift.common import utils, constraints
from swift.common.http_protocol import SwiftHttpProtocol, \
    SwiftHttpProxiedProtocol
from swift.common.storage_policy import BindPortsCache
from swift.common.swob import Request, wsgi_unquote
from swift.common.utils import capture_stdio, disable_fallocate, \
    drop_privileges, get_logger, NullLogger, config_true_value, \
    validate_configuration, get_hub, config_auto_int_value, \
    reiterate, clean_up_daemon_hygiene, systemd_notify, NicerInterpolation

SIGNUM_TO_NAME = {getattr(signal, n): n for n in dir(signal)
                  if n.startswith('SIG') and '_' not in n}
NOTIFY_FD_ENV_KEY = '__SWIFT_SERVER_NOTIFY_FD'
CHILD_STATE_FD_ENV_KEY = '__SWIFT_SERVER_CHILD_STATE_FD'

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
        self.parser.read_file(self.contents)

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
                context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                context.verify_mode = ssl.CERT_NONE
                context.load_cert_chain(conf['cert_file'], conf['key_file'])
                warn_ssl = True
                sock = context.wrap_socket(sock, server_side=True)
        except socket.error as err:
            if err.args[0] != errno.EADDRINUSE:
                raise
            sleep(0.1)
    if not sock:
        raise Exception('Could not bind to %(addr)s:%(port)s '
                        'after trying for %(timeout)s seconds' % {
                            'addr': bind_addr[0], 'port': bind_addr[1],
                            'timeout': bind_timeout})
    # in my experience, sockets can hang around forever without keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, keepidle)
    if warn_ssl:
        ssl_warning_message = ('WARNING: SSL should only be enabled for '
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


def loadapp(conf_file, global_conf=None, allow_modify_pipeline=True):
    """
    Loads a context from a config file, and if the context is a pipeline
    then presents the app with the opportunity to modify the pipeline.

    :param conf_file: path to a config file
    :param global_conf: a dict of options to update the loaded config. Options
        in ``global_conf`` will override those in ``conf_file`` except where
        the ``conf_file`` option is preceded by ``set``.
    :param allow_modify_pipeline: if True, and the context is a pipeline, and
        the loaded app has a ``modify_wsgi_pipeline`` property, then that
        property will be called before the pipeline is loaded.
    :return: the loaded app
    """
    global_conf = global_conf or {}
    ctx = loadcontext(loadwsgi.APP, conf_file, global_conf=global_conf)
    if ctx.object_type.name == 'pipeline':
        # give app the opportunity to modify the pipeline context
        ultimate_app = ctx.app_context.create()
        func = getattr(ultimate_app, 'modify_wsgi_pipeline', None)
        if func and allow_modify_pipeline:
            func(PipelineWrapper(ctx))
        filters = [c.create() for c in reversed(ctx.filter_contexts)]
        pipeline = [ultimate_app]
        request_logging_app = app = ultimate_app
        for filter_app in filters:
            app = filter_app(pipeline[0])
            pipeline.insert(0, app)
            if request_logging_app is ultimate_app and \
                    app.__class__.__name__ == 'ProxyLoggingMiddleware':
                request_logging_app = filter_app(ultimate_app)
                # Set some separate-pipeline attrs
                request_logging_app._pipeline = [
                    request_logging_app, ultimate_app]
                request_logging_app._pipeline_request_logging_app = \
                    request_logging_app
                request_logging_app._pipeline_final_app = ultimate_app

        for app in pipeline:
            app._pipeline = pipeline
            # For things like making (logged) backend requests for
            # get_account_info and get_container_info
            app._pipeline_request_logging_app = request_logging_app
            # For getting proxy-server options like *_existence_skip_cache_pct
            app._pipeline_final_app = ultimate_app

        return pipeline[0]
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


def run_server(conf, logger, sock, global_conf=None, ready_callback=None,
               allow_modify_pipeline=True):
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
    app = loadapp(conf['__file__'], global_conf=global_conf,
                  allow_modify_pipeline=allow_modify_pipeline)
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
    if conf.get('keepalive_timeout'):
        server_kwargs['keepalive'] = float(conf['keepalive_timeout']) or False

    if ready_callback:
        ready_callback()
    # Yes, eventlet, we know -- we have to support bad clients, though
    warnings.filterwarnings(
        'ignore', message='capitalize_response_headers is disabled')
    try:
        wsgi.server(sock, app, wsgi_logger, **server_kwargs)
    except socket.error as err:
        if err.errno != errno.EINVAL:
            raise
    finally:
        pool.waitall()
        if hasattr(app._pipeline_final_app, 'watchdog'):
            app._pipeline_final_app.watchdog.kill()


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

        # When doing a seamless reload, we inherit a bunch of child processes
        # that should all clean themselves up fairly quickly; track them here
        self.reload_pids = dict()
        # If they don't cleanup quickly, we'll start killing them after this
        self.stale_worker_timeout = utils.non_negative_float(
            conf.get('stale_worker_timeout', 86400))

    def post_fork_hook(self):
        """
        Called in each forked-off child process, prior to starting the actual
        wsgi server, to perform any initialization such as drop privileges.
        """

        if not self.signaled_ready:
            capture_stdio(self.logger)
        drop_privileges(self.conf.get('user', 'swift'))
        del self.tracking_data  # children don't need to track siblings
        # only MAINPID should be sending systemd notifications
        os.environ.pop('NOTIFY_SOCKET', None)

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
            if ',' in reexec_signal_fd:
                reexec_signal_fd, worker_state_fd = reexec_signal_fd.split(',')
            reexec_signal_fd = int(reexec_signal_fd)
            os.write(reexec_signal_fd, str(os.getpid()).encode('utf8'))
            os.close(reexec_signal_fd)
        worker_state_fd = os.getenv(CHILD_STATE_FD_ENV_KEY)
        try:
            self.read_state_from_old_manager(worker_state_fd)
        except Exception as e:
            # This was all opportunistic anyway; old swift wouldn't even
            # *try* to send us any state -- we don't want *new* code to
            # fail just because *old* code didn't live up to its promise
            self.logger.warning(
                'Failed to read state from the old manager: %r', e,
                exc_info=True)

        # Finally, signal systemd (if appropriate) that process started
        # properly.
        systemd_notify(logger=self.logger)

        self.signaled_ready = True

    def read_state_from_old_manager(self, worker_state_fd):
        """
        Read worker state from the old manager's socket-closer.

        The socket-closing process is the last thing to still have the worker
        PIDs in its head, so it sends us a JSON dict (prefixed by its length)
        of the form::

           {
             "old_pids": {
               "<old worker>": "<first reload time>",
               ...
             }
           }

        More data may be added in the future.

        :param worker_state_fd: The file descriptor that should have the
                                old worker state. Should be passed to us
                                via the ``__SWIFT_SERVER_CHILD_STATE_FD``
                                environment variable.
        """
        if not worker_state_fd:
            return
        worker_state_fd = int(worker_state_fd)
        try:
            # The temporary manager may have up and died while trying to send
            # state; hopefully its logs will have more about what went wrong
            # -- let's just log at warning here
            data_len = os.read(worker_state_fd, 4)
            if len(data_len) != 4:
                self.logger.warning(
                    'Invalid worker state received; expected 4 bytes '
                    'followed by a payload but only received %d bytes',
                    len(data_len))
                return

            data_len = struct.unpack('!I', data_len)[0]
            data = b''
            while len(data) < data_len:
                chunk = os.read(worker_state_fd, data_len - len(data))
                if not chunk:
                    break
                data += chunk
            if len(data) != data_len:
                self.logger.warning(
                    'Incomplete worker state received; expected %d '
                    'bytes but only received %d', data_len, len(data))
                return

            # OK, the temporary manager was able to tell us how much it wanted
            # to send and send it; from here on, error seems appropriate.
            try:
                old_state = json.loads(data)
            except ValueError:
                self.logger.error(
                    'Invalid worker state received; '
                    'invalid JSON: %r', data)
                return

            try:
                old_pids = {
                    int(pid): float(reloaded)
                    for pid, reloaded in old_state["old_pids"].items()}
            except (KeyError, TypeError) as err:
                self.logger.error(
                    'Invalid worker state received; '
                    'error reading old pids: %s', err)
            self.logger.debug('Received old worker pids: %s', old_pids)
            self.reload_pids.update(old_pids)

            def smother(old_pids=old_pids, timeout=self.stale_worker_timeout):
                own_pid = os.getpid()
                kill_times = sorted(((reloaded + timeout, pid)
                                     for pid, reloaded in old_pids.items()),
                                    reverse=True)
                while kill_times:
                    kill_time, pid = kill_times.pop()
                    now = time.time()
                    if kill_time > now:
                        sleep(kill_time - now)
                    try:
                        ppid = utils.get_ppid(pid)
                    except OSError as e:
                        if e.errno != errno.ESRCH:
                            self.logger.error("Could not determine parent "
                                              "for stale pid %d: %s", pid, e)
                        continue
                    if ppid == own_pid:
                        self.logger.notice("Killing long-running stale worker "
                                           "%d after %ds", pid, int(timeout))
                        try:
                            os.kill(pid, signal.SIGKILL)
                        except OSError as e:
                            if e.errno != errno.ESRCH:
                                self.logger.error(
                                    "Could not kill stale pid %d: %s", pid, e)
                    # else, pid got re-used?

            eventlet.spawn_n(smother)

        finally:
            os.close(worker_state_fd)


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

        if self.reload_pids.pop(pid, None):
            self.logger.notice('Removing stale child %d from parent %d',
                               pid, os.getpid())
            return

        sock = self.tracking_data.pop(pid, None)
        if sock is None:
            self.logger.warning('Ignoring wait() result from unknown PID %d',
                                pid)
        else:
            self.logger.error('Removing dead child %d from parent %d',
                              pid, os.getpid())
            greenio.shutdown_safe(sock)
            sock.close()

    def iter_sockets(self):
        """
        Yields all known listen sockets.
        """

        for sock in self.tracking_data.values():
            yield sock

    def get_worker_pids(self):
        return list(self.tracking_data.keys())


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

        # typically ring_ip will be the same as bind_ip, but in a container the
        # bind_ip might be differnt than the host ip address used to lookup
        # devices/ports in the ring
        ring_ip = conf.get('ring_ip', conf.get('bind_ip', '0.0.0.0'))
        self.cache = BindPortsCache(self.swift_dir, ring_ip)

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
        :param tuple data: The socket's (port, server_idx) as yielded by
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

        if self.reload_pids.pop(pid, None):
            self.logger.notice('Removing stale child %d from parent %d',
                               pid, os.getpid())
            return

        for port_data in self.tracking_data.values():
            for idx, (child_pid, sock) in enumerate(port_data):
                if child_pid == pid:
                    self.logger.error('Removing dead child %d from parent %d',
                                      pid, os.getpid())
                    port_data[idx] = (None, None)
                    greenio.shutdown_safe(sock)
                    sock.close()
                    return

        self.logger.warning('Ignoring wait() result from unknown PID %d', pid)

    def iter_sockets(self):
        """
        Yields all known listen sockets.
        """

        for port_data in self.tracking_data.values():
            for _pid, sock in port_data:
                yield sock

    def get_worker_pids(self):
        return [
            pid
            for port_data in self.tracking_data.values()
            for pid, _sock in port_data]


def check_config(conf_path, app_section, *args, **kwargs):
    # Load configuration, Set logger and Load request processor
    (conf, logger, log_name) = \
        _initrp(conf_path, app_section, *args, **kwargs)

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
            if not (1 <= int(conf['bind_port']) <= 2 ** 16 - 1):
                raise ValueError
        except (ValueError, KeyError, TypeError):
            error_msg = 'bind_port wasn\'t properly set in the config file. ' \
                        'It must be explicitly set to a valid port number.'
            logger.error(error_msg)
            raise ConfigFileError(error_msg)

    # patch event before loadapp
    utils.monkey_patch()

    # Ensure the configuration and application can be loaded before proceeding.
    global_conf = {'log_name': log_name}
    loadapp(conf_path, global_conf=global_conf)
    if 'global_conf_callback' in kwargs:
        kwargs['global_conf_callback'](conf, global_conf)

    # set utils.FALLOCATE_RESERVE if desired
    utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
        utils.config_fallocate_value(conf.get('fallocate_reserve', '1%'))
    return conf, logger, global_conf, strategy


def run_wsgi(conf_path, app_section, *args, **kwargs):
    """
    Runs the server according to some strategy.  The default strategy runs a
    specified number of workers in pre-fork model.  The object-server (only)
    may use a servers-per-port strategy if its config has a servers_per_port
    setting with a value greater than zero.

    :param conf_path: Path to paste.deploy style configuration file/directory
    :param app_section: App name from conf file to load config from
    :param allow_modify_pipeline: Boolean for whether the server should have
                                  an opportunity to change its own pipeline.
                                  Defaults to True
    :param test_config: if False (the default) then load and validate the
        config and if successful then continue to run the server; if True then
        load and validate the config but do not run the server.
    :returns: 0 if successful, nonzero otherwise
    """
    try:
        conf, logger, global_conf, strategy = check_config(
            conf_path, app_section, *args, **kwargs)
    except ConfigFileError as err:
        print(err)
        return 1

    if kwargs.get('test_config'):
        return 0

    # Do some daemonization process hygene before we fork any children or run a
    # server without forking.
    clean_up_daemon_hygiene()

    allow_modify_pipeline = kwargs.get('allow_modify_pipeline', True)
    no_fork_sock = strategy.no_fork_sock()
    if no_fork_sock:
        run_server(conf, logger, no_fork_sock, global_conf=global_conf,
                   ready_callback=strategy.signal_ready,
                   allow_modify_pipeline=allow_modify_pipeline)
        systemd_notify(logger, "STOPPING=1")
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
        new_workers = {}  # pid -> status pipe
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

                run_server(conf, logger, sock, ready_callback=notify,
                           allow_modify_pipeline=allow_modify_pipeline)
                strategy.log_sock_exit(sock, sock_info)
                return 0
            else:
                os.close(write_fd)
                new_workers[pid] = read_fd
                strategy.register_worker_start(sock, sock_info, pid)

        for pid, read_fd in new_workers.items():
            worker_status = os.read(read_fd, 30)
            os.close(read_fd)
            if worker_status != b'ready':
                raise Exception(
                    'worker %d did not start normally: %r' %
                    (pid, worker_status))

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
            logger.notice('%s received (%s)', signame, os.getpid())
    if running_context[1] == signal.SIGTERM:
        systemd_notify(logger, "STOPPING=1")
        os.killpg(0, signal.SIGTERM)
    elif running_context[1] == signal.SIGUSR1:
        systemd_notify(logger, "RELOADING=1")
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
        state_rfd, state_wfd = os.pipe()
        orig_server_pid = os.getpid()
        child_pid = os.fork()
        if child_pid:
            # parent; set env var for fds and reexec ourselves
            os.close(read_fd)
            os.close(state_wfd)
            os.putenv(NOTIFY_FD_ENV_KEY, str(write_fd))
            os.putenv(CHILD_STATE_FD_ENV_KEY, str(state_rfd))
            myself = os.path.realpath(sys.argv[0])
            logger.info("Old server PID=%d re'execing as: %r",
                        orig_server_pid, [myself] + list(sys.argv))
            if hasattr(os, 'set_inheritable'):
                # See https://www.python.org/dev/peps/pep-0446/
                os.set_inheritable(write_fd, True)
                os.set_inheritable(state_rfd, True)
            os.execv(myself, sys.argv)  # nosec B606
            logger.error('Somehow lived past os.execv()?!')
            exit('Somehow lived past os.execv()?!')
        elif child_pid == 0:
            # child
            os.close(write_fd)
            os.close(state_rfd)
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
                    # Ensure new process knows about old children
                    stale_pids = dict(strategy.reload_pids)
                    stale_pids[os.getpid()] = now = time.time()
                    stale_pids.update({
                        pid: now for pid in strategy.get_worker_pids()})
                    data = json.dumps({
                        "old_pids": stale_pids,
                    }).encode('ascii')
                    os.write(state_wfd, struct.pack('!I', len(data)) + data)
                    os.close(state_wfd)
                else:
                    logger.warning('Old server temporary child PID=%d *NOT* '
                                   'notified to shutdown old listen sockets; '
                                   'the pipe just *died*.', os.getpid())
            try:
                os.close(read_fd)
            except Exception:
                pass
    else:
        # SIGHUP or, less likely, run in "once" mode
        systemd_notify(logger, "STOPPING=1")

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
