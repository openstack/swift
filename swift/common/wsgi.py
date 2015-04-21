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
import inspect
import os
import signal
import time
import mimetools
from swift import gettext_ as _
from StringIO import StringIO
from textwrap import dedent

import eventlet
import eventlet.debug
from eventlet import greenio, GreenPool, sleep, wsgi, listen
from paste.deploy import loadwsgi
from eventlet.green import socket, ssl
from urllib import unquote

from swift.common import utils, constraints
from swift.common.swob import Request
from swift.common.utils import capture_stdio, disable_fallocate, \
    drop_privileges, get_logger, NullLogger, config_true_value, \
    validate_configuration, get_hub, config_auto_int_value, \
    CloseableChain

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
        context = super(NamedConfigLoader, self).get_context(
            object_type, name=name, global_conf=global_conf)
        context.name = name
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
            '__file__': "string",
        }
        self.parser = loadwsgi.NicerConfigParser("string", defaults=defaults)
        self.parser.optionxform = str  # Don't lower-case keys
        self.parser.readfp(self.contents)


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


def monkey_patch_mimetools():
    """
    mimetools.Message defaults content-type to "text/plain"
    This changes it to default to None, so we can detect missing headers.
    """

    orig_parsetype = mimetools.Message.parsetype

    def parsetype(self):
        if not self.typeheader:
            self.type = None
            self.maintype = None
            self.subtype = None
            self.plisttext = ''
        else:
            orig_parsetype(self)
    parsetype.patched = True

    if not getattr(mimetools.Message.parsetype, 'patched', None):
        mimetools.Message.parsetype = parsetype


def get_socket(conf):
    """Bind socket to bind ip:port in conf

    :param conf: Configuration dict to read settings from

    :returns : a socket object as returned from socket.listen or
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
        raise Exception(_('Could not bind to %s:%s '
                          'after trying for %s seconds') % (
                              bind_addr[0], bind_addr[1], bind_timeout))
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # in my experience, sockets can hang around forever without keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    if hasattr(socket, 'TCP_KEEPIDLE'):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 600)
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
    return ctx.create()


def run_server(conf, logger, sock, global_conf=None):
    # Ensure TZ environment variable exists to avoid stat('/etc/localtime') on
    # some platforms. This locks in reported times to the timezone in which
    # the server first starts running in locations that periodically change
    # timezones.
    os.environ['TZ'] = time.strftime("%z", time.gmtime())

    wsgi.HttpProtocol.default_request_version = "HTTP/1.0"
    # Turn off logging requests by the underlying WSGI software.
    wsgi.HttpProtocol.log_request = lambda *a: None
    # Redirect logging other messages by the underlying WSGI software.
    wsgi.HttpProtocol.log_message = \
        lambda s, f, *a: logger.error('ERROR WSGI: ' + f % a)
    wsgi.WRITE_TIMEOUT = int(conf.get('client_timeout') or 60)

    eventlet.hubs.use_hub(get_hub())
    eventlet.patcher.monkey_patch(all=False, socket=True)
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
    try:
        # Disable capitalizing headers in Eventlet if possible.  This is
        # necessary for the AWS SDK to work with swift3 middleware.
        argspec = inspect.getargspec(wsgi.server)
        if 'capitalize_response_headers' in argspec.args:
            wsgi.server(sock, app, wsgi_logger, custom_pool=pool,
                        capitalize_response_headers=False)
        else:
            wsgi.server(sock, app, wsgi_logger, custom_pool=pool)
    except socket.error as err:
        if err[0] != errno.EINVAL:
            raise
    pool.waitall()


#TODO(clayg): pull more pieces of this to test more
def run_wsgi(conf_path, app_section, *args, **kwargs):
    """
    Runs the server using the specified number of workers.

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

    # bind to address and port
    try:
        sock = get_socket(conf)
    except ConfigFilePortError:
        msg = 'bind_port wasn\'t properly set in the config file. ' \
              'It must be explicitly set to a valid port number.'
        logger.error(msg)
        print(msg)
        return 1
    # remaining tasks should not require elevated privileges
    drop_privileges(conf.get('user', 'swift'))

    # Ensure the configuration and application can be loaded before proceeding.
    global_conf = {'log_name': log_name}
    if 'global_conf_callback' in kwargs:
        kwargs['global_conf_callback'](conf, global_conf)
    loadapp(conf_path, global_conf=global_conf)

    # set utils.FALLOCATE_RESERVE if desired
    reserve = int(conf.get('fallocate_reserve', 0))
    if reserve > 0:
        utils.FALLOCATE_RESERVE = reserve
    # redirect errors to logger and close stdio
    capture_stdio(logger)

    worker_count = config_auto_int_value(conf.get('workers'), CPU_COUNT)

    # Useful for profiling [no forks].
    if worker_count == 0:
        run_server(conf, logger, sock, global_conf=global_conf)
        return 0

    def kill_children(*args):
        """Kills the entire process group."""
        logger.error('SIGTERM received')
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        running[0] = False
        os.killpg(0, signal.SIGTERM)

    def hup(*args):
        """Shuts down the server, but allows running requests to complete"""
        logger.error('SIGHUP received')
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        running[0] = False

    running = [True]
    signal.signal(signal.SIGTERM, kill_children)
    signal.signal(signal.SIGHUP, hup)
    children = []
    while running[0]:
        while len(children) < worker_count:
            pid = os.fork()
            if pid == 0:
                signal.signal(signal.SIGHUP, signal.SIG_DFL)
                signal.signal(signal.SIGTERM, signal.SIG_DFL)
                run_server(conf, logger, sock)
                logger.notice('Child %d exiting normally' % os.getpid())
                return 0
            else:
                logger.notice('Started child %s' % pid)
                children.append(pid)
        try:
            pid, status = os.wait()
            if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                logger.error('Removing dead child %s' % pid)
                children.remove(pid)
        except OSError as err:
            if err.errno not in (errno.EINTR, errno.ECHILD):
                raise
        except KeyboardInterrupt:
            logger.notice('User quit')
            break
    greenio.shutdown_safe(sock)
    sock.close()
    logger.notice('Exited')
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

    monkey_patch_mimetools()
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
        self._response_headers = headers
        self._response_exc_info = exc_info

    def _app_call(self, env):
        """
        Ensures start_response has been called before returning.
        """
        self._response_status = None
        self._response_headers = None
        self._response_exc_info = None
        resp = self.app(env, self._start_response)
        # if start_response has been called, just return the iter
        if self._response_status is not None:
            return resp
        resp = iter(resp)
        try:
            first_chunk = resp.next()
        except StopIteration:
            return iter([])
        else:  # We got a first_chunk
            return CloseableChain([first_chunk], resp)

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
    for name in ('eventlet.posthooks', 'HTTP_USER_AGENT', 'HTTP_HOST',
                 'PATH_INFO', 'QUERY_STRING', 'REMOTE_USER', 'REQUEST_METHOD',
                 'SCRIPT_NAME', 'SERVER_NAME', 'SERVER_PORT',
                 'HTTP_ORIGIN', 'HTTP_ACCESS_CONTROL_REQUEST_METHOD',
                 'SERVER_PROTOCOL', 'swift.cache', 'swift.source',
                 'swift.trans_id', 'swift.authorize_override',
                 'swift.authorize'):
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
    newenv['wsgi.input'] = StringIO('')
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
    newenv = make_env(env, method, path=unquote(path), agent=agent,
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
