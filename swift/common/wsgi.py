# Copyright (c) 2010-2011 OpenStack, LLC.
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
import os
import signal
import sys
import time
import mimetools

import eventlet
from eventlet import greenio, GreenPool, sleep, wsgi, listen
from paste.deploy import loadapp, appconfig

# Hook to ensure connection resets don't blow up our servers.
# Remove with next release of Eventlet that has it in the set already.
from errno import ECONNRESET
wsgi.ACCEPT_ERRNO.add(ECONNRESET)

from eventlet.green import socket, ssl

from swift.common.utils import get_logger, drop_privileges, \
    validate_configuration, capture_stdio, NullLogger


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

    mimetools.Message.parsetype = parsetype


def get_socket(conf, default_port=8080):
    """Bind socket to bind ip:port in conf

    :param conf: Configuration dict to read settings from
    :param default_port: port to use if not specified in conf

    :returns : a socket object as returned from socket.listen or
               ssl.wrap_socket if conf specifies cert_file
    """
    bind_addr = (conf.get('bind_ip', '0.0.0.0'),
                 int(conf.get('bind_port', default_port)))
    sock = None
    retry_until = time.time() + 30
    while not sock and time.time() < retry_until:
        try:
            sock = listen(bind_addr, backlog=int(conf.get('backlog', 4096)))
            if 'cert_file' in conf:
                sock = ssl.wrap_socket(sock, certfile=conf['cert_file'],
                    keyfile=conf['key_file'])
        except socket.error, err:
            if err.args[0] != errno.EADDRINUSE:
                raise
            sleep(0.1)
    if not sock:
        raise Exception('Could not bind to %s:%s after trying for 30 seconds' %
                        bind_addr)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # in my experience, sockets can hang around forever without keepalive
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 600)
    return sock


# TODO: pull pieces of this out to test
def run_wsgi(conf_file, app_section, *args, **kwargs):
    """
    Loads common settings from conf, then instantiates app and runs
    the server using the specified number of workers.

    :param conf_file: Path to paste.deploy style configuration file
    :param app_section: App name from conf file to load config from
    """

    try:
        conf = appconfig('config:%s' % conf_file, name=app_section)
    except Exception, e:
        print "Error trying to load config %s: %s" % (conf_file, e)
        return
    validate_configuration()

    # pre-configure logger
    log_name = conf.get('log_name', app_section)
    if 'logger' in kwargs:
        logger = kwargs.pop('logger')
    else:
        logger = get_logger(conf, log_name,
                            log_to_console=kwargs.pop('verbose', False))

    # redirect errors to logger and close stdio
    capture_stdio(logger)
    # bind to address and port
    sock = get_socket(conf, default_port=kwargs.get('default_port', 8080))
    # remaining tasks should not require elevated privileges
    drop_privileges(conf.get('user', 'swift'))

    # finally after binding to ports and privilege drop, run app __init__ code
    app = loadapp('config:%s' % conf_file, global_conf={'log_name': log_name})

    def run_server():
        wsgi.HttpProtocol.default_request_version = "HTTP/1.0"
        eventlet.hubs.use_hub('poll')
        eventlet.patcher.monkey_patch(all=False, socket=True)
        monkey_patch_mimetools()
        pool = GreenPool(size=1024)
        try:
            wsgi.server(sock, app, NullLogger(), custom_pool=pool)
        except socket.error, err:
            if err[0] != errno.EINVAL:
                raise
        pool.waitall()

    worker_count = int(conf.get('workers', '1'))
    # Useful for profiling [no forks].
    if worker_count == 0:
        run_server()
        return

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
                run_server()
                logger.info('Child %d exiting normally' % os.getpid())
                return
            else:
                logger.info('Started child %s' % pid)
                children.append(pid)
        try:
            pid, status = os.wait()
            if os.WIFEXITED(status) or os.WIFSIGNALED(status):
                logger.error('Removing dead child %s' % pid)
                children.remove(pid)
        except OSError, err:
            if err.errno not in (errno.EINTR, errno.ECHILD):
                raise
        except KeyboardInterrupt:
            logger.info('User quit')
            break
    greenio.shutdown_safe(sock)
    sock.close()
    logger.info('Exited')
