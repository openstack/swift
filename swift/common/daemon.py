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

import errno
import os
import sys
import time
import signal
from re import sub

import eventlet
import eventlet.debug

from swift.common import utils


class Daemon(object):
    """
    Daemon base class

    A daemon has a run method that accepts a ``once`` kwarg and will dispatch
    to :meth:`run_once` or :meth:`run_forever`.

    A subclass of Daemon must implement :meth:`run_once` and
    :meth:`run_forever`.

    A subclass of Daemon may override :meth:`get_worker_args` to dispatch
    arguments to individual child process workers and :meth:`is_healthy` to
    perform context specific periodic wellness checks which can reset worker
    arguments.

    Implementations of Daemon do not know *how* to daemonize, or execute
    multiple daemonized workers, they simply provide the behavior of the daemon
    and context specific knowledge about how workers should be started.
    """
    WORKERS_HEALTHCHECK_INTERVAL = 5.0

    def __init__(self, conf):
        self.conf = conf
        self.logger = utils.get_logger(conf, log_route='daemon')

    def run_once(self, *args, **kwargs):
        """Override this to run the script once"""
        raise NotImplementedError('run_once not implemented')

    def run_forever(self, *args, **kwargs):
        """Override this to run forever"""
        raise NotImplementedError('run_forever not implemented')

    def run(self, once=False, **kwargs):
        if once:
            self.run_once(**kwargs)
        else:
            self.run_forever(**kwargs)

    def post_multiprocess_run(self):
        """
        Override this to do something after running using multiple worker
        processes. This method is called in the parent process.

        This is probably only useful for run-once mode since there is no
        "after running" in run-forever mode.
        """
        pass

    def get_worker_args(self, once=False, **kwargs):
        """
        For each worker yield a (possibly empty) dict of kwargs to pass along
        to the daemon's :meth:`run` method after fork.  The length of elements
        returned from this method will determine the number of processes
        created.

        If the returned iterable is empty, the Strategy will fallback to
        run-inline strategy.

        :param once: False if the worker(s) will be daemonized, True if the
            worker(s) will be run once
        :param kwargs: plumbed through via command line argparser

        :returns: an iterable of dicts, each element represents the kwargs to
                  be passed to a single worker's :meth:`run` method after fork.
        """
        return []

    def is_healthy(self):
        """
        This method is called very frequently on the instance of the daemon
        held by the parent process.  If it returns False, all child workers are
        terminated, and new workers will be created.

        :returns: a boolean, True only if all workers should continue to run
        """
        return True


class DaemonStrategy(object):
    """
    This is the execution strategy for using subclasses of Daemon.  The default
    behavior is to invoke the daemon's :meth:`Daemon.run` method from within
    the parent process.  When the :meth:`Daemon.run` method returns the parent
    process will exit.

    However, if the Daemon returns a non-empty iterable from
    :meth:`Daemon.get_worker_args`, the daemon's :meth:`Daemon.run` method will
    be invoked in child processes, with the arguments provided from the parent
    process's instance of the daemon.  If a child process exits it will be
    restarted with the same options, unless it was executed in once mode.

    :param daemon: an instance of a :class:`Daemon` (has a `run` method)
    :param logger: a logger instance
    """

    def __init__(self, daemon, logger):
        self.daemon = daemon
        self.logger = logger
        self.running = False
        # only used by multi-worker strategy
        self.options_by_pid = {}
        self.unspawned_worker_options = []

    def setup(self, **kwargs):
        utils.validate_configuration()
        utils.drop_privileges(self.daemon.conf.get('user', 'swift'))
        utils.clean_up_daemon_hygiene()
        utils.capture_stdio(self.logger, **kwargs)

        def kill_children(*args):
            self.running = False
            self.logger.notice('SIGTERM received (%s)', os.getpid())
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            os.killpg(0, signal.SIGTERM)
            os._exit(0)

        signal.signal(signal.SIGTERM, kill_children)
        self.running = True
        utils.systemd_notify(self.logger)

    def _run_inline(self, once=False, **kwargs):
        """Run the daemon"""
        self.daemon.run(once=once, **kwargs)

    def run(self, once=False, **kwargs):
        """Daemonize and execute our strategy"""
        self.setup(**kwargs)
        try:
            self._run(once=once, **kwargs)
        except KeyboardInterrupt:
            self.logger.notice('User quit')
        finally:
            self.cleanup(stopping=True)
        self.running = False

    def _fork(self, once, **kwargs):
        pid = os.fork()
        if pid == 0:
            signal.signal(signal.SIGHUP, signal.SIG_DFL)
            signal.signal(signal.SIGTERM, signal.SIG_DFL)
            # only MAINPID should be sending notifications
            os.environ.pop('NOTIFY_SOCKET', None)

            self.daemon.run(once, **kwargs)

            self.logger.debug('Forked worker %s finished', os.getpid())
            # do not return from this stack, nor execute any finally blocks
            os._exit(0)
        else:
            self.register_worker_start(pid, kwargs)
        return pid

    def iter_unspawned_workers(self):
        while True:
            try:
                per_worker_options = self.unspawned_worker_options.pop()
            except IndexError:
                return
            yield per_worker_options

    def spawned_pids(self):
        return list(self.options_by_pid.keys())

    def register_worker_start(self, pid, per_worker_options):
        self.logger.debug('Spawned worker %s with %r', pid, per_worker_options)
        self.options_by_pid[pid] = per_worker_options

    def register_worker_exit(self, pid):
        self.unspawned_worker_options.append(self.options_by_pid.pop(pid))

    def ask_daemon_to_prepare_workers(self, once, **kwargs):
        self.unspawned_worker_options = list(
            self.daemon.get_worker_args(once=once, **kwargs))

    def abort_workers_if_daemon_would_like(self):
        if not self.daemon.is_healthy():
            self.logger.debug(
                'Daemon needs to change options, aborting workers')
            self.cleanup()
            return True
        return False

    def check_on_all_running_workers(self):
        for p in self.spawned_pids():
            try:
                pid, status = os.waitpid(p, os.WNOHANG)
            except OSError as err:
                if err.errno not in (errno.EINTR, errno.ECHILD):
                    raise
                self.logger.notice('Worker %s died', p)
            else:
                if pid == 0:
                    # child still running
                    continue
                self.logger.debug('Worker %s exited', p)
            self.register_worker_exit(p)

    def _run(self, once, **kwargs):
        self.ask_daemon_to_prepare_workers(once, **kwargs)
        if not self.unspawned_worker_options:
            return self._run_inline(once, **kwargs)
        for per_worker_options in self.iter_unspawned_workers():
            if self._fork(once, **per_worker_options) == 0:
                return 0
        while self.running:
            if self.abort_workers_if_daemon_would_like():
                self.ask_daemon_to_prepare_workers(once, **kwargs)
            self.check_on_all_running_workers()
            if not once:
                for per_worker_options in self.iter_unspawned_workers():
                    if self._fork(once, **per_worker_options) == 0:
                        return 0
            else:
                if not self.spawned_pids():
                    self.logger.notice('Finished %s', os.getpid())
                    break
            time.sleep(self.daemon.WORKERS_HEALTHCHECK_INTERVAL)
        self.daemon.post_multiprocess_run()
        return 0

    def cleanup(self, stopping=False):
        """
        Cleanup worker processes

        :param stopping: if set, tell systemd we're stopping
        """

        if stopping:
            utils.systemd_notify(self.logger, "STOPPING=1")
        for p in self.spawned_pids():
            try:
                os.kill(p, signal.SIGTERM)
            except OSError as err:
                if err.errno not in (errno.ESRCH, errno.EINTR, errno.ECHILD):
                    raise
            self.register_worker_exit(p)
            self.logger.debug('Cleaned up worker %s', p)


def run_daemon(klass, conf_file, section_name='', once=False, **kwargs):
    """
    Loads settings from conf, then instantiates daemon ``klass`` and runs the
    daemon with the specified ``once`` kwarg.  The section_name will be derived
    from the daemon ``klass`` if not provided (e.g. ObjectReplicator =>
    object-replicator).

    :param klass: Class to instantiate, subclass of :class:`Daemon`
    :param conf_file: Path to configuration file
    :param section_name: Section name from conf file to load config from
    :param once: Passed to daemon :meth:`Daemon.run` method
    """
    # very often the config section_name is based on the class name
    # the None singleton will be passed through to readconf as is
    if section_name == '':
        section_name = sub(r'([a-z])([A-Z])', r'\1-\2',
                           klass.__name__).lower()
    try:
        conf = utils.readconf(conf_file, section_name,
                              log_name=kwargs.get('log_name'))
    except (ValueError, IOError) as e:
        # The message will be printed to stderr
        # and results in an exit code of 1.
        sys.exit(e)

    # patch eventlet/logging early
    utils.monkey_patch()
    eventlet.hubs.use_hub(utils.get_hub())

    # once on command line (i.e. daemonize=false) will over-ride config
    once = once or not utils.config_true_value(conf.get('daemonize', 'true'))

    # pre-configure logger
    if 'logger' in kwargs:
        logger = kwargs.pop('logger')
    else:
        logger = utils.get_logger(conf, conf.get('log_name', section_name),
                                  log_to_console=kwargs.pop('verbose', False),
                                  log_route=section_name)

    # optional nice/ionice priority scheduling
    utils.modify_priority(conf, logger)

    # disable fallocate if desired
    if utils.config_true_value(conf.get('disable_fallocate', 'no')):
        utils.disable_fallocate()
    # set utils.FALLOCATE_RESERVE if desired
    utils.FALLOCATE_RESERVE, utils.FALLOCATE_IS_PERCENT = \
        utils.config_fallocate_value(conf.get('fallocate_reserve', '1%'))

    # By default, disable eventlet printing stacktraces
    eventlet_debug = utils.config_true_value(conf.get('eventlet_debug', 'no'))
    eventlet.debug.hub_exceptions(eventlet_debug)

    # Ensure TZ environment variable exists to avoid stat('/etc/localtime') on
    # some platforms. This locks in reported times to UTC.
    os.environ['TZ'] = 'UTC+0'
    time.tzset()

    logger.notice('Starting %s', os.getpid())
    try:
        d = klass(conf)
        DaemonStrategy(d, logger).run(once=once, **kwargs)
    except KeyboardInterrupt:
        logger.info('User quit')
    logger.notice('Exited %s', os.getpid())
    return d
