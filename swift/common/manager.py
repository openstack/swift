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

from __future__ import print_function
import functools
import errno
import os
import resource
import signal
import time
import subprocess
import re
from swift import gettext_ as _
import tempfile

from swift.common.utils import search_tree, remove_file, write_file
from swift.common.exceptions import InvalidPidFileException

SWIFT_DIR = '/etc/swift'
RUN_DIR = '/var/run/swift'
PROC_DIR = '/proc'

ALL_SERVERS = ['account-auditor', 'account-server', 'container-auditor',
               'container-replicator', 'container-reconciler',
               'container-server', 'container-sharder', 'container-sync',
               'container-updater', 'object-auditor', 'object-server',
               'object-expirer', 'object-replicator',
               'object-reconstructor', 'object-updater',
               'proxy-server', 'account-replicator', 'account-reaper']
MAIN_SERVERS = ['proxy-server', 'account-server', 'container-server',
                'object-server']
REST_SERVERS = [s for s in ALL_SERVERS if s not in MAIN_SERVERS]
# aliases mapping
ALIASES = {'all': ALL_SERVERS, 'main': MAIN_SERVERS, 'rest': REST_SERVERS}
GRACEFUL_SHUTDOWN_SERVERS = MAIN_SERVERS
START_ONCE_SERVERS = REST_SERVERS
# These are servers that match a type (account-*, container-*, object-*) but
# don't use that type-server.conf file and instead use their own.
STANDALONE_SERVERS = ['object-expirer', 'container-reconciler']

KILL_WAIT = 15  # seconds to wait for servers to die (by default)
WARNING_WAIT = 3  # seconds to wait after message that may just be a warning

MAX_DESCRIPTORS = 32768
MAX_MEMORY = (1024 * 1024 * 1024) * 2  # 2 GB
MAX_PROCS = 8192  # workers * disks, can get high


def setup_env():
    """Try to increase resource limits of the OS. Move PYTHON_EGG_CACHE to /tmp
    """
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE,
                           (MAX_DESCRIPTORS, MAX_DESCRIPTORS))
    except ValueError:
        print(_("WARNING: Unable to modify file descriptor limit.  "
                "Running as non-root?"))

    try:
        resource.setrlimit(resource.RLIMIT_DATA,
                           (MAX_MEMORY, MAX_MEMORY))
    except ValueError:
        print(_("WARNING: Unable to modify memory limit.  "
                "Running as non-root?"))

    try:
        resource.setrlimit(resource.RLIMIT_NPROC,
                           (MAX_PROCS, MAX_PROCS))
    except ValueError:
        print(_("WARNING: Unable to modify max process limit.  "
                "Running as non-root?"))

    # Set PYTHON_EGG_CACHE if it isn't already set
    os.environ.setdefault('PYTHON_EGG_CACHE', tempfile.gettempdir())


def command(func):
    """
    Decorator to declare which methods are accessible as commands, commands
    always return 1 or 0, where 0 should indicate success.

    :param func: function to make public
    """
    func.publicly_accessible = True

    @functools.wraps(func)
    def wrapped(*a, **kw):
        rv = func(*a, **kw)
        return 1 if rv else 0
    return wrapped


def watch_server_pids(server_pids, interval=1, **kwargs):
    """Monitor a collection of server pids yielding back those pids that
    aren't responding to signals.

    :param server_pids: a dict, lists of pids [int,...] keyed on
                        Server objects
    """
    status = {}
    start = time.time()
    end = start + interval
    server_pids = dict(server_pids)  # make a copy
    while True:
        for server, pids in server_pids.items():
            for pid in pids:
                try:
                    # let pid stop if it wants to
                    os.waitpid(pid, os.WNOHANG)
                except OSError as e:
                    if e.errno not in (errno.ECHILD, errno.ESRCH):
                        raise  # else no such child/process
            # check running pids for server
            status[server] = server.get_running_pids(**kwargs)
            for pid in pids:
                # original pids no longer in running pids!
                if pid not in status[server]:
                    yield server, pid
            # update active pids list using running_pids
            server_pids[server] = status[server]
        if not [p for server, pids in status.items() for p in pids]:
            # no more running pids
            break
        if time.time() > end:
            break
        else:
            time.sleep(0.1)


def safe_kill(pid, sig, name):
    """Send signal to process and check process name

    : param pid: process id
    : param sig: signal to send
    : param name: name to ensure target process
    """

    # check process name for SIG_DFL
    if sig == signal.SIG_DFL:
        try:
            proc_file = '%s/%d/cmdline' % (PROC_DIR, pid)
            if os.path.exists(proc_file):
                with open(proc_file, 'r') as fd:
                    if name not in fd.read():
                        # unknown process is using the pid
                        raise InvalidPidFileException()
        except IOError:
            pass

    os.kill(pid, sig)


def kill_group(pid, sig):
    """Send signal to process group

    : param pid: process id
    : param sig: signal to send
    """
    # Negative PID means process group
    os.kill(-pid, sig)


class UnknownCommandError(Exception):
    pass


class Manager(object):
    """Main class for performing commands on groups of servers.

    :param servers: list of server names as strings

    """

    def __init__(self, servers, run_dir=RUN_DIR):
        self.server_names = set()
        self._default_strict = True
        for server in servers:
            if server in ALIASES:
                self.server_names.update(ALIASES[server])
                self._default_strict = False
            elif '*' in server:
                # convert glob to regex
                self.server_names.update([
                    s for s in ALL_SERVERS if
                    re.match(server.replace('*', '.*'), s)])
                self._default_strict = False
            else:
                self.server_names.add(server)

        self.servers = set()
        for name in self.server_names:
            self.servers.add(Server(name, run_dir))

    def __iter__(self):
        return iter(self.servers)

    @command
    def status(self, **kwargs):
        """display status of tracked pids for server
        """
        status = 0
        for server in self.servers:
            status += server.status(**kwargs)
        return status

    @command
    def start(self, **kwargs):
        """starts a server
        """
        setup_env()
        status = 0

        strict = kwargs.get('strict')
        # if strict not set explicitly
        if strict is None:
            strict = self._default_strict

        for server in self.servers:
            status += 0 if server.launch(**kwargs) else 1

        if not strict:
            status = 0

        if not kwargs.get('daemon', True):
            for server in self.servers:
                try:
                    status += server.interact(**kwargs)
                except KeyboardInterrupt:
                    print(_('\nuser quit'))
                    self.stop(**kwargs)
                    break
        elif kwargs.get('wait', True):
            for server in self.servers:
                status += server.wait(**kwargs)
        return status

    @command
    def no_wait(self, **kwargs):
        """spawn server and return immediately
        """
        kwargs['wait'] = False
        return self.start(**kwargs)

    @command
    def no_daemon(self, **kwargs):
        """start a server interactively
        """
        kwargs['daemon'] = False
        return self.start(**kwargs)

    @command
    def once(self, **kwargs):
        """start server and run one pass on supporting daemons
        """
        kwargs['once'] = True
        return self.start(**kwargs)

    @command
    def stop(self, **kwargs):
        """stops a server
        """
        server_pids = {}
        for server in self.servers:
            signaled_pids = server.stop(**kwargs)
            if not signaled_pids:
                print(_('No %s running') % server)
            else:
                server_pids[server] = signaled_pids

        # all signaled_pids, i.e. list(itertools.chain(*server_pids.values()))
        signaled_pids = [p for server, pids in server_pids.items()
                         for p in pids]
        # keep track of the pids yeiled back as killed for all servers
        killed_pids = set()
        kill_wait = kwargs.get('kill_wait', KILL_WAIT)
        for server, killed_pid in watch_server_pids(server_pids,
                                                    interval=kill_wait,
                                                    **kwargs):
            print(_("%(server)s (%(pid)s) appears to have stopped") %
                  {'server': server, 'pid': killed_pid})
            killed_pids.add(killed_pid)
            if not killed_pids.symmetric_difference(signaled_pids):
                # all processes have been stopped
                return 0

        # reached interval n watch_pids w/o killing all servers
        kill_after_timeout = kwargs.get('kill_after_timeout', False)
        for server, pids in server_pids.items():
            if not killed_pids.issuperset(pids):
                # some pids of this server were not killed
                if kill_after_timeout:
                    print(_('Waited %(kill_wait)s seconds for %(server)s '
                            'to die; killing') %
                          {'kill_wait': kill_wait, 'server': server})
                    # Send SIGKILL to all remaining pids
                    for pid in set(pids.keys()) - killed_pids:
                        print(_('Signal %(server)s  pid: %(pid)s  signal: '
                                '%(signal)s') % {'server': server,
                                                 'pid': pid,
                                                 'signal': signal.SIGKILL})
                        # Send SIGKILL to process group
                        try:
                            kill_group(pid, signal.SIGKILL)
                        except OSError as e:
                            # PID died before kill_group can take action?
                            if e.errno != errno.ESRCH:
                                raise
                else:
                    print(_('Waited %(kill_wait)s seconds for %(server)s '
                            'to die; giving up') %
                          {'kill_wait': kill_wait, 'server': server})
        return 1

    @command
    def kill(self, **kwargs):
        """stop a server (no error if not running)
        """
        status = self.stop(**kwargs)
        kwargs['quiet'] = True
        if status and not self.status(**kwargs):
            # only exit error if the server is still running
            return status
        return 0

    @command
    def shutdown(self, **kwargs):
        """allow current requests to finish on supporting servers
        """
        kwargs['graceful'] = True
        status = 0
        status += self.stop(**kwargs)
        return status

    @command
    def restart(self, **kwargs):
        """stops then restarts server
        """
        status = 0
        status += self.stop(**kwargs)
        status += self.start(**kwargs)
        return status

    @command
    def reload(self, **kwargs):
        """graceful shutdown then restart on supporting servers
        """
        kwargs['graceful'] = True
        status = 0
        for server in self.server_names:
            m = Manager([server])
            status += m.stop(**kwargs)
            status += m.start(**kwargs)
        return status

    @command
    def force_reload(self, **kwargs):
        """alias for reload
        """
        return self.reload(**kwargs)

    def get_command(self, cmd):
        """Find and return the decorated method named like cmd

        :param cmd: the command to get, a string, if not found raises
                    UnknownCommandError

        """
        cmd = cmd.lower().replace('-', '_')
        f = getattr(self, cmd, None)
        if f is None:
            raise UnknownCommandError(cmd)
        if not hasattr(f, 'publicly_accessible'):
            raise UnknownCommandError(cmd)
        return f

    @classmethod
    def list_commands(cls):
        """Get all publicly accessible commands

        :returns: a list of string tuples (cmd, help), the method names who are
                  decorated as commands
        """
        get_method = lambda cmd: getattr(cls, cmd)
        return sorted([(x.replace('_', '-'), get_method(x).__doc__.strip())
                       for x in dir(cls) if
                       getattr(get_method(x), 'publicly_accessible', False)])

    def run_command(self, cmd, **kwargs):
        """Find the named command and run it

        :param cmd: the command name to run

        """
        f = self.get_command(cmd)
        return f(**kwargs)


class Server(object):
    """Manage operations on a server or group of servers of similar type

    :param server: name of server
    """

    def __init__(self, server, run_dir=RUN_DIR):
        self.server = server.lower()
        if '.' in self.server:
            self.server, self.conf = self.server.rsplit('.', 1)
        else:
            self.conf = None
        if '-' not in self.server:
            self.server = '%s-server' % self.server
        self.type = self.server.rsplit('-', 1)[0]
        self.cmd = 'swift-%s' % self.server
        self.procs = []
        self.run_dir = run_dir

    def __str__(self):
        return self.server

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(str(self)))

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        try:
            return self.server == other.server
        except AttributeError:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def get_pid_file_name(self, conf_file):
        """Translate conf_file to a corresponding pid_file

        :param conf_file: an conf_file for this server, a string

        :returns: the pid_file for this conf_file

        """
        return conf_file.replace(
            os.path.normpath(SWIFT_DIR), self.run_dir, 1).replace(
                '%s-server' % self.type, self.server, 1).replace(
                    '.conf', '.pid', 1)

    def get_conf_file_name(self, pid_file):
        """Translate pid_file to a corresponding conf_file

        :param pid_file: a pid_file for this server, a string

        :returns: the conf_file for this pid_file

        """
        if self.server in STANDALONE_SERVERS:
            return pid_file.replace(
                os.path.normpath(self.run_dir), SWIFT_DIR, 1).replace(
                    '.pid', '.conf', 1)
        else:
            return pid_file.replace(
                os.path.normpath(self.run_dir), SWIFT_DIR, 1).replace(
                    self.server, '%s-server' % self.type, 1).replace(
                        '.pid', '.conf', 1)

    def conf_files(self, **kwargs):
        """Get conf files for this server

        :param: number, if supplied will only lookup the nth server

        :returns: list of conf files
        """
        if self.server in STANDALONE_SERVERS:
            server_search = self.server
        else:
            server_search = "%s-server" % self.type
        if self.conf is not None:
            found_conf_files = search_tree(SWIFT_DIR, server_search,
                                           self.conf + '.conf',
                                           dir_ext=self.conf + '.conf.d')
        else:
            found_conf_files = search_tree(SWIFT_DIR, server_search + '*',
                                           '.conf', dir_ext='.conf.d')
        number = kwargs.get('number')
        if number:
            try:
                conf_files = [found_conf_files[number - 1]]
            except IndexError:
                conf_files = []
        else:
            conf_files = found_conf_files
        if not conf_files:
            # maybe there's a config file(s) out there, but I couldn't find it!
            if not kwargs.get('quiet'):
                if number:
                    print(_('Unable to locate config number %(number)s for'
                            ' %(server)s') %
                          {'number': number, 'server': self.server})
                else:
                    print(_('Unable to locate config for %s') % self.server)
            if kwargs.get('verbose') and not kwargs.get('quiet'):
                if found_conf_files:
                    print(_('Found configs:'))
                for i, conf_file in enumerate(found_conf_files):
                    print('  %d) %s' % (i + 1, conf_file))

        return conf_files

    def pid_files(self, **kwargs):
        """Get pid files for this server

        :param: number, if supplied will only lookup the nth server

        :returns: list of pid files
        """
        if self.conf is not None:
            pid_files = search_tree(self.run_dir, '%s*' % self.server,
                                    exts=[self.conf + '.pid',
                                          self.conf + '.pid.d'])
        else:
            pid_files = search_tree(self.run_dir, '%s*' % self.server)
        if kwargs.get('number', 0):
            conf_files = self.conf_files(**kwargs)
            # filter pid_files to match the index of numbered conf_file
            pid_files = [pid_file for pid_file in pid_files if
                         self.get_conf_file_name(pid_file) in conf_files]
        return pid_files

    def iter_pid_files(self, **kwargs):
        """Generator, yields (pid_file, pids)
        """
        for pid_file in self.pid_files(**kwargs):
            try:
                pid = int(open(pid_file).read().strip())
            except ValueError:
                pid = None
            yield pid_file, pid

    def signal_pids(self, sig, **kwargs):
        """Send a signal to pids for this server

        :param sig: signal to send

        :returns: a dict mapping pids (ints) to pid_files (paths)

        """
        pids = {}
        for pid_file, pid in self.iter_pid_files(**kwargs):
            if not pid:  # Catches None and 0
                print(_('Removing pid file %s with invalid pid') % pid_file)
                remove_file(pid_file)
                continue
            try:
                if sig != signal.SIG_DFL:
                    print(_('Signal %(server)s  pid: %(pid)s  signal: '
                            '%(signal)s') %
                          {'server': self.server, 'pid': pid, 'signal': sig})
                safe_kill(pid, sig, 'swift-%s' % self.server)
            except InvalidPidFileException as e:
                if kwargs.get('verbose'):
                    print(_('Removing pid file %(pid_file)s with wrong pid '
                            '%(pid)d') % {'pid_file': pid_file, 'pid': pid})
                remove_file(pid_file)
            except OSError as e:
                if e.errno == errno.ESRCH:
                    # pid does not exist
                    if kwargs.get('verbose'):
                        print(_("Removing stale pid file %s") % pid_file)
                    remove_file(pid_file)
                elif e.errno == errno.EPERM:
                    print(_("No permission to signal PID %d") % pid)
            else:
                # process exists
                pids[pid] = pid_file
        return pids

    def get_running_pids(self, **kwargs):
        """Get running pids

        :returns: a dict mapping pids (ints) to pid_files (paths)

        """
        return self.signal_pids(signal.SIG_DFL, **kwargs)  # send noop

    def kill_running_pids(self, **kwargs):
        """Kill running pids

        :param graceful: if True, attempt SIGHUP on supporting servers

        :returns: a dict mapping pids (ints) to pid_files (paths)

        """
        graceful = kwargs.get('graceful')
        if graceful and self.server in GRACEFUL_SHUTDOWN_SERVERS:
            sig = signal.SIGHUP
        else:
            sig = signal.SIGTERM
        return self.signal_pids(sig, **kwargs)

    def status(self, pids=None, **kwargs):
        """Display status of server

        :param: pids, if not supplied pids will be populated automatically
        :param: number, if supplied will only lookup the nth server

        :returns: 1 if server is not running, 0 otherwise
        """
        if pids is None:
            pids = self.get_running_pids(**kwargs)
        if not pids:
            number = kwargs.get('number', 0)
            if number:
                kwargs['quiet'] = True
                conf_files = self.conf_files(**kwargs)
                if conf_files:
                    print(_("%(server)s #%(number)d not running (%(conf)s)") %
                          {'server': self.server, 'number': number,
                           'conf': conf_files[0]})
            else:
                print(_("No %s running") % self.server)
            return 1
        for pid, pid_file in pids.items():
            conf_file = self.get_conf_file_name(pid_file)
            print(_("%(server)s running (%(pid)s - %(conf)s)") %
                  {'server': self.server, 'pid': pid, 'conf': conf_file})
        return 0

    def spawn(self, conf_file, once=False, wait=True, daemon=True,
              additional_args=None, **kwargs):
        """Launch a subprocess for this server.

        :param conf_file: path to conf_file to use as first arg
        :param once: boolean, add once argument to command
        :param wait: boolean, if true capture stdout with a pipe
        :param daemon: boolean, if false ask server to log to console
        :param additional_args: list of additional arguments to pass
                                on the command line

        :returns: the pid of the spawned process
        """
        args = [self.cmd, conf_file]
        if once:
            args.append('once')
        if not daemon:
            # ask the server to log to console
            args.append('verbose')
        if additional_args:
            if isinstance(additional_args, str):
                additional_args = [additional_args]
            args.extend(additional_args)

        # figure out what we're going to do with stdio
        if not daemon:
            # do nothing, this process is open until the spawns close anyway
            re_out = None
            re_err = None
        else:
            re_err = subprocess.STDOUT
            if wait:
                # we're going to need to block on this...
                re_out = subprocess.PIPE
            else:
                re_out = open(os.devnull, 'w+b')
        proc = subprocess.Popen(args, stdout=re_out, stderr=re_err)
        pid_file = self.get_pid_file_name(conf_file)
        write_file(pid_file, proc.pid)
        self.procs.append(proc)
        return proc.pid

    def wait(self, **kwargs):
        """
        wait on spawned procs to start
        """
        status = 0
        for proc in self.procs:
            # wait for process to close its stdout (if we haven't done that)
            if proc.stdout.closed:
                output = ''
            else:
                output = proc.stdout.read()
                proc.stdout.close()

            if kwargs.get('once', False):
                # if you don't want once to wait you can send it to the
                # background on the command line, I generally just run with
                # no-daemon anyway, but this is quieter
                proc.wait()
            if output:
                print(output)
                start = time.time()
                # wait for process to die (output may just be a warning)
                while time.time() - start < WARNING_WAIT:
                    time.sleep(0.1)
                    if proc.poll() is not None:
                        status += proc.returncode
                        break
        return status

    def interact(self, **kwargs):
        """
        wait on spawned procs to terminate
        """
        status = 0
        for proc in self.procs:
            # wait for process to terminate
            proc.communicate()  # should handle closing pipes
            if proc.returncode:
                status += 1
        return status

    def launch(self, **kwargs):
        """
        Collect conf files and attempt to spawn the processes for this server
        """
        conf_files = self.conf_files(**kwargs)
        if not conf_files:
            return {}

        pids = self.get_running_pids(**kwargs)

        already_started = False
        for pid, pid_file in pids.items():
            conf_file = self.get_conf_file_name(pid_file)
            # for legacy compat you can't start other servers if one server is
            # already running (unless -n specifies which one you want), this
            # restriction could potentially be lifted, and launch could start
            # any unstarted instances
            if conf_file in conf_files:
                already_started = True
                print(_("%(server)s running (%(pid)s - %(conf)s)") %
                      {'server': self.server, 'pid': pid, 'conf': conf_file})
            elif not kwargs.get('number', 0):
                already_started = True
                print(_("%(server)s running (%(pid)s - %(pid_file)s)") %
                      {'server': self.server, 'pid': pid,
                       'pid_file': pid_file})

        if already_started:
            print(_("%s already started...") % self.server)
            return {}

        if self.server not in START_ONCE_SERVERS:
            kwargs['once'] = False

        pids = {}
        for conf_file in conf_files:
            if kwargs.get('once'):
                msg = _('Running %s once') % self.server
            else:
                msg = _('Starting %s') % self.server
            print('%s...(%s)' % (msg, conf_file))
            try:
                pid = self.spawn(conf_file, **kwargs)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    # TODO(clayg): should I check if self.cmd exists earlier?
                    print(_("%s does not exist") % self.cmd)
                    break
                else:
                    raise
            pids[pid] = conf_file

        return pids

    def stop(self, **kwargs):
        """Send stop signals to pids for this server

        :returns: a dict mapping pids (ints) to pid_files (paths)

        """
        return self.kill_running_pids(**kwargs)
