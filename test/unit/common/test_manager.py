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
import unittest
from test.unit import temptree

import mock
import os
import sys
import resource
import signal
import errno
from collections import defaultdict
from time import sleep, time
import tempfile

from six.moves import reload_module

from swift.common import manager
from swift.common.exceptions import InvalidPidFileException

import eventlet
threading = eventlet.patcher.original('threading')

DUMMY_SIG = 1


class MockOs(object):
    RAISE_EPERM_SIG = 99

    def __init__(self, pids):
        self.running_pids = pids
        self.pid_sigs = defaultdict(list)
        self.closed_fds = []
        self.child_pid = 9999  # fork defaults to test parent process path
        self.execlp_called = False

    def kill(self, pid, sig):
        if sig == self.RAISE_EPERM_SIG:
            raise OSError(errno.EPERM, 'Operation not permitted')
        if pid not in self.running_pids:
            raise OSError(3, 'No such process')
        self.pid_sigs[pid].append(sig)

    def __getattr__(self, name):
        # I only over-ride portions of the os module
        try:
            return object.__getattr__(self, name)
        except AttributeError:
            return getattr(os, name)


def pop_stream(f):
    """read everything out of file from the top and clear it out
    """
    f.flush()
    f.seek(0)
    output = f.read()
    f.seek(0)
    f.truncate()
    return output


class TestManagerModule(unittest.TestCase):

    def test_servers(self):
        main_plus_rest = set(manager.MAIN_SERVERS + manager.REST_SERVERS)
        self.assertEqual(set(manager.ALL_SERVERS), main_plus_rest)
        # make sure there's no server listed in both
        self.assertEqual(len(main_plus_rest), len(manager.MAIN_SERVERS) +
                         len(manager.REST_SERVERS))

    def test_setup_env(self):
        class MockResource(object):
            def __init__(self, error=None):
                self.error = error
                self.called_with_args = []

            def setrlimit(self, resource, limits):
                if self.error:
                    raise self.error
                self.called_with_args.append((resource, limits))

            def __getattr__(self, name):
                # I only over-ride portions of the resource module
                try:
                    return object.__getattr__(self, name)
                except AttributeError:
                    return getattr(resource, name)

        _orig_resource = manager.resource
        _orig_environ = os.environ
        try:
            manager.resource = MockResource()
            manager.os.environ = {}
            manager.setup_env()
            expected = [
                (resource.RLIMIT_NOFILE, (manager.MAX_DESCRIPTORS,
                                          manager.MAX_DESCRIPTORS)),
                (resource.RLIMIT_DATA, (manager.MAX_MEMORY,
                                        manager.MAX_MEMORY)),
                (resource.RLIMIT_NPROC, (manager.MAX_PROCS,
                                         manager.MAX_PROCS)),
            ]
            self.assertEqual(manager.resource.called_with_args, expected)
            self.assertTrue(
                manager.os.environ['PYTHON_EGG_CACHE'].startswith(
                    tempfile.gettempdir()))

            # test error condition
            manager.resource = MockResource(error=ValueError())
            manager.os.environ = {}
            manager.setup_env()
            self.assertEqual(manager.resource.called_with_args, [])
            self.assertTrue(
                manager.os.environ['PYTHON_EGG_CACHE'].startswith(
                    tempfile.gettempdir()))

            manager.resource = MockResource(error=OSError())
            manager.os.environ = {}
            self.assertRaises(OSError, manager.setup_env)
            self.assertIsNone(manager.os.environ.get('PYTHON_EGG_CACHE'))
        finally:
            manager.resource = _orig_resource
            os.environ = _orig_environ

    def test_command_wrapper(self):
        @manager.command
        def myfunc(arg1):
            """test doc
            """
            return arg1

        self.assertEqual(myfunc.__doc__.strip(), 'test doc')
        self.assertEqual(myfunc(1), 1)
        self.assertEqual(myfunc(0), 0)
        self.assertEqual(myfunc(True), 1)
        self.assertEqual(myfunc(False), 0)
        self.assertTrue(hasattr(myfunc, 'publicly_accessible'))
        self.assertTrue(myfunc.publicly_accessible)

    def test_watch_server_pids(self):
        class MockOs(object):
            WNOHANG = os.WNOHANG

            def __init__(self, pid_map=None):
                if pid_map is None:
                    pid_map = {}
                self.pid_map = {}
                for pid, v in pid_map.items():
                    self.pid_map[pid] = (x for x in v)

            def waitpid(self, pid, options):
                try:
                    rv = next(self.pid_map[pid])
                except StopIteration:
                    raise OSError(errno.ECHILD, os.strerror(errno.ECHILD))
                except KeyError:
                    raise OSError(errno.ESRCH, os.strerror(errno.ESRCH))
                if isinstance(rv, Exception):
                    raise rv
                else:
                    return rv

        class MockTime(object):
            def __init__(self, ticks=None):
                self.tock = time()
                if not ticks:
                    ticks = []

                self.ticks = (t for t in ticks)

            def time(self):
                try:
                    self.tock += next(self.ticks)
                except StopIteration:
                    self.tock += 1
                return self.tock

            def sleep(*args):
                return

        class MockServer(object):

            def __init__(self, pids, run_dir=manager.RUN_DIR, zombie=0):
                self.heartbeat = (pids for _ in range(zombie))

            def get_running_pids(self):
                try:
                    rv = next(self.heartbeat)
                    return rv
                except StopIteration:
                    return {}

        _orig_os = manager.os
        _orig_time = manager.time
        _orig_server = manager.Server
        try:
            manager.time = MockTime()
            manager.os = MockOs()
            # this server always says it's dead when you ask for running pids
            server = MockServer([1])
            # list of pids keyed on servers to watch
            server_pids = {
                server: [1],
            }
            # basic test, server dies
            gen = manager.watch_server_pids(server_pids)
            expected = [(server, 1)]
            self.assertEqual([x for x in gen], expected)
            # start long running server and short interval
            server = MockServer([1], zombie=15)
            server_pids = {
                server: [1],
            }
            gen = manager.watch_server_pids(server_pids)
            self.assertEqual([x for x in gen], [])
            # wait a little longer
            gen = manager.watch_server_pids(server_pids, interval=15)
            self.assertEqual([x for x in gen], [(server, 1)])
            # zombie process
            server = MockServer([1], zombie=200)
            server_pids = {
                server: [1],
            }
            # test weird os error
            manager.os = MockOs({1: [OSError()]})
            gen = manager.watch_server_pids(server_pids)
            self.assertRaises(OSError, lambda: [x for x in gen])
            # test multi-server
            server1 = MockServer([1, 10], zombie=200)
            server2 = MockServer([2, 20], zombie=8)
            server_pids = {
                server1: [1, 10],
                server2: [2, 20],
            }
            pid_map = {
                1: [None for _ in range(10)],
                2: [None for _ in range(8)],
                20: [None for _ in range(4)],
            }
            manager.os = MockOs(pid_map)
            gen = manager.watch_server_pids(server_pids,
                                            interval=manager.KILL_WAIT)
            expected = [
                (server2, 2),
                (server2, 20),
            ]
            self.assertEqual([x for x in gen], expected)

        finally:
            manager.os = _orig_os
            manager.time = _orig_time
            manager.Server = _orig_server

    def test_safe_kill(self):
        manager.os = MockOs([1, 2, 3, 4])

        proc_files = (
            ('1/cmdline', 'same-procname'),
            ('2/cmdline', 'another-procname'),
            ('4/cmdline', 'another-procname'),
        )
        files, contents = zip(*proc_files)
        with temptree(files, contents) as t:
            manager.PROC_DIR = t
            manager.safe_kill(1, signal.SIG_DFL, 'same-procname')
            self.assertRaises(InvalidPidFileException, manager.safe_kill,
                              2, signal.SIG_DFL, 'same-procname')
            manager.safe_kill(3, signal.SIG_DFL, 'same-procname')
            manager.safe_kill(4, signal.SIGHUP, 'same-procname')

    def test_exc(self):
        self.assertTrue(issubclass(manager.UnknownCommandError, Exception))


class TestServer(unittest.TestCase):

    def tearDown(self):
        reload_module(manager)

    def join_swift_dir(self, path):
        return os.path.join(manager.SWIFT_DIR, path)

    def join_run_dir(self, path):
        return os.path.join(manager.RUN_DIR, path)

    def test_create_server(self):
        server = manager.Server('proxy')
        self.assertEqual(server.server, 'proxy-server')
        self.assertEqual(server.type, 'proxy')
        self.assertEqual(server.cmd, 'swift-proxy-server')
        server = manager.Server('object-replicator')
        self.assertEqual(server.server, 'object-replicator')
        self.assertEqual(server.type, 'object')
        self.assertEqual(server.cmd, 'swift-object-replicator')

    def test_server_to_string(self):
        server = manager.Server('Proxy')
        self.assertEqual(str(server), 'proxy-server')
        server = manager.Server('object-replicator')
        self.assertEqual(str(server), 'object-replicator')

    def test_server_repr(self):
        server = manager.Server('proxy')
        self.assertTrue(server.__class__.__name__ in repr(server))
        self.assertTrue(str(server) in repr(server))

    def test_server_equality(self):
        server1 = manager.Server('Proxy')
        server2 = manager.Server('proxy-server')
        self.assertEqual(server1, server2)
        # it is NOT a string
        self.assertNotEqual(server1, 'proxy-server')

    def test_get_pid_file_name(self):
        server = manager.Server('proxy')
        conf_file = self.join_swift_dir('proxy-server.conf')
        pid_file = self.join_run_dir('proxy-server.pid')
        self.assertEqual(pid_file, server.get_pid_file_name(conf_file))
        server = manager.Server('object-replicator')
        conf_file = self.join_swift_dir('object-server/1.conf')
        pid_file = self.join_run_dir('object-replicator/1.pid')
        self.assertEqual(pid_file, server.get_pid_file_name(conf_file))
        server = manager.Server('container-auditor')
        conf_file = self.join_swift_dir(
            'container-server/1/container-auditor.conf')
        pid_file = self.join_run_dir(
            'container-auditor/1/container-auditor.pid')
        self.assertEqual(pid_file, server.get_pid_file_name(conf_file))

    def test_get_custom_pid_file_name(self):
        random_run_dir = "/random/dir"
        get_random_run_dir = lambda x: os.path.join(random_run_dir, x)
        server = manager.Server('proxy', run_dir=random_run_dir)
        conf_file = self.join_swift_dir('proxy-server.conf')
        pid_file = get_random_run_dir('proxy-server.pid')
        self.assertEqual(pid_file, server.get_pid_file_name(conf_file))
        server = manager.Server('object-replicator', run_dir=random_run_dir)
        conf_file = self.join_swift_dir('object-server/1.conf')
        pid_file = get_random_run_dir('object-replicator/1.pid')
        self.assertEqual(pid_file, server.get_pid_file_name(conf_file))
        server = manager.Server('container-auditor', run_dir=random_run_dir)
        conf_file = self.join_swift_dir(
            'container-server/1/container-auditor.conf')
        pid_file = get_random_run_dir(
            'container-auditor/1/container-auditor.pid')
        self.assertEqual(pid_file, server.get_pid_file_name(conf_file))

    def test_get_conf_file_name(self):
        server = manager.Server('proxy')
        conf_file = self.join_swift_dir('proxy-server.conf')
        pid_file = self.join_run_dir('proxy-server.pid')
        self.assertEqual(conf_file, server.get_conf_file_name(pid_file))
        server = manager.Server('object-replicator')
        conf_file = self.join_swift_dir('object-server/1.conf')
        pid_file = self.join_run_dir('object-replicator/1.pid')
        self.assertEqual(conf_file, server.get_conf_file_name(pid_file))
        server = manager.Server('container-auditor')
        conf_file = self.join_swift_dir(
            'container-server/1/container-auditor.conf')
        pid_file = self.join_run_dir(
            'container-auditor/1/container-auditor.pid')
        self.assertEqual(conf_file, server.get_conf_file_name(pid_file))
        server_name = manager.STANDALONE_SERVERS[0]
        server = manager.Server(server_name)
        conf_file = self.join_swift_dir(server_name + '.conf')
        pid_file = self.join_run_dir(server_name + '.pid')
        self.assertEqual(conf_file, server.get_conf_file_name(pid_file))

    def test_conf_files(self):
        # test get single conf file
        conf_files = (
            'proxy-server.conf',
            'proxy-server.ini',
            'auth-server.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server('proxy')
            conf_files = server.conf_files()
            self.assertEqual(len(conf_files), 1)
            conf_file = conf_files[0]
            proxy_conf = self.join_swift_dir('proxy-server.conf')
            self.assertEqual(conf_file, proxy_conf)

        # test multi server conf files & grouping of server-type config
        conf_files = (
            'object-server1.conf',
            'object-server/2.conf',
            'object-server/object3.conf',
            'object-server/conf/server4.conf',
            'object-server.txt',
            'proxy-server.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server('object-replicator')
            conf_files = server.conf_files()
            self.assertEqual(len(conf_files), 4)
            c1 = self.join_swift_dir('object-server1.conf')
            c2 = self.join_swift_dir('object-server/2.conf')
            c3 = self.join_swift_dir('object-server/object3.conf')
            c4 = self.join_swift_dir('object-server/conf/server4.conf')
            for c in [c1, c2, c3, c4]:
                self.assertTrue(c in conf_files)
            # test configs returned sorted
            sorted_confs = sorted([c1, c2, c3, c4])
            self.assertEqual(conf_files, sorted_confs)

        # test get single numbered conf
        conf_files = (
            'account-server/1.conf',
            'account-server/2.conf',
            'account-server/3.conf',
            'account-server/4.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server('account')
            conf_files = server.conf_files(number=2)
            self.assertEqual(len(conf_files), 1)
            conf_file = conf_files[0]
            self.assertEqual(conf_file,
                             self.join_swift_dir('account-server/2.conf'))
            # test missing config number
            conf_files = server.conf_files(number=5)
            self.assertFalse(conf_files)

        # test getting specific conf
        conf_files = (
            'account-server/1.conf',
            'account-server/2.conf',
            'account-server/3.conf',
            'account-server/4.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server('account.2')
            conf_files = server.conf_files()
            self.assertEqual(len(conf_files), 1)
            conf_file = conf_files[0]
            self.assertEqual(conf_file,
                             self.join_swift_dir('account-server/2.conf'))

        # test verbose & quiet
        conf_files = (
            'auth-server.ini',
            'container-server/1.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            old_stdout = sys.stdout
            try:
                with open(os.path.join(t, 'output'), 'w+') as f:
                    sys.stdout = f
                    server = manager.Server('auth')
                    # check warn "unable to locate"
                    conf_files = server.conf_files()
                    self.assertFalse(conf_files)
                    self.assertTrue('unable to locate config for auth'
                                    in pop_stream(f).lower())
                    # check quiet will silence warning
                    conf_files = server.conf_files(verbose=True, quiet=True)
                    self.assertEqual(pop_stream(f), '')
                    # check found config no warning
                    server = manager.Server('container-auditor')
                    conf_files = server.conf_files()
                    self.assertEqual(pop_stream(f), '')
                    # check missing config number warn "unable to locate"
                    conf_files = server.conf_files(number=2)
                    self.assertTrue(
                        'unable to locate config number 2 for ' +
                        'container-auditor' in pop_stream(f).lower())
                    # check verbose lists configs
                    conf_files = server.conf_files(number=2, verbose=True)
                    c1 = self.join_swift_dir('container-server/1.conf')
                    self.assertIn(c1, pop_stream(f))
            finally:
                sys.stdout = old_stdout

        # test standalone conf file
        server_name = manager.STANDALONE_SERVERS[0]
        conf_files = (server_name + '.conf',)
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server(server_name)
            conf_files = server.conf_files()
            self.assertEqual(len(conf_files), 1)
            conf_file = conf_files[0]
            conf = self.join_swift_dir(server_name + '.conf')
            self.assertEqual(conf_file, conf)

    def test_proxy_conf_dir(self):
        conf_files = (
            'proxy-server.conf.d/00.conf',
            'proxy-server.conf.d/01.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server('proxy')
            conf_dirs = server.conf_files()
            self.assertEqual(len(conf_dirs), 1)
            conf_dir = conf_dirs[0]
            proxy_conf_dir = self.join_swift_dir('proxy-server.conf.d')
            self.assertEqual(proxy_conf_dir, conf_dir)

    def test_named_conf_dir(self):
        conf_files = (
            'object-server/base.conf-template',
            'object-server/object-server.conf.d/00_base.conf',
            'object-server/object-server.conf.d/10_server.conf',
            'object-server/object-replication.conf.d/00_base.conf',
            'object-server/object-replication.conf.d/10_server.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server('object.replication')
            conf_dirs = server.conf_files()
            self.assertEqual(len(conf_dirs), 1)
            conf_dir = conf_dirs[0]
            replication_server_conf_dir = self.join_swift_dir(
                'object-server/object-replication.conf.d')
            self.assertEqual(replication_server_conf_dir, conf_dir)
            # and again with no named filter
            server = manager.Server('object')
            conf_dirs = server.conf_files()
            self.assertEqual(len(conf_dirs), 2)
            for named_conf in ('server', 'replication'):
                conf_dir = self.join_swift_dir(
                    'object-server/object-%s.conf.d' % named_conf)
                self.assertTrue(conf_dir in conf_dirs)

    def test_conf_dir(self):
        conf_files = (
            'object-server/object-server.conf-base',
            'object-server/1.conf.d/base.conf',
            'object-server/1.conf.d/1.conf',
            'object-server/2.conf.d/base.conf',
            'object-server/2.conf.d/2.conf',
            'object-server/3.conf.d/base.conf',
            'object-server/3.conf.d/3.conf',
            'object-server/4.conf.d/base.conf',
            'object-server/4.conf.d/4.conf',
        )
        with temptree(conf_files) as t:
            manager.SWIFT_DIR = t
            server = manager.Server('object-replicator')
            conf_dirs = server.conf_files()
            self.assertEqual(len(conf_dirs), 4)
            c1 = self.join_swift_dir('object-server/1.conf.d')
            c2 = self.join_swift_dir('object-server/2.conf.d')
            c3 = self.join_swift_dir('object-server/3.conf.d')
            c4 = self.join_swift_dir('object-server/4.conf.d')
            for c in [c1, c2, c3, c4]:
                self.assertTrue(c in conf_dirs)
            # test configs returned sorted
            sorted_confs = sorted([c1, c2, c3, c4])
            self.assertEqual(conf_dirs, sorted_confs)

    def test_named_conf_dir_pid_files(self):
        conf_files = (
            'object-server/object-server.pid.d',
            'object-server/object-replication.pid.d',
        )
        with temptree(conf_files) as t:
            manager.RUN_DIR = t
            server = manager.Server('object.replication', run_dir=t)
            pid_files = server.pid_files()
            self.assertEqual(len(pid_files), 1)
            pid_file = pid_files[0]
            replication_server_pid = self.join_run_dir(
                'object-server/object-replication.pid.d')
            self.assertEqual(replication_server_pid, pid_file)
            # and again with no named filter
            server = manager.Server('object', run_dir=t)
            pid_files = server.pid_files()
            self.assertEqual(len(pid_files), 2)
            for named_pid in ('server', 'replication'):
                pid_file = self.join_run_dir(
                    'object-server/object-%s.pid.d' % named_pid)
                self.assertTrue(pid_file in pid_files)

    def test_iter_pid_files(self):
        """
        Server.iter_pid_files is kinda boring, test the
        Server.pid_files stuff here as well
        """
        pid_files = (
            ('proxy-server.pid', 1),
            ('auth-server.pid', 'blah'),
            ('object-replicator/1.pid', 11),
            ('object-replicator/2.pid', 12),
        )
        files, contents = zip(*pid_files)
        with temptree(files, contents) as t:
            manager.RUN_DIR = t
            server = manager.Server('proxy', run_dir=t)
            # test get one file
            iterator = server.iter_pid_files()
            pid_file, pid = next(iterator)
            self.assertEqual(pid_file, self.join_run_dir('proxy-server.pid'))
            self.assertEqual(pid, 1)
            # ... and only one file
            self.assertRaises(StopIteration, next, iterator)
            # test invalid value in pid file
            server = manager.Server('auth', run_dir=t)
            pid_file, pid = next(server.iter_pid_files())
            self.assertIsNone(pid)
            # test object-server doesn't steal pids from object-replicator
            server = manager.Server('object', run_dir=t)
            self.assertRaises(StopIteration, next, server.iter_pid_files())
            # test multi-pid iter
            server = manager.Server('object-replicator', run_dir=t)
            real_map = {
                11: self.join_run_dir('object-replicator/1.pid'),
                12: self.join_run_dir('object-replicator/2.pid'),
            }
            pid_map = {}
            for pid_file, pid in server.iter_pid_files():
                pid_map[pid] = pid_file
            self.assertEqual(pid_map, real_map)

        # test get pid_files by number
        conf_files = (
            'object-server/1.conf',
            'object-server/2.conf',
            'object-server/3.conf',
            'object-server/4.conf',
        )

        pid_files = (
            ('object-server/1.pid', 1),
            ('object-server/2.pid', 2),
            ('object-server/5.pid', 5),
        )

        with temptree(conf_files) as swift_dir:
            manager.SWIFT_DIR = swift_dir
            files, pids = zip(*pid_files)
            with temptree(files, pids) as t:
                manager.RUN_DIR = t
                server = manager.Server('object', run_dir=t)
                # test get all pid files
                real_map = {
                    1: self.join_run_dir('object-server/1.pid'),
                    2: self.join_run_dir('object-server/2.pid'),
                    5: self.join_run_dir('object-server/5.pid'),
                }
                pid_map = {}
                for pid_file, pid in server.iter_pid_files():
                    pid_map[pid] = pid_file
                self.assertEqual(pid_map, real_map)
                # test get pid with matching conf
                pids = list(server.iter_pid_files(number=2))
                self.assertEqual(len(pids), 1)
                pid_file, pid = pids[0]
                self.assertEqual(pid, 2)
                pid_two = self.join_run_dir('object-server/2.pid')
                self.assertEqual(pid_file, pid_two)
                # try to iter on a pid number with a matching conf but no pid
                pids = list(server.iter_pid_files(number=3))
                self.assertFalse(pids)
                # test get pids w/o matching conf
                pids = list(server.iter_pid_files(number=5))
                self.assertFalse(pids)

        # test get pid_files by conf name
        conf_files = (
            'object-server/1.conf',
            'object-server/2.conf',
            'object-server/3.conf',
            'object-server/4.conf',
        )

        pid_files = (
            ('object-server/1.pid', 1),
            ('object-server/2.pid', 2),
            ('object-server/5.pid', 5),
        )

        with temptree(conf_files) as swift_dir:
            manager.SWIFT_DIR = swift_dir
            files, pids = zip(*pid_files)
            with temptree(files, pids) as t:
                manager.RUN_DIR = t
                server = manager.Server('object.2', run_dir=t)
                # test get pid with matching conf
                pids = list(server.iter_pid_files())
                self.assertEqual(len(pids), 1)
                pid_file, pid = pids[0]
                self.assertEqual(pid, 2)
                pid_two = self.join_run_dir('object-server/2.pid')
                self.assertEqual(pid_file, pid_two)

    def test_signal_pids(self):
        temp_files = (
            ('var/run/zero-server.pid', 0),
            ('var/run/proxy-server.pid', 1),
            ('var/run/auth-server.pid', 2),
            ('var/run/one-server.pid', 3),
            ('var/run/object-server.pid', 4),
            ('var/run/invalid-server.pid', 'Forty-Two'),
            ('proc/3/cmdline', 'swift-another-server')
        )
        with temptree(*zip(*temp_files)) as t:
            manager.RUN_DIR = os.path.join(t, 'var/run')
            manager.PROC_DIR = os.path.join(t, 'proc')
            # mock os with so both the first and second are running
            manager.os = MockOs([1, 2])
            server = manager.Server('proxy', run_dir=manager.RUN_DIR)
            pids = server.signal_pids(DUMMY_SIG)
            self.assertEqual(len(pids), 1)
            self.assertIn(1, pids)
            self.assertEqual(manager.os.pid_sigs[1], [DUMMY_SIG])
            # make sure other process not signaled
            self.assertNotIn(2, pids)
            self.assertNotIn(2, manager.os.pid_sigs)
            # capture stdio
            old_stdout = sys.stdout
            try:
                with open(os.path.join(t, 'output'), 'w+') as f:
                    sys.stdout = f
                    # test print details
                    pids = server.signal_pids(DUMMY_SIG)
                    output = pop_stream(f)
                    self.assertIn('pid: %s' % 1, output)
                    self.assertIn('signal: %s' % DUMMY_SIG, output)
                    # test no details on signal.SIG_DFL
                    pids = server.signal_pids(signal.SIG_DFL)
                    self.assertEqual(pop_stream(f), '')
                    # reset mock os so only the second server is running
                    manager.os = MockOs([2])
                    # test pid not running
                    pids = server.signal_pids(signal.SIG_DFL)
                    self.assertNotIn(1, pids)
                    self.assertNotIn(1, manager.os.pid_sigs)
                    # test remove stale pid file
                    self.assertFalse(os.path.exists(
                        self.join_run_dir('proxy-server.pid')))
                    # reset mock os with no running pids
                    manager.os = MockOs([])
                    server = manager.Server('auth', run_dir=manager.RUN_DIR)
                    # test verbose warns on removing stale pid file
                    pids = server.signal_pids(signal.SIG_DFL, verbose=True)
                    output = pop_stream(f)
                    self.assertTrue('stale pid' in output.lower())
                    auth_pid = self.join_run_dir('auth-server.pid')
                    self.assertTrue(auth_pid in output)
                    # reset mock os so only the third server is running
                    manager.os = MockOs([3])
                    server = manager.Server('one', run_dir=manager.RUN_DIR)
                    # test verbose warns on removing invalid pid file
                    pids = server.signal_pids(signal.SIG_DFL, verbose=True)
                    output = pop_stream(f)
                    old_stdout.write('output %s' % output)
                    self.assertTrue('removing pid file' in output.lower())
                    one_pid = self.join_run_dir('one-server.pid')
                    self.assertTrue(one_pid in output)

                    server = manager.Server('zero', run_dir=manager.RUN_DIR)
                    self.assertTrue(os.path.exists(
                        self.join_run_dir('zero-server.pid')))  # sanity
                    # test verbose warns on removing pid file with invalid pid
                    pids = server.signal_pids(signal.SIG_DFL, verbose=True)
                    output = pop_stream(f)
                    old_stdout.write('output %s' % output)
                    self.assertTrue('with invalid pid' in output.lower())
                    self.assertFalse(os.path.exists(
                        self.join_run_dir('zero-server.pid')))
                    server = manager.Server('invalid-server',
                                            run_dir=manager.RUN_DIR)
                    self.assertTrue(os.path.exists(
                        self.join_run_dir('invalid-server.pid')))  # sanity
                    # test verbose warns on removing pid file with invalid pid
                    pids = server.signal_pids(signal.SIG_DFL, verbose=True)
                    output = pop_stream(f)
                    old_stdout.write('output %s' % output)
                    self.assertTrue('with invalid pid' in output.lower())
                    self.assertFalse(os.path.exists(
                        self.join_run_dir('invalid-server.pid')))

                    # reset mock os with no running pids
                    manager.os = MockOs([])
                    # test warning with insufficient permissions
                    server = manager.Server('object', run_dir=manager.RUN_DIR)
                    pids = server.signal_pids(manager.os.RAISE_EPERM_SIG)
                    output = pop_stream(f)
                    self.assertTrue('no permission to signal pid 4' in
                                    output.lower(), output)
            finally:
                sys.stdout = old_stdout

    def test_get_running_pids(self):
        # test only gets running pids
        temp_files = (
            ('var/run/test-server1.pid', 1),
            ('var/run/test-server2.pid', 2),
            ('var/run/test-server3.pid', 3),
            ('proc/1/cmdline', 'swift-test-server'),
            ('proc/3/cmdline', 'swift-another-server')
        )
        with temptree(*zip(*temp_files)) as t:
            manager.RUN_DIR = os.path.join(t, 'var/run')
            manager.PROC_DIR = os.path.join(t, 'proc')
            server = manager.Server(
                'test-server', run_dir=manager.RUN_DIR)
            # mock os, only pid '1' is running
            manager.os = MockOs([1, 3])
            running_pids = server.get_running_pids()
            self.assertEqual(len(running_pids), 1)
            self.assertIn(1, running_pids)
            self.assertNotIn(2, running_pids)
            self.assertNotIn(3, running_pids)
            # test persistent running pid files
            self.assertTrue(os.path.exists(
                os.path.join(manager.RUN_DIR, 'test-server1.pid')))
            # test clean up stale pids
            pid_two = self.join_swift_dir('test-server2.pid')
            self.assertFalse(os.path.exists(pid_two))
            pid_three = self.join_swift_dir('test-server3.pid')
            self.assertFalse(os.path.exists(pid_three))
            # reset mock os, no pids running
            manager.os = MockOs([])
            running_pids = server.get_running_pids()
            self.assertFalse(running_pids)
            # and now all pid files are cleaned out
            pid_one = self.join_run_dir('test-server1.pid')
            self.assertFalse(os.path.exists(pid_one))
            all_pids = os.listdir(manager.RUN_DIR)
            self.assertEqual(len(all_pids), 0)

        # test only get pids for right server
        pid_files = (
            ('thing-doer.pid', 1),
            ('thing-sayer.pid', 2),
            ('other-doer.pid', 3),
            ('other-sayer.pid', 4),
        )
        files, pids = zip(*pid_files)
        with temptree(files, pids) as t:
            manager.RUN_DIR = t
            # all pids are running
            manager.os = MockOs(pids)
            server = manager.Server('thing-doer', run_dir=t)
            running_pids = server.get_running_pids()
            # only thing-doer.pid, 1
            self.assertEqual(len(running_pids), 1)
            self.assertIn(1, running_pids)
            # no other pids returned
            for n in (2, 3, 4):
                self.assertNotIn(n, running_pids)
            # assert stale pids for other servers ignored
            manager.os = MockOs([1])  # only thing-doer is running
            running_pids = server.get_running_pids()
            for f in ('thing-sayer.pid', 'other-doer.pid', 'other-sayer.pid'):
                # other server pid files persist
                self.assertTrue(os.path.exists, os.path.join(t, f))
            # verify that servers are in fact not running
            for server_name in ('thing-sayer', 'other-doer', 'other-sayer'):
                server = manager.Server(server_name, run_dir=t)
                running_pids = server.get_running_pids()
                self.assertFalse(running_pids)
            # and now all OTHER pid files are cleaned out
            all_pids = os.listdir(t)
            self.assertEqual(len(all_pids), 1)
            self.assertTrue(os.path.exists(os.path.join(t, 'thing-doer.pid')))

    def test_kill_running_pids(self):
        pid_files = (
            ('object-server.pid', 1),
            ('object-replicator1.pid', 11),
            ('object-replicator2.pid', 12),
        )
        files, running_pids = zip(*pid_files)
        with temptree(files, running_pids) as t:
            manager.RUN_DIR = t
            server = manager.Server('object', run_dir=t)
            # test no servers running
            manager.os = MockOs([])
            pids = server.kill_running_pids()
            self.assertFalse(pids, pids)
        files, running_pids = zip(*pid_files)
        with temptree(files, running_pids) as t:
            manager.RUN_DIR = t
            server.run_dir = t
            # start up pid
            manager.os = MockOs([1])
            server = manager.Server('object', run_dir=t)
            # test kill one pid
            pids = server.kill_running_pids()
            self.assertEqual(len(pids), 1)
            self.assertIn(1, pids)
            self.assertEqual(manager.os.pid_sigs[1], [signal.SIGTERM])
            # reset os mock
            manager.os = MockOs([1])
            # test shutdown
            self.assertTrue('object-server' in
                            manager.GRACEFUL_SHUTDOWN_SERVERS)
            pids = server.kill_running_pids(graceful=True)
            self.assertEqual(len(pids), 1)
            self.assertIn(1, pids)
            self.assertEqual(manager.os.pid_sigs[1], [signal.SIGHUP])
            # start up other servers
            manager.os = MockOs([11, 12])
            # test multi server kill & ignore graceful on unsupported server
            self.assertFalse('object-replicator' in
                             manager.GRACEFUL_SHUTDOWN_SERVERS)
            server = manager.Server('object-replicator', run_dir=t)
            pids = server.kill_running_pids(graceful=True)
            self.assertEqual(len(pids), 2)
            for pid in (11, 12):
                self.assertTrue(pid in pids)
                self.assertEqual(manager.os.pid_sigs[pid],
                                 [signal.SIGTERM])
            # and the other pid is of course not signaled
            self.assertNotIn(1, manager.os.pid_sigs)

    def test_status(self):
        conf_files = (
            'test-server/1.conf',
            'test-server/2.conf',
            'test-server/3.conf',
            'test-server/4.conf',
        )

        pid_files = (
            ('test-server/1.pid', 1),
            ('test-server/2.pid', 2),
            ('test-server/3.pid', 3),
            ('test-server/4.pid', 4),
        )

        with temptree(conf_files) as swift_dir:
            manager.SWIFT_DIR = swift_dir
            files, pids = zip(*pid_files)
            with temptree(files, pids) as t:
                manager.RUN_DIR = t
                # setup running servers
                server = manager.Server('test', run_dir=t)
                # capture stdio
                old_stdout = sys.stdout
                try:
                    with open(os.path.join(t, 'output'), 'w+') as f:
                        sys.stdout = f
                        # test status for all running
                        manager.os = MockOs(pids)
                        proc_files = (
                            ('1/cmdline', 'swift-test-server'),
                            ('2/cmdline', 'swift-test-server'),
                            ('3/cmdline', 'swift-test-server'),
                            ('4/cmdline', 'swift-test-server'),
                        )
                        files, contents = zip(*proc_files)
                        with temptree(files, contents) as t:
                            manager.PROC_DIR = t
                            self.assertEqual(server.status(), 0)
                            output = pop_stream(f).strip().splitlines()
                            self.assertEqual(len(output), 4)
                            for line in output:
                                self.assertTrue('test-server running' in line)
                        # test get single server by number
                        with temptree([], []) as t:
                            manager.PROC_DIR = t
                            self.assertEqual(server.status(number=4), 0)
                            output = pop_stream(f).strip().splitlines()
                            self.assertEqual(len(output), 1)
                            line = output[0]
                            self.assertTrue('test-server running' in line)
                            conf_four = self.join_swift_dir(conf_files[3])
                            self.assertTrue('4 - %s' % conf_four in line)
                        # test some servers not running
                        manager.os = MockOs([1, 2, 3])
                        proc_files = (
                            ('1/cmdline', 'swift-test-server'),
                            ('2/cmdline', 'swift-test-server'),
                            ('3/cmdline', 'swift-test-server'),
                        )
                        files, contents = zip(*proc_files)
                        with temptree(files, contents) as t:
                            manager.PROC_DIR = t
                            self.assertEqual(server.status(), 0)
                            output = pop_stream(f).strip().splitlines()
                            self.assertEqual(len(output), 3)
                            for line in output:
                                self.assertTrue('test-server running' in line)
                        # test single server not running
                        manager.os = MockOs([1, 2])
                        proc_files = (
                            ('1/cmdline', 'swift-test-server'),
                            ('2/cmdline', 'swift-test-server'),
                        )
                        files, contents = zip(*proc_files)
                        with temptree(files, contents) as t:
                            manager.PROC_DIR = t
                            self.assertEqual(server.status(number=3), 1)
                            output = pop_stream(f).strip().splitlines()
                            self.assertEqual(len(output), 1)
                            line = output[0]
                            self.assertTrue('not running' in line)
                            conf_three = self.join_swift_dir(conf_files[2])
                            self.assertTrue(conf_three in line)
                        # test no running pids
                        manager.os = MockOs([])
                        with temptree([], []) as t:
                            manager.PROC_DIR = t
                            self.assertEqual(server.status(), 1)
                            output = pop_stream(f).lower()
                            self.assertTrue('no test-server running' in output)
                        # test use provided pids
                        pids = {
                            1: '1.pid',
                            2: '2.pid',
                        }
                        # shouldn't call get_running_pids
                        called = []

                        def mock(*args, **kwargs):
                            called.append(True)
                        server.get_running_pids = mock
                        status = server.status(pids=pids)
                        self.assertEqual(status, 0)
                        self.assertFalse(called)
                        output = pop_stream(f).strip().splitlines()
                        self.assertEqual(len(output), 2)
                        for line in output:
                            self.assertTrue('test-server running' in line)
                finally:
                    sys.stdout = old_stdout

    def test_spawn(self):

        # mocks
        class MockProcess(object):

            NOTHING = 'default besides None'
            STDOUT = 'stdout'
            PIPE = 'pipe'

            def __init__(self, pids=None):
                if pids is None:
                    pids = []
                self.pids = (p for p in pids)

            def Popen(self, args, **kwargs):
                return MockProc(next(self.pids), args, **kwargs)

        class MockProc(object):

            def __init__(self, pid, args, stdout=MockProcess.NOTHING,
                         stderr=MockProcess.NOTHING):
                self.pid = pid
                self.args = args
                self.stdout = stdout
                if stderr == MockProcess.STDOUT:
                    self.stderr = self.stdout
                else:
                    self.stderr = stderr

        # setup running servers
        server = manager.Server('test')

        with temptree(['test-server.conf']) as swift_dir:
            manager.SWIFT_DIR = swift_dir
            with temptree([]) as t:
                manager.RUN_DIR = t
                server.run_dir = t
                old_subprocess = manager.subprocess
                try:
                    # test single server process calls spawn once
                    manager.subprocess = MockProcess([1])
                    conf_file = self.join_swift_dir('test-server.conf')
                    # spawn server no kwargs
                    server.spawn(conf_file)
                    # test pid file
                    pid_file = self.join_run_dir('test-server.pid')
                    self.assertTrue(os.path.exists(pid_file))
                    pid_on_disk = int(open(pid_file).read().strip())
                    self.assertEqual(pid_on_disk, 1)
                    # assert procs args
                    self.assertTrue(server.procs)
                    self.assertEqual(len(server.procs), 1)
                    proc = server.procs[0]
                    expected_args = [
                        'swift-test-server',
                        conf_file,
                    ]
                    self.assertEqual(proc.args, expected_args)
                    # assert stdout is piped
                    self.assertEqual(proc.stdout, MockProcess.PIPE)
                    self.assertEqual(proc.stderr, proc.stdout)
                    # test multi server process calls spawn multiple times
                    manager.subprocess = MockProcess([11, 12, 13, 14])
                    conf1 = self.join_swift_dir('test-server/1.conf')
                    conf2 = self.join_swift_dir('test-server/2.conf')
                    conf3 = self.join_swift_dir('test-server/3.conf')
                    conf4 = self.join_swift_dir('test-server/4.conf')
                    server = manager.Server('test', run_dir=t)
                    # test server run once
                    server.spawn(conf1, once=True)
                    self.assertTrue(server.procs)
                    self.assertEqual(len(server.procs), 1)
                    proc = server.procs[0]
                    expected_args = ['swift-test-server', conf1, 'once']
                    # assert stdout is piped
                    self.assertEqual(proc.stdout, MockProcess.PIPE)
                    self.assertEqual(proc.stderr, proc.stdout)
                    # test server not daemon
                    server.spawn(conf2, daemon=False)
                    self.assertTrue(server.procs)
                    self.assertEqual(len(server.procs), 2)
                    proc = server.procs[1]
                    expected_args = ['swift-test-server', conf2, 'verbose']
                    self.assertEqual(proc.args, expected_args)
                    # assert stdout is not changed
                    self.assertIsNone(proc.stdout)
                    self.assertIsNone(proc.stderr)
                    # test server wait
                    server.spawn(conf3, wait=False)
                    self.assertTrue(server.procs)
                    self.assertEqual(len(server.procs), 3)
                    proc = server.procs[2]
                    # assert stdout is /dev/null
                    with open('/dev/null', 'wb+') as fp:
                        self.assertTrue(isinstance(proc.stdout, type(fp)))
                    self.assertEqual(proc.stdout.name, os.devnull)
                    self.assertIn('b', proc.stdout.mode)
                    self.assertTrue(any(x in proc.stdout.mode for x in 'aw+'),
                                    'mode must be writable, not %r' %
                                    proc.stdout.mode)
                    self.assertEqual(proc.stderr, proc.stdout)
                    # test not daemon over-rides wait
                    server.spawn(conf4, wait=False, daemon=False, once=True)
                    self.assertTrue(server.procs)
                    self.assertEqual(len(server.procs), 4)
                    proc = server.procs[3]
                    expected_args = ['swift-test-server', conf4, 'once',
                                     'verbose']
                    self.assertEqual(proc.args, expected_args)
                    # daemon behavior should trump wait, once shouldn't matter
                    self.assertIsNone(proc.stdout)
                    self.assertIsNone(proc.stderr)
                    # assert pids
                    for i, proc in enumerate(server.procs):
                        pid_file = self.join_run_dir('test-server/%d.pid' %
                                                     (i + 1))
                        pid_on_disk = int(open(pid_file).read().strip())
                        self.assertEqual(pid_on_disk, proc.pid)
                finally:
                    manager.subprocess = old_subprocess

    def test_wait(self):
        server = manager.Server('test')
        self.assertEqual(server.wait(), 0)

        class MockProcess(threading.Thread):
            def __init__(self, delay=0.1, fail_to_start=False):
                threading.Thread.__init__(self)
                # setup pipe
                rfd, wfd = os.pipe()
                # subprocess connection to read stdout
                self.stdout = os.fdopen(rfd)
                # real process connection to write stdout
                self._stdout = os.fdopen(wfd, 'w')
                self.delay = delay
                self.finished = False
                self.returncode = None
                if fail_to_start:
                    self._returncode = 1
                    self.run = self.fail
                else:
                    self._returncode = 0

            def __enter__(self):
                self.start()
                return self

            def __exit__(self, *args):
                if self.isAlive():
                    self.join()

            def close_stdout(self):
                self._stdout.flush()
                with open(os.devnull, 'wb') as nullfile:
                    try:
                        os.dup2(nullfile.fileno(), self._stdout.fileno())
                    except OSError:
                        pass

            def fail(self):
                print('mock process started', file=self._stdout)
                sleep(self.delay)  # perform setup processing
                print('mock process failed to start', file=self._stdout)
                self.close_stdout()

            def poll(self):
                self.returncode = self._returncode
                return self.returncode or None

            def run(self):
                print('mock process started', file=self._stdout)
                sleep(self.delay)  # perform setup processing
                print('setup complete!', file=self._stdout)
                self.close_stdout()
                sleep(self.delay)  # do some more processing
                print('mock process finished', file=self._stdout)
                self.finished = True

        class MockTime(object):

            def time(self):
                return time()

            def sleep(self, *args, **kwargs):
                pass

        with temptree([]) as t:
            old_stdout = sys.stdout
            old_wait = manager.WARNING_WAIT
            old_time = manager.time
            try:
                manager.WARNING_WAIT = 0.01
                manager.time = MockTime()
                with open(os.path.join(t, 'output'), 'w+') as f:
                    # actually capture the read stdout (for prints)
                    sys.stdout = f
                    # test closing pipe in subprocess unblocks read
                    with MockProcess() as proc:
                        server.procs = [proc]
                        status = server.wait()
                        self.assertEqual(status, 0)
                        # wait should return before process exits
                        self.assertTrue(proc.isAlive())
                        self.assertFalse(proc.finished)
                    self.assertTrue(proc.finished)  # make sure it did finish
                    # test output kwarg prints subprocess output
                    with MockProcess() as proc:
                        server.procs = [proc]
                        status = server.wait(output=True)
                    output = pop_stream(f)
                    self.assertIn('mock process started', output)
                    self.assertIn('setup complete', output)
                    # make sure we don't get prints after stdout was closed
                    self.assertNotIn('mock process finished', output)
                    # test process which fails to start
                    with MockProcess(fail_to_start=True) as proc:
                        server.procs = [proc]
                        status = server.wait()
                        self.assertEqual(status, 1)
                    self.assertIn('failed', pop_stream(f))
                    # test multiple procs
                    procs = [MockProcess(delay=.5) for i in range(3)]
                    for proc in procs:
                        proc.start()
                    server.procs = procs
                    status = server.wait()
                    self.assertEqual(status, 0)
                    for proc in procs:
                        self.assertTrue(proc.isAlive())
                    for proc in procs:
                        proc.join()
            finally:
                sys.stdout = old_stdout
                manager.WARNING_WAIT = old_wait
                manager.time = old_time

    def test_interact(self):
        class MockProcess(object):

            def __init__(self, fail=False):
                self.returncode = None
                if fail:
                    self._returncode = 1
                else:
                    self._returncode = 0

            def communicate(self):
                self.returncode = self._returncode
                return '', ''

        server = manager.Server('test')
        server.procs = [MockProcess()]
        self.assertEqual(server.interact(), 0)
        server.procs = [MockProcess(fail=True)]
        self.assertEqual(server.interact(), 1)
        procs = []
        for fail in (False, True, True):
            procs.append(MockProcess(fail=fail))
        server.procs = procs
        self.assertTrue(server.interact() > 0)

    def test_launch(self):
        # stubs
        conf_files = (
            'proxy-server.conf',
            'auth-server.conf',
            'object-server/1.conf',
            'object-server/2.conf',
            'object-server/3.conf',
            'object-server/4.conf',
        )
        pid_files = (
            ('proxy-server.pid', 1),
            ('proxy-server/2.pid', 2),
        )

        # mocks
        class MockSpawn(object):

            def __init__(self, pids=None):
                self.conf_files = []
                self.kwargs = []
                if not pids:
                    def one_forever():
                        while True:
                            yield 1
                    self.pids = one_forever()
                else:
                    self.pids = (x for x in pids)

            def __call__(self, conf_file, **kwargs):
                self.conf_files.append(conf_file)
                self.kwargs.append(kwargs)
                rv = next(self.pids)
                if isinstance(rv, Exception):
                    raise rv
                else:
                    return rv

        with temptree(conf_files) as swift_dir:
            manager.SWIFT_DIR = swift_dir
            files, pids = zip(*pid_files)
            with temptree(files, pids) as t:
                manager.RUN_DIR = t
                old_stdout = sys.stdout
                try:
                    with open(os.path.join(t, 'output'), 'w+') as f:
                        sys.stdout = f
                        # can't start server w/o an conf
                        server = manager.Server('test', run_dir=t)
                        self.assertFalse(server.launch())
                        # start mock os running all pids
                        manager.os = MockOs(pids)
                        proc_files = (
                            ('1/cmdline', 'swift-proxy-server'),
                            ('2/cmdline', 'swift-proxy-server'),
                        )
                        files, contents = zip(*proc_files)
                        with temptree(files, contents) as proc_dir:
                            manager.PROC_DIR = proc_dir
                            server = manager.Server('proxy', run_dir=t)
                            # can't start server if it's already running
                            self.assertFalse(server.launch())
                            output = pop_stream(f)
                            self.assertTrue('running' in output)
                            conf_file = self.join_swift_dir(
                                'proxy-server.conf')
                            self.assertTrue(conf_file in output)
                            pid_file = self.join_run_dir('proxy-server/2.pid')
                            self.assertTrue(pid_file in output)
                            self.assertTrue('already started' in output)

                        # no running pids
                        manager.os = MockOs([])
                        with temptree([], []) as proc_dir:
                            manager.PROC_DIR = proc_dir
                            # test ignore once for non-start-once server
                            mock_spawn = MockSpawn([1])
                            server.spawn = mock_spawn
                            conf_file = self.join_swift_dir(
                                'proxy-server.conf')
                            expected = {
                                1: conf_file,
                            }
                            self.assertEqual(server.launch(once=True),
                                             expected)
                            self.assertEqual(mock_spawn.conf_files,
                                             [conf_file])
                            expected = {
                                'once': False,
                            }
                            self.assertEqual(mock_spawn.kwargs, [expected])
                            output = pop_stream(f)
                            self.assertIn('Starting', output)
                            self.assertNotIn('once', output)
                        # test multi-server kwarg once
                        server = manager.Server('object-replicator')
                        with temptree([], []) as proc_dir:
                            manager.PROC_DIR = proc_dir
                            mock_spawn = MockSpawn([1, 2, 3, 4])
                            server.spawn = mock_spawn
                            conf1 = self.join_swift_dir('object-server/1.conf')
                            conf2 = self.join_swift_dir('object-server/2.conf')
                            conf3 = self.join_swift_dir('object-server/3.conf')
                            conf4 = self.join_swift_dir('object-server/4.conf')
                            expected = {
                                1: conf1,
                                2: conf2,
                                3: conf3,
                                4: conf4,
                            }
                            self.assertEqual(server.launch(once=True),
                                             expected)
                            self.assertEqual(mock_spawn.conf_files, [
                                conf1, conf2, conf3, conf4])
                            expected = {
                                'once': True,
                            }
                            self.assertEqual(len(mock_spawn.kwargs), 4)
                            for kwargs in mock_spawn.kwargs:
                                self.assertEqual(kwargs, expected)
                            # test number kwarg
                            mock_spawn = MockSpawn([4])
                            manager.PROC_DIR = proc_dir
                            server.spawn = mock_spawn
                            expected = {
                                4: conf4,
                            }
                            self.assertEqual(server.launch(number=4),
                                             expected)
                            self.assertEqual(mock_spawn.conf_files, [conf4])
                            expected = {
                                'number': 4
                            }
                            self.assertEqual(mock_spawn.kwargs, [expected])
                        # test cmd does not exist
                        server = manager.Server('auth')
                        with temptree([], []) as proc_dir:
                            manager.PROC_DIR = proc_dir
                            mock_spawn = MockSpawn([OSError(errno.ENOENT,
                                                            'blah')])
                            server.spawn = mock_spawn
                            self.assertEqual(server.launch(), {})
                            self.assertTrue(
                                'swift-auth-server does not exist' in
                                pop_stream(f))
                finally:
                    sys.stdout = old_stdout

    def test_stop(self):
        conf_files = (
            'account-server/1.conf',
            'account-server/2.conf',
            'account-server/3.conf',
            'account-server/4.conf',
        )
        pid_files = (
            ('account-reaper/1.pid', 1),
            ('account-reaper/2.pid', 2),
            ('account-reaper/3.pid', 3),
            ('account-reaper/4.pid', 4),
        )

        with temptree(conf_files) as swift_dir:
            manager.SWIFT_DIR = swift_dir
            files, pids = zip(*pid_files)
            with temptree(files, pids) as t:
                manager.RUN_DIR = t
                # start all pids in mock os
                manager.os = MockOs(pids)
                server = manager.Server('account-reaper', run_dir=t)
                # test kill all running pids
                pids = server.stop()
                self.assertEqual(len(pids), 4)
                for pid in (1, 2, 3, 4):
                    self.assertTrue(pid in pids)
                    self.assertEqual(manager.os.pid_sigs[pid],
                                     [signal.SIGTERM])
                conf1 = self.join_swift_dir('account-reaper/1.conf')
                conf2 = self.join_swift_dir('account-reaper/2.conf')
                conf3 = self.join_swift_dir('account-reaper/3.conf')
                conf4 = self.join_swift_dir('account-reaper/4.conf')
                # reset mock os with only 2 running pids
                manager.os = MockOs([3, 4])
                pids = server.stop()
                self.assertEqual(len(pids), 2)
                for pid in (3, 4):
                    self.assertTrue(pid in pids)
                    self.assertEqual(manager.os.pid_sigs[pid],
                                     [signal.SIGTERM])
                self.assertFalse(os.path.exists(conf1))
                self.assertFalse(os.path.exists(conf2))
                # test number kwarg
                manager.os = MockOs([3, 4])
                pids = server.stop(number=3)
                self.assertEqual(len(pids), 1)
                expected = {
                    3: conf3,
                }
                self.assertTrue(pids, expected)
                self.assertEqual(manager.os.pid_sigs[3], [signal.SIGTERM])
                self.assertFalse(os.path.exists(conf4))
                self.assertFalse(os.path.exists(conf3))


class TestManager(unittest.TestCase):

    def test_create(self):
        m = manager.Manager(['test'])
        self.assertEqual(len(m.servers), 1)
        server = m.servers.pop()
        self.assertTrue(isinstance(server, manager.Server))
        self.assertEqual(server.server, 'test-server')
        # test multi-server and simple dedupe
        servers = ['object-replicator', 'object-auditor', 'object-replicator']
        m = manager.Manager(servers)
        self.assertEqual(len(m.servers), 2)
        for server in m.servers:
            self.assertTrue(server.server in servers)
        # test all
        m = manager.Manager(['all'])
        self.assertEqual(len(m.servers), len(manager.ALL_SERVERS))
        for server in m.servers:
            self.assertTrue(server.server in manager.ALL_SERVERS)
        # test main
        m = manager.Manager(['main'])
        self.assertEqual(len(m.servers), len(manager.MAIN_SERVERS))
        for server in m.servers:
            self.assertTrue(server.server in manager.MAIN_SERVERS)
        # test rest
        m = manager.Manager(['rest'])
        self.assertEqual(len(m.servers), len(manager.REST_SERVERS))
        for server in m.servers:
            self.assertTrue(server.server in manager.REST_SERVERS)
        # test main + rest == all
        m = manager.Manager(['main', 'rest'])
        self.assertEqual(len(m.servers), len(manager.ALL_SERVERS))
        for server in m.servers:
            self.assertTrue(server.server in manager.ALL_SERVERS)
        # test dedupe
        m = manager.Manager(['main', 'rest', 'proxy', 'object',
                             'container', 'account'])
        self.assertEqual(len(m.servers), len(manager.ALL_SERVERS))
        for server in m.servers:
            self.assertTrue(server.server in manager.ALL_SERVERS)
        # test glob
        m = manager.Manager(['object-*'])
        object_servers = [s for s in manager.ALL_SERVERS if
                          s.startswith('object')]
        self.assertEqual(len(m.servers), len(object_servers))
        for s in m.servers:
            self.assertTrue(str(s) in object_servers)
        m = manager.Manager(['*-replicator'])
        replicators = [s for s in manager.ALL_SERVERS if
                       s.endswith('replicator')]
        for s in m.servers:
            self.assertTrue(str(s) in replicators)

    def test_iter(self):
        m = manager.Manager(['all'])
        self.assertEqual(len(list(m)), len(manager.ALL_SERVERS))
        for server in m:
            self.assertTrue(server.server in manager.ALL_SERVERS)

    def test_default_strict(self):
        # test default strict
        m = manager.Manager(['proxy'])
        self.assertEqual(m._default_strict, True)
        # aliases
        m = manager.Manager(['main'])
        self.assertEqual(m._default_strict, False)
        m = manager.Manager(['proxy*'])
        self.assertEqual(m._default_strict, False)

    def test_status(self):
        class MockServer(object):

            def __init__(self, server, run_dir=manager.RUN_DIR):
                self.server = server
                self.called_kwargs = []

            def status(self, **kwargs):
                self.called_kwargs.append(kwargs)
                if 'error' in self.server:
                    return 1
                else:
                    return 0

        old_server_class = manager.Server
        try:
            manager.Server = MockServer
            m = manager.Manager(['test'])
            status = m.status()
            self.assertEqual(status, 0)
            m = manager.Manager(['error'])
            status = m.status()
            self.assertEqual(status, 1)
            # test multi-server
            m = manager.Manager(['test', 'error'])
            kwargs = {'key': 'value'}
            status = m.status(**kwargs)
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called_kwargs, [kwargs])
        finally:
            manager.Server = old_server_class

    def test_start(self):
        def mock_setup_env():
            getattr(mock_setup_env, 'called', []).append(True)

        class MockServer(object):
            def __init__(self, server, run_dir=manager.RUN_DIR):
                self.server = server
                self.called = defaultdict(list)

            def launch(self, **kwargs):
                self.called['launch'].append(kwargs)
                if 'noconfig' in self.server:
                    return {}
                elif 'somerunning' in self.server:
                    return {}
                else:
                    return {1: self.server[0]}

            def wait(self, **kwargs):
                self.called['wait'].append(kwargs)
                return int('error' in self.server)

            def stop(self, **kwargs):
                self.called['stop'].append(kwargs)

            def interact(self, **kwargs):
                self.called['interact'].append(kwargs)
                if 'raise' in self.server:
                    raise KeyboardInterrupt
                elif 'error' in self.server:
                    return 1
                else:
                    return 0

        old_setup_env = manager.setup_env
        old_swift_server = manager.Server
        try:
            manager.setup_env = mock_setup_env
            manager.Server = MockServer

            # test no errors on launch
            m = manager.Manager(['proxy'])
            status = m.start()
            self.assertEqual(status, 0)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{}])

            # test error on launch
            m = manager.Manager(['proxy', 'error'])
            status = m.start()
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{}])
                self.assertEqual(server.called['wait'], [{}])

            # test interact
            m = manager.Manager(['proxy', 'error'])
            kwargs = {'daemon': False}
            status = m.start(**kwargs)
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [kwargs])
                self.assertEqual(server.called['interact'], [kwargs])
            m = manager.Manager(['raise'])
            kwargs = {'daemon': False}
            status = m.start(**kwargs)

            # test no config
            m = manager.Manager(['proxy', 'noconfig'])
            status = m.start()
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{}])
                self.assertEqual(server.called['wait'], [{}])

            # test no config with --non-strict
            m = manager.Manager(['proxy', 'noconfig'])
            status = m.start(strict=False)
            self.assertEqual(status, 0)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': False}])
                self.assertEqual(server.called['wait'], [{'strict': False}])

            # test no config --strict
            m = manager.Manager(['proxy', 'noconfig'])
            status = m.start(strict=True)
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': True}])
                self.assertEqual(server.called['wait'], [{'strict': True}])

            # test no config with alias
            m = manager.Manager(['main', 'noconfig'])
            status = m.start()
            self.assertEqual(status, 0)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{}])
                self.assertEqual(server.called['wait'], [{}])

            # test no config with alias and --non-strict
            m = manager.Manager(['main', 'noconfig'])
            status = m.start(strict=False)
            self.assertEqual(status, 0)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': False}])
                self.assertEqual(server.called['wait'], [{'strict': False}])

            # test no config with alias and --strict
            m = manager.Manager(['main', 'noconfig'])
            status = m.start(strict=True)
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': True}])
                self.assertEqual(server.called['wait'], [{'strict': True}])

            # test already all running
            m = manager.Manager(['proxy', 'somerunning'])
            status = m.start()
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{}])
                self.assertEqual(server.called['wait'], [{}])

            # test already all running --non-strict
            m = manager.Manager(['proxy', 'somerunning'])
            status = m.start(strict=False)
            self.assertEqual(status, 0)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': False}])
                self.assertEqual(server.called['wait'], [{'strict': False}])

            # test already all running --strict
            m = manager.Manager(['proxy', 'somerunning'])
            status = m.start(strict=True)
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': True}])
                self.assertEqual(server.called['wait'], [{'strict': True}])

            # test already all running with alias
            m = manager.Manager(['main', 'somerunning'])
            status = m.start()
            self.assertEqual(status, 0)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{}])
                self.assertEqual(server.called['wait'], [{}])

            # test already all running with alias and --non-strict
            m = manager.Manager(['main', 'somerunning'])
            status = m.start(strict=False)
            self.assertEqual(status, 0)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': False}])
                self.assertEqual(server.called['wait'], [{'strict': False}])

            # test already all running with alias and --strict
            m = manager.Manager(['main', 'somerunning'])
            status = m.start(strict=True)
            self.assertEqual(status, 1)
            for server in m.servers:
                self.assertEqual(server.called['launch'], [{'strict': True}])
                self.assertEqual(server.called['wait'], [{'strict': True}])

        finally:
            manager.setup_env = old_setup_env
            manager.Server = old_swift_server

    def test_no_wait(self):
        class MockServer(object):
            def __init__(self, server, run_dir=manager.RUN_DIR):
                self.server = server
                self.called = defaultdict(list)

            def launch(self, **kwargs):
                self.called['launch'].append(kwargs)
                # must return non-empty dict if launch succeeded
                return {1: self.server[0]}

            def wait(self, **kwargs):
                self.called['wait'].append(kwargs)
                return int('error' in self.server)

        orig_swift_server = manager.Server
        try:
            manager.Server = MockServer
            # test success
            init = manager.Manager(['proxy'])
            status = init.no_wait()
            self.assertEqual(status, 0)
            for server in init.servers:
                self.assertEqual(len(server.called['launch']), 1)
                called_kwargs = server.called['launch'][0]
                self.assertFalse(called_kwargs['wait'])
                self.assertFalse(server.called['wait'])
            # test no errocode status even on error
            init = manager.Manager(['error'])
            status = init.no_wait()
            self.assertEqual(status, 0)
            for server in init.servers:
                self.assertEqual(len(server.called['launch']), 1)
                called_kwargs = server.called['launch'][0]
                self.assertTrue('wait' in called_kwargs)
                self.assertFalse(called_kwargs['wait'])
                self.assertFalse(server.called['wait'])
            # test wait with once option
            init = manager.Manager(['updater', 'replicator-error'])
            status = init.no_wait(once=True)
            self.assertEqual(status, 0)
            for server in init.servers:
                self.assertEqual(len(server.called['launch']), 1)
                called_kwargs = server.called['launch'][0]
                self.assertTrue('wait' in called_kwargs)
                self.assertFalse(called_kwargs['wait'])
                self.assertTrue('once' in called_kwargs)
                self.assertTrue(called_kwargs['once'])
                self.assertFalse(server.called['wait'])
        finally:
            manager.Server = orig_swift_server

    def test_no_daemon(self):
        class MockServer(object):

            def __init__(self, server, run_dir=manager.RUN_DIR):
                self.server = server
                self.called = defaultdict(list)

            def launch(self, **kwargs):
                self.called['launch'].append(kwargs)
                # must return non-empty dict if launch succeeded
                return {1: self.server[0]}

            def interact(self, **kwargs):
                self.called['interact'].append(kwargs)
                return int('error' in self.server)

        orig_swift_server = manager.Server
        try:
            manager.Server = MockServer
            # test success
            init = manager.Manager(['proxy'])
            stats = init.no_daemon()
            self.assertEqual(stats, 0)
            # test error
            init = manager.Manager(['proxy', 'object-error'])
            stats = init.no_daemon()
            self.assertEqual(stats, 1)
            # test once
            init = manager.Manager(['proxy', 'object-error'])
            stats = init.no_daemon()
            for server in init.servers:
                self.assertEqual(len(server.called['launch']), 1)
                self.assertEqual(len(server.called['wait']), 0)
                self.assertEqual(len(server.called['interact']), 1)
        finally:
            manager.Server = orig_swift_server

    def test_once(self):
        class MockServer(object):

            def __init__(self, server, run_dir=manager.RUN_DIR):
                self.server = server
                self.called = defaultdict(list)

            def wait(self, **kwargs):
                self.called['wait'].append(kwargs)
                if 'error' in self.server:
                    return 1
                else:
                    return 0

            def launch(self, **kwargs):
                self.called['launch'].append(kwargs)
                return {1: 'account-reaper'}

        orig_swift_server = manager.Server
        try:
            manager.Server = MockServer
            # test no errors
            init = manager.Manager(['account-reaper'])
            status = init.once()
            self.assertEqual(status, 0)
            # test error code on error
            init = manager.Manager(['error-reaper'])
            status = init.once()
            self.assertEqual(status, 1)
            for server in init.servers:
                self.assertEqual(len(server.called['launch']), 1)
                called_kwargs = server.called['launch'][0]
                self.assertEqual(called_kwargs, {'once': True})
                self.assertEqual(len(server.called['wait']), 1)
                self.assertEqual(len(server.called['interact']), 0)
        finally:
            manager.Server = orig_swift_server

    def test_stop(self):
        class MockServerFactory(object):
            class MockServer(object):
                def __init__(self, pids, run_dir=manager.RUN_DIR):
                    self.pids = pids

                def stop(self, **kwargs):
                    return self.pids

                def status(self, **kwargs):
                    return not self.pids

            def __init__(self, server_pids, run_dir=manager.RUN_DIR):
                self.server_pids = server_pids

            def __call__(self, server, run_dir=manager.RUN_DIR):
                return MockServerFactory.MockServer(self.server_pids[server])

        def mock_watch_server_pids(server_pids, **kwargs):
            for server, pids in server_pids.items():
                for pid in pids:
                    if pid is None:
                        continue
                    yield server, pid

        def mock_kill_group(pid, sig):
            self.fail('kill_group should not be called')

        _orig_server = manager.Server
        _orig_watch_server_pids = manager.watch_server_pids
        _orig_kill_group = manager.kill_group
        try:
            manager.watch_server_pids = mock_watch_server_pids
            manager.kill_group = mock_kill_group
            # test stop one server
            server_pids = {
                'test': {1: "dummy.pid"}
            }
            manager.Server = MockServerFactory(server_pids)
            m = manager.Manager(['test'])
            status = m.stop()
            self.assertEqual(status, 0)
            # test not running
            server_pids = {
                'test': {}
            }
            manager.Server = MockServerFactory(server_pids)
            m = manager.Manager(['test'])
            status = m.stop()
            self.assertEqual(status, 1)
            # test kill not running
            server_pids = {
                'test': {}
            }
            manager.Server = MockServerFactory(server_pids)
            m = manager.Manager(['test'])
            status = m.kill()
            self.assertEqual(status, 0)
            # test won't die
            server_pids = {
                'test': {None: None}
            }
            manager.Server = MockServerFactory(server_pids)
            m = manager.Manager(['test'])
            status = m.stop()
            self.assertEqual(status, 1)

        finally:
            manager.Server = _orig_server
            manager.watch_server_pids = _orig_watch_server_pids
            manager.kill_group = _orig_kill_group

    def test_stop_kill_after_timeout(self):
        class MockServerFactory(object):
            class MockServer(object):
                def __init__(self, pids, run_dir=manager.RUN_DIR):
                    self.pids = pids

                def stop(self, **kwargs):
                    return self.pids

                def status(self, **kwargs):
                    return not self.pids

            def __init__(self, server_pids, run_dir=manager.RUN_DIR):
                self.server_pids = server_pids

            def __call__(self, server, run_dir=manager.RUN_DIR):
                return MockServerFactory.MockServer(self.server_pids[server])

        def mock_watch_server_pids(server_pids, **kwargs):
            for server, pids in server_pids.items():
                for pid in pids:
                    if pid is None:
                        continue
                    yield server, pid

        mock_kill_group_called = []

        def mock_kill_group(*args):
            mock_kill_group_called.append(args)

        def mock_kill_group_oserr(*args):
            raise OSError()

        def mock_kill_group_oserr_ESRCH(*args):
            raise OSError(errno.ESRCH, 'No such process')

        _orig_server = manager.Server
        _orig_watch_server_pids = manager.watch_server_pids
        _orig_kill_group = manager.kill_group
        try:
            manager.watch_server_pids = mock_watch_server_pids
            manager.kill_group = mock_kill_group
            # test stop one server
            server_pids = {
                'test': {None: None}
            }
            manager.Server = MockServerFactory(server_pids)
            m = manager.Manager(['test'])
            status = m.stop(kill_after_timeout=True)
            self.assertEqual(status, 1)
            self.assertEqual(mock_kill_group_called, [(None, 9)])

            manager.kill_group = mock_kill_group_oserr
            # test stop one server - OSError
            server_pids = {
                'test': {None: None}
            }
            manager.Server = MockServerFactory(server_pids)
            m = manager.Manager(['test'])
            with self.assertRaises(OSError):
                status = m.stop(kill_after_timeout=True)

            manager.kill_group = mock_kill_group_oserr_ESRCH
            # test stop one server - OSError: No such process
            server_pids = {
                'test': {None: None}
            }
            manager.Server = MockServerFactory(server_pids)
            m = manager.Manager(['test'])
            status = m.stop(kill_after_timeout=True)
            self.assertEqual(status, 1)
        finally:
            manager.Server = _orig_server
            manager.watch_server_pids = _orig_watch_server_pids
            manager.kill_group = _orig_kill_group

    def test_shutdown(self):
        m = manager.Manager(['test'])
        m.stop_was_called = False

        def mock_stop(*args, **kwargs):
            m.stop_was_called = True
            expected = {'graceful': True}
            self.assertEqual(kwargs, expected)
            return 0
        m.stop = mock_stop
        status = m.shutdown()
        self.assertEqual(status, 0)
        self.assertEqual(m.stop_was_called, True)

    def test_restart(self):
        m = manager.Manager(['test'])
        m.stop_was_called = False

        def mock_stop(*args, **kwargs):
            m.stop_was_called = True
            return 0
        m.start_was_called = False

        def mock_start(*args, **kwargs):
            m.start_was_called = True
            return 0
        m.stop = mock_stop
        m.start = mock_start
        status = m.restart()
        self.assertEqual(status, 0)
        self.assertEqual(m.stop_was_called, True)
        self.assertEqual(m.start_was_called, True)

    def test_reload(self):
        def do_test(graceful):
            called = defaultdict(list)

            def stop(self, **kwargs):
                called[self].append(('stop', kwargs))
                return 0

            def start(self, **kwargs):
                called[self].append(('start', kwargs))
                return 0

            m = manager.Manager(['*-server'])
            self.assertEqual(len(m.servers), 4)
            expected_servers = set([server.server for server in m.servers])
            for server in expected_servers:
                self.assertIn(server, manager.GRACEFUL_SHUTDOWN_SERVERS)

            with mock.patch('swift.common.manager.Manager.start', start):
                with mock.patch('swift.common.manager.Manager.stop', stop):
                    status = m.reload(graceful=graceful)

            self.assertEqual(status, 0)
            self.assertEqual(4, len(called))
            actual_servers = set()
            for m, calls in called.items():
                self.assertEqual(calls, [('stop', {'graceful': True}),
                                         ('start', {'graceful': True})])
                actual_servers.update([server.server for server in m.servers])
            self.assertEqual(expected_servers, actual_servers)

        do_test(graceful=True)
        do_test(graceful=False)  # graceful is forced regardless of the kwarg

    def test_force_reload(self):
        m = manager.Manager(['test'])
        m.reload_was_called = False

        def mock_reload(*args, **kwargs):
            m.reload_was_called = True
            return 0
        m.reload = mock_reload
        status = m.force_reload()
        self.assertEqual(status, 0)
        self.assertEqual(m.reload_was_called, True)

    def test_get_command(self):
        m = manager.Manager(['test'])
        self.assertEqual(m.start, m.get_command('start'))
        self.assertEqual(m.force_reload, m.get_command('force-reload'))
        self.assertEqual(m.get_command('force-reload'),
                         m.get_command('force_reload'))
        self.assertRaises(manager.UnknownCommandError, m.get_command,
                          'no_command')
        self.assertRaises(manager.UnknownCommandError, m.get_command,
                          '__init__')

    def test_list_commands(self):
        for cmd, help in manager.Manager.list_commands():
            method = getattr(manager.Manager, cmd.replace('-', '_'), None)
            self.assertTrue(method, '%s is not a command' % cmd)
            self.assertTrue(getattr(method, 'publicly_accessible', False))
            self.assertEqual(method.__doc__.strip(), help)

    def test_run_command(self):
        m = manager.Manager(['test'])
        m.cmd_was_called = False

        def mock_cmd(*args, **kwargs):
            m.cmd_was_called = True
            expected = {'kw1': True, 'kw2': False}
            self.assertEqual(kwargs, expected)
            return 0
        mock_cmd.publicly_accessible = True
        m.mock_cmd = mock_cmd
        kwargs = {'kw1': True, 'kw2': False}
        status = m.run_command('mock_cmd', **kwargs)
        self.assertEqual(status, 0)
        self.assertEqual(m.cmd_was_called, True)

if __name__ == '__main__':
    unittest.main()
