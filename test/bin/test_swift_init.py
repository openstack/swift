# Copyright (c) 2010 OpenStack, LLC.
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

import unittest

import os
import signal
from shutil import rmtree
from contextlib import contextmanager
from tempfile import mkdtemp
from collections import defaultdict

from test import bin  # for reloading...
from test.bin import swift_init  # for testing...


CONF_FILES = (
    'auth-server.conf',
    'auth-server.pid',
    'proxy-server/proxy-server.conf',
    'proxy-server.conf',
    'proxy.conf',
    'object-server/1.conf',
    'object-server/2.conf',
    'object-server/3.conf',
    'object-server/4.conf',
    'account-server/a1.conf',
    'account-server/a2.conf',
    'account-server/a3.conf',
    'container-server/1/container-server.conf',
    'container-server/2/container-server.conf',
    'container-server/3/container-server.conf',
    'container-server/4/container-server.conf',
    'container-server/5/container-server.conf',
    'container-replicator/bogus.conf',
)

PID_FILES = (
    ('auth-server.pid', 10),
    ('proxy-server/proxy-server.pid', 21),
    ('proxy-server.pid', 22),
    ('object-server/1.pid', 31),
    ('object-server/2.pid', 32),
    ('object-server/3.pid', 33),
    ('object-server/4.pid', 34),
    ('object-auditor/2.pid', 42),
    ('object-auditor/1.pid', 41),
    ('object-auditor/3.pid', 43),
    ('container-replicator/1/container-server.pid', 51),
    ('container-replicator/2/container-server.pid', 52),
    ('container-replicator/3/container-server.pid', 53),
    ('container-replicator/4/container-server.pid', 54),
    ('container-replicator/5/container-server.pid', 55),
)

# maps all servers to a list of running pids as defined in PID_FILES
PID_MAP = defaultdict(list)
for server in swift_init.ALL_SERVERS:
    for pid_file, pid in PID_FILES:
        if pid_file.startswith(server) and pid_file.endswith('.pid'):
            PID_MAP[server].append(pid)
    PID_MAP[server].sort()
ALL_PIDS = [p for server, pids in PID_MAP.items() for p in pids]

DUMMY_SIG = 1


@contextmanager
def temptree(files, contents=''):
    # generate enough contents to fill the files
    c = len(files)
    contents = (list(contents) + [''] * c)[:c]
    tempdir = mkdtemp()
    for path, content in zip(files, contents):
        if os.path.isabs(path):
            path = '.' + path
        new_path = os.path.join(tempdir, path)
        subdir = os.path.dirname(new_path)
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        with open(new_path, 'w') as f:
            f.write(str(content))
    try:
        yield tempdir
    finally:
        rmtree(tempdir)


class MockOs():

    def __init__(self, pids):
        self.pid_sigs = defaultdict(list)
        for pid in pids:
            self.pid_sigs[pid]
        self.closed_fds = []
        self.child_pid = 9999  # fork defaults to test parent process path
        self.execlp_called = False

    def kill(self, pid, sig):
        if pid not in self.pid_sigs:
            raise OSError(3, 'No such process')
        self.pid_sigs[pid].append(sig)

    """
    def pass_func(self, *args, **kwargs):
        pass

    chdir = setsid = umask = pass_func

    def dup2(self, source, target):
        self.closed_fds.append(target)

    def fork(self):
        return self.child_pid

    def execlp(self, *args):
        self.execlp_called = True

    """

    def __getattr__(self, name):
        # I only over-ride portions of the os module
        try:
            return object.__getattr__(self, name)
        except AttributeError:
            return getattr(os, name)


class TestSwiftInitModule(unittest.TestCase):

    def test_servers(self):
        main_plus_rest = set(swift_init.MAIN_SERVERS + swift_init.REST_SERVERS)
        self.assertEquals(set(swift_init.ALL_SERVERS), main_plus_rest)
        # make sure there's no server listed in both
        self.assertEquals(len(main_plus_rest), len(swift_init.MAIN_SERVERS) +
                          len(swift_init.REST_SERVERS))

    def test_search_tree(self):
        with temptree(CONF_FILES) as t:
            auth_conf = swift_init.search_tree(t, 'auth-server*', '.conf')
            self.assertEquals(len(auth_conf), 1)
            self.assertEquals(auth_conf[0],
                              os.path.join(t, 'auth-server.conf'))
            proxy_conf = swift_init.search_tree(t, 'proxy*', 'conf')
            self.assertEquals(len(proxy_conf), 3)
            self.assertEquals(proxy_conf[0],
                              os.path.join(t, 'proxy-server.conf'))
            object_confs = swift_init.search_tree(t, 'object-server*', '.conf')
            self.assertEquals(len(object_confs), 4)
            for i, conf in enumerate(object_confs):
                path = os.path.join(t, 'object-server/%d.conf' % (i + 1))
                self.assertEquals(conf, path)
            account_confs = swift_init.search_tree(t, 'account-server*',
                                                   '.conf')
            self.assertEquals(len(account_confs), 3)
            for i, conf in enumerate(account_confs):
                path = os.path.join(t, 'account-server/a%d.conf' % (i + 1))
                self.assertEquals(conf, path)
            container_confs = swift_init.search_tree(t, 'container-server*',
                                                     '.conf')
            self.assertEquals(len(container_confs), 5)
            for i, conf in enumerate(container_confs):
                path = os.path.join(t,
                        'container-server/%s/container-server.conf' % (i + 1))
                self.assertEquals(conf, path)

    def test_write_file(self):
        with temptree([]) as t:
            file_name = os.path.join(t, 'test')
            swift_init.write_file(file_name, 'test')
            with open(file_name, 'r') as f:
                contents = f.read()
            self.assertEquals(contents, 'test')

    def test_remove_file(self):
        with temptree([]) as t:
            file_name = os.path.join(t, 'blah.pid')
            # assert no raise
            self.assertEquals(os.path.exists(file_name), False)
            self.assertEquals(swift_init.remove_file(file_name), None)
            with open(file_name, 'w') as f:
                f.write('1')
            self.assert_(os.path.exists(file_name))
            self.assertEquals(swift_init.remove_file(file_name), None)
            self.assertFalse(os.path.exists(file_name))

    def test_command_wrapper(self):
        @swift_init.command
        def myfunc(arg1):
            """test doc
            """
            return arg1

        self.assertEquals(myfunc.__doc__.strip(), 'test doc')
        self.assertEquals(myfunc(1), 1)
        self.assertEquals(myfunc(0), 0)
        self.assertEquals(myfunc(True), 1)
        self.assertEquals(myfunc(False), 0)
        self.assert_(hasattr(myfunc, 'publicly_accessible'))
        self.assert_(myfunc.publicly_accessible)

    def test_exc(self):
        self.assert_(issubclass(swift_init.UnknownCommand, Exception))


class TestSwiftServerClass(unittest.TestCase):

    def tearDown(self):
        reload(bin)

    def join_swift_dir(self, path):
        return os.path.join(swift_init.SWIFT_DIR, path)

    def join_run_dir(self, path):
        return os.path.join(swift_init.RUN_DIR, path)

    def test_server_init(self):
        server = swift_init.SwiftServer('proxy')
        self.assertEquals(server.server, 'proxy-server')
        self.assertEquals(server.type, 'proxy')
        self.assertEquals(server.cmd, 'swift-proxy-server')
        server = swift_init.SwiftServer('object-replicator')
        self.assertEquals(server.server, 'object-replicator')
        self.assertEquals(server.type, 'object')
        self.assertEquals(server.cmd, 'swift-object-replicator')

    def test_get_pid_file_name(self):
        server = swift_init.SwiftServer('proxy')
        ini_file = self.join_swift_dir('proxy-server.conf')
        pid_file = self.join_run_dir('proxy-server.pid')
        self.assertEquals(pid_file, server.get_pid_file_name(ini_file))
        server = swift_init.SwiftServer('object-replicator')
        ini_file = self.join_swift_dir('object-server/1.conf')
        pid_file = self.join_run_dir('object-replicator/1.pid')
        self.assertEquals(pid_file, server.get_pid_file_name(ini_file))
        server = swift_init.SwiftServer('container-auditor')
        ini_file = self.join_swift_dir(
            'container-server/1/container-auditor.conf')
        pid_file = self.join_run_dir(
            'container-auditor/1/container-auditor.pid')
        self.assertEquals(pid_file, server.get_pid_file_name(ini_file))

    def test_get_ini_file_name(self):
        server = swift_init.SwiftServer('proxy')
        ini_file = self.join_swift_dir('proxy-server.conf')
        pid_file = self.join_run_dir('proxy-server.pid')
        self.assertEquals(ini_file, server.get_ini_file_name(pid_file))
        server = swift_init.SwiftServer('object-replicator')
        ini_file = self.join_swift_dir('object-server/1.conf')
        pid_file = self.join_run_dir('object-replicator/1.pid')
        self.assertEquals(ini_file, server.get_ini_file_name(pid_file))
        server = swift_init.SwiftServer('container-auditor')
        ini_file = self.join_swift_dir(
            'container-server/1/container-auditor.conf')
        pid_file = self.join_run_dir(
            'container-auditor/1/container-auditor.pid')
        self.assertEquals(ini_file, server.get_ini_file_name(pid_file))

    def test_ini_files(self):
        with temptree(CONF_FILES) as t:
            swift_init.SWIFT_DIR = t
            servers = \
                'auth object account-auditor container-replicator'.split()
            for server in servers:
                server = swift_init.SwiftServer(server)
                ini_files = server.ini_files()
                conf_files = [self.join_swift_dir(x) for x in CONF_FILES if
                              x.startswith('%s-server' % server.type) and
                              x.endswith('.conf')]
                self.assertEquals(ini_files, conf_files)

                # test config number kwarg returns the correct single config
                for i in range(len(ini_files)):
                    ini_files = server.ini_files(number=i + 1)
                    self.assertEquals(len(ini_files), 1)
                    self.assertEquals(ini_files[0], conf_files[i])

                # and returns empty for missing config number
                ini_files = server.ini_files(number=i + 2)
                self.assertEquals(ini_files, [])


    def test_pid_files(self):
        files, contents = zip(*PID_FILES)
        # throw some garbage files in the mix too
        files += CONF_FILES
        files += tuple([f.replace('.conf', '.bogus') for f in CONF_FILES])

        def assert_pid_files(server):
            server = swift_init.SwiftServer(server)
            pid_files = server.pid_files()
            # should only find pids files
            self.assertEquals(len(pid_files), len(PID_MAP[server.server]))
            real_files = sorted([x for x in files if x.startswith(server.server) and
                          x.endswith('.pid')])
            for i, pid_file in enumerate(pid_files):
                self.assertEquals(pid_file, self.join_run_dir(real_files[i]))

            # test config number kwarg returns the correct single pid file
            for i in range(len(pid_files)):
                ini_files = server.ini_files(number=i + 1)
                pid_files = server.pid_files(number=i + 1)
                self.assertEquals(len(pid_files), 1)
                pid_file = pid_files[0]
                self.assertEquals(pid_file, self.join_run_dir(real_files[i]))

        with temptree(files, contents) as t:
            swift_init.RUN_DIR = t
            swift_init.SWIFT_DIR = t
            servers = \
                'auth proxy object object-auditor container-replicator'.split()
            for server in servers:
                assert_pid_files(server)

    def test_iter_pid_files(self):
        with temptree(*zip(*PID_FILES)) as t:
            swift_init.RUN_DIR = t
            servers = \
                'auth proxy object object-auditor container-replicator'.split()
            # build up a mapping of pid_files to pids for these server types
            pid_file_map = {}
            for name, pid in PID_FILES:
                if [server for server in servers if server in name]:
                    pid_file_map[self.join_run_dir(name)] = pid
            for i, server in enumerate(servers):
                server = swift_init.SwiftServer(server)
                for pid_file, pid in server.iter_pid_files():
                    self.assertEquals(pid, pid_file_map[pid_file])
                    del pid_file_map[pid_file]
            self.assert_(pid_file_map == {})  # all pid_files used up

    def test_signal_pids(self):
        # stale pid file cleanup is tested in get_running_pids
        def assert_server_signaled_pids(server):
            server = swift_init.SwiftServer(server)
            signaled_pids = server.signal_pids(DUMMY_SIG)
            self.assertEquals(sorted(signaled_pids.keys()), PID_MAP[server.server])
            for pid in signaled_pids:
                self.assertEquals(swift_init.os.pid_sigs[pid], [1])

        with temptree(*zip(*PID_FILES)) as t:
            swift_init.RUN_DIR = t
            swift_init.os = MockOs(ALL_PIDS)
            servers = \
                'auth proxy object object-auditor container-replicator'.split()
            for server in servers:
                assert_server_signaled_pids(server)

    def test_get_running_pids(self):
        def assert_running_pids(server):
            # all pids running to start
            swift_init.os = MockOs(ALL_PIDS)
            server = swift_init.SwiftServer(server)
            running_pids = server.get_running_pids()
            # make sure we only get pids for this server
            self.assertEquals(sorted(running_pids.keys()), PID_MAP[server.server])
            for pid in running_pids:
                # verify the correct signal on the mock
                self.assertEquals(swift_init.os.pid_sigs[pid],
                                  [signal.SIG_DFL])
            current_pids = list(PID_MAP[server.server])
            # each pass will assert the pid from the previous run was removed
            last_pid = -1  # this pid is skipped on the first pass
            for pid in list(current_pids):
                # setup current running pids
                swift_init.os = MockOs(current_pids)
                if not (last_pid == -1):
                    # any valid pid should show up in iter_pid_files
                    self.assert_(last_pid in [
                        p for f, p in list(server.iter_pid_files())])
                running_pids = server.get_running_pids()
                self.assert_(pid in running_pids)
                self.assertFalse(last_pid in running_pids)
                # assert stale pid files were removed!
                self.assertEquals(len(running_pids), len(server.pid_files()))
                if last_pid in ALL_PIDS:
                    self.assertFalse(last_pid in [
                        p for f, p in list(server.iter_pid_files())])
                # get ready for next iter, simulate pid died (i.e. remove # pid)
                last_pid = pid
                current_pids.remove(pid)
            self.assertEquals(len(current_pids), 0)
            swift_init.os = MockOs(current_pids)
            self.assertEquals(len(server.get_running_pids()), 0)

        with temptree(*zip(*PID_FILES)) as t:
            swift_init.RUN_DIR = t
            servers = \
                'auth proxy object object-auditor container-replicator'.split()
            for server in servers:
                assert_running_pids(server)

    def test_kill_running_pids(self):
        def assert_kill_running_pids(server):
            server = swift_init.SwiftServer(server)
            # all pids running
            swift_init.os = MockOs(ALL_PIDS)

            pid_files = server.pid_files()
            killed_pids = server.kill_running_pids()
            self.assertEquals(len(killed_pids), len(pid_files))

            # reinit mock os
            swift_init.os = MockOs(ALL_PIDS)
            # make sure graceful sends sighup on servers that support it
            huped_pids = server.kill_running_pids(graceful=True)
            if server.server in swift_init.GRACEFUL_SHUTDOWN_SERVERS:
                captured_sig = signal.SIGHUP
            else:
                captured_sig = signal.SIGTERM
            for pid in huped_pids:
                self.assertEquals(swift_init.os.pid_sigs[pid], [captured_sig])

        with temptree(*zip(*PID_FILES)) as t:
            swift_init.RUN_DIR = t
            servers = \
                'auth proxy object object-auditor container-replicator'.split()
            for server in servers:
                assert_kill_running_pids(server)

    #TODO: more tests
    def test_status(self):
        pass
    
    def test_spawn(self):
        pass

    def test_wait(self):
        pass

    def test_interact(self):
        pass

    def test_launch(self):
        pass

    def test_stop(self):
        pass


#TODO: test SwiftInit class
class TestSwiftInitClass(unittest.TestCase):

    def test_placeholder(self):
        pass


#TODO: test main
class TestMain(unittest.TestCase):

    def test_placeholder(self):
        pass


if __name__ == '__main__':
    unittest.main()
