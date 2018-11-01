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

import os
from six import StringIO
import time
import unittest
from getpass import getuser
import logging
from test.unit import tmpfile
import mock
import signal
from contextlib import contextmanager
import itertools
from collections import defaultdict
import errno

from swift.common import daemon, utils
from test.unit import debug_logger


class MyDaemon(daemon.Daemon):

    def __init__(self, conf):
        self.conf = conf
        self.logger = debug_logger('my-daemon')
        MyDaemon.forever_called = False
        MyDaemon.once_called = False

    def run_forever(self):
        MyDaemon.forever_called = True

    def run_once(self):
        MyDaemon.once_called = True

    def run_raise(self):
        raise OSError

    def run_quit(self):
        raise KeyboardInterrupt


class TestDaemon(unittest.TestCase):

    def test_create(self):
        d = daemon.Daemon({})
        self.assertEqual(d.conf, {})
        self.assertTrue(isinstance(d.logger, utils.LogAdapter))

    def test_stubs(self):
        d = daemon.Daemon({})
        self.assertRaises(NotImplementedError, d.run_once)
        self.assertRaises(NotImplementedError, d.run_forever)


class MyWorkerDaemon(MyDaemon):

    def __init__(self, *a, **kw):
        super(MyWorkerDaemon, self).__init__(*a, **kw)
        MyWorkerDaemon.post_multiprocess_run_called = False

    def get_worker_args(self, once=False, **kwargs):
        return [kwargs for i in range(int(self.conf.get('workers', 0)))]

    def is_healthy(self):
        try:
            return getattr(self, 'health_side_effects', []).pop(0)
        except IndexError:
            return True

    def post_multiprocess_run(self):
        MyWorkerDaemon.post_multiprocess_run_called = True


class TestWorkerDaemon(unittest.TestCase):

    def test_stubs(self):
        d = daemon.Daemon({})
        self.assertRaises(NotImplementedError, d.run_once)
        self.assertRaises(NotImplementedError, d.run_forever)
        self.assertEqual([], d.get_worker_args())
        self.assertEqual(True, d.is_healthy())

    def test_my_worker_daemon(self):
        d = MyWorkerDaemon({})
        self.assertEqual([], d.get_worker_args())
        self.assertTrue(d.is_healthy())
        d = MyWorkerDaemon({'workers': '3'})
        self.assertEqual([{'key': 'val'}] * 3, d.get_worker_args(key='val'))
        d.health_side_effects = [True, False]
        self.assertTrue(d.is_healthy())
        self.assertFalse(d.is_healthy())
        self.assertTrue(d.is_healthy())


class TestRunDaemon(unittest.TestCase):

    def setUp(self):
        for patcher in [
            mock.patch.object(utils, 'HASH_PATH_PREFIX', b'startcap'),
            mock.patch.object(utils, 'HASH_PATH_SUFFIX', b'endcap'),
            mock.patch.object(utils, 'drop_privileges', lambda *args: None),
            mock.patch.object(utils, 'capture_stdio', lambda *args: None),
        ]:
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_run(self):
        d = MyDaemon({})
        self.assertFalse(MyDaemon.forever_called)
        self.assertFalse(MyDaemon.once_called)
        # test default
        d.run()
        self.assertEqual(d.forever_called, True)
        # test once
        d.run(once=True)
        self.assertEqual(d.once_called, True)

    def test_signal(self):
        d = MyDaemon({})
        with mock.patch('swift.common.daemon.signal') as mock_signal:
            mock_signal.SIGTERM = signal.SIGTERM
            daemon.DaemonStrategy(d, d.logger).run()
        signal_args, kwargs = mock_signal.signal.call_args
        sig, func = signal_args
        self.assertEqual(sig, signal.SIGTERM)
        with mock.patch('swift.common.daemon.os') as mock_os:
            func()
        self.assertEqual(mock_os.method_calls, [
            mock.call.killpg(0, signal.SIGTERM),
            # hard exit because bare except handlers can trap SystemExit
            mock.call._exit(0)
        ])

    def test_run_daemon(self):
        sample_conf = "[my-daemon]\nuser = %s\n" % getuser()
        with tmpfile(sample_conf) as conf_file, \
                mock.patch('swift.common.daemon.use_hub') as mock_use_hub:
            with mock.patch.dict('os.environ', {'TZ': ''}), \
                    mock.patch('time.tzset') as mock_tzset:
                daemon.run_daemon(MyDaemon, conf_file)
                self.assertTrue(MyDaemon.forever_called)
                self.assertEqual(os.environ['TZ'], 'UTC+0')
                self.assertEqual(mock_tzset.mock_calls, [mock.call()])
                self.assertEqual(mock_use_hub.mock_calls,
                                 [mock.call(utils.get_hub())])
            daemon.run_daemon(MyDaemon, conf_file, once=True)
            self.assertEqual(MyDaemon.once_called, True)

            # test raise in daemon code
            with mock.patch.object(MyDaemon, 'run_once', MyDaemon.run_raise):
                self.assertRaises(OSError, daemon.run_daemon, MyDaemon,
                                  conf_file, once=True)

            # test user quit
            sio = StringIO()
            logger = logging.getLogger('server')
            logger.addHandler(logging.StreamHandler(sio))
            logger = utils.get_logger(None, 'server', log_route='server')
            with mock.patch.object(MyDaemon, 'run_forever', MyDaemon.run_quit):
                daemon.run_daemon(MyDaemon, conf_file, logger=logger)
            self.assertTrue('user quit' in sio.getvalue().lower())

            # test missing section
            sample_conf = "[default]\nuser = %s\n" % getuser()
            with tmpfile(sample_conf) as conf_file:
                self.assertRaisesRegexp(SystemExit,
                                        'Unable to find my-daemon '
                                        'config section in.*',
                                        daemon.run_daemon, MyDaemon,
                                        conf_file, once=True)

    def test_run_daemon_diff_tz(self):
        old_tz = os.environ.get('TZ', '')
        try:
            os.environ['TZ'] = 'EST+05EDT,M4.1.0,M10.5.0'
            time.tzset()
            self.assertEqual((1970, 1, 1, 0, 0, 0), time.gmtime(0)[:6])
            self.assertEqual((1969, 12, 31, 19, 0, 0), time.localtime(0)[:6])
            self.assertEqual(18000, time.timezone)

            sample_conf = "[my-daemon]\nuser = %s\n" % getuser()
            with tmpfile(sample_conf) as conf_file, \
                    mock.patch('swift.common.daemon.use_hub'):
                daemon.run_daemon(MyDaemon, conf_file)
                self.assertFalse(MyDaemon.once_called)
                self.assertTrue(MyDaemon.forever_called)

            self.assertEqual((1970, 1, 1, 0, 0, 0), time.gmtime(0)[:6])
            self.assertEqual((1970, 1, 1, 0, 0, 0), time.localtime(0)[:6])
            self.assertEqual(0, time.timezone)
        finally:
            os.environ['TZ'] = old_tz
            time.tzset()

    @contextmanager
    def mock_os(self, child_worker_cycles=3):
        self.waitpid_calls = defaultdict(int)

        def mock_waitpid(p, *args):
            self.waitpid_calls[p] += 1
            if self.waitpid_calls[p] >= child_worker_cycles:
                rv = p
            else:
                rv = 0
            return rv, 0
        with mock.patch('swift.common.daemon.os.fork') as mock_fork, \
                mock.patch('swift.common.daemon.os.waitpid', mock_waitpid), \
                mock.patch('swift.common.daemon.os.kill') as mock_kill:
            mock_fork.side_effect = (
                'mock-pid-%s' % i for i in itertools.count())
            self.mock_fork = mock_fork
            self.mock_kill = mock_kill
            yield

    def test_fork_workers(self):
        d = MyWorkerDaemon({'workers': 3})
        strategy = daemon.DaemonStrategy(d, d.logger)
        with self.mock_os():
            strategy.run(once=True)
        self.assertEqual([mock.call()] * 3, self.mock_fork.call_args_list)
        self.assertEqual(self.waitpid_calls, {
            'mock-pid-0': 3,
            'mock-pid-1': 3,
            'mock-pid-2': 3,
        })
        self.assertEqual([], self.mock_kill.call_args_list)
        self.assertIn('Finished', d.logger.get_lines_for_level('notice')[-1])
        self.assertTrue(MyWorkerDaemon.post_multiprocess_run_called)

    def test_forked_worker(self):
        d = MyWorkerDaemon({'workers': 3})
        strategy = daemon.DaemonStrategy(d, d.logger)
        with mock.patch('swift.common.daemon.os.fork') as mock_fork, \
                mock.patch('swift.common.daemon.os._exit') as mock_exit:
            mock_fork.return_value = 0
            mock_exit.side_effect = SystemExit
            self.assertRaises(SystemExit, strategy.run, once=True)
        self.assertTrue(d.once_called)

    def test_restart_workers(self):
        d = MyWorkerDaemon({'workers': 3})
        strategy = daemon.DaemonStrategy(d, d.logger)
        d.health_side_effects = [True, False]
        with self.mock_os():
            self.mock_kill.side_effect = lambda *args, **kwargs: setattr(
                strategy, 'running', False)
            strategy.run()
        # six workers forked in total
        self.assertEqual([mock.call()] * 6, self.mock_fork.call_args_list)
        # since the daemon starts healthy, first pass checks children once
        self.assertEqual(self.waitpid_calls, {
            'mock-pid-0': 1,
            'mock-pid-1': 1,
            'mock-pid-2': 1,
        })
        # second pass is not healthy, original pid's killed
        self.assertEqual(set([
            ('mock-pid-0', signal.SIGTERM),
            ('mock-pid-1', signal.SIGTERM),
            ('mock-pid-2', signal.SIGTERM),
        ]), set(c[0] for c in self.mock_kill.call_args_list[:3]))
        # our mock_kill side effect breaks out of running, and cleanup kills
        # remaining pids
        self.assertEqual(set([
            ('mock-pid-3', signal.SIGTERM),
            ('mock-pid-4', signal.SIGTERM),
            ('mock-pid-5', signal.SIGTERM),
        ]), set(c[0] for c in self.mock_kill.call_args_list[3:]))

    def test_worker_disappears(self):
        d = MyWorkerDaemon({'workers': 3})
        strategy = daemon.DaemonStrategy(d, d.logger)
        strategy.register_worker_start('mock-pid', {'mock_options': True})
        self.assertEqual(strategy.unspawned_worker_options, [])
        self.assertEqual(strategy.options_by_pid, {
            'mock-pid': {'mock_options': True}
        })
        # still running
        with mock.patch('swift.common.daemon.os.waitpid') as mock_waitpid:
            mock_waitpid.return_value = (0, 0)
            strategy.check_on_all_running_workers()
        self.assertEqual(strategy.unspawned_worker_options, [])
        self.assertEqual(strategy.options_by_pid, {
            'mock-pid': {'mock_options': True}
        })
        # finished
        strategy = daemon.DaemonStrategy(d, d.logger)
        strategy.register_worker_start('mock-pid', {'mock_options': True})
        with mock.patch('swift.common.daemon.os.waitpid') as mock_waitpid:
            mock_waitpid.return_value = ('mock-pid', 0)
            strategy.check_on_all_running_workers()
        self.assertEqual(strategy.unspawned_worker_options, [
            {'mock_options': True}])
        self.assertEqual(strategy.options_by_pid, {})
        self.assertEqual(d.logger.get_lines_for_level('debug')[-1],
                         'Worker mock-pid exited')
        # disappeared
        strategy = daemon.DaemonStrategy(d, d.logger)
        strategy.register_worker_start('mock-pid', {'mock_options': True})
        with mock.patch('swift.common.daemon.os.waitpid') as mock_waitpid:
            mock_waitpid.side_effect = OSError(
                errno.ECHILD, os.strerror(errno.ECHILD))
            mock_waitpid.return_value = ('mock-pid', 0)
            strategy.check_on_all_running_workers()
        self.assertEqual(strategy.unspawned_worker_options, [
            {'mock_options': True}])
        self.assertEqual(strategy.options_by_pid, {})
        self.assertEqual(d.logger.get_lines_for_level('notice')[-1],
                         'Worker mock-pid died')

    def test_worker_kills_pids_in_cleanup(self):
        d = MyWorkerDaemon({'workers': 2})
        strategy = daemon.DaemonStrategy(d, d.logger)
        strategy.register_worker_start('mock-pid-1', {'mock_options': True})
        strategy.register_worker_start('mock-pid-2', {'mock_options': True})
        self.assertEqual(strategy.unspawned_worker_options, [])
        self.assertEqual(strategy.options_by_pid, {
            'mock-pid-1': {'mock_options': True},
            'mock-pid-2': {'mock_options': True},
        })
        with mock.patch('swift.common.daemon.os.kill') as mock_kill:
            strategy.cleanup()
        self.assertEqual(strategy.unspawned_worker_options, [
            {'mock_options': True}] * 2)
        self.assertEqual(strategy.options_by_pid, {})
        self.assertEqual(set([
            ('mock-pid-1', signal.SIGTERM),
            ('mock-pid-2', signal.SIGTERM),
        ]), set(c[0] for c in mock_kill.call_args_list))
        self.assertEqual(set(d.logger.get_lines_for_level('debug')[-2:]),
                         set(['Cleaned up worker mock-pid-1',
                              'Cleaned up worker mock-pid-2']))

    def test_worker_disappears_in_cleanup(self):
        d = MyWorkerDaemon({'workers': 2})
        strategy = daemon.DaemonStrategy(d, d.logger)
        strategy.register_worker_start('mock-pid-1', {'mock_options': True})
        strategy.register_worker_start('mock-pid-2', {'mock_options': True})
        self.assertEqual(strategy.unspawned_worker_options, [])
        self.assertEqual(strategy.options_by_pid, {
            'mock-pid-1': {'mock_options': True},
            'mock-pid-2': {'mock_options': True},
        })
        with mock.patch('swift.common.daemon.os.kill') as mock_kill:
            mock_kill.side_effect = [None, OSError(errno.ECHILD,
                                                   os.strerror(errno.ECHILD))]
            strategy.cleanup()
        self.assertEqual(strategy.unspawned_worker_options, [
            {'mock_options': True}] * 2)
        self.assertEqual(strategy.options_by_pid, {})
        self.assertEqual(set([
            ('mock-pid-1', signal.SIGTERM),
            ('mock-pid-2', signal.SIGTERM),
        ]), set(c[0] for c in mock_kill.call_args_list))
        self.assertEqual(set(d.logger.get_lines_for_level('debug')[-2:]),
                         set(['Cleaned up worker mock-pid-1',
                              'Cleaned up worker mock-pid-2']))


if __name__ == '__main__':
    unittest.main()
