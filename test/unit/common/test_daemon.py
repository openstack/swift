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

# TODO(clayg): Test kill_children signal handlers

import os
from six import StringIO
from six.moves import reload_module
import unittest
from getpass import getuser
import logging
from test.unit import tmpfile
import mock
import signal

from swift.common import daemon, utils


class MyDaemon(daemon.Daemon):

    def __init__(self, conf):
        self.conf = conf
        self.logger = utils.get_logger(None, 'server', log_route='server')
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


class TestRunDaemon(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.HASH_PATH_PREFIX = 'startcap'
        utils.drop_privileges = lambda *args: None
        utils.capture_stdio = lambda *args: None

    def tearDown(self):
        reload_module(utils)

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
            d.run()
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
        with tmpfile(sample_conf) as conf_file:
            with mock.patch.dict('os.environ', {'TZ': ''}):
                daemon.run_daemon(MyDaemon, conf_file)
                self.assertEqual(MyDaemon.forever_called, True)
                self.assertTrue(os.environ['TZ'] is not '')
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


if __name__ == '__main__':
    unittest.main()
