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

# TODO: Test kill_children signal handlers

import unittest
from getpass import getuser
import logging
from StringIO import StringIO
from test.unit import tmpfile

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
        self.assertEquals(d.conf, {})
        self.assert_(isinstance(d.logger, utils.LogAdapter))

    def test_stubs(self):
        d = daemon.Daemon({})
        self.assertRaises(NotImplementedError, d.run_once)
        self.assertRaises(NotImplementedError, d.run_forever)


class TestRunDaemon(unittest.TestCase):

    def setUp(self):
        utils.HASH_PATH_SUFFIX = 'endcap'
        utils.drop_privileges = lambda *args: None
        utils.capture_stdio = lambda *args: None

    def tearDown(self):
        reload(utils)

    def test_run(self):
        d = MyDaemon({})
        self.assertFalse(MyDaemon.forever_called)
        self.assertFalse(MyDaemon.once_called)
        # test default
        d.run()
        self.assertEquals(d.forever_called, True)
        # test once
        d.run(once=True)
        self.assertEquals(d.once_called, True)

    def test_run_daemon(self):
        sample_conf = """[my-daemon]
user = %s
""" % getuser()
        with tmpfile(sample_conf) as conf_file:
            daemon.run_daemon(MyDaemon, conf_file)
            self.assertEquals(MyDaemon.forever_called, True)
            daemon.run_daemon(MyDaemon, conf_file, once=True)
            self.assertEquals(MyDaemon.once_called, True)

            # test raise in daemon code
            MyDaemon.run_once = MyDaemon.run_raise
            self.assertRaises(OSError, daemon.run_daemon, MyDaemon,
                              conf_file, once=True)

            # test user quit
            MyDaemon.run_forever = MyDaemon.run_quit
            sio = StringIO()
            logger = logging.getLogger('server')
            logger.addHandler(logging.StreamHandler(sio))
            logger = utils.get_logger(None, 'server', log_route='server')
            daemon.run_daemon(MyDaemon, conf_file, logger=logger)
            self.assert_('user quit' in sio.getvalue().lower())


if __name__ == '__main__':
    unittest.main()
